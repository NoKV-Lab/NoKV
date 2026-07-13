use super::snapshot::VersionedSnapshotPin;
use super::*;

/// A top-level child of the rollback target captured before the atomic swap, so it
/// can be CAS-guarded in the swap commit and torn down afterward.
struct OldChild {
    name: DentryName,
    entry: DentryWithAttr,
    dentry_version: Version,
}

/// A node of the now-detached pre-rollback subtree, queued for teardown.
struct DetachedNode {
    inode: InodeId,
    generation: u64,
    body: Option<BodyDescriptor>,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Revert the subtree rooted at `target_root` to the state captured by a prior
    /// snapshot, atomically and as a copy-on-write operation.
    ///
    /// `snapshot_id` must name a retained snapshot pin taken of `target_root`
    /// (`snapshot_subtree(target_root)`); rolling back to a snapshot of a different
    /// root is rejected. The subtree is restored to exactly what it looked like at
    /// the snapshot's `read_version`: post-snapshot creates vanish, post-snapshot
    /// deletes return, and post-snapshot modifications are undone. Reads under
    /// `target_root` immediately observe the restored state.
    ///
    /// # Mechanism
    ///
    /// Rollback is *clone-from-the-snapshot plus an atomic graft*:
    ///
    /// 1. [`NoKvFs::materialize_subtree_at`] reproduces the snapshot's subtree under a
    ///    fresh detached root, sharing the snapshot's object blocks (no data copy, so
    ///    this is O(metadata-size)).
    /// 2. A single metadata commit grafts the materialized children onto
    ///    `target_root` — overwriting same-named entries, dropping entries the delta
    ///    added, and re-parenting the fresh nodes — while CAS-guarding every current
    ///    child dentry. After this one commit, every lookup under `target_root`
    ///    resolves to the restored tree; `target_root` keeps its inode identity, so no
    ///    parent re-link is needed. The detached pre-rollback subtree becomes
    ///    unreachable.
    /// 3. The detached subtree is torn down, enqueueing its inode-owned object blocks
    ///    for GC.
    ///
    /// # GC correctness
    ///
    /// The snapshot's content and the discarded delta share inode identity (the delta
    /// mutated the very inodes the snapshot captured). The restored tree therefore
    /// borrows blocks whose keys are owned by inodes that the teardown destroys. Two
    /// rules keep the shared blocks alive while letting the delta's private blocks go:
    ///
    /// * **The swap installs durable retention.** The same atomic commit that exposes
    ///   restored children creates a [`ForkBinding`] keyed by the materialized
    ///   root's unique inode. Unlike the construction snapshot lease, the binding
    ///   does not expire and its mount-wide history floor keeps every owner-side GC
    ///   row blocked. The materialized root may then be deleted after its children
    ///   escape to `target_root`, just like a clone binding survives its root being
    ///   unlinked while hardlinks remain.
    /// * **Owner GC rows are preserved.** A pre-rollback rewrite/delete may already
    ///   have queued the snapshot blocks. Teardown also queues blocks still owned by
    ///   the discarded current tree. Those rows remain durable; once every borrowed
    ///   reference is rewritten or removed, explicit snapshot retirement releases the
    ///   binding and normal object GC reclaims them.
    ///
    /// The net effect mirrors [`NoKvFs::owns_block_object_key`]: a block reachable
    /// from the live namespace is never reclaimed, a block reachable only from the
    /// discarded delta is.
    pub fn rollback_subtree(
        &self,
        target_root: InodeId,
        snapshot_id: u64,
    ) -> Result<(), MetadError> {
        // Rollback is a rare administrative operation. Holding the in-process
        // GC gate across materialize -> swap prevents an expiring pin from
        // allowing this owner to delete a borrowed block mid-operation. HA GC
        // is separately fail-closed by the durable failover marker.
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let pin = self.live_rollback_snapshot_pin(target_root, snapshot_id)?;
        let snapshot_version = Version::new(pin.pin.read_version)?;

        if self
            .get_attr_at_version_for_purpose(
                target_root,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .is_none_or(|attr| attr.file_type != FileType::Directory)
        {
            return Err(MetadError::NotDirectory);
        }
        self.ensure_rollback_tree_has_no_hardlinks(
            target_root,
            snapshot_version,
            ReadPurpose::Snapshot,
        )?;
        self.ensure_rollback_tree_has_no_hardlinks(
            target_root,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?;

        // 1. Reproduce the snapshot's subtree under a fresh detached root, sharing
        //    the snapshot's blocks. Snapshot-aware scans enumerate entries deleted
        //    by the delta through the retained-history key index.
        let restored_root = self.materialize_subtree_at(target_root, snapshot_version)?;
        let restored_blocks = self.subtree_object_blocks(restored_root)?;

        // 2. Capture both sides' top-level children, then graft atomically.
        let old_children = self.capture_top_level_children(target_root)?;
        let restored_children = self.read_dir_plus_at_version_for_purpose(
            restored_root,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?;
        self.commit_rollback_swap(
            target_root,
            restored_root,
            snapshot_id,
            &old_children,
            &restored_children,
            &restored_blocks,
        )?;

        // 3. Tear down the now-detached pre-rollback subtree, reclaiming the delta's
        //    owner-side metadata and durably queueing its blocks behind the binding.
        self.purge_detached_subtree(&old_children)?;
        // The materialization root is bound (and intentionally deleted by the
        // swap), while the discarded tree is now purged. Clear the slow-path
        // marker only if a mount-wide proof finds no older unsafe orphan.
        let _ = self.reconcile_materialization_orphan_state_under_gc_gate();
        Ok(())
    }

    /// Eager rollback currently materializes each dentry as a fresh inode. Until
    /// hardlink groups are restored as groups, accepting a multiply-linked node
    /// would either split one identity or let teardown delete an inode still named
    /// outside the target subtree. Controlled restore must therefore fail closed.
    fn ensure_rollback_tree_has_no_hardlinks(
        &self,
        root: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<(), MetadError> {
        let mut queue = vec![root];
        while let Some(dir) = queue.pop() {
            for child in self.read_dir_plus_at_version_for_purpose(dir, version, purpose)? {
                if child.attr.file_type == FileType::Directory {
                    queue.push(child.attr.inode);
                } else if child.attr.nlink > 1 {
                    return Err(MetadError::InvalidPath(format!(
                        "rollback requires a hardlink-free subtree; inode {} has {} links",
                        child.attr.inode.get(),
                        child.attr.nlink
                    )));
                }
            }
        }
        Ok(())
    }

    /// Capture each distinct block named by the materialized tree. The atomic
    /// swap proactively ensures an owner-side GC row for these borrowed objects;
    /// this covers historical base generations whose original delete path could
    /// not otherwise enqueue them again.
    fn subtree_object_blocks(&self, root: InodeId) -> Result<Vec<BlockDescriptor>, MetadError> {
        let version = self.read_version()?;
        let mut seen = HashSet::new();
        let mut blocks = Vec::new();
        let mut queue = vec![root];
        while let Some(dir) = queue.pop() {
            for child in self.read_dir_plus_at_version_for_purpose(
                dir,
                version,
                ReadPurpose::WritePlanLocal,
            )? {
                if child.attr.file_type == FileType::Directory {
                    queue.push(child.attr.inode);
                }
                let Some(body) = child.body.as_ref() else {
                    continue;
                };
                for block in self
                    .chunk_manifests_for_body_at_version(
                        child.attr.inode,
                        body,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .into_iter()
                    .flat_map(|manifest| manifest.slices)
                    .flat_map(|slice| slice.blocks)
                {
                    if seen.insert(block.object_key.clone()) {
                        blocks.push(block);
                    }
                }
            }
        }
        Ok(blocks)
    }

    fn live_rollback_snapshot_pin(
        &self,
        target_root: InodeId,
        snapshot_id: u64,
    ) -> Result<VersionedSnapshotPin, MetadError> {
        let pin = self
            .versioned_snapshot_pin_at(
                snapshot_id,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        if pin.pin.root != target_root {
            return Err(MetadError::InvalidPath(format!(
                "snapshot {snapshot_id} pins inode {} but rollback target is {}",
                pin.pin.root.get(),
                target_root.get()
            )));
        }
        self.ensure_snapshot_pin_live(&pin.pin)?;
        Ok(pin)
    }

    /// Path variant of [`NoKvFs::rollback_subtree`]. Resolves `target_path` to its
    /// directory inode and reverts it to `snapshot_id`.
    pub fn rollback_subtree_path(
        &self,
        target_path: &str,
        snapshot_id: u64,
    ) -> Result<(), MetadError> {
        let target_root = self.resolve_directory_path(target_path)?;
        self.rollback_subtree(target_root, snapshot_id)
    }

    /// Snapshot the target's current top-level children with their dentry versions,
    /// so the swap can CAS-guard them and the teardown can walk the detached subtree.
    fn capture_top_level_children(
        &self,
        target_root: InodeId,
    ) -> Result<Vec<OldChild>, MetadError> {
        let version = self.read_version()?;
        let entries = self.read_dir_plus_at_version_for_purpose(
            target_root,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        let mut children = Vec::with_capacity(entries.len());
        for entry in entries {
            let name = entry.dentry.name.clone();
            let Some((_, dentry_version)) = self.lookup_plus_at_version_for_purpose(
                target_root,
                &name,
                version,
                ReadPurpose::WritePlanLocal,
            )?
            else {
                return Err(MetadError::NotFound);
            };
            children.push(OldChild {
                name,
                entry,
                dentry_version,
            });
        }
        Ok(children)
    }

    /// The single atomic commit that installs the restored subtree over
    /// `target_root`: it re-parents the materialized children onto `target_root`,
    /// removes delta-only children, deletes the now-empty materialized root inode,
    /// installs durable rollback retention, and ensures owner-side GC rows for
    /// every block the restored tree borrows.
    fn commit_rollback_swap(
        &self,
        target_root: InodeId,
        restored_root: InodeId,
        snapshot_id: u64,
        old_children: &[OldChild],
        restored_children: &[DentryWithAttr],
        restored_blocks: &[BlockDescriptor],
    ) -> Result<(), MetadError> {
        // Capture both retention fences before the final planning reads. The
        // pin is read again under the GC gate, then its exact record version and
        // the exact durable Open epoch join the atomic swap.
        let object_reference = self.begin_object_reference_mutation()?;
        let pin = self.live_rollback_snapshot_pin(target_root, snapshot_id)?;
        let version = self.next_version()?;
        let read_version = predecessor(version)?;

        let restored_names: HashSet<&[u8]> = restored_children
            .iter()
            .map(|child| child.dentry.name.as_bytes())
            .collect();

        let binding_key = fork_binding_key(self.mount, restored_root);
        let binding = ForkBinding {
            fork_root: restored_root,
            source_root: target_root,
            pinned_read_version: pin.pin.read_version,
            snapshot_id,
            created_version: version.get(),
        };

        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, target_root),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Snapshot,
                key: snapshot_pin_key(self.mount, snapshot_id),
                predicate: Predicate::VersionEquals(pin.version),
            },
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.clone(),
                predicate: Predicate::NotExists,
            },
        ];
        let mut mutations = vec![Mutation {
            family: RecordFamily::ForkBinding,
            key: binding_key,
            op: MutationOp::Put,
            value: Some(Value(encode_fork_binding(&binding))),
        }];
        mutations.extend(self.rollback_object_gc_mutations(restored_blocks, version)?);

        // Guard every current child dentry, and delete the ones the restored tree
        // does not re-establish (same-named entries are overwritten by the puts
        // below, so they need no explicit delete).
        for old in old_children {
            let key = dentry_key(self.mount, target_root, &old.name);
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: key.clone(),
                predicate: Predicate::VersionEquals(old.dentry_version),
            });
            if !restored_names.contains(old.name.as_bytes()) {
                mutations.push(delete_mutation(RecordFamily::Dentry, key));
            }
        }

        // Re-parent each materialized child from the detached root onto the target
        // root, then drop the detached root inode.
        for child in restored_children {
            let mut projection = projection(
                target_root,
                child.dentry.name.clone(),
                child.attr.clone(),
                child.body.clone(),
            );
            projection.dentry.parent = target_root;
            mutations.push(delete_mutation(
                RecordFamily::Dentry,
                dentry_key(self.mount, restored_root, &child.dentry.name),
            ));
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                dentry_key(self.mount, target_root, &child.dentry.name),
                &projection,
            ));
        }
        mutations.push(delete_mutation(
            RecordFamily::Inode,
            inode_key(self.mount, restored_root),
        ));

        // Materialization and swap planning may be large enough to cross the
        // lease deadline. Reject before exposing the restored references; the
        // pin CAS handles concurrent renew/reap after this check.
        self.ensure_snapshot_pin_live(&pin.pin)?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"rollback-subtree-swap", self.mount, target_root, version),
            kind: CommandKind::RenameReplace,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(self.mount, target_root),
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn rollback_object_gc_mutations(
        &self,
        restored_blocks: &[BlockDescriptor],
        enqueue_version: Version,
    ) -> Result<Vec<Mutation>, MetadError> {
        let enqueue_unix_ms = current_time_ms();
        restored_blocks
            .iter()
            .map(|block| {
                let (inode, generation, chunk_index, block_index) =
                    self.canonical_block_object_identity(&block.object_key)?;
                let record = ObjectGcRecord {
                    inode,
                    generation,
                    object_key: block.object_key.clone(),
                    size: block.len,
                    digest_uri: block.digest_uri.clone(),
                    enqueue_version: enqueue_version.get(),
                    enqueue_unix_ms,
                };
                Ok(Mutation {
                    family: RecordFamily::Gc,
                    key: gc_object_key(
                        self.mount,
                        enqueue_version.get(),
                        inode,
                        generation,
                        chunk_index,
                        block_index,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_record(&record))),
                })
            })
            .collect()
    }

    /// Tear down the detached pre-rollback subtree rooted at the captured top-level
    /// children. Each node's metadata is deleted bottom-up and its inode-owned blocks
    /// are enqueued for GC. The rollback binding installed by the swap holds the
    /// retention floor while the restored tree still borrows any of those blocks.
    fn purge_detached_subtree(&self, old_children: &[OldChild]) -> Result<(), MetadError> {
        let version = self.read_version()?;
        // Discover the full detached subtree (the top-level dentries are already
        // gone, but the inodes and their descendants persist until purged).
        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        for old in old_children {
            self.classify_detached_node(&old.entry, &mut nodes, &mut dirs);
        }
        while let Some(dir) = dirs.pop() {
            for child in self.read_dir_plus_at_version_for_purpose(
                dir,
                version,
                ReadPurpose::WritePlanLocal,
            )? {
                self.classify_detached_node(&child, &mut nodes, &mut dirs);
            }
        }
        let retained_object_keys = HashSet::new();
        for node in nodes {
            self.purge_detached_node(&node, &retained_object_keys)?;
        }
        Ok(())
    }

    fn classify_detached_node(
        &self,
        entry: &DentryWithAttr,
        nodes: &mut Vec<DetachedNode>,
        dirs: &mut Vec<InodeId>,
    ) {
        nodes.push(DetachedNode {
            inode: entry.attr.inode,
            generation: entry
                .body
                .as_ref()
                .map_or(entry.attr.generation, |body| body.generation),
            body: entry.body.clone(),
        });
        if entry.attr.file_type == FileType::Directory {
            dirs.push(entry.attr.inode);
        }
    }

    /// Delete one detached inode and its side records (dentries under it, xattrs,
    /// chunk manifests) in a single commit, enqueueing its owned blocks for GC.
    fn purge_detached_node(
        &self,
        node: &DetachedNode,
        retained_object_keys: &HashSet<String>,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let mut mutations = vec![delete_mutation(
            RecordFamily::Inode,
            inode_key(self.mount, node.inode),
        )];

        // Any residual dentries the inode parented (defensive: descendants are
        // purged separately, but a directory inode may still own stale dentry rows).
        for key in self.metadata.scan_keys(KeyScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, node.inode),
            start_after: None,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })? {
            mutations.push(delete_mutation(RecordFamily::Dentry, key));
        }
        for key in self.metadata.scan_keys(KeyScanRequest {
            family: RecordFamily::Xattr,
            prefix: xattr_prefix(self.mount, node.inode),
            start_after: None,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })? {
            mutations.push(delete_mutation(RecordFamily::Xattr, key));
        }
        if node.body.is_some() {
            mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                node.inode,
                node.generation,
                version,
                retained_object_keys,
            )?);
        }

        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"rollback-subtree-purge", self.mount, node.inode, version),
            kind: CommandKind::RemoveFile,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(self.mount, node.inode),
            predicates: Vec::new(),
            mutations,
            watch: Vec::new(),
        })?;
        Ok(())
    }
}
