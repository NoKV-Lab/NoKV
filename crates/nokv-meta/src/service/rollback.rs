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
    /// * **Teardown skips shared blocks.** The detached subtree is purged with the
    ///   restored tree's referenced object keys passed as `retained_object_keys`, so
    ///   [`NoKvFs::chunk_manifest_delete_and_gc_mutations`] never enqueues a block the
    ///   restored tree still points at (e.g. an unchanged file's block). Only blocks
    ///   unique to the delta (a rewritten file's new body, a delta-only file) are
    ///   enqueued.
    /// * **The swap cancels stale enqueues for restored blocks.** A pre-rollback
    ///   rewrite or delete already queued the snapshot's blocks for GC (blocked only
    ///   by the retention floor). The swap commit deletes any pending GC record whose
    ///   object key the restored tree references, resurrecting those blocks as live so
    ///   they survive even after the snapshot pin is retired.
    ///
    /// The net effect mirrors [`NoKvFs::owns_block_object_key`]: a block reachable
    /// from the live namespace is never reclaimed, a block reachable only from the
    /// discarded delta is.
    fn rollback_subtree(&self, target_root: InodeId, snapshot_id: u64) -> Result<(), MetadError> {
        let pin = self
            .snapshot_pin(snapshot_id)?
            .ok_or(MetadError::NotFound)?;
        if pin.root != target_root {
            return Err(MetadError::InvalidPath(format!(
                "snapshot {snapshot_id} pins inode {} but rollback target is {}",
                pin.root.get(),
                target_root.get()
            )));
        }
        if self
            .metadata
            .get(
                RecordFamily::ForkBinding,
                &fork_binding_key(self.mount, target_root),
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .is_some()
        {
            return Err(MetadError::InvalidPath(
                "in-place rollback is not supported for a restore-to-fork destination".to_owned(),
            ));
        }
        let snapshot_version = Version::new(pin.read_version)?;
        if self.subtree_contains_fork_state(target_root, snapshot_version, ReadPurpose::Snapshot)? {
            return Err(MetadError::InvalidPath(
                "in-place rollback is not supported for a restore-to-fork destination".to_owned(),
            ));
        }

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

        // 1. Reproduce the snapshot's subtree under a fresh detached root, sharing
        //    the snapshot's blocks. Snapshot-aware scans enumerate entries deleted
        //    by the delta through the retained-history key index.
        let restored_root = self.materialize_subtree_at(target_root, snapshot_version)?;
        let restored_keys = self.subtree_object_keys(restored_root)?;

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
            &old_children,
            &restored_children,
            &restored_keys,
        )?;

        // 3. Tear down the now-detached pre-rollback subtree, reclaiming the delta's
        //    private blocks while leaving the restored tree's shared blocks live.
        self.purge_detached_subtree(&old_children, &restored_keys)?;
        Ok(())
    }

    /// Path variant of [`NoKvFs::rollback_subtree`]. Resolves `target_path` to its
    /// directory inode and reverts it to `snapshot_id`.
    pub fn rollback_subtree_path(
        &self,
        target_path: &str,
        snapshot_id: u64,
    ) -> Result<(), MetadError> {
        let target_root = self.resolve_non_fork_rollback_path(target_path)?;
        self.rollback_subtree(target_root, snapshot_id)
    }

    fn resolve_non_fork_rollback_path(&self, target_path: &str) -> Result<InodeId, MetadError> {
        let components = parse_absolute_path(target_path)?;
        let version = self.read_version()?;
        let mut current = InodeId::root();
        for name in components {
            if self.has_fork_binding(current, version)? {
                return Err(MetadError::InvalidPath(
                    "in-place rollback is not supported for a restore-to-fork destination"
                        .to_owned(),
                ));
            }
            if self
                .metadata
                .get(
                    RecordFamily::ForkShadow,
                    &fork_shadow_key(self.mount, current, &name),
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .is_some()
            {
                return Err(MetadError::InvalidPath(
                    "in-place rollback is not supported for a restore-to-fork destination"
                        .to_owned(),
                ));
            }
            let (entry, _) = self
                .lookup_plus_at_version_for_purpose(
                    current,
                    &name,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            current = entry.attr.inode;
        }
        if self.has_fork_binding(current, version)? {
            return Err(MetadError::InvalidPath(
                "in-place rollback is not supported for a restore-to-fork destination".to_owned(),
            ));
        }
        Ok(current)
    }

    fn has_fork_binding(&self, inode: InodeId, version: Version) -> Result<bool, MetadError> {
        Ok(self
            .metadata
            .get(
                RecordFamily::ForkBinding,
                &fork_binding_key(self.mount, inode),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .is_some())
    }

    fn subtree_contains_fork_state(
        &self,
        root: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<bool, MetadError> {
        let mut queue = vec![root];
        while let Some(parent) = queue.pop() {
            for child in self.read_dir_plus_at_version_for_purpose(parent, version, purpose)? {
                if self
                    .metadata
                    .get(
                        RecordFamily::ForkShadow,
                        &fork_shadow_key(self.mount, parent, &child.dentry.name),
                        version,
                        purpose,
                    )?
                    .is_some()
                {
                    return Ok(true);
                }
                if child.attr.file_type == FileType::Directory {
                    queue.push(child.attr.inode);
                    continue;
                }
                let Some(body) = &child.body else {
                    continue;
                };
                let manifests = self.chunk_manifests_for_body_at_version(
                    child.attr.inode,
                    body,
                    version,
                    purpose,
                )?;
                let block_prefix = format!("blocks/{}/", self.mount.get());
                if chunk_object_keys(&manifests).iter().any(|object_key| {
                    object_key.starts_with(&block_prefix)
                        && !self.owns_block_object_key(
                            child.attr.inode,
                            body.generation,
                            object_key,
                        )
                }) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Collect every object key referenced by the materialized subtree's file
    /// bodies. These are the blocks the restored tree shares with the snapshot; the
    /// teardown must not reclaim them and the swap must cancel any pending GC for
    /// them.
    fn subtree_object_keys(&self, root: InodeId) -> Result<HashSet<String>, MetadError> {
        let version = self.read_version()?;
        let mut keys = HashSet::new();
        let mut queue = vec![root];
        while let Some(dir) = queue.pop() {
            for child in
                self.read_dir_plus_at_version_for_purpose(dir, version, ReadPurpose::Snapshot)?
            {
                match child.attr.file_type {
                    FileType::Directory => queue.push(child.attr.inode),
                    _ => {
                        if let Some(body) = &child.body {
                            let manifests = self.chunk_manifests_for_body_at_version(
                                child.attr.inode,
                                body,
                                version,
                                ReadPurpose::Snapshot,
                            )?;
                            keys.extend(chunk_object_keys(&manifests));
                        }
                    }
                }
            }
        }
        Ok(keys)
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
    /// and cancels pending GC for the blocks the restored tree resurrects.
    fn commit_rollback_swap(
        &self,
        target_root: InodeId,
        restored_root: InodeId,
        old_children: &[OldChild],
        restored_children: &[DentryWithAttr],
        restored_keys: &HashSet<String>,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let read_version = predecessor(version)?;

        let restored_names: HashSet<&[u8]> = restored_children
            .iter()
            .map(|child| child.dentry.name.as_bytes())
            .collect();

        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, target_root),
            predicate: Predicate::Exists,
        }];
        let mut mutations = Vec::new();

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

        // Resurrect the snapshot's blocks the restored tree now references by
        // cancelling any pending GC record an earlier rewrite/delete left for them.
        mutations.extend(self.pending_gc_cancellations_for_keys(restored_keys, read_version)?);

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

    /// Delete mutations for every pending object-GC record whose block the restored
    /// tree references, so those blocks stop being scheduled for deletion.
    fn pending_gc_cancellations_for_keys(
        &self,
        restored_keys: &HashSet<String>,
        version: Version,
    ) -> Result<Vec<Mutation>, MetadError> {
        if restored_keys.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(self.mount),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut mutations = Vec::new();
        for row in rows {
            let record = decode_object_gc_record(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if restored_keys.contains(&record.object_key) {
                mutations.push(delete_mutation(RecordFamily::Gc, row.key));
            }
        }
        Ok(mutations)
    }

    /// Tear down the detached pre-rollback subtree rooted at the captured top-level
    /// children. Each node's metadata is deleted bottom-up and its inode-owned blocks
    /// are enqueued for GC, except blocks the restored tree still references
    /// (`retained_object_keys`), which stay live.
    fn purge_detached_subtree(
        &self,
        old_children: &[OldChild],
        retained_object_keys: &HashSet<String>,
    ) -> Result<(), MetadError> {
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
        for node in nodes {
            self.purge_detached_node(&node, retained_object_keys)?;
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
