use super::*;
use std::collections::HashMap;

const NAMESPACE_PROOF_PAGE_ROWS: usize = 256;

struct PreparedCreateBatch {
    entries: Vec<DentryWithAttr>,
    command: MetadataCommand,
}

struct PreparedRemoveFile {
    entry: DentryWithAttr,
    command: MetadataCommand,
}

struct PreparedRemoveEmptyDir {
    entry: DentryWithAttr,
    command: MetadataCommand,
    starts_restore_release: bool,
}

#[derive(Default)]
struct FinalBodyCleanupPlan {
    predicates: Vec<PredicateRef>,
    mutations: Vec<Mutation>,
}

#[derive(Clone, Debug)]
pub(super) struct LinkedDentryProjection {
    pub(super) key: Vec<u8>,
    pub(super) projection: DentryProjection,
    pub(super) version: Version,
}

#[derive(Default)]
struct NamespaceReachability {
    directories: HashSet<InodeId>,
    inodes: HashSet<InodeId>,
    dentries: HashSet<Vec<u8>>,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<InodeAttr, MetadError> {
        self.ensure_object_gc_claim_record()?;
        let version = self.next_version()?;
        let root = directory_attr(InodeId::root(), mode, uid, gid, version.get());
        let command = MetadataCommand {
            request_id: request_id(b"bootstrap-root", self.mount, InodeId::root(), version),
            kind: CommandKind::CreateDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(self.mount, InodeId::root()),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, InodeId::root()),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, InodeId::root()),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&root))),
            }],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(_) | Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                // `new` starts fail-closed because callers may hand it a
                // pre-populated store. Once a root exists, one recovery proof
                // establishes the healthy fast-path or keeps slow checking on.
                self.recover_restore_staging_visibility()?;
                self.recover_materialization_orphan_state()?;
                Ok(root)
            }
            Err(err) => Err(err),
        }
    }

    pub fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = directory_attr(inode, mode, uid, gid, version.get());
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateDir, &projection, version)?;
        Ok(projection.into())
    }

    /// Graft a foreign subtree directory into this shard's namespace.
    ///
    /// `target_inode` is owned by ANOTHER shard (the subtree shard). This writes
    /// ONLY the dentry projection — a stub directory attr embedded for the
    /// foreign inode — and deliberately NO `inode_key` Inode record. Two reasons:
    ///   1. Reads need nothing more: `lookup_plus`/`read_dir_plus` decode the
    ///      attr embedded in the projection and never fetch `inode_key`, so the
    ///      parent shard can satisfy `lookup(parent, name)` and `readdir(parent)`
    ///      with just this dentry, returning the foreign inode as the child.
    ///   2. Allocator safety: an Inode record for `target_inode` would be folded
    ///      by the Inode arm of `recover_allocator_state` and could poison this
    ///      shard's allocator. We never write one, and recovery's Dentry arm is
    ///      shard-guarded so the foreign `child`/`attr.inode` here is excluded
    ///      from this shard's high-water on a fallback rebuild.
    ///
    /// We do NOT call `next_inode()`: no local inode is minted. The graft is the
    /// parent-shard half of a cross-shard mount point; the subtree dir itself is
    /// created (with a real Inode record) on the owning shard.
    pub fn create_graft(
        &self,
        parent: InodeId,
        name: DentryName,
        target_inode: InodeId,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        // A graft MUST point at a foreign (child-shard) inode. A same-shard
        // "graft" would write a projection-only dentry with no backing Inode
        // record in this shard — a dangling entry that `remove_graft` would then
        // be required to clean up via the cross-shard path. Refuse it here, the
        // mirror of the same-shard refusal `remove_graft`/`is_graft_child` apply.
        if target_inode.shard_index() == self.shard_index() {
            return Err(MetadError::InvalidPath(
                "create_graft target must be a foreign child-shard inode, not a same-shard inode"
                    .to_owned(),
            ));
        }
        let version = self.next_version()?;
        let attr = directory_attr(target_inode, mode, uid, gid, version.get());
        let projection = projection(parent, name, attr, None);
        let command = self.create_graft_command(&projection, version)?;
        self.commit_metadata(command)?;
        Ok(projection.into())
    }

    /// Remove the parent-shard half of a cross-shard graft: the single dentry
    /// projection under `parent` named `name`. This is the dedicated teardown
    /// path that DELIBERATELY bypasses the `prepare_remove_empty_dir` graft guard
    /// (which exists to stop a *blind* rmdir from orphaning the child subtree).
    ///
    /// Safety rails that keep this from becoming a generic delete escape hatch:
    ///   - The target MUST be a graft (foreign child); a same-shard dentry is
    ///     rejected with `NotDirectory`/`GraftPoint`-free `MetadError` so this can
    ///     never delete a real local dir+inode (which would leak the inode and
    ///     skip the PrefixEmpty check). A normal dir goes through `remove_empty_dir`.
    ///   - Only the dentry projection is deleted — there is, by construction, no
    ///     local Inode record for the foreign child, so there is nothing else to
    ///     remove on this shard. The child subtree itself is reaped on its owning
    ///     shard by the caller (`unregister_graft`).
    ///
    /// Idempotent: returns `Ok(None)` when the dentry is already absent.
    pub fn remove_graft(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let Some((entry, dentry_version)) = self.lookup_plus_for_write_plan(parent, name)? else {
            return Ok(None);
        };
        if entry.attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        // Refuse to delete a same-shard child through this projection-only path:
        // that would leak its Inode record and skip emptiness checking. Such a
        // dentry is a normal directory and must use `remove_empty_dir`.
        if !self.is_graft_child(&entry) {
            return Err(MetadError::InvalidPath(
                "remove_graft target is not a cross-shard graft point".to_owned(),
            ));
        }
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let key = dentry_key(self.mount, parent, name);
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: key.clone(),
            predicate: Predicate::VersionEquals(dentry_version),
        }];
        predicates.extend(self.restore_namespace_write_predicates(&[parent], read_version)?);
        let commit = self.commit_metadata(MetadataCommand {
            request_id: request_id(b"remove-graft", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveEmptyDir,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: key.clone(),
            // Only the dentry version is guarded: there is no Inode record and no
            // local subtree to assert empty (the child's contents live on the
            // owning shard).
            predicates,
            mutations: vec![delete_mutation(RecordFamily::Dentry, key)],
            watch: self
                .watch_projection(
                    parent,
                    WatchEvent {
                        kind: WatchEventKind::Remove,
                        parent: Some(parent),
                        name: Some(name.clone()),
                        inode: entry.attr.inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        });
        match commit {
            Ok(_) => Ok(Some(entry)),
            // Idempotency under concurrent teardown: a racing remover (or a
            // re-driven retry of this same call) can delete the dentry between
            // our read and this commit, so the version predicate fails. If the
            // dentry is genuinely gone now, the desired post-state already holds,
            // so report success rather than surfacing a spurious conflict.
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                if self.lookup_plus_for_write_plan(parent, name)?.is_none() {
                    Ok(None)
                } else {
                    Err(MetadError::Metadata(MetadataError::PredicateFailed))
                }
            }
            Err(err) => Err(err),
        }
    }

    pub fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: 0,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateFile, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_file_prepared(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<CreatedPreparedArtifact, MetadError> {
        let entry = self.create_file(parent, name.clone(), mode, uid, gid)?;
        let prepared = self.prepare_artifact_replace(parent, name)?;
        Ok(CreatedPreparedArtifact { entry, prepared })
    }

    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        if target.is_empty() || target.contains(&0) {
            return Err(MetadError::InvalidPath(
                "symlink target must be non-empty and must not contain NUL".to_owned(),
            ));
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let digest_uri = body_digest_uri(&target);
        let request = PublishArtifact {
            parent,
            name: name.clone(),
            producer: "nokv-symlink".to_owned(),
            digest_uri,
            content_type: "text/plain; charset=utf-8".to_owned(),
            manifest_id: format!("symlink/{}/{}", parent.get(), inode.get()),
            bytes: target,
            mode,
            uid,
            gid,
        };
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::Symlink,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::Symlink.initial_link_count(),
            size: body.size,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name, attr, Some(body));
        if let Err(err) = self.commit_create_projection_with_chunks(
            CommandKind::CreateSymlink,
            &projection,
            &chunks,
            version,
            object_reference,
        ) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(err),
                staged,
            });
        }
        Ok(projection.into())
    }

    pub fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> Result<DentryWithAttr, MetadError> {
        if !spec.file_type.is_special_node() {
            return Err(MetadError::InvalidPath(format!(
                "file type {:?} is not a special node",
                spec.file_type
            )));
        }
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: spec.file_type,
            mode: spec.mode,
            uid: spec.uid,
            gid: spec.gid,
            rdev: spec.rdev,
            nlink: spec.file_type.initial_link_count(),
            size: 0,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateSpecialNode, &projection, version)?;
        Ok(projection.into())
    }

    pub fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        // Fence a cross-shard hardlink before any lookup: the linked `inode` and
        // the destination directory `new_parent` must both live in this shard (see
        // `ensure_same_shard`). A hardlink across shards would name an inode from a
        // foreign namespace, which this shard cannot own or GC.
        self.ensure_same_shard(inode, new_parent)?;
        // A hardlink can turn a previously unreachable borrowed body into a live
        // object reference. Serialize its reachability proof + commit with fork
        // retirement and local object GC, and join the exact durable Open claim
        // so a remote/failover GC transition cannot race the exposure.
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let source_inode_key = inode_key(self.mount, inode);
        let Some(inode_item) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &source_inode_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        let mut attr = decode_inode_attr(&inode_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        if attr.file_type == FileType::Directory {
            return Err(MetadError::NotFile);
        }
        let Some(parent_attr) = self.get_attr_at_version_for_purpose(
            new_parent,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        if parent_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        let reachable = self.namespace_reachability_for_exposure(read_version)?;
        if reachable.as_ref().is_some_and(|reachable| {
            !reachable.directories.contains(&new_parent) || !reachable.inodes.contains(&inode)
        }) {
            // Inode-addressed APIs must not provide an escape hatch for partial
            // clone/rollback materializations which have no durable binding.
            return Err(MetadError::NotFound);
        }
        let linked = self.linked_dentry_projections_for_inode(inode, read_version)?;
        let first_link = match &reachable {
            Some(reachable) => linked
                .iter()
                .find(|linked| reachable.dentries.contains(&linked.key)),
            None => linked.first(),
        };
        let Some(first_link) = first_link else {
            return Err(MetadError::NotFound);
        };
        attr.nlink = attr
            .nlink
            .checked_add(1)
            .ok_or_else(|| MetadError::InvalidPath("inode link count overflow".to_owned()))?;
        attr.generation = version.get();
        attr.ctime_ms = current_time_ms();
        let new_projection = projection(
            new_parent,
            new_name,
            attr.clone(),
            first_link.projection.body.clone(),
        );
        let destination_key = dentry_key(self.mount, new_parent, &new_projection.dentry.name);

        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: source_inode_key.clone(),
                predicate: Predicate::VersionEquals(inode_item.version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, new_parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::NotExists,
            },
            object_reference.predicate(self.mount),
        ];
        let restore_write_predicates =
            self.restore_namespace_write_predicates(&[inode, new_parent], read_version)?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        let enrollment = self.restore_namespace_enrollment_plan(
            new_parent,
            std::slice::from_ref(&new_projection),
            read_version,
        )?;
        predicates.extend(enrollment.predicates);
        let mut mutations = vec![Mutation {
            family: RecordFamily::Inode,
            key: source_inode_key,
            op: MutationOp::Put,
            value: Some(Value(encode_inode_attr(&attr))),
        }];
        let mut updated_existing_links = Vec::with_capacity(linked.len());
        for linked in &linked {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: linked.key.clone(),
                predicate: Predicate::VersionEquals(linked.version),
            });
            let mut projection = linked.projection.clone();
            projection.attr = attr.clone();
            projection.dentry.attr_generation = attr.generation;
            updated_existing_links.push((linked.projection.clone(), projection.clone()));
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                linked.key.clone(),
                &projection,
            ));
        }
        mutations.push(put_projection_mutation(
            RecordFamily::Dentry,
            destination_key.clone(),
            &new_projection,
        ));
        mutations.extend(enrollment.mutations);
        let restore_index = self.restore_index_link_plan(&updated_existing_links, version)?;
        predicates.extend(restore_index.predicates);
        mutations.extend(restore_index.mutations);
        let command = MetadataCommand {
            request_id: request_id(b"link", self.mount, inode, version),
            kind: CommandKind::Link,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: destination_key,
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    new_parent,
                    WatchEvent {
                        kind: WatchEventKind::Create,
                        parent: Some(new_parent),
                        name: Some(new_projection.dentry.name.clone()),
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(&command, "restore namespace link")?;
        }
        self.commit_metadata(command)?;
        Ok(new_projection.into())
    }

    pub fn update_attrs(
        &self,
        parent: InodeId,
        name: &DentryName,
        changes: UpdateAttr,
    ) -> Result<DentryWithAttr, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if changes.is_empty() {
            return Ok(entry);
        }
        let object_reference = changes
            .size
            .map(|_| self.begin_object_reference_mutation())
            .transpose()?;
        let version = self.next_version()?;
        let mut attr = entry.attr.clone();
        if let Some(mode) = changes.mode {
            attr.mode = mode;
        }
        if let Some(uid) = changes.uid {
            attr.uid = uid;
        }
        if let Some(gid) = changes.gid {
            attr.gid = gid;
        }
        if let Some(mtime_ms) = changes.mtime_ms {
            attr.mtime_ms = mtime_ms;
        }
        if changes.size.is_some() && changes.mtime_ms.is_none() {
            attr.mtime_ms = current_time_ms();
        }
        attr.ctime_ms = changes.ctime_ms.unwrap_or_else(current_time_ms);
        // `attr.generation` is the file's content generation and the key under
        // which the body summary / chunk manifests are stored (reads resolve the
        // body via `attr.generation`). Only advance it when the body actually
        // changes (a size change re-stages the body below). An attribute-only
        // update (chmod/chown/utimes) must leave it equal to the existing
        // `body.generation`; bumping it would point the dentry at a generation
        // that has no body summary, surfacing as MissingBodyDescriptor on read.
        if changes.size.is_some() {
            attr.generation = version.get();
        }

        let mut body = entry.body.clone();
        let mut chunks = Vec::new();
        let mut old_chunks = Vec::new();
        let mut old_generation = None;
        if let Some(size) = changes.size {
            if attr.file_type == FileType::Directory {
                return Err(MetadError::NotFile);
            }
            let read_version = self.read_version()?;
            old_generation = body.as_ref().map(|body| body.generation);
            let digest_uri =
                self.resized_body_digest_uri(entry.attr.inode, body.as_ref(), size, read_version)?;
            old_chunks = body
                .as_ref()
                .map(|body| {
                    self.chunk_manifests_for_body_at_version(
                        entry.attr.inode,
                        body,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )
                })
                .transpose()?
                .unwrap_or_default();
            chunks = merge_session_chunks(size, old_chunks.clone(), Vec::new())?;
            body = Some(BodyDescriptor {
                producer: body
                    .as_ref()
                    .map(|body| body.producer.clone())
                    .unwrap_or_else(|| "nokv-metadata".to_owned()),
                digest_uri,
                size,
                content_type: body
                    .as_ref()
                    .map(|body| body.content_type.clone())
                    .unwrap_or_else(|| "application/octet-stream".to_owned()),
                manifest_id: body
                    .as_ref()
                    .map(|body| body.manifest_id.clone())
                    .unwrap_or_else(|| format!("metadata/{}/{}", parent.get(), attr.inode.get())),
                generation: version.get(),
                // Self-contained: merge_session_chunks re-materializes every chunk.
                base_generation: 0,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
            });
            attr.size = size;
        }

        let projection = projection(parent, name.clone(), attr, body);
        self.commit_replace_projection_with_chunks(ReplaceProjectionCommit {
            request_id: None,
            kind: CommandKind::UpdateAttr,
            projection: &projection,
            chunks: &chunks,
            old_chunks: &old_chunks,
            dentry_version,
            old_generation,
            version,
            path_index: None,
            object_reference,
        })?;
        Ok(projection.into())
    }

    pub fn update_root_attrs(&self, changes: UpdateAttr) -> Result<InodeAttr, MetadError> {
        let key = inode_key(self.mount, InodeId::root());
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        let mut attr =
            decode_inode_attr(&item.value.0).map_err(|err| MetadError::Codec(err.to_string()))?;
        if changes.is_empty() {
            return Ok(attr);
        }
        if changes.size.is_some() {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        if let Some(mode) = changes.mode {
            attr.mode = mode;
        }
        if let Some(uid) = changes.uid {
            attr.uid = uid;
        }
        if let Some(gid) = changes.gid {
            attr.gid = gid;
        }
        if let Some(mtime_ms) = changes.mtime_ms {
            attr.mtime_ms = mtime_ms;
        }
        attr.ctime_ms = changes.ctime_ms.unwrap_or_else(current_time_ms);
        attr.generation = version.get();

        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"update-root-attr", self.mount, InodeId::root(), version),
            kind: CommandKind::UpdateAttr,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: key.clone(),
                predicate: Predicate::VersionEquals(item.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&attr))),
            }],
            watch: Vec::new(),
        })?;
        Ok(attr)
    }

    pub fn create_dir_path(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = directory_attr(inode, mode, uid, gid, version.get());
        let projection = projection(parent, name.clone(), attr, None);
        self.commit_create_projection(CommandKind::CreateDir, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_file_path(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: 0,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name.clone(), attr, None);
        self.commit_create_projection(CommandKind::CreateFile, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_files_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        self.create_files_in_dir_with_parent_components(parent, names, mode, uid, gid)
    }

    pub fn create_dirs_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(names.len())?;
        let projections = names
            .into_iter()
            .zip(inodes)
            .map(|(name, inode)| {
                projection(
                    parent,
                    name,
                    directory_attr(inode, mode, uid, gid, version.get()),
                    None,
                )
            })
            .collect::<Vec<_>>();
        self.commit_create_projections_with_path_indexes(
            CommandKind::CreateDir,
            &projections,
            version,
            None,
        )?;
        self.record_create_dirs_batch(projections.len());
        Ok(projections.into_iter().map(Into::into).collect())
    }

    pub fn create_file_batches_in_dir_path(
        &self,
        batches: Vec<CreateInDirPathBatch>,
    ) -> Vec<Result<Vec<DentryWithAttr>, MetadError>> {
        self.create_batches_in_dir_path(CommandKind::CreateFiles, batches)
    }

    pub fn create_dir_batches_in_dir_path(
        &self,
        batches: Vec<CreateInDirPathBatch>,
    ) -> Vec<Result<Vec<DentryWithAttr>, MetadError>> {
        self.create_batches_in_dir_path(CommandKind::CreateDir, batches)
    }

    fn create_batches_in_dir_path(
        &self,
        kind: CommandKind,
        batches: Vec<CreateInDirPathBatch>,
    ) -> Vec<Result<Vec<DentryWithAttr>, MetadError>> {
        let mut results = Vec::with_capacity(batches.len());
        results.resize_with(batches.len(), || None);
        let mut prepared = Vec::new();
        for (index, batch) in batches.into_iter().enumerate() {
            if batch.names.is_empty() {
                results[index] = Some(Ok(Vec::new()));
                continue;
            }
            match self.prepare_create_batch_in_dir_path(kind, batch) {
                Ok(batch) => prepared.push((index, batch)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        let commands = prepared
            .iter()
            .map(|(_, batch)| batch.command.clone())
            .collect::<Vec<_>>();
        let committed = self.commit_independent_metadata_batch(&commands);
        for ((index, batch), result) in prepared.into_iter().zip(committed) {
            match result {
                Ok(_) => {
                    self.record_create_batch(kind, batch.entries.len());
                    results[index] = Some(Ok(batch.entries));
                }
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(
                        MetadataError::Backend("batched create result was not recorded".to_owned())
                            .into(),
                    )
                })
            })
            .collect()
    }

    fn prepare_create_batch_in_dir_path(
        &self,
        kind: CommandKind,
        batch: CreateInDirPathBatch,
    ) -> Result<PreparedCreateBatch, MetadError> {
        ensure_unique_names(&batch.names)?;
        let parent_components = parse_absolute_path(&batch.parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(batch.names.len())?;
        let now_ms = current_time_ms();
        let projections = batch
            .names
            .into_iter()
            .zip(inodes)
            .map(|(name, inode)| {
                let attr = match kind {
                    CommandKind::CreateDir => {
                        directory_attr(inode, batch.mode, batch.uid, batch.gid, version.get())
                    }
                    CommandKind::CreateFiles => InodeAttr {
                        inode,
                        file_type: FileType::File,
                        mode: batch.mode,
                        uid: batch.uid,
                        gid: batch.gid,
                        rdev: 0,
                        nlink: FileType::File.initial_link_count(),
                        size: 0,
                        generation: version.get(),
                        mtime_ms: now_ms,
                        ctime_ms: now_ms,
                    },
                    _ => unreachable!("create batch only supports files and directories"),
                };
                projection(parent, name, attr, None)
            })
            .collect::<Vec<_>>();
        let command = self.create_projections_command(kind, &projections, version, None)?;
        Ok(PreparedCreateBatch {
            entries: projections.into_iter().map(Into::into).collect(),
            command,
        })
    }

    fn record_create_batch(&self, kind: CommandKind, entries: usize) {
        match kind {
            CommandKind::CreateDir => self.record_create_dirs_batch(entries),
            CommandKind::CreateFiles => self.record_create_files_batch(entries),
            _ => {}
        }
    }

    pub fn create_files_in_dir(
        &self,
        parent: InodeId,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        self.create_files_in_dir_with_parent_components(parent, names, mode, uid, gid)
    }

    fn create_files_in_dir_with_parent_components(
        &self,
        parent: InodeId,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(names.len())?;
        let now_ms = current_time_ms();
        let projections = names
            .into_iter()
            .zip(inodes)
            .map(|(name, inode)| {
                let attr = InodeAttr {
                    inode,
                    file_type: FileType::File,
                    mode,
                    uid,
                    gid,
                    rdev: 0,
                    nlink: FileType::File.initial_link_count(),
                    size: 0,
                    generation: version.get(),
                    mtime_ms: now_ms,
                    ctime_ms: now_ms,
                };
                projection(parent, name, attr, None)
            })
            .collect::<Vec<_>>();
        self.commit_create_projections_with_path_indexes(
            CommandKind::CreateFiles,
            &projections,
            version,
            None,
        )?;
        self.record_create_files_batch(projections.len());
        Ok(projections.into_iter().map(Into::into).collect())
    }

    fn record_create_files_batch(&self, entries: usize) {
        if entries <= 1 {
            return;
        }
        self.create_files_batch_total
            .fetch_add(1, Ordering::Relaxed);
        self.create_files_entry_total
            .fetch_add(entries as u64, Ordering::Relaxed);
    }

    fn record_create_dirs_batch(&self, entries: usize) {
        if entries <= 1 {
            return;
        }
        self.create_dirs_batch_total.fetch_add(1, Ordering::Relaxed);
        self.create_dirs_entry_total
            .fetch_add(entries as u64, Ordering::Relaxed);
    }

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.remove_file_inner(parent, name, None)
    }

    fn remove_file_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let prepared = self.prepare_remove_file(parent, name, path_components, version)?;
        self.commit_metadata(prepared.command)?;
        Ok(prepared.entry)
    }

    fn prepare_remove_file(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
        version: Version,
    ) -> Result<PreparedRemoveFile, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        // A graft child is a foreign-shard directory; `unlink` on it would also
        // hit the directory check below, but report the actionable graft-point
        // error first (and guard the path explicitly) so a misrouted unlink can
        // never delete the parent's dentry and strand the child subtree.
        if self.is_graft_child(&entry) {
            return Err(MetadError::GraftPoint);
        }
        if entry.attr.file_type == FileType::Directory {
            return Err(MetadError::NotFile);
        }
        let inode_key = inode_key(self.mount, entry.attr.inode);
        let Some(inode_item) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &inode_key,
            predecessor(version)?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        let mut canonical_attr = decode_inode_attr(&inode_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        let key = dentry_key(self.mount, parent, name);
        let mut predicates = Vec::new();
        let mut mutations = vec![delete_mutation(RecordFamily::Dentry, key.clone())];
        if let Some(path_index) =
            self.live_path_index_key_for_entry(path_components, parent, name, &entry, version)?
        {
            mutations.push(delete_mutation(RecordFamily::PathIndex, path_index));
        }
        if canonical_attr.nlink == 0 {
            return Err(MetadError::InvalidPath(
                "inode link count is already zero".to_owned(),
            ));
        }
        let final_link = canonical_attr.nlink == 1;
        let (linked, updated_remaining) = if final_link {
            mutations.push(delete_mutation(RecordFamily::Inode, inode_key.clone()));
            (Vec::new(), Vec::new())
        } else {
            let linked =
                self.linked_dentry_projections_for_inode(entry.attr.inode, predecessor(version)?)?;
            canonical_attr.nlink -= 1;
            canonical_attr.generation = version.get();
            canonical_attr.ctime_ms = current_time_ms();
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&canonical_attr))),
            });
            let mut updated_remaining = Vec::with_capacity(linked.len().saturating_sub(1));
            for linked in &linked {
                if linked.key == key {
                    continue;
                }
                let mut projection = linked.projection.clone();
                projection.attr = canonical_attr.clone();
                projection.dentry.attr_generation = canonical_attr.generation;
                updated_remaining.push((linked.projection.clone(), projection.clone()));
                mutations.push(put_projection_mutation(
                    RecordFamily::Dentry,
                    linked.key.clone(),
                    &projection,
                ));
            }
            (linked, updated_remaining)
        };
        if final_link {
            if let Some(body) = &entry.body {
                let cleanup = self.final_body_cleanup_plan(
                    entry.attr.inode,
                    body,
                    predecessor(version)?,
                    version,
                )?;
                predicates.extend(cleanup.predicates);
                mutations.extend(cleanup.mutations);
            }
        }
        predicates.extend([
            PredicateRef {
                family: RecordFamily::Dentry,
                key: key.clone(),
                predicate: Predicate::VersionEquals(dentry_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key,
                predicate: Predicate::VersionEquals(inode_item.version),
            },
        ]);
        let restore_write_predicates = self.restore_namespace_write_predicates(
            &[parent, entry.attr.inode],
            predecessor(version)?,
        )?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        for linked in linked {
            if linked.key == dentry_key(self.mount, parent, name) {
                continue;
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: linked.key,
                predicate: Predicate::VersionEquals(linked.version),
            });
        }
        let removed_projection = DentryProjection {
            dentry: entry.dentry.clone(),
            attr: entry.attr.clone(),
            body: entry.body.clone(),
        };
        let restore_index =
            self.restore_index_remove_plan(&removed_projection, &updated_remaining, version)?;
        predicates.extend(restore_index.predicates);
        mutations.extend(restore_index.mutations);
        let command = MetadataCommand {
            request_id: request_id(b"remove-file", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveFile,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: key.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    parent,
                    WatchEvent {
                        kind: WatchEventKind::Remove,
                        parent: Some(parent),
                        name: Some(name.clone()),
                        inode: entry.attr.inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(
                &command,
                "restore namespace remove file",
            )?;
        }
        Ok(PreparedRemoveFile { entry, command })
    }

    /// Build the atomic metadata closure for deleting the last namespace link
    /// to a file body. An append or sparse publish stores only its dirty chunks
    /// in the top generation and falls through `base_generation`; deleting the
    /// top rows alone strands the older summaries/manifests and leaves their
    /// objects without a fresh GC candidate.
    ///
    /// Every physical row in the reachable generation chain is version-fenced
    /// and deleted by the caller's namespace transaction. Every canonical block
    /// minted by this inode is enqueued using the generation encoded in its
    /// object key. That distinction matters after chain compaction: a new
    /// self-contained manifest can retain an unchanged block from an older
    /// generation whose physical manifest row was already deleted. Cloned
    /// manifests contain keys owned by another inode; those borrowed keys stay
    /// protected by their source plus snapshot/exact-reference policy.
    fn final_body_cleanup_plan(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        read_version: Version,
        enqueue_version: Version,
    ) -> Result<FinalBodyCleanupPlan, MetadError> {
        let generations =
            self.resolve_generation_chain(inode, body, read_version, ReadPurpose::WritePlanLocal)?;
        let enqueue_unix_ms = current_time_ms();
        let mut queued_objects: HashMap<String, (u64, String)> = HashMap::new();
        let mut plan = FinalBodyCleanupPlan::default();

        for generation in generations {
            let prefix = chunk_manifest_prefix(self.mount, inode, generation);
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::ChunkManifest,
                prefix: prefix.clone(),
                start_after: None,
                version: read_version,
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            let mut saw_summary = false;

            for row in rows {
                if row.key.len() != prefix.len() + std::mem::size_of::<u64>()
                    || !row.key.starts_with(&prefix)
                {
                    return Err(MetadError::Codec(
                        "chunk manifest scan returned a key outside its generation prefix"
                            .to_owned(),
                    ));
                }
                let chunk_index = chunk_index_from_manifest_key(&row.key)?;
                plan.predicates.push(PredicateRef {
                    family: RecordFamily::ChunkManifest,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                });
                plan.mutations
                    .push(delete_mutation(RecordFamily::ChunkManifest, row.key));

                if chunk_index == BODY_SUMMARY_CHUNK_INDEX {
                    let summary = decode_body_descriptor(&row.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?;
                    if summary.generation != generation {
                        return Err(MetadError::Codec(
                            "body summary generation does not match its manifest key".to_owned(),
                        ));
                    }
                    saw_summary = true;
                    continue;
                }

                let manifest = decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                if manifest.chunk_index != chunk_index {
                    return Err(MetadError::Codec(
                        "chunk manifest index does not match its manifest key".to_owned(),
                    ));
                }
                for block in manifest.slices.iter().flat_map(|slice| &slice.blocks) {
                    if !self.block_object_is_owned_by_inode(inode, &block.object_key)? {
                        continue;
                    }
                    let (owner, object_generation, object_chunk, block_index) =
                        self.canonical_block_object_identity(&block.object_key)?;
                    if owner != inode || object_chunk != chunk_index {
                        return Err(MetadError::Codec(
                            "owned block object identity does not match its manifest".to_owned(),
                        ));
                    }

                    match queued_objects.get(&block.object_key) {
                        Some((_, digest_uri)) if digest_uri != &block.digest_uri => {
                            return Err(MetadError::Codec(
                                "one block object key has inconsistent manifest identity"
                                    .to_owned(),
                            ));
                        }
                        Some(_) => continue,
                        None => {
                            queued_objects.insert(
                                block.object_key.clone(),
                                (block.len, block.digest_uri.clone()),
                            );
                        }
                    }
                    let record = ObjectGcRecord {
                        inode: owner,
                        generation: object_generation,
                        object_key: block.object_key.clone(),
                        size: block.len,
                        digest_uri: block.digest_uri.clone(),
                        enqueue_version: enqueue_version.get(),
                        enqueue_unix_ms,
                    };
                    plan.mutations.push(Mutation {
                        family: RecordFamily::Gc,
                        key: gc_object_key(
                            self.mount,
                            enqueue_version.get(),
                            owner,
                            object_generation,
                            object_chunk,
                            block_index,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_object_gc_record(&record))),
                    });
                }
            }

            if !saw_summary {
                return Err(MetadError::MissingBodyDescriptor);
            }
        }

        Ok(plan)
    }

    pub fn remove_file_path(&self, path: &str) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        self.remove_file_inner(parent, name, Some(&components))
    }

    pub fn remove_files_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
    ) -> Result<Vec<Result<DentryWithAttr, MetadError>>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        let mut results = Vec::with_capacity(names.len());
        results.resize_with(names.len(), || None);
        let mut prepared = Vec::new();
        for (index, name) in names.into_iter().enumerate() {
            let version = self.next_version()?;
            let mut path_components = parent_components.clone();
            path_components.push(name.clone());
            match self.prepare_remove_file(parent, &name, Some(&path_components), version) {
                Ok(remove) => prepared.push((index, remove)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        let commands = prepared
            .iter()
            .map(|(_, remove)| remove.command.clone())
            .collect::<Vec<_>>();
        let committed = self.commit_independent_metadata_batch(&commands);
        for ((index, remove), result) in prepared.into_iter().zip(committed) {
            results[index] = Some(result.map(|_| remove.entry));
        }

        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(
                        MetadataError::Backend("batched remove result was not recorded".to_owned())
                            .into(),
                    )
                })
            })
            .collect())
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.remove_empty_dir_inner(parent, name, None)
    }

    fn remove_empty_dir_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let prepared = self.prepare_remove_empty_dir(parent, name, path_components, version)?;
        map_remove_empty_dir_commit(
            self.commit_restore_release_transition(
                prepared.command,
                prepared.starts_restore_release,
            ),
        )?;
        Ok(prepared.entry)
    }

    fn prepare_remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
        version: Version,
    ) -> Result<PreparedRemoveEmptyDir, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if entry.attr.inode == InodeId::root() {
            return Err(MetadError::CannotRemoveRoot);
        }
        // A graft point's child lives on another shard. `PrefixEmpty` below scans
        // only THIS shard's dentry subspace, which is always empty for a foreign
        // child, so a plain rmdir would succeed and orphan the whole child
        // subtree. Reject; removal goes through the graft lifecycle.
        if self.is_graft_child(&entry) {
            return Err(MetadError::GraftPoint);
        }
        let source_key = dentry_key(self.mount, parent, name);
        let child_prefix = dentry_prefix(self.mount, entry.attr.inode);
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, source_key.clone()),
            delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
        ];
        if let Some(path_index) =
            self.live_path_index_key_for_entry(path_components, parent, name, &entry, version)?
        {
            mutations.push(delete_mutation(RecordFamily::PathIndex, path_index));
        }
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Dentry,
                key: source_key.clone(),
                predicate: Predicate::VersionEquals(dentry_version),
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: child_prefix,
                predicate: Predicate::PrefixEmpty,
            },
        ];
        let restore_write_predicates = self.restore_namespace_write_predicates(
            &[parent, entry.attr.inode],
            predecessor(version)?,
        )?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        let restore_release = self.prepare_restore_root_release(entry.attr.inode, version)?;
        if let Some(release) = restore_release.as_ref() {
            predicates.extend(release.predicates.clone());
            mutations.extend(release.mutations.clone());
        }
        let restore_index = self.restore_index_unlink_plan(
            &DentryProjection {
                dentry: entry.dentry.clone(),
                attr: entry.attr.clone(),
                body: entry.body.clone(),
            },
            version,
        )?;
        predicates.extend(restore_index.predicates);
        mutations.extend(restore_index.mutations);
        let command = MetadataCommand {
            request_id: request_id(b"remove-empty-dir", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveEmptyDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: source_key.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    parent,
                    WatchEvent {
                        kind: WatchEventKind::Remove,
                        parent: Some(parent),
                        name: Some(name.clone()),
                        inode: entry.attr.inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(
                &command,
                "restore namespace remove directory",
            )?;
        }
        Ok(PreparedRemoveEmptyDir {
            entry,
            command,
            starts_restore_release: restore_release.is_some(),
        })
    }

    pub fn remove_empty_dir_path(&self, path: &str) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        self.remove_empty_dir_inner(parent, name, Some(&components))
    }

    pub fn remove_empty_dirs_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
    ) -> Result<Vec<Result<DentryWithAttr, MetadError>>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        let mut results = Vec::with_capacity(names.len());
        results.resize_with(names.len(), || None);
        let mut prepared = Vec::new();
        for (index, name) in names.into_iter().enumerate() {
            let version = self.next_version()?;
            let mut path_components = parent_components.clone();
            path_components.push(name.clone());
            match self.prepare_remove_empty_dir(parent, &name, Some(&path_components), version) {
                Ok(remove) => prepared.push((index, remove)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        let committed = if prepared
            .iter()
            .any(|(_, remove)| remove.starts_restore_release)
        {
            prepared
                .iter()
                .map(|(_, remove)| {
                    self.commit_restore_release_transition(
                        remove.command.clone(),
                        remove.starts_restore_release,
                    )
                })
                .collect()
        } else {
            let commands = prepared
                .iter()
                .map(|(_, remove)| remove.command.clone())
                .collect::<Vec<_>>();
            self.commit_independent_metadata_batch(&commands)
        };
        for ((index, remove), result) in prepared.into_iter().zip(committed) {
            results[index] = Some(map_remove_empty_dir_commit(result).map(|_| remove.entry));
        }

        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(
                        MetadataError::Backend("batched rmdir result was not recorded".to_owned())
                            .into(),
                    )
                })
            })
            .collect())
    }

    pub fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, false, None)
            .map(|outcome| outcome.entry)
    }

    pub fn rename_path(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<DentryWithAttr, MetadError> {
        let source_components = parse_absolute_path(source)?;
        let destination_components = parse_absolute_path(destination)?;
        let Some((name, parent_components)) = source_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let Some((new_name, new_parent_components)) = destination_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let new_parent = self.resolve_components_as_directory(new_parent_components)?;
        self.rename_inner(
            parent,
            name,
            new_parent,
            new_name.clone(),
            false,
            Some((&source_components, &destination_components)),
        )
        .map(|outcome| outcome.entry)
    }

    pub fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, true, None)
    }

    pub fn rename_replace_path(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, MetadError> {
        let source_components = parse_absolute_path(source)?;
        let destination_components = parse_absolute_path(destination)?;
        let Some((name, parent_components)) = source_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let Some((new_name, new_parent_components)) = destination_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let new_parent = self.resolve_components_as_directory(new_parent_components)?;
        self.rename_inner(
            parent,
            name,
            new_parent,
            new_name.clone(),
            true,
            Some((&source_components, &destination_components)),
        )
    }

    /// Authoritative cross-shard fence for inode-addressed dual-endpoint ops
    /// (rename, hardlink). Both directory inodes must live in *this* service's
    /// shard: an inode carries its owning shard in its high bits, so a `src` and
    /// `dst` in different shards (or addressed to the wrong service by a
    /// misrouted/buggy client) can never be one in-shard commit. Reject before any
    /// lookup or mutation so the op fails as a clean `EXDEV` instead of resolving
    /// the foreign endpoint as `NotFound` (or, worse, a partial cross-DB write).
    ///
    /// Within a single shard every inode carries that shard's index, so this is a
    /// no-op for legitimate same-shard ops.
    fn ensure_same_shard(&self, src: InodeId, dst: InodeId) -> Result<(), MetadError> {
        let here = self.shard_index();
        if src.shard_index() != here || dst.shard_index() != here {
            return Err(MetadError::CrossShard {
                source_shard: src.shard_index(),
                dest_shard: dst.shard_index(),
            });
        }
        Ok(())
    }

    /// Whether `entry` is a cross-shard graft point — a dentry in THIS shard
    /// whose child inode is minted by ANOTHER shard. Such a dentry projects a
    /// foreign subtree directory whose contents live on the owning shard, so any
    /// emptiness check or content move performed here is blind to them. Callers
    /// that delete or relink a dentry MUST reject these (see
    /// [`MetadError::GraftPoint`]) and route through the graft lifecycle instead.
    ///
    /// For shard 0 every owned child carries shard index 0 (`compose(0, x) == x`),
    /// so this is always `false` and the single-shard paths are unchanged.
    fn is_graft_child(&self, entry: &DentryWithAttr) -> bool {
        entry.attr.inode.shard_index() != self.shard_index()
    }

    pub(super) fn rename_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
        replace: bool,
        path_index: Option<(&[DentryName], &[DentryName])>,
    ) -> Result<RenameReplaceResult, MetadError> {
        // Fence cross-shard renames before touching the namespace (see
        // `ensure_same_shard`): `parent`/`new_parent` are the source and
        // destination directory inodes.
        self.ensure_same_shard(parent, new_parent)?;
        // Rename can expose a dentry that was left behind by a failed detached
        // materialization. Hold the same gate as fork retirement from the
        // reachability proof through the commit; the durable Open-claim CAS
        // below also fences object GC on another owner.
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let (source, source_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if parent == new_parent && *name == new_name {
            return Ok(RenameReplaceResult {
                entry: source,
                replaced: None,
            });
        }
        // Moving a graft point would rewrite the parent's dentry projection under
        // a new key (and copy the foreign attr/body), detaching it from where the
        // child shard's namespace is rooted and orphaning the subtree. A
        // self-rename (handled above) is harmless; any actual move is rejected.
        if self.is_graft_child(&source) {
            return Err(MetadError::GraftPoint);
        }
        let destination = self.lookup_plus_for_write_plan(new_parent, &new_name)?;
        if !replace && destination.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        // Overwriting a graft point as the rename DESTINATION would delete its
        // foreign child's dentry here and decrement a foreign inode this shard
        // does not own — same orphaning hazard from the other side.
        if let Some((entry, _)) = &destination {
            if self.is_graft_child(entry) {
                return Err(MetadError::GraftPoint);
            }
        }
        let replacing_complete_restore_directory = if replace
            && source.attr.file_type == FileType::Directory
            && destination
                .as_ref()
                .is_some_and(|(entry, _)| entry.attr.file_type == FileType::Directory)
        {
            self.is_complete_restore_root(
                destination
                    .as_ref()
                    .expect("checked destination")
                    .0
                    .attr
                    .inode,
            )?
        } else {
            false
        };
        let moving_complete_restore_directory = if replace
            && source.attr.file_type == FileType::Directory
            && destination
                .as_ref()
                .is_some_and(|(entry, _)| entry.attr.file_type == FileType::Directory)
        {
            self.is_complete_restore_root(source.attr.inode)?
        } else {
            false
        };
        if replace
            && (source.attr.file_type == FileType::Directory
                || destination
                    .as_ref()
                    .is_some_and(|(entry, _)| entry.attr.file_type == FileType::Directory))
            && !replacing_complete_restore_directory
            && !moving_complete_restore_directory
        {
            return Err(MetadError::NotFile);
        }

        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let restore_release = if replacing_complete_restore_directory {
            let victim = destination
                .as_ref()
                .expect("checked restore victim")
                .0
                .attr
                .inode;
            Some(
                self.prepare_restore_root_release(victim, version)?
                    .ok_or(MetadError::RestoreRootChanged { root: victim })?,
            )
        } else {
            None
        };
        let source_key = dentry_key(self.mount, parent, name);
        let destination_key = dentry_key(self.mount, new_parent, &new_name);
        let reachable = self.namespace_reachability_for_exposure(read_version)?;
        if reachable.as_ref().is_some_and(|reachable| {
            !reachable.directories.contains(&parent)
                || !reachable.directories.contains(&new_parent)
                || !reachable.dentries.contains(&source_key)
        }) {
            return Err(MetadError::NotFound);
        }
        let projection = projection(
            new_parent,
            new_name,
            source.attr.clone(),
            source.body.clone(),
        );
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, new_parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: source_key.clone(),
                predicate: Predicate::VersionEquals(source_version),
            },
            object_reference.predicate(self.mount),
        ];
        let mut guarded_inodes = vec![parent, new_parent, source.attr.inode];
        if let Some((entry, _)) = &destination {
            guarded_inodes.push(entry.attr.inode);
        }
        let restore_write_predicates =
            self.restore_namespace_write_predicates(&guarded_inodes, read_version)?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        let enrollment = self.restore_namespace_enrollment_plan(
            new_parent,
            std::slice::from_ref(&projection),
            read_version,
        )?;
        predicates.extend(enrollment.predicates);
        let replaced = if let Some((entry, destination_version)) = destination {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::VersionEquals(destination_version),
            });
            Some(entry)
        } else {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::NotExists,
            });
            None
        };

        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, source_key),
            Mutation {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(&projection))),
            },
        ];
        mutations.extend(enrollment.mutations);
        let mut replaced_link_updates = Vec::new();
        if let Some(source_path) = self.live_path_index_key_for_entry(
            path_index.map(|(source, _)| source),
            parent,
            name,
            &source,
            version,
        )? {
            let destination_path = path_index
                .map(|(_, destination)| path_index_key(self.mount, destination))
                .ok_or_else(|| {
                    MetadataError::Backend(
                        "live source path index requires destination path context".to_owned(),
                    )
                })?;
            mutations.push(delete_mutation(RecordFamily::PathIndex, source_path));
            mutations.push(put_projection_mutation(
                RecordFamily::PathIndex,
                destination_path,
                &projection,
            ));
        }
        if let Some(replaced) = &replaced {
            let replaced_inode_key = inode_key(self.mount, replaced.attr.inode);
            let Some(replaced_inode_item) = self.metadata.get_versioned(
                RecordFamily::Inode,
                &replaced_inode_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            else {
                return Err(MetadError::NotFound);
            };
            let mut replaced_attr = decode_inode_attr(&replaced_inode_item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if replaced_attr.nlink == 0 {
                return Err(MetadError::InvalidPath(
                    "replaced inode link count is already zero".to_owned(),
                ));
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Inode,
                key: replaced_inode_key.clone(),
                predicate: Predicate::VersionEquals(replaced_inode_item.version),
            });
            if moving_complete_restore_directory && !replacing_complete_restore_directory {
                // POSIX directory replacement requires an empty ordinary
                // victim. The restored source stays Complete and merely moves;
                // the victim is removed with explicit Dentry/Xattr closure so
                // it cannot leave a hidden namespace tail behind.
                predicates.extend([
                    PredicateRef {
                        family: RecordFamily::Dentry,
                        key: dentry_prefix(self.mount, replaced.attr.inode),
                        predicate: Predicate::PrefixEmpty,
                    },
                    PredicateRef {
                        family: RecordFamily::Xattr,
                        key: xattr_prefix(self.mount, replaced.attr.inode),
                        predicate: Predicate::PrefixEmpty,
                    },
                ]);
                mutations.push(delete_mutation(RecordFamily::Inode, replaced_inode_key));
            } else if replaced_attr.nlink == 1 {
                mutations.push(delete_mutation(RecordFamily::Inode, replaced_inode_key));
                if let Some(body) = &replaced.body {
                    let cleanup = self.final_body_cleanup_plan(
                        replaced.attr.inode,
                        body,
                        read_version,
                        version,
                    )?;
                    predicates.extend(cleanup.predicates);
                    mutations.extend(cleanup.mutations);
                }
            } else {
                replaced_attr.nlink -= 1;
                replaced_attr.generation = version.get();
                replaced_attr.ctime_ms = current_time_ms();
                mutations.push(Mutation {
                    family: RecordFamily::Inode,
                    key: replaced_inode_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_inode_attr(&replaced_attr))),
                });
                for linked in
                    self.linked_dentry_projections_for_inode(replaced.attr.inode, read_version)?
                {
                    if linked.key == destination_key {
                        continue;
                    }
                    predicates.push(PredicateRef {
                        family: RecordFamily::Dentry,
                        key: linked.key.clone(),
                        predicate: Predicate::VersionEquals(linked.version),
                    });
                    let old_projection = linked.projection;
                    let mut updated_projection = old_projection.clone();
                    updated_projection.attr = replaced_attr.clone();
                    updated_projection.dentry.attr_generation = replaced_attr.generation;
                    replaced_link_updates.push((old_projection, updated_projection.clone()));
                    mutations.push(put_projection_mutation(
                        RecordFamily::Dentry,
                        linked.key,
                        &updated_projection,
                    ));
                }
            }
        }
        if let Some(release) = restore_release {
            predicates.extend(release.predicates);
            mutations.extend(release.mutations);
        }
        let source_projection = DentryProjection {
            dentry: source.dentry.clone(),
            attr: source.attr.clone(),
            body: source.body.clone(),
        };
        let replaced_projection = replaced.as_ref().map(|entry| DentryProjection {
            dentry: entry.dentry.clone(),
            attr: entry.attr.clone(),
            body: entry.body.clone(),
        });
        let mut restore_index = self.restore_index_rename_plan(
            &source_projection,
            &projection,
            replaced_projection.as_ref(),
            version,
        )?;
        restore_index.extend(self.restore_index_link_plan(&replaced_link_updates, version)?)?;
        predicates.extend(restore_index.predicates);
        mutations.extend(restore_index.mutations);
        let mut watch = Vec::new();
        if let Some(replaced) = &replaced {
            if let Some(event) = self.watch_projection(
                new_parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(new_parent),
                    name: Some(projection.dentry.name.clone()),
                    inode: replaced.attr.inode,
                    version: version.get(),
                },
            ) {
                watch.push(event);
            }
        }
        if let Some(event) = self.watch_projection(
            parent,
            WatchEvent {
                kind: WatchEventKind::Remove,
                parent: Some(parent),
                name: Some(name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ) {
            watch.push(event);
        }
        if let Some(event) = self.watch_projection(
            new_parent,
            WatchEvent {
                kind: WatchEventKind::Rename,
                parent: Some(new_parent),
                name: Some(projection.dentry.name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ) {
            watch.push(event);
        }

        let command = MetadataCommand {
            request_id: request_id(
                if replace {
                    b"rename-replace"
                } else {
                    b"rename"
                },
                self.mount,
                source.attr.inode,
                version,
            ),
            kind: if replace {
                CommandKind::RenameReplace
            } else {
                CommandKind::Rename
            },
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: destination_key,
            predicates,
            mutations,
            watch,
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(&command, "restore namespace rename")?;
        }
        self.commit_restore_release_transition(command, replacing_complete_restore_directory)?;
        Ok(RenameReplaceResult {
            entry: projection.into(),
            replaced,
        })
    }

    fn live_path_index_key_for_entry(
        &self,
        components: Option<&[DentryName]>,
        parent: InodeId,
        name: &DentryName,
        entry: &DentryWithAttr,
        version: Version,
    ) -> Result<Option<Vec<u8>>, MetadError> {
        let Some(components) = components else {
            return Ok(None);
        };
        if entry.body.is_none() {
            return Ok(None);
        }
        let key = path_index_key(self.mount, components);
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::PathIndex,
            &key,
            predecessor(version)?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(None);
        };
        let indexed = crate::layout::decode_dentry_projection(&item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        let matches_canonical = indexed.attr.inode == entry.attr.inode
            && indexed.dentry.parent == parent
            && indexed.dentry.name == *name;
        Ok(matches_canonical.then_some(key))
    }

    pub(super) fn linked_dentry_projections_for_inode(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<Vec<LinkedDentryProjection>, MetadError> {
        let mut linked = Vec::new();
        self.visit_linked_dentry_projections_for_inode(inode, version, |projection| {
            linked.push(projection);
            Ok(false)
        })?;
        Ok(linked)
    }

    /// Return at most `limit` current links for restore cleanup. The physical
    /// mount scan is always paged and stops as soon as the requested number of
    /// matching links is found; callers never materialize an unbounded hardlink
    /// fanout merely to delete one bounded release batch.
    pub(super) fn restore_linked_dentry_projection_page(
        &self,
        inode: InodeId,
        version: Version,
        limit: usize,
    ) -> Result<Vec<LinkedDentryProjection>, MetadError> {
        let limit = limit.clamp(1, NAMESPACE_PROOF_PAGE_ROWS);
        let mut linked = Vec::with_capacity(limit);
        let prefix = dentry_mount_prefix(self.mount);
        let mut start_after = None;
        'pages: loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: NAMESPACE_PROOF_PAGE_ROWS,
                purpose: ReadPurpose::RestoreStaging,
            })?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let projection = decode_dentry_projection(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                if projection.attr.inode != inode {
                    continue;
                }
                let expected_key = dentry_key(
                    self.mount,
                    projection.dentry.parent,
                    &projection.dentry.name,
                );
                if row.key != expected_key
                    || projection.dentry.child != projection.attr.inode
                    || projection.dentry.child_type != projection.attr.file_type
                    || projection.dentry.attr_generation != projection.attr.generation
                    || projection.body.as_ref().is_some_and(|body| {
                        body.size != projection.attr.size
                            || body.generation == 0
                            || body.generation > projection.attr.generation
                    })
                {
                    return Err(MetadError::Codec(format!(
                        "restore release dentry changed identity for inode {}",
                        inode.get()
                    )));
                }
                linked.push(LinkedDentryProjection {
                    key: row.key.clone(),
                    projection,
                    version: row.version,
                });
                if linked.len() >= limit {
                    break 'pages;
                }
            }
            let reached_tail = rows.len() < NAMESPACE_PROOF_PAGE_ROWS;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }
        Ok(linked)
    }

    fn visit_linked_dentry_projections_for_inode<F>(
        &self,
        inode: InodeId,
        version: Version,
        mut visit: F,
    ) -> Result<bool, MetadError>
    where
        F: FnMut(LinkedDentryProjection) -> Result<bool, MetadError>,
    {
        let prefix = dentry_mount_prefix(self.mount);
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: NAMESPACE_PROOF_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if rows.is_empty() {
                return Ok(false);
            }
            for row in &rows {
                let projection = decode_dentry_projection(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                if projection.attr.inode != inode {
                    continue;
                }
                self.validate_current_dentry_projection(&row.key, &projection, version)?;
                if visit(LinkedDentryProjection {
                    key: row.key.clone(),
                    projection,
                    version: row.version,
                })? {
                    return Ok(true);
                }
            }
            let reached_tail = rows.len() < NAMESPACE_PROOF_PAGE_ROWS;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                return Ok(false);
            }
        }
    }

    /// Enter fail-closed mode before the first metadata write of a detached
    /// materialization. Callers hold `object_gc_gate`, so a link/rename cannot
    /// observe the old healthy state and then race the first orphan-producing
    /// commit.
    pub(super) fn mark_materialization_orphan_possible_under_gc_gate(&self) {
        self.materialization_orphan_possible
            .store(true, Ordering::Release);
    }

    /// Reconstruct the process-local fast-path state after bootstrap, reopen, or
    /// checkpoint installation. An error leaves the marker set, so callers never
    /// mistake an unproven namespace for the healthy state.
    pub(super) fn recover_materialization_orphan_state(&self) -> Result<(), MetadError> {
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.mark_materialization_orphan_possible_under_gc_gate();

        // `open_existing` is also the server's construction path for a brand-new
        // shard: it opens an empty store first and bootstraps the mount root only
        // after the service exists. Keep that pristine case fail-closed until
        // `bootstrap_root` creates the root and runs the full proof. A missing
        // root in any non-empty namespace is different: serving it would hide
        // corruption (including a detached materialization whose binding was
        // never committed), so startup must fail.
        let version = self.read_version()?;
        if self
            .get_attr_at_version_for_purpose(InodeId::root(), version, ReadPurpose::WritePlanLocal)?
            .is_none()
        {
            if self.root_dependent_namespace_records_exist_at(version)? {
                return Err(MetadError::Codec(
                    "mount root is missing while namespace records still exist".to_owned(),
                ));
            }
            return Ok(());
        }

        self.reconcile_materialization_orphan_state_under_gc_gate()?;
        Ok(())
    }

    /// Return whether the mount has any current record whose existence depends
    /// on a bootstrapped namespace. System/Mount state, command dedupe, and GC
    /// control records are deliberately excluded: those can be written before
    /// the root commit and must not make a retryable bootstrap look corrupt.
    fn root_dependent_namespace_records_exist_at(
        &self,
        version: Version,
    ) -> Result<bool, MetadError> {
        const ROOT_DEPENDENT_FAMILIES: [RecordFamily; 11] = [
            RecordFamily::Inode,
            RecordFamily::Dentry,
            RecordFamily::Parent,
            RecordFamily::Xattr,
            RecordFamily::ChunkManifest,
            RecordFamily::Session,
            RecordFamily::PathIndex,
            RecordFamily::Watch,
            RecordFamily::Snapshot,
            RecordFamily::ForkBinding,
            RecordFamily::ForkShadow,
        ];
        let mount_prefix = inode_prefix(self.mount);
        for family in ROOT_DEPENDENT_FAMILIES {
            if !self
                .metadata
                .scan(ScanRequest {
                    family,
                    prefix: mount_prefix.clone(),
                    start_after: None,
                    version,
                    limit: 1,
                    purpose: ReadPurpose::WritePlanLocal,
                })?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Clear the slow-path marker only after proving that every current inode
    /// and dentry belongs to the mount-root/ForkBinding-root reachability graph.
    /// Callers hold `object_gc_gate`; failures leave the marker unchanged.
    pub(super) fn reconcile_materialization_orphan_state_under_gc_gate(
        &self,
    ) -> Result<bool, MetadError> {
        let version = self.read_version()?;
        let reachable = self.namespace_reachability_at(version)?;
        let proven_safe = self.all_current_namespace_records_reachable(&reachable, version)?;
        self.materialization_orphan_possible
            .store(!proven_safe, Ordering::Release);
        Ok(proven_safe)
    }

    /// Healthy services skip the mount-wide scan entirely. The slow path is
    /// entered only after materialization began or startup/restore has not yet
    /// proved the installed state. A harmless failed preflight can self-heal:
    /// when the slow scan finds no unbound records it clears the marker for later
    /// operations, while this operation still uses the proof it just computed.
    fn namespace_reachability_for_exposure(
        &self,
        version: Version,
    ) -> Result<Option<NamespaceReachability>, MetadError> {
        if !self.materialization_orphan_possible.load(Ordering::Acquire) {
            return Ok(None);
        }
        let reachable = self.namespace_reachability_at(version)?;
        if self.all_current_namespace_records_reachable(&reachable, version)? {
            self.materialization_orphan_possible
                .store(false, Ordering::Release);
        }
        Ok(Some(reachable))
    }

    fn all_current_namespace_records_reachable(
        &self,
        reachable: &NamespaceReachability,
        version: Version,
    ) -> Result<bool, MetadError> {
        let purpose = ReadPurpose::WritePlanLocal;
        let dentry_prefix = dentry_mount_prefix(self.mount);
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: dentry_prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: NAMESPACE_PROOF_PAGE_ROWS,
                purpose,
            })?;
            if rows
                .iter()
                .any(|row| !reachable.dentries.contains(&row.key))
            {
                return Ok(false);
            }
            let reached_tail = rows.len() < NAMESPACE_PROOF_PAGE_ROWS;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }

        let inode_prefix = inode_prefix(self.mount);
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Inode,
                prefix: inode_prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: NAMESPACE_PROOF_PAGE_ROWS,
                purpose,
            })?;
            for row in &rows {
                let attr = decode_inode_attr(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                if row.key != inode_key(self.mount, attr.inode) {
                    return Err(MetadError::Codec(format!(
                        "inode row key does not match inode {} during materialization recovery",
                        attr.inode.get()
                    )));
                }
                if !reachable.inodes.contains(&attr.inode) {
                    return Ok(false);
                }
            }
            let reached_tail = rows.len() < NAMESPACE_PROOF_PAGE_ROWS;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn materialization_orphan_slow_path_enabled(&self) -> bool {
        self.materialization_orphan_possible.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn namespace_reachability_scan_count(&self) -> u64 {
        self.namespace_reachability_scans.load(Ordering::Relaxed)
    }

    /// Enumerate the namespace roots from which an inode-addressed mutation may
    /// expose a dentry. Besides the mount root, a detached clone root is legal
    /// only while its validated durable ForkBinding exists and its root inode is
    /// still present. Rollback bindings deliberately survive deletion of their
    /// materialization root; those missing roots are not reachability anchors.
    ///
    /// Callers hold `object_gc_gate` across this walk and their metadata commit.
    /// Fork retirement holds the same gate across its mount-wide proof and CAS,
    /// so neither side can change whether an unbound orphan is reachable in the
    /// middle of the other's decision.
    fn namespace_reachability_at(
        &self,
        version: Version,
    ) -> Result<NamespaceReachability, MetadError> {
        let mut reachable = NamespaceReachability::default();
        self.visit_namespace_reachable_entries_at(version, |key, attr, _| {
            reachable.inodes.insert(attr.inode);
            if attr.inode.shard_index() == self.shard_index()
                && attr.file_type == FileType::Directory
            {
                reachable.directories.insert(attr.inode);
            }
            if let Some(key) = key {
                reachable.dentries.insert(key.to_vec());
            }
            Ok(false)
        })?;
        Ok(reachable)
    }

    /// Prove a batch of inode reachability with one namespace walk. The walk
    /// stops as soon as every candidate is found; an unreachable candidate
    /// requires a complete but physically paged traversal at one stable MVCC
    /// version.
    pub(super) fn restore_reachable_inodes_at(
        &self,
        candidates: &HashSet<InodeId>,
        version: Version,
    ) -> Result<HashSet<InodeId>, MetadError> {
        if candidates.is_empty() {
            return Ok(HashSet::new());
        }
        let mut found = HashSet::with_capacity(candidates.len());
        self.visit_namespace_reachable_entries_at(version, |_, attr, _| {
            if candidates.contains(&attr.inode) {
                found.insert(attr.inode);
            }
            Ok(found.len() == candidates.len())
        })?;
        Ok(found)
    }

    /// Return one reachable body descriptor per candidate inode in one walk.
    /// A reachable body-less inode is represented by `None`; an absent map key
    /// means the inode is not reachable from any namespace anchor.
    pub(super) fn restore_reachable_inode_bodies_at(
        &self,
        candidates: &HashSet<InodeId>,
        version: Version,
    ) -> Result<HashMap<InodeId, Option<BodyDescriptor>>, MetadError> {
        if candidates.is_empty() {
            return Ok(HashMap::new());
        }
        let mut found = HashMap::with_capacity(candidates.len());
        self.visit_namespace_reachable_entries_at(version, |_, attr, body| {
            if candidates.contains(&attr.inode) {
                found.entry(attr.inode).or_insert_with(|| body.cloned());
            }
            Ok(found.len() == candidates.len())
        })?;
        Ok(found)
    }

    fn visit_namespace_reachable_entries_at<F>(
        &self,
        version: Version,
        mut visit: F,
    ) -> Result<bool, MetadError>
    where
        F: FnMut(Option<&[u8]>, &InodeAttr, Option<&BodyDescriptor>) -> Result<bool, MetadError>,
    {
        #[cfg(test)]
        self.namespace_reachability_scans
            .fetch_add(1, Ordering::Relaxed);
        let purpose = ReadPurpose::WritePlanLocal;
        let Some(root_attr) =
            self.namespace_reachability_inode_attr_at(InodeId::root(), version, purpose)?
        else {
            return Err(MetadError::Codec(
                "mount root is missing during namespace reachability proof".to_owned(),
            ));
        };
        if root_attr.file_type != FileType::Directory {
            return Err(MetadError::Codec(
                "mount root is not a directory during namespace reachability proof".to_owned(),
            ));
        }
        if visit(None, &root_attr, None)? {
            return Ok(true);
        }

        let mut pending = vec![InodeId::root()];
        if self.visit_versioned_fork_bindings_at(version, purpose, |versioned| {
            if !self.restore_fork_binding_is_namespace_anchor(&versioned.binding, version)? {
                return Ok(false);
            }
            let root = versioned.binding.fork_root;
            let Some(attr) = self.namespace_reachability_inode_attr_at(root, version, purpose)?
            else {
                return Ok(false);
            };
            if attr.file_type != FileType::Directory {
                return Err(MetadError::Codec(format!(
                    "fork binding root {} is not a directory",
                    root.get()
                )));
            }
            pending.push(root);
            visit(None, &attr, None)
        })? {
            return Ok(true);
        }

        let mut visited_directories = HashSet::new();
        while let Some(parent) = pending.pop() {
            if !visited_directories.insert(parent) {
                continue;
            }
            let prefix = dentry_prefix(self.mount, parent);
            let mut start_after = None;
            loop {
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::Dentry,
                    prefix: prefix.clone(),
                    start_after: start_after.clone(),
                    version,
                    limit: NAMESPACE_PROOF_PAGE_ROWS,
                    purpose,
                })?;
                for row in &rows {
                    let projection = decode_dentry_projection(&row.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?;
                    self.validate_current_dentry_projection(&row.key, &projection, version)?;
                    if visit(Some(&row.key), &projection.attr, projection.body.as_ref())? {
                        return Ok(true);
                    }
                    if projection.attr.inode.shard_index() == self.shard_index()
                        && projection.attr.file_type == FileType::Directory
                    {
                        pending.push(projection.attr.inode);
                    }
                }
                let reached_tail = rows.len() < NAMESPACE_PROOF_PAGE_ROWS;
                start_after = rows.last().map(|row| row.key.clone());
                if reached_tail {
                    break;
                }
            }
        }
        Ok(false)
    }

    /// Read an inode for the internal reachability graph without re-entering
    /// restore visibility. The graph itself is the visibility proof, and it is
    /// also called while the restore visibility fence may already be held.
    fn namespace_reachability_inode_attr_at(
        &self,
        inode: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let Some(value) = self.metadata.get(
            RecordFamily::Inode,
            &inode_key(self.mount, inode),
            version,
            purpose,
        )?
        else {
            return Ok(None);
        };
        let attr =
            decode_inode_attr(&value.0).map_err(|error| MetadError::Codec(error.to_string()))?;
        if attr.inode != inode {
            return Err(MetadError::Codec(format!(
                "inode row key does not match inode {} during namespace reachability proof",
                inode.get()
            )));
        }
        Ok(Some(attr))
    }

    pub(super) fn restore_inode_reachable_from_mount(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<bool, MetadError> {
        let candidates = HashSet::from([inode]);
        Ok(self
            .restore_reachable_inodes_at(&candidates, version)?
            .contains(&inode))
    }

    pub(super) fn commit_create_projection(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<(), MetadError> {
        self.commit_create_projection_with_chunks_and_path_index(
            kind,
            projection,
            &[],
            version,
            None,
            None,
            None,
        )
    }

    pub(super) fn commit_create_projections_with_path_indexes(
        &self,
        kind: CommandKind,
        projections: &[DentryProjection],
        version: Version,
        path_indexes: Option<&[Vec<u8>]>,
    ) -> Result<(), MetadError> {
        let command = self.create_projections_command(kind, projections, version, path_indexes)?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn create_projections_command(
        &self,
        kind: CommandKind,
        projections: &[DentryProjection],
        version: Version,
        path_indexes: Option<&[Vec<u8>]>,
    ) -> Result<MetadataCommand, MetadError> {
        let Some(first) = projections.first() else {
            return Err(MetadError::InvalidPath(
                "batched create requires at least one projection".to_owned(),
            ));
        };
        if let Some(path_indexes) = path_indexes {
            if path_indexes.len() != projections.len() {
                return Err(MetadError::InvalidPath(
                    "batched path index count does not match projections".to_owned(),
                ));
            }
        }
        let parent = first.dentry.parent;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, parent),
            predicate: Predicate::Exists,
        }];
        let restore_write_predicates =
            self.restore_namespace_write_predicates(&[parent], predecessor(version)?)?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        let mut mutations =
            Vec::with_capacity(projections.len() * if path_indexes.is_some() { 3 } else { 2 });
        let mut watch = Vec::with_capacity(projections.len());
        for (index, projection) in projections.iter().enumerate() {
            if projection.dentry.parent != parent {
                return Err(MetadError::InvalidPath(
                    "batched create requires one parent".to_owned(),
                ));
            }
            let inode = projection.attr.inode;
            let dentry = dentry_key(
                self.mount,
                projection.dentry.parent,
                &projection.dentry.name,
            );
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            });
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                dentry,
                projection,
            ));
            if let Some(path_indexes) = path_indexes {
                mutations.push(put_projection_mutation(
                    RecordFamily::PathIndex,
                    path_indexes[index].clone(),
                    projection,
                ));
            }
            if let Some(event) = self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: create_watch_kind(kind),
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            ) {
                watch.push(event);
            }
        }
        let enrollment =
            self.restore_namespace_enrollment_plan(parent, projections, predecessor(version)?)?;
        predicates.extend(enrollment.predicates);
        mutations.extend(enrollment.mutations);
        let command = MetadataCommand {
            request_id: request_id(kind_name(kind), self.mount, parent, version),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, parent),
            predicates,
            mutations,
            watch,
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(
                &command,
                "restore namespace create batch",
            )?;
        }
        Ok(command)
    }

    /// Build the cross-shard graft command. Unlike every other create path this
    /// emits a SINGLE mutation — the dentry projection — and NO Inode record for
    /// the (foreign) child. Predicates match the single-projection create path:
    /// parent inode must Exist (the graft lands under a real local directory) and
    /// the dentry must NotExist (idempotent re-runs surface as `PredicateFailed`,
    /// which the client orchestration tolerates).
    fn create_graft_command(
        &self,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<MetadataCommand, MetadError> {
        let parent = projection.dentry.parent;
        let dentry = dentry_key(self.mount, parent, &projection.dentry.name);
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            },
        ];
        let restore_write_predicates =
            self.restore_namespace_write_predicates(&[parent], predecessor(version)?)?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        let enrollment = self.restore_namespace_enrollment_plan(
            parent,
            std::slice::from_ref(projection),
            predecessor(version)?,
        )?;
        predicates.extend(enrollment.predicates);
        let mut mutations = vec![put_projection_mutation(
            RecordFamily::Dentry,
            dentry.clone(),
            projection,
        )];
        mutations.extend(enrollment.mutations);
        let command = MetadataCommand {
            request_id: request_id(
                kind_name(CommandKind::CreateDir),
                self.mount,
                parent,
                version,
            ),
            kind: CommandKind::CreateDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates,
            // Exactly one mutation: the dentry projection. No Inode record for the
            // foreign child — that is the allocator-safety invariant of a graft.
            mutations,
            watch: self
                .watch_projection(
                    parent,
                    WatchEvent {
                        kind: create_watch_kind(CommandKind::CreateDir),
                        parent: Some(parent),
                        name: Some(projection.dentry.name.clone()),
                        inode: projection.attr.inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(
                &command,
                "restore namespace create graft",
            )?;
        }
        Ok(command)
    }

    pub(super) fn commit_create_projection_with_chunks(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        version: Version,
        object_reference: ObjectReferenceMutation,
    ) -> Result<(), MetadError> {
        self.commit_create_projection_with_chunks_and_path_index(
            kind,
            projection,
            chunks,
            version,
            None,
            Some(object_reference),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn commit_create_projection_with_chunks_and_path_index(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        version: Version,
        path_index: Option<Vec<u8>>,
        object_reference: Option<ObjectReferenceMutation>,
        request_id_override: Option<Vec<u8>>,
    ) -> Result<(), MetadError> {
        let inode = projection.attr.inode;
        let dentry = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            },
            put_projection_mutation(RecordFamily::Dentry, dentry.clone(), projection),
        ];
        if let Some(path_index) = path_index {
            mutations.push(put_projection_mutation(
                RecordFamily::PathIndex,
                path_index,
                projection,
            ));
        }
        if let Some(body) = &projection.body {
            mutations.push(Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
            for chunk in chunks {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
                    op: MutationOp::Put,
                    value: Some(Value(encode_chunk_manifest(chunk))),
                });
            }
        }
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, projection.dentry.parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            },
        ];
        let restore_write_predicates = self.restore_namespace_write_predicates(
            &[projection.dentry.parent],
            predecessor(version)?,
        )?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        predicates.extend(restore_write_predicates);
        let enrollment = self.restore_namespace_enrollment_plan(
            projection.dentry.parent,
            std::slice::from_ref(projection),
            predecessor(version)?,
        )?;
        predicates.extend(enrollment.predicates);
        mutations.extend(enrollment.mutations);
        if let Some(object_reference) = object_reference {
            predicates.push(object_reference.predicate(self.mount));
        }
        let command = MetadataCommand {
            request_id: request_id_override
                .unwrap_or_else(|| request_id(kind_name(kind), self.mount, inode, version)),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    projection.dentry.parent,
                    WatchEvent {
                        kind: create_watch_kind(kind),
                        parent: Some(projection.dentry.parent),
                        name: Some(projection.dentry.name.clone()),
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(
                &command,
                "restore namespace publish create",
            )?;
        }
        self.commit_metadata(command)?;
        Ok(())
    }

    pub(super) fn commit_replace_projection_with_chunks(
        &self,
        commit: ReplaceProjectionCommit<'_>,
    ) -> Result<(), MetadError> {
        let ReplaceProjectionCommit {
            request_id: request_id_override,
            kind,
            projection,
            chunks,
            old_chunks,
            dentry_version,
            old_generation,
            version,
            path_index,
            object_reference,
        } = commit;
        let inode = projection.attr.inode;
        let dentry = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        let read_version = predecessor(version)?;
        let restore_write_predicates = self
            .restore_namespace_write_predicates(&[projection.dentry.parent, inode], read_version)?;
        let restore_guarded =
            super::restore::restore_write_predicates_include_owner(&restore_write_predicates);
        let linked = if projection.attr.nlink <= 1 {
            let old_projection = if restore_guarded {
                let old = self
                    .metadata
                    .get_versioned(
                        RecordFamily::Dentry,
                        &dentry,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or(MetadError::NotFound)?;
                if old.version != dentry_version {
                    return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                }
                decode_dentry_projection(&old.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))?
            } else {
                // Preserve the original prepared-publish contract outside a
                // completed restore. In particular, do not require historical
                // dentry state which may already have been pruned: the exact
                // VersionEquals CAS below reports a stale plan, and Holt's
                // request-id dedupe makes an identical retry idempotent.
                projection.clone()
            };
            vec![LinkedDentryProjection {
                key: dentry.clone(),
                projection: old_projection,
                version: dentry_version,
            }]
        } else {
            self.linked_dentry_projections_for_inode(inode, read_version)?
        };
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::VersionEquals(dentry_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                predicate: Predicate::Exists,
            },
        ];
        predicates.extend(restore_write_predicates);
        let primary_overlay_managed = if restore_guarded && path_index.is_some() {
            let primary = linked
                .iter()
                .find(|linked| linked.key == dentry)
                .ok_or_else(|| {
                    MetadError::Codec(
                        "restore publish primary dentry is missing from its inode links".to_owned(),
                    )
                })?;
            self.restore_index_manages_projection_location(&primary.projection, read_version)?
        } else {
            false
        };
        if let Some(object_reference) = object_reference {
            predicates.push(object_reference.predicate(self.mount));
        }
        let mut mutations = vec![Mutation {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, inode),
            op: MutationOp::Put,
            value: Some(Value(encode_inode_attr(&projection.attr))),
        }];
        let mut primary_projection_updated = false;
        let mut updated_link_projections = Vec::with_capacity(linked.len());
        for linked in linked {
            if linked.key != dentry {
                predicates.push(PredicateRef {
                    family: RecordFamily::Dentry,
                    key: linked.key.clone(),
                    predicate: Predicate::VersionEquals(linked.version),
                });
            } else {
                primary_projection_updated = true;
            }
            let old = linked.projection;
            let mut updated = old.clone();
            updated.attr = projection.attr.clone();
            updated.dentry.attr_generation = projection.attr.generation;
            updated.body = projection.body.clone();
            updated_link_projections.push((old, updated.clone()));
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                linked.key,
                &updated,
            ));
        }
        if !primary_projection_updated {
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                dentry.clone(),
                projection,
            ));
        }
        let restore_index = self.restore_index_link_plan(&updated_link_projections, version)?;
        predicates.extend(restore_index.predicates);
        mutations.extend(restore_index.mutations);
        if let Some(path_index) = path_index.filter(|_| !primary_overlay_managed) {
            mutations.push(put_projection_mutation(
                RecordFamily::PathIndex,
                path_index,
                projection,
            ));
        }
        if let Some(body) = &projection.body {
            // Only reclaim the prior generation when this one is self-contained
            // (base_generation == 0: a fresh write or a compaction that
            // re-materialized every chunk). A delta/sparse generation falls
            // through to its base, which must survive until chain collapse
            // (compaction reclaims it then, see Phase 4 GC predicate).
            if let Some(old_generation) = old_generation.filter(|_| body.base_generation == 0) {
                let retained_object_keys = chunk_object_keys(chunks);
                // A self-contained generation supersedes the ENTIRE old chain,
                // not just its top — reclaim every now-unreachable generation.
                mutations.extend(self.collapse_chain_gc_mutations(
                    inode,
                    old_generation,
                    old_chunks,
                    version,
                    &retained_object_keys,
                )?);
            }
            mutations.push(Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
            for chunk in chunks {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
                    op: MutationOp::Put,
                    value: Some(Value(encode_chunk_manifest(chunk))),
                });
            }
        }
        let command = MetadataCommand {
            request_id: request_id_override
                .unwrap_or_else(|| request_id(kind_name(kind), self.mount, inode, version)),
            kind,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    projection.dentry.parent,
                    WatchEvent {
                        kind: create_watch_kind(kind),
                        parent: Some(projection.dentry.parent),
                        name: Some(projection.dentry.name.clone()),
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        if restore_guarded {
            super::restore::validate_restore_command_bounds(
                &command,
                "restore namespace publish replace",
            )?;
        }
        self.commit_metadata(command)?;
        Ok(())
    }
}

fn map_remove_empty_dir_commit(result: Result<CommitResult, MetadError>) -> Result<(), MetadError> {
    match result {
        Ok(_) => Ok(()),
        Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
            Err(MetadError::DirectoryNotEmpty)
        }
        Err(err) => Err(err),
    }
}
