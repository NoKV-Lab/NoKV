use super::*;

/// One node of the source subtree paired with the fork inode it copies into.
struct CloneFrame {
    src_inode: InodeId,
    dst_inode: InodeId,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Create a writable copy-on-write fork of the directory subtree rooted at
    /// `src_root`.
    ///
    /// The returned [`CloneHandle::root`] is a new namespace root that sees every
    /// file and directory the source had at clone time. File bodies are **shared,
    /// not copied**: the fork's chunk manifests reference the same object blocks
    /// (same `blocks/{mount}/{inode}/{generation}/...` keys) as the source. The
    /// data is therefore zero-copy and the clone is **independent of data size**;
    /// the metadata work is **O(entries)** — one inode + one commit per descendant,
    /// the same complexity class as any per-entry namespace copy (batching these
    /// commits, or a lazy CoW namespace fork, is future work). Each fork node gets a
    /// fresh inode, so the fork's namespace is fully isolated: writing or deleting
    /// in the fork does not affect the source, and vice versa.
    ///
    /// Divergence is copy-on-write at the object layer. The first time a fork file
    /// is rewritten, the normal publish path mints a fresh generation under the
    /// fork's own inode, producing new object keys; the borrowed source blocks are
    /// left untouched (a borrower never GCs another namespace's blocks).
    ///
    /// The clone retains a snapshot pin on the source ([`CloneHandle::snapshot_id`])
    /// so the GC retention floor protects the shared base blocks while the fork
    /// references them. Retire it with [`NoKvFs::retire_snapshot`] when the fork no
    /// longer needs the source's base objects (typically when the fork is deleted).
    pub fn clone_subtree(&self, src_root: InodeId) -> Result<CloneHandle, MetadError> {
        // Pin the source first so the read version we copy from is stable and the
        // shared base objects are GC-protected from the moment the fork exists.
        let pin = self.snapshot_subtree(src_root)?;
        let version = Version::new(pin.read_version)?;
        let dst_root = self.materialize_subtree_at(src_root, version)?;
        Ok(CloneHandle {
            root: dst_root,
            snapshot_id: pin.snapshot_id,
        })
    }

    /// Materialize the directory subtree rooted at `src_root`, **as seen at
    /// `read_version`**, into a brand-new detached namespace root and return that
    /// root inode.
    ///
    /// This is the shared copy-on-write walk behind both [`NoKvFs::clone_subtree`]
    /// (which materializes the *current* state) and [`NoKvFs::rollback_subtree`]
    /// (which materializes a *prior snapshot's* state). Every node is reproduced
    /// under a fresh inode while keeping each file body's `generation`, so the new
    /// tree's chunk manifests reference the same `blocks/{mount}/{inode}/{generation}/...`
    /// object keys as the source captured at `read_version` — the bodies are shared,
    /// not copied, so this is zero-copy in data and O(entries) in metadata (one
    /// commit per descendant). The returned root is detached: no
    /// dentry names it yet, so the caller must link it (clone) or graft it over an
    /// existing root (rollback).
    ///
    /// `read_version` must be a stable, GC-protected read version (a snapshot pin's
    /// `read_version`), otherwise the source blocks the new manifests borrow could be
    /// reclaimed out from under it.
    ///
    /// Snapshot-aware metadata scans merge current keys with the derived retained-
    /// history key index, so deleted entries are enumerated without a whole-history
    /// scan or a second point-read reconstruction pass.
    pub(super) fn materialize_subtree_at(
        &self,
        src_root: InodeId,
        read_version: Version,
    ) -> Result<InodeId, MetadError> {
        let Some(src_attr) =
            self.get_attr_at_version_for_purpose(src_root, read_version, ReadPurpose::Snapshot)?
        else {
            return Err(MetadError::NotFound);
        };
        if src_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }

        let dst_root = self.next_inode()?;
        let root_version = self.next_version()?;
        let root_attr = directory_attr(
            dst_root,
            src_attr.mode,
            src_attr.uid,
            src_attr.gid,
            root_version.get(),
        );
        self.commit_root_inode(&root_attr, root_version)?;
        self.copy_inode_xattrs(src_root, dst_root, read_version)?;

        // Breadth-first so a fork parent always exists before its children (the
        // create path predicates on the parent inode existing). Each directory's
        // children are materialized in a single batched commit, so a clone costs
        // one commit per source directory rather than one per entry.
        let mut queue = vec![CloneFrame {
            src_inode: src_root,
            dst_inode: dst_root,
        }];
        while let Some(frame) = queue.pop() {
            let children = self.list_dir_at_version(frame.src_inode, read_version)?;
            if !children.is_empty() {
                let mut sub_frames =
                    self.clone_children_into(frame.dst_inode, &children, read_version)?;
                queue.append(&mut sub_frames);
            }
        }

        Ok(dst_root)
    }

    /// Enumerate the children of `dir` at `version` through snapshot-aware scan.
    fn list_dir_at_version(
        &self,
        dir: InodeId,
        version: Version,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        self.read_dir_plus_at_version_for_purpose(dir, version, ReadPurpose::Snapshot)
    }

    /// Path variant of [`NoKvFs::clone_subtree`].
    pub fn clone_subtree_path(&self, path: &str) -> Result<CloneHandle, MetadError> {
        let root = self.resolve_directory_path(path)?;
        self.clone_subtree(root)
    }

    /// Clone the subtree at `src_path` and link the resulting fork root into the
    /// namespace at `dst_path`, so the fork is a usable, navigable directory.
    ///
    /// This is [`NoKvFs::clone_subtree_path`] plus a single dentry that attaches the
    /// detached fork root under `dst_path`'s parent. The fork shares the source's
    /// object blocks until it diverges on write (see [`NoKvFs::clone_subtree`]) and
    /// the returned [`CloneHandle`] carries the retained snapshot pin exactly as the
    /// detached clone does. `dst_path`'s parent must already exist and `dst_path`
    /// itself must be free.
    pub fn clone_subtree_path_into(
        &self,
        src_path: &str,
        dst_path: &str,
    ) -> Result<CloneHandle, MetadError> {
        let src_root = self.resolve_directory_path(src_path)?;
        let (dst_parent, dst_name) = self.resolve_parent_path(dst_path)?;
        let handle = self.clone_subtree(src_root)?;
        self.link_clone_root(handle.root, dst_parent, dst_name)?;
        Ok(handle)
    }

    /// Attach an existing (detached) fork root inode under `dst_parent` as
    /// `dst_name`, committing only the directory dentry that names it. The inode
    /// already exists from [`NoKvFs::clone_subtree`]; this just makes it reachable.
    fn link_clone_root(
        &self,
        root: InodeId,
        dst_parent: InodeId,
        dst_name: DentryName,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let Some(mut attr) =
            self.get_attr_at_version_for_purpose(root, read_version, ReadPurpose::WritePlanLocal)?
        else {
            return Err(MetadError::NotFound);
        };
        attr.ctime_ms = current_time_ms();
        let projection = projection(dst_parent, dst_name, attr, None);
        let dentry = dentry_key(self.mount, dst_parent, &projection.dentry.name);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"clone-subtree-link", self.mount, root, version),
            kind: CommandKind::CreateDir,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, dst_parent),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![put_projection_mutation(
                RecordFamily::Dentry,
                dentry,
                &projection,
            )],
            watch: Vec::new(),
        })?;
        Ok(())
    }

    /// Report the path-level differences between two subtrees as a flat list of
    /// [`SubtreeDelta`]s. Paths are relative to the subtree roots. An entry that
    /// exists only under `b_root` is `Added`, only under `a_root` is `Removed`, and
    /// present under both but with a different type or content is `Modified`.
    ///
    /// Two files are considered identical when they share the same content
    /// generation (the copy-on-write sharing signal a clone establishes) along with
    /// the same size and content digest; a divergent write bumps the generation, so
    /// rewritten files surface as `Modified` while still-shared files do not.
    pub fn diff_subtrees(
        &self,
        a_root: InodeId,
        b_root: InodeId,
    ) -> Result<Vec<SubtreeDelta>, MetadError> {
        let version = self.read_version()?;
        if self
            .get_attr_at_version(a_root, version)?
            .is_none_or(|attr| attr.file_type != FileType::Directory)
        {
            return Err(MetadError::NotDirectory);
        }
        if self
            .get_attr_at_version(b_root, version)?
            .is_none_or(|attr| attr.file_type != FileType::Directory)
        {
            return Err(MetadError::NotDirectory);
        }
        let mut deltas = Vec::new();
        self.diff_dirs(a_root, b_root, "", version, &mut deltas)?;
        Ok(deltas)
    }

    /// Path variant of [`NoKvFs::diff_subtrees`]. Resolves both subtree roots from
    /// their paths and reports the deltas with `a_path` as the base direction.
    pub fn diff_subtrees_path(
        &self,
        a_path: &str,
        b_path: &str,
    ) -> Result<Vec<SubtreeDelta>, MetadError> {
        let a_root = self.resolve_directory_path(a_path)?;
        let b_root = self.resolve_directory_path(b_path)?;
        self.diff_subtrees(a_root, b_root)
    }

    fn diff_dirs(
        &self,
        a_dir: InodeId,
        b_dir: InodeId,
        prefix: &str,
        version: Version,
        deltas: &mut Vec<SubtreeDelta>,
    ) -> Result<(), MetadError> {
        let a_entries = self.entries_by_name(a_dir, version)?;
        let b_entries = self.entries_by_name(b_dir, version)?;
        for (name, a_entry) in &a_entries {
            let path = child_path(prefix, name)?;
            match b_entries.get(name) {
                None => deltas.push(SubtreeDelta {
                    path,
                    kind: SubtreeDeltaKind::Removed,
                    digest: entry_digest(a_entry),
                    size_delta: -(a_entry.attr.size as i64),
                }),
                Some(b_entry) => {
                    let both_dirs = a_entry.attr.file_type == FileType::Directory
                        && b_entry.attr.file_type == FileType::Directory;
                    if both_dirs {
                        self.diff_dirs(
                            a_entry.attr.inode,
                            b_entry.attr.inode,
                            &path,
                            version,
                            deltas,
                        )?;
                    } else if !entries_equivalent(a_entry, b_entry) {
                        deltas.push(SubtreeDelta {
                            path,
                            kind: SubtreeDeltaKind::Modified,
                            digest: entry_digest(b_entry),
                            size_delta: b_entry.attr.size as i64 - a_entry.attr.size as i64,
                        });
                    }
                }
            }
        }
        for (name, b_entry) in &b_entries {
            if !a_entries.contains_key(name) {
                deltas.push(SubtreeDelta {
                    path: child_path(prefix, name)?,
                    kind: SubtreeDeltaKind::Added,
                    digest: entry_digest(b_entry),
                    size_delta: b_entry.attr.size as i64,
                });
            }
        }
        Ok(())
    }

    fn entries_by_name(
        &self,
        dir: InodeId,
        version: Version,
    ) -> Result<BTreeMap<Vec<u8>, DentryWithAttr>, MetadError> {
        let entries =
            self.read_dir_plus_at_version_for_purpose(dir, version, ReadPurpose::UserStrong)?;
        Ok(entries
            .into_iter()
            .map(|entry| (entry.dentry.name.as_bytes().to_vec(), entry))
            .collect())
    }

    fn commit_root_inode(&self, attr: &InodeAttr, version: Version) -> Result<(), MetadError> {
        let key = inode_key(self.mount, attr.inode);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"clone-subtree-root", self.mount, attr.inode, version),
            kind: CommandKind::CreateDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(attr))),
            }],
            watch: Vec::new(),
        })?;
        Ok(())
    }

    /// Materialize every `child` of one source directory into the fork under
    /// `dst_parent` in a **single batched commit**, keeping each file body's
    /// generation (so chunk manifests keep referencing the source's object
    /// blocks). Returns the child directories to recurse into.
    ///
    /// One commit for a whole directory's children rather than one per child is
    /// what keeps clone's per-commit overhead from dominating: cost scales with the
    /// number of *directories*, not the number of entries.
    fn clone_children_into(
        &self,
        dst_parent: InodeId,
        children: &[DentryWithAttr],
        read_version: Version,
    ) -> Result<Vec<CloneFrame>, MetadError> {
        let commit_version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, dst_parent),
            predicate: Predicate::Exists,
        }];
        let mut mutations = Vec::new();
        let mut watch = Vec::new();
        let mut sub_frames = Vec::new();
        // Xattrs are copied after the batch commit (so the dst inodes exist); most
        // entries have none, so this is usually empty.
        let mut xattr_copies = Vec::new();

        for child in children {
            let dst_inode = self.next_inode()?;
            let mut attr = child.attr.clone();
            attr.inode = dst_inode;
            attr.nlink = attr.file_type.initial_link_count();

            let (body, chunks) = match &child.body {
                Some(body) => {
                    // Carry the body descriptor verbatim, including its generation:
                    // the fork's chunk manifests land under (dst_inode, generation)
                    // but still point at the source's object blocks.
                    attr.generation = body.generation;
                    let chunks = self.chunk_manifests_for_body_at_version(
                        child.attr.inode,
                        body,
                        read_version,
                        ReadPurpose::Snapshot,
                    )?;
                    (Some(body.clone()), chunks)
                }
                None => {
                    attr.generation = commit_version.get();
                    (None, Vec::new())
                }
            };

            let proj = projection(dst_parent, child.dentry.name.clone(), attr, body);
            let dentry = dentry_key(self.mount, dst_parent, &proj.dentry.name);
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, dst_inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&proj.attr))),
            });
            mutations.push(put_projection_mutation(RecordFamily::Dentry, dentry, &proj));
            if let Some(body) = &proj.body {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(
                        self.mount,
                        dst_inode,
                        body.generation,
                        BODY_SUMMARY_CHUNK_INDEX,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_body_descriptor(body))),
                });
                for chunk in &chunks {
                    mutations.push(Mutation {
                        family: RecordFamily::ChunkManifest,
                        key: chunk_manifest_key(
                            self.mount,
                            dst_inode,
                            body.generation,
                            chunk.chunk_index,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_chunk_manifest(chunk))),
                    });
                }
            }
            if let Some(event) = self.watch_projection(
                dst_parent,
                WatchEvent {
                    kind: create_watch_kind(clone_command_kind(child.attr.file_type)),
                    parent: Some(dst_parent),
                    name: Some(child.dentry.name.clone()),
                    inode: dst_inode,
                    version: commit_version.get(),
                },
            ) {
                watch.push(event);
            }
            xattr_copies.push((child.attr.inode, dst_inode));
            if child.attr.file_type == FileType::Directory {
                sub_frames.push(CloneFrame {
                    src_inode: child.attr.inode,
                    dst_inode,
                });
            }
        }

        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"clone-subtree-batch",
                self.mount,
                dst_parent,
                commit_version,
            ),
            kind: CommandKind::CreateFiles,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, dst_parent),
            predicates,
            mutations,
            watch,
        })?;

        for (src, dst) in xattr_copies {
            self.copy_inode_xattrs(src, dst, read_version)?;
        }

        Ok(sub_frames)
    }

    fn copy_inode_xattrs(
        &self,
        src_inode: InodeId,
        dst_inode: InodeId,
        version: Version,
    ) -> Result<(), MetadError> {
        let prefix = xattr_prefix(self.mount, src_inode);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Xattr,
            prefix: prefix.clone(),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::Snapshot,
        })?;
        for row in rows {
            let Some(name) = row.key.strip_prefix(prefix.as_slice()) else {
                continue;
            };
            self.set_xattr(dst_inode, name, row.value.0, XattrSetMode::Create)?;
        }
        Ok(())
    }
}

fn clone_command_kind(file_type: FileType) -> CommandKind {
    match file_type {
        FileType::Directory => CommandKind::CreateDir,
        FileType::Symlink => CommandKind::CreateSymlink,
        FileType::File => CommandKind::CreateFile,
        FileType::NamedPipe | FileType::CharDevice | FileType::BlockDevice | FileType::Socket => {
            CommandKind::CreateSpecialNode
        }
    }
}

/// Two non-directory entries are equivalent when they have the same type and, for
/// content-bearing nodes, the same size, content generation, and digest. A
/// divergent write bumps the generation, so a rewritten file is never equivalent
/// to its shared origin.
/// The content digest of an entry's body, if it has one.
fn entry_digest(entry: &DentryWithAttr) -> Option<String> {
    entry.body.as_ref().map(|body| body.digest_uri.clone())
}

fn entries_equivalent(a: &DentryWithAttr, b: &DentryWithAttr) -> bool {
    if a.attr.file_type != b.attr.file_type {
        return false;
    }
    if a.attr.size != b.attr.size || a.attr.generation != b.attr.generation {
        return false;
    }
    match (&a.body, &b.body) {
        (Some(a_body), Some(b_body)) => {
            a_body.generation == b_body.generation && a_body.digest_uri == b_body.digest_uri
        }
        (None, None) => true,
        _ => false,
    }
}

fn child_path(prefix: &str, name: &[u8]) -> Result<String, MetadError> {
    let name = std::str::from_utf8(name)
        .map_err(|_| MetadError::InvalidPath("subtree diff requires utf-8 names".to_owned()))?;
    Ok(format!("{prefix}/{name}"))
}
