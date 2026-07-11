//! Atomic multi-shard checkpoint publish.
//!
//! A large-model training checkpoint is N per-rank shard blobs that must become
//! visible **all-at-once or not-at-all** — a crash mid-write must never leave a
//! half-checkpoint a restore would silently load. NoKV already has the pieces a
//! generic object-backed FS lacks: object-first staging, an atomic metadata
//! commit, a byte-range index (`BlockDescriptor` maps logical → object ranges, so
//! **reshard-on-read is just a range read**), and CoW version pins (snapshot the
//! checkpoint dir = a parallelism-agnostic checkpoint *version*). This module adds
//! the one missing primitive: committing all N shard files in a **single**
//! metadata command so the whole checkpoint version is atomic.
//!
//! This is the storage-backend contract ByteCheckpoint (NSDI'25) / PyTorch DCP
//! converged on (tmp → atomic visibility behind a barrier); the per-tensor shard
//! index (FQN → byte range) stays the framework's concern — NoKV serves the
//! atomic publish, the range reads, and the version pin.

use super::*;

/// One shard of a checkpoint to publish atomically.
pub struct CheckpointShard {
    pub name: DentryName,
    pub bytes: Vec<u8>,
}

/// The shards published by [`NoKvFs::publish_checkpoint`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointHandle {
    pub parent: InodeId,
    pub shards: Vec<(DentryName, InodeId)>,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Publish `shards` into directory `parent` as a **single atomic checkpoint**:
    /// every shard file becomes visible in one metadata commit, or none does.
    /// Objects are staged first (object-first), so a failure before the commit
    /// leaves only GC-able orphan objects — never a partial checkpoint. Snapshot
    /// `parent` afterwards to pin a durable, parallelism-agnostic version.
    pub fn publish_checkpoint(
        &self,
        parent: InodeId,
        shards: Vec<CheckpointShard>,
        uid: u32,
        gid: u32,
    ) -> Result<CheckpointHandle, MetadError> {
        if shards.is_empty() {
            return Err(MetadError::InvalidPath(
                "checkpoint requires at least one shard".to_owned(),
            ));
        }
        let object_reference = self.begin_object_reference_mutation()?;

        // Stage every shard's objects + build its (projection, chunks). Track the
        // staged object sets so a mid-stage failure reclaims them instead of
        // leaking, and commits nothing.
        let mut staged_artifacts: Vec<(DentryProjection, Vec<ChunkManifest>)> =
            Vec::with_capacity(shards.len());
        let mut staged_sets: Vec<StagedObjectSet> = Vec::with_capacity(shards.len());
        let mut published: Vec<(DentryName, InodeId)> = Vec::with_capacity(shards.len());

        for shard in &shards {
            let prepared = match self.prepare_artifact_create(parent, shard.name.clone()) {
                Ok(prepared) => prepared,
                Err(err) => {
                    self.abort_staged_checkpoint(&staged_sets);
                    return Err(err);
                }
            };
            let version = Version::new(prepared.generation)?;
            if prepared.object_gc_claim_version != object_reference.version().get() {
                self.abort_staged_checkpoint(&staged_sets);
                return Err(MetadError::Metadata(MetadataError::PredicateFailed));
            }
            let request = PublishArtifact {
                parent,
                name: shard.name.clone(),
                producer: "checkpoint".to_owned(),
                digest_uri: format!("sha256:checkpoint:{}", prepared.inode.get()),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: String::from_utf8_lossy(shard.name.as_bytes()).into_owned(),
                bytes: shard.bytes.clone(),
                mode: 0o644,
                uid,
                gid,
            };
            let StagedArtifactBody {
                body,
                chunks,
                staged,
                ..
            } = match self.stage_artifact_body(&request, prepared.inode, version) {
                Ok(staged) => staged,
                Err(err) => {
                    self.abort_staged_checkpoint(&staged_sets);
                    return Err(err);
                }
            };
            staged_sets.push(staged);

            let attr = InodeAttr {
                inode: prepared.inode,
                file_type: FileType::File,
                mode: 0o644,
                uid,
                gid,
                rdev: 0,
                nlink: FileType::File.initial_link_count(),
                size: body.size,
                generation: prepared.generation,
                mtime_ms: prepared.mtime_ms,
                ctime_ms: prepared.ctime_ms,
            };
            let proj = projection(parent, shard.name.clone(), attr, Some(body));
            published.push((shard.name.clone(), prepared.inode));
            staged_artifacts.push((proj, chunks));
        }

        // One atomic commit makes every shard visible together.
        let commit_version = self.next_version()?;
        if let Err(err) = self.commit_checkpoint_shards(
            parent,
            &staged_artifacts,
            commit_version,
            object_reference,
        ) {
            self.abort_staged_checkpoint(&staged_sets);
            return Err(err);
        }
        Ok(CheckpointHandle {
            parent,
            shards: published,
        })
    }

    fn abort_staged_checkpoint(&self, staged: &[StagedObjectSet]) {
        for set in staged {
            let _ = self.objects.delete_staged(set);
        }
    }

    fn commit_checkpoint_shards(
        &self,
        parent: InodeId,
        shards: &[(DentryProjection, Vec<ChunkManifest>)],
        commit_version: Version,
        object_reference: ObjectReferenceMutation,
    ) -> Result<(), MetadError> {
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, parent),
            predicate: Predicate::Exists,
        }];
        predicates.push(object_reference.predicate(self.mount));
        let mut mutations = Vec::new();
        for (proj, chunks) in shards {
            let inode = proj.attr.inode;
            let dentry = dentry_key(self.mount, parent, &proj.dentry.name);
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&proj.attr))),
            });
            mutations.push(put_projection_mutation(RecordFamily::Dentry, dentry, proj));
            if let Some(body) = &proj.body {
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
                        key: chunk_manifest_key(
                            self.mount,
                            inode,
                            body.generation,
                            chunk.chunk_index,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_chunk_manifest(chunk))),
                    });
                }
            }
        }
        let watch = shards
            .iter()
            .flat_map(|(projection, _)| {
                self.watch_projection(
                    parent,
                    WatchEvent {
                        kind: WatchEventKind::PublishArtifact,
                        parent: Some(parent),
                        name: Some(projection.dentry.name.clone()),
                        inode: projection.attr.inode,
                        version: commit_version.get(),
                    },
                )
            })
            .collect();
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"publish-checkpoint", self.mount, parent, commit_version),
            kind: CommandKind::PublishArtifact,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, parent),
            predicates,
            mutations,
            watch,
        })?;
        Ok(())
    }
}
