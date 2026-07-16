use super::*;

struct PreparedArtifactPublish<'a> {
    prepared: PreparedArtifact,
    body: BodyDescriptor,
    chunks: Vec<ChunkManifest>,
    old_chunks: &'a [ChunkManifest],
    mode: u32,
    uid: u32,
    gid: u32,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn publish_artifact(&self, request: PublishArtifact) -> Result<DentryWithAttr, MetadError> {
        self.restore_namespace_write_predicates(&[request.parent], self.read_version()?)?;
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: body.size,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        if let Err(err) = self.commit_create_projection_with_chunks(
            CommandKind::PublishArtifact,
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

    pub fn replace_artifact(
        &self,
        request: PublishArtifact,
    ) -> Result<RenameReplaceResult, MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let (existing, dentry_version) = self
            .lookup_plus_for_write_plan(request.parent, &request.name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        self.restore_namespace_write_predicates(
            &[request.parent, existing.attr.inode],
            self.read_version()?,
        )?;
        let version = self.next_version()?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, existing.attr.inode, version)?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode: existing.attr.inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            rdev: 0,
            nlink: existing.attr.nlink,
            size: body.size,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        let old_generation = existing.body.as_ref().map(|body| body.generation);
        if let Err(err) = self.commit_replace_projection_with_chunks(ReplaceProjectionCommit {
            request_id: None,
            kind: CommandKind::ReplaceArtifact,
            projection: &projection,
            chunks: &chunks,
            old_chunks: &[],
            dentry_version,
            old_generation,
            version,
            path_index: None,
            object_reference: Some(object_reference),
        }) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(err),
                staged,
            });
        }
        Ok(RenameReplaceResult {
            entry: projection.into(),
            replaced: Some(existing),
        })
    }

    pub fn prepare_artifact_create(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<PreparedArtifact, MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        self.prepare_artifact_create_with_object_reference(parent, name, object_reference)
    }

    fn prepare_artifact_create_with_object_reference(
        &self,
        parent: InodeId,
        name: DentryName,
        object_reference: ObjectReferenceMutation,
    ) -> Result<PreparedArtifact, MetadError> {
        let read_version = self.read_version()?;
        self.restore_namespace_write_predicates(&[parent], read_version)?;
        let Some(parent_attr) = self.get_attr_at_version_for_purpose(
            parent,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        if parent_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if self.lookup_plus_for_write_plan(parent, &name)?.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        let generation = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        Ok(PreparedArtifact {
            parent,
            name,
            path: None,
            inode,
            generation: generation.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
            replace: false,
            dentry_version: None,
            old_generation: None,
            object_gc_claim_version: object_reference.version().get(),
        })
    }

    pub fn prepare_artifact_create_path(&self, path: &str) -> Result<PreparedArtifact, MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let components = parse_absolute_path(path)?;
        let (parent, name) = self.resolve_parent_path(path)?;
        let mut prepared =
            self.prepare_artifact_create_with_object_reference(parent, name, object_reference)?;
        prepared.path = Some(canonical_path(&components)?);
        Ok(prepared)
    }

    pub fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<PreparedArtifact, MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        self.prepare_artifact_replace_with_object_reference(parent, name, object_reference)
    }

    fn prepare_artifact_replace_with_object_reference(
        &self,
        parent: InodeId,
        name: DentryName,
        object_reference: ObjectReferenceMutation,
    ) -> Result<PreparedArtifact, MetadError> {
        let (existing, dentry_version) = self
            .lookup_plus_for_write_plan(parent, &name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        self.restore_namespace_write_predicates(
            &[parent, existing.attr.inode],
            self.read_version()?,
        )?;
        let generation = self.next_version()?;
        let now_ms = current_time_ms();
        Ok(PreparedArtifact {
            parent,
            name,
            path: None,
            inode: existing.attr.inode,
            generation: generation.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
            replace: true,
            dentry_version: Some(dentry_version.get()),
            old_generation: existing.body.as_ref().map(|body| body.generation),
            object_gc_claim_version: object_reference.version().get(),
        })
    }

    pub fn prepare_artifact_replace_path(
        &self,
        path: &str,
    ) -> Result<PreparedArtifact, MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let (parent, name) = self.resolve_parent_path(path)?;
        let components = parse_absolute_path(path)?;
        let mut prepared =
            self.prepare_artifact_replace_with_object_reference(parent, name, object_reference)?;
        prepared.path = Some(canonical_path(&components)?);
        Ok(prepared)
    }

    /// Refresh an unpublished prepared upload after an unrelated object-GC
    /// epoch invalidated its object reference token. The namespace proof remains
    /// fixed, while a fresh generation makes it
    /// impossible to reuse objects uploaded under the stale GC epoch. Callers
    /// must persist this returned identity before fully restaging the body.
    pub fn refresh_prepared_artifact_object_gc_epoch(
        &self,
        mut prepared: PreparedArtifact,
    ) -> Result<PreparedArtifact, MetadError> {
        self.restore_namespace_write_predicates(
            &[prepared.parent, prepared.inode],
            self.read_version()?,
        )?;
        if let Some(path) = prepared.path.as_deref() {
            let components = parse_absolute_path(path)?;
            let Some((name, parent_components)) = components.split_last() else {
                return Err(MetadError::InvalidPreparedArtifact(
                    "prepared artifact path has no leaf".to_owned(),
                ));
            };
            if name != &prepared.name
                || self.resolve_components_as_directory(parent_components)? != prepared.parent
            {
                return Err(MetadError::InvalidPreparedArtifact(
                    "prepared artifact path no longer matches its parent and name".to_owned(),
                ));
            }
        }

        let current = self.lookup_plus_for_write_plan(prepared.parent, &prepared.name)?;
        if prepared.replace {
            let expected = Version::new(prepared.dentry_version.ok_or_else(|| {
                MetadError::InvalidPreparedArtifact(
                    "replace artifact is missing dentry version".to_owned(),
                )
            })?)?;
            let Some((entry, version)) = current else {
                return Err(MetadError::NotFound);
            };
            if entry.attr.file_type != FileType::File {
                return Err(MetadError::NotFile);
            }
            if entry.attr.inode != prepared.inode || version != expected {
                return Err(MetadError::Metadata(MetadataError::PredicateFailed));
            }
        } else {
            if prepared.dentry_version.is_some() || prepared.old_generation.is_some() {
                return Err(MetadError::InvalidPreparedArtifact(
                    "create artifact must not carry replace state".to_owned(),
                ));
            }
            if current.is_some() || self.get_attr(prepared.inode)?.is_some() {
                return Err(MetadError::Metadata(MetadataError::PredicateFailed));
            }
            let parent = self
                .get_attr(prepared.parent)?
                .ok_or(MetadError::NotFound)?;
            if parent.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
        }

        match self.ensure_prepared_object_gc_epoch(prepared.object_gc_claim_version) {
            Ok(()) => return Ok(prepared),
            Err(MetadError::StalePreparedArtifactObjectGcEpoch { .. }) => {}
            Err(err) => return Err(err),
        }

        let object_reference = self.begin_object_reference_mutation()?;
        let generation = self.next_version()?;
        let now_ms = current_time_ms();
        prepared.generation = generation.get();
        prepared.mtime_ms = now_ms;
        prepared.ctime_ms = now_ms;
        prepared.object_gc_claim_version = object_reference.version().get();
        Ok(prepared)
    }

    pub fn publish_prepared_artifact(
        &self,
        prepared: PreparedArtifact,
        body: BodyDescriptor,
        chunks: Vec<ChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<RenameReplaceResult, MetadError> {
        validate_new_prepared_block_identities(self.mount, &prepared, &chunks)?;
        self.publish_prepared_artifact_impl(
            PreparedArtifactPublish {
                prepared,
                body,
                chunks,
                old_chunks: &[],
                mode,
                uid,
                gid,
            },
            None,
        )
    }

    fn publish_prepared_artifact_impl(
        &self,
        request: PreparedArtifactPublish<'_>,
        request_id_override: Option<Vec<u8>>,
    ) -> Result<RenameReplaceResult, MetadError> {
        validate_prepared_artifact(
            self.mount,
            &request.prepared,
            &request.body,
            &request.chunks,
        )?;
        let request_id = request_id_override
            .unwrap_or_else(|| prepared_artifact_request_id(self.mount, &request));
        let PreparedArtifactPublish {
            prepared,
            body,
            chunks,
            old_chunks,
            mode,
            uid,
            gid,
        } = request;
        let version = Version::new(prepared.generation)?;
        let path_index = prepared
            .path
            .as_deref()
            .map(|path| {
                parse_absolute_path(path).map(|components| path_index_key(self.mount, &components))
            })
            .transpose()?;
        let expected_dentry_version = if prepared.replace {
            Some(Version::new(prepared.dentry_version.ok_or_else(|| {
                MetadError::InvalidPreparedArtifact(
                    "replace artifact is missing dentry version".to_owned(),
                )
            })?)?)
        } else {
            if prepared.dentry_version.is_some() || prepared.old_generation.is_some() {
                return Err(MetadError::InvalidPreparedArtifact(
                    "create artifact must not carry replace state".to_owned(),
                ));
            }
            None
        };
        let mut attr = InodeAttr {
            inode: prepared.inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: body.size,
            generation: prepared.generation,
            mtime_ms: prepared.mtime_ms,
            ctime_ms: prepared.ctime_ms,
        };
        if prepared.replace {
            if let Some((existing, _)) =
                self.lookup_plus_for_write_plan(prepared.parent, &prepared.name)?
            {
                attr.nlink = existing.attr.nlink;
            }
        }
        let projection = projection(prepared.parent, prepared.name.clone(), attr, Some(body));
        let terminal_result = || {
            self.prepared_terminal_commit_result(&request_id, version)
                .map(|terminal| {
                    terminal.map(|_| RenameReplaceResult {
                        entry: projection.clone().into(),
                        replaced: None,
                    })
                })
        };
        if let Some(result) = terminal_result()? {
            return Ok(result);
        }

        let publish = (|| {
            self.ensure_prepared_object_gc_epoch(prepared.object_gc_claim_version)?;
            if let Some(expected_dentry_version) = expected_dentry_version {
                let replaced = self
                    .lookup_plus_for_write_plan(prepared.parent, &prepared.name)?
                    .and_then(|(existing, current_dentry_version)| {
                        (existing.attr.file_type == FileType::File
                            && existing.attr.inode == prepared.inode
                            && current_dentry_version == expected_dentry_version)
                            .then_some(existing)
                    });
                self.commit_replace_projection_with_chunks(ReplaceProjectionCommit {
                    request_id: Some(request_id.clone()),
                    kind: CommandKind::ReplaceArtifact,
                    projection: &projection,
                    chunks: &chunks,
                    old_chunks,
                    dentry_version: expected_dentry_version,
                    old_generation: prepared.old_generation,
                    version,
                    path_index,
                    object_reference: Some(ObjectReferenceMutation::from_version(Version::new(
                        prepared.object_gc_claim_version,
                    )?)),
                })
                .map_err(|err| {
                    self.classify_prepared_object_gc_epoch_error(
                        err,
                        prepared.object_gc_claim_version,
                    )
                })?;
                Ok(RenameReplaceResult {
                    entry: projection.clone().into(),
                    replaced,
                })
            } else {
                self.commit_create_projection_with_chunks_and_path_index(
                    CommandKind::PublishArtifact,
                    &projection,
                    &chunks,
                    version,
                    path_index,
                    Some(ObjectReferenceMutation::from_version(Version::new(
                        prepared.object_gc_claim_version,
                    )?)),
                    Some(request_id.clone()),
                )
                .map_err(|err| {
                    self.classify_prepared_object_gc_epoch_error(
                        err,
                        prepared.object_gc_claim_version,
                    )
                })?;
                Ok(RenameReplaceResult {
                    entry: projection.clone().into(),
                    replaced: None,
                })
            }
        })();
        match publish {
            Ok(result) => Ok(result),
            Err(error) => terminal_result()?.ok_or(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepared_terminal_rename_result(
        &self,
        request_id: &[u8],
        version: Version,
        prepared: &PreparedArtifact,
        body: &BodyDescriptor,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Option<RenameReplaceResult>, MetadError> {
        if self
            .prepared_terminal_commit_result(request_id, version)?
            .is_none()
        {
            return Ok(None);
        }
        if let Some((current, _)) =
            self.lookup_plus_for_write_plan(prepared.parent, &prepared.name)?
        {
            if current.attr.inode == prepared.inode
                && current.attr.generation == prepared.generation
            {
                return Ok(Some(RenameReplaceResult {
                    entry: current,
                    replaced: None,
                }));
            }
        }
        let nlink = if prepared.replace {
            self.lookup_plus_for_write_plan(prepared.parent, &prepared.name)?
                .map(|(entry, _)| entry.attr.nlink)
                .unwrap_or_else(|| FileType::File.initial_link_count())
        } else {
            FileType::File.initial_link_count()
        };
        let attr = InodeAttr {
            inode: prepared.inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink,
            size: body.size,
            generation: prepared.generation,
            mtime_ms: prepared.mtime_ms,
            ctime_ms: prepared.ctime_ms,
        };
        Ok(Some(RenameReplaceResult {
            entry: projection(
                prepared.parent,
                prepared.name.clone(),
                attr,
                Some(body.clone()),
            )
            .into(),
            replaced: None,
        }))
    }

    fn ensure_prepared_object_gc_epoch(&self, expected: u64) -> Result<(), MetadError> {
        let key = object_gc_claim_key(self.mount);
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("durable object GC claim is not initialized".to_owned())
            })?;
        if item.version.get() == expected
            && matches!(
                decode_object_gc_claim(self.mount, &item.value.0)?,
                ObjectGcClaim::Open
            )
        {
            return Ok(());
        }
        Err(MetadError::StalePreparedArtifactObjectGcEpoch {
            expected,
            current: item.version.get(),
        })
    }

    fn classify_prepared_object_gc_epoch_error(
        &self,
        err: MetadError,
        expected: u64,
    ) -> MetadError {
        if !matches!(err, MetadError::Metadata(MetadataError::PredicateFailed)) {
            return err;
        }
        self.ensure_prepared_object_gc_epoch(expected)
            .err()
            .filter(|current| {
                matches!(
                    current,
                    MetadError::StalePreparedArtifactObjectGcEpoch { .. }
                )
            })
            .unwrap_or(err)
    }

    pub fn publish_prepared_artifact_session(
        &self,
        prepared: PreparedArtifact,
        request: PublishArtifactSession,
    ) -> Result<RenameReplaceResult, MetadError> {
        if prepared.parent != request.parent || prepared.name != request.name {
            return Err(MetadError::InvalidPreparedArtifact(
                "prepared artifact target does not match publish session".to_owned(),
            ));
        }
        let request_id = prepared_artifact_session_request_id(self.mount, &prepared, &request);
        let version = Version::new(prepared.generation)?;
        let terminal_body = BodyDescriptor {
            producer: request.producer.clone(),
            digest_uri: request.digest_uri.clone(),
            size: request.size,
            content_type: request.content_type.clone(),
            manifest_id: request.manifest_id.clone(),
            generation: version.get(),
            base_generation: 0,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
        };
        if let Some(result) = self.prepared_terminal_rename_result(
            &request_id,
            version,
            &prepared,
            &terminal_body,
            request.mode,
            request.uid,
            request.gid,
        )? {
            return Ok(result);
        }
        self.restore_namespace_write_predicates(
            &[prepared.parent, prepared.inode],
            self.read_version()?,
        )?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks,
            staged,
        } = self.stage_artifact_session(&request, &prepared, version)?;
        self.publish_prepared_artifact_impl(
            PreparedArtifactPublish {
                prepared,
                body,
                chunks,
                old_chunks: &old_chunks,
                mode: request.mode,
                uid: request.uid,
                gid: request.gid,
            },
            Some(request_id),
        )
        .map_err(|err| MetadError::PublishArtifactFailed {
            source: Box::new(err),
            staged,
        })
    }

    pub fn stage_prepared_artifact_ranges(
        &self,
        prepared: &PreparedArtifact,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> Result<ChunkedWrite, MetadError> {
        self.restore_namespace_write_predicates(
            &[prepared.parent, prepared.inode],
            self.read_version()?,
        )?;
        let dirty_ranges = ranges
            .iter()
            .filter(|range| !range.bytes.is_empty())
            .map(|range| ChunkWriteRange {
                logical_offset: range.offset,
                bytes: range.bytes.clone().into(),
            })
            .collect::<Vec<_>>();
        match self.objects.write_ranges_with_block_index_base(
            dirty_ranges,
            ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: self.mount.get(),
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            block_index_base,
        ) {
            Ok(written) => {
                self.object_puts
                    .fetch_add(written.object_puts as u64, Ordering::Relaxed);
                self.object_put_bytes
                    .fetch_add(written.object_put_bytes, Ordering::Relaxed);
                Ok(written)
            }
            Err(err) => {
                if let ObjectError::StagedWriteFailed { staged, .. } = &err {
                    let _ = self.objects.delete_staged(staged);
                }
                Err(err.into())
            }
        }
    }

    pub fn publish_prepared_artifact_staged_session(
        &self,
        prepared: PreparedArtifact,
        request: PublishArtifactStagedSession,
    ) -> Result<RenameReplaceResult, MetadError> {
        if prepared.parent != request.parent || prepared.name != request.name {
            return Err(MetadError::InvalidPreparedArtifact(
                "prepared artifact target does not match staged publish session".to_owned(),
            ));
        }
        let request_id =
            prepared_artifact_staged_session_request_id(self.mount, &prepared, &request);
        let version = Version::new(prepared.generation)?;
        let terminal_body = BodyDescriptor {
            producer: request.producer.clone(),
            digest_uri: request.digest_uri.clone(),
            size: request.size,
            content_type: request.content_type.clone(),
            manifest_id: request.manifest_id.clone(),
            generation: version.get(),
            base_generation: prepared.old_generation.unwrap_or(0),
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
        };
        if let Some(result) = self.prepared_terminal_rename_result(
            &request_id,
            version,
            &prepared,
            &terminal_body,
            request.mode,
            request.uid,
            request.gid,
        )? {
            return Ok(result);
        }
        validate_new_prepared_block_identities(self.mount, &prepared, &request.chunks)?;
        let (chunks, base_generation) =
            self.resolve_session_chunks(&prepared, request.size, request.chunks)?;
        self.manifest_chunks
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks
            .fetch_add(manifest_block_count(&chunks), Ordering::Relaxed);
        let body = BodyDescriptor {
            producer: request.producer,
            digest_uri: request.digest_uri,
            size: request.size,
            content_type: request.content_type,
            manifest_id: request.manifest_id,
            generation: version.get(),
            base_generation,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
        };
        // A delta publish preserves its base generation; the prior generation's
        // blocks are reclaimed later at chain collapse (compaction), not here.
        let old_chunks: Vec<ChunkManifest> = Vec::new();
        self.publish_prepared_artifact_impl(
            PreparedArtifactPublish {
                prepared,
                body,
                chunks,
                old_chunks: &old_chunks,
                mode: request.mode,
                uid: request.uid,
                gid: request.gid,
            },
            Some(request_id),
        )
        .map_err(|err| MetadError::PublishArtifactFailed {
            source: Box::new(err),
            staged: request.staged,
        })
    }

    pub(super) fn stage_artifact_body(
        &self,
        request: &PublishArtifact,
        inode: InodeId,
        version: Version,
    ) -> Result<StagedArtifactBody, MetadError> {
        let written = self.objects.write_bytes(
            &request.bytes,
            ChunkWriteOptions {
                manifest_id: request.manifest_id.clone(),
                mount: self.mount.get(),
                inode: inode.get(),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        )?;
        let staged = written.staged_objects()?;
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.object_put_bytes
            .fetch_add(written.object_put_bytes, Ordering::Relaxed);
        self.manifest_chunks
            .fetch_add(written.chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            written
                .chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        let chunks = written
            .chunks
            .into_iter()
            .map(|chunk| ChunkManifest {
                chunk_index: chunk.chunk_index,
                logical_offset: chunk.logical_offset,
                len: chunk.len,
                slices: vec![SliceManifest {
                    slice_id: 1,
                    logical_offset: chunk.logical_offset,
                    len: chunk.len,
                    blocks: chunk
                        .blocks
                        .into_iter()
                        .map(|block| BlockDescriptor {
                            object_key: block.object_key,
                            logical_offset: block.logical_offset,
                            object_offset: block.object_offset,
                            len: block.len,
                            digest_uri: block.digest_uri,
                        })
                        .collect(),
                }],
            })
            .collect();
        Ok(StagedArtifactBody {
            body: BodyDescriptor {
                producer: request.producer.clone(),
                digest_uri: request.digest_uri.clone(),
                size: written.size,
                content_type: request.content_type.clone(),
                manifest_id: written.manifest_id,
                generation: version.get(),
                // Fresh write: no prior generation to fall through to.
                base_generation: 0,
                chunk_size: written.chunk_size,
                block_size: written.block_size,
            },
            chunks,
            old_chunks: Vec::new(),
            staged,
        })
    }

    pub(super) fn stage_artifact_session(
        &self,
        request: &PublishArtifactSession,
        prepared: &PreparedArtifact,
        version: Version,
    ) -> Result<StagedArtifactBody, MetadError> {
        validate_artifact_ranges(request)?;
        let dirty_ranges = request
            .ranges
            .iter()
            .filter(|range| !range.bytes.is_empty())
            .map(|range| ChunkWriteRange {
                logical_offset: range.offset,
                bytes: range.bytes.clone().into(),
            })
            .collect::<Vec<_>>();
        let written = self.objects.write_ranges_with_block_index_base(
            dirty_ranges,
            ChunkWriteOptions {
                manifest_id: request.manifest_id.clone(),
                mount: self.mount.get(),
                inode: prepared.inode.get(),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            0,
        )?;
        let staged = written.staged_objects()?;
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.object_put_bytes
            .fetch_add(written.object_put_bytes, Ordering::Relaxed);

        let old_chunks = self.prepared_old_chunks(prepared)?;
        let dirty_chunks = written.chunk_manifests();
        let chunks = merge_session_chunks(request.size, old_chunks.clone(), dirty_chunks)?;
        self.manifest_chunks
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks
            .fetch_add(manifest_block_count(&chunks), Ordering::Relaxed);
        Ok(StagedArtifactBody {
            body: BodyDescriptor {
                producer: request.producer.clone(),
                digest_uri: request.digest_uri.clone(),
                size: request.size,
                content_type: request.content_type.clone(),
                manifest_id: written.manifest_id,
                generation: version.get(),
                // Self-contained today (merge_session_chunks re-materializes);
                // Phase 2 makes this the delta path and sets the prior gen here.
                base_generation: 0,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
            },
            chunks,
            old_chunks,
            staged,
        })
    }

    fn prepared_old_chunks(
        &self,
        prepared: &PreparedArtifact,
    ) -> Result<Vec<ChunkManifest>, MetadError> {
        if !prepared.replace {
            return Ok(Vec::new());
        }
        let Some(generation) = prepared.old_generation else {
            return Ok(Vec::new());
        };
        let version = self.read_version()?;
        let Some(body) = self.body_descriptor_at_version_for_purpose(
            prepared.inode,
            generation,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(Vec::new());
        };
        self.chunk_manifests_for_body_at_version(
            prepared.inode,
            &body,
            version,
            ReadPurpose::WritePlanLocal,
        )
    }

    /// Resolve the chunk manifests to commit for a write session plus the
    /// `base_generation` to record on the body.
    ///
    /// A fresh / non-replace write produces a self-contained full manifest
    /// (`base_generation == 0`). A replace over an existing body produces a
    /// **delta**: only the dirty chunks, each self-contained (its prior blocks
    /// inherited from the old generation's chain), with `base_generation` set to
    /// the prior generation so untouched chunks fall through to it on read. A
    /// partial write therefore commits O(dirty chunks) manifest records instead
    /// of re-materializing the whole file.
    fn resolve_session_chunks(
        &self,
        prepared: &PreparedArtifact,
        size: u64,
        dirty_chunks: Vec<ChunkManifest>,
    ) -> Result<(Vec<ChunkManifest>, u64), MetadError> {
        let old_generation = if prepared.replace {
            prepared.old_generation
        } else {
            None
        };
        let Some(old_generation) = old_generation else {
            return Ok((merge_session_chunks(size, Vec::new(), dirty_chunks)?, 0));
        };
        let version = self.read_version()?;
        let Some(old_body) = self.body_descriptor_at_version_for_purpose(
            prepared.inode,
            old_generation,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            // The prior generation is gone; fall back to a self-contained write.
            return Ok((merge_session_chunks(size, Vec::new(), dirty_chunks)?, 0));
        };
        let old_chain = self.resolve_generation_chain(
            prepared.inode,
            &old_body,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        // Compaction trigger: once the fall-through chain reaches the depth
        // threshold, re-materialize a self-contained generation instead of
        // extending the chain. This collapses the chain back to length 1 so
        // reads stay shallow and the chain never approaches the hard read cap.
        // O(file) at this write, but amortized over the cheap O(write) deltas
        // between compactions.
        const COMPACTION_CHAIN_DEPTH: usize = 8;
        if old_chain.len() >= COMPACTION_CHAIN_DEPTH {
            let old_chunks = self.chunk_manifests_for_body_at_version(
                prepared.inode,
                &old_body,
                version,
                ReadPurpose::WritePlanLocal,
            )?;
            let chunks = merge_session_chunks(size, old_chunks, dirty_chunks)?;
            // Coalesce accumulated slices so compaction actually bounds read
            // amplification, not just chain depth.
            let chunks = compact_chunk_slices(chunks)?;
            return Ok((chunks, 0));
        }
        let chunks =
            self.delta_session_chunks(prepared.inode, &old_chain, size, dirty_chunks, version)?;
        Ok((chunks, old_generation))
    }

    /// Build self-contained manifests for only the chunks a write touched:
    /// inherit each dirty chunk's prior blocks from the old generation's chain
    /// (older slice ids), then append the new blocks (newer slice ids, which win
    /// on overlap). Chunks that resolve to no blocks are omitted so they fall
    /// through to the base on read rather than shadowing it with a hole.
    fn delta_session_chunks(
        &self,
        inode: InodeId,
        old_chain: &[u64],
        size: u64,
        dirty_chunks: Vec<ChunkManifest>,
        version: Version,
    ) -> Result<Vec<ChunkManifest>, MetadError> {
        let mut dirty_indexes = std::collections::BTreeSet::<u64>::new();
        for chunk in &dirty_chunks {
            for slice in &chunk.slices {
                dirty_indexes.insert(slice.logical_offset / DEFAULT_CHUNK_SIZE);
            }
        }
        let mut chunks = std::collections::BTreeMap::<u64, ChunkManifest>::new();
        for &chunk_index in &dirty_indexes {
            if let Some(old) = self.chain_chunk_manifest(
                inode,
                old_chain,
                chunk_index,
                version,
                ReadPurpose::WritePlanLocal,
            )? {
                append_chunk_manifest_slices(&mut chunks, old, size)?;
            }
        }
        for chunk in dirty_chunks {
            append_chunk_manifest_slices(&mut chunks, chunk, size)?;
        }
        Ok(chunks.into_values().collect())
    }
}

fn prepared_artifact_request_id(mount: MountId, request: &PreparedArtifactPublish<'_>) -> Vec<u8> {
    let PreparedArtifactPublish {
        prepared,
        body,
        chunks,
        old_chunks,
        mode,
        uid,
        gid,
    } = request;
    let (kind, mut hasher) =
        prepared_request_hasher(mount, prepared, b"nokv.prepared-artifact.request.v1");
    hash_prepared_field(&mut hasher, b"body", &encode_body_descriptor(body));
    hash_prepared_field(
        &mut hasher,
        b"chunk_count",
        &(chunks.len() as u64).to_be_bytes(),
    );
    for chunk in chunks {
        hash_prepared_field(&mut hasher, b"chunk", &encode_chunk_manifest(chunk));
    }
    hash_prepared_field(
        &mut hasher,
        b"old_chunk_count",
        &(old_chunks.len() as u64).to_be_bytes(),
    );
    for chunk in *old_chunks {
        hash_prepared_field(&mut hasher, b"old_chunk", &encode_chunk_manifest(chunk));
    }
    hash_prepared_field(&mut hasher, b"mode", &mode.to_be_bytes());
    hash_prepared_field(&mut hasher, b"uid", &uid.to_be_bytes());
    hash_prepared_field(&mut hasher, b"gid", &gid.to_be_bytes());

    finish_prepared_request_id(kind, hasher)
}

fn prepared_artifact_session_request_id(
    mount: MountId,
    prepared: &PreparedArtifact,
    request: &PublishArtifactSession,
) -> Vec<u8> {
    let (kind, mut hasher) = prepared_request_hasher(
        mount,
        prepared,
        b"nokv.prepared-artifact.session.request.v1",
    );
    hash_prepared_field(&mut hasher, b"producer", request.producer.as_bytes());
    hash_prepared_field(&mut hasher, b"digest_uri", request.digest_uri.as_bytes());
    hash_prepared_field(
        &mut hasher,
        b"content_type",
        request.content_type.as_bytes(),
    );
    hash_prepared_field(&mut hasher, b"manifest_id", request.manifest_id.as_bytes());
    hash_prepared_field(&mut hasher, b"size", &request.size.to_be_bytes());
    hash_prepared_field(
        &mut hasher,
        b"range_count",
        &(request.ranges.len() as u64).to_be_bytes(),
    );
    for range in &request.ranges {
        hash_prepared_field(&mut hasher, b"range_offset", &range.offset.to_be_bytes());
        hash_prepared_field(&mut hasher, b"range_bytes", &range.bytes);
    }
    hash_prepared_field(&mut hasher, b"mode", &request.mode.to_be_bytes());
    hash_prepared_field(&mut hasher, b"uid", &request.uid.to_be_bytes());
    hash_prepared_field(&mut hasher, b"gid", &request.gid.to_be_bytes());
    finish_prepared_request_id(kind, hasher)
}

fn prepared_artifact_staged_session_request_id(
    mount: MountId,
    prepared: &PreparedArtifact,
    request: &PublishArtifactStagedSession,
) -> Vec<u8> {
    let (kind, mut hasher) = prepared_request_hasher(
        mount,
        prepared,
        b"nokv.prepared-artifact.staged-session.request.v1",
    );
    hash_prepared_field(&mut hasher, b"producer", request.producer.as_bytes());
    hash_prepared_field(&mut hasher, b"digest_uri", request.digest_uri.as_bytes());
    hash_prepared_field(
        &mut hasher,
        b"content_type",
        request.content_type.as_bytes(),
    );
    hash_prepared_field(&mut hasher, b"manifest_id", request.manifest_id.as_bytes());
    hash_prepared_field(&mut hasher, b"size", &request.size.to_be_bytes());
    hash_prepared_field(
        &mut hasher,
        b"chunk_count",
        &(request.chunks.len() as u64).to_be_bytes(),
    );
    for chunk in &request.chunks {
        hash_prepared_field(&mut hasher, b"chunk", &encode_chunk_manifest(chunk));
    }
    hash_prepared_field(
        &mut hasher,
        b"staged_count",
        &(request.staged.len() as u64).to_be_bytes(),
    );
    for object in request.staged.objects() {
        hash_prepared_field(&mut hasher, b"staged_key", object.key.as_str().as_bytes());
        hash_prepared_field(&mut hasher, b"staged_size", &object.size.to_be_bytes());
    }
    hash_prepared_field(&mut hasher, b"mode", &request.mode.to_be_bytes());
    hash_prepared_field(&mut hasher, b"uid", &request.uid.to_be_bytes());
    hash_prepared_field(&mut hasher, b"gid", &request.gid.to_be_bytes());
    finish_prepared_request_id(kind, hasher)
}

fn prepared_request_hasher(
    mount: MountId,
    prepared: &PreparedArtifact,
    domain: &[u8],
) -> (CommandKind, Sha256) {
    let kind = if prepared.replace {
        CommandKind::ReplaceArtifact
    } else {
        CommandKind::PublishArtifact
    };
    let mut hasher = Sha256::new();
    hash_prepared_field(&mut hasher, b"domain", domain);
    hash_prepared_field(&mut hasher, b"kind", kind_name(kind));
    hash_prepared_field(&mut hasher, b"mount", &mount.get().to_be_bytes());
    hash_prepared_field(&mut hasher, b"parent", &prepared.parent.get().to_be_bytes());
    hash_prepared_field(&mut hasher, b"name", prepared.name.as_bytes());
    hash_prepared_optional_bytes(
        &mut hasher,
        b"path",
        prepared.path.as_deref().map(str::as_bytes),
    );
    hash_prepared_field(&mut hasher, b"inode", &prepared.inode.get().to_be_bytes());
    hash_prepared_field(
        &mut hasher,
        b"generation",
        &prepared.generation.to_be_bytes(),
    );
    hash_prepared_field(&mut hasher, b"mtime_ms", &prepared.mtime_ms.to_be_bytes());
    hash_prepared_field(&mut hasher, b"ctime_ms", &prepared.ctime_ms.to_be_bytes());
    hash_prepared_field(&mut hasher, b"replace", &[u8::from(prepared.replace)]);
    hash_prepared_optional_u64(&mut hasher, b"dentry_version", prepared.dentry_version);
    hash_prepared_optional_u64(&mut hasher, b"old_generation", prepared.old_generation);
    hash_prepared_field(
        &mut hasher,
        b"object_gc_claim_version",
        &prepared.object_gc_claim_version.to_be_bytes(),
    );
    (kind, hasher)
}

fn finish_prepared_request_id(kind: CommandKind, hasher: Sha256) -> Vec<u8> {
    let prefix = kind_name(kind);
    let digest = hasher.finalize();
    let mut out =
        Vec::with_capacity(prefix.len() + PREPARED_ARTIFACT_REQUEST_ID_DOMAIN.len() + digest.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(PREPARED_ARTIFACT_REQUEST_ID_DOMAIN);
    out.extend_from_slice(&digest);
    out
}

fn hash_prepared_optional_u64(hasher: &mut Sha256, name: &[u8], value: Option<u64>) {
    match value {
        Some(value) => {
            hash_prepared_field(hasher, name, &[1]);
            hash_prepared_field(hasher, name, &value.to_be_bytes());
        }
        None => hash_prepared_field(hasher, name, &[0]),
    }
}

fn hash_prepared_optional_bytes(hasher: &mut Sha256, name: &[u8], value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hash_prepared_field(hasher, name, &[1]);
            hash_prepared_field(hasher, name, value);
        }
        None => hash_prepared_field(hasher, name, &[0]),
    }
}

fn hash_prepared_field(hasher: &mut Sha256, name: &[u8], value: &[u8]) {
    hasher.update((name.len() as u64).to_be_bytes());
    hasher.update(name);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}
