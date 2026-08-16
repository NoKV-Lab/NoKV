/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};

use nokv_object::{
    plan_artifact_upload, read_artifact_window, upload_artifact_from_plan, verify_artifact_bytes,
    ArtifactBlock, ArtifactBlockCache, ArtifactKeyspace, ArtifactManifest, ArtifactObjectStore,
    ArtifactReadStats, ArtifactReadWindow, ArtifactUploadOptions, ArtifactUploadPlan,
    ArtifactUploadStats, ObjectError, ObjectKey, DEFAULT_ARTIFACT_BLOCK_SIZE,
};
use nokv_protocol::{
    parse_sha256_digest_uri, seal_artifact_publish_plan, sha256_digest_uri,
    AbortArtifactPublishRequest, AppendSegment, ArtifactDescriptor, ArtifactManifestRow,
    ArtifactRevisionIdentity, BeginArtifactPublishRequest, ByteRange,
    CompleteArtifactPublishRequest, ContentType, Digest, ErrorCode, FieldValue,
    GetOperationRequest, GetPathRequest, LogicalShardIdentity, MarkArtifactObjectsUploadedRequest,
    ObjectIdentity, ObjectUploadProof, OperationIdentity, OperationKind, OperationResult,
    OperationState, OperationStatus, OperationToken, PageRequest, PathMetadata, PathReadResult,
    PublicationAuthority, PublishCondition, PublishResult, RootRoute, StageArtifactManifestRequest,
    StageArtifactObjectsRequest, StagedObject, WorkspacePath, WorkspaceReadView, WorkspaceRequest,
    WorkspaceResult, MAX_ARTIFACT_DEPENDENCY_DEPTH, MAX_ARTIFACT_DEPENDENCY_OWNERS,
    MAX_ARTIFACT_PUBLISH_BATCH_ROWS, MAX_ARTIFACT_READ_PLAN_ROWS,
};
use nokv_types::{ArtifactRevisionId, LogicalShardId, RootId};
use sha2::{Digest as _, Sha256};

use crate::{
    ArtifactPublishStage, ClientCall, ClientError, RouteResolver, RpcTransport, WorkspaceClient,
};

const ARTIFACT_READ_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

/// Complete caller-owned identity and metadata for one immutable publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPublishOptions {
    pub operation_id: OperationIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub target: WorkspacePath,
    pub authority: PublicationAuthority,
    pub condition: PublishCondition,
    pub content_type: ContentType,
    pub producer: Option<String>,
    pub manifest_identity: Option<String>,
    pub index_fields: Vec<FieldValue>,
    pub block_size: usize,
}

impl ArtifactPublishOptions {
    pub fn new(
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        target: WorkspacePath,
        condition: PublishCondition,
        content_type: ContentType,
    ) -> Self {
        Self {
            operation_id,
            artifact_revision_id,
            target,
            authority: PublicationAuthority::Visible,
            condition,
            content_type,
            producer: None,
            manifest_identity: None,
            index_fields: Vec::new(),
            block_size: DEFAULT_ARTIFACT_BLOCK_SIZE,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn with_authority(mut self, authority: PublicationAuthority) -> Self {
        self.authority = authority;
        self
    }

    pub fn with_producer(mut self, producer: impl Into<String>) -> Self {
        self.producer = Some(producer.into());
        self
    }

    pub fn with_manifest_identity(mut self, manifest_identity: impl Into<String>) -> Self {
        self.manifest_identity = Some(manifest_identity.into());
        self
    }

    pub fn with_index_fields(mut self, index_fields: Vec<FieldValue>) -> Self {
        self.index_fields = index_fields;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPublishOutcome {
    pub publication: ClientCall<PublishResult>,
    pub upload_stats: ArtifactUploadStats,
}

/// Caller-owned identity seed and policy for one logical append.
///
/// A conflicting create/append race derives deterministic attempt identities
/// from the supplied identities. Retrying this method with the same options
/// therefore replays the same attempt sequence instead of applying the delta a
/// second time after response loss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactAppendOptions {
    pub operation_id: OperationIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub target: WorkspacePath,
    /// Explicit content-type override for an existing artifact.
    pub content_type: Option<ContentType>,
    /// Payload-derived content type used only when append creates the path.
    pub create_content_type: ContentType,
    pub block_size: usize,
    pub max_logical_size: Option<u64>,
}

impl ArtifactAppendOptions {
    pub fn new(
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        target: WorkspacePath,
        create_content_type: ContentType,
    ) -> Self {
        Self {
            operation_id,
            artifact_revision_id,
            target,
            content_type: None,
            create_content_type,
            block_size: DEFAULT_ARTIFACT_BLOCK_SIZE,
            max_logical_size: None,
        }
    }

    pub fn with_content_type(mut self, content_type: ContentType) -> Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn with_max_logical_size(mut self, max_logical_size: u64) -> Self {
        self.max_logical_size = Some(max_logical_size);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactAppendOutcome {
    pub publication: ClientCall<PublishResult>,
    /// Complete resulting descriptor, including an inherited content type.
    pub descriptor: ArtifactDescriptor,
    pub upload_stats: ArtifactUploadStats,
    pub base_read_stats: ArtifactReadStats,
    pub created: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactReadOutcome {
    pub metadata: PathMetadata,
    pub bytes: Vec<u8>,
    pub stats: ArtifactReadStats,
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// Publish one immutable artifact through the sole ordered data path.
    ///
    /// Planning and descriptor validation complete before `Begin`. All planned
    /// object rows are durable before the first provider write. Failures after
    /// `Begin` never delete objects directly; the durable publication operation
    /// is driven to `Abort` or reports that abort could not be confirmed.
    pub fn publish_artifact(
        &self,
        store: &dyn ArtifactObjectStore,
        options: ArtifactPublishOptions,
        bytes: &[u8],
    ) -> Result<ArtifactPublishOutcome, ClientError> {
        if matches!(options.condition, PublishCondition::Append { .. }) {
            return Err(ClientError::InvalidOptions(
                "high-level append publication requires a sealed base-manifest plan and is not \
                 implemented"
                    .to_owned(),
            ));
        }

        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;
        let logical_shard = route.logical_shard_id;
        let object_plan = plan_artifact_upload(
            ArtifactUploadOptions::new(
                LogicalShardId::from(logical_shard),
                RootId::from(self.root_id()),
                ArtifactRevisionId::from(options.artifact_revision_id),
            )
            .with_block_size(options.block_size),
            bytes,
        )?;
        let (staged_objects, manifest_rows) =
            publication_rows(options.artifact_revision_id, &object_plan.manifest)?;
        let dependencies = Vec::new();
        let seals = seal_artifact_publish_plan(
            options.artifact_revision_id,
            &staged_objects,
            &manifest_rows,
        )?;
        let descriptor = ArtifactDescriptor {
            logical_size: object_plan.manifest.logical_len,
            body_digest: sha256_digest_uri(Digest(object_plan.manifest.sha256)),
            manifest_digest: sha256_digest_uri(seals.manifest_seal),
            content_type: options.content_type.clone(),
            producer: options.producer.clone(),
            manifest_identity: options.manifest_identity.clone(),
            index_fields: options.index_fields.clone(),
        };
        self.publish_artifact_plan(
            store,
            logical_shard,
            options.operation_id,
            options.artifact_revision_id,
            options.target,
            options.authority,
            options.condition,
            object_plan,
            staged_objects,
            manifest_rows,
            dependencies,
            descriptor,
            bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_artifact_plan(
        &self,
        store: &dyn ArtifactObjectStore,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        target: WorkspacePath,
        authority: PublicationAuthority,
        condition: PublishCondition,
        object_plan: ArtifactUploadPlan,
        staged_objects: Vec<StagedObject>,
        manifest_rows: Vec<ArtifactManifestRow>,
        dependencies: Vec<ArtifactRevisionIdentity>,
        descriptor: ArtifactDescriptor,
        bytes: &[u8],
    ) -> Result<ArtifactPublishOutcome, ClientError> {
        let seals =
            seal_artifact_publish_plan(artifact_revision_id, &staged_objects, &manifest_rows)?;
        descriptor.validate()?;
        let begin = self.begin_publish_on_shard(
            logical_shard,
            BeginArtifactPublishRequest {
                operation_id,
                artifact_revision_id,
                target,
                authority,
                condition,
                staged_object_count: seals.staged_object_count,
                staged_object_seal: seals.staged_object_seal,
                manifest_row_count: seals.manifest_row_count,
                manifest_seal: seals.manifest_seal,
                dependency_owner_revision_ids: dependencies.clone(),
            },
        );
        let (mut token, resume, begin_replayed) = match begin {
            Ok(call) => {
                let (replayed, commit_version) = (call.replayed, call.commit_version);
                let status = match validated_publish_status(call.value, operation_id) {
                    Ok(status) => status,
                    Err(source) => {
                        return Err(self.failed_publication(
                            logical_shard,
                            operation_id,
                            None,
                            ArtifactPublishStage::Begin,
                            source,
                        ));
                    }
                };
                // Begin observes whichever durable state the operation row is
                // already in, so every state the engine can declare is handled
                // here rather than collapsed into one running-state assertion.
                match status.state {
                    OperationState::Succeeded => {
                        // The engine compares the operation identity and
                        // initialization digests before it replays a terminal
                        // row, so this record describes exactly this
                        // publication: same target, same revision, same bytes.
                        // Nothing was staged or uploaded on this attempt.
                        let value = match published_result_from_status(&status) {
                            Ok(value) => value,
                            Err(source) => {
                                return Err(self.failed_publication(
                                    logical_shard,
                                    operation_id,
                                    None,
                                    ArtifactPublishStage::Begin,
                                    source,
                                ));
                            }
                        };
                        // The replay branch is the one path the engine takes
                        // without re-checking the live path claim, so a
                        // visible publication whose path was later removed and
                        // reclaimed by another writer would otherwise be
                        // reported as current. Commit and restore both re-read
                        // live state before returning a replayed terminal
                        // result; publish does the same rather than handing
                        // back a generation the caller could CAS against.
                        if matches!(authority, PublicationAuthority::Visible) {
                            if let Err(source) =
                                self.confirm_replayed_publication_is_live(logical_shard, &value)
                            {
                                return Err(ClientError::ArtifactPublishFailed {
                                    stage: ArtifactPublishStage::Begin,
                                    source: Box::new(source),
                                    abort_failure: None,
                                });
                            }
                        }
                        return Ok(ArtifactPublishOutcome {
                            publication: ClientCall {
                                value,
                                commit_version,
                                replayed: true,
                            },
                            upload_stats: ArtifactUploadStats::default(),
                        });
                    }
                    OperationState::Aborting
                    | OperationState::Failed
                    | OperationState::Quarantined => {
                        // The operation identity is durably spent. Aborting it
                        // again is meaningless, so report the terminal state
                        // instead of attempting one.
                        return Err(ClientError::ArtifactPublishFailed {
                            stage: ArtifactPublishStage::Begin,
                            source: Box::new(ClientError::ResponseMismatch(format!(
                                "artifact publication operation is durably {:?} and cannot be \
                                 resumed; retry with a new operation identity",
                                status.state
                            ))),
                            abort_failure: None,
                        });
                    }
                    OperationState::Running => match running_publish_resume(
                        status,
                        operation_id,
                        staged_objects.len(),
                        manifest_rows.len(),
                    ) {
                        Ok(resume) => (resume.token, resume, replayed),
                        Err(source) => {
                            return Err(self.failed_publication(
                                logical_shard,
                                operation_id,
                                None,
                                ArtifactPublishStage::Begin,
                                source,
                            ));
                        }
                    },
                }
            }
            Err(source) if is_definitive_append_race(&source) => return Err(source),
            Err(source) => {
                return Err(self.failed_publication(
                    logical_shard,
                    operation_id,
                    None,
                    ArtifactPublishStage::Begin,
                    source,
                ));
            }
        };

        for batch in
            staged_objects[resume.staged_object_cursor..].chunks(MAX_ARTIFACT_PUBLISH_BATCH_ROWS)
        {
            let request = StageArtifactObjectsRequest {
                token,
                objects: batch.to_vec(),
            };
            match self.publish_status_on_shard(
                logical_shard,
                WorkspaceRequest::StageArtifactObjects(request),
            ) {
                Ok(status) => match running_publish_token(status.value, operation_id) {
                    Ok(next_token) => token = next_token,
                    Err(source) => {
                        return Err(self.failed_publication(
                            logical_shard,
                            operation_id,
                            Some(token),
                            ArtifactPublishStage::StageObjects,
                            source,
                        ));
                    }
                },
                Err(source) => {
                    return Err(self.failed_publication(
                        logical_shard,
                        operation_id,
                        Some(token),
                        ArtifactPublishStage::StageObjects,
                        source,
                    ));
                }
            }
        }

        let upload = match upload_artifact_from_plan(store, &object_plan, bytes) {
            Ok(upload) => upload,
            Err(source) => {
                return Err(self.failed_publication(
                    logical_shard,
                    operation_id,
                    Some(token),
                    ArtifactPublishStage::UploadObjects,
                    ClientError::ArtifactUpload(Box::new(source)),
                ));
            }
        };

        let upload_proofs = upload_proofs(&staged_objects);
        for batch in
            upload_proofs[resume.uploaded_object_cursor..].chunks(MAX_ARTIFACT_PUBLISH_BATCH_ROWS)
        {
            let request = MarkArtifactObjectsUploadedRequest {
                token,
                objects: batch.to_vec(),
            };
            match self.publish_status_on_shard(
                logical_shard,
                WorkspaceRequest::MarkArtifactObjectsUploaded(request),
            ) {
                Ok(status) => match running_publish_token(status.value, operation_id) {
                    Ok(next_token) => token = next_token,
                    Err(source) => {
                        return Err(self.failed_publication(
                            logical_shard,
                            operation_id,
                            Some(token),
                            ArtifactPublishStage::MarkObjectsUploaded,
                            source,
                        ));
                    }
                },
                Err(source) => {
                    return Err(self.failed_publication(
                        logical_shard,
                        operation_id,
                        Some(token),
                        ArtifactPublishStage::MarkObjectsUploaded,
                        source,
                    ));
                }
            }
        }

        for batch in manifest_rows[resume.manifest_cursor..].chunks(MAX_ARTIFACT_PUBLISH_BATCH_ROWS)
        {
            let request = StageArtifactManifestRequest {
                token,
                rows: batch.to_vec(),
                dependency_owner_revision_ids: dependencies.clone(),
            };
            match self.publish_status_on_shard(
                logical_shard,
                WorkspaceRequest::StageArtifactManifest(request),
            ) {
                Ok(status) => match running_publish_token(status.value, operation_id) {
                    Ok(next_token) => token = next_token,
                    Err(source) => {
                        return Err(self.failed_publication(
                            logical_shard,
                            operation_id,
                            Some(token),
                            ArtifactPublishStage::StageManifest,
                            source,
                        ));
                    }
                },
                Err(source) => {
                    return Err(self.failed_publication(
                        logical_shard,
                        operation_id,
                        Some(token),
                        ArtifactPublishStage::StageManifest,
                        source,
                    ));
                }
            }
        }

        let complete = CompleteArtifactPublishRequest {
            token,
            artifact: descriptor,
        };
        let complete_result = self.execute_on_logical_shard(
            self.new_request_id(),
            WorkspaceRequest::CompleteArtifactPublish(complete),
            logical_shard,
        );
        let mut publication = match complete_result.and_then(|call| call.map(expect_published)) {
            Ok(publication) => publication,
            Err(source) => match self.recover_completed_publish(logical_shard, operation_id) {
                Ok(Some(recovered)) => recovered,
                Ok(None) => {
                    return Err(self.failed_publication(
                        logical_shard,
                        operation_id,
                        Some(token),
                        ArtifactPublishStage::Complete,
                        source,
                    ));
                }
                Err(recovery_error) => {
                    return Err(self.failed_publication(
                        logical_shard,
                        operation_id,
                        Some(token),
                        ArtifactPublishStage::Complete,
                        recovery_error,
                    ));
                }
            },
        };
        publication.replayed |= begin_replayed || resume.completed_rows != 0;

        Ok(ArtifactPublishOutcome {
            publication,
            upload_stats: upload.stats,
        })
    }

    /// Append one immutable delta, creating the path when it is absent.
    ///
    /// Within the dependency bounds, existing manifest rows are borrowed as
    /// direct physical-owner references and only `delta` objects are staged.
    /// Before `Begin`, an append that would exceed either bound instead reads
    /// and verifies the complete base, then rematerializes the resulting body
    /// into dependency-free objects owned by the new revision. Both paths keep
    /// append generation CAS and publish through the same durable pipeline.
    pub fn append_artifact(
        &self,
        store: &dyn ArtifactObjectStore,
        options: ArtifactAppendOptions,
        delta: &[u8],
    ) -> Result<ArtifactAppendOutcome, ClientError> {
        for attempt in 0..self.max_attempts() {
            let (operation_id, artifact_revision_id) = append_attempt_identities(
                options.operation_id,
                options.artifact_revision_id,
                attempt,
            );
            match self.append_artifact_attempt(
                store,
                &options,
                operation_id,
                artifact_revision_id,
                delta,
            ) {
                Err(error)
                    if is_append_retry_error(&error) && attempt + 1 < self.max_attempts() =>
                {
                    continue;
                }
                Err(error) if is_append_retry_error(&error) => {
                    return Err(ClientError::RetryExhausted {
                        attempts: attempt + 1,
                        last_error: Box::new(error),
                    });
                }
                result => return result,
            }
        }
        unreachable!("validated max_attempts is non-zero")
    }

    fn append_artifact_attempt(
        &self,
        store: &dyn ArtifactObjectStore,
        options: &ArtifactAppendOptions,
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        delta: &[u8],
    ) -> Result<ArtifactAppendOutcome, ClientError> {
        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;
        let logical_shard = route.logical_shard_id;
        let metadata = match self.load_artifact_metadata(
            logical_shard,
            &options.target,
            WorkspaceReadView::Live,
        ) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => None,
            Err(error) => return Err(error),
        };

        let delta_len = u64::try_from(delta.len()).map_err(|_| {
            ClientError::InvalidOptions("append delta length exceeds u64".to_owned())
        })?;
        let (
            condition,
            object_plan,
            staged_objects,
            manifest_rows,
            dependencies,
            descriptor,
            base_read_stats,
            created,
            rematerialized_body,
        ) = match metadata {
            None => {
                enforce_append_size(delta_len, options.max_logical_size)?;
                let object_plan = plan_artifact_upload(
                    ArtifactUploadOptions::new(
                        LogicalShardId::from(logical_shard),
                        RootId::from(self.root_id()),
                        ArtifactRevisionId::from(artifact_revision_id),
                    )
                    .with_block_size(options.block_size),
                    delta,
                )?;
                let (staged_objects, manifest_rows) =
                    publication_rows(artifact_revision_id, &object_plan.manifest)?;
                let seals = seal_artifact_publish_plan(
                    artifact_revision_id,
                    &staged_objects,
                    &manifest_rows,
                )?;
                let descriptor = ArtifactDescriptor {
                    logical_size: delta_len,
                    body_digest: sha256_digest_uri(Digest(object_plan.manifest.sha256)),
                    manifest_digest: sha256_digest_uri(seals.manifest_seal),
                    content_type: options
                        .content_type
                        .clone()
                        .unwrap_or_else(|| options.create_content_type.clone()),
                    producer: None,
                    manifest_identity: None,
                    index_fields: Vec::new(),
                };
                (
                    PublishCondition::CreateOnly,
                    object_plan,
                    staged_objects,
                    manifest_rows,
                    Vec::new(),
                    descriptor,
                    ArtifactReadStats::default(),
                    true,
                    None,
                )
            }
            Some(metadata) => {
                let logical_size = metadata
                    .descriptor
                    .logical_size
                    .checked_add(delta_len)
                    .ok_or_else(|| {
                        ClientError::InvalidOptions(
                            "append result logical size overflows u64".to_owned(),
                        )
                    })?;
                enforce_append_size(logical_size, options.max_logical_size)?;

                let base_rows = if metadata.descriptor.logical_size == 0 {
                    Vec::new()
                } else {
                    self.load_artifact_range_rows(
                        logical_shard,
                        &options.target,
                        WorkspaceReadView::Live,
                        ByteRange {
                            offset: 0,
                            length: metadata.descriptor.logical_size,
                        },
                        Some(&metadata),
                    )?
                    .rows
                };
                let base_manifest =
                    manifest_from_rows(self.root_id(), logical_shard, &metadata, &base_rows)?;
                let dependency_plan =
                    append_dependency_plan(&metadata, artifact_revision_id, &base_rows)?;
                let condition = PublishCondition::Append {
                    expected_generation: Some(metadata.generation),
                };

                if dependency_plan.requires_rematerialization {
                    let (mut body, base_read_stats) =
                        materialize_artifact_body(store, &base_manifest)?;
                    body.try_reserve(delta.len()).map_err(|_| {
                        ClientError::InvalidOptions(
                            "append result cannot be materialized in client memory".to_owned(),
                        )
                    })?;
                    body.extend_from_slice(delta);
                    let object_plan = plan_artifact_upload(
                        ArtifactUploadOptions::new(
                            LogicalShardId::from(logical_shard),
                            RootId::from(self.root_id()),
                            ArtifactRevisionId::from(artifact_revision_id),
                        )
                        .with_block_size(options.block_size),
                        &body,
                    )?;
                    let (staged_objects, manifest_rows) =
                        publication_rows(artifact_revision_id, &object_plan.manifest)?;
                    let seals = seal_artifact_publish_plan(
                        artifact_revision_id,
                        &staged_objects,
                        &manifest_rows,
                    )?;
                    let mut descriptor = metadata.descriptor;
                    descriptor.logical_size = logical_size;
                    descriptor.body_digest = sha256_digest_uri(Digest(object_plan.manifest.sha256));
                    descriptor.manifest_digest = sha256_digest_uri(seals.manifest_seal);
                    if let Some(content_type) = &options.content_type {
                        descriptor.content_type = content_type.clone();
                    }
                    (
                        condition,
                        object_plan,
                        staged_objects,
                        manifest_rows,
                        Vec::new(),
                        descriptor,
                        base_read_stats,
                        false,
                        Some(body),
                    )
                } else {
                    let object_plan = plan_artifact_upload(
                        ArtifactUploadOptions::new(
                            LogicalShardId::from(logical_shard),
                            RootId::from(self.root_id()),
                            ArtifactRevisionId::from(artifact_revision_id),
                        )
                        .with_block_size(options.block_size),
                        delta,
                    )?;
                    let (staged_objects, mut delta_rows) =
                        publication_rows(artifact_revision_id, &object_plan.manifest)?;
                    let (mut body_hasher, base_read_stats) =
                        stream_artifact_digest(store, &base_manifest)?;
                    body_hasher.update(delta);
                    let body_digest = Digest(body_hasher.finalize().into());

                    let first_delta_index = u64::try_from(base_rows.len()).map_err(|_| {
                        ClientError::InvalidOptions(
                            "base manifest row count exceeds u64".to_owned(),
                        )
                    })?;
                    let segment_sequence = next_append_segment_sequence(&base_rows)?;
                    for row in &mut delta_rows {
                        let segment_offset = row.logical_offset;
                        row.object_index = row
                            .object_index
                            .checked_add(first_delta_index)
                            .ok_or_else(|| {
                                ClientError::InvalidOptions(
                                    "append manifest object index overflows".to_owned(),
                                )
                            })?;
                        row.logical_offset = row
                            .logical_offset
                            .checked_add(metadata.descriptor.logical_size)
                            .ok_or_else(|| {
                                ClientError::InvalidOptions(
                                    "append manifest logical offset overflows".to_owned(),
                                )
                            })?;
                        row.append_segment = Some(AppendSegment {
                            segment_sequence,
                            segment_offset,
                        });
                    }

                    let mut manifest_rows = base_rows;
                    manifest_rows.extend(delta_rows);
                    let seals = seal_artifact_publish_plan(
                        artifact_revision_id,
                        &staged_objects,
                        &manifest_rows,
                    )?;
                    let mut descriptor = metadata.descriptor;
                    descriptor.logical_size = logical_size;
                    descriptor.body_digest = sha256_digest_uri(body_digest);
                    descriptor.manifest_digest = sha256_digest_uri(seals.manifest_seal);
                    if let Some(content_type) = &options.content_type {
                        descriptor.content_type = content_type.clone();
                    }
                    (
                        condition,
                        object_plan,
                        staged_objects,
                        manifest_rows,
                        dependency_plan.owners,
                        descriptor,
                        base_read_stats,
                        false,
                        None,
                    )
                }
            }
        };

        let result_descriptor = descriptor.clone();
        let upload_bytes = rematerialized_body.as_deref().unwrap_or(delta);
        let outcome = self.publish_artifact_plan(
            store,
            logical_shard,
            operation_id,
            artifact_revision_id,
            options.target.clone(),
            PublicationAuthority::Visible,
            condition,
            object_plan,
            staged_objects,
            manifest_rows,
            dependencies,
            descriptor,
            upload_bytes,
        )?;
        Ok(ArtifactAppendOutcome {
            publication: outcome.publication,
            descriptor: result_descriptor,
            upload_stats: outcome.upload_stats,
            base_read_stats,
            created,
        })
    }

    pub fn read_artifact(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        for attempt in 1..=self.max_attempts() {
            match self.read_artifact_once(store, cache, target.clone(), view.clone()) {
                Err(error) if error.retryable() && attempt < self.max_attempts() => {}
                Err(error) if error.retryable() => {
                    return Err(ClientError::RetryExhausted {
                        attempts: attempt,
                        last_error: Box::new(error),
                    });
                }
                result => return result,
            }
        }
        unreachable!("validated max_attempts is non-zero")
    }

    pub fn read_artifact_range(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
        offset: u64,
        len: usize,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        for attempt in 1..=self.max_attempts() {
            match self.read_artifact_range_once(
                store,
                cache,
                target.clone(),
                view.clone(),
                offset,
                len,
            ) {
                Err(error) if error.retryable() && attempt < self.max_attempts() => {}
                Err(error) if error.retryable() => {
                    return Err(ClientError::RetryExhausted {
                        attempts: attempt,
                        last_error: Box::new(error),
                    });
                }
                result => return result,
            }
        }
        unreachable!("validated max_attempts is non-zero")
    }

    fn begin_publish_on_shard(
        &self,
        logical_shard: LogicalShardIdentity,
        request: BeginArtifactPublishRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.publish_status_on_shard(
            logical_shard,
            WorkspaceRequest::BeginArtifactPublish(request),
        )
    }

    fn publish_status_on_shard(
        &self,
        logical_shard: LogicalShardIdentity,
        operation: WorkspaceRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute_on_logical_shard(self.new_request_id(), operation, logical_shard)?
            .map(expect_operation)
    }

    fn load_operation_on_shard(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute_on_logical_shard(
            self.new_request_id(),
            WorkspaceRequest::GetOperation(GetOperationRequest { operation_id }),
            logical_shard,
        )?
        .map(expect_operation)
    }

    fn failed_publication(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        known_token: Option<OperationToken>,
        stage: ArtifactPublishStage,
        source: ClientError,
    ) -> ClientError {
        let abort_failure = self
            .durable_abort(logical_shard, operation_id, known_token, stage)
            .err()
            .map(Box::new);
        ClientError::ArtifactPublishFailed {
            stage,
            source: Box::new(source),
            abort_failure,
        }
    }

    fn durable_abort(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        known_token: Option<OperationToken>,
        stage: ArtifactPublishStage,
    ) -> Result<(), ClientError> {
        let token = match self.load_operation_on_shard(logical_shard, operation_id) {
            Ok(status) => {
                let status = validated_publish_status(status.value, operation_id)?;
                match status.state {
                    OperationState::Aborting
                    | OperationState::Failed
                    | OperationState::Quarantined => return Ok(()),
                    OperationState::Running => status.token,
                    OperationState::Succeeded => {
                        return Err(ClientError::ResponseMismatch(
                            "cannot abort an artifact publication that already succeeded"
                                .to_owned(),
                        ));
                    }
                }
            }
            Err(load_error) => known_token.ok_or(load_error)?,
        };
        let status = self
            .publish_status_on_shard(
                logical_shard,
                WorkspaceRequest::AbortArtifactPublish(AbortArtifactPublishRequest {
                    token,
                    reason: format!("client publication failed during {stage}"),
                }),
            )?
            .value;
        let status = validated_publish_status(status, operation_id)?;
        match status.state {
            OperationState::Aborting | OperationState::Failed | OperationState::Quarantined => {
                Ok(())
            }
            OperationState::Running | OperationState::Succeeded => {
                Err(ClientError::ResponseMismatch(
                    "abort response did not durably enter an abort or terminal failure state"
                        .to_owned(),
                ))
            }
        }
    }

    /// Confirm a replayed publication still describes the live path.
    ///
    /// `begin_publish` returns a terminal operation row after comparing its
    /// digests, without re-evaluating the path claim, so a create-only
    /// publication whose path was later removed and reclaimed replays as
    /// succeeded while the path holds a different revision. Returning that
    /// record unchecked would hand the caller a generation to compare-and-swap
    /// against a revision that is not theirs.
    fn confirm_replayed_publication_is_live(
        &self,
        logical_shard: LogicalShardIdentity,
        published: &PublishResult,
    ) -> Result<(), ClientError> {
        let metadata = self
            .load_artifact_metadata(logical_shard, &published.target, WorkspaceReadView::Live)
            .map_err(|source| match source {
                ClientError::Rpc(failure) if failure.code == ErrorCode::NotFound => {
                    ClientError::ResponseMismatch(
                        "this publication succeeded earlier but its path no longer exists; the \
                         replayed result cannot describe live state"
                            .to_owned(),
                    )
                }
                other => other,
            })?;
        if metadata.artifact_revision_id != published.artifact_revision_id {
            return Err(ClientError::ResponseMismatch(
                "this publication succeeded earlier but its path now holds a different revision; \
                 the replayed result cannot describe live state"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn recover_completed_publish(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
    ) -> Result<Option<ClientCall<PublishResult>>, ClientError> {
        let operation = match self.load_operation_on_shard(logical_shard, operation_id) {
            Ok(operation) => operation,
            Err(_) => return Ok(None),
        };
        let status = validated_publish_status(operation.value, operation_id)?;
        if status.state != OperationState::Succeeded {
            return Ok(None);
        }
        let result = published_result_from_status(&status)?;
        Ok(Some(ClientCall {
            value: result,
            commit_version: operation.commit_version,
            replayed: operation.replayed,
        }))
    }

    fn read_artifact_once(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;
        let metadata =
            self.load_artifact_metadata(route.logical_shard_id, &target, view.clone())?;
        let logical_len = metadata.descriptor.logical_size;
        let output_len = usize::try_from(logical_len).map_err(|_| {
            ClientError::ArtifactIntegrity("artifact length is not addressable".to_owned())
        })?;
        if logical_len == 0 {
            let manifest =
                manifest_from_rows(self.root_id(), route.logical_shard_id, &metadata, &[])?;
            verify_artifact_bytes(&manifest, &[])?;
            return Ok(ArtifactReadOutcome {
                metadata,
                bytes: Vec::new(),
                stats: ArtifactReadStats::default(),
            });
        }

        let mut bytes = Vec::with_capacity(output_len);
        let mut stats = ArtifactReadStats::default();
        let mut complete_rows = BTreeMap::<u64, ArtifactManifestRow>::new();
        let mut offset = 0_u64;
        while offset < logical_len {
            let window_len_u64 = (logical_len - offset).min(ARTIFACT_READ_WINDOW_BYTES);
            let window_len = usize::try_from(window_len_u64).map_err(|_| {
                ClientError::ArtifactIntegrity("read window length is not addressable".to_owned())
            })?;
            let loaded = self.load_artifact_range_rows(
                route.logical_shard_id,
                &target,
                view.clone(),
                ByteRange {
                    offset,
                    length: window_len_u64,
                },
                Some(&metadata),
            )?;
            let window = window_from_rows(
                self.root_id(),
                route.logical_shard_id,
                &loaded.metadata,
                &loaded.rows,
                offset,
                window_len,
            )?;
            let read = read_artifact_window(store, cache, &window, offset, window_len)?;
            bytes.extend_from_slice(&read.bytes);
            merge_read_stats(&mut stats, read.stats);
            for row in loaded.rows {
                match complete_rows.entry(row.object_index) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(row);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &row => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(ClientError::ArtifactReadFenceChanged);
                    }
                }
            }
            offset = offset.checked_add(window_len_u64).ok_or_else(|| {
                ClientError::ArtifactIntegrity("full-read offset overflows".to_owned())
            })?;
        }

        let rows = complete_rows.into_values().collect::<Vec<_>>();
        let manifest =
            manifest_from_rows(self.root_id(), route.logical_shard_id, &metadata, &rows)?;
        verify_artifact_bytes(&manifest, &bytes)?;
        Ok(ArtifactReadOutcome {
            metadata,
            bytes,
            stats,
        })
    }

    fn read_artifact_range_once(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
        offset: u64,
        len: usize,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;
        let length = u64::try_from(len).map_err(|_| {
            ClientError::ArtifactIntegrity("requested range length exceeds u64".to_owned())
        })?;
        let loaded = self.load_artifact_range_rows(
            route.logical_shard_id,
            &target,
            view,
            ByteRange { offset, length },
            None,
        )?;
        let window = window_from_rows(
            self.root_id(),
            route.logical_shard_id,
            &loaded.metadata,
            &loaded.rows,
            offset,
            len,
        )?;
        let read = read_artifact_window(store, cache, &window, offset, len)?;
        Ok(ArtifactReadOutcome {
            metadata: loaded.metadata,
            bytes: read.bytes,
            stats: read.stats,
        })
    }

    fn load_artifact_metadata(
        &self,
        logical_shard: LogicalShardIdentity,
        target: &WorkspacePath,
        view: WorkspaceReadView,
    ) -> Result<PathMetadata, ClientError> {
        let result = self
            .execute_on_logical_shard(
                self.new_request_id(),
                WorkspaceRequest::GetPath(GetPathRequest {
                    target: target.clone(),
                    view,
                    range: None,
                    plan_page: None,
                    if_none_match: None,
                }),
                logical_shard,
            )?
            .map(expect_path)?
            .value;
        validate_path_result(&result, target, None)?;
        result.metadata.ok_or_else(|| {
            ClientError::ResponseMismatch("artifact read omitted path metadata".to_owned())
        })
    }

    fn load_artifact_range_rows(
        &self,
        logical_shard: LogicalShardIdentity,
        target: &WorkspacePath,
        view: WorkspaceReadView,
        range: ByteRange,
        expected_metadata: Option<&PathMetadata>,
    ) -> Result<LoadedRangeRows, ClientError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut metadata = expected_metadata.cloned();
        let mut rows = Vec::new();

        loop {
            let result = self
                .execute_on_logical_shard(
                    self.new_request_id(),
                    WorkspaceRequest::GetPath(GetPathRequest {
                        target: target.clone(),
                        view: view.clone(),
                        range: Some(range),
                        plan_page: Some(PageRequest {
                            cursor: cursor.clone(),
                            limit: MAX_ARTIFACT_READ_PLAN_ROWS as u32,
                        }),
                        if_none_match: None,
                    }),
                    logical_shard,
                )?
                .map(expect_path)?
                .value;
            validate_path_result(&result, target, Some(range))?;
            let page_metadata = result.metadata.ok_or_else(|| {
                ClientError::ResponseMismatch("range plan omitted path metadata".to_owned())
            })?;
            if metadata
                .as_ref()
                .is_some_and(|expected| expected != &page_metadata)
            {
                return Err(ClientError::ArtifactReadFenceChanged);
            }
            metadata.get_or_insert_with(|| page_metadata.clone());
            append_range_page(&mut rows, result.blocks)?;

            let Some(next_cursor) = result.next_cursor else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(ClientError::ArtifactIntegrity(
                    "range-plan cursor loop detected".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }

        validate_range_coverage(&rows, range)?;
        Ok(LoadedRangeRows {
            metadata: metadata.expect("every valid range page includes metadata"),
            rows,
        })
    }
}

struct LoadedRangeRows {
    metadata: PathMetadata,
    rows: Vec<ArtifactManifestRow>,
}

fn validate_path_result(
    result: &PathReadResult,
    target: &WorkspacePath,
    expected_range: Option<ByteRange>,
) -> Result<(), ClientError> {
    if result.not_modified {
        return Err(ClientError::ResponseMismatch(
            "unconditional artifact read returned not-modified".to_owned(),
        ));
    }
    if result.range != expected_range {
        return Err(ClientError::ResponseMismatch(
            "artifact read response range differs from its request".to_owned(),
        ));
    }
    let metadata = result.metadata.as_ref().ok_or_else(|| {
        ClientError::ResponseMismatch("artifact read omitted path metadata".to_owned())
    })?;
    if &metadata.path != target {
        return Err(ClientError::ResponseMismatch(
            "artifact metadata belongs to another workspace path".to_owned(),
        ));
    }
    match expected_range {
        None if !result.blocks.is_empty() || result.next_cursor.is_some() => {
            Err(ClientError::ResponseMismatch(
                "metadata-only artifact response included a read plan".to_owned(),
            ))
        }
        Some(_) if result.blocks.is_empty() => Err(ClientError::ArtifactIntegrity(
            "range-plan page contains no manifest rows".to_owned(),
        )),
        _ => Ok(()),
    }
}

fn append_range_page(
    rows: &mut Vec<ArtifactManifestRow>,
    mut page: Vec<ArtifactManifestRow>,
) -> Result<(), ClientError> {
    if let (Some(previous), Some(first)) = (rows.last(), page.first()) {
        let previous_end = previous
            .logical_offset
            .checked_add(previous.length)
            .ok_or_else(|| {
                ClientError::ArtifactIntegrity("manifest row range overflows".to_owned())
            })?;
        if previous.object_index.checked_add(1) != Some(first.object_index)
            || first.logical_offset != previous_end
        {
            return Err(ClientError::ArtifactIntegrity(
                "range-plan pages have a gap, overlap, or object-index discontinuity".to_owned(),
            ));
        }
    }
    rows.append(&mut page);
    Ok(())
}

fn validate_range_coverage(
    rows: &[ArtifactManifestRow],
    range: ByteRange,
) -> Result<(), ClientError> {
    let range_end = range.offset.checked_add(range.length).ok_or_else(|| {
        ClientError::ArtifactIntegrity("requested range end overflows".to_owned())
    })?;
    let first = rows.first().ok_or_else(|| {
        ClientError::ArtifactIntegrity("range plan has no manifest rows".to_owned())
    })?;
    let first_end = first
        .logical_offset
        .checked_add(first.length)
        .ok_or_else(|| {
            ClientError::ArtifactIntegrity("first manifest row range overflows".to_owned())
        })?;
    if first.logical_offset > range.offset || first_end <= range.offset {
        return Err(ClientError::ArtifactIntegrity(
            "range plan does not cover the requested start".to_owned(),
        ));
    }

    let mut previous: Option<(u64, u64)> = None;
    for row in rows {
        let row_end = row.logical_offset.checked_add(row.length).ok_or_else(|| {
            ClientError::ArtifactIntegrity("manifest row range overflows".to_owned())
        })?;
        if previous.is_some_and(|(object_index, logical_end)| {
            object_index.checked_add(1) != Some(row.object_index)
                || logical_end != row.logical_offset
        }) {
            return Err(ClientError::ArtifactIntegrity(
                "range plan contains a gap, overlap, or object-index discontinuity".to_owned(),
            ));
        }
        previous = Some((row.object_index, row_end));
    }
    if previous.is_none_or(|(_, logical_end)| logical_end < range_end) {
        return Err(ClientError::ArtifactIntegrity(
            "range plan does not cover the requested end".to_owned(),
        ));
    }
    Ok(())
}

fn window_from_rows(
    root_id: nokv_protocol::RootIdentity,
    logical_shard_id: LogicalShardIdentity,
    metadata: &PathMetadata,
    rows: &[ArtifactManifestRow],
    offset: u64,
    len: usize,
) -> Result<ArtifactReadWindow, ClientError> {
    let length = u64::try_from(len).map_err(|_| {
        ClientError::ArtifactIntegrity("requested range length exceeds u64".to_owned())
    })?;
    validate_range_coverage(rows, ByteRange { offset, length })?;
    parse_sha256_digest_uri(&metadata.descriptor.body_digest)?;
    parse_sha256_digest_uri(&metadata.descriptor.manifest_digest)?;
    Ok(ArtifactReadWindow {
        logical_shard_id: LogicalShardId::from(logical_shard_id),
        root_id: RootId::from(root_id),
        artifact_revision_id: ArtifactRevisionId::from(metadata.artifact_revision_id),
        artifact_logical_len: metadata.descriptor.logical_size,
        blocks: blocks_from_rows(root_id, logical_shard_id, rows)?,
    })
}

fn blocks_from_rows(
    root_id: nokv_protocol::RootIdentity,
    logical_shard_id: LogicalShardIdentity,
    rows: &[ArtifactManifestRow],
) -> Result<Vec<ArtifactBlock>, ClientError> {
    let mut blocks = Vec::with_capacity(rows.len());
    for row in rows {
        if row.object_offset != 0 {
            return Err(ClientError::ArtifactIntegrity(format!(
                "manifest object {} addresses a packed object range; the Agent artifact path \
                 requires one immutable object per block",
                row.object_index
            )));
        }
        let owner_revision = ArtifactRevisionId::from(row.physical_owner_revision_id);
        let key = ObjectKey::new(row.object_identity.as_str())?;
        let keyspace = ArtifactKeyspace::new(
            LogicalShardId::from(logical_shard_id),
            RootId::from(root_id),
            owner_revision,
        );
        let key_object_index = keyspace.object_index(&key)?;
        if key_object_index != row.physical_object_index {
            return Err(ClientError::ArtifactIntegrity(format!(
                "manifest object {} declares physical index {} but its immutable key carries {}",
                row.object_index, row.physical_object_index, key_object_index
            )));
        }
        blocks.push(ArtifactBlock {
            owner_revision_id: owner_revision,
            object_index: row.physical_object_index,
            logical_offset: row.logical_offset,
            len: row.length,
            sha256: parse_sha256_digest_uri(&row.digest)?.0,
            key,
        });
    }
    Ok(blocks)
}

fn merge_read_stats(total: &mut ArtifactReadStats, next: ArtifactReadStats) {
    total.planned_blocks = total.planned_blocks.saturating_add(next.planned_blocks);
    total.cache_hits = total.cache_hits.saturating_add(next.cache_hits);
    total.cache_misses = total.cache_misses.saturating_add(next.cache_misses);
    total.store_reads = total.store_reads.saturating_add(next.store_reads);
    total.store_read_bytes = total.store_read_bytes.saturating_add(next.store_read_bytes);
    total.verified_blocks = total.verified_blocks.saturating_add(next.verified_blocks);
}

fn require_object_namespace(
    store: &dyn ArtifactObjectStore,
    route: RootRoute,
) -> Result<(), ClientError> {
    let expected: nokv_types::ObjectNamespaceId = route.object_namespace_id.into();
    match store.object_namespace() {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(ObjectError::ObjectNamespaceMismatch { expected, actual }.into()),
        None => Err(ClientError::InvalidOptions(
            "artifact object store was not verified against the root route namespace".to_owned(),
        )),
    }
}

fn stream_artifact_digest(
    store: &dyn ArtifactObjectStore,
    manifest: &ArtifactManifest,
) -> Result<(Sha256, ArtifactReadStats), ClientError> {
    stream_artifact_blocks(store, manifest, |_| Ok(()))
}

fn materialize_artifact_body(
    store: &dyn ArtifactObjectStore,
    manifest: &ArtifactManifest,
) -> Result<(Vec<u8>, ArtifactReadStats), ClientError> {
    let expected_len = usize::try_from(manifest.logical_len).map_err(|_| {
        ClientError::InvalidOptions(
            "append base is too large to rematerialize in client memory".to_owned(),
        )
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(expected_len).map_err(|_| {
        ClientError::InvalidOptions(
            "append base cannot be rematerialized in client memory".to_owned(),
        )
    })?;
    let (_, stats) = stream_artifact_blocks(store, manifest, |block| {
        bytes.extend_from_slice(block);
        Ok(())
    })?;
    if bytes.len() != expected_len {
        return Err(ClientError::ArtifactIntegrity(format!(
            "materialized base has {} bytes but descriptor declares {expected_len}",
            bytes.len()
        )));
    }
    Ok((bytes, stats))
}

fn stream_artifact_blocks(
    store: &dyn ArtifactObjectStore,
    manifest: &ArtifactManifest,
    mut consume: impl FnMut(&[u8]) -> Result<(), ClientError>,
) -> Result<(Sha256, ArtifactReadStats), ClientError> {
    let mut hasher = Sha256::new();
    let mut stats = ArtifactReadStats::default();
    for block in &manifest.blocks {
        let len = usize::try_from(block.len).map_err(|_| {
            ClientError::ArtifactIntegrity("artifact block length is not addressable".to_owned())
        })?;
        let window = ArtifactReadWindow {
            logical_shard_id: manifest.logical_shard_id,
            root_id: manifest.root_id,
            artifact_revision_id: manifest.artifact_revision_id,
            artifact_logical_len: manifest.logical_len,
            blocks: vec![block.clone()],
        };
        let read = read_artifact_window(store, None, &window, block.logical_offset, len)?;
        consume(&read.bytes)?;
        hasher.update(&read.bytes);
        merge_read_stats(&mut stats, read.stats);
    }
    let streamed_digest: [u8; 32] = hasher.clone().finalize().into();
    if streamed_digest != manifest.sha256 {
        return Err(ClientError::ArtifactIntegrity(
            "streamed base body digest does not match its descriptor".to_owned(),
        ));
    }
    Ok((hasher, stats))
}

fn next_append_segment_sequence(rows: &[ArtifactManifestRow]) -> Result<u32, ClientError> {
    rows.iter()
        .filter_map(|row| row.append_segment.map(|segment| segment.segment_sequence))
        .max()
        .map_or(Ok(0), |sequence| {
            sequence.checked_add(1).ok_or_else(|| {
                ClientError::InvalidOptions("append segment sequence overflows u32".to_owned())
            })
        })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AppendDependencyPlan {
    owners: Vec<ArtifactRevisionIdentity>,
    requires_rematerialization: bool,
}

fn append_dependency_plan(
    metadata: &PathMetadata,
    artifact_revision_id: ArtifactRevisionIdentity,
    rows: &[ArtifactManifestRow],
) -> Result<AppendDependencyPlan, ClientError> {
    if artifact_revision_id == metadata.artifact_revision_id {
        return Err(ClientError::InvalidOptions(
            "append revision identity must differ from the current revision".to_owned(),
        ));
    }
    let base_dependencies = manifest_dependency_owners(metadata.artifact_revision_id, rows);
    let observed_base_count = u32::try_from(base_dependencies.len()).map_err(|_| {
        ClientError::ArtifactIntegrity("base manifest dependency count exceeds u32".to_owned())
    })?;
    if observed_base_count != metadata.dependency_count {
        return Err(ClientError::ArtifactIntegrity(format!(
            "base manifest has {observed_base_count} dependency owners but revision metadata declares {}",
            metadata.dependency_count
        )));
    }

    let owners = manifest_dependency_owners(artifact_revision_id, rows);
    let base_owns_objects = rows
        .iter()
        .any(|row| row.physical_owner_revision_id == metadata.artifact_revision_id);
    let dependency_depth = if owners.is_empty() {
        0
    } else if base_owns_objects {
        metadata.dependency_depth.checked_add(1).ok_or_else(|| {
            ClientError::ArtifactIntegrity("append dependency depth overflows u8".to_owned())
        })?
    } else {
        metadata.dependency_depth
    };
    let requires_rematerialization = owners.len()
        > usize::try_from(MAX_ARTIFACT_DEPENDENCY_OWNERS)
            .expect("artifact dependency owner limit fits usize")
        || dependency_depth > MAX_ARTIFACT_DEPENDENCY_DEPTH;
    Ok(AppendDependencyPlan {
        owners,
        requires_rematerialization,
    })
}

fn manifest_dependency_owners(
    excluded_revision_id: ArtifactRevisionIdentity,
    rows: &[ArtifactManifestRow],
) -> Vec<ArtifactRevisionIdentity> {
    rows.iter()
        .filter_map(|row| {
            (row.physical_owner_revision_id != excluded_revision_id)
                .then_some(row.physical_owner_revision_id)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn enforce_append_size(logical_size: u64, maximum: Option<u64>) -> Result<(), ClientError> {
    if maximum.is_some_and(|maximum| logical_size > maximum) {
        return Err(ClientError::InvalidOptions(format!(
            "append result is {logical_size} bytes, maximum is {}",
            maximum.expect("maximum is present")
        )));
    }
    Ok(())
}

fn append_attempt_identities(
    operation_id: OperationIdentity,
    artifact_revision_id: ArtifactRevisionIdentity,
    attempt: u32,
) -> (OperationIdentity, ArtifactRevisionIdentity) {
    if attempt == 0 {
        return (operation_id, artifact_revision_id);
    }
    let derive = |domain: &[u8]| {
        let mut hasher = Sha256::new();
        hasher.update(domain);
        hasher.update(operation_id.0);
        hasher.update(artifact_revision_id.0);
        hasher.update(attempt.to_be_bytes());
        let digest = hasher.finalize();
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&digest[..16]);
        identity
    };
    (
        OperationIdentity(derive(b"nokv.append.operation-attempt.v1\0")),
        ArtifactRevisionIdentity(derive(b"nokv.append.revision-attempt.v1\0")),
    )
}

fn is_definitive_append_race(error: &ClientError) -> bool {
    [
        ErrorCode::Conflict,
        ErrorCode::AlreadyExists,
        ErrorCode::NotFound,
    ]
    .into_iter()
    .any(|code| error.rpc_code() == Some(code))
}

fn is_append_retry_error(error: &ClientError) -> bool {
    match error {
        ClientError::ArtifactReadFenceChanged => true,
        ClientError::Rpc(_) => is_definitive_append_race(error),
        ClientError::RetryExhausted { last_error, .. } => is_append_retry_error(last_error),
        ClientError::ArtifactPublishFailed {
            source,
            abort_failure: None,
            ..
        } => is_append_retry_error(source),
        ClientError::ArtifactPublishFailed {
            abort_failure: Some(_),
            ..
        }
        | ClientError::InvalidOptions(_)
        | ClientError::InvalidRoute(_)
        | ClientError::Transport(_)
        | ClientError::Protocol(_)
        | ClientError::ResponseMismatch(_)
        | ClientError::MissingCapabilities(_)
        | ClientError::ArtifactIntegrity(_)
        | ClientError::Object(_)
        | ClientError::ArtifactUpload(_) => false,
    }
}

fn publication_rows(
    artifact_revision_id: ArtifactRevisionIdentity,
    manifest: &ArtifactManifest,
) -> Result<(Vec<StagedObject>, Vec<ArtifactManifestRow>), ClientError> {
    let mut staged_objects = Vec::with_capacity(manifest.blocks.len());
    let mut manifest_rows = Vec::with_capacity(manifest.blocks.len());
    for block in &manifest.blocks {
        let sequence = u32::try_from(block.object_index).map_err(|_| {
            ClientError::InvalidOptions("artifact object count exceeds u32".to_owned())
        })?;
        let object_identity = ObjectIdentity::new(block.key.as_str())?;
        let digest = sha256_digest_uri(Digest(block.sha256));
        staged_objects.push(StagedObject {
            sequence,
            object_identity: object_identity.clone(),
            expected_length: block.len,
            expected_digest: digest.clone(),
            multipart_token: None,
        });
        manifest_rows.push(ArtifactManifestRow {
            object_index: block.object_index,
            physical_object_index: block.object_index,
            logical_offset: block.logical_offset,
            physical_owner_revision_id: artifact_revision_id,
            object_identity,
            object_offset: 0,
            length: block.len,
            digest,
            append_segment: None,
        });
    }
    Ok((staged_objects, manifest_rows))
}

fn upload_proofs(staged_objects: &[StagedObject]) -> Vec<ObjectUploadProof> {
    staged_objects
        .iter()
        .map(|object| ObjectUploadProof {
            sequence: object.sequence,
            observed_length: object.expected_length,
            observed_digest: object.expected_digest.clone(),
        })
        .collect()
}

fn manifest_from_rows(
    root_id: nokv_protocol::RootIdentity,
    logical_shard_id: LogicalShardIdentity,
    metadata: &PathMetadata,
    rows: &[ArtifactManifestRow],
) -> Result<ArtifactManifest, ClientError> {
    let current_revision = metadata.artifact_revision_id;
    let mut logical_offset = 0_u64;
    let mut dependencies = BTreeSet::new();
    for (ordinal, row) in (0_u64..).zip(rows) {
        if row.object_index != ordinal {
            return Err(ClientError::ArtifactIntegrity(format!(
                "manifest row {ordinal} has non-canonical object index {}",
                row.object_index
            )));
        }
        if row.logical_offset != logical_offset {
            return Err(ClientError::ArtifactIntegrity(format!(
                "manifest row {ordinal} starts at {}, expected {logical_offset}",
                row.logical_offset
            )));
        }
        logical_offset = logical_offset.checked_add(row.length).ok_or_else(|| {
            ClientError::ArtifactIntegrity("artifact logical length overflows".to_owned())
        })?;
        if row.physical_owner_revision_id != current_revision {
            dependencies.insert(row.physical_owner_revision_id);
        }
    }

    if logical_offset != metadata.descriptor.logical_size {
        return Err(ClientError::ArtifactIntegrity(format!(
            "manifest covers {logical_offset} bytes but descriptor declares {}",
            metadata.descriptor.logical_size
        )));
    }
    let body_digest = parse_sha256_digest_uri(&metadata.descriptor.body_digest)?;
    if dependencies.len()
        > usize::try_from(MAX_ARTIFACT_DEPENDENCY_OWNERS)
            .expect("artifact dependency owner limit fits usize")
    {
        return Err(ClientError::ArtifactIntegrity(format!(
            "artifact dependency closure exceeds {} owner revisions",
            MAX_ARTIFACT_DEPENDENCY_OWNERS
        )));
    }
    let seals = seal_artifact_publish_plan(current_revision, &[], rows)?;
    let descriptor_manifest_digest = parse_sha256_digest_uri(&metadata.descriptor.manifest_digest)?;
    if descriptor_manifest_digest != seals.manifest_seal {
        return Err(ClientError::ArtifactIntegrity(
            "provider-neutral manifest rows do not match the descriptor seal".to_owned(),
        ));
    }

    let manifest = ArtifactManifest {
        logical_shard_id: LogicalShardId::from(logical_shard_id),
        root_id: RootId::from(root_id),
        artifact_revision_id: ArtifactRevisionId::from(current_revision),
        logical_len: metadata.descriptor.logical_size,
        sha256: body_digest.0,
        blocks: blocks_from_rows(root_id, logical_shard_id, rows)?,
    };
    manifest.validate()?;
    Ok(manifest)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PublishResume {
    token: OperationToken,
    staged_object_cursor: usize,
    uploaded_object_cursor: usize,
    manifest_cursor: usize,
    completed_rows: u64,
}

fn running_publish_resume(
    status: OperationStatus,
    operation_id: OperationIdentity,
    staged_object_count: usize,
    manifest_row_count: usize,
) -> Result<PublishResume, ClientError> {
    let status = validated_publish_status(status, operation_id)?;
    if status.state != OperationState::Running {
        return Err(ClientError::ResponseMismatch(
            "artifact publication stage did not remain in running state".to_owned(),
        ));
    }
    let staged_object_count = u64::try_from(staged_object_count).map_err(|_| {
        ClientError::ResponseMismatch("planned staged-object count exceeds u64".to_owned())
    })?;
    let manifest_row_count = u64::try_from(manifest_row_count).map_err(|_| {
        ClientError::ResponseMismatch("planned manifest-row count exceeds u64".to_owned())
    })?;
    let expected_total = staged_object_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(manifest_row_count))
        .ok_or_else(|| {
            ClientError::ResponseMismatch("planned publication progress overflows".to_owned())
        })?;
    if status
        .progress
        .total_rows
        .is_some_and(|total| total != expected_total)
        || (status.progress.total_rows.is_none() && status.progress.completed_rows != 0)
        || status.progress.completed_rows > expected_total
    {
        return Err(ClientError::ResponseMismatch(
            "artifact publication progress does not match its sealed plan".to_owned(),
        ));
    }
    let completed_rows = status.progress.completed_rows;
    let staged_cursor = completed_rows.min(staged_object_count);
    let uploaded_cursor = completed_rows
        .saturating_sub(staged_object_count)
        .min(staged_object_count);
    let manifest_cursor = completed_rows
        .saturating_sub(staged_object_count.saturating_mul(2))
        .min(manifest_row_count);
    Ok(PublishResume {
        token: status.token,
        staged_object_cursor: usize::try_from(staged_cursor).map_err(|_| {
            ClientError::ResponseMismatch(
                "staged-object progress is not addressable by the client".to_owned(),
            )
        })?,
        uploaded_object_cursor: usize::try_from(uploaded_cursor).map_err(|_| {
            ClientError::ResponseMismatch(
                "uploaded-object progress is not addressable by the client".to_owned(),
            )
        })?,
        manifest_cursor: usize::try_from(manifest_cursor).map_err(|_| {
            ClientError::ResponseMismatch(
                "manifest progress is not addressable by the client".to_owned(),
            )
        })?,
        completed_rows,
    })
}

/// The publication a terminal, successful operation record carries.
///
/// One definition shared by every path that observes a succeeded publication,
/// whether it is discovered at `Begin` or recovered after a lost `Complete`
/// response.
fn published_result_from_status(status: &OperationStatus) -> Result<PublishResult, ClientError> {
    match &status.result {
        Some(OperationResult::ArtifactPublish(result)) => Ok(result.clone()),
        _ => Err(ClientError::ResponseMismatch(
            "succeeded artifact operation did not contain a publish result".to_owned(),
        )),
    }
}

fn running_publish_token(
    status: OperationStatus,
    operation_id: OperationIdentity,
) -> Result<OperationToken, ClientError> {
    let status = validated_publish_status(status, operation_id)?;
    if status.state != OperationState::Running {
        return Err(ClientError::ResponseMismatch(
            "artifact publication stage did not remain in running state".to_owned(),
        ));
    }
    Ok(status.token)
}

fn validated_publish_status(
    status: OperationStatus,
    operation_id: OperationIdentity,
) -> Result<OperationStatus, ClientError> {
    if status.kind != OperationKind::ArtifactPublish {
        return Err(ClientError::ResponseMismatch(
            "operation status is not an artifact publication".to_owned(),
        ));
    }
    if status.token.operation_id != operation_id {
        return Err(ClientError::ResponseMismatch(
            "operation status belongs to another operation".to_owned(),
        ));
    }
    Ok(status)
}

fn expect_operation(result: WorkspaceResult) -> Result<OperationStatus, ClientError> {
    match result {
        WorkspaceResult::Operation(operation) => Ok(operation),
        _ => Err(ClientError::ResponseMismatch(
            "expected operation result".to_owned(),
        )),
    }
}

fn expect_published(result: WorkspaceResult) -> Result<PublishResult, ClientError> {
    match result {
        WorkspaceResult::Published(published) => Ok(published),
        _ => Err(ClientError::ResponseMismatch(
            "expected published result".to_owned(),
        )),
    }
}

fn expect_path(result: WorkspaceResult) -> Result<nokv_protocol::PathReadResult, ClientError> {
    match result {
        WorkspaceResult::Path(path) => Ok(path),
        _ => Err(ClientError::ResponseMismatch(
            "expected path result".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use nokv_object::{
        ArtifactStoreCapabilities, ImmutableCreateOutcome, MemoryArtifactStore,
        ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectRange,
    };
    use nokv_protocol::{
        decode_request, encode_response, ConflictKind, OperationProgress, PathReadResult,
        RelativePath, RequestIdentity, RootIdentity, RootRoute, RpcFailure, ScalarValue,
        WorkbenchName, WorkspaceIdentity, WorkspaceRpcOutcome, WorkspaceRpcRequest,
        WorkspaceRpcResponse,
    };

    use super::*;
    use crate::{ClientOptions, StaticRouteResolver, TransportError};

    #[derive(Clone)]
    struct ScriptedArtifactTransport {
        state: Arc<Mutex<ScriptedArtifactState>>,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    struct ScriptedArtifactState {
        replay: BTreeMap<RequestIdentity, WorkspaceRpcResponse>,
        attempts: Vec<(&'static str, RequestIdentity)>,
        begin: Option<BeginArtifactPublishRequest>,
        staged_objects: Vec<StagedObject>,
        manifest_rows: Vec<ArtifactManifestRow>,
        manifest_dependency_batches: Vec<Vec<ArtifactRevisionIdentity>>,
        operation: Option<OperationStatus>,
        path: Option<PathReadResult>,
        token_sequence: u8,
        commit_version: u64,
        lose_stage_response_once: bool,
        stage_response_lost: bool,
        lose_complete_responses: bool,
        stage_applies: usize,
        abort_applies: usize,
        change_fence_on_second_page_once: bool,
        fence_changed: bool,
        append_begin_conflicts: usize,
    }

    impl ScriptedArtifactTransport {
        fn new(events: Arc<Mutex<Vec<&'static str>>>, lose_stage_response_once: bool) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedArtifactState {
                    replay: BTreeMap::new(),
                    attempts: Vec::new(),
                    begin: None,
                    staged_objects: Vec::new(),
                    manifest_rows: Vec::new(),
                    manifest_dependency_batches: Vec::new(),
                    operation: None,
                    path: None,
                    token_sequence: 0,
                    commit_version: 0,
                    lose_stage_response_once,
                    stage_response_lost: false,
                    lose_complete_responses: false,
                    stage_applies: 0,
                    abort_applies: 0,
                    change_fence_on_second_page_once: false,
                    fence_changed: false,
                    append_begin_conflicts: 0,
                })),
                events,
            }
        }

        fn state(&self) -> std::sync::MutexGuard<'_, ScriptedArtifactState> {
            self.state.lock().unwrap()
        }

        fn change_fence_on_second_page_once(&self) {
            self.state.lock().unwrap().change_fence_on_second_page_once = true;
        }

        fn lose_complete_responses(&self) {
            self.state.lock().unwrap().lose_complete_responses = true;
        }

        fn conflict_append_begins(&self, count: usize) {
            self.state.lock().unwrap().append_begin_conflicts = count;
        }
    }

    impl ScriptedArtifactState {
        fn next_token(&mut self, operation_id: OperationIdentity) -> OperationToken {
            self.token_sequence = self.token_sequence.saturating_add(1);
            OperationToken {
                operation_id,
                state_digest: Digest([self.token_sequence; 32]),
            }
        }

        /// The durable operation row the engine would replay for `operation_id`
        /// when it has already reached a terminal state.
        fn replayable_terminal_status(
            &self,
            operation_id: OperationIdentity,
        ) -> Option<OperationStatus> {
            let status = self.operation.as_ref()?;
            if status.token.operation_id != operation_id || status.state == OperationState::Running
            {
                return None;
            }
            Some(status.clone())
        }

        fn running_status(&mut self, operation_id: OperationIdentity) -> OperationStatus {
            OperationStatus {
                token: self.next_token(operation_id),
                kind: OperationKind::ArtifactPublish,
                commit_preparation: None,
                restore_preparation: None,
                state: OperationState::Running,
                progress: OperationProgress {
                    completed_rows: 0,
                    total_rows: None,
                    completed_bytes: 0,
                    total_bytes: None,
                },
                result: None,
                failure: None,
            }
        }
    }

    impl RpcTransport for ScriptedArtifactTransport {
        fn round_trip(
            &self,
            _endpoint: std::net::SocketAddr,
            encoded: &[u8],
        ) -> Result<Vec<u8>, TransportError> {
            let request = decode_request(encoded)
                .map_err(|error| TransportError::new(error.to_string(), false))?;
            let label = operation_label(&request.operation);
            let mut state = self.state.lock().unwrap();
            state.attempts.push((label, request.request_id));
            if let Some(mut replayed) = state.replay.get(&request.request_id).cloned() {
                if label == "complete" && state.lose_complete_responses {
                    return Err(TransportError::new(
                        "injected complete response loss after durable success",
                        true,
                    ));
                }
                replayed.replayed = true;
                return encode_response(&replayed)
                    .map_err(|error| TransportError::new(error.to_string(), false));
            }

            let injected_append_conflict = matches!(
                &request.operation,
                WorkspaceRequest::BeginArtifactPublish(BeginArtifactPublishRequest {
                    condition: PublishCondition::Append { .. },
                    ..
                })
            ) && state.append_begin_conflicts > 0;
            let missing_path =
                matches!(&request.operation, WorkspaceRequest::GetPath(_)) && state.path.is_none();
            if injected_append_conflict || missing_path {
                if injected_append_conflict {
                    state.append_begin_conflicts -= 1;
                }
                let response = WorkspaceRpcResponse {
                    route: request.route,
                    request_id: request.request_id,
                    commit_version: None,
                    replayed: false,
                    outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
                        code: if injected_append_conflict {
                            ErrorCode::Conflict
                        } else {
                            ErrorCode::NotFound
                        },
                        message: if injected_append_conflict {
                            "injected append generation conflict".to_owned()
                        } else {
                            "path does not exist".to_owned()
                        },
                        retryable: false,
                        conflict: injected_append_conflict.then_some(ConflictKind::PathGeneration),
                        current_generation: state
                            .path
                            .as_ref()
                            .and_then(|path| path.metadata.as_ref())
                            .map(|metadata| metadata.generation),
                        route_hint: None,
                    }),
                };
                state.replay.insert(request.request_id, response.clone());
                return encode_response(&response)
                    .map_err(|error| TransportError::new(error.to_string(), false));
            }

            self.events.lock().unwrap().push(label);
            state.commit_version = state.commit_version.saturating_add(1);
            let result = apply_request(&mut state, &request)?;
            let response = WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version: Some(state.commit_version),
                replayed: false,
                outcome: WorkspaceRpcOutcome::Success(Box::new(result)),
            };
            state.replay.insert(request.request_id, response.clone());
            if matches!(request.operation, WorkspaceRequest::StageArtifactObjects(_))
                && state.lose_stage_response_once
                && !state.stage_response_lost
            {
                state.stage_response_lost = true;
                return Err(TransportError::new(
                    "injected response loss after durable stage",
                    true,
                ));
            }
            if matches!(
                request.operation,
                WorkspaceRequest::CompleteArtifactPublish(_)
            ) && state.lose_complete_responses
            {
                return Err(TransportError::new(
                    "injected complete response loss after durable success",
                    true,
                ));
            }
            encode_response(&response)
                .map_err(|error| TransportError::new(error.to_string(), false))
        }
    }

    fn apply_request(
        state: &mut ScriptedArtifactState,
        request: &WorkspaceRpcRequest,
    ) -> Result<WorkspaceResult, TransportError> {
        match &request.operation {
            WorkspaceRequest::BeginArtifactPublish(begin) => {
                // The engine replays a durable operation row whose identity and
                // initialization digests match, including one that already
                // reached a terminal state. Model that here: an exact retry of a
                // publication that already succeeded observes the stored
                // terminal record rather than a fresh running one.
                if let Some(status) = state.replayable_terminal_status(begin.operation_id) {
                    return Ok(WorkspaceResult::Operation(status));
                }
                state.begin = Some(begin.clone());
                state.staged_objects.clear();
                state.manifest_rows.clear();
                state.manifest_dependency_batches.clear();
                let status = state.running_status(begin.operation_id);
                state.operation = Some(status.clone());
                Ok(WorkspaceResult::Operation(status))
            }
            WorkspaceRequest::StageArtifactObjects(stage) => {
                require_token(state, stage.token)?;
                state.stage_applies = state.stage_applies.saturating_add(1);
                state.staged_objects.extend(stage.objects.clone());
                let status = state.running_status(stage.token.operation_id);
                state.operation = Some(status.clone());
                Ok(WorkspaceResult::Operation(status))
            }
            WorkspaceRequest::MarkArtifactObjectsUploaded(mark) => {
                require_token(state, mark.token)?;
                let status = state.running_status(mark.token.operation_id);
                state.operation = Some(status.clone());
                Ok(WorkspaceResult::Operation(status))
            }
            WorkspaceRequest::StageArtifactManifest(stage) => {
                require_token(state, stage.token)?;
                state.manifest_rows.extend(stage.rows.clone());
                state
                    .manifest_dependency_batches
                    .push(stage.dependency_owner_revision_ids.clone());
                let status = state.running_status(stage.token.operation_id);
                state.operation = Some(status.clone());
                Ok(WorkspaceResult::Operation(status))
            }
            WorkspaceRequest::CompleteArtifactPublish(complete) => {
                require_token(state, complete.token)?;
                let begin = state
                    .begin
                    .clone()
                    .ok_or_else(|| TransportError::new("complete before begin", false))?;
                let current = state.path.as_ref().and_then(|path| path.metadata.as_ref());
                let current_generation = current.map(|metadata| metadata.generation);
                let generation = match begin.condition {
                    PublishCondition::CreateOnly if current.is_none() => 1,
                    PublishCondition::CreateOnly => {
                        return Err(TransportError::new(
                            "create-only publication found an existing path",
                            false,
                        ));
                    }
                    PublishCondition::ReplaceOnly {
                        expected_generation,
                    }
                    | PublishCondition::Append {
                        expected_generation: Some(expected_generation),
                    } if current_generation == Some(expected_generation) => {
                        expected_generation.saturating_add(1)
                    }
                    PublishCondition::Append {
                        expected_generation: None,
                    } if current.is_some() => current_generation.unwrap().saturating_add(1),
                    PublishCondition::ReplaceOnly { .. } | PublishCondition::Append { .. } => {
                        return Err(TransportError::new(
                            "conditional publication generation changed",
                            false,
                        ));
                    }
                };
                let workspace_revision = current
                    .map(|metadata| metadata.workspace_revision.saturating_add(1))
                    .unwrap_or(1);
                let dependency_count = u32::try_from(begin.dependency_owner_revision_ids.len())
                    .map_err(|_| TransportError::new("dependency count exceeds u32", false))?;
                let dependency_depth = if dependency_count == 0 {
                    0
                } else {
                    current
                        .map(|metadata| metadata.dependency_depth)
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| TransportError::new("dependency depth exceeds u8", false))?
                };
                let published = PublishResult {
                    operation_id: begin.operation_id,
                    target: begin.target.clone(),
                    workspace_revision,
                    generation,
                    artifact_revision_id: begin.artifact_revision_id,
                    logical_size: complete.artifact.logical_size,
                    body_digest: complete.artifact.body_digest.clone(),
                };
                let status = OperationStatus {
                    token: state.next_token(begin.operation_id),
                    kind: OperationKind::ArtifactPublish,
                    commit_preparation: None,
                    restore_preparation: None,
                    state: OperationState::Succeeded,
                    progress: OperationProgress {
                        completed_rows: state.manifest_rows.len() as u64,
                        total_rows: Some(state.manifest_rows.len() as u64),
                        completed_bytes: complete.artifact.logical_size,
                        total_bytes: Some(complete.artifact.logical_size),
                    },
                    result: Some(OperationResult::ArtifactPublish(published.clone())),
                    failure: None,
                };
                state.operation = Some(status);
                state.path = Some(PathReadResult {
                    not_modified: false,
                    metadata: Some(PathMetadata {
                        path: begin.target,
                        workspace_incarnation_id: WorkspaceIdentity([9; 16]),
                        workspace_revision,
                        generation,
                        artifact_revision_id: begin.artifact_revision_id,
                        dependency_count,
                        dependency_depth,
                        descriptor: complete.artifact.clone(),
                    }),
                    range: None,
                    blocks: state.manifest_rows.clone(),
                    next_cursor: None,
                });
                Ok(WorkspaceResult::Published(published))
            }
            WorkspaceRequest::AbortArtifactPublish(abort) => {
                require_token(state, abort.token)?;
                state.abort_applies = state.abort_applies.saturating_add(1);
                let status = OperationStatus {
                    token: state.next_token(abort.token.operation_id),
                    kind: OperationKind::ArtifactPublish,
                    commit_preparation: None,
                    restore_preparation: None,
                    state: OperationState::Aborting,
                    progress: OperationProgress {
                        completed_rows: 0,
                        total_rows: None,
                        completed_bytes: 0,
                        total_bytes: None,
                    },
                    result: None,
                    failure: None,
                };
                state.operation = Some(status.clone());
                Ok(WorkspaceResult::Operation(status))
            }
            WorkspaceRequest::GetOperation(get) => {
                let status = state
                    .operation
                    .clone()
                    .filter(|status| status.token.operation_id == get.operation_id)
                    .ok_or_else(|| TransportError::new("operation does not exist", false))?;
                Ok(WorkspaceResult::Operation(status))
            }
            WorkspaceRequest::GetPath(get) => {
                let mut stored = state
                    .path
                    .clone()
                    .ok_or_else(|| TransportError::new("path does not exist", false))?;
                let Some(range) = get.range else {
                    return Ok(WorkspaceResult::Path(PathReadResult {
                        not_modified: false,
                        metadata: stored.metadata,
                        range: None,
                        blocks: Vec::new(),
                        next_cursor: None,
                    }));
                };
                let page = get
                    .plan_page
                    .as_ref()
                    .ok_or_else(|| TransportError::new("missing plan page", false))?;
                let start = page
                    .cursor
                    .as_deref()
                    .map(decode_test_cursor)
                    .transpose()?
                    .unwrap_or(0);
                if start != 0 && state.change_fence_on_second_page_once && !state.fence_changed {
                    state.fence_changed = true;
                    if let Some(metadata) = stored.metadata.as_mut() {
                        metadata.generation = metadata.generation.saturating_add(1);
                    }
                }
                let range_end = range
                    .offset
                    .checked_add(range.length)
                    .ok_or_else(|| TransportError::new("range overflow", false))?;
                let matching = stored
                    .blocks
                    .into_iter()
                    .filter(|row| {
                        row.logical_offset < range_end
                            && row.logical_offset + row.length > range.offset
                    })
                    .collect::<Vec<_>>();
                let end = start
                    .saturating_add(page.limit as usize)
                    .min(matching.len());
                if start >= end {
                    return Err(TransportError::new("range page is empty", false));
                }
                let next_cursor =
                    (end < matching.len()).then(|| (end as u64).to_be_bytes().to_vec());
                Ok(WorkspaceResult::Path(PathReadResult {
                    not_modified: false,
                    metadata: stored.metadata,
                    range: Some(range),
                    blocks: matching[start..end].to_vec(),
                    next_cursor,
                }))
            }
            _ => Err(TransportError::new(
                "unexpected request in artifact script",
                false,
            )),
        }
    }

    fn decode_test_cursor(cursor: &[u8]) -> Result<usize, TransportError> {
        let encoded: [u8; 8] = cursor
            .try_into()
            .map_err(|_| TransportError::new("invalid test cursor", false))?;
        usize::try_from(u64::from_be_bytes(encoded))
            .map_err(|_| TransportError::new("test cursor exceeds usize", false))
    }

    fn require_token(
        state: &ScriptedArtifactState,
        token: OperationToken,
    ) -> Result<(), TransportError> {
        if state
            .operation
            .as_ref()
            .is_some_and(|operation| operation.token == token)
        {
            Ok(())
        } else {
            Err(TransportError::new("operation token mismatch", false))
        }
    }

    fn operation_label(operation: &WorkspaceRequest) -> &'static str {
        match operation {
            WorkspaceRequest::BeginArtifactPublish(_) => "begin",
            WorkspaceRequest::StageArtifactObjects(_) => "stage_objects",
            WorkspaceRequest::MarkArtifactObjectsUploaded(_) => "mark_uploaded",
            WorkspaceRequest::StageArtifactManifest(_) => "stage_manifest",
            WorkspaceRequest::CompleteArtifactPublish(_) => "complete",
            WorkspaceRequest::AbortArtifactPublish(_) => "abort",
            WorkspaceRequest::GetOperation(_) => "get_operation",
            WorkspaceRequest::GetPath(_) => "get_path",
            _ => "other",
        }
    }

    struct RecordingStore {
        inner: MemoryArtifactStore,
        events: Arc<Mutex<Vec<&'static str>>>,
        creates: AtomicUsize,
        deletes: AtomicUsize,
        reads: AtomicUsize,
        temporary_read_failures: AtomicUsize,
        fail_create_at: Option<usize>,
        corrupt_reads: AtomicBool,
    }

    impl RecordingStore {
        fn new(events: Arc<Mutex<Vec<&'static str>>>, fail_create_at: Option<usize>) -> Self {
            Self {
                inner: MemoryArtifactStore::new(),
                events,
                creates: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                reads: AtomicUsize::new(0),
                temporary_read_failures: AtomicUsize::new(0),
                fail_create_at,
                corrupt_reads: AtomicBool::new(false),
            }
        }

        fn fail_next_reads(&self, count: usize) {
            self.temporary_read_failures.store(count, Ordering::SeqCst);
        }
    }

    impl ArtifactObjectStore for RecordingStore {
        fn object_namespace(&self) -> Option<nokv_types::ObjectNamespaceId> {
            Some(nokv_types::ObjectNamespaceId::from_bytes([8; 16]))
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

        fn create_immutable(
            &self,
            key: &ObjectKey,
            bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            self.events.lock().unwrap().push("object_create");
            let attempt = self.creates.fetch_add(1, Ordering::SeqCst);
            if self.fail_create_at == Some(attempt) {
                return Err(ObjectError::backend_failure(
                    "injected upload failure",
                    false,
                ));
            }
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self
                .temporary_read_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ObjectError::backend_failure(
                    "request failed for endpoint=http://127.0.0.1:9000 bucket=private",
                    true,
                ));
            }
            let mut bytes = self.inner.read(key, range)?;
            if self.corrupt_reads.load(Ordering::SeqCst) && !bytes.is_empty() {
                bytes[0] ^= 0xff;
            }
            Ok(bytes)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(key)
        }
    }

    fn route() -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: nokv_protocol::ObjectNamespaceIdentity([8; 16]),
            placement_generation: 3,
            owner_epoch: 4,
        }
    }

    fn client(
        transport: ScriptedArtifactTransport,
    ) -> WorkspaceClient<ScriptedArtifactTransport, StaticRouteResolver> {
        WorkspaceClient::new(
            route().root_id,
            transport,
            StaticRouteResolver::new(route(), ([127, 0, 0, 1], 4100).into()).unwrap(),
            ClientOptions::default(),
        )
        .unwrap()
    }

    fn target() -> WorkspacePath {
        WorkspacePath {
            workbench: WorkbenchName::new("run-42").unwrap(),
            path: RelativePath::new("outputs/result.bin").unwrap(),
        }
    }

    fn publish_options(block_size: usize) -> ArtifactPublishOptions {
        ArtifactPublishOptions::new(
            OperationIdentity([7; 16]),
            ArtifactRevisionIdentity([8; 16]),
            target(),
            PublishCondition::CreateOnly,
            ContentType::new("application/octet-stream").unwrap(),
        )
        .with_block_size(block_size)
    }

    fn append_options(block_size: usize) -> ArtifactAppendOptions {
        ArtifactAppendOptions::new(
            OperationIdentity([0x17; 16]),
            ArtifactRevisionIdentity([0x18; 16]),
            target(),
            ContentType::new("text/plain").unwrap(),
        )
        .with_block_size(block_size)
    }

    #[test]
    fn append_creates_a_missing_path_from_only_delta_objects() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        let outcome = client
            .append_artifact(
                &store,
                append_options(2).with_max_logical_size(16),
                b"hello",
            )
            .unwrap();
        assert!(outcome.created);
        assert_eq!(outcome.publication.value.generation, 1);
        assert_eq!(outcome.publication.value.logical_size, 5);
        assert_eq!(outcome.descriptor.content_type.as_str(), "text/plain");
        assert_eq!(outcome.upload_stats.created, 3);
        assert_eq!(store.creates.load(Ordering::SeqCst), 3);
        let state = inspector.state();
        assert_eq!(state.staged_objects.len(), 3);
        assert!(state
            .begin
            .as_ref()
            .is_some_and(|begin| matches!(begin.condition, PublishCondition::CreateOnly)));
        drop(state);
        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, b"hello");
    }

    #[test]
    fn append_retries_head_conflict_without_reuploading_or_double_applying_base() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let base = b"abcdefgh";
        let delta = b"XYZ";

        client
            .publish_artifact(&store, publish_options(4), base)
            .unwrap();
        let creates_before_append = store.creates.load(Ordering::SeqCst);
        inspector.conflict_append_begins(1);
        let outcome = client
            .append_artifact(&store, append_options(2).with_max_logical_size(64), delta)
            .unwrap();

        assert!(!outcome.created);
        assert_eq!(outcome.publication.value.generation, 2);
        assert_eq!(outcome.publication.value.logical_size, 11);
        assert_eq!(
            outcome.publication.value.body_digest,
            sha256_digest_uri(Digest(Sha256::digest(b"abcdefghXYZ").into()))
        );
        assert_eq!(
            outcome.descriptor.content_type.as_str(),
            "application/octet-stream",
            "an append without an override must inherit the base content type"
        );
        assert_eq!(outcome.base_read_stats.store_read_bytes, 8);
        assert_eq!(outcome.upload_stats.created, 2);
        assert_eq!(
            store.creates.load(Ordering::SeqCst) - creates_before_append,
            2,
            "only the two delta blocks may be uploaded after a head conflict"
        );

        let state = inspector.state();
        let begin = state.begin.as_ref().unwrap();
        assert!(matches!(
            begin.condition,
            PublishCondition::Append {
                expected_generation: Some(1)
            }
        ));
        assert_eq!(
            begin.dependency_owner_revision_ids,
            vec![ArtifactRevisionIdentity([8; 16])]
        );
        assert_eq!(state.staged_objects.len(), 2);
        assert_eq!(state.manifest_rows.len(), 4);
        let seals = seal_artifact_publish_plan(
            begin.artifact_revision_id,
            &state.staged_objects,
            &state.manifest_rows,
        )
        .unwrap();
        assert_eq!(begin.staged_object_seal, seals.staged_object_seal);
        assert_eq!(begin.manifest_seal, seals.manifest_seal);
        assert!(state
            .manifest_dependency_batches
            .iter()
            .all(|owners| owners == &begin.dependency_owner_revision_ids));
        assert!(state.manifest_rows[..2]
            .iter()
            .all(|row| row.physical_owner_revision_id == ArtifactRevisionIdentity([8; 16])));
        assert_eq!(
            state.manifest_rows[..2]
                .iter()
                .map(|row| (row.object_index, row.physical_object_index))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 1)],
            "borrowed base rows must preserve their owner-local physical indexes",
        );
        assert!(state.manifest_rows[2..].iter().all(|row| {
            row.physical_owner_revision_id == begin.artifact_revision_id
                && row.append_segment.is_some()
        }));
        assert_eq!(
            state.manifest_rows[2..]
                .iter()
                .map(|row| (row.object_index, row.physical_object_index))
                .collect::<Vec<_>>(),
            vec![(2, 0), (3, 1)],
            "delta manifest positions must be rebased without rebasing physical object indexes",
        );
        assert_eq!(
            state
                .attempts
                .iter()
                .filter(|(label, _)| *label == "begin")
                .count(),
            3,
            "base publish plus one rejected and one successful append begin"
        );
        drop(state);

        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, b"abcdefghXYZ");
    }

    #[test]
    fn ninth_append_rematerializes_and_survives_ancestor_object_gc() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let mut expected_body = vec![b'a'];

        client
            .publish_artifact(&store, publish_options(1), &expected_body)
            .unwrap();
        for append_index in 0_u8..8 {
            let delta = [b'b'.checked_add(append_index).unwrap()];
            expected_body.extend_from_slice(&delta);
            client
                .append_artifact(
                    &store,
                    ArtifactAppendOptions::new(
                        OperationIdentity([0x30 + append_index; 16]),
                        ArtifactRevisionIdentity([0x50 + append_index; 16]),
                        target(),
                        ContentType::new("text/plain").unwrap(),
                    )
                    .with_block_size(1),
                    &delta,
                )
                .unwrap();
        }

        let ancestor_keys = {
            let state = inspector.state();
            let metadata = state
                .path
                .as_ref()
                .and_then(|path| path.metadata.as_ref())
                .unwrap();
            assert_eq!(metadata.dependency_count, 8);
            assert_eq!(metadata.dependency_depth, MAX_ARTIFACT_DEPENDENCY_DEPTH);
            state
                .manifest_rows
                .iter()
                .map(|row| ObjectKey::new(row.object_identity.as_str()).unwrap())
                .collect::<BTreeSet<_>>()
        };

        let final_delta = *b"j";
        expected_body.extend_from_slice(&final_delta);
        let creates_before_squash = store.creates.load(Ordering::SeqCst);
        inspector.conflict_append_begins(1);
        let outcome = client
            .append_artifact(
                &store,
                ArtifactAppendOptions::new(
                    OperationIdentity([0x38; 16]),
                    ArtifactRevisionIdentity([0x58; 16]),
                    target(),
                    ContentType::new("text/plain").unwrap(),
                )
                .with_block_size(1),
                &final_delta,
            )
            .unwrap();

        assert_eq!(outcome.base_read_stats.store_read_bytes, 9);
        assert_eq!(outcome.upload_stats.created, 10);
        assert_eq!(
            store.creates.load(Ordering::SeqCst) - creates_before_squash,
            10,
            "a rejected squash Begin must not upload the full body"
        );
        assert_eq!(
            outcome.descriptor.body_digest,
            sha256_digest_uri(Digest(Sha256::digest(&expected_body).into()))
        );
        let state = inspector.state();
        let begin = state.begin.as_ref().unwrap();
        assert!(matches!(
            begin.condition,
            PublishCondition::Append {
                expected_generation: Some(9)
            }
        ));
        assert!(begin.dependency_owner_revision_ids.is_empty());
        let final_revision = begin.artifact_revision_id;
        assert_eq!(state.staged_objects.len(), expected_body.len());
        assert_eq!(state.manifest_rows.len(), expected_body.len());
        assert!(state.manifest_rows.iter().all(|row| {
            row.physical_owner_revision_id == final_revision
                && row.append_segment.is_none()
                && row.object_index == row.physical_object_index
        }));
        let metadata = state
            .path
            .as_ref()
            .and_then(|path| path.metadata.as_ref())
            .unwrap();
        assert_eq!(metadata.dependency_count, 0);
        assert_eq!(metadata.dependency_depth, 0);
        drop(state);

        for key in ancestor_keys {
            assert_eq!(store.delete(&key).unwrap(), ObjectDeleteOutcome::Deleted);
        }
        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, expected_body);
    }

    #[test]
    fn append_dependency_owner_limit_triggers_rematerialization_before_begin() {
        let current_revision = ArtifactRevisionIdentity([0xe0; 16]);
        let next_revision = ArtifactRevisionIdentity([0xe1; 16]);
        let metadata = PathMetadata {
            path: target(),
            workspace_incarnation_id: WorkspaceIdentity([9; 16]),
            workspace_revision: 1,
            generation: 1,
            artifact_revision_id: current_revision,
            dependency_count: MAX_ARTIFACT_DEPENDENCY_OWNERS,
            dependency_depth: 1,
            descriptor: ArtifactDescriptor {
                logical_size: u64::from(MAX_ARTIFACT_DEPENDENCY_OWNERS) + 1,
                body_digest: sha256_digest_uri(Digest([1; 32])),
                manifest_digest: sha256_digest_uri(Digest([2; 32])),
                content_type: ContentType::new("application/octet-stream").unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        };
        let mut rows = (0..MAX_ARTIFACT_DEPENDENCY_OWNERS)
            .map(|index| ArtifactManifestRow {
                object_index: u64::from(index),
                physical_object_index: 0,
                logical_offset: u64::from(index),
                physical_owner_revision_id: ArtifactRevisionIdentity(
                    (u128::from(index) + 1).to_be_bytes(),
                ),
                object_identity: ObjectIdentity::new(format!("owner-{index}")).unwrap(),
                object_offset: 0,
                length: 1,
                digest: sha256_digest_uri(Digest([3; 32])),
                append_segment: None,
            })
            .collect::<Vec<_>>();

        let at_limit = append_dependency_plan(&metadata, next_revision, &rows).unwrap();
        assert_eq!(
            at_limit.owners.len(),
            usize::try_from(MAX_ARTIFACT_DEPENDENCY_OWNERS).unwrap()
        );
        assert!(!at_limit.requires_rematerialization);

        rows.push(ArtifactManifestRow {
            object_index: u64::from(MAX_ARTIFACT_DEPENDENCY_OWNERS),
            physical_object_index: 0,
            logical_offset: u64::from(MAX_ARTIFACT_DEPENDENCY_OWNERS),
            physical_owner_revision_id: current_revision,
            object_identity: ObjectIdentity::new("current-owner").unwrap(),
            object_offset: 0,
            length: 1,
            digest: sha256_digest_uri(Digest([4; 32])),
            append_segment: None,
        });
        let overflow = append_dependency_plan(&metadata, next_revision, &rows).unwrap();
        assert_eq!(
            overflow.owners.len(),
            usize::try_from(MAX_ARTIFACT_DEPENDENCY_OWNERS).unwrap() + 1
        );
        assert!(overflow.requires_rematerialization);
    }

    #[test]
    fn append_size_gate_fails_before_begin_or_delta_upload() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        let creates = store.creates.load(Ordering::SeqCst);
        let begins = inspector
            .state()
            .attempts
            .iter()
            .filter(|(label, _)| *label == "begin")
            .count();

        let error = client
            .append_artifact(&store, append_options(2).with_max_logical_size(8), b"!")
            .unwrap_err();
        assert!(matches!(error, ClientError::InvalidOptions(_)));
        assert_eq!(store.creates.load(Ordering::SeqCst), creates);
        assert_eq!(
            inspector
                .state()
                .attempts
                .iter()
                .filter(|(label, _)| *label == "begin")
                .count(),
            begins
        );
    }

    #[test]
    fn every_planned_row_is_durable_before_the_first_object_write() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(Arc::clone(&events), None);
        let bytes = vec![0x5a; MAX_ARTIFACT_PUBLISH_BATCH_ROWS + 1];

        let outcome = client
            .publish_artifact(&store, publish_options(1), &bytes)
            .unwrap();
        assert_eq!(
            outcome.upload_stats.blocks,
            (MAX_ARTIFACT_PUBLISH_BATCH_ROWS + 1) as u64
        );

        let events = events.lock().unwrap();
        let first_create = events
            .iter()
            .position(|event| *event == "object_create")
            .unwrap();
        let stage_positions = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| (*event == "stage_objects").then_some(index))
            .collect::<Vec<_>>();
        assert_eq!(stage_positions.len(), 2);
        assert!(stage_positions
            .into_iter()
            .all(|index| index < first_create));
        assert_eq!(events[0], "begin");
    }

    #[test]
    fn invalid_index_fields_fail_before_begin_or_object_write() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(Arc::clone(&events), None);
        let options = publish_options(4).with_index_fields(vec![
            FieldValue {
                field_id: "z".to_owned(),
                value: ScalarValue::Unsigned(1),
            },
            FieldValue {
                field_id: "a".to_owned(),
                value: ScalarValue::Unsigned(2),
            },
        ]);

        let error = client
            .publish_artifact(&store, options, b"abcdefgh")
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::Protocol(nokv_protocol::ProtocolError::InvalidField {
                field: "artifact.index_fields",
                ..
            })
        ));
        assert_eq!(store.creates.load(Ordering::SeqCst), 0);
        assert!(events.lock().unwrap().is_empty());
        let state = inspector.state();
        assert!(state.begin.is_none());
        assert!(state.attempts.is_empty());
    }

    #[test]
    fn valid_index_fields_cross_the_workspace_rpc_publication_path() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let index_fields = vec![
            FieldValue {
                field_id: "agent.owner".to_owned(),
                value: ScalarValue::String("planner".to_owned()),
            },
            FieldValue {
                field_id: "agent.score".to_owned(),
                value: ScalarValue::Unsigned(7),
            },
        ];

        client
            .publish_artifact(
                &store,
                publish_options(4).with_index_fields(index_fields.clone()),
                b"abcdefgh",
            )
            .unwrap();

        let state = inspector.state();
        let metadata = state
            .path
            .as_ref()
            .and_then(|path| path.metadata.as_ref())
            .expect("publication exposes the visible path metadata");
        assert_eq!(metadata.descriptor.index_fields, index_fields);
        assert!(state.attempts.iter().any(|(label, _)| *label == "complete"));
    }

    #[test]
    fn response_loss_replays_the_exact_stage_request_without_duplicate_apply() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), true);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        let state = inspector.state();
        let stage_attempts = state
            .attempts
            .iter()
            .filter(|(label, _)| *label == "stage_objects")
            .map(|(_, request_id)| *request_id)
            .collect::<Vec<_>>();
        assert_eq!(stage_attempts.len(), 2);
        assert_eq!(stage_attempts[0], stage_attempts[1]);
        assert_eq!(state.stage_applies, 1);
    }

    #[test]
    fn begin_progress_recovers_each_ordered_publication_cursor() {
        let operation_id = OperationIdentity([0x44; 16]);
        let status = OperationStatus {
            token: OperationToken {
                operation_id,
                state_digest: Digest([0x55; 32]),
            },
            kind: OperationKind::ArtifactPublish,
            commit_preparation: None,
            restore_preparation: None,
            state: OperationState::Running,
            progress: OperationProgress {
                completed_rows: 5,
                total_rows: Some(8),
                completed_bytes: 0,
                total_bytes: None,
            },
            result: None,
            failure: None,
        };
        let resume = running_publish_resume(status, operation_id, 3, 2).unwrap();
        assert_eq!(resume.staged_object_cursor, 3);
        assert_eq!(resume.uploaded_object_cursor, 2);
        assert_eq!(resume.manifest_cursor, 0);
        assert_eq!(resume.completed_rows, 5);
    }

    #[test]
    fn complete_response_loss_recovers_succeeded_operation_before_abort() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        inspector.lose_complete_responses();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        let outcome = client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        assert_eq!(outcome.publication.value.logical_size, 8);

        let state = inspector.state();
        assert_eq!(
            state
                .attempts
                .iter()
                .filter(|(label, _)| *label == "complete")
                .count(),
            ClientOptions::default().max_attempts as usize
        );
        assert!(state
            .attempts
            .iter()
            .any(|(label, _)| *label == "get_operation"));
        assert_eq!(state.abort_applies, 0);
        assert_eq!(
            state.operation.as_ref().unwrap().state,
            OperationState::Succeeded
        );
    }

    #[test]
    fn retrying_a_succeeded_publication_converges_without_reuploading() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        let first = client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        assert!(!first.publication.replayed);
        let uploads_after_first = inspector.state().stage_applies;

        // A scheduler resubmits the job. A fresh process republishes the same
        // bytes to the same path under the same durable identities.
        let retry = client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        assert!(
            retry.publication.replayed,
            "an exact retry must report replay"
        );
        assert_eq!(retry.publication.value, first.publication.value);
        assert_eq!(
            inspector.state().stage_applies,
            uploads_after_first,
            "a replayed publication must not stage objects again"
        );
        assert_eq!(
            inspector.state().abort_applies,
            0,
            "a replayed publication must not attempt an abort"
        );
        assert_eq!(
            (retry.upload_stats.blocks, retry.upload_stats.bytes),
            (0, 0),
            "a replayed publication uploads nothing"
        );
    }

    #[test]
    fn a_replayed_publication_whose_path_moved_is_not_reported_as_current() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        // The path is removed and reclaimed by another writer, so the durable
        // operation row still says succeeded while the path holds a revision
        // that is not this caller's.
        {
            let mut state = inspector.state();
            let metadata = state.path.as_mut().unwrap().metadata.as_mut().unwrap();
            metadata.artifact_revision_id = ArtifactRevisionIdentity([0x5c; 16]);
        }

        let error = client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap_err();
        let ClientError::ArtifactPublishFailed {
            stage,
            source,
            abort_failure,
        } = error
        else {
            panic!("expected a publish failure, got {error:?}");
        };
        assert_eq!(stage, ArtifactPublishStage::Begin);
        assert!(
            abort_failure.is_none(),
            "a terminal row must not be aborted"
        );
        assert!(
            matches!(*source, ClientError::ResponseMismatch(ref reason)
                if reason.contains("holds a different revision")),
            "unexpected source: {source:?}"
        );
        assert_eq!(inspector.state().abort_applies, 0);
    }

    #[test]
    fn upload_failure_durably_aborts_without_direct_object_delete() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, Some(1));

        let error = client
            .publish_artifact(&store, publish_options(4), b"abcdefghijkl")
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::ArtifactPublishFailed {
                stage: ArtifactPublishStage::UploadObjects,
                abort_failure: None,
                ..
            }
        ));
        assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
        let state = inspector.state();
        assert_eq!(state.abort_applies, 1);
        assert_eq!(
            state.operation.as_ref().unwrap().state,
            OperationState::Aborting
        );
    }

    #[test]
    fn raw_object_store_is_rejected_before_metadata_or_object_mutation() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = MemoryArtifactStore::new();

        let error = client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .expect_err("unverified stores must fail closed");

        assert!(matches!(error, ClientError::InvalidOptions(message)
            if message.contains("not verified against the root route namespace")));
        assert!(inspector.state().attempts.is_empty());
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn temporary_object_read_is_retried_and_recovers() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        let reads_before = store.reads.load(Ordering::SeqCst);
        store.fail_next_reads(1);
        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .expect("a temporary backend failure must be retried");

        assert_eq!(read.bytes, b"abcdefgh");
        assert!(store.reads.load(Ordering::SeqCst) >= reads_before + 3);
    }

    #[test]
    fn exhausted_object_read_preserves_retryability_without_provider_details() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        store.fail_next_reads(ClientOptions::default().max_attempts as usize);
        let error = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .expect_err("all object reads are temporarily unavailable");

        assert!(matches!(
            error,
            ClientError::RetryExhausted { attempts, .. }
                if attempts == ClientOptions::default().max_attempts
        ));
        assert!(error.retryable());
        let message = error.to_string();
        assert!(!message.contains("127.0.0.1"));
        assert!(!message.contains("private"));
        assert!(message.contains("artifact object backend is unavailable"));
    }

    #[test]
    fn range_reads_rebuild_and_verify_the_complete_provider_neutral_manifest() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let bytes = b"abcdefghijkl";
        client
            .publish_artifact(&store, publish_options(4), bytes)
            .unwrap();

        let read = client
            .read_artifact_range(&store, None, target(), WorkspaceReadView::Live, 3, 6)
            .unwrap();
        assert_eq!(read.bytes, bytes[3..9]);
        assert_eq!(read.stats.verified_blocks, 3);

        store.corrupt_reads.store(true, Ordering::SeqCst);
        assert!(matches!(
            client.read_artifact_range(&store, None, target(), WorkspaceReadView::Live, 3, 6,),
            Err(ClientError::Object(ObjectError::DigestMismatch { .. }))
        ));
    }

    #[test]
    fn reads_reject_a_physical_index_that_disagrees_with_the_immutable_key() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        inspector.state().path.as_mut().unwrap().blocks[0].physical_object_index = 7;

        assert!(matches!(
            client.read_artifact(&store, None, target(), WorkspaceReadView::Live),
            Err(ClientError::ArtifactIntegrity(message))
                if message.contains("declares physical index 7")
        ));
    }

    #[test]
    fn full_read_pages_manifest_rows_and_restarts_on_a_metadata_fence_change() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let bytes = vec![0x6b; MAX_ARTIFACT_READ_PLAN_ROWS + 1];
        client
            .publish_artifact(&store, publish_options(1), &bytes)
            .unwrap();
        inspector.change_fence_on_second_page_once();

        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, bytes);
        assert_eq!(
            read.stats.verified_blocks,
            (MAX_ARTIFACT_READ_PLAN_ROWS + 1) as u64
        );
        let state = inspector.state();
        assert!(state.fence_changed);
        assert!(
            state
                .attempts
                .iter()
                .filter(|(label, _)| *label == "get_path")
                .count()
                >= 6
        );
    }
}
