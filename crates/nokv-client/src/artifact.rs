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
    inline_artifact_body_digest, parse_sha256_digest_uri, seal_artifact_publish_plan,
    sha256_digest_uri, AbortArtifactPublishRequest, AppendAttemptBinding, AppendSegment,
    ArtifactDescriptor, ArtifactManifestRow, ArtifactRevisionIdentity, BeginArtifactPublishRequest,
    ByteRange, CompleteArtifactPublishRequest, ContentType, Digest, ErrorCode, FieldValue,
    FinishArtifactPublishRequest, GetOperationRequest, GetPathRequest, LogicalShardIdentity,
    MarkArtifactObjectsUploadedRequest, ObjectIdentity, ObjectUploadProof, OperationIdentity,
    OperationKind, OperationResult, OperationState, OperationStatus, OperationToken, PageRequest,
    PathMetadata, PathReadResult, PrepareArtifactPublishRequest, PublicationAuthority,
    PublishCondition, PublishInlineArtifactRequest, PublishResult,
    ReadRestoreSourceRunManifestRequest, RootRoute, StageArtifactManifestRequest,
    StageArtifactObjectsRequest, StagedObject, WorkspaceIdentity, WorkspacePath, WorkspaceReadView,
    WorkspaceRequest, WorkspaceResult, MAX_ARTIFACT_DEPENDENCY_DEPTH,
    MAX_ARTIFACT_DEPENDENCY_OWNERS, MAX_ARTIFACT_PUBLISH_BATCH_ROWS, MAX_ARTIFACT_READ_PLAN_ROWS,
    MAX_INLINE_ARTIFACT_BODY_BYTES,
};
use nokv_types::{ArtifactRevisionId, LogicalShardId, RootId};
use sha2::{Digest as _, Sha256};

use crate::body_digest::BodyDigestState;
use crate::{
    ArtifactPublishStage, ClientCall, ClientError, RouteResolver, RpcTransport, WorkspaceClient,
};

const ARTIFACT_READ_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum number of artifacts in one bounded range-batch attempt.
pub const MAX_ARTIFACT_RANGE_BATCH_REQUESTS: usize = 128;
/// Maximum total number of caller ranges in one bounded range-batch attempt.
pub const MAX_ARTIFACT_RANGE_BATCH_RANGES: usize = 4_096;
/// Maximum total bytes returned by one bounded range-batch attempt.
pub const MAX_ARTIFACT_RANGE_BATCH_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum total bytes fetched after per-artifact gap coalescing.
pub const MAX_ARTIFACT_RANGE_BATCH_READ_BYTES: u64 = 64 * 1024 * 1024;

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
    /// Optional expected workspace incarnation fence, evaluated atomically by
    /// the owner with the path generation claim (see
    /// `BeginArtifactPublishRequest::expected_workspace_incarnation_id`).
    pub expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
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
            expected_workspace_incarnation_id: None,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    /// Refuse the publish unless the workbench is still this exact incarnation.
    pub fn with_expected_workspace_incarnation(
        mut self,
        workspace_incarnation_id: WorkspaceIdentity,
    ) -> Self {
        self.expected_workspace_incarnation_id = Some(workspace_incarnation_id);
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
/// A conflicting create/append race derives attempt identities within one
/// invocation. This legacy API does not provide cross-process intent replay;
/// use `WorkspaceClient::append_artifact_idempotent` for a caller-stable action.
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

/// Complete path authority required before reading immutable artifact objects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactReadAuthority {
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub workspace_revision: u64,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub generation: u64,
}

impl From<&PathMetadata> for ArtifactReadAuthority {
    fn from(metadata: &PathMetadata) -> Self {
        Self {
            workspace_incarnation_id: metadata.workspace_incarnation_id,
            workspace_revision: metadata.workspace_revision,
            artifact_revision_id: metadata.artifact_revision_id,
            generation: metadata.generation,
        }
    }
}

/// Whether two metadata reads name the same immutable artifact revision.
///
/// A live paged read stays consistent while the path keeps its generation and
/// revision: the bytes are immutable. `workspace_revision` is deliberately not
/// compared, because every publication to any other path of the workspace
/// advances it; comparing it would make concurrent writers elsewhere fail an
/// otherwise unaffected read. Callers that need the exact workspace revision
/// use `read_artifact_at_authority`.
fn same_artifact_revision(left: &PathMetadata, right: &PathMetadata) -> bool {
    left.path == right.path
        && left.workspace_incarnation_id == right.workspace_incarnation_id
        && left.generation == right.generation
        && left.artifact_revision_id == right.artifact_revision_id
        && left.dependency_count == right.dependency_count
        && left.dependency_depth == right.dependency_depth
        && left.descriptor == right.descriptor
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedArtifactReadFence {
    Generation(u64),
    Authority(ArtifactReadAuthority),
}

/// Ordered ranges for one path-native artifact inside a bounded batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRangeBatchRequest {
    pub target: WorkspacePath,
    pub ranges: Vec<ByteRange>,
    pub expected_generation: Option<u64>,
    pub max_gap_bytes: u64,
}

/// Ordered range results for one artifact request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRangeBatchItem {
    pub metadata: PathMetadata,
    pub ranges: Vec<Vec<u8>>,
}

/// Complete all-or-error result of one bounded range-batch attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRangeBatchOutcome {
    pub items: Vec<ArtifactRangeBatchItem>,
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

        if publishes_inline(&options.authority, bytes, options.block_size) {
            // No provider write happens, so provider admission is not needed.
            let route = self.resolve_artifact_route()?;
            require_object_namespace(store, route)?;
            let descriptor = inline_descriptor(
                bytes,
                options.content_type.clone(),
                options.producer.clone(),
                options.manifest_identity.clone(),
                options.index_fields.clone(),
            );
            return self.publish_inline_artifact(
                route.logical_shard_id,
                options.operation_id,
                options.artifact_revision_id,
                &options.target,
                options.authority,
                options.condition,
                options.expected_workspace_incarnation_id,
                None,
                None,
                &descriptor,
                bytes,
            );
        }
        require_provider_admission(store, options.block_size)?;

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
            options.expected_workspace_incarnation_id,
            None,
            None,
            object_plan,
            staged_objects,
            manifest_rows,
            dependencies,
            descriptor,
            bytes,
        )
    }

    /// Publish a body stored inline in shard metadata with one request.
    ///
    /// The request admits and completes the publication under one durable
    /// acknowledgement. A retry with the same operation identity converges
    /// on the first durable outcome.
    #[allow(clippy::too_many_arguments)]
    fn publish_inline_artifact(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        target: &WorkspacePath,
        authority: PublicationAuthority,
        condition: PublishCondition,
        expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
        append_intent_digest: Option<Digest>,
        append_attempt: Option<AppendAttemptBinding>,
        descriptor: &ArtifactDescriptor,
        body: &[u8],
    ) -> Result<ArtifactPublishOutcome, ClientError> {
        descriptor.validate()?;
        let request = PublishInlineArtifactRequest {
            begin: BeginArtifactPublishRequest {
                operation_id,
                artifact_revision_id,
                target: target.clone(),
                authority,
                condition,
                expected_workspace_incarnation_id,
                append_intent_digest,
                append_attempt,
                staged_object_count: 0,
                staged_object_seal: Digest([0; 32]),
                manifest_row_count: 0,
                manifest_seal: Digest([0; 32]),
                dependency_owner_revision_ids: Vec::new(),
                inline_body_digest: Some(inline_artifact_body_digest(body)),
            },
            body: body.to_vec(),
            artifact: descriptor.clone(),
        };
        let stage = ArtifactPublishStage::PublishInline;
        for attempt in 1..=self.max_attempts() {
            let error = match self.execute_on_logical_shard(
                self.new_request_id(),
                WorkspaceRequest::PublishInlineArtifact(request.clone()),
                logical_shard,
            ) {
                Ok(call) => {
                    return self.inline_publication_outcome(
                        logical_shard,
                        operation_id,
                        authority,
                        append_intent_digest.is_some(),
                        call,
                    )
                }
                Err(error) => error,
            };
            // A lost response, the refusal of an epoch-bound identity after an
            // owner restart, and a reused identity all arrive here. Only this
            // publication's own durable row may stand in for its response.
            let resumable = should_resume_artifact_publication(&error);
            match self.observe_inline_publication(logical_shard, &request.begin, descriptor) {
                InlinePublicationRow::Succeeded(call) => {
                    if append_intent_digest.is_none()
                        && matches!(authority, PublicationAuthority::Visible)
                    {
                        self.confirm_replayed_publication_is_live(logical_shard, &call.value)
                            .map_err(|source| publication_failure_without_abort(stage, source))?;
                    }
                    return Ok(ArtifactPublishOutcome {
                        publication: call,
                        upload_stats: ArtifactUploadStats::default(),
                    });
                }
                InlinePublicationRow::Running(token) if !resumable => {
                    return Err(if append_intent_digest.is_some() {
                        publication_failure_without_abort(stage, error)
                    } else {
                        self.failed_publication(
                            logical_shard,
                            operation_id,
                            Some(token),
                            stage,
                            error,
                        )
                    });
                }
                InlinePublicationRow::Unobserved if !resumable => {
                    return Err(if append_intent_digest.is_some() {
                        publication_failure_without_abort(stage, error)
                    } else {
                        self.failed_publication(logical_shard, operation_id, None, stage, error)
                    });
                }
                // Nothing was admitted, or the row is terminal or belongs to
                // other inputs: there is nothing of this publication to abort.
                InlinePublicationRow::Absent | InlinePublicationRow::Unrelated if !resumable => {
                    return Err(publication_failure_without_abort(stage, error));
                }
                _ => {}
            }
            if attempt == self.max_attempts() {
                return Err(ClientError::RetryExhausted {
                    attempts: attempt,
                    last_error: Box::new(publication_failure_without_abort(stage, error)),
                });
            }
        }
        unreachable!("validated max_attempts is non-zero")
    }

    fn inline_publication_outcome(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        authority: PublicationAuthority,
        stable_append: bool,
        call: ClientCall<WorkspaceResult>,
    ) -> Result<ArtifactPublishOutcome, ClientError> {
        let stage = ArtifactPublishStage::PublishInline;
        let (replayed, commit_version) = (call.replayed, call.commit_version);
        let status = match call.value {
            WorkspaceResult::Published(value) => {
                return Ok(ArtifactPublishOutcome {
                    publication: ClientCall {
                        value,
                        commit_version,
                        replayed,
                    },
                    upload_stats: ArtifactUploadStats::default(),
                });
            }
            WorkspaceResult::Operation(status) => validated_publish_status(status, operation_id)
                .map_err(|source| publication_failure_without_abort(stage, source))?,
            _ => {
                return Err(publication_failure_without_abort(
                    stage,
                    ClientError::ResponseMismatch(
                        "inline publication returned neither a publication nor its operation"
                            .to_owned(),
                    ),
                ))
            }
        };
        match status.state {
            OperationState::Succeeded => {
                let value = published_result_from_status(&status)
                    .map_err(|source| publication_failure_without_abort(stage, source))?;
                // As for a replayed Begin, a visible create whose path was
                // removed and reclaimed must not be reported as current.
                if !stable_append && matches!(authority, PublicationAuthority::Visible) {
                    self.confirm_replayed_publication_is_live(logical_shard, &value)
                        .map_err(|source| publication_failure_without_abort(stage, source))?;
                }
                Ok(ArtifactPublishOutcome {
                    publication: ClientCall {
                        value,
                        commit_version,
                        replayed: true,
                    },
                    upload_stats: ArtifactUploadStats::default(),
                })
            }
            state => Err(publication_failure_without_abort(
                stage,
                ClientError::ResponseMismatch(format!(
                    "artifact publication operation is durably {state:?} and cannot be \
                     resumed; retry with a new operation identity"
                )),
            )),
        }
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
        expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
        append_intent_digest: Option<Digest>,
        append_attempt: Option<AppendAttemptBinding>,
        object_plan: ArtifactUploadPlan,
        staged_objects: Vec<StagedObject>,
        manifest_rows: Vec<ArtifactManifestRow>,
        dependencies: Vec<ArtifactRevisionIdentity>,
        descriptor: ArtifactDescriptor,
        bytes: &[u8],
    ) -> Result<ArtifactPublishOutcome, ClientError> {
        require_provider_admission(store, object_plan.block_size)?;
        let mut upload_stats = ArtifactUploadStats::default();
        for attempt in 1..=self.max_attempts() {
            match self.publish_artifact_plan_once(
                store,
                logical_shard,
                operation_id,
                artifact_revision_id,
                &target,
                &authority,
                &condition,
                expected_workspace_incarnation_id,
                append_intent_digest,
                append_attempt,
                &object_plan,
                &staged_objects,
                &manifest_rows,
                &dependencies,
                &descriptor,
                bytes,
                &mut upload_stats,
            ) {
                Err(error)
                    if should_resume_artifact_publication(&error)
                        && attempt < self.max_attempts() =>
                {
                    continue;
                }
                Err(error) if should_resume_artifact_publication(&error) => {
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

    #[allow(clippy::too_many_arguments)]
    fn publish_artifact_plan_once(
        &self,
        store: &dyn ArtifactObjectStore,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        target: &WorkspacePath,
        authority: &PublicationAuthority,
        condition: &PublishCondition,
        expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
        append_intent_digest: Option<Digest>,
        append_attempt: Option<AppendAttemptBinding>,
        object_plan: &ArtifactUploadPlan,
        staged_objects: &[StagedObject],
        manifest_rows: &[ArtifactManifestRow],
        dependencies: &[ArtifactRevisionIdentity],
        descriptor: &ArtifactDescriptor,
        bytes: &[u8],
        upload_stats: &mut ArtifactUploadStats,
    ) -> Result<ArtifactPublishOutcome, ClientError> {
        let fail = |known_token, stage, source| {
            if append_intent_digest.is_some() {
                publication_failure_without_abort(stage, source)
            } else {
                self.failed_publication(logical_shard, operation_id, known_token, stage, source)
            }
        };
        let fail_or_resume = |token, stage, source| {
            if append_intent_digest.is_some() {
                publication_failure_without_abort(stage, source)
            } else {
                self.failed_or_resumable_publication(
                    logical_shard,
                    operation_id,
                    token,
                    stage,
                    source,
                )
            }
        };
        let seals =
            seal_artifact_publish_plan(artifact_revision_id, staged_objects, manifest_rows)?;
        descriptor.validate()?;
        let begin = BeginArtifactPublishRequest {
            operation_id,
            artifact_revision_id,
            target: target.clone(),
            authority: *authority,
            condition: *condition,
            expected_workspace_incarnation_id,
            append_intent_digest,
            append_attempt,
            staged_object_count: seals.staged_object_count,
            staged_object_seal: seals.staged_object_seal,
            manifest_row_count: seals.manifest_row_count,
            manifest_seal: seals.manifest_seal,
            dependency_owner_revision_ids: dependencies.to_vec(),
            inline_body_digest: None,
        };
        // A plan whose objects fit one batch is admitted and staged under one
        // durable acknowledgement; a larger plan stages in batches after Begin.
        let (begin_stage, begin) = if staged_objects.len() <= MAX_ARTIFACT_PUBLISH_BATCH_ROWS {
            (
                ArtifactPublishStage::Prepare,
                self.publish_status_on_shard(
                    logical_shard,
                    WorkspaceRequest::PrepareArtifactPublish(PrepareArtifactPublishRequest {
                        begin,
                        objects: staged_objects.to_vec(),
                    }),
                ),
            )
        } else {
            (
                ArtifactPublishStage::Begin,
                self.begin_publish_on_shard(logical_shard, begin),
            )
        };
        let (mut token, resume, begin_replayed) = match begin {
            Ok(call) => {
                let (replayed, commit_version) = (call.replayed, call.commit_version);
                let status = match validated_publish_status(call.value, operation_id) {
                    Ok(status) => status,
                    Err(source) => {
                        return Err(fail(None, begin_stage, source));
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
                                return Err(fail(None, begin_stage, source));
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
                        if append_intent_digest.is_none()
                            && matches!(authority, PublicationAuthority::Visible)
                        {
                            if let Err(source) =
                                self.confirm_replayed_publication_is_live(logical_shard, &value)
                            {
                                return Err(ClientError::ArtifactPublishFailed {
                                    stage: begin_stage,
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
                            upload_stats: *upload_stats,
                        });
                    }
                    OperationState::Aborting
                    | OperationState::Failed
                    | OperationState::Quarantined => {
                        // The operation identity is durably spent. Aborting it
                        // again is meaningless, so report the terminal state
                        // instead of attempting one.
                        return Err(ClientError::ArtifactPublishFailed {
                            stage: begin_stage,
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
                            return Err(fail(None, begin_stage, source));
                        }
                    },
                }
            }
            Err(source) if is_definitive_append_race(&source) => return Err(source),
            Err(source) => return Err(publication_failure_without_abort(begin_stage, source)),
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
                        return Err(fail(
                            Some(token),
                            ArtifactPublishStage::StageObjects,
                            source,
                        ));
                    }
                },
                Err(source) => {
                    return Err(fail_or_resume(
                        token,
                        ArtifactPublishStage::StageObjects,
                        source,
                    ))
                }
            }
        }

        if resume.uploaded_object_cursor == 0 {
            let upload = match upload_artifact_from_plan(store, object_plan, bytes) {
                Ok(upload) => upload,
                Err(source) => {
                    return Err(fail_or_resume(
                        token,
                        ArtifactPublishStage::UploadObjects,
                        ClientError::ArtifactUpload(Box::new(source)),
                    ));
                }
            };
            accumulate_upload_stats(upload_stats, upload.stats);
        }

        let upload_proofs = upload_proofs(staged_objects);
        let remaining_proofs = &upload_proofs[resume.uploaded_object_cursor..];
        let remaining_rows = &manifest_rows[resume.manifest_cursor..];
        let (final_stage, final_result) = if remaining_proofs.len()
            <= MAX_ARTIFACT_PUBLISH_BATCH_ROWS
            && remaining_rows.len() <= MAX_ARTIFACT_PUBLISH_BATCH_ROWS
        {
            // The remaining upload proofs, the manifest, and the completion
            // share one durable acknowledgement.
            let finish = FinishArtifactPublishRequest {
                token,
                uploaded_objects: remaining_proofs.to_vec(),
                manifest_rows: remaining_rows.to_vec(),
                dependency_owner_revision_ids: dependencies.to_vec(),
                artifact: descriptor.clone(),
            };
            (
                ArtifactPublishStage::Finish,
                self.execute_on_logical_shard(
                    self.new_request_id(),
                    WorkspaceRequest::FinishArtifactPublish(finish),
                    logical_shard,
                ),
            )
        } else {
            for batch in remaining_proofs.chunks(MAX_ARTIFACT_PUBLISH_BATCH_ROWS) {
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
                            return Err(fail(
                                Some(token),
                                ArtifactPublishStage::MarkObjectsUploaded,
                                source,
                            ));
                        }
                    },
                    Err(source) => {
                        return Err(fail_or_resume(
                            token,
                            ArtifactPublishStage::MarkObjectsUploaded,
                            source,
                        ))
                    }
                }
            }

            for batch in remaining_rows.chunks(MAX_ARTIFACT_PUBLISH_BATCH_ROWS) {
                let request = StageArtifactManifestRequest {
                    token,
                    rows: batch.to_vec(),
                    dependency_owner_revision_ids: dependencies.to_vec(),
                };
                match self.publish_status_on_shard(
                    logical_shard,
                    WorkspaceRequest::StageArtifactManifest(request),
                ) {
                    Ok(status) => match running_publish_token(status.value, operation_id) {
                        Ok(next_token) => token = next_token,
                        Err(source) => {
                            return Err(fail(
                                Some(token),
                                ArtifactPublishStage::StageManifest,
                                source,
                            ));
                        }
                    },
                    Err(source) => {
                        return Err(fail_or_resume(
                            token,
                            ArtifactPublishStage::StageManifest,
                            source,
                        ))
                    }
                }
            }

            let complete = CompleteArtifactPublishRequest {
                token,
                artifact: descriptor.clone(),
            };
            (
                ArtifactPublishStage::Complete,
                self.execute_on_logical_shard(
                    self.new_request_id(),
                    WorkspaceRequest::CompleteArtifactPublish(complete),
                    logical_shard,
                ),
            )
        };
        let mut publication = match final_result.and_then(|call| call.map(expect_published)) {
            Ok(publication) => publication,
            Err(source) => match self.recover_completed_publish(logical_shard, operation_id) {
                Ok(Some(recovered)) => recovered,
                Ok(None) => {
                    return Err(fail_or_resume(token, final_stage, source));
                }
                Err(recovery_error) => {
                    return Err(fail_or_resume(token, final_stage, recovery_error));
                }
            },
        };
        // A fresh Prepare reports its own staging as progress; an earlier
        // attempt of this publication always replays its admission.
        publication.replayed |= begin_replayed
            || (begin_stage == ArtifactPublishStage::Begin && resume.completed_rows != 0);

        Ok(ArtifactPublishOutcome {
            publication,
            upload_stats: *upload_stats,
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
        require_provider_admission(store, options.block_size)?;
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
                None,
                None,
                None,
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_artifact_attempt(
        &self,
        store: &dyn ArtifactObjectStore,
        options: &ArtifactAppendOptions,
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        delta: &[u8],
        expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
        append_intent_digest: Option<Digest>,
        append_attempt: Option<AppendAttemptBinding>,
    ) -> Result<ArtifactAppendOutcome, ClientError> {
        require_provider_admission(store, options.block_size)?;
        if append_attempt.is_some()
            && !store
                .provider_admission_receipt()
                .is_some_and(|receipt| receipt.admits_append_store(store, options.block_size))
        {
            return Err(ObjectError::ProviderAdmissionRequired.into());
        }
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
            owned_body,
            body_digest,
        ) = match metadata {
            None => {
                enforce_append_size(delta_len, options.max_logical_size)?;
                if publishes_inline(&PublicationAuthority::Visible, delta, options.block_size) {
                    let descriptor = inline_descriptor(
                        delta,
                        options
                            .content_type
                            .clone()
                            .unwrap_or_else(|| options.create_content_type.clone()),
                        None,
                        None,
                        Vec::new(),
                    );
                    let outcome = self.publish_inline_artifact(
                        logical_shard,
                        operation_id,
                        artifact_revision_id,
                        &options.target,
                        PublicationAuthority::Visible,
                        PublishCondition::CreateOnly,
                        expected_workspace_incarnation_id,
                        append_intent_digest,
                        append_attempt,
                        &descriptor,
                        delta,
                    )?;
                    return Ok(ArtifactAppendOutcome {
                        publication: outcome.publication,
                        descriptor,
                        upload_stats: outcome.upload_stats,
                        base_read_stats: ArtifactReadStats::default(),
                        created: true,
                    });
                }
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
                let mut body_digest = BodyDigestState::new();
                body_digest.update(delta);
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
                    body_digest,
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
                    let loaded = self.load_artifact_range_rows(
                        logical_shard,
                        &options.target,
                        WorkspaceReadView::Live,
                        ByteRange {
                            offset: 0,
                            length: metadata.descriptor.logical_size,
                        },
                        Some(&metadata),
                    )?;
                    if let Some(base) = loaded.inline_body {
                        return self.append_to_inline_base(
                            store,
                            options,
                            logical_shard,
                            operation_id,
                            artifact_revision_id,
                            &metadata,
                            base,
                            delta,
                            expected_workspace_incarnation_id,
                            append_intent_digest,
                            append_attempt,
                        );
                    }
                    loaded.rows
                };
                let base_manifest =
                    manifest_from_rows(self.root_id(), logical_shard, &metadata, &base_rows)?;
                let layout = append_layout(&metadata, artifact_revision_id, &base_rows, delta_len)?;
                let condition = PublishCondition::Append {
                    expected_generation: Some(metadata.generation),
                };

                // The new revision owns the re-owned base suffix, if any, and
                // the delta; it borrows every row before that suffix.
                let base = self.append_base(
                    store,
                    &metadata,
                    &base_rows,
                    &base_manifest,
                    layout.reown_from_row,
                )?;
                let mut body_digest = base.digest;
                body_digest.update(delta);
                let reowns_base = layout.reown_from_row < base_rows.len();
                let mut owned = base.reowned;
                owned.try_reserve(delta.len()).map_err(|_| {
                    ClientError::InvalidOptions(
                        "append result cannot be materialized in client memory".to_owned(),
                    )
                })?;
                owned.extend_from_slice(delta);
                let object_plan = plan_artifact_upload(
                    ArtifactUploadOptions::new(
                        LogicalShardId::from(logical_shard),
                        RootId::from(self.root_id()),
                        ArtifactRevisionId::from(artifact_revision_id),
                    )
                    .with_block_size(options.block_size),
                    &owned,
                )?;
                let (staged_objects, mut owned_rows) =
                    publication_rows(artifact_revision_id, &object_plan.manifest)?;
                let first_owned_index = u64::try_from(layout.reown_from_row).map_err(|_| {
                    ClientError::InvalidOptions("base manifest row count exceeds u64".to_owned())
                })?;
                let owned_offset = base_rows
                    .get(layout.reown_from_row)
                    .map_or(metadata.descriptor.logical_size, |row| row.logical_offset);
                // Only a delta appended after every borrowed row is a new
                // append segment; re-owned bytes are recorded like a
                // rematerialized body.
                let segment_sequence = if reowns_base {
                    None
                } else {
                    Some(next_append_segment_sequence(&base_rows)?)
                };
                for row in &mut owned_rows {
                    let segment_offset = row.logical_offset;
                    row.object_index =
                        row.object_index
                            .checked_add(first_owned_index)
                            .ok_or_else(|| {
                                ClientError::InvalidOptions(
                                    "append manifest object index overflows".to_owned(),
                                )
                            })?;
                    row.logical_offset =
                        row.logical_offset
                            .checked_add(owned_offset)
                            .ok_or_else(|| {
                                ClientError::InvalidOptions(
                                    "append manifest logical offset overflows".to_owned(),
                                )
                            })?;
                    row.append_segment = segment_sequence.map(|segment_sequence| AppendSegment {
                        segment_sequence,
                        segment_offset,
                    });
                }

                let mut manifest_rows = base_rows;
                manifest_rows.truncate(layout.reown_from_row);
                manifest_rows.extend(owned_rows);
                let seals = seal_artifact_publish_plan(
                    artifact_revision_id,
                    &staged_objects,
                    &manifest_rows,
                )?;
                let mut descriptor = metadata.descriptor;
                descriptor.logical_size = logical_size;
                descriptor.body_digest = sha256_digest_uri(Digest(body_digest.digest()));
                descriptor.manifest_digest = sha256_digest_uri(seals.manifest_seal);
                if let Some(content_type) = &options.content_type {
                    descriptor.content_type = content_type.clone();
                }
                (
                    condition,
                    object_plan,
                    staged_objects,
                    manifest_rows,
                    layout.owners,
                    descriptor,
                    base.read_stats,
                    false,
                    Some(owned),
                    body_digest,
                )
            }
        };

        let result_descriptor = descriptor.clone();
        let upload_bytes = owned_body.as_deref().unwrap_or(delta);
        let published_rows = manifest_rows.clone();
        let outcome = self.publish_artifact_plan(
            store,
            logical_shard,
            operation_id,
            artifact_revision_id,
            options.target.clone(),
            PublicationAuthority::Visible,
            condition,
            expected_workspace_incarnation_id,
            append_intent_digest,
            append_attempt,
            object_plan,
            staged_objects,
            manifest_rows,
            dependencies,
            descriptor,
            upload_bytes,
        )?;
        self.body_digests()
            .insert(artifact_revision_id, body_digest, &published_rows);
        Ok(ArtifactAppendOutcome {
            publication: outcome.publication,
            descriptor: result_descriptor,
            upload_stats: outcome.upload_stats,
            base_read_stats,
            created,
        })
    }

    /// Append to an inline base. The base owns no objects to borrow, so the
    /// result is a complete body owned by the new revision: inline while it
    /// fits the bound, otherwise rematerialized into dependency-free objects.
    #[allow(clippy::too_many_arguments)]
    fn append_to_inline_base(
        &self,
        store: &dyn ArtifactObjectStore,
        options: &ArtifactAppendOptions,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        artifact_revision_id: ArtifactRevisionIdentity,
        metadata: &PathMetadata,
        mut body: Vec<u8>,
        delta: &[u8],
        expected_workspace_incarnation_id: Option<WorkspaceIdentity>,
        append_intent_digest: Option<Digest>,
        append_attempt: Option<AppendAttemptBinding>,
    ) -> Result<ArtifactAppendOutcome, ClientError> {
        body.try_reserve(delta.len()).map_err(|_| {
            ClientError::InvalidOptions(
                "append result cannot be materialized in client memory".to_owned(),
            )
        })?;
        body.extend_from_slice(delta);
        let condition = PublishCondition::Append {
            expected_generation: Some(metadata.generation),
        };
        let content_type = options
            .content_type
            .clone()
            .unwrap_or_else(|| metadata.descriptor.content_type.clone());
        if publishes_inline(&PublicationAuthority::Visible, &body, options.block_size) {
            let descriptor = inline_descriptor(
                &body,
                content_type,
                metadata.descriptor.producer.clone(),
                metadata.descriptor.manifest_identity.clone(),
                metadata.descriptor.index_fields.clone(),
            );
            let outcome = self.publish_inline_artifact(
                logical_shard,
                operation_id,
                artifact_revision_id,
                &options.target,
                PublicationAuthority::Visible,
                condition,
                expected_workspace_incarnation_id,
                append_intent_digest,
                append_attempt,
                &descriptor,
                &body,
            )?;
            return Ok(ArtifactAppendOutcome {
                publication: outcome.publication,
                descriptor,
                upload_stats: outcome.upload_stats,
                base_read_stats: ArtifactReadStats::default(),
                created: false,
            });
        }
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
        let seals =
            seal_artifact_publish_plan(artifact_revision_id, &staged_objects, &manifest_rows)?;
        let mut descriptor = metadata.descriptor.clone();
        descriptor.logical_size = object_plan.manifest.logical_len;
        descriptor.body_digest = sha256_digest_uri(Digest(object_plan.manifest.sha256));
        descriptor.manifest_digest = sha256_digest_uri(seals.manifest_seal);
        descriptor.content_type = content_type;
        let mut body_digest = BodyDigestState::new();
        body_digest.update(&body);
        let published_rows = manifest_rows.clone();
        let outcome = self.publish_artifact_plan(
            store,
            logical_shard,
            operation_id,
            artifact_revision_id,
            options.target.clone(),
            PublicationAuthority::Visible,
            condition,
            expected_workspace_incarnation_id,
            append_intent_digest,
            append_attempt,
            object_plan,
            staged_objects,
            manifest_rows,
            Vec::new(),
            descriptor.clone(),
            &body,
        )?;
        self.body_digests()
            .insert(artifact_revision_id, body_digest, &published_rows);
        Ok(ArtifactAppendOutcome {
            publication: outcome.publication,
            descriptor,
            upload_stats: outcome.upload_stats,
            base_read_stats: ArtifactReadStats::default(),
            created: false,
        })
    }

    pub fn read_artifact(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        self.read_artifact_with_expected_fence(store, cache, target, view, None)
    }

    /// Read one complete artifact only if its authoritative generation matches.
    ///
    /// The generation is checked against the frozen path metadata before any
    /// object read. Empty artifacts still pass through canonical manifest and
    /// body-digest validation; callers cannot use this as a metadata-only
    /// shortcut.
    pub fn read_artifact_at_generation(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
        expected_generation: u64,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        if expected_generation == 0 {
            return Err(ClientError::InvalidOptions(
                "expected artifact generation must be greater than zero".to_owned(),
            ));
        }
        self.read_artifact_with_expected_fence(
            store,
            cache,
            target,
            view,
            Some(ExpectedArtifactReadFence::Generation(expected_generation)),
        )
    }

    /// Read one complete artifact only if its full path authority matches.
    ///
    /// The authority is checked after the metadata point read and before any
    /// manifest or object read, including for a canonical empty artifact.
    pub fn read_artifact_at_authority(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
        expected_authority: ArtifactReadAuthority,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        if expected_authority.generation == 0 {
            return Err(ClientError::InvalidOptions(
                "expected artifact generation must be greater than zero".to_owned(),
            ));
        }
        self.read_artifact_with_expected_fence(
            store,
            cache,
            target,
            view,
            Some(ExpectedArtifactReadFence::Authority(expected_authority)),
        )
    }

    /// Resolve metadata only if the current path has the exact authority.
    ///
    /// This is a bounded preflight for callers that must enforce a logical
    /// size policy before invoking [`Self::read_artifact_at_authority`]. The
    /// full read repeats the same fence before touching immutable objects.
    pub fn artifact_metadata_at_authority(
        &self,
        target: WorkspacePath,
        view: WorkspaceReadView,
        expected_authority: ArtifactReadAuthority,
    ) -> Result<PathMetadata, ClientError> {
        if expected_authority.generation == 0 {
            return Err(ClientError::InvalidOptions(
                "expected artifact generation must be greater than zero".to_owned(),
            ));
        }
        let route = self.resolve_artifact_route()?;
        let metadata = self.load_artifact_metadata(route.logical_shard_id, &target, view)?;
        if ArtifactReadAuthority::from(&metadata) != expected_authority {
            return Err(ClientError::ArtifactReadFenceChanged);
        }
        Ok(metadata)
    }

    fn read_artifact_with_expected_fence(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
        expected_fence: Option<ExpectedArtifactReadFence>,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        for attempt in 1..=self.max_attempts() {
            match self.read_artifact_once(
                store,
                cache,
                target.clone(),
                view.clone(),
                expected_fence,
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

    /// Reads ordered ranges from path-native artifacts through one bounded SDK
    /// attempt. Every unique target is resolved to authoritative metadata once
    /// per attempt. This fences all windows of that artifact to one generation
    /// and revision, but does not claim a global snapshot across distinct live
    /// targets. Use a snapshot read view when callers need a shared frozen view.
    pub fn read_artifact_ranges_batch(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        requests: Vec<ArtifactRangeBatchRequest>,
        view: WorkspaceReadView,
    ) -> Result<ArtifactRangeBatchOutcome, ClientError> {
        validate_artifact_range_batch_shape(&requests)?;
        for attempt in 1..=self.max_attempts() {
            match self.read_artifact_ranges_batch_once(store, cache, &requests, view.clone()) {
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

    /// Materializes the immutable source commit run manifest retained by one
    /// restore operation. The operation, rather than a caller-selected path or
    /// read view, is the authority for every metadata and read-plan page.
    pub(crate) fn read_restore_source_run_manifest_artifact(
        &self,
        store: &dyn ArtifactObjectStore,
        operation_id: OperationIdentity,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        for attempt in 1..=self.max_attempts() {
            match self.read_restore_source_run_manifest_artifact_once(store, operation_id) {
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

    /// The base digest state and re-owned bytes of an append, read from the
    /// longest body prefix whose digest state this client already holds.
    fn append_base(
        &self,
        store: &dyn ArtifactObjectStore,
        metadata: &PathMetadata,
        rows: &[ArtifactManifestRow],
        manifest: &ArtifactManifest,
        reown_from_row: usize,
    ) -> Result<AppendBase, ClientError> {
        let cached = {
            let digests = self.body_digests();
            digests
                .exact(metadata.artifact_revision_id, rows)
                .map(|digest| (rows.len(), digest))
                .or_else(|| digests.longest_prefix(rows))
        };
        if let Some((covered_rows, digest)) = cached {
            match read_append_base(store, manifest, covered_rows, digest, reown_from_row) {
                // A cached state that does not reproduce the sealed body
                // digest is discarded for a complete read.
                Err(ClientError::ArtifactIntegrity(_)) => {}
                result => return result,
            }
        }
        read_append_base(store, manifest, 0, BodyDigestState::new(), reown_from_row)
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

    fn failed_or_resumable_publication(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        token: OperationToken,
        stage: ArtifactPublishStage,
        source: ClientError,
    ) -> ClientError {
        if should_resume_artifact_publication(&source) {
            publication_failure_without_abort(stage, source)
        } else {
            self.failed_publication(logical_shard, operation_id, Some(token), stage, source)
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

    /// Read the durable row of one inline publication after its request
    /// failed, accepting a receipt only when it records this publication.
    fn observe_inline_publication(
        &self,
        logical_shard: LogicalShardIdentity,
        begin: &BeginArtifactPublishRequest,
        descriptor: &ArtifactDescriptor,
    ) -> InlinePublicationRow {
        let observed = match self.load_operation_on_shard(logical_shard, begin.operation_id) {
            Ok(observed) => observed,
            Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => {
                return InlinePublicationRow::Absent
            }
            Err(_) => return InlinePublicationRow::Unobserved,
        };
        let Ok(status) = validated_publish_status(observed.value, begin.operation_id) else {
            return InlinePublicationRow::Unrelated;
        };
        let prepared_here = status
            .publish_preparation
            .as_deref()
            .is_some_and(|preparation| {
                preparation.target == begin.target
                    && preparation.artifact_revision_id == begin.artifact_revision_id
            });
        match status.state {
            OperationState::Running if prepared_here => InlinePublicationRow::Running(status.token),
            OperationState::Succeeded => match published_result_from_status(&status) {
                Ok(value) if records_inline_publication(&value, begin, descriptor) => {
                    InlinePublicationRow::Succeeded(ClientCall {
                        value,
                        commit_version: observed.commit_version,
                        replayed: true,
                    })
                }
                _ => InlinePublicationRow::Unrelated,
            },
            _ => InlinePublicationRow::Unrelated,
        }
    }

    fn read_artifact_once(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        target: WorkspacePath,
        view: WorkspaceReadView,
        expected_fence: Option<ExpectedArtifactReadFence>,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;
        let metadata =
            self.load_artifact_metadata(route.logical_shard_id, &target, view.clone())?;
        let fence_matches = match expected_fence {
            None => true,
            Some(ExpectedArtifactReadFence::Generation(generation)) => {
                metadata.generation == generation
            }
            Some(ExpectedArtifactReadFence::Authority(authority)) => {
                ArtifactReadAuthority::from(&metadata) == authority
            }
        };
        if !fence_matches {
            return Err(ClientError::ArtifactReadFenceChanged);
        }
        let logical_len = metadata.descriptor.logical_size;
        let output_len = usize::try_from(logical_len).map_err(|_| {
            ClientError::ArtifactIntegrity("artifact length is not addressable".to_owned())
        })?;
        if logical_len == 0 {
            let manifest =
                manifest_from_rows(self.root_id(), route.logical_shard_id, &metadata, &[])?;
            verify_artifact_bytes(&manifest, &[])
                .map_err(|error| ClientError::ArtifactIntegrity(error.to_string()))?;
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
            if let Some(body) = loaded.inline_body {
                // The first window of an inline revision is its whole body,
                // already verified against the sealed descriptor.
                return Ok(ArtifactReadOutcome {
                    metadata,
                    bytes: body,
                    stats,
                });
            }
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
        verify_artifact_bytes(&manifest, &bytes)
            .map_err(|error| ClientError::ArtifactIntegrity(error.to_string()))?;
        let mut digest = BodyDigestState::new();
        digest.update(&bytes);
        self.body_digests()
            .insert(metadata.artifact_revision_id, digest, &rows);
        Ok(ArtifactReadOutcome {
            metadata,
            bytes,
            stats,
        })
    }

    fn read_restore_source_run_manifest_artifact_once(
        &self,
        store: &dyn ArtifactObjectStore,
        operation_id: OperationIdentity,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;
        let metadata =
            self.load_restore_source_run_manifest_metadata(route.logical_shard_id, operation_id)?;
        let logical_len = metadata.descriptor.logical_size;
        let output_len = usize::try_from(logical_len).map_err(|_| {
            ClientError::ArtifactIntegrity(
                "restore source run manifest length is not addressable".to_owned(),
            )
        })?;
        if logical_len == 0 {
            return Err(ClientError::ArtifactIntegrity(
                "restore source run manifest must not be empty".to_owned(),
            ));
        }

        let mut bytes = Vec::with_capacity(output_len);
        let mut stats = ArtifactReadStats::default();
        let mut complete_rows = BTreeMap::<u64, ArtifactManifestRow>::new();
        let mut offset = 0_u64;
        while offset < logical_len {
            let window_len_u64 = (logical_len - offset).min(ARTIFACT_READ_WINDOW_BYTES);
            let window_len = usize::try_from(window_len_u64).map_err(|_| {
                ClientError::ArtifactIntegrity(
                    "restore source read window length is not addressable".to_owned(),
                )
            })?;
            let loaded = self.load_restore_source_run_manifest_range_rows(
                route.logical_shard_id,
                operation_id,
                ByteRange {
                    offset,
                    length: window_len_u64,
                },
                &metadata,
            )?;
            let window = window_from_rows(
                self.root_id(),
                route.logical_shard_id,
                &loaded.metadata,
                &loaded.rows,
                offset,
                window_len,
            )?;
            let read = read_artifact_window(store, None, &window, offset, window_len)?;
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
        verify_artifact_bytes(&manifest, &bytes)
            .map_err(|error| ClientError::ArtifactIntegrity(error.to_string()))?;
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
        let (bytes, stats) = read_loaded_range(
            store,
            cache,
            self.root_id(),
            route.logical_shard_id,
            &loaded,
            offset,
            len,
        )?;
        Ok(ArtifactReadOutcome {
            metadata: loaded.metadata,
            bytes,
            stats,
        })
    }

    fn read_artifact_ranges_batch_once(
        &self,
        store: &dyn ArtifactObjectStore,
        cache: Option<&dyn ArtifactBlockCache>,
        requests: &[ArtifactRangeBatchRequest],
        view: WorkspaceReadView,
    ) -> Result<ArtifactRangeBatchOutcome, ClientError> {
        let route = self.resolve_artifact_route()?;
        require_object_namespace(store, route)?;

        let mut frozen = Vec::<(WorkspacePath, PathMetadata)>::new();
        let mut planned = Vec::with_capacity(requests.len());
        let mut planned_read_bytes = 0_u64;
        for request in requests {
            let metadata = match frozen.iter().find(|(target, _)| target == &request.target) {
                Some((_, metadata)) => metadata.clone(),
                None => {
                    let metadata = self.load_artifact_metadata(
                        route.logical_shard_id,
                        &request.target,
                        view.clone(),
                    )?;
                    frozen.push((request.target.clone(), metadata.clone()));
                    metadata
                }
            };
            if request
                .expected_generation
                .is_some_and(|expected| expected != metadata.generation)
            {
                return Err(ClientError::ArtifactReadFenceChanged);
            }
            validate_ranges_within_artifact(request, metadata.descriptor.logical_size)?;
            let merged = coalesce_artifact_ranges(&request.ranges, request.max_gap_bytes)?;
            for range in &merged {
                planned_read_bytes =
                    planned_read_bytes
                        .checked_add(range.length())
                        .ok_or_else(|| {
                            ClientError::InvalidOptions(
                                "range batch coalesced read bytes overflow u64".to_owned(),
                            )
                        })?;
                if planned_read_bytes > MAX_ARTIFACT_RANGE_BATCH_READ_BYTES {
                    return Err(ClientError::InvalidOptions(format!(
                        "range batch coalesced reads exceed {MAX_ARTIFACT_RANGE_BATCH_READ_BYTES} bytes"
                    )));
                }
            }
            planned.push(PlannedArtifactRangeBatchItem {
                request,
                metadata,
                merged,
            });
        }

        let mut items = Vec::with_capacity(planned.len());
        let mut total_stats = ArtifactReadStats::default();
        for item in planned {
            let mut outputs = vec![None; item.request.ranges.len()];
            for merged in item.merged {
                let len = usize::try_from(merged.length()).map_err(|_| {
                    ClientError::InvalidOptions(
                        "coalesced range length is not addressable".to_owned(),
                    )
                })?;
                let loaded = self.load_artifact_range_rows(
                    route.logical_shard_id,
                    &item.request.target,
                    view.clone(),
                    ByteRange {
                        offset: merged.offset,
                        length: merged.length(),
                    },
                    Some(&item.metadata),
                )?;
                let (read_bytes, read_stats) = read_loaded_range(
                    store,
                    cache,
                    self.root_id(),
                    route.logical_shard_id,
                    &loaded,
                    merged.offset,
                    len,
                )?;
                if read_bytes.len() != len {
                    return Err(ClientError::ArtifactIntegrity(format!(
                        "coalesced range returned {} bytes, expected {len}",
                        read_bytes.len()
                    )));
                }
                merge_read_stats(&mut total_stats, read_stats);
                for member in merged.members {
                    let range = item.request.ranges[member];
                    let start = usize::try_from(range.offset - merged.offset).map_err(|_| {
                        ClientError::ArtifactIntegrity(
                            "range scatter offset is not addressable".to_owned(),
                        )
                    })?;
                    let range_len = usize::try_from(range.length).map_err(|_| {
                        ClientError::ArtifactIntegrity(
                            "range scatter length is not addressable".to_owned(),
                        )
                    })?;
                    let end = start.checked_add(range_len).ok_or_else(|| {
                        ClientError::ArtifactIntegrity(
                            "range scatter end overflows usize".to_owned(),
                        )
                    })?;
                    let bytes = read_bytes.get(start..end).ok_or_else(|| {
                        ClientError::ArtifactIntegrity(
                            "coalesced range omitted requested scatter bytes".to_owned(),
                        )
                    })?;
                    if outputs[member].replace(bytes.to_vec()).is_some() {
                        return Err(ClientError::ArtifactIntegrity(
                            "range batch produced one result slot twice".to_owned(),
                        ));
                    }
                }
            }
            let ranges = outputs
                .into_iter()
                .enumerate()
                .map(|(index, output)| {
                    output.ok_or_else(|| {
                        ClientError::ArtifactIntegrity(format!(
                            "range batch omitted result slot {index}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            items.push(ArtifactRangeBatchItem {
                metadata: item.metadata,
                ranges,
            });
        }
        Ok(ArtifactRangeBatchOutcome {
            items,
            stats: total_stats,
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
                    expected_read_version: None,
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

    fn load_restore_source_run_manifest_metadata(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
    ) -> Result<PathMetadata, ClientError> {
        let result = self
            .execute_on_logical_shard(
                self.new_request_id(),
                WorkspaceRequest::ReadRestoreSourceRunManifest(
                    ReadRestoreSourceRunManifestRequest {
                        operation_id,
                        range: None,
                        plan_page: None,
                    },
                ),
                logical_shard,
            )?
            .map(expect_restore_source_run_manifest)?
            .value;
        validate_restore_source_run_manifest_result(&result, None)?;
        result.metadata.ok_or_else(|| {
            ClientError::ResponseMismatch(
                "restore source run manifest read omitted path metadata".to_owned(),
            )
        })
    }

    fn load_restore_source_run_manifest_range_rows(
        &self,
        logical_shard: LogicalShardIdentity,
        operation_id: OperationIdentity,
        range: ByteRange,
        expected_metadata: &PathMetadata,
    ) -> Result<LoadedRangeRows, ClientError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut rows = Vec::new();
        loop {
            let result = self
                .execute_on_logical_shard(
                    self.new_request_id(),
                    WorkspaceRequest::ReadRestoreSourceRunManifest(
                        ReadRestoreSourceRunManifestRequest {
                            operation_id,
                            range: Some(range),
                            plan_page: Some(PageRequest {
                                cursor: cursor.clone(),
                                limit: MAX_ARTIFACT_READ_PLAN_ROWS as u32,
                            }),
                        },
                    ),
                    logical_shard,
                )?
                .map(expect_restore_source_run_manifest)?
                .value;
            validate_restore_source_run_manifest_result(&result, Some(range))?;
            let page_metadata = result.metadata.ok_or_else(|| {
                ClientError::ResponseMismatch(
                    "restore source range plan omitted path metadata".to_owned(),
                )
            })?;
            if !same_artifact_revision(&page_metadata, expected_metadata) {
                return Err(ClientError::ArtifactReadFenceChanged);
            }
            append_range_page(&mut rows, result.blocks)?;
            let Some(next_cursor) = result.next_cursor else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(ClientError::ArtifactIntegrity(
                    "restore source range-plan cursor loop detected".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        validate_range_coverage(&rows, range)?;
        Ok(LoadedRangeRows {
            metadata: expected_metadata.clone(),
            rows,
            inline_body: None,
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
                        expected_read_version: None,
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
                .is_some_and(|expected| !same_artifact_revision(expected, &page_metadata))
            {
                return Err(ClientError::ArtifactReadFenceChanged);
            }
            metadata.get_or_insert_with(|| page_metadata.clone());
            if let Some(body) = result.inline_body {
                if cursor.is_some() || !rows.is_empty() {
                    return Err(ClientError::ArtifactIntegrity(
                        "an inline body followed a block-plan page".to_owned(),
                    ));
                }
                let metadata = metadata.expect("every valid range page includes metadata");
                verify_inline_body(&metadata, &body)?;
                return Ok(LoadedRangeRows {
                    metadata,
                    rows,
                    inline_body: Some(body),
                });
            }
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
            inline_body: None,
        })
    }
}

struct PlannedArtifactRangeBatchItem<'a> {
    request: &'a ArtifactRangeBatchRequest,
    metadata: PathMetadata,
    merged: Vec<CoalescedArtifactRange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CoalescedArtifactRange {
    offset: u64,
    end: u64,
    members: Vec<usize>,
}

impl CoalescedArtifactRange {
    fn length(&self) -> u64 {
        self.end - self.offset
    }
}

fn validate_artifact_range_batch_shape(
    requests: &[ArtifactRangeBatchRequest],
) -> Result<(), ClientError> {
    if requests.is_empty() {
        return Err(ClientError::InvalidOptions(
            "range batch must contain at least one artifact request".to_owned(),
        ));
    }
    if requests.len() > MAX_ARTIFACT_RANGE_BATCH_REQUESTS {
        return Err(ClientError::InvalidOptions(format!(
            "range batch contains {} artifact requests, maximum is {MAX_ARTIFACT_RANGE_BATCH_REQUESTS}",
            requests.len()
        )));
    }
    let mut range_count = 0_usize;
    let mut output_bytes = 0_u64;
    for request in requests {
        if request.ranges.is_empty() {
            return Err(ClientError::InvalidOptions(format!(
                "range batch request for {:?} has no ranges",
                request.target.path.as_str()
            )));
        }
        if request.expected_generation == Some(0) {
            return Err(ClientError::InvalidOptions(
                "range batch expected generation must be greater than zero".to_owned(),
            ));
        }
        range_count = range_count
            .checked_add(request.ranges.len())
            .ok_or_else(|| {
                ClientError::InvalidOptions("range batch range count overflows usize".to_owned())
            })?;
        if range_count > MAX_ARTIFACT_RANGE_BATCH_RANGES {
            return Err(ClientError::InvalidOptions(format!(
                "range batch contains {range_count} ranges, maximum is {MAX_ARTIFACT_RANGE_BATCH_RANGES}"
            )));
        }
        for range in &request.ranges {
            range.validate()?;
            output_bytes = output_bytes.checked_add(range.length).ok_or_else(|| {
                ClientError::InvalidOptions("range batch output bytes overflow u64".to_owned())
            })?;
            if output_bytes > MAX_ARTIFACT_RANGE_BATCH_OUTPUT_BYTES {
                return Err(ClientError::InvalidOptions(format!(
                    "range batch output exceeds {MAX_ARTIFACT_RANGE_BATCH_OUTPUT_BYTES} bytes"
                )));
            }
        }
    }
    Ok(())
}

fn validate_ranges_within_artifact(
    request: &ArtifactRangeBatchRequest,
    logical_size: u64,
) -> Result<(), ClientError> {
    for range in &request.ranges {
        let end = range.offset.checked_add(range.length).ok_or_else(|| {
            ClientError::InvalidOptions("range offset plus length overflows u64".to_owned())
        })?;
        if end > logical_size {
            return Err(ClientError::InvalidOptions(format!(
                "range [{}, {end}) exceeds artifact {:?} length {logical_size}",
                range.offset,
                request.target.path.as_str()
            )));
        }
    }
    Ok(())
}

fn coalesce_artifact_ranges(
    ranges: &[ByteRange],
    max_gap_bytes: u64,
) -> Result<Vec<CoalescedArtifactRange>, ClientError> {
    let mut ordered = ranges
        .iter()
        .copied()
        .enumerate()
        .map(|(index, range)| {
            let end = range.offset.checked_add(range.length).ok_or_else(|| {
                ClientError::InvalidOptions("range offset plus length overflows u64".to_owned())
            })?;
            Ok((range.offset, end, index))
        })
        .collect::<Result<Vec<_>, ClientError>>()?;
    ordered.sort_by_key(|(offset, end, index)| (*offset, *end, *index));

    let mut merged = Vec::<CoalescedArtifactRange>::new();
    for (offset, end, index) in ordered {
        let append = merged.last_mut().is_some_and(|current| {
            let gap = offset.saturating_sub(current.end);
            let candidate_end = current.end.max(end);
            gap <= max_gap_bytes && candidate_end - current.offset <= ARTIFACT_READ_WINDOW_BYTES
        });
        if append {
            let current = merged
                .last_mut()
                .expect("append decision requires one coalesced range");
            current.end = current.end.max(end);
            current.members.push(index);
        } else {
            if end - offset > ARTIFACT_READ_WINDOW_BYTES {
                return Err(ClientError::InvalidOptions(format!(
                    "one range exceeds the {ARTIFACT_READ_WINDOW_BYTES}-byte SDK read window"
                )));
            }
            merged.push(CoalescedArtifactRange {
                offset,
                end,
                members: vec![index],
            });
        }
    }
    Ok(merged)
}

struct LoadedRangeRows {
    metadata: PathMetadata,
    rows: Vec<ArtifactManifestRow>,
    /// The complete, verified body of an inline revision, which has no rows.
    inline_body: Option<Vec<u8>>,
}

/// Read `len` bytes at `offset` of a loaded range: an inline body is sliced
/// from its verified bytes, and manifest rows are read from the object store.
fn read_loaded_range(
    store: &dyn ArtifactObjectStore,
    cache: Option<&dyn ArtifactBlockCache>,
    root_id: nokv_protocol::RootIdentity,
    logical_shard_id: LogicalShardIdentity,
    loaded: &LoadedRangeRows,
    offset: u64,
    len: usize,
) -> Result<(Vec<u8>, ArtifactReadStats), ClientError> {
    if let Some(body) = &loaded.inline_body {
        let bytes = usize::try_from(offset)
            .ok()
            .and_then(|start| body.get(start..start.checked_add(len)?))
            .ok_or_else(|| {
                ClientError::ArtifactIntegrity(
                    "inline body does not cover the requested range".to_owned(),
                )
            })?;
        return Ok((bytes.to_vec(), ArtifactReadStats::default()));
    }
    let window = window_from_rows(
        root_id,
        logical_shard_id,
        &loaded.metadata,
        &loaded.rows,
        offset,
        len,
    )?;
    let read = read_artifact_window(store, cache, &window, offset, len)?;
    Ok((read.bytes, read.stats))
}

/// Whether a publication stores its body inline in shard metadata: a
/// visible, non-empty body that would be one object no larger than the
/// inline bound.
fn publishes_inline(authority: &PublicationAuthority, bytes: &[u8], block_size: usize) -> bool {
    matches!(authority, PublicationAuthority::Visible)
        && !bytes.is_empty()
        && bytes.len() <= MAX_INLINE_ARTIFACT_BODY_BYTES.min(block_size)
}

/// An inline revision has no manifest rows, so its manifest digest is the
/// empty closure; its body digest covers the inline bytes.
fn inline_descriptor(
    bytes: &[u8],
    content_type: ContentType,
    producer: Option<String>,
    manifest_identity: Option<String>,
    index_fields: Vec<FieldValue>,
) -> ArtifactDescriptor {
    ArtifactDescriptor {
        logical_size: bytes.len() as u64,
        body_digest: sha256_digest_uri(inline_artifact_body_digest(bytes)),
        manifest_digest: sha256_digest_uri(Digest([0; 32])),
        content_type,
        producer,
        manifest_identity,
        index_fields,
    }
}

fn verify_inline_body(metadata: &PathMetadata, body: &[u8]) -> Result<(), ClientError> {
    if body.len() as u64 != metadata.descriptor.logical_size
        || metadata.descriptor.body_digest != sha256_digest_uri(inline_artifact_body_digest(body))
        || metadata.descriptor.manifest_digest != sha256_digest_uri(Digest([0; 32]))
    {
        return Err(ClientError::ArtifactIntegrity(
            "inline body does not match its sealed descriptor".to_owned(),
        ));
    }
    Ok(())
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
        None if !result.blocks.is_empty()
            || result.next_cursor.is_some()
            || result.inline_body.is_some() =>
        {
            Err(ClientError::ResponseMismatch(
                "metadata-only artifact response included a read plan".to_owned(),
            ))
        }
        Some(_) if result.blocks.is_empty() && result.inline_body.is_none() => Err(
            ClientError::ArtifactIntegrity("range-plan page contains no manifest rows".to_owned()),
        ),
        _ => Ok(()),
    }
}

fn validate_restore_source_run_manifest_result(
    result: &PathReadResult,
    expected_range: Option<ByteRange>,
) -> Result<(), ClientError> {
    if result.not_modified {
        return Err(ClientError::ResponseMismatch(
            "restore source run manifest returned not-modified".to_owned(),
        ));
    }
    if result.range != expected_range {
        return Err(ClientError::ResponseMismatch(
            "restore source run manifest range differs from its request".to_owned(),
        ));
    }
    let metadata = result.metadata.as_ref().ok_or_else(|| {
        ClientError::ResponseMismatch(
            "restore source run manifest omitted path metadata".to_owned(),
        )
    })?;
    if metadata.path.path.as_str() != "metadata/run_manifest.json" {
        return Err(ClientError::ResponseMismatch(
            "restore source read returned a different canonical path".to_owned(),
        ));
    }
    match expected_range {
        None if !result.blocks.is_empty() || result.next_cursor.is_some() => {
            Err(ClientError::ResponseMismatch(
                "restore source metadata response included a read plan".to_owned(),
            ))
        }
        Some(_) if result.blocks.is_empty() => Err(ClientError::ArtifactIntegrity(
            "restore source range-plan page contains no manifest rows".to_owned(),
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

fn require_provider_admission(
    store: &dyn ArtifactObjectStore,
    block_size: usize,
) -> Result<(), ClientError> {
    // Keep zero-size validation owned by the upload planner. Every positive
    // block must fit the endpoint payload actually exercised by admission.
    if block_size == 0 {
        return Ok(());
    }
    let receipt = store
        .provider_admission_receipt()
        .ok_or(ObjectError::ProviderAdmissionRequired)?;
    if !receipt.is_bound_to_store(store) {
        return Err(ObjectError::ProviderAdmissionRequired.into());
    }
    if block_size > receipt.max_verified_object_bytes() {
        return Err(ObjectError::ProviderAdmissionBlockSizeExceeded {
            requested: block_size,
            admitted: receipt.max_verified_object_bytes(),
        }
        .into());
    }
    if !receipt.admits_store(store, block_size) {
        return Err(ObjectError::ProviderAdmissionRequired.into());
    }
    Ok(())
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

/// Which base rows a new append revision borrows.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AppendLayout {
    /// First base row the new revision re-owns; every earlier row is
    /// borrowed. Zero rematerializes the whole body; the row count borrows
    /// every row.
    reown_from_row: usize,
    /// Sorted owners of the borrowed rows.
    owners: Vec<ArtifactRevisionIdentity>,
}

/// Consecutive base rows with one physical owner.
struct OwnerRun {
    owner: ArtifactRevisionIdentity,
    first_row: usize,
    bytes: u64,
}

fn owner_runs(rows: &[ArtifactManifestRow]) -> Vec<OwnerRun> {
    let mut runs: Vec<OwnerRun> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        match runs.last_mut() {
            Some(run) if run.owner == row.physical_owner_revision_id => {
                run.bytes = run.bytes.saturating_add(row.length);
            }
            _ => runs.push(OwnerRun {
                owner: row.physical_owner_revision_id,
                first_row: index,
                bytes: row.length,
            }),
        }
    }
    runs
}

/// Plan an append as a binary counter over the base's owner runs.
///
/// In a body built by appends, the owner of the i-th run was published
/// borrowing the i runs before it, so its dependency depth is i and a
/// revision that borrows the first k runs has depth k. The newest runs merge
/// into the new revision while the next one is no larger than the bytes it
/// would join, or while the depth would exceed its bound, so each byte is
/// rewritten O(log n) times rather than with every rematerialization of the
/// whole body. A base whose runs do not reproduce its sealed depth keeps the
/// conservative rule: borrow every row within the bounds, else rematerialize.
fn append_layout(
    metadata: &PathMetadata,
    artifact_revision_id: ArtifactRevisionIdentity,
    rows: &[ArtifactManifestRow],
    delta_len: u64,
) -> Result<AppendLayout, ClientError> {
    let dependency_plan = append_dependency_plan(metadata, artifact_revision_id, rows)?;
    let runs = owner_runs(rows);
    let distinct_owners = runs
        .iter()
        .map(|run| run.owner)
        .collect::<BTreeSet<_>>()
        .len()
        == runs.len();
    let base_owns_last_run = runs
        .last()
        .is_some_and(|run| run.owner == metadata.artifact_revision_id);
    let inferred_depth = runs.len() - usize::from(base_owns_last_run);
    let append_chain =
        distinct_owners && u8::try_from(inferred_depth).ok() == Some(metadata.dependency_depth);
    let borrowed_runs = if append_chain {
        let mut keep = runs.len();
        let mut pending = delta_len;
        while keep > 0
            && (runs[keep - 1].bytes <= pending
                || keep > usize::from(MAX_ARTIFACT_DEPENDENCY_DEPTH))
        {
            pending = pending.saturating_add(runs[keep - 1].bytes);
            keep -= 1;
        }
        keep
    } else if dependency_plan.requires_rematerialization {
        0
    } else {
        runs.len()
    };
    let reown_from_row = runs
        .get(borrowed_runs)
        .map_or(rows.len(), |run| run.first_row);
    Ok(AppendLayout {
        reown_from_row,
        owners: manifest_dependency_owners(artifact_revision_id, &rows[..reown_from_row]),
    })
}

/// A base body's digest state and the bytes of the rows it hands over.
struct AppendBase {
    digest: BodyDigestState,
    reowned: Vec<u8>,
    read_stats: ArtifactReadStats,
}

/// Continue `digest`, which covers the first `covered_rows` rows, over the
/// rest of the base, and collect the bytes of every row from
/// `reown_from_row` on. The result must reproduce the sealed body digest.
fn read_append_base(
    store: &dyn ArtifactObjectStore,
    manifest: &ArtifactManifest,
    covered_rows: usize,
    mut digest: BodyDigestState,
    reown_from_row: usize,
) -> Result<AppendBase, ClientError> {
    let mut reowned = Vec::new();
    let mut read_stats = ArtifactReadStats::default();
    let first_row = covered_rows.min(reown_from_row);
    for (index, block) in manifest.blocks.iter().enumerate().skip(first_row) {
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
        if index >= covered_rows {
            digest.update(&read.bytes);
        }
        if index >= reown_from_row {
            reowned.try_reserve(read.bytes.len()).map_err(|_| {
                ClientError::InvalidOptions(
                    "append base cannot be rematerialized in client memory".to_owned(),
                )
            })?;
            reowned.extend_from_slice(&read.bytes);
        }
        merge_read_stats(&mut read_stats, read.stats);
    }
    if digest.length() != manifest.logical_len || digest.digest() != manifest.sha256 {
        return Err(ClientError::ArtifactIntegrity(
            "append base body digest does not match its descriptor".to_owned(),
        ));
    }
    Ok(AppendBase {
        digest,
        reowned,
        read_stats,
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
        | ClientError::ArtifactUpload(_)
        | ClientError::AppendUnresolved { .. }
        | ClientError::AppendCleanupUnresolved { .. } => false,
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
    manifest
        .validate()
        .map_err(|error| ClientError::ArtifactIntegrity(error.to_string()))?;
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

fn should_resume_artifact_publication(error: &ClientError) -> bool {
    error.retryable()
        || error.rpc_failure().is_some_and(|failure| {
            failure.code == ErrorCode::Conflict
                && failure.conflict == Some(nokv_protocol::ConflictKind::OperationState)
        })
}

fn publication_failure_without_abort(
    stage: ArtifactPublishStage,
    source: ClientError,
) -> ClientError {
    ClientError::ArtifactPublishFailed {
        stage,
        source: Box::new(source),
        abort_failure: None,
    }
}

fn accumulate_upload_stats(total: &mut ArtifactUploadStats, next: ArtifactUploadStats) {
    total.blocks = total.blocks.saturating_add(next.blocks);
    total.bytes = total.bytes.saturating_add(next.bytes);
    total.created = total.created.saturating_add(next.created);
    total.replayed = total.replayed.saturating_add(next.replayed);
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

/// The durable state of one inline publication, observed after its request
/// failed.
enum InlinePublicationRow {
    /// This publication succeeded; carries its stored receipt.
    Succeeded(ClientCall<PublishResult>),
    /// This publication was admitted and is still running.
    Running(OperationToken),
    /// No operation row exists, so nothing was admitted.
    Absent,
    /// The row is terminal without success, or records other inputs.
    Unrelated,
    /// The row could not be read.
    Unobserved,
}

/// Whether a stored receipt records exactly this inline publication. The
/// engine binds the target, revision, body, and claim into the identity, so a
/// receipt that differs in any of them belongs to another publication.
fn records_inline_publication(
    published: &PublishResult,
    begin: &BeginArtifactPublishRequest,
    descriptor: &ArtifactDescriptor,
) -> bool {
    let generation_follows_claim = match begin.condition {
        PublishCondition::CreateOnly => published.generation == 1,
        PublishCondition::ReplaceOnly {
            expected_generation,
        }
        | PublishCondition::Append {
            expected_generation: Some(expected_generation),
        } => expected_generation.checked_add(1) == Some(published.generation),
        PublishCondition::Append {
            expected_generation: None,
        } => true,
    };
    published.operation_id == begin.operation_id
        && published.target == begin.target
        && published.artifact_revision_id == begin.artifact_revision_id
        && published.logical_size == descriptor.logical_size
        && published.body_digest == descriptor.body_digest
        && generation_follows_claim
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

fn expect_restore_source_run_manifest(
    result: WorkspaceResult,
) -> Result<PathReadResult, ClientError> {
    match result {
        WorkspaceResult::RestoreSourceRunManifest(value) => Ok(value),
        _ => Err(ClientError::ResponseMismatch(
            "expected restore_source_run_manifest result".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use nokv_object::{
        admit_artifact_provider, ArtifactStoreCapabilities, ImmutableCreateOutcome,
        MemoryArtifactStore, ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectRange,
        ProviderAdmissionProfile, ProviderAdmissionReceipt,
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
        uploaded_objects: usize,
        manifest_rows: Vec<ArtifactManifestRow>,
        manifest_dependency_batches: Vec<Vec<ArtifactRevisionIdentity>>,
        operation: Option<OperationStatus>,
        path: Option<PathReadResult>,
        token_sequence: u8,
        /// Sealed dependency depth of every revision the model published.
        revision_depths: BTreeMap<ArtifactRevisionIdentity, u8>,
        commit_version: u64,
        lose_stage_response_once: bool,
        stage_response_lost: bool,
        lose_complete_responses: bool,
        stage_applies: usize,
        abort_applies: usize,
        change_fence_on_second_page_once: bool,
        change_fence_on_every_range: bool,
        advance_workspace_on_every_range: bool,
        fence_changed: bool,
        append_begin_conflicts: usize,
        advance_stage_before_response_once: bool,
        advance_finish_before_response_once: bool,
        finish_requests: Vec<FinishArtifactPublishRequest>,
        lose_inline_responses: bool,
        /// Inline retries of the stored intent to refuse as the engine does
        /// once the owner epoch bound into a visible identity has moved.
        inline_replay_rejections: usize,
    }

    impl ScriptedArtifactTransport {
        fn new(events: Arc<Mutex<Vec<&'static str>>>, lose_stage_response_once: bool) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedArtifactState {
                    replay: BTreeMap::new(),
                    attempts: Vec::new(),
                    begin: None,
                    staged_objects: Vec::new(),
                    uploaded_objects: 0,
                    manifest_rows: Vec::new(),
                    manifest_dependency_batches: Vec::new(),
                    operation: None,
                    path: None,
                    token_sequence: 0,
                    revision_depths: BTreeMap::new(),
                    commit_version: 0,
                    lose_stage_response_once,
                    stage_response_lost: false,
                    lose_complete_responses: false,
                    stage_applies: 0,
                    abort_applies: 0,
                    change_fence_on_second_page_once: false,
                    change_fence_on_every_range: false,
                    advance_workspace_on_every_range: false,
                    fence_changed: false,
                    append_begin_conflicts: 0,
                    advance_stage_before_response_once: false,
                    advance_finish_before_response_once: false,
                    finish_requests: Vec::new(),
                    lose_inline_responses: false,
                    inline_replay_rejections: 0,
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

        fn advance_workspace_on_every_range(&self) {
            self.state.lock().unwrap().advance_workspace_on_every_range = true;
        }

        fn change_fence_on_every_range(&self) {
            self.state.lock().unwrap().change_fence_on_every_range = true;
        }

        fn lose_complete_responses(&self) {
            self.state.lock().unwrap().lose_complete_responses = true;
        }

        fn lose_inline_responses(&self) {
            self.state.lock().unwrap().lose_inline_responses = true;
        }

        fn reject_inline_replays(&self, count: usize) {
            self.state.lock().unwrap().inline_replay_rejections = count;
        }

        fn conflict_append_begins(&self, count: usize) {
            self.state.lock().unwrap().append_begin_conflicts = count;
        }

        fn advance_stage_before_response_once(&self) {
            self.state
                .lock()
                .unwrap()
                .advance_stage_before_response_once = true;
        }

        fn advance_finish_before_response_once(&self) {
            self.state
                .lock()
                .unwrap()
                .advance_finish_before_response_once = true;
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

        /// The typed refusal the engine returns before it writes any row: an
        /// operation identity reused with other immutable inputs, a create-only
        /// claim on a path that already exists, or an unknown operation.
        fn admission_failure(&mut self, operation: &WorkspaceRequest) -> Option<RpcFailure> {
            let failure = |code, message: &str, conflict| RpcFailure {
                code,
                message: message.to_owned(),
                retryable: false,
                conflict,
                current_generation: None,
                route_hint: None,
            };
            let begin = match operation {
                WorkspaceRequest::BeginArtifactPublish(begin) => begin,
                WorkspaceRequest::PrepareArtifactPublish(prepare) => &prepare.begin,
                WorkspaceRequest::PublishInlineArtifact(publish) => &publish.begin,
                WorkspaceRequest::GetOperation(get) => {
                    let known = self
                        .operation
                        .as_ref()
                        .is_some_and(|status| status.token.operation_id == get.operation_id);
                    return (!known)
                        .then(|| failure(ErrorCode::NotFound, "operation does not exist", None));
                }
                _ => return None,
            };
            let mismatch = failure(
                ErrorCode::RequestReplayMismatch,
                "publish operation id was reused with different immutable inputs",
                None,
            );
            if let Some(stored) = self
                .begin
                .as_ref()
                .filter(|stored| stored.operation_id == begin.operation_id)
            {
                if stored != begin {
                    return Some(mismatch);
                }
                if matches!(operation, WorkspaceRequest::PublishInlineArtifact(_))
                    && self.inline_replay_rejections > 0
                {
                    self.inline_replay_rejections -= 1;
                    return Some(mismatch);
                }
                return None;
            }
            if begin.condition == PublishCondition::CreateOnly && self.path.is_some() {
                return Some(failure(
                    ErrorCode::AlreadyExists,
                    "path already exists",
                    Some(ConflictKind::PathGeneration),
                ));
            }
            None
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

        fn publish_preparation(&self) -> Option<Box<nokv_protocol::PublishPreparation>> {
            let begin = self.begin.as_ref().expect("publication has begun");
            Some(Box::new(nokv_protocol::PublishPreparation {
                append_intent_digest: begin.append_intent_digest,
                append_attempt: begin.append_attempt,
                target: begin.target.clone(),
                workspace_incarnation_id: WorkspaceIdentity([9; 16]),
                artifact_revision_id: begin.artifact_revision_id,
            }))
        }

        /// A running status whose progress counts the rows durably recorded so
        /// far, in the engine's staged, uploaded, then manifest order.
        fn running_status(&mut self, operation_id: OperationIdentity) -> OperationStatus {
            let begin = self.begin.as_ref().expect("publication has begun");
            let total_rows = u64::from(begin.staged_object_count)
                .saturating_mul(2)
                .saturating_add(u64::from(begin.manifest_row_count));
            let completed_rows = (self.staged_objects.len()
                + self.uploaded_objects
                + self.manifest_rows.len()) as u64;
            OperationStatus {
                token: self.next_token(operation_id),
                kind: OperationKind::ArtifactPublish,
                append_preparation: None,
                publish_preparation: self.publish_preparation(),
                commit_preparation: None,
                restore_preparation: None,
                state: OperationState::Running,
                progress: OperationProgress {
                    completed_rows,
                    total_rows: Some(total_rows),
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
                if matches!(label, "complete" | "finish") && state.lose_complete_responses {
                    return Err(TransportError::new(
                        "injected complete response loss after durable success",
                        true,
                    ));
                }
                if label == "publish_inline" && state.lose_inline_responses {
                    return Err(TransportError::new(
                        "injected inline response loss after durable success",
                        true,
                    ));
                }
                replayed.replayed = true;
                return encode_response(&replayed)
                    .map_err(|error| TransportError::new(error.to_string(), false));
            }

            if let Some(failure) = state.admission_failure(&request.operation) {
                let response = WorkspaceRpcResponse {
                    route: request.route,
                    request_id: request.request_id,
                    commit_version: None,
                    replayed: false,
                    outcome: WorkspaceRpcOutcome::Failure(failure),
                };
                state.replay.insert(request.request_id, response.clone());
                return encode_response(&response)
                    .map_err(|error| TransportError::new(error.to_string(), false));
            }

            let begin_condition = match &request.operation {
                WorkspaceRequest::BeginArtifactPublish(begin) => Some(begin.condition),
                WorkspaceRequest::PrepareArtifactPublish(prepare) => Some(prepare.begin.condition),
                _ => None,
            };
            let injected_append_conflict =
                matches!(begin_condition, Some(PublishCondition::Append { .. }))
                    && state.append_begin_conflicts > 0;
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

            if let WorkspaceRequest::StageArtifactObjects(stage) = &request.operation {
                if state.advance_stage_before_response_once {
                    state.advance_stage_before_response_once = false;
                    state.stage_applies = state.stage_applies.saturating_add(1);
                    state.staged_objects.extend(stage.objects.clone());
                    let staged_object_count = state
                        .begin
                        .as_ref()
                        .expect("stage follows begin")
                        .staged_object_count;
                    let manifest_row_count = state
                        .begin
                        .as_ref()
                        .expect("stage follows begin")
                        .manifest_row_count;
                    let status = OperationStatus {
                        token: state.next_token(stage.token.operation_id),
                        kind: OperationKind::ArtifactPublish,
                        append_preparation: None,
                        publish_preparation: state.publish_preparation(),
                        commit_preparation: None,
                        restore_preparation: None,
                        state: OperationState::Running,
                        progress: OperationProgress {
                            completed_rows: u64::from(staged_object_count),
                            total_rows: Some(
                                u64::from(staged_object_count)
                                    .saturating_mul(2)
                                    .saturating_add(u64::from(manifest_row_count)),
                            ),
                            completed_bytes: 0,
                            total_bytes: None,
                        },
                        result: None,
                        failure: None,
                    };
                    state.operation = Some(status);
                    let response = WorkspaceRpcResponse {
                        route: request.route,
                        request_id: request.request_id,
                        commit_version: None,
                        replayed: false,
                        outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
                            code: ErrorCode::Conflict,
                            message: "operation token state digest is stale".to_owned(),
                            retryable: false,
                            conflict: Some(ConflictKind::OperationState),
                            current_generation: None,
                            route_hint: None,
                        }),
                    };
                    state.replay.insert(request.request_id, response.clone());
                    return encode_response(&response)
                        .map_err(|error| TransportError::new(error.to_string(), false));
                }
            }

            if let WorkspaceRequest::FinishArtifactPublish(finish) = &request.operation {
                state.finish_requests.push(finish.clone());
                if state.advance_finish_before_response_once {
                    state.advance_finish_before_response_once = false;
                    // The winner's uploads and manifest are durable; this
                    // caller's token no longer names the current state.
                    state.uploaded_objects += finish.uploaded_objects.len();
                    state.manifest_rows.extend(finish.manifest_rows.clone());
                    let status = state.running_status(finish.token.operation_id);
                    state.operation = Some(status);
                    let response = WorkspaceRpcResponse {
                        route: request.route,
                        request_id: request.request_id,
                        commit_version: None,
                        replayed: false,
                        outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
                            code: ErrorCode::Conflict,
                            message: "operation token state digest is stale".to_owned(),
                            retryable: false,
                            conflict: Some(ConflictKind::OperationState),
                            current_generation: None,
                            route_hint: None,
                        }),
                    };
                    state.replay.insert(request.request_id, response.clone());
                    return encode_response(&response)
                        .map_err(|error| TransportError::new(error.to_string(), false));
                }
            }

            self.events.lock().unwrap().push(label);
            state.commit_version = state.commit_version.saturating_add(1);
            let stage_applies = state.stage_applies;
            let result = apply_request(&mut state, &request)?;
            let response = WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version: Some(state.commit_version),
                replayed: false,
                outcome: WorkspaceRpcOutcome::Success(Box::new(result)),
            };
            state.replay.insert(request.request_id, response.clone());
            if state.stage_applies > stage_applies
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
                WorkspaceRequest::PublishInlineArtifact(_)
            ) && state.lose_inline_responses
            {
                return Err(TransportError::new(
                    "injected inline response loss after durable success",
                    true,
                ));
            }
            if matches!(
                request.operation,
                WorkspaceRequest::CompleteArtifactPublish(_)
                    | WorkspaceRequest::FinishArtifactPublish(_)
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
                if state.begin.as_ref() == Some(begin) {
                    if let Some(status) = state.operation.clone() {
                        return Ok(WorkspaceResult::Operation(status));
                    }
                }
                state.begin = Some(begin.clone());
                state.staged_objects.clear();
                state.uploaded_objects = 0;
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
                state.uploaded_objects += mark.objects.len();
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
            WorkspaceRequest::PrepareArtifactPublish(prepare) => {
                let begun = apply_request(
                    state,
                    &publication_step(
                        request,
                        WorkspaceRequest::BeginArtifactPublish(prepare.begin.clone()),
                    ),
                )?;
                let WorkspaceResult::Operation(status) = &begun else {
                    return Ok(begun);
                };
                if prepare.objects.is_empty()
                    || status.state != OperationState::Running
                    || status.progress.completed_rows != 0
                {
                    return Ok(begun);
                }
                apply_request(
                    state,
                    &publication_step(
                        request,
                        WorkspaceRequest::StageArtifactObjects(StageArtifactObjectsRequest {
                            token: status.token,
                            objects: prepare.objects.clone(),
                        }),
                    ),
                )
            }
            WorkspaceRequest::FinishArtifactPublish(finish) => {
                let mut token = finish.token;
                if !finish.uploaded_objects.is_empty() {
                    token = running_token(apply_request(
                        state,
                        &publication_step(
                            request,
                            WorkspaceRequest::MarkArtifactObjectsUploaded(
                                MarkArtifactObjectsUploadedRequest {
                                    token,
                                    objects: finish.uploaded_objects.clone(),
                                },
                            ),
                        ),
                    )?)?;
                }
                if !finish.manifest_rows.is_empty() {
                    token = running_token(apply_request(
                        state,
                        &publication_step(
                            request,
                            WorkspaceRequest::StageArtifactManifest(StageArtifactManifestRequest {
                                token,
                                rows: finish.manifest_rows.clone(),
                                dependency_owner_revision_ids: finish
                                    .dependency_owner_revision_ids
                                    .clone(),
                            }),
                        ),
                    )?)?;
                }
                apply_request(
                    state,
                    &publication_step(
                        request,
                        WorkspaceRequest::CompleteArtifactPublish(CompleteArtifactPublishRequest {
                            token,
                            artifact: finish.artifact.clone(),
                        }),
                    ),
                )
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
                // Like the engine: one more than the deepest direct owner.
                let dependency_depth = match begin
                    .dependency_owner_revision_ids
                    .iter()
                    .map(|owner| state.revision_depths.get(owner).copied())
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| TransportError::new("unknown dependency owner", false))?
                    .into_iter()
                    .max()
                {
                    None => 0,
                    Some(deepest) => deepest
                        .checked_add(1)
                        .filter(|depth| *depth <= MAX_ARTIFACT_DEPENDENCY_DEPTH)
                        .ok_or_else(|| {
                            TransportError::new("dependency depth exceeds bound", false)
                        })?,
                };
                state
                    .revision_depths
                    .insert(begin.artifact_revision_id, dependency_depth);
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
                    append_preparation: None,
                    publish_preparation: state.publish_preparation(),
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
                    inline_body: None,
                });
                Ok(WorkspaceResult::Published(published))
            }
            WorkspaceRequest::PublishInlineArtifact(publish) => {
                let begun = apply_request(
                    state,
                    &publication_step(
                        request,
                        WorkspaceRequest::BeginArtifactPublish(publish.begin.clone()),
                    ),
                )?;
                let WorkspaceResult::Operation(status) = &begun else {
                    return Ok(begun);
                };
                if status.state != OperationState::Running {
                    return Ok(begun);
                }
                let published = apply_request(
                    state,
                    &publication_step(
                        request,
                        WorkspaceRequest::CompleteArtifactPublish(CompleteArtifactPublishRequest {
                            token: status.token,
                            artifact: publish.artifact.clone(),
                        }),
                    ),
                )?;
                if let Some(path) = state.path.as_mut() {
                    path.inline_body = Some(publish.body.clone());
                }
                Ok(published)
            }
            WorkspaceRequest::AbortArtifactPublish(abort) => {
                require_token(state, abort.token)?;
                state.abort_applies = state.abort_applies.saturating_add(1);
                let status = OperationStatus {
                    token: state.next_token(abort.token.operation_id),
                    kind: OperationKind::ArtifactPublish,
                    append_preparation: None,
                    publish_preparation: state.publish_preparation(),
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
                        inline_body: None,
                    }));
                };
                if let Some(body) = stored.inline_body.take() {
                    return Ok(WorkspaceResult::Path(PathReadResult {
                        not_modified: false,
                        metadata: stored.metadata,
                        range: Some(range),
                        blocks: Vec::new(),
                        next_cursor: None,
                        inline_body: Some(body),
                    }));
                }
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
                if state.change_fence_on_every_range {
                    if let Some(metadata) = stored.metadata.as_mut() {
                        metadata.generation = metadata.generation.saturating_add(1);
                    }
                }
                if state.advance_workspace_on_every_range {
                    // Another path of the same workspace was published.
                    if let Some(metadata) =
                        state.path.as_mut().and_then(|path| path.metadata.as_mut())
                    {
                        metadata.workspace_revision = metadata.workspace_revision.saturating_add(1);
                    }
                    if let Some(metadata) = stored.metadata.as_mut() {
                        metadata.workspace_revision = metadata.workspace_revision.saturating_add(1);
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
                    inline_body: None,
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

    /// Requests that admitted a publication, stepwise or fused.
    fn admissions(state: &ScriptedArtifactState) -> usize {
        state
            .attempts
            .iter()
            .filter(|(label, _)| matches!(*label, "begin" | "prepare" | "publish_inline"))
            .count()
    }

    fn publication_step(
        request: &WorkspaceRpcRequest,
        operation: WorkspaceRequest,
    ) -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route: request.route,
            request_id: request.request_id,
            operation,
        }
    }

    fn running_token(result: WorkspaceResult) -> Result<OperationToken, TransportError> {
        match result {
            WorkspaceResult::Operation(status) if status.state == OperationState::Running => {
                Ok(status.token)
            }
            _ => Err(TransportError::new(
                "intermediate publication step did not stay running",
                false,
            )),
        }
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
            WorkspaceRequest::PrepareArtifactPublish(_) => "prepare",
            WorkspaceRequest::FinishArtifactPublish(_) => "finish",
            WorkspaceRequest::PublishInlineArtifact(_) => "publish_inline",
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
        short_reads: AtomicBool,
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
                short_reads: AtomicBool::new(false),
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

        fn provider_handle_identity(&self) -> nokv_object::ProviderHandleIdentity {
            self.inner.provider_handle_identity()
        }

        fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
            self.inner.provider_admission_receipt()
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
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
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
            if self.short_reads.load(Ordering::SeqCst) && !bytes.is_empty() {
                bytes.pop();
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

    struct AdmissionOverrideStore {
        inner: RecordingStore,
        receipt: Option<ProviderAdmissionReceipt>,
    }

    impl AdmissionOverrideStore {
        fn unadmitted(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                inner: RecordingStore::new(events, None),
                receipt: None,
            }
        }

        fn admitted_for(
            events: Arc<Mutex<Vec<&'static str>>>,
            max_verified_object_bytes: usize,
        ) -> Self {
            let inner = RecordingStore::new(events, None);
            let receipt = admit_artifact_provider(
                &inner,
                ProviderAdmissionProfile::single_put(max_verified_object_bytes).unwrap(),
            )
            .unwrap();
            inner.events.lock().unwrap().clear();
            inner.creates.store(0, Ordering::SeqCst);
            inner.deletes.store(0, Ordering::SeqCst);
            inner.reads.store(0, Ordering::SeqCst);
            Self {
                inner,
                receipt: Some(receipt),
            }
        }
    }

    impl ArtifactObjectStore for AdmissionOverrideStore {
        fn object_namespace(&self) -> Option<nokv_types::ObjectNamespaceId> {
            self.inner.object_namespace()
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

        fn provider_handle_identity(&self) -> nokv_object::ProviderHandleIdentity {
            self.inner.provider_handle_identity()
        }

        fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
            self.receipt.as_ref()
        }

        fn create_immutable(
            &self,
            key: &ObjectKey,
            bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.inner.read(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
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
    fn provider_admission_is_required_before_begin_publish_rpc() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = AdmissionOverrideStore::unadmitted(Arc::clone(&events));

        assert!(matches!(
            client
                .publish_artifact(&store, publish_options(4), b"abcdefgh")
                .unwrap_err(),
            ClientError::Object(ObjectError::ProviderAdmissionRequired)
        ));
        assert!(events.lock().unwrap().is_empty());
        assert_eq!(store.inner.creates.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn block_size_above_admission_is_rejected_before_begin_publish_rpc() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = AdmissionOverrideStore::admitted_for(Arc::clone(&events), 4);

        assert!(matches!(
            client
                .publish_artifact(&store, publish_options(5), b"abcdefgh")
                .unwrap_err(),
            ClientError::Object(ObjectError::ProviderAdmissionBlockSizeExceeded {
                requested: 5,
                admitted: 4,
            })
        ));
        assert!(events.lock().unwrap().is_empty());
        assert_eq!(store.inner.creates.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_receipt_from_provider_a_cannot_admit_provider_b() {
        let source_events = Arc::new(Mutex::new(Vec::new()));
        let source = RecordingStore::new(source_events, None);
        let foreign_receipt =
            admit_artifact_provider(&source, ProviderAdmissionProfile::single_put(4).unwrap())
                .unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = AdmissionOverrideStore {
            inner: RecordingStore::new(Arc::clone(&events), None),
            receipt: Some(foreign_receipt),
        };

        assert!(matches!(
            client
                .publish_artifact(&store, publish_options(4), b"abcdefgh")
                .unwrap_err(),
            ClientError::Object(ObjectError::ProviderAdmissionRequired)
        ));
        assert!(events.lock().unwrap().is_empty());
        assert_eq!(store.inner.creates.load(Ordering::SeqCst), 0);
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
            admissions(&state),
            3,
            "base publish plus one rejected and one successful append admission"
        );
        drop(state);

        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, b"abcdefghXYZ");
    }

    #[test]
    fn appends_merge_owner_runs_like_a_binary_counter_and_survive_ancestor_gc() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let mut expected_body = vec![b'a'];
        client
            .publish_artifact(&store, publish_options(1), &expected_body)
            .unwrap();

        // One-byte runs merge like a binary counter. Per append: objects
        // uploaded, dependency owners, and dependency depth afterwards.
        let expected = [
            (2, 0, 0),
            (1, 1, 1),
            (4, 0, 0),
            (1, 1, 1),
            (2, 1, 1),
            (1, 2, 2),
            (8, 0, 0),
        ];
        let mut ancestor_keys = BTreeSet::new();
        for (index, (uploaded, owners, depth)) in expected.into_iter().enumerate() {
            let index = u8::try_from(index).unwrap();
            ancestor_keys.extend(
                inspector
                    .state()
                    .manifest_rows
                    .iter()
                    .map(|row| ObjectKey::new(row.object_identity.as_str()).unwrap()),
            );
            let delta = [b'b' + index];
            expected_body.extend_from_slice(&delta);
            let reowns_everything = owners == 0;
            if reowns_everything {
                // A rejected admission precedes every upload, so re-owning
                // the whole body uploads it once.
                inspector.conflict_append_begins(1);
            }
            let creates_before = store.creates.load(Ordering::SeqCst);
            let outcome = client
                .append_artifact(
                    &store,
                    ArtifactAppendOptions::new(
                        OperationIdentity([0x30 + index; 16]),
                        ArtifactRevisionIdentity([0x50 + index; 16]),
                        target(),
                        ContentType::new("text/plain").unwrap(),
                    )
                    .with_block_size(1),
                    &delta,
                )
                .unwrap();
            assert_eq!(
                store.creates.load(Ordering::SeqCst) - creates_before,
                uploaded,
                "append {index}"
            );
            assert_eq!(
                outcome.descriptor.body_digest,
                sha256_digest_uri(Digest(Sha256::digest(&expected_body).into()))
            );
            let state = inspector.state();
            let metadata = state
                .path
                .as_ref()
                .and_then(|path| path.metadata.as_ref())
                .unwrap();
            assert_eq!(
                (metadata.dependency_count, metadata.dependency_depth),
                (owners, depth),
                "append {index}"
            );
        }

        // The last append re-owned the whole body, so no ancestor object is
        // still referenced.
        let state = inspector.state();
        let final_revision = state.begin.as_ref().unwrap().artifact_revision_id;
        assert_eq!(state.manifest_rows.len(), expected_body.len());
        assert!(state.manifest_rows.iter().all(|row| {
            row.physical_owner_revision_id == final_revision
                && row.append_segment.is_none()
                && row.object_index == row.physical_object_index
        }));
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
        let begins = admissions(&inspector.state());

        let error = client
            .append_artifact(&store, append_options(2).with_max_logical_size(8), b"!")
            .unwrap_err();
        assert!(matches!(error, ClientError::InvalidOptions(_)));
        assert_eq!(store.creates.load(Ordering::SeqCst), creates);
        assert_eq!(admissions(&inspector.state()), begins);
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

    fn append_options_numbered(index: u8, block_size: usize) -> ArtifactAppendOptions {
        ArtifactAppendOptions::new(
            OperationIdentity([0x60 + index; 16]),
            ArtifactRevisionIdentity([0x70 + index; 16]),
            target(),
            ContentType::new("text/plain").unwrap(),
        )
        .with_block_size(block_size)
    }

    #[test]
    fn a_one_block_body_publishes_inline_in_one_request_without_provider_writes() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        // Nothing reaches the provider, so its admission is not required.
        let store = AdmissionOverrideStore::unadmitted(Arc::clone(&events));
        let body = br#"{"turn":3,"role":"tool","text":"ok"}"#;

        let outcome = client
            .publish_artifact(&store, publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE), body)
            .unwrap();
        assert_eq!(outcome.upload_stats, ArtifactUploadStats::default());
        assert_eq!(events.lock().unwrap().as_slice(), ["publish_inline"]);

        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, body);
        assert_eq!(read.stats, ArtifactReadStats::default());
        let range = client
            .read_artifact_range(&store, None, target(), WorkspaceReadView::Live, 2, 5)
            .unwrap();
        assert_eq!(range.bytes, &body[2..7]);
        let batch = client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![ArtifactRangeBatchRequest {
                    target: target(),
                    ranges: vec![
                        ByteRange {
                            offset: 0,
                            length: 1,
                        },
                        ByteRange {
                            offset: 3,
                            length: 4,
                        },
                    ],
                    expected_generation: None,
                    max_gap_bytes: 0,
                }],
                WorkspaceReadView::Live,
            )
            .unwrap();
        assert_eq!(
            batch.items[0].ranges,
            vec![body[0..1].to_vec(), body[3..7].to_vec()]
        );
        assert!(!events.lock().unwrap().contains(&"object_create"));
        assert_eq!(store.inner.reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn an_inline_body_that_misses_its_sealed_digest_fails_integrity() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(
                &store,
                publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"exact",
            )
            .unwrap();
        inspector
            .state()
            .path
            .as_mut()
            .and_then(|path| path.inline_body.as_mut())
            .unwrap()[0] ^= 0xff;
        let error = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap_err();
        assert!(
            matches!(error, ClientError::ArtifactIntegrity(_)),
            "{error:?}"
        );
    }

    fn expect_inline_failure_without_abort(error: &ClientError, code: ErrorCode) {
        match error {
            ClientError::ArtifactPublishFailed {
                stage: ArtifactPublishStage::PublishInline,
                source,
                abort_failure: None,
            } => assert_eq!(source.rpc_code(), Some(code), "{error:?}"),
            other => panic!("expected an inline {code:?} failure without abort: {other:?}"),
        }
    }

    #[test]
    fn an_inline_identity_reused_for_other_inputs_is_refused_without_a_stale_receipt() {
        for (bytes, revision) in [(&b"other"[..], [8; 16]), (&b"first"[..], [0x38; 16])] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
            let inspector = transport.clone();
            let client = client(transport);
            let store = RecordingStore::new(events, None);
            client
                .publish_artifact(
                    &store,
                    publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                    b"first",
                )
                .unwrap();

            let mut options = publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE);
            options.artifact_revision_id = ArtifactRevisionIdentity(revision);
            let error = client.publish_artifact(&store, options, bytes).unwrap_err();

            expect_inline_failure_without_abort(&error, ErrorCode::RequestReplayMismatch);
            let state = inspector.state();
            assert_eq!(state.abort_applies, 0);
            assert_eq!(
                state
                    .path
                    .as_ref()
                    .and_then(|path| path.inline_body.as_deref()),
                Some(&b"first"[..])
            );
        }
    }

    #[test]
    fn an_inline_publication_whose_response_was_lost_returns_its_receipt_as_a_replay() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        inspector.lose_inline_responses();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        let outcome = client
            .publish_artifact(
                &store,
                publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"exact",
            )
            .unwrap();

        assert!(outcome.publication.replayed);
        assert_eq!(
            outcome.publication.value.artifact_revision_id,
            ArtifactRevisionIdentity([8; 16])
        );
        let state = inspector.state();
        // Transport resends repeat one request; the client admits no second one.
        let requests: std::collections::BTreeSet<_> = state
            .attempts
            .iter()
            .filter(|(label, _)| *label == "publish_inline")
            .map(|(_, request_id)| *request_id)
            .collect();
        assert_eq!(requests.len(), 1);
        assert_eq!(state.abort_applies, 0);
    }

    #[test]
    fn an_inline_retry_refused_across_owner_epochs_recovers_its_own_receipt() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let first = client
            .publish_artifact(
                &store,
                publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"exact",
            )
            .unwrap();
        // A visible identity binds its initiating owner epoch, so after an
        // owner restart the engine refuses even an exact retry.
        inspector.reject_inline_replays(1);

        let retried = client
            .publish_artifact(
                &store,
                publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"exact",
            )
            .unwrap();

        assert!(retried.publication.replayed);
        assert_eq!(retried.publication.value, first.publication.value);
        assert_eq!(inspector.state().abort_applies, 0);
    }

    #[test]
    fn an_inline_create_refused_at_admission_does_not_abort_an_absent_operation() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(
                &store,
                publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"first",
            )
            .unwrap();
        let mut options = publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE);
        options.operation_id = OperationIdentity([0x27; 16]);
        options.artifact_revision_id = ArtifactRevisionIdentity([0x28; 16]);

        let error = client
            .publish_artifact(&store, options, b"second")
            .unwrap_err();

        expect_inline_failure_without_abort(&error, ErrorCode::AlreadyExists);
        let state = inspector.state();
        assert_eq!(state.abort_applies, 0);
        assert!(!state.attempts.iter().any(|(label, _)| *label == "abort"));
    }

    #[test]
    fn appends_to_an_inline_base_stay_inline_until_the_bound_then_rematerialize() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(
                &store,
                publish_options(DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"base",
            )
            .unwrap();

        let appended = client
            .append_artifact(
                &store,
                append_options_numbered(1, DEFAULT_ARTIFACT_BLOCK_SIZE),
                b"+delta",
            )
            .unwrap();
        assert_eq!(appended.upload_stats, ArtifactUploadStats::default());
        assert_eq!(appended.descriptor.logical_size, 10);
        assert_eq!(store.creates.load(Ordering::SeqCst), 0);
        assert_eq!(
            inspector
                .state()
                .path
                .as_ref()
                .and_then(|path| path.inline_body.as_deref()),
            Some(&b"base+delta"[..])
        );

        // The next body outgrows the bound: the new revision owns all of it.
        let large = vec![b'x'; MAX_INLINE_ARTIFACT_BODY_BYTES];
        let appended = client
            .append_artifact(
                &store,
                append_options_numbered(2, DEFAULT_ARTIFACT_BLOCK_SIZE),
                &large,
            )
            .unwrap();
        assert_eq!(appended.upload_stats.blocks, 1);
        assert_eq!(store.creates.load(Ordering::SeqCst), 1);
        let state = inspector.state();
        assert!(state
            .begin
            .as_ref()
            .unwrap()
            .dependency_owner_revision_ids
            .is_empty());
        assert_eq!(state.path.as_ref().unwrap().inline_body, None);
        drop(state);
        let mut expected = b"base+delta".to_vec();
        expected.extend_from_slice(&large);
        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, expected);
        assert_eq!(read.metadata.descriptor.logical_size, expected.len() as u64);
    }

    #[test]
    fn one_batch_plan_publishes_with_one_request_before_and_one_after_upload() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(Arc::clone(&events), None);

        client
            .publish_artifact(&store, publish_options(4), b"abcdefghijkl")
            .unwrap();

        let events = events.lock().unwrap();
        assert_eq!(
            events.as_slice(),
            [
                "prepare",
                "object_create",
                "object_create",
                "object_create",
                "finish"
            ],
            "the plan is durable before the first provider write and complete after the last"
        );
    }

    #[test]
    fn finish_after_concurrent_progress_resumes_with_only_the_remaining_steps() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        inspector.advance_finish_before_response_once();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        let outcome = client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        assert_eq!(outcome.publication.value.logical_size, 8);
        assert_eq!(store.creates.load(Ordering::SeqCst), 2);
        let state = inspector.state();
        assert_eq!(state.abort_applies, 0, "a stale token proves progress");
        assert_eq!(admissions(&state), 2);
        assert_eq!(state.stage_applies, 1);
        let [first, resumed] = state.finish_requests.as_slice() else {
            panic!("expected two finish requests: {:?}", state.finish_requests);
        };
        assert_eq!(
            (first.uploaded_objects.len(), first.manifest_rows.len()),
            (2, 2)
        );
        assert!(
            resumed.uploaded_objects.is_empty() && resumed.manifest_rows.is_empty(),
            "the resumed finish carries only the completion"
        );
        assert_eq!(
            state.manifest_rows.len(),
            2,
            "no manifest row is applied twice"
        );
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
        assert!(state.attempts.iter().any(|(label, _)| *label == "finish"));
    }

    #[test]
    fn response_loss_replays_the_exact_stage_request_without_duplicate_apply() {
        // A one-batch plan stages inside Prepare; a larger plan stages in
        // stepwise batches after Begin.
        for (block_size, bytes, staging_label, stage_applies) in [
            (4, b"abcdefgh".to_vec(), "prepare", 1),
            (
                1,
                vec![0x5a; MAX_ARTIFACT_PUBLISH_BATCH_ROWS + 1],
                "stage_objects",
                2,
            ),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let transport = ScriptedArtifactTransport::new(Arc::clone(&events), true);
            let inspector = transport.clone();
            let client = client(transport);
            let store = RecordingStore::new(events, None);

            client
                .publish_artifact(&store, publish_options(block_size), &bytes)
                .unwrap();

            let state = inspector.state();
            let attempts = state
                .attempts
                .iter()
                .filter(|(label, _)| *label == staging_label)
                .map(|(_, request_id)| *request_id)
                .collect::<Vec<_>>();
            assert_eq!(attempts.len(), stage_applies + 1, "{staging_label}");
            assert_eq!(attempts[0], attempts[1], "{staging_label}");
            assert_eq!(state.stage_applies, stage_applies, "{staging_label}");
        }
    }

    #[test]
    fn concurrent_progress_reloads_the_durable_cursor_without_aborting() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        inspector.advance_stage_before_response_once();
        let client = client(transport);
        let store = RecordingStore::new(events, None);

        // Only a multi-batch plan stages outside Prepare's single session.
        let bytes = vec![0x5a; MAX_ARTIFACT_PUBLISH_BATCH_ROWS + 1];
        let outcome = client
            .publish_artifact(&store, publish_options(1), &bytes)
            .unwrap();

        assert_eq!(outcome.publication.value.logical_size, bytes.len() as u64);
        let state = inspector.state();
        assert_eq!(
            state
                .attempts
                .iter()
                .filter(|(label, _)| *label == "begin")
                .count(),
            2,
            "the losing caller must reload the exact durable operation"
        );
        assert_eq!(
            state.stage_applies, 1,
            "the durable cursor must skip the stage completed by the winner"
        );
        assert_eq!(
            state.abort_applies, 0,
            "a stale token proves concurrent progress, not publication failure"
        );
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
            append_preparation: None,
            publish_preparation: Some(Box::new(nokv_protocol::PublishPreparation {
                append_intent_digest: None,
                append_attempt: None,
                target: target(),
                workspace_incarnation_id: WorkspaceIdentity([9; 16]),
                artifact_revision_id: ArtifactRevisionIdentity([8; 16]),
            })),
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
                .filter(|(label, _)| *label == "finish")
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
        assert_eq!(stage, ArtifactPublishStage::Prepare);
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
    fn empty_full_read_classifies_a_noncanonical_descriptor_as_artifact_integrity() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), &[])
            .unwrap();
        inspector
            .state()
            .path
            .as_mut()
            .and_then(|path| path.metadata.as_mut())
            .expect("published path has metadata")
            .descriptor
            .body_digest = sha256_digest_uri(Digest([0x07; 32]));

        let error = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .expect_err("empty descriptor with a non-empty digest must fail closed");

        assert!(matches!(error, ClientError::ArtifactIntegrity(_)));
        assert!(!error.retryable());
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
    fn full_read_survives_publications_to_other_paths_of_the_workspace() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        let bytes = vec![0x5a; MAX_ARTIFACT_READ_PLAN_ROWS + 1];
        client
            .publish_artifact(&store, publish_options(1), &bytes)
            .unwrap();
        inspector.advance_workspace_on_every_range();

        let read = client
            .read_artifact(&store, None, target(), WorkspaceReadView::Live)
            .unwrap();
        assert_eq!(read.bytes, bytes);
        // One metadata read and one plan read per page, without a restart.
        assert_eq!(
            inspector
                .state()
                .attempts
                .iter()
                .filter(|(label, _)| *label == "get_path")
                .count(),
            3
        );
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

    fn batch_request(
        ranges: Vec<ByteRange>,
        expected_generation: Option<u64>,
        max_gap_bytes: u64,
    ) -> ArtifactRangeBatchRequest {
        ArtifactRangeBatchRequest {
            target: target(),
            ranges,
            expected_generation,
            max_gap_bytes,
        }
    }

    #[test]
    fn full_read_authority_rejects_same_generation_aba_before_object_reads() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"same bytes")
            .unwrap();
        let expected = {
            let state = inspector.state();
            let metadata = state
                .path
                .as_ref()
                .and_then(|path| path.metadata.as_ref())
                .expect("published artifact has path metadata");
            ArtifactReadAuthority::from(metadata)
        };
        {
            let mut state = inspector.state();
            let metadata = state
                .path
                .as_mut()
                .and_then(|path| path.metadata.as_mut())
                .expect("published artifact has mutable path metadata");
            metadata.workspace_incarnation_id = WorkspaceIdentity([0xa1; 16]);
            metadata.workspace_revision = metadata.workspace_revision.saturating_add(1);
            metadata.artifact_revision_id = ArtifactRevisionIdentity([0xa2; 16]);
            assert_eq!(metadata.generation, expected.generation);
        }
        let reads_before = store.reads.load(Ordering::SeqCst);

        let error = client
            .read_artifact_at_authority(&store, None, target(), WorkspaceReadView::Live, expected)
            .unwrap_err();

        assert!(matches!(
            error,
            ClientError::RetryExhausted { last_error, .. }
                if matches!(*last_error, ClientError::ArtifactReadFenceChanged)
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), reads_before);
    }

    #[test]
    fn range_batch_preserves_request_range_order_duplicates_and_overlaps() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefghijkl")
            .unwrap();

        let outcome = client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![batch_request(
                    vec![
                        ByteRange {
                            offset: 6,
                            length: 2,
                        },
                        ByteRange {
                            offset: 0,
                            length: 3,
                        },
                        ByteRange {
                            offset: 2,
                            length: 4,
                        },
                        ByteRange {
                            offset: 0,
                            length: 3,
                        },
                    ],
                    Some(1),
                    2,
                )],
                WorkspaceReadView::Live,
            )
            .unwrap();

        assert_eq!(outcome.items.len(), 1);
        assert_eq!(
            outcome.items[0].ranges,
            vec![
                b"gh".to_vec(),
                b"abc".to_vec(),
                b"cdef".to_vec(),
                b"abc".to_vec()
            ]
        );
        assert_eq!(outcome.items[0].metadata.generation, 1);
    }

    #[test]
    fn range_batch_rejects_empty_zero_overflow_and_out_of_bounds_inputs() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();

        for requests in [
            Vec::new(),
            vec![batch_request(Vec::new(), None, 0)],
            vec![batch_request(
                vec![ByteRange {
                    offset: 0,
                    length: 0,
                }],
                None,
                0,
            )],
            vec![batch_request(
                vec![ByteRange {
                    offset: u64::MAX,
                    length: 2,
                }],
                None,
                0,
            )],
            vec![batch_request(
                vec![ByteRange {
                    offset: 7,
                    length: 2,
                }],
                None,
                0,
            )],
        ] {
            assert!(client
                .read_artifact_ranges_batch(&store, None, requests, WorkspaceReadView::Live,)
                .is_err());
        }
    }

    #[test]
    fn range_batch_rejects_generation_drift_across_merged_windows() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefghijkl")
            .unwrap();
        inspector.change_fence_on_every_range();

        let error = client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![batch_request(
                    vec![ByteRange {
                        offset: 0,
                        length: 2,
                    }],
                    Some(1),
                    0,
                )],
                WorkspaceReadView::Live,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::RetryExhausted { last_error, .. }
                if matches!(*last_error, ClientError::ArtifactReadFenceChanged)
        ));
    }

    #[test]
    fn range_batch_short_provider_read_fails_instead_of_truncating_output() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        store.short_reads.store(true, Ordering::SeqCst);

        assert!(client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![batch_request(
                    vec![ByteRange {
                        offset: 0,
                        length: 3,
                    }],
                    None,
                    0,
                )],
                WorkspaceReadView::Live,
            )
            .is_err());
    }

    #[test]
    fn range_batch_retries_the_whole_bounded_attempt_and_exhausts_cleanly() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        store.fail_next_reads(ClientOptions::default().max_attempts as usize);

        let error = client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![batch_request(
                    vec![ByteRange {
                        offset: 0,
                        length: 3,
                    }],
                    None,
                    0,
                )],
                WorkspaceReadView::Live,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::RetryExhausted { attempts, .. }
                if attempts == ClientOptions::default().max_attempts
        ));
    }

    #[test]
    fn range_batch_max_gap_coalesces_only_inside_the_declared_bound() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let inspector = transport.clone();
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefghijkl")
            .unwrap();

        let ranges = vec![
            ByteRange {
                offset: 0,
                length: 2,
            },
            ByteRange {
                offset: 4,
                length: 2,
            },
        ];
        inspector.state().attempts.clear();
        client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![batch_request(ranges.clone(), None, 2)],
                WorkspaceReadView::Live,
            )
            .unwrap();
        assert_eq!(
            inspector
                .state()
                .attempts
                .iter()
                .filter(|(label, _)| *label == "get_path")
                .count(),
            2,
            "one metadata read plus one coalesced range plan"
        );

        inspector.state().attempts.clear();
        client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![batch_request(ranges, None, 1)],
                WorkspaceReadView::Live,
            )
            .unwrap();
        assert_eq!(
            inspector
                .state()
                .attempts
                .iter()
                .filter(|(label, _)| *label == "get_path")
                .count(),
            3,
            "one metadata read plus two unmerged range plans"
        );
    }

    #[test]
    fn duplicate_target_checks_every_expected_generation_before_object_reads() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedArtifactTransport::new(Arc::clone(&events), false);
        let client = client(transport);
        let store = RecordingStore::new(events, None);
        client
            .publish_artifact(&store, publish_options(4), b"abcdefgh")
            .unwrap();
        let reads_before = store.reads.load(Ordering::SeqCst);

        let error = client
            .read_artifact_ranges_batch(
                &store,
                None,
                vec![
                    batch_request(
                        vec![ByteRange {
                            offset: 0,
                            length: 1,
                        }],
                        Some(1),
                        0,
                    ),
                    batch_request(
                        vec![ByteRange {
                            offset: 1,
                            length: 1,
                        }],
                        Some(2),
                        0,
                    ),
                ],
                WorkspaceReadView::Live,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::RetryExhausted {
                attempts,
                last_error,
            } if attempts == ClientOptions::default().max_attempts
                && matches!(*last_error, ClientError::ArtifactReadFenceChanged)
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), reads_before);
    }
}
