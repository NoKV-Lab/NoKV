/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::types::{
    validate_capability_set, validate_field_id, validate_optional_text, ArtifactDescriptor,
    ArtifactManifestRow, ArtifactRevisionIdentity, ByteRange, CommitIdentity, Digest, DigestUri,
    OperationIdentity, OperationToken, PageRequest, PublicationAuthority, PublishCondition,
    RequestIdentity, RootRoute, ScalarValue, SnapshotAlias, SnapshotSelector, StagedObject,
    WorkbenchName, WorkspaceCapability, WorkspaceIdentity, WorkspacePath, WorkspaceReadView,
};
use crate::{
    MAX_ARTIFACT_PUBLISH_BATCH_ROWS, MAX_ARTIFACT_PUBLISH_OBJECTS, WORKSPACE_CAPABILITY_SCHEMA,
    WORKSPACE_PREFLIGHT_SCHEMA, WORKSPACE_PROTOCOL_SCHEMA,
};

const MAX_QUERY_PREDICATES: usize = 64;
const MAX_QUERY_IN_VALUES: usize = 64;
const MAX_QUERY_FIELDS: usize = 64;
const MAX_SORT_FIELDS: usize = 8;
const MAX_FACET_FIELDS: usize = 16;
/// Maximum page size accepted by metadata query, catalog, discovery, and event RPCs.
pub const MAX_QUERY_PAGE_LIMIT: u32 = 256;
pub(crate) const MAX_PARENT_COMMITS: usize = 32;
const MAX_RESTORE_MANIFEST_BYTES: u64 = 1024 * 1024;
const _: () = assert!(MAX_QUERY_PAGE_LIMIT <= PageRequest::MAX_LIMIT);

/// One root-routed metadata or lifecycle request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRpcRequest {
    pub route: RootRoute,
    pub request_id: RequestIdentity,
    pub operation: WorkspaceRequest,
}

impl WorkspaceRpcRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.route.validate()?;
        self.operation.validate()
    }
}

/// Complete Agent workspace RPC surface.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum WorkspaceRequest {
    Preflight(WorkspacePreflightRequest),
    CreateWorkspace(CreateWorkspaceRequest),
    GetWorkspace(GetWorkspaceRequest),
    GetPath(GetPathRequest),
    ListPaths(ListPathsRequest),
    RemovePath(RemovePathRequest),
    BeginArtifactPublish(BeginArtifactPublishRequest),
    StageArtifactObjects(StageArtifactObjectsRequest),
    MarkArtifactObjectsUploaded(MarkArtifactObjectsUploadedRequest),
    StageArtifactManifest(StageArtifactManifestRequest),
    CompleteArtifactPublish(CompleteArtifactPublishRequest),
    AbortArtifactPublish(AbortArtifactPublishRequest),
    ReconcileQuarantinedArtifactPublish(ReconcileQuarantinedArtifactPublishRequest),
    Commit(CommitRequest),
    GetSnapshot(GetSnapshotRequest),
    MintSnapshot(MintSnapshotRequest),
    RenewSnapshot(RenewSnapshotRequest),
    RetireSnapshot(RetireSnapshotRequest),
    ListSnapshots(ListSnapshotsRequest),
    PrepareRestore(PrepareRestoreRequest),
    BindRestoreDestination(BindRestoreDestinationRequest),
    ReadRestoreSourceRunManifest(ReadRestoreSourceRunManifestRequest),
    FinalizeRestore(FinalizeRestoreRequest),
    GetOperation(GetOperationRequest),
    Search(SearchRequest),
    Aggregate(AggregateRequest),
    Catalog(CatalogRequest),
    FindWorkspaces(FindWorkspacesRequest),
    ReadChanges(ChangePageRequest),
}

impl WorkspaceRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Preflight(request) => request.validate(),
            Self::CreateWorkspace(request) => request.validate(),
            Self::GetWorkspace(request) => request.validate(),
            Self::GetPath(request) => request.validate(),
            Self::ListPaths(request) => request.validate(),
            Self::RemovePath(request) => request.validate(),
            Self::BeginArtifactPublish(request) => request.validate(),
            Self::StageArtifactObjects(request) => request.validate(),
            Self::MarkArtifactObjectsUploaded(request) => request.validate(),
            Self::StageArtifactManifest(request) => request.validate(),
            Self::CompleteArtifactPublish(request) => request.validate(),
            Self::AbortArtifactPublish(request) => request.validate(),
            Self::ReconcileQuarantinedArtifactPublish(request) => request.validate(),
            Self::Commit(request) => request.validate(),
            Self::GetSnapshot(request) => request.validate(),
            Self::MintSnapshot(request) => request.validate(),
            Self::RenewSnapshot(request) => request.validate(),
            Self::RetireSnapshot(request) => request.validate(),
            Self::ListSnapshots(request) => request.validate(),
            Self::PrepareRestore(request) => request.validate(),
            Self::BindRestoreDestination(request) => request.validate(),
            Self::ReadRestoreSourceRunManifest(request) => request.validate(),
            Self::FinalizeRestore(request) => request.validate(),
            Self::GetOperation(request) => request.validate(),
            Self::Search(request) => request.validate(),
            Self::Aggregate(request) => request.validate(),
            Self::Catalog(request) => request.validate(),
            Self::FindWorkspaces(request) => request.validate(),
            Self::ReadChanges(request) => request.validate(),
        }
    }
}

/// Fail-closed capability negotiation for one exact root route.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspacePreflightRequest {
    pub preflight_schema: String,
    pub protocol_schema: String,
    pub capability_schema: String,
    pub required_capabilities: Vec<WorkspaceCapability>,
}

impl WorkspacePreflightRequest {
    pub fn new(required_capabilities: impl IntoIterator<Item = WorkspaceCapability>) -> Self {
        let mut required_capabilities = required_capabilities.into_iter().collect::<Vec<_>>();
        required_capabilities.sort_unstable();
        required_capabilities.dedup();
        Self {
            preflight_schema: WORKSPACE_PREFLIGHT_SCHEMA.to_owned(),
            protocol_schema: WORKSPACE_PROTOCOL_SCHEMA.to_owned(),
            capability_schema: WORKSPACE_CAPABILITY_SCHEMA.to_owned(),
            required_capabilities,
        }
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_exact_schema(
            "preflight.preflight_schema",
            &self.preflight_schema,
            WORKSPACE_PREFLIGHT_SCHEMA,
        )?;
        validate_exact_schema(
            "preflight.protocol_schema",
            &self.protocol_schema,
            WORKSPACE_PROTOCOL_SCHEMA,
        )?;
        validate_exact_schema(
            "preflight.capability_schema",
            &self.capability_schema,
            WORKSPACE_CAPABILITY_SCHEMA,
        )?;
        validate_capability_set(
            "preflight.required_capabilities",
            &self.required_capabilities,
        )
    }
}

fn validate_exact_schema(
    field: &'static str,
    actual: &str,
    expected: &'static str,
) -> Result<(), ProtocolError> {
    if actual != expected {
        return Err(ProtocolError::invalid(
            field,
            format!("must equal {expected:?}"),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CreateWorkspaceRequest {
    pub workbench: WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetWorkspaceRequest {
    pub workbench: WorkbenchName,
}

impl GetWorkspaceRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
}

impl CreateWorkspaceRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetPathRequest {
    pub target: WorkspacePath,
    pub view: WorkspaceReadView,
    pub range: Option<ByteRange>,
    pub plan_page: Option<PageRequest>,
    pub if_none_match: Option<u64>,
}

impl GetPathRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.view.validate("get_path.view")?;
        match (self.range, self.plan_page.as_ref()) {
            (None, None) => {}
            (Some(range), Some(page)) => {
                range.validate()?;
                page.validate("get_path.plan_page")?;
                if page.limit as usize > crate::MAX_ARTIFACT_READ_PLAN_ROWS {
                    return Err(ProtocolError::invalid(
                        "get_path.plan_page.limit",
                        format!("exceeds {}", crate::MAX_ARTIFACT_READ_PLAN_ROWS),
                    ));
                }
            }
            (None, Some(_)) => {
                return Err(ProtocolError::invalid(
                    "get_path.plan_page",
                    "metadata-only reads must not include a plan page",
                ));
            }
            (Some(_), None) => {
                return Err(ProtocolError::invalid(
                    "get_path.plan_page",
                    "ranged reads require an explicit plan page",
                ));
            }
        }
        if self.if_none_match == Some(0) {
            return Err(ProtocolError::invalid(
                "get_path.if_none_match",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Stable target-workspace state that authorizes a live page continuation at a
/// newer root read version.
pub struct WorkspaceContinuationFence {
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub workspace_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListPathsRequest {
    pub workbench: WorkbenchName,
    pub prefix: Option<crate::types::RelativePath>,
    pub recursive: bool,
    pub view: WorkspaceReadView,
    pub expected_read_version: Option<u64>,
    pub workspace_continuation_fence: Option<WorkspaceContinuationFence>,
    pub page: PageRequest,
}

impl ListPathsRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.view.validate("list_paths.view")?;
        if self.expected_read_version == Some(0) {
            return Err(ProtocolError::invalid(
                "list_paths.expected_read_version",
                "must be greater than zero",
            ));
        }
        if self.expected_read_version.is_some() && self.workspace_continuation_fence.is_some() {
            return Err(ProtocolError::invalid(
                "list_paths.workspace_continuation_fence",
                "must not be combined with expected_read_version",
            ));
        }
        if matches!(self.view, WorkspaceReadView::Snapshot(_))
            && self.workspace_continuation_fence.is_some()
        {
            return Err(ProtocolError::invalid(
                "list_paths.workspace_continuation_fence",
                "is only valid for live workspace reads",
            ));
        }
        if self.page.cursor.is_some()
            && self.expected_read_version.is_none()
            && self.workspace_continuation_fence.is_none()
        {
            return Err(ProtocolError::invalid(
                "list_paths.expected_read_version",
                "or workspace_continuation_fence is required when continuing from a page cursor",
            ));
        }
        self.page.validate("list_paths.page")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemovePathRequest {
    pub target: WorkspacePath,
    pub expected_generation: u64,
}

impl RemovePathRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.expected_generation == 0 {
            return Err(ProtocolError::invalid(
                "remove_path.expected_generation",
                "must be greater than zero",
            ));
        }
        if matches!(
            self.target.path.as_str(),
            "metadata/run_manifest.json" | "metadata/restore_manifest.json"
        ) {
            return Err(ProtocolError::invalid(
                "remove_path.target.path",
                "canonical Workbench manifests are lifecycle-owned and cannot be removed directly",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BeginArtifactPublishRequest {
    pub operation_id: OperationIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub target: WorkspacePath,
    pub authority: PublicationAuthority,
    pub condition: PublishCondition,
    pub staged_object_count: u32,
    pub staged_object_seal: Digest,
    pub manifest_row_count: u32,
    pub manifest_seal: Digest,
    pub dependency_owner_revision_ids: Vec<ArtifactRevisionIdentity>,
}

impl BeginArtifactPublishRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.condition.validate()?;
        match self.authority {
            PublicationAuthority::Visible => {
                if matches!(
                    self.target.path.as_str(),
                    "metadata/run_manifest.json" | "metadata/restore_manifest.json"
                ) {
                    return Err(ProtocolError::invalid(
                        "begin_artifact_publish.authority",
                        "canonical Workbench manifests require their lifecycle-owned staging authority",
                    ));
                }
            }
            PublicationAuthority::CommitStaging { .. } => {
                if self.target.path.as_str() != "metadata/run_manifest.json"
                    || matches!(self.condition, PublishCondition::Append { .. })
                    || !self.dependency_owner_revision_ids.is_empty()
                {
                    return Err(ProtocolError::invalid(
                        "begin_artifact_publish.authority",
                        "commit staging may only create or replace one dependency-free metadata/run_manifest.json",
                    ));
                }
            }
            PublicationAuthority::RestoreStaging { .. } => {
                if !matches!(
                    self.target.path.as_str(),
                    "metadata/run_manifest.json" | "metadata/restore_manifest.json"
                ) || !matches!(self.condition, PublishCondition::CreateOnly)
                    || !self.dependency_owner_revision_ids.is_empty()
                {
                    return Err(ProtocolError::invalid(
                        "begin_artifact_publish.authority",
                        "restore staging may only create dependency-free metadata/run_manifest.json and metadata/restore_manifest.json",
                    ));
                }
            }
        }
        if self.staged_object_count as usize > MAX_ARTIFACT_PUBLISH_OBJECTS {
            return Err(ProtocolError::invalid(
                "begin_artifact_publish.staged_object_count",
                format!("exceeds {MAX_ARTIFACT_PUBLISH_OBJECTS}"),
            ));
        }
        if self.manifest_row_count as usize > MAX_ARTIFACT_PUBLISH_OBJECTS {
            return Err(ProtocolError::invalid(
                "begin_artifact_publish.manifest_row_count",
                format!("exceeds {MAX_ARTIFACT_PUBLISH_OBJECTS}"),
            ));
        }
        if self.dependency_owner_revision_ids.len()
            > usize::try_from(crate::MAX_ARTIFACT_DEPENDENCY_OWNERS)
                .expect("artifact dependency owner limit fits usize")
        {
            return Err(ProtocolError::invalid(
                "begin_artifact_publish.dependency_owner_revision_ids",
                format!("exceeds {}", crate::MAX_ARTIFACT_DEPENDENCY_OWNERS),
            ));
        }
        if !self
            .dependency_owner_revision_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(ProtocolError::invalid(
                "begin_artifact_publish.dependency_owner_revision_ids",
                "must be sorted and unique",
            ));
        }
        if self
            .dependency_owner_revision_ids
            .binary_search(&self.artifact_revision_id)
            .is_ok()
        {
            return Err(ProtocolError::invalid(
                "begin_artifact_publish.dependency_owner_revision_ids",
                "must not contain the new artifact revision",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StageArtifactObjectsRequest {
    pub token: OperationToken,
    pub objects: Vec<StagedObject>,
}

impl StageArtifactObjectsRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_artifact_batch("stage_artifact_objects.objects", &self.objects)?;
        for object in &self.objects {
            object.validate()?;
        }
        require_strict_sequences(
            "stage_artifact_objects.objects",
            self.objects.iter().map(|object| u64::from(object.sequence)),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectUploadProof {
    pub sequence: u32,
    pub observed_length: u64,
    pub observed_digest: DigestUri,
}

impl ObjectUploadProof {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.observed_length == 0 {
            return Err(ProtocolError::invalid(
                "object_upload_proof.observed_length",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MarkArtifactObjectsUploadedRequest {
    pub token: OperationToken,
    pub objects: Vec<ObjectUploadProof>,
}

impl MarkArtifactObjectsUploadedRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_artifact_batch("mark_artifact_objects_uploaded.objects", &self.objects)?;
        for object in &self.objects {
            object.validate()?;
        }
        require_strict_sequences(
            "mark_artifact_objects_uploaded.objects",
            self.objects.iter().map(|object| u64::from(object.sequence)),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StageArtifactManifestRequest {
    pub token: OperationToken,
    pub rows: Vec<ArtifactManifestRow>,
    pub dependency_owner_revision_ids: Vec<ArtifactRevisionIdentity>,
}

impl StageArtifactManifestRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_artifact_batch("stage_artifact_manifest.rows", &self.rows)?;
        for row in &self.rows {
            row.validate()?;
        }
        let mut previous = None;
        for row in &self.rows {
            if previous.is_some_and(|previous| previous >= row.object_index) {
                return Err(ProtocolError::invalid(
                    "stage_artifact_manifest.rows",
                    "object indexes must be strictly increasing",
                ));
            }
            previous = Some(row.object_index);
        }
        if self.dependency_owner_revision_ids.len()
            > usize::try_from(crate::MAX_ARTIFACT_DEPENDENCY_OWNERS)
                .expect("artifact dependency owner limit fits usize")
        {
            return Err(ProtocolError::invalid(
                "stage_artifact_manifest.dependency_owner_revision_ids",
                format!("exceeds {}", crate::MAX_ARTIFACT_DEPENDENCY_OWNERS),
            ));
        }
        if !self
            .dependency_owner_revision_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(ProtocolError::invalid(
                "stage_artifact_manifest.dependency_owner_revision_ids",
                "must be sorted and unique",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompleteArtifactPublishRequest {
    pub token: OperationToken,
    pub artifact: ArtifactDescriptor,
}

impl CompleteArtifactPublishRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.artifact.validate()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AbortArtifactPublishRequest {
    pub token: OperationToken,
    pub reason: String,
}

impl AbortArtifactPublishRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_required_text("abort_artifact_publish.reason", &self.reason, 1_024)
    }
}

/// Operator verdict about provider-side object state for one quarantined
/// artifact publication. The operator must verify at the provider before
/// calling; the server atomically enforces the machine-checkable half of the
/// verdict and refuses on contradiction.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineResolution {
    /// Every staged provider key was verified absent and the artifact
    /// revision was never published; the revision identity is released for a
    /// fresh publication.
    ProviderObjectsAbsent,
    /// The artifact revision is already published; staged provider keys are
    /// the published revision's live objects and only this operation's
    /// private bookkeeping rows are removed.
    RevisionPublished,
}

/// Operator reconciliation of one quarantined artifact publication.
///
/// The token must digest the exact quarantined operation state the operator
/// inspected, so a concurrent change invalidates the verdict.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReconcileQuarantinedArtifactPublishRequest {
    pub token: OperationToken,
    pub resolution: QuarantineResolution,
    /// Audit reason durably retained in the resolved operation.
    pub reason: String,
    /// Digest of the operator's provider verification transcript, durably
    /// bound into the resolved operation's evidence chain.
    pub evidence_digest: Digest,
}

impl ReconcileQuarantinedArtifactPublishRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_required_text(
            "reconcile_quarantined_artifact_publish.reason",
            &self.reason,
            1_024,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub operation_id: OperationIdentity,
    pub workbench: WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub commit_id: CommitIdentity,
    pub content_digest: DigestUri,
    pub manifest_digest: DigestUri,
    /// Domain-separated digest of every Agent run-manifest projection input
    /// except the first owner-observed durable timestamp. This authenticates
    /// recovery before an immutable manifest revision exists.
    pub projection_input_digest: Digest,
    pub tree_manifest_revision_id: ArtifactRevisionIdentity,
    /// Original Agent authorization for replacing an existing commit head.
    /// This is durable request identity, not a hint that may be recomputed on
    /// retry from the current head.
    pub replace: bool,
    /// Exact frozen claim for publishing `metadata/run_manifest.json`.
    pub run_manifest_condition: PublishCondition,
    pub expected_head_generation: Option<u64>,
    pub parents: Vec<CommitIdentity>,
    pub producer: Option<String>,
    pub lineage_projection: Vec<u8>,
}

impl CommitRequest {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.run_manifest_condition.validate()?;
        if matches!(self.run_manifest_condition, PublishCondition::Append { .. }) {
            return Err(ProtocolError::invalid(
                "commit.run_manifest_condition",
                "commit run-manifest publication does not support append",
            ));
        }
        if self.expected_head_generation == Some(0) {
            return Err(ProtocolError::invalid(
                "commit.expected_head_generation",
                "must be greater than zero",
            ));
        }
        if self.parents.len() > MAX_PARENT_COMMITS {
            return Err(ProtocolError::invalid(
                "commit.parents",
                format!("exceeds {MAX_PARENT_COMMITS}"),
            ));
        }
        if !self.parents.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(ProtocolError::invalid(
                "commit.parents",
                "must be sorted and unique",
            ));
        }
        validate_optional_text("commit.producer", self.producer.as_deref(), 512)?;
        if self.lineage_projection.len() > 64 * 1_024 {
            return Err(ProtocolError::invalid(
                "commit.lineage_projection",
                "exceeds 65536 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetSnapshotRequest {
    pub workbench: WorkbenchName,
    pub selector: SnapshotSelector,
}

impl GetSnapshotRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.selector.validate("get_snapshot.selector")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MintSnapshotRequest {
    pub workbench: WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub snapshot_id: u64,
    pub lease_deadline_ms: u64,
    pub alias: Option<SnapshotAlias>,
    pub annotation: Vec<u8>,
}

impl MintSnapshotRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.snapshot_id == 0 {
            return Err(ProtocolError::invalid(
                "mint_snapshot.snapshot_id",
                "must be greater than zero",
            ));
        }
        if self.lease_deadline_ms == 0 {
            return Err(ProtocolError::invalid(
                "mint_snapshot.lease_deadline_ms",
                "must be greater than zero",
            ));
        }
        if self.annotation.len() > 4_096 {
            return Err(ProtocolError::invalid(
                "mint_snapshot.annotation",
                "exceeds 4096 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RenewSnapshotRequest {
    pub workbench: WorkbenchName,
    pub selector: SnapshotSelector,
    pub lease_deadline_ms: u64,
}

impl RenewSnapshotRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.selector.validate("renew_snapshot.selector")?;
        if self.lease_deadline_ms == 0 {
            return Err(ProtocolError::invalid(
                "renew_snapshot.lease_deadline_ms",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetireSnapshotRequest {
    pub workbench: WorkbenchName,
    pub selector: SnapshotSelector,
    pub retire_annotation: Option<Vec<u8>>,
}

impl RetireSnapshotRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.selector.validate("retire_snapshot.selector")?;
        if self
            .retire_annotation
            .as_ref()
            .is_some_and(|annotation| annotation.len() > 4_096)
        {
            return Err(ProtocolError::invalid(
                "retire_snapshot.retire_annotation",
                "exceeds 4096 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListSnapshotsRequest {
    pub workbench: WorkbenchName,
    pub page: PageRequest,
}

impl ListSnapshotsRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.page.validate("list_snapshots.page")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestoreSource {
    Snapshot(SnapshotSelector),
    Commit(CommitIdentity),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreManifestDescriptor {
    pub body_digest: DigestUri,
    pub logical_size: u64,
    pub content_type: crate::types::ContentType,
}

impl RestoreManifestDescriptor {
    fn validate(&self) -> Result<(), ProtocolError> {
        crate::parse_sha256_digest_uri(&self.body_digest).map_err(|error| {
            ProtocolError::invalid(
                "prepare_restore.restore_manifest.body_digest",
                error.to_string(),
            )
        })?;
        if !(1..=MAX_RESTORE_MANIFEST_BYTES).contains(&self.logical_size) {
            return Err(ProtocolError::invalid(
                "prepare_restore.restore_manifest.logical_size",
                format!("must be between 1 and {MAX_RESTORE_MANIFEST_BYTES}"),
            ));
        }
        if self.content_type.as_str() != "application/json" {
            return Err(ProtocolError::invalid(
                "prepare_restore.restore_manifest.content_type",
                "must equal application/json",
            ));
        }
        Ok(())
    }
}

/// Immutable publication identities reserved for one destination manifest.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreManifestIdentity {
    pub publication_operation_id: OperationIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PrepareRestoreRequest {
    pub operation_id: OperationIdentity,
    pub source_workbench: WorkbenchName,
    pub source_workspace_incarnation_id: WorkspaceIdentity,
    pub source: RestoreSource,
    pub destination_workbench: WorkbenchName,
    pub destination_workspace_incarnation_id: WorkspaceIdentity,
    pub destination_restore_manifest_identity: RestoreManifestIdentity,
    pub restore_manifest: RestoreManifestDescriptor,
}

impl PrepareRestoreRequest {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.source_workbench == self.destination_workbench {
            return Err(ProtocolError::invalid(
                "prepare_restore.destination_workbench",
                "must differ from source workbench",
            ));
        }
        if let RestoreSource::Snapshot(selector) = &self.source {
            selector.validate("prepare_restore.source")?;
            if matches!(selector, SnapshotSelector::Alias(_)) {
                return Err(ProtocolError::invalid(
                    "prepare_restore.source",
                    "must use a concrete snapshot id",
                ));
            }
        }
        if self.operation_id
            == self
                .destination_restore_manifest_identity
                .publication_operation_id
        {
            return Err(ProtocolError::invalid(
                "prepare_restore.destination_restore_manifest_identity",
                "restore and manifest publication operations must be distinct",
            ));
        }
        self.restore_manifest.validate()
    }
}

/// Late-bind the destination commit after the exact materialized source
/// closure has been sealed by the restore operation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindRestoreDestinationRequest {
    pub operation_id: OperationIdentity,
    pub destination_commit_id: CommitIdentity,
    pub effective_content_digest: DigestUri,
    pub destination_run_manifest_projection_input_digest: Digest,
    pub destination_run_manifest_identity: RestoreManifestIdentity,
    pub destination_restore_manifest_identity: RestoreManifestIdentity,
}

impl BindRestoreDestinationRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        crate::parse_sha256_digest_uri(&self.effective_content_digest).map_err(|error| {
            ProtocolError::invalid(
                "bind_restore_destination.effective_content_digest",
                error.to_string(),
            )
        })?;
        if self.destination_run_manifest_projection_input_digest == Digest([0; 32]) {
            return Err(ProtocolError::invalid(
                "bind_restore_destination.destination_run_manifest_projection_input_digest",
                "must not be the zero digest",
            ));
        }
        if self
            .destination_run_manifest_identity
            .publication_operation_id
            == self
                .destination_restore_manifest_identity
                .publication_operation_id
            || self.destination_run_manifest_identity.artifact_revision_id
                == self
                    .destination_restore_manifest_identity
                    .artifact_revision_id
        {
            return Err(ProtocolError::invalid(
                "bind_restore_destination.destination_manifests",
                "run and restore manifests require distinct publication operations and artifact revisions",
            ));
        }
        if self.operation_id
            == self
                .destination_run_manifest_identity
                .publication_operation_id
            || self.operation_id
                == self
                    .destination_restore_manifest_identity
                    .publication_operation_id
        {
            return Err(ProtocolError::invalid(
                "bind_restore_destination.destination_manifests",
                "restore and manifest publication operations must be distinct",
            ));
        }
        Ok(())
    }
}

/// Read the exact source commit-owned run manifest bound to one restore.
///
/// The operation identity provides the durable source binding; callers cannot
/// substitute a path or read view.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadRestoreSourceRunManifestRequest {
    pub operation_id: OperationIdentity,
    pub range: Option<ByteRange>,
    pub plan_page: Option<PageRequest>,
}

impl ReadRestoreSourceRunManifestRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        match (self.range, self.plan_page.as_ref()) {
            (None, None) => Ok(()),
            (Some(range), Some(page)) => {
                range.validate()?;
                page.validate("read_restore_source_run_manifest.plan_page")?;
                if page.limit as usize > crate::MAX_ARTIFACT_READ_PLAN_ROWS {
                    return Err(ProtocolError::invalid(
                        "read_restore_source_run_manifest.plan_page.limit",
                        format!("exceeds {}", crate::MAX_ARTIFACT_READ_PLAN_ROWS),
                    ));
                }
                Ok(())
            }
            (None, Some(_)) => Err(ProtocolError::invalid(
                "read_restore_source_run_manifest.plan_page",
                "metadata-only reads must not include a plan page",
            )),
            (Some(_), None) => Err(ProtocolError::invalid(
                "read_restore_source_run_manifest.plan_page",
                "ranged reads require an explicit plan page",
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FinalizeRestoreRequest {
    pub operation_id: OperationIdentity,
}

impl FinalizeRestoreRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetOperationRequest {
    pub operation_id: OperationIdentity,
}

impl GetOperationRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryScope {
    Root {
        path_prefix: Option<crate::types::RelativePath>,
    },
    Workspace {
        workbench: WorkbenchName,
        path_prefix: Option<crate::types::RelativePath>,
    },
}

impl QueryScope {
    fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryOperator {
    Equal,
    NotEqual,
    In,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Prefix,
    Suffix,
    Contains,
    Exists,
    NotExists,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QueryOperand {
    None,
    Scalar(ScalarValue),
    Set(Vec<ScalarValue>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryPredicate {
    pub field_id: String,
    pub operator: QueryOperator,
    pub operand: QueryOperand,
}

impl QueryPredicate {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_field_id("query_predicate.field_id", &self.field_id)?;
        match (self.operator, &self.operand) {
            (QueryOperator::Exists | QueryOperator::NotExists, QueryOperand::None) => Ok(()),
            (QueryOperator::In, QueryOperand::Set(values)) => {
                if values.len() > MAX_QUERY_IN_VALUES {
                    return Err(ProtocolError::invalid(
                        "query_predicate.operand",
                        format!("set exceeds {MAX_QUERY_IN_VALUES} values"),
                    ));
                }
                for (index, value) in values.iter().enumerate() {
                    value.validate()?;
                    if values[..index].contains(value) {
                        return Err(ProtocolError::invalid(
                            "query_predicate.operand",
                            "set values must be distinct",
                        ));
                    }
                }
                Ok(())
            }
            (
                QueryOperator::Equal
                | QueryOperator::NotEqual
                | QueryOperator::Less
                | QueryOperator::LessOrEqual
                | QueryOperator::Greater
                | QueryOperator::GreaterOrEqual
                | QueryOperator::Prefix
                | QueryOperator::Suffix
                | QueryOperator::Contains,
                QueryOperand::Scalar(value),
            ) => value.validate(),
            (QueryOperator::Exists | QueryOperator::NotExists, _) => Err(ProtocolError::invalid(
                "query_predicate.operand",
                "existence operators require the no-value operand",
            )),
            (QueryOperator::In, _) => Err(ProtocolError::invalid(
                "query_predicate.operand",
                "in requires a set operand",
            )),
            (_, _) => Err(ProtocolError::invalid(
                "query_predicate.operand",
                "comparison operator requires one scalar operand",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SortField {
    pub field_id: String,
    pub direction: SortDirection,
}

impl SortField {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_field_id("sort.field_id", &self.field_id)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub scope: QueryScope,
    pub predicates: Vec<QueryPredicate>,
    pub projection: Vec<String>,
    pub sort: Vec<SortField>,
    pub facets: Vec<String>,
    pub page: PageRequest,
}

impl SearchRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.predicates.len() > MAX_QUERY_PREDICATES {
            return Err(ProtocolError::invalid(
                "search.predicates",
                format!("exceeds {MAX_QUERY_PREDICATES}"),
            ));
        }
        for predicate in &self.predicates {
            predicate.validate()?;
        }
        validate_field_ids("search.projection", &self.projection, MAX_QUERY_FIELDS)?;
        if self.sort.len() > MAX_SORT_FIELDS {
            return Err(ProtocolError::invalid(
                "search.sort",
                format!("exceeds {MAX_SORT_FIELDS}"),
            ));
        }
        for sort in &self.sort {
            sort.validate()?;
        }
        validate_field_ids("search.facets", &self.facets, MAX_FACET_FIELDS)?;
        validate_query_page(&self.page, "search.page")
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunction {
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateSpec {
    pub function: AggregateFunction,
    pub field_id: Option<String>,
    pub result_id: String,
}

impl AggregateSpec {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_field_id("aggregate.result_id", &self.result_id)?;
        match (self.function, self.field_id.as_deref()) {
            (AggregateFunction::Count, None) => Ok(()),
            (AggregateFunction::Count, Some(field)) => {
                validate_field_id("aggregate.field_id", field)
            }
            (_, Some(field)) => validate_field_id("aggregate.field_id", field),
            (_, None) => Err(ProtocolError::invalid(
                "aggregate.field_id",
                "is required for this function",
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateRequest {
    pub scope: QueryScope,
    pub predicates: Vec<QueryPredicate>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateSpec>,
    pub sort: Vec<SortField>,
    pub page: PageRequest,
}

impl AggregateRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.predicates.len() > MAX_QUERY_PREDICATES {
            return Err(ProtocolError::invalid(
                "aggregate.predicates",
                format!("exceeds {MAX_QUERY_PREDICATES}"),
            ));
        }
        for predicate in &self.predicates {
            predicate.validate()?;
        }
        validate_field_ids("aggregate.group_by", &self.group_by, 8)?;
        if self.aggregates.is_empty() || self.aggregates.len() > 16 {
            return Err(ProtocolError::invalid(
                "aggregate.aggregates",
                "must contain between 1 and 16 specifications",
            ));
        }
        for aggregate in &self.aggregates {
            aggregate.validate()?;
        }
        if self.sort.len() > MAX_SORT_FIELDS {
            return Err(ProtocolError::invalid(
                "aggregate.sort",
                format!("exceeds {MAX_SORT_FIELDS}"),
            ));
        }
        for sort in &self.sort {
            sort.validate()?;
        }
        validate_query_page(&self.page, "aggregate.page")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequest {
    pub scope: QueryScope,
    pub field_prefix: Option<String>,
    pub page: PageRequest,
}

impl CatalogRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if let Some(prefix) = self.field_prefix.as_deref() {
            validate_field_id("catalog.field_prefix", prefix)?;
        }
        validate_query_page(&self.page, "catalog.page")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FindWorkspacesRequest {
    pub committed_only: bool,
    pub page: PageRequest,
}

impl FindWorkspacesRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_query_page(&self.page, "find_workspaces.page")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangePageRequest {
    pub after_commit_version: Option<u64>,
    pub workbench: Option<WorkbenchName>,
    pub page: PageRequest,
}

impl ChangePageRequest {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.after_commit_version == Some(0) {
            return Err(ProtocolError::invalid(
                "read_changes.after_commit_version",
                "must be greater than zero",
            ));
        }
        validate_query_page(&self.page, "read_changes.page")
    }
}

fn validate_query_page(page: &PageRequest, field: &'static str) -> Result<(), ProtocolError> {
    if !(1..=MAX_QUERY_PAGE_LIMIT).contains(&page.limit) {
        return Err(ProtocolError::invalid(
            field,
            format!("limit must be between 1 and {MAX_QUERY_PAGE_LIMIT}"),
        ));
    }
    page.validate(field)
}

fn validate_artifact_batch<T>(field: &'static str, rows: &[T]) -> Result<(), ProtocolError> {
    if rows.is_empty() {
        return Err(ProtocolError::invalid(field, "must not be empty"));
    }
    if rows.len() > MAX_ARTIFACT_PUBLISH_BATCH_ROWS {
        return Err(ProtocolError::invalid(
            field,
            format!("exceeds {MAX_ARTIFACT_PUBLISH_BATCH_ROWS} rows"),
        ));
    }
    Ok(())
}

fn require_strict_sequences(
    field: &'static str,
    sequences: impl Iterator<Item = u64>,
) -> Result<(), ProtocolError> {
    let mut previous = None;
    for sequence in sequences {
        if previous.is_some_and(|previous| previous >= sequence) {
            return Err(ProtocolError::invalid(
                field,
                "sequences must be strictly increasing",
            ));
        }
        previous = Some(sequence);
    }
    Ok(())
}

fn validate_required_text(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::invalid(field, "must not be empty"));
    }
    validate_optional_text(field, Some(value), max)
}

fn validate_field_ids(
    field: &'static str,
    values: &[String],
    max: usize,
) -> Result<(), ProtocolError> {
    if values.len() > max {
        return Err(ProtocolError::invalid(
            field,
            format!("exceeds {max} fields"),
        ));
    }
    for value in values {
        validate_field_id(field, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_QUERY_PAGE_LIMIT: u32 = 256;

    fn query_requests(limit: u32) -> Vec<(WorkspaceRequest, &'static str)> {
        let page = || PageRequest {
            cursor: None,
            limit,
        };
        let scope = || QueryScope::Root { path_prefix: None };
        vec![
            (
                WorkspaceRequest::Search(SearchRequest {
                    scope: scope(),
                    predicates: Vec::new(),
                    projection: Vec::new(),
                    sort: Vec::new(),
                    facets: Vec::new(),
                    page: page(),
                }),
                "search.page",
            ),
            (
                WorkspaceRequest::Aggregate(AggregateRequest {
                    scope: scope(),
                    predicates: Vec::new(),
                    group_by: Vec::new(),
                    aggregates: vec![AggregateSpec {
                        function: AggregateFunction::Count,
                        field_id: None,
                        result_id: "count".to_owned(),
                    }],
                    sort: Vec::new(),
                    page: page(),
                }),
                "aggregate.page",
            ),
            (
                WorkspaceRequest::Catalog(CatalogRequest {
                    scope: scope(),
                    field_prefix: None,
                    page: page(),
                }),
                "catalog.page",
            ),
            (
                WorkspaceRequest::FindWorkspaces(FindWorkspacesRequest {
                    committed_only: false,
                    page: page(),
                }),
                "find_workspaces.page",
            ),
            (
                WorkspaceRequest::ReadChanges(ChangePageRequest {
                    after_commit_version: None,
                    workbench: None,
                    page: page(),
                }),
                "read_changes.page",
            ),
        ]
    }

    #[test]
    fn query_requests_reject_pages_above_the_execution_limit() {
        assert_eq!(MAX_QUERY_PAGE_LIMIT, EXPECTED_QUERY_PAGE_LIMIT);
        for (request, _) in query_requests(EXPECTED_QUERY_PAGE_LIMIT) {
            request.validate().unwrap();
        }

        for (request, expected_field) in query_requests(EXPECTED_QUERY_PAGE_LIMIT + 1) {
            let ProtocolError::InvalidField { field, .. } = request.validate().unwrap_err() else {
                panic!("query page limit must fail as invalid input");
            };
            assert_eq!(field, expected_field);
        }
    }

    fn get_path(range: Option<ByteRange>, plan_page: Option<PageRequest>) -> GetPathRequest {
        GetPathRequest {
            target: WorkspacePath {
                workbench: WorkbenchName::new("run-42").unwrap(),
                path: crate::RelativePath::new("outputs/result.bin").unwrap(),
            },
            view: WorkspaceReadView::Live,
            range,
            plan_page,
            if_none_match: None,
        }
    }

    #[test]
    fn list_paths_rejects_a_zero_expected_read_version() {
        let request = ListPathsRequest {
            workbench: WorkbenchName::new("run-42").unwrap(),
            prefix: None,
            recursive: true,
            view: WorkspaceReadView::Live,
            expected_read_version: Some(0),
            workspace_continuation_fence: None,
            page: PageRequest {
                cursor: None,
                limit: 1,
            },
        };

        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "list_paths.expected_read_version",
                ..
            })
        ));
    }

    #[test]
    fn list_paths_requires_a_read_version_fence_for_continuations() {
        let request = ListPathsRequest {
            workbench: WorkbenchName::new("run-42").unwrap(),
            prefix: None,
            recursive: true,
            view: WorkspaceReadView::Live,
            expected_read_version: None,
            workspace_continuation_fence: None,
            page: PageRequest {
                cursor: Some(b"next".to_vec()),
                limit: 1,
            },
        };

        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "list_paths.expected_read_version",
                ..
            })
        ));
    }

    #[test]
    fn list_paths_accepts_a_workspace_fence_for_live_continuations() {
        let request = ListPathsRequest {
            workbench: WorkbenchName::new("run-42").unwrap(),
            prefix: None,
            recursive: true,
            view: WorkspaceReadView::Live,
            expected_read_version: None,
            workspace_continuation_fence: Some(WorkspaceContinuationFence {
                workspace_incarnation_id: WorkspaceIdentity([7; 16]),
                workspace_revision: 3,
            }),
            page: PageRequest {
                cursor: Some(b"next".to_vec()),
                limit: 1,
            },
        };

        request.validate().unwrap();
    }

    #[test]
    fn list_paths_rejects_ambiguous_or_snapshot_workspace_fences() {
        let fence = WorkspaceContinuationFence {
            workspace_incarnation_id: WorkspaceIdentity([7; 16]),
            workspace_revision: 3,
        };
        let mut request = ListPathsRequest {
            workbench: WorkbenchName::new("run-42").unwrap(),
            prefix: None,
            recursive: true,
            view: WorkspaceReadView::Live,
            expected_read_version: Some(9),
            workspace_continuation_fence: Some(fence.clone()),
            page: PageRequest {
                cursor: Some(b"next".to_vec()),
                limit: 1,
            },
        };

        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "list_paths.workspace_continuation_fence",
                ..
            })
        ));
        request.expected_read_version = None;
        request.view = WorkspaceReadView::Snapshot(SnapshotSelector::Id(9));
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "list_paths.workspace_continuation_fence",
                ..
            })
        ));
    }

    fn begin_manifest_publish(
        path: &str,
        authority: PublicationAuthority,
        condition: PublishCondition,
    ) -> BeginArtifactPublishRequest {
        BeginArtifactPublishRequest {
            operation_id: OperationIdentity([1; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([2; 16]),
            target: WorkspacePath {
                workbench: WorkbenchName::new("run-42").unwrap(),
                path: crate::RelativePath::new(path).unwrap(),
            },
            authority,
            condition,
            staged_object_count: 1,
            staged_object_seal: Digest([3; 32]),
            manifest_row_count: 1,
            manifest_seal: Digest([4; 32]),
            dependency_owner_revision_ids: Vec::new(),
        }
    }

    #[test]
    fn get_path_separates_metadata_only_reads_from_bounded_plan_pages() {
        get_path(None, None).validate().unwrap();

        let range = ByteRange {
            offset: 7,
            length: 11,
        };
        let page = PageRequest {
            cursor: None,
            limit: crate::MAX_ARTIFACT_READ_PLAN_ROWS as u32,
        };
        get_path(Some(range), Some(page.clone()))
            .validate()
            .unwrap();

        assert!(matches!(
            get_path(None, Some(page.clone())).validate(),
            Err(ProtocolError::InvalidField {
                field: "get_path.plan_page",
                ..
            })
        ));
        assert!(matches!(
            get_path(Some(range), None).validate(),
            Err(ProtocolError::InvalidField {
                field: "get_path.plan_page",
                ..
            })
        ));

        let oversized_page = PageRequest {
            cursor: None,
            limit: crate::MAX_ARTIFACT_READ_PLAN_ROWS as u32 + 1,
        };
        assert!(matches!(
            get_path(Some(range), Some(oversized_page)).validate(),
            Err(ProtocolError::InvalidField {
                field: "get_path.plan_page.limit",
                ..
            })
        ));
    }

    #[test]
    fn canonical_manifests_require_their_exact_lifecycle_authority() {
        let run_manifest = "metadata/run_manifest.json";
        let restore_manifest = "metadata/restore_manifest.json";
        assert!(matches!(
            begin_manifest_publish(
                run_manifest,
                PublicationAuthority::Visible,
                PublishCondition::CreateOnly,
            )
            .validate(),
            Err(ProtocolError::InvalidField {
                field: "begin_artifact_publish.authority",
                ..
            })
        ));
        assert!(matches!(
            begin_manifest_publish(
                restore_manifest,
                PublicationAuthority::CommitStaging {
                    commit_operation_id: OperationIdentity([5; 16]),
                },
                PublishCondition::CreateOnly,
            )
            .validate(),
            Err(ProtocolError::InvalidField {
                field: "begin_artifact_publish.authority",
                ..
            })
        ));
        assert!(matches!(
            begin_manifest_publish(
                run_manifest,
                PublicationAuthority::CommitStaging {
                    commit_operation_id: OperationIdentity([5; 16]),
                },
                PublishCondition::Append {
                    expected_generation: Some(1),
                },
            )
            .validate(),
            Err(ProtocolError::InvalidField {
                field: "begin_artifact_publish.authority",
                ..
            })
        ));
        begin_manifest_publish(
            run_manifest,
            PublicationAuthority::CommitStaging {
                commit_operation_id: OperationIdentity([5; 16]),
            },
            PublishCondition::CreateOnly,
        )
        .validate()
        .unwrap();
        begin_manifest_publish(
            restore_manifest,
            PublicationAuthority::RestoreStaging {
                restore_operation_id: OperationIdentity([6; 16]),
            },
            PublishCondition::CreateOnly,
        )
        .validate()
        .unwrap();
        begin_manifest_publish(
            run_manifest,
            PublicationAuthority::RestoreStaging {
                restore_operation_id: OperationIdentity([6; 16]),
            },
            PublishCondition::CreateOnly,
        )
        .validate()
        .unwrap();

        assert!(matches!(
            begin_manifest_publish(
                run_manifest,
                PublicationAuthority::RestoreStaging {
                    restore_operation_id: OperationIdentity([6; 16]),
                },
                PublishCondition::Append {
                    expected_generation: Some(1),
                },
            )
            .validate(),
            Err(ProtocolError::InvalidField {
                field: "begin_artifact_publish.authority",
                ..
            })
        ));

        for manifest_path in [run_manifest, restore_manifest] {
            let request = RemovePathRequest {
                target: WorkspacePath {
                    workbench: WorkbenchName::new("run-42").unwrap(),
                    path: crate::RelativePath::new(manifest_path).unwrap(),
                },
                expected_generation: 1,
            };
            assert!(matches!(
                request.validate(),
                Err(ProtocolError::InvalidField {
                    field: "remove_path.target.path",
                    ..
                })
            ));
        }
    }

    #[test]
    fn restore_manifest_descriptor_is_required_and_bounded() {
        let descriptor = RestoreManifestDescriptor {
            body_digest: DigestUri::new(format!("sha256:{}", "ab".repeat(32))).unwrap(),
            logical_size: 128,
            content_type: crate::ContentType::new("application/json").unwrap(),
        };
        descriptor.validate().unwrap();

        let mut wrong_type = descriptor.clone();
        wrong_type.content_type = crate::ContentType::new("text/plain").unwrap();
        assert!(matches!(
            wrong_type.validate(),
            Err(ProtocolError::InvalidField {
                field: "prepare_restore.restore_manifest.content_type",
                ..
            })
        ));

        let mut empty = descriptor;
        empty.logical_size = 0;
        assert!(matches!(
            empty.validate(),
            Err(ProtocolError::InvalidField {
                field: "prepare_restore.restore_manifest.logical_size",
                ..
            })
        ));

        let noncanonical = RestoreManifestDescriptor {
            body_digest: DigestUri::new(format!("sha256:{}", "AB".repeat(32))).unwrap(),
            logical_size: 128,
            content_type: crate::ContentType::new("application/json").unwrap(),
        };
        assert!(matches!(
            noncanonical.validate(),
            Err(ProtocolError::InvalidField {
                field: "prepare_restore.restore_manifest.body_digest",
                ..
            })
        ));
    }

    #[test]
    fn restore_preparation_requires_a_concrete_snapshot_id() {
        let descriptor = RestoreManifestDescriptor {
            body_digest: DigestUri::new(format!("sha256:{}", "ab".repeat(32))).unwrap(),
            logical_size: 128,
            content_type: crate::ContentType::new("application/json").unwrap(),
        };
        let mut request = PrepareRestoreRequest {
            operation_id: OperationIdentity([9; 16]),
            source_workbench: WorkbenchName::new("source").unwrap(),
            source_workspace_incarnation_id: WorkspaceIdentity([1; 16]),
            source: RestoreSource::Snapshot(SnapshotSelector::Id(7)),
            destination_workbench: WorkbenchName::new("destination").unwrap(),
            destination_workspace_incarnation_id: WorkspaceIdentity([2; 16]),
            destination_restore_manifest_identity: RestoreManifestIdentity {
                publication_operation_id: OperationIdentity([7; 16]),
                artifact_revision_id: ArtifactRevisionIdentity([8; 16]),
            },
            restore_manifest: descriptor,
        };
        request.validate().unwrap();

        request.source = RestoreSource::Snapshot(SnapshotSelector::Alias(
            SnapshotAlias::new("latest").unwrap(),
        ));
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "prepare_restore.source",
                ..
            })
        ));
    }

    #[test]
    fn restore_destination_binding_requires_distinct_manifest_publications() {
        let mut request = BindRestoreDestinationRequest {
            operation_id: OperationIdentity([1; 16]),
            destination_commit_id: CommitIdentity([2; 32]),
            effective_content_digest: DigestUri::new(format!("sha256:{}", "03".repeat(32)))
                .unwrap(),
            destination_run_manifest_projection_input_digest: Digest([4; 32]),
            destination_run_manifest_identity: RestoreManifestIdentity {
                publication_operation_id: OperationIdentity([5; 16]),
                artifact_revision_id: ArtifactRevisionIdentity([6; 16]),
            },
            destination_restore_manifest_identity: RestoreManifestIdentity {
                publication_operation_id: OperationIdentity([7; 16]),
                artifact_revision_id: ArtifactRevisionIdentity([8; 16]),
            },
        };
        request.validate().unwrap();

        request
            .destination_restore_manifest_identity
            .publication_operation_id = request
            .destination_run_manifest_identity
            .publication_operation_id;
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "bind_restore_destination.destination_manifests",
                ..
            })
        ));
        request
            .destination_restore_manifest_identity
            .publication_operation_id = OperationIdentity([7; 16]);
        request
            .destination_restore_manifest_identity
            .artifact_revision_id = request
            .destination_run_manifest_identity
            .artifact_revision_id;
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "bind_restore_destination.destination_manifests",
                ..
            })
        ));

        request
            .destination_restore_manifest_identity
            .artifact_revision_id = ArtifactRevisionIdentity([8; 16]);
        request.destination_run_manifest_projection_input_digest = Digest([0; 32]);
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "bind_restore_destination.destination_run_manifest_projection_input_digest",
                ..
            })
        ));
    }

    #[test]
    fn restore_source_run_manifest_read_requires_a_bounded_plan() {
        let operation_id = OperationIdentity([1; 16]);
        ReadRestoreSourceRunManifestRequest {
            operation_id,
            range: None,
            plan_page: None,
        }
        .validate()
        .unwrap();
        let range = ByteRange {
            offset: 5,
            length: 8,
        };
        ReadRestoreSourceRunManifestRequest {
            operation_id,
            range: Some(range),
            plan_page: Some(PageRequest {
                cursor: None,
                limit: crate::MAX_ARTIFACT_READ_PLAN_ROWS as u32,
            }),
        }
        .validate()
        .unwrap();

        assert!(ReadRestoreSourceRunManifestRequest {
            operation_id,
            range: Some(range),
            plan_page: None,
        }
        .validate()
        .is_err());
        assert!(ReadRestoreSourceRunManifestRequest {
            operation_id,
            range: Some(range),
            plan_page: Some(PageRequest {
                cursor: vec![0; PageRequest::MAX_CURSOR_BYTES + 1].into(),
                limit: 1,
            }),
        }
        .validate()
        .is_err());
    }

    #[test]
    fn get_snapshot_accepts_aliases_and_rejects_zero_ids_at_its_exact_field() {
        GetSnapshotRequest {
            workbench: WorkbenchName::new("run-42").unwrap(),
            selector: SnapshotSelector::Alias(SnapshotAlias::new("checkpoint").unwrap()),
        }
        .validate()
        .unwrap();

        assert!(matches!(
            GetSnapshotRequest {
                workbench: WorkbenchName::new("run-42").unwrap(),
                selector: SnapshotSelector::Id(0),
            }
            .validate(),
            Err(ProtocolError::InvalidField {
                field: "get_snapshot.selector",
                ..
            })
        ));
    }
}
