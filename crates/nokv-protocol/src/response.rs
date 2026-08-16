/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use serde::{Deserialize, Serialize};

use crate::error::{ProtocolError, RpcFailure};
use crate::request::{CommitRequest, PrepareRestoreRequest, QueryOperator};
use crate::types::{
    validate_capability_set, validate_field_id, ArtifactDescriptor, ArtifactManifestRow,
    ArtifactRevisionIdentity, ByteRange, CommitIdentity, Digest, DigestUri, FieldValue,
    OperationIdentity, OperationKind, OperationToken, PathMetadata, RequestIdentity, RootRoute,
    ScalarValue, SnapshotAlias, WorkbenchName, WorkspaceCapability, WorkspaceIdentity,
    WorkspacePath,
};
use crate::{WORKSPACE_CAPABILITY_SCHEMA, WORKSPACE_PREFLIGHT_SCHEMA, WORKSPACE_PROTOCOL_SCHEMA};

/// One response to one exact root-routed request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRpcResponse {
    pub route: RootRoute,
    pub request_id: RequestIdentity,
    pub commit_version: Option<u64>,
    pub replayed: bool,
    pub outcome: WorkspaceRpcOutcome,
}

impl WorkspaceRpcResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.route.validate()?;
        if self.commit_version == Some(0) {
            return Err(ProtocolError::invalid(
                "response.commit_version",
                "must be greater than zero",
            ));
        }
        if let WorkspaceRpcOutcome::Success(result) = &self.outcome {
            if let WorkspaceResult::Preflight(preflight) = result.as_ref() {
                if preflight.route != self.route {
                    return Err(ProtocolError::invalid(
                        "preflight.route",
                        "must equal the response envelope route",
                    ));
                }
                if self.commit_version.is_some() || self.replayed {
                    return Err(ProtocolError::invalid(
                        "preflight",
                        "must not report a metadata commit or replay",
                    ));
                }
            }
        }
        self.outcome.validate()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", content = "body", rename_all = "snake_case")]
pub enum WorkspaceRpcOutcome {
    Success(Box<WorkspaceResult>),
    Failure(RpcFailure),
}

impl WorkspaceRpcOutcome {
    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Success(result) => result.validate(),
            Self::Failure(failure) => failure.validate(),
        }
    }
}

/// Result variants correspond directly to [`crate::WorkspaceRequest`] variants.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum WorkspaceResult {
    Preflight(WorkspacePreflightResult),
    Workspace(WorkspaceSummary),
    Path(PathReadResult),
    Paths(PathPage),
    Removed(RemovePathResult),
    Operation(OperationStatus),
    Published(PublishResult),
    Commit(CommitResult),
    Snapshot(SnapshotResult),
    Snapshots(SnapshotPage),
    RestorePrepared(RestorePreparation),
    RestoreSourceRunManifest(PathReadResult),
    Restored(RestoreResult),
    Search(SearchResult),
    Aggregate(AggregateResult),
    Catalog(CatalogResult),
    Workspaces(FindWorkspacesResult),
    Changes(ChangePage),
}

impl WorkspaceResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Preflight(preflight) => preflight.validate(),
            Self::Workspace(workspace) => workspace.validate(),
            Self::Path(path) => path.validate(),
            Self::Paths(paths) => paths.validate(),
            Self::Removed(removed) => removed.validate(),
            Self::Operation(operation) => operation.validate(),
            Self::Published(published) => published.validate(),
            Self::Commit(commit) => commit.validate(),
            Self::Snapshot(snapshot) => snapshot.validate(),
            Self::Snapshots(snapshots) => snapshots.validate(),
            Self::RestorePrepared(prepared) => prepared.validate(),
            Self::RestoreSourceRunManifest(manifest) => {
                manifest.validate_restore_source_run_manifest()
            }
            Self::Restored(restored) => restored.validate(),
            Self::Search(search) => search.validate(),
            Self::Aggregate(aggregate) => aggregate.validate(),
            Self::Catalog(catalog) => catalog.validate(),
            Self::Workspaces(workspaces) => workspaces.validate(),
            Self::Changes(changes) => changes.validate(),
        }
    }
}

/// Exact capability report for the current root owner and route fence.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspacePreflightResult {
    pub preflight_schema: String,
    pub protocol_schema: String,
    pub capability_schema: String,
    pub route: RootRoute,
    pub supported_capabilities: Vec<WorkspaceCapability>,
}

impl WorkspacePreflightResult {
    pub fn new(
        route: RootRoute,
        supported_capabilities: impl IntoIterator<Item = WorkspaceCapability>,
    ) -> Self {
        let mut supported_capabilities = supported_capabilities.into_iter().collect::<Vec<_>>();
        supported_capabilities.sort_unstable();
        supported_capabilities.dedup();
        Self {
            preflight_schema: WORKSPACE_PREFLIGHT_SCHEMA.to_owned(),
            protocol_schema: WORKSPACE_PROTOCOL_SCHEMA.to_owned(),
            capability_schema: WORKSPACE_CAPABILITY_SCHEMA.to_owned(),
            route,
            supported_capabilities,
        }
    }

    pub fn supports(&self, capability: WorkspaceCapability) -> bool {
        self.supported_capabilities
            .binary_search(&capability)
            .is_ok()
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_response_schema(
            "preflight.preflight_schema",
            &self.preflight_schema,
            WORKSPACE_PREFLIGHT_SCHEMA,
        )?;
        validate_response_schema(
            "preflight.protocol_schema",
            &self.protocol_schema,
            WORKSPACE_PROTOCOL_SCHEMA,
        )?;
        validate_response_schema(
            "preflight.capability_schema",
            &self.capability_schema,
            WORKSPACE_CAPABILITY_SCHEMA,
        )?;
        self.route.validate()?;
        validate_capability_set(
            "preflight.supported_capabilities",
            &self.supported_capabilities,
        )
    }
}

fn validate_response_schema(
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
pub struct WorkspaceSummary {
    pub workbench: WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub workspace_revision: u64,
    pub commit_head: Option<CommitIdentity>,
    pub commit_head_generation: Option<u64>,
}

impl WorkspaceSummary {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.commit_head.is_some() != self.commit_head_generation.is_some() {
            return Err(ProtocolError::invalid(
                "workspace.commit_head",
                "head identity and generation must be present together",
            ));
        }
        if self.commit_head_generation == Some(0) {
            return Err(ProtocolError::invalid(
                "workspace.commit_head_generation",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Exact stat plus an optional provider-neutral immutable block plan.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PathReadResult {
    pub not_modified: bool,
    pub metadata: Option<PathMetadata>,
    pub range: Option<ByteRange>,
    pub blocks: Vec<ArtifactManifestRow>,
    pub next_cursor: Option<Vec<u8>>,
}

impl PathReadResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.not_modified {
            if self.metadata.is_some()
                || self.range.is_some()
                || !self.blocks.is_empty()
                || self.next_cursor.is_some()
            {
                return Err(ProtocolError::invalid(
                    "path",
                    "not-modified result must not include metadata or a read plan",
                ));
            }
            return Ok(());
        }
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| ProtocolError::invalid("path.metadata", "is required"))?;
        metadata.validate()?;
        match self.range {
            None => {
                if !self.blocks.is_empty() || self.next_cursor.is_some() {
                    return Err(ProtocolError::invalid(
                        "path",
                        "metadata-only result must not include a read plan",
                    ));
                }
            }
            Some(range) => {
                range.validate()?;
                if self.blocks.is_empty() {
                    return Err(ProtocolError::invalid(
                        "path.blocks",
                        "ranged read-plan page must not be empty",
                    ));
                }
                if self.blocks.len() > crate::MAX_ARTIFACT_READ_PLAN_ROWS {
                    return Err(ProtocolError::invalid(
                        "path.blocks",
                        format!("exceeds {} rows", crate::MAX_ARTIFACT_READ_PLAN_ROWS),
                    ));
                }
                validate_cursor("path.next_cursor", self.next_cursor.as_deref())?;
                let range_end = range.offset.checked_add(range.length).ok_or_else(|| {
                    ProtocolError::invalid("path.range", "offset plus length overflows")
                })?;
                let mut previous = None;
                for row in &self.blocks {
                    row.validate()?;
                    let row_end = row.logical_offset.checked_add(row.length).ok_or_else(|| {
                        ProtocolError::invalid("path.blocks", "logical row range overflows")
                    })?;
                    if row.logical_offset >= range_end || row_end <= range.offset {
                        return Err(ProtocolError::invalid(
                            "path.blocks",
                            "row does not intersect the requested range",
                        ));
                    }
                    if previous.is_some_and(|(object_index, logical_end)| {
                        row.object_index <= object_index || row.logical_offset != logical_end
                    }) {
                        return Err(ProtocolError::invalid(
                            "path.blocks",
                            "rows must be strictly ordered and logically contiguous",
                        ));
                    }
                    previous = Some((row.object_index, row_end));
                }
            }
        }
        Ok(())
    }

    fn validate_restore_source_run_manifest(&self) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.not_modified {
            return Err(ProtocolError::invalid(
                "restore_source_run_manifest",
                "cannot be not-modified without a conditional request",
            ));
        }
        let metadata = self
            .metadata
            .as_ref()
            .expect("ordinary path validation requires metadata");
        if metadata.path.path.as_str() != "metadata/run_manifest.json"
            || metadata.dependency_count != 0
            || metadata.dependency_depth != 0
            || metadata.descriptor.logical_size == 0
            || metadata.descriptor.content_type.as_str() != "application/json"
            || metadata.descriptor.producer.is_some()
            || metadata.descriptor.manifest_identity.is_some()
            || !metadata.descriptor.index_fields.is_empty()
        {
            return Err(ProtocolError::invalid(
                "restore_source_run_manifest.metadata",
                "must be the dependency-free canonical JSON run manifest",
            ));
        }
        crate::parse_sha256_digest_uri(&metadata.descriptor.body_digest).map_err(|error| {
            ProtocolError::invalid(
                "restore_source_run_manifest.metadata.body_digest",
                error.to_string(),
            )
        })?;
        crate::parse_sha256_digest_uri(&metadata.descriptor.manifest_digest).map_err(|error| {
            ProtocolError::invalid(
                "restore_source_run_manifest.metadata.manifest_digest",
                error.to_string(),
            )
        })?;
        Ok(())
    }
}

/// One item in an ordered path listing.
///
/// `Prefix` identifies an implicit direct-child grouping. It carries no
/// durable directory identity or artifact metadata.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
// Artifact rows dominate recursive pages. Keeping them inline avoids one heap
// allocation per hot-path row; direct-prefix pages accept the larger slot.
#[allow(clippy::large_enum_variant)]
pub enum PathListEntry {
    Artifact(PathMetadata),
    Prefix(WorkspacePath),
}

impl PathListEntry {
    pub fn path(&self) -> &WorkspacePath {
        match self {
            Self::Artifact(metadata) => &metadata.path,
            Self::Prefix(path) => path,
        }
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Artifact(metadata) => metadata.validate(),
            Self::Prefix(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PathPage {
    pub entries: Vec<PathListEntry>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: u64,
}

impl PathPage {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.entries.len() > 1_000 {
            return Err(ProtocolError::invalid("paths.entries", "exceeds 1000 rows"));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "paths.read_version",
                "must be greater than zero",
            ));
        }
        if self
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > 4_096)
        {
            return Err(ProtocolError::invalid(
                "paths.next_cursor",
                "exceeds 4096 bytes",
            ));
        }
        for entry in &self.entries {
            entry.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemovePathResult {
    pub removed: bool,
    pub workspace_revision: u64,
    pub removed_artifact_revision_id: Option<ArtifactRevisionIdentity>,
}

impl RemovePathResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.removed != self.removed_artifact_revision_id.is_some() {
            return Err(ProtocolError::invalid(
                "removed.removed_artifact_revision_id",
                "must be present exactly when removed is true",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishResult {
    pub operation_id: OperationIdentity,
    pub target: WorkspacePath,
    pub workspace_revision: u64,
    pub generation: u64,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub logical_size: u64,
    pub body_digest: DigestUri,
}

impl PublishResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.generation == 0 {
            return Err(ProtocolError::invalid(
                "published.generation",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommitResult {
    pub operation_id: OperationIdentity,
    pub commit_id: CommitIdentity,
    pub workbench: WorkbenchName,
    pub head_generation: u64,
    pub member_count: u64,
    pub member_digest: Digest,
}

/// Immutable commit-owned run-manifest binding. This describes the revision,
/// not whichever artifact currently occupies the Workbench path.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommitManifestBinding {
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub descriptor: ArtifactDescriptor,
}

impl CommitManifestBinding {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.descriptor.validate()?;
        if self.descriptor.content_type.as_str() != "application/json"
            || self.descriptor.producer.is_some()
            || self.descriptor.manifest_identity.is_some()
            || !self.descriptor.index_fields.is_empty()
        {
            return Err(ProtocolError::invalid(
                "operation.commit_preparation.manifest",
                "commit-owned run manifest must be dependency-free canonical JSON metadata",
            ));
        }
        Ok(())
    }
}

/// Durable inputs needed to reconstruct and authenticate the exact Workbench
/// commit after process loss. Clients must resubmit `request` unchanged and
/// must rebuild the canonical envelope only with the frozen timestamp.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommitPreparation {
    pub request: Box<CommitRequest>,
    pub committed_at_unix_seconds: u64,
    pub manifest: Option<CommitManifestBinding>,
}

impl CommitPreparation {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.request.validate()?;
        if self.committed_at_unix_seconds == 0 {
            return Err(ProtocolError::invalid(
                "operation.commit_preparation.committed_at_unix_seconds",
                "must be greater than zero",
            ));
        }
        if let Some(manifest) = &self.manifest {
            manifest.validate()?;
            if manifest.workspace_incarnation_id != self.request.workspace_incarnation_id
                || manifest.artifact_revision_id != self.request.tree_manifest_revision_id
            {
                return Err(ProtocolError::invalid(
                    "operation.commit_preparation.manifest",
                    "does not match the durable commit request",
                ));
            }
        }
        Ok(())
    }
}

impl CommitResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.head_generation == 0 {
            return Err(ProtocolError::invalid(
                "commit.head_generation",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    Alive,
    Expired,
    ReapClaimed,
    Retired,
    Reaped,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotResult {
    pub snapshot_id: u64,
    pub workbench: WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub read_version: u64,
    pub lease_deadline_ms: u64,
    pub alias: Option<SnapshotAlias>,
    pub annotation: Vec<u8>,
    pub retire_annotation: Option<Vec<u8>>,
    pub status: SnapshotStatus,
    pub consumer_count: u64,
}

impl SnapshotResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.snapshot_id == 0 {
            return Err(ProtocolError::invalid(
                "snapshot.snapshot_id",
                "must be greater than zero",
            ));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "snapshot.read_version",
                "must be greater than zero",
            ));
        }
        if self.lease_deadline_ms == 0 {
            return Err(ProtocolError::invalid(
                "snapshot.lease_deadline_ms",
                "must be greater than zero",
            ));
        }
        if self.annotation.len() > 4_096 {
            return Err(ProtocolError::invalid(
                "snapshot.annotation",
                "exceeds 4096 bytes",
            ));
        }
        if self
            .retire_annotation
            .as_ref()
            .is_some_and(|annotation| annotation.len() > 4_096)
        {
            return Err(ProtocolError::invalid(
                "snapshot.retire_annotation",
                "exceeds 4096 bytes",
            ));
        }
        if self.retire_annotation.is_some() && self.status != SnapshotStatus::Retired {
            return Err(ProtocolError::invalid(
                "snapshot.retire_annotation",
                "is only valid for retired snapshots",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPage {
    pub snapshots: Vec<SnapshotResult>,
    pub next_cursor: Option<Vec<u8>>,
}

impl SnapshotPage {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.snapshots.len() > 1_000 {
            return Err(ProtocolError::invalid("snapshots", "exceeds 1000 rows"));
        }
        if self
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > 4_096)
        {
            return Err(ProtocolError::invalid(
                "snapshots.next_cursor",
                "exceeds 4096 bytes",
            ));
        }
        for snapshot in &self.snapshots {
            snapshot.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreResult {
    pub operation_id: OperationIdentity,
    pub destination: WorkspaceSummary,
    pub member_count: u64,
    pub member_digest: Digest,
    pub metadata_rows_copied: u64,
    pub object_bytes_copied: u64,
}

/// Exact immutable source commit resolved for one restore operation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreSourceCommitBinding {
    pub commit_id: CommitIdentity,
    pub content_digest: DigestUri,
    pub manifest_digest: DigestUri,
    pub tree_manifest_revision_id: ArtifactRevisionIdentity,
    pub member_count: u64,
    pub member_digest: Digest,
}

impl RestoreSourceCommitBinding {
    fn validate(&self) -> Result<(), ProtocolError> {
        crate::parse_sha256_digest_uri(&self.content_digest).map_err(|error| {
            ProtocolError::invalid("restore.source_commit.content_digest", error.to_string())
        })?;
        crate::parse_sha256_digest_uri(&self.manifest_digest).map_err(|error| {
            ProtocolError::invalid("restore.source_commit.manifest_digest", error.to_string())
        })?;
        validate_restore_member_seal(
            "restore.source_commit.members",
            self.member_count,
            self.member_digest,
        )
    }
}

/// Immutable destination manifest publication bound to one restore.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreManifestBinding {
    pub publication_operation_id: OperationIdentity,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub descriptor: ArtifactDescriptor,
}

impl RestoreManifestBinding {
    fn validate(&self, field: &'static str) -> Result<(), ProtocolError> {
        self.descriptor.validate()?;
        if self.descriptor.logical_size == 0 {
            return Err(ProtocolError::invalid(
                field,
                "canonical JSON manifest must not be empty",
            ));
        }
        crate::parse_sha256_digest_uri(&self.descriptor.body_digest)
            .map_err(|error| ProtocolError::invalid(field, error.to_string()))?;
        crate::parse_sha256_digest_uri(&self.descriptor.manifest_digest)
            .map_err(|error| ProtocolError::invalid(field, error.to_string()))?;
        if self.descriptor.content_type.as_str() != "application/json"
            || self.descriptor.producer.is_some()
            || self.descriptor.manifest_identity.is_some()
            || !self.descriptor.index_fields.is_empty()
        {
            return Err(ProtocolError::invalid(
                field,
                "restore-owned manifest must be dependency-free canonical JSON metadata",
            ));
        }
        Ok(())
    }
}

/// Both destination-owned canonical manifests. They become durable together;
/// neither binding is independently optional.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreDestinationManifestBindings {
    pub run_manifest: RestoreManifestBinding,
    pub restore_manifest: RestoreManifestBinding,
}

impl RestoreDestinationManifestBindings {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.run_manifest
            .validate("operation.restore_preparation.destination_manifests.run_manifest")?;
        self.restore_manifest
            .validate("operation.restore_preparation.destination_manifests.restore_manifest")?;
        Ok(())
    }
}

/// Durable late-bound destination commit intent. Expected publication
/// identities are frozen before object-first publication; final bindings only
/// appear after both manifests have been accepted.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreDestinationBinding {
    pub destination_commit_id: CommitIdentity,
    pub effective_content_digest: DigestUri,
    pub destination_run_manifest_projection_input_digest: Digest,
    pub destination_run_manifest_identity: crate::request::RestoreManifestIdentity,
    pub destination_restore_manifest_identity: crate::request::RestoreManifestIdentity,
    pub destination_manifests: Option<RestoreDestinationManifestBindings>,
}

impl RestoreDestinationBinding {
    fn validate(
        &self,
        request: &PrepareRestoreRequest,
        source_commit: &RestoreSourceCommitBinding,
        source_matches_base_commit: bool,
    ) -> Result<(), ProtocolError> {
        self.validate_common(
            request.operation_id,
            request.destination_workspace_incarnation_id,
            source_commit,
            source_matches_base_commit,
        )?;
        if self.destination_restore_manifest_identity
            != request.destination_restore_manifest_identity
        {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_binding.destination_restore_manifest_identity",
                "does not match the durable prepare request",
            ));
        }
        if let Some(manifests) = &self.destination_manifests {
            if manifests.restore_manifest.descriptor.body_digest
                != request.restore_manifest.body_digest
                || manifests.restore_manifest.descriptor.logical_size
                    != request.restore_manifest.logical_size
                || manifests.restore_manifest.descriptor.content_type
                    != request.restore_manifest.content_type
            {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation.destination_binding.destination_manifests",
                    "restore manifest does not exactly match the durable prepare descriptor",
                ));
            }
        }
        Ok(())
    }

    fn validate_common(
        &self,
        restore_operation_id: OperationIdentity,
        destination_workspace_incarnation_id: WorkspaceIdentity,
        source_commit: &RestoreSourceCommitBinding,
        source_matches_base_commit: bool,
    ) -> Result<(), ProtocolError> {
        crate::parse_sha256_digest_uri(&self.effective_content_digest).map_err(|error| {
            ProtocolError::invalid(
                "operation.restore_preparation.destination_binding.effective_content_digest",
                error.to_string(),
            )
        })?;
        if self.destination_commit_id == source_commit.commit_id {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_binding.destination_commit_id",
                "must differ from the source commit",
            ));
        }
        if source_matches_base_commit
            != (self.effective_content_digest == source_commit.content_digest)
        {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_binding.effective_content_digest",
                "must preserve the base content digest exactly for a clean source and use a distinct materialized digest otherwise",
            ));
        }
        if self.destination_run_manifest_projection_input_digest == Digest([0; 32]) {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_binding.destination_run_manifest_projection_input_digest",
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
            || restore_operation_id
                == self
                    .destination_run_manifest_identity
                    .publication_operation_id
            || restore_operation_id
                == self
                    .destination_restore_manifest_identity
                    .publication_operation_id
        {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_binding.destination_manifests",
                "restore, run-manifest, and restore-manifest operations and revisions must remain distinct",
            ));
        }
        if let Some(manifests) = &self.destination_manifests {
            manifests.validate()?;
            let run_identity = self.destination_run_manifest_identity;
            let restore_identity = self.destination_restore_manifest_identity;
            if manifests.run_manifest.publication_operation_id
                != run_identity.publication_operation_id
                || manifests.run_manifest.artifact_revision_id != run_identity.artifact_revision_id
                || manifests.restore_manifest.publication_operation_id
                    != restore_identity.publication_operation_id
                || manifests.restore_manifest.artifact_revision_id
                    != restore_identity.artifact_revision_id
                || manifests.run_manifest.workspace_incarnation_id
                    != destination_workspace_incarnation_id
                || manifests.restore_manifest.workspace_incarnation_id
                    != destination_workspace_incarnation_id
            {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation.destination_binding.destination_manifests",
                    "do not exactly match the durable identities and destination incarnation",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestorePreparation {
    pub operation_id: OperationIdentity,
    pub destination_workbench: WorkbenchName,
    pub destination_workspace_incarnation_id: WorkspaceIdentity,
    pub source_commit: RestoreSourceCommitBinding,
    pub destination_committed_at_unix_seconds: u64,
    pub source_member_count: u64,
    pub source_member_digest: Digest,
    pub materialized_member_count: u64,
    pub materialized_member_digest: Digest,
    pub source_matches_base_commit: bool,
    pub destination_binding: Option<Box<RestoreDestinationBinding>>,
}

/// Durable restore inputs returned by operation lookup. A client may use this
/// projection to reconstruct the exact prepare DTO after losing local state,
/// but must still submit that complete DTO to authenticate a replay.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreOperationPreparation {
    pub request: PrepareRestoreRequest,
    pub source_snapshot_read_version: Option<u64>,
    pub source_commit: RestoreSourceCommitBinding,
    pub destination_committed_at_unix_seconds: u64,
    pub source_member_count: Option<u64>,
    pub source_member_digest: Option<Digest>,
    pub materialized_member_count: Option<u64>,
    pub materialized_member_digest: Option<Digest>,
    pub source_matches_base_commit: Option<bool>,
    pub destination_binding: Option<Box<RestoreDestinationBinding>>,
}

impl RestoreOperationPreparation {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.request.validate()?;
        self.source_commit.validate()?;
        match (&self.request.source, self.source_snapshot_read_version) {
            (crate::request::RestoreSource::Snapshot(_), Some(version)) if version != 0 => {}
            (crate::request::RestoreSource::Commit(_), None) => {}
            _ => {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation.source_snapshot_read_version",
                    "must be positive exactly for snapshot restores",
                ));
            }
        }
        if let crate::request::RestoreSource::Commit(commit_id) = &self.request.source {
            if *commit_id != self.source_commit.commit_id {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation.source_commit",
                    "does not match the requested source commit",
                ));
            }
        }
        if self.destination_committed_at_unix_seconds == 0 {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_committed_at_unix_seconds",
                "must be greater than zero",
            ));
        }
        let seal_fields_present = [
            self.source_member_count.is_some(),
            self.source_member_digest.is_some(),
            self.materialized_member_count.is_some(),
            self.materialized_member_digest.is_some(),
            self.source_matches_base_commit.is_some(),
        ];
        if seal_fields_present.iter().any(|present| *present)
            && seal_fields_present.iter().any(|present| !*present)
        {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation",
                "raw and materialized source seals and their base-commit comparison must be present together",
            ));
        }
        if let (
            Some(source_member_count),
            Some(source_member_digest),
            Some(materialized_member_count),
            Some(materialized_member_digest),
            Some(source_matches_base_commit),
        ) = (
            self.source_member_count,
            self.source_member_digest,
            self.materialized_member_count,
            self.materialized_member_digest,
            self.source_matches_base_commit,
        ) {
            validate_restore_member_seal(
                "operation.restore_preparation.source_members",
                source_member_count,
                source_member_digest,
            )?;
            validate_restore_member_seal(
                "operation.restore_preparation.materialized_members",
                materialized_member_count,
                materialized_member_digest,
            )?;
            validate_materialized_member_subset(
                "operation.restore_preparation.materialized_members",
                source_member_count,
                materialized_member_count,
            )?;
            validate_source_commit_match(
                &self.source_commit,
                source_member_count,
                source_member_digest,
                source_matches_base_commit,
            )?;
            if matches!(
                self.request.source,
                crate::request::RestoreSource::Commit(_)
            ) && !source_matches_base_commit
            {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation.source_matches_base_commit",
                    "must be true for a direct commit source",
                ));
            }
            if let Some(binding) = &self.destination_binding {
                binding.validate(
                    &self.request,
                    &self.source_commit,
                    source_matches_base_commit,
                )?;
            }
        } else if self.destination_binding.is_some() {
            return Err(ProtocolError::invalid(
                "operation.restore_preparation.destination_binding",
                "cannot precede the sealed raw and materialized source closures",
            ));
        }
        Ok(())
    }
}

impl RestorePreparation {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.source_commit.validate()?;
        if self.destination_committed_at_unix_seconds == 0 {
            return Err(ProtocolError::invalid(
                "restore_prepared.destination_committed_at_unix_seconds",
                "must be greater than zero",
            ));
        }
        validate_restore_member_seal(
            "restore_prepared.source_members",
            self.source_member_count,
            self.source_member_digest,
        )?;
        validate_restore_member_seal(
            "restore_prepared.materialized_members",
            self.materialized_member_count,
            self.materialized_member_digest,
        )?;
        validate_materialized_member_subset(
            "restore_prepared.materialized_members",
            self.source_member_count,
            self.materialized_member_count,
        )?;
        validate_source_commit_match(
            &self.source_commit,
            self.source_member_count,
            self.source_member_digest,
            self.source_matches_base_commit,
        )?;
        if let Some(binding) = &self.destination_binding {
            // The compact prepare response does not repeat the prepare
            // descriptor; operation lookup performs that final exact check.
            binding.validate_common(
                self.operation_id,
                self.destination_workspace_incarnation_id,
                &self.source_commit,
                self.source_matches_base_commit,
            )?;
        }
        Ok(())
    }
}

fn validate_restore_member_seal(
    field: &'static str,
    member_count: u64,
    member_digest: Digest,
) -> Result<(), ProtocolError> {
    if (member_count == 0) != (member_digest == Digest([0; 32])) {
        return Err(ProtocolError::invalid(
            field,
            "zero member count and zero digest must be present together",
        ));
    }
    Ok(())
}

fn validate_materialized_member_subset(
    field: &'static str,
    source_member_count: u64,
    materialized_member_count: u64,
) -> Result<(), ProtocolError> {
    let skipped_provenance_members = source_member_count
        .checked_sub(materialized_member_count)
        .ok_or_else(|| {
            ProtocolError::invalid(field, "cannot contain rows absent from the raw source")
        })?;
    if !(1..=2).contains(&skipped_provenance_members) {
        return Err(ProtocolError::invalid(
            field,
            "must omit the source run manifest and at most one restore manifest",
        ));
    }
    Ok(())
}

fn validate_source_commit_match(
    source_commit: &RestoreSourceCommitBinding,
    source_member_count: u64,
    source_member_digest: Digest,
    source_matches_base_commit: bool,
) -> Result<(), ProtocolError> {
    let exact_match = source_member_count == source_commit.member_count
        && source_member_digest == source_commit.member_digest;
    if source_matches_base_commit != exact_match {
        return Err(ProtocolError::invalid(
            "restore.source_matches_base_commit",
            "must equal the exact raw-source/base-commit member seal comparison",
        ));
    }
    Ok(())
}

impl RestoreResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.destination.validate()?;
        if self.destination.commit_head.is_none()
            || self.destination.commit_head_generation.is_none()
        {
            return Err(ProtocolError::invalid(
                "restore.destination.commit_head",
                "a successful restore must return its committed destination head",
            ));
        }
        validate_restore_member_seal("restore.members", self.member_count, self.member_digest)?;
        if self.object_bytes_copied != 0 {
            return Err(ProtocolError::invalid(
                "restore.object_bytes_copied",
                "same-shard restore is zero-copy",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Running,
    Succeeded,
    Aborting,
    Failed,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationProgress {
    pub completed_rows: u64,
    pub total_rows: Option<u64>,
    pub completed_bytes: u64,
    pub total_bytes: Option<u64>,
}

impl OperationProgress {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self
            .total_rows
            .is_some_and(|total| self.completed_rows > total)
        {
            return Err(ProtocolError::invalid(
                "operation.progress.completed_rows",
                "exceeds total rows",
            ));
        }
        if self
            .total_bytes
            .is_some_and(|total| self.completed_bytes > total)
        {
            return Err(ProtocolError::invalid(
                "operation.progress.completed_bytes",
                "exceeds total bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum OperationResult {
    ArtifactPublish(PublishResult),
    Commit(CommitResult),
    Restore(RestoreResult),
}

impl OperationResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::ArtifactPublish(result) => result.validate(),
            Self::Commit(result) => result.validate(),
            Self::Restore(result) => result.validate(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationStatus {
    pub token: OperationToken,
    pub kind: OperationKind,
    /// Present exactly for commit operations, including terminal ones.
    pub commit_preparation: Option<Box<CommitPreparation>>,
    /// Present exactly for restore operations, including terminal ones.
    pub restore_preparation: Option<Box<RestoreOperationPreparation>>,
    pub state: OperationState,
    pub progress: OperationProgress,
    pub result: Option<OperationResult>,
    pub failure: Option<RpcFailure>,
}

impl OperationStatus {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.progress.validate()?;
        match (self.kind, self.commit_preparation.as_ref()) {
            (OperationKind::Commit, Some(preparation)) => preparation.validate()?,
            (OperationKind::Commit, None) => {
                return Err(ProtocolError::invalid(
                    "operation.commit_preparation",
                    "is required for commit operations",
                ));
            }
            (_, Some(_)) => {
                return Err(ProtocolError::invalid(
                    "operation.commit_preparation",
                    "is only valid for commit operations",
                ));
            }
            (_, None) => {}
        }
        match (self.kind, self.restore_preparation.as_ref()) {
            (OperationKind::Restore, Some(preparation)) => preparation.validate()?,
            (OperationKind::Restore, None) => {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation",
                    "is required for restore operations",
                ));
            }
            (_, Some(_)) => {
                return Err(ProtocolError::invalid(
                    "operation.restore_preparation",
                    "is only valid for restore operations",
                ));
            }
            (_, None) => {}
        }
        match self.state {
            OperationState::Succeeded if self.result.is_none() || self.failure.is_some() => {
                return Err(ProtocolError::invalid(
                    "operation",
                    "succeeded state requires a result and forbids a failure",
                ));
            }
            OperationState::Failed | OperationState::Quarantined
                if self.failure.is_none() || self.result.is_some() =>
            {
                return Err(ProtocolError::invalid(
                    "operation",
                    "terminal error state requires a failure and forbids a result",
                ));
            }
            OperationState::Running | OperationState::Aborting
                if self.result.is_some() || self.failure.is_some() =>
            {
                return Err(ProtocolError::invalid(
                    "operation",
                    "non-terminal state forbids a result or failure",
                ));
            }
            _ => {}
        }
        if let Some(result) = &self.result {
            result.validate()?;
            let (expected_kind, result_operation_id) = match result {
                OperationResult::ArtifactPublish(result) => {
                    (OperationKind::ArtifactPublish, result.operation_id)
                }
                OperationResult::Commit(result) => (OperationKind::Commit, result.operation_id),
                OperationResult::Restore(result) => (OperationKind::Restore, result.operation_id),
            };
            if self.kind != expected_kind {
                return Err(ProtocolError::invalid(
                    "operation.kind",
                    "does not match terminal result",
                ));
            }
            if self.token.operation_id != result_operation_id {
                return Err(ProtocolError::invalid(
                    "operation.result.operation_id",
                    "does not match the operation token",
                ));
            }
            if let OperationResult::Commit(result) = result {
                let preparation = self.commit_preparation.as_ref().ok_or_else(|| {
                    ProtocolError::invalid(
                        "operation.commit_preparation",
                        "is required for a terminal commit result",
                    )
                })?;
                if preparation.request.commit_id != result.commit_id
                    || preparation.request.workbench != result.workbench
                    || preparation.manifest.is_none()
                {
                    return Err(ProtocolError::invalid(
                        "operation.commit_preparation",
                        "does not contain the exact terminal commit and manifest binding",
                    ));
                }
                let expected_generation = preparation
                    .request
                    .expected_head_generation
                    .map_or(Some(1), |generation| generation.checked_add(1));
                if expected_generation != Some(result.head_generation) {
                    return Err(ProtocolError::invalid(
                        "operation.commit_preparation.expected_head_generation",
                        "does not lead to the terminal head generation",
                    ));
                }
            }
            if let OperationResult::Restore(result) = result {
                let preparation = self.restore_preparation.as_ref().ok_or_else(|| {
                    ProtocolError::invalid(
                        "operation.restore_preparation",
                        "is required for a terminal restore result",
                    )
                })?;
                if preparation.request.destination_workbench != result.destination.workbench
                    || preparation.request.destination_workspace_incarnation_id
                        != result.destination.workspace_incarnation_id
                    || preparation.materialized_member_count != Some(result.member_count)
                    || preparation.materialized_member_digest != Some(result.member_digest)
                {
                    return Err(ProtocolError::invalid(
                        "operation.restore_preparation",
                        "does not match the terminal restore result",
                    ));
                }
                let destination_binding =
                    preparation.destination_binding.as_ref().ok_or_else(|| {
                        ProtocolError::invalid(
                            "operation.restore_preparation.destination_binding",
                            "is required for a terminal restore result",
                        )
                    })?;
                if destination_binding.destination_manifests.is_none()
                    || result.destination.commit_head
                        != Some(destination_binding.destination_commit_id)
                    || result.destination.commit_head_generation != Some(1)
                {
                    return Err(ProtocolError::invalid(
                        "operation.restore_preparation.destination_binding",
                        "does not contain the final manifest bindings and generation-one destination commit receipt",
                    ));
                }
            }
        }
        if let Some(failure) = &self.failure {
            failure.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchHit {
    pub metadata: PathMetadata,
    pub projection: Vec<FieldValue>,
}

impl SearchHit {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.metadata.validate()?;
        if self.projection.len() > 64 {
            return Err(ProtocolError::invalid(
                "search_hit.projection",
                "exceeds 64 fields",
            ));
        }
        for field in &self.projection {
            field.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FacetBucket {
    pub value: ScalarValue,
    pub count: u64,
}

impl FacetBucket {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.value.validate()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FacetResult {
    pub field_id: String,
    pub buckets: Vec<FacetBucket>,
    pub distinct_count: u64,
    pub truncated: bool,
}

impl FacetResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_field_id("facet.field_id", &self.field_id)?;
        if self.buckets.len() > 256 {
            return Err(ProtocolError::invalid(
                "facet.buckets",
                "exceeds 256 buckets",
            ));
        }
        if self.distinct_count < self.buckets.len() as u64 {
            return Err(ProtocolError::invalid(
                "facet.distinct_count",
                "is smaller than returned bucket count",
            ));
        }
        for bucket in &self.buckets {
            bucket.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub facets: Vec<FacetResult>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: u64,
}

impl SearchResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.hits.len() > 1_000 {
            return Err(ProtocolError::invalid("search.hits", "exceeds 1000 rows"));
        }
        if self.facets.len() > 16 {
            return Err(ProtocolError::invalid("search.facets", "exceeds 16 fields"));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "search.read_version",
                "must be greater than zero",
            ));
        }
        validate_cursor("search.next_cursor", self.next_cursor.as_deref())?;
        for hit in &self.hits {
            hit.validate()?;
        }
        for facet in &self.facets {
            facet.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateGroup {
    pub keys: Vec<FieldValue>,
    pub values: Vec<FieldValue>,
}

impl AggregateGroup {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.keys.len() > 8 || self.values.len() > 16 {
            return Err(ProtocolError::invalid(
                "aggregate.group",
                "exceeds key or value bounds",
            ));
        }
        for field in self.keys.iter().chain(&self.values) {
            field.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateResult {
    pub groups: Vec<AggregateGroup>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: u64,
}

impl AggregateResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.groups.len() > 1_000 {
            return Err(ProtocolError::invalid(
                "aggregate.groups",
                "exceeds 1000 rows",
            ));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "aggregate.read_version",
                "must be greater than zero",
            ));
        }
        validate_cursor("aggregate.next_cursor", self.next_cursor.as_deref())?;
        for group in &self.groups {
            group.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CatalogField {
    pub field_id: String,
    pub scalar_type: String,
    pub operators: Vec<QueryOperator>,
    pub sortable: bool,
    pub facetable: bool,
    pub aggregatable: bool,
}

impl CatalogField {
    fn validate(&self) -> Result<(), ProtocolError> {
        validate_field_id("catalog.field_id", &self.field_id)?;
        validate_field_id("catalog.scalar_type", &self.scalar_type)?;
        if self.operators.is_empty() {
            return Err(ProtocolError::invalid(
                "catalog.operators",
                "must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CatalogResult {
    pub fields: Vec<CatalogField>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: u64,
}

impl CatalogResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.fields.len() > 1_000 {
            return Err(ProtocolError::invalid(
                "catalog.fields",
                "exceeds 1000 rows",
            ));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "catalog.read_version",
                "must be greater than zero",
            ));
        }
        validate_cursor("catalog.next_cursor", self.next_cursor.as_deref())?;
        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSummaryWithCommit {
    pub workspace: WorkspaceSummary,
    pub commit: Option<CommitResult>,
}

impl WorkspaceSummaryWithCommit {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.workspace.validate()?;
        if let Some(commit) = &self.commit {
            commit.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FindWorkspacesResult {
    pub workspaces: Vec<WorkspaceSummaryWithCommit>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: u64,
}

impl FindWorkspacesResult {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.workspaces.len() > 1_000 {
            return Err(ProtocolError::invalid("workspaces", "exceeds 1000 rows"));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "workspaces.read_version",
                "must be greater than zero",
            ));
        }
        validate_cursor("workspaces.next_cursor", self.next_cursor.as_deref())?;
        for workspace in &self.workspaces {
            workspace.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    WorkspaceCreated,
    ArtifactPublished,
    PathRemoved,
    CommitPublished,
    CommitRetired,
    SnapshotMinted,
    SnapshotRenewed,
    SnapshotRetired,
    SnapshotReapClaimed,
    SnapshotReaped,
    SnapshotConsumerAttached,
    SnapshotConsumerReleased,
    WorkspaceRestored,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangeEvent {
    pub commit_version: u64,
    pub event_sequence: u32,
    pub kind: ChangeKind,
    pub workbench: Option<WorkbenchName>,
    pub path: Option<WorkspacePath>,
    pub artifact_revision_id: Option<ArtifactRevisionIdentity>,
    pub commit_id: Option<CommitIdentity>,
    pub operation_id: Option<OperationIdentity>,
}

impl ChangeEvent {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.commit_version == 0 {
            return Err(ProtocolError::invalid(
                "change.commit_version",
                "must be greater than zero",
            ));
        }
        if let (Some(workbench), Some(path)) = (&self.workbench, &self.path) {
            if workbench != &path.workbench {
                return Err(ProtocolError::invalid(
                    "change.path",
                    "workbench does not match event workbench",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangePage {
    pub events: Vec<ChangeEvent>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: u64,
}

impl ChangePage {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.events.len() > 1_000 {
            return Err(ProtocolError::invalid(
                "changes.events",
                "exceeds 1000 events",
            ));
        }
        if self.read_version == 0 {
            return Err(ProtocolError::invalid(
                "changes.read_version",
                "must be greater than zero",
            ));
        }
        validate_cursor("changes.next_cursor", self.next_cursor.as_deref())?;
        for change in &self.events {
            change.validate()?;
            if change.commit_version > self.read_version {
                return Err(ProtocolError::invalid(
                    "changes.events",
                    "event commit version exceeds page read version",
                ));
            }
        }
        Ok(())
    }
}

fn validate_cursor(field: &'static str, cursor: Option<&[u8]>) -> Result<(), ProtocolError> {
    if cursor.is_some_and(|cursor| cursor.len() > 4_096) {
        return Err(ProtocolError::invalid(field, "exceeds 4096 bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_requires_a_positive_read_version() {
        let mut result = CatalogResult {
            fields: Vec::new(),
            next_cursor: None,
            read_version: 0,
        };
        assert!(matches!(
            result.validate(),
            Err(ProtocolError::InvalidField {
                field: "catalog.read_version",
                ..
            })
        ));

        result.read_version = 1;
        result.validate().unwrap();
    }

    fn running_status(kind: OperationKind) -> OperationStatus {
        OperationStatus {
            token: OperationToken {
                operation_id: OperationIdentity([1; 16]),
                state_digest: Digest([2; 32]),
            },
            kind,
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

    fn commit_request(
        expected_head_generation: Option<u64>,
        parents: Vec<CommitIdentity>,
    ) -> CommitRequest {
        CommitRequest {
            operation_id: OperationIdentity([1; 16]),
            workbench: WorkbenchName::new("run").unwrap(),
            workspace_incarnation_id: WorkspaceIdentity([5; 16]),
            commit_id: CommitIdentity([3; 32]),
            content_digest: DigestUri::new(format!("sha256:{}", "06".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "07".repeat(32))).unwrap(),
            projection_input_digest: Digest([0x08; 32]),
            tree_manifest_revision_id: ArtifactRevisionIdentity([4; 16]),
            replace: expected_head_generation.is_some(),
            run_manifest_condition: if expected_head_generation.is_some() {
                crate::PublishCondition::ReplaceOnly {
                    expected_generation: 3,
                }
            } else {
                crate::PublishCondition::CreateOnly
            },
            expected_head_generation,
            parents,
            producer: None,
            lineage_projection: Vec::new(),
        }
    }

    fn commit_manifest() -> CommitManifestBinding {
        CommitManifestBinding {
            workspace_incarnation_id: WorkspaceIdentity([5; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([4; 16]),
            descriptor: ArtifactDescriptor {
                logical_size: 64,
                body_digest: DigestUri::new(format!("sha256:{}", "08".repeat(32))).unwrap(),
                manifest_digest: DigestUri::new(format!("sha256:{}", "09".repeat(32))).unwrap(),
                content_type: crate::ContentType::new("application/json").unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        }
    }

    fn restore_descriptor(body_byte: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            logical_size: 128,
            body_digest: DigestUri::new(format!("sha256:{}", body_byte.repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "cd".repeat(32))).unwrap(),
            content_type: crate::ContentType::new("application/json").unwrap(),
            producer: None,
            manifest_identity: None,
            index_fields: Vec::new(),
        }
    }

    fn restore_operation_preparation(destination: &WorkbenchName) -> RestoreOperationPreparation {
        let source_commit = RestoreSourceCommitBinding {
            commit_id: CommitIdentity([0x11; 32]),
            content_digest: DigestUri::new(format!("sha256:{}", "12".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "13".repeat(32))).unwrap(),
            tree_manifest_revision_id: ArtifactRevisionIdentity([0x14; 16]),
            member_count: 2,
            member_digest: Digest([0x15; 32]),
        };
        let restore_identity = crate::request::RestoreManifestIdentity {
            publication_operation_id: OperationIdentity([0x16; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([0x17; 16]),
        };
        RestoreOperationPreparation {
            request: PrepareRestoreRequest {
                operation_id: OperationIdentity([1; 16]),
                source_workbench: WorkbenchName::new("source").unwrap(),
                source_workspace_incarnation_id: WorkspaceIdentity([3; 16]),
                source: crate::request::RestoreSource::Snapshot(crate::SnapshotSelector::Id(7)),
                destination_workbench: destination.clone(),
                destination_workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                destination_restore_manifest_identity: restore_identity,
                restore_manifest: crate::request::RestoreManifestDescriptor {
                    body_digest: DigestUri::new(format!("sha256:{}", "ab".repeat(32))).unwrap(),
                    logical_size: 128,
                    content_type: crate::ContentType::new("application/json").unwrap(),
                },
            },
            source_snapshot_read_version: Some(9),
            source_commit: source_commit.clone(),
            destination_committed_at_unix_seconds: 1_700_000_000,
            source_member_count: Some(2),
            source_member_digest: Some(Digest([0x15; 32])),
            materialized_member_count: Some(1),
            materialized_member_digest: Some(Digest([0x18; 32])),
            source_matches_base_commit: Some(true),
            destination_binding: Some(Box::new(RestoreDestinationBinding {
                destination_commit_id: CommitIdentity([0x19; 32]),
                effective_content_digest: source_commit.content_digest.clone(),
                destination_run_manifest_projection_input_digest: Digest([0x1a; 32]),
                destination_run_manifest_identity: crate::request::RestoreManifestIdentity {
                    publication_operation_id: OperationIdentity([0x1b; 16]),
                    artifact_revision_id: ArtifactRevisionIdentity([0x1c; 16]),
                },
                destination_restore_manifest_identity: restore_identity,
                destination_manifests: Some(RestoreDestinationManifestBindings {
                    run_manifest: RestoreManifestBinding {
                        publication_operation_id: OperationIdentity([0x1b; 16]),
                        workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                        artifact_revision_id: ArtifactRevisionIdentity([0x1c; 16]),
                        descriptor: restore_descriptor("de"),
                    },
                    restore_manifest: RestoreManifestBinding {
                        publication_operation_id: restore_identity.publication_operation_id,
                        workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                        artifact_revision_id: restore_identity.artifact_revision_id,
                        descriptor: restore_descriptor("ab"),
                    },
                }),
            })),
        }
    }

    #[test]
    fn commit_status_requires_exact_durable_preparation() {
        let mut status = running_status(OperationKind::Commit);
        assert!(status.validate().is_err());

        status.commit_preparation = Some(Box::new(CommitPreparation {
            request: Box::new(commit_request(None, Vec::new())),
            committed_at_unix_seconds: 1_700_000_000,
            manifest: None,
        }));
        status.validate().unwrap();

        let mut publish = running_status(OperationKind::ArtifactPublish);
        publish.commit_preparation = status.commit_preparation.clone();
        assert!(publish.validate().is_err());
    }

    #[test]
    fn commit_preparation_requires_bounded_sorted_unique_parents() {
        let mut status = running_status(OperationKind::Commit);
        status.commit_preparation = Some(Box::new(CommitPreparation {
            request: Box::new(commit_request(
                None,
                vec![CommitIdentity([2; 32]), CommitIdentity([1; 32])],
            )),
            committed_at_unix_seconds: 1_700_000_000,
            manifest: None,
        }));
        assert!(status.validate().is_err());

        status.commit_preparation.as_mut().unwrap().request.parents = (0
            ..=crate::request::MAX_PARENT_COMMITS)
            .map(|index| {
                let mut identity = [0; 32];
                identity[31] = u8::try_from(index).unwrap();
                CommitIdentity(identity)
            })
            .collect();
        assert!(status.validate().is_err());
    }

    #[test]
    fn terminal_commit_head_generation_must_follow_the_durable_expected_head() {
        let mut status = running_status(OperationKind::Commit);
        status.commit_preparation = Some(Box::new(CommitPreparation {
            request: Box::new(commit_request(Some(7), Vec::new())),
            committed_at_unix_seconds: 1_700_000_000,
            manifest: Some(commit_manifest()),
        }));
        status.state = OperationState::Succeeded;
        status.result = Some(OperationResult::Commit(CommitResult {
            operation_id: status.token.operation_id,
            commit_id: CommitIdentity([3; 32]),
            workbench: WorkbenchName::new("run").unwrap(),
            head_generation: 9,
            member_count: 0,
            member_digest: Digest([0; 32]),
        }));
        assert!(status.validate().is_err());

        let Some(OperationResult::Commit(result)) = status.result.as_mut() else {
            unreachable!();
        };
        result.head_generation = 8;
        status.validate().unwrap();
    }

    #[test]
    fn restore_status_requires_durable_identity_source_and_seal() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut status = running_status(OperationKind::Restore);
        status.restore_preparation = Some(Box::new(restore_operation_preparation(&destination)));
        status.validate().unwrap();

        status.state = OperationState::Succeeded;
        status.result = Some(OperationResult::Restore(RestoreResult {
            operation_id: status.token.operation_id,
            destination: WorkspaceSummary {
                workbench: destination,
                workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                workspace_revision: 1,
                commit_head: Some(CommitIdentity([0x19; 32])),
                commit_head_generation: Some(1),
            },
            member_count: 1,
            member_digest: Digest([0x18; 32]),
            metadata_rows_copied: 1,
            object_bytes_copied: 0,
        }));
        status.validate().unwrap();

        status
            .restore_preparation
            .as_mut()
            .unwrap()
            .materialized_member_count = Some(3);
        assert!(status.validate().is_err());
    }

    #[test]
    fn restore_operation_preparation_uses_phase_aware_complete_seals() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation.source_member_count = None;
        preparation.source_member_digest = None;
        preparation.materialized_member_count = None;
        preparation.materialized_member_digest = None;
        preparation.source_matches_base_commit = None;
        preparation.destination_binding = None;
        preparation.validate().unwrap();

        preparation.source_member_count = Some(2);
        assert!(preparation.validate().is_err());
    }

    #[test]
    fn restore_materialized_members_are_a_subset_of_the_raw_source() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation.source_member_count = Some(2);
        preparation.source_member_digest = Some(Digest([0x15; 32]));
        preparation.materialized_member_count = Some(3);
        preparation.materialized_member_digest = Some(Digest([0x18; 32]));

        assert!(matches!(
            preparation.validate(),
            Err(ProtocolError::InvalidField {
                field: "operation.restore_preparation.materialized_members",
                ..
            })
        ));
    }

    #[test]
    fn restore_source_match_is_recomputed_from_the_base_commit_seal() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation.source_matches_base_commit = Some(false);
        assert!(matches!(
            preparation.validate(),
            Err(ProtocolError::InvalidField {
                field: "restore.source_matches_base_commit",
                ..
            })
        ));
    }

    #[test]
    fn dirty_restore_requires_a_distinct_effective_content_digest() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation.source_member_digest = Some(Digest([0x44; 32]));
        preparation.source_matches_base_commit = Some(false);
        assert!(preparation.validate().is_err());

        preparation
            .destination_binding
            .as_mut()
            .unwrap()
            .effective_content_digest =
            DigestUri::new(format!("sha256:{}", "45".repeat(32))).unwrap();
        preparation.validate().unwrap();
    }

    #[test]
    fn direct_commit_restore_forbids_dirty_source_semantics() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation.request.source =
            crate::request::RestoreSource::Commit(preparation.source_commit.commit_id);
        preparation.source_snapshot_read_version = None;
        preparation.source_member_digest = Some(Digest([0x44; 32]));
        preparation.source_matches_base_commit = Some(false);
        preparation
            .destination_binding
            .as_mut()
            .unwrap()
            .effective_content_digest =
            DigestUri::new(format!("sha256:{}", "45".repeat(32))).unwrap();
        assert!(matches!(
            preparation.validate(),
            Err(ProtocolError::InvalidField {
                field: "operation.restore_preparation.source_matches_base_commit",
                ..
            })
        ));
    }

    #[test]
    fn final_restore_manifests_exact_bind_expected_identities_and_incarnation() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation
            .destination_binding
            .as_mut()
            .unwrap()
            .destination_manifests
            .as_mut()
            .unwrap()
            .run_manifest
            .workspace_incarnation_id = WorkspaceIdentity([0x46; 16]);
        assert!(matches!(
            preparation.validate(),
            Err(ProtocolError::InvalidField {
                field: "operation.restore_preparation.destination_binding.destination_manifests",
                ..
            })
        ));
    }

    #[test]
    fn source_commit_binding_rejects_noncanonical_member_seals() {
        let destination = WorkbenchName::new("destination").unwrap();
        let mut preparation = restore_operation_preparation(&destination);
        preparation.source_commit.member_count = 0;
        assert!(matches!(
            preparation.validate(),
            Err(ProtocolError::InvalidField {
                field: "restore.source_commit.members",
                ..
            })
        ));
    }

    #[test]
    fn successful_restore_result_requires_a_committed_destination() {
        let result = RestoreResult {
            operation_id: OperationIdentity([1; 16]),
            destination: WorkspaceSummary {
                workbench: WorkbenchName::new("destination").unwrap(),
                workspace_incarnation_id: WorkspaceIdentity([4; 16]),
                workspace_revision: 1,
                commit_head: None,
                commit_head_generation: None,
            },
            member_count: 0,
            member_digest: Digest([0; 32]),
            metadata_rows_copied: 0,
            object_bytes_copied: 0,
        };
        assert!(matches!(
            result.validate(),
            Err(ProtocolError::InvalidField {
                field: "restore.destination.commit_head",
                ..
            })
        ));
    }

    #[test]
    fn restore_source_run_manifest_response_cannot_claim_not_modified() {
        let result = WorkspaceResult::RestoreSourceRunManifest(PathReadResult {
            not_modified: true,
            metadata: None,
            range: None,
            blocks: Vec::new(),
            next_cursor: None,
        });
        assert!(matches!(
            result.validate(),
            Err(ProtocolError::InvalidField {
                field: "restore_source_run_manifest",
                ..
            })
        ));
    }
}
