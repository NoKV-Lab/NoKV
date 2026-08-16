/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Storage-neutral RPC contract for Agent workspaces.
//!
//! Every request is routed by an explicit [`RootRoute`]. Paths are validated by
//! `nokv-types`, and the wire surface exposes neither Holt layout nor
//! object-provider configuration.

mod artifact_publish;
mod codec;
mod error;
mod handshake;
mod request;
mod response;
mod types;

/// Maximum distinct physical owner revisions retained by one artifact.
pub const MAX_ARTIFACT_DEPENDENCY_OWNERS: u32 = 64;
/// Maximum sealed artifact revision dependency depth.
pub const MAX_ARTIFACT_DEPENDENCY_DEPTH: u8 = 8;

pub use artifact_publish::{
    parse_sha256_digest_uri, seal_artifact_publish_plan, sha256_digest_uri,
    ArtifactPublishPlanSeal, MAX_ARTIFACT_PUBLISH_BATCH_ROWS, MAX_ARTIFACT_PUBLISH_OBJECTS,
    MAX_ARTIFACT_READ_PLAN_ROWS,
};
pub use codec::{
    decode_request, decode_response, encode_request, encode_response, MAX_FRAME_BYTES,
    WORKSPACE_CAPABILITY_SCHEMA, WORKSPACE_PREFLIGHT_SCHEMA, WORKSPACE_PROTOCOL_SCHEMA,
};
pub use error::{ConflictKind, ErrorCode, ProtocolError, RpcFailure};
pub use handshake::{
    decode_handshake_frame, decode_handshake_payload, encode_handshake_frame,
    encode_handshake_payload, has_handshake_magic, HandshakeError, HandshakeKind,
    WorkspaceHandshake, HANDSHAKE_FRAME_BYTES, HANDSHAKE_PAYLOAD_BYTES, HANDSHAKE_SCHEMA_BYTES,
    WORKSPACE_HANDSHAKE_MAGIC,
};
pub use request::{
    AbortArtifactPublishRequest, AggregateFunction, AggregateRequest, AggregateSpec,
    BeginArtifactPublishRequest, BindRestoreDestinationRequest, CatalogRequest, ChangePageRequest,
    CommitRequest, CompleteArtifactPublishRequest, CreateWorkspaceRequest, FinalizeRestoreRequest,
    FindWorkspacesRequest, GetOperationRequest, GetPathRequest, GetSnapshotRequest,
    GetWorkspaceRequest, ListPathsRequest, ListSnapshotsRequest,
    MarkArtifactObjectsUploadedRequest, MintSnapshotRequest, ObjectUploadProof,
    PrepareRestoreRequest, QuarantineResolution, QueryOperand, QueryOperator, QueryPredicate,
    QueryScope, ReadRestoreSourceRunManifestRequest, ReconcileQuarantinedArtifactPublishRequest,
    RemovePathRequest, RenamePathRequest, RenewSnapshotRequest, RestoreManifestDescriptor,
    RestoreManifestIdentity, RestoreSource, RetireSnapshotRequest, SearchRequest, SortDirection,
    SortField, StageArtifactManifestRequest, StageArtifactObjectsRequest,
    WorkspaceContinuationFence, WorkspacePreflightRequest, WorkspaceRequest, WorkspaceRpcRequest,
    MAX_QUERY_PAGE_LIMIT,
};
pub use response::{
    AggregateGroup, AggregateResult, CatalogField, CatalogResult, ChangeEvent, ChangeKind,
    ChangePage, CommitManifestBinding, CommitPreparation, CommitResult, FacetBucket, FacetResult,
    FindWorkspacesResult, OperationProgress, OperationResult, OperationState, OperationStatus,
    PathListEntry, PathPage, PathReadResult, PublishResult, RemovePathResult, RenamePathResult,
    RestoreDestinationBinding, RestoreDestinationManifestBindings, RestoreManifestBinding,
    RestoreOperationPreparation, RestorePreparation, RestoreResult, RestoreSourceCommitBinding,
    SearchHit, SearchResult, SnapshotPage, SnapshotResult, SnapshotStatus,
    WorkspacePreflightResult, WorkspaceResult, WorkspaceRpcOutcome, WorkspaceRpcResponse,
    WorkspaceSummary, WorkspaceSummaryWithCommit,
};
pub use types::{
    AppendSegment, ArtifactDescriptor, ArtifactManifestRow, ArtifactRevisionIdentity, ByteRange,
    CommitIdentity, ContentType, Digest, DigestUri, FieldValue, LogicalShardIdentity,
    ObjectIdentity, ObjectNamespaceIdentity, OperationIdentity, OperationKind, OperationToken,
    PageRequest, PathMetadata, PublicationAuthority, PublishCondition, RelativePath,
    RequestIdentity, RootIdentity, RootRoute, ScalarValue, SnapshotAlias, SnapshotSelector,
    StagedObject, Tag, WorkbenchName, WorkspaceCapability, WorkspaceIdentity, WorkspacePath,
    WorkspaceReadView,
};
