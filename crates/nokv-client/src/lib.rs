/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Rust SDK for root-routed Agent workspaces.
//!
//! The SDK owns route refresh, exact request replay, retry policy, and typed
//! result extraction. Metadata layout and shard-owner implementation remain
//! outside this crate.

mod append_operations;
mod artifact;
mod body_digest;
mod error;
mod generic_index;
mod idempotent_append;
mod idempotent_publish;
mod operation_status;
mod route;
mod sdk;
mod snapshot_workflow;
mod transport;
mod workbench_lifecycle;
mod workbench_workflow;

pub use append_operations::{
    AppendOperationInspection, AppendRecoveryRequestResult, DEFAULT_APPEND_INSPECTION_LIMIT,
    MAX_APPEND_INSPECTION_LIMIT,
};
pub use artifact::{
    visible_body_publishes_inline, ArtifactAppendOptions, ArtifactAppendOutcome,
    ArtifactPublishOptions, ArtifactPublishOutcome, ArtifactRangeBatchItem,
    ArtifactRangeBatchOutcome, ArtifactRangeBatchRequest, ArtifactReadAuthority,
    ArtifactReadOutcome, MAX_ARTIFACT_RANGE_BATCH_OUTPUT_BYTES, MAX_ARTIFACT_RANGE_BATCH_RANGES,
    MAX_ARTIFACT_RANGE_BATCH_READ_BYTES, MAX_ARTIFACT_RANGE_BATCH_REQUESTS,
};
pub use error::{rpc_failure_class, ArtifactPublishStage, ClientError, TransportError};
pub use generic_index::{
    GenericIndexAbortOutcome, GenericIndexRegistrationOutcome, GenericIndexRegistrationPlan,
};
pub use idempotent_append::{
    append_operation_recovery, AppendNextAction, AppendRecovery, AppendRecoveryState,
    IdempotentAppendOptions, DEFAULT_APPEND_MAX_LOGICAL_SIZE, MAX_APPEND_DELTA_BYTES,
};
pub use idempotent_publish::{
    publish_attempt_identity, IdempotentPublishError, IdempotentPublishOptions,
    IdempotentPublishReceipt, PublishAttemptIdentity, MAX_IDEMPOTENT_PUBLISH_ATTEMPTS,
};
pub use operation_status::{operation_observation, OperationObservation, OperationObservedState};
#[cfg(feature = "control")]
pub use route::ControlRouteResolver;
#[cfg(feature = "etcd")]
pub use route::EtcdRouteOptions;
pub use route::{ResolvedRoute, RouteRefreshMode, RouteResolver, StaticRouteResolver};
pub use sdk::{ClientCall, ClientOptions, WorkspaceClient};
pub use snapshot_workflow::{
    SnapshotMintOptions, SnapshotRenewOptions, SnapshotRetireOptions, SnapshotRetireOutcome,
};
pub use transport::{FramedTcpOptions, FramedTcpTransport, RpcClientTls, RpcTransport};
pub use workbench_lifecycle::{
    RestoreManifestProjectionContext, RestoredRunManifestProjectionContext,
    RunManifestProjectionContext, VerifiedWorkbenchRestoreManifest, VerifiedWorkbenchRunManifest,
    WorkbenchAdmission, WorkbenchCommitOutcome, WorkbenchCommitRequest, WorkbenchLifecycleError,
    WorkbenchLifecycleFacade, WorkbenchLifecycleOptions, WorkbenchProjection,
    WorkbenchRestoreOrigin, WorkbenchRestoreOutcome, WorkbenchRestoreRequest,
    WorkbenchRestoreSource, WorkbenchSnapshotSelector,
};
pub use workbench_workflow::{
    CommitRecoveryRequest, CommitWorkflowError, CommitWorkflowIdentities, CommitWorkflowOptions,
    CommitWorkflowOutcome, CommitWorkflowRequest, RestoreDestinationPlan,
    RestoreManifestIdentities, RestoreManifestPublication, RestoreRecoveryRequest,
    RestoreWorkflowError, RestoreWorkflowIdentities, RestoreWorkflowOptions,
    RestoreWorkflowOutcome, RestoreWorkflowRequest,
};
