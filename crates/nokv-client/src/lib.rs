/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Rust SDK for root-routed Agent workspaces.
//!
//! The SDK owns route refresh, exact request replay, retry policy, and typed
//! result extraction. Metadata layout and shard-owner implementation remain
//! outside this crate.

mod artifact;
mod error;
mod route;
mod sdk;
mod snapshot_workflow;
mod transport;
mod workbench_workflow;

pub use artifact::{
    ArtifactAppendOptions, ArtifactAppendOutcome, ArtifactPublishOptions, ArtifactPublishOutcome,
    ArtifactReadOutcome,
};
pub use error::{ArtifactPublishStage, ClientError, TransportError};
#[cfg(feature = "control")]
pub use route::ControlRouteResolver;
#[cfg(feature = "etcd")]
pub use route::EtcdRouteOptions;
pub use route::{ResolvedRoute, RouteRefreshMode, RouteResolver, StaticRouteResolver};
pub use sdk::{ClientCall, ClientOptions, WorkspaceClient};
pub use snapshot_workflow::{
    SnapshotMintOptions, SnapshotRenewOptions, SnapshotRetireOptions, SnapshotRetireOutcome,
};
pub use transport::{FramedTcpOptions, FramedTcpTransport, RpcTransport};
pub use workbench_workflow::{
    CommitRecoveryRequest, CommitWorkflowError, CommitWorkflowIdentities, CommitWorkflowOptions,
    CommitWorkflowOutcome, CommitWorkflowRequest, RestoreManifestIdentities,
    RestoreRecoveryRequest, RestoreWorkflowError, RestoreWorkflowIdentities,
    RestoreWorkflowOptions, RestoreWorkflowOutcome, RestoreWorkflowRequest,
};
