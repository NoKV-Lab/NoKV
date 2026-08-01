/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Root-affine shard-owner RPC process for Agent workspaces.
//!
//! The server validates the exact installed root route, frames the sole
//! workspace protocol, and delegates domain execution. Metadata semantics stay
//! in `nokv-meta`.

mod bootstrap;
mod error;
mod executor;
mod lifecycle;
mod registry;
mod server;
mod service;

pub use bootstrap::{
    bootstrap_root_owner, BootstrappedRootOwner, ControlBackedRootOwner, MetadataStoreOpen,
    OwnerAdmission, RootOwnerBootstrapRequest,
};
pub use error::ServerError;
pub use executor::MetadataWorkspaceRequestExecutor;
pub use lifecycle::{
    ArtifactLifecycleDeleter, LifecycleAbsenceProof, LifecycleCycleReport,
    LifecycleDeleteDisposition, LifecycleDeleteError, LifecycleDeletePurpose,
    LifecycleDeleteRequest, LifecycleError, LifecycleObjectDeleter, LifecycleRunner,
    LifecycleRunnerOptions,
};
pub use registry::RootOwnerRegistry;
pub use server::{OwnerLossSignal, ServerHealth, ServerOptions, WorkspaceServer};
pub use service::{ExecutedRequest, WorkspaceRequestExecutor};
