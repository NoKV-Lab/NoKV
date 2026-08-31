/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Logical-shard owner RPC process for Agent workspaces.
//!
//! The server validates the exact installed root route, frames the sole
//! workspace protocol, and delegates domain execution. Metadata semantics stay
//! in `nokv-meta`.

mod error;
mod executor;
#[cfg(feature = "fdb")]
mod fdb_runtime;
mod holt_runtime;
mod legacy_rejection;
mod lifecycle;
mod metadata_url;
mod registry;
mod server;
mod service;
#[cfg(test)]
mod test_support;

pub use error::ServerError;
pub use executor::MetadataWorkspaceRequestExecutor;
#[cfg(feature = "restore-crash-test-support")]
pub use executor::{
    RestoreInitializationBarrier, RestoreInitializationBarrierEvidence,
    RestoreInitializationBarrierPhase, RestoreManifestBindingEvidence,
    RestoreManifestPublicationEvidence,
};
#[cfg(feature = "fdb")]
pub use fdb_runtime::{
    format_fdb, provision_fdb, serve_fdb, FdbFormatOutcome, FdbFormatState, FdbProvisionOutcome,
    FdbServedRoot, FdbServingRuntime,
};
pub use holt_runtime::{
    format_holt, provision_holt, serve_holt, HoltFormatOutcome, HoltFormatState,
    HoltProvisionOutcome, HoltRootCatalogEntry, HoltServingRuntime,
};
pub use lifecycle::{
    ArtifactLifecycleDeleter, CommittedMetadataDurability, LifecycleAbsenceProof,
    LifecycleCycleReport, LifecycleDeleteDisposition, LifecycleDeleteError, LifecycleDeletePurpose,
    LifecycleDeleteRequest, LifecycleDurabilityBarrier, LifecycleDurabilityError, LifecycleError,
    LifecycleObjectDeleter, LifecycleRunner, LifecycleRunnerOptions,
};
pub use metadata_url::{
    FoundationDbMetadataUrl, HoltMetadataUrl, MetadataUrl, MetadataUrlError,
    MAX_FOUNDATIONDB_PREFIX_BYTES,
};
pub use registry::RootOwnerRegistry;
pub use server::{
    OwnerLossSignal, RouteDiscoverySource, ServerHealth, ServerOptions, WorkspaceServer,
};
pub use service::{ExecutedRequest, WorkspaceRequestExecutor};
