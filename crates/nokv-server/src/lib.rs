/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Logical-shard owner RPC process for Agent workspaces.
//!
//! The server validates the exact installed root route, frames the sole
//! workspace protocol, and delegates domain execution. Metadata semantics stay
//! in `nokv-meta`.

mod bootstrap;
mod error;
mod executor;
mod legacy_rejection;
mod lifecycle;
mod metadata_url;
mod recovery_installer;
mod recovery_publisher;
mod registry;
mod server;
mod service;
#[cfg(test)]
mod test_support;

pub use bootstrap::{bootstrap_shard, LeaseMode, OpenMode, RootAttach, ShardBoot, ShardOwner};
pub use error::ServerError;
pub use executor::MetadataWorkspaceRequestExecutor;
#[cfg(feature = "restore-crash-test-support")]
pub use executor::{
    RestoreInitializationBarrier, RestoreInitializationBarrierEvidence,
    RestoreInitializationBarrierPhase, RestoreManifestBindingEvidence,
    RestoreManifestPublicationEvidence,
};
pub use lifecycle::{
    ArtifactLifecycleDeleter, LifecycleAbsenceProof, LifecycleCycleReport,
    LifecycleDeleteDisposition, LifecycleDeleteError, LifecycleDeletePurpose,
    LifecycleDeleteRequest, LifecycleDurabilityBarrier, LifecycleError, LifecycleObjectDeleter,
    LifecycleRunner, LifecycleRunnerOptions,
};
pub use metadata_url::{
    FoundationDbMetadataUrl, HoltMetadataUrl, MetadataUrl, MetadataUrlError,
    MAX_FOUNDATIONDB_PREFIX_BYTES,
};
pub use recovery_installer::{
    install_recovery_log, validate_local_recovery_prefix, LocalRecoveryPrefixReport,
    RecoveryInstallationReport, RecoveryInstallerError, MAX_RECOVERY_INSTALL_PAYLOAD_BYTES,
    MAX_RECOVERY_INSTALL_RECEIPT_BYTES, MAX_RECOVERY_INSTALL_SEGMENTS,
};
pub use recovery_publisher::{
    RecoveryPublicationMode, RecoveryPublisher, RecoveryPublisherError, RecoveryPublishingExecutor,
};
pub use registry::RootOwnerRegistry;
pub use server::{
    OwnerLossSignal, RouteDiscoverySource, ServerHealth, ServerOptions, WorkspaceServer,
};
pub use service::{ExecutedRequest, WorkspaceRequestExecutor};
