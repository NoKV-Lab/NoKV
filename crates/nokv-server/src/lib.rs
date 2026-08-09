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
mod metadata_runtime;
mod registry;
mod runtime_registry;
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
#[cfg(feature = "foundationdb-provider")]
pub use metadata_runtime::{
    foundationdb_runtime_descriptor, foundationdb_runtime_factory, FoundationDbRuntimeConfig,
    FoundationDbTransactionPolicy,
};
pub use metadata_runtime::{
    holt_file_runtime_factory, holt_reserved_existing_runtime_factory, holt_runtime_descriptor,
    FOUNDATIONDB_METADATA_PROFILE_ID, HOLT_LOCAL_METADATA_PROFILE_ID,
};
pub use nokv_meta::built_in_holt::{
    acquire_existing_file_store_reservation_v1, HoltExistingStoreReservation, HoltRuntimeGuard,
    HoltRuntimeGuardError, HoltStoreObjectIdentity,
};
pub use nokv_meta::workspace::{
    AcknowledgedMetadataFrontier, ClaimedMetadataCommitReceiptPersistCommandV1,
    ClaimedMetadataCommitReceiptPoisonCommandV1, ClaimedMetadataCommitReceiptResolveCommandV1,
    ClaimedMetadataPendingRecoveryOpenCommandV1, MetadataAuthorityCommitActionV1,
    MetadataCommandCommitClassV1, MetadataCommitPurposeV1, MetadataCommitReceiptDirtySourceV1,
    MetadataCommitReceiptErrorV1, MetadataCommitReceiptMutationBackendResultV1,
    MetadataCommitReceiptMutationNotDispatchedV1, MetadataCommitReceiptPersistBackendResultV1,
    MetadataCommitReceiptPersistCommandV1, MetadataCommitReceiptPersistErrorV1,
    MetadataCommitReceiptPersistNotDispatchedV1, MetadataCommitReceiptPersistOutcomeV1,
    MetadataCommitReceiptPoisonCommandV1, MetadataCommitReceiptPoisonOutcomeV1,
    MetadataCommitReceiptPoisonReasonV1, MetadataCommitReceiptQualificationV1,
    MetadataCommitReceiptResolveCommandV1, MetadataCommitReceiptResolveOutcomeV1,
    MetadataCommitReceiptStateV1, MetadataCommitReceiptStoreV1,
    MetadataCommitRecoveryFenceFactoryV1, MetadataCommitResolutionBasisV1,
    MetadataCommitResolutionV1, MetadataFrontierPointV1, MetadataOldDispatchExcludedProviderV1,
    MetadataOldDispatchExclusionInstallationV1, MetadataPendingRecoveryOpenBackendResultV1,
    MetadataPendingRecoveryOpenCommandV1, MetadataPendingRecoveryOpenErrorV1,
    MetadataPendingRecoveryOpenNotDispatchedV1, MetadataPendingRecoveryOpenOutcomeV1,
    MetadataPendingRecoveryOpenWitnessV1, MetadataRuntimeCommitBundleV1, MetadataStoreIdentity,
    PlannedMetadataCommitV1,
};
pub use registry::RootOwnerRegistry;
pub use runtime_registry::{
    AdmissionCode, BootstrapAdmission, ExternalOwnerRuntimeBundle, LifecycleCapabilities,
    LifecycleTransition, OpenIntent, OwnerReceiptMode, OwnerReleaseReceipt,
    OwnerReleaseReceiptError, QualificationCode, ResolvedRuntime, RuntimeAdmissionError,
    RuntimeConsistencyDomain, RuntimeDescriptor, RuntimeDescriptorError, RuntimeDescriptorRegistry,
    RuntimeDescriptorRegistryError, RuntimeFactory, RuntimeFactoryError, RuntimeFactoryErrorCode,
    RuntimeLifecycleValidationError, RuntimeLifecycleValidator, RuntimeProviderBinding,
    RuntimeProviderFactory, RuntimeQualification, RuntimeRegistry, RuntimeRegistryError,
};
pub use server::{OwnerLossSignal, ServerHealth, ServerOptions, WorkspaceServer};
pub use service::{ExecutedRequest, WorkspaceRequestExecutor};
