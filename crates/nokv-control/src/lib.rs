//! Durable NoKV root placement, logical-shard ownership fencing, and recovery
//! publication.
//!
//! This crate owns only control-plane state. Namespace metadata, artifact
//! lifecycle, storage-engine details, and client routing policy stay in their
//! respective packages.
//!
//! The planned-owner coordinator remains test-only until a sealed factory can
//! supply its exact durable-attempt bundle; crate-external safe composition is
//! intentionally unavailable.
//!
//! ```compile_fail
//! use nokv_control::{OwnerAdmissionCoordinatorV1, TrustedOwnerAdmissionAttemptPortV1};
//! ```

mod codec;
mod errors;
#[cfg(feature = "etcd")]
mod etcd;
mod options;
mod owner_admission;
mod owner_admission_command;
#[cfg(test)]
pub mod owner_admission_coordinator;
mod owner_admission_state;
mod store;
mod types;

pub use codec::{
    decode_logical_shard_record, decode_metadata_authority_record, decode_owner_admission_claim,
    decode_owner_admission_intent, decode_owner_admission_plan_sentinel,
    decode_planned_owner_admission, decode_root_placement, encode_logical_shard_record,
    encode_metadata_authority_record, encode_owner_admission_claim, encode_owner_admission_intent,
    encode_owner_admission_plan_sentinel, encode_planned_owner_admission, encode_root_placement,
};
pub use errors::ControlError;
#[cfg(feature = "etcd")]
pub use etcd::EtcdControlStore;
pub use options::EtcdControlStoreOptions;
pub use owner_admission::{
    FiniteOwnerSessionLifetimeObservationV1, OwnerAdmissionAbortReasonV1,
    OwnerAdmissionClaimDigestV1, OwnerAdmissionClaimIdentityV1, OwnerAdmissionClaimPhaseV1,
    OwnerAdmissionClaimV1, OwnerAdmissionIntentDigestV1, OwnerAdmissionIntentV1,
    OwnerAdmissionKindV1, OwnerAdmissionPlanDigestV1, OwnerAdmissionPlanSentinelV1,
    OwnerAdmissionRecordDigestV1, OwnerAdmissionRejectionReasonV1,
    OwnerAdmissionTerminationReasonV1, OwnerLeaseExpiryEvidenceDigest,
    OwnerRuntimeReservationDigest, OwnerServingPublicationDigestV1, OwnerSessionBindingDigestV1,
    OwnerSessionLifetimeObservationV1, OwnerSessionLifetimeProofDigestV1,
    OwnerSessionRenewalTargetDigestV1, OwnerSessionRenewalTargetV1, PlannedOwnerAdmissionV1,
    PlannedOwnerServingPublicationV1,
};
pub use owner_admission_command::{
    AbortOwnerAdmissionCommandV1, AbortOwnerAdmissionInspectionV1,
    AbortOwnerAdmissionNotDispatchedV1, AbortOwnerAdmissionOutcomeV1, AbortOwnerAdmissionResultV1,
    ClaimedAbortOwnerAdmissionCommandV1, ClaimedCommitOwnerAdmissionCommandV1,
    ClaimedPrepareOwnerAdmissionCommandV1, ClaimedPublishOwnerServingCommandV1,
    ClaimedReconcileOwnerAdmissionCommandV1, ClaimedRenewOwnerSessionCommandV1,
    ClaimedTerminateOwnerAdmissionCommandV1, CommitOwnerAdmissionCommandV1,
    CommitOwnerAdmissionNotDispatchedV1, CommitOwnerAdmissionOutcomeV1,
    CommitOwnerAdmissionResultV1, OwnerAdmissionOutcomeUnknownV1,
    OwnerServingPublicationOutcomeUnknownV1, OwnerSessionRenewalOutcomeUnknownV1,
    PrepareOwnerAdmissionCommandV1, PrepareOwnerAdmissionNotDispatchedV1,
    PrepareOwnerAdmissionOutcomeV1, PrepareOwnerAdmissionResultV1, PublishOwnerServingCommandV1,
    PublishOwnerServingInspectionV1, PublishOwnerServingNotDispatchedV1,
    PublishOwnerServingOutcomeV1, PublishOwnerServingResultV1, ReconcileOwnerAdmissionCommandV1,
    ReconcileOwnerAdmissionNotDispatchedV1, ReconcileOwnerAdmissionOutcomeV1,
    ReconcileOwnerAdmissionResultV1, ReconcileOwnerAdmissionTargetV1, RenewOwnerSessionCommandV1,
    RenewOwnerSessionInspectionV1, RenewOwnerSessionNotDispatchedV1, RenewOwnerSessionOutcomeV1,
    RenewOwnerSessionResultV1, TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionInspectionV1, TerminateOwnerAdmissionNotDispatchedV1,
    TerminateOwnerAdmissionOutcomeV1, TerminateOwnerAdmissionResultV1,
};
pub use owner_admission_state::OwnerAdmissionInconsistencyCode;
pub use store::{ControlStore, InMemoryControlStore, OwnerServingAdmission};
pub use types::{
    CheckpointRef, CommitVersion, ConsistencyDomainId, FreshRootProvisioningDisposition,
    FreshRootProvisioningOutcome, LogRef, LogSegmentRef, LogicalShardId, LogicalShardLease,
    LogicalShardRecord, LogicalShardState, MetadataAuthorityBinding, MetadataAuthorityFence,
    MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRecord,
    MetadataAuthorityRevision, MetadataContractDigest, MetadataMigration, MetadataMigrationPhase,
    MetadataMigrationTargetBinding, MetadataProviderProfileId, MetadataProviderProfileIdError,
    MetadataRecoveryFrontier, NodeId, NodeIdError, OperationId, OwnerEpoch, OwnerIncarnationId,
    OwnerLeaseModel, OwnerReleaseOutcome, PlacementGeneration, RecoveryPublication, RootId,
    RootLayoutFence, RootLayoutGeneration, RootLayoutProfile, RootPartitionId, RootPlacement,
    RootPlacementLifecycle, SourceQuiesceReceipt, TargetActivationToken, UnknownLogicalShardState,
    UnknownMetadataMigrationPhase,
};
