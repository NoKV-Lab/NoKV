//! Exact durable receipts for provider-neutral metadata commits.
//!
//! A runtime bundle is one concrete allocation that both opens the metadata
//! provider and owns this receipt state. The engine persists `Pending` before
//! it creates a provider write transaction. Recovery resolves that pending
//! state only from one provider-consistent view of the exact prior or next
//! frontier plus purpose-specific durable evidence.

use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use nokv_types::TargetActivationToken;
use nokv_types::{
    CommandDigest, MetadataMigrationTargetBinding, OperationId, OwnerEpoch, PlacementGeneration,
    RequestId, RootId, SourceQuiesceReceipt,
};
use sha2::{Digest, Sha256};

use super::authority::{
    encode_store_identity, validate_metadata_store_identity, AcknowledgedMetadataFrontier,
    MetadataStoreIdentity,
};
use super::commit_recovery_fence::{
    MetadataCommitRecoveryFenceFactoryV1, MetadataCommitRecoveryOpenAllocationV1,
};

const COMMIT_PLAN_DOMAIN: &[u8] = b"nokv.metadata.commit-plan.v1\0";
const PURPOSE_EVIDENCE_DOMAIN: &[u8] = b"nokv.metadata.commit-purpose-evidence.v1\0";

/// Whether the receipt authority is durable enough for distributed runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptQualificationV1 {
    /// Receipt state survives process restart and is exact-bound to the
    /// provider runtime bundle.
    Durable,
    /// Crate-internal standalone/test mode. This is deliberately not accepted
    /// by the public provider-runtime constructors.
    UntrackedStandalone,
}

/// One exact provider metadata frontier, including the pre-genesis absence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataFrontierPointV1 {
    Absent,
    Exact(AcknowledgedMetadataFrontier),
}

impl MetadataFrontierPointV1 {
    #[must_use]
    pub const fn exact(self) -> Option<AcknowledgedMetadataFrontier> {
        match self {
            Self::Absent => None,
            Self::Exact(frontier) => Some(frontier),
        }
    }
}

/// Closed classification for metadata-command receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommandCommitClassV1 {
    Domain,
    RootFence,
}

/// Closed classification for metadata-authority-only writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataAuthorityCommitActionV1 {
    Quiesce {
        migration_id: OperationId,
        owner_epoch: OwnerEpoch,
    },
    FenceQuiescedSource {
        migration_id: OperationId,
        source_receipt_digest: [u8; 32],
    },
    ActivateTarget {
        migration_id: OperationId,
        activation_token_digest: [u8; 32],
    },
    FenceTarget {
        migration_id: OperationId,
        target_binding_digest: [u8; 32],
    },
}

/// Storage-neutral logical identity of one metadata commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataCommitPurposeV1 {
    Genesis {
        authority_marker_digest: [u8; 32],
    },
    AdvanceOwnerEpoch {
        expected: Option<OwnerEpoch>,
        next: OwnerEpoch,
    },
    ObserveLeaseClock {
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        observed_ms: u64,
    },
    MetadataCommand {
        class: MetadataCommandCommitClassV1,
        root_id: RootId,
        request_id: RequestId,
        command_digest: CommandDigest,
        lease_deadline_ms: Option<u64>,
    },
    Authority {
        action: MetadataAuthorityCommitActionV1,
        prior_marker_digest: [u8; 32],
        next_marker_digest: [u8; 32],
    },
}

impl MetadataCommitPurposeV1 {
    pub(crate) fn expected_delta(&self) -> MetadataFrontierDeltaV1 {
        match self {
            Self::Genesis { .. } => MetadataFrontierDeltaV1 {
                write_sequence: 0,
                commit_version: 1,
                recovery_lsn: 0,
            },
            Self::AdvanceOwnerEpoch { .. } | Self::ObserveLeaseClock { .. } => {
                MetadataFrontierDeltaV1 {
                    write_sequence: 1,
                    commit_version: 0,
                    recovery_lsn: 1,
                }
            }
            Self::MetadataCommand { .. } => MetadataFrontierDeltaV1 {
                write_sequence: 1,
                commit_version: 1,
                recovery_lsn: 1,
            },
            Self::Authority { .. } => MetadataFrontierDeltaV1 {
                write_sequence: 1,
                commit_version: 0,
                recovery_lsn: 0,
            },
        }
    }

    pub(crate) fn hash_into(&self, hasher: &mut Sha256) {
        match self {
            Self::Genesis {
                authority_marker_digest,
            } => {
                hasher.update([1]);
                hasher.update(authority_marker_digest);
            }
            Self::AdvanceOwnerEpoch { expected, next } => {
                hasher.update([2]);
                match expected {
                    Some(expected) => {
                        hasher.update([1]);
                        hasher.update(expected.get().to_be_bytes());
                    }
                    None => hasher.update([0]),
                }
                hasher.update(next.get().to_be_bytes());
            }
            Self::ObserveLeaseClock {
                root_id,
                placement_generation,
                owner_epoch,
                observed_ms,
            } => {
                hasher.update([3]);
                hasher.update(root_id.as_bytes());
                hasher.update(placement_generation.get().to_be_bytes());
                hasher.update(owner_epoch.get().to_be_bytes());
                hasher.update(observed_ms.to_be_bytes());
            }
            Self::MetadataCommand {
                class,
                root_id,
                request_id,
                command_digest,
                lease_deadline_ms,
            } => {
                hasher.update([4]);
                hasher.update([match class {
                    MetadataCommandCommitClassV1::Domain => 1,
                    MetadataCommandCommitClassV1::RootFence => 2,
                }]);
                hasher.update(root_id.as_bytes());
                hasher.update(request_id.as_bytes());
                hasher.update(command_digest.as_bytes());
                match lease_deadline_ms {
                    Some(deadline) => {
                        hasher.update([1]);
                        hasher.update(deadline.to_be_bytes());
                    }
                    None => hasher.update([0]),
                }
            }
            Self::Authority {
                action,
                prior_marker_digest,
                next_marker_digest,
            } => {
                hasher.update([5]);
                hash_authority_action(hasher, *action);
                hasher.update(prior_marker_digest);
                hasher.update(next_marker_digest);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MetadataFrontierDeltaV1 {
    write_sequence: u64,
    commit_version: u64,
    recovery_lsn: u64,
}

/// One exact logical commit persisted before provider write admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedMetadataCommitV1 {
    store_identity: MetadataStoreIdentity,
    frozen_bundle_digest: [u8; 32],
    purpose: MetadataCommitPurposeV1,
    prior: MetadataFrontierPointV1,
    exact_next: AcknowledgedMetadataFrontier,
    canonical_digest: [u8; 32],
}

impl PlannedMetadataCommitV1 {
    pub(crate) fn plan_exact(
        store_identity: MetadataStoreIdentity,
        frozen_bundle_digest: [u8; 32],
        purpose: MetadataCommitPurposeV1,
        prior: MetadataFrontierPointV1,
        exact_next: AcknowledgedMetadataFrontier,
    ) -> Result<Self, MetadataCommitReceiptErrorV1> {
        validate_metadata_store_identity(store_identity)
            .map_err(|_| MetadataCommitReceiptErrorV1::InvalidBinding)?;
        if frozen_bundle_digest.iter().all(|byte| *byte == 0) {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        validate_purpose(&purpose)?;
        validate_delta(&purpose, prior, exact_next)?;
        let canonical_digest = plan_digest(
            store_identity,
            frozen_bundle_digest,
            &purpose,
            prior,
            exact_next,
        );
        Ok(Self {
            store_identity,
            frozen_bundle_digest,
            purpose,
            prior,
            exact_next,
            canonical_digest,
        })
    }

    /// Reconstruct one durable plan after decoding every provider-neutral
    /// field. The canonical digest is recomputed and must equal the separately
    /// persisted digest; invalid identities, purposes, deltas, zero bundle
    /// bindings, and non-genesis `Absent` predecessors are rejected.
    pub fn from_durable_parts_v1(
        store_identity: MetadataStoreIdentity,
        frozen_bundle_digest: [u8; 32],
        purpose: MetadataCommitPurposeV1,
        prior: MetadataFrontierPointV1,
        exact_next: AcknowledgedMetadataFrontier,
        persisted_canonical_digest: [u8; 32],
    ) -> Result<Self, MetadataCommitReceiptErrorV1> {
        let planned = Self::plan_exact(
            store_identity,
            frozen_bundle_digest,
            purpose,
            prior,
            exact_next,
        )?;
        if planned.canonical_digest != persisted_canonical_digest {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        Ok(planned)
    }

    #[must_use]
    pub const fn store_identity(&self) -> MetadataStoreIdentity {
        self.store_identity
    }

    #[must_use]
    pub const fn frozen_bundle_digest(&self) -> [u8; 32] {
        self.frozen_bundle_digest
    }

    #[must_use]
    pub const fn purpose(&self) -> &MetadataCommitPurposeV1 {
        &self.purpose
    }

    #[must_use]
    pub const fn prior(&self) -> MetadataFrontierPointV1 {
        self.prior
    }

    #[must_use]
    pub const fn exact_next(&self) -> AcknowledgedMetadataFrontier {
        self.exact_next
    }

    #[must_use]
    pub const fn canonical_digest(&self) -> [u8; 32] {
        self.canonical_digest
    }

    pub(crate) fn validate_binding(
        &self,
        store_identity: MetadataStoreIdentity,
        frozen_bundle_digest: [u8; 32],
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        let expected = plan_digest(
            self.store_identity,
            self.frozen_bundle_digest,
            &self.purpose,
            self.prior,
            self.exact_next,
        );
        if validate_metadata_store_identity(self.store_identity).is_err()
            || validate_purpose(&self.purpose).is_err()
            || self.store_identity != store_identity
            || self.frozen_bundle_digest != frozen_bundle_digest
            || self.canonical_digest != expected
        {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        validate_delta(&self.purpose, self.prior, self.exact_next)
    }
}

fn validate_purpose(purpose: &MetadataCommitPurposeV1) -> Result<(), MetadataCommitReceiptErrorV1> {
    let all_zero = |bytes: &[u8]| bytes.iter().all(|byte| *byte == 0);
    let invalid = match purpose {
        MetadataCommitPurposeV1::Genesis {
            authority_marker_digest,
        } => all_zero(authority_marker_digest),
        MetadataCommitPurposeV1::AdvanceOwnerEpoch { expected, next } => {
            expected.is_some_and(|expected| next.get() <= expected.get())
        }
        MetadataCommitPurposeV1::ObserveLeaseClock { observed_ms, .. } => *observed_ms == 0,
        MetadataCommitPurposeV1::MetadataCommand {
            command_digest,
            lease_deadline_ms,
            ..
        } => {
            all_zero(command_digest.as_bytes()) || lease_deadline_ms.is_some_and(|value| value == 0)
        }
        MetadataCommitPurposeV1::Authority {
            action,
            prior_marker_digest,
            next_marker_digest,
        } => {
            let invalid_action = match action {
                MetadataAuthorityCommitActionV1::Quiesce { migration_id, .. } => {
                    all_zero(migration_id.as_bytes())
                }
                MetadataAuthorityCommitActionV1::FenceQuiescedSource {
                    migration_id,
                    source_receipt_digest,
                } => all_zero(migration_id.as_bytes()) || all_zero(source_receipt_digest),
                MetadataAuthorityCommitActionV1::ActivateTarget {
                    migration_id,
                    activation_token_digest,
                } => all_zero(migration_id.as_bytes()) || all_zero(activation_token_digest),
                MetadataAuthorityCommitActionV1::FenceTarget {
                    migration_id,
                    target_binding_digest,
                } => all_zero(migration_id.as_bytes()) || all_zero(target_binding_digest),
            };
            invalid_action
                || all_zero(prior_marker_digest)
                || all_zero(next_marker_digest)
                || prior_marker_digest == next_marker_digest
        }
    };
    if invalid {
        Err(MetadataCommitReceiptErrorV1::InvalidBinding)
    } else {
        Ok(())
    }
}

/// Durable state of the exact commit receipt authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptStateV1 {
    Clean {
        store_identity: MetadataStoreIdentity,
        frozen_bundle_digest: [u8; 32],
        frontier: MetadataFrontierPointV1,
    },
    Pending(PlannedMetadataCommitV1),
    PoisonedSettled(PlannedMetadataCommitV1),
    PoisonedUnsettled(PlannedMetadataCommitV1),
    UntrackedStandalone,
}

/// Exact durable dirty variant observed before one resolution attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptDirtySourceV1 {
    Pending,
    PoisonedSettled,
    PoisonedUnsettled,
}

impl MetadataCommitReceiptDirtySourceV1 {
    #[must_use]
    pub fn matches_state(
        self,
        state: &MetadataCommitReceiptStateV1,
        planned: &PlannedMetadataCommitV1,
    ) -> bool {
        match (self, state) {
            (Self::Pending, MetadataCommitReceiptStateV1::Pending(durable))
            | (Self::PoisonedSettled, MetadataCommitReceiptStateV1::PoisonedSettled(durable))
            | (Self::PoisonedUnsettled, MetadataCommitReceiptStateV1::PoisonedUnsettled(durable)) => {
                durable == planned
            }
            _ => false,
        }
    }
}

/// Closed evidence basis for one engine-proven exact terminal resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitResolutionBasisV1 {
    ExactNextApplied,
    ExactPriorNotAppliedSettled,
}

/// Exact terminal resolution carried only inside an engine-minted command.
///
/// Its fields and constructors are private so an ordinary runtime-bundle
/// holder cannot manufacture `Applied` or `NotApplied` evidence. Receipt
/// backends inspect it through the read-only accessors after receiving a
/// nominal resolve command.
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataCommitResolutionV1;
///
/// let forged = MetadataCommitResolutionV1 {
///     source: todo!(),
///     basis: todo!(),
///     exact_next: None,
///     exact_prior: None,
///     purpose_evidence_digest: [1; 32],
/// };
/// ```
#[derive(PartialEq, Eq)]
pub struct MetadataCommitResolutionV1 {
    source: MetadataCommitReceiptDirtySourceV1,
    basis: MetadataCommitResolutionBasisV1,
    exact_next: Option<AcknowledgedMetadataFrontier>,
    exact_prior: Option<MetadataFrontierPointV1>,
    purpose_evidence_digest: [u8; 32],
}

impl MetadataCommitResolutionV1 {
    pub(super) const fn applied(
        _authority: &super::engine::MetadataCommitEngineMintAuthorityV1,
        source: MetadataCommitReceiptDirtySourceV1,
        exact_next: AcknowledgedMetadataFrontier,
        purpose_evidence_digest: [u8; 32],
    ) -> Self {
        Self {
            source,
            basis: MetadataCommitResolutionBasisV1::ExactNextApplied,
            exact_next: Some(exact_next),
            exact_prior: None,
            purpose_evidence_digest,
        }
    }

    pub(super) const fn not_applied_settled(
        _authority: &super::engine::MetadataCommitEngineMintAuthorityV1,
        exact_prior: MetadataFrontierPointV1,
        purpose_evidence_digest: [u8; 32],
    ) -> Self {
        Self {
            source: MetadataCommitReceiptDirtySourceV1::PoisonedSettled,
            basis: MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled,
            exact_next: None,
            exact_prior: Some(exact_prior),
            purpose_evidence_digest,
        }
    }

    #[must_use]
    pub const fn source(&self) -> MetadataCommitReceiptDirtySourceV1 {
        self.source
    }

    #[must_use]
    pub const fn basis(&self) -> MetadataCommitResolutionBasisV1 {
        self.basis
    }

    #[must_use]
    pub const fn applied_exact_next(&self) -> Option<AcknowledgedMetadataFrontier> {
        self.exact_next
    }

    #[must_use]
    pub const fn not_applied_exact_prior(&self) -> Option<MetadataFrontierPointV1> {
        self.exact_prior
    }

    #[must_use]
    pub const fn purpose_evidence_digest(&self) -> [u8; 32] {
        self.purpose_evidence_digest
    }
}

impl fmt::Debug for MetadataCommitResolutionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCommitResolutionV1")
            .field("source", &self.source)
            .field("basis", &self.basis)
            .field("frontier", &"<redacted>")
            .field("purpose_evidence", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// Redacted receipt-authority failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptErrorV1 {
    Poisoned,
    Unavailable,
    InvalidBinding,
}

impl fmt::Display for MetadataCommitReceiptErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Poisoned => formatter.write_str("metadata commit receipt is poisoned"),
            Self::Unavailable => formatter.write_str("metadata commit receipt is unavailable"),
            Self::InvalidBinding => {
                formatter.write_str("metadata commit receipt binding is invalid")
            }
        }
    }
}

impl std::error::Error for MetadataCommitReceiptErrorV1 {}

/// Closed outcome of attempting to durably install one pending commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptPersistErrorV1 {
    /// The receipt authority was unavailable and proves it made no change.
    UnavailableBeforeEffect,
    /// The binding or prior state was rejected and proves it made no change.
    InvalidBindingBeforeEffect,
    /// The receipt may already be pending and must be loaded during recovery.
    RecoveryRequired,
}

impl fmt::Display for MetadataCommitReceiptPersistErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnavailableBeforeEffect => {
                formatter.write_str("metadata commit receipt was unavailable before persistence")
            }
            Self::InvalidBindingBeforeEffect => formatter
                .write_str("metadata commit receipt binding was rejected before persistence"),
            Self::RecoveryRequired => {
                formatter.write_str("metadata commit receipt recovery is required")
            }
        }
    }
}

impl std::error::Error for MetadataCommitReceiptPersistErrorV1 {}

/// Definite reason a pending receipt mutation was not dispatched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptPersistNotDispatchedV1 {
    Unavailable,
    InvalidBinding,
}

/// Closed result available only after consuming a claimed persist command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptPersistBackendResultV1 {
    Persisted,
    NotDispatched(MetadataCommitReceiptPersistNotDispatchedV1),
    RecoveryRequired,
}

/// Definite reason a resolve or poison mutation was not dispatched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptMutationNotDispatchedV1 {
    Poisoned,
    Unavailable,
    InvalidBinding,
}

/// Closed result available only after consuming a claimed resolve or poison
/// command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptMutationBackendResultV1 {
    Completed,
    NotDispatched(MetadataCommitReceiptMutationNotDispatchedV1),
    OutcomeUnknown,
}

struct MetadataCommitReceiptPersistCommandCoreV1 {
    planned: PlannedMetadataCommitV1,
}

/// One exact live commit allocation whose `Pending` receipt was durably
/// confirmed by its matching persist outcome and witness.
///
/// This authority is non-Clone. Poison and resolve commands consume it, and
/// only an exact successful poison outcome can return the monotonically
/// updated source variant.
pub(super) struct MetadataCommitLiveResolutionOriginV1 {
    persist_core: Arc<MetadataCommitReceiptPersistCommandCoreV1>,
    source: MetadataCommitReceiptDirtySourceV1,
}

impl MetadataCommitLiveResolutionOriginV1 {
    pub(super) fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.persist_core.planned
    }

    pub(super) fn source(&self) -> MetadataCommitReceiptDirtySourceV1 {
        self.source
    }

    fn with_poisoned_source(self, source: MetadataCommitReceiptDirtySourceV1) -> Self {
        Self {
            persist_core: self.persist_core,
            source,
        }
    }
}

impl fmt::Debug for MetadataCommitLiveResolutionOriginV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCommitLiveResolutionOriginV1")
            .field("source", &self.source)
            .field("identity", &"<opaque>")
            .finish()
    }
}

pub(crate) struct MetadataCommitReceiptPersistWitnessV1 {
    core: Arc<MetadataCommitReceiptPersistCommandCoreV1>,
}

/// One engine-authorized pending-receipt mutation.
///
/// The command is private-construction and deliberately not cloneable. The
/// ultimate backend must claim it before durable access and return the outcome
/// produced by the claimed command's `complete` method. Forwarding wrappers
/// pass it through without claiming it.
///
/// ```compile_fail
/// use nokv_meta::workspace::{
///     MetadataCommitReceiptPersistCommandV1, PlannedMetadataCommitV1,
/// };
///
/// let planned: PlannedMetadataCommitV1 = todo!();
/// let forged = MetadataCommitReceiptPersistCommandV1 {
///     planned,
///     execution: todo!(),
/// };
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataCommitReceiptPersistCommandV1;
///
/// fn duplicate(command: MetadataCommitReceiptPersistCommandV1) {
///     let _copy = command.clone();
/// }
/// ```
///
/// Claiming consumes the only command value, so a second claim is impossible:
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataCommitReceiptPersistCommandV1;
///
/// fn claim_twice(command: MetadataCommitReceiptPersistCommandV1) {
///     let _claimed = command.claim_execution();
///     let _second = command.claim_execution();
/// }
/// ```
///
/// An unclaimed command has no success-completion method:
///
/// ```compile_fail
/// use nokv_meta::workspace::{
///     MetadataCommitReceiptPersistBackendResultV1,
///     MetadataCommitReceiptPersistCommandV1,
/// };
///
/// fn skip_claim(command: MetadataCommitReceiptPersistCommandV1) {
///     let _ = command.complete(MetadataCommitReceiptPersistBackendResultV1::Persisted);
/// }
/// ```
///
/// A read-only planned receipt cannot be supplied to the mutation SPI:
///
/// ```compile_fail
/// use nokv_meta::workspace::{
///     MetadataCommitReceiptStoreV1, PlannedMetadataCommitV1,
/// };
///
/// fn bypass(
///     receipt: &dyn MetadataCommitReceiptStoreV1,
///     planned: PlannedMetadataCommitV1,
/// ) {
///     let _ = receipt.persist_pending_commit_v1(planned);
/// }
/// ```
pub struct MetadataCommitReceiptPersistCommandV1 {
    core: Arc<MetadataCommitReceiptPersistCommandCoreV1>,
}

impl MetadataCommitReceiptPersistCommandV1 {
    pub(super) fn mint(
        _authority: &super::engine::MetadataCommitEngineMintAuthorityV1,
        planned: &PlannedMetadataCommitV1,
    ) -> (Self, MetadataCommitReceiptPersistWitnessV1) {
        let core = Arc::new(MetadataCommitReceiptPersistCommandCoreV1 {
            planned: planned.clone(),
        });
        (
            Self {
                core: Arc::clone(&core),
            },
            MetadataCommitReceiptPersistWitnessV1 { core },
        )
    }

    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn claim_execution(self) -> ClaimedMetadataCommitReceiptPersistCommandV1 {
        ClaimedMetadataCommitReceiptPersistCommandV1 { core: self.core }
    }

    #[must_use]
    pub fn reject_before_execution(
        self,
        reason: MetadataCommitReceiptPersistNotDispatchedV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        MetadataCommitReceiptPersistOutcomeV1 {
            core: self.core,
            status: MetadataCommitReceiptPersistOutcomeStatusV1::RejectedBeforeExecution(reason),
        }
    }
}

/// The unique claimed phase of one pending-receipt command.
pub struct ClaimedMetadataCommitReceiptPersistCommandV1 {
    core: Arc<MetadataCommitReceiptPersistCommandCoreV1>,
}

impl ClaimedMetadataCommitReceiptPersistCommandV1 {
    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn complete(
        self,
        result: MetadataCommitReceiptPersistBackendResultV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        MetadataCommitReceiptPersistOutcomeV1 {
            core: self.core,
            status: MetadataCommitReceiptPersistOutcomeStatusV1::Backend(result),
        }
    }
}

enum MetadataCommitReceiptPersistOutcomeStatusV1 {
    RejectedBeforeExecution(MetadataCommitReceiptPersistNotDispatchedV1),
    Backend(MetadataCommitReceiptPersistBackendResultV1),
}

/// Closed completion of one exact pending-receipt command.
///
/// Forwarders cannot supply an arbitrary result transform that upgrades
/// recovery-required state back to success:
///
/// ```compile_fail
/// use nokv_meta::workspace::{
///     MetadataCommitReceiptPersistBackendResultV1,
///     MetadataCommitReceiptPersistOutcomeV1,
/// };
///
/// fn upgrade(outcome: MetadataCommitReceiptPersistOutcomeV1) {
///     let _ = outcome.map_backend_result_for_forwarding(|_| {
///         MetadataCommitReceiptPersistBackendResultV1::Persisted
///     });
/// }
/// ```
pub struct MetadataCommitReceiptPersistOutcomeV1 {
    core: Arc<MetadataCommitReceiptPersistCommandCoreV1>,
    status: MetadataCommitReceiptPersistOutcomeStatusV1,
}

impl MetadataCommitReceiptPersistOutcomeV1 {
    /// Read the backend phase without gaining mutation authority.
    #[must_use]
    pub const fn backend_result_for_forwarding(
        &self,
    ) -> Option<MetadataCommitReceiptPersistBackendResultV1> {
        match self.status {
            MetadataCommitReceiptPersistOutcomeStatusV1::RejectedBeforeExecution(_) => None,
            MetadataCommitReceiptPersistOutcomeStatusV1::Backend(result) => Some(result),
        }
    }

    /// Monotonically downgrade a forwarded success after a failed post-check.
    /// Existing not-dispatched and recovery-required results are unchanged.
    #[must_use]
    pub fn downgrade_after_forwarding_failure(self) -> Self {
        Self {
            core: self.core,
            status: match self.status {
                MetadataCommitReceiptPersistOutcomeStatusV1::RejectedBeforeExecution(reason) => {
                    MetadataCommitReceiptPersistOutcomeStatusV1::RejectedBeforeExecution(reason)
                }
                MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                    MetadataCommitReceiptPersistBackendResultV1::Persisted,
                ) => MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                    MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired,
                ),
                MetadataCommitReceiptPersistOutcomeStatusV1::Backend(result) => {
                    MetadataCommitReceiptPersistOutcomeStatusV1::Backend(result)
                }
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn into_result_for(
        self,
        witness: MetadataCommitReceiptPersistWitnessV1,
    ) -> Result<(), MetadataCommitReceiptPersistErrorV1> {
        if !Arc::ptr_eq(&self.core, &witness.core) {
            Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired)
        } else {
            match self.status {
                MetadataCommitReceiptPersistOutcomeStatusV1::RejectedBeforeExecution(reason)
                | MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                    MetadataCommitReceiptPersistBackendResultV1::NotDispatched(reason),
                ) => Err(match reason {
                    MetadataCommitReceiptPersistNotDispatchedV1::Unavailable => {
                        MetadataCommitReceiptPersistErrorV1::UnavailableBeforeEffect
                    }
                    MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding => {
                        MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect
                    }
                }),
                MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                    MetadataCommitReceiptPersistBackendResultV1::Persisted,
                ) => Ok(()),
                MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                    MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired,
                ) => Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired),
            }
        }
    }

    pub(super) fn into_live_resolution_origin_for(
        self,
        witness: MetadataCommitReceiptPersistWitnessV1,
    ) -> Result<MetadataCommitLiveResolutionOriginV1, MetadataCommitReceiptPersistErrorV1> {
        if !Arc::ptr_eq(&self.core, &witness.core) {
            return Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired);
        }
        match self.status {
            MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                MetadataCommitReceiptPersistBackendResultV1::Persisted,
            ) => Ok(MetadataCommitLiveResolutionOriginV1 {
                persist_core: self.core,
                source: MetadataCommitReceiptDirtySourceV1::Pending,
            }),
            MetadataCommitReceiptPersistOutcomeStatusV1::RejectedBeforeExecution(reason)
            | MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                MetadataCommitReceiptPersistBackendResultV1::NotDispatched(reason),
            ) => Err(match reason {
                MetadataCommitReceiptPersistNotDispatchedV1::Unavailable => {
                    MetadataCommitReceiptPersistErrorV1::UnavailableBeforeEffect
                }
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding => {
                    MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect
                }
            }),
            MetadataCommitReceiptPersistOutcomeStatusV1::Backend(
                MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired,
            ) => Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired),
        }
    }
}

struct MetadataCommitReceiptResolveCommandCoreV1 {
    planned: PlannedMetadataCommitV1,
    resolution: MetadataCommitResolutionV1,
    _origin: MetadataCommitResolutionOriginV1,
}

enum MetadataCommitResolutionOriginV1 {
    Live {
        _origin: MetadataCommitLiveResolutionOriginV1,
    },
    Recovery {
        _origin: MetadataCommitRecoveryOpenAllocationV1,
    },
}

pub(crate) struct MetadataCommitReceiptResolveWitnessV1 {
    core: Arc<MetadataCommitReceiptResolveCommandCoreV1>,
}

/// One engine-authorized exact-resolution receipt mutation.
///
/// This command is neither externally constructible nor cloneable. The
/// resolution payload is frozen when the engine mints the command after
/// checking one provider-consistent evidence view.
pub struct MetadataCommitReceiptResolveCommandV1 {
    core: Arc<MetadataCommitReceiptResolveCommandCoreV1>,
}

impl MetadataCommitReceiptResolveCommandV1 {
    pub(super) fn mint_live(
        _authority: &super::engine::MetadataCommitEngineMintAuthorityV1,
        origin: MetadataCommitLiveResolutionOriginV1,
        resolution: MetadataCommitResolutionV1,
    ) -> Result<(Self, MetadataCommitReceiptResolveWitnessV1), MetadataCommitReceiptErrorV1> {
        let planned = origin.planned().clone();
        if origin.source() != resolution.source {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        validate_resolution(&planned, &resolution)?;
        Ok(Self::mint_with_origin(
            planned,
            resolution,
            MetadataCommitResolutionOriginV1::Live { _origin: origin },
        ))
    }

    pub(super) fn mint_recovery(
        _authority: &super::engine::MetadataCommitEngineMintAuthorityV1,
        planned: &PlannedMetadataCommitV1,
        origin: MetadataCommitRecoveryOpenAllocationV1,
        resolution: MetadataCommitResolutionV1,
    ) -> Result<(Self, MetadataCommitReceiptResolveWitnessV1), MetadataCommitReceiptErrorV1> {
        if !origin.matches(planned, resolution.source) {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        validate_resolution(planned, &resolution)?;
        Ok(Self::mint_with_origin(
            planned.clone(),
            resolution,
            MetadataCommitResolutionOriginV1::Recovery { _origin: origin },
        ))
    }

    fn mint_with_origin(
        planned: PlannedMetadataCommitV1,
        resolution: MetadataCommitResolutionV1,
        origin: MetadataCommitResolutionOriginV1,
    ) -> (Self, MetadataCommitReceiptResolveWitnessV1) {
        let core = Arc::new(MetadataCommitReceiptResolveCommandCoreV1 {
            planned,
            resolution,
            _origin: origin,
        });
        (
            Self {
                core: Arc::clone(&core),
            },
            MetadataCommitReceiptResolveWitnessV1 { core },
        )
    }

    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn resolution(&self) -> &MetadataCommitResolutionV1 {
        &self.core.resolution
    }

    #[must_use]
    pub fn claim_execution(self) -> ClaimedMetadataCommitReceiptResolveCommandV1 {
        ClaimedMetadataCommitReceiptResolveCommandV1 { core: self.core }
    }

    #[must_use]
    pub fn reject_before_execution(
        self,
        reason: MetadataCommitReceiptMutationNotDispatchedV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        MetadataCommitReceiptResolveOutcomeV1 {
            core: self.core,
            status: MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason),
        }
    }
}

fn validate_resolution(
    planned: &PlannedMetadataCommitV1,
    resolution: &MetadataCommitResolutionV1,
) -> Result<(), MetadataCommitReceiptErrorV1> {
    if resolution
        .purpose_evidence_digest
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
    }
    match resolution.basis {
        MetadataCommitResolutionBasisV1::ExactNextApplied
            if resolution.exact_next == Some(planned.exact_next())
                && resolution.exact_prior.is_none() =>
        {
            Ok(())
        }
        MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled
            if resolution.source == MetadataCommitReceiptDirtySourceV1::PoisonedSettled
                && resolution.exact_next.is_none()
                && resolution.exact_prior == Some(planned.prior()) =>
        {
            Ok(())
        }
        _ => Err(MetadataCommitReceiptErrorV1::InvalidBinding),
    }
}

/// The unique claimed phase of one exact-resolution command.
pub struct ClaimedMetadataCommitReceiptResolveCommandV1 {
    core: Arc<MetadataCommitReceiptResolveCommandCoreV1>,
}

impl ClaimedMetadataCommitReceiptResolveCommandV1 {
    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn resolution(&self) -> &MetadataCommitResolutionV1 {
        &self.core.resolution
    }

    #[must_use]
    pub fn complete(
        self,
        result: MetadataCommitReceiptMutationBackendResultV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        MetadataCommitReceiptResolveOutcomeV1 {
            core: self.core,
            status: MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result),
        }
    }
}

enum MetadataCommitReceiptMutationOutcomeStatusV1 {
    RejectedBeforeExecution(MetadataCommitReceiptMutationNotDispatchedV1),
    Backend(MetadataCommitReceiptMutationBackendResultV1),
}

/// Closed completion of one exact resolution command.
pub struct MetadataCommitReceiptResolveOutcomeV1 {
    core: Arc<MetadataCommitReceiptResolveCommandCoreV1>,
    status: MetadataCommitReceiptMutationOutcomeStatusV1,
}

impl MetadataCommitReceiptResolveOutcomeV1 {
    /// Transform only the closed result while preserving command identity.
    #[must_use]
    pub const fn backend_result_for_forwarding(
        &self,
    ) -> Option<MetadataCommitReceiptMutationBackendResultV1> {
        match self.status {
            MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(_) => None,
            MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result) => Some(result),
        }
    }

    /// Monotonically downgrade a forwarded success after a failed post-check.
    #[must_use]
    pub fn downgrade_after_forwarding_failure(self) -> Self {
        Self {
            core: self.core,
            status: match self.status {
                MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason) => {
                    MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason)
                }
                MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                    MetadataCommitReceiptMutationBackendResultV1::Completed,
                ) => MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                    MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown,
                ),
                MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result) => {
                    MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result)
                }
            },
        }
    }

    pub(crate) fn into_result_for(
        self,
        witness: MetadataCommitReceiptResolveWitnessV1,
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        if !Arc::ptr_eq(&self.core, &witness.core) {
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        } else {
            mutation_outcome_result(self.status)
        }
    }
}

/// Closed durability meaning of one engine-authorized poison transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataCommitReceiptPoisonReasonV1 {
    SettledCommitOutcome,
    UnsettledCommitOutcome,
}

struct MetadataCommitReceiptPoisonCommandCoreV1 {
    planned: PlannedMetadataCommitV1,
    reason: MetadataCommitReceiptPoisonReasonV1,
    live_origin: MetadataCommitLiveResolutionOriginV1,
}

pub(crate) struct MetadataCommitReceiptPoisonWitnessV1 {
    core: Arc<MetadataCommitReceiptPoisonCommandCoreV1>,
}

/// One engine-authorized poison receipt mutation.
///
/// This command is private-construction and deliberately not cloneable. Its
/// closed reason prevents a backend from collapsing an unsettled native
/// commit into a receipt whose exact-prior state may later become terminal.
pub struct MetadataCommitReceiptPoisonCommandV1 {
    core: Arc<MetadataCommitReceiptPoisonCommandCoreV1>,
}

impl MetadataCommitReceiptPoisonCommandV1 {
    pub(super) fn mint(
        _authority: &super::engine::MetadataCommitEngineMintAuthorityV1,
        live_origin: MetadataCommitLiveResolutionOriginV1,
        reason: MetadataCommitReceiptPoisonReasonV1,
    ) -> Result<(Self, MetadataCommitReceiptPoisonWitnessV1), MetadataCommitReceiptErrorV1> {
        if live_origin.source() != MetadataCommitReceiptDirtySourceV1::Pending {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        let core = Arc::new(MetadataCommitReceiptPoisonCommandCoreV1 {
            planned: live_origin.planned().clone(),
            reason,
            live_origin,
        });
        Ok((
            Self {
                core: Arc::clone(&core),
            },
            MetadataCommitReceiptPoisonWitnessV1 { core },
        ))
    }

    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn reason(&self) -> MetadataCommitReceiptPoisonReasonV1 {
        self.core.reason
    }

    #[must_use]
    pub fn claim_execution(self) -> ClaimedMetadataCommitReceiptPoisonCommandV1 {
        ClaimedMetadataCommitReceiptPoisonCommandV1 { core: self.core }
    }

    #[must_use]
    pub fn reject_before_execution(
        self,
        reason: MetadataCommitReceiptMutationNotDispatchedV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        MetadataCommitReceiptPoisonOutcomeV1 {
            core: self.core,
            status: MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason),
        }
    }
}

/// The unique claimed phase of one poison command.
pub struct ClaimedMetadataCommitReceiptPoisonCommandV1 {
    core: Arc<MetadataCommitReceiptPoisonCommandCoreV1>,
}

impl ClaimedMetadataCommitReceiptPoisonCommandV1 {
    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn reason(&self) -> MetadataCommitReceiptPoisonReasonV1 {
        self.core.reason
    }

    #[must_use]
    pub fn complete(
        self,
        result: MetadataCommitReceiptMutationBackendResultV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        MetadataCommitReceiptPoisonOutcomeV1 {
            core: self.core,
            status: MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result),
        }
    }
}

/// Closed completion of one exact poison command.
pub struct MetadataCommitReceiptPoisonOutcomeV1 {
    core: Arc<MetadataCommitReceiptPoisonCommandCoreV1>,
    status: MetadataCommitReceiptMutationOutcomeStatusV1,
}

impl MetadataCommitReceiptPoisonOutcomeV1 {
    /// Transform only the closed result while preserving command identity.
    #[must_use]
    pub const fn backend_result_for_forwarding(
        &self,
    ) -> Option<MetadataCommitReceiptMutationBackendResultV1> {
        match self.status {
            MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(_) => None,
            MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result) => Some(result),
        }
    }

    /// Monotonically downgrade a forwarded success after a failed post-check.
    #[must_use]
    pub fn downgrade_after_forwarding_failure(self) -> Self {
        Self {
            core: self.core,
            status: match self.status {
                MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason) => {
                    MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason)
                }
                MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                    MetadataCommitReceiptMutationBackendResultV1::Completed,
                ) => MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                    MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown,
                ),
                MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result) => {
                    MetadataCommitReceiptMutationOutcomeStatusV1::Backend(result)
                }
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn into_result_for(
        self,
        witness: MetadataCommitReceiptPoisonWitnessV1,
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        if !Arc::ptr_eq(&self.core, &witness.core) {
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        } else {
            mutation_outcome_result(self.status)
        }
    }

    pub(super) fn into_live_resolution_origin_for(
        self,
        witness: MetadataCommitReceiptPoisonWitnessV1,
    ) -> Result<MetadataCommitLiveResolutionOriginV1, MetadataCommitReceiptErrorV1> {
        if !Arc::ptr_eq(&self.core, &witness.core) {
            return Err(MetadataCommitReceiptErrorV1::Unavailable);
        }
        drop(witness);
        match self.status {
            MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                MetadataCommitReceiptMutationBackendResultV1::Completed,
            ) => {
                let core = Arc::try_unwrap(self.core)
                    .map_err(|_| MetadataCommitReceiptErrorV1::Unavailable)?;
                let source = match core.reason {
                    MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome => {
                        MetadataCommitReceiptDirtySourceV1::PoisonedSettled
                    }
                    MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome => {
                        MetadataCommitReceiptDirtySourceV1::PoisonedUnsettled
                    }
                };
                Ok(core.live_origin.with_poisoned_source(source))
            }
            MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason)
            | MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                MetadataCommitReceiptMutationBackendResultV1::NotDispatched(reason),
            ) => Err(match reason {
                MetadataCommitReceiptMutationNotDispatchedV1::Poisoned => {
                    MetadataCommitReceiptErrorV1::Poisoned
                }
                MetadataCommitReceiptMutationNotDispatchedV1::Unavailable => {
                    MetadataCommitReceiptErrorV1::Unavailable
                }
                MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding => {
                    MetadataCommitReceiptErrorV1::InvalidBinding
                }
            }),
            MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
                MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown,
            ) => Err(MetadataCommitReceiptErrorV1::Unavailable),
        }
    }
}

fn mutation_outcome_result(
    status: MetadataCommitReceiptMutationOutcomeStatusV1,
) -> Result<(), MetadataCommitReceiptErrorV1> {
    match status {
        MetadataCommitReceiptMutationOutcomeStatusV1::RejectedBeforeExecution(reason)
        | MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
            MetadataCommitReceiptMutationBackendResultV1::NotDispatched(reason),
        ) => Err(match reason {
            MetadataCommitReceiptMutationNotDispatchedV1::Poisoned => {
                MetadataCommitReceiptErrorV1::Poisoned
            }
            MetadataCommitReceiptMutationNotDispatchedV1::Unavailable => {
                MetadataCommitReceiptErrorV1::Unavailable
            }
            MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding => {
                MetadataCommitReceiptErrorV1::InvalidBinding
            }
        }),
        MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
            MetadataCommitReceiptMutationBackendResultV1::Completed,
        ) => Ok(()),
        MetadataCommitReceiptMutationOutcomeStatusV1::Backend(
            MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown,
        ) => Err(MetadataCommitReceiptErrorV1::Unavailable),
    }
}

/// Durable receipt half of one concrete metadata runtime bundle.
///
/// `load_commit_receipt_v1` is recovery-only and must remain available after
/// either poison state. `persist_pending_commit_v1` must durably replace
/// the exact matching `Clean(prior)` state before returning success.
/// `UnavailableBeforeEffect` and `InvalidBindingBeforeEffect` prove no receipt
/// state was changed. `RecoveryRequired` is the only valid error after the
/// implementation might have durably installed `Pending`; the current runtime
/// allocation must fail-stop and a new allocation must load and reconcile the
/// exact plan before serving.
/// A reopened runtime must not interpret exact `prior` as terminal for a
/// durable `Pending`: the receipt alone cannot prove whether an older runtime
/// dispatched the backend commit. Making `prior` terminal requires a future
/// durable pre-dispatch marker or a backend fence that excludes the old
/// dispatch; neither is part of this SPI.
/// `resolve_pending_commit_v1` must atomically resolve the exact matching plan
/// from `Pending`, `PoisonedSettled`, or `PoisonedUnsettled`. Exact prior may
/// terminally resolve only `PoisonedSettled`; elapsed time or process restart
/// never upgrades `PoisonedUnsettled` into a settlement barrier. Poisoning
/// restricts serving but never destroys exact recovery authority.
pub trait MetadataCommitReceiptStoreV1: Send + Sync {
    fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1;

    fn frozen_runtime_bundle_digest_v1(&self) -> [u8; 32];

    fn load_commit_receipt_v1(
        &self,
        store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1>;

    fn persist_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptPersistCommandV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1;

    fn resolve_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptResolveCommandV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1;

    fn poison_commit_receipt_v1(
        &self,
        command: MetadataCommitReceiptPoisonCommandV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1;
}

/// One concrete allocation that owns both provider open and durable receipts.
///
/// Engine constructors accept one `Arc<B>` where `B` implements this trait;
/// there is no API that accepts a provider factory and receipt store
/// separately. The object must bind its factory and receipt authority to the
/// same immutable provider-store identity in this allocation; forwarding the
/// two halves to independently replaceable objects violates this contract.
/// Its frozen digest must be derived from that actual, secret-free store
/// binding and must not be an arbitrary caller-supplied token.
pub trait MetadataRuntimeCommitBundleV1:
    MetadataCommitRecoveryFenceFactoryV1 + MetadataCommitReceiptStoreV1
{
}

impl<T> MetadataRuntimeCommitBundleV1 for T where
    T: MetadataCommitRecoveryFenceFactoryV1 + MetadataCommitReceiptStoreV1
{
}

pub(crate) fn purpose_evidence_digest(
    planned: &PlannedMetadataCommitV1,
    applied: bool,
    evidence_bytes: &[u8],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PURPOSE_EVIDENCE_DOMAIN);
    hasher.update(planned.canonical_digest);
    hasher.update([u8::from(applied)]);
    hasher.update((evidence_bytes.len() as u64).to_be_bytes());
    hasher.update(evidence_bytes);
    hasher.finalize().into()
}

pub(crate) fn digest_authority_marker(encoded: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.authority-marker.v1\0");
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    hasher.finalize().into()
}

pub(crate) fn digest_source_receipt(receipt: &SourceQuiesceReceipt) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.source-quiesce-receipt.v1\0");
    hash_source_receipt(&mut hasher, receipt);
    hasher.finalize().into()
}

#[cfg(test)]
pub(crate) fn digest_target_token(token: &TargetActivationToken) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.target-activation-token.v1\0");
    hash_target_token(&mut hasher, token);
    hasher.finalize().into()
}

pub(crate) fn digest_target_binding(binding: &MetadataMigrationTargetBinding) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.migration-target-binding.v1\0");
    hash_target_binding(&mut hasher, binding);
    hasher.finalize().into()
}

fn validate_delta(
    purpose: &MetadataCommitPurposeV1,
    prior: MetadataFrontierPointV1,
    next: AcknowledgedMetadataFrontier,
) -> Result<(), MetadataCommitReceiptErrorV1> {
    if next.chain_digest.iter().all(|byte| *byte == 0) {
        return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
    }
    let delta = purpose.expected_delta();
    match (purpose, prior) {
        (MetadataCommitPurposeV1::Genesis { .. }, MetadataFrontierPointV1::Absent) => {
            if next.write_sequence != delta.write_sequence
                || next.commit_version.get() != delta.commit_version
                || next.recovery_lsn != delta.recovery_lsn
            {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
        }
        (MetadataCommitPurposeV1::Genesis { .. }, MetadataFrontierPointV1::Exact(_))
        | (_, MetadataFrontierPointV1::Absent) => {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        (_, MetadataFrontierPointV1::Exact(prior)) => {
            if prior.chain_digest.iter().all(|byte| *byte == 0) {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
            if next.write_sequence
                != prior
                    .write_sequence
                    .checked_add(delta.write_sequence)
                    .ok_or(MetadataCommitReceiptErrorV1::InvalidBinding)?
                || next.commit_version.get()
                    != prior
                        .commit_version
                        .get()
                        .checked_add(delta.commit_version)
                        .ok_or(MetadataCommitReceiptErrorV1::InvalidBinding)?
                || next.recovery_lsn
                    != prior
                        .recovery_lsn
                        .checked_add(delta.recovery_lsn)
                        .ok_or(MetadataCommitReceiptErrorV1::InvalidBinding)?
            {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
            if delta.recovery_lsn == 0 && next.chain_digest != prior.chain_digest {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
            if delta.recovery_lsn != 0 && next.chain_digest == prior.chain_digest {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
        }
    }
    Ok(())
}

fn plan_digest(
    store_identity: MetadataStoreIdentity,
    frozen_bundle_digest: [u8; 32],
    purpose: &MetadataCommitPurposeV1,
    prior: MetadataFrontierPointV1,
    exact_next: AcknowledgedMetadataFrontier,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(COMMIT_PLAN_DOMAIN);
    let identity = encode_store_identity(store_identity);
    hasher.update((identity.len() as u64).to_be_bytes());
    hasher.update(identity);
    hasher.update(frozen_bundle_digest);
    purpose.hash_into(&mut hasher);
    hash_frontier_point(&mut hasher, prior);
    hash_frontier(&mut hasher, exact_next);
    hasher.finalize().into()
}

fn hash_frontier_point(hasher: &mut Sha256, point: MetadataFrontierPointV1) {
    match point {
        MetadataFrontierPointV1::Absent => hasher.update([0]),
        MetadataFrontierPointV1::Exact(frontier) => {
            hasher.update([1]);
            hash_frontier(hasher, frontier);
        }
    }
}

fn hash_frontier(hasher: &mut Sha256, frontier: AcknowledgedMetadataFrontier) {
    hasher.update(frontier.write_sequence.to_be_bytes());
    hasher.update(frontier.commit_version.get().to_be_bytes());
    hasher.update(frontier.recovery_lsn.to_be_bytes());
    hasher.update(frontier.chain_digest);
}

fn hash_authority_action(hasher: &mut Sha256, action: MetadataAuthorityCommitActionV1) {
    match action {
        MetadataAuthorityCommitActionV1::Quiesce {
            migration_id,
            owner_epoch,
        } => {
            hasher.update([1]);
            hasher.update(migration_id.as_bytes());
            hasher.update(owner_epoch.get().to_be_bytes());
        }
        MetadataAuthorityCommitActionV1::FenceQuiescedSource {
            migration_id,
            source_receipt_digest,
        } => {
            hasher.update([2]);
            hasher.update(migration_id.as_bytes());
            hasher.update(source_receipt_digest);
        }
        MetadataAuthorityCommitActionV1::ActivateTarget {
            migration_id,
            activation_token_digest,
        } => {
            hasher.update([3]);
            hasher.update(migration_id.as_bytes());
            hasher.update(activation_token_digest);
        }
        MetadataAuthorityCommitActionV1::FenceTarget {
            migration_id,
            target_binding_digest,
        } => {
            hasher.update([4]);
            hasher.update(migration_id.as_bytes());
            hasher.update(target_binding_digest);
        }
    }
}

fn hash_source_receipt(hasher: &mut Sha256, receipt: &SourceQuiesceReceipt) {
    hasher.update(receipt.logical_shard_id.as_bytes());
    hasher.update(receipt.migration_id.as_bytes());
    hasher.update(receipt.source_authority_id.as_bytes());
    hasher.update(receipt.source_authority_generation.get().to_be_bytes());
    hasher.update(receipt.owner_epoch.get().to_be_bytes());
    hash_recovery_frontier(hasher, &receipt.frontier);
    hasher.update(receipt.contract_digest.as_bytes());
}

#[cfg(test)]
fn hash_target_token(hasher: &mut Sha256, token: &TargetActivationToken) {
    hasher.update(token.logical_shard_id.as_bytes());
    hasher.update(token.migration_id.as_bytes());
    hasher.update(token.source_authority_id.as_bytes());
    hasher.update(token.source_authority_generation.get().to_be_bytes());
    hasher.update(token.target_authority_id.as_bytes());
    hasher.update(token.target_authority_generation.get().to_be_bytes());
    hash_recovery_frontier(hasher, &token.frontier);
    hasher.update(token.contract_digest.as_bytes());
    hasher.update(token.source_receipt_digest);
}

fn hash_target_binding(hasher: &mut Sha256, binding: &MetadataMigrationTargetBinding) {
    hasher.update(binding.logical_shard_id.as_bytes());
    hasher.update(binding.migration_id.as_bytes());
    hasher.update(binding.source_authority_id.as_bytes());
    hasher.update(binding.source_authority_generation.get().to_be_bytes());
    hasher.update(binding.target_authority_id.as_bytes());
    hasher.update(binding.target_authority_generation.get().to_be_bytes());
    hasher.update(binding.contract_digest.as_bytes());
}

fn hash_recovery_frontier(hasher: &mut Sha256, frontier: &nokv_types::MetadataRecoveryFrontier) {
    hasher.update(frontier.recovery_lsn.to_be_bytes());
    hasher.update(frontier.chain_digest);
    hasher.update(frontier.commit_version.get().to_be_bytes());
    hasher.update(frontier.state_digest);
}

#[cfg(test)]
mod tests {
    use nokv_types::{
        CommitVersion, ConsistencyDomainId, LogicalShardId, MetadataAuthorityGeneration,
        MetadataAuthorityId,
    };

    use super::*;
    use crate::workspace::workspace_metadata_contract_digest;

    fn identity() -> MetadataStoreIdentity {
        MetadataStoreIdentity {
            logical_shard_id: LogicalShardId::from_bytes([1; 16]),
            authority_id: MetadataAuthorityId::from_bytes([2; 16]),
            authority_generation: MetadataAuthorityGeneration::new(3).unwrap(),
            consistency_domain_id: ConsistencyDomainId::from_bytes([4; 16]),
            profile_fingerprint: [5; 32],
            contract_digest: workspace_metadata_contract_digest(),
        }
    }

    fn frontier(write: u64, commit: u64, recovery: u64, chain: u8) -> AcknowledgedMetadataFrontier {
        AcknowledgedMetadataFrontier {
            write_sequence: write,
            commit_version: CommitVersion::new(commit).unwrap(),
            recovery_lsn: recovery,
            chain_digest: [chain; 32],
        }
    }

    fn mint_authority() -> super::super::engine::MetadataCommitEngineMintAuthorityV1 {
        super::super::engine::MetadataCommitEngineMintAuthorityV1::for_test()
    }

    fn persisted_origin(planned: &PlannedMetadataCommitV1) -> MetadataCommitLiveResolutionOriginV1 {
        let authority = mint_authority();
        let (command, witness) = MetadataCommitReceiptPersistCommandV1::mint(&authority, planned);
        command
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::Persisted)
            .into_live_resolution_origin_for(witness)
            .unwrap()
    }

    fn poisoned_origin(
        planned: &PlannedMetadataCommitV1,
        reason: MetadataCommitReceiptPoisonReasonV1,
    ) -> MetadataCommitLiveResolutionOriginV1 {
        let authority = mint_authority();
        let origin = persisted_origin(planned);
        let (command, witness) =
            MetadataCommitReceiptPoisonCommandV1::mint(&authority, origin, reason).unwrap();
        command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed)
            .into_live_resolution_origin_for(witness)
            .unwrap()
    }

    #[test]
    fn all_four_production_deltas_are_exact_and_plan_digest_binds_bundle() {
        let command_purpose = MetadataCommitPurposeV1::MetadataCommand {
            class: MetadataCommandCommitClassV1::Domain,
            root_id: RootId::from_bytes([7; 16]),
            request_id: RequestId::from_bytes([8; 16]),
            command_digest: CommandDigest::from_bytes([9; 32]),
            lease_deadline_ms: None,
        };
        let prior = MetadataFrontierPointV1::Exact(frontier(10, 11, 12, 13));
        let command_next = frontier(11, 12, 13, 14);
        let first = PlannedMetadataCommitV1::plan_exact(
            identity(),
            [15; 32],
            command_purpose.clone(),
            prior,
            command_next,
        )
        .unwrap();
        let second = PlannedMetadataCommitV1::plan_exact(
            identity(),
            [16; 32],
            command_purpose,
            prior,
            command_next,
        )
        .unwrap();
        assert_ne!(first.canonical_digest(), second.canonical_digest());

        for purpose in [
            MetadataCommitPurposeV1::AdvanceOwnerEpoch {
                expected: Some(OwnerEpoch::new(4).unwrap()),
                next: OwnerEpoch::new(5).unwrap(),
            },
            MetadataCommitPurposeV1::ObserveLeaseClock {
                root_id: RootId::from_bytes([17; 16]),
                placement_generation: PlacementGeneration::new(6).unwrap(),
                owner_epoch: OwnerEpoch::new(5).unwrap(),
                observed_ms: 99,
            },
        ] {
            PlannedMetadataCommitV1::plan_exact(
                identity(),
                [15; 32],
                purpose,
                prior,
                frontier(11, 11, 13, 14),
            )
            .unwrap();
        }

        PlannedMetadataCommitV1::plan_exact(
            identity(),
            [15; 32],
            MetadataCommitPurposeV1::Authority {
                action: MetadataAuthorityCommitActionV1::Quiesce {
                    migration_id: OperationId::from_bytes([18; 16]),
                    owner_epoch: OwnerEpoch::new(5).unwrap(),
                },
                prior_marker_digest: [19; 32],
                next_marker_digest: [20; 32],
            },
            prior,
            frontier(11, 11, 12, 13),
        )
        .unwrap();

        assert_eq!(
            PlannedMetadataCommitV1::plan_exact(
                identity(),
                [15; 32],
                MetadataCommitPurposeV1::AdvanceOwnerEpoch {
                    expected: None,
                    next: OwnerEpoch::new(1).unwrap(),
                },
                prior,
                command_next,
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
    }

    #[test]
    fn durable_reconstruction_recomputes_digest_and_rejects_tampered_fields() {
        let purpose = MetadataCommitPurposeV1::MetadataCommand {
            class: MetadataCommandCommitClassV1::Domain,
            root_id: RootId::from_bytes([21; 16]),
            request_id: RequestId::from_bytes([22; 16]),
            command_digest: CommandDigest::from_bytes([23; 32]),
            lease_deadline_ms: Some(24),
        };
        let prior = MetadataFrontierPointV1::Exact(frontier(30, 31, 32, 33));
        let next = frontier(31, 32, 33, 34);
        let planned =
            PlannedMetadataCommitV1::plan_exact(identity(), [35; 32], purpose.clone(), prior, next)
                .unwrap();
        assert_eq!(
            PlannedMetadataCommitV1::from_durable_parts_v1(
                identity(),
                [35; 32],
                purpose.clone(),
                prior,
                next,
                planned.canonical_digest(),
            )
            .unwrap(),
            planned
        );

        let mut tampered_digest = planned.canonical_digest();
        tampered_digest[0] ^= 1;
        assert_eq!(
            PlannedMetadataCommitV1::from_durable_parts_v1(
                identity(),
                [35; 32],
                purpose.clone(),
                prior,
                next,
                tampered_digest,
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
        assert_eq!(
            PlannedMetadataCommitV1::from_durable_parts_v1(
                identity(),
                [35; 32],
                MetadataCommitPurposeV1::MetadataCommand {
                    class: MetadataCommandCommitClassV1::Domain,
                    root_id: RootId::from_bytes([21; 16]),
                    request_id: RequestId::from_bytes([22; 16]),
                    command_digest: CommandDigest::from_bytes([36; 32]),
                    lease_deadline_ms: Some(24),
                },
                prior,
                next,
                planned.canonical_digest(),
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );

        let mut invalid_identity = identity();
        invalid_identity.profile_fingerprint = [0; 32];
        assert_eq!(
            PlannedMetadataCommitV1::from_durable_parts_v1(
                invalid_identity,
                [35; 32],
                purpose.clone(),
                prior,
                next,
                planned.canonical_digest(),
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
        assert_eq!(
            PlannedMetadataCommitV1::from_durable_parts_v1(
                identity(),
                [0; 32],
                purpose,
                prior,
                next,
                planned.canonical_digest(),
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
    }

    #[test]
    fn genesis_requires_absent_to_canonical_zero_user_frontier() {
        let purpose = MetadataCommitPurposeV1::Genesis {
            authority_marker_digest: [7; 32],
        };
        let genesis = frontier(0, 1, 0, 8);
        PlannedMetadataCommitV1::plan_exact(
            identity(),
            [9; 32],
            purpose.clone(),
            MetadataFrontierPointV1::Absent,
            genesis,
        )
        .unwrap();
        assert_eq!(
            PlannedMetadataCommitV1::plan_exact(
                identity(),
                [9; 32],
                purpose,
                MetadataFrontierPointV1::Exact(genesis),
                genesis,
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
    }

    #[test]
    fn forwarding_outcomes_can_only_downgrade_after_backend_execution() {
        let prior = MetadataFrontierPointV1::Exact(frontier(10, 11, 12, 13));
        let next = frontier(11, 12, 13, 14);
        let planned = PlannedMetadataCommitV1::plan_exact(
            identity(),
            [15; 32],
            MetadataCommitPurposeV1::MetadataCommand {
                class: MetadataCommandCommitClassV1::Domain,
                root_id: RootId::from_bytes([7; 16]),
                request_id: RequestId::from_bytes([8; 16]),
                command_digest: CommandDigest::from_bytes([9; 32]),
                lease_deadline_ms: None,
            },
            prior,
            next,
        )
        .unwrap();

        let authority = mint_authority();
        let (persist_command, persist_witness) =
            MetadataCommitReceiptPersistCommandV1::mint(&authority, &planned);
        let persisted = persist_command
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::Persisted);
        assert_eq!(
            persisted.backend_result_for_forwarding(),
            Some(MetadataCommitReceiptPersistBackendResultV1::Persisted)
        );
        let downgraded = persisted.downgrade_after_forwarding_failure();
        assert_eq!(
            downgraded.backend_result_for_forwarding(),
            Some(MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired)
        );
        assert_eq!(
            downgraded.into_result_for(persist_witness),
            Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired)
        );

        let (recovery_command, _) =
            MetadataCommitReceiptPersistCommandV1::mint(&authority, &planned);
        let recovery_required = recovery_command
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired)
            .downgrade_after_forwarding_failure();
        assert_eq!(
            recovery_required.backend_result_for_forwarding(),
            Some(MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired)
        );

        let (not_dispatched_command, not_dispatched_witness) =
            MetadataCommitReceiptPersistCommandV1::mint(&authority, &planned);
        let not_dispatched = not_dispatched_command
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
            ))
            .downgrade_after_forwarding_failure();
        assert_eq!(
            not_dispatched.backend_result_for_forwarding(),
            Some(MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
            ))
        );
        assert_eq!(
            not_dispatched.into_result_for(not_dispatched_witness),
            Err(MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect)
        );

        let resolve_origin = persisted_origin(&planned);
        let (resolve_command, resolve_witness) = MetadataCommitReceiptResolveCommandV1::mint_live(
            &authority,
            resolve_origin,
            MetadataCommitResolutionV1::applied(
                &authority,
                MetadataCommitReceiptDirtySourceV1::Pending,
                next,
                [16; 32],
            ),
        )
        .unwrap();
        let resolved = resolve_command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed)
            .downgrade_after_forwarding_failure();
        assert_eq!(
            resolved.backend_result_for_forwarding(),
            Some(MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown)
        );
        assert_eq!(
            resolved.into_result_for(resolve_witness),
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        );

        let (poison_command, poison_witness) = MetadataCommitReceiptPoisonCommandV1::mint(
            &authority,
            persisted_origin(&planned),
            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
        )
        .unwrap();
        let rejected_poison = poison_command
            .reject_before_execution(MetadataCommitReceiptMutationNotDispatchedV1::Poisoned)
            .downgrade_after_forwarding_failure();
        assert_eq!(rejected_poison.backend_result_for_forwarding(), None);
        assert_eq!(
            rejected_poison.into_result_for(poison_witness),
            Err(MetadataCommitReceiptErrorV1::Poisoned)
        );
    }

    #[test]
    fn outcomes_are_bound_to_one_minted_command_allocation_and_semantics() {
        let prior = MetadataFrontierPointV1::Exact(frontier(10, 11, 12, 13));
        let next = frontier(11, 12, 13, 14);
        let planned = PlannedMetadataCommitV1::plan_exact(
            identity(),
            [15; 32],
            MetadataCommitPurposeV1::MetadataCommand {
                class: MetadataCommandCommitClassV1::Domain,
                root_id: RootId::from_bytes([7; 16]),
                request_id: RequestId::from_bytes([8; 16]),
                command_digest: CommandDigest::from_bytes([9; 32]),
                lease_deadline_ms: None,
            },
            prior,
            next,
        )
        .unwrap();

        let authority = mint_authority();
        let (old_persist, _) = MetadataCommitReceiptPersistCommandV1::mint(&authority, &planned);
        let old_persist_outcome = old_persist
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::Persisted);
        let (_, new_persist_witness) =
            MetadataCommitReceiptPersistCommandV1::mint(&authority, &planned);
        assert_eq!(
            old_persist_outcome.into_result_for(new_persist_witness),
            Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired)
        );

        let source_swap = MetadataCommitReceiptResolveCommandV1::mint_live(
            &authority,
            persisted_origin(&planned),
            MetadataCommitResolutionV1::applied(
                &authority,
                MetadataCommitReceiptDirtySourceV1::PoisonedSettled,
                next,
                [16; 32],
            ),
        );
        assert!(matches!(
            source_swap,
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        ));

        let foreign_plan = PlannedMetadataCommitV1::plan_exact(
            identity(),
            [15; 32],
            MetadataCommitPurposeV1::MetadataCommand {
                class: MetadataCommandCommitClassV1::Domain,
                root_id: RootId::from_bytes([7; 16]),
                request_id: RequestId::from_bytes([18; 16]),
                command_digest: CommandDigest::from_bytes([19; 32]),
                lease_deadline_ms: None,
            },
            prior,
            frontier(11, 12, 13, 15),
        )
        .unwrap();
        let plan_swap = MetadataCommitReceiptResolveCommandV1::mint_live(
            &authority,
            persisted_origin(&foreign_plan),
            MetadataCommitResolutionV1::applied(
                &authority,
                MetadataCommitReceiptDirtySourceV1::Pending,
                next,
                [16; 32],
            ),
        );
        assert!(matches!(
            plan_swap,
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        ));

        let (applied_command, _) = MetadataCommitReceiptResolveCommandV1::mint_live(
            &authority,
            persisted_origin(&planned),
            MetadataCommitResolutionV1::applied(
                &authority,
                MetadataCommitReceiptDirtySourceV1::Pending,
                next,
                [16; 32],
            ),
        )
        .unwrap();
        let applied_outcome = applied_command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed);
        let (_, not_applied_witness) = MetadataCommitReceiptResolveCommandV1::mint_live(
            &authority,
            poisoned_origin(
                &planned,
                MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
            ),
            MetadataCommitResolutionV1::not_applied_settled(&authority, prior, [17; 32]),
        )
        .unwrap();
        assert_eq!(
            applied_outcome.into_result_for(not_applied_witness),
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        );

        let (settled_command, _) = MetadataCommitReceiptPoisonCommandV1::mint(
            &authority,
            persisted_origin(&planned),
            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
        )
        .unwrap();
        let settled_outcome = settled_command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed);
        let (_, unsettled_witness) = MetadataCommitReceiptPoisonCommandV1::mint(
            &authority,
            persisted_origin(&planned),
            MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
        )
        .unwrap();
        assert_eq!(
            settled_outcome.into_result_for(unsettled_witness),
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        );
    }
}
