use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;

use crate::codec::{
    compute_owner_admission_claim_digest, compute_owner_admission_intent_digest,
    compute_owner_admission_plan_digest, compute_owner_admission_record_digest,
    compute_owner_serving_publication_digest, compute_owner_session_binding_digest,
    compute_owner_session_renewal_target_digest,
};
use crate::owner_admission_state::validate_committed_record_descendant;
use crate::store::{
    prepare_mark_serving, validate_logical_shard_record, validate_owner_incarnation_id,
};
use crate::types::{endpoint_is_canonical, SHA256_BYTES};
use crate::{
    ControlError, LogicalShardId, LogicalShardLease, LogicalShardRecord, LogicalShardState, NodeId,
    OperationId, OwnerEpoch, OwnerIncarnationId, OwnerServingAdmission, RecoveryPublication,
};

const MAX_OWNER_ID_BYTES: usize = 256;
const MAX_OWNER_ENDPOINT_BYTES: usize = 2_048;

macro_rules! nonzero_digest_type {
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; SHA256_BYTES]);

        impl $name {
            pub fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Result<Self, ControlError> {
                if bytes.iter().all(|byte| *byte == 0) {
                    return Err(ControlError::InvalidRecord(concat!($label, " must be non-zero").to_owned()));
                }
                Ok(Self(bytes))
            }

            pub const fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "(<redacted>)"))
            }
        }
    };
}

macro_rules! canonical_digest_type {
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; SHA256_BYTES]);

        impl $name {
            pub(crate) fn from_canonical_bytes(
                bytes: [u8; SHA256_BYTES],
            ) -> Result<Self, ControlError> {
                if bytes.iter().all(|byte| *byte == 0) {
                    return Err(ControlError::InvalidRecord(
                        concat!($label, " must be non-zero").to_owned(),
                    ));
                }
                Ok(Self(bytes))
            }

            pub const fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "(<redacted>)"))
            }
        }
    };
}

nonzero_digest_type!(
    /// Digest computed from the exact-bound durable recorder/runtime bundle.
    ///
    /// This identity is not a liveness capability. Admission and serving must
    /// still retain and validate the exact runtime-reservation guard.
    OwnerRuntimeReservationDigest,
    "owner runtime reservation digest"
);
nonzero_digest_type!(
    /// Canonical identity of one exact owner-admission intent.
    OwnerAdmissionIntentDigestV1,
    "owner admission intent digest"
);
nonzero_digest_type!(
    /// Canonical identity of one exact owner-admission plan.
    OwnerAdmissionPlanDigestV1,
    "owner admission plan digest"
);
nonzero_digest_type!(
    /// Durable backend evidence proving expiration of one exact lease.
    OwnerLeaseExpiryEvidenceDigest,
    "owner lease expiry evidence digest"
);
nonzero_digest_type!(
    /// Opaque backend proof identity for one positive lifetime observation.
    ///
    /// The digest is evidence metadata, not a clock, lease, or serving permit.
    OwnerSessionLifetimeProofDigestV1,
    "owner session lifetime proof digest"
);
canonical_digest_type!(
    /// Canonical identity of one exact logical-shard owner record.
    OwnerAdmissionRecordDigestV1,
    "owner admission record digest"
);
canonical_digest_type!(
    /// Canonical identity of one exact permanent owner-admission claim.
    OwnerAdmissionClaimDigestV1,
    "owner admission claim digest"
);
canonical_digest_type!(
    /// Canonical identity of one exact owner-session binding.
    OwnerSessionBindingDigestV1,
    "owner session binding digest"
);
canonical_digest_type!(
    /// Canonical identity of an exact Recovering-to-Serving publication plan.
    OwnerServingPublicationDigestV1,
    "owner serving publication digest"
);
canonical_digest_type!(
    /// Canonical identity of one exact owner-session renewal target.
    OwnerSessionRenewalTargetDigestV1,
    "owner session renewal target digest"
);

/// Storage-neutral observation that one exact owner session is currently live.
///
/// This value is deliberately not a durable owner-admission record. A finite
/// observation carries backend-neutral exact bindings, a positive TTL, and the
/// private nominal allocation origin of the claimed command that created it,
/// but no wall-clock or monotonic timestamp. The coordinator anchors its own
/// deadline at dispatch time only after the witness accepts that same command
/// origin. Cloning an observation preserves its old origin and therefore
/// cannot refresh its TTL through a newer command allocation.
#[derive(Clone, PartialEq, Eq)]
pub enum OwnerSessionLifetimeObservationV1 {
    NonExpiring,
    Finite(FiniteOwnerSessionLifetimeObservationV1),
}

impl fmt::Debug for OwnerSessionLifetimeObservationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NonExpiring => "OwnerSessionLifetimeObservationV1::NonExpiring",
            Self::Finite(_) => "OwnerSessionLifetimeObservationV1::Finite(<redacted>)",
        })
    }
}

/// Exact backend-neutral bindings of one positive finite-TTL observation.
///
/// All fields are private. External backends can obtain this only through a
/// claimed command's validated observation builder. The command-origin field
/// is pointer-identity compared by the matching one-shot witness.
#[derive(Clone)]
pub struct FiniteOwnerSessionLifetimeObservationV1 {
    command_origin: Arc<crate::owner_admission_command::OwnerAdmissionCommandCore>,
    lease: LogicalShardLease,
    plan_digest: OwnerAdmissionPlanDigestV1,
    record_digest: OwnerAdmissionRecordDigestV1,
    session_binding_digest: OwnerSessionBindingDigestV1,
    observed_ttl_seconds: NonZeroU64,
    proof_digest: OwnerSessionLifetimeProofDigestV1,
}

impl PartialEq for FiniteOwnerSessionLifetimeObservationV1 {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.command_origin, &other.command_origin)
            && self.lease == other.lease
            && self.plan_digest == other.plan_digest
            && self.record_digest == other.record_digest
            && self.session_binding_digest == other.session_binding_digest
            && self.observed_ttl_seconds == other.observed_ttl_seconds
            && self.proof_digest == other.proof_digest
    }
}

impl Eq for FiniteOwnerSessionLifetimeObservationV1 {}

impl FiniteOwnerSessionLifetimeObservationV1 {
    pub const fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub const fn plan_digest(&self) -> OwnerAdmissionPlanDigestV1 {
        self.plan_digest
    }

    pub const fn record_digest(&self) -> OwnerAdmissionRecordDigestV1 {
        self.record_digest
    }

    pub const fn session_binding_digest(&self) -> OwnerSessionBindingDigestV1 {
        self.session_binding_digest
    }

    pub const fn observed_ttl_seconds(&self) -> NonZeroU64 {
        self.observed_ttl_seconds
    }

    pub const fn proof_digest(&self) -> OwnerSessionLifetimeProofDigestV1 {
        self.proof_digest
    }
}

impl fmt::Debug for FiniteOwnerSessionLifetimeObservationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FiniteOwnerSessionLifetimeObservationV1(<redacted>)")
    }
}

/// Whether an intent installs the first owner or the successor to a released
/// last-installed owner.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OwnerAdmissionKindV1 {
    Fresh = 1,
    Successor = 2,
}

impl TryFrom<u8> for OwnerAdmissionKindV1 {
    type Error = ControlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Fresh),
            2 => Ok(Self::Successor),
            value => Err(ControlError::Codec(format!(
                "unsupported owner admission kind {value}"
            ))),
        }
    }
}

impl From<OwnerAdmissionKindV1> for u8 {
    fn from(value: OwnerAdmissionKindV1) -> Self {
        value as u8
    }
}

/// Closed reason why a prepared plan was aborted before commitment.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OwnerAdmissionAbortReasonV1 {
    OwnerCasRejected = 1,
    LeaseLostBeforeCommit = 2,
    RuntimeReservationLost = 3,
}

/// Closed reason why an absent claim key was permanently rejected.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OwnerAdmissionRejectionReasonV1 {
    ExpectedShardChanged = 1,
    PreviousClaimChanged = 2,
    ServingAdmissionChanged = 3,
    RuntimeReservationConflict = 4,
    ActivePlanExists = 5,
    /// Claim-absent seal won before any late prepare could linearize.
    PrepareAmbiguitySealed = 6,
}

impl TryFrom<u8> for OwnerAdmissionAbortReasonV1 {
    type Error = ControlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::OwnerCasRejected),
            2 => Ok(Self::LeaseLostBeforeCommit),
            3 => Ok(Self::RuntimeReservationLost),
            value => Err(ControlError::Codec(format!(
                "unsupported owner admission abort reason {value}"
            ))),
        }
    }
}

impl From<OwnerAdmissionAbortReasonV1> for u8 {
    fn from(value: OwnerAdmissionAbortReasonV1) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for OwnerAdmissionRejectionReasonV1 {
    type Error = ControlError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::ExpectedShardChanged),
            2 => Ok(Self::PreviousClaimChanged),
            3 => Ok(Self::ServingAdmissionChanged),
            4 => Ok(Self::RuntimeReservationConflict),
            5 => Ok(Self::ActivePlanExists),
            6 => Ok(Self::PrepareAmbiguitySealed),
            value => Err(ControlError::Codec(format!(
                "unsupported owner admission rejection reason {value}"
            ))),
        }
    }
}

impl From<OwnerAdmissionRejectionReasonV1> for u8 {
    fn from(value: OwnerAdmissionRejectionReasonV1) -> Self {
        value as u8
    }
}

/// Closed reason why a committed owner admission permanently stopped serving.
#[derive(Clone, PartialEq, Eq)]
pub enum OwnerAdmissionTerminationReasonV1 {
    Released,
    LeaseExpired {
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    AuthorityCutover {
        migration_id: OperationId,
    },
}

impl fmt::Debug for OwnerAdmissionTerminationReasonV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Released => "Released",
            Self::LeaseExpired { .. } => "LeaseExpired",
            Self::AuthorityCutover { .. } => "AuthorityCutover",
        })
    }
}

/// Permanent identity of one claim, independent of its monotonic phase.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerAdmissionClaimIdentityV1 {
    logical_shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
    intent_digest: OwnerAdmissionIntentDigestV1,
    reservation_digest: OwnerRuntimeReservationDigest,
    planned_epoch: OwnerEpoch,
}

impl fmt::Debug for OwnerAdmissionClaimIdentityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerAdmissionClaimIdentityV1(<redacted>)")
    }
}

impl OwnerAdmissionClaimIdentityV1 {
    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn owner_incarnation_id(&self) -> OwnerIncarnationId {
        self.owner_incarnation_id
    }

    pub const fn intent_digest(&self) -> OwnerAdmissionIntentDigestV1 {
        self.intent_digest
    }

    pub const fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.reservation_digest
    }

    pub const fn planned_epoch(&self) -> OwnerEpoch {
        self.planned_epoch
    }

    pub(crate) fn from_durable_parts(
        logical_shard_id: LogicalShardId,
        owner_incarnation_id: OwnerIncarnationId,
        intent_digest: OwnerAdmissionIntentDigestV1,
        reservation_digest: OwnerRuntimeReservationDigest,
        planned_epoch: OwnerEpoch,
    ) -> Result<Self, ControlError> {
        validate_logical_shard_id(logical_shard_id)?;
        validate_owner_incarnation_id(owner_incarnation_id)?;
        Ok(Self {
            logical_shard_id,
            owner_incarnation_id,
            intent_digest,
            reservation_digest,
            planned_epoch,
        })
    }

    fn for_intent(intent: &OwnerAdmissionIntentV1) -> Self {
        Self {
            logical_shard_id: intent.logical_shard_id(),
            owner_incarnation_id: intent.owner_incarnation_id,
            intent_digest: intent.digest,
            reservation_digest: intent.reservation_digest,
            planned_epoch: intent.planned_epoch,
        }
    }
}

/// Durable monotonic phase of one exact owner-admission claim.
#[derive(Clone, PartialEq, Eq)]
pub enum OwnerAdmissionClaimPhaseV1 {
    Prepared {
        lease: LogicalShardLease,
        plan_digest: OwnerAdmissionPlanDigestV1,
    },
    Committed {
        lease: LogicalShardLease,
        plan_digest: OwnerAdmissionPlanDigestV1,
    },
    Terminated {
        lease: LogicalShardLease,
        plan_digest: OwnerAdmissionPlanDigestV1,
        reason: OwnerAdmissionTerminationReasonV1,
    },
    Aborted {
        lease: LogicalShardLease,
        plan_digest: OwnerAdmissionPlanDigestV1,
        reason: OwnerAdmissionAbortReasonV1,
    },
    Rejected {
        reason: OwnerAdmissionRejectionReasonV1,
    },
}

impl OwnerAdmissionClaimPhaseV1 {
    pub const fn tag(&self) -> &'static str {
        match self {
            Self::Prepared { .. } => "Prepared",
            Self::Committed { .. } => "Committed",
            Self::Terminated { .. } => "Terminated",
            Self::Aborted { .. } => "Aborted",
            Self::Rejected { .. } => "Rejected",
        }
    }
}

impl fmt::Debug for OwnerAdmissionClaimPhaseV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.tag())
    }
}

/// Permanent claim for one exact planned owner incarnation.
///
/// `OutcomeUnknown` is intentionally absent: ambiguity is a caller state that
/// must reconcile the same permanent claim, not a durable claim phase.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerAdmissionClaimV1 {
    identity: OwnerAdmissionClaimIdentityV1,
    phase: OwnerAdmissionClaimPhaseV1,
}

impl fmt::Debug for OwnerAdmissionClaimV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionClaimV1")
            .field("phase", &self.phase.tag())
            .finish_non_exhaustive()
    }
}

impl OwnerAdmissionClaimV1 {
    pub fn prepared(plan: &PlannedOwnerAdmissionV1) -> Result<Self, ControlError> {
        let claim = Self {
            identity: OwnerAdmissionClaimIdentityV1::for_intent(plan.intent()),
            phase: OwnerAdmissionClaimPhaseV1::Prepared {
                lease: plan.lease().clone(),
                plan_digest: plan.digest(),
            },
        };
        claim.validate()?;
        Ok(claim)
    }

    /// Construct the only claim that may be installed when the permanent
    /// claim key was absent and planning was rejected without owner mutation.
    pub fn rejected_from_absent(
        intent: &OwnerAdmissionIntentV1,
        reason: OwnerAdmissionRejectionReasonV1,
    ) -> Result<Self, ControlError> {
        let claim = Self {
            identity: OwnerAdmissionClaimIdentityV1::for_intent(intent),
            phase: OwnerAdmissionClaimPhaseV1::Rejected { reason },
        };
        claim.validate()?;
        Ok(claim)
    }

    pub fn commit(self) -> Result<Self, ControlError> {
        match self.phase {
            OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest } => Ok(Self {
                identity: self.identity,
                phase: OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest },
            }),
            phase => Err(invalid_claim_transition(&phase, "Committed")),
        }
    }

    pub fn abort(self, reason: OwnerAdmissionAbortReasonV1) -> Result<Self, ControlError> {
        match self.phase {
            OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest } => Ok(Self {
                identity: self.identity,
                phase: OwnerAdmissionClaimPhaseV1::Aborted {
                    lease,
                    plan_digest,
                    reason,
                },
            }),
            phase => Err(invalid_claim_transition(&phase, "Aborted")),
        }
    }

    pub fn terminate(
        self,
        reason: OwnerAdmissionTerminationReasonV1,
    ) -> Result<Self, ControlError> {
        match self.phase {
            OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest } => {
                validate_termination_reason(&reason)?;
                Ok(Self {
                    identity: self.identity,
                    phase: OwnerAdmissionClaimPhaseV1::Terminated {
                        lease,
                        plan_digest,
                        reason,
                    },
                })
            }
            phase => Err(invalid_claim_transition(&phase, "Terminated")),
        }
    }

    pub const fn identity(&self) -> &OwnerAdmissionClaimIdentityV1 {
        &self.identity
    }

    pub const fn phase(&self) -> &OwnerAdmissionClaimPhaseV1 {
        &self.phase
    }

    pub const fn is_terminated(&self) -> bool {
        matches!(self.phase, OwnerAdmissionClaimPhaseV1::Terminated { .. })
    }

    pub(crate) fn from_durable_parts(
        identity: OwnerAdmissionClaimIdentityV1,
        phase: OwnerAdmissionClaimPhaseV1,
    ) -> Result<Self, ControlError> {
        let claim = Self { identity, phase };
        claim.validate()?;
        Ok(claim)
    }

    pub(crate) fn validate(&self) -> Result<(), ControlError> {
        validate_owner_incarnation_id(self.identity.owner_incarnation_id)?;
        match &self.phase {
            OwnerAdmissionClaimPhaseV1::Prepared { lease, .. }
            | OwnerAdmissionClaimPhaseV1::Committed { lease, .. }
            | OwnerAdmissionClaimPhaseV1::Aborted { lease, .. } => {
                validate_lease_identity(lease)?;
                if lease.logical_shard_id != self.identity.logical_shard_id
                    || lease.owner_incarnation_id != self.identity.owner_incarnation_id
                    || lease.owner_epoch != self.identity.planned_epoch
                {
                    return Err(ControlError::InvalidRecord(
                        "prepared claim lease does not match claim identity".to_owned(),
                    ));
                }
            }
            OwnerAdmissionClaimPhaseV1::Terminated { lease, reason, .. } => {
                validate_lease_identity(lease)?;
                if lease.logical_shard_id != self.identity.logical_shard_id
                    || lease.owner_incarnation_id != self.identity.owner_incarnation_id
                    || lease.owner_epoch != self.identity.planned_epoch
                {
                    return Err(ControlError::InvalidRecord(
                        "terminated claim lease does not match claim identity".to_owned(),
                    ));
                }
                validate_termination_reason(reason)?;
            }
            OwnerAdmissionClaimPhaseV1::Rejected { .. } => {}
        }
        Ok(())
    }
}

/// Exact, canonical intent persisted before any owner CAS is attempted.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerAdmissionIntentV1 {
    kind: OwnerAdmissionKindV1,
    admission: OwnerServingAdmission,
    expected_unowned_shard: LogicalShardRecord,
    expected_previous_claim: Option<OwnerAdmissionClaimV1>,
    owner: NodeId,
    owner_incarnation_id: OwnerIncarnationId,
    endpoint: String,
    planned_epoch: OwnerEpoch,
    reservation_digest: OwnerRuntimeReservationDigest,
    digest: OwnerAdmissionIntentDigestV1,
}

impl fmt::Debug for OwnerAdmissionIntentV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionIntentV1")
            .field("kind", &self.kind)
            .field(
                "has_previous_claim",
                &self.expected_previous_claim.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl OwnerAdmissionIntentV1 {
    pub fn fresh(
        admission: OwnerServingAdmission,
        expected_never_owned_shard: LogicalShardRecord,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
        reservation_digest: OwnerRuntimeReservationDigest,
    ) -> Result<Self, ControlError> {
        Self::build(
            OwnerAdmissionKindV1::Fresh,
            admission,
            expected_never_owned_shard,
            None,
            owner,
            owner_incarnation_id,
            endpoint,
            reservation_digest,
        )
    }

    pub fn successor(
        admission: OwnerServingAdmission,
        expected_released_shard: LogicalShardRecord,
        expected_previous_claim: OwnerAdmissionClaimV1,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
        reservation_digest: OwnerRuntimeReservationDigest,
    ) -> Result<Self, ControlError> {
        Self::build(
            OwnerAdmissionKindV1::Successor,
            admission,
            expected_released_shard,
            Some(expected_previous_claim),
            owner,
            owner_incarnation_id,
            endpoint,
            reservation_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_durable_parts(
        kind: OwnerAdmissionKindV1,
        admission: OwnerServingAdmission,
        expected_unowned_shard: LogicalShardRecord,
        expected_previous_claim: Option<OwnerAdmissionClaimV1>,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
        reservation_digest: OwnerRuntimeReservationDigest,
        stored_planned_epoch: OwnerEpoch,
        stored_digest: OwnerAdmissionIntentDigestV1,
    ) -> Result<Self, ControlError> {
        let intent = Self::build(
            kind,
            admission,
            expected_unowned_shard,
            expected_previous_claim,
            owner,
            owner_incarnation_id,
            endpoint,
            reservation_digest,
        )?;
        if intent.planned_epoch != stored_planned_epoch || intent.digest != stored_digest {
            return Err(ControlError::Codec(
                "owner admission intent epoch or digest does not match canonical input".to_owned(),
            ));
        }
        Ok(intent)
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        kind: OwnerAdmissionKindV1,
        admission: OwnerServingAdmission,
        expected_unowned_shard: LogicalShardRecord,
        expected_previous_claim: Option<OwnerAdmissionClaimV1>,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
        reservation_digest: OwnerRuntimeReservationDigest,
    ) -> Result<Self, ControlError> {
        validate_logical_shard_record(&expected_unowned_shard)?;
        validate_expected_unowned_shard(&expected_unowned_shard)?;
        validate_admission_ids(&admission)?;
        validate_logical_shard_id(expected_unowned_shard.logical_shard_id)?;
        validate_owner_incarnation_id(owner_incarnation_id)?;
        validate_owner_and_endpoint(&owner, &endpoint)?;
        if admission.logical_shard_id() != expected_unowned_shard.logical_shard_id {
            return Err(ControlError::InvalidRecord(
                "owner admission and expected shard belong to different logical shards".to_owned(),
            ));
        }

        let planned_epoch = match kind {
            OwnerAdmissionKindV1::Fresh => {
                if expected_unowned_shard.owner_epoch.is_some()
                    || expected_unowned_shard.owner_incarnation_id.is_some()
                    || expected_previous_claim.is_some()
                {
                    return Err(ControlError::InvalidRecord(
                        "fresh owner admission requires a never-owned shard and absent previous claim"
                            .to_owned(),
                    ));
                }
                OwnerEpoch::new(1)
                    .map_err(|error| ControlError::InvalidRecord(error.to_string()))?
            }
            OwnerAdmissionKindV1::Successor => {
                let previous_epoch = expected_unowned_shard.owner_epoch.ok_or_else(|| {
                    ControlError::InvalidRecord(
                        "successor owner admission requires a released last-installed epoch"
                            .to_owned(),
                    )
                })?;
                let previous_incarnation = expected_unowned_shard.owner_incarnation_id.ok_or_else(
                    || {
                        ControlError::InvalidRecord(
                            "successor owner admission requires a released last-installed incarnation"
                                .to_owned(),
                        )
                    },
                )?;
                if previous_incarnation == owner_incarnation_id {
                    return Err(ControlError::InvalidRecord(
                        "successor owner admission must use a new incarnation id".to_owned(),
                    ));
                }
                let previous_claim = expected_previous_claim.as_ref().ok_or_else(|| {
                    ControlError::InvalidRecord(
                        "successor owner admission requires the previous permanent terminated claim"
                            .to_owned(),
                    )
                })?;
                if !previous_claim.is_terminated()
                    || previous_claim.identity.logical_shard_id
                        != expected_unowned_shard.logical_shard_id
                    || previous_claim.identity.owner_incarnation_id != previous_incarnation
                    || previous_claim.identity.planned_epoch != previous_epoch
                {
                    return Err(ControlError::InvalidRecord(
                        "successor previous claim does not exactly bind the released shard"
                            .to_owned(),
                    ));
                }
                let terminal_predecessor_is_admissible = matches!(
                    previous_claim.phase(),
                    OwnerAdmissionClaimPhaseV1::Terminated {
                        reason: OwnerAdmissionTerminationReasonV1::Released
                            | OwnerAdmissionTerminationReasonV1::LeaseExpired { .. }
                            | OwnerAdmissionTerminationReasonV1::AuthorityCutover { .. },
                        ..
                    }
                );
                if !terminal_predecessor_is_admissible {
                    return Err(ControlError::InvalidRecord(
                        "successor owner admission requires a Released, LeaseExpired, or AuthorityCutover predecessor"
                            .to_owned(),
                    ));
                }
                let next = previous_epoch.get().checked_add(1).ok_or_else(|| {
                    ControlError::InvalidRecord("owner epoch is exhausted".to_owned())
                })?;
                OwnerEpoch::new(next)
                    .map_err(|error| ControlError::InvalidRecord(error.to_string()))?
            }
        };

        let digest = compute_owner_admission_intent_digest(
            kind,
            &admission,
            &expected_unowned_shard,
            expected_previous_claim.as_ref(),
            &owner,
            owner_incarnation_id,
            &endpoint,
            planned_epoch,
            reservation_digest,
        )?;
        Ok(Self {
            kind,
            admission,
            expected_unowned_shard,
            expected_previous_claim,
            owner,
            owner_incarnation_id,
            endpoint,
            planned_epoch,
            reservation_digest,
            digest,
        })
    }

    pub const fn kind(&self) -> OwnerAdmissionKindV1 {
        self.kind
    }

    pub const fn admission(&self) -> &OwnerServingAdmission {
        &self.admission
    }

    pub const fn expected_unowned_shard(&self) -> &LogicalShardRecord {
        &self.expected_unowned_shard
    }

    pub fn expected_previous_claim(&self) -> Option<&OwnerAdmissionClaimV1> {
        self.expected_previous_claim.as_ref()
    }

    pub const fn owner(&self) -> &NodeId {
        &self.owner
    }

    pub const fn owner_incarnation_id(&self) -> OwnerIncarnationId {
        self.owner_incarnation_id
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub const fn planned_epoch(&self) -> OwnerEpoch {
        self.planned_epoch
    }

    pub const fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.reservation_digest
    }

    pub const fn digest(&self) -> OwnerAdmissionIntentDigestV1 {
        self.digest
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.expected_unowned_shard.logical_shard_id
    }
}

/// Durable owner plan. The plan body has no TTL; a separate exact sentinel may
/// later be attached to the backend lease represented by `lease`.
#[derive(Clone, PartialEq, Eq)]
pub struct PlannedOwnerAdmissionV1 {
    intent: OwnerAdmissionIntentV1,
    lease: LogicalShardLease,
    digest: OwnerAdmissionPlanDigestV1,
}

impl fmt::Debug for PlannedOwnerAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlannedOwnerAdmissionV1")
            .field("intent_kind", &self.intent.kind)
            .finish_non_exhaustive()
    }
}

impl PlannedOwnerAdmissionV1 {
    pub fn new(
        intent: OwnerAdmissionIntentV1,
        lease: LogicalShardLease,
    ) -> Result<Self, ControlError> {
        validate_plan_lease(&intent, &lease)?;
        let digest = compute_owner_admission_plan_digest(&intent, &lease)?;
        Ok(Self {
            intent,
            lease,
            digest,
        })
    }

    pub(crate) fn from_durable_parts(
        intent: OwnerAdmissionIntentV1,
        lease: LogicalShardLease,
        stored_digest: OwnerAdmissionPlanDigestV1,
    ) -> Result<Self, ControlError> {
        let plan = Self::new(intent, lease)?;
        if plan.digest != stored_digest {
            return Err(ControlError::Codec(
                "owner admission plan digest does not match canonical input".to_owned(),
            ));
        }
        Ok(plan)
    }

    pub const fn intent(&self) -> &OwnerAdmissionIntentV1 {
        &self.intent
    }

    pub const fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub const fn digest(&self) -> OwnerAdmissionPlanDigestV1 {
        self.digest
    }
}

impl OwnerSessionLifetimeObservationV1 {
    pub(crate) fn non_expiring_for_committed_descendant(
        plan: &PlannedOwnerAdmissionV1,
        record: &LogicalShardRecord,
        session: &LogicalShardLease,
    ) -> Result<Self, ControlError> {
        if session != plan.lease() {
            return Err(ControlError::InvalidRecord(
                "non-expiring owner-session observation does not match the exact plan session"
                    .to_owned(),
            ));
        }
        validate_committed_record_descendant(plan, record).map_err(|code| {
            ControlError::InvalidRecord(format!(
                "non-expiring owner-session observation has an invalid committed record: {code:?}"
            ))
        })?;
        Ok(Self::NonExpiring)
    }

    pub(crate) fn finite_for_committed_descendant(
        command_origin: Arc<crate::owner_admission_command::OwnerAdmissionCommandCore>,
        plan: &PlannedOwnerAdmissionV1,
        record: &LogicalShardRecord,
        session: &LogicalShardLease,
        observed_ttl_seconds: NonZeroU64,
        proof_digest: OwnerSessionLifetimeProofDigestV1,
    ) -> Result<Self, ControlError> {
        if session != plan.lease() {
            return Err(ControlError::InvalidRecord(
                "finite owner-session observation does not match the exact plan session".to_owned(),
            ));
        }
        validate_committed_record_descendant(plan, record).map_err(|code| {
            ControlError::InvalidRecord(format!(
                "finite owner-session observation has an invalid committed record: {code:?}"
            ))
        })?;
        Ok(Self::Finite(FiniteOwnerSessionLifetimeObservationV1 {
            command_origin,
            lease: session.clone(),
            plan_digest: plan.digest(),
            record_digest: compute_owner_admission_record_digest(record)?,
            session_binding_digest: compute_owner_session_binding_digest(session)?,
            observed_ttl_seconds,
            proof_digest,
        }))
    }

    #[cfg(test)]
    pub(crate) fn validates_exact(
        &self,
        plan: &PlannedOwnerAdmissionV1,
        record: &LogicalShardRecord,
        session: &LogicalShardLease,
    ) -> bool {
        match self {
            Self::NonExpiring => true,
            Self::Finite(observation) => {
                observation.lease == *session
                    && observation.plan_digest == plan.digest()
                    && compute_owner_admission_record_digest(record)
                        .is_ok_and(|digest| digest == observation.record_digest)
                    && compute_owner_session_binding_digest(session)
                        .is_ok_and(|digest| digest == observation.session_binding_digest)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn validates_command_origin(
        &self,
        command_origin: &Arc<crate::owner_admission_command::OwnerAdmissionCommandCore>,
    ) -> bool {
        match self {
            Self::NonExpiring => true,
            Self::Finite(observation) => Arc::ptr_eq(&observation.command_origin, command_origin),
        }
    }
}

/// Exact Recovering-to-Serving publication bound to one committed plan.
///
/// The Serving target and every digest are derived from canonical inputs. No
/// caller can supply a target record or digest independently.
#[derive(Clone, PartialEq, Eq)]
pub struct PlannedOwnerServingPublicationV1 {
    plan: PlannedOwnerAdmissionV1,
    source: LogicalShardRecord,
    publication: RecoveryPublication,
    target: LogicalShardRecord,
    source_digest: OwnerAdmissionRecordDigestV1,
    target_digest: OwnerAdmissionRecordDigestV1,
    digest: OwnerServingPublicationDigestV1,
}

impl PlannedOwnerServingPublicationV1 {
    pub(crate) fn new(
        plan: PlannedOwnerAdmissionV1,
        source: LogicalShardRecord,
        publication: RecoveryPublication,
    ) -> Result<Self, ControlError> {
        validate_committed_record_descendant(&plan, &source).map_err(|code| {
            ControlError::InvalidRecord(format!(
                "owner serving publication source is not a committed descendant: {code:?}"
            ))
        })?;
        if source.state != LogicalShardState::Recovering {
            return Err(ControlError::InvalidRecord(
                "owner serving publication source must be Recovering".to_owned(),
            ));
        }
        let target = prepare_mark_serving(&source, plan.lease(), publication.clone())?;
        let source_digest = compute_owner_admission_record_digest(&source)?;
        let target_digest = compute_owner_admission_record_digest(&target)?;
        let digest =
            compute_owner_serving_publication_digest(&plan, &source, &publication, &target)?;
        Ok(Self {
            plan,
            source,
            publication,
            target,
            source_digest,
            target_digest,
            digest,
        })
    }

    pub const fn plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.plan
    }

    pub const fn source(&self) -> &LogicalShardRecord {
        &self.source
    }

    pub const fn publication(&self) -> &RecoveryPublication {
        &self.publication
    }

    pub const fn target(&self) -> &LogicalShardRecord {
        &self.target
    }

    pub const fn source_digest(&self) -> OwnerAdmissionRecordDigestV1 {
        self.source_digest
    }

    pub const fn target_digest(&self) -> OwnerAdmissionRecordDigestV1 {
        self.target_digest
    }

    pub const fn digest(&self) -> OwnerServingPublicationDigestV1 {
        self.digest
    }

    pub(crate) fn validate(&self) -> Result<(), ControlError> {
        let recomputed = Self::new(
            self.plan.clone(),
            self.source.clone(),
            self.publication.clone(),
        )?;
        if &recomputed != self {
            return Err(ControlError::InvalidRecord(
                "owner serving publication does not match its canonical inputs".to_owned(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for PlannedOwnerServingPublicationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PlannedOwnerServingPublicationV1(<redacted>)")
    }
}

/// Exact plan, Committed claim, and session targeted by one renewal attempt.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerSessionRenewalTargetV1 {
    plan: PlannedOwnerAdmissionV1,
    claim: OwnerAdmissionClaimV1,
    session: LogicalShardLease,
    claim_digest: OwnerAdmissionClaimDigestV1,
    session_binding_digest: OwnerSessionBindingDigestV1,
    digest: OwnerSessionRenewalTargetDigestV1,
}

impl OwnerSessionRenewalTargetV1 {
    pub(crate) fn new(
        plan: PlannedOwnerAdmissionV1,
        claim: OwnerAdmissionClaimV1,
    ) -> Result<Self, ControlError> {
        let expected_claim = OwnerAdmissionClaimV1::prepared(&plan)?.commit()?;
        if claim != expected_claim {
            return Err(ControlError::InvalidRecord(
                "owner-session renewal target requires the exact Committed claim".to_owned(),
            ));
        }
        let session = plan.lease().clone();
        let claim_digest = compute_owner_admission_claim_digest(&claim)?;
        let session_binding_digest = compute_owner_session_binding_digest(&session)?;
        let digest = compute_owner_session_renewal_target_digest(&plan, &claim, &session)?;
        Ok(Self {
            plan,
            claim,
            session,
            claim_digest,
            session_binding_digest,
            digest,
        })
    }

    pub const fn plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.plan
    }

    pub const fn claim(&self) -> &OwnerAdmissionClaimV1 {
        &self.claim
    }

    pub const fn session(&self) -> &LogicalShardLease {
        &self.session
    }

    pub const fn claim_digest(&self) -> OwnerAdmissionClaimDigestV1 {
        self.claim_digest
    }

    pub const fn session_binding_digest(&self) -> OwnerSessionBindingDigestV1 {
        self.session_binding_digest
    }

    pub const fn digest(&self) -> OwnerSessionRenewalTargetDigestV1 {
        self.digest
    }

    pub(crate) fn validate(&self) -> Result<(), ControlError> {
        let recomputed = Self::new(self.plan.clone(), self.claim.clone())?;
        if &recomputed != self {
            return Err(ControlError::InvalidRecord(
                "owner-session renewal target does not match its canonical inputs".to_owned(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for OwnerSessionRenewalTargetV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerSessionRenewalTargetV1(<redacted>)")
    }
}

/// Exact lease-attached sentinel for one durable plan body.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerAdmissionPlanSentinelV1 {
    logical_shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
    lease_id: u64,
    plan_digest: OwnerAdmissionPlanDigestV1,
}

impl fmt::Debug for OwnerAdmissionPlanSentinelV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerAdmissionPlanSentinelV1(<redacted>)")
    }
}

impl OwnerAdmissionPlanSentinelV1 {
    pub fn for_plan(plan: &PlannedOwnerAdmissionV1) -> Self {
        Self {
            logical_shard_id: plan.lease.logical_shard_id,
            owner_incarnation_id: plan.lease.owner_incarnation_id,
            lease_id: plan.lease.lease_id,
            plan_digest: plan.digest,
        }
    }

    pub(crate) fn from_durable_parts(
        logical_shard_id: LogicalShardId,
        owner_incarnation_id: OwnerIncarnationId,
        lease_id: u64,
        plan_digest: OwnerAdmissionPlanDigestV1,
    ) -> Result<Self, ControlError> {
        validate_logical_shard_id(logical_shard_id)?;
        validate_owner_incarnation_id(owner_incarnation_id)?;
        if lease_id == 0 {
            return Err(ControlError::InvalidRecord(
                "owner admission plan sentinel lease id must be non-zero".to_owned(),
            ));
        }
        Ok(Self {
            logical_shard_id,
            owner_incarnation_id,
            lease_id,
            plan_digest,
        })
    }

    pub fn validate_plan(&self, plan: &PlannedOwnerAdmissionV1) -> Result<(), ControlError> {
        if self != &Self::for_plan(plan) {
            return Err(ControlError::InvalidRecord(
                "owner admission plan sentinel does not exactly match the plan lease".to_owned(),
            ));
        }
        Ok(())
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn owner_incarnation_id(&self) -> OwnerIncarnationId {
        self.owner_incarnation_id
    }

    pub const fn lease_id(&self) -> u64 {
        self.lease_id
    }

    pub const fn plan_digest(&self) -> OwnerAdmissionPlanDigestV1 {
        self.plan_digest
    }
}

fn validate_expected_unowned_shard(record: &LogicalShardRecord) -> Result<(), ControlError> {
    if record.owner.is_some()
        || record.lease_id != 0
        || record.state != LogicalShardState::Unassigned
        || record.endpoint.is_some()
    {
        return Err(ControlError::InvalidRecord(
            "owner admission expected shard must be exactly unowned".to_owned(),
        ));
    }
    Ok(())
}

fn validate_owner_and_endpoint(owner: &NodeId, endpoint: &str) -> Result<(), ControlError> {
    if owner.as_str().len() > MAX_OWNER_ID_BYTES {
        return Err(ControlError::InvalidRecord(format!(
            "owner id exceeds {MAX_OWNER_ID_BYTES} bytes"
        )));
    }
    if endpoint.len() > MAX_OWNER_ENDPOINT_BYTES || !endpoint_is_canonical(endpoint) {
        return Err(ControlError::InvalidRecord(format!(
            "owner endpoint must be canonical and contain at most {MAX_OWNER_ENDPOINT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_admission_ids(admission: &OwnerServingAdmission) -> Result<(), ControlError> {
    if admission
        .placement()
        .root_id
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(ControlError::InvalidRecord(
            "owner admission root id must be non-zero".to_owned(),
        ));
    }
    validate_logical_shard_id(admission.logical_shard_id())
}

fn validate_logical_shard_id(logical_shard_id: LogicalShardId) -> Result<(), ControlError> {
    if logical_shard_id.as_bytes().iter().all(|byte| *byte == 0) {
        return Err(ControlError::InvalidRecord(
            "owner admission logical shard id must be non-zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_lease_identity(lease: &LogicalShardLease) -> Result<(), ControlError> {
    validate_logical_shard_id(lease.logical_shard_id)?;
    validate_owner_incarnation_id(lease.owner_incarnation_id)?;
    if lease.owner.as_str().len() > MAX_OWNER_ID_BYTES {
        return Err(ControlError::InvalidRecord(format!(
            "planned owner id exceeds {MAX_OWNER_ID_BYTES} bytes"
        )));
    }
    if lease.lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "planned owner lease id must be non-zero".to_owned(),
        ));
    }
    if lease.authority.logical_shard_id != lease.logical_shard_id
        || lease
            .authority
            .authority_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
    {
        return Err(ControlError::InvalidRecord(
            "planned owner lease authority fence is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_plan_lease(
    intent: &OwnerAdmissionIntentV1,
    lease: &LogicalShardLease,
) -> Result<(), ControlError> {
    validate_lease_identity(lease)?;
    if lease.logical_shard_id != intent.logical_shard_id()
        || lease.owner != intent.owner
        || lease.owner_epoch != intent.planned_epoch
        || lease.owner_incarnation_id != intent.owner_incarnation_id
        || lease.authority != intent.admission.authority().fence()
    {
        return Err(ControlError::InvalidRecord(
            "planned owner lease is not exactly derived from the intent".to_owned(),
        ));
    }
    Ok(())
}

fn validate_termination_reason(
    reason: &OwnerAdmissionTerminationReasonV1,
) -> Result<(), ControlError> {
    match reason {
        OwnerAdmissionTerminationReasonV1::Released
        | OwnerAdmissionTerminationReasonV1::LeaseExpired { .. } => Ok(()),
        OwnerAdmissionTerminationReasonV1::AuthorityCutover { migration_id } => {
            if migration_id.as_bytes().iter().all(|byte| *byte == 0) {
                return Err(ControlError::InvalidRecord(
                    "authority-cutover migration id must be non-zero".to_owned(),
                ));
            }
            Ok(())
        }
    }
}

fn invalid_claim_transition(phase: &OwnerAdmissionClaimPhaseV1, target: &str) -> ControlError {
    ControlError::InvalidRecord(format!(
        "owner admission claim transition from {} to {target} is not allowed",
        phase.tag()
    ))
}
