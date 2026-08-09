// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! Durable orchestration for planned owner admission.
//!
//! This test-only module characterizes the intended single boundary that mints
//! owner-admission commands and consumes their witnesses. Production wiring is
//! deliberately absent until a sealed factory can supply the exact durable
//! attempt bundle. An ordinary [`ControlStore`] can execute a command, but it
//! cannot construct or duplicate one.

use std::fmt;
use std::time::{Duration, Instant};

use crate::owner_admission::{
    OwnerAdmissionAbortReasonV1, OwnerAdmissionClaimV1, OwnerAdmissionIntentV1,
    OwnerAdmissionPlanDigestV1, OwnerAdmissionPlanSentinelV1, OwnerAdmissionTerminationReasonV1,
    OwnerLeaseExpiryEvidenceDigest, OwnerRuntimeReservationDigest,
    OwnerSessionLifetimeObservationV1, OwnerSessionRenewalTargetV1, PlannedOwnerAdmissionV1,
    PlannedOwnerServingPublicationV1,
};
use crate::owner_admission_command::{
    mint_abort_owner_admission_command, mint_authority_cutover_terminate_owner_admission_command,
    mint_commit_owner_admission_command, mint_expired_terminate_owner_admission_command,
    mint_prepare_owner_admission_command, mint_publish_owner_serving_command,
    mint_reconcile_owner_admission_intent_command, mint_reconcile_owner_admission_plan_command,
    mint_reconcile_owner_serving_command, mint_released_terminate_owner_admission_command,
    mint_renew_owner_session_command, AbortOwnerAdmissionResultV1, CommitOwnerAdmissionResultV1,
    OwnerAdmissionCommandWitnessError, PrepareOwnerAdmissionResultV1, PublishOwnerServingResultV1,
    ReconcileOwnerAdmissionResultV1, ReconcileOwnerAdmissionTargetV1, ReconciledOwnerLeaseExpiryV1,
    RenewOwnerSessionResultV1, TerminateOwnerAdmissionResultV1,
};
use crate::store::ControlStore;
use crate::{LogicalShardLease, LogicalShardRecord, OperationId, RecoveryPublication};

/// Closed failure reported by the trusted durable-attempt composition port.
///
/// No variant implies that a failed write did not reach durable storage. The
/// coordinator retains the exact recovery target and fails closed for every
/// error.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionAttemptPortErrorV1 {
    ReservationUnavailable = 1,
    ReservationNotHeld = 2,
    ReservationBindingMismatch = 3,
    DurableRecordConflict = 4,
    DurableRecordInconsistent = 5,
    BackendUnavailable = 6,
    DurabilityOutcomeUnknown = 7,
}

impl fmt::Display for OwnerAdmissionAttemptPortErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ReservationUnavailable => "owner-admission reservation is unavailable",
            Self::ReservationNotHeld => "owner-admission reservation is not held",
            Self::ReservationBindingMismatch => {
                "owner-admission reservation binding does not match"
            }
            Self::DurableRecordConflict => "owner-admission durable record conflicts",
            Self::DurableRecordInconsistent => "owner-admission durable record is inconsistent",
            Self::BackendUnavailable => "owner-admission attempt store is unavailable",
            Self::DurabilityOutcomeUnknown => {
                "owner-admission attempt durability outcome is unknown"
            }
        })
    }
}

impl std::error::Error for OwnerAdmissionAttemptPortErrorV1 {}

/// Operation whose exact attempt receipt is being recorded or reconciled.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionCoordinatorOperationV1 {
    Begin = 1,
    ResumeIntent = 2,
    ResumePlan = 3,
    Prepare = 4,
    Commit = 5,
    Abort = 6,
    Terminate = 7,
    Reconcile = 8,
    ProviderAdoption = 9,
    PublishServing = 10,
    Renew = 11,
    ResumePendingRenew = 12,
    ResumePendingPublish = 13,
    ResumeServing = 14,
}

/// Closed public projection of a crate-private command-witness rejection.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionWitnessFailureCodeV1 {
    ForeignCommand = 1,
    ExecutionNotCompleted = 2,
    ResultBindingMismatch = 3,
}

impl From<OwnerAdmissionCommandWitnessError> for OwnerAdmissionWitnessFailureCodeV1 {
    fn from(value: OwnerAdmissionCommandWitnessError) -> Self {
        match value {
            OwnerAdmissionCommandWitnessError::ForeignCommand => Self::ForeignCommand,
            OwnerAdmissionCommandWitnessError::ExecutionNotCompleted => Self::ExecutionNotCompleted,
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch => Self::ResultBindingMismatch,
        }
    }
}

/// Exact durable recovery target held by one attempt guard.
#[derive(Clone, Copy)]
pub enum OwnerAdmissionDurableTargetV1<'a> {
    Intent(&'a OwnerAdmissionIntentV1),
    Plan(&'a PlannedOwnerAdmissionV1),
    PendingPublish(&'a PlannedOwnerServingPublicationV1),
    Serving(&'a PlannedOwnerServingPublicationV1),
    PendingAbort {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: OwnerAdmissionAbortReasonV1,
    },
    PendingTerminate {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: &'a OwnerAdmissionTerminationReasonV1,
    },
    PendingRenew {
        publication: &'a PlannedOwnerServingPublicationV1,
        target: &'a OwnerSessionRenewalTargetV1,
        generation: u64,
    },
}

impl OwnerAdmissionDurableTargetV1<'_> {
    fn name(&self) -> &'static str {
        match self {
            Self::Intent(_) => "intent",
            Self::Plan(_) => "plan",
            Self::PendingPublish(_) => "pending-publish",
            Self::Serving(_) => "serving-publication",
            Self::PendingAbort { .. } => "pending-abort",
            Self::PendingTerminate { .. } => "pending-terminate",
            Self::PendingRenew { .. } => "pending-renew",
        }
    }
}

impl fmt::Debug for OwnerAdmissionDurableTargetV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("OwnerAdmissionDurableTargetV1")
            .field(&self.name())
            .finish()
    }
}

/// Whether a terminal receipt was produced by a fresh command or exact replay.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionCommittedReceiptKindV1 {
    Committed = 1,
    AlreadyCommitted = 2,
    Reconciled = 3,
}

/// How an exact termination result was observed.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionTerminatedReceiptKindV1 {
    Terminated = 1,
    AlreadyTerminated = 2,
    Superseded = 3,
}

/// How an exact Recovering-to-Serving receipt was observed.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerServingPublishedReceiptKindV1 {
    Published = 1,
    AlreadyPublished = 2,
    Reconciled = 3,
}

/// Canonical durable update for one exact owner-admission attempt.
///
/// This is deliberately a closed, typed enum. Implementations must not replace
/// it with backend strings or a lossy phase number. Every variant is exact
/// bound to the intent or plan carried by the same held attempt guard.
pub enum OwnerAdmissionDurableUpdateV1<'a> {
    Intent {
        intent: &'a OwnerAdmissionIntentV1,
    },
    Prepared {
        plan: &'a PlannedOwnerAdmissionV1,
        claim: &'a OwnerAdmissionClaimV1,
        sentinel: &'a OwnerAdmissionPlanSentinelV1,
    },
    ExpiredPrepared {
        plan: &'a PlannedOwnerAdmissionV1,
        claim: &'a OwnerAdmissionClaimV1,
        expected_sentinel: &'a OwnerAdmissionPlanSentinelV1,
    },
    Rejected {
        intent: &'a OwnerAdmissionIntentV1,
        claim: &'a OwnerAdmissionClaimV1,
    },
    Committed {
        kind: OwnerAdmissionCommittedReceiptKindV1,
        plan: &'a PlannedOwnerAdmissionV1,
        shard: &'a LogicalShardRecord,
        lease: &'a LogicalShardLease,
        claim: &'a OwnerAdmissionClaimV1,
    },
    PendingPublish {
        publication: &'a PlannedOwnerServingPublicationV1,
    },
    ServingPublished {
        kind: OwnerServingPublishedReceiptKindV1,
        publication: &'a PlannedOwnerServingPublicationV1,
        shard: &'a LogicalShardRecord,
        claim: &'a OwnerAdmissionClaimV1,
    },
    /// Typed observation for one exact PendingPublish. `OutcomeUnknown` keeps
    /// PendingPublish current; a matching published receipt promotes to
    /// `ServingPublished`, while a matching non-Unknown closed result may
    /// clear the pending phase without creating Serving.
    PublishClosed {
        publication: &'a PlannedOwnerServingPublicationV1,
        result: &'a PublishOwnerServingResultV1,
    },
    /// Typed observation for one exact pending renewal generation, not an
    /// unconditional transition back to Serving. Persistence must CAS the
    /// matching `(publication, target, generation)`. `OutcomeUnknown` keeps
    /// that `PendingRenew` current; only a matching non-Unknown result may
    /// clear it and make the ordinary Serving state current again.
    RenewClosed {
        publication: &'a PlannedOwnerServingPublicationV1,
        target: &'a OwnerSessionRenewalTargetV1,
        generation: u64,
        result: &'a RenewOwnerSessionResultV1,
    },
    PendingRenew {
        publication: &'a PlannedOwnerServingPublicationV1,
        target: &'a OwnerSessionRenewalTargetV1,
        generation: u64,
    },
    PendingAbort {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: OwnerAdmissionAbortReasonV1,
    },
    Aborted {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: OwnerAdmissionAbortReasonV1,
        claim: &'a OwnerAdmissionClaimV1,
    },
    PendingTerminate {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: &'a OwnerAdmissionTerminationReasonV1,
    },
    Terminated {
        kind: OwnerAdmissionTerminatedReceiptKindV1,
        plan: &'a PlannedOwnerAdmissionV1,
        reason: &'a OwnerAdmissionTerminationReasonV1,
        shard: &'a LogicalShardRecord,
        claim: &'a OwnerAdmissionClaimV1,
    },
    ExpiredCommitted {
        plan: &'a PlannedOwnerAdmissionV1,
        shard: &'a LogicalShardRecord,
        claim: &'a OwnerAdmissionClaimV1,
        expected_session: &'a LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    PrepareClosed {
        intent: &'a OwnerAdmissionIntentV1,
        result: &'a PrepareOwnerAdmissionResultV1,
    },
    CommitClosed {
        plan: &'a PlannedOwnerAdmissionV1,
        result: &'a CommitOwnerAdmissionResultV1,
    },
    AbortClosed {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: OwnerAdmissionAbortReasonV1,
        result: &'a AbortOwnerAdmissionResultV1,
    },
    TerminateClosed {
        plan: &'a PlannedOwnerAdmissionV1,
        reason: &'a OwnerAdmissionTerminationReasonV1,
        result: &'a TerminateOwnerAdmissionResultV1,
    },
    ReconcileClosed {
        target: OwnerAdmissionDurableTargetV1<'a>,
        result: &'a ReconcileOwnerAdmissionResultV1,
    },
    RecoveryRequired {
        operation: OwnerAdmissionCoordinatorOperationV1,
        target: OwnerAdmissionDurableTargetV1<'a>,
        witness_failure: OwnerAdmissionWitnessFailureCodeV1,
    },
}

impl OwnerAdmissionDurableUpdateV1<'_> {
    pub const fn tag(&self) -> &'static str {
        match self {
            Self::Intent { .. } => "Intent",
            Self::Prepared { .. } => "Prepared",
            Self::ExpiredPrepared { .. } => "ExpiredPrepared",
            Self::Rejected { .. } => "Rejected",
            Self::Committed { .. } => "Committed",
            Self::PendingPublish { .. } => "PendingPublish",
            Self::ServingPublished { .. } => "ServingPublished",
            Self::PublishClosed { .. } => "PublishClosed",
            Self::RenewClosed { .. } => "RenewClosed",
            Self::PendingRenew { .. } => "PendingRenew",
            Self::PendingAbort { .. } => "PendingAbort",
            Self::Aborted { .. } => "Aborted",
            Self::PendingTerminate { .. } => "PendingTerminate",
            Self::Terminated { .. } => "Terminated",
            Self::ExpiredCommitted { .. } => "ExpiredCommitted",
            Self::PrepareClosed { .. } => "PrepareClosed",
            Self::CommitClosed { .. } => "CommitClosed",
            Self::AbortClosed { .. } => "AbortClosed",
            Self::TerminateClosed { .. } => "TerminateClosed",
            Self::ReconcileClosed { .. } => "ReconcileClosed",
            Self::RecoveryRequired { .. } => "RecoveryRequired",
        }
    }

    fn target(&self) -> OwnerAdmissionDurableTargetV1<'_> {
        match self {
            Self::Intent { intent }
            | Self::Rejected { intent, .. }
            | Self::PrepareClosed { intent, .. } => OwnerAdmissionDurableTargetV1::Intent(intent),
            Self::Prepared { plan, .. }
            | Self::ExpiredPrepared { plan, .. }
            | Self::Committed { plan, .. }
            | Self::ExpiredCommitted { plan, .. }
            | Self::CommitClosed { plan, .. } => OwnerAdmissionDurableTargetV1::Plan(plan),
            Self::PendingPublish { publication } | Self::PublishClosed { publication, .. } => {
                OwnerAdmissionDurableTargetV1::PendingPublish(publication)
            }
            Self::ServingPublished { publication, .. } => {
                OwnerAdmissionDurableTargetV1::Serving(publication)
            }
            Self::PendingRenew {
                publication,
                target,
                generation,
            }
            | Self::RenewClosed {
                publication,
                target,
                generation,
                ..
            } => OwnerAdmissionDurableTargetV1::PendingRenew {
                publication,
                target,
                generation: *generation,
            },
            Self::PendingAbort { plan, reason }
            | Self::Aborted { plan, reason, .. }
            | Self::AbortClosed { plan, reason, .. } => {
                OwnerAdmissionDurableTargetV1::PendingAbort {
                    plan,
                    reason: *reason,
                }
            }
            Self::PendingTerminate { plan, reason }
            | Self::Terminated { plan, reason, .. }
            | Self::TerminateClosed { plan, reason, .. } => {
                OwnerAdmissionDurableTargetV1::PendingTerminate { plan, reason }
            }
            Self::ReconcileClosed { target, .. } | Self::RecoveryRequired { target, .. } => *target,
        }
    }
}

impl fmt::Debug for OwnerAdmissionDurableUpdateV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionDurableUpdateV1")
            .field("phase", &self.tag())
            .field("target", &self.target().name())
            .finish_non_exhaustive()
    }
}

/// Attempt-scoped held reservation plus canonical durability journal.
///
/// # Trusted composition contract
///
/// This trait is a trusted durability SPI; it is intentionally not an
/// `unsafe trait`, because violating it does not cause Rust memory unsafety.
/// While the object is alive it must retain one cross-process reservation
/// exact-bound to [`Self::reservation_digest`]. `validate_held` must consult
/// the authoritative reservation, not process-local belief.
///
/// `persist` must be exact-idempotent: replaying the same canonical phase and
/// binding may only confirm or rewrite that same value, while a foreign value
/// must fail with a closed conflict. It may return `Ok(())` only while the
/// reservation was held both before and after the write and after the
/// canonical update is durable across process restart, including every fsync,
/// directory sync, replicated log commit, or equivalent step required by the
/// implementation. `confirm` may return `Ok(())` only when the reservation was
/// held before and after its read and the exact canonical target is already
/// durable as the current recovery target. An ancestor Plan is not confirmable
/// after a Serving publication exists or while PendingPublish, PendingAbort,
/// PendingTerminate, or PendingRenew is current; this prevents restart from
/// routing around a possibly dispatched mutation or a locally adopted Serving
/// session. PendingPublish and terminal Serving are distinct confirmable
/// targets even when they carry the same publication digest.
/// `RenewClosed` is a typed observation of the exact current `PendingRenew`,
/// not proof that the ambiguity is closed: implementations must atomically
/// compare its publication, target, and generation. An `OutcomeUnknown` result
/// must leave that `PendingRenew` current; only a matching non-Unknown receipt
/// may CAS the recovery target back to ordinary Serving.
/// Rust types do not prove either claim. Every implementation requires
/// crash/reopen and mismatch conformance tests.
///
/// An error from either method is ambiguous with respect to durability. It
/// must never be interpreted as proof that a write did not take effect.
pub trait TrustedOwnerAdmissionAttemptV1: Send {
    fn reservation_digest(&self) -> OwnerRuntimeReservationDigest;

    fn validate_held(&mut self) -> Result<(), OwnerAdmissionAttemptPortErrorV1>;

    fn persist(
        &mut self,
        update: OwnerAdmissionDurableUpdateV1<'_>,
    ) -> Result<(), OwnerAdmissionAttemptPortErrorV1>;

    fn confirm(
        &mut self,
        target: OwnerAdmissionDurableTargetV1<'_>,
    ) -> Result<(), OwnerAdmissionAttemptPortErrorV1>;
}

/// Trusted composition boundary that acquires an attempt-scoped reservation.
///
/// `acquire` must obtain the cross-process reservation before returning its
/// non-cloneable guard. It performs no control-store mutation. The coordinator
/// separately checks the returned digest before any durable write or command.
pub trait TrustedOwnerAdmissionAttemptPortV1: Send + Sync {
    fn acquire(
        &self,
        intent: &OwnerAdmissionIntentV1,
    ) -> Result<Box<dyn TrustedOwnerAdmissionAttemptV1>, OwnerAdmissionAttemptPortErrorV1>;
}

/// Last coarse phase whose durability was positively acknowledged in this
/// process. `ServingPublication` is not itself a restart authority: the exact
/// durable target still distinguishes PendingPublish, terminal Serving, and
/// PendingRenew.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionKnownDurableStageV1 {
    None = 0,
    Intent = 1,
    Plan = 2,
    ServingPublication = 3,
}

/// Closed reason why the coordinator requires exact reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionRecoveryCauseV1 {
    AttemptPort {
        operation: OwnerAdmissionCoordinatorOperationV1,
        error: OwnerAdmissionAttemptPortErrorV1,
    },
    ReservationBindingMismatch,
    BackendOutcomeUnknown {
        operation: OwnerAdmissionCoordinatorOperationV1,
    },
    WitnessRejected {
        operation: OwnerAdmissionCoordinatorOperationV1,
        code: OwnerAdmissionWitnessFailureCodeV1,
    },
    PendingMutationUnresolved,
    SessionValidityExpired {
        operation: OwnerAdmissionCoordinatorOperationV1,
    },
    SessionEvidenceMismatch {
        operation: OwnerAdmissionCoordinatorOperationV1,
    },
}

/// Exact mutation input retained when recovery interrupted abort or terminate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionPendingMutationV1 {
    Abort(OwnerAdmissionAbortReasonV1),
    Terminate(OwnerAdmissionTerminationReasonV1),
    Renew {
        publication: PlannedOwnerServingPublicationV1,
        target: OwnerSessionRenewalTargetV1,
        generation: u64,
    },
}

/// Attempt acquisition failed before an owned held guard existed.
pub struct OwnerAdmissionAttemptNotHeldV1 {
    intent: OwnerAdmissionIntentV1,
    operation: OwnerAdmissionCoordinatorOperationV1,
    error: OwnerAdmissionAttemptPortErrorV1,
}

impl OwnerAdmissionAttemptNotHeldV1 {
    pub const fn intent(&self) -> &OwnerAdmissionIntentV1 {
        &self.intent
    }

    pub const fn operation(&self) -> OwnerAdmissionCoordinatorOperationV1 {
        self.operation
    }

    pub const fn error(&self) -> OwnerAdmissionAttemptPortErrorV1 {
        self.error
    }

    pub fn into_intent(self) -> OwnerAdmissionIntentV1 {
        self.intent
    }
}

impl fmt::Debug for OwnerAdmissionAttemptNotHeldV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionAttemptNotHeldV1")
            .field("operation", &self.operation)
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

/// Exact recovery state that continues to own the same attempt guard.
pub struct OwnerAdmissionRecoveryRequiredV1 {
    intent: Box<OwnerAdmissionIntentV1>,
    observed_plan: Option<Box<PlannedOwnerAdmissionV1>>,
    observed_serving_publication: Option<Box<PlannedOwnerServingPublicationV1>>,
    known_durable_stage: OwnerAdmissionKnownDurableStageV1,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    pending_mutation: Option<OwnerAdmissionPendingMutationV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

impl OwnerAdmissionRecoveryRequiredV1 {
    pub fn intent(&self) -> &OwnerAdmissionIntentV1 {
        self.intent.as_ref()
    }

    fn observed_plan(&self) -> Option<&PlannedOwnerAdmissionV1> {
        self.observed_plan.as_deref()
    }

    pub const fn known_durable_stage(&self) -> OwnerAdmissionKnownDurableStageV1 {
        self.known_durable_stage
    }

    pub const fn cause(&self) -> OwnerAdmissionRecoveryCauseV1 {
        self.cause
    }

    pub const fn recording_error(&self) -> Option<OwnerAdmissionAttemptPortErrorV1> {
        self.recording_error
    }

    pub const fn pending_mutation(&self) -> Option<&OwnerAdmissionPendingMutationV1> {
        self.pending_mutation.as_ref()
    }

    pub fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.attempt.reservation_digest()
    }
}

impl fmt::Debug for OwnerAdmissionRecoveryRequiredV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionRecoveryRequiredV1")
            .field("has_observed_plan", &self.observed_plan.is_some())
            .field(
                "has_observed_serving_publication",
                &self.observed_serving_publication.is_some(),
            )
            .field("known_durable_stage", &self.known_durable_stage)
            .field("cause", &self.cause)
            .field("recording_error", &self.recording_error)
            .field("pending_mutation", &self.pending_mutation)
            .finish_non_exhaustive()
    }
}

/// Durable exact intent plus the owned reservation guard for its attempt.
pub struct DurableOwnerAdmissionIntentV1 {
    intent: OwnerAdmissionIntentV1,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

impl DurableOwnerAdmissionIntentV1 {
    pub const fn intent(&self) -> &OwnerAdmissionIntentV1 {
        &self.intent
    }
}

impl fmt::Debug for DurableOwnerAdmissionIntentV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableOwnerAdmissionIntentV1(<redacted>)")
    }
}

/// Durable exact plan plus the same owned reservation guard as its intent.
pub struct DurablePlannedOwnerAdmissionV1 {
    plan: PlannedOwnerAdmissionV1,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

impl DurablePlannedOwnerAdmissionV1 {
    pub const fn plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.plan
    }
}

impl fmt::Debug for DurablePlannedOwnerAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurablePlannedOwnerAdmissionV1(<redacted>)")
    }
}

/// Durable intent or plan continuation accepted by exact reconciliation.
pub enum DurableOwnerAdmissionContinuationV1 {
    Intent(DurableOwnerAdmissionIntentV1),
    Plan(DurablePlannedOwnerAdmissionV1),
    Serving(DurableOwnerServingPublicationV1),
}

impl fmt::Debug for DurableOwnerAdmissionContinuationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Intent(_) => "DurableOwnerAdmissionContinuationV1::Intent(<redacted>)",
            Self::Plan(_) => "DurableOwnerAdmissionContinuationV1::Plan(<redacted>)",
            Self::Serving(_) => "DurableOwnerAdmissionContinuationV1::Serving(<redacted>)",
        })
    }
}

/// Result of beginning or resuming one exact durable intent.
pub enum OpenDurableOwnerAdmissionIntentResultV1 {
    Ready(DurableOwnerAdmissionIntentV1),
    NotHeld(OwnerAdmissionAttemptNotHeldV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for OpenDurableOwnerAdmissionIntentResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ready(_) => "OpenDurableOwnerAdmissionIntentResultV1::Ready(<redacted>)",
            Self::NotHeld(_) => "OpenDurableOwnerAdmissionIntentResultV1::NotHeld(<redacted>)",
            Self::RecoveryRequired(_) => {
                "OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Result of resuming one exact durable plan.
enum OpenDurableOwnerAdmissionPlanResultV1 {
    Ready(DurablePlannedOwnerAdmissionV1),
    NotHeld(OwnerAdmissionAttemptNotHeldV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for OpenDurableOwnerAdmissionPlanResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ready(_) => "OpenDurableOwnerAdmissionPlanResultV1::Ready(<redacted>)",
            Self::NotHeld(_) => "OpenDurableOwnerAdmissionPlanResultV1::NotHeld(<redacted>)",
            Self::RecoveryRequired(_) => {
                "OpenDurableOwnerAdmissionPlanResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Restart result. A confirmed durable target is reconciled immediately; it
/// is never returned as a continuation that could mint prepare or commit.
pub enum ResumeOwnerAdmissionResultV1 {
    Reconciled(Box<CoordinateReconcileOwnerAdmissionResultV1>),
    NotHeld(Box<OwnerAdmissionAttemptNotHeldV1>),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

enum ResumePendingRenewResultV1 {
    Recovered(CoordinateRecoverOwnerAdmissionResultV1),
    NotHeld(OwnerAdmissionAttemptNotHeldV1),
}

/// New-allocation result for an exact durable PendingPublish or Serving
/// publication. Neither variant exposes an immediately usable Serving
/// session: a recovered target must cross a fresh exact renewal first.
enum ResumeServingPublicationResultV1 {
    PendingPublishReady(RecoveredPendingPublishReadyV1),
    AwaitingRenew(RecoveredServingAwaitingRenewV1),
    NotHeld(OwnerAdmissionAttemptNotHeldV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

struct RecoveredPendingPublishReadyV1(DurableOwnerServingPublicationV1);

struct RecoveredServingAwaitingRenewV1(OwnerServingSessionV1);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResumeServingPublicationPhaseV1 {
    PendingPublish,
    Serving,
}

impl fmt::Debug for ResumeServingPublicationResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PendingPublishReady(_) => {
                "ResumeServingPublicationResultV1::PendingPublishReady(<redacted>)"
            }
            Self::AwaitingRenew(_) => "ResumeServingPublicationResultV1::AwaitingRenew(<redacted>)",
            Self::NotHeld(failure) => {
                let _ = failure.operation();
                "ResumeServingPublicationResultV1::NotHeld(<redacted>)"
            }
            Self::RecoveryRequired(recovery) => {
                let _ = recovery.cause();
                "ResumeServingPublicationResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

impl fmt::Debug for ResumeOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Reconciled(_) => "ResumeOwnerAdmissionResultV1::Reconciled(<redacted>)",
            Self::NotHeld(_) => "ResumeOwnerAdmissionResultV1::NotHeld(<redacted>)",
            Self::RecoveryRequired(_) => {
                "ResumeOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Prepare completed with a witnessed and durably recorded result.
pub enum CoordinatePrepareOwnerAdmissionResultV1 {
    Prepared {
        continuation: DurablePlannedOwnerAdmissionV1,
        result: PrepareOwnerAdmissionResultV1,
    },
    Recorded {
        continuation: DurableOwnerAdmissionContinuationV1,
        result: PrepareOwnerAdmissionResultV1,
    },
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinatePrepareOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Prepared { .. } => {
                "CoordinatePrepareOwnerAdmissionResultV1::Prepared(<redacted>)"
            }
            Self::Recorded { .. } => {
                "CoordinatePrepareOwnerAdmissionResultV1::Recorded(<redacted>)"
            }
            Self::RecoveryRequired(_) => {
                "CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Exact committed receipt which alone allows a future provider open.
///
/// The permit deliberately owns the same non-cloneable attempt guard. This
/// slice exposes inspection only; a future provider-adoption boundary must
/// consume the permit before it can transfer the guard to serving lifecycle
/// state.
pub struct OwnerAdmissionProviderOpenPermitV1 {
    plan: PlannedOwnerAdmissionV1,
    shard: LogicalShardRecord,
    lease: LogicalShardLease,
    claim: OwnerAdmissionClaimV1,
    validity: OwnerSessionValidityPermitV1,
    kind: OwnerAdmissionCommittedReceiptKindV1,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

/// Test-only monotonic time boundary used by every coordinator deadline cut.
///
/// The trait and its implementations are private so this characterization
/// cannot become a caller-selected production clock surface by accident.
trait SealedMonotonicClockV1: Send + Sync {
    fn now(&self) -> Instant;
}

struct SystemMonotonicClockV1;

impl SealedMonotonicClockV1 for SystemMonotonicClockV1 {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

static SYSTEM_MONOTONIC_CLOCK_V1: SystemMonotonicClockV1 = SystemMonotonicClockV1;

impl OwnerAdmissionProviderOpenPermitV1 {
    pub const fn plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.plan
    }

    pub const fn shard(&self) -> &LogicalShardRecord {
        &self.shard
    }

    pub const fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub const fn claim(&self) -> &OwnerAdmissionClaimV1 {
        &self.claim
    }

    pub const fn validity(&self) -> &OwnerSessionValidityPermitV1 {
        &self.validity
    }

    pub const fn kind(&self) -> OwnerAdmissionCommittedReceiptKindV1 {
        self.kind
    }

    pub fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.attempt.reservation_digest()
    }
}

/// Inspection supplied to the sealed provider-adoption/RootFence boundary.
///
/// It deliberately contains no attempt guard and no consumable permit.
#[derive(Clone, Copy)]
struct ProviderAdoptionRootFenceInspectionV1<'a> {
    plan: &'a PlannedOwnerAdmissionV1,
    shard: &'a LogicalShardRecord,
    lease: &'a LogicalShardLease,
    claim: &'a OwnerAdmissionClaimV1,
}

/// Private proof-producing seam for provider adoption and exact RootFence
/// activation. A crate caller cannot implement or name this trait.
trait SealedProviderAdoptionRootFenceV1: Send + Sync {
    fn adopt_provider_and_activate_root_fence(
        &self,
        inspection: ProviderAdoptionRootFenceInspectionV1<'_>,
    ) -> Result<RecoveryPublication, OwnerAdmissionAttemptPortErrorV1>;
}

/// Exact publication continuation after the sealed provider boundary has
/// consumed the provider-open permit.
pub struct DurableOwnerServingPublicationV1 {
    publication: PlannedOwnerServingPublicationV1,
    validity: Option<OwnerSessionValidityPermitV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

impl DurableOwnerServingPublicationV1 {
    pub const fn publication(&self) -> &PlannedOwnerServingPublicationV1 {
        &self.publication
    }

    pub const fn validity(&self) -> Option<&OwnerSessionValidityPermitV1> {
        self.validity.as_ref()
    }

    pub fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.attempt.reservation_digest()
    }
}

impl fmt::Debug for DurableOwnerServingPublicationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableOwnerServingPublicationV1(<redacted>)")
    }
}

/// Exact locally adopted Serving session. The held guard remains private and
/// can move only through coordinator renewal, reconciliation, or termination.
pub struct OwnerServingSessionV1 {
    publication: PlannedOwnerServingPublicationV1,
    shard: LogicalShardRecord,
    claim: OwnerAdmissionClaimV1,
    validity: OwnerSessionValidityPermitV1,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

impl OwnerServingSessionV1 {
    pub const fn publication(&self) -> &PlannedOwnerServingPublicationV1 {
        &self.publication
    }

    pub const fn shard(&self) -> &LogicalShardRecord {
        &self.shard
    }

    pub const fn claim(&self) -> &OwnerAdmissionClaimV1 {
        &self.claim
    }

    pub const fn validity(&self) -> &OwnerSessionValidityPermitV1 {
        &self.validity
    }

    pub fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.attempt.reservation_digest()
    }
}

impl fmt::Debug for OwnerServingSessionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerServingSessionV1")
            .field("validity_generation", &self.validity.generation())
            .finish_non_exhaustive()
    }
}

enum CoordinateProviderAdoptionResultV1 {
    PublicationReady(DurableOwnerServingPublicationV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

/// Result of the exact Recovering-to-Serving publication continuation.
pub enum CoordinatePublishOwnerServingResultV1 {
    Serving(OwnerServingSessionV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinatePublishOwnerServingResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Serving(_) => "CoordinatePublishOwnerServingResultV1::Serving(<redacted>)",
            Self::RecoveryRequired(_) => {
                "CoordinatePublishOwnerServingResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Result of renewing the same exact Serving session.
pub enum CoordinateRenewOwnerSessionResultV1 {
    Current(OwnerServingSessionV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinateRenewOwnerSessionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Current(_) => "CoordinateRenewOwnerSessionResultV1::Current(<redacted>)",
            Self::RecoveryRequired(_) => {
                "CoordinateRenewOwnerSessionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Process-local proof that one exact owner session is still live.
///
/// Finite observations are anchored to the monotonic instant immediately
/// before dispatching the ultimate control-store call. The deadline is never
/// serialized or reconstructed after restart.
pub struct OwnerSessionValidityPermitV1 {
    observation: OwnerSessionLifetimeObservationV1,
    valid_until: Option<Instant>,
    generation: u64,
}

impl OwnerSessionValidityPermitV1 {
    fn from_observation(
        dispatch_started: Instant,
        observed_at: Instant,
        observation: OwnerSessionLifetimeObservationV1,
        generation: u64,
    ) -> Option<Self> {
        let valid_until = match &observation {
            OwnerSessionLifetimeObservationV1::NonExpiring => None,
            OwnerSessionLifetimeObservationV1::Finite(finite) => {
                let valid_until = dispatch_started
                    .checked_add(Duration::from_secs(finite.observed_ttl_seconds().get()))?;
                if observed_at >= valid_until {
                    return None;
                }
                Some(valid_until)
            }
        };
        Some(Self {
            observation,
            valid_until,
            generation,
        })
    }

    pub const fn observation(&self) -> &OwnerSessionLifetimeObservationV1 {
        &self.observation
    }

    pub const fn valid_until(&self) -> Option<Instant> {
        self.valid_until
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn is_current(&self) -> bool {
        self.is_current_at(SYSTEM_MONOTONIC_CLOCK_V1.now())
    }

    fn is_current_at(&self, now: Instant) -> bool {
        self.valid_until.is_none_or(|valid_until| now < valid_until)
    }
}

impl fmt::Debug for OwnerSessionValidityPermitV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionValidityPermitV1")
            .field("finite", &self.valid_until.is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for OwnerAdmissionProviderOpenPermitV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionProviderOpenPermitV1")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Commit result; only `ProviderOpenPermitted` may cross into provider open.
pub enum CoordinateCommitOwnerAdmissionResultV1 {
    ProviderOpenPermitted(OwnerAdmissionProviderOpenPermitV1),
    Recorded {
        continuation: DurablePlannedOwnerAdmissionV1,
        result: CommitOwnerAdmissionResultV1,
    },
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinateCommitOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ProviderOpenPermitted(_) => {
                "CoordinateCommitOwnerAdmissionResultV1::ProviderOpenPermitted(<redacted>)"
            }
            Self::Recorded { .. } => "CoordinateCommitOwnerAdmissionResultV1::Recorded(<redacted>)",
            Self::RecoveryRequired(_) => {
                "CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

pub enum CoordinateAbortOwnerAdmissionResultV1 {
    Recorded {
        continuation: Box<DurablePlannedOwnerAdmissionV1>,
        result: Box<AbortOwnerAdmissionResultV1>,
    },
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinateAbortOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Recorded { .. } => "CoordinateAbortOwnerAdmissionResultV1::Recorded(<redacted>)",
            Self::RecoveryRequired(_) => {
                "CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

pub enum CoordinateTerminateOwnerAdmissionResultV1 {
    Recorded {
        continuation: Box<DurablePlannedOwnerAdmissionV1>,
        result: Box<TerminateOwnerAdmissionResultV1>,
    },
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinateTerminateOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Recorded { .. } => {
                "CoordinateTerminateOwnerAdmissionResultV1::Recorded(<redacted>)"
            }
            Self::RecoveryRequired(_) => {
                "CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Verified expired-session continuation. Construction requires a witnessed
/// `ExpiredCommitted` reconciliation and a durable exact receipt.
pub struct VerifiedExpiredOwnerAdmissionV1 {
    plan: PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionTerminationReasonV1,
    permit: ReconciledOwnerLeaseExpiryV1,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
}

impl VerifiedExpiredOwnerAdmissionV1 {
    pub fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
        self.attempt.reservation_digest()
    }
}

impl fmt::Debug for VerifiedExpiredOwnerAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerifiedExpiredOwnerAdmissionV1(<redacted>)")
    }
}

pub enum CoordinateReconcileOwnerAdmissionResultV1 {
    ProviderOpenPermitted(Box<OwnerAdmissionProviderOpenPermitV1>),
    PublicationReady(DurableOwnerServingPublicationV1),
    Serving(OwnerServingSessionV1),
    LeaseExpired(Box<VerifiedExpiredOwnerAdmissionV1>),
    Recorded {
        continuation: Box<DurableOwnerAdmissionContinuationV1>,
        result: Box<ReconcileOwnerAdmissionResultV1>,
    },
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinateReconcileOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ProviderOpenPermitted(_) => {
                "CoordinateReconcileOwnerAdmissionResultV1::ProviderOpenPermitted(<redacted>)"
            }
            Self::PublicationReady(_) => {
                "CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(<redacted>)"
            }
            Self::Serving(_) => "CoordinateReconcileOwnerAdmissionResultV1::Serving(<redacted>)",
            Self::LeaseExpired(_) => {
                "CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(<redacted>)"
            }
            Self::Recorded { .. } => {
                "CoordinateReconcileOwnerAdmissionResultV1::Recorded(<redacted>)"
            }
            Self::RecoveryRequired(_) => {
                "CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Closed observation available while an interrupted mutation remains pending.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveredPendingOwnerMutationObservationV1 {
    Prepared = 1,
    ExpiredPrepared = 2,
    Committed = 3,
    ExpiredCommitted = 4,
    TerminalOrConflict = 5,
    Unresolved = 6,
}

/// Opaque exact reconciliation result for an interrupted Abort/Terminate.
///
/// This value deliberately provides no accessor for the reconciled
/// continuation or provider-open permit. It must be consumed by
/// [`OwnerAdmissionCoordinatorV1::continue_pending_mutation`] before the held
/// guard can cross another mutation boundary.
pub struct RecoveredPendingOwnerMutationV1 {
    mutation: OwnerAdmissionPendingMutationV1,
    reconciled: Box<CoordinateReconcileOwnerAdmissionResultV1>,
}

impl RecoveredPendingOwnerMutationV1 {
    pub const fn pending_mutation(&self) -> &OwnerAdmissionPendingMutationV1 {
        &self.mutation
    }

    pub fn observation(&self) -> RecoveredPendingOwnerMutationObservationV1 {
        match self.reconciled.as_ref() {
            CoordinateReconcileOwnerAdmissionResultV1::ProviderOpenPermitted(_) => {
                RecoveredPendingOwnerMutationObservationV1::Committed
            }
            CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(_)
            | CoordinateReconcileOwnerAdmissionResultV1::Serving(_) => {
                RecoveredPendingOwnerMutationObservationV1::Committed
            }
            CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(_) => {
                RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted
            }
            CoordinateReconcileOwnerAdmissionResultV1::Recorded { result, .. } => {
                match result.as_ref() {
                    ReconcileOwnerAdmissionResultV1::Prepared { .. } => {
                        RecoveredPendingOwnerMutationObservationV1::Prepared
                    }
                    ReconcileOwnerAdmissionResultV1::ExpiredPrepared { .. } => {
                        RecoveredPendingOwnerMutationObservationV1::ExpiredPrepared
                    }
                    ReconcileOwnerAdmissionResultV1::Committed { .. } => {
                        RecoveredPendingOwnerMutationObservationV1::Committed
                    }
                    ReconcileOwnerAdmissionResultV1::ExpiredCommitted { .. } => {
                        RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted
                    }
                    ReconcileOwnerAdmissionResultV1::Rejected { .. }
                    | ReconcileOwnerAdmissionResultV1::DurableConflict { .. }
                    | ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. } => {
                        RecoveredPendingOwnerMutationObservationV1::TerminalOrConflict
                    }
                    ReconcileOwnerAdmissionResultV1::DurableInconsistent(_)
                    | ReconcileOwnerAdmissionResultV1::NotDispatched(_)
                    | ReconcileOwnerAdmissionResultV1::OutcomeUnknown(_) => {
                        RecoveredPendingOwnerMutationObservationV1::Unresolved
                    }
                }
            }
            CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(_) => {
                RecoveredPendingOwnerMutationObservationV1::Unresolved
            }
        }
    }

    fn into_plan_continuation(
        self,
    ) -> Result<
        (
            OwnerAdmissionPendingMutationV1,
            DurablePlannedOwnerAdmissionV1,
        ),
        Self,
    > {
        let Self {
            mutation,
            reconciled,
        } = self;
        match *reconciled {
            CoordinateReconcileOwnerAdmissionResultV1::ProviderOpenPermitted(permit) => {
                let permit = *permit;
                let OwnerAdmissionProviderOpenPermitV1 { plan, attempt, .. } = permit;
                Ok((mutation, DurablePlannedOwnerAdmissionV1 { plan, attempt }))
            }
            CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(continuation) => {
                let DurableOwnerServingPublicationV1 {
                    publication,
                    attempt,
                    ..
                } = continuation;
                Ok((
                    mutation,
                    DurablePlannedOwnerAdmissionV1 {
                        plan: publication.plan().clone(),
                        attempt,
                    },
                ))
            }
            CoordinateReconcileOwnerAdmissionResultV1::Serving(session) => {
                let OwnerServingSessionV1 {
                    publication,
                    attempt,
                    ..
                } = session;
                Ok((
                    mutation,
                    DurablePlannedOwnerAdmissionV1 {
                        plan: publication.plan().clone(),
                        attempt,
                    },
                ))
            }
            CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(expired) => {
                let expired = *expired;
                let VerifiedExpiredOwnerAdmissionV1 { plan, attempt, .. } = expired;
                Ok((mutation, DurablePlannedOwnerAdmissionV1 { plan, attempt }))
            }
            CoordinateReconcileOwnerAdmissionResultV1::Recorded {
                continuation,
                result,
            } => match *continuation {
                DurableOwnerAdmissionContinuationV1::Plan(continuation) => {
                    Ok((mutation, continuation))
                }
                DurableOwnerAdmissionContinuationV1::Serving(continuation) => {
                    let DurableOwnerServingPublicationV1 {
                        publication,
                        attempt,
                        ..
                    } = continuation;
                    Ok((
                        mutation,
                        DurablePlannedOwnerAdmissionV1 {
                            plan: publication.plan().clone(),
                            attempt,
                        },
                    ))
                }
                DurableOwnerAdmissionContinuationV1::Intent(continuation) => Err(Self {
                    mutation,
                    reconciled: Box::new(CoordinateReconcileOwnerAdmissionResultV1::Recorded {
                        continuation: Box::new(DurableOwnerAdmissionContinuationV1::Intent(
                            continuation,
                        )),
                        result,
                    }),
                }),
            },
            CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(recovery) => Err(Self {
                mutation,
                reconciled: Box::new(CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                    recovery,
                )),
            }),
        }
    }

    fn into_verified_expiry(
        self,
    ) -> Result<
        (
            OwnerAdmissionPendingMutationV1,
            VerifiedExpiredOwnerAdmissionV1,
        ),
        Self,
    > {
        let Self {
            mutation,
            reconciled,
        } = self;
        match *reconciled {
            CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(expired) => {
                Ok((mutation, *expired))
            }
            reconciled => Err(Self {
                mutation,
                reconciled: Box::new(reconciled),
            }),
        }
    }

    fn into_serving_session(
        self,
    ) -> Result<(OwnerAdmissionPendingMutationV1, OwnerServingSessionV1), Self> {
        let Self {
            mutation,
            reconciled,
        } = self;
        match *reconciled {
            CoordinateReconcileOwnerAdmissionResultV1::Serving(session) => Ok((mutation, session)),
            reconciled => Err(Self {
                mutation,
                reconciled: Box::new(reconciled),
            }),
        }
    }

    fn into_recovery(self) -> OwnerAdmissionRecoveryRequiredV1 {
        let Self {
            mutation,
            reconciled,
        } = self;
        let pending_mutation = Some(mutation);
        let cause = OwnerAdmissionRecoveryCauseV1::PendingMutationUnresolved;
        match *reconciled {
            CoordinateReconcileOwnerAdmissionResultV1::ProviderOpenPermitted(permit) => {
                let permit = *permit;
                let OwnerAdmissionProviderOpenPermitV1 { plan, attempt, .. } = permit;
                recovery_for_plan(
                    plan,
                    OwnerAdmissionKnownDurableStageV1::Plan,
                    cause,
                    None,
                    pending_mutation,
                    attempt,
                )
            }
            CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(continuation) => {
                let DurableOwnerServingPublicationV1 {
                    publication,
                    attempt,
                    ..
                } = continuation;
                let mut recovery = recovery_for_serving(
                    publication,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    cause,
                    None,
                    attempt,
                );
                recovery.pending_mutation = pending_mutation;
                recovery
            }
            CoordinateReconcileOwnerAdmissionResultV1::Serving(session) => {
                let OwnerServingSessionV1 {
                    publication,
                    attempt,
                    ..
                } = session;
                let mut recovery = recovery_for_serving(
                    publication,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    cause,
                    None,
                    attempt,
                );
                recovery.pending_mutation = pending_mutation;
                recovery
            }
            CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(expired) => {
                let expired = *expired;
                let VerifiedExpiredOwnerAdmissionV1 { plan, attempt, .. } = expired;
                recovery_for_plan(
                    plan,
                    OwnerAdmissionKnownDurableStageV1::Plan,
                    cause,
                    None,
                    pending_mutation,
                    attempt,
                )
            }
            CoordinateReconcileOwnerAdmissionResultV1::Recorded { continuation, .. } => {
                match *continuation {
                    DurableOwnerAdmissionContinuationV1::Intent(continuation) => {
                        recovery_for_intent(
                            continuation.intent,
                            OwnerAdmissionKnownDurableStageV1::Intent,
                            cause,
                            None,
                            pending_mutation,
                            continuation.attempt,
                        )
                    }
                    DurableOwnerAdmissionContinuationV1::Plan(continuation) => recovery_for_plan(
                        continuation.plan,
                        OwnerAdmissionKnownDurableStageV1::Plan,
                        cause,
                        None,
                        pending_mutation,
                        continuation.attempt,
                    ),
                    DurableOwnerAdmissionContinuationV1::Serving(continuation) => {
                        let mut recovery = recovery_for_serving(
                            continuation.publication,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            cause,
                            None,
                            continuation.attempt,
                        );
                        recovery.pending_mutation = pending_mutation;
                        recovery
                    }
                }
            }
            CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(mut recovery) => {
                if recovery.pending_mutation.is_none() {
                    recovery.pending_mutation = pending_mutation;
                }
                recovery
            }
        }
    }
}

impl fmt::Debug for RecoveredPendingOwnerMutationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveredPendingOwnerMutationV1")
            .field("mutation", &self.mutation)
            .field("observation", &self.observation())
            .finish_non_exhaustive()
    }
}

/// Result of consuming a recovery object with its original held guard.
///
/// The coordinator never replays an interrupted mutation in `recover`.
/// Pending mutation results remain opaque and cannot expose a provider-open
/// permit or ordinary continuation.
pub enum CoordinateRecoverOwnerAdmissionResultV1 {
    Reconciled(Box<CoordinateReconcileOwnerAdmissionResultV1>),
    PendingMutation(RecoveredPendingOwnerMutationV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for CoordinateRecoverOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Reconciled(_) => {
                "CoordinateRecoverOwnerAdmissionResultV1::Reconciled(<redacted>)"
            }
            Self::PendingMutation(_) => {
                "CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(<redacted>)"
            }
            Self::RecoveryRequired(_) => {
                "CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Result of explicitly continuing an interrupted exact mutation.
pub enum ContinuePendingOwnerMutationResultV1 {
    Abort(CoordinateAbortOwnerAdmissionResultV1),
    Terminate(CoordinateTerminateOwnerAdmissionResultV1),
    Renew(CoordinateRenewOwnerSessionResultV1),
    Terminal(RecoveredPendingOwnerMutationV1),
    RecoveryRequired(OwnerAdmissionRecoveryRequiredV1),
}

impl fmt::Debug for ContinuePendingOwnerMutationResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Abort(_) => "ContinuePendingOwnerMutationResultV1::Abort(<redacted>)",
            Self::Terminate(_) => "ContinuePendingOwnerMutationResultV1::Terminate(<redacted>)",
            Self::Renew(_) => "ContinuePendingOwnerMutationResultV1::Renew(<redacted>)",
            Self::Terminal(_) => "ContinuePendingOwnerMutationResultV1::Terminal(<redacted>)",
            Self::RecoveryRequired(_) => {
                "ContinuePendingOwnerMutationResultV1::RecoveryRequired(<redacted>)"
            }
        })
    }
}

/// Storage-neutral facade for one exact planned-owner attempt.
pub struct OwnerAdmissionCoordinatorV1<'a> {
    control: &'a dyn ControlStore,
    attempts: &'a dyn TrustedOwnerAdmissionAttemptPortV1,
    clock: &'a dyn SealedMonotonicClockV1,
}

impl<'a> OwnerAdmissionCoordinatorV1<'a> {
    pub const fn new(
        control: &'a dyn ControlStore,
        attempts: &'a dyn TrustedOwnerAdmissionAttemptPortV1,
    ) -> Self {
        Self {
            control,
            attempts,
            clock: &SYSTEM_MONOTONIC_CLOCK_V1,
        }
    }

    fn with_clock(
        control: &'a dyn ControlStore,
        attempts: &'a dyn TrustedOwnerAdmissionAttemptPortV1,
        clock: &'a dyn SealedMonotonicClockV1,
    ) -> Self {
        Self {
            control,
            attempts,
            clock,
        }
    }

    /// Acquire a held reservation, exact-bind it, and durably sync the
    /// canonical intent before any prepare command can be minted.
    pub fn begin(&self, intent: OwnerAdmissionIntentV1) -> OpenDurableOwnerAdmissionIntentResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Begin;
        let mut attempt = match self.attempts.acquire(&intent) {
            Ok(attempt) => attempt,
            Err(error) => {
                return OpenDurableOwnerAdmissionIntentResultV1::NotHeld(
                    OwnerAdmissionAttemptNotHeldV1 {
                        intent,
                        operation,
                        error,
                    },
                );
            }
        };
        if !reservation_matches(attempt.as_ref(), &intent) {
            return OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::None,
                OwnerAdmissionRecoveryCauseV1::ReservationBindingMismatch,
                None,
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.validate_held() {
            return OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::None,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        if let Err(error) =
            attempt.persist(OwnerAdmissionDurableUpdateV1::Intent { intent: &intent })
        {
            return OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::None,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        OpenDurableOwnerAdmissionIntentResultV1::Ready(DurableOwnerAdmissionIntentV1 {
            intent,
            attempt,
        })
    }

    /// Reacquire the exact reservation and confirm an already durable intent.
    /// No current owner name, endpoint, or epoch is guessed during resume.
    fn open_resume_intent(
        &self,
        intent: OwnerAdmissionIntentV1,
    ) -> OpenDurableOwnerAdmissionIntentResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::ResumeIntent;
        let mut attempt = match self.attempts.acquire(&intent) {
            Ok(attempt) => attempt,
            Err(error) => {
                return OpenDurableOwnerAdmissionIntentResultV1::NotHeld(
                    OwnerAdmissionAttemptNotHeldV1 {
                        intent,
                        operation,
                        error,
                    },
                );
            }
        };
        if !reservation_matches(attempt.as_ref(), &intent) {
            return OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::None,
                OwnerAdmissionRecoveryCauseV1::ReservationBindingMismatch,
                None,
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.validate_held() {
            return OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::None,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.confirm(OwnerAdmissionDurableTargetV1::Intent(&intent)) {
            return OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::None,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        OpenDurableOwnerAdmissionIntentResultV1::Ready(DurableOwnerAdmissionIntentV1 {
            intent,
            attempt,
        })
    }

    /// Reacquire the exact reservation and confirm an already durable plan.
    fn open_resume_plan(
        &self,
        plan: PlannedOwnerAdmissionV1,
    ) -> OpenDurableOwnerAdmissionPlanResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::ResumePlan;
        let intent = plan.intent().clone();
        let mut attempt = match self.attempts.acquire(&intent) {
            Ok(attempt) => attempt,
            Err(error) => {
                return OpenDurableOwnerAdmissionPlanResultV1::NotHeld(
                    OwnerAdmissionAttemptNotHeldV1 {
                        intent,
                        operation,
                        error,
                    },
                );
            }
        };
        if !reservation_matches(attempt.as_ref(), &intent) {
            return OpenDurableOwnerAdmissionPlanResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::None,
                OwnerAdmissionRecoveryCauseV1::ReservationBindingMismatch,
                None,
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.validate_held() {
            return OpenDurableOwnerAdmissionPlanResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::None,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.confirm(OwnerAdmissionDurableTargetV1::Plan(&plan)) {
            return OpenDurableOwnerAdmissionPlanResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::None,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        OpenDurableOwnerAdmissionPlanResultV1::Ready(DurablePlannedOwnerAdmissionV1 {
            plan,
            attempt,
        })
    }

    /// Restart only from an exact durable intent and reconcile it immediately.
    pub fn resume_intent(&self, intent: OwnerAdmissionIntentV1) -> ResumeOwnerAdmissionResultV1 {
        match self.open_resume_intent(intent) {
            OpenDurableOwnerAdmissionIntentResultV1::Ready(continuation) => {
                ResumeOwnerAdmissionResultV1::Reconciled(Box::new(
                    self.reconcile(DurableOwnerAdmissionContinuationV1::Intent(continuation)),
                ))
            }
            OpenDurableOwnerAdmissionIntentResultV1::NotHeld(failure) => {
                ResumeOwnerAdmissionResultV1::NotHeld(Box::new(failure))
            }
            OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery) => {
                ResumeOwnerAdmissionResultV1::RecoveryRequired(recovery)
            }
        }
    }

    /// Restart only from an exact durable plan and reconcile it immediately.
    pub fn resume_plan(&self, plan: PlannedOwnerAdmissionV1) -> ResumeOwnerAdmissionResultV1 {
        match self.open_resume_plan(plan) {
            OpenDurableOwnerAdmissionPlanResultV1::Ready(continuation) => {
                ResumeOwnerAdmissionResultV1::Reconciled(Box::new(
                    self.reconcile(DurableOwnerAdmissionContinuationV1::Plan(continuation)),
                ))
            }
            OpenDurableOwnerAdmissionPlanResultV1::NotHeld(failure) => {
                ResumeOwnerAdmissionResultV1::NotHeld(Box::new(failure))
            }
            OpenDurableOwnerAdmissionPlanResultV1::RecoveryRequired(recovery) => {
                ResumeOwnerAdmissionResultV1::RecoveryRequired(recovery)
            }
        }
    }

    /// Reopen one exact durable PendingPublish on a new allocation. A source
    /// observation can only return the opaque publish continuation; a target
    /// observation remains opaque until a fresh exact renewal succeeds.
    fn resume_pending_publish(
        &self,
        publication: PlannedOwnerServingPublicationV1,
    ) -> ResumeServingPublicationResultV1 {
        self.resume_serving_publication(
            publication,
            ResumeServingPublicationPhaseV1::PendingPublish,
            OwnerAdmissionCoordinatorOperationV1::ResumePendingPublish,
        )
    }

    /// Reopen one exact durable Serving publication on a new allocation. The
    /// reconciled control observation is not a working session; the only
    /// continuation is a fresh exact renewal on the same held guard.
    fn resume_serving(
        &self,
        publication: PlannedOwnerServingPublicationV1,
    ) -> ResumeServingPublicationResultV1 {
        self.resume_serving_publication(
            publication,
            ResumeServingPublicationPhaseV1::Serving,
            OwnerAdmissionCoordinatorOperationV1::ResumeServing,
        )
    }

    fn resume_serving_publication(
        &self,
        publication: PlannedOwnerServingPublicationV1,
        phase: ResumeServingPublicationPhaseV1,
        operation: OwnerAdmissionCoordinatorOperationV1,
    ) -> ResumeServingPublicationResultV1 {
        let intent = publication.plan().intent().clone();
        if publication.validate().is_err() {
            return ResumeServingPublicationResultV1::NotHeld(OwnerAdmissionAttemptNotHeldV1 {
                intent,
                operation,
                error: OwnerAdmissionAttemptPortErrorV1::DurableRecordInconsistent,
            });
        }
        let mut attempt = match self.attempts.acquire(&intent) {
            Ok(attempt) => attempt,
            Err(error) => {
                return ResumeServingPublicationResultV1::NotHeld(OwnerAdmissionAttemptNotHeldV1 {
                    intent,
                    operation,
                    error,
                });
            }
        };
        if !reservation_matches(attempt.as_ref(), &intent) {
            return ResumeServingPublicationResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                OwnerAdmissionRecoveryCauseV1::ReservationBindingMismatch,
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.validate_held() {
            return ResumeServingPublicationResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                port_cause(operation, error),
                None,
                attempt,
            ));
        }
        let durable_target = match phase {
            ResumeServingPublicationPhaseV1::PendingPublish => {
                OwnerAdmissionDurableTargetV1::PendingPublish(&publication)
            }
            ResumeServingPublicationPhaseV1::Serving => {
                OwnerAdmissionDurableTargetV1::Serving(&publication)
            }
        };
        if let Err(error) = attempt.confirm(durable_target) {
            return ResumeServingPublicationResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                port_cause(operation, error),
                None,
                attempt,
            ));
        }

        let expected_publication = publication.clone();
        match self.reconcile_with_serving_phase(
            DurableOwnerAdmissionContinuationV1::Serving(DurableOwnerServingPublicationV1 {
                publication,
                validity: None,
                attempt,
            }),
            phase,
        ) {
            CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(continuation)
                if phase == ResumeServingPublicationPhaseV1::PendingPublish =>
            {
                ResumeServingPublicationResultV1::PendingPublishReady(
                    RecoveredPendingPublishReadyV1(continuation),
                )
            }
            CoordinateReconcileOwnerAdmissionResultV1::Serving(session) => {
                ResumeServingPublicationResultV1::AwaitingRenew(RecoveredServingAwaitingRenewV1(
                    session,
                ))
            }
            CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(recovery) => {
                ResumeServingPublicationResultV1::RecoveryRequired(recovery)
            }
            reconciled => ResumeServingPublicationResultV1::RecoveryRequired(
                recovery_from_unexpected_serving_reconcile(
                    expected_publication,
                    operation,
                    reconciled,
                ),
            ),
        }
    }

    /// Continue a restarted PendingPublish without exposing its intermediate
    /// published session. A successful publication is immediately fenced by a
    /// durable PendingRenew and fresh exact renewal.
    fn continue_recovered_pending_publish(
        &self,
        recovered: RecoveredPendingPublishReadyV1,
    ) -> CoordinateRenewOwnerSessionResultV1 {
        match self.publish_serving(recovered.0) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => self.renew(session),
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => {
                CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery)
            }
        }
    }

    fn renew_recovered_serving(
        &self,
        recovered: RecoveredServingAwaitingRenewV1,
    ) -> CoordinateRenewOwnerSessionResultV1 {
        self.renew(recovered.0)
    }

    /// Restart only from an exact durable PendingRenew record. The new
    /// allocation reacquires the same reservation binding, confirms the exact
    /// publication and renewal target, then reconciles before any fresh
    /// keepalive can be dispatched.
    fn resume_pending_renew(
        &self,
        publication: PlannedOwnerServingPublicationV1,
        target: OwnerSessionRenewalTargetV1,
        generation: u64,
    ) -> ResumePendingRenewResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::ResumePendingRenew;
        let intent = publication.plan().intent().clone();
        if publication.validate().is_err()
            || target.validate().is_err()
            || target.plan() != publication.plan()
            || generation <= 1
        {
            return ResumePendingRenewResultV1::NotHeld(OwnerAdmissionAttemptNotHeldV1 {
                intent,
                operation,
                error: OwnerAdmissionAttemptPortErrorV1::DurableRecordInconsistent,
            });
        }
        let mut attempt = match self.attempts.acquire(&intent) {
            Ok(attempt) => attempt,
            Err(error) => {
                return ResumePendingRenewResultV1::NotHeld(OwnerAdmissionAttemptNotHeldV1 {
                    intent,
                    operation,
                    error,
                });
            }
        };
        if !reservation_matches(attempt.as_ref(), &intent) {
            return ResumePendingRenewResultV1::Recovered(
                CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                    recovery_for_pending_renew(
                        publication,
                        target,
                        generation,
                        OwnerAdmissionKnownDurableStageV1::ServingPublication,
                        OwnerAdmissionRecoveryCauseV1::ReservationBindingMismatch,
                        None,
                        attempt,
                    ),
                ),
            );
        }
        if let Err(error) = attempt.validate_held() {
            return ResumePendingRenewResultV1::Recovered(
                CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                    recovery_for_pending_renew(
                        publication,
                        target,
                        generation,
                        OwnerAdmissionKnownDurableStageV1::ServingPublication,
                        port_cause(operation, error),
                        None,
                        attempt,
                    ),
                ),
            );
        }
        if let Err(error) = attempt.confirm(OwnerAdmissionDurableTargetV1::PendingRenew {
            publication: &publication,
            target: &target,
            generation,
        }) {
            return ResumePendingRenewResultV1::Recovered(
                CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                    recovery_for_pending_renew(
                        publication,
                        target,
                        generation,
                        OwnerAdmissionKnownDurableStageV1::ServingPublication,
                        port_cause(operation, error),
                        None,
                        attempt,
                    ),
                ),
            );
        }
        ResumePendingRenewResultV1::Recovered(self.recover(recovery_for_pending_renew(
            publication,
            target,
            generation,
            OwnerAdmissionKnownDurableStageV1::ServingPublication,
            OwnerAdmissionRecoveryCauseV1::PendingMutationUnresolved,
            None,
            attempt,
        )))
    }
}

fn reservation_matches(
    attempt: &dyn TrustedOwnerAdmissionAttemptV1,
    intent: &OwnerAdmissionIntentV1,
) -> bool {
    attempt.reservation_digest() == intent.reservation_digest()
}

fn recovery_from_unexpected_serving_reconcile(
    publication: PlannedOwnerServingPublicationV1,
    operation: OwnerAdmissionCoordinatorOperationV1,
    reconciled: CoordinateReconcileOwnerAdmissionResultV1,
) -> OwnerAdmissionRecoveryRequiredV1 {
    let attempt = match reconciled {
        CoordinateReconcileOwnerAdmissionResultV1::ProviderOpenPermitted(permit) => {
            let OwnerAdmissionProviderOpenPermitV1 { attempt, .. } = *permit;
            attempt
        }
        CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(continuation) => {
            continuation.attempt
        }
        CoordinateReconcileOwnerAdmissionResultV1::Serving(session) => session.attempt,
        CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(expired) => {
            let VerifiedExpiredOwnerAdmissionV1 { attempt, .. } = *expired;
            attempt
        }
        CoordinateReconcileOwnerAdmissionResultV1::Recorded { continuation, .. } => {
            match *continuation {
                DurableOwnerAdmissionContinuationV1::Intent(continuation) => continuation.attempt,
                DurableOwnerAdmissionContinuationV1::Plan(continuation) => continuation.attempt,
                DurableOwnerAdmissionContinuationV1::Serving(continuation) => continuation.attempt,
            }
        }
        CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(recovery) => return recovery,
    };
    recovery_for_serving(
        publication,
        OwnerAdmissionKnownDurableStageV1::ServingPublication,
        OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
        None,
        attempt,
    )
}

fn port_cause(
    operation: OwnerAdmissionCoordinatorOperationV1,
    error: OwnerAdmissionAttemptPortErrorV1,
) -> OwnerAdmissionRecoveryCauseV1 {
    OwnerAdmissionRecoveryCauseV1::AttemptPort { operation, error }
}

fn recovery_for_intent(
    intent: OwnerAdmissionIntentV1,
    known_durable_stage: OwnerAdmissionKnownDurableStageV1,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    pending_mutation: Option<OwnerAdmissionPendingMutationV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> OwnerAdmissionRecoveryRequiredV1 {
    OwnerAdmissionRecoveryRequiredV1 {
        intent: Box::new(intent),
        observed_plan: None,
        observed_serving_publication: None,
        known_durable_stage,
        cause,
        recording_error,
        pending_mutation,
        attempt,
    }
}

fn recovery_for_plan(
    plan: PlannedOwnerAdmissionV1,
    known_durable_stage: OwnerAdmissionKnownDurableStageV1,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    pending_mutation: Option<OwnerAdmissionPendingMutationV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> OwnerAdmissionRecoveryRequiredV1 {
    OwnerAdmissionRecoveryRequiredV1 {
        intent: Box::new(plan.intent().clone()),
        observed_plan: Some(Box::new(plan)),
        observed_serving_publication: None,
        known_durable_stage,
        cause,
        recording_error,
        pending_mutation,
        attempt,
    }
}

fn recovery_after_observed_prepare(
    intent: OwnerAdmissionIntentV1,
    observed_plan: Option<PlannedOwnerAdmissionV1>,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> OwnerAdmissionRecoveryRequiredV1 {
    OwnerAdmissionRecoveryRequiredV1 {
        intent: Box::new(intent),
        observed_plan: observed_plan.map(Box::new),
        observed_serving_publication: None,
        known_durable_stage: OwnerAdmissionKnownDurableStageV1::Intent,
        cause,
        recording_error,
        pending_mutation: None,
        attempt,
    }
}

fn recovery_for_serving(
    publication: PlannedOwnerServingPublicationV1,
    known_durable_stage: OwnerAdmissionKnownDurableStageV1,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> OwnerAdmissionRecoveryRequiredV1 {
    OwnerAdmissionRecoveryRequiredV1 {
        intent: Box::new(publication.plan().intent().clone()),
        observed_plan: Some(Box::new(publication.plan().clone())),
        observed_serving_publication: Some(Box::new(publication)),
        known_durable_stage,
        cause,
        recording_error,
        pending_mutation: None,
        attempt,
    }
}

fn recovery_for_pending_renew(
    publication: PlannedOwnerServingPublicationV1,
    target: OwnerSessionRenewalTargetV1,
    generation: u64,
    known_durable_stage: OwnerAdmissionKnownDurableStageV1,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> OwnerAdmissionRecoveryRequiredV1 {
    let mut recovery = recovery_for_serving(
        publication.clone(),
        known_durable_stage,
        cause,
        recording_error,
        attempt,
    );
    recovery.pending_mutation = Some(OwnerAdmissionPendingMutationV1::Renew {
        publication,
        target,
        generation,
    });
    recovery
}

fn observed_prepare_plan(
    result: &PrepareOwnerAdmissionResultV1,
) -> Option<PlannedOwnerAdmissionV1> {
    match result {
        PrepareOwnerAdmissionResultV1::Prepared { plan, .. }
        | PrepareOwnerAdmissionResultV1::ExpiredPrepared { plan, .. }
        | PrepareOwnerAdmissionResultV1::DurableConflict { plan, .. } => Some((**plan).clone()),
        PrepareOwnerAdmissionResultV1::Rejected { .. }
        | PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
        | PrepareOwnerAdmissionResultV1::DurableInconsistent(_)
        | PrepareOwnerAdmissionResultV1::NotDispatched(_)
        | PrepareOwnerAdmissionResultV1::OutcomeUnknown(_) => None,
    }
}

fn prepare_update<'a>(
    intent: &'a OwnerAdmissionIntentV1,
    result: &'a PrepareOwnerAdmissionResultV1,
) -> OwnerAdmissionDurableUpdateV1<'a> {
    match result {
        PrepareOwnerAdmissionResultV1::Prepared {
            plan,
            claim,
            sentinel,
        } => OwnerAdmissionDurableUpdateV1::Prepared {
            plan,
            claim,
            sentinel,
        },
        PrepareOwnerAdmissionResultV1::ExpiredPrepared {
            plan,
            claim,
            expected_sentinel,
        } => OwnerAdmissionDurableUpdateV1::ExpiredPrepared {
            plan,
            claim,
            expected_sentinel,
        },
        PrepareOwnerAdmissionResultV1::Rejected { claim } => {
            OwnerAdmissionDurableUpdateV1::Rejected { intent, claim }
        }
        PrepareOwnerAdmissionResultV1::DurableConflict { .. }
        | PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
        | PrepareOwnerAdmissionResultV1::DurableInconsistent(_)
        | PrepareOwnerAdmissionResultV1::NotDispatched(_)
        | PrepareOwnerAdmissionResultV1::OutcomeUnknown(_) => {
            OwnerAdmissionDurableUpdateV1::PrepareClosed { intent, result }
        }
    }
}

impl OwnerAdmissionCoordinatorV1<'_> {
    /// Execute prepare only after an exact intent is durably synced.
    pub fn prepare(
        &self,
        continuation: DurableOwnerAdmissionIntentV1,
    ) -> CoordinatePrepareOwnerAdmissionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Prepare;
        let DurableOwnerAdmissionIntentV1 {
            intent,
            mut attempt,
        } = continuation;
        if let Err(error) = attempt.validate_held() {
            return CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(recovery_for_intent(
                intent,
                OwnerAdmissionKnownDurableStageV1::Intent,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }

        let (command, witness) = mint_prepare_owner_admission_command(intent.clone());
        let resolved = witness.resolve(self.control.prepare_owner_admission(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure.code());
                let recovery_intent = failure.into_recovery_intent();
                if let Err(error) = held_after {
                    return CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_intent(
                            recovery_intent,
                            OwnerAdmissionKnownDurableStageV1::Intent,
                            port_cause(operation, error),
                            None,
                            None,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: OwnerAdmissionDurableTargetV1::Intent(&recovery_intent),
                        witness_failure: code,
                    })
                    .err();
                CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(recovery_for_intent(
                    recovery_intent,
                    OwnerAdmissionKnownDurableStageV1::Intent,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    None,
                    attempt,
                ))
            }
            Ok(result) => {
                let observed_plan = observed_prepare_plan(&result);
                if let Err(error) = held_after {
                    return CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_after_observed_prepare(
                            intent,
                            observed_plan,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                if let Err(error) = attempt.persist(prepare_update(&intent, &result)) {
                    return CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_after_observed_prepare(
                            intent,
                            observed_plan,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }

                if let PrepareOwnerAdmissionResultV1::Prepared { plan, .. } = &result {
                    return CoordinatePrepareOwnerAdmissionResultV1::Prepared {
                        continuation: DurablePlannedOwnerAdmissionV1 {
                            plan: (**plan).clone(),
                            attempt,
                        },
                        result,
                    };
                }
                if matches!(result, PrepareOwnerAdmissionResultV1::OutcomeUnknown(_)) {
                    return CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_intent(
                            intent,
                            OwnerAdmissionKnownDurableStageV1::Intent,
                            OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation },
                            None,
                            None,
                            attempt,
                        ),
                    );
                }
                CoordinatePrepareOwnerAdmissionResultV1::Recorded {
                    continuation: DurableOwnerAdmissionContinuationV1::Intent(
                        DurableOwnerAdmissionIntentV1 { intent, attempt },
                    ),
                    result,
                }
            }
        }
    }
}

fn durable_target(
    target: &ReconcileOwnerAdmissionTargetV1,
    serving_phase: ResumeServingPublicationPhaseV1,
) -> OwnerAdmissionDurableTargetV1<'_> {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            OwnerAdmissionDurableTargetV1::Intent(intent)
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => {
            OwnerAdmissionDurableTargetV1::Plan(plan)
        }
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => match serving_phase {
            ResumeServingPublicationPhaseV1::PendingPublish => {
                OwnerAdmissionDurableTargetV1::PendingPublish(publication)
            }
            ResumeServingPublicationPhaseV1::Serving => {
                OwnerAdmissionDurableTargetV1::Serving(publication)
            }
        },
    }
}

fn recovery_for_target(
    target: ReconcileOwnerAdmissionTargetV1,
    observed_plan: Option<PlannedOwnerAdmissionV1>,
    cause: OwnerAdmissionRecoveryCauseV1,
    recording_error: Option<OwnerAdmissionAttemptPortErrorV1>,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> OwnerAdmissionRecoveryRequiredV1 {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => OwnerAdmissionRecoveryRequiredV1 {
            intent: Box::new(intent),
            observed_plan: observed_plan.map(Box::new),
            observed_serving_publication: None,
            known_durable_stage: OwnerAdmissionKnownDurableStageV1::Intent,
            cause,
            recording_error,
            pending_mutation: None,
            attempt,
        },
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => recovery_for_plan(
            plan,
            OwnerAdmissionKnownDurableStageV1::Plan,
            cause,
            recording_error,
            None,
            attempt,
        ),
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => recovery_for_serving(
            publication,
            OwnerAdmissionKnownDurableStageV1::ServingPublication,
            cause,
            recording_error,
            attempt,
        ),
    }
}

fn observed_reconcile_plan(
    result: &ReconcileOwnerAdmissionResultV1,
) -> Option<PlannedOwnerAdmissionV1> {
    match result {
        ReconcileOwnerAdmissionResultV1::Prepared { plan, .. }
        | ReconcileOwnerAdmissionResultV1::ExpiredPrepared { plan, .. }
        | ReconcileOwnerAdmissionResultV1::Committed { plan, .. }
        | ReconcileOwnerAdmissionResultV1::ExpiredCommitted { plan, .. }
        | ReconcileOwnerAdmissionResultV1::DurableConflict { plan, .. } => Some((**plan).clone()),
        ReconcileOwnerAdmissionResultV1::Rejected { .. }
        | ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
        | ReconcileOwnerAdmissionResultV1::DurableInconsistent(_)
        | ReconcileOwnerAdmissionResultV1::NotDispatched(_)
        | ReconcileOwnerAdmissionResultV1::OutcomeUnknown(_) => None,
    }
}

fn reconcile_update<'a>(
    target: &'a ReconcileOwnerAdmissionTargetV1,
    result: &'a ReconcileOwnerAdmissionResultV1,
    serving_phase: ResumeServingPublicationPhaseV1,
) -> OwnerAdmissionDurableUpdateV1<'a> {
    match result {
        ReconcileOwnerAdmissionResultV1::Rejected { claim } => {
            OwnerAdmissionDurableUpdateV1::Rejected {
                intent: match target {
                    ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => intent,
                    ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => plan.intent(),
                    ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
                        publication.plan().intent()
                    }
                },
                claim,
            }
        }
        ReconcileOwnerAdmissionResultV1::Prepared {
            plan,
            claim,
            sentinel,
        } => OwnerAdmissionDurableUpdateV1::Prepared {
            plan,
            claim,
            sentinel,
        },
        ReconcileOwnerAdmissionResultV1::ExpiredPrepared {
            plan,
            claim,
            expected_sentinel,
        } => OwnerAdmissionDurableUpdateV1::ExpiredPrepared {
            plan,
            claim,
            expected_sentinel,
        },
        ReconcileOwnerAdmissionResultV1::Committed {
            plan,
            shard,
            lease,
            claim,
            lifetime: _,
        } => match target {
            ReconcileOwnerAdmissionTargetV1::ExactServing(publication)
                if shard == publication.target() =>
            {
                OwnerAdmissionDurableUpdateV1::ServingPublished {
                    kind: OwnerServingPublishedReceiptKindV1::Reconciled,
                    publication,
                    shard,
                    claim,
                }
            }
            ReconcileOwnerAdmissionTargetV1::ExactServing(_) => {
                OwnerAdmissionDurableUpdateV1::ReconcileClosed {
                    target: durable_target(target, serving_phase),
                    result,
                }
            }
            ReconcileOwnerAdmissionTargetV1::IntentOnly(_)
            | ReconcileOwnerAdmissionTargetV1::ExactPlan(_) => {
                OwnerAdmissionDurableUpdateV1::Committed {
                    kind: OwnerAdmissionCommittedReceiptKindV1::Reconciled,
                    plan,
                    shard,
                    lease,
                    claim,
                }
            }
        },
        ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
            plan,
            shard,
            claim,
            expected_session,
            evidence_digest,
        } => OwnerAdmissionDurableUpdateV1::ExpiredCommitted {
            plan,
            shard,
            claim,
            expected_session,
            evidence_digest: *evidence_digest,
        },
        ReconcileOwnerAdmissionResultV1::DurableConflict { .. }
        | ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
        | ReconcileOwnerAdmissionResultV1::DurableInconsistent(_)
        | ReconcileOwnerAdmissionResultV1::NotDispatched(_)
        | ReconcileOwnerAdmissionResultV1::OutcomeUnknown(_) => {
            OwnerAdmissionDurableUpdateV1::ReconcileClosed {
                target: durable_target(target, serving_phase),
                result,
            }
        }
    }
}

impl OwnerAdmissionCoordinatorV1<'_> {
    /// Continue exact recovery with the same held attempt guard.
    ///
    /// This method never reacquires the reservation, creates a new
    /// incarnation, or replays prepare/commit. It reconciles only the last
    /// positively durable Intent or Plan. When the original canonical Intent
    /// write had an unknown outcome, it exact-idempotently persists that same
    /// Intent again before reconciliation. Any failure retains the same guard
    /// and remains fail closed.
    pub fn recover(
        &self,
        recovery: OwnerAdmissionRecoveryRequiredV1,
    ) -> CoordinateRecoverOwnerAdmissionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Reconcile;
        let OwnerAdmissionRecoveryRequiredV1 {
            intent,
            observed_plan,
            observed_serving_publication,
            known_durable_stage,
            cause: _,
            recording_error,
            pending_mutation,
            mut attempt,
        } = recovery;
        let intent = *intent;
        let observed_plan = observed_plan.map(|plan| *plan);
        let observed_serving_publication =
            observed_serving_publication.map(|publication| *publication);

        if let Err(error) = attempt.validate_held() {
            return CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                OwnerAdmissionRecoveryRequiredV1 {
                    intent: Box::new(intent),
                    observed_plan: observed_plan.map(Box::new),
                    observed_serving_publication: observed_serving_publication.map(Box::new),
                    known_durable_stage,
                    cause: port_cause(operation, error),
                    recording_error,
                    pending_mutation,
                    attempt,
                },
            );
        }

        let continuation = if let Some(publication) = observed_serving_publication {
            if known_durable_stage != OwnerAdmissionKnownDurableStageV1::ServingPublication {
                if let Err(error) = attempt.persist(OwnerAdmissionDurableUpdateV1::PendingPublish {
                    publication: &publication,
                }) {
                    return CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_serving(
                            publication,
                            known_durable_stage,
                            port_cause(operation, error),
                            recording_error,
                            attempt,
                        ),
                    );
                }
            }
            DurableOwnerAdmissionContinuationV1::Serving(DurableOwnerServingPublicationV1 {
                publication,
                validity: None,
                attempt,
            })
        } else {
            match known_durable_stage {
                OwnerAdmissionKnownDurableStageV1::Plan => {
                    let Some(plan) = observed_plan else {
                        return CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                            OwnerAdmissionRecoveryRequiredV1 {
                                intent: Box::new(intent),
                                observed_plan: None,
                                observed_serving_publication: None,
                                known_durable_stage,
                                cause: port_cause(
                                    operation,
                                    OwnerAdmissionAttemptPortErrorV1::DurableRecordInconsistent,
                                ),
                                recording_error,
                                pending_mutation,
                                attempt,
                            },
                        );
                    };
                    DurableOwnerAdmissionContinuationV1::Plan(DurablePlannedOwnerAdmissionV1 {
                        plan,
                        attempt,
                    })
                }
                OwnerAdmissionKnownDurableStageV1::Intent => {
                    DurableOwnerAdmissionContinuationV1::Intent(DurableOwnerAdmissionIntentV1 {
                        intent,
                        attempt,
                    })
                }
                OwnerAdmissionKnownDurableStageV1::None => {
                    if let Err(error) =
                        attempt.persist(OwnerAdmissionDurableUpdateV1::Intent { intent: &intent })
                    {
                        return CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                            OwnerAdmissionRecoveryRequiredV1 {
                                intent: Box::new(intent),
                                observed_plan: observed_plan.map(Box::new),
                                observed_serving_publication: observed_serving_publication
                                    .map(Box::new),
                                known_durable_stage,
                                cause: port_cause(operation, error),
                                recording_error,
                                pending_mutation,
                                attempt,
                            },
                        );
                    }
                    DurableOwnerAdmissionContinuationV1::Intent(DurableOwnerAdmissionIntentV1 {
                        intent,
                        attempt,
                    })
                }
                OwnerAdmissionKnownDurableStageV1::ServingPublication => {
                    return CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(
                        OwnerAdmissionRecoveryRequiredV1 {
                            intent: Box::new(intent),
                            observed_plan: observed_plan.map(Box::new),
                            observed_serving_publication: None,
                            known_durable_stage,
                            cause: port_cause(
                                operation,
                                OwnerAdmissionAttemptPortErrorV1::DurableRecordInconsistent,
                            ),
                            recording_error,
                            pending_mutation,
                            attempt,
                        },
                    );
                }
            }
        };

        match self.reconcile(continuation) {
            CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(mut next) => {
                if next.pending_mutation.is_none() {
                    next.pending_mutation = pending_mutation;
                }
                CoordinateRecoverOwnerAdmissionResultV1::RecoveryRequired(next)
            }
            result => match pending_mutation {
                Some(mutation) => CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(
                    RecoveredPendingOwnerMutationV1 {
                        mutation,
                        reconciled: Box::new(result),
                    },
                ),
                None => CoordinateRecoverOwnerAdmissionResultV1::Reconciled(Box::new(result)),
            },
        }
    }

    /// Explicitly continue the exact mutation retained by `recover`.
    ///
    /// This is the only method that can consume a recovered pending mutation.
    /// It reuses the same held guard and never exposes the internally observed
    /// provider-open permit or ordinary continuation. Abort is retried only
    /// while the exact plan remains Prepared; Terminate is retried only for a
    /// compatible exact plan, and LeaseExpired additionally requires a newly
    /// verified expiry permit from reconciliation.
    pub fn continue_pending_mutation(
        &self,
        pending: RecoveredPendingOwnerMutationV1,
    ) -> ContinuePendingOwnerMutationResultV1 {
        let observation = pending.observation();
        match pending.pending_mutation().clone() {
            OwnerAdmissionPendingMutationV1::Abort(reason) => match observation {
                RecoveredPendingOwnerMutationObservationV1::Prepared
                | RecoveredPendingOwnerMutationObservationV1::ExpiredPrepared => {
                    match pending.into_plan_continuation() {
                        Ok((_mutation, continuation)) => {
                            ContinuePendingOwnerMutationResultV1::Abort(
                                self.abort(continuation, reason),
                            )
                        }
                        Err(pending) => ContinuePendingOwnerMutationResultV1::RecoveryRequired(
                            pending.into_recovery(),
                        ),
                    }
                }
                RecoveredPendingOwnerMutationObservationV1::Unresolved => {
                    ContinuePendingOwnerMutationResultV1::RecoveryRequired(pending.into_recovery())
                }
                RecoveredPendingOwnerMutationObservationV1::Committed
                | RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted
                | RecoveredPendingOwnerMutationObservationV1::TerminalOrConflict => {
                    ContinuePendingOwnerMutationResultV1::Terminal(pending)
                }
            },
            OwnerAdmissionPendingMutationV1::Terminate(reason) => match (&reason, observation) {
                (_, RecoveredPendingOwnerMutationObservationV1::Unresolved) => {
                    ContinuePendingOwnerMutationResultV1::RecoveryRequired(pending.into_recovery())
                }
                (_, RecoveredPendingOwnerMutationObservationV1::TerminalOrConflict) => {
                    ContinuePendingOwnerMutationResultV1::Terminal(pending)
                }
                (
                    OwnerAdmissionTerminationReasonV1::LeaseExpired { .. },
                    RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted,
                ) => match pending.into_verified_expiry() {
                    Ok((_mutation, continuation)) => {
                        ContinuePendingOwnerMutationResultV1::Terminate(
                            self.terminate_expired(continuation),
                        )
                    }
                    Err(pending) => ContinuePendingOwnerMutationResultV1::RecoveryRequired(
                        pending.into_recovery(),
                    ),
                },
                (OwnerAdmissionTerminationReasonV1::LeaseExpired { .. }, _) => {
                    ContinuePendingOwnerMutationResultV1::Terminal(pending)
                }
                (
                    OwnerAdmissionTerminationReasonV1::Released,
                    RecoveredPendingOwnerMutationObservationV1::Prepared
                    | RecoveredPendingOwnerMutationObservationV1::ExpiredPrepared
                    | RecoveredPendingOwnerMutationObservationV1::Committed
                    | RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted,
                ) => match pending.into_plan_continuation() {
                    Ok((_mutation, continuation)) => {
                        ContinuePendingOwnerMutationResultV1::Terminate(
                            self.terminate_released(continuation),
                        )
                    }
                    Err(pending) => ContinuePendingOwnerMutationResultV1::RecoveryRequired(
                        pending.into_recovery(),
                    ),
                },
                (
                    OwnerAdmissionTerminationReasonV1::AuthorityCutover { migration_id },
                    RecoveredPendingOwnerMutationObservationV1::Prepared
                    | RecoveredPendingOwnerMutationObservationV1::ExpiredPrepared
                    | RecoveredPendingOwnerMutationObservationV1::Committed
                    | RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted,
                ) => match pending.into_plan_continuation() {
                    Ok((_mutation, continuation)) => {
                        ContinuePendingOwnerMutationResultV1::Terminate(
                            self.terminate_authority_cutover(continuation, *migration_id),
                        )
                    }
                    Err(pending) => ContinuePendingOwnerMutationResultV1::RecoveryRequired(
                        pending.into_recovery(),
                    ),
                },
            },
            OwnerAdmissionPendingMutationV1::Renew {
                publication,
                target,
                generation,
            } => match observation {
                RecoveredPendingOwnerMutationObservationV1::Committed => {
                    match pending.into_serving_session() {
                        Ok((_mutation, mut session))
                            if session.publication() == &publication
                                && session.claim() == target.claim()
                                && session.publication().plan() == target.plan()
                                && generation > 1 =>
                        {
                            session.validity.generation = generation - 1;
                            ContinuePendingOwnerMutationResultV1::Renew(self.renew(session))
                        }
                        Ok((_mutation, session)) => {
                            let OwnerServingSessionV1 { attempt, .. } = session;
                            ContinuePendingOwnerMutationResultV1::RecoveryRequired(
                                recovery_for_pending_renew(
                                    publication,
                                    target,
                                    generation,
                                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                    OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch {
                                        operation: OwnerAdmissionCoordinatorOperationV1::Renew,
                                    },
                                    None,
                                    attempt,
                                ),
                            )
                        }
                        Err(pending) => ContinuePendingOwnerMutationResultV1::RecoveryRequired(
                            pending.into_recovery(),
                        ),
                    }
                }
                RecoveredPendingOwnerMutationObservationV1::Prepared
                | RecoveredPendingOwnerMutationObservationV1::ExpiredPrepared
                | RecoveredPendingOwnerMutationObservationV1::ExpiredCommitted
                | RecoveredPendingOwnerMutationObservationV1::TerminalOrConflict
                | RecoveredPendingOwnerMutationObservationV1::Unresolved => {
                    ContinuePendingOwnerMutationResultV1::RecoveryRequired(pending.into_recovery())
                }
            },
        }
    }

    pub fn reconcile(
        &self,
        continuation: DurableOwnerAdmissionContinuationV1,
    ) -> CoordinateReconcileOwnerAdmissionResultV1 {
        self.reconcile_with_serving_phase(continuation, ResumeServingPublicationPhaseV1::Serving)
    }

    fn reconcile_with_serving_phase(
        &self,
        continuation: DurableOwnerAdmissionContinuationV1,
        serving_phase: ResumeServingPublicationPhaseV1,
    ) -> CoordinateReconcileOwnerAdmissionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Reconcile;
        let (target, mut attempt) = match continuation {
            DurableOwnerAdmissionContinuationV1::Intent(continuation) => (
                ReconcileOwnerAdmissionTargetV1::IntentOnly(continuation.intent),
                continuation.attempt,
            ),
            DurableOwnerAdmissionContinuationV1::Plan(continuation) => (
                ReconcileOwnerAdmissionTargetV1::ExactPlan(continuation.plan),
                continuation.attempt,
            ),
            DurableOwnerAdmissionContinuationV1::Serving(continuation) => (
                ReconcileOwnerAdmissionTargetV1::ExactServing(continuation.publication),
                continuation.attempt,
            ),
        };
        if let Err(error) = attempt.validate_held() {
            return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                recovery_for_target(target, None, port_cause(operation, error), None, attempt),
            );
        }

        let (command, witness) = match &target {
            ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
                mint_reconcile_owner_admission_intent_command(intent.clone())
            }
            ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => {
                mint_reconcile_owner_admission_plan_command(plan.clone())
            }
            ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
                mint_reconcile_owner_serving_command(publication.clone())
            }
        };
        let dispatch_started = self.clock.now();
        let resolved = witness.resolve(self.control.reconcile_owner_admission(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure.code());
                let recovery_target = failure.into_recovery_target();
                if let Err(error) = held_after {
                    return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_target(
                            recovery_target,
                            None,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: durable_target(&recovery_target, serving_phase),
                        witness_failure: code,
                    })
                    .err();
                CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(recovery_for_target(
                    recovery_target,
                    None,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    attempt,
                ))
            }
            Ok(verified) => {
                let observed_plan = observed_reconcile_plan(verified.result());
                if let Err(error) = held_after {
                    return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_target(
                            target,
                            observed_plan,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                let outcome_unknown = matches!(
                    verified.result(),
                    ReconcileOwnerAdmissionResultV1::OutcomeUnknown(_)
                );
                if let Err(error) =
                    attempt.persist(reconcile_update(&target, verified.result(), serving_phase))
                {
                    let cause = if outcome_unknown {
                        OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                    } else {
                        port_cause(operation, error)
                    };
                    let recording_error = outcome_unknown.then_some(error);
                    return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_target(target, observed_plan, cause, recording_error, attempt),
                    );
                }

                if matches!(
                    verified.result(),
                    ReconcileOwnerAdmissionResultV1::Committed { .. }
                ) {
                    let serving_publication = match &target {
                        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
                            Some(publication.clone())
                        }
                        ReconcileOwnerAdmissionTargetV1::IntentOnly(_)
                        | ReconcileOwnerAdmissionTargetV1::ExactPlan(_) => None,
                    };
                    let result = verified.into_result();
                    return match result {
                        ReconcileOwnerAdmissionResultV1::Committed {
                            plan,
                            shard,
                            lease,
                            claim,
                            lifetime,
                        } => {
                            if !lifetime.validates_exact(&plan, &shard, &lease) {
                                return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                                    recovery_for_target(
                                        target,
                                        Some(*plan),
                                        OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch {
                                            operation,
                                        },
                                        None,
                                        attempt,
                                    ),
                                );
                            }
                            let Some(validity) = OwnerSessionValidityPermitV1::from_observation(
                                dispatch_started,
                                self.clock.now(),
                                lifetime,
                                1,
                            ) else {
                                return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                                    recovery_for_target(
                                        target,
                                        Some(*plan),
                                        OwnerAdmissionRecoveryCauseV1::SessionValidityExpired {
                                            operation,
                                        },
                                        None,
                                        attempt,
                                    ),
                                );
                            };
                            match serving_publication {
                                Some(publication) if shard == *publication.target() => {
                                    CoordinateReconcileOwnerAdmissionResultV1::Serving(
                                        OwnerServingSessionV1 {
                                            publication,
                                            shard,
                                            claim: *claim,
                                            validity,
                                            attempt,
                                        },
                                    )
                                }
                                Some(publication) if shard == *publication.source() => {
                                    CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(
                                        DurableOwnerServingPublicationV1 {
                                            publication,
                                            validity: Some(validity),
                                            attempt,
                                        },
                                    )
                                }
                                Some(publication) => {
                                    CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                                        recovery_for_serving(
                                            publication,
                                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                            OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch {
                                                operation,
                                            },
                                            None,
                                            attempt,
                                        ),
                                    )
                                }
                                None => CoordinateReconcileOwnerAdmissionResultV1::ProviderOpenPermitted(
                                    Box::new(OwnerAdmissionProviderOpenPermitV1 {
                                        plan: *plan,
                                        shard,
                                        lease,
                                        claim: *claim,
                                        validity,
                                        kind: OwnerAdmissionCommittedReceiptKindV1::Reconciled,
                                        attempt,
                                    }),
                                ),
                            }
                        }
                        result => CoordinateReconcileOwnerAdmissionResultV1::Recorded {
                            continuation: Box::new(continuation_for_target(target, attempt)),
                            result: Box::new(result),
                        },
                    };
                }

                if let ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                    evidence_digest, ..
                } = verified.result()
                {
                    if let ReconcileOwnerAdmissionTargetV1::ExactServing(publication) = &target {
                        return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                            recovery_for_serving(
                                publication.clone(),
                                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                                None,
                                attempt,
                            ),
                        );
                    }
                    let reason = OwnerAdmissionTerminationReasonV1::LeaseExpired {
                        evidence_digest: *evidence_digest,
                    };
                    return match (observed_plan, verified.into_lease_expiry()) {
                        (Some(plan), Ok(permit)) => {
                            CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(Box::new(
                                VerifiedExpiredOwnerAdmissionV1 {
                                    plan,
                                    reason,
                                    permit,
                                    attempt,
                                },
                            ))
                        }
                        (None, Ok(_permit)) => {
                            CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                                recovery_for_target(
                                    target,
                                    None,
                                    port_cause(
                                        operation,
                                        OwnerAdmissionAttemptPortErrorV1::DurableRecordInconsistent,
                                    ),
                                    None,
                                    attempt,
                                ),
                            )
                        }
                        (_, Err(verified)) => CoordinateReconcileOwnerAdmissionResultV1::Recorded {
                            continuation: Box::new(continuation_for_target(target, attempt)),
                            result: Box::new(verified.into_result()),
                        },
                    };
                }

                if outcome_unknown {
                    return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_target(
                            target,
                            None,
                            OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation },
                            None,
                            attempt,
                        ),
                    );
                }

                if let ReconcileOwnerAdmissionTargetV1::ExactServing(publication) = target {
                    return CoordinateReconcileOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_serving(
                            publication,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
                            None,
                            attempt,
                        ),
                    );
                }

                let result = verified.into_result();
                let continuation = match &result {
                    ReconcileOwnerAdmissionResultV1::Prepared { plan, .. } => {
                        DurableOwnerAdmissionContinuationV1::Plan(DurablePlannedOwnerAdmissionV1 {
                            plan: (**plan).clone(),
                            attempt,
                        })
                    }
                    ReconcileOwnerAdmissionResultV1::ExpiredPrepared { .. }
                    | ReconcileOwnerAdmissionResultV1::Rejected { .. }
                    | ReconcileOwnerAdmissionResultV1::DurableConflict { .. }
                    | ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
                    | ReconcileOwnerAdmissionResultV1::DurableInconsistent(_)
                    | ReconcileOwnerAdmissionResultV1::NotDispatched(_) => {
                        continuation_for_target(target, attempt)
                    }
                    ReconcileOwnerAdmissionResultV1::Committed { .. }
                    | ReconcileOwnerAdmissionResultV1::ExpiredCommitted { .. }
                    | ReconcileOwnerAdmissionResultV1::OutcomeUnknown(_) => {
                        continuation_for_target(target, attempt)
                    }
                };
                CoordinateReconcileOwnerAdmissionResultV1::Recorded {
                    continuation: Box::new(continuation),
                    result: Box::new(result),
                }
            }
        }
    }
}

fn continuation_for_target(
    target: ReconcileOwnerAdmissionTargetV1,
    attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
) -> DurableOwnerAdmissionContinuationV1 {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            DurableOwnerAdmissionContinuationV1::Intent(DurableOwnerAdmissionIntentV1 {
                intent,
                attempt,
            })
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => {
            DurableOwnerAdmissionContinuationV1::Plan(DurablePlannedOwnerAdmissionV1 {
                plan,
                attempt,
            })
        }
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            DurableOwnerAdmissionContinuationV1::Serving(DurableOwnerServingPublicationV1 {
                publication,
                validity: None,
                attempt,
            })
        }
    }
}

enum OwnerAdmissionTerminationMintV1 {
    Released,
    AuthorityCutover(OperationId),
    LeaseExpired(Box<ReconciledOwnerLeaseExpiryV1>),
}

fn terminate_update<'a>(
    plan: &'a PlannedOwnerAdmissionV1,
    reason: &'a OwnerAdmissionTerminationReasonV1,
    result: &'a TerminateOwnerAdmissionResultV1,
) -> OwnerAdmissionDurableUpdateV1<'a> {
    match result {
        TerminateOwnerAdmissionResultV1::Terminated { shard, claim } => {
            OwnerAdmissionDurableUpdateV1::Terminated {
                kind: OwnerAdmissionTerminatedReceiptKindV1::Terminated,
                plan,
                reason,
                shard,
                claim,
            }
        }
        TerminateOwnerAdmissionResultV1::AlreadyTerminated { shard, claim } => {
            OwnerAdmissionDurableUpdateV1::Terminated {
                kind: OwnerAdmissionTerminatedReceiptKindV1::AlreadyTerminated,
                plan,
                reason,
                shard,
                claim,
            }
        }
        TerminateOwnerAdmissionResultV1::Superseded { shard, claim } => {
            OwnerAdmissionDurableUpdateV1::Terminated {
                kind: OwnerAdmissionTerminatedReceiptKindV1::Superseded,
                plan,
                reason,
                shard,
                claim,
            }
        }
        TerminateOwnerAdmissionResultV1::DurableConflict { .. }
        | TerminateOwnerAdmissionResultV1::DurableInconsistent(_)
        | TerminateOwnerAdmissionResultV1::NotDispatched(_)
        | TerminateOwnerAdmissionResultV1::OutcomeUnknown(_) => {
            OwnerAdmissionDurableUpdateV1::TerminateClosed {
                plan,
                reason,
                result,
            }
        }
    }
}

impl OwnerAdmissionCoordinatorV1<'_> {
    pub fn terminate_released(
        &self,
        continuation: DurablePlannedOwnerAdmissionV1,
    ) -> CoordinateTerminateOwnerAdmissionResultV1 {
        let DurablePlannedOwnerAdmissionV1 { plan, attempt } = continuation;
        self.terminate_with_mint(
            plan,
            attempt,
            OwnerAdmissionTerminationReasonV1::Released,
            OwnerAdmissionTerminationMintV1::Released,
        )
    }

    pub fn terminate_authority_cutover(
        &self,
        continuation: DurablePlannedOwnerAdmissionV1,
        migration_id: OperationId,
    ) -> CoordinateTerminateOwnerAdmissionResultV1 {
        let DurablePlannedOwnerAdmissionV1 { plan, attempt } = continuation;
        self.terminate_with_mint(
            plan,
            attempt,
            OwnerAdmissionTerminationReasonV1::AuthorityCutover { migration_id },
            OwnerAdmissionTerminationMintV1::AuthorityCutover(migration_id),
        )
    }

    /// Lease-expiry termination accepts only the non-forgeable permit produced
    /// by a witnessed and durably recorded `ExpiredCommitted` reconciliation.
    pub fn terminate_expired(
        &self,
        continuation: VerifiedExpiredOwnerAdmissionV1,
    ) -> CoordinateTerminateOwnerAdmissionResultV1 {
        let VerifiedExpiredOwnerAdmissionV1 {
            plan,
            reason,
            permit,
            attempt,
        } = continuation;
        self.terminate_with_mint(
            plan,
            attempt,
            reason,
            OwnerAdmissionTerminationMintV1::LeaseExpired(Box::new(permit)),
        )
    }

    fn terminate_with_mint(
        &self,
        plan: PlannedOwnerAdmissionV1,
        mut attempt: Box<dyn TrustedOwnerAdmissionAttemptV1>,
        reason: OwnerAdmissionTerminationReasonV1,
        mint: OwnerAdmissionTerminationMintV1,
    ) -> CoordinateTerminateOwnerAdmissionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Terminate;
        let pending = || Some(OwnerAdmissionPendingMutationV1::Terminate(reason.clone()));
        if let Err(error) = attempt.validate_held() {
            return CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                pending(),
                attempt,
            ));
        }
        if let Err(error) = attempt.persist(OwnerAdmissionDurableUpdateV1::PendingTerminate {
            plan: &plan,
            reason: &reason,
        }) {
            return CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                pending(),
                attempt,
            ));
        }

        let (command, witness) = match mint {
            OwnerAdmissionTerminationMintV1::Released => {
                mint_released_terminate_owner_admission_command(plan.clone())
            }
            OwnerAdmissionTerminationMintV1::AuthorityCutover(migration_id) => {
                mint_authority_cutover_terminate_owner_admission_command(plan.clone(), migration_id)
            }
            OwnerAdmissionTerminationMintV1::LeaseExpired(permit) => {
                mint_expired_terminate_owner_admission_command(*permit)
            }
        };
        let resolved = witness.resolve(self.control.terminate_owner_admission(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure.code());
                let (recovery_plan, recovery_reason) = failure.into_recovery();
                let pending = Some(OwnerAdmissionPendingMutationV1::Terminate(
                    recovery_reason.clone(),
                ));
                if let Err(error) = held_after {
                    return CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            recovery_plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            port_cause(operation, error),
                            None,
                            pending,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: OwnerAdmissionDurableTargetV1::PendingTerminate {
                            plan: &recovery_plan,
                            reason: &recovery_reason,
                        },
                        witness_failure: code,
                    })
                    .err();
                CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                    recovery_plan,
                    OwnerAdmissionKnownDurableStageV1::Plan,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    Some(OwnerAdmissionPendingMutationV1::Terminate(recovery_reason)),
                    attempt,
                ))
            }
            Ok(result) => {
                if let Err(error) = held_after {
                    return CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            port_cause(operation, error),
                            None,
                            pending(),
                            attempt,
                        ),
                    );
                }
                let outcome_unknown =
                    matches!(result, TerminateOwnerAdmissionResultV1::OutcomeUnknown(_));
                let mutation_unresolved = !matches!(
                    result,
                    TerminateOwnerAdmissionResultV1::Terminated { .. }
                        | TerminateOwnerAdmissionResultV1::AlreadyTerminated { .. }
                        | TerminateOwnerAdmissionResultV1::Superseded { .. }
                );
                if let Err(error) = attempt.persist(terminate_update(&plan, &reason, &result)) {
                    let cause = if outcome_unknown {
                        OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                    } else {
                        port_cause(operation, error)
                    };
                    let recording_error = outcome_unknown.then_some(error);
                    return CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            cause,
                            recording_error,
                            pending(),
                            attempt,
                        ),
                    );
                }
                if mutation_unresolved {
                    return CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            if outcome_unknown {
                                OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                            } else {
                                OwnerAdmissionRecoveryCauseV1::PendingMutationUnresolved
                            },
                            None,
                            pending(),
                            attempt,
                        ),
                    );
                }
                CoordinateTerminateOwnerAdmissionResultV1::Recorded {
                    continuation: Box::new(DurablePlannedOwnerAdmissionV1 { plan, attempt }),
                    result: Box::new(result),
                }
            }
        }
    }
}

fn abort_update<'a>(
    plan: &'a PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionAbortReasonV1,
    result: &'a AbortOwnerAdmissionResultV1,
) -> OwnerAdmissionDurableUpdateV1<'a> {
    match result {
        AbortOwnerAdmissionResultV1::Aborted { claim } => OwnerAdmissionDurableUpdateV1::Aborted {
            plan,
            reason,
            claim,
        },
        AbortOwnerAdmissionResultV1::DurableConflict { .. }
        | AbortOwnerAdmissionResultV1::DurableInconsistent(_)
        | AbortOwnerAdmissionResultV1::NotDispatched(_)
        | AbortOwnerAdmissionResultV1::OutcomeUnknown(_) => {
            OwnerAdmissionDurableUpdateV1::AbortClosed {
                plan,
                reason,
                result,
            }
        }
    }
}

impl OwnerAdmissionCoordinatorV1<'_> {
    pub fn abort(
        &self,
        continuation: DurablePlannedOwnerAdmissionV1,
        reason: OwnerAdmissionAbortReasonV1,
    ) -> CoordinateAbortOwnerAdmissionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Abort;
        let pending = || Some(OwnerAdmissionPendingMutationV1::Abort(reason));
        let DurablePlannedOwnerAdmissionV1 { plan, mut attempt } = continuation;
        if let Err(error) = attempt.validate_held() {
            return CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                pending(),
                attempt,
            ));
        }
        if let Err(error) = attempt.persist(OwnerAdmissionDurableUpdateV1::PendingAbort {
            plan: &plan,
            reason,
        }) {
            return CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                pending(),
                attempt,
            ));
        }

        let (command, witness) = mint_abort_owner_admission_command(plan.clone(), reason);
        let resolved = witness.resolve(self.control.abort_owner_admission(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure.code());
                let (recovery_plan, recovery_reason) = failure.into_recovery();
                let recovery_pending =
                    Some(OwnerAdmissionPendingMutationV1::Abort(recovery_reason));
                if let Err(error) = held_after {
                    return CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            recovery_plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            port_cause(operation, error),
                            None,
                            recovery_pending,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: OwnerAdmissionDurableTargetV1::PendingAbort {
                            plan: &recovery_plan,
                            reason: recovery_reason,
                        },
                        witness_failure: code,
                    })
                    .err();
                CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                    recovery_plan,
                    OwnerAdmissionKnownDurableStageV1::Plan,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    recovery_pending,
                    attempt,
                ))
            }
            Ok(result) => {
                if let Err(error) = held_after {
                    return CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            port_cause(operation, error),
                            None,
                            pending(),
                            attempt,
                        ),
                    );
                }
                let outcome_unknown =
                    matches!(result, AbortOwnerAdmissionResultV1::OutcomeUnknown(_));
                let mutation_unresolved =
                    !matches!(result, AbortOwnerAdmissionResultV1::Aborted { .. });
                if let Err(error) = attempt.persist(abort_update(&plan, reason, &result)) {
                    let cause = if outcome_unknown {
                        OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                    } else {
                        port_cause(operation, error)
                    };
                    let recording_error = outcome_unknown.then_some(error);
                    return CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            cause,
                            recording_error,
                            pending(),
                            attempt,
                        ),
                    );
                }
                if mutation_unresolved {
                    return CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            if outcome_unknown {
                                OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                            } else {
                                OwnerAdmissionRecoveryCauseV1::PendingMutationUnresolved
                            },
                            None,
                            pending(),
                            attempt,
                        ),
                    );
                }
                CoordinateAbortOwnerAdmissionResultV1::Recorded {
                    continuation: Box::new(DurablePlannedOwnerAdmissionV1 { plan, attempt }),
                    result: Box::new(result),
                }
            }
        }
    }
}

fn commit_update<'a>(
    plan: &'a PlannedOwnerAdmissionV1,
    result: &'a CommitOwnerAdmissionResultV1,
) -> OwnerAdmissionDurableUpdateV1<'a> {
    match result {
        CommitOwnerAdmissionResultV1::Committed {
            shard,
            lease,
            claim,
            lifetime: _,
        } => OwnerAdmissionDurableUpdateV1::Committed {
            kind: OwnerAdmissionCommittedReceiptKindV1::Committed,
            plan,
            shard,
            lease,
            claim,
        },
        CommitOwnerAdmissionResultV1::AlreadyCommitted {
            shard,
            lease,
            claim,
            lifetime: _,
        } => OwnerAdmissionDurableUpdateV1::Committed {
            kind: OwnerAdmissionCommittedReceiptKindV1::AlreadyCommitted,
            plan,
            shard,
            lease,
            claim,
        },
        CommitOwnerAdmissionResultV1::DurableConflict { .. }
        | CommitOwnerAdmissionResultV1::DurableInconsistent(_)
        | CommitOwnerAdmissionResultV1::NotDispatched(_)
        | CommitOwnerAdmissionResultV1::OutcomeUnknown(_) => {
            OwnerAdmissionDurableUpdateV1::CommitClosed { plan, result }
        }
    }
}

impl OwnerAdmissionCoordinatorV1<'_> {
    /// Execute commit and issue a provider-open permit only after the exact
    /// committed receipt is durably synced.
    pub fn commit(
        &self,
        continuation: DurablePlannedOwnerAdmissionV1,
    ) -> CoordinateCommitOwnerAdmissionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Commit;
        let DurablePlannedOwnerAdmissionV1 { plan, mut attempt } = continuation;
        if let Err(error) = attempt.validate_held() {
            return CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }

        let (command, witness) = mint_commit_owner_admission_command(plan.clone());
        let dispatch_started = self.clock.now();
        let resolved = witness.resolve(self.control.commit_owner_admission(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure.code());
                let recovery_plan = failure.into_recovery_plan();
                if let Err(error) = held_after {
                    return CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            recovery_plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            port_cause(operation, error),
                            None,
                            None,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: OwnerAdmissionDurableTargetV1::Plan(&recovery_plan),
                        witness_failure: code,
                    })
                    .err();
                CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(recovery_for_plan(
                    recovery_plan,
                    OwnerAdmissionKnownDurableStageV1::Plan,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    None,
                    attempt,
                ))
            }
            Ok(result) => {
                if let Err(error) = held_after {
                    return CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            port_cause(operation, error),
                            None,
                            None,
                            attempt,
                        ),
                    );
                }

                let committed = match &result {
                    CommitOwnerAdmissionResultV1::Committed {
                        shard,
                        lease,
                        claim,
                        lifetime,
                    } => Some((
                        OwnerAdmissionCommittedReceiptKindV1::Committed,
                        shard.clone(),
                        lease.clone(),
                        claim.clone(),
                        lifetime.clone(),
                    )),
                    CommitOwnerAdmissionResultV1::AlreadyCommitted {
                        shard,
                        lease,
                        claim,
                        lifetime,
                    } => Some((
                        OwnerAdmissionCommittedReceiptKindV1::AlreadyCommitted,
                        shard.clone(),
                        lease.clone(),
                        claim.clone(),
                        lifetime.clone(),
                    )),
                    CommitOwnerAdmissionResultV1::DurableConflict { .. }
                    | CommitOwnerAdmissionResultV1::DurableInconsistent(_)
                    | CommitOwnerAdmissionResultV1::NotDispatched(_)
                    | CommitOwnerAdmissionResultV1::OutcomeUnknown(_) => None,
                };
                let outcome_unknown =
                    matches!(result, CommitOwnerAdmissionResultV1::OutcomeUnknown(_));
                if let Err(error) = attempt.persist(commit_update(&plan, &result)) {
                    let cause = if outcome_unknown {
                        OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                    } else {
                        port_cause(operation, error)
                    };
                    let recording_error = outcome_unknown.then_some(error);
                    return CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            cause,
                            recording_error,
                            None,
                            attempt,
                        ),
                    );
                }

                if let Some((kind, shard, lease, claim, lifetime)) = committed {
                    let Some(validity) = OwnerSessionValidityPermitV1::from_observation(
                        dispatch_started,
                        self.clock.now(),
                        lifetime,
                        1,
                    ) else {
                        return CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(
                            recovery_for_plan(
                                plan,
                                OwnerAdmissionKnownDurableStageV1::Plan,
                                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                                None,
                                None,
                                attempt,
                            ),
                        );
                    };
                    return CoordinateCommitOwnerAdmissionResultV1::ProviderOpenPermitted(
                        OwnerAdmissionProviderOpenPermitV1 {
                            plan,
                            shard,
                            lease,
                            claim,
                            validity,
                            kind,
                            attempt,
                        },
                    );
                }
                if outcome_unknown {
                    return CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation },
                            None,
                            None,
                            attempt,
                        ),
                    );
                }
                CoordinateCommitOwnerAdmissionResultV1::Recorded {
                    continuation: DurablePlannedOwnerAdmissionV1 { plan, attempt },
                    result,
                }
            }
        }
    }
}

fn publish_update<'a>(
    publication: &'a PlannedOwnerServingPublicationV1,
    result: &'a PublishOwnerServingResultV1,
) -> OwnerAdmissionDurableUpdateV1<'a> {
    match result {
        PublishOwnerServingResultV1::Published { shard, claim, .. } => {
            OwnerAdmissionDurableUpdateV1::ServingPublished {
                kind: OwnerServingPublishedReceiptKindV1::Published,
                publication,
                shard,
                claim,
            }
        }
        PublishOwnerServingResultV1::AlreadyPublished { shard, claim, .. } => {
            OwnerAdmissionDurableUpdateV1::ServingPublished {
                kind: OwnerServingPublishedReceiptKindV1::AlreadyPublished,
                publication,
                shard,
                claim,
            }
        }
        PublishOwnerServingResultV1::PublicationConflict { .. }
        | PublishOwnerServingResultV1::ExpiredCommitted { .. }
        | PublishOwnerServingResultV1::Terminated { .. }
        | PublishOwnerServingResultV1::Superseded { .. }
        | PublishOwnerServingResultV1::DurableConflict { .. }
        | PublishOwnerServingResultV1::DurableInconsistent(_)
        | PublishOwnerServingResultV1::NotDispatched(_)
        | PublishOwnerServingResultV1::OutcomeUnknown(_) => {
            OwnerAdmissionDurableUpdateV1::PublishClosed {
                publication,
                result,
            }
        }
    }
}

impl OwnerAdmissionCoordinatorV1<'_> {
    /// Consume the provider-open permit through the private provider-adoption
    /// and RootFence proof boundary. No caller-supplied publication can bypass
    /// this step, and the attempt guard never leaves coordinator-owned state.
    fn adopt_provider_and_root_fence(
        &self,
        permit: OwnerAdmissionProviderOpenPermitV1,
        adoption: &dyn SealedProviderAdoptionRootFenceV1,
    ) -> CoordinateProviderAdoptionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::ProviderAdoption;
        let OwnerAdmissionProviderOpenPermitV1 {
            plan,
            shard,
            lease,
            claim,
            validity,
            kind: _,
            mut attempt,
        } = permit;
        if let Err(error) = attempt.validate_held() {
            return CoordinateProviderAdoptionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        if !validity.is_current_at(self.clock.now()) {
            return CoordinateProviderAdoptionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                None,
                None,
                attempt,
            ));
        }
        let inspection = ProviderAdoptionRootFenceInspectionV1 {
            plan: &plan,
            shard: &shard,
            lease: &lease,
            claim: &claim,
        };
        let publication = match adoption.adopt_provider_and_activate_root_fence(inspection) {
            Ok(publication) => publication,
            Err(error) => {
                return CoordinateProviderAdoptionResultV1::RecoveryRequired(recovery_for_plan(
                    plan,
                    OwnerAdmissionKnownDurableStageV1::Plan,
                    port_cause(operation, error),
                    None,
                    None,
                    attempt,
                ));
            }
        };
        if let Err(error) = attempt.validate_held() {
            return CoordinateProviderAdoptionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                None,
                attempt,
            ));
        }
        if !validity.is_current_at(self.clock.now()) {
            return CoordinateProviderAdoptionResultV1::RecoveryRequired(recovery_for_plan(
                plan,
                OwnerAdmissionKnownDurableStageV1::Plan,
                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                None,
                None,
                attempt,
            ));
        }
        let serving_publication =
            match PlannedOwnerServingPublicationV1::new(plan.clone(), shard, publication) {
                Ok(publication) => publication,
                Err(_) => {
                    return CoordinateProviderAdoptionResultV1::RecoveryRequired(
                        recovery_for_plan(
                            plan,
                            OwnerAdmissionKnownDurableStageV1::Plan,
                            OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
                            None,
                            None,
                            attempt,
                        ),
                    );
                }
            };
        CoordinateProviderAdoptionResultV1::PublicationReady(DurableOwnerServingPublicationV1 {
            publication: serving_publication,
            validity: Some(validity),
            attempt,
        })
    }

    /// Durably record PendingPublish, execute the exact witnessed publication,
    /// persist its closed receipt, and only then mint a Serving session.
    pub fn publish_serving(
        &self,
        continuation: DurableOwnerServingPublicationV1,
    ) -> CoordinatePublishOwnerServingResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::PublishServing;
        let DurableOwnerServingPublicationV1 {
            publication,
            validity,
            mut attempt,
        } = continuation;
        if let Err(error) = attempt.validate_held() {
            return CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                attempt,
            ));
        }
        let Some(previous_validity) = validity else {
            return CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
                None,
                attempt,
            ));
        };
        if !previous_validity.is_current_at(self.clock.now()) {
            return CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::Plan,
                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.persist(OwnerAdmissionDurableUpdateV1::PendingPublish {
            publication: &publication,
        }) {
            return CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::Plan,
                port_cause(operation, error),
                None,
                attempt,
            ));
        }
        if let Err(error) = attempt.validate_held() {
            return CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                port_cause(operation, error),
                None,
                attempt,
            ));
        }
        if !previous_validity.is_current_at(self.clock.now()) {
            return CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                None,
                attempt,
            ));
        }

        let (command, witness) = mint_publish_owner_serving_command(publication.clone());
        let dispatch_started = self.clock.now();
        let resolved = witness.resolve(self.control.publish_owner_serving(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure);
                if let Err(error) = held_after {
                    return CoordinatePublishOwnerServingResultV1::RecoveryRequired(
                        recovery_for_serving(
                            publication,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: OwnerAdmissionDurableTargetV1::PendingPublish(&publication),
                        witness_failure: code,
                    })
                    .err();
                CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                    publication,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    attempt,
                ))
            }
            Ok(result) => {
                if let Err(error) = held_after {
                    return CoordinatePublishOwnerServingResultV1::RecoveryRequired(
                        recovery_for_serving(
                            publication,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                let published = match &result {
                    PublishOwnerServingResultV1::Published {
                        shard,
                        claim,
                        lifetime,
                    } => Some((shard.clone(), claim.clone(), lifetime.clone())),
                    PublishOwnerServingResultV1::AlreadyPublished {
                        shard,
                        claim,
                        lifetime,
                    } => Some((shard.clone(), claim.clone(), lifetime.clone())),
                    PublishOwnerServingResultV1::PublicationConflict { .. }
                    | PublishOwnerServingResultV1::ExpiredCommitted { .. }
                    | PublishOwnerServingResultV1::Terminated { .. }
                    | PublishOwnerServingResultV1::Superseded { .. }
                    | PublishOwnerServingResultV1::DurableConflict { .. }
                    | PublishOwnerServingResultV1::DurableInconsistent(_)
                    | PublishOwnerServingResultV1::NotDispatched(_)
                    | PublishOwnerServingResultV1::OutcomeUnknown(_) => None,
                };
                let outcome_unknown =
                    matches!(result, PublishOwnerServingResultV1::OutcomeUnknown(_));
                if let Err(error) = attempt.persist(publish_update(&publication, &result)) {
                    let cause = if outcome_unknown {
                        OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                    } else {
                        port_cause(operation, error)
                    };
                    return CoordinatePublishOwnerServingResultV1::RecoveryRequired(
                        recovery_for_serving(
                            publication,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            cause,
                            outcome_unknown.then_some(error),
                            attempt,
                        ),
                    );
                }
                if let Some((shard, claim, lifetime)) = published {
                    if shard != *publication.target()
                        || !lifetime.validates_exact(
                            publication.plan(),
                            &shard,
                            publication.plan().lease(),
                        )
                    {
                        return CoordinatePublishOwnerServingResultV1::RecoveryRequired(
                            recovery_for_serving(
                                publication,
                                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch {
                                    operation,
                                },
                                None,
                                attempt,
                            ),
                        );
                    }
                    let Some(generation) = previous_validity.generation().checked_add(1) else {
                        return CoordinatePublishOwnerServingResultV1::RecoveryRequired(
                            recovery_for_serving(
                                publication,
                                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch {
                                    operation,
                                },
                                None,
                                attempt,
                            ),
                        );
                    };
                    let Some(validity) = OwnerSessionValidityPermitV1::from_observation(
                        dispatch_started,
                        self.clock.now(),
                        lifetime,
                        generation,
                    ) else {
                        return CoordinatePublishOwnerServingResultV1::RecoveryRequired(
                            recovery_for_serving(
                                publication,
                                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                                None,
                                attempt,
                            ),
                        );
                    };
                    return CoordinatePublishOwnerServingResultV1::Serving(OwnerServingSessionV1 {
                        publication,
                        shard,
                        claim,
                        validity,
                        attempt,
                    });
                }
                let cause = if outcome_unknown {
                    OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                } else if matches!(result, PublishOwnerServingResultV1::ExpiredCommitted { .. }) {
                    OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation }
                } else {
                    OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation }
                };
                CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery_for_serving(
                    publication,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    cause,
                    None,
                    attempt,
                ))
            }
        }
    }

    /// Renew one exact Serving session, persist the closed result, and replace
    /// its validity only after post-dispatch reservation and deadline checks.
    pub fn renew(&self, session: OwnerServingSessionV1) -> CoordinateRenewOwnerSessionResultV1 {
        let operation = OwnerAdmissionCoordinatorOperationV1::Renew;
        let OwnerServingSessionV1 {
            publication,
            shard: previous_shard,
            claim: previous_claim,
            validity: previous_validity,
            mut attempt,
        } = session;
        if let Err(error) = attempt.validate_held() {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                port_cause(operation, error),
                None,
                attempt,
            ));
        }
        if !previous_validity.is_current_at(self.clock.now()) {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                None,
                attempt,
            ));
        }
        if previous_shard != *publication.target() {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
                None,
                attempt,
            ));
        }
        let target =
            match OwnerSessionRenewalTargetV1::new(publication.plan().clone(), previous_claim) {
                Ok(target) => target,
                Err(_) => {
                    return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                        recovery_for_serving(
                            publication,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
                            None,
                            attempt,
                        ),
                    );
                }
            };
        let Some(renewal_generation) = previous_validity.generation().checked_add(1) else {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery_for_serving(
                publication,
                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation },
                None,
                attempt,
            ));
        };
        if let Err(error) = attempt.persist(OwnerAdmissionDurableUpdateV1::PendingRenew {
            publication: &publication,
            target: &target,
            generation: renewal_generation,
        }) {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                recovery_for_pending_renew(
                    publication,
                    target,
                    renewal_generation,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    port_cause(operation, error),
                    None,
                    attempt,
                ),
            );
        }
        if let Err(error) = attempt.validate_held() {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                recovery_for_pending_renew(
                    publication,
                    target,
                    renewal_generation,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    port_cause(operation, error),
                    None,
                    attempt,
                ),
            );
        }
        if !previous_validity.is_current_at(self.clock.now()) {
            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                recovery_for_pending_renew(
                    publication,
                    target,
                    renewal_generation,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                    None,
                    attempt,
                ),
            );
        }
        let (command, witness) = mint_renew_owner_session_command(target.clone());
        let dispatch_started = self.clock.now();
        let resolved = witness.resolve(self.control.renew_owner_session(command));
        let held_after = attempt.validate_held();
        match resolved {
            Err(failure) => {
                let code = OwnerAdmissionWitnessFailureCodeV1::from(failure);
                if let Err(error) = held_after {
                    return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                        recovery_for_pending_renew(
                            publication,
                            target,
                            renewal_generation,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                let recording_error = attempt
                    .persist(OwnerAdmissionDurableUpdateV1::RecoveryRequired {
                        operation,
                        target: OwnerAdmissionDurableTargetV1::PendingRenew {
                            publication: &publication,
                            target: &target,
                            generation: renewal_generation,
                        },
                        witness_failure: code,
                    })
                    .err();
                CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery_for_pending_renew(
                    publication,
                    target,
                    renewal_generation,
                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                    OwnerAdmissionRecoveryCauseV1::WitnessRejected { operation, code },
                    recording_error,
                    attempt,
                ))
            }
            Ok(result) => {
                if let Err(error) = held_after {
                    return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                        recovery_for_pending_renew(
                            publication,
                            target,
                            renewal_generation,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            port_cause(operation, error),
                            None,
                            attempt,
                        ),
                    );
                }
                let current = match &result {
                    RenewOwnerSessionResultV1::Current {
                        shard,
                        claim,
                        lifetime,
                    } => {
                        if shard != publication.target()
                            || claim != target.claim()
                            || !lifetime.validates_exact(target.plan(), shard, target.session())
                        {
                            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                                recovery_for_pending_renew(
                                    publication,
                                    target,
                                    renewal_generation,
                                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                    OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch {
                                        operation,
                                    },
                                    None,
                                    attempt,
                                ),
                            );
                        }
                        let Some(validity) = OwnerSessionValidityPermitV1::from_observation(
                            dispatch_started,
                            self.clock.now(),
                            lifetime.clone(),
                            renewal_generation,
                        ) else {
                            return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                                recovery_for_pending_renew(
                                    publication,
                                    target,
                                    renewal_generation,
                                    OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                    OwnerAdmissionRecoveryCauseV1::SessionValidityExpired {
                                        operation,
                                    },
                                    None,
                                    attempt,
                                ),
                            );
                        };
                        Some((shard.clone(), claim.clone(), validity))
                    }
                    RenewOwnerSessionResultV1::ExpiredCommitted { .. }
                    | RenewOwnerSessionResultV1::Terminated { .. }
                    | RenewOwnerSessionResultV1::Superseded { .. }
                    | RenewOwnerSessionResultV1::DurableConflict { .. }
                    | RenewOwnerSessionResultV1::DurableInconsistent(_)
                    | RenewOwnerSessionResultV1::NotDispatched(_)
                    | RenewOwnerSessionResultV1::OutcomeUnknown(_) => None,
                };
                let outcome_unknown =
                    matches!(result, RenewOwnerSessionResultV1::OutcomeUnknown(_));
                if let Err(error) = attempt.persist(OwnerAdmissionDurableUpdateV1::RenewClosed {
                    publication: &publication,
                    target: &target,
                    generation: renewal_generation,
                    result: &result,
                }) {
                    let cause = if outcome_unknown {
                        OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                    } else {
                        port_cause(operation, error)
                    };
                    return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                        recovery_for_pending_renew(
                            publication,
                            target,
                            renewal_generation,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            cause,
                            outcome_unknown.then_some(error),
                            attempt,
                        ),
                    );
                }
                if let Some((shard, claim, validity)) = current {
                    if !validity.is_current_at(self.clock.now()) {
                        return CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                            recovery_for_serving(
                                publication,
                                OwnerAdmissionKnownDurableStageV1::ServingPublication,
                                OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation },
                                None,
                                attempt,
                            ),
                        );
                    }
                    return CoordinateRenewOwnerSessionResultV1::Current(OwnerServingSessionV1 {
                        publication,
                        shard,
                        claim,
                        validity,
                        attempt,
                    });
                }
                let cause = if outcome_unknown {
                    OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown { operation }
                } else if matches!(result, RenewOwnerSessionResultV1::ExpiredCommitted { .. }) {
                    OwnerAdmissionRecoveryCauseV1::SessionValidityExpired { operation }
                } else {
                    OwnerAdmissionRecoveryCauseV1::SessionEvidenceMismatch { operation }
                };
                if outcome_unknown {
                    CoordinateRenewOwnerSessionResultV1::RecoveryRequired(
                        recovery_for_pending_renew(
                            publication,
                            target,
                            renewal_generation,
                            OwnerAdmissionKnownDurableStageV1::ServingPublication,
                            cause,
                            None,
                            attempt,
                        ),
                    )
                } else {
                    CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery_for_serving(
                        publication,
                        OwnerAdmissionKnownDurableStageV1::ServingPublication,
                        cause,
                        None,
                        attempt,
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::owner_admission_command::{
        AbortOwnerAdmissionCommandV1, AbortOwnerAdmissionNotDispatchedV1,
        AbortOwnerAdmissionOutcomeV1, CommitOwnerAdmissionCommandV1, CommitOwnerAdmissionOutcomeV1,
        PrepareOwnerAdmissionCommandV1, PrepareOwnerAdmissionOutcomeV1,
        ReconcileOwnerAdmissionCommandV1, ReconcileOwnerAdmissionOutcomeV1,
        TerminateOwnerAdmissionCommandV1, TerminateOwnerAdmissionNotDispatchedV1,
        TerminateOwnerAdmissionOutcomeV1,
    };
    use crate::owner_admission_state::expected_recovering_record_for_plan;
    use crate::{
        ConsistencyDomainId, ControlError, FreshRootProvisioningOutcome, LogicalShardId,
        MetadataAuthorityBinding, MetadataAuthorityGeneration, MetadataAuthorityId,
        MetadataAuthorityRecord, MetadataAuthorityRevision, MetadataContractDigest,
        MetadataProviderProfileId, NodeId, OwnerEpoch, OwnerIncarnationId, OwnerLeaseModel,
        OwnerReleaseOutcome, PlacementGeneration, RecoveryPublication, RootId,
        RootLayoutGeneration, RootLayoutProfile, RootPartitionId, RootPlacement,
        RootPlacementLifecycle,
    };

    const MODE_NORMAL: u8 = 0;
    const MODE_PREPARE_BINDING_MISMATCH: u8 = 1;
    const MODE_COMMIT_UNKNOWN: u8 = 2;
    const MODE_RECONCILE_EXPIRED: u8 = 3;
    const MODE_ABORT_UNKNOWN: u8 = 4;
    const MODE_TERMINATE_UNKNOWN: u8 = 5;
    const MODE_RECONCILE_COMMITTED: u8 = 6;
    const MODE_ABORT_NOT_DISPATCHED: u8 = 7;
    const MODE_TERMINATE_NOT_DISPATCHED: u8 = 8;
    const MODE_PUBLISH_UNKNOWN: u8 = 9;
    const MODE_RENEW_EXPIRED: u8 = 10;
    const MODE_RENEW_BINDING_MISMATCH: u8 = 11;
    const MODE_RECONCILE_SERVING: u8 = 12;
    const MODE_COMMIT_FINITE: u8 = 13;
    const MODE_PUBLISH_FINITE: u8 = 14;
    const MODE_RENEW_UNKNOWN: u8 = 15;
    const MODE_RENEW_SOURCE_DESCENDANT: u8 = 16;
    const MODE_RENEW_FINITE: u8 = 17;

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn owner_incarnation(value: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([value; 16])
    }

    fn admission() -> crate::OwnerServingAdmission {
        let logical_shard_id = shard_id(2);
        let placement = RootPlacement {
            root_id: RootId::from_bytes([1; 16]),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id,
            placement_generation: PlacementGeneration::new(2).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
        };
        let authority = MetadataAuthorityRecord {
            logical_shard_id,
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
            active: MetadataAuthorityBinding {
                authority_id: MetadataAuthorityId::from_bytes([3; 16]),
                provider_profile_id: MetadataProviderProfileId::new("holt-primary").unwrap(),
                profile_fingerprint: [4; 32],
                consistency_domain_id: ConsistencyDomainId::from_bytes([5; 16]),
                contract_digest: MetadataContractDigest::from_bytes([6; 32]),
            },
            migration: None,
        };
        crate::OwnerServingAdmission::stable(placement, authority).unwrap()
    }

    fn intent(endpoint: &str) -> OwnerAdmissionIntentV1 {
        OwnerAdmissionIntentV1::fresh(
            admission(),
            LogicalShardRecord::unassigned(shard_id(2)),
            NodeId::new("node-a").unwrap(),
            owner_incarnation(8),
            endpoint.to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([7; 32]).unwrap(),
        )
        .unwrap()
    }

    fn plan_for_intent(intent: OwnerAdmissionIntentV1, lease_id: u64) -> PlannedOwnerAdmissionV1 {
        let lease = LogicalShardLease {
            logical_shard_id: intent.logical_shard_id(),
            owner: intent.owner().clone(),
            owner_epoch: intent.planned_epoch(),
            owner_incarnation_id: intent.owner_incarnation_id(),
            lease_id,
            authority: intent.admission().authority().fence(),
        };
        PlannedOwnerAdmissionV1::new(intent, lease).unwrap()
    }

    fn terminated_record(plan: &PlannedOwnerAdmissionV1) -> LogicalShardRecord {
        let mut record = plan.intent().expected_unowned_shard().clone();
        record.owner_epoch = Some(plan.intent().planned_epoch());
        record.owner_incarnation_id = Some(plan.intent().owner_incarnation_id());
        record
    }

    fn push_event(events: &Arc<Mutex<Vec<String>>>, event: impl Into<String>) {
        events.lock().unwrap().push(event.into());
    }

    #[derive(Default)]
    struct FakeAttemptState {
        validate_calls: usize,
        fail_validate_at: Option<usize>,
        fail_persist_tag: Option<&'static str>,
        fail_persist_after_record_tag: Option<&'static str>,
        acquire_error: Option<OwnerAdmissionAttemptPortErrorV1>,
        digest_override: Option<OwnerRuntimeReservationDigest>,
        durable_intent: bool,
        durable_plan: bool,
        durable_pending_abort: Option<(OwnerAdmissionPlanDigestV1, OwnerAdmissionAbortReasonV1)>,
        durable_pending_terminate: Option<(
            OwnerAdmissionPlanDigestV1,
            OwnerAdmissionTerminationReasonV1,
        )>,
        durable_pending_publish: Option<crate::OwnerServingPublicationDigestV1>,
        durable_serving_publication: Option<crate::OwnerServingPublicationDigestV1>,
        durable_pending_renew: Option<(
            crate::OwnerServingPublicationDigestV1,
            crate::OwnerSessionRenewalTargetDigestV1,
            u64,
        )>,
        logical_intent_records: usize,
        logical_pending_abort_records: usize,
        logical_pending_terminate_records: usize,
        logical_pending_publish_records: usize,
        logical_pending_renew_records: usize,
        next_guard_id: usize,
        observed_guard_ids: Vec<usize>,
        advance_clock_on_persist: Option<(&'static str, Arc<ManualMonotonicClockV1>, Duration)>,
    }

    struct FakeAttemptPort {
        events: Arc<Mutex<Vec<String>>>,
        state: Arc<Mutex<FakeAttemptState>>,
    }

    impl FakeAttemptPort {
        fn new(events: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                events,
                state: Arc::new(Mutex::new(FakeAttemptState::default())),
            }
        }

        fn fail_persist(&self, tag: &'static str) {
            self.state.lock().unwrap().fail_persist_tag = Some(tag);
        }

        fn fail_persist_after_record(&self, tag: &'static str) {
            self.state.lock().unwrap().fail_persist_after_record_tag = Some(tag);
        }

        fn fail_validate_at(&self, call: usize) {
            self.state.lock().unwrap().fail_validate_at = Some(call);
        }

        fn allow_persist(&self) {
            self.state.lock().unwrap().fail_persist_tag = None;
        }

        fn logical_intent_records(&self) -> usize {
            self.state.lock().unwrap().logical_intent_records
        }

        fn logical_pending_abort_records(&self) -> usize {
            self.state.lock().unwrap().logical_pending_abort_records
        }

        fn logical_pending_terminate_records(&self) -> usize {
            self.state.lock().unwrap().logical_pending_terminate_records
        }

        fn logical_pending_publish_records(&self) -> usize {
            self.state.lock().unwrap().logical_pending_publish_records
        }

        fn logical_pending_renew_records(&self) -> usize {
            self.state.lock().unwrap().logical_pending_renew_records
        }

        fn has_pending_publish(&self) -> bool {
            self.state.lock().unwrap().durable_pending_publish.is_some()
        }

        fn has_pending_renew(&self) -> bool {
            self.state.lock().unwrap().durable_pending_renew.is_some()
        }

        fn observed_guard_ids(&self) -> Vec<usize> {
            self.state.lock().unwrap().observed_guard_ids.clone()
        }

        fn advance_clock_on_persist(
            &self,
            tag: &'static str,
            clock: Arc<ManualMonotonicClockV1>,
            duration: Duration,
        ) {
            self.state.lock().unwrap().advance_clock_on_persist = Some((tag, clock, duration));
        }
    }

    struct ManualMonotonicClockV1 {
        now: Mutex<Instant>,
    }

    impl ManualMonotonicClockV1 {
        fn new(now: Instant) -> Self {
            Self {
                now: Mutex::new(now),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock().unwrap();
            *now = now.checked_add(duration).unwrap();
        }
    }

    impl SealedMonotonicClockV1 for ManualMonotonicClockV1 {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    struct FakeAttempt {
        id: usize,
        digest: OwnerRuntimeReservationDigest,
        events: Arc<Mutex<Vec<String>>>,
        state: Arc<Mutex<FakeAttemptState>>,
    }

    impl Drop for FakeAttempt {
        fn drop(&mut self) {
            push_event(&self.events, "guard:drop");
        }
    }

    impl TrustedOwnerAdmissionAttemptV1 for FakeAttempt {
        fn reservation_digest(&self) -> OwnerRuntimeReservationDigest {
            self.digest
        }

        fn validate_held(&mut self) -> Result<(), OwnerAdmissionAttemptPortErrorV1> {
            let mut state = self.state.lock().unwrap();
            state.validate_calls += 1;
            state.observed_guard_ids.push(self.id);
            let call = state.validate_calls;
            push_event(&self.events, format!("guard:validate:{call}"));
            if state.fail_validate_at == Some(call) {
                return Err(OwnerAdmissionAttemptPortErrorV1::ReservationNotHeld);
            }
            Ok(())
        }

        fn persist(
            &mut self,
            update: OwnerAdmissionDurableUpdateV1<'_>,
        ) -> Result<(), OwnerAdmissionAttemptPortErrorV1> {
            let tag = update.tag();
            push_event(&self.events, format!("journal:persist:{tag}"));
            let target_digest = match update.target() {
                OwnerAdmissionDurableTargetV1::Intent(intent) => intent.reservation_digest(),
                OwnerAdmissionDurableTargetV1::Plan(plan) => plan.intent().reservation_digest(),
                OwnerAdmissionDurableTargetV1::PendingPublish(publication)
                | OwnerAdmissionDurableTargetV1::Serving(publication) => {
                    publication.plan().intent().reservation_digest()
                }
                OwnerAdmissionDurableTargetV1::PendingAbort { plan, .. }
                | OwnerAdmissionDurableTargetV1::PendingTerminate { plan, .. } => {
                    plan.intent().reservation_digest()
                }
                OwnerAdmissionDurableTargetV1::PendingRenew { publication, .. } => {
                    publication.plan().intent().reservation_digest()
                }
            };
            if target_digest != self.digest {
                return Err(OwnerAdmissionAttemptPortErrorV1::ReservationBindingMismatch);
            }
            let mut state = self.state.lock().unwrap();
            if state.fail_persist_tag == Some(tag) {
                return Err(OwnerAdmissionAttemptPortErrorV1::DurabilityOutcomeUnknown);
            }
            match update {
                OwnerAdmissionDurableUpdateV1::Intent { .. } => {
                    if !state.durable_intent {
                        state.logical_intent_records += 1;
                    }
                    state.durable_intent = true;
                }
                OwnerAdmissionDurableUpdateV1::Prepared { .. }
                | OwnerAdmissionDurableUpdateV1::ExpiredPrepared { .. }
                | OwnerAdmissionDurableUpdateV1::Committed { .. }
                | OwnerAdmissionDurableUpdateV1::Aborted { .. }
                | OwnerAdmissionDurableUpdateV1::Terminated { .. }
                | OwnerAdmissionDurableUpdateV1::ExpiredCommitted { .. } => {
                    state.durable_intent = true;
                    state.durable_plan = true;
                }
                OwnerAdmissionDurableUpdateV1::PendingAbort { plan, reason } => {
                    let exact = (plan.digest(), reason);
                    if let Some(recorded) = &state.durable_pending_abort {
                        if recorded != &exact {
                            return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                        }
                    } else {
                        state.durable_pending_abort = Some(exact);
                        state.logical_pending_abort_records += 1;
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                }
                OwnerAdmissionDurableUpdateV1::PendingTerminate { plan, reason } => {
                    let exact = (plan.digest(), reason.clone());
                    if let Some(recorded) = &state.durable_pending_terminate {
                        if recorded != &exact {
                            return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                        }
                    } else {
                        state.durable_pending_terminate = Some(exact);
                        state.logical_pending_terminate_records += 1;
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                }
                OwnerAdmissionDurableUpdateV1::PendingPublish { publication } => {
                    let exact = publication.digest();
                    if let Some(recorded) = state.durable_pending_publish {
                        if recorded != exact {
                            return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                        }
                    } else if state.durable_serving_publication.is_some() {
                        return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                    } else {
                        state.logical_pending_publish_records += 1;
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                    state.durable_pending_publish = Some(exact);
                }
                OwnerAdmissionDurableUpdateV1::ServingPublished { publication, .. } => {
                    let exact = publication.digest();
                    if state.durable_pending_publish != Some(exact)
                        && state.durable_serving_publication != Some(exact)
                    {
                        return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                    state.durable_pending_publish = None;
                    state.durable_serving_publication = Some(exact);
                }
                OwnerAdmissionDurableUpdateV1::PublishClosed {
                    publication,
                    result,
                } => {
                    let exact = publication.digest();
                    if state.durable_pending_publish != Some(exact) {
                        return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                    }
                    if !matches!(result, PublishOwnerServingResultV1::OutcomeUnknown(_)) {
                        state.durable_pending_publish = None;
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                }
                OwnerAdmissionDurableUpdateV1::PendingRenew {
                    publication,
                    target,
                    generation,
                } => {
                    let exact = (publication.digest(), target.digest(), generation);
                    if state.durable_pending_publish.is_some()
                        || state.durable_serving_publication != Some(publication.digest())
                    {
                        return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                    }
                    if let Some(recorded) = state.durable_pending_renew {
                        if recorded != exact {
                            return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                        }
                    } else {
                        state.durable_pending_renew = Some(exact);
                        state.logical_pending_renew_records += 1;
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                }
                OwnerAdmissionDurableUpdateV1::RenewClosed {
                    publication,
                    target,
                    generation,
                    result,
                } => {
                    let exact = (publication.digest(), target.digest(), generation);
                    if state.durable_pending_renew != Some(exact) {
                        return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
                    }
                    if !matches!(result, RenewOwnerSessionResultV1::OutcomeUnknown(_)) {
                        state.durable_pending_renew = None;
                    }
                    state.durable_intent = true;
                    state.durable_plan = true;
                }
                OwnerAdmissionDurableUpdateV1::Rejected { .. }
                | OwnerAdmissionDurableUpdateV1::PrepareClosed { .. }
                | OwnerAdmissionDurableUpdateV1::CommitClosed { .. }
                | OwnerAdmissionDurableUpdateV1::AbortClosed { .. }
                | OwnerAdmissionDurableUpdateV1::TerminateClosed { .. }
                | OwnerAdmissionDurableUpdateV1::ReconcileClosed { .. }
                | OwnerAdmissionDurableUpdateV1::RecoveryRequired { .. } => {}
            }
            let advance_clock = match state.advance_clock_on_persist.as_ref() {
                Some((advance_tag, _, _)) if *advance_tag == tag => {
                    state.advance_clock_on_persist.take()
                }
                _ => None,
            };
            let fail_after_record = state.fail_persist_after_record_tag == Some(tag);
            if fail_after_record {
                state.fail_persist_after_record_tag = None;
            }
            drop(state);
            if let Some((_, clock, duration)) = advance_clock {
                clock.advance(duration);
            }
            if fail_after_record {
                return Err(OwnerAdmissionAttemptPortErrorV1::DurabilityOutcomeUnknown);
            }
            Ok(())
        }

        fn confirm(
            &mut self,
            target: OwnerAdmissionDurableTargetV1<'_>,
        ) -> Result<(), OwnerAdmissionAttemptPortErrorV1> {
            let (name, present, digest) = {
                let state = self.state.lock().unwrap();
                match target {
                    OwnerAdmissionDurableTargetV1::Intent(intent) => {
                        ("intent", state.durable_intent, intent.reservation_digest())
                    }
                    OwnerAdmissionDurableTargetV1::Plan(plan) => (
                        "plan",
                        state.durable_plan
                            && state.durable_pending_abort.is_none()
                            && state.durable_pending_terminate.is_none()
                            && state.durable_pending_publish.is_none()
                            && state.durable_pending_renew.is_none()
                            && state.durable_serving_publication.is_none(),
                        plan.intent().reservation_digest(),
                    ),
                    OwnerAdmissionDurableTargetV1::PendingAbort { plan, reason } => {
                        let exact = (plan.digest(), reason);
                        (
                            "pending-abort",
                            state.durable_pending_abort.as_ref() == Some(&exact),
                            plan.intent().reservation_digest(),
                        )
                    }
                    OwnerAdmissionDurableTargetV1::PendingTerminate { plan, reason } => {
                        let exact = (plan.digest(), reason.clone());
                        (
                            "pending-terminate",
                            state.durable_pending_terminate.as_ref() == Some(&exact),
                            plan.intent().reservation_digest(),
                        )
                    }
                    OwnerAdmissionDurableTargetV1::PendingPublish(publication) => (
                        "pending-publish",
                        state.durable_pending_publish == Some(publication.digest()),
                        publication.plan().intent().reservation_digest(),
                    ),
                    OwnerAdmissionDurableTargetV1::Serving(publication) => (
                        "serving-publication",
                        state.durable_pending_publish.is_none()
                            && state.durable_pending_renew.is_none()
                            && state.durable_serving_publication == Some(publication.digest()),
                        publication.plan().intent().reservation_digest(),
                    ),
                    OwnerAdmissionDurableTargetV1::PendingRenew {
                        publication,
                        target,
                        generation,
                    } => {
                        let exact = (publication.digest(), target.digest(), generation);
                        (
                            "pending-renew",
                            state.durable_pending_renew == Some(exact),
                            publication.plan().intent().reservation_digest(),
                        )
                    }
                }
            };
            push_event(&self.events, format!("journal:confirm:{name}"));
            if digest != self.digest {
                return Err(OwnerAdmissionAttemptPortErrorV1::ReservationBindingMismatch);
            }
            if !present {
                return Err(OwnerAdmissionAttemptPortErrorV1::DurableRecordConflict);
            }
            Ok(())
        }
    }

    impl TrustedOwnerAdmissionAttemptPortV1 for FakeAttemptPort {
        fn acquire(
            &self,
            intent: &OwnerAdmissionIntentV1,
        ) -> Result<Box<dyn TrustedOwnerAdmissionAttemptV1>, OwnerAdmissionAttemptPortErrorV1>
        {
            push_event(&self.events, "port:acquire");
            let mut state = self.state.lock().unwrap();
            if let Some(error) = state.acquire_error {
                return Err(error);
            }
            let digest = state.digest_override.unwrap_or(intent.reservation_digest());
            state.next_guard_id += 1;
            let id = state.next_guard_id;
            drop(state);
            Ok(Box::new(FakeAttempt {
                id,
                digest,
                events: Arc::clone(&self.events),
                state: Arc::clone(&self.state),
            }))
        }
    }

    struct ScriptedControlStore {
        events: Arc<Mutex<Vec<String>>>,
        mode: AtomicU8,
    }

    impl ScriptedControlStore {
        fn new(events: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                events,
                mode: AtomicU8::new(MODE_NORMAL),
            }
        }

        fn set_mode(&self, mode: u8) {
            self.mode.store(mode, Ordering::Release);
        }

        fn mode(&self) -> u8 {
            self.mode.load(Ordering::Acquire)
        }
    }

    struct FakeProviderAdoption {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl SealedProviderAdoptionRootFenceV1 for FakeProviderAdoption {
        fn adopt_provider_and_activate_root_fence(
            &self,
            inspection: ProviderAdoptionRootFenceInspectionV1<'_>,
        ) -> Result<RecoveryPublication, OwnerAdmissionAttemptPortErrorV1> {
            assert_eq!(inspection.lease, inspection.plan.lease());
            assert_eq!(
                inspection.claim,
                &OwnerAdmissionClaimV1::prepared(inspection.plan)
                    .unwrap()
                    .commit()
                    .unwrap()
            );
            assert_eq!(
                inspection.shard,
                &expected_recovering_record_for_plan(inspection.plan)
            );
            push_event(&self.events, "provider:adopt-and-activate-root-fence");
            Ok(RecoveryPublication {
                checkpoint: None,
                log: None,
                durable_lsn: 0,
            })
        }
    }

    fn unused_control<T>() -> Result<T, ControlError> {
        Err(ControlError::Backend(
            "unused scripted control operation".to_owned(),
        ))
    }

    impl ControlStore for ScriptedControlStore {
        fn owner_lease_model(&self) -> OwnerLeaseModel {
            OwnerLeaseModel::NonExpiring
        }

        fn prepare_owner_admission(
            &self,
            command: PrepareOwnerAdmissionCommandV1,
        ) -> PrepareOwnerAdmissionOutcomeV1 {
            push_event(&self.events, "control:prepare");
            let claimed = command.claim_execution();
            let exact_plan = plan_for_intent(claimed.inspect().clone(), 9);
            let result = if self.mode() == MODE_PREPARE_BINDING_MISMATCH {
                let foreign_plan = plan_for_intent(claimed.inspect().clone(), 10);
                PrepareOwnerAdmissionResultV1::Prepared {
                    plan: Box::new(exact_plan.clone()),
                    claim: OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap(),
                    sentinel: OwnerAdmissionPlanSentinelV1::for_plan(&exact_plan),
                }
            } else {
                PrepareOwnerAdmissionResultV1::Prepared {
                    claim: OwnerAdmissionClaimV1::prepared(&exact_plan).unwrap(),
                    sentinel: OwnerAdmissionPlanSentinelV1::for_plan(&exact_plan),
                    plan: Box::new(exact_plan),
                }
            };
            claimed.complete(result)
        }

        fn commit_owner_admission(
            &self,
            command: CommitOwnerAdmissionCommandV1,
        ) -> CommitOwnerAdmissionOutcomeV1 {
            push_event(&self.events, "control:commit");
            let claimed = command.claim_execution();
            let result = if self.mode() == MODE_COMMIT_UNKNOWN {
                claimed.outcome_unknown()
            } else {
                let plan = claimed.inspect();
                let shard = expected_recovering_record_for_plan(plan);
                let lease = plan.lease().clone();
                let lifetime = if self.mode() == MODE_COMMIT_FINITE {
                    claimed
                        .finite_lifetime_observation(
                            &shard,
                            &lease,
                            std::num::NonZeroU64::new(1).unwrap(),
                            crate::OwnerSessionLifetimeProofDigestV1::from_bytes([40; 32]).unwrap(),
                        )
                        .unwrap()
                } else {
                    claimed
                        .non_expiring_lifetime_observation(&shard, &lease)
                        .unwrap()
                };
                CommitOwnerAdmissionResultV1::Committed {
                    shard,
                    lease,
                    claim: OwnerAdmissionClaimV1::prepared(plan)
                        .unwrap()
                        .commit()
                        .unwrap(),
                    lifetime,
                }
            };
            claimed.complete(result)
        }

        fn abort_owner_admission(
            &self,
            command: AbortOwnerAdmissionCommandV1,
        ) -> AbortOwnerAdmissionOutcomeV1 {
            push_event(&self.events, "control:abort");
            let claimed = command.claim_execution();
            let result = if self.mode() == MODE_ABORT_UNKNOWN {
                claimed.outcome_unknown()
            } else if self.mode() == MODE_ABORT_NOT_DISPATCHED {
                AbortOwnerAdmissionResultV1::NotDispatched(
                    AbortOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                )
            } else {
                let inspection = claimed.inspect();
                AbortOwnerAdmissionResultV1::Aborted {
                    claim: OwnerAdmissionClaimV1::prepared(inspection.plan)
                        .unwrap()
                        .abort(inspection.reason)
                        .unwrap(),
                }
            };
            claimed.complete(result)
        }

        fn terminate_owner_admission(
            &self,
            command: TerminateOwnerAdmissionCommandV1,
        ) -> TerminateOwnerAdmissionOutcomeV1 {
            push_event(&self.events, "control:terminate");
            let claimed = command.claim_execution();
            let result = if self.mode() == MODE_TERMINATE_UNKNOWN {
                claimed.outcome_unknown()
            } else if self.mode() == MODE_TERMINATE_NOT_DISPATCHED {
                TerminateOwnerAdmissionResultV1::NotDispatched(
                    TerminateOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                )
            } else {
                let inspection = claimed.inspect();
                TerminateOwnerAdmissionResultV1::Terminated {
                    shard: terminated_record(inspection.plan),
                    claim: OwnerAdmissionClaimV1::prepared(inspection.plan)
                        .unwrap()
                        .commit()
                        .unwrap()
                        .terminate(inspection.reason.clone())
                        .unwrap(),
                }
            };
            claimed.complete(result)
        }

        fn reconcile_owner_admission(
            &self,
            command: ReconcileOwnerAdmissionCommandV1,
        ) -> ReconcileOwnerAdmissionOutcomeV1 {
            push_event(&self.events, "control:reconcile");
            let claimed = command.claim_execution();
            let plan = match claimed.inspect() {
                ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
                    plan_for_intent(intent.clone(), 9)
                }
                ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => plan.clone(),
                ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
                    publication.plan().clone()
                }
            };
            let result = if self.mode() == MODE_RECONCILE_EXPIRED {
                ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                    shard: expected_recovering_record_for_plan(&plan),
                    claim: Box::new(
                        OwnerAdmissionClaimV1::prepared(&plan)
                            .unwrap()
                            .commit()
                            .unwrap(),
                    ),
                    expected_session: plan.lease().clone(),
                    evidence_digest: OwnerLeaseExpiryEvidenceDigest::from_bytes([23; 32]).unwrap(),
                    plan: Box::new(plan),
                }
            } else if matches!(
                self.mode(),
                MODE_RECONCILE_COMMITTED | MODE_RECONCILE_SERVING
            ) {
                let shard = if self.mode() == MODE_RECONCILE_SERVING {
                    match claimed.inspect() {
                        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
                            publication.target().clone()
                        }
                        ReconcileOwnerAdmissionTargetV1::IntentOnly(_)
                        | ReconcileOwnerAdmissionTargetV1::ExactPlan(_) => {
                            expected_recovering_record_for_plan(&plan)
                        }
                    }
                } else {
                    expected_recovering_record_for_plan(&plan)
                };
                let lease = plan.lease().clone();
                let lifetime = claimed
                    .non_expiring_lifetime_observation(&plan, &shard, &lease)
                    .unwrap();
                ReconcileOwnerAdmissionResultV1::Committed {
                    shard,
                    lease,
                    claim: Box::new(
                        OwnerAdmissionClaimV1::prepared(&plan)
                            .unwrap()
                            .commit()
                            .unwrap(),
                    ),
                    lifetime,
                    plan: Box::new(plan),
                }
            } else {
                ReconcileOwnerAdmissionResultV1::Prepared {
                    claim: OwnerAdmissionClaimV1::prepared(&plan).unwrap(),
                    sentinel: OwnerAdmissionPlanSentinelV1::for_plan(&plan),
                    plan: Box::new(plan),
                }
            };
            claimed.complete(result)
        }

        fn publish_owner_serving(
            &self,
            command: crate::PublishOwnerServingCommandV1,
        ) -> crate::PublishOwnerServingOutcomeV1 {
            push_event(&self.events, "control:publish-serving");
            let claimed = command.claim_execution();
            let result = if self.mode() == MODE_PUBLISH_UNKNOWN {
                claimed.outcome_unknown()
            } else {
                let inspection = claimed.inspect();
                let claim = OwnerAdmissionClaimV1::prepared(inspection.plan)
                    .unwrap()
                    .commit()
                    .unwrap();
                let lifetime = if self.mode() == MODE_PUBLISH_FINITE {
                    claimed
                        .finite_lifetime_observation(
                            inspection.target,
                            &claim,
                            inspection.plan.lease(),
                            std::num::NonZeroU64::new(1).unwrap(),
                            crate::OwnerSessionLifetimeProofDigestV1::from_bytes([41; 32]).unwrap(),
                        )
                        .unwrap()
                } else {
                    claimed
                        .non_expiring_lifetime_observation(
                            inspection.target,
                            &claim,
                            inspection.plan.lease(),
                        )
                        .unwrap()
                };
                crate::PublishOwnerServingResultV1::Published {
                    shard: inspection.target.clone(),
                    claim,
                    lifetime,
                }
            };
            claimed.complete(result)
        }

        fn renew_owner_session(
            &self,
            command: crate::RenewOwnerSessionCommandV1,
        ) -> crate::RenewOwnerSessionOutcomeV1 {
            push_event(&self.events, "control:renew-session");
            let claimed = command.claim_execution();
            let result = if self.mode() == MODE_RENEW_UNKNOWN {
                claimed.outcome_unknown()
            } else {
                let inspection = claimed.inspect();
                let mut shard = expected_recovering_record_for_plan(inspection.plan);
                if self.mode() != MODE_RENEW_SOURCE_DESCENDANT {
                    shard.state = crate::LogicalShardState::Serving;
                }
                if self.mode() == MODE_RENEW_EXPIRED {
                    crate::RenewOwnerSessionResultV1::ExpiredCommitted {
                        shard,
                        claim: inspection.claim.clone(),
                        expected_session: inspection.session.clone(),
                        evidence_digest: OwnerLeaseExpiryEvidenceDigest::from_bytes([24; 32])
                            .unwrap(),
                    }
                } else {
                    let lifetime = if self.mode() == MODE_RENEW_FINITE {
                        claimed
                            .finite_lifetime_observation(
                                &shard,
                                inspection.claim,
                                inspection.session,
                                std::num::NonZeroU64::new(1).unwrap(),
                                crate::OwnerSessionLifetimeProofDigestV1::from_bytes([42; 32])
                                    .unwrap(),
                            )
                            .unwrap()
                    } else {
                        claimed
                            .non_expiring_lifetime_observation(
                                &shard,
                                inspection.claim,
                                inspection.session,
                            )
                            .unwrap()
                    };
                    if self.mode() == MODE_RENEW_BINDING_MISMATCH {
                        shard.endpoint = Some("10.0.0.99:7000".to_owned());
                    }
                    crate::RenewOwnerSessionResultV1::Current {
                        shard,
                        claim: inspection.claim.clone(),
                        lifetime,
                    }
                }
            };
            claimed.complete(result)
        }

        fn provision_fresh_root(
            &self,
            _initial_placement: RootPlacement,
            _initial_authority: MetadataAuthorityRecord,
        ) -> Result<FreshRootProvisioningOutcome, ControlError> {
            unused_control()
        }

        fn create_root_placement(
            &self,
            _placement: RootPlacement,
        ) -> Result<RootPlacement, ControlError> {
            unused_control()
        }

        fn get_root_placement(
            &self,
            _root_id: &RootId,
        ) -> Result<Option<RootPlacement>, ControlError> {
            unused_control()
        }

        fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
            unused_control()
        }

        fn compare_and_set_root_placement(
            &self,
            _expected: &RootPlacement,
            _next: RootPlacement,
        ) -> Result<RootPlacement, ControlError> {
            unused_control()
        }

        fn create_logical_shard(
            &self,
            _logical_shard_id: LogicalShardId,
        ) -> Result<LogicalShardRecord, ControlError> {
            unused_control()
        }

        fn get_logical_shard(
            &self,
            _logical_shard_id: &LogicalShardId,
        ) -> Result<Option<LogicalShardRecord>, ControlError> {
            unused_control()
        }

        fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
            unused_control()
        }

        fn create_metadata_authority(
            &self,
            _authority: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, ControlError> {
            unused_control()
        }

        fn get_metadata_authority(
            &self,
            _logical_shard_id: &LogicalShardId,
        ) -> Result<Option<MetadataAuthorityRecord>, ControlError> {
            unused_control()
        }

        fn compare_and_set_metadata_authority(
            &self,
            _expected: &MetadataAuthorityRecord,
            _next: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, ControlError> {
            unused_control()
        }

        fn acquire_owner(
            &self,
            _admission: &crate::OwnerServingAdmission,
            _owner: NodeId,
            _owner_incarnation_id: OwnerIncarnationId,
            _endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            push_event(&self.events, "control:raw-acquire");
            unused_control()
        }

        fn acquire_successor(
            &self,
            _admission: &crate::OwnerServingAdmission,
            _expected_owner_epoch: OwnerEpoch,
            _owner: NodeId,
            _owner_incarnation_id: OwnerIncarnationId,
            _endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            push_event(&self.events, "control:raw-successor");
            unused_control()
        }

        fn renew_owner(
            &self,
            _lease: &LogicalShardLease,
            _admission: &crate::OwnerServingAdmission,
        ) -> Result<LogicalShardRecord, ControlError> {
            unused_control()
        }

        fn mark_serving(
            &self,
            _lease: &LogicalShardLease,
            _admission: &crate::OwnerServingAdmission,
            _publication: RecoveryPublication,
        ) -> Result<LogicalShardRecord, ControlError> {
            unused_control()
        }

        fn release_owner(
            &self,
            _lease: &LogicalShardLease,
        ) -> Result<OwnerReleaseOutcome, ControlError> {
            push_event(&self.events, "control:raw-release");
            unused_control()
        }
    }

    fn begin_ready(
        coordinator: &OwnerAdmissionCoordinatorV1<'_>,
        exact_intent: OwnerAdmissionIntentV1,
    ) -> DurableOwnerAdmissionIntentV1 {
        match coordinator.begin(exact_intent) {
            OpenDurableOwnerAdmissionIntentResultV1::Ready(continuation) => continuation,
            result => panic!("expected durable intent, got {result:?}"),
        }
    }

    fn prepare_ready(
        coordinator: &OwnerAdmissionCoordinatorV1<'_>,
        continuation: DurableOwnerAdmissionIntentV1,
    ) -> DurablePlannedOwnerAdmissionV1 {
        match coordinator.prepare(continuation) {
            CoordinatePrepareOwnerAdmissionResultV1::Prepared { continuation, .. } => continuation,
            result => panic!("expected durable plan, got {result:?}"),
        }
    }

    fn provider_open_permit(
        coordinator: &OwnerAdmissionCoordinatorV1<'_>,
        endpoint: &str,
    ) -> OwnerAdmissionProviderOpenPermitV1 {
        let plan = prepare_ready(coordinator, begin_ready(coordinator, intent(endpoint)));
        match coordinator.commit(plan) {
            CoordinateCommitOwnerAdmissionResultV1::ProviderOpenPermitted(permit) => permit,
            result => panic!("expected provider-open permit, got {result:?}"),
        }
    }

    fn publication_ready(
        coordinator: &OwnerAdmissionCoordinatorV1<'_>,
        adoption: &FakeProviderAdoption,
        endpoint: &str,
    ) -> DurableOwnerServingPublicationV1 {
        match coordinator
            .adopt_provider_and_root_fence(provider_open_permit(coordinator, endpoint), adoption)
        {
            CoordinateProviderAdoptionResultV1::PublicationReady(continuation) => continuation,
            CoordinateProviderAdoptionResultV1::RecoveryRequired(recovery) => {
                panic!("expected publication continuation, got {recovery:?}")
            }
        }
    }

    fn event_snapshot(events: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        events.lock().unwrap().clone()
    }

    fn assert_one_held_guard(attempts: &FakeAttemptPort) {
        let guard_ids = attempts.observed_guard_ids();
        assert!(!guard_ids.is_empty());
        assert!(guard_ids.iter().all(|guard_id| *guard_id == guard_ids[0]));
    }

    #[test]
    fn elapsed_finite_observation_cannot_mint_a_local_validity_permit() {
        let plan = plan_for_intent(intent("10.0.0.1:7000"), 9);
        let (command, _witness) = mint_commit_owner_admission_command(plan.clone());
        let claimed = command.claim_execution();
        let shard = expected_recovering_record_for_plan(&plan);
        let observation = claimed
            .finite_lifetime_observation(
                &shard,
                plan.lease(),
                std::num::NonZeroU64::new(1).unwrap(),
                crate::OwnerSessionLifetimeProofDigestV1::from_bytes([31; 32]).unwrap(),
            )
            .unwrap();
        let dispatch_started = Instant::now();
        let observed_at = dispatch_started
            .checked_add(Duration::from_secs(2))
            .unwrap();

        assert!(OwnerSessionValidityPermitV1::from_observation(
            dispatch_started,
            observed_at,
            observation,
            1,
        )
        .is_none());
    }

    #[test]
    fn positive_ttl_elapsed_during_receipt_persistence_mints_no_permit_or_session() {
        let commit_events = Arc::new(Mutex::new(Vec::new()));
        let commit_attempts = FakeAttemptPort::new(Arc::clone(&commit_events));
        let commit_control = ScriptedControlStore::new(Arc::clone(&commit_events));
        commit_control.set_mode(MODE_COMMIT_FINITE);
        let commit_clock = Arc::new(ManualMonotonicClockV1::new(Instant::now()));
        commit_attempts.advance_clock_on_persist(
            "Committed",
            Arc::clone(&commit_clock),
            Duration::from_secs(2),
        );
        let commit_coordinator = OwnerAdmissionCoordinatorV1::with_clock(
            &commit_control,
            &commit_attempts,
            commit_clock.as_ref(),
        );
        let plan = prepare_ready(
            &commit_coordinator,
            begin_ready(&commit_coordinator, intent("10.0.0.25:7000")),
        );
        let recovery = match commit_coordinator.commit(plan) {
            CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("elapsed committed TTL must not mint a permit: {result:?}"),
        };
        assert!(matches!(
            recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::SessionValidityExpired {
                operation: OwnerAdmissionCoordinatorOperationV1::Commit
            }
        ));

        let publish_events = Arc::new(Mutex::new(Vec::new()));
        let publish_attempts = FakeAttemptPort::new(Arc::clone(&publish_events));
        let publish_control = ScriptedControlStore::new(Arc::clone(&publish_events));
        let publish_clock = Arc::new(ManualMonotonicClockV1::new(Instant::now()));
        let publish_coordinator = OwnerAdmissionCoordinatorV1::with_clock(
            &publish_control,
            &publish_attempts,
            publish_clock.as_ref(),
        );
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&publish_events),
        };
        let ready = publication_ready(&publish_coordinator, &adoption, "10.0.0.26:7000");
        publish_control.set_mode(MODE_PUBLISH_FINITE);
        publish_attempts.advance_clock_on_persist(
            "ServingPublished",
            Arc::clone(&publish_clock),
            Duration::from_secs(2),
        );
        let recovery = match publish_coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("elapsed publication TTL must not mint a session: {result:?}"),
        };
        assert!(matches!(
            recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::SessionValidityExpired {
                operation: OwnerAdmissionCoordinatorOperationV1::PublishServing
            }
        ));

        let renew_events = Arc::new(Mutex::new(Vec::new()));
        let renew_attempts = FakeAttemptPort::new(Arc::clone(&renew_events));
        let renew_control = ScriptedControlStore::new(Arc::clone(&renew_events));
        let renew_clock = Arc::new(ManualMonotonicClockV1::new(Instant::now()));
        let renew_coordinator = OwnerAdmissionCoordinatorV1::with_clock(
            &renew_control,
            &renew_attempts,
            renew_clock.as_ref(),
        );
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&renew_events),
        };
        let ready = publication_ready(&renew_coordinator, &adoption, "10.0.0.36:7000");
        let session = match renew_coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected initial Serving session, got {result:?}"),
        };
        renew_control.set_mode(MODE_RENEW_FINITE);
        renew_attempts.advance_clock_on_persist(
            "RenewClosed",
            Arc::clone(&renew_clock),
            Duration::from_secs(2),
        );
        let recovery = match renew_coordinator.renew(session) {
            CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("elapsed renewal TTL must not mint a session: {result:?}"),
        };
        assert!(matches!(
            recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::SessionValidityExpired {
                operation: OwnerAdmissionCoordinatorOperationV1::Renew
            }
        ));
    }

    #[test]
    fn strict_order_holds_one_guard_until_provider_open_permit_is_dropped() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);

        let durable_intent = begin_ready(&coordinator, intent("10.0.0.1:7000"));
        let durable_plan = prepare_ready(&coordinator, durable_intent);
        let permit = match coordinator.commit(durable_plan) {
            CoordinateCommitOwnerAdmissionResultV1::ProviderOpenPermitted(permit) => permit,
            result => panic!("expected provider-open permit, got {result:?}"),
        };

        assert_eq!(
            event_snapshot(&events),
            vec![
                "port:acquire",
                "guard:validate:1",
                "journal:persist:Intent",
                "guard:validate:2",
                "control:prepare",
                "guard:validate:3",
                "journal:persist:Prepared",
                "guard:validate:4",
                "control:commit",
                "guard:validate:5",
                "journal:persist:Committed",
            ]
        );
        assert_eq!(
            permit.kind(),
            OwnerAdmissionCommittedReceiptKindV1::Committed
        );
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "guard:drop"));
        drop(permit);
        assert_eq!(event_snapshot(&events).last().unwrap(), "guard:drop");
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event.contains("raw-")));
    }

    #[test]
    fn provider_adoption_publish_and_renew_form_one_linear_guarded_session() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };

        let publication = publication_ready(&coordinator, &adoption, "10.0.0.20:7000");
        let session = match coordinator.publish_serving(publication) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        assert!(session.validity().is_current());
        let published_generation = session.validity().generation();
        let renewed = match coordinator.renew(session) {
            CoordinateRenewOwnerSessionResultV1::Current(session) => session,
            result => panic!("expected renewed session, got {result:?}"),
        };
        assert!(renewed.validity().is_current());
        assert_eq!(
            renewed.validity().generation(),
            published_generation.checked_add(1).unwrap()
        );

        let snapshot = event_snapshot(&events);
        let adoption_index = snapshot
            .iter()
            .position(|event| event == "provider:adopt-and-activate-root-fence")
            .unwrap();
        let pending_index = snapshot
            .iter()
            .position(|event| event == "journal:persist:PendingPublish")
            .unwrap();
        let publish_index = snapshot
            .iter()
            .position(|event| event == "control:publish-serving")
            .unwrap();
        let published_index = snapshot
            .iter()
            .position(|event| event == "journal:persist:ServingPublished")
            .unwrap();
        let renew_index = snapshot
            .iter()
            .position(|event| event == "control:renew-session")
            .unwrap();
        let pending_renew_index = snapshot
            .iter()
            .position(|event| event == "journal:persist:PendingRenew")
            .unwrap();
        let renew_closed_index = snapshot
            .iter()
            .position(|event| event == "journal:persist:RenewClosed")
            .unwrap();
        assert!(adoption_index < pending_index);
        assert!(pending_index < publish_index);
        assert!(publish_index < published_index);
        assert!(published_index < renew_index);
        assert!(published_index < pending_renew_index);
        assert!(pending_renew_index < renew_index);
        assert!(renew_index < renew_closed_index);
        assert_eq!(attempts.logical_pending_publish_records(), 1);
        assert_one_held_guard(&attempts);
        assert!(!snapshot.iter().any(|event| event.contains("raw-")));
    }

    #[test]
    fn exact_serving_reconcile_distinguishes_source_from_published_target() {
        let source_events = Arc::new(Mutex::new(Vec::new()));
        let source_attempts = FakeAttemptPort::new(Arc::clone(&source_events));
        let source_control = ScriptedControlStore::new(Arc::clone(&source_events));
        let source = OwnerAdmissionCoordinatorV1::new(&source_control, &source_attempts);
        let source_adoption = FakeProviderAdoption {
            events: Arc::clone(&source_events),
        };
        let publication = publication_ready(&source, &source_adoption, "10.0.0.21:7000");
        source_control.set_mode(MODE_PUBLISH_UNKNOWN);
        let recovery = match source.publish_serving(publication) {
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown publication must retain exact recovery: {result:?}"),
        };
        source_control.set_mode(MODE_RECONCILE_COMMITTED);
        let ready = match source.recover(recovery) {
            CoordinateRecoverOwnerAdmissionResultV1::Reconciled(result) => match *result {
                CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(continuation) => {
                    continuation
                }
                result => panic!("Recovering source must remain publication-ready: {result:?}"),
            },
            result => panic!("expected exact Serving reconciliation: {result:?}"),
        };
        source_control.set_mode(MODE_NORMAL);
        assert!(matches!(
            source.publish_serving(ready),
            CoordinatePublishOwnerServingResultV1::Serving(_)
        ));
        assert_one_held_guard(&source_attempts);

        let target_events = Arc::new(Mutex::new(Vec::new()));
        let target_attempts = FakeAttemptPort::new(Arc::clone(&target_events));
        let target_control = ScriptedControlStore::new(Arc::clone(&target_events));
        let target = OwnerAdmissionCoordinatorV1::new(&target_control, &target_attempts);
        let target_adoption = FakeProviderAdoption {
            events: Arc::clone(&target_events),
        };
        let publication = publication_ready(&target, &target_adoption, "10.0.0.22:7000");
        target_control.set_mode(MODE_PUBLISH_UNKNOWN);
        let recovery = match target.publish_serving(publication) {
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown publication must retain exact recovery: {result:?}"),
        };
        target_control.set_mode(MODE_RECONCILE_SERVING);
        assert!(matches!(
            target.recover(recovery),
            CoordinateRecoverOwnerAdmissionResultV1::Reconciled(result)
                if matches!(
                    result.as_ref(),
                    CoordinateReconcileOwnerAdmissionResultV1::Serving(_)
                )
        ));
        assert_one_held_guard(&target_attempts);
    }

    #[test]
    fn pending_publish_response_loss_blocks_dispatch_and_replays_one_logical_record() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.27:7000");
        attempts.fail_persist_after_record("PendingPublish");

        let recovery = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown PendingPublish durability must recover: {result:?}"),
        };
        assert_eq!(attempts.logical_pending_publish_records(), 1);
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "control:publish-serving"));

        control.set_mode(MODE_RECONCILE_COMMITTED);
        assert!(matches!(
            coordinator.recover(recovery),
            CoordinateRecoverOwnerAdmissionResultV1::Reconciled(result)
                if matches!(
                    result.as_ref(),
                    CoordinateReconcileOwnerAdmissionResultV1::PublicationReady(_)
                )
        ));
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "journal:persist:PendingPublish")
                .count(),
            2
        );
        assert_eq!(attempts.logical_pending_publish_records(), 1);
        assert!(!snapshot
            .iter()
            .any(|event| event == "control:publish-serving"));
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn serving_restart_reacquires_then_requires_fresh_exact_renew() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.32:7000");
        let session = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        let publication = session.publication().clone();
        drop(session);

        assert!(matches!(
            coordinator.resume_pending_publish(publication.clone()),
            ResumeServingPublicationResultV1::RecoveryRequired(_)
        ));
        control.set_mode(MODE_RECONCILE_SERVING);
        let awaiting = match coordinator.resume_serving(publication) {
            ResumeServingPublicationResultV1::AwaitingRenew(awaiting) => awaiting,
            result => panic!("Serving restart must await fresh renew: {result:?}"),
        };
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "control:renew-session")
                .count(),
            0
        );

        control.set_mode(MODE_NORMAL);
        let session = match coordinator.renew_recovered_serving(awaiting) {
            CoordinateRenewOwnerSessionResultV1::Current(session) => session,
            result => panic!("fresh exact renew must create the new session: {result:?}"),
        };
        assert_eq!(session.validity().generation(), 2);
        let snapshot = event_snapshot(&events);
        let confirm = snapshot
            .iter()
            .position(|event| event == "journal:confirm:serving-publication")
            .unwrap();
        let reconcile = snapshot
            .iter()
            .rposition(|event| event == "control:reconcile")
            .unwrap();
        let pending = snapshot
            .iter()
            .rposition(|event| event == "journal:persist:PendingRenew")
            .unwrap();
        let renew = snapshot
            .iter()
            .rposition(|event| event == "control:renew-session")
            .unwrap();
        assert!(confirm < reconcile);
        assert!(reconcile < pending);
        assert!(pending < renew);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            3
        );
    }

    #[test]
    fn pending_publish_restart_source_continues_publish_then_fresh_renew() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.33:7000");
        let publication = ready.publication().clone();
        attempts.fail_persist_after_record("PendingPublish");
        let recovery = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("lost PendingPublish response must recover: {result:?}"),
        };
        drop(recovery);
        assert!(attempts.has_pending_publish());

        assert!(matches!(
            coordinator.resume_serving(publication.clone()),
            ResumeServingPublicationResultV1::RecoveryRequired(_)
        ));
        control.set_mode(MODE_RECONCILE_COMMITTED);
        let ready = match coordinator.resume_pending_publish(publication) {
            ResumeServingPublicationResultV1::PendingPublishReady(ready) => ready,
            result => panic!("source restart must only expose pending publish: {result:?}"),
        };
        assert!(attempts.has_pending_publish());
        control.set_mode(MODE_NORMAL);
        let session = match coordinator.continue_recovered_pending_publish(ready) {
            CoordinateRenewOwnerSessionResultV1::Current(session) => session,
            result => panic!("restarted publish must finish with fresh renew: {result:?}"),
        };
        assert_eq!(session.validity().generation(), 3);
        assert!(!attempts.has_pending_publish());
        let snapshot = event_snapshot(&events);
        let confirm = snapshot
            .iter()
            .position(|event| event == "journal:confirm:pending-publish")
            .unwrap();
        let reconcile = snapshot
            .iter()
            .rposition(|event| event == "control:reconcile")
            .unwrap();
        let publish = snapshot
            .iter()
            .rposition(|event| event == "control:publish-serving")
            .unwrap();
        let pending_renew = snapshot
            .iter()
            .rposition(|event| event == "journal:persist:PendingRenew")
            .unwrap();
        let renew = snapshot
            .iter()
            .rposition(|event| event == "control:renew-session")
            .unwrap();
        assert!(confirm < reconcile);
        assert!(reconcile < publish);
        assert!(publish < pending_renew);
        assert!(pending_renew < renew);
    }

    #[test]
    fn pending_publish_restart_target_promotes_then_requires_fresh_renew() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.34:7000");
        let publication = ready.publication().clone();
        control.set_mode(MODE_PUBLISH_UNKNOWN);
        let recovery = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("ambiguous publication must recover: {result:?}"),
        };
        drop(recovery);
        assert!(attempts.has_pending_publish());

        control.set_mode(MODE_RECONCILE_SERVING);
        let awaiting = match coordinator.resume_pending_publish(publication) {
            ResumeServingPublicationResultV1::AwaitingRenew(awaiting) => awaiting,
            result => panic!("published restart target must await renew: {result:?}"),
        };
        assert!(!attempts.has_pending_publish());
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "control:renew-session")
                .count(),
            0
        );
        control.set_mode(MODE_NORMAL);
        assert!(matches!(
            coordinator.renew_recovered_serving(awaiting),
            CoordinateRenewOwnerSessionResultV1::Current(_)
        ));
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "control:publish-serving")
                .count(),
            1
        );
    }

    #[test]
    fn renewal_expiry_and_foreign_evidence_retain_the_same_guard_for_recovery() {
        for (mode, endpoint) in [
            (MODE_RENEW_EXPIRED, "10.0.0.23:7000"),
            (MODE_RENEW_BINDING_MISMATCH, "10.0.0.24:7000"),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let attempts = FakeAttemptPort::new(Arc::clone(&events));
            let control = ScriptedControlStore::new(Arc::clone(&events));
            let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
            let adoption = FakeProviderAdoption {
                events: Arc::clone(&events),
            };
            let ready = publication_ready(&coordinator, &adoption, endpoint);
            let session = match coordinator.publish_serving(ready) {
                CoordinatePublishOwnerServingResultV1::Serving(session) => session,
                result => panic!("expected Serving session, got {result:?}"),
            };
            control.set_mode(mode);
            let recovery = match coordinator.renew(session) {
                CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery) => recovery,
                result => panic!("invalid renewal evidence must recover: {result:?}"),
            };
            assert_eq!(
                recovery.known_durable_stage(),
                OwnerAdmissionKnownDurableStageV1::ServingPublication
            );
            assert!(!event_snapshot(&events)
                .iter()
                .any(|event| event == "guard:drop"));
            assert_one_held_guard(&attempts);
        }
    }

    #[test]
    fn renew_current_source_descendant_cannot_clear_exact_pending_renew() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.35:7000");
        let session = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        let publication = session.publication().clone();
        let target =
            OwnerSessionRenewalTargetV1::new(publication.plan().clone(), session.claim().clone())
                .unwrap();
        let generation = session.validity().generation().checked_add(1).unwrap();
        control.set_mode(MODE_RENEW_SOURCE_DESCENDANT);

        let recovery = match coordinator.renew(session) {
            CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("source descendant must not renew target session: {result:?}"),
        };
        assert!(matches!(
            recovery.pending_mutation(),
            Some(OwnerAdmissionPendingMutationV1::Renew { .. })
        ));
        assert!(attempts.has_pending_renew());
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "journal:persist:RenewClosed")
                .count(),
            0
        );
        drop(recovery);

        control.set_mode(MODE_RECONCILE_SERVING);
        assert!(matches!(
            coordinator.resume_pending_renew(publication, target, generation),
            ResumePendingRenewResultV1::Recovered(
                CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(_)
            )
        ));
    }

    #[test]
    fn pending_renew_persist_failure_and_lost_ack_block_dispatch_until_exact_restart() {
        let failed_events = Arc::new(Mutex::new(Vec::new()));
        let failed_attempts = FakeAttemptPort::new(Arc::clone(&failed_events));
        let failed_control = ScriptedControlStore::new(Arc::clone(&failed_events));
        let failed = OwnerAdmissionCoordinatorV1::new(&failed_control, &failed_attempts);
        let failed_adoption = FakeProviderAdoption {
            events: Arc::clone(&failed_events),
        };
        let ready = publication_ready(&failed, &failed_adoption, "10.0.0.28:7000");
        let session = match failed.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        failed_attempts.fail_persist("PendingRenew");
        assert!(matches!(
            failed.renew(session),
            CoordinateRenewOwnerSessionResultV1::RecoveryRequired(_)
        ));
        assert_eq!(failed_attempts.logical_pending_renew_records(), 0);
        assert!(!event_snapshot(&failed_events)
            .iter()
            .any(|event| event == "control:renew-session"));

        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.29:7000");
        let session = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        let publication = session.publication().clone();
        let renewal_generation = session.validity().generation().checked_add(1).unwrap();
        let target =
            OwnerSessionRenewalTargetV1::new(publication.plan().clone(), session.claim().clone())
                .unwrap();
        attempts.fail_persist_after_record("PendingRenew");
        let recovery = match coordinator.renew(session) {
            CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown PendingRenew durability must recover: {result:?}"),
        };
        assert!(matches!(
            recovery.pending_mutation(),
            Some(OwnerAdmissionPendingMutationV1::Renew { .. })
        ));
        assert_eq!(attempts.logical_pending_renew_records(), 1);
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "control:renew-session"));
        drop(recovery);

        assert!(matches!(
            coordinator.resume_serving(publication.clone()),
            ResumeServingPublicationResultV1::RecoveryRequired(_)
        ));

        let reconcile_count = event_snapshot(&events)
            .iter()
            .filter(|event| event.as_str() == "control:reconcile")
            .count();
        assert!(matches!(
            coordinator.resume_plan(publication.plan().clone()),
            ResumeOwnerAdmissionResultV1::RecoveryRequired(_)
        ));
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "control:reconcile")
                .count(),
            reconcile_count
        );

        control.set_mode(MODE_RECONCILE_SERVING);
        let pending =
            match coordinator.resume_pending_renew(publication, target, renewal_generation) {
                ResumePendingRenewResultV1::Recovered(
                    CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending),
                ) => pending,
                ResumePendingRenewResultV1::Recovered(result) => {
                    panic!("restart must recover exact PendingRenew: {result:?}")
                }
                ResumePendingRenewResultV1::NotHeld(failure) => {
                    panic!("restart must reacquire exact PendingRenew: {failure:?}")
                }
            };
        control.set_mode(MODE_NORMAL);
        let renewed = match coordinator.continue_pending_mutation(pending) {
            ContinuePendingOwnerMutationResultV1::Renew(
                CoordinateRenewOwnerSessionResultV1::Current(session),
            ) => session,
            result => panic!("restart must close PendingRenew: {result:?}"),
        };
        assert_eq!(renewed.validity().generation(), renewal_generation);
        let snapshot = event_snapshot(&events);
        assert_eq!(attempts.logical_pending_renew_records(), 1);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "journal:persist:PendingRenew")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:renew-session")
                .count(),
            1
        );
        let confirm = snapshot
            .iter()
            .position(|event| event == "journal:confirm:pending-renew")
            .unwrap();
        let reconcile = snapshot
            .iter()
            .rposition(|event| event == "control:reconcile")
            .unwrap();
        assert!(confirm < reconcile);
    }

    #[test]
    fn renew_closed_persist_failure_keeps_pending_across_restart_and_fresh_renew() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.30:7000");
        let session = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        let publication = session.publication().clone();
        let target =
            OwnerSessionRenewalTargetV1::new(publication.plan().clone(), session.claim().clone())
                .unwrap();
        let renewal_generation = session.validity().generation().checked_add(1).unwrap();
        attempts.fail_persist("RenewClosed");

        let recovery = match coordinator.renew(session) {
            CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("failed closed receipt must retain PendingRenew: {result:?}"),
        };
        assert!(matches!(
            recovery.pending_mutation(),
            Some(OwnerAdmissionPendingMutationV1::Renew { .. })
        ));
        assert_eq!(attempts.logical_pending_renew_records(), 1);
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "control:renew-session")
                .count(),
            1
        );
        drop(recovery);
        attempts.allow_persist();

        control.set_mode(MODE_RECONCILE_SERVING);
        let pending =
            match coordinator.resume_pending_renew(publication, target, renewal_generation) {
                ResumePendingRenewResultV1::Recovered(
                    CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending),
                ) => pending,
                ResumePendingRenewResultV1::Recovered(result) => {
                    panic!("restart must retain the possibly-dispatched renew: {result:?}")
                }
                ResumePendingRenewResultV1::NotHeld(failure) => {
                    panic!("restart must reacquire the pending renew: {failure:?}")
                }
            };
        control.set_mode(MODE_NORMAL);
        let renewed = match coordinator.continue_pending_mutation(pending) {
            ContinuePendingOwnerMutationResultV1::Renew(
                CoordinateRenewOwnerSessionResultV1::Current(session),
            ) => session,
            result => panic!("fresh exact renew must close the pending generation: {result:?}"),
        };
        assert_eq!(renewed.validity().generation(), renewal_generation);
        let snapshot = event_snapshot(&events);
        assert_eq!(attempts.logical_pending_renew_records(), 1);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:renew-session")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "journal:persist:PendingRenew")
                .count(),
            2
        );
    }

    #[test]
    fn backend_renew_outcome_unknown_stays_pending_until_restart_fresh_renew() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let adoption = FakeProviderAdoption {
            events: Arc::clone(&events),
        };
        let ready = publication_ready(&coordinator, &adoption, "10.0.0.31:7000");
        let session = match coordinator.publish_serving(ready) {
            CoordinatePublishOwnerServingResultV1::Serving(session) => session,
            result => panic!("expected Serving session, got {result:?}"),
        };
        let publication = session.publication().clone();
        let target =
            OwnerSessionRenewalTargetV1::new(publication.plan().clone(), session.claim().clone())
                .unwrap();
        let renewal_generation = session.validity().generation().checked_add(1).unwrap();
        control.set_mode(MODE_RENEW_UNKNOWN);

        let recovery = match coordinator.renew(session) {
            CoordinateRenewOwnerSessionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("backend renew ambiguity must not issue a session: {result:?}"),
        };
        assert!(matches!(
            recovery.pending_mutation(),
            Some(OwnerAdmissionPendingMutationV1::Renew { .. })
        ));
        assert_eq!(attempts.logical_pending_renew_records(), 1);
        let before_restart = event_snapshot(&events);
        let pending_index = before_restart
            .iter()
            .position(|event| event == "journal:persist:PendingRenew")
            .unwrap();
        let dispatch_index = before_restart
            .iter()
            .position(|event| event == "control:renew-session")
            .unwrap();
        let closed_index = before_restart
            .iter()
            .position(|event| event == "journal:persist:RenewClosed")
            .unwrap();
        assert!(pending_index < dispatch_index);
        assert!(dispatch_index < closed_index);
        drop(recovery);

        control.set_mode(MODE_RECONCILE_SERVING);
        let pending =
            match coordinator.resume_pending_renew(publication, target, renewal_generation) {
                ResumePendingRenewResultV1::Recovered(
                    CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending),
                ) => pending,
                ResumePendingRenewResultV1::Recovered(result) => {
                    panic!("restart must preserve backend renew ambiguity: {result:?}")
                }
                ResumePendingRenewResultV1::NotHeld(failure) => {
                    panic!("restart must reacquire backend renew ambiguity: {failure:?}")
                }
            };
        control.set_mode(MODE_NORMAL);
        let renewed = match coordinator.continue_pending_mutation(pending) {
            ContinuePendingOwnerMutationResultV1::Renew(
                CoordinateRenewOwnerSessionResultV1::Current(session),
            ) => session,
            result => panic!("fresh exact renew must close backend ambiguity: {result:?}"),
        };
        assert_eq!(renewed.validity().generation(), renewal_generation);
        let snapshot = event_snapshot(&events);
        assert_eq!(attempts.logical_pending_renew_records(), 1);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:renew-session")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "journal:persist:RenewClosed")
                .count(),
            2
        );
    }

    #[test]
    fn lost_intent_persist_response_replays_same_record_without_reacquire() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        attempts.fail_persist_after_record("Intent");
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);

        let recovery = match coordinator.begin(intent("10.0.0.9:7000")) {
            OpenDurableOwnerAdmissionIntentResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("lost response must retain exact intent: {result:?}"),
        };
        assert_eq!(
            recovery.known_durable_stage(),
            OwnerAdmissionKnownDurableStageV1::None
        );
        assert_eq!(attempts.logical_intent_records(), 1);

        let recovered = coordinator.recover(recovery);
        match recovered {
            CoordinateRecoverOwnerAdmissionResultV1::Reconciled(result) => match result.as_ref() {
                CoordinateReconcileOwnerAdmissionResultV1::Recorded { continuation, .. } => {
                    assert!(matches!(
                        continuation.as_ref(),
                        DurableOwnerAdmissionContinuationV1::Plan(_)
                    ));
                }
                result => panic!("expected recorded plan, got {result:?}"),
            },
            result => panic!("expected reconciled plan, got {result:?}"),
        }
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "journal:persist:Intent")
                .count(),
            2
        );
        assert_eq!(attempts.logical_intent_records(), 1);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
        assert!(!snapshot
            .iter()
            .any(|event| event == "journal:confirm:intent"));
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn failed_plan_sync_retains_intent_plan_and_same_guard_for_recovery() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        attempts.fail_persist("Prepared");
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);

        let durable_intent = begin_ready(&coordinator, intent("10.0.0.2:7000"));
        let recovery = match coordinator.prepare(durable_intent) {
            CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("expected recovery, got {result:?}"),
        };
        assert_eq!(
            recovery.known_durable_stage(),
            OwnerAdmissionKnownDurableStageV1::Intent
        );
        assert!(recovery.observed_plan().is_some());
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "guard:drop"));

        attempts.allow_persist();
        let recovered = coordinator.recover(recovery);
        match recovered {
            CoordinateRecoverOwnerAdmissionResultV1::Reconciled(result) => match result.as_ref() {
                CoordinateReconcileOwnerAdmissionResultV1::Recorded { continuation, .. } => {
                    assert!(matches!(
                        continuation.as_ref(),
                        DurableOwnerAdmissionContinuationV1::Plan(_)
                    ));
                }
                result => panic!("expected recorded plan, got {result:?}"),
            },
            result => panic!("expected reconciled plan, got {result:?}"),
        }
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
        assert!(snapshot.iter().any(|event| event == "control:reconcile"));
        assert!(!snapshot.iter().any(|event| event == "control:commit"));
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn committed_receipt_failure_and_unknown_never_issue_open_permit() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);

        let durable_plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.3:7000")),
        );
        attempts.fail_persist("Committed");
        let recovery = match coordinator.commit(durable_plan) {
            CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("receipt failure must not open provider: {result:?}"),
        };
        assert_eq!(
            recovery.known_durable_stage(),
            OwnerAdmissionKnownDurableStageV1::Plan
        );
        assert!(recovery.observed_plan().is_some());
        attempts.allow_persist();
        drop(coordinator.recover(recovery));
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );

        let second_events = Arc::new(Mutex::new(Vec::new()));
        let second_attempts = FakeAttemptPort::new(Arc::clone(&second_events));
        let second_control = ScriptedControlStore::new(Arc::clone(&second_events));
        let second = OwnerAdmissionCoordinatorV1::new(&second_control, &second_attempts);
        let plan = prepare_ready(&second, begin_ready(&second, intent("10.0.0.4:7000")));
        second_control.set_mode(MODE_COMMIT_UNKNOWN);
        let unknown = match second.commit(plan) {
            CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown commit must require recovery: {result:?}"),
        };
        assert!(matches!(
            unknown.cause(),
            OwnerAdmissionRecoveryCauseV1::BackendOutcomeUnknown {
                operation: OwnerAdmissionCoordinatorOperationV1::Commit
            }
        ));
        assert!(event_snapshot(&second_events)
            .iter()
            .any(|event| event == "journal:persist:CommitClosed"));
        second_control.set_mode(MODE_NORMAL);
        drop(second.recover(unknown));
        assert_eq!(
            event_snapshot(&second_events)
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
    }

    #[test]
    fn post_commit_reservation_loss_blocks_receipt_and_open_permit() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        attempts.fail_validate_at(5);
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.5:7000")),
        );

        let recovery = match coordinator.commit(plan) {
            CoordinateCommitOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("lost post-binding must recover: {result:?}"),
        };
        assert!(matches!(
            recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::AttemptPort {
                operation: OwnerAdmissionCoordinatorOperationV1::Commit,
                error: OwnerAdmissionAttemptPortErrorV1::ReservationNotHeld,
            }
        ));
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "journal:persist:Committed"));
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "guard:drop"));
    }

    #[test]
    fn witness_failure_is_recorded_and_recovers_without_reacquire() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        control.set_mode(MODE_PREPARE_BINDING_MISMATCH);
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let durable = begin_ready(&coordinator, intent("10.0.0.6:7000"));
        let recovery = match coordinator.prepare(durable) {
            CoordinatePrepareOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("forged result must recover: {result:?}"),
        };
        assert!(matches!(
            recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::WitnessRejected {
                operation: OwnerAdmissionCoordinatorOperationV1::Prepare,
                code: OwnerAdmissionWitnessFailureCodeV1::ResultBindingMismatch,
            }
        ));
        assert!(event_snapshot(&events)
            .iter()
            .any(|event| event == "journal:persist:RecoveryRequired"));
        control.set_mode(MODE_NORMAL);
        drop(coordinator.recover(recovery));
        assert_eq!(
            event_snapshot(&events)
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
    }

    #[test]
    fn pending_abort_response_loss_blocks_control_and_retries_same_logical_record() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.13:7000")),
        );
        let reason = OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit;
        attempts.fail_persist_after_record("PendingAbort");

        let recovery = match coordinator.abort(plan, reason) {
            CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown pending-abort durability must recover: {result:?}"),
        };
        assert!(matches!(
            recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::AttemptPort {
                operation: OwnerAdmissionCoordinatorOperationV1::Abort,
                error: OwnerAdmissionAttemptPortErrorV1::DurabilityOutcomeUnknown,
            }
        ));
        assert_eq!(
            recovery.pending_mutation(),
            Some(&OwnerAdmissionPendingMutationV1::Abort(reason))
        );
        let before_recovery = event_snapshot(&events);
        assert_eq!(
            before_recovery
                .iter()
                .filter(|event| event.as_str() == "journal:persist:PendingAbort")
                .count(),
            1
        );
        assert!(!before_recovery.iter().any(|event| event == "control:abort"));
        assert_eq!(attempts.logical_pending_abort_records(), 1);

        let pending = match coordinator.recover(recovery) {
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending) => pending,
            result => panic!("pending abort must remain opaque after reconcile: {result:?}"),
        };
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "control:abort"));
        let continued = coordinator.continue_pending_mutation(pending);
        assert!(matches!(
            continued,
            ContinuePendingOwnerMutationResultV1::Abort(
                CoordinateAbortOwnerAdmissionResultV1::Recorded { .. }
            )
        ));
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "journal:persist:PendingAbort")
                .count(),
            2
        );
        assert_eq!(attempts.logical_pending_abort_records(), 1);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:abort")
                .count(),
            1
        );
        assert!(
            snapshot
                .iter()
                .rposition(|event| event == "journal:persist:PendingAbort")
                .unwrap()
                < snapshot
                    .iter()
                    .position(|event| event == "control:abort")
                    .unwrap()
        );
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn pending_terminate_persist_failure_blocks_control_until_exact_retry() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.14:7000")),
        );
        attempts.fail_persist("PendingTerminate");

        let recovery = match coordinator.terminate_released(plan) {
            CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("failed pending-terminate durability must recover: {result:?}"),
        };
        assert_eq!(
            recovery.pending_mutation(),
            Some(&OwnerAdmissionPendingMutationV1::Terminate(
                OwnerAdmissionTerminationReasonV1::Released,
            ))
        );
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "control:terminate"));
        assert_eq!(attempts.logical_pending_terminate_records(), 0);

        attempts.allow_persist();
        let pending = match coordinator.recover(recovery) {
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending) => pending,
            result => panic!("pending termination must remain opaque after reconcile: {result:?}"),
        };
        assert!(!event_snapshot(&events)
            .iter()
            .any(|event| event == "control:terminate"));
        let continued = coordinator.continue_pending_mutation(pending);
        assert!(matches!(
            continued,
            ContinuePendingOwnerMutationResultV1::Terminate(
                CoordinateTerminateOwnerAdmissionResultV1::Recorded { .. }
            )
        ));
        let snapshot = event_snapshot(&events);
        assert_eq!(attempts.logical_pending_terminate_records(), 1);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:terminate")
                .count(),
            1
        );
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn not_dispatched_abort_and_terminate_recovery_stays_opaque() {
        let abort_events = Arc::new(Mutex::new(Vec::new()));
        let abort_attempts = FakeAttemptPort::new(Arc::clone(&abort_events));
        let abort_control = ScriptedControlStore::new(Arc::clone(&abort_events));
        let abort_coordinator = OwnerAdmissionCoordinatorV1::new(&abort_control, &abort_attempts);
        let abort_plan = prepare_ready(
            &abort_coordinator,
            begin_ready(&abort_coordinator, intent("10.0.0.15:7000")),
        );
        abort_control.set_mode(MODE_ABORT_NOT_DISPATCHED);
        let abort_recovery = match abort_coordinator.abort(
            abort_plan,
            OwnerAdmissionAbortReasonV1::RuntimeReservationLost,
        ) {
            CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("not-dispatched abort must retain pending state: {result:?}"),
        };
        assert!(matches!(
            abort_recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::PendingMutationUnresolved
        ));
        abort_control.set_mode(MODE_NORMAL);
        assert!(matches!(
            abort_coordinator.recover(abort_recovery),
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(_)
        ));
        let abort_snapshot = event_snapshot(&abort_events);
        assert!(
            abort_snapshot
                .iter()
                .position(|event| event == "journal:persist:PendingAbort")
                .unwrap()
                < abort_snapshot
                    .iter()
                    .position(|event| event == "control:abort")
                    .unwrap()
        );

        let terminate_events = Arc::new(Mutex::new(Vec::new()));
        let terminate_attempts = FakeAttemptPort::new(Arc::clone(&terminate_events));
        let terminate_control = ScriptedControlStore::new(Arc::clone(&terminate_events));
        let terminate_coordinator =
            OwnerAdmissionCoordinatorV1::new(&terminate_control, &terminate_attempts);
        let terminate_plan = prepare_ready(
            &terminate_coordinator,
            begin_ready(&terminate_coordinator, intent("10.0.0.16:7000")),
        );
        terminate_control.set_mode(MODE_TERMINATE_NOT_DISPATCHED);
        let terminate_recovery = match terminate_coordinator.terminate_released(terminate_plan) {
            CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("not-dispatched terminate must retain pending state: {result:?}"),
        };
        assert!(matches!(
            terminate_recovery.cause(),
            OwnerAdmissionRecoveryCauseV1::PendingMutationUnresolved
        ));
        terminate_control.set_mode(MODE_NORMAL);
        assert!(matches!(
            terminate_coordinator.recover(terminate_recovery),
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(_)
        ));
        let terminate_snapshot = event_snapshot(&terminate_events);
        assert!(
            terminate_snapshot
                .iter()
                .position(|event| event == "journal:persist:PendingTerminate")
                .unwrap()
                < terminate_snapshot
                    .iter()
                    .position(|event| event == "control:terminate")
                    .unwrap()
        );
    }

    #[test]
    fn abort_unknown_recovery_keeps_exact_pending_reason_without_replay() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.10:7000")),
        );
        let reason = OwnerAdmissionAbortReasonV1::OwnerCasRejected;

        control.set_mode(MODE_ABORT_UNKNOWN);
        let recovery = match coordinator.abort(plan, reason) {
            CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown abort must retain recovery: {result:?}"),
        };
        assert_eq!(
            recovery.pending_mutation(),
            Some(&OwnerAdmissionPendingMutationV1::Abort(reason))
        );

        control.set_mode(MODE_NORMAL);
        let pending = match coordinator.recover(recovery) {
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending) => pending,
            result => panic!("recovery lost the exact pending abort: {result:?}"),
        };
        assert_eq!(
            pending.pending_mutation(),
            &OwnerAdmissionPendingMutationV1::Abort(reason)
        );
        assert_eq!(
            pending.observation(),
            RecoveredPendingOwnerMutationObservationV1::Prepared
        );
        let before_continue = event_snapshot(&events);
        assert_eq!(
            before_continue
                .iter()
                .filter(|event| event.as_str() == "control:abort")
                .count(),
            1
        );
        let continued = coordinator.continue_pending_mutation(pending);
        match continued {
            ContinuePendingOwnerMutationResultV1::Abort(
                CoordinateAbortOwnerAdmissionResultV1::Recorded { result, .. },
            ) => assert!(matches!(
                result.as_ref(),
                AbortOwnerAdmissionResultV1::Aborted { .. }
            )),
            result => panic!("expected exact abort continuation, got {result:?}"),
        }
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:abort")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn abort_unknown_observing_committed_stays_opaque_and_does_not_replay() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.12:7000")),
        );
        let reason = OwnerAdmissionAbortReasonV1::RuntimeReservationLost;

        control.set_mode(MODE_ABORT_UNKNOWN);
        let recovery = match coordinator.abort(plan, reason) {
            CoordinateAbortOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown abort must retain recovery: {result:?}"),
        };
        control.set_mode(MODE_RECONCILE_COMMITTED);
        let pending = match coordinator.recover(recovery) {
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending) => pending,
            result => panic!("committed observation must remain opaque: {result:?}"),
        };
        assert_eq!(
            pending.observation(),
            RecoveredPendingOwnerMutationObservationV1::Committed
        );

        let terminal = match coordinator.continue_pending_mutation(pending) {
            ContinuePendingOwnerMutationResultV1::Terminal(terminal) => terminal,
            result => panic!("committed plan must not replay abort or expose permit: {result:?}"),
        };
        assert_eq!(
            terminal.pending_mutation(),
            &OwnerAdmissionPendingMutationV1::Abort(reason)
        );
        assert_eq!(
            terminal.observation(),
            RecoveredPendingOwnerMutationObservationV1::Committed
        );
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:abort")
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn terminate_unknown_recovery_keeps_exact_pending_reason_without_replay() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.11:7000")),
        );

        control.set_mode(MODE_TERMINATE_UNKNOWN);
        let recovery = match coordinator.terminate_released(plan) {
            CoordinateTerminateOwnerAdmissionResultV1::RecoveryRequired(recovery) => recovery,
            result => panic!("unknown terminate must retain recovery: {result:?}"),
        };
        assert_eq!(
            recovery.pending_mutation(),
            Some(&OwnerAdmissionPendingMutationV1::Terminate(
                OwnerAdmissionTerminationReasonV1::Released,
            ))
        );

        control.set_mode(MODE_RECONCILE_COMMITTED);
        let pending = match coordinator.recover(recovery) {
            CoordinateRecoverOwnerAdmissionResultV1::PendingMutation(pending) => pending,
            result => panic!("recovery lost the exact pending termination: {result:?}"),
        };
        assert_eq!(
            pending.pending_mutation(),
            &OwnerAdmissionPendingMutationV1::Terminate(
                OwnerAdmissionTerminationReasonV1::Released,
            )
        );
        assert_eq!(
            pending.observation(),
            RecoveredPendingOwnerMutationObservationV1::Committed
        );
        let before_continue = event_snapshot(&events);
        assert_eq!(
            before_continue
                .iter()
                .filter(|event| event.as_str() == "control:terminate")
                .count(),
            1
        );
        let continued = coordinator.continue_pending_mutation(pending);
        match continued {
            ContinuePendingOwnerMutationResultV1::Terminate(
                CoordinateTerminateOwnerAdmissionResultV1::Recorded { result, .. },
            ) => assert!(matches!(
                result.as_ref(),
                TerminateOwnerAdmissionResultV1::Terminated { .. }
            )),
            result => panic!("expected exact terminate continuation, got {result:?}"),
        }
        let snapshot = event_snapshot(&events);
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "control:terminate")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
        assert_one_held_guard(&attempts);
    }

    #[test]
    fn restart_confirms_exact_plan_then_reconciles_without_commit() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let prepared = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.7:7000")),
        );
        let exact_plan = prepared.plan().clone();
        drop(prepared);
        events.lock().unwrap().clear();

        let resumed = coordinator.resume_plan(exact_plan);
        assert!(matches!(
            resumed,
            ResumeOwnerAdmissionResultV1::Reconciled(_)
        ));
        let snapshot = event_snapshot(&events);
        let confirm = snapshot
            .iter()
            .position(|event| event == "journal:confirm:plan")
            .unwrap();
        let reconcile = snapshot
            .iter()
            .position(|event| event == "control:reconcile")
            .unwrap();
        assert!(confirm < reconcile);
        assert!(!snapshot.iter().any(|event| event == "control:commit"));
        assert!(!snapshot.iter().any(|event| event == "control:prepare"));
    }

    #[test]
    fn lease_expiry_termination_requires_verified_reconcile_permit() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let attempts = FakeAttemptPort::new(Arc::clone(&events));
        let control = ScriptedControlStore::new(Arc::clone(&events));
        let coordinator = OwnerAdmissionCoordinatorV1::new(&control, &attempts);
        let plan = prepare_ready(
            &coordinator,
            begin_ready(&coordinator, intent("10.0.0.8:7000")),
        );
        control.set_mode(MODE_RECONCILE_EXPIRED);
        let expired = match coordinator.reconcile(DurableOwnerAdmissionContinuationV1::Plan(plan)) {
            CoordinateReconcileOwnerAdmissionResultV1::LeaseExpired(expired) => expired,
            result => panic!("expected verified expiry, got {result:?}"),
        };
        let terminated = coordinator.terminate_expired(*expired);
        match terminated {
            CoordinateTerminateOwnerAdmissionResultV1::Recorded { result, .. } => {
                assert!(matches!(
                    result.as_ref(),
                    TerminateOwnerAdmissionResultV1::Terminated { .. }
                ));
            }
            result => panic!("expected exact expiry termination, got {result:?}"),
        }
        let snapshot = event_snapshot(&events);
        assert!(snapshot
            .iter()
            .any(|event| event == "journal:persist:ExpiredCommitted"));
        assert!(snapshot
            .iter()
            .any(|event| event == "journal:persist:Terminated"));
        assert_eq!(
            snapshot
                .iter()
                .filter(|event| event.as_str() == "port:acquire")
                .count(),
            1
        );
    }
}
