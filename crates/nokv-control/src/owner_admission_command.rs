// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! One-shot backend commands for planned owner admission.
//!
//! A forwarding backend may inspect a command and move it to its delegate. The
//! ultimate backend consumes `claim_execution` before its first backend access.
//! Only that claimed command can complete an outcome. A coordinator witness
//! accepts the outcome only when both the allocation identity and the returned
//! durable values exactly match the command payload. Positive-TTL lifetime
//! observations also retain the private command-core Arc that created them;
//! moving or cloning an older observation into a newer command is rejected by
//! the newer witness even when plan, record, and session bindings are equal.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

#[cfg(test)]
use crate::owner_admission::OwnerAdmissionClaimPhaseV1;
use crate::owner_admission::{
    OwnerAdmissionAbortReasonV1, OwnerAdmissionClaimDigestV1, OwnerAdmissionClaimV1,
    OwnerAdmissionIntentDigestV1, OwnerAdmissionIntentV1, OwnerAdmissionPlanDigestV1,
    OwnerAdmissionPlanSentinelV1, OwnerAdmissionRecordDigestV1, OwnerAdmissionTerminationReasonV1,
    OwnerLeaseExpiryEvidenceDigest, OwnerServingPublicationDigestV1, OwnerSessionBindingDigestV1,
    OwnerSessionLifetimeObservationV1, OwnerSessionLifetimeProofDigestV1,
    OwnerSessionRenewalTargetDigestV1, OwnerSessionRenewalTargetV1, PlannedOwnerAdmissionV1,
    PlannedOwnerServingPublicationV1,
};
use crate::owner_admission_state::validate_committed_record_descendant;
use crate::owner_admission_state::OwnerAdmissionInconsistencyCode;
#[cfg(test)]
use crate::owner_admission_state::{
    expected_recovering_record_for_plan, validate_terminated_record_descendant,
};
#[cfg(test)]
use crate::store::validate_logical_shard_record;
#[cfg(test)]
use crate::LogicalShardState;
#[cfg(test)]
use crate::OperationId;
use crate::{
    LogicalShardId, LogicalShardLease, LogicalShardRecord, OwnerIncarnationId, RecoveryPublication,
};

const EXECUTION_MINTED: u8 = 0;
const EXECUTION_CLAIMED: u8 = 1;
const EXECUTION_COMPLETED: u8 = 2;

/// Crate-private nominal identity for one command allocation. A positive-TTL
/// observation retains this exact Arc; no raw identity or constructor is
/// exposed to a forwarding backend.
pub(crate) struct OwnerAdmissionCommandCore {
    state: AtomicU8,
}

impl OwnerAdmissionCommandCore {
    #[cfg(test)]
    fn mint() -> Self {
        Self {
            state: AtomicU8::new(EXECUTION_MINTED),
        }
    }

    fn claim(&self) {
        let prior = self.state.compare_exchange(
            EXECUTION_MINTED,
            EXECUTION_CLAIMED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        assert!(
            prior.is_ok(),
            "an owner-admission command allocation was claimed more than once"
        );
    }

    fn complete(&self) {
        let prior = self.state.compare_exchange(
            EXECUTION_CLAIMED,
            EXECUTION_COMPLETED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        assert!(
            prior.is_ok(),
            "an owner-admission command was completed without its exact claim"
        );
    }

    #[cfg(test)]
    fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) == EXECUTION_COMPLETED
    }

    fn state_name(&self) -> &'static str {
        match self.state.load(Ordering::Acquire) {
            EXECUTION_MINTED => "minted",
            EXECUTION_CLAIMED => "claimed",
            EXECUTION_COMPLETED => "completed",
            _ => "invalid",
        }
    }
}

struct CommandEnvelope<P> {
    core: Arc<OwnerAdmissionCommandCore>,
    payload: P,
}

struct ClaimedCommandEnvelope<P> {
    core: Arc<OwnerAdmissionCommandCore>,
    payload: P,
}

struct CommandOutcomeEnvelope<R> {
    core: Arc<OwnerAdmissionCommandCore>,
    result: R,
}

#[cfg(test)]
struct CommandWitnessEnvelope<P> {
    core: Arc<OwnerAdmissionCommandCore>,
    payload: P,
}

#[cfg(test)]
fn mint_command<P: Clone>(payload: P) -> (CommandEnvelope<P>, CommandWitnessEnvelope<P>) {
    let core = Arc::new(OwnerAdmissionCommandCore::mint());
    (
        CommandEnvelope {
            core: Arc::clone(&core),
            payload: payload.clone(),
        },
        CommandWitnessEnvelope { core, payload },
    )
}

fn claim_command<P>(command: CommandEnvelope<P>) -> ClaimedCommandEnvelope<P> {
    command.core.claim();
    ClaimedCommandEnvelope {
        core: command.core,
        payload: command.payload,
    }
}

fn complete_command<P, R>(
    command: ClaimedCommandEnvelope<P>,
    result: R,
) -> CommandOutcomeEnvelope<R> {
    command.core.complete();
    CommandOutcomeEnvelope {
        core: command.core,
        result,
    }
}

#[cfg(test)]
fn resolve_command<P, R>(
    witness: CommandWitnessEnvelope<P>,
    outcome: CommandOutcomeEnvelope<R>,
    validate: impl FnOnce(&P, &R, &Arc<OwnerAdmissionCommandCore>) -> bool,
) -> Result<R, CommandWitnessFailure<P>> {
    if !Arc::ptr_eq(&witness.core, &outcome.core) {
        return Err(CommandWitnessFailure {
            code: OwnerAdmissionCommandWitnessError::ForeignCommand,
            recovery_target: witness.payload,
        });
    }
    if !outcome.core.is_completed() {
        return Err(CommandWitnessFailure {
            code: OwnerAdmissionCommandWitnessError::ExecutionNotCompleted,
            recovery_target: witness.payload,
        });
    }
    if !validate(&witness.payload, &outcome.result, &witness.core) {
        return Err(CommandWitnessFailure {
            code: OwnerAdmissionCommandWitnessError::ResultBindingMismatch,
            recovery_target: witness.payload,
        });
    }
    Ok(outcome.result)
}

#[cfg(test)]
struct CommandWitnessFailure<P> {
    code: OwnerAdmissionCommandWitnessError,
    recovery_target: P,
}

#[cfg(test)]
impl<P> fmt::Debug for CommandWitnessFailure<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandWitnessFailure")
            .field("code", &self.code)
            .finish_non_exhaustive()
    }
}

/// Closed coordinator error for a forged or inconsistent backend outcome.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionCommandWitnessError {
    /// The outcome belongs to another independently minted command.
    ForeignCommand,
    /// The exact allocation did not pass through claim and completion.
    ExecutionNotCompleted,
    /// A returned claim, plan, lease, record, or ambiguity identity does not
    /// exactly bind the witnessed command payload.
    ResultBindingMismatch,
}

#[cfg(test)]
impl fmt::Display for OwnerAdmissionCommandWitnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ForeignCommand => "owner-admission outcome belongs to a foreign command",
            Self::ExecutionNotCompleted => "owner-admission command execution was not completed",
            Self::ResultBindingMismatch => {
                "owner-admission outcome does not exactly bind its command"
            }
        })
    }
}

#[cfg(test)]
impl std::error::Error for OwnerAdmissionCommandWitnessError {}

/// Exact identity retained when a backend operation may have been dispatched
/// but no terminal result can be proved.
///
/// Construction is private. An ultimate backend obtains this value only from
/// its claimed command, so it cannot insert a free-form source or substitute a
/// caller-chosen identity.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerAdmissionOutcomeUnknownV1 {
    logical_shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
    intent_digest: OwnerAdmissionIntentDigestV1,
    plan_digest: Option<OwnerAdmissionPlanDigestV1>,
    serving_publication_digest: Option<OwnerServingPublicationDigestV1>,
    source_record_digest: Option<OwnerAdmissionRecordDigestV1>,
    target_record_digest: Option<OwnerAdmissionRecordDigestV1>,
}

impl OwnerAdmissionOutcomeUnknownV1 {
    fn for_intent(intent: &OwnerAdmissionIntentV1) -> Self {
        Self {
            logical_shard_id: intent.logical_shard_id(),
            owner_incarnation_id: intent.owner_incarnation_id(),
            intent_digest: intent.digest(),
            plan_digest: None,
            serving_publication_digest: None,
            source_record_digest: None,
            target_record_digest: None,
        }
    }

    fn for_plan(plan: &PlannedOwnerAdmissionV1) -> Self {
        Self {
            logical_shard_id: plan.intent().logical_shard_id(),
            owner_incarnation_id: plan.intent().owner_incarnation_id(),
            intent_digest: plan.intent().digest(),
            plan_digest: Some(plan.digest()),
            serving_publication_digest: None,
            source_record_digest: None,
            target_record_digest: None,
        }
    }

    fn for_serving_publication(publication: &PlannedOwnerServingPublicationV1) -> Self {
        Self {
            logical_shard_id: publication.plan().intent().logical_shard_id(),
            owner_incarnation_id: publication.plan().intent().owner_incarnation_id(),
            intent_digest: publication.plan().intent().digest(),
            plan_digest: Some(publication.plan().digest()),
            serving_publication_digest: Some(publication.digest()),
            source_record_digest: Some(publication.source_digest()),
            target_record_digest: Some(publication.target_digest()),
        }
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn owner_incarnation_id(&self) -> OwnerIncarnationId {
        self.owner_incarnation_id
    }

    pub const fn intent_digest(&self) -> OwnerAdmissionIntentDigestV1 {
        self.intent_digest
    }

    pub const fn plan_digest(&self) -> Option<OwnerAdmissionPlanDigestV1> {
        self.plan_digest
    }

    pub const fn serving_publication_digest(&self) -> Option<OwnerServingPublicationDigestV1> {
        self.serving_publication_digest
    }

    pub const fn source_record_digest(&self) -> Option<OwnerAdmissionRecordDigestV1> {
        self.source_record_digest
    }

    pub const fn target_record_digest(&self) -> Option<OwnerAdmissionRecordDigestV1> {
        self.target_record_digest
    }
}

impl fmt::Debug for OwnerAdmissionOutcomeUnknownV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerAdmissionOutcomeUnknownV1")
            .field("plan_bound", &self.plan_digest.is_some())
            .field(
                "serving_publication_bound",
                &self.serving_publication_digest.is_some(),
            )
            .finish_non_exhaustive()
    }
}

macro_rules! not_dispatched_code {
    ($(#[$meta:meta])* $name:ident { $($(#[$variant_meta:meta])* $variant:ident),+ $(,)? }) => {
        $(#[$meta])*
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }
    };
}

not_dispatched_code!(
    /// Exact reason why prepare proved that it dispatched no effect.
    PrepareOwnerAdmissionNotDispatchedV1 {
        InvalidInputBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        RuntimeReservationLostBeforeEffect,
        ControlBindingLostBeforeEffect,
    }
);

not_dispatched_code!(
    /// Exact reason why commit proved that it dispatched no effect.
    CommitOwnerAdmissionNotDispatchedV1 {
        InvalidPlanBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        RuntimeReservationLostBeforeEffect,
        PreparedBindingLostBeforeEffect,
    }
);

not_dispatched_code!(
    /// Exact reason why abort proved that it dispatched no effect.
    AbortOwnerAdmissionNotDispatchedV1 {
        InvalidPlanBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        PreparedBindingLostBeforeEffect,
    }
);

not_dispatched_code!(
    /// Exact reason why terminate proved that it dispatched no effect.
    TerminateOwnerAdmissionNotDispatchedV1 {
        InvalidPlanBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        RuntimeReservationLostBeforeEffect,
        ExactOwnerBindingLostBeforeEffect,
    }
);

not_dispatched_code!(
    /// Exact reason why reconcile proved that it dispatched no read or effect.
    ReconcileOwnerAdmissionNotDispatchedV1 {
        InvalidTargetBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        RuntimeReservationLostBeforeEffect,
        ControlBindingLostBeforeEffect,
    }
);

not_dispatched_code!(
    /// Exact reason why a serving publication proved that it dispatched no
    /// read or effect.
    PublishOwnerServingNotDispatchedV1 {
        InvalidPublicationBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        RuntimeReservationLostBeforeEffect,
        ExactOwnerBindingLostBeforeEffect,
    }
);

not_dispatched_code!(
    /// Exact reason why a session renewal proved that it dispatched no read
    /// or keepalive effect.
    RenewOwnerSessionNotDispatchedV1 {
        InvalidTargetBeforeEffect,
        CodecRejectedBeforeEffect,
        BackendUnavailableBeforeEffect,
        RuntimeReservationLostBeforeEffect,
        ExactSessionBindingLostBeforeEffect,
    }
);

/// Closed result of one prepare attempt.
pub enum PrepareOwnerAdmissionResultV1 {
    Prepared {
        plan: Box<PlannedOwnerAdmissionV1>,
        claim: OwnerAdmissionClaimV1,
        sentinel: OwnerAdmissionPlanSentinelV1,
    },
    /// The exact Prepared plan remains durable but its lease-attached sentinel
    /// is authoritatively absent, so the plan must not be committed.
    ExpiredPrepared {
        plan: Box<PlannedOwnerAdmissionV1>,
        claim: OwnerAdmissionClaimV1,
        expected_sentinel: OwnerAdmissionPlanSentinelV1,
    },
    Rejected {
        claim: OwnerAdmissionClaimV1,
    },
    /// The exact intent already reached a post-Prepared durable claim phase.
    /// Rejected claims use `Rejected`; live and expired Prepared claims use
    /// their dedicated variants.
    DurableConflict {
        plan: Box<PlannedOwnerAdmissionV1>,
        claim: OwnerAdmissionClaimV1,
    },
    /// The permanent key for this `(shard, incarnation)` is occupied by a
    /// foreign intent identity. The foreign claim is evidence only and must
    /// never be adopted as this attempt's durable state.
    IncarnationAlreadyClaimed {
        claim: OwnerAdmissionClaimV1,
    },
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(PrepareOwnerAdmissionNotDispatchedV1),
    OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1),
}

impl fmt::Debug for PrepareOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prepared { .. } => formatter.write_str("PrepareOwnerAdmissionResultV1::Prepared"),
            Self::ExpiredPrepared { .. } => {
                formatter.write_str("PrepareOwnerAdmissionResultV1::ExpiredPrepared")
            }
            Self::Rejected { .. } => formatter.write_str("PrepareOwnerAdmissionResultV1::Rejected"),
            Self::DurableConflict { .. } => {
                formatter.write_str("PrepareOwnerAdmissionResultV1::DurableConflict")
            }
            Self::IncarnationAlreadyClaimed { .. } => {
                formatter.write_str("PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("PrepareOwnerAdmissionResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("PrepareOwnerAdmissionResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("PrepareOwnerAdmissionResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Closed result of one commit attempt.
pub enum CommitOwnerAdmissionResultV1 {
    Committed {
        shard: LogicalShardRecord,
        lease: LogicalShardLease,
        claim: OwnerAdmissionClaimV1,
        lifetime: OwnerSessionLifetimeObservationV1,
    },
    AlreadyCommitted {
        shard: LogicalShardRecord,
        lease: LogicalShardLease,
        claim: OwnerAdmissionClaimV1,
        lifetime: OwnerSessionLifetimeObservationV1,
    },
    /// The exact claim is returned so callers can distinguish Aborted,
    /// Terminated, and a Committed claim whose installed record or session is
    /// inconsistent. An exact planned command can never adopt Rejected.
    DurableConflict {
        claim: OwnerAdmissionClaimV1,
    },
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(CommitOwnerAdmissionNotDispatchedV1),
    OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1),
}

impl fmt::Debug for CommitOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Committed { .. } => {
                formatter.write_str("CommitOwnerAdmissionResultV1::Committed")
            }
            Self::AlreadyCommitted { .. } => {
                formatter.write_str("CommitOwnerAdmissionResultV1::AlreadyCommitted")
            }
            Self::DurableConflict { .. } => {
                formatter.write_str("CommitOwnerAdmissionResultV1::DurableConflict")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("CommitOwnerAdmissionResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("CommitOwnerAdmissionResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("CommitOwnerAdmissionResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Closed result of one abort attempt.
pub enum AbortOwnerAdmissionResultV1 {
    Aborted {
        claim: OwnerAdmissionClaimV1,
    },
    /// The exact claim exposes the permanent conflicting phase without a
    /// backend-provided string or source.
    DurableConflict {
        claim: OwnerAdmissionClaimV1,
    },
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(AbortOwnerAdmissionNotDispatchedV1),
    OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1),
}

impl fmt::Debug for AbortOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aborted { .. } => formatter.write_str("AbortOwnerAdmissionResultV1::Aborted"),
            Self::DurableConflict { .. } => {
                formatter.write_str("AbortOwnerAdmissionResultV1::DurableConflict")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("AbortOwnerAdmissionResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("AbortOwnerAdmissionResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("AbortOwnerAdmissionResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Closed result of one exact Committed -> Terminated attempt.
pub enum TerminateOwnerAdmissionResultV1 {
    Terminated {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    AlreadyTerminated {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    /// The exact termination was durable and a later owner epoch has already
    /// replaced its terminal unowned shard state.
    Superseded {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    /// An exact plan-bound terminal phase prevents this termination reason.
    /// Foreign claims and live non-terminal phases must not be adopted here.
    DurableConflict {
        claim: OwnerAdmissionClaimV1,
    },
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(TerminateOwnerAdmissionNotDispatchedV1),
    OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1),
}

impl fmt::Debug for TerminateOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Terminated { .. } => {
                formatter.write_str("TerminateOwnerAdmissionResultV1::Terminated")
            }
            Self::AlreadyTerminated { .. } => {
                formatter.write_str("TerminateOwnerAdmissionResultV1::AlreadyTerminated")
            }
            Self::Superseded { .. } => {
                formatter.write_str("TerminateOwnerAdmissionResultV1::Superseded")
            }
            Self::DurableConflict { .. } => {
                formatter.write_str("TerminateOwnerAdmissionResultV1::DurableConflict")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("TerminateOwnerAdmissionResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("TerminateOwnerAdmissionResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("TerminateOwnerAdmissionResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Durable identity available when reconciling prepare ambiguity or an exact
/// planned/commit ambiguity.
#[derive(Clone, PartialEq, Eq)]
pub enum ReconcileOwnerAdmissionTargetV1 {
    IntentOnly(OwnerAdmissionIntentV1),
    ExactPlan(PlannedOwnerAdmissionV1),
    ExactServing(PlannedOwnerServingPublicationV1),
}

impl ReconcileOwnerAdmissionTargetV1 {
    #[cfg(test)]
    fn intent(&self) -> &OwnerAdmissionIntentV1 {
        match self {
            Self::IntentOnly(intent) => intent,
            Self::ExactPlan(plan) => plan.intent(),
            Self::ExactServing(publication) => publication.plan().intent(),
        }
    }

    fn outcome_unknown(&self) -> OwnerAdmissionOutcomeUnknownV1 {
        match self {
            Self::IntentOnly(intent) => OwnerAdmissionOutcomeUnknownV1::for_intent(intent),
            Self::ExactPlan(plan) => OwnerAdmissionOutcomeUnknownV1::for_plan(plan),
            Self::ExactServing(publication) => {
                OwnerAdmissionOutcomeUnknownV1::for_serving_publication(publication)
            }
        }
    }
}

impl fmt::Debug for ReconcileOwnerAdmissionTargetV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::IntentOnly(_) => "ReconcileOwnerAdmissionTargetV1::IntentOnly",
            Self::ExactPlan(_) => "ReconcileOwnerAdmissionTargetV1::ExactPlan",
            Self::ExactServing(_) => "ReconcileOwnerAdmissionTargetV1::ExactServing",
        })
    }
}

/// Closed result of reconciling one exact intent or plan.
pub enum ReconcileOwnerAdmissionResultV1 {
    /// The exact intent has a permanent Rejected claim. This covers both a
    /// rejection written by prepare and an absent-claim CAS used to seal
    /// prepare ambiguity; a read-only absence is never returned as terminal.
    Rejected {
        claim: OwnerAdmissionClaimV1,
    },
    Prepared {
        plan: Box<PlannedOwnerAdmissionV1>,
        claim: OwnerAdmissionClaimV1,
        sentinel: OwnerAdmissionPlanSentinelV1,
    },
    ExpiredPrepared {
        plan: Box<PlannedOwnerAdmissionV1>,
        claim: OwnerAdmissionClaimV1,
        expected_sentinel: OwnerAdmissionPlanSentinelV1,
    },
    Committed {
        plan: Box<PlannedOwnerAdmissionV1>,
        shard: LogicalShardRecord,
        lease: LogicalShardLease,
        claim: Box<OwnerAdmissionClaimV1>,
        lifetime: OwnerSessionLifetimeObservationV1,
    },
    /// The exact claim is Committed and the exact owner record remains
    /// installed, but the backend-authoritative session key is absent. The
    /// digest binds the backend's exact lease-expiry proof and is the only
    /// input from which a LeaseExpired termination permit can be derived.
    ExpiredCommitted {
        plan: Box<PlannedOwnerAdmissionV1>,
        shard: LogicalShardRecord,
        claim: Box<OwnerAdmissionClaimV1>,
        expected_session: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    /// Exact post-Prepared terminal or conflicting phase. Witness validation
    /// rejects a foreign plan or claim.
    DurableConflict {
        plan: Box<PlannedOwnerAdmissionV1>,
        claim: OwnerAdmissionClaimV1,
    },
    /// The same permanent incarnation key contains a foreign intent identity.
    IncarnationAlreadyClaimed {
        claim: OwnerAdmissionClaimV1,
    },
    /// A same-revision read proved a durable split that normal retry must not
    /// guess through.
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(ReconcileOwnerAdmissionNotDispatchedV1),
    OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1),
}

impl fmt::Debug for ReconcileOwnerAdmissionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::Rejected")
            }
            Self::Prepared { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::Prepared")
            }
            Self::ExpiredPrepared { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::ExpiredPrepared")
            }
            Self::Committed { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::Committed")
            }
            Self::ExpiredCommitted { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::ExpiredCommitted")
            }
            Self::DurableConflict { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::DurableConflict")
            }
            Self::IncarnationAlreadyClaimed { .. } => {
                formatter.write_str("ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("ReconcileOwnerAdmissionResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("ReconcileOwnerAdmissionResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("ReconcileOwnerAdmissionResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Exact publication identity retained after a possibly-dispatched response
/// is lost. Construction is available only from the claimed command payload.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerServingPublicationOutcomeUnknownV1 {
    logical_shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
    plan_digest: OwnerAdmissionPlanDigestV1,
    source_record_digest: OwnerAdmissionRecordDigestV1,
    target_record_digest: OwnerAdmissionRecordDigestV1,
    publication_digest: OwnerServingPublicationDigestV1,
}

impl OwnerServingPublicationOutcomeUnknownV1 {
    fn for_publication(publication: &PlannedOwnerServingPublicationV1) -> Self {
        Self {
            logical_shard_id: publication.plan().intent().logical_shard_id(),
            owner_incarnation_id: publication.plan().intent().owner_incarnation_id(),
            plan_digest: publication.plan().digest(),
            source_record_digest: publication.source_digest(),
            target_record_digest: publication.target_digest(),
            publication_digest: publication.digest(),
        }
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn owner_incarnation_id(&self) -> OwnerIncarnationId {
        self.owner_incarnation_id
    }

    pub const fn plan_digest(&self) -> OwnerAdmissionPlanDigestV1 {
        self.plan_digest
    }

    pub const fn source_record_digest(&self) -> OwnerAdmissionRecordDigestV1 {
        self.source_record_digest
    }

    pub const fn target_record_digest(&self) -> OwnerAdmissionRecordDigestV1 {
        self.target_record_digest
    }

    pub const fn publication_digest(&self) -> OwnerServingPublicationDigestV1 {
        self.publication_digest
    }
}

impl fmt::Debug for OwnerServingPublicationOutcomeUnknownV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerServingPublicationOutcomeUnknownV1(<redacted>)")
    }
}

/// Exact renewal identity retained after a possibly-dispatched response is
/// lost. Construction is available only from the claimed command payload.
#[derive(Clone, PartialEq, Eq)]
pub struct OwnerSessionRenewalOutcomeUnknownV1 {
    logical_shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
    plan_digest: OwnerAdmissionPlanDigestV1,
    claim_digest: OwnerAdmissionClaimDigestV1,
    session_binding_digest: OwnerSessionBindingDigestV1,
    renewal_target_digest: OwnerSessionRenewalTargetDigestV1,
}

impl OwnerSessionRenewalOutcomeUnknownV1 {
    fn for_target(target: &OwnerSessionRenewalTargetV1) -> Self {
        Self {
            logical_shard_id: target.plan().intent().logical_shard_id(),
            owner_incarnation_id: target.plan().intent().owner_incarnation_id(),
            plan_digest: target.plan().digest(),
            claim_digest: target.claim_digest(),
            session_binding_digest: target.session_binding_digest(),
            renewal_target_digest: target.digest(),
        }
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn owner_incarnation_id(&self) -> OwnerIncarnationId {
        self.owner_incarnation_id
    }

    pub const fn plan_digest(&self) -> OwnerAdmissionPlanDigestV1 {
        self.plan_digest
    }

    pub const fn claim_digest(&self) -> OwnerAdmissionClaimDigestV1 {
        self.claim_digest
    }

    pub const fn session_binding_digest(&self) -> OwnerSessionBindingDigestV1 {
        self.session_binding_digest
    }

    pub const fn renewal_target_digest(&self) -> OwnerSessionRenewalTargetDigestV1 {
        self.renewal_target_digest
    }
}

impl fmt::Debug for OwnerSessionRenewalOutcomeUnknownV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerSessionRenewalOutcomeUnknownV1(<redacted>)")
    }
}

/// Closed result of one exact Recovering-to-Serving publication attempt.
pub enum PublishOwnerServingResultV1 {
    Published {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        lifetime: OwnerSessionLifetimeObservationV1,
    },
    AlreadyPublished {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        lifetime: OwnerSessionLifetimeObservationV1,
    },
    PublicationConflict {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    ExpiredCommitted {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        expected_session: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    Terminated {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Superseded {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    DurableConflict {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(PublishOwnerServingNotDispatchedV1),
    OutcomeUnknown(OwnerServingPublicationOutcomeUnknownV1),
}

impl fmt::Debug for PublishOwnerServingResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Published { .. } => formatter.write_str("PublishOwnerServingResultV1::Published"),
            Self::AlreadyPublished { .. } => {
                formatter.write_str("PublishOwnerServingResultV1::AlreadyPublished")
            }
            Self::PublicationConflict { .. } => {
                formatter.write_str("PublishOwnerServingResultV1::PublicationConflict")
            }
            Self::ExpiredCommitted { .. } => {
                formatter.write_str("PublishOwnerServingResultV1::ExpiredCommitted")
            }
            Self::Terminated { .. } => {
                formatter.write_str("PublishOwnerServingResultV1::Terminated")
            }
            Self::Superseded { .. } => {
                formatter.write_str("PublishOwnerServingResultV1::Superseded")
            }
            Self::DurableConflict { .. } => {
                formatter.write_str("PublishOwnerServingResultV1::DurableConflict")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("PublishOwnerServingResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("PublishOwnerServingResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("PublishOwnerServingResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Closed result of one exact owner-session keepalive and TTL observation.
pub enum RenewOwnerSessionResultV1 {
    Current {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        lifetime: OwnerSessionLifetimeObservationV1,
    },
    ExpiredCommitted {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        expected_session: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    Terminated {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Superseded {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    DurableConflict {
        shard: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    DurableInconsistent(OwnerAdmissionInconsistencyCode),
    NotDispatched(RenewOwnerSessionNotDispatchedV1),
    OutcomeUnknown(OwnerSessionRenewalOutcomeUnknownV1),
}

impl fmt::Debug for RenewOwnerSessionResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Current { .. } => formatter.write_str("RenewOwnerSessionResultV1::Current"),
            Self::ExpiredCommitted { .. } => {
                formatter.write_str("RenewOwnerSessionResultV1::ExpiredCommitted")
            }
            Self::Terminated { .. } => formatter.write_str("RenewOwnerSessionResultV1::Terminated"),
            Self::Superseded { .. } => formatter.write_str("RenewOwnerSessionResultV1::Superseded"),
            Self::DurableConflict { .. } => {
                formatter.write_str("RenewOwnerSessionResultV1::DurableConflict")
            }
            Self::DurableInconsistent(code) => formatter
                .debug_tuple("RenewOwnerSessionResultV1::DurableInconsistent")
                .field(code)
                .finish(),
            Self::NotDispatched(code) => formatter
                .debug_tuple("RenewOwnerSessionResultV1::NotDispatched")
                .field(code)
                .finish(),
            Self::OutcomeUnknown(evidence) => formatter
                .debug_tuple("RenewOwnerSessionResultV1::OutcomeUnknown")
                .field(evidence)
                .finish(),
        }
    }
}

/// Coordinator-only proof that one exact reconciled owner session is
/// authoritatively absent.
///
/// Fields are private and the value is not cloneable. It can only be obtained
/// by consuming a verified `ExpiredCommitted` reconciliation.
#[cfg(test)]
pub(crate) struct ReconciledOwnerLeaseExpiryV1 {
    plan: PlannedOwnerAdmissionV1,
    evidence_digest: OwnerLeaseExpiryEvidenceDigest,
}

#[cfg(test)]
impl fmt::Debug for ReconciledOwnerLeaseExpiryV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReconciledOwnerLeaseExpiryV1(<redacted>)")
    }
}

/// Exact result accepted by the coordinator-held reconciliation witness.
///
/// Keeping this wrapper crate-private prevents a raw public result carrying an
/// arbitrary expiry digest from authorizing a LeaseExpired termination.
#[cfg(test)]
pub(crate) struct VerifiedReconcileOwnerAdmissionV1 {
    result: Box<ReconcileOwnerAdmissionResultV1>,
}

#[cfg(test)]
impl VerifiedReconcileOwnerAdmissionV1 {
    pub(crate) const fn result(&self) -> &ReconcileOwnerAdmissionResultV1 {
        &self.result
    }

    pub(crate) fn into_result(self) -> ReconcileOwnerAdmissionResultV1 {
        *self.result
    }

    pub(crate) fn into_lease_expiry(self) -> Result<ReconciledOwnerLeaseExpiryV1, Self> {
        match *self.result {
            ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                plan,
                evidence_digest,
                ..
            } => Ok(ReconciledOwnerLeaseExpiryV1 {
                plan: *plan,
                evidence_digest,
            }),
            result => Err(Self {
                result: Box::new(result),
            }),
        }
    }
}

#[cfg(test)]
impl fmt::Debug for VerifiedReconcileOwnerAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedReconcileOwnerAdmissionV1")
            .field("result", &self.result)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
fn claim_matches_intent_identity(
    claim: &OwnerAdmissionClaimV1,
    intent: &OwnerAdmissionIntentV1,
) -> bool {
    let identity = claim.identity();
    identity.logical_shard_id() == intent.logical_shard_id()
        && identity.owner_incarnation_id() == intent.owner_incarnation_id()
        && identity.intent_digest() == intent.digest()
        && identity.reservation_digest() == intent.reservation_digest()
        && identity.planned_epoch() == intent.planned_epoch()
}

#[cfg(test)]
fn claim_is_same_key_foreign_intent(
    claim: &OwnerAdmissionClaimV1,
    intent: &OwnerAdmissionIntentV1,
) -> bool {
    claim.identity().logical_shard_id() == intent.logical_shard_id()
        && claim.identity().owner_incarnation_id() == intent.owner_incarnation_id()
        && !claim_matches_intent_identity(claim, intent)
}

#[cfg(test)]
fn sentinel_matches_plan(
    sentinel: &OwnerAdmissionPlanSentinelV1,
    plan: &PlannedOwnerAdmissionV1,
) -> bool {
    *sentinel == OwnerAdmissionPlanSentinelV1::for_plan(plan)
}

#[cfg(test)]
fn claim_phase_matches_plan(claim: &OwnerAdmissionClaimV1, plan: &PlannedOwnerAdmissionV1) -> bool {
    match claim.phase() {
        OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Terminated {
            lease, plan_digest, ..
        }
        | OwnerAdmissionClaimPhaseV1::Aborted {
            lease, plan_digest, ..
        } => lease == plan.lease() && *plan_digest == plan.digest(),
        OwnerAdmissionClaimPhaseV1::Rejected { .. } => false,
    }
}

#[cfg(test)]
fn claim_matches_plan_identity(
    claim: &OwnerAdmissionClaimV1,
    plan: &PlannedOwnerAdmissionV1,
) -> bool {
    claim_matches_intent_identity(claim, plan.intent()) && claim_phase_matches_plan(claim, plan)
}

#[cfg(test)]
fn expected_prepared_claim(plan: &PlannedOwnerAdmissionV1) -> Option<OwnerAdmissionClaimV1> {
    OwnerAdmissionClaimV1::prepared(plan).ok()
}

#[cfg(test)]
fn expected_committed_claim(plan: &PlannedOwnerAdmissionV1) -> Option<OwnerAdmissionClaimV1> {
    expected_prepared_claim(plan)?.commit().ok()
}

#[cfg(test)]
fn expected_aborted_claim(
    plan: &PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionAbortReasonV1,
) -> Option<OwnerAdmissionClaimV1> {
    expected_prepared_claim(plan)?.abort(reason).ok()
}

#[cfg(test)]
fn expected_terminated_claim(
    plan: &PlannedOwnerAdmissionV1,
    reason: &OwnerAdmissionTerminationReasonV1,
) -> Option<OwnerAdmissionClaimV1> {
    expected_committed_claim(plan)?
        .terminate(reason.clone())
        .ok()
}

#[cfg(test)]
fn validate_prepared_result(
    intent: &OwnerAdmissionIntentV1,
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
    sentinel: &OwnerAdmissionPlanSentinelV1,
) -> bool {
    plan.intent() == intent
        && expected_prepared_claim(plan).is_some_and(|expected| expected == *claim)
        && sentinel_matches_plan(sentinel, plan)
}

#[cfg(test)]
fn validate_expired_prepared_result(
    intent: &OwnerAdmissionIntentV1,
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
    expected_sentinel: &OwnerAdmissionPlanSentinelV1,
) -> bool {
    plan.intent() == intent
        && expected_prepared_claim(plan).is_some_and(|expected| expected == *claim)
        && sentinel_matches_plan(expected_sentinel, plan)
}

#[cfg(test)]
fn validate_rejected_result(
    intent: &OwnerAdmissionIntentV1,
    claim: &OwnerAdmissionClaimV1,
) -> bool {
    let OwnerAdmissionClaimPhaseV1::Rejected { reason } = claim.phase() else {
        return false;
    };
    OwnerAdmissionClaimV1::rejected_from_absent(intent, *reason)
        .is_ok_and(|expected| expected == *claim)
}

#[cfg(test)]
fn validate_committed_result(
    plan: &PlannedOwnerAdmissionV1,
    shard: &LogicalShardRecord,
    lease: &LogicalShardLease,
    claim: &OwnerAdmissionClaimV1,
    lifetime: &OwnerSessionLifetimeObservationV1,
) -> bool {
    lease == plan.lease()
        && *shard == expected_recovering_record_for_plan(plan)
        && expected_committed_claim(plan).is_some_and(|expected| expected == *claim)
        && lifetime.validates_exact(plan, shard, lease)
}

#[cfg(test)]
fn validate_committed_descendant_result(
    plan: &PlannedOwnerAdmissionV1,
    shard: &LogicalShardRecord,
    lease: &LogicalShardLease,
    claim: &OwnerAdmissionClaimV1,
    lifetime: &OwnerSessionLifetimeObservationV1,
) -> bool {
    validate_committed_descendant_identity(plan, shard, lease, claim)
        && lifetime.validates_exact(plan, shard, lease)
}

#[cfg(test)]
fn validate_committed_descendant_identity(
    plan: &PlannedOwnerAdmissionV1,
    shard: &LogicalShardRecord,
    lease: &LogicalShardLease,
    claim: &OwnerAdmissionClaimV1,
) -> bool {
    if lease != plan.lease()
        || !expected_committed_claim(plan).is_some_and(|expected| expected == *claim)
        || validate_committed_record_descendant(plan, shard).is_err()
    {
        return false;
    }
    true
}

fn validate_live_record_for_target(
    target: &ReconcileOwnerAdmissionTargetV1,
    plan: &PlannedOwnerAdmissionV1,
    shard: &LogicalShardRecord,
) -> bool {
    if validate_committed_record_descendant(plan, shard).is_err() {
        return false;
    }
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(_)
        | ReconcileOwnerAdmissionTargetV1::ExactPlan(_) => true,
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            shard == publication.source() || shard == publication.target()
        }
    }
}

#[cfg(test)]
fn validate_terminated_result(
    plan: &PlannedOwnerAdmissionV1,
    reason: &OwnerAdmissionTerminationReasonV1,
    shard: &LogicalShardRecord,
    claim: &OwnerAdmissionClaimV1,
) -> bool {
    expected_terminated_claim(plan, reason).is_some_and(|expected| expected == *claim)
        && validate_terminated_record_descendant(plan, shard).is_ok()
}

#[cfg(test)]
fn validate_superseding_record(plan: &PlannedOwnerAdmissionV1, shard: &LogicalShardRecord) -> bool {
    validate_logical_shard_record(shard).is_ok()
        && shard.logical_shard_id == plan.intent().logical_shard_id()
        && shard
            .owner_epoch
            .is_some_and(|epoch| epoch > plan.intent().planned_epoch())
}

fn validate_plan_for_target(
    target: &ReconcileOwnerAdmissionTargetV1,
    plan: &PlannedOwnerAdmissionV1,
) -> bool {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => plan.intent() == intent,
        ReconcileOwnerAdmissionTargetV1::ExactPlan(expected) => plan == expected,
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => plan == publication.plan(),
    }
}

#[cfg(test)]
fn validate_prepare_outcome(
    intent: &OwnerAdmissionIntentV1,
    result: &PrepareOwnerAdmissionResultV1,
    _command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    match result {
        PrepareOwnerAdmissionResultV1::Prepared {
            plan,
            claim,
            sentinel,
        } => validate_prepared_result(intent, plan, claim, sentinel),
        PrepareOwnerAdmissionResultV1::ExpiredPrepared {
            plan,
            claim,
            expected_sentinel,
        } => validate_expired_prepared_result(intent, plan, claim, expected_sentinel),
        PrepareOwnerAdmissionResultV1::Rejected { claim } => {
            validate_rejected_result(intent, claim)
        }
        PrepareOwnerAdmissionResultV1::DurableConflict { plan, claim } => {
            plan.intent() == intent
                && claim_matches_plan_identity(claim, plan)
                && matches!(
                    claim.phase(),
                    OwnerAdmissionClaimPhaseV1::Committed { .. }
                        | OwnerAdmissionClaimPhaseV1::Terminated { .. }
                        | OwnerAdmissionClaimPhaseV1::Aborted { .. }
                )
        }
        PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim } => {
            claim_is_same_key_foreign_intent(claim, intent)
        }
        PrepareOwnerAdmissionResultV1::DurableInconsistent(_) => true,
        PrepareOwnerAdmissionResultV1::NotDispatched(_) => true,
        PrepareOwnerAdmissionResultV1::OutcomeUnknown(evidence) => {
            *evidence == OwnerAdmissionOutcomeUnknownV1::for_intent(intent)
        }
    }
}

#[cfg(test)]
fn validate_commit_outcome(
    plan: &PlannedOwnerAdmissionV1,
    result: &CommitOwnerAdmissionResultV1,
    command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    match result {
        CommitOwnerAdmissionResultV1::Committed {
            shard,
            lease,
            claim,
            lifetime,
        } => {
            lifetime.validates_command_origin(command_origin)
                && validate_committed_result(plan, shard, lease, claim, lifetime)
        }
        CommitOwnerAdmissionResultV1::AlreadyCommitted {
            shard,
            lease,
            claim,
            lifetime,
        } => {
            lifetime.validates_command_origin(command_origin)
                && validate_committed_descendant_result(plan, shard, lease, claim, lifetime)
        }
        CommitOwnerAdmissionResultV1::DurableConflict { claim } => {
            claim_matches_plan_identity(claim, plan)
                && matches!(
                    claim.phase(),
                    OwnerAdmissionClaimPhaseV1::Committed { .. }
                        | OwnerAdmissionClaimPhaseV1::Terminated { .. }
                        | OwnerAdmissionClaimPhaseV1::Aborted { .. }
                )
        }
        CommitOwnerAdmissionResultV1::DurableInconsistent(_) => true,
        CommitOwnerAdmissionResultV1::NotDispatched(_) => true,
        CommitOwnerAdmissionResultV1::OutcomeUnknown(evidence) => {
            *evidence == OwnerAdmissionOutcomeUnknownV1::for_plan(plan)
        }
    }
}

#[derive(Clone)]
struct AbortOwnerAdmissionPayloadV1 {
    plan: PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionAbortReasonV1,
}

#[derive(Clone)]
struct TerminateOwnerAdmissionPayloadV1 {
    plan: PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionTerminationReasonV1,
}

#[cfg(test)]
pub(crate) struct PrepareOwnerAdmissionWitnessFailureV1 {
    code: OwnerAdmissionCommandWitnessError,
    recovery_intent: Box<OwnerAdmissionIntentV1>,
}

#[cfg(test)]
impl PrepareOwnerAdmissionWitnessFailureV1 {
    pub(crate) const fn code(&self) -> OwnerAdmissionCommandWitnessError {
        self.code
    }

    #[cfg(test)]
    pub(crate) const fn recovery_intent(&self) -> &OwnerAdmissionIntentV1 {
        &self.recovery_intent
    }

    pub(crate) fn into_recovery_intent(self) -> OwnerAdmissionIntentV1 {
        *self.recovery_intent
    }
}

#[cfg(test)]
impl fmt::Debug for PrepareOwnerAdmissionWitnessFailureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_witness_failure_debug(
            formatter,
            "PrepareOwnerAdmissionWitnessFailureV1",
            self.code,
            "intent",
        )
    }
}

#[cfg(test)]
pub(crate) struct CommitOwnerAdmissionWitnessFailureV1 {
    code: OwnerAdmissionCommandWitnessError,
    recovery_plan: Box<PlannedOwnerAdmissionV1>,
}

#[cfg(test)]
impl CommitOwnerAdmissionWitnessFailureV1 {
    pub(crate) const fn code(&self) -> OwnerAdmissionCommandWitnessError {
        self.code
    }

    #[cfg(test)]
    pub(crate) const fn recovery_plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.recovery_plan
    }

    pub(crate) fn into_recovery_plan(self) -> PlannedOwnerAdmissionV1 {
        *self.recovery_plan
    }
}

#[cfg(test)]
impl fmt::Debug for CommitOwnerAdmissionWitnessFailureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_witness_failure_debug(
            formatter,
            "CommitOwnerAdmissionWitnessFailureV1",
            self.code,
            "plan",
        )
    }
}

#[cfg(test)]
pub(crate) struct AbortOwnerAdmissionWitnessFailureV1 {
    code: OwnerAdmissionCommandWitnessError,
    recovery: Box<AbortOwnerAdmissionPayloadV1>,
}

#[cfg(test)]
impl AbortOwnerAdmissionWitnessFailureV1 {
    pub(crate) const fn code(&self) -> OwnerAdmissionCommandWitnessError {
        self.code
    }

    #[cfg(test)]
    pub(crate) const fn recovery_plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.recovery.plan
    }

    #[cfg(test)]
    pub(crate) const fn recovery_reason(&self) -> OwnerAdmissionAbortReasonV1 {
        self.recovery.reason
    }

    pub(crate) fn into_recovery(self) -> (PlannedOwnerAdmissionV1, OwnerAdmissionAbortReasonV1) {
        (self.recovery.plan, self.recovery.reason)
    }
}

#[cfg(test)]
impl fmt::Debug for AbortOwnerAdmissionWitnessFailureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_witness_failure_debug(
            formatter,
            "AbortOwnerAdmissionWitnessFailureV1",
            self.code,
            "plan-and-reason",
        )
    }
}

#[cfg(test)]
pub(crate) struct TerminateOwnerAdmissionWitnessFailureV1 {
    code: OwnerAdmissionCommandWitnessError,
    recovery: Box<TerminateOwnerAdmissionPayloadV1>,
}

#[cfg(test)]
impl TerminateOwnerAdmissionWitnessFailureV1 {
    pub(crate) const fn code(&self) -> OwnerAdmissionCommandWitnessError {
        self.code
    }

    #[cfg(test)]
    pub(crate) const fn recovery_plan(&self) -> &PlannedOwnerAdmissionV1 {
        &self.recovery.plan
    }

    #[cfg(test)]
    pub(crate) const fn recovery_reason(&self) -> &OwnerAdmissionTerminationReasonV1 {
        &self.recovery.reason
    }

    pub(crate) fn into_recovery(
        self,
    ) -> (PlannedOwnerAdmissionV1, OwnerAdmissionTerminationReasonV1) {
        (self.recovery.plan, self.recovery.reason)
    }
}

#[cfg(test)]
impl fmt::Debug for TerminateOwnerAdmissionWitnessFailureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_witness_failure_debug(
            formatter,
            "TerminateOwnerAdmissionWitnessFailureV1",
            self.code,
            "plan-and-reason",
        )
    }
}

#[cfg(test)]
pub(crate) struct ReconcileOwnerAdmissionWitnessFailureV1 {
    code: OwnerAdmissionCommandWitnessError,
    recovery_target: Box<ReconcileOwnerAdmissionTargetV1>,
}

#[cfg(test)]
impl ReconcileOwnerAdmissionWitnessFailureV1 {
    pub(crate) const fn code(&self) -> OwnerAdmissionCommandWitnessError {
        self.code
    }

    #[cfg(test)]
    pub(crate) const fn recovery_target(&self) -> &ReconcileOwnerAdmissionTargetV1 {
        &self.recovery_target
    }

    pub(crate) fn into_recovery_target(self) -> ReconcileOwnerAdmissionTargetV1 {
        *self.recovery_target
    }
}

#[cfg(test)]
impl fmt::Debug for ReconcileOwnerAdmissionWitnessFailureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let target = match self.recovery_target.as_ref() {
            ReconcileOwnerAdmissionTargetV1::IntentOnly(_) => "intent",
            ReconcileOwnerAdmissionTargetV1::ExactPlan(_) => "plan",
            ReconcileOwnerAdmissionTargetV1::ExactServing(_) => "serving-publication",
        };
        redacted_witness_failure_debug(
            formatter,
            "ReconcileOwnerAdmissionWitnessFailureV1",
            self.code,
            target,
        )
    }
}

#[cfg(test)]
fn validate_abort_outcome(
    payload: &AbortOwnerAdmissionPayloadV1,
    result: &AbortOwnerAdmissionResultV1,
    _command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    match result {
        AbortOwnerAdmissionResultV1::Aborted { claim } => {
            expected_aborted_claim(&payload.plan, payload.reason)
                .is_some_and(|expected| expected == *claim)
        }
        AbortOwnerAdmissionResultV1::DurableConflict { claim } => {
            claim_matches_plan_identity(claim, &payload.plan)
                && matches!(
                    claim.phase(),
                    OwnerAdmissionClaimPhaseV1::Committed { .. }
                        | OwnerAdmissionClaimPhaseV1::Terminated { .. }
                        | OwnerAdmissionClaimPhaseV1::Aborted { .. }
                )
        }
        AbortOwnerAdmissionResultV1::DurableInconsistent(_) => true,
        AbortOwnerAdmissionResultV1::NotDispatched(_) => true,
        AbortOwnerAdmissionResultV1::OutcomeUnknown(evidence) => {
            *evidence == OwnerAdmissionOutcomeUnknownV1::for_plan(&payload.plan)
        }
    }
}

#[cfg(test)]
fn validate_terminate_outcome(
    payload: &TerminateOwnerAdmissionPayloadV1,
    result: &TerminateOwnerAdmissionResultV1,
    _command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    match result {
        TerminateOwnerAdmissionResultV1::Terminated { shard, claim }
        | TerminateOwnerAdmissionResultV1::AlreadyTerminated { shard, claim } => {
            validate_terminated_result(&payload.plan, &payload.reason, shard, claim)
        }
        TerminateOwnerAdmissionResultV1::Superseded { shard, claim } => {
            expected_terminated_claim(&payload.plan, &payload.reason)
                .is_some_and(|expected| expected == *claim)
                && validate_superseding_record(&payload.plan, shard)
        }
        TerminateOwnerAdmissionResultV1::DurableConflict { claim } => {
            if !claim_matches_plan_identity(claim, &payload.plan) {
                return false;
            }
            match claim.phase() {
                OwnerAdmissionClaimPhaseV1::Aborted { .. } => true,
                OwnerAdmissionClaimPhaseV1::Terminated { reason, .. } => reason != &payload.reason,
                OwnerAdmissionClaimPhaseV1::Prepared { .. }
                | OwnerAdmissionClaimPhaseV1::Committed { .. }
                | OwnerAdmissionClaimPhaseV1::Rejected { .. } => false,
            }
        }
        TerminateOwnerAdmissionResultV1::DurableInconsistent(_) => true,
        TerminateOwnerAdmissionResultV1::NotDispatched(_) => true,
        TerminateOwnerAdmissionResultV1::OutcomeUnknown(evidence) => {
            *evidence == OwnerAdmissionOutcomeUnknownV1::for_plan(&payload.plan)
        }
    }
}

#[cfg(test)]
fn validate_reconcile_outcome(
    target: &ReconcileOwnerAdmissionTargetV1,
    result: &ReconcileOwnerAdmissionResultV1,
    command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    match result {
        ReconcileOwnerAdmissionResultV1::Rejected { claim } => {
            matches!(target, ReconcileOwnerAdmissionTargetV1::IntentOnly(_))
                && validate_rejected_result(target.intent(), claim)
        }
        ReconcileOwnerAdmissionResultV1::Prepared {
            plan,
            claim,
            sentinel,
        } => {
            validate_plan_for_target(target, plan)
                && validate_prepared_result(target.intent(), plan, claim, sentinel)
        }
        ReconcileOwnerAdmissionResultV1::ExpiredPrepared {
            plan,
            claim,
            expected_sentinel,
        } => {
            validate_plan_for_target(target, plan)
                && validate_expired_prepared_result(target.intent(), plan, claim, expected_sentinel)
        }
        ReconcileOwnerAdmissionResultV1::Committed {
            plan,
            shard,
            lease,
            claim,
            lifetime,
        } => {
            validate_plan_for_target(target, plan)
                && validate_live_record_for_target(target, plan, shard)
                && lifetime.validates_command_origin(command_origin)
                && validate_committed_descendant_result(plan, shard, lease, claim, lifetime)
        }
        ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
            plan,
            shard,
            claim,
            expected_session,
            evidence_digest: _,
        } => {
            validate_plan_for_target(target, plan)
                && expected_session == plan.lease()
                && validate_live_record_for_target(target, plan, shard)
                && validate_committed_descendant_identity(plan, shard, expected_session, claim)
        }
        ReconcileOwnerAdmissionResultV1::DurableConflict { plan, claim } => match claim.phase() {
            OwnerAdmissionClaimPhaseV1::Rejected { .. } => false,
            OwnerAdmissionClaimPhaseV1::Prepared { .. } => false,
            OwnerAdmissionClaimPhaseV1::Committed { .. }
            | OwnerAdmissionClaimPhaseV1::Terminated { .. }
            | OwnerAdmissionClaimPhaseV1::Aborted { .. } => {
                validate_plan_for_target(target, plan) && claim_matches_plan_identity(claim, plan)
            }
        },
        ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim } => {
            claim_is_same_key_foreign_intent(claim, target.intent())
        }
        ReconcileOwnerAdmissionResultV1::DurableInconsistent(_) => true,
        ReconcileOwnerAdmissionResultV1::NotDispatched(_) => true,
        ReconcileOwnerAdmissionResultV1::OutcomeUnknown(evidence) => {
            *evidence == target.outcome_unknown()
        }
    }
}

#[cfg(test)]
fn validate_plan_bound_terminated_claim(
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
) -> bool {
    claim_matches_plan_identity(claim, plan)
        && matches!(claim.phase(), OwnerAdmissionClaimPhaseV1::Terminated { .. })
}

#[cfg(test)]
fn validate_same_shard_record(plan: &PlannedOwnerAdmissionV1, shard: &LogicalShardRecord) -> bool {
    validate_logical_shard_record(shard).is_ok()
        && shard.logical_shard_id == plan.intent().logical_shard_id()
}

#[cfg(test)]
fn validate_publication_live_record(
    publication: &PlannedOwnerServingPublicationV1,
    shard: &LogicalShardRecord,
) -> bool {
    shard == publication.source() || shard == publication.target()
}

#[cfg(test)]
fn validate_publish_owner_serving_outcome(
    publication: &PlannedOwnerServingPublicationV1,
    result: &PublishOwnerServingResultV1,
    command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    let plan = publication.plan();
    match result {
        PublishOwnerServingResultV1::Published {
            shard,
            claim,
            lifetime,
        }
        | PublishOwnerServingResultV1::AlreadyPublished {
            shard,
            claim,
            lifetime,
        } => {
            shard == publication.target()
                && expected_committed_claim(plan).is_some_and(|expected| expected == *claim)
                && lifetime.validates_command_origin(command_origin)
                && lifetime.validates_exact(plan, shard, plan.lease())
        }
        PublishOwnerServingResultV1::PublicationConflict { shard, claim } => {
            expected_committed_claim(plan).is_some_and(|expected| expected == *claim)
                && validate_committed_record_descendant(plan, shard).is_ok()
                && shard.state == LogicalShardState::Serving
                && shard != publication.target()
        }
        PublishOwnerServingResultV1::ExpiredCommitted {
            shard,
            claim,
            expected_session,
            evidence_digest: _,
        } => {
            expected_session == plan.lease()
                && expected_committed_claim(plan).is_some_and(|expected| expected == *claim)
                && validate_publication_live_record(publication, shard)
        }
        PublishOwnerServingResultV1::Terminated { shard, claim } => {
            validate_plan_bound_terminated_claim(plan, claim)
                && validate_terminated_record_descendant(plan, shard).is_ok()
        }
        PublishOwnerServingResultV1::Superseded { shard, claim } => {
            validate_plan_bound_terminated_claim(plan, claim)
                && validate_superseding_record(plan, shard)
        }
        PublishOwnerServingResultV1::DurableConflict { shard, claim } => {
            validate_same_shard_record(plan, shard)
                && claim_matches_plan_identity(claim, plan)
                && matches!(claim.phase(), OwnerAdmissionClaimPhaseV1::Aborted { .. })
        }
        PublishOwnerServingResultV1::DurableInconsistent(_) => true,
        PublishOwnerServingResultV1::NotDispatched(_) => true,
        PublishOwnerServingResultV1::OutcomeUnknown(evidence) => {
            *evidence == OwnerServingPublicationOutcomeUnknownV1::for_publication(publication)
        }
    }
}

#[cfg(test)]
fn validate_renew_owner_session_outcome(
    target: &OwnerSessionRenewalTargetV1,
    result: &RenewOwnerSessionResultV1,
    command_origin: &Arc<OwnerAdmissionCommandCore>,
) -> bool {
    let plan = target.plan();
    match result {
        RenewOwnerSessionResultV1::Current {
            shard,
            claim,
            lifetime,
        } => {
            claim == target.claim()
                && validate_committed_record_descendant(plan, shard).is_ok()
                && lifetime.validates_command_origin(command_origin)
                && lifetime.validates_exact(plan, shard, target.session())
        }
        RenewOwnerSessionResultV1::ExpiredCommitted {
            shard,
            claim,
            expected_session,
            evidence_digest: _,
        } => {
            claim == target.claim()
                && expected_session == target.session()
                && validate_committed_record_descendant(plan, shard).is_ok()
        }
        RenewOwnerSessionResultV1::Terminated { shard, claim } => {
            validate_plan_bound_terminated_claim(plan, claim)
                && validate_terminated_record_descendant(plan, shard).is_ok()
        }
        RenewOwnerSessionResultV1::Superseded { shard, claim } => {
            validate_plan_bound_terminated_claim(plan, claim)
                && validate_superseding_record(plan, shard)
        }
        RenewOwnerSessionResultV1::DurableConflict { shard, claim } => {
            validate_same_shard_record(plan, shard)
                && claim_matches_plan_identity(claim, plan)
                && matches!(claim.phase(), OwnerAdmissionClaimPhaseV1::Aborted { .. })
        }
        RenewOwnerSessionResultV1::DurableInconsistent(_) => true,
        RenewOwnerSessionResultV1::NotDispatched(_) => true,
        RenewOwnerSessionResultV1::OutcomeUnknown(evidence) => {
            *evidence == OwnerSessionRenewalOutcomeUnknownV1::for_target(target)
        }
    }
}

/// One coordinator-minted prepare command.
///
/// The command is neither constructible nor cloneable by a backend:
///
/// ```compile_fail
/// use nokv_control::{OwnerAdmissionIntentV1, PrepareOwnerAdmissionCommandV1};
/// let intent: OwnerAdmissionIntentV1 = todo!();
/// let _forged = PrepareOwnerAdmissionCommandV1 { intent };
/// ```
///
/// ```compile_fail
/// use nokv_control::PrepareOwnerAdmissionCommandV1;
/// fn duplicate(command: PrepareOwnerAdmissionCommandV1) {
///     let _copy = command.clone();
/// }
/// ```
///
/// ```compile_fail
/// use nokv_control::PrepareOwnerAdmissionCommandV1;
/// fn forge_default() -> PrepareOwnerAdmissionCommandV1 {
///     Default::default()
/// }
/// ```
///
/// ```compile_fail
/// use nokv_control::PrepareOwnerAdmissionCommandV1;
/// fn serialize(command: &PrepareOwnerAdmissionCommandV1) {
///     let _bytes = serde_json::to_vec(command).unwrap();
/// }
/// ```
pub struct PrepareOwnerAdmissionCommandV1(CommandEnvelope<OwnerAdmissionIntentV1>);

impl PrepareOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> &OwnerAdmissionIntentV1 {
        &self.0.payload
    }

    pub fn claim_execution(self) -> ClaimedPrepareOwnerAdmissionCommandV1 {
        ClaimedPrepareOwnerAdmissionCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for PrepareOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "PrepareOwnerAdmissionCommandV1", &self.0.core)
    }
}

pub struct ClaimedPrepareOwnerAdmissionCommandV1(ClaimedCommandEnvelope<OwnerAdmissionIntentV1>);

impl ClaimedPrepareOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> &OwnerAdmissionIntentV1 {
        &self.0.payload
    }

    pub fn outcome_unknown(&self) -> PrepareOwnerAdmissionResultV1 {
        PrepareOwnerAdmissionResultV1::OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1::for_intent(
            &self.0.payload,
        ))
    }

    pub fn complete(self, result: PrepareOwnerAdmissionResultV1) -> PrepareOwnerAdmissionOutcomeV1 {
        PrepareOwnerAdmissionOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedPrepareOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(
            formatter,
            "ClaimedPrepareOwnerAdmissionCommandV1",
            &self.0.core,
        )
    }
}

pub struct PrepareOwnerAdmissionOutcomeV1(CommandOutcomeEnvelope<PrepareOwnerAdmissionResultV1>);

impl fmt::Debug for PrepareOwnerAdmissionOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "PrepareOwnerAdmissionOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

#[cfg(test)]
pub(crate) struct PrepareOwnerAdmissionWitnessV1(CommandWitnessEnvelope<OwnerAdmissionIntentV1>);

#[cfg(test)]
impl PrepareOwnerAdmissionWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: PrepareOwnerAdmissionOutcomeV1,
    ) -> Result<PrepareOwnerAdmissionResultV1, PrepareOwnerAdmissionWitnessFailureV1> {
        resolve_command(self.0, outcome.0, validate_prepare_outcome).map_err(|failure| {
            PrepareOwnerAdmissionWitnessFailureV1 {
                code: failure.code,
                recovery_intent: Box::new(failure.recovery_target),
            }
        })
    }
}

pub struct CommitOwnerAdmissionCommandV1(CommandEnvelope<PlannedOwnerAdmissionV1>);

impl CommitOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> &PlannedOwnerAdmissionV1 {
        &self.0.payload
    }

    pub fn claim_execution(self) -> ClaimedCommitOwnerAdmissionCommandV1 {
        ClaimedCommitOwnerAdmissionCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for CommitOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "CommitOwnerAdmissionCommandV1", &self.0.core)
    }
}

pub struct ClaimedCommitOwnerAdmissionCommandV1(ClaimedCommandEnvelope<PlannedOwnerAdmissionV1>);

impl ClaimedCommitOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> &PlannedOwnerAdmissionV1 {
        &self.0.payload
    }

    pub fn outcome_unknown(&self) -> CommitOwnerAdmissionResultV1 {
        CommitOwnerAdmissionResultV1::OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1::for_plan(
            &self.0.payload,
        ))
    }

    pub fn non_expiring_lifetime_observation(
        &self,
        returned_record: &LogicalShardRecord,
        returned_session: &LogicalShardLease,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        OwnerSessionLifetimeObservationV1::non_expiring_for_committed_descendant(
            &self.0.payload,
            returned_record,
            returned_session,
        )
    }

    pub fn finite_lifetime_observation(
        &self,
        returned_record: &LogicalShardRecord,
        returned_session: &LogicalShardLease,
        observed_ttl_seconds: NonZeroU64,
        proof_digest: OwnerSessionLifetimeProofDigestV1,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        OwnerSessionLifetimeObservationV1::finite_for_committed_descendant(
            Arc::clone(&self.0.core),
            &self.0.payload,
            returned_record,
            returned_session,
            observed_ttl_seconds,
            proof_digest,
        )
    }

    pub fn complete(self, result: CommitOwnerAdmissionResultV1) -> CommitOwnerAdmissionOutcomeV1 {
        CommitOwnerAdmissionOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedCommitOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(
            formatter,
            "ClaimedCommitOwnerAdmissionCommandV1",
            &self.0.core,
        )
    }
}

pub struct CommitOwnerAdmissionOutcomeV1(CommandOutcomeEnvelope<CommitOwnerAdmissionResultV1>);

impl fmt::Debug for CommitOwnerAdmissionOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "CommitOwnerAdmissionOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

#[cfg(test)]
pub(crate) struct CommitOwnerAdmissionWitnessV1(CommandWitnessEnvelope<PlannedOwnerAdmissionV1>);

#[cfg(test)]
impl CommitOwnerAdmissionWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: CommitOwnerAdmissionOutcomeV1,
    ) -> Result<CommitOwnerAdmissionResultV1, CommitOwnerAdmissionWitnessFailureV1> {
        resolve_command(self.0, outcome.0, validate_commit_outcome).map_err(|failure| {
            CommitOwnerAdmissionWitnessFailureV1 {
                code: failure.code,
                recovery_plan: Box::new(failure.recovery_target),
            }
        })
    }
}

/// Read-only storage-neutral input of one abort command.
#[derive(Clone, Copy)]
pub struct AbortOwnerAdmissionInspectionV1<'a> {
    pub plan: &'a PlannedOwnerAdmissionV1,
    pub reason: OwnerAdmissionAbortReasonV1,
}

impl fmt::Debug for AbortOwnerAdmissionInspectionV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AbortOwnerAdmissionInspectionV1")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

pub struct AbortOwnerAdmissionCommandV1(CommandEnvelope<AbortOwnerAdmissionPayloadV1>);

impl AbortOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> AbortOwnerAdmissionInspectionV1<'_> {
        AbortOwnerAdmissionInspectionV1 {
            plan: &self.0.payload.plan,
            reason: self.0.payload.reason,
        }
    }

    pub fn claim_execution(self) -> ClaimedAbortOwnerAdmissionCommandV1 {
        ClaimedAbortOwnerAdmissionCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for AbortOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "AbortOwnerAdmissionCommandV1", &self.0.core)
    }
}

pub struct ClaimedAbortOwnerAdmissionCommandV1(
    ClaimedCommandEnvelope<AbortOwnerAdmissionPayloadV1>,
);

impl ClaimedAbortOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> AbortOwnerAdmissionInspectionV1<'_> {
        AbortOwnerAdmissionInspectionV1 {
            plan: &self.0.payload.plan,
            reason: self.0.payload.reason,
        }
    }

    pub fn outcome_unknown(&self) -> AbortOwnerAdmissionResultV1 {
        AbortOwnerAdmissionResultV1::OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1::for_plan(
            &self.0.payload.plan,
        ))
    }

    pub fn complete(self, result: AbortOwnerAdmissionResultV1) -> AbortOwnerAdmissionOutcomeV1 {
        AbortOwnerAdmissionOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedAbortOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(
            formatter,
            "ClaimedAbortOwnerAdmissionCommandV1",
            &self.0.core,
        )
    }
}

pub struct AbortOwnerAdmissionOutcomeV1(CommandOutcomeEnvelope<AbortOwnerAdmissionResultV1>);

impl fmt::Debug for AbortOwnerAdmissionOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "AbortOwnerAdmissionOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

#[cfg(test)]
pub(crate) struct AbortOwnerAdmissionWitnessV1(
    CommandWitnessEnvelope<AbortOwnerAdmissionPayloadV1>,
);

#[cfg(test)]
impl AbortOwnerAdmissionWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: AbortOwnerAdmissionOutcomeV1,
    ) -> Result<AbortOwnerAdmissionResultV1, AbortOwnerAdmissionWitnessFailureV1> {
        resolve_command(self.0, outcome.0, validate_abort_outcome).map_err(|failure| {
            AbortOwnerAdmissionWitnessFailureV1 {
                code: failure.code,
                recovery: Box::new(failure.recovery_target),
            }
        })
    }
}

/// Read-only storage-neutral input of one terminate command.
#[derive(Clone, Copy)]
pub struct TerminateOwnerAdmissionInspectionV1<'a> {
    pub plan: &'a PlannedOwnerAdmissionV1,
    pub reason: &'a OwnerAdmissionTerminationReasonV1,
}

impl fmt::Debug for TerminateOwnerAdmissionInspectionV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminateOwnerAdmissionInspectionV1")
            .field("reason", self.reason)
            .finish_non_exhaustive()
    }
}

pub struct TerminateOwnerAdmissionCommandV1(CommandEnvelope<TerminateOwnerAdmissionPayloadV1>);

impl TerminateOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> TerminateOwnerAdmissionInspectionV1<'_> {
        TerminateOwnerAdmissionInspectionV1 {
            plan: &self.0.payload.plan,
            reason: &self.0.payload.reason,
        }
    }

    pub fn claim_execution(self) -> ClaimedTerminateOwnerAdmissionCommandV1 {
        ClaimedTerminateOwnerAdmissionCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for TerminateOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "TerminateOwnerAdmissionCommandV1", &self.0.core)
    }
}

pub struct ClaimedTerminateOwnerAdmissionCommandV1(
    ClaimedCommandEnvelope<TerminateOwnerAdmissionPayloadV1>,
);

impl ClaimedTerminateOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> TerminateOwnerAdmissionInspectionV1<'_> {
        TerminateOwnerAdmissionInspectionV1 {
            plan: &self.0.payload.plan,
            reason: &self.0.payload.reason,
        }
    }

    pub fn outcome_unknown(&self) -> TerminateOwnerAdmissionResultV1 {
        TerminateOwnerAdmissionResultV1::OutcomeUnknown(OwnerAdmissionOutcomeUnknownV1::for_plan(
            &self.0.payload.plan,
        ))
    }

    pub fn complete(
        self,
        result: TerminateOwnerAdmissionResultV1,
    ) -> TerminateOwnerAdmissionOutcomeV1 {
        TerminateOwnerAdmissionOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedTerminateOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(
            formatter,
            "ClaimedTerminateOwnerAdmissionCommandV1",
            &self.0.core,
        )
    }
}

pub struct TerminateOwnerAdmissionOutcomeV1(
    CommandOutcomeEnvelope<TerminateOwnerAdmissionResultV1>,
);

impl fmt::Debug for TerminateOwnerAdmissionOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "TerminateOwnerAdmissionOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

#[cfg(test)]
pub(crate) struct TerminateOwnerAdmissionWitnessV1(
    CommandWitnessEnvelope<TerminateOwnerAdmissionPayloadV1>,
);

#[cfg(test)]
impl TerminateOwnerAdmissionWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: TerminateOwnerAdmissionOutcomeV1,
    ) -> Result<TerminateOwnerAdmissionResultV1, TerminateOwnerAdmissionWitnessFailureV1> {
        resolve_command(self.0, outcome.0, validate_terminate_outcome).map_err(|failure| {
            TerminateOwnerAdmissionWitnessFailureV1 {
                code: failure.code,
                recovery: Box::new(failure.recovery_target),
            }
        })
    }
}

pub struct ReconcileOwnerAdmissionCommandV1(CommandEnvelope<ReconcileOwnerAdmissionTargetV1>);

impl ReconcileOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> &ReconcileOwnerAdmissionTargetV1 {
        &self.0.payload
    }

    pub fn claim_execution(self) -> ClaimedReconcileOwnerAdmissionCommandV1 {
        ClaimedReconcileOwnerAdmissionCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for ReconcileOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "ReconcileOwnerAdmissionCommandV1", &self.0.core)
    }
}

pub struct ClaimedReconcileOwnerAdmissionCommandV1(
    ClaimedCommandEnvelope<ReconcileOwnerAdmissionTargetV1>,
);

impl ClaimedReconcileOwnerAdmissionCommandV1 {
    pub fn inspect(&self) -> &ReconcileOwnerAdmissionTargetV1 {
        &self.0.payload
    }

    pub fn outcome_unknown(&self) -> ReconcileOwnerAdmissionResultV1 {
        ReconcileOwnerAdmissionResultV1::OutcomeUnknown(self.0.payload.outcome_unknown())
    }

    pub fn non_expiring_lifetime_observation(
        &self,
        returned_plan: &PlannedOwnerAdmissionV1,
        returned_record: &LogicalShardRecord,
        returned_session: &LogicalShardLease,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        self.validate_live_observation_bindings(returned_plan, returned_record, returned_session)?;
        Ok(OwnerSessionLifetimeObservationV1::NonExpiring)
    }

    pub fn finite_lifetime_observation(
        &self,
        returned_plan: &PlannedOwnerAdmissionV1,
        returned_record: &LogicalShardRecord,
        returned_session: &LogicalShardLease,
        observed_ttl_seconds: NonZeroU64,
        proof_digest: OwnerSessionLifetimeProofDigestV1,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        self.validate_live_observation_bindings(returned_plan, returned_record, returned_session)?;
        OwnerSessionLifetimeObservationV1::finite_for_committed_descendant(
            Arc::clone(&self.0.core),
            returned_plan,
            returned_record,
            returned_session,
            observed_ttl_seconds,
            proof_digest,
        )
    }

    fn validate_live_observation_bindings(
        &self,
        returned_plan: &PlannedOwnerAdmissionV1,
        returned_record: &LogicalShardRecord,
        returned_session: &LogicalShardLease,
    ) -> Result<(), crate::ControlError> {
        if !validate_plan_for_target(&self.0.payload, returned_plan) {
            return Err(crate::ControlError::InvalidRecord(
                "reconciled lifetime observation returned a foreign owner-admission plan"
                    .to_owned(),
            ));
        }
        if returned_session != returned_plan.lease() {
            return Err(crate::ControlError::InvalidRecord(
                "reconciled lifetime observation returned a foreign owner session".to_owned(),
            ));
        }
        if !validate_live_record_for_target(&self.0.payload, returned_plan, returned_record) {
            return Err(crate::ControlError::InvalidRecord(
                "reconciled lifetime observation returned a foreign committed record".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn complete(
        self,
        result: ReconcileOwnerAdmissionResultV1,
    ) -> ReconcileOwnerAdmissionOutcomeV1 {
        ReconcileOwnerAdmissionOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedReconcileOwnerAdmissionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(
            formatter,
            "ClaimedReconcileOwnerAdmissionCommandV1",
            &self.0.core,
        )
    }
}

pub struct ReconcileOwnerAdmissionOutcomeV1(
    CommandOutcomeEnvelope<ReconcileOwnerAdmissionResultV1>,
);

impl fmt::Debug for ReconcileOwnerAdmissionOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "ReconcileOwnerAdmissionOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

#[cfg(test)]
pub(crate) struct ReconcileOwnerAdmissionWitnessV1(
    CommandWitnessEnvelope<ReconcileOwnerAdmissionTargetV1>,
);

#[cfg(test)]
impl ReconcileOwnerAdmissionWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: ReconcileOwnerAdmissionOutcomeV1,
    ) -> Result<VerifiedReconcileOwnerAdmissionV1, ReconcileOwnerAdmissionWitnessFailureV1> {
        resolve_command(self.0, outcome.0, validate_reconcile_outcome)
            .map(|result| VerifiedReconcileOwnerAdmissionV1 {
                result: Box::new(result),
            })
            .map_err(|failure| ReconcileOwnerAdmissionWitnessFailureV1 {
                code: failure.code,
                recovery_target: Box::new(failure.recovery_target),
            })
    }
}

/// Read-only storage-neutral input of one exact serving publication command.
#[derive(Clone, Copy)]
pub struct PublishOwnerServingInspectionV1<'a> {
    pub plan: &'a PlannedOwnerAdmissionV1,
    pub source: &'a LogicalShardRecord,
    pub publication: &'a RecoveryPublication,
    pub target: &'a LogicalShardRecord,
}

impl fmt::Debug for PublishOwnerServingInspectionV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PublishOwnerServingInspectionV1(<redacted>)")
    }
}

/// One coordinator-minted, exact Recovering-to-Serving command.
pub struct PublishOwnerServingCommandV1(CommandEnvelope<PlannedOwnerServingPublicationV1>);

impl PublishOwnerServingCommandV1 {
    pub fn inspect(&self) -> PublishOwnerServingInspectionV1<'_> {
        publish_owner_serving_inspection(&self.0.payload)
    }

    pub fn claim_execution(self) -> ClaimedPublishOwnerServingCommandV1 {
        ClaimedPublishOwnerServingCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for PublishOwnerServingCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "PublishOwnerServingCommandV1", &self.0.core)
    }
}

pub struct ClaimedPublishOwnerServingCommandV1(
    ClaimedCommandEnvelope<PlannedOwnerServingPublicationV1>,
);

impl ClaimedPublishOwnerServingCommandV1 {
    pub fn inspect(&self) -> PublishOwnerServingInspectionV1<'_> {
        publish_owner_serving_inspection(&self.0.payload)
    }

    pub fn outcome_unknown(&self) -> PublishOwnerServingResultV1 {
        PublishOwnerServingResultV1::OutcomeUnknown(
            OwnerServingPublicationOutcomeUnknownV1::for_publication(&self.0.payload),
        )
    }

    pub fn non_expiring_lifetime_observation(
        &self,
        returned_record: &LogicalShardRecord,
        returned_claim: &OwnerAdmissionClaimV1,
        returned_session: &LogicalShardLease,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        self.validate_current_bindings(returned_record, returned_claim, returned_session)?;
        OwnerSessionLifetimeObservationV1::non_expiring_for_committed_descendant(
            self.0.payload.plan(),
            returned_record,
            returned_session,
        )
    }

    pub fn finite_lifetime_observation(
        &self,
        returned_record: &LogicalShardRecord,
        returned_claim: &OwnerAdmissionClaimV1,
        returned_session: &LogicalShardLease,
        observed_ttl_seconds: NonZeroU64,
        proof_digest: OwnerSessionLifetimeProofDigestV1,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        self.validate_current_bindings(returned_record, returned_claim, returned_session)?;
        OwnerSessionLifetimeObservationV1::finite_for_committed_descendant(
            Arc::clone(&self.0.core),
            self.0.payload.plan(),
            returned_record,
            returned_session,
            observed_ttl_seconds,
            proof_digest,
        )
    }

    fn validate_current_bindings(
        &self,
        returned_record: &LogicalShardRecord,
        returned_claim: &OwnerAdmissionClaimV1,
        returned_session: &LogicalShardLease,
    ) -> Result<(), crate::ControlError> {
        let expected_claim = OwnerAdmissionClaimV1::prepared(self.0.payload.plan())?.commit()?;
        if returned_record != self.0.payload.target()
            || returned_claim != &expected_claim
            || returned_session != self.0.payload.plan().lease()
        {
            return Err(crate::ControlError::InvalidRecord(
                "serving publication lifetime observation is not exact-bound".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn complete(self, result: PublishOwnerServingResultV1) -> PublishOwnerServingOutcomeV1 {
        PublishOwnerServingOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedPublishOwnerServingCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(
            formatter,
            "ClaimedPublishOwnerServingCommandV1",
            &self.0.core,
        )
    }
}

pub struct PublishOwnerServingOutcomeV1(CommandOutcomeEnvelope<PublishOwnerServingResultV1>);

impl fmt::Debug for PublishOwnerServingOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "PublishOwnerServingOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

fn publish_owner_serving_inspection(
    publication: &PlannedOwnerServingPublicationV1,
) -> PublishOwnerServingInspectionV1<'_> {
    PublishOwnerServingInspectionV1 {
        plan: publication.plan(),
        source: publication.source(),
        publication: publication.publication(),
        target: publication.target(),
    }
}

/// Read-only storage-neutral input of one exact session-renewal command.
#[derive(Clone, Copy)]
pub struct RenewOwnerSessionInspectionV1<'a> {
    pub plan: &'a PlannedOwnerAdmissionV1,
    pub claim: &'a OwnerAdmissionClaimV1,
    pub session: &'a LogicalShardLease,
}

impl fmt::Debug for RenewOwnerSessionInspectionV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RenewOwnerSessionInspectionV1(<redacted>)")
    }
}

/// One coordinator-minted, exact owner-session renewal command.
pub struct RenewOwnerSessionCommandV1(CommandEnvelope<OwnerSessionRenewalTargetV1>);

impl RenewOwnerSessionCommandV1 {
    pub fn inspect(&self) -> RenewOwnerSessionInspectionV1<'_> {
        renew_owner_session_inspection(&self.0.payload)
    }

    pub fn claim_execution(self) -> ClaimedRenewOwnerSessionCommandV1 {
        ClaimedRenewOwnerSessionCommandV1(claim_command(self.0))
    }
}

impl fmt::Debug for RenewOwnerSessionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "RenewOwnerSessionCommandV1", &self.0.core)
    }
}

pub struct ClaimedRenewOwnerSessionCommandV1(ClaimedCommandEnvelope<OwnerSessionRenewalTargetV1>);

impl ClaimedRenewOwnerSessionCommandV1 {
    pub fn inspect(&self) -> RenewOwnerSessionInspectionV1<'_> {
        renew_owner_session_inspection(&self.0.payload)
    }

    pub fn outcome_unknown(&self) -> RenewOwnerSessionResultV1 {
        RenewOwnerSessionResultV1::OutcomeUnknown(OwnerSessionRenewalOutcomeUnknownV1::for_target(
            &self.0.payload,
        ))
    }

    pub fn non_expiring_lifetime_observation(
        &self,
        returned_record: &LogicalShardRecord,
        returned_claim: &OwnerAdmissionClaimV1,
        returned_session: &LogicalShardLease,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        self.validate_current_bindings(returned_claim, returned_session)?;
        OwnerSessionLifetimeObservationV1::non_expiring_for_committed_descendant(
            self.0.payload.plan(),
            returned_record,
            returned_session,
        )
    }

    pub fn finite_lifetime_observation(
        &self,
        returned_record: &LogicalShardRecord,
        returned_claim: &OwnerAdmissionClaimV1,
        returned_session: &LogicalShardLease,
        observed_ttl_seconds: NonZeroU64,
        proof_digest: OwnerSessionLifetimeProofDigestV1,
    ) -> Result<OwnerSessionLifetimeObservationV1, crate::ControlError> {
        self.validate_current_bindings(returned_claim, returned_session)?;
        OwnerSessionLifetimeObservationV1::finite_for_committed_descendant(
            Arc::clone(&self.0.core),
            self.0.payload.plan(),
            returned_record,
            returned_session,
            observed_ttl_seconds,
            proof_digest,
        )
    }

    fn validate_current_bindings(
        &self,
        returned_claim: &OwnerAdmissionClaimV1,
        returned_session: &LogicalShardLease,
    ) -> Result<(), crate::ControlError> {
        if returned_claim != self.0.payload.claim() || returned_session != self.0.payload.session()
        {
            return Err(crate::ControlError::InvalidRecord(
                "owner-session renewal lifetime observation is not exact-bound".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn complete(self, result: RenewOwnerSessionResultV1) -> RenewOwnerSessionOutcomeV1 {
        RenewOwnerSessionOutcomeV1(complete_command(self.0, result))
    }
}

impl fmt::Debug for ClaimedRenewOwnerSessionCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_execution_debug(formatter, "ClaimedRenewOwnerSessionCommandV1", &self.0.core)
    }
}

pub struct RenewOwnerSessionOutcomeV1(CommandOutcomeEnvelope<RenewOwnerSessionResultV1>);

impl fmt::Debug for RenewOwnerSessionOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_outcome_debug(
            formatter,
            "RenewOwnerSessionOutcomeV1",
            &self.0.core,
            &self.0.result,
        )
    }
}

fn renew_owner_session_inspection(
    target: &OwnerSessionRenewalTargetV1,
) -> RenewOwnerSessionInspectionV1<'_> {
    RenewOwnerSessionInspectionV1 {
        plan: target.plan(),
        claim: target.claim(),
        session: target.session(),
    }
}

#[cfg(test)]
pub(crate) struct PublishOwnerServingWitnessV1(
    CommandWitnessEnvelope<PlannedOwnerServingPublicationV1>,
);

#[cfg(test)]
impl PublishOwnerServingWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: PublishOwnerServingOutcomeV1,
    ) -> Result<PublishOwnerServingResultV1, OwnerAdmissionCommandWitnessError> {
        resolve_command(self.0, outcome.0, validate_publish_owner_serving_outcome)
            .map_err(|failure| failure.code)
    }
}

#[cfg(test)]
pub(crate) struct RenewOwnerSessionWitnessV1(CommandWitnessEnvelope<OwnerSessionRenewalTargetV1>);

#[cfg(test)]
impl RenewOwnerSessionWitnessV1 {
    pub(crate) fn resolve(
        self,
        outcome: RenewOwnerSessionOutcomeV1,
    ) -> Result<RenewOwnerSessionResultV1, OwnerAdmissionCommandWitnessError> {
        resolve_command(self.0, outcome.0, validate_renew_owner_session_outcome)
            .map_err(|failure| failure.code)
    }
}

fn redacted_execution_debug(
    formatter: &mut fmt::Formatter<'_>,
    type_name: &str,
    core: &OwnerAdmissionCommandCore,
) -> fmt::Result {
    formatter
        .debug_struct(type_name)
        .field("execution_state", &core.state_name())
        .finish_non_exhaustive()
}

fn redacted_outcome_debug(
    formatter: &mut fmt::Formatter<'_>,
    type_name: &str,
    core: &OwnerAdmissionCommandCore,
    result: &impl fmt::Debug,
) -> fmt::Result {
    formatter
        .debug_struct(type_name)
        .field("execution_state", &core.state_name())
        .field("result", result)
        .finish_non_exhaustive()
}

#[cfg(test)]
fn redacted_witness_failure_debug(
    formatter: &mut fmt::Formatter<'_>,
    type_name: &str,
    code: OwnerAdmissionCommandWitnessError,
    recovery_target: &str,
) -> fmt::Result {
    formatter
        .debug_struct(type_name)
        .field("code", &code)
        .field("recovery_target", &recovery_target)
        .finish_non_exhaustive()
}

#[cfg(test)]
pub(crate) fn mint_prepare_owner_admission_command(
    intent: OwnerAdmissionIntentV1,
) -> (
    PrepareOwnerAdmissionCommandV1,
    PrepareOwnerAdmissionWitnessV1,
) {
    let (command, witness) = mint_command(intent);
    (
        PrepareOwnerAdmissionCommandV1(command),
        PrepareOwnerAdmissionWitnessV1(witness),
    )
}

#[cfg(test)]
pub(crate) fn mint_commit_owner_admission_command(
    plan: PlannedOwnerAdmissionV1,
) -> (CommitOwnerAdmissionCommandV1, CommitOwnerAdmissionWitnessV1) {
    let (command, witness) = mint_command(plan);
    (
        CommitOwnerAdmissionCommandV1(command),
        CommitOwnerAdmissionWitnessV1(witness),
    )
}

#[cfg(test)]
pub(crate) fn mint_abort_owner_admission_command(
    plan: PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionAbortReasonV1,
) -> (AbortOwnerAdmissionCommandV1, AbortOwnerAdmissionWitnessV1) {
    let (command, witness) = mint_command(AbortOwnerAdmissionPayloadV1 { plan, reason });
    (
        AbortOwnerAdmissionCommandV1(command),
        AbortOwnerAdmissionWitnessV1(witness),
    )
}

#[cfg(test)]
pub(crate) fn mint_released_terminate_owner_admission_command(
    plan: PlannedOwnerAdmissionV1,
) -> (
    TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionWitnessV1,
) {
    mint_terminate_owner_admission_command(plan, OwnerAdmissionTerminationReasonV1::Released)
}

#[cfg(test)]
pub(crate) fn mint_authority_cutover_terminate_owner_admission_command(
    plan: PlannedOwnerAdmissionV1,
    migration_id: OperationId,
) -> (
    TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionWitnessV1,
) {
    mint_terminate_owner_admission_command(
        plan,
        OwnerAdmissionTerminationReasonV1::AuthorityCutover { migration_id },
    )
}

#[cfg(test)]
pub(crate) fn mint_expired_terminate_owner_admission_command(
    permit: ReconciledOwnerLeaseExpiryV1,
) -> (
    TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionWitnessV1,
) {
    mint_terminate_owner_admission_command(
        permit.plan,
        OwnerAdmissionTerminationReasonV1::LeaseExpired {
            evidence_digest: permit.evidence_digest,
        },
    )
}

#[cfg(test)]
fn mint_terminate_owner_admission_command(
    plan: PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionTerminationReasonV1,
) -> (
    TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionWitnessV1,
) {
    let (command, witness) = mint_command(TerminateOwnerAdmissionPayloadV1 { plan, reason });
    (
        TerminateOwnerAdmissionCommandV1(command),
        TerminateOwnerAdmissionWitnessV1(witness),
    )
}

#[cfg(test)]
pub(crate) fn mint_reconcile_owner_admission_intent_command(
    intent: OwnerAdmissionIntentV1,
) -> (
    ReconcileOwnerAdmissionCommandV1,
    ReconcileOwnerAdmissionWitnessV1,
) {
    mint_reconcile_owner_admission_command(ReconcileOwnerAdmissionTargetV1::IntentOnly(intent))
}

#[cfg(test)]
pub(crate) fn mint_reconcile_owner_admission_plan_command(
    plan: PlannedOwnerAdmissionV1,
) -> (
    ReconcileOwnerAdmissionCommandV1,
    ReconcileOwnerAdmissionWitnessV1,
) {
    mint_reconcile_owner_admission_command(ReconcileOwnerAdmissionTargetV1::ExactPlan(plan))
}

#[cfg(test)]
pub(crate) fn mint_reconcile_owner_serving_command(
    publication: PlannedOwnerServingPublicationV1,
) -> (
    ReconcileOwnerAdmissionCommandV1,
    ReconcileOwnerAdmissionWitnessV1,
) {
    publication
        .validate()
        .expect("test serving publication must remain canonical");
    mint_reconcile_owner_admission_command(ReconcileOwnerAdmissionTargetV1::ExactServing(
        publication,
    ))
}

#[cfg(test)]
pub(crate) fn mint_publish_owner_serving_command(
    publication: PlannedOwnerServingPublicationV1,
) -> (PublishOwnerServingCommandV1, PublishOwnerServingWitnessV1) {
    publication
        .validate()
        .expect("test serving publication must remain canonical");
    let (command, witness) = mint_command(publication);
    (
        PublishOwnerServingCommandV1(command),
        PublishOwnerServingWitnessV1(witness),
    )
}

#[cfg(test)]
pub(crate) fn mint_renew_owner_session_command(
    target: OwnerSessionRenewalTargetV1,
) -> (RenewOwnerSessionCommandV1, RenewOwnerSessionWitnessV1) {
    target
        .validate()
        .expect("test owner-session renewal target must remain canonical");
    let (command, witness) = mint_command(target);
    (
        RenewOwnerSessionCommandV1(command),
        RenewOwnerSessionWitnessV1(witness),
    )
}

#[cfg(test)]
fn mint_reconcile_owner_admission_command(
    target: ReconcileOwnerAdmissionTargetV1,
) -> (
    ReconcileOwnerAdmissionCommandV1,
    ReconcileOwnerAdmissionWitnessV1,
) {
    let (command, witness) = mint_command(target);
    (
        ReconcileOwnerAdmissionCommandV1(command),
        ReconcileOwnerAdmissionWitnessV1(witness),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnerServingAdmission;
    use crate::{
        ConsistencyDomainId, LogicalShardState, MetadataAuthorityBinding,
        MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRecord,
        MetadataAuthorityRevision, MetadataContractDigest, MetadataProviderProfileId, NodeId,
        OwnerAdmissionRejectionReasonV1, OwnerAdmissionTerminationReasonV1,
        OwnerRuntimeReservationDigest, PlacementGeneration, RootId, RootLayoutGeneration,
        RootLayoutProfile, RootPartitionId, RootPlacement, RootPlacementLifecycle,
    };

    #[derive(Debug, PartialEq, Eq)]
    enum TestResult {
        Terminal,
    }

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn owner_incarnation(value: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([value; 16])
    }

    fn expiry_digest(value: u8) -> OwnerLeaseExpiryEvidenceDigest {
        OwnerLeaseExpiryEvidenceDigest::from_bytes([value; 32]).unwrap()
    }

    fn lifetime_proof_digest(value: u8) -> OwnerSessionLifetimeProofDigestV1 {
        OwnerSessionLifetimeProofDigestV1::from_bytes([value; 32]).unwrap()
    }

    fn admission() -> OwnerServingAdmission {
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
        OwnerServingAdmission::stable(placement, authority).unwrap()
    }

    fn intent_with_incarnation(endpoint: &str, incarnation: u8) -> OwnerAdmissionIntentV1 {
        OwnerAdmissionIntentV1::fresh(
            admission(),
            LogicalShardRecord::unassigned(shard_id(2)),
            NodeId::new("node-a").unwrap(),
            owner_incarnation(incarnation),
            endpoint.to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([7; 32]).unwrap(),
        )
        .unwrap()
    }

    fn intent(endpoint: &str) -> OwnerAdmissionIntentV1 {
        intent_with_incarnation(endpoint, 8)
    }

    fn plan_for_intent(intent: OwnerAdmissionIntentV1) -> PlannedOwnerAdmissionV1 {
        let lease = LogicalShardLease {
            logical_shard_id: intent.logical_shard_id(),
            owner: intent.owner().clone(),
            owner_epoch: intent.planned_epoch(),
            owner_incarnation_id: intent.owner_incarnation_id(),
            lease_id: 9,
            authority: intent.admission().authority().fence(),
        };
        PlannedOwnerAdmissionV1::new(intent, lease).unwrap()
    }

    fn plan(endpoint: &str) -> PlannedOwnerAdmissionV1 {
        plan_for_intent(intent(endpoint))
    }

    fn serving_publication(plan: PlannedOwnerAdmissionV1) -> PlannedOwnerServingPublicationV1 {
        let source = expected_recovering_record_for_plan(&plan);
        let publication = RecoveryPublication {
            checkpoint: None,
            log: None,
            durable_lsn: source.durable_lsn,
        };
        PlannedOwnerServingPublicationV1::new(plan, source, publication).unwrap()
    }

    fn terminated_record(plan: &PlannedOwnerAdmissionV1) -> LogicalShardRecord {
        let mut record = plan.intent().expected_unowned_shard().clone();
        record.owner_epoch = Some(plan.intent().planned_epoch());
        record.owner_incarnation_id = Some(plan.intent().owner_incarnation_id());
        record
    }

    fn successor_plan(
        predecessor: &PlannedOwnerAdmissionV1,
        predecessor_reason: &OwnerAdmissionTerminationReasonV1,
        endpoint: &str,
        incarnation: u8,
    ) -> PlannedOwnerAdmissionV1 {
        let intent = OwnerAdmissionIntentV1::successor(
            admission(),
            terminated_record(predecessor),
            expected_terminated_claim(predecessor, predecessor_reason).unwrap(),
            NodeId::new(format!("node-{incarnation}")).unwrap(),
            owner_incarnation(incarnation),
            endpoint.to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([incarnation; 32]).unwrap(),
        )
        .unwrap();
        plan_for_intent(intent)
    }

    #[test]
    fn exact_witness_accepts_only_claimed_and_completed_command() {
        let (command, witness) = mint_command(7_u8);
        assert_eq!(command.payload, 7);
        let claimed = claim_command(command);
        assert_eq!(claimed.payload, 7);
        let outcome = complete_command(claimed, TestResult::Terminal);
        assert!(matches!(
            resolve_command(witness, outcome, |payload, result, _| {
                *payload == 7 && *result == TestResult::Terminal
            }),
            Ok(TestResult::Terminal)
        ));
    }

    #[test]
    fn exact_witness_rejects_foreign_command_and_foreign_result() {
        let (first, first_witness) = mint_command(1_u8);
        let (second, _second_witness) = mint_command(2_u8);
        let foreign_outcome = complete_command(claim_command(second), TestResult::Terminal);
        let failure = resolve_command(first_witness, foreign_outcome, |_, _, _| true).unwrap_err();
        assert_eq!(
            failure.code,
            OwnerAdmissionCommandWitnessError::ForeignCommand
        );
        assert_eq!(failure.recovery_target, 1);
        drop(first);

        let (command, witness) = mint_command(3_u8);
        let outcome = complete_command(claim_command(command), TestResult::Terminal);
        let failure = resolve_command(witness, outcome, |_, _, _| false).unwrap_err();
        assert_eq!(
            failure.code,
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(failure.recovery_target, 3);
    }

    #[test]
    fn prepare_witness_rejects_plan_from_another_command() {
        let expected_intent = intent("10.0.0.1:7000");
        let recovery_intent = expected_intent.clone();
        let foreign_plan = plan("10.0.0.2:7000");
        let foreign_claim = OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap();
        let (command, witness) = mint_prepare_owner_admission_command(expected_intent);
        let claimed = command.claim_execution();
        let outcome = claimed.complete(PrepareOwnerAdmissionResultV1::Prepared {
            sentinel: OwnerAdmissionPlanSentinelV1::for_plan(&foreign_plan),
            plan: Box::new(foreign_plan),
            claim: foreign_claim,
        });
        let failure = witness.resolve(outcome).unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(failure.recovery_intent(), &recovery_intent);
        let rendered = format!("{failure:?}");
        assert!(!rendered.contains("10.0.0.1:7000"));
        assert_eq!(failure.into_recovery_intent(), recovery_intent);
    }

    #[test]
    fn exact_plan_reconcile_rejects_backend_substitution() {
        let expected_plan = plan("10.0.0.1:7000");
        let foreign_plan = plan("10.0.0.2:7000");
        let foreign_claim = OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap();
        let (command, witness) = mint_reconcile_owner_admission_plan_command(expected_plan);
        let claimed = command.claim_execution();
        let outcome = claimed.complete(ReconcileOwnerAdmissionResultV1::Prepared {
            sentinel: OwnerAdmissionPlanSentinelV1::for_plan(&foreign_plan),
            plan: Box::new(foreign_plan),
            claim: foreign_claim,
        });
        assert!(matches!(
            witness.resolve(outcome),
            Err(failure)
                if failure.code()
                    == OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        ));
    }

    #[test]
    fn outcome_unknown_is_derived_from_the_exact_command() {
        let expected_plan = plan("10.0.0.1:7000");
        let expected_intent_digest = expected_plan.intent().digest();
        let expected_plan_digest = expected_plan.digest();
        let (command, witness) = mint_commit_owner_admission_command(expected_plan);
        let claimed = command.claim_execution();
        let result = claimed.outcome_unknown();
        let outcome = claimed.complete(result);
        let CommitOwnerAdmissionResultV1::OutcomeUnknown(evidence) =
            witness.resolve(outcome).unwrap()
        else {
            panic!("expected outcome unknown")
        };
        assert_eq!(evidence.intent_digest(), expected_intent_digest);
        assert_eq!(evidence.plan_digest(), Some(expected_plan_digest));
        assert!(!format!("{evidence:?}").contains("070707"));
    }

    #[test]
    fn commit_fresh_result_is_recovering_but_replay_accepts_exact_serving_descendant() {
        let exact_plan = plan("10.0.0.1:7000");
        let committed = OwnerAdmissionClaimV1::prepared(&exact_plan)
            .unwrap()
            .commit()
            .unwrap();
        let mut serving = expected_recovering_record_for_plan(&exact_plan);
        serving.state = LogicalShardState::Serving;

        let (command, witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let outcome = command
            .claim_execution()
            .complete(CommitOwnerAdmissionResultV1::Committed {
                shard: serving.clone(),
                lease: exact_plan.lease().clone(),
                claim: committed.clone(),
                lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
            });
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let (command, witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(CommitOwnerAdmissionResultV1::AlreadyCommitted {
                    shard: serving.clone(),
                    lease: exact_plan.lease().clone(),
                    claim: committed.clone(),
                    lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(CommitOwnerAdmissionResultV1::AlreadyCommitted { .. })
        ));

        serving.endpoint = Some("10.0.0.99:7000".to_owned());
        let (command, witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(CommitOwnerAdmissionResultV1::AlreadyCommitted {
                    shard: serving,
                    lease: exact_plan.lease().clone(),
                    claim: committed,
                    lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
                });
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
    }

    #[test]
    fn terminate_accepts_released_authority_cutover_and_verified_lease_expiry() {
        let exact_plan = plan("10.0.0.1:7000");
        let terminal = terminated_record(&exact_plan);

        let released_reason = OwnerAdmissionTerminationReasonV1::Released;
        let released_claim = expected_terminated_claim(&exact_plan, &released_reason).unwrap();
        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        assert_eq!(command.inspect().reason, &released_reason);
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Terminated {
                    shard: terminal.clone(),
                    claim: released_claim.clone(),
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::Terminated { .. })
        ));

        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        let outcome = command.claim_execution().complete(
            TerminateOwnerAdmissionResultV1::AlreadyTerminated {
                shard: terminal.clone(),
                claim: released_claim,
            },
        );
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::AlreadyTerminated { .. })
        ));

        let migration_id = OperationId::from_bytes([11; 16]);
        let cutover_reason = OwnerAdmissionTerminationReasonV1::AuthorityCutover { migration_id };
        let cutover_claim = expected_terminated_claim(&exact_plan, &cutover_reason).unwrap();
        let (command, witness) = mint_authority_cutover_terminate_owner_admission_command(
            exact_plan.clone(),
            migration_id,
        );
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Terminated {
                    shard: terminal.clone(),
                    claim: cutover_claim,
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::Terminated { .. })
        ));

        let evidence_digest = expiry_digest(12);
        let committed = expected_committed_claim(&exact_plan).unwrap();
        let (reconcile, reconcile_witness) =
            mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let reconcile_outcome = reconcile.claim_execution().complete(
            ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                plan: Box::new(exact_plan.clone()),
                shard: expected_recovering_record_for_plan(&exact_plan),
                claim: Box::new(committed),
                expected_session: exact_plan.lease().clone(),
                evidence_digest,
            },
        );
        let verified = reconcile_witness.resolve(reconcile_outcome).unwrap();
        assert!(matches!(
            verified.result(),
            ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                evidence_digest: actual,
                ..
            } if *actual == evidence_digest
        ));
        let permit = verified.into_lease_expiry().unwrap();
        let expiry_reason = OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest };
        let expiry_claim = expected_terminated_claim(&exact_plan, &expiry_reason).unwrap();
        let (command, witness) = mint_expired_terminate_owner_admission_command(permit);
        assert_eq!(command.inspect().reason, &expiry_reason);
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Terminated {
                    shard: terminal,
                    claim: expiry_claim,
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::Terminated { .. })
        ));
    }

    #[test]
    fn terminate_witness_rejects_foreign_claim_record_and_reason() {
        let exact_plan = plan("10.0.0.1:7000");
        let exact_reason = OwnerAdmissionTerminationReasonV1::Released;
        let foreign_plan = plan("10.0.0.2:7000");
        let foreign_claim =
            expected_terminated_claim(&foreign_plan, &OwnerAdmissionTerminationReasonV1::Released)
                .unwrap();
        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Terminated {
                    shard: terminated_record(&foreign_plan),
                    claim: foreign_claim,
                });
        let failure = witness.resolve(outcome).unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(failure.recovery_plan(), &exact_plan);
        assert_eq!(failure.recovery_reason(), &exact_reason);
        assert!(!format!("{failure:?}").contains("10.0.0.1:7000"));
        assert_eq!(
            failure.into_recovery(),
            (exact_plan.clone(), exact_reason.clone())
        );

        let exact_claim = expected_terminated_claim(&exact_plan, &exact_reason).unwrap();
        let mut forged_record = terminated_record(&exact_plan);
        forged_record.endpoint = Some("10.0.0.99:7000".to_owned());
        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        let outcome = command.claim_execution().complete(
            TerminateOwnerAdmissionResultV1::AlreadyTerminated {
                shard: forged_record,
                claim: exact_claim.clone(),
            },
        );
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        let outcome = command
            .claim_execution()
            .complete(TerminateOwnerAdmissionResultV1::DurableConflict { claim: exact_claim });
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let conflicting_reason = OwnerAdmissionTerminationReasonV1::AuthorityCutover {
            migration_id: OperationId::from_bytes([13; 16]),
        };
        let conflicting_claim =
            expected_terminated_claim(&exact_plan, &conflicting_reason).unwrap();
        let (command, witness) = mint_released_terminate_owner_admission_command(exact_plan);
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::DurableConflict {
                    claim: conflicting_claim,
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::DurableConflict { .. })
        ));
    }

    #[test]
    fn old_termination_replay_closes_after_a_successor_commit() {
        let reason = OwnerAdmissionTerminationReasonV1::Released;
        let first_plan = plan("10.0.0.1:7000");
        let old_plan = successor_plan(&first_plan, &reason, "10.0.0.2:7000", 9);
        let old_claim = expected_terminated_claim(&old_plan, &reason).unwrap();
        let old_terminal = terminated_record(&old_plan);

        let (command, witness) = mint_released_terminate_owner_admission_command(old_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Terminated {
                    shard: old_terminal.clone(),
                    claim: old_claim.clone(),
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::Terminated { .. })
        ));

        let successor = successor_plan(&old_plan, &reason, "10.0.0.3:7000", 10);
        let successor_shard = expected_recovering_record_for_plan(&successor);
        let (command, witness) = mint_released_terminate_owner_admission_command(old_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Superseded {
                    shard: successor_shard.clone(),
                    claim: old_claim.clone(),
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::Superseded { .. })
        ));

        for non_successor in [
            old_terminal,
            terminated_record(&first_plan),
            LogicalShardRecord::unassigned(old_plan.intent().logical_shard_id()),
        ] {
            let (command, witness) =
                mint_released_terminate_owner_admission_command(old_plan.clone());
            let outcome =
                command
                    .claim_execution()
                    .complete(TerminateOwnerAdmissionResultV1::Superseded {
                        shard: non_successor,
                        claim: old_claim.clone(),
                    });
            assert_eq!(
                witness.resolve(outcome).unwrap_err().code(),
                OwnerAdmissionCommandWitnessError::ResultBindingMismatch
            );
        }

        let different_reason = OwnerAdmissionTerminationReasonV1::AuthorityCutover {
            migration_id: OperationId::from_bytes([18; 16]),
        };
        let (command, witness) = mint_released_terminate_owner_admission_command(old_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(TerminateOwnerAdmissionResultV1::Superseded {
                    shard: successor_shard,
                    claim: expected_terminated_claim(&old_plan, &different_reason).unwrap(),
                });
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
    }

    #[test]
    fn only_verified_expired_committed_can_mint_a_lease_expiry_termination() {
        let exact_plan = plan("10.0.0.1:7000");
        let (reconcile, witness) = mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let outcome =
            reconcile
                .claim_execution()
                .complete(ReconcileOwnerAdmissionResultV1::NotDispatched(
                    ReconcileOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                ));
        let verified = witness.resolve(outcome).unwrap();
        let verified = verified.into_lease_expiry().unwrap_err();
        assert!(matches!(
            verified.into_result(),
            ReconcileOwnerAdmissionResultV1::NotDispatched(
                ReconcileOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect
            )
        ));

        let foreign_plan = plan("10.0.0.2:7000");
        let committed = expected_committed_claim(&foreign_plan).unwrap();
        let (reconcile, witness) = mint_reconcile_owner_admission_plan_command(exact_plan);
        let outcome = reconcile.claim_execution().complete(
            ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                plan: Box::new(foreign_plan.clone()),
                shard: expected_recovering_record_for_plan(&foreign_plan),
                claim: Box::new(committed),
                expected_session: foreign_plan.lease().clone(),
                evidence_digest: expiry_digest(14),
            },
        );
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let exact_plan = plan("10.0.0.1:7000");
        let committed = expected_committed_claim(&exact_plan).unwrap();
        let mut wrong_session = exact_plan.lease().clone();
        wrong_session.lease_id += 1;
        let (reconcile, witness) = mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let outcome = reconcile.claim_execution().complete(
            ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                plan: Box::new(exact_plan.clone()),
                shard: expected_recovering_record_for_plan(&exact_plan),
                claim: Box::new(committed),
                expected_session: wrong_session,
                evidence_digest: expiry_digest(15),
            },
        );
        assert_eq!(
            witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
    }

    #[test]
    fn lease_expiry_witness_failure_retains_verified_recovery_identity() {
        let exact_plan = plan("10.0.0.1:7000");
        let evidence_digest = expiry_digest(16);
        let committed = expected_committed_claim(&exact_plan).unwrap();
        let (reconcile, reconcile_witness) =
            mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let reconcile_outcome = reconcile.claim_execution().complete(
            ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                plan: Box::new(exact_plan.clone()),
                shard: expected_recovering_record_for_plan(&exact_plan),
                claim: Box::new(committed),
                expected_session: exact_plan.lease().clone(),
                evidence_digest,
            },
        );
        let permit = reconcile_witness
            .resolve(reconcile_outcome)
            .unwrap()
            .into_lease_expiry()
            .unwrap();
        let (command, witness) = mint_expired_terminate_owner_admission_command(permit);
        let foreign_plan = plan("10.0.0.2:7000");
        let foreign_reason = OwnerAdmissionTerminationReasonV1::LeaseExpired {
            evidence_digest: expiry_digest(17),
        };
        let outcome = command.claim_execution().complete(
            TerminateOwnerAdmissionResultV1::AlreadyTerminated {
                shard: terminated_record(&foreign_plan),
                claim: expected_terminated_claim(&foreign_plan, &foreign_reason).unwrap(),
            },
        );
        let failure = witness.resolve(outcome).unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(failure.recovery_plan(), &exact_plan);
        assert_eq!(
            failure.recovery_reason(),
            &OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest }
        );
        assert_eq!(
            failure.into_recovery(),
            (
                exact_plan,
                OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest },
            )
        );
    }

    #[test]
    fn command_debug_does_not_format_endpoint_or_digest() {
        let (command, _witness) = mint_prepare_owner_admission_command(intent("secret:7000"));
        let rendered = format!("{command:?}");
        assert!(!rendered.contains("secret:7000"));
        assert!(!rendered.contains("digest"));
        assert!(rendered.contains("minted"));

        let exact_plan = plan("terminate-secret:7000");
        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        assert_eq!(command.inspect().plan, &exact_plan);
        let rendered = format!("{command:?}");
        assert!(!rendered.contains("terminate-secret:7000"));
        assert!(!rendered.contains("digest"));
        let claimed = command.claim_execution();
        assert_eq!(claimed.inspect().plan, &exact_plan);
        let result = claimed.outcome_unknown();
        let outcome = claimed.complete(result);
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::OutcomeUnknown(_))
        ));
    }

    #[test]
    fn five_command_families_are_nominally_distinct() {
        fn prepare(_: PrepareOwnerAdmissionCommandV1) {}
        fn commit(_: CommitOwnerAdmissionCommandV1) {}
        fn abort(_: AbortOwnerAdmissionCommandV1) {}
        fn terminate(_: TerminateOwnerAdmissionCommandV1) {}
        fn reconcile(_: ReconcileOwnerAdmissionCommandV1) {}
        let _families = (prepare, commit, abort, terminate, reconcile);
    }

    #[test]
    fn serving_publication_and_session_renewal_are_nominally_distinct() {
        fn publish(_: PublishOwnerServingCommandV1) {}
        fn renew(_: RenewOwnerSessionCommandV1) {}
        let _families = (publish, renew);
    }

    #[test]
    fn finite_lifetime_is_exact_bound_and_redacted() {
        let exact_plan = plan("10.0.0.1:7000");
        let exact_claim = expected_committed_claim(&exact_plan).unwrap();
        let publication = serving_publication(exact_plan.clone());
        let returned_record = publication.target().clone();
        let returned_session = exact_plan.lease().clone();
        let expected_publication_digest = publication.digest();
        let expected_source_digest = publication.source_digest();
        let expected_target_digest = publication.target_digest();
        let (command, witness) = mint_publish_owner_serving_command(publication);
        let claimed = command.claim_execution();
        let lifetime = claimed
            .finite_lifetime_observation(
                &returned_record,
                &exact_claim,
                &returned_session,
                NonZeroU64::new(7).unwrap(),
                lifetime_proof_digest(21),
            )
            .unwrap();
        let OwnerSessionLifetimeObservationV1::Finite(observation) = &lifetime else {
            panic!("expected finite lifetime")
        };
        assert_eq!(observation.lease(), &returned_session);
        assert_eq!(observation.plan_digest(), exact_plan.digest());
        assert_eq!(observation.observed_ttl_seconds().get(), 7);
        assert_eq!(
            format!("{observation:?}"),
            "FiniteOwnerSessionLifetimeObservationV1(<redacted>)"
        );
        let outcome = claimed.complete(PublishOwnerServingResultV1::Published {
            shard: returned_record,
            claim: exact_claim,
            lifetime,
        });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(PublishOwnerServingResultV1::Published { .. })
        ));

        let publication = serving_publication(exact_plan);
        let (command, witness) = mint_publish_owner_serving_command(publication);
        let claimed = command.claim_execution();
        let result = claimed.outcome_unknown();
        let outcome = claimed.complete(result);
        let PublishOwnerServingResultV1::OutcomeUnknown(evidence) =
            witness.resolve(outcome).unwrap()
        else {
            panic!("expected outcome unknown")
        };
        assert_eq!(evidence.publication_digest(), expected_publication_digest);
        assert_eq!(evidence.source_record_digest(), expected_source_digest);
        assert_eq!(evidence.target_record_digest(), expected_target_digest);
        assert!(format!("{evidence:?}").contains("<redacted>"));
    }

    #[test]
    fn finite_lifetime_observation_replay_is_rejected_by_new_command_allocation() {
        let exact_plan = plan("10.0.0.31:7000");
        let recovering = expected_recovering_record_for_plan(&exact_plan);
        let committed = expected_committed_claim(&exact_plan).unwrap();

        let (old_commit, _old_witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let old_commit = old_commit.claim_execution();
        let old_lifetime = old_commit
            .finite_lifetime_observation(
                &recovering,
                exact_plan.lease(),
                NonZeroU64::new(7).unwrap(),
                lifetime_proof_digest(31),
            )
            .unwrap();
        let (new_commit, new_witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let outcome =
            new_commit
                .claim_execution()
                .complete(CommitOwnerAdmissionResultV1::Committed {
                    shard: recovering.clone(),
                    lease: exact_plan.lease().clone(),
                    claim: committed.clone(),
                    lifetime: old_lifetime,
                });
        assert_eq!(
            new_witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let (old_reconcile, _old_witness) =
            mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let old_reconcile = old_reconcile.claim_execution();
        let old_lifetime = old_reconcile
            .finite_lifetime_observation(
                &exact_plan,
                &recovering,
                exact_plan.lease(),
                NonZeroU64::new(7).unwrap(),
                lifetime_proof_digest(32),
            )
            .unwrap();
        let (new_reconcile, new_witness) =
            mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let outcome =
            new_reconcile
                .claim_execution()
                .complete(ReconcileOwnerAdmissionResultV1::Committed {
                    plan: Box::new(exact_plan.clone()),
                    shard: recovering.clone(),
                    lease: exact_plan.lease().clone(),
                    claim: Box::new(committed.clone()),
                    lifetime: old_lifetime,
                });
        assert_eq!(
            new_witness.resolve(outcome).unwrap_err().code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let publication = serving_publication(exact_plan.clone());
        let serving = publication.target().clone();
        let (old_publish, _old_witness) = mint_publish_owner_serving_command(publication.clone());
        let old_publish = old_publish.claim_execution();
        let old_lifetime = old_publish
            .finite_lifetime_observation(
                &serving,
                &committed,
                exact_plan.lease(),
                NonZeroU64::new(7).unwrap(),
                lifetime_proof_digest(33),
            )
            .unwrap();
        let (new_publish, new_witness) = mint_publish_owner_serving_command(publication);
        let outcome =
            new_publish
                .claim_execution()
                .complete(PublishOwnerServingResultV1::Published {
                    shard: serving.clone(),
                    claim: committed.clone(),
                    lifetime: old_lifetime,
                });
        assert_eq!(
            new_witness.resolve(outcome).unwrap_err(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let target =
            OwnerSessionRenewalTargetV1::new(exact_plan.clone(), committed.clone()).unwrap();
        let (old_renew, _old_witness) = mint_renew_owner_session_command(target.clone());
        let old_renew = old_renew.claim_execution();
        let old_lifetime = old_renew
            .finite_lifetime_observation(
                &serving,
                &committed,
                exact_plan.lease(),
                NonZeroU64::new(7).unwrap(),
                lifetime_proof_digest(34),
            )
            .unwrap();
        let (new_renew, new_witness) = mint_renew_owner_session_command(target);
        let outcome = new_renew
            .claim_execution()
            .complete(RenewOwnerSessionResultV1::Current {
                shard: serving,
                claim: committed,
                lifetime: old_lifetime,
            });
        assert_eq!(
            new_witness.resolve(outcome).unwrap_err(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
    }

    #[test]
    fn publication_conflict_carries_the_conflicting_exact_shard() {
        let exact_plan = plan("10.0.0.1:7000");
        let expected_publication = serving_publication(exact_plan.clone());
        let source = expected_recovering_record_for_plan(&exact_plan);
        let conflicting_publication = PlannedOwnerServingPublicationV1::new(
            exact_plan.clone(),
            source,
            RecoveryPublication {
                checkpoint: Some(crate::CheckpointRef {
                    object_key: "checkpoint-1".to_owned(),
                    lsn: 1,
                    image_bytes: 1,
                    image_digest: "image-digest".to_owned(),
                    digest: "state-digest".to_owned(),
                }),
                log: None,
                durable_lsn: 1,
            },
        )
        .unwrap();
        let committed = expected_committed_claim(&exact_plan).unwrap();
        let (command, witness) = mint_publish_owner_serving_command(expected_publication);
        let outcome =
            command
                .claim_execution()
                .complete(PublishOwnerServingResultV1::PublicationConflict {
                    shard: conflicting_publication.target().clone(),
                    claim: committed,
                });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(PublishOwnerServingResultV1::PublicationConflict { .. })
        ));
    }

    #[test]
    fn renewal_target_and_unknown_bind_exact_claim_and_session() {
        let exact_plan = plan("10.0.0.1:7000");
        let committed = expected_committed_claim(&exact_plan).unwrap();
        assert!(OwnerSessionRenewalTargetV1::new(
            exact_plan.clone(),
            OwnerAdmissionClaimV1::prepared(&exact_plan).unwrap(),
        )
        .is_err());
        let target =
            OwnerSessionRenewalTargetV1::new(exact_plan.clone(), committed.clone()).unwrap();
        let expected_target_digest = target.digest();
        let expected_claim_digest = target.claim_digest();
        let expected_session_digest = target.session_binding_digest();
        let returned_record = expected_recovering_record_for_plan(&exact_plan);
        let (command, witness) = mint_renew_owner_session_command(target);
        let claimed = command.claim_execution();
        let lifetime = claimed
            .finite_lifetime_observation(
                &returned_record,
                &committed,
                exact_plan.lease(),
                NonZeroU64::new(5).unwrap(),
                lifetime_proof_digest(22),
            )
            .unwrap();
        let outcome = claimed.complete(RenewOwnerSessionResultV1::Current {
            shard: returned_record,
            claim: committed,
            lifetime,
        });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(RenewOwnerSessionResultV1::Current { .. })
        ));

        let committed = expected_committed_claim(&exact_plan).unwrap();
        let target = OwnerSessionRenewalTargetV1::new(exact_plan, committed).unwrap();
        let (command, witness) = mint_renew_owner_session_command(target);
        let claimed = command.claim_execution();
        let result = claimed.outcome_unknown();
        let outcome = claimed.complete(result);
        let RenewOwnerSessionResultV1::OutcomeUnknown(evidence) = witness.resolve(outcome).unwrap()
        else {
            panic!("expected outcome unknown")
        };
        assert_eq!(evidence.renewal_target_digest(), expected_target_digest);
        assert_eq!(evidence.claim_digest(), expected_claim_digest);
        assert_eq!(evidence.session_binding_digest(), expected_session_digest);
    }

    #[test]
    fn reconcile_lifetime_builder_requires_backend_returned_exact_plan_record_and_session() {
        let exact_plan = plan("10.0.0.1:7000");
        let exact_record = expected_recovering_record_for_plan(&exact_plan);
        let (command, _witness) =
            mint_reconcile_owner_admission_intent_command(exact_plan.intent().clone());
        let claimed = command.claim_execution();
        let foreign_plan = plan("10.0.0.2:7000");
        assert!(claimed
            .finite_lifetime_observation(
                &foreign_plan,
                &expected_recovering_record_for_plan(&foreign_plan),
                foreign_plan.lease(),
                NonZeroU64::new(5).unwrap(),
                lifetime_proof_digest(23),
            )
            .is_err());
        let mut foreign_session = exact_plan.lease().clone();
        foreign_session.lease_id += 1;
        assert!(claimed
            .finite_lifetime_observation(
                &exact_plan,
                &exact_record,
                &foreign_session,
                NonZeroU64::new(5).unwrap(),
                lifetime_proof_digest(24),
            )
            .is_err());
    }

    #[test]
    fn rejection_reason_is_closed_and_exactly_bound() {
        let intent = intent("10.0.0.1:7000");
        let claim = OwnerAdmissionClaimV1::rejected_from_absent(
            &intent,
            OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
        )
        .unwrap();
        assert!(validate_rejected_result(&intent, &claim));
    }

    #[test]
    fn exact_plan_paths_reject_a_rejected_claim() {
        fn rejected(plan: &PlannedOwnerAdmissionV1) -> OwnerAdmissionClaimV1 {
            OwnerAdmissionClaimV1::rejected_from_absent(
                plan.intent(),
                OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
            )
            .unwrap()
        }

        let exact_plan = plan("10.0.0.1:7000");
        let (command, witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(CommitOwnerAdmissionResultV1::DurableConflict {
                    claim: rejected(&exact_plan),
                });
        let failure = witness.resolve(outcome).unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(failure.recovery_plan(), &exact_plan);
        assert_eq!(failure.into_recovery_plan(), exact_plan);

        let exact_plan = plan("10.0.0.1:7000");
        let abort_reason = OwnerAdmissionAbortReasonV1::OwnerCasRejected;
        let (command, witness) =
            mint_abort_owner_admission_command(exact_plan.clone(), abort_reason);
        let outcome =
            command
                .claim_execution()
                .complete(AbortOwnerAdmissionResultV1::DurableConflict {
                    claim: rejected(&exact_plan),
                });
        let failure = witness.resolve(outcome).unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(failure.recovery_plan(), &exact_plan);
        assert_eq!(failure.recovery_reason(), abort_reason);
        assert_eq!(failure.into_recovery(), (exact_plan, abort_reason));

        let exact_plan = plan("10.0.0.1:7000");
        let (command, witness) = mint_reconcile_owner_admission_plan_command(exact_plan.clone());
        let outcome =
            command
                .claim_execution()
                .complete(ReconcileOwnerAdmissionResultV1::Rejected {
                    claim: rejected(&exact_plan),
                });
        let failure = witness.resolve(outcome).unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert_eq!(
            failure.recovery_target(),
            &ReconcileOwnerAdmissionTargetV1::ExactPlan(exact_plan.clone())
        );
        assert_eq!(
            failure.into_recovery_target(),
            ReconcileOwnerAdmissionTargetV1::ExactPlan(exact_plan)
        );
    }

    #[test]
    fn abort_and_intent_reconcile_witnesses_validate_their_exact_payloads() {
        let plan = plan("10.0.0.1:7000");
        let reason = OwnerAdmissionAbortReasonV1::OwnerCasRejected;
        let claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .abort(reason)
            .unwrap();
        let (command, witness) = mint_abort_owner_admission_command(plan, reason);
        let outcome = command
            .claim_execution()
            .complete(AbortOwnerAdmissionResultV1::Aborted { claim });
        assert!(matches!(
            witness.resolve(outcome),
            Ok(AbortOwnerAdmissionResultV1::Aborted { .. })
        ));

        let (command, witness) =
            mint_reconcile_owner_admission_intent_command(intent("10.0.0.1:7000"));
        let outcome =
            command
                .claim_execution()
                .complete(ReconcileOwnerAdmissionResultV1::NotDispatched(
                    ReconcileOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                ));
        assert!(matches!(
            witness
                .resolve(outcome)
                .map(|verified| verified.into_result()),
            Ok(ReconcileOwnerAdmissionResultV1::NotDispatched(
                ReconcileOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect
            ))
        ));
    }

    #[test]
    fn prepare_closed_state_matrix_requires_exact_sentinel_and_claim_lineage() {
        fn resolve_prepare(
            plan: &PlannedOwnerAdmissionV1,
            result: PrepareOwnerAdmissionResultV1,
        ) -> Result<PrepareOwnerAdmissionResultV1, PrepareOwnerAdmissionWitnessFailureV1> {
            let (command, witness) = mint_prepare_owner_admission_command(plan.intent().clone());
            witness.resolve(command.claim_execution().complete(result))
        }

        let exact_plan = plan("10.0.0.1:7000");
        let prepared = OwnerAdmissionClaimV1::prepared(&exact_plan).unwrap();
        let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&exact_plan);
        assert!(matches!(
            resolve_prepare(
                &exact_plan,
                PrepareOwnerAdmissionResultV1::Prepared {
                    plan: Box::new(exact_plan.clone()),
                    claim: prepared.clone(),
                    sentinel: sentinel.clone(),
                }
            ),
            Ok(PrepareOwnerAdmissionResultV1::Prepared { .. })
        ));
        assert!(matches!(
            resolve_prepare(
                &exact_plan,
                PrepareOwnerAdmissionResultV1::ExpiredPrepared {
                    plan: Box::new(exact_plan.clone()),
                    claim: prepared.clone(),
                    expected_sentinel: sentinel,
                }
            ),
            Ok(PrepareOwnerAdmissionResultV1::ExpiredPrepared { .. })
        ));

        let committed = prepared.clone().commit().unwrap();
        let aborted = prepared
            .clone()
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap();
        let terminated = committed
            .clone()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        for claim in [committed, aborted, terminated] {
            assert!(matches!(
                resolve_prepare(
                    &exact_plan,
                    PrepareOwnerAdmissionResultV1::DurableConflict {
                        plan: Box::new(exact_plan.clone()),
                        claim,
                    }
                ),
                Ok(PrepareOwnerAdmissionResultV1::DurableConflict { .. })
            ));
        }

        let foreign_plan = plan("10.0.0.2:7000");
        let foreign_claim = OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap();
        assert!(matches!(
            resolve_prepare(
                &exact_plan,
                PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed {
                    claim: foreign_claim,
                }
            ),
            Ok(PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. })
        ));

        let wrong_sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&foreign_plan);
        let failure = resolve_prepare(
            &exact_plan,
            PrepareOwnerAdmissionResultV1::Prepared {
                plan: Box::new(exact_plan.clone()),
                claim: prepared.clone(),
                sentinel: wrong_sentinel,
            },
        )
        .unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let failure = resolve_prepare(
            &exact_plan,
            PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim: prepared },
        )
        .unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );

        let other_incarnation_plan = plan_for_intent(intent_with_incarnation("10.0.0.3:7000", 9));
        let failure = resolve_prepare(
            &exact_plan,
            PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed {
                claim: OwnerAdmissionClaimV1::prepared(&other_incarnation_plan).unwrap(),
            },
        )
        .unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
    }

    #[test]
    fn deterministic_inconsistency_is_closed_for_each_effectful_phase() {
        let exact_plan = plan("10.0.0.1:7000");
        let code = OwnerAdmissionInconsistencyCode::PlanSentinelMismatch;

        let (command, witness) = mint_prepare_owner_admission_command(exact_plan.intent().clone());
        let outcome = command
            .claim_execution()
            .complete(PrepareOwnerAdmissionResultV1::DurableInconsistent(code));
        assert!(matches!(
            witness.resolve(outcome),
            Ok(PrepareOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch
            ))
        ));

        let (command, witness) = mint_commit_owner_admission_command(exact_plan.clone());
        let outcome = command
            .claim_execution()
            .complete(CommitOwnerAdmissionResultV1::DurableInconsistent(code));
        assert!(matches!(
            witness.resolve(outcome),
            Ok(CommitOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch
            ))
        ));

        let (command, witness) = mint_abort_owner_admission_command(
            exact_plan.clone(),
            OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit,
        );
        let outcome = command
            .claim_execution()
            .complete(AbortOwnerAdmissionResultV1::DurableInconsistent(code));
        assert!(matches!(
            witness.resolve(outcome),
            Ok(AbortOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch
            ))
        ));

        let (command, witness) =
            mint_released_terminate_owner_admission_command(exact_plan.clone());
        let outcome = command
            .claim_execution()
            .complete(TerminateOwnerAdmissionResultV1::DurableInconsistent(code));
        assert!(matches!(
            witness.resolve(outcome),
            Ok(TerminateOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch
            ))
        ));

        for not_dispatched in [
            TerminateOwnerAdmissionNotDispatchedV1::InvalidPlanBeforeEffect,
            TerminateOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
            TerminateOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
            TerminateOwnerAdmissionNotDispatchedV1::RuntimeReservationLostBeforeEffect,
            TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
        ] {
            let (command, witness) =
                mint_released_terminate_owner_admission_command(exact_plan.clone());
            let outcome =
                command
                    .claim_execution()
                    .complete(TerminateOwnerAdmissionResultV1::NotDispatched(
                        not_dispatched,
                    ));
            assert!(matches!(
                witness.resolve(outcome),
                Ok(TerminateOwnerAdmissionResultV1::NotDispatched(actual))
                    if actual == not_dispatched
            ));
        }
    }

    #[test]
    fn reconcile_closed_state_matrix_distinguishes_expiry_conflict_and_split() {
        fn resolve_intent(
            intent: &OwnerAdmissionIntentV1,
            result: ReconcileOwnerAdmissionResultV1,
        ) -> Result<ReconcileOwnerAdmissionResultV1, ReconcileOwnerAdmissionWitnessFailureV1>
        {
            let (command, witness) = mint_reconcile_owner_admission_intent_command(intent.clone());
            witness
                .resolve(command.claim_execution().complete(result))
                .map(VerifiedReconcileOwnerAdmissionV1::into_result)
        }

        fn resolve_plan(
            plan: &PlannedOwnerAdmissionV1,
            result: ReconcileOwnerAdmissionResultV1,
        ) -> Result<ReconcileOwnerAdmissionResultV1, ReconcileOwnerAdmissionWitnessFailureV1>
        {
            let (command, witness) = mint_reconcile_owner_admission_plan_command(plan.clone());
            witness
                .resolve(command.claim_execution().complete(result))
                .map(VerifiedReconcileOwnerAdmissionV1::into_result)
        }

        let exact_plan = plan("10.0.0.1:7000");
        let prepared = OwnerAdmissionClaimV1::prepared(&exact_plan).unwrap();
        let committed = prepared.clone().commit().unwrap();
        let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&exact_plan);

        let sealed = OwnerAdmissionClaimV1::rejected_from_absent(
            exact_plan.intent(),
            OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
        )
        .unwrap();
        assert!(matches!(
            resolve_intent(
                exact_plan.intent(),
                ReconcileOwnerAdmissionResultV1::Rejected { claim: sealed }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::Rejected { .. })
        ));
        let ordinary_rejection = OwnerAdmissionClaimV1::rejected_from_absent(
            exact_plan.intent(),
            OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
        )
        .unwrap();
        assert!(matches!(
            resolve_intent(
                exact_plan.intent(),
                ReconcileOwnerAdmissionResultV1::Rejected {
                    claim: ordinary_rejection,
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::Rejected { .. })
        ));
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::Prepared {
                    plan: Box::new(exact_plan.clone()),
                    claim: prepared.clone(),
                    sentinel: sentinel.clone(),
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::Prepared { .. })
        ));
        let wrong_sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&plan("10.0.0.2:7000"));
        let failure = resolve_plan(
            &exact_plan,
            ReconcileOwnerAdmissionResultV1::Prepared {
                plan: Box::new(exact_plan.clone()),
                claim: prepared.clone(),
                sentinel: wrong_sentinel,
            },
        )
        .unwrap_err();
        assert_eq!(
            failure.code(),
            OwnerAdmissionCommandWitnessError::ResultBindingMismatch
        );
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::ExpiredPrepared {
                    plan: Box::new(exact_plan.clone()),
                    claim: prepared.clone(),
                    expected_sentinel: sentinel,
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::ExpiredPrepared { .. })
        ));

        let recovering_shard = expected_recovering_record_for_plan(&exact_plan);
        let mut serving_shard = recovering_shard.clone();
        serving_shard.state = LogicalShardState::Serving;
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::Committed {
                    plan: Box::new(exact_plan.clone()),
                    shard: serving_shard,
                    lease: exact_plan.lease().clone(),
                    claim: Box::new(committed.clone()),
                    lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::Committed { .. })
        ));
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                    plan: Box::new(exact_plan.clone()),
                    shard: recovering_shard,
                    claim: Box::new(committed),
                    expected_session: exact_plan.lease().clone(),
                    evidence_digest: expiry_digest(10),
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::ExpiredCommitted { .. })
        ));

        let aborted = prepared
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap();
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::DurableConflict {
                    plan: Box::new(exact_plan.clone()),
                    claim: aborted,
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::DurableConflict { .. })
        ));

        let foreign_plan = plan("10.0.0.2:7000");
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed {
                    claim: OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap(),
                }
            ),
            Ok(ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. })
        ));
        assert!(matches!(
            resolve_plan(
                &exact_plan,
                ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                    OwnerAdmissionInconsistencyCode::PlanSentinelMismatch,
                )
            ),
            Ok(ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch
            ))
        ));
    }
}
