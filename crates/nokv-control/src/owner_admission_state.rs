// Copyright 2024-2026 The NoKV Authors
// SPDX-License-Identifier: Apache-2.0

//! Backend-neutral state transitions for planned logical-shard ownership.
//!
//! This module does not perform I/O, allocate leases, read clocks, or infer
//! missing state. Backends capture one bounded exact snapshot, ask this module
//! for a closed mutation plan, and lower every expected value into one native
//! atomic compare-and-mutate operation.

use crate::store::{apply_recovery_publication, validate_logical_shard_record};
use crate::{
    LogicalShardId, LogicalShardLease, LogicalShardRecord, LogicalShardState,
    MetadataAuthorityRecord, OwnerAdmissionAbortReasonV1, OwnerAdmissionClaimPhaseV1,
    OwnerAdmissionClaimV1, OwnerAdmissionIntentV1, OwnerAdmissionPlanSentinelV1,
    OwnerAdmissionRejectionReasonV1, OwnerAdmissionTerminationReasonV1, OwnerIncarnationId,
    OwnerLeaseExpiryEvidenceDigest, OwnerSessionRenewalTargetV1, PlannedOwnerAdmissionV1,
    PlannedOwnerServingPublicationV1, RecoveryPublication, RootPlacement,
};

/// One bounded, same-revision view of every control value relevant to an
/// exact planned-owner incarnation.
///
/// `candidate_claim` is the permanent claim at the requested incarnation key.
/// `previous_claim` is absent for a fresh intent and is the exact last-installed
/// terminal claim for a successor intent. `active_plan` and `sentinel` are the
/// shard-singleton values, not values selected by digest alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionExactSnapshot {
    pub(crate) logical_shard_record: Option<LogicalShardRecord>,
    pub(crate) placement: Option<RootPlacement>,
    pub(crate) authority: Option<MetadataAuthorityRecord>,
    pub(crate) session: Option<LogicalShardLease>,
    pub(crate) session_absence_evidence: Option<AuthoritativeSessionAbsenceEvidence>,
    pub(crate) active_plan: Option<PlannedOwnerAdmissionV1>,
    pub(crate) sentinel: Option<OwnerAdmissionPlanSentinelV1>,
    pub(crate) sentinel_absence_evidence: Option<AuthoritativeSentinelAbsenceEvidence>,
    pub(crate) candidate_claim: Option<OwnerAdmissionClaimV1>,
    pub(crate) previous_claim: Option<OwnerAdmissionClaimV1>,
}

impl OwnerAdmissionExactSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        logical_shard_record: Option<LogicalShardRecord>,
        placement: Option<RootPlacement>,
        authority: Option<MetadataAuthorityRecord>,
        session: Option<LogicalShardLease>,
        session_absence_evidence: Option<AuthoritativeSessionAbsenceEvidence>,
        active_plan: Option<PlannedOwnerAdmissionV1>,
        sentinel: Option<OwnerAdmissionPlanSentinelV1>,
        sentinel_absence_evidence: Option<AuthoritativeSentinelAbsenceEvidence>,
        candidate_claim: Option<OwnerAdmissionClaimV1>,
        previous_claim: Option<OwnerAdmissionClaimV1>,
    ) -> Self {
        Self {
            logical_shard_record,
            placement,
            authority,
            session,
            session_absence_evidence,
            active_plan,
            sentinel,
            sentinel_absence_evidence,
            candidate_claim,
            previous_claim,
        }
    }
}

/// Exact binding produced only after a backend has authoritatively proved that
/// the lease-attached sentinel for this plan is absent.
///
/// The value deliberately contains the complete expected sentinel. It is not
/// an expiry clock or liveness capability; the backend remains responsible for
/// establishing authoritative absence immediately before applying the abort
/// transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthoritativeSentinelAbsenceEvidence {
    expected_sentinel: OwnerAdmissionPlanSentinelV1,
}

impl AuthoritativeSentinelAbsenceEvidence {
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn after_backend_check(plan: &PlannedOwnerAdmissionV1) -> Self {
        Self {
            expected_sentinel: OwnerAdmissionPlanSentinelV1::for_plan(plan),
        }
    }

    pub(crate) const fn expected_sentinel(&self) -> &OwnerAdmissionPlanSentinelV1 {
        &self.expected_sentinel
    }
}

/// Exact sentinel comparison that an abort backend must lower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionSentinelExpectation {
    Exact(OwnerAdmissionPlanSentinelV1),
    AuthoritativelyAbsent {
        expected: OwnerAdmissionPlanSentinelV1,
    },
}

/// Exact binding produced only after a backend has authoritatively proved that
/// one committed owner-session key is absent because its lease expired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthoritativeSessionAbsenceEvidence {
    expected_session: LogicalShardLease,
    evidence_digest: OwnerLeaseExpiryEvidenceDigest,
}

impl AuthoritativeSessionAbsenceEvidence {
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn after_backend_check(
        plan: &PlannedOwnerAdmissionV1,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    ) -> Self {
        Self {
            expected_session: plan.lease().clone(),
            evidence_digest,
        }
    }

    pub(crate) const fn expected_session(&self) -> &LogicalShardLease {
        &self.expected_session
    }

    pub(crate) const fn evidence_digest(&self) -> OwnerLeaseExpiryEvidenceDigest {
        self.evidence_digest
    }
}

/// Exact session comparison that a termination backend must lower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionSessionExpectation {
    Exact(LogicalShardLease),
    AuthoritativelyAbsent {
        expected: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
}

/// One complete, backend-neutral atomic mutation.
///
/// Every stored value used as a compare is carried in full. A backend must not
/// replace these comparisons with digest-only, epoch-only, or key-existence
/// checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionPrepareMutation {
    pub(crate) expected_shard: LogicalShardRecord,
    pub(crate) expected_placement: RootPlacement,
    pub(crate) expected_authority: MetadataAuthorityRecord,
    pub(crate) expected_session: Option<LogicalShardLease>,
    pub(crate) expected_active_plan: Option<PlannedOwnerAdmissionV1>,
    pub(crate) expected_sentinel: Option<OwnerAdmissionPlanSentinelV1>,
    pub(crate) expected_candidate_claim: Option<OwnerAdmissionClaimV1>,
    pub(crate) expected_previous_claim: Option<OwnerAdmissionClaimV1>,
    pub(crate) next_claim: OwnerAdmissionClaimV1,
    pub(crate) next_plan: PlannedOwnerAdmissionV1,
    pub(crate) next_sentinel: OwnerAdmissionPlanSentinelV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionRejectMutation {
    pub(crate) expected_snapshot: OwnerAdmissionExactSnapshot,
    pub(crate) next_claim: OwnerAdmissionClaimV1,
}

/// Intent-only terminal seal used after a prepare outcome becomes ambiguous.
/// The backend compares only the exact candidate claim key for absence before
/// installing `next_claim`; no plan or lease is invented by reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionSealRejectedMutation {
    pub(crate) logical_shard_id: LogicalShardId,
    pub(crate) owner_incarnation_id: OwnerIncarnationId,
    pub(crate) expected_candidate_claim: Option<OwnerAdmissionClaimV1>,
    pub(crate) next_claim: OwnerAdmissionClaimV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionCommitMutation {
    pub(crate) expected_shard: LogicalShardRecord,
    pub(crate) expected_placement: RootPlacement,
    pub(crate) expected_authority: MetadataAuthorityRecord,
    pub(crate) expected_session: Option<LogicalShardLease>,
    pub(crate) expected_claim: OwnerAdmissionClaimV1,
    pub(crate) expected_plan: PlannedOwnerAdmissionV1,
    pub(crate) expected_sentinel: OwnerAdmissionPlanSentinelV1,
    pub(crate) expected_previous_claim: Option<OwnerAdmissionClaimV1>,
    pub(crate) next_shard: LogicalShardRecord,
    pub(crate) next_session: LogicalShardLease,
    pub(crate) next_claim: OwnerAdmissionClaimV1,
    pub(crate) delete_plan: PlannedOwnerAdmissionV1,
    pub(crate) delete_sentinel: OwnerAdmissionPlanSentinelV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionAbortMutation {
    pub(crate) expected_shard: LogicalShardRecord,
    pub(crate) expected_session: Option<LogicalShardLease>,
    pub(crate) expected_claim: OwnerAdmissionClaimV1,
    pub(crate) expected_plan: PlannedOwnerAdmissionV1,
    pub(crate) expected_sentinel: OwnerAdmissionSentinelExpectation,
    pub(crate) next_claim: OwnerAdmissionClaimV1,
    pub(crate) delete_plan: PlannedOwnerAdmissionV1,
    pub(crate) delete_sentinel: OwnerAdmissionPlanSentinelV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerAdmissionTerminateMutation {
    pub(crate) expected_shard: LogicalShardRecord,
    pub(crate) expected_session: OwnerAdmissionSessionExpectation,
    pub(crate) expected_claim: OwnerAdmissionClaimV1,
    pub(crate) expected_active_plan: Option<PlannedOwnerAdmissionV1>,
    pub(crate) expected_sentinel: Option<OwnerAdmissionPlanSentinelV1>,
    pub(crate) next_shard: LogicalShardRecord,
    pub(crate) next_claim: OwnerAdmissionClaimV1,
    pub(crate) delete_session: Option<LogicalShardLease>,
}

/// Exact compare-and-set required to publish one planned Serving record.
///
/// The claim and session are comparisons, not writes. They ensure a backend
/// cannot publish after the committed owner session or permanent claim moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerServingPublicationMutation {
    pub(crate) expected_shard: LogicalShardRecord,
    pub(crate) expected_session: LogicalShardLease,
    pub(crate) expected_claim: OwnerAdmissionClaimV1,
    pub(crate) expected_active_plan: Option<PlannedOwnerAdmissionV1>,
    pub(crate) expected_sentinel: Option<OwnerAdmissionPlanSentinelV1>,
    pub(crate) next_shard: LogicalShardRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionMutationPlan {
    Prepare(Box<OwnerAdmissionPrepareMutation>),
    Reject(Box<OwnerAdmissionRejectMutation>),
    SealRejected(Box<OwnerAdmissionSealRejectedMutation>),
    Commit(Box<OwnerAdmissionCommitMutation>),
    Abort(Box<OwnerAdmissionAbortMutation>),
    Terminate(Box<OwnerAdmissionTerminateMutation>),
}

/// A legal transition that cannot proceed from the exact current state.
///
/// These codes describe closed precondition failures. They are distinct from
/// [`OwnerAdmissionInconsistencyCode`], which identifies a durable split that
/// a normal retry must never guess through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionTransitionBlockCode {
    IncarnationClaimExists,
    ClaimNotPrepared,
    ClaimNotCommitted,
    ExpectedShardChanged,
    PreviousClaimChanged,
    ServingAdmissionChanged,
    PreparedSentinelExpired,
    ExpiredSentinelRequiresLeaseLostAbort,
    LeaseExpiryEvidenceMismatch,
    LeaseExpiryRequiresAuthoritativeSessionAbsence,
    ExactSessionRequired,
    RecoveringSourceChanged,
}

/// Closed corruption/split-brain classifications for one exact snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerAdmissionInconsistencyCode {
    LogicalShardKeyMismatch,
    PlacementKeyMismatch,
    AuthorityKeyMismatch,
    SessionKeyMismatch,
    ActivePlanShardMismatch,
    SentinelShardMismatch,
    SentinelWithoutPlan,
    PlanSentinelMismatch,
    SentinelPresentWithAbsenceEvidence,
    SessionPresentWithAbsenceEvidence,
    CandidateClaimKeyMismatch,
    PlanWithoutPreparedClaim,
    PreparedClaimPlanMismatch,
    PreparedClaimRecordSplit,
    PreparedClaimSessionSplit,
    PreparedSentinelAbsenceUnproven,
    PreparedSentinelAbsenceEvidenceMismatch,
    CommittedClaimPlanSplit,
    CommittedClaimRecordSplit,
    CommittedRecordNotRecoveryDescendant,
    CommittedClaimSessionSplit,
    CommittedSessionAbsenceUnproven,
    CommittedSessionAbsenceEvidenceMismatch,
    TerminatedRecordNotRecoveryDescendant,
    SupersedingRecordInvalid,
    TerminalClaimHasActivePlan,
    TerminalClaimStillOwnsRecord,
    TerminalClaimStillHasSession,
    SameEpochDifferentInstalledIncarnation,
    TypedValueConstructionFailed,
}

/// Exact durable state of one requested owner incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionReconcileClassification {
    NotStarted,
    IncarnationAlreadyClaimed,
    Prepared {
        claim: OwnerAdmissionClaimV1,
        plan: PlannedOwnerAdmissionV1,
        sentinel: OwnerAdmissionPlanSentinelV1,
    },
    ExpiredPrepared {
        claim: OwnerAdmissionClaimV1,
        plan: PlannedOwnerAdmissionV1,
        expected_sentinel: OwnerAdmissionPlanSentinelV1,
    },
    Committed {
        claim: OwnerAdmissionClaimV1,
        record: LogicalShardRecord,
        session: LogicalShardLease,
    },
    ExpiredCommitted {
        claim: OwnerAdmissionClaimV1,
        record: LogicalShardRecord,
        expected_session: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    Rejected {
        claim: OwnerAdmissionClaimV1,
        reason: OwnerAdmissionRejectionReasonV1,
    },
    Aborted {
        claim: OwnerAdmissionClaimV1,
        reason: OwnerAdmissionAbortReasonV1,
    },
    Terminated {
        claim: OwnerAdmissionClaimV1,
        reason: OwnerAdmissionTerminationReasonV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionReconcileDecision {
    Classified(Box<OwnerAdmissionReconcileClassification>),
    Inconsistent(OwnerAdmissionInconsistencyCode),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerAdmissionStateDecision {
    Mutation(OwnerAdmissionMutationPlan),
    Reconciled(Box<OwnerAdmissionReconcileClassification>),
    Blocked(OwnerAdmissionTransitionBlockCode),
    Inconsistent(OwnerAdmissionInconsistencyCode),
}

/// Closed same-revision decision for one exact Recovering-to-Serving plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerServingPublicationStateDecision {
    Mutation(Box<OwnerServingPublicationMutation>),
    AlreadyPublished {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        session: LogicalShardLease,
    },
    PublicationConflict {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        session: LogicalShardLease,
    },
    ExpiredCommitted {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        expected_session: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    Terminated {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Superseded {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    DurableConflict {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Blocked(OwnerAdmissionTransitionBlockCode),
    Inconsistent(OwnerAdmissionInconsistencyCode),
}

/// Closed same-revision decision for one exact owner-session renewal target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerSessionRenewalStateDecision {
    Current {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        session: LogicalShardLease,
    },
    ExpiredCommitted {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
        expected_session: LogicalShardLease,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    },
    Terminated {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Superseded {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    DurableConflict {
        record: LogicalShardRecord,
        claim: OwnerAdmissionClaimV1,
    },
    Blocked(OwnerAdmissionTransitionBlockCode),
    Inconsistent(OwnerAdmissionInconsistencyCode),
}

/// Plan the first atomic installation of the permanent Prepared triple.
pub(crate) fn plan_owner_admission_prepare(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerAdmissionStateDecision {
    if let Err(code) = validate_admission_snapshot_bindings(requested, snapshot) {
        return OwnerAdmissionStateDecision::Inconsistent(code);
    }

    if snapshot.candidate_claim.is_some() {
        return match reconcile_owner_admission(requested, snapshot) {
            OwnerAdmissionReconcileDecision::Classified(classification) => {
                OwnerAdmissionStateDecision::Reconciled(classification)
            }
            OwnerAdmissionReconcileDecision::Inconsistent(code) => {
                OwnerAdmissionStateDecision::Inconsistent(code)
            }
        };
    }

    if let Some(active_plan) = snapshot.active_plan.as_ref() {
        if active_plan == requested {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PlanWithoutPreparedClaim,
            );
        }
        return prepare_rejection(
            requested,
            snapshot,
            OwnerAdmissionRejectionReasonV1::ActivePlanExists,
        );
    }

    let Some(record) = snapshot.logical_shard_record.as_ref() else {
        return prepare_rejection(
            requested,
            snapshot,
            OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
        );
    };
    if record != requested.intent().expected_unowned_shard() {
        return prepare_rejection(
            requested,
            snapshot,
            OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
        );
    }
    if snapshot.session.is_some() {
        return OwnerAdmissionStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::PreparedClaimSessionSplit,
        );
    }
    if snapshot.previous_claim.as_ref() != requested.intent().expected_previous_claim() {
        return prepare_rejection(
            requested,
            snapshot,
            OwnerAdmissionRejectionReasonV1::PreviousClaimChanged,
        );
    }
    if snapshot.placement.as_ref() != Some(requested.intent().admission().placement())
        || snapshot.authority.as_ref() != Some(requested.intent().admission().authority())
    {
        return prepare_rejection(
            requested,
            snapshot,
            OwnerAdmissionRejectionReasonV1::ServingAdmissionChanged,
        );
    }

    let next_claim = match OwnerAdmissionClaimV1::prepared(requested) {
        Ok(claim) => claim,
        Err(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            );
        }
    };
    let next_sentinel = OwnerAdmissionPlanSentinelV1::for_plan(requested);
    OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Prepare(Box::new(
        OwnerAdmissionPrepareMutation {
            expected_shard: record.clone(),
            expected_placement: requested.intent().admission().placement().clone(),
            expected_authority: requested.intent().admission().authority().clone(),
            expected_session: None,
            expected_active_plan: None,
            expected_sentinel: None,
            expected_candidate_claim: None,
            expected_previous_claim: snapshot.previous_claim.clone(),
            next_claim,
            next_plan: requested.clone(),
            next_sentinel,
        },
    )))
}

/// Plan the exact Prepared -> Committed transition.
pub(crate) fn plan_owner_admission_commit(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerAdmissionStateDecision {
    if let Err(code) = validate_admission_snapshot_bindings(requested, snapshot) {
        return OwnerAdmissionStateDecision::Inconsistent(code);
    }

    let Some(claim) = snapshot.candidate_claim.as_ref() else {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ClaimNotPrepared,
        );
    };
    if !claim_identity_matches_plan(claim, requested) {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::IncarnationClaimExists,
        );
    }
    if !claim_is_exact_phase(claim, requested, ExactClaimPhase::Prepared) {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ClaimNotPrepared,
        );
    }
    match snapshot.active_plan.as_ref() {
        Some(active) if active == requested => {}
        _ => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedClaimPlanMismatch,
            );
        }
    }
    let expected_sentinel = OwnerAdmissionPlanSentinelV1::for_plan(requested);
    match snapshot.sentinel.as_ref() {
        Some(actual) if actual == &expected_sentinel => {}
        None => {
            return OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::PreparedSentinelExpired,
            );
        }
        Some(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch,
            );
        }
    }

    let Some(record) = snapshot.logical_shard_record.as_ref() else {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ExpectedShardChanged,
        );
    };
    if record != requested.intent().expected_unowned_shard() {
        if record_is_exact_owner(record, requested) {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedClaimRecordSplit,
            );
        }
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ExpectedShardChanged,
        );
    }
    if snapshot.session.is_some() {
        return OwnerAdmissionStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::PreparedClaimSessionSplit,
        );
    }
    if snapshot.previous_claim.as_ref() != requested.intent().expected_previous_claim() {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::PreviousClaimChanged,
        );
    }
    if snapshot.placement.as_ref() != Some(requested.intent().admission().placement())
        || snapshot.authority.as_ref() != Some(requested.intent().admission().authority())
    {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ServingAdmissionChanged,
        );
    }

    let next_claim = match claim.clone().commit() {
        Ok(claim) => claim,
        Err(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            );
        }
    };
    let next_shard = expected_recovering_record_for_plan(requested);

    OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Commit(Box::new(
        OwnerAdmissionCommitMutation {
            expected_shard: record.clone(),
            expected_placement: requested.intent().admission().placement().clone(),
            expected_authority: requested.intent().admission().authority().clone(),
            expected_session: None,
            expected_claim: claim.clone(),
            expected_plan: requested.clone(),
            expected_sentinel: expected_sentinel.clone(),
            expected_previous_claim: snapshot.previous_claim.clone(),
            next_shard,
            next_session: requested.lease().clone(),
            next_claim,
            delete_plan: requested.clone(),
            delete_sentinel: expected_sentinel,
        },
    )))
}

/// Plan Prepared -> Aborted without consulting placement or authority state.
pub(crate) fn plan_owner_admission_abort(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    reason: OwnerAdmissionAbortReasonV1,
) -> OwnerAdmissionStateDecision {
    if let Err(code) = validate_shard_snapshot_bindings(requested, snapshot) {
        return OwnerAdmissionStateDecision::Inconsistent(code);
    }
    let Some(claim) = snapshot.candidate_claim.as_ref() else {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ClaimNotPrepared,
        );
    };
    if !claim_identity_matches_plan(claim, requested) {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::IncarnationClaimExists,
        );
    }
    if !claim_is_exact_phase(claim, requested, ExactClaimPhase::Prepared) {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ClaimNotPrepared,
        );
    }
    match snapshot.active_plan.as_ref() {
        Some(active) if active == requested => {}
        _ => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedClaimPlanMismatch,
            );
        }
    }
    let Some(record) = snapshot.logical_shard_record.as_ref() else {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ExpectedShardChanged,
        );
    };
    if record != requested.intent().expected_unowned_shard() {
        if record_is_exact_owner(record, requested) {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedClaimRecordSplit,
            );
        }
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ExpectedShardChanged,
        );
    }
    if snapshot.session.is_some() {
        return OwnerAdmissionStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::PreparedClaimSessionSplit,
        );
    }

    let delete_sentinel = OwnerAdmissionPlanSentinelV1::for_plan(requested);
    let expected_sentinel = match snapshot.sentinel.as_ref() {
        Some(actual) if actual == &delete_sentinel => {
            OwnerAdmissionSentinelExpectation::Exact(actual.clone())
        }
        Some(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch,
            );
        }
        None => {
            let Some(evidence) = snapshot.sentinel_absence_evidence.as_ref() else {
                return OwnerAdmissionStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven,
                );
            };
            if evidence.expected_sentinel() != &delete_sentinel {
                return OwnerAdmissionStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceEvidenceMismatch,
                );
            }
            if reason != OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit {
                return OwnerAdmissionStateDecision::Blocked(
                    OwnerAdmissionTransitionBlockCode::ExpiredSentinelRequiresLeaseLostAbort,
                );
            }
            OwnerAdmissionSentinelExpectation::AuthoritativelyAbsent {
                expected: delete_sentinel.clone(),
            }
        }
    };

    let next_claim = match claim.clone().abort(reason) {
        Ok(claim) => claim,
        Err(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            );
        }
    };
    OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Abort(Box::new(
        OwnerAdmissionAbortMutation {
            expected_shard: record.clone(),
            expected_session: None,
            expected_claim: claim.clone(),
            expected_plan: requested.clone(),
            expected_sentinel,
            next_claim,
            delete_plan: requested.clone(),
            delete_sentinel,
        },
    )))
}

/// Plan Committed -> Terminated without consulting placement, authority, or
/// migration state. Release must remain possible after those records drift.
pub(crate) fn plan_owner_admission_terminate(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    reason: OwnerAdmissionTerminationReasonV1,
) -> OwnerAdmissionStateDecision {
    if let Err(code) = validate_shard_snapshot_bindings(requested, snapshot) {
        return OwnerAdmissionStateDecision::Inconsistent(code);
    }
    let Some(claim) = snapshot.candidate_claim.as_ref() else {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ClaimNotCommitted,
        );
    };
    if !claim_identity_matches_plan(claim, requested) {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::IncarnationClaimExists,
        );
    }
    if matches!(claim.phase(), OwnerAdmissionClaimPhaseV1::Terminated { .. }) {
        return match reconcile_owner_admission(requested, snapshot) {
            OwnerAdmissionReconcileDecision::Classified(classification) => {
                OwnerAdmissionStateDecision::Reconciled(classification)
            }
            OwnerAdmissionReconcileDecision::Inconsistent(code) => {
                OwnerAdmissionStateDecision::Inconsistent(code)
            }
        };
    }
    if !claim_is_exact_phase(claim, requested, ExactClaimPhase::Committed) {
        return OwnerAdmissionStateDecision::Blocked(
            OwnerAdmissionTransitionBlockCode::ClaimNotCommitted,
        );
    }
    if snapshot.active_plan.is_some() || snapshot.sentinel.is_some() {
        return OwnerAdmissionStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
        );
    }
    let Some(record) = snapshot.logical_shard_record.as_ref() else {
        return OwnerAdmissionStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        );
    };
    if !record_is_exact_owner(record, requested) {
        return OwnerAdmissionStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        );
    }
    if let Err(code) = validate_committed_record_descendant(requested, record) {
        return OwnerAdmissionStateDecision::Inconsistent(code);
    }

    let (expected_session, delete_session) = match &reason {
        OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest } => {
            if let Some(session) = snapshot.session.as_ref() {
                if session != requested.lease() {
                    return OwnerAdmissionStateDecision::Inconsistent(
                        OwnerAdmissionInconsistencyCode::CommittedClaimSessionSplit,
                    );
                }
                return OwnerAdmissionStateDecision::Blocked(
                    OwnerAdmissionTransitionBlockCode::LeaseExpiryRequiresAuthoritativeSessionAbsence,
                );
            }
            let Some(evidence) = snapshot.session_absence_evidence.as_ref() else {
                return OwnerAdmissionStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
                );
            };
            if evidence.expected_session() != requested.lease() {
                return OwnerAdmissionStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceEvidenceMismatch,
                );
            }
            if evidence.evidence_digest() != *evidence_digest {
                return OwnerAdmissionStateDecision::Blocked(
                    OwnerAdmissionTransitionBlockCode::LeaseExpiryEvidenceMismatch,
                );
            }
            (
                OwnerAdmissionSessionExpectation::AuthoritativelyAbsent {
                    expected: requested.lease().clone(),
                    evidence_digest: *evidence_digest,
                },
                None,
            )
        }
        OwnerAdmissionTerminationReasonV1::Released
        | OwnerAdmissionTerminationReasonV1::AuthorityCutover { .. } => {
            let Some(session) = snapshot.session.as_ref() else {
                let Some(evidence) = snapshot.session_absence_evidence.as_ref() else {
                    return OwnerAdmissionStateDecision::Inconsistent(
                        OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
                    );
                };
                if evidence.expected_session() != requested.lease() {
                    return OwnerAdmissionStateDecision::Inconsistent(
                        OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceEvidenceMismatch,
                    );
                }
                return OwnerAdmissionStateDecision::Blocked(
                    OwnerAdmissionTransitionBlockCode::ExactSessionRequired,
                );
            };
            if session != requested.lease() {
                return OwnerAdmissionStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimSessionSplit,
                );
            }
            (
                OwnerAdmissionSessionExpectation::Exact(session.clone()),
                Some(session.clone()),
            )
        }
    };

    let next_claim = match claim.clone().terminate(reason) {
        Ok(claim) => claim,
        Err(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            );
        }
    };
    let mut next_shard = record.clone();
    next_shard.owner = None;
    next_shard.lease_id = 0;
    next_shard.state = LogicalShardState::Unassigned;
    next_shard.endpoint = None;
    if let Err(code) = validate_terminated_record_descendant(requested, &next_shard) {
        return OwnerAdmissionStateDecision::Inconsistent(code);
    }

    OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(Box::new(
        OwnerAdmissionTerminateMutation {
            expected_shard: record.clone(),
            expected_session,
            expected_claim: claim.clone(),
            expected_active_plan: None,
            expected_sentinel: None,
            next_shard,
            next_claim,
            delete_session,
        },
    )))
}

/// Plan or classify one exact Recovering-to-Serving publication from a single
/// bounded snapshot. No durable gap is filled and no liveness is inferred.
pub(crate) fn plan_owner_serving_publication(
    requested: &PlannedOwnerServingPublicationV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerServingPublicationStateDecision {
    if requested.validate().is_err() {
        return OwnerServingPublicationStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        );
    }
    let plan = requested.plan();
    let decision = reconcile_owner_admission(plan, snapshot);
    let classification = match decision {
        OwnerAdmissionReconcileDecision::Classified(classification) => classification,
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            return OwnerServingPublicationStateDecision::Inconsistent(code);
        }
    };

    match *classification {
        OwnerAdmissionReconcileClassification::Committed {
            claim,
            record,
            session,
        } => {
            if record == *requested.source() {
                return OwnerServingPublicationStateDecision::Mutation(Box::new(
                    OwnerServingPublicationMutation {
                        expected_shard: record,
                        expected_session: session,
                        expected_claim: claim,
                        expected_active_plan: snapshot.active_plan.clone(),
                        expected_sentinel: snapshot.sentinel.clone(),
                        next_shard: requested.target().clone(),
                    },
                ));
            }
            if record == *requested.target() {
                return OwnerServingPublicationStateDecision::AlreadyPublished {
                    record,
                    claim,
                    session,
                };
            }
            if record.state == LogicalShardState::Serving {
                return OwnerServingPublicationStateDecision::PublicationConflict {
                    record,
                    claim,
                    session,
                };
            }
            OwnerServingPublicationStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::RecoveringSourceChanged,
            )
        }
        OwnerAdmissionReconcileClassification::ExpiredCommitted {
            claim,
            record,
            expected_session,
            evidence_digest,
        } => OwnerServingPublicationStateDecision::ExpiredCommitted {
            record,
            claim,
            expected_session,
            evidence_digest,
        },
        OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            match classify_terminal_record(plan, snapshot) {
                Ok(OwnerAdmissionTerminalRecordClassification::Terminated(record)) => {
                    OwnerServingPublicationStateDecision::Terminated { record, claim }
                }
                Ok(OwnerAdmissionTerminalRecordClassification::Superseded(record)) => {
                    OwnerServingPublicationStateDecision::Superseded { record, claim }
                }
                Err(code) => OwnerServingPublicationStateDecision::Inconsistent(code),
            }
        }
        OwnerAdmissionReconcileClassification::Aborted { claim, .. } => {
            match exact_conflict_record(plan, snapshot) {
                Ok(record) => {
                    OwnerServingPublicationStateDecision::DurableConflict { record, claim }
                }
                Err(code) => OwnerServingPublicationStateDecision::Inconsistent(code),
            }
        }
        OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => {
            OwnerServingPublicationStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::IncarnationClaimExists,
            )
        }
        OwnerAdmissionReconcileClassification::NotStarted
        | OwnerAdmissionReconcileClassification::Prepared { .. }
        | OwnerAdmissionReconcileClassification::ExpiredPrepared { .. }
        | OwnerAdmissionReconcileClassification::Rejected { .. } => {
            OwnerServingPublicationStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::ClaimNotCommitted,
            )
        }
    }
}

/// Classify one exact renewal target from a single bounded snapshot.
///
/// A backend may perform keepalive and TTL observation only for `Current`.
/// This function itself performs no keepalive and creates no lifetime permit.
pub(crate) fn classify_owner_session_renewal(
    target: &OwnerSessionRenewalTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerSessionRenewalStateDecision {
    if target.validate().is_err() {
        return OwnerSessionRenewalStateDecision::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        );
    }
    let plan = target.plan();
    let decision = reconcile_owner_admission(plan, snapshot);
    let classification = match decision {
        OwnerAdmissionReconcileDecision::Classified(classification) => classification,
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            return OwnerSessionRenewalStateDecision::Inconsistent(code);
        }
    };

    match *classification {
        OwnerAdmissionReconcileClassification::Committed {
            claim,
            record,
            session,
        } => {
            if claim != *target.claim() {
                return OwnerSessionRenewalStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
                );
            }
            if session != *target.session() {
                return OwnerSessionRenewalStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimSessionSplit,
                );
            }
            OwnerSessionRenewalStateDecision::Current {
                record,
                claim,
                session,
            }
        }
        OwnerAdmissionReconcileClassification::ExpiredCommitted {
            claim,
            record,
            expected_session,
            evidence_digest,
        } => {
            if claim != *target.claim() {
                return OwnerSessionRenewalStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
                );
            }
            if expected_session != *target.session() {
                return OwnerSessionRenewalStateDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceEvidenceMismatch,
                );
            }
            OwnerSessionRenewalStateDecision::ExpiredCommitted {
                record,
                claim,
                expected_session,
                evidence_digest,
            }
        }
        OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            match classify_terminal_record(plan, snapshot) {
                Ok(OwnerAdmissionTerminalRecordClassification::Terminated(record)) => {
                    OwnerSessionRenewalStateDecision::Terminated { record, claim }
                }
                Ok(OwnerAdmissionTerminalRecordClassification::Superseded(record)) => {
                    OwnerSessionRenewalStateDecision::Superseded { record, claim }
                }
                Err(code) => OwnerSessionRenewalStateDecision::Inconsistent(code),
            }
        }
        OwnerAdmissionReconcileClassification::Aborted { claim, .. } => {
            match exact_conflict_record(plan, snapshot) {
                Ok(record) => OwnerSessionRenewalStateDecision::DurableConflict { record, claim },
                Err(code) => OwnerSessionRenewalStateDecision::Inconsistent(code),
            }
        }
        OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => {
            OwnerSessionRenewalStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::IncarnationClaimExists,
            )
        }
        OwnerAdmissionReconcileClassification::NotStarted
        | OwnerAdmissionReconcileClassification::Prepared { .. }
        | OwnerAdmissionReconcileClassification::ExpiredPrepared { .. }
        | OwnerAdmissionReconcileClassification::Rejected { .. } => {
            OwnerSessionRenewalStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::ClaimNotCommitted,
            )
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OwnerAdmissionTerminalRecordClassification {
    Terminated(LogicalShardRecord),
    Superseded(LogicalShardRecord),
}

fn classify_terminal_record(
    plan: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<OwnerAdmissionTerminalRecordClassification, OwnerAdmissionInconsistencyCode> {
    let Some(record) = snapshot.logical_shard_record.as_ref() else {
        return Err(OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant);
    };
    if record
        .owner_epoch
        .is_some_and(|epoch| epoch.get() > plan.lease().owner_epoch.get())
    {
        if record.logical_shard_id != plan.intent().logical_shard_id()
            || validate_logical_shard_record(record).is_err()
        {
            return Err(OwnerAdmissionInconsistencyCode::SupersedingRecordInvalid);
        }
        return Ok(OwnerAdmissionTerminalRecordClassification::Superseded(
            record.clone(),
        ));
    }
    validate_terminated_record_descendant(plan, record)?;
    Ok(OwnerAdmissionTerminalRecordClassification::Terminated(
        record.clone(),
    ))
}

fn exact_conflict_record(
    plan: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<LogicalShardRecord, OwnerAdmissionInconsistencyCode> {
    let Some(record) = snapshot.logical_shard_record.as_ref() else {
        return Err(OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit);
    };
    if record.logical_shard_id != plan.intent().logical_shard_id()
        || validate_logical_shard_record(record).is_err()
    {
        return Err(OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit);
    }
    Ok(record.clone())
}

/// Classify one exact durable incarnation without mutating or filling gaps.
pub(crate) fn reconcile_owner_admission(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerAdmissionReconcileDecision {
    if let Err(code) = validate_shard_snapshot_bindings(requested, snapshot) {
        return OwnerAdmissionReconcileDecision::Inconsistent(code);
    }
    let expected_sentinel = OwnerAdmissionPlanSentinelV1::for_plan(requested);
    let Some(claim) = snapshot.candidate_claim.as_ref() else {
        if snapshot.active_plan.as_ref() == Some(requested)
            || snapshot.sentinel.as_ref() == Some(&expected_sentinel)
        {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PlanWithoutPreparedClaim,
            );
        }
        if snapshot
            .logical_shard_record
            .as_ref()
            .is_some_and(|record| record_is_exact_owner(record, requested))
        {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
            );
        }
        if snapshot.session.as_ref() == Some(requested.lease()) {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedClaimSessionSplit,
            );
        }
        return OwnerAdmissionReconcileDecision::Classified(Box::new(
            OwnerAdmissionReconcileClassification::NotStarted,
        ));
    };
    if !claim_identity_matches_plan(claim, requested) {
        return OwnerAdmissionReconcileDecision::Classified(Box::new(
            OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed,
        ));
    }

    match claim.phase() {
        OwnerAdmissionClaimPhaseV1::Prepared { .. } => {
            if !claim_is_exact_phase(claim, requested, ExactClaimPhase::Prepared) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedClaimPlanMismatch,
                );
            }
            if snapshot.active_plan.as_ref() != Some(requested) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedClaimPlanMismatch,
                );
            }
            if snapshot.logical_shard_record.as_ref()
                != Some(requested.intent().expected_unowned_shard())
            {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedClaimRecordSplit,
                );
            }
            if snapshot.session.is_some() {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedClaimSessionSplit,
                );
            }
            match snapshot.sentinel.as_ref() {
                Some(sentinel) if sentinel == &expected_sentinel => {
                    OwnerAdmissionReconcileDecision::Classified(Box::new(
                        OwnerAdmissionReconcileClassification::Prepared {
                            claim: claim.clone(),
                            plan: requested.clone(),
                            sentinel: sentinel.clone(),
                        },
                    ))
                }
                None => {
                    let Some(evidence) = snapshot.sentinel_absence_evidence.as_ref() else {
                        return OwnerAdmissionReconcileDecision::Inconsistent(
                            OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven,
                        );
                    };
                    if evidence.expected_sentinel() != &expected_sentinel {
                        return OwnerAdmissionReconcileDecision::Inconsistent(
                            OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceEvidenceMismatch,
                        );
                    }
                    OwnerAdmissionReconcileDecision::Classified(Box::new(
                        OwnerAdmissionReconcileClassification::ExpiredPrepared {
                            claim: claim.clone(),
                            plan: requested.clone(),
                            expected_sentinel,
                        },
                    ))
                }
                Some(_) => OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PlanSentinelMismatch,
                ),
            }
        }
        OwnerAdmissionClaimPhaseV1::Committed { .. } => {
            if !claim_is_exact_phase(claim, requested, ExactClaimPhase::Committed) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
                );
            }
            if snapshot.active_plan.is_some() || snapshot.sentinel.is_some() {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
                );
            }
            let Some(record) = snapshot.logical_shard_record.as_ref() else {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
                );
            };
            if !record_is_exact_owner(record, requested) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
                );
            }
            if let Err(code) = validate_committed_record_descendant(requested, record) {
                return OwnerAdmissionReconcileDecision::Inconsistent(code);
            }
            match snapshot.session.as_ref() {
                Some(session) if session == requested.lease() => {
                    OwnerAdmissionReconcileDecision::Classified(Box::new(
                        OwnerAdmissionReconcileClassification::Committed {
                            claim: claim.clone(),
                            record: record.clone(),
                            session: session.clone(),
                        },
                    ))
                }
                None => {
                    let Some(evidence) = snapshot.session_absence_evidence.as_ref() else {
                        return OwnerAdmissionReconcileDecision::Inconsistent(
                            OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
                        );
                    };
                    if evidence.expected_session() != requested.lease() {
                        return OwnerAdmissionReconcileDecision::Inconsistent(
                            OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceEvidenceMismatch,
                        );
                    }
                    OwnerAdmissionReconcileDecision::Classified(Box::new(
                        OwnerAdmissionReconcileClassification::ExpiredCommitted {
                            claim: claim.clone(),
                            record: record.clone(),
                            expected_session: requested.lease().clone(),
                            evidence_digest: evidence.evidence_digest(),
                        },
                    ))
                }
                Some(_) => OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimSessionSplit,
                ),
            }
        }
        OwnerAdmissionClaimPhaseV1::Rejected { reason } => {
            if exact_plan_artifact_is_active(requested, snapshot) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimHasActivePlan,
                );
            }
            if candidate_still_owns_record_or_session(requested, snapshot) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimStillOwnsRecord,
                );
            }
            OwnerAdmissionReconcileDecision::Classified(Box::new(
                OwnerAdmissionReconcileClassification::Rejected {
                    claim: claim.clone(),
                    reason: *reason,
                },
            ))
        }
        OwnerAdmissionClaimPhaseV1::Aborted {
            lease,
            plan_digest,
            reason,
        } => {
            if lease != requested.lease() || *plan_digest != requested.digest() {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::PreparedClaimPlanMismatch,
                );
            }
            if exact_plan_artifact_is_active(requested, snapshot) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimHasActivePlan,
                );
            }
            if candidate_still_owns_record_or_session(requested, snapshot) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimStillOwnsRecord,
                );
            }
            OwnerAdmissionReconcileDecision::Classified(Box::new(
                OwnerAdmissionReconcileClassification::Aborted {
                    claim: claim.clone(),
                    reason: *reason,
                },
            ))
        }
        OwnerAdmissionClaimPhaseV1::Terminated {
            lease,
            plan_digest,
            reason,
        } => {
            if lease != requested.lease() || *plan_digest != requested.digest() {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
                );
            }
            if exact_plan_artifact_is_active(requested, snapshot) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimHasActivePlan,
                );
            }
            if candidate_still_owns_record_or_session(requested, snapshot) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimStillOwnsRecord,
                );
            }
            if snapshot
                .logical_shard_record
                .as_ref()
                .is_some_and(|record| {
                    record.owner_epoch == Some(requested.lease().owner_epoch)
                        && record.owner_incarnation_id
                            != Some(requested.lease().owner_incarnation_id)
                })
            {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::SameEpochDifferentInstalledIncarnation,
                );
            }
            if snapshot.session.as_ref().is_some_and(|session| {
                session.owner_incarnation_id == requested.lease().owner_incarnation_id
            }) {
                return OwnerAdmissionReconcileDecision::Inconsistent(
                    OwnerAdmissionInconsistencyCode::TerminalClaimStillHasSession,
                );
            }
            OwnerAdmissionReconcileDecision::Classified(Box::new(
                OwnerAdmissionReconcileClassification::Terminated {
                    claim: claim.clone(),
                    reason: reason.clone(),
                },
            ))
        }
    }
}

/// Reconcile an ambiguous prepare using only its durable intent.
///
/// Claims that carry a lease reconstruct the one canonical plan and verify its
/// stored plan digest before delegating to the plan-level classifier. This is
/// the only supported way to recover a plan after response loss; callers must
/// never invent a lease or accept a different intent at the same claim key.
pub(crate) fn reconcile_owner_admission_intent(
    requested: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerAdmissionReconcileDecision {
    if let Err(code) = validate_intent_snapshot_bindings(requested, snapshot) {
        return OwnerAdmissionReconcileDecision::Inconsistent(code);
    }
    let Some(claim) = snapshot.candidate_claim.as_ref() else {
        if same_candidate_plan_artifact_is_active(requested, snapshot) {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PlanWithoutPreparedClaim,
            );
        }
        if intent_candidate_still_owns_record_or_session(requested, snapshot) {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
            );
        }
        return OwnerAdmissionReconcileDecision::Classified(Box::new(
            OwnerAdmissionReconcileClassification::NotStarted,
        ));
    };
    if !claim_identity_matches_intent(claim, requested) {
        return OwnerAdmissionReconcileDecision::Classified(Box::new(
            OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed,
        ));
    }

    if let OwnerAdmissionClaimPhaseV1::Rejected { reason } = claim.phase() {
        if same_candidate_plan_artifact_is_active(requested, snapshot) {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TerminalClaimHasActivePlan,
            );
        }
        if intent_candidate_still_owns_record_or_session(requested, snapshot) {
            return OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TerminalClaimStillOwnsRecord,
            );
        }
        return OwnerAdmissionReconcileDecision::Classified(Box::new(
            OwnerAdmissionReconcileClassification::Rejected {
                claim: claim.clone(),
                reason: *reason,
            },
        ));
    }

    let plan = match reconstruct_plan_from_claim(requested, claim) {
        Ok(plan) => plan,
        Err(code) => return OwnerAdmissionReconcileDecision::Inconsistent(code),
    };
    reconcile_owner_admission(&plan, snapshot)
}

/// Seal one ambiguous prepare by permanently rejecting its exact candidate
/// claim key when that key is still absent.
///
/// The returned mutation intentionally compares no shard, placement,
/// authority, session, plan, or sentinel value. The prepare transaction and
/// this seal transaction both compare the same candidate claim key for
/// absence, so at most one can commit. A lost seal race must be followed by a
/// fresh same-revision intent reconciliation.
pub(crate) fn plan_owner_admission_intent_seal_rejected(
    requested: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> OwnerAdmissionStateDecision {
    match reconcile_owner_admission_intent(requested, snapshot) {
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            return OwnerAdmissionStateDecision::Inconsistent(code);
        }
        OwnerAdmissionReconcileDecision::Classified(classification)
            if !matches!(
                classification.as_ref(),
                OwnerAdmissionReconcileClassification::NotStarted
            ) =>
        {
            return OwnerAdmissionStateDecision::Reconciled(classification);
        }
        OwnerAdmissionReconcileDecision::Classified(_) => {}
    }

    let next_claim = match OwnerAdmissionClaimV1::rejected_from_absent(
        requested,
        OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
    ) {
        Ok(claim) => claim,
        Err(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            );
        }
    };
    OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::SealRejected(Box::new(
        OwnerAdmissionSealRejectedMutation {
            logical_shard_id: requested.logical_shard_id(),
            owner_incarnation_id: requested.owner_incarnation_id(),
            expected_candidate_claim: None,
            next_claim,
        },
    )))
}

fn reconstruct_plan_from_claim(
    intent: &OwnerAdmissionIntentV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<PlannedOwnerAdmissionV1, OwnerAdmissionInconsistencyCode> {
    let (lease, stored_plan_digest, mismatch_code) = match claim.phase() {
        OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Aborted {
            lease, plan_digest, ..
        } => (
            lease,
            *plan_digest,
            OwnerAdmissionInconsistencyCode::PreparedClaimPlanMismatch,
        ),
        OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Terminated {
            lease, plan_digest, ..
        } => (
            lease,
            *plan_digest,
            OwnerAdmissionInconsistencyCode::CommittedClaimPlanSplit,
        ),
        OwnerAdmissionClaimPhaseV1::Rejected { .. } => {
            return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
        }
    };
    let plan =
        PlannedOwnerAdmissionV1::new(intent.clone(), lease.clone()).map_err(|_| mismatch_code)?;
    if plan.digest() != stored_plan_digest {
        return Err(mismatch_code);
    }
    Ok(plan)
}

fn same_candidate_plan_artifact_is_active(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> bool {
    snapshot.active_plan.as_ref().is_some_and(|plan| {
        plan.intent().logical_shard_id() == intent.logical_shard_id()
            && plan.intent().owner_incarnation_id() == intent.owner_incarnation_id()
    }) || snapshot.sentinel.as_ref().is_some_and(|sentinel| {
        sentinel.logical_shard_id() == intent.logical_shard_id()
            && sentinel.owner_incarnation_id() == intent.owner_incarnation_id()
    })
}

fn intent_candidate_still_owns_record_or_session(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> bool {
    snapshot
        .logical_shard_record
        .as_ref()
        .is_some_and(|record| {
            record.logical_shard_id == intent.logical_shard_id()
                && record.owner_incarnation_id == Some(intent.owner_incarnation_id())
        })
        || snapshot.session.as_ref().is_some_and(|session| {
            session.logical_shard_id == intent.logical_shard_id()
                && session.owner_incarnation_id == intent.owner_incarnation_id()
        })
}

fn prepare_rejection(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    reason: OwnerAdmissionRejectionReasonV1,
) -> OwnerAdmissionStateDecision {
    let next_claim = match OwnerAdmissionClaimV1::rejected_from_absent(requested.intent(), reason) {
        Ok(claim) => claim,
        Err(_) => {
            return OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            );
        }
    };
    OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Reject(Box::new(
        OwnerAdmissionRejectMutation {
            expected_snapshot: snapshot.clone(),
            next_claim,
        },
    )))
}

fn validate_admission_snapshot_bindings(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    validate_shard_snapshot_bindings(requested, snapshot)?;
    let shard_id = requested.intent().logical_shard_id();
    if snapshot.placement.as_ref().is_some_and(|placement| {
        placement.root_id != requested.intent().admission().placement().root_id
            || placement.logical_shard_id != shard_id
    }) {
        return Err(OwnerAdmissionInconsistencyCode::PlacementKeyMismatch);
    }
    if snapshot
        .authority
        .as_ref()
        .is_some_and(|authority| authority.logical_shard_id != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::AuthorityKeyMismatch);
    }
    Ok(())
}

fn validate_shard_snapshot_bindings(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    validate_snapshot_key_bindings(
        requested.intent().logical_shard_id(),
        requested.intent().owner_incarnation_id(),
        snapshot,
    )
}

fn validate_intent_snapshot_bindings(
    requested: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    validate_snapshot_key_bindings(
        requested.logical_shard_id(),
        requested.owner_incarnation_id(),
        snapshot,
    )
}

fn validate_snapshot_key_bindings(
    shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    if snapshot
        .logical_shard_record
        .as_ref()
        .is_some_and(|record| record.logical_shard_id != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::LogicalShardKeyMismatch);
    }
    if snapshot
        .session
        .as_ref()
        .is_some_and(|session| session.logical_shard_id != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::SessionKeyMismatch);
    }
    if snapshot.session.is_some() && snapshot.session_absence_evidence.is_some() {
        return Err(OwnerAdmissionInconsistencyCode::SessionPresentWithAbsenceEvidence);
    }
    if snapshot
        .session_absence_evidence
        .as_ref()
        .is_some_and(|evidence| evidence.expected_session().logical_shard_id != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::SessionKeyMismatch);
    }
    if snapshot
        .active_plan
        .as_ref()
        .is_some_and(|plan| plan.intent().logical_shard_id() != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::ActivePlanShardMismatch);
    }
    if snapshot
        .sentinel
        .as_ref()
        .is_some_and(|sentinel| sentinel.logical_shard_id() != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::SentinelShardMismatch);
    }
    if snapshot.sentinel.is_some() && snapshot.sentinel_absence_evidence.is_some() {
        return Err(OwnerAdmissionInconsistencyCode::SentinelPresentWithAbsenceEvidence);
    }
    if snapshot
        .sentinel_absence_evidence
        .as_ref()
        .is_some_and(|evidence| evidence.expected_sentinel().logical_shard_id() != shard_id)
    {
        return Err(OwnerAdmissionInconsistencyCode::SentinelShardMismatch);
    }
    match (snapshot.active_plan.as_ref(), snapshot.sentinel.as_ref()) {
        (None, Some(_)) => {
            return Err(OwnerAdmissionInconsistencyCode::SentinelWithoutPlan);
        }
        (Some(plan), Some(sentinel)) if sentinel.validate_plan(plan).is_err() => {
            return Err(OwnerAdmissionInconsistencyCode::PlanSentinelMismatch);
        }
        _ => {}
    }
    if snapshot.candidate_claim.as_ref().is_some_and(|claim| {
        claim.identity().logical_shard_id() != shard_id
            || claim.identity().owner_incarnation_id() != owner_incarnation_id
    }) {
        return Err(OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch);
    }
    Ok(())
}

fn claim_key_matches_intent(
    claim: &OwnerAdmissionClaimV1,
    intent: &OwnerAdmissionIntentV1,
) -> bool {
    let identity = claim.identity();
    identity.logical_shard_id() == intent.logical_shard_id()
        && identity.owner_incarnation_id() == intent.owner_incarnation_id()
}

fn claim_identity_matches_plan(
    claim: &OwnerAdmissionClaimV1,
    plan: &PlannedOwnerAdmissionV1,
) -> bool {
    claim_identity_matches_intent(claim, plan.intent())
}

fn claim_identity_matches_intent(
    claim: &OwnerAdmissionClaimV1,
    intent: &OwnerAdmissionIntentV1,
) -> bool {
    let identity = claim.identity();
    claim_key_matches_intent(claim, intent)
        && identity.intent_digest() == intent.digest()
        && identity.reservation_digest() == intent.reservation_digest()
        && identity.planned_epoch() == intent.planned_epoch()
}

#[derive(Clone, Copy)]
enum ExactClaimPhase {
    Prepared,
    Committed,
}

fn claim_is_exact_phase(
    claim: &OwnerAdmissionClaimV1,
    plan: &PlannedOwnerAdmissionV1,
    expected_phase: ExactClaimPhase,
) -> bool {
    if !claim_identity_matches_plan(claim, plan) {
        return false;
    }
    match (expected_phase, claim.phase()) {
        (
            ExactClaimPhase::Prepared,
            OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest },
        )
        | (
            ExactClaimPhase::Committed,
            OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest },
        ) => lease == plan.lease() && *plan_digest == plan.digest(),
        _ => false,
    }
}

pub(crate) fn expected_recovering_record_for_plan(
    plan: &PlannedOwnerAdmissionV1,
) -> LogicalShardRecord {
    let mut record = plan.intent().expected_unowned_shard().clone();
    record.owner = Some(plan.intent().owner().clone());
    record.owner_epoch = Some(plan.intent().planned_epoch());
    record.owner_incarnation_id = Some(plan.intent().owner_incarnation_id());
    record.lease_id = plan.lease().lease_id;
    record.state = LogicalShardState::Recovering;
    record.endpoint = Some(plan.intent().endpoint().to_owned());
    record
}

/// Prove that a current committed record is a legal monotonic recovery
/// descendant of the exact unowned record bound into its plan.
pub(crate) fn validate_committed_record_descendant(
    plan: &PlannedOwnerAdmissionV1,
    record: &LogicalShardRecord,
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    if validate_logical_shard_record(record).is_err()
        || !matches!(
            record.state,
            LogicalShardState::Recovering | LogicalShardState::Serving
        )
    {
        return Err(OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant);
    }

    let mut expected = expected_recovering_record_for_plan(plan);
    let publication = RecoveryPublication {
        checkpoint: record.checkpoint.clone(),
        log: record.log.clone(),
        durable_lsn: record.durable_lsn,
    };
    if apply_recovery_publication(&mut expected, publication).is_err() {
        return Err(OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant);
    }
    expected.state = record.state;
    if expected != *record {
        return Err(OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant);
    }
    Ok(())
}

/// Prove that a terminal unowned record preserves one plan's complete,
/// monotonic recovery lineage and its last-installed identity.
pub(crate) fn validate_terminated_record_descendant(
    plan: &PlannedOwnerAdmissionV1,
    record: &LogicalShardRecord,
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    if validate_logical_shard_record(record).is_err()
        || record.owner.is_some()
        || record.owner_epoch != Some(plan.intent().planned_epoch())
        || record.owner_incarnation_id != Some(plan.intent().owner_incarnation_id())
        || record.lease_id != 0
        || record.state != LogicalShardState::Unassigned
        || record.endpoint.is_some()
    {
        return Err(OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant);
    }

    let mut expected = plan.intent().expected_unowned_shard().clone();
    let publication = RecoveryPublication {
        checkpoint: record.checkpoint.clone(),
        log: record.log.clone(),
        durable_lsn: record.durable_lsn,
    };
    if apply_recovery_publication(&mut expected, publication).is_err() {
        return Err(OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant);
    }
    expected.owner_epoch = Some(plan.intent().planned_epoch());
    expected.owner_incarnation_id = Some(plan.intent().owner_incarnation_id());
    if expected != *record {
        return Err(OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant);
    }
    Ok(())
}

fn record_is_exact_owner(record: &LogicalShardRecord, plan: &PlannedOwnerAdmissionV1) -> bool {
    record.logical_shard_id == plan.lease().logical_shard_id
        && record.owner.as_ref() == Some(&plan.lease().owner)
        && record.owner_epoch == Some(plan.lease().owner_epoch)
        && record.owner_incarnation_id == Some(plan.lease().owner_incarnation_id)
        && record.lease_id == plan.lease().lease_id
        && record.endpoint.as_deref() == Some(plan.intent().endpoint())
        && record.state != LogicalShardState::Unassigned
}

fn exact_plan_artifact_is_active(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> bool {
    snapshot.active_plan.as_ref() == Some(requested)
        || snapshot.sentinel.as_ref() == Some(&OwnerAdmissionPlanSentinelV1::for_plan(requested))
}

fn candidate_still_owns_record_or_session(
    requested: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> bool {
    snapshot
        .logical_shard_record
        .as_ref()
        .is_some_and(|record| record_is_exact_owner(record, requested))
        || snapshot.session.as_ref() == Some(requested.lease())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CheckpointRef, ConsistencyDomainId, MetadataAuthorityBinding, MetadataAuthorityGeneration,
        MetadataAuthorityId, MetadataAuthorityRevision, MetadataContractDigest,
        MetadataProviderProfileId, NodeId, OwnerAdmissionIntentV1, OwnerEpoch, OwnerIncarnationId,
        OwnerRuntimeReservationDigest, OwnerSessionRenewalTargetV1, PlacementGeneration,
        PlannedOwnerServingPublicationV1, RootId, RootLayoutGeneration, RootLayoutProfile,
        RootPartitionId, RootPlacementLifecycle,
    };

    fn root_id(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard_id(value: u8) -> crate::LogicalShardId {
        crate::LogicalShardId::from_bytes([value; 16])
    }

    fn incarnation(value: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([value; 16])
    }

    fn active_placement() -> RootPlacement {
        RootPlacement {
            root_id: root_id(1),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: shard_id(1),
            placement_generation: PlacementGeneration::new(2).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
        }
    }

    fn authority() -> MetadataAuthorityRecord {
        MetadataAuthorityRecord {
            logical_shard_id: shard_id(1),
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
            active: MetadataAuthorityBinding {
                authority_id: MetadataAuthorityId::from_bytes([2; 16]),
                provider_profile_id: MetadataProviderProfileId::new("holt-primary").unwrap(),
                profile_fingerprint: [3; 32],
                consistency_domain_id: ConsistencyDomainId::from_bytes([4; 16]),
                contract_digest: MetadataContractDigest::from_bytes([5; 32]),
            },
            migration: None,
        }
    }

    fn fresh_plan(incarnation_value: u8, lease_id: u64) -> PlannedOwnerAdmissionV1 {
        fresh_plan_with_reservation(incarnation_value, lease_id, incarnation_value)
    }

    fn fresh_plan_with_reservation(
        incarnation_value: u8,
        lease_id: u64,
        reservation_value: u8,
    ) -> PlannedOwnerAdmissionV1 {
        let admission =
            crate::OwnerServingAdmission::stable(active_placement(), authority()).unwrap();
        let intent = OwnerAdmissionIntentV1::fresh(
            admission.clone(),
            LogicalShardRecord::unassigned(shard_id(1)),
            NodeId::new("node-a").unwrap(),
            incarnation(incarnation_value),
            "node-a:7000".to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([reservation_value; 32]).unwrap(),
        )
        .unwrap();
        let lease = LogicalShardLease {
            logical_shard_id: shard_id(1),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            owner_incarnation_id: incarnation(incarnation_value),
            lease_id,
            authority: admission.authority().fence(),
        };
        PlannedOwnerAdmissionV1::new(intent, lease).unwrap()
    }

    fn released_predecessor() -> (LogicalShardRecord, OwnerAdmissionClaimV1) {
        let plan = fresh_plan(1, 11);
        let claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .commit()
            .unwrap()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        let mut record = LogicalShardRecord::unassigned(shard_id(1));
        record.owner_epoch = Some(OwnerEpoch::new(1).unwrap());
        record.owner_incarnation_id = Some(incarnation(1));
        (record, claim)
    }

    fn successor_plan(
        incarnation_value: u8,
        lease_id: u64,
        released: &LogicalShardRecord,
        predecessor: &OwnerAdmissionClaimV1,
    ) -> PlannedOwnerAdmissionV1 {
        let admission =
            crate::OwnerServingAdmission::stable(active_placement(), authority()).unwrap();
        let intent = OwnerAdmissionIntentV1::successor(
            admission.clone(),
            released.clone(),
            predecessor.clone(),
            NodeId::new("node-b").unwrap(),
            incarnation(incarnation_value),
            "node-b:7000".to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([incarnation_value; 32]).unwrap(),
        )
        .unwrap();
        let lease = LogicalShardLease {
            logical_shard_id: shard_id(1),
            owner: NodeId::new("node-b").unwrap(),
            owner_epoch: OwnerEpoch::new(2).unwrap(),
            owner_incarnation_id: incarnation(incarnation_value),
            lease_id,
            authority: admission.authority().fence(),
        };
        PlannedOwnerAdmissionV1::new(intent, lease).unwrap()
    }

    fn empty_snapshot(plan: &PlannedOwnerAdmissionV1) -> OwnerAdmissionExactSnapshot {
        OwnerAdmissionExactSnapshot::new(
            Some(plan.intent().expected_unowned_shard().clone()),
            Some(plan.intent().admission().placement().clone()),
            Some(plan.intent().admission().authority().clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            plan.intent().expected_previous_claim().cloned(),
        )
    }

    fn prepared_snapshot(plan: &PlannedOwnerAdmissionV1) -> OwnerAdmissionExactSnapshot {
        OwnerAdmissionExactSnapshot::new(
            Some(plan.intent().expected_unowned_shard().clone()),
            Some(plan.intent().admission().placement().clone()),
            Some(plan.intent().admission().authority().clone()),
            None,
            None,
            Some(plan.clone()),
            Some(OwnerAdmissionPlanSentinelV1::for_plan(plan)),
            None,
            Some(OwnerAdmissionClaimV1::prepared(plan).unwrap()),
            plan.intent().expected_previous_claim().cloned(),
        )
    }

    fn committed_snapshot(plan: &PlannedOwnerAdmissionV1) -> OwnerAdmissionExactSnapshot {
        let prepared = prepared_snapshot(plan);
        let commit = plan_owner_admission_commit(plan, &prepared);
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Commit(mutation)) =
            commit
        else {
            panic!("expected commit mutation");
        };
        let OwnerAdmissionCommitMutation {
            next_shard,
            next_session,
            next_claim,
            ..
        } = *mutation;
        OwnerAdmissionExactSnapshot::new(
            Some(next_shard),
            Some(plan.intent().admission().placement().clone()),
            Some(plan.intent().admission().authority().clone()),
            Some(next_session),
            None,
            None,
            None,
            None,
            Some(next_claim),
            plan.intent().expected_previous_claim().cloned(),
        )
    }

    fn serving_publication(plan: &PlannedOwnerAdmissionV1) -> PlannedOwnerServingPublicationV1 {
        let source = expected_recovering_record_for_plan(plan);
        PlannedOwnerServingPublicationV1::new(
            plan.clone(),
            source.clone(),
            RecoveryPublication {
                checkpoint: None,
                log: None,
                durable_lsn: source.durable_lsn,
            },
        )
        .unwrap()
    }

    fn serving_record_with_checkpoint(
        plan: &PlannedOwnerAdmissionV1,
        lsn: u64,
    ) -> LogicalShardRecord {
        let mut record = expected_recovering_record_for_plan(plan);
        apply_recovery_publication(
            &mut record,
            RecoveryPublication {
                checkpoint: Some(CheckpointRef {
                    object_key: format!("checkpoint/{lsn}"),
                    lsn,
                    image_bytes: 128,
                    image_digest: format!("image-{lsn}"),
                    digest: format!("state-{lsn}"),
                }),
                log: None,
                durable_lsn: lsn,
            },
        )
        .unwrap();
        record.state = LogicalShardState::Serving;
        record
    }

    #[test]
    fn prepare_then_commit_emits_exact_atomic_mutations() {
        let plan = fresh_plan(1, 11);
        let prepare = plan_owner_admission_prepare(&plan, &empty_snapshot(&plan));
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Prepare(mutation)) =
            prepare
        else {
            panic!("expected prepare mutation");
        };
        let OwnerAdmissionPrepareMutation {
            next_claim,
            next_plan,
            next_sentinel,
            ..
        } = *mutation;
        assert!(matches!(
            next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Prepared { .. }
        ));
        assert_eq!(next_plan, plan);
        assert_eq!(next_sentinel, OwnerAdmissionPlanSentinelV1::for_plan(&plan));

        let commit = plan_owner_admission_commit(&plan, &prepared_snapshot(&plan));
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Commit(mutation)) =
            commit
        else {
            panic!("expected commit mutation");
        };
        let OwnerAdmissionCommitMutation {
            next_shard,
            next_session,
            next_claim,
            delete_plan,
            delete_sentinel,
            ..
        } = *mutation;
        assert_eq!(next_shard.state, LogicalShardState::Recovering);
        assert_eq!(next_shard.owner_incarnation_id, Some(incarnation(1)));
        assert_eq!(next_session, *plan.lease());
        assert!(matches!(
            next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Committed { .. }
        ));
        assert_eq!(delete_plan, plan);
        assert_eq!(
            delete_sentinel,
            OwnerAdmissionPlanSentinelV1::for_plan(&plan)
        );
    }

    #[test]
    fn prepare_then_abort_supports_live_and_authoritatively_expired_sentinels() {
        let plan = fresh_plan(1, 11);
        let live = plan_owner_admission_abort(
            &plan,
            &prepared_snapshot(&plan),
            OwnerAdmissionAbortReasonV1::OwnerCasRejected,
        );
        assert!(matches!(
            live,
            OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Abort(mutation))
                if matches!(
                    mutation.expected_sentinel,
                    OwnerAdmissionSentinelExpectation::Exact(_)
                )
        ));

        let mut impossible_live = prepared_snapshot(&plan);
        impossible_live.sentinel_absence_evidence = Some(
            AuthoritativeSentinelAbsenceEvidence::after_backend_check(&plan),
        );
        assert_eq!(
            reconcile_owner_admission(&plan, &impossible_live),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::SentinelPresentWithAbsenceEvidence
            )
        );

        let mut expired = prepared_snapshot(&plan);
        expired.sentinel = None;
        assert_eq!(
            reconcile_owner_admission(&plan, &expired),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven
            )
        );
        assert_eq!(
            plan_owner_admission_abort(
                &plan,
                &expired,
                OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit,
            ),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven
            )
        );

        let foreign = fresh_plan(2, 12);
        let mut mismatched = expired.clone();
        mismatched.sentinel_absence_evidence = Some(
            AuthoritativeSentinelAbsenceEvidence::after_backend_check(&foreign),
        );
        assert_eq!(
            reconcile_owner_admission(&plan, &mismatched),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceEvidenceMismatch
            )
        );

        expired.sentinel_absence_evidence = Some(
            AuthoritativeSentinelAbsenceEvidence::after_backend_check(&plan),
        );
        assert_eq!(
            reconcile_owner_admission(&plan, &expired),
            OwnerAdmissionReconcileDecision::Classified(Box::new(
                OwnerAdmissionReconcileClassification::ExpiredPrepared {
                    claim: OwnerAdmissionClaimV1::prepared(&plan).unwrap(),
                    plan: plan.clone(),
                    expected_sentinel: OwnerAdmissionPlanSentinelV1::for_plan(&plan),
                }
            ))
        );
        assert!(matches!(
            plan_owner_admission_abort(
                &plan,
                &expired,
                OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit,
            ),
            OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Abort(mutation))
                if matches!(
                    mutation.expected_sentinel,
                    OwnerAdmissionSentinelExpectation::AuthoritativelyAbsent { .. }
                )
        ));
    }

    #[test]
    fn aborted_plan_does_not_burn_the_next_epoch() {
        let (released, predecessor) = released_predecessor();
        let first_attempt = successor_plan(2, 12, &released, &predecessor);
        let abort = plan_owner_admission_abort(
            &first_attempt,
            &prepared_snapshot(&first_attempt),
            OwnerAdmissionAbortReasonV1::OwnerCasRejected,
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Abort(mutation)) =
            abort
        else {
            panic!("expected abort mutation");
        };
        let aborted_claim = mutation.next_claim;
        assert!(matches!(
            aborted_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Aborted { .. }
        ));

        let next_attempt = successor_plan(3, 13, &released, &predecessor);
        assert_eq!(
            first_attempt.intent().planned_epoch(),
            next_attempt.intent().planned_epoch()
        );
        assert!(matches!(
            plan_owner_admission_prepare(&next_attempt, &empty_snapshot(&next_attempt)),
            OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Prepare(_))
        ));
    }

    #[test]
    fn rejected_plan_does_not_burn_the_next_epoch() {
        let (released, predecessor) = released_predecessor();
        let rejected_attempt = successor_plan(2, 12, &released, &predecessor);
        let foreign_active = successor_plan(9, 19, &released, &predecessor);
        let mut conflicted = empty_snapshot(&rejected_attempt);
        conflicted.active_plan = Some(foreign_active.clone());
        conflicted.sentinel = Some(OwnerAdmissionPlanSentinelV1::for_plan(&foreign_active));
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Reject(mutation)) =
            plan_owner_admission_prepare(&rejected_attempt, &conflicted)
        else {
            panic!("expected durable rejection");
        };
        assert!(matches!(
            mutation.next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Rejected {
                reason: OwnerAdmissionRejectionReasonV1::ActivePlanExists
            }
        ));

        let next_attempt = successor_plan(3, 13, &released, &predecessor);
        assert_eq!(
            rejected_attempt.intent().planned_epoch(),
            next_attempt.intent().planned_epoch()
        );
        assert!(matches!(
            plan_owner_admission_prepare(&next_attempt, &empty_snapshot(&next_attempt)),
            OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Prepare(_))
        ));
    }

    #[test]
    fn prepare_replays_existing_committed_aborted_and_terminated_claims() {
        let plan = fresh_plan(1, 11);

        assert!(matches!(
            plan_owner_admission_prepare(&plan, &committed_snapshot(&plan)),
            OwnerAdmissionStateDecision::Reconciled(classification)
                if matches!(
                    *classification,
                    OwnerAdmissionReconcileClassification::Committed { .. }
                )
        ));

        let aborted_claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap();
        let aborted = OwnerAdmissionExactSnapshot::new(
            Some(plan.intent().expected_unowned_shard().clone()),
            Some(plan.intent().admission().placement().clone()),
            Some(plan.intent().admission().authority().clone()),
            None,
            None,
            None,
            None,
            None,
            Some(aborted_claim),
            None,
        );
        assert!(matches!(
            plan_owner_admission_prepare(&plan, &aborted),
            OwnerAdmissionStateDecision::Reconciled(classification)
                if matches!(
                    *classification,
                    OwnerAdmissionReconcileClassification::Aborted { .. }
                )
        ));

        let committed = committed_snapshot(&plan);
        let terminate = plan_owner_admission_terminate(
            &plan,
            &committed,
            OwnerAdmissionTerminationReasonV1::Released,
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(mutation)) =
            terminate
        else {
            panic!("expected terminate mutation");
        };
        let terminated = OwnerAdmissionExactSnapshot::new(
            Some(mutation.next_shard),
            Some(plan.intent().admission().placement().clone()),
            Some(plan.intent().admission().authority().clone()),
            None,
            None,
            None,
            None,
            None,
            Some(mutation.next_claim),
            None,
        );
        assert!(matches!(
            plan_owner_admission_prepare(&plan, &terminated),
            OwnerAdmissionStateDecision::Reconciled(classification)
                if matches!(
                    *classification,
                    OwnerAdmissionReconcileClassification::Terminated { .. }
                )
        ));
    }

    #[test]
    fn intent_seal_and_prepare_share_the_candidate_claim_absence_cas() {
        let plan = fresh_plan(1, 11);
        let initial = empty_snapshot(&plan);
        let prepare = plan_owner_admission_prepare(&plan, &initial);
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Prepare(
            prepare_mutation,
        )) = prepare
        else {
            panic!("expected prepare mutation");
        };
        let seal = plan_owner_admission_intent_seal_rejected(plan.intent(), &initial);
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::SealRejected(
            seal_mutation,
        )) = seal
        else {
            panic!("expected intent-only seal mutation");
        };
        assert_eq!(prepare_mutation.expected_candidate_claim, None);
        assert_eq!(seal_mutation.expected_candidate_claim, None);
        assert_eq!(
            seal_mutation.logical_shard_id,
            plan.intent().logical_shard_id()
        );
        assert_eq!(
            seal_mutation.owner_incarnation_id,
            plan.intent().owner_incarnation_id()
        );

        assert!(matches!(
            plan_owner_admission_intent_seal_rejected(
                plan.intent(),
                &prepared_snapshot(&plan),
            ),
            OwnerAdmissionStateDecision::Reconciled(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Prepared { .. }
                )
        ));

        let mut sealed = empty_snapshot(&plan);
        sealed.candidate_claim = Some(seal_mutation.next_claim.clone());
        assert!(matches!(
            plan_owner_admission_prepare(&plan, &sealed),
            OwnerAdmissionStateDecision::Reconciled(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Rejected {
                        reason: OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
                        ..
                    }
                )
        ));
    }

    #[test]
    fn intent_reconcile_reconstructs_exact_non_rejected_plans() {
        let plan = fresh_plan(1, 11);
        assert!(matches!(
            reconcile_owner_admission_intent(plan.intent(), &prepared_snapshot(&plan)),
            OwnerAdmissionReconcileDecision::Classified(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Prepared { .. }
                )
        ));
        assert!(matches!(
            reconcile_owner_admission_intent(plan.intent(), &committed_snapshot(&plan)),
            OwnerAdmissionReconcileDecision::Classified(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Committed { .. }
                )
        ));

        let aborted_claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap();
        let aborted = OwnerAdmissionExactSnapshot::new(
            Some(plan.intent().expected_unowned_shard().clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(aborted_claim),
            None,
        );
        assert!(matches!(
            reconcile_owner_admission_intent(plan.intent(), &aborted),
            OwnerAdmissionReconcileDecision::Classified(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Aborted { .. }
                )
        ));

        let committed = committed_snapshot(&plan);
        let terminate = plan_owner_admission_terminate(
            &plan,
            &committed,
            OwnerAdmissionTerminationReasonV1::Released,
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(mutation)) =
            terminate
        else {
            panic!("expected termination mutation");
        };
        let terminated = OwnerAdmissionExactSnapshot::new(
            Some(mutation.next_shard),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(mutation.next_claim),
            None,
        );
        assert!(matches!(
            reconcile_owner_admission_intent(plan.intent(), &terminated),
            OwnerAdmissionReconcileDecision::Classified(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Terminated { .. }
                )
        ));
    }

    #[test]
    fn foreign_claim_plan_and_sentinel_fail_closed() {
        let requested = fresh_plan(1, 11);
        let foreign = fresh_plan(2, 12);

        let mut foreign_claim = empty_snapshot(&requested);
        foreign_claim.candidate_claim = Some(OwnerAdmissionClaimV1::prepared(&foreign).unwrap());
        assert_eq!(
            plan_owner_admission_prepare(&requested, &foreign_claim),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch
            )
        );

        let same_key_foreign = fresh_plan_with_reservation(1, 21, 8);
        let mut claimed_incarnation = empty_snapshot(&requested);
        claimed_incarnation.candidate_claim =
            Some(OwnerAdmissionClaimV1::prepared(&same_key_foreign).unwrap());
        assert_eq!(
            plan_owner_admission_prepare(&requested, &claimed_incarnation),
            OwnerAdmissionStateDecision::Reconciled(Box::new(
                OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed
            ))
        );
        assert_eq!(
            plan_owner_admission_commit(&requested, &claimed_incarnation),
            OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::IncarnationClaimExists
            )
        );

        let mut foreign_plan = empty_snapshot(&requested);
        foreign_plan.active_plan = Some(foreign.clone());
        foreign_plan.sentinel = Some(OwnerAdmissionPlanSentinelV1::for_plan(&foreign));
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Reject(mutation)) =
            plan_owner_admission_prepare(&requested, &foreign_plan)
        else {
            panic!("foreign singleton plan must durably reject the candidate");
        };
        let next_claim = mutation.next_claim;
        assert!(matches!(
            next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Rejected {
                reason: OwnerAdmissionRejectionReasonV1::ActivePlanExists
            }
        ));

        let mut foreign_sentinel = prepared_snapshot(&requested);
        foreign_sentinel.sentinel = Some(OwnerAdmissionPlanSentinelV1::for_plan(&foreign));
        assert_eq!(
            plan_owner_admission_commit(&requested, &foreign_sentinel),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PlanSentinelMismatch
            )
        );
    }

    #[test]
    fn unowned_record_with_live_session_is_a_split() {
        let plan = fresh_plan(1, 11);
        let mut snapshot = empty_snapshot(&plan);
        snapshot.session = Some(plan.lease().clone());
        assert_eq!(
            plan_owner_admission_prepare(&plan, &snapshot),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::PreparedClaimSessionSplit
            )
        );
    }

    #[test]
    fn commit_and_abort_share_the_same_exact_prepared_oracle() {
        let plan = fresh_plan(1, 11);
        let snapshot = prepared_snapshot(&plan);
        let commit = plan_owner_admission_commit(&plan, &snapshot);
        let abort = plan_owner_admission_abort(
            &plan,
            &snapshot,
            OwnerAdmissionAbortReasonV1::OwnerCasRejected,
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Commit(mutation)) =
            commit
        else {
            panic!("expected commit mutation");
        };
        let OwnerAdmissionCommitMutation {
            expected_claim: commit_claim,
            expected_plan: commit_plan,
            expected_sentinel: commit_sentinel,
            ..
        } = *mutation;
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Abort(mutation)) =
            abort
        else {
            panic!("expected abort mutation");
        };
        let OwnerAdmissionAbortMutation {
            expected_claim: abort_claim,
            expected_plan: abort_plan,
            expected_sentinel: OwnerAdmissionSentinelExpectation::Exact(abort_sentinel),
            ..
        } = *mutation
        else {
            panic!("expected a live sentinel comparison");
        };
        assert_eq!(commit_claim, abort_claim);
        assert_eq!(commit_plan, abort_plan);
        assert_eq!(commit_sentinel, abort_sentinel);
    }

    #[test]
    fn terminate_preserves_installed_identity_and_recovery_frontier() {
        let plan = fresh_plan(1, 11);
        let mut snapshot = committed_snapshot(&plan);
        let record = snapshot.logical_shard_record.as_mut().unwrap();
        record.state = LogicalShardState::Serving;
        record.checkpoint = Some(CheckpointRef {
            object_key: "checkpoint/7".to_owned(),
            lsn: 7,
            image_bytes: 128,
            image_digest: "image-7".to_owned(),
            digest: "state-7".to_owned(),
        });
        record.durable_lsn = 7;

        let terminate = plan_owner_admission_terminate(
            &plan,
            &snapshot,
            OwnerAdmissionTerminationReasonV1::Released,
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(mutation)) =
            terminate
        else {
            panic!("expected terminate mutation");
        };
        let OwnerAdmissionTerminateMutation {
            next_shard,
            next_claim,
            ..
        } = *mutation;
        assert_eq!(next_shard.owner, None);
        assert_eq!(next_shard.owner_epoch, Some(plan.lease().owner_epoch));
        assert_eq!(
            next_shard.owner_incarnation_id,
            Some(plan.lease().owner_incarnation_id)
        );
        assert_eq!(next_shard.lease_id, 0);
        assert_eq!(
            next_shard.checkpoint,
            snapshot.logical_shard_record.unwrap().checkpoint
        );
        assert_eq!(next_shard.durable_lsn, 7);
        assert!(matches!(
            next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Terminated {
                reason: OwnerAdmissionTerminationReasonV1::Released,
                ..
            }
        ));
        assert_eq!(
            validate_terminated_record_descendant(&plan, &next_shard),
            Ok(())
        );
    }

    #[test]
    fn committed_record_must_descend_monotonically_from_the_planned_frontier() {
        let (mut released, predecessor) = released_predecessor();
        released.checkpoint = Some(CheckpointRef {
            object_key: "checkpoint/7".to_owned(),
            lsn: 7,
            image_bytes: 128,
            image_digest: "image-7".to_owned(),
            digest: "state-7".to_owned(),
        });
        released.durable_lsn = 7;
        let plan = successor_plan(2, 12, &released, &predecessor);
        let valid = committed_snapshot(&plan);

        assert!(matches!(
            reconcile_owner_admission(&plan, &valid),
            OwnerAdmissionReconcileDecision::Classified(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Committed { .. }
                )
        ));

        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(
            terminal_mutation,
        )) = plan_owner_admission_terminate(
            &plan,
            &valid,
            OwnerAdmissionTerminationReasonV1::Released,
        )
        else {
            panic!("expected terminal mutation");
        };
        assert_eq!(
            validate_terminated_record_descendant(&plan, &terminal_mutation.next_shard),
            Ok(())
        );
        let mut forged_terminal = terminal_mutation.next_shard.clone();
        forged_terminal.checkpoint.as_mut().unwrap().digest = "forged-state-7".to_owned();
        assert!(validate_logical_shard_record(&forged_terminal).is_ok());
        assert_eq!(
            validate_terminated_record_descendant(&plan, &forged_terminal),
            Err(OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant)
        );

        let mut rolled_back = valid.clone();
        let rolled_back_record = rolled_back.logical_shard_record.as_mut().unwrap();
        rolled_back_record.checkpoint = Some(CheckpointRef {
            object_key: "checkpoint/6".to_owned(),
            lsn: 6,
            image_bytes: 128,
            image_digest: "image-6".to_owned(),
            digest: "state-6".to_owned(),
        });
        rolled_back_record.durable_lsn = 6;
        assert!(validate_logical_shard_record(rolled_back_record).is_ok());
        assert_eq!(
            reconcile_owner_admission(&plan, &rolled_back),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant
            )
        );
        assert_eq!(
            plan_owner_admission_terminate(
                &plan,
                &rolled_back,
                OwnerAdmissionTerminationReasonV1::Released,
            ),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant
            )
        );

        let mut forged_expired = valid;
        forged_expired.session = None;
        let forged_record = forged_expired.logical_shard_record.as_mut().unwrap();
        forged_record.checkpoint.as_mut().unwrap().digest = "forged-state-7".to_owned();
        assert!(validate_logical_shard_record(forged_record).is_ok());
        assert_eq!(
            reconcile_owner_admission(&plan, &forged_expired),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant
            )
        );

        let expiry_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([7; 32]).unwrap();
        assert_eq!(
            plan_owner_admission_terminate(
                &plan,
                &forged_expired,
                OwnerAdmissionTerminationReasonV1::LeaseExpired {
                    evidence_digest: expiry_digest,
                },
            ),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedRecordNotRecoveryDescendant
            )
        );
    }

    #[test]
    fn expired_committed_requires_exact_authoritative_session_evidence() {
        let plan = fresh_plan(1, 11);
        let live = committed_snapshot(&plan);
        let mut expired = live.clone();
        expired.session = None;
        let committed_claim = expired.candidate_claim.clone().unwrap();
        let committed_record = expired.logical_shard_record.clone().unwrap();

        assert_eq!(
            reconcile_owner_admission(&plan, &expired),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven
            )
        );
        assert_eq!(
            plan_owner_admission_terminate(
                &plan,
                &expired,
                OwnerAdmissionTerminationReasonV1::LeaseExpired {
                    evidence_digest: OwnerLeaseExpiryEvidenceDigest::from_bytes([7; 32]).unwrap(),
                },
            ),
            OwnerAdmissionStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven
            )
        );

        let expiry_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([7; 32]).unwrap();
        let evidence =
            AuthoritativeSessionAbsenceEvidence::after_backend_check(&plan, expiry_digest);
        expired.session_absence_evidence = Some(evidence.clone());
        assert_eq!(
            reconcile_owner_admission(&plan, &expired),
            OwnerAdmissionReconcileDecision::Classified(Box::new(
                OwnerAdmissionReconcileClassification::ExpiredCommitted {
                    claim: committed_claim,
                    record: committed_record,
                    expected_session: plan.lease().clone(),
                    evidence_digest: expiry_digest,
                }
            ))
        );

        assert_eq!(
            plan_owner_admission_terminate(
                &plan,
                &live,
                OwnerAdmissionTerminationReasonV1::LeaseExpired {
                    evidence_digest: expiry_digest,
                },
            ),
            OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::LeaseExpiryRequiresAuthoritativeSessionAbsence
            )
        );
        let mut impossible_live = live.clone();
        impossible_live.session_absence_evidence = Some(evidence.clone());
        assert_eq!(
            reconcile_owner_admission(&plan, &impossible_live),
            OwnerAdmissionReconcileDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::SessionPresentWithAbsenceEvidence
            )
        );

        let wrong_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([8; 32]).unwrap();
        let wrong_evidence =
            AuthoritativeSessionAbsenceEvidence::after_backend_check(&plan, wrong_digest);
        let mut wrong_expiry = expired.clone();
        wrong_expiry.session_absence_evidence = Some(wrong_evidence);
        assert_eq!(
            plan_owner_admission_terminate(
                &plan,
                &wrong_expiry,
                OwnerAdmissionTerminationReasonV1::LeaseExpired {
                    evidence_digest: expiry_digest,
                },
            ),
            OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::LeaseExpiryEvidenceMismatch
            )
        );

        let terminate = plan_owner_admission_terminate(
            &plan,
            &expired,
            OwnerAdmissionTerminationReasonV1::LeaseExpired {
                evidence_digest: expiry_digest,
            },
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(mutation)) =
            terminate
        else {
            panic!("expected expiry termination mutation");
        };
        assert_eq!(
            mutation.expected_session,
            OwnerAdmissionSessionExpectation::AuthoritativelyAbsent {
                expected: plan.lease().clone(),
                evidence_digest: expiry_digest,
            }
        );
        assert_eq!(mutation.delete_session, None);
        assert!(matches!(
            mutation.next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Terminated {
                reason: OwnerAdmissionTerminationReasonV1::LeaseExpired {
                    evidence_digest: actual,
                },
                ..
            } if *actual == expiry_digest
        ));
    }

    #[test]
    fn released_requires_and_deletes_the_exact_live_session() {
        let plan = fresh_plan(1, 11);
        let live = committed_snapshot(&plan);
        let mut expired = live.clone();
        expired.session = None;
        expired.session_absence_evidence =
            Some(AuthoritativeSessionAbsenceEvidence::after_backend_check(
                &plan,
                OwnerLeaseExpiryEvidenceDigest::from_bytes([7; 32]).unwrap(),
            ));
        assert_eq!(
            plan_owner_admission_terminate(
                &plan,
                &expired,
                OwnerAdmissionTerminationReasonV1::Released,
            ),
            OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::ExactSessionRequired
            )
        );

        let terminate = plan_owner_admission_terminate(
            &plan,
            &live,
            OwnerAdmissionTerminationReasonV1::Released,
        );
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(mutation)) =
            terminate
        else {
            panic!("expected live release mutation");
        };
        assert_eq!(
            mutation.expected_session,
            OwnerAdmissionSessionExpectation::Exact(plan.lease().clone())
        );
        assert_eq!(mutation.delete_session, Some(plan.lease().clone()));

        let late_snapshot = OwnerAdmissionExactSnapshot::new(
            Some(mutation.next_shard.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(mutation.next_claim.clone()),
            None,
        );
        assert!(matches!(
            plan_owner_admission_terminate(
                &plan,
                &late_snapshot,
                OwnerAdmissionTerminationReasonV1::Released,
            ),
            OwnerAdmissionStateDecision::Reconciled(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::Terminated {
                        reason: OwnerAdmissionTerminationReasonV1::Released,
                        ..
                    }
                )
        ));
    }

    #[test]
    fn commit_blocks_placement_and_authority_drift_but_abort_and_terminate_do_not() {
        let plan = fresh_plan(1, 11);
        let mut placement_drift = prepared_snapshot(&plan);
        placement_drift
            .placement
            .as_mut()
            .unwrap()
            .placement_generation = PlacementGeneration::new(3).unwrap();
        assert_eq!(
            plan_owner_admission_commit(&plan, &placement_drift),
            OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::ServingAdmissionChanged
            )
        );
        assert!(matches!(
            plan_owner_admission_abort(
                &plan,
                &placement_drift,
                OwnerAdmissionAbortReasonV1::OwnerCasRejected,
            ),
            OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Abort(_))
        ));

        let mut authority_drift = prepared_snapshot(&plan);
        authority_drift.authority.as_mut().unwrap().record_revision =
            MetadataAuthorityRevision::new(2).unwrap();
        assert_eq!(
            plan_owner_admission_commit(&plan, &authority_drift),
            OwnerAdmissionStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::ServingAdmissionChanged
            )
        );

        let mut committed = committed_snapshot(&plan);
        committed.placement = None;
        committed.authority = None;
        assert!(matches!(
            plan_owner_admission_terminate(
                &plan,
                &committed,
                OwnerAdmissionTerminationReasonV1::Released,
            ),
            OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(_))
        ));
    }

    #[test]
    fn publish_serving_plans_exact_source_and_classifies_replay_and_conflict() {
        let plan = fresh_plan(1, 11);
        let publication = serving_publication(&plan);
        let source_snapshot = committed_snapshot(&plan);

        let OwnerServingPublicationStateDecision::Mutation(mutation) =
            plan_owner_serving_publication(&publication, &source_snapshot)
        else {
            panic!("exact Recovering source must produce a publication mutation");
        };
        assert_eq!(mutation.expected_shard, *publication.source());
        assert_eq!(mutation.expected_session, *plan.lease());
        assert_eq!(
            mutation.expected_claim,
            OwnerAdmissionClaimV1::prepared(&plan)
                .unwrap()
                .commit()
                .unwrap()
        );
        assert_eq!(mutation.expected_active_plan, None);
        assert_eq!(mutation.expected_sentinel, None);
        assert_eq!(mutation.next_shard, *publication.target());

        let mut replay = source_snapshot.clone();
        replay.logical_shard_record = Some(publication.target().clone());
        assert!(matches!(
            plan_owner_serving_publication(&publication, &replay),
            OwnerServingPublicationStateDecision::AlreadyPublished { record, .. }
                if record == *publication.target()
        ));

        let conflicting = serving_record_with_checkpoint(&plan, 7);
        let mut conflict = source_snapshot;
        conflict.logical_shard_record = Some(conflicting.clone());
        assert!(matches!(
            plan_owner_serving_publication(&publication, &conflict),
            OwnerServingPublicationStateDecision::PublicationConflict { record, .. }
                if record == conflicting
        ));
    }

    #[test]
    fn publish_serving_blocks_changed_recovering_source_and_closes_expiry() {
        let plan = fresh_plan(1, 11);
        let publication = serving_publication(&plan);
        let mut changed_source = committed_snapshot(&plan);
        let mut changed_record = serving_record_with_checkpoint(&plan, 7);
        changed_record.state = LogicalShardState::Recovering;
        changed_source.logical_shard_record = Some(changed_record);
        assert_eq!(
            plan_owner_serving_publication(&publication, &changed_source),
            OwnerServingPublicationStateDecision::Blocked(
                OwnerAdmissionTransitionBlockCode::RecoveringSourceChanged
            )
        );

        let mut expired = committed_snapshot(&plan);
        expired.session = None;
        let evidence_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([10; 32]).unwrap();
        expired.session_absence_evidence = Some(
            AuthoritativeSessionAbsenceEvidence::after_backend_check(&plan, evidence_digest),
        );
        assert!(matches!(
            plan_owner_serving_publication(&publication, &expired),
            OwnerServingPublicationStateDecision::ExpiredCommitted {
                expected_session,
                evidence_digest: actual,
                ..
            } if expected_session == *plan.lease() && actual == evidence_digest
        ));
    }

    #[test]
    fn publish_serving_classifies_terminal_superseded_and_inconsistent_records() {
        let plan = fresh_plan(1, 11);
        let publication = serving_publication(&plan);
        let committed = committed_snapshot(&plan);
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(
            termination,
        )) = plan_owner_admission_terminate(
            &plan,
            &committed,
            OwnerAdmissionTerminationReasonV1::Released,
        )
        else {
            panic!("expected termination mutation");
        };
        let terminal_claim = termination.next_claim.clone();
        let terminal = OwnerAdmissionExactSnapshot::new(
            Some(termination.next_shard.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(terminal_claim.clone()),
            None,
        );
        assert!(matches!(
            plan_owner_serving_publication(&publication, &terminal),
            OwnerServingPublicationStateDecision::Terminated { record, claim }
                if record == termination.next_shard && claim == terminal_claim
        ));

        let successor = successor_plan(2, 12, &termination.next_shard, &terminal_claim);
        let successor_live = committed_snapshot(&successor);
        let superseded = OwnerAdmissionExactSnapshot::new(
            successor_live.logical_shard_record.clone(),
            None,
            None,
            successor_live.session.clone(),
            None,
            None,
            None,
            None,
            Some(terminal_claim.clone()),
            None,
        );
        assert!(matches!(
            plan_owner_serving_publication(&publication, &superseded),
            OwnerServingPublicationStateDecision::Superseded { claim, .. }
                if claim == terminal_claim
        ));

        let mut split = superseded;
        split.logical_shard_record.as_mut().unwrap().owner_epoch = Some(plan.lease().owner_epoch);
        assert_eq!(
            plan_owner_serving_publication(&publication, &split),
            OwnerServingPublicationStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::SameEpochDifferentInstalledIncarnation
            )
        );
    }

    #[test]
    fn renew_classifies_current_recovering_or_serving_and_returns_exact_record() {
        let plan = fresh_plan(1, 11);
        let committed_claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .commit()
            .unwrap();
        let target = OwnerSessionRenewalTargetV1::new(plan.clone(), committed_claim).unwrap();
        let recovering = committed_snapshot(&plan);
        assert!(matches!(
            classify_owner_session_renewal(&target, &recovering),
            OwnerSessionRenewalStateDecision::Current { record, session, .. }
                if record == expected_recovering_record_for_plan(&plan)
                    && session == *plan.lease()
        ));

        let serving_record = serving_record_with_checkpoint(&plan, 7);
        let mut serving = recovering.clone();
        serving.logical_shard_record = Some(serving_record.clone());
        assert!(matches!(
            classify_owner_session_renewal(&target, &serving),
            OwnerSessionRenewalStateDecision::Current { record, .. }
                if record == serving_record
        ));

        let mut foreign_session = recovering;
        foreign_session.session.as_mut().unwrap().lease_id += 1;
        assert_eq!(
            classify_owner_session_renewal(&target, &foreign_session),
            OwnerSessionRenewalStateDecision::Inconsistent(
                OwnerAdmissionInconsistencyCode::CommittedClaimSessionSplit
            )
        );
    }

    #[test]
    fn renew_classifies_authoritative_expiry_without_inventing_a_session() {
        let plan = fresh_plan(1, 11);
        let committed_claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .commit()
            .unwrap();
        let target =
            OwnerSessionRenewalTargetV1::new(plan.clone(), committed_claim.clone()).unwrap();
        let mut expired = committed_snapshot(&plan);
        expired.session = None;
        let evidence_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([9; 32]).unwrap();
        expired.session_absence_evidence = Some(
            AuthoritativeSessionAbsenceEvidence::after_backend_check(&plan, evidence_digest),
        );
        assert!(matches!(
            classify_owner_session_renewal(&target, &expired),
            OwnerSessionRenewalStateDecision::ExpiredCommitted {
                claim,
                expected_session,
                evidence_digest: actual,
                ..
            } if claim == committed_claim
                && expected_session == *plan.lease()
                && actual == evidence_digest
        ));
    }

    #[test]
    fn renew_classifies_terminated_and_superseded_without_reusing_the_old_session() {
        let plan = fresh_plan(1, 11);
        let committed_claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .commit()
            .unwrap();
        let target =
            OwnerSessionRenewalTargetV1::new(plan.clone(), committed_claim.clone()).unwrap();
        let committed = committed_snapshot(&plan);
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(
            termination,
        )) = plan_owner_admission_terminate(
            &plan,
            &committed,
            OwnerAdmissionTerminationReasonV1::Released,
        )
        else {
            panic!("expected termination mutation");
        };
        let terminal_claim = termination.next_claim.clone();
        let terminal = OwnerAdmissionExactSnapshot::new(
            Some(termination.next_shard.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(terminal_claim.clone()),
            None,
        );
        assert!(matches!(
            classify_owner_session_renewal(&target, &terminal),
            OwnerSessionRenewalStateDecision::Terminated { claim, .. }
                if claim == terminal_claim
        ));

        let successor = successor_plan(2, 12, &termination.next_shard, &terminal_claim);
        let successor_live = committed_snapshot(&successor);
        let superseded = OwnerAdmissionExactSnapshot::new(
            successor_live.logical_shard_record,
            None,
            None,
            successor_live.session,
            None,
            None,
            None,
            None,
            Some(terminal_claim.clone()),
            None,
        );
        assert!(matches!(
            classify_owner_session_renewal(&target, &superseded),
            OwnerSessionRenewalStateDecision::Superseded { claim, .. }
                if claim == terminal_claim
        ));
    }
}
