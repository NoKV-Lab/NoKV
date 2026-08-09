use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::owner_admission::OwnerSessionLifetimeObservationV1;
use crate::owner_admission_command::{
    AbortOwnerAdmissionCommandV1, AbortOwnerAdmissionNotDispatchedV1, AbortOwnerAdmissionOutcomeV1,
    AbortOwnerAdmissionResultV1, CommitOwnerAdmissionCommandV1,
    CommitOwnerAdmissionNotDispatchedV1, CommitOwnerAdmissionOutcomeV1,
    CommitOwnerAdmissionResultV1, PrepareOwnerAdmissionCommandV1,
    PrepareOwnerAdmissionNotDispatchedV1, PrepareOwnerAdmissionOutcomeV1,
    PrepareOwnerAdmissionResultV1, PublishOwnerServingCommandV1,
    PublishOwnerServingNotDispatchedV1, PublishOwnerServingOutcomeV1, PublishOwnerServingResultV1,
    ReconcileOwnerAdmissionCommandV1, ReconcileOwnerAdmissionNotDispatchedV1,
    ReconcileOwnerAdmissionOutcomeV1, ReconcileOwnerAdmissionResultV1,
    ReconcileOwnerAdmissionTargetV1, RenewOwnerSessionCommandV1, RenewOwnerSessionNotDispatchedV1,
    RenewOwnerSessionOutcomeV1, RenewOwnerSessionResultV1, TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionNotDispatchedV1, TerminateOwnerAdmissionOutcomeV1,
    TerminateOwnerAdmissionResultV1,
};
use crate::owner_admission_state::{
    classify_owner_session_renewal, plan_owner_admission_abort, plan_owner_admission_commit,
    plan_owner_admission_intent_seal_rejected, plan_owner_admission_prepare,
    plan_owner_admission_terminate, plan_owner_serving_publication, reconcile_owner_admission,
    reconcile_owner_admission_intent, validate_terminated_record_descendant,
    OwnerAdmissionExactSnapshot, OwnerAdmissionInconsistencyCode, OwnerAdmissionMutationPlan,
    OwnerAdmissionReconcileClassification, OwnerAdmissionReconcileDecision,
    OwnerAdmissionSentinelExpectation, OwnerAdmissionSessionExpectation,
    OwnerAdmissionStateDecision, OwnerServingPublicationStateDecision,
    OwnerSessionRenewalStateDecision,
};
use crate::types::endpoint_is_canonical;
use crate::{
    CheckpointRef, ControlError, FreshRootProvisioningDisposition, FreshRootProvisioningOutcome,
    LogRef, LogicalShardId, LogicalShardLease, LogicalShardRecord, LogicalShardState,
    MetadataAuthorityBinding, MetadataAuthorityFence, MetadataAuthorityGeneration,
    MetadataAuthorityRecord, MetadataMigration, MetadataMigrationPhase, MetadataRecoveryFrontier,
    NodeId, OwnerAdmissionAbortReasonV1, OwnerAdmissionClaimPhaseV1, OwnerAdmissionClaimV1,
    OwnerAdmissionIntentV1, OwnerAdmissionPlanSentinelV1, OwnerAdmissionTerminationReasonV1,
    OwnerEpoch, OwnerIncarnationId, OwnerLeaseModel, OwnerReleaseOutcome,
    OwnerSessionRenewalTargetV1, PlannedOwnerAdmissionV1, PlannedOwnerServingPublicationV1,
    RecoveryPublication, RootId, RootLayoutProfile, RootPartitionId, RootPlacement,
    RootPlacementLifecycle, SourceQuiesceReceipt, TargetActivationToken,
};

/// Exact, request-scoped control-plane state admitted for one root route.
///
/// This guard is deliberately not persisted in a shard lease: one physical
/// shard owner may serve several independently fenced roots. Every owner
/// acquire, renewal, and Serving publication must nevertheless compare the
/// exact root placement and authority that bootstrap validated. A future
/// qualified migration protocol may add a separate constructor with an
/// explicit source-serving policy; `stable` never admits any migration phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerServingAdmission {
    placement: RootPlacement,
    authority: MetadataAuthorityRecord,
}

impl OwnerServingAdmission {
    pub fn stable(
        placement: RootPlacement,
        authority: MetadataAuthorityRecord,
    ) -> Result<Self, ControlError> {
        validate_qualified_single_shard_layout(&placement)?;
        if placement.lifecycle != RootPlacementLifecycle::Active {
            return Err(ControlError::InvalidPlacementMutation {
                root_id: placement.root_id,
                reason: "owner Serving admission requires an Active root placement".to_owned(),
            });
        }
        validate_metadata_authority_record(&authority)?;
        if placement.logical_shard_id != authority.logical_shard_id {
            return Err(ControlError::MetadataAuthorityAdmission {
                logical_shard_id: authority.logical_shard_id,
                reason: "owner Serving placement and metadata authority belong to different logical shards"
                    .to_owned(),
            });
        }
        if authority.migration.is_some() {
            return Err(ControlError::MetadataAuthorityAdmission {
                logical_shard_id: authority.logical_shard_id,
                reason: "stable owner Serving admission requires metadata migration to be absent"
                    .to_owned(),
            });
        }
        Ok(Self {
            placement,
            authority,
        })
    }

    pub const fn placement(&self) -> &RootPlacement {
        &self.placement
    }

    pub const fn authority(&self) -> &MetadataAuthorityRecord {
        &self.authority
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.placement.logical_shard_id
    }
}

/// Durable control-plane operations, split between immutable root placement
/// and physical logical-shard ownership.
pub trait ControlStore: Send + Sync {
    /// Report the immutable owner-session lifetime model implemented by this
    /// store. This capability query must not perform backend I/O or mutate
    /// control state; bootstrap uses it before any control or provider access.
    fn owner_lease_model(&self) -> OwnerLeaseModel;

    /// Execute one intent-bound planned-owner prepare as an ultimate backend.
    /// The command is consumed exactly once and always completes with a closed
    /// outcome after execution is claimed.
    fn prepare_owner_admission(
        &self,
        command: PrepareOwnerAdmissionCommandV1,
    ) -> PrepareOwnerAdmissionOutcomeV1;

    /// Execute the exact Prepared -> Committed transition.
    fn commit_owner_admission(
        &self,
        command: CommitOwnerAdmissionCommandV1,
    ) -> CommitOwnerAdmissionOutcomeV1;

    /// Execute the exact Prepared -> Aborted transition.
    fn abort_owner_admission(
        &self,
        command: AbortOwnerAdmissionCommandV1,
    ) -> AbortOwnerAdmissionOutcomeV1;

    /// Execute one exact Committed -> Terminated transition.
    fn terminate_owner_admission(
        &self,
        command: TerminateOwnerAdmissionCommandV1,
    ) -> TerminateOwnerAdmissionOutcomeV1;

    /// Reconcile one intent or exact plan without inventing missing state.
    fn reconcile_owner_admission(
        &self,
        command: ReconcileOwnerAdmissionCommandV1,
    ) -> ReconcileOwnerAdmissionOutcomeV1;

    /// Atomically publish the exact planned Recovering -> Serving record.
    fn publish_owner_serving(
        &self,
        command: PublishOwnerServingCommandV1,
    ) -> PublishOwnerServingOutcomeV1;

    /// Renew and re-prove one exact committed owner session.
    fn renew_owner_session(
        &self,
        command: RenewOwnerSessionCommandV1,
    ) -> RenewOwnerSessionOutcomeV1;

    /// Atomically install a never-before-seen shard, generation-one metadata
    /// authority, and Provisioning root placement. Partial state is never
    /// adopted. A complete exact bundle may be replayed read-only.
    fn provision_fresh_root(
        &self,
        initial_placement: RootPlacement,
        initial_authority: MetadataAuthorityRecord,
    ) -> Result<FreshRootProvisioningOutcome, ControlError>;

    fn create_root_placement(
        &self,
        placement: RootPlacement,
    ) -> Result<RootPlacement, ControlError>;
    fn get_root_placement(&self, root_id: &RootId) -> Result<Option<RootPlacement>, ControlError>;
    fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError>;
    fn compare_and_set_root_placement(
        &self,
        expected: &RootPlacement,
        next: RootPlacement,
    ) -> Result<RootPlacement, ControlError>;

    fn create_logical_shard(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<LogicalShardRecord, ControlError>;
    fn get_logical_shard(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<LogicalShardRecord>, ControlError>;
    fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError>;

    /// Install generation one of the metadata authority for a logical shard.
    fn create_metadata_authority(
        &self,
        authority: MetadataAuthorityRecord,
    ) -> Result<MetadataAuthorityRecord, ControlError>;
    fn get_metadata_authority(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<MetadataAuthorityRecord>, ControlError>;
    /// Advance migration evidence or change the active authority by exact CAS.
    fn compare_and_set_metadata_authority(
        &self,
        expected: &MetadataAuthorityRecord,
        next: MetadataAuthorityRecord,
    ) -> Result<MetadataAuthorityRecord, ControlError>;

    /// Install the first owner epoch under one exact root/authority admission.
    fn acquire_owner(
        &self,
        admission: &OwnerServingAdmission,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError>;

    /// Install a successor owner by exact comparison with the last durable
    /// owner epoch. A backend must also prove that the previous session is gone.
    fn acquire_successor(
        &self,
        admission: &OwnerServingAdmission,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError>;

    fn renew_owner(
        &self,
        lease: &LogicalShardLease,
        admission: &OwnerServingAdmission,
    ) -> Result<LogicalShardRecord, ControlError>;

    /// Publish a caller-serialized recovery frontier and make the exact owner
    /// generation routable.
    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        admission: &OwnerServingAdmission,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError>;

    /// Release or reconcile one exact owner session.
    ///
    /// A backend error is permitted only before a release attempt can have
    /// taken effect. Once an attempt may have reached the backend, the method
    /// returns `OutcomeUnknown` unless it can prove a terminal exact state.
    fn release_owner(&self, lease: &LogicalShardLease)
        -> Result<OwnerReleaseOutcome, ControlError>;
}

#[derive(Default)]
struct InMemoryState {
    root_placements: BTreeMap<RootId, RootPlacement>,
    logical_shards: BTreeMap<LogicalShardId, LogicalShardRecord>,
    metadata_authorities: BTreeMap<LogicalShardId, MetadataAuthorityRecord>,
    owner_sessions: BTreeMap<LogicalShardId, LogicalShardLease>,
    owner_admission_claims: BTreeMap<(LogicalShardId, OwnerIncarnationId), OwnerAdmissionClaimV1>,
    owner_admission_plans: BTreeMap<LogicalShardId, PlannedOwnerAdmissionV1>,
    owner_admission_sentinels: BTreeMap<LogicalShardId, OwnerAdmissionPlanSentinelV1>,
    next_lease_id: u64,
}

#[derive(Default)]
pub struct InMemoryControlStore {
    state: Mutex<InMemoryState>,
}

impl InMemoryControlStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlStore for InMemoryControlStore {
    fn owner_lease_model(&self) -> OwnerLeaseModel {
        OwnerLeaseModel::NonExpiring
    }

    fn prepare_owner_admission(
        &self,
        command: PrepareOwnerAdmissionCommandV1,
    ) -> PrepareOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let intent = claimed.inspect().clone();
        let result = {
            let mut state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_prepare_owner_admission(&mut state, &intent)
        };
        claimed.complete(result)
    }

    fn commit_owner_admission(
        &self,
        command: CommitOwnerAdmissionCommandV1,
    ) -> CommitOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let plan = claimed.inspect().clone();
        let result = {
            let mut state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_commit_owner_admission(&mut state, &plan)
        };
        claimed.complete(result)
    }

    fn abort_owner_admission(
        &self,
        command: AbortOwnerAdmissionCommandV1,
    ) -> AbortOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let plan = inspection.plan.clone();
        let reason = inspection.reason;
        let result = {
            let mut state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_abort_owner_admission(&mut state, &plan, reason)
        };
        claimed.complete(result)
    }

    fn terminate_owner_admission(
        &self,
        command: TerminateOwnerAdmissionCommandV1,
    ) -> TerminateOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let plan = inspection.plan.clone();
        let reason = inspection.reason.clone();
        let result = {
            let mut state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_terminate_owner_admission(&mut state, &plan, reason)
        };
        claimed.complete(result)
    }

    fn reconcile_owner_admission(
        &self,
        command: ReconcileOwnerAdmissionCommandV1,
    ) -> ReconcileOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let target = claimed.inspect().clone();
        let result = {
            let mut state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_reconcile_owner_admission(&mut state, &target)
        };
        claimed.complete(result)
    }

    fn publish_owner_serving(
        &self,
        command: PublishOwnerServingCommandV1,
    ) -> PublishOwnerServingOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let publication = match PlannedOwnerServingPublicationV1::new(
            inspection.plan.clone(),
            inspection.source.clone(),
            inspection.publication.clone(),
        ) {
            Ok(publication) if publication.target() == inspection.target => publication,
            _ => {
                return claimed.complete(PublishOwnerServingResultV1::NotDispatched(
                    PublishOwnerServingNotDispatchedV1::InvalidPublicationBeforeEffect,
                ));
            }
        };
        let result = {
            let mut state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_publish_owner_serving(&mut state, &publication, &claimed)
        };
        claimed.complete(result)
    }

    fn renew_owner_session(
        &self,
        command: RenewOwnerSessionCommandV1,
    ) -> RenewOwnerSessionOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let target = match OwnerSessionRenewalTargetV1::new(
            inspection.plan.clone(),
            inspection.claim.clone(),
        ) {
            Ok(target) if target.session() == inspection.session => target,
            _ => {
                return claimed.complete(RenewOwnerSessionResultV1::NotDispatched(
                    RenewOwnerSessionNotDispatchedV1::InvalidTargetBeforeEffect,
                ));
            }
        };
        let result = {
            let state = self.state.lock().expect("control store mutex poisoned");
            execute_in_memory_renew_owner_session(&state, &target, &claimed)
        };
        claimed.complete(result)
    }

    fn provision_fresh_root(
        &self,
        initial_placement: RootPlacement,
        initial_authority: MetadataAuthorityRecord,
    ) -> Result<FreshRootProvisioningOutcome, ControlError> {
        validate_fresh_root_provisioning_input(&initial_placement, &initial_authority)?;
        let desired_shard = LogicalShardRecord::unassigned(initial_placement.logical_shard_id);
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let actual_shard = state
            .logical_shards
            .get(&initial_placement.logical_shard_id)
            .cloned();
        let actual_authority = state
            .metadata_authorities
            .get(&initial_placement.logical_shard_id)
            .cloned();
        let actual_placement = state
            .root_placements
            .get(&initial_placement.root_id)
            .cloned();
        let actual_session = state
            .owner_sessions
            .get(&initial_placement.logical_shard_id)
            .cloned();

        if actual_shard.is_none()
            && actual_authority.is_none()
            && actual_placement.is_none()
            && actual_session.is_none()
        {
            state
                .logical_shards
                .insert(initial_placement.logical_shard_id, desired_shard.clone());
            state.metadata_authorities.insert(
                initial_placement.logical_shard_id,
                initial_authority.clone(),
            );
            state
                .root_placements
                .insert(initial_placement.root_id, initial_placement.clone());
            return Ok(FreshRootProvisioningOutcome {
                disposition: FreshRootProvisioningDisposition::Created,
                logical_shard: desired_shard,
                metadata_authority: initial_authority,
                root_placement: initial_placement,
            });
        }

        classify_fresh_root_provisioning_replay(
            &initial_placement,
            &initial_authority,
            actual_shard.as_ref(),
            actual_authority.as_ref(),
            actual_placement.as_ref(),
            actual_session.as_ref(),
        )
    }

    fn create_root_placement(
        &self,
        placement: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        validate_new_root_placement(&placement)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        if !state
            .logical_shards
            .contains_key(&placement.logical_shard_id)
        {
            return Err(ControlError::LogicalShardNotFound(
                placement.logical_shard_id,
            ));
        }
        if let Some(current) = state.root_placements.get(&placement.root_id) {
            if current == &placement {
                return Ok(current.clone());
            }
            if current.logical_shard_id != placement.logical_shard_id {
                return Err(ControlError::ImmutableShardAffinity {
                    root_id: placement.root_id,
                    existing: current.logical_shard_id,
                    requested: placement.logical_shard_id,
                });
            }
            return Err(ControlError::RootPlacementAlreadyExists(placement.root_id));
        }
        state
            .root_placements
            .insert(placement.root_id, placement.clone());
        Ok(placement)
    }

    fn get_root_placement(&self, root_id: &RootId) -> Result<Option<RootPlacement>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.root_placements.get(root_id).cloned())
    }

    fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.root_placements.values().cloned().collect())
    }

    fn compare_and_set_root_placement(
        &self,
        expected: &RootPlacement,
        next: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        validate_root_placement_update(expected, &next)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let actual = state.root_placements.get(&expected.root_id).cloned();
        if actual.as_ref() == Some(&next) {
            return Ok(next);
        }
        if actual.as_ref() != Some(expected) {
            return Err(ControlError::RootPlacementCasConflict {
                expected: Box::new(expected.clone()),
                actual: actual.map(Box::new),
            });
        }
        state.root_placements.insert(next.root_id, next.clone());
        Ok(next)
    }

    fn create_logical_shard(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<LogicalShardRecord, ControlError> {
        let desired = LogicalShardRecord::unassigned(logical_shard_id);
        let mut state = self.state.lock().expect("control store mutex poisoned");
        if let Some(current) = state.logical_shards.get(&logical_shard_id) {
            if current == &desired {
                return Ok(current.clone());
            }
            return Err(ControlError::LogicalShardAlreadyExists(logical_shard_id));
        }
        state
            .logical_shards
            .insert(logical_shard_id, desired.clone());
        Ok(desired)
    }

    fn get_logical_shard(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<LogicalShardRecord>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.logical_shards.get(logical_shard_id).cloned())
    }

    fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.logical_shards.values().cloned().collect())
    }

    fn create_metadata_authority(
        &self,
        authority: MetadataAuthorityRecord,
    ) -> Result<MetadataAuthorityRecord, ControlError> {
        validate_new_metadata_authority(&authority)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let shard = state
            .logical_shards
            .get(&authority.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(
                authority.logical_shard_id,
            ))?;
        if let Some(current) = state.metadata_authorities.get(&authority.logical_shard_id) {
            if current == &authority {
                return Ok(current.clone());
            }
            return Err(ControlError::MetadataAuthorityAlreadyExists(
                authority.logical_shard_id,
            ));
        }
        validate_fresh_authority_shard(&shard)?;
        if state
            .owner_sessions
            .contains_key(&authority.logical_shard_id)
        {
            return Err(ControlError::MetadataAuthorityAdoptionRejected {
                logical_shard_id: authority.logical_shard_id,
                reason: "an owner session already exists".to_owned(),
            });
        }
        state
            .metadata_authorities
            .insert(authority.logical_shard_id, authority.clone());
        Ok(authority)
    }

    fn get_metadata_authority(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<MetadataAuthorityRecord>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.metadata_authorities.get(logical_shard_id).cloned())
    }

    fn compare_and_set_metadata_authority(
        &self,
        expected: &MetadataAuthorityRecord,
        next: MetadataAuthorityRecord,
    ) -> Result<MetadataAuthorityRecord, ControlError> {
        validate_metadata_authority_update(expected, &next)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let actual = state
            .metadata_authorities
            .get(&expected.logical_shard_id)
            .cloned();
        if actual.as_ref() == Some(&next) {
            if let Some(receipt) =
                metadata_authority_update_installs_source_receipt(expected, &next)
            {
                let shard = state.logical_shards.get(&expected.logical_shard_id).ok_or(
                    ControlError::LogicalShardNotFound(expected.logical_shard_id),
                )?;
                let session = state.owner_sessions.get(&expected.logical_shard_id);
                validate_source_receipt_control_epoch(expected, receipt, shard, session)?;
            }
            return Ok(next);
        }
        if actual.as_ref() != Some(expected) {
            return Err(ControlError::MetadataAuthorityCasConflict {
                expected: Box::new(expected.clone()),
                actual: actual.map(Box::new),
            });
        }
        if let Some(receipt) = metadata_authority_update_installs_source_receipt(expected, &next) {
            let shard = state
                .logical_shards
                .get(&expected.logical_shard_id)
                .cloned()
                .ok_or(ControlError::LogicalShardNotFound(
                    expected.logical_shard_id,
                ))?;
            let session = state.owner_sessions.get(&expected.logical_shard_id);
            validate_source_receipt_control_epoch(expected, receipt, &shard, session)?;
        }
        if metadata_authority_update_requires_unowned(expected, &next) {
            if state
                .owner_sessions
                .contains_key(&expected.logical_shard_id)
            {
                return Err(ControlError::MetadataAuthorityAdmission {
                    logical_shard_id: expected.logical_shard_id,
                    reason: "migration cutover requires the owner session to be absent".to_owned(),
                });
            }
            let shard = state
                .logical_shards
                .get(&expected.logical_shard_id)
                .cloned()
                .ok_or(ControlError::LogicalShardNotFound(
                    expected.logical_shard_id,
                ))?;
            if metadata_authority_update_enters_ready(expected, &next) {
                let receipt = next
                    .migration
                    .as_ref()
                    .and_then(|migration| migration.source_quiesce_receipt.as_ref())
                    .expect("ReadyToCutover validation requires a source receipt");
                validate_source_receipt_control_epoch(expected, receipt, &shard, None)?;
                let cleaned = prepare_expired_owner_cleanup(&shard)?;
                state
                    .logical_shards
                    .insert(expected.logical_shard_id, cleaned);
            } else {
                ensure_unowned_for_cutover(&shard)?;
            }
        }
        state
            .metadata_authorities
            .insert(next.logical_shard_id, next.clone());
        Ok(next)
    }

    fn acquire_owner(
        &self,
        admission: &OwnerServingAdmission,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        validate_endpoint(&endpoint)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        validate_in_memory_serving_admission(&state, admission)?;
        let logical_shard_id = admission.logical_shard_id();
        let current = state
            .logical_shards
            .get(&logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
        if let (Some(current_owner), Some(owner_epoch)) =
            (current.owner.clone(), current.owner_epoch)
        {
            return Err(ControlError::LogicalShardAlreadyOwned {
                logical_shard_id,
                owner: current_owner,
                owner_epoch,
            });
        }
        let lease_id = allocate_lease_id(&mut state, logical_shard_id)?;
        let (next, lease) = prepare_owner_acquisition(
            &current,
            None,
            owner,
            owner_incarnation_id,
            endpoint,
            lease_id,
            admission.authority().fence(),
        )?;
        state.logical_shards.insert(logical_shard_id, next);
        state.owner_sessions.insert(logical_shard_id, lease.clone());
        Ok(lease)
    }

    fn acquire_successor(
        &self,
        admission: &OwnerServingAdmission,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        validate_endpoint(&endpoint)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        validate_in_memory_serving_admission(&state, admission)?;
        let logical_shard_id = admission.logical_shard_id();
        let current = state
            .logical_shards
            .get(&logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
        if current.owner_epoch != Some(expected_owner_epoch) {
            return Err(ControlError::StaleOwnerEpoch {
                logical_shard_id,
                expected: Some(expected_owner_epoch),
                actual: current.owner_epoch,
            });
        }
        if current.owner.is_some() {
            return Err(ControlError::PreviousOwnerSessionLive {
                logical_shard_id,
                owner_epoch: expected_owner_epoch,
            });
        }
        let lease_id = allocate_lease_id(&mut state, logical_shard_id)?;
        let (next, lease) = prepare_owner_acquisition(
            &current,
            Some(expected_owner_epoch),
            owner,
            owner_incarnation_id,
            endpoint,
            lease_id,
            admission.authority().fence(),
        )?;
        state.logical_shards.insert(logical_shard_id, next);
        state.owner_sessions.insert(logical_shard_id, lease.clone());
        Ok(lease)
    }

    fn renew_owner(
        &self,
        lease: &LogicalShardLease,
        admission: &OwnerServingAdmission,
    ) -> Result<LogicalShardRecord, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        validate_in_memory_serving_admission(&state, admission)?;
        validate_lease_serving_admission(lease, admission)?;
        let record = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(record, lease)?;
        let authority = state
            .metadata_authorities
            .get(&lease.logical_shard_id)
            .ok_or(ControlError::MetadataAuthorityNotFound(
                lease.logical_shard_id,
            ))?;
        validate_authority_for_owner_operation(authority, Some(lease))?;
        validate_in_memory_owner_session(&state, lease)?;
        Ok(record.clone())
    }

    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        admission: &OwnerServingAdmission,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        validate_in_memory_serving_admission(&state, admission)?;
        validate_lease_serving_admission(lease, admission)?;
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        let authority = state
            .metadata_authorities
            .get(&lease.logical_shard_id)
            .ok_or(ControlError::MetadataAuthorityNotFound(
                lease.logical_shard_id,
            ))?;
        validate_authority_for_owner_operation(authority, Some(lease))?;
        validate_in_memory_owner_session(&state, lease)?;
        let next = prepare_mark_serving(&current, lease, publication)?;
        state
            .logical_shards
            .insert(lease.logical_shard_id, next.clone());
        Ok(next)
    }

    fn release_owner(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<OwnerReleaseOutcome, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        let exact_record = current.owner.as_ref() == Some(&lease.owner)
            && current.owner_epoch == Some(lease.owner_epoch)
            && current.owner_incarnation_id == Some(lease.owner_incarnation_id)
            && current.lease_id == lease.lease_id;
        let exact_session = state.owner_sessions.get(&lease.logical_shard_id) == Some(lease);
        if !exact_record {
            if exact_session {
                return Err(ControlError::InvalidRecord(
                    "owner session remains exact after its shard record changed".to_owned(),
                ));
            }
            if current.owner.is_none()
                && current.owner_epoch == Some(lease.owner_epoch)
                && current.owner_incarnation_id == Some(lease.owner_incarnation_id)
                && current.lease_id == 0
            {
                return Ok(OwnerReleaseOutcome::AlreadyReleased(current));
            }
            if current.owner_epoch == Some(lease.owner_epoch)
                && current.owner_incarnation_id != Some(lease.owner_incarnation_id)
            {
                return Err(ControlError::InvalidRecord(
                    "one owner epoch cannot identify different installed incarnations".to_owned(),
                ));
            }
            if current
                .owner_epoch
                .is_some_and(|epoch| epoch.get() > lease.owner_epoch.get())
            {
                return Ok(OwnerReleaseOutcome::Superseded(current));
            }
            return Err(ControlError::StaleLease(lease.clone()));
        }
        if !exact_session {
            return Err(ControlError::InvalidRecord(
                "exact owner record has no matching non-expiring owner session".to_owned(),
            ));
        }
        let next = prepare_owner_release(&current, lease)?;
        state
            .logical_shards
            .insert(lease.logical_shard_id, next.clone());
        state.owner_sessions.remove(&lease.logical_shard_id);
        Ok(OwnerReleaseOutcome::Released(next))
    }
}

fn owner_admission_claim_key(
    logical_shard_id: LogicalShardId,
    owner_incarnation_id: OwnerIncarnationId,
) -> (LogicalShardId, OwnerIncarnationId) {
    (logical_shard_id, owner_incarnation_id)
}

fn in_memory_owner_admission_snapshot(
    state: &InMemoryState,
    intent: &OwnerAdmissionIntentV1,
) -> OwnerAdmissionExactSnapshot {
    let logical_shard_id = intent.logical_shard_id();
    let candidate_key = owner_admission_claim_key(logical_shard_id, intent.owner_incarnation_id());
    let previous_claim = intent.expected_previous_claim().and_then(|expected| {
        let identity = expected.identity();
        state
            .owner_admission_claims
            .get(&owner_admission_claim_key(
                identity.logical_shard_id(),
                identity.owner_incarnation_id(),
            ))
            .cloned()
    });
    OwnerAdmissionExactSnapshot::new(
        state.logical_shards.get(&logical_shard_id).cloned(),
        state
            .root_placements
            .get(&intent.admission().placement().root_id)
            .cloned(),
        state.metadata_authorities.get(&logical_shard_id).cloned(),
        state.owner_sessions.get(&logical_shard_id).cloned(),
        None,
        state.owner_admission_plans.get(&logical_shard_id).cloned(),
        state
            .owner_admission_sentinels
            .get(&logical_shard_id)
            .cloned(),
        None,
        state.owner_admission_claims.get(&candidate_key).cloned(),
        previous_claim,
    )
}

fn execute_in_memory_publish_owner_serving(
    state: &mut InMemoryState,
    publication: &PlannedOwnerServingPublicationV1,
    claimed: &crate::owner_admission_command::ClaimedPublishOwnerServingCommandV1,
) -> PublishOwnerServingResultV1 {
    let snapshot = in_memory_owner_admission_snapshot(state, publication.plan().intent());
    match plan_owner_serving_publication(publication, &snapshot) {
        OwnerServingPublicationStateDecision::Mutation(mutation) => {
            let logical_shard_id = publication.plan().intent().logical_shard_id();
            let candidate_key = owner_admission_claim_key(
                logical_shard_id,
                publication.plan().intent().owner_incarnation_id(),
            );
            if snapshot.logical_shard_record.as_ref() != Some(&mutation.expected_shard)
                || snapshot.session.as_ref() != Some(&mutation.expected_session)
                || snapshot.candidate_claim.as_ref() != Some(&mutation.expected_claim)
                || snapshot.active_plan != mutation.expected_active_plan
                || snapshot.sentinel != mutation.expected_sentinel
                || mutation.next_shard != *publication.target()
                || state.owner_admission_claims.get(&candidate_key)
                    != Some(&mutation.expected_claim)
            {
                return PublishOwnerServingResultV1::NotDispatched(
                    PublishOwnerServingNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
                );
            }

            let lifetime = match claimed.non_expiring_lifetime_observation(
                &mutation.next_shard,
                &mutation.expected_claim,
                &mutation.expected_session,
            ) {
                Ok(lifetime) => lifetime,
                Err(_) => {
                    return PublishOwnerServingResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
                    );
                }
            };
            state
                .logical_shards
                .insert(logical_shard_id, mutation.next_shard.clone());
            PublishOwnerServingResultV1::Published {
                shard: mutation.next_shard,
                claim: mutation.expected_claim,
                lifetime,
            }
        }
        OwnerServingPublicationStateDecision::AlreadyPublished {
            record,
            claim,
            session,
        } => match claimed.non_expiring_lifetime_observation(&record, &claim, &session) {
            Ok(lifetime) => PublishOwnerServingResultV1::AlreadyPublished {
                shard: record,
                claim,
                lifetime,
            },
            Err(_) => PublishOwnerServingResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ),
        },
        OwnerServingPublicationStateDecision::PublicationConflict { record, claim, .. } => {
            PublishOwnerServingResultV1::PublicationConflict {
                shard: record,
                claim,
            }
        }
        OwnerServingPublicationStateDecision::ExpiredCommitted {
            record,
            claim,
            expected_session,
            evidence_digest,
        } => PublishOwnerServingResultV1::ExpiredCommitted {
            shard: record,
            claim,
            expected_session,
            evidence_digest,
        },
        OwnerServingPublicationStateDecision::Terminated { record, claim } => {
            PublishOwnerServingResultV1::Terminated {
                shard: record,
                claim,
            }
        }
        OwnerServingPublicationStateDecision::Superseded { record, claim } => {
            PublishOwnerServingResultV1::Superseded {
                shard: record,
                claim,
            }
        }
        OwnerServingPublicationStateDecision::DurableConflict { record, claim } => {
            PublishOwnerServingResultV1::DurableConflict {
                shard: record,
                claim,
            }
        }
        OwnerServingPublicationStateDecision::Blocked(_) => {
            PublishOwnerServingResultV1::NotDispatched(
                PublishOwnerServingNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
            )
        }
        OwnerServingPublicationStateDecision::Inconsistent(code) => {
            PublishOwnerServingResultV1::DurableInconsistent(code)
        }
    }
}

fn execute_in_memory_renew_owner_session(
    state: &InMemoryState,
    target: &OwnerSessionRenewalTargetV1,
    claimed: &crate::owner_admission_command::ClaimedRenewOwnerSessionCommandV1,
) -> RenewOwnerSessionResultV1 {
    let snapshot = in_memory_owner_admission_snapshot(state, target.plan().intent());
    match classify_owner_session_renewal(target, &snapshot) {
        OwnerSessionRenewalStateDecision::Current {
            record,
            claim,
            session,
        } => match claimed.non_expiring_lifetime_observation(&record, &claim, &session) {
            Ok(lifetime) => RenewOwnerSessionResultV1::Current {
                shard: record,
                claim,
                lifetime,
            },
            Err(_) => RenewOwnerSessionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ),
        },
        OwnerSessionRenewalStateDecision::ExpiredCommitted {
            record,
            claim,
            expected_session,
            evidence_digest,
        } => RenewOwnerSessionResultV1::ExpiredCommitted {
            shard: record,
            claim,
            expected_session,
            evidence_digest,
        },
        OwnerSessionRenewalStateDecision::Terminated { record, claim } => {
            RenewOwnerSessionResultV1::Terminated {
                shard: record,
                claim,
            }
        }
        OwnerSessionRenewalStateDecision::Superseded { record, claim } => {
            RenewOwnerSessionResultV1::Superseded {
                shard: record,
                claim,
            }
        }
        OwnerSessionRenewalStateDecision::DurableConflict { record, claim } => {
            RenewOwnerSessionResultV1::DurableConflict {
                shard: record,
                claim,
            }
        }
        OwnerSessionRenewalStateDecision::Blocked(_) => RenewOwnerSessionResultV1::NotDispatched(
            RenewOwnerSessionNotDispatchedV1::ExactSessionBindingLostBeforeEffect,
        ),
        OwnerSessionRenewalStateDecision::Inconsistent(code) => {
            RenewOwnerSessionResultV1::DurableInconsistent(code)
        }
    }
}

fn plan_in_memory_owner_admission(
    state: &InMemoryState,
    intent: &OwnerAdmissionIntentV1,
) -> Result<PlannedOwnerAdmissionV1, PrepareOwnerAdmissionNotDispatchedV1> {
    let lease_id = state
        .next_lease_id
        .checked_add(1)
        .filter(|lease_id| *lease_id != 0)
        .ok_or(PrepareOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect)?;
    let lease = LogicalShardLease {
        logical_shard_id: intent.logical_shard_id(),
        owner: intent.owner().clone(),
        owner_epoch: intent.planned_epoch(),
        owner_incarnation_id: intent.owner_incarnation_id(),
        lease_id,
        authority: intent.admission().authority().fence(),
    };
    PlannedOwnerAdmissionV1::new(intent.clone(), lease)
        .map_err(|_| PrepareOwnerAdmissionNotDispatchedV1::InvalidInputBeforeEffect)
}

fn reconstruct_plan_from_classified_claim(
    intent: &OwnerAdmissionIntentV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<PlannedOwnerAdmissionV1, OwnerAdmissionInconsistencyCode> {
    let (lease, stored_plan_digest) = match claim.phase() {
        OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Aborted {
            lease, plan_digest, ..
        }
        | OwnerAdmissionClaimPhaseV1::Terminated {
            lease, plan_digest, ..
        } => (lease, *plan_digest),
        OwnerAdmissionClaimPhaseV1::Rejected { .. } => {
            return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
        }
    };
    let plan = PlannedOwnerAdmissionV1::new(intent.clone(), lease.clone())
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
    if plan.digest() != stored_plan_digest {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    }
    Ok(plan)
}

fn execute_in_memory_prepare_owner_admission(
    state: &mut InMemoryState,
    intent: &OwnerAdmissionIntentV1,
) -> PrepareOwnerAdmissionResultV1 {
    let snapshot = in_memory_owner_admission_snapshot(state, intent);
    match reconcile_owner_admission_intent(intent, &snapshot) {
        OwnerAdmissionReconcileDecision::Classified(classification)
            if matches!(
                classification.as_ref(),
                OwnerAdmissionReconcileClassification::NotStarted
            ) => {}
        OwnerAdmissionReconcileDecision::Classified(classification) => {
            return in_memory_prepare_result_from_classification(intent, &snapshot, classification);
        }
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            return PrepareOwnerAdmissionResultV1::DurableInconsistent(code);
        }
    }

    let plan = match plan_in_memory_owner_admission(state, intent) {
        Ok(plan) => plan,
        Err(code) => return PrepareOwnerAdmissionResultV1::NotDispatched(code),
    };
    match plan_owner_admission_prepare(&plan, &snapshot) {
        OwnerAdmissionStateDecision::Mutation(mutation) => {
            apply_in_memory_prepare_mutation(state, intent, plan.lease().lease_id, mutation)
                .unwrap_or(PrepareOwnerAdmissionResultV1::NotDispatched(
                    PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
                ))
        }
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            in_memory_prepare_result_from_classification(intent, &snapshot, classification)
        }
        OwnerAdmissionStateDecision::Blocked(_) => PrepareOwnerAdmissionResultV1::NotDispatched(
            PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
        ),
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            PrepareOwnerAdmissionResultV1::DurableInconsistent(code)
        }
    }
}

fn apply_in_memory_prepare_mutation(
    state: &mut InMemoryState,
    intent: &OwnerAdmissionIntentV1,
    expected_lease_id: u64,
    mutation: OwnerAdmissionMutationPlan,
) -> Option<PrepareOwnerAdmissionResultV1> {
    let logical_shard_id = intent.logical_shard_id();
    let candidate_key = owner_admission_claim_key(logical_shard_id, intent.owner_incarnation_id());
    match mutation {
        OwnerAdmissionMutationPlan::Prepare(mutation) => {
            let expected_snapshot = OwnerAdmissionExactSnapshot::new(
                Some(mutation.expected_shard.clone()),
                Some(mutation.expected_placement.clone()),
                Some(mutation.expected_authority.clone()),
                mutation.expected_session.clone(),
                None,
                mutation.expected_active_plan.clone(),
                mutation.expected_sentinel.clone(),
                None,
                mutation.expected_candidate_claim.clone(),
                mutation.expected_previous_claim.clone(),
            );
            if in_memory_owner_admission_snapshot(state, intent) != expected_snapshot
                || mutation.next_plan.intent() != intent
                || mutation.next_plan.lease().lease_id != expected_lease_id
                || state.next_lease_id.checked_add(1) != Some(expected_lease_id)
            {
                return None;
            }
            let result = PrepareOwnerAdmissionResultV1::Prepared {
                plan: Box::new(mutation.next_plan.clone()),
                claim: mutation.next_claim.clone(),
                sentinel: mutation.next_sentinel.clone(),
            };
            state.next_lease_id = expected_lease_id;
            state
                .owner_admission_claims
                .insert(candidate_key, mutation.next_claim);
            state
                .owner_admission_plans
                .insert(logical_shard_id, mutation.next_plan);
            state
                .owner_admission_sentinels
                .insert(logical_shard_id, mutation.next_sentinel);
            Some(result)
        }
        OwnerAdmissionMutationPlan::Reject(mutation) => {
            if in_memory_owner_admission_snapshot(state, intent) != mutation.expected_snapshot {
                return None;
            }
            let result = PrepareOwnerAdmissionResultV1::Rejected {
                claim: mutation.next_claim.clone(),
            };
            state
                .owner_admission_claims
                .insert(candidate_key, mutation.next_claim);
            Some(result)
        }
        _ => None,
    }
}

fn in_memory_prepare_result_from_classification(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    classification: Box<OwnerAdmissionReconcileClassification>,
) -> PrepareOwnerAdmissionResultV1 {
    match *classification {
        OwnerAdmissionReconcileClassification::NotStarted => {
            PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            )
        }
        OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => {
            snapshot.candidate_claim.clone().map_or_else(
                || {
                    PrepareOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch,
                    )
                },
                |claim| PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim },
            )
        }
        OwnerAdmissionReconcileClassification::Prepared {
            claim,
            plan,
            sentinel,
        } => PrepareOwnerAdmissionResultV1::Prepared {
            plan: Box::new(plan),
            claim,
            sentinel,
        },
        OwnerAdmissionReconcileClassification::ExpiredPrepared {
            claim: _,
            plan: _,
            expected_sentinel: _,
        } => PrepareOwnerAdmissionResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven,
        ),
        OwnerAdmissionReconcileClassification::Rejected { claim, .. } => {
            PrepareOwnerAdmissionResultV1::Rejected { claim }
        }
        OwnerAdmissionReconcileClassification::Committed { claim, .. }
        | OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            match reconstruct_plan_from_classified_claim(intent, &claim) {
                Ok(plan) => PrepareOwnerAdmissionResultV1::DurableConflict {
                    plan: Box::new(plan),
                    claim,
                },
                Err(code) => PrepareOwnerAdmissionResultV1::DurableInconsistent(code),
            }
        }
        OwnerAdmissionReconcileClassification::ExpiredCommitted { .. } => {
            PrepareOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
            )
        }
    }
}

fn execute_in_memory_commit_owner_admission(
    state: &mut InMemoryState,
    plan: &PlannedOwnerAdmissionV1,
) -> CommitOwnerAdmissionResultV1 {
    let snapshot = in_memory_owner_admission_snapshot(state, plan.intent());
    match plan_owner_admission_commit(plan, &snapshot) {
        OwnerAdmissionStateDecision::Mutation(mutation) => apply_in_memory_commit_mutation(
            state, plan, mutation,
        )
        .unwrap_or(CommitOwnerAdmissionResultV1::NotDispatched(
            CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
        )),
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            in_memory_commit_result_from_classification(classification)
        }
        OwnerAdmissionStateDecision::Blocked(_) => CommitOwnerAdmissionResultV1::NotDispatched(
            CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
        ),
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            CommitOwnerAdmissionResultV1::DurableInconsistent(code)
        }
    }
}

fn apply_in_memory_commit_mutation(
    state: &mut InMemoryState,
    plan: &PlannedOwnerAdmissionV1,
    mutation: OwnerAdmissionMutationPlan,
) -> Option<CommitOwnerAdmissionResultV1> {
    let OwnerAdmissionMutationPlan::Commit(mutation) = mutation else {
        return None;
    };
    let expected_snapshot = OwnerAdmissionExactSnapshot::new(
        Some(mutation.expected_shard.clone()),
        Some(mutation.expected_placement.clone()),
        Some(mutation.expected_authority.clone()),
        mutation.expected_session.clone(),
        None,
        Some(mutation.expected_plan.clone()),
        Some(mutation.expected_sentinel.clone()),
        None,
        Some(mutation.expected_claim.clone()),
        mutation.expected_previous_claim.clone(),
    );
    if in_memory_owner_admission_snapshot(state, plan.intent()) != expected_snapshot
        || mutation.delete_plan != mutation.expected_plan
        || mutation.delete_sentinel != mutation.expected_sentinel
    {
        return None;
    }

    let logical_shard_id = plan.intent().logical_shard_id();
    let candidate_key =
        owner_admission_claim_key(logical_shard_id, plan.intent().owner_incarnation_id());
    let result = CommitOwnerAdmissionResultV1::Committed {
        shard: mutation.next_shard.clone(),
        lease: mutation.next_session.clone(),
        claim: mutation.next_claim.clone(),
        lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
    };
    state
        .logical_shards
        .insert(logical_shard_id, mutation.next_shard);
    state
        .owner_sessions
        .insert(logical_shard_id, mutation.next_session);
    state
        .owner_admission_claims
        .insert(candidate_key, mutation.next_claim);
    state.owner_admission_plans.remove(&logical_shard_id);
    state.owner_admission_sentinels.remove(&logical_shard_id);
    Some(result)
}

fn in_memory_commit_result_from_classification(
    classification: Box<OwnerAdmissionReconcileClassification>,
) -> CommitOwnerAdmissionResultV1 {
    match *classification {
        OwnerAdmissionReconcileClassification::Committed {
            claim,
            record,
            session,
        } => CommitOwnerAdmissionResultV1::AlreadyCommitted {
            shard: record,
            lease: session,
            claim,
            lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
        },
        OwnerAdmissionReconcileClassification::ExpiredCommitted { .. } => {
            CommitOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
            )
        }
        OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            CommitOwnerAdmissionResultV1::DurableConflict { claim }
        }
        _ => CommitOwnerAdmissionResultV1::NotDispatched(
            CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
        ),
    }
}

fn execute_in_memory_abort_owner_admission(
    state: &mut InMemoryState,
    plan: &PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionAbortReasonV1,
) -> AbortOwnerAdmissionResultV1 {
    let snapshot = in_memory_owner_admission_snapshot(state, plan.intent());
    match plan_owner_admission_abort(plan, &snapshot, reason) {
        OwnerAdmissionStateDecision::Mutation(mutation) => apply_in_memory_abort_mutation(
            state, plan, mutation,
        )
        .unwrap_or(AbortOwnerAdmissionResultV1::NotDispatched(
            AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
        )),
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            in_memory_abort_result_from_classification(classification)
        }
        OwnerAdmissionStateDecision::Blocked(_) => AbortOwnerAdmissionResultV1::NotDispatched(
            AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
        ),
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            AbortOwnerAdmissionResultV1::DurableInconsistent(code)
        }
    }
}

fn apply_in_memory_abort_mutation(
    state: &mut InMemoryState,
    plan: &PlannedOwnerAdmissionV1,
    mutation: OwnerAdmissionMutationPlan,
) -> Option<AbortOwnerAdmissionResultV1> {
    let OwnerAdmissionMutationPlan::Abort(mutation) = mutation else {
        return None;
    };
    let logical_shard_id = plan.intent().logical_shard_id();
    let candidate_key =
        owner_admission_claim_key(logical_shard_id, plan.intent().owner_incarnation_id());
    let sentinel_matches = match &mutation.expected_sentinel {
        OwnerAdmissionSentinelExpectation::Exact(expected) => {
            state.owner_admission_sentinels.get(&logical_shard_id) == Some(expected)
        }
        OwnerAdmissionSentinelExpectation::AuthoritativelyAbsent { expected } => {
            !state
                .owner_admission_sentinels
                .contains_key(&logical_shard_id)
                && expected == &mutation.delete_sentinel
        }
    };
    if state.logical_shards.get(&logical_shard_id) != Some(&mutation.expected_shard)
        || state.owner_sessions.get(&logical_shard_id) != mutation.expected_session.as_ref()
        || state.owner_admission_claims.get(&candidate_key) != Some(&mutation.expected_claim)
        || state.owner_admission_plans.get(&logical_shard_id) != Some(&mutation.expected_plan)
        || !sentinel_matches
        || mutation.delete_plan != mutation.expected_plan
    {
        return None;
    }

    let result = AbortOwnerAdmissionResultV1::Aborted {
        claim: mutation.next_claim.clone(),
    };
    state
        .owner_admission_claims
        .insert(candidate_key, mutation.next_claim);
    state.owner_admission_plans.remove(&logical_shard_id);
    state.owner_admission_sentinels.remove(&logical_shard_id);
    Some(result)
}

fn in_memory_abort_result_from_classification(
    classification: Box<OwnerAdmissionReconcileClassification>,
) -> AbortOwnerAdmissionResultV1 {
    match *classification {
        OwnerAdmissionReconcileClassification::Committed { claim, .. }
        | OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            AbortOwnerAdmissionResultV1::DurableConflict { claim }
        }
        OwnerAdmissionReconcileClassification::ExpiredCommitted { .. } => {
            AbortOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
            )
        }
        _ => AbortOwnerAdmissionResultV1::NotDispatched(
            AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
        ),
    }
}

fn execute_in_memory_terminate_owner_admission(
    state: &mut InMemoryState,
    plan: &PlannedOwnerAdmissionV1,
    reason: OwnerAdmissionTerminationReasonV1,
) -> TerminateOwnerAdmissionResultV1 {
    let snapshot = in_memory_owner_admission_snapshot(state, plan.intent());
    match plan_owner_admission_terminate(plan, &snapshot, reason.clone()) {
        OwnerAdmissionStateDecision::Mutation(mutation) => apply_in_memory_terminate_mutation(
            state, plan, mutation,
        )
        .unwrap_or(TerminateOwnerAdmissionResultV1::NotDispatched(
            TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
        )),
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            in_memory_terminate_result_from_classification(plan, &reason, &snapshot, classification)
        }
        OwnerAdmissionStateDecision::Blocked(_) => TerminateOwnerAdmissionResultV1::NotDispatched(
            TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
        ),
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            TerminateOwnerAdmissionResultV1::DurableInconsistent(code)
        }
    }
}

fn apply_in_memory_terminate_mutation(
    state: &mut InMemoryState,
    plan: &PlannedOwnerAdmissionV1,
    mutation: OwnerAdmissionMutationPlan,
) -> Option<TerminateOwnerAdmissionResultV1> {
    let OwnerAdmissionMutationPlan::Terminate(mutation) = mutation else {
        return None;
    };
    let logical_shard_id = plan.intent().logical_shard_id();
    let candidate_key =
        owner_admission_claim_key(logical_shard_id, plan.intent().owner_incarnation_id());
    let OwnerAdmissionSessionExpectation::Exact(expected_session) = &mutation.expected_session
    else {
        return None;
    };
    if state.logical_shards.get(&logical_shard_id) != Some(&mutation.expected_shard)
        || state.owner_sessions.get(&logical_shard_id) != Some(expected_session)
        || mutation.delete_session.as_ref() != Some(expected_session)
        || state.owner_admission_claims.get(&candidate_key) != Some(&mutation.expected_claim)
        || state.owner_admission_plans.get(&logical_shard_id)
            != mutation.expected_active_plan.as_ref()
        || state.owner_admission_sentinels.get(&logical_shard_id)
            != mutation.expected_sentinel.as_ref()
    {
        return None;
    }
    if let Err(code) = validate_terminated_record_descendant(plan, &mutation.next_shard) {
        return Some(TerminateOwnerAdmissionResultV1::DurableInconsistent(code));
    }

    let result = TerminateOwnerAdmissionResultV1::Terminated {
        shard: mutation.next_shard.clone(),
        claim: mutation.next_claim.clone(),
    };
    state
        .logical_shards
        .insert(logical_shard_id, mutation.next_shard);
    state.owner_sessions.remove(&logical_shard_id);
    state
        .owner_admission_claims
        .insert(candidate_key, mutation.next_claim);
    Some(result)
}

fn in_memory_terminate_result_from_classification(
    plan: &PlannedOwnerAdmissionV1,
    requested_reason: &OwnerAdmissionTerminationReasonV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    classification: Box<OwnerAdmissionReconcileClassification>,
) -> TerminateOwnerAdmissionResultV1 {
    match *classification {
        OwnerAdmissionReconcileClassification::Terminated { claim, reason } => {
            if &reason != requested_reason {
                return TerminateOwnerAdmissionResultV1::DurableConflict { claim };
            }
            let Some(record) = snapshot.logical_shard_record.as_ref() else {
                return TerminateOwnerAdmissionResultV1::DurableInconsistent(
                    OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant,
                );
            };
            if record
                .owner_epoch
                .is_some_and(|epoch| epoch.get() > plan.lease().owner_epoch.get())
            {
                if record.logical_shard_id != plan.intent().logical_shard_id()
                    || validate_logical_shard_record(record).is_err()
                {
                    return TerminateOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::SupersedingRecordInvalid,
                    );
                }
                return TerminateOwnerAdmissionResultV1::Superseded {
                    shard: record.clone(),
                    claim,
                };
            }
            if let Err(code) = validate_terminated_record_descendant(plan, record) {
                return TerminateOwnerAdmissionResultV1::DurableInconsistent(code);
            }
            TerminateOwnerAdmissionResultV1::AlreadyTerminated {
                shard: record.clone(),
                claim,
            }
        }
        OwnerAdmissionReconcileClassification::Aborted { claim, .. } => {
            TerminateOwnerAdmissionResultV1::DurableConflict { claim }
        }
        OwnerAdmissionReconcileClassification::ExpiredCommitted { .. } => {
            TerminateOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
            )
        }
        _ => TerminateOwnerAdmissionResultV1::NotDispatched(
            TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
        ),
    }
}

fn execute_in_memory_reconcile_owner_admission(
    state: &mut InMemoryState,
    target: &ReconcileOwnerAdmissionTargetV1,
) -> ReconcileOwnerAdmissionResultV1 {
    let intent = match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => intent,
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => plan.intent(),
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => publication.plan().intent(),
    };
    let snapshot = in_memory_owner_admission_snapshot(state, intent);
    let decision = match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            reconcile_owner_admission_intent(intent, &snapshot)
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => {
            reconcile_owner_admission(plan, &snapshot)
        }
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            reconcile_owner_admission(publication.plan(), &snapshot)
        }
    };

    if matches!(target, ReconcileOwnerAdmissionTargetV1::IntentOnly(_))
        && matches!(
            &decision,
            OwnerAdmissionReconcileDecision::Classified(classification)
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::NotStarted
                )
        )
    {
        return seal_and_reconcile_in_memory_owner_admission(state, target, intent, &snapshot);
    }
    in_memory_reconcile_result_from_decision(target, &snapshot, decision)
}

fn seal_and_reconcile_in_memory_owner_admission(
    state: &mut InMemoryState,
    target: &ReconcileOwnerAdmissionTargetV1,
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> ReconcileOwnerAdmissionResultV1 {
    match plan_owner_admission_intent_seal_rejected(intent, snapshot) {
        OwnerAdmissionStateDecision::Mutation(mutation) => {
            let _applied = apply_in_memory_seal_rejected_mutation(state, intent, mutation);
        }
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            return in_memory_reconcile_result_from_classification(
                target,
                snapshot,
                classification,
            );
        }
        OwnerAdmissionStateDecision::Blocked(_) => {
            return ReconcileOwnerAdmissionResultV1::NotDispatched(
                ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            );
        }
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            return ReconcileOwnerAdmissionResultV1::DurableInconsistent(code);
        }
    }

    let refreshed = in_memory_owner_admission_snapshot(state, intent);
    let decision = reconcile_owner_admission_intent(intent, &refreshed);
    in_memory_reconcile_result_from_decision(target, &refreshed, decision)
}

fn apply_in_memory_seal_rejected_mutation(
    state: &mut InMemoryState,
    intent: &OwnerAdmissionIntentV1,
    mutation: OwnerAdmissionMutationPlan,
) -> bool {
    let OwnerAdmissionMutationPlan::SealRejected(mutation) = mutation else {
        return false;
    };
    let candidate_key =
        owner_admission_claim_key(mutation.logical_shard_id, mutation.owner_incarnation_id);
    if mutation.logical_shard_id != intent.logical_shard_id()
        || mutation.owner_incarnation_id != intent.owner_incarnation_id()
        || state.owner_admission_claims.get(&candidate_key)
            != mutation.expected_candidate_claim.as_ref()
    {
        return false;
    }
    state
        .owner_admission_claims
        .insert(candidate_key, mutation.next_claim);
    true
}

fn in_memory_reconcile_result_from_decision(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    decision: OwnerAdmissionReconcileDecision,
) -> ReconcileOwnerAdmissionResultV1 {
    match decision {
        OwnerAdmissionReconcileDecision::Classified(classification) => {
            in_memory_reconcile_result_from_classification(target, snapshot, classification)
        }
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(code)
        }
    }
}

fn in_memory_reconcile_result_from_classification(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    classification: Box<OwnerAdmissionReconcileClassification>,
) -> ReconcileOwnerAdmissionResultV1 {
    match *classification {
        OwnerAdmissionReconcileClassification::NotStarted => {
            ReconcileOwnerAdmissionResultV1::NotDispatched(
                ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            )
        }
        OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => {
            snapshot.candidate_claim.clone().map_or_else(
                || {
                    ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch,
                    )
                },
                |claim| ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim },
            )
        }
        OwnerAdmissionReconcileClassification::Rejected { claim, .. } => {
            if matches!(target, ReconcileOwnerAdmissionTargetV1::IntentOnly(_)) {
                ReconcileOwnerAdmissionResultV1::Rejected { claim }
            } else {
                ReconcileOwnerAdmissionResultV1::NotDispatched(
                    ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
                )
            }
        }
        OwnerAdmissionReconcileClassification::Prepared {
            claim,
            plan,
            sentinel,
        } => ReconcileOwnerAdmissionResultV1::Prepared {
            plan: Box::new(plan),
            claim,
            sentinel,
        },
        OwnerAdmissionReconcileClassification::ExpiredPrepared {
            claim: _,
            plan: _,
            expected_sentinel: _,
        } => ReconcileOwnerAdmissionResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven,
        ),
        OwnerAdmissionReconcileClassification::Committed {
            claim,
            record,
            session,
        } => match plan_for_reconcile_target(target, &claim) {
            Ok(plan) => ReconcileOwnerAdmissionResultV1::Committed {
                plan: Box::new(plan),
                shard: record,
                lease: session,
                claim: Box::new(claim),
                lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
            },
            Err(code) => ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
        },
        OwnerAdmissionReconcileClassification::ExpiredCommitted { .. } => {
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven,
            )
        }
        OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            match plan_for_reconcile_target(target, &claim) {
                Ok(plan) => ReconcileOwnerAdmissionResultV1::DurableConflict {
                    plan: Box::new(plan),
                    claim,
                },
                Err(code) => ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
            }
        }
    }
}

fn plan_for_reconcile_target(
    target: &ReconcileOwnerAdmissionTargetV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<PlannedOwnerAdmissionV1, OwnerAdmissionInconsistencyCode> {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            reconstruct_plan_from_classified_claim(intent, claim)
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => Ok(plan.clone()),
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            Ok(publication.plan().clone())
        }
    }
}

fn validate_in_memory_serving_admission(
    state: &InMemoryState,
    admission: &OwnerServingAdmission,
) -> Result<(), ControlError> {
    let actual_placement = state
        .root_placements
        .get(&admission.placement().root_id)
        .cloned();
    if actual_placement.as_ref() != Some(admission.placement()) {
        return Err(ControlError::RootPlacementCasConflict {
            expected: Box::new(admission.placement().clone()),
            actual: actual_placement.map(Box::new),
        });
    }
    let actual_authority = state
        .metadata_authorities
        .get(&admission.logical_shard_id())
        .cloned();
    if actual_authority.as_ref() != Some(admission.authority()) {
        return Err(ControlError::MetadataAuthorityCasConflict {
            expected: Box::new(admission.authority().clone()),
            actual: actual_authority.map(Box::new),
        });
    }
    Ok(())
}

pub(crate) fn validate_lease_serving_admission(
    lease: &LogicalShardLease,
    admission: &OwnerServingAdmission,
) -> Result<(), ControlError> {
    if lease.logical_shard_id != admission.logical_shard_id()
        || lease.authority != admission.authority().fence()
    {
        return Err(ControlError::MetadataAuthorityAdmission {
            logical_shard_id: lease.logical_shard_id,
            reason: "owner lease does not match the exact root Serving admission".to_owned(),
        });
    }
    Ok(())
}

fn validate_in_memory_owner_session(
    state: &InMemoryState,
    lease: &LogicalShardLease,
) -> Result<(), ControlError> {
    if state.owner_sessions.get(&lease.logical_shard_id) == Some(lease) {
        Ok(())
    } else {
        Err(ControlError::StaleLease(lease.clone()))
    }
}

pub(crate) fn validate_fresh_authority_shard(
    record: &LogicalShardRecord,
) -> Result<(), ControlError> {
    validate_logical_shard_record(record)?;
    if record == &LogicalShardRecord::unassigned(record.logical_shard_id) {
        Ok(())
    } else {
        Err(ControlError::MetadataAuthorityAdoptionRejected {
            logical_shard_id: record.logical_shard_id,
            reason: "only a never-owned Unassigned shard with no recovery state may install its first authority"
                .to_owned(),
        })
    }
}

fn allocate_lease_id(
    state: &mut InMemoryState,
    logical_shard_id: LogicalShardId,
) -> Result<u64, ControlError> {
    state.next_lease_id = state
        .next_lease_id
        .checked_add(1)
        .ok_or(ControlError::LeaseIdExhausted(logical_shard_id))?;
    Ok(state.next_lease_id)
}

pub(crate) fn validate_fresh_root_provisioning_input(
    initial_placement: &RootPlacement,
    initial_authority: &MetadataAuthorityRecord,
) -> Result<(), ControlError> {
    validate_new_root_placement(initial_placement)?;
    validate_new_metadata_authority(initial_authority)?;
    if initial_placement.logical_shard_id != initial_authority.logical_shard_id {
        return Err(ControlError::InvalidFreshRootProvisioning {
            root_id: initial_placement.root_id,
            reason: "root placement and metadata authority logical shard ids differ".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn classify_fresh_root_provisioning_replay(
    initial_placement: &RootPlacement,
    initial_authority: &MetadataAuthorityRecord,
    actual_shard: Option<&LogicalShardRecord>,
    actual_authority: Option<&MetadataAuthorityRecord>,
    actual_placement: Option<&RootPlacement>,
    actual_session: Option<&LogicalShardLease>,
) -> Result<FreshRootProvisioningOutcome, ControlError> {
    let (Some(actual_shard), Some(actual_authority), Some(actual_placement)) =
        (actual_shard, actual_authority, actual_placement)
    else {
        return Err(fresh_root_provisioning_conflict(
            initial_placement,
            format!(
                "partial bundle: shard={}, authority={}, placement={}, session={}",
                actual_shard.is_some(),
                actual_authority.is_some(),
                actual_placement.is_some(),
                actual_session.is_some()
            ),
        ));
    };

    validate_logical_shard_record(actual_shard).map_err(|error| {
        fresh_root_provisioning_conflict(
            initial_placement,
            format!("stored logical shard is invalid: {error}"),
        )
    })?;
    if actual_shard.logical_shard_id != initial_placement.logical_shard_id {
        return Err(fresh_root_provisioning_conflict(
            initial_placement,
            "stored logical shard identity differs",
        ));
    }
    if actual_authority != initial_authority {
        return Err(fresh_root_provisioning_conflict(
            initial_placement,
            "metadata authority is missing, advanced, or different",
        ));
    }
    let placement_is_exact = actual_placement == initial_placement;
    let placement_is_activated = actual_placement.lifecycle == RootPlacementLifecycle::Active
        && validate_root_placement_update(initial_placement, actual_placement).is_ok();
    if !placement_is_exact && !placement_is_activated {
        return Err(fresh_root_provisioning_conflict(
            initial_placement,
            "root placement is missing, has different affinity, or is not the legal gen2 Active successor",
        ));
    }

    if let Some(session) = actual_session {
        validate_record_lease(actual_shard, session).map_err(|error| {
            fresh_root_provisioning_conflict(
                initial_placement,
                format!("owner session and logical shard differ: {error}"),
            )
        })?;
        validate_authority_for_owner_operation(actual_authority, Some(session)).map_err(
            |error| {
                fresh_root_provisioning_conflict(
                    initial_placement,
                    format!("owner session and metadata authority differ: {error}"),
                )
            },
        )?;
    }

    Ok(FreshRootProvisioningOutcome {
        disposition: FreshRootProvisioningDisposition::Replayed,
        logical_shard: actual_shard.clone(),
        metadata_authority: actual_authority.clone(),
        root_placement: actual_placement.clone(),
    })
}

fn fresh_root_provisioning_conflict(
    initial_placement: &RootPlacement,
    reason: impl Into<String>,
) -> ControlError {
    ControlError::FreshRootProvisioningConflict {
        root_id: initial_placement.root_id,
        logical_shard_id: initial_placement.logical_shard_id,
        reason: reason.into(),
    }
}

pub(crate) fn validate_new_metadata_authority(
    authority: &MetadataAuthorityRecord,
) -> Result<(), ControlError> {
    validate_metadata_authority_record(authority)?;
    if authority.authority_generation.get() != 1 {
        return Err(invalid_authority(
            authority.logical_shard_id,
            "initial metadata authority generation must be 1",
        ));
    }
    if authority.record_revision.get() != 1 {
        return Err(invalid_authority(
            authority.logical_shard_id,
            "initial metadata authority record revision must be 1",
        ));
    }
    if authority.migration.is_some() {
        return Err(invalid_authority(
            authority.logical_shard_id,
            "initial metadata authority must not contain a migration",
        ));
    }
    Ok(())
}

pub(crate) fn validate_metadata_authority_record(
    authority: &MetadataAuthorityRecord,
) -> Result<(), ControlError> {
    validate_authority_binding(authority.logical_shard_id, "active", &authority.active)?;
    let Some(migration) = authority.migration.as_ref() else {
        return Ok(());
    };

    if migration
        .migration_id
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(invalid_authority(
            authority.logical_shard_id,
            "migration id must not be all zeroes",
        ));
    }
    validate_authority_binding(authority.logical_shard_id, "source", &migration.source)?;
    validate_authority_binding(authority.logical_shard_id, "target", &migration.target)?;
    if migration.source.authority_id == migration.target.authority_id {
        return Err(invalid_authority(
            authority.logical_shard_id,
            "migration source and target authority ids must differ",
        ));
    }
    if migration.source.contract_digest != migration.target.contract_digest {
        return Err(invalid_authority(
            authority.logical_shard_id,
            "migration source and target must implement the same metadata contract",
        ));
    }

    for (name, frontier) in [
        ("source", migration.source_frontier.as_ref()),
        ("target", migration.target_frontier.as_ref()),
        ("cutover", migration.cutover_frontier.as_ref()),
    ] {
        if let Some(frontier) = frontier {
            validate_recovery_frontier(authority.logical_shard_id, name, frontier)?;
        }
    }

    let source_generation = if migration.phase == MetadataMigrationPhase::CutoverComplete {
        authority
            .authority_generation
            .get()
            .checked_sub(1)
            .and_then(|generation| MetadataAuthorityGeneration::new(generation).ok())
            .ok_or_else(|| {
                invalid_authority(
                    authority.logical_shard_id,
                    "completed cutover has no predecessor source authority generation",
                )
            })?
    } else {
        authority.authority_generation
    };
    if let Some(receipt) = migration.source_quiesce_receipt.as_ref() {
        validate_source_quiesce_receipt(
            authority.logical_shard_id,
            migration,
            source_generation,
            receipt,
        )?;
    }

    let expected_active = if migration.phase == MetadataMigrationPhase::CutoverComplete {
        &migration.target
    } else {
        &migration.source
    };
    if &authority.active != expected_active {
        return Err(invalid_authority(
            authority.logical_shard_id,
            format!(
                "active binding must equal the migration {} binding in phase {:?}",
                if migration.phase == MetadataMigrationPhase::CutoverComplete {
                    "target"
                } else {
                    "source"
                },
                migration.phase
            ),
        ));
    }

    match migration.phase {
        MetadataMigrationPhase::Preparing => {
            if migration.source_frontier.is_some()
                || migration.target_frontier.is_some()
                || migration.cutover_frontier.is_some()
                || migration.source_quiesce_receipt.is_some()
                || migration.target_activation_token.is_some()
            {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "Preparing migration must not publish recovery frontiers",
                ));
            }
        }
        MetadataMigrationPhase::Copying | MetadataMigrationPhase::CatchingUp => {
            if migration.cutover_frontier.is_some()
                || migration.source_quiesce_receipt.is_some()
                || migration.target_activation_token.is_some()
            {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "copy and catch-up phases must not publish a cutover frontier",
                ));
            }
        }
        MetadataMigrationPhase::Quiescing => {
            if migration.source_frontier.is_none()
                || migration.target_frontier.is_none()
                || migration.cutover_frontier.is_some()
                || migration.target_activation_token.is_some()
            {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "Quiescing requires source and target frontiers and no cutover frontier",
                ));
            }
            if let Some(receipt) = migration.source_quiesce_receipt {
                if migration.source_frontier != Some(receipt.frontier) {
                    return Err(invalid_authority(
                        authority.logical_shard_id,
                        "Quiescing source frontier must equal the exact provider receipt frontier",
                    ));
                }
            }
        }
        MetadataMigrationPhase::ReadyToCutover => {
            let Some(receipt) = migration.source_quiesce_receipt else {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "ReadyToCutover requires an exact provider source-quiesce receipt",
                ));
            };
            let Some(source) = migration.source_frontier else {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "cutover requires a source frontier",
                ));
            };
            if source != receipt.frontier
                || migration.target_frontier != Some(source)
                || migration.cutover_frontier != Some(source)
            {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "source, target, and cutover frontiers must match exactly at cutover",
                ));
            }
            if migration.target_activation_token.is_some() {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "ReadyToCutover must not publish a target activation token before cutover",
                ));
            }
        }
        MetadataMigrationPhase::CutoverComplete => {
            let Some(receipt) = migration.source_quiesce_receipt else {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "CutoverComplete requires the frozen source-quiesce receipt",
                ));
            };
            let Some(source) = migration.source_frontier else {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "completed cutover requires a source frontier",
                ));
            };
            if source != receipt.frontier
                || migration.target_frontier != Some(source)
                || migration.cutover_frontier != Some(source)
            {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "completed cutover frontiers must equal the frozen source receipt",
                ));
            }
            let expected_token = TargetActivationToken::for_cutover(
                &receipt,
                migration.target.authority_id,
                authority.authority_generation,
            );
            if migration.target_activation_token != Some(expected_token) {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "completed cutover requires the deterministic target activation token",
                ));
            }
        }
        MetadataMigrationPhase::Aborted => {
            if migration.cutover_frontier.is_some() || migration.target_activation_token.is_some() {
                return Err(invalid_authority(
                    authority.logical_shard_id,
                    "aborted migration must not publish a cutover frontier",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_metadata_authority_update(
    expected: &MetadataAuthorityRecord,
    next: &MetadataAuthorityRecord,
) -> Result<(), ControlError> {
    validate_metadata_authority_record(expected)?;
    validate_metadata_authority_record(next)?;
    if expected == next {
        return Ok(());
    }
    if next.logical_shard_id != expected.logical_shard_id {
        return Err(invalid_authority(
            expected.logical_shard_id,
            "logical shard id is immutable",
        ));
    }
    let expected_revision = expected
        .record_revision
        .get()
        .checked_add(1)
        .ok_or_else(|| {
            invalid_authority(
                expected.logical_shard_id,
                "metadata authority record revision is exhausted",
            )
        })?;
    if next.record_revision.get() != expected_revision {
        return Err(invalid_authority(
            expected.logical_shard_id,
            format!(
                "next metadata authority record revision must be {expected_revision}, got {}",
                next.record_revision
            ),
        ));
    }

    match (expected.migration.as_ref(), next.migration.as_ref()) {
        (None, Some(next_migration)) => {
            if next_migration.phase != MetadataMigrationPhase::Preparing {
                return Err(invalid_authority(
                    expected.logical_shard_id,
                    "a migration must start in Preparing",
                ));
            }
            if next_migration.source != expected.active
                || next.active != expected.active
                || next.authority_generation != expected.authority_generation
            {
                return Err(invalid_authority(
                    expected.logical_shard_id,
                    "starting a migration must preserve the active binding and generation",
                ));
            }
        }
        (Some(expected_migration), Some(next_migration)) => {
            validate_same_migration(
                expected.logical_shard_id,
                expected_migration,
                next_migration,
            )?;
            validate_frontier_advance(
                expected.logical_shard_id,
                "source",
                expected_migration.source_frontier,
                next_migration.source_frontier,
            )?;
            validate_frontier_advance(
                expected.logical_shard_id,
                "target",
                expected_migration.target_frontier,
                next_migration.target_frontier,
            )?;
            validate_frontier_advance(
                expected.logical_shard_id,
                "cutover",
                expected_migration.cutover_frontier,
                next_migration.cutover_frontier,
            )?;
            validate_evidence_advance(
                expected.logical_shard_id,
                "source quiesce receipt",
                expected_migration.source_quiesce_receipt,
                next_migration.source_quiesce_receipt,
            )?;
            validate_evidence_advance(
                expected.logical_shard_id,
                "target activation token",
                expected_migration.target_activation_token,
                next_migration.target_activation_token,
            )?;

            let cuts_over = expected_migration.phase == MetadataMigrationPhase::ReadyToCutover
                && next_migration.phase == MetadataMigrationPhase::CutoverComplete;
            if cuts_over {
                if next_migration.source_frontier != expected_migration.source_frontier
                    || next_migration.target_frontier != expected_migration.target_frontier
                    || next_migration.cutover_frontier != expected_migration.cutover_frontier
                    || next_migration.source_quiesce_receipt
                        != expected_migration.source_quiesce_receipt
                {
                    return Err(invalid_authority(
                        expected.logical_shard_id,
                        "ReadyToCutover frontiers are frozen through CutoverComplete",
                    ));
                }
                let generation = expected
                    .authority_generation
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        invalid_authority(
                            expected.logical_shard_id,
                            "metadata authority generation is exhausted",
                        )
                    })?;
                if next.authority_generation.get() != generation
                    || next.active != next_migration.target
                {
                    return Err(invalid_authority(
                        expected.logical_shard_id,
                        format!(
                            "cutover must install target binding at authority generation {generation}"
                        ),
                    ));
                }
            } else if next.authority_generation != expected.authority_generation
                || next.active != expected.active
            {
                return Err(invalid_authority(
                    expected.logical_shard_id,
                    "migration evidence updates must preserve active binding and generation",
                ));
            }
        }
        (Some(expected_migration), None) => {
            if !matches!(
                expected_migration.phase,
                MetadataMigrationPhase::CutoverComplete | MetadataMigrationPhase::Aborted
            ) {
                return Err(invalid_authority(
                    expected.logical_shard_id,
                    "only a completed or aborted migration may be cleared",
                ));
            }
            if next.active != expected.active
                || next.authority_generation != expected.authority_generation
            {
                return Err(invalid_authority(
                    expected.logical_shard_id,
                    "clearing terminal migration evidence must preserve authority",
                ));
            }
        }
        (None, None) => {
            return Err(invalid_authority(
                expected.logical_shard_id,
                "active authority changes require an explicit migration",
            ));
        }
    }
    Ok(())
}

pub(crate) fn metadata_authority_update_requires_unowned(
    expected: &MetadataAuthorityRecord,
    next: &MetadataAuthorityRecord,
) -> bool {
    if expected == next {
        return false;
    }
    matches!(
        next.migration.as_ref().map(|migration| migration.phase),
        Some(MetadataMigrationPhase::ReadyToCutover | MetadataMigrationPhase::CutoverComplete)
    )
}

pub(crate) fn metadata_authority_update_enters_ready(
    expected: &MetadataAuthorityRecord,
    next: &MetadataAuthorityRecord,
) -> bool {
    matches!(
        (
            expected.migration.as_ref().map(|migration| migration.phase),
            next.migration.as_ref().map(|migration| migration.phase),
        ),
        (
            Some(MetadataMigrationPhase::Quiescing),
            Some(MetadataMigrationPhase::ReadyToCutover)
        )
    )
}

pub(crate) fn metadata_authority_update_installs_source_receipt<'a>(
    expected: &MetadataAuthorityRecord,
    next: &'a MetadataAuthorityRecord,
) -> Option<&'a SourceQuiesceReceipt> {
    let expected_receipt = expected
        .migration
        .as_ref()
        .and_then(|migration| migration.source_quiesce_receipt.as_ref());
    let next_receipt = next
        .migration
        .as_ref()
        .and_then(|migration| migration.source_quiesce_receipt.as_ref());
    if expected_receipt.is_none() {
        next_receipt
    } else {
        None
    }
}

pub(crate) fn validate_source_receipt_control_epoch(
    authority: &MetadataAuthorityRecord,
    receipt: &SourceQuiesceReceipt,
    shard: &LogicalShardRecord,
    session: Option<&LogicalShardLease>,
) -> Result<(), ControlError> {
    if shard.logical_shard_id != authority.logical_shard_id
        || shard.owner_epoch != Some(receipt.owner_epoch)
    {
        return Err(ControlError::MetadataAuthorityAdmission {
            logical_shard_id: authority.logical_shard_id,
            reason: format!(
                "source-quiesce receipt owner epoch {} does not match durable shard epoch {}",
                receipt.owner_epoch,
                shard
                    .owner_epoch
                    .map(|epoch| epoch.to_string())
                    .unwrap_or_else(|| "none".to_owned())
            ),
        });
    }
    if let Some(session) = session {
        validate_record_lease(shard, session)?;
        if session.owner_epoch != receipt.owner_epoch || session.authority != authority.fence() {
            return Err(ControlError::MetadataAuthorityAdmission {
                logical_shard_id: authority.logical_shard_id,
                reason: "source-quiesce receipt does not match the live owner session and authority fence"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

pub(crate) fn prepare_expired_owner_cleanup(
    record: &LogicalShardRecord,
) -> Result<LogicalShardRecord, ControlError> {
    validate_logical_shard_record(record)?;
    if record.owner.is_none() {
        ensure_unowned_for_cutover(record)?;
        return Ok(record.clone());
    }
    let mut cleaned = record.clone();
    cleaned.owner = None;
    cleaned.lease_id = 0;
    cleaned.state = LogicalShardState::Unassigned;
    cleaned.endpoint = None;
    validate_logical_shard_record(&cleaned)?;
    Ok(cleaned)
}

pub(crate) fn ensure_unowned_for_cutover(record: &LogicalShardRecord) -> Result<(), ControlError> {
    if record.owner.is_none()
        && record.lease_id == 0
        && record.state == LogicalShardState::Unassigned
    {
        Ok(())
    } else {
        Err(ControlError::MetadataAuthorityAdmission {
            logical_shard_id: record.logical_shard_id,
            reason: "migration cutover requires an unowned logical shard".to_owned(),
        })
    }
}

pub(crate) fn validate_authority_for_owner_operation(
    authority: &MetadataAuthorityRecord,
    lease: Option<&LogicalShardLease>,
) -> Result<(), ControlError> {
    validate_metadata_authority_record(authority)?;
    if let Some(lease) = lease {
        if lease.authority != authority.fence()
            || lease.logical_shard_id != authority.logical_shard_id
            || lease.authority.logical_shard_id != lease.logical_shard_id
        {
            return Err(ControlError::MetadataAuthorityAdmission {
                logical_shard_id: lease.logical_shard_id,
                reason: format!(
                    "owner authority fence {:?} does not match active fence {:?}",
                    lease.authority,
                    authority.fence()
                ),
            });
        }
    }
    if matches!(
        authority
            .migration
            .as_ref()
            .map(|migration| migration.phase),
        Some(MetadataMigrationPhase::Quiescing | MetadataMigrationPhase::ReadyToCutover)
    ) {
        return Err(ControlError::MetadataAuthorityAdmission {
            logical_shard_id: authority.logical_shard_id,
            reason:
                "owner acquisition, renewal, and serving publication are fenced during quiescence"
                    .to_owned(),
        });
    }
    Ok(())
}

fn validate_authority_binding(
    logical_shard_id: LogicalShardId,
    name: &str,
    binding: &MetadataAuthorityBinding,
) -> Result<(), ControlError> {
    if binding
        .authority_id
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} authority id must not be all zeroes"),
        ));
    }
    if binding.profile_fingerprint.iter().all(|byte| *byte == 0) {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} provider profile fingerprint must not be all zeroes"),
        ));
    }
    if binding
        .consistency_domain_id
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} consistency domain id must not be all zeroes"),
        ));
    }
    if binding
        .contract_digest
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} metadata contract digest must not be all zeroes"),
        ));
    }
    Ok(())
}

fn validate_recovery_frontier(
    logical_shard_id: LogicalShardId,
    name: &str,
    frontier: &MetadataRecoveryFrontier,
) -> Result<(), ControlError> {
    if frontier.chain_digest.iter().all(|byte| *byte == 0) {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} recovery chain digest must not be all zeroes"),
        ));
    }
    if frontier.state_digest.iter().all(|byte| *byte == 0) {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} recovery state digest must not be all zeroes"),
        ));
    }
    Ok(())
}

fn validate_source_quiesce_receipt(
    logical_shard_id: LogicalShardId,
    migration: &MetadataMigration,
    source_generation: MetadataAuthorityGeneration,
    receipt: &SourceQuiesceReceipt,
) -> Result<(), ControlError> {
    if receipt.logical_shard_id != logical_shard_id
        || receipt.migration_id != migration.migration_id
        || receipt.source_authority_id != migration.source.authority_id
        || receipt.source_authority_generation != source_generation
        || receipt.contract_digest != migration.source.contract_digest
    {
        return Err(invalid_authority(
            logical_shard_id,
            "source-quiesce receipt does not match the migration source authority",
        ));
    }
    validate_recovery_frontier(logical_shard_id, "source receipt", &receipt.frontier)
}

fn validate_same_migration(
    logical_shard_id: LogicalShardId,
    expected: &MetadataMigration,
    next: &MetadataMigration,
) -> Result<(), ControlError> {
    if expected.migration_id != next.migration_id
        || expected.source != next.source
        || expected.target != next.target
    {
        return Err(invalid_authority(
            logical_shard_id,
            "migration id, source, and target are immutable",
        ));
    }
    if expected.phase == next.phase
        && matches!(
            expected.phase,
            MetadataMigrationPhase::ReadyToCutover
                | MetadataMigrationPhase::CutoverComplete
                | MetadataMigrationPhase::Aborted
        )
    {
        return Err(invalid_authority(
            logical_shard_id,
            format!(
                "migration phase {:?} is immutable once installed",
                expected.phase
            ),
        ));
    }
    let valid_phase = expected.phase == next.phase
        || matches!(
            (expected.phase, next.phase),
            (
                MetadataMigrationPhase::Preparing,
                MetadataMigrationPhase::Copying | MetadataMigrationPhase::Aborted
            ) | (
                MetadataMigrationPhase::Copying,
                MetadataMigrationPhase::CatchingUp | MetadataMigrationPhase::Aborted
            ) | (
                MetadataMigrationPhase::CatchingUp,
                MetadataMigrationPhase::Quiescing | MetadataMigrationPhase::Aborted
            ) | (
                MetadataMigrationPhase::Quiescing,
                MetadataMigrationPhase::ReadyToCutover
            ) | (
                MetadataMigrationPhase::ReadyToCutover,
                MetadataMigrationPhase::CutoverComplete
            )
        );
    if !valid_phase {
        return Err(invalid_authority(
            logical_shard_id,
            format!(
                "migration phase transition {:?} -> {:?} is not allowed",
                expected.phase, next.phase
            ),
        ));
    }
    Ok(())
}

fn validate_frontier_advance(
    logical_shard_id: LogicalShardId,
    name: &str,
    expected: Option<MetadataRecoveryFrontier>,
    next: Option<MetadataRecoveryFrontier>,
) -> Result<(), ControlError> {
    match (expected, next) {
        (None, _) | (Some(_), Some(_)) => {}
        (Some(_), None) => {
            return Err(invalid_authority(
                logical_shard_id,
                format!("{name} recovery frontier must not be removed"),
            ));
        }
    }
    let (Some(expected), Some(next)) = (expected, next) else {
        return Ok(());
    };
    if next.recovery_lsn < expected.recovery_lsn || next.commit_version < expected.commit_version {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} recovery frontier must not move backwards"),
        ));
    }
    if next.recovery_lsn == expected.recovery_lsn && next != expected {
        return Err(invalid_authority(
            logical_shard_id,
            format!("{name} recovery frontier identity differs at the same LSN"),
        ));
    }
    Ok(())
}

fn validate_evidence_advance<T: Copy + PartialEq>(
    logical_shard_id: LogicalShardId,
    name: &str,
    expected: Option<T>,
    next: Option<T>,
) -> Result<(), ControlError> {
    match (expected, next) {
        (None, _) | (Some(_), Some(_)) => {}
        (Some(_), None) => {
            return Err(invalid_authority(
                logical_shard_id,
                format!("{name} must not be removed"),
            ));
        }
    }
    if let (Some(expected), Some(next)) = (expected, next) {
        if expected != next {
            return Err(invalid_authority(
                logical_shard_id,
                format!("{name} is immutable once installed"),
            ));
        }
    }
    Ok(())
}

fn invalid_authority(logical_shard_id: LogicalShardId, reason: impl Into<String>) -> ControlError {
    ControlError::InvalidMetadataAuthorityMutation {
        logical_shard_id,
        reason: reason.into(),
    }
}

pub(crate) fn validate_new_root_placement(placement: &RootPlacement) -> Result<(), ControlError> {
    validate_qualified_single_shard_layout(placement)?;
    if placement.placement_generation.get() != 1 {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: placement.root_id,
            reason: "initial placement generation must be 1".to_owned(),
        });
    }
    if placement.lifecycle != RootPlacementLifecycle::Provisioning {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: placement.root_id,
            reason: "initial placement lifecycle must be Provisioning".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn validate_qualified_single_shard_layout(
    placement: &RootPlacement,
) -> Result<(), ControlError> {
    if placement.layout_profile != RootLayoutProfile::SingleShardRoot {
        return Err(ControlError::RootLayoutNotQualified {
            root_id: placement.root_id,
            profile: placement.layout_profile,
        });
    }
    if placement.layout_generation.get() != 1 {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: placement.root_id,
            reason: format!(
                "root layout generation {} is NOT QUALIFIED; this runtime supports generation 1 only",
                placement.layout_generation
            ),
        });
    }
    if placement.partition_id != RootPartitionId::SINGLE_SHARD {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: placement.root_id,
            reason: "SingleShardRoot must use the reserved SINGLE_SHARD partition id".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn validate_root_placement_update(
    expected: &RootPlacement,
    next: &RootPlacement,
) -> Result<(), ControlError> {
    validate_qualified_single_shard_layout(expected)?;
    validate_qualified_single_shard_layout(next)?;
    if next.root_id != expected.root_id {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: "root id is immutable".to_owned(),
        });
    }
    if next.layout_profile != expected.layout_profile {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: "root layout profile is immutable outside a fenced layout migration".to_owned(),
        });
    }
    if next.layout_generation != expected.layout_generation {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: "root layout generation is immutable outside a fenced layout migration"
                .to_owned(),
        });
    }
    if next.partition_id != expected.partition_id {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: "root partition binding is immutable outside a fenced layout migration"
                .to_owned(),
        });
    }
    if next.logical_shard_id != expected.logical_shard_id {
        return Err(ControlError::ImmutableShardAffinity {
            root_id: expected.root_id,
            existing: expected.logical_shard_id,
            requested: next.logical_shard_id,
        });
    }
    let expected_generation = expected
        .placement_generation
        .get()
        .checked_add(1)
        .ok_or_else(|| ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: "placement generation is exhausted".to_owned(),
        })?;
    if next.placement_generation.get() != expected_generation {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: format!(
                "next placement generation must be {expected_generation}, got {}",
                next.placement_generation
            ),
        });
    }
    validate_placement_transition(expected.root_id, expected.lifecycle, next.lifecycle)
}

fn validate_placement_transition(
    root_id: RootId,
    current: RootPlacementLifecycle,
    next: RootPlacementLifecycle,
) -> Result<(), ControlError> {
    let valid = matches!(
        (current, next),
        (
            RootPlacementLifecycle::Provisioning,
            RootPlacementLifecycle::Active
        ) | (
            RootPlacementLifecycle::Provisioning,
            RootPlacementLifecycle::Retired
        ) | (
            RootPlacementLifecycle::Active,
            RootPlacementLifecycle::Draining
        ) | (
            RootPlacementLifecycle::Draining,
            RootPlacementLifecycle::Active
        ) | (
            RootPlacementLifecycle::Draining,
            RootPlacementLifecycle::Retired
        )
    );
    if valid {
        Ok(())
    } else {
        Err(ControlError::InvalidPlacementMutation {
            root_id,
            reason: format!("lifecycle transition {current:?} -> {next:?} is not allowed"),
        })
    }
}

pub(crate) fn prepare_owner_acquisition(
    current: &LogicalShardRecord,
    expected_owner_epoch: Option<OwnerEpoch>,
    owner: NodeId,
    owner_incarnation_id: OwnerIncarnationId,
    endpoint: String,
    lease_id: u64,
    authority: MetadataAuthorityFence,
) -> Result<(LogicalShardRecord, LogicalShardLease), ControlError> {
    validate_logical_shard_record(current)?;
    validate_endpoint(&endpoint)?;
    validate_owner_incarnation_id(owner_incarnation_id)?;
    if lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "active owner lease id must be non-zero".to_owned(),
        ));
    }
    if authority.logical_shard_id != current.logical_shard_id {
        return Err(ControlError::MetadataAuthorityAdmission {
            logical_shard_id: current.logical_shard_id,
            reason: "owner authority fence belongs to a different logical shard".to_owned(),
        });
    }
    if current.owner_epoch != expected_owner_epoch {
        return Err(ControlError::StaleOwnerEpoch {
            logical_shard_id: current.logical_shard_id,
            expected: expected_owner_epoch,
            actual: current.owner_epoch,
        });
    }
    if let (Some(_), Some(owner_epoch)) = (&current.owner, expected_owner_epoch) {
        return Err(ControlError::PreviousOwnerSessionLive {
            logical_shard_id: current.logical_shard_id,
            owner_epoch,
        });
    }
    if expected_owner_epoch.is_some() && current.owner_incarnation_id == Some(owner_incarnation_id)
    {
        return Err(ControlError::InvalidRecord(
            "successor owner must use a new incarnation id".to_owned(),
        ));
    }
    if expected_owner_epoch.is_none() {
        if let (Some(current_owner), Some(owner_epoch)) =
            (current.owner.clone(), current.owner_epoch)
        {
            return Err(ControlError::LogicalShardAlreadyOwned {
                logical_shard_id: current.logical_shard_id,
                owner: current_owner,
                owner_epoch,
            });
        }
        if current.owner.is_some() {
            return Err(ControlError::InvalidRecord(
                "owned shard is missing its owner epoch".to_owned(),
            ));
        }
    }
    let next_epoch = match expected_owner_epoch {
        None => OwnerEpoch::new(1).expect("one is a valid owner epoch"),
        Some(current_epoch) => {
            let next = current_epoch
                .get()
                .checked_add(1)
                .ok_or(ControlError::OwnerEpochExhausted(current.logical_shard_id))?;
            OwnerEpoch::new(next)
                .map_err(|_| ControlError::OwnerEpochExhausted(current.logical_shard_id))?
        }
    };
    let lease = LogicalShardLease {
        logical_shard_id: current.logical_shard_id,
        owner: owner.clone(),
        owner_epoch: next_epoch,
        owner_incarnation_id,
        lease_id,
        authority,
    };
    let mut next = current.clone();
    next.owner = Some(owner);
    next.owner_epoch = Some(next_epoch);
    next.owner_incarnation_id = Some(owner_incarnation_id);
    next.lease_id = lease_id;
    next.state = LogicalShardState::Recovering;
    next.endpoint = Some(endpoint);
    validate_logical_shard_record(&next)?;
    Ok((next, lease))
}

pub(crate) fn prepare_mark_serving(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
    publication: RecoveryPublication,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    let mut next = current.clone();
    apply_recovery_publication(&mut next, publication)?;
    next.state = LogicalShardState::Serving;
    validate_logical_shard_record(&next)?;
    Ok(next)
}

pub(crate) fn prepare_owner_release(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    let mut next = current.clone();
    next.owner = None;
    next.lease_id = 0;
    next.state = LogicalShardState::Unassigned;
    next.endpoint = None;
    validate_logical_shard_record(&next)?;
    Ok(next)
}

pub(crate) fn validate_record_lease(
    record: &LogicalShardRecord,
    lease: &LogicalShardLease,
) -> Result<(), ControlError> {
    if lease.authority.logical_shard_id != lease.logical_shard_id
        || validate_owner_incarnation_id(lease.owner_incarnation_id).is_err()
    {
        return Err(ControlError::StaleLease(lease.clone()));
    }
    if record.owner.as_ref() != Some(&lease.owner) {
        return Err(ControlError::NotOwner {
            logical_shard_id: lease.logical_shard_id,
        });
    }
    if record.logical_shard_id != lease.logical_shard_id
        || record.owner_epoch != Some(lease.owner_epoch)
        || record.owner_incarnation_id != Some(lease.owner_incarnation_id)
        || record.lease_id != lease.lease_id
    {
        return Err(ControlError::StaleLease(lease.clone()));
    }
    Ok(())
}

pub(crate) fn validate_logical_shard_record(
    record: &LogicalShardRecord,
) -> Result<(), ControlError> {
    if record.owner_epoch.is_some() != record.owner_incarnation_id.is_some() {
        return Err(ControlError::InvalidRecord(
            "logical shard owner epoch and incarnation must be present together".to_owned(),
        ));
    }
    if let Some(owner_incarnation_id) = record.owner_incarnation_id {
        validate_owner_incarnation_id(owner_incarnation_id)?;
    }
    if record.owner_epoch.is_none()
        && (record.checkpoint.is_some() || record.log.is_some() || record.durable_lsn != 0)
    {
        return Err(ControlError::InvalidRecord(
            "never-owned logical shard cannot have recovery state".to_owned(),
        ));
    }
    match record.owner.as_ref() {
        None => {
            if record.endpoint.is_some() {
                return Err(ControlError::InvalidRecord(
                    "unowned logical shard must not have an endpoint".to_owned(),
                ));
            }
            if record.lease_id != 0 {
                return Err(ControlError::InvalidRecord(
                    "unowned logical shard must have lease id zero".to_owned(),
                ));
            }
            if record.state != LogicalShardState::Unassigned {
                return Err(ControlError::InvalidRecord(
                    "unowned logical shard must be Unassigned".to_owned(),
                ));
            }
        }
        Some(_) => {
            if record.owner_epoch.is_none() {
                return Err(ControlError::InvalidRecord(
                    "owned logical shard must have an owner epoch".to_owned(),
                ));
            }
            if record.lease_id == 0 {
                return Err(ControlError::InvalidRecord(
                    "owned logical shard must have a non-zero lease id".to_owned(),
                ));
            }
            let endpoint = record.endpoint.as_deref().ok_or_else(|| {
                ControlError::InvalidRecord(
                    "owned logical shard must have a reachable endpoint".to_owned(),
                )
            })?;
            if !endpoint_is_canonical(endpoint) {
                return Err(ControlError::InvalidRecord(
                    "logical-shard endpoint is empty or non-canonical".to_owned(),
                ));
            }
            if record.state == LogicalShardState::Unassigned {
                return Err(ControlError::InvalidRecord(
                    "owned logical shard cannot be Unassigned".to_owned(),
                ));
            }
        }
    }

    if let Some(checkpoint) = record.checkpoint.as_ref() {
        validate_checkpoint_ref(checkpoint).map_err(ControlError::InvalidRecord)?;
    }
    if let Some(log) = record.log.as_ref() {
        validate_log_ref(record, None, log).map_err(ControlError::InvalidRecord)?;
    }
    let reference_lsn = match (record.checkpoint.as_ref(), record.log.as_ref()) {
        (Some(checkpoint), Some(log)) => checkpoint.lsn.max(log.durable_lsn),
        (Some(checkpoint), None) => checkpoint.lsn,
        (None, Some(log)) => log.durable_lsn,
        (None, None) => 0,
    };
    if record.durable_lsn != reference_lsn {
        return Err(ControlError::InvalidRecord(format!(
            "durable LSN {} does not match recovery reference tail {reference_lsn}",
            record.durable_lsn
        )));
    }
    durable_tail_digest(record).map_err(ControlError::InvalidRecord)?;
    Ok(())
}

pub(crate) fn validate_owner_incarnation_id(
    owner_incarnation_id: OwnerIncarnationId,
) -> Result<(), ControlError> {
    if owner_incarnation_id
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        Err(ControlError::InvalidRecord(
            "owner incarnation id must be non-zero".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), ControlError> {
    if endpoint_is_canonical(endpoint) {
        Ok(())
    } else {
        Err(ControlError::InvalidEndpoint(endpoint.to_owned()))
    }
}

fn validate_checkpoint_ref(checkpoint: &CheckpointRef) -> Result<(), String> {
    if checkpoint.object_key.is_empty() {
        return Err("checkpoint object key must not be empty".to_owned());
    }
    if checkpoint.image_bytes == 0 {
        return Err("checkpoint image size must be non-zero".to_owned());
    }
    if checkpoint.image_digest.is_empty() {
        return Err("checkpoint image digest must not be empty".to_owned());
    }
    if checkpoint.digest.is_empty() {
        return Err("checkpoint state digest must not be empty".to_owned());
    }
    Ok(())
}

/// Merge one caller-serialized, owner-fenced recovery publication.
///
/// Checkpoints dominate logs they fully cover. A later log may advance beyond
/// that checkpoint only by preserving the full durable segment chain.
pub(crate) fn apply_recovery_publication(
    record: &mut LogicalShardRecord,
    publication: RecoveryPublication,
) -> Result<(), ControlError> {
    let RecoveryPublication {
        checkpoint,
        log,
        durable_lsn,
    } = publication;
    let logical_shard_id = record.logical_shard_id;
    let conflict = |reason: String| ControlError::RecoveryPublicationConflict {
        logical_shard_id,
        reason,
    };

    if let Some(checkpoint) = checkpoint.as_ref() {
        validate_checkpoint_ref(checkpoint).map_err(&conflict)?;
    }
    let reference_lsn = match (checkpoint.as_ref(), log.as_ref()) {
        (Some(checkpoint), Some(log)) => checkpoint.lsn.max(log.durable_lsn),
        (Some(checkpoint), None) => checkpoint.lsn,
        (None, Some(log)) => log.durable_lsn,
        (None, None) => {
            if durable_lsn != record.durable_lsn {
                return Err(conflict(format!(
                    "empty publication durable LSN {durable_lsn} does not confirm current durable LSN {}",
                    record.durable_lsn
                )));
            }
            return Ok(());
        }
    };
    if durable_lsn != reference_lsn {
        return Err(conflict(format!(
            "publication durable LSN {durable_lsn} does not match reference tail {reference_lsn}"
        )));
    }

    if let Some(checkpoint) = checkpoint.as_ref() {
        if checkpoint.lsn < record.durable_lsn {
            return Err(conflict(format!(
                "checkpoint LSN {} is behind durable LSN {}",
                checkpoint.lsn, record.durable_lsn
            )));
        }
    }
    if let Some(log) = log.as_ref() {
        if log.durable_lsn < record.durable_lsn {
            return Err(conflict(format!(
                "log LSN {} is behind durable LSN {}",
                log.durable_lsn, record.durable_lsn
            )));
        }
        validate_log_ref(record, checkpoint.as_ref(), log).map_err(&conflict)?;
        if let Some(current) = record
            .log
            .as_ref()
            .filter(|current| current.durable_lsn == log.durable_lsn)
        {
            if current != log {
                return Err(conflict(format!(
                    "log identity differs at LSN {}",
                    log.durable_lsn
                )));
            }
        }
    }

    if let (Some(checkpoint), Some(log)) = (checkpoint.as_ref(), log.as_ref()) {
        if checkpoint.lsn == log.durable_lsn && checkpoint.digest != log.digest {
            return Err(conflict(format!(
                "checkpoint and log digests differ at LSN {}",
                checkpoint.lsn
            )));
        }
    }

    let current_tail_digest = durable_tail_digest(record).map_err(&conflict)?;
    if let Some(expected) = current_tail_digest.as_deref() {
        if let Some(checkpoint) = checkpoint
            .as_ref()
            .filter(|checkpoint| checkpoint.lsn == record.durable_lsn)
        {
            if checkpoint.digest != expected {
                return Err(conflict(format!(
                    "checkpoint digest differs from durable tail at LSN {}",
                    checkpoint.lsn
                )));
            }
        }
        if let Some(log) = log
            .as_ref()
            .filter(|log| log.durable_lsn == record.durable_lsn)
        {
            if log.digest != expected {
                return Err(conflict(format!(
                    "log digest differs from durable tail at LSN {}",
                    log.durable_lsn
                )));
            }
        }
    }

    if let Some(checkpoint) = checkpoint {
        record.checkpoint = Some(checkpoint);
    }
    if let Some(log) = log {
        record.log = Some(log);
    }
    if let Some(checkpoint) = record.checkpoint.as_ref() {
        if record
            .log
            .as_ref()
            .is_some_and(|log| log.durable_lsn <= checkpoint.lsn)
        {
            record.log = None;
        }
    }
    record.durable_lsn = durable_lsn;
    Ok(())
}

fn validate_log_ref(
    record: &LogicalShardRecord,
    incoming_checkpoint: Option<&CheckpointRef>,
    log: &LogRef,
) -> Result<(), String> {
    if log.segments.is_empty() {
        return Err("log segment chain is empty".to_owned());
    }
    if log.digest.is_empty() {
        return Err("log tail digest must not be empty".to_owned());
    }

    let checkpoint_lsn = incoming_checkpoint
        .or(record.checkpoint.as_ref())
        .map_or(0, |checkpoint| checkpoint.lsn);
    if log.durable_lsn < checkpoint_lsn {
        return Err(format!(
            "log durable LSN {} is behind checkpoint LSN {checkpoint_lsn}",
            log.durable_lsn
        ));
    }

    for (index, segment) in log.segments.iter().enumerate() {
        if segment.segment_key.is_empty() {
            return Err(format!("log segment {index} has an empty object key"));
        }
        if segment.digest.is_empty() {
            return Err(format!("log segment {index} has an empty digest"));
        }
        if segment.first_lsn == 0 || segment.first_lsn > segment.last_lsn {
            return Err(format!(
                "log segment {index} has invalid LSN range {}..{}",
                segment.first_lsn, segment.last_lsn
            ));
        }
        if let Some(previous) = index.checked_sub(1).map(|index| &log.segments[index]) {
            let expected = previous
                .last_lsn
                .checked_add(1)
                .ok_or_else(|| "log segment LSN range is exhausted".to_owned())?;
            if segment.first_lsn != expected {
                return Err(format!(
                    "log segment {index} starts at {}, expected {expected}",
                    segment.first_lsn
                ));
            }
        }
    }

    let first = log
        .segments
        .first()
        .expect("non-empty segment chain has a first segment");
    if log.durable_lsn > checkpoint_lsn {
        let expected_first = checkpoint_lsn
            .checked_add(1)
            .ok_or_else(|| "checkpoint LSN is exhausted".to_owned())?;
        if first.first_lsn != expected_first {
            return Err(format!(
                "log chain starts at LSN {}, expected {expected_first} after checkpoint",
                first.first_lsn
            ));
        }
    }

    let last = log
        .segments
        .last()
        .expect("non-empty segment chain has a last segment");
    if last.last_lsn != log.durable_lsn {
        return Err(format!(
            "log tail segment ends at LSN {}, durable LSN is {}",
            last.last_lsn, log.durable_lsn
        ));
    }
    if last.digest != log.digest {
        return Err("log tail digest does not match the final segment digest".to_owned());
    }

    if let Some(current) = record.log.as_ref().filter(|current| {
        current.durable_lsn > checkpoint_lsn && log.durable_lsn > current.durable_lsn
    }) {
        if log.segments.len() < current.segments.len()
            || log.segments[..current.segments.len()] != current.segments
        {
            return Err(
                "advanced log chain does not preserve the durable segment prefix".to_owned(),
            );
        }
    }
    Ok(())
}

fn durable_tail_digest(record: &LogicalShardRecord) -> Result<Option<String>, String> {
    let checkpoint = record
        .checkpoint
        .as_ref()
        .filter(|checkpoint| checkpoint.lsn == record.durable_lsn);
    let log = record
        .log
        .as_ref()
        .filter(|log| log.durable_lsn == record.durable_lsn);
    if let (Some(checkpoint), Some(log)) = (checkpoint, log) {
        if checkpoint.digest != log.digest {
            return Err(format!(
                "stored checkpoint and log digests differ at durable LSN {}",
                record.durable_lsn
            ));
        }
    }
    Ok(log
        .map(|log| log.digest.clone())
        .or_else(|| checkpoint.map(|checkpoint| checkpoint.digest.clone())))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;
    use crate::owner_admission_command::{
        mint_abort_owner_admission_command, mint_commit_owner_admission_command,
        mint_prepare_owner_admission_command, mint_publish_owner_serving_command,
        mint_reconcile_owner_admission_intent_command, mint_reconcile_owner_admission_plan_command,
        mint_reconcile_owner_serving_command, mint_released_terminate_owner_admission_command,
        mint_renew_owner_session_command,
    };
    use crate::{
        CommitVersion, ConsistencyDomainId, LogSegmentRef, MetadataAuthorityGeneration,
        MetadataAuthorityId, MetadataAuthorityRevision, MetadataContractDigest,
        MetadataProviderProfileId, OperationId, OwnerRuntimeReservationDigest, PlacementGeneration,
        RootLayoutGeneration,
    };

    fn root_id(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn incarnation(value: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([value; 16])
    }

    fn node(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn authority_binding(value: u8, profile: &str) -> MetadataAuthorityBinding {
        MetadataAuthorityBinding {
            authority_id: MetadataAuthorityId::from_bytes([value; 16]),
            provider_profile_id: MetadataProviderProfileId::new(profile).unwrap(),
            profile_fingerprint: [value; 32],
            consistency_domain_id: ConsistencyDomainId::from_bytes([value; 16]),
            contract_digest: MetadataContractDigest::from_bytes([9; 32]),
        }
    }

    fn initial_authority(shard: u8) -> MetadataAuthorityRecord {
        MetadataAuthorityRecord {
            logical_shard_id: shard_id(shard),
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
            active: authority_binding(1, "holt-primary"),
            migration: None,
        }
    }

    fn next_authority(current: &MetadataAuthorityRecord) -> MetadataAuthorityRecord {
        let mut next = current.clone();
        next.record_revision =
            MetadataAuthorityRevision::new(current.record_revision.get() + 1).unwrap();
        next
    }

    fn frontier(value: u8) -> MetadataRecoveryFrontier {
        MetadataRecoveryFrontier {
            recovery_lsn: u64::from(value),
            chain_digest: [value; 32],
            commit_version: CommitVersion::new(u64::from(value)).unwrap(),
            state_digest: [value; 32],
        }
    }

    fn source_receipt(
        authority: &MetadataAuthorityRecord,
        owner_epoch: OwnerEpoch,
        frontier: MetadataRecoveryFrontier,
    ) -> SourceQuiesceReceipt {
        let migration = authority.migration.as_ref().unwrap();
        SourceQuiesceReceipt {
            logical_shard_id: authority.logical_shard_id,
            migration_id: migration.migration_id,
            source_authority_id: migration.source.authority_id,
            source_authority_generation: authority.authority_generation,
            owner_epoch,
            frontier,
            contract_digest: migration.source.contract_digest,
        }
    }

    fn initial_placement(root: u8, shard: u8) -> RootPlacement {
        RootPlacement {
            root_id: root_id(root),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: shard_id(shard),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        }
    }

    fn next_placement(current: &RootPlacement, lifecycle: RootPlacementLifecycle) -> RootPlacement {
        RootPlacement {
            placement_generation: PlacementGeneration::new(current.placement_generation.get() + 1)
                .unwrap(),
            lifecycle,
            ..current.clone()
        }
    }

    fn create_placed_shard(store: &InMemoryControlStore, root: u8, shard: u8) -> RootPlacement {
        store.create_logical_shard(shard_id(shard)).unwrap();
        store
            .create_metadata_authority(initial_authority(shard))
            .unwrap();
        let placement = initial_placement(root, shard);
        store.create_root_placement(placement.clone()).unwrap();
        placement
    }

    fn activate_placed_shard(
        store: &InMemoryControlStore,
        placement: &RootPlacement,
    ) -> RootPlacement {
        let active = next_placement(placement, RootPlacementLifecycle::Active);
        store
            .compare_and_set_root_placement(placement, active.clone())
            .unwrap()
    }

    fn stable_admission(store: &InMemoryControlStore, root: u8) -> OwnerServingAdmission {
        let placement = store.get_root_placement(&root_id(root)).unwrap().unwrap();
        let authority = store
            .get_metadata_authority(&placement.logical_shard_id)
            .unwrap()
            .unwrap();
        OwnerServingAdmission::stable(placement, authority).unwrap()
    }

    fn acquire_placed_shard(
        store: &InMemoryControlStore,
        root: u8,
        shard: u8,
    ) -> (LogicalShardLease, OwnerServingAdmission) {
        let placement = create_placed_shard(store, root, shard);
        activate_placed_shard(store, &placement);
        let admission = stable_admission(store, root);
        let lease = store
            .acquire_owner(
                &admission,
                node("node-a"),
                incarnation(1),
                "node-a:7000".to_owned(),
            )
            .unwrap();
        (lease, admission)
    }

    fn log_ref(first_lsn: u64, last_lsn: u64, digest: &str) -> LogRef {
        LogRef {
            segments: vec![LogSegmentRef {
                segment_key: format!("logs/{first_lsn}-{last_lsn}"),
                first_lsn,
                last_lsn,
                digest: digest.to_owned(),
            }],
            durable_lsn: last_lsn,
            digest: digest.to_owned(),
        }
    }

    fn planned_owner_store() -> (InMemoryControlStore, OwnerAdmissionIntentV1) {
        let store = InMemoryControlStore::new();
        let placement = create_placed_shard(&store, 1, 1);
        activate_placed_shard(&store, &placement);
        let intent = fresh_owner_intent(&store, 1, 1, "node-a");
        (store, intent)
    }

    fn fresh_owner_intent(
        store: &InMemoryControlStore,
        incarnation_value: u8,
        reservation_value: u8,
        owner: &str,
    ) -> OwnerAdmissionIntentV1 {
        OwnerAdmissionIntentV1::fresh(
            stable_admission(store, 1),
            store.get_logical_shard(&shard_id(1)).unwrap().unwrap(),
            node(owner),
            incarnation(incarnation_value),
            format!("{owner}:7000"),
            OwnerRuntimeReservationDigest::from_bytes([reservation_value; 32]).unwrap(),
        )
        .unwrap()
    }

    fn successor_owner_intent(
        store: &InMemoryControlStore,
        released: LogicalShardRecord,
        predecessor: OwnerAdmissionClaimV1,
        incarnation_value: u8,
        owner: &str,
    ) -> OwnerAdmissionIntentV1 {
        OwnerAdmissionIntentV1::successor(
            stable_admission(store, 1),
            released,
            predecessor,
            node(owner),
            incarnation(incarnation_value),
            format!("{owner}:7000"),
            OwnerRuntimeReservationDigest::from_bytes([incarnation_value; 32]).unwrap(),
        )
        .unwrap()
    }

    fn execute_prepare(
        store: &dyn ControlStore,
        intent: OwnerAdmissionIntentV1,
    ) -> PrepareOwnerAdmissionResultV1 {
        let (command, witness) = mint_prepare_owner_admission_command(intent);
        witness
            .resolve(store.prepare_owner_admission(command))
            .unwrap()
    }

    fn execute_commit(
        store: &dyn ControlStore,
        plan: PlannedOwnerAdmissionV1,
    ) -> CommitOwnerAdmissionResultV1 {
        let (command, witness) = mint_commit_owner_admission_command(plan);
        witness
            .resolve(store.commit_owner_admission(command))
            .unwrap()
    }

    fn execute_abort(
        store: &dyn ControlStore,
        plan: PlannedOwnerAdmissionV1,
        reason: OwnerAdmissionAbortReasonV1,
    ) -> AbortOwnerAdmissionResultV1 {
        let (command, witness) = mint_abort_owner_admission_command(plan, reason);
        witness
            .resolve(store.abort_owner_admission(command))
            .unwrap()
    }

    fn execute_reconcile_intent(
        store: &dyn ControlStore,
        intent: OwnerAdmissionIntentV1,
    ) -> ReconcileOwnerAdmissionResultV1 {
        let (command, witness) = mint_reconcile_owner_admission_intent_command(intent);
        witness
            .resolve(store.reconcile_owner_admission(command))
            .unwrap()
            .into_result()
    }

    fn execute_reconcile_plan(
        store: &dyn ControlStore,
        plan: PlannedOwnerAdmissionV1,
    ) -> ReconcileOwnerAdmissionResultV1 {
        let (command, witness) = mint_reconcile_owner_admission_plan_command(plan);
        witness
            .resolve(store.reconcile_owner_admission(command))
            .unwrap()
            .into_result()
    }

    fn execute_released_terminate(
        store: &dyn ControlStore,
        plan: PlannedOwnerAdmissionV1,
    ) -> TerminateOwnerAdmissionResultV1 {
        let (command, witness) = mint_released_terminate_owner_admission_command(plan);
        witness
            .resolve(store.terminate_owner_admission(command))
            .unwrap()
    }

    #[test]
    fn in_memory_owner_leases_are_explicitly_non_expiring() {
        assert_eq!(
            InMemoryControlStore::new().owner_lease_model(),
            OwnerLeaseModel::NonExpiring
        );
    }

    #[test]
    fn in_memory_publication_and_renewal_use_exact_committed_commands() {
        let (store, intent) = planned_owner_store();
        let plan = match execute_prepare(&store, intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        let (source, claim) = match execute_commit(&store, plan.clone()) {
            CommitOwnerAdmissionResultV1::Committed { shard, claim, .. } => (shard, claim),
            result => panic!("expected Committed, got {result:?}"),
        };
        let expected_claim = claim.clone();
        let publication = PlannedOwnerServingPublicationV1::new(
            plan.clone(),
            source,
            RecoveryPublication {
                checkpoint: None,
                log: None,
                durable_lsn: 0,
            },
        )
        .unwrap();
        let (publish_command, publish_witness) =
            mint_publish_owner_serving_command(publication.clone());
        let published = publish_witness
            .resolve(store.publish_owner_serving(publish_command))
            .unwrap();
        assert!(matches!(
            published,
            PublishOwnerServingResultV1::Published {
                ref shard,
                ref claim,
                lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
            } if shard == publication.target() && claim == &expected_claim
        ));

        let (replay_command, replay_witness) =
            mint_publish_owner_serving_command(publication.clone());
        assert!(matches!(
            replay_witness
                .resolve(store.publish_owner_serving(replay_command))
                .unwrap(),
            PublishOwnerServingResultV1::AlreadyPublished {
                shard,
                lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
                ..
            } if shard == *publication.target()
        ));

        let (reconcile_command, reconcile_witness) =
            mint_reconcile_owner_serving_command(publication.clone());
        assert!(matches!(
            reconcile_witness
                .resolve(store.reconcile_owner_admission(reconcile_command))
                .unwrap()
                .into_result(),
            ReconcileOwnerAdmissionResultV1::Committed {
                shard,
                lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
                ..
            } if shard == *publication.target()
        ));

        let renewal = OwnerSessionRenewalTargetV1::new(plan, claim).unwrap();
        let (renew_command, renew_witness) = mint_renew_owner_session_command(renewal);
        assert!(matches!(
            renew_witness
                .resolve(store.renew_owner_session(renew_command))
                .unwrap(),
            RenewOwnerSessionResultV1::Current {
                shard,
                lifetime: OwnerSessionLifetimeObservationV1::NonExpiring,
                ..
            } if shard == *publication.target()
        ));
    }

    #[test]
    fn planned_owner_commands_are_object_safe_and_replay_exactly() {
        let (store, intent) = planned_owner_store();
        let store: Arc<dyn ControlStore> = Arc::new(store);

        let prepared = execute_prepare(store.as_ref(), intent.clone());
        let (plan, prepared_claim, prepared_sentinel) = match prepared {
            PrepareOwnerAdmissionResultV1::Prepared {
                plan,
                claim,
                sentinel,
            } => (*plan, claim, sentinel),
            result => panic!("expected Prepared, got {result:?}"),
        };
        assert_ne!(plan.lease().lease_id, 0);
        assert_eq!(
            store.get_logical_shard(&shard_id(1)).unwrap(),
            Some(intent.expected_unowned_shard().clone())
        );

        match execute_prepare(store.as_ref(), intent.clone()) {
            PrepareOwnerAdmissionResultV1::Prepared {
                plan: replayed_plan,
                claim,
                sentinel,
            } => {
                assert_eq!(*replayed_plan, plan);
                assert_eq!(claim, prepared_claim);
                assert_eq!(sentinel, prepared_sentinel);
            }
            result => panic!("expected Prepared replay, got {result:?}"),
        }

        match execute_commit(store.as_ref(), plan.clone()) {
            CommitOwnerAdmissionResultV1::Committed { shard, lease, .. } => {
                assert_eq!(shard.state, LogicalShardState::Recovering);
                assert_eq!(lease, *plan.lease());
            }
            result => panic!("expected Committed, got {result:?}"),
        }
        assert!(matches!(
            execute_reconcile_plan(store.as_ref(), plan.clone()),
            ReconcileOwnerAdmissionResultV1::Committed { .. }
        ));

        let (terminal_shard, terminal_claim) =
            match execute_released_terminate(store.as_ref(), plan.clone()) {
                TerminateOwnerAdmissionResultV1::Terminated { shard, claim } => (shard, claim),
                result => panic!("expected Terminated, got {result:?}"),
            };
        match execute_released_terminate(store.as_ref(), plan.clone()) {
            TerminateOwnerAdmissionResultV1::AlreadyTerminated { shard, claim } => {
                assert_eq!(shard, terminal_shard);
                assert_eq!(claim, terminal_claim);
            }
            result => panic!("expected AlreadyTerminated, got {result:?}"),
        }

        let successor_intent = OwnerAdmissionIntentV1::successor(
            intent.admission().clone(),
            terminal_shard,
            terminal_claim,
            node("node-b"),
            incarnation(2),
            "node-b:7000".to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([2; 32]).unwrap(),
        )
        .unwrap();
        let successor_plan = match execute_prepare(store.as_ref(), successor_intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected successor Prepared, got {result:?}"),
        };
        let successor_shard = match execute_commit(store.as_ref(), successor_plan) {
            CommitOwnerAdmissionResultV1::Committed { shard, .. } => shard,
            result => panic!("expected successor Committed, got {result:?}"),
        };
        match execute_released_terminate(store.as_ref(), plan) {
            TerminateOwnerAdmissionResultV1::Superseded { shard, .. } => {
                assert_eq!(shard, successor_shard);
            }
            result => panic!("expected Superseded, got {result:?}"),
        }
    }

    #[test]
    fn invalid_higher_epoch_record_does_not_masquerade_as_superseded() {
        let (store, intent) = planned_owner_store();
        let plan = match execute_prepare(&store, intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        assert!(matches!(
            execute_commit(&store, plan.clone()),
            CommitOwnerAdmissionResultV1::Committed { .. }
        ));
        let mut invalid_higher = match execute_released_terminate(&store, plan.clone()) {
            TerminateOwnerAdmissionResultV1::Terminated { shard, .. } => shard,
            result => panic!("expected Terminated, got {result:?}"),
        };
        invalid_higher.owner_epoch = Some(OwnerEpoch::new(2).unwrap());
        invalid_higher.owner_incarnation_id = Some(incarnation(2));
        invalid_higher.lease_id = 99;
        store
            .state
            .lock()
            .unwrap()
            .logical_shards
            .insert(shard_id(1), invalid_higher);

        assert!(matches!(
            execute_released_terminate(&store, plan),
            TerminateOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::SupersedingRecordInvalid
            )
        ));
    }

    #[test]
    fn prepare_and_ambiguity_seal_share_one_candidate_claim_cas() {
        let (sealed_store, sealed_intent) = planned_owner_store();
        let sealed_claim = match execute_reconcile_intent(&sealed_store, sealed_intent.clone()) {
            ReconcileOwnerAdmissionResultV1::Rejected { claim } => claim,
            result => panic!("expected ambiguity seal, got {result:?}"),
        };
        assert!(matches!(
            sealed_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Rejected { .. }
        ));
        match execute_prepare(&sealed_store, sealed_intent) {
            PrepareOwnerAdmissionResultV1::Rejected { claim } => {
                assert_eq!(claim, sealed_claim);
            }
            result => panic!("expected sealed rejection replay, got {result:?}"),
        }

        let (prepared_store, prepared_intent) = planned_owner_store();
        let prepared_plan = match execute_prepare(&prepared_store, prepared_intent.clone()) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        match execute_reconcile_intent(&prepared_store, prepared_intent) {
            ReconcileOwnerAdmissionResultV1::Prepared { plan, .. } => {
                assert_eq!(*plan, prepared_plan);
            }
            result => panic!("expected Prepared reconciliation, got {result:?}"),
        }
    }

    #[test]
    fn commit_and_abort_have_one_exact_prepared_winner_in_both_orders() {
        let (commit_store, commit_intent) = planned_owner_store();
        let commit_plan = match execute_prepare(&commit_store, commit_intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        assert!(matches!(
            execute_commit(&commit_store, commit_plan.clone()),
            CommitOwnerAdmissionResultV1::Committed { .. }
        ));
        assert!(matches!(
            execute_abort(
                &commit_store,
                commit_plan.clone(),
                OwnerAdmissionAbortReasonV1::OwnerCasRejected,
            ),
            AbortOwnerAdmissionResultV1::NotDispatched(
                AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect
            )
        ));
        assert!(matches!(
            execute_reconcile_plan(&commit_store, commit_plan),
            ReconcileOwnerAdmissionResultV1::Committed { .. }
        ));

        let (abort_store, abort_intent) = planned_owner_store();
        let abort_plan = match execute_prepare(&abort_store, abort_intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        assert!(matches!(
            execute_abort(
                &abort_store,
                abort_plan.clone(),
                OwnerAdmissionAbortReasonV1::OwnerCasRejected,
            ),
            AbortOwnerAdmissionResultV1::Aborted { .. }
        ));
        assert!(matches!(
            execute_commit(&abort_store, abort_plan.clone()),
            CommitOwnerAdmissionResultV1::NotDispatched(
                CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect
            )
        ));
        assert!(matches!(
            execute_reconcile_plan(&abort_store, abort_plan),
            ReconcileOwnerAdmissionResultV1::DurableConflict { claim, .. }
                if matches!(claim.phase(), OwnerAdmissionClaimPhaseV1::Aborted { .. })
        ));
    }

    #[test]
    fn same_candidate_key_with_foreign_intent_is_never_adopted() {
        let (store, intent) = planned_owner_store();
        let first_claim = match execute_prepare(&store, intent.clone()) {
            PrepareOwnerAdmissionResultV1::Prepared { claim, .. } => claim,
            result => panic!("expected Prepared, got {result:?}"),
        };
        let foreign = fresh_owner_intent(&store, 1, 9, "node-a");

        match execute_prepare(&store, foreign.clone()) {
            PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim } => {
                assert_eq!(claim, first_claim);
            }
            result => panic!("expected foreign-claim conflict, got {result:?}"),
        }
        assert!(matches!(
            execute_reconcile_intent(&store, foreign),
            ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim }
                if claim == first_claim
        ));
    }

    #[test]
    fn non_expiring_missing_sentinel_or_session_is_durable_inconsistency() {
        let (prepared_store, prepared_intent) = planned_owner_store();
        let prepared_plan = match execute_prepare(&prepared_store, prepared_intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        prepared_store
            .state
            .lock()
            .unwrap()
            .owner_admission_sentinels
            .remove(&shard_id(1));
        assert!(matches!(
            execute_reconcile_plan(&prepared_store, prepared_plan.clone()),
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven
            )
        ));
        assert!(matches!(
            execute_abort(
                &prepared_store,
                prepared_plan,
                OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit,
            ),
            AbortOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PreparedSentinelAbsenceUnproven
            )
        ));

        let (committed_store, committed_intent) = planned_owner_store();
        let committed_plan = match execute_prepare(&committed_store, committed_intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        assert!(matches!(
            execute_commit(&committed_store, committed_plan.clone()),
            CommitOwnerAdmissionResultV1::Committed { .. }
        ));
        committed_store
            .state
            .lock()
            .unwrap()
            .owner_sessions
            .remove(&shard_id(1));
        assert!(matches!(
            execute_reconcile_plan(&committed_store, committed_plan.clone()),
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven
            )
        ));
        assert!(matches!(
            execute_released_terminate(&committed_store, committed_plan),
            TerminateOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::CommittedSessionAbsenceUnproven
            )
        ));
    }

    #[test]
    fn aborted_successor_does_not_burn_the_next_owner_epoch() {
        let (store, initial_intent) = planned_owner_store();
        let initial_plan = match execute_prepare(&store, initial_intent) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        assert!(matches!(
            execute_commit(&store, initial_plan.clone()),
            CommitOwnerAdmissionResultV1::Committed { .. }
        ));
        let (released, predecessor) = match execute_released_terminate(&store, initial_plan) {
            TerminateOwnerAdmissionResultV1::Terminated { shard, claim } => (shard, claim),
            result => panic!("expected Terminated, got {result:?}"),
        };

        let first_successor =
            successor_owner_intent(&store, released.clone(), predecessor.clone(), 2, "node-b");
        let first_plan = match execute_prepare(&store, first_successor) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected successor Prepared, got {result:?}"),
        };
        assert!(matches!(
            execute_abort(
                &store,
                first_plan.clone(),
                OwnerAdmissionAbortReasonV1::OwnerCasRejected,
            ),
            AbortOwnerAdmissionResultV1::Aborted { .. }
        ));

        let next_successor = successor_owner_intent(&store, released, predecessor, 3, "node-c");
        let next_plan = match execute_prepare(&store, next_successor) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected next successor Prepared, got {result:?}"),
        };
        assert_eq!(
            first_plan.intent().planned_epoch(),
            next_plan.intent().planned_epoch()
        );
        assert_ne!(
            first_plan.intent().owner_incarnation_id(),
            next_plan.intent().owner_incarnation_id()
        );
    }

    #[test]
    fn durable_plan_without_candidate_claim_fails_closed() {
        let (store, intent) = planned_owner_store();
        let plan = match execute_prepare(&store, intent.clone()) {
            PrepareOwnerAdmissionResultV1::Prepared { plan, .. } => *plan,
            result => panic!("expected Prepared, got {result:?}"),
        };
        store
            .state
            .lock()
            .unwrap()
            .owner_admission_claims
            .remove(&owner_admission_claim_key(
                intent.logical_shard_id(),
                intent.owner_incarnation_id(),
            ));

        assert!(matches!(
            execute_reconcile_plan(&store, plan),
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanWithoutPreparedClaim
            )
        ));
        assert!(matches!(
            execute_prepare(&store, intent),
            PrepareOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::PlanWithoutPreparedClaim
            )
        ));
    }

    #[test]
    fn fresh_root_provisioning_replays_after_activation_and_serving() {
        let store = InMemoryControlStore::new();
        let requested_placement = initial_placement(1, 1);
        let requested_authority = initial_authority(1);
        let created = store
            .provision_fresh_root(requested_placement.clone(), requested_authority.clone())
            .unwrap();
        assert_eq!(
            created.disposition,
            FreshRootProvisioningDisposition::Created
        );
        assert_eq!(created.root_placement, requested_placement);
        assert_eq!(created.metadata_authority, requested_authority);
        assert_eq!(
            created.logical_shard,
            LogicalShardRecord::unassigned(shard_id(1))
        );

        let replayed = store
            .provision_fresh_root(requested_placement.clone(), requested_authority.clone())
            .unwrap();
        assert_eq!(
            replayed.disposition,
            FreshRootProvisioningDisposition::Replayed
        );

        let active = next_placement(&requested_placement, RootPlacementLifecycle::Active);
        store
            .compare_and_set_root_placement(&requested_placement, active.clone())
            .unwrap();
        let activated_replay = store
            .provision_fresh_root(requested_placement.clone(), requested_authority.clone())
            .unwrap();
        assert_eq!(activated_replay.root_placement, active);

        let admission = stable_admission(&store, 1);
        let lease = store
            .acquire_owner(
                &admission,
                node("node-a"),
                incarnation(1),
                "node-a:7000".to_owned(),
            )
            .unwrap();
        store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(log_ref(1, 1, "state-1")),
                    durable_lsn: 1,
                },
            )
            .unwrap();
        let serving_replay = store
            .provision_fresh_root(requested_placement.clone(), requested_authority.clone())
            .unwrap();
        assert_eq!(
            serving_replay.disposition,
            FreshRootProvisioningDisposition::Replayed
        );
        assert_eq!(
            serving_replay.logical_shard.state,
            LogicalShardState::Serving
        );
        assert_eq!(serving_replay.logical_shard.durable_lsn, 1);

        {
            let mut state = store.state.lock().unwrap();
            state.owner_sessions.get_mut(&shard_id(1)).unwrap().owner = node("corrupt-session");
        }
        assert!(matches!(
            store.provision_fresh_root(requested_placement, requested_authority),
            Err(ControlError::FreshRootProvisioningConflict { .. })
        ));
    }

    #[test]
    fn fresh_root_provisioning_rejects_crash_partial_state_without_adoption() {
        let store = InMemoryControlStore::new();
        store.create_logical_shard(shard_id(1)).unwrap();

        assert!(matches!(
            store.provision_fresh_root(initial_placement(1, 1), initial_authority(1)),
            Err(ControlError::FreshRootProvisioningConflict { .. })
        ));
        assert!(store
            .get_metadata_authority(&shard_id(1))
            .unwrap()
            .is_none());
        assert!(store.get_root_placement(&root_id(1)).unwrap().is_none());

        let store = InMemoryControlStore::new();
        store.create_logical_shard(shard_id(1)).unwrap();
        store
            .create_metadata_authority(initial_authority(1))
            .unwrap();
        assert!(matches!(
            store.provision_fresh_root(initial_placement(1, 1), initial_authority(1)),
            Err(ControlError::FreshRootProvisioningConflict { .. })
        ));
        assert!(store.get_root_placement(&root_id(1)).unwrap().is_none());
    }

    #[test]
    fn fresh_root_provisioning_fences_exact_layout_and_rejects_partitioned_profile() {
        let partitioned = RootPlacement {
            layout_profile: RootLayoutProfile::PartitionedRoot,
            partition_id: RootPartitionId::from_bytes([7; 16]),
            ..initial_placement(1, 1)
        };
        let unsupported = InMemoryControlStore::new();
        assert!(matches!(
            unsupported.provision_fresh_root(partitioned, initial_authority(1)),
            Err(ControlError::RootLayoutNotQualified {
                profile: RootLayoutProfile::PartitionedRoot,
                ..
            })
        ));
        assert!(unsupported
            .get_root_placement(&root_id(1))
            .unwrap()
            .is_none());
        assert!(unsupported
            .get_logical_shard(&shard_id(1))
            .unwrap()
            .is_none());

        let requested = initial_placement(1, 1);
        for mutate in [
            |stored: &mut RootPlacement| {
                stored.layout_generation = RootLayoutGeneration::new(2).unwrap()
            },
            |stored: &mut RootPlacement| stored.partition_id = RootPartitionId::from_bytes([8; 16]),
        ] {
            let store = InMemoryControlStore::new();
            store
                .provision_fresh_root(requested.clone(), initial_authority(1))
                .unwrap();
            mutate(
                store
                    .state
                    .lock()
                    .unwrap()
                    .root_placements
                    .get_mut(&root_id(1))
                    .unwrap(),
            );
            assert!(matches!(
                store.provision_fresh_root(requested.clone(), initial_authority(1)),
                Err(ControlError::FreshRootProvisioningConflict { .. })
            ));
        }
    }

    #[test]
    fn fresh_root_provisioning_rejects_different_binding_affinity_and_partial_session() {
        let invalid = InMemoryControlStore::new();
        assert!(matches!(
            invalid.provision_fresh_root(initial_placement(1, 1), initial_authority(2)),
            Err(ControlError::InvalidFreshRootProvisioning { .. })
        ));

        let store = InMemoryControlStore::new();
        let placement = initial_placement(1, 1);
        let authority = initial_authority(1);
        store
            .provision_fresh_root(placement.clone(), authority.clone())
            .unwrap();

        let mut different_authority = authority.clone();
        different_authority.active = authority_binding(2, "fdb-primary");
        assert!(matches!(
            store.provision_fresh_root(placement.clone(), different_authority),
            Err(ControlError::FreshRootProvisioningConflict { .. })
        ));
        assert!(matches!(
            store.provision_fresh_root(initial_placement(1, 2), initial_authority(2)),
            Err(ControlError::FreshRootProvisioningConflict { .. })
        ));

        let partial = InMemoryControlStore::new();
        {
            let mut state = partial.state.lock().unwrap();
            state.owner_sessions.insert(
                shard_id(1),
                LogicalShardLease {
                    logical_shard_id: shard_id(1),
                    owner: node("orphan"),
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    owner_incarnation_id: incarnation(1),
                    lease_id: 7,
                    authority: authority.fence(),
                },
            );
        }
        assert!(matches!(
            partial.provision_fresh_root(placement, authority),
            Err(ControlError::FreshRootProvisioningConflict { .. })
        ));
    }

    #[test]
    fn concurrent_identical_fresh_root_provisioning_creates_once() {
        let store = Arc::new(InMemoryControlStore::new());
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                store
                    .provision_fresh_root(initial_placement(1, 1), initial_authority(1))
                    .unwrap()
                    .disposition
            }));
        }
        barrier.wait();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == FreshRootProvisioningDisposition::Created)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == FreshRootProvisioningDisposition::Replayed)
                .count(),
            1
        );
    }

    #[test]
    fn root_placement_must_exist_before_owner_write_authority() {
        let store = InMemoryControlStore::new();
        let logical_shard_id = shard_id(1);
        store.create_logical_shard(logical_shard_id).unwrap();
        store
            .create_metadata_authority(initial_authority(1))
            .unwrap();

        let placement = next_placement(&initial_placement(1, 1), RootPlacementLifecycle::Active);
        let admission = OwnerServingAdmission::stable(placement, initial_authority(1)).unwrap();

        assert!(matches!(
            store.acquire_owner(
                &admission,
                node("node-a"),
                incarnation(1),
                "node-a:7000".to_owned(),
            ),
            Err(ControlError::RootPlacementCasConflict { actual: None, .. })
        ));

        let initial = initial_placement(1, 1);
        store.create_root_placement(initial.clone()).unwrap();
        store
            .compare_and_set_root_placement(&initial, admission.placement().clone())
            .unwrap();
        assert_eq!(
            store
                .acquire_owner(
                    &admission,
                    node("node-a"),
                    incarnation(1),
                    "node-a:7000".to_owned(),
                )
                .unwrap()
                .owner_epoch
                .get(),
            1
        );
    }

    #[test]
    fn owner_admission_rejects_an_unqualified_durable_layout() {
        let placement = RootPlacement {
            layout_profile: RootLayoutProfile::PartitionedRoot,
            partition_id: RootPartitionId::from_bytes([7; 16]),
            ..next_placement(&initial_placement(1, 1), RootPlacementLifecycle::Active)
        };

        assert!(matches!(
            OwnerServingAdmission::stable(placement, initial_authority(1)),
            Err(ControlError::RootLayoutNotQualified {
                profile: RootLayoutProfile::PartitionedRoot,
                ..
            })
        ));
    }

    #[test]
    fn owner_acquisition_requires_an_explicit_metadata_authority() {
        let store = InMemoryControlStore::new();
        store.create_logical_shard(shard_id(1)).unwrap();
        let initial = initial_placement(1, 1);
        store.create_root_placement(initial.clone()).unwrap();
        let placement = activate_placed_shard(&store, &initial);
        let admission = OwnerServingAdmission::stable(placement, initial_authority(1)).unwrap();

        assert!(matches!(
            store.acquire_owner(
                &admission,
                node("node-a"),
                incarnation(1),
                "node-a:7000".to_owned(),
            ),
            Err(ControlError::MetadataAuthorityCasConflict { actual: None, .. })
        ));
    }

    #[test]
    fn stale_target_placement_cannot_acquire_when_another_root_is_active_on_the_shard() {
        let store = InMemoryControlStore::new();
        let first = create_placed_shard(&store, 1, 1);
        activate_placed_shard(&store, &first);
        let admission = stable_admission(&store, 1);

        let second = initial_placement(2, 1);
        store.create_root_placement(second.clone()).unwrap();
        let second = activate_placed_shard(&store, &second);

        let draining = next_placement(admission.placement(), RootPlacementLifecycle::Draining);
        store
            .compare_and_set_root_placement(admission.placement(), draining.clone())
            .unwrap();

        assert!(matches!(
            store.acquire_owner(
                &admission,
                node("node-a"),
                incarnation(1),
                "node-a:7000".to_owned(),
            ),
            Err(ControlError::RootPlacementCasConflict {
                actual: Some(actual),
                ..
            }) if *actual == draining
        ));
        let reactivated = next_placement(&draining, RootPlacementLifecycle::Active);
        store
            .compare_and_set_root_placement(&draining, reactivated.clone())
            .unwrap();
        assert!(matches!(
            store.acquire_owner(
                &admission,
                node("node-a"),
                incarnation(1),
                "node-a:7000".to_owned(),
            ),
            Err(ControlError::RootPlacementCasConflict {
                actual: Some(actual),
                ..
            }) if *actual == reactivated
        ));
        assert_eq!(
            store.get_root_placement(&root_id(2)).unwrap(),
            Some(second),
            "another Active root on the shard must not authorize the stale target"
        );
        assert!(store
            .get_logical_shard(&shard_id(1))
            .unwrap()
            .unwrap()
            .owner
            .is_none());
    }

    #[test]
    fn placement_change_after_acquire_fences_renewal_and_serving_publication() {
        let store = InMemoryControlStore::new();
        let (lease, admission) = acquire_placed_shard(&store, 1, 1);
        let draining = next_placement(admission.placement(), RootPlacementLifecycle::Draining);
        store
            .compare_and_set_root_placement(admission.placement(), draining)
            .unwrap();

        assert!(matches!(
            store.renew_owner(&lease, &admission),
            Err(ControlError::RootPlacementCasConflict { .. })
        ));
        assert!(matches!(
            store.mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            ),
            Err(ControlError::RootPlacementCasConflict { .. })
        ));
        assert_eq!(
            store
                .get_logical_shard(&shard_id(1))
                .unwrap()
                .unwrap()
                .state,
            LogicalShardState::Recovering
        );
    }

    #[test]
    fn authority_migration_after_acquire_fences_renewal_and_serving_publication() {
        let store = InMemoryControlStore::new();
        let (lease, admission) = acquire_placed_shard(&store, 1, 1);
        let mut preparing = next_authority(admission.authority());
        preparing.migration = Some(MetadataMigration {
            migration_id: OperationId::from_bytes([0x51; 16]),
            source: admission.authority().active.clone(),
            target: authority_binding(2, "fdb-primary"),
            phase: MetadataMigrationPhase::Preparing,
            source_frontier: None,
            target_frontier: None,
            cutover_frontier: None,
            source_quiesce_receipt: None,
            target_activation_token: None,
        });
        store
            .compare_and_set_metadata_authority(admission.authority(), preparing.clone())
            .unwrap();

        assert!(matches!(
            OwnerServingAdmission::stable(admission.placement().clone(), preparing),
            Err(ControlError::MetadataAuthorityAdmission { .. })
        ));
        assert!(matches!(
            store.renew_owner(&lease, &admission),
            Err(ControlError::MetadataAuthorityCasConflict { .. })
        ));
        assert!(matches!(
            store.mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            ),
            Err(ControlError::MetadataAuthorityCasConflict { .. })
        ));
        assert_eq!(
            store
                .get_logical_shard(&shard_id(1))
                .unwrap()
                .unwrap()
                .state,
            LogicalShardState::Recovering
        );
    }

    #[test]
    fn first_authority_cannot_adopt_a_previously_owned_shard() {
        let store = InMemoryControlStore::new();
        store.create_logical_shard(shard_id(1)).unwrap();
        {
            let mut state = store.state.lock().unwrap();
            let shard = state.logical_shards.get_mut(&shard_id(1)).unwrap();
            shard.owner_epoch = Some(OwnerEpoch::new(1).unwrap());
            shard.owner_incarnation_id = Some(incarnation(1));
        }

        assert!(matches!(
            store.create_metadata_authority(initial_authority(1)),
            Err(ControlError::MetadataAuthorityAdoptionRejected {
                logical_shard_id,
                ..
            }) if logical_shard_id == shard_id(1)
        ));
    }

    #[test]
    fn metadata_authority_create_and_cas_are_idempotent_and_exact() {
        let store = InMemoryControlStore::new();
        store.create_logical_shard(shard_id(1)).unwrap();
        let authority = initial_authority(1);
        assert_eq!(
            store.create_metadata_authority(authority.clone()).unwrap(),
            authority
        );
        assert_eq!(
            store.create_metadata_authority(authority.clone()).unwrap(),
            authority
        );

        let mut conflicting = authority.clone();
        conflicting.active = authority_binding(2, "fdb-primary");
        assert!(matches!(
            store.create_metadata_authority(conflicting),
            Err(ControlError::MetadataAuthorityAlreadyExists(id)) if id == shard_id(1)
        ));

        let mut preparing = next_authority(&authority);
        preparing.migration = Some(MetadataMigration {
            migration_id: OperationId::from_bytes([3; 16]),
            source: authority.active.clone(),
            target: authority_binding(2, "fdb-primary"),
            phase: MetadataMigrationPhase::Preparing,
            source_frontier: None,
            target_frontier: None,
            cutover_frontier: None,
            source_quiesce_receipt: None,
            target_activation_token: None,
        });
        let mut stale_revision = preparing.clone();
        stale_revision.record_revision = authority.record_revision;
        assert!(matches!(
            store.compare_and_set_metadata_authority(&authority, stale_revision),
            Err(ControlError::InvalidMetadataAuthorityMutation { .. })
        ));
        assert_eq!(
            store
                .compare_and_set_metadata_authority(&authority, preparing.clone())
                .unwrap(),
            preparing
        );
        assert_eq!(
            store
                .compare_and_set_metadata_authority(&authority, preparing.clone())
                .unwrap(),
            preparing,
            "response replay returns the installed value"
        );

        let mut competing = preparing.clone();
        competing.migration.as_mut().unwrap().migration_id = OperationId::from_bytes([4; 16]);
        match store
            .compare_and_set_metadata_authority(&authority, competing.clone())
            .unwrap_err()
        {
            ControlError::MetadataAuthorityCasConflict { expected, actual } => {
                assert_eq!(*expected, authority);
                assert_eq!(actual.map(|record| *record), Some(preparing.clone()));
            }
            error => panic!("expected metadata authority CAS conflict, got {error}"),
        }

        let mut aborted = next_authority(&preparing);
        aborted.migration.as_mut().unwrap().phase = MetadataMigrationPhase::Aborted;
        store
            .compare_and_set_metadata_authority(&preparing, aborted.clone())
            .unwrap();
        let mut cleared = next_authority(&aborted);
        cleared.migration = None;
        store
            .compare_and_set_metadata_authority(&aborted, cleared.clone())
            .unwrap();
        assert_eq!(cleared.active, authority.active);
        assert_ne!(
            cleared, authority,
            "record revision prevents ABA after cleanup"
        );
        assert!(matches!(
            store.compare_and_set_metadata_authority(&authority, competing),
            Err(ControlError::MetadataAuthorityCasConflict { .. })
        ));
    }

    #[test]
    fn migration_quiescence_fences_owner_and_cutover_changes_the_authority_generation() {
        let store = InMemoryControlStore::new();
        let (lease, admission) = acquire_placed_shard(&store, 1, 1);
        store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(log_ref(1, 1, "state-1")),
                    durable_lsn: 1,
                },
            )
            .unwrap();
        let initial = store.get_metadata_authority(&shard_id(1)).unwrap().unwrap();
        assert_eq!(lease.authority, initial.fence());

        let target = authority_binding(2, "fdb-primary");
        let mut preparing = next_authority(&initial);
        preparing.migration = Some(MetadataMigration {
            migration_id: OperationId::from_bytes([3; 16]),
            source: initial.active.clone(),
            target: target.clone(),
            phase: MetadataMigrationPhase::Preparing,
            source_frontier: None,
            target_frontier: None,
            cutover_frontier: None,
            source_quiesce_receipt: None,
            target_activation_token: None,
        });
        store
            .compare_and_set_metadata_authority(&initial, preparing.clone())
            .unwrap();

        let mut copying = next_authority(&preparing);
        copying.migration.as_mut().unwrap().phase = MetadataMigrationPhase::Copying;
        copying.migration.as_mut().unwrap().source_frontier = Some(frontier(7));
        store
            .compare_and_set_metadata_authority(&preparing, copying.clone())
            .unwrap();

        let mut catching_up = next_authority(&copying);
        catching_up.migration.as_mut().unwrap().phase = MetadataMigrationPhase::CatchingUp;
        catching_up.migration.as_mut().unwrap().target_frontier = Some(frontier(7));
        store
            .compare_and_set_metadata_authority(&copying, catching_up.clone())
            .unwrap();

        let mut quiescing = next_authority(&catching_up);
        quiescing.migration.as_mut().unwrap().phase = MetadataMigrationPhase::Quiescing;
        store
            .compare_and_set_metadata_authority(&catching_up, quiescing.clone())
            .unwrap();
        assert!(matches!(
            store.renew_owner(&lease, &admission),
            Err(ControlError::MetadataAuthorityCasConflict { .. })
        ));

        let receipt = source_receipt(&quiescing, lease.owner_epoch, frontier(7));
        let mut receipt_record = next_authority(&quiescing);
        receipt_record
            .migration
            .as_mut()
            .unwrap()
            .source_quiesce_receipt = Some(receipt);
        store
            .compare_and_set_metadata_authority(&quiescing, receipt_record.clone())
            .unwrap();

        let mut ready = next_authority(&receipt_record);
        ready.migration.as_mut().unwrap().phase = MetadataMigrationPhase::ReadyToCutover;
        ready.migration.as_mut().unwrap().cutover_frontier = Some(frontier(7));
        assert!(matches!(
            store.compare_and_set_metadata_authority(&receipt_record, ready.clone()),
            Err(ControlError::MetadataAuthorityAdmission { .. })
        ));

        {
            let mut state = store.state.lock().unwrap();
            state.owner_sessions.remove(&shard_id(1));
        }
        store
            .compare_and_set_metadata_authority(&receipt_record, ready.clone())
            .unwrap();
        let cleaned = store.get_logical_shard(&shard_id(1)).unwrap().unwrap();
        assert!(cleaned.owner.is_none());
        assert_eq!(cleaned.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(
            cleaned.owner_incarnation_id,
            Some(lease.owner_incarnation_id)
        );
        assert_eq!(cleaned.lease_id, 0);
        assert_eq!(cleaned.durable_lsn, 1);
        assert_eq!(cleaned.log.unwrap().digest, "state-1");

        let mut changed_frontier = next_authority(&ready);
        changed_frontier.authority_generation = MetadataAuthorityGeneration::new(2).unwrap();
        changed_frontier.active = target.clone();
        let changed_migration = changed_frontier.migration.as_mut().unwrap();
        changed_migration.phase = MetadataMigrationPhase::CutoverComplete;
        changed_migration.source_frontier = Some(frontier(8));
        changed_migration.target_frontier = Some(frontier(8));
        changed_migration.cutover_frontier = Some(frontier(8));
        assert!(matches!(
            store.compare_and_set_metadata_authority(&ready, changed_frontier),
            Err(ControlError::InvalidMetadataAuthorityMutation { .. })
        ));

        let mut complete = next_authority(&ready);
        complete.authority_generation = MetadataAuthorityGeneration::new(2).unwrap();
        complete.active = target;
        let complete_migration = complete.migration.as_mut().unwrap();
        complete_migration.phase = MetadataMigrationPhase::CutoverComplete;
        complete_migration.target_activation_token = Some(TargetActivationToken::for_cutover(
            &receipt,
            complete_migration.target.authority_id,
            complete.authority_generation,
        ));
        store
            .compare_and_set_metadata_authority(&ready, complete.clone())
            .unwrap();
        assert_eq!(
            store
                .get_metadata_authority(&shard_id(1))
                .unwrap()
                .unwrap()
                .fence(),
            complete.fence()
        );

        let mut stable = next_authority(&complete);
        stable.migration = None;
        store
            .compare_and_set_metadata_authority(&complete, stable.clone())
            .unwrap();
        let successor_admission = stable_admission(&store, 1);

        let successor = store
            .acquire_successor(
                &successor_admission,
                lease.owner_epoch,
                node("node-b"),
                incarnation(2),
                "node-b:7000".to_owned(),
            )
            .unwrap();
        assert_eq!(successor.authority, complete.fence());
        let mut stale_authority = successor.clone();
        stale_authority.authority = lease.authority;
        assert!(matches!(
            store.renew_owner(&stale_authority, &successor_admission),
            Err(ControlError::MetadataAuthorityAdmission { .. })
        ));
    }

    #[test]
    fn source_receipt_rejects_a_stale_owner_copy_before_ready() {
        let store = InMemoryControlStore::new();
        let (first, admission) = acquire_placed_shard(&store, 1, 1);
        store.release_owner(&first).unwrap();
        let current = store
            .acquire_successor(
                &admission,
                first.owner_epoch,
                node("node-current"),
                incarnation(2),
                "node-current:7000".to_owned(),
            )
            .unwrap();
        let initial = store.get_metadata_authority(&shard_id(1)).unwrap().unwrap();
        let mut preparing = next_authority(&initial);
        preparing.migration = Some(MetadataMigration {
            migration_id: OperationId::from_bytes([0x41; 16]),
            source: initial.active.clone(),
            target: authority_binding(2, "fdb-primary"),
            phase: MetadataMigrationPhase::Preparing,
            source_frontier: None,
            target_frontier: None,
            cutover_frontier: None,
            source_quiesce_receipt: None,
            target_activation_token: None,
        });
        store
            .compare_and_set_metadata_authority(&initial, preparing.clone())
            .unwrap();
        let mut copying = next_authority(&preparing);
        let migration = copying.migration.as_mut().unwrap();
        migration.phase = MetadataMigrationPhase::Copying;
        migration.source_frontier = Some(frontier(9));
        store
            .compare_and_set_metadata_authority(&preparing, copying.clone())
            .unwrap();
        let mut catching_up = next_authority(&copying);
        let migration = catching_up.migration.as_mut().unwrap();
        migration.phase = MetadataMigrationPhase::CatchingUp;
        migration.target_frontier = Some(frontier(9));
        store
            .compare_and_set_metadata_authority(&copying, catching_up.clone())
            .unwrap();
        let mut quiescing = next_authority(&catching_up);
        quiescing.migration.as_mut().unwrap().phase = MetadataMigrationPhase::Quiescing;
        store
            .compare_and_set_metadata_authority(&catching_up, quiescing.clone())
            .unwrap();

        let mut stale_receipt_record = next_authority(&quiescing);
        stale_receipt_record
            .migration
            .as_mut()
            .unwrap()
            .source_quiesce_receipt =
            Some(source_receipt(&quiescing, first.owner_epoch, frontier(9)));
        assert!(matches!(
            store.compare_and_set_metadata_authority(&quiescing, stale_receipt_record),
            Err(ControlError::MetadataAuthorityAdmission { .. })
        ));

        let mut current_receipt_record = next_authority(&quiescing);
        current_receipt_record
            .migration
            .as_mut()
            .unwrap()
            .source_quiesce_receipt =
            Some(source_receipt(&quiescing, current.owner_epoch, frontier(9)));
        store
            .compare_and_set_metadata_authority(&quiescing, current_receipt_record)
            .unwrap();
    }

    #[test]
    fn migration_rejects_skipped_phases_and_unverified_frontiers() {
        let initial = initial_authority(1);
        let mut invalid = next_authority(&initial);
        invalid.migration = Some(MetadataMigration {
            migration_id: OperationId::from_bytes([3; 16]),
            source: initial.active.clone(),
            target: authority_binding(2, "fdb-primary"),
            phase: MetadataMigrationPhase::Copying,
            source_frontier: None,
            target_frontier: None,
            cutover_frontier: None,
            source_quiesce_receipt: None,
            target_activation_token: None,
        });
        assert!(matches!(
            validate_metadata_authority_update(&initial, &invalid),
            Err(ControlError::InvalidMetadataAuthorityMutation { .. })
        ));

        invalid.migration.as_mut().unwrap().phase = MetadataMigrationPhase::ReadyToCutover;
        invalid.migration.as_mut().unwrap().source_frontier = Some(frontier(7));
        invalid.migration.as_mut().unwrap().target_frontier = Some(frontier(8));
        invalid.migration.as_mut().unwrap().cutover_frontier = Some(frontier(7));
        assert!(matches!(
            validate_metadata_authority_record(&invalid),
            Err(ControlError::InvalidMetadataAuthorityMutation { .. })
        ));
    }

    #[test]
    fn populated_root_placement_never_changes_logical_shard_affinity() {
        let store = InMemoryControlStore::new();
        store.create_logical_shard(shard_id(1)).unwrap();
        store.create_logical_shard(shard_id(2)).unwrap();
        let placement = initial_placement(1, 1);
        store.create_root_placement(placement.clone()).unwrap();

        let conflicting = initial_placement(1, 2);
        assert!(matches!(
            store.create_root_placement(conflicting),
            Err(ControlError::ImmutableShardAffinity { .. })
        ));

        let mut mutated = next_placement(&placement, RootPlacementLifecycle::Active);
        mutated.logical_shard_id = shard_id(2);
        assert!(matches!(
            store.compare_and_set_root_placement(&placement, mutated),
            Err(ControlError::ImmutableShardAffinity { .. })
        ));

        let mut changed_layout = next_placement(&placement, RootPlacementLifecycle::Active);
        changed_layout.layout_generation = RootLayoutGeneration::new(2).unwrap();
        assert!(matches!(
            store.compare_and_set_root_placement(&placement, changed_layout),
            Err(ControlError::InvalidPlacementMutation { .. })
        ));

        let mut changed_partition = next_placement(&placement, RootPlacementLifecycle::Active);
        changed_partition.partition_id = RootPartitionId::from_bytes([4; 16]);
        assert!(matches!(
            store.compare_and_set_root_placement(&placement, changed_partition),
            Err(ControlError::InvalidPlacementMutation { .. })
        ));

        let partitioned = RootPlacement {
            layout_profile: RootLayoutProfile::PartitionedRoot,
            partition_id: RootPartitionId::from_bytes([5; 16]),
            ..placement
        };
        let partitioned_next = next_placement(&partitioned, RootPlacementLifecycle::Active);
        assert!(matches!(
            validate_root_placement_update(&partitioned, &partitioned_next),
            Err(ControlError::RootLayoutNotQualified { .. })
        ));
    }

    #[test]
    fn root_generation_cas_fences_concurrent_lifecycle_updates() {
        let store = InMemoryControlStore::new();
        let initial = create_placed_shard(&store, 1, 1);
        let active = next_placement(&initial, RootPlacementLifecycle::Active);
        assert_eq!(
            store
                .compare_and_set_root_placement(&initial, active.clone())
                .unwrap(),
            active
        );
        assert_eq!(
            store
                .compare_and_set_root_placement(&initial, active.clone())
                .unwrap(),
            active,
            "response replay is idempotent"
        );

        let retired = next_placement(&initial, RootPlacementLifecycle::Retired);
        assert!(matches!(
            store.compare_and_set_root_placement(&initial, retired),
            Err(ControlError::RootPlacementCasConflict { .. })
        ));
    }

    #[test]
    fn owner_epoch_cas_is_monotonic_and_fences_races() {
        let store = InMemoryControlStore::new();
        let (first, admission) = acquire_placed_shard(&store, 1, 1);
        assert_eq!(first.owner_epoch.get(), 1);
        let OwnerReleaseOutcome::Released(released) = store.release_owner(&first).unwrap() else {
            panic!("first release must perform the mutation");
        };
        assert_eq!(released.owner_epoch, Some(first.owner_epoch));
        assert_eq!(
            released.owner_incarnation_id,
            Some(first.owner_incarnation_id)
        );
        assert!(released.owner.is_none());
        assert!(matches!(
            store.release_owner(&first).unwrap(),
            OwnerReleaseOutcome::AlreadyReleased(record) if record == released
        ));

        let second = store
            .acquire_successor(
                &admission,
                first.owner_epoch,
                node("node-b"),
                incarnation(2),
                "node-b:7000".to_owned(),
            )
            .unwrap();
        assert_eq!(second.owner_epoch.get(), 2);
        assert_ne!(second.owner_incarnation_id, first.owner_incarnation_id);
        assert!(matches!(
            store.release_owner(&first).unwrap(),
            OwnerReleaseOutcome::Superseded(record)
                if record.owner_epoch == Some(second.owner_epoch)
                    && record.owner.as_ref() == Some(&second.owner)
        ));
        assert!(matches!(
            store.acquire_successor(
                &admission,
                first.owner_epoch,
                node("node-c"),
                incarnation(3),
                "node-c:7000".to_owned()
            ),
            Err(ControlError::StaleOwnerEpoch { .. })
        ));
        assert!(matches!(
            store.renew_owner(&first, &admission),
            Err(ControlError::NotOwner { .. }) | Err(ControlError::StaleLease(_))
        ));
    }

    #[test]
    fn same_node_and_endpoint_never_replace_exact_incarnation_authority() {
        let store = InMemoryControlStore::new();
        let (first, admission) = acquire_placed_shard(&store, 1, 1);
        store.release_owner(&first).unwrap();

        assert!(matches!(
            store.acquire_successor(
                &admission,
                first.owner_epoch,
                first.owner.clone(),
                first.owner_incarnation_id,
                "node-a:7000".to_owned(),
            ),
            Err(ControlError::InvalidRecord(reason))
                if reason.contains("new incarnation")
        ));

        let successor = store
            .acquire_successor(
                &admission,
                first.owner_epoch,
                first.owner.clone(),
                incarnation(2),
                "node-a:7000".to_owned(),
            )
            .unwrap();
        let mut foreign_incarnation = successor.clone();
        foreign_incarnation.owner_incarnation_id = incarnation(3);

        assert!(matches!(
            store.renew_owner(&foreign_incarnation, &admission),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            store.mark_serving(
                &foreign_incarnation,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            ),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            store.release_owner(&foreign_incarnation),
            Err(ControlError::InvalidRecord(reason))
                if reason.contains("different installed incarnations")
        ));
        assert!(matches!(
            store.release_owner(&first).unwrap(),
            OwnerReleaseOutcome::Superseded(_)
        ));

        let current = store.get_logical_shard(&shard_id(1)).unwrap().unwrap();
        assert_eq!(current.owner.as_ref(), Some(&successor.owner));
        assert_eq!(current.owner_epoch, Some(successor.owner_epoch));
        assert_eq!(
            current.owner_incarnation_id,
            Some(successor.owner_incarnation_id)
        );
        assert_eq!(current.lease_id, successor.lease_id);
    }

    #[test]
    fn zero_owner_incarnation_is_rejected_before_owner_installation() {
        let store = InMemoryControlStore::new();
        let placement = create_placed_shard(&store, 1, 1);
        activate_placed_shard(&store, &placement);
        let admission = stable_admission(&store, 1);

        assert!(matches!(
            store.acquire_owner(
                &admission,
                node("node-a"),
                incarnation(0),
                "node-a:7000".to_owned(),
            ),
            Err(ControlError::InvalidRecord(reason))
                if reason.contains("incarnation id must be non-zero")
        ));
        assert_eq!(
            store.get_logical_shard(&shard_id(1)).unwrap().unwrap(),
            LogicalShardRecord::unassigned(shard_id(1))
        );
        assert!(!store
            .state
            .lock()
            .unwrap()
            .owner_sessions
            .contains_key(&shard_id(1)));
    }

    #[test]
    fn list_operations_are_stably_sorted_by_typed_identity() {
        let store = InMemoryControlStore::new();
        for value in [3, 1, 2] {
            store.create_logical_shard(shard_id(value)).unwrap();
            store
                .create_root_placement(initial_placement(value, value))
                .unwrap();
        }
        let shards = store.list_logical_shards().unwrap();
        assert_eq!(
            shards
                .iter()
                .map(|record| record.logical_shard_id)
                .collect::<Vec<_>>(),
            vec![shard_id(1), shard_id(2), shard_id(3)]
        );
        let roots = store.list_root_placements().unwrap();
        assert_eq!(
            roots
                .iter()
                .map(|placement| placement.root_id)
                .collect::<Vec<_>>(),
            vec![root_id(1), root_id(2), root_id(3)]
        );
    }

    #[test]
    fn checkpoint_and_log_publication_merge_is_monotonic() {
        let store = InMemoryControlStore::new();
        let (lease, admission) = acquire_placed_shard(&store, 1, 1);
        let first_log = log_ref(1, 2, "state-2");
        let first = store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(first_log.clone()),
                    durable_lsn: 2,
                },
            )
            .unwrap();
        assert_eq!(first.log, Some(first_log));
        assert_eq!(first.durable_lsn, 2);

        assert!(matches!(
            store.mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(log_ref(1, 1, "state-1")),
                    durable_lsn: 1,
                }
            ),
            Err(ControlError::RecoveryPublicationConflict { .. })
        ));

        let checkpoint = CheckpointRef {
            object_key: "checkpoints/2".to_owned(),
            lsn: 2,
            image_bytes: 4096,
            image_digest: "image-2".to_owned(),
            digest: "state-2".to_owned(),
        };
        let checkpointed = store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: Some(checkpoint.clone()),
                    log: None,
                    durable_lsn: 2,
                },
            )
            .unwrap();
        assert_eq!(checkpointed.checkpoint, Some(checkpoint));
        assert!(checkpointed.log.is_none(), "checkpoint prunes covered log");

        let advanced = store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(log_ref(3, 4, "state-4")),
                    durable_lsn: 4,
                },
            )
            .unwrap();
        assert_eq!(advanced.durable_lsn, 4);
        assert_eq!(advanced.log.unwrap().durable_lsn, 4);
    }

    #[test]
    fn same_tail_log_identity_conflict_does_not_overwrite_durable_state() {
        let store = InMemoryControlStore::new();
        let (lease, admission) = acquire_placed_shard(&store, 1, 1);
        let durable = log_ref(1, 2, "state-2");
        store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(durable.clone()),
                    durable_lsn: 2,
                },
            )
            .unwrap();

        let mut conflict = durable.clone();
        conflict.segments[0].segment_key = "logs/other".to_owned();
        assert!(matches!(
            store.mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(conflict),
                    durable_lsn: 2,
                }
            ),
            Err(ControlError::RecoveryPublicationConflict { .. })
        ));
        assert_eq!(
            store.get_logical_shard(&shard_id(1)).unwrap().unwrap().log,
            Some(durable)
        );
    }

    #[test]
    fn empty_publication_must_confirm_current_durable_frontier() {
        let store = InMemoryControlStore::new();
        let (first, admission) = acquire_placed_shard(&store, 1, 1);
        let durable = log_ref(1, 2, "state-2");
        store
            .mark_serving(
                &first,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(durable.clone()),
                    durable_lsn: 2,
                },
            )
            .unwrap();
        store.release_owner(&first).unwrap();
        let successor = store
            .acquire_successor(
                &admission,
                first.owner_epoch,
                node("node-b"),
                incarnation(2),
                "node-b:7000".to_owned(),
            )
            .unwrap();

        assert!(matches!(
            store.mark_serving(
                &successor,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                }
            ),
            Err(ControlError::RecoveryPublicationConflict { .. })
        ));
        let serving = store
            .mark_serving(
                &successor,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 2,
                },
            )
            .unwrap();
        assert_eq!(serving.log, Some(durable));
        assert_eq!(serving.durable_lsn, 2);
        assert_eq!(serving.state, LogicalShardState::Serving);
    }

    #[test]
    fn malformed_log_chain_is_rejected() {
        let store = InMemoryControlStore::new();
        let (lease, admission) = acquire_placed_shard(&store, 1, 1);
        let malformed = LogRef {
            segments: Vec::new(),
            durable_lsn: 1,
            digest: "state-1".to_owned(),
        };
        assert!(matches!(
            store.mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(malformed),
                    durable_lsn: 1,
                }
            ),
            Err(ControlError::RecoveryPublicationConflict { .. })
        ));
    }

    #[test]
    fn never_owned_record_cannot_claim_recovery_state() {
        let mut record = LogicalShardRecord::unassigned(shard_id(1));
        record.checkpoint = Some(CheckpointRef {
            object_key: "checkpoints/0".to_owned(),
            lsn: 0,
            image_bytes: 1,
            image_digest: "image-0".to_owned(),
            digest: "state-0".to_owned(),
        });

        assert!(matches!(
            validate_logical_shard_record(&record),
            Err(ControlError::InvalidRecord(_))
        ));
    }
}
