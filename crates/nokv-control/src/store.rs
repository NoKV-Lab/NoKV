use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::types::endpoint_is_canonical;
use crate::{
    CheckpointRef, ControlError, LogRef, LogicalShardId, LogicalShardLease, LogicalShardRecord,
    LogicalShardState, NodeId, OwnerEpoch, RecoveryPublication, RootId, RootPlacement,
    RootPlacementLifecycle,
};

/// Durable control-plane operations, split between immutable root placement
/// and physical logical-shard ownership.
pub trait ControlStore: Send + Sync {
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

    /// Install the first owner epoch. This succeeds only for a never-owned
    /// logical shard with at least one non-retired root placement.
    fn acquire_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError>;

    /// Install a successor owner by exact comparison with the last durable
    /// owner epoch. A backend must also prove that the previous session is gone.
    fn acquire_successor(
        &self,
        logical_shard_id: &LogicalShardId,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError>;

    /// Rebind the lease/session of an unfinished recovery attempt without
    /// consuming another owner epoch. The previous session must be gone.
    fn reacquire_recovery(
        &self,
        logical_shard_id: &LogicalShardId,
        recovery_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError>;

    fn renew_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError>;

    /// Publish a caller-serialized recovery frontier and make the exact owner
    /// generation routable.
    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError>;

    /// End the live session for a boot that has not reached `Serving`, while
    /// retaining its durable recovery epoch for an exact retry.
    fn suspend_recovery(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<LogicalShardRecord, ControlError>;

    fn release_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError>;
}

#[derive(Default)]
struct InMemoryState {
    root_placements: BTreeMap<RootId, RootPlacement>,
    logical_shards: BTreeMap<LogicalShardId, LogicalShardRecord>,
    owner_sessions: BTreeMap<LogicalShardId, LogicalShardLease>,
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
                expected: expected.clone(),
                actual,
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

    fn acquire_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        validate_endpoint(&endpoint)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        ensure_root_placement(&state, logical_shard_id)?;
        let current = state
            .logical_shards
            .get(logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
        if let (Some(current_owner), Some(owner_epoch)) =
            (current.owner.clone(), current.owner_epoch)
        {
            return Err(ControlError::LogicalShardAlreadyOwned {
                logical_shard_id: *logical_shard_id,
                owner: current_owner,
                owner_epoch,
            });
        }
        let lease_id = allocate_lease_id(&mut state, *logical_shard_id)?;
        let (next, lease) = prepare_owner_acquisition(&current, None, owner, endpoint, lease_id)?;
        state.logical_shards.insert(*logical_shard_id, next);
        state
            .owner_sessions
            .insert(*logical_shard_id, lease.clone());
        Ok(lease)
    }

    fn acquire_successor(
        &self,
        logical_shard_id: &LogicalShardId,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        validate_endpoint(&endpoint)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        ensure_root_placement(&state, logical_shard_id)?;
        let current = state
            .logical_shards
            .get(logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
        if current.owner_epoch != Some(expected_owner_epoch) {
            return Err(ControlError::StaleOwnerEpoch {
                logical_shard_id: *logical_shard_id,
                expected: Some(expected_owner_epoch),
                actual: current.owner_epoch,
            });
        }
        if current.state == LogicalShardState::Recovering {
            return Err(ControlError::RecoveryAttemptPending {
                logical_shard_id: *logical_shard_id,
                owner_epoch: expected_owner_epoch,
            });
        }
        if state.owner_sessions.contains_key(logical_shard_id) {
            return Err(ControlError::PreviousOwnerSessionLive {
                logical_shard_id: *logical_shard_id,
                owner_epoch: expected_owner_epoch,
            });
        }
        let lease_id = allocate_lease_id(&mut state, *logical_shard_id)?;
        let (next, lease) = prepare_owner_acquisition(
            &current,
            Some(expected_owner_epoch),
            owner,
            endpoint,
            lease_id,
        )?;
        state.logical_shards.insert(*logical_shard_id, next);
        state
            .owner_sessions
            .insert(*logical_shard_id, lease.clone());
        Ok(lease)
    }

    fn reacquire_recovery(
        &self,
        logical_shard_id: &LogicalShardId,
        recovery_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        validate_endpoint(&endpoint)?;
        let mut state = self.state.lock().expect("control store mutex poisoned");
        ensure_root_placement(&state, logical_shard_id)?;
        let current = state
            .logical_shards
            .get(logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
        if current.owner_epoch != Some(recovery_epoch) {
            return Err(ControlError::StaleOwnerEpoch {
                logical_shard_id: *logical_shard_id,
                expected: Some(recovery_epoch),
                actual: current.owner_epoch,
            });
        }
        if state.owner_sessions.contains_key(logical_shard_id) {
            return Err(ControlError::PreviousOwnerSessionLive {
                logical_shard_id: *logical_shard_id,
                owner_epoch: recovery_epoch,
            });
        }
        let lease_id = allocate_lease_id(&mut state, *logical_shard_id)?;
        let (next, lease) =
            prepare_recovery_reacquisition(&current, recovery_epoch, owner, endpoint, lease_id)?;
        state.logical_shards.insert(*logical_shard_id, next);
        state
            .owner_sessions
            .insert(*logical_shard_id, lease.clone());
        Ok(lease)
    }

    fn renew_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        let record = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(record, lease)?;
        validate_in_memory_session(&state, lease)?;
        Ok(record.clone())
    }

    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(&current, lease)?;
        validate_in_memory_session(&state, lease)?;
        let next = prepare_mark_serving(&current, lease, publication)?;
        state
            .logical_shards
            .insert(lease.logical_shard_id, next.clone());
        Ok(next)
    }

    fn suspend_recovery(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(&current, lease)?;
        validate_in_memory_session(&state, lease)?;
        let retained = prepare_recovery_suspension(&current, lease)?;
        state.owner_sessions.remove(&lease.logical_shard_id);
        Ok(retained)
    }

    fn release_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(&current, lease)?;
        validate_in_memory_session(&state, lease)?;
        let next = prepare_owner_release(&current, lease)?;
        state
            .logical_shards
            .insert(lease.logical_shard_id, next.clone());
        state.owner_sessions.remove(&lease.logical_shard_id);
        Ok(next)
    }
}

fn validate_in_memory_session(
    state: &InMemoryState,
    lease: &LogicalShardLease,
) -> Result<(), ControlError> {
    if state.owner_sessions.get(&lease.logical_shard_id) == Some(lease) {
        Ok(())
    } else {
        Err(ControlError::StaleLease(lease.clone()))
    }
}

fn ensure_root_placement(
    state: &InMemoryState,
    logical_shard_id: &LogicalShardId,
) -> Result<(), ControlError> {
    if state.root_placements.values().any(|placement| {
        placement.logical_shard_id == *logical_shard_id
            && placement.lifecycle != RootPlacementLifecycle::Retired
    }) {
        Ok(())
    } else {
        Err(ControlError::RootPlacementRequired(*logical_shard_id))
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

pub(crate) fn validate_new_root_placement(placement: &RootPlacement) -> Result<(), ControlError> {
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

pub(crate) fn validate_root_placement_update(
    expected: &RootPlacement,
    next: &RootPlacement,
) -> Result<(), ControlError> {
    if next.root_id != expected.root_id {
        return Err(ControlError::InvalidPlacementMutation {
            root_id: expected.root_id,
            reason: "root id is immutable".to_owned(),
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
    endpoint: String,
    lease_id: u64,
) -> Result<(LogicalShardRecord, LogicalShardLease), ControlError> {
    validate_logical_shard_record(current)?;
    validate_endpoint(&endpoint)?;
    if lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "active owner lease id must be non-zero".to_owned(),
        ));
    }
    if current.owner_epoch != expected_owner_epoch {
        return Err(ControlError::StaleOwnerEpoch {
            logical_shard_id: current.logical_shard_id,
            expected: expected_owner_epoch,
            actual: current.owner_epoch,
        });
    }
    if let Some(owner_epoch) = expected_owner_epoch {
        if current.state == LogicalShardState::Recovering {
            return Err(ControlError::RecoveryAttemptPending {
                logical_shard_id: current.logical_shard_id,
                owner_epoch,
            });
        }
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
        lease_id,
    };
    let mut next = current.clone();
    next.owner = Some(owner);
    next.owner_epoch = Some(next_epoch);
    next.lease_id = lease_id;
    next.state = LogicalShardState::Recovering;
    next.endpoint = Some(endpoint);
    validate_logical_shard_record(&next)?;
    Ok((next, lease))
}

pub(crate) fn prepare_recovery_reacquisition(
    current: &LogicalShardRecord,
    recovery_epoch: OwnerEpoch,
    owner: NodeId,
    endpoint: String,
    lease_id: u64,
) -> Result<(LogicalShardRecord, LogicalShardLease), ControlError> {
    validate_logical_shard_record(current)?;
    validate_endpoint(&endpoint)?;
    if current.state != LogicalShardState::Recovering {
        return Err(ControlError::RecoveryStateConflict {
            logical_shard_id: current.logical_shard_id,
            actual: current.state,
        });
    }
    if current.owner_epoch != Some(recovery_epoch) {
        return Err(ControlError::StaleOwnerEpoch {
            logical_shard_id: current.logical_shard_id,
            expected: Some(recovery_epoch),
            actual: current.owner_epoch,
        });
    }
    if lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "active owner lease id must be non-zero".to_owned(),
        ));
    }
    let lease = LogicalShardLease {
        logical_shard_id: current.logical_shard_id,
        owner: owner.clone(),
        owner_epoch: recovery_epoch,
        lease_id,
    };
    let mut next = current.clone();
    next.owner = Some(owner);
    next.lease_id = lease_id;
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
    if current.state == LogicalShardState::Recovering {
        return Err(ControlError::RecoveryAttemptPending {
            logical_shard_id: current.logical_shard_id,
            owner_epoch: lease.owner_epoch,
        });
    }
    let mut next = current.clone();
    next.owner = None;
    next.lease_id = 0;
    next.state = LogicalShardState::Unassigned;
    next.endpoint = None;
    validate_logical_shard_record(&next)?;
    Ok(next)
}

pub(crate) fn prepare_recovery_suspension(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    if current.state != LogicalShardState::Recovering {
        return Err(ControlError::RecoveryStateConflict {
            logical_shard_id: current.logical_shard_id,
            actual: current.state,
        });
    }
    Ok(current.clone())
}

pub(crate) fn validate_record_lease(
    record: &LogicalShardRecord,
    lease: &LogicalShardLease,
) -> Result<(), ControlError> {
    if record.owner.as_ref() != Some(&lease.owner) {
        return Err(ControlError::NotOwner {
            logical_shard_id: lease.logical_shard_id,
        });
    }
    if record.logical_shard_id != lease.logical_shard_id
        || record.owner_epoch != Some(lease.owner_epoch)
        || record.lease_id != lease.lease_id
    {
        return Err(ControlError::StaleLease(lease.clone()));
    }
    Ok(())
}

pub(crate) fn validate_logical_shard_record(
    record: &LogicalShardRecord,
) -> Result<(), ControlError> {
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
    use super::*;
    use crate::{LogSegmentRef, PlacementGeneration};

    fn root_id(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn node(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn initial_placement(root: u8, shard: u8) -> RootPlacement {
        RootPlacement {
            root_id: root_id(root),
            logical_shard_id: shard_id(shard),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        }
    }

    fn next_placement(current: &RootPlacement, lifecycle: RootPlacementLifecycle) -> RootPlacement {
        RootPlacement {
            root_id: current.root_id,
            logical_shard_id: current.logical_shard_id,
            placement_generation: PlacementGeneration::new(current.placement_generation.get() + 1)
                .unwrap(),
            lifecycle,
        }
    }

    fn create_placed_shard(store: &InMemoryControlStore, root: u8, shard: u8) -> RootPlacement {
        store.create_logical_shard(shard_id(shard)).unwrap();
        let placement = initial_placement(root, shard);
        store.create_root_placement(placement.clone()).unwrap();
        placement
    }

    fn acquire_placed_shard(
        store: &InMemoryControlStore,
        root: u8,
        shard: u8,
    ) -> LogicalShardLease {
        create_placed_shard(store, root, shard);
        store
            .acquire_owner(&shard_id(shard), node("node-a"), "node-a:7000".to_owned())
            .unwrap()
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

    #[test]
    fn root_placement_must_exist_before_owner_write_authority() {
        let store = InMemoryControlStore::new();
        let logical_shard_id = shard_id(1);
        store.create_logical_shard(logical_shard_id).unwrap();

        assert!(matches!(
            store.acquire_owner(
                &logical_shard_id,
                node("node-a"),
                "node-a:7000".to_owned()
            ),
            Err(ControlError::RootPlacementRequired(id)) if id == logical_shard_id
        ));

        store
            .create_root_placement(initial_placement(1, 1))
            .unwrap();
        assert_eq!(
            store
                .acquire_owner(&logical_shard_id, node("node-a"), "node-a:7000".to_owned())
                .unwrap()
                .owner_epoch
                .get(),
            1
        );
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
        let first = acquire_placed_shard(&store, 1, 1);
        assert_eq!(first.owner_epoch.get(), 1);
        store
            .mark_serving(
                &first,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();
        let released = store.release_owner(&first).unwrap();
        assert_eq!(released.owner_epoch, Some(first.owner_epoch));
        assert!(released.owner.is_none());
        assert!(matches!(
            store.release_owner(&first),
            Err(ControlError::NotOwner { .. })
        ));

        let second = store
            .acquire_successor(
                &shard_id(1),
                first.owner_epoch,
                node("node-b"),
                "node-b:7000".to_owned(),
            )
            .unwrap();
        assert_eq!(second.owner_epoch.get(), 2);
        assert!(matches!(
            store.acquire_successor(
                &shard_id(1),
                first.owner_epoch,
                node("node-c"),
                "node-c:7000".to_owned()
            ),
            Err(ControlError::StaleOwnerEpoch { .. })
        ));
        assert!(matches!(
            store.renew_owner(&first),
            Err(ControlError::NotOwner { .. }) | Err(ControlError::StaleLease(_))
        ));
    }

    #[test]
    fn unfinished_recovery_rebinds_the_same_epoch_after_session_loss() {
        let store = InMemoryControlStore::new();
        let first = acquire_placed_shard(&store, 1, 1);
        assert_eq!(first.owner_epoch.get(), 1);

        let retained = store.suspend_recovery(&first).unwrap();
        assert_eq!(retained.state, LogicalShardState::Recovering);
        assert_eq!(retained.owner_epoch, Some(first.owner_epoch));
        assert_eq!(retained.owner.as_ref(), Some(&first.owner));
        assert_eq!(retained.lease_id, first.lease_id);
        assert!(matches!(
            store.renew_owner(&first),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            store.acquire_successor(
                &shard_id(1),
                first.owner_epoch,
                node("node-b"),
                "node-b:7000".to_owned()
            ),
            Err(ControlError::RecoveryAttemptPending { .. })
        ));

        let rebound = store
            .reacquire_recovery(
                &shard_id(1),
                first.owner_epoch,
                node("node-b"),
                "node-b:7000".to_owned(),
            )
            .unwrap();
        assert_eq!(rebound.owner_epoch, first.owner_epoch);
        assert_ne!(rebound.lease_id, first.lease_id);
        assert!(matches!(
            store.reacquire_recovery(
                &shard_id(1),
                first.owner_epoch,
                node("node-c"),
                "node-c:7000".to_owned()
            ),
            Err(ControlError::PreviousOwnerSessionLive { .. })
        ));

        store
            .mark_serving(
                &rebound,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();
        store.release_owner(&rebound).unwrap();
    }

    #[test]
    fn serving_owner_cannot_be_suspended_as_recovery() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        store
            .mark_serving(
                &lease,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();

        assert!(matches!(
            store.suspend_recovery(&lease),
            Err(ControlError::RecoveryStateConflict {
                actual: LogicalShardState::Serving,
                ..
            })
        ));
        store.release_owner(&lease).unwrap();
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
        let lease = acquire_placed_shard(&store, 1, 1);
        let first_log = log_ref(1, 2, "state-2");
        let first = store
            .mark_serving(
                &lease,
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
        let lease = acquire_placed_shard(&store, 1, 1);
        let durable = log_ref(1, 2, "state-2");
        store
            .mark_serving(
                &lease,
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
        let first = acquire_placed_shard(&store, 1, 1);
        let durable = log_ref(1, 2, "state-2");
        store
            .mark_serving(
                &first,
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
                &shard_id(1),
                first.owner_epoch,
                node("node-b"),
                "node-b:7000".to_owned(),
            )
            .unwrap();

        assert!(matches!(
            store.mark_serving(
                &successor,
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
        let lease = acquire_placed_shard(&store, 1, 1);
        let malformed = LogRef {
            segments: Vec::new(),
            durable_lsn: 1,
            digest: "state-1".to_owned(),
        };
        assert!(matches!(
            store.mark_serving(
                &lease,
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
