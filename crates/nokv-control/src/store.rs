use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::types::endpoint_is_canonical;
use crate::{
    CheckpointRef, ControlError, LogRef, LogicalShardId, LogicalShardLease, LogicalShardRecord,
    LogicalShardState, NodeId, OwnerEpoch, RecoveryPublication, RecoveryUploadIntent,
    RootAgentBinding, RootId, RootObjectNamespaceBinding, RootPlacement, RootPlacementLifecycle,
};

/// Maximum opaque object plan retained in one logical-shard control record.
pub const MAX_RECOVERY_UPLOAD_PLAN_BYTES: usize = 64 * 1024;
pub const MAX_RECOVERY_UPLOAD_RECEIPT_BYTES: usize = 64 * 1024;
/// Maximum canonical JSON bytes for one logical-shard record.
///
/// Owner-fenced etcd mutations also carry the lease-attached owner session.
/// Keeping each record below 448 KiB leaves more than 128 KiB of headroom
/// beneath etcd's 1 MiB portable request floor after the record and maximum
/// same-owner session are encoded in one transaction.
pub const MAX_LOGICAL_SHARD_RECORD_BYTES: usize = 448 * 1024;
/// Maximum aggregate opaque receipt bytes retained by one shared-log chain.
/// The encoded-record limit may stop a chain before this secondary opaque-byte
/// bound because JSON byte arrays expand on the wire.
pub const MAX_RECOVERY_LOG_RECEIPT_BYTES: usize = 1024 * 1024;
/// Hard bound for one control-plane log chain until checkpoint compaction is
/// available. Publication fails before acknowledgement when this is reached.
pub const MAX_RECOVERY_LOG_SEGMENTS: usize = 4_096;

/// Durable control-plane operations, split between immutable root placement
/// and physical logical-shard ownership.
pub trait ControlStore: Send + Sync {
    /// Create the immutable Agent authority for a root. An exact replay is
    /// idempotent; the root can never be rebound to another Agent.
    fn create_root_agent_binding(
        &self,
        binding: RootAgentBinding,
    ) -> Result<RootAgentBinding, ControlError>;
    fn get_root_agent_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootAgentBinding>, ControlError>;

    /// Create the immutable object namespace identity for a root. An exact
    /// replay is idempotent; a different identity is never adopted.
    fn create_root_object_namespace_binding(
        &self,
        binding: RootObjectNamespaceBinding,
    ) -> Result<RootObjectNamespaceBinding, ControlError>;
    fn get_root_object_namespace_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootObjectNamespaceBinding>, ControlError>;

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

    /// Persist the complete immutable-object plan before its first create.
    fn prepare_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        intent: RecoveryUploadIntent,
    ) -> Result<LogicalShardRecord, ControlError>;

    /// Publish the exact uploaded segment and clear its durable intent in one
    /// owner-fenced mutation. Exact lost-ACK readback is idempotent.
    fn finalize_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        expected_intent: &RecoveryUploadIntent,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError>;

    /// Abandon one exact incomplete immutable upload while retaining the last
    /// fully published recovery frontier. Exact readback after a concurrent
    /// finalization is idempotent.
    fn abort_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        expected_intent: &RecoveryUploadIntent,
    ) -> Result<LogicalShardRecord, ControlError>;

    /// End the live session for a boot that has not reached `Serving`, while
    /// retaining its durable recovery epoch for an exact retry.
    ///
    /// If the exact recorded recovery session has already expired, suspension
    /// is an idempotent success. A different live or rebound session remains
    /// fenced and must never be removed by this operation.
    fn suspend_recovery(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<LogicalShardRecord, ControlError>;

    fn release_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError>;
}

#[derive(Default)]
struct InMemoryState {
    root_agents: BTreeMap<RootId, RootAgentBinding>,
    root_object_namespaces: BTreeMap<RootId, RootObjectNamespaceBinding>,
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
    fn create_root_agent_binding(
        &self,
        binding: RootAgentBinding,
    ) -> Result<RootAgentBinding, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        if let Some(current) = state.root_agents.get(&binding.root_id) {
            if *current == binding {
                return Ok(binding);
            }
            return Err(ControlError::RootAgentAlreadyBound {
                root_id: binding.root_id,
            });
        }
        state.root_agents.insert(binding.root_id, binding);
        Ok(binding)
    }

    fn get_root_agent_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootAgentBinding>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.root_agents.get(root_id).copied())
    }

    fn create_root_object_namespace_binding(
        &self,
        binding: RootObjectNamespaceBinding,
    ) -> Result<RootObjectNamespaceBinding, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        if let Some(current) = state.root_object_namespaces.get(&binding.root_id) {
            if *current == binding {
                return Ok(binding);
            }
            return Err(ControlError::RootObjectNamespaceAlreadyBound {
                root_id: binding.root_id,
                existing: current.object_namespace_id,
                requested: binding.object_namespace_id,
            });
        }
        state
            .root_object_namespaces
            .insert(binding.root_id, binding);
        Ok(binding)
    }

    fn get_root_object_namespace_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootObjectNamespaceBinding>, ControlError> {
        let state = self.state.lock().expect("control store mutex poisoned");
        Ok(state.root_object_namespaces.get(root_id).copied())
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

    fn abort_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        expected_intent: &RecoveryUploadIntent,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(&current, lease)?;
        validate_in_memory_session(&state, lease)?;
        let next = prepare_recovery_upload_abort(&current, lease, expected_intent)?;
        state
            .logical_shards
            .insert(lease.logical_shard_id, next.clone());
        Ok(next)
    }

    fn prepare_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        intent: RecoveryUploadIntent,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut state = self.state.lock().expect("control store mutex poisoned");
        let current = state
            .logical_shards
            .get(&lease.logical_shard_id)
            .cloned()
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(&current, lease)?;
        validate_in_memory_session(&state, lease)?;
        let next = prepare_recovery_upload_intent(&current, lease, intent)?;
        state
            .logical_shards
            .insert(lease.logical_shard_id, next.clone());
        Ok(next)
    }

    fn finalize_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        expected_intent: &RecoveryUploadIntent,
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
        let next =
            prepare_recovery_upload_finalization(&current, lease, expected_intent, publication)?;
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
        let retained = prepare_recovery_suspension(&current, lease)?;

        match state.owner_sessions.get(&lease.logical_shard_id).cloned() {
            None => Ok(retained),
            Some(session) if session == *lease => {
                state.owner_sessions.remove(&lease.logical_shard_id);
                Ok(retained)
            }
            Some(_) => Err(ControlError::StaleLease(lease.clone())),
        }
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
    if next.pending_recovery_upload.is_some() {
        return Err(ControlError::RecoveryUploadConflict {
            logical_shard_id: current.logical_shard_id,
            reason: "logical shard cannot become Serving with a pending recovery upload".to_owned(),
        });
    }
    next.state = LogicalShardState::Serving;
    validate_logical_shard_record(&next)?;
    Ok(next)
}

pub(crate) fn prepare_recovery_upload_intent(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
    intent: RecoveryUploadIntent,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    validate_recovery_upload_intent(current, &intent)?;
    if let Some(pending) = current.pending_recovery_upload.as_ref() {
        if pending == &intent {
            return Ok(current.clone());
        }
        return Err(ControlError::RecoveryUploadConflict {
            logical_shard_id: current.logical_shard_id,
            reason: "another recovery upload intent is already pending".to_owned(),
        });
    }
    let mut next = current.clone();
    next.pending_recovery_upload = Some(intent);
    validate_logical_shard_record(&next)?;
    Ok(next)
}

fn recovery_publication_after_intent(
    current: &LogicalShardRecord,
    intent: &RecoveryUploadIntent,
) -> RecoveryPublication {
    let mut segments = current
        .log
        .as_ref()
        .map_or_else(Vec::new, |log| log.segments.clone());
    segments.push(crate::LogSegmentRef {
        segment_key: intent.manifest_key.clone(),
        first_lsn: intent.first_lsn,
        last_lsn: intent.last_lsn,
        digest: intent.last_chain_digest.clone(),
        receipt: intent.receipt.clone(),
    });
    RecoveryPublication {
        checkpoint: None,
        log: Some(crate::LogRef {
            segments,
            durable_lsn: intent.last_lsn,
            digest: intent.last_chain_digest.clone(),
        }),
        durable_lsn: intent.last_lsn,
    }
}

pub(crate) fn prepare_recovery_upload_finalization(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
    expected_intent: &RecoveryUploadIntent,
    publication: RecoveryPublication,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    validate_recovery_upload_intent_shape(expected_intent).map_err(|reason| {
        ControlError::RecoveryUploadConflict {
            logical_shard_id: current.logical_shard_id,
            reason,
        }
    })?;
    match current.pending_recovery_upload.as_ref() {
        Some(actual) if actual == expected_intent => {}
        Some(_) => {
            return Err(ControlError::RecoveryUploadConflict {
                logical_shard_id: current.logical_shard_id,
                reason: "pending recovery upload does not match finalization intent".to_owned(),
            });
        }
        None => {
            let mut completed = current.clone();
            apply_recovery_publication(&mut completed, publication)?;
            if completed == *current && recovery_upload_is_published(current, expected_intent) {
                return Ok(current.clone());
            }
            return Err(ControlError::RecoveryUploadConflict {
                logical_shard_id: current.logical_shard_id,
                reason: "recovery upload intent is not pending and its publication is not current"
                    .to_owned(),
            });
        }
    }
    validate_upload_publication(expected_intent, &publication).map_err(|reason| {
        ControlError::RecoveryUploadConflict {
            logical_shard_id: current.logical_shard_id,
            reason,
        }
    })?;
    let mut next = current.clone();
    apply_recovery_publication(&mut next, publication)?;
    next.pending_recovery_upload = None;
    validate_logical_shard_record(&next)?;
    Ok(next)
}

pub(crate) fn prepare_recovery_upload_abort(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
    expected_intent: &RecoveryUploadIntent,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    validate_recovery_upload_intent_shape(expected_intent).map_err(|reason| {
        ControlError::RecoveryUploadConflict {
            logical_shard_id: current.logical_shard_id,
            reason,
        }
    })?;
    match current.pending_recovery_upload.as_ref() {
        Some(actual) if actual == expected_intent => {}
        Some(_) => {
            return Err(ControlError::RecoveryUploadConflict {
                logical_shard_id: current.logical_shard_id,
                reason: "pending recovery upload does not match abort intent".to_owned(),
            });
        }
        None if recovery_upload_is_published(current, expected_intent) => {
            return Ok(current.clone());
        }
        None => {
            return Err(ControlError::RecoveryUploadConflict {
                logical_shard_id: current.logical_shard_id,
                reason: "recovery upload intent is neither pending nor exactly published"
                    .to_owned(),
            });
        }
    }
    if current.state != LogicalShardState::Recovering {
        return Err(ControlError::RecoveryStateConflict {
            logical_shard_id: current.logical_shard_id,
            actual: current.state,
        });
    }
    let mut next = current.clone();
    next.pending_recovery_upload = None;
    validate_logical_shard_record(&next)?;
    Ok(next)
}

pub(crate) fn prepare_owner_release(
    current: &LogicalShardRecord,
    lease: &LogicalShardLease,
) -> Result<LogicalShardRecord, ControlError> {
    validate_record_lease(current, lease)?;
    if current.pending_recovery_upload.is_some() {
        return Err(ControlError::RecoveryUploadConflict {
            logical_shard_id: current.logical_shard_id,
            reason: "owner cannot be released while a recovery upload is pending".to_owned(),
        });
    }
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

fn validate_recovery_upload_intent(
    record: &LogicalShardRecord,
    intent: &RecoveryUploadIntent,
) -> Result<(), ControlError> {
    let conflict = |reason: String| ControlError::RecoveryUploadConflict {
        logical_shard_id: record.logical_shard_id,
        reason,
    };
    if !matches!(
        record.state,
        LogicalShardState::Recovering | LogicalShardState::Serving
    ) {
        return Err(conflict(format!(
            "recovery upload requires Recovering or Serving state, actual {:?}",
            record.state
        )));
    }
    validate_recovery_upload_intent_shape(intent).map_err(&conflict)?;
    let retained_segments = record.log.as_ref().map_or(0, |log| log.segments.len());
    if retained_segments >= MAX_RECOVERY_LOG_SEGMENTS {
        return Err(conflict(format!(
            "shared recovery log already retains {retained_segments} segments; publish a checkpoint before preparing another upload"
        )));
    }
    let retained_receipt_bytes = record
        .log
        .as_ref()
        .map(|log| {
            log.segments.iter().try_fold(0_usize, |total, segment| {
                total
                    .checked_add(segment.receipt.len())
                    .ok_or_else(|| conflict("log receipt byte total overflows".to_owned()))
            })
        })
        .transpose()?
        .unwrap_or(0);
    let prepared_receipt_bytes = retained_receipt_bytes
        .checked_add(intent.receipt.len())
        .ok_or_else(|| conflict("prepared log receipt byte total overflows".to_owned()))?;
    if prepared_receipt_bytes > MAX_RECOVERY_LOG_RECEIPT_BYTES {
        return Err(conflict(format!(
            "prepared log receipt byte total {prepared_receipt_bytes} exceeds {MAX_RECOVERY_LOG_RECEIPT_BYTES}"
        )));
    }
    let expected_first = record
        .durable_lsn
        .checked_add(1)
        .ok_or_else(|| conflict("durable recovery LSN is exhausted".to_owned()))?;
    if intent.first_lsn != expected_first {
        return Err(conflict(format!(
            "upload starts at LSN {}, expected {expected_first}",
            intent.first_lsn
        )));
    }
    if let Some(expected) = durable_tail_digest(record).map_err(&conflict)? {
        if intent.previous_chain_digest != expected {
            return Err(conflict(
                "upload previous digest differs from the durable tail".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_recovery_upload_intent_shape(intent: &RecoveryUploadIntent) -> Result<(), String> {
    if intent.first_lsn == 0 || intent.last_lsn < intent.first_lsn {
        return Err("recovery upload has an invalid LSN range".to_owned());
    }
    for (name, digest) in [
        ("previous chain", intent.previous_chain_digest.as_str()),
        ("last chain", intent.last_chain_digest.as_str()),
        ("segment", intent.segment_digest.as_str()),
    ] {
        if !canonical_sha256_hex(digest) {
            return Err(format!(
                "recovery upload {name} digest is not canonical SHA-256 hex"
            ));
        }
    }
    if intent.manifest_key.is_empty()
        || intent.manifest_key.trim() != intent.manifest_key
        || intent.manifest_key.chars().any(char::is_control)
    {
        return Err("recovery upload manifest key is empty or non-canonical".to_owned());
    }
    if intent.receipt.is_empty() || intent.receipt.len() > MAX_RECOVERY_UPLOAD_RECEIPT_BYTES {
        return Err(format!(
            "recovery upload receipt length {} is outside 1..={MAX_RECOVERY_UPLOAD_RECEIPT_BYTES}",
            intent.receipt.len()
        ));
    }
    if intent.plan.is_empty() || intent.plan.len() > MAX_RECOVERY_UPLOAD_PLAN_BYTES {
        return Err(format!(
            "recovery upload plan length {} is outside 1..={MAX_RECOVERY_UPLOAD_PLAN_BYTES}",
            intent.plan.len()
        ));
    }
    Ok(())
}

fn validate_upload_publication(
    intent: &RecoveryUploadIntent,
    publication: &RecoveryPublication,
) -> Result<(), String> {
    if publication.durable_lsn != intent.last_lsn {
        return Err("publication durable LSN does not match upload intent".to_owned());
    }
    let log = publication
        .log
        .as_ref()
        .ok_or_else(|| "upload finalization requires a log publication".to_owned())?;
    if log.durable_lsn != intent.last_lsn || log.digest != intent.last_chain_digest {
        return Err("log tail does not match upload intent".to_owned());
    }
    let segment = log
        .segments
        .last()
        .ok_or_else(|| "upload finalization log has no segment".to_owned())?;
    if segment.segment_key != intent.manifest_key
        || segment.first_lsn != intent.first_lsn
        || segment.last_lsn != intent.last_lsn
        || segment.digest != intent.last_chain_digest
        || segment.receipt != intent.receipt
    {
        return Err("log segment does not match upload intent".to_owned());
    }
    Ok(())
}

fn recovery_upload_is_published(
    record: &LogicalShardRecord,
    intent: &RecoveryUploadIntent,
) -> bool {
    record.durable_lsn == intent.last_lsn
        && record.log.as_ref().is_some_and(|log| {
            log.durable_lsn == intent.last_lsn
                && log.digest == intent.last_chain_digest
                && log.segments.last().is_some_and(|segment| {
                    segment.segment_key == intent.manifest_key
                        && segment.first_lsn == intent.first_lsn
                        && segment.last_lsn == intent.last_lsn
                        && segment.digest == intent.last_chain_digest
                        && segment.receipt == intent.receipt
                })
        })
}

fn canonical_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn validate_logical_shard_record(
    record: &LogicalShardRecord,
) -> Result<(), ControlError> {
    if record.owner_epoch.is_none()
        && (record.checkpoint.is_some()
            || record.log.is_some()
            || record.durable_lsn != 0
            || record.pending_recovery_upload.is_some())
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
    if let Some(intent) = record.pending_recovery_upload.as_ref() {
        validate_recovery_upload_intent(record, intent).map_err(|error| {
            ControlError::InvalidRecord(format!("pending recovery upload is invalid: {error}"))
        })?;
        validate_pending_recovery_upload_final_record(record, intent).map_err(|error| {
            ControlError::InvalidRecord(format!(
                "pending recovery upload cannot produce a durable final record: {error}"
            ))
        })?;
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
    crate::codec::validate_logical_shard_record_encoded_size(record)?;
    Ok(())
}

fn validate_pending_recovery_upload_final_record(
    record: &LogicalShardRecord,
    intent: &RecoveryUploadIntent,
) -> Result<(), ControlError> {
    let mut completed = record.clone();
    apply_recovery_publication(
        &mut completed,
        recovery_publication_after_intent(record, intent),
    )?;
    completed.pending_recovery_upload = None;
    validate_logical_shard_record(&completed)
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
    if checkpoint.receipt.is_empty() || checkpoint.receipt.len() > MAX_RECOVERY_UPLOAD_RECEIPT_BYTES
    {
        return Err(format!(
            "checkpoint receipt length {} is outside 1..={MAX_RECOVERY_UPLOAD_RECEIPT_BYTES}",
            checkpoint.receipt.len()
        ));
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
    if log.segments.len() > MAX_RECOVERY_LOG_SEGMENTS {
        return Err(format!(
            "log segment chain has {} rows; maximum is {MAX_RECOVERY_LOG_SEGMENTS}",
            log.segments.len()
        ));
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

    let mut aggregate_receipt_bytes = 0_usize;
    for (index, segment) in log.segments.iter().enumerate() {
        if segment.segment_key.is_empty() {
            return Err(format!("log segment {index} has an empty object key"));
        }
        if segment.digest.is_empty() {
            return Err(format!("log segment {index} has an empty digest"));
        }
        if segment.receipt.is_empty() || segment.receipt.len() > MAX_RECOVERY_UPLOAD_RECEIPT_BYTES {
            return Err(format!(
                "log segment {index} receipt length {} is outside 1..={MAX_RECOVERY_UPLOAD_RECEIPT_BYTES}",
                segment.receipt.len()
            ));
        }
        aggregate_receipt_bytes = aggregate_receipt_bytes
            .checked_add(segment.receipt.len())
            .ok_or_else(|| "log receipt byte total overflows".to_owned())?;
        if aggregate_receipt_bytes > MAX_RECOVERY_LOG_RECEIPT_BYTES {
            return Err(format!(
                "log receipt byte total {aggregate_receipt_bytes} exceeds {MAX_RECOVERY_LOG_RECEIPT_BYTES}"
            ));
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
    use crate::codec::validate_logical_shard_record_encoded_size;
    use crate::{AgentId, LogSegmentRef, PlacementGeneration};

    fn root_id(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn node(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn object_namespace(root: u8, namespace: u8) -> RootObjectNamespaceBinding {
        RootObjectNamespaceBinding {
            root_id: root_id(root),
            object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([namespace; 16]),
        }
    }

    fn agent_binding(root: u8, agent: u8) -> RootAgentBinding {
        RootAgentBinding {
            root_id: root_id(root),
            agent_id: AgentId::from_bytes([agent; 16]),
        }
    }

    #[test]
    fn root_agent_binding_is_immutable_and_exact_replay_is_idempotent() {
        let store = InMemoryControlStore::new();
        let binding = agent_binding(1, 7);
        assert_eq!(store.create_root_agent_binding(binding).unwrap(), binding);
        assert_eq!(store.create_root_agent_binding(binding).unwrap(), binding);
        assert!(matches!(
            store.create_root_agent_binding(agent_binding(1, 8)),
            Err(ControlError::RootAgentAlreadyBound { .. })
        ));
        assert_eq!(
            store.get_root_agent_binding(&root_id(1)).unwrap(),
            Some(binding)
        );
    }

    #[test]
    fn concurrent_root_agent_claims_admit_exactly_one_agent() {
        let store = Arc::new(InMemoryControlStore::new());
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for agent in [7, 8] {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                store.create_root_agent_binding(agent_binding(1, agent))
            }));
        }
        barrier.wait();

        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ControlError::RootAgentAlreadyBound { .. })))
                .count(),
            1
        );
        let admitted = results.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(
            store.get_root_agent_binding(&root_id(1)).unwrap(),
            Some(admitted)
        );
    }

    #[test]
    fn object_namespace_binding_is_immutable_and_exact_replay_is_idempotent() {
        let store = InMemoryControlStore::new();
        let binding = object_namespace(1, 7);
        assert_eq!(
            store.create_root_object_namespace_binding(binding).unwrap(),
            binding
        );
        assert_eq!(
            store.create_root_object_namespace_binding(binding).unwrap(),
            binding
        );
        assert!(matches!(
            store.create_root_object_namespace_binding(object_namespace(1, 8)),
            Err(ControlError::RootObjectNamespaceAlreadyBound { .. })
        ));
        assert_eq!(
            store
                .get_root_object_namespace_binding(&root_id(1))
                .unwrap(),
            Some(binding)
        );
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
                receipt: vec![4, 5, 6],
            }],
            durable_lsn: last_lsn,
            digest: digest.to_owned(),
        }
    }

    fn recovery_upload(first_lsn: u64, last_lsn: u64, digest: char) -> RecoveryUploadIntent {
        let digest = std::iter::repeat_n(digest, 64).collect::<String>();
        RecoveryUploadIntent {
            object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([7; 16]),
            first_lsn,
            last_lsn,
            previous_chain_digest: std::iter::repeat_n('0', 64).collect(),
            last_chain_digest: digest.clone(),
            segment_digest: std::iter::repeat_n('a', 64).collect(),
            manifest_key: format!("nokv/recovery/log-segments/v1/{first_lsn}-{last_lsn}"),
            receipt: vec![4, 5, 6],
            plan: vec![1, 2, 3],
        }
    }

    fn recovery_upload_publication(intent: &RecoveryUploadIntent) -> RecoveryPublication {
        RecoveryPublication {
            checkpoint: None,
            log: Some(LogRef {
                segments: vec![LogSegmentRef {
                    segment_key: intent.manifest_key.clone(),
                    first_lsn: intent.first_lsn,
                    last_lsn: intent.last_lsn,
                    digest: intent.last_chain_digest.clone(),
                    receipt: intent.receipt.clone(),
                }],
                durable_lsn: intent.last_lsn,
                digest: intent.last_chain_digest.clone(),
            }),
            durable_lsn: intent.last_lsn,
        }
    }

    #[test]
    fn recovery_upload_intent_is_durable_before_publication_and_lost_ack_is_exact() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        let intent = recovery_upload(1, 3, 'b');

        let prepared = store
            .prepare_recovery_upload(&lease, intent.clone())
            .unwrap();
        assert_eq!(prepared.pending_recovery_upload.as_ref(), Some(&intent));
        assert_eq!(prepared.durable_lsn, 0);
        assert_eq!(
            store
                .prepare_recovery_upload(&lease, intent.clone())
                .unwrap(),
            prepared,
            "exact intent replay is idempotent"
        );

        let publication = recovery_upload_publication(&intent);
        let finalized = store
            .finalize_recovery_upload(&lease, &intent, publication.clone())
            .unwrap();
        assert_eq!(finalized.durable_lsn, 3);
        assert!(finalized.pending_recovery_upload.is_none());
        assert_eq!(finalized.state, LogicalShardState::Recovering);
        assert_eq!(
            store
                .finalize_recovery_upload(&lease, &intent, publication)
                .unwrap(),
            finalized,
            "exact finalization readback accepts a lost response"
        );
    }

    #[test]
    fn recovery_upload_rejects_conflicting_intent_or_publication() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        let intent = recovery_upload(1, 3, 'b');
        store
            .prepare_recovery_upload(&lease, intent.clone())
            .unwrap();

        let mut conflicting = intent.clone();
        conflicting.plan.push(4);
        assert!(matches!(
            store.prepare_recovery_upload(&lease, conflicting),
            Err(ControlError::RecoveryUploadConflict { .. })
        ));

        let mut publication = recovery_upload_publication(&intent);
        publication.log.as_mut().unwrap().segments[0].segment_key = "foreign".to_owned();
        assert!(matches!(
            store.finalize_recovery_upload(&lease, &intent, publication),
            Err(ControlError::RecoveryUploadConflict { .. })
        ));
        assert_eq!(
            store
                .get_logical_shard(&lease.logical_shard_id)
                .unwrap()
                .unwrap()
                .pending_recovery_upload,
            Some(intent)
        );
    }

    #[test]
    fn recovery_upload_abort_is_exact_owner_fenced_and_finalization_aware() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        let intent = recovery_upload(1, 3, 'b');
        store
            .prepare_recovery_upload(&lease, intent.clone())
            .unwrap();

        let mut foreign = intent.clone();
        foreign.plan.push(4);
        assert!(matches!(
            store.abort_recovery_upload(&lease, &foreign),
            Err(ControlError::RecoveryUploadConflict { .. })
        ));
        assert_eq!(
            store
                .get_logical_shard(&lease.logical_shard_id)
                .unwrap()
                .unwrap()
                .pending_recovery_upload,
            Some(intent.clone())
        );

        let aborted = store.abort_recovery_upload(&lease, &intent).unwrap();
        assert!(aborted.pending_recovery_upload.is_none());
        assert_eq!(aborted.durable_lsn, 0);
        assert_eq!(aborted.state, LogicalShardState::Recovering);

        store
            .prepare_recovery_upload(&lease, intent.clone())
            .unwrap();
        let publication = recovery_upload_publication(&intent);
        let finalized = store
            .finalize_recovery_upload(&lease, &intent, publication)
            .unwrap();
        assert_eq!(
            store.abort_recovery_upload(&lease, &intent).unwrap(),
            finalized,
            "an abort replay must recognize an exact completed publication"
        );
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
    fn expired_recovery_session_cleanup_is_idempotent_and_rebinds_same_epoch() {
        let store = InMemoryControlStore::new();
        let first = acquire_placed_shard(&store, 1, 1);

        // Simulate backend lease expiry without calling suspend_recovery().
        // The durable Recovering record remains, but its liveness session is gone.
        let expired = store
            .state
            .lock()
            .expect("control store mutex poisoned")
            .owner_sessions
            .remove(&first.logical_shard_id);

        assert_eq!(expired, Some(first.clone()));
        assert!(matches!(
            store.renew_owner(&first),
            Err(ControlError::StaleLease(_))
        ));

        // Current main fails here with StaleLease even though the exact durable
        // record still identifies this recovery attempt and no live session exists.
        let retained = store.suspend_recovery(&first).unwrap();

        assert_eq!(retained.state, LogicalShardState::Recovering);
        assert_eq!(retained.owner_epoch, Some(first.owner_epoch));
        assert_eq!(retained.owner.as_ref(), Some(&first.owner));
        assert_eq!(retained.lease_id, first.lease_id);

        // Cleanup should remain idempotent while the exact record is unchanged
        // and the session remains absent.
        assert_eq!(
            store.suspend_recovery(&first).unwrap(),
            retained,
            "suspending an already-expired exact recovery session is idempotent"
        );

        // The durable recovery epoch must be rebound, not incremented.
        let rebound = store
            .reacquire_recovery(
                &first.logical_shard_id,
                first.owner_epoch,
                node("node-b"),
                "node-b:7000".to_owned(),
            )
            .unwrap();

        assert_eq!(rebound.owner_epoch, first.owner_epoch);
        assert_ne!(rebound.lease_id, first.lease_id);

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
    fn expired_recovery_cleanup_cannot_suspend_rebound_live_session() {
        let store = InMemoryControlStore::new();
        let first = acquire_placed_shard(&store, 1, 1);

        let expired = store
            .state
            .lock()
            .expect("control store mutex poisoned")
            .owner_sessions
            .remove(&first.logical_shard_id);

        assert_eq!(expired, Some(first.clone()));

        // Rebind using the same owner and epoch, but a different lease ID. This
        // isolates the lease-ID fence rather than relying on an owner mismatch.
        let rebound = store
            .reacquire_recovery(
                &first.logical_shard_id,
                first.owner_epoch,
                first.owner.clone(),
                "node-a:7001".to_owned(),
            )
            .unwrap();

        assert_eq!(rebound.owner, first.owner);
        assert_eq!(rebound.owner_epoch, first.owner_epoch);
        assert_ne!(rebound.lease_id, first.lease_id);

        // The old lease must never be allowed to suspend the rebound session.
        assert!(matches!(
            store.suspend_recovery(&first),
            Err(ControlError::StaleLease(_))
        ));

        // Prove that the rebound session is still live and was not removed.
        let renewed = store.renew_owner(&rebound).unwrap();
        assert_eq!(renewed.lease_id, rebound.lease_id);
        assert_eq!(renewed.owner_epoch, Some(rebound.owner_epoch));

        store.suspend_recovery(&rebound).unwrap();
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
            receipt: vec![7, 8, 9],
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
        assert_eq!(advanced.log.as_ref().unwrap().durable_lsn, 4);

        let first_segment = advanced
            .log
            .as_ref()
            .expect("advanced log must remain present")
            .segments[0]
            .clone();
        let second_segment = LogSegmentRef {
            segment_key: "logs/5-6".to_owned(),
            first_lsn: 5,
            last_lsn: 6,
            digest: "state-6".to_owned(),
            receipt: vec![7, 8, 9],
        };
        let advanced = store
            .mark_serving(
                &lease,
                RecoveryPublication {
                    checkpoint: None,
                    log: Some(LogRef {
                        segments: vec![first_segment, second_segment],
                        durable_lsn: 6,
                        digest: "state-6".to_owned(),
                    }),
                    durable_lsn: 6,
                },
            )
            .unwrap();
        assert_eq!(advanced.checkpoint.as_ref().unwrap().lsn, 2);
        assert_eq!(advanced.log.as_ref().unwrap().segments.len(), 2);

        store.release_owner(&lease).unwrap();
        let successor = store
            .acquire_successor(
                &shard_id(1),
                lease.owner_epoch,
                node("node-b"),
                "node-b:7000".to_owned(),
            )
            .unwrap();
        let serving = store
            .mark_serving(
                &successor,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 6,
                },
            )
            .unwrap();
        assert_eq!(serving.state, LogicalShardState::Serving);
        assert_eq!(serving.checkpoint.as_ref().unwrap().lsn, 2);
        assert_eq!(serving.log.as_ref().unwrap().durable_lsn, 6);
    }

    #[test]
    fn checkpoint_publication_requires_a_bounded_recovery_receipt() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        let checkpoint = |receipt: Vec<u8>| CheckpointRef {
            object_key: "checkpoints/2".to_owned(),
            lsn: 2,
            image_bytes: 4096,
            image_digest: "image-2".to_owned(),
            digest: "state-2".to_owned(),
            receipt,
        };

        for receipt in [Vec::new(), vec![0; MAX_RECOVERY_UPLOAD_RECEIPT_BYTES + 1]] {
            assert!(matches!(
                store.mark_serving(
                    &lease,
                    RecoveryPublication {
                        checkpoint: Some(checkpoint(receipt)),
                        log: None,
                        durable_lsn: 2,
                    },
                ),
                Err(ControlError::RecoveryPublicationConflict { .. })
            ));
        }
        assert_eq!(
            store
                .get_logical_shard(&shard_id(1))
                .unwrap()
                .unwrap()
                .durable_lsn,
            0
        );
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
    fn log_chain_cap_fails_closed_before_publication() {
        let record = LogicalShardRecord::unassigned(shard_id(1));
        let segments = (1..=MAX_RECOVERY_LOG_SEGMENTS + 1)
            .map(|lsn| LogSegmentRef {
                segment_key: format!("logs/{lsn}-{lsn}"),
                first_lsn: lsn as u64,
                last_lsn: lsn as u64,
                digest: format!("state-{lsn}"),
                receipt: vec![1],
            })
            .collect::<Vec<_>>();
        let log = LogRef {
            durable_lsn: segments.last().unwrap().last_lsn,
            digest: segments.last().unwrap().digest.clone(),
            segments,
        };

        assert_eq!(
            validate_log_ref(&record, None, &log).unwrap_err(),
            format!(
                "log segment chain has {} rows; maximum is {MAX_RECOVERY_LOG_SEGMENTS}",
                MAX_RECOVERY_LOG_SEGMENTS + 1
            )
        );
    }

    #[test]
    fn aggregate_log_receipt_bytes_fail_closed_before_publication() {
        let record = LogicalShardRecord::unassigned(shard_id(1));
        let segment_count = MAX_RECOVERY_LOG_RECEIPT_BYTES
            .checked_div(MAX_RECOVERY_UPLOAD_RECEIPT_BYTES)
            .unwrap()
            + 1;
        let segments = (1..=segment_count)
            .map(|lsn| LogSegmentRef {
                segment_key: format!("logs/{lsn}-{lsn}"),
                first_lsn: lsn as u64,
                last_lsn: lsn as u64,
                digest: format!("state-{lsn}"),
                receipt: vec![1; MAX_RECOVERY_UPLOAD_RECEIPT_BYTES],
            })
            .collect::<Vec<_>>();
        let log = LogRef {
            durable_lsn: segments.last().unwrap().last_lsn,
            digest: segments.last().unwrap().digest.clone(),
            segments,
        };

        assert_eq!(
            validate_log_ref(&record, None, &log).unwrap_err(),
            format!(
                "log receipt byte total {} exceeds {MAX_RECOVERY_LOG_RECEIPT_BYTES}",
                segment_count * MAX_RECOVERY_UPLOAD_RECEIPT_BYTES
            )
        );
    }

    #[test]
    fn recovery_upload_prepare_rejects_a_chain_that_cannot_be_finalized() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        let base = store.renew_owner(&lease).unwrap();
        let intent_after = |record: &LogicalShardRecord| RecoveryUploadIntent {
            object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([7; 16]),
            first_lsn: record.durable_lsn + 1,
            last_lsn: record.durable_lsn + 1,
            previous_chain_digest: record.log.as_ref().unwrap().digest.clone(),
            last_chain_digest: "b".repeat(64),
            segment_digest: "a".repeat(64),
            manifest_key: "nokv/recovery/log-segments/v1/next".to_owned(),
            receipt: vec![1],
            plan: vec![1],
        };
        let chain = |count: usize, receipt_len: usize| {
            let segments = (1..=count)
                .map(|lsn| LogSegmentRef {
                    segment_key: format!("logs/{lsn}-{lsn}"),
                    first_lsn: lsn as u64,
                    last_lsn: lsn as u64,
                    digest: format!("{:064x}", lsn),
                    receipt: vec![1; receipt_len],
                })
                .collect::<Vec<_>>();
            LogRef {
                durable_lsn: count as u64,
                digest: segments.last().unwrap().digest.clone(),
                segments,
            }
        };

        let mut full = base.clone();
        full.log = Some(chain(MAX_RECOVERY_LOG_SEGMENTS, 1));
        full.durable_lsn = MAX_RECOVERY_LOG_SEGMENTS as u64;
        let error = prepare_recovery_upload_intent(&full, &lease, intent_after(&full))
            .expect_err("a full log chain cannot accept an unfinishable upload intent");
        assert!(error.to_string().contains("publish a checkpoint"));

        let exact_receipt_segments =
            MAX_RECOVERY_LOG_RECEIPT_BYTES / MAX_RECOVERY_UPLOAD_RECEIPT_BYTES;
        let mut receipt_full = base;
        receipt_full.log = Some(chain(
            exact_receipt_segments,
            MAX_RECOVERY_UPLOAD_RECEIPT_BYTES,
        ));
        receipt_full.durable_lsn = exact_receipt_segments as u64;
        let error =
            prepare_recovery_upload_intent(&receipt_full, &lease, intent_after(&receipt_full))
                .expect_err("an aggregate-full receipt chain cannot accept another intent");
        assert!(error
            .to_string()
            .contains("prepared log receipt byte total"));
    }

    #[test]
    fn recovery_upload_prepare_rejects_an_unpersistable_final_record() {
        let store = InMemoryControlStore::new();
        let lease = acquire_placed_shard(&store, 1, 1);
        let mut record = store.renew_owner(&lease).unwrap();
        let mut segments = Vec::new();

        loop {
            let lsn = segments.len() as u64 + 1;
            segments.push(LogSegmentRef {
                segment_key: format!("nokv/recovery/log-segments/v1/{lsn:020}-{}", "k".repeat(96)),
                first_lsn: lsn,
                last_lsn: lsn,
                digest: format!("{lsn:064x}"),
                receipt: vec![255; 210],
            });
            record.log = Some(LogRef {
                segments: segments.clone(),
                durable_lsn: lsn,
                digest: format!("{lsn:064x}"),
            });
            record.durable_lsn = lsn;
            if validate_logical_shard_record_encoded_size(&record).is_err() {
                segments.pop();
                let tail = segments.last().expect("one segment fits the record budget");
                record.log = Some(LogRef {
                    segments: segments.clone(),
                    durable_lsn: tail.last_lsn,
                    digest: tail.digest.clone(),
                });
                record.durable_lsn = tail.last_lsn;
                break;
            }
            assert!(segments.len() < MAX_RECOVERY_LOG_SEGMENTS);
        }
        assert!(validate_logical_shard_record_encoded_size(&record).is_ok());

        let next_lsn = record.durable_lsn + 1;
        let intent = RecoveryUploadIntent {
            object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([7; 16]),
            first_lsn: next_lsn,
            last_lsn: next_lsn,
            previous_chain_digest: record.log.as_ref().unwrap().digest.clone(),
            last_chain_digest: "b".repeat(64),
            segment_digest: "a".repeat(64),
            manifest_key: format!(
                "nokv/recovery/log-segments/v1/{next_lsn:020}-{}",
                "k".repeat(96)
            ),
            receipt: vec![255; 210],
            plan: vec![1],
        };

        let error = prepare_recovery_upload_intent(&record, &lease, intent)
            .expect_err("prepare must reject a final record outside the persistence budget");
        assert!(error
            .to_string()
            .contains("encoded logical shard recovery state"));
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
            receipt: vec![1],
        });

        assert!(matches!(
            validate_logical_shard_record(&record),
            Err(ControlError::InvalidRecord(_))
        ));
    }
}
