use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use nokv_control::{
    CheckpointRef, ControlError, ControlStore, LogRef, NodeId, ShardId, ShardLease, ShardRecord,
    ShardState,
};
use nokv_meta::{MetadataStore, NoKvFs};
use nokv_object::ObjectStore;

use crate::server::ServerError;

const DEFAULT_SHARD_OWNER_RENEWAL_INTERVAL: Duration = Duration::from_secs(5);
/// Default lease TTL the owner self-fences against. Must be `<=` the control
/// plane's own lease TTL so the local deadline never outlives the control
/// plane's expiry (matches the etcd backend's default TTL).
const DEFAULT_SHARD_LEASE_TTL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerShardOwnerOptions {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub acquisition: ServerShardAcquisition,
    pub renewal: Option<ServerShardOwnerRenewalOptions>,
    pub shared_log: Option<ServerSharedLogOptions>,
    /// Stable shard index to register for this shard before acquiring, when this
    /// owner is responsible for declaring its identity. `None` adopts whatever
    /// index the control record already carries (the in-process fleet path, where
    /// a separate `register_shard` step seeds identity; and the single default
    /// shard, which is index 0). A multi-process etcd fleet sets this so each
    /// process declares its own non-default shard index. The shard's path prefix
    /// is derived from `shard_id` by the control store.
    pub shard_index: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerShardAcquisition {
    Fresh,
    Failover { previous_epoch: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerShardOwnerRenewalOptions {
    pub interval: Duration,
    pub run_immediately: bool,
    /// TTL used to arm the owner's wall-clock self-fence. The deadline is
    /// refreshed to `renew_start + lease_ttl` on every successful renewal, so an
    /// owner that loses contact with the control plane stops committing here.
    pub lease_ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerSharedLogOptions {
    pub archive_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerShardOwnerState {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub epoch: u64,
    pub lease_id: u64,
    pub state: ShardState,
    pub checkpoint: Option<CheckpointRef>,
    pub log: Option<LogRef>,
    pub durable_lsn: u64,
}

#[derive(Clone)]
pub(crate) struct ServerShardOwner {
    store: Arc<dyn ControlStore>,
    lease: ShardLease,
    recovery_publish_gate: Arc<RwLock<()>>,
    /// Serializes exact recovery-reference CAS operations with lease renewal.
    /// The visibility gate may be held across a multi-command RPC, while this
    /// narrower gate is intentionally re-acquired by each post-commit log-tail
    /// publication from inside that RPC.
    recovery_control_gate: Arc<Mutex<()>>,
    published_recovery_state: Arc<RwLock<Option<ServerShardOwnerState>>>,
    recovery_dirty: Arc<AtomicBool>,
    /// Lease TTL (ms) for the wall-clock self-fence; `None` when auto-renewal is
    /// disabled (manual/test owners and single-node dev, which keep the fence
    /// off and rely on the epoch fence alone).
    lease_ttl_ms: Option<u64>,
}

impl Default for ServerShardOwnerRenewalOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_SHARD_OWNER_RENEWAL_INTERVAL,
            run_immediately: false,
            lease_ttl: DEFAULT_SHARD_LEASE_TTL,
        }
    }
}

impl ServerShardOwnerRenewalOptions {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            run_immediately: false,
            lease_ttl: DEFAULT_SHARD_LEASE_TTL,
        }
    }
}

impl ServerShardOwnerOptions {
    pub fn fresh(shard_id: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            shard_id: ShardId::new(shard_id),
            node_id: NodeId::new(node_id),
            acquisition: ServerShardAcquisition::Fresh,
            renewal: Some(ServerShardOwnerRenewalOptions::default()),
            shared_log: None,
            shard_index: None,
        }
    }

    pub fn failover(
        shard_id: impl Into<String>,
        node_id: impl Into<String>,
        previous_epoch: u64,
    ) -> Self {
        Self {
            shard_id: ShardId::new(shard_id),
            node_id: NodeId::new(node_id),
            acquisition: ServerShardAcquisition::Failover { previous_epoch },
            renewal: Some(ServerShardOwnerRenewalOptions::default()),
            shared_log: None,
            shard_index: None,
        }
    }

    pub fn with_renewal(mut self, renewal: Option<ServerShardOwnerRenewalOptions>) -> Self {
        self.renewal = renewal;
        self
    }

    /// Declare the stable shard index this owner registers before acquiring. Used
    /// by a multi-process fleet so each process seeds its own shard identity.
    pub fn with_shard_index(mut self, shard_index: Option<u16>) -> Self {
        self.shard_index = shard_index;
        self
    }

    pub fn with_shared_log(mut self, shared_log: Option<ServerSharedLogOptions>) -> Self {
        self.shared_log = shared_log;
        self
    }
}

impl ServerSharedLogOptions {
    pub fn new(archive_prefix: impl Into<String>) -> Self {
        Self {
            archive_prefix: archive_prefix.into(),
        }
    }
}

impl ServerShardOwner {
    pub(crate) fn acquire<M, O>(
        store: Arc<dyn ControlStore>,
        options: ServerShardOwnerOptions,
        service: &NoKvFs<M, O>,
    ) -> Result<Self, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        // Only arm the wall-clock self-fence when auto-renewal is on; manual/test
        // owners (renewal = None) keep it disabled and rely on the epoch fence.
        let lease_ttl_ms = options
            .renewal
            .map(|renewal| renewal.lease_ttl.as_millis() as u64)
            .filter(|ms| *ms > 0);
        let basis_ms = service.now_ms();
        let acquisition = options.acquisition;
        let lease = match acquisition {
            ServerShardAcquisition::Fresh => {
                store.acquire_unassigned(options.shard_id, options.node_id)?
            }
            ServerShardAcquisition::Failover { previous_epoch } => {
                store.acquire_after_failure(options.shard_id, options.node_id, previous_epoch)?
            }
        };
        // This check is deliberately after the atomic acquire. A pre-read has a
        // TOCTOU window with another owner generation; the returned lease plus
        // an exact record read-back is the authoritative proof. Epoch > 1 may
        // still be a controlled Fresh resurrection only when no generation has
        // ever reached Serving and no recovery identity has ever been published.
        if matches!(acquisition, ServerShardAcquisition::Fresh) && lease.epoch != 1 {
            let record = match store.get_shard(&lease.shard_id) {
                Ok(record) => record,
                Err(err) => {
                    let _ = store.release(&lease);
                    return Err(err.into());
                }
            };
            let exact_lease = record.owner.as_ref() == Some(&lease.owner)
                && record.epoch == lease.epoch
                && record.lease_id == lease.lease_id
                && record.state == ShardState::Recovering;
            let never_served_empty = !record.ever_served
                && record.checkpoint.is_none()
                && record.log.is_none()
                && record.durable_lsn == 0;
            if !exact_lease || !never_served_empty {
                let err = nokv_control::ControlError::FreshAcquireRequiresFailover {
                    shard_id: lease.shard_id.clone(),
                    epoch: lease.epoch,
                };
                let _ = store.release(&lease);
                return Err(err.into());
            }
        }
        if let Err(err) = service.install_owner_epoch(lease.epoch) {
            // The control lease already exists, but no owner object has been
            // constructed yet to release it. Do not strand a Recovering owner
            // when local epoch installation rejects startup.
            let _ = store.release(&lease);
            return Err(err.into());
        }
        if let Some(ttl) = lease_ttl_ms {
            service.set_lease_deadline(basis_ms.saturating_add(ttl));
        }
        Ok(Self {
            store,
            lease,
            recovery_publish_gate: Arc::new(RwLock::new(())),
            recovery_control_gate: Arc::new(Mutex::new(())),
            published_recovery_state: Arc::new(RwLock::new(None)),
            recovery_dirty: Arc::new(AtomicBool::new(false)),
            lease_ttl_ms,
        })
    }

    pub(crate) fn lock_recovery_visibility(&self) -> RwLockReadGuard<'_, ()> {
        self.recovery_publish_gate
            .read()
            .unwrap_or_else(|err| err.into_inner())
    }

    pub(crate) fn lock_recovery_publication(&self) -> RwLockWriteGuard<'_, ()> {
        self.recovery_publish_gate
            .write()
            .unwrap_or_else(|err| err.into_inner())
    }

    pub(crate) fn published_recovery_state(&self) -> Option<ServerShardOwnerState> {
        self.published_recovery_state
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    pub(crate) fn mark_recovery_dirty(&self) {
        self.recovery_dirty.store(true, Ordering::Release);
    }

    pub(crate) fn mark_recovery_clean(&self) {
        self.recovery_dirty.store(false, Ordering::Release);
    }

    pub(crate) fn recovery_is_dirty(&self) -> bool {
        self.recovery_dirty.load(Ordering::Acquire)
    }

    pub(crate) fn verify_read_lease<M, O>(&self, service: &NoKvFs<M, O>) -> Result<(), ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        if self.lease_ttl_ms.is_some() {
            service.verify_owner_lease().map_err(ServerError::Metadata)
        } else {
            // Explicit no-background-renew mode has no local expiry deadline.
            // Validate the stable control session on every read instead of
            // allowing a replaced owner to serve stale data indefinitely.
            match self.renew(service) {
                Ok(_) => service.verify_owner_lease().map_err(ServerError::Metadata),
                Err(err)
                    if matches!(
                        &err,
                        ServerError::Control(
                            ControlError::NotOwner { .. } | ControlError::StaleLease { .. }
                        )
                    ) =>
                {
                    let endpoint = self
                        .store
                        .get_shard(&self.lease.shard_id)
                        .ok()
                        .and_then(|record| record.endpoint);
                    Err(ServerError::NotOwner {
                        shard_id: self.lease.shard_id.as_str().to_owned(),
                        endpoint,
                    })
                }
                Err(err) => Err(err),
            }
        }
    }

    fn cache_published_recovery_state(&self, state: ServerShardOwnerState) {
        *self
            .published_recovery_state
            .write()
            .unwrap_or_else(|err| err.into_inner()) = Some(state);
    }

    pub(crate) fn mark_serving<M, O>(
        &self,
        service: &NoKvFs<M, O>,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        self.mark_serving_with_recovery_refs(service, None, None, 0)
    }

    pub(crate) fn mark_serving_with_recovery_refs<M, O>(
        &self,
        service: &NoKvFs<M, O>,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        let _publication = self.lock_recovery_publication();
        self.mark_serving_with_recovery_refs_locked(service, checkpoint, log, durable_lsn)
    }

    /// Publish an exact recovery identity while serializing on the narrower
    /// control gate. Ordinary callers also hold `recovery_publish_gate` to
    /// protect namespace visibility; synchronous post-commit publication from
    /// a multi-command RPC already runs inside that outer critical section and
    /// deliberately acquires only the control gate here.
    pub(crate) fn mark_serving_with_recovery_refs_locked<M, O>(
        &self,
        service: &NoKvFs<M, O>,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        let _control = self
            .recovery_control_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Local deadline/epoch fencing is checked immediately before the
        // control-plane lease CAS. An owner that self-fenced must not revive
        // itself by publishing a recovery reference.
        service.verify_owner_lease()?;
        let cached = self.published_recovery_state();
        let checkpoint_for_readback = checkpoint.clone();
        let log_for_readback = log.clone();
        let record = match self
            .store
            .mark_serving(&self.lease, checkpoint, log, durable_lsn)
        {
            Ok(record) => record,
            Err(mark_err) => match self.store.get_shard(&self.lease.shard_id) {
                Ok(record)
                    if recovery_publication_matches(
                        &self.lease,
                        &record,
                        cached.as_ref(),
                        checkpoint_for_readback.as_ref(),
                        log_for_readback.as_ref(),
                        durable_lsn,
                    ) =>
                {
                    record
                }
                Ok(record) => {
                    service.fence_required_owner_epoch(record.epoch)?;
                    return Err(mark_err.into());
                }
                Err(_) => return Err(mark_err.into()),
            },
        };
        // `validate_record_lease` guarantees this is the same immutable epoch
        // installed during acquisition. Re-installing it here would attempt
        // restore-visibility recovery while a restore commit still owns that
        // fence, deadlocking synchronous post-commit publication.
        // `mark_serving` is a control-record CAS, not a lease keepalive. Never
        // extend the local deadline here: only a successful `renew` round trip
        // proves that the control plane extended the real lease.
        let state = owner_state(&self.lease, &record);
        self.cache_published_recovery_state(state.clone());
        Ok(state)
    }

    pub(crate) fn renew<M, O>(
        &self,
        service: &NoKvFs<M, O>,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        let _control = self
            .recovery_control_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Capture the deadline basis BEFORE the round-trip so a slow renew never
        // pushes the local deadline past the control plane's real lease expiry.
        let basis_ms = service.now_ms();
        match self.store.renew(&self.lease) {
            Ok(record) => {
                // The lease epoch is immutable and was installed by acquire.
                // Avoid re-entering restore visibility while a post-commit
                // publication is waiting on this control gate.
                if let Some(ttl) = self.lease_ttl_ms {
                    service.set_lease_deadline(basis_ms.saturating_add(ttl));
                }
                let state = owner_state(&self.lease, &record);
                self.cache_published_recovery_state(state.clone());
                Ok(state)
            }
            Err(err) => {
                // Best-effort: observe a bumped epoch if the control plane is
                // reachable. If it is NOT reachable, the lease deadline armed on
                // the last successful renew still fences this owner once it
                // passes, so a partitioned owner cannot keep committing.
                if let Ok(record) = self.store.get_shard(&self.lease.shard_id) {
                    service.fence_required_owner_epoch(record.epoch)?;
                }
                Err(err.into())
            }
        }
    }

    pub(crate) fn state(&self) -> Result<ServerShardOwnerState, ServerError> {
        let record = self.store.get_shard(&self.lease.shard_id)?;
        Ok(owner_state(&self.lease, &record))
    }

    /// Relinquish ownership so a standby can acquire immediately instead of
    /// waiting out the lease TTL. Used on graceful shutdown.
    pub(crate) fn release(&self) -> Result<(), ServerError> {
        let _control = self
            .recovery_control_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.store.release(&self.lease)?;
        Ok(())
    }
}

fn recovery_publication_matches(
    lease: &ShardLease,
    record: &ShardRecord,
    cached: Option<&ServerShardOwnerState>,
    checkpoint: Option<&CheckpointRef>,
    log: Option<&LogRef>,
    durable_lsn: u64,
) -> bool {
    if record.owner.as_ref() != Some(&lease.owner)
        || record.epoch != lease.epoch
        || record.lease_id != lease.lease_id
        || record.state != ShardState::Serving
        || record.durable_lsn != durable_lsn
    {
        return false;
    }

    let expected_checkpoint = checkpoint
        .cloned()
        .or_else(|| cached.and_then(|state| state.checkpoint.clone()));
    let Some(expected_checkpoint) = expected_checkpoint else {
        // An ACK-lost no-reference publication cannot be proven without an
        // exact previously-published recovery identity.
        return false;
    };
    if record.checkpoint.as_ref() != Some(&expected_checkpoint) {
        return false;
    }

    let mut expected_log = log
        .cloned()
        .or_else(|| cached.and_then(|state| state.log.clone()));
    if expected_log
        .as_ref()
        .is_some_and(|expected_log| expected_log.durable_lsn <= expected_checkpoint.lsn)
        || (checkpoint.is_some() && expected_checkpoint.lsn == durable_lsn && log.is_none())
    {
        expected_log = None;
    }
    record.log == expected_log
}

fn owner_state(lease: &ShardLease, record: &ShardRecord) -> ServerShardOwnerState {
    ServerShardOwnerState {
        shard_id: lease.shard_id.clone(),
        node_id: lease.owner.clone(),
        epoch: lease.epoch,
        lease_id: lease.lease_id,
        state: record.state,
        checkpoint: record.checkpoint.clone(),
        log: record.log.clone(),
        durable_lsn: record.durable_lsn,
    }
}
