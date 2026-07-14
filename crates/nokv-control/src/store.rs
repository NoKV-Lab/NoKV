use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::{
    CheckpointRef, ControlError, LogRef, NodeId, ShardId, ShardLease, ShardRecord, ShardState,
};

pub trait ControlStore: Send + Sync {
    fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError>;
    /// Register a shard's stable identity (prefix + index) before it is acquired.
    /// Idempotent; identity is only set while the shard is unowned so a live
    /// owner's routing cannot change underneath it.
    fn register_shard(
        &self,
        shard_id: ShardId,
        prefix: String,
        shard_index: u16,
    ) -> Result<ShardRecord, ControlError>;
    /// Record (or, with `None`, clear) the durable subtree-root inode for a
    /// subtree shard — the atomic registration point of a cross-shard graft.
    /// Idempotent and not lease-gated: it is a topology fact about the shard's
    /// own namespace, set by `register_graft` before the (reconcilable) parent
    /// graft dentry is written, and cleared by `unregister_graft` after the
    /// graft is torn down. Returns the updated record.
    fn set_subtree_root_inode(
        &self,
        shard_id: &ShardId,
        subtree_root_inode: Option<u64>,
    ) -> Result<ShardRecord, ControlError>;
    /// Enumerate every known shard record so clients can build the routing map
    /// and placement can find unowned/owned shards.
    fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError>;
    fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError>;
    fn acquire_unassigned(
        &self,
        shard_id: ShardId,
        owner: NodeId,
    ) -> Result<ShardLease, ControlError>;
    fn acquire_after_failure(
        &self,
        shard_id: ShardId,
        owner: NodeId,
        previous_epoch: u64,
    ) -> Result<ShardLease, ControlError>;
    fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError>;
    /// Publish recovery references for the current lease.
    ///
    /// This API is owner-fenced but deliberately does not infer capture order
    /// between concurrent calls made with the same lease. A caller that can
    /// publish from multiple tasks must provide one single-writer critical
    /// section spanning capture/prepare, this CAS, and pruning of the superseded
    /// object(s). The server's recovery-publication gate is the production
    /// implementation of that contract. Pruning before a successful CAS, or
    /// allowing a later capture to overtake an earlier one, is unsafe.
    fn mark_serving(
        &self,
        lease: &ShardLease,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ShardRecord, ControlError>;
    fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError>;
}

/// Apply a `register_shard` to a record in place, enforcing that a shard's stable
/// identity (`prefix`, `shard_index`) can only be set while the record is pristine
/// — never leased (`epoch == 0`). Once a shard has taken a lease its identity is
/// frozen: it is encoded in inode high bits and the client routing map, so a drift
/// after a release would misroute existing data. Idempotent: re-registering the
/// same identity always succeeds. Shared by both control-store backends.
pub(crate) fn register_shard_identity(
    record: &mut ShardRecord,
    prefix: String,
    shard_index: u16,
) -> Result<(), ControlError> {
    if record.prefix == prefix && record.shard_index == shard_index {
        return Ok(());
    }
    // Pristine == never owned and never leased. `epoch == 0` is the durable
    // marker (acquire bumps it to >= 1 and release does not reset it).
    if record.epoch == 0 && record.owner.is_none() {
        record.prefix = prefix;
        record.shard_index = shard_index;
        return Ok(());
    }
    Err(ControlError::ShardIdentityLocked {
        shard_id: record.shard_id.clone(),
    })
}

/// Whether a shard id denotes the default/root shard (prefix `/`), which is the
/// single shard allowed to be acquired without a prior `register_shard` — its
/// identity (prefix `/`, index 0) is the unambiguous bootstrap default. Every
/// non-root shard must be registered first so its index cannot silently be 0.
pub(crate) fn is_default_shard(shard_id: &ShardId) -> bool {
    shard_id
        .as_str()
        .split_once(':')
        .map(|(_, path)| path)
        .unwrap_or("/")
        == "/"
}

/// Merge one caller-serialized, owner-fenced recovery publication.
///
/// This rejects durable-LSN rollback and conflicting comparable log identities,
/// but it is not a concurrency sequencer: in particular, checkpoint-only images
/// at LSN zero may legitimately replace one another. Capture order must already
/// be serialized by the `mark_serving` caller as documented on [`ControlStore`].
///
/// Checkpoints dominate logs they fully cover: once a checkpoint at the
/// durable tail is published, its covered log chain can be pruned and must not
/// be reattached by a delayed same-LSN log publication. A later log may advance
/// beyond that checkpoint and retains it as its replay base.
pub(crate) fn apply_recovery_publication(
    record: &mut ShardRecord,
    checkpoint: Option<CheckpointRef>,
    log: Option<LogRef>,
    durable_lsn: u64,
) -> Result<(), ControlError> {
    let shard_id = record.shard_id.clone();
    let conflict = |reason: String| ControlError::RecoveryPublicationConflict {
        shard_id: shard_id.clone(),
        reason,
    };

    let reference_lsn = match (checkpoint.as_ref(), log.as_ref()) {
        (Some(checkpoint), Some(log)) => checkpoint.lsn.max(log.durable_lsn),
        (Some(checkpoint), None) => checkpoint.lsn,
        (None, Some(log)) => log.durable_lsn,
        (None, None) => {
            if durable_lsn > record.durable_lsn {
                return Err(conflict(format!(
                    "durable LSN {durable_lsn} has no checkpoint or log identity"
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
        // Checkpoint-only deployments have no logical log allocator, so every
        // image legitimately carries LSN 0 and the zero digest even as metadata
        // changes. The owner publication gate serializes those images; at the
        // durable-store boundary only their logical tail digest is comparable.
    }
    if let Some(log) = log.as_ref() {
        if log.durable_lsn < record.durable_lsn {
            return Err(conflict(format!(
                "log LSN {} is behind durable LSN {}",
                log.durable_lsn, record.durable_lsn
            )));
        }
        validate_log_ref(record, checkpoint.as_ref(), log).map_err(conflict)?;
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

    let current_tail_digest = durable_tail_digest(record).map_err(conflict)?;
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
    record: &ShardRecord,
    incoming_checkpoint: Option<&CheckpointRef>,
    log: &LogRef,
) -> Result<(), String> {
    if log.segments.is_empty() {
        return Err("log segment chain is empty".to_owned());
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
        if let Some(previous) = index.checked_sub(1).map(|previous| &log.segments[previous]) {
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
    // A log at the checkpoint tail is a delayed, fully-covered publication. It
    // is still validated internally and then discarded by the checkpoint-cover
    // rule, but its historical first LSN need not follow the new checkpoint.
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

fn durable_tail_digest(record: &ShardRecord) -> Result<Option<String>, String> {
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

#[derive(Default)]
pub struct InMemoryControlStore {
    shards: Mutex<BTreeMap<ShardId, ShardRecord>>,
}

impl InMemoryControlStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_lease(record: &ShardRecord) -> u64 {
        record.lease_id.saturating_add(1).max(1)
    }

    fn validate_lease(record: &ShardRecord, lease: &ShardLease) -> Result<(), ControlError> {
        if record.owner.as_ref() != Some(&lease.owner) {
            return Err(ControlError::NotOwner {
                shard_id: lease.shard_id.clone(),
            });
        }
        if record.epoch != lease.epoch || record.lease_id != lease.lease_id {
            return Err(ControlError::StaleLease {
                shard_id: lease.shard_id.clone(),
                epoch: lease.epoch,
                lease_id: lease.lease_id,
            });
        }
        Ok(())
    }
}

impl ControlStore for InMemoryControlStore {
    fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .entry(shard_id.clone())
            .or_insert_with(|| ShardRecord::unassigned(shard_id));
        Ok(record.clone())
    }

    fn register_shard(
        &self,
        shard_id: ShardId,
        prefix: String,
        shard_index: u16,
    ) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .entry(shard_id.clone())
            .or_insert_with(|| ShardRecord::unassigned(shard_id));
        register_shard_identity(record, prefix, shard_index)?;
        Ok(record.clone())
    }

    fn set_subtree_root_inode(
        &self,
        shard_id: &ShardId,
        subtree_root_inode: Option<u64>,
    ) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))?;
        record.subtree_root_inode = subtree_root_inode;
        Ok(record.clone())
    }

    fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError> {
        let shards = self.shards.lock().expect("control store mutex poisoned");
        Ok(shards.values().cloned().collect())
    }

    fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError> {
        let shards = self.shards.lock().expect("control store mutex poisoned");
        shards
            .get(shard_id)
            .cloned()
            .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))
    }

    fn acquire_unassigned(
        &self,
        shard_id: ShardId,
        owner: NodeId,
    ) -> Result<ShardLease, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        // A non-default shard MUST be registered first: auto-creating it here via
        // `unassigned` would default its `shard_index` to 0 and collide with the
        // root shard, breaking shard-index uniqueness and inode routing. The
        // default/root shard keeps its bootstrap path (auto-create with index 0).
        let record = match shards.get_mut(&shard_id) {
            Some(record) => record,
            None if is_default_shard(&shard_id) => shards
                .entry(shard_id.clone())
                .or_insert_with(|| ShardRecord::unassigned(shard_id.clone())),
            None => {
                return Err(ControlError::ShardNotRegistered { shard_id });
            }
        };
        if let Some(existing_owner) = record.owner.clone() {
            return Err(ControlError::ShardAlreadyOwned {
                shard_id,
                owner: existing_owner,
                epoch: record.epoch,
            });
        }
        record.owner = Some(owner.clone());
        record.endpoint = Some(owner.as_str().to_owned());
        record.epoch = record.epoch.saturating_add(1).max(1);
        record.lease_id = Self::next_lease(record);
        // Acquisition only establishes exclusive recovery ownership. The shard
        // must remain unroutable until the owner has restored local metadata,
        // installed its durability fences, and published an exact recovery
        // checkpoint through `mark_serving`.
        record.state = ShardState::Recovering;
        Ok(ShardLease {
            shard_id,
            owner,
            epoch: record.epoch,
            lease_id: record.lease_id,
        })
    }

    fn acquire_after_failure(
        &self,
        shard_id: ShardId,
        owner: NodeId,
        previous_epoch: u64,
    ) -> Result<ShardLease, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(&shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))?;
        if record.epoch != previous_epoch {
            return Err(ControlError::StaleEpoch {
                shard_id,
                expected: previous_epoch,
                actual: record.epoch,
            });
        }
        record.owner = Some(owner.clone());
        record.endpoint = Some(owner.as_str().to_owned());
        record.epoch = record.epoch.saturating_add(1);
        record.lease_id = Self::next_lease(record);
        record.state = ShardState::Recovering;
        Ok(ShardLease {
            shard_id,
            owner,
            epoch: record.epoch,
            lease_id: record.lease_id,
        })
    }

    fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
        let shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get(&lease.shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(lease.shard_id.clone()))?;
        Self::validate_lease(record, lease)?;
        Ok(record.clone())
    }

    fn mark_serving(
        &self,
        lease: &ShardLease,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(&lease.shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(lease.shard_id.clone()))?;
        Self::validate_lease(record, lease)?;
        apply_recovery_publication(record, checkpoint, log, durable_lsn)?;
        record.state = ShardState::Serving;
        record.ever_served = true;
        Ok(record.clone())
    }

    fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
        let mut shards = self.shards.lock().expect("control store mutex poisoned");
        let record = shards
            .get_mut(&lease.shard_id)
            .ok_or_else(|| ControlError::ShardNotFound(lease.shard_id.clone()))?;
        Self::validate_lease(record, lease)?;
        record.owner = None;
        record.endpoint = None;
        record.state = ShardState::Unassigned;
        Ok(record.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogSegmentRef;

    fn shard() -> ShardId {
        ShardId::new("mount-1:/runs")
    }

    fn node(raw: &str) -> NodeId {
        NodeId::new(raw)
    }

    /// A store with the non-default test shard already registered, matching the
    /// production precondition that every non-root shard is registered before it
    /// is acquired.
    fn registered_store() -> InMemoryControlStore {
        let store = InMemoryControlStore::new();
        store
            .register_shard(shard(), "/runs".to_owned(), 2)
            .unwrap();
        store
    }

    fn checkpoint_ref(lsn: u64, digest: &str, identity: &str) -> CheckpointRef {
        CheckpointRef {
            object_key: format!("meta/checkpoints/{identity}"),
            lsn,
            image_bytes: 1024,
            image_digest: format!("sha256:{identity}"),
            digest: digest.to_owned(),
        }
    }

    fn log_ref(lsn: u64, digest: &str, identity: &str) -> LogRef {
        log_ref_range(1, lsn, digest, identity)
    }

    fn log_ref_range(first_lsn: u64, last_lsn: u64, digest: &str, identity: &str) -> LogRef {
        LogRef {
            segments: vec![LogSegmentRef {
                segment_key: format!("meta/log/{identity}"),
                first_lsn,
                last_lsn,
                digest: digest.to_owned(),
            }],
            durable_lsn: last_lsn,
            digest: digest.to_owned(),
        }
    }

    fn extend_log_ref(current: &LogRef, lsn: u64, digest: &str, identity: &str) -> LogRef {
        let mut segments = current.segments.clone();
        segments.push(LogSegmentRef {
            segment_key: format!("meta/log/{identity}"),
            first_lsn: current.durable_lsn + 1,
            last_lsn: lsn,
            digest: digest.to_owned(),
        });
        LogRef {
            segments,
            durable_lsn: lsn,
            digest: digest.to_owned(),
        }
    }

    #[test]
    fn fresh_acquire_sets_owner_epoch_and_lease() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();

        assert_eq!(lease.epoch, 1);
        assert_eq!(lease.lease_id, 1);

        let record = store.get_shard(&lease.shard_id).unwrap();
        assert_eq!(record.owner, Some(node("node-a")));
        assert_eq!(record.state, ShardState::Recovering);
        assert!(!record.ever_served);
        // Registered identity survives acquisition.
        assert_eq!(record.shard_index, 2);
        assert_eq!(record.prefix, "/runs");
    }

    #[test]
    fn second_fresh_owner_is_rejected() {
        let store = registered_store();
        let _lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();

        let err = store
            .acquire_unassigned(shard(), node("node-b"))
            .unwrap_err();

        assert!(matches!(
            err,
            ControlError::ShardAlreadyOwned {
                owner,
                epoch: 1,
                ..
            } if owner == node("node-a")
        ));
    }

    #[test]
    fn failover_bumps_epoch_and_fences_old_lease() {
        let store = registered_store();
        let old = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let new = store
            .acquire_after_failure(shard(), node("node-b"), old.epoch)
            .unwrap();

        assert_eq!(new.epoch, 2);
        assert_eq!(new.lease_id, 2);
        assert_eq!(store.renew(&new).unwrap().state, ShardState::Recovering);
        assert!(matches!(
            store.renew(&old).unwrap_err(),
            ControlError::NotOwner { .. }
        ));
    }

    #[test]
    fn mark_serving_requires_current_lease() {
        let store = registered_store();
        let old = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let new = store
            .acquire_after_failure(shard(), node("node-b"), old.epoch)
            .unwrap();

        assert!(store.mark_serving(&old, None, None, 0).is_err());

        let record = store.mark_serving(&new, None, None, 0).unwrap();
        assert_eq!(record.state, ShardState::Serving);
        assert_eq!(record.durable_lsn, 0);
        assert!(record.ever_served);
    }

    #[test]
    fn mark_serving_preserves_recovery_refs_when_not_replaced() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let checkpoint = checkpoint_ref(9, "abc123", "ckpt-9");
        let log = log_ref_range(10, 10, "def456", "segment-10");
        store
            .mark_serving(&lease, Some(checkpoint.clone()), None, 9)
            .unwrap();
        store
            .mark_serving(&lease, None, Some(log.clone()), 10)
            .unwrap();

        let record = store.mark_serving(&lease, None, None, 0).unwrap();

        assert_eq!(record.checkpoint, Some(checkpoint));
        assert_eq!(record.log, Some(log));
        assert_eq!(record.durable_lsn, 10);
    }

    #[test]
    fn mark_serving_rejects_a_log_behind_the_durable_tail() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let older = log_ref(9, "digest-9", "segment-9");
        let newer = extend_log_ref(&older, 10, "digest-10", "segment-10");
        store
            .mark_serving(&lease, None, Some(older.clone()), 9)
            .unwrap();
        store
            .mark_serving(&lease, None, Some(newer.clone()), 10)
            .unwrap();

        let err = store
            .mark_serving(&lease, None, Some(older), 9)
            .unwrap_err();
        assert!(matches!(
            err,
            ControlError::RecoveryPublicationConflict { .. }
        ));
        let record = store.get_shard(&lease.shard_id).unwrap();
        assert_eq!(record.durable_lsn, 10);
        assert_eq!(record.log, Some(newer));
    }

    #[test]
    fn mark_serving_rejects_conflicting_log_identity_at_the_same_lsn() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let published = log_ref(9, "digest-9", "segment-a");
        store
            .mark_serving(&lease, None, Some(published.clone()), 9)
            .unwrap();

        for conflicting in [
            log_ref(9, "digest-9", "segment-b"),
            log_ref(9, "different-digest", "segment-a"),
        ] {
            assert!(matches!(
                store.mark_serving(&lease, None, Some(conflicting), 9),
                Err(ControlError::RecoveryPublicationConflict { .. })
            ));
        }
        assert_eq!(
            store.get_shard(&lease.shard_id).unwrap().log,
            Some(published)
        );
    }

    #[test]
    fn malformed_log_publication_cannot_replace_the_durable_chain() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let durable = log_ref(2, "digest-2", "segment-1-2");
        store
            .mark_serving(&lease, None, Some(durable.clone()), 2)
            .unwrap();

        let truncated = log_ref_range(3, 3, "digest-3", "segment-3");
        let mut gap = durable.clone();
        gap.segments.push(LogSegmentRef {
            segment_key: "meta/log/segment-4".to_owned(),
            first_lsn: 4,
            last_lsn: 4,
            digest: "digest-4".to_owned(),
        });
        gap.durable_lsn = 4;
        gap.digest = "digest-4".to_owned();
        let mut tail_mismatch = extend_log_ref(&durable, 3, "digest-3", "segment-3");
        tail_mismatch.durable_lsn = 4;
        let mut digest_mismatch = extend_log_ref(&durable, 3, "digest-3", "segment-3");
        digest_mismatch.digest = "different-tail-digest".to_owned();

        for malformed in [
            LogRef {
                segments: Vec::new(),
                durable_lsn: 3,
                digest: "digest-3".to_owned(),
            },
            truncated,
            gap,
            tail_mismatch,
            digest_mismatch,
        ] {
            assert!(matches!(
                store.mark_serving(&lease, None, Some(malformed.clone()), malformed.durable_lsn,),
                Err(ControlError::RecoveryPublicationConflict { .. })
            ));
            let record = store.get_shard(&lease.shard_id).unwrap();
            assert_eq!(record.log, Some(durable.clone()));
            assert_eq!(record.durable_lsn, 2);
        }
    }

    #[test]
    fn checkpoint_covers_log_and_only_a_newer_log_reopens_the_chain() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let covered_log = log_ref(9, "digest-9", "segment-9");
        store
            .mark_serving(&lease, None, Some(covered_log.clone()), 9)
            .unwrap();
        let checkpoint = checkpoint_ref(9, "digest-9", "ckpt-9");
        let covered = store
            .mark_serving(&lease, Some(checkpoint.clone()), None, 9)
            .unwrap();
        assert_eq!(covered.checkpoint, Some(checkpoint.clone()));
        assert_eq!(covered.log, None);

        let delayed = store
            .mark_serving(&lease, None, Some(covered_log), 9)
            .unwrap();
        assert_eq!(delayed.checkpoint, Some(checkpoint.clone()));
        assert_eq!(delayed.log, None);

        let advanced_log = log_ref_range(10, 10, "digest-10", "segment-10");
        let advanced = store
            .mark_serving(&lease, None, Some(advanced_log.clone()), 10)
            .unwrap();
        assert_eq!(advanced.checkpoint, Some(checkpoint));
        assert_eq!(advanced.log, Some(advanced_log));
        assert_eq!(advanced.durable_lsn, 10);
    }

    #[test]
    fn checkpoint_only_publication_allows_a_new_image_at_lsn_zero() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let first = checkpoint_ref(0, "zero-digest", "image-a");
        let second = checkpoint_ref(0, "zero-digest", "image-b");
        store.mark_serving(&lease, Some(first), None, 0).unwrap();

        let record = store
            .mark_serving(&lease, Some(second.clone()), None, 0)
            .unwrap();
        assert_eq!(record.checkpoint, Some(second));
        assert_eq!(record.durable_lsn, 0);
    }

    #[test]
    fn mark_serving_cannot_advance_without_a_recovery_identity() {
        let store = registered_store();
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();

        assert!(matches!(
            store.mark_serving(&lease, None, None, 1),
            Err(ControlError::RecoveryPublicationConflict { .. })
        ));
        let record = store.get_shard(&lease.shard_id).unwrap();
        assert_eq!(record.durable_lsn, 0);
        assert_eq!(record.state, ShardState::Recovering);
    }

    #[test]
    fn set_subtree_root_inode_records_and_clears() {
        let store = InMemoryControlStore::new();
        store.ensure_shard(shard()).unwrap();

        // Set, then read back through the durable record.
        let updated = store
            .set_subtree_root_inode(&shard(), Some(0x0001_0000_0000_0002))
            .unwrap();
        assert_eq!(updated.subtree_root_inode, Some(0x0001_0000_0000_0002));
        assert_eq!(
            store.get_shard(&shard()).unwrap().subtree_root_inode,
            Some(0x0001_0000_0000_0002)
        );

        // Idempotent re-set to the same value.
        store
            .set_subtree_root_inode(&shard(), Some(0x0001_0000_0000_0002))
            .unwrap();

        // Clearing (unregister) returns to None.
        let cleared = store.set_subtree_root_inode(&shard(), None).unwrap();
        assert_eq!(cleared.subtree_root_inode, None);
        assert_eq!(store.get_shard(&shard()).unwrap().subtree_root_inode, None);
    }

    #[test]
    fn set_subtree_root_inode_on_missing_shard_is_not_found() {
        let store = InMemoryControlStore::new();
        let err = store
            .set_subtree_root_inode(&ShardId::new("mount-1:/absent"), Some(7))
            .unwrap_err();
        assert!(matches!(err, ControlError::ShardNotFound(_)));
    }

    #[test]
    fn release_requires_current_lease() {
        let store = registered_store();
        let old = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        let new = store
            .acquire_after_failure(shard(), node("node-b"), old.epoch)
            .unwrap();

        assert!(store.release(&old).is_err());

        let released = store.release(&new).unwrap();
        assert_eq!(released.owner, None);
        assert_eq!(released.state, ShardState::Unassigned);
        assert_eq!(released.epoch, new.epoch);
    }

    #[test]
    fn register_shard_freezes_identity_after_first_lease() {
        let store = registered_store();
        // Re-registering the SAME identity is idempotent, even while owned.
        let lease = store.acquire_unassigned(shard(), node("node-a")).unwrap();
        store
            .register_shard(shard(), "/runs".to_owned(), 2)
            .unwrap();

        // Releasing leaves epoch > 0; identity must stay frozen so a later
        // re-register cannot drift the index a live client routes by.
        store.release(&lease).unwrap();
        let record = store.get_shard(&shard()).unwrap();
        assert!(record.owner.is_none());
        assert!(record.epoch > 0);

        let err = store
            .register_shard(shard(), "/runs".to_owned(), 9)
            .unwrap_err();
        assert!(matches!(err, ControlError::ShardIdentityLocked { .. }));
        // The original index is unchanged.
        assert_eq!(store.get_shard(&shard()).unwrap().shard_index, 2);
    }

    #[test]
    fn register_shard_assigns_identity_while_pristine() {
        let store = InMemoryControlStore::new();
        // Before any lease, identity is freely (re)assignable.
        store
            .register_shard(shard(), "/runs".to_owned(), 2)
            .unwrap();
        let record = store
            .register_shard(shard(), "/runs".to_owned(), 5)
            .unwrap();
        assert_eq!(record.shard_index, 5);
        assert_eq!(record.epoch, 0);
    }

    #[test]
    fn acquire_unassigned_requires_registration_for_non_default_shard() {
        let store = InMemoryControlStore::new();
        // No register_shard: a non-default shard cannot be acquired (would
        // otherwise auto-create with shard_index 0 and collide with root).
        let err = store
            .acquire_unassigned(shard(), node("node-a"))
            .unwrap_err();
        assert!(matches!(err, ControlError::ShardNotRegistered { .. }));
        assert!(store.get_shard(&shard()).is_err());
    }

    #[test]
    fn acquire_unassigned_bootstraps_default_shard_without_registration() {
        let store = InMemoryControlStore::new();
        let default_shard = ShardId::new("mount-1:/");
        let lease = store
            .acquire_unassigned(default_shard.clone(), node("node-a"))
            .unwrap();
        assert_eq!(lease.epoch, 1);
        let record = store.get_shard(&default_shard).unwrap();
        assert_eq!(record.shard_index, 0);
        assert_eq!(record.prefix, "/");
    }
}
