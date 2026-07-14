use std::future::Future;

use etcd_client::{Client, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use tokio::runtime::{Builder, Runtime};

use crate::store::{apply_recovery_publication, ControlStore};
use crate::{
    decode_shard_record, encode_shard_record, CheckpointRef, ControlError, EtcdControlStoreOptions,
    LogRef, NodeId, ShardId, ShardLease, ShardRecord, ShardState,
};

pub struct EtcdControlStore {
    options: EtcdControlStoreOptions,
    runtime: Runtime,
    client: Client,
}

struct EtcdShardRecord {
    record: ShardRecord,
    mod_revision: i64,
}

impl EtcdControlStore {
    pub fn connect(options: EtcdControlStoreOptions) -> Result<Self, ControlError> {
        options.validate()?;
        let runtime = Builder::new_multi_thread()
            .thread_name("nokv-control-etcd")
            .enable_all()
            .build()
            .map_err(|err| ControlError::Backend(format!("etcd runtime init failed: {err}")))?;
        let client = runtime
            .block_on(Client::connect(options.endpoints(), None))
            .map_err(etcd_backend)?;
        Ok(Self {
            options,
            runtime,
            client,
        })
    }

    pub fn options(&self) -> &EtcdControlStoreOptions {
        &self.options
    }

    fn block_on<T>(
        &self,
        future: impl Future<Output = Result<T, ControlError>>,
    ) -> Result<T, ControlError> {
        self.runtime.block_on(future)
    }
}

impl ControlStore for EtcdControlStore {
    fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let record = ensure_shard_record(&mut client, &options, shard_id).await?;
            Ok(record.record)
        })
    }

    fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let shard_id = shard_id.clone();
        self.block_on(async move {
            let key = options.shard_record_key(&shard_id);
            Ok(fetch_shard_record(&mut client, key, &shard_id)
                .await?
                .record)
        })
    }

    fn register_shard(
        &self,
        shard_id: ShardId,
        prefix: String,
        shard_index: u16,
    ) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&shard_id);
            let current = ensure_shard_record(&mut client, &options, shard_id.clone()).await?;
            // Re-registering the same identity is always idempotent.
            if current.record.prefix == prefix && current.record.shard_index == shard_index {
                return Ok(current.record);
            }
            // Identity is mutable ONLY while the record is pristine (never leased:
            // epoch == 0 and unowned). Once a shard has served, its index is baked
            // into inode high bits and the client routing map, so a drift after a
            // release would misroute existing data. Reject the change. Mirrors
            // `register_shard_identity` in the in-memory backend.
            if current.record.epoch != 0 || current.record.owner.is_some() {
                return Err(ControlError::ShardIdentityLocked { shard_id });
            }
            let mut next = current.record.clone();
            next.prefix = prefix;
            next.shard_index = shard_index;
            let txn = Txn::new()
                .when(vec![Compare::mod_revision(
                    record_key.clone(),
                    CompareOp::Equal,
                    current.mod_revision,
                )])
                .and_then(vec![TxnOp::put(
                    record_key.clone(),
                    encode_shard_record(&next)?,
                    None,
                )]);
            let response = client.txn(txn).await.map_err(etcd_backend)?;
            if response.succeeded() {
                return Ok(next);
            }
            // Lost a concurrent registration race; return the durable record.
            Ok(fetch_shard_record(&mut client, record_key, &shard_id)
                .await?
                .record)
        })
    }

    fn set_subtree_root_inode(
        &self,
        shard_id: &ShardId,
        subtree_root_inode: Option<u64>,
    ) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let shard_id = shard_id.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&shard_id);
            loop {
                let current =
                    fetch_shard_record(&mut client, record_key.clone(), &shard_id).await?;
                if current.record.subtree_root_inode == subtree_root_inode {
                    // Idempotent: already in the desired state.
                    return Ok(current.record);
                }
                let mut next = current.record.clone();
                next.subtree_root_inode = subtree_root_inode;
                let txn = Txn::new()
                    .when(vec![Compare::mod_revision(
                        record_key.clone(),
                        CompareOp::Equal,
                        current.mod_revision,
                    )])
                    .and_then(vec![TxnOp::put(
                        record_key.clone(),
                        encode_shard_record(&next)?,
                        None,
                    )]);
                let response = client.txn(txn).await.map_err(etcd_backend)?;
                if response.succeeded() {
                    return Ok(next);
                }
                // Lost a concurrent write race; reread and retry the CAS.
            }
        })
    }

    fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let prefix = options.shards_prefix();
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_backend)?;
            let mut records = Vec::with_capacity(response.kvs().len());
            for kv in response.kvs() {
                records.push(decode_shard_record(kv.value())?);
            }
            Ok(records)
        })
    }

    fn acquire_unassigned(
        &self,
        shard_id: ShardId,
        owner: NodeId,
    ) -> Result<ShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&shard_id);
            // A non-default shard MUST be registered first: auto-creating it here
            // would default `shard_index` to 0 and collide with the root shard.
            // The default/root shard keeps its bootstrap path (auto-create).
            let current = if crate::store::is_default_shard(&shard_id) {
                ensure_shard_record(&mut client, &options, shard_id.clone()).await?
            } else {
                match fetch_shard_record(&mut client, record_key.clone(), &shard_id).await {
                    Ok(current) => current,
                    Err(ControlError::ShardNotFound(_)) => {
                        return Err(ControlError::ShardNotRegistered { shard_id });
                    }
                    Err(err) => return Err(err),
                }
            };
            if let Some(existing_owner) = current.record.owner.clone() {
                // A crashed owner leaves its durable record behind after the
                // lease-attached session key expires. Fresh may take over that
                // stale record only when durable control state proves this shard
                // never reached Serving and has no recovery identity. The txn's
                // session-key create_revision guard below is the atomic liveness
                // proof; an active prior session still rejects the takeover.
                let never_served_empty = !current.record.ever_served
                    && current.record.state == ShardState::Recovering
                    && current.record.checkpoint.is_none()
                    && current.record.log.is_none()
                    && current.record.durable_lsn == 0;
                if !never_served_empty {
                    return Err(ControlError::ShardAlreadyOwned {
                        shard_id,
                        owner: existing_owner,
                        epoch: current.record.epoch,
                    });
                }
            }

            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let mut next = current.record.clone();
            next.owner = Some(owner.clone());
            next.endpoint = Some(owner.as_str().to_owned());
            next.epoch = next.epoch.saturating_add(1).max(1);
            next.lease_id = lease_id;
            // A fresh lease grants recovery authority, not readiness. Only the
            // exact-lease CAS in `mark_serving` may make the shard routable after
            // restore and checkpoint publication have completed.
            next.state = ShardState::Recovering;

            let lease = ShardLease {
                shard_id: shard_id.clone(),
                owner,
                epoch: next.epoch,
                lease_id,
            };
            let session_key = options.shard_session_key(&shard_id);
            // `create_revision == 0` means no live session key exists for this
            // shard (the stable key is attached to the owner's lease, so it is
            // present iff some owner's lease is alive). Combined with the record
            // mod_revision guard, this is the atomic "shard is genuinely
            // unowned" check.
            let txn = Txn::new()
                .when(vec![
                    Compare::mod_revision(
                        record_key.clone(),
                        CompareOp::Equal,
                        current.mod_revision,
                    ),
                    Compare::create_revision(session_key.clone(), CompareOp::Equal, 0),
                ])
                .and_then(vec![
                    TxnOp::put(record_key.clone(), encode_shard_record(&next)?, None),
                    TxnOp::put(
                        session_key,
                        encode_shard_lease(&lease)?,
                        Some(PutOptions::new().with_lease(lease_id_i64(lease_id)?)),
                    ),
                ]);
            let response = client.txn(txn).await.map_err(etcd_backend)?;
            if response.succeeded() {
                return Ok(lease);
            }
            revoke_lease_best_effort(&mut client, lease_id).await;

            let latest = fetch_shard_record(&mut client, record_key, &shard_id).await?;
            if let Some(existing_owner) = latest.record.owner {
                return Err(ControlError::ShardAlreadyOwned {
                    shard_id,
                    owner: existing_owner,
                    epoch: latest.record.epoch,
                });
            }
            // Record reads unowned but the txn failed: a live session key still
            // guards the shard (a previous owner's lease has not yet expired).
            Err(ControlError::Backend(
                "previous owner session still live".to_owned(),
            ))
        })
    }

    fn acquire_after_failure(
        &self,
        shard_id: ShardId,
        owner: NodeId,
        previous_epoch: u64,
    ) -> Result<ShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&shard_id);
            let current = fetch_shard_record(&mut client, record_key.clone(), &shard_id).await?;
            // Up-front epoch check gives a clear error when the caller raced
            // another failover; the binding guard is still the atomic txn below.
            if current.record.epoch != previous_epoch {
                return Err(ControlError::StaleEpoch {
                    shard_id,
                    expected: previous_epoch,
                    actual: current.record.epoch,
                });
            }

            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let mut next = current.record.clone();
            next.owner = Some(owner.clone());
            next.endpoint = Some(owner.as_str().to_owned());
            next.epoch = next.epoch.saturating_add(1);
            next.lease_id = lease_id;
            next.state = ShardState::Recovering;

            let lease = ShardLease {
                shard_id: shard_id.clone(),
                owner,
                epoch: next.epoch,
                lease_id,
            };
            let session_key = options.shard_session_key(&shard_id);
            // The stable session key is created iff some owner's lease is alive.
            // `create_revision == 0` therefore means the previous owner's lease
            // has expired and its session key auto-deleted — this folds the
            // "previous session absent" check INTO the txn (no separate GET, no
            // TOCTOU). If the old owner is still keeping its lease alive, the key
            // is present, `create_revision != 0`, and the txn fails atomically.
            let txn = Txn::new()
                .when(vec![
                    Compare::mod_revision(
                        record_key.clone(),
                        CompareOp::Equal,
                        current.mod_revision,
                    ),
                    Compare::create_revision(session_key.clone(), CompareOp::Equal, 0),
                ])
                .and_then(vec![
                    TxnOp::put(record_key.clone(), encode_shard_record(&next)?, None),
                    TxnOp::put(
                        session_key,
                        encode_shard_lease(&lease)?,
                        Some(PutOptions::new().with_lease(lease_id_i64(lease_id)?)),
                    ),
                ]);
            let response = client.txn(txn).await.map_err(etcd_backend)?;
            if response.succeeded() {
                return Ok(lease);
            }
            revoke_lease_best_effort(&mut client, lease_id).await;

            let latest = fetch_shard_record(&mut client, record_key, &shard_id).await?;
            if latest.record.epoch != previous_epoch {
                return Err(ControlError::StaleEpoch {
                    shard_id,
                    expected: previous_epoch,
                    actual: latest.record.epoch,
                });
            }
            if let Some(existing_owner) = latest.record.owner {
                return Err(ControlError::ShardAlreadyOwned {
                    shard_id,
                    owner: existing_owner,
                    epoch: latest.record.epoch,
                });
            }
            // Epoch and owner are unchanged, so the failure is the session-key
            // guard: the previous owner's lease is still alive (keepalive holding
            // it) and its stable session key still exists. Refuse the failover —
            // this is the mutual-exclusion fence.
            Err(ControlError::Backend(
                "previous owner session still live".to_owned(),
            ))
        })
    }

    fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&lease.shard_id);
            let current = fetch_shard_record(&mut client, record_key, &lease.shard_id).await?;
            validate_record_lease(&current.record, &lease)?;
            validate_session_key(&mut client, &options, &lease).await?;

            let lease_id = lease_id_i64(lease.lease_id)?;
            let (mut keeper, mut stream) = client
                .lease_keep_alive(lease_id)
                .await
                .map_err(etcd_backend)?;
            keeper.keep_alive().await.map_err(etcd_backend)?;
            let response = stream
                .message()
                .await
                .map_err(etcd_backend)?
                .ok_or_else(|| {
                    ControlError::Backend("etcd lease keepalive stream ended".to_owned())
                })?;
            if response.ttl() <= 0 {
                return Err(ControlError::StaleLease {
                    shard_id: lease.shard_id,
                    epoch: lease.epoch,
                    lease_id: lease.lease_id,
                });
            }
            Ok(current.record)
        })
    }

    fn mark_serving(
        &self,
        lease: &ShardLease,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&lease.shard_id);
            let current =
                fetch_shard_record(&mut client, record_key.clone(), &lease.shard_id).await?;
            validate_record_lease(&current.record, &lease)?;

            let mut next = current.record.clone();
            apply_recovery_publication(&mut next, checkpoint, log, durable_lsn)?;
            next.state = ShardState::Serving;
            next.ever_served = true;

            // `Compare::lease(session_key) == my lease_id` proves I am still the
            // live owner: the stable session key exists AND is attached to my
            // lease (not a successor's). A fenced owner fails this guard.
            let session_key = options.shard_session_key(&lease.shard_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::mod_revision(
                        record_key.clone(),
                        CompareOp::Equal,
                        current.mod_revision,
                    ),
                    Compare::lease(
                        session_key.clone(),
                        CompareOp::Equal,
                        lease_id_i64(lease.lease_id)?,
                    ),
                ])
                .and_then(vec![TxnOp::put(
                    record_key.clone(),
                    encode_shard_record(&next)?,
                    None,
                )]);
            let response = client.txn(txn).await.map_err(etcd_backend)?;
            if response.succeeded() {
                return Ok(next);
            }
            classify_owner_compare_failure(&mut client, &options, &record_key, &lease).await
        })
    }

    fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let record_key = options.shard_record_key(&lease.shard_id);
            let current =
                fetch_shard_record(&mut client, record_key.clone(), &lease.shard_id).await?;
            validate_record_lease(&current.record, &lease)?;

            let mut next = current.record.clone();
            next.owner = None;
            next.endpoint = None;
            next.state = ShardState::Unassigned;

            // Same liveness guard as mark_serving; on success we delete the
            // stable session key and then revoke the lease so the shard is
            // immediately re-acquirable.
            let session_key = options.shard_session_key(&lease.shard_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::mod_revision(
                        record_key.clone(),
                        CompareOp::Equal,
                        current.mod_revision,
                    ),
                    Compare::lease(
                        session_key.clone(),
                        CompareOp::Equal,
                        lease_id_i64(lease.lease_id)?,
                    ),
                ])
                .and_then(vec![
                    TxnOp::put(record_key.clone(), encode_shard_record(&next)?, None),
                    TxnOp::delete(session_key, None),
                ]);
            let response = client.txn(txn).await.map_err(etcd_backend)?;
            if response.succeeded() {
                revoke_lease_best_effort(&mut client, lease.lease_id).await;
                return Ok(next);
            }
            classify_owner_compare_failure(&mut client, &options, &record_key, &lease).await
        })
    }
}

async fn ensure_shard_record(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    shard_id: ShardId,
) -> Result<EtcdShardRecord, ControlError> {
    let key = options.shard_record_key(&shard_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    if let Some(kv) = response.kvs().first() {
        return Ok(EtcdShardRecord {
            record: decode_shard_record(kv.value())?,
            mod_revision: kv.mod_revision(),
        });
    }

    let record = ShardRecord::unassigned(shard_id.clone());
    let txn = Txn::new()
        .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
        .and_then(vec![TxnOp::put(
            key.clone(),
            encode_shard_record(&record)?,
            None,
        )]);
    let response = client.txn(txn).await.map_err(etcd_backend)?;
    if response.succeeded() {
        return fetch_shard_record(client, key, &shard_id).await;
    }
    fetch_shard_record(client, key, &shard_id).await
}

async fn fetch_shard_record(
    client: &mut Client,
    key: Vec<u8>,
    shard_id: &ShardId,
) -> Result<EtcdShardRecord, ControlError> {
    let response = client.get(key, None).await.map_err(etcd_backend)?;
    let kv = response
        .kvs()
        .first()
        .ok_or_else(|| ControlError::ShardNotFound(shard_id.clone()))?;
    Ok(EtcdShardRecord {
        record: decode_shard_record(kv.value())?,
        mod_revision: kv.mod_revision(),
    })
}

async fn grant_lease(client: &mut Client, ttl_seconds: i64) -> Result<u64, ControlError> {
    let response = client
        .lease_grant(ttl_seconds, None)
        .await
        .map_err(etcd_backend)?;
    u64::try_from(response.id()).map_err(|_| {
        ControlError::Backend(format!("etcd returned negative lease id {}", response.id()))
    })
}

async fn revoke_lease_best_effort(client: &mut Client, lease_id: u64) {
    if let Ok(lease_id) = lease_id_i64(lease_id) {
        let _ = client.lease_revoke(lease_id).await;
    }
}

/// Confirm the stable session key still carries *my* lease. The key is
/// identity-only, so a successor that re-acquired the shard would have replaced
/// the key's value and re-attached it to its own lease; either an absent key or
/// a different attached lease means I have been fenced.
async fn validate_session_key(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &ShardLease,
) -> Result<(), ControlError> {
    let session_key = options.shard_session_key(&lease.shard_id);
    let response = client.get(session_key, None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Err(stale_lease(lease));
    };
    if kv.lease() != lease_id_i64(lease.lease_id)? {
        return Err(stale_lease(lease));
    }
    Ok(())
}

async fn classify_owner_compare_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    record_key: &[u8],
    lease: &ShardLease,
) -> Result<ShardRecord, ControlError> {
    let latest = fetch_shard_record(client, record_key.to_vec(), &lease.shard_id).await?;
    validate_record_lease(&latest.record, lease)?;
    validate_session_key(client, options, lease).await?;
    Err(ControlError::Backend(
        "etcd owner compare failed while lease was still current".to_owned(),
    ))
}

fn validate_record_lease(record: &ShardRecord, lease: &ShardLease) -> Result<(), ControlError> {
    if record.owner.as_ref() != Some(&lease.owner) {
        return Err(ControlError::NotOwner {
            shard_id: lease.shard_id.clone(),
        });
    }
    if record.epoch != lease.epoch || record.lease_id != lease.lease_id {
        return Err(stale_lease(lease));
    }
    Ok(())
}

fn encode_shard_lease(lease: &ShardLease) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(lease).map_err(|err| ControlError::Codec(err.to_string()))
}

fn lease_id_i64(lease_id: u64) -> Result<i64, ControlError> {
    i64::try_from(lease_id)
        .map_err(|_| ControlError::Codec(format!("etcd lease id {lease_id} exceeds i64")))
}

fn stale_lease(lease: &ShardLease) -> ControlError {
    ControlError::StaleLease {
        shard_id: lease.shard_id.clone(),
        epoch: lease.epoch,
        lease_id: lease.lease_id,
    }
}

fn etcd_backend(err: etcd_client::Error) -> ControlError {
    ControlError::Backend(format!("etcd: {err}"))
}

#[cfg(test)]
mod etcd_session_tests {
    use super::*;
    use crate::store::ControlStore;
    use crate::{EtcdControlStoreOptions, NodeId, ShardId};
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Live-etcd proof that the stable per-shard session key gives mutual
    /// exclusion across owner generations. Skipped unless `NOKV_ETCD_ENDPOINTS`
    /// is set (CSV of endpoints); the CI default-test run skips it. The parent
    /// validates this against a real etcd.
    #[test]
    fn stable_session_key_fences_failover_until_lease_expires() {
        let endpoints = match std::env::var("NOKV_ETCD_ENDPOINTS") {
            Ok(raw) if !raw.trim().is_empty() => raw
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>(),
            _ => return,
        };
        if endpoints.is_empty() {
            return;
        }

        // Unique prefix per run so concurrent/leftover runs cannot collide.
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key_prefix = format!("/nokv/test/control/{}/{}", process::id(), unique);
        let options = EtcdControlStoreOptions::new(endpoints.clone())
            .with_key_prefix(key_prefix.clone())
            // A long-ish TTL keeps owner A's lease comfortably alive across the
            // "failover must fail" assertion; we end A's tenure by revoking the
            // lease directly (a crash), not by waiting it out.
            .with_lease_ttl_seconds(30);

        let store = EtcdControlStore::connect(options).expect("connect etcd control store");
        let shard_id = ShardId::new(format!("mount-test:/{unique}"));
        let owner_a = NodeId::new("node-a");
        let owner_b = NodeId::new("node-b");

        // A non-default shard must be registered before it can be acquired.
        store
            .register_shard(shard_id.clone(), format!("/{unique}"), 7)
            .expect("register non-default shard identity");

        // Owner A acquires the unassigned shard.
        let lease_a = store
            .acquire_unassigned(shard_id.clone(), owner_a.clone())
            .expect("owner A acquires unassigned shard");
        assert_eq!(lease_a.epoch, 1);

        // A second fresh acquire must be rejected: the shard is owned.
        let second = store.acquire_unassigned(shard_id.clone(), owner_b.clone());
        assert!(
            matches!(second, Err(ControlError::ShardAlreadyOwned { .. })),
            "second acquire_unassigned must fail with ShardAlreadyOwned, got {second:?}"
        );

        // MUTUAL-EXCLUSION PROOF: failover must FAIL while A's lease is alive.
        // A's stable session key is still present (create_revision != 0), so the
        // txn guard fails atomically — no TOCTOU window.
        let failover_live = store.acquire_after_failure(shard_id.clone(), owner_b.clone(), 1);
        assert!(
            failover_live.is_err(),
            "failover must be refused while owner A's lease is alive, got {failover_live:?}"
        );

        // Simulate A crashing: revoke its etcd lease directly. The lease-attached
        // session key auto-deletes; the durable record still names A at epoch 1.
        revoke_lease_via_raw_client(&endpoints, lease_a.lease_id);
        // Poll until the session key is observably gone (lease revoke + key
        // deletion is asynchronous from this client's perspective).
        let failover_dead = await_failover_success(&store, &shard_id, &owner_b, 1);
        assert_eq!(
            failover_dead.epoch, 2,
            "failover after lease expiry must bump to epoch 2"
        );
        assert_eq!(failover_dead.owner, owner_b);

        // Clean up: release B and best-effort delete the shard's keys.
        let _ = store.release(&failover_dead);
        cleanup_keys(&endpoints, &key_prefix);
    }

    #[test]
    fn fresh_resurrects_only_a_never_served_owner_after_session_expiry() {
        let Some(endpoints) = live_etcd_endpoints() else {
            return;
        };
        let key_prefix = unique_key_prefix();
        let options = EtcdControlStoreOptions::new(endpoints.clone())
            .with_key_prefix(key_prefix.clone())
            .with_lease_ttl_seconds(30);
        let store = EtcdControlStore::connect(options).expect("connect etcd control store");
        let shard_id = ShardId::new(format!("{key_prefix}:/runs"));
        store
            .register_shard(shard_id.clone(), "/runs".to_owned(), 2)
            .expect("register shard");

        let first = store
            .acquire_unassigned(shard_id.clone(), NodeId::new("node-a"))
            .expect("first Fresh acquire");
        assert!(!store.get_shard(&shard_id).unwrap().ever_served);
        assert!(matches!(
            store.acquire_unassigned(shard_id.clone(), NodeId::new("node-b")),
            Err(ControlError::ShardAlreadyOwned { .. })
        ));

        // Crash before mark_serving: the durable record keeps owner A, while
        // revoking its lease removes the only liveness proof.
        revoke_lease_via_raw_client(&endpoints, first.lease_id);
        let second = await_fresh_success(&store, &shard_id, &NodeId::new("node-b"));
        assert_eq!(second.epoch, 2);
        let resurrected = store.get_shard(&shard_id).unwrap();
        assert_eq!(resurrected.owner, Some(NodeId::new("node-b")));
        assert!(!resurrected.ever_served);
        assert!(resurrected.checkpoint.is_none());
        assert!(resurrected.log.is_none());
        assert_eq!(resurrected.durable_lsn, 0);

        // Once any generation reaches Serving, an expired session can only use
        // explicit Failover; Fresh must never reinterpret it as empty.
        store
            .mark_serving(&second, None, None, 0)
            .expect("mark resurrected shard serving");
        revoke_lease_via_raw_client(&endpoints, second.lease_id);
        let served_fresh = store.acquire_unassigned(shard_id.clone(), NodeId::new("node-c"));
        assert!(matches!(
            served_fresh,
            Err(ControlError::ShardAlreadyOwned { .. })
        ));

        cleanup_keys(&endpoints, &key_prefix);
    }

    /// Retry the failover until the revoked lease's session key has been reaped,
    /// so the create_revision==0 guard can pass. Bounded so a real failure still
    /// surfaces rather than hanging.
    fn await_failover_success(
        store: &EtcdControlStore,
        shard_id: &ShardId,
        owner: &NodeId,
        previous_epoch: u64,
    ) -> crate::ShardLease {
        use std::thread::sleep;
        use std::time::{Duration, Instant};
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match store.acquire_after_failure(shard_id.clone(), owner.clone(), previous_epoch) {
                Ok(lease) => return lease,
                Err(err) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(100));
                    let _ = err;
                }
                Err(err) => panic!("failover did not succeed after lease revoke: {err:?}"),
            }
        }
    }

    fn await_fresh_success(
        store: &EtcdControlStore,
        shard_id: &ShardId,
        owner: &NodeId,
    ) -> crate::ShardLease {
        use std::thread::sleep;
        use std::time::{Duration, Instant};
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match store.acquire_unassigned(shard_id.clone(), owner.clone()) {
                Ok(lease) => return lease,
                Err(err) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(100));
                    let _ = err;
                }
                Err(err) => {
                    panic!("Fresh resurrection did not succeed after lease revoke: {err:?}")
                }
            }
        }
    }

    fn revoke_lease_via_raw_client(endpoints: &[String], lease_id: u64) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let mut client = etcd_client::Client::connect(endpoints, None)
                .await
                .expect("connect raw etcd client");
            let lease_id = lease_id_i64(lease_id).expect("lease id fits i64");
            client.lease_revoke(lease_id).await.expect("revoke lease");
        });
    }

    /// Endpoints for the live-etcd tests, or `None` to skip (the CI default-test
    /// run skips it; the parent validates against a real etcd).
    fn live_etcd_endpoints() -> Option<Vec<String>> {
        let raw = std::env::var("NOKV_ETCD_ENDPOINTS").ok()?;
        let endpoints: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        (!endpoints.is_empty()).then_some(endpoints)
    }

    fn unique_key_prefix() -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/nokv/test/control/{}/{}", process::id(), unique)
    }

    /// Live-etcd parity for `register_shard_identity`: a shard's identity is frozen
    /// once it has taken a lease, even after a release leaves it unowned.
    #[test]
    fn etcd_register_shard_freezes_identity_after_first_lease() {
        let Some(endpoints) = live_etcd_endpoints() else {
            return;
        };
        let key_prefix = unique_key_prefix();
        let options = EtcdControlStoreOptions::new(endpoints.clone())
            .with_key_prefix(key_prefix.clone())
            .with_lease_ttl_seconds(30);
        let store = EtcdControlStore::connect(options).expect("connect etcd control store");
        let shard_id = ShardId::new(format!("{key_prefix}:/runs"));

        store
            .register_shard(shard_id.clone(), "/runs".to_owned(), 2)
            .expect("register pristine identity");
        let lease = store
            .acquire_unassigned(shard_id.clone(), NodeId::new("node-a"))
            .expect("acquire registered shard");
        // Re-registering the SAME identity stays idempotent while owned.
        store
            .register_shard(shard_id.clone(), "/runs".to_owned(), 2)
            .expect("same-identity re-register is idempotent");
        store.release(&lease).expect("release shard");

        // After a lease, a DIFFERENT identity is rejected even though unowned.
        let err = store
            .register_shard(shard_id.clone(), "/runs".to_owned(), 9)
            .expect_err("identity must be frozen after first lease");
        assert!(
            matches!(err, ControlError::ShardIdentityLocked { .. }),
            "got {err:?}"
        );
        assert_eq!(store.get_shard(&shard_id).unwrap().shard_index, 2);

        cleanup_keys(&endpoints, &key_prefix);
    }

    /// Live-etcd parity for the acquire-requires-registration rule: a non-default
    /// shard cannot be acquired unless registered first, while the default/root
    /// shard keeps its bootstrap path.
    #[test]
    fn etcd_acquire_unassigned_requires_registration_for_non_default_shard() {
        let Some(endpoints) = live_etcd_endpoints() else {
            return;
        };
        let key_prefix = unique_key_prefix();
        let options = EtcdControlStoreOptions::new(endpoints.clone())
            .with_key_prefix(key_prefix.clone())
            .with_lease_ttl_seconds(30);
        let store = EtcdControlStore::connect(options).expect("connect etcd control store");

        // Unregistered non-default shard: acquisition refused.
        let non_default = ShardId::new(format!("{key_prefix}:/runs"));
        let err = store
            .acquire_unassigned(non_default.clone(), NodeId::new("node-a"))
            .expect_err("unregistered non-default shard must be refused");
        assert!(
            matches!(err, ControlError::ShardNotRegistered { .. }),
            "got {err:?}"
        );

        // Default/root shard: bootstraps without registration, index 0.
        let default_shard = ShardId::new(format!("{key_prefix}:/"));
        let lease = store
            .acquire_unassigned(default_shard.clone(), NodeId::new("node-a"))
            .expect("default shard bootstraps without registration");
        assert_eq!(lease.epoch, 1);
        assert_eq!(store.get_shard(&default_shard).unwrap().shard_index, 0);
        let _ = store.release(&lease);

        cleanup_keys(&endpoints, &key_prefix);
    }

    #[test]
    fn etcd_rejects_malformed_log_without_overwriting_durable_ref() {
        let Some(endpoints) = live_etcd_endpoints() else {
            return;
        };
        let key_prefix = unique_key_prefix();
        let options = EtcdControlStoreOptions::new(endpoints.clone())
            .with_key_prefix(key_prefix.clone())
            .with_lease_ttl_seconds(30);
        let store = EtcdControlStore::connect(options).expect("connect etcd control store");
        let shard_id = ShardId::new(format!("{key_prefix}:/runs"));
        store
            .register_shard(shard_id.clone(), "/runs".to_owned(), 2)
            .expect("register shard");
        let lease = store
            .acquire_unassigned(shard_id.clone(), NodeId::new("node-a"))
            .expect("acquire shard");
        let durable = LogRef {
            segments: vec![crate::LogSegmentRef {
                segment_key: "meta/log/segment-1".to_owned(),
                first_lsn: 1,
                last_lsn: 1,
                digest: "digest-1".to_owned(),
            }],
            durable_lsn: 1,
            digest: "digest-1".to_owned(),
        };
        store
            .mark_serving(&lease, None, Some(durable.clone()), 1)
            .expect("publish durable log");

        let malformed = LogRef {
            segments: Vec::new(),
            durable_lsn: 2,
            digest: "digest-2".to_owned(),
        };
        assert!(matches!(
            store.mark_serving(&lease, None, Some(malformed), 2),
            Err(ControlError::RecoveryPublicationConflict { .. })
        ));
        let record = store.get_shard(&shard_id).unwrap();
        assert_eq!(record.log, Some(durable));
        assert_eq!(record.durable_lsn, 1);

        let _ = store.release(&lease);
        cleanup_keys(&endpoints, &key_prefix);
    }

    fn cleanup_keys(endpoints: &[String], key_prefix: &str) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            if let Ok(mut client) = etcd_client::Client::connect(endpoints, None).await {
                let _ = client
                    .delete(
                        key_prefix.as_bytes().to_vec(),
                        Some(etcd_client::DeleteOptions::new().with_prefix()),
                    )
                    .await;
            }
        });
    }
}
