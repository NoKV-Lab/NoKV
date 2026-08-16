use std::future::Future;

use etcd_client::{Client, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use tokio::runtime::{Builder, Runtime};

use crate::codec::{
    decode_logical_shard_record, decode_owner_session, decode_root_object_namespace_binding,
    decode_root_placement, encode_logical_shard_record, encode_owner_session,
    encode_root_object_namespace_binding, encode_root_placement,
};
use crate::store::{
    prepare_mark_serving, prepare_owner_acquisition, prepare_owner_release,
    prepare_recovery_reacquisition, prepare_recovery_suspension, validate_new_root_placement,
    validate_record_lease, validate_root_placement_update, ControlStore,
};
use crate::{
    ControlError, EtcdControlStoreOptions, LogicalShardId, LogicalShardLease, LogicalShardRecord,
    NodeId, OwnerEpoch, RecoveryPublication, RootId, RootObjectNamespaceBinding, RootPlacement,
    RootPlacementLifecycle,
};

pub struct EtcdControlStore {
    options: EtcdControlStoreOptions,
    runtime: Runtime,
    client: Client,
}

struct StoredRootPlacement {
    placement: RootPlacement,
    encoded: Vec<u8>,
}

struct StoredLogicalShard {
    record: LogicalShardRecord,
    encoded: Vec<u8>,
}

struct StoredOwnerSession {
    lease: LogicalShardLease,
    encoded: Vec<u8>,
    attached_lease_id: i64,
}

impl EtcdControlStore {
    pub fn connect(options: EtcdControlStoreOptions) -> Result<Self, ControlError> {
        options.validate()?;
        let runtime = Builder::new_multi_thread()
            .thread_name("nokv-control-etcd")
            .enable_all()
            .build()
            .map_err(|error| {
                ControlError::Backend(format!("etcd runtime initialization failed: {error}"))
            })?;
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
    fn create_root_object_namespace_binding(
        &self,
        binding: RootObjectNamespaceBinding,
    ) -> Result<RootObjectNamespaceBinding, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let key = options.root_object_namespace_key(&binding.root_id);
            let encoded = encode_root_object_namespace_binding(&binding)?;
            let txn = Txn::new()
                .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                .and_then(vec![TxnOp::put(key, encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(binding);
            }
            let current =
                fetch_root_object_namespace_binding(&mut client, &options, &binding.root_id)
                    .await?
                    .ok_or_else(|| {
                        ControlError::Backend(
                            "root object namespace binding disappeared after create conflict"
                                .to_owned(),
                        )
                    })?;
            if current == binding {
                Ok(current)
            } else {
                Err(ControlError::RootObjectNamespaceAlreadyBound {
                    root_id: binding.root_id,
                    existing: current.object_namespace_id,
                    requested: binding.object_namespace_id,
                })
            }
        })
    }

    fn get_root_object_namespace_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootObjectNamespaceBinding>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let root_id = *root_id;
        self.block_on(async move {
            fetch_root_object_namespace_binding(&mut client, &options, &root_id).await
        })
    }

    fn create_root_placement(
        &self,
        placement: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        validate_new_root_placement(&placement)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            if fetch_logical_shard(&mut client, &options, &placement.logical_shard_id)
                .await?
                .is_none()
            {
                return Err(ControlError::LogicalShardNotFound(
                    placement.logical_shard_id,
                ));
            }

            let key = options.root_placement_key(&placement.root_id);
            let encoded = encode_root_placement(&placement)?;
            let txn = Txn::new()
                .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                .and_then(vec![TxnOp::put(key, encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(placement);
            }

            let current = fetch_root_placement(&mut client, &options, &placement.root_id)
                .await?
                .ok_or(ControlError::RootPlacementNotFound(placement.root_id))?;
            if current.placement == placement {
                return Ok(current.placement);
            }
            if current.placement.logical_shard_id != placement.logical_shard_id {
                return Err(ControlError::ImmutableShardAffinity {
                    root_id: placement.root_id,
                    existing: current.placement.logical_shard_id,
                    requested: placement.logical_shard_id,
                });
            }
            Err(ControlError::RootPlacementAlreadyExists(placement.root_id))
        })
    }

    fn get_root_placement(&self, root_id: &RootId) -> Result<Option<RootPlacement>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let root_id = *root_id;
        self.block_on(async move {
            Ok(fetch_root_placement(&mut client, &options, &root_id)
                .await?
                .map(|stored| stored.placement))
        })
    }

    fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let mut placements = fetch_root_placements(&mut client, &options)
                .await?
                .into_iter()
                .map(|stored| stored.placement)
                .collect::<Vec<_>>();
            placements.sort_by_key(|placement| placement.root_id);
            Ok(placements)
        })
    }

    fn compare_and_set_root_placement(
        &self,
        expected: &RootPlacement,
        next: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        validate_root_placement_update(expected, &next)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        let expected = expected.clone();
        self.block_on(async move {
            let key = options.root_placement_key(&expected.root_id);
            let expected_encoded = encode_root_placement(&expected)?;
            let next_encoded = encode_root_placement(&next)?;
            let txn = Txn::new()
                .when(vec![Compare::value(
                    key.clone(),
                    CompareOp::Equal,
                    expected_encoded,
                )])
                .and_then(vec![TxnOp::put(key, next_encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(next);
            }

            let actual = fetch_root_placement(&mut client, &options, &expected.root_id)
                .await?
                .map(|stored| stored.placement);
            if actual.as_ref() == Some(&next) {
                return Ok(next);
            }
            Err(ControlError::RootPlacementCasConflict { expected, actual })
        })
    }

    fn create_logical_shard(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let desired = LogicalShardRecord::unassigned(logical_shard_id);
            let key = options.logical_shard_record_key(&logical_shard_id);
            let encoded = encode_logical_shard_record(&desired)?;
            let txn = Txn::new()
                .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                .and_then(vec![TxnOp::put(key, encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(desired);
            }

            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            if current.record == desired {
                Ok(current.record)
            } else {
                Err(ControlError::LogicalShardAlreadyExists(logical_shard_id))
            }
        })
    }

    fn get_logical_shard(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<LogicalShardRecord>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let logical_shard_id = *logical_shard_id;
        self.block_on(async move {
            Ok(
                fetch_logical_shard(&mut client, &options, &logical_shard_id)
                    .await?
                    .map(|stored| stored.record),
            )
        })
    }

    fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let mut records = fetch_logical_shards(&mut client, &options)
                .await?
                .into_iter()
                .map(|stored| stored.record)
                .collect::<Vec<_>>();
            records.sort_by_key(|record| record.logical_shard_id);
            Ok(records)
        })
    }

    fn acquire_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let logical_shard_id = *logical_shard_id;
        self.block_on(async move {
            let placement = find_write_placement(&mut client, &options, &logical_shard_id).await?;
            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            if let (Some(current_owner), Some(owner_epoch)) =
                (current.record.owner.clone(), current.record.owner_epoch)
            {
                return Err(ControlError::LogicalShardAlreadyOwned {
                    logical_shard_id,
                    owner: current_owner,
                    owner_epoch,
                });
            }
            if current.record.owner_epoch.is_some() {
                return Err(ControlError::StaleOwnerEpoch {
                    logical_shard_id,
                    expected: None,
                    actual: current.record.owner_epoch,
                });
            }
            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let (next, lease) =
                match prepare_owner_acquisition(&current.record, None, owner, endpoint, lease_id) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        revoke_lease_best_effort(&mut client, lease_id).await;
                        return Err(error);
                    }
                };
            install_owner(
                &mut client,
                &options,
                &current,
                &placement,
                &next,
                &lease,
                None,
            )
            .await
        })
    }

    fn acquire_successor(
        &self,
        logical_shard_id: &LogicalShardId,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let logical_shard_id = *logical_shard_id;
        self.block_on(async move {
            let placement = find_write_placement(&mut client, &options, &logical_shard_id).await?;
            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            if current.record.owner_epoch != Some(expected_owner_epoch) {
                return Err(ControlError::StaleOwnerEpoch {
                    logical_shard_id,
                    expected: Some(expected_owner_epoch),
                    actual: current.record.owner_epoch,
                });
            }
            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let (next, lease) = match prepare_owner_acquisition(
                &current.record,
                Some(expected_owner_epoch),
                owner,
                endpoint,
                lease_id,
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    revoke_lease_best_effort(&mut client, lease_id).await;
                    return Err(error);
                }
            };
            install_owner(
                &mut client,
                &options,
                &current,
                &placement,
                &next,
                &lease,
                Some(expected_owner_epoch),
            )
            .await
        })
    }

    fn reacquire_recovery(
        &self,
        logical_shard_id: &LogicalShardId,
        recovery_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let logical_shard_id = *logical_shard_id;
        self.block_on(async move {
            let placement = find_write_placement(&mut client, &options, &logical_shard_id).await?;
            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            if current.record.owner_epoch != Some(recovery_epoch) {
                return Err(ControlError::StaleOwnerEpoch {
                    logical_shard_id,
                    expected: Some(recovery_epoch),
                    actual: current.record.owner_epoch,
                });
            }
            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let (next, lease) = match prepare_recovery_reacquisition(
                &current.record,
                recovery_epoch,
                owner,
                endpoint,
                lease_id,
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    revoke_lease_best_effort(&mut client, lease_id).await;
                    return Err(error);
                }
            };
            install_owner(
                &mut client,
                &options,
                &current,
                &placement,
                &next,
                &lease,
                Some(recovery_epoch),
            )
            .await
        })
    }

    fn renew_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let current = fetch_logical_shard(&mut client, &options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            validate_record_lease(&current.record, &lease)?;
            validate_owner_session(&mut client, &options, &lease).await?;

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
                    ControlError::Backend(
                        "etcd lease keepalive stream ended before a response".to_owned(),
                    )
                })?;
            if response.id() != lease_id || response.ttl() <= 0 {
                return Err(ControlError::StaleLease(lease));
            }
            Ok(current.record)
        })
    }

    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let current = fetch_logical_shard(&mut client, &options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            let session = validate_owner_session(&mut client, &options, &lease).await?;
            let next = prepare_mark_serving(&current.record, &lease, publication)?;
            let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
            let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::value(record_key.clone(), CompareOp::Equal, current.encoded),
                    Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
                    Compare::lease(session_key, CompareOp::Equal, lease_id_i64(lease.lease_id)?),
                ])
                .and_then(vec![TxnOp::put(
                    record_key,
                    encode_logical_shard_record(&next)?,
                    None,
                )]);
            match client.txn(txn).await {
                Ok(response) if response.succeeded() => Ok(next),
                Ok(_) | Err(_) => {
                    classify_mark_serving_failure(&mut client, &options, &lease, &next).await
                }
            }
        })
    }

    fn suspend_recovery(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let current = fetch_logical_shard(&mut client, &options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            let session = validate_owner_session(&mut client, &options, &lease).await?;
            let retained = prepare_recovery_suspension(&current.record, &lease)?;
            let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
            let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::value(record_key, CompareOp::Equal, current.encoded),
                    Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
                    Compare::lease(
                        session_key.clone(),
                        CompareOp::Equal,
                        lease_id_i64(lease.lease_id)?,
                    ),
                ])
                .and_then(vec![TxnOp::delete(session_key, None)]);
            let result = match client.txn(txn).await {
                Ok(response) if response.succeeded() => Ok(retained),
                Ok(_) | Err(_) => {
                    classify_suspend_recovery_failure(&mut client, &options, &lease, &retained)
                        .await
                }
            };
            if result.is_ok() {
                revoke_lease_best_effort(&mut client, lease.lease_id).await;
            }
            result
        })
    }

    fn release_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let current = fetch_logical_shard(&mut client, &options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            let session = validate_owner_session(&mut client, &options, &lease).await?;
            let next = prepare_owner_release(&current.record, &lease)?;
            let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
            let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::value(record_key.clone(), CompareOp::Equal, current.encoded),
                    Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
                    Compare::lease(
                        session_key.clone(),
                        CompareOp::Equal,
                        lease_id_i64(lease.lease_id)?,
                    ),
                ])
                .and_then(vec![
                    TxnOp::put(record_key, encode_logical_shard_record(&next)?, None),
                    TxnOp::delete(session_key, None),
                ]);
            match client.txn(txn).await {
                Ok(response) if response.succeeded() => {
                    revoke_lease_best_effort(&mut client, lease.lease_id).await;
                    Ok(next)
                }
                Ok(_) | Err(_) => {
                    let result =
                        classify_release_failure(&mut client, &options, &lease, &next).await;
                    if result.is_ok() {
                        revoke_lease_best_effort(&mut client, lease.lease_id).await;
                    }
                    result
                }
            }
        })
    }
}

async fn fetch_root_object_namespace_binding(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    root_id: &RootId,
) -> Result<Option<RootObjectNamespaceBinding>, ControlError> {
    let key = options.root_object_namespace_key(root_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let binding = decode_root_object_namespace_binding(kv.value())?;
    if binding.root_id != *root_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "root object namespace key and value identities differ".to_owned(),
        ));
    }
    Ok(Some(binding))
}

async fn fetch_root_placement(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    root_id: &RootId,
) -> Result<Option<StoredRootPlacement>, ControlError> {
    let key = options.root_placement_key(root_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let placement = decode_root_placement(kv.value())?;
    if placement.root_id != *root_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "root placement key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_root_placement(&placement)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "root placement value is not canonical".to_owned(),
        ));
    }
    Ok(Some(StoredRootPlacement {
        placement,
        encoded: canonical,
    }))
}

async fn fetch_root_placements(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
) -> Result<Vec<StoredRootPlacement>, ControlError> {
    let response = client
        .get(
            options.root_placements_prefix(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(etcd_backend)?;
    let mut placements = Vec::with_capacity(response.kvs().len());
    for kv in response.kvs() {
        let placement = decode_root_placement(kv.value())?;
        if kv.key() != options.root_placement_key(&placement.root_id) {
            return Err(ControlError::Codec(
                "root placement key and value identities differ".to_owned(),
            ));
        }
        let canonical = encode_root_placement(&placement)?;
        if canonical != kv.value() {
            return Err(ControlError::Codec(
                "root placement value is not canonical".to_owned(),
            ));
        }
        placements.push(StoredRootPlacement {
            placement,
            encoded: canonical,
        });
    }
    placements.sort_by_key(|stored| stored.placement.root_id);
    Ok(placements)
}

async fn fetch_logical_shard(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Result<Option<StoredLogicalShard>, ControlError> {
    let key = options.logical_shard_record_key(logical_shard_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let record = decode_logical_shard_record(kv.value())?;
    if record.logical_shard_id != *logical_shard_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "logical shard key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_logical_shard_record(&record)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "logical shard value is not canonical".to_owned(),
        ));
    }
    Ok(Some(StoredLogicalShard {
        record,
        encoded: canonical,
    }))
}

async fn fetch_logical_shards(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
) -> Result<Vec<StoredLogicalShard>, ControlError> {
    let response = client
        .get(
            options.logical_shard_records_prefix(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(etcd_backend)?;
    let mut records = Vec::with_capacity(response.kvs().len());
    for kv in response.kvs() {
        let record = decode_logical_shard_record(kv.value())?;
        if kv.key() != options.logical_shard_record_key(&record.logical_shard_id) {
            return Err(ControlError::Codec(
                "logical shard key and value identities differ".to_owned(),
            ));
        }
        let canonical = encode_logical_shard_record(&record)?;
        if canonical != kv.value() {
            return Err(ControlError::Codec(
                "logical shard value is not canonical".to_owned(),
            ));
        }
        records.push(StoredLogicalShard {
            record,
            encoded: canonical,
        });
    }
    records.sort_by_key(|stored| stored.record.logical_shard_id);
    Ok(records)
}

async fn find_write_placement(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Result<StoredRootPlacement, ControlError> {
    fetch_root_placements(client, options)
        .await?
        .into_iter()
        .find(|stored| {
            stored.placement.logical_shard_id == *logical_shard_id
                && stored.placement.lifecycle != RootPlacementLifecycle::Retired
        })
        .ok_or(ControlError::RootPlacementRequired(*logical_shard_id))
}

async fn fetch_owner_session(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Result<Option<StoredOwnerSession>, ControlError> {
    let key = options.logical_shard_session_key(logical_shard_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let lease = decode_owner_session(kv.value())?;
    if lease.logical_shard_id != *logical_shard_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "owner session key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_owner_session(&lease)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "owner session value is not canonical".to_owned(),
        ));
    }
    let attached_lease_id = kv.lease();
    if attached_lease_id == 0 || attached_lease_id != lease_id_i64(lease.lease_id)? {
        return Err(ControlError::Codec(
            "owner session value and attached lease differ".to_owned(),
        ));
    }
    Ok(Some(StoredOwnerSession {
        lease,
        encoded: canonical,
        attached_lease_id,
    }))
}

async fn validate_owner_session(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
) -> Result<StoredOwnerSession, ControlError> {
    let session = fetch_owner_session(client, options, &lease.logical_shard_id)
        .await?
        .ok_or_else(|| ControlError::StaleLease(lease.clone()))?;
    if session.lease != *lease || session.attached_lease_id != lease_id_i64(lease.lease_id)? {
        return Err(ControlError::StaleLease(lease.clone()));
    }
    Ok(session)
}

async fn install_owner(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    current: &StoredLogicalShard,
    placement: &StoredRootPlacement,
    next: &LogicalShardRecord,
    lease: &LogicalShardLease,
    expected_owner_epoch: Option<OwnerEpoch>,
) -> Result<LogicalShardLease, ControlError> {
    let record_encoded = match encode_logical_shard_record(next) {
        Ok(encoded) => encoded,
        Err(error) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            return Err(error);
        }
    };
    let session_encoded = match encode_owner_session(lease) {
        Ok(encoded) => encoded,
        Err(error) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            return Err(error);
        }
    };
    let attached_lease_id = match lease_id_i64(lease.lease_id) {
        Ok(lease_id) => lease_id,
        Err(error) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            return Err(error);
        }
    };
    let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
    let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
    let txn = Txn::new()
        .when(vec![
            Compare::value(
                record_key.clone(),
                CompareOp::Equal,
                current.encoded.clone(),
            ),
            Compare::create_revision(session_key.clone(), CompareOp::Equal, 0),
            Compare::value(
                options.root_placement_key(&placement.placement.root_id),
                CompareOp::Equal,
                placement.encoded.clone(),
            ),
        ])
        .and_then(vec![
            TxnOp::put(record_key, record_encoded, None),
            TxnOp::put(
                session_key,
                session_encoded,
                Some(PutOptions::new().with_lease(attached_lease_id)),
            ),
        ]);

    match client.txn(txn).await {
        Ok(response) if response.succeeded() => Ok(lease.clone()),
        Ok(_) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            classify_acquire_failure(
                client,
                options,
                &lease.logical_shard_id,
                expected_owner_epoch,
                placement,
            )
            .await
        }
        Err(error) => {
            let backend_error = etcd_backend(error);
            match owner_install_is_visible(client, options, next, lease).await {
                Ok(true) => Ok(lease.clone()),
                Ok(false) => {
                    revoke_lease_best_effort(client, lease.lease_id).await;
                    Err(backend_error)
                }
                // The commit outcome is unknown. Do not revoke a lease that may
                // guard a committed owner; without keepalive it expires safely.
                Err(_) => Err(backend_error),
            }
        }
    }
}

async fn owner_install_is_visible(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    expected_record: &LogicalShardRecord,
    expected_lease: &LogicalShardLease,
) -> Result<bool, ControlError> {
    let record = fetch_logical_shard(client, options, &expected_lease.logical_shard_id).await?;
    if record.as_ref().map(|stored| &stored.record) != Some(expected_record) {
        return Ok(false);
    }
    let attached_lease_id = lease_id_i64(expected_lease.lease_id)?;
    let session = fetch_owner_session(client, options, &expected_lease.logical_shard_id).await?;
    Ok(session.is_some_and(|session| {
        session.lease == *expected_lease && session.attached_lease_id == attached_lease_id
    }))
}

async fn classify_acquire_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
    expected_owner_epoch: Option<OwnerEpoch>,
    placement: &StoredRootPlacement,
) -> Result<LogicalShardLease, ControlError> {
    let latest = fetch_logical_shard(client, options, logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
    if latest.record.owner_epoch != expected_owner_epoch {
        if expected_owner_epoch.is_none() {
            if let (Some(owner), Some(owner_epoch)) =
                (latest.record.owner.clone(), latest.record.owner_epoch)
            {
                return Err(ControlError::LogicalShardAlreadyOwned {
                    logical_shard_id: *logical_shard_id,
                    owner,
                    owner_epoch,
                });
            }
        }
        return Err(ControlError::StaleOwnerEpoch {
            logical_shard_id: *logical_shard_id,
            expected: expected_owner_epoch,
            actual: latest.record.owner_epoch,
        });
    }
    if let Some(session) = fetch_owner_session(client, options, logical_shard_id).await? {
        return Err(ControlError::PreviousOwnerSessionLive {
            logical_shard_id: *logical_shard_id,
            owner_epoch: session.lease.owner_epoch,
        });
    }
    let actual_placement = fetch_root_placement(client, options, &placement.placement.root_id)
        .await?
        .map(|stored| stored.placement);
    if actual_placement.as_ref() != Some(&placement.placement) {
        return Err(ControlError::RootPlacementCasConflict {
            expected: placement.placement.clone(),
            actual: actual_placement,
        });
    }
    Err(ControlError::Backend(
        "owner acquisition CAS failed without an observable competing update".to_owned(),
    ))
}

async fn classify_mark_serving_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
    completed: &LogicalShardRecord,
) -> Result<LogicalShardRecord, ControlError> {
    let latest = fetch_logical_shard(client, options, &lease.logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
    if latest.record == *completed {
        validate_owner_session(client, options, lease).await?;
        return Ok(completed.clone());
    }
    validate_record_lease(&latest.record, lease)?;
    validate_owner_session(client, options, lease).await?;
    Err(ControlError::Backend(
        "owner mutation CAS failed while the exact owner session remained current".to_owned(),
    ))
}

async fn classify_suspend_recovery_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
    retained: &LogicalShardRecord,
) -> Result<LogicalShardRecord, ControlError> {
    let latest = fetch_logical_shard(client, options, &lease.logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
    if latest.record == *retained
        && fetch_owner_session(client, options, &lease.logical_shard_id)
            .await?
            .is_none()
    {
        return Ok(retained.clone());
    }
    validate_record_lease(&latest.record, lease)?;
    validate_owner_session(client, options, lease).await?;
    Err(ControlError::Backend(
        "recovery suspension CAS failed while the exact owner session remained current".to_owned(),
    ))
}

async fn classify_release_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
    completed: &LogicalShardRecord,
) -> Result<LogicalShardRecord, ControlError> {
    let latest = fetch_logical_shard(client, options, &lease.logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
    if latest.record == *completed {
        if fetch_owner_session(client, options, &lease.logical_shard_id)
            .await?
            .is_none()
        {
            return Ok(completed.clone());
        }
        return Err(ControlError::Backend(
            "released owner record still has an attached session".to_owned(),
        ));
    }
    validate_record_lease(&latest.record, lease)?;
    validate_owner_session(client, options, lease).await?;
    Err(ControlError::Backend(
        "owner release CAS failed while the exact owner session remained current".to_owned(),
    ))
}

async fn grant_lease(client: &mut Client, ttl_seconds: i64) -> Result<u64, ControlError> {
    let response = client
        .lease_grant(ttl_seconds, None)
        .await
        .map_err(etcd_backend)?;
    let lease_id = u64::try_from(response.id()).map_err(|_| {
        ControlError::Backend(format!("etcd returned negative lease id {}", response.id()))
    })?;
    if lease_id == 0 {
        return Err(ControlError::Backend(
            "etcd returned lease id zero".to_owned(),
        ));
    }
    Ok(lease_id)
}

async fn revoke_lease_best_effort(client: &mut Client, lease_id: u64) {
    if let Ok(lease_id) = lease_id_i64(lease_id) {
        let _ = client.lease_revoke(lease_id).await;
    }
}

fn lease_id_i64(lease_id: u64) -> Result<i64, ControlError> {
    if lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "lease id must be non-zero".to_owned(),
        ));
    }
    i64::try_from(lease_id)
        .map_err(|_| ControlError::Codec(format!("etcd lease id {lease_id} exceeds i64")))
}

fn etcd_backend(error: etcd_client::Error) -> ControlError {
    ControlError::Backend(format!("etcd: {error}"))
}
