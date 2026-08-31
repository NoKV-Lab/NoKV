/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;
use std::time::Instant;

use nokv_control::{
    plan_fail_closed, plan_heartbeat_renewal, plan_owner_acquisition, plan_owner_release,
    plan_route_activation, validate_root_catalog_transition, validate_shard_catalog_transition,
    CatalogEntryState, ControlError, CreateOutcome, DistributedControlStore, LogicalShardId,
    NodeId, OwnerHeartbeat, OwnerSession, OwnershipSnapshot, RootCatalogEntry, RootId, RpcEndpoint,
    ShardCatalogEntry, ShardRoute, ShardRouteState, StoreManifest,
};
use nokv_fdb::{
    FdbDatabase, FdbErrorDisposition, FdbOpenError, FdbOperationError, FdbRangeRequest, FdbRuntime,
    FdbTransaction,
};

use crate::codec::{
    decode_heartbeat, decode_manifest, decode_root_catalog, decode_route, decode_session,
    decode_shard_catalog, encode_heartbeat, encode_manifest, encode_root_catalog, encode_route,
    encode_session, encode_shard_catalog,
};
use crate::observer::OwnershipObserver;
use crate::{FdbControlKeys, FdbControlOptions};

const LIST_PAGE_ROWS: usize = 1_000;
const LIST_TARGET_BYTES: usize = 256 * 1024;

/// FoundationDB implementation of the distributed control-store contract.
/// Every mutation performs one raw commit attempt.
#[derive(Clone)]
pub struct FdbControlStore {
    database: FdbDatabase,
    keys: FdbControlKeys,
    options: FdbControlOptions,
    manifest: StoreManifest,
    observer: Arc<OwnershipObserver>,
    monotonic_origin: Instant,
}

impl FdbControlStore {
    pub fn format(
        runtime: &FdbRuntime,
        options: &FdbControlOptions,
        manifest: StoreManifest,
    ) -> Result<CreateOutcome<StoreManifest>, ControlError> {
        options.validate()?;
        options.validate_manifest_binding(&manifest)?;
        let database = FdbDatabase::open(runtime, options.connection()).map_err(map_open_error)?;
        let keys = FdbControlKeys::new(options.physical_prefix());
        let key = keys.manifest_key();
        let transaction = database.transaction().map_err(map_operation_error)?;
        if let Some(value) = transaction.get(&key, false).map_err(map_operation_error)? {
            let current = decode_manifest(&value)?;
            return if current == manifest {
                Ok(CreateOutcome::Existing(current))
            } else {
                Err(ControlError::StoreManifestMismatch {
                    expected: Box::new(manifest),
                    actual: Box::new(current),
                })
            };
        }
        transaction.set(&key, &encode_manifest(&manifest)?);
        match commit(transaction, "format the FoundationDB store") {
            Ok(()) => Ok(CreateOutcome::Created(manifest)),
            Err(error @ ControlError::TransactionConflict { .. })
            | Err(error @ ControlError::CommitOutcomeUnknown { .. }) => {
                reconcile_created_manifest(&database, &key, manifest, error)
            }
            Err(error) => Err(error),
        }
    }

    pub fn inspect_manifest(
        runtime: &FdbRuntime,
        options: &FdbControlOptions,
    ) -> Result<StoreManifest, ControlError> {
        options.validate()?;
        let database = FdbDatabase::open(runtime, options.connection()).map_err(map_open_error)?;
        let keys = FdbControlKeys::new(options.physical_prefix());
        let transaction = database.transaction().map_err(map_operation_error)?;
        let value = transaction
            .get(&keys.manifest_key(), true)
            .map_err(map_operation_error)?
            .ok_or(ControlError::StoreNotFormatted)?;
        let manifest = decode_manifest(&value)?;
        options.validate_manifest_binding(&manifest)?;
        Ok(manifest)
    }

    pub fn open(
        runtime: &FdbRuntime,
        options: FdbControlOptions,
        expected_manifest: StoreManifest,
    ) -> Result<Self, ControlError> {
        options.validate()?;
        options.validate_manifest_binding(&expected_manifest)?;
        let database = FdbDatabase::open(runtime, options.connection()).map_err(map_open_error)?;
        let keys = FdbControlKeys::new(options.physical_prefix());
        let transaction = database.transaction().map_err(map_operation_error)?;
        let value = transaction
            .get(&keys.manifest_key(), true)
            .map_err(map_operation_error)?
            .ok_or(ControlError::StoreNotFormatted)?;
        let actual = decode_manifest(&value)?;
        if actual != expected_manifest {
            return Err(ControlError::StoreManifestMismatch {
                expected: Box::new(expected_manifest),
                actual: Box::new(actual),
            });
        }
        Ok(Self {
            database,
            keys,
            options,
            manifest: actual,
            observer: Arc::new(OwnershipObserver::default()),
            monotonic_origin: Instant::now(),
        })
    }

    pub fn keys(&self) -> &FdbControlKeys {
        &self.keys
    }

    fn now(&self) -> std::time::Duration {
        self.monotonic_origin.elapsed()
    }

    fn transaction(&self) -> Result<FdbTransaction, ControlError> {
        self.database.transaction().map_err(map_operation_error)
    }

    fn read_ownership(
        &self,
        transaction: &FdbTransaction,
        logical_shard_id: &LogicalShardId,
        snapshot: bool,
    ) -> Result<OwnershipSnapshot, ControlError> {
        let route = read_route(transaction, &self.keys, logical_shard_id, snapshot)?
            .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
        let session = transaction
            .get(&self.keys.session_key(logical_shard_id), snapshot)
            .map_err(map_operation_error)?
            .map(|value| decode_session(&value))
            .transpose()?;
        let heartbeat = transaction
            .get(&self.keys.heartbeat_key(logical_shard_id), snapshot)
            .map_err(map_operation_error)?
            .map(|value| decode_heartbeat(&value))
            .transpose()?;
        OwnershipSnapshot::new(route, session, heartbeat)
    }

    fn commit_ownership(
        &self,
        transaction: FdbTransaction,
        update: &nokv_control::OwnershipUpdate,
        operation: &'static str,
        write_route: bool,
        write_session: bool,
        write_heartbeat: bool,
    ) -> Result<(), ControlError> {
        let shard = update.route().logical_shard_id();
        if write_route {
            transaction.set(&self.keys.route_key(&shard), &encode_route(update.route())?);
        }
        if write_session {
            match update.session() {
                Some(session) => {
                    transaction.set(&self.keys.session_key(&shard), &encode_session(session)?);
                }
                None => transaction.clear(&self.keys.session_key(&shard)),
            }
        }
        if write_heartbeat {
            transaction.set(
                &self.keys.heartbeat_key(&shard),
                &encode_heartbeat(update.heartbeat())?,
            );
        }
        commit(transaction, operation)?;
        self.observer.record(&update.snapshot()?, self.now());
        Ok(())
    }
}

impl DistributedControlStore for FdbControlStore {
    fn manifest(&self) -> &StoreManifest {
        &self.manifest
    }

    fn create_root_catalog(
        &self,
        entry: RootCatalogEntry,
    ) -> Result<CreateOutcome<RootCatalogEntry>, ControlError> {
        if entry.state() != CatalogEntryState::Provisioning {
            return Err(ControlError::InvalidCatalogTransition {
                record: "root catalog",
                reason: "create requires Provisioning state".to_owned(),
            });
        }
        let transaction = self.transaction()?;
        let key = self.keys.root_catalog_key(&entry.root_id());
        if let Some(value) = transaction.get(&key, false).map_err(map_operation_error)? {
            let current = decode_root_catalog(&value)?;
            if current.root_id() != entry.root_id() {
                return Err(ControlError::InvalidRecord(
                    "root catalog value names a different root".to_owned(),
                ));
            }
            return if validate_root_catalog_transition(&entry, &current).is_ok() {
                Ok(CreateOutcome::Existing(current))
            } else {
                Err(ControlError::RootCatalogAlreadyExists(entry.root_id()))
            };
        }
        let shard = read_shard_catalog(&transaction, &self.keys, &entry.logical_shard_id(), false)?
            .ok_or(ControlError::LogicalShardNotFound(entry.logical_shard_id()))?;
        if shard.state() == CatalogEntryState::Retired {
            return Err(ControlError::InvalidCatalogTransition {
                record: "root catalog",
                reason: "cannot place a root on a retired shard".to_owned(),
            });
        }
        transaction.set(&key, &encode_root_catalog(&entry));
        commit(transaction, "create a root catalog entry")?;
        Ok(CreateOutcome::Created(entry))
    }

    fn get_root_catalog(&self, root_id: &RootId) -> Result<Option<RootCatalogEntry>, ControlError> {
        let transaction = self.transaction()?;
        read_root_catalog(&transaction, &self.keys, root_id, true)
    }

    fn list_root_catalog(&self) -> Result<Vec<RootCatalogEntry>, ControlError> {
        let transaction = self.transaction()?;
        let (begin, end) = self.keys.root_catalog_range()?;
        scan_catalog(&transaction, begin, end, |key, value| {
            let entry = decode_root_catalog(value)?;
            if self.keys.root_catalog_key(&entry.root_id()) != key {
                return Err(ControlError::InvalidRecord(
                    "root catalog key does not match its encoded root id".to_owned(),
                ));
            }
            Ok(entry)
        })
    }

    fn compare_and_set_root_catalog(
        &self,
        expected: &RootCatalogEntry,
        next: RootCatalogEntry,
    ) -> Result<RootCatalogEntry, ControlError> {
        validate_root_catalog_transition(expected, &next)?;
        let transaction = self.transaction()?;
        let current = read_root_catalog(&transaction, &self.keys, &expected.root_id(), false)?;
        if current.as_ref() == Some(&next) {
            return Ok(next);
        }
        if current.as_ref() != Some(expected) {
            return Err(ControlError::RootCatalogCasConflict {
                expected: Box::new(*expected),
                actual: Box::new(current),
            });
        }
        transaction.set(
            &self.keys.root_catalog_key(&next.root_id()),
            &encode_root_catalog(&next),
        );
        commit(transaction, "change a root catalog entry")?;
        Ok(next)
    }

    fn create_shard_catalog(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<CreateOutcome<ShardCatalogEntry>, ControlError> {
        let entry = ShardCatalogEntry::new(logical_shard_id, CatalogEntryState::Provisioning);
        let initial_route = ShardRoute::unassigned(logical_shard_id);
        let transaction = self.transaction()?;
        let shard_key = self.keys.shard_catalog_key(&logical_shard_id);
        let route_key = self.keys.route_key(&logical_shard_id);
        let current_shard = transaction
            .get(&shard_key, false)
            .map_err(map_operation_error)?
            .map(|value| decode_shard_catalog(&value))
            .transpose()?;
        let current_route = transaction
            .get(&route_key, false)
            .map_err(map_operation_error)?
            .map(|value| decode_route(&value))
            .transpose()?;
        match (current_shard, current_route) {
            (Some(current), Some(route)) => {
                if current.logical_shard_id() != logical_shard_id
                    || route.logical_shard_id() != logical_shard_id
                {
                    return Err(ControlError::InvalidRecord(
                        "shard catalog or route value names a different shard".to_owned(),
                    ));
                }
                self.read_ownership(&transaction, &logical_shard_id, false)?;
                return Ok(CreateOutcome::Existing(current));
            }
            (Some(_), None) => {
                return Err(ControlError::InvalidRecord(
                    "shard catalog exists without its route".to_owned(),
                ));
            }
            (None, Some(_)) => {
                return Err(ControlError::InvalidRecord(
                    "shard route exists without its shard catalog".to_owned(),
                ));
            }
            (None, None) => {}
        }
        if transaction
            .get(&self.keys.session_key(&logical_shard_id), false)
            .map_err(map_operation_error)?
            .is_some()
            || transaction
                .get(&self.keys.heartbeat_key(&logical_shard_id), false)
                .map_err(map_operation_error)?
                .is_some()
        {
            return Err(ControlError::InvalidRecord(
                "owner state exists before its shard catalog".to_owned(),
            ));
        }
        transaction.set(&shard_key, &encode_shard_catalog(&entry));
        transaction.set(&route_key, &encode_route(&initial_route)?);
        commit(transaction, "create a logical shard catalog")?;
        Ok(CreateOutcome::Created(entry))
    }

    fn get_shard_catalog(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<ShardCatalogEntry>, ControlError> {
        let transaction = self.transaction()?;
        read_shard_catalog(&transaction, &self.keys, logical_shard_id, true)
    }

    fn list_shard_catalog(&self) -> Result<Vec<ShardCatalogEntry>, ControlError> {
        let transaction = self.transaction()?;
        let (begin, end) = self.keys.shard_catalog_range()?;
        scan_catalog(&transaction, begin, end, |key, value| {
            let entry = decode_shard_catalog(value)?;
            if self.keys.shard_catalog_key(&entry.logical_shard_id()) != key {
                return Err(ControlError::InvalidRecord(
                    "shard catalog key does not match its encoded shard id".to_owned(),
                ));
            }
            Ok(entry)
        })
    }

    fn compare_and_set_shard_catalog(
        &self,
        expected: &ShardCatalogEntry,
        next: ShardCatalogEntry,
    ) -> Result<ShardCatalogEntry, ControlError> {
        validate_shard_catalog_transition(expected, &next)?;
        let transaction = self.transaction()?;
        let current = read_shard_catalog(
            &transaction,
            &self.keys,
            &expected.logical_shard_id(),
            false,
        )?;
        let exact_replay = current.as_ref() == Some(&next);
        if !exact_replay && current.as_ref() != Some(expected) {
            return Err(ControlError::ShardCatalogCasConflict {
                expected: Box::new(*expected),
                actual: Box::new(current),
            });
        }
        if next.state() == CatalogEntryState::Retired {
            let ownership = self.read_ownership(&transaction, &next.logical_shard_id(), false)?;
            if ownership.route().state() != ShardRouteState::Unassigned
                || ownership.session().is_some()
            {
                return Err(ControlError::InvalidCatalogTransition {
                    record: "shard catalog",
                    reason: "retirement requires an unassigned route with no live session"
                        .to_owned(),
                });
            }
        }
        if exact_replay {
            return Ok(next);
        }
        transaction.set(
            &self.keys.shard_catalog_key(&next.logical_shard_id()),
            &encode_shard_catalog(&next),
        );
        commit(transaction, "change a logical shard catalog")?;
        Ok(next)
    }

    fn get_route(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<ShardRoute>, ControlError> {
        let transaction = self.transaction()?;
        let catalog = read_shard_catalog(&transaction, &self.keys, logical_shard_id, true)?;
        let route = read_route(&transaction, &self.keys, logical_shard_id, true)?;
        match (catalog, route) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(ControlError::InvalidRecord(
                "shard route exists without its shard catalog".to_owned(),
            )),
            (Some(_), None) => Err(ControlError::InvalidRecord(
                "shard catalog exists without its route".to_owned(),
            )),
            (Some(catalog), Some(_)) => {
                let ownership = self.read_ownership(&transaction, logical_shard_id, true)?;
                validate_catalog_ownership(&catalog, &ownership)?;
                Ok(Some(ownership.route().clone()))
            }
        }
    }

    fn observe_ownership(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<OwnershipSnapshot, ControlError> {
        let transaction = self.transaction()?;
        let catalog = read_shard_catalog(&transaction, &self.keys, logical_shard_id, true)?
            .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
        let ownership = self.read_ownership(&transaction, logical_shard_id, true)?;
        validate_catalog_ownership(&catalog, &ownership)?;
        self.observer.record(&ownership, self.now());
        Ok(ownership)
    }

    fn acquire_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: NodeId,
        endpoint: RpcEndpoint,
    ) -> Result<OwnerSession, ControlError> {
        let transaction = self.transaction()?;
        let shard = read_shard_catalog(&transaction, &self.keys, logical_shard_id, false)?
            .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
        if shard.state() != CatalogEntryState::Ready {
            return Err(ControlError::InvalidCatalogTransition {
                record: "shard catalog",
                reason: format!(
                    "owner acquisition requires Ready, actual {:?}",
                    shard.state()
                ),
            });
        }
        let current = self.read_ownership(&transaction, logical_shard_id, false)?;
        let immediate =
            current.route().state() == ShardRouteState::Unassigned && current.session().is_none();
        if !immediate {
            if let Some(remaining) =
                self.observer
                    .remaining(&current, self.now(), self.options.lease_ttl())
            {
                return Err(ControlError::OwnershipObservationPending {
                    logical_shard_id: *logical_shard_id,
                    remaining_millis: u64::try_from(remaining.as_millis())
                        .expect("validated ownership TTL fits u64 milliseconds"),
                });
            }
        }
        let update = plan_owner_acquisition(&current, owner, endpoint)?;
        let session = update
            .session()
            .expect("owner acquisition always creates a session")
            .clone();
        self.commit_ownership(
            transaction,
            &update,
            "acquire a logical shard owner",
            true,
            true,
            true,
        )?;
        Ok(session)
    }

    fn renew_owner(&self, session: &OwnerSession) -> Result<OwnerHeartbeat, ControlError> {
        let transaction = self.transaction()?;
        let current = self.read_ownership(&transaction, &session.logical_shard_id(), false)?;
        let update = plan_heartbeat_renewal(&current, session)?;
        let heartbeat = update.heartbeat().clone();
        self.commit_ownership(
            transaction,
            &update,
            "renew a logical shard owner",
            false,
            false,
            true,
        )?;
        Ok(heartbeat)
    }

    fn activate_route(&self, session: &OwnerSession) -> Result<ShardRoute, ControlError> {
        let transaction = self.transaction()?;
        let current = self.read_ownership(&transaction, &session.logical_shard_id(), false)?;
        let update = plan_route_activation(&current, session)?;
        let route = update.route().clone();
        self.commit_ownership(
            transaction,
            &update,
            "activate a logical shard route",
            true,
            false,
            false,
        )?;
        Ok(route)
    }

    fn fail_closed(&self, session: &OwnerSession) -> Result<ShardRoute, ControlError> {
        let transaction = self.transaction()?;
        let current = self.read_ownership(&transaction, &session.logical_shard_id(), false)?;
        let update = plan_fail_closed(&current, session)?;
        let route = update.route().clone();
        self.commit_ownership(
            transaction,
            &update,
            "fail closed a logical shard route",
            true,
            false,
            false,
        )?;
        Ok(route)
    }

    fn release_owner(&self, session: &OwnerSession) -> Result<ShardRoute, ControlError> {
        let transaction = self.transaction()?;
        let current = self.read_ownership(&transaction, &session.logical_shard_id(), false)?;
        let update = plan_owner_release(&current, session)?;
        let route = update.route().clone();
        self.commit_ownership(
            transaction,
            &update,
            "release a logical shard owner",
            true,
            true,
            true,
        )?;
        Ok(route)
    }
}

fn reconcile_created_manifest(
    database: &FdbDatabase,
    key: &[u8],
    expected: StoreManifest,
    unresolved: ControlError,
) -> Result<CreateOutcome<StoreManifest>, ControlError> {
    let transaction = database.transaction().map_err(map_operation_error)?;
    let value = transaction
        .get(key, true)
        .map_err(map_operation_error)?
        .ok_or(unresolved)?;
    let actual = decode_manifest(&value)?;
    if actual == expected {
        Ok(CreateOutcome::Existing(actual))
    } else {
        Err(ControlError::StoreManifestMismatch {
            expected: Box::new(expected),
            actual: Box::new(actual),
        })
    }
}

fn read_root_catalog(
    transaction: &FdbTransaction,
    keys: &FdbControlKeys,
    root_id: &RootId,
    snapshot: bool,
) -> Result<Option<RootCatalogEntry>, ControlError> {
    transaction
        .get(&keys.root_catalog_key(root_id), snapshot)
        .map_err(map_operation_error)?
        .map(|value| {
            let entry = decode_root_catalog(&value)?;
            if entry.root_id() != *root_id {
                return Err(ControlError::InvalidRecord(
                    "root catalog value names a different root".to_owned(),
                ));
            }
            Ok(entry)
        })
        .transpose()
}

fn read_shard_catalog(
    transaction: &FdbTransaction,
    keys: &FdbControlKeys,
    logical_shard_id: &LogicalShardId,
    snapshot: bool,
) -> Result<Option<ShardCatalogEntry>, ControlError> {
    transaction
        .get(&keys.shard_catalog_key(logical_shard_id), snapshot)
        .map_err(map_operation_error)?
        .map(|value| {
            let entry = decode_shard_catalog(&value)?;
            if entry.logical_shard_id() != *logical_shard_id {
                return Err(ControlError::InvalidRecord(
                    "shard catalog value names a different shard".to_owned(),
                ));
            }
            Ok(entry)
        })
        .transpose()
}

fn read_route(
    transaction: &FdbTransaction,
    keys: &FdbControlKeys,
    logical_shard_id: &LogicalShardId,
    snapshot: bool,
) -> Result<Option<ShardRoute>, ControlError> {
    transaction
        .get(&keys.route_key(logical_shard_id), snapshot)
        .map_err(map_operation_error)?
        .map(|value| {
            let route = decode_route(&value)?;
            if route.logical_shard_id() != *logical_shard_id {
                return Err(ControlError::InvalidRecord(
                    "route value names a different shard".to_owned(),
                ));
            }
            Ok(route)
        })
        .transpose()
}

fn validate_catalog_ownership(
    catalog: &ShardCatalogEntry,
    ownership: &OwnershipSnapshot,
) -> Result<(), ControlError> {
    if catalog.logical_shard_id() != ownership.route().logical_shard_id() {
        return Err(ControlError::InvalidRecord(
            "shard catalog and ownership state name different shards".to_owned(),
        ));
    }
    if ownership.route().state() != ShardRouteState::Unassigned
        && catalog.state() != CatalogEntryState::Ready
    {
        return Err(ControlError::OwnershipStateConflict {
            logical_shard_id: catalog.logical_shard_id(),
            reason: format!(
                "route is {:?} while shard catalog is {:?}",
                ownership.route().state(),
                catalog.state()
            ),
        });
    }
    Ok(())
}

fn scan_catalog<T>(
    transaction: &FdbTransaction,
    mut begin: Vec<u8>,
    end: Vec<u8>,
    mut decode: impl FnMut(Vec<u8>, &[u8]) -> Result<T, ControlError>,
) -> Result<Vec<T>, ControlError> {
    let mut entries = Vec::new();
    let mut iteration = 1;
    while begin < end {
        let page = transaction
            .get_range(&FdbRangeRequest {
                begin: begin.clone(),
                end: end.clone(),
                limit: Some(LIST_PAGE_ROWS),
                target_bytes: LIST_TARGET_BYTES,
                iteration,
                snapshot: true,
                reverse: false,
            })
            .map_err(map_operation_error)?;
        let Some(last) = page.items.last() else {
            break;
        };
        for item in &page.items {
            entries.push(decode(item.key.clone(), &item.value)?);
        }
        if !page.more {
            break;
        }
        begin = last.key.clone();
        begin.push(0);
        iteration = iteration.checked_add(1).ok_or_else(|| {
            ControlError::InvalidRecord("catalog scan iteration overflows usize".to_owned())
        })?;
    }
    Ok(entries)
}

fn commit(transaction: FdbTransaction, operation: &'static str) -> Result<(), ControlError> {
    transaction
        .commit()
        .map_err(|error| map_commit_error(operation, error))
}

fn map_open_error(error: FdbOpenError) -> ControlError {
    match error {
        FdbOpenError::Config(error) => ControlError::InvalidOptions(error.to_string()),
        FdbOpenError::Operation(error) => map_operation_error(error),
    }
}

fn map_operation_error(error: FdbOperationError) -> ControlError {
    ControlError::Backend(error.to_string())
}

fn map_commit_error(operation: &'static str, error: FdbOperationError) -> ControlError {
    match error.disposition() {
        FdbErrorDisposition::Conflict => ControlError::TransactionConflict { operation },
        FdbErrorDisposition::CommitUnknown => ControlError::CommitOutcomeUnknown {
            operation,
            reason: error.to_string(),
        },
        FdbErrorDisposition::Limit(_) | FdbErrorDisposition::Unavailable => {
            ControlError::Backend(error.to_string())
        }
    }
}
