/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(feature = "fdb")]

use std::env;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use nokv_control::{
    CatalogEntryState, ControlError, DistributedControlStore, LogicalShardId, NodeId, RpcEndpoint,
    ShardRouteState, StoreId, StoreManifest, StoreProvider, SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_control_fdb::{FdbControlOptions, FdbControlStore};
use nokv_fdb::{
    lexicographic_successor, FdbConnectionOptions, FdbDatabase, FdbRuntime, FdbStorePrefix,
    FDB_PHYSICAL_ENCODING_VERSION,
};
use uuid::Uuid;

#[test]
#[ignore = "requires NOKV_TEST_FDB_CLUSTER_FILE, a compatible libfdb_c, and a disposable cluster"]
fn live_concurrent_contenders_and_takeover_fencing() {
    let runtime = FdbRuntime::start().expect("start shared FoundationDB runtime");
    let cluster_file = cluster_file();
    let prefix = format!("nokv-control-test-{}", Uuid::new_v4().simple());
    let options = FdbControlOptions::new(&cluster_file, &prefix)
        .unwrap()
        .with_lease_ttl(Duration::from_millis(1))
        .unwrap();
    let guard = PrefixGuard::fresh(&runtime, &cluster_file, prefix.as_bytes());
    let manifest = StoreManifest::new(
        StoreId::from_bytes(*Uuid::new_v4().as_bytes()),
        StoreProvider::FoundationDb,
        SUPPORTED_WORKSPACE_FORMAT_VERSION,
        FDB_PHYSICAL_ENCODING_VERSION,
        options.provider_namespace_digest(),
        "live-test",
    )
    .unwrap();
    FdbControlStore::format(&runtime, &options, manifest.clone()).unwrap();
    let store = FdbControlStore::open(&runtime, options.clone(), manifest).unwrap();
    let shard = LogicalShardId::from_bytes([7; 16]);
    let provisioning = match store.create_shard_catalog(shard).unwrap() {
        nokv_control::CreateOutcome::Created(entry)
        | nokv_control::CreateOutcome::Existing(entry) => entry,
    };
    store
        .compare_and_set_shard_catalog(
            &provisioning,
            provisioning.with_state(CatalogEntryState::Ready),
        )
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let mut contenders = Vec::new();
    for (node, endpoint) in [
        ("node-a", "node-a.example:7750"),
        ("node-b", "node-b.example:7750"),
    ] {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        contenders.push(thread::spawn(move || {
            barrier.wait();
            store.acquire_owner(
                &shard,
                NodeId::new(node).unwrap(),
                RpcEndpoint::new(endpoint).unwrap(),
            )
        }));
    }
    barrier.wait();
    let results = contenders
        .into_iter()
        .map(|contender| contender.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ControlError::TransactionConflict { .. })))
            .count(),
        1
    );
    let first_session = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        store.activate_route(&first_session).unwrap().state(),
        ShardRouteState::Serving
    );

    let successor = FdbControlStore::open(&runtime, options, store.manifest().clone()).unwrap();
    successor.observe_ownership(&shard).unwrap();
    thread::sleep(Duration::from_millis(2));
    let second_session = successor
        .acquire_owner(
            &shard,
            NodeId::new("node-c").unwrap(),
            RpcEndpoint::new("node-c.example:7750").unwrap(),
        )
        .unwrap();
    assert_eq!(second_session.owner_epoch().get(), 2);
    assert!(matches!(
        store.renew_owner(&first_session),
        Err(ControlError::NotOwner { .. })
    ));
    successor.release_owner(&second_session).unwrap();

    drop(successor);
    drop(store);
    drop(guard);
    drop(runtime);
}

fn cluster_file() -> PathBuf {
    let value = env::var_os("NOKV_TEST_FDB_CLUSTER_FILE")
        .expect("set NOKV_TEST_FDB_CLUSTER_FILE to an absolute FoundationDB cluster file");
    let path = PathBuf::from(value);
    assert!(path.is_absolute());
    path
}

struct PrefixGuard {
    database: FdbDatabase,
    begin: Vec<u8>,
    end: Vec<u8>,
}

impl PrefixGuard {
    fn fresh(runtime: &FdbRuntime, cluster_file: &Path, prefix: &[u8]) -> Self {
        let begin = FdbStorePrefix::new(prefix).unwrap().as_bytes().to_vec();
        let end = lexicographic_successor(&begin).unwrap();
        let guard = Self {
            database: FdbDatabase::open(runtime, &FdbConnectionOptions::new(cluster_file)).unwrap(),
            begin,
            end,
        };
        guard.clear();
        guard
    }

    fn clear(&self) {
        let transaction = self.database.transaction().unwrap();
        transaction.clear_range(&self.begin, &self.end);
        transaction.commit().unwrap();
    }
}

impl Drop for PrefixGuard {
    fn drop(&mut self) {
        self.clear();
    }
}
