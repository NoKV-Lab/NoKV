/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(feature = "fdb")]

use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nokv_control::{
    CatalogEntryState, DistributedControlStore, ShardRouteState, StoreId, StoreManifest,
    StoreProvider, SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_control_fdb::{FdbControlOptions, FdbControlStore};
use nokv_fdb::{
    lexicographic_successor, FdbConnectionOptions, FdbDatabase, FdbRuntime, FdbStorePrefix,
    FDB_PHYSICAL_ENCODING_VERSION,
};
use nokv_server::{prepare_fdb_provision, MetadataUrl};
use nokv_types::{AgentId, RootId};

#[test]
#[ignore = "requires NOKV_TEST_FDB_CLUSTER_FILE, a compatible libfdb_c, and a disposable cluster"]
fn live_prepare_releases_ownership_and_recovers_catalog_crash_cuts() {
    let runtime = FdbRuntime::start().expect("start shared FoundationDB runtime");
    let cluster_file = cluster_file();
    let prefix = unique_prefix();
    let guard = PrefixGuard::fresh(&runtime, &cluster_file, prefix.as_bytes());
    let options = FdbControlOptions::new(&cluster_file, &prefix)
        .unwrap()
        .with_lease_ttl(Duration::from_secs(10))
        .unwrap();
    let manifest = StoreManifest::new(
        StoreId::from_bytes([0x41; 16]),
        StoreProvider::FoundationDb,
        SUPPORTED_WORKSPACE_FORMAT_VERSION,
        FDB_PHYSICAL_ENCODING_VERSION,
        options.provider_namespace_digest(),
        "live-provision-test",
    )
    .unwrap();
    FdbControlStore::format(&runtime, &options, manifest.clone()).unwrap();
    let control = FdbControlStore::open(&runtime, options, manifest).unwrap();
    let metadata: MetadataUrl = format!("fdb://{}?prefix={prefix}", cluster_file.display())
        .parse()
        .unwrap();
    let url = metadata.as_foundationdb().unwrap();

    let first_root_id = RootId::from_bytes([0x51; 16]);
    let first = prepare_fdb_provision(url, first_root_id, AgentId::from_bytes([0x61; 16]))
        .expect("prepare first root");
    assert!(!first.preexisting());
    assert_eq!(first.root().state(), CatalogEntryState::Provisioning);
    assert_eq!(first.shard().state(), CatalogEntryState::Provisioning);
    assert_released(&control, first.shard().logical_shard_id());

    let first_catalog = control.get_root_catalog(&first_root_id).unwrap().unwrap();
    control
        .compare_and_set_root_catalog(
            &first_catalog,
            first_catalog.with_state(CatalogEntryState::Ready),
        )
        .expect("simulate a crash after root Ready and before shard Ready");
    let first_outcome = first
        .finalize_after_namespace_admission()
        .expect("recover the partial root/shard Ready transition");
    assert_eq!(first_outcome.root.state(), CatalogEntryState::Ready);
    assert_eq!(first_outcome.shard.state(), CatalogEntryState::Ready);
    assert_released(&control, first_outcome.shard.logical_shard_id());

    let second_root_id = RootId::from_bytes([0x52; 16]);
    let abandoned = prepare_fdb_provision(url, second_root_id, AgentId::from_bytes([0x62; 16]))
        .expect("prepare a new root on the Ready shared shard");
    assert!(!abandoned.preexisting());
    assert_eq!(abandoned.root().state(), CatalogEntryState::Provisioning);
    assert_eq!(abandoned.shard().state(), CatalogEntryState::Ready);
    assert_released(&control, abandoned.shard().logical_shard_id());
    drop(abandoned);

    let resumed = prepare_fdb_provision(url, second_root_id, AgentId::from_bytes([0x62; 16]))
        .expect("resume after namespace admission was abandoned");
    assert!(resumed.preexisting());
    assert_eq!(resumed.root().state(), CatalogEntryState::Provisioning);
    assert_eq!(resumed.shard().state(), CatalogEntryState::Ready);
    assert_released(&control, resumed.shard().logical_shard_id());
    let second_outcome = resumed
        .finalize_after_namespace_admission()
        .expect("finalize the resumed root");
    assert_eq!(second_outcome.root.state(), CatalogEntryState::Ready);
    assert_eq!(second_outcome.shard.state(), CatalogEntryState::Ready);
    assert_released(&control, second_outcome.shard.logical_shard_id());

    let ready = prepare_fdb_provision(url, second_root_id, AgentId::from_bytes([0x62; 16]))
        .expect("reopen an already Ready root");
    assert!(ready.preexisting());
    assert_released(&control, ready.shard().logical_shard_id());
    let replay = ready
        .finalize_after_namespace_admission()
        .expect("Ready finalization is idempotent");
    assert_eq!(replay, second_outcome);

    drop(ready);
    drop(resumed);
    drop(first);
    drop(control);
    drop(guard);
    drop(runtime);
}

fn assert_released(control: &FdbControlStore, shard: nokv_types::LogicalShardId) {
    let ownership = control.observe_ownership(&shard).unwrap();
    assert!(ownership.session().is_none());
    assert_eq!(ownership.route().state(), ShardRouteState::Unassigned);
}

fn cluster_file() -> PathBuf {
    let value = env::var_os("NOKV_TEST_FDB_CLUSTER_FILE")
        .expect("set NOKV_TEST_FDB_CLUSTER_FILE to an absolute FoundationDB cluster file");
    let path = PathBuf::from(value);
    assert!(path.is_absolute());
    path
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("nokv-server-provision-{}-{nanos:x}", std::process::id())
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
