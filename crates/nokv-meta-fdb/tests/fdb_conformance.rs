/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(feature = "fdb")]

use std::env;
use std::path::{Path, PathBuf};

use nokv_fdb::{
    lexicographic_successor, FdbConnectionOptions, FdbDatabase, FdbStorePrefix, FdbTransaction,
};
use nokv_meta_fdb::{FdbMetadataSessionFence, FdbOptions, FdbRuntime, FdbStore};
use nokv_meta_store::conformance;
use nokv_meta_store::{
    Commit, Key, Keyspace, Mutation, ReadBatch, ReadOp, ReadResult, Scan, ScanItem, StoreError,
    TxnStore, WriteTxn,
};
use uuid::Uuid;

#[allow(dead_code)]
#[path = "../src/codec.rs"]
mod production_codec;

use production_codec::KeyCodec;

const LIVE: Keyspace = Keyspace::new(0x7e01);

#[test]
#[ignore = "requires NOKV_TEST_FDB_CLUSTER_FILE, a compatible libfdb_c, and a disposable cluster"]
fn fdb_live_conformance_isolation_and_session_fencing() {
    let runtime = FdbRuntime::start().expect("start process-global FoundationDB runtime");
    let cluster_file = cluster_file();
    let first_namespace = unique_namespace();
    let second_namespace = unique_namespace();
    let first_guard = NamespaceGuard::fresh(&runtime, &cluster_file, &first_namespace);
    let second_guard = NamespaceGuard::fresh(&runtime, &cluster_file, &second_namespace);
    let first_authority =
        LiveAuthority::provision(&runtime, &cluster_file, &first_namespace, [0x71; 16]);
    let second_authority =
        LiveAuthority::provision(&runtime, &cluster_file, &second_namespace, [0x72; 16]);

    let first_options = FdbOptions::new(
        &cluster_file,
        first_namespace.clone(),
        first_authority.metadata_fence(),
    );
    let reopen_options = first_options.clone();
    let first_runtime = runtime.clone();
    let reopen_runtime = runtime.clone();
    conformance::run(
        move || FdbStore::open(&first_runtime, first_options),
        move || FdbStore::open(&reopen_runtime, reopen_options),
    );

    check_binary_scan_and_delimiter(
        &runtime,
        &cluster_file,
        &first_namespace,
        &first_authority.metadata_fence(),
    );
    check_namespace_isolation(
        &runtime,
        &cluster_file,
        &first_namespace,
        &first_authority.metadata_fence(),
        &second_namespace,
        &second_authority.metadata_fence(),
    );
    check_real_foundationdb_conflict(&first_guard, &first_namespace);
    check_heartbeat_and_takeover_fencing(
        &runtime,
        &cluster_file,
        &first_namespace,
        &first_authority,
    );

    drop(second_authority);
    drop(first_authority);
    drop(second_guard);
    drop(first_guard);
    drop(runtime);
}

fn cluster_file() -> PathBuf {
    let value = env::var_os("NOKV_TEST_FDB_CLUSTER_FILE")
        .expect("set NOKV_TEST_FDB_CLUSTER_FILE to an absolute FoundationDB cluster file");
    let path = PathBuf::from(value);
    assert!(
        path.is_absolute(),
        "NOKV_TEST_FDB_CLUSTER_FILE must be absolute"
    );
    path
}

fn unique_namespace() -> Vec<u8> {
    let mut namespace = env::var("NOKV_TEST_FDB_NAMESPACE")
        .unwrap_or_else(|_| "nokv-test".to_owned())
        .into_bytes();
    assert!(
        namespace.len() <= 31,
        "NOKV_TEST_FDB_NAMESPACE must contain at most 31 UTF-8 bytes"
    );
    namespace.push(b'/');
    namespace.extend_from_slice(Uuid::new_v4().simple().to_string().as_bytes());
    namespace
}

fn open_store(
    runtime: &FdbRuntime,
    cluster_file: &Path,
    namespace: &[u8],
    session_fence: &FdbMetadataSessionFence,
) -> FdbStore {
    FdbStore::open(
        runtime,
        FdbOptions::new(cluster_file, namespace.to_vec(), session_fence.clone()),
    )
    .unwrap_or_else(|error| panic!("open live FdbStore: {error}"))
}

fn check_binary_scan_and_delimiter(
    runtime: &FdbRuntime,
    cluster_file: &Path,
    namespace: &[u8],
    session_fence: &FdbMetadataSessionFence,
) {
    let store = open_store(runtime, cluster_file, namespace, session_fence);
    for (bytes, value) in [
        (b"binary/\x00".as_slice(), b"zero".as_slice()),
        (b"binary/a/\x00".as_slice(), b"nested-zero".as_slice()),
        (b"binary/a/\xff".as_slice(), b"nested-max".as_slice()),
        (b"binary/\xff".as_slice(), b"max".as_slice()),
    ] {
        assert_eq!(
            store
                .commit(WriteTxn {
                    checks: vec![],
                    mutations: vec![Mutation::Put {
                        key: Key::new(LIVE, bytes),
                        value: value.to_vec(),
                    }],
                })
                .unwrap_or_else(|error| panic!("write binary scan row: {error}")),
            Commit::Applied
        );
    }

    let ordered = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: LIVE,
                prefix: b"binary/".to_vec(),
                after: None,
                limit: 8,
                max_bytes: store.profile().limits.max_result_bytes,
                delimiter: None,
            })],
        })
        .expect("scan ordered binary rows");
    let ReadResult::Scan(ordered) = &ordered.results[0] else {
        panic!("binary scan returned a point result");
    };
    assert!(!ordered.more);
    assert_eq!(
        ordered.items.iter().map(ScanItem::key).collect::<Vec<_>>(),
        vec![
            b"binary/\x00".as_slice(),
            b"binary/a/\x00".as_slice(),
            b"binary/a/\xff".as_slice(),
            b"binary/\xff".as_slice(),
        ]
    );

    let first_page = delimiter_page(&store, None, 1);
    assert!(first_page.more);
    assert_eq!(
        first_page.items,
        vec![ScanItem::Row {
            key: b"binary/\x00".to_vec(),
            value: b"zero".to_vec(),
        }]
    );
    let second_page = delimiter_page(&store, Some(first_page.items[0].key().to_vec()), 1);
    assert!(second_page.more);
    assert_eq!(
        second_page.items,
        vec![ScanItem::CommonPrefix(b"binary/a/".to_vec())]
    );
    let third_page = delimiter_page(&store, Some(second_page.items[0].key().to_vec()), 2);
    assert!(!third_page.more);
    assert_eq!(
        third_page.items,
        vec![ScanItem::Row {
            key: b"binary/\xff".to_vec(),
            value: b"max".to_vec(),
        }]
    );
}

fn delimiter_page(
    store: &FdbStore,
    after: Option<Vec<u8>>,
    limit: usize,
) -> nokv_meta_store::ScanPage {
    let snapshot = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: LIVE,
                prefix: b"binary/".to_vec(),
                after,
                limit,
                max_bytes: store.profile().limits.max_result_bytes,
                delimiter: Some(b'/'),
            })],
        })
        .expect("scan delimiter page");
    match snapshot.results.into_iter().next().unwrap() {
        ReadResult::Scan(page) => page,
        ReadResult::Get(_) => panic!("delimiter scan returned a point result"),
    }
}

fn check_namespace_isolation(
    runtime: &FdbRuntime,
    cluster_file: &Path,
    first_namespace: &[u8],
    first_session_fence: &FdbMetadataSessionFence,
    second_namespace: &[u8],
    second_session_fence: &FdbMetadataSessionFence,
) {
    let first = open_store(runtime, cluster_file, first_namespace, first_session_fence);
    let second = open_store(
        runtime,
        cluster_file,
        second_namespace,
        second_session_fence,
    );
    let key = Key::new(LIVE, b"namespace-isolation");
    assert_eq!(
        first
            .commit(WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: key.clone(),
                    value: b"first".to_vec(),
                }],
            })
            .expect("write first namespace"),
        Commit::Applied
    );
    let snapshot = second
        .read(ReadBatch {
            ops: vec![ReadOp::Get(key)],
        })
        .expect("read second namespace");
    assert_eq!(snapshot.results, vec![ReadResult::Get(None)]);
}

fn check_heartbeat_and_takeover_fencing(
    runtime: &FdbRuntime,
    cluster_file: &Path,
    namespace: &[u8],
    authority: &LiveAuthority,
) {
    let stale_fence = authority.metadata_fence();
    let stale_store = open_store(runtime, cluster_file, namespace, &stale_fence);
    let baseline = Key::new(LIVE, b"session-fence-baseline");
    assert_eq!(
        stale_store
            .commit(WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: baseline.clone(),
                    value: b"visible".to_vec(),
                }],
            })
            .expect("write before heartbeat renewal"),
        Commit::Applied
    );

    let heartbeat_overlap = authority.transaction();
    assert_eq!(
        heartbeat_overlap
            .get(&authority.session_key, false)
            .expect("read exact session before overlapping heartbeat"),
        Some(authority.session_value.clone())
    );
    heartbeat_overlap.set(
        &KeyCodec::new(namespace)
            .unwrap()
            .encode(LIVE, b"heartbeat-overlap"),
        b"no-conflict",
    );
    authority.renew_heartbeat();
    heartbeat_overlap
        .commit()
        .expect("heartbeat key must not conflict with a session-fenced metadata transaction");
    assert_eq!(
        stale_store
            .commit(WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(LIVE, b"after-heartbeat"),
                    value: b"still-owned".to_vec(),
                }],
            })
            .expect("heartbeat renewal must not conflict with metadata"),
        Commit::Applied
    );

    let successor_fence = authority.install_successor_session();

    let expected_fenced = StoreError::Fenced {
        expected_owner_epoch: authority.owner_epoch,
        expected_session_generation: authority.session_generation,
    };
    let stale_open = match FdbStore::open(
        runtime,
        FdbOptions::new(cluster_file, namespace.to_vec(), stale_fence.clone()),
    ) {
        Ok(_) => panic!("opening with the replaced session must fail"),
        Err(error) => error,
    };
    assert_eq!(stale_open, expected_fenced);
    assert_eq!(
        stale_store
            .read(ReadBatch {
                ops: vec![ReadOp::Get(baseline.clone())],
            })
            .expect_err("takeover must fence the old store's next read"),
        expected_fenced
    );
    assert_eq!(
        stale_store
            .commit(WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(LIVE, b"stale-owner-write"),
                    value: b"must-not-apply".to_vec(),
                }],
            })
            .expect_err("takeover must fence the old store's next write"),
        expected_fenced
    );

    let successor_store = open_store(runtime, cluster_file, namespace, &successor_fence);
    let visible = successor_store
        .read(ReadBatch {
            ops: vec![ReadOp::Get(baseline)],
        })
        .expect("successor reads metadata under its exact session");
    assert_eq!(
        visible.results,
        vec![ReadResult::Get(Some(b"visible".to_vec()))]
    );
    authority.clear_session();
    assert_eq!(
        successor_store
            .read(ReadBatch {
                ops: vec![ReadOp::Get(Key::new(LIVE, b"after-heartbeat"))],
            })
            .expect_err("an absent session must fence the serving store"),
        StoreError::Fenced {
            expected_owner_epoch: authority.owner_epoch + 1,
            expected_session_generation: authority.session_generation + 1,
        }
    );
}

struct LiveAuthority {
    database: FdbDatabase,
    session_key: Vec<u8>,
    heartbeat_key: Vec<u8>,
    session_value: Vec<u8>,
    owner_epoch: u64,
    session_generation: u64,
}

impl LiveAuthority {
    fn provision(
        runtime: &FdbRuntime,
        cluster_file: &Path,
        namespace: &[u8],
        shard_bytes: [u8; 16],
    ) -> Self {
        let prefix = FdbStorePrefix::new(namespace).expect("generated store prefix is valid");
        let session_key = prefix
            .subspace(nokv_fdb::FdbSubspaceKind::LeaseSession)
            .component(&shard_bytes)
            .expect("logical shard identity is a bounded component")
            .as_bytes()
            .to_vec();
        let heartbeat_key = prefix
            .subspace(nokv_fdb::FdbSubspaceKind::LeaseHeartbeat)
            .component(&shard_bytes)
            .expect("logical shard identity is a bounded component")
            .as_bytes()
            .to_vec();
        let authority = Self {
            database: FdbDatabase::open(runtime, &FdbConnectionOptions::new(cluster_file))
                .expect("open live authority database"),
            session_key,
            heartbeat_key,
            session_value: b"encoded-owner-session-v1".to_vec(),
            owner_epoch: 1,
            session_generation: 1,
        };
        let transaction = authority.transaction();
        transaction.set(&authority.session_key, &authority.session_value);
        transaction.set(&authority.heartbeat_key, b"heartbeat-v1");
        transaction
            .commit()
            .expect("install live owner session and heartbeat");
        authority
    }

    fn metadata_fence(&self) -> FdbMetadataSessionFence {
        FdbMetadataSessionFence::new(
            self.session_key.clone(),
            self.session_value.clone(),
            self.owner_epoch,
            self.session_generation,
        )
        .expect("bind metadata store to exact owner session")
    }

    fn renew_heartbeat(&self) {
        let transaction = self.transaction();
        transaction.set(&self.heartbeat_key, b"heartbeat-v2");
        transaction
            .commit()
            .expect("renew only the separate heartbeat key");
    }

    fn install_successor_session(&self) -> FdbMetadataSessionFence {
        let successor_value = b"encoded-owner-session-v2".to_vec();
        let transaction = self.transaction();
        assert_eq!(
            transaction
                .get(&self.session_key, false)
                .expect("read predecessor session"),
            Some(self.session_value.clone())
        );
        transaction.set(&self.session_key, &successor_value);
        transaction.set(&self.heartbeat_key, b"heartbeat-v3");
        transaction.commit().expect("install successor session");
        FdbMetadataSessionFence::new(
            self.session_key.clone(),
            successor_value,
            self.owner_epoch + 1,
            self.session_generation + 1,
        )
        .expect("bind successor metadata session")
    }

    fn clear_session(&self) {
        let transaction = self.transaction();
        transaction.clear(&self.session_key);
        transaction.commit().expect("clear live owner session");
    }

    fn transaction(&self) -> FdbTransaction {
        self.database
            .transaction()
            .expect("create live authority transaction")
    }
}

fn check_real_foundationdb_conflict(guard: &NamespaceGuard, namespace: &[u8]) {
    let physical_key = KeyCodec::new(namespace)
        .unwrap()
        .encode(LIVE, b"raw-conflict");
    let first = guard.transaction();
    let second = guard.transaction();
    assert!(first
        .get(&physical_key, false)
        .expect("read first conflict transaction")
        .is_none());
    assert!(second
        .get(&physical_key, false)
        .expect("read second conflict transaction")
        .is_none());
    first.set(&physical_key, b"first");
    second.set(&physical_key, b"second");
    first
        .commit()
        .expect("commit first conflicting transaction");
    let error = second
        .commit()
        .expect_err("second transaction must conflict");
    assert_eq!(error.code(), 1020, "expected FoundationDB not_committed");
}

struct NamespaceGuard {
    database: FdbDatabase,
    begin: Vec<u8>,
    end: Vec<u8>,
}

impl NamespaceGuard {
    fn fresh(runtime: &FdbRuntime, cluster_file: &Path, namespace: &[u8]) -> Self {
        let codec = KeyCodec::new(namespace).expect("generated namespace is valid");
        let begin = codec.store_prefix().to_vec();
        let end = lexicographic_successor(&begin)
            .expect("generated FoundationDB namespace prefix has a successor");
        let guard = Self {
            database: FdbDatabase::open(runtime, &FdbConnectionOptions::new(cluster_file))
                .expect("open cleanup database"),
            begin,
            end,
        };
        guard.clear().expect("clear fresh generated namespace");
        guard
    }

    fn transaction(&self) -> FdbTransaction {
        self.database
            .transaction()
            .expect("create live FoundationDB transaction")
    }

    fn clear(&self) -> Result<(), String> {
        let transaction = self.transaction();
        transaction.clear_range(&self.begin, &self.end);
        transaction
            .commit()
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

impl Drop for NamespaceGuard {
    fn drop(&mut self) {
        if let Err(error) = self.clear() {
            eprintln!("failed to clean generated FoundationDB namespace: {error}");
        }
    }
}
