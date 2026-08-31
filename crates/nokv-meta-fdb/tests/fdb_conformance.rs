/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(feature = "fdb")]

use std::env;
use std::path::{Path, PathBuf};

use foundationdb::options::TransactionOption;
use foundationdb::Database;
use futures::executor::block_on;
use nokv_meta_fdb::{FdbOptions, FdbStore};
use nokv_meta_store::conformance;
use nokv_meta_store::{
    Commit, Key, Keyspace, Mutation, ReadBatch, ReadOp, ReadResult, Scan, ScanItem, TxnStore,
    WriteTxn,
};
use uuid::Uuid;

#[allow(dead_code)]
#[path = "../src/codec.rs"]
mod production_codec;

use production_codec::{lexicographic_successor, KeyCodec};

const LIVE: Keyspace = Keyspace::new(0x7e01);
const TRANSACTION_TIMEOUT_MILLIS: i32 = 4_000;

#[test]
#[ignore = "requires NOKV_TEST_FDB_CLUSTER_FILE, a compatible libfdb_c, and a disposable cluster"]
fn fdb_live_conformance_and_isolation() {
    // SAFETY: this integration-test binary boots the FoundationDB client once,
    // and `network` is dropped only after every database and store handle.
    let network = unsafe { foundationdb::boot() };
    let cluster_file = cluster_file();
    let first_namespace = unique_namespace();
    let second_namespace = unique_namespace();
    let first_guard = NamespaceGuard::fresh(&cluster_file, &first_namespace);
    let second_guard = NamespaceGuard::fresh(&cluster_file, &second_namespace);

    let first_options = FdbOptions::new(&cluster_file, first_namespace.clone());
    let reopen_options = first_options.clone();
    conformance::run(
        move || FdbStore::open(first_options),
        move || FdbStore::open(reopen_options),
    );

    check_binary_scan_and_delimiter(&cluster_file, &first_namespace);
    check_namespace_isolation(&cluster_file, &first_namespace, &second_namespace);
    check_real_foundationdb_conflict(&first_guard, &first_namespace);

    drop(second_guard);
    drop(first_guard);
    drop(network);
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

fn open_store(cluster_file: &Path, namespace: &[u8]) -> FdbStore {
    FdbStore::open(FdbOptions::new(cluster_file, namespace.to_vec()))
        .unwrap_or_else(|error| panic!("open live FdbStore: {error}"))
}

fn check_binary_scan_and_delimiter(cluster_file: &Path, namespace: &[u8]) {
    let store = open_store(cluster_file, namespace);
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

fn check_namespace_isolation(cluster_file: &Path, first_namespace: &[u8], second_namespace: &[u8]) {
    let first = open_store(cluster_file, first_namespace);
    let second = open_store(cluster_file, second_namespace);
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

fn check_real_foundationdb_conflict(guard: &NamespaceGuard, namespace: &[u8]) {
    let physical_key = KeyCodec::new(namespace).encode(LIVE, b"raw-conflict");
    let first = guard.transaction();
    let second = guard.transaction();
    assert!(block_on(first.get(&physical_key, false))
        .expect("read first conflict transaction")
        .is_none());
    assert!(block_on(second.get(&physical_key, false))
        .expect("read second conflict transaction")
        .is_none());
    first.set(&physical_key, b"first");
    second.set(&physical_key, b"second");
    block_on(first.commit()).expect("commit first conflicting transaction");
    let error = block_on(second.commit()).expect_err("second transaction must conflict");
    assert_eq!(error.code(), 1020, "expected FoundationDB not_committed");
}

struct NamespaceGuard {
    database: Database,
    begin: Vec<u8>,
    end: Vec<u8>,
}

impl NamespaceGuard {
    fn fresh(cluster_file: &Path, namespace: &[u8]) -> Self {
        let cluster_file = cluster_file
            .to_str()
            .expect("NOKV_TEST_FDB_CLUSTER_FILE must be UTF-8");
        let codec = KeyCodec::new(namespace);
        let begin = codec.store_prefix().to_vec();
        let end = lexicographic_successor(&begin)
            .expect("generated FoundationDB namespace prefix has a successor");
        let guard = Self {
            database: Database::from_path(cluster_file).expect("open cleanup database"),
            begin,
            end,
        };
        guard.clear().expect("clear fresh generated namespace");
        guard
    }

    fn transaction(&self) -> foundationdb::Transaction {
        let transaction = self
            .database
            .create_trx()
            .expect("create live FoundationDB transaction");
        transaction
            .set_option(TransactionOption::Timeout(TRANSACTION_TIMEOUT_MILLIS))
            .expect("set live FoundationDB transaction timeout");
        transaction
    }

    fn clear(&self) -> Result<(), String> {
        let transaction = self.transaction();
        transaction.clear_range(&self.begin, &self.end);
        block_on(transaction.commit())
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
