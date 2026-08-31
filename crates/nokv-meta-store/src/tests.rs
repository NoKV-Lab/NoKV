/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{
    AckBoundary, Authority, Check, Commit, Key, Keyspace, LimitKind, Mutation, ReadBatch, ReadOp,
    ReadResult, ReadSnapshot, RecoveryMode, Scan, ScanItem, ScanPage, StoreError, StoreLimits,
    StoreProfile, TxnStore, UnknownCommit, WriteTxn,
};

const FIRST: Keyspace = Keyspace::new(1);

fn limits() -> StoreLimits {
    StoreLimits {
        max_reads: 8,
        max_checks: 8,
        max_mutations: 8,
        max_key_bytes: 32,
        max_value_bytes: 64,
        max_read_bytes: 1_024,
        max_transaction_bytes: 256,
        max_result_rows: 16,
        max_result_bytes: 512,
    }
}

struct FakeStore;

impl TxnStore for FakeStore {
    fn profile(&self) -> StoreProfile {
        StoreProfile {
            limits: limits(),
            transaction_target_bytes: limits().max_transaction_bytes,
            ack: AckBoundary::LocalSync,
            authority: Authority::Local,
            recovery: RecoveryMode::LocalJournal,
        }
    }

    fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
        batch.validate(&self.profile().limits)?;
        let results = batch
            .ops
            .iter()
            .map(|op| match op {
                ReadOp::Get(_) => ReadResult::Get(None),
                ReadOp::Scan(_) => ReadResult::Scan(ScanPage {
                    items: Vec::new(),
                    more: false,
                }),
            })
            .collect();
        let snapshot = ReadSnapshot { results };
        snapshot.validate(&batch, &self.profile().limits)?;
        Ok(snapshot)
    }

    fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
        txn.validate(&self.profile().limits)?;
        Ok(Commit::Applied)
    }

    fn ready(&self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[test]
fn trait_is_object_safe() {
    let store: Box<dyn TxnStore> = Box::new(FakeStore);
    store.ready().unwrap();
    assert_eq!(store.profile().ack, AckBoundary::LocalSync);
    assert_eq!(store.profile().authority, Authority::Local);
    assert_eq!(store.profile().recovery, RecoveryMode::LocalJournal);
    assert_eq!(
        store.profile().transaction_target_bytes,
        limits().max_transaction_bytes
    );
    assert_eq!(
        store
            .commit(WriteTxn {
                checks: vec![],
                mutations: vec![],
            })
            .unwrap(),
        Commit::Applied
    );

    let snapshot = store
        .read(ReadBatch {
            ops: vec![ReadOp::Get(Key::new(FIRST, b"missing".to_vec()))],
        })
        .unwrap();
    assert_eq!(snapshot.results, vec![ReadResult::Get(None)]);
}

#[test]
fn read_validation_enforces_affected_and_result_byte_budgets() {
    let mut roomy_rows = limits();
    roomy_rows.max_result_rows = 64;
    let affected = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"path/".to_vec(),
            after: None,
            limit: 32,
            max_bytes: 128,
            delimiter: None,
        })],
    };
    assert_eq!(
        affected.validate(&roomy_rows),
        Err(StoreError::LimitExceeded {
            kind: LimitKind::ReadBytes,
            actual: 1_067,
            maximum: 1_024,
        })
    );

    let result = ReadBatch {
        ops: (0..5)
            .map(|_| {
                ReadOp::Scan(Scan {
                    keyspace: FIRST,
                    prefix: b"p".to_vec(),
                    after: None,
                    limit: 1,
                    max_bytes: 128,
                    delimiter: None,
                })
            })
            .collect(),
    };
    assert_eq!(
        result.validate(&limits()),
        Err(StoreError::LimitExceeded {
            kind: LimitKind::ResultBytes,
            actual: 640,
            maximum: 512,
        })
    );
}

#[test]
fn read_validation_rejects_zero_limit_and_foreign_cursor() {
    let zero = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"path/".to_vec(),
            after: None,
            limit: 0,
            max_bytes: 128,
            delimiter: None,
        })],
    };
    assert_eq!(
        zero.validate(&limits()),
        Err(StoreError::InvalidRequest(
            "scan limit must be positive".to_owned()
        ))
    );

    let foreign = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"path/".to_vec(),
            after: Some(b"other/item".to_vec()),
            limit: 1,
            max_bytes: 128,
            delimiter: None,
        })],
    };
    assert_eq!(
        foreign.validate(&limits()),
        Err(StoreError::InvalidRequest(
            "scan cursor is not a canonical output key".to_owned()
        ))
    );

    let nested = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"path/".to_vec(),
            after: Some(b"path/a/item".to_vec()),
            limit: 1,
            max_bytes: 128,
            delimiter: Some(b'/'),
        })],
    };
    assert_eq!(
        nested.validate(&limits()),
        Err(StoreError::InvalidRequest(
            "scan cursor is not a canonical output key".to_owned()
        ))
    );
}

#[test]
fn snapshot_validation_enforces_delimiter_and_order() {
    let batch = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"path/".to_vec(),
            after: Some(b"path/a".to_vec()),
            limit: 3,
            max_bytes: 128,
            delimiter: Some(b'/'),
        })],
    };
    let valid = ReadSnapshot {
        results: vec![ReadResult::Scan(ScanPage {
            items: vec![
                ScanItem::CommonPrefix(b"path/b/".to_vec()),
                ScanItem::Row {
                    key: b"path/c".to_vec(),
                    value: b"value".to_vec(),
                },
            ],
            more: true,
        })],
    };
    valid.validate(&batch, &limits()).unwrap();

    ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"path/".to_vec(),
            after: Some(b"path/b/".to_vec()),
            limit: 3,
            max_bytes: 128,
            delimiter: Some(b'/'),
        })],
    }
    .validate(&limits())
    .unwrap();

    let unfolded = ReadSnapshot {
        results: vec![ReadResult::Scan(ScanPage {
            items: vec![ScanItem::Row {
                key: b"path/b/item".to_vec(),
                value: b"value".to_vec(),
            }],
            more: false,
        })],
    };
    assert_eq!(
        unfolded.validate(&batch, &limits()),
        Err(StoreError::Corrupt(
            "scan returned an unfolded row after the delimiter".to_owned()
        ))
    );

    let noncanonical = ReadSnapshot {
        results: vec![ReadResult::Scan(ScanPage {
            items: vec![ScanItem::CommonPrefix(b"path/z".to_vec())],
            more: false,
        })],
    };
    assert_eq!(
        noncanonical.validate(&batch, &limits()),
        Err(StoreError::Corrupt(
            "scan returned a non-canonical common prefix".to_owned()
        ))
    );

    let missing_cursor = ReadSnapshot {
        results: vec![ReadResult::Scan(ScanPage {
            items: Vec::new(),
            more: true,
        })],
    };
    assert_eq!(
        missing_cursor.validate(&batch, &limits()),
        Err(StoreError::Corrupt(
            "scan page claims more rows without a continuation cursor".to_owned()
        ))
    );
}

#[test]
fn snapshot_validation_enforces_each_scan_byte_limit() {
    let batch = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: b"p/".to_vec(),
            after: None,
            limit: 2,
            max_bytes: 96,
            delimiter: None,
        })],
    };
    let mut first = b"p/".to_vec();
    first.extend_from_slice(&[b'a'; 30]);
    let mut second = b"p/".to_vec();
    second.extend_from_slice(&[b'b'; 30]);
    let oversized = ReadSnapshot {
        results: vec![ReadResult::Scan(ScanPage {
            items: vec![
                ScanItem::Row {
                    key: first,
                    value: vec![0; 20],
                },
                ScanItem::Row {
                    key: second,
                    value: vec![0; 20],
                },
            ],
            more: false,
        })],
    };
    assert_eq!(
        oversized.validate(&batch, &limits()),
        Err(StoreError::Corrupt(
            "read result has 104 result bytes, maximum 96".to_owned()
        ))
    );

    let get = ReadBatch {
        ops: vec![ReadOp::Get(Key::new(FIRST, b"key".to_vec()))],
    };
    let oversized_value = ReadSnapshot {
        results: vec![ReadResult::Get(Some(vec![0; 65]))],
    };
    assert_eq!(
        oversized_value.validate(&get, &limits()),
        Err(StoreError::Corrupt(
            "read result has 65 value bytes, maximum 64".to_owned()
        ))
    );
}

#[test]
fn unknown_commit_states_remain_distinct() {
    let cases = [
        (UnknownCommit::Settled, "settled"),
        (UnknownCommit::MayCommit, "may still commit"),
        (UnknownCommit::Poisoned, "store poisoned"),
    ];
    for (state, label) in cases {
        assert_eq!(state.to_string(), label);
        let error = StoreError::OutcomeUnknown {
            state,
            reason: "test".to_owned(),
        };
        assert!(error.to_string().contains(label));
    }
}

#[test]
fn write_validation_enforces_keys_values_budgets_and_uniqueness() {
    let key = Key::new(FIRST, b"same".to_vec());
    let duplicate = WriteTxn {
        checks: vec![],
        mutations: vec![
            Mutation::Delete { key: key.clone() },
            Mutation::Put {
                key,
                value: b"value".to_vec(),
            },
        ],
    };
    assert_eq!(
        duplicate.validate(&limits()),
        Err(StoreError::InvalidRequest(
            "transaction contains duplicate mutation keys".to_owned()
        ))
    );

    let too_large = WriteTxn {
        checks: vec![],
        mutations: vec![Mutation::Put {
            key: Key::new(FIRST, b"key".to_vec()),
            value: vec![0; 65],
        }],
    };
    assert_eq!(
        too_large.validate(&limits()),
        Err(StoreError::LimitExceeded {
            kind: LimitKind::ValueBytes,
            actual: 65,
            maximum: 64,
        })
    );

    let mut range_limit = limits();
    range_limit.max_transaction_bytes = 96;
    let empty_prefix = WriteTxn {
        checks: vec![Check::EmptyPrefix {
            keyspace: FIRST,
            prefix: vec![b'p'; 32],
        }],
        mutations: vec![],
    };
    assert_eq!(
        empty_prefix.validate(&range_limit),
        Err(StoreError::LimitExceeded {
            kind: LimitKind::TransactionBytes,
            actual: 97,
            maximum: 96,
        })
    );
}
