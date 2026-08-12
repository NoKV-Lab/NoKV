/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reusable conformance checks for [`TxnStore`] adapters.
//!
//! Enable the `test-support` feature from an adapter's development dependency.
//! The runner uses the production store trait and does not define a second
//! adapter interface. Fault injection stays adapter-specific. The fault helpers
//! accept closures so an adapter can keep its controls private to its tests.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;

use crate::{
    Check, Commit, Key, Keyspace, LimitKind, Mutation, ReadBatch, ReadOp, ReadResult, Scan,
    ScanItem, ScanPage, StoreError, StoreProfile, TxnStore, UnknownCommit, WriteTxn,
};

const LINEAR: Keyspace = Keyspace::new(0x7f01);
const ATOMIC_FIRST: Keyspace = Keyspace::new(0x7f02);
const ATOMIC_SECOND: Keyspace = Keyspace::new(0x7f03);
const CHECKS: Keyspace = Keyspace::new(0x7f04);
const EMPTY_PREFIX: Keyspace = Keyspace::new(0x7f05);
const SCANS: Keyspace = Keyspace::new(0x7f06);
const BYTE_LIMIT: Keyspace = Keyspace::new(0x7f07);
const REOPEN: Keyspace = Keyspace::new(0x7f08);

/// Run the storage-neutral adapter checks against a fresh store and its exact
/// reopen path.
///
/// Both closures must address the same physical namespace and authority. The
/// first closure must create an empty namespace. The second closure runs only
/// after the first store instance and all of its clones are dropped.
pub fn run<S>(
    open_new: impl FnOnce() -> Result<S, StoreError>,
    reopen: impl FnOnce() -> Result<S, StoreError>,
) where
    S: TxnStore + 'static,
{
    let store = Arc::new(
        open_new().unwrap_or_else(|error| panic!("open fresh conformance store: {error}")),
    );
    store
        .ready()
        .unwrap_or_else(|error| panic!("fresh conformance store is not ready: {error}"));
    let profile = store.profile();
    check_profile(profile);
    check_empty_initial_state(store.as_ref());
    check_read_after_applied(store.as_ref());
    check_atomic_snapshots(Arc::clone(&store));
    check_exact_checks(store.as_ref());
    check_empty_prefix(store.as_ref());
    check_cursor_and_delimiter(store.as_ref());
    check_byte_limited_scan(store.as_ref());
    check_limit_rejection(store.as_ref());

    commit_applied(
        store.as_ref(),
        WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: Key::new(REOPEN, b"r"),
                value: b"durable".to_vec(),
            }],
        },
        "write reopen marker",
    );
    let persisted = [
        (Key::new(LINEAR, b"k"), b"v2".to_vec()),
        (Key::new(CHECKS, b"c"), b"created".to_vec()),
        (Key::new(EMPTY_PREFIX, b"e1"), b"first".to_vec()),
        (Key::new(SCANS, b"a"), b"a".to_vec()),
        (Key::new(REOPEN, b"r"), b"durable".to_vec()),
    ];
    drop(store);

    let reopened = reopen().unwrap_or_else(|error| panic!("reopen conformance store: {error}"));
    reopened
        .ready()
        .unwrap_or_else(|error| panic!("reopened conformance store is not ready: {error}"));
    let reopened_profile = reopened.profile();
    check_profile(reopened_profile);
    assert_eq!(
        reopened_profile.authority, profile.authority,
        "reopen changed the recovery authority"
    );
    for (key, expected) in persisted {
        assert_eq!(
            read_values(&reopened, vec![key]),
            vec![Some(expected)],
            "reopen lost an earlier commit that returned Applied"
        );
    }
}

/// Assert that an adapter-specific fault returned the required unknown state.
pub fn assert_unknown(result: Result<Commit, StoreError>, expected: UnknownCommit) {
    match result {
        Err(StoreError::OutcomeUnknown { state, .. }) => assert_eq!(
            state, expected,
            "commit returned the wrong unknown-outcome state"
        ),
        other => panic!("expected OutcomeUnknown({expected}), got {other:?}"),
    }
}

/// Assert that a healthy store becomes unavailable after a poisoned outcome.
///
/// The helper first verifies a nonempty point read and check-only transaction.
/// The trigger must poison the instance before it returns. Each adapter must
/// use its own barrier-based test for operations that overlap the transition.
pub fn assert_poisoned<S>(
    store: &S,
    healthy_key: Key,
    healthy_value: Vec<u8>,
    trigger: impl FnOnce() -> Result<Commit, StoreError>,
) where
    S: TxnStore + ?Sized,
{
    let healthy_read = ReadBatch {
        ops: vec![ReadOp::Get(healthy_key.clone())],
    };
    let healthy_snapshot = store
        .read(healthy_read.clone())
        .unwrap_or_else(|error| panic!("healthy read failed before poison trigger: {error}"));
    healthy_snapshot
        .validate(&healthy_read, &store.profile().limits)
        .unwrap_or_else(|error| panic!("healthy read returned invalid data: {error}"));
    assert_eq!(
        healthy_snapshot.results,
        vec![ReadResult::Get(Some(healthy_value.clone()))],
        "healthy read returned the wrong value before poison trigger"
    );

    let healthy_check = WriteTxn {
        checks: vec![Check::Value {
            key: healthy_key,
            expected: healthy_value,
        }],
        mutations: vec![],
    };
    assert_eq!(
        store
            .commit(healthy_check.clone())
            .unwrap_or_else(|error| panic!("healthy check failed before poison trigger: {error}")),
        Commit::Applied,
        "healthy check conflicted before poison trigger"
    );

    assert_unknown(trigger(), UnknownCommit::Poisoned);
    assert!(
        store.ready().is_err(),
        "ready succeeded after a poisoned outcome"
    );
    assert!(
        store.read(healthy_read).is_err(),
        "read succeeded after a poisoned outcome"
    );
    assert!(
        store.commit(healthy_check).is_err(),
        "commit succeeded after a poisoned outcome"
    );
}

fn check_profile(profile: StoreProfile) {
    let limits = profile.limits;
    assert!(
        limits.max_reads >= 2,
        "store must support two reads per batch"
    );
    assert!(limits.max_checks >= 1, "store must support one check");
    assert!(
        limits.max_mutations >= 2,
        "store must support two atomic mutations"
    );
    assert!(limits.max_key_bytes >= 3, "store key limit is too small");
    assert!(limits.max_value_bytes >= 1, "store value limit is zero");
    assert!(
        limits.max_result_rows >= 2,
        "store must return two rows per page"
    );
    let full_row_bytes = limits
        .max_key_bytes
        .checked_add(limits.max_value_bytes)
        .expect("store row byte limits overflow usize");
    assert!(
        limits.max_result_bytes >= full_row_bytes,
        "store result-byte limit cannot hold one maximum-size row"
    );
}

fn check_empty_initial_state(store: &dyn TxnStore) {
    assert_eq!(
        read_values(store, vec![Key::new(LINEAR, b"k")]),
        vec![None],
        "fresh store exposed a conformance row"
    );
}

fn check_read_after_applied(store: &dyn TxnStore) {
    commit_applied(
        store,
        WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: Key::new(LINEAR, b"k"),
                value: b"v1".to_vec(),
            }],
        },
        "write read-after-Applied row",
    );
    assert_eq!(
        read_values(store, vec![Key::new(LINEAR, b"k")]),
        vec![Some(b"v1".to_vec())],
        "read did not include an earlier Applied commit"
    );

    commit_applied(
        store,
        WriteTxn {
            checks: vec![Check::Value {
                key: Key::new(LINEAR, b"k"),
                expected: b"v1".to_vec(),
            }],
            mutations: vec![Mutation::Put {
                key: Key::new(LINEAR, b"k"),
                value: b"v2".to_vec(),
            }],
        },
        "apply exact value check",
    );
    assert_eq!(
        read_values(store, vec![Key::new(LINEAR, b"k")]),
        vec![Some(b"v2".to_vec())]
    );
}

fn check_atomic_snapshots<S>(store: Arc<S>)
where
    S: TxnStore + 'static,
{
    let first = Key::new(ATOMIC_FIRST, b"k");
    let second = Key::new(ATOMIC_SECOND, b"p/k");
    write_pair(store.as_ref(), &first, &second, 0);

    let start = Arc::new(Barrier::new(2));
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let (first_write_tx, first_write_rx) = mpsc::sync_channel(0);
    let writer_store = Arc::clone(&store);
    let writer_start = Arc::clone(&start);
    let writer_stop = Arc::clone(&stop);
    let writer_writes = Arc::clone(&writes);
    let writer_first = first.clone();
    let writer_second = second.clone();
    let writer = thread::spawn(move || {
        writer_start.wait();
        while !writer_stop.load(Ordering::Acquire) {
            let index = writer_writes.load(Ordering::Relaxed);
            write_pair(
                writer_store.as_ref(),
                &writer_first,
                &writer_second,
                u8::from(!index.is_multiple_of(2)),
            );
            let completed = writer_writes.fetch_add(1, Ordering::Release) + 1;
            if completed == 1 {
                first_write_tx
                    .send(())
                    .expect("atomicity reader stopped before the first concurrent write");
            }
            thread::yield_now();
        }
    });

    start.wait();
    first_write_rx
        .recv()
        .expect("atomicity writer stopped before its first concurrent write");
    let writes_before_reads = writes.load(Ordering::Acquire);
    assert!(
        writes_before_reads >= 1,
        "atomicity reader started before the writer committed"
    );
    let reader = catch_unwind(AssertUnwindSafe(|| {
        for _ in 0..512 {
            check_mixed_snapshot(store.as_ref(), &first, &second);
            thread::yield_now();
        }
    }));
    let writes_during_reads = writes.load(Ordering::Acquire);
    stop.store(true, Ordering::Release);
    writer.join().expect("atomicity writer thread panicked");
    if let Err(payload) = reader {
        resume_unwind(payload);
    }
    assert!(
        writes_during_reads > writes_before_reads,
        "writer made no progress while mixed read batches ran"
    );
}

fn check_mixed_snapshot(store: &dyn TxnStore, point: &Key, range_row: &Key) {
    let batch = ReadBatch {
        ops: vec![
            ReadOp::Get(point.clone()),
            ReadOp::Scan(Scan {
                keyspace: range_row.keyspace,
                prefix: b"p/".to_vec(),
                after: None,
                limit: 1,
                max_bytes: minimum_scan_bytes(store.profile()),
                delimiter: None,
            }),
        ],
    };
    batch
        .validate(&store.profile().limits)
        .unwrap_or_else(|error| panic!("store profile cannot admit mixed read batch: {error}"));
    let snapshot = store
        .read(batch.clone())
        .unwrap_or_else(|error| panic!("mixed read batch failed: {error}"));
    snapshot
        .validate(&batch, &store.profile().limits)
        .unwrap_or_else(|error| panic!("mixed read batch returned invalid data: {error}"));
    let [ReadResult::Get(Some(point_value)), ReadResult::Scan(page)] = snapshot.results.as_slice()
    else {
        panic!("mixed read batch returned the wrong result shape");
    };
    assert_eq!(
        page.items,
        vec![ScanItem::Row {
            key: range_row.bytes.clone(),
            value: point_value.clone(),
        }],
        "one read batch observed a torn point and range snapshot"
    );
    assert!(!page.more, "single-row mixed scan reported more rows");
}

fn write_pair(store: &dyn TxnStore, first: &Key, second: &Key, value: u8) {
    commit_applied(
        store,
        WriteTxn {
            checks: vec![],
            mutations: vec![
                Mutation::Put {
                    key: first.clone(),
                    value: vec![value],
                },
                Mutation::Put {
                    key: second.clone(),
                    value: vec![value],
                },
            ],
        },
        "write atomic multi-keyspace pair",
    );
}

fn check_exact_checks(store: &dyn TxnStore) {
    let guard = Key::new(CHECKS, b"g");
    let target = Key::new(CHECKS, b"t");
    let created = Key::new(CHECKS, b"c");
    let other = Key::new(ATOMIC_SECOND, b"f");
    commit_applied(
        store,
        WriteTxn {
            checks: vec![],
            mutations: vec![
                Mutation::Put {
                    key: guard.clone(),
                    value: b"actual".to_vec(),
                },
                Mutation::Put {
                    key: target.clone(),
                    value: b"before".to_vec(),
                },
            ],
        },
        "seed false-check rows",
    );

    commit_applied(
        store,
        WriteTxn {
            checks: vec![Check::Absent {
                key: created.clone(),
            }],
            mutations: vec![Mutation::Put {
                key: created.clone(),
                value: b"created".to_vec(),
            }],
        },
        "apply absence check and create the guarded key",
    );
    assert_eq!(
        read_values(store, vec![created]),
        vec![Some(b"created".to_vec())],
        "true absence check did not publish its mutation"
    );

    let conflict = store
        .commit(WriteTxn {
            checks: vec![Check::Value {
                key: guard.clone(),
                expected: b"wrong".to_vec(),
            }],
            mutations: vec![
                Mutation::Put {
                    key: target.clone(),
                    value: b"after".to_vec(),
                },
                Mutation::Put {
                    key: other.clone(),
                    value: b"after".to_vec(),
                },
            ],
        })
        .unwrap_or_else(|error| panic!("false value check returned an error: {error}"));
    assert_eq!(conflict, Commit::Conflict);
    assert_eq!(
        read_values(store, vec![target.clone(), other.clone()]),
        vec![Some(b"before".to_vec()), None],
        "false value check applied a mutation"
    );

    let absent = store
        .commit(WriteTxn {
            checks: vec![Check::Absent { key: guard }],
            mutations: vec![Mutation::Delete {
                key: target.clone(),
            }],
        })
        .unwrap_or_else(|error| panic!("false absence check returned an error: {error}"));
    assert_eq!(absent, Commit::Conflict);
    assert_eq!(
        read_values(store, vec![target.clone()]),
        vec![Some(b"before".to_vec())],
        "false absence check applied a mutation"
    );

    commit_applied(
        store,
        WriteTxn {
            checks: vec![Check::Value {
                key: target.clone(),
                expected: b"before".to_vec(),
            }],
            mutations: vec![Mutation::Delete {
                key: target.clone(),
            }],
        },
        "apply checked delete",
    );
    assert_eq!(
        read_values(store, vec![target]),
        vec![None],
        "applied delete remained visible"
    );
}

fn check_empty_prefix(store: &dyn TxnStore) {
    let prefix = b"e".to_vec();
    commit_applied(
        store,
        WriteTxn {
            checks: vec![Check::EmptyPrefix {
                keyspace: EMPTY_PREFIX,
                prefix: prefix.clone(),
            }],
            mutations: vec![Mutation::Put {
                key: Key::new(EMPTY_PREFIX, b"e1"),
                value: b"first".to_vec(),
            }],
        },
        "apply true empty-prefix check",
    );

    let blocked = Key::new(ATOMIC_FIRST, b"e");
    let conflict = store
        .commit(WriteTxn {
            checks: vec![Check::EmptyPrefix {
                keyspace: EMPTY_PREFIX,
                prefix,
            }],
            mutations: vec![Mutation::Put {
                key: blocked.clone(),
                value: b"bad".to_vec(),
            }],
        })
        .unwrap_or_else(|error| panic!("false empty-prefix check returned an error: {error}"));
    assert_eq!(conflict, Commit::Conflict);
    assert_eq!(read_values(store, vec![blocked]), vec![None]);
}

fn check_cursor_and_delimiter(store: &dyn TxnStore) {
    for (key, value) in [
        (b"a".as_slice(), b"a".as_slice()),
        (b"b/x".as_slice(), b"x".as_slice()),
        (b"b/y".as_slice(), b"y".as_slice()),
        (b"c".as_slice(), b"c".as_slice()),
    ] {
        commit_applied(
            store,
            WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(SCANS, key),
                    value: value.to_vec(),
                }],
            },
            "seed delimiter scan",
        );
    }

    let max_bytes = minimum_scan_bytes(store.profile());
    let first = scan(
        store,
        Scan {
            keyspace: SCANS,
            prefix: vec![],
            after: None,
            limit: 2,
            max_bytes,
            delimiter: Some(b'/'),
        },
    );
    assert_eq!(
        first.items,
        vec![
            ScanItem::Row {
                key: b"a".to_vec(),
                value: b"a".to_vec(),
            },
            ScanItem::CommonPrefix(b"b/".to_vec()),
        ]
    );
    assert!(first.more, "full delimiter page did not report more rows");

    let second = scan(
        store,
        Scan {
            keyspace: SCANS,
            prefix: vec![],
            after: Some(b"b/".to_vec()),
            limit: 2,
            max_bytes,
            delimiter: Some(b'/'),
        },
    );
    assert_eq!(
        second.items,
        vec![ScanItem::Row {
            key: b"c".to_vec(),
            value: b"c".to_vec(),
        }]
    );
    assert!(!second.more, "final delimiter page reported more rows");

    for key in [b"d/1".as_slice(), b"d/2".as_slice()] {
        commit_applied(
            store,
            WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(SCANS, key),
                    value: b"nested".to_vec(),
                }],
            },
            "seed single-prefix delimiter scan",
        );
    }
    let single_prefix = scan(
        store,
        Scan {
            keyspace: SCANS,
            prefix: b"d".to_vec(),
            after: None,
            limit: 2,
            max_bytes,
            delimiter: Some(b'/'),
        },
    );
    assert_eq!(
        single_prefix.items,
        vec![ScanItem::CommonPrefix(b"d/".to_vec())]
    );
    assert!(
        !single_prefix.more,
        "one folded common prefix reported hidden physical rows as more"
    );

    for key in [b"f1".as_slice(), b"f2".as_slice()] {
        commit_applied(
            store,
            WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(SCANS, key),
                    value: b"full".to_vec(),
                }],
            },
            "seed exact-limit delimiter scan",
        );
    }
    let exact_limit = scan(
        store,
        Scan {
            keyspace: SCANS,
            prefix: b"f".to_vec(),
            after: None,
            limit: 2,
            max_bytes,
            delimiter: Some(b'/'),
        },
    );
    assert_eq!(
        exact_limit.items,
        vec![
            ScanItem::Row {
                key: b"f1".to_vec(),
                value: b"full".to_vec(),
            },
            ScanItem::Row {
                key: b"f2".to_vec(),
                value: b"full".to_vec(),
            },
        ]
    );
    assert!(
        !exact_limit.more,
        "exact-limit delimiter page reported more at EOF"
    );
}

fn check_byte_limited_scan(store: &dyn TxnStore) {
    let profile = store.profile();
    let max_bytes = minimum_scan_bytes(profile);
    let item_bytes = max_bytes / 2 + 1;
    assert!(
        profile.limits.max_transaction_bytes >= item_bytes,
        "store transaction limit cannot construct the byte-limit conformance case"
    );
    let key_len = profile
        .limits
        .max_key_bytes
        .min(item_bytes.saturating_sub(1))
        .max(2);
    let value_len = item_bytes - key_len;
    assert!(value_len <= profile.limits.max_value_bytes);
    let mut first_key = vec![b'a'; key_len];
    let mut second_key = vec![b'b'; key_len];
    first_key[0] = b'z';
    second_key[0] = b'z';
    let first_value = vec![b'1'; value_len];
    let second_value = vec![b'2'; value_len];
    for (key, value) in [
        (first_key.clone(), first_value.clone()),
        (second_key.clone(), second_value.clone()),
    ] {
        commit_applied(
            store,
            WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(BYTE_LIMIT, key),
                    value,
                }],
            },
            "seed byte-limited scan",
        );
    }

    let first = scan(
        store,
        Scan {
            keyspace: BYTE_LIMIT,
            prefix: b"z".to_vec(),
            after: None,
            limit: 2,
            max_bytes,
            delimiter: None,
        },
    );
    assert_eq!(
        first.items,
        vec![ScanItem::Row {
            key: first_key.clone(),
            value: first_value,
        }]
    );
    assert!(first.more, "byte-limited page did not report more rows");

    let second = scan(
        store,
        Scan {
            keyspace: BYTE_LIMIT,
            prefix: b"z".to_vec(),
            after: Some(first_key),
            limit: 2,
            max_bytes,
            delimiter: None,
        },
    );
    assert_eq!(
        second.items,
        vec![ScanItem::Row {
            key: second_key,
            value: second_value,
        }]
    );
    assert!(!second.more, "final byte-limited page reported more rows");
}

fn check_limit_rejection(store: &dyn TxnStore) {
    let limits = store.profile().limits;
    let guard = Key::new(CHECKS, b"limit-guard");
    let stable = b"stable".to_vec();
    commit_applied(
        store,
        WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: guard.clone(),
                value: stable.clone(),
            }],
        },
        "seed limit-rejection guard",
    );

    let read_count = limits
        .max_reads
        .checked_add(1)
        .expect("read-count limit cannot be exercised at usize::MAX");
    expect_limit(
        store.read(ReadBatch {
            ops: vec![ReadOp::Get(guard.clone()); read_count],
        }),
        LimitKind::Reads,
        "over-limit read count",
    );

    let result_rows = limits
        .max_result_rows
        .checked_add(1)
        .expect("result-row limit cannot be exercised at usize::MAX");
    expect_limit(
        store.read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: SCANS,
                prefix: b"l".to_vec(),
                after: None,
                limit: result_rows,
                max_bytes: minimum_scan_bytes(store.profile()),
                delimiter: None,
            })],
        }),
        LimitKind::ResultRows,
        "over-limit result-row budget",
    );

    let result_bytes = limits
        .max_result_bytes
        .checked_add(1)
        .expect("result-byte limit cannot be exercised at usize::MAX");
    expect_limit(
        store.read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: SCANS,
                prefix: b"l".to_vec(),
                after: None,
                limit: 1,
                max_bytes: result_bytes,
                delimiter: None,
            })],
        }),
        LimitKind::ResultBytes,
        "over-limit result-byte budget",
    );

    let scan_limit = limits
        .max_read_bytes
        .saturating_sub(3)
        .checked_div(limits.max_key_bytes)
        .expect("store key limit is zero")
        .max(1);
    assert!(
        scan_limit <= limits.max_result_rows,
        "test profile cannot exercise the read-byte budget independently"
    );
    let affected_bytes = scan_limit
        .checked_add(1)
        .and_then(|rows| rows.checked_mul(limits.max_key_bytes))
        .and_then(|keys| keys.checked_add(3))
        .expect("read-byte conformance request overflows usize");
    assert!(
        affected_bytes > limits.max_read_bytes,
        "read-byte conformance request does not exceed its budget"
    );
    expect_limit(
        store.read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: SCANS,
                prefix: b"l".to_vec(),
                after: None,
                limit: scan_limit,
                max_bytes: minimum_scan_bytes(store.profile()),
                delimiter: None,
            })],
        }),
        LimitKind::ReadBytes,
        "over-limit affected-read budget",
    );

    let checks = limits
        .max_checks
        .checked_add(1)
        .expect("check-count limit cannot be exercised at usize::MAX");
    expect_limit(
        store.commit(WriteTxn {
            checks: vec![Check::Absent { key: guard.clone() }; checks],
            mutations: vec![Mutation::Put {
                key: guard.clone(),
                value: b"changed-checks".to_vec(),
            }],
        }),
        LimitKind::Checks,
        "over-limit check count",
    );
    assert_value(
        store,
        &guard,
        &stable,
        "over-limit check count mutated data",
    );

    let mutations = limits
        .max_mutations
        .checked_add(1)
        .expect("mutation-count limit cannot be exercised at usize::MAX");
    expect_limit(
        store.commit(WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Delete { key: guard.clone() }; mutations],
        }),
        LimitKind::Mutations,
        "over-limit mutation count",
    );
    assert_value(
        store,
        &guard,
        &stable,
        "over-limit mutation count mutated data",
    );

    let key_bytes = limits
        .max_key_bytes
        .checked_add(1)
        .expect("key-byte limit cannot be exercised at usize::MAX");
    expect_limit(
        store.commit(WriteTxn {
            checks: vec![],
            mutations: vec![
                Mutation::Put {
                    key: guard.clone(),
                    value: b"changed-key".to_vec(),
                },
                Mutation::Delete {
                    key: Key::new(CHECKS, vec![b'k'; key_bytes]),
                },
            ],
        }),
        LimitKind::KeyBytes,
        "over-limit key",
    );
    assert_value(store, &guard, &stable, "over-limit key mutated data");

    let value_bytes = limits
        .max_value_bytes
        .checked_add(1)
        .expect("value-byte limit cannot be exercised at usize::MAX");
    expect_limit(
        store.commit(WriteTxn {
            checks: vec![],
            mutations: vec![
                Mutation::Put {
                    key: guard.clone(),
                    value: b"changed-value".to_vec(),
                },
                Mutation::Put {
                    key: Key::new(CHECKS, b"large-value"),
                    value: vec![b'v'; value_bytes],
                },
            ],
        }),
        LimitKind::ValueBytes,
        "over-limit value",
    );
    assert_value(store, &guard, &stable, "over-limit value mutated data");

    let full_row_bytes = minimum_scan_bytes(store.profile());
    assert!(
        full_row_bytes <= limits.max_transaction_bytes,
        "test profile cannot admit one maximum-size transaction row"
    );
    let transaction_rows = limits
        .max_transaction_bytes
        .checked_div(full_row_bytes)
        .and_then(|rows| rows.checked_add(1))
        .expect("transaction-byte limit cannot be exercised");
    assert!(
        transaction_rows <= limits.max_mutations,
        "test profile cannot exercise the transaction-byte budget independently"
    );
    let transaction_guard = indexed_full_key(0, limits.max_key_bytes);
    let transaction_stable = vec![b's'; limits.max_value_bytes];
    commit_applied(
        store,
        WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: transaction_guard.clone(),
                value: transaction_stable.clone(),
            }],
        },
        "seed transaction-byte guard",
    );
    let oversized_transaction = WriteTxn {
        checks: vec![],
        mutations: (0..transaction_rows)
            .map(|index| Mutation::Put {
                key: indexed_full_key(index, limits.max_key_bytes),
                value: vec![b'x'; limits.max_value_bytes],
            })
            .collect(),
    };
    expect_limit(
        store.commit(oversized_transaction),
        LimitKind::TransactionBytes,
        "over-limit transaction-byte budget",
    );
    assert_value(
        store,
        &transaction_guard,
        &transaction_stable,
        "over-limit transaction mutated an earlier row",
    );
    assert_eq!(
        read_values(store, vec![indexed_full_key(1, limits.max_key_bytes)]),
        vec![None],
        "over-limit transaction created a later row"
    );
    assert_value(
        store,
        &guard,
        &stable,
        "an over-limit request mutated the shared guard",
    );
}

fn expect_limit<T: std::fmt::Debug>(
    result: Result<T, StoreError>,
    expected: LimitKind,
    operation: &str,
) {
    match result {
        Err(StoreError::LimitExceeded {
            kind,
            actual,
            maximum,
        }) => {
            assert_eq!(kind, expected, "{operation} returned the wrong limit kind");
            assert!(
                actual > maximum,
                "{operation} reported a non-exceeded limit"
            );
        }
        other => panic!("{operation} returned {other:?}"),
    }
}

fn indexed_full_key(index: usize, length: usize) -> Key {
    let mut bytes = vec![0_u8; length];
    let encoded = index.to_be_bytes();
    let copied = length.min(encoded.len());
    bytes[length - copied..].copy_from_slice(&encoded[encoded.len() - copied..]);
    Key::new(REOPEN, bytes)
}

fn assert_value(store: &dyn TxnStore, key: &Key, expected: &[u8], message: &str) {
    assert_eq!(
        read_values(store, vec![key.clone()]),
        vec![Some(expected.to_vec())],
        "{message}"
    );
}

fn minimum_scan_bytes(profile: StoreProfile) -> usize {
    profile
        .limits
        .max_key_bytes
        .checked_add(profile.limits.max_value_bytes)
        .expect("store row byte limits overflow usize")
}

fn scan(store: &dyn TxnStore, request: Scan) -> ScanPage {
    let batch = ReadBatch {
        ops: vec![ReadOp::Scan(request)],
    };
    batch
        .validate(&store.profile().limits)
        .unwrap_or_else(|error| panic!("store profile cannot admit conformance scan: {error}"));
    let snapshot = store
        .read(batch.clone())
        .unwrap_or_else(|error| panic!("conformance scan failed: {error}"));
    snapshot
        .validate(&batch, &store.profile().limits)
        .unwrap_or_else(|error| panic!("conformance scan returned invalid data: {error}"));
    let mut results = snapshot.results;
    match results.pop() {
        Some(ReadResult::Scan(page)) if results.is_empty() => page,
        _ => panic!("conformance scan returned the wrong result shape"),
    }
}

fn read_values(store: &dyn TxnStore, keys: Vec<Key>) -> Vec<Option<Vec<u8>>> {
    let batch = ReadBatch {
        ops: keys.into_iter().map(ReadOp::Get).collect(),
    };
    batch
        .validate(&store.profile().limits)
        .unwrap_or_else(|error| panic!("store profile cannot admit conformance read: {error}"));
    let snapshot = store
        .read(batch.clone())
        .unwrap_or_else(|error| panic!("conformance read failed: {error}"));
    snapshot
        .validate(&batch, &store.profile().limits)
        .unwrap_or_else(|error| panic!("conformance read returned invalid data: {error}"));
    snapshot
        .results
        .into_iter()
        .map(|result| match result {
            ReadResult::Get(value) => value,
            ReadResult::Scan(_) => panic!("point read returned a scan result"),
        })
        .collect()
}

fn commit_applied(store: &dyn TxnStore, txn: WriteTxn, operation: &str) {
    txn.validate(&store.profile().limits)
        .unwrap_or_else(|error| panic!("store profile cannot admit {operation}: {error}"));
    let result = store
        .commit(txn)
        .unwrap_or_else(|error| panic!("{operation}: {error}"));
    assert_eq!(result, Commit::Applied, "{operation} returned Conflict");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use super::*;
    use crate::{AckBoundary, Authority, ReadSnapshot, StoreLimits};

    #[derive(Clone)]
    struct MemoryStore {
        rows: Arc<Mutex<BTreeMap<Key, Vec<u8>>>>,
    }

    impl TxnStore for MemoryStore {
        fn profile(&self) -> StoreProfile {
            profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            batch.validate(&self.profile().limits)?;
            let rows = self.rows.lock().expect("memory store lock poisoned");
            let results = batch
                .ops
                .iter()
                .map(|op| match op {
                    ReadOp::Get(key) => ReadResult::Get(rows.get(key).cloned()),
                    ReadOp::Scan(scan) => ReadResult::Scan(memory_scan(&rows, scan)),
                })
                .collect();
            let snapshot = ReadSnapshot { results };
            snapshot.validate(&batch, &self.profile().limits)?;
            Ok(snapshot)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            txn.validate(&self.profile().limits)?;
            let mut rows = self.rows.lock().expect("memory store lock poisoned");
            let checks_hold = txn.checks.iter().all(|check| match check {
                Check::Value { key, expected } => rows.get(key) == Some(expected),
                Check::Absent { key } => !rows.contains_key(key),
                Check::EmptyPrefix { keyspace, prefix } => !rows
                    .keys()
                    .any(|key| key.keyspace == *keyspace && key.bytes.starts_with(prefix)),
            });
            if !checks_hold {
                return Ok(Commit::Conflict);
            }
            for mutation in txn.mutations {
                match mutation {
                    Mutation::Put { key, value } => {
                        rows.insert(key, value);
                    }
                    Mutation::Delete { key } => {
                        rows.remove(&key);
                    }
                }
            }
            Ok(Commit::Applied)
        }

        fn ready(&self) -> Result<(), StoreError> {
            Ok(())
        }
    }

    fn profile() -> StoreProfile {
        StoreProfile {
            limits: StoreLimits {
                max_reads: 8,
                max_checks: 8,
                max_mutations: 8,
                max_key_bytes: 32,
                max_value_bytes: 64,
                max_read_bytes: 512,
                max_transaction_bytes: 256,
                max_result_rows: 16,
                max_result_bytes: 512,
            },
            ack: AckBoundary::LocalSync,
            authority: Authority::Local,
        }
    }

    fn memory_scan(rows: &BTreeMap<Key, Vec<u8>>, scan: &Scan) -> ScanPage {
        let mut logical = Vec::new();
        for (key, value) in rows {
            if key.keyspace != scan.keyspace || !key.bytes.starts_with(&scan.prefix) {
                continue;
            }
            let suffix = &key.bytes[scan.prefix.len()..];
            let item = scan
                .delimiter
                .and_then(|delimiter| {
                    suffix
                        .iter()
                        .position(|byte| *byte == delimiter)
                        .map(|offset| {
                            ScanItem::CommonPrefix(
                                key.bytes[..scan.prefix.len() + offset + 1].to_vec(),
                            )
                        })
                })
                .unwrap_or_else(|| ScanItem::Row {
                    key: key.bytes.clone(),
                    value: value.clone(),
                });
            if logical
                .last()
                .is_none_or(|last: &ScanItem| last.key() != item.key())
            {
                logical.push(item);
            }
        }
        let mut candidates = logical
            .into_iter()
            .filter(|item| {
                scan.after
                    .as_ref()
                    .is_none_or(|after| item.key() > after.as_slice())
            })
            .peekable();
        let mut items = Vec::new();
        let mut bytes = 0_usize;
        while let Some(item) = candidates.peek() {
            let item_bytes = item.key().len()
                + match item {
                    ScanItem::Row { value, .. } => value.len(),
                    ScanItem::CommonPrefix(_) => 0,
                };
            if items.len() == scan.limit || bytes + item_bytes > scan.max_bytes {
                break;
            }
            bytes += item_bytes;
            items.push(candidates.next().expect("peeked scan item disappeared"));
        }
        ScanPage {
            items,
            more: candidates.peek().is_some(),
        }
    }

    #[test]
    fn runner_accepts_a_conforming_store_and_reopen() {
        let rows = Arc::new(Mutex::new(BTreeMap::new()));
        let fresh_rows = Arc::clone(&rows);
        let reopened_rows = Arc::clone(&rows);
        run(
            move || Ok(MemoryStore { rows: fresh_rows }),
            move || {
                Ok(MemoryStore {
                    rows: reopened_rows,
                })
            },
        );
    }

    struct PoisonStore {
        poisoned: AtomicBool,
    }

    impl TxnStore for PoisonStore {
        fn profile(&self) -> StoreProfile {
            profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            batch.validate(&self.profile().limits)?;
            if self.poisoned.load(Ordering::Acquire) {
                Err(StoreError::Unavailable("poisoned".to_owned()))
            } else {
                let results = batch
                    .ops
                    .iter()
                    .map(|op| match op {
                        ReadOp::Get(key) => ReadResult::Get(
                            (key == &Key::new(LINEAR, b"healthy")).then(|| b"value".to_vec()),
                        ),
                        ReadOp::Scan(_) => ReadResult::Scan(ScanPage {
                            items: vec![],
                            more: false,
                        }),
                    })
                    .collect();
                let snapshot = ReadSnapshot { results };
                snapshot.validate(&batch, &self.profile().limits)?;
                Ok(snapshot)
            }
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            txn.validate(&self.profile().limits)?;
            if self.poisoned.load(Ordering::Acquire) {
                Err(StoreError::Unavailable("poisoned".to_owned()))
            } else {
                Ok(Commit::Applied)
            }
        }

        fn ready(&self) -> Result<(), StoreError> {
            if self.poisoned.load(Ordering::Acquire) {
                Err(StoreError::Unavailable("poisoned".to_owned()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn poisoned_helper_rejects_later_success() {
        let store = PoisonStore {
            poisoned: AtomicBool::new(false),
        };
        assert_poisoned(
            &store,
            Key::new(LINEAR, b"healthy"),
            b"value".to_vec(),
            || {
                store.poisoned.store(true, Ordering::Release);
                Err(StoreError::OutcomeUnknown {
                    state: UnknownCommit::Poisoned,
                    reason: "injected".to_owned(),
                })
            },
        );
    }

    #[test]
    fn unknown_helper_accepts_each_recovery_state() {
        for state in [
            UnknownCommit::Settled,
            UnknownCommit::MayCommit,
            UnknownCommit::Poisoned,
        ] {
            assert_unknown(
                Err(StoreError::OutcomeUnknown {
                    state,
                    reason: "injected".to_owned(),
                }),
                state,
            );
        }
    }
}
