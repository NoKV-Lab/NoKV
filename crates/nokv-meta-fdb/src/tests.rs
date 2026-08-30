/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::PathBuf;
use std::time::Duration;

use nokv_meta_store::{
    AckBoundary, Authority, Check, Key, Keyspace, LimitKind, Mutation, ReadBatch, ReadOp, Scan,
    StoreError, WriteTxn,
};

use crate::affected_bytes::{ensure_observed_transaction_size, validate_read, validate_write};
use crate::codec::{lexicographic_successor, KeyCodec};
use crate::errors::{classify_error, ErrorDisposition};
use crate::options::MAX_NAMESPACE_BYTES;
use crate::profile::{FDB_LIMITS, FDB_PROFILE, PHYSICAL_AFFECTED_BYTES};
use crate::FdbOptions;

const FIRST: Keyspace = Keyspace::new(0x0102);
const SECOND: Keyspace = Keyspace::new(0x0103);

#[test]
fn options_require_explicit_bounded_configuration() {
    let valid = FdbOptions::new("/tmp/fdb.cluster", b"test".to_vec());
    valid.validate().unwrap();
    assert_eq!(valid.transaction_timeout(), Duration::from_secs(4));

    assert!(matches!(
        FdbOptions::new("relative.cluster", b"test".to_vec()).validate(),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        FdbOptions::new(PathBuf::from("/"), b"test".to_vec()).validate(),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        FdbOptions::new("/tmp/fdb\0.cluster", b"test".to_vec()).validate(),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        FdbOptions::new("/tmp/fdb.cluster", Vec::<u8>::new()).validate(),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        FdbOptions::new("/tmp/fdb.cluster", vec![0; MAX_NAMESPACE_BYTES + 1]).validate(),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        valid
            .clone()
            .with_transaction_timeout(Duration::ZERO)
            .validate(),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        valid
            .with_transaction_timeout(Duration::from_millis(4001))
            .validate(),
        Err(StoreError::InvalidRequest(_))
    ));
}

#[test]
fn physical_encoding_is_namespace_and_keyspace_safe() {
    let first = KeyCodec::new(b"a");
    let second = KeyCodec::new(b"ab");
    assert_ne!(first.store_prefix(), second.store_prefix());
    assert!(!second.store_prefix().starts_with(first.store_prefix()));

    let first_keyspace = first.encode(FIRST, b"key");
    let second_keyspace = first.encode(SECOND, b"key");
    assert!(first_keyspace < second_keyspace);
    assert_eq!(
        first.decode_key(FIRST, &first_keyspace).unwrap(),
        b"key".to_vec()
    );
    assert!(matches!(
        first.decode_key(SECOND, &first_keyspace),
        Err(StoreError::Corrupt(_))
    ));

    let binary = KeyCodec::new(&[0, u8::MAX]);
    let binary_key = [0, b'/', u8::MAX, 0];
    let encoded_binary = binary.encode(FIRST, &binary_key);
    assert_eq!(
        binary.decode_key(FIRST, &encoded_binary).unwrap(),
        binary_key
    );
    assert_eq!(lexicographic_successor(&[0, u8::MAX]), Some(vec![1]));
    assert_eq!(lexicographic_successor(&[u8::MAX]), None);
    assert!(matches!(
        binary.encoded_len(usize::MAX),
        Err(StoreError::InvalidRequest(_))
    ));
}

#[test]
fn scan_cursors_distinguish_rows_from_common_prefixes() {
    let codec = KeyCodec::new(b"test");
    let row_at_prefix = Scan {
        keyspace: FIRST,
        prefix: b"p/".to_vec(),
        after: Some(b"p/".to_vec()),
        limit: 1,
        max_bytes: FDB_LIMITS.max_key_bytes + FDB_LIMITS.max_value_bytes,
        delimiter: Some(b'/'),
    };
    let (row_begin, _) = codec.scan_bounds(&row_at_prefix).unwrap();
    let mut expected_row_begin = codec.encode(FIRST, b"p/");
    expected_row_begin.push(0);
    assert_eq!(row_begin, expected_row_begin);

    let common_prefix = Scan {
        after: Some(b"p/a/".to_vec()),
        ..row_at_prefix
    };
    let (common_begin, _) = codec.scan_bounds(&common_prefix).unwrap();
    assert_eq!(
        common_begin,
        lexicographic_successor(&codec.encode(FIRST, b"p/a/")).unwrap()
    );
}

#[test]
fn profile_is_shared_and_below_serving_transaction_limits() {
    assert_eq!(FDB_PROFILE.authority, Authority::Shared);
    assert_eq!(FDB_PROFILE.ack, AckBoundary::SharedCommit);
    assert_eq!(FDB_PROFILE.limits, FDB_LIMITS);
    assert_eq!(FDB_LIMITS.max_transaction_bytes, 2_900_000);
}

#[test]
fn affected_byte_budget_accounts_for_encoded_ranges_and_mutations() {
    let codec = KeyCodec::new(&[b'n'; MAX_NAMESPACE_BYTES]);
    let read = ReadBatch {
        ops: vec![
            ReadOp::Get(Key::new(FIRST, b"point")),
            ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"prefix".to_vec(),
                after: None,
                limit: 2,
                max_bytes: FDB_LIMITS.max_key_bytes + FDB_LIMITS.max_value_bytes,
                delimiter: Some(b'/'),
            }),
        ],
    };
    validate_read(&codec, &read).unwrap();

    let write = WriteTxn {
        checks: vec![
            Check::Absent {
                key: Key::new(FIRST, b"new"),
            },
            Check::EmptyPrefix {
                keyspace: SECOND,
                prefix: b"empty".to_vec(),
            },
        ],
        mutations: vec![
            Mutation::Put {
                key: Key::new(FIRST, b"new"),
                value: b"value".to_vec(),
            },
            Mutation::Delete {
                key: Key::new(SECOND, b"old"),
            },
        ],
    };
    validate_write(&codec, &write).unwrap();

    assert!(matches!(
        ensure_observed_transaction_size((PHYSICAL_AFFECTED_BYTES + 1) as i64),
        Err(StoreError::LimitExceeded {
            kind: LimitKind::TransactionBytes,
            ..
        })
    ));
    assert!(matches!(
        ensure_observed_transaction_size(-1),
        Err(StoreError::Corrupt(_))
    ));

    let overflowing_scan = ReadBatch {
        ops: vec![ReadOp::Scan(Scan {
            keyspace: FIRST,
            prefix: Vec::new(),
            after: None,
            limit: usize::MAX,
            max_bytes: FDB_LIMITS.max_key_bytes + FDB_LIMITS.max_value_bytes,
            delimiter: None,
        })],
    };
    assert!(matches!(
        validate_read(&codec, &overflowing_scan),
        Err(StoreError::InvalidRequest(_))
    ));
}

#[test]
fn fdb_error_codes_preserve_conflict_unknown_and_limits() {
    assert_eq!(classify_error(1020, false), ErrorDisposition::Conflict);
    assert_eq!(classify_error(1021, false), ErrorDisposition::Unknown);
    assert_eq!(classify_error(1020, true), ErrorDisposition::Unknown);
    assert_eq!(
        classify_error(2101, false),
        ErrorDisposition::Limit(LimitKind::TransactionBytes)
    );
    assert_eq!(
        classify_error(2102, false),
        ErrorDisposition::Limit(LimitKind::KeyBytes)
    );
    assert_eq!(
        classify_error(2103, false),
        ErrorDisposition::Limit(LimitKind::ValueBytes)
    );
    assert_eq!(classify_error(1007, false), ErrorDisposition::Unavailable);
}
