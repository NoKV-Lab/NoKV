/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta_store::{Check, LimitKind, Mutation, ReadBatch, ReadOp, StoreError, WriteTxn};

use crate::codec::KeyCodec;
use crate::profile::{FDB_LIMITS, FDB_PHYSICAL_TRANSACTION_GUARD_BYTES, FDB_SESSION_FENCE_READS};
use crate::FdbMetadataSessionFence;

pub(crate) fn validate_read(
    codec: &KeyCodec,
    session_fence: &FdbMetadataSessionFence,
    batch: &ReadBatch,
) -> Result<(), StoreError> {
    let mut bytes = session_fence_affected_bytes(session_fence)?;
    for op in &batch.ops {
        match op {
            ReadOp::Get(key) => {
                let encoded = codec.encoded_len(key.bytes.len())?;
                bytes = add(bytes, point_range_bytes(encoded)?)?;
            }
            ReadOp::Scan(scan) => {
                let (begin, end) = codec.scan_bounds(scan)?;
                bytes = add(bytes, begin.len())?;
                bytes = add(bytes, end.len())?;
                let rows = scan.limit.checked_add(1).ok_or_else(overflow)?;
                let encoded_row = codec.encoded_len(FDB_LIMITS.max_key_bytes)?;
                bytes = add(bytes, multiply(rows, encoded_row)?)?;
            }
        }
    }
    ensure_budget(LimitKind::ReadBytes, bytes)
}

pub(crate) fn validate_write(
    codec: &KeyCodec,
    session_fence: &FdbMetadataSessionFence,
    txn: &WriteTxn,
) -> Result<(), StoreError> {
    ensure_budget(
        LimitKind::TransactionBytes,
        write_affected_bytes(codec, session_fence, txn)?,
    )
}

pub(crate) fn write_affected_bytes(
    codec: &KeyCodec,
    session_fence: &FdbMetadataSessionFence,
    txn: &WriteTxn,
) -> Result<usize, StoreError> {
    let mut bytes = session_fence_affected_bytes(session_fence)?;
    for check in &txn.checks {
        match check {
            Check::Value { key, .. } | Check::Absent { key } => {
                let encoded = codec.encoded_len(key.bytes.len())?;
                bytes = add(bytes, point_range_bytes(encoded)?)?;
            }
            Check::EmptyPrefix { keyspace, prefix } => {
                let scan = nokv_meta_store::Scan {
                    keyspace: *keyspace,
                    prefix: prefix.clone(),
                    after: None,
                    limit: 1,
                    max_bytes: FDB_LIMITS.max_key_bytes + FDB_LIMITS.max_value_bytes,
                    delimiter: None,
                };
                let (begin, end) = codec.scan_bounds(&scan)?;
                bytes = add(bytes, begin.len())?;
                bytes = add(bytes, end.len())?;
                bytes = add(bytes, codec.encoded_len(FDB_LIMITS.max_key_bytes)?)?;
            }
        }
    }
    for mutation in &txn.mutations {
        match mutation {
            Mutation::Put { key, value } => {
                let encoded = codec.encoded_len(key.bytes.len())?;
                bytes = add(bytes, write_range_bytes(encoded)?)?;
                bytes = add(bytes, value.len())?;
            }
            Mutation::Delete { key } => {
                let encoded = codec.encoded_len(key.bytes.len())?;
                bytes = add(bytes, write_range_bytes(encoded)?)?;
            }
        }
    }
    Ok(bytes)
}

pub(crate) fn ensure_observed_transaction_size(bytes: i64) -> Result<(), StoreError> {
    let actual = usize::try_from(bytes).map_err(|_| {
        StoreError::Corrupt(format!(
            "FoundationDB returned invalid approximate transaction size {bytes}"
        ))
    })?;
    ensure_budget(LimitKind::TransactionBytes, actual)
}

pub(crate) fn session_fence_affected_bytes(
    session_fence: &FdbMetadataSessionFence,
) -> Result<usize, StoreError> {
    add(
        multiply(
            FDB_SESSION_FENCE_READS,
            point_range_bytes(session_fence.key().len())?,
        )?,
        session_fence.expected_value().len(),
    )
}

fn point_range_bytes(encoded: usize) -> Result<usize, StoreError> {
    multiply(encoded, 2).and_then(|bytes| add(bytes, 1))
}

fn write_range_bytes(encoded: usize) -> Result<usize, StoreError> {
    multiply(encoded, 3).and_then(|bytes| add(bytes, 1))
}

fn ensure_budget(kind: LimitKind, actual: usize) -> Result<(), StoreError> {
    if actual <= FDB_PHYSICAL_TRANSACTION_GUARD_BYTES {
        Ok(())
    } else {
        Err(StoreError::LimitExceeded {
            kind,
            actual,
            maximum: FDB_PHYSICAL_TRANSACTION_GUARD_BYTES,
        })
    }
}

fn add(left: usize, right: usize) -> Result<usize, StoreError> {
    left.checked_add(right).ok_or_else(overflow)
}

fn multiply(left: usize, right: usize) -> Result<usize, StoreError> {
    left.checked_mul(right).ok_or_else(overflow)
}

fn overflow() -> StoreError {
    StoreError::InvalidRequest("FdbStore affected-byte estimate overflows usize".to_owned())
}
