/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use foundationdb::options::{StreamingMode, TransactionOption};
use foundationdb::{Database, FdbError, RangeOption, Transaction};
use futures::executor::block_on;
use nokv_meta_store::{
    Check, Commit, Keyspace, LimitKind, Mutation, ReadBatch, ReadOp, ReadResult, ReadSnapshot,
    Scan, ScanItem, ScanPage, StoreError, StoreLimits, StoreProfile, TxnStore, UnknownCommit,
    WriteTxn,
};

use crate::affected_bytes::{ensure_observed_transaction_size, validate_read, validate_write};
use crate::codec::{lexicographic_successor, KeyCodec};
use crate::errors::{classify_error, ErrorDisposition};
use crate::profile::{FDB_LIMITS, FDB_PROFILE, PHYSICAL_AFFECTED_BYTES};
use crate::FdbOptions;

/// FoundationDB implementation of the storage-neutral metadata transaction contract.
///
/// This characterization adapter is intentionally not wired into NoKV serving.
pub struct FdbStore {
    database: Database,
    options: FdbOptions,
    codec: KeyCodec,
}

impl FdbStore {
    pub fn open(options: FdbOptions) -> Result<Self, StoreError> {
        options.validate()?;
        let database = Database::from_path(options.cluster_file_str())
            .map_err(|error| map_operation_error("open FoundationDB database", error))?;
        let codec = KeyCodec::new(options.namespace());
        Ok(Self {
            database,
            options,
            codec,
        })
    }

    fn transaction(&self) -> Result<Transaction, StoreError> {
        let transaction = self
            .database
            .create_trx()
            .map_err(|error| map_operation_error("create FoundationDB transaction", error))?;
        transaction
            .set_option(TransactionOption::Timeout(
                self.options.transaction_timeout_millis(),
            ))
            .map_err(|error| map_operation_error("set FoundationDB transaction timeout", error))?;
        Ok(transaction)
    }

    async fn read_async(&self, batch: &ReadBatch) -> Result<ReadSnapshot, StoreError> {
        let transaction = self.transaction()?;
        let mut results = Vec::with_capacity(batch.ops.len());
        for op in &batch.ops {
            match op {
                ReadOp::Get(key) => {
                    let physical_key = self.codec.encode_key(key);
                    let value = transaction
                        .get(&physical_key, true)
                        .await
                        .map_err(|error| map_operation_error("read FoundationDB key", error))?
                        .map(|value| value.as_ref().to_vec());
                    results.push(ReadResult::Get(value));
                }
                ReadOp::Scan(scan) => {
                    results.push(ReadResult::Scan(
                        scan_page(&transaction, &self.codec, scan, &FDB_LIMITS).await?,
                    ));
                }
            }
        }
        let snapshot = ReadSnapshot { results };
        snapshot.validate(batch, &FDB_LIMITS)?;
        Ok(snapshot)
    }

    async fn commit_async(&self, txn: &WriteTxn) -> Result<Commit, StoreError> {
        let transaction = self.transaction()?;
        for check in &txn.checks {
            let matches = match check {
                Check::Value { key, expected } => {
                    let physical_key = self.codec.encode_key(key);
                    transaction
                        .get(&physical_key, false)
                        .await
                        .map_err(|error| {
                            map_operation_error("evaluate FoundationDB value check", error)
                        })?
                        .as_deref()
                        == Some(expected.as_slice())
                }
                Check::Absent { key } => {
                    let physical_key = self.codec.encode_key(key);
                    transaction
                        .get(&physical_key, false)
                        .await
                        .map_err(|error| {
                            map_operation_error("evaluate FoundationDB absent check", error)
                        })?
                        .is_none()
                }
                Check::EmptyPrefix { keyspace, prefix } => {
                    prefix_is_empty(&transaction, &self.codec, *keyspace, prefix).await?
                }
            };
            if !matches {
                return Ok(Commit::Conflict);
            }
        }

        for mutation in &txn.mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    transaction.set(&self.codec.encode_key(key), value);
                }
                Mutation::Delete { key } => {
                    transaction.clear(&self.codec.encode_key(key));
                }
            }
        }

        let observed_size = transaction
            .get_approximate_size()
            .await
            .map_err(|error| map_operation_error("measure FoundationDB transaction size", error))?;
        ensure_observed_transaction_size(observed_size)?;

        match transaction.commit().await {
            Ok(_) => Ok(Commit::Applied),
            Err(error) => map_commit_error(error.into()),
        }
    }
}

impl TxnStore for FdbStore {
    fn profile(&self) -> StoreProfile {
        FDB_PROFILE
    }

    fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
        batch.validate(&FDB_LIMITS)?;
        validate_read(&self.codec, &batch)?;
        block_on(self.read_async(&batch))
    }

    fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
        txn.validate(&FDB_LIMITS)?;
        validate_write(&self.codec, &txn)?;
        block_on(self.commit_async(&txn))
    }

    fn ready(&self) -> Result<(), StoreError> {
        block_on(async {
            self.transaction()?
                .get_read_version()
                .await
                .map_err(|error| map_operation_error("obtain FoundationDB read version", error))?;
            Ok(())
        })
    }
}

async fn prefix_is_empty(
    transaction: &Transaction,
    codec: &KeyCodec,
    keyspace: Keyspace,
    prefix: &[u8],
) -> Result<bool, StoreError> {
    let scan = Scan {
        keyspace,
        prefix: prefix.to_vec(),
        after: None,
        limit: 1,
        max_bytes: FDB_LIMITS.max_key_bytes + FDB_LIMITS.max_value_bytes,
        delimiter: None,
    };
    let (begin, end) = codec.scan_bounds(&scan)?;
    let mut options = RangeOption::from((begin, end));
    options.limit = Some(1);
    options.mode = StreamingMode::WantAll;
    let values = transaction
        .get_range(&options, 1, false)
        .await
        .map_err(|error| map_operation_error("evaluate FoundationDB prefix check", error))?;
    Ok(values.is_empty())
}

async fn scan_page(
    transaction: &Transaction,
    codec: &KeyCodec,
    scan: &Scan,
    limits: &StoreLimits,
) -> Result<ScanPage, StoreError> {
    let (mut begin, end) = codec.scan_bounds(scan)?;
    let mut items = Vec::with_capacity(scan.limit);
    let mut result_bytes = 0_usize;

    while begin < end {
        let remaining = scan.limit.saturating_sub(items.len());
        let fetch_limit = if scan.delimiter.is_some() {
            1
        } else {
            remaining.saturating_add(1)
        };
        let mut options = RangeOption::from((begin.clone(), end.clone()));
        options.limit = Some(fetch_limit);
        options.target_bytes = scan.max_bytes;
        options.mode = StreamingMode::WantAll;
        let values = transaction
            .get_range(&options, 1, true)
            .await
            .map_err(|error| map_operation_error("scan FoundationDB range", error))?;
        if values.is_empty() {
            break;
        }

        let batch_has_more = values.more();
        let mut next_begin = None;
        let mut skipped_common_prefix = false;
        for value in &values {
            let logical_key = codec.decode_key(scan.keyspace, value.key())?;
            if !logical_key.starts_with(scan.prefix.as_slice()) {
                return Err(StoreError::Corrupt(
                    "FoundationDB returned a key outside the requested logical prefix".to_owned(),
                ));
            }
            validate_row(&logical_key, value.value(), limits)?;
            let item = fold_scan_item(scan, logical_key, value.value());
            let item_bytes = scan_item_bytes(&item)?;
            let next_bytes = result_bytes.checked_add(item_bytes).ok_or_else(|| {
                StoreError::Corrupt(
                    "FoundationDB scan result byte count overflows usize".to_owned(),
                )
            })?;
            if items.len() == scan.limit || next_bytes > scan.max_bytes {
                if items.is_empty() {
                    return Err(StoreError::Corrupt(
                        "FoundationDB row exceeds the advertised key or value limit".to_owned(),
                    ));
                }
                return Ok(ScanPage { items, more: true });
            }

            let common_prefix = match &item {
                ScanItem::CommonPrefix(prefix) => Some(prefix.clone()),
                ScanItem::Row { .. } => None,
            };
            result_bytes = next_bytes;
            items.push(item);

            if let Some(common_prefix) = common_prefix {
                next_begin = Some(
                    lexicographic_successor(&codec.encode(scan.keyspace, &common_prefix))
                        .ok_or_else(|| {
                            StoreError::Corrupt(
                                "FoundationDB common prefix has no successor".to_owned(),
                            )
                        })?,
                );
                skipped_common_prefix = true;
                break;
            }

            let mut row_successor = value.key().to_vec();
            row_successor.push(0);
            next_begin = Some(row_successor);
        }

        let Some(candidate_begin) = next_begin else {
            return Err(StoreError::Corrupt(
                "FoundationDB returned a nonempty range without a continuation key".to_owned(),
            ));
        };
        begin = candidate_begin;
        if !skipped_common_prefix && !batch_has_more {
            break;
        }
    }

    Ok(ScanPage { items, more: false })
}

fn fold_scan_item(scan: &Scan, key: Vec<u8>, value: &[u8]) -> ScanItem {
    if let Some(delimiter) = scan.delimiter {
        let suffix = &key[scan.prefix.len()..];
        if let Some(offset) = suffix.iter().position(|byte| *byte == delimiter) {
            let prefix_end = scan.prefix.len() + offset + 1;
            return ScanItem::CommonPrefix(key[..prefix_end].to_vec());
        }
    }
    ScanItem::Row {
        key,
        value: value.to_vec(),
    }
}

fn validate_row(key: &[u8], value: &[u8], limits: &StoreLimits) -> Result<(), StoreError> {
    if key.len() > limits.max_key_bytes {
        return Err(StoreError::Corrupt(format!(
            "FoundationDB row has {} logical key bytes, maximum {}",
            key.len(),
            limits.max_key_bytes
        )));
    }
    if value.len() > limits.max_value_bytes {
        return Err(StoreError::Corrupt(format!(
            "FoundationDB row has {} value bytes, maximum {}",
            value.len(),
            limits.max_value_bytes
        )));
    }
    Ok(())
}

fn scan_item_bytes(item: &ScanItem) -> Result<usize, StoreError> {
    match item {
        ScanItem::Row { key, value } => key.len().checked_add(value.len()).ok_or_else(|| {
            StoreError::Corrupt("FoundationDB scan row byte count overflows usize".to_owned())
        }),
        ScanItem::CommonPrefix(prefix) => Ok(prefix.len()),
    }
}

fn map_commit_error(error: FdbError) -> Result<Commit, StoreError> {
    match classify_error(error.code(), error.is_maybe_committed()) {
        ErrorDisposition::Conflict => Ok(Commit::Conflict),
        ErrorDisposition::Unknown => Err(StoreError::OutcomeUnknown {
            state: UnknownCommit::MayCommit,
            reason: fdb_reason("commit FoundationDB transaction", error),
        }),
        ErrorDisposition::Limit(kind) => Err(limit_error(kind)),
        ErrorDisposition::Unavailable => Err(StoreError::Unavailable(fdb_reason(
            "commit FoundationDB transaction",
            error,
        ))),
    }
}

fn map_operation_error(operation: &str, error: FdbError) -> StoreError {
    match classify_error(error.code(), false) {
        ErrorDisposition::Limit(kind) => limit_error(kind),
        ErrorDisposition::Conflict | ErrorDisposition::Unknown | ErrorDisposition::Unavailable => {
            StoreError::Unavailable(fdb_reason(operation, error))
        }
    }
}

fn limit_error(kind: LimitKind) -> StoreError {
    let maximum = match kind {
        LimitKind::KeyBytes => FDB_LIMITS.max_key_bytes,
        LimitKind::ValueBytes => FDB_LIMITS.max_value_bytes,
        LimitKind::TransactionBytes => PHYSICAL_AFFECTED_BYTES,
        LimitKind::ReadBytes => PHYSICAL_AFFECTED_BYTES,
        LimitKind::Reads
        | LimitKind::Checks
        | LimitKind::Mutations
        | LimitKind::ResultRows
        | LimitKind::ResultBytes => PHYSICAL_AFFECTED_BYTES,
    };
    StoreError::LimitExceeded {
        kind,
        actual: maximum.saturating_add(1),
        maximum,
    }
}

fn fdb_reason(operation: &str, error: FdbError) -> String {
    format!(
        "{operation} failed with FoundationDB error {}: {}",
        error.code(),
        error.message()
    )
}
