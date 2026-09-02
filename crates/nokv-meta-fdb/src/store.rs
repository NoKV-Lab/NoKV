/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_fdb::{
    lexicographic_successor, FdbDatabase, FdbErrorDisposition, FdbLimit, FdbOpenError,
    FdbOperationError, FdbRangeRequest, FdbRuntime, FdbTransaction,
};
use nokv_meta_store::{
    Check, Commit, Keyspace, LimitKind, Mutation, ReadBatch, ReadOp, ReadResult, ReadSnapshot,
    Scan, ScanItem, ScanPage, StoreError, StoreLimits, StoreProfile, TxnStore, UnknownCommit,
    WriteTxn,
};

use crate::affected_bytes::{
    ensure_observed_transaction_size, validate_read, validate_write, write_affected_bytes,
};
use crate::codec::KeyCodec;
use crate::diagnostics::{FdbStoreDiagnosticCounters, FdbStoreDiagnostics};
use crate::profile::{FDB_LIMITS, FDB_PHYSICAL_TRANSACTION_GUARD_BYTES, FDB_PROFILE};
use crate::{FdbMetadataSessionFence, FdbOptions};

/// FoundationDB implementation of the storage-neutral metadata transaction contract.
///
/// It is selected only by the feature-gated FDB serving candidate; source
/// wiring alone is not FoundationDB serving qualification.
pub struct FdbStore {
    database: FdbDatabase,
    codec: KeyCodec,
    session_fence: FdbMetadataSessionFence,
    diagnostics: FdbStoreDiagnosticCounters,
}

impl FdbStore {
    pub fn open(runtime: &FdbRuntime, options: FdbOptions) -> Result<Self, StoreError> {
        options.validate()?;
        let database =
            FdbDatabase::open(runtime, options.connection_options()).map_err(map_open_error)?;
        let codec = KeyCodec::new(options.namespace())?;
        let store = Self {
            database,
            codec,
            session_fence: options.session_fence().clone(),
            diagnostics: FdbStoreDiagnosticCounters::default(),
        };
        store.ready()?;
        Ok(store)
    }

    fn transaction(&self) -> Result<FdbTransaction, StoreError> {
        let transaction = self.database.transaction().map_err(map_operation_error)?;
        Ok(transaction)
    }

    fn read_inner(&self, batch: &ReadBatch) -> Result<ReadSnapshot, StoreError> {
        let transaction = self.transaction()?;
        self.require_current_session(&transaction)?;
        let mut results = Vec::with_capacity(batch.ops.len());
        for op in &batch.ops {
            match op {
                ReadOp::Get(key) => {
                    let physical_key = self.codec.encode_key(key);
                    let value = transaction
                        .get(&physical_key, true)
                        .map_err(map_operation_error)?;
                    results.push(ReadResult::Get(value));
                }
                ReadOp::Scan(scan) => {
                    results.push(ReadResult::Scan(scan_page(
                        &transaction,
                        &self.codec,
                        scan,
                        &FDB_LIMITS,
                    )?));
                }
            }
        }
        let snapshot = ReadSnapshot { results };
        snapshot.validate(batch, &FDB_LIMITS)?;
        Ok(snapshot)
    }

    fn commit_inner(&self, txn: &WriteTxn) -> Result<Commit, StoreError> {
        self.diagnostics.record_attempt();
        let result = (|| {
            let transaction = self.transaction()?;
            self.require_current_session(&transaction)?;
            for check in &txn.checks {
                let matches = match check {
                    Check::Value { key, expected } => {
                        let physical_key = self.codec.encode_key(key);
                        transaction
                            .get(&physical_key, false)
                            .map_err(map_operation_error)?
                            .as_deref()
                            == Some(expected.as_slice())
                    }
                    Check::Absent { key } => {
                        let physical_key = self.codec.encode_key(key);
                        transaction
                            .get(&physical_key, false)
                            .map_err(map_operation_error)?
                            .is_none()
                    }
                    Check::EmptyPrefix { keyspace, prefix } => {
                        prefix_is_empty(&transaction, &self.codec, *keyspace, prefix)?
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
                .approximate_size()
                .map_err(map_operation_error)?;
            self.diagnostics.record_approximate_size(observed_size);
            if let Err(error) = ensure_observed_transaction_size(observed_size) {
                self.diagnostics.record_physical_guard_rejection();
                return Err(error);
            }

            match transaction.commit() {
                Ok(_) => Ok(Commit::Applied),
                Err(error) => self.map_commit_error(error),
            }
        })();
        self.diagnostics.record_result(&result);
        result
    }

    fn require_current_session(&self, transaction: &FdbTransaction) -> Result<(), StoreError> {
        let current = transaction
            .get(self.session_fence.key(), false)
            .map_err(map_operation_error)?;
        if current.as_deref() == Some(self.session_fence.expected_value()) {
            Ok(())
        } else {
            Err(self.fenced())
        }
    }

    fn verify_current_session(&self) -> Result<(), StoreError> {
        let transaction = self.transaction()?;
        self.require_current_session(&transaction)
    }

    fn fenced(&self) -> StoreError {
        StoreError::Fenced {
            expected_owner_epoch: self.session_fence.expected_owner_epoch(),
            expected_session_generation: self.session_fence.expected_session_generation(),
        }
    }

    fn map_commit_error(&self, error: FdbOperationError) -> Result<Commit, StoreError> {
        match error.disposition() {
            FdbErrorDisposition::Conflict => {
                // A takeover can race after the in-transaction session read.
                // Re-read only the stable session key in a fresh transaction
                // so that ownership loss is never reported as a domain check
                // conflict. This is not a raw commit retry.
                self.verify_current_session()?;
                Ok(Commit::Conflict)
            }
            FdbErrorDisposition::CommitUnknown => Err(StoreError::OutcomeUnknown {
                state: UnknownCommit::MayCommit,
                reason: error.to_string(),
            }),
            FdbErrorDisposition::Limit(kind) => Err(limit_error(kind)),
            FdbErrorDisposition::Unavailable => Err(StoreError::Unavailable(error.to_string())),
        }
    }

    /// Return a read-only snapshot of this handle's transaction diagnostics.
    pub fn diagnostics(&self) -> FdbStoreDiagnostics {
        self.diagnostics.snapshot()
    }

    /// Estimate the conservative physical affected-byte envelope used by the
    /// adapter before a write transaction is dispatched.
    pub fn conservative_affected_bytes(&self, txn: &WriteTxn) -> Result<usize, StoreError> {
        txn.validate(&FDB_LIMITS)?;
        write_affected_bytes(&self.codec, &self.session_fence, txn)
    }
}

impl TxnStore for FdbStore {
    fn profile(&self) -> StoreProfile {
        FDB_PROFILE
    }

    fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
        batch.validate(&FDB_LIMITS)?;
        validate_read(&self.codec, &self.session_fence, &batch)?;
        self.read_inner(&batch)
    }

    fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
        txn.validate(&FDB_LIMITS)?;
        validate_write(&self.codec, &self.session_fence, &txn)?;
        self.commit_inner(&txn)
    }

    fn ready(&self) -> Result<(), StoreError> {
        let transaction = self.transaction()?;
        self.require_current_session(&transaction)?;
        transaction.read_version().map_err(map_operation_error)?;
        Ok(())
    }
}

fn prefix_is_empty(
    transaction: &FdbTransaction,
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
    let values = transaction
        .get_range(&FdbRangeRequest {
            begin,
            end,
            limit: Some(1),
            target_bytes: 0,
            iteration: 1,
            snapshot: false,
            reverse: false,
        })
        .map_err(map_operation_error)?;
    Ok(values.items.is_empty())
}

fn scan_page(
    transaction: &FdbTransaction,
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
        let values = transaction
            .get_range(&FdbRangeRequest {
                begin: begin.clone(),
                end: end.clone(),
                limit: Some(fetch_limit),
                target_bytes: scan.max_bytes,
                iteration: 1,
                snapshot: true,
                reverse: false,
            })
            .map_err(map_operation_error)?;
        if values.items.is_empty() {
            break;
        }

        let batch_has_more = values.more;
        let mut next_begin = None;
        let mut skipped_common_prefix = false;
        for value in &values.items {
            let logical_key = codec.decode_key(scan.keyspace, &value.key)?;
            if !logical_key.starts_with(scan.prefix.as_slice()) {
                return Err(StoreError::Corrupt(
                    "FoundationDB returned a key outside the requested logical prefix".to_owned(),
                ));
            }
            validate_row(&logical_key, &value.value, limits)?;
            let item = fold_scan_item(scan, logical_key, &value.value);
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

            let mut row_successor = value.key.clone();
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

fn map_open_error(error: FdbOpenError) -> StoreError {
    match error {
        FdbOpenError::Config(error) => StoreError::InvalidRequest(error.to_string()),
        FdbOpenError::Operation(error) => map_operation_error(error),
    }
}

fn map_operation_error(error: FdbOperationError) -> StoreError {
    match error.disposition() {
        FdbErrorDisposition::Limit(kind) => limit_error(kind),
        FdbErrorDisposition::Conflict
        | FdbErrorDisposition::CommitUnknown
        | FdbErrorDisposition::Unavailable => StoreError::Unavailable(error.to_string()),
    }
}

fn limit_error(fdb_kind: FdbLimit) -> StoreError {
    let (kind, maximum) = match fdb_kind {
        FdbLimit::KeyBytes => (LimitKind::KeyBytes, FDB_LIMITS.max_key_bytes),
        FdbLimit::ValueBytes => (LimitKind::ValueBytes, FDB_LIMITS.max_value_bytes),
        FdbLimit::TransactionBytes => (
            LimitKind::TransactionBytes,
            FDB_PHYSICAL_TRANSACTION_GUARD_BYTES,
        ),
    };
    StoreError::LimitExceeded {
        kind,
        actual: maximum.saturating_add(1),
        maximum,
    }
}
