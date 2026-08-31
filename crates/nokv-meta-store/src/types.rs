/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeSet;

use crate::{LimitKind, StoreError};

/// Opaque keyspace number assigned by the NoKV metadata schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keyspace(u16);

impl Keyspace {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

/// One raw key in a schema-owned keyspace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    pub keyspace: Keyspace,
    pub bytes: Vec<u8>,
}

impl Key {
    pub fn new(keyspace: Keyspace, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            keyspace,
            bytes: bytes.into(),
        }
    }
}

/// Durability boundary reached before `Commit::Applied` is returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckBoundary {
    /// A synchronous process-local durability boundary.
    LocalSync,
    /// A successful commit to a shared transactional store.
    SharedCommit,
    /// A quorum commit followed by apply on the serving replica.
    QuorumCommit,
}

/// Location of the authoritative state used for owner recovery.
///
/// Any successor admitted under this profile must first observe every commit
/// that reached [`Commit::Applied`] under the same authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Authority {
    /// One exclusive, persistent local namespace. A restarted owner may
    /// reattach only by reopening that same namespace and completing its
    /// WAL/schema/recovery-chain and owner-epoch admission checks.
    Local,
    /// One shared transactional namespace available to a successor.
    Shared,
    /// One replicated state machine that admits a successor after catch-up.
    Replicated,
}

/// Durable evidence used to recover a command that reached its acknowledgement
/// boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Each command transaction must include the local recovery receipt and
    /// journal material required to reopen the same exclusive authority.
    LocalJournal,
    /// The shared or replicated transaction store is itself the successor's
    /// recovery authority; domain commands do not write a second local journal.
    StoreAuthority,
}

/// Hard request and response limits for one store instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreLimits {
    /// Maximum operations in one consistent read batch.
    pub max_reads: usize,
    pub max_checks: usize,
    pub max_mutations: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    /// Maximum logical affected bytes from read keys and range endpoints.
    ///
    /// Adapters must reserve their physical keyspace-encoding overhead before
    /// they advertise this value.
    pub max_read_bytes: usize,
    pub max_transaction_bytes: usize,
    pub max_result_rows: usize,
    pub max_result_bytes: usize,
}

/// Store limits, planning target, acknowledgement boundary, and recovery
/// authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreProfile {
    pub limits: StoreLimits,
    /// Preferred logical affected-byte ceiling used by domain batch planners.
    ///
    /// The adapter still enforces `limits.max_transaction_bytes` as its hard
    /// request limit. This target must be nonzero and no greater than that hard
    /// limit.
    pub transaction_target_bytes: usize,
    pub ack: AckBoundary,
    pub authority: Authority,
    pub recovery: RecoveryMode,
}

/// Point and prefix reads that must share one consistent snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadBatch {
    pub ops: Vec<ReadOp>,
}

/// One operation in a consistent read batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadOp {
    Get(Key),
    Scan(Scan),
}

/// One ordered prefix scan.
///
/// `after` is an exclusive output-key cursor. A returned common prefix is a
/// valid cursor for the next page. An empty prefix selects the full keyspace.
/// When `delimiter` is set, the store folds keys at the first matching byte
/// after `prefix` and returns that byte in each common prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scan {
    pub keyspace: Keyspace,
    pub prefix: Vec<u8>,
    pub after: Option<Vec<u8>>,
    pub limit: usize,
    /// Maximum key and value bytes returned by this scan.
    ///
    /// This must allow one maximum-size row so a valid row cannot prevent
    /// cursor progress.
    pub max_bytes: usize,
    pub delimiter: Option<u8>,
}

/// Results aligned by position with the source read batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSnapshot {
    pub results: Vec<ReadResult>,
}

/// Result of one point or prefix read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadResult {
    Get(Option<Vec<u8>>),
    Scan(ScanPage),
}

/// One bounded scan result and its completeness signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage {
    pub items: Vec<ScanItem>,
    /// The store stopped before it proved that the prefix was exhausted.
    pub more: bool,
}

/// One raw row or delimiter-folded prefix in a scan page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScanItem {
    Row { key: Vec<u8>, value: Vec<u8> },
    CommonPrefix(Vec<u8>),
}

impl ScanItem {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Row { key, .. } | Self::CommonPrefix(key) => key,
        }
    }
}

/// Checks and mutations evaluated in one serializable transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteTxn {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

/// Condition that must hold before any mutation can apply.
///
/// The store compares exact raw values. If any check is false, it returns
/// [`Commit::Conflict`] and applies no mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Check {
    Value { key: Key, expected: Vec<u8> },
    Absent { key: Key },
    EmptyPrefix { keyspace: Keyspace, prefix: Vec<u8> },
}

/// One raw mutation in an atomic transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Put { key: Key, value: Vec<u8> },
    Delete { key: Key },
}

/// Definite result of a conditional commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Commit {
    /// Every mutation reached the profile's acknowledgement boundary and is
    /// visible to later reads on this store instance. Any admitted successor
    /// must also include this commit before it begins serving.
    Applied,
    /// At least one check was false and no mutation applied.
    Conflict,
}

impl ReadBatch {
    pub fn validate(&self, limits: &StoreLimits) -> Result<(), StoreError> {
        check_limit(LimitKind::Reads, self.ops.len(), limits.max_reads)?;

        let mut result_rows = 0_usize;
        let mut result_bytes = 0_usize;
        let mut read_bytes = 0_usize;
        let minimum_scan_bytes = add(
            limits.max_key_bytes,
            limits.max_value_bytes,
            LimitKind::ResultBytes,
        )?;
        for op in &self.ops {
            match op {
                ReadOp::Get(key) => {
                    check_key(&key.bytes, limits)?;
                    result_rows = add(result_rows, 1, LimitKind::ResultRows)?;
                    result_bytes =
                        add(result_bytes, limits.max_value_bytes, LimitKind::ResultBytes)?;
                    read_bytes = add(read_bytes, key.bytes.len(), LimitKind::ReadBytes)?;
                }
                ReadOp::Scan(scan) => {
                    if scan.limit == 0 {
                        return Err(StoreError::InvalidRequest(
                            "scan limit must be positive".to_owned(),
                        ));
                    }
                    if scan.max_bytes < minimum_scan_bytes {
                        return Err(StoreError::InvalidRequest(format!(
                            "scan byte limit must be at least {minimum_scan_bytes}"
                        )));
                    }
                    check_limit(
                        LimitKind::ResultBytes,
                        scan.max_bytes,
                        limits.max_result_bytes,
                    )?;
                    check_key(&scan.prefix, limits)?;
                    if let Some(after) = &scan.after {
                        check_key(after, limits)?;
                        if !is_canonical_output_key(scan, after) {
                            return Err(StoreError::InvalidRequest(
                                "scan cursor is not a canonical output key".to_owned(),
                            ));
                        }
                    }
                    result_rows = add(result_rows, scan.limit, LimitKind::ResultRows)?;
                    result_bytes = add(result_bytes, scan.max_bytes, LimitKind::ResultBytes)?;

                    let start_bytes = scan.after.as_ref().map_or(scan.prefix.len(), Vec::len);
                    let end_bytes = add(scan.prefix.len(), 1, LimitKind::ReadBytes)?;
                    let row_count = add(scan.limit, 1, LimitKind::ReadBytes)?;
                    let row_key_bytes =
                        row_count.checked_mul(limits.max_key_bytes).ok_or_else(|| {
                            StoreError::InvalidRequest("read byte count overflows usize".to_owned())
                        })?;
                    read_bytes = add(read_bytes, start_bytes, LimitKind::ReadBytes)?;
                    read_bytes = add(read_bytes, end_bytes, LimitKind::ReadBytes)?;
                    read_bytes = add(read_bytes, row_key_bytes, LimitKind::ReadBytes)?;
                }
            }
        }
        check_limit(LimitKind::ResultRows, result_rows, limits.max_result_rows)?;
        check_limit(
            LimitKind::ResultBytes,
            result_bytes,
            limits.max_result_bytes,
        )?;
        check_limit(LimitKind::ReadBytes, read_bytes, limits.max_read_bytes)
    }
}

impl ReadSnapshot {
    pub fn validate(&self, batch: &ReadBatch, limits: &StoreLimits) -> Result<(), StoreError> {
        batch.validate(limits)?;
        if self.results.len() != batch.ops.len() {
            return Err(StoreError::Corrupt(format!(
                "read result count {}, expected {}",
                self.results.len(),
                batch.ops.len()
            )));
        }

        let mut result_rows = 0_usize;
        let mut result_bytes = 0_usize;
        for (op, result) in batch.ops.iter().zip(&self.results) {
            match (op, result) {
                (ReadOp::Get(_), ReadResult::Get(value)) => {
                    if let Some(value) = value {
                        check_result_limit(
                            LimitKind::ValueBytes,
                            value.len(),
                            limits.max_value_bytes,
                        )?;
                        result_rows = add_result(result_rows, 1, LimitKind::ResultRows)?;
                        result_bytes =
                            add_result(result_bytes, value.len(), LimitKind::ResultBytes)?;
                    }
                }
                (ReadOp::Scan(scan), ReadResult::Scan(page)) => {
                    if page.more && page.items.is_empty() {
                        return Err(StoreError::Corrupt(
                            "scan page claims more rows without a continuation cursor".to_owned(),
                        ));
                    }
                    check_result_limit(LimitKind::ResultRows, page.items.len(), scan.limit)?;
                    validate_scan_items(scan, &page.items, limits)?;
                    result_rows = add_result(result_rows, page.items.len(), LimitKind::ResultRows)?;
                    let mut scan_bytes = 0_usize;
                    for item in &page.items {
                        scan_bytes =
                            add_result(scan_bytes, item.key().len(), LimitKind::ResultBytes)?;
                        if let ScanItem::Row { value, .. } = item {
                            scan_bytes =
                                add_result(scan_bytes, value.len(), LimitKind::ResultBytes)?;
                        }
                    }
                    check_result_limit(LimitKind::ResultBytes, scan_bytes, scan.max_bytes)?;
                    result_bytes = add_result(result_bytes, scan_bytes, LimitKind::ResultBytes)?;
                }
                _ => {
                    return Err(StoreError::Corrupt(
                        "read result kind does not match its request".to_owned(),
                    ));
                }
            }
        }

        check_result_limit(LimitKind::ResultRows, result_rows, limits.max_result_rows)?;
        check_result_limit(
            LimitKind::ResultBytes,
            result_bytes,
            limits.max_result_bytes,
        )
    }
}

impl WriteTxn {
    pub fn validate(&self, limits: &StoreLimits) -> Result<(), StoreError> {
        check_limit(LimitKind::Checks, self.checks.len(), limits.max_checks)?;
        check_limit(
            LimitKind::Mutations,
            self.mutations.len(),
            limits.max_mutations,
        )?;

        let mut bytes = 0_usize;
        let mut exact_checks = BTreeSet::new();
        for check in &self.checks {
            match check {
                Check::Value { key, expected } => {
                    check_exact_check(&mut exact_checks, key)?;
                    check_key(&key.bytes, limits)?;
                    check_limit(
                        LimitKind::ValueBytes,
                        expected.len(),
                        limits.max_value_bytes,
                    )?;
                    bytes = add(bytes, key.bytes.len(), LimitKind::TransactionBytes)?;
                    bytes = add(bytes, expected.len(), LimitKind::TransactionBytes)?;
                }
                Check::Absent { key } => {
                    check_exact_check(&mut exact_checks, key)?;
                    check_key(&key.bytes, limits)?;
                    bytes = add(bytes, key.bytes.len(), LimitKind::TransactionBytes)?;
                }
                Check::EmptyPrefix { prefix, .. } => {
                    check_key(prefix, limits)?;
                    let end_bytes = add(prefix.len(), 1, LimitKind::TransactionBytes)?;
                    bytes = add(bytes, prefix.len(), LimitKind::TransactionBytes)?;
                    bytes = add(bytes, end_bytes, LimitKind::TransactionBytes)?;
                    bytes = add(bytes, limits.max_key_bytes, LimitKind::TransactionBytes)?;
                }
            }
        }

        let mut mutation_keys = BTreeSet::new();
        for mutation in &self.mutations {
            let key = match mutation {
                Mutation::Put { key, value } => {
                    check_limit(LimitKind::ValueBytes, value.len(), limits.max_value_bytes)?;
                    bytes = add(bytes, value.len(), LimitKind::TransactionBytes)?;
                    key
                }
                Mutation::Delete { key } => key,
            };
            check_key(&key.bytes, limits)?;
            bytes = add(bytes, key.bytes.len(), LimitKind::TransactionBytes)?;
            if !mutation_keys.insert((key.keyspace, key.bytes.as_slice())) {
                return Err(StoreError::InvalidRequest(
                    "transaction contains duplicate mutation keys".to_owned(),
                ));
            }
        }

        check_limit(
            LimitKind::TransactionBytes,
            bytes,
            limits.max_transaction_bytes,
        )
    }
}

fn check_exact_check<'a>(
    keys: &mut BTreeSet<(Keyspace, &'a [u8])>,
    key: &'a Key,
) -> Result<(), StoreError> {
    if keys.insert((key.keyspace, &key.bytes)) {
        Ok(())
    } else {
        Err(StoreError::InvalidRequest(
            "transaction contains duplicate exact checks".to_owned(),
        ))
    }
}

fn is_canonical_output_key(scan: &Scan, key: &[u8]) -> bool {
    if !key.starts_with(&scan.prefix) {
        return false;
    }
    let Some(delimiter) = scan.delimiter else {
        return true;
    };
    let suffix = &key[scan.prefix.len()..];
    suffix
        .iter()
        .position(|byte| *byte == delimiter)
        .is_none_or(|offset| offset + 1 == suffix.len())
}

fn validate_scan_items(
    scan: &Scan,
    items: &[ScanItem],
    limits: &StoreLimits,
) -> Result<(), StoreError> {
    let mut previous = scan.after.as_deref();
    for item in items {
        let key = item.key();
        check_result_limit(LimitKind::KeyBytes, key.len(), limits.max_key_bytes)?;
        if !key.starts_with(&scan.prefix) {
            return Err(StoreError::Corrupt(
                "scan returned a key outside the requested prefix".to_owned(),
            ));
        }
        if previous.is_some_and(|previous| key <= previous) {
            return Err(StoreError::Corrupt(
                "scan results are not in strict cursor order".to_owned(),
            ));
        }

        let suffix = &key[scan.prefix.len()..];
        match (item, scan.delimiter) {
            (ScanItem::Row { value, .. }, delimiter) => {
                check_result_limit(LimitKind::ValueBytes, value.len(), limits.max_value_bytes)?;
                if delimiter.is_some_and(|delimiter| suffix.contains(&delimiter)) {
                    return Err(StoreError::Corrupt(
                        "scan returned an unfolded row after the delimiter".to_owned(),
                    ));
                }
            }
            (ScanItem::CommonPrefix(_), None) => {
                return Err(StoreError::Corrupt(
                    "scan returned a common prefix without a delimiter".to_owned(),
                ));
            }
            (ScanItem::CommonPrefix(_), Some(delimiter)) => {
                if suffix.last().copied() != Some(delimiter)
                    || suffix[..suffix.len() - 1].contains(&delimiter)
                {
                    return Err(StoreError::Corrupt(
                        "scan returned a non-canonical common prefix".to_owned(),
                    ));
                }
            }
        }
        previous = Some(key);
    }
    Ok(())
}

fn check_key(bytes: &[u8], limits: &StoreLimits) -> Result<(), StoreError> {
    check_limit(LimitKind::KeyBytes, bytes.len(), limits.max_key_bytes)
}

fn check_limit(kind: LimitKind, actual: usize, maximum: usize) -> Result<(), StoreError> {
    if actual <= maximum {
        Ok(())
    } else {
        Err(StoreError::LimitExceeded {
            kind,
            actual,
            maximum,
        })
    }
}

fn check_result_limit(kind: LimitKind, actual: usize, maximum: usize) -> Result<(), StoreError> {
    if actual <= maximum {
        Ok(())
    } else {
        Err(StoreError::Corrupt(format!(
            "read result has {actual} {kind}, maximum {maximum}"
        )))
    }
}

fn add(current: usize, amount: usize, kind: LimitKind) -> Result<usize, StoreError> {
    current
        .checked_add(amount)
        .ok_or_else(|| StoreError::InvalidRequest(format!("{kind} count overflows usize")))
}

fn add_result(current: usize, amount: usize, kind: LimitKind) -> Result<usize, StoreError> {
    current
        .checked_add(amount)
        .ok_or_else(|| StoreError::Corrupt(format!("read result {kind} count overflows usize")))
}
