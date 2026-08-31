/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

pub const NOT_COMMITTED: i32 = 1020;
pub const COMMIT_UNKNOWN_RESULT: i32 = 1021;
pub const TRANSACTION_TOO_LARGE: i32 = 2101;
pub const KEY_TOO_LARGE: i32 = 2102;
pub const VALUE_TOO_LARGE: i32 = 2103;

/// FoundationDB physical limit represented without a metadata-store dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdbLimit {
    TransactionBytes,
    KeyBytes,
    ValueBytes,
}

/// Stable disposition used by metadata and control adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdbErrorDisposition {
    Conflict,
    CommitUnknown,
    Limit(FdbLimit),
    Unavailable,
}

pub fn classify_error(code: i32, maybe_committed: bool) -> FdbErrorDisposition {
    if maybe_committed || code == COMMIT_UNKNOWN_RESULT {
        return FdbErrorDisposition::CommitUnknown;
    }
    match code {
        NOT_COMMITTED => FdbErrorDisposition::Conflict,
        TRANSACTION_TOO_LARGE => FdbErrorDisposition::Limit(FdbLimit::TransactionBytes),
        KEY_TOO_LARGE => FdbErrorDisposition::Limit(FdbLimit::KeyBytes),
        VALUE_TOO_LARGE => FdbErrorDisposition::Limit(FdbLimit::ValueBytes),
        _ => FdbErrorDisposition::Unavailable,
    }
}

/// One failed FoundationDB binding operation with commit ambiguity retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbOperationError {
    operation: &'static str,
    code: i32,
    message: String,
    maybe_committed: bool,
}

impl FdbOperationError {
    pub fn from_parts(
        operation: &'static str,
        code: i32,
        message: impl Into<String>,
        maybe_committed: bool,
    ) -> Self {
        Self {
            operation,
            code,
            message: message.into(),
            maybe_committed,
        }
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn maybe_committed(&self) -> bool {
        self.maybe_committed
    }

    pub fn disposition(&self) -> FdbErrorDisposition {
        classify_error(self.code, self.maybe_committed)
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn from_binding(operation: &'static str, error: foundationdb::FdbError) -> Self {
        Self::from_parts(
            operation,
            error.code(),
            error.message(),
            error.is_maybe_committed(),
        )
    }
}

impl fmt::Display for FdbOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failed with FoundationDB error {}: {}",
            self.operation, self.code, self.message
        )
    }
}

impl std::error::Error for FdbOperationError {}
