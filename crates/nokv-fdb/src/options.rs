/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(4);
const MIN_TRANSACTION_TIMEOUT: Duration = Duration::from_millis(1);
const MAX_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(4);

/// Connection settings shared by every NoKV FoundationDB adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbConnectionOptions {
    cluster_file: PathBuf,
    transaction_timeout: Duration,
}

impl FdbConnectionOptions {
    /// Select one explicit local FoundationDB cluster file.
    pub fn new(cluster_file: impl Into<PathBuf>) -> Self {
        Self {
            cluster_file: cluster_file.into(),
            transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
        }
    }

    /// Set the hard timeout installed on every newly created transaction.
    pub fn with_transaction_timeout(mut self, timeout: Duration) -> Self {
        self.transaction_timeout = timeout;
        self
    }

    pub fn cluster_file(&self) -> &Path {
        &self.cluster_file
    }

    pub fn transaction_timeout(&self) -> Duration {
        self.transaction_timeout
    }

    pub fn validate(&self) -> Result<(), FdbConfigError> {
        if !self.cluster_file.is_absolute() {
            return Err(FdbConfigError::ClusterFileNotAbsolute);
        }
        if self.cluster_file.file_name().is_none() {
            return Err(FdbConfigError::ClusterFileMissingName);
        }
        let cluster_file = self
            .cluster_file
            .to_str()
            .ok_or(FdbConfigError::ClusterFileNotUtf8)?;
        if cluster_file.as_bytes().contains(&0) {
            return Err(FdbConfigError::ClusterFileContainsNul);
        }
        if self.transaction_timeout < MIN_TRANSACTION_TIMEOUT
            || self.transaction_timeout > MAX_TRANSACTION_TIMEOUT
        {
            return Err(FdbConfigError::TransactionTimeoutOutsideBounds {
                actual_millis: self.transaction_timeout.as_millis(),
                minimum_millis: MIN_TRANSACTION_TIMEOUT.as_millis(),
                maximum_millis: MAX_TRANSACTION_TIMEOUT.as_millis(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn cluster_file_str(&self) -> &str {
        self.cluster_file
            .to_str()
            .expect("validated FoundationDB cluster file is UTF-8")
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn transaction_timeout_millis(&self) -> i32 {
        i32::try_from(self.transaction_timeout.as_millis())
            .expect("validated FoundationDB timeout fits i32 milliseconds")
    }
}

/// Invalid common FoundationDB configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FdbConfigError {
    ClusterFileNotAbsolute,
    ClusterFileMissingName,
    ClusterFileNotUtf8,
    ClusterFileContainsNul,
    TransactionTimeoutOutsideBounds {
        actual_millis: u128,
        minimum_millis: u128,
        maximum_millis: u128,
    },
    StorePrefixLength {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    ComponentLength {
        actual: usize,
        maximum: usize,
    },
}

impl fmt::Display for FdbConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterFileNotAbsolute => {
                formatter.write_str("FoundationDB cluster file must be absolute")
            }
            Self::ClusterFileMissingName => {
                formatter.write_str("FoundationDB cluster file must name a file")
            }
            Self::ClusterFileNotUtf8 => {
                formatter.write_str("FoundationDB cluster file must be valid UTF-8")
            }
            Self::ClusterFileContainsNul => {
                formatter.write_str("FoundationDB cluster file must not contain NUL bytes")
            }
            Self::TransactionTimeoutOutsideBounds {
                actual_millis,
                minimum_millis,
                maximum_millis,
            } => write!(
                formatter,
                "FoundationDB transaction timeout {actual_millis}ms is outside \
                 {minimum_millis}..={maximum_millis}ms"
            ),
            Self::StorePrefixLength {
                actual,
                minimum,
                maximum,
            } => write!(
                formatter,
                "FoundationDB store prefix has {actual} bytes, expected {minimum}..={maximum}"
            ),
            Self::ComponentLength { actual, maximum } => write!(
                formatter,
                "FoundationDB key component has {actual} bytes, maximum {maximum}"
            ),
        }
    }
}

impl std::error::Error for FdbConfigError {}
