/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(any(feature = "fdb", test))]
use nokv_meta_store::StoreError;

#[cfg(any(feature = "fdb", test))]
pub(crate) const MAX_NAMESPACE_BYTES: usize = 64;
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(4);
#[cfg(any(feature = "fdb", test))]
const MIN_TRANSACTION_TIMEOUT: Duration = Duration::from_millis(1);
#[cfg(any(feature = "fdb", test))]
const MAX_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(4);

/// Physical FoundationDB connection and namespace options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbOptions {
    cluster_file: PathBuf,
    namespace: Vec<u8>,
    transaction_timeout: Duration,
}

impl FdbOptions {
    /// Configure one explicit cluster file and one binary application namespace.
    pub fn new(cluster_file: impl Into<PathBuf>, namespace: impl Into<Vec<u8>>) -> Self {
        Self {
            cluster_file: cluster_file.into(),
            namespace: namespace.into(),
            transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
        }
    }

    /// Set the hard FoundationDB transaction timeout for every adapter call.
    pub fn with_transaction_timeout(mut self, timeout: Duration) -> Self {
        self.transaction_timeout = timeout;
        self
    }

    pub fn cluster_file(&self) -> &Path {
        &self.cluster_file
    }

    pub fn namespace(&self) -> &[u8] {
        &self.namespace
    }

    pub fn transaction_timeout(&self) -> Duration {
        self.transaction_timeout
    }

    #[cfg(any(feature = "fdb", test))]
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        if !self.cluster_file.is_absolute() || self.cluster_file.file_name().is_none() {
            return Err(StoreError::InvalidRequest(
                "FdbStore cluster file must be an absolute file path".to_owned(),
            ));
        }
        let cluster_file = self.cluster_file.to_str().ok_or_else(|| {
            StoreError::InvalidRequest("FdbStore cluster file must be valid UTF-8".to_owned())
        })?;
        if cluster_file.as_bytes().contains(&0) {
            return Err(StoreError::InvalidRequest(
                "FdbStore cluster file must not contain NUL bytes".to_owned(),
            ));
        }
        if self.namespace.is_empty() || self.namespace.len() > MAX_NAMESPACE_BYTES {
            return Err(StoreError::InvalidRequest(format!(
                "FdbStore namespace must contain 1 through {MAX_NAMESPACE_BYTES} bytes"
            )));
        }
        if self.transaction_timeout < MIN_TRANSACTION_TIMEOUT
            || self.transaction_timeout > MAX_TRANSACTION_TIMEOUT
        {
            return Err(StoreError::InvalidRequest(format!(
                "FdbStore transaction timeout must be between {} and {} milliseconds",
                MIN_TRANSACTION_TIMEOUT.as_millis(),
                MAX_TRANSACTION_TIMEOUT.as_millis()
            )));
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
