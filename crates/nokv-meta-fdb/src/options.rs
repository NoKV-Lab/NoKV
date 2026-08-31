/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::time::Duration;

use nokv_fdb::FdbConnectionOptions;
#[cfg(any(feature = "fdb", test))]
use nokv_fdb::FdbStorePrefix;
#[cfg(test)]
use nokv_fdb::MAX_STORE_PREFIX_BYTES;
#[cfg(any(feature = "fdb", test))]
use nokv_meta_store::StoreError;

#[cfg(test)]
pub(crate) const MAX_NAMESPACE_BYTES: usize = MAX_STORE_PREFIX_BYTES;

/// Physical FoundationDB connection and namespace options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbOptions {
    connection: FdbConnectionOptions,
    namespace: Vec<u8>,
}

impl FdbOptions {
    /// Configure one explicit cluster file and one binary application namespace.
    pub fn new(cluster_file: impl Into<PathBuf>, namespace: impl Into<Vec<u8>>) -> Self {
        Self {
            connection: FdbConnectionOptions::new(cluster_file),
            namespace: namespace.into(),
        }
    }

    /// Set the hard FoundationDB transaction timeout for every adapter call.
    pub fn with_transaction_timeout(mut self, timeout: Duration) -> Self {
        self.connection = self.connection.with_transaction_timeout(timeout);
        self
    }

    pub fn cluster_file(&self) -> &Path {
        self.connection.cluster_file()
    }

    pub fn namespace(&self) -> &[u8] {
        &self.namespace
    }

    pub fn transaction_timeout(&self) -> Duration {
        self.connection.transaction_timeout()
    }

    #[cfg(any(feature = "fdb", test))]
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        self.connection
            .validate()
            .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
        FdbStorePrefix::new(&self.namespace)
            .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
        Ok(())
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn connection_options(&self) -> &FdbConnectionOptions {
        &self.connection
    }
}
