/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::time::Duration;

use nokv_fdb::FdbConnectionOptions;
#[cfg(test)]
use nokv_fdb::MAX_STORE_PREFIX_BYTES;
#[cfg(any(feature = "fdb", test))]
use nokv_fdb::{FdbStorePrefix, FdbSubspaceKind};
use nokv_meta_store::StoreError;

#[cfg(test)]
pub(crate) const MAX_NAMESPACE_BYTES: usize = MAX_STORE_PREFIX_BYTES;
const MAX_SESSION_KEY_BYTES: usize = 1_024;
const MAX_SESSION_VALUE_BYTES: usize = 4_096;

/// Immutable FoundationDB predicate for one serving metadata owner session.
///
/// `nokv-control-fdb` owns the session record encoding. The server composition
/// root copies the exact key and encoded value into this storage-only type so
/// the metadata adapter does not depend on control-plane packages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbMetadataSessionFence {
    key: Vec<u8>,
    expected_value: Vec<u8>,
    expected_owner_epoch: u64,
    expected_session_generation: u64,
}

impl FdbMetadataSessionFence {
    pub fn new(
        key: impl Into<Vec<u8>>,
        expected_value: impl Into<Vec<u8>>,
        expected_owner_epoch: u64,
        expected_session_generation: u64,
    ) -> Result<Self, StoreError> {
        let fence = Self {
            key: key.into(),
            expected_value: expected_value.into(),
            expected_owner_epoch,
            expected_session_generation,
        };
        fence.validate()?;
        Ok(fence)
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn expected_value(&self) -> &[u8] {
        &self.expected_value
    }

    pub const fn expected_owner_epoch(&self) -> u64 {
        self.expected_owner_epoch
    }

    pub const fn expected_session_generation(&self) -> u64 {
        self.expected_session_generation
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.key.is_empty() || self.key.len() > MAX_SESSION_KEY_BYTES {
            return Err(StoreError::InvalidRequest(format!(
                "FoundationDB session key must contain 1..={MAX_SESSION_KEY_BYTES} bytes"
            )));
        }
        if self.expected_value.is_empty() || self.expected_value.len() > MAX_SESSION_VALUE_BYTES {
            return Err(StoreError::InvalidRequest(format!(
                "FoundationDB session value must contain 1..={MAX_SESSION_VALUE_BYTES} bytes"
            )));
        }
        if self.expected_owner_epoch == 0 || self.expected_session_generation == 0 {
            return Err(StoreError::InvalidRequest(
                "FoundationDB owner epoch and session generation must be nonzero".to_owned(),
            ));
        }
        Ok(())
    }

    #[cfg(any(feature = "fdb", test))]
    fn validate_for_prefix(&self, prefix: &FdbStorePrefix) -> Result<(), StoreError> {
        self.validate()?;
        let session_prefix = prefix.subspace(FdbSubspaceKind::LeaseSession);
        if !self.key.starts_with(session_prefix.as_bytes())
            || self.key.len() == session_prefix.as_bytes().len()
        {
            return Err(StoreError::InvalidRequest(
                "FoundationDB session key is outside the selected store prefix".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Physical FoundationDB connection and namespace options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbOptions {
    connection: FdbConnectionOptions,
    namespace: Vec<u8>,
    session_fence: FdbMetadataSessionFence,
}

impl FdbOptions {
    /// Configure one explicit cluster file, store prefix, and owner session.
    pub fn new(
        cluster_file: impl Into<PathBuf>,
        namespace: impl Into<Vec<u8>>,
        session_fence: FdbMetadataSessionFence,
    ) -> Self {
        Self {
            connection: FdbConnectionOptions::new(cluster_file),
            namespace: namespace.into(),
            session_fence,
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

    pub fn session_fence(&self) -> &FdbMetadataSessionFence {
        &self.session_fence
    }

    pub fn transaction_timeout(&self) -> Duration {
        self.connection.transaction_timeout()
    }

    #[cfg(any(feature = "fdb", test))]
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        self.connection
            .validate()
            .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
        let prefix = FdbStorePrefix::new(&self.namespace)
            .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
        self.session_fence.validate_for_prefix(&prefix)?;
        Ok(())
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn connection_options(&self) -> &FdbConnectionOptions {
        &self.connection
    }
}
