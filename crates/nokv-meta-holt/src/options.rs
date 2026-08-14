/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use holt::{Durability, Storage, TreeConfig};
use nokv_meta_store::{Keyspace, StoreError, StoreLimits};

const HOLT_MAX_KEY_BYTES: usize = u16::MAX as usize - 1;
const HOLT_MAX_VALUE_BYTES: usize = u16::MAX as usize;
// Holt uses one 16 MiB WAL ring entry for each DB atomic batch.
const HOLT_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
const HOLT_DB_RECORD_FIXED_BYTES: usize = 33;
const HOLT_DB_MUTATION_OVERHEAD_BYTES: usize = 17;

/// Physical Holt configuration for one local metadata authority.
#[derive(Clone, Debug)]
pub struct HoltOptions {
    /// Holt database configuration.
    pub config: TreeConfig,
    /// Exact physical tree catalog supplied by the metadata schema.
    pub catalog: Vec<TreeBinding>,
    /// Serving request and result limits advertised by the adapter.
    pub limits: StoreLimits,
}

/// One storage-neutral keyspace to physical Holt tree mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBinding {
    /// Storage-neutral keyspace selected by `nokv-meta`.
    pub keyspace: Keyspace,
    /// Existing format-specific Holt tree name selected by `nokv-meta`.
    pub name: String,
}

impl TreeBinding {
    /// Define one physical tree catalog entry.
    pub fn new(keyspace: Keyspace, name: impl Into<String>) -> Self {
        Self {
            keyspace,
            name: name.into(),
        }
    }
}

impl HoltOptions {
    /// Build a file-backed configuration that fsyncs each applied commit.
    pub fn file(
        path: impl Into<PathBuf>,
        catalog: impl IntoIterator<Item = TreeBinding>,
        limits: StoreLimits,
    ) -> Self {
        let mut config = TreeConfig::new(path);
        config.durability = Durability::Wal { sync: true };
        config.checkpoint.auto_vacuum = false;
        Self {
            config,
            catalog: catalog.into_iter().collect(),
            limits,
        }
    }

    pub(crate) fn validate(
        &self,
        allow_memory: bool,
    ) -> Result<BTreeMap<Keyspace, String>, StoreError> {
        if self.config.is_memory() && !allow_memory {
            return Err(StoreError::InvalidRequest(
                "HoltStore production configuration must be file-backed".to_owned(),
            ));
        }
        if !self.config.is_memory() && self.config.durability != (Durability::Wal { sync: true }) {
            return Err(StoreError::InvalidRequest(
                "HoltStore requires a synchronous WAL acknowledgement".to_owned(),
            ));
        }
        if !self.config.is_memory() && self.config.checkpoint.auto_vacuum {
            return Err(StoreError::InvalidRequest(
                "HoltStore automatic vacuum is not qualified".to_owned(),
            ));
        }
        if self.catalog.is_empty() {
            return Err(StoreError::InvalidRequest(
                "HoltStore requires at least one keyspace".to_owned(),
            ));
        }

        let mut trees = BTreeMap::new();
        let mut names = BTreeSet::new();
        for tree in &self.catalog {
            if tree.name.is_empty() || tree.name.as_bytes().first() == Some(&0) {
                return Err(StoreError::InvalidRequest(format!(
                    "HoltStore tree name is invalid for keyspace {:04x}",
                    tree.keyspace.get()
                )));
            }
            if trees.insert(tree.keyspace, tree.name.clone()).is_some() {
                return Err(StoreError::InvalidRequest(
                    "HoltStore configuration contains duplicate keyspaces".to_owned(),
                ));
            }
            if !names.insert(tree.name.clone()) {
                return Err(StoreError::InvalidRequest(
                    "HoltStore configuration contains duplicate tree names".to_owned(),
                ));
            }
        }
        if self.limits.max_key_bytes > HOLT_MAX_KEY_BYTES {
            return Err(StoreError::InvalidRequest(format!(
                "HoltStore key limit exceeds the physical maximum of {HOLT_MAX_KEY_BYTES} bytes"
            )));
        }
        if self.limits.max_value_bytes > HOLT_MAX_VALUE_BYTES {
            return Err(StoreError::InvalidRequest(format!(
                "HoltStore value limit exceeds the physical maximum of {HOLT_MAX_VALUE_BYTES} bytes"
            )));
        }
        if self.limits.max_reads == 0
            || self.limits.max_checks == 0
            || self.limits.max_mutations == 0
            || self.limits.max_key_bytes == 0
            || self.limits.max_value_bytes == 0
            || self.limits.max_read_bytes == 0
            || self.limits.max_transaction_bytes == 0
            || self.limits.max_result_rows == 0
            || self.limits.max_result_bytes == 0
        {
            return Err(StoreError::InvalidRequest(
                "HoltStore limits must all be positive".to_owned(),
            ));
        }
        let row_bytes = self
            .limits
            .max_key_bytes
            .checked_add(self.limits.max_value_bytes)
            .ok_or_else(|| {
                StoreError::InvalidRequest("HoltStore row limit overflows usize".to_owned())
            })?;
        if self.limits.max_result_bytes < row_bytes {
            return Err(StoreError::InvalidRequest(format!(
                "HoltStore result limit must hold one maximum-size row ({row_bytes} bytes)"
            )));
        }
        let mutation_overhead = self
            .limits
            .max_mutations
            .checked_mul(HOLT_DB_MUTATION_OVERHEAD_BYTES)
            .ok_or_else(|| {
                StoreError::InvalidRequest("HoltStore WAL reserve overflows usize".to_owned())
            })?;
        let maximum_wal_record = HOLT_DB_RECORD_FIXED_BYTES
            .checked_add(self.limits.max_transaction_bytes)
            .and_then(|bytes| bytes.checked_add(mutation_overhead))
            .ok_or_else(|| {
                StoreError::InvalidRequest("HoltStore WAL reserve overflows usize".to_owned())
            })?;
        if maximum_wal_record > HOLT_WAL_RECORD_BYTES {
            return Err(StoreError::InvalidRequest(format!(
                "HoltStore limits can encode a {maximum_wal_record}-byte WAL record, maximum {HOLT_WAL_RECORD_BYTES}"
            )));
        }
        Ok(trees)
    }

    pub(crate) fn file_path(&self) -> Result<Option<&Path>, StoreError> {
        match &self.config.storage {
            Storage::File { dir } => Ok(Some(dir)),
            Storage::Memory => Ok(None),
            _ => Err(StoreError::InvalidRequest(
                "HoltStore does not support this storage mode".to_owned(),
            )),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn memory(
        catalog: impl IntoIterator<Item = TreeBinding>,
        limits: StoreLimits,
    ) -> Self {
        Self {
            config: TreeConfig::memory(),
            catalog: catalog.into_iter().collect(),
            limits,
        }
    }
}
