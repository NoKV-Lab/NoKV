/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;
use std::sync::Arc;

use nokv_meta_holt::{HoltOptions, HoltStore, TreeBinding};
use nokv_types::LogicalShardId;

use super::{keyspaces, store_limits, MetaError, MetaShard};

pub(crate) fn memory(logical_shard_id: LogicalShardId) -> Result<MetaShard, MetaError> {
    let store = HoltStore::memory(catalog(), store_limits())
        .map_err(|source| store_error("create test metadata store", source))?;
    MetaShard::initialize(Arc::new(store), logical_shard_id)
}

pub(crate) fn initialize_file(
    path: &Path,
    logical_shard_id: LogicalShardId,
) -> Result<MetaShard, MetaError> {
    let store = HoltStore::initialize(HoltOptions::file(path, catalog(), store_limits()))
        .map_err(|source| store_error("initialize test metadata store", source))?;
    MetaShard::initialize(Arc::new(store), logical_shard_id)
}

pub(crate) fn open_file(
    path: &Path,
    logical_shard_id: LogicalShardId,
) -> Result<MetaShard, MetaError> {
    let store = HoltStore::open(HoltOptions::file(path, catalog(), store_limits()))
        .map_err(|source| store_error("open test metadata store", source))?;
    MetaShard::open(Arc::new(store), logical_shard_id)
}

fn catalog() -> Vec<TreeBinding> {
    keyspaces()
        .iter()
        .map(|definition| TreeBinding::new(definition.id, definition.name))
        .collect()
}

fn store_error(operation: &'static str, source: nokv_meta_store::StoreError) -> MetaError {
    MetaError::Store { operation, source }
}
