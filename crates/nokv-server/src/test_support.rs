/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use nokv_meta::workspace::{self as meta, MetaShard};
use nokv_meta_holt::{HoltStore, TreeBinding};
use nokv_meta_store::TxnStore;
use nokv_types::LogicalShardId;

pub(crate) fn meta_shard(shard_id: LogicalShardId) -> Arc<MetaShard> {
    let catalog = meta::keyspaces()
        .iter()
        .map(|definition| TreeBinding::new(definition.id, definition.name));
    let holt = Arc::new(
        HoltStore::memory(catalog, meta::store_limits())
            .expect("create in-memory Holt metadata store"),
    );
    let store: Arc<dyn TxnStore> = holt;
    Arc::new(MetaShard::initialize(store, shard_id).expect("initialize metadata shard"))
}
