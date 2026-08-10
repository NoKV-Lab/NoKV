/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg(feature = "test-support")]

use nokv_meta_holt::{HoltStore, TreeBinding};
use nokv_meta_store::{Keyspace, StoreLimits, TxnStore};

#[test]
fn feature_exposes_memory_store() {
    let limits = StoreLimits {
        max_reads: 8,
        max_checks: 8,
        max_mutations: 8,
        max_key_bytes: 128,
        max_value_bytes: 1024,
        max_read_bytes: 2048,
        max_transaction_bytes: 2048,
        max_result_rows: 32,
        max_result_bytes: 2048,
    };
    let store = HoltStore::memory([TreeBinding::new(Keyspace::new(1), "test-tree")], limits)
        .expect("open in-memory Holt store");

    assert_eq!(store.profile().limits, limits);
    store.ready().expect("in-memory store is ready");
}
