/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta_store::{AckBoundary, Authority, StoreLimits, StoreProfile};

pub(crate) const PHYSICAL_AFFECTED_BYTES: usize = 9_500_000;

pub(crate) const FDB_LIMITS: StoreLimits = StoreLimits {
    max_reads: 8,
    max_checks: 1024,
    max_mutations: 1024,
    max_key_bytes: 8205,
    max_value_bytes: 65_535,
    max_read_bytes: 4_500_000,
    max_transaction_bytes: 2_900_000,
    max_result_rows: 1024,
    max_result_bytes: 8 * 1024 * 1024,
};

pub(crate) const FDB_PROFILE: StoreProfile = StoreProfile {
    limits: FDB_LIMITS,
    ack: AckBoundary::SharedCommit,
    authority: Authority::Shared,
};
