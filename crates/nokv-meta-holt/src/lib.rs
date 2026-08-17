/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Embedded Holt implementation of the NoKV metadata transaction store.
//!
//! The adapter maps each configured [`nokv_meta_store::Keyspace`] to the
//! caller-configured physical Holt tree. It does not know the workspace schema
//! or record codecs. One `HoltStore` instance owns one local physical authority.
//! The `test-support` feature exposes an in-memory constructor plus explicit
//! checkpoint and keyspace-stat probes to test and diagnostic packages.

mod checkpoint;
mod options;
#[cfg(feature = "read-stats")]
mod stats;
mod store;

pub use options::{HoltOptions, TreeBinding};
#[cfg(feature = "read-stats")]
pub use stats::{
    HoltReadStats, HoltReadStatsDeltaError, HoltReadStatsSession, HoltReadStatsSessionError,
};
pub use store::HoltStore;

#[cfg(test)]
mod tests;
pub use checkpoint::{
    HoltFreshCheckpointInstaller, HoltRecoveryStagingAdoptionAuthority,
    HoltRecoveryStagingIdentity, HOLT_CHECKPOINT_FORMAT_ID,
};
