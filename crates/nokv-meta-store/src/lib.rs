/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Ordered byte-key transaction interface for NoKV metadata stores.
//!
//! This crate defines physical store requests and results. Workspace schema,
//! record codecs, command planning, history, and lifecycle rules stay in
//! `nokv-meta`. Store adapters translate these byte operations to one physical
//! transaction system without exposing backend types through this interface.

mod checkpoint;
mod errors;
mod store;
mod types;

#[cfg(any(test, feature = "test-support"))]
pub mod conformance;

pub use errors::{LimitKind, StoreError, UnknownCommit};
pub use store::TxnStore;
pub use types::{
    AckBoundary, Authority, Check, Commit, Key, Keyspace, Mutation, ReadBatch, ReadOp, ReadResult,
    ReadSnapshot, RecoveryMode, Scan, ScanItem, ScanPage, StoreLimits, StoreProfile, WriteTxn,
};

#[cfg(test)]
mod tests;
pub use checkpoint::{
    CheckpointCatalogCommitment, CheckpointError, CheckpointFormatId, CheckpointInstallError,
    CheckpointInstallState, FreshStoreCheckpointInstaller, StoreCheckpointEnvelope,
    WholeStoreCheckpointSource, MAX_CHECKPOINT_IMAGE_BYTES,
};
