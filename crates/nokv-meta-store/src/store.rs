/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{Commit, ReadBatch, ReadSnapshot, StoreError, StoreProfile, WriteTxn};

/// Synchronous ordered transaction store for one logical metadata shard.
///
/// Every read batch is linearizable and observes one consistent snapshot. A
/// read includes every commit that this instance reported as [`Commit::Applied`]
/// before the read began. Separate `read` calls do not share a snapshot.
///
/// Every write transaction evaluates its checks and mutations atomically at
/// the store's configured acknowledgement boundary. A false check returns
/// [`Commit::Conflict`] and applies no mutation. An uncertain commit returns
/// [`StoreError::OutcomeUnknown`]. The store never reports it as a conflict.
/// Loss of the physical ownership authority bound to the handle returns
/// [`StoreError::Fenced`] and applies no mutation.
///
/// An adapter that returns a poisoned unknown outcome must enter that state
/// before it returns. No overlapping or later `read` or `commit` may return
/// success after the poison transition. The adapter must serialize completion
/// with that transition or recheck its state before publishing a result.
/// `ready` must remain unavailable until the caller opens a new instance, but
/// calling it is not a substitute for method-level checks.
pub trait TxnStore: Send + Sync {
    /// Return immutable limits, acknowledgement, and authority for this instance.
    fn profile(&self) -> StoreProfile;

    /// Execute all operations against one consistent snapshot.
    fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError>;

    /// Evaluate all checks and mutations in one serializable transaction.
    fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError>;

    /// Report whether this instance can safely accept reads and commits.
    fn ready(&self) -> Result<(), StoreError>;
}
