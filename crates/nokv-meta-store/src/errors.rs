/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

/// Reason a request exceeded one configured store limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitKind {
    Reads,
    Checks,
    Mutations,
    KeyBytes,
    ValueBytes,
    ReadBytes,
    TransactionBytes,
    ResultRows,
    ResultBytes,
}

/// Physical state after a commit returns an unknown outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnknownCommit {
    /// The original call cannot change store state after the error returns.
    Settled,
    /// The original call can still commit after the error returns.
    MayCommit,
    /// The live store view may be ahead of its acknowledgement boundary.
    ///
    /// The adapter poisoned the instance before it returned this state. Every
    /// overlapping or later read and commit must fail. The caller must open a
    /// new instance.
    Poisoned,
}

/// Storage-layer error with no adapter-specific type in the public contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// The request is malformed independently of current store state.
    InvalidRequest(String),
    /// The request exceeds a configured serving or physical limit.
    LimitExceeded {
        kind: LimitKind,
        actual: usize,
        maximum: usize,
    },
    /// The operation could not run because the store was unavailable.
    ///
    /// No mutation from this call applied. An upper layer may retry the same
    /// domain request without fencing the store; an outcome that may have
    /// applied must use [`StoreError::OutcomeUnknown`] instead.
    Unavailable(String),
    /// A commit may or may not have reached its acknowledgement boundary.
    ///
    /// Callers must reconcile the domain request before they issue another
    /// physical commit.
    OutcomeUnknown {
        state: UnknownCommit,
        reason: String,
    },
    /// Physical state or a returned record violates the store contract.
    Corrupt(String),
}

impl fmt::Display for LimitKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Reads => "reads",
            Self::Checks => "checks",
            Self::Mutations => "mutations",
            Self::KeyBytes => "key bytes",
            Self::ValueBytes => "value bytes",
            Self::ReadBytes => "read bytes",
            Self::TransactionBytes => "transaction bytes",
            Self::ResultRows => "result rows",
            Self::ResultBytes => "result bytes",
        })
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(reason) => write!(formatter, "invalid store request: {reason}"),
            Self::LimitExceeded {
                kind,
                actual,
                maximum,
            } => write!(
                formatter,
                "store {kind} limit exceeded: {actual}, maximum {maximum}"
            ),
            Self::Unavailable(reason) => {
                write!(formatter, "metadata store unavailable: {reason}")
            }
            Self::OutcomeUnknown { state, reason } => {
                write!(
                    formatter,
                    "metadata commit outcome is unknown: {reason} ({state})"
                )
            }
            Self::Corrupt(reason) => write!(formatter, "metadata store is corrupt: {reason}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl fmt::Display for UnknownCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Settled => "settled",
            Self::MayCommit => "may still commit",
            Self::Poisoned => "store poisoned",
        })
    }
}
