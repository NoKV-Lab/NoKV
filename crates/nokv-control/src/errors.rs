/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use crate::{LogicalShardId, RootCatalogEntry, RootId, ShardCatalogEntry, StoreManifest};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    InvalidEndpoint(String),
    StoreNotFormatted,
    StoreManifestMismatch {
        expected: Box<StoreManifest>,
        actual: Box<StoreManifest>,
    },
    RootCatalogAlreadyExists(RootId),
    RootCatalogCasConflict {
        expected: Box<RootCatalogEntry>,
        actual: Box<Option<RootCatalogEntry>>,
    },
    ShardCatalogCasConflict {
        expected: Box<ShardCatalogEntry>,
        actual: Box<Option<ShardCatalogEntry>>,
    },
    InvalidCatalogTransition {
        record: &'static str,
        reason: String,
    },
    OwnershipStateConflict {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    OwnershipObservationPending {
        logical_shard_id: LogicalShardId,
        remaining_millis: u64,
    },
    OwnershipCounterExhausted {
        logical_shard_id: LogicalShardId,
        counter: &'static str,
    },
    TransactionConflict {
        operation: &'static str,
    },
    CommitOutcomeUnknown {
        operation: &'static str,
        reason: String,
    },
    LogicalShardNotFound(LogicalShardId),
    NotOwner {
        logical_shard_id: LogicalShardId,
    },
    OwnerEpochExhausted(LogicalShardId),
    InvalidRecord(String),
    InvalidOptions(String),
    UnsupportedRecordVersion {
        record: &'static str,
        version: u8,
        supported: u8,
    },
    Backend(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint(endpoint) => {
                write!(formatter, "invalid logical-shard endpoint {endpoint:?}")
            }
            Self::StoreNotFormatted => {
                formatter.write_str("metadata store has no durable manifest")
            }
            Self::StoreManifestMismatch { expected, actual } => write!(
                formatter,
                "store manifest mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::RootCatalogAlreadyExists(root_id) => {
                write!(formatter, "root catalog {root_id:?} already exists")
            }
            Self::RootCatalogCasConflict { expected, actual } => write!(
                formatter,
                "root catalog CAS conflict: expected {expected:?}, actual {actual:?}"
            ),
            Self::ShardCatalogCasConflict { expected, actual } => write!(
                formatter,
                "logical shard catalog CAS conflict: expected {expected:?}, actual {actual:?}"
            ),
            Self::InvalidCatalogTransition { record, reason } => {
                write!(formatter, "invalid {record} transition: {reason}")
            }
            Self::OwnershipStateConflict {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} ownership state is inconsistent: {reason}"
            ),
            Self::OwnershipObservationPending {
                logical_shard_id,
                remaining_millis,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} ownership observation needs another {remaining_millis}ms"
            ),
            Self::OwnershipCounterExhausted {
                logical_shard_id,
                counter,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} {counter} is exhausted"
            ),
            Self::TransactionConflict { operation } => {
                write!(formatter, "control transaction conflicted while trying to {operation}")
            }
            Self::CommitOutcomeUnknown { operation, reason } => write!(
                formatter,
                "control transaction outcome is unknown while trying to {operation}: {reason}"
            ),
            Self::LogicalShardNotFound(logical_shard_id) => {
                write!(formatter, "logical shard {logical_shard_id:?} was not found")
            }
            Self::NotOwner { logical_shard_id } => write!(
                formatter,
                "session holder does not own logical shard {logical_shard_id:?}"
            ),
            Self::OwnerEpochExhausted(logical_shard_id) => write!(
                formatter,
                "logical shard {logical_shard_id:?} owner epoch is exhausted"
            ),
            Self::InvalidRecord(reason) => write!(formatter, "invalid control record: {reason}"),
            Self::InvalidOptions(reason) => {
                write!(formatter, "invalid control store options: {reason}")
            }
            Self::UnsupportedRecordVersion {
                record,
                version,
                supported,
            } => write!(
                formatter,
                "control store {record} uses codec version {version}; this reader supports versions up to {supported}"
            ),
            Self::Backend(reason) => write!(formatter, "control store backend error: {reason}"),
        }
    }
}

impl std::error::Error for ControlError {}
