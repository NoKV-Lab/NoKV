use std::fmt;

use crate::{
    LogicalShardId, LogicalShardLease, LogicalShardState, NodeId, ObjectNamespaceId, OwnerEpoch,
    RootCatalogEntry, RootId, RootPlacement, ShardCatalogEntry, StoreManifest,
};

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
    RootPlacementNotFound(RootId),
    RootPlacementAlreadyExists(RootId),
    RootAgentAlreadyBound {
        root_id: RootId,
    },
    RootObjectNamespaceAlreadyBound {
        root_id: RootId,
        existing: ObjectNamespaceId,
        requested: ObjectNamespaceId,
    },
    ImmutableShardAffinity {
        root_id: RootId,
        existing: LogicalShardId,
        requested: LogicalShardId,
    },
    RootPlacementCasConflict {
        expected: RootPlacement,
        actual: Option<RootPlacement>,
    },
    InvalidPlacementMutation {
        root_id: RootId,
        reason: String,
    },
    LogicalShardNotFound(LogicalShardId),
    LogicalShardAlreadyExists(LogicalShardId),
    RootPlacementRequired(LogicalShardId),
    LogicalShardAlreadyOwned {
        logical_shard_id: LogicalShardId,
        owner: NodeId,
        owner_epoch: OwnerEpoch,
    },
    PreviousOwnerSessionLive {
        logical_shard_id: LogicalShardId,
        owner_epoch: OwnerEpoch,
    },
    RecoveryAttemptPending {
        logical_shard_id: LogicalShardId,
        owner_epoch: OwnerEpoch,
    },
    RecoveryStateConflict {
        logical_shard_id: LogicalShardId,
        actual: LogicalShardState,
    },
    StaleOwnerEpoch {
        logical_shard_id: LogicalShardId,
        expected: Option<OwnerEpoch>,
        actual: Option<OwnerEpoch>,
    },
    NotOwner {
        logical_shard_id: LogicalShardId,
    },
    StaleLease(LogicalShardLease),
    OwnerEpochExhausted(LogicalShardId),
    LeaseIdExhausted(LogicalShardId),
    RecoveryPublicationConflict {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    RecoveryUploadConflict {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    InvalidRecord(String),
    InvalidOptions(String),
    /// A durable control record declares a codec version this reader does not
    /// implement. The record itself is intact; the reader is too old.
    UnsupportedRecordVersion {
        record: &'static str,
        version: u8,
        supported: u8,
    },
    Codec(String),
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
            Self::RootPlacementNotFound(root_id) => {
                write!(formatter, "root placement {root_id:?} was not found")
            }
            Self::RootPlacementAlreadyExists(root_id) => {
                write!(formatter, "root placement {root_id:?} already exists")
            }
            Self::RootAgentAlreadyBound { root_id, .. } => {
                write!(formatter, "root {root_id:?} is already bound to another Agent")
            }
            Self::RootObjectNamespaceAlreadyBound { root_id, .. } => write!(
                formatter,
                "root {root_id:?} is already bound to another artifact object namespace"
            ),
            Self::ImmutableShardAffinity {
                root_id,
                existing,
                requested,
            } => write!(
                formatter,
                "root {root_id:?} is permanently bound to logical shard {existing:?}, not {requested:?}"
            ),
            Self::RootPlacementCasConflict { expected, actual } => write!(
                formatter,
                "root placement CAS conflict: expected {expected:?}, actual {actual:?}"
            ),
            Self::InvalidPlacementMutation { root_id, reason } => {
                write!(formatter, "invalid root placement mutation for {root_id:?}: {reason}")
            }
            Self::LogicalShardNotFound(logical_shard_id) => {
                write!(formatter, "logical shard {logical_shard_id:?} was not found")
            }
            Self::LogicalShardAlreadyExists(logical_shard_id) => {
                write!(formatter, "logical shard {logical_shard_id:?} already exists")
            }
            Self::RootPlacementRequired(logical_shard_id) => write!(
                formatter,
                "logical shard {logical_shard_id:?} needs a non-retired root placement before owner acquisition"
            ),
            Self::LogicalShardAlreadyOwned {
                logical_shard_id,
                owner,
                owner_epoch,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} is owned by {owner} at epoch {owner_epoch}"
            ),
            Self::PreviousOwnerSessionLive {
                logical_shard_id,
                owner_epoch,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} still has a live owner session at epoch {owner_epoch}"
            ),
            Self::RecoveryAttemptPending {
                logical_shard_id,
                owner_epoch,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} has an unfinished recovery attempt at epoch {owner_epoch}; reacquire that recovery epoch instead of advancing it"
            ),
            Self::RecoveryStateConflict {
                logical_shard_id,
                actual,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} is {actual:?}, not Recovering"
            ),
            Self::StaleOwnerEpoch {
                logical_shard_id,
                expected,
                actual,
            } => write!(
                formatter,
                "logical shard {logical_shard_id:?} expected owner epoch {}, actual {}",
                display_epoch(*expected),
                display_epoch(*actual)
            ),
            Self::NotOwner { logical_shard_id } => write!(
                formatter,
                "lease holder does not own logical shard {logical_shard_id:?}"
            ),
            Self::StaleLease(lease) => write!(
                formatter,
                "stale lease for logical shard {:?} at epoch {} lease {}",
                lease.logical_shard_id, lease.owner_epoch, lease.lease_id
            ),
            Self::OwnerEpochExhausted(logical_shard_id) => write!(
                formatter,
                "logical shard {logical_shard_id:?} owner epoch is exhausted"
            ),
            Self::LeaseIdExhausted(logical_shard_id) => write!(
                formatter,
                "logical shard {logical_shard_id:?} lease id allocator is exhausted"
            ),
            Self::RecoveryPublicationConflict {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "recovery publication for logical shard {logical_shard_id:?} conflicts with durable state: {reason}"
            ),
            Self::RecoveryUploadConflict {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "recovery upload for logical shard {logical_shard_id:?} conflicts with durable state: {reason}"
            ),
            Self::InvalidRecord(reason) => {
                write!(formatter, "invalid control record: {reason}")
            }
            Self::InvalidOptions(reason) => {
                write!(formatter, "invalid control store options: {reason}")
            }
            Self::UnsupportedRecordVersion {
                record,
                version,
                supported,
            } => write!(
                formatter,
                "control store {record} uses codec version {version}; this reader supports \
                 versions up to {supported}, so this client or owner must be upgraded before it \
                 can use this control plane"
            ),
            Self::Codec(reason) => write!(formatter, "control store codec error: {reason}"),
            Self::Backend(reason) => write!(formatter, "control store backend error: {reason}"),
        }
    }
}

impl std::error::Error for ControlError {}

fn display_epoch(epoch: Option<OwnerEpoch>) -> String {
    epoch.map_or_else(|| "never-owned".to_owned(), |epoch| epoch.get().to_string())
}
