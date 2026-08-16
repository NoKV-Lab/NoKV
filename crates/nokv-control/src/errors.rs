use std::fmt;

use crate::{
    LogicalShardId, LogicalShardLease, LogicalShardState, NodeId, ObjectNamespaceId, OwnerEpoch,
    RootId, RootPlacement,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    InvalidEndpoint(String),
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
    InvalidRecord(String),
    InvalidOptions(String),
    Codec(String),
    Backend(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint(endpoint) => {
                write!(formatter, "invalid logical-shard endpoint {endpoint:?}")
            }
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
            Self::InvalidRecord(reason) => {
                write!(formatter, "invalid control record: {reason}")
            }
            Self::InvalidOptions(reason) => {
                write!(formatter, "invalid control store options: {reason}")
            }
            Self::Codec(reason) => write!(formatter, "control store codec error: {reason}"),
            Self::Backend(reason) => write!(formatter, "control store backend error: {reason}"),
        }
    }
}

impl std::error::Error for ControlError {}

fn display_epoch(epoch: Option<OwnerEpoch>) -> String {
    epoch.map_or_else(|| "never-owned".to_owned(), |epoch| epoch.get().to_string())
}
