use std::fmt;

use crate::{
    LogicalShardId, LogicalShardLease, MetadataAuthorityRecord, NodeId, OwnerEpoch, RootId,
    RootLayoutProfile, RootPlacement,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    InvalidEndpoint(String),
    RootPlacementNotFound(RootId),
    RootPlacementAlreadyExists(RootId),
    ImmutableShardAffinity {
        root_id: RootId,
        existing: LogicalShardId,
        requested: LogicalShardId,
    },
    RootPlacementCasConflict {
        expected: Box<RootPlacement>,
        actual: Option<Box<RootPlacement>>,
    },
    InvalidPlacementMutation {
        root_id: RootId,
        reason: String,
    },
    RootLayoutNotQualified {
        root_id: RootId,
        profile: RootLayoutProfile,
    },
    InvalidFreshRootProvisioning {
        root_id: RootId,
        reason: String,
    },
    FreshRootProvisioningConflict {
        root_id: RootId,
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    LogicalShardNotFound(LogicalShardId),
    LogicalShardAlreadyExists(LogicalShardId),
    MetadataAuthorityNotFound(LogicalShardId),
    MetadataAuthorityAlreadyExists(LogicalShardId),
    MetadataAuthorityAdoptionRejected {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    MetadataAuthorityCasConflict {
        expected: Box<MetadataAuthorityRecord>,
        actual: Option<Box<MetadataAuthorityRecord>>,
    },
    InvalidMetadataAuthorityMutation {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    MetadataAuthorityAdmission {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
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
    RootPlacementCodecUpgradeRequired {
        stored_version: u8,
        required_version: u8,
    },
    LogicalShardRecordCodecUpgradeRequired {
        stored_version: u8,
        required_version: u8,
    },
    MetadataAuthorityCodecUpgradeRequired {
        stored_version: u8,
        required_version: u8,
    },
    OwnerSessionCodecUpgradeRequired {
        stored_version: u8,
        required_version: u8,
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
            Self::RootPlacementNotFound(root_id) => {
                write!(formatter, "root placement {root_id:?} was not found")
            }
            Self::RootPlacementAlreadyExists(root_id) => {
                write!(formatter, "root placement {root_id:?} already exists")
            }
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
            Self::RootLayoutNotQualified { root_id, profile } => write!(
                formatter,
                "root layout {profile:?} for {root_id:?} is NOT QUALIFIED by this runtime"
            ),
            Self::InvalidFreshRootProvisioning { root_id, reason } => write!(
                formatter,
                "invalid fresh-root provisioning request for {root_id:?}: {reason}"
            ),
            Self::FreshRootProvisioningConflict {
                root_id,
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "fresh-root provisioning for {root_id:?} on logical shard {logical_shard_id:?} conflicts with control state: {reason}"
            ),
            Self::LogicalShardNotFound(logical_shard_id) => {
                write!(formatter, "logical shard {logical_shard_id:?} was not found")
            }
            Self::LogicalShardAlreadyExists(logical_shard_id) => {
                write!(formatter, "logical shard {logical_shard_id:?} already exists")
            }
            Self::MetadataAuthorityNotFound(logical_shard_id) => write!(
                formatter,
                "metadata authority for logical shard {logical_shard_id:?} was not found"
            ),
            Self::MetadataAuthorityAlreadyExists(logical_shard_id) => write!(
                formatter,
                "metadata authority for logical shard {logical_shard_id:?} already exists"
            ),
            Self::MetadataAuthorityAdoptionRejected {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "metadata authority cannot adopt logical shard {logical_shard_id:?}: {reason}"
            ),
            Self::MetadataAuthorityCasConflict { expected, actual } => write!(
                formatter,
                "metadata authority CAS conflict: expected {expected:?}, actual {actual:?}"
            ),
            Self::InvalidMetadataAuthorityMutation {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "invalid metadata authority mutation for logical shard {logical_shard_id:?}: {reason}"
            ),
            Self::MetadataAuthorityAdmission {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "metadata authority admission failed for logical shard {logical_shard_id:?}: {reason}"
            ),
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
            Self::RootPlacementCodecUpgradeRequired {
                stored_version,
                required_version,
            } => write!(
                formatter,
                "root placement codec version {stored_version} requires an explicit upgrade to version {required_version}"
            ),
            Self::LogicalShardRecordCodecUpgradeRequired {
                stored_version,
                required_version,
            } => write!(
                formatter,
                "logical shard record codec version {stored_version} has no owner incarnation; automatic adoption is forbidden, version {required_version} is required"
            ),
            Self::MetadataAuthorityCodecUpgradeRequired {
                stored_version,
                required_version,
            } => write!(
                formatter,
                "metadata authority codec version {stored_version} requires an explicit upgrade to version {required_version}"
            ),
            Self::OwnerSessionCodecUpgradeRequired {
                stored_version,
                required_version,
            } => write!(
                formatter,
                "owner session codec version {stored_version} has no exact owner incarnation; automatic adoption is forbidden, version {required_version} is required"
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
