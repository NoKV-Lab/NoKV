use std::fmt;

use crate::{NodeId, ShardId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    ShardAlreadyOwned {
        shard_id: ShardId,
        owner: NodeId,
        epoch: u64,
    },
    ShardNotFound(ShardId),
    StaleEpoch {
        shard_id: ShardId,
        expected: u64,
        actual: u64,
    },
    NotOwner {
        shard_id: ShardId,
    },
    /// A `register_shard` tried to change a shard's stable identity (`prefix` or
    /// `shard_index`) after it had already taken a lease (epoch > 0). Identity is
    /// baked into inode high bits and client routing, so it is frozen once the
    /// shard has ever served.
    ShardIdentityLocked {
        shard_id: ShardId,
    },
    /// A non-default shard was acquired without first being registered. The
    /// stable shard index must be seeded by `register_shard` before acquisition
    /// so it does not silently default to 0 and collide with the root shard.
    ShardNotRegistered {
        shard_id: ShardId,
    },
    /// A server requested a fresh start for a shard that has already held an
    /// owner epoch. Its local disk cannot be assumed authoritative; recovery
    /// must use the explicit failover path and a checkpoint archive.
    FreshAcquireRequiresFailover {
        shard_id: ShardId,
        epoch: u64,
    },
    StaleLease {
        shard_id: ShardId,
        epoch: u64,
        lease_id: u64,
    },
    /// A checkpoint/log publication would make the durable recovery identity
    /// ambiguous or move one of its references behind the recorded durable
    /// boundary. Reject it instead of allowing the last writer to win.
    RecoveryPublicationConflict {
        shard_id: ShardId,
        reason: String,
    },
    InvalidOptions(String),
    Codec(String),
    Backend(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShardAlreadyOwned {
                shard_id,
                owner,
                epoch,
            } => write!(
                f,
                "shard {} is already owned by {} at epoch {}",
                shard_id.as_str(),
                owner.as_str(),
                epoch
            ),
            Self::ShardNotFound(shard_id) => write!(f, "shard {} was not found", shard_id.as_str()),
            Self::StaleEpoch {
                shard_id,
                expected,
                actual,
            } => write!(
                f,
                "shard {} expected epoch {}, actual epoch {}",
                shard_id.as_str(),
                expected,
                actual
            ),
            Self::NotOwner { shard_id } => {
                write!(f, "lease holder does not own shard {}", shard_id.as_str())
            }
            Self::ShardIdentityLocked { shard_id } => write!(
                f,
                "shard {} identity is locked after its first lease",
                shard_id.as_str()
            ),
            Self::ShardNotRegistered { shard_id } => write!(
                f,
                "shard {} must be registered before it can be acquired",
                shard_id.as_str()
            ),
            Self::FreshAcquireRequiresFailover { shard_id, epoch } => write!(
                f,
                "shard {} has prior owner epoch {}; use explicit failover recovery",
                shard_id.as_str(),
                epoch
            ),
            Self::StaleLease {
                shard_id,
                epoch,
                lease_id,
            } => write!(
                f,
                "stale lease for shard {} at epoch {} lease {}",
                shard_id.as_str(),
                epoch,
                lease_id
            ),
            Self::RecoveryPublicationConflict { shard_id, reason } => write!(
                f,
                "recovery publication for shard {} conflicts with durable state: {reason}",
                shard_id.as_str()
            ),
            Self::InvalidOptions(err) => write!(f, "invalid control store options: {err}"),
            Self::Codec(err) => write!(f, "control store codec error: {err}"),
            Self::Backend(err) => write!(f, "control store backend error: {err}"),
        }
    }
}

impl std::error::Error for ControlError {}
