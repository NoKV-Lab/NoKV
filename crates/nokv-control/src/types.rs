use std::fmt;

pub use nokv_types::{
    AgentId, LogicalShardId, ObjectNamespaceId, OwnerEpoch, PlacementGeneration, RootId,
    RootPlacementLifecycle,
};

/// Stable identity of one physical metadata-shard process.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

/// Construction error for a [`NodeId`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeIdError {
    Empty,
    NonCanonical,
}

/// Runtime state of one physical logical-shard owner.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalShardState {
    Unassigned = 1,
    Recovering = 2,
    Serving = 3,
    Draining = 4,
    ReadOnly = 5,
}

/// Fail-closed error for an unknown logical-shard state discriminant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownLogicalShardState(pub u8);

/// Immutable checkpoint image published by a fenced shard owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointRef {
    pub object_key: String,
    pub lsn: u64,
    pub image_bytes: u64,
    pub image_digest: String,
    /// Digest of the logical metadata state at `lsn`.
    pub digest: String,
    /// Provider-neutral, strict object receipt used to locate and verify the
    /// manifest and every chunk without object listing.
    pub receipt: Vec<u8>,
}

/// One immutable segment in the ordered shared-log chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogSegmentRef {
    pub segment_key: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub digest: String,
    /// Provider-neutral, strict object receipt used to locate and verify the
    /// manifest and every chunk without object listing.
    pub receipt: Vec<u8>,
}

/// Complete ordered shared-log chain above the retained checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRef {
    pub segments: Vec<LogSegmentRef>,
    pub durable_lsn: u64,
    pub digest: String,
}

/// One owner-fenced publication of the durable recovery frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPublication {
    pub checkpoint: Option<CheckpointRef>,
    pub log: Option<LogRef>,
    pub durable_lsn: u64,
}

/// Durable intent for one immutable recovery-log upload.
///
/// The opaque plan is produced by `nokv-object` and must be persisted before
/// its first object create. Control keeps the typed boundary fields so the
/// final recovery publication can be checked without depending on the object
/// package or provider coordinates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryUploadIntent {
    pub object_namespace_id: ObjectNamespaceId,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub previous_chain_digest: String,
    pub last_chain_digest: String,
    pub segment_digest: String,
    pub manifest_key: String,
    pub receipt: Vec<u8>,
    pub plan: Vec<u8>,
}

/// Durable, immutable root-to-logical-shard affinity plus its CAS fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootPlacement {
    pub root_id: RootId,
    pub logical_shard_id: LogicalShardId,
    pub placement_generation: PlacementGeneration,
    pub lifecycle: RootPlacementLifecycle,
}

/// Immutable provider-neutral binding from one root to its artifact namespace.
///
/// This is separate from placement lifecycle so v1 placements remain readable
/// and a legacy root can acquire the missing identity exactly once without
/// pretending that its shard affinity changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootObjectNamespaceBinding {
    pub root_id: RootId,
    pub object_namespace_id: ObjectNamespaceId,
}

/// Immutable admission binding from one root to its stable Agent principal.
///
/// Presentation paths and process identities never participate in this
/// authority. One Agent may own multiple roots, but one root can never be
/// rebound to a different Agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootAgentBinding {
    pub root_id: RootId,
    pub agent_id: AgentId,
}

/// Durable ownership and recovery state for one logical metadata shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalShardRecord {
    pub logical_shard_id: LogicalShardId,
    /// Last owner admitted into this durable generation. Session liveness is
    /// proved separately by the backend's lease-attached session key.
    pub owner: Option<NodeId>,
    /// `None` means the shard has never had an owner. Once assigned, the last
    /// epoch remains present after release and every successor must increment it.
    pub owner_epoch: Option<OwnerEpoch>,
    /// Backend lease identity installed with this record. A non-zero value may
    /// remain after lease expiry; only the separate session key proves that it
    /// is still live. A clean `Serving` release resets it to zero.
    pub lease_id: u64,
    pub state: LogicalShardState,
    /// Endpoint admitted with `owner`. It may describe an expired session
    /// retained for recovery reconciliation. `None` only when unassigned.
    pub endpoint: Option<String>,
    pub checkpoint: Option<CheckpointRef>,
    pub log: Option<LogRef>,
    pub durable_lsn: u64,
    /// Exact object plan installed before the first immutable create and
    /// cleared atomically with the corresponding recovery publication.
    pub pending_recovery_upload: Option<RecoveryUploadIntent>,
}

/// Exact owner fence presented to every owner-only mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalShardLease {
    pub logical_shard_id: LogicalShardId,
    pub owner: NodeId,
    pub owner_epoch: OwnerEpoch,
    pub lease_id: u64,
}

impl NodeId {
    /// Construct a canonical, non-empty endpoint identity.
    pub fn new(value: impl Into<String>) -> Result<Self, NodeIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NodeIdError::Empty);
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(NodeIdError::NonCanonical);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for NodeIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("node id must not be empty"),
            Self::NonCanonical => formatter
                .write_str("node id must not contain surrounding whitespace or control characters"),
        }
    }
}

impl std::error::Error for NodeIdError {}

impl TryFrom<u8> for LogicalShardState {
    type Error = UnknownLogicalShardState;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Unassigned),
            2 => Ok(Self::Recovering),
            3 => Ok(Self::Serving),
            4 => Ok(Self::Draining),
            5 => Ok(Self::ReadOnly),
            value => Err(UnknownLogicalShardState(value)),
        }
    }
}

impl From<LogicalShardState> for u8 {
    fn from(value: LogicalShardState) -> Self {
        value as u8
    }
}

impl fmt::Display for UnknownLogicalShardState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown LogicalShardState durable discriminant {}",
            self.0
        )
    }
}

impl std::error::Error for UnknownLogicalShardState {}

impl LogicalShardRecord {
    pub fn unassigned(logical_shard_id: LogicalShardId) -> Self {
        Self {
            logical_shard_id,
            owner: None,
            owner_epoch: None,
            lease_id: 0,
            state: LogicalShardState::Unassigned,
            endpoint: None,
            checkpoint: None,
            log: None,
            durable_lsn: 0,
            pending_recovery_upload: None,
        }
    }
}

pub(crate) fn endpoint_is_canonical(endpoint: &str) -> bool {
    !endpoint.is_empty() && endpoint.trim() == endpoint && !endpoint.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_rejects_empty_and_noncanonical_identities() {
        assert_eq!(NodeId::new("").unwrap_err(), NodeIdError::Empty);
        assert_eq!(
            NodeId::new(" node-a").unwrap_err(),
            NodeIdError::NonCanonical
        );
        assert_eq!(
            NodeId::new("node-a\n").unwrap_err(),
            NodeIdError::NonCanonical
        );
        assert_eq!(NodeId::new("node-a").unwrap().as_str(), "node-a");
    }

    #[test]
    fn logical_shard_state_rejects_unknown_discriminants() {
        assert_eq!(
            LogicalShardState::try_from(0),
            Err(UnknownLogicalShardState(0))
        );
        assert_eq!(
            LogicalShardState::try_from(6),
            Err(UnknownLogicalShardState(6))
        );
    }
}
