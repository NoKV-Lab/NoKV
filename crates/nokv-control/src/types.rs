use std::fmt;

pub use nokv_types::{
    CommitVersion, ConsistencyDomainId, LogicalShardId, MetadataAuthorityGeneration,
    MetadataAuthorityId, MetadataAuthorityRevision, MetadataContractDigest,
    MetadataMigrationTargetBinding, OperationId, OwnerEpoch, OwnerIncarnationId,
    PlacementGeneration, RootId, RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
    RootPlacementLifecycle, SourceQuiesceReceipt, TargetActivationToken, SHA256_BYTES,
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

/// Stable configuration identity for one metadata-provider installation.
///
/// Provider credentials and backend-specific configuration remain outside the
/// durable authority record. The fingerprint in [`MetadataAuthorityBinding`]
/// binds this name to the exact resolved configuration used at admission.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataProviderProfileId(String);

/// Construction error for a [`MetadataProviderProfileId`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataProviderProfileIdError {
    Empty,
    NonCanonical,
    TooLong { bytes: usize, max: usize },
}

/// Exact authority fence carried by a physical owner session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataAuthorityFence {
    pub logical_shard_id: LogicalShardId,
    pub authority_id: MetadataAuthorityId,
    pub authority_generation: MetadataAuthorityGeneration,
}

/// Exact root-layout and partition fence installed by a serving owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootLayoutFence {
    pub root_id: RootId,
    pub profile: RootLayoutProfile,
    pub layout_generation: RootLayoutGeneration,
    pub partition_id: RootPartitionId,
    pub logical_shard_id: LogicalShardId,
}

/// Provider-neutral identity of one authority installation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataAuthorityBinding {
    pub authority_id: MetadataAuthorityId,
    pub provider_profile_id: MetadataProviderProfileId,
    /// SHA-256 of the resolved, secret-free provider configuration.
    pub profile_fingerprint: [u8; SHA256_BYTES],
    pub consistency_domain_id: ConsistencyDomainId,
    pub contract_digest: MetadataContractDigest,
}

pub use nokv_types::MetadataRecoveryFrontier;

/// Durable phase of one authority migration.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataMigrationPhase {
    Preparing = 1,
    Copying = 2,
    CatchingUp = 3,
    Quiescing = 4,
    ReadyToCutover = 5,
    CutoverComplete = 6,
    Aborted = 7,
}

/// Fail-closed error for an unknown metadata-migration phase discriminant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownMetadataMigrationPhase(pub u8);

/// Durable evidence and bindings for one authority migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataMigration {
    pub migration_id: OperationId,
    pub source: MetadataAuthorityBinding,
    pub target: MetadataAuthorityBinding,
    pub phase: MetadataMigrationPhase,
    pub source_frontier: Option<MetadataRecoveryFrontier>,
    pub target_frontier: Option<MetadataRecoveryFrontier>,
    pub cutover_frontier: Option<MetadataRecoveryFrontier>,
    /// Provider-issued proof that the source installed its durable write
    /// barrier at the final source frontier.
    pub source_quiesce_receipt: Option<SourceQuiesceReceipt>,
    /// Deterministic token issued by the Ready-to-Cutover control record and
    /// consumed by the exact target authority after cutover.
    pub target_activation_token: Option<TargetActivationToken>,
}

/// Single durable authority record for one logical metadata shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataAuthorityRecord {
    pub logical_shard_id: LogicalShardId,
    pub record_revision: MetadataAuthorityRevision,
    pub authority_generation: MetadataAuthorityGeneration,
    pub active: MetadataAuthorityBinding,
    pub migration: Option<MetadataMigration>,
}

/// Whether one atomic fresh-root provisioning call installed or replayed the
/// complete control-plane bundle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshRootProvisioningDisposition {
    Created,
    Replayed,
}

/// Typed result of atomically provisioning a new root and metadata authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshRootProvisioningOutcome {
    pub disposition: FreshRootProvisioningDisposition,
    /// Current shard state. It may have advanced after a completed provision.
    pub logical_shard: LogicalShardRecord,
    /// The exact generation-one authority requested by the caller.
    pub metadata_authority: MetadataAuthorityRecord,
    /// Current placement, either initial Provisioning or legally activated.
    pub root_placement: RootPlacement,
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
}

/// One immutable segment in the ordered shared-log chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogSegmentRef {
    pub segment_key: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub digest: String,
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

/// Durable binding of one root partition to one logical shard.
///
/// The current control key admits exactly one record per root and therefore
/// accepts only the `SingleShardRoot` profile. The explicit partition and
/// layout fields are nevertheless durable fences; a future partition map can
/// key the same binding by `(root_id, partition_id)` without overloading a
/// provider profile or placement lifecycle generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootPlacement {
    pub root_id: RootId,
    pub layout_profile: RootLayoutProfile,
    pub layout_generation: RootLayoutGeneration,
    pub partition_id: RootPartitionId,
    pub logical_shard_id: LogicalShardId,
    pub placement_generation: PlacementGeneration,
    pub lifecycle: RootPlacementLifecycle,
}

/// Durable ownership and recovery state for one logical metadata shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalShardRecord {
    pub logical_shard_id: LogicalShardId,
    pub owner: Option<NodeId>,
    /// `None` means the shard has never had an owner. Once assigned, the last
    /// epoch remains present after release and every successor must increment it.
    pub owner_epoch: Option<OwnerEpoch>,
    /// Never-reused identity of the current or most recently released owner
    /// process/session. It remains present with `owner_epoch` after release.
    pub owner_incarnation_id: Option<OwnerIncarnationId>,
    /// Backend lease identity. Zero means there is no current owner session.
    pub lease_id: u64,
    pub state: LogicalShardState,
    /// Reachable endpoint of the current owner. `None` while unowned.
    pub endpoint: Option<String>,
    pub checkpoint: Option<CheckpointRef>,
    pub log: Option<LogRef>,
    pub durable_lsn: u64,
}

/// Lifetime model enforced by one control-store owner session.
///
/// This is a closed, provider-neutral capability rather than an inference from
/// a backend name or configured TTL. A finite backend may be admitted for
/// Serving only after the complete owner-expiry contract is qualified across
/// control, bootstrap, request execution, lifecycle work, and supervision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerLeaseModel {
    /// Ownership remains current until an exact release or successor mutation.
    NonExpiring,
    /// The backend expires ownership using an authoritative finite TTL.
    FiniteAuthoritativeTtl,
}

/// Exact owner fence presented to every owner-only mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalShardLease {
    pub logical_shard_id: LogicalShardId,
    pub owner: NodeId,
    pub owner_epoch: OwnerEpoch,
    pub owner_incarnation_id: OwnerIncarnationId,
    pub lease_id: u64,
    pub authority: MetadataAuthorityFence,
}

/// Closed result of releasing one exact owner session.
///
/// `OutcomeUnknown` is deliberately a value rather than a backend error: the
/// caller must retain the exact lease capability and reconcile the same
/// release. It must not acquire, resume, or reopen a provider from that state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerReleaseOutcome {
    /// This call atomically removed the exact owner session.
    Released(LogicalShardRecord),
    /// The same owner epoch is already durably unowned.
    AlreadyReleased(LogicalShardRecord),
    /// A later or different owner state has made this lease definitively stale.
    Superseded(LogicalShardRecord),
    /// No terminal control-plane state could be proved after the release
    /// attempt. Retrying the exact release is the only admitted mutation.
    OutcomeUnknown,
}

impl OwnerReleaseOutcome {
    pub fn terminal_record(&self) -> Option<&LogicalShardRecord> {
        match self {
            Self::Released(record) | Self::AlreadyReleased(record) | Self::Superseded(record) => {
                Some(record)
            }
            Self::OutcomeUnknown => None,
        }
    }
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

impl MetadataProviderProfileId {
    pub const MAX_BYTES: usize = 128;

    pub fn new(value: impl Into<String>) -> Result<Self, MetadataProviderProfileIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetadataProviderProfileIdError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(MetadataProviderProfileIdError::TooLong {
                bytes: value.len(),
                max: Self::MAX_BYTES,
            });
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(MetadataProviderProfileIdError::NonCanonical);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MetadataProviderProfileId {
    type Error = MetadataProviderProfileIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for MetadataProviderProfileId {
    type Error = MetadataProviderProfileIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for MetadataProviderProfileId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for MetadataProviderProfileIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("metadata provider profile id must not be empty"),
            Self::NonCanonical => formatter.write_str(
                "metadata provider profile id must not contain surrounding whitespace or control characters",
            ),
            Self::TooLong { bytes, max } => write!(
                formatter,
                "metadata provider profile id contains {bytes} bytes, maximum is {max}"
            ),
        }
    }
}

impl std::error::Error for MetadataProviderProfileIdError {}

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

impl TryFrom<u8> for MetadataMigrationPhase {
    type Error = UnknownMetadataMigrationPhase;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Preparing),
            2 => Ok(Self::Copying),
            3 => Ok(Self::CatchingUp),
            4 => Ok(Self::Quiescing),
            5 => Ok(Self::ReadyToCutover),
            6 => Ok(Self::CutoverComplete),
            7 => Ok(Self::Aborted),
            value => Err(UnknownMetadataMigrationPhase(value)),
        }
    }
}

impl From<MetadataMigrationPhase> for u8 {
    fn from(value: MetadataMigrationPhase) -> Self {
        value as u8
    }
}

impl fmt::Display for UnknownMetadataMigrationPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown MetadataMigrationPhase durable discriminant {}",
            self.0
        )
    }
}

impl std::error::Error for UnknownMetadataMigrationPhase {}

impl MetadataAuthorityRecord {
    pub fn fence(&self) -> MetadataAuthorityFence {
        MetadataAuthorityFence {
            logical_shard_id: self.logical_shard_id,
            authority_id: self.active.authority_id,
            authority_generation: self.authority_generation,
        }
    }
}

impl RootPlacement {
    pub fn layout_fence(&self) -> RootLayoutFence {
        RootLayoutFence {
            root_id: self.root_id,
            profile: self.layout_profile,
            layout_generation: self.layout_generation,
            partition_id: self.partition_id,
            logical_shard_id: self.logical_shard_id,
        }
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
            owner_incarnation_id: None,
            lease_id: 0,
            state: LogicalShardState::Unassigned,
            endpoint: None,
            checkpoint: None,
            log: None,
            durable_lsn: 0,
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

    #[test]
    fn metadata_profile_id_is_bounded_and_canonical() {
        assert_eq!(
            MetadataProviderProfileId::new("").unwrap_err(),
            MetadataProviderProfileIdError::Empty
        );
        assert_eq!(
            MetadataProviderProfileId::new(" profile").unwrap_err(),
            MetadataProviderProfileIdError::NonCanonical
        );
        assert!(matches!(
            MetadataProviderProfileId::new("x".repeat(MetadataProviderProfileId::MAX_BYTES + 1)),
            Err(MetadataProviderProfileIdError::TooLong { .. })
        ));
    }

    #[test]
    fn metadata_migration_phase_rejects_unknown_discriminants() {
        assert_eq!(
            MetadataMigrationPhase::try_from(0),
            Err(UnknownMetadataMigrationPhase(0))
        );
        assert_eq!(
            MetadataMigrationPhase::try_from(8),
            Err(UnknownMetadataMigrationPhase(8))
        );
    }
}
