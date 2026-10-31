use std::fmt;
use std::path::PathBuf;

use nokv_types::ErrorClass;

use crate::{
    CredentialFingerprint, LogicalShardId, LogicalShardLease, LogicalShardState, NodeId,
    ObjectNamespaceId, OwnerEpoch, PrincipalId, RootId, RootPlacement, TenantId,
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
    RecoveryUploadConflict {
        logical_shard_id: LogicalShardId,
        reason: String,
    },
    TenantNotFound(TenantId),
    PrincipalNotFound {
        tenant_id: TenantId,
        principal_id: PrincipalId,
    },
    /// The fingerprint already authenticates another principal of the tenant,
    /// or the same principal with another expiry.
    CredentialAlreadyBound {
        tenant_id: TenantId,
        fingerprint: CredentialFingerprint,
    },
    RootTenantAlreadyBound {
        root_id: RootId,
    },
    /// The root already presents under another Workbench root.
    RootPresentationAlreadyBound {
        root_id: RootId,
    },
    /// A root-scoped role names a root that the tenant does not own.
    RootNotInTenant {
        root_id: RootId,
        tenant_id: TenantId,
    },
    TenantPolicyLimitExceeded {
        tenant_id: TenantId,
        record: &'static str,
        limit: usize,
    },
    InvalidRecord(String),
    InvalidOptions(String),
    /// The local control store refuses its directory, its `sessions`
    /// directory or a record file at `path`: group or other users can write
    /// it, another user owns it, or this process cannot create what it needs
    /// in it. Someone else may have changed the records, so an operator
    /// checks them and fixes the permissions. Nothing ran.
    ControlPathRefused {
        path: PathBuf,
        reason: String,
    },
    /// A durable control record declares a codec version this reader does not
    /// implement. The record itself is intact; the reader is too old.
    UnsupportedRecordVersion {
        record: &'static str,
        version: u8,
        supported: u8,
    },
    /// The control plane does not accept this process's identity: its
    /// certificate names no user, or authentication failed. Nothing ran.
    Unauthenticated(String),
    /// The control plane's roles do not grant the key this operation needs.
    /// Nothing ran.
    PermissionDenied(String),
    Codec(String),
    Backend(String),
}

impl ControlError {
    /// What this control-plane failure means for the caller.
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::InvalidEndpoint(_)
            | Self::InvalidOptions(_)
            | Self::InvalidPlacementMutation { .. } => ErrorClass::InvalidInput,
            Self::TenantNotFound(_) | Self::PrincipalNotFound { .. } => ErrorClass::NotFound,
            Self::RootPlacementAlreadyExists(_)
            | Self::CredentialAlreadyBound { .. }
            | Self::RootTenantAlreadyBound { .. }
            | Self::RootPresentationAlreadyBound { .. }
            | Self::RootNotInTenant { .. }
            | Self::RootAgentAlreadyBound { .. }
            | Self::RootObjectNamespaceAlreadyBound { .. }
            | Self::ImmutableShardAffinity { .. }
            | Self::LogicalShardAlreadyExists(_) => ErrorClass::PreconditionFailed,
            // Ownership is moving or contended: a later attempt observes the
            // settled state.
            Self::RootPlacementCasConflict { .. }
            | Self::LogicalShardAlreadyOwned { .. }
            | Self::PreviousOwnerSessionLive { .. }
            | Self::RecoveryAttemptPending { .. }
            | Self::RecoveryStateConflict { .. }
            | Self::StaleOwnerEpoch { .. }
            | Self::NotOwner { .. }
            | Self::StaleLease(_)
            | Self::RecoveryPublicationConflict { .. }
            | Self::RecoveryUploadConflict { .. }
            | Self::Backend(_) => ErrorClass::Transient,
            Self::UnsupportedRecordVersion { .. } => ErrorClass::Incompatible,
            Self::Unauthenticated(_) => ErrorClass::Unauthenticated,
            Self::PermissionDenied(_) => ErrorClass::PermissionDenied,
            Self::RootPlacementNotFound(_)
            | Self::LogicalShardNotFound(_)
            | Self::RootPlacementRequired(_)
            | Self::OwnerEpochExhausted(_)
            | Self::LeaseIdExhausted(_)
            | Self::TenantPolicyLimitExceeded { .. }
            | Self::InvalidRecord(_)
            | Self::ControlPathRefused { .. }
            | Self::Codec(_) => ErrorClass::OperatorRequired,
        }
    }
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
            Self::RecoveryUploadConflict {
                logical_shard_id,
                reason,
            } => write!(
                formatter,
                "recovery upload for logical shard {logical_shard_id:?} conflicts with durable state: {reason}"
            ),
            Self::TenantNotFound(tenant_id) => {
                write!(formatter, "tenant {tenant_id:?} was not found")
            }
            Self::PrincipalNotFound {
                tenant_id,
                principal_id,
            } => write!(
                formatter,
                "principal {principal_id:?} was not found in tenant {tenant_id:?}"
            ),
            Self::CredentialAlreadyBound {
                tenant_id,
                fingerprint,
            } => write!(
                formatter,
                "credential {fingerprint:?} is already bound differently in tenant {tenant_id:?}"
            ),
            Self::RootTenantAlreadyBound { root_id } => {
                write!(formatter, "root {root_id:?} is already bound to another tenant")
            }
            Self::RootPresentationAlreadyBound { root_id } => write!(
                formatter,
                "root {root_id:?} is already bound to another Workbench root"
            ),
            Self::RootNotInTenant { root_id, tenant_id } => write!(
                formatter,
                "root {root_id:?} is not owned by tenant {tenant_id:?}"
            ),
            Self::TenantPolicyLimitExceeded {
                tenant_id,
                record,
                limit,
            } => write!(
                formatter,
                "tenant {tenant_id:?} already holds the maximum of {limit} {record} records"
            ),
            Self::InvalidRecord(reason) => {
                write!(formatter, "invalid control record: {reason}")
            }
            Self::InvalidOptions(reason) => {
                write!(formatter, "invalid control store options: {reason}")
            }
            Self::ControlPathRefused { path, reason } => write!(
                formatter,
                "local control store refuses {}: {reason}",
                path.display()
            ),
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
            Self::Unauthenticated(reason) => {
                write!(formatter, "control plane does not accept this identity: {reason}")
            }
            Self::PermissionDenied(reason) => {
                write!(formatter, "control plane denies this operation: {reason}")
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
