//! Durable NoKV root placement, logical-shard ownership fencing, and recovery
//! publication.
//!
//! This crate owns only control-plane state. Namespace metadata, artifact
//! lifecycle, storage-engine details, and client routing policy stay in their
//! respective packages.

mod codec;
#[cfg(test)]
mod conformance;
mod errors;
#[cfg(feature = "etcd")]
mod etcd;
#[cfg(feature = "etcd")]
mod etcd_identity;
mod identity;
#[cfg(test)]
mod identity_conformance;
#[cfg(unix)]
mod local;
mod options;
mod store;
mod types;

pub use codec::{
    decode_logical_shard_record, decode_logical_shard_record_value,
    decode_logical_shard_recovery_state, decode_root_agent_binding,
    decode_root_object_namespace_binding, decode_root_placement,
    encode_logical_shard_recovery_state, encode_logical_shard_routing_record,
    encode_root_agent_binding, encode_root_object_namespace_binding, encode_root_placement,
    DecodedLogicalShardRecord, LogicalShardRecordWireKind, LOGICAL_SHARD_RECOVERY_CODEC_VERSION,
    LOGICAL_SHARD_ROUTING_CODEC_VERSION,
};
pub use errors::ControlError;
#[cfg(feature = "etcd")]
pub use etcd::EtcdControlStore;
pub use identity::{IdentityLimits, IdentityStore};
#[cfg(unix)]
pub use local::LocalControlStore;
pub use options::EtcdControlStoreOptions;
pub use store::{
    ControlStore, InMemoryControlStore, MAX_LOGICAL_SHARD_RECORD_BYTES,
    MAX_RECOVERY_LOG_RECEIPT_BYTES, MAX_RECOVERY_LOG_SEGMENTS,
};
pub use types::{
    AgentId, CheckpointRef, CredentialBinding, CredentialFingerprint, LogRef, LogSegmentRef,
    LogicalShardId, LogicalShardLease, LogicalShardRecord, LogicalShardRecoveryState,
    LogicalShardState, NodeId, NodeIdError, ObjectNamespaceId, OwnerEpoch, PlacementGeneration,
    PrincipalId, PrincipalRecord, RecoveryPublication, RecoveryUploadIntent, Role, RoleBinding,
    RoleScope, RootAgentBinding, RootId, RootObjectNamespaceBinding, RootPlacement,
    RootPlacementLifecycle, RootPresentationBinding, RootTenantBinding, TenantId, TenantPolicy,
    TenantRecord, UnknownLogicalShardState,
};
