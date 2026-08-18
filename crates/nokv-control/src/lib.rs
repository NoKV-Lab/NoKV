//! Durable NoKV root placement, logical-shard ownership fencing, and recovery
//! publication.
//!
//! This crate owns only control-plane state. Namespace metadata, artifact
//! lifecycle, storage-engine details, and client routing policy stay in their
//! respective packages.

mod codec;
mod errors;
#[cfg(feature = "etcd")]
mod etcd;
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
pub use options::EtcdControlStoreOptions;
pub use store::{
    ControlStore, InMemoryControlStore, MAX_LOGICAL_SHARD_RECORD_BYTES,
    MAX_RECOVERY_LOG_RECEIPT_BYTES, MAX_RECOVERY_LOG_SEGMENTS,
};
pub use types::{
    AgentId, CheckpointRef, LogRef, LogSegmentRef, LogicalShardId, LogicalShardLease,
    LogicalShardRecord, LogicalShardRecoveryState, LogicalShardState, NodeId, NodeIdError,
    ObjectNamespaceId, OwnerEpoch, PlacementGeneration, RecoveryPublication, RecoveryUploadIntent,
    RootAgentBinding, RootId, RootObjectNamespaceBinding, RootPlacement, RootPlacementLifecycle,
    UnknownLogicalShardState,
};
