//! Durable NoKV root placement, logical-shard ownership fencing, and recovery
//! publication.
//!
//! This crate owns only control-plane state. Namespace metadata, artifact
//! lifecycle, storage-engine details, and client routing policy stay in their
//! respective packages.

mod catalog;
mod codec;
mod distributed_store;
mod errors;
#[cfg(feature = "etcd")]
mod etcd;
mod options;
mod ownership;
mod store;
mod types;

pub use catalog::{
    validate_root_catalog_transition, validate_shard_catalog_transition, CatalogEntryState,
    CreateOutcome, RootCatalogEntry, ShardCatalogEntry, StoreId, StoreManifest, StoreProvider,
    MAX_CREATED_BY_VERSION_BYTES, PROVIDER_NAMESPACE_DIGEST_BYTES, STORE_ID_BYTES,
    SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
pub use codec::{
    decode_logical_shard_record, decode_logical_shard_record_value,
    decode_logical_shard_recovery_state, decode_root_agent_binding,
    decode_root_object_namespace_binding, decode_root_placement,
    encode_logical_shard_recovery_state, encode_logical_shard_routing_record,
    encode_root_agent_binding, encode_root_object_namespace_binding, encode_root_placement,
    DecodedLogicalShardRecord, LogicalShardRecordWireKind, LOGICAL_SHARD_RECOVERY_CODEC_VERSION,
    LOGICAL_SHARD_ROUTING_CODEC_VERSION,
};
pub use distributed_store::DistributedControlStore;
pub use errors::ControlError;
#[cfg(feature = "etcd")]
pub use etcd::EtcdControlStore;
pub use options::EtcdControlStoreOptions;
pub use ownership::{
    plan_fail_closed, plan_heartbeat_renewal, plan_owner_acquisition, plan_owner_release,
    plan_route_activation, HeartbeatSequence, OwnerHeartbeat, OwnerSession, OwnershipSnapshot,
    OwnershipUpdate, RpcEndpoint, SessionGeneration, ShardRoute, ShardRouteState,
    MAX_RPC_ENDPOINT_BYTES,
};
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
