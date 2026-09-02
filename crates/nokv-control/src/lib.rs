//! Provider-neutral NoKV catalog and distributed ownership fencing.
//!
//! This crate owns only control-plane state. Namespace metadata, artifact
//! lifecycle, storage-engine details, and client routing policy stay in their
//! respective packages.

mod catalog;
mod distributed_store;
mod errors;
mod node_id;
mod ownership;

pub use catalog::{
    validate_root_catalog_transition, validate_shard_catalog_transition, CatalogEntryState,
    CreateOutcome, RootCatalogEntry, ShardCatalogEntry, StoreId, StoreManifest, StoreProvider,
    MAX_CREATED_BY_VERSION_BYTES, PROVIDER_NAMESPACE_DIGEST_BYTES, STORE_ID_BYTES,
    SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
pub use distributed_store::DistributedControlStore;
pub use errors::ControlError;
pub use node_id::{NodeId, NodeIdError};
pub use nokv_types::{
    AgentId, LogicalShardId, ObjectNamespaceId, OwnerEpoch, PlacementGeneration, RootId,
};
pub use ownership::{
    plan_fail_closed, plan_heartbeat_renewal, plan_owner_acquisition, plan_owner_release,
    plan_route_activation, HeartbeatSequence, OwnerHeartbeat, OwnerSession, OwnershipSnapshot,
    OwnershipUpdate, RpcEndpoint, SessionGeneration, ShardRoute, ShardRouteState,
    MAX_RPC_ENDPOINT_BYTES,
};
