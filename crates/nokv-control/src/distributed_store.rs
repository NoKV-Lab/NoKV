/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{
    ControlError, CreateOutcome, LogicalShardId, OwnerHeartbeat, OwnerSession, OwnershipSnapshot,
    RootCatalogEntry, RootId, RpcEndpoint, ShardCatalogEntry, ShardRoute, StoreManifest,
};

/// Provider-neutral catalog and ownership contract for shared metadata modes.
///
/// Provider implementations perform one explicit physical transaction per
/// mutation. Callers reconcile unknown commit outcomes through exact readback;
/// implementations do not retry raw commits.
pub trait DistributedControlStore: Send + Sync {
    fn manifest(&self) -> &StoreManifest;

    fn create_root_catalog(
        &self,
        entry: RootCatalogEntry,
    ) -> Result<CreateOutcome<RootCatalogEntry>, ControlError>;
    fn get_root_catalog(&self, root_id: &RootId) -> Result<Option<RootCatalogEntry>, ControlError>;
    fn list_root_catalog(&self) -> Result<Vec<RootCatalogEntry>, ControlError>;
    fn compare_and_set_root_catalog(
        &self,
        expected: &RootCatalogEntry,
        next: RootCatalogEntry,
    ) -> Result<RootCatalogEntry, ControlError>;

    /// Create the stable shard catalog and its initial Unassigned route in one
    /// transaction. The shard starts Provisioning and cannot be acquired until
    /// an exact CAS moves it to Ready.
    fn create_shard_catalog(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<CreateOutcome<ShardCatalogEntry>, ControlError>;
    fn get_shard_catalog(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<ShardCatalogEntry>, ControlError>;
    fn list_shard_catalog(&self) -> Result<Vec<ShardCatalogEntry>, ControlError>;
    fn compare_and_set_shard_catalog(
        &self,
        expected: &ShardCatalogEntry,
        next: ShardCatalogEntry,
    ) -> Result<ShardCatalogEntry, ControlError>;

    fn get_route(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<ShardRoute>, ControlError>;

    /// Read route, session, and heartbeat consistently and start or continue
    /// the caller's local monotonic TTL observation.
    fn observe_ownership(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<OwnershipSnapshot, ControlError>;

    /// Acquire the fenced session used only to initialize a Provisioning
    /// shard. The route remains Activating and cannot be published until the
    /// shard catalog reaches Ready.
    fn acquire_provisioning_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: crate::NodeId,
        endpoint: RpcEndpoint,
    ) -> Result<OwnerSession, ControlError>;

    /// Acquire an unassigned shard immediately, or take over only after the
    /// exact observed session/heartbeat pair has remained unchanged for the
    /// configured local TTL.
    fn acquire_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: crate::NodeId,
        endpoint: RpcEndpoint,
    ) -> Result<OwnerSession, ControlError>;

    fn renew_owner(&self, session: &OwnerSession) -> Result<OwnerHeartbeat, ControlError>;
    fn activate_route(&self, session: &OwnerSession) -> Result<ShardRoute, ControlError>;
    fn fail_closed(&self, session: &OwnerSession) -> Result<ShardRoute, ControlError>;
    fn release_owner(&self, session: &OwnerSession) -> Result<ShardRoute, ControlError>;
}
