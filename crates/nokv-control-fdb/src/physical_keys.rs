/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_control::{ControlError, LogicalShardId, RootId};
use nokv_fdb::{lexicographic_successor, FdbStorePrefix, FdbSubspace, FdbSubspaceKind};

/// Exact physical keys below one versioned NoKV FoundationDB store prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbControlKeys {
    system: FdbSubspace,
    roots: FdbSubspace,
    shards: FdbSubspace,
    routes: FdbSubspace,
    sessions: FdbSubspace,
    heartbeats: FdbSubspace,
}

impl FdbControlKeys {
    pub fn new(prefix: &FdbStorePrefix) -> Self {
        Self {
            system: prefix.subspace(FdbSubspaceKind::System),
            roots: prefix.subspace(FdbSubspaceKind::CatalogRoot),
            shards: prefix.subspace(FdbSubspaceKind::CatalogShard),
            routes: prefix.subspace(FdbSubspaceKind::RouteShard),
            sessions: prefix.subspace(FdbSubspaceKind::LeaseSession),
            heartbeats: prefix.subspace(FdbSubspaceKind::LeaseHeartbeat),
        }
    }

    pub fn manifest_key(&self) -> Vec<u8> {
        self.system
            .component(b"manifest")
            .expect("manifest is a bounded static FDB component")
            .as_bytes()
            .to_vec()
    }

    pub fn root_catalog_key(&self, root_id: &RootId) -> Vec<u8> {
        component_key(&self.roots, root_id.as_bytes())
    }

    pub fn root_catalog_range(&self) -> Result<(Vec<u8>, Vec<u8>), ControlError> {
        subspace_range(&self.roots, "root catalog")
    }

    pub fn shard_catalog_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        component_key(&self.shards, logical_shard_id.as_bytes())
    }

    pub fn shard_catalog_range(&self) -> Result<(Vec<u8>, Vec<u8>), ControlError> {
        subspace_range(&self.shards, "shard catalog")
    }

    pub fn route_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        component_key(&self.routes, logical_shard_id.as_bytes())
    }

    pub fn session_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        component_key(&self.sessions, logical_shard_id.as_bytes())
    }

    pub fn heartbeat_key(&self, logical_shard_id: &LogicalShardId) -> Vec<u8> {
        component_key(&self.heartbeats, logical_shard_id.as_bytes())
    }
}

fn component_key(subspace: &FdbSubspace, component: &[u8]) -> Vec<u8> {
    subspace
        .component(component)
        .expect("fixed NoKV identities fit an FDB key component")
        .as_bytes()
        .to_vec()
}

fn subspace_range(
    subspace: &FdbSubspace,
    description: &'static str,
) -> Result<(Vec<u8>, Vec<u8>), ControlError> {
    let begin = subspace.as_bytes().to_vec();
    let end = lexicographic_successor(&begin).ok_or_else(|| {
        ControlError::InvalidOptions(format!(
            "FoundationDB {description} subspace has no lexicographic successor"
        ))
    })?;
    Ok((begin, end))
}
