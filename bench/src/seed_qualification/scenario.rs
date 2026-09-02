/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::net::SocketAddr;

use nokv_protocol::{DiscoveredRoute, LogicalShardIdentity, OwnerEndpoint};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub(super) struct ScenarioResult {
    pub name: &'static str,
    pub status: &'static str,
    pub detail: String,
}

impl ScenarioResult {
    pub(super) fn pass(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: "PASS",
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct RouteEvidence {
    pub root_id: String,
    pub logical_shard_id: String,
    pub object_namespace_id: String,
    pub placement_generation: u64,
    pub owner_epoch: u64,
    pub session_generation: u64,
    pub owner_endpoint: String,
    pub route_state: String,
}

impl From<&DiscoveredRoute> for RouteEvidence {
    fn from(route: &DiscoveredRoute) -> Self {
        Self {
            root_id: encode(&route.root_id.0),
            logical_shard_id: encode(&route.logical_shard_id.0),
            object_namespace_id: encode(&route.object_namespace_id.0),
            placement_generation: route.placement_generation,
            owner_epoch: route.owner_epoch,
            session_generation: route.session_generation,
            owner_endpoint: route.owner_endpoint.to_string(),
            route_state: format!("{:?}", route.route_state),
        }
    }
}

pub(super) fn require_takeover(
    initial: &DiscoveredRoute,
    successor: &DiscoveredRoute,
) -> Result<(), String> {
    if initial.root_id != successor.root_id
        || initial.logical_shard_id != successor.logical_shard_id
        || initial.object_namespace_id != successor.object_namespace_id
        || initial.placement_generation != successor.placement_generation
    {
        return Err("owner takeover changed an immutable route identity".to_owned());
    }
    if successor.owner_epoch <= initial.owner_epoch {
        return Err("owner takeover did not advance owner_epoch".to_owned());
    }
    if successor.session_generation <= initial.session_generation {
        return Err("owner takeover did not advance session_generation".to_owned());
    }
    if successor.owner_endpoint == initial.owner_endpoint {
        return Err("owner takeover did not change the owner endpoint".to_owned());
    }
    Ok(())
}

pub(super) fn endpoint_drift(
    authoritative: &DiscoveredRoute,
    endpoint: SocketAddr,
) -> Result<DiscoveredRoute, String> {
    let mut drifted = authoritative.clone();
    drifted.owner_endpoint =
        OwnerEndpoint::new(endpoint.to_string()).map_err(|error| error.to_string())?;
    Ok(drifted)
}

pub(super) fn immutable_identity_drift(authoritative: &DiscoveredRoute) -> DiscoveredRoute {
    let mut drifted = authoritative.clone();
    let mut shard = drifted.logical_shard_id.0;
    shard[0] ^= 0x80;
    drifted.logical_shard_id = LogicalShardIdentity(shard);
    drifted
}

fn encode(value: &[u8]) -> String {
    crate::qualification_runtime::lowercase_hex(value)
}

#[cfg(test)]
mod tests {
    use nokv_protocol::{ObjectNamespaceIdentity, OwnerEndpoint, RootIdentity, RouteState};

    use super::*;

    fn route(epoch: u64, session: u64, endpoint: &str) -> DiscoveredRoute {
        DiscoveredRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: ObjectNamespaceIdentity([3; 16]),
            placement_generation: 4,
            owner_epoch: epoch,
            session_generation: session,
            owner_endpoint: OwnerEndpoint::new(endpoint).unwrap(),
            route_state: RouteState::Serving,
        }
    }

    #[test]
    fn takeover_requires_both_fences_and_a_new_endpoint() {
        let initial = route(5, 6, "127.0.0.1:17001");
        assert!(require_takeover(&initial, &route(6, 7, "127.0.0.1:17002")).is_ok());
        assert!(require_takeover(&initial, &route(5, 7, "127.0.0.1:17002")).is_err());
        assert!(require_takeover(&initial, &route(6, 6, "127.0.0.1:17002")).is_err());
        assert!(require_takeover(&initial, &route(6, 7, "127.0.0.1:17001")).is_err());
    }

    #[test]
    fn synthetic_faults_preserve_protocol_validity() {
        let authoritative = route(5, 6, "127.0.0.1:17001");
        let drifted = endpoint_drift(&authoritative, "127.0.0.1:17002".parse().unwrap()).unwrap();
        drifted.validate().unwrap();
        immutable_identity_drift(&authoritative).validate().unwrap();
    }
}
