/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_control::{OwnershipSnapshot, ShardRouteState};
use serde::Serialize;

use crate::qualification_runtime::{lowercase_hex, unix_millis};

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct OwnershipEvidence {
    pub observed_unix_millis: u64,
    pub logical_shard_id: String,
    pub route_state: String,
    pub owner: Option<String>,
    pub endpoint: Option<String>,
    pub owner_epoch: Option<u64>,
    pub session_generation: Option<u64>,
    pub heartbeat_sequence: Option<u64>,
}

impl From<&OwnershipSnapshot> for OwnershipEvidence {
    fn from(snapshot: &OwnershipSnapshot) -> Self {
        let route = snapshot.route();
        Self {
            observed_unix_millis: unix_millis(),
            logical_shard_id: lowercase_hex(route.logical_shard_id().as_bytes()),
            route_state: format!("{:?}", route.state()),
            owner: route.owner().map(ToString::to_string),
            endpoint: route
                .endpoint()
                .map(|endpoint| endpoint.as_str().to_owned()),
            owner_epoch: route.owner_epoch().map(|epoch| epoch.get()),
            session_generation: route
                .session_generation()
                .map(|generation| generation.get()),
            heartbeat_sequence: snapshot
                .heartbeat()
                .map(|heartbeat| heartbeat.sequence().get()),
        }
    }
}

pub(super) fn require_state(
    snapshot: &OwnershipSnapshot,
    expected: ShardRouteState,
) -> Result<(), String> {
    if snapshot.route().state() != expected {
        return Err(format!(
            "expected route state {expected:?}, observed {:?}",
            snapshot.route().state()
        ));
    }
    Ok(())
}

pub(super) fn require_successor(
    initial: &OwnershipSnapshot,
    successor: &OwnershipSnapshot,
) -> Result<(), String> {
    let first = initial
        .session()
        .ok_or_else(|| "initial ownership snapshot has no live session".to_owned())?;
    let next = successor
        .session()
        .ok_or_else(|| "successor ownership snapshot has no live session".to_owned())?;
    if first.logical_shard_id() != next.logical_shard_id() {
        return Err("owner takeover changed the logical shard identity".to_owned());
    }
    if next.owner_epoch() <= first.owner_epoch() {
        return Err("owner takeover did not advance owner_epoch".to_owned());
    }
    if next.session_generation() <= first.session_generation() {
        return Err("owner takeover did not advance session_generation".to_owned());
    }
    if next == first {
        return Err("owner takeover retained the stale exact session".to_owned());
    }
    require_state(successor, ShardRouteState::Serving)
}

#[cfg(test)]
mod tests {
    use nokv_control::{
        HeartbeatSequence, NodeId, OwnerHeartbeat, OwnerSession, RpcEndpoint, SessionGeneration,
        ShardRoute,
    };
    use nokv_types::{LogicalShardId, OwnerEpoch};

    use super::*;

    fn snapshot(epoch: u64, generation: u64, state: ShardRouteState) -> OwnershipSnapshot {
        let shard = LogicalShardId::from_bytes([1; 16]);
        let owner = NodeId::new(format!("node-{epoch}")).unwrap();
        let endpoint = RpcEndpoint::new(format!("127.0.0.1:{}", 17000 + epoch)).unwrap();
        let epoch = OwnerEpoch::new(epoch).unwrap();
        let generation = SessionGeneration::new(generation).unwrap();
        let session = OwnerSession::new(shard, owner.clone(), epoch, generation);
        OwnershipSnapshot::new(
            ShardRoute::new(
                shard,
                state,
                Some(owner),
                Some(endpoint),
                Some(epoch),
                Some(generation),
            )
            .unwrap(),
            Some(session.clone()),
            Some(OwnerHeartbeat::new(
                session,
                HeartbeatSequence::new(1).unwrap(),
            )),
        )
        .unwrap()
    }

    #[test]
    fn successor_requires_both_fences_to_advance() {
        let initial = snapshot(1, 1, ShardRouteState::Serving);
        assert!(require_successor(&initial, &snapshot(2, 2, ShardRouteState::Serving)).is_ok());
        assert!(require_successor(&initial, &snapshot(1, 2, ShardRouteState::Serving)).is_err());
        assert!(require_successor(&initial, &snapshot(2, 1, ShardRouteState::Serving)).is_err());
    }
}
