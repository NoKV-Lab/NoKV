/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::time::Duration;

use nokv_control::{
    plan_heartbeat_renewal, plan_owner_acquisition, AgentId, CatalogEntryState, ControlError,
    LogicalShardId, NodeId, ObjectNamespaceId, OwnershipSnapshot, PlacementGeneration,
    RootCatalogEntry, RootId, RpcEndpoint, ShardCatalogEntry, ShardRoute, StoreId, StoreManifest,
    StoreProvider, PROVIDER_NAMESPACE_DIGEST_BYTES, STORE_ID_BYTES,
};
use nokv_fdb::{FdbStorePrefix, FDB_PHYSICAL_ENCODING_VERSION};

use crate::codec::*;
use crate::observer::OwnershipObserver;
use crate::{FdbControlKeys, FdbControlOptions, FdbSessionFence};

fn root(value: u8) -> RootId {
    RootId::from_bytes([value; 16])
}

fn shard(value: u8) -> LogicalShardId {
    LogicalShardId::from_bytes([value; 16])
}

fn manifest(digest: [u8; PROVIDER_NAMESPACE_DIGEST_BYTES]) -> StoreManifest {
    StoreManifest::new(
        StoreId::from_bytes([1; STORE_ID_BYTES]),
        StoreProvider::FoundationDb,
        11,
        FDB_PHYSICAL_ENCODING_VERSION,
        digest,
        "0.11.0",
    )
    .unwrap()
}

#[test]
fn options_bind_the_exact_prefix_and_bounded_monotonic_ttl() {
    let first = FdbControlOptions::new("/tmp/fdb.cluster", "nokv-prod").unwrap();
    let second = FdbControlOptions::new("/tmp/fdb.cluster", "nokv-stage").unwrap();
    assert_ne!(
        first.provider_namespace_digest(),
        second.provider_namespace_digest()
    );
    assert_eq!(first.lease_ttl(), Duration::from_secs(10));
    first
        .validate_manifest_binding(&manifest(first.provider_namespace_digest()))
        .unwrap();
    assert!(first
        .validate_manifest_binding(&manifest(second.provider_namespace_digest()))
        .is_err());
    assert!(first
        .validate_manifest_binding(
            &StoreManifest::new(
                StoreId::from_bytes([1; STORE_ID_BYTES]),
                StoreProvider::FoundationDb,
                10,
                FDB_PHYSICAL_ENCODING_VERSION,
                first.provider_namespace_digest(),
                "0.10.0",
            )
            .unwrap()
        )
        .is_err());
    assert!(first.clone().with_lease_ttl(Duration::ZERO).is_err());
    assert!(FdbControlOptions::new("relative.cluster", "nokv-prod").is_err());
    assert!(FdbControlOptions::new("/tmp/fdb.cluster", "").is_err());
}

#[test]
fn physical_control_keys_are_versioned_component_safe_and_disjoint() {
    let prefix = FdbStorePrefix::new([0, 0xff]).unwrap();
    let keys = FdbControlKeys::new(&prefix);
    let root_key = keys.root_catalog_key(&root(1));
    let shard_key = keys.shard_catalog_key(&shard(1));
    assert_ne!(root_key, shard_key);
    assert_ne!(keys.session_key(&shard(1)), keys.heartbeat_key(&shard(1)));
    assert!(!keys
        .root_catalog_key(&root(2))
        .starts_with(&keys.root_catalog_key(&root(1))));
    let (begin, end) = keys.root_catalog_range().unwrap();
    assert!(begin < root_key && root_key < end);
}

#[test]
fn frozen_control_records_round_trip_and_reject_corruption() {
    let options = FdbControlOptions::new("/tmp/fdb.cluster", "nokv-prod").unwrap();
    let manifest = manifest(options.provider_namespace_digest());
    assert_eq!(
        decode_manifest(&encode_manifest(&manifest).unwrap()).unwrap(),
        manifest
    );

    let root_entry = RootCatalogEntry::new(
        root(1),
        AgentId::from_bytes([7; 16]),
        ObjectNamespaceId::from_bytes([2; 16]),
        shard(3),
        PlacementGeneration::new(4).unwrap(),
        CatalogEntryState::Ready,
    );
    assert_eq!(
        decode_root_catalog(&encode_root_catalog(&root_entry)).unwrap(),
        root_entry
    );
    let shard_entry = ShardCatalogEntry::new(shard(3), CatalogEntryState::Ready);
    assert_eq!(
        decode_shard_catalog(&encode_shard_catalog(&shard_entry)).unwrap(),
        shard_entry
    );

    let initial = OwnershipSnapshot::new(ShardRoute::unassigned(shard(3)), None, None).unwrap();
    let acquired = plan_owner_acquisition(
        &initial,
        NodeId::new("node-a").unwrap(),
        RpcEndpoint::new("node-a.example:7750").unwrap(),
    )
    .unwrap();
    assert_eq!(
        decode_route(&encode_route(acquired.route()).unwrap()).unwrap(),
        *acquired.route()
    );
    assert_eq!(
        decode_session(&encode_session(acquired.session().unwrap()).unwrap()).unwrap(),
        *acquired.session().unwrap()
    );
    let keys = FdbControlKeys::new(&FdbStorePrefix::new(b"nokv-prod").unwrap());
    let fence = FdbSessionFence::new(&keys, acquired.session().unwrap().clone()).unwrap();
    assert_eq!(
        decode_session(fence.expected_value()).unwrap(),
        *acquired.session().unwrap()
    );
    assert_eq!(fence.key(), keys.session_key(&shard(3)).as_slice());
    assert_eq!(
        decode_heartbeat(&encode_heartbeat(acquired.heartbeat()).unwrap()).unwrap(),
        *acquired.heartbeat()
    );
    let mut unknown_route_state = encode_route(acquired.route()).unwrap();
    unknown_route_state[b"\x16nokv-control\0".len() + 2 + 16] = 0;
    assert!(matches!(
        decode_route(&unknown_route_state),
        Err(ControlError::InvalidRecord(_))
    ));
    let mut truncated_heartbeat = encode_heartbeat(acquired.heartbeat()).unwrap();
    truncated_heartbeat.pop();
    assert!(matches!(
        decode_heartbeat(&truncated_heartbeat),
        Err(ControlError::InvalidRecord(_))
    ));
    assert!(matches!(
        decode_heartbeat(&encode_session(acquired.session().unwrap()).unwrap()),
        Err(ControlError::InvalidRecord(_))
    ));

    let mut future = encode_manifest(&manifest).unwrap();
    future[b"\x16nokv-control\0".len()] = 2;
    assert!(matches!(
        decode_manifest(&future),
        Err(ControlError::UnsupportedRecordVersion { .. })
    ));
    let mut trailing = encode_manifest(&manifest).unwrap();
    trailing.push(0);
    assert!(matches!(
        decode_manifest(&trailing),
        Err(ControlError::InvalidRecord(_))
    ));
}

#[test]
fn observer_uses_only_unchanged_local_monotonic_duration() {
    let initial = OwnershipSnapshot::new(ShardRoute::unassigned(shard(5)), None, None).unwrap();
    let acquired = plan_owner_acquisition(
        &initial,
        NodeId::new("node-a").unwrap(),
        RpcEndpoint::new("node-a.example:7750").unwrap(),
    )
    .unwrap()
    .snapshot()
    .unwrap();
    let observer = OwnershipObserver::default();
    let ttl = Duration::from_secs(5);
    observer.record(&acquired, Duration::from_secs(10));
    assert_eq!(
        observer.remaining(&acquired, Duration::from_secs(14), ttl),
        Some(Duration::from_secs(1))
    );
    observer.record(&acquired, Duration::from_secs(14));
    assert_eq!(
        observer.remaining(&acquired, Duration::from_secs(15), ttl),
        None
    );

    let renewed = plan_heartbeat_renewal(&acquired, acquired.session().unwrap())
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(
        observer.remaining(&renewed, Duration::from_secs(15), ttl),
        Some(ttl)
    );
}
