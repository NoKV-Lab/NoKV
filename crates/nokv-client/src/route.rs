/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#[cfg(feature = "control")]
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

#[cfg(feature = "control")]
use nokv_control::{ControlStore, LogicalShardState, RootPlacementLifecycle};
#[cfg(feature = "etcd")]
use nokv_control::{EtcdControlStore, EtcdControlStoreOptions};
#[cfg(feature = "control")]
use nokv_protocol::LogicalShardIdentity;
use nokv_protocol::{RootIdentity, RootRoute};
#[cfg(feature = "control")]
use nokv_types::RootId;

use crate::ClientError;

/// One persisted routing fence plus the current physical owner endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedRoute {
    pub route: RootRoute,
    pub endpoint: SocketAddr,
}

impl ResolvedRoute {
    pub fn new(route: RootRoute, endpoint: SocketAddr) -> Result<Self, ClientError> {
        route
            .validate()
            .map_err(|error| ClientError::InvalidRoute(error.to_string()))?;
        Ok(Self { route, endpoint })
    }
}

/// Resolves a root to its persisted logical-shard placement.
pub trait RouteResolver: Send + Sync {
    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError>;
}

impl<T> RouteResolver for Arc<T>
where
    T: RouteResolver + ?Sized,
{
    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError> {
        (**self).resolve(root_id, refresh)
    }
}

/// Resolver for single-route deployments and tests.
#[derive(Clone, Debug)]
pub struct StaticRouteResolver {
    resolved: Arc<RwLock<ResolvedRoute>>,
}

impl StaticRouteResolver {
    pub fn new(route: RootRoute, endpoint: SocketAddr) -> Result<Self, ClientError> {
        let resolved = ResolvedRoute::new(route, endpoint)?;
        Ok(Self {
            resolved: Arc::new(RwLock::new(resolved)),
        })
    }

    pub fn replace(&self, route: RootRoute, endpoint: SocketAddr) -> Result<(), ClientError> {
        let resolved = ResolvedRoute::new(route, endpoint)?;
        let mut current = self
            .resolved
            .write()
            .map_err(|_| ClientError::InvalidRoute("route lock is poisoned".to_owned()))?;
        if resolved.route.root_id != current.route.root_id {
            return Err(ClientError::InvalidRoute(
                "replacement route belongs to another root".to_owned(),
            ));
        }
        *current = resolved;
        Ok(())
    }
}

impl RouteResolver for StaticRouteResolver {
    fn resolve(&self, root_id: RootIdentity, _refresh: bool) -> Result<ResolvedRoute, ClientError> {
        let resolved = *self
            .resolved
            .read()
            .map_err(|_| ClientError::InvalidRoute("route lock is poisoned".to_owned()))?;
        if resolved.route.root_id != root_id {
            return Err(ClientError::InvalidRoute(
                "resolver route belongs to another root".to_owned(),
            ));
        }
        Ok(resolved)
    }
}

/// Resolver backed by the authoritative NoKV control plane.
///
/// Cached routes are used for ordinary calls. A caller-requested refresh always
/// reloads both the root placement and its logical shard's current owner; stale
/// cache state is never returned after a failed refresh.
#[cfg(feature = "control")]
#[derive(Clone)]
pub struct ControlRouteResolver {
    store: Arc<dyn ControlStore>,
    cached: Arc<RwLock<BTreeMap<RootIdentity, ResolvedRoute>>>,
}

#[cfg(feature = "control")]
impl ControlRouteResolver {
    pub fn new(store: Arc<dyn ControlStore>) -> Self {
        Self {
            store,
            cached: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Connect an etcd-backed control store and use it as the sole route source.
    #[cfg(feature = "etcd")]
    pub fn connect_etcd(options: EtcdRouteOptions) -> Result<Self, ClientError> {
        options.validate()?;
        let store =
            EtcdControlStore::connect(options.into_control_options()).map_err(control_error)?;
        Ok(Self::new(Arc::new(store)))
    }

    fn load(&self, root_identity: RootIdentity) -> Result<ResolvedRoute, ClientError> {
        let root_id = RootId::from(root_identity);
        let placement = self
            .store
            .get_root_placement(&root_id)
            .map_err(control_error)?
            .ok_or_else(|| invalid_route("root placement does not exist"))?;
        if placement.lifecycle != RootPlacementLifecycle::Active {
            return Err(invalid_route(format!(
                "root placement is {:?}, not Active",
                placement.lifecycle
            )));
        }

        let shard = self
            .store
            .get_logical_shard(&placement.logical_shard_id)
            .map_err(control_error)?
            .ok_or_else(|| invalid_route("logical shard record does not exist"))?;
        if shard.logical_shard_id != placement.logical_shard_id {
            return Err(invalid_route(
                "logical shard record identity differs from root placement",
            ));
        }
        if shard.state != LogicalShardState::Serving {
            return Err(invalid_route(format!(
                "logical shard is {:?}, not Serving",
                shard.state
            )));
        }
        let owner_epoch = shard
            .owner_epoch
            .ok_or_else(|| invalid_route("serving logical shard has no owner epoch"))?;
        let endpoint = shard
            .endpoint
            .as_deref()
            .ok_or_else(|| invalid_route("serving logical shard has no endpoint"))?
            .parse::<SocketAddr>()
            .map_err(|error| {
                invalid_route(format!("owner endpoint is not a socket address: {error}"))
            })?;

        ResolvedRoute::new(
            RootRoute {
                root_id: root_identity,
                logical_shard_id: LogicalShardIdentity::from(placement.logical_shard_id),
                placement_generation: placement.placement_generation.get(),
                owner_epoch: owner_epoch.get(),
            },
            endpoint,
        )
    }
}

#[cfg(feature = "control")]
impl RouteResolver for ControlRouteResolver {
    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError> {
        if refresh {
            self.cached
                .write()
                .map_err(|_| invalid_route("control route cache lock is poisoned"))?
                .remove(&root_id);
        } else {
            let cached = self
                .cached
                .read()
                .map_err(|_| invalid_route("control route cache lock is poisoned"))?;
            if let Some(route) = cached.get(&root_id) {
                return Ok(*route);
            }
        }

        let resolved = self.load(root_id)?;
        self.cached
            .write()
            .map_err(|_| invalid_route("control route cache lock is poisoned"))?
            .insert(root_id, resolved);
        Ok(resolved)
    }
}

/// Etcd connection settings for the shared control-backed route resolver.
#[cfg(feature = "etcd")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EtcdRouteOptions {
    control: EtcdControlStoreOptions,
}

#[cfg(feature = "etcd")]
impl EtcdRouteOptions {
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            control: EtcdControlStoreOptions::new(endpoints),
        }
    }

    pub fn with_key_prefix(mut self, key_prefix: impl Into<String>) -> Self {
        self.control = self.control.with_key_prefix(key_prefix);
        self
    }

    pub fn with_lease_ttl_seconds(mut self, lease_ttl_seconds: i64) -> Self {
        self.control = self.control.with_lease_ttl_seconds(lease_ttl_seconds);
        self
    }

    pub fn endpoints(&self) -> &[String] {
        self.control.endpoints()
    }

    pub fn key_prefix(&self) -> &str {
        self.control.key_prefix()
    }

    pub fn lease_ttl_seconds(&self) -> i64 {
        self.control.lease_ttl_seconds()
    }

    pub fn validate(&self) -> Result<(), ClientError> {
        self.control.validate().map_err(|error| {
            ClientError::InvalidOptions(format!("invalid etcd routing configuration: {error}"))
        })
    }

    fn into_control_options(self) -> EtcdControlStoreOptions {
        self.control
    }
}

#[cfg(feature = "control")]
fn control_error(error: impl std::fmt::Display) -> ClientError {
    invalid_route(format!("control-plane lookup failed: {error}"))
}

#[cfg(feature = "control")]
fn invalid_route(message: impl Into<String>) -> ClientError {
    ClientError::InvalidRoute(message.into())
}

#[cfg(all(test, feature = "control"))]
mod control_tests {
    use super::*;
    use nokv_control::{
        ConsistencyDomainId, InMemoryControlStore, LogicalShardId, MetadataAuthorityBinding,
        MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRecord,
        MetadataAuthorityRevision, MetadataContractDigest, MetadataProviderProfileId, NodeId,
        OwnerIncarnationId, OwnerServingAdmission, PlacementGeneration, RecoveryPublication,
        RootLayoutGeneration, RootLayoutProfile, RootPartitionId, RootPlacement,
    };

    fn id(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn serving_store(
        endpoint: &str,
    ) -> (
        Arc<InMemoryControlStore>,
        RootIdentity,
        nokv_control::LogicalShardLease,
    ) {
        let store = Arc::new(InMemoryControlStore::new());
        let root_id = RootId::from_bytes(id(1));
        let logical_shard_id = LogicalShardId::from_bytes(id(2));
        store.create_logical_shard(logical_shard_id).unwrap();
        store
            .create_metadata_authority(MetadataAuthorityRecord {
                logical_shard_id,
                record_revision: MetadataAuthorityRevision::new(1).unwrap(),
                authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
                active: MetadataAuthorityBinding {
                    authority_id: MetadataAuthorityId::from_bytes(id(3)),
                    provider_profile_id: MetadataProviderProfileId::new("route-test-v1").unwrap(),
                    profile_fingerprint: [4; 32],
                    consistency_domain_id: ConsistencyDomainId::from_bytes(id(5)),
                    contract_digest: MetadataContractDigest::from_bytes([6; 32]),
                },
                migration: None,
            })
            .unwrap();
        let provisioning = store
            .create_root_placement(RootPlacement {
                root_id,
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                logical_shard_id,
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        let active = RootPlacement {
            placement_generation: PlacementGeneration::new(2).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
            ..provisioning.clone()
        };
        store
            .compare_and_set_root_placement(&provisioning, active.clone())
            .unwrap();
        let admission = OwnerServingAdmission::stable(
            active,
            store
                .get_metadata_authority(&logical_shard_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let lease = store
            .acquire_owner(
                &admission,
                NodeId::new("node-a").unwrap(),
                OwnerIncarnationId::from_bytes(id(7)),
                endpoint.to_owned(),
            )
            .unwrap();
        store
            .mark_serving(
                &lease,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();
        (store, RootIdentity(id(1)), lease)
    }

    #[test]
    fn resolves_only_active_serving_owner() {
        let (store, root_id, _) = serving_store("127.0.0.1:7750");
        let resolver = ControlRouteResolver::new(store);
        let resolved = resolver.resolve(root_id, false).unwrap();
        assert_eq!(resolved.route.root_id, root_id);
        assert_eq!(resolved.route.logical_shard_id, LogicalShardIdentity(id(2)));
        assert_eq!(resolved.route.placement_generation, 2);
        assert_eq!(resolved.route.owner_epoch, 1);
        assert_eq!(
            resolved.endpoint,
            "127.0.0.1:7750".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn explicit_refresh_reloads_placement_and_owner() {
        let (store, root_id, first) = serving_store("127.0.0.1:7750");
        let resolver = ControlRouteResolver::new(store.clone());
        let old = resolver.resolve(root_id, false).unwrap();

        let active = store
            .get_root_placement(&RootId::from(root_id))
            .unwrap()
            .unwrap();
        let draining = RootPlacement {
            placement_generation: PlacementGeneration::new(3).unwrap(),
            lifecycle: RootPlacementLifecycle::Draining,
            ..active.clone()
        };
        store
            .compare_and_set_root_placement(&active, draining.clone())
            .unwrap();
        store
            .compare_and_set_root_placement(
                &draining,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(4).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..draining.clone()
                },
            )
            .unwrap();
        let admission = OwnerServingAdmission::stable(
            store
                .get_root_placement(&RootId::from(root_id))
                .unwrap()
                .unwrap(),
            store
                .get_metadata_authority(&first.logical_shard_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        store.release_owner(&first).unwrap();
        let second = store
            .acquire_successor(
                &admission,
                first.owner_epoch,
                NodeId::new("node-b").unwrap(),
                OwnerIncarnationId::from_bytes(id(8)),
                "127.0.0.1:7751".to_owned(),
            )
            .unwrap();
        store
            .mark_serving(
                &second,
                &admission,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();

        assert_eq!(resolver.resolve(root_id, false).unwrap(), old);
        let refreshed = resolver.resolve(root_id, true).unwrap();
        assert_eq!(refreshed.route.placement_generation, 4);
        assert_eq!(refreshed.route.owner_epoch, 2);
        assert_eq!(
            refreshed.endpoint,
            "127.0.0.1:7751".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn refresh_rejects_non_serving_shard_without_using_stale_state() {
        let (store, root_id, lease) = serving_store("127.0.0.1:7750");
        let resolver = ControlRouteResolver::new(store.clone());
        resolver.resolve(root_id, false).unwrap();
        store.release_owner(&lease).unwrap();

        let error = resolver.resolve(root_id, true).unwrap_err();
        assert!(matches!(error, ClientError::InvalidRoute(_)));
        assert!(error.to_string().contains("not Serving"));

        let error = resolver.resolve(root_id, false).unwrap_err();
        assert!(error.to_string().contains("not Serving"));
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn etcd_options_preserve_and_validate_client_configuration() {
        let options = EtcdRouteOptions::new(["http://127.0.0.1:2379", "http://127.0.0.1:22379"])
            .with_key_prefix("/nokv/test-control")
            .with_lease_ttl_seconds(27);
        options.validate().unwrap();
        assert_eq!(
            options.endpoints(),
            ["http://127.0.0.1:2379", "http://127.0.0.1:22379"]
        );
        assert_eq!(options.key_prefix(), "/nokv/test-control");
        assert_eq!(options.lease_ttl_seconds(), 27);
    }
}
