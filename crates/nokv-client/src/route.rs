/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#[cfg(feature = "control")]
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

#[cfg(feature = "control")]
use nokv_control::{ControlStore, IdentityStore, LogicalShardState, RootPlacementLifecycle};
#[cfg(feature = "etcd")]
use nokv_control::{EtcdControlStore, EtcdControlStoreOptions};
#[cfg(feature = "control")]
use nokv_protocol::{LogicalShardIdentity, ObjectNamespaceIdentity};
use nokv_protocol::{RootIdentity, RootRoute};
#[cfg(feature = "control")]
use nokv_types::RootId;
#[cfg(feature = "control")]
use nokv_types::WorkbenchRoot;

use crate::ClientError;

/// Describes where a resolver obtains a route after a refresh request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteRefreshMode {
    /// Reloads route state from an authoritative control source.
    Authoritative,
    /// Observes only caller-supplied replacement snapshots.
    CallerManaged,
}

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
    /// Existing resolvers are authoritative by contract unless they explicitly
    /// identify themselves as caller-managed snapshots.
    fn refresh_mode(&self) -> RouteRefreshMode {
        RouteRefreshMode::Authoritative
    }

    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError>;

    /// The Workbench root the control plane binds `root_id` to, which every
    /// caller must present the root under. `None` when the control plane
    /// has bound none, or when this resolver reads no control plane at all.
    fn workbench_root(
        &self,
        root_id: RootIdentity,
    ) -> Result<Option<nokv_types::WorkbenchRoot>, ClientError> {
        let _ = root_id;
        Ok(None)
    }
}

impl<T> RouteResolver for Arc<T>
where
    T: RouteResolver + ?Sized,
{
    fn refresh_mode(&self) -> RouteRefreshMode {
        (**self).refresh_mode()
    }

    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError> {
        (**self).resolve(root_id, refresh)
    }

    fn workbench_root(
        &self,
        root_id: RootIdentity,
    ) -> Result<Option<nokv_types::WorkbenchRoot>, ClientError> {
        (**self).workbench_root(root_id)
    }
}

/// Caller-managed point-in-time route for single-owner deployments and tests.
///
/// Resolving with `refresh = true` observes a concurrent [`Self::replace`], but
/// never discovers placement or ownership by itself. Long-running clients need
/// a control-plane-backed resolver.
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
    fn refresh_mode(&self) -> RouteRefreshMode {
        RouteRefreshMode::CallerManaged
    }

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
    /// The same control plane's identity records, when the caller opened a
    /// store that holds them; a resolver over routing records alone cannot
    /// answer for the Workbench root.
    identity: Option<Arc<dyn IdentityStore>>,
    cached: Arc<RwLock<BTreeMap<RootIdentity, ResolvedRoute>>>,
}

#[cfg(feature = "control")]
impl ControlRouteResolver {
    pub fn new(store: Arc<dyn ControlStore>) -> Self {
        Self {
            store,
            identity: None,
            cached: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// A resolver that also answers for the Workbench root each root is
    /// bound to, from the identity records of the same control plane.
    pub fn with_identity(store: Arc<dyn ControlStore>, identity: Arc<dyn IdentityStore>) -> Self {
        Self {
            identity: Some(identity),
            ..Self::new(store)
        }
    }

    /// Use the single-host control directory shared with the owner as the
    /// sole route source.
    #[cfg(unix)]
    pub fn open_local(directory: impl Into<std::path::PathBuf>) -> Result<Self, ClientError> {
        let store =
            Arc::new(nokv_control::LocalControlStore::open(directory).map_err(control_error)?);
        Ok(Self::with_identity(store.clone(), store))
    }

    /// Connect an etcd-backed control store and use it as the sole route source.
    #[cfg(feature = "etcd")]
    pub fn connect_etcd(options: EtcdRouteOptions) -> Result<Self, ClientError> {
        options.validate()?;
        let store = Arc::new(
            EtcdControlStore::connect(options.into_control_options()).map_err(control_error)?,
        );
        Ok(Self::with_identity(store.clone(), store))
    }

    /// The route cached for `root_id`, if one was resolved and not refreshed
    /// away since.
    pub fn cached_route(&self, root_id: RootIdentity) -> Option<ResolvedRoute> {
        self.cached
            .read()
            .ok()
            .and_then(|cached| cached.get(&root_id).copied())
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
        let object_namespace = self
            .store
            .get_root_object_namespace_binding(&root_id)
            .map_err(control_error)?
            .ok_or_else(|| invalid_route("root object namespace binding does not exist"))?;
        if object_namespace.root_id != root_id {
            return Err(invalid_route(
                "object namespace binding identity differs from root placement",
            ));
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
                object_namespace_id: ObjectNamespaceIdentity::from(
                    object_namespace.object_namespace_id,
                ),
                placement_generation: placement.placement_generation.get(),
                owner_epoch: owner_epoch.get(),
            },
            endpoint,
        )
    }
}

#[cfg(feature = "control")]
impl RouteResolver for ControlRouteResolver {
    fn refresh_mode(&self) -> RouteRefreshMode {
        RouteRefreshMode::Authoritative
    }

    fn workbench_root(&self, root_id: RootIdentity) -> Result<Option<WorkbenchRoot>, ClientError> {
        let identity = self.identity.as_ref().ok_or_else(|| {
            invalid_route(
                "this resolver reads routing records only; open the control plane with its \
                 identity records to learn a root's Workbench root",
            )
        })?;
        Ok(identity
            .get_root_presentation(&RootId::from(root_id))
            .map_err(control_error)?
            .map(|binding| binding.workbench_root))
    }

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
        InMemoryControlStore, LogicalShardId, NodeId, PlacementGeneration, RecoveryPublication,
        RootObjectNamespaceBinding, RootPlacement,
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
        let (root_id, lease) = serve_root(store.as_ref(), endpoint);
        (store, root_id, lease)
    }

    fn serve_root(
        store: &dyn ControlStore,
        endpoint: &str,
    ) -> (RootIdentity, nokv_control::LogicalShardLease) {
        let root_id = RootId::from_bytes(id(1));
        let logical_shard_id = LogicalShardId::from_bytes(id(2));
        store.create_logical_shard(logical_shard_id).unwrap();
        store
            .create_root_object_namespace_binding(RootObjectNamespaceBinding {
                root_id,
                object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes(id(8)),
            })
            .unwrap();
        let provisioning = store
            .create_root_placement(RootPlacement {
                root_id,
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
            .compare_and_set_root_placement(&provisioning, active)
            .unwrap();
        let lease = store
            .acquire_owner(
                &logical_shard_id,
                NodeId::new("node-a").unwrap(),
                endpoint.to_owned(),
            )
            .unwrap();
        store
            .mark_serving(
                &lease,
                RecoveryPublication {
                    checkpoint: None,
                    log: None,
                    durable_lsn: 0,
                },
            )
            .unwrap();
        (RootIdentity(id(1)), lease)
    }

    #[cfg(unix)]
    #[test]
    fn a_local_control_directory_routes_to_its_serving_owner() {
        let directory = tempfile::tempdir().unwrap();
        let owner = nokv_control::LocalControlStore::open(directory.path()).unwrap();
        let (root_id, _lease) = serve_root(&owner, "127.0.0.1:7751");

        // A client opens the same directory and holds no session of its own.
        let resolver = ControlRouteResolver::open_local(directory.path()).unwrap();
        let resolved = resolver.resolve(root_id, true).unwrap();
        assert_eq!(resolved.route.placement_generation, 2);
        assert_eq!(resolved.route.owner_epoch, 1);
        assert_eq!(
            resolved.endpoint,
            "127.0.0.1:7751".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn control_resolvers_read_the_bound_workbench_root_and_static_ones_know_none() {
        let (store, root_id, _) = serving_store("127.0.0.1:7750");
        store
            .bind_root_presentation(nokv_control::RootPresentationBinding {
                root_id: RootId::from_bytes(id(1)),
                workbench_root: nokv_types::WorkbenchRoot::new("/agents/one/wb").unwrap(),
            })
            .unwrap();
        let identity: Arc<dyn nokv_control::IdentityStore> = store.clone();
        let control: Arc<dyn ControlStore> = store.clone();
        let resolver = ControlRouteResolver::with_identity(control.clone(), identity);
        assert_eq!(
            resolver
                .workbench_root(root_id)
                .unwrap()
                .as_ref()
                .map(nokv_types::WorkbenchRoot::as_str),
            Some("/agents/one/wb")
        );
        assert_eq!(resolver.workbench_root(RootIdentity(id(9))).unwrap(), None);

        // A resolver over routing records alone cannot answer, and says so
        // rather than guessing; a static route has no control plane at all.
        let routing_only = ControlRouteResolver::new(control);
        assert!(matches!(
            routing_only.workbench_root(root_id),
            Err(ClientError::InvalidRoute(_))
        ));
        let pinned = StaticRouteResolver::new(
            RootRoute {
                root_id,
                logical_shard_id: LogicalShardIdentity(id(2)),
                object_namespace_id: nokv_protocol::ObjectNamespaceIdentity(id(8)),
                placement_generation: 2,
                owner_epoch: 1,
            },
            "127.0.0.1:7750".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(pinned.workbench_root(root_id).unwrap(), None);
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

        store.release_owner(&first).unwrap();
        let second = store
            .acquire_successor(
                &first.logical_shard_id,
                first.owner_epoch,
                NodeId::new("node-b").unwrap(),
                "127.0.0.1:7751".to_owned(),
            )
            .unwrap();
        store
            .mark_serving(
                &second,
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
