/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

#[cfg(feature = "control")]
use nokv_control::{ControlStore, LogicalShardState, RootPlacementLifecycle};
#[cfg(feature = "etcd")]
use nokv_control::{EtcdControlStore, EtcdControlStoreOptions};
use nokv_protocol::{
    decode_response, encode_request, DiscoverRouteOutcome, DiscoverRouteRequest, DiscoveredRoute,
    RootIdentity, RootRoute, RpcRequest, RpcResponse,
};
#[cfg(feature = "control")]
use nokv_protocol::{LogicalShardIdentity, ObjectNamespaceIdentity};
#[cfg(feature = "control")]
use nokv_types::RootId;

use crate::{ClientError, RpcTransport};

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
    pub session_generation: u64,
}

impl ResolvedRoute {
    pub fn new(route: RootRoute, endpoint: SocketAddr) -> Result<Self, ClientError> {
        route
            .validate()
            .map_err(|error| ClientError::InvalidRoute(error.to_string()))?;
        if endpoint.port() == 0 {
            return Err(ClientError::InvalidRoute(
                "owner endpoint port must be nonzero".to_owned(),
            ));
        }
        Ok(Self {
            route,
            endpoint,
            session_generation: 1,
        })
    }

    pub fn from_discovered(discovered: &DiscoveredRoute) -> Result<Self, ClientError> {
        discovered
            .validate()
            .map_err(|error| ClientError::InvalidRoute(error.to_string()))?;
        Ok(Self {
            route: discovered.route(),
            endpoint: discovered.owner_endpoint.socket_addr(),
            session_generation: discovered.session_generation,
        })
    }
}

/// Resolves a root to its persisted logical-shard placement.
pub trait RouteResolver: Send + Sync {
    /// Existing resolvers are authoritative by contract unless they explicitly
    /// identify themselves as caller-managed snapshots.
    fn refresh_mode(&self) -> RouteRefreshMode {
        RouteRefreshMode::Authoritative
    }

    /// Whether this resolver compares the discovery session generation in
    /// addition to the long-lived root route fence.
    fn tracks_session_generation(&self) -> bool {
        false
    }

    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError>;

    /// Observe a complete owner hint without making it authoritative. Seed
    /// resolvers install only monotonic hints; other resolvers ignore them.
    fn observe_hint(
        &self,
        _root_id: RootIdentity,
        _hint: &DiscoveredRoute,
    ) -> Result<Option<ResolvedRoute>, ClientError> {
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

    fn tracks_session_generation(&self) -> bool {
        (**self).tracks_session_generation()
    }

    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError> {
        (**self).resolve(root_id, refresh)
    }

    fn observe_hint(
        &self,
        root_id: RootIdentity,
        hint: &DiscoveredRoute,
    ) -> Result<Option<ResolvedRoute>, ClientError> {
        (**self).observe_hint(root_id, hint)
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

/// Bounded discovery and backoff settings for [`SeedRouteResolver`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeedRouteOptions {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub maximum_backoff: Duration,
}

impl Default for SeedRouteOptions {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_backoff: Duration::from_millis(5),
            maximum_backoff: Duration::from_millis(100),
        }
    }
}

impl SeedRouteOptions {
    fn validate(self) -> Result<(), ClientError> {
        if !(1..=64).contains(&self.max_attempts) {
            return Err(ClientError::InvalidOptions(
                "seed max_attempts must be between 1 and 64".to_owned(),
            ));
        }
        if self.maximum_backoff < self.initial_backoff {
            return Err(ClientError::InvalidOptions(
                "seed maximum_backoff must not be less than initial_backoff".to_owned(),
            ));
        }
        if self.maximum_backoff > Duration::from_secs(5) {
            return Err(ClientError::InvalidOptions(
                "seed maximum_backoff must not exceed five seconds".to_owned(),
            ));
        }
        Ok(())
    }
}

struct SeedRouteState {
    next_seed: AtomicUsize,
    cached: RwLock<BTreeMap<RootIdentity, ResolvedRoute>>,
}

/// Resolves routes exclusively through NoKV seed RPCs.
///
/// Clients never read FoundationDB or etcd. Seeds are retained in caller order
/// after exact deduplication, and each discovery starts at the next seed.
#[derive(Clone)]
pub struct SeedRouteResolver<Transport> {
    transport: Arc<Transport>,
    seeds: Arc<[SocketAddr]>,
    options: SeedRouteOptions,
    state: Arc<SeedRouteState>,
}

impl<Transport> SeedRouteResolver<Transport>
where
    Transport: RpcTransport,
{
    pub fn new(
        transport: Transport,
        seeds: impl IntoIterator<Item = SocketAddr>,
        options: SeedRouteOptions,
    ) -> Result<Self, ClientError> {
        options.validate()?;
        let mut unique = std::collections::BTreeSet::new();
        let seeds = seeds
            .into_iter()
            .filter(|seed| unique.insert(*seed))
            .collect::<Vec<_>>();
        if seeds.is_empty() {
            return Err(ClientError::InvalidOptions(
                "at least one NoKV seed endpoint is required".to_owned(),
            ));
        }
        if seeds
            .iter()
            .any(|seed| seed.port() == 0 || seed.ip().is_unspecified())
        {
            return Err(ClientError::InvalidOptions(
                "seed endpoints must be connectable addresses with nonzero ports".to_owned(),
            ));
        }
        Ok(Self {
            transport: Arc::new(transport),
            seeds: seeds.into(),
            options,
            state: Arc::new(SeedRouteState {
                next_seed: AtomicUsize::new(0),
                cached: RwLock::new(BTreeMap::new()),
            }),
        })
    }

    pub fn seeds(&self) -> &[SocketAddr] {
        &self.seeds
    }

    fn cached(&self, root_id: RootIdentity) -> Result<Option<ResolvedRoute>, ClientError> {
        Ok(self
            .state
            .cached
            .read()
            .map_err(|_| ClientError::InvalidRoute("seed route cache is poisoned".to_owned()))?
            .get(&root_id)
            .copied())
    }

    fn discover(&self, root_id: RootIdentity) -> Result<ResolvedRoute, ClientError> {
        let request = RpcRequest::DiscoverRoute(DiscoverRouteRequest { root_id });
        let encoded = encode_request(&request)?;
        let start = self.state.next_seed.fetch_add(1, Ordering::Relaxed) % self.seeds.len();
        let mut backoff = self.options.initial_backoff;
        let mut last_error = None;

        for attempt in 0..self.options.max_attempts {
            let seed = self.seeds[(start + attempt as usize) % self.seeds.len()];
            let result = self
                .transport
                .round_trip(seed, &encoded)
                .map_err(ClientError::Transport)
                .and_then(|encoded| decode_discovery_response(&encoded, root_id));
            match result {
                Ok(discovered) => match self.install(root_id, &discovered) {
                    Ok(resolved) => return Ok(resolved),
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error),
            }

            if attempt + 1 < self.options.max_attempts {
                if !backoff.is_zero() {
                    thread::sleep(backoff);
                }
                backoff = backoff
                    .checked_mul(2)
                    .unwrap_or(self.options.maximum_backoff)
                    .min(self.options.maximum_backoff);
            }
        }

        Err(ClientError::RetryExhausted {
            attempts: self.options.max_attempts,
            last_error: Box::new(last_error.unwrap_or_else(|| {
                ClientError::InvalidRoute("seed discovery made no attempts".to_owned())
            })),
        })
    }

    fn install(
        &self,
        root_id: RootIdentity,
        discovered: &DiscoveredRoute,
    ) -> Result<ResolvedRoute, ClientError> {
        let candidate = ResolvedRoute::from_discovered(discovered)?;
        if candidate.route.root_id != root_id {
            return Err(ClientError::InvalidRoute(
                "seed returned a route for another root".to_owned(),
            ));
        }
        let mut cached =
            self.state.cached.write().map_err(|_| {
                ClientError::InvalidRoute("seed route cache is poisoned".to_owned())
            })?;
        if let Some(current) = cached.get(&root_id).copied() {
            validate_route_identity(candidate, current)?;
            match compare_route_generation(candidate, current) {
                std::cmp::Ordering::Less => {
                    return Err(ClientError::InvalidRoute(
                        "seed returned a route older than the cached route".to_owned(),
                    ));
                }
                std::cmp::Ordering::Equal if candidate.endpoint != current.endpoint => {
                    return Err(ClientError::InvalidRoute(
                        "seed changed the endpoint without advancing the route generation"
                            .to_owned(),
                    ));
                }
                _ => {}
            }
        }
        cached.insert(root_id, candidate);
        Ok(candidate)
    }
}

impl<Transport> RouteResolver for SeedRouteResolver<Transport>
where
    Transport: RpcTransport,
{
    fn tracks_session_generation(&self) -> bool {
        true
    }

    fn resolve(&self, root_id: RootIdentity, refresh: bool) -> Result<ResolvedRoute, ClientError> {
        if !refresh {
            if let Some(cached) = self.cached(root_id)? {
                return Ok(cached);
            }
        }
        self.discover(root_id)
    }

    fn observe_hint(
        &self,
        root_id: RootIdentity,
        hint: &DiscoveredRoute,
    ) -> Result<Option<ResolvedRoute>, ClientError> {
        let candidate = ResolvedRoute::from_discovered(hint)?;
        if candidate.route.root_id != root_id {
            return Err(ClientError::InvalidRoute(
                "owner hint belongs to another root".to_owned(),
            ));
        }
        if let Some(current) = self.cached(root_id)? {
            validate_route_identity(candidate, current)?;
            if compare_route_generation(candidate, current) == std::cmp::Ordering::Less {
                return Ok(None);
            }
        }
        self.install(root_id, hint).map(Some)
    }
}

fn decode_discovery_response(
    encoded: &[u8],
    root_id: RootIdentity,
) -> Result<DiscoveredRoute, ClientError> {
    match decode_response(encoded)? {
        RpcResponse::DiscoverRoute(response) if response.root_id == root_id => {
            match response.outcome {
                DiscoverRouteOutcome::Found(route) => Ok(route),
                DiscoverRouteOutcome::Failure(failure) => Err(ClientError::Discovery(failure)),
            }
        }
        RpcResponse::DiscoverRoute(_) => Err(ClientError::ResponseMismatch(
            "seed response belongs to another root".to_owned(),
        )),
        RpcResponse::Workspace(_) => Err(ClientError::ResponseMismatch(
            "seed returned a workspace response to discovery".to_owned(),
        )),
    }
}

fn validate_route_identity(
    candidate: ResolvedRoute,
    current: ResolvedRoute,
) -> Result<(), ClientError> {
    if candidate.route.root_id != current.route.root_id
        || candidate.route.logical_shard_id != current.route.logical_shard_id
        || candidate.route.object_namespace_id != current.route.object_namespace_id
    {
        return Err(ClientError::InvalidRoute(
            "discovery changed an immutable route identity".to_owned(),
        ));
    }
    Ok(())
}

fn compare_route_generation(
    candidate: ResolvedRoute,
    current: ResolvedRoute,
) -> std::cmp::Ordering {
    if candidate.route.placement_generation < current.route.placement_generation
        || candidate.route.owner_epoch < current.route.owner_epoch
        || candidate.session_generation < current.session_generation
    {
        std::cmp::Ordering::Less
    } else if candidate.route.placement_generation == current.route.placement_generation
        && candidate.route.owner_epoch == current.route.owner_epoch
        && candidate.session_generation == current.session_generation
    {
        std::cmp::Ordering::Equal
    } else {
        std::cmp::Ordering::Greater
    }
}

#[cfg(test)]
mod seed_tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use nokv_protocol::{
        DiscoverRouteResponse, ErrorCode, LogicalShardIdentity, ObjectNamespaceIdentity,
        OwnerEndpoint, RouteState, RpcFailure,
    };

    use super::*;
    use crate::TransportError;

    struct ScriptedTransport {
        responses: Mutex<VecDeque<Result<RpcResponse, TransportError>>>,
        endpoints: Mutex<Vec<SocketAddr>>,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<Result<RpcResponse, TransportError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                endpoints: Mutex::new(Vec::new()),
            }
        }
    }

    impl RpcTransport for ScriptedTransport {
        fn round_trip(
            &self,
            endpoint: SocketAddr,
            request: &[u8],
        ) -> Result<Vec<u8>, TransportError> {
            let request = nokv_protocol::decode_request(request)
                .map_err(|error| TransportError::new(error.to_string(), false))?;
            assert!(matches!(request, RpcRequest::DiscoverRoute(_)));
            self.endpoints.lock().unwrap().push(endpoint);
            let response = self.responses.lock().unwrap().pop_front().unwrap()?;
            nokv_protocol::encode_response(&response)
                .map_err(|error| TransportError::new(error.to_string(), false))
        }
    }

    fn root(value: u8) -> RootIdentity {
        RootIdentity([value; 16])
    }

    fn discovered(
        root_id: RootIdentity,
        placement_generation: u64,
        owner_epoch: u64,
        session_generation: u64,
        port: u16,
    ) -> DiscoveredRoute {
        DiscoveredRoute::new(
            RootRoute {
                root_id,
                logical_shard_id: LogicalShardIdentity([2; 16]),
                object_namespace_id: ObjectNamespaceIdentity([3; 16]),
                placement_generation,
                owner_epoch,
            },
            session_generation,
            OwnerEndpoint::new(format!("127.0.0.1:{port}")).unwrap(),
            RouteState::Serving,
        )
        .unwrap()
    }

    fn found(route: DiscoveredRoute) -> Result<RpcResponse, TransportError> {
        Ok(RpcResponse::DiscoverRoute(DiscoverRouteResponse {
            root_id: route.root_id,
            outcome: DiscoverRouteOutcome::Found(route),
        }))
    }

    fn unavailable(root_id: RootIdentity) -> Result<RpcResponse, TransportError> {
        Ok(RpcResponse::DiscoverRoute(DiscoverRouteResponse {
            root_id,
            outcome: DiscoverRouteOutcome::Failure(RpcFailure {
                code: ErrorCode::RouteUnavailable,
                message: "route is activating".to_owned(),
                retryable: true,
                conflict: None,
                current_generation: None,
                route_hint: None,
            }),
        }))
    }

    fn options(max_attempts: u32) -> SeedRouteOptions {
        SeedRouteOptions {
            max_attempts,
            initial_backoff: Duration::ZERO,
            maximum_backoff: Duration::ZERO,
        }
    }

    #[test]
    fn validates_deduplicates_and_rotates_seeds() {
        let first: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let second: SocketAddr = "127.0.0.1:7002".parse().unwrap();
        let root_id = root(1);
        let transport = ScriptedTransport::new(vec![
            unavailable(root_id),
            found(discovered(root_id, 4, 5, 6, 7100)),
            found(discovered(root_id, 4, 6, 7, 7101)),
        ]);
        let resolver =
            SeedRouteResolver::new(transport, [first, first, second], options(2)).unwrap();
        assert_eq!(resolver.seeds(), [first, second]);

        let initial = resolver.resolve(root_id, false).unwrap();
        assert_eq!(initial.session_generation, 6);
        assert_eq!(resolver.resolve(root_id, false).unwrap(), initial);
        let refreshed = resolver.resolve(root_id, true).unwrap();
        assert_eq!(refreshed.session_generation, 7);

        assert_eq!(
            *resolver.transport.endpoints.lock().unwrap(),
            vec![first, second, second]
        );
    }

    #[test]
    fn refresh_skips_stale_seed_and_preserves_monotonic_cache() {
        let first: SocketAddr = "127.0.0.1:7201".parse().unwrap();
        let second: SocketAddr = "127.0.0.1:7202".parse().unwrap();
        let root_id = root(9);
        let transport = ScriptedTransport::new(vec![
            found(discovered(root_id, 5, 7, 9, 7300)),
            found(discovered(root_id, 5, 6, 8, 7299)),
            found(discovered(root_id, 5, 8, 10, 7301)),
        ]);
        let resolver = SeedRouteResolver::new(transport, [first, second], options(2)).unwrap();

        resolver.resolve(root_id, false).unwrap();
        let refreshed = resolver.resolve(root_id, true).unwrap();
        assert_eq!(refreshed.route.owner_epoch, 8);
        assert_eq!(refreshed.session_generation, 10);

        let stale_hint = discovered(root_id, 5, 7, 9, 7300);
        assert_eq!(resolver.observe_hint(root_id, &stale_hint).unwrap(), None);
        assert_eq!(resolver.resolve(root_id, false).unwrap(), refreshed);
    }

    #[test]
    fn rejects_endpoint_change_without_generation_advance() {
        let seed: SocketAddr = "127.0.0.1:7401".parse().unwrap();
        let root_id = root(4);
        let transport = ScriptedTransport::new(vec![
            found(discovered(root_id, 2, 3, 4, 7500)),
            found(discovered(root_id, 2, 3, 4, 7501)),
        ]);
        let resolver = SeedRouteResolver::new(transport, [seed], options(1)).unwrap();
        resolver.resolve(root_id, false).unwrap();
        let error = resolver.resolve(root_id, true).unwrap_err();
        assert!(error
            .to_string()
            .contains("endpoint without advancing the route generation"));
    }

    #[test]
    fn broken_seed_does_not_block_a_later_healthy_seed() {
        let first: SocketAddr = "127.0.0.1:7601".parse().unwrap();
        let second: SocketAddr = "127.0.0.1:7602".parse().unwrap();
        let root_id = root(6);
        let transport = ScriptedTransport::new(vec![
            Err(TransportError::new("incompatible seed", false)),
            found(discovered(root_id, 3, 4, 5, 7700)),
        ]);
        let resolver = SeedRouteResolver::new(transport, [first, second], options(2)).unwrap();
        let resolved = resolver.resolve(root_id, false).unwrap();
        assert_eq!(resolved.endpoint.port(), 7700);
        assert_eq!(
            *resolver.transport.endpoints.lock().unwrap(),
            vec![first, second]
        );
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
