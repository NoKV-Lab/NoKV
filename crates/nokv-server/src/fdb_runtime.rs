/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Explicit FoundationDB format, provision, and serving composition.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nokv_control::{
    plan_owner_acquisition, validate_root_catalog_transition, CatalogEntryState, ControlError,
    CreateOutcome, DistributedControlStore, NodeId, OwnerSession, RootCatalogEntry, RpcEndpoint,
    ShardCatalogEntry, ShardRouteState, StoreId, StoreManifest, StoreProvider,
    SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_control_fdb::{FdbControlOptions, FdbControlStore, FdbSessionFence};
use nokv_fdb::{FdbRuntime, FDB_PHYSICAL_ENCODING_VERSION};
use nokv_meta::workspace as meta;
use nokv_meta_fdb::{FdbMetadataSessionFence, FdbOptions, FdbStore};
use nokv_meta_store::TxnStore;
use nokv_protocol::{
    DiscoveredRoute, ErrorCode, LogicalShardIdentity, ObjectNamespaceIdentity, OwnerEndpoint,
    RootIdentity, RootRoute, RouteState, RpcFailure,
};
use nokv_types::{
    AgentId, CommandDigest, LogicalShardId, ObjectNamespaceId, PlacementGeneration, RequestId,
    RootActivationState, RootId, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use crate::server::OwnershipMaintenance;
use crate::{
    FoundationDbMetadataUrl, MetadataWorkspaceRequestExecutor, RootOwnerRegistry,
    RouteDiscoverySource, ServerError, ServerOptions, WorkspaceRequestExecutor, WorkspaceServer,
};

const STORE_ID_DOMAIN: &[u8] = b"nokv/fdb/store-id/v1\0";
const SHARD_ID_DOMAIN: &[u8] = b"nokv/fdb/logical-shard-id/v1\0";
const NAMESPACE_ID_DOMAIN: &[u8] = b"nokv/fdb/object-namespace-id/v1\0";
const REQUEST_ID_DOMAIN: &[u8] = b"nokv/fdb/provision-request-id/v1\0";
const PROVISION_OWNER_DOMAIN: &[u8] = b"nokv/fdb/provision-owner/v1\0";
const PROVISION_ENDPOINT: &str = "127.0.0.1:1";
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(10);
const OWNERSHIP_OBSERVATION_POLL: Duration = Duration::from_millis(100);
const OWNERSHIP_CONFLICT_BACKOFF: Duration = Duration::from_millis(5);

static STORE_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static NEVER_SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdbFormatState {
    Created,
    Existing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbFormatOutcome {
    pub state: FdbFormatState,
    pub manifest: StoreManifest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbProvisionOutcome {
    pub root: RootCatalogEntry,
    pub shard: ShardCatalogEntry,
    pub preexisting: bool,
}

#[derive(Clone)]
pub struct FdbServedRoot {
    route: RootRoute,
    meta: Arc<meta::MetaShard>,
}

impl FdbServedRoot {
    pub const fn route(&self) -> RootRoute {
        self.route
    }

    pub fn meta(&self) -> &Arc<meta::MetaShard> {
        &self.meta
    }
}

/// Live distributed composition. The process-global network runtime and every
/// exact owner session remain retained until server release and drop.
pub struct FdbServingRuntime {
    _runtime: FdbRuntime,
    manifest: StoreManifest,
    roots: Vec<FdbServedRoot>,
    registry: Arc<RootOwnerRegistry>,
    ownership: Arc<FdbOwnership>,
    discovery: Arc<FdbRouteDiscovery>,
    lease_renew_interval: Duration,
}

impl FdbServingRuntime {
    pub fn manifest(&self) -> &StoreManifest {
        &self.manifest
    }

    pub fn roots(&self) -> &[FdbServedRoot] {
        &self.roots
    }

    pub fn registry(&self) -> &Arc<RootOwnerRegistry> {
        &self.registry
    }

    pub const fn lease_renew_interval(&self) -> Duration {
        self.lease_renew_interval
    }

    /// Publish every prepared route as Serving. Call this only after the RPC
    /// listener and lifecycle workers are ready.
    pub fn activate_routes(&self) -> Result<(), ServerError> {
        self.ownership.activate()
    }

    /// Release every prepared or serving owner session. This is idempotent so
    /// composition code can use it while unwinding before a server exists.
    pub fn release_ownership(&self) -> Result<(), ServerError> {
        self.ownership.release()
    }

    pub fn workspace_server(&self, options: ServerOptions) -> Result<WorkspaceServer, ServerError> {
        let maintenance: Arc<dyn OwnershipMaintenance> = self.ownership.clone();
        Ok(
            WorkspaceServer::new_distributed(options, Arc::clone(&self.registry), maintenance)?
                .with_discovery_source(self.discovery.clone()),
        )
    }
}

#[derive(Clone)]
struct FdbRouteDiscovery {
    control: Arc<FdbControlStore>,
}

impl RouteDiscoverySource for FdbRouteDiscovery {
    fn discover_route(&self, root_id: RootIdentity) -> Result<DiscoveredRoute, RpcFailure> {
        let root = RootId::from(root_id);
        let catalog = self
            .control
            .get_root_catalog(&root)
            .map_err(|_| discovery_unavailable("shared root catalog is unavailable", true))?
            .ok_or_else(|| discovery_unavailable("root is not provisioned", false))?;
        if catalog.state() != CatalogEntryState::Ready {
            return Err(discovery_unavailable(
                "root provisioning is not complete",
                true,
            ));
        }
        let route = self
            .control
            .get_route(&catalog.logical_shard_id())
            .map_err(|_| discovery_unavailable("shared shard route is unavailable", true))?
            .ok_or_else(|| discovery_unavailable("logical shard route is absent", true))?;
        if route.state() != ShardRouteState::Serving {
            return Err(RpcFailure {
                code: ErrorCode::RouteUnavailable,
                message: "logical shard route is not serving".to_owned(),
                retryable: true,
                conflict: None,
                current_generation: Some(catalog.placement_generation().get()),
                route_hint: None,
            });
        }
        let owner_epoch = route
            .owner_epoch()
            .ok_or_else(|| discovery_unavailable("serving route has no owner epoch", true))?;
        let session_generation = route.session_generation().ok_or_else(|| {
            discovery_unavailable("serving route has no session generation", true)
        })?;
        let endpoint = route
            .endpoint()
            .ok_or_else(|| discovery_unavailable("serving route has no endpoint", true))?;
        let owner_endpoint = OwnerEndpoint::new(endpoint.as_str()).map_err(|_| {
            discovery_unavailable("serving route has an invalid owner endpoint", true)
        })?;
        DiscoveredRoute::new(
            RootRoute {
                root_id,
                logical_shard_id: LogicalShardIdentity::from(catalog.logical_shard_id()),
                object_namespace_id: ObjectNamespaceIdentity::from(catalog.object_namespace_id()),
                placement_generation: catalog.placement_generation().get(),
                owner_epoch: owner_epoch.get(),
            },
            session_generation.get(),
            owner_endpoint,
            RouteState::Serving,
        )
        .map_err(|_| discovery_unavailable("serving route is internally inconsistent", true))
    }
}

fn discovery_unavailable(message: &str, retryable: bool) -> RpcFailure {
    RpcFailure {
        code: ErrorCode::RouteUnavailable,
        message: message.to_owned(),
        retryable,
        conflict: None,
        current_generation: None,
        route_hint: None,
    }
}

struct FdbOwnership {
    control: Arc<FdbControlStore>,
    registry: Arc<RootOwnerRegistry>,
    sessions: Vec<OwnerSession>,
    activated: AtomicBool,
    failed: AtomicBool,
    release_gate: Mutex<()>,
    released: AtomicBool,
}

impl FdbOwnership {
    fn activate(&self) -> Result<(), ServerError> {
        if self.released.load(Ordering::Acquire) {
            return Err(ServerError::InvalidBootstrap(
                "FoundationDB ownership was already released".to_owned(),
            ));
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(ServerError::InvalidBootstrap(
                "FoundationDB ownership is fail-closed".to_owned(),
            ));
        }
        if self.activated.load(Ordering::Acquire) {
            return Ok(());
        }
        for session in &self.sessions {
            if let Err(primary) = activate_route_exact(self.control.as_ref(), session) {
                return Err(self.fail_closed_after(primary));
            }
        }
        self.activated.store(true, Ordering::Release);
        Ok(())
    }

    fn fail_closed_after(&self, primary: ServerError) -> ServerError {
        self.failed.store(true, Ordering::Release);
        match self.fail_closed_all() {
            Ok(()) => primary,
            Err(cleanup) => ServerError::BootstrapRollback {
                primary: primary.to_string(),
                rollback: cleanup.to_string(),
            },
        }
    }

    fn fail_closed_all(&self) -> Result<(), ServerError> {
        let mut failures = Vec::new();
        for session in &self.sessions {
            if let Err(error) = self
                .registry
                .fail_closed_shard(LogicalShardIdentity::from(session.logical_shard_id()))
            {
                failures.push(format!(
                    "remove shard {:?} routes: {error}",
                    session.logical_shard_id()
                ));
            }
        }
        for session in &self.sessions {
            if let Err(error) = self.control.fail_closed(session) {
                failures.push(format!(
                    "fail-close shard {:?} control route: {error}",
                    session.logical_shard_id()
                ));
            }
        }
        combine_failures("fail-close FoundationDB ownership", failures)
    }
}

impl OwnershipMaintenance for FdbOwnership {
    fn renew(&self) -> Result<(), ServerError> {
        if self.released.load(Ordering::Acquire) {
            return Err(ServerError::InvalidBootstrap(
                "FoundationDB ownership was released".to_owned(),
            ));
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(ServerError::InvalidBootstrap(
                "FoundationDB ownership is fail-closed".to_owned(),
            ));
        }
        if !self.activated.load(Ordering::Acquire) {
            return Err(ServerError::InvalidBootstrap(
                "FoundationDB routes were not activated".to_owned(),
            ));
        }
        for session in &self.sessions {
            if let Err(error) = self.control.renew_owner(session) {
                return Err(self.fail_closed_after(ServerError::Control(error)));
            }
        }
        Ok(())
    }

    fn release(&self) -> Result<(), ServerError> {
        let _release_guard = self.release_gate.lock().map_err(|_| {
            ServerError::InvalidBootstrap("FoundationDB owner release lock is poisoned".to_owned())
        })?;
        if self.released.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut failures = Vec::new();
        for session in &self.sessions {
            if let Err(error) = self
                .registry
                .fail_closed_shard(LogicalShardIdentity::from(session.logical_shard_id()))
            {
                failures.push(format!(
                    "remove shard {:?} routes: {error}",
                    session.logical_shard_id()
                ));
            }
        }
        for session in &self.sessions {
            if let Err(error) = release_owner_exact(self.control.as_ref(), session) {
                failures.push(format!(
                    "release shard {:?}: {error}",
                    session.logical_shard_id()
                ));
            }
        }
        combine_failures("release FoundationDB ownership", failures)?;
        self.released.store(true, Ordering::Release);
        Ok(())
    }
}

fn combine_failures(operation: &str, failures: Vec<String>) -> Result<(), ServerError> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(ServerError::BootstrapRollback {
            primary: operation.to_owned(),
            rollback: failures.join("; "),
        })
    }
}

/// Create or inspect the exact FoundationDB manifest below the selected
/// prefix. No catalog, route, session, or metadata row is created here.
pub fn format_fdb(
    url: &FoundationDbMetadataUrl,
    created_by_version: &str,
) -> Result<FdbFormatOutcome, ServerError> {
    let runtime = start_runtime()?;
    let options = control_options(url)?;
    match FdbControlStore::inspect_manifest(&runtime, &options) {
        Ok(manifest) => Ok(FdbFormatOutcome {
            state: FdbFormatState::Existing,
            manifest,
        }),
        Err(ControlError::StoreNotFormatted) => {
            let candidate = StoreManifest::new(
                new_store_id(url),
                StoreProvider::FoundationDb,
                SUPPORTED_WORKSPACE_FORMAT_VERSION,
                FDB_PHYSICAL_ENCODING_VERSION,
                options.provider_namespace_digest(),
                created_by_version,
            )?;
            match FdbControlStore::format(&runtime, &options, candidate.clone()) {
                Ok(CreateOutcome::Created(manifest)) => Ok(FdbFormatOutcome {
                    state: FdbFormatState::Created,
                    manifest,
                }),
                Ok(CreateOutcome::Existing(manifest)) => Ok(FdbFormatOutcome {
                    state: FdbFormatState::Existing,
                    manifest,
                }),
                Err(ControlError::StoreManifestMismatch { actual, .. }) => {
                    options.validate_manifest_binding(&actual)?;
                    Ok(FdbFormatOutcome {
                        state: FdbFormatState::Existing,
                        manifest: *actual,
                    })
                }
                Err(error) => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Provision one root through create-only shared catalog records and an exact
/// session-fenced metadata root fence.
pub fn provision_fdb(
    url: &FoundationDbMetadataUrl,
    root_id: RootId,
    agent_id: AgentId,
) -> Result<FdbProvisionOutcome, ServerError> {
    let runtime = start_runtime()?;
    let options = control_options(url)?;
    let manifest = FdbControlStore::inspect_manifest(&runtime, &options)?;
    let control = Arc::new(FdbControlStore::open(&runtime, options, manifest.clone())?);
    let logical_shard_id = derive_logical_shard_id(manifest.store_id());
    let namespace_id = derive_namespace_id(manifest.store_id());
    let shard = create_or_load_shard(control.as_ref(), logical_shard_id)?;
    let desired_root = RootCatalogEntry::new(
        root_id,
        agent_id,
        namespace_id,
        logical_shard_id,
        PlacementGeneration::new(1).expect("one is a valid placement generation"),
        CatalogEntryState::Provisioning,
    );
    let (root, preexisting) = create_or_load_root(control.as_ref(), desired_root)?;
    if root.state() == CatalogEntryState::Ready && shard.state() == CatalogEntryState::Ready {
        return Ok(FdbProvisionOutcome {
            root,
            shard,
            preexisting: true,
        });
    }
    let session = acquire_exact_session(
        control.as_ref(),
        logical_shard_id,
        shard.state(),
        provision_owner(manifest.store_id(), root_id)?,
        RpcEndpoint::new(PROVISION_ENDPOINT)?,
        false,
        &NEVER_SHUTDOWN,
    )?;
    let result = (|| {
        let meta =
            open_provisioning_meta(&runtime, url, control.as_ref(), &session, shard.state())?;
        advance_shared_owner(meta.as_ref(), &session)?;
        reconcile_root_fence(meta.as_ref(), manifest.store_id(), &session, root)?;
        let ready_root = cas_root_ready(control.as_ref(), root)?;
        let ready_shard = cas_shard_ready(control.as_ref(), shard)?;
        Ok(FdbProvisionOutcome {
            root: ready_root,
            shard: ready_shard,
            preexisting,
        })
    })();
    let release = release_owner_exact(control.as_ref(), &session);
    match (result, release) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => Err(ServerError::BootstrapRollback {
            primary: primary.to_string(),
            rollback: cleanup.to_string(),
        }),
    }
}

/// Acquire every Ready shard, open exact session-fenced metadata handles, and
/// prepare local registry routes. Routes remain Activating until the caller
/// invokes [`FdbServingRuntime::activate_routes`].
pub fn serve_fdb(
    url: &FoundationDbMetadataUrl,
    node_id: NodeId,
    endpoint: SocketAddr,
    shutdown: &AtomicBool,
) -> Result<FdbServingRuntime, ServerError> {
    if endpoint.port() == 0 {
        return Err(ServerError::InvalidOptions(
            "FoundationDB advertised endpoint must have a nonzero port".to_owned(),
        ));
    }
    let runtime = start_runtime()?;
    let options = control_options(url)?;
    let lease_renew_interval = options.lease_ttl() / 3;
    let manifest = FdbControlStore::inspect_manifest(&runtime, &options)?;
    let control = Arc::new(FdbControlStore::open(&runtime, options, manifest.clone())?);
    let mut roots_by_shard = BTreeMap::<LogicalShardId, Vec<RootCatalogEntry>>::new();
    for root in control.list_root_catalog()? {
        if root.state() == CatalogEntryState::Ready {
            roots_by_shard
                .entry(root.logical_shard_id())
                .or_default()
                .push(root);
        }
    }
    if roots_by_shard.is_empty() {
        return Err(ServerError::InvalidBootstrap(
            "FoundationDB store has no Ready roots; run provision first".to_owned(),
        ));
    }
    let endpoint = RpcEndpoint::new(endpoint.to_string())?;
    let registry = Arc::new(RootOwnerRegistry::new());
    let mut sessions = Vec::with_capacity(roots_by_shard.len());
    let mut served_roots = Vec::new();
    let prepared = (|| {
        for (logical_shard_id, mut roots) in roots_by_shard {
            roots.sort_by_key(RootCatalogEntry::root_id);
            let shard = control
                .get_shard_catalog(&logical_shard_id)?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            if shard.state() != CatalogEntryState::Ready {
                return Err(ServerError::InvalidBootstrap(format!(
                    "Ready roots belong to shard {logical_shard_id:?} in state {:?}",
                    shard.state()
                )));
            }
            let session = acquire_exact_session(
                control.as_ref(),
                logical_shard_id,
                CatalogEntryState::Ready,
                node_id.clone(),
                endpoint.clone(),
                true,
                shutdown,
            )?;
            sessions.push(session.clone());
            let meta = open_fenced_meta(&runtime, url, control.as_ref(), &session)?;
            advance_shared_owner(meta.as_ref(), &session)?;
            let executor: Arc<dyn WorkspaceRequestExecutor> =
                Arc::new(MetadataWorkspaceRequestExecutor::new(Arc::clone(&meta)));
            for root in roots {
                validate_ready_root_fence(meta.as_ref(), logical_shard_id, root)?;
                let route = root_route(&session, root);
                registry.install(route, Arc::clone(&executor))?;
                served_roots.push(FdbServedRoot {
                    route,
                    meta: Arc::clone(&meta),
                });
            }
        }
        Ok::<(), ServerError>(())
    })();
    if let Err(primary) = prepared {
        let cleanup = rollback_prepared(control.as_ref(), &registry, &sessions);
        return match cleanup {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(ServerError::BootstrapRollback {
                primary: primary.to_string(),
                rollback: cleanup.to_string(),
            }),
        };
    }
    served_roots.sort_by_key(|root| root.route.root_id);
    let ownership = Arc::new(FdbOwnership {
        control: Arc::clone(&control),
        registry: Arc::clone(&registry),
        sessions,
        activated: AtomicBool::new(false),
        failed: AtomicBool::new(false),
        release_gate: Mutex::new(()),
        released: AtomicBool::new(false),
    });
    Ok(FdbServingRuntime {
        _runtime: runtime,
        manifest,
        roots: served_roots,
        registry,
        ownership,
        discovery: Arc::new(FdbRouteDiscovery { control }),
        lease_renew_interval,
    })
}

fn start_runtime() -> Result<FdbRuntime, ServerError> {
    FdbRuntime::start().map_err(|error| {
        ServerError::InvalidBootstrap(format!(
            "cannot start the process-global FoundationDB runtime: {error}"
        ))
    })
}

fn control_options(url: &FoundationDbMetadataUrl) -> Result<FdbControlOptions, ServerError> {
    Ok(FdbControlOptions::new(url.cluster_file(), url.prefix())?
        .with_lease_ttl(DEFAULT_LEASE_TTL)?)
}

fn create_or_load_shard(
    control: &FdbControlStore,
    logical_shard_id: LogicalShardId,
) -> Result<ShardCatalogEntry, ServerError> {
    match control.create_shard_catalog(logical_shard_id) {
        Ok(CreateOutcome::Created(entry) | CreateOutcome::Existing(entry)) => Ok(entry),
        Err(
            error @ (ControlError::TransactionConflict { .. }
            | ControlError::CommitOutcomeUnknown { .. }),
        ) => match control.get_shard_catalog(&logical_shard_id)? {
            Some(entry) => Ok(entry),
            None => Err(error.into()),
        },
        Err(error) => Err(error.into()),
    }
}

fn create_or_load_root(
    control: &FdbControlStore,
    desired: RootCatalogEntry,
) -> Result<(RootCatalogEntry, bool), ServerError> {
    match control.create_root_catalog(desired) {
        Ok(CreateOutcome::Created(entry)) => Ok((entry, false)),
        Ok(CreateOutcome::Existing(entry)) => Ok((entry, true)),
        Err(
            error @ (ControlError::RootCatalogAlreadyExists(_)
            | ControlError::TransactionConflict { .. }
            | ControlError::CommitOutcomeUnknown { .. }),
        ) => {
            let Some(current) = control.get_root_catalog(&desired.root_id())? else {
                return Err(error.into());
            };
            validate_root_catalog_transition(&desired, &current)?;
            Ok((current, true))
        }
        Err(error) => Err(error.into()),
    }
}

fn cas_root_ready(
    control: &FdbControlStore,
    root: RootCatalogEntry,
) -> Result<RootCatalogEntry, ServerError> {
    if root.state() == CatalogEntryState::Ready {
        return Ok(root);
    }
    let ready = root.with_state(CatalogEntryState::Ready);
    match control.compare_and_set_root_catalog(&root, ready) {
        Ok(entry) => Ok(entry),
        Err(
            error @ (ControlError::RootCatalogCasConflict { .. }
            | ControlError::TransactionConflict { .. }
            | ControlError::CommitOutcomeUnknown { .. }),
        ) => {
            if control.get_root_catalog(&root.root_id())? == Some(ready) {
                Ok(ready)
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn cas_shard_ready(
    control: &FdbControlStore,
    shard: ShardCatalogEntry,
) -> Result<ShardCatalogEntry, ServerError> {
    if shard.state() == CatalogEntryState::Ready {
        return Ok(shard);
    }
    let ready = shard.with_state(CatalogEntryState::Ready);
    match control.compare_and_set_shard_catalog(&shard, ready) {
        Ok(entry) => Ok(entry),
        Err(
            error @ (ControlError::ShardCatalogCasConflict { .. }
            | ControlError::TransactionConflict { .. }
            | ControlError::CommitOutcomeUnknown { .. }),
        ) => {
            if control.get_shard_catalog(&shard.logical_shard_id())? == Some(ready) {
                Ok(ready)
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn acquire_exact_session(
    control: &FdbControlStore,
    logical_shard_id: LogicalShardId,
    state: CatalogEntryState,
    owner: NodeId,
    endpoint: RpcEndpoint,
    wait_for_takeover: bool,
    shutdown: &AtomicBool,
) -> Result<OwnerSession, ServerError> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Err(ServerError::InvalidBootstrap(
                "FoundationDB owner acquisition was cancelled by shutdown".to_owned(),
            ));
        }
        let observed = control.observe_ownership(&logical_shard_id)?;
        let expected = plan_owner_acquisition(&observed, owner.clone(), endpoint.clone())?
            .session()
            .expect("owner acquisition plan has a session")
            .clone();
        let result = match state {
            CatalogEntryState::Provisioning => control.acquire_provisioning_owner(
                &logical_shard_id,
                owner.clone(),
                endpoint.clone(),
            ),
            CatalogEntryState::Ready => {
                control.acquire_owner(&logical_shard_id, owner.clone(), endpoint.clone())
            }
            CatalogEntryState::Retired => {
                return Err(ServerError::InvalidBootstrap(format!(
                    "retired shard {logical_shard_id:?} cannot acquire an owner"
                )));
            }
        };
        match result {
            Ok(session) if session == expected => return Ok(session),
            Ok(_) => {
                return Err(ServerError::InvalidBootstrap(
                    "FoundationDB owner acquisition returned an unexpected session".to_owned(),
                ));
            }
            Err(error @ ControlError::CommitOutcomeUnknown { .. }) => {
                let current = control.observe_ownership(&logical_shard_id)?;
                return if current.session() == Some(&expected) {
                    Ok(expected)
                } else {
                    Err(error.into())
                };
            }
            Err(ControlError::OwnershipObservationPending {
                remaining_millis, ..
            }) if wait_for_takeover => {
                let remaining = Duration::from_millis(remaining_millis.max(1));
                thread::sleep(remaining.min(OWNERSHIP_OBSERVATION_POLL));
            }
            Err(ControlError::TransactionConflict { .. }) if wait_for_takeover => {
                // This is a new high-level observation/acquisition attempt,
                // not an automatic retry of the conflicted raw commit.
                thread::sleep(OWNERSHIP_CONFLICT_BACKOFF);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn open_provisioning_meta(
    runtime: &FdbRuntime,
    url: &FoundationDbMetadataUrl,
    control: &FdbControlStore,
    session: &OwnerSession,
    state: CatalogEntryState,
) -> Result<Arc<meta::MetaShard>, ServerError> {
    let store = open_fenced_store(runtime, url, control, session)?;
    match state {
        CatalogEntryState::Ready => Ok(Arc::new(meta::MetaShard::open(
            store,
            session.logical_shard_id(),
        )?)),
        CatalogEntryState::Provisioning => {
            match meta::MetaShard::initialize(Arc::clone(&store), session.logical_shard_id()) {
                Ok(meta) => Ok(Arc::new(meta)),
                Err(initialization) => {
                    match meta::MetaShard::open(store, session.logical_shard_id()) {
                        Ok(meta) => Ok(Arc::new(meta)),
                        Err(_) => Err(initialization.into()),
                    }
                }
            }
        }
        CatalogEntryState::Retired => Err(ServerError::InvalidBootstrap(
            "retired shard metadata cannot be provisioned".to_owned(),
        )),
    }
}

fn open_fenced_meta(
    runtime: &FdbRuntime,
    url: &FoundationDbMetadataUrl,
    control: &FdbControlStore,
    session: &OwnerSession,
) -> Result<Arc<meta::MetaShard>, ServerError> {
    let store = open_fenced_store(runtime, url, control, session)?;
    Ok(Arc::new(meta::MetaShard::open(
        store,
        session.logical_shard_id(),
    )?))
}

fn open_fenced_store(
    runtime: &FdbRuntime,
    url: &FoundationDbMetadataUrl,
    control: &FdbControlStore,
    session: &OwnerSession,
) -> Result<Arc<dyn TxnStore>, ServerError> {
    let fence = FdbSessionFence::new(control.keys(), session.clone())?;
    let metadata_fence = FdbMetadataSessionFence::new(
        fence.key(),
        fence.expected_value(),
        session.owner_epoch().get(),
        session.session_generation().get(),
    )?;
    let store = FdbStore::open(
        runtime,
        FdbOptions::new(url.cluster_file(), url.prefix().as_bytes(), metadata_fence),
    )?;
    Ok(Arc::new(store))
}

fn advance_shared_owner(meta: &meta::MetaShard, session: &OwnerSession) -> Result<(), ServerError> {
    let current = meta.current_owner_epoch()?;
    if current == Some(session.owner_epoch()) {
        return Ok(());
    }
    if current.is_some_and(|epoch| epoch > session.owner_epoch()) {
        return Err(ServerError::InvalidBootstrap(format!(
            "FoundationDB metadata owner epoch is {current:?}, ahead of session {:?}",
            session.owner_epoch()
        )));
    }
    // A control owner may fail before opening metadata and still consume its
    // epoch. The next exact session therefore advances from the durable
    // metadata epoch directly; requiring N-1 would strand a healthy store
    // after any pre-open crash.
    meta.advance_owner_epoch(current, session.owner_epoch())?;
    Ok(())
}

fn reconcile_root_fence(
    meta: &meta::MetaShard,
    store_id: StoreId,
    session: &OwnerSession,
    root: RootCatalogEntry,
) -> Result<(), ServerError> {
    match meta.root_fence(root.root_id())? {
        None => execute_fence_command(
            meta,
            store_id,
            session,
            root,
            meta::RootFenceAction::Install,
            b"install",
        )?,
        Some(fence) => validate_root_fence(session.logical_shard_id(), root, fence)?,
    }
    let fence = meta.root_fence(root.root_id())?.ok_or_else(|| {
        ServerError::InvalidBootstrap("root fence install produced no durable fence".to_owned())
    })?;
    validate_root_fence(session.logical_shard_id(), root, fence)?;
    if fence.activation_state == RootActivationState::Installing {
        execute_fence_command(
            meta,
            store_id,
            session,
            root,
            meta::RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            b"activate",
        )?;
    }
    validate_ready_root_fence(meta, session.logical_shard_id(), root)
}

fn execute_fence_command(
    meta: &meta::MetaShard,
    store_id: StoreId,
    session: &OwnerSession,
    root: RootCatalogEntry,
    action: meta::RootFenceAction,
    action_name: &[u8],
) -> Result<(), ServerError> {
    let expected_state = match action {
        meta::RootFenceAction::Install => RootActivationState::Installing,
        meta::RootFenceAction::Transition { next, .. } => next,
        _ => {
            return Err(ServerError::InvalidBootstrap(
                "FDB provisioning uses only install and activate root-fence actions".to_owned(),
            ));
        }
    };
    let command = meta::MetadataCommand {
        schema_id: meta::SCHEMA_ID.to_owned(),
        root_id: root.root_id(),
        logical_shard_id: session.logical_shard_id(),
        object_namespace_id: Some(root.object_namespace_id()),
        placement_generation: root.placement_generation(),
        owner_epoch: session.owner_epoch(),
        request_id: derive_request_id(store_id, root.root_id(), action_name),
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: meta.current_read_version()?,
        root_fence_action: action,
        predicates: Vec::new(),
        mutations: Vec::new(),
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result: [b"nokv.fdb.root-fence.v1/".as_slice(), action_name].concat(),
    }
    .seal();
    if let Err(primary) = meta.execute(&command) {
        let reconciled = meta.root_fence(root.root_id())?;
        if reconciled.is_some_and(|fence| {
            validate_root_fence(session.logical_shard_id(), root, fence).is_ok()
                && (fence.activation_state == expected_state
                    || (expected_state == RootActivationState::Installing
                        && fence.activation_state == RootActivationState::Active))
        }) {
            return Ok(());
        }
        return Err(primary.into());
    }
    Ok(())
}

fn validate_ready_root_fence(
    meta: &meta::MetaShard,
    logical_shard_id: LogicalShardId,
    root: RootCatalogEntry,
) -> Result<(), ServerError> {
    let fence = meta.root_fence(root.root_id())?.ok_or_else(|| {
        ServerError::InvalidBootstrap(format!(
            "Ready root {:?} has no metadata fence",
            root.root_id()
        ))
    })?;
    validate_root_fence(logical_shard_id, root, fence)?;
    if fence.activation_state != RootActivationState::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "Ready root {:?} fence is {:?}, expected Active",
            root.root_id(),
            fence.activation_state
        )));
    }
    Ok(())
}

fn validate_root_fence(
    logical_shard_id: LogicalShardId,
    root: RootCatalogEntry,
    fence: meta::RootFence,
) -> Result<(), ServerError> {
    if fence.logical_shard_id != logical_shard_id
        || fence.object_namespace_id != Some(root.object_namespace_id())
        || fence.placement_generation != root.placement_generation()
    {
        return Err(ServerError::InvalidBootstrap(format!(
            "root {:?} catalog does not match its metadata fence",
            root.root_id()
        )));
    }
    if matches!(
        fence.activation_state,
        RootActivationState::Draining | RootActivationState::Fenced
    ) {
        return Err(ServerError::InvalidBootstrap(format!(
            "root {:?} fence is {:?}",
            root.root_id(),
            fence.activation_state
        )));
    }
    Ok(())
}

fn root_route(session: &OwnerSession, root: RootCatalogEntry) -> RootRoute {
    RootRoute {
        root_id: RootIdentity::from(root.root_id()),
        logical_shard_id: LogicalShardIdentity::from(session.logical_shard_id()),
        object_namespace_id: ObjectNamespaceIdentity::from(root.object_namespace_id()),
        placement_generation: root.placement_generation().get(),
        owner_epoch: session.owner_epoch().get(),
    }
}

fn activate_route_exact(
    control: &FdbControlStore,
    session: &OwnerSession,
) -> Result<(), ServerError> {
    match control.activate_route(session) {
        Ok(route)
            if route.state() == ShardRouteState::Serving
                && route.owner_epoch() == Some(session.owner_epoch())
                && route.session_generation() == Some(session.session_generation()) =>
        {
            Ok(())
        }
        Ok(_) => Err(ServerError::InvalidBootstrap(
            "FoundationDB route activation returned a mismatching route".to_owned(),
        )),
        Err(error @ ControlError::CommitOutcomeUnknown { .. }) => {
            let route = control.get_route(&session.logical_shard_id())?;
            if route.as_ref().is_some_and(|route| {
                route.state() == ShardRouteState::Serving
                    && route.owner_epoch() == Some(session.owner_epoch())
                    && route.session_generation() == Some(session.session_generation())
            }) {
                Ok(())
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn release_owner_exact(
    control: &FdbControlStore,
    session: &OwnerSession,
) -> Result<(), ServerError> {
    match control.release_owner(session) {
        Ok(route)
            if route.state() == ShardRouteState::Unassigned
                && route.owner_epoch() == Some(session.owner_epoch())
                && route.session_generation() == Some(session.session_generation()) =>
        {
            Ok(())
        }
        Ok(_) => Err(ServerError::InvalidBootstrap(
            "FoundationDB owner release returned a mismatching route".to_owned(),
        )),
        Err(error @ ControlError::CommitOutcomeUnknown { .. }) => {
            let ownership = control.observe_ownership(&session.logical_shard_id())?;
            if ownership.route().state() == ShardRouteState::Unassigned
                && ownership.session().is_none()
                && ownership.route().owner_epoch() == Some(session.owner_epoch())
                && ownership.route().session_generation() == Some(session.session_generation())
            {
                Ok(())
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn rollback_prepared(
    control: &FdbControlStore,
    registry: &RootOwnerRegistry,
    sessions: &[OwnerSession],
) -> Result<(), ServerError> {
    let mut failures = Vec::new();
    for session in sessions {
        if let Err(error) =
            registry.fail_closed_shard(LogicalShardIdentity::from(session.logical_shard_id()))
        {
            failures.push(error.to_string());
        }
        if let Err(error) = release_owner_exact(control, session) {
            failures.push(error.to_string());
        }
    }
    combine_failures("rollback prepared FoundationDB owners", failures)
}

fn provision_owner(store_id: StoreId, root_id: RootId) -> Result<NodeId, ServerError> {
    let digest = derive_digest(
        PROVISION_OWNER_DOMAIN,
        &[store_id.as_bytes(), root_id.as_bytes()],
    );
    NodeId::new(format!(
        "provision-{}-{}",
        lower_hex(&digest[..8]),
        std::process::id()
    ))
    .map_err(|error| ServerError::InvalidBootstrap(format!("invalid provision owner: {error:?}")))
}

fn new_store_id(url: &FoundationDbMetadataUrl) -> StoreId {
    let mut hasher = Sha256::new();
    hasher.update(STORE_ID_DOMAIN);
    hasher.update(url.prefix().as_bytes());
    hasher.update(std::process::id().to_be_bytes());
    hasher.update(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_be_bytes(),
    );
    hasher.update(
        STORE_ID_SEQUENCE
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes(),
    );
    let digest: [u8; 32] = hasher.finalize().into();
    StoreId::from_bytes(nonzero_fixed_id(&digest))
}

fn derive_logical_shard_id(store_id: StoreId) -> LogicalShardId {
    let digest = derive_digest(SHARD_ID_DOMAIN, &[store_id.as_bytes()]);
    LogicalShardId::from_bytes(nonzero_fixed_id(&digest))
}

fn derive_namespace_id(store_id: StoreId) -> ObjectNamespaceId {
    let digest = derive_digest(NAMESPACE_ID_DOMAIN, &[store_id.as_bytes()]);
    ObjectNamespaceId::from_bytes(nonzero_fixed_id(&digest))
}

fn derive_request_id(store_id: StoreId, root_id: RootId, action: &[u8]) -> RequestId {
    let digest = derive_digest(
        REQUEST_ID_DOMAIN,
        &[store_id.as_bytes(), root_id.as_bytes(), action],
    );
    RequestId::from_bytes(nonzero_fixed_id(&digest))
}

fn derive_digest(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn nonzero_fixed_id(digest: &[u8; 32]) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    if id.iter().all(|byte| *byte == 0) {
        id[15] = 1;
    }
    id
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_store_children_are_stable_nonzero_and_domain_separated() {
        let store = StoreId::from_bytes([1; 16]);
        let shard = derive_logical_shard_id(store);
        let namespace = derive_namespace_id(store);
        assert_ne!(shard.as_bytes(), &[0; 16]);
        assert_ne!(namespace.as_bytes(), &[0; 16]);
        assert_ne!(shard.as_bytes(), namespace.as_bytes());
        assert_eq!(derive_logical_shard_id(store), shard);
        assert_eq!(derive_namespace_id(store), namespace);
    }

    #[test]
    fn provision_request_ids_are_action_and_store_bound() {
        let first = StoreId::from_bytes([1; 16]);
        let second = StoreId::from_bytes([2; 16]);
        let root = RootId::from_bytes([3; 16]);
        assert_ne!(
            derive_request_id(first, root, b"install"),
            derive_request_id(first, root, b"activate")
        );
        assert_ne!(
            derive_request_id(first, root, b"install"),
            derive_request_id(second, root, b"install")
        );
    }
}
