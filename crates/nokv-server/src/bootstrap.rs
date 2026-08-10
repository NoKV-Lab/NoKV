/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use nokv_control::{
    ControlStore, LogicalShardLease, LogicalShardRecord, NodeId, OwnerEpoch, RecoveryPublication,
    RootId, RootPlacement, RootPlacementLifecycle,
};
use nokv_meta::workspace as meta;
use nokv_meta_holt::{HoltOptions, HoltStore, TreeBinding};
use nokv_meta_store::TxnStore;
use nokv_protocol::{LogicalShardIdentity, RootIdentity, RootRoute};
use nokv_types::{CommandDigest, RequestId, RootActivationState, SHA256_BYTES};

use crate::{
    MetadataWorkspaceRequestExecutor, RootOwnerRegistry, ServerError, WorkspaceRequestExecutor,
};

/// Explicit metadata-store opening mode. Startup never guesses from path
/// existence and never falls back between new and existing stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenMode {
    New(PathBuf),
    Existing(PathBuf),
}

/// Exact control-plane admission used by one logical-shard bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseMode {
    Acquire {
        owner: NodeId,
        endpoint: String,
        previous_epoch: Option<OwnerEpoch>,
    },
    Resume {
        lease: LogicalShardLease,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootAttach {
    /// Root whose persisted placement must point at this shard.
    pub root_id: RootId,
    /// Idempotency key for the durable fence installation.
    pub install_id: RequestId,
    /// Idempotency key for the durable transition to Active.
    pub activate_id: RequestId,
}

/// Complete input for opening and serving one logical metadata shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardBoot {
    /// Logical shard that owns the store and lease.
    pub shard_id: nokv_types::LogicalShardId,
    /// Exact physical-store open mode.
    pub open: OpenMode,
    /// Exact control-plane lease admission mode.
    pub lease: LeaseMode,
    /// Recovery frontier that the serving record will publish.
    pub recovery: RecoveryPublication,
    /// Initial roots to fence and route through the shared executor.
    pub roots: Vec<RootAttach>,
}

/// One control-backed logical-shard owner retained by the serving runtime.
///
/// A failed renewal removes every exact local route before exposing the lease
/// failure. All attached roots share one lease, metadata shard, and executor.
pub struct ShardOwner {
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    lease: LogicalShardLease,
    serving: LogicalShardRecord,
    meta: Arc<meta::MetaShard>,
    routes: Vec<RootRoute>,
}

impl ShardOwner {
    pub fn shard_id(&self) -> nokv_types::LogicalShardId {
        self.lease.logical_shard_id
    }

    pub fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub fn serving_record(&self) -> &LogicalShardRecord {
        &self.serving
    }

    pub fn meta(&self) -> &Arc<meta::MetaShard> {
        &self.meta
    }

    pub fn routes(&self) -> &[RootRoute] {
        &self.routes
    }

    pub(crate) fn is_for_registry(&self, registry: &Arc<RootOwnerRegistry>) -> bool {
        Arc::ptr_eq(&self.registry, registry)
    }

    /// Renew the shard lease. Lease loss removes every route before returning.
    pub fn renew_or_uninstall(&self) -> Result<LogicalShardRecord, ServerError> {
        match self.control.renew_owner(&self.lease) {
            Ok(record) => Ok(record),
            Err(error) => {
                let primary = ServerError::Control(error);
                let cleanup = uninstall_routes(&self.registry, &self.routes, "lease-lost");
                if cleanup.is_empty() {
                    Err(primary)
                } else {
                    Err(ServerError::BootstrapRollback {
                        primary: primary.to_string(),
                        rollback: cleanup.join("; "),
                    })
                }
            }
        }
    }

    /// Stop admission for every attached root and release the shard lease once.
    pub fn release(self) -> Result<LogicalShardRecord, ServerError> {
        let cleanup = uninstall_routes(&self.registry, &self.routes, "released");
        match (self.control.release_owner(&self.lease), cleanup.is_empty()) {
            (Ok(record), true) => Ok(record),
            (Ok(_), false) => Err(ServerError::BootstrapRollback {
                primary: "release logical-shard owner succeeded".to_owned(),
                rollback: cleanup.join("; "),
            }),
            (Err(control), true) => Err(ServerError::Control(control)),
            (Err(control), false) => Err(ServerError::BootstrapRollback {
                primary: format!("release logical-shard owner: {control}"),
                rollback: cleanup.join("; "),
            }),
        }
    }
}

/// Acquire or resume one logical-shard lease, open one metadata shard, attach
/// all initial roots, and publish the shard as serving.
pub fn bootstrap_shard(
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    boot: ShardBoot,
) -> Result<ShardOwner, ServerError> {
    validate_boot(&boot)?;
    let placements = load_placements(control.as_ref(), boot.shard_id, &boot.roots)?;
    reject_unrecovered_shared_frontier(control.as_ref(), boot.shard_id, &boot.recovery)?;
    validate_open_lease(&boot.open, &boot.lease)?;

    // Initialize a new first-owner store, or reopen the same prepared
    // epoch-zero store, before the control plane consumes the first epoch.
    // Exact resumes keep the existing admission-before-open order.
    let prepared_meta = prepare_first_owner_meta(&boot.open, &boot.lease, boot.shard_id)?;

    let (lease, acquired) = admit_owner(control.as_ref(), boot.shard_id, &boot.lease)?;
    let mut routes = Vec::with_capacity(placements.len());

    let meta = match prepared_meta {
        Some(meta) => meta,
        None => {
            let meta = match open_meta(boot.open, boot.shard_id) {
                Ok(meta) => meta,
                Err(error) => {
                    return Err(rollback_bootstrap(
                        error,
                        control.as_ref(),
                        &registry,
                        &routes,
                        &lease,
                        acquired,
                    ));
                }
            };
            if let Err(error) = validate_meta_shard(&meta, boot.shard_id) {
                return Err(rollback_bootstrap(
                    error,
                    control.as_ref(),
                    &registry,
                    &routes,
                    &lease,
                    acquired,
                ));
            }
            meta
        }
    };
    if let Err(error) = activate_shard(&meta, &lease) {
        return Err(rollback_bootstrap(
            error,
            control.as_ref(),
            &registry,
            &routes,
            &lease,
            acquired,
        ));
    }

    let executor = Arc::new(MetadataWorkspaceRequestExecutor::new(Arc::clone(&meta)));
    for (placement, root) in placements.iter().zip(&boot.roots) {
        match attach_root(&meta, &registry, &executor, placement, &lease, root) {
            Ok(route) => routes.push(route),
            Err(error) => {
                return Err(rollback_bootstrap(
                    error,
                    control.as_ref(),
                    &registry,
                    &routes,
                    &lease,
                    acquired,
                ));
            }
        }
    }
    if let Err(error) = control.renew_owner(&lease) {
        return Err(rollback_bootstrap(
            ServerError::Control(error),
            control.as_ref(),
            &registry,
            &routes,
            &lease,
            acquired,
        ));
    }
    let serving = match control.mark_serving(&lease, boot.recovery) {
        Ok(record) => record,
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::Control(error),
                control.as_ref(),
                &registry,
                &routes,
                &lease,
                acquired,
            ));
        }
    };
    Ok(ShardOwner {
        control,
        registry,
        lease,
        serving,
        meta,
        routes,
    })
}

fn validate_boot(boot: &ShardBoot) -> Result<(), ServerError> {
    if boot.roots.is_empty() {
        return Err(ServerError::InvalidBootstrap(
            "logical-shard bootstrap requires at least one root".to_owned(),
        ));
    }
    let mut roots = BTreeSet::new();
    for root in &boot.roots {
        if root.install_id == root.activate_id {
            return Err(ServerError::InvalidBootstrap(format!(
                "root {:?} install and activate request ids must differ",
                root.root_id
            )));
        }
        if !roots.insert(root.root_id) {
            return Err(ServerError::InvalidBootstrap(format!(
                "root {:?} is attached more than once",
                root.root_id
            )));
        }
    }
    Ok(())
}

fn load_placements(
    control: &dyn ControlStore,
    shard_id: nokv_types::LogicalShardId,
    roots: &[RootAttach],
) -> Result<Vec<RootPlacement>, ServerError> {
    roots
        .iter()
        .map(|root| {
            let placement = control.get_root_placement(&root.root_id)?.ok_or_else(|| {
                ServerError::InvalidBootstrap(format!(
                    "root {:?} placement does not exist",
                    root.root_id
                ))
            })?;
            validate_serving_placement(&placement)?;
            if placement.logical_shard_id != shard_id {
                return Err(ServerError::InvalidBootstrap(format!(
                    "root {:?} belongs to logical shard {:?}, requested {:?}",
                    placement.root_id, placement.logical_shard_id, shard_id
                )));
            }
            Ok(placement)
        })
        .collect()
}

fn validate_open_lease(open: &OpenMode, lease: &LeaseMode) -> Result<(), ServerError> {
    match (open, lease) {
        (
            OpenMode::New(_) | OpenMode::Existing(_),
            LeaseMode::Acquire {
                previous_epoch: None,
                ..
            },
        )
        | (OpenMode::Existing(_), LeaseMode::Resume { .. }) => Ok(()),
        (
            _,
            LeaseMode::Acquire {
                previous_epoch: Some(previous),
                ..
            },
        ) => Err(ServerError::InvalidBootstrap(format!(
            "successor acquisition after owner epoch {previous} is unavailable in the local-WAL profile; verified checkpoint/log recovery must complete before a new owner may serve"
        ))),
        (OpenMode::New(_), LeaseMode::Resume { .. }) => {
            Err(ServerError::InvalidBootstrap(
                "an exact owner resume must open its existing metadata store".to_owned(),
            ))
        }
    }
}

fn prepare_first_owner_meta(
    open: &OpenMode,
    lease: &LeaseMode,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<Option<Arc<meta::MetaShard>>, ServerError> {
    if !matches!(
        lease,
        LeaseMode::Acquire {
            previous_epoch: None,
            ..
        }
    ) {
        return Ok(None);
    }

    let meta = open_meta(open.clone(), logical_shard_id)?;
    validate_meta_shard(&meta, logical_shard_id)?;
    if let Some(owner_epoch) = meta.current_owner_epoch()? {
        return Err(ServerError::InvalidBootstrap(format!(
            "metadata shard is already fenced at owner epoch {owner_epoch}; reopen it only with Resume and the exact lease"
        )));
    }
    Ok(Some(meta))
}

fn reject_unrecovered_shared_frontier(
    control: &dyn ControlStore,
    shard_id: nokv_types::LogicalShardId,
    requested: &RecoveryPublication,
) -> Result<(), ServerError> {
    let shard = control
        .get_logical_shard(&shard_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("logical shard does not exist".to_owned()))?;
    let persisted_nonempty =
        shard.durable_lsn != 0 || shard.checkpoint.is_some() || shard.log.is_some();
    let requested_nonempty =
        requested.durable_lsn != 0 || requested.checkpoint.is_some() || requested.log.is_some();
    if persisted_nonempty || requested_nonempty {
        return Err(ServerError::InvalidBootstrap(format!(
            "logical shard {:?} has an unverified shared recovery frontier (persisted LSN {}, requested LSN {}); checkpoint/log install, replay, and fsck are not implemented, so Serving is refused",
            shard_id, shard.durable_lsn, requested.durable_lsn
        )));
    }
    Ok(())
}

fn validate_serving_placement(placement: &RootPlacement) -> Result<(), ServerError> {
    if matches!(
        placement.lifecycle,
        RootPlacementLifecycle::Draining | RootPlacementLifecycle::Retired
    ) {
        return Err(ServerError::InvalidBootstrap(format!(
            "root placement is {:?}",
            placement.lifecycle
        )));
    }
    Ok(())
}

fn admit_owner(
    control: &dyn ControlStore,
    shard_id: nokv_types::LogicalShardId,
    mode: &LeaseMode,
) -> Result<(LogicalShardLease, bool), ServerError> {
    match mode {
        LeaseMode::Acquire {
            owner,
            endpoint,
            previous_epoch,
        } => {
            let lease = match previous_epoch {
                None => control.acquire_owner(&shard_id, owner.clone(), endpoint.clone())?,
                Some(expected) => control.acquire_successor(
                    &shard_id,
                    *expected,
                    owner.clone(),
                    endpoint.clone(),
                )?,
            };
            Ok((lease, true))
        }
        LeaseMode::Resume { lease } => {
            if lease.logical_shard_id != shard_id {
                return Err(ServerError::InvalidBootstrap(
                    "resumed lease belongs to another logical shard".to_owned(),
                ));
            }
            control.renew_owner(lease)?;
            Ok((lease.clone(), false))
        }
    }
}

fn open_meta(
    mode: OpenMode,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<Arc<meta::MetaShard>, ServerError> {
    let catalog = || {
        meta::keyspaces()
            .iter()
            .map(|definition| TreeBinding::new(definition.id, definition.name))
    };
    match mode {
        OpenMode::New(path) => {
            let holt =
                HoltStore::initialize(HoltOptions::file(path, catalog(), meta::store_limits()))?;
            let store: Arc<dyn TxnStore> = Arc::new(holt);
            Ok(Arc::new(meta::MetaShard::initialize(
                store,
                logical_shard_id,
            )?))
        }
        OpenMode::Existing(path) => {
            let holt = HoltStore::open(HoltOptions::file(path, catalog(), meta::store_limits()))?;
            let store: Arc<dyn TxnStore> = Arc::new(holt);
            Ok(Arc::new(meta::MetaShard::open(store, logical_shard_id)?))
        }
    }
}

fn validate_meta_shard(
    meta: &meta::MetaShard,
    requested: nokv_types::LogicalShardId,
) -> Result<(), ServerError> {
    if meta.logical_shard_id() == requested {
        Ok(())
    } else {
        Err(ServerError::InvalidBootstrap(
            "metadata shard identity differs from the requested shard".to_owned(),
        ))
    }
}

fn root_route(placement: &RootPlacement, lease: &LogicalShardLease) -> RootRoute {
    RootRoute {
        root_id: RootIdentity::from(placement.root_id),
        logical_shard_id: LogicalShardIdentity::from(placement.logical_shard_id),
        placement_generation: placement.placement_generation.get(),
        owner_epoch: lease.owner_epoch.get(),
    }
}

fn activate_shard(meta: &meta::MetaShard, lease: &LogicalShardLease) -> Result<(), ServerError> {
    let current_owner = meta.current_owner_epoch()?;
    if current_owner != Some(lease.owner_epoch) {
        let expected_previous = predecessor(lease.owner_epoch);
        if current_owner != expected_previous {
            return Err(ServerError::InvalidBootstrap(format!(
                "metadata owner epoch is {}, lease requires predecessor {} or exact epoch {}",
                display_epoch(current_owner),
                display_epoch(expected_previous),
                lease.owner_epoch
            )));
        }
        meta.advance_owner_epoch(expected_previous, lease.owner_epoch)?;
    }
    Ok(())
}

fn attach_root(
    meta: &meta::MetaShard,
    registry: &RootOwnerRegistry,
    executor: &Arc<MetadataWorkspaceRequestExecutor>,
    placement: &RootPlacement,
    lease: &LogicalShardLease,
    root: &RootAttach,
) -> Result<RootRoute, ServerError> {
    debug_assert_eq!(placement.root_id, root.root_id);

    match meta.root_fence(placement.root_id)? {
        None => execute_fence_command(
            meta,
            placement,
            lease,
            root.install_id,
            meta::RootFenceAction::Install,
            b"nokv.root-fence.install.v1".to_vec(),
        )?,
        Some(fence) => validate_existing_fence(placement, fence)?,
    }

    let fence = meta.root_fence(placement.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap("root fence install produced no durable fence".to_owned())
    })?;
    validate_existing_fence(placement, fence)?;
    if fence.activation_state == RootActivationState::Installing {
        execute_fence_command(
            meta,
            placement,
            lease,
            root.activate_id,
            meta::RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            b"nokv.root-fence.activate.v1".to_vec(),
        )?;
    }

    let active = meta
        .root_fence(placement.root_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("active root fence disappeared".to_owned()))?;
    validate_existing_fence(placement, active)?;
    if active.activation_state != RootActivationState::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "root fence is {:?}, expected Active",
            active.activation_state
        )));
    }
    let route = root_route(placement, lease);
    let installed_executor: Arc<dyn WorkspaceRequestExecutor> = executor.clone();
    registry.install(route, installed_executor)?;
    Ok(route)
}

fn validate_existing_fence(
    placement: &RootPlacement,
    fence: meta::RootFence,
) -> Result<(), ServerError> {
    if fence.logical_shard_id != placement.logical_shard_id
        || fence.placement_generation != placement.placement_generation
    {
        return Err(ServerError::InvalidBootstrap(
            "root fence placement does not match the control-plane placement".to_owned(),
        ));
    }
    if matches!(
        fence.activation_state,
        RootActivationState::Draining | RootActivationState::Fenced
    ) {
        return Err(ServerError::InvalidBootstrap(format!(
            "root fence is {:?}",
            fence.activation_state
        )));
    }
    Ok(())
}

fn execute_fence_command(
    meta: &meta::MetaShard,
    placement: &RootPlacement,
    lease: &LogicalShardLease,
    request_id: RequestId,
    action: meta::RootFenceAction,
    deterministic_result: Vec<u8>,
) -> Result<(), ServerError> {
    let command = meta::MetadataCommand {
        schema_id: meta::SCHEMA_ID.to_owned(),
        root_id: placement.root_id,
        logical_shard_id: placement.logical_shard_id,
        placement_generation: placement.placement_generation,
        owner_epoch: lease.owner_epoch,
        request_id,
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: meta.current_read_version()?,
        root_fence_action: action,
        predicates: Vec::new(),
        mutations: Vec::new(),
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result,
    }
    .seal();
    meta.execute(&command)?;
    Ok(())
}

fn predecessor(epoch: OwnerEpoch) -> Option<OwnerEpoch> {
    let previous = epoch.get() - 1;
    (previous != 0).then(|| OwnerEpoch::new(previous).expect("nonzero predecessor is valid"))
}

fn display_epoch(epoch: Option<OwnerEpoch>) -> String {
    epoch.map_or_else(|| "epoch-zero".to_owned(), |epoch| epoch.get().to_string())
}

fn rollback_bootstrap(
    primary: ServerError,
    control: &dyn ControlStore,
    registry: &RootOwnerRegistry,
    routes: &[RootRoute],
    lease: &LogicalShardLease,
    release_acquired_lease: bool,
) -> ServerError {
    let mut rollback = uninstall_routes(registry, routes, "bootstrap rollback");
    if release_acquired_lease {
        if let Err(error) = control.release_owner(lease) {
            rollback.push(format!("release logical-shard lease: {error}"));
        }
    }
    if rollback.is_empty() {
        primary
    } else {
        ServerError::BootstrapRollback {
            primary: primary.to_string(),
            rollback: rollback.join("; "),
        }
    }
}

fn uninstall_routes(
    registry: &RootOwnerRegistry,
    routes: &[RootRoute],
    reason: &str,
) -> Vec<String> {
    routes
        .iter()
        .rev()
        .filter_map(|route| {
            registry
                .remove(*route)
                .err()
                .map(|error| format!("remove {reason} root route {:?}: {error}", route.root_id))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nokv_control::{
        InMemoryControlStore, LogRef, LogSegmentRef, LogicalShardState, PlacementGeneration,
        RootPlacement,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{ServerOptions, WorkspaceServer};

    fn root(fill: u8) -> RootId {
        RootId::from_bytes([fill; nokv_types::FIXED_ID_BYTES])
    }

    fn shard() -> nokv_types::LogicalShardId {
        nokv_types::LogicalShardId::from_bytes([0x40; nokv_types::FIXED_ID_BYTES])
    }

    fn other_shard() -> nokv_types::LogicalShardId {
        nokv_types::LogicalShardId::from_bytes([0x41; nokv_types::FIXED_ID_BYTES])
    }

    fn request_id(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; nokv_types::FIXED_ID_BYTES])
    }

    fn empty_recovery() -> RecoveryPublication {
        RecoveryPublication {
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        }
    }

    fn log_recovery(lsn: u64) -> RecoveryPublication {
        let digest = format!("state-{lsn}");
        RecoveryPublication {
            checkpoint: None,
            log: Some(LogRef {
                segments: vec![LogSegmentRef {
                    segment_key: format!("logs/{lsn}-{lsn}"),
                    first_lsn: lsn,
                    last_lsn: lsn,
                    digest: digest.clone(),
                }],
                durable_lsn: lsn,
                digest,
            }),
            durable_lsn: lsn,
        }
    }

    fn add_active_root(
        control: &InMemoryControlStore,
        root_id: RootId,
        logical_shard_id: nokv_types::LogicalShardId,
    ) {
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id,
                logical_shard_id,
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        control
            .compare_and_set_root_placement(
                &provisioning,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..provisioning
                },
            )
            .unwrap();
    }

    fn active_control(roots: &[RootId]) -> Arc<InMemoryControlStore> {
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        for root_id in roots {
            add_active_root(control.as_ref(), *root_id, shard());
        }
        control
    }

    fn root_attach(root_id: RootId, seed: u8) -> RootAttach {
        RootAttach {
            root_id,
            install_id: request_id(seed),
            activate_id: request_id(seed.saturating_add(1)),
        }
    }

    fn acquire_boot(path: PathBuf, roots: &[RootId]) -> ShardBoot {
        ShardBoot {
            shard_id: shard(),
            open: OpenMode::New(path),
            lease: LeaseMode::Acquire {
                owner: NodeId::new("node-a").unwrap(),
                endpoint: "127.0.0.1:9010".to_owned(),
                previous_epoch: None,
            },
            recovery: empty_recovery(),
            roots: roots
                .iter()
                .enumerate()
                .map(|(index, root_id)| {
                    root_attach(
                        *root_id,
                        3_u8.saturating_add((index as u8).saturating_mul(2)),
                    )
                })
                .collect(),
        }
    }

    fn resume_boot(
        path: PathBuf,
        lease: LogicalShardLease,
        roots: &[RootId],
        seed: u8,
    ) -> ShardBoot {
        ShardBoot {
            shard_id: shard(),
            open: OpenMode::Existing(path),
            lease: LeaseMode::Resume { lease },
            recovery: empty_recovery(),
            roots: roots
                .iter()
                .enumerate()
                .map(|(index, root_id)| {
                    root_attach(
                        *root_id,
                        seed.saturating_add((index as u8).saturating_mul(2)),
                    )
                })
                .collect(),
        }
    }

    fn as_control(control: &Arc<InMemoryControlStore>) -> Arc<dyn ControlStore> {
        control.clone()
    }

    fn options() -> ServerOptions {
        ServerOptions {
            bind: "127.0.0.1:0".parse().unwrap(),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
            lease_renew_interval: Duration::from_millis(10),
        }
    }

    #[test]
    fn fresh_bootstrap_activates_fence_installs_route_and_marks_serving() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(temporary.path().join("metadata"), &[root_id]),
        )
        .unwrap();

        let route = owner.routes()[0];
        assert_eq!(route.root_id, RootIdentity::from(root_id));
        assert_eq!(route.logical_shard_id, LogicalShardIdentity::from(shard()));
        assert_eq!(route.placement_generation, 2);
        assert_eq!(route.owner_epoch, 1);
        assert!(registry.contains_exact(route).unwrap());
        assert_eq!(
            owner
                .meta()
                .root_fence(root_id)
                .unwrap()
                .unwrap()
                .activation_state,
            RootActivationState::Active
        );
        assert_eq!(
            owner.meta().current_owner_epoch().unwrap(),
            Some(owner.lease().owner_epoch)
        );
        assert_eq!(owner.serving_record().state, LogicalShardState::Serving);

        let server = WorkspaceServer::new(options(), Arc::clone(&registry), vec![owner]).unwrap();
        server.renew_ownership().unwrap();
        assert_eq!(server.health().unwrap().installed_roots, 1);
        server.release_ownership().unwrap();
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn exact_current_owner_reopens_without_reinstalling_the_active_fence() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let route = first.routes()[0];
        let lease = first.lease().clone();
        assert!(registry.remove(route).unwrap());
        drop(first);

        let reopened = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            resume_boot(database, lease.clone(), &[root_id], 5),
        )
        .unwrap();

        assert_eq!(reopened.routes(), &[route]);
        assert_eq!(reopened.lease(), &lease);
        assert_eq!(
            reopened
                .meta()
                .root_fence(root_id)
                .unwrap()
                .unwrap()
                .activation_state,
            RootActivationState::Active
        );
        reopened.release().unwrap();
    }

    #[test]
    fn exact_owner_resume_cannot_open_a_new_store() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database, &[root_id]),
        )
        .unwrap();
        let route = first.routes()[0];
        let lease = first.lease().clone();
        assert!(registry.remove(route).unwrap());
        drop(first);

        let mut boot = resume_boot(
            temporary.path().join("replacement"),
            lease.clone(),
            &[root_id],
            35,
        );
        boot.open = OpenMode::New(temporary.path().join("replacement"));
        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), boot)
            .err()
            .unwrap();

        assert!(error.to_string().contains("must open its existing"));
        assert!(!temporary.path().join("replacement").exists());
        assert_eq!(
            control
                .get_logical_shard(&shard())
                .unwrap()
                .unwrap()
                .owner_epoch,
            Some(lease.owner_epoch)
        );
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        control.release_owner(&lease).unwrap();
    }

    #[test]
    fn stale_store_placement_is_rejected_and_never_installed() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let route = first.routes()[0];
        let lease = first.lease().clone();
        registry.remove(route).unwrap();
        drop(first);

        let active = control.get_root_placement(&root_id).unwrap().unwrap();
        let draining = control
            .compare_and_set_root_placement(
                &active,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(3).unwrap(),
                    lifecycle: RootPlacementLifecycle::Draining,
                    ..active
                },
            )
            .unwrap();
        control
            .compare_and_set_root_placement(
                &draining,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(4).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..draining
                },
            )
            .unwrap();

        let error = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            resume_boot(database, lease.clone(), &[root_id], 7),
        )
        .err()
        .unwrap();
        assert!(matches!(error, ServerError::InvalidBootstrap(_)));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        control.release_owner(&lease).unwrap();
    }

    #[test]
    fn missing_prepared_store_is_rejected_before_acquiring_a_lease() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut boot = acquire_boot(temporary.path().join("missing"), &[root_id]);
        boot.open = OpenMode::Existing(temporary.path().join("missing"));

        assert!(bootstrap_shard(as_control(&control), registry.clone(), boot).is_err());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert!(record.owner.is_none());
        assert_eq!(record.lease_id, 0);
        assert!(record.owner_epoch.is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn prepared_first_owner_store_retries_after_settled_acquire_failure() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut first = acquire_boot(database.clone(), &[root_id]);
        let LeaseMode::Acquire { endpoint, .. } = &mut first.lease else {
            unreachable!("acquire_boot always constructs an acquisition");
        };
        endpoint.clear();

        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), first)
            .err()
            .unwrap();

        assert!(error.to_string().contains("invalid logical-shard endpoint"));
        assert!(database.exists());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert!(record.owner.is_none());
        assert_eq!(record.lease_id, 0);
        assert!(record.owner_epoch.is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);

        let mut retry = acquire_boot(database.clone(), &[root_id]);
        retry.open = OpenMode::Existing(database);
        let owner = bootstrap_shard(as_control(&control), Arc::clone(&registry), retry).unwrap();

        assert_eq!(owner.lease().owner_epoch.get(), 1);
        assert_eq!(
            owner.meta().current_owner_epoch().unwrap(),
            Some(owner.lease().owner_epoch)
        );
        assert_eq!(owner.serving_record().state, LogicalShardState::Serving);
        owner.release().unwrap();
    }

    #[test]
    fn owned_existing_store_requires_exact_resume_without_advancing_epoch() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let route = first.routes()[0];
        let lease = first.lease().clone();
        assert!(registry.remove(route).unwrap());
        drop(first);
        let before = control.get_logical_shard(&shard()).unwrap().unwrap();

        let mut acquire = acquire_boot(database.clone(), &[root_id]);
        acquire.open = OpenMode::Existing(database.clone());
        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), acquire)
            .err()
            .unwrap();

        assert!(error.to_string().contains("Resume and the exact lease"));
        assert_eq!(control.get_logical_shard(&shard()).unwrap(), Some(before));
        assert_eq!(registry.installed_root_count().unwrap(), 0);

        let resumed = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            resume_boot(database, lease.clone(), &[root_id], 41),
        )
        .unwrap();
        assert_eq!(resumed.lease(), &lease);
        assert_eq!(resumed.lease().owner_epoch.get(), 1);
        resumed.release().unwrap();
    }

    #[test]
    fn failed_fresh_initialize_does_not_consume_owner_epoch_or_install_routes() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        std::fs::create_dir(&database).unwrap();
        std::fs::write(database.join("owner.txt"), b"foreign").unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());

        let error = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database.clone(), &[root_id]),
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("is not empty"));
        assert_eq!(
            std::fs::read(database.join("owner.txt")).unwrap(),
            b"foreign"
        );
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert!(record.owner.is_none());
        assert_eq!(record.lease_id, 0);
        assert!(record.owner_epoch.is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn empty_frontier_successor_is_rejected_before_opening_any_store() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        let boot = ShardBoot {
            shard_id: shard(),
            open: OpenMode::Existing(database),
            lease: LeaseMode::Acquire {
                owner: NodeId::new("node-b").unwrap(),
                endpoint: "127.0.0.1:9020".to_owned(),
                previous_epoch: Some(first_epoch),
            },
            recovery: empty_recovery(),
            roots: vec![root_attach(root_id, 33)],
        };
        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), boot)
            .err()
            .unwrap();

        assert!(error.to_string().contains("local-WAL profile"));
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, Some(first_epoch));
        assert_eq!(record.durable_lsn, 0);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn unverified_requested_recovery_frontier_is_rejected_before_acquire() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut boot = acquire_boot(temporary.path().join("metadata"), &[root_id]);
        boot.recovery.durable_lsn = 1;

        assert!(bootstrap_shard(as_control(&control), registry.clone(), boot).is_err());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert!(record.owner.is_none());
        assert_eq!(record.lease_id, 0);
        assert!(record.owner_epoch.is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn persisted_shared_frontier_is_rejected_before_successor_acquire() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let lease = control
            .acquire_owner(
                &shard(),
                NodeId::new("node-a").unwrap(),
                "127.0.0.1:9010".to_owned(),
            )
            .unwrap();
        let recovery = log_recovery(1);
        control.mark_serving(&lease, recovery.clone()).unwrap();
        control.release_owner(&lease).unwrap();

        let boot = ShardBoot {
            shard_id: shard(),
            open: OpenMode::New(temporary.path().join("metadata")),
            lease: LeaseMode::Acquire {
                owner: NodeId::new("node-b").unwrap(),
                endpoint: "127.0.0.1:9020".to_owned(),
                previous_epoch: Some(lease.owner_epoch),
            },
            recovery,
            roots: vec![root_attach(root_id, 31)],
        };
        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), boot)
            .err()
            .unwrap();

        assert!(error
            .to_string()
            .contains("unverified shared recovery frontier"));
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(record.durable_lsn, 1);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn two_roots_share_one_shard_owner() {
        let temporary = TempDir::new().unwrap();
        let roots = [root(1), root(2)];
        let control = active_control(&roots);
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(temporary.path().join("metadata"), &roots),
        )
        .unwrap();

        assert_eq!(owner.shard_id(), shard());
        assert_eq!(owner.routes().len(), 2);
        assert_eq!(registry.installed_root_count().unwrap(), 2);
        for (root_id, route) in roots.iter().zip(owner.routes()) {
            assert_eq!(route.root_id, RootIdentity::from(*root_id));
            assert_eq!(route.owner_epoch, owner.lease().owner_epoch.get());
            assert_eq!(
                owner
                    .meta()
                    .root_fence(*root_id)
                    .unwrap()
                    .unwrap()
                    .activation_state,
                RootActivationState::Active
            );
        }
        owner.release().unwrap();
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn root_from_another_shard_is_rejected_before_lease_acquire() {
        let temporary = TempDir::new().unwrap();
        let roots = [root(1), root(2)];
        let control = active_control(&roots[..1]);
        control.create_logical_shard(other_shard()).unwrap();
        add_active_root(control.as_ref(), roots[1], other_shard());
        let registry = Arc::new(RootOwnerRegistry::new());
        let boot = acquire_boot(temporary.path().join("metadata"), &roots);

        assert!(bootstrap_shard(as_control(&control), Arc::clone(&registry), boot).is_err());
        assert!(!temporary.path().join("metadata").exists());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, None);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn duplicate_root_attach_is_rejected_before_lease_acquire() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut boot = acquire_boot(temporary.path().join("metadata"), &[root_id]);
        boot.roots.push(root_attach(root_id, 11));

        assert!(bootstrap_shard(as_control(&control), Arc::clone(&registry), boot).is_err());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, None);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn partial_attach_failure_uninstalls_all_routes() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let roots = [root(1), root(2)];
        let control = active_control(&roots);
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(database.clone(), &roots[1..]),
        )
        .unwrap();
        let lease = first.lease().clone();
        assert!(registry.remove(first.routes()[0]).unwrap());
        drop(first);

        let active = control.get_root_placement(&roots[1]).unwrap().unwrap();
        let draining = control
            .compare_and_set_root_placement(
                &active,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(3).unwrap(),
                    lifecycle: RootPlacementLifecycle::Draining,
                    ..active
                },
            )
            .unwrap();
        control
            .compare_and_set_root_placement(
                &draining,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(4).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..draining
                },
            )
            .unwrap();

        let error = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            resume_boot(database, lease.clone(), &roots, 21),
        )
        .err()
        .unwrap();
        assert!(matches!(error, ServerError::InvalidBootstrap(_)));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        control.release_owner(&lease).unwrap();
    }

    #[test]
    fn renewal_loss_uninstalls_every_shard_route() {
        let temporary = TempDir::new().unwrap();
        let roots = [root(1), root(2)];
        let control = active_control(&roots);
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(temporary.path().join("metadata"), &roots),
        )
        .unwrap();
        let routes = owner.routes().to_vec();
        let lease = owner.lease().clone();
        owner.renew_or_uninstall().unwrap();
        control.release_owner(&lease).unwrap();

        assert!(owner.renew_or_uninstall().is_err());
        for route in routes {
            assert!(!registry.contains_exact(route).unwrap());
        }
    }

    #[test]
    fn release_removes_every_route_and_releases_the_shard_once() {
        let temporary = TempDir::new().unwrap();
        let roots = [root(1), root(2)];
        let control = active_control(&roots);
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(temporary.path().join("metadata"), &roots),
        )
        .unwrap();
        let routes = owner.routes().to_vec();

        let released = owner.release().unwrap();
        assert_eq!(released.state, LogicalShardState::Unassigned);
        assert_eq!(released.lease_id, 0);
        for route in routes {
            assert!(!registry.contains_exact(route).unwrap());
        }
    }

    #[test]
    fn supervised_runtime_marks_owner_loss_before_further_accepts() {
        let temporary = TempDir::new().unwrap();
        let roots = [root(1), root(2)];
        let control = active_control(&roots);
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            acquire_boot(temporary.path().join("metadata"), &roots),
        )
        .unwrap();
        let routes = owner.routes().to_vec();
        let lease = owner.lease().clone();
        let server = WorkspaceServer::new(options(), Arc::clone(&registry), vec![owner]).unwrap();
        control.release_owner(&lease).unwrap();

        assert!(server.renew_ownership().is_err());
        assert!(server.owner_loss_signal().is_lost());
        for route in routes {
            assert!(!registry.contains_exact(route).unwrap());
        }
    }
}
