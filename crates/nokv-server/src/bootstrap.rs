/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::PathBuf;
use std::sync::Arc;

use nokv_control::{
    ControlStore, LogicalShardLease, LogicalShardRecord, NodeId, OwnerEpoch, RecoveryPublication,
    RootId, RootPlacement, RootPlacementLifecycle,
};
use nokv_meta::workspace as meta;
use nokv_protocol::{LogicalShardIdentity, RootIdentity, RootRoute};
use nokv_types::{CommandDigest, RequestId, RootActivationState, SHA256_BYTES};

use crate::{
    MetadataWorkspaceRequestExecutor, RootOwnerRegistry, ServerError, WorkspaceRequestExecutor,
};

/// Explicit metadata-store opening mode. Startup never guesses from path
/// existence and never falls back between create and reopen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataStoreOpen {
    Create(PathBuf),
    Reopen(PathBuf),
}

/// Exact control-plane admission used by one root-owner bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerAdmission {
    Acquire {
        owner: NodeId,
        endpoint: String,
        expected_previous_epoch: Option<OwnerEpoch>,
    },
    Resume {
        lease: LogicalShardLease,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootOwnerBootstrapRequest {
    pub root_id: RootId,
    pub metadata: MetadataStoreOpen,
    pub admission: OwnerAdmission,
    pub install_request_id: RequestId,
    pub activate_request_id: RequestId,
    pub recovery: RecoveryPublication,
}

/// Control-backed ownership token retained by the serving runtime.
///
/// A failed renewal immediately removes the exact local route before exposing
/// the lease failure to the caller. The runtime can then stop accepting work
/// without relying on a stale in-memory route.
#[derive(Clone)]
pub struct ControlBackedRootOwner {
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    route: RootRoute,
    lease: LogicalShardLease,
}

impl ControlBackedRootOwner {
    pub fn route(&self) -> RootRoute {
        self.route
    }

    pub fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub fn is_for_registry(&self, registry: &Arc<RootOwnerRegistry>) -> bool {
        Arc::ptr_eq(&self.registry, registry)
    }

    /// Renew the exact owner session. Any failure is treated as lease loss and
    /// uninstalls the route before returning.
    pub fn renew_or_uninstall(&self) -> Result<LogicalShardRecord, ServerError> {
        match self.control.renew_owner(&self.lease) {
            Ok(record) => Ok(record),
            Err(error) => {
                let primary = ServerError::Control(error);
                match self.registry.remove(self.route) {
                    Ok(_) => Err(primary),
                    Err(rollback) => Err(ServerError::BootstrapRollback {
                        primary: primary.to_string(),
                        rollback: format!("remove lease-lost registry route: {rollback}"),
                    }),
                }
            }
        }
    }

    /// Stop local admission and release the exact control-plane owner session.
    pub fn release(&self) -> Result<LogicalShardRecord, ServerError> {
        let registry_result = self.registry.remove(self.route);
        let release_result = self.control.release_owner(&self.lease);
        match (registry_result, release_result) {
            (Ok(_), Ok(record)) => Ok(record),
            (Err(registry), Ok(_)) => Err(registry),
            (Ok(_), Err(control)) => Err(ServerError::Control(control)),
            (Err(registry), Err(control)) => Err(ServerError::BootstrapRollback {
                primary: format!("release control owner: {control}"),
                rollback: format!("remove registry route: {registry}"),
            }),
        }
    }
}

/// Fully activated owner state returned to the server runtime.
pub struct BootstrappedRootOwner {
    pub route: RootRoute,
    pub lease: LogicalShardLease,
    pub serving_record: LogicalShardRecord,
    pub store: Arc<meta::AgentMetadataStore>,
    pub executor: Arc<MetadataWorkspaceRequestExecutor>,
    pub ownership: ControlBackedRootOwner,
}

/// Acquire or resume one exact control-plane owner, open its metadata store,
/// activate the matching root fence, install the registry route, and only then
/// publish the owner as serving.
pub fn bootstrap_root_owner(
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    request: RootOwnerBootstrapRequest,
) -> Result<BootstrappedRootOwner, ServerError> {
    if request.install_request_id == request.activate_request_id {
        return Err(ServerError::InvalidBootstrap(
            "install and activate request ids must differ".to_owned(),
        ));
    }
    let placement = control
        .get_root_placement(&request.root_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("root placement does not exist".to_owned()))?;
    validate_serving_placement(&placement)?;
    reject_unrecovered_shared_frontier(control.as_ref(), &placement, &request.recovery)?;
    validate_open_admission(&request.metadata, &request.admission)?;

    let (lease, acquired) = admit_owner(control.as_ref(), &placement, &request.admission)?;
    let route = root_route(&placement, &lease);

    let store = match open_store(request.metadata, placement.logical_shard_id) {
        Ok(store) => Arc::new(store),
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::Metadata(error),
                control.as_ref(),
                &registry,
                route,
                &lease,
                false,
                acquired,
            ));
        }
    };
    if store.logical_shard_id() != placement.logical_shard_id {
        return Err(rollback_bootstrap(
            ServerError::InvalidBootstrap(
                "metadata store logical shard differs from root placement".to_owned(),
            ),
            control.as_ref(),
            &registry,
            route,
            &lease,
            false,
            acquired,
        ));
    }
    if let Err(error) = activate_root_fence(
        &store,
        &placement,
        &lease,
        request.install_request_id,
        request.activate_request_id,
    ) {
        return Err(rollback_bootstrap(
            error,
            control.as_ref(),
            &registry,
            route,
            &lease,
            false,
            acquired,
        ));
    }

    let executor = Arc::new(MetadataWorkspaceRequestExecutor::new(Arc::clone(&store)));
    let installed_executor: Arc<dyn WorkspaceRequestExecutor> = executor.clone();
    if let Err(error) = registry.install(route, installed_executor) {
        return Err(rollback_bootstrap(
            error,
            control.as_ref(),
            &registry,
            route,
            &lease,
            false,
            acquired,
        ));
    }
    if let Err(error) = control.renew_owner(&lease) {
        return Err(rollback_bootstrap(
            ServerError::Control(error),
            control.as_ref(),
            &registry,
            route,
            &lease,
            true,
            acquired,
        ));
    }
    let serving_record = match control.mark_serving(&lease, request.recovery) {
        Ok(record) => record,
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::Control(error),
                control.as_ref(),
                &registry,
                route,
                &lease,
                true,
                acquired,
            ));
        }
    };
    let ownership = ControlBackedRootOwner {
        control,
        registry,
        route,
        lease: lease.clone(),
    };
    Ok(BootstrappedRootOwner {
        route,
        lease,
        serving_record,
        store,
        executor,
        ownership,
    })
}

fn validate_open_admission(
    metadata: &MetadataStoreOpen,
    admission: &OwnerAdmission,
) -> Result<(), ServerError> {
    match (metadata, admission) {
        (
            MetadataStoreOpen::Create(_),
            OwnerAdmission::Acquire {
                expected_previous_epoch: None,
                ..
            },
        )
        | (MetadataStoreOpen::Reopen(_), OwnerAdmission::Resume { .. }) => Ok(()),
        (
            _,
            OwnerAdmission::Acquire {
                expected_previous_epoch: Some(previous),
                ..
            },
        ) => Err(ServerError::InvalidBootstrap(format!(
            "successor acquisition after owner epoch {previous} is unavailable in the local-WAL profile; verified checkpoint/log recovery must complete before a new owner may serve"
        ))),
        (MetadataStoreOpen::Reopen(_), OwnerAdmission::Acquire { .. }) => {
            Err(ServerError::InvalidBootstrap(
                "a first owner must explicitly create a fresh metadata store".to_owned(),
            ))
        }
        (MetadataStoreOpen::Create(_), OwnerAdmission::Resume { .. }) => {
            Err(ServerError::InvalidBootstrap(
                "an exact owner resume must reopen its existing metadata store".to_owned(),
            ))
        }
    }
}

fn reject_unrecovered_shared_frontier(
    control: &dyn ControlStore,
    placement: &RootPlacement,
    requested: &RecoveryPublication,
) -> Result<(), ServerError> {
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("logical shard does not exist".to_owned()))?;
    let persisted_nonempty =
        shard.durable_lsn != 0 || shard.checkpoint.is_some() || shard.log.is_some();
    let requested_nonempty =
        requested.durable_lsn != 0 || requested.checkpoint.is_some() || requested.log.is_some();
    if persisted_nonempty || requested_nonempty {
        return Err(ServerError::InvalidBootstrap(format!(
            "logical shard {:?} has an unverified shared recovery frontier (persisted LSN {}, requested LSN {}); checkpoint/log install, replay, and fsck are not implemented, so Serving is refused",
            placement.logical_shard_id, shard.durable_lsn, requested.durable_lsn
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
    placement: &RootPlacement,
    admission: &OwnerAdmission,
) -> Result<(LogicalShardLease, bool), ServerError> {
    match admission {
        OwnerAdmission::Acquire {
            owner,
            endpoint,
            expected_previous_epoch,
        } => {
            let lease = match expected_previous_epoch {
                None => control.acquire_owner(
                    &placement.logical_shard_id,
                    owner.clone(),
                    endpoint.clone(),
                )?,
                Some(expected) => control.acquire_successor(
                    &placement.logical_shard_id,
                    *expected,
                    owner.clone(),
                    endpoint.clone(),
                )?,
            };
            Ok((lease, true))
        }
        OwnerAdmission::Resume { lease } => {
            if lease.logical_shard_id != placement.logical_shard_id {
                return Err(ServerError::InvalidBootstrap(
                    "resumed lease belongs to another logical shard".to_owned(),
                ));
            }
            control.renew_owner(lease)?;
            Ok((lease.clone(), false))
        }
    }
}

fn open_store(
    mode: MetadataStoreOpen,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<meta::AgentMetadataStore, meta::AgentMetadataError> {
    match mode {
        MetadataStoreOpen::Create(path) => {
            meta::AgentMetadataStore::create_file(path, logical_shard_id)
        }
        MetadataStoreOpen::Reopen(path) => {
            meta::AgentMetadataStore::reopen_file(path, logical_shard_id)
        }
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

fn activate_root_fence(
    store: &meta::AgentMetadataStore,
    placement: &RootPlacement,
    lease: &LogicalShardLease,
    install_request_id: RequestId,
    activate_request_id: RequestId,
) -> Result<(), ServerError> {
    let current_owner = store.current_owner_epoch()?;
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
        store.advance_owner_epoch(expected_previous, lease.owner_epoch)?;
    }

    match store.root_fence(placement.root_id)? {
        None => execute_fence_command(
            store,
            placement,
            lease,
            install_request_id,
            meta::RootFenceAction::Install,
            b"nokv.root-fence.install.v1".to_vec(),
        )?,
        Some(fence) => validate_existing_fence(placement, fence)?,
    }

    let fence = store.root_fence(placement.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap("root fence install produced no durable fence".to_owned())
    })?;
    validate_existing_fence(placement, fence)?;
    if fence.activation_state == RootActivationState::Installing {
        execute_fence_command(
            store,
            placement,
            lease,
            activate_request_id,
            meta::RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            b"nokv.root-fence.activate.v1".to_vec(),
        )?;
    }

    let active = store
        .root_fence(placement.root_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("active root fence disappeared".to_owned()))?;
    validate_existing_fence(placement, active)?;
    if active.activation_state != RootActivationState::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "root fence is {:?}, expected Active",
            active.activation_state
        )));
    }
    Ok(())
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
    store: &meta::AgentMetadataStore,
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
        read_version: store.current_read_version()?,
        root_fence_action: action,
        predicates: Vec::new(),
        mutations: Vec::new(),
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result,
    }
    .seal();
    store.execute(&command)?;
    Ok(())
}

fn predecessor(epoch: OwnerEpoch) -> Option<OwnerEpoch> {
    let previous = epoch.get() - 1;
    (previous != 0).then(|| OwnerEpoch::new(previous).expect("nonzero predecessor is valid"))
}

fn display_epoch(epoch: Option<OwnerEpoch>) -> String {
    epoch.map_or_else(|| "epoch-zero".to_owned(), |epoch| epoch.get().to_string())
}

#[allow(clippy::too_many_arguments)]
fn rollback_bootstrap(
    primary: ServerError,
    control: &dyn ControlStore,
    registry: &RootOwnerRegistry,
    route: RootRoute,
    lease: &LogicalShardLease,
    registry_installed: bool,
    release_acquired_lease: bool,
) -> ServerError {
    let mut rollback = Vec::new();
    if registry_installed {
        if let Err(error) = registry.remove(route) {
            rollback.push(format!("remove registry route: {error}"));
        }
    }
    if release_acquired_lease {
        if let Err(error) = control.release_owner(lease) {
            rollback.push(format!("release exact owner lease: {error}"));
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

    fn root() -> RootId {
        RootId::from_bytes([1; nokv_types::FIXED_ID_BYTES])
    }

    fn shard() -> nokv_types::LogicalShardId {
        nokv_types::LogicalShardId::from_bytes([2; nokv_types::FIXED_ID_BYTES])
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

    fn active_control() -> Arc<InMemoryControlStore> {
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id: root(),
                logical_shard_id: shard(),
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
        control
    }

    fn acquire_request(path: PathBuf) -> RootOwnerBootstrapRequest {
        RootOwnerBootstrapRequest {
            root_id: root(),
            metadata: MetadataStoreOpen::Create(path),
            admission: OwnerAdmission::Acquire {
                owner: NodeId::new("node-a").unwrap(),
                endpoint: "127.0.0.1:9010".to_owned(),
                expected_previous_epoch: None,
            },
            install_request_id: request_id(3),
            activate_request_id: request_id(4),
            recovery: empty_recovery(),
        }
    }

    fn as_control(control: &Arc<InMemoryControlStore>) -> Arc<dyn ControlStore> {
        control.clone()
    }

    #[test]
    fn fresh_bootstrap_activates_fence_installs_route_and_marks_serving() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();

        assert_eq!(owner.route.root_id, RootIdentity::from(root()));
        assert_eq!(
            owner.route.logical_shard_id,
            LogicalShardIdentity::from(shard())
        );
        assert_eq!(owner.route.placement_generation, 2);
        assert_eq!(owner.route.owner_epoch, 1);
        assert!(registry.contains_exact(owner.route).unwrap());
        assert_eq!(
            owner
                .store
                .root_fence(root())
                .unwrap()
                .unwrap()
                .activation_state,
            RootActivationState::Active
        );
        assert_eq!(
            owner.store.current_owner_epoch().unwrap(),
            Some(owner.lease.owner_epoch)
        );
        assert_eq!(owner.serving_record.state, LogicalShardState::Serving);

        let server = WorkspaceServer::new(
            ServerOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                lease_renew_interval: Duration::from_millis(10),
            },
            registry,
            vec![owner.ownership],
        )
        .unwrap();
        server.renew_ownership().unwrap();
        assert_eq!(server.health().unwrap().installed_roots, 1);
    }

    #[test]
    fn exact_current_owner_reopens_without_reinstalling_the_active_fence() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .unwrap();
        let route = first.route;
        let lease = first.lease.clone();
        assert!(registry.remove(route).unwrap());
        drop(first);

        let reopened = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                metadata: MetadataStoreOpen::Reopen(database),
                admission: OwnerAdmission::Resume {
                    lease: lease.clone(),
                },
                install_request_id: request_id(5),
                activate_request_id: request_id(6),
                recovery: empty_recovery(),
            },
        )
        .unwrap();

        assert_eq!(reopened.route, route);
        assert_eq!(reopened.lease, lease);
        assert_eq!(
            reopened
                .store
                .root_fence(root())
                .unwrap()
                .unwrap()
                .activation_state,
            RootActivationState::Active
        );
    }

    #[test]
    fn exact_owner_resume_cannot_create_a_replacement_store() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database),
        )
        .unwrap();
        let route = first.route;
        let lease = first.lease.clone();
        assert!(registry.remove(route).unwrap());

        let error = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                metadata: MetadataStoreOpen::Create(temporary.path().join("replacement")),
                admission: OwnerAdmission::Resume {
                    lease: lease.clone(),
                },
                install_request_id: request_id(35),
                activate_request_id: request_id(36),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("must reopen"));
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
    }

    #[test]
    fn stale_store_placement_is_rejected_and_never_installed() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .unwrap();
        let lease = first.lease.clone();
        registry.remove(first.route).unwrap();
        drop(first);

        let active = control.get_root_placement(&root()).unwrap().unwrap();
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

        let error = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                metadata: MetadataStoreOpen::Reopen(database),
                admission: OwnerAdmission::Resume { lease },
                install_request_id: request_id(7),
                activate_request_id: request_id(8),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();
        assert!(matches!(error, ServerError::InvalidBootstrap(_)));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn first_owner_reopen_is_rejected_before_acquiring_a_lease() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut request = acquire_request(temporary.path().join("missing"));
        request.metadata = MetadataStoreOpen::Reopen(temporary.path().join("missing"));

        assert!(bootstrap_root_owner(as_control(&control), registry.clone(), request).is_err());
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
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .unwrap();
        let first_epoch = first.lease.owner_epoch;
        first.ownership.release().unwrap();

        let error = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                metadata: MetadataStoreOpen::Reopen(database),
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: Some(first_epoch),
                },
                install_request_id: request_id(33),
                activate_request_id: request_id(34),
                recovery: empty_recovery(),
            },
        )
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
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut request = acquire_request(temporary.path().join("metadata"));
        request.recovery.durable_lsn = 1;

        assert!(bootstrap_root_owner(as_control(&control), registry.clone(), request).is_err());
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
        let control = active_control();
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

        let error = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                metadata: MetadataStoreOpen::Create(temporary.path().join("metadata")),
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: Some(lease.owner_epoch),
                },
                install_request_id: request_id(31),
                activate_request_id: request_id(32),
                recovery,
            },
        )
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
    fn renewal_loss_uninstalls_the_exact_route() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();
        owner.ownership.renew_or_uninstall().unwrap();
        control.release_owner(&owner.lease).unwrap();

        assert!(owner.ownership.renew_or_uninstall().is_err());
        assert!(!registry.contains_exact(owner.route).unwrap());
    }

    #[test]
    fn supervised_runtime_marks_owner_loss_before_further_accepts() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();
        let route = owner.route;
        let lease = owner.lease.clone();
        let server = WorkspaceServer::new(
            ServerOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                lease_renew_interval: Duration::from_millis(10),
            },
            Arc::clone(&registry),
            vec![owner.ownership],
        )
        .unwrap();
        control.release_owner(&lease).unwrap();

        assert!(server.renew_ownership().is_err());
        assert!(server.owner_loss_signal().is_lost());
        assert!(!registry.contains_exact(route).unwrap());
    }
}
