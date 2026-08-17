/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nokv_control::{
    ControlStore, LogicalShardLease, LogicalShardRecord, LogicalShardState, NodeId, OwnerEpoch,
    RecoveryPublication, RootId, RootPlacement, RootPlacementLifecycle,
};
use nokv_meta::workspace as meta;
use nokv_meta_holt::{HoltOptions, HoltStore, TreeBinding};
use nokv_meta_store::TxnStore;
use nokv_object::ArtifactObjectStore;
use nokv_protocol::{LogicalShardIdentity, ObjectNamespaceIdentity, RootIdentity, RootRoute};
use nokv_types::{CommandDigest, ObjectNamespaceId, RequestId, RootActivationState, SHA256_BYTES};

use crate::recovery_installer::{
    cleanup_pending_recovery_upload, install_durable_recovery_log, install_pending_recovery_upload,
    validate_local_durable_recovery_prefix, validate_local_recovery_prefix,
    validate_recovery_control_references, PendingRecoveryInstallOutcome,
};
use crate::{
    MetadataWorkspaceRequestExecutor, RecoveryPublisher, RecoveryPublishingExecutor,
    RootOwnerRegistry, ServerError, WorkspaceRequestExecutor,
};

/// Explicit metadata-store opening mode. Startup never falls back between new,
/// existing, and shared-log recovery. `RecoverLog` alone may distinguish a
/// missing/empty install target from its own exact resumable target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenMode {
    New(PathBuf),
    Existing(PathBuf),
    /// Create or resume a fresh local authority from the exact shared-log
    /// receipts stored in Control.
    RecoverLog(PathBuf),
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
    /// Immutable object namespace loaded from the root's control binding.
    pub object_namespace_id: ObjectNamespaceId,
    /// Idempotency key for the durable fence installation.
    pub install_id: RequestId,
    /// Idempotency key for upgrading a legacy unbound Holt fence.
    pub bind_object_namespace_id: RequestId,
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
    recovery: Arc<RecoveryPublisher>,
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

    pub fn recovery_publisher(&self) -> &Arc<RecoveryPublisher> {
        &self.recovery
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
                let cleanup = self
                    .registry
                    .fail_closed_shard(LogicalShardIdentity::from(self.shard_id()))
                    .err();
                match cleanup {
                    None => Err(primary),
                    Some(cleanup) => Err(ServerError::BootstrapRollback {
                        primary: primary.to_string(),
                        rollback: format!("fail-close lease-lost logical shard: {}", cleanup),
                    }),
                }
            }
        }
    }

    /// Stop admission for every attached root and release the shard lease once.
    pub fn release(self) -> Result<LogicalShardRecord, ServerError> {
        let cleanup = self
            .registry
            .fail_closed_shard(LogicalShardIdentity::from(self.shard_id()))
            .err();
        let publication = self.recovery.publish_current().map_err(ServerError::from);
        if let Err(publication) = publication {
            return match cleanup {
                None => Err(publication),
                Some(cleanup) => Err(ServerError::BootstrapRollback {
                    primary: publication.to_string(),
                    rollback: cleanup.to_string(),
                }),
            };
        }
        match (self.control.release_owner(&self.lease), cleanup) {
            (Ok(record), None) => Ok(record),
            (Ok(_), Some(cleanup)) => Err(ServerError::BootstrapRollback {
                primary: "release logical-shard owner succeeded".to_owned(),
                rollback: cleanup.to_string(),
            }),
            (Err(control), None) => Err(ServerError::Control(control)),
            (Err(control), Some(cleanup)) => Err(ServerError::BootstrapRollback {
                primary: format!("release logical-shard owner: {control}"),
                rollback: cleanup.to_string(),
            }),
        }
    }
}

/// Acquire or resume one logical-shard lease, open one metadata shard, attach
/// all initial roots, and publish the shard as serving.
pub fn bootstrap_shard(
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    recovery_objects: Arc<dyn ArtifactObjectStore>,
    boot: ShardBoot,
) -> Result<ShardOwner, ServerError> {
    validate_boot(&boot)?;
    let expected_namespace = boot
        .roots
        .first()
        .expect("validated bootstrap has at least one root")
        .object_namespace_id;
    if boot
        .roots
        .iter()
        .any(|root| root.object_namespace_id != expected_namespace)
    {
        return Err(ServerError::InvalidBootstrap(
            "one logical-shard owner cannot attach roots from different object namespaces"
                .to_owned(),
        ));
    }
    if recovery_objects.object_namespace() != Some(expected_namespace) {
        return Err(ServerError::InvalidBootstrap(
            "recovery object handle is not bound to the shard object namespace".to_owned(),
        ));
    }
    let placements = load_placements(control.as_ref(), boot.shard_id, &boot.roots)?;
    validate_open_lease(&boot.open, &boot.lease)?;
    let control_record =
        load_control_record(control.as_ref(), boot.shard_id, &boot.open, &boot.recovery)?;

    // Every new acquisition opens and validates the exclusive local authority
    // before it mutates the control epoch. This prevents a bad/missing/stale
    // local store from consuming an epoch that the store cannot install.
    // Exact live-session resumes retain their admission-before-open order.
    let prepared_meta = prepare_acquiring_meta(
        &boot.open,
        &boot.lease,
        boot.shard_id,
        &control_record,
        expected_namespace,
        recovery_objects.as_ref(),
    )?;
    let prepared_first_owner_path = match &boot.lease {
        LeaseMode::Acquire {
            previous_epoch: None,
            ..
        } if prepared_meta.is_some() => Some(match &boot.open {
            OpenMode::New(path) | OpenMode::Existing(path) | OpenMode::RecoverLog(path) => {
                path.clone()
            }
        }),
        _ => None,
    };

    let admission = admit_owner(
        control.as_ref(),
        boot.shard_id,
        &boot.lease,
        &control_record,
    );
    let (lease, acquired) = match (admission, prepared_first_owner_path) {
        (Ok(admission), _) => admission,
        (Err(ServerError::Control(source)), Some(path)) => {
            return Err(ServerError::PreparedOwnerAdmission { path, source });
        }
        (Err(error), _) => return Err(error),
    };
    let mut routes = Vec::with_capacity(placements.len());

    let meta = match prepared_meta {
        Some(meta) => meta,
        None => {
            let meta = match open_meta(boot.open.clone(), boot.shard_id) {
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
    let _reconciled_control = match reconcile_acquired_recovery(
        control.as_ref(),
        &lease,
        &boot.open,
        expected_namespace,
        recovery_objects.as_ref(),
        &meta,
    ) {
        Ok(record) => record,
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

    let recovery = match RecoveryPublisher::new(
        Arc::clone(&control),
        lease.clone(),
        Arc::clone(&meta),
        recovery_objects,
    ) {
        Ok(recovery) => Arc::new(recovery),
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::from(error),
                control.as_ref(),
                &registry,
                &routes,
                &lease,
                acquired,
            ));
        }
    };
    if let Err(error) = recovery.publish_current() {
        return Err(rollback_bootstrap(
            ServerError::from(error),
            control.as_ref(),
            &registry,
            &routes,
            &lease,
            acquired,
        ));
    }
    let metadata_executor: Arc<dyn WorkspaceRequestExecutor> =
        Arc::new(MetadataWorkspaceRequestExecutor::new(Arc::clone(&meta)));
    let executor: Arc<dyn WorkspaceRequestExecutor> = Arc::new(RecoveryPublishingExecutor::new(
        metadata_executor,
        Arc::clone(&recovery),
    ));
    for (placement, root) in placements.iter().zip(&boot.roots) {
        match attach_root(
            &meta, &registry, &executor, &recovery, placement, &lease, root,
        ) {
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
    let published = match recovery.publish_current() {
        Ok(record) => record,
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::from(error),
                control.as_ref(),
                &registry,
                &routes,
                &lease,
                acquired,
            ));
        }
    };
    let serving = match control.mark_serving(
        &lease,
        RecoveryPublication {
            checkpoint: None,
            log: None,
            durable_lsn: published.durable_lsn,
        },
    ) {
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
        recovery,
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
        if root.install_id == root.bind_object_namespace_id
            || root.install_id == root.activate_id
            || root.bind_object_namespace_id == root.activate_id
        {
            return Err(ServerError::InvalidBootstrap(format!(
                "root {:?} install, namespace-bind, and activate request ids must differ",
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
            let binding = control
                .get_root_object_namespace_binding(&root.root_id)?
                .ok_or_else(|| {
                    ServerError::InvalidBootstrap(format!(
                        "root {:?} object namespace binding does not exist",
                        root.root_id
                    ))
                })?;
            if binding.object_namespace_id != root.object_namespace_id {
                return Err(ServerError::InvalidBootstrap(format!(
                    "root {:?} object namespace differs from its control binding",
                    root.root_id
                )));
            }
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
        | (
            OpenMode::Existing(_),
            LeaseMode::Acquire {
                previous_epoch: Some(_),
                ..
            },
        )
        | (OpenMode::Existing(_), LeaseMode::Resume { .. }) => Ok(()),
        (
            OpenMode::RecoverLog(_),
            LeaseMode::Acquire {
                previous_epoch: Some(_),
                ..
            },
        ) => Ok(()),
        (
            OpenMode::New(_),
            LeaseMode::Acquire {
                previous_epoch: Some(previous),
                ..
            },
        ) => Err(ServerError::InvalidBootstrap(format!(
            "successor acquisition after owner epoch {previous} must reopen the existing local metadata authority"
        ))),
        (OpenMode::New(_), LeaseMode::Resume { .. }) => {
            Err(ServerError::InvalidBootstrap(
                "an exact owner resume must open its existing metadata store".to_owned(),
            ))
        }
        (
            OpenMode::RecoverLog(_),
            LeaseMode::Acquire {
                previous_epoch: None,
                ..
            },
        ) => Err(ServerError::InvalidBootstrap(
            "shared-log recovery requires a previously owned logical shard".to_owned(),
        )),
        (OpenMode::RecoverLog(_), LeaseMode::Resume { .. }) => {
            Err(ServerError::InvalidBootstrap(
                "an exact owner resume must reopen its existing metadata store".to_owned(),
            ))
        }
    }
}

fn prepare_acquiring_meta(
    open: &OpenMode,
    lease: &LeaseMode,
    logical_shard_id: nokv_types::LogicalShardId,
    control_record: &LogicalShardRecord,
    object_namespace_id: ObjectNamespaceId,
    recovery_objects: &dyn ArtifactObjectStore,
) -> Result<Option<Arc<meta::MetaShard>>, ServerError> {
    let LeaseMode::Acquire { previous_epoch, .. } = lease else {
        return Ok(None);
    };

    let meta = open_meta(open.clone(), logical_shard_id)?;
    validate_meta_shard(&meta, logical_shard_id)?;
    match open {
        OpenMode::RecoverLog(_) => {
            validate_recovery_control_references(control_record, object_namespace_id, &meta)?;
            install_durable_recovery_log(control_record, recovery_objects, &meta)?;
            validate_local_durable_recovery_prefix(control_record, object_namespace_id, &meta)?;
        }
        OpenMode::New(_) | OpenMode::Existing(_) => {
            validate_local_recovery_prefix(control_record, object_namespace_id, &meta)?;
        }
    }
    let local_epoch = meta.current_owner_epoch()?;
    match previous_epoch {
        None => {
            if let Some(owner_epoch) = local_epoch {
                return Err(ServerError::InvalidBootstrap(format!(
                    "metadata shard is already fenced at owner epoch {owner_epoch}; reopen it only with Resume and the exact lease or acquire from that durable epoch"
                )));
            }
        }
        Some(expected) => {
            if control_record.owner_epoch != Some(*expected) {
                return Err(ServerError::InvalidBootstrap(format!(
                    "control record changed while preparing local recovery: requested epoch {expected}, actual {}",
                    display_epoch(control_record.owner_epoch)
                )));
            }
            let valid_local_epoch = if control_record.state == LogicalShardState::Recovering {
                local_epoch == Some(*expected) || local_epoch == predecessor(*expected)
            } else {
                local_epoch == Some(*expected)
            };
            if !valid_local_epoch {
                let requirement = if control_record.state == LogicalShardState::Recovering {
                    format!(
                        "predecessor {} or exact recovery epoch {expected}",
                        display_epoch(predecessor(*expected))
                    )
                } else {
                    format!("exact durable epoch {expected}")
                };
                return Err(ServerError::InvalidBootstrap(format!(
                    "local metadata authority is fenced at {}, but control state {:?} requires {requirement}",
                    display_epoch(local_epoch),
                    control_record.state
                )));
            }
        }
    }
    Ok(Some(meta))
}

fn load_control_record(
    control: &dyn ControlStore,
    shard_id: nokv_types::LogicalShardId,
    open: &OpenMode,
    requested: &RecoveryPublication,
) -> Result<LogicalShardRecord, ServerError> {
    let shard = control
        .get_logical_shard(&shard_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("logical shard does not exist".to_owned()))?;
    let persisted = recovery_publication(&shard);
    if &persisted != requested {
        return Err(ServerError::InvalidBootstrap(format!(
            "logical shard {:?} recovery commitment differs from control (persisted LSN {}, requested LSN {})",
            shard_id, shard.durable_lsn, requested.durable_lsn,
        )));
    }
    let persisted_nonempty = shard.durable_lsn != 0
        || shard.checkpoint.is_some()
        || shard.log.is_some()
        || shard.pending_recovery_upload.is_some();
    if matches!(open, OpenMode::New(_)) && persisted_nonempty {
        return Err(ServerError::InvalidBootstrap(format!(
            "new metadata store cannot adopt logical shard {:?} recovery frontier at LSN {}; reopen or install a verified checkpoint instead",
            shard_id, shard.durable_lsn,
        )));
    }
    Ok(shard)
}

fn recovery_publication(record: &LogicalShardRecord) -> RecoveryPublication {
    RecoveryPublication {
        checkpoint: record.checkpoint.clone(),
        log: record.log.clone(),
        durable_lsn: record.durable_lsn,
    }
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
    control_record: &LogicalShardRecord,
) -> Result<(LogicalShardLease, bool), ServerError> {
    match mode {
        LeaseMode::Acquire {
            owner,
            endpoint,
            previous_epoch,
        } => {
            let lease = match previous_epoch {
                None => control.acquire_owner(&shard_id, owner.clone(), endpoint.clone())?,
                Some(expected) if control_record.state == LogicalShardState::Recovering => control
                    .reacquire_recovery(&shard_id, *expected, owner.clone(), endpoint.clone())?,
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

fn reconcile_acquired_recovery(
    control: &dyn ControlStore,
    lease: &LogicalShardLease,
    open: &OpenMode,
    object_namespace_id: ObjectNamespaceId,
    recovery_objects: &dyn ArtifactObjectStore,
    meta: &meta::MetaShard,
) -> Result<LogicalShardRecord, ServerError> {
    let mut record = control.renew_owner(lease)?;
    validate_control_record_lease(&record, lease)?;

    if matches!(open, OpenMode::RecoverLog(_)) {
        validate_recovery_control_references(&record, object_namespace_id, meta)?;
        install_durable_recovery_log(&record, recovery_objects, meta)?;
        validate_local_durable_recovery_prefix(&record, object_namespace_id, meta)?;
        if let PendingRecoveryInstallOutcome::CleanupRequired { .. } =
            install_pending_recovery_upload(&record, recovery_objects, meta)?
        {
            let expected_intent = record
                .pending_recovery_upload
                .as_ref()
                .expect("typed pending abort requires an exact Control intent");
            cleanup_pending_recovery_upload(&record, recovery_objects, meta)?;
            record = control.abort_recovery_upload(lease, expected_intent)?;
            validate_control_record_lease(&record, lease)?;
            validate_recovery_control_references(&record, object_namespace_id, meta)?;
            install_durable_recovery_log(&record, recovery_objects, meta)?;
        }
    }

    validate_local_recovery_prefix(&record, object_namespace_id, meta)?;
    Ok(record)
}

fn validate_control_record_lease(
    record: &LogicalShardRecord,
    lease: &LogicalShardLease,
) -> Result<(), ServerError> {
    if record.logical_shard_id != lease.logical_shard_id
        || record.owner.as_ref() != Some(&lease.owner)
        || record.owner_epoch != Some(lease.owner_epoch)
        || record.lease_id != lease.lease_id
    {
        return Err(ServerError::InvalidBootstrap(
            "post-admission Control record does not match the exact acquired lease".to_owned(),
        ));
    }
    Ok(())
}

fn open_meta(
    mode: OpenMode,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<Arc<meta::MetaShard>, ServerError> {
    match mode {
        OpenMode::New(path) => initialize_meta(path, logical_shard_id),
        OpenMode::Existing(path) => open_existing_meta(path, logical_shard_id),
        OpenMode::RecoverLog(path) => {
            if recovery_path_is_missing_or_empty(&path)? {
                initialize_meta(path, logical_shard_id)
            } else {
                open_existing_meta(path, logical_shard_id)
            }
        }
    }
}

fn metadata_catalog() -> impl Iterator<Item = TreeBinding> {
    meta::keyspaces()
        .iter()
        .map(|definition| TreeBinding::new(definition.id, definition.name))
}

fn initialize_meta(
    path: PathBuf,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<Arc<meta::MetaShard>, ServerError> {
    let holt = HoltStore::initialize(HoltOptions::file(
        path,
        metadata_catalog(),
        meta::store_limits(),
    ))?;
    let store: Arc<dyn TxnStore> = Arc::new(holt);
    Ok(Arc::new(meta::MetaShard::initialize(
        store,
        logical_shard_id,
    )?))
}

fn open_existing_meta(
    path: PathBuf,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<Arc<meta::MetaShard>, ServerError> {
    let holt = HoltStore::open(HoltOptions::file(
        path,
        metadata_catalog(),
        meta::store_limits(),
    ))?;
    let store: Arc<dyn TxnStore> = Arc::new(holt);
    Ok(Arc::new(meta::MetaShard::open(store, logical_shard_id)?))
}

fn recovery_path_is_missing_or_empty(path: &Path) -> Result<bool, ServerError> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => entries
            .next()
            .transpose()
            .map(|entry| entry.is_none())
            .map_err(|source| ServerError::RecoveryPath {
                path: path.to_path_buf(),
                source,
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotADirectory => Ok(false),
        Err(source) => Err(ServerError::RecoveryPath {
            path: path.to_path_buf(),
            source,
        }),
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

fn root_route(
    placement: &RootPlacement,
    object_namespace_id: ObjectNamespaceId,
    lease: &LogicalShardLease,
) -> RootRoute {
    RootRoute {
        root_id: RootIdentity::from(placement.root_id),
        logical_shard_id: LogicalShardIdentity::from(placement.logical_shard_id),
        object_namespace_id: ObjectNamespaceIdentity::from(object_namespace_id),
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
    executor: &Arc<dyn WorkspaceRequestExecutor>,
    recovery: &RecoveryPublisher,
    placement: &RootPlacement,
    lease: &LogicalShardLease,
    root: &RootAttach,
) -> Result<RootRoute, ServerError> {
    debug_assert_eq!(placement.root_id, root.root_id);

    match meta.root_fence(placement.root_id)? {
        None => execute_fence_command(
            meta,
            placement,
            root.object_namespace_id,
            lease,
            root.install_id,
            meta::RootFenceAction::Install,
            b"nokv.root-fence.install.v1".to_vec(),
        )?,
        Some(fence) => validate_existing_fence(placement, root.object_namespace_id, fence)?,
    }

    let fence = meta.root_fence(placement.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap("root fence install produced no durable fence".to_owned())
    })?;
    validate_existing_fence(placement, root.object_namespace_id, fence)?;
    if fence.object_namespace_id.is_none() {
        execute_fence_command(
            meta,
            placement,
            root.object_namespace_id,
            lease,
            root.bind_object_namespace_id,
            meta::RootFenceAction::BindObjectNamespace {
                expected: fence.activation_state,
            },
            b"nokv.root-fence.bind-object-namespace.v1".to_vec(),
        )?;
    }

    let fence = meta.root_fence(placement.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap(
            "root fence namespace bind produced no durable fence".to_owned(),
        )
    })?;
    validate_bound_fence(placement, root.object_namespace_id, fence)?;
    if fence.activation_state == RootActivationState::Installing {
        execute_fence_command(
            meta,
            placement,
            root.object_namespace_id,
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
    validate_bound_fence(placement, root.object_namespace_id, active)?;
    if active.activation_state != RootActivationState::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "root fence is {:?}, expected Active",
            active.activation_state
        )));
    }
    recovery.publish_current()?;
    let route = root_route(placement, root.object_namespace_id, lease);
    registry.install(route, Arc::clone(executor))?;
    Ok(route)
}

fn validate_existing_fence(
    placement: &RootPlacement,
    object_namespace_id: ObjectNamespaceId,
    fence: meta::RootFence,
) -> Result<(), ServerError> {
    if fence.logical_shard_id != placement.logical_shard_id
        || fence.placement_generation != placement.placement_generation
        || fence
            .object_namespace_id
            .is_some_and(|actual| actual != object_namespace_id)
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

fn validate_bound_fence(
    placement: &RootPlacement,
    object_namespace_id: ObjectNamespaceId,
    fence: meta::RootFence,
) -> Result<(), ServerError> {
    validate_existing_fence(placement, object_namespace_id, fence)?;
    if fence.object_namespace_id != Some(object_namespace_id) {
        return Err(ServerError::InvalidBootstrap(
            "root fence has no durable object namespace binding".to_owned(),
        ));
    }
    Ok(())
}

fn execute_fence_command(
    meta: &meta::MetaShard,
    placement: &RootPlacement,
    object_namespace_id: ObjectNamespaceId,
    lease: &LogicalShardLease,
    request_id: RequestId,
    action: meta::RootFenceAction,
    deterministic_result: Vec<u8>,
) -> Result<(), ServerError> {
    let command = meta::MetadataCommand {
        schema_id: meta::SCHEMA_ID.to_owned(),
        root_id: placement.root_id,
        logical_shard_id: placement.logical_shard_id,
        object_namespace_id: Some(object_namespace_id),
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
        if let Err(error) = control.suspend_recovery(lease) {
            rollback.push(format!("suspend logical-shard recovery: {error}"));
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
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use nokv_control::{
        ControlError, InMemoryControlStore, LogRef, LogSegmentRef, LogicalShardState,
        PlacementGeneration, RootPlacement,
    };
    use nokv_object::{
        ensure_object_namespace, ArtifactObjectStore, ArtifactStoreCapabilities,
        BoundArtifactStore, ImmutableCreateOutcome, MemoryArtifactStore, ObjectDeleteOutcome,
        ObjectError, ObjectInfo, ObjectKey, ObjectRange, ProviderAdmissionReceipt,
        ProviderHandleIdentity,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{ServerOptions, WorkspaceServer};

    #[derive(Clone)]
    struct FailNextCreateStore {
        inner: BoundArtifactStore<MemoryArtifactStore>,
        fail_next_create: Arc<AtomicBool>,
    }

    #[derive(Clone)]
    struct FailCreateAndDeleteStore {
        inner: BoundArtifactStore<MemoryArtifactStore>,
        create_attempts: Arc<AtomicUsize>,
        fail_create_at: usize,
        delete_attempts: Arc<AtomicUsize>,
        fail_delete_at: Arc<AtomicUsize>,
    }

    impl FailCreateAndDeleteStore {
        fn new(inner: BoundArtifactStore<MemoryArtifactStore>, fail_create_at: usize) -> Self {
            Self {
                inner,
                create_attempts: Arc::new(AtomicUsize::new(0)),
                fail_create_at,
                delete_attempts: Arc::new(AtomicUsize::new(0)),
                fail_delete_at: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn fail_delete_at(&self, attempt: usize) {
            self.fail_delete_at.store(attempt, Ordering::SeqCst);
        }
    }

    impl FailNextCreateStore {
        fn new(inner: BoundArtifactStore<MemoryArtifactStore>) -> Self {
            Self {
                inner,
                fail_next_create: Arc::new(AtomicBool::new(false)),
            }
        }

        fn fail_next_create(&self) {
            self.fail_next_create.store(true, Ordering::SeqCst);
        }
    }

    impl ArtifactObjectStore for FailNextCreateStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.inner.object_namespace()
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

        fn provider_handle_identity(&self) -> ProviderHandleIdentity {
            self.inner.provider_handle_identity()
        }

        fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
            self.inner.provider_admission_receipt()
        }

        fn create_immutable(
            &self,
            key: &ObjectKey,
            bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            if self.fail_next_create.swap(false, Ordering::SeqCst) {
                return Err(ObjectError::Backend {
                    detail: "injected definite recovery create failure".to_owned(),
                    retryable: true,
                });
            }
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.inner.read(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            self.inner.delete(key)
        }
    }

    impl ArtifactObjectStore for FailCreateAndDeleteStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.inner.object_namespace()
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

        fn provider_handle_identity(&self) -> ProviderHandleIdentity {
            self.inner.provider_handle_identity()
        }

        fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
            self.inner.provider_admission_receipt()
        }

        fn create_immutable(
            &self,
            key: &ObjectKey,
            bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            let attempt = self.create_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == self.fail_create_at {
                return Err(ObjectError::Backend {
                    detail: "injected partial recovery create failure".to_owned(),
                    retryable: false,
                });
            }
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.inner.read(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            let attempt = self.delete_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == self.fail_delete_at.load(Ordering::SeqCst) {
                return Err(ObjectError::DeleteAmbiguous {
                    key: key.clone(),
                    detail: "injected pending cleanup ambiguity".to_owned(),
                });
            }
            self.inner.delete(key)
        }
    }

    struct LoseFinalizeAckControlStore {
        inner: Arc<InMemoryControlStore>,
        lose_next_finalize_ack: AtomicBool,
        fail_next_finalize_before_apply: AtomicBool,
        logical_shard_override: Mutex<Option<LogicalShardRecord>>,
        advance_before_successor: Mutex<Option<(Arc<RecoveryPublisher>, LogicalShardLease)>>,
    }

    impl LoseFinalizeAckControlStore {
        fn new(inner: Arc<InMemoryControlStore>) -> Self {
            Self {
                inner,
                lose_next_finalize_ack: AtomicBool::new(true),
                fail_next_finalize_before_apply: AtomicBool::new(false),
                logical_shard_override: Mutex::new(None),
                advance_before_successor: Mutex::new(None),
            }
        }

        fn with_read_override(
            inner: Arc<InMemoryControlStore>,
            record: LogicalShardRecord,
        ) -> Self {
            Self {
                inner,
                lose_next_finalize_ack: AtomicBool::new(false),
                fail_next_finalize_before_apply: AtomicBool::new(false),
                logical_shard_override: Mutex::new(Some(record)),
                advance_before_successor: Mutex::new(None),
            }
        }

        fn advance_before_successor(
            inner: Arc<InMemoryControlStore>,
            publisher: Arc<RecoveryPublisher>,
            lease: LogicalShardLease,
        ) -> Self {
            Self {
                inner,
                lose_next_finalize_ack: AtomicBool::new(false),
                fail_next_finalize_before_apply: AtomicBool::new(false),
                logical_shard_override: Mutex::new(None),
                advance_before_successor: Mutex::new(Some((publisher, lease))),
            }
        }

        fn fail_finalize_before_apply(inner: Arc<InMemoryControlStore>) -> Self {
            Self {
                inner,
                lose_next_finalize_ack: AtomicBool::new(false),
                fail_next_finalize_before_apply: AtomicBool::new(true),
                logical_shard_override: Mutex::new(None),
                advance_before_successor: Mutex::new(None),
            }
        }
    }

    impl ControlStore for LoseFinalizeAckControlStore {
        fn create_root_agent_binding(
            &self,
            binding: nokv_control::RootAgentBinding,
        ) -> Result<nokv_control::RootAgentBinding, ControlError> {
            self.inner.create_root_agent_binding(binding)
        }

        fn get_root_agent_binding(
            &self,
            root_id: &RootId,
        ) -> Result<Option<nokv_control::RootAgentBinding>, ControlError> {
            self.inner.get_root_agent_binding(root_id)
        }

        fn create_root_object_namespace_binding(
            &self,
            binding: nokv_control::RootObjectNamespaceBinding,
        ) -> Result<nokv_control::RootObjectNamespaceBinding, ControlError> {
            self.inner.create_root_object_namespace_binding(binding)
        }

        fn get_root_object_namespace_binding(
            &self,
            root_id: &RootId,
        ) -> Result<Option<nokv_control::RootObjectNamespaceBinding>, ControlError> {
            self.inner.get_root_object_namespace_binding(root_id)
        }

        fn create_root_placement(
            &self,
            placement: RootPlacement,
        ) -> Result<RootPlacement, ControlError> {
            self.inner.create_root_placement(placement)
        }

        fn get_root_placement(
            &self,
            root_id: &RootId,
        ) -> Result<Option<RootPlacement>, ControlError> {
            self.inner.get_root_placement(root_id)
        }

        fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
            self.inner.list_root_placements()
        }

        fn compare_and_set_root_placement(
            &self,
            expected: &RootPlacement,
            next: RootPlacement,
        ) -> Result<RootPlacement, ControlError> {
            self.inner.compare_and_set_root_placement(expected, next)
        }

        fn create_logical_shard(
            &self,
            logical_shard_id: nokv_types::LogicalShardId,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.create_logical_shard(logical_shard_id)
        }

        fn get_logical_shard(
            &self,
            logical_shard_id: &nokv_types::LogicalShardId,
        ) -> Result<Option<LogicalShardRecord>, ControlError> {
            if let Some(record) = self
                .logical_shard_override
                .lock()
                .expect("test logical-shard override lock must remain available")
                .as_ref()
                .filter(|record| &record.logical_shard_id == logical_shard_id)
            {
                return Ok(Some(record.clone()));
            }
            self.inner.get_logical_shard(logical_shard_id)
        }

        fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
            self.inner.list_logical_shards()
        }

        fn acquire_owner(
            &self,
            logical_shard_id: &nokv_types::LogicalShardId,
            owner: NodeId,
            endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            self.inner.acquire_owner(logical_shard_id, owner, endpoint)
        }

        fn acquire_successor(
            &self,
            logical_shard_id: &nokv_types::LogicalShardId,
            expected_owner_epoch: nokv_control::OwnerEpoch,
            owner: NodeId,
            endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            if let Some((publisher, lease)) = self
                .advance_before_successor
                .lock()
                .expect("test successor hook lock must remain available")
                .take()
            {
                publisher
                    .publish_current()
                    .map_err(|error| ControlError::Backend(error.to_string()))?;
                self.inner.release_owner(&lease)?;
            }
            self.inner
                .acquire_successor(logical_shard_id, expected_owner_epoch, owner, endpoint)
        }

        fn reacquire_recovery(
            &self,
            logical_shard_id: &nokv_types::LogicalShardId,
            recovery_epoch: nokv_control::OwnerEpoch,
            owner: NodeId,
            endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            self.inner
                .reacquire_recovery(logical_shard_id, recovery_epoch, owner, endpoint)
        }

        fn renew_owner(
            &self,
            lease: &LogicalShardLease,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.renew_owner(lease)
        }

        fn mark_serving(
            &self,
            lease: &LogicalShardLease,
            publication: RecoveryPublication,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.mark_serving(lease, publication)
        }

        fn prepare_recovery_upload(
            &self,
            lease: &LogicalShardLease,
            intent: nokv_control::RecoveryUploadIntent,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.prepare_recovery_upload(lease, intent)
        }

        fn finalize_recovery_upload(
            &self,
            lease: &LogicalShardLease,
            expected_intent: &nokv_control::RecoveryUploadIntent,
            publication: RecoveryPublication,
        ) -> Result<LogicalShardRecord, ControlError> {
            if self
                .fail_next_finalize_before_apply
                .swap(false, Ordering::SeqCst)
            {
                return Err(ControlError::Backend(
                    "injected recovery-finalize failure before apply".to_owned(),
                ));
            }
            let applied =
                self.inner
                    .finalize_recovery_upload(lease, expected_intent, publication)?;
            if self.lose_next_finalize_ack.swap(false, Ordering::SeqCst) {
                return Err(ControlError::Backend(
                    "injected recovery-finalize response loss".to_owned(),
                ));
            }
            Ok(applied)
        }

        fn abort_recovery_upload(
            &self,
            lease: &LogicalShardLease,
            expected_intent: &nokv_control::RecoveryUploadIntent,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.abort_recovery_upload(lease, expected_intent)
        }

        fn suspend_recovery(
            &self,
            lease: &LogicalShardLease,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.suspend_recovery(lease)
        }

        fn release_owner(
            &self,
            lease: &LogicalShardLease,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.release_owner(lease)
        }
    }

    struct LeaseClockExecutor {
        meta: Arc<meta::MetaShard>,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: nokv_control::OwnerEpoch,
    }

    impl WorkspaceRequestExecutor for LeaseClockExecutor {
        fn execute(
            &self,
            request: &nokv_protocol::WorkspaceRpcRequest,
        ) -> Result<crate::ExecutedRequest, nokv_protocol::RpcFailure> {
            self.meta
                .observe_lease_clock(
                    self.root_id,
                    self.placement_generation,
                    self.owner_epoch,
                    41,
                )
                .expect("test lease-clock mutation must apply");
            Ok(crate::ExecutedRequest {
                result: nokv_protocol::WorkspaceResult::Preflight(
                    nokv_protocol::WorkspacePreflightResult::new(request.route, []),
                ),
                commit_version: None,
                replayed: false,
            })
        }
    }

    fn bootstrap_shard(
        control: Arc<dyn ControlStore>,
        registry: Arc<RootOwnerRegistry>,
        boot: ShardBoot,
    ) -> Result<ShardOwner, ServerError> {
        static OBJECTS: OnceLock<Mutex<BTreeMap<ObjectNamespaceId, MemoryArtifactStore>>> =
            OnceLock::new();
        let namespace = boot
            .roots
            .first()
            .expect("test bootstrap requires a root")
            .object_namespace_id;
        let raw = OBJECTS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .expect("test recovery object map lock must remain available")
            .entry(namespace)
            .or_default()
            .clone();
        ensure_object_namespace(&raw, namespace).expect("install test object namespace");
        let bound = BoundArtifactStore::open(raw, namespace).expect("bind test object namespace");
        super::bootstrap_shard(control, registry, Arc::new(bound), boot)
    }

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
                    receipt: vec![1, 2, 3],
                }],
                durable_lsn: lsn,
                digest,
            }),
            durable_lsn: lsn,
        }
    }

    fn current_recovery(control: &InMemoryControlStore) -> RecoveryPublication {
        let record = control
            .get_logical_shard(&shard())
            .unwrap()
            .expect("test logical shard must exist");
        recovery_publication(&record)
    }

    fn add_active_root(
        control: &InMemoryControlStore,
        root_id: RootId,
        logical_shard_id: nokv_types::LogicalShardId,
    ) {
        control
            .create_root_object_namespace_binding(nokv_control::RootObjectNamespaceBinding {
                root_id,
                object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([10; 16]),
            })
            .unwrap();
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
            object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([10; 16]),
            install_id: request_id(seed),
            bind_object_namespace_id: request_id(seed.saturating_add(1)),
            activate_id: request_id(seed.saturating_add(2)),
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
        control: &InMemoryControlStore,
        path: PathBuf,
        lease: LogicalShardLease,
        roots: &[RootId],
        seed: u8,
    ) -> ShardBoot {
        ShardBoot {
            shard_id: shard(),
            open: OpenMode::Existing(path),
            lease: LeaseMode::Resume { lease },
            recovery: current_recovery(control),
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

    fn recovery_objects() -> Arc<BoundArtifactStore<MemoryArtifactStore>> {
        let namespace = ObjectNamespaceId::from_bytes([10; nokv_types::FIXED_ID_BYTES]);
        let raw = MemoryArtifactStore::new();
        ensure_object_namespace(&raw, namespace).unwrap();
        Arc::new(BoundArtifactStore::open(raw, namespace).unwrap())
    }

    fn options() -> ServerOptions {
        ServerOptions {
            bind: "127.0.0.1:0".parse().unwrap(),
            handshake_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
            lease_renew_interval: Duration::from_millis(10),
            max_inflight_connections: 8,
        }
    }

    #[test]
    fn rpc_success_waits_for_object_first_recovery_publication_and_repairs_pending_intent() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let registry = Arc::new(RootOwnerRegistry::new());
        let namespace = ObjectNamespaceId::from_bytes([10; nokv_types::FIXED_ID_BYTES]);
        let raw = MemoryArtifactStore::new();
        ensure_object_namespace(&raw, namespace).unwrap();
        let bound = BoundArtifactStore::open(raw, namespace).unwrap();
        let objects = FailNextCreateStore::new(bound);
        let owner = super::bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            Arc::new(objects.clone()),
            acquire_boot(temporary.path().join("metadata"), &[root_id]),
        )
        .unwrap();
        let route = owner.routes()[0];
        let executor = RecoveryPublishingExecutor::new(
            Arc::new(LeaseClockExecutor {
                meta: Arc::clone(owner.meta()),
                root_id,
                placement_generation: PlacementGeneration::new(route.placement_generation).unwrap(),
                owner_epoch: owner.lease().owner_epoch,
            }),
            Arc::clone(owner.recovery_publisher()),
        );
        let request = nokv_protocol::WorkspaceRpcRequest {
            route,
            request_id: nokv_protocol::RequestIdentity([0x71; nokv_types::FIXED_ID_BYTES]),
            operation: nokv_protocol::WorkspaceRequest::Preflight(
                nokv_protocol::WorkspacePreflightRequest::new([]),
            ),
        };

        objects.fail_next_create();
        let failure = executor.execute(&request).unwrap_err();
        assert_eq!(failure.code, nokv_protocol::ErrorCode::ObjectUnavailable);
        assert!(failure.retryable);
        let interrupted = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(interrupted.pending_recovery_upload.is_some());
        assert!(
            interrupted.durable_lsn < owner.meta().recovery_state().unwrap().applied_recovery_lsn,
            "an inner metadata success must not advance control after object creation failed"
        );

        let result = executor.execute(&request).unwrap();
        assert!(matches!(
            result.result,
            nokv_protocol::WorkspaceResult::Preflight(_)
        ));
        let repaired = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(repaired.pending_recovery_upload.is_none());
        assert_eq!(
            repaired.durable_lsn,
            owner.meta().recovery_state().unwrap().applied_recovery_lsn
        );
        assert!(repaired.log.as_ref().is_some_and(|log| log
            .segments
            .iter()
            .all(|segment| !segment.receipt.is_empty())));
        owner.release().unwrap();
    }

    #[test]
    fn recovery_finalize_response_loss_reuses_the_same_epoch_and_exact_publication() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let inner = active_control(&[root_id]);
        let control = Arc::new(LoseFinalizeAckControlStore::new(Arc::clone(&inner)));
        let namespace = ObjectNamespaceId::from_bytes([10; nokv_types::FIXED_ID_BYTES]);
        let raw = MemoryArtifactStore::new();
        ensure_object_namespace(&raw, namespace).unwrap();
        let objects = Arc::new(BoundArtifactStore::open(raw, namespace).unwrap());

        let error = super::bootstrap_shard(
            control.clone(),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(database.clone(), &[root_id]),
        )
        .err()
        .expect("injected finalization response loss must interrupt bootstrap");
        assert!(error
            .to_string()
            .contains("injected recovery-finalize response loss"));
        let interrupted = inner.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(interrupted.state, LogicalShardState::Recovering);
        assert!(interrupted.pending_recovery_upload.is_none());
        assert!(interrupted.durable_lsn > 0);
        let recovery_epoch = interrupted.owner_epoch.unwrap();

        let recovered = super::bootstrap_shard(
            control,
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(recovery_epoch),
                },
                recovery: current_recovery(&inner),
                roots: vec![root_attach(root_id, 73)],
            },
        )
        .unwrap();
        assert_eq!(recovered.lease().owner_epoch, recovery_epoch);
        assert!(recovered.serving_record().pending_recovery_upload.is_none());
        assert_eq!(
            recovered.serving_record().durable_lsn,
            recovered
                .meta()
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn
        );
        recovered.release().unwrap();
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
            resume_boot(&control, database, lease.clone(), &[root_id], 5),
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
            &control,
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
            resume_boot(&control, database, lease.clone(), &[root_id], 7),
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
    fn prepared_first_owner_store_reports_reopen_after_settled_acquire_failure() {
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
        assert!(matches!(
            error,
            ServerError::PreparedOwnerAdmission { ref path, .. } if *path == database
        ));
        assert!(error.to_string().contains("--metadata-reopen"));
        assert!(database.exists());
        assert!(database.read_dir().unwrap().next().is_some());
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
    fn backend_owner_admission_failure_keeps_prepared_state_fail_closed() {
        let error = ServerError::PreparedOwnerAdmission {
            path: PathBuf::from("prepared-meta"),
            source: nokv_control::ControlError::Backend("outcome unavailable".to_owned()),
        }
        .to_string();
        assert!(error.contains("outcome is unknown"));
        assert!(error.contains("preserve the store"));
        assert!(error.contains("--metadata-reopen prepared-meta"));
        assert!(error.contains("rebind the durable Recovering epoch one"));
        assert!(error.contains("do not delete the prepared store"));
    }

    #[test]
    fn caller_owned_empty_directory_is_preserved_as_a_prepared_reopen() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        std::fs::create_dir(&database).unwrap();
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

        assert!(matches!(
            error,
            ServerError::PreparedOwnerAdmission { path, .. } if path == database
        ));
        assert!(database.exists());
        assert!(database.read_dir().unwrap().next().is_some());

        let mut retry = acquire_boot(database.clone(), &[root_id]);
        retry.open = OpenMode::Existing(database);
        let owner = bootstrap_shard(as_control(&control), registry, retry).unwrap();
        assert_eq!(owner.lease().owner_epoch.get(), 1);
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
        acquire.recovery = current_recovery(&control);
        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), acquire)
            .err()
            .unwrap();

        assert!(error.to_string().contains("Resume and the exact lease"));
        assert_eq!(control.get_logical_shard(&shard()).unwrap(), Some(before));
        assert_eq!(registry.installed_root_count().unwrap(), 0);

        let resumed = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            resume_boot(&control, database, lease.clone(), &[root_id], 41),
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
    fn released_local_authority_restarts_as_the_next_owner() {
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
        let durable_fence = first.meta().root_fence(root_id).unwrap().unwrap();
        let durable_version = first.meta().current_read_version().unwrap();
        first.release().unwrap();
        let registry = Arc::new(RootOwnerRegistry::new());

        let boot = ShardBoot {
            shard_id: shard(),
            open: OpenMode::Existing(database),
            lease: LeaseMode::Acquire {
                owner: NodeId::new("node-b").unwrap(),
                endpoint: "127.0.0.1:9020".to_owned(),
                previous_epoch: Some(first_epoch),
            },
            recovery: current_recovery(&control),
            roots: vec![root_attach(root_id, 33)],
        };
        let successor = bootstrap_shard(as_control(&control), Arc::clone(&registry), boot).unwrap();

        assert_eq!(successor.lease().owner_epoch.get(), first_epoch.get() + 1);
        assert_eq!(
            successor.meta().current_owner_epoch().unwrap(),
            Some(successor.lease().owner_epoch)
        );
        assert_eq!(
            successor.meta().root_fence(root_id).unwrap(),
            Some(durable_fence)
        );
        assert!(successor.meta().current_read_version().unwrap() >= durable_version);
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Serving);
        assert_eq!(record.owner_epoch, Some(successor.lease().owner_epoch));
        assert_eq!(
            record.durable_lsn,
            successor
                .meta()
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn
        );
        assert_eq!(registry.installed_root_count().unwrap(), 1);
        successor.release().unwrap();
    }

    #[test]
    fn live_local_authority_lock_blocks_takeover_before_control_mutation() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let before = control.get_logical_shard(&shard()).unwrap().unwrap();

        let error = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("competing-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(first.lease().owner_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(root_id, 35)],
            },
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("incompatible live access mode"));
        assert_eq!(control.get_logical_shard(&shard()).unwrap(), Some(before));
        first.release().unwrap();
    }

    #[test]
    fn bootstrap_rollback_rebinds_the_same_recovery_epoch() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let roots = [root(1), root(2)];
        let control = active_control(&roots);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &roots[..1]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        let future_root_two = RootPlacement {
            root_id: roots[1],
            logical_shard_id: shard(),
            placement_generation: PlacementGeneration::new(4).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
        };
        execute_fence_command(
            first.meta(),
            &future_root_two,
            nokv_types::ObjectNamespaceId::from_bytes([10; 16]),
            first.lease(),
            request_id(61),
            meta::RootFenceAction::Install,
            b"future root-two fence".to_vec(),
        )
        .unwrap();
        first.release().unwrap();

        let registry = Arc::new(RootOwnerRegistry::new());
        let error = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database.clone()),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("interrupted-node").unwrap(),
                    endpoint: "127.0.0.1:9015".to_owned(),
                    previous_epoch: Some(first_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(roots[0], 47), root_attach(roots[1], 49)],
            },
        )
        .err()
        .unwrap();

        assert!(error
            .to_string()
            .contains("root fence placement does not match the control-plane placement"));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        let interrupted = control.get_logical_shard(&shard()).unwrap().unwrap();
        let recovery_epoch = interrupted.owner_epoch.unwrap();
        assert_eq!(recovery_epoch.get(), first_epoch.get() + 1);
        assert_eq!(interrupted.state, LogicalShardState::Recovering);
        assert_eq!(
            open_meta(OpenMode::Existing(database.clone()), shard())
                .unwrap()
                .current_owner_epoch()
                .unwrap(),
            Some(recovery_epoch),
            "the failed boot had already installed its local fence"
        );

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

        let recovered = bootstrap_shard(
            as_control(&control),
            Arc::clone(&registry),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(recovery_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(roots[0], 51), root_attach(roots[1], 53)],
            },
        )
        .unwrap();

        assert_eq!(recovered.lease().owner_epoch, recovery_epoch);
        assert_eq!(registry.installed_root_count().unwrap(), 2);
        recovered.release().unwrap();
    }

    #[test]
    fn interrupted_successor_reuses_its_recovery_epoch_before_local_fence() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        let interrupted = control
            .acquire_successor(
                &shard(),
                first_epoch,
                NodeId::new("interrupted-node").unwrap(),
                "127.0.0.1:9015".to_owned(),
            )
            .unwrap();
        control.suspend_recovery(&interrupted).unwrap();
        assert_eq!(
            open_meta(OpenMode::Existing(database.clone()), shard())
                .unwrap()
                .current_owner_epoch()
                .unwrap(),
            Some(first_epoch),
            "the crash happened before the local fence advanced"
        );

        let recovered = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(interrupted.owner_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(root_id, 37)],
            },
        )
        .unwrap();

        assert_eq!(recovered.lease().owner_epoch, interrupted.owner_epoch);
        assert_eq!(
            recovered.meta().current_owner_epoch().unwrap(),
            Some(interrupted.owner_epoch)
        );
        recovered.release().unwrap();
    }

    #[test]
    fn interrupted_successor_reuses_its_recovery_epoch_after_local_fence() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        let interrupted = control
            .acquire_successor(
                &shard(),
                first_epoch,
                NodeId::new("interrupted-node").unwrap(),
                "127.0.0.1:9015".to_owned(),
            )
            .unwrap();
        let meta = open_meta(OpenMode::Existing(database.clone()), shard()).unwrap();
        meta.advance_owner_epoch(Some(first_epoch), interrupted.owner_epoch)
            .unwrap();
        drop(meta);
        control.suspend_recovery(&interrupted).unwrap();

        let recovered = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(interrupted.owner_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(root_id, 39)],
            },
        )
        .unwrap();

        assert_eq!(recovered.lease().owner_epoch, interrupted.owner_epoch);
        assert_eq!(
            recovered.meta().current_owner_epoch().unwrap(),
            Some(interrupted.owner_epoch)
        );
        recovered.release().unwrap();
    }

    #[test]
    fn stale_local_epoch_is_rejected_before_consuming_another_control_epoch() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        // Advance only the control plane. The local authority remains at the
        // first epoch and therefore cannot prove it contains the second
        // owner's applied history.
        let control_only = control
            .acquire_successor(
                &shard(),
                first_epoch,
                NodeId::new("control-only-node").unwrap(),
                "127.0.0.1:9015".to_owned(),
            )
            .unwrap();
        let durable = current_recovery(&control);
        control.mark_serving(&control_only, durable).unwrap();
        control.release_owner(&control_only).unwrap();

        let error = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("stale-store-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(control_only.owner_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(root_id, 43)],
            },
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("exact durable epoch 2"));
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.owner_epoch, Some(control_only.owner_epoch));
        assert_eq!(record.state, LogicalShardState::Unassigned);
    }

    #[test]
    fn corrupt_recovery_outbox_is_rejected_before_successor_acquisition() {
        use nokv_meta_store::{Commit, Key, Mutation, WriteTxn};

        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        let catalog = meta::keyspaces()
            .iter()
            .map(|definition| TreeBinding::new(definition.id, definition.name));
        let holt =
            HoltStore::open(HoltOptions::file(&database, catalog, meta::store_limits())).unwrap();
        let recovery_keyspace = meta::keyspaces()
            .iter()
            .find(|definition| definition.name == "recovery_outbox")
            .unwrap()
            .id;
        assert_eq!(
            holt.commit(WriteTxn {
                checks: Vec::new(),
                mutations: vec![Mutation::Put {
                    key: Key::new(recovery_keyspace, b"malformed-key".to_vec()),
                    value: b"malformed-value".to_vec(),
                }],
            })
            .unwrap(),
            Commit::Applied
        );
        drop(holt);

        let error = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(first_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(root_id, 45)],
            },
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("RecoveryOutbox key"));
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.owner_epoch, Some(first_epoch));
        assert_eq!(record.state, LogicalShardState::Unassigned);
    }

    #[test]
    fn legacy_unbound_root_fence_is_bound_once_during_successor_reopen() {
        use nokv_meta_store::{Commit, Key, Mutation, TxnStore, WriteTxn};

        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let first = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(database.clone(), &[root_id]),
        )
        .unwrap();
        let first_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        // Recreate the exact v1 fence bytes that predate object namespace
        // binding. The control placement and owner epoch remain untouched.
        let catalog = meta::keyspaces()
            .iter()
            .map(|definition| TreeBinding::new(definition.id, definition.name));
        let holt =
            HoltStore::open(HoltOptions::file(&database, catalog, meta::store_limits())).unwrap();
        let root_fence_keyspace = meta::keyspaces()
            .iter()
            .find(|definition| definition.name == "root_fence")
            .unwrap()
            .id;
        let legacy = meta::RootFence {
            logical_shard_id: shard(),
            object_namespace_id: None,
            placement_generation: PlacementGeneration::new(2).unwrap(),
            activation_state: RootActivationState::Active,
        }
        .encode()
        .unwrap();
        assert_eq!(
            holt.commit(WriteTxn {
                checks: Vec::new(),
                mutations: vec![Mutation::Put {
                    key: Key::new(root_fence_keyspace, root_id.as_bytes().to_vec()),
                    value: legacy,
                }],
            })
            .unwrap(),
            Commit::Applied
        );
        drop(holt);

        let owner = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(database),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("successor").unwrap(),
                    endpoint: "127.0.0.1:9030".to_owned(),
                    previous_epoch: Some(first_epoch),
                },
                recovery: current_recovery(&control),
                roots: vec![root_attach(root_id, 71)],
            },
        )
        .unwrap();

        assert_eq!(owner.lease().owner_epoch.get(), first_epoch.get() + 1);
        assert_eq!(
            owner
                .meta()
                .root_fence(root_id)
                .unwrap()
                .unwrap()
                .object_namespace_id,
            Some(nokv_types::ObjectNamespaceId::from_bytes([10; 16]))
        );
        owner.release().unwrap();
    }

    #[test]
    fn wrong_control_object_namespace_is_rejected_before_epoch_or_holt_creation() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let before = control.get_logical_shard(&shard()).unwrap().unwrap();
        let mut boot = acquire_boot(database.clone(), &[root_id]);
        boot.roots[0].object_namespace_id = nokv_types::ObjectNamespaceId::from_bytes([0x44; 16]);

        let error = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            boot,
        )
        .err()
        .expect("control namespace mismatch must fail closed");

        assert!(error
            .to_string()
            .contains("object namespace differs from its control binding"));
        assert_eq!(control.get_logical_shard(&shard()).unwrap(), Some(before));
        assert!(!database.exists());
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
    fn persisted_shared_frontier_requires_an_explicit_fresh_recovery_mode() {
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
                previous_epoch: None,
            },
            recovery,
            roots: vec![root_attach(root_id, 31)],
        };
        let error = bootstrap_shard(as_control(&control), Arc::clone(&registry), boot)
            .err()
            .unwrap();

        assert!(error
            .to_string()
            .contains("new metadata store cannot adopt logical shard"));
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
            resume_boot(&control, database, lease.clone(), &roots, 21),
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
    fn recover_log_initializes_missing_and_empty_targets_from_exact_control_receipts() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let objects = recovery_objects();
        let first = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("source"), &[root_id]),
        )
        .unwrap();
        let mut previous_epoch = first.lease().owner_epoch;
        first.release().unwrap();

        for (index, create_empty) in [false, true].into_iter().enumerate() {
            let target = temporary.path().join(format!("recovered-{index}"));
            if create_empty {
                std::fs::create_dir(&target).unwrap();
            }
            let record = control.get_logical_shard(&shard()).unwrap().unwrap();
            let owner = super::bootstrap_shard(
                as_control(&control),
                Arc::new(RootOwnerRegistry::new()),
                objects.clone(),
                ShardBoot {
                    shard_id: shard(),
                    open: OpenMode::RecoverLog(target),
                    lease: LeaseMode::Acquire {
                        owner: NodeId::new(format!("recovery-node-{index}")).unwrap(),
                        endpoint: format!("127.0.0.1:90{}0", index + 2),
                        previous_epoch: Some(previous_epoch),
                    },
                    recovery: recovery_publication(&record),
                    roots: vec![root_attach(root_id, 80 + index as u8 * 3)],
                },
            )
            .unwrap();

            assert_eq!(
                owner.serving_record().durable_lsn,
                owner.meta().recovery_state().unwrap().applied_recovery_lsn
            );
            assert!(owner.serving_record().durable_lsn >= record.durable_lsn);
            previous_epoch = owner.lease().owner_epoch;
            owner.release().unwrap();
        }
    }

    #[test]
    fn recover_log_opens_a_partial_nonempty_store_and_replays_only_the_missing_suffix() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let objects = recovery_objects();
        let first = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("source"), &[root_id]),
        )
        .unwrap();
        let previous_epoch = first.lease().owner_epoch;
        first.release().unwrap();
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        let durable_log = record.log.as_ref().unwrap();
        assert!(durable_log.segments.len() >= 2);
        let first_segment = durable_log.segments[0].clone();
        let mut partial = record.clone();
        partial.log = Some(LogRef {
            segments: vec![first_segment.clone()],
            durable_lsn: first_segment.last_lsn,
            digest: first_segment.digest,
        });
        partial.durable_lsn = first_segment.last_lsn;
        partial.pending_recovery_upload = None;
        let target = temporary.path().join("partial");
        let partial_meta = open_meta(OpenMode::New(target.clone()), shard()).unwrap();
        install_durable_recovery_log(&partial, objects.as_ref(), partial_meta.as_ref()).unwrap();
        let partial_state = partial_meta.recovery_state().unwrap();
        drop(partial_meta);

        let recovered = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::RecoverLog(target),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(previous_epoch),
                },
                recovery: recovery_publication(&record),
                roots: vec![root_attach(root_id, 86)],
            },
        )
        .unwrap();

        assert!(
            recovered
                .meta()
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn
                > partial_state.applied_recovery_lsn
        );
        recovered.release().unwrap();
    }

    #[test]
    fn existing_preflight_rejects_behind_divergent_and_untrusted_control_without_side_effects() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let objects = recovery_objects();
        let source_path = temporary.path().join("source");
        let first = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(source_path.clone(), &[root_id]),
        )
        .unwrap();
        let previous_epoch = first.lease().owner_epoch;
        first.release().unwrap();
        let before_control = control.get_logical_shard(&shard()).unwrap().unwrap();
        let durable_log = before_control.log.as_ref().unwrap();
        assert!(durable_log.segments.len() >= 2);

        let first_segment = durable_log.segments[0].clone();
        let mut partial = before_control.clone();
        partial.log = Some(LogRef {
            segments: vec![first_segment.clone()],
            durable_lsn: first_segment.last_lsn,
            digest: first_segment.digest,
        });
        partial.durable_lsn = first_segment.last_lsn;
        partial.pending_recovery_upload = None;
        let behind_path = temporary.path().join("behind");
        let behind = open_meta(OpenMode::New(behind_path.clone()), shard()).unwrap();
        install_durable_recovery_log(&partial, objects.as_ref(), behind.as_ref()).unwrap();
        let before_behind = behind.recovery_state().unwrap();
        drop(behind);
        let behind_error = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::Existing(behind_path.clone()),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("behind-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(previous_epoch),
                },
                recovery: recovery_publication(&before_control),
                roots: vec![root_attach(root_id, 95)],
            },
        )
        .err()
        .expect("a local authority behind Control must fail before acquisition");
        assert!(behind_error.to_string().contains("ahead of local"));
        assert_eq!(
            control.get_logical_shard(&shard()).unwrap().unwrap(),
            before_control
        );
        let behind = open_meta(OpenMode::Existing(behind_path), shard()).unwrap();
        assert_eq!(behind.recovery_state().unwrap(), before_behind);
        drop(behind);

        let source = open_meta(OpenMode::Existing(source_path.clone()), shard()).unwrap();
        let before_source = source.recovery_state().unwrap();
        drop(source);
        let mut invalid_records = Vec::new();

        let mut wrong_digest = before_control.clone();
        let log = wrong_digest.log.as_mut().unwrap();
        log.digest = "ff".repeat(SHA256_BYTES);
        log.segments.last_mut().unwrap().digest = "ff".repeat(SHA256_BYTES);
        invalid_records.push(wrong_digest);

        let mut garbage_log = before_control.clone();
        garbage_log.log.as_mut().unwrap().segments[0].receipt = vec![0; 32];
        invalid_records.push(garbage_log);

        let mut foreign_log = before_control.clone();
        foreign_log.log.as_mut().unwrap().segments[0].receipt[8 + 2] ^= 0x01;
        invalid_records.push(foreign_log);

        let mut garbage_checkpoint = before_control.clone();
        garbage_checkpoint.checkpoint = Some(nokv_control::CheckpointRef {
            object_key: "nokv/recovery/checkpoint".to_owned(),
            lsn: 1,
            image_bytes: 1,
            image_digest: "00".repeat(SHA256_BYTES),
            digest: "00".repeat(SHA256_BYTES),
            receipt: vec![0; 32],
        });
        invalid_records.push(garbage_checkpoint);

        for (index, invalid) in invalid_records.into_iter().enumerate() {
            let control_view: Arc<dyn ControlStore> = Arc::new(
                LoseFinalizeAckControlStore::with_read_override(control.clone(), invalid.clone()),
            );
            let error = super::bootstrap_shard(
                control_view,
                Arc::new(RootOwnerRegistry::new()),
                objects.clone(),
                ShardBoot {
                    shard_id: shard(),
                    open: OpenMode::Existing(source_path.clone()),
                    lease: LeaseMode::Acquire {
                        owner: NodeId::new(format!("invalid-node-{index}")).unwrap(),
                        endpoint: format!("127.0.0.1:91{index}0"),
                        previous_epoch: Some(previous_epoch),
                    },
                    recovery: recovery_publication(&invalid),
                    roots: vec![root_attach(root_id, 100 + index as u8 * 3)],
                },
            )
            .err()
            .expect("invalid Control receipt or digest must fail before acquisition");
            assert!(matches!(error, ServerError::RecoveryInstallation(_)));
            assert_eq!(
                control.get_logical_shard(&shard()).unwrap().unwrap(),
                before_control
            );
            let source = open_meta(OpenMode::Existing(source_path.clone()), shard()).unwrap();
            assert_eq!(source.recovery_state().unwrap(), before_source);
            drop(source);
        }
    }

    #[test]
    fn recover_log_never_falls_back_to_initialize_for_a_nonempty_corrupt_path() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let objects = recovery_objects();
        let first = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("source"), &[root_id]),
        )
        .unwrap();
        let previous_epoch = first.lease().owner_epoch;
        first.release().unwrap();
        let before = control.get_logical_shard(&shard()).unwrap().unwrap();
        let target = temporary.path().join("corrupt");
        std::fs::create_dir(&target).unwrap();
        let sentinel = target.join("not-a-holt-store");
        std::fs::write(&sentinel, b"preserve me").unwrap();

        let error = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::RecoverLog(target),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(previous_epoch),
                },
                recovery: recovery_publication(&before),
                roots: vec![root_attach(root_id, 89)],
            },
        )
        .err()
        .unwrap();

        assert!(matches!(error, ServerError::Store(_)));
        assert_eq!(
            control.get_logical_shard(&shard()).unwrap().unwrap(),
            before
        );
        assert_eq!(std::fs::read(sentinel).unwrap(), b"preserve me");
    }

    #[test]
    fn recover_log_cleans_partial_pending_objects_before_exact_abort_and_retries_ambiguity() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let namespace = ObjectNamespaceId::from_bytes([10; nokv_types::FIXED_ID_BYTES]);
        let raw = MemoryArtifactStore::new();
        ensure_object_namespace(&raw, namespace).unwrap();
        let bound = BoundArtifactStore::open(raw, namespace).unwrap();
        let objects = Arc::new(FailCreateAndDeleteStore::new(bound, 2));

        let first_error = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("lost-source"), &[root_id]),
        )
        .err()
        .expect("the second immutable create must leave a durable pending intent");
        assert!(matches!(
            first_error,
            ServerError::RecoveryPublication(crate::RecoveryPublisherError::Object(
                nokv_object::RecoveryLogSegmentError::Object(ObjectError::Backend { .. })
            ))
        ));
        let interrupted = control.get_logical_shard(&shard()).unwrap().unwrap();
        let pending = interrupted
            .pending_recovery_upload
            .clone()
            .expect("partial create must retain exact cleanup authority");
        let recovery_epoch = interrupted.owner_epoch.unwrap();
        assert_eq!(interrupted.state, LogicalShardState::Recovering);

        objects.fail_delete_at(2);
        let recovered_path = temporary.path().join("recovered");
        let boot = ShardBoot {
            shard_id: shard(),
            open: OpenMode::RecoverLog(recovered_path.clone()),
            lease: LeaseMode::Acquire {
                owner: NodeId::new("recovery-node").unwrap(),
                endpoint: "127.0.0.1:9020".to_owned(),
                previous_epoch: Some(recovery_epoch),
            },
            recovery: recovery_publication(&interrupted),
            roots: vec![root_attach(root_id, 92)],
        };
        let cleanup_error = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            boot.clone(),
        )
        .err()
        .expect("ambiguous cleanup must retain the pending intent");
        assert!(cleanup_error
            .to_string()
            .contains("pending recovery cleanup failed"));
        let retained = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(retained.pending_recovery_upload.as_ref(), Some(&pending));
        assert_eq!(retained.owner_epoch, Some(recovery_epoch));
        let local = open_meta(OpenMode::Existing(recovered_path), shard()).unwrap();
        assert_eq!(local.current_owner_epoch().unwrap(), None);
        drop(local);

        let recovered = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects,
            boot,
        )
        .unwrap();
        assert!(recovered.serving_record().pending_recovery_upload.is_none());
        assert_eq!(recovered.lease().owner_epoch, recovery_epoch);
        recovered.release().unwrap();
    }

    #[test]
    fn recover_log_installs_a_complete_pending_object_before_publisher_finalizes_it() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let inner = active_control(&[root_id]);
        let control: Arc<dyn ControlStore> = Arc::new(
            LoseFinalizeAckControlStore::fail_finalize_before_apply(inner.clone()),
        );
        let objects = recovery_objects();

        let first_error = super::bootstrap_shard(
            control,
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("lost-source"), &[root_id]),
        )
        .err()
        .expect("injected finalization failure must retain a complete pending object");
        assert!(first_error
            .to_string()
            .contains("injected recovery-finalize failure before apply"));
        let interrupted = inner.get_logical_shard(&shard()).unwrap().unwrap();
        let pending = interrupted
            .pending_recovery_upload
            .as_ref()
            .expect("complete pending object must retain its exact intent");
        let receipt = nokv_object::RecoveryLogSegmentReceipt::decode(&pending.receipt).unwrap();
        nokv_object::read_recovery_log_segment(objects.as_ref(), receipt.identity(), &receipt)
            .unwrap();

        let recovered = super::bootstrap_shard(
            as_control(&inner),
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::RecoverLog(temporary.path().join("recovered")),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: interrupted.owner_epoch,
                },
                recovery: recovery_publication(&interrupted),
                roots: vec![root_attach(root_id, 107)],
            },
        )
        .unwrap();

        assert!(recovered.serving_record().pending_recovery_upload.is_none());
        assert!(recovered.serving_record().durable_lsn >= pending.last_lsn);
        recovered.release().unwrap();
    }

    #[test]
    fn recover_log_resumes_the_same_target_after_pending_replay_crashes_before_finalize() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let inner = active_control(&[root_id]);
        let control: Arc<dyn ControlStore> = Arc::new(
            LoseFinalizeAckControlStore::fail_finalize_before_apply(inner.clone()),
        );
        let objects = recovery_objects();
        super::bootstrap_shard(
            control,
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("lost-source"), &[root_id]),
        )
        .err()
        .expect("injected finalization failure must retain pending recovery");
        let interrupted = inner.get_logical_shard(&shard()).unwrap().unwrap();
        let recovery_epoch = interrupted.owner_epoch.unwrap();
        let target = temporary.path().join("recovered");
        let meta = open_meta(OpenMode::RecoverLog(target.clone()), shard()).unwrap();
        validate_recovery_control_references(
            &interrupted,
            root_attach(root_id, 120).object_namespace_id,
            &meta,
        )
        .unwrap();
        install_durable_recovery_log(&interrupted, objects.as_ref(), &meta).unwrap();
        let lease = inner
            .reacquire_recovery(
                &shard(),
                recovery_epoch,
                NodeId::new("crashing-recovery-node").unwrap(),
                "127.0.0.1:9120".to_owned(),
            )
            .unwrap();
        let acquired = inner.renew_owner(&lease).unwrap();
        assert!(matches!(
            install_pending_recovery_upload(&acquired, objects.as_ref(), &meta).unwrap(),
            PendingRecoveryInstallOutcome::Installed { .. }
        ));
        let replayed = meta.recovery_state().unwrap();
        assert!(replayed.applied_recovery_lsn > acquired.durable_lsn);
        inner.suspend_recovery(&lease).unwrap();
        drop(meta);
        let suspended = inner.get_logical_shard(&shard()).unwrap().unwrap();

        let recovered = super::bootstrap_shard(
            as_control(&inner),
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::RecoverLog(target),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("restarted-recovery-node").unwrap(),
                    endpoint: "127.0.0.1:9121".to_owned(),
                    previous_epoch: Some(recovery_epoch),
                },
                recovery: recovery_publication(&suspended),
                roots: vec![root_attach(root_id, 123)],
            },
        )
        .unwrap();
        assert!(recovered.serving_record().pending_recovery_upload.is_none());
        assert!(recovered.serving_record().durable_lsn >= replayed.applied_recovery_lsn);
        recovered.release().unwrap();
    }

    #[test]
    fn recover_log_resumes_the_same_target_after_activation_crashes_before_publication() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let objects = recovery_objects();
        let first = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("source"), &[root_id]),
        )
        .unwrap();
        let previous_epoch = first.lease().owner_epoch;
        first.release().unwrap();
        let durable = control.get_logical_shard(&shard()).unwrap().unwrap();
        let target = temporary.path().join("recovered");
        let meta = open_meta(OpenMode::RecoverLog(target.clone()), shard()).unwrap();
        validate_recovery_control_references(
            &durable,
            root_attach(root_id, 126).object_namespace_id,
            &meta,
        )
        .unwrap();
        install_durable_recovery_log(&durable, objects.as_ref(), &meta).unwrap();
        let lease = control
            .acquire_successor(
                &shard(),
                previous_epoch,
                NodeId::new("crashing-successor").unwrap(),
                "127.0.0.1:9122".to_owned(),
            )
            .unwrap();
        activate_shard(&meta, &lease).unwrap();
        let activated = meta.recovery_state().unwrap();
        assert!(activated.applied_recovery_lsn > durable.durable_lsn);
        control.suspend_recovery(&lease).unwrap();
        drop(meta);
        let suspended = control.get_logical_shard(&shard()).unwrap().unwrap();

        let recovered = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::RecoverLog(target),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("restarted-successor").unwrap(),
                    endpoint: "127.0.0.1:9123".to_owned(),
                    previous_epoch: Some(lease.owner_epoch),
                },
                recovery: recovery_publication(&suspended),
                roots: vec![root_attach(root_id, 129)],
            },
        )
        .unwrap();
        assert!(recovered.serving_record().durable_lsn >= activated.applied_recovery_lsn);
        recovered.release().unwrap();
    }

    #[test]
    fn recover_log_reloads_and_installs_a_frontier_advanced_during_successor_acquisition() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let objects = recovery_objects();
        let first = super::bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            objects.clone(),
            acquire_boot(temporary.path().join("source"), &[root_id]),
        )
        .unwrap();
        let route = first.routes()[0];
        first
            .meta()
            .observe_lease_clock(
                root_id,
                PlacementGeneration::new(route.placement_generation).unwrap(),
                first.lease().owner_epoch,
                77,
            )
            .unwrap();
        let advanced_local = first.meta().recovery_state().unwrap();
        let initial = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(advanced_local.applied_recovery_lsn > initial.durable_lsn);
        let previous_epoch = first.lease().owner_epoch;
        let control_view: Arc<dyn ControlStore> =
            Arc::new(LoseFinalizeAckControlStore::advance_before_successor(
                control.clone(),
                Arc::clone(first.recovery_publisher()),
                first.lease().clone(),
            ));

        let recovered = super::bootstrap_shard(
            control_view,
            Arc::new(RootOwnerRegistry::new()),
            objects,
            ShardBoot {
                shard_id: shard(),
                open: OpenMode::RecoverLog(temporary.path().join("recovered")),
                lease: LeaseMode::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    previous_epoch: Some(previous_epoch),
                },
                recovery: recovery_publication(&initial),
                roots: vec![root_attach(root_id, 110)],
            },
        )
        .unwrap();

        assert!(
            recovered
                .meta()
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn
                >= advanced_local.applied_recovery_lsn
        );
        assert_eq!(
            recovered.serving_record().durable_lsn,
            recovered
                .meta()
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn
        );
        recovered.release().unwrap();
        drop(first);
    }

    #[test]
    fn stale_recovery_publisher_cannot_ack_after_a_successor_acquires_the_same_frontier() {
        let temporary = TempDir::new().unwrap();
        let root_id = root(1);
        let control = active_control(&[root_id]);
        let owner = bootstrap_shard(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_boot(temporary.path().join("metadata"), &[root_id]),
        )
        .unwrap();
        let stale = Arc::clone(owner.recovery_publisher());
        let previous_epoch = owner.lease().owner_epoch;
        owner.release().unwrap();
        let successor = control
            .acquire_successor(
                &shard(),
                previous_epoch,
                NodeId::new("node-b").unwrap(),
                "127.0.0.1:9020".to_owned(),
            )
            .unwrap();

        assert!(matches!(
            stale.publish_current(),
            Err(crate::RecoveryPublisherError::Control(
                ControlError::NotOwner { .. }
            ))
        ));
        control.suspend_recovery(&successor).unwrap();
    }

    #[test]
    fn post_admission_record_must_match_every_acquired_lease_field() {
        let lease = LogicalShardLease {
            logical_shard_id: shard(),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(7).unwrap(),
            lease_id: 11,
        };
        let mut record = LogicalShardRecord::unassigned(shard());
        record.owner = Some(lease.owner.clone());
        record.owner_epoch = Some(lease.owner_epoch);
        record.lease_id = lease.lease_id;
        record.state = LogicalShardState::Recovering;
        record.endpoint = Some("127.0.0.1:9010".to_owned());
        assert!(validate_control_record_lease(&record, &lease).is_ok());

        let mut wrong_shard = record.clone();
        wrong_shard.logical_shard_id = other_shard();
        assert!(validate_control_record_lease(&wrong_shard, &lease).is_err());
        let mut wrong_owner = record.clone();
        wrong_owner.owner = Some(NodeId::new("node-b").unwrap());
        assert!(validate_control_record_lease(&wrong_owner, &lease).is_err());
        let mut wrong_epoch = record.clone();
        wrong_epoch.owner_epoch = Some(OwnerEpoch::new(8).unwrap());
        assert!(validate_control_record_lease(&wrong_epoch, &lease).is_err());
        let mut wrong_lease = record;
        wrong_lease.lease_id += 1;
        assert!(validate_control_record_lease(&wrong_lease, &lease).is_err());
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
