use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nokv_control::{CheckpointRef, ControlError, ControlStore, LogRef, LogSegmentRef, ShardId};
use nokv_meta::holtstore::HoltMetadataStore;
use nokv_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, MetadataArchiveConfig,
    MetadataBackupOptions, MetadataBackupOutcome, MetadataBackupWorker, MetadataBackupWorkerState,
    MetadataCheckpointIdentity, MetadataCheckpointStore, MetadataLogArchiveConfig,
    MetadataLogPublicationState, MetadataLogSegment, MetadataLogSegmentPointer,
    MetadataLogSyncConfig, NoKvFs, ObjectGcWorker, ObjectGcWorkerState, METADATA_LOG_ZERO_DIGEST,
};
use nokv_object::{ConfiguredObjectStore, ObjectError};
use nokv_protocol::{request_routing_key, MetadataRpcRequest, RoutingKey};
use nokv_types::{
    DentryName, InodeId, MountId, ShardMap, ShardPrefix, ShardRoute, DEFAULT_SHARD_INDEX,
};

use crate::control::{
    ServerShardAcquisition, ServerShardOwner, ServerShardOwnerOptions,
    ServerShardOwnerRenewalOptions, ServerShardOwnerState,
};
use crate::http;
use crate::metadata::ServerMetadataStore;
use crate::options::{ServerControlOptions, ServerControlStoreOptions, ServerOptions};
use crate::rpc;

const DEFAULT_ROOT_MODE: u32 = 0o755;
const SERVER_CONNECTION_WORKERS: usize = 256;
const SERVER_CONNECTION_QUEUE: usize = 1024;
const DEFAULT_ARCHIVE_KEEP_LAST: usize = 8;

type OpenedControlStore = (Arc<dyn ControlStore>, Vec<ServerShardOwnerOptions>);

enum ServerMetadataBackupWorker {
    Standalone(MetadataBackupWorker),
    Controlled(ControlledMetadataBackupWorker),
}

impl ServerMetadataBackupWorker {
    fn state(&self) -> MetadataBackupWorkerState {
        match self {
            Self::Standalone(worker) => worker.state(),
            Self::Controlled(worker) => worker.state(),
        }
    }
}

/// One metadata shard hosted by this node: its routing identity, its own Holt
/// engine + service, and the per-shard background workers and control-plane
/// owner lease. A single-node dev server holds exactly one (the default shard).
pub(crate) struct ShardSlot {
    /// Stable shard index, encoded into the high bits of every inode this shard
    /// mints. The default/root shard is [`DEFAULT_SHARD_INDEX`].
    shard_index: u16,
    /// The `(mount, path)` subtree this shard owns; used to build the routing map.
    prefix: ShardPrefix,
    service: Arc<NoKvFs<ServerMetadataStore, ConfiguredObjectStore>>,
    owner: Option<ServerShardOwner>,
    renewal: Option<ServerShardOwnerRenewalWorker>,
    object_gc: ObjectGcWorker,
    history_gc: HistoryGcWorker,
    metadata_backup: Option<ServerMetadataBackupWorker>,
    metadata_archive: Option<MetadataArchiveConfig>,
}

impl ShardSlot {
    pub(crate) fn service(&self) -> &NoKvFs<ServerMetadataStore, ConfiguredObjectStore> {
        &self.service
    }

    pub(crate) fn shard_index(&self) -> u16 {
        self.shard_index
    }
}

impl Drop for ShardSlot {
    fn drop(&mut self) {
        // A slot can be dropped before `Server` is fully constructed when a
        // later shard fails startup. Keep lease renewal active while every
        // state-mutating worker drains; otherwise a successor could acquire the
        // shard while an old GC iteration is still committing or deleting.
        // Release only after renewal itself has stopped.
        self.metadata_backup.take();
        self.object_gc.stop();
        self.history_gc.stop();
        self.renewal.take();
        if let Some(owner) = self.owner.as_ref() {
            let _ = owner.release();
        }
    }
}

pub struct Server {
    shards: BTreeMap<ShardId, ShardSlot>,
    shard_map: ShardMap,
    mount: MountId,
    /// Control store handle, retained so a parent shard can self-heal cross-shard
    /// grafts on startup (see [`Server::reconcile_local_grafts`]). `None` in the
    /// single-node dev path (no control plane, no cross-shard grafts).
    control: Option<Arc<dyn ControlStore>>,
    framed_rpc_workers: rpc::RpcWorkerPool,
    #[cfg(test)]
    _test_meta_dir: Option<tempfile::TempDir>,
}

#[derive(Debug)]
pub enum ServerError {
    Io(io::Error),
    Control(ControlError),
    Metadata(MetadError),
    Object(ObjectError),
    /// The addressed shard is not owned by this node — routing resolved a shard
    /// index that no local slot serves. Surfaced to clients as a re-resolve hint.
    NotOwner {
        shard_id: String,
        endpoint: Option<String>,
    },
}

pub fn run(options: ServerOptions) -> Result<(), ServerError> {
    let bind = options.bind;
    let server = Server::open(options)?;
    let listener = TcpListener::bind(bind).map_err(ServerError::Io)?;
    server.serve(listener)
}

/// Reconstruct the metadata namespace from the object-store archive into a fresh
/// local store, without serving. Run this on a replacement node with an empty
/// `--meta-path` before starting the server. Returns a JSON report.
pub fn restore(options: ServerOptions) -> Result<String, ServerError> {
    let objects = options.object.open()?;
    restore_with_objects(options, objects)
}

fn restore_with_objects(
    options: ServerOptions,
    objects: ConfiguredObjectStore,
) -> Result<String, ServerError> {
    let Some(prefix) = options.metadata_checkpoint_archive_prefix.clone() else {
        return Err(ServerError::Metadata(MetadError::InvalidPath(
            "metadata checkpoint archive is not configured \
             (pass --metadata-checkpoint-archive-prefix)"
                .to_owned(),
        )));
    };
    let metadata_state_path = default_metadata_state_path(&options.meta_path);
    let store = HoltMetadataStore::open_file(&metadata_state_path).map_err(MetadError::from)?;
    let metadata = ServerMetadataStore::direct(store);
    // Install into a fresh store: do NOT bootstrap_root, which would create trees
    // the checkpoint install then collides with.
    let service = NoKvFs::new(options.mount, metadata, objects);
    let archive = MetadataArchiveConfig::new(prefix, DEFAULT_ARCHIVE_KEEP_LAST);
    match service.restore_metadata(&archive)? {
        Some(outcome) => {
            let key = format!("\"{}\"", escape_json_string(&outcome.checkpoint_key));
            Ok(format!(
                r#"{{"restored":true,"checkpoint_key":{key},"image_bytes":{},"commit_version":{}}}
"#,
                outcome.image_bytes, outcome.commit_version,
            ))
        }
        None => Ok("{\"restored\":false,\"reason\":\"no archived checkpoint found\"}\n".to_owned()),
    }
}

impl Server {
    pub fn open(options: ServerOptions) -> Result<Self, ServerError> {
        let objects = options.object.open()?;
        let control = open_configured_control(options.control.clone())?;
        Self::open_with_objects(options, objects, control)
    }

    pub fn open_with_control(
        options: ServerOptions,
        control_store: Arc<dyn ControlStore>,
        shard_owners: Vec<ServerShardOwnerOptions>,
    ) -> Result<Self, ServerError> {
        let objects = options.object.open()?;
        Self::open_with_objects(options, objects, Some((control_store, shard_owners)))
    }

    pub(crate) fn open_with_objects(
        options: ServerOptions,
        objects: ConfiguredObjectStore,
        control: Option<OpenedControlStore>,
    ) -> Result<Self, ServerError> {
        let framed_rpc_workers = rpc::RpcWorkerPool::new(
            rpc::default_framed_rpc_worker_count(),
            rpc::default_framed_rpc_queue_capacity(),
        );
        let mut shards: BTreeMap<ShardId, ShardSlot> = BTreeMap::new();
        let control_handle = control.as_ref().map(|(store, _)| Arc::clone(store));

        match control {
            None => {
                // Single-node dev path: one default shard (index 0, prefix "/"),
                // no control plane, no owner lease.
                let shard_id = ShardId::new(format!("mount-{}:/", options.mount.get()));
                let prefix = ShardPrefix::new(options.mount, "/");
                let metadata_state_path = default_metadata_state_path(&options.meta_path);
                let store =
                    HoltMetadataStore::open_file(&metadata_state_path).map_err(MetadError::from)?;
                let metadata = ServerMetadataStore::direct(store);
                let service = Arc::new(NoKvFs::open_existing(
                    options.mount,
                    metadata,
                    objects.clone(),
                    0,
                )?);
                service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
                let metadata_archive =
                    options
                        .metadata_checkpoint_archive_prefix
                        .as_ref()
                        .map(|prefix| {
                            MetadataArchiveConfig::new(prefix.clone(), DEFAULT_ARCHIVE_KEEP_LAST)
                        });
                // An archive makes this state recoverable on another server.
                // Persist the failover fence before resolving a crash-left
                // deletion claim or admitting workers and writers.
                if metadata_archive.is_some() {
                    service.require_failover_durability()?;
                }
                service.recover_object_gc_claim()?;
                if let Some(archive) = metadata_archive.as_ref() {
                    // Publish a fresh CURRENT before serving. Historical
                    // checkpoints may reference objects deleted before the
                    // failover fence was installed.
                    service.backup_metadata(archive)?;
                }
                let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
                let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
                let metadata_backup = metadata_archive.as_ref().map(|archive| {
                    let mut backup = MetadataBackupOptions::new(archive.clone());
                    backup.run_immediately = false;
                    ServerMetadataBackupWorker::Standalone(MetadataBackupWorker::spawn(
                        Arc::clone(&service),
                        backup,
                    ))
                });
                shards.insert(
                    shard_id,
                    ShardSlot {
                        shard_index: DEFAULT_SHARD_INDEX,
                        prefix,
                        service,
                        owner: None,
                        renewal: None,
                        object_gc,
                        history_gc,
                        metadata_backup,
                        metadata_archive,
                    },
                );
            }
            Some((store, shard_owners)) => {
                for shard_owner in shard_owners {
                    let (shard_id, slot) =
                        open_shard_slot(&options, &objects, Arc::clone(&store), shard_owner)?;
                    shards.insert(shard_id, slot);
                }
            }
        }

        // Build the routing map from every non-default subtree shard. The default
        // shard owns "/" implicitly (ShardMap returns DEFAULT_SHARD_INDEX when
        // nothing more specific matches), so it is not entered as a route.
        let routes = shards
            .values()
            .filter(|slot| slot.shard_index != DEFAULT_SHARD_INDEX)
            .map(|slot| ShardRoute {
                shard_index: slot.shard_index,
                prefix: slot.prefix.clone(),
            })
            .collect::<Vec<_>>();
        let shard_map = ShardMap::from_routes(routes);

        Ok(Self {
            shards,
            shard_map,
            mount: options.mount,
            control: control_handle,
            framed_rpc_workers,
            #[cfg(test)]
            _test_meta_dir: None,
        })
    }

    /// Self-heal cross-shard grafts whose PARENT is a shard this server hosts.
    ///
    /// `register_graft` records the subtree-root inode durably in the control
    /// plane BEFORE writing the (reconcilable) parent graft dentry. If that
    /// dentry write was lost (parent-shard crash between the two), the control
    /// record still says the graft should exist. On startup a parent shard reads
    /// `list_shards` and, for every subtree shard with a durable
    /// `subtree_root_inode` whose parent prefix it owns LOCALLY, idempotently
    /// re-creates the graft dentry against its own local service (no RPC — the
    /// write lands on this very shard). Reconciliation is fail-closed: the
    /// server cannot accept reads until every local mutation is represented by
    /// the exact control-published recovery tail.
    pub(crate) fn reconcile_local_grafts(&self) -> Result<(), ServerError> {
        let Some(control) = &self.control else {
            return Ok(());
        };
        let records = control.list_shards()?;
        // Reconciliation ownership is determined from the complete control
        // topology, not merely from the shards hosted by this process. Without
        // the global map, a nested graft whose parent belongs to a remote
        // subtree shard incorrectly falls back to the local default shard.
        let global_shard_map = control_shard_map_for_mount(self.mount, &records)?;
        for record in records {
            let parsed = ShardPrefix::parse(record.shard_id.as_str()).map_err(|err| {
                ServerError::Metadata(MetadError::Codec(format!(
                    "control shard {} has invalid identity: {err}",
                    record.shard_id
                )))
            })?;
            if parsed.mount != self.mount {
                continue;
            }
            if record.shard_index == DEFAULT_SHARD_INDEX {
                continue;
            }
            let Some(subtree_root_raw) = record.subtree_root_inode else {
                continue;
            };
            // Which shard owns the PARENT prefix? If it is not one this server
            // hosts, skip — that parent shard reconciles its own grafts.
            let (parent_prefix, basename) = split_graft_prefix(&record.prefix);
            let parent_index = global_shard_map.route(self.mount, &parent_prefix);
            let Some(parent_slot) = self
                .shards
                .values()
                .find(|slot| slot.shard_index == parent_index)
            else {
                continue;
            };
            let child_inode = InodeId::new(subtree_root_raw).map_err(|err| {
                ServerError::Metadata(MetadError::Codec(format!(
                    "hosted graft {} has invalid subtree root inode: {err}",
                    record.prefix
                )))
            })?;
            if child_inode.local() == 0 || child_inode.shard_index() != record.shard_index {
                return Err(ServerError::Metadata(MetadError::Codec(format!(
                    "hosted graft {} subtree root inode {} does not belong to shard {}",
                    record.prefix, subtree_root_raw, record.shard_index
                ))));
            }
            let name = DentryName::new(basename.into_bytes()).map_err(|err| {
                ServerError::Metadata(MetadError::Codec(format!(
                    "hosted graft {} has invalid basename: {err}",
                    record.prefix
                )))
            })?;
            let reconcile = self.execute_with_shard_visibility(parent_slot, true, || {
                // Resolve the parent while holding the same write gate as the
                // mutation and its control publication.
                let parent_inode = if parent_prefix == "/" {
                    InodeId::root()
                } else {
                    parent_slot
                        .service()
                        .lookup_path(&parent_prefix)?
                        .ok_or(MetadError::NotFound)?
                        .attr
                        .inode
                };
                let create = parent_slot
                    .service()
                    .create_graft(
                        parent_inode,
                        name.clone(),
                        child_inode,
                        DEFAULT_ROOT_MODE,
                        0,
                        0,
                    );
                match create {
                    Ok(entry) => Ok(entry),
                    Err(MetadError::Metadata(err)) if is_predicate_failed(&err) => {
                        // PredicateFailed only proves that a name already
                        // exists. It is idempotent success exclusively when the
                        // existing projection is the exact registered graft;
                        // accepting any other file/dir would silently bind the
                        // control topology to unrelated namespace state.
                        match parent_slot.service().lookup_plus(parent_inode, &name)? {
                            Some(existing)
                                if existing.attr.file_type
                                    == nokv_types::FileType::Directory
                                    && existing.attr.inode == child_inode =>
                            {
                                Ok(existing)
                            }
                            Some(existing) => Err(ServerError::Metadata(
                                MetadError::InvalidPath(format!(
                                    "graft {} conflicts with existing {:?} inode {}",
                                    record.prefix,
                                    existing.attr.file_type,
                                    existing.attr.inode.get()
                                )),
                            )),
                            None => Err(ServerError::Metadata(MetadError::InvalidPath(format!(
                                "graft {} create conflicted but no exact existing target was visible",
                                record.prefix
                            )))),
                        }
                    }
                    Err(err) => Err(ServerError::Metadata(err)),
                }
            });
            match reconcile {
                Ok(_) => {
                    eprintln!(
                        "nokv-server: reconciled missing graft for prefix {} -> inode {}",
                        record.prefix, subtree_root_raw
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub fn serve(self, listener: TcpListener) -> Result<(), ServerError> {
        // Heal any graft whose parent we own before accepting traffic.
        self.reconcile_local_grafts()?;
        let server = Arc::new(self);
        let workers = ConnectionWorkerPool::new(
            Arc::clone(&server),
            SERVER_CONNECTION_WORKERS,
            SERVER_CONNECTION_QUEUE,
        )?;
        for stream in listener.incoming() {
            let stream = stream.map_err(ServerError::Io)?;
            workers.submit(stream)?;
        }
        Ok(())
    }

    /// The default shard's service (index 0) if present, else the first hosted
    /// shard. Test convenience for the common single-shard deployment.
    #[cfg(test)]
    pub(crate) fn service(&self) -> &NoKvFs<ServerMetadataStore, ConfiguredObjectStore> {
        self.default_slot().service()
    }

    fn default_slot(&self) -> &ShardSlot {
        self.shards
            .values()
            .find(|slot| slot.shard_index == DEFAULT_SHARD_INDEX)
            .or_else(|| self.shards.values().next())
            .expect("server always hosts at least one shard")
    }

    /// Resolve the local slot serving `shard_index`. A slot present in the map is
    /// hosted (and therefore served) by this node, whether or not it carries a
    /// control-plane owner lease (single-node dev shards have no owner).
    fn slot_by_index(&self, shard_index: u16) -> Option<&ShardSlot> {
        self.shards
            .values()
            .find(|slot| slot.shard_index == shard_index)
    }

    /// Route a request to the slot that owns its target shard, returning a
    /// `NotOwner` error (with a re-resolve hint) when no local slot serves it.
    pub(crate) fn route(&self, request: &MetadataRpcRequest) -> Result<&ShardSlot, ServerError> {
        let shard_index = match request_routing_key(request) {
            RoutingKey::Path(path) => self.shard_map.route(self.mount, path),
            RoutingKey::Inode(raw) => InodeId::new(raw)
                .map_err(|err| ServerError::Metadata(err.into()))?
                .shard_index(),
            RoutingKey::Default => DEFAULT_SHARD_INDEX,
        };
        self.slot_by_index(shard_index)
            .ok_or_else(|| self.not_owner_error(shard_index))
    }

    fn not_owner_error(&self, shard_index: u16) -> ServerError {
        ServerError::NotOwner {
            shard_id: format!("mount-{}:#shard-{}", self.mount.get(), shard_index),
            endpoint: None,
        }
    }

    pub fn shard_owner_state(&self) -> Result<Option<ServerShardOwnerState>, ServerError> {
        slot_owner_state(self.default_slot())
    }

    /// Renew every hosted shard's owner lease; return the default/sole shard's
    /// resulting owner state.
    pub fn renew_shard_owner_lease(&self) -> Result<Option<ServerShardOwnerState>, ServerError> {
        let default_index = self.default_slot().shard_index;
        let mut default_state = None;
        let mut first_err = None;
        for slot in self.shards.values() {
            let Some(owner) = slot.owner.as_ref() else {
                continue;
            };
            match owner.renew(slot.service()) {
                Ok(state) => {
                    if slot.shard_index == default_index {
                        default_state = Some(state);
                    }
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(default_state)
    }

    fn publish_slot_log_ref(
        slot: &ShardSlot,
        log: LogRef,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        let durable_lsn = log.durable_lsn;
        slot.owner
            .as_ref()
            .map(|owner| {
                owner.mark_serving_with_recovery_refs(slot.service(), None, Some(log), durable_lsn)
            })
            .transpose()
    }

    /// Publish a log ref for the default/sole shard. Retained for callers (and
    /// tests) that target the primary shard directly.
    pub fn publish_shard_owner_log_ref(
        &self,
        log: LogRef,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        Self::publish_slot_log_ref(self.default_slot(), log)
    }

    fn publish_slot_latest_log_ref(
        slot: &ShardSlot,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        let Some(owner) = slot.owner.as_ref() else {
            return Ok(None);
        };
        Self::publish_owner_latest_log_ref(slot.service(), owner)
    }

    pub(crate) fn execute_with_shard_visibility<T>(
        &self,
        slot: &ShardSlot,
        commits_metadata: bool,
        execute: impl FnOnce() -> Result<T, ServerError>,
    ) -> Result<T, ServerError> {
        let Some(owner) = slot.owner.as_ref() else {
            return execute();
        };

        if commits_metadata {
            let _visibility = owner.lock_recovery_publication();
            // Set before raw execution so a partial commit, returned error, or
            // panic cannot release the gate looking clean.
            owner.mark_recovery_dirty();
            let result = execute();
            // A clone/rollback or any other multi-stage mutator may have
            // committed even when its final business result is an error. The
            // publication barrier therefore runs on both result paths and its
            // failure takes precedence, keeping later reads fail-closed.
            Self::publish_owner_latest_log_ref_locked(slot.service(), owner)?;
            return result;
        }

        let visibility = owner.lock_recovery_visibility();
        owner.verify_read_lease(slot.service())?;
        if metadata_log_publication_matches_cached_control(slot.service(), owner) {
            return execute();
        }
        drop(visibility);

        // Dirty readers serialize behind one repairer. Hold the write side
        // through the read itself so another writer cannot create a new local
        // tail between repair and observation.
        let _visibility = owner.lock_recovery_publication();
        Self::publish_owner_latest_log_ref_locked(slot.service(), owner)?;
        execute()
    }

    fn publish_owner_latest_log_ref<M, O>(
        service: &NoKvFs<M, O>,
        owner: &ServerShardOwner,
    ) -> Result<Option<ServerShardOwnerState>, ServerError>
    where
        M: nokv_meta::MetadataStore,
        O: nokv_object::ObjectStore,
    {
        // Snapshot selection and the control CAS are one owner-local
        // publication critical section. Otherwise an older request can pause
        // after taking its snapshot, let a newer request publish, and then
        // overwrite the control LogRef with the older chain.
        let _publication = owner.lock_recovery_publication();
        Self::publish_owner_latest_log_ref_locked(service, owner)
    }

    fn publish_owner_latest_log_ref_locked<M, O>(
        service: &NoKvFs<M, O>,
        owner: &ServerShardOwner,
    ) -> Result<Option<ServerShardOwnerState>, ServerError>
    where
        M: nokv_meta::MetadataStore,
        O: nokv_object::ObjectStore,
    {
        service.verify_owner_lease()?;
        service.flush_sync_metadata_log_for_publication()?;
        let Some(mut publication) = service.sync_metadata_log_publication_state() else {
            return Err(ServerError::Metadata(MetadError::Codec(
                "control-owned shard has no synchronous metadata log publication state".to_owned(),
            )));
        };

        if let Some(cached) = owner.published_recovery_state() {
            prune_control_covered_log_segments(service, &publication, &cached)?;
            publication = service
                .sync_metadata_log_publication_state()
                .expect("enabled metadata log remains enabled under publication gate");
            if metadata_log_publication_matches_control(&publication, &cached) {
                owner.mark_recovery_clean();
                return Ok(Some(cached));
            }
        }

        match publish_metadata_log_snapshot_locked(service, owner, &publication) {
            Ok(state) => complete_metadata_log_publication(service, owner, state).map(Some),
            Err(_) => {
                // A checkpoint/log CAS may have committed and lost its ACK, or
                // another publication by this same owner may have advanced the
                // authoritative checkpoint while local covered pointers remain.
                // Dirty repair alone may perform this remote validation.
                let authoritative = owner.renew(service)?;
                prune_control_covered_log_segments(service, &publication, &authoritative)?;
                let repaired = service
                    .sync_metadata_log_publication_state()
                    .expect("enabled metadata log remains enabled under publication gate");
                if metadata_log_publication_matches_control(&repaired, &authoritative) {
                    owner.mark_recovery_clean();
                    return Ok(Some(authoritative));
                }
                let state = publish_metadata_log_snapshot_locked(service, owner, &repaired)?;
                complete_metadata_log_publication(service, owner, state).map(Some)
            }
        }
    }

    /// Publish the latest sync-log ref of the default/sole shard. Retained for
    /// callers (and tests) that target the primary shard directly.
    pub fn publish_latest_metadata_log_ref(
        &self,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        Self::publish_slot_latest_log_ref(self.default_slot())
    }

    #[cfg(test)]
    fn shard_owner_renewal_state(&self) -> Option<ServerShardOwnerRenewalWorkerState> {
        self.default_slot()
            .renewal
            .as_ref()
            .map(ServerShardOwnerRenewalWorker::state)
    }

    pub(crate) fn framed_rpc_workers(&self) -> &rpc::RpcWorkerPool {
        &self.framed_rpc_workers
    }

    /// Readiness is the same safety claim as a controlled strong read: every
    /// hosted owner lease is live and the exact local recovery tail is already
    /// represented in control. A dirty slot may repair under its publication
    /// write gate; publication failure keeps the server unready. Standalone
    /// slots pass through unchanged.
    pub(crate) fn check_readiness(&self) -> Result<(), ServerError> {
        for slot in self.shards.values() {
            self.execute_with_shard_visibility(slot, false, || Ok(()))?;
        }
        Ok(())
    }

    pub fn stats_json(&self) -> String {
        let ready = self.check_readiness().is_ok();
        let slot = self.default_slot();
        let service = slot.service();
        let objects = service.object_stats();
        let metadata = service.metadata_store_stats();
        let metadata_service = service.metadata_service_stats();
        let object_gc = slot.object_gc.state();
        let history_gc = slot.history_gc.state();
        // Stats are an operational safety surface. If the persisted marker
        // cannot be read or decoded, report the failover requirement as active
        // rather than presenting an unsafe deployment as unconstrained.
        let failover_durability_required =
            service.failover_durability_is_required().unwrap_or(true);
        format!(
            "{{\"ready\":{},\"block_cache_enabled\":{},\"object_puts\":{},\"object_put_bytes\":{},\"object_gets\":{},\"object_get_bytes\":{},\"coalesced_gets\":{},\"coalesced_get_bytes\":{},\"cache_hits\":{},\"cache_hit_bytes\":{},\"prefetch_enqueued\":{},\"prefetch_dropped\":{},\"prefetch_completed\":{},\"prefetch_failed\":{},\"prefetch_object_gets\":{},\"prefetch_object_get_bytes\":{},\"prefetch_cache_hits\":{},\"prefetch_cache_hit_bytes\":{},\"read_plan_cache_hits\":{},\"read_plan_cache_misses\":{},\"object_writeback_enqueued\":{},\"object_writeback_inline\":{},\"object_writeback_completed\":{},\"object_writeback_failed\":{},\"object_writeback_staged_bytes\":{},\"object_writeback_uploaded_bytes\":{},\"object_writeback_queue_wait_ns\":{},\"object_writeback_queue_max_wait_ns\":{},\"object_writeback_upload_ns\":{},\"object_writeback_upload_max_ns\":{},\"object_writeback_collect_ns\":{},\"object_writeback_digest_ns\":{},\"object_writeback_store_put_ns\":{},\"object_writeback_cache_put_ns\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"metadata_store\":{},\"metadata_service\":{},\"shard_owner\":{},\"object_gc\":{},\"history_gc\":{},\"metadata_backup\":{}}}\n",
            ready,
            service.block_cache_enabled(),
            objects.object_puts,
            objects.object_put_bytes,
            objects.object_gets,
            objects.object_get_bytes,
            objects.coalesced_gets,
            objects.coalesced_get_bytes,
            objects.cache_hits,
            objects.cache_hit_bytes,
            objects.prefetch_enqueued,
            objects.prefetch_dropped,
            objects.prefetch_completed,
            objects.prefetch_failed,
            objects.prefetch_object_gets,
            objects.prefetch_object_get_bytes,
            objects.prefetch_cache_hits,
            objects.prefetch_cache_hit_bytes,
            objects.read_plan_cache_hits,
            objects.read_plan_cache_misses,
            objects.object_writeback_enqueued,
            objects.object_writeback_inline,
            objects.object_writeback_completed,
            objects.object_writeback_failed,
            objects.object_writeback_staged_bytes,
            objects.object_writeback_uploaded_bytes,
            objects.object_writeback_queue_wait_ns,
            objects.object_writeback_queue_max_wait_ns,
            objects.object_writeback_upload_ns,
            objects.object_writeback_upload_max_ns,
            objects.object_writeback_collect_ns,
            objects.object_writeback_digest_ns,
            objects.object_writeback_store_put_ns,
            objects.object_writeback_cache_put_ns,
            objects.manifest_chunks,
            objects.manifest_blocks,
            metadata_store_json(&metadata),
            metadata_service_json(&metadata_service),
            self.shard_owner_json(),
            object_gc_json(&object_gc, failover_durability_required),
            history_gc_json(&history_gc),
            self.metadata_backup_json(),
        )
    }

    /// Run GC across every hosted shard, summing the per-shard outcomes.
    pub fn run_manual_gc(&self, limit: usize) -> Result<String, ServerError> {
        let mut object = nokv_meta::PendingObjectCleanupOutcome::default();
        let mut history = nokv_meta::HistoryPruneOutcome::default();
        for slot in self.shards.values() {
            let service = slot.service();
            let object_outcome = service.cleanup_pending_objects(limit)?;
            let history_outcome = service.cleanup_history(limit)?;
            object.scanned += object_outcome.scanned;
            object.blocked_by_snapshots += object_outcome.blocked_by_snapshots;
            object.blocked_by_read_leases += object_outcome.blocked_by_read_leases;
            object.blocked_by_failover_durability += object_outcome.blocked_by_failover_durability;
            object.attempted += object_outcome.attempted;
            object.deleted += object_outcome.deleted;
            object.missing += object_outcome.missing;
            object.records_removed += object_outcome.records_removed;
            object.snapshot_reap.scanned += object_outcome.snapshot_reap.scanned;
            object.snapshot_reap.expired_candidates +=
                object_outcome.snapshot_reap.expired_candidates;
            object.snapshot_reap.reaped += object_outcome.snapshot_reap.reaped;
            object.snapshot_reap.conflicted += object_outcome.snapshot_reap.conflicted;
            history.scanned += history_outcome.scanned;
            history.removed += history_outcome.removed;
            history.retained_by_snapshots += history_outcome.retained_by_snapshots;
        }
        Ok(format!(
            r#"{{"object_gc":{{"scanned":{},"blocked_by_snapshots":{},"blocked_by_read_leases":{},"blocked_by_failover_durability":{},"attempted":{},"deleted":{},"missing":{},"records_removed":{},"snapshot_reap":{{"scanned":{},"expired_candidates":{},"reaped":{},"conflicted":{}}}}},"history_gc":{{"scanned":{},"removed":{},"retained_by_snapshots":{}}}}}
"#,
            object.scanned,
            object.blocked_by_snapshots,
            object.blocked_by_read_leases,
            object.blocked_by_failover_durability,
            object.attempted,
            object.deleted,
            object.missing,
            object.records_removed,
            object.snapshot_reap.scanned,
            object.snapshot_reap.expired_candidates,
            object.snapshot_reap.reaped,
            object.snapshot_reap.conflicted,
            history.scanned,
            history.removed,
            history.retained_by_snapshots,
        ))
    }

    /// Checkpoint every hosted shard's metadata engine.
    pub fn run_manual_checkpoint(&self) -> Result<String, ServerError> {
        for slot in self.shards.values() {
            slot.service()
                .metadata_store()
                .checkpoint()
                .map_err(|err| ServerError::Metadata(MetadError::from(err)))?;
        }
        Ok("{\"checkpointed\":true}\n".to_owned())
    }

    /// Back up the default/sole shard's metadata to its archive and publish the
    /// resulting checkpoint ref.
    pub fn run_manual_backup(&self) -> Result<String, ServerError> {
        let slot = self.default_slot();
        let Some(archive) = slot.metadata_archive.as_ref() else {
            return Err(ServerError::Metadata(MetadError::InvalidPath(
                "metadata checkpoint archive is not configured \
                 (start the server with --metadata-checkpoint-archive-prefix)"
                    .to_owned(),
            )));
        };
        let outcome = match slot.owner.as_ref() {
            Some(owner) => run_controlled_metadata_backup_once(slot.service(), owner, archive)?,
            None => slot.service().backup_metadata(archive)?,
        };
        let key = format!("\"{}\"", escape_json_string(&outcome.checkpoint_key));
        Ok(format!(
            r#"{{"checkpoint_key":{key},"image_bytes":{},"commit_version":{},"pruned":{},"log_segments_pruned":{},"log_segment_objects_deleted":{},"log_segment_objects_missing":{},"log_segment_delete_failures":{}}}
"#,
            outcome.image_bytes,
            outcome.commit_version,
            outcome.pruned,
            outcome.log_segments_pruned,
            outcome.log_segment_objects_deleted,
            outcome.log_segment_objects_missing,
            outcome.log_segment_delete_failures,
        ))
    }

    /// Fsck dangling blocks across every hosted shard, summing the report.
    pub fn run_fsck(&self) -> Result<String, ServerError> {
        let mut inodes_scanned = 0_usize;
        let mut files_scanned = 0_usize;
        let mut blocks_checked = 0_usize;
        let mut dangling_entries = Vec::new();
        for slot in self.shards.values() {
            let report = slot.service().fsck_dangling_blocks(0)?;
            inodes_scanned += report.inodes_scanned;
            files_scanned += report.files_scanned;
            blocks_checked += report.blocks_checked;
            for entry in &report.dangling {
                dangling_entries.push(format!(
                    "{{\"inode\":{},\"generation\":{},\"object_key\":\"{}\"}}",
                    entry.inode,
                    entry.generation,
                    escape_json_string(&entry.object_key)
                ));
            }
        }
        let dangling_count = dangling_entries.len();
        let dangling = dangling_entries.join(",");
        Ok(format!(
            r#"{{"inodes_scanned":{},"files_scanned":{},"blocks_checked":{},"dangling_count":{},"dangling":[{}]}}
"#,
            inodes_scanned, files_scanned, blocks_checked, dangling_count, dangling,
        ))
    }

    fn metadata_backup_json(&self) -> String {
        match &self.default_slot().metadata_backup {
            Some(worker) => {
                let state = worker.state();
                format!(
                    "{{\"enabled\":true,\"iterations\":{},\"last_error\":{}}}",
                    state.iterations,
                    json_string_or_null(state.last_error.as_deref())
                )
            }
            None => "{\"enabled\":false}".to_owned(),
        }
    }

    fn shard_owner_json(&self) -> String {
        let renewal = self.shard_owner_renewal_json();
        match self.shard_owner_state() {
            Ok(Some(state)) => format!(
                "{{\"enabled\":true,\"shard_id\":\"{}\",\"node_id\":\"{}\",\"epoch\":{},\"lease_id\":{},\"state\":\"{}\",\"durable_lsn\":{},\"log\":{},\"renewal\":{renewal}}}",
                escape_json_string(state.shard_id.as_str()),
                escape_json_string(state.node_id.as_str()),
                state.epoch,
                state.lease_id,
                shard_state_name(state.state),
                state.durable_lsn,
                log_ref_json(state.log.as_ref()),
            ),
            Ok(None) => "{\"enabled\":false}".to_owned(),
            Err(err) => format!(
                "{{\"enabled\":true,\"error\":\"{}\",\"renewal\":{renewal}}}",
                escape_json_string(&err.to_string()),
            ),
        }
    }

    fn shard_owner_renewal_json(&self) -> String {
        match &self.default_slot().renewal {
            Some(worker) => {
                let state = worker.state();
                format!(
                    "{{\"enabled\":true,\"iterations\":{},\"last_error\":{}}}",
                    state.iterations,
                    json_string_or_null(state.last_error.as_deref()),
                )
            }
            None => "{\"enabled\":false}".to_owned(),
        }
    }
}

fn metadata_log_ref(snapshot: &nokv_meta::MetadataLogSyncSnapshot) -> Option<LogRef> {
    (!snapshot.segments.is_empty()).then(|| LogRef {
        segments: snapshot
            .segments
            .iter()
            .map(|segment| LogSegmentRef {
                segment_key: segment.segment_key.clone(),
                first_lsn: segment.first_lsn,
                last_lsn: segment.last_lsn,
                digest: hex_digest(&segment.last_digest),
            })
            .collect(),
        durable_lsn: snapshot.durable_lsn,
        digest: hex_digest(&snapshot.last_digest),
    })
}

fn metadata_log_publication_matches_control(
    publication: &MetadataLogPublicationState,
    control: &ServerShardOwnerState,
) -> bool {
    if publication.has_pending_segment
        || publication.has_unresolved_commit_group
        || control.state != nokv_control::ShardState::Serving
        || control.durable_lsn != publication.snapshot.durable_lsn
    {
        return false;
    }
    match metadata_log_ref(&publication.snapshot) {
        Some(log) => control.log.as_ref() == Some(&log),
        None => {
            control.log.is_none()
                && control.checkpoint.as_ref().is_some_and(|checkpoint| {
                    checkpoint.lsn == publication.snapshot.durable_lsn
                        && checkpoint.digest == hex_digest(&publication.snapshot.last_digest)
                })
        }
    }
}

fn metadata_log_publication_matches_cached_control<M, O>(
    service: &NoKvFs<M, O>,
    owner: &ServerShardOwner,
) -> bool
where
    M: nokv_meta::MetadataStore,
    O: nokv_object::ObjectStore,
{
    if owner.recovery_is_dirty() {
        return false;
    }
    let Some(publication) = service.sync_metadata_log_publication_state() else {
        return false;
    };
    owner
        .published_recovery_state()
        .as_ref()
        .is_some_and(|control| metadata_log_publication_matches_control(&publication, control))
}

fn publish_metadata_log_snapshot_locked<M, O>(
    service: &NoKvFs<M, O>,
    owner: &ServerShardOwner,
    publication: &MetadataLogPublicationState,
) -> Result<ServerShardOwnerState, ServerError>
where
    M: nokv_meta::MetadataStore,
    O: nokv_object::ObjectStore,
{
    if publication.has_pending_segment || publication.has_unresolved_commit_group {
        return Err(ServerError::Metadata(MetadError::SyncLogArchiveFailed {
            committed: true,
            message: "metadata log publication state remains unresolved after flush".to_owned(),
        }));
    }
    let snapshot = &publication.snapshot;
    match metadata_log_ref(snapshot) {
        Some(log) => owner.mark_serving_with_recovery_refs_locked(
            service,
            None,
            Some(log),
            snapshot.durable_lsn,
        ),
        None => {
            owner.mark_serving_with_recovery_refs_locked(service, None, None, snapshot.durable_lsn)
        }
    }
}

fn prune_control_covered_log_segments<M, O>(
    service: &NoKvFs<M, O>,
    publication: &MetadataLogPublicationState,
    control: &ServerShardOwnerState,
) -> Result<(), ServerError>
where
    M: nokv_meta::MetadataStore,
    O: nokv_object::ObjectStore,
{
    let Some(checkpoint) = control.checkpoint.as_ref() else {
        return Ok(());
    };
    let Some(last_covered) = publication
        .snapshot
        .segments
        .iter()
        .take_while(|segment| segment.last_lsn <= checkpoint.lsn)
        .last()
    else {
        return Ok(());
    };
    let expected_digest = parse_hex_digest(&checkpoint.digest)?;
    if last_covered.last_lsn != checkpoint.lsn || last_covered.last_digest != expected_digest {
        return Err(ServerError::Metadata(MetadError::Codec(format!(
            "authoritative checkpoint tail {}:{} does not match the local covered log boundary",
            checkpoint.lsn, checkpoint.digest
        ))));
    }
    let outcome = service.prune_sync_metadata_log_segments(checkpoint.lsn);
    if outcome.delete_failures > 0 {
        eprintln!(
            "nokv-server: {} control-covered metadata log segment object(s) leaked during publication repair",
            outcome.delete_failures
        );
    }
    Ok(())
}

fn complete_metadata_log_publication<M, O>(
    service: &NoKvFs<M, O>,
    owner: &ServerShardOwner,
    control: ServerShardOwnerState,
) -> Result<ServerShardOwnerState, ServerError>
where
    M: nokv_meta::MetadataStore,
    O: nokv_object::ObjectStore,
{
    let publication = service
        .sync_metadata_log_publication_state()
        .ok_or_else(|| {
            ServerError::Metadata(MetadError::Codec(
                "synchronous metadata log disappeared during publication".to_owned(),
            ))
        })?;
    prune_control_covered_log_segments(service, &publication, &control)?;
    let final_publication = service
        .sync_metadata_log_publication_state()
        .expect("enabled metadata log remains enabled under publication gate");
    if !metadata_log_publication_matches_control(&final_publication, &control) {
        return Err(ServerError::Metadata(MetadError::Codec(
            "authoritative recovery refs do not exactly cover the local metadata log tail"
                .to_owned(),
        )));
    }
    owner.mark_recovery_clean();
    Ok(control)
}

fn slot_owner_state(slot: &ShardSlot) -> Result<Option<ServerShardOwnerState>, ServerError> {
    slot.owner.as_ref().map(ServerShardOwner::state).transpose()
}

fn open_configured_control(
    options: Option<ServerControlOptions>,
) -> Result<Option<OpenedControlStore>, ServerError> {
    let Some(options) = options else {
        return Ok(None);
    };
    let store = match options.store {
        ServerControlStoreOptions::Etcd(options) => open_etcd_control_store(options)?,
    };
    Ok(Some((store, vec![options.shard_owner])))
}

/// Derive a shard's path prefix from its `mount-<n>:<path>` id, defaulting to `/`.
/// Mirrors the control store's own derivation so a server-side `register_shard`
/// produces the same prefix the control record would otherwise carry.
fn shard_prefix_from_id(shard_id: &str) -> String {
    shard_id
        .split_once(':')
        .map(|(_, path)| path)
        .filter(|path| path.starts_with('/'))
        .unwrap_or("/")
        .to_owned()
}

/// Split a graft prefix (e.g. `/dataset` or `/a/b`) into its parent prefix and
/// basename. A top-level prefix has parent `/`. Mirrors the client's
/// `rpc_parent_and_name` so server-side and client-side reconcile agree.
fn split_graft_prefix(prefix: &str) -> (String, String) {
    let trimmed = prefix.trim_end_matches('/');
    match trimmed.rsplit_once('/') {
        Some(("", basename)) => ("/".to_owned(), basename.to_owned()),
        Some((parent, basename)) => (parent.to_owned(), basename.to_owned()),
        None => ("/".to_owned(), trimmed.to_owned()),
    }
}

/// Build the authoritative longest-prefix routing table for one mount from
/// control-plane records. Every record for the mount must carry an identity,
/// prefix, and default/non-default index that agree exactly; startup must not
/// guess around malformed topology.
fn control_shard_map_for_mount(
    mount: MountId,
    records: &[nokv_control::ShardRecord],
) -> Result<ShardMap, ServerError> {
    let mut routes = Vec::new();
    for record in records {
        let parsed = ShardPrefix::parse(record.shard_id.as_str()).map_err(|err| {
            ServerError::Metadata(MetadError::Codec(format!(
                "control shard {} has invalid identity: {err}",
                record.shard_id
            )))
        })?;
        if parsed.mount != mount {
            continue;
        }
        if parsed.path != record.prefix {
            return Err(ServerError::Metadata(MetadError::Codec(format!(
                "control shard {} prefix {:?} does not match its identity prefix {:?}",
                record.shard_id, record.prefix, parsed.path
            ))));
        }
        if record.shard_index == DEFAULT_SHARD_INDEX {
            if parsed.path != "/" {
                return Err(ServerError::Metadata(MetadError::Codec(format!(
                    "control shard {} uses the default shard index for non-root prefix {:?}",
                    record.shard_id, record.prefix
                ))));
            }
            continue;
        }
        if parsed.path == "/" {
            return Err(ServerError::Metadata(MetadError::Codec(format!(
                "control shard {} uses non-default index {} for the root prefix",
                record.shard_id, record.shard_index
            ))));
        }
        routes.push(ShardRoute {
            shard_index: record.shard_index,
            prefix: parsed,
        });
    }
    Ok(ShardMap::from_routes(routes))
}

/// Whether a metadata backend error is a predicate failure — the idempotent
/// "graft dentry already exists" signal during reconcile.
fn is_predicate_failed(err: &nokv_meta::MetadataError) -> bool {
    matches!(err, nokv_meta::MetadataError::PredicateFailed)
}

/// Sanitize a shard id into a filesystem-safe directory component (each shard's
/// local Holt engine lives in its own subdirectory under `--meta-path`).
fn sanitize_shard_id(shard_id: &str) -> String {
    shard_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

/// Join an archive prefix with a sanitized shard id so each shard's checkpoint /
/// shared-log archive is isolated from every other shard's.
fn shard_archive_prefix(prefix: &str, sanitized_shard_id: &str) -> String {
    format!("{}/{}", prefix.trim_end_matches('/'), sanitized_shard_id)
}

fn checkpoint_ref_for_backup(outcome: &MetadataBackupOutcome) -> CheckpointRef {
    CheckpointRef {
        object_key: outcome.checkpoint_key.clone(),
        lsn: outcome.log_lsn,
        image_bytes: outcome.image_bytes,
        image_digest: outcome.image_digest.clone(),
        digest: hex_digest(&outcome.log_digest),
    }
}

fn checkpoint_identity(checkpoint: &CheckpointRef) -> MetadataCheckpointIdentity {
    MetadataCheckpointIdentity {
        checkpoint_key: checkpoint.object_key.clone(),
        image_bytes: checkpoint.image_bytes,
        image_digest: checkpoint.image_digest.clone(),
    }
}

/// Publish one controlled checkpoint without touching standalone `CURRENT`.
/// The old control ref remains authoritative through every upload failure and
/// is pruned only after the new exact identity wins the owner-lease CAS.
fn run_controlled_metadata_backup_once<M, O>(
    service: &NoKvFs<M, O>,
    owner: &ServerShardOwner,
    archive: &MetadataArchiveConfig,
) -> Result<MetadataBackupOutcome, ServerError>
where
    M: nokv_meta::MetadataStore + MetadataCheckpointStore,
    O: nokv_object::ObjectStore,
{
    let _publication = owner.lock_recovery_publication();
    service.verify_owner_lease()?;
    let prior = owner.renew(service)?;
    let mut outcome = service.prepare_immutable_metadata_backup(archive)?;
    let published = owner.mark_serving_with_recovery_refs_locked(
        service,
        Some(checkpoint_ref_for_backup(&outcome)),
        None,
        outcome.log_lsn,
    )?;
    let log_prune = service.prune_sync_metadata_log_segments(outcome.log_lsn);
    outcome.log_segments_pruned = log_prune.pointers_pruned;
    outcome.log_segment_objects_deleted = log_prune.objects_deleted;
    outcome.log_segment_objects_missing = log_prune.objects_missing;
    outcome.log_segment_delete_failures = log_prune.delete_failures;
    if log_prune.delete_failures > 0 {
        // The control CAS already made the checkpoint authoritative. Cleanup
        // failure is observable but cannot turn publication into an error: a
        // caller retry would publish another checkpoint and obscure success.
        eprintln!(
            "nokv-server: {} covered metadata log segment object(s) leaked after checkpoint publication",
            log_prune.delete_failures
        );
    }
    let final_publication = service
        .sync_metadata_log_publication_state()
        .ok_or_else(|| {
            ServerError::Metadata(MetadError::Codec(
                "control-owned shard lost synchronous metadata log state during checkpoint publication"
                    .to_owned(),
            ))
        })?;
    if !metadata_log_publication_matches_control(&final_publication, &published) {
        return Err(ServerError::Metadata(MetadError::Codec(
            "published checkpoint does not exactly cover the local metadata log tail".to_owned(),
        )));
    }
    owner.mark_recovery_clean();

    if let Some(previous) = prior.checkpoint.as_ref() {
        if previous.object_key != outcome.checkpoint_key && !previous.image_digest.is_empty() {
            match service.prune_immutable_metadata_backup(archive, &checkpoint_identity(previous)) {
                Ok(pruned) => outcome.pruned = pruned,
                Err(err) => {
                    // The control CAS already made the new image authoritative;
                    // pruning failure is a safe leak and must not invite a retry
                    // that could obscure the successful publication.
                    eprintln!("nokv-server: controlled metadata checkpoint prune skipped: {err}");
                }
            }
        }
    }
    Ok(outcome)
}

/// Build one [`ShardSlot`] from its control-plane owner options: open the shard's
/// own Holt engine, derive its index/prefix from the (registered) control record,
/// acquire the owner lease, restore on failover, install durability, recover a
/// crash-left object-GC claim, mark serving, and spawn the background workers.
fn open_shard_slot(
    options: &ServerOptions,
    objects: &ConfiguredObjectStore,
    store: Arc<dyn ControlStore>,
    shard_owner: ServerShardOwnerOptions,
) -> Result<(ShardId, ShardSlot), ServerError> {
    let shard_id = shard_owner.shard_id.clone();
    let sanitized = sanitize_shard_id(shard_id.as_str());
    // Every control-owned generation can eventually be replaced by another
    // server. Require the shared log before even registering/reading control
    // state, creating a local directory, or touching object storage; otherwise
    // a Fresh owner could ACK writes above its checkpoint that a later owner
    // has no configured way to recover.
    let shared_log_options = shard_owner.shared_log.clone().ok_or_else(|| {
        ControlError::InvalidOptions(format!(
            "control-owned shard {} requires synchronous shared_log configuration",
            shard_id.as_str()
        ))
    })?;
    let shared_log_archive = MetadataLogArchiveConfig::new(shard_archive_prefix(
        &shared_log_options.archive_prefix,
        &sanitized,
    ));

    // When this owner declares its own shard index (a multi-process fleet, where
    // no separate registration step has seeded identity), register the shard's
    // (prefix, index) before reading the record. register_shard is idempotent and
    // only (re)assigns identity while the shard is unowned, so a live owner keeps
    // its routing. The prefix is derived from the shard id (`mount-N:<path>`).
    if let Some(shard_index) = shard_owner.shard_index {
        let prefix = shard_prefix_from_id(shard_id.as_str());
        store.register_shard(shard_id.clone(), prefix, shard_index)?;
    }

    // The shard's stable identity (index + prefix) must already be registered.
    // ensure_shard returns (creating if absent) the record so we read its index
    // and prefix; for the default shard the derived prefix is "/" and index 0.
    let record = store.ensure_shard(shard_id.clone())?;
    let is_failover = matches!(
        shard_owner.acquisition,
        ServerShardAcquisition::Failover { .. }
    );
    // Derive and validate the exact recovery namespace before creating a local
    // Holt directory, acquiring/bumping the epoch, or touching object storage.
    let metadata_archive = options
        .metadata_checkpoint_archive_prefix
        .as_ref()
        .map(|prefix| {
            MetadataArchiveConfig::new(
                shard_archive_prefix(prefix, &sanitized),
                DEFAULT_ARCHIVE_KEEP_LAST,
            )
        });
    if metadata_archive.is_none() {
        let message = if is_failover {
            "shard failover recovery requires a metadata checkpoint archive"
        } else {
            "control-owned shard requires a metadata checkpoint archive"
        };
        return Err(ServerError::Metadata(MetadError::InvalidPath(
            message.to_owned(),
        )));
    }
    if is_failover {
        let checkpoint = record.checkpoint.as_ref().ok_or_else(|| {
            ControlError::InvalidOptions(format!(
                "shard {} has no durable checkpoint identity for failover",
                shard_id.as_str()
            ))
        })?;
        metadata_archive
            .as_ref()
            .expect("archive presence checked above")
            .validate_controlled_checkpoint_identity(&checkpoint_identity(checkpoint))?;
        // The image identity validation above covers object key, proof-key
        // derivation, size, and image digest. Validate the logical tail digest
        // here as well so restore cannot discover it only after acquisition.
        parse_hex_digest(&checkpoint.digest)?;
        if let Some(log) = record.log.as_ref() {
            for segment in &log.segments {
                shared_log_archive.validate_segment_key(&segment.segment_key)?;
            }
        }
    }
    let shard_index = record.shard_index;
    let prefix = ShardPrefix::parse(&format!("mount-{}:{}", options.mount.get(), record.prefix))
        .unwrap_or_else(|_| ShardPrefix::new(options.mount, record.prefix.clone()));

    // Per-shard Holt engine, isolated under {meta-path}/{sanitized-shard-id}/.
    let shard_meta_dir = options.meta_path.join(&sanitized);
    if let Err(err) = std::fs::create_dir_all(&shard_meta_dir) {
        if err.kind() != io::ErrorKind::AlreadyExists {
            return Err(ServerError::Io(err));
        }
    }
    let metadata_state_path = default_metadata_state_path(&shard_meta_dir);
    let holt = HoltMetadataStore::open_file(&metadata_state_path).map_err(MetadError::from)?;
    let metadata = ServerMetadataStore::direct(holt);

    let service = if is_failover {
        Arc::new(
            NoKvFs::new(options.mount, metadata, objects.clone()).with_shard_index(shard_index),
        )
    } else {
        Arc::new(NoKvFs::open_existing(
            options.mount,
            metadata,
            objects.clone(),
            shard_index,
        )?)
    };

    let renewal_options = shard_owner.renewal;
    let owner = ServerShardOwner::acquire(store, shard_owner, service.as_ref())?;
    // Renewal starts immediately after acquisition and remains the same worker
    // throughout restore, checkpoint export/upload, and publication. A large
    // checkpoint must not consume the whole lease before the shard can serve.
    let mut renewal = renewal_options.map(|renewal| {
        ServerShardOwnerRenewalWorker::spawn(Arc::clone(&service), owner.clone(), renewal)
    });

    let startup = (|| -> Result<(), ServerError> {
        let restored_from_control = if is_failover {
            match metadata_archive.as_ref() {
                Some(archive) => {
                    if !restore_shard_owner_recovery_refs(
                        service.as_ref(),
                        &owner,
                        archive,
                        &shared_log_archive,
                    )? {
                        return Err(ServerError::Metadata(MetadError::NotFound));
                    }
                    true
                }
                None => unreachable!("failover without a checkpoint archive is rejected above"),
            }
        } else {
            false
        };
        if !restored_from_control {
            service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
        }

        // Every control-owned shard can be recovered by a different server.
        // Install this policy before claim recovery, readiness, or GC workers.
        service.require_failover_durability()?;
        service.recover_object_gc_claim()?;
        if is_failover {
            // Recovery proved the durable claim is Open. Rewrite it at a fresh
            // version before enabling writers so every prepared upload minted
            // by the failed owner is fenced from publishing after takeover.
            service.rotate_object_gc_claim_for_failover()?;
        }

        let state = owner.state()?;
        let last_digest = control_recovery_digest(&state)?;
        let inherited_segments = inherited_log_segments(&state)?;
        // Isolate each shard's shared-log archive under its own prefix.
        service.enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                shared_log_archive.prefix.clone(),
                state.shard_id.as_str(),
                state.epoch,
                state.durable_lsn,
                last_digest,
            )
            .with_segments(inherited_segments),
        )?;
        // A fresh archive supersedes any historical CURRENT that could retain
        // object references from before the deletion fence. The immutable image
        // is prepared first; only the owner-lease CAS makes it authoritative.
        if let Some(archive) = metadata_archive.as_ref() {
            run_controlled_metadata_backup_once(service.as_ref(), &owner, archive)?;
        } else {
            owner.mark_serving(service.as_ref())?;
        }
        Ok(())
    })();
    if let Err(err) = startup {
        // Stop before release so an in-flight renew cannot reassert a lease that
        // startup has abandoned. Stale release is best-effort after failover.
        renewal.take();
        let _ = owner.release();
        return Err(err);
    }

    let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
    let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
    let metadata_backup = metadata_archive.as_ref().map(|archive| {
        let mut backup = MetadataBackupOptions::new(archive.clone());
        // Back up on the interval, not on every boot (avoids startup stalls).
        backup.run_immediately = false;
        ServerMetadataBackupWorker::Controlled(ControlledMetadataBackupWorker::spawn(
            Arc::clone(&service),
            owner.clone(),
            backup,
        ))
    });

    Ok((
        shard_id,
        ShardSlot {
            shard_index,
            prefix,
            service,
            owner: Some(owner),
            renewal,
            object_gc,
            history_gc,
            metadata_backup,
            metadata_archive,
        },
    ))
}

#[cfg(feature = "etcd")]
fn open_etcd_control_store(
    options: nokv_control::EtcdControlStoreOptions,
) -> Result<Arc<dyn ControlStore>, ServerError> {
    Ok(Arc::new(nokv_control::EtcdControlStore::connect(options)?))
}

#[cfg(not(feature = "etcd"))]
fn open_etcd_control_store(
    options: nokv_control::EtcdControlStoreOptions,
) -> Result<Arc<dyn ControlStore>, ServerError> {
    let _ = options;
    Err(ControlError::InvalidOptions(
        "nokv-server was built without the etcd control feature".to_owned(),
    )
    .into())
}

struct ControlledMetadataBackupWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    state: Arc<Mutex<MetadataBackupWorkerState>>,
    handle: Option<JoinHandle<()>>,
}

impl ControlledMetadataBackupWorker {
    fn spawn(
        service: Arc<NoKvFs<ServerMetadataStore, ConfiguredObjectStore>>,
        owner: ServerShardOwner,
        options: MetadataBackupOptions,
    ) -> Self {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = Arc::new(Mutex::new(MetadataBackupWorkerState::default()));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let interval = options.interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            if options.run_immediately {
                run_controlled_metadata_backup_worker_once(
                    &service,
                    &owner,
                    &options.config,
                    &worker_state,
                );
            }
            loop {
                let (lock, cvar) = &*worker_stop;
                let stopped = match lock.lock() {
                    Ok(stopped) => stopped,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                let (stopped, _) = match cvar.wait_timeout(stopped, interval) {
                    Ok(waited) => waited,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                drop(stopped);
                run_controlled_metadata_backup_worker_once(
                    &service,
                    &owner,
                    &options.config,
                    &worker_state,
                );
            }
        });
        Self {
            stop,
            state,
            handle: Some(handle),
        }
    }

    fn state(&self) -> MetadataBackupWorkerState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|err| err.into_inner().clone())
    }

    fn stop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ControlledMetadataBackupWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_controlled_metadata_backup_worker_once(
    service: &NoKvFs<ServerMetadataStore, ConfiguredObjectStore>,
    owner: &ServerShardOwner,
    config: &MetadataArchiveConfig,
    state: &Arc<Mutex<MetadataBackupWorkerState>>,
) {
    let outcome = run_controlled_metadata_backup_once(service, owner, config);
    if let Ok(mut state) = state.lock() {
        state.iterations += 1;
        match outcome {
            Ok(outcome) => {
                state.last_outcome = Some(outcome);
                state.last_error = None;
            }
            Err(err) => state.last_error = Some(err.to_string()),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ServerShardOwnerRenewalWorkerState {
    iterations: u64,
    last_error: Option<String>,
}

struct ServerShardOwnerRenewalWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    state: Arc<Mutex<ServerShardOwnerRenewalWorkerState>>,
    handle: Option<JoinHandle<()>>,
}

impl ServerShardOwnerRenewalWorker {
    fn spawn<M, O>(
        service: Arc<NoKvFs<M, O>>,
        owner: ServerShardOwner,
        options: ServerShardOwnerRenewalOptions,
    ) -> Self
    where
        M: nokv_meta::MetadataStore + Send + Sync + 'static,
        O: nokv_object::ObjectStore + Send + Sync + 'static,
    {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = Arc::new(Mutex::new(ServerShardOwnerRenewalWorkerState::default()));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let interval = options.interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            if options.run_immediately {
                run_shard_owner_renewal_once(&service, &owner, &worker_state);
            }
            loop {
                let (lock, cvar) = &*worker_stop;
                let stopped = match lock.lock() {
                    Ok(stopped) => stopped,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                let (stopped, _) = match cvar.wait_timeout(stopped, interval) {
                    Ok(waited) => waited,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                drop(stopped);
                run_shard_owner_renewal_once(&service, &owner, &worker_state);
            }
        });
        Self {
            stop,
            state,
            handle: Some(handle),
        }
    }

    fn state(&self) -> ServerShardOwnerRenewalWorkerState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|err| err.into_inner().clone())
    }

    fn stop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ServerShardOwnerRenewalWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_shard_owner_renewal_once<M, O>(
    service: &Arc<NoKvFs<M, O>>,
    owner: &ServerShardOwner,
    state: &Arc<Mutex<ServerShardOwnerRenewalWorkerState>>,
) where
    M: nokv_meta::MetadataStore,
    O: nokv_object::ObjectStore,
{
    let result = owner.renew(service.as_ref());
    let mut state = match state.lock() {
        Ok(state) => state,
        Err(err) => err.into_inner(),
    };
    state.iterations = state.iterations.saturating_add(1);
    state.last_error = result.err().map(|err| err.to_string());
}

struct ConnectionWorkerPool {
    sender: mpsc::SyncSender<TcpStream>,
}

impl ConnectionWorkerPool {
    fn new(server: Arc<Server>, workers: usize, queue: usize) -> Result<Self, ServerError> {
        let (sender, receiver) = mpsc::sync_channel::<TcpStream>(queue.max(workers));
        let receiver = Arc::new(Mutex::new(receiver));
        for worker in 0..workers {
            let server = Arc::clone(&server);
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("nokv-conn-{worker}"))
                .spawn(move || loop {
                    let stream = {
                        let receiver = match receiver.lock() {
                            Ok(receiver) => receiver,
                            Err(_) => return,
                        };
                        receiver.recv()
                    };
                    match stream {
                        Ok(stream) => {
                            if let Err(err) = http::handle_stream(Arc::clone(&server), stream) {
                                eprintln!("nokv-server connection failed: {err}");
                            }
                        }
                        Err(_) => return,
                    }
                })
                .map_err(ServerError::Io)?;
        }
        Ok(Self { sender })
    }

    fn submit(&self, stream: TcpStream) -> Result<(), ServerError> {
        self.sender.send(stream).map_err(|_| {
            ServerError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "nokv connection worker pool stopped",
            ))
        })
    }
}

fn default_metadata_state_path(meta_path: &Path) -> PathBuf {
    meta_path.join("metadata-state.holt")
}

fn metadata_store_json(stats: &nokv_meta::MetadataStoreStats) -> String {
    format!(
        "{{\"get_total\":{},\"get_user_strong_total\":{},\"get_write_plan_local_total\":{},\"get_snapshot_total\":{},\"scan_total\":{},\"scan_user_strong_total\":{},\"scan_write_plan_local_total\":{},\"scan_snapshot_total\":{},\"scan_cache_hit_total\":{},\"scan_key_visited_total\":{},\"scan_key_returned_total\":{},\"history_lookup_total\":{},\"active_snapshot_pin_total\":{},\"commit_total\":{},\"dedupe_hit_total\":{},\"predicate_total\":{},\"prefix_empty_predicate_total\":{},\"current_put_total\":{},\"current_delete_total\":{},\"history_write_total\":{},\"watch_write_total\":{},\"dedupe_write_total\":{},\"commit_prepare_ns_total\":{},\"atomic_apply_total\":{},\"atomic_apply_command_total\":{},\"atomic_apply_max_batch\":{},\"atomic_apply_ns_total\":{}}}",
        stats.get_total,
        stats.get_user_strong_total,
        stats.get_write_plan_local_total,
        stats.get_snapshot_total,
        stats.scan_total,
        stats.scan_user_strong_total,
        stats.scan_write_plan_local_total,
        stats.scan_snapshot_total,
        stats.scan_cache_hit_total,
        stats.scan_key_visited_total,
        stats.scan_key_returned_total,
        stats.history_lookup_total,
        stats.active_snapshot_pin_total,
        stats.commit_total,
        stats.dedupe_hit_total,
        stats.predicate_total,
        stats.prefix_empty_predicate_total,
        stats.current_put_total,
        stats.current_delete_total,
        stats.history_write_total,
        stats.watch_write_total,
        stats.dedupe_write_total,
        stats.commit_prepare_ns_total,
        stats.atomic_apply_total,
        stats.atomic_apply_command_total,
        stats.atomic_apply_max_batch,
        stats.atomic_apply_ns_total,
    )
}

fn metadata_service_json(stats: &nokv_meta::MetadataServiceStats) -> String {
    format!(
        "{{\"path_index_lookup_total\":{},\"path_index_hit_total\":{},\"path_index_miss_total\":{},\"path_index_stale_total\":{},\"path_index_scan_stale_total\":{},\"path_index_fallback_total\":{},\"create_files_batch_total\":{},\"create_files_entry_total\":{},\"create_dirs_batch_total\":{},\"create_dirs_entry_total\":{},\"read_dir_plus_total\":{},\"read_dir_plus_entry_total\":{},\"read_dir_plus_projection_hit_total\":{},\"metadata_log_segments_archived_total\":{},\"metadata_log_entries_archived_total\":{},\"metadata_log_archive_bytes_total\":{}}}",
        stats.path_index_lookup_total,
        stats.path_index_hit_total,
        stats.path_index_miss_total,
        stats.path_index_stale_total,
        stats.path_index_scan_stale_total,
        stats.path_index_fallback_total,
        stats.create_files_batch_total,
        stats.create_files_entry_total,
        stats.create_dirs_batch_total,
        stats.create_dirs_entry_total,
        stats.read_dir_plus_total,
        stats.read_dir_plus_entry_total,
        stats.read_dir_plus_projection_hit_total,
        stats.metadata_log_segments_archived_total,
        stats.metadata_log_entries_archived_total,
        stats.metadata_log_archive_bytes_total,
    )
}

fn restore_shard_owner_recovery_refs(
    service: &NoKvFs<ServerMetadataStore, ConfiguredObjectStore>,
    owner: &ServerShardOwner,
    archive: &MetadataArchiveConfig,
    log_archive: &MetadataLogArchiveConfig,
) -> Result<bool, ServerError> {
    let state = owner.state()?;
    let Some(checkpoint) = state.checkpoint.clone() else {
        return Ok(false);
    };
    let checkpoint_digest = parse_hex_digest(&checkpoint.digest)?;
    // Replay every archived segment whose tail is above the checkpoint LSN, in
    // order. A single-pointer LogRef would drop all but the newest segment and
    // silently lose acknowledged metadata on any multi-segment failover.
    let segment_refs: Vec<LogSegmentRef> = state
        .log
        .as_ref()
        .map(|log| {
            log.segments
                .iter()
                .filter(|segment| segment.last_lsn > checkpoint.lsn)
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if state.durable_lsn > checkpoint.lsn && segment_refs.is_empty() {
        return Err(ServerError::Metadata(MetadError::Codec(
            "control record is missing log segments for checkpoint replay".to_owned(),
        )));
    }
    let mut segments = Vec::with_capacity(segment_refs.len());
    for segment_ref in &segment_refs {
        log_archive.validate_segment_key(&segment_ref.segment_key)?;
        let segment = service.load_metadata_log_segment(&segment_ref.segment_key)?;
        validate_control_log_segment_identity(segment_ref, &segment)?;
        segments.push(segment);
    }
    let identity = checkpoint_identity(&checkpoint);
    let outcome = service.restore_metadata_checkpoint_with_log_segments(
        archive,
        state.shard_id.as_str(),
        &identity,
        &segments,
        checkpoint.lsn,
        checkpoint_digest,
    )?;
    if outcome.checkpoint.checkpoint_key != checkpoint.object_key {
        return Err(ServerError::Metadata(MetadError::Codec(format!(
            "restored checkpoint {} does not match control checkpoint {}",
            outcome.checkpoint.checkpoint_key, checkpoint.object_key
        ))));
    }
    if outcome.durable_lsn != state.durable_lsn {
        return Err(ServerError::Metadata(MetadError::Codec(format!(
            "restored durable_lsn {} does not match control durable_lsn {}",
            outcome.durable_lsn, state.durable_lsn
        ))));
    }
    let expected_digest = control_recovery_digest(&state)?;
    if outcome.last_digest != expected_digest {
        return Err(ServerError::Metadata(MetadError::Codec(format!(
            "restored metadata log digest {} does not match control digest {}",
            hex_digest(&outcome.last_digest),
            hex_digest(&expected_digest)
        ))));
    }
    Ok(true)
}

fn validate_control_log_segment_identity(
    reference: &LogSegmentRef,
    segment: &MetadataLogSegment,
) -> Result<(), ServerError> {
    let expected_digest = parse_hex_digest(&reference.digest)?;
    if segment.first_lsn != reference.first_lsn
        || segment.last_lsn != reference.last_lsn
        || segment.last_digest != expected_digest
    {
        return Err(ServerError::Metadata(MetadError::Codec(format!(
            "metadata log segment {} does not match control identity {}..{}:{}",
            reference.segment_key, reference.first_lsn, reference.last_lsn, reference.digest,
        ))));
    }
    Ok(())
}

fn object_gc_json(state: &ObjectGcWorkerState, failover_durability_required: bool) -> String {
    let outcome = state.last_outcome.map_or_else(
        || "null".to_owned(),
        |outcome| {
            format!(
                "{{\"scanned\":{},\"blocked_by_snapshots\":{},\"blocked_by_read_leases\":{},\"blocked_by_failover_durability\":{},\"attempted\":{},\"deleted\":{},\"missing\":{},\"records_removed\":{},\"snapshot_reap\":{{\"scanned\":{},\"expired_candidates\":{},\"reaped\":{},\"conflicted\":{}}}}}",
                outcome.scanned,
                outcome.blocked_by_snapshots,
                outcome.blocked_by_read_leases,
                outcome.blocked_by_failover_durability,
                outcome.attempted,
                outcome.deleted,
                outcome.missing,
                outcome.records_removed,
                outcome.snapshot_reap.scanned,
                outcome.snapshot_reap.expired_candidates,
                outcome.snapshot_reap.reaped,
                outcome.snapshot_reap.conflicted,
            )
        },
    );
    let reap = state.snapshot_reap;
    format!(
        "{{\"failover_durability_required\":{},\"iterations\":{},\"last_outcome\":{},\"last_error\":{},\"snapshot_reap\":{{\"scanned\":{},\"expired_candidates\":{},\"reaped\":{},\"conflicted\":{}}}}}",
        failover_durability_required,
        state.iterations,
        outcome,
        json_string_or_null(state.last_error.as_deref()),
        reap.scanned,
        reap.expired_candidates,
        reap.reaped,
        reap.conflicted,
    )
}

fn history_gc_json(state: &HistoryGcWorkerState) -> String {
    format!(
        "{{\"iterations\":{},\"last_error\":{}}}",
        state.iterations,
        json_string_or_null(state.last_error.as_deref())
    )
}

fn log_ref_json(log: Option<&LogRef>) -> String {
    match log {
        Some(log) => {
            let segments = log
                .segments
                .iter()
                .map(|segment| {
                    format!(
                        "{{\"segment_key\":\"{}\",\"first_lsn\":{},\"last_lsn\":{},\"digest\":\"{}\"}}",
                        escape_json_string(&segment.segment_key),
                        segment.first_lsn,
                        segment.last_lsn,
                        escape_json_string(&segment.digest),
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{{\"segments\":[{}],\"durable_lsn\":{},\"digest\":\"{}\"}}",
                segments,
                log.durable_lsn,
                escape_json_string(&log.digest),
            )
        }
        None => "null".to_owned(),
    }
}

/// Rebuild the meta-side segment chain (above the latest checkpoint) from the
/// control record so a re-opened or failed-over owner keeps publishing the full
/// chain instead of overwriting it with only its own new segments.
fn inherited_log_segments(
    state: &ServerShardOwnerState,
) -> Result<Vec<MetadataLogSegmentPointer>, ServerError> {
    let Some(log) = state.log.as_ref() else {
        return Ok(Vec::new());
    };
    let checkpoint_lsn = state.checkpoint.as_ref().map(|c| c.lsn).unwrap_or(0);
    log.segments
        .iter()
        .filter(|segment| segment.last_lsn > checkpoint_lsn)
        .map(|segment| {
            Ok(MetadataLogSegmentPointer {
                segment_key: segment.segment_key.clone(),
                first_lsn: segment.first_lsn,
                last_lsn: segment.last_lsn,
                last_digest: parse_hex_digest(&segment.digest)?,
            })
        })
        .collect()
}

fn control_recovery_digest(state: &ServerShardOwnerState) -> Result<[u8; 32], ServerError> {
    if state.durable_lsn == 0 {
        return Ok(METADATA_LOG_ZERO_DIGEST);
    }
    if let Some(log) = state.log.as_ref() {
        if log.durable_lsn == state.durable_lsn {
            return parse_hex_digest(&log.digest);
        }
    }
    if let Some(checkpoint) = state.checkpoint.as_ref() {
        if checkpoint.lsn == state.durable_lsn {
            return parse_hex_digest(&checkpoint.digest);
        }
    }
    Err(ServerError::Metadata(MetadError::Codec(
        "control record has durable_lsn without matching recovery digest".to_owned(),
    )))
}

fn parse_hex_digest(raw: &str) -> Result<[u8; 32], ServerError> {
    if raw.len() != 64 {
        return Err(ServerError::Metadata(MetadError::Codec(
            "metadata log digest must be 64 hex characters".to_owned(),
        )));
    }
    let mut out = [0_u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        let offset = index * 2;
        let hi = hex_nibble(raw.as_bytes()[offset])?;
        let lo = hex_nibble(raw.as_bytes()[offset + 1])?;
        *byte = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, ServerError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ServerError::Metadata(MetadError::Codec(
            "metadata log digest is not valid hex".to_owned(),
        ))),
    }
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn json_string_or_null(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", escape_json_string(value)),
        None => "null".to_owned(),
    }
}

fn escape_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

impl From<MetadError> for ServerError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ControlError> for ServerError {
    fn from(err: ControlError) -> Self {
        Self::Control(err)
    }
}

impl From<ObjectError> for ServerError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Control(err) => write!(f, "{err}"),
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
            Self::NotOwner { shard_id, endpoint } => match endpoint {
                Some(endpoint) => write!(
                    f,
                    "shard {shard_id} is not owned here; current owner endpoint is {endpoint}"
                ),
                None => write!(f, "shard {shard_id} is not owned here"),
            },
        }
    }
}

fn shard_state_name(state: nokv_control::ShardState) -> &'static str {
    match state {
        nokv_control::ShardState::Unassigned => "unassigned",
        nokv_control::ShardState::Recovering => "recovering",
        nokv_control::ShardState::Serving => "serving",
        nokv_control::ShardState::Draining => "draining",
        nokv_control::ShardState::ReadOnly => "read_only",
    }
}

impl Error for ServerError {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant};
    #[cfg(feature = "etcd")]
    use std::time::{SystemTime, UNIX_EPOCH};
    #[cfg(feature = "etcd")]
    use std::{env, process};

    use nokv_control::{InMemoryControlStore, NodeId, ShardLease, ShardRecord, ShardState};
    use nokv_meta::layout::{
        decode_snapshot_pin, failover_durability_required_key, gc_object_key, object_gc_claim_key,
        snapshot_pin_prefix,
    };
    use nokv_meta::{
        CommandKind, CommitResult, HistoryGcOptions, MetadataCommand, MetadataLogEntry,
        MetadataStore, Mutation, MutationOp, ObjectGcOptions, Predicate, PredicateRef, ReadPurpose,
        ScanRequest, Value, Version,
    };
    use nokv_object::{
        MemoryObjectStore, ObjectKey, ObjectStore, ObjectStoreConfig, S3ObjectStoreOptions,
    };
    use nokv_types::{MountId, RecordFamily};
    use tempfile::tempdir;

    pub(crate) fn test_options(root: &Path) -> ServerOptions {
        ServerOptions {
            bind: crate::options::DEFAULT_SERVER_BIND,
            mount: MountId::new(1).unwrap(),
            meta_path: root.join("meta"),
            metadata_checkpoint_archive_prefix: None,
            object: ObjectStoreConfig::s3(S3ObjectStoreOptions {
                bucket: "test".to_owned(),
                root: "/".to_owned(),
                region: "auto".to_owned(),
                endpoint: Some("http://127.0.0.1:1".to_owned()),
                access_key_id: Some("test".to_owned()),
                secret_access_key: Some("test".to_owned()),
                session_token: None,
                virtual_host_style: false,
                skip_signature: true,
            }),
            uid: 1000,
            gid: 1000,
            object_gc: ObjectGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
                read_lease_grace: ObjectGcOptions::default().read_lease_grace,
            },
            history_gc: HistoryGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
            control: None,
        }
    }

    pub(crate) fn test_server() -> Server {
        let dir = tempdir().unwrap();
        let mut server = Server::open(test_options(dir.path())).unwrap();
        server._test_meta_dir = Some(dir);
        server
    }

    fn open_memory_controlled<C>(
        root: &Path,
        control: Arc<C>,
        owners: Vec<ServerShardOwnerOptions>,
    ) -> Result<Server, ServerError>
    where
        C: ControlStore + 'static,
    {
        let mut options = test_options(root);
        options.metadata_checkpoint_archive_prefix = Some("metadata/control-test".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let control: Arc<dyn ControlStore> = control;
        let owners = owners
            .into_iter()
            .map(|owner| {
                if owner.shared_log.is_some() {
                    owner
                } else {
                    owner.with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/control-test-log",
                    )))
                }
            })
            .collect();
        Server::open_with_objects(options, objects, Some((control, owners)))
    }

    pub(crate) struct FailingCheckpointPublishControl {
        inner: InMemoryControlStore,
        fail_marks: AtomicUsize,
        fail_renews: AtomicUsize,
        mark_calls: AtomicUsize,
        fail_on_call: AtomicUsize,
    }

    impl FailingCheckpointPublishControl {
        pub(crate) fn new() -> Self {
            Self {
                inner: InMemoryControlStore::new(),
                fail_marks: AtomicUsize::new(0),
                fail_renews: AtomicUsize::new(0),
                mark_calls: AtomicUsize::new(0),
                fail_on_call: AtomicUsize::new(0),
            }
        }

        pub(crate) fn fail_next_marks(&self, count: usize) {
            self.fail_marks.store(count, AtomicOrdering::SeqCst);
        }

        pub(crate) fn fail_next_renews(&self, count: usize) {
            self.fail_renews.store(count, AtomicOrdering::SeqCst);
        }

        fn fail_mark_on_call(&self, call: usize) {
            self.mark_calls.store(0, AtomicOrdering::SeqCst);
            self.fail_on_call.store(call, AtomicOrdering::SeqCst);
        }
    }

    impl ControlStore for FailingCheckpointPublishControl {
        fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError> {
            self.inner.ensure_shard(shard_id)
        }

        fn register_shard(
            &self,
            shard_id: ShardId,
            prefix: String,
            shard_index: u16,
        ) -> Result<ShardRecord, ControlError> {
            self.inner.register_shard(shard_id, prefix, shard_index)
        }

        fn set_subtree_root_inode(
            &self,
            shard_id: &ShardId,
            subtree_root_inode: Option<u64>,
        ) -> Result<ShardRecord, ControlError> {
            self.inner
                .set_subtree_root_inode(shard_id, subtree_root_inode)
        }

        fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError> {
            self.inner.list_shards()
        }

        fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError> {
            self.inner.get_shard(shard_id)
        }

        fn acquire_unassigned(
            &self,
            shard_id: ShardId,
            owner: NodeId,
        ) -> Result<ShardLease, ControlError> {
            self.inner.acquire_unassigned(shard_id, owner)
        }

        fn acquire_after_failure(
            &self,
            shard_id: ShardId,
            owner: NodeId,
            previous_epoch: u64,
        ) -> Result<ShardLease, ControlError> {
            self.inner
                .acquire_after_failure(shard_id, owner, previous_epoch)
        }

        fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
            if self
                .fail_renews
                .fetch_update(
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(ControlError::Backend(
                    "injected owner renew failure".to_owned(),
                ));
            }
            self.inner.renew(lease)
        }

        fn mark_serving(
            &self,
            lease: &ShardLease,
            checkpoint: Option<CheckpointRef>,
            log: Option<LogRef>,
            durable_lsn: u64,
        ) -> Result<ShardRecord, ControlError> {
            let call = self.mark_calls.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            if self.fail_on_call.load(AtomicOrdering::SeqCst) == call {
                return Err(ControlError::Backend(
                    "injected checkpoint publish failure".to_owned(),
                ));
            }
            if self
                .fail_marks
                .fetch_update(
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(ControlError::Backend(
                    "injected checkpoint publish failure".to_owned(),
                ));
            }
            self.inner.mark_serving(lease, checkpoint, log, durable_lsn)
        }

        fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
            self.inner.release(lease)
        }
    }

    #[derive(Default)]
    struct BlockingGateState {
        armed: bool,
        reached: bool,
        released: bool,
    }

    #[derive(Clone)]
    struct BlockingPutStore {
        inner: MemoryObjectStore,
        gate: Arc<(Mutex<BlockingGateState>, Condvar)>,
        fail_put_substring: Arc<Mutex<Option<String>>>,
        fail_delete_substring: Arc<Mutex<Option<String>>>,
    }

    impl BlockingPutStore {
        fn new() -> Self {
            Self {
                inner: MemoryObjectStore::new(),
                gate: Arc::new((Mutex::new(BlockingGateState::default()), Condvar::new())),
                fail_put_substring: Arc::new(Mutex::new(None)),
                fail_delete_substring: Arc::new(Mutex::new(None)),
            }
        }

        fn fail_puts_containing(&self, substring: &str) {
            *self.fail_put_substring.lock().unwrap() = Some(substring.to_owned());
        }

        fn clear_put_failures(&self) {
            *self.fail_put_substring.lock().unwrap() = None;
        }

        fn fail_deletes_containing(&self, substring: &str) {
            *self.fail_delete_substring.lock().unwrap() = Some(substring.to_owned());
        }

        fn arm(&self) {
            let (lock, _) = &*self.gate;
            *lock.lock().unwrap() = BlockingGateState {
                armed: true,
                reached: false,
                released: false,
            };
        }

        fn wait_until_reached(&self) {
            wait_for_blocking_gate(&self.gate, "checkpoint PUT");
        }

        fn has_reached(&self) -> bool {
            let (lock, _) = &*self.gate;
            lock.lock().unwrap().reached
        }

        fn release(&self) {
            release_blocking_gate(&self.gate);
        }
    }

    impl ObjectStore for BlockingPutStore {
        fn put(
            &self,
            key: &ObjectKey,
            bytes: impl Into<nokv_object::ObjectBytes>,
        ) -> Result<nokv_object::ObjectInfo, ObjectError> {
            let bytes = bytes.into();
            if self
                .fail_put_substring
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|substring| key.as_str().contains(substring))
            {
                return Err(ObjectError::Backend(
                    "injected object PUT failure".to_owned(),
                ));
            }
            let (lock, changed) = &*self.gate;
            let mut state = lock.lock().unwrap();
            if state.armed {
                state.armed = false;
                state.reached = true;
                changed.notify_all();
                while !state.released {
                    state = changed.wait(state).unwrap();
                }
            }
            drop(state);
            self.inner.put(key, bytes)
        }

        fn get(
            &self,
            key: &ObjectKey,
            range: Option<nokv_object::ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.inner.get(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<nokv_object::ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
            if self
                .fail_delete_substring
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|substring| key.as_str().contains(substring))
            {
                return Err(ObjectError::Backend(
                    "injected object DELETE failure".to_owned(),
                ));
            }
            self.inner.delete(key)
        }
    }

    fn controlled_sync_log_backup_fixture(
        control: Arc<dyn ControlStore>,
        objects: BlockingPutStore,
        prefix: &str,
    ) -> (
        Arc<NoKvFs<HoltMetadataStore, BlockingPutStore>>,
        ServerShardOwner,
        MetadataArchiveConfig,
        MetadataBackupOutcome,
    ) {
        let service = Arc::new(NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects,
        ));
        service
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        let owner = ServerShardOwner::acquire(
            control,
            ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None),
            service.as_ref(),
        )
        .unwrap();
        let archive = MetadataArchiveConfig::new(format!("{prefix}/checkpoint"), 2);
        let owner_state = owner.state().unwrap();
        service
            .enable_sync_metadata_log(MetadataLogSyncConfig::new(
                format!("{prefix}/shared-log"),
                owner_state.shard_id.as_str(),
                owner_state.epoch,
                0,
                METADATA_LOG_ZERO_DIGEST,
            ))
            .unwrap();
        let initial =
            run_controlled_metadata_backup_once(service.as_ref(), &owner, &archive).unwrap();
        (service, owner, archive, initial)
    }

    fn local_snapshot_pins<O: ObjectStore>(
        service: &NoKvFs<HoltMetadataStore, O>,
    ) -> Vec<nokv_types::SnapshotPin> {
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::Snapshot,
                prefix: snapshot_pin_prefix(MountId::new(1).unwrap()),
                start_after: None,
                version: Version::new(u64::MAX).unwrap(),
                limit: usize::MAX,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .into_iter()
            .map(|item| decode_snapshot_pin(&item.value.0).unwrap())
            .collect()
    }

    fn local_log_segment_keys(snapshot: &nokv_meta::MetadataLogSyncSnapshot) -> Vec<String> {
        snapshot
            .segments
            .iter()
            .map(|segment| segment.segment_key.clone())
            .collect()
    }

    pub(crate) struct BlockingServingControl {
        inner: InMemoryControlStore,
        block_next_mark: AtomicBool,
        gate: Arc<(Mutex<BlockingGateState>, Condvar)>,
    }

    impl BlockingServingControl {
        pub(crate) fn new() -> Self {
            Self {
                inner: InMemoryControlStore::new(),
                block_next_mark: AtomicBool::new(false),
                gate: Arc::new((Mutex::new(BlockingGateState::default()), Condvar::new())),
            }
        }

        pub(crate) fn arm_mark(&self) {
            self.block_next_mark.store(true, AtomicOrdering::SeqCst);
            let (lock, _) = &*self.gate;
            *lock.lock().unwrap() = BlockingGateState {
                armed: true,
                reached: false,
                released: false,
            };
        }

        pub(crate) fn wait_until_mark_reached(&self) {
            wait_for_blocking_gate(&self.gate, "mark-serving CAS");
        }

        fn mark_reached(&self) -> bool {
            let (lock, _) = &*self.gate;
            lock.lock().unwrap().reached
        }

        pub(crate) fn release_mark(&self) {
            release_blocking_gate(&self.gate);
        }
    }

    fn wait_for_blocking_gate(gate: &Arc<(Mutex<BlockingGateState>, Condvar)>, operation: &str) {
        let (lock, changed) = &**gate;
        let mut state = lock.lock().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !state.reached {
            let now = Instant::now();
            assert!(now < deadline, "timed out waiting for {operation}");
            let (next, timeout) = changed.wait_timeout(state, deadline - now).unwrap();
            state = next;
            assert!(
                !timeout.timed_out() || state.reached,
                "timed out waiting for {operation}"
            );
        }
    }

    fn release_blocking_gate(gate: &Arc<(Mutex<BlockingGateState>, Condvar)>) {
        let (lock, changed) = &**gate;
        let mut state = lock.lock().unwrap();
        state.released = true;
        changed.notify_all();
    }

    impl ControlStore for BlockingServingControl {
        fn ensure_shard(&self, shard_id: ShardId) -> Result<ShardRecord, ControlError> {
            self.inner.ensure_shard(shard_id)
        }

        fn register_shard(
            &self,
            shard_id: ShardId,
            prefix: String,
            shard_index: u16,
        ) -> Result<ShardRecord, ControlError> {
            self.inner.register_shard(shard_id, prefix, shard_index)
        }

        fn set_subtree_root_inode(
            &self,
            shard_id: &ShardId,
            subtree_root_inode: Option<u64>,
        ) -> Result<ShardRecord, ControlError> {
            self.inner
                .set_subtree_root_inode(shard_id, subtree_root_inode)
        }

        fn list_shards(&self) -> Result<Vec<ShardRecord>, ControlError> {
            self.inner.list_shards()
        }

        fn get_shard(&self, shard_id: &ShardId) -> Result<ShardRecord, ControlError> {
            self.inner.get_shard(shard_id)
        }

        fn acquire_unassigned(
            &self,
            shard_id: ShardId,
            owner: NodeId,
        ) -> Result<ShardLease, ControlError> {
            self.inner.acquire_unassigned(shard_id, owner)
        }

        fn acquire_after_failure(
            &self,
            shard_id: ShardId,
            owner: NodeId,
            previous_epoch: u64,
        ) -> Result<ShardLease, ControlError> {
            self.inner
                .acquire_after_failure(shard_id, owner, previous_epoch)
        }

        fn renew(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
            self.inner.renew(lease)
        }

        fn mark_serving(
            &self,
            lease: &ShardLease,
            checkpoint: Option<CheckpointRef>,
            log: Option<LogRef>,
            durable_lsn: u64,
        ) -> Result<ShardRecord, ControlError> {
            if self.block_next_mark.swap(false, AtomicOrdering::SeqCst) {
                let (lock, changed) = &*self.gate;
                let mut state = lock.lock().unwrap();
                state.reached = true;
                changed.notify_all();
                while !state.released {
                    state = changed.wait(state).unwrap();
                }
            }
            self.inner.mark_serving(lease, checkpoint, log, durable_lsn)
        }

        fn release(&self, lease: &ShardLease) -> Result<ShardRecord, ControlError> {
            self.inner.release(lease)
        }
    }

    /// Seed the exact durable state left by a crash after object-GC claimed a
    /// row but before it reopened the claim. The malformed row makes startup
    /// ordering observable: ordinary recovery quarantines it, while failover
    /// durability must preserve it.
    struct SeededObjectGcClaim {
        metadata_path: PathBuf,
        gc_key: Vec<u8>,
        claim_key: Vec<u8>,
        encoded_claim: Vec<u8>,
        operation_token: u64,
    }

    fn seed_crash_left_object_gc_claim(
        options: &ServerOptions,
        metadata_dir: &Path,
        objects: ConfiguredObjectStore,
    ) -> SeededObjectGcClaim {
        std::fs::create_dir_all(metadata_dir).unwrap();
        let metadata_path = default_metadata_state_path(metadata_dir);
        let holt = HoltMetadataStore::open_file(&metadata_path).unwrap();
        let service = NoKvFs::new(
            options.mount,
            ServerMetadataStore::direct(holt.clone()),
            objects,
        );
        service
            .bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)
            .unwrap();

        let claim_key = object_gc_claim_key(options.mount);
        let claim = holt
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();
        // Bootstrap reserves a large version window. Stay within that window
        // while placing the synthetic crash record after every bootstrap row.
        let commit_version = Version::new(claim.version.get() + 100).unwrap();
        let operation_token = claim.version.get();
        let gc_key = gc_object_key(
            options.mount,
            commit_version.get(),
            InodeId::root(),
            1,
            0,
            0,
        );
        let mut deleting_claim = Vec::with_capacity(29 + gc_key.len());
        deleting_claim.push(2);
        deleting_claim.extend_from_slice(&1_u64.to_be_bytes());
        deleting_claim.extend_from_slice(&operation_token.to_be_bytes());
        deleting_claim.extend_from_slice(&commit_version.get().to_be_bytes());
        deleting_claim.extend_from_slice(&(gc_key.len() as u32).to_be_bytes());
        deleting_claim.extend_from_slice(&gc_key);
        holt.commit_metadata(MetadataCommand {
            request_id: b"seed-crash-left-object-gc-claim".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: Version::new(commit_version.get() - 1).unwrap(),
            commit_version,
            primary_family: RecordFamily::System,
            primary_key: claim_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: claim_key.clone(),
                    predicate: Predicate::VersionEquals(claim.version),
                },
                PredicateRef {
                    family: RecordFamily::Gc,
                    key: gc_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![
                Mutation {
                    family: RecordFamily::Gc,
                    key: gc_key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xff])),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: claim_key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(deleting_claim.clone())),
                },
            ],
            watch: Vec::new(),
        })
        .unwrap();
        drop(service);
        drop(holt);
        SeededObjectGcClaim {
            metadata_path,
            gc_key,
            claim_key,
            encoded_claim: deleting_claim,
            operation_token,
        }
    }

    fn assert_startup_requires_object_gc_intervention(
        result: Result<Server, ServerError>,
        operation_token: u64,
    ) {
        match result {
            Err(ServerError::Metadata(MetadError::ObjectGcRecoveryRequiresIntervention {
                owner_epoch,
                operation_token: actual_token,
            })) => {
                assert_eq!(owner_epoch, 1);
                assert_eq!(actual_token, operation_token);
            }
            Err(err) => panic!("unexpected startup error: {err}"),
            Ok(_) => panic!("server must not become ready with an uncertain object deletion"),
        }
    }

    fn assert_failover_gate_preserves_crash_claim(
        options: &ServerOptions,
        seeded: &SeededObjectGcClaim,
        expect_marker: bool,
    ) {
        let holt = HoltMetadataStore::open_file(&seeded.metadata_path).unwrap();
        let latest = Version::new(u64::MAX).unwrap();
        assert_eq!(
            holt.get(
                RecordFamily::Gc,
                &seeded.gc_key,
                latest,
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
            vec![0xff],
            "failover policy must preserve the claimed GC row"
        );
        assert_eq!(
            holt.get(
                RecordFamily::System,
                &seeded.claim_key,
                latest,
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
            seeded.encoded_claim,
            "failover policy must keep the deletion claim closed"
        );
        assert_eq!(
            holt.get(
                RecordFamily::System,
                &failover_durability_required_key(options.mount),
                latest,
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .is_some(),
            expect_marker,
            "deployment preflight must not install the marker before shared-log validation"
        );
    }

    fn fast_renewal_options(run_immediately: bool) -> ServerShardOwnerRenewalOptions {
        ServerShardOwnerRenewalOptions {
            interval: Duration::from_millis(10),
            run_immediately,
            ..ServerShardOwnerRenewalOptions::default()
        }
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("condition was not met before deadline");
    }

    #[test]
    fn controlled_checkpoint_cas_failure_does_not_delete_covered_log_segment() {
        let objects = BlockingPutStore::new();
        let control = Arc::new(FailingCheckpointPublishControl::new());
        let (service, owner, archive, initial) = controlled_sync_log_backup_fixture(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            objects.clone(),
            "metadata/log-prune-cas-failure",
        );
        service
            .create_dir_path("/covered", 0o755, 1000, 1000)
            .unwrap();
        let before = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(before.segments.len(), 1);
        let covered_key = ObjectKey::new(before.segments[0].segment_key.clone()).unwrap();
        let prior = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(
            prior.checkpoint.as_ref().unwrap().object_key,
            initial.checkpoint_key
        );

        control.fail_next_marks(1);
        assert!(matches!(
            run_controlled_metadata_backup_once(service.as_ref(), &owner, &archive),
            Err(ServerError::Control(ControlError::Backend(message)))
                if message.contains("injected checkpoint publish failure")
        ));

        assert_eq!(service.sync_metadata_log_snapshot().unwrap(), before);
        assert!(objects.head(&covered_key).unwrap().is_some());
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint,
            prior.checkpoint
        );
        owner.release().unwrap();
    }

    #[test]
    fn controlled_checkpoint_deletes_only_segments_at_or_before_captured_lsn() {
        let objects = BlockingPutStore::new();
        let control = Arc::new(InMemoryControlStore::new());
        let (service, owner, archive, _initial) = controlled_sync_log_backup_fixture(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            objects.clone(),
            "metadata/log-prune-boundary",
        );
        service
            .create_dir_path("/covered", 0o755, 1000, 1000)
            .unwrap();
        let covered = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(covered.segments.len(), 1);
        let covered_pointer = covered.segments[0].clone();
        let covered_key = ObjectKey::new(covered_pointer.segment_key.clone()).unwrap();

        objects.arm();
        let backup_service = Arc::clone(&service);
        let backup_owner = owner.clone();
        let backup_archive = archive.clone();
        let backup = thread::spawn(move || {
            run_controlled_metadata_backup_once(
                backup_service.as_ref(),
                &backup_owner,
                &backup_archive,
            )
        });
        objects.wait_until_reached();

        // Checkpoint capture is complete and its image PUT is blocked. This
        // later commit must remain as the replay tail above the captured LSN.
        service.create_dir_path("/tail", 0o755, 1000, 1000).unwrap();
        let before_publish = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(before_publish.segments.len(), 2);
        let tail_pointer = before_publish.segments[1].clone();
        let tail_key = ObjectKey::new(tail_pointer.segment_key.clone()).unwrap();
        assert!(tail_pointer.first_lsn > covered_pointer.last_lsn);

        objects.release();
        let err = backup.join().unwrap().unwrap_err();
        assert!(
            err.to_string()
                .contains("published checkpoint does not exactly cover"),
            "unexpected stale-capture result: {err}"
        );
        assert!(objects.head(&covered_key).unwrap().is_none());
        assert!(objects.head(&tail_key).unwrap().is_some());
        assert_eq!(
            service.sync_metadata_log_snapshot().unwrap().segments,
            vec![tail_pointer.clone()]
        );
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint
                .unwrap()
                .lsn,
            covered_pointer.last_lsn
        );
        let published = Server::publish_owner_latest_log_ref(service.as_ref(), &owner)
            .unwrap()
            .unwrap();
        assert_eq!(
            published.log,
            metadata_log_ref(&service.sync_metadata_log_snapshot().unwrap())
        );
        owner.release().unwrap();
    }

    #[test]
    fn controlled_checkpoint_delete_failure_keeps_publication_success_observable() {
        let objects = BlockingPutStore::new();
        let control = Arc::new(InMemoryControlStore::new());
        let (service, owner, archive, initial) = controlled_sync_log_backup_fixture(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            objects.clone(),
            "metadata/log-prune-delete-failure",
        );
        service
            .create_dir_path("/covered", 0o755, 1000, 1000)
            .unwrap();
        let covered = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(covered.segments.len(), 1);
        let covered_key = ObjectKey::new(covered.segments[0].segment_key.clone()).unwrap();
        objects.fail_deletes_containing(covered_key.as_str());

        let outcome =
            run_controlled_metadata_backup_once(service.as_ref(), &owner, &archive).unwrap();

        assert_ne!(outcome.checkpoint_key, initial.checkpoint_key);
        assert_eq!(outcome.log_segments_pruned, 1);
        assert_eq!(outcome.log_segment_objects_deleted, 0);
        assert_eq!(outcome.log_segment_objects_missing, 0);
        assert_eq!(outcome.log_segment_delete_failures, 1);
        assert!(objects.head(&covered_key).unwrap().is_some());
        assert!(service
            .sync_metadata_log_snapshot()
            .unwrap()
            .segments
            .is_empty());
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint
                .unwrap()
                .object_key,
            outcome.checkpoint_key
        );
        owner.release().unwrap();
    }

    #[test]
    fn snapshot_create_archive_failure_stays_unpublished_until_exact_pending_flush() {
        let objects = BlockingPutStore::new();
        let control = Arc::new(InMemoryControlStore::new());
        let (service, owner, archive, initial) = controlled_sync_log_backup_fixture(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            objects.clone(),
            "metadata/snapshot-create-pending",
        );
        service
            .create_dir_path("/scope", 0o755, 1000, 1000)
            .unwrap();
        Server::publish_owner_latest_log_ref(service.as_ref(), &owner).unwrap();
        let before = service.sync_metadata_log_snapshot().unwrap();
        let control_before = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(control_before.durable_lsn, before.durable_lsn);

        objects.fail_puts_containing("metadata/snapshot-create-pending/shared-log/log/");
        owner.mark_recovery_dirty();
        assert!(matches!(
            service.snapshot_subtree_path_with_lease("/scope", 10_000),
            Err(MetadError::SyncLogArchiveFailed {
                committed: true,
                ..
            })
        ));
        assert!(matches!(
            Server::publish_owner_latest_log_ref(service.as_ref(), &owner),
            Err(ServerError::Metadata(MetadError::SyncLogArchiveFailed {
                committed: true,
                ..
            }))
        ));
        let pins = local_snapshot_pins(service.as_ref());
        assert_eq!(pins.len(), 1, "the failed ACK still applied one exact pin");
        assert_eq!(service.sync_metadata_log_snapshot().unwrap(), before);
        assert_eq!(
            control.get_shard(&ShardId::new("mount-1:/")).unwrap(),
            control_before
        );

        // Retrying while the exact committed segment is still unavailable must
        // fail before applying a second snapshot pin.
        assert!(matches!(
            service.snapshot_subtree_path_with_lease("/scope", 10_000),
            Err(MetadError::SyncLogArchiveFailed {
                committed: false,
                ..
            })
        ));
        assert_eq!(local_snapshot_pins(service.as_ref()), pins);

        objects.clear_put_failures();
        service
            .create_dir_path("/flush-trigger", 0o755, 1000, 1000)
            .unwrap();
        let published = Server::publish_owner_latest_log_ref(service.as_ref(), &owner)
            .unwrap()
            .unwrap();
        assert!(!owner.recovery_is_dirty());
        let tail = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(published.durable_lsn, tail.durable_lsn);
        assert!(tail.durable_lsn >= before.durable_lsn + 2);

        let recovered = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        );
        recovered
            .restore_metadata_checkpoint_with_archived_log_segments(
                &archive,
                "mount-1:/",
                &MetadataCheckpointIdentity {
                    checkpoint_key: initial.checkpoint_key,
                    image_bytes: initial.image_bytes,
                    image_digest: initial.image_digest,
                },
                &local_log_segment_keys(&tail),
                initial.log_lsn,
                initial.log_digest,
            )
            .unwrap();
        assert_eq!(
            recovered
                .snapshot_pin_path("/scope", pins[0].snapshot_id)
                .unwrap(),
            Some(pins[0].clone())
        );
        owner.release().unwrap();
    }

    #[test]
    fn snapshot_renew_exact_retry_cannot_ack_before_pending_tail_publication() {
        let objects = BlockingPutStore::new();
        let control = Arc::new(InMemoryControlStore::new());
        let (service, owner, archive, initial) = controlled_sync_log_backup_fixture(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            objects.clone(),
            "metadata/snapshot-renew-pending",
        );
        service.set_clock_override_ms(1_000);
        service
            .create_dir_path("/scope", 0o755, 1000, 1000)
            .unwrap();
        let pin = service
            .snapshot_subtree_path_with_lease("/scope", 10_000)
            .unwrap();
        Server::publish_owner_latest_log_ref(service.as_ref(), &owner).unwrap();
        let before = service.sync_metadata_log_snapshot().unwrap();
        let control_before = control.get_shard(&ShardId::new("mount-1:/")).unwrap();

        objects.fail_puts_containing("metadata/snapshot-renew-pending/shared-log/log/");
        owner.mark_recovery_dirty();
        assert!(matches!(
            service.renew_snapshot_path("/scope", pin.snapshot_id, 20_000),
            Err(MetadError::SyncLogArchiveFailed {
                committed: true,
                ..
            })
        ));
        let renewed = service
            .snapshot_pin_path("/scope", pin.snapshot_id)
            .unwrap()
            .unwrap();
        assert_eq!(renewed.lease_expires_unix_ms, 21_000);
        assert_eq!(service.sync_metadata_log_snapshot().unwrap(), before);
        assert_eq!(
            control.get_shard(&ShardId::new("mount-1:/")).unwrap(),
            control_before
        );

        // Exact retry observes the locally-applied expiry and returns a no-op;
        // the RPC post barrier must still reject it while the pending segment
        // cannot be archived and control remains at the old tail.
        assert!(matches!(
            service
                .renew_snapshot_path("/scope", pin.snapshot_id, 20_000)
                .unwrap(),
            nokv_meta::SnapshotRenewOutcome::Renewed {
                extended: false,
                ..
            }
        ));
        assert!(matches!(
            Server::publish_owner_latest_log_ref(service.as_ref(), &owner),
            Err(ServerError::Metadata(MetadError::SyncLogArchiveFailed {
                committed: true,
                ..
            }))
        ));
        assert_eq!(
            control.get_shard(&ShardId::new("mount-1:/")).unwrap(),
            control_before
        );

        objects.clear_put_failures();
        assert!(matches!(
            service
                .renew_snapshot_path("/scope", pin.snapshot_id, 20_000)
                .unwrap(),
            nokv_meta::SnapshotRenewOutcome::Renewed {
                extended: false,
                ..
            }
        ));
        let published = Server::publish_owner_latest_log_ref(service.as_ref(), &owner)
            .unwrap()
            .unwrap();
        let tail = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(published.durable_lsn, before.durable_lsn + 1);
        assert_eq!(tail.durable_lsn, published.durable_lsn);
        assert!(!owner.recovery_is_dirty());

        let recovered = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        );
        recovered
            .restore_metadata_checkpoint_with_archived_log_segments(
                &archive,
                "mount-1:/",
                &MetadataCheckpointIdentity {
                    checkpoint_key: initial.checkpoint_key,
                    image_bytes: initial.image_bytes,
                    image_digest: initial.image_digest,
                },
                &local_log_segment_keys(&tail),
                initial.log_lsn,
                initial.log_digest,
            )
            .unwrap();
        assert_eq!(
            recovered
                .snapshot_pin_path("/scope", pin.snapshot_id)
                .unwrap()
                .unwrap()
                .lease_expires_unix_ms,
            renewed.lease_expires_unix_ms
        );
        owner.release().unwrap();
    }

    #[test]
    fn controlled_startup_renewal_survives_checkpoint_put_longer_than_ttl() {
        let objects = BlockingPutStore::new();
        let service = Arc::new(NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        ));
        service
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let renewal_options = ServerShardOwnerRenewalOptions {
            interval: Duration::from_millis(5),
            run_immediately: true,
            lease_ttl: Duration::from_millis(30),
        };
        let owner = ServerShardOwner::acquire(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                .with_renewal(Some(renewal_options)),
            service.as_ref(),
        )
        .unwrap();
        let state = owner.state().unwrap();
        service
            .enable_sync_metadata_log(MetadataLogSyncConfig::new(
                "metadata/slow-startup-log",
                state.shard_id.as_str(),
                state.epoch,
                0,
                METADATA_LOG_ZERO_DIGEST,
            ))
            .unwrap();
        let mut renewal = ServerShardOwnerRenewalWorker::spawn(
            Arc::clone(&service),
            owner.clone(),
            renewal_options,
        );
        let archive = MetadataArchiveConfig::new("metadata/slow-startup", 2);
        objects.arm();
        let backup_service = Arc::clone(&service);
        let backup_owner = owner.clone();
        let backup = thread::spawn(move || {
            run_controlled_metadata_backup_once(backup_service.as_ref(), &backup_owner, &archive)
        });
        objects.wait_until_reached();
        let recovering = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(recovering.state, ShardState::Recovering);
        assert!(recovering.checkpoint.is_none());
        let blocked_at = Instant::now();
        wait_until(|| {
            blocked_at.elapsed() >= Duration::from_millis(60) && renewal.state().iterations >= 3
        });
        objects.release();

        let outcome = backup.join().unwrap().unwrap();
        assert!(outcome.checkpoint_key.contains("/controlled/sha256/"));
        assert!(renewal.state().last_error.is_none());
        let state = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(state.state, ShardState::Serving);
        assert_eq!(state.checkpoint.unwrap().object_key, outcome.checkpoint_key);
        renewal.stop();
        owner.release().unwrap();
    }

    #[test]
    fn concurrent_controlled_backups_publish_in_capture_order_and_prune_after_cas() {
        let objects = BlockingPutStore::new();
        let service = Arc::new(NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        ));
        service
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        let control = Arc::new(BlockingServingControl::new());
        let owner = ServerShardOwner::acquire(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None),
            service.as_ref(),
        )
        .unwrap();
        let archive = MetadataArchiveConfig::new("metadata/ordered-checkpoint", 2);
        let state = owner.state().unwrap();
        service
            .enable_sync_metadata_log(MetadataLogSyncConfig::new(
                "metadata/ordered-checkpoint-log",
                state.shard_id.as_str(),
                state.epoch,
                0,
                METADATA_LOG_ZERO_DIGEST,
            ))
            .unwrap();

        let _initial =
            run_controlled_metadata_backup_once(service.as_ref(), &owner, &archive).unwrap();
        service
            .create_dir_path("/captured-by-a", 0o755, 1000, 1000)
            .unwrap();

        control.arm_mark();
        let a_service = Arc::clone(&service);
        let a_owner = owner.clone();
        let a_archive = archive.clone();
        let backup_a = thread::spawn(move || {
            run_controlled_metadata_backup_once(a_service.as_ref(), &a_owner, &a_archive)
        });
        control.wait_until_mark_reached();

        // A has captured and uploaded its image but has not published it. The
        // later mutation must belong only to B's checkpoint.
        service
            .create_dir_path("/captured-by-b", 0o755, 1000, 1000)
            .unwrap();
        objects.arm();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let b_service = Arc::clone(&service);
        let b_owner = owner.clone();
        let b_archive = archive.clone();
        let backup_b = thread::spawn(move || {
            started_tx.send(()).unwrap();
            run_controlled_metadata_backup_once(b_service.as_ref(), &b_owner, &b_archive)
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            !objects.has_reached(),
            "backup B must not capture/upload while backup A owns the publication gate"
        );

        control.release_mark();
        objects.wait_until_reached();
        let err = backup_a.join().unwrap().unwrap_err();
        assert!(
            err.to_string()
                .contains("published checkpoint does not exactly cover"),
            "unexpected stale-capture result: {err}"
        );
        let after_a = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        let checkpoint_a = after_a.checkpoint.as_ref().unwrap().clone();
        assert!(objects
            .head(&ObjectKey::new(checkpoint_a.object_key.clone()).unwrap())
            .unwrap()
            .is_some());
        assert!(objects
            .head(&ObjectKey::new(format!("{}.proof", checkpoint_a.object_key)).unwrap())
            .unwrap()
            .is_some());

        // B's image PUT is still blocked, so A remains both authoritative and
        // physically present. Only B's successful control CAS may prune A.
        objects.release();
        let outcome_b = backup_b.join().unwrap().unwrap();
        let final_record = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        let final_checkpoint = final_record.checkpoint.unwrap();
        assert_eq!(final_checkpoint.object_key, outcome_b.checkpoint_key);
        assert_ne!(checkpoint_a.object_key, outcome_b.checkpoint_key);
        assert!(objects
            .head(&ObjectKey::new(checkpoint_a.object_key.clone()).unwrap())
            .unwrap()
            .is_none());
        assert!(objects
            .head(&ObjectKey::new(format!("{}.proof", checkpoint_a.object_key)).unwrap())
            .unwrap()
            .is_none());
        assert!(objects
            .head(&ObjectKey::new(outcome_b.checkpoint_key.clone()).unwrap())
            .unwrap()
            .is_some());

        let restored = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects,
        );
        restored
            .restore_metadata_checkpoint(&archive, &checkpoint_identity(&final_checkpoint))
            .unwrap();
        assert!(restored.lookup_path("/captured-by-a").unwrap().is_some());
        assert!(restored.lookup_path("/captured-by-b").unwrap().is_some());
        owner.release().unwrap();
    }

    #[test]
    fn controlled_startup_failure_releases_current_and_prior_shard_owners() {
        let dir = tempdir().unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/partial-open".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let control = Arc::new(FailingCheckpointPublishControl::new());
        control.fail_mark_on_call(2);

        let result = Server::open_with_objects(
            options,
            objects,
            Some((
                control.clone(),
                vec![
                    ServerShardOwnerOptions::fresh("mount-1:/", "node-root")
                        .with_renewal(None)
                        .with_shard_index(Some(0))
                        .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                            "metadata/partial-open-log",
                        ))),
                    ServerShardOwnerOptions::fresh("mount-1:/dataset", "node-dataset")
                        .with_renewal(None)
                        .with_shard_index(Some(1))
                        .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                            "metadata/partial-open-log",
                        ))),
                ],
            )),
        );
        assert!(result.is_err());

        for shard_id in ["mount-1:/", "mount-1:/dataset"] {
            let record = control.get_shard(&ShardId::new(shard_id)).unwrap();
            assert!(record.owner.is_none(), "{shard_id} owner must be released");
            assert_eq!(record.state, ShardState::Unassigned);
        }

        // Neither a failed current slot nor an already-open prior slot may
        // strand ownership and block an immediate retry.
        let lease = control
            .acquire_unassigned(ShardId::new("mount-1:/"), NodeId::new("node-retry"))
            .unwrap();
        assert_eq!(
            control.get_shard(&lease.shard_id).unwrap().state,
            ShardState::Recovering
        );
        control.release(&lease).unwrap();
    }

    #[test]
    fn controlled_startup_lost_lease_never_marks_serving() {
        let dir = tempdir().unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/lost-startup".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let control = Arc::new(BlockingServingControl::new());
        control.arm_mark();
        let open_control = Arc::clone(&control);
        let open = thread::spawn(move || {
            Server::open_with_objects(
                options,
                objects,
                Some((
                    open_control as Arc<dyn ControlStore>,
                    vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                        .with_renewal(Some(ServerShardOwnerRenewalOptions {
                            interval: Duration::from_millis(10),
                            run_immediately: true,
                            lease_ttl: Duration::from_secs(5),
                        }))
                        .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                            "metadata/lost-startup-log",
                        )))],
                )),
            )
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !control.mark_reached() && !open.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        if open.is_finished() {
            match open.join().unwrap() {
                Err(err) => panic!("startup failed before mark-serving CAS: {err}"),
                Ok(_) => panic!("startup unexpectedly completed before mark-serving CAS"),
            }
        }
        control.wait_until_mark_reached();
        control
            .acquire_after_failure(ShardId::new("mount-1:/"), NodeId::new("node-b"), 1)
            .unwrap();
        control.release_mark();

        assert!(open.join().unwrap().is_err());
        let state = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(state.owner, Some(NodeId::new("node-b")));
        assert_eq!(state.epoch, 2);
        assert_eq!(state.state, ShardState::Recovering);
        assert!(state.checkpoint.is_none());
    }

    #[test]
    fn offline_restore_preserves_archived_failover_marker() {
        let dir = tempdir().unwrap();
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let source_dir = dir.path().join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_store = HoltMetadataStore::open_file(source_dir.join("metadata.holt")).unwrap();
        let source = NoKvFs::new(
            MountId::new(1).unwrap(),
            ServerMetadataStore::direct(source_store),
            objects.clone(),
        );
        source
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        let archive = MetadataArchiveConfig::new("metadata/offline-restore", 2);
        let backup = source.backup_metadata(&archive).unwrap();
        drop(source);

        let target_root = dir.path().join("target");
        let mut options = test_options(&target_root);
        options.metadata_checkpoint_archive_prefix = Some(archive.prefix.clone());
        let report = restore_with_objects(options.clone(), objects).unwrap();
        assert!(report.contains(&format!("\"commit_version\":{}", backup.commit_version)));

        let restored =
            HoltMetadataStore::open_file(default_metadata_state_path(&options.meta_path)).unwrap();
        let marker = restored
            .get_versioned(
                RecordFamily::System,
                &failover_durability_required_key(options.mount),
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .expect("offline restore must persist the failover marker");
        assert_eq!(marker.value.0, vec![1]);
    }

    #[test]
    fn single_node_startup_recovers_object_gc_claim_before_ready() {
        let dir = tempdir().unwrap();
        let options = test_options(dir.path());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let seeded = seed_crash_left_object_gc_claim(&options, &options.meta_path, objects.clone());

        let server = Server::open_with_objects(options, objects, None).unwrap();
        assert!(server.stats_json().contains("\"ready\":true"));
        drop(server);

        let holt = HoltMetadataStore::open_file(&seeded.metadata_path).unwrap();
        let latest = Version::new(u64::MAX).unwrap();
        assert!(holt
            .get_versioned(
                RecordFamily::Gc,
                &seeded.gc_key,
                latest,
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .is_none());
        assert_eq!(
            holt.get(
                RecordFamily::System,
                &seeded.claim_key,
                latest,
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
            vec![1]
        );
    }

    #[test]
    fn single_node_archive_marker_blocks_uncertain_object_gc_recovery() {
        let dir = tempdir().unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/single-node".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let seeded = seed_crash_left_object_gc_claim(&options, &options.meta_path, objects.clone());

        let result = Server::open_with_objects(options.clone(), objects, None);
        assert_startup_requires_object_gc_intervention(result, seeded.operation_token);
        assert_failover_gate_preserves_crash_claim(&options, &seeded, true);
    }

    #[test]
    fn shared_log_marker_is_installed_before_object_gc_claim_recovery() {
        let dir = tempdir().unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/startup-order-ck".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let shard_id = "mount-1:/";
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard_id));
        let seeded = seed_crash_left_object_gc_claim(&options, &shard_meta_dir, objects.clone());
        let control: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
        let owner = ServerShardOwnerOptions::fresh(shard_id, "node-a").with_shared_log(Some(
            crate::ServerSharedLogOptions::new("metadata/startup-order"),
        ));

        let result =
            Server::open_with_objects(options.clone(), objects, Some((control, vec![owner])));
        assert_startup_requires_object_gc_intervention(result, seeded.operation_token);
        assert_failover_gate_preserves_crash_claim(&options, &seeded, true);
    }

    #[test]
    fn controlled_marker_without_shared_log_blocks_uncertain_object_gc_recovery() {
        let dir = tempdir().unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/controlled-marker".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let shard_id = "mount-1:/";
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard_id));
        let seeded = seed_crash_left_object_gc_claim(&options, &shard_meta_dir, objects.clone());
        let control: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
        let owner = ServerShardOwnerOptions::fresh(shard_id, "node-a");

        let result =
            Server::open_with_objects(options.clone(), objects, Some((control, vec![owner])));
        assert!(matches!(
            result,
            Err(ServerError::Control(ControlError::InvalidOptions(message)))
                if message.contains("requires synchronous shared_log")
        ));
        assert_failover_gate_preserves_crash_claim(&options, &seeded, false);
    }

    #[test]
    fn manual_gc_reports_empty_outcomes() {
        let server = test_server();
        assert!(server.stats_json().contains("\"ready\":true"));
        assert!(server
            .stats_json()
            .contains("\"shard_owner\":{\"enabled\":false}"));
        let body = server.run_manual_gc(128).unwrap();
        assert!(body.contains("\"object_gc\""));
        assert!(body.contains("\"history_gc\""));
    }

    #[test]
    fn controlled_stats_report_persisted_failover_fence_with_empty_gc_queue() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = open_memory_controlled(
            dir.path(),
            control,
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")],
        )
        .unwrap();

        let cleanup = server.service().cleanup_pending_objects(128).unwrap();
        assert_eq!(cleanup.scanned, 0);
        assert!(server
            .stats_json()
            .contains("\"object_gc\":{\"failover_durability_required\":true"));
    }

    #[test]
    fn object_gc_health_keeps_cumulative_reaper_conflicts() {
        let last_outcome = nokv_meta::PendingObjectCleanupOutcome {
            blocked_by_failover_durability: 4,
            ..Default::default()
        };
        let state = ObjectGcWorkerState {
            iterations: 9,
            last_outcome: Some(last_outcome),
            snapshot_reap: nokv_meta::SnapshotReapOutcome {
                scanned: 12,
                expired_candidates: 3,
                reaped: 2,
                conflicted: 1,
            },
            last_error: None,
        };

        let json = object_gc_json(&state, true);
        assert!(json.contains("\"failover_durability_required\":true"));
        assert!(json.contains("\"iterations\":9"));
        assert!(json.contains("\"blocked_by_failover_durability\":4"));
        assert!(json.contains("\"conflicted\":1"));
    }

    #[test]
    fn controlled_open_acquires_shard_and_installs_owner_epoch() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = open_memory_controlled(
            dir.path(),
            control,
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")],
        )
        .unwrap();

        let state = server.shard_owner_state().unwrap().unwrap();
        assert_eq!(state.shard_id.as_str(), "mount-1:/");
        assert_eq!(state.node_id.as_str(), "node-a");
        assert_eq!(state.epoch, 1);
        assert_eq!(state.lease_id, 1);
        assert_eq!(state.state, nokv_control::ShardState::Serving);
        assert_eq!(server.service().allocator_epoch(), 1);
        assert_eq!(server.service().required_owner_epoch(), 1);
        assert!(server
            .stats_json()
            .contains("\"shard_owner\":{\"enabled\":true"));
        assert!(server
            .stats_json()
            .contains("\"renewal\":{\"enabled\":true"));
    }

    #[test]
    fn controlled_archive_is_refreshed_and_published_before_serving() {
        let dir = tempdir().unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/controlled".to_owned());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let control = Arc::new(InMemoryControlStore::new());

        let server = Server::open_with_objects(
            options,
            objects,
            Some((
                control.clone(),
                vec![
                    ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_shared_log(Some(
                        crate::ServerSharedLogOptions::new("metadata/controlled-log"),
                    )),
                ],
            )),
        )
        .unwrap();

        let state = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(state.state, nokv_control::ShardState::Serving);
        let checkpoint = state
            .checkpoint
            .expect("serving transition must publish the startup checkpoint");
        assert!(checkpoint
            .object_key
            .starts_with("metadata/controlled/mount_1__/controlled/sha256/"));
        assert!(checkpoint.image_bytes > 0);
        assert!(checkpoint.image_digest.starts_with("sha256:"));
        assert_eq!(checkpoint.lsn, state.durable_lsn);
        assert!(server.stats_json().contains("\"ready\":true"));
    }

    #[test]
    fn controlled_periodic_backup_is_exactly_restorable_after_failover() {
        let dir = tempdir().unwrap();
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let control = Arc::new(InMemoryControlStore::new());
        let mut first_options = test_options(&dir.path().join("first"));
        first_options.metadata_checkpoint_archive_prefix = Some("metadata/periodic".to_owned());
        let first = Server::open_with_objects(
            first_options,
            objects.clone(),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new("metadata/log")))],
            )),
        )
        .unwrap();
        first
            .service()
            .create_dir_path("/periodic", 0o755, 1000, 1000)
            .unwrap();

        let slot = first.default_slot();
        let mut options = MetadataBackupOptions::new(slot.metadata_archive.clone().unwrap());
        options.interval = Duration::from_secs(3600);
        options.run_immediately = true;
        let mut worker = ControlledMetadataBackupWorker::spawn(
            Arc::clone(&slot.service),
            slot.owner.clone().unwrap(),
            options,
        );
        wait_until(|| worker.state().iterations >= 1);
        assert!(worker.state().last_error.is_none());
        worker.stop();
        let periodic_ref = control
            .get_shard(&ShardId::new("mount-1:/"))
            .unwrap()
            .checkpoint
            .unwrap();
        drop(first);

        let mut second_options = test_options(&dir.path().join("second"));
        second_options.metadata_checkpoint_archive_prefix = Some("metadata/periodic".to_owned());
        let second = Server::open_with_objects(
            second_options,
            objects.clone(),
            Some((
                control,
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/periodic-log",
                    )))],
            )),
        )
        .unwrap();
        assert!(second.service().lookup_path("/periodic").unwrap().is_some());
        let successor_ref = second
            .shard_owner_state()
            .unwrap()
            .unwrap()
            .checkpoint
            .unwrap();
        let digest = successor_ref.image_digest.strip_prefix("sha256:").unwrap();
        assert_eq!(
            successor_ref.object_key,
            format!("metadata/periodic/mount_1__/controlled/sha256/{digest}.image")
        );
        assert!(objects
            .head(&ObjectKey::new(format!("{}.proof", successor_ref.object_key)).unwrap())
            .unwrap()
            .is_some());
        assert_ne!(
            successor_ref.object_key, periodic_ref.object_key,
            "failover rotates the object-GC claim version before its startup checkpoint"
        );
        assert!(objects
            .head(&ObjectKey::new(periodic_ref.object_key.clone()).unwrap())
            .unwrap()
            .is_none());
        assert!(objects
            .head(&ObjectKey::new(format!("{}.proof", periodic_ref.object_key)).unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn controlled_startup_upload_window_keeps_previous_ref_recoverable() {
        let dir = tempdir().unwrap();
        let memory = Arc::new(MemoryObjectStore::new());
        let objects = ConfiguredObjectStore::Memory(Arc::clone(&memory));
        let control = Arc::new(InMemoryControlStore::new());
        let mut first_options = test_options(&dir.path().join("first"));
        first_options.metadata_checkpoint_archive_prefix = Some("metadata/window".to_owned());
        let first = Server::open_with_objects(
            first_options,
            objects.clone(),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/window-log",
                    )))],
            )),
        )
        .unwrap();
        first
            .service()
            .create_dir_path("/authoritative", 0o755, 1000, 1000)
            .unwrap();
        first.run_manual_backup().unwrap();
        let authoritative = control
            .get_shard(&ShardId::new("mount-1:/"))
            .unwrap()
            .checkpoint
            .unwrap();
        first
            .service()
            .create_dir_path("/orphan", 0o755, 1000, 1000)
            .unwrap();
        let archive = first.default_slot().metadata_archive.as_ref().unwrap();
        let orphan = first
            .service()
            .prepare_immutable_metadata_backup(archive)
            .unwrap();
        assert_ne!(orphan.checkpoint_key, authoritative.object_key);
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint
                .as_ref(),
            Some(&authoritative)
        );
        assert!(memory
            .head(&ObjectKey::new(authoritative.object_key.clone()).unwrap())
            .unwrap()
            .is_some());
        assert!(memory
            .head(&ObjectKey::new(orphan.checkpoint_key).unwrap())
            .unwrap()
            .is_some());
        assert!(memory
            .head(&ObjectKey::new("metadata/window/mount_1__/CURRENT").unwrap())
            .unwrap()
            .is_none());
        drop(first);

        let mut second_options = test_options(&dir.path().join("second"));
        second_options.metadata_checkpoint_archive_prefix = Some("metadata/window".to_owned());
        let second = Server::open_with_objects(
            second_options,
            objects,
            Some((
                control,
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/window-log",
                    )))],
            )),
        )
        .unwrap();
        assert!(second
            .service()
            .lookup_path("/authoritative")
            .unwrap()
            .is_some());
        assert!(second.service().lookup_path("/orphan").unwrap().is_none());
    }

    #[test]
    fn expired_or_stale_owner_cannot_publish_or_prune_controlled_archive() {
        let dir = tempdir().unwrap();
        let memory = Arc::new(MemoryObjectStore::new());
        let objects = ConfiguredObjectStore::Memory(Arc::clone(&memory));
        let control = Arc::new(InMemoryControlStore::new());
        let mut first_options = test_options(&dir.path().join("first"));
        first_options.metadata_checkpoint_archive_prefix = Some("metadata/fenced-owner".to_owned());
        let first = Server::open_with_objects(
            first_options,
            objects.clone(),
            Some((
                control.clone(),
                vec![
                    ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_shared_log(Some(
                        crate::ServerSharedLogOptions::new("metadata/fenced-owner-log"),
                    )),
                ],
            )),
        )
        .unwrap();
        let old_service = Arc::clone(&first.default_slot().service);
        let old_owner = first.default_slot().owner.clone().unwrap();
        let archive = first.default_slot().metadata_archive.clone().unwrap();
        let first_ref = control
            .get_shard(&ShardId::new("mount-1:/"))
            .unwrap()
            .checkpoint
            .unwrap();

        let deadline = old_service.lease_deadline_ms();
        assert!(deadline > 0);
        old_service.set_clock_override_ms(deadline);
        assert!(matches!(
            run_controlled_metadata_backup_once(&old_service, &old_owner, &archive),
            Err(ServerError::Metadata(MetadError::LeaseExpired { .. }))
        ));
        old_service.set_clock_override_ms(0);
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint
                .as_ref(),
            Some(&first_ref)
        );
        drop(first);

        let mut second_options = test_options(&dir.path().join("second"));
        second_options.metadata_checkpoint_archive_prefix =
            Some("metadata/fenced-owner".to_owned());
        let second = Server::open_with_objects(
            second_options,
            objects,
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/fenced-owner-log",
                    )))],
            )),
        )
        .unwrap();
        let successor_ref = control
            .get_shard(&ShardId::new("mount-1:/"))
            .unwrap()
            .checkpoint
            .unwrap();
        assert!(run_controlled_metadata_backup_once(&old_service, &old_owner, &archive).is_err());
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint
                .as_ref(),
            Some(&successor_ref)
        );
        assert!(memory
            .head(&ObjectKey::new(successor_ref.object_key.clone()).unwrap())
            .unwrap()
            .is_some());
        assert!(memory
            .head(&ObjectKey::new(format!("{}.proof", successor_ref.object_key)).unwrap())
            .unwrap()
            .is_some());
        assert!(memory
            .head(&ObjectKey::new("metadata/fenced-owner/mount_1__/CURRENT").unwrap())
            .unwrap()
            .is_none());
        drop(second);
    }

    #[test]
    fn repeated_control_publish_failures_preserve_previous_checkpoint() {
        let dir = tempdir().unwrap();
        let memory = Arc::new(MemoryObjectStore::new());
        let objects = ConfiguredObjectStore::Memory(Arc::clone(&memory));
        let control = Arc::new(FailingCheckpointPublishControl::new());
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/publish-failure".to_owned());
        let server = Server::open_with_objects(
            options,
            objects,
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/publish-failure-log",
                    )))],
            )),
        )
        .unwrap();
        let previous = control
            .get_shard(&ShardId::new("mount-1:/"))
            .unwrap()
            .checkpoint
            .unwrap();
        server
            .service()
            .create_dir_path("/new-state", 0o755, 1000, 1000)
            .unwrap();
        control.fail_next_marks(2);
        assert!(server.run_manual_backup().is_err());
        assert!(server.run_manual_backup().is_err());
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .checkpoint
                .as_ref(),
            Some(&previous)
        );
        assert!(memory
            .head(&ObjectKey::new(previous.object_key.clone()).unwrap())
            .unwrap()
            .is_some());
        assert!(memory
            .head(&ObjectKey::new(format!("{}.proof", previous.object_key)).unwrap())
            .unwrap()
            .is_some());
        assert!(memory
            .head(&ObjectKey::new("metadata/publish-failure/mount_1__/CURRENT").unwrap())
            .unwrap()
            .is_none());

        let report = server.run_manual_backup().unwrap();
        assert!(report.contains("\"checkpoint_key\""));
        let published = control
            .get_shard(&ShardId::new("mount-1:/"))
            .unwrap()
            .checkpoint
            .unwrap();
        assert_ne!(published.object_key, previous.object_key);
        assert!(memory
            .head(&ObjectKey::new(previous.object_key).unwrap())
            .unwrap()
            .is_none());
    }

    /// An owner that declares `shard_index` registers its shard identity (prefix +
    /// index) on open even when nothing pre-registered it — the path a multi-process
    /// `nokv serve --shard-index N` fleet relies on. The control record then carries
    /// the declared index, and the server routes the shard's subtree to that slot.
    #[test]
    fn controlled_open_with_shard_index_registers_identity() {
        use nokv_protocol::MetadataRpcRequest;
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        // No register_shard call: the owner's declared index is the only source of
        // the shard's identity.
        let server = open_memory_controlled(
            dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/dataset", "node-a")
                .with_renewal(None)
                .with_shard_index(Some(1))],
        )
        .unwrap();

        // The control record now carries the declared index and the derived prefix.
        let record = control
            .get_shard(&nokv_control::ShardId::new("mount-1:/dataset"))
            .unwrap();
        assert_eq!(record.shard_index, 1, "open registered the declared index");
        assert_eq!(
            record.prefix, "/dataset",
            "prefix derived from the shard id"
        );

        // The server routes a /dataset path to the index-1 slot it just registered.
        let slot = server
            .route(&MetadataRpcRequest::CreateDirPath {
                path: "/dataset/run".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        assert_eq!(slot.shard_index(), 1);
    }

    #[test]
    fn graft_reconcile_does_not_fall_back_to_default_for_a_remote_nested_parent() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        nokv_control::register_shard(
            control.as_ref(),
            ShardId::new("mount-1:/"),
            "/",
            DEFAULT_SHARD_INDEX,
        )
        .unwrap();
        nokv_control::register_shard(
            control.as_ref(),
            ShardId::new("mount-1:/dataset"),
            "/dataset",
            1,
        )
        .unwrap();
        nokv_control::register_shard(
            control.as_ref(),
            ShardId::new("mount-1:/dataset/images"),
            "/dataset/images",
            2,
        )
        .unwrap();

        // This process hosts only the default shard. Leave a local /dataset
        // directory in place so the former local-only routing bug would have
        // inserted the nested "images" graft into it.
        let server = open_memory_controlled(
            dir.path(),
            Arc::clone(&control),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-root").with_renewal(None)],
        )
        .unwrap();
        server
            .service()
            .create_dir_path("/dataset", 0o755, 1000, 1000)
            .unwrap();
        server.publish_latest_metadata_log_ref().unwrap();

        let child = InodeId::compose(2, 7).unwrap();
        control
            .set_subtree_root_inode(&ShardId::new("mount-1:/dataset/images"), Some(child.get()))
            .unwrap();
        assert!(server
            .service()
            .lookup_path("/dataset/images")
            .unwrap()
            .is_none());

        server.reconcile_local_grafts().unwrap();

        assert!(server
            .service()
            .lookup_path("/dataset/images")
            .unwrap()
            .is_none());
    }

    fn assert_failover_without_checkpoint_archive_is_rejected(
        control: Arc<InMemoryControlStore>,
        with_shared_log: bool,
    ) {
        let dir = tempdir().unwrap();
        let mut owner =
            ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1).with_renewal(None);
        if with_shared_log {
            owner = owner.with_shared_log(Some(crate::ServerSharedLogOptions::new("metadata/log")));
        }
        let result = Server::open_with_objects(
            test_options(dir.path()),
            ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new())),
            Some((control, vec![owner])),
        );
        match result {
            Err(ServerError::Metadata(MetadError::InvalidPath(message))) => assert!(
                message.contains("failover recovery requires a metadata checkpoint archive"),
                "unexpected strict-restore error: {message}"
            ),
            Err(err) => panic!("unexpected failover error: {err}"),
            Ok(_) => panic!("failover without a checkpoint archive must be rejected"),
        }
    }

    fn test_controlled_checkpoint_ref(archive_prefix: &str, shard_id: &str) -> CheckpointRef {
        let image_hex = "a".repeat(64);
        CheckpointRef {
            object_key: format!(
                "{}/controlled/sha256/{image_hex}.image",
                shard_archive_prefix(archive_prefix, &sanitize_shard_id(shard_id))
            ),
            lsn: 0,
            image_bytes: 1,
            image_digest: format!("sha256:{image_hex}"),
            digest: "0".repeat(64),
        }
    }

    #[test]
    fn fresh_control_owned_open_requires_a_checkpoint_archive_before_acquire() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let result = Server::open_with_objects(
            test_options(dir.path()),
            ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new("metadata/log")))],
            )),
        );
        match result {
            Err(ServerError::Metadata(MetadError::InvalidPath(message))) => assert!(
                message.contains("control-owned shard requires a metadata checkpoint archive"),
                "unexpected deployment-gate error: {message}"
            ),
            Err(err) => panic!("unexpected controlled-open error: {err}"),
            Ok(_) => panic!("control-owned open without an archive must be rejected"),
        }
        let record = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(record.epoch, 0, "deployment preflight must precede acquire");
        assert_eq!(record.owner, None);
        assert_eq!(record.state, ShardState::Unassigned);
    }

    #[test]
    fn fresh_control_owned_open_requires_shared_log_before_any_side_effect() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let before = control.ensure_shard(shard.clone()).unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/checkpoint".to_owned());
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard.as_str()));
        let objects = MemoryObjectStore::new();

        let result = Server::open_with_objects(
            options,
            ConfiguredObjectStore::Memory(Arc::new(objects.clone())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
            )),
        );

        assert!(matches!(
            result,
            Err(ServerError::Control(ControlError::InvalidOptions(message)))
                if message.contains("control-owned shard")
                    && message.contains("requires synchronous shared_log")
        ));
        assert_eq!(control.get_shard(&shard).unwrap(), before);
        assert!(!shard_meta_dir.exists());
        assert_eq!(objects.object_count(), 0);
    }

    #[test]
    fn failover_without_refs_still_requires_a_checkpoint_archive() {
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard, NodeId::new("node-a"))
            .unwrap();
        control.mark_serving(&lease, None, None, 0).unwrap();
        control.release(&lease).unwrap();

        assert_failover_without_checkpoint_archive_is_rejected(control, true);
    }

    #[test]
    fn failover_with_only_a_log_ref_still_requires_a_checkpoint_archive() {
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard, NodeId::new("node-a"))
            .unwrap();
        let log = LogRef {
            segments: vec![LogSegmentRef {
                segment_key: "meta/log/segment-1".to_owned(),
                first_lsn: 1,
                last_lsn: 1,
                digest: "digest-1".to_owned(),
            }],
            durable_lsn: 1,
            digest: "digest-1".to_owned(),
        };
        control.mark_serving(&lease, None, Some(log), 1).unwrap();
        control.release(&lease).unwrap();

        assert_failover_without_checkpoint_archive_is_rejected(control, true);
    }

    #[test]
    fn failover_requires_checkpoint_identity_before_acquire_or_object_io() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard.clone(), NodeId::new("node-a"))
            .unwrap();
        control.mark_serving(&lease, None, None, 0).unwrap();
        control.release(&lease).unwrap();

        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/checkpoint".to_owned());
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard.as_str()));
        let objects = MemoryObjectStore::new();
        let result = Server::open_with_objects(
            options,
            ConfiguredObjectStore::Memory(Arc::new(objects.clone())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new("metadata/log")))],
            )),
        );

        assert!(matches!(
            result,
            Err(ServerError::Control(ControlError::InvalidOptions(message)))
                if message.contains("no durable checkpoint identity")
        ));
        let record = control.get_shard(&shard).unwrap();
        assert_eq!(record.epoch, 1, "checkpoint preflight must precede acquire");
        assert_eq!(record.owner, None);
        assert!(!shard_meta_dir.exists());
        assert_eq!(
            objects.object_count(),
            0,
            "preflight must precede object I/O"
        );
    }

    #[test]
    fn failover_rejects_cross_prefix_checkpoint_identity_before_acquire() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard.clone(), NodeId::new("node-a"))
            .unwrap();
        let mut checkpoint = test_controlled_checkpoint_ref("metadata/checkpoint", shard.as_str());
        checkpoint.object_key =
            checkpoint
                .object_key
                .replacen("metadata/checkpoint/", "metadata/another-shard/", 1);
        control
            .mark_serving(&lease, Some(checkpoint), None, 0)
            .unwrap();
        control.release(&lease).unwrap();

        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/checkpoint".to_owned());
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard.as_str()));
        let objects = MemoryObjectStore::new();
        let result = Server::open_with_objects(
            options,
            ConfiguredObjectStore::Memory(Arc::new(objects.clone())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new("metadata/log")))],
            )),
        );

        assert!(matches!(
            result,
            Err(ServerError::Metadata(MetadError::Codec(message)))
                if message.contains("does not match archive prefix and image digest")
        ));
        let record = control.get_shard(&shard).unwrap();
        assert_eq!(record.epoch, 1, "identity preflight must precede acquire");
        assert_eq!(record.owner, None);
        assert!(!shard_meta_dir.exists());
        assert_eq!(
            objects.object_count(),
            0,
            "preflight must precede object I/O"
        );
    }

    #[test]
    fn failover_rejects_cross_prefix_log_identity_before_acquire() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard.clone(), NodeId::new("node-a"))
            .unwrap();
        let checkpoint = test_controlled_checkpoint_ref("metadata/checkpoint", shard.as_str());
        let tail_digest = "b".repeat(64);
        let log = LogRef {
            segments: vec![LogSegmentRef {
                segment_key: format!(
                    "metadata/log/mount_2__/log/00000000000000000001-00000000000000000001-{tail_digest}.segment"
                ),
                first_lsn: 1,
                last_lsn: 1,
                digest: tail_digest.clone(),
            }],
            durable_lsn: 1,
            digest: tail_digest,
        };
        control
            .mark_serving(&lease, Some(checkpoint), Some(log), 1)
            .unwrap();
        control.release(&lease).unwrap();

        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/checkpoint".to_owned());
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard.as_str()));
        let objects = MemoryObjectStore::new();
        let result = Server::open_with_objects(
            options,
            ConfiguredObjectStore::Memory(Arc::new(objects.clone())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new("metadata/log")))],
            )),
        );

        assert!(matches!(
            result,
            Err(ServerError::Metadata(MetadError::Codec(message)))
                if message.contains("outside archive prefix")
        ));
        let record = control.get_shard(&shard).unwrap();
        assert_eq!(record.epoch, 1, "log preflight must precede acquire");
        assert_eq!(record.owner, None);
        assert!(!shard_meta_dir.exists());
        assert_eq!(
            objects.object_count(),
            0,
            "log preflight must precede object I/O"
        );
    }

    #[test]
    fn control_log_segment_identity_binds_loaded_range_and_tail_digest() {
        let command = MetadataCommand {
            request_id: b"control-segment-identity".to_vec(),
            kind: CommandKind::RegisterNamespaceIndex,
            read_version: Version::new(1).unwrap(),
            commit_version: Version::new(2).unwrap(),
            primary_family: RecordFamily::System,
            primary_key: b"control-segment-identity".to_vec(),
            predicates: Vec::new(),
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: b"control-segment-identity".to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"value".to_vec())),
            }],
            watch: Vec::new(),
        };
        let entry = MetadataLogEntry::seal(
            "mount-1:/",
            1,
            1,
            command,
            CommitResult {
                commit_version: Version::new(2).unwrap(),
                applied_mutations: 1,
                watch_events: 0,
            },
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap();
        let segment = MetadataLogSegment::seal(vec![entry]).unwrap();
        let reference = LogSegmentRef {
            segment_key: "metadata/log/mount_1__/log/segment".to_owned(),
            first_lsn: segment.first_lsn,
            last_lsn: segment.last_lsn,
            digest: hex_digest(&segment.last_digest),
        };
        validate_control_log_segment_identity(&reference, &segment).unwrap();

        let mut wrong_range = reference.clone();
        wrong_range.last_lsn += 1;
        assert!(matches!(
            validate_control_log_segment_identity(&wrong_range, &segment),
            Err(ServerError::Metadata(MetadError::Codec(message)))
                if message.contains("does not match control identity")
        ));

        let mut wrong_digest = reference;
        wrong_digest.digest = "c".repeat(64);
        assert!(matches!(
            validate_control_log_segment_identity(&wrong_digest, &segment),
            Err(ServerError::Metadata(MetadError::Codec(message)))
                if message.contains("does not match control identity")
        ));
    }

    #[test]
    fn failover_requires_shared_log_at_a_valid_zero_lsn_checkpoint() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard.clone(), NodeId::new("node-a"))
            .unwrap();
        let checkpoint = test_controlled_checkpoint_ref("metadata/checkpoint", shard.as_str());
        control
            .mark_serving(&lease, Some(checkpoint), None, 0)
            .unwrap();
        control.release(&lease).unwrap();
        let before = control.get_shard(&shard).unwrap();
        assert_eq!(before.state, ShardState::Unassigned);
        assert_eq!(before.durable_lsn, 0);
        assert!(before.log.is_none());

        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/checkpoint".to_owned());
        let shard_meta_dir = options.meta_path.join(sanitize_shard_id(shard.as_str()));
        let objects = MemoryObjectStore::new();
        let result = Server::open_with_objects(
            options,
            ConfiguredObjectStore::Memory(Arc::new(objects.clone())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1).with_renewal(None)],
            )),
        );

        assert!(matches!(
            result,
            Err(ServerError::Control(ControlError::InvalidOptions(message)))
                if message.contains("control-owned shard")
                    && message.contains("requires synchronous shared_log")
        ));
        let after = control.get_shard(&shard).unwrap();
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(after.owner, before.owner);
        assert_eq!(after.state, before.state);
        assert_eq!(after.checkpoint, before.checkpoint);
        assert!(!shard_meta_dir.exists());
        assert_eq!(objects.object_count(), 0);
    }

    #[test]
    fn failover_with_durable_log_requires_shared_log_before_acquire() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let lease = control
            .acquire_unassigned(shard.clone(), NodeId::new("node-a"))
            .unwrap();
        let log = LogRef {
            segments: vec![LogSegmentRef {
                segment_key: "meta/log/segment-1".to_owned(),
                first_lsn: 1,
                last_lsn: 1,
                digest: "digest-1".to_owned(),
            }],
            durable_lsn: 1,
            digest: "digest-1".to_owned(),
        };
        let checkpoint = test_controlled_checkpoint_ref("metadata/checkpoint", shard.as_str());
        control
            .mark_serving(&lease, Some(checkpoint), Some(log), 1)
            .unwrap();
        control.release(&lease).unwrap();
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/checkpoint".to_owned());

        let result = Server::open_with_objects(
            options,
            ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new())),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1).with_renewal(None)],
            )),
        );

        assert!(matches!(
            result,
            Err(ServerError::Control(ControlError::InvalidOptions(message)))
                if message.contains("control-owned shard")
                    && message.contains("requires synchronous shared_log")
        ));
        let record = control.get_shard(&shard).unwrap();
        assert_eq!(record.epoch, 1, "shared-log preflight must precede acquire");
        assert_eq!(record.owner, None);
        assert!(
            !dir.path().join(sanitize_shard_id(shard.as_str())).exists(),
            "shared-log preflight must precede local metadata creation and object publication"
        );
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn configured_etcd_control_store_expires_session_and_allows_failover() {
        let endpoints = match env::var("NOKV_ETCD_ENDPOINTS") {
            Ok(raw) if !raw.trim().is_empty() => raw
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>(),
            _ => return,
        };
        if endpoints.is_empty() {
            return;
        }
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key_prefix = format!("/nokv/test/server/{}/{}", process::id(), unique);
        let etcd_options = || {
            nokv_control::EtcdControlStoreOptions::new(endpoints.clone())
                .with_key_prefix(key_prefix.clone())
                .with_lease_ttl_seconds(1)
        };
        let control = Arc::new(
            nokv_control::EtcdControlStore::connect(etcd_options())
                .expect("connect etcd control store"),
        );
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));

        let first_dir = tempdir().unwrap();
        let mut first_options = test_options(first_dir.path());
        first_options.metadata_checkpoint_archive_prefix =
            Some("metadata/etcd-failover".to_owned());
        let first = Server::open_with_objects(
            first_options,
            objects.clone(),
            Some((
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                    .with_renewal(None)
                    .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                        "metadata/etcd-failover-log",
                    )))],
            )),
        )
        .unwrap();
        let first_state = first.shard_owner_state().unwrap().unwrap();
        assert_eq!(first_state.node_id.as_str(), "node-a");
        assert_eq!(first_state.epoch, 1);

        let second_dir = tempdir().unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        let second = loop {
            let mut second_options = test_options(second_dir.path());
            second_options.metadata_checkpoint_archive_prefix =
                Some("metadata/etcd-failover".to_owned());
            match Server::open_with_objects(
                second_options,
                objects.clone(),
                Some((
                    control.clone(),
                    vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                        .with_renewal(None)
                        .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                            "metadata/etcd-failover-log",
                        )))],
                )),
            ) {
                Ok(server) => break server,
                Err(ServerError::Control(ControlError::ShardAlreadyOwned { .. }))
                    if Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(err) => panic!("etcd failover did not acquire shard: {err}"),
            }
        };

        let state = second.shard_owner_state().unwrap().unwrap();
        assert_eq!(state.node_id.as_str(), "node-b");
        assert_eq!(state.epoch, 2);
        assert_eq!(state.state, nokv_control::ShardState::Serving);
        assert_eq!(second.service().allocator_epoch(), 2);
        assert_eq!(second.service().required_owner_epoch(), 2);
        assert_eq!(first.service().required_owner_epoch(), 1);
    }

    #[test]
    fn shard_owner_log_ref_publish_updates_control_record() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = open_memory_controlled(
            dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
        )
        .unwrap();
        let log = LogRef {
            segments: vec![LogSegmentRef {
                segment_key: "meta/shared-log/log/segment-1".to_owned(),
                first_lsn: 1,
                last_lsn: 3,
                digest: "abc123".to_owned(),
            }],
            durable_lsn: 3,
            digest: "abc123".to_owned(),
        };

        let state = server
            .publish_shard_owner_log_ref(log.clone())
            .unwrap()
            .unwrap();

        assert_eq!(state.log, Some(log.clone()));
        assert_eq!(state.durable_lsn, 3);
        let record = control
            .get_shard(&nokv_control::ShardId::new("mount-1:/"))
            .unwrap();
        assert_eq!(record.log, Some(log));
        assert_eq!(record.durable_lsn, 3);
        assert!(server.stats_json().contains(
            "\"log\":{\"segments\":[{\"segment_key\":\"meta/shared-log/log/segment-1\",\"first_lsn\":1,\"last_lsn\":3,\"digest\":\"abc123\"}],\"durable_lsn\":3"
        ));
    }

    #[test]
    fn prepare_retry_flushes_allocator_reservation_before_control_ack() {
        let objects = BlockingPutStore::new();
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        );
        service
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let owner = ServerShardOwner::acquire(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None),
            &service,
        )
        .unwrap();
        // Initialize the claim outside the log; subsequent prepare-only calls
        // produce no metadata command until the allocator boundary is crossed.
        service
            .prepare_artifact_create(
                InodeId::root(),
                DentryName::new(b"claim-warmup.bin".to_vec()).unwrap(),
            )
            .unwrap();
        service
            .enable_sync_metadata_log(MetadataLogSyncConfig::new(
                "metadata/allocator-ack-log",
                "mount-1:/",
                1,
                0,
                METADATA_LOG_ZERO_DIGEST,
            ))
            .unwrap();
        objects.fail_puts_containing("metadata/allocator-ack-log/log/");

        let mut first_error = None;
        for _ in 0..4096 {
            match service.prepare_artifact_create(
                InodeId::root(),
                DentryName::new(b"prepare-only.bin".to_vec()).unwrap(),
            ) {
                Ok(_) => {}
                Err(err) => {
                    first_error = Some(err);
                    break;
                }
            }
        }
        assert!(matches!(
            first_error,
            Some(MetadError::SyncLogArchiveFailed {
                committed: true,
                ..
            })
        ));
        assert_eq!(service.sync_metadata_log_snapshot().unwrap().durable_lsn, 0);
        assert_eq!(
            control
                .get_shard(&ShardId::new("mount-1:/"))
                .unwrap()
                .durable_lsn,
            0
        );

        objects.clear_put_failures();
        service
            .prepare_artifact_create(
                InodeId::root(),
                DentryName::new(b"retry-after-archive.bin".to_vec()).unwrap(),
            )
            .unwrap();
        let local = service.sync_metadata_log_snapshot().unwrap();
        assert_eq!(local.durable_lsn, 2);
        // This is the same publication helper the RPC response path invokes.
        // A successful external ACK occurs only after this owner-fenced CAS.
        let published = Server::publish_owner_latest_log_ref(&service, &owner)
            .unwrap()
            .unwrap();
        assert_eq!(published.durable_lsn, local.durable_lsn);
        let durable = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(durable.durable_lsn, 2);
        assert_eq!(durable.log.as_ref().unwrap().segments.len(), 2);
        owner.release().unwrap();
    }

    #[test]
    fn latest_log_snapshot_and_publication_are_owner_ordered() {
        let dir = tempdir().unwrap();
        let control = Arc::new(BlockingServingControl::new());
        let objects = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
        let mut options = test_options(dir.path());
        options.metadata_checkpoint_archive_prefix = Some("metadata/ordered-log-ck".to_owned());
        let server = Arc::new(
            Server::open_with_objects(
                options,
                objects,
                Some((
                    control.clone(),
                    vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                        .with_renewal(None)
                        .with_shared_log(Some(crate::ServerSharedLogOptions::new(
                            "meta/ordered-log",
                        )))],
                )),
            )
            .unwrap(),
        );
        server
            .service()
            .create_dir_path("/first", 0o755, 1000, 1000)
            .unwrap();

        control.arm_mark();
        let first_server = Arc::clone(&server);
        let first_publish = thread::spawn(move || first_server.publish_latest_metadata_log_ref());
        control.wait_until_mark_reached();

        server
            .service()
            .create_dir_path("/second", 0o755, 1000, 1000)
            .unwrap();
        let newest_lsn = server
            .service()
            .sync_metadata_log_snapshot()
            .unwrap()
            .durable_lsn;
        let (sent, received) = std::sync::mpsc::channel();
        let second_server = Arc::clone(&server);
        let second_publish = thread::spawn(move || {
            let result = second_server.publish_latest_metadata_log_ref();
            sent.send(result).unwrap();
        });

        // The first control CAS is deliberately blocked. The newer publisher
        // must not reach the control store (or complete) until the older
        // snapshot publication leaves the owner gate.
        if let Ok(premature) = received.recv_timeout(Duration::from_millis(200)) {
            control.release_mark();
            let _ = first_publish.join();
            let _ = second_publish.join();
            panic!("newer log publication escaped the owner gate: {premature:?}");
        }
        control.release_mark();
        let first_error = first_publish.join().unwrap().unwrap_err();
        assert!(
            first_error
                .to_string()
                .contains("authoritative recovery refs do not exactly cover"),
            "unexpected stale publication result: {first_error}"
        );
        received
            .recv_timeout(Duration::from_secs(2))
            .expect("newer publication should finish after the gate opens")
            .unwrap();
        second_publish.join().unwrap();

        let record = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
        assert_eq!(record.durable_lsn, newest_lsn);
        assert_eq!(record.log.unwrap().durable_lsn, newest_lsn);
    }

    #[test]
    fn owner_arms_lease_deadline_when_renewal_enabled() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = open_memory_controlled(
            dir.path(),
            control,
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")],
        )
        .unwrap();
        assert!(
            server.service().lease_deadline_ms() > 0,
            "an auto-renewal owner must arm a wall-clock self-fence deadline"
        );
    }

    #[test]
    fn mark_serving_does_not_extend_local_deadline_but_renew_does() {
        let control: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            ServerMetadataStore::direct(HoltMetadataStore::open_memory().unwrap()),
            MemoryObjectStore::new(),
        );
        service
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        service.set_clock_override_ms(1_000);
        let owner = ServerShardOwner::acquire(
            control,
            ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(Some(
                ServerShardOwnerRenewalOptions {
                    interval: Duration::from_millis(10),
                    run_immediately: false,
                    lease_ttl: Duration::from_millis(100),
                },
            )),
            &service,
        )
        .unwrap();
        assert_eq!(service.lease_deadline_ms(), 1_100);

        service.set_clock_override_ms(1_050);
        owner.mark_serving(&service).unwrap();
        assert_eq!(
            service.lease_deadline_ms(),
            1_100,
            "mark_serving is not a keepalive and must not extend the local fence"
        );

        service.set_clock_override_ms(1_075);
        owner.renew(&service).unwrap();
        assert_eq!(
            service.lease_deadline_ms(),
            1_175,
            "only a successful control keepalive may extend the deadline"
        );
        service.set_clock_override_ms(1_175);
        assert!(matches!(
            service.verify_owner_lease(),
            Err(MetadError::LeaseExpired {
                now_ms: 1_175,
                deadline_ms: 1_175,
            })
        ));
        owner.release().unwrap();
    }

    #[test]
    fn owner_without_renewal_has_no_lease_deadline() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = open_memory_controlled(
            dir.path(),
            control,
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
        )
        .unwrap();
        assert_eq!(
            server.service().lease_deadline_ms(),
            0,
            "manual/test owners keep the time fence off and rely on the epoch fence"
        );
    }

    #[test]
    fn dropping_server_releases_shard_owner_lease() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = nokv_control::ShardId::new("mount-1:/");
        {
            let _server = open_memory_controlled(
                dir.path(),
                control.clone(),
                vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
            )
            .unwrap();
            let record = control.get_shard(&shard).unwrap();
            assert_eq!(record.state, nokv_control::ShardState::Serving);
            assert!(record.owner.is_some());
        }
        // Graceful drop relinquishes the lease so a standby need not wait the TTL.
        let record = control.get_shard(&shard).unwrap();
        assert!(
            record.owner.is_none(),
            "dropping the server should release the shard owner lease"
        );
    }

    #[test]
    fn fresh_open_after_release_requires_explicit_failover() {
        let first_dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let shard = ShardId::new("mount-1:/");
        let first = open_memory_controlled(
            first_dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
        )
        .unwrap();
        drop(first);

        let second_dir = tempdir().unwrap();
        let result = open_memory_controlled(
            second_dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-b").with_renewal(None)],
        );
        assert!(matches!(
            result,
            Err(ServerError::Control(
                ControlError::FreshAcquireRequiresFailover { epoch: 2, .. }
            ))
        ));
        let record = control.get_shard(&shard).unwrap();
        assert_eq!(record.epoch, 2);
        assert_eq!(record.owner, None, "rejected fresh lease must be released");
        assert_eq!(record.state, ShardState::Unassigned);
    }

    #[test]
    fn never_served_fresh_shard_can_retry_after_checkpoint_publication_failure() {
        let first_dir = tempdir().unwrap();
        let control = Arc::new(FailingCheckpointPublishControl::new());
        let shard = ShardId::new("mount-1:/");
        control.fail_next_marks(1);

        let first = open_memory_controlled(
            first_dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
        );
        assert!(matches!(
            first,
            Err(ServerError::Control(ControlError::Backend(message)))
                if message.contains("injected checkpoint publish failure")
        ));
        let abandoned = control.get_shard(&shard).unwrap();
        assert_eq!(abandoned.epoch, 1);
        assert_eq!(abandoned.owner, None);
        assert!(!abandoned.ever_served);
        assert!(abandoned.checkpoint.is_none());
        assert!(abandoned.log.is_none());
        assert_eq!(abandoned.durable_lsn, 0);

        let second_dir = tempdir().unwrap();
        let second = open_memory_controlled(
            second_dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-b").with_renewal(None)],
        )
        .unwrap();
        let resurrected = control.get_shard(&shard).unwrap();
        assert_eq!(resurrected.epoch, 2);
        assert_eq!(resurrected.state, ShardState::Serving);
        assert!(resurrected.ever_served);
        assert!(resurrected.checkpoint.is_some());
        drop(second);
    }

    #[test]
    fn stale_owner_renew_observes_new_epoch_and_fences_commits() {
        let first_dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let first = open_memory_controlled(
            first_dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")],
        )
        .unwrap();

        let successor = control
            .acquire_after_failure(ShardId::new("mount-1:/"), NodeId::new("node-b"), 1)
            .unwrap();

        let err = first.renew_shard_owner_lease().unwrap_err();
        assert!(matches!(
            err,
            ServerError::Control(ControlError::NotOwner { .. })
        ));
        assert_eq!(first.service().required_owner_epoch(), 2);
        assert!(matches!(
            first
                .service()
                .create_dir_path("/stale-owner", 0o755, 1000, 1000),
            Err(MetadError::StaleOwnerEpoch {
                owner_epoch: 1,
                required_epoch: 2
            })
        ));
        control.release(&successor).unwrap();
    }

    #[test]
    fn shard_owner_auto_renewal_reports_success() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = open_memory_controlled(
            dir.path(),
            control,
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                .with_renewal(Some(fast_renewal_options(true)))],
        )
        .unwrap();

        wait_until(|| {
            server
                .shard_owner_renewal_state()
                .map(|state| state.iterations > 0)
                .unwrap_or(false)
        });

        let state = server.shard_owner_renewal_state().unwrap();
        assert_eq!(state.last_error, None);
        assert!(server
            .stats_json()
            .contains("\"renewal\":{\"enabled\":true,\"iterations\":"));
    }

    #[test]
    fn shard_owner_auto_renewal_detects_failover_and_fences_commits() {
        let first_dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let first = open_memory_controlled(
            first_dir.path(),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                .with_renewal(Some(fast_renewal_options(false)))],
        )
        .unwrap();

        let successor = control
            .acquire_after_failure(ShardId::new("mount-1:/"), NodeId::new("node-b"), 1)
            .unwrap();

        wait_until(|| {
            first
                .shard_owner_renewal_state()
                .and_then(|state| state.last_error)
                .is_some()
        });

        assert_eq!(first.service().required_owner_epoch(), 2);
        assert!(matches!(
            first
                .service()
                .create_dir_path("/stale-auto-renew-owner", 0o755, 1000, 1000),
            Err(MetadError::StaleOwnerEpoch {
                owner_epoch: 1,
                required_epoch: 2
            })
        ));
        control.release(&successor).unwrap();
    }
}
