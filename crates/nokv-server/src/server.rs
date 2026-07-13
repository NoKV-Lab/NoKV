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
    MetadataBackupOptions, MetadataBackupOutcome, MetadataBackupWorker, MetadataCheckpointStore,
    MetadataLogSegmentPointer, MetadataLogSyncConfig, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
    METADATA_LOG_ZERO_DIGEST,
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
    metadata_backup: Option<MetadataBackupWorker>,
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

impl Drop for Server {
    fn drop(&mut self) {
        // Stop renewing BEFORE releasing so a renewal worker cannot re-assert
        // ownership after the release, then relinquish each lease so a standby
        // can take over immediately instead of waiting out the full lease TTL.
        for slot in self.shards.values_mut() {
            slot.renewal.take();
            if let Some(owner) = slot.owner.as_ref() {
                let _ = owner.release();
            }
        }
    }
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
    let Some(prefix) = options.metadata_checkpoint_archive_prefix.clone() else {
        return Err(ServerError::Metadata(MetadError::InvalidPath(
            "metadata checkpoint archive is not configured \
             (pass --metadata-checkpoint-archive-prefix)"
                .to_owned(),
        )));
    };
    let objects = options.object.open()?;
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
                let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
                let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
                let metadata_backup = metadata_archive.as_ref().map(|archive| {
                    let mut backup = MetadataBackupOptions::new(archive.clone());
                    backup.run_immediately = false;
                    MetadataBackupWorker::spawn(Arc::clone(&service), backup)
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
    /// write lands on this very shard). Best-effort: a reconcile failure is logged
    /// and does not block serving (the CLI `reconcile-grafts` can retry).
    pub(crate) fn reconcile_local_grafts(&self) {
        let Some(control) = &self.control else {
            return;
        };
        let records = match control.list_shards() {
            Ok(records) => records,
            Err(err) => {
                eprintln!("nokv-server: graft reconcile skipped (list_shards failed): {err}");
                return;
            }
        };
        for record in records {
            if record.shard_index == DEFAULT_SHARD_INDEX {
                continue;
            }
            let Some(subtree_root_raw) = record.subtree_root_inode else {
                continue;
            };
            let Ok(child_inode) = InodeId::new(subtree_root_raw) else {
                continue;
            };
            // Which shard owns the PARENT prefix? If it is not one this server
            // hosts, skip — that parent shard reconciles its own grafts.
            let (parent_prefix, basename) = split_graft_prefix(&record.prefix);
            let parent_index = self.shard_map.route(self.mount, &parent_prefix);
            let Some(parent_slot) = self
                .shards
                .values()
                .find(|slot| slot.shard_index == parent_index)
            else {
                continue;
            };
            let Ok(name) = DentryName::new(basename.into_bytes()) else {
                continue;
            };
            // Resolve the parent inode on the parent shard's own namespace. The
            // root-level case (parent == "/") is the global root; a nested parent
            // is resolved via the parent shard's local path lookup.
            let parent_inode = if parent_prefix == "/" {
                InodeId::root()
            } else {
                match parent_slot.service().lookup_path(&parent_prefix) {
                    Ok(Some(entry)) => entry.attr.inode,
                    _ => continue,
                }
            };
            match parent_slot.service().create_graft(
                parent_inode,
                name,
                child_inode,
                DEFAULT_ROOT_MODE,
                0,
                0,
            ) {
                Ok(_) => {
                    eprintln!(
                        "nokv-server: reconciled missing graft for prefix {} -> inode {}",
                        record.prefix, subtree_root_raw
                    );
                }
                // Already present (the common case): nothing to heal.
                Err(MetadError::Metadata(err)) if is_predicate_failed(&err) => {}
                Err(err) => {
                    eprintln!(
                        "nokv-server: graft reconcile for prefix {} failed: {err:?}",
                        record.prefix
                    );
                }
            }
        }
    }

    pub fn serve(self, listener: TcpListener) -> Result<(), ServerError> {
        // Heal any graft whose parent we own before accepting traffic.
        self.reconcile_local_grafts();
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

    fn publish_slot_checkpoint_ref(
        slot: &ShardSlot,
        checkpoint: CheckpointRef,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        let durable_lsn = checkpoint.lsn;
        slot.owner
            .as_ref()
            .map(|owner| {
                owner.mark_serving_with_recovery_refs(
                    slot.service(),
                    Some(checkpoint),
                    None,
                    durable_lsn,
                )
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

    pub fn publish_shard_owner_checkpoint_ref(
        &self,
        checkpoint: CheckpointRef,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        Self::publish_slot_checkpoint_ref(self.default_slot(), checkpoint)
    }

    fn publish_checkpoint_for_backup(
        slot: &ShardSlot,
        outcome: &MetadataBackupOutcome,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        if slot.owner.is_none() {
            return Ok(None);
        }
        // Use the (lsn, digest) captured atomically with the image inside
        // backup_metadata, NOT a fresh snapshot read — a concurrent commit
        // between the export and a later read could otherwise stamp the
        // CheckpointRef with an LSN ahead of the image and drop that write on
        // restore.
        let state = Self::publish_slot_checkpoint_ref(
            slot,
            CheckpointRef {
                object_key: outcome.checkpoint_key.clone(),
                lsn: outcome.log_lsn,
                image_bytes: outcome.image_bytes,
                digest: hex_digest(&outcome.log_digest),
            },
        )?;
        // Segments fully covered by this checkpoint are redundant now; drop them
        // from the live chain so the chain and published LogRef stay bounded.
        slot.service()
            .prune_sync_metadata_log_segments(outcome.log_lsn);
        Ok(state)
    }

    /// Publish the latest sync-log ref of the shard that handled a committing
    /// request, identified by its index.
    pub(crate) fn publish_latest_metadata_log_ref_for_index(
        &self,
        shard_index: u16,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        let Some(slot) = self.slot_by_index(shard_index) else {
            return Ok(None);
        };
        Self::publish_slot_latest_log_ref(slot)
    }

    fn publish_slot_latest_log_ref(
        slot: &ShardSlot,
    ) -> Result<Option<ServerShardOwnerState>, ServerError> {
        let Some(snapshot) = slot.service().sync_metadata_log_snapshot() else {
            return Ok(None);
        };
        Self::publish_slot_log_ref(
            slot,
            LogRef {
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
            },
        )
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

    pub fn stats_json(&self) -> String {
        let slot = self.default_slot();
        let service = slot.service();
        let objects = service.object_stats();
        let metadata = service.metadata_store_stats();
        let metadata_service = service.metadata_service_stats();
        let object_gc = slot.object_gc.state();
        let history_gc = slot.history_gc.state();
        format!(
            "{{\"ready\":true,\"block_cache_enabled\":{},\"object_puts\":{},\"object_put_bytes\":{},\"object_gets\":{},\"object_get_bytes\":{},\"coalesced_gets\":{},\"coalesced_get_bytes\":{},\"cache_hits\":{},\"cache_hit_bytes\":{},\"prefetch_enqueued\":{},\"prefetch_dropped\":{},\"prefetch_completed\":{},\"prefetch_failed\":{},\"prefetch_object_gets\":{},\"prefetch_object_get_bytes\":{},\"prefetch_cache_hits\":{},\"prefetch_cache_hit_bytes\":{},\"read_plan_cache_hits\":{},\"read_plan_cache_misses\":{},\"object_writeback_enqueued\":{},\"object_writeback_inline\":{},\"object_writeback_completed\":{},\"object_writeback_failed\":{},\"object_writeback_staged_bytes\":{},\"object_writeback_uploaded_bytes\":{},\"object_writeback_queue_wait_ns\":{},\"object_writeback_queue_max_wait_ns\":{},\"object_writeback_upload_ns\":{},\"object_writeback_upload_max_ns\":{},\"object_writeback_collect_ns\":{},\"object_writeback_digest_ns\":{},\"object_writeback_store_put_ns\":{},\"object_writeback_cache_put_ns\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"metadata_store\":{},\"metadata_service\":{},\"shard_owner\":{},\"object_gc\":{},\"history_gc\":{},\"metadata_backup\":{}}}\n",
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
            object_gc_json(&object_gc),
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
            r#"{{"object_gc":{{"scanned":{},"blocked_by_snapshots":{},"blocked_by_read_leases":{},"attempted":{},"deleted":{},"missing":{},"records_removed":{},"snapshot_reap":{{"scanned":{},"expired_candidates":{},"reaped":{},"conflicted":{}}}}},"history_gc":{{"scanned":{},"removed":{},"retained_by_snapshots":{}}}}}
"#,
            object.scanned,
            object.blocked_by_snapshots,
            object.blocked_by_read_leases,
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
        let outcome = slot.service().backup_metadata(archive)?;
        let _checkpoint_state = Self::publish_checkpoint_for_backup(slot, &outcome)?;
        let key = format!("\"{}\"", escape_json_string(&outcome.checkpoint_key));
        Ok(format!(
            r#"{{"checkpoint_key":{key},"image_bytes":{},"commit_version":{},"pruned":{}}}
"#,
            outcome.image_bytes, outcome.commit_version, outcome.pruned,
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

/// Build one [`ShardSlot`] from its control-plane owner options: open the shard's
/// own Holt engine, derive its index/prefix from the (registered) control record,
/// acquire the owner lease, restore on failover, enable the shared log, mark
/// serving, and spawn the per-shard background workers.
fn open_shard_slot(
    options: &ServerOptions,
    objects: &ConfiguredObjectStore,
    store: Arc<dyn ControlStore>,
    shard_owner: ServerShardOwnerOptions,
) -> Result<(ShardId, ShardSlot), ServerError> {
    let shard_id = shard_owner.shard_id.clone();
    let sanitized = sanitize_shard_id(shard_id.as_str());

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

    let is_failover = matches!(
        shard_owner.acquisition,
        ServerShardAcquisition::Failover { .. }
    );
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
    let shared_log_options = shard_owner.shared_log.clone();
    // Each shard's checkpoint archive is isolated under its own prefix.
    let metadata_archive = options
        .metadata_checkpoint_archive_prefix
        .as_ref()
        .map(|prefix| {
            MetadataArchiveConfig::new(
                shard_archive_prefix(prefix, &sanitized),
                DEFAULT_ARCHIVE_KEEP_LAST,
            )
        });

    let owner = ServerShardOwner::acquire(store, shard_owner, service.as_ref())?;

    let restored_from_control = if is_failover {
        match metadata_archive.as_ref() {
            Some(archive) => restore_shard_owner_recovery_refs(service.as_ref(), &owner, archive)?,
            None => {
                let state = owner.state()?;
                if state.checkpoint.is_some() {
                    return Err(ServerError::Metadata(MetadError::InvalidPath(
                        "metadata checkpoint archive is required for shard failover restore"
                            .to_owned(),
                    )));
                }
                false
            }
        }
    } else {
        false
    };
    if !restored_from_control {
        service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
    }

    if let Some(shared_log) = shared_log_options {
        let state = owner.state()?;
        let last_digest = control_recovery_digest(&state)?;
        let inherited_segments = inherited_log_segments(&state)?;
        // Isolate each shard's shared-log archive under its own prefix.
        service.enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                shard_archive_prefix(&shared_log.archive_prefix, &sanitized),
                state.shard_id.as_str(),
                state.epoch,
                state.durable_lsn,
                last_digest,
            )
            .with_segments(inherited_segments),
        )?;
    }
    owner.mark_serving(service.as_ref())?;

    let renewal = renewal_options.map(|renewal| {
        ServerShardOwnerRenewalWorker::spawn(Arc::clone(&service), owner.clone(), renewal)
    });
    let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
    let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
    let metadata_backup = metadata_archive.as_ref().map(|archive| {
        let mut backup = MetadataBackupOptions::new(archive.clone());
        // Back up on the interval, not on every boot (avoids startup stalls).
        backup.run_immediately = false;
        MetadataBackupWorker::spawn(Arc::clone(&service), backup)
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
    fn spawn(
        service: Arc<NoKvFs<ServerMetadataStore, ConfiguredObjectStore>>,
        owner: ServerShardOwner,
        options: ServerShardOwnerRenewalOptions,
    ) -> Self {
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

fn run_shard_owner_renewal_once(
    service: &Arc<NoKvFs<ServerMetadataStore, ConfiguredObjectStore>>,
    owner: &ServerShardOwner,
    state: &Arc<Mutex<ServerShardOwnerRenewalWorkerState>>,
) {
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
) -> Result<bool, ServerError> {
    let state = owner.state()?;
    let Some(checkpoint) = state.checkpoint.clone() else {
        return Ok(false);
    };
    let checkpoint_digest = parse_hex_digest(&checkpoint.digest)?;
    // Replay every archived segment whose tail is above the checkpoint LSN, in
    // order. A single-pointer LogRef would drop all but the newest segment and
    // silently lose acknowledged metadata on any multi-segment failover.
    let segment_keys: Vec<String> = state
        .log
        .as_ref()
        .map(|log| {
            log.segments
                .iter()
                .filter(|segment| segment.last_lsn > checkpoint.lsn)
                .map(|segment| segment.segment_key.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if state.durable_lsn > checkpoint.lsn && segment_keys.is_empty() {
        return Err(ServerError::Metadata(MetadError::Codec(
            "control record is missing log segments for checkpoint replay".to_owned(),
        )));
    }
    let Some(outcome) = service.restore_metadata_with_archived_log_segments(
        archive,
        &segment_keys,
        checkpoint.lsn,
        checkpoint_digest,
    )?
    else {
        return Err(ServerError::Metadata(MetadError::NotFound));
    };
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
    Ok(true)
}

fn object_gc_json(state: &ObjectGcWorkerState) -> String {
    let reap = state.snapshot_reap;
    format!(
        "{{\"iterations\":{},\"last_error\":{},\"snapshot_reap\":{{\"scanned\":{},\"expired_candidates\":{},\"reaped\":{},\"conflicted\":{}}}}}",
        state.iterations,
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
    use std::time::{Duration, Instant};
    #[cfg(feature = "etcd")]
    use std::time::{SystemTime, UNIX_EPOCH};
    #[cfg(feature = "etcd")]
    use std::{env, process};

    use nokv_control::InMemoryControlStore;
    use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokv_object::{ObjectStoreConfig, S3ObjectStoreOptions};
    use nokv_types::MountId;
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
    fn object_gc_health_keeps_cumulative_reaper_conflicts() {
        let state = ObjectGcWorkerState {
            iterations: 9,
            last_outcome: Some(nokv_meta::PendingObjectCleanupOutcome::default()),
            snapshot_reap: nokv_meta::SnapshotReapOutcome {
                scanned: 12,
                expired_candidates: 3,
                reaped: 2,
                conflicted: 1,
            },
            last_error: None,
        };

        let json = object_gc_json(&state);
        assert!(json.contains("\"iterations\":9"));
        assert!(json.contains("\"conflicted\":1"));
    }

    #[test]
    fn controlled_open_acquires_shard_and_installs_owner_epoch() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = Server::open_with_control(
            test_options(dir.path()),
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
        let server = Server::open_with_control(
            test_options(dir.path()),
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
    fn controlled_failover_installs_bumped_epoch() {
        let first_dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let first = Server::open_with_control(
            test_options(first_dir.path()),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")],
        )
        .unwrap();
        assert_eq!(first.shard_owner_state().unwrap().unwrap().epoch, 1);

        let second_dir = tempdir().unwrap();
        let second = Server::open_with_control(
            test_options(second_dir.path()),
            control,
            vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)],
        )
        .unwrap();

        let state = second.shard_owner_state().unwrap().unwrap();
        assert_eq!(state.node_id.as_str(), "node-b");
        assert_eq!(state.epoch, 2);
        assert_eq!(state.lease_id, 2);
        assert_eq!(state.state, nokv_control::ShardState::Serving);
        assert_eq!(second.service().allocator_epoch(), 2);
        assert_eq!(second.service().required_owner_epoch(), 2);
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

        let first_dir = tempdir().unwrap();
        let mut first_options = test_options(first_dir.path());
        first_options.control = Some(ServerControlOptions {
            store: ServerControlStoreOptions::Etcd(etcd_options()),
            shard_owner: ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None),
        });
        let first = Server::open(first_options).unwrap();
        let first_state = first.shard_owner_state().unwrap().unwrap();
        assert_eq!(first_state.node_id.as_str(), "node-a");
        assert_eq!(first_state.epoch, 1);

        let second_dir = tempdir().unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        let second = loop {
            let mut second_options = test_options(second_dir.path());
            second_options.control = Some(ServerControlOptions {
                store: ServerControlStoreOptions::Etcd(etcd_options()),
                shard_owner: ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                    .with_renewal(None),
            });
            match Server::open(second_options) {
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
        let server = Server::open_with_control(
            test_options(dir.path()),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None)],
        )
        .unwrap();
        let log = LogRef {
            segments: vec![LogSegmentRef {
                segment_key: "meta/shared-log/log/segment-1".to_owned(),
                first_lsn: 40,
                last_lsn: 42,
                digest: "abc123".to_owned(),
            }],
            durable_lsn: 42,
            digest: "abc123".to_owned(),
        };

        let state = server
            .publish_shard_owner_log_ref(log.clone())
            .unwrap()
            .unwrap();

        assert_eq!(state.log, Some(log.clone()));
        assert_eq!(state.durable_lsn, 42);
        let record = control
            .get_shard(&nokv_control::ShardId::new("mount-1:/"))
            .unwrap();
        assert_eq!(record.log, Some(log));
        assert_eq!(record.durable_lsn, 42);
        assert!(server.stats_json().contains(
            "\"log\":{\"segments\":[{\"segment_key\":\"meta/shared-log/log/segment-1\",\"first_lsn\":40,\"last_lsn\":42,\"digest\":\"abc123\"}],\"durable_lsn\":42"
        ));
    }

    #[test]
    fn owner_arms_lease_deadline_when_renewal_enabled() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = Server::open_with_control(
            test_options(dir.path()),
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
    fn owner_without_renewal_has_no_lease_deadline() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = Server::open_with_control(
            test_options(dir.path()),
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
            let _server = Server::open_with_control(
                test_options(dir.path()),
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
    fn stale_owner_renew_observes_new_epoch_and_fences_commits() {
        let first_dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let first = Server::open_with_control(
            test_options(first_dir.path()),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")],
        )
        .unwrap();

        let second_dir = tempdir().unwrap();
        let _second = Server::open_with_control(
            test_options(second_dir.path()),
            control,
            vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)],
        )
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
    }

    #[test]
    fn shard_owner_auto_renewal_reports_success() {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        let server = Server::open_with_control(
            test_options(dir.path()),
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
        let first = Server::open_with_control(
            test_options(first_dir.path()),
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                .with_renewal(Some(fast_renewal_options(false)))],
        )
        .unwrap();

        let second_dir = tempdir().unwrap();
        let _second = Server::open_with_control(
            test_options(second_dir.path()),
            control,
            vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1).with_renewal(None)],
        )
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
    }
}
