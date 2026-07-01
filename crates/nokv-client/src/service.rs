use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use nokv_control::{ControlStore, ShardId, ShardRecord};
use nokv_meta::{
    DentryWithAttr, PublishArtifactStagedSession, RenameReplaceResult, SubtreeDelta, UpdateAttr,
    XattrSetMode,
};
use nokv_object::ObjectReadPlan;
use nokv_protocol::{
    decode_envelope, decode_name_cursor, decode_xattr_name, encode_advisory_lock_kind,
    encode_file_type, encode_name_cursor, encode_request, encode_xattr_name, request_routing_key,
    MetadataRpcRequest, MetadataRpcResult, RoutingKey, WireOpenPathReadPlanRequest,
};
use nokv_types::{
    AdvisoryLock, AdvisoryLockRequest, BodyDescriptor, ChunkManifest, DentryName, FileType,
    InodeAttr, InodeId, MountId, PathMetadata, ReadLease, ShardMap, ShardPrefix, ShardRoute,
    SnapshotPin, SpecialNodeSpec, DEFAULT_SHARD_INDEX,
};

use crate::ClientError;

use crate::framed::PipelinedConnection;
#[cfg(test)]
use crate::framed::{read_frame, write_frame, FRAMED_RPC_MAGIC};
use crate::wire::*;

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BATCH_RPC_REQUESTS: usize = 128;

/// Bound for re-resolve+retry in fleet mode: enough attempts to ride out a
/// single owner handoff (old owner -> control update -> new owner) without
/// retrying forever against a permanently-missing shard.
const FLEET_MAX_ATTEMPTS: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataClientOptions {
    pub address: SocketAddr,
    pub timeout: Duration,
}

/// How a [`MetadataClient`] picks the target endpoint for each RPC.
///
/// `SingleShard` pins every request to one address (the legacy behavior, used by
/// `connect`/`single_shard`). `Fleet` resolves the owning shard per-request from
/// a control-plane-derived routing map and endpoint table, refreshing both on a
/// `NotOwner`/stale-owner handoff. All routing lives here so the ~60 typed RPC
/// methods stay routing-agnostic — they just call [`MetadataClient::call`].
enum RoutingMode {
    SingleShard { address: SocketAddr },
    Fleet(FleetRouter),
}

/// Fleet-mode routing state: the control store (source of truth), the mount this
/// client serves, and the locally-cached `(shard_map, endpoints)` rebuilt from
/// `control.list_shards()`. The two caches are refreshed together so a request
/// never resolves a shard index the endpoint table can't map.
struct FleetRouter {
    control: Arc<dyn ControlStore>,
    mount: MountId,
    shard_map: RwLock<ShardMap>,
    endpoints: RwLock<HashMap<u16, SocketAddr>>,
}

pub struct MetadataClient {
    mode: RoutingMode,
    timeout: Duration,
    next_request_id: AtomicU64,
    connections: Mutex<HashMap<SocketAddr, Arc<PipelinedConnection>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientPreparedArtifact {
    pub mount: u64,
    pub parent: InodeId,
    pub name: DentryName,
    pub path: Option<String>,
    pub inode: InodeId,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientCreatedPreparedArtifact {
    pub entry: DentryWithAttr,
    pub prepared: ClientPreparedArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientReadDirPlusPage {
    pub entries: Vec<DentryWithAttr>,
    pub next_cursor: Option<DentryName>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathLayoutOpen {
    pub metadata: PathMetadata,
    pub lease: ReadLease,
    pub plan: ObjectReadPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathLayoutOpenRequest {
    pub path: String,
    pub offset: u64,
    pub len: usize,
    pub expected_generation: Option<u64>,
}

/// Result of a copy-on-write subtree clone: the fork's namespace root inode and the
/// retained snapshot pin that protects the shared base blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneOutcome {
    pub root: InodeId,
    pub snapshot_id: u64,
}

/// Result of pinning a subtree snapshot: the durable `snapshot_id` to pass to a
/// later rollback and the read version the snapshot captured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotOutcome {
    pub snapshot_id: u64,
    pub read_version: u64,
}

const DEFAULT_LIST_PAGE_SIZE: usize = 1024;

impl MetadataClientOptions {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }
}

impl PathLayoutOpenRequest {
    pub fn new(path: impl Into<String>, offset: u64, len: usize) -> Self {
        Self {
            path: path.into(),
            offset,
            len,
            expected_generation: None,
        }
    }

    pub fn with_expected_generation(mut self, generation: u64) -> Self {
        self.expected_generation = Some(generation);
        self
    }
}

impl MetadataClient {
    /// Build a single-shard client from explicit options. Every request targets
    /// `options.address`.
    pub fn new(options: MetadataClientOptions) -> Self {
        Self {
            mode: RoutingMode::SingleShard {
                address: options.address,
            },
            timeout: options.timeout,
            next_request_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Single-shard client: every request goes to `address`. Equivalent to
    /// [`MetadataClient::connect`]; named to contrast with [`fleet`].
    ///
    /// [`fleet`]: MetadataClient::fleet
    pub fn single_shard(address: SocketAddr) -> Self {
        Self::new(MetadataClientOptions::new(address))
    }

    pub fn connect(address: SocketAddr) -> Self {
        Self::single_shard(address)
    }

    /// Build a fleet client that routes each request to the owning shard's
    /// endpoint, resolved from `control`. Performs the initial `list_shards`
    /// build of the routing map + endpoint table; later handoffs are picked up by
    /// the re-resolve+retry path in [`call`].
    ///
    /// [`call`]: MetadataClient::call
    pub fn fleet(control: Arc<dyn ControlStore>, mount: MountId) -> Result<Self, ClientError> {
        let (shard_map, endpoints) = resolve_fleet_routes(control.as_ref(), mount)?;
        Ok(Self {
            mode: RoutingMode::Fleet(FleetRouter {
                control,
                mount,
                shard_map: RwLock::new(shard_map),
                endpoints: RwLock::new(endpoints),
            }),
            timeout: DEFAULT_RPC_TIMEOUT,
            next_request_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
        })
    }

    /// Resolve, without sending, the endpoint a request would target in fleet
    /// mode against the currently cached routing map (no control refresh). This
    /// is the same `route` + `endpoint` resolution `call` performs, exposed for
    /// deterministic unit tests of the address router.
    #[cfg(test)]
    fn resolve_target_for_test(
        &self,
        request: &MetadataRpcRequest,
    ) -> Result<Option<SocketAddr>, ClientError> {
        match &self.mode {
            RoutingMode::SingleShard { address } => Ok(Some(*address)),
            RoutingMode::Fleet(router) => Ok(router.endpoint(router.route(request)?)),
        }
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::BootstrapRoot { mode, uid, gid })? {
            MetadataRpcResult::InodeAttr { .. } => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, ClientError> {
        match self.call(MetadataRpcRequest::GetAttr { inode: inode.get() })? {
            MetadataRpcResult::InodeAttr { attr } => attr
                .map(|attr| attr.into_inode_attr().map_err(protocol_error))
                .transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, ClientError> {
        match self.call(MetadataRpcRequest::GetAttrAtSnapshot {
            snapshot_id,
            inode: inode.get(),
        })? {
            MetadataRpcResult::InodeAttr { attr } => attr
                .map(|attr| attr.into_inode_attr().map_err(protocol_error))
                .transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup_plus(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPlus {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn current_dentry_version(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<u64>, ClientError> {
        match self.call(MetadataRpcRequest::CurrentDentryVersion {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::DentryVersion { version } => Ok(version),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPlusAtSnapshot {
            snapshot_id,
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_dir_plus_page(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPage {
            parent: parent.get(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
    ) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusAtSnapshot {
            snapshot_id,
            parent: parent.get(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    /// Write the parent-shard half of a cross-shard graft: a dentry `name` under
    /// `parent` pointing at `target_inode` (owned by another shard). Routes by
    /// `parent` to the parent shard. See [`Self::register_graft`] for the
    /// end-to-end orchestration that also creates the subtree dir.
    pub fn create_graft(
        &self,
        parent: InodeId,
        name: DentryName,
        target_inode: InodeId,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateGraft {
            parent: parent.get(),
            name: rpc_name(&name)?,
            target_inode: target_inode.get(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    /// Remove the parent-shard half of a cross-shard graft: the dentry `name`
    /// under `parent`. Routes by `parent` to the parent shard. Idempotent — a
    /// `None` result means the dentry was already gone. See
    /// [`Self::unregister_graft`] for the end-to-end teardown that also reaps the
    /// child subtree and clears the control-plane record.
    pub fn remove_graft(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::RemoveGraft {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_file_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_file_prepared_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<ClientCreatedPreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::CreateFilePrepared {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::CreatedPreparedArtifact { entry, prepared } => {
                Ok(ClientCreatedPreparedArtifact {
                    entry: wire_dentry(*entry)?,
                    prepared: wire_prepared_artifact(prepared)?,
                })
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateSymlink {
            parent: parent.get(),
            name: rpc_name(&name)?,
            target,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateSpecialNode {
            parent: parent.get(),
            name: rpc_name(&name)?,
            file_type: encode_file_type(spec.file_type).to_owned(),
            mode: spec.mode,
            rdev: spec.rdev,
            uid: spec.uid,
            gid: spec.gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn update_attrs(
        &self,
        parent: InodeId,
        name: DentryName,
        changes: UpdateAttr,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::UpdateAttrs {
            parent: parent.get(),
            name: rpc_name(&name)?,
            changes: update_attr_to_wire(changes),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn update_root_attrs(&self, changes: UpdateAttr) -> Result<InodeAttr, ClientError> {
        match self.call(MetadataRpcRequest::UpdateRootAttrs {
            changes: update_attr_to_wire(changes),
        })? {
            MetadataRpcResult::InodeAttr { attr: Some(attr) } => {
                attr.into_inode_attr().map_err(protocol_error)
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::SetXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
            value,
            mode: xattr_set_mode_to_wire(mode),
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_xattr(&self, inode: InodeId, name: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.call(MetadataRpcRequest::GetXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
        })? {
            MetadataRpcResult::XattrValue { value } => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_xattr(&self, inode: InodeId) -> Result<Vec<Vec<u8>>, ClientError> {
        match self.call(MetadataRpcRequest::ListXattr { inode: inode.get() })? {
            MetadataRpcResult::XattrNames { names_hex } => names_hex
                .iter()
                .map(|name| decode_xattr_name(name).map_err(protocol_error))
                .collect(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::RemoveXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_advisory_lock(
        &self,
        request: AdvisoryLockRequest,
    ) -> Result<Option<AdvisoryLock>, ClientError> {
        match self.call(MetadataRpcRequest::GetAdvisoryLock {
            inode: request.inode.get(),
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: encode_advisory_lock_kind(request.kind).to_owned(),
            pid: request.pid,
        })? {
            MetadataRpcResult::AdvisoryLock { lock } => lock.map(wire_advisory_lock).transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn set_advisory_lock(&self, request: AdvisoryLockRequest) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::SetAdvisoryLock {
            inode: request.inode.get(),
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: encode_advisory_lock_kind(request.kind).to_owned(),
            pid: request.pid,
            wait: request.wait,
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateDirPath {
            path: path.to_owned(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    /// The fleet control store, or a clear error in single-shard mode. Graft
    /// lifecycle (register/reconcile/unregister) is a fleet-only concern: in
    /// single-shard mode every inode is local and there are no cross-shard grafts.
    fn fleet_control(&self) -> Result<&Arc<dyn ControlStore>, ClientError> {
        match &self.mode {
            RoutingMode::Fleet(router) => Ok(&router.control),
            RoutingMode::SingleShard { .. } => Err(ClientError::Protocol(
                "graft lifecycle requires fleet (multi-shard) mode".to_owned(),
            )),
        }
    }

    /// Resolve the `ShardId` of the shard whose stable index is `shard_index`,
    /// from the control plane's durable shard list.
    fn shard_id_for_index(&self, shard_index: u16) -> Result<ShardId, ClientError> {
        let control = self.fleet_control()?;
        let records = control
            .list_shards()
            .map_err(|err| ClientError::Protocol(format!("control list_shards failed: {err}")))?;
        records
            .into_iter()
            .find(|record| record.shard_index == shard_index)
            .map(|record| record.shard_id)
            .ok_or_else(|| {
                ClientError::Protocol(format!(
                    "no control-plane shard record for shard index {shard_index}"
                ))
            })
    }

    /// Resolve the parent inode for a graft `prefix_path` on the PARENT shard,
    /// plus the basename. Root-level prefixes (e.g. `/dataset`) have the global
    /// root inode as parent.
    fn graft_parent_inode_and_name(
        &self,
        prefix_path: &str,
    ) -> Result<(InodeId, DentryName), ClientError> {
        let (parent_path, basename) = rpc_parent_and_name(prefix_path)?;
        let name = DentryName::new(basename.into_bytes())
            .map_err(|err| ClientError::InvalidName(err.to_string()))?;
        let parent_inode = if parent_path == "/" {
            InodeId::root()
        } else {
            self.lookup(&parent_path)?
                .ok_or_else(|| {
                    ClientError::Protocol(format!(
                        "graft parent directory {parent_path} does not exist"
                    ))
                })?
                .attr
                .inode
        };
        Ok((parent_inode, name))
    }

    /// Register a cross-shard graft for `prefix_path` so multi-shard FUSE
    /// traversal can cross from the parent shard into the subtree shard.
    ///
    /// `prefix_path` (e.g. `/dataset`) routes by prefix to its OWNING (subtree)
    /// shard, where the directory is created with a real inode. The parent path
    /// routes to the PARENT shard, whose own root has no dentry for the subtree —
    /// so a FUSE `lookup(parent_ino, basename)` would ENOENT. This installs that
    /// missing dentry: a graft under the parent inode pointing at the subtree
    /// dir's (foreign) inode.
    ///
    /// Crash-safe registration: the child subtree dir is created, then its inode
    /// is recorded DURABLY in the control plane (`subtree_root_inode`) as the
    /// single atomic registration point, and only then is the (reconcilable)
    /// parent graft dentry written. If the parent-dentry write is lost after the
    /// control record lands, [`Self::reconcile_grafts`] re-creates it.
    ///
    /// Idempotent: re-running tolerates both the subtree dir and the graft dentry
    /// already existing, returning the existing graft dentry. Returns the graft
    /// dentry as seen from the parent shard (its `attr.inode` is the foreign
    /// subtree inode).
    pub fn register_graft(
        &self,
        prefix_path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        // Graft lifecycle is fleet-only; fail fast (and clearly) otherwise.
        let _ = self.fleet_control()?;

        // 1. Ensure the subtree directory exists on its owning shard and learn
        //    its inode. Tolerate a prior run having created it.
        let child_inode = match self.mkdir(prefix_path, mode, uid, gid) {
            Ok(entry) => entry.attr.inode,
            Err(err) if crate::is_metadata_predicate_failed(&err) => {
                self.lookup(prefix_path)?
                    .ok_or_else(|| {
                        ClientError::Protocol(format!(
                        "graft subtree {prefix_path} reported existing but lookup found nothing"
                    ))
                    })?
                    .attr
                    .inode
            }
            Err(err) => return Err(err),
        };

        // 2. ATOMIC REGISTRATION POINT: record the subtree-root inode durably in
        //    the control plane, on the owning (subtree) shard's record. After
        //    this single write the graft is durably "registered"; the parent
        //    graft dentry below is reconcilable from it. The subtree shard is the
        //    one whose stable index is encoded in the child inode's high bits.
        let subtree_shard_id = self.shard_id_for_index(child_inode.shard_index())?;
        self.fleet_control()?
            .set_subtree_root_inode(&subtree_shard_id, Some(child_inode.get()))
            .map_err(|err| {
                ClientError::Protocol(format!("control set_subtree_root_inode failed: {err}"))
            })?;

        // 3. Resolve the parent inode + basename on the PARENT shard.
        let (parent_inode, name) = self.graft_parent_inode_and_name(prefix_path)?;

        // 4. Write the graft dentry under the parent inode (routes to the parent
        //    shard). Tolerate a prior run having installed it.
        match self.create_graft(parent_inode, name.clone(), child_inode, mode, uid, gid) {
            Ok(entry) => Ok(entry),
            Err(err) if crate::is_metadata_predicate_failed(&err) => {
                self.lookup_plus(parent_inode, name)?.ok_or_else(|| {
                    ClientError::Protocol(format!(
                        "graft {prefix_path} reported existing but lookup found nothing"
                    ))
                })
            }
            Err(err) => Err(err),
        }
    }

    /// Reconcile every cross-shard graft dentry against the control plane, which
    /// is the source of truth. Reads `list_shards` and, for each non-default
    /// subtree shard, drives the parent graft dentry to match the shard's durable
    /// `subtree_root_inode`:
    ///
    ///   * `Some(inode)` (registered): idempotently ensure the parent graft
    ///     dentry is PRESENT — heals a `register_graft` whose parent-dentry write
    ///     was lost after the control record landed.
    ///   * `None` (de-registered): idempotently ensure any stale parent graft
    ///     dentry into this shard is REMOVED — heals an `unregister_graft` that
    ///     crashed after the child `rmdir` but before `remove_graft`, which would
    ///     otherwise leave an orphan parent dentry pointing at a reaped subtree.
    ///
    /// Both directions are idempotent, so this is safe to run on a parent shard's
    /// startup. It closes the documented crash gap; it is not a cross-shard 2PC.
    ///
    /// Returns the prefixes whose graft dentry was changed (created or removed).
    pub fn reconcile_grafts(&self) -> Result<Vec<String>, ClientError> {
        let control = self.fleet_control()?;
        let records = control
            .list_shards()
            .map_err(|err| ClientError::Protocol(format!("control list_shards failed: {err}")))?;
        let mut reconciled = Vec::new();
        for record in records {
            // The default/root shard is never a graft child.
            if record.shard_index == DEFAULT_SHARD_INDEX {
                continue;
            }
            let prefix_path = record.prefix.clone();
            match record.subtree_root_inode {
                Some(subtree_root_raw) => {
                    let child_inode = InodeId::new(subtree_root_raw)
                        .map_err(|err| ClientError::Protocol(err.to_string()))?;
                    let (parent_inode, name) = self.graft_parent_inode_and_name(&prefix_path)?;
                    // Idempotent: present already -> nothing to do; absent ->
                    // (re)create. `create_graft` surfaces an existing dentry as
                    // PredicateFailed. The graft dentry is a stub projection; on
                    // traversal the real subtree-dir attrs are served from the
                    // owning shard, so these stub mode/owner bits are cosmetic.
                    match self.create_graft(parent_inode, name, child_inode, 0o755, 0, 0) {
                        Ok(_) => reconciled.push(prefix_path),
                        Err(err) if crate::is_metadata_predicate_failed(&err) => {}
                        Err(err) => return Err(err),
                    }
                }
                None => {
                    if self.reconcile_remove_stale_graft(&record)? {
                        reconciled.push(prefix_path);
                    }
                }
            }
        }
        Ok(reconciled)
    }

    /// De-registration side of [`Self::reconcile_grafts`]: remove a parent graft
    /// dentry left behind by an `unregister_graft` that crashed after the child
    /// `rmdir` but before `remove_graft`. Conservative — it only removes a dentry
    /// that still resolves AND is a graft into THIS subtree shard, so it never
    /// touches a live local entry or a graft owned by another shard. Idempotent:
    /// returns `false` (nothing changed) when there is no stale dentry to heal.
    fn reconcile_remove_stale_graft(&self, record: &ShardRecord) -> Result<bool, ClientError> {
        let prefix_path = record.prefix.clone();
        // If the parent directory itself is gone there is nothing to heal.
        let (parent_inode, name) = match self.graft_parent_inode_and_name(&prefix_path) {
            Ok(resolved) => resolved,
            Err(_) => return Ok(false),
        };
        let Some(entry) = self.lookup_plus(parent_inode, name.clone())? else {
            return Ok(false);
        };
        // Only a directory dentry pointing INTO this subtree shard is a stale
        // graft for this record. A non-graft local dentry, or a graft into a
        // different shard, is left untouched (the latter is healed by that
        // shard's own record).
        if entry.dentry.child_type != FileType::Directory
            || entry.dentry.child.shard_index() != record.shard_index
        {
            return Ok(false);
        }
        // `remove_graft` is idempotent (Ok(None) if already gone) and refuses a
        // same-shard/non-graft dentry, so this can only drop a genuine orphan.
        self.remove_graft(parent_inode, name)?;
        Ok(true)
    }

    /// Tear down a cross-shard graft for `prefix_path`: remove the parent graft
    /// dentry AND reap the (empty) child subtree, then clear the control-plane
    /// registration. Idempotent and crash-safe.
    ///
    /// Ordering (control record = source of truth):
    ///   1. Clear `subtree_root_inode` in the control plane FIRST, so a
    ///      concurrent/subsequent `reconcile_grafts` can never re-create this
    ///      graft. This is the atomic DE-registration point.
    ///   2. `rmdir` the child subtree root ON THE CHILD SHARD (routed by the
    ///      prefix path, where the contents actually live). This is the atomic
    ///      emptiness gate: a non-empty subtree fails with `DirectoryNotEmpty`,
    ///      in which case the control record is RESTORED and the parent dentry is
    ///      left untouched — the graft stays fully registered (a clean no-op).
    ///   3. Remove the parent graft dentry via the dedicated `remove_graft` path.
    ///
    /// Recursive subtree deletion is OUT OF SCOPE: a non-empty child subtree
    /// returns `DirectoryNotEmpty`; the caller must empty it first.
    pub fn unregister_graft(&self, prefix_path: &str) -> Result<(), ClientError> {
        let control = self.fleet_control()?.clone();
        let (parent_inode, name) = self.graft_parent_inode_and_name(prefix_path)?;

        // Find the subtree shard record (by prefix) so we can clear/restore its
        // durable graft target. If there is no such record, the graft was never
        // registered through this path; fall through to a best-effort dentry
        // removal so the op stays idempotent.
        let records = control
            .list_shards()
            .map_err(|err| ClientError::Protocol(format!("control list_shards failed: {err}")))?;
        let subtree_record: Option<ShardRecord> = records.into_iter().find(|record| {
            record.shard_index != DEFAULT_SHARD_INDEX && record.prefix == prefix_path
        });

        // 1. De-register first: reconcile must never resurrect this graft.
        if let Some(record) = &subtree_record {
            control
                .set_subtree_root_inode(&record.shard_id, None)
                .map_err(|err| {
                    ClientError::Protocol(format!("control clear subtree_root_inode failed: {err}"))
                })?;
        }

        // 2. Reap the child subtree root on the CHILD shard (path-routed). The
        //    emptiness check happens here, where the contents live.
        match self.rmdir(prefix_path) {
            Ok(_) => {}
            // Already gone (idempotent re-run, or never created): fine.
            Err(err) if crate::is_not_found(&err) => {}
            // ANY other child-removal failure (non-empty, transport, fence, ...)
            // means the subtree still exists, so the graft must stay registered:
            // restore the durable target we cleared in step 1 and surface the
            // error with the graft fully intact. Otherwise we would leave the
            // control plane de-registered while the child subtree is still live,
            // and a later reconcile could not heal it (no target to rebuild from).
            Err(err) => {
                if let Some(record) = &subtree_record {
                    let _ =
                        control.set_subtree_root_inode(&record.shard_id, record.subtree_root_inode);
                }
                return Err(err);
            }
        }

        // 3. Remove the parent graft dentry (idempotent: None when already gone).
        self.remove_graft(parent_inode, name)?;
        Ok(())
    }

    pub fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::CreateDirPath {
                    path: path.clone(),
                    mode,
                    uid,
                    gid,
                })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
                    MetadataRpcResult::Batch { results } => {
                        results.into_iter().map(envelope_result).collect()
                    }
                    other => return Err(unexpected_result(other)),
                };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateFilePath {
            path: path.to_owned(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let request = create_files_request(chunk, mode, uid, gid)?;
            let results: Vec<Result<MetadataRpcResult, ClientError>> = match self.call(request)? {
                MetadataRpcResult::Batch { results } => {
                    results.into_iter().map(envelope_result).collect()
                }
                other => return Err(unexpected_result(other)),
            };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, ClientError> {
        match self.call(MetadataRpcRequest::StatPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::PathMetadata { metadata } => {
                metadata.map(wire_path_metadata).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_page(path, cursor.as_ref(), DEFAULT_LIST_PAGE_SIZE)?;
            let page_empty = page.entries.is_empty();
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if page_empty || cursor.as_ref() == Some(&next_cursor) {
                return Err(ClientError::Protocol(
                    "metadata list page cursor did not advance".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        Ok(entries)
    }

    pub fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_indexed_page(path, cursor.as_ref(), DEFAULT_LIST_PAGE_SIZE)?;
            let page_empty = page.entries.is_empty();
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if page_empty || cursor.as_ref() == Some(&next_cursor) {
                return Err(ClientError::Protocol(
                    "indexed metadata list page cursor did not advance".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        Ok(entries)
    }

    pub fn list_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPathPage {
            path: path.to_owned(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_indexed_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadIndexedPathPage {
            path: path.to_owned(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn stat_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Option<PathMetadata>, ClientError> {
        match self.call(MetadataRpcRequest::StatPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::PathMetadata { metadata } => {
                metadata.map(wire_path_metadata).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveFilePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_many(
        &self,
        paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::RemoveFilePath { path: path.clone() })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
                    MetadataRpcResult::Batch { results } => {
                        results.into_iter().map(envelope_result).collect()
                    }
                    other => return Err(unexpected_result(other)),
                };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDirPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::Link {
            inode: inode.get(),
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rmdir_many(
        &self,
        paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::RemoveEmptyDirPath { path: path.clone() })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
                    MetadataRpcResult::Batch { results } => {
                        results.into_iter().map(envelope_result).collect()
                    }
                    other => return Err(unexpected_result(other)),
                };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        self.ensure_same_shard_paths(source, destination)?;
        match self.call(MetadataRpcRequest::RenamePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::Rename {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, ClientError> {
        self.ensure_same_shard_paths(source, destination)?;
        match self.call(MetadataRpcRequest::RenameReplacePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_replace_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::RenameReplace {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot(&self, path: &str) -> Result<SnapshotPin, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtreePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn clone_subtree_path(&self, src: &str, dst: &str) -> Result<CloneOutcome, ClientError> {
        self.ensure_same_shard_paths(src, dst)?;
        match self.call(MetadataRpcRequest::CloneSubtreePath {
            src_path: src.to_owned(),
            dst_path: dst.to_owned(),
        })? {
            MetadataRpcResult::CloneSubtree { root, snapshot_id } => Ok(CloneOutcome {
                root: inode_id(root)?,
                snapshot_id,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn diff_subtrees(&self, a: &str, b: &str) -> Result<Vec<SubtreeDelta>, ClientError> {
        self.ensure_same_shard_paths(a, b)?;
        match self.call(MetadataRpcRequest::DiffSubtrees {
            a_path: a.to_owned(),
            b_path: b.to_owned(),
        })? {
            MetadataRpcResult::SubtreeDeltas { deltas } => {
                Ok(deltas.into_iter().map(subtree_delta).collect())
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_subtree_path(&self, path: &str) -> Result<SnapshotOutcome, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtreePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Snapshot { snapshot } => Ok(SnapshotOutcome {
                snapshot_id: snapshot.snapshot_id,
                read_version: snapshot.read_version,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rollback_subtree_path(&self, target: &str, snapshot_id: u64) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::RollbackSubtreePath {
            target_path: target.to_owned(),
            snapshot_id,
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtree { root: root.get() })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotPin { snapshot_id })? {
            MetadataRpcResult::SnapshotPin { snapshot } => snapshot.map(wire_snapshot).transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, ClientError> {
        match self.call(MetadataRpcRequest::RetireSnapshot { snapshot_id })? {
            MetadataRpcResult::RetiredSnapshot { retired } => Ok(retired),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn renew_snapshot(&self, snapshot_id: u64, lease_ms: u64) -> Result<bool, ClientError> {
        match self.call(MetadataRpcRequest::RenewSnapshot {
            snapshot_id,
            lease_ms,
        })? {
            MetadataRpcResult::RenewedSnapshot { renewed } => Ok(renewed),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_body_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<ObjectReadPlan, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("body read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadBodyPlan {
            inode: inode.get(),
            generation,
            offset,
            len,
        })? {
            MetadataRpcResult::BodyReadPlan { plan } => wire_body_read_plan(plan),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn open_path_read_plan(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<PathLayoutOpen, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("path read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::OpenPathReadPlan {
            path: path.to_owned(),
            offset,
            len,
            expected_generation,
        })? {
            MetadataRpcResult::OpenPathReadPlan {
                metadata,
                lease,
                plan,
            } => Ok(PathLayoutOpen {
                metadata: wire_path_metadata(metadata)?,
                lease: wire_read_lease(lease)?,
                plan: wire_body_read_plan(plan)?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    /// Open read plans for a batch of paths, returning plans in the SAME order as
    /// `requests` (callers rely on positional `plans[i] ↔ requests[i]`).
    ///
    /// In single-shard mode this is one contiguous-chunked batch RPC. In fleet
    /// mode the requests may target different shards, so they are grouped by their
    /// owning shard (each batch RPC must be single-shard — the server routes a
    /// batch by its first entry's path), one chunked batch is sent per shard, and
    /// the returned plans are re-scattered back into the original input order.
    ///
    /// Generation pinning: a single file's windows can span more than one internal
    /// chunk (`> MAX_BATCH_RPC_REQUESTS`), and each chunk is a separate RPC at its
    /// own read version. To avoid a torn read spliced across two generations, the
    /// generation observed for a path in its first chunk is pinned as the
    /// `expected_generation` of that path's later chunks (when the caller did not
    /// already pin one). A concurrent rewrite between chunks then surfaces a clean
    /// `StaleBodyGeneration` instead of mixed bytes. (The single-file
    /// `read_path_ranges` applies the same discipline window-to-window.)
    pub fn open_path_read_plan_batch(
        &self,
        requests: &[PathLayoutOpenRequest],
    ) -> Result<Vec<PathLayoutOpen>, ClientError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        // Generation pinned per path from its first opened chunk, applied to that
        // path's later chunks so all of a file's windows resolve at one generation.
        let mut pinned: HashMap<String, u64> = HashMap::new();
        match &self.mode {
            RoutingMode::SingleShard { .. } => {
                // Every request shares the one endpoint: send contiguous chunks in
                // input order and collect the plans as they arrive.
                let mut opens = Vec::with_capacity(requests.len());
                for chunk in requests.chunks(MAX_BATCH_RPC_REQUESTS) {
                    self.send_open_path_read_plan_chunk(
                        chunk.to_vec(),
                        &mut pinned,
                        &mut |open| {
                            opens.push(open);
                            Ok(())
                        },
                    )?;
                }
                Ok(opens)
            }
            RoutingMode::Fleet(router) => {
                // Group the input indices by owning shard so each batch RPC stays
                // single-shard, then re-scatter the per-shard results back into the
                // caller's order.
                let mut by_shard: HashMap<u16, Vec<usize>> = HashMap::new();
                for (index, request) in requests.iter().enumerate() {
                    let shard = router.route_path(&request.path);
                    by_shard.entry(shard).or_default().push(index);
                }
                let mut opens: Vec<Option<PathLayoutOpen>> =
                    (0..requests.len()).map(|_| None).collect::<Vec<_>>();
                for indices in by_shard.values() {
                    for chunk_indices in indices.chunks(MAX_BATCH_RPC_REQUESTS) {
                        let chunk = chunk_indices
                            .iter()
                            .map(|&index| requests[index].clone())
                            .collect::<Vec<_>>();
                        let mut targets = chunk_indices.iter();
                        self.send_open_path_read_plan_chunk(chunk, &mut pinned, &mut |open| {
                            let target = *targets.next().ok_or_else(|| {
                                ClientError::Protocol(
                                    "fleet batch returned more plans than requested".to_owned(),
                                )
                            })?;
                            opens[target] = Some(open);
                            Ok(())
                        })?;
                    }
                }
                opens
                    .into_iter()
                    .enumerate()
                    .map(|(index, open)| {
                        open.ok_or_else(|| {
                            ClientError::Protocol(format!(
                                "fleet batch read plan missing for request {index}"
                            ))
                        })
                    })
                    .collect()
            }
        }
    }

    /// Send one single-shard chunk of layout-open requests and hand each decoded
    /// plan to `place` in chunk order. The chunk must be non-empty and
    /// single-shard; `self.call` routes it by its first entry's path (with
    /// fleet-mode refresh+retry on an owner handoff).
    ///
    /// `pinned` carries the generation observed for each path across chunks: an
    /// entry without a caller-supplied `expected_generation` inherits its path's
    /// pinned generation (if any), and every returned plan records its path's
    /// generation back into the map for the path's subsequent chunks.
    fn send_open_path_read_plan_chunk(
        &self,
        mut chunk: Vec<PathLayoutOpenRequest>,
        pinned: &mut HashMap<String, u64>,
        place: &mut dyn FnMut(PathLayoutOpen) -> Result<(), ClientError>,
    ) -> Result<(), ClientError> {
        for request in &mut chunk {
            if request.expected_generation.is_none() {
                request.expected_generation = pinned.get(&request.path).copied();
            }
        }
        let wire_requests = chunk
            .iter()
            .map(wire_open_path_read_plan_request)
            .collect::<Result<Vec<_>, ClientError>>()?;
        match self.call(MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: wire_requests,
        })? {
            MetadataRpcResult::OpenPathReadPlanBatch { plans } => {
                if plans.len() != chunk.len() {
                    return Err(ClientError::Protocol(format!(
                        "metadata returned {} batch read plans for {} requests",
                        plans.len(),
                        chunk.len()
                    )));
                }
                for (open, request) in plans.into_iter().zip(&chunk) {
                    let open = PathLayoutOpen {
                        metadata: wire_path_metadata(open.metadata)?,
                        lease: wire_read_lease(open.lease)?,
                        plan: wire_body_read_plan(open.plan)?,
                    };
                    // Pin this path's generation for its later chunks so a file's
                    // windows never splice two generations together.
                    pinned
                        .entry(request.path.clone())
                        .or_insert(open.lease.generation);
                    place(open)?;
                }
                Ok(())
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_artifact_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadArtifactPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_file_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("snapshot read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadFilePathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
            offset,
            len,
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("snapshot read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadFileAtSnapshot {
            snapshot_id,
            inode: inode.get(),
            offset,
            len,
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadSymlink { inode: inode.get() })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadSymlinkAtSnapshot {
            snapshot_id,
            inode: inode.get(),
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn prepare_artifact(
        &self,
        parent: InodeId,
        name: DentryName,
        replace: bool,
    ) -> Result<ClientPreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::PrepareArtifact {
            parent: parent.get(),
            name: rpc_name(&name)?,
            replace,
        })? {
            MetadataRpcResult::PreparedArtifact { prepared } => wire_prepared_artifact(prepared),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn prepare_artifact_path(
        &self,
        path: &str,
        replace: bool,
    ) -> Result<ClientPreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::PrepareArtifactPath {
            path: path.to_owned(),
            replace,
        })? {
            MetadataRpcResult::PreparedArtifact { prepared } => wire_prepared_artifact(prepared),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn publish_prepared_artifact(
        &self,
        prepared: ClientPreparedArtifact,
        body: BodyDescriptor,
        chunks: Vec<ChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::PublishPreparedArtifact {
            prepared: prepared_artifact_to_wire(&prepared)?,
            body: Box::new(body_to_wire(&body)),
            chunks: chunks.iter().map(chunk_to_wire).collect(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn publish_prepared_artifact_staged_session(
        &self,
        prepared: ClientPreparedArtifact,
        request: PublishArtifactStagedSession,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared: prepared_artifact_to_wire(&prepared)?,
            producer: request.producer,
            digest_uri: request.digest_uri,
            content_type: request.content_type,
            manifest_id: request.manifest_id,
            size: request.size,
            chunks: request.chunks.iter().map(chunk_to_wire).collect(),
            staged: staged_object_set_to_wire(&request.staged),
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    /// The single chokepoint every typed RPC flows through. Resolves the target
    /// shard endpoint, sends the request, and — in fleet mode — re-resolves the
    /// routing map and retries on an owner handoff (`NotOwner`/stale owner).
    fn call(&self, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ClientError> {
        let body =
            encode_request(&request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        match &self.mode {
            RoutingMode::SingleShard { address } => self.call_endpoint(*address, &body),
            RoutingMode::Fleet(router) => self.call_fleet(router, &request, &body),
        }
    }

    /// Primary cross-shard fence for path-pair ops (rename, clone, diff). A
    /// dual-path op routes on its *source* path, so the server resolves the
    /// destination inside the source shard's namespace and a cross-shard
    /// destination would surface as a misleading `NotFound`. Resolving both paths
    /// to their owning shard with the same longest-prefix router and rejecting a
    /// mismatch here gives the correct `EXDEV` (and skips the RPC entirely).
    ///
    /// SingleShard mode has exactly one shard, so this can never trip — skip it.
    fn ensure_same_shard_paths(&self, source: &str, destination: &str) -> Result<(), ClientError> {
        if let RoutingMode::Fleet(router) = &self.mode {
            let source_shard = router.route_path(source);
            let dest_shard = router.route_path(destination);
            if source_shard != dest_shard {
                return Err(ClientError::Metadata(nokv_meta::MetadError::CrossShard {
                    source_shard,
                    dest_shard,
                }));
            }
        }
        Ok(())
    }

    /// Fleet-mode dispatch: route the request to the owning shard's endpoint and
    /// retry on a handoff. Each retry first refreshes the routing map+endpoints
    /// from the control plane, then re-resolves, bounded by [`FLEET_MAX_ATTEMPTS`]
    /// so a permanently-missing shard fails fast instead of looping.
    fn call_fleet(
        &self,
        router: &FleetRouter,
        request: &MetadataRpcRequest,
        body: &[u8],
    ) -> Result<MetadataRpcResult, ClientError> {
        let mut last_resolve_err: Option<ClientError> = None;
        for attempt in 0..FLEET_MAX_ATTEMPTS {
            // On every attempt after the first, the previous one hit a handoff (or
            // an unresolved index): refresh both caches before re-resolving.
            if attempt > 0 {
                router.refresh(self)?;
            }
            let shard_index = router.route(request)?;
            let address = match router.endpoint(shard_index) {
                Some(address) => address,
                None => {
                    // The map names a shard we have no endpoint for yet. Refresh
                    // and retry; if it is still missing after the refresh on the
                    // next iteration, surface a clear error.
                    last_resolve_err = Some(ClientError::Protocol(format!(
                        "fleet routing has no endpoint for shard index {shard_index}"
                    )));
                    continue;
                }
            };
            match self.call_endpoint(address, body) {
                Ok(result) => return Ok(result),
                Err(err) if attempt + 1 < FLEET_MAX_ATTEMPTS && is_owner_handoff(&err) => {
                    // Owner moved: loop to refresh + re-resolve against the new owner.
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_resolve_err.unwrap_or_else(|| {
            ClientError::Protocol(
                "fleet routing exhausted retries without resolving a shard owner".to_owned(),
            )
        }))
    }

    /// Send one already-encoded request to a fixed endpoint over the pooled
    /// pipelined connection, dropping the connection on a transport error so the
    /// next call reconnects.
    fn call_endpoint(
        &self,
        address: SocketAddr,
        body: &[u8],
    ) -> Result<MetadataRpcResult, ClientError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let connection = self.connection(address)?;
        let body = match connection.call(request_id, body, self.timeout) {
            Ok(body) => body,
            Err(err @ ClientError::Io(_)) => {
                self.drop_connection(address);
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let envelope =
            decode_envelope(&body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        envelope_result(envelope)
    }

    fn connection(&self, address: SocketAddr) -> Result<Arc<PipelinedConnection>, ClientError> {
        let mut guard = self.connections.lock().expect("metadata rpc connections");
        if let Some(connection) = guard.get(&address) {
            return Ok(Arc::clone(connection));
        }
        let connection = Arc::new(PipelinedConnection::connect(address)?);
        guard.insert(address, Arc::clone(&connection));
        Ok(connection)
    }

    fn drop_connection(&self, address: SocketAddr) {
        self.connections
            .lock()
            .expect("metadata rpc connections")
            .remove(&address);
    }
}

impl FleetRouter {
    /// Resolve the target shard index for a request using the *same* routing-key
    /// extractor the server uses, against the locally cached map. Path requests
    /// match by longest prefix; bare-inode requests read the index out of the id.
    fn route(&self, request: &MetadataRpcRequest) -> Result<u16, ClientError> {
        Ok(match request_routing_key(request) {
            RoutingKey::Path(path) => {
                let map = self.shard_map.read().expect("fleet shard map");
                map.route(self.mount, path)
            }
            RoutingKey::Inode(raw) => InodeId::new(raw)
                .map_err(|err| ClientError::Protocol(err.to_string()))?
                .shard_index(),
            RoutingKey::Default => DEFAULT_SHARD_INDEX,
        })
    }

    /// Resolve the owning shard index for an absolute `path` against the cached
    /// routing map — the same longest-prefix match `route` applies to a path
    /// request. Used to group a layout-open batch by shard before fan-out.
    fn route_path(&self, path: &str) -> u16 {
        self.shard_map
            .read()
            .expect("fleet shard map")
            .route(self.mount, path)
    }

    /// The endpoint currently mapped to `shard_index`, if the cache knows it.
    fn endpoint(&self, shard_index: u16) -> Option<SocketAddr> {
        self.endpoints
            .read()
            .expect("fleet endpoints")
            .get(&shard_index)
            .copied()
    }

    /// Rebuild the routing map and endpoint table from `control.list_shards()`.
    /// Both caches are swapped under their write locks so a concurrent `route`
    /// never sees a map entry whose endpoint is absent.
    fn refresh(&self, client: &MetadataClient) -> Result<(), ClientError> {
        let (shard_map, endpoints) = resolve_fleet_routes(self.control.as_ref(), self.mount)?;
        // Drop pooled connections to endpoints that are no longer current so a
        // handed-off owner's stale socket is not reused.
        let live: std::collections::HashSet<SocketAddr> = endpoints.values().copied().collect();
        {
            let mut pool = client.connections.lock().expect("metadata rpc connections");
            pool.retain(|address, _| live.contains(address));
        }
        *self.shard_map.write().expect("fleet shard map") = shard_map;
        *self.endpoints.write().expect("fleet endpoints") = endpoints;
        Ok(())
    }
}

/// Build the `(shard_map, endpoints)` pair for fleet routing from the control
/// store. Each registered shard contributes a longest-prefix route (non-default
/// shards only — the default shard owns `/` implicitly) and a
/// `shard_index -> endpoint` entry. Shards without an endpoint (currently
/// unowned) are skipped; a request that routes to one fails resolution and is
/// retried after the next refresh.
fn resolve_fleet_routes(
    control: &dyn ControlStore,
    mount: MountId,
) -> Result<(ShardMap, HashMap<u16, SocketAddr>), ClientError> {
    let records = control
        .list_shards()
        .map_err(|err| ClientError::Protocol(format!("control list_shards failed: {err}")))?;
    let mut routes = Vec::new();
    let mut endpoints = HashMap::new();
    for record in records {
        if let Some(endpoint) = record.endpoint.as_deref() {
            let address = endpoint.parse::<SocketAddr>().map_err(|err| {
                ClientError::Protocol(format!(
                    "control shard {} has unparseable endpoint {endpoint:?}: {err}",
                    record.shard_id
                ))
            })?;
            endpoints.insert(record.shard_index, address);
        }
        // The default shard owns "/" implicitly; only non-default subtree shards
        // are entered as longest-prefix routes (mirrors the server's ShardMap).
        if record.shard_index != DEFAULT_SHARD_INDEX {
            let prefix = ShardPrefix::parse(&format!("mount-{}:{}", mount.get(), record.prefix))
                .map_err(|err| {
                    ClientError::Protocol(format!(
                        "control shard {} has invalid prefix {:?}: {err}",
                        record.shard_id, record.prefix
                    ))
                })?;
            routes.push(ShardRoute {
                shard_index: record.shard_index,
                prefix,
            });
        }
    }
    Ok((ShardMap::from_routes(routes), endpoints))
}

/// Whether an error means "the owner moved" — re-resolve the shard map and retry
/// against the new owner. Covers an explicit `NotOwner`, a stale owner epoch, and
/// a self-fenced expired lease.
fn is_owner_handoff(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Metadata(
            nokv_meta::MetadError::NotOwner { .. }
                | nokv_meta::MetadError::StaleOwnerEpoch { .. }
                | nokv_meta::MetadError::LeaseExpired { .. }
        )
    )
}

fn create_files_request(
    paths: &[String],
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<MetadataRpcRequest, ClientError> {
    let mut parent_path = None;
    let mut names = Vec::with_capacity(paths.len());
    for path in paths {
        let (parent, name) = rpc_parent_and_name(path)?;
        if parent_path
            .as_deref()
            .is_some_and(|existing| existing != parent)
        {
            let requests = paths
                .iter()
                .map(|path| MetadataRpcRequest::CreateFilePath {
                    path: path.clone(),
                    mode,
                    uid,
                    gid,
                })
                .collect();
            return Ok(MetadataRpcRequest::Batch { requests });
        }
        parent_path = Some(parent);
        names.push(name);
    }
    Ok(MetadataRpcRequest::CreateFilesInDirPath {
        parent_path: parent_path.unwrap_or_else(|| "/".to_owned()),
        names,
        mode,
        uid,
        gid,
    })
}

fn wire_open_path_read_plan_request(
    request: &PathLayoutOpenRequest,
) -> Result<WireOpenPathReadPlanRequest, ClientError> {
    let len = u64::try_from(request.len)
        .map_err(|_| ClientError::Protocol("path read length exceeds u64".to_owned()))?;
    Ok(WireOpenPathReadPlanRequest {
        path: request.path.clone(),
        offset: request.offset,
        len,
        expected_generation: request.expected_generation,
    })
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
