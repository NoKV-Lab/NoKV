/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Explicit standalone composition for one exclusively opened Holt store.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_control::{
    AgentId, CatalogEntryState, ObjectNamespaceId, StoreId, StoreManifest, StoreProvider,
    PROVIDER_NAMESPACE_DIGEST_BYTES, SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_meta::workspace as meta;
use nokv_meta_holt::{HoltOptions, HoltStore, TreeBinding, HOLT_PHYSICAL_ENCODING_VERSION};
use nokv_meta_store::TxnStore;
use nokv_protocol::{
    DiscoveredRoute, ErrorCode, LogicalShardIdentity, ObjectNamespaceIdentity, OwnerEndpoint,
    RootIdentity, RootRoute, RouteState, RpcFailure,
};
use nokv_types::{
    CommandDigest, LogicalShardId, OwnerEpoch, PlacementGeneration, RequestId, RootActivationState,
    RootId, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use crate::{
    HoltMetadataUrl, MetadataWorkspaceRequestExecutor, RootOwnerRegistry, RouteDiscoverySource,
    ServerError, ServerOptions, WorkspaceRequestExecutor, WorkspaceServer,
};

const HEADER_FILE: &str = "nokv-store.bin";
const HEADER_MAGIC: &[u8; 8] = b"NOKVHLT1";
const HEADER_ENCODING_VERSION: u8 = 1;
const HEADER_CHECKSUM_BYTES: usize = 32;
const MAX_HEADER_BYTES: usize = 16 * 1024 * 1024;
const MAX_LOCAL_ROOTS: usize = 65_536;
const PATH_DIGEST_DOMAIN: &[u8] = b"nokv/holt/provider-namespace/v1\0";
const STORE_ID_DOMAIN: &[u8] = b"nokv/holt/store-id/v1\0";
const SHARD_ID_DOMAIN: &[u8] = b"nokv/holt/logical-shard-id/v1\0";
const NAMESPACE_ID_DOMAIN: &[u8] = b"nokv/holt/object-namespace-id/v1\0";
const REQUEST_ID_DOMAIN: &[u8] = b"nokv/holt/provision-request-id/v1\0";

static STORE_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static HEADER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoltFormatState {
    Created,
    Existing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoltFormatOutcome {
    pub state: HoltFormatState,
    pub manifest: StoreManifest,
    pub logical_shard_id: LogicalShardId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HoltRootCatalogEntry {
    pub root_id: RootId,
    pub agent_id: AgentId,
    pub object_namespace_id: ObjectNamespaceId,
    pub placement_generation: PlacementGeneration,
    pub state: CatalogEntryState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoltProvisionOutcome {
    pub root: HoltRootCatalogEntry,
    pub logical_shard_id: LogicalShardId,
    pub preexisting: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HoltHeader {
    manifest: StoreManifest,
    logical_shard_id: LogicalShardId,
    roots: Vec<HoltRootCatalogEntry>,
}

/// Live standalone owner. Its `MetaShard` retains the only Holt handle and,
/// therefore, the OS-backed exclusive database lock until this value and all
/// derived server/executor handles are dropped.
pub struct HoltServingRuntime {
    manifest: StoreManifest,
    logical_shard_id: LogicalShardId,
    owner_epoch: OwnerEpoch,
    roots: Vec<HoltRootCatalogEntry>,
    meta: Arc<meta::MetaShard>,
    registry: Arc<RootOwnerRegistry>,
    discovery: Arc<HoltRouteDiscovery>,
}

impl HoltServingRuntime {
    pub fn manifest(&self) -> &StoreManifest {
        &self.manifest
    }

    pub const fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub const fn owner_epoch(&self) -> OwnerEpoch {
        self.owner_epoch
    }

    pub fn roots(&self) -> &[HoltRootCatalogEntry] {
        &self.roots
    }

    pub fn meta(&self) -> &Arc<meta::MetaShard> {
        &self.meta
    }

    pub fn registry(&self) -> &Arc<RootOwnerRegistry> {
        &self.registry
    }

    pub fn workspace_server(&self, options: ServerOptions) -> Result<WorkspaceServer, ServerError> {
        Ok(
            WorkspaceServer::new_local(options, Arc::clone(&self.registry))?
                .with_discovery_source(self.discovery.clone()),
        )
    }
}

#[derive(Clone)]
struct HoltRouteDiscovery {
    routes: Vec<DiscoveredRoute>,
}

impl RouteDiscoverySource for HoltRouteDiscovery {
    fn discover_route(&self, root_id: RootIdentity) -> Result<DiscoveredRoute, RpcFailure> {
        self.routes
            .iter()
            .find(|route| route.root_id == root_id)
            .cloned()
            .ok_or_else(|| RpcFailure {
                code: ErrorCode::RouteUnavailable,
                message: "root is not provisioned in this standalone Holt store".to_owned(),
                retryable: false,
                conflict: None,
                current_generation: None,
                route_hint: None,
            })
    }
}

/// Create or inspect one explicitly selected standalone Holt store.
///
/// An existing store is opened and verified; it is never initialized through
/// the open path. A nonempty unmarked or mismatching directory fails closed.
pub fn format_holt(
    url: &HoltMetadataUrl,
    created_by_version: &str,
) -> Result<HoltFormatOutcome, ServerError> {
    let identity = canonical_format_path(url.path())?;
    if location_is_missing_or_empty(&identity)? {
        format_new_holt(&identity, created_by_version)
    } else {
        let (header, _meta) = open_holt(&identity)?;
        Ok(HoltFormatOutcome {
            state: HoltFormatState::Existing,
            manifest: header.manifest,
            logical_shard_id: header.logical_shard_id,
        })
    }
}

/// Provision one root in the fixed standalone shard. Exact repeats preserve
/// the object namespace and placement. A different Agent identity fails
/// closed.
pub fn provision_holt(
    url: &HoltMetadataUrl,
    root_id: RootId,
    agent_id: AgentId,
) -> Result<HoltProvisionOutcome, ServerError> {
    let identity = canonical_existing_path(url.path())?;
    let (mut header, meta) = open_holt(&identity)?;
    let existing = header
        .roots
        .iter()
        .position(|entry| entry.root_id == root_id);
    let preexisting = existing.is_some();
    let mut root = match existing {
        Some(index) => {
            let current = header.roots[index];
            if current.agent_id != agent_id {
                return Err(ServerError::InvalidBootstrap(format!(
                    "root {root_id:?} is already bound to another Agent identity"
                )));
            }
            match current.state {
                CatalogEntryState::Ready => {
                    validate_ready_root_fence(&meta, header.logical_shard_id, current)?;
                    return Ok(HoltProvisionOutcome {
                        root: current,
                        logical_shard_id: header.logical_shard_id,
                        preexisting: true,
                    });
                }
                CatalogEntryState::Provisioning => current,
                CatalogEntryState::Retired => {
                    return Err(ServerError::InvalidBootstrap(format!(
                        "root {root_id:?} is retired and cannot be reprovisioned"
                    )));
                }
            }
        }
        None => {
            if header.roots.len() >= MAX_LOCAL_ROOTS {
                return Err(ServerError::InvalidBootstrap(format!(
                    "standalone Holt catalog exceeds {MAX_LOCAL_ROOTS} roots"
                )));
            }
            let entry = HoltRootCatalogEntry {
                root_id,
                agent_id,
                object_namespace_id: derive_namespace_id(header.manifest.store_id()),
                placement_generation: PlacementGeneration::new(1)
                    .expect("one is a valid placement generation"),
                state: CatalogEntryState::Provisioning,
            };
            header.roots.push(entry);
            header.roots.sort_by_key(|entry| entry.root_id);
            persist_header(&identity, &header)?;
            entry
        }
    };

    let owner_epoch = advance_local_owner(&meta)?;
    reconcile_root_fence(&meta, header.logical_shard_id, owner_epoch, root)?;
    if root.state != CatalogEntryState::Ready {
        root.state = CatalogEntryState::Ready;
        let stored = header
            .roots
            .iter_mut()
            .find(|entry| entry.root_id == root_id)
            .expect("new or existing root remains in header");
        *stored = root;
        persist_header(&identity, &header)?;
    }
    Ok(HoltProvisionOutcome {
        root,
        logical_shard_id: header.logical_shard_id,
        preexisting,
    })
}

/// Open the exact formatted Holt store, complete local recovery, advance the
/// local owner epoch, install all ready routes, and expose local seed
/// discovery. No control service or distributed recovery worker is involved.
pub fn serve_holt(
    url: &HoltMetadataUrl,
    endpoint: SocketAddr,
) -> Result<HoltServingRuntime, ServerError> {
    if endpoint.port() == 0 {
        return Err(ServerError::InvalidOptions(
            "standalone advertised endpoint must have a nonzero port".to_owned(),
        ));
    }
    let identity = canonical_existing_path(url.path())?;
    let (header, meta) = open_holt(&identity)?;
    meta.fsck_recovery()?;
    let owner_epoch = advance_local_owner(&meta)?;
    let ready = header
        .roots
        .iter()
        .copied()
        .filter(|entry| entry.state == CatalogEntryState::Ready)
        .collect::<Vec<_>>();
    if ready.is_empty() {
        return Err(ServerError::InvalidBootstrap(
            "standalone Holt store has no Ready roots; run provision first".to_owned(),
        ));
    }
    let registry = Arc::new(RootOwnerRegistry::new());
    let executor: Arc<dyn WorkspaceRequestExecutor> =
        Arc::new(MetadataWorkspaceRequestExecutor::new(Arc::clone(&meta)));
    let owner_endpoint = OwnerEndpoint::new(endpoint.to_string())?;
    let mut discovered = Vec::with_capacity(ready.len());
    for root in &ready {
        validate_ready_root_fence(&meta, header.logical_shard_id, *root)?;
        let route = root_route(header.logical_shard_id, owner_epoch, *root);
        registry.install(route, Arc::clone(&executor))?;
        discovered.push(DiscoveredRoute::new(
            route,
            owner_epoch.get(),
            owner_endpoint.clone(),
            RouteState::Serving,
        )?);
    }
    Ok(HoltServingRuntime {
        manifest: header.manifest,
        logical_shard_id: header.logical_shard_id,
        owner_epoch,
        roots: ready,
        meta,
        registry,
        discovery: Arc::new(HoltRouteDiscovery { routes: discovered }),
    })
}

fn format_new_holt(
    path: &Path,
    created_by_version: &str,
) -> Result<HoltFormatOutcome, ServerError> {
    let store_id = new_store_id(path);
    let logical_shard_id = derive_logical_shard_id(store_id);
    let manifest = StoreManifest::new(
        store_id,
        StoreProvider::Holt,
        SUPPORTED_WORKSPACE_FORMAT_VERSION,
        HOLT_PHYSICAL_ENCODING_VERSION,
        provider_namespace_digest(path),
        created_by_version,
    )?;
    let holt = HoltStore::initialize(holt_options(path))?;
    let store: Arc<dyn TxnStore> = Arc::new(holt);
    let meta = meta::MetaShard::initialize(store, logical_shard_id)?;
    meta.fsck_recovery()?;
    let header = HoltHeader {
        manifest: manifest.clone(),
        logical_shard_id,
        roots: Vec::new(),
    };
    persist_header(path, &header)?;
    drop(meta);
    Ok(HoltFormatOutcome {
        state: HoltFormatState::Created,
        manifest,
        logical_shard_id,
    })
}

fn open_holt(path: &Path) -> Result<(HoltHeader, Arc<meta::MetaShard>), ServerError> {
    let holt = HoltStore::open(holt_options(path))?;
    let header = read_header(path)?;
    validate_header(path, &header)?;
    let store: Arc<dyn TxnStore> = Arc::new(holt);
    let meta = Arc::new(meta::MetaShard::open(store, header.logical_shard_id)?);
    Ok((header, meta))
}

fn holt_options(path: &Path) -> HoltOptions {
    HoltOptions::file(
        path,
        meta::keyspaces()
            .iter()
            .map(|definition| TreeBinding::new(definition.id, definition.name)),
        meta::store_limits(),
    )
}

fn advance_local_owner(meta: &meta::MetaShard) -> Result<OwnerEpoch, ServerError> {
    let current = meta.current_owner_epoch()?;
    let next_value = current
        .map(OwnerEpoch::get)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| {
            ServerError::InvalidBootstrap("local owner epoch is exhausted".to_owned())
        })?;
    let next = OwnerEpoch::new(next_value)
        .map_err(|error| ServerError::InvalidBootstrap(error.to_string()))?;
    meta.advance_owner_epoch(current, next)?;
    Ok(next)
}

fn reconcile_root_fence(
    meta: &meta::MetaShard,
    logical_shard_id: LogicalShardId,
    owner_epoch: OwnerEpoch,
    root: HoltRootCatalogEntry,
) -> Result<(), ServerError> {
    match meta.root_fence(root.root_id)? {
        None => execute_fence_command(
            meta,
            logical_shard_id,
            owner_epoch,
            root,
            meta::RootFenceAction::Install,
            b"install",
        )?,
        Some(fence) => validate_root_fence(logical_shard_id, root, fence)?,
    }
    let fence = meta.root_fence(root.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap("root fence install produced no durable fence".to_owned())
    })?;
    validate_root_fence(logical_shard_id, root, fence)?;
    if fence.activation_state == RootActivationState::Installing {
        execute_fence_command(
            meta,
            logical_shard_id,
            owner_epoch,
            root,
            meta::RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            b"activate",
        )?;
    }
    validate_ready_root_fence(meta, logical_shard_id, root)
}

fn execute_fence_command(
    meta: &meta::MetaShard,
    logical_shard_id: LogicalShardId,
    owner_epoch: OwnerEpoch,
    root: HoltRootCatalogEntry,
    action: meta::RootFenceAction,
    action_name: &[u8],
) -> Result<(), ServerError> {
    let command = meta::MetadataCommand {
        schema_id: meta::SCHEMA_ID.to_owned(),
        root_id: root.root_id,
        logical_shard_id,
        object_namespace_id: Some(root.object_namespace_id),
        placement_generation: root.placement_generation,
        owner_epoch,
        request_id: derive_request_id(root.root_id, action_name),
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: meta.current_read_version()?,
        root_fence_action: action,
        predicates: Vec::new(),
        mutations: Vec::new(),
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result: [b"nokv.holt.root-fence.v1/".as_slice(), action_name].concat(),
    }
    .seal();
    meta.execute(&command)?;
    Ok(())
}

fn validate_ready_root_fence(
    meta: &meta::MetaShard,
    logical_shard_id: LogicalShardId,
    root: HoltRootCatalogEntry,
) -> Result<(), ServerError> {
    let fence = meta.root_fence(root.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap(format!(
            "Ready root {:?} has no durable metadata fence",
            root.root_id
        ))
    })?;
    validate_root_fence(logical_shard_id, root, fence)?;
    if fence.activation_state != RootActivationState::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "root {:?} fence is {:?}, expected Active",
            root.root_id, fence.activation_state
        )));
    }
    Ok(())
}

fn validate_root_fence(
    logical_shard_id: LogicalShardId,
    root: HoltRootCatalogEntry,
    fence: meta::RootFence,
) -> Result<(), ServerError> {
    if fence.logical_shard_id != logical_shard_id
        || fence.object_namespace_id != Some(root.object_namespace_id)
        || fence.placement_generation != root.placement_generation
    {
        return Err(ServerError::InvalidBootstrap(format!(
            "root {:?} catalog does not match its metadata fence",
            root.root_id
        )));
    }
    if matches!(
        fence.activation_state,
        RootActivationState::Draining | RootActivationState::Fenced
    ) {
        return Err(ServerError::InvalidBootstrap(format!(
            "root {:?} fence is {:?}",
            root.root_id, fence.activation_state
        )));
    }
    Ok(())
}

fn root_route(
    logical_shard_id: LogicalShardId,
    owner_epoch: OwnerEpoch,
    root: HoltRootCatalogEntry,
) -> RootRoute {
    RootRoute {
        root_id: RootIdentity::from(root.root_id),
        logical_shard_id: LogicalShardIdentity::from(logical_shard_id),
        object_namespace_id: ObjectNamespaceIdentity::from(root.object_namespace_id),
        placement_generation: root.placement_generation.get(),
        owner_epoch: owner_epoch.get(),
    }
}

fn location_is_missing_or_empty(path: &Path) -> Result<bool, ServerError> {
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

fn canonical_format_path(path: &Path) -> Result<PathBuf, ServerError> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or_else(|| {
                ServerError::InvalidOptions("Holt metadata path has no parent".to_owned())
            })?;
            let name = path.file_name().ok_or_else(|| {
                ServerError::InvalidOptions("Holt metadata path has no final component".to_owned())
            })?;
            let parent =
                std::fs::canonicalize(parent).map_err(|source| ServerError::RecoveryPath {
                    path: parent.to_path_buf(),
                    source,
                })?;
            Ok(parent.join(name))
        }
        Err(source) => Err(ServerError::RecoveryPath {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn canonical_existing_path(path: &Path) -> Result<PathBuf, ServerError> {
    std::fs::canonicalize(path).map_err(|source| ServerError::RecoveryPath {
        path: path.to_path_buf(),
        source,
    })
}

fn provider_namespace_digest(path: &Path) -> [u8; PROVIDER_NAMESPACE_DIGEST_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(PATH_DIGEST_DOMAIN);
    hasher.update(path.as_os_str().as_encoded_bytes());
    hasher.finalize().into()
}

fn new_store_id(path: &Path) -> StoreId {
    let mut hasher = Sha256::new();
    hasher.update(STORE_ID_DOMAIN);
    hasher.update(path.as_os_str().as_encoded_bytes());
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
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    if id.iter().all(|byte| *byte == 0) {
        id[15] = 1;
    }
    StoreId::from_bytes(id)
}

fn derive_logical_shard_id(store_id: StoreId) -> LogicalShardId {
    LogicalShardId::from_bytes(derive_fixed_id(SHARD_ID_DOMAIN, &[store_id.as_bytes()]))
}

fn derive_namespace_id(store_id: StoreId) -> ObjectNamespaceId {
    ObjectNamespaceId::from_bytes(derive_fixed_id(NAMESPACE_ID_DOMAIN, &[store_id.as_bytes()]))
}

fn derive_request_id(root_id: RootId, action: &[u8]) -> RequestId {
    RequestId::from_bytes(derive_fixed_id(
        REQUEST_ID_DOMAIN,
        &[root_id.as_bytes(), action],
    ))
}

fn derive_fixed_id(domain: &[u8], parts: &[&[u8]]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update(part);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    if id.iter().all(|byte| *byte == 0) {
        id[15] = 1;
    }
    id
}

fn validate_header(path: &Path, header: &HoltHeader) -> Result<(), ServerError> {
    let manifest = &header.manifest;
    if manifest.provider() != StoreProvider::Holt
        || manifest.workspace_format_version() != SUPPORTED_WORKSPACE_FORMAT_VERSION
        || manifest.physical_encoding_version() != HOLT_PHYSICAL_ENCODING_VERSION
        || manifest.provider_namespace_digest() != &provider_namespace_digest(path)
    {
        return Err(ServerError::InvalidBootstrap(
            "Holt store manifest does not match the selected provider, format, encoding, or canonical path"
                .to_owned(),
        ));
    }
    if header.roots.len() > MAX_LOCAL_ROOTS {
        return Err(ServerError::InvalidBootstrap(format!(
            "Holt local catalog exceeds {MAX_LOCAL_ROOTS} roots"
        )));
    }
    for pair in header.roots.windows(2) {
        if pair[0].root_id >= pair[1].root_id {
            return Err(ServerError::InvalidBootstrap(
                "Holt local root catalog is not strictly ordered".to_owned(),
            ));
        }
    }
    let expected_namespace = derive_namespace_id(header.manifest.store_id());
    if header
        .roots
        .iter()
        .any(|root| root.object_namespace_id != expected_namespace)
    {
        return Err(ServerError::InvalidBootstrap(
            "Holt local catalog contains a foreign object namespace".to_owned(),
        ));
    }
    Ok(())
}

fn read_header(path: &Path) -> Result<HoltHeader, ServerError> {
    let marker = path.join(HEADER_FILE);
    let metadata =
        std::fs::symlink_metadata(&marker).map_err(|source| ServerError::RecoveryPath {
            path: marker.clone(),
            source,
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ServerError::InvalidBootstrap(format!(
            "{} is not a regular Holt store manifest",
            marker.display()
        )));
    }
    if metadata.len() > MAX_HEADER_BYTES as u64 {
        return Err(ServerError::InvalidBootstrap(format!(
            "Holt store manifest exceeds {MAX_HEADER_BYTES} bytes"
        )));
    }
    let file = File::open(&marker).map_err(|source| ServerError::RecoveryPath {
        path: marker.clone(),
        source,
    })?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((MAX_HEADER_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| ServerError::RecoveryPath {
            path: marker,
            source,
        })?;
    decode_header(&bytes)
}

fn persist_header(path: &Path, header: &HoltHeader) -> Result<(), ServerError> {
    let bytes = encode_header(header)?;
    let sequence = HEADER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = path.join(format!(
        "{HEADER_FILE}.tmp.{}.{sequence}",
        std::process::id()
    ));
    let final_path = path.join(HEADER_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| ServerError::RecoveryPath {
            path: temporary.clone(),
            source,
        })?;
    let write_result = (|| {
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &final_path)?;
        File::open(path)?.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    if let Err(source) = write_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(ServerError::RecoveryPath {
            path: final_path,
            source,
        });
    }
    Ok(())
}

fn encode_header(header: &HoltHeader) -> Result<Vec<u8>, ServerError> {
    if header.roots.len() > MAX_LOCAL_ROOTS {
        return Err(ServerError::InvalidBootstrap(format!(
            "Holt local catalog exceeds {MAX_LOCAL_ROOTS} roots"
        )));
    }
    let created_by = header.manifest.created_by_version().as_bytes();
    let created_len = u16::try_from(created_by.len()).map_err(|_| {
        ServerError::InvalidBootstrap("created-by version does not fit Holt manifest".to_owned())
    })?;
    let root_count = u32::try_from(header.roots.len()).expect("bounded root count fits u32");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(HEADER_MAGIC);
    bytes.push(HEADER_ENCODING_VERSION);
    bytes.extend_from_slice(header.manifest.store_id().as_bytes());
    bytes.push(header.manifest.provider() as u8);
    bytes.extend_from_slice(&header.manifest.workspace_format_version().to_be_bytes());
    bytes.push(header.manifest.physical_encoding_version());
    bytes.extend_from_slice(header.manifest.provider_namespace_digest());
    bytes.extend_from_slice(&created_len.to_be_bytes());
    bytes.extend_from_slice(created_by);
    bytes.extend_from_slice(header.logical_shard_id.as_bytes());
    bytes.extend_from_slice(&root_count.to_be_bytes());
    for root in &header.roots {
        bytes.extend_from_slice(root.root_id.as_bytes());
        bytes.extend_from_slice(root.agent_id.as_bytes());
        bytes.extend_from_slice(root.object_namespace_id.as_bytes());
        bytes.extend_from_slice(&root.placement_generation.get().to_be_bytes());
        bytes.push(root.state as u8);
    }
    let checksum: [u8; HEADER_CHECKSUM_BYTES] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    if bytes.len() > MAX_HEADER_BYTES {
        return Err(ServerError::InvalidBootstrap(format!(
            "Holt store manifest exceeds {MAX_HEADER_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

fn decode_header(bytes: &[u8]) -> Result<HoltHeader, ServerError> {
    if bytes.len() < HEADER_MAGIC.len() + 1 + HEADER_CHECKSUM_BYTES {
        return Err(invalid_header("header is truncated"));
    }
    let (body, checksum) = bytes.split_at(bytes.len() - HEADER_CHECKSUM_BYTES);
    if Sha256::digest(body).as_slice() != checksum {
        return Err(invalid_header("checksum mismatch"));
    }
    let mut decoder = HeaderDecoder::new(body);
    if decoder.take(HEADER_MAGIC.len(), "magic")? != HEADER_MAGIC {
        return Err(invalid_header("magic mismatch"));
    }
    if decoder.byte("header encoding")? != HEADER_ENCODING_VERSION {
        return Err(invalid_header("unknown header encoding"));
    }
    let store_id = StoreId::from_bytes(decoder.fixed("store id")?);
    let provider = StoreProvider::try_from(decoder.byte("provider")?)?;
    let workspace_format_version = decoder.u32("workspace format")?;
    let physical_encoding_version = decoder.byte("physical encoding")?;
    let provider_namespace_digest = decoder.fixed("provider namespace digest")?;
    let created_len = usize::from(decoder.u16("created-by length")?);
    let created_by = std::str::from_utf8(decoder.take(created_len, "created-by version")?)
        .map_err(|_| invalid_header("created-by version is not UTF-8"))?;
    let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed("logical shard id")?);
    let root_count = usize::try_from(decoder.u32("root count")?)
        .map_err(|_| invalid_header("root count does not fit usize"))?;
    if root_count > MAX_LOCAL_ROOTS {
        return Err(invalid_header("root count exceeds the serving bound"));
    }
    let manifest = StoreManifest::new(
        store_id,
        provider,
        workspace_format_version,
        physical_encoding_version,
        provider_namespace_digest,
        created_by,
    )?;
    let mut roots = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        let root_id = RootId::from_bytes(decoder.fixed("root id")?);
        let agent_id = AgentId::from_bytes(decoder.fixed("agent id")?);
        let object_namespace_id = ObjectNamespaceId::from_bytes(decoder.fixed("namespace id")?);
        let placement_generation = PlacementGeneration::new(decoder.u64("placement generation")?)
            .map_err(|error| invalid_header(&error.to_string()))?;
        let state = CatalogEntryState::try_from(decoder.byte("root state")?)?;
        roots.push(HoltRootCatalogEntry {
            root_id,
            agent_id,
            object_namespace_id,
            placement_generation,
            state,
        });
    }
    decoder.finish()?;
    Ok(HoltHeader {
        manifest,
        logical_shard_id,
        roots,
    })
}

fn invalid_header(reason: &str) -> ServerError {
    ServerError::InvalidBootstrap(format!("invalid Holt store manifest: {reason}"))
}

struct HeaderDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> HeaderDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize, field: &str) -> Result<&'a [u8], ServerError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| invalid_header(&format!("{field} length overflows")))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid_header(&format!("{field} is truncated")))?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self, field: &str) -> Result<u8, ServerError> {
        Ok(self.take(1, field)?[0])
    }

    fn fixed<const N: usize>(&mut self, field: &str) -> Result<[u8; N], ServerError> {
        self.take(N, field)?
            .try_into()
            .map_err(|_| invalid_header(&format!("{field} has invalid length")))
    }

    fn u16(&mut self, field: &str) -> Result<u16, ServerError> {
        Ok(u16::from_be_bytes(self.fixed(field)?))
    }

    fn u32(&mut self, field: &str) -> Result<u32, ServerError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &str) -> Result<u64, ServerError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn finish(self) -> Result<(), ServerError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid_header("trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use nokv_protocol::RootIdentity;
    use tempfile::tempdir;

    use super::*;

    fn root(byte: u8) -> RootId {
        RootId::from_bytes([byte; 16])
    }

    fn agent(byte: u8) -> AgentId {
        AgentId::from_bytes([byte; 16])
    }

    fn url(path: &Path) -> HoltMetadataUrl {
        let parsed = crate::MetadataUrl::from_str(&format!("holt://{}", path.display())).unwrap();
        parsed.as_holt().unwrap().clone()
    }

    #[test]
    fn format_is_create_only_and_reopens_the_exact_manifest() {
        let directory = tempdir().unwrap();
        let store = directory.path().join("metadata");
        let selected = url(&store);
        let created = format_holt(&selected, "0.11.0").unwrap();
        assert_eq!(created.state, HoltFormatState::Created);
        assert_eq!(created.manifest.provider(), StoreProvider::Holt);
        assert_eq!(
            created.manifest.workspace_format_version(),
            SUPPORTED_WORKSPACE_FORMAT_VERSION
        );

        let existing = format_holt(&selected, "9.99.0").unwrap();
        assert_eq!(existing.state, HoltFormatState::Existing);
        assert_eq!(existing.manifest, created.manifest);
        assert_eq!(existing.logical_shard_id, created.logical_shard_id);
    }

    #[test]
    fn unmarked_and_relocated_stores_fail_closed() {
        let directory = tempdir().unwrap();
        let unmarked = directory.path().join("unmarked");
        let holt = HoltStore::initialize(holt_options(&unmarked)).unwrap();
        let store: Arc<dyn TxnStore> = Arc::new(holt);
        meta::MetaShard::initialize(store, LogicalShardId::from_bytes([7; 16])).unwrap();
        assert!(format_holt(&url(&unmarked), "0.11.0").is_err());

        let original = directory.path().join("original");
        format_holt(&url(&original), "0.11.0").unwrap();
        let moved = directory.path().join("moved");
        std::fs::rename(&original, &moved).unwrap();
        assert!(format_holt(&url(&moved), "0.11.0").is_err());
    }

    #[test]
    fn provision_and_serve_reconcile_one_local_catalog_and_discovery() {
        let directory = tempdir().unwrap();
        let store = directory.path().join("metadata");
        let selected = url(&store);
        format_holt(&selected, "0.11.0").unwrap();
        let provisioned = provision_holt(&selected, root(1), agent(2)).unwrap();
        assert!(!provisioned.preexisting);
        assert_eq!(provisioned.root.state, CatalogEntryState::Ready);
        let replay = provision_holt(&selected, root(1), agent(2)).unwrap();
        assert!(replay.preexisting);
        assert_eq!(replay.root, provisioned.root);
        let before_rejection = {
            let (_, meta) = open_holt(&canonical_existing_path(&store).unwrap()).unwrap();
            meta.current_owner_epoch().unwrap()
        };
        assert!(provision_holt(&selected, root(1), agent(9)).is_err());
        let after_rejection = {
            let (_, meta) = open_holt(&canonical_existing_path(&store).unwrap()).unwrap();
            meta.current_owner_epoch().unwrap()
        };
        assert_eq!(after_rejection, before_rejection);

        let endpoint: SocketAddr = "127.0.0.1:17750".parse().unwrap();
        let serving = serve_holt(&selected, endpoint).unwrap();
        assert_eq!(serving.roots(), &[provisioned.root]);
        let route = serving
            .discovery
            .discover_route(RootIdentity::from(root(1)))
            .unwrap();
        assert_eq!(route.owner_endpoint.socket_addr(), endpoint);
        assert_eq!(route.owner_epoch, serving.owner_epoch().get());
        assert!(route.session_generation > 0);
        let server = serving
            .workspace_server(ServerOptions {
                bind: endpoint,
                handshake_timeout: std::time::Duration::from_secs(1),
                read_timeout: std::time::Duration::from_secs(1),
                write_timeout: std::time::Duration::from_secs(1),
                lease_renew_interval: std::time::Duration::from_secs(1),
                max_inflight_connections: 4,
            })
            .unwrap();
        assert_eq!(server.health().unwrap().installed_roots, 1);

        assert!(serve_holt(&selected, endpoint).is_err());
        drop(server);
        drop(serving);
        assert!(serve_holt(&selected, endpoint).is_ok());
    }

    #[test]
    fn corrupt_header_checksum_is_rejected_without_rewriting_it() {
        let directory = tempdir().unwrap();
        let store = directory.path().join("metadata");
        let selected = url(&store);
        format_holt(&selected, "0.11.0").unwrap();
        let marker = store.join(HEADER_FILE);
        let mut bytes = std::fs::read(&marker).unwrap();
        bytes[10] ^= 0x80;
        std::fs::write(&marker, &bytes).unwrap();
        assert!(format_holt(&selected, "0.11.0").is_err());
        assert_eq!(std::fs::read(marker).unwrap(), bytes);
    }
}
