/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared harness for the client integration gates: a real in-process
//! server (in-memory control plane, bootstrap, framed RPC) plus the real
//! SDK client against a namespace-bound memory object store.

// Each integration-test binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nokv_client::{
    ArtifactAppendOptions, ArtifactPublishOptions, ClientOptions, FramedTcpOptions,
    FramedTcpTransport, StaticRouteResolver, WorkspaceClient,
};
use nokv_control::{
    ControlStore, InMemoryControlStore, NodeId, PlacementGeneration, RecoveryPublication,
    RootObjectNamespaceBinding, RootPlacement, RootPlacementLifecycle,
};
use nokv_object::{
    ensure_object_namespace, ArtifactObjectStore, BoundArtifactStore, MemoryArtifactStore,
};
use nokv_protocol::{
    ArtifactRevisionIdentity, ContentType, CreateWorkspaceRequest, LogicalShardIdentity,
    ObjectNamespaceIdentity, OperationIdentity, PublishCondition, RelativePath, RootIdentity,
    RootRoute, WorkbenchName, WorkspaceIdentity, WorkspacePath, WorkspaceReadView,
};
use nokv_server::{
    bootstrap_shard, LeaseMode, OpenMode, RecoveryPublicationMode, RootAttach, RootOwnerRegistry,
    ServerOptions, ShardBoot, WorkspaceServer,
};
use nokv_types::{LogicalShardId, ObjectNamespaceId, RequestId, RootId};

pub const SHARD_BYTES: [u8; 16] = [0x21; 16];
pub const ROOT_BYTES: [u8; 16] = [0x42; 16];
pub const NAMESPACE_BYTES: [u8; 16] = [10; 16];

pub type Store = BoundArtifactStore<MemoryArtifactStore>;
pub type Client = WorkspaceClient<FramedTcpTransport, StaticRouteResolver>;

pub struct Harness<S: ArtifactObjectStore + Clone = MemoryArtifactStore> {
    pub _control: Arc<dyn ControlStore>,
    pub bind: SocketAddr,
    pub client: Client,
    pub store: BoundArtifactStore<S>,
    pub workbench: WorkbenchName,
}

pub fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes([seed; 16])
}

pub fn spawn_server(roots: &[RootId]) -> (SocketAddr, Arc<dyn ControlStore>) {
    let shard = LogicalShardId::from_bytes(SHARD_BYTES);
    let concrete = Arc::new(InMemoryControlStore::new());
    let control: Arc<dyn ControlStore> = concrete;
    control.create_logical_shard(shard).unwrap();
    for root_id in roots {
        control
            .create_root_object_namespace_binding(RootObjectNamespaceBinding {
                root_id: *root_id,
                object_namespace_id: ObjectNamespaceId::from_bytes(NAMESPACE_BYTES),
            })
            .unwrap();
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id: *root_id,
                logical_shard_id: shard,
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
    let registry = Arc::new(RootOwnerRegistry::new());
    let temporary = tempfile::TempDir::new().unwrap();
    let boot = ShardBoot {
        shard_id: shard,
        open: OpenMode::New(temporary.path().join("meta")),
        lease: LeaseMode::Acquire {
            owner: NodeId::new("node-a").unwrap(),
            endpoint: "127.0.0.1:9010".to_owned(),
            previous_epoch: None,
        },
        recovery: RecoveryPublication {
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        },
        recovery_publication: RecoveryPublicationMode::LocalOnly,
        roots: roots
            .iter()
            .enumerate()
            .map(|(index, root_id)| RootAttach {
                root_id: *root_id,
                object_namespace_id: ObjectNamespaceId::from_bytes(NAMESPACE_BYTES),
                install_id: request_id(3 + (index as u8) * 2),
                bind_object_namespace_id: request_id(4 + (index as u8) * 2),
                activate_id: request_id(5 + (index as u8) * 2),
            })
            .collect(),
    };
    let recovery_objects: Arc<dyn ArtifactObjectStore> = Arc::new(bound_store());
    let owner = bootstrap_shard(
        Arc::clone(&control),
        Arc::clone(&registry),
        recovery_objects,
        boot,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    let server = WorkspaceServer::new(
        ServerOptions {
            bind,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            max_inflight_connections: 32,
        },
        registry,
        vec![owner],
    )
    .unwrap();
    thread::spawn(move || {
        let _temporary = temporary;
        let _ = server.serve(listener);
    });
    (bind, control)
}

pub fn connect(bind: SocketAddr) -> Client {
    let route = RootRoute {
        root_id: RootIdentity(ROOT_BYTES),
        logical_shard_id: LogicalShardIdentity(SHARD_BYTES),
        object_namespace_id: ObjectNamespaceIdentity(NAMESPACE_BYTES),
        placement_generation: 2,
        owner_epoch: 1,
    };
    let resolver = StaticRouteResolver::new(route, bind).unwrap();
    WorkspaceClient::new(
        RootIdentity(ROOT_BYTES),
        FramedTcpTransport::new(FramedTcpOptions::default()).unwrap(),
        resolver,
        ClientOptions::default(),
    )
    .unwrap()
}

pub fn bound_store() -> Store {
    let inner = MemoryArtifactStore::new();
    let namespace_id = ObjectNamespaceId::from_bytes(NAMESPACE_BYTES);
    ensure_object_namespace(&inner, namespace_id).unwrap();
    BoundArtifactStore::open(inner, namespace_id).unwrap()
}

pub fn harness(workbench_name: &str) -> Harness {
    let root = RootId::from_bytes(ROOT_BYTES);
    let (bind, control) = spawn_server(&[root]);
    let client = connect(bind);
    let store = bound_store();
    let workbench = WorkbenchName::new(workbench_name).unwrap();
    client
        .create_workspace(
            client.new_request_id(),
            CreateWorkspaceRequest {
                workbench: workbench.clone(),
                workspace_incarnation_id: WorkspaceIdentity([1; 16]),
            },
        )
        .unwrap();
    Harness {
        _control: control,
        bind,
        client,
        store,
        workbench,
    }
}

pub fn target(workbench: &WorkbenchName, path: &str) -> WorkspacePath {
    WorkspacePath {
        workbench: workbench.clone(),
        path: RelativePath::new(path).unwrap(),
    }
}

pub fn read_all<S: ArtifactObjectStore>(
    client: &Client,
    store: &BoundArtifactStore<S>,
    workbench: &WorkbenchName,
    path: &str,
) -> Vec<u8> {
    client
        .read_artifact(
            store,
            None,
            target(workbench, path),
            WorkspaceReadView::Live,
        )
        .unwrap()
        .bytes
}

pub fn publish_base<S: ArtifactObjectStore>(
    client: &Client,
    store: &BoundArtifactStore<S>,
    workbench: &WorkbenchName,
    path: &str,
    bytes: &[u8],
    seed: u8,
) -> u64 {
    let options = ArtifactPublishOptions::new(
        OperationIdentity([seed; 16]),
        ArtifactRevisionIdentity([seed + 1; 16]),
        target(workbench, path),
        PublishCondition::CreateOnly,
        ContentType::new("text/plain").unwrap(),
    )
    .with_block_size(2);
    let outcome = client.publish_artifact(store, options, bytes).unwrap();
    outcome.publication.value.generation
}

pub fn append<S: ArtifactObjectStore>(
    client: &Client,
    store: &BoundArtifactStore<S>,
    workbench: &WorkbenchName,
    path: &str,
    delta: &[u8],
    seed: u8,
) -> (bool, u64, u64) {
    let options = ArtifactAppendOptions::new(
        OperationIdentity([seed; 16]),
        ArtifactRevisionIdentity([seed + 1; 16]),
        target(workbench, path),
        ContentType::new("text/plain").unwrap(),
    )
    .with_block_size(2);
    let outcome = client.append_artifact(store, options, delta).unwrap();
    (
        outcome.created,
        outcome.publication.value.generation,
        outcome.publication.value.logical_size,
    )
}
