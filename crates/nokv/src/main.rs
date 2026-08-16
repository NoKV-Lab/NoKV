/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Custom CLI, MCP adapter, and shard-owner process for NoKV Agent workspaces.

mod backend;
mod build_info;
mod cli;
mod connection;
mod object_store;
mod provision;
mod transfer;
mod workbench_mcp;

use std::io::{self, BufReader};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use nokv_agent::{
    execute_tool, tool_definitions, ReadRequest, ReadView, ScopedPath, SdkWorkbenchToolHandler,
    Section, WorkbenchBackend, WorkbenchToolHandler, WORKBENCH_CONTRACT_SCHEMA,
};
use nokv_types::{NormalizedRelativePath, WorkbenchId};
use serde_json::{json, Value};

use backend::CliWorkbenchBackend;
use cli::{Command, Invocation};
use object_store::CliObjectStore;

type CliHandler = SdkWorkbenchToolHandler<CliWorkbenchBackend>;

const WORKBENCH_REQUIRED_RPC_CAPABILITIES: [nokv_protocol::WorkspaceCapability; 8] = [
    nokv_protocol::WorkspaceCapability::ArtifactPublishV1,
    nokv_protocol::WorkspaceCapability::ArtifactRangeReadV1,
    nokv_protocol::WorkspaceCapability::CommitV1,
    nokv_protocol::WorkspaceCapability::QueryV1,
    nokv_protocol::WorkspaceCapability::RestoreV1,
    nokv_protocol::WorkspaceCapability::SnapshotLeaseV1,
    nokv_protocol::WorkspaceCapability::WorkspaceLifecycleV1,
    nokv_protocol::WorkspaceCapability::WorkspacePathV1,
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("nokv: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let invocation = cli::parse(std::env::args().skip(1)).map_err(|error| error.to_string())?;
    match &invocation.command {
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::Schema => print_schema(),
        Command::Version { json } => print_version(*json),
        Command::Provision {
            logical_shard_id,
            adopt_legacy_object_namespace,
        } => run_provision(
            &invocation,
            logical_shard_id,
            *adopt_legacy_object_namespace,
        ),
        Command::Serve => run_server(&invocation),
        Command::Workbench { tool, arguments } => {
            let handler = build_handler(&invocation)?;
            let arguments: Value = serde_json::from_str(arguments)
                .map_err(|error| format!("Workbench arguments are not valid JSON: {error}"))?;
            let result = execute_tool(&handler, tool, &arguments).map_err(agent_error)?;
            print_json(&result)
        }
        Command::Mcp => {
            let handler = build_handler(&invocation)?;
            let input = io::stdin();
            let output = io::stdout();
            workbench_mcp::serve(&handler, BufReader::new(input.lock()), output.lock())
                .map_err(|error| format!("MCP transport failed: {error}"))
        }
        Command::Materialize {
            workbench,
            section,
            path,
            destination,
        } => {
            let backend = build_backend(&invocation)?;
            materialize(&backend, workbench, section, path, destination)
        }
        Command::Collect {
            workbench,
            section,
            source,
            path,
            replace,
            content_type,
        } => {
            let handler = build_handler(&invocation)?;
            collect(
                &handler,
                invocation.client.max_artifact_bytes,
                workbench,
                section,
                source,
                path,
                *replace,
                content_type.as_deref(),
            )
        }
    }
}

fn build_backend(invocation: &Invocation) -> Result<CliWorkbenchBackend, String> {
    let client = connection::connect(&invocation.client).map_err(|error| error.to_string())?;
    let preflight = client
        .preflight(WORKBENCH_REQUIRED_RPC_CAPABILITIES)
        .map_err(|error| format!("workspace preflight failed: {error}"))?;
    let expected_namespace = preflight.value.route.object_namespace_id.into();
    let objects =
        Arc::new(CliObjectStore::build(&invocation.client.object)?.bind(expected_namespace)?);
    objects.validate_agent_capabilities()?;
    Ok(CliWorkbenchBackend::new(
        client,
        objects,
        invocation.client.max_artifact_bytes,
    ))
}

fn build_handler(invocation: &Invocation) -> Result<CliHandler, String> {
    let workbench_root = invocation
        .workbench_root
        .as_deref()
        .ok_or_else(|| "--workbench-root is required for Agent-facing commands".to_owned())?;
    SdkWorkbenchToolHandler::with_limits(
        build_backend(invocation)?,
        invocation.client.max_artifact_bytes,
        usize::try_from(invocation.client.max_attempts)
            .map_err(|_| "--max-attempts does not fit this platform".to_owned())?,
        workbench_root,
    )
    .map_err(agent_error)
}

fn materialize(
    backend: &CliWorkbenchBackend,
    workbench: &str,
    section: &str,
    path: &str,
    destination: &Path,
) -> Result<(), String> {
    let scoped = scoped_artifact(workbench, section, path)?;
    let artifact = backend
        .read(ReadRequest {
            path: scoped,
            view: ReadView::Live,
        })
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{section}/{path} does not exist"))?;
    let written = transfer::write_materialized_file(destination, &artifact.bytes)?;
    print_json(&json!({
        "status": "success",
        "destination": written,
        "size_bytes": artifact.bytes.len(),
        "generation": artifact.metadata.generation,
        "digest_uri": artifact.metadata.digest_uri,
    }))
}

#[allow(clippy::too_many_arguments)]
fn collect(
    handler: &impl WorkbenchToolHandler,
    max_bytes: usize,
    workbench: &str,
    section: &str,
    source: &Path,
    path: &str,
    replace: bool,
    content_type: Option<&str>,
) -> Result<(), String> {
    let bytes = transfer::read_collect_source(source, max_bytes)?;
    let mut arguments = json!({
        "id": workbench,
        "section": section,
        "path": path,
        "base64": STANDARD.encode(bytes),
        "replace": replace,
    });
    if let Some(content_type) = content_type {
        arguments["content_type"] = Value::String(content_type.to_owned());
    }
    let result = execute_tool(handler, "workbench_put_file", &arguments).map_err(agent_error)?;
    print_json(&result)
}

fn agent_error(error: nokv_agent::AgentError) -> String {
    serde_json::to_string(&error.as_value()).unwrap_or_else(|_| error.to_string())
}

fn scoped_artifact(workbench: &str, section: &str, path: &str) -> Result<ScopedPath, String> {
    Ok(ScopedPath {
        workbench_id: WorkbenchId::new(workbench).map_err(|error| error.to_string())?,
        section: Some(parse_section(section)?),
        relative_path: Some(NormalizedRelativePath::new(path).map_err(|error| error.to_string())?),
    })
}

fn parse_section(value: &str) -> Result<Section, String> {
    match value {
        "input" => Ok(Section::Input),
        "scripts" => Ok(Section::Scripts),
        "outputs" => Ok(Section::Outputs),
        "logs" => Ok(Section::Logs),
        "metadata" => Ok(Section::Metadata),
        _ => Err(format!("unknown Workbench section {value:?}")),
    }
}

fn print_schema() -> Result<(), String> {
    let tools = tool_definitions()
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            })
        })
        .collect::<Vec<_>>();
    print_json(&json!({
        "schema": WORKBENCH_CONTRACT_SCHEMA,
        "tools": tools,
    }))
}

fn print_version(json: bool) -> Result<(), String> {
    if json {
        print_json(&build_info::identity(
            WORKBENCH_CONTRACT_SCHEMA,
            tool_definitions().len(),
        ))
    } else {
        println!("nokv {}", build_info::VERSION);
        Ok(())
    }
}

fn print_json(value: &Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value)
            .map_err(|error| format!("cannot encode command result: {error}"))?
    );
    Ok(())
}

fn encode_lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn print_help() {
    println!(
        "\
NoKV Agent-workspace CLI

USAGE:
  nokv [connection/object options] workbench <tool> '<json arguments>'
  nokv [connection/object options] mcp
  nokv [connection/object options] materialize <workbench> <section> <path> <destination>
  nokv [connection/object options] collect <workbench> <section> <source> <path> [--replace] [--content-type TYPE]
  nokv --root-id HEX32 --etcd-endpoint URL provision <logical-shard-id-hex32> [--adopt-legacy-object-namespace]
  nokv [owner options] serve
  nokv schema
  nokv version [--json]
  nokv --version

CLIENT ROUTING:
  --root-id HEX32
  --metadata-address HOST:PORT --logical-shard-id HEX32 --object-namespace-id HEX32
    --placement-generation N --owner-epoch N
  static routing is a point-in-time pin and cannot refresh after placement changes or owner restarts
  use --etcd-endpoint for self-refreshing routing
  --etcd-endpoint URL [--etcd-endpoint URL ...]

AGENT PRESENTATION:
  --workbench-root /agents/AGENT_ID/wb is required by workbench, collect, and mcp
  keep this presentation root stable: it shapes responses and canonical v1 manifest paths
  RootId remains the only storage/routing identity; the presentation root never enters Holt keys

OWNER:
  provision creates the logical shard, immutable object-namespace binding, and root affinity
  --adopt-legacy-object-namespace is a one-time explicit migration after verifying bucket/prefix
  --root-id HEX32 --etcd-endpoint URL --node-id ID
  --advertise-endpoint HOST:PORT --bind HOST:PORT
  --metadata-create PATH starts the first standalone local-WAL owner
  --metadata-reopen PATH restarts the same exclusive local-WAL authority after lease loss

OBJECT DATA:
  --object-bucket NAME [--object-endpoint URL] [--object-root PREFIX]
  [--hot-cache-dir PATH --hot-cache-bytes BYTES]

NoKV exposes SDK, CLI, and MCP operations. It does not expose FUSE or POSIX."
    );
}

#[cfg(feature = "etcd")]
fn connect_control(
    route: &cli::EtcdRoutingConfig,
) -> Result<Arc<dyn nokv_control::ControlStore>, String> {
    let options = nokv_control::EtcdControlStoreOptions::new(route.endpoints.clone())
        .with_key_prefix(route.key_prefix.clone())
        .with_lease_ttl_seconds(route.lease_ttl_seconds);
    Ok(Arc::new(
        nokv_control::EtcdControlStore::connect(options).map_err(|error| error.to_string())?,
    ))
}

#[cfg(feature = "etcd")]
fn run_provision(
    invocation: &Invocation,
    logical_shard_id: &str,
    adopt_legacy_object_namespace: bool,
) -> Result<(), String> {
    use nokv_control::{LogicalShardId, RootId};
    use nokv_types::RootPlacementLifecycle;

    let cli::RoutingConfig::Etcd(route) = &invocation.client.routing else {
        return Err("provision requires control-backed etcd routing".to_owned());
    };
    let root_id = RootId::from(
        connection::configured_root_id(&invocation.client).map_err(|error| error.to_string())?,
    );
    let logical_shard_id = LogicalShardId::from(
        connection::parse_logical_shard_id(logical_shard_id).map_err(|error| error.to_string())?,
    );
    let control = connect_control(route)?;
    let existing_placement =
        provision::preflight_provision(control.as_ref(), root_id, logical_shard_id)
            .map_err(|error| error.to_string())?;
    let objects = CliObjectStore::build(&invocation.client.object)?;
    objects.validate_agent_capabilities()?;
    let namespace_id = match control
        .get_root_object_namespace_binding(&root_id)
        .map_err(|error| error.to_string())?
    {
        Some(binding) => {
            objects.clone().bind(binding.object_namespace_id)?;
            binding.object_namespace_id
        }
        None => {
            if existing_placement.is_some() && !adopt_legacy_object_namespace {
                return Err(
                    "legacy root has no durable object namespace binding; verify the configured bucket/prefix, then rerun provision with --adopt-legacy-object-namespace"
                        .to_owned(),
                );
            }
            match objects.load_namespace()? {
                Some(existing) => existing,
                None => {
                    let created = provision::new_object_namespace_id(root_id);
                    objects.ensure_namespace(created)?;
                    created
                }
            }
        }
    };
    let outcome = provision::provision_and_activate(
        control.as_ref(),
        root_id,
        logical_shard_id,
        namespace_id,
    )
    .map_err(|error| error.to_string())?;
    objects.bind(namespace_id)?;
    let lifecycle = match outcome.placement.lifecycle {
        RootPlacementLifecycle::Provisioning => "provisioning",
        RootPlacementLifecycle::Active => "active",
        RootPlacementLifecycle::Draining => "draining",
        RootPlacementLifecycle::Retired => "retired",
    };
    print_json(&json!({
        "status": "success",
        "root_id": encode_lowercase_hex(root_id.as_bytes()),
        "logical_shard_id": encode_lowercase_hex(logical_shard_id.as_bytes()),
        "object_namespace_id": encode_lowercase_hex(namespace_id.as_bytes()),
        "placement_generation": outcome.placement.placement_generation.get(),
        "lifecycle": lifecycle,
        "logical_shard_preexisting": outcome.logical_shard_preexisting,
        "object_namespace_preexisting": outcome.object_namespace_preexisting,
        "placement_preexisting": outcome.placement_preexisting,
        "activation_required": outcome.activation_required,
    }))
}

#[cfg(not(feature = "etcd"))]
fn run_provision(
    _invocation: &Invocation,
    _logical_shard_id: &str,
    _adopt_legacy_object_namespace: bool,
) -> Result<(), String> {
    Err("provision requires the nokv etcd feature".to_owned())
}

#[cfg(feature = "etcd")]
fn active_shard_placements(
    control: &dyn nokv_control::ControlStore,
    seed: &nokv_control::RootPlacement,
) -> Result<Vec<nokv_control::RootPlacement>, String> {
    let mut placements = control
        .list_root_placements()
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|placement| {
            placement.logical_shard_id == seed.logical_shard_id
                && placement.lifecycle == nokv_control::RootPlacementLifecycle::Active
        })
        .collect::<Vec<_>>();
    placements.sort_by_key(|placement| placement.root_id);
    if !placements
        .iter()
        .any(|placement| placement.root_id == seed.root_id)
    {
        return Err("configured root placement must be Active before serve".to_owned());
    }
    Ok(placements)
}

#[cfg(feature = "etcd")]
fn join_lifecycle_workers(
    workers: Vec<std::thread::JoinHandle<Result<(), nokv_server::LifecycleError>>>,
    failures: &mut Vec<String>,
) {
    for worker in workers {
        match worker.join() {
            Ok(Err(error)) if !matches!(error, nokv_server::LifecycleError::OwnerLost(_)) => {
                failures.push(format!("lifecycle worker failed: {error}"));
            }
            Ok(_) => {}
            Err(_) => failures.push("lifecycle worker panicked".to_owned()),
        }
    }
}

#[cfg(feature = "etcd")]
fn run_server(invocation: &Invocation) -> Result<(), String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nokv_control::{NodeId, RecoveryPublication, RootId};
    use nokv_server::{
        bootstrap_shard, ArtifactLifecycleDeleter, LeaseMode, LifecycleObjectDeleter,
        LifecycleRunner, LifecycleRunnerOptions, OpenMode, RootAttach, RootOwnerRegistry,
        ServerOptions, ShardBoot, WorkspaceServer,
    };
    use nokv_types::RequestId;
    use sha2::{Digest, Sha256};

    static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn request_id(domain: &[u8], root: RootId) -> RequestId {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.owner.bootstrap\0");
        hasher.update(domain);
        hasher.update(root.as_bytes());
        hasher.update(std::process::id().to_be_bytes());
        hasher.update(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_be_bytes(),
        );
        hasher.update(
            REQUEST_SEQUENCE
                .fetch_add(1, Ordering::Relaxed)
                .to_be_bytes(),
        );
        let digest: [u8; 32] = hasher.finalize().into();
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        RequestId::from_bytes(id)
    }

    let cli::RoutingConfig::Etcd(route) = &invocation.client.routing else {
        return Err("serve requires control-backed etcd routing".to_owned());
    };
    let metadata = match invocation
        .server
        .metadata_store
        .clone()
        .ok_or_else(|| "serve requires --metadata-create or --metadata-reopen".to_owned())?
    {
        cli::MetadataStoreConfig::Create(path) => OpenMode::New(path),
        cli::MetadataStoreConfig::Reopen(path) => OpenMode::Existing(path),
    };
    let node_id = NodeId::new(
        invocation
            .server
            .node_id
            .clone()
            .ok_or_else(|| "serve requires --node-id".to_owned())?,
    )
    .map_err(|error| format!("invalid --node-id: {error:?}"))?;
    let endpoint = invocation
        .server
        .advertise_endpoint
        .clone()
        .ok_or_else(|| "serve requires --advertise-endpoint".to_owned())?;
    let root_id = RootId::from(
        connection::configured_root_id(&invocation.client).map_err(|error| error.to_string())?,
    );
    let control = connect_control(route)?;
    let placement = control
        .get_root_placement(&root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "root placement does not exist; provision it before serve".to_owned())?;
    let object_namespace = control
        .get_root_object_namespace_binding(&root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            "root object namespace is not bound; run provision after verifying the configured bucket/prefix"
                .to_owned()
        })?;
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "logical shard does not exist; provision it before serve".to_owned())?;
    let recovery = RecoveryPublication {
        checkpoint: shard.checkpoint.clone(),
        log: shard.log.clone(),
        durable_lsn: shard.durable_lsn,
    };
    let placements = active_shard_placements(control.as_ref(), &placement)?;
    if invocation.server.lifecycle_interval_millis == 0
        || invocation.server.lifecycle_interval_millis > 60_000
    {
        return Err("--lifecycle-interval-millis must be within 1..=60000".to_owned());
    }
    let expected_namespace = object_namespace.object_namespace_id;
    let namespace_bindings = placements
        .iter()
        .map(|placement| {
            control
                .get_root_object_namespace_binding(&placement.root_id)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    "every root on a served shard must have an object namespace binding".to_owned()
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if namespace_bindings
        .iter()
        .any(|binding| binding.object_namespace_id != expected_namespace)
    {
        return Err(
            "one shard owner cannot serve roots bound to different object namespaces".to_owned(),
        );
    }
    let objects =
        Arc::new(CliObjectStore::build(&invocation.client.object)?.bind(expected_namespace)?);
    objects.validate_agent_capabilities()?;
    let registry = Arc::new(RootOwnerRegistry::new());
    let owner = bootstrap_shard(
        Arc::clone(&control),
        Arc::clone(&registry),
        ShardBoot {
            shard_id: placement.logical_shard_id,
            open: metadata,
            lease: LeaseMode::Acquire {
                owner: node_id,
                endpoint,
                previous_epoch: shard.owner_epoch,
            },
            recovery,
            roots: placements
                .iter()
                .map(|placement| RootAttach {
                    root_id: placement.root_id,
                    object_namespace_id: expected_namespace,
                    install_id: request_id(b"install", placement.root_id),
                    bind_object_namespace_id: request_id(
                        b"bind-object-namespace",
                        placement.root_id,
                    ),
                    activate_id: request_id(b"activate", placement.root_id),
                })
                .collect(),
        },
    )
    .map_err(|error| error.to_string())?;

    let renew_seconds = u64::try_from(route.lease_ttl_seconds)
        .map_err(|_| "etcd lease TTL must be positive".to_owned())?
        .saturating_div(3)
        .max(1);
    let meta = Arc::clone(owner.meta());
    let owner_routes = owner.routes().to_vec();
    let server = WorkspaceServer::new(
        ServerOptions {
            bind: invocation.server.bind,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(renew_seconds),
        },
        Arc::clone(&registry),
        vec![owner],
    )
    .map_err(|error| error.to_string())?;
    let owner_loss = server.owner_loss_signal();
    let lifecycle_objects: Arc<dyn LifecycleObjectDeleter> =
        Arc::new(ArtifactLifecycleDeleter::new(objects));
    let mut lifecycles = Vec::with_capacity(owner_routes.len());
    for route in owner_routes {
        match LifecycleRunner::new(
            Arc::clone(&meta),
            Arc::clone(&registry),
            route,
            owner_loss.clone(),
            Arc::clone(&lifecycle_objects),
            LifecycleRunnerOptions {
                poll_interval: Duration::from_millis(invocation.server.lifecycle_interval_millis),
                ..LifecycleRunnerOptions::default()
            },
        ) {
            Ok(lifecycle) => lifecycles.push(lifecycle),
            Err(error) => {
                let primary = error.to_string();
                let release = server.release_ownership();
                return Err(match release {
                    Ok(()) => primary,
                    Err(release) => format!("{primary}; owner release failed: {release}"),
                });
            }
        }
    }
    let mut lifecycle_workers = Vec::with_capacity(lifecycles.len());
    for (index, lifecycle) in lifecycles.into_iter().enumerate() {
        let worker_owner_loss = owner_loss.clone();
        let worker = std::thread::Builder::new()
            .name(format!("nokv-workspace-lifecycle-{index}"))
            .spawn(move || {
                let result = lifecycle.run_until_owner_loss();
                if result.is_err() {
                    worker_owner_loss.fail_closed();
                }
                result
            });
        match worker {
            Ok(worker) => lifecycle_workers.push(worker),
            Err(error) => {
                owner_loss.fail_closed();
                let mut failures = vec![format!("cannot start lifecycle worker: {error}")];
                join_lifecycle_workers(lifecycle_workers, &mut failures);
                if let Err(error) = server.release_ownership() {
                    failures.push(format!("owner release failed: {error}"));
                }
                return Err(failures.join("; "));
            }
        }
    }

    let server_result = server.run();
    owner_loss.fail_closed();

    let mut failures = Vec::new();
    join_lifecycle_workers(lifecycle_workers, &mut failures);
    let release_result = server.release_ownership();
    if let Err(error) = server_result {
        failures.push(format!("RPC server stopped: {error}"));
    }
    if let Err(error) = release_result {
        failures.push(format!("owner release failed: {error}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

#[cfg(not(feature = "etcd"))]
fn run_server(_invocation: &Invocation) -> Result<(), String> {
    Err("serve requires the nokv etcd feature".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_artifact_accepts_only_the_five_virtual_sections() {
        let path = scoped_artifact("run-1", "outputs", "result.json").unwrap();
        assert_eq!(path.logical_path(), "outputs/result.json");
        assert!(scoped_artifact("run-1", "tmp", "result.json").is_err());
        assert!(scoped_artifact("run-1", "outputs", "../result.json").is_err());
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn shard_startup_selects_every_active_root_and_no_other_placement() {
        use nokv_control::{ControlStore, InMemoryControlStore, RootPlacement};
        use nokv_types::{LogicalShardId, PlacementGeneration, RootId, RootPlacementLifecycle};

        let root = |fill| RootId::from_bytes([fill; nokv_types::FIXED_ID_BYTES]);
        let shard = |fill| LogicalShardId::from_bytes([fill; nokv_types::FIXED_ID_BYTES]);
        let control = InMemoryControlStore::new();
        let namespace = nokv_types::ObjectNamespaceId::from_bytes([10; 16]);
        provision::provision_and_activate(&control, root(1), shard(9), namespace).unwrap();
        provision::provision_and_activate(&control, root(2), shard(9), namespace).unwrap();
        provision::provision_and_activate(&control, root(3), shard(8), namespace).unwrap();
        control
            .create_root_placement(RootPlacement {
                root_id: root(4),
                logical_shard_id: shard(9),
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();

        let seed = control.get_root_placement(&root(1)).unwrap().unwrap();
        let placements = active_shard_placements(&control, &seed).unwrap();
        assert_eq!(
            placements
                .iter()
                .map(|placement| placement.root_id)
                .collect::<Vec<_>>(),
            vec![root(1), root(2)]
        );

        let provisioning = control.get_root_placement(&root(4)).unwrap().unwrap();
        assert!(active_shard_placements(&control, &provisioning).is_err());
    }
}
