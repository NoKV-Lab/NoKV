/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Custom CLI, MCP adapter, and shard-owner process for NoKV Agent workspaces.

mod backend;
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
        Command::Provision { logical_shard_id } => run_provision(&invocation, logical_shard_id),
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
    client
        .preflight(WORKBENCH_REQUIRED_RPC_CAPABILITIES)
        .map_err(|error| format!("workspace preflight failed: {error}"))?;
    let objects = Arc::new(CliObjectStore::build(&invocation.client.object)?);
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
  nokv --root-id HEX32 --etcd-endpoint URL provision <logical-shard-id-hex32>
  nokv [owner options] serve
  nokv schema

CLIENT ROUTING:
  --root-id HEX32
  --metadata-address HOST:PORT --logical-shard-id HEX32
  --etcd-endpoint URL [--etcd-endpoint URL ...]

AGENT PRESENTATION:
  --workbench-root /agents/AGENT_ID/wb is required by workbench, collect, and mcp
  keep this presentation root stable: it shapes responses and canonical v1 manifest paths
  RootId remains the only storage/routing identity; the presentation root never enters Holt keys

OWNER:
  provision creates the logical shard, installs immutable root affinity, and activates it
  --root-id HEX32 --etcd-endpoint URL --node-id ID
  --advertise-endpoint HOST:PORT --bind HOST:PORT
  --metadata-create PATH starts the first standalone local-WAL owner
  --metadata-reopen PATH is recovery admission only; standalone successors currently fail closed

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
fn run_provision(invocation: &Invocation, logical_shard_id: &str) -> Result<(), String> {
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
    let outcome = provision::provision_and_activate(control.as_ref(), root_id, logical_shard_id)
        .map_err(|error| error.to_string())?;
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
        "placement_generation": outcome.placement.placement_generation.get(),
        "lifecycle": lifecycle,
        "logical_shard_preexisting": outcome.logical_shard_preexisting,
        "placement_preexisting": outcome.placement_preexisting,
        "activation_required": outcome.activation_required,
    }))
}

#[cfg(not(feature = "etcd"))]
fn run_provision(_invocation: &Invocation, _logical_shard_id: &str) -> Result<(), String> {
    Err("provision requires the nokv etcd feature".to_owned())
}

#[cfg(feature = "etcd")]
fn run_server(invocation: &Invocation) -> Result<(), String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nokv_control::{NodeId, RecoveryPublication, RootId};
    use nokv_server::{
        bootstrap_root_owner, ArtifactLifecycleDeleter, LifecycleError, LifecycleObjectDeleter,
        LifecycleRunner, LifecycleRunnerOptions, MetadataStoreOpen, OwnerAdmission,
        RootOwnerBootstrapRequest, RootOwnerRegistry, ServerOptions, WorkspaceServer,
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
        cli::MetadataStoreConfig::Create(path) => MetadataStoreOpen::Create(path),
        cli::MetadataStoreConfig::Reopen(path) => MetadataStoreOpen::Reopen(path),
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
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "logical shard does not exist; provision it before serve".to_owned())?;
    let recovery = RecoveryPublication {
        checkpoint: shard.checkpoint.clone(),
        log: shard.log.clone(),
        durable_lsn: shard.durable_lsn,
    };
    let registry = Arc::new(RootOwnerRegistry::new());
    let owner = bootstrap_root_owner(
        Arc::clone(&control),
        Arc::clone(&registry),
        RootOwnerBootstrapRequest {
            root_id,
            metadata,
            admission: OwnerAdmission::Acquire {
                owner: node_id,
                endpoint,
                expected_previous_epoch: shard.owner_epoch,
            },
            install_request_id: request_id(b"install", root_id),
            activate_request_id: request_id(b"activate", root_id),
            recovery,
        },
    )
    .map_err(|error| error.to_string())?;

    let renew_seconds = u64::try_from(route.lease_ttl_seconds)
        .map_err(|_| "etcd lease TTL must be positive".to_owned())?
        .saturating_div(3)
        .max(1);
    let runtime = (|| -> Result<(WorkspaceServer, LifecycleRunner), String> {
        if invocation.server.lifecycle_interval_millis == 0
            || invocation.server.lifecycle_interval_millis > 60_000
        {
            return Err("--lifecycle-interval-millis must be within 1..=60000".to_owned());
        }
        let objects = Arc::new(CliObjectStore::build(&invocation.client.object)?);
        objects.validate_agent_capabilities()?;
        let server = WorkspaceServer::new(
            ServerOptions {
                bind: invocation.server.bind,
                read_timeout: Duration::from_secs(30),
                write_timeout: Duration::from_secs(30),
                lease_renew_interval: Duration::from_secs(renew_seconds),
            },
            Arc::clone(&registry),
            vec![owner.ownership.clone()],
        )
        .map_err(|error| error.to_string())?;
        let owner_loss = server.owner_loss_signal();
        let lifecycle_objects: Arc<dyn LifecycleObjectDeleter> =
            Arc::new(ArtifactLifecycleDeleter::new(objects));
        let lifecycle = LifecycleRunner::new(
            Arc::clone(&owner.store),
            Arc::clone(&registry),
            owner.route,
            owner_loss,
            lifecycle_objects,
            LifecycleRunnerOptions {
                poll_interval: Duration::from_millis(invocation.server.lifecycle_interval_millis),
                ..LifecycleRunnerOptions::default()
            },
        )
        .map_err(|error| error.to_string())?;
        Ok((server, lifecycle))
    })();
    let (server, lifecycle) = match runtime {
        Ok(runtime) => runtime,
        Err(primary) => {
            let release = owner.ownership.release();
            return Err(match release {
                Ok(_) => primary,
                Err(release) => format!("{primary}; owner release failed: {release}"),
            });
        }
    };
    let owner_loss = server.owner_loss_signal();
    let worker_owner_loss = owner_loss.clone();
    let lifecycle_worker = std::thread::Builder::new()
        .name("nokv-workspace-lifecycle".to_owned())
        .spawn(move || {
            let result = lifecycle.run_until_owner_loss();
            if result.is_err() {
                worker_owner_loss.fail_closed();
            }
            result
        });
    let lifecycle_worker = match lifecycle_worker {
        Ok(worker) => worker,
        Err(error) => {
            owner_loss.fail_closed();
            let primary = format!("cannot start lifecycle worker: {error}");
            let release = owner.ownership.release();
            return Err(match release {
                Ok(_) => primary,
                Err(release) => format!("{primary}; owner release failed: {release}"),
            });
        }
    };

    let server_result = server.run();
    owner_loss.fail_closed();
    let lifecycle_result = lifecycle_worker.join();
    let release_result = owner.ownership.release();

    let mut failures = Vec::new();
    match lifecycle_result {
        Ok(Err(error)) if !matches!(error, LifecycleError::OwnerLost(_)) => {
            failures.push(format!("lifecycle worker failed: {error}"));
        }
        Ok(_) => {}
        Err(_) => failures.push("lifecycle worker panicked".to_owned()),
    }
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
}
