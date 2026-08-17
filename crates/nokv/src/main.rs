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
#[cfg(feature = "etcd")]
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use nokv_agent::{
    execute_tool, tool_definitions, ReadRequest, ReadView, ScopedPath, SdkGenericAgentToolHandler,
    SdkWorkbenchToolHandler, Section, WorkbenchBackend, WorkbenchToolHandler,
    WORKBENCH_CONTRACT_SCHEMA,
};
use nokv_types::{NormalizedRelativePath, WorkbenchId};
use serde_json::{json, Value};

use backend::CliWorkbenchBackend;
use cli::{Command, Invocation, McpProfile, WorkspacePathCommand};
#[cfg(feature = "etcd")]
use object_store::CliObjectStore;

type CliHandler = SdkWorkbenchToolHandler<CliWorkbenchBackend>;
type CliGenericAgentHandler = SdkGenericAgentToolHandler<CliWorkbenchBackend>;

#[cfg(feature = "etcd")]
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
            adopt_legacy_agent_binding,
        } => run_provision(
            &invocation,
            logical_shard_id,
            *adopt_legacy_object_namespace,
            *adopt_legacy_agent_binding,
        ),
        Command::Serve => run_server(&invocation),
        Command::Workbench { tool, arguments } => {
            let handler = build_handler(&invocation)?;
            let arguments: Value = serde_json::from_str(arguments)
                .map_err(|error| format!("Workbench arguments are not valid JSON: {error}"))?;
            let result = execute_tool(&handler, tool, &arguments).map_err(agent_error)?;
            print_json(&result)
        }
        Command::Mcp { profile } => {
            let input = io::stdin();
            let output = io::stdout();
            match profile {
                McpProfile::Workbench => {
                    let handler = build_handler(&invocation)?;
                    workbench_mcp::serve(&handler, BufReader::new(input.lock()), output.lock())
                }
                McpProfile::Agent => {
                    let handler = build_generic_agent_handler(&invocation)?;
                    workbench_mcp::serve_agent(
                        &handler,
                        BufReader::new(input.lock()),
                        output.lock(),
                    )
                }
            }
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
        Command::WorkspacePath(command) => run_workspace_path(&invocation, command),
    }
}

#[derive(Debug)]
struct ConfiguredAgentAdmission<'a> {
    route: &'a cli::EtcdRoutingConfig,
    root_id: nokv_control::RootId,
    agent_id: nokv_types::AgentId,
}

fn configured_agent_admission(
    invocation: &Invocation,
) -> Result<ConfiguredAgentAdmission<'_>, String> {
    let cli::RoutingConfig::Etcd(route) = &invocation.client.routing else {
        return Err(
            "Agent-facing commands require durable etcd control routing; static routes cannot prove the RootId-to-AgentId binding"
                .to_owned(),
        );
    };
    let root_id = nokv_control::RootId::from(
        connection::configured_root_id(&invocation.client).map_err(|error| error.to_string())?,
    );
    let agent_id = connection::parse_agent_id(
        invocation
            .agent_id
            .as_deref()
            .ok_or_else(|| "--agent-id is required for Agent-facing commands".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(ConfiguredAgentAdmission {
        route,
        root_id,
        agent_id,
    })
}

fn load_root_agent_binding(
    control: &dyn nokv_control::ControlStore,
    root_id: nokv_control::RootId,
) -> Result<nokv_control::RootAgentBinding, String> {
    let binding = control
        .get_root_agent_binding(&root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "root {root_id:?} has no durable Agent binding; run provision with its stable --agent-id, adding --adopt-legacy-agent-binding only for a verified legacy root"
            )
        })?;
    if binding.root_id != root_id {
        return Err(format!(
            "root {root_id:?} Agent binding key/value identity mismatch"
        ));
    }
    Ok(binding)
}

fn after_agent_root_admission<T>(
    control: &dyn nokv_control::ControlStore,
    root_id: nokv_control::RootId,
    agent_id: nokv_types::AgentId,
    operation: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    let binding = load_root_agent_binding(control, root_id)?;
    if binding.agent_id != agent_id {
        return Err(nokv_control::ControlError::RootAgentAlreadyBound { root_id }.to_string());
    }
    operation()
}

fn build_backend(invocation: &Invocation) -> Result<CliWorkbenchBackend, String> {
    let admission = configured_agent_admission(invocation)?;
    #[cfg(feature = "etcd")]
    {
        let control = connect_control(admission.route)?;
        after_agent_root_admission(
            control.as_ref(),
            admission.root_id,
            admission.agent_id,
            || build_backend_after_agent_admission(invocation),
        )
    }
    #[cfg(not(feature = "etcd"))]
    {
        let ConfiguredAgentAdmission {
            route,
            root_id,
            agent_id,
        } = admission;
        let _ = (route, root_id, agent_id);
        Err("Agent-facing commands require the nokv etcd feature".to_owned())
    }
}

#[cfg(feature = "etcd")]
fn build_backend_after_agent_admission(
    invocation: &Invocation,
) -> Result<CliWorkbenchBackend, String> {
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

fn build_generic_agent_handler(invocation: &Invocation) -> Result<CliGenericAgentHandler, String> {
    let workbench_root = invocation
        .workbench_root
        .as_deref()
        .ok_or_else(|| "--workbench-root is required for Agent-facing commands".to_owned())?;
    SdkGenericAgentToolHandler::with_max_bytes(
        build_backend(invocation)?,
        invocation.client.max_artifact_bytes,
        workbench_root,
    )
    .map_err(agent_error)
}

fn build_workspace_path_client(
    invocation: &Invocation,
) -> Result<connection::CliWorkspaceClient, String> {
    let admission = configured_agent_admission(invocation)?;
    #[cfg(feature = "etcd")]
    {
        let control = connect_control(admission.route)?;
        after_agent_root_admission(
            control.as_ref(),
            admission.root_id,
            admission.agent_id,
            || {
                let client =
                    connection::connect(&invocation.client).map_err(|error| error.to_string())?;
                client
                    .preflight([nokv_protocol::WorkspaceCapability::WorkspacePathV1])
                    .map_err(|error| format!("workspace preflight failed: {error}"))?;
                Ok(client)
            },
        )
    }
    #[cfg(not(feature = "etcd"))]
    {
        let ConfiguredAgentAdmission {
            route,
            root_id,
            agent_id,
        } = admission;
        let _ = (route, root_id, agent_id);
        Err("workspace-path commands require the nokv etcd feature".to_owned())
    }
}

fn run_workspace_path(
    invocation: &Invocation,
    command: &WorkspacePathCommand,
) -> Result<(), String> {
    let (request_id, operation) = match command {
        WorkspacePathCommand::Rename {
            workbench,
            section,
            source,
            destination,
            expected_generation,
            request_id,
        } => (
            nokv_protocol::RequestIdentity(*request_id),
            nokv_protocol::WorkspaceRequest::RenamePath(nokv_protocol::RenamePathRequest {
                source: canonical_workspace_path(workbench, section, source)?,
                destination: canonical_workspace_path(workbench, section, destination)?,
                expected_generation: *expected_generation,
            }),
        ),
        WorkspacePathCommand::Remove {
            workbench,
            section,
            path,
            expected_generation,
            request_id,
        } => (
            nokv_protocol::RequestIdentity(*request_id),
            nokv_protocol::WorkspaceRequest::RemovePath(nokv_protocol::RemovePathRequest {
                target: canonical_workspace_path(workbench, section, path)?,
                expected_generation: *expected_generation,
            }),
        ),
    };
    let client = build_workspace_path_client(invocation)?;
    match operation {
        nokv_protocol::WorkspaceRequest::RenamePath(request) => {
            let call = client
                .rename_path(request_id, request.clone())
                .map_err(|error| error.to_string())?;
            print_json(&rename_path_json(request_id, &request, &call)?)
        }
        nokv_protocol::WorkspaceRequest::RemovePath(request) => {
            let call = client
                .remove_path(request_id, request.clone())
                .map_err(|error| error.to_string())?;
            print_json(&remove_path_json(request_id, &request, &call)?)
        }
        _ => unreachable!("workspace-path CLI constructs only path mutations"),
    }
}

fn canonical_workspace_path(
    workbench: &str,
    section: &str,
    path: &str,
) -> Result<nokv_protocol::WorkspacePath, String> {
    let section = parse_section(section)?;
    let relative = NormalizedRelativePath::new(path).map_err(|error| error.to_string())?;
    if relative.components().next() == Some(section.as_str()) {
        return Err(format!(
            "path is relative to section {section}; remove the duplicated section prefix"
        ));
    }
    let canonical = NormalizedRelativePath::new(format!("{section}/{relative}"))
        .map_err(|error| error.to_string())?;
    if matches!(
        canonical.as_str(),
        "metadata/run_manifest.json" | "metadata/restore_manifest.json"
    ) {
        return Err(format!(
            "{} is a reserved Workbench projection and cannot be changed by workspace-path",
            canonical.as_str()
        ));
    }
    Ok(nokv_protocol::WorkspacePath {
        workbench: nokv_protocol::WorkbenchName::new(workbench)
            .map_err(|error| error.to_string())?,
        path: nokv_protocol::RelativePath::new(canonical.as_str())
            .map_err(|error| error.to_string())?,
    })
}

fn rename_path_json(
    request_id: nokv_protocol::RequestIdentity,
    request: &nokv_protocol::RenamePathRequest,
    call: &nokv_client::ClientCall<nokv_protocol::RenamePathResult>,
) -> Result<Value, String> {
    let commit_version = call
        .commit_version
        .ok_or_else(|| "rename response omitted its metadata commit version".to_owned())?;
    Ok(json!({
        "status": "success",
        "operation": "rename",
        "request_id": encode_lowercase_hex(&request_id.0),
        "workbench_id": call.value.source.workbench.as_str(),
        "path": call.value.source.path.as_str(),
        "destination_path": call.value.destination.path.as_str(),
        "previous_generation": request.expected_generation,
        "generation": call.value.generation,
        "idempotent_replay": call.replayed,
        "workspace_revision": call.value.workspace_revision,
        "artifact_revision_id": encode_lowercase_hex(&call.value.artifact_revision_id.0),
        "commit_version": commit_version,
    }))
}

fn remove_path_json(
    request_id: nokv_protocol::RequestIdentity,
    request: &nokv_protocol::RemovePathRequest,
    call: &nokv_client::ClientCall<nokv_protocol::RemovePathResult>,
) -> Result<Value, String> {
    let commit_version = call
        .commit_version
        .ok_or_else(|| "remove response omitted its metadata commit version".to_owned())?;
    let revision = call
        .value
        .removed_artifact_revision_id
        .ok_or_else(|| "successful remove response omitted its artifact revision".to_owned())?;
    Ok(json!({
        "status": "success",
        "operation": "remove",
        "request_id": encode_lowercase_hex(&request_id.0),
        "workbench_id": request.target.workbench.as_str(),
        "path": request.target.path.as_str(),
        "previous_generation": request.expected_generation,
        "generation": request.expected_generation,
        "idempotent_replay": call.replayed,
        "workspace_revision": call.value.workspace_revision,
        "removed_artifact_revision_id": encode_lowercase_hex(&revision.0),
        "commit_version": commit_version,
    }))
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
  nokv [connection/object options] mcp [--profile workbench|agent]
  nokv [connection/object options] materialize <workbench> <section> <path> <destination>
  nokv [connection/object options] collect <workbench> <section> <source> <path> [--replace] [--content-type TYPE]
  nokv [route/agent options] workspace-path rename <workbench> <section> <source> <destination> --expected-generation N --request-id HEX32
  nokv [route/agent options] workspace-path remove <workbench> <section> <path> --expected-generation N --request-id HEX32
  nokv --root-id HEX32 --agent-id HEX32 --etcd-endpoint URL provision <logical-shard-id-hex32> [adoption options]
  nokv [owner options] serve
  nokv schema
  nokv version [--json]
  nokv --version

AGENT CONTROL ROUTING:
  --root-id HEX32
  --agent-id HEX32 is required by provision, workbench, mcp, materialize, collect, and workspace-path
  AgentId is a durable deployment identity used to prevent root misconfiguration; it is not an authentication credential
  --etcd-endpoint URL [--etcd-endpoint URL ...]

AGENT PRESENTATION:
  --workbench-root /agents/AGENT_NAME/wb is required by workbench, collect, and mcp
  keep this presentation root stable: it shapes responses and canonical v1 manifest paths
  RootId remains the only storage/routing identity; the presentation root never enters Holt keys
  mcp defaults to the 18-tool Workbench profile; --profile agent selects the seven path tools
  workspace-path is a custom CLI surface; it does not add to the fixed 18 Workbench tools
  workspace-path requires an explicit lowercase HEX32 request id for exact cross-process replay

OWNER:
  provision first binds RootId to an explicit AgentId, then creates namespace, shard, and affinity
  --adopt-legacy-agent-binding is a one-time explicit migration for a verified legacy root
  --adopt-legacy-object-namespace is a one-time explicit migration after verifying bucket/prefix
  serve validates every active root's Agent binding; it does not require or compare one shard-wide AgentId
  --root-id HEX32 --etcd-endpoint URL --node-id ID
  --advertise-endpoint HOST:PORT --bind HOST:PORT
  --handshake-timeout-millis N --max-inflight-connections N
  --metadata-create PATH starts the first standalone local-WAL owner
  --metadata-reopen PATH restarts the same exclusive local-WAL authority after lease loss
  --metadata-recover-log PATH installs or resumes one exact receipt-directed shared-log frontier

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
    adopt_legacy_agent_binding: bool,
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
    let agent_id = connection::parse_agent_id(
        invocation
            .agent_id
            .as_deref()
            .ok_or_else(|| "provision requires --agent-id".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    let control = connect_control(route)?;
    let existing_placement =
        provision::preflight_provision(control.as_ref(), root_id, logical_shard_id)
            .map_err(|error| error.to_string())?;
    let binding_preexisting = provision::ensure_root_agent_binding(
        control.as_ref(),
        root_id,
        agent_id,
        adopt_legacy_agent_binding,
    )
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
        "agent_id": encode_lowercase_hex(agent_id.as_bytes()),
        "binding_preexisting": binding_preexisting,
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
    _adopt_legacy_agent_binding: bool,
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
fn validate_served_root_agent_bindings(
    control: &dyn nokv_control::ControlStore,
    placements: &[nokv_control::RootPlacement],
) -> Result<(), String> {
    for placement in placements {
        load_root_agent_binding(control, placement.root_id)?;
    }
    Ok(())
}

#[cfg(feature = "etcd")]
fn join_lifecycle_workers(
    workers: Vec<std::thread::JoinHandle<Result<(), nokv_server::LifecycleError>>>,
    failures: &mut Vec<String>,
) {
    for worker in workers {
        match worker.join() {
            Ok(Err(error)) => failures.push(format!("lifecycle worker failed: {error}")),
            Ok(Ok(())) => {}
            Err(_) => failures.push("lifecycle worker panicked".to_owned()),
        }
    }
}

#[cfg(feature = "etcd")]
fn run_server(invocation: &Invocation) -> Result<(), String> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

    fn install_shutdown_signal() -> Result<Arc<AtomicBool>, String> {
        let shutdown = Arc::new(AtomicBool::new(false));
        for (name, signal) in [
            ("SIGINT", signal_hook::consts::SIGINT),
            ("SIGTERM", signal_hook::consts::SIGTERM),
        ] {
            signal_hook::flag::register(signal, Arc::clone(&shutdown))
                .map_err(|error| format!("cannot install {name} shutdown handler: {error}"))?;
        }
        Ok(shutdown)
    }

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

    let shutdown = install_shutdown_signal()?;
    let cli::RoutingConfig::Etcd(route) = &invocation.client.routing else {
        return Err("serve requires control-backed etcd routing".to_owned());
    };
    let metadata = match invocation.server.metadata_store.clone().ok_or_else(|| {
        "serve requires --metadata-create, --metadata-reopen, or --metadata-recover-log".to_owned()
    })? {
        cli::MetadataStoreConfig::Create(path) => OpenMode::New(path),
        cli::MetadataStoreConfig::Reopen(path) => OpenMode::Existing(path),
        cli::MetadataStoreConfig::RecoverLog(path) => OpenMode::RecoverLog(path),
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
    let placements = active_shard_placements(control.as_ref(), &placement)?;
    validate_served_root_agent_bindings(control.as_ref(), &placements)?;
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
    if invocation.server.lifecycle_interval_millis == 0
        || invocation.server.lifecycle_interval_millis > 60_000
    {
        return Err("--lifecycle-interval-millis must be within 1..=60000".to_owned());
    }
    if invocation.server.handshake_timeout_millis == 0
        || invocation.server.handshake_timeout_millis > 60_000
    {
        return Err("--handshake-timeout-millis must be within 1..=60000".to_owned());
    }
    if invocation.server.max_inflight_connections == 0
        || invocation.server.max_inflight_connections > 4_096
    {
        return Err("--max-inflight-connections must be within 1..=4096".to_owned());
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
        objects.clone(),
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
    let recovery = Arc::clone(owner.recovery_publisher());
    let owner_routes = owner.routes().to_vec();
    let server = WorkspaceServer::new(
        ServerOptions {
            bind: invocation.server.bind,
            handshake_timeout: Duration::from_millis(invocation.server.handshake_timeout_millis),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(renew_seconds),
            max_inflight_connections: invocation.server.max_inflight_connections,
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
            recovery.clone(),
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
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = std::thread::Builder::new()
            .name(format!("nokv-workspace-lifecycle-{index}"))
            .spawn(move || {
                let result = lifecycle.run_until_owner_loss_or_shutdown(worker_shutdown.as_ref());
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

    let server_result = server.run_until_shutdown(shutdown.as_ref());
    if server_result.is_err() {
        owner_loss.fail_closed();
    }

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
    use std::cell::Cell;

    use super::*;

    #[test]
    fn scoped_artifact_accepts_only_the_five_virtual_sections() {
        let path = scoped_artifact("run-1", "outputs", "result.json").unwrap();
        assert_eq!(path.logical_path(), "outputs/result.json");
        assert!(scoped_artifact("run-1", "tmp", "result.json").is_err());
        assert!(scoped_artifact("run-1", "outputs", "../result.json").is_err());
    }

    #[test]
    fn workspace_path_maps_sections_without_accepting_reserved_or_duplicated_paths() {
        let path = canonical_workspace_path("run-42", "outputs", "result.json").unwrap();
        assert_eq!(path.workbench.as_str(), "run-42");
        assert_eq!(path.path.as_str(), "outputs/result.json");
        assert!(canonical_workspace_path("run-42", "outputs", "outputs/result.json").is_err());
        assert!(canonical_workspace_path("run-42", "metadata", "run_manifest.json").is_err());
        assert!(canonical_workspace_path("run-42", "tmp", "result.json").is_err());
    }

    #[test]
    fn workspace_path_json_is_exact_and_replay_auditable() {
        let request_id = nokv_protocol::RequestIdentity([0xab; 16]);
        let source = canonical_workspace_path("run-42", "outputs", "a.bin").unwrap();
        let destination = canonical_workspace_path("run-42", "outputs", "b.bin").unwrap();
        let rename = nokv_protocol::RenamePathRequest {
            source: source.clone(),
            destination: destination.clone(),
            expected_generation: 7,
        };
        let rename_call = nokv_client::ClientCall {
            value: nokv_protocol::RenamePathResult {
                source: source.clone(),
                destination,
                workspace_revision: 9,
                generation: 7,
                artifact_revision_id: nokv_protocol::ArtifactRevisionIdentity([0xcd; 16]),
            },
            commit_version: Some(11),
            replayed: true,
        };
        assert_eq!(
            rename_path_json(request_id, &rename, &rename_call).unwrap(),
            json!({
                "status": "success",
                "operation": "rename",
                "request_id": "abababababababababababababababab",
                "workbench_id": "run-42",
                "path": "outputs/a.bin",
                "destination_path": "outputs/b.bin",
                "previous_generation": 7,
                "generation": 7,
                "idempotent_replay": true,
                "workspace_revision": 9,
                "artifact_revision_id": "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
                "commit_version": 11,
            })
        );

        let remove = nokv_protocol::RemovePathRequest {
            target: source,
            expected_generation: 7,
        };
        let remove_call = nokv_client::ClientCall {
            value: nokv_protocol::RemovePathResult {
                removed: true,
                workspace_revision: 10,
                removed_artifact_revision_id: Some(nokv_protocol::ArtifactRevisionIdentity(
                    [0xef; 16],
                )),
            },
            commit_version: Some(12),
            replayed: false,
        };
        assert_eq!(
            remove_path_json(request_id, &remove, &remove_call).unwrap(),
            json!({
                "status": "success",
                "operation": "remove",
                "request_id": "abababababababababababababababab",
                "workbench_id": "run-42",
                "path": "outputs/a.bin",
                "previous_generation": 7,
                "generation": 7,
                "idempotent_replay": false,
                "workspace_revision": 10,
                "removed_artifact_revision_id": "efefefefefefefefefefefefefefefef",
                "commit_version": 12,
            })
        );
    }

    #[test]
    fn agent_binding_mismatch_stops_before_client_preflight() {
        use nokv_control::{ControlStore, InMemoryControlStore, RootAgentBinding};
        use nokv_types::{AgentId, RootId};

        let control = InMemoryControlStore::new();
        let root_id = RootId::from_bytes([1; nokv_types::FIXED_ID_BYTES]);
        control
            .create_root_agent_binding(RootAgentBinding {
                root_id,
                agent_id: AgentId::from_bytes([7; nokv_types::FIXED_ID_BYTES]),
            })
            .unwrap();
        let rpc_preflight_called = Cell::new(false);

        let error = after_agent_root_admission(
            &control,
            root_id,
            AgentId::from_bytes([8; nokv_types::FIXED_ID_BYTES]),
            || {
                rpc_preflight_called.set(true);
                Ok(())
            },
        )
        .unwrap_err();

        assert!(error.contains("already bound to another Agent"));
        assert!(!rpc_preflight_called.get());
        assert!(!error.contains(&"07".repeat(nokv_types::FIXED_ID_BYTES)));
        assert!(!error.contains(&"08".repeat(nokv_types::FIXED_ID_BYTES)));

        after_agent_root_admission(
            &control,
            root_id,
            AgentId::from_bytes([7; nokv_types::FIXED_ID_BYTES]),
            || {
                rpc_preflight_called.set(true);
                Ok(())
            },
        )
        .unwrap();
        assert!(rpc_preflight_called.get());
    }

    #[test]
    fn missing_agent_binding_stops_before_client_preflight() {
        use nokv_control::InMemoryControlStore;
        use nokv_types::{AgentId, RootId};

        let control = InMemoryControlStore::new();
        let rpc_preflight_called = Cell::new(false);
        let error = after_agent_root_admission(
            &control,
            RootId::from_bytes([1; nokv_types::FIXED_ID_BYTES]),
            AgentId::from_bytes([7; nokv_types::FIXED_ID_BYTES]),
            || {
                rpc_preflight_called.set(true);
                Ok(())
            },
        )
        .unwrap_err();

        assert!(error.contains("no durable Agent binding"));
        assert!(error.contains("--adopt-legacy-agent-binding"));
        assert!(!rpc_preflight_called.get());
    }

    #[test]
    fn static_agent_route_fails_before_rpc_configuration() {
        let invocation = cli::parse(
            [
                "--root-id",
                "11111111111111111111111111111111",
                "--agent-id",
                "77777777777777777777777777777777",
                "--workbench-root",
                "/agents/test/wb",
                "--logical-shard-id",
                "not-a-valid-shard-id",
                "mcp",
                "--profile",
                "agent",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();

        assert_eq!(
            invocation.command,
            Command::Mcp {
                profile: McpProfile::Agent
            }
        );

        let error = configured_agent_admission(&invocation).unwrap_err();
        assert!(error.contains("durable etcd control"));
        assert!(!error.contains("logical-shard-id"));
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn shard_startup_selects_every_active_root_and_no_other_placement() {
        use nokv_control::{ControlStore, InMemoryControlStore, RootAgentBinding, RootPlacement};
        use nokv_types::{
            AgentId, LogicalShardId, PlacementGeneration, RootId, RootPlacementLifecycle,
        };

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

        let owner_before = control
            .get_logical_shard(&shard(9))
            .unwrap()
            .unwrap()
            .owner_epoch;
        let error = validate_served_root_agent_bindings(&control, &placements).unwrap_err();
        assert!(error.contains("no durable Agent binding"));
        assert_eq!(
            control
                .get_logical_shard(&shard(9))
                .unwrap()
                .unwrap()
                .owner_epoch,
            owner_before
        );

        for (root_id, agent_fill) in [(root(1), 7), (root(2), 8)] {
            control
                .create_root_agent_binding(RootAgentBinding {
                    root_id,
                    agent_id: AgentId::from_bytes([agent_fill; nokv_types::FIXED_ID_BYTES]),
                })
                .unwrap();
        }
        validate_served_root_agent_bindings(&control, &placements).unwrap();

        let provisioning = control.get_root_placement(&root(4)).unwrap().unwrap();
        assert!(active_shard_placements(&control, &provisioning).is_err());
    }
}
