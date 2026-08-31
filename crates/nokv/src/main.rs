/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

// Custom CLI, MCP adapter, and shard-owner process for NoKV Agent workspaces.

mod backend;
mod build_info;
mod cli;
mod connection;
mod object_store;
mod transfer;
mod workbench_mcp;

use std::io::{self, BufReader};
use std::path::Path;
use std::process::ExitCode;
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
use object_store::CliObjectStore;

type CliHandler = SdkWorkbenchToolHandler<CliWorkbenchBackend>;
type CliGenericAgentHandler = SdkGenericAgentToolHandler<CliWorkbenchBackend>;

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
    if matches!(invocation.command, Command::Mcp { .. }) {
        // stderr only: stdout carries the line-delimited JSON-RPC stream.
        eprintln!(
            "warning: `nokv mcp` is deprecated and is not a supported NoKV integration \
             surface. Use `nokv workbench <tool>` or the direct Python SDK. The sidecar is \
             retained only as the live qualification harness transport."
        );
    }
    match &invocation.command {
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::Schema => print_schema(),
        Command::Version { json } => print_version(*json),
        Command::Format => run_format(&invocation),
        Command::Provision => run_provision(&invocation),
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
            expected_generation,
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
                *expected_generation,
                content_type.as_deref(),
            )
        }
        Command::WorkspacePath(command) => run_workspace_path(&invocation, command),
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
    let client = connection::connect(&invocation.client).map_err(|error| error.to_string())?;
    client
        .preflight([nokv_protocol::WorkspaceCapability::WorkspacePathV1])
        .map_err(|error| format!("workspace preflight failed: {error}"))?;
    Ok(client)
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
    expected_generation: Option<u64>,
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
    if let Some(expected_generation) = expected_generation {
        arguments["expected_generation"] = Value::from(expected_generation);
    }
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

CLIENT:
  nokv --root-id HEX32 --seed HOST:PORT [--seed HOST:PORT ...] \\
    --workbench-root /agents/NAME/wb workbench <tool> '<json arguments>'
  nokv --root-id HEX32 --seed HOST:PORT [--seed HOST:PORT ...] \\
    materialize <workbench> <section> <path> <destination>
  nokv --root-id HEX32 --seed HOST:PORT [--seed HOST:PORT ...] \\
    --workbench-root /agents/NAME/wb collect <workbench> <section> <source> <path> [--replace]

Clients discover the current owner only through NoKV seeds. Repeat --seed for
failover; clients never connect to the metadata database directly.

METADATA RUNTIMES:
  nokv format --meta-url holt:///absolute/path
  nokv --root-id HEX32 --agent-id HEX32 provision --meta-url holt:///absolute/path
  nokv --advertise-endpoint HOST:PORT [owner options] serve --meta-url holt:///absolute/path

  nokv-fdb format --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'
  nokv-fdb --root-id HEX32 --agent-id HEX32 provision \\
    --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'
  nokv-fdb --node-id ID --advertise-endpoint HOST:PORT [owner options] serve \\
    --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'

Holt is one exclusive standalone metadata store with no control plane. FDB is
the shared metadata, catalog, route, session, and lease authority. FDB support
is feature gated and remains NOT QUALIFIED until the live serving gates pass.

OWNER OPTIONS:
  --bind HOST:PORT --handshake-timeout-millis N
  --max-inflight-connections N --lifecycle-interval-millis N

OBJECT DATA:
  --object-bucket NAME [--object-endpoint URL] [--object-root PREFIX]
  [--hot-cache-dir PATH --hot-cache-bytes BYTES]

OTHER:
  nokv schema
  nokv version [--json]
  nokv --version

NoKV exposes SDK and CLI operations. It does not expose FUSE or POSIX."
    );
}

fn configured_metadata_url(
    invocation: &Invocation,
) -> Result<Option<nokv_server::MetadataUrl>, String> {
    invocation
        .server
        .metadata_url
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(|error: nokv_server::MetadataUrlError| error.to_string())
}

fn run_format(invocation: &Invocation) -> Result<(), String> {
    let metadata = configured_metadata_url(invocation)?
        .ok_or_else(|| "format requires --meta-url".to_owned())?;
    match metadata {
        nokv_server::MetadataUrl::Holt(url) => {
            let outcome = nokv_server::format_holt(&url, build_info::VERSION)
                .map_err(|error| error.to_string())?;
            print_json(&json!({
                "status": "success",
                "operation": "format",
                "provider": "holt",
                "created": outcome.state == nokv_server::HoltFormatState::Created,
                "store_id": encode_lowercase_hex(outcome.manifest.store_id().as_bytes()),
                "logical_shard_id": encode_lowercase_hex(outcome.logical_shard_id.as_bytes()),
                "workspace_format_version": outcome.manifest.workspace_format_version(),
                "physical_encoding_version": outcome.manifest.physical_encoding_version(),
            }))
        }
        nokv_server::MetadataUrl::FoundationDb(url) => {
            #[cfg(feature = "fdb")]
            {
                let outcome = nokv_server::format_fdb(&url, build_info::VERSION)
                    .map_err(|error| error.to_string())?;
                print_json(&json!({
                    "status": "success",
                    "operation": "format",
                    "provider": "foundationdb",
                    "created": outcome.state == nokv_server::FdbFormatState::Created,
                    "store_id": encode_lowercase_hex(outcome.manifest.store_id().as_bytes()),
                    "workspace_format_version": outcome.manifest.workspace_format_version(),
                    "physical_encoding_version": outcome.manifest.physical_encoding_version(),
                }))
            }
            #[cfg(not(feature = "fdb"))]
            {
                let _ = url;
                Err(
                    "this binary has no FoundationDB runtime composition; use the feature-enabled nokv-fdb binary"
                        .to_owned(),
                )
            }
        }
    }
}

fn run_provision(invocation: &Invocation) -> Result<(), String> {
    let metadata = configured_metadata_url(invocation)?
        .ok_or_else(|| "provision requires --meta-url".to_owned())?;
    run_metadata_provision(invocation, metadata)
}

fn run_metadata_provision(
    invocation: &Invocation,
    metadata: nokv_server::MetadataUrl,
) -> Result<(), String> {
    let root_id = nokv_control::RootId::from(
        connection::configured_root_id(&invocation.client).map_err(|error| error.to_string())?,
    );
    let agent_id = connection::parse_agent_id(
        invocation
            .agent_id
            .as_deref()
            .ok_or_else(|| "provision requires --agent-id".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    match metadata {
        nokv_server::MetadataUrl::Holt(url) => {
            let outcome = nokv_server::provision_holt(&url, root_id, agent_id)
                .map_err(|error| error.to_string())?;
            let objects = CliObjectStore::build(&invocation.client.object)?;
            objects.ensure_namespace(outcome.root.object_namespace_id)?;
            let objects = objects.bind(outcome.root.object_namespace_id)?;
            objects.validate_agent_capabilities()?;
            print_json(&json!({
                "status": "success",
                "operation": "provision",
                "provider": "holt",
                "preexisting": outcome.preexisting,
                "root_id": encode_lowercase_hex(outcome.root.root_id.as_bytes()),
                "agent_id": encode_lowercase_hex(outcome.root.agent_id.as_bytes()),
                "logical_shard_id": encode_lowercase_hex(outcome.logical_shard_id.as_bytes()),
                "object_namespace_id": encode_lowercase_hex(outcome.root.object_namespace_id.as_bytes()),
                "placement_generation": outcome.root.placement_generation.get(),
                "lifecycle": "ready",
            }))
        }
        nokv_server::MetadataUrl::FoundationDb(url) => {
            #[cfg(feature = "fdb")]
            {
                let objects = CliObjectStore::build(&invocation.client.object)?;
                let prepared = nokv_server::prepare_fdb_provision(&url, root_id, agent_id)
                    .map_err(|error| error.to_string())?;
                let namespace_id = prepared.root().object_namespace_id();
                objects.ensure_namespace(namespace_id)?;
                let objects = objects.bind(namespace_id)?;
                objects.validate_agent_capabilities()?;
                let outcome = prepared
                    .finalize_after_namespace_admission()
                    .map_err(|error| error.to_string())?;
                print_json(&json!({
                    "status": "success",
                    "operation": "provision",
                    "provider": "foundationdb",
                    "preexisting": outcome.preexisting,
                    "root_id": encode_lowercase_hex(outcome.root.root_id().as_bytes()),
                    "agent_id": encode_lowercase_hex(outcome.root.agent_id().as_bytes()),
                    "logical_shard_id": encode_lowercase_hex(outcome.shard.logical_shard_id().as_bytes()),
                    "object_namespace_id": encode_lowercase_hex(namespace_id.as_bytes()),
                    "placement_generation": outcome.root.placement_generation().get(),
                    "lifecycle": "ready",
                }))
            }
            #[cfg(not(feature = "fdb"))]
            {
                let _ = (url, agent_id);
                Err(
                    "this binary has no FoundationDB runtime composition; use the feature-enabled nokv-fdb binary"
                        .to_owned(),
                )
            }
        }
    }
}

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

fn install_shutdown_signal() -> Result<Arc<std::sync::atomic::AtomicBool>, String> {
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for (name, signal) in [
        ("SIGINT", signal_hook::consts::SIGINT),
        ("SIGTERM", signal_hook::consts::SIGTERM),
    ] {
        signal_hook::flag::register(signal, Arc::clone(&shutdown))
            .map_err(|error| format!("cannot install {name} shutdown handler: {error}"))?;
    }
    Ok(shutdown)
}

fn run_server(invocation: &Invocation) -> Result<(), String> {
    match configured_metadata_url(invocation)?
        .ok_or_else(|| "serve requires --meta-url".to_owned())?
    {
        nokv_server::MetadataUrl::Holt(url) => run_holt_server(invocation, &url),
        nokv_server::MetadataUrl::FoundationDb(url) => {
            #[cfg(feature = "fdb")]
            {
                run_fdb_server(invocation, &url)
            }
            #[cfg(not(feature = "fdb"))]
            {
                let _ = url;
                Err(
                    "this binary has no FoundationDB runtime composition; use the feature-enabled nokv-fdb binary"
                        .to_owned(),
                )
            }
        }
    }
}

#[cfg(feature = "fdb")]
fn release_fdb_runtime_after(
    runtime: &nokv_server::FdbServingRuntime,
    primary: impl Into<String>,
) -> String {
    let primary = primary.into();
    match runtime.release_ownership() {
        Ok(()) => primary,
        Err(cleanup) => format!("{primary}; FoundationDB owner release failed: {cleanup}"),
    }
}

#[cfg(feature = "fdb")]
fn run_fdb_server(
    invocation: &Invocation,
    metadata: &nokv_server::FoundationDbMetadataUrl,
) -> Result<(), String> {
    use std::net::TcpListener;
    use std::time::Duration;

    use nokv_control::NodeId;
    use nokv_server::{
        ArtifactLifecycleDeleter, CommittedMetadataDurability, LifecycleObjectDeleter,
        LifecycleRunner, LifecycleRunnerOptions, ServerOptions,
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
    let node_id = NodeId::new(
        invocation
            .server
            .node_id
            .clone()
            .ok_or_else(|| "FoundationDB serve requires --node-id".to_owned())?,
    )
    .map_err(|error| format!("invalid --node-id: {error:?}"))?;
    let endpoint = invocation
        .server
        .advertise_endpoint
        .as_deref()
        .ok_or_else(|| "serve requires --advertise-endpoint".to_owned())?
        .parse::<std::net::SocketAddr>()
        .map_err(|error| format!("invalid --advertise-endpoint: {error}"))?;
    let shutdown = install_shutdown_signal()?;
    let runtime = nokv_server::serve_fdb(metadata, node_id, endpoint, shutdown.as_ref())
        .map_err(|error| error.to_string())?;
    let namespace = runtime
        .roots()
        .first()
        .expect("serve_fdb requires at least one Ready root")
        .route()
        .object_namespace_id;
    if runtime
        .roots()
        .iter()
        .any(|root| root.route().object_namespace_id != namespace)
    {
        return Err(release_fdb_runtime_after(
            &runtime,
            "FoundationDB roots do not share one object namespace",
        ));
    }
    let namespace_id = nokv_types::ObjectNamespaceId::from(namespace);
    let objects = match CliObjectStore::build(&invocation.client.object)
        .and_then(|objects| objects.bind(namespace_id))
        .and_then(|objects| {
            objects.validate_agent_capabilities()?;
            Ok(objects)
        }) {
        Ok(objects) => Arc::new(objects),
        Err(primary) => return Err(release_fdb_runtime_after(&runtime, primary)),
    };
    let listener = match TcpListener::bind(invocation.server.bind) {
        Ok(listener) => listener,
        Err(error) => {
            return Err(release_fdb_runtime_after(
                &runtime,
                format!("cannot bind RPC listener: {error}"),
            ));
        }
    };
    if let Err(error) = listener.set_nonblocking(true) {
        return Err(release_fdb_runtime_after(
            &runtime,
            format!("cannot prepare RPC listener: {error}"),
        ));
    }
    let server = runtime
        .workspace_server(ServerOptions {
            bind: invocation.server.bind,
            handshake_timeout: Duration::from_millis(invocation.server.handshake_timeout_millis),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval: runtime.lease_renew_interval(),
            max_inflight_connections: invocation.server.max_inflight_connections,
        })
        .map_err(|error| error.to_string())?;
    let owner_loss = server.owner_loss_signal();
    let lifecycle_objects: Arc<dyn LifecycleObjectDeleter> =
        Arc::new(ArtifactLifecycleDeleter::new(objects));
    let durability = Arc::new(CommittedMetadataDurability);
    let mut lifecycles = Vec::with_capacity(runtime.roots().len());
    for root in runtime.roots() {
        match LifecycleRunner::new(
            Arc::clone(root.meta()),
            Arc::clone(runtime.registry()),
            root.route(),
            owner_loss.clone(),
            Arc::clone(&lifecycle_objects),
            durability.clone(),
            LifecycleRunnerOptions {
                poll_interval: Duration::from_millis(invocation.server.lifecycle_interval_millis),
                ..LifecycleRunnerOptions::default()
            },
        ) {
            Ok(lifecycle) => lifecycles.push(lifecycle),
            Err(error) => {
                let primary = error.to_string();
                return Err(match server.release_ownership() {
                    Ok(()) => primary,
                    Err(cleanup) => {
                        format!("{primary}; FoundationDB owner release failed: {cleanup}")
                    }
                });
            }
        }
    }
    let mut lifecycle_workers = Vec::with_capacity(lifecycles.len());
    for (index, lifecycle) in lifecycles.into_iter().enumerate() {
        let worker_owner_loss = owner_loss.clone();
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = std::thread::Builder::new()
            .name(format!("nokv-fdb-lifecycle-{index}"))
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
                    failures.push(format!("FoundationDB owner release failed: {error}"));
                }
                return Err(failures.join("; "));
            }
        }
    }
    if let Err(error) = runtime.activate_routes() {
        owner_loss.fail_closed();
        let mut failures = vec![format!("cannot publish FoundationDB routes: {error}")];
        join_lifecycle_workers(lifecycle_workers, &mut failures);
        if let Err(error) = server.release_ownership() {
            failures.push(format!("FoundationDB owner release failed: {error}"));
        }
        return Err(failures.join("; "));
    }

    let server_result = server.serve_until_shutdown(listener, shutdown.as_ref());
    if server_result.is_err() {
        owner_loss.fail_closed();
    }
    let mut failures = Vec::new();
    join_lifecycle_workers(lifecycle_workers, &mut failures);
    if let Err(error) = server.release_ownership() {
        failures.push(format!("FoundationDB owner release failed: {error}"));
    }
    if let Err(error) = server_result {
        failures.push(format!("RPC server stopped: {error}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn run_holt_server(
    invocation: &Invocation,
    metadata: &nokv_server::HoltMetadataUrl,
) -> Result<(), String> {
    use std::time::Duration;

    use nokv_protocol::{LogicalShardIdentity, ObjectNamespaceIdentity, RootIdentity, RootRoute};
    use nokv_server::{
        ArtifactLifecycleDeleter, CommittedMetadataDurability, LifecycleObjectDeleter,
        LifecycleRunner, LifecycleRunnerOptions, ServerOptions,
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
    let endpoint = invocation
        .server
        .advertise_endpoint
        .as_deref()
        .ok_or_else(|| "serve requires --advertise-endpoint".to_owned())?
        .parse::<std::net::SocketAddr>()
        .map_err(|error| format!("invalid --advertise-endpoint: {error}"))?;
    let shutdown = install_shutdown_signal()?;
    let runtime = nokv_server::serve_holt(metadata, endpoint).map_err(|error| error.to_string())?;
    let namespace = runtime
        .roots()
        .first()
        .expect("serve_holt requires at least one Ready root")
        .object_namespace_id;
    if runtime
        .roots()
        .iter()
        .any(|root| root.object_namespace_id != namespace)
    {
        return Err("standalone Holt roots do not share one object namespace".to_owned());
    }
    let objects = Arc::new(CliObjectStore::build(&invocation.client.object)?.bind(namespace)?);
    objects.validate_agent_capabilities()?;
    let server = runtime
        .workspace_server(ServerOptions {
            bind: invocation.server.bind,
            handshake_timeout: Duration::from_millis(invocation.server.handshake_timeout_millis),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(30),
            max_inflight_connections: invocation.server.max_inflight_connections,
        })
        .map_err(|error| error.to_string())?;
    let owner_loss = server.owner_loss_signal();
    let lifecycle_objects: Arc<dyn LifecycleObjectDeleter> =
        Arc::new(ArtifactLifecycleDeleter::new(objects));
    let durability = Arc::new(CommittedMetadataDurability);
    let mut lifecycles = Vec::with_capacity(runtime.roots().len());
    for root in runtime.roots() {
        let route = RootRoute {
            root_id: RootIdentity::from(root.root_id),
            logical_shard_id: LogicalShardIdentity::from(runtime.logical_shard_id()),
            object_namespace_id: ObjectNamespaceIdentity::from(root.object_namespace_id),
            placement_generation: root.placement_generation.get(),
            owner_epoch: runtime.owner_epoch().get(),
        };
        let lifecycle = LifecycleRunner::new(
            Arc::clone(runtime.meta()),
            Arc::clone(runtime.registry()),
            route,
            owner_loss.clone(),
            Arc::clone(&lifecycle_objects),
            durability.clone(),
            LifecycleRunnerOptions {
                poll_interval: Duration::from_millis(invocation.server.lifecycle_interval_millis),
                ..LifecycleRunnerOptions::default()
            },
        )
        .map_err(|error| error.to_string())?;
        lifecycles.push(lifecycle);
    }
    let mut lifecycle_workers = Vec::with_capacity(lifecycles.len());
    for (index, lifecycle) in lifecycles.into_iter().enumerate() {
        let worker_owner_loss = owner_loss.clone();
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = std::thread::Builder::new()
            .name(format!("nokv-holt-lifecycle-{index}"))
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
                    failures.push(format!("standalone owner release failed: {error}"));
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
    if let Err(error) = server.release_ownership() {
        failures.push(format!("standalone owner release failed: {error}"));
    }
    if let Err(error) = server_result {
        failures.push(format!("RPC server stopped: {error}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
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
}
