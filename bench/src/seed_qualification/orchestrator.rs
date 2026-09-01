/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nokv_client::{
    ClientOptions, ResolvedRoute, RouteResolver, SeedRouteOptions, SeedRouteResolver,
    WorkspaceClient,
};
use nokv_protocol::{
    decode_response, encode_request, CreateWorkspaceRequest, DiscoverRouteOutcome,
    DiscoverRouteRequest, DiscoveredRoute, GetWorkspaceRequest, OwnerEndpoint, RootIdentity,
    RpcRequest, RpcResponse, WorkbenchName, WorkspaceIdentity,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use url::Url;

use super::peer::{DiscoveryControl, QualificationPeer, RecordingTransport, TransportEvent};
use super::scenario::{
    endpoint_drift, immutable_identity_drift, require_takeover, RouteEvidence, ScenarioResult,
};
use super::QualificationOptions;
use crate::qualification_runtime::{
    lowercase_hex, sha256_bytes, sha256_file, unix_millis, EvidenceBundle, ProcessExit, ProcessSet,
};

const RESULT_SCHEMA: &str = "nokv.fdb.seed-qualification.result.v1";
const ENVIRONMENT_SCHEMA: &str = "nokv.fdb.seed-qualification.environment.v1";

#[derive(Serialize)]
struct EnvironmentEvidence {
    schema: &'static str,
    run_id: String,
    source_revision: String,
    source_dirty: bool,
    candidate_binary: String,
    candidate_sha256: String,
    candidate_version: Value,
    rust_toolchain: String,
    operating_system: &'static str,
    architecture: &'static str,
    monotonic_clock: &'static str,
    fdb_cluster_file: String,
    fdb_cluster_file_sha256: String,
    fdb_prefix: String,
    fdbcli_version: String,
    rustfs_service_identity: String,
    rustfs_health_url: String,
    object_endpoint: String,
    object_bucket: String,
    object_region: String,
    object_root: String,
    object_namespace_binding_sha256: String,
    owner_a_endpoint: SocketAddr,
    owner_b_backend_endpoint: SocketAddr,
    owner_b_endpoint: SocketAddr,
    seed_peer_endpoint: SocketAddr,
    failed_seed_endpoint: SocketAddr,
    takeover_timeout_seconds: u64,
    operation_timeout_seconds: u64,
}

#[derive(Serialize)]
struct TerminalResult {
    schema: &'static str,
    status: &'static str,
    error: Option<String>,
    scenarios: Vec<ScenarioResult>,
    owner_a_route: Option<RouteEvidence>,
    owner_b_route: Option<RouteEvidence>,
    process_exits: Vec<ProcessExit>,
    timeline: Vec<TimelineEvent>,
}

#[derive(Clone, Debug, Serialize)]
struct TimelineEvent {
    event: &'static str,
    unix_millis: u64,
}

struct RunState {
    processes: ProcessSet,
    seed_peer: Option<QualificationPeer>,
    owner_proxy: Option<QualificationPeer>,
    transport: Option<RecordingTransport>,
    scenarios: Vec<ScenarioResult>,
    owner_a_route: Option<DiscoveredRoute>,
    owner_b_route: Option<DiscoveredRoute>,
    timeline: Vec<TimelineEvent>,
}

impl RunState {
    fn new() -> Self {
        Self {
            processes: ProcessSet::default(),
            seed_peer: None,
            owner_proxy: None,
            transport: None,
            scenarios: Vec::new(),
            owner_a_route: None,
            owner_b_route: None,
            timeline: Vec::new(),
        }
    }

    fn record(&mut self, event: &'static str) {
        self.timeline.push(TimelineEvent {
            event,
            unix_millis: unix_millis(),
        });
    }
}

pub fn run(options: QualificationOptions) -> Result<PathBuf, String> {
    let evidence = EvidenceBundle::create(options.evidence_dir.clone())?;
    let run_id = run_id(&options);
    let fdb_prefix = format!("{}-{run_id}", options.fdb_prefix_base);
    let object_root = format!(
        "{}/{run_id}",
        options.object_root_base.trim_end_matches('/')
    );
    let metadata_url = metadata_url(&options.fdb_cluster_file, &fdb_prefix)?;
    let root_id = RootIdentity(identity_bytes(&run_id, "root"));
    let agent_id = identity_bytes(&run_id, "agent");
    let mut state = RunState::new();

    let before_health = capture_fdb_status(&evidence, &options, "fdb-status-before")
        .and_then(|()| capture_rustfs_health(&evidence, &options, "rustfs-health-before"));
    let execution = before_health.and_then(|()| {
        capture_environment(&evidence, &options, &run_id, &fdb_prefix, &object_root)?;
        execute_live(
            &evidence,
            &options,
            &run_id,
            &metadata_url,
            &object_root,
            root_id,
            agent_id,
            &mut state,
        )
    });

    if let Some(peer) = state.seed_peer.as_ref() {
        let _ = evidence.write_json("peers/discovery.json", &peer.transcripts());
    }
    if let Some(peer) = state.owner_proxy.as_ref() {
        let _ = evidence.write_json("peers/owner-b-proxy.json", &peer.transcripts());
    }
    if let Some(transport) = state.transport.as_ref() {
        let _ = evidence.write_json("client-transport.json", &transport.events());
    }
    if let Some(peer) = state.seed_peer.as_mut() {
        peer.stop();
    }
    if let Some(peer) = state.owner_proxy.as_mut() {
        peer.stop();
    }
    let process_exits = state.processes.reap_all();
    state.record("processes_reaped");
    let _ = evidence.write_json("owners/processes.json", &process_exits);
    let _ = evidence.write_json("scenarios.json", &state.scenarios);
    let _ = evidence.write_json("timeline.json", &state.timeline);

    let after_health = capture_fdb_status(&evidence, &options, "fdb-status-after")
        .and_then(|()| capture_rustfs_health(&evidence, &options, "rustfs-health-after"));
    let outcome = match (execution, after_health) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(health)) => Err(health),
        (Err(primary), Err(health)) => Err(format!("{primary}; post-run health: {health}")),
    };
    let result = TerminalResult {
        schema: RESULT_SCHEMA,
        status: if outcome.is_ok() { "PASS" } else { "FAIL" },
        error: outcome.as_ref().err().cloned(),
        scenarios: state.scenarios,
        owner_a_route: state.owner_a_route.as_ref().map(RouteEvidence::from),
        owner_b_route: state.owner_b_route.as_ref().map(RouteEvidence::from),
        process_exits,
        timeline: state.timeline,
    };
    evidence.finalize(&result)?;
    match outcome {
        Ok(()) => Ok(options.evidence_dir),
        Err(error) => Err(format!(
            "{error}; retained evidence: {}",
            evidence.root().display()
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_live(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    run_id: &str,
    metadata_url: &str,
    object_root: &str,
    root_id: RootIdentity,
    agent_id: [u8; 16],
    state: &mut RunState,
) -> Result<(), String> {
    state.record("setup_started");
    require_failed_endpoint(options.failed_seed_endpoint)?;
    run_format(evidence, options, metadata_url)?;
    run_provision(
        evidence,
        options,
        metadata_url,
        object_root,
        root_id,
        agent_id,
    )?;
    state.record("root_provisioned");

    let mut owner_a = owner_command(
        options,
        metadata_url,
        object_root,
        options.owner_a_endpoint,
        options.owner_a_endpoint,
        &format!("gate7-a-{run_id}"),
    );
    state.processes.spawn("owner-a", &mut owner_a, evidence)?;
    state.record("owner_a_started");
    let owner_a_route = wait_for_route(
        options.owner_a_endpoint,
        root_id,
        options.operation_timeout,
        "owner-a",
        &mut state.processes,
    )?;
    if owner_a_route.owner_endpoint.socket_addr() != options.owner_a_endpoint {
        return Err("owner A published an unexpected endpoint".to_owned());
    }
    evidence.write_json("routes/owner-a.json", &RouteEvidence::from(&owner_a_route))?;
    state.owner_a_route = Some(owner_a_route.clone());
    state.record("owner_a_serving");

    let (seed_peer, seed_control) = QualificationPeer::start_discovery(
        "seed",
        options.seed_peer_endpoint,
        owner_a_route.clone(),
    )?;
    if seed_peer.endpoint() != options.seed_peer_endpoint {
        return Err("seed qualification peer did not bind the requested endpoint".to_owned());
    }
    state.seed_peer = Some(seed_peer);
    let (owner_proxy, proxy_control) = QualificationPeer::start_proxy(
        "owner-b-proxy",
        options.owner_b_endpoint,
        options.owner_b_backend_endpoint,
        options.operation_timeout,
    )?;
    if owner_proxy.endpoint() != options.owner_b_endpoint {
        return Err("owner B qualification proxy did not bind the requested endpoint".to_owned());
    }
    state.owner_proxy = Some(owner_proxy);

    let transport = RecordingTransport::new(options.operation_timeout)?;
    state.transport = Some(transport.clone());
    let resolver = SeedRouteResolver::new(
        transport.clone(),
        [
            options.failed_seed_endpoint,
            options.seed_peer_endpoint,
            options.owner_b_backend_endpoint,
        ],
        SeedRouteOptions {
            max_attempts: 9,
            initial_backoff: Duration::from_millis(10),
            maximum_backoff: Duration::from_millis(100),
        },
    )
    .map_err(|error| error.to_string())?;
    let client = WorkspaceClient::new(
        root_id,
        transport.clone(),
        resolver.clone(),
        ClientOptions { max_attempts: 8 },
    )
    .map_err(|error| error.to_string())?;

    let initial_event = transport.events().len();
    let initial = resolver
        .resolve(root_id, false)
        .map_err(|error| format!("initial seed resolution failed: {error}"))?;
    require_resolved(&initial, &owner_a_route)?;
    client
        .preflight([])
        .map_err(|error| format!("owner A preflight failed: {error}"))?;
    let initial_events = transport.events();
    qualify_initial_seed_order(
        &initial_events[initial_event..],
        options.failed_seed_endpoint,
        options.seed_peer_endpoint,
    )?;
    state.scenarios.push(ScenarioResult::pass(
        "multiple_seeds",
        "the persistent resolver contacted two distinct configured seed endpoints",
    ));
    state.scenarios.push(ScenarioResult::pass(
        "failed_first_seed",
        "the first seed connection failed and the later typed seed resolved owner A",
    ));

    let mut owner_b = owner_command(
        options,
        metadata_url,
        object_root,
        options.owner_b_backend_endpoint,
        options.owner_b_endpoint,
        &format!("gate7-b-{run_id}"),
    );
    state.processes.spawn("owner-b", &mut owner_b, evidence)?;
    state.record("owner_b_contender_started");
    thread::sleep(Duration::from_millis(250));
    state.processes.require_running("owner-b")?;
    state.processes.terminate("owner-a")?;
    state.record("owner_a_terminated");

    let owner_b_route = wait_for_route(
        options.owner_b_backend_endpoint,
        root_id,
        options.takeover_timeout,
        "owner-b",
        &mut state.processes,
    )?;
    require_takeover(&owner_a_route, &owner_b_route)?;
    if owner_b_route.owner_endpoint.socket_addr() != options.owner_b_endpoint {
        return Err("owner B published an endpoint other than the qualification proxy".to_owned());
    }
    evidence.write_json("routes/owner-b.json", &RouteEvidence::from(&owner_b_route))?;
    state.owner_b_route = Some(owner_b_route.clone());
    state.record("owner_b_serving");
    seed_control.set_default(owner_b_route.clone());

    let takeover_event = transport.events().len();
    client
        .preflight([])
        .map_err(|error| format!("persistent client did not recover through owner B: {error}"))?;
    qualify_endpoint_change(
        &transport.events()[takeover_event..],
        options.owner_a_endpoint,
        options.owner_b_endpoint,
    )?;
    state.scenarios.push(ScenarioResult::pass(
        "owner_endpoint_change",
        format!(
            "owner epoch {} -> {} and session generation {} -> {}",
            owner_a_route.owner_epoch,
            owner_b_route.owner_epoch,
            owner_a_route.session_generation,
            owner_b_route.session_generation
        ),
    ));

    let stale_action = seed_control.enqueue("stale-discovery", owner_a_route.clone())?;
    exercise_discovery_action(
        &resolver,
        &seed_control,
        &stale_action,
        root_id,
        &owner_b_route,
    )?;
    state.scenarios.push(ScenarioResult::pass(
        "stale_discovery",
        "the resolver rejected authentic owner A state after owner B was cached",
    ));

    let drifted = endpoint_drift(&owner_b_route, options.owner_a_endpoint)?;
    let drift_action = seed_control.enqueue("same-generation-endpoint-drift", drifted)?;
    exercise_discovery_action(
        &resolver,
        &seed_control,
        &drift_action,
        root_id,
        &owner_b_route,
    )?;
    state.scenarios.push(ScenarioResult::pass(
        "endpoint_drift",
        "the resolver rejected an endpoint change with unchanged route generations",
    ));

    let mut foreign = immutable_identity_drift(&owner_b_route);
    foreign.owner_endpoint = OwnerEndpoint::new(options.failed_seed_endpoint.to_string())
        .map_err(|error| error.to_string())?;
    let foreign_action = seed_control.enqueue("immutable-identity-drift", foreign)?;
    let foreign_event = transport.events().len();
    exercise_discovery_action(
        &resolver,
        &seed_control,
        &foreign_action,
        root_id,
        &owner_b_route,
    )?;
    client
        .preflight([])
        .map_err(|error| format!("client failed after immutable identity drift: {error}"))?;
    if transport.events()[foreign_event..].iter().any(|event| {
        event.request_kind == "workspace" && event.endpoint == options.failed_seed_endpoint
    }) {
        return Err("client sent a workspace request to the foreign route endpoint".to_owned());
    }
    state.scenarios.push(ScenarioResult::pass(
        "immutable_identity_drift",
        "the foreign shard identity was rejected and received no workspace request",
    ));

    let stale_hint_action =
        proxy_control.inject_not_owner_once("stale-owner-hint", owner_a_route.clone())?;
    client
        .preflight([])
        .map_err(|error| format!("client failed after stale NotOwner hint: {error}"))?;
    if !proxy_control.saw_action(&stale_hint_action) {
        return Err("owner B proxy did not exercise the stale NotOwner hint".to_owned());
    }
    let retained = resolver
        .resolve(root_id, false)
        .map_err(|error| format!("cannot inspect route after stale owner hint: {error}"))?;
    require_resolved(&retained, &owner_b_route)?;
    state.scenarios.push(ScenarioResult::pass(
        "stale_owner_hint",
        "a typed NotOwner response carried A; the persistent resolver retained B",
    ));

    let workbench = WorkbenchName::new(format!("gate7-{run_id}"))
        .map_err(|error| format!("cannot build qualification workbench name: {error}"))?;
    let workspace_incarnation_id = WorkspaceIdentity(identity_bytes(run_id, "workspace"));
    let created = client
        .create_workspace(
            client.new_request_id(),
            CreateWorkspaceRequest {
                workbench: workbench.clone(),
                workspace_incarnation_id,
            },
        )
        .map_err(|error| format!("final metadata mutation failed: {error}"))?;
    if created.commit_version.is_none()
        || created.value.workspace_incarnation_id != workspace_incarnation_id
    {
        return Err("final metadata mutation returned an invalid durable result".to_owned());
    }
    let read = client
        .get_workspace(GetWorkspaceRequest {
            workbench: workbench.clone(),
        })
        .map_err(|error| format!("final metadata read-back failed: {error}"))?;
    if read.value.workbench != workbench
        || read.value.workspace_incarnation_id != workspace_incarnation_id
        || read.value != created.value
    {
        return Err("final metadata read-back did not match the created workspace".to_owned());
    }
    state.scenarios.push(ScenarioResult::pass(
        "final_mutation",
        format!(
            "created and read back {} at commit version {} through owner B",
            workbench,
            created.commit_version.expect("checked above")
        ),
    ));
    evidence.write_json("scenarios.json", &state.scenarios)?;
    state.record("qualification_completed");
    Ok(())
}

fn capture_environment(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    run_id: &str,
    fdb_prefix: &str,
    object_root: &str,
) -> Result<(), String> {
    let mut version = Command::new(&options.candidate_binary);
    version.args(["version", "--json"]);
    let version = run_checked_command(evidence, "candidate-version", &mut version)?;
    let candidate_version: Value = serde_json::from_str(&version)
        .map_err(|error| format!("candidate version output is not JSON: {error}"))?;
    if candidate_version.get("git_commit").and_then(Value::as_str)
        != Some(options.source_revision.as_str())
    {
        return Err(
            "candidate binary git_commit does not match --source-revision; rebuild the exact head"
                .to_owned(),
        );
    }
    if options.source_dirty {
        return Err("a live qualification PASS requires --source-dirty false".to_owned());
    }
    let rust_toolchain = command_stdout(Command::new("rustc").arg("--version"))?;
    let fdbcli_version = command_stdout(Command::new(&options.fdbcli).arg("--version"))?;
    let binding = format!(
        "{}\0{}\0{}\0{}",
        options.object_endpoint, options.object_bucket, options.object_region, object_root
    );
    let environment = EnvironmentEvidence {
        schema: ENVIRONMENT_SCHEMA,
        run_id: run_id.to_owned(),
        source_revision: options.source_revision.clone(),
        source_dirty: options.source_dirty,
        candidate_binary: options.candidate_binary.display().to_string(),
        candidate_sha256: sha256_file(&options.candidate_binary)?,
        candidate_version,
        rust_toolchain: rust_toolchain.trim().to_owned(),
        operating_system: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
        monotonic_clock: "std::time::Instant",
        fdb_cluster_file: options.fdb_cluster_file.display().to_string(),
        fdb_cluster_file_sha256: sha256_file(&options.fdb_cluster_file)?,
        fdb_prefix: fdb_prefix.to_owned(),
        fdbcli_version: fdbcli_version.trim().to_owned(),
        rustfs_service_identity: options.rustfs_service_identity.clone(),
        rustfs_health_url: options.rustfs_health_url.clone(),
        object_endpoint: options.object_endpoint.clone(),
        object_bucket: options.object_bucket.clone(),
        object_region: options.object_region.clone(),
        object_root: object_root.to_owned(),
        object_namespace_binding_sha256: sha256_bytes(binding.as_bytes()),
        owner_a_endpoint: options.owner_a_endpoint,
        owner_b_backend_endpoint: options.owner_b_backend_endpoint,
        owner_b_endpoint: options.owner_b_endpoint,
        seed_peer_endpoint: options.seed_peer_endpoint,
        failed_seed_endpoint: options.failed_seed_endpoint,
        takeover_timeout_seconds: options.takeover_timeout.as_secs(),
        operation_timeout_seconds: options.operation_timeout.as_secs(),
    };
    evidence.write_json("environment.json", &environment)
}

fn run_format(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    metadata_url: &str,
) -> Result<(), String> {
    let mut command = Command::new(&options.candidate_binary);
    command.args(["format", "--meta-url", metadata_url]);
    let output = run_checked_command(evidence, "format", &mut command)?;
    let value: Value = serde_json::from_str(&output)
        .map_err(|error| format!("format output is not JSON: {error}"))?;
    if value.get("created") != Some(&Value::Bool(true))
        || value.get("provider").and_then(Value::as_str) != Some("foundationdb")
    {
        return Err("format did not create a fresh FoundationDB store".to_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_provision(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    metadata_url: &str,
    object_root: &str,
    root_id: RootIdentity,
    agent_id: [u8; 16],
) -> Result<(), String> {
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(["--root-id", &lowercase_hex(&root_id.0)])
        .args(["--agent-id", &lowercase_hex(&agent_id)]);
    append_object_arguments(&mut command, options, object_root);
    command.args(["provision", "--meta-url", metadata_url]);
    let output = run_checked_command(evidence, "provision", &mut command)?;
    let value: Value = serde_json::from_str(&output)
        .map_err(|error| format!("provision output is not JSON: {error}"))?;
    if value.get("preexisting") != Some(&Value::Bool(false))
        || value.get("provider").and_then(Value::as_str) != Some("foundationdb")
        || value.get("lifecycle").and_then(Value::as_str) != Some("ready")
        || value.get("root_id").and_then(Value::as_str) != Some(lowercase_hex(&root_id.0).as_str())
    {
        return Err("provision did not create the requested fresh Ready root".to_owned());
    }
    Ok(())
}

fn owner_command(
    options: &QualificationOptions,
    metadata_url: &str,
    object_root: &str,
    bind: SocketAddr,
    advertise: SocketAddr,
    node_id: &str,
) -> Command {
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(["--bind", &bind.to_string()])
        .args(["--advertise-endpoint", &advertise.to_string()])
        .args(["--node-id", node_id]);
    append_object_arguments(&mut command, options, object_root);
    command.args(["serve", "--meta-url", metadata_url]);
    command
}

fn append_object_arguments(
    command: &mut Command,
    options: &QualificationOptions,
    object_root: &str,
) {
    command
        .args(["--object-endpoint", &options.object_endpoint])
        .args(["--object-bucket", &options.object_bucket])
        .args(["--object-region", &options.object_region])
        .args(["--object-root", object_root])
        .args(["--object-access-key-id", &options.object_access_key_id])
        .args([
            "--object-secret-access-key",
            &options.object_secret_access_key,
        ]);
}

fn metadata_url(cluster_file: &Path, prefix: &str) -> Result<String, String> {
    let cluster_file = cluster_file
        .to_str()
        .ok_or_else(|| "FDB cluster-file path must be valid UTF-8".to_owned())?;
    let mut url = Url::parse("fdb:///").expect("static FDB URL is valid");
    url.set_path(cluster_file);
    url.query_pairs_mut().append_pair("prefix", prefix);
    Ok(url.to_string())
}

fn capture_fdb_status(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    name: &str,
) -> Result<(), String> {
    let mut command = Command::new(&options.fdbcli);
    command
        .args(["-C"])
        .arg(&options.fdb_cluster_file)
        .args(["--exec", "status json"]);
    let output = run_checked_command(evidence, name, &mut command)?;
    let status: Value = serde_json::from_str(&output)
        .map_err(|error| format!("{name} output is not FoundationDB status JSON: {error}"))?;
    let client_available = status
        .pointer("/client/database_status/available")
        .and_then(Value::as_bool);
    let client_healthy = status
        .pointer("/client/database_status/healthy")
        .and_then(Value::as_bool);
    let cluster_available = status
        .pointer("/cluster/database_available")
        .and_then(Value::as_bool);
    if client_available != Some(true)
        || client_healthy != Some(true)
        || cluster_available == Some(false)
    {
        return Err(format!(
            "{name} does not report a healthy, available FoundationDB database"
        ));
    }
    Ok(())
}

fn capture_rustfs_health(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    name: &str,
) -> Result<(), String> {
    let mut command = Command::new(&options.curl);
    command.args([
        "--silent",
        "--show-error",
        "--fail",
        "--output",
        "/dev/null",
        "--write-out",
        "%{http_code}\n",
        &options.rustfs_health_url,
    ]);
    let output = run_checked_command(evidence, name, &mut command)?;
    if output.trim() != "200" {
        return Err(format!(
            "{name} returned unexpected HTTP status {:?}",
            output.trim()
        ));
    }
    Ok(())
}

fn run_checked_command(
    evidence: &EvidenceBundle,
    name: &str,
    command: &mut Command,
) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("cannot run qualification command {name:?}: {error}"))?;
    evidence.write_bytes(format!("commands/{name}.stdout"), &output.stdout)?;
    evidence.write_bytes(format!("commands/{name}.stderr"), &output.stderr)?;
    evidence.write_json(
        format!("commands/{name}.status.json"),
        &CommandStatus {
            success: output.status.success(),
            code: output.status.code(),
        },
    )?;
    if !output.status.success() {
        return Err(format!(
            "qualification command {name:?} exited with {:?}",
            output.status.code()
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| format!("qualification command {name:?} stdout is not UTF-8"))
}

fn command_stdout(command: &mut Command) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("cannot run environment command: {error}"))?;
    if !output.status.success() {
        return Err("environment command did not exit successfully".to_owned());
    }
    String::from_utf8(output.stdout)
        .map_err(|_| "environment command stdout is not UTF-8".to_owned())
}

#[derive(Serialize)]
struct CommandStatus {
    success: bool,
    code: Option<i32>,
}

fn wait_for_route(
    endpoint: SocketAddr,
    root_id: RootIdentity,
    timeout: Duration,
    process: &str,
    processes: &mut ProcessSet,
) -> Result<DiscoveredRoute, String> {
    let transport = RecordingTransport::new(Duration::from_secs(1))?;
    let request = encode_request(&RpcRequest::DiscoverRoute(DiscoverRouteRequest { root_id }))
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "route polling deadline overflowed".to_owned())?;
    let mut last_error = "no discovery attempt was made".to_owned();
    while Instant::now() < deadline {
        processes.require_running(process)?;
        match nokv_client::RpcTransport::round_trip(&transport, endpoint, &request) {
            Ok(response) => match decode_response(&response) {
                Ok(RpcResponse::DiscoverRoute(response)) if response.root_id == root_id => {
                    match response.outcome {
                        DiscoverRouteOutcome::Found(route) => return Ok(route),
                        DiscoverRouteOutcome::Failure(failure) => last_error = failure.message,
                    }
                }
                Ok(_) => last_error = "seed returned a mismatching response kind".to_owned(),
                Err(error) => last_error = error.to_string(),
            },
            Err(error) => last_error = error.to_string(),
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "timed out waiting for {process} route at {endpoint}: {last_error}"
    ))
}

fn exercise_discovery_action(
    resolver: &SeedRouteResolver<RecordingTransport>,
    control: &DiscoveryControl,
    action: &str,
    root_id: RootIdentity,
    authoritative: &DiscoveredRoute,
) -> Result<(), String> {
    for _ in 0..4 {
        let resolved = resolver
            .resolve(root_id, true)
            .map_err(|error| format!("discovery fault {action:?} exhausted: {error}"))?;
        require_resolved(&resolved, authoritative)?;
        if control.saw_action(action) {
            return Ok(());
        }
    }
    Err(format!(
        "discovery fault {action:?} was not contacted within one seed rotation"
    ))
}

fn require_resolved(resolved: &ResolvedRoute, expected: &DiscoveredRoute) -> Result<(), String> {
    if resolved.route != expected.route()
        || resolved.session_generation != expected.session_generation
        || resolved.endpoint != expected.owner_endpoint.socket_addr()
    {
        return Err(
            "persistent resolver does not retain the expected authoritative route".to_owned(),
        );
    }
    Ok(())
}

fn qualify_initial_seed_order(
    events: &[TransportEvent],
    failed: SocketAddr,
    healthy: SocketAddr,
) -> Result<(), String> {
    let discoveries = events
        .iter()
        .filter(|event| event.request_kind == "discover_route")
        .collect::<Vec<_>>();
    if discoveries.len() < 2
        || discoveries[0].endpoint != failed
        || !discoveries[0].outcome.starts_with("error:")
        || discoveries[1].endpoint != healthy
        || discoveries[1].outcome != "success"
    {
        return Err(
            "initial discovery did not exercise failed-first ordered seed failover".to_owned(),
        );
    }
    Ok(())
}

fn qualify_endpoint_change(
    events: &[TransportEvent],
    initial: SocketAddr,
    successor: SocketAddr,
) -> Result<(), String> {
    let failed_initial = events.iter().any(|event| {
        event.request_kind == "workspace"
            && event.endpoint == initial
            && event.outcome.starts_with("error:")
    });
    let successful_successor = events.iter().any(|event| {
        event.request_kind == "workspace"
            && event.endpoint == successor
            && event.outcome == "success"
    });
    let refreshed = events
        .iter()
        .any(|event| event.request_kind == "discover_route");
    if !failed_initial || !refreshed || !successful_successor {
        return Err(
            "persistent client did not fail at A, refresh, and succeed through B".to_owned(),
        );
    }
    Ok(())
}

fn require_failed_endpoint(endpoint: SocketAddr) -> Result<(), String> {
    match TcpStream::connect_timeout(&endpoint, Duration::from_millis(100)) {
        Ok(_) => Err(format!(
            "configured failed seed endpoint {endpoint} unexpectedly accepts connections"
        )),
        Err(_) => Ok(()),
    }
}

fn run_id(options: &QualificationOptions) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-seed-qualification/run/v1\0");
    digest.update(options.source_revision.as_bytes());
    digest.update(now.to_be_bytes());
    digest.update(std::process::id().to_be_bytes());
    lowercase_hex(&digest.finalize()[..8])
}

fn identity_bytes(run_id: &str, domain: &str) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-seed-qualification/identity/v1\0");
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(run_id.as_bytes());
    let digest = digest.finalize();
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest[..16]);
    identity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_domains_are_stable_and_distinct() {
        assert_eq!(identity_bytes("run", "root"), identity_bytes("run", "root"));
        assert_ne!(
            identity_bytes("run", "root"),
            identity_bytes("run", "agent")
        );
    }

    #[test]
    fn metadata_url_percent_encodes_the_path_and_prefix() {
        let url = metadata_url(Path::new("/tmp/fdb cluster"), "gate-7").unwrap();
        assert_eq!(url, "fdb:///tmp/fdb%20cluster?prefix=gate-7");
    }
}
