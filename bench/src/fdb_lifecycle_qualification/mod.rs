/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Environment-gated FoundationDB lifecycle qualification.

mod inspection;
mod lost_delete_proxy;

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nokv_fdb::FDB_API_VERSION;
use nokv_protocol::RootIdentity;
use nokv_types::{ArtifactRevisionId, CommitId, RequestId, WorkspaceIncarnationId};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use url::Url;

use self::inspection::{
    completed_gc_count, has_commit_state, has_operation_phase, quarantined_candidate_count,
    retired_snapshot_count, LifecycleInspector, LifecycleSnapshot,
};
use self::lost_delete_proxy::{LostDeleteEvent, LostDeleteProxy};
use crate::fdb_live_runtime::{
    append_client_arguments, append_object_arguments, capture_health, command_stdout, metadata_url,
    require_unused_endpoint, run_checked_command, HealthOptions, LiveControl,
    ObjectProviderOptions,
};
use crate::qualification_runtime::{
    lowercase_hex, sha256_bytes, sha256_file, EvidenceBundle, ProcessExit, ProcessSet,
};

pub const LIVE_GATE_ENV: &str = "NOKV_FDB_LIFECYCLE_QUALIFICATION";

const RESULT_SCHEMA: &str = "nokv.fdb.lifecycle-qualification.result.v1";
const ENVIRONMENT_SCHEMA: &str = "nokv.fdb.lifecycle-qualification.environment.v1";
const WORKBENCH_ROOT: &str = "/agents/fdb-gate8/wb";
const SOURCE_WORKBENCH: &str = "fdb-gate8-source";
const RESTORED_WORKBENCH: &str = "fdb-gate8-restored";
const NORMAL_GC_WORKBENCH: &str = "fdb-gate8-gc-normal";
const AMBIGUOUS_GC_WORKBENCH: &str = "fdb-gate8-gc-ambiguous";
const SNAPSHOT_NAME: &str = "fdb-gate8-frozen";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QualificationOptions {
    pub candidate_binary: PathBuf,
    pub fdb_cluster_file: PathBuf,
    pub fdb_client_library: PathBuf,
    pub fdb_prefix_base: String,
    pub fdbcli: PathBuf,
    pub curl: PathBuf,
    pub object_endpoint: String,
    pub object_bucket: String,
    pub object_region: String,
    pub object_root_base: String,
    pub object_access_key_id: String,
    pub object_secret_access_key: String,
    pub rustfs_service_identity: String,
    pub rustfs_health_url: String,
    pub owner_endpoint: SocketAddr,
    pub proxy_endpoint: SocketAddr,
    pub evidence_dir: PathBuf,
    pub source_revision: String,
    pub source_dirty: bool,
    pub activation_timeout: Duration,
    pub operation_timeout: Duration,
    pub lifecycle_timeout: Duration,
}

impl QualificationOptions {
    pub fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.peekable();
        let mut candidate_binary = None;
        let mut fdb_cluster_file = None;
        let mut fdb_client_library = None;
        let mut fdb_prefix_base = None;
        let mut fdbcli = None;
        let mut curl = None;
        let mut object_endpoint = None;
        let mut object_bucket = None;
        let mut object_region = Some("us-east-1".to_owned());
        let mut object_root_base = None;
        let mut object_access_key_id = None;
        let mut object_secret_access_key = None;
        let mut rustfs_service_identity = None;
        let mut rustfs_health_url = None;
        let mut owner_endpoint = None;
        let mut proxy_endpoint = None;
        let mut evidence_dir = None;
        let mut source_revision = None;
        let mut source_dirty = None;
        let mut activation_timeout = Some(Duration::from_secs(30));
        let mut operation_timeout = Some(Duration::from_secs(30));
        let mut lifecycle_timeout = Some(Duration::from_secs(60));

        while let Some(flag) = arguments.next() {
            let value = next_value(&mut arguments, &flag)?;
            match flag.as_str() {
                "--candidate-binary" => candidate_binary = Some(PathBuf::from(value)),
                "--fdb-cluster-file" => fdb_cluster_file = Some(PathBuf::from(value)),
                "--fdb-client-library" => fdb_client_library = Some(PathBuf::from(value)),
                "--fdb-prefix-base" => fdb_prefix_base = Some(value),
                "--fdbcli" => fdbcli = Some(PathBuf::from(value)),
                "--curl" => curl = Some(PathBuf::from(value)),
                "--object-endpoint" => object_endpoint = Some(value),
                "--object-bucket" => object_bucket = Some(value),
                "--object-region" => object_region = Some(value),
                "--object-root-base" => object_root_base = Some(value),
                "--object-access-key-id" => object_access_key_id = Some(value),
                "--object-secret-access-key" => object_secret_access_key = Some(value),
                "--rustfs-service-identity" => rustfs_service_identity = Some(value),
                "--rustfs-health-url" => rustfs_health_url = Some(value),
                "--owner-endpoint" => owner_endpoint = Some(parse_endpoint(&flag, &value)?),
                "--proxy-endpoint" => proxy_endpoint = Some(parse_endpoint(&flag, &value)?),
                "--evidence-dir" => evidence_dir = Some(PathBuf::from(value)),
                "--source-revision" => source_revision = Some(value),
                "--source-dirty" => source_dirty = Some(parse_bool(&flag, &value)?),
                "--activation-timeout-seconds" => {
                    activation_timeout = Some(parse_duration(&flag, &value)?)
                }
                "--operation-timeout-seconds" => {
                    operation_timeout = Some(parse_duration(&flag, &value)?)
                }
                "--lifecycle-timeout-seconds" => {
                    lifecycle_timeout = Some(parse_duration(&flag, &value)?)
                }
                _ => {
                    return Err(format!(
                        "unknown qualification option {flag:?}\n{}",
                        usage()
                    ))
                }
            }
        }

        let options = Self {
            candidate_binary: required(candidate_binary, "--candidate-binary")?,
            fdb_cluster_file: required(fdb_cluster_file, "--fdb-cluster-file")?,
            fdb_client_library: required(fdb_client_library, "--fdb-client-library")?,
            fdb_prefix_base: required(fdb_prefix_base, "--fdb-prefix-base")?,
            fdbcli: required(fdbcli, "--fdbcli")?,
            curl: required(curl, "--curl")?,
            object_endpoint: required(object_endpoint, "--object-endpoint")?,
            object_bucket: required(object_bucket, "--object-bucket")?,
            object_region: required(object_region, "--object-region")?,
            object_root_base: required(object_root_base, "--object-root-base")?,
            object_access_key_id: required(object_access_key_id, "--object-access-key-id")?,
            object_secret_access_key: required(
                object_secret_access_key,
                "--object-secret-access-key",
            )?,
            rustfs_service_identity: required(
                rustfs_service_identity,
                "--rustfs-service-identity",
            )?,
            rustfs_health_url: required(rustfs_health_url, "--rustfs-health-url")?,
            owner_endpoint: required(owner_endpoint, "--owner-endpoint")?,
            proxy_endpoint: required(proxy_endpoint, "--proxy-endpoint")?,
            evidence_dir: required(evidence_dir, "--evidence-dir")?,
            source_revision: required(source_revision, "--source-revision")?,
            source_dirty: required(source_dirty, "--source-dirty")?,
            activation_timeout: required(activation_timeout, "--activation-timeout-seconds")?,
            operation_timeout: required(operation_timeout, "--operation-timeout-seconds")?,
            lifecycle_timeout: required(lifecycle_timeout, "--lifecycle-timeout-seconds")?,
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<(), String> {
        for (name, path) in [
            ("candidate binary", &self.candidate_binary),
            ("FDB cluster file", &self.fdb_cluster_file),
            ("FDB client library", &self.fdb_client_library),
            ("fdbcli", &self.fdbcli),
            ("curl", &self.curl),
        ] {
            if !path.is_absolute() || !path.is_file() {
                return Err(format!(
                    "{name} must be an existing absolute file: {}",
                    path.display()
                ));
            }
        }
        for (name, value) in [
            ("FDB prefix base", self.fdb_prefix_base.as_str()),
            ("object bucket", self.object_bucket.as_str()),
            ("object root base", self.object_root_base.as_str()),
            (
                "RustFS service identity",
                self.rustfs_service_identity.as_str(),
            ),
        ] {
            if value.is_empty() {
                return Err(format!("{name} must not be empty"));
            }
        }
        if self.owner_endpoint == self.proxy_endpoint {
            return Err("owner and proxy endpoints must differ".to_owned());
        }
        if self.evidence_dir.exists() {
            return Err(format!(
                "evidence directory already exists: {}",
                self.evidence_dir.display()
            ));
        }
        upstream_endpoint(&self.object_endpoint)?;
        Ok(())
    }
}

#[derive(Serialize)]
struct EnvironmentEvidence {
    schema: &'static str,
    run_id: String,
    source_revision: String,
    source_dirty: bool,
    candidate_binary: String,
    candidate_sha256: String,
    candidate_version: Value,
    qualification_binary: String,
    qualification_sha256: String,
    rust_toolchain: String,
    operating_system: &'static str,
    architecture: &'static str,
    fdb_api_version: i32,
    fdb_cluster_file: String,
    fdb_cluster_file_sha256: String,
    fdb_client_library: String,
    fdb_client_library_sha256: String,
    fdbcli_version: String,
    fdb_prefix: String,
    fdb_prefix_sha256: String,
    rustfs_service_identity: String,
    rustfs_health_url: String,
    object_endpoint: String,
    object_bucket: String,
    object_region: String,
    object_root: String,
    object_binding_sha256: String,
    owner_endpoint: SocketAddr,
    proxy_endpoint: SocketAddr,
    proxy_upstream: SocketAddr,
    proxy_contract: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioResult {
    scenario: &'static str,
    status: &'static str,
    detail: String,
}

#[derive(Serialize)]
struct TerminalResult {
    schema: &'static str,
    status: &'static str,
    error: Option<String>,
    scenarios: Vec<ScenarioResult>,
    lost_delete: Option<LostDeleteEvent>,
    process_exits: Vec<ProcessExit>,
}

#[derive(Clone, Debug, Serialize)]
struct OwnershipEvidence {
    logical_shard_id: String,
    route_state: String,
    owner_epoch: Option<u64>,
    session_generation: Option<u64>,
    owner_endpoint: Option<String>,
    heartbeat_sequence: Option<u64>,
}

struct RunState {
    processes: ProcessSet,
    scenarios: Vec<ScenarioResult>,
    lost_delete: Option<LostDeleteEvent>,
}

impl RunState {
    fn new() -> Self {
        Self {
            processes: ProcessSet::default(),
            scenarios: Vec::new(),
            lost_delete: None,
        }
    }

    fn pass(&mut self, scenario: &'static str, detail: impl Into<String>) {
        self.scenarios.push(ScenarioResult {
            scenario,
            status: "PASS",
            detail: detail.into(),
        });
    }
}

pub fn run(options: QualificationOptions) -> Result<PathBuf, String> {
    let evidence = EvidenceBundle::create(options.evidence_dir.clone())?;
    let run_id = run_id(&options);
    let prefix = format!("{}-gate8-{run_id}", options.fdb_prefix_base);
    let object_root = format!(
        "{}/gate8/{run_id}",
        options.object_root_base.trim_end_matches('/')
    );
    let metadata_url = metadata_url(&options.fdb_cluster_file, &prefix)?;
    let root = RootIdentity(identity(&run_id, "root"));
    let agent = identity(&run_id, "agent");
    let upstream = upstream_endpoint(&options.object_endpoint)?;
    let health = HealthOptions {
        fdbcli: options.fdbcli.clone(),
        fdb_cluster_file: options.fdb_cluster_file.clone(),
        curl: options.curl.clone(),
        rustfs_health_url: options.rustfs_health_url.clone(),
    };
    let mut state = RunState::new();

    let setup = require_unused_endpoint(options.owner_endpoint)
        .and_then(|()| require_unused_endpoint(options.proxy_endpoint))
        .and_then(|()| capture_health(&evidence, &health, "before"))
        .and_then(|()| {
            capture_environment(
                &evidence,
                &options,
                &run_id,
                &prefix,
                &object_root,
                upstream,
            )
        });
    if let Err(error) = setup {
        let result = TerminalResult {
            schema: RESULT_SCHEMA,
            status: "NOT_QUALIFIED",
            error: Some(error.clone()),
            scenarios: state.scenarios,
            lost_delete: None,
            process_exits: state.processes.reap_all(),
        };
        evidence.finalize(&result)?;
        return Err(error);
    }

    let proxy = LostDeleteProxy::start(options.proxy_endpoint, upstream)?;
    let candidate_objects = ObjectProviderOptions {
        endpoint: format!("http://{}", proxy.endpoint()),
        bucket: options.object_bucket.clone(),
        region: options.object_region.clone(),
        root: object_root,
        access_key_id: options.object_access_key_id.clone(),
        secret_access_key: options.object_secret_access_key.clone(),
    };

    let execution = execute(
        &evidence,
        &options,
        &metadata_url,
        &prefix,
        root,
        agent,
        &candidate_objects,
        &proxy,
        &mut state,
    );
    let process_exits = state.processes.reap_all();
    let proxy_result = proxy.finish();
    if state.lost_delete.is_none() {
        state.lost_delete = proxy_result.as_ref().ok().cloned();
    }
    let after = capture_health(&evidence, &health, "after");
    let final_error = execution
        .err()
        .or_else(|| proxy_result.err())
        .or_else(|| after.err());
    let status = if final_error.is_none() {
        "PASS"
    } else {
        "FAIL"
    };
    let result = TerminalResult {
        schema: RESULT_SCHEMA,
        status,
        error: final_error.clone(),
        scenarios: state.scenarios,
        lost_delete: state.lost_delete,
        process_exits,
    };
    evidence.finalize(&result)?;
    match final_error {
        None => Ok(options.evidence_dir),
        Some(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    metadata_url: &str,
    prefix: &str,
    root: RootIdentity,
    agent: [u8; 16],
    objects: &ObjectProviderOptions,
    proxy: &LostDeleteProxy,
    state: &mut RunState,
) -> Result<(), String> {
    run_format(evidence, options, metadata_url)?;
    run_provision(evidence, options, metadata_url, root, agent, objects)?;
    let control = LiveControl::open(&options.fdb_cluster_file, prefix, root)?;

    let mut owner = Command::new(&options.candidate_binary);
    owner
        .args(["--bind", &options.owner_endpoint.to_string()])
        .args(["--advertise-endpoint", &options.owner_endpoint.to_string()])
        .args(["--node-id", "fdb-gate8-owner"])
        .args(["--lifecycle-interval-millis", "50"]);
    append_object_arguments(&mut owner, objects);
    owner.args(["serve", "--meta-url", metadata_url]);
    state.processes.spawn("owner", &mut owner, evidence)?;
    let ownership = control.wait_for_serving(
        options.owner_endpoint,
        options.activation_timeout,
        "owner",
        &mut state.processes,
    )?;
    evidence.write_json("owners/serving.json", &ownership_evidence(&ownership))?;
    let route = control.root_route(root, &ownership)?;
    let inspector = LifecycleInspector::new(control.open_meta()?, route);
    let baseline = inspector.capture()?;
    evidence.write_json("snapshots/baseline.json", &baseline)?;

    public_lifecycle(evidence, options, root, objects)?;
    let public = wait_for_snapshot(&inspector, options.lifecycle_timeout, |snapshot| {
        has_operation_phase(snapshot, "Publish", "Published")
            && has_operation_phase(snapshot, "Restore", "Complete")
            && retired_snapshot_count(snapshot) > retired_snapshot_count(&baseline)
    })?;
    evidence.write_json("snapshots/public-lifecycle.json", &public)?;
    state.pass(
        "publication_restore_snapshot",
        "public CLI publication, snapshot renew/retire, and restore reached terminal FDB records",
    );

    let normal_gc_before = completed_gc_count(&public);
    create_disposable(
        evidence,
        options,
        root,
        objects,
        NORMAL_GC_WORKBENCH,
        "normal-delete.txt",
        "normal-gc-body\n",
        "normal-gc",
    )?;
    let normal_gc = wait_for_snapshot(&inspector, options.lifecycle_timeout, |snapshot| {
        completed_gc_count(snapshot) > normal_gc_before
    })?;
    evidence.write_json("snapshots/normal-gc.json", &normal_gc)?;
    state.pass(
        "revision_gc",
        "public exact-generation removal reached a Deleted GC operation through RustFS",
    );

    let quarantine_before = quarantined_candidate_count(&normal_gc);
    let ambiguous_generation = put_disposable(
        evidence,
        options,
        root,
        objects,
        AMBIGUOUS_GC_WORKBENCH,
        "ambiguous-delete.txt",
        "ambiguous-gc-body\n",
        "ambiguous-put",
    )?;
    proxy.arm()?;
    remove_disposable(
        evidence,
        options,
        root,
        AMBIGUOUS_GC_WORKBENCH,
        "ambiguous-delete.txt",
        ambiguous_generation,
        "ambiguous-remove",
    )?;
    let ambiguous = wait_for_snapshot(&inspector, options.lifecycle_timeout, |snapshot| {
        quarantined_candidate_count(snapshot) > quarantine_before
    })?;
    let event = wait_for_proxy(proxy, options.lifecycle_timeout)?;
    if !event.successful_upstream_delete || event.forwarded_response_bytes != 0 {
        return Err(
            "lost-delete proxy evidence is not one successful lost acknowledgement".to_owned(),
        );
    }
    evidence.write_json("faults/lost-delete.json", &event)?;
    evidence.write_json("snapshots/ambiguous-gc.json", &ambiguous)?;
    state.lost_delete = Some(event);
    state.pass(
        "ambiguous_delete_quarantine",
        "RustFS completed DELETE, the proxy dropped all response bytes, and FDB retained quarantine evidence",
    );

    let commit_id = CommitId::from_bytes(identity32("gate8-retire-commit", root.0));
    let revision_id =
        ArtifactRevisionId::from_bytes(identity(&lowercase_hex(&root.0), "retire-revision"));
    let workspace =
        WorkspaceIncarnationId::from_bytes(identity(&lowercase_hex(&root.0), "retire-workspace"));
    let request = RequestId::from_bytes(identity(&lowercase_hex(&root.0), "retire-request"));
    inspector.seed_zero_consumer_commit(commit_id, revision_id, workspace, request)?;
    let retired = wait_for_snapshot(&inspector, options.lifecycle_timeout, |snapshot| {
        has_commit_state(snapshot, commit_id, nokv_types::CommitState::Retired)
            && has_operation_phase(snapshot, "CommitRetire", "Complete")
    })?;
    evidence.write_json("snapshots/commit-retirement.json", &retired)?;
    state.pass(
        "commit_retirement",
        "the exact candidate discovered and completed a session-fenced zero-consumer commit retirement",
    );

    let final_ownership = control.observe()?;
    evidence.write_json(
        "owners/final-serving.json",
        &ownership_evidence(&final_ownership),
    )?;
    state.pass(
        "session_fence_continuity",
        "all lifecycle transitions remained under one exact Serving owner session",
    );
    Ok(())
}

fn public_lifecycle(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    root: RootIdentity,
    objects: &ObjectProviderOptions,
) -> Result<(), String> {
    workbench(
        evidence,
        options,
        root,
        objects,
        "source-create",
        "workbench_create",
        json!({"id": SOURCE_WORKBENCH}),
    )?;
    workbench(
        evidence,
        options,
        root,
        objects,
        "source-put",
        "workbench_put_file",
        json!({
            "id": SOURCE_WORKBENCH,
            "section": "input",
            "path": "source.txt",
            "text": "frozen-source\n",
            "content_type": "text/plain",
            "replace": false,
        }),
    )?;
    workbench(
        evidence,
        options,
        root,
        objects,
        "source-commit",
        "workbench_commit",
        json!({
            "id": SOURCE_WORKBENCH,
            "manifest": {"gate": 8, "state": "frozen"},
            "content_digest_uri": format!("sha256:{}", sha256_bytes(b"frozen-source\n")),
            "replace": false,
        }),
    )?;
    workbench(
        evidence,
        options,
        root,
        objects,
        "snapshot-mint",
        "workbench_snapshot",
        json!({"id": SOURCE_WORKBENCH, "name": SNAPSHOT_NAME, "ttl_days": 1, "reason": "Gate 8 frozen source"}),
    )?;
    workbench(
        evidence,
        options,
        root,
        objects,
        "source-replace",
        "workbench_put_file",
        json!({
            "id": SOURCE_WORKBENCH,
            "section": "input",
            "path": "source.txt",
            "text": "live-source\n",
            "content_type": "text/plain",
            "replace": true,
            "expected_generation": 1,
        }),
    )?;
    workbench(
        evidence,
        options,
        root,
        objects,
        "snapshot-renew",
        "workbench_snapshot_renew",
        json!({"id": SOURCE_WORKBENCH, "name": SNAPSHOT_NAME, "ttl_days": 2}),
    )?;
    workbench(
        evidence,
        options,
        root,
        objects,
        "snapshot-restore",
        "workbench_restore",
        json!({"id": SOURCE_WORKBENCH, "at_snapshot": SNAPSHOT_NAME, "destination_id": RESTORED_WORKBENCH}),
    )?;
    let restored = workbench(
        evidence,
        options,
        root,
        objects,
        "restored-read",
        "workbench_read",
        json!({"id": RESTORED_WORKBENCH, "section": "input", "path": "source.txt", "format": "structured"}),
    )?;
    if !restored.to_string().contains("frozen-source")
        || restored.to_string().contains("live-source")
    {
        return Err("restored snapshot did not preserve the frozen artifact body".to_owned());
    }
    workbench(
        evidence,
        options,
        root,
        objects,
        "snapshot-retire",
        "workbench_snapshot_retire",
        json!({"id": SOURCE_WORKBENCH, "name": SNAPSHOT_NAME, "reason": "Gate 8 complete"}),
    )?;
    let snapshots = workbench(
        evidence,
        options,
        root,
        objects,
        "snapshot-list",
        "workbench_snapshot_list",
        json!({"id": SOURCE_WORKBENCH}),
    )?;
    if !snapshots.to_string().contains("retired") {
        return Err("snapshot list did not expose the retired snapshot".to_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create_disposable(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    root: RootIdentity,
    objects: &ObjectProviderOptions,
    workbench_id: &str,
    path: &str,
    body: &str,
    label: &str,
) -> Result<(), String> {
    let generation = put_disposable(
        evidence,
        options,
        root,
        objects,
        workbench_id,
        path,
        body,
        &format!("{label}-put"),
    )?;
    remove_disposable(
        evidence,
        options,
        root,
        workbench_id,
        path,
        generation,
        &format!("{label}-remove"),
    )
}

#[allow(clippy::too_many_arguments)]
fn put_disposable(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    root: RootIdentity,
    objects: &ObjectProviderOptions,
    workbench_id: &str,
    path: &str,
    body: &str,
    label: &str,
) -> Result<u64, String> {
    workbench(
        evidence,
        options,
        root,
        objects,
        &format!("{label}-create"),
        "workbench_create",
        json!({"id": workbench_id}),
    )?;
    let value = workbench(
        evidence,
        options,
        root,
        objects,
        label,
        "workbench_put_file",
        json!({
            "id": workbench_id,
            "section": "outputs",
            "path": path,
            "text": body,
            "content_type": "text/plain",
            "replace": false,
        }),
    )?;
    value
        .get("generation")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{label} did not return a positive generation"))
}

fn remove_disposable(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    root: RootIdentity,
    workbench_id: &str,
    path: &str,
    generation: u64,
    label: &str,
) -> Result<(), String> {
    let request_id = lowercase_hex(&identity(label, "remove-request"));
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(["--root-id", &lowercase_hex(&root.0)])
        .args(["--seed", &options.owner_endpoint.to_string()])
        .args([
            "workspace-path",
            "remove",
            workbench_id,
            "outputs",
            path,
            "--expected-generation",
            &generation.to_string(),
            "--request-id",
            &request_id,
        ]);
    let output = run_checked_command(evidence, label, &mut command)?;
    let value: Value = serde_json::from_str(&output)
        .map_err(|error| format!("{label} output is not JSON: {error}"))?;
    if value.get("status").and_then(Value::as_str) != Some("success")
        || value.get("operation").and_then(Value::as_str) != Some("remove")
        || value
            .get("removed_artifact_revision_id")
            .and_then(Value::as_str)
            .is_none()
    {
        return Err(format!("{label} did not return a complete remove result"));
    }
    Ok(())
}

fn workbench(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    root: RootIdentity,
    objects: &ObjectProviderOptions,
    label: &str,
    tool: &str,
    arguments: Value,
) -> Result<Value, String> {
    let encoded = serde_json::to_string(&arguments)
        .map_err(|error| format!("cannot encode {label} arguments: {error}"))?;
    let mut command = Command::new(&options.candidate_binary);
    append_client_arguments(
        &mut command,
        root,
        options.owner_endpoint,
        WORKBENCH_ROOT,
        objects,
    );
    command.args(["workbench", tool, &encoded]);
    let output = run_checked_command(evidence, label, &mut command)?;
    serde_json::from_str(&output).map_err(|error| format!("{label} output is not JSON: {error}"))
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

fn run_provision(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    metadata_url: &str,
    root: RootIdentity,
    agent: [u8; 16],
    objects: &ObjectProviderOptions,
) -> Result<(), String> {
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(["--root-id", &lowercase_hex(&root.0)])
        .args(["--agent-id", &lowercase_hex(&agent)]);
    append_object_arguments(&mut command, objects);
    command.args(["provision", "--meta-url", metadata_url]);
    let output = run_checked_command(evidence, "provision", &mut command)?;
    let value: Value = serde_json::from_str(&output)
        .map_err(|error| format!("provision output is not JSON: {error}"))?;
    if value.get("preexisting") != Some(&Value::Bool(false))
        || value.get("provider").and_then(Value::as_str) != Some("foundationdb")
        || value.get("lifecycle").and_then(Value::as_str) != Some("ready")
    {
        return Err("provision did not create the fresh Ready root".to_owned());
    }
    Ok(())
}

fn capture_environment(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    run_id: &str,
    prefix: &str,
    object_root: &str,
    upstream: SocketAddr,
) -> Result<(), String> {
    let mut version = Command::new(&options.candidate_binary);
    version.args(["version", "--json"]);
    let version = run_checked_command(evidence, "candidate-version", &mut version)?;
    let candidate_version: Value = serde_json::from_str(&version)
        .map_err(|error| format!("candidate version output is not JSON: {error}"))?;
    if candidate_version.get("git_commit").and_then(Value::as_str)
        != Some(options.source_revision.as_str())
    {
        return Err("candidate git_commit does not match --source-revision".to_owned());
    }
    if options.source_dirty {
        return Err("a live qualification PASS requires --source-dirty false".to_owned());
    }
    let qualification = std::env::current_exe()
        .map_err(|error| format!("cannot resolve qualification executable: {error}"))?;
    let fdbcli_version = command_stdout(Command::new(&options.fdbcli).arg("--version"))?;
    let binding = format!(
        "{}\0{}\0{}\0{}",
        options.object_endpoint, options.object_bucket, options.object_region, object_root
    );
    evidence.write_json(
        "environment.json",
        &EnvironmentEvidence {
            schema: ENVIRONMENT_SCHEMA,
            run_id: run_id.to_owned(),
            source_revision: options.source_revision.clone(),
            source_dirty: options.source_dirty,
            candidate_binary: options.candidate_binary.display().to_string(),
            candidate_sha256: sha256_file(&options.candidate_binary)?,
            candidate_version,
            qualification_binary: qualification.display().to_string(),
            qualification_sha256: sha256_file(&qualification)?,
            rust_toolchain: command_stdout(Command::new("rustc").arg("--version"))?
                .trim()
                .to_owned(),
            operating_system: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            fdb_api_version: FDB_API_VERSION,
            fdb_cluster_file: options.fdb_cluster_file.display().to_string(),
            fdb_cluster_file_sha256: sha256_file(&options.fdb_cluster_file)?,
            fdb_client_library: options.fdb_client_library.display().to_string(),
            fdb_client_library_sha256: sha256_file(&options.fdb_client_library)?,
            fdbcli_version: fdbcli_version.trim().to_owned(),
            fdb_prefix: prefix.to_owned(),
            fdb_prefix_sha256: sha256_bytes(prefix.as_bytes()),
            rustfs_service_identity: options.rustfs_service_identity.clone(),
            rustfs_health_url: options.rustfs_health_url.clone(),
            object_endpoint: options.object_endpoint.clone(),
            object_bucket: options.object_bucket.clone(),
            object_region: options.object_region.clone(),
            object_root: object_root.to_owned(),
            object_binding_sha256: sha256_bytes(binding.as_bytes()),
            owner_endpoint: options.owner_endpoint,
            proxy_endpoint: options.proxy_endpoint,
            proxy_upstream: upstream,
            proxy_contract: "forwards the selected DELETE to RustFS, retains a successful response digest, forwards zero response bytes, then closes the candidate connection",
        },
    )
}

fn wait_for_snapshot(
    inspector: &LifecycleInspector,
    timeout: Duration,
    predicate: impl Fn(&LifecycleSnapshot) -> bool,
) -> Result<LifecycleSnapshot, String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "lifecycle polling deadline overflows".to_owned())?;
    let mut last = None;
    while Instant::now() < deadline {
        let snapshot = inspector.capture()?;
        if predicate(&snapshot) {
            return Ok(snapshot);
        }
        last = Some(snapshot);
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "timed out waiting for lifecycle state; last read version was {:?}",
        last.map(|snapshot| snapshot.read_version)
    ))
}

fn wait_for_proxy(proxy: &LostDeleteProxy, timeout: Duration) -> Result<LostDeleteEvent, String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "proxy polling deadline overflows".to_owned())?;
    while Instant::now() < deadline {
        if let Some(event) = proxy.event()? {
            return Ok(event);
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err("timed out waiting for the selected RustFS DELETE".to_owned())
}

fn ownership_evidence(snapshot: &nokv_control::OwnershipSnapshot) -> OwnershipEvidence {
    OwnershipEvidence {
        logical_shard_id: lowercase_hex(snapshot.route().logical_shard_id().as_bytes()),
        route_state: format!("{:?}", snapshot.route().state()),
        owner_epoch: snapshot.route().owner_epoch().map(|value| value.get()),
        session_generation: snapshot
            .session()
            .map(|session| session.session_generation().get()),
        owner_endpoint: snapshot
            .route()
            .endpoint()
            .map(|endpoint| endpoint.as_str().to_owned()),
        heartbeat_sequence: snapshot
            .heartbeat()
            .map(|heartbeat| heartbeat.sequence().get()),
    }
}

fn upstream_endpoint(endpoint: &str) -> Result<SocketAddr, String> {
    let url = Url::parse(endpoint).map_err(|error| format!("invalid object endpoint: {error}"))?;
    if url.scheme() != "http"
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "Gate 8 object endpoint must be an HTTP origin without path, query, or fragment"
                .to_owned(),
        );
    }
    let host = url
        .host_str()
        .ok_or_else(|| "object endpoint has no host".to_owned())?;
    let port = url.port_or_known_default().unwrap_or(80);
    let mut endpoints = (host, port)
        .to_socket_addrs()
        .map_err(|error| format!("cannot resolve object endpoint: {error}"))?;
    let first = endpoints
        .next()
        .ok_or_else(|| "object endpoint resolved to no addresses".to_owned())?;
    if endpoints.any(|endpoint| endpoint != first) {
        return Err(
            "object endpoint resolves to multiple addresses; pin one qualification address"
                .to_owned(),
        );
    }
    Ok(first)
}

fn run_id(options: &QualificationOptions) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-lifecycle-qualification/run/v1\0");
    digest.update(options.source_revision.as_bytes());
    digest.update(now.to_be_bytes());
    digest.update(std::process::id().to_be_bytes());
    lowercase_hex(&digest.finalize()[..8])
}

fn identity(run_id: &str, domain: &str) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-lifecycle-qualification/identity/v1\0");
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(run_id.as_bytes());
    digest.finalize()[..16]
        .try_into()
        .expect("SHA-256 has sixteen bytes")
}

fn identity32(domain: &str, input: [u8; 16]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-lifecycle-qualification/identity32/v1\0");
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(input);
    digest.finalize().into()
}

fn next_value(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn required<T>(value: Option<T>, flag: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("missing required option {flag}\n{}", usage()))
}

fn parse_endpoint(flag: &str, value: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|error| format!("{flag} has invalid socket address {value:?}: {error}"))
}

fn parse_bool(flag: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{flag} must be true or false")),
    }
}

fn parse_duration(flag: &str, value: &str) -> Result<Duration, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|error| format!("{flag} has invalid seconds {value:?}: {error}"))?;
    if seconds == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(Duration::from_secs(seconds))
}

fn usage() -> &'static str {
    "required: --candidate-binary PATH --fdb-cluster-file PATH --fdb-client-library PATH \
--fdb-prefix-base PREFIX --fdbcli PATH --curl PATH --object-endpoint URL \
--object-bucket NAME --object-root-base PREFIX --object-access-key-id VALUE \
--object-secret-access-key VALUE --rustfs-service-identity VALUE \
--rustfs-health-url URL --owner-endpoint HOST:PORT --proxy-endpoint HOST:PORT \
--evidence-dir PATH --source-revision SHA --source-dirty true|false"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn identities_are_stable_and_domain_separated() {
        assert_eq!(identity("run", "root"), identity("run", "root"));
        assert_ne!(identity("run", "root"), identity("run", "agent"));
    }

    #[test]
    fn object_endpoint_requires_one_plain_http_origin() {
        assert_eq!(
            upstream_endpoint("http://127.0.0.1:9000").unwrap(),
            "127.0.0.1:9000".parse().unwrap()
        );
        assert!(upstream_endpoint("https://127.0.0.1:9000").is_err());
        assert!(upstream_endpoint("http://127.0.0.1:9000/path").is_err());
    }

    #[test]
    fn option_names_are_unique() {
        let flags = usage()
            .split_whitespace()
            .filter(|value| value.starts_with("--"))
            .collect::<Vec<_>>();
        assert_eq!(
            flags.len(),
            flags.iter().copied().collect::<BTreeSet<_>>().len()
        );
    }
}
