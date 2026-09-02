/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nokv_client::{
    ClientOptions, FramedTcpOptions, FramedTcpTransport, StaticRouteResolver, WorkspaceClient,
};
use nokv_control::{DistributedControlStore, OwnerSession, OwnershipSnapshot, ShardRouteState};
use nokv_control_fdb::{FdbControlOptions, FdbControlStore, FdbSessionFence};
use nokv_fdb::{FdbRuntime, FDB_API_VERSION};
use nokv_meta_fdb::{FdbMetadataSessionFence, FdbOptions, FdbStore};
use nokv_meta_store::{Commit, Key, Keyspace, Mutation, StoreError, TxnStore, WriteTxn};
use nokv_protocol::{
    CreateWorkspaceRequest, GetWorkspaceRequest, LogicalShardIdentity, ObjectNamespaceIdentity,
    RootIdentity, RootRoute, WorkbenchName, WorkspaceIdentity,
};
use nokv_types::RootId;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use url::Url;

use super::scenario::{require_state, require_successor, OwnershipEvidence, ScenarioResult};
use super::QualificationOptions;
use crate::qualification_runtime::{
    lowercase_hex, sha256_bytes, sha256_file, unix_millis, EvidenceBundle, ProcessExit, ProcessSet,
};

const RESULT_SCHEMA: &str = "nokv.fdb.serve-qualification.result.v1";
const ENVIRONMENT_SCHEMA: &str = "nokv.fdb.serve-qualification.environment.v1";
const CONTROL_LEASE_TTL: Duration = Duration::from_secs(10);
const STALE_WRITE_KEYSPACE: u16 = u16::MAX;

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
    monotonic_clock: &'static str,
    fdb_api_version: i32,
    fdb_cluster_file: String,
    fdb_cluster_file_sha256: String,
    fdb_client_library: String,
    fdb_client_library_sha256: String,
    fdb_prefix: String,
    fdb_prefix_sha256: String,
    fdbcli_version: String,
    fault_controller: String,
    fault_controller_sha256: String,
    fault_controller_contract: &'static str,
    rustfs_service_identity: String,
    rustfs_health_url: String,
    object_endpoint: String,
    object_bucket: String,
    object_region: String,
    object_root: String,
    object_namespace_binding_sha256: String,
    owner_endpoints: [SocketAddr; 4],
    activation_timeout_seconds: u64,
    takeover_timeout_seconds: u64,
    operation_timeout_seconds: u64,
    renewal_failure_timeout_seconds: u64,
    recovery_timeout_seconds: u64,
}

#[derive(Serialize)]
struct TerminalResult {
    schema: &'static str,
    status: &'static str,
    error: Option<String>,
    scenarios: Vec<ScenarioResult>,
    ownership: Vec<NamedOwnership>,
    process_exits: Vec<ProcessExit>,
    timeline: Vec<TimelineEvent>,
}

#[derive(Clone, Debug, Serialize)]
struct TimelineEvent {
    event: String,
    unix_millis: u64,
}

#[derive(Clone, Debug, Serialize)]
struct NamedOwnership {
    name: String,
    ownership: OwnershipEvidence,
}

#[derive(Clone, Debug, Serialize)]
struct MutationEvidence {
    label: String,
    create_request_id: String,
    create_replayed: bool,
    read_replayed: bool,
    workbench: String,
    workspace_incarnation_id: String,
    workspace_revision: u64,
    commit_version: u64,
    owner_epoch: u64,
    session_generation: u64,
    owner_endpoint: String,
}

#[derive(Clone, Debug)]
struct CommittedWorkspace {
    workbench: WorkbenchName,
    workspace_incarnation_id: WorkspaceIdentity,
    commit_version: u64,
}

#[derive(Clone, Debug, Serialize)]
struct RetainedMutationEvidence {
    label: String,
    workbench: String,
    workspace_incarnation_id: String,
    original_commit_version: u64,
    successor_workspace_revision: u64,
    successor_owner_epoch: u64,
    successor_session_generation: u64,
    successor_owner_endpoint: String,
}

#[derive(Clone, Debug, Serialize)]
struct StaleWriteEvidence {
    expected_owner_epoch: u64,
    expected_session_generation: u64,
    outcome: &'static str,
}

struct RunState {
    processes: ProcessSet,
    scenarios: Vec<ScenarioResult>,
    ownership: Vec<NamedOwnership>,
    timeline: Vec<TimelineEvent>,
}

impl RunState {
    fn new() -> Self {
        Self {
            processes: ProcessSet::default(),
            scenarios: Vec::new(),
            ownership: Vec::new(),
            timeline: Vec::new(),
        }
    }

    fn record(&mut self, event: impl Into<String>) {
        self.timeline.push(TimelineEvent {
            event: event.into(),
            unix_millis: unix_millis(),
        });
    }

    fn record_ownership(
        &mut self,
        evidence: &EvidenceBundle,
        name: &str,
        snapshot: &OwnershipSnapshot,
    ) -> Result<(), String> {
        let ownership = OwnershipEvidence::from(snapshot);
        evidence.write_json(format!("snapshots/{name}.json"), &ownership)?;
        self.ownership.push(NamedOwnership {
            name: name.to_owned(),
            ownership,
        });
        Ok(())
    }
}

struct FaultController {
    executable: PathBuf,
    active: bool,
    sequence: u64,
}

impl FaultController {
    fn new(executable: PathBuf) -> Self {
        Self {
            executable,
            active: false,
            sequence: 0,
        }
    }

    fn outage(&mut self, evidence: &EvidenceBundle) -> Result<(), String> {
        if self.active {
            return Err("FoundationDB outage is already active".to_owned());
        }
        self.active = true;
        self.invoke(evidence, "outage")
    }

    fn recover(&mut self, evidence: &EvidenceBundle) -> Result<(), String> {
        if !self.active {
            return Ok(());
        }
        self.invoke(evidence, "recover")?;
        self.active = false;
        Ok(())
    }

    fn invoke(&mut self, evidence: &EvidenceBundle, action: &str) -> Result<(), String> {
        self.sequence = self.sequence.saturating_add(1);
        let name = format!("{action}-{:02}", self.sequence);
        let output = Command::new(&self.executable)
            .arg(action)
            .output()
            .map_err(|error| format!("cannot run FDB fault controller {action:?}: {error}"))?;
        evidence.write_bytes(format!("faults/{name}.stdout"), &output.stdout)?;
        evidence.write_bytes(format!("faults/{name}.stderr"), &output.stderr)?;
        evidence.write_json(
            format!("faults/{name}.status.json"),
            &CommandStatus {
                success: output.status.success(),
                code: output.status.code(),
            },
        )?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "FDB fault controller {action:?} exited with {:?}",
                output.status.code()
            ))
        }
    }
}

impl Drop for FaultController {
    fn drop(&mut self) {
        if self.active {
            let _ = Command::new(&self.executable).arg("recover").status();
            self.active = false;
        }
    }
}

struct LiveControl {
    runtime: FdbRuntime,
    store: FdbControlStore,
    root: nokv_control::RootCatalogEntry,
    prefix: String,
    cluster_file: PathBuf,
}

impl LiveControl {
    fn open(cluster_file: &Path, prefix: &str, root_id: RootIdentity) -> Result<Self, String> {
        let runtime = FdbRuntime::start().map_err(|error| error.to_string())?;
        let options = FdbControlOptions::new(cluster_file, prefix)
            .and_then(|options| options.with_lease_ttl(CONTROL_LEASE_TTL))
            .map_err(|error| error.to_string())?;
        let manifest = FdbControlStore::inspect_manifest(&runtime, &options)
            .map_err(|error| error.to_string())?;
        let store = FdbControlStore::open(&runtime, options, manifest)
            .map_err(|error| error.to_string())?;
        let root = store
            .get_root_catalog(&RootId::from_bytes(root_id.0))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "provisioned root is absent from the FDB catalog".to_owned())?;
        Ok(Self {
            runtime,
            store,
            root,
            prefix: prefix.to_owned(),
            cluster_file: cluster_file.to_path_buf(),
        })
    }

    fn observe(&self) -> Result<OwnershipSnapshot, String> {
        self.store
            .observe_ownership(&self.root.logical_shard_id())
            .map_err(|error| error.to_string())
    }

    fn wait_for_state(
        &self,
        state: ShardRouteState,
        endpoint: SocketAddr,
        timeout: Duration,
        process: &str,
        processes: &mut ProcessSet,
    ) -> Result<OwnershipSnapshot, String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "ownership polling deadline overflowed".to_owned())?;
        let mut last = "no ownership observation was made".to_owned();
        while Instant::now() < deadline {
            processes.require_running(process)?;
            match self
                .store
                .get_route(&self.root.logical_shard_id())
                .map_err(|error| error.to_string())
            {
                Ok(Some(route))
                    if route.state() == state
                        && route.endpoint().is_some_and(|value| {
                            value.as_str().parse::<SocketAddr>() == Ok(endpoint)
                        }) =>
                {
                    let snapshot = self.observe()?;
                    if snapshot.route().state() == state
                        && snapshot.route().endpoint().is_some_and(|value| {
                            value.as_str().parse::<SocketAddr>() == Ok(endpoint)
                        })
                    {
                        return Ok(snapshot);
                    }
                    last = format!("route changed while confirming {state:?}");
                }
                Ok(Some(route)) => {
                    last = format!(
                        "observed {:?} at {:?}",
                        route.state(),
                        route.endpoint().map(|endpoint| endpoint.as_str())
                    );
                }
                Ok(None) => last = "route is absent".to_owned(),
                Err(error) => last = error,
            }
            thread::sleep(Duration::from_millis(2));
        }
        Err(format!(
            "timed out waiting for {process} to publish {state:?} at {endpoint}: {last}"
        ))
    }

    fn wait_for_heartbeat_advance(
        &self,
        initial: &OwnershipSnapshot,
        timeout: Duration,
        process: &str,
        processes: &mut ProcessSet,
    ) -> Result<OwnershipSnapshot, String> {
        let initial_session = initial
            .session()
            .ok_or_else(|| "initial heartbeat snapshot has no live session".to_owned())?;
        let initial_sequence = initial
            .heartbeat()
            .ok_or_else(|| "initial heartbeat snapshot has no heartbeat".to_owned())?
            .sequence();
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "heartbeat polling deadline overflowed".to_owned())?;
        while Instant::now() < deadline {
            processes.require_running(process)?;
            let current = self.observe()?;
            if current.session() != Some(initial_session) {
                return Err("owner session changed while waiting for heartbeat renewal".to_owned());
            }
            if current
                .heartbeat()
                .is_some_and(|heartbeat| heartbeat.sequence() > initial_sequence)
            {
                return Ok(current);
            }
            thread::sleep(Duration::from_millis(25));
        }
        Err(format!(
            "timed out waiting for {process} steady-state heartbeat renewal"
        ))
    }

    fn open_stale_store(&self, session: &OwnerSession) -> Result<FdbStore, String> {
        let fence = FdbSessionFence::new(self.store.keys(), session.clone())
            .map_err(|error| error.to_string())?;
        let metadata_fence = FdbMetadataSessionFence::new(
            fence.key(),
            fence.expected_value(),
            session.owner_epoch().get(),
            session.session_generation().get(),
        )
        .map_err(|error| error.to_string())?;
        FdbStore::open(
            &self.runtime,
            FdbOptions::new(
                &self.cluster_file,
                self.prefix.as_bytes().to_vec(),
                metadata_fence,
            ),
        )
        .map_err(|error| error.to_string())
    }

    fn root_route(
        &self,
        root_id: RootIdentity,
        snapshot: &OwnershipSnapshot,
    ) -> Result<RootRoute, String> {
        let route = snapshot.route();
        let owner_epoch = route
            .owner_epoch()
            .ok_or_else(|| "serving route has no owner epoch".to_owned())?;
        Ok(RootRoute {
            root_id,
            logical_shard_id: LogicalShardIdentity::from(self.root.logical_shard_id()),
            object_namespace_id: ObjectNamespaceIdentity::from(self.root.object_namespace_id()),
            placement_generation: self.root.placement_generation().get(),
            owner_epoch: owner_epoch.get(),
        })
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
    let mut fault = FaultController::new(options.fault_controller.clone());

    let execution = capture_health_pair(&evidence, &options, "run-before")
        .and_then(|()| capture_environment(&evidence, &options, &run_id, &fdb_prefix, &object_root))
        .and_then(|()| run_format(&evidence, &options, &metadata_url))
        .and_then(|()| {
            run_provision(
                &evidence,
                &options,
                &metadata_url,
                &object_root,
                root_id,
                agent_id,
            )
        })
        .and_then(|()| {
            let control = LiveControl::open(&options.fdb_cluster_file, &fdb_prefix, root_id)?;
            execute_live(
                &evidence,
                &options,
                &run_id,
                &metadata_url,
                &object_root,
                root_id,
                &control,
                &mut state,
                &mut fault,
            )
        });

    let recovery = fault.recover(&evidence);
    let process_exits = state.processes.reap_all();
    state.record("processes_reaped");
    let _ = evidence.write_json("owners/processes.json", &process_exits);
    let _ = evidence.write_json("scenarios.json", &state.scenarios);
    let _ = evidence.write_json("snapshots/ownership.json", &state.ownership);
    let _ = evidence.write_json("timeline.json", &state.timeline);
    let after_health = wait_for_fdb_health(&options, options.recovery_timeout)
        .and_then(|()| capture_health_pair(&evidence, &options, "run-after"));
    let outcome = combine_outcomes([execution, recovery, after_health]);
    let result = TerminalResult {
        schema: RESULT_SCHEMA,
        status: if outcome.is_ok() { "PASS" } else { "FAIL" },
        error: outcome.as_ref().err().cloned(),
        scenarios: state.scenarios,
        ownership: state.ownership,
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
    control: &LiveControl,
    state: &mut RunState,
    fault: &mut FaultController,
) -> Result<(), String> {
    for endpoint in [
        options.owner_a_endpoint,
        options.owner_b_endpoint,
        options.owner_c_endpoint,
        options.owner_d_endpoint,
    ] {
        require_unused_endpoint(endpoint)?;
    }

    capture_health_pair(evidence, options, "pre-activation-crash-before")?;
    let mut owner_a = owner_command(
        options,
        metadata_url,
        object_root,
        options.owner_a_endpoint,
        &format!("gate6-pre-a-{run_id}"),
    );
    state
        .processes
        .spawn("owner-a-pre-activation", &mut owner_a, evidence)?;
    state.record("owner_a_started");
    let owner_a = control.wait_for_state(
        ShardRouteState::Activating,
        options.owner_a_endpoint,
        options.activation_timeout,
        "owner-a-pre-activation",
        &mut state.processes,
    )?;
    state.record_ownership(evidence, "owner-a-activating", &owner_a)?;
    state.processes.terminate("owner-a-pre-activation")?;
    state.record("owner_a_killed_while_activating");
    thread::sleep(Duration::from_millis(250));
    let owner_a_dead = control.observe()?;
    require_state(&owner_a_dead, ShardRouteState::Activating)?;
    if owner_a_dead.session() != owner_a.session()
        || owner_a_dead.heartbeat() != owner_a.heartbeat()
    {
        return Err("dead pre-activation owner changed its exact session or heartbeat".to_owned());
    }
    state.record_ownership(evidence, "owner-a-dead-activating", &owner_a_dead)?;
    capture_health_pair(evidence, options, "pre-activation-crash-after")?;
    state.scenarios.push(ScenarioResult::pass(
        "pre_activation_crash",
        "owner A was killed while Activating; its dead exact session never became Serving",
    ));

    capture_health_pair(evidence, options, "pre-activation-successor-before")?;
    let mut owner_b = owner_command(
        options,
        metadata_url,
        object_root,
        options.owner_b_endpoint,
        &format!("gate6-successor-b-{run_id}"),
    );
    state.processes.spawn("owner-b", &mut owner_b, evidence)?;
    state.record("owner_b_started");
    let owner_b = control.wait_for_state(
        ShardRouteState::Serving,
        options.owner_b_endpoint,
        options.takeover_timeout,
        "owner-b",
        &mut state.processes,
    )?;
    require_successor(&owner_a_dead, &owner_b)?;
    state.record_ownership(evidence, "owner-b-serving", &owner_b)?;
    let owner_b_steady = control.wait_for_heartbeat_advance(
        &owner_b,
        options.operation_timeout,
        "owner-b",
        &mut state.processes,
    )?;
    state.record_ownership(evidence, "owner-b-steady-renewal", &owner_b_steady)?;
    let owner_b_mutation = mutate_and_read(
        evidence,
        options,
        run_id,
        "before-post-activation-kill",
        root_id,
        control,
        &owner_b_steady,
    )?;
    capture_health_pair(evidence, options, "pre-activation-successor-after")?;
    state.scenarios.push(ScenarioResult::pass(
        "pre_activation_successor_takeover",
        format!(
            "owner epoch {} -> {}, session generation {} -> {}",
            owner_a_dead
                .session()
                .expect("checked above")
                .owner_epoch()
                .get(),
            owner_b_steady
                .session()
                .expect("checked above")
                .owner_epoch()
                .get(),
            owner_a_dead
                .session()
                .expect("checked above")
                .session_generation()
                .get(),
            owner_b_steady
                .session()
                .expect("checked above")
                .session_generation()
                .get()
        ),
    ));

    capture_health_pair(evidence, options, "post-activation-owner-loss-before")?;
    let stale_session = owner_b_steady
        .session()
        .ok_or_else(|| "owner B has no exact session".to_owned())?
        .clone();
    let stale_store = control.open_stale_store(&stale_session)?;
    state.processes.terminate("owner-b")?;
    state.record("owner_b_killed_after_committed_mutation");
    let mut owner_c = owner_command(
        options,
        metadata_url,
        object_root,
        options.owner_c_endpoint,
        &format!("gate6-successor-c-{run_id}"),
    );
    state.processes.spawn("owner-c", &mut owner_c, evidence)?;
    let owner_c = control.wait_for_state(
        ShardRouteState::Serving,
        options.owner_c_endpoint,
        options.takeover_timeout,
        "owner-c",
        &mut state.processes,
    )?;
    require_successor(&owner_b_steady, &owner_c)?;
    state.record_ownership(evidence, "owner-c-serving", &owner_c)?;
    read_committed_workspace(
        evidence,
        options,
        "after-post-activation-takeover",
        root_id,
        control,
        &owner_c,
        &owner_b_mutation,
    )?;
    capture_health_pair(evidence, options, "post-activation-owner-loss-after")?;
    state.scenarios.push(ScenarioResult::pass(
        "post_activation_owner_loss",
        "owner B died after a committed mutation; owner C advanced both fences and read back B's exact committed workspace",
    ));

    capture_health_pair(evidence, options, "stale-session-write-before")?;
    require_stale_write_fenced(evidence, &stale_store, &stale_session, run_id)?;
    let _owner_c_mutation = mutate_and_read(
        evidence,
        options,
        run_id,
        "after-post-activation-takeover",
        root_id,
        control,
        &owner_c,
    )?;
    capture_health_pair(evidence, options, "stale-session-write-after")?;
    state.scenarios.push(ScenarioResult::pass(
        "stale_session_write",
        "the retained owner B FDB metadata handle returned StoreError::Fenced after owner C takeover",
    ));

    capture_health_pair(evidence, options, "renewal-failure-before")?;
    let owner_c_steady = control.wait_for_heartbeat_advance(
        &owner_c,
        options.operation_timeout,
        "owner-c",
        &mut state.processes,
    )?;
    state.record_ownership(evidence, "owner-c-before-outage", &owner_c_steady)?;
    fault.outage(evidence)?;
    state.record("fdb_outage_injected");
    capture_fdb_status_raw(evidence, options, "renewal-failure-during-fdb-status")?;
    capture_rustfs_health(evidence, options, "renewal-failure-during-rustfs")?;
    state
        .processes
        .wait_for_exit("owner-c", options.renewal_failure_timeout)?;
    state.record("owner_c_exited_after_renewal_failure");
    require_typed_renewal_failure(evidence, "owner-c")?;
    require_unused_endpoint(options.owner_c_endpoint)?;
    require_rejected_mutation(
        options,
        run_id,
        root_id,
        control,
        &owner_c_steady,
        "after-renewal-failure",
    )?;

    fault.recover(evidence)?;
    state.record("fdb_outage_recovered");
    wait_for_fdb_health(options, options.recovery_timeout)?;
    let owner_c_dead = control.observe()?;
    state.record_ownership(evidence, "owner-c-after-recovery", &owner_c_dead)?;
    capture_health_pair(evidence, options, "renewal-failure-after")?;
    state.scenarios.push(ScenarioResult::pass(
        "renewal_failure_fail_close",
        "external FDB outage caused a typed control renewal failure; owner C closed admission and exited before another mutation",
    ));

    capture_health_pair(evidence, options, "final-successor-before")?;
    let mut owner_d = owner_command(
        options,
        metadata_url,
        object_root,
        options.owner_d_endpoint,
        &format!("gate6-successor-d-{run_id}"),
    );
    state.processes.spawn("owner-d", &mut owner_d, evidence)?;
    let owner_d = control.wait_for_state(
        ShardRouteState::Serving,
        options.owner_d_endpoint,
        options.takeover_timeout,
        "owner-d",
        &mut state.processes,
    )?;
    require_successor(&owner_c_steady, &owner_d)?;
    state.record_ownership(evidence, "owner-d-serving", &owner_d)?;
    let _owner_d_mutation = mutate_and_read(
        evidence,
        options,
        run_id,
        "after-renewal-failure-takeover",
        root_id,
        control,
        &owner_d,
    )?;
    capture_health_pair(evidence, options, "final-successor-after")?;
    state.scenarios.push(ScenarioResult::pass(
        "final_successor_mutation",
        "owner D advanced both session fences and completed a new mutation/read after FDB recovery",
    ));
    evidence.write_json("scenarios.json", &state.scenarios)?;
    state.record("qualification_completed");
    Ok(())
}

fn require_stale_write_fenced(
    evidence: &EvidenceBundle,
    store: &FdbStore,
    session: &OwnerSession,
    run_id: &str,
) -> Result<(), String> {
    let result = store.commit(WriteTxn {
        checks: Vec::new(),
        mutations: vec![Mutation::Put {
            key: Key::new(
                Keyspace::new(STALE_WRITE_KEYSPACE),
                format!("gate6/stale/{run_id}").into_bytes(),
            ),
            value: b"must-not-apply".to_vec(),
        }],
    });
    match result {
        Err(StoreError::Fenced {
            expected_owner_epoch,
            expected_session_generation,
        }) if expected_owner_epoch == session.owner_epoch().get()
            && expected_session_generation == session.session_generation().get() =>
        {
            evidence.write_json(
                "snapshots/stale-write.json",
                &StaleWriteEvidence {
                    expected_owner_epoch,
                    expected_session_generation,
                    outcome: "Fenced",
                },
            )?;
            Ok(())
        }
        Ok(Commit::Applied) => Err("stale owner metadata write was applied".to_owned()),
        Ok(Commit::Conflict) => {
            Err("stale owner metadata write returned Conflict, not Fenced".to_owned())
        }
        Err(error) => Err(format!(
            "stale owner metadata write returned {error}, expected exact Fenced"
        )),
    }
}

fn require_typed_renewal_failure(evidence: &EvidenceBundle, process: &str) -> Result<(), String> {
    let path = evidence.root().join(format!("owners/{process}.stderr"));
    let stderr = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if !stderr.contains("RPC server stopped: control plane failed") {
        return Err(format!(
            "{process} did not report a typed control-plane renewal failure"
        ));
    }
    Ok(())
}

fn require_rejected_mutation(
    options: &QualificationOptions,
    run_id: &str,
    root_id: RootIdentity,
    control: &LiveControl,
    snapshot: &OwnershipSnapshot,
    label: &str,
) -> Result<(), String> {
    let client = workspace_client(options, root_id, control, snapshot)?;
    let request = CreateWorkspaceRequest {
        workbench: WorkbenchName::new(format!("gate6-{label}-{run_id}"))
            .map_err(|error| error.to_string())?,
        workspace_incarnation_id: WorkspaceIdentity(identity_bytes(run_id, label)),
    };
    match client.create_workspace(client.new_request_id(), request) {
        Err(_) => Ok(()),
        Ok(_) => Err("owner accepted a mutation after typed renewal failure".to_owned()),
    }
}

fn mutate_and_read(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    run_id: &str,
    label: &str,
    root_id: RootIdentity,
    control: &LiveControl,
    snapshot: &OwnershipSnapshot,
) -> Result<CommittedWorkspace, String> {
    require_state(snapshot, ShardRouteState::Serving)?;
    let session = snapshot
        .session()
        .ok_or_else(|| "mutation owner has no exact session".to_owned())?;
    let client = workspace_client(options, root_id, control, snapshot)?;
    let workbench =
        WorkbenchName::new(format!("gate6-{label}-{run_id}")).map_err(|error| error.to_string())?;
    let workspace_incarnation_id = WorkspaceIdentity(identity_bytes(run_id, label));
    let create_request_id = client.new_request_id();
    let created = client
        .create_workspace(
            create_request_id,
            CreateWorkspaceRequest {
                workbench: workbench.clone(),
                workspace_incarnation_id,
            },
        )
        .map_err(|error| format!("{label} mutation failed: {error}"))?;
    let commit_version = created
        .commit_version
        .ok_or_else(|| format!("{label} mutation returned no commit version"))?;
    let read = client
        .get_workspace(GetWorkspaceRequest {
            workbench: workbench.clone(),
        })
        .map_err(|error| format!("{label} read-back failed: {error}"))?;
    if read.value != created.value
        || read.value.workspace_incarnation_id != workspace_incarnation_id
    {
        return Err(format!(
            "{label} read-back did not match the durable mutation"
        ));
    }
    evidence.write_json(
        format!("peers/workspace-{label}.json"),
        &MutationEvidence {
            label: label.to_owned(),
            create_request_id: lowercase_hex(&create_request_id.0),
            create_replayed: created.replayed,
            read_replayed: read.replayed,
            workbench: workbench.to_string(),
            workspace_incarnation_id: lowercase_hex(&workspace_incarnation_id.0),
            workspace_revision: created.value.workspace_revision,
            commit_version,
            owner_epoch: session.owner_epoch().get(),
            session_generation: session.session_generation().get(),
            owner_endpoint: snapshot
                .route()
                .endpoint()
                .expect("serving snapshot has endpoint")
                .as_str()
                .to_owned(),
        },
    )?;
    Ok(CommittedWorkspace {
        workbench,
        workspace_incarnation_id,
        commit_version,
    })
}

fn read_committed_workspace(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    label: &str,
    root_id: RootIdentity,
    control: &LiveControl,
    snapshot: &OwnershipSnapshot,
    expected: &CommittedWorkspace,
) -> Result<(), String> {
    require_state(snapshot, ShardRouteState::Serving)?;
    let session = snapshot
        .session()
        .ok_or_else(|| "successor owner has no exact session".to_owned())?;
    let client = workspace_client(options, root_id, control, snapshot)?;
    let read = client
        .get_workspace(GetWorkspaceRequest {
            workbench: expected.workbench.clone(),
        })
        .map_err(|error| format!("{label} retained-mutation read failed: {error}"))?;
    if read.value.workbench != expected.workbench
        || read.value.workspace_incarnation_id != expected.workspace_incarnation_id
    {
        return Err(format!(
            "{label} successor did not read the exact committed predecessor workspace"
        ));
    }
    evidence.write_json(
        format!("peers/retained-workspace-{label}.json"),
        &RetainedMutationEvidence {
            label: label.to_owned(),
            workbench: expected.workbench.to_string(),
            workspace_incarnation_id: lowercase_hex(&expected.workspace_incarnation_id.0),
            original_commit_version: expected.commit_version,
            successor_workspace_revision: read.value.workspace_revision,
            successor_owner_epoch: session.owner_epoch().get(),
            successor_session_generation: session.session_generation().get(),
            successor_owner_endpoint: snapshot
                .route()
                .endpoint()
                .expect("serving snapshot has endpoint")
                .as_str()
                .to_owned(),
        },
    )
}

fn workspace_client(
    options: &QualificationOptions,
    root_id: RootIdentity,
    control: &LiveControl,
    snapshot: &OwnershipSnapshot,
) -> Result<WorkspaceClient<FramedTcpTransport, StaticRouteResolver>, String> {
    let endpoint = snapshot
        .route()
        .endpoint()
        .ok_or_else(|| "serving route has no endpoint".to_owned())?
        .as_str()
        .parse::<SocketAddr>()
        .map_err(|error| format!("serving endpoint is invalid: {error}"))?;
    let transport = FramedTcpTransport::new(FramedTcpOptions {
        connect_timeout: options.operation_timeout.min(Duration::from_secs(2)),
        handshake_timeout: options.operation_timeout,
        read_timeout: options.operation_timeout,
        write_timeout: options.operation_timeout,
    })
    .map_err(|error| error.to_string())?;
    let route = control.root_route(root_id, snapshot)?;
    let resolver = StaticRouteResolver::new(route, endpoint).map_err(|error| error.to_string())?;
    WorkspaceClient::new(
        root_id,
        transport,
        resolver,
        ClientOptions { max_attempts: 2 },
    )
    .map_err(|error| error.to_string())
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
    let qualification_binary = std::env::current_exe()
        .map_err(|error| format!("cannot resolve qualification binary path: {error}"))?;
    let rust_toolchain = command_stdout(Command::new("rustc").arg("--version"))?;
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
            qualification_binary: qualification_binary.display().to_string(),
            qualification_sha256: sha256_file(&qualification_binary)?,
            rust_toolchain: rust_toolchain.trim().to_owned(),
            operating_system: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            monotonic_clock: "std::time::Instant",
            fdb_api_version: FDB_API_VERSION,
            fdb_cluster_file: options.fdb_cluster_file.display().to_string(),
            fdb_cluster_file_sha256: sha256_file(&options.fdb_cluster_file)?,
            fdb_client_library: options.fdb_client_library.display().to_string(),
            fdb_client_library_sha256: sha256_file(&options.fdb_client_library)?,
            fdb_prefix: fdb_prefix.to_owned(),
            fdb_prefix_sha256: sha256_bytes(fdb_prefix.as_bytes()),
            fdbcli_version: fdbcli_version.trim().to_owned(),
            fault_controller: options.fault_controller.display().to_string(),
            fault_controller_sha256: sha256_file(&options.fault_controller)?,
            fault_controller_contract:
                "argv[1] is exactly outage or recover; both actions are idempotent",
            rustfs_service_identity: options.rustfs_service_identity.clone(),
            rustfs_health_url: options.rustfs_health_url.clone(),
            object_endpoint: options.object_endpoint.clone(),
            object_bucket: options.object_bucket.clone(),
            object_region: options.object_region.clone(),
            object_root: object_root.to_owned(),
            object_namespace_binding_sha256: sha256_bytes(binding.as_bytes()),
            owner_endpoints: [
                options.owner_a_endpoint,
                options.owner_b_endpoint,
                options.owner_c_endpoint,
                options.owner_d_endpoint,
            ],
            activation_timeout_seconds: options.activation_timeout.as_secs(),
            takeover_timeout_seconds: options.takeover_timeout.as_secs(),
            operation_timeout_seconds: options.operation_timeout.as_secs(),
            renewal_failure_timeout_seconds: options.renewal_failure_timeout.as_secs(),
            recovery_timeout_seconds: options.recovery_timeout.as_secs(),
        },
    )
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
    endpoint: SocketAddr,
    node_id: &str,
) -> Command {
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(["--bind", &endpoint.to_string()])
        .args(["--advertise-endpoint", &endpoint.to_string()])
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

fn capture_health_pair(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    name: &str,
) -> Result<(), String> {
    capture_fdb_health(evidence, options, &format!("{name}-fdb"))?;
    capture_rustfs_health(evidence, options, &format!("{name}-rustfs"))
}

fn capture_fdb_health(
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
    if status
        .pointer("/client/database_status/available")
        .and_then(Value::as_bool)
        != Some(true)
        || status
            .pointer("/client/database_status/healthy")
            .and_then(Value::as_bool)
            != Some(true)
        || status
            .pointer("/cluster/database_available")
            .and_then(Value::as_bool)
            == Some(false)
    {
        return Err(format!(
            "{name} does not report a healthy, available FoundationDB database"
        ));
    }
    Ok(())
}

fn capture_fdb_status_raw(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    name: &str,
) -> Result<(), String> {
    let mut command = Command::new(&options.fdbcli);
    command
        .args(["-C"])
        .arg(&options.fdb_cluster_file)
        .args(["--exec", "status json"]);
    let _ = run_recorded_command(evidence, name, &mut command)?;
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

fn wait_for_fdb_health(options: &QualificationOptions, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "FDB recovery deadline overflowed".to_owned())?;
    let mut last = "no FDB health attempt was made".to_owned();
    while Instant::now() < deadline {
        let output = Command::new(&options.fdbcli)
            .args(["-C"])
            .arg(&options.fdb_cluster_file)
            .args(["--exec", "status json"])
            .output();
        match output {
            Ok(output) if output.status.success() => {
                match serde_json::from_slice::<Value>(&output.stdout) {
                    Ok(status)
                        if status
                            .pointer("/client/database_status/available")
                            .and_then(Value::as_bool)
                            == Some(true)
                            && status
                                .pointer("/client/database_status/healthy")
                                .and_then(Value::as_bool)
                                == Some(true) =>
                    {
                        return Ok(())
                    }
                    Ok(_) => last = "FDB status is not yet healthy".to_owned(),
                    Err(error) => last = error.to_string(),
                }
            }
            Ok(output) => last = format!("fdbcli exited with {:?}", output.status.code()),
            Err(error) => last = error.to_string(),
        }
        thread::sleep(Duration::from_millis(250));
    }
    Err(format!("timed out waiting for FDB recovery: {last}"))
}

fn run_checked_command(
    evidence: &EvidenceBundle,
    name: &str,
    command: &mut Command,
) -> Result<String, String> {
    let output = run_recorded_command(evidence, name, command)?;
    if !output.success {
        return Err(format!(
            "qualification command {name:?} exited with {:?}",
            output.code
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| format!("qualification command {name:?} stdout is not UTF-8"))
}

struct RecordedCommand {
    success: bool,
    code: Option<i32>,
    stdout: Vec<u8>,
}

fn run_recorded_command(
    evidence: &EvidenceBundle,
    name: &str,
    command: &mut Command,
) -> Result<RecordedCommand, String> {
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
    Ok(RecordedCommand {
        success: output.status.success(),
        code: output.status.code(),
        stdout: output.stdout,
    })
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

fn require_unused_endpoint(endpoint: SocketAddr) -> Result<(), String> {
    match TcpStream::connect_timeout(&endpoint, Duration::from_millis(100)) {
        Ok(_) => Err(format!(
            "qualification endpoint {endpoint} unexpectedly accepts connections"
        )),
        Err(_) => Ok(()),
    }
}

fn combine_outcomes<const N: usize>(outcomes: [Result<(), String>; N]) -> Result<(), String> {
    let failures = outcomes
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn run_id(options: &QualificationOptions) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-serve-qualification/run/v1\0");
    digest.update(options.source_revision.as_bytes());
    digest.update(now.to_be_bytes());
    digest.update(std::process::id().to_be_bytes());
    lowercase_hex(&digest.finalize()[..8])
}

fn identity_bytes(run_id: &str, domain: &str) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-serve-qualification/identity/v1\0");
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
    fn metadata_url_percent_encodes_path_and_prefix() {
        let url = metadata_url(Path::new("/tmp/fdb cluster"), "gate-6").unwrap();
        assert_eq!(url, "fdb:///tmp/fdb%20cluster?prefix=gate-6");
    }

    #[test]
    fn identity_domains_are_stable_and_distinct() {
        assert_eq!(identity_bytes("run", "root"), identity_bytes("run", "root"));
        assert_ne!(
            identity_bytes("run", "root"),
            identity_bytes("run", "agent")
        );
    }
}
