/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic real-commit lost-ack qualification for FoundationDB Gate 2.

mod control;
mod evidence;
mod metadata;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use evidence::{
    validate_injector_events, ChildResult, EnvironmentEvidence, InjectorEvent, Scenario,
    ScenarioEvidence, TerminalResult, REQUIRED_REPETITIONS,
};
use nokv_control::{
    AgentId, LogicalShardId, NodeId, ObjectNamespaceId, PlacementGeneration, RootCatalogEntry,
    RootId, RpcEndpoint, StoreId, StoreManifest, StoreProvider, SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_control_fdb::FdbControlOptions;
use nokv_fdb::{
    lexicographic_successor, FdbConnectionOptions, FdbDatabase, FdbRangeRequest, FdbRuntime,
    FdbStorePrefix, FDB_PHYSICAL_ENCODING_VERSION,
};
use nokv_types::{CommandDigest, OwnerEpoch, RequestId};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::qualification_runtime::{
    lowercase_hex, sha256_bytes, sha256_file, unix_millis, EvidenceBundle,
};

pub const LIVE_GATE_ENV: &str = "NOKV_FDB_UNKNOWN_OUTCOME_QUALIFICATION";
const LEASE_TTL_MILLIS: u64 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QualificationOptions {
    candidate_binary: PathBuf,
    qualification_binary: PathBuf,
    shim_library: PathBuf,
    fdb_cluster_file: PathBuf,
    fdb_client_library: PathBuf,
    fdbcli: PathBuf,
    curl: PathBuf,
    fdb_prefix_base: String,
    rustfs_health_url: String,
    rustfs_service_identity: String,
    evidence_dir: PathBuf,
    source_revision: String,
    source_dirty: bool,
}

impl QualificationOptions {
    pub fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.peekable();
        let mut candidate_binary = None;
        let mut shim_library = None;
        let mut fdb_cluster_file = None;
        let mut fdb_client_library = None;
        let mut fdbcli = None;
        let mut curl = None;
        let mut fdb_prefix_base = None;
        let mut rustfs_health_url = None;
        let mut rustfs_service_identity = None;
        let mut evidence_dir = None;
        let mut source_revision = None;
        let mut source_dirty = None;

        while let Some(flag) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| format!("missing value for {flag:?}\n{}", usage()))?;
            match flag.as_str() {
                "--candidate-binary" => candidate_binary = Some(PathBuf::from(value)),
                "--shim-library" => shim_library = Some(PathBuf::from(value)),
                "--fdb-cluster-file" => fdb_cluster_file = Some(PathBuf::from(value)),
                "--fdb-client-library" => fdb_client_library = Some(PathBuf::from(value)),
                "--fdbcli" => fdbcli = Some(PathBuf::from(value)),
                "--curl" => curl = Some(PathBuf::from(value)),
                "--fdb-prefix-base" => fdb_prefix_base = Some(value),
                "--rustfs-health-url" => rustfs_health_url = Some(value),
                "--rustfs-service-identity" => rustfs_service_identity = Some(value),
                "--evidence-dir" => evidence_dir = Some(PathBuf::from(value)),
                "--source-revision" => source_revision = Some(value),
                "--source-dirty" => source_dirty = Some(parse_bool(&flag, &value)?),
                _ => return Err(format!("unknown option {flag:?}\n{}", usage())),
            }
        }

        let qualification_binary = std::env::current_exe()
            .map_err(|error| format!("cannot resolve qualification binary path: {error}"))?;
        let options = Self {
            candidate_binary: required(candidate_binary, "--candidate-binary")?,
            qualification_binary,
            shim_library: required(shim_library, "--shim-library")?,
            fdb_cluster_file: required(fdb_cluster_file, "--fdb-cluster-file")?,
            fdb_client_library: required(fdb_client_library, "--fdb-client-library")?,
            fdbcli: required(fdbcli, "--fdbcli")?,
            curl: required(curl, "--curl")?,
            fdb_prefix_base: required(fdb_prefix_base, "--fdb-prefix-base")?,
            rustfs_health_url: required(rustfs_health_url, "--rustfs-health-url")?,
            rustfs_service_identity: required(
                rustfs_service_identity,
                "--rustfs-service-identity",
            )?,
            evidence_dir: required(evidence_dir, "--evidence-dir")?,
            source_revision: required(source_revision, "--source-revision")?,
            source_dirty: required(source_dirty, "--source-dirty")?,
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<(), String> {
        for (flag, path) in [
            ("--candidate-binary", &self.candidate_binary),
            ("qualification binary", &self.qualification_binary),
            ("--shim-library", &self.shim_library),
            ("--fdb-cluster-file", &self.fdb_cluster_file),
            ("--fdb-client-library", &self.fdb_client_library),
            ("--fdbcli", &self.fdbcli),
            ("--curl", &self.curl),
            ("--evidence-dir", &self.evidence_dir),
        ] {
            if !path.is_absolute() {
                return Err(format!("{flag} must be an absolute path"));
            }
        }
        for (flag, path) in [
            ("--candidate-binary", &self.candidate_binary),
            ("qualification binary", &self.qualification_binary),
            ("--shim-library", &self.shim_library),
            ("--fdb-cluster-file", &self.fdb_cluster_file),
            ("--fdb-client-library", &self.fdb_client_library),
            ("--fdbcli", &self.fdbcli),
            ("--curl", &self.curl),
        ] {
            if !path.is_file() {
                return Err(format!("{flag} must name an existing file"));
            }
        }
        if self.evidence_dir.exists() {
            return Err("--evidence-dir must not already exist".to_owned());
        }
        if self.fdb_prefix_base.is_empty()
            || self.fdb_prefix_base.len() > 24
            || !self
                .fdb_prefix_base
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(
                "--fdb-prefix-base must contain 1..=24 ASCII letters, digits, '-' or '_'"
                    .to_owned(),
            );
        }
        if self.source_revision.len() != 40
            || !self
                .source_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("--source-revision must be 40 lowercase hexadecimal characters".to_owned());
        }
        if self.rustfs_service_identity.trim().is_empty()
            || self.rustfs_service_identity.trim() != self.rustfs_service_identity
        {
            return Err("--rustfs-service-identity must be canonical and non-empty".to_owned());
        }
        let health = url::Url::parse(&self.rustfs_health_url)
            .map_err(|error| format!("--rustfs-health-url is invalid: {error}"))?;
        if !matches!(health.scheme(), "http" | "https") || health.host_str().is_none() {
            return Err("--rustfs-health-url must be an absolute HTTP(S) URL".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ChildOptions {
    scenario: Scenario,
    fdb_cluster_file: PathBuf,
    prefix: String,
    seed: String,
}

impl ChildOptions {
    fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        if arguments.next().as_deref() != Some("--child-operation") {
            return Err("child invocation must begin with --child-operation".to_owned());
        }
        let scenario = arguments
            .next()
            .ok_or_else(|| "missing child scenario".to_owned())?
            .parse()?;
        let mut fdb_cluster_file = None;
        let mut prefix = None;
        let mut seed = None;
        while let Some(flag) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| format!("missing child value for {flag:?}"))?;
            match flag.as_str() {
                "--fdb-cluster-file" => fdb_cluster_file = Some(PathBuf::from(value)),
                "--fdb-prefix" => prefix = Some(value),
                "--seed" => seed = Some(value),
                _ => return Err(format!("unknown child option {flag:?}")),
            }
        }
        let options = Self {
            scenario,
            fdb_cluster_file: required(fdb_cluster_file, "--fdb-cluster-file")?,
            prefix: required(prefix, "--fdb-prefix")?,
            seed: required(seed, "--seed")?,
        };
        if !options.fdb_cluster_file.is_absolute() || !options.fdb_cluster_file.is_file() {
            return Err("child FDB cluster file must be an existing absolute path".to_owned());
        }
        FdbStorePrefix::new(options.prefix.as_bytes()).map_err(|error| error.to_string())?;
        validate_token("child seed", &options.seed, 96)?;
        Ok(options)
    }
}

#[derive(Clone)]
pub(crate) struct ScenarioContext {
    pub(crate) cluster_file: PathBuf,
    pub(crate) prefix: String,
    pub(crate) identity: ScenarioIdentity,
}

#[derive(Clone)]
pub(crate) struct ScenarioIdentity {
    pub(crate) store_id: StoreId,
    pub(crate) shard_id: LogicalShardId,
    pub(crate) root_id: RootId,
    pub(crate) agent_id: AgentId,
    pub(crate) object_namespace_id: ObjectNamespaceId,
    pub(crate) placement_generation: PlacementGeneration,
    pub(crate) owner_a: NodeId,
    pub(crate) owner_b: NodeId,
    pub(crate) endpoint_a: RpcEndpoint,
    pub(crate) endpoint_b: RpcEndpoint,
    pub(crate) request_id: RequestId,
}

impl ScenarioContext {
    fn new(cluster_file: &Path, prefix: String, seed: &str) -> Result<Self, String> {
        Ok(Self {
            cluster_file: cluster_file.to_path_buf(),
            prefix,
            identity: ScenarioIdentity::new(seed)?,
        })
    }

    pub(crate) fn control_options(&self) -> Result<FdbControlOptions, String> {
        FdbControlOptions::new(&self.cluster_file, &self.prefix)
            .and_then(|options| {
                options.with_lease_ttl(std::time::Duration::from_millis(LEASE_TTL_MILLIS))
            })
            .map_err(|error| error.to_string())
    }

    pub(crate) fn manifest(&self) -> Result<StoreManifest, String> {
        let options = self.control_options()?;
        StoreManifest::new(
            self.identity.store_id,
            StoreProvider::FoundationDb,
            SUPPORTED_WORKSPACE_FORMAT_VERSION,
            FDB_PHYSICAL_ENCODING_VERSION,
            options.provider_namespace_digest(),
            "gate2-qualification",
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn root_entry(&self, state: nokv_control::CatalogEntryState) -> RootCatalogEntry {
        RootCatalogEntry::new(
            self.identity.root_id,
            self.identity.agent_id,
            self.identity.object_namespace_id,
            self.identity.shard_id,
            self.identity.placement_generation,
            state,
        )
    }
}

impl ScenarioIdentity {
    fn new(seed: &str) -> Result<Self, String> {
        let owner_suffix = &sha256_bytes(seed.as_bytes())[..12];
        Ok(Self {
            store_id: StoreId::from_bytes(identity_bytes(seed, "store")),
            shard_id: LogicalShardId::from_bytes(identity_bytes(seed, "shard")),
            root_id: RootId::from_bytes(identity_bytes(seed, "root")),
            agent_id: AgentId::from_bytes(identity_bytes(seed, "agent")),
            object_namespace_id: ObjectNamespaceId::from_bytes(identity_bytes(seed, "object")),
            placement_generation: PlacementGeneration::new(1).expect("one is nonzero"),
            owner_a: NodeId::new(format!("gate2-a-{owner_suffix}"))
                .map_err(|error| error.to_string())?,
            owner_b: NodeId::new(format!("gate2-b-{owner_suffix}"))
                .map_err(|error| error.to_string())?,
            endpoint_a: RpcEndpoint::new("127.0.0.1:47501").map_err(|error| error.to_string())?,
            endpoint_b: RpcEndpoint::new("127.0.0.1:47502").map_err(|error| error.to_string())?,
            request_id: RequestId::from_bytes(identity_bytes(seed, "request")),
        })
    }
}

pub fn dispatch(arguments: impl Iterator<Item = String>) -> Result<(), String> {
    let arguments = arguments.collect::<Vec<_>>();
    if arguments.first().map(String::as_str) == Some("--child-operation") {
        return run_child(ChildOptions::parse(arguments.into_iter())?);
    }
    let evidence = run(QualificationOptions::parse(arguments.into_iter())?)?;
    println!("{}", evidence.display());
    Ok(())
}

pub fn run(options: QualificationOptions) -> Result<PathBuf, String> {
    let evidence = EvidenceBundle::create(options.evidence_dir.clone())?;
    let execution = run_inner(&evidence, &options);
    let completed_scenarios = count_scenario_evidence(evidence.root()).unwrap_or(0);
    let required_scenarios = 2 + usize::from(REQUIRED_REPETITIONS) * Scenario::ALL.len();
    let inventory_sha256 = if execution.is_ok() {
        Some(inventory_digest(evidence.root())?)
    } else {
        None
    };
    evidence.finalize(&TerminalResult {
        status: if execution.is_ok() { "PASS" } else { "FAIL" },
        source_revision: options.source_revision.clone(),
        completed_scenarios,
        required_scenarios,
        failure: execution.as_ref().err().cloned(),
        inventory_sha256,
    })?;
    execution?;
    Ok(options.evidence_dir)
}

fn run_inner(evidence: &EvidenceBundle, options: &QualificationOptions) -> Result<(), String> {
    capture_environment(evidence, options)?;
    capture_health(evidence, options, "before")?;
    let runtime = FdbRuntime::start().map_err(|error| error.to_string())?;

    run_one(
        evidence,
        options,
        &runtime,
        "control",
        0,
        Scenario::ControlManifestFormat,
        false,
    )?;
    run_one(
        evidence,
        options,
        &runtime,
        "smoke",
        0,
        Scenario::ControlManifestFormat,
        true,
    )?;
    for repetition in 1..=REQUIRED_REPETITIONS {
        for scenario in Scenario::ALL {
            run_one(
                evidence, options, &runtime, "matrix", repetition, scenario, true,
            )?;
        }
    }
    capture_health(evidence, options, "after")?;
    Ok(())
}

fn run_one(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    runtime: &FdbRuntime,
    phase: &str,
    repetition: u8,
    scenario: Scenario,
    injected: bool,
) -> Result<(), String> {
    let seed = format!(
        "{}-{phase}-{repetition}-{}-{}",
        options.source_revision,
        scenario.slug(),
        unix_millis()
    );
    let suffix = &sha256_bytes(seed.as_bytes())[..12];
    let prefix = format!("{}-{suffix}", options.fdb_prefix_base);
    let context = ScenarioContext::new(&options.fdb_cluster_file, prefix.clone(), &seed)?;
    require_prefix_empty(runtime, &context)?;
    let directory = format!("scenarios/{phase}/{repetition:02}/{}", scenario.slug());
    let execution = (|| {
        if scenario.is_metadata() {
            metadata::setup(runtime, &context, scenario)?;
        } else {
            control::setup(runtime, &context, scenario)?;
        }
        let target_key = if scenario.is_metadata() {
            metadata::target_key(&context, scenario)?
        } else {
            control::target_key(&context, scenario)?
        };
        let child = if injected {
            run_injected_child(
                evidence,
                options,
                &context,
                scenario,
                &seed,
                &directory,
                &target_key,
            )?
        } else {
            run_plain_child(evidence, options, &context, scenario, &seed, &directory)?
        };
        let exact_readback = if scenario.is_metadata() {
            metadata::readback(runtime, &context, scenario)?
        } else {
            control::readback(runtime, &context, scenario)?
        };
        Ok((target_key, child, exact_readback))
    })();
    let cleanup =
        cleanup_prefix(runtime, &context).and_then(|()| require_prefix_empty(runtime, &context));
    match (execution, cleanup) {
        (Ok((target_key, child, exact_readback)), Ok(())) => evidence.write_json(
            format!("{directory}/result.json"),
            &ScenarioEvidence {
                phase: phase.to_owned(),
                repetition,
                scenario,
                prefix_sha256: sha256_bytes(prefix.as_bytes()),
                target_key_sha256: sha256_bytes(&target_key),
                selector: scenario.selector(),
                mutation_kind: scenario.mutation_kind(),
                child,
                exact_readback,
                cleanup_verified: true,
                status: "PASS",
            },
        ),
        (Err(primary), Ok(())) => {
            let _ = evidence.write_json(
                format!("{directory}/failure.json"),
                &serde_json::json!({
                    "scenario": scenario,
                    "failure": primary,
                    "cleanup_verified": true,
                }),
            );
            Err(primary)
        }
        (Ok(_), Err(cleanup)) => Err(format!(
            "{scenario} completed but prefix cleanup failed: {cleanup}"
        )),
        (Err(primary), Err(cleanup)) => Err(format!(
            "{scenario} failed: {primary}; prefix cleanup also failed: {cleanup}"
        )),
    }
}

fn run_plain_child(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    context: &ScenarioContext,
    scenario: Scenario,
    seed: &str,
    directory: &str,
) -> Result<ChildResult, String> {
    let mut command = child_command(options, context, scenario, seed);
    let output = command
        .output()
        .map_err(|error| format!("cannot start plain qualification child: {error}"))?;
    record_child_output(evidence, directory, &output)?;
    if !output.status.success() {
        return Err(format!(
            "plain {} child exited with {:?}",
            scenario,
            output.status.code()
        ));
    }
    parse_child_result(&output.stdout, scenario)
}

fn run_injected_child(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    context: &ScenarioContext,
    scenario: Scenario,
    seed: &str,
    directory: &str,
    target_key: &[u8],
) -> Result<ChildResult, String> {
    let absolute_directory = evidence.root().join(directory);
    fs::create_dir_all(&absolute_directory).map_err(|error| {
        format!(
            "cannot create scenario evidence {}: {error}",
            absolute_directory.display()
        )
    })?;
    let event_path = absolute_directory.join("injector.jsonl");
    let arm_path = absolute_directory.join("arm.txt");
    let nonce = format!(
        "g2_{}_{:x}",
        scenario.slug().replace('-', "_"),
        unix_millis()
    );
    validate_token("injector nonce", &nonce, 64)?;
    if scenario.selector() == evidence::Selector::Armed {
        fs::write(&arm_path, format!("arm-v1:{nonce}\n"))
            .map_err(|error| format!("cannot write arm message: {error}"))?;
    } else {
        fs::write(&arm_path, [])
            .map_err(|error| format!("cannot write empty arm descriptor: {error}"))?;
    }

    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("exec 8<\"$1\"; exec 9>>\"$2\"; shift 2; exec \"$@\"")
        .arg("nokv-gate2-child")
        .arg(&arm_path)
        .arg(&event_path)
        .arg(&options.qualification_binary)
        .args(child_arguments(context, scenario, seed))
        .env("LD_PRELOAD", &options.shim_library)
        .env("LD_LIBRARY_PATH", fdb_library_path(options)?)
        .env("NOKV_FDB_UNKNOWN_V1", "1")
        .env("NOKV_FDB_UNKNOWN_RUN_NONCE", &nonce)
        .env("NOKV_FDB_UNKNOWN_TARGET_KEY_HEX", lowercase_hex(target_key))
        .env(
            "NOKV_FDB_UNKNOWN_MUTATION",
            scenario.mutation_kind().as_str(),
        )
        .env("NOKV_FDB_UNKNOWN_MODE", scenario.selector().as_str())
        .env("NOKV_FDB_UNKNOWN_EVENT_FD", "9");
    match scenario.selector() {
        evidence::Selector::Ordinal => {
            command.env("NOKV_FDB_UNKNOWN_ORDINAL", "1");
        }
        evidence::Selector::Armed => {
            command.env("NOKV_FDB_UNKNOWN_ARM_FD", "8");
        }
    }
    let output = command
        .output()
        .map_err(|error| format!("cannot start injected qualification child: {error}"))?;
    record_child_output(evidence, directory, &output)?;
    if !output.status.success() {
        return Err(format!(
            "injected {} child exited with {:?}",
            scenario,
            output.status.code()
        ));
    }
    let child = parse_child_result(&output.stdout, scenario)?;
    let events = read_injector_events(&event_path)?;
    validate_injector_events(&events, &nonce, &sha256_bytes(target_key), scenario)?;
    Ok(child)
}

fn child_command(
    options: &QualificationOptions,
    context: &ScenarioContext,
    scenario: Scenario,
    seed: &str,
) -> Command {
    let mut command = Command::new(&options.qualification_binary);
    command.args(child_arguments(context, scenario, seed)).env(
        "LD_LIBRARY_PATH",
        fdb_library_path(options).unwrap_or_default(),
    );
    command
}

fn child_arguments(context: &ScenarioContext, scenario: Scenario, seed: &str) -> Vec<String> {
    vec![
        "--child-operation".to_owned(),
        scenario.slug().to_owned(),
        "--fdb-cluster-file".to_owned(),
        context.cluster_file.display().to_string(),
        "--fdb-prefix".to_owned(),
        context.prefix.clone(),
        "--seed".to_owned(),
        seed.to_owned(),
    ]
}

fn run_child(options: ChildOptions) -> Result<(), String> {
    let runtime = FdbRuntime::start().map_err(|error| error.to_string())?;
    let context = ScenarioContext::new(&options.fdb_cluster_file, options.prefix, &options.seed)?;
    let result = if options.scenario.is_metadata() {
        metadata::execute_child(&runtime, &context, options.scenario)?
    } else {
        control::execute_child(&runtime, &context, options.scenario)?
    };
    println!(
        "{}",
        serde_json::to_string(&result)
            .map_err(|error| format!("cannot encode child result: {error}"))?
    );
    Ok(())
}

fn parse_child_result(bytes: &[u8], scenario: Scenario) -> Result<ChildResult, String> {
    let stdout =
        std::str::from_utf8(bytes).map_err(|_| format!("{scenario} child stdout is not UTF-8"))?;
    let line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| format!("{scenario} child emitted no result"))?;
    let result: ChildResult = serde_json::from_str(line)
        .map_err(|error| format!("{scenario} child result is invalid JSON: {error}"))?;
    if result.scenario != scenario {
        return Err(format!(
            "child reported scenario {}, expected {scenario}",
            result.scenario
        ));
    }
    Ok(result)
}

fn record_child_output(
    evidence: &EvidenceBundle,
    directory: &str,
    output: &std::process::Output,
) -> Result<(), String> {
    evidence.write_bytes(format!("{directory}/child.stdout"), &output.stdout)?;
    evidence.write_bytes(format!("{directory}/child.stderr"), &output.stderr)?;
    evidence.write_json(
        format!("{directory}/child-status.json"),
        &serde_json::json!({
            "success": output.status.success(),
            "code": output.status.code(),
        }),
    )
}

fn read_injector_events(path: &Path) -> Result<Vec<InjectorEvent>, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("cannot read injector events {}: {error}", path.display()))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| format!("injector events {} are not UTF-8", path.display()))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|error| format!("invalid injector event JSON: {error}"))
        })
        .collect()
}

fn capture_environment(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
) -> Result<(), String> {
    if options.source_dirty {
        return Err("a Gate 2 PASS requires --source-dirty false".to_owned());
    }
    let mut version = Command::new(&options.candidate_binary);
    version
        .args(["version", "--json"])
        .env("LD_LIBRARY_PATH", fdb_library_path(options)?);
    let output = version
        .output()
        .map_err(|error| format!("cannot inspect candidate version: {error}"))?;
    evidence.write_bytes("commands/candidate-version.stdout", &output.stdout)?;
    evidence.write_bytes("commands/candidate-version.stderr", &output.stderr)?;
    if !output.status.success() {
        return Err(format!(
            "candidate version exited with {:?}",
            output.status.code()
        ));
    }
    let version: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("candidate version output is not JSON: {error}"))?;
    if version.get("git_commit").and_then(Value::as_str) != Some(options.source_revision.as_str()) {
        return Err("candidate binary revision does not match --source-revision".to_owned());
    }
    evidence.write_json(
        "environment.json",
        &EnvironmentEvidence {
            source_revision: options.source_revision.clone(),
            source_dirty: options.source_dirty,
            candidate_sha256: sha256_file(&options.candidate_binary)?,
            qualification_sha256: sha256_file(&options.qualification_binary)?,
            injector_sha256: sha256_file(&options.shim_library)?,
            fdb_cluster_file_sha256: sha256_file(&options.fdb_cluster_file)?,
            fdb_client_sha256: sha256_file(&options.fdb_client_library)?,
            rustfs_service_identity: options.rustfs_service_identity.clone(),
            rustfs_health_url: options.rustfs_health_url.clone(),
            required_repetitions: REQUIRED_REPETITIONS,
        },
    )
}

fn capture_health(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    phase: &str,
) -> Result<(), String> {
    let fdb = Command::new(&options.fdbcli)
        .args(["-C"])
        .arg(&options.fdb_cluster_file)
        .args(["--exec", "status json"])
        .output()
        .map_err(|error| format!("cannot inspect FDB health: {error}"))?;
    evidence.write_bytes(format!("commands/{phase}-fdb.stdout"), &fdb.stdout)?;
    evidence.write_bytes(format!("commands/{phase}-fdb.stderr"), &fdb.stderr)?;
    if !fdb.status.success() {
        return Err(format!("{phase} FDB health command failed"));
    }
    let status: Value = serde_json::from_slice(&fdb.stdout)
        .map_err(|error| format!("{phase} FDB status is not JSON: {error}"))?;
    if status
        .pointer("/client/database_status/available")
        .and_then(Value::as_bool)
        != Some(true)
        || status
            .pointer("/client/database_status/healthy")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err(format!("{phase} FDB status is not healthy and available"));
    }
    let rustfs = Command::new(&options.curl)
        .args([
            "--silent",
            "--show-error",
            "--fail",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}\\n",
            &options.rustfs_health_url,
        ])
        .output()
        .map_err(|error| format!("cannot inspect RustFS health: {error}"))?;
    evidence.write_bytes(format!("commands/{phase}-rustfs.stdout"), &rustfs.stdout)?;
    evidence.write_bytes(format!("commands/{phase}-rustfs.stderr"), &rustfs.stderr)?;
    if !rustfs.status.success() || rustfs.stdout != b"200\n" {
        return Err(format!("{phase} RustFS health check did not return 200"));
    }
    Ok(())
}

fn require_prefix_empty(runtime: &FdbRuntime, context: &ScenarioContext) -> Result<(), String> {
    let database = database(runtime, context)?;
    let prefix =
        FdbStorePrefix::new(context.prefix.as_bytes()).map_err(|error| error.to_string())?;
    let end = lexicographic_successor(prefix.as_bytes())
        .ok_or_else(|| "FDB store prefix has no successor".to_owned())?;
    let transaction = database.transaction().map_err(|error| error.to_string())?;
    let page = transaction
        .get_range(&FdbRangeRequest {
            begin: prefix.as_bytes().to_vec(),
            end,
            limit: Some(1),
            target_bytes: 0,
            iteration: 1,
            snapshot: true,
            reverse: false,
        })
        .map_err(|error| error.to_string())?;
    if page.items.is_empty() {
        Ok(())
    } else {
        Err("scenario FDB prefix is not empty".to_owned())
    }
}

fn cleanup_prefix(runtime: &FdbRuntime, context: &ScenarioContext) -> Result<(), String> {
    let database = database(runtime, context)?;
    let prefix =
        FdbStorePrefix::new(context.prefix.as_bytes()).map_err(|error| error.to_string())?;
    let end = lexicographic_successor(prefix.as_bytes())
        .ok_or_else(|| "FDB store prefix has no successor".to_owned())?;
    let transaction = database.transaction().map_err(|error| error.to_string())?;
    transaction.clear_range(prefix.as_bytes(), &end);
    transaction.commit().map_err(|error| error.to_string())
}

fn database(runtime: &FdbRuntime, context: &ScenarioContext) -> Result<FdbDatabase, String> {
    FdbDatabase::open(runtime, &FdbConnectionOptions::new(&context.cluster_file))
        .map_err(|error| error.to_string())
}

fn inventory_digest(root: &Path) -> Result<String, String> {
    fn walk(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in fs::read_dir(current)
            .map_err(|error| format!("cannot inventory {}: {error}", current.display()))?
        {
            let entry = entry.map_err(|error| format!("cannot read inventory entry: {error}"))?;
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, files)?;
            } else if path.file_name().and_then(|name| name.to_str()) != Some("result.json") {
                files.push(
                    path.strip_prefix(root)
                        .map_err(|error| error.to_string())?
                        .to_path_buf(),
                );
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    files.sort();
    let mut digest = Sha256::new();
    for relative in files {
        let bytes = fs::read(root.join(&relative))
            .map_err(|error| format!("cannot hash inventory file: {error}"))?;
        let path = relative
            .to_str()
            .ok_or_else(|| "evidence inventory path is not UTF-8".to_owned())?;
        digest.update(path.as_bytes());
        digest.update([0]);
        digest.update(Sha256::digest(&bytes));
    }
    Ok(lowercase_hex(&digest.finalize()))
}

fn count_scenario_evidence(root: &Path) -> Result<usize, String> {
    let scenarios = root.join("scenarios");
    if !scenarios.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![scenarios];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("cannot count scenario evidence: {error}"))?
        {
            let path = entry
                .map_err(|error| format!("cannot read scenario evidence entry: {error}"))?
                .path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().and_then(|name| name.to_str()) == Some("result.json") {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn fdb_library_path(options: &QualificationOptions) -> Result<String, String> {
    let parent = options
        .fdb_client_library
        .parent()
        .ok_or_else(|| "FDB client library has no parent directory".to_owned())?;
    let mut value = parent.display().to_string();
    if let Some(current) = std::env::var_os("LD_LIBRARY_PATH") {
        value.push(':');
        value.push_str(&current.to_string_lossy());
    }
    Ok(value)
}

fn identity_bytes(seed: &str, label: &str) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/gate2/identity/v1\0");
    digest.update(seed.as_bytes());
    digest.update([0]);
    digest.update(label.as_bytes());
    let digest = digest.finalize();
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest[..16]);
    if identity.iter().all(|byte| *byte == 0) {
        identity[15] = 1;
    }
    identity
}

pub(crate) fn zero_digest() -> CommandDigest {
    CommandDigest::from_bytes([0; 32])
}

pub(crate) fn owner_epoch(value: u64) -> OwnerEpoch {
    OwnerEpoch::new(value).expect("qualification owner epochs are nonzero")
}

fn validate_token(label: &str, value: &str, maximum: usize) -> Result<(), String> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(format!(
            "{label} must contain 1..={maximum} ASCII letters, digits, '-' or '_'"
        ))
    } else {
        Ok(())
    }
}

fn required<T>(value: Option<T>, flag: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("missing required option {flag}\n{}", usage()))
}

fn parse_bool(flag: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{flag} must be true or false")),
    }
}

fn usage() -> &'static str {
    "usage: nokv-fdb-unknown-outcome-qualification \\\n+  --candidate-binary ABSOLUTE --shim-library ABSOLUTE \\\n+  --fdb-cluster-file ABSOLUTE --fdb-client-library ABSOLUTE \\\n+  --fdbcli ABSOLUTE --curl ABSOLUTE --fdb-prefix-base TOKEN \\\n+  --rustfs-health-url URL --rustfs-service-identity ID \\\n+  --evidence-dir ABSOLUTE --source-revision HEX --source-dirty true|false"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_identity_is_deterministic_and_separated() {
        let first = ScenarioIdentity::new("seed-1").unwrap();
        let second = ScenarioIdentity::new("seed-1").unwrap();
        assert_eq!(first.shard_id, second.shard_id);
        assert_ne!(first.shard_id.as_bytes(), first.root_id.as_bytes());
        assert_ne!(first.owner_a, first.owner_b);
    }

    #[test]
    fn child_parser_rejects_unknown_flags() {
        let error = ChildOptions::parse(
            [
                "--child-operation",
                "control-manifest-format",
                "--unexpected",
                "value",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap_err();
        assert!(error.contains("unknown child option"));
    }
}
