/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Environment-gated FoundationDB transaction-envelope qualification.

mod envelope;
mod inspection;
mod object_separation;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nokv_fdb::FDB_API_VERSION;
use nokv_meta_fdb::FDB_PHYSICAL_TRANSACTION_GUARD_BYTES;
use nokv_protocol::RootIdentity;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use self::envelope::EnvelopeReport;
use self::object_separation::ObjectSeparationReport;
use crate::fdb_live_runtime::{
    append_object_arguments, capture_health, command_stdout, metadata_url, require_unused_endpoint,
    run_checked_command, HealthOptions, LiveControl, ObjectProviderOptions,
};
use crate::qualification_runtime::{
    lowercase_hex, sha256_bytes, sha256_file, EvidenceBundle, ProcessExit, ProcessSet,
};

pub const LIVE_GATE_ENV: &str = "NOKV_FDB_LIMITS_QUALIFICATION";

const RESULT_SCHEMA: &str = "nokv.fdb.limits-qualification.result.v1";
const ENVIRONMENT_SCHEMA: &str = "nokv.fdb.limits-qualification.environment.v1";
const DEFAULT_PAYLOAD_BYTES: usize = 3_200_000;

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
    pub evidence_dir: PathBuf,
    pub source_revision: String,
    pub source_dirty: bool,
    pub activation_timeout: Duration,
    pub payload_bytes: usize,
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
        let mut evidence_dir = None;
        let mut source_revision = None;
        let mut source_dirty = None;
        let mut activation_timeout = Some(Duration::from_secs(30));
        let mut payload_bytes = Some(DEFAULT_PAYLOAD_BYTES);

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
                "--evidence-dir" => evidence_dir = Some(PathBuf::from(value)),
                "--source-revision" => source_revision = Some(value),
                "--source-dirty" => source_dirty = Some(parse_bool(&flag, &value)?),
                "--activation-timeout-seconds" => {
                    activation_timeout = Some(parse_duration(&flag, &value)?)
                }
                "--payload-bytes" => payload_bytes = Some(parse_usize(&flag, &value)?),
                _ => {
                    return Err(format!(
                        "unknown qualification option {flag:?}\n{}",
                        usage()
                    ));
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
            evidence_dir: required(evidence_dir, "--evidence-dir")?,
            source_revision: required(source_revision, "--source-revision")?,
            source_dirty: required(source_dirty, "--source-dirty")?,
            activation_timeout: required(activation_timeout, "--activation-timeout-seconds")?,
            payload_bytes: required(payload_bytes, "--payload-bytes")?,
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
            ("object endpoint", self.object_endpoint.as_str()),
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
        if self.evidence_dir.exists() {
            return Err(format!(
                "evidence directory already exists: {}",
                self.evidence_dir.display()
            ));
        }
        if self.payload_bytes <= 2_900_000 || self.payload_bytes > 8 * 1024 * 1024 {
            return Err("--payload-bytes must be within 2900001..=8388608".to_owned());
        }
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
    physical_transaction_guard_bytes: usize,
    payload_bytes: usize,
    rustfs_service_identity: String,
    rustfs_health_url: String,
    object_endpoint: String,
    object_bucket: String,
    object_region: String,
    object_root: String,
    object_binding_sha256: String,
    owner_endpoint: SocketAddr,
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

#[derive(Clone, Debug, Serialize)]
struct ScenarioResult {
    scenario: &'static str,
    status: &'static str,
    detail: &'static str,
}

#[derive(Serialize)]
struct TerminalResult {
    schema: &'static str,
    status: &'static str,
    error: Option<String>,
    scenarios: Vec<ScenarioResult>,
    envelope: Option<EnvelopeReport>,
    object_separation: Option<ObjectSeparationReport>,
    process_exits: Vec<ProcessExit>,
}

pub fn run(options: QualificationOptions) -> Result<PathBuf, String> {
    let evidence = EvidenceBundle::create(options.evidence_dir.clone())?;
    let run_id = run_id(&options);
    let prefix = format!("{}-gate9-{run_id}", options.fdb_prefix_base);
    let object_root = format!(
        "{}/gate9/{run_id}",
        options.object_root_base.trim_end_matches('/')
    );
    let url = metadata_url(&options.fdb_cluster_file, &prefix)?;
    let root = RootIdentity(identity(&run_id, "root"));
    let agent = identity(&run_id, "agent");
    let objects = ObjectProviderOptions {
        endpoint: options.object_endpoint.clone(),
        bucket: options.object_bucket.clone(),
        region: options.object_region.clone(),
        root: object_root.clone(),
        access_key_id: options.object_access_key_id.clone(),
        secret_access_key: options.object_secret_access_key.clone(),
    };
    let health = HealthOptions {
        fdbcli: options.fdbcli.clone(),
        fdb_cluster_file: options.fdb_cluster_file.clone(),
        curl: options.curl.clone(),
        rustfs_health_url: options.rustfs_health_url.clone(),
    };
    let mut processes = ProcessSet::default();

    let setup = require_unused_endpoint(options.owner_endpoint)
        .and_then(|()| capture_health(&evidence, &health, "before"))
        .and_then(|()| capture_environment(&evidence, &options, &run_id, &prefix, &object_root));
    if let Err(error) = setup {
        evidence.finalize(&TerminalResult {
            schema: RESULT_SCHEMA,
            status: "NOT_QUALIFIED",
            error: Some(error.clone()),
            scenarios: Vec::new(),
            envelope: None,
            object_separation: None,
            process_exits: processes.reap_all(),
        })?;
        return Err(error);
    }

    let execution = execute(
        &evidence,
        &options,
        &url,
        &prefix,
        &run_id,
        root,
        agent,
        &objects,
        &mut processes,
    );
    let process_exits = processes.reap_all();
    let after = capture_health(&evidence, &health, "after");
    let mut errors = Vec::new();
    if let Err(error) = &execution {
        errors.push(error.clone());
    }
    if let Err(error) = after {
        errors.push(error);
    }
    let error = (!errors.is_empty()).then(|| errors.join("; "));
    let (envelope, object_separation) = match execution {
        Ok((envelope, object)) => (Some(envelope), Some(object)),
        Err(_) => (None, None),
    };
    let scenarios = if error.is_none() {
        vec![
            ScenarioResult {
                scenario: "transaction_envelope",
                status: "PASS",
                detail: "real FdbStore transactions retained logical, conservative, and observed physical byte evidence",
            },
            ScenarioResult {
                scenario: "large_object_separation",
                status: "PASS",
                detail: "an object larger than the logical metadata limit round-tripped through the exact candidate without payload bytes in FDB rows",
            },
        ]
    } else {
        Vec::new()
    };
    evidence.finalize(&TerminalResult {
        schema: RESULT_SCHEMA,
        status: if error.is_none() { "PASS" } else { "FAIL" },
        error: error.clone(),
        scenarios,
        envelope,
        object_separation,
        process_exits,
    })?;
    match error {
        None => Ok(options.evidence_dir),
        Some(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    url: &str,
    prefix: &str,
    run_id: &str,
    root: RootIdentity,
    agent: [u8; 16],
    objects: &ObjectProviderOptions,
    processes: &mut ProcessSet,
) -> Result<(EnvelopeReport, ObjectSeparationReport), String> {
    run_format(evidence, options, url)?;
    run_provision(evidence, options, url, root, agent, objects)?;
    let control = LiveControl::open(&options.fdb_cluster_file, prefix, root)?;
    let mut owner = Command::new(&options.candidate_binary);
    owner
        .args(["--bind", &options.owner_endpoint.to_string()])
        .args(["--advertise-endpoint", &options.owner_endpoint.to_string()])
        .args(["--node-id", "fdb-gate9-owner"])
        .args(["--lifecycle-interval-millis", "100"]);
    append_object_arguments(&mut owner, objects);
    owner.args(["serve", "--meta-url", url]);
    processes.spawn("owner", &mut owner, evidence)?;
    let ownership = control.wait_for_serving(
        options.owner_endpoint,
        options.activation_timeout,
        "owner",
        processes,
    )?;
    evidence.write_json("owners/serving.json", &ownership_evidence(&ownership))?;

    let store = control.open_store()?;
    let logical_limit = nokv_meta_store::TxnStore::profile(&store)
        .limits
        .max_transaction_bytes;
    let envelope = envelope::run(&store)?;
    evidence.write_json("transactions/envelope.json", &envelope)?;
    let object = object_separation::run(
        evidence,
        &options.candidate_binary,
        &options.fdb_cluster_file,
        prefix,
        root,
        options.owner_endpoint,
        objects,
        run_id,
        options.payload_bytes,
        logical_limit,
    )?;
    evidence.write_json("objects/separation.json", &object)?;
    Ok((envelope, object))
}

fn run_format(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    url: &str,
) -> Result<(), String> {
    let mut command = Command::new(&options.candidate_binary);
    command.args(["format", "--meta-url", url]);
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
    url: &str,
    root: RootIdentity,
    agent: [u8; 16],
    objects: &ObjectProviderOptions,
) -> Result<(), String> {
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(["--root-id", &lowercase_hex(&root.0)])
        .args(["--agent-id", &lowercase_hex(&agent)]);
    append_object_arguments(&mut command, objects);
    command.args(["provision", "--meta-url", url]);
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
            physical_transaction_guard_bytes: FDB_PHYSICAL_TRANSACTION_GUARD_BYTES,
            payload_bytes: options.payload_bytes,
            rustfs_service_identity: options.rustfs_service_identity.clone(),
            rustfs_health_url: options.rustfs_health_url.clone(),
            object_endpoint: options.object_endpoint.clone(),
            object_bucket: options.object_bucket.clone(),
            object_region: options.object_region.clone(),
            object_root: object_root.to_owned(),
            object_binding_sha256: sha256_bytes(binding.as_bytes()),
            owner_endpoint: options.owner_endpoint,
        },
    )
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

fn run_id(options: &QualificationOptions) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-limits-qualification/run/v1\0");
    digest.update(options.source_revision.as_bytes());
    digest.update(now.to_be_bytes());
    lowercase_hex(&digest.finalize()[..8])
}

fn identity(run_id: &str, label: &str) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-limits-qualification/identity/v1\0");
    digest.update(run_id.as_bytes());
    digest.update([0]);
    digest.update(label.as_bytes());
    digest.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has fixed length")
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

fn parse_usize(flag: &str, value: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|error| format!("{flag} has invalid byte count {value:?}: {error}"))
}

fn usage() -> &'static str {
    "required: --candidate-binary PATH --fdb-cluster-file PATH --fdb-client-library PATH \
--fdb-prefix-base PREFIX --fdbcli PATH --curl PATH --object-endpoint URL \
--object-bucket NAME --object-root-base PREFIX --object-access-key-id VALUE \
--object-secret-access-key VALUE --rustfs-service-identity VALUE \
--rustfs-health-url URL --owner-endpoint HOST:PORT --evidence-dir PATH \
--source-revision SHA --source-dirty true|false"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn option_names_are_unique() {
        let names = usage()
            .split_whitespace()
            .filter(|word| word.starts_with("--"))
            .collect::<Vec<_>>();
        assert_eq!(names.len(), names.iter().collect::<BTreeSet<_>>().len());
    }

    #[test]
    fn identities_are_domain_separated() {
        assert_eq!(identity("run", "root"), identity("run", "root"));
        assert_ne!(identity("run", "root"), identity("run", "agent"));
    }
}
