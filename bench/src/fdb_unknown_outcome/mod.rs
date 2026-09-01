/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic real-commit lost-ack qualification for FoundationDB Gate 2.

mod control;
mod evidence;
mod metadata;

use std::fs::{self, File};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use evidence::{
    validate_injector_events, validate_injector_events_exact, CandidateEvidence,
    CandidateOrdinaryEvidence, ChildResult, EnvironmentEvidence, InjectorEvent, Scenario,
    ScenarioEvidence, TerminalResult, REQUIRED_REPETITIONS,
};
use nokv_client::{
    ClientError, ClientOptions, FramedTcpOptions, FramedTcpTransport, SeedRouteOptions,
    SeedRouteResolver, WorkspaceClient,
};
use nokv_control::{
    AgentId, CatalogEntryState, DistributedControlStore, LogicalShardId, NodeId, ObjectNamespaceId,
    OwnershipSnapshot, PlacementGeneration, RootCatalogEntry, RootId, RpcEndpoint, ShardRouteState,
    StoreId, StoreManifest, StoreProvider, SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_control_fdb::{FdbControlKeys, FdbControlOptions, FdbControlStore};
use nokv_fdb::{
    lexicographic_successor, FdbConnectionOptions, FdbDatabase, FdbRangeRequest, FdbRuntime,
    FdbStorePrefix, FDB_PHYSICAL_ENCODING_VERSION,
};
use nokv_meta::workspace::{workspace_current_key, MetaShard, RootFence};
use nokv_protocol::{
    ConflictKind, CreateWorkspaceRequest, ErrorCode, GetWorkspaceRequest, RequestIdentity,
    RootIdentity, WorkbenchName, WorkspaceIdentity,
};
use nokv_types::{CommandDigest, OwnerEpoch, RequestId, WorkbenchId};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::qualification_runtime::{
    lowercase_hex, sha256_bytes, sha256_file, unix_millis, EvidenceBundle,
};

pub const LIVE_GATE_ENV: &str = "NOKV_FDB_UNKNOWN_OUTCOME_QUALIFICATION";
const LEASE_TTL_MILLIS: u64 = 10;
const CANDIDATE_PROCESS_TIMEOUT: Duration = Duration::from_secs(45);
const CANDIDATE_POLL_INTERVAL: Duration = Duration::from_millis(20);
const CANDIDATE_BOOTSTRAP_CASES: usize = 10;

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
    object_endpoint: String,
    object_bucket: String,
    object_region: String,
    object_root_base: String,
    object_access_key_id: String,
    object_secret_access_key: String,
    owner_a_endpoint: SocketAddr,
    owner_b_endpoint: SocketAddr,
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
        let mut object_endpoint = None;
        let mut object_bucket = None;
        let mut object_region = Some("us-east-1".to_owned());
        let mut object_root_base = None;
        let mut object_access_key_id = None;
        let mut object_secret_access_key = None;
        let mut owner_a_endpoint = None;
        let mut owner_b_endpoint = None;
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
                "--object-endpoint" => object_endpoint = Some(value),
                "--object-bucket" => object_bucket = Some(value),
                "--object-region" => object_region = Some(value),
                "--object-root-base" => object_root_base = Some(value),
                "--object-access-key-id" => object_access_key_id = Some(value),
                "--object-secret-access-key" => object_secret_access_key = Some(value),
                "--owner-a-endpoint" => owner_a_endpoint = Some(parse_endpoint(&flag, &value)?),
                "--owner-b-endpoint" => owner_b_endpoint = Some(parse_endpoint(&flag, &value)?),
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
            object_endpoint: required(object_endpoint, "--object-endpoint")?,
            object_bucket: required(object_bucket, "--object-bucket")?,
            object_region: required(object_region, "--object-region")?,
            object_root_base: required(object_root_base, "--object-root-base")?,
            object_access_key_id: required(object_access_key_id, "--object-access-key-id")?,
            object_secret_access_key: required(
                object_secret_access_key,
                "--object-secret-access-key",
            )?,
            owner_a_endpoint: required(owner_a_endpoint, "--owner-a-endpoint")?,
            owner_b_endpoint: required(owner_b_endpoint, "--owner-b-endpoint")?,
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
        for (flag, value) in [
            ("--object-endpoint", self.object_endpoint.as_str()),
            ("--object-bucket", self.object_bucket.as_str()),
            ("--object-region", self.object_region.as_str()),
            ("--object-root-base", self.object_root_base.as_str()),
            ("--object-access-key-id", self.object_access_key_id.as_str()),
            (
                "--object-secret-access-key",
                self.object_secret_access_key.as_str(),
            ),
        ] {
            if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
                return Err(format!("{flag} must be canonical and non-empty"));
            }
        }
        let object_endpoint = url::Url::parse(&self.object_endpoint)
            .map_err(|error| format!("--object-endpoint is invalid: {error}"))?;
        if !matches!(object_endpoint.scheme(), "http" | "https")
            || object_endpoint.host_str().is_none()
            || !object_endpoint.username().is_empty()
            || object_endpoint.password().is_some()
        {
            return Err(
                "--object-endpoint must be an absolute HTTP(S) URL without userinfo".to_owned(),
            );
        }
        if self.owner_a_endpoint == self.owner_b_endpoint
            || [self.owner_a_endpoint, self.owner_b_endpoint]
                .iter()
                .any(|endpoint| endpoint.ip().is_unspecified() || endpoint.port() == 0)
        {
            return Err("owner endpoints must be pairwise distinct and connectable".to_owned());
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
    let redaction = verify_evidence_redaction(evidence.root(), &options);
    let execution = match (execution, redaction) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(redaction)) => Err(redaction),
        (Err(primary), Err(redaction)) => Err(format!(
            "{primary}; evidence credential scan also failed: {redaction}"
        )),
    };
    let completed_scenarios = count_scenario_evidence(evidence.root()).unwrap_or(0);
    let required_scenarios =
        3 + CANDIDATE_BOOTSTRAP_CASES + usize::from(REQUIRED_REPETITIONS) * Scenario::ALL.len();
    let candidate_cases_complete =
        count_candidate_evidence(evidence.root()).unwrap_or(0) == CANDIDATE_BOOTSTRAP_CASES + 1;
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
        candidate_cases_complete,
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
    run_candidate_bootstrap_matrix(evidence, options, &runtime)?;
    run_candidate_ordinary_failover(evidence, options, &runtime)?;
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
        if scenario == Scenario::ControlManifestFormat {
            let target_key = control::target_key(&context, scenario)?;
            let child = run_candidate_format(
                evidence,
                options,
                &context,
                &directory,
                &target_key,
                injected,
            )?;
            let exact_readback = candidate_manifest_readback(runtime, &context)?;
            return Ok((target_key, child, exact_readback));
        }
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

fn run_candidate_format(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    context: &ScenarioContext,
    directory: &str,
    target_key: &[u8],
    injected: bool,
) -> Result<ChildResult, String> {
    let arguments = vec![
        "format".to_owned(),
        "--meta-url".to_owned(),
        metadata_url(context)?,
    ];
    let output = if injected {
        run_injected_candidate(
            evidence, options, directory, "format", &arguments, target_key, "set", 1, 1,
        )?
    } else {
        run_plain_candidate(evidence, options, directory, "format", &arguments)?
    };
    let value = candidate_json(&output.stdout, "candidate format")?;
    let expected_created = !injected;
    if value.get("created") != Some(&Value::Bool(expected_created))
        || value.get("provider").and_then(Value::as_str) != Some("foundationdb")
    {
        return Err(format!(
            "candidate format returned an unexpected outcome: {value}"
        ));
    }
    Ok(ChildResult {
        scenario: Scenario::ControlManifestFormat,
        outcome: if injected {
            "candidate_reconciled_success"
        } else {
            "candidate_created"
        }
        .to_owned(),
        typed_error: None,
    })
}

fn candidate_manifest_readback(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
) -> Result<String, String> {
    let options = context.control_options()?;
    let manifest =
        FdbControlStore::inspect_manifest(runtime, &options).map_err(|error| error.to_string())?;
    options
        .validate_manifest_binding(&manifest)
        .map_err(|error| error.to_string())?;
    if manifest.provider() != StoreProvider::FoundationDb
        || manifest.workspace_format_version() != SUPPORTED_WORKSPACE_FORMAT_VERSION
        || manifest.physical_encoding_version() != FDB_PHYSICAL_ENCODING_VERSION
    {
        return Err(
            "candidate manifest does not match the complete FDB format contract".to_owned(),
        );
    }
    Ok("production candidate manifest is complete and prefix-bound".to_owned())
}

#[derive(Clone, Copy)]
enum CandidateBootstrapTarget {
    ShardCreate,
    RootCreate,
    RootReadyCas,
    ShardReadyCas,
    ProvisioningAcquire,
    MetadataInitialize,
    MetadataOwnerEpoch,
    RootFenceInstall,
    RootFenceActivate,
    OwnerRelease,
}

impl CandidateBootstrapTarget {
    const ALL: [Self; CANDIDATE_BOOTSTRAP_CASES] = [
        Self::ShardCreate,
        Self::RootCreate,
        Self::RootReadyCas,
        Self::ShardReadyCas,
        Self::ProvisioningAcquire,
        Self::MetadataInitialize,
        Self::MetadataOwnerEpoch,
        Self::RootFenceInstall,
        Self::RootFenceActivate,
        Self::OwnerRelease,
    ];

    const fn slug(self) -> &'static str {
        match self {
            Self::ShardCreate => "shard-create",
            Self::RootCreate => "root-create",
            Self::RootReadyCas => "root-ready-cas",
            Self::ShardReadyCas => "shard-ready-cas",
            Self::ProvisioningAcquire => "provisioning-acquire",
            Self::MetadataInitialize => "metadata-initialize",
            Self::MetadataOwnerEpoch => "metadata-owner-epoch",
            Self::RootFenceInstall => "root-fence-install",
            Self::RootFenceActivate => "root-fence-activate",
            Self::OwnerRelease => "owner-release",
        }
    }

    const fn mutation_kind(self) -> &'static str {
        if matches!(self, Self::OwnerRelease) {
            "clear"
        } else {
            "set"
        }
    }

    const fn ordinal(self) -> u64 {
        match self {
            Self::RootReadyCas
            | Self::ShardReadyCas
            | Self::MetadataOwnerEpoch
            | Self::RootFenceActivate => 2,
            _ => 1,
        }
    }

    const fn expected_matches(self) -> u64 {
        match self {
            Self::ShardCreate
            | Self::RootCreate
            | Self::RootReadyCas
            | Self::ShardReadyCas
            | Self::MetadataOwnerEpoch
            | Self::RootFenceInstall
            | Self::RootFenceActivate => 2,
            Self::ProvisioningAcquire | Self::MetadataInitialize | Self::OwnerRelease => 1,
        }
    }
}

fn run_candidate_bootstrap_matrix(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    runtime: &FdbRuntime,
) -> Result<(), String> {
    for target in CandidateBootstrapTarget::ALL {
        run_candidate_bootstrap_case(evidence, options, runtime, target)?;
    }
    Ok(())
}

fn run_candidate_bootstrap_case(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    runtime: &FdbRuntime,
    target: CandidateBootstrapTarget,
) -> Result<(), String> {
    let seed = format!(
        "{}-candidate-bootstrap-{}-{}",
        options.source_revision,
        target.slug(),
        unix_millis()
    );
    let suffix = &sha256_bytes(seed.as_bytes())[..12];
    let prefix = format!("{}-{suffix}", options.fdb_prefix_base);
    let context = ScenarioContext::new(&options.fdb_cluster_file, prefix.clone(), &seed)?;
    let directory = format!("scenarios/candidate-bootstrap/00/{}", target.slug());
    require_prefix_empty(runtime, &context)?;
    let execution = (|| {
        let format_arguments = vec![
            "format".to_owned(),
            "--meta-url".to_owned(),
            metadata_url(&context)?,
        ];
        let format =
            run_plain_candidate(evidence, options, &directory, "format", &format_arguments)?;
        let format = candidate_json(&format.stdout, "candidate bootstrap format")?;
        if format.get("created") != Some(&Value::Bool(true)) {
            return Err("candidate bootstrap format did not create a fresh store".to_owned());
        }
        let manifest = FdbControlStore::inspect_manifest(runtime, &context.control_options()?)
            .map_err(|error| error.to_string())?;
        let shard_id = derive_logical_shard_id(manifest.store_id());
        let target_key = candidate_bootstrap_target_key(&context, target, shard_id)?;
        let object_root = format!(
            "{}/{}",
            options.object_root_base.trim_end_matches('/'),
            suffix
        );
        let provision_arguments = candidate_provision_arguments(options, &context, &object_root)?;
        let provision = run_injected_candidate(
            evidence,
            options,
            &directory,
            "provision",
            &provision_arguments,
            &target_key,
            target.mutation_kind(),
            target.ordinal(),
            target.expected_matches(),
        )?;
        let provision = candidate_json(&provision.stdout, "candidate bootstrap provision")?;
        let expected_root_id = lowercase_hex(context.identity.root_id.as_bytes());
        if provision.get("preexisting") != Some(&Value::Bool(false))
            || provision.get("provider").and_then(Value::as_str) != Some("foundationdb")
            || provision.get("lifecycle").and_then(Value::as_str) != Some("ready")
            || provision.get("root_id").and_then(Value::as_str) != Some(expected_root_id.as_str())
        {
            return Err(format!(
                "candidate provision did not complete exact bootstrap: {provision}"
            ));
        }
        let exact_readback = candidate_provision_readback(runtime, &context, manifest, shard_id)?;
        Ok((target_key, exact_readback))
    })();
    let cleanup =
        cleanup_prefix(runtime, &context).and_then(|()| require_prefix_empty(runtime, &context));
    match (execution, cleanup) {
        (Ok((target_key, exact_readback)), Ok(())) => evidence.write_json(
            format!("{directory}/result.json"),
            &CandidateEvidence {
                case: target.slug().to_owned(),
                prefix_sha256: sha256_bytes(prefix.as_bytes()),
                target_key_sha256: sha256_bytes(&target_key),
                mutation_kind: target.mutation_kind().to_owned(),
                ordinal: target.ordinal(),
                expected_matches: target.expected_matches(),
                candidate_outcome: "provision_completed_after_exact_reconciliation".to_owned(),
                exact_readback,
                cleanup_verified: true,
                status: "PASS",
            },
        ),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) => Err(format!(
            "candidate bootstrap {} cleanup failed: {cleanup}",
            target.slug()
        )),
        (Err(primary), Err(cleanup)) => Err(format!(
            "candidate bootstrap {} failed: {primary}; cleanup also failed: {cleanup}",
            target.slug()
        )),
    }
}

fn candidate_bootstrap_target_key(
    context: &ScenarioContext,
    target: CandidateBootstrapTarget,
    shard_id: LogicalShardId,
) -> Result<Vec<u8>, String> {
    let prefix =
        FdbStorePrefix::new(context.prefix.as_bytes()).map_err(|error| error.to_string())?;
    let keys = FdbControlKeys::new(&prefix);
    match target {
        CandidateBootstrapTarget::ShardCreate | CandidateBootstrapTarget::ShardReadyCas => {
            Ok(keys.shard_catalog_key(&shard_id))
        }
        CandidateBootstrapTarget::RootCreate | CandidateBootstrapTarget::RootReadyCas => {
            Ok(keys.root_catalog_key(&context.identity.root_id))
        }
        CandidateBootstrapTarget::ProvisioningAcquire | CandidateBootstrapTarget::OwnerRelease => {
            Ok(keys.session_key(&shard_id))
        }
        CandidateBootstrapTarget::MetadataInitialize => {
            candidate_metadata_key(context, 0x0101, b"schema")
        }
        CandidateBootstrapTarget::MetadataOwnerEpoch => {
            candidate_metadata_key(context, 0x0101, b"owner_fence")
        }
        CandidateBootstrapTarget::RootFenceInstall
        | CandidateBootstrapTarget::RootFenceActivate => {
            candidate_metadata_key(context, 0x0102, context.identity.root_id.as_bytes())
        }
    }
}

fn candidate_provision_readback(
    runtime: &FdbRuntime,
    context: &ScenarioContext,
    manifest: StoreManifest,
    shard_id: LogicalShardId,
) -> Result<String, String> {
    let store_id = manifest.store_id();
    let control = FdbControlStore::open(runtime, context.control_options()?, manifest)
        .map_err(|error| error.to_string())?;
    let expected_namespace = derive_namespace_id(store_id);
    let expected_root = RootCatalogEntry::new(
        context.identity.root_id,
        context.identity.agent_id,
        expected_namespace,
        shard_id,
        PlacementGeneration::new(1).expect("one is nonzero"),
        CatalogEntryState::Ready,
    );
    if control
        .get_root_catalog(&context.identity.root_id)
        .map_err(|error| error.to_string())?
        != Some(expected_root)
        || control
            .get_shard_catalog(&shard_id)
            .map_err(|error| error.to_string())?
            != Some(nokv_control::ShardCatalogEntry::new(
                shard_id,
                CatalogEntryState::Ready,
            ))
    {
        return Err("candidate provision catalog readback is not the exact Ready state".to_owned());
    }
    let released = control
        .observe_ownership(&shard_id)
        .map_err(|error| error.to_string())?;
    if released.route().state() != ShardRouteState::Unassigned
        || released.session().is_some()
        || released.route().owner_epoch().map(OwnerEpoch::get) != Some(1)
        || released
            .route()
            .session_generation()
            .map(nokv_control::SessionGeneration::get)
            != Some(1)
    {
        return Err("candidate provision did not release the exact bootstrap session".to_owned());
    }
    let successor = control
        .acquire_owner(
            &shard_id,
            context.identity.owner_b.clone(),
            context.identity.endpoint_b.clone(),
        )
        .map_err(|error| error.to_string())?;
    let shard = MetaShard::open(
        metadata::metadata_store(runtime, context, &control, &successor)?,
        shard_id,
    )
    .map_err(|error| error.to_string())?;
    shard
        .advance_owner_epoch(Some(owner_epoch(1)), owner_epoch(2))
        .map_err(|error| error.to_string())?;
    let expected_fence = RootFence {
        logical_shard_id: shard_id,
        object_namespace_id: Some(expected_namespace),
        placement_generation: PlacementGeneration::new(1).expect("one is nonzero"),
        activation_state: nokv_types::RootActivationState::Active,
    };
    if shard
        .root_fence(context.identity.root_id)
        .map_err(|error| error.to_string())?
        != Some(expected_fence)
    {
        return Err("candidate provision root fence is not the exact Active record".to_owned());
    }
    Ok(
        "candidate catalogs Ready, bootstrap session released, exact Active root fence reopened"
            .to_owned(),
    )
}

fn run_candidate_ordinary_failover(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    runtime: &FdbRuntime,
) -> Result<(), String> {
    let seed = format!(
        "{}-candidate-ordinary-{}",
        options.source_revision,
        unix_millis()
    );
    let suffix = &sha256_bytes(seed.as_bytes())[..12];
    let prefix = format!("{}-{suffix}", options.fdb_prefix_base);
    let context = ScenarioContext::new(&options.fdb_cluster_file, prefix.clone(), &seed)?;
    let directory = "scenarios/candidate-ordinary/00/metadata-ordinary-command";
    let object_root = format!(
        "{}/{}",
        options.object_root_base.trim_end_matches('/'),
        suffix
    );
    let mut owner_a = None;
    let mut owner_b = None;
    require_prefix_empty(runtime, &context)?;

    let execution = (|| {
        let format_arguments = vec![
            "format".to_owned(),
            "--meta-url".to_owned(),
            metadata_url(&context)?,
        ];
        let format = run_plain_candidate(
            evidence,
            options,
            directory,
            "ordinary-format",
            &format_arguments,
        )?;
        let format = candidate_json(&format.stdout, "candidate ordinary format")?;
        if format.get("created") != Some(&Value::Bool(true)) {
            return Err("candidate ordinary format did not create a fresh store".to_owned());
        }
        let provision_arguments = candidate_provision_arguments(options, &context, &object_root)?;
        let provision = run_plain_candidate(
            evidence,
            options,
            directory,
            "ordinary-provision",
            &provision_arguments,
        )?;
        let provision = candidate_json(&provision.stdout, "candidate ordinary provision")?;
        let expected_root_id = lowercase_hex(context.identity.root_id.as_bytes());
        if provision.get("preexisting") != Some(&Value::Bool(false))
            || provision.get("provider").and_then(Value::as_str) != Some("foundationdb")
            || provision.get("lifecycle").and_then(Value::as_str) != Some("ready")
            || provision.get("root_id").and_then(Value::as_str) != Some(expected_root_id.as_str())
        {
            return Err(format!(
                "candidate ordinary provision did not create the exact Ready root: {provision}"
            ));
        }

        let control_options = context.control_options()?;
        let manifest = FdbControlStore::inspect_manifest(runtime, &control_options)
            .map_err(|error| error.to_string())?;
        control_options
            .validate_manifest_binding(&manifest)
            .map_err(|error| error.to_string())?;
        let control = FdbControlStore::open(runtime, control_options, manifest)
            .map_err(|error| error.to_string())?;
        let root = control
            .get_root_catalog(&context.identity.root_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "candidate ordinary root catalog entry is absent".to_owned())?;
        if root.state() != CatalogEntryState::Ready {
            return Err("candidate ordinary root catalog entry is not Ready".to_owned());
        }
        let shard_id = root.logical_shard_id();

        let workbench =
            WorkbenchName::new(format!("gate2-{suffix}")).map_err(|error| error.to_string())?;
        let workbench_id =
            WorkbenchId::new(workbench.as_str().to_owned()).map_err(|error| error.to_string())?;
        let request_id = RequestIdentity(identity_bytes(&seed, "candidate-request"));
        let workspace_incarnation_id =
            WorkspaceIdentity(identity_bytes(&seed, "candidate-workspace"));
        let request = CreateWorkspaceRequest {
            workbench: workbench.clone(),
            workspace_incarnation_id,
        };
        let request_sha256 = sha256_bytes(
            &serde_json::to_vec(&(request_id, &request))
                .map_err(|error| format!("cannot encode candidate request evidence: {error}"))?,
        );
        let target_key = candidate_metadata_key(
            &context,
            0x0202,
            &workspace_current_key(context.identity.root_id, &workbench_id),
        )?;

        let owner_a_node =
            NodeId::new(format!("gate2-owner-a-{suffix}")).map_err(|error| error.to_string())?;
        let owner_a_arguments = candidate_serve_arguments(
            options,
            &context,
            &object_root,
            options.owner_a_endpoint,
            owner_a_node.as_str(),
        )?;
        let (process_a, injection) = spawn_injected_candidate_owner(
            evidence,
            options,
            directory,
            "owner-a",
            &owner_a_arguments,
            &target_key,
        )?;
        owner_a = Some(process_a);
        let owner_a_snapshot = wait_for_candidate_serving(
            &control,
            shard_id,
            &owner_a_node,
            options.owner_a_endpoint,
            CANDIDATE_PROCESS_TIMEOUT,
            owner_a.as_mut().expect("owner A was installed"),
        )?;
        let (owner_a_epoch, owner_a_generation) = ownership_tokens(&owner_a_snapshot)?;

        let root_identity = RootIdentity(*context.identity.root_id.as_bytes());
        let client_a = candidate_seed_client(root_identity, [options.owner_a_endpoint], 1, 1)?;
        let first = match client_a.create_workspace(request_id, request.clone()) {
            Ok(_) => {
                return Err(
                    "the injected candidate request unexpectedly acknowledged success".to_owned(),
                )
            }
            Err(error) => error,
        };
        validate_candidate_unknown_outcome(&first)?;

        let departed = wait_for_candidate_departure(
            &control,
            shard_id,
            options.owner_a_endpoint,
            CANDIDATE_PROCESS_TIMEOUT,
        )?;
        let (departed_epoch, departed_generation) = ownership_tokens(&departed)?;
        if departed_epoch != owner_a_epoch || departed_generation != owner_a_generation {
            return Err("owner A departure changed its durable fencing token".to_owned());
        }
        stop_and_record_candidate(
            evidence,
            options,
            directory,
            owner_a.as_mut().expect("owner A was installed"),
            true,
        )?;
        let events = read_injector_events(&injection.event_path)?;
        validate_injector_events_exact(
            &events,
            &injection.nonce,
            &sha256_bytes(&target_key),
            "set",
            "armed",
            1,
            1,
        )?;
        require_endpoint_closed(options.owner_a_endpoint)?;

        let owner_b_node =
            NodeId::new(format!("gate2-owner-b-{suffix}")).map_err(|error| error.to_string())?;
        let owner_b_arguments = candidate_serve_arguments(
            options,
            &context,
            &object_root,
            options.owner_b_endpoint,
            owner_b_node.as_str(),
        )?;
        owner_b = Some(spawn_plain_candidate_owner(
            evidence,
            options,
            directory,
            "owner-b",
            &owner_b_arguments,
        )?);
        let owner_b_snapshot = wait_for_candidate_serving(
            &control,
            shard_id,
            &owner_b_node,
            options.owner_b_endpoint,
            CANDIDATE_PROCESS_TIMEOUT,
            owner_b.as_mut().expect("owner B was installed"),
        )?;
        let (owner_b_epoch, owner_b_generation) = ownership_tokens(&owner_b_snapshot)?;
        if owner_b_epoch <= owner_a_epoch || owner_b_generation <= owner_a_generation {
            return Err(format!(
                "successor fences did not advance strictly: A={owner_a_epoch}/{owner_a_generation}, B={owner_b_epoch}/{owner_b_generation}"
            ));
        }

        let client_b = candidate_seed_client(
            root_identity,
            [options.owner_a_endpoint, options.owner_b_endpoint],
            1,
            2,
        )?;
        let replay = client_b
            .create_workspace(request_id, request.clone())
            .map_err(|error| format!("successor exact replay failed: {error}"))?;
        let commit_version = replay
            .commit_version
            .ok_or_else(|| "successor exact replay returned no commit version".to_owned())?;
        if !replay.replayed
            || replay.value.workbench != workbench
            || replay.value.workspace_incarnation_id != workspace_incarnation_id
        {
            return Err("successor did not return the exact durable replay result".to_owned());
        }
        let read = client_b
            .get_workspace(GetWorkspaceRequest {
                workbench: workbench.clone(),
            })
            .map_err(|error| format!("successor exact readback failed: {error}"))?;
        if read.value != replay.value
            || read.value.workspace_incarnation_id != workspace_incarnation_id
        {
            return Err("successor workspace readback differs from the replay result".to_owned());
        }
        stop_and_record_candidate(
            evidence,
            options,
            directory,
            owner_b.as_mut().expect("owner B was installed"),
            false,
        )?;

        Ok(CandidateOrdinaryEvidence {
            prefix_sha256: sha256_bytes(prefix.as_bytes()),
            request_sha256,
            target_key_sha256: sha256_bytes(&target_key),
            owner_a_epoch,
            owner_a_generation,
            owner_b_epoch,
            owner_b_generation,
            first_outcome: "retryable_not_owner_after_real_commit_ack_loss".to_owned(),
            replayed: replay.replayed,
            commit_version,
            seed_failover: "ordered seeds tried the closed owner A endpoint before owner B"
                .to_owned(),
            cleanup_verified: true,
            status: "PASS",
        })
    })();

    let process_cleanup =
        cleanup_candidate_processes(evidence, options, directory, [&mut owner_a, &mut owner_b]);
    let prefix_cleanup = process_cleanup.and_then(|()| {
        cleanup_prefix(runtime, &context).and_then(|()| require_prefix_empty(runtime, &context))
    });
    match (execution, prefix_cleanup) {
        (Ok(result), Ok(())) => evidence.write_json(format!("{directory}/result.json"), &result),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) => Err(format!(
            "candidate ordinary failover completed but cleanup failed: {cleanup}"
        )),
        (Err(primary), Err(cleanup)) => Err(format!(
            "candidate ordinary failover failed: {primary}; cleanup also failed: {cleanup}"
        )),
    }
}

fn candidate_serve_arguments(
    options: &QualificationOptions,
    context: &ScenarioContext,
    object_root: &str,
    endpoint: SocketAddr,
    node_id: &str,
) -> Result<Vec<String>, String> {
    let endpoint = endpoint.to_string();
    let mut arguments = vec![
        "--bind".to_owned(),
        endpoint.clone(),
        "--advertise-endpoint".to_owned(),
        endpoint,
        "--node-id".to_owned(),
        node_id.to_owned(),
    ];
    append_object_arguments(&mut arguments, options, object_root);
    arguments.extend([
        "serve".to_owned(),
        "--meta-url".to_owned(),
        metadata_url(context)?,
    ]);
    Ok(arguments)
}

struct CandidateInjection {
    event_path: PathBuf,
    nonce: String,
}

struct CandidateProcess {
    name: String,
    child: Child,
    exit: Option<ExitStatus>,
    termination: Option<&'static str>,
}

impl CandidateProcess {
    fn require_running(&mut self) -> Result<(), String> {
        self.refresh()?;
        match &self.exit {
            None => Ok(()),
            Some(status) => Err(format!(
                "candidate process {:?} exited early with {:?}",
                self.name,
                status.code()
            )),
        }
    }

    fn refresh(&mut self) -> Result<(), String> {
        if self.exit.is_none() {
            self.exit = self.child.try_wait().map_err(|error| {
                format!("cannot inspect candidate process {:?}: {error}", self.name)
            })?;
            if self.exit.is_some() && self.termination.is_none() {
                self.termination = Some("natural");
            }
        }
        Ok(())
    }

    fn stop_gracefully(&mut self, timeout: Duration) -> Result<(), String> {
        self.refresh()?;
        if self.exit.is_some() {
            return Ok(());
        }
        let signal = Command::new("/bin/kill")
            .args(["-TERM", &self.child.id().to_string()])
            .output()
            .map_err(|error| format!("cannot signal candidate process {:?}: {error}", self.name))?;
        if !signal.status.success() {
            return Err(format!(
                "SIGTERM for candidate process {:?} exited with {:?}",
                self.name,
                signal.status.code()
            ));
        }
        self.termination = Some("sigterm");
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "candidate shutdown deadline overflowed".to_owned())?;
        while Instant::now() < deadline {
            self.refresh()?;
            if self.exit.is_some() {
                return Ok(());
            }
            thread::sleep(CANDIDATE_POLL_INTERVAL);
        }
        self.child.kill().map_err(|error| {
            format!(
                "cannot force-stop candidate process {:?}: {error}",
                self.name
            )
        })?;
        self.exit =
            Some(self.child.wait().map_err(|error| {
                format!("cannot reap candidate process {:?}: {error}", self.name)
            })?);
        self.termination = Some("forced");
        Err(format!(
            "candidate process {:?} did not stop after SIGTERM",
            self.name
        ))
    }
}

impl Drop for CandidateProcess {
    fn drop(&mut self) {
        if self.exit.is_none() {
            let _ = self.child.kill();
            self.exit = self.child.wait().ok();
        }
    }
}

fn spawn_plain_candidate_owner(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    name: &str,
    arguments: &[String],
) -> Result<CandidateProcess, String> {
    let mut command = Command::new(&options.candidate_binary);
    command
        .args(arguments)
        .env("LD_LIBRARY_PATH", fdb_library_path(options)?);
    spawn_candidate_process(evidence, directory, name, &mut command)
}

fn spawn_injected_candidate_owner(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    name: &str,
    arguments: &[String],
    target_key: &[u8],
) -> Result<(CandidateProcess, CandidateInjection), String> {
    let absolute_directory = evidence.root().join(directory);
    fs::create_dir_all(&absolute_directory).map_err(|error| {
        format!(
            "cannot create candidate owner evidence {}: {error}",
            absolute_directory.display()
        )
    })?;
    let event_path = absolute_directory.join(format!("{name}-injector.jsonl"));
    let arm_path = absolute_directory.join(format!("{name}-arm.txt"));
    let nonce = format!("g2_candidate_owner_{:x}", unix_millis());
    validate_token("candidate owner injector nonce", &nonce, 64)?;
    fs::write(&arm_path, format!("arm-v1:{nonce}\n"))
        .map_err(|error| format!("cannot write candidate owner arm message: {error}"))?;
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("exec 8<\"$1\"; exec 9>>\"$2\"; shift 2; exec \"$@\"")
        .arg("nokv-gate2-candidate-owner")
        .arg(&arm_path)
        .arg(&event_path)
        .arg(&options.candidate_binary)
        .args(arguments)
        .env("LD_PRELOAD", &options.shim_library)
        .env("LD_LIBRARY_PATH", fdb_library_path(options)?)
        .env("NOKV_FDB_UNKNOWN_V1", "1")
        .env("NOKV_FDB_UNKNOWN_RUN_NONCE", &nonce)
        .env("NOKV_FDB_UNKNOWN_TARGET_KEY_HEX", lowercase_hex(target_key))
        .env("NOKV_FDB_UNKNOWN_MUTATION", "set")
        .env("NOKV_FDB_UNKNOWN_MODE", "armed")
        .env("NOKV_FDB_UNKNOWN_EXPECTED_MATCHES", "1")
        .env("NOKV_FDB_UNKNOWN_ARM_FD", "8")
        .env("NOKV_FDB_UNKNOWN_EVENT_FD", "9");
    let process = spawn_candidate_process(evidence, directory, name, &mut command)?;
    Ok((process, CandidateInjection { event_path, nonce }))
}

fn spawn_candidate_process(
    evidence: &EvidenceBundle,
    directory: &str,
    name: &str,
    command: &mut Command,
) -> Result<CandidateProcess, String> {
    let absolute_directory = evidence.root().join(directory);
    fs::create_dir_all(&absolute_directory).map_err(|error| {
        format!(
            "cannot create candidate process evidence {}: {error}",
            absolute_directory.display()
        )
    })?;
    let stdout_path = absolute_directory.join(format!("{name}.stdout"));
    let stderr_path = absolute_directory.join(format!("{name}.stderr"));
    let stdout = File::create(&stdout_path)
        .map_err(|error| format!("cannot create {}: {error}", stdout_path.display()))?;
    let stderr = File::create(&stderr_path)
        .map_err(|error| format!("cannot create {}: {error}", stderr_path.display()))?;
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| format!("cannot start candidate process {name:?}: {error}"))?;
    Ok(CandidateProcess {
        name: name.to_owned(),
        child,
        exit: None,
        termination: None,
    })
}

fn stop_and_record_candidate(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    process: &mut CandidateProcess,
    allow_failure: bool,
) -> Result<(), String> {
    let stopped = process.stop_gracefully(CANDIDATE_PROCESS_TIMEOUT);
    redact_candidate_log(evidence.root(), directory, &process.name, options)?;
    evidence.write_json(
        format!("{directory}/{}-status.json", process.name),
        &serde_json::json!({
            "success": process.exit.as_ref().is_some_and(ExitStatus::success),
            "code": process.exit.as_ref().and_then(ExitStatus::code),
            "termination": process.termination,
        }),
    )?;
    stopped?;
    if !allow_failure && !process.exit.as_ref().is_some_and(ExitStatus::success) {
        return Err(format!(
            "candidate process {:?} did not exit successfully",
            process.name
        ));
    }
    Ok(())
}

fn cleanup_candidate_processes(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    processes: [&mut Option<CandidateProcess>; 2],
) -> Result<(), String> {
    let mut failures = Vec::new();
    for process in processes.into_iter().filter_map(Option::as_mut) {
        if let Err(error) = stop_and_record_candidate(evidence, options, directory, process, true) {
            failures.push(error);
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn wait_for_candidate_serving(
    control: &FdbControlStore,
    shard_id: LogicalShardId,
    node: &NodeId,
    endpoint: SocketAddr,
    timeout: Duration,
    process: &mut CandidateProcess,
) -> Result<OwnershipSnapshot, String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "candidate serving deadline overflowed".to_owned())?;
    let mut last = "no ownership observation was made".to_owned();
    while Instant::now() < deadline {
        process.require_running()?;
        match control.observe_ownership(&shard_id) {
            Ok(snapshot)
                if snapshot.route().state() == ShardRouteState::Serving
                    && snapshot.route().owner() == Some(node)
                    && route_endpoint(&snapshot) == Some(endpoint) =>
            {
                return Ok(snapshot);
            }
            Ok(snapshot) => {
                last = format!(
                    "observed {:?} at {:?}",
                    snapshot.route().state(),
                    snapshot.route().endpoint().map(RpcEndpoint::as_str)
                );
            }
            Err(error) => last = error.to_string(),
        }
        thread::sleep(CANDIDATE_POLL_INTERVAL);
    }
    Err(format!(
        "timed out waiting for candidate {:?} to serve at {endpoint}: {last}",
        process.name
    ))
}

fn wait_for_candidate_departure(
    control: &FdbControlStore,
    shard_id: LogicalShardId,
    owner_endpoint: SocketAddr,
    timeout: Duration,
) -> Result<OwnershipSnapshot, String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "candidate departure deadline overflowed".to_owned())?;
    let mut last = "no ownership observation was made".to_owned();
    while Instant::now() < deadline {
        match control.observe_ownership(&shard_id) {
            Ok(snapshot)
                if snapshot.route().state() == ShardRouteState::FailClosed
                    && route_endpoint(&snapshot) == Some(owner_endpoint) =>
            {
                return Ok(snapshot);
            }
            Ok(snapshot)
                if snapshot.route().state() == ShardRouteState::Unassigned
                    && snapshot.session().is_none() =>
            {
                return Ok(snapshot);
            }
            Ok(snapshot) => {
                last = format!(
                    "observed {:?} at {:?}",
                    snapshot.route().state(),
                    snapshot.route().endpoint().map(RpcEndpoint::as_str)
                );
            }
            Err(error) => last = error.to_string(),
        }
        thread::sleep(CANDIDATE_POLL_INTERVAL);
    }
    Err(format!(
        "timed out waiting for owner A to leave Serving: {last}"
    ))
}

fn route_endpoint(snapshot: &OwnershipSnapshot) -> Option<SocketAddr> {
    snapshot.route().endpoint()?.as_str().parse().ok()
}

fn ownership_tokens(snapshot: &OwnershipSnapshot) -> Result<(u64, u64), String> {
    let owner_epoch = snapshot
        .route()
        .owner_epoch()
        .ok_or_else(|| "ownership snapshot has no owner epoch".to_owned())?;
    let session_generation = snapshot
        .route()
        .session_generation()
        .ok_or_else(|| "ownership snapshot has no session generation".to_owned())?;
    Ok((owner_epoch.get(), session_generation.get()))
}

fn validate_candidate_unknown_outcome(error: &ClientError) -> Result<(), String> {
    let ClientError::RetryExhausted {
        attempts: 1,
        last_error,
    } = error
    else {
        return Err(format!(
            "candidate acknowledgement loss returned an unexpected client error: {error}"
        ));
    };
    let ClientError::Rpc(failure) = last_error.as_ref() else {
        return Err(format!(
            "candidate acknowledgement loss did not return a typed RPC failure: {last_error}"
        ));
    };
    if failure.code != ErrorCode::NotOwner
        || failure.conflict != Some(ConflictKind::RootPlacement)
        || !failure.retryable
        || !failure.message.contains("may still commit")
    {
        return Err(format!(
            "candidate acknowledgement loss returned the wrong typed failure: {failure:?}"
        ));
    }
    Ok(())
}

fn candidate_seed_client<const N: usize>(
    root_id: RootIdentity,
    seeds: [SocketAddr; N],
    client_attempts: u32,
    seed_attempts: u32,
) -> Result<WorkspaceClient<FramedTcpTransport, SeedRouteResolver<FramedTcpTransport>>, String> {
    let transport = FramedTcpTransport::new(FramedTcpOptions {
        connect_timeout: Duration::from_millis(500),
        handshake_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
    })
    .map_err(|error| error.to_string())?;
    let resolver = SeedRouteResolver::new(
        transport.clone(),
        seeds,
        SeedRouteOptions {
            max_attempts: seed_attempts,
            initial_backoff: Duration::from_millis(10),
            maximum_backoff: Duration::from_millis(20),
        },
    )
    .map_err(|error| error.to_string())?;
    WorkspaceClient::new(
        root_id,
        transport,
        resolver,
        ClientOptions {
            max_attempts: client_attempts,
        },
    )
    .map_err(|error| error.to_string())
}

fn require_endpoint_closed(endpoint: SocketAddr) -> Result<(), String> {
    match TcpStream::connect_timeout(&endpoint, Duration::from_millis(200)) {
        Err(_) => Ok(()),
        Ok(stream) => {
            drop(stream);
            Err(format!(
                "owner A endpoint {endpoint} still accepted connections after shutdown"
            ))
        }
    }
}

fn redact_candidate_log(
    root: &Path,
    directory: &str,
    name: &str,
    options: &QualificationOptions,
) -> Result<(), String> {
    for extension in ["stdout", "stderr"] {
        let path = root.join(format!("{directory}/{name}.{extension}"));
        if !path.exists() {
            continue;
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("cannot read candidate log {}: {error}", path.display()))?;
        let redacted = redact_candidate_bytes(&bytes, options);
        if redacted != bytes {
            fs::write(&path, redacted).map_err(|error| {
                format!("cannot redact candidate log {}: {error}", path.display())
            })?;
        }
    }
    Ok(())
}

fn redact_candidate_bytes(bytes: &[u8], options: &QualificationOptions) -> Vec<u8> {
    let mut redacted = bytes.to_vec();
    let mut secrets = [
        options.object_access_key_id.as_bytes(),
        options.object_secret_access_key.as_bytes(),
    ];
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    for secret in secrets {
        redacted = replace_bytes(&redacted, secret, b"[REDACTED]");
    }
    redacted
}

fn replace_bytes(haystack: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return haystack.to_vec();
    }
    let mut output = Vec::with_capacity(haystack.len());
    let mut offset = 0;
    while let Some(relative) = haystack[offset..]
        .windows(needle.len())
        .position(|window| window == needle)
    {
        let position = offset + relative;
        output.extend_from_slice(&haystack[offset..position]);
        output.extend_from_slice(replacement);
        offset = position + needle.len();
    }
    output.extend_from_slice(&haystack[offset..]);
    output
}

fn run_plain_candidate(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    name: &str,
    arguments: &[String],
) -> Result<std::process::Output, String> {
    let output = Command::new(&options.candidate_binary)
        .args(arguments)
        .env("LD_LIBRARY_PATH", fdb_library_path(options)?)
        .output()
        .map_err(|error| format!("cannot start candidate {name}: {error}"))?;
    record_named_output(evidence, options, directory, name, &output)?;
    if !output.status.success() {
        return Err(format!(
            "candidate {name} exited with {:?}",
            output.status.code()
        ));
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn run_injected_candidate(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    name: &str,
    arguments: &[String],
    target_key: &[u8],
    mutation_kind: &str,
    ordinal: u64,
    expected_matches: u64,
) -> Result<std::process::Output, String> {
    let absolute_directory = evidence.root().join(directory);
    fs::create_dir_all(&absolute_directory).map_err(|error| {
        format!(
            "cannot create candidate evidence {}: {error}",
            absolute_directory.display()
        )
    })?;
    let event_path = absolute_directory.join(format!("{name}-injector.jsonl"));
    let nonce = format!(
        "g2_candidate_{}_{:x}",
        name.replace('-', "_"),
        unix_millis()
    );
    validate_token("candidate injector nonce", &nonce, 64)?;
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("exec 9>>\"$1\"; shift; exec \"$@\"")
        .arg("nokv-gate2-candidate")
        .arg(&event_path)
        .arg(&options.candidate_binary)
        .args(arguments)
        .env("LD_PRELOAD", &options.shim_library)
        .env("LD_LIBRARY_PATH", fdb_library_path(options)?)
        .env("NOKV_FDB_UNKNOWN_V1", "1")
        .env("NOKV_FDB_UNKNOWN_RUN_NONCE", &nonce)
        .env("NOKV_FDB_UNKNOWN_TARGET_KEY_HEX", lowercase_hex(target_key))
        .env("NOKV_FDB_UNKNOWN_MUTATION", mutation_kind)
        .env("NOKV_FDB_UNKNOWN_MODE", "ordinal")
        .env("NOKV_FDB_UNKNOWN_ORDINAL", ordinal.to_string())
        .env(
            "NOKV_FDB_UNKNOWN_EXPECTED_MATCHES",
            expected_matches.to_string(),
        )
        .env("NOKV_FDB_UNKNOWN_EVENT_FD", "9");
    let output = command
        .output()
        .map_err(|error| format!("cannot start injected candidate {name}: {error}"))?;
    record_named_output(evidence, options, directory, name, &output)?;
    if !output.status.success() {
        return Err(format!(
            "injected candidate {name} exited with {:?}",
            output.status.code()
        ));
    }
    let events = read_injector_events(&event_path)?;
    validate_injector_events_exact(
        &events,
        &nonce,
        &sha256_bytes(target_key),
        mutation_kind,
        "ordinal",
        ordinal,
        expected_matches,
    )?;
    Ok(output)
}

fn record_named_output(
    evidence: &EvidenceBundle,
    options: &QualificationOptions,
    directory: &str,
    name: &str,
    output: &std::process::Output,
) -> Result<(), String> {
    evidence.write_bytes(
        format!("{directory}/{name}.stdout"),
        &redact_candidate_bytes(&output.stdout, options),
    )?;
    evidence.write_bytes(
        format!("{directory}/{name}.stderr"),
        &redact_candidate_bytes(&output.stderr, options),
    )?;
    evidence.write_json(
        format!("{directory}/{name}-status.json"),
        &serde_json::json!({
            "success": output.status.success(),
            "code": output.status.code(),
        }),
    )
}

fn candidate_json(bytes: &[u8], operation: &str) -> Result<Value, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| format!("{operation} stdout is not UTF-8"))?;
    let document = text.trim();
    if document.is_empty() {
        return Err(format!("{operation} emitted no JSON"));
    }
    serde_json::from_str(document).map_err(|error| format!("{operation} JSON is invalid: {error}"))
}

fn candidate_provision_arguments(
    options: &QualificationOptions,
    context: &ScenarioContext,
    object_root: &str,
) -> Result<Vec<String>, String> {
    let mut arguments = vec![
        "--root-id".to_owned(),
        lowercase_hex(context.identity.root_id.as_bytes()),
        "--agent-id".to_owned(),
        lowercase_hex(context.identity.agent_id.as_bytes()),
    ];
    append_object_arguments(&mut arguments, options, object_root);
    arguments.extend([
        "provision".to_owned(),
        "--meta-url".to_owned(),
        metadata_url(context)?,
    ]);
    Ok(arguments)
}

fn append_object_arguments(
    arguments: &mut Vec<String>,
    options: &QualificationOptions,
    object_root: &str,
) {
    arguments.extend([
        "--object-endpoint".to_owned(),
        options.object_endpoint.clone(),
        "--object-bucket".to_owned(),
        options.object_bucket.clone(),
        "--object-region".to_owned(),
        options.object_region.clone(),
        "--object-root".to_owned(),
        object_root.to_owned(),
        "--object-access-key-id".to_owned(),
        options.object_access_key_id.clone(),
        "--object-secret-access-key".to_owned(),
        options.object_secret_access_key.clone(),
    ]);
}

fn metadata_url(context: &ScenarioContext) -> Result<String, String> {
    let cluster_file = context
        .cluster_file
        .to_str()
        .ok_or_else(|| "FDB cluster-file path is not UTF-8".to_owned())?;
    let mut url = url::Url::parse("fdb:///").expect("static FDB URL is valid");
    url.set_path(cluster_file);
    url.query_pairs_mut().append_pair("prefix", &context.prefix);
    Ok(url.to_string())
}

fn candidate_metadata_key(
    context: &ScenarioContext,
    keyspace: u16,
    logical_key: &[u8],
) -> Result<Vec<u8>, String> {
    let prefix =
        FdbStorePrefix::new(context.prefix.as_bytes()).map_err(|error| error.to_string())?;
    let subspace = prefix
        .subspace(nokv_fdb::FdbSubspaceKind::Metadata)
        .component(&keyspace.to_be_bytes())
        .map_err(|error| error.to_string())?;
    Ok(subspace.key(logical_key))
}

fn derive_logical_shard_id(store_id: StoreId) -> LogicalShardId {
    LogicalShardId::from_bytes(derive_candidate_identity(
        b"nokv/fdb/logical-shard-id/v1\0",
        store_id.as_bytes(),
    ))
}

fn derive_namespace_id(store_id: StoreId) -> ObjectNamespaceId {
    ObjectNamespaceId::from_bytes(derive_candidate_identity(
        b"nokv/fdb/object-namespace-id/v1\0",
        store_id.as_bytes(),
    ))
}

fn derive_candidate_identity(domain: &[u8], value: &[u8]) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(value);
    let digest = digest.finalize();
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest[..16]);
    if identity.iter().all(|byte| *byte == 0) {
        identity[15] = 1;
    }
    identity
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
        .env("NOKV_FDB_UNKNOWN_EXPECTED_MATCHES", "1")
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
            object_binding_sha256: sha256_bytes(
                format!(
                    "{}\0{}\0{}\0{}",
                    options.object_endpoint,
                    options.object_bucket,
                    options.object_region,
                    options.object_root_base
                )
                .as_bytes(),
            ),
            owner_endpoints: [options.owner_a_endpoint, options.owner_b_endpoint],
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

fn count_candidate_evidence(root: &Path) -> Result<usize, String> {
    let mut count = 0;
    for relative in [
        "scenarios/candidate-bootstrap",
        "scenarios/candidate-ordinary",
    ] {
        let start = root.join(relative);
        if !start.exists() {
            continue;
        }
        let mut stack = vec![start];
        while let Some(directory) = stack.pop() {
            for entry in fs::read_dir(&directory)
                .map_err(|error| format!("cannot count candidate evidence: {error}"))?
            {
                let path = entry
                    .map_err(|error| format!("cannot read candidate evidence entry: {error}"))?
                    .path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.file_name().and_then(|name| name.to_str()) == Some("result.json") {
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}

fn verify_evidence_redaction(root: &Path, options: &QualificationOptions) -> Result<(), String> {
    let secrets = [
        options.object_access_key_id.as_bytes(),
        options.object_secret_access_key.as_bytes(),
    ];
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("cannot scan evidence credentials: {error}"))?
        {
            let path = entry
                .map_err(|error| format!("cannot read evidence credential-scan entry: {error}"))?
                .path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let bytes = fs::read(&path).map_err(|error| {
                format!(
                    "cannot read evidence file {} for credential scan: {error}",
                    path.display()
                )
            })?;
            if secrets
                .iter()
                .any(|secret| bytes.windows(secret.len()).any(|window| window == *secret))
            {
                let relative = path.strip_prefix(root).unwrap_or(&path);
                return Err(format!(
                    "evidence credential scan found an unredacted value in {}",
                    relative.display()
                ));
            }
        }
    }
    Ok(())
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

fn parse_endpoint(flag: &str, value: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|error| format!("{flag} must be a socket address: {error}"))
}

fn usage() -> &'static str {
    "usage: nokv-fdb-unknown-outcome-qualification \\\n  --candidate-binary ABSOLUTE --shim-library ABSOLUTE \\\n  --fdb-cluster-file ABSOLUTE --fdb-client-library ABSOLUTE \\\n  --fdbcli ABSOLUTE --curl ABSOLUTE --fdb-prefix-base TOKEN \\\n  --rustfs-health-url URL --rustfs-service-identity ID \\\n  --object-endpoint URL --object-bucket NAME --object-region NAME \\\n  --object-root-base PREFIX --object-access-key-id VALUE \\\n  --object-secret-access-key VALUE --owner-a-endpoint IP:PORT \\\n  --owner-b-endpoint IP:PORT --evidence-dir ABSOLUTE \\\n  --source-revision HEX --source-dirty true|false"
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

    #[test]
    fn evidence_redaction_replaces_every_exact_occurrence() {
        assert_eq!(
            replace_bytes(
                b"before-secret-middle-secret-after",
                b"secret",
                b"[REDACTED]"
            ),
            b"before-[REDACTED]-middle-[REDACTED]-after"
        );
        assert_eq!(replace_bytes(b"unchanged", b"secret", b"x"), b"unchanged");
    }

    #[test]
    fn usage_has_no_patch_markers() {
        assert!(!usage().contains("\n+"));
        assert!(usage().contains("--object-secret-access-key"));
        assert!(usage().contains("--owner-b-endpoint"));
    }

    #[test]
    fn candidate_json_accepts_pretty_cli_output() {
        let parsed = candidate_json(b"{\n  \"created\": true\n}\n", "format").unwrap();
        assert_eq!(parsed.get("created"), Some(&Value::Bool(true)));
    }
}
