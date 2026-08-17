/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Feature-gated owner used only by the restore crash-consistency gate.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nokv_client::{
    ClientOptions, ControlRouteResolver, EtcdRouteOptions, FramedTcpOptions, FramedTcpTransport,
    RestoreWorkflowIdentities, WorkspaceClient,
};
use nokv_control::{
    ControlStore, EtcdControlStore, EtcdControlStoreOptions, NodeId, RecoveryPublication, RootId,
    RootPlacement, RootPlacementLifecycle,
};
use nokv_object::{
    admit_artifact_provider, ArtifactObjectStore, ArtifactStoreCapabilities, BoundArtifactStore,
    ImmutableCreateOutcome, ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange,
    ProviderAdmissionProfile, ProviderAdmissionReceipt, ProviderHandleIdentity, S3ArtifactStore,
    S3ArtifactStoreOptions, DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE,
};
use nokv_protocol::{
    GetOperationRequest, GetWorkspaceRequest, OperationIdentity, RootIdentity, WorkbenchName,
    WorkspaceIdentity,
};
use nokv_server::{
    bootstrap_shard, LeaseMode, MetadataWorkspaceRequestExecutor, OpenMode,
    RestoreInitializationBarrier, RestoreInitializationBarrierEvidence, RootAttach,
    RootOwnerRegistry, ServerOptions, ShardBoot, WorkspaceRequestExecutor, WorkspaceServer,
};
use nokv_types::{RequestId, FIXED_ID_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const ARM_SCHEMA: &str = "nokv.restore-crash.arm.v1";
const EVIDENCE_SCHEMA: &str = "nokv.restore-crash.evidence.v1";
const MAX_ARM_BYTES: u64 = 64 * 1024;
const EXIT_CRASH_REACHED: i32 = 86;
const EXIT_CRASH_INVALID: i32 = 87;
const DEFAULT_ETCD_KEY_PREFIX: &str = "/nokv/control";
const DEFAULT_LEASE_TTL_SECONDS: i64 = 10;
const DEFAULT_HANDSHAKE_TIMEOUT_MILLIS: u64 = 5_000;
const DEFAULT_MAX_INFLIGHT_CONNECTIONS: usize = 256;

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CrashArm {
    schema: String,
    run_id: String,
    root_id: RootIdentity,
    source_workbench: WorkbenchName,
    source_workspace_incarnation_id: WorkspaceIdentity,
    snapshot_id: u64,
    destination_workbench: WorkbenchName,
    destination_workspace_incarnation_id: WorkspaceIdentity,
    operation_id: OperationIdentity,
}

impl CrashArm {
    fn validate(&self) -> Result<(), String> {
        if self.schema != ARM_SCHEMA {
            return Err(format!(
                "unsupported restore crash arm schema {:?}",
                self.schema
            ));
        }
        if self.run_id.is_empty()
            || self.run_id.len() > 128
            || !self
                .run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("restore crash arm run_id must be 1..=128 portable ASCII bytes".to_owned());
        }
        if self.root_id.0 == [0; FIXED_ID_BYTES]
            || self.source_workspace_incarnation_id.0 == [0; FIXED_ID_BYTES]
            || self.destination_workspace_incarnation_id.0 == [0; FIXED_ID_BYTES]
            || self.operation_id.0 == [0; FIXED_ID_BYTES]
        {
            return Err("restore crash arm identities must not be zero".to_owned());
        }
        if self.snapshot_id == 0 || self.source_workbench == self.destination_workbench {
            return Err(
                "restore crash arm requires a concrete snapshot and distinct workbenches"
                    .to_owned(),
            );
        }
        let expected = RestoreWorkflowIdentities::derive_snapshot(
            self.root_id,
            &self.source_workbench,
            self.source_workspace_incarnation_id,
            self.snapshot_id,
            &self.destination_workbench,
        );
        if expected.operation_id != self.operation_id
            || expected.destination_workspace_incarnation_id
                != self.destination_workspace_incarnation_id
        {
            return Err(
                "restore crash arm identities do not match the public workflow derivation"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct CrashEvidenceEnvelope<'a> {
    schema: &'static str,
    run_id: &'a str,
    root_id: RootIdentity,
    operation_id: OperationIdentity,
    evidence: &'a RestoreInitializationBarrierEvidence,
}

struct CrashBarrier {
    arm: CrashArm,
    evidence_path: PathBuf,
    state: AtomicU8,
}

impl CrashBarrier {
    fn new(arm: CrashArm, evidence_path: PathBuf) -> Self {
        Self {
            arm,
            evidence_path,
            state: AtomicU8::new(0),
        }
    }
}

impl RestoreInitializationBarrier for CrashBarrier {
    fn reached(
        &self,
        evidence: Result<RestoreInitializationBarrierEvidence, nokv_protocol::RpcFailure>,
    ) -> ! {
        match self
            .state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let Ok(evidence) = evidence else {
                    process_exit(EXIT_CRASH_INVALID);
                };
                if evidence.route.root_id != self.arm.root_id
                    || evidence.operation_id != self.arm.operation_id
                {
                    process_exit(EXIT_CRASH_INVALID);
                }
                let envelope = CrashEvidenceEnvelope {
                    schema: EVIDENCE_SCHEMA,
                    run_id: &self.arm.run_id,
                    root_id: self.arm.root_id,
                    operation_id: self.arm.operation_id,
                    evidence: &evidence,
                };
                match write_create_new_synced(&self.evidence_path, &envelope) {
                    Ok(()) => process_exit(EXIT_CRASH_REACHED),
                    Err(_) => process_exit(EXIT_CRASH_INVALID),
                }
            }
            Err(1) => loop {
                std::thread::park();
            },
            Err(_) => process_exit(EXIT_CRASH_INVALID),
        }
    }
}

fn process_exit(code: i32) -> ! {
    // SAFETY: this binary is a dedicated process-level fault injector. It must
    // not run Rust destructors or release the owner lease on either outcome.
    unsafe { libc::_exit(code) }
}

fn write_create_new_synced(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let encoded = serde_json::to_vec(value).map_err(io::Error::other)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&encoded)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    File::open(parent.unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    Ok(())
}

fn read_arm(path: &Path) -> Result<CrashArm, String> {
    let metadata = fs::metadata(path).map_err(|error| {
        format!(
            "cannot inspect restore crash arm {}: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() || metadata.len() > MAX_ARM_BYTES {
        return Err(format!(
            "restore crash arm {} must be a regular file no larger than {MAX_ARM_BYTES} bytes",
            path.display()
        ));
    }
    let mut encoded = String::new();
    File::open(path)
        .and_then(|mut file| file.read_to_string(&mut encoded))
        .map_err(|error| format!("cannot read restore crash arm {}: {error}", path.display()))?;
    let arm: CrashArm = serde_json::from_str(&encoded)
        .map_err(|error| format!("invalid restore crash arm {}: {error}", path.display()))?;
    arm.validate()?;
    Ok(arm)
}

#[derive(Clone, Debug)]
struct ClientRouteConfig {
    etcd_endpoints: Vec<String>,
    etcd_key_prefix: String,
    lease_ttl_seconds: i64,
    root_id: RootIdentity,
}

type EtcdWorkspaceClient = WorkspaceClient<FramedTcpTransport, ControlRouteResolver>;

fn workspace_client(config: &ClientRouteConfig) -> Result<EtcdWorkspaceClient, String> {
    let routes = ControlRouteResolver::connect_etcd(
        EtcdRouteOptions::new(config.etcd_endpoints.clone())
            .with_key_prefix(config.etcd_key_prefix.clone())
            .with_lease_ttl_seconds(config.lease_ttl_seconds),
    )
    .map_err(|error| error.to_string())?;
    let transport =
        FramedTcpTransport::new(FramedTcpOptions::default()).map_err(|error| error.to_string())?;
    WorkspaceClient::new(
        config.root_id,
        transport,
        routes,
        ClientOptions { max_attempts: 3 },
    )
    .map_err(|error| error.to_string())
}

#[derive(Debug)]
struct ArmConfig {
    route: ClientRouteConfig,
    run_id: String,
    source_workbench: WorkbenchName,
    snapshot_id: u64,
    destination_workbench: WorkbenchName,
    output: PathBuf,
}

fn parse_arm_config(arguments: impl IntoIterator<Item = String>) -> Result<ArmConfig, String> {
    let mut arguments = arguments.into_iter();
    let mut etcd_endpoints = Vec::new();
    let mut etcd_key_prefix = DEFAULT_ETCD_KEY_PREFIX.to_owned();
    let mut lease_ttl_seconds = DEFAULT_LEASE_TTL_SECONDS;
    let mut root_id = None;
    let mut run_id = None;
    let mut source_workbench = None;
    let mut snapshot_id = None;
    let mut destination_workbench = None;
    let mut output = None;
    while let Some(argument) = arguments.next() {
        let mut value = |name: &str| {
            arguments
                .next()
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match argument.as_str() {
            "--etcd-endpoint" => etcd_endpoints.push(value("--etcd-endpoint")?),
            "--etcd-key-prefix" => etcd_key_prefix = value("--etcd-key-prefix")?,
            "--lease-ttl-seconds" => {
                lease_ttl_seconds = parse_number(&value("--lease-ttl-seconds")?, argument.as_str())?
            }
            "--root-id" => {
                root_id = Some(RootIdentity(parse_fixed_hex::<FIXED_ID_BYTES>(
                    &value("--root-id")?,
                    "--root-id",
                )?))
            }
            "--run-id" => run_id = Some(value("--run-id")?),
            "--source-workbench" => {
                let raw = value("--source-workbench")?;
                source_workbench = Some(
                    WorkbenchName::new(&raw)
                        .map_err(|error| format!("invalid --source-workbench: {error}"))?,
                )
            }
            "--snapshot-id" => {
                snapshot_id = Some(parse_number(&value("--snapshot-id")?, argument.as_str())?)
            }
            "--destination-workbench" => {
                let raw = value("--destination-workbench")?;
                destination_workbench = Some(
                    WorkbenchName::new(&raw)
                        .map_err(|error| format!("invalid --destination-workbench: {error}"))?,
                )
            }
            "--output" => output = Some(PathBuf::from(value("--output")?)),
            _ => return Err(format!("unknown restore crash arm option {argument:?}")),
        }
    }
    if etcd_endpoints.is_empty() {
        return Err("at least one --etcd-endpoint is required".to_owned());
    }
    if lease_ttl_seconds <= 0 {
        return Err("--lease-ttl-seconds must be positive".to_owned());
    }
    Ok(ArmConfig {
        route: ClientRouteConfig {
            etcd_endpoints,
            etcd_key_prefix,
            lease_ttl_seconds,
            root_id: root_id.ok_or_else(|| "--root-id is required".to_owned())?,
        },
        run_id: run_id.ok_or_else(|| "--run-id is required".to_owned())?,
        source_workbench: source_workbench
            .ok_or_else(|| "--source-workbench is required".to_owned())?,
        snapshot_id: snapshot_id.ok_or_else(|| "--snapshot-id is required".to_owned())?,
        destination_workbench: destination_workbench
            .ok_or_else(|| "--destination-workbench is required".to_owned())?,
        output: output.ok_or_else(|| "--output is required".to_owned())?,
    })
}

fn run_arm(config: ArmConfig) -> Result<(), String> {
    let client = workspace_client(&config.route)?;
    let source = client
        .get_workspace(GetWorkspaceRequest {
            workbench: config.source_workbench.clone(),
        })
        .map_err(|error| error.to_string())?
        .value;
    let identities = RestoreWorkflowIdentities::derive_snapshot(
        config.route.root_id,
        &config.source_workbench,
        source.workspace_incarnation_id,
        config.snapshot_id,
        &config.destination_workbench,
    );
    let arm = CrashArm {
        schema: ARM_SCHEMA.to_owned(),
        run_id: config.run_id,
        root_id: config.route.root_id,
        source_workbench: config.source_workbench,
        source_workspace_incarnation_id: source.workspace_incarnation_id,
        snapshot_id: config.snapshot_id,
        destination_workbench: config.destination_workbench,
        destination_workspace_incarnation_id: identities.destination_workspace_incarnation_id,
        operation_id: identities.operation_id,
    };
    arm.validate()?;
    write_create_new_synced(&config.output, &arm)
        .map_err(|error| format!("cannot create restore crash arm: {error}"))?;
    println!(
        "{}",
        serde_json::to_string(&arm).map_err(|error| error.to_string())?
    );
    Ok(())
}

#[derive(Debug)]
struct InspectConfig {
    route: ClientRouteConfig,
    operation_id: OperationIdentity,
}

fn parse_inspect_config(
    arguments: impl IntoIterator<Item = String>,
) -> Result<InspectConfig, String> {
    let mut arguments = arguments.into_iter();
    let mut etcd_endpoints = Vec::new();
    let mut etcd_key_prefix = DEFAULT_ETCD_KEY_PREFIX.to_owned();
    let mut lease_ttl_seconds = DEFAULT_LEASE_TTL_SECONDS;
    let mut root_id = None;
    let mut operation_id = None;
    while let Some(argument) = arguments.next() {
        let mut value = |name: &str| {
            arguments
                .next()
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match argument.as_str() {
            "--etcd-endpoint" => etcd_endpoints.push(value("--etcd-endpoint")?),
            "--etcd-key-prefix" => etcd_key_prefix = value("--etcd-key-prefix")?,
            "--lease-ttl-seconds" => {
                lease_ttl_seconds = parse_number(&value("--lease-ttl-seconds")?, argument.as_str())?
            }
            "--root-id" => {
                root_id = Some(RootIdentity(parse_fixed_hex::<FIXED_ID_BYTES>(
                    &value("--root-id")?,
                    "--root-id",
                )?))
            }
            "--operation-id" => {
                operation_id = Some(OperationIdentity(parse_fixed_hex::<FIXED_ID_BYTES>(
                    &value("--operation-id")?,
                    "--operation-id",
                )?))
            }
            _ => return Err(format!("unknown restore crash inspect option {argument:?}")),
        }
    }
    if etcd_endpoints.is_empty() {
        return Err("at least one --etcd-endpoint is required".to_owned());
    }
    if lease_ttl_seconds <= 0 {
        return Err("--lease-ttl-seconds must be positive".to_owned());
    }
    Ok(InspectConfig {
        route: ClientRouteConfig {
            etcd_endpoints,
            etcd_key_prefix,
            lease_ttl_seconds,
            root_id: root_id.ok_or_else(|| "--root-id is required".to_owned())?,
        },
        operation_id: operation_id.ok_or_else(|| "--operation-id is required".to_owned())?,
    })
}

fn run_inspect(config: InspectConfig) -> Result<(), String> {
    let client = workspace_client(&config.route)?;
    let operation = client
        .get_operation(GetOperationRequest {
            operation_id: config.operation_id,
        })
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "schema": "nokv.restore-crash.operation-inspection.v1",
            "root_id": config.route.root_id,
            "operation_id": config.operation_id,
            "commit_version": operation.commit_version,
            "replayed": operation.replayed,
            "operation": operation.value,
        }))
        .map_err(|error| error.to_string())?
    );
    Ok(())
}

#[derive(Debug)]
struct ServeConfig {
    etcd_endpoints: Vec<String>,
    etcd_key_prefix: String,
    lease_ttl_seconds: i64,
    root_id: RootId,
    node_id: String,
    advertise_endpoint: String,
    bind: SocketAddr,
    metadata_reopen: PathBuf,
    object_bucket: String,
    object_endpoint: Option<String>,
    object_root: String,
    object_region: String,
    object_access_key_id: Option<String>,
    object_secret_access_key: Option<String>,
    object_session_token: Option<String>,
    arm_file: PathBuf,
    evidence_file: PathBuf,
    handshake_timeout_millis: u64,
    max_inflight_connections: usize,
}

fn parse_serve_config(arguments: impl IntoIterator<Item = String>) -> Result<ServeConfig, String> {
    let mut arguments = arguments.into_iter();
    let mut etcd_endpoints = Vec::new();
    let mut etcd_key_prefix = DEFAULT_ETCD_KEY_PREFIX.to_owned();
    let mut lease_ttl_seconds = DEFAULT_LEASE_TTL_SECONDS;
    let mut root_id = None;
    let mut node_id = None;
    let mut advertise_endpoint = None;
    let mut bind = None;
    let mut metadata_reopen = None;
    let mut object_bucket = None;
    let mut object_endpoint = None;
    let mut object_root = "/".to_owned();
    let mut object_region = "us-east-1".to_owned();
    let mut object_access_key_id = None;
    let mut object_secret_access_key = None;
    let mut object_session_token = None;
    let mut arm_file = None;
    let mut evidence_file = None;
    let mut handshake_timeout_millis = DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
    let mut max_inflight_connections = DEFAULT_MAX_INFLIGHT_CONNECTIONS;

    while let Some(argument) = arguments.next() {
        let mut value = |name: &str| {
            arguments
                .next()
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match argument.as_str() {
            "--etcd-endpoint" => etcd_endpoints.push(value("--etcd-endpoint")?),
            "--etcd-key-prefix" => etcd_key_prefix = value("--etcd-key-prefix")?,
            "--lease-ttl-seconds" => {
                lease_ttl_seconds = parse_number(&value("--lease-ttl-seconds")?, argument.as_str())?
            }
            "--root-id" => {
                root_id = Some(RootId::from_bytes(parse_fixed_hex::<FIXED_ID_BYTES>(
                    &value("--root-id")?,
                    "--root-id",
                )?))
            }
            "--node-id" => node_id = Some(value("--node-id")?),
            "--advertise-endpoint" => advertise_endpoint = Some(value("--advertise-endpoint")?),
            "--bind" => {
                let raw = value("--bind")?;
                bind = Some(
                    raw.parse()
                        .map_err(|error| format!("invalid --bind {raw:?}: {error}"))?,
                )
            }
            "--metadata-reopen" => {
                metadata_reopen = Some(PathBuf::from(value("--metadata-reopen")?))
            }
            "--object-bucket" => object_bucket = Some(value("--object-bucket")?),
            "--object-endpoint" => object_endpoint = Some(value("--object-endpoint")?),
            "--object-root" => object_root = value("--object-root")?,
            "--object-region" => object_region = value("--object-region")?,
            "--object-access-key-id" => {
                object_access_key_id = Some(value("--object-access-key-id")?)
            }
            "--object-secret-access-key" => {
                object_secret_access_key = Some(value("--object-secret-access-key")?)
            }
            "--object-session-token" => {
                object_session_token = Some(value("--object-session-token")?)
            }
            "--arm-file" => arm_file = Some(PathBuf::from(value("--arm-file")?)),
            "--evidence-file" => evidence_file = Some(PathBuf::from(value("--evidence-file")?)),
            "--handshake-timeout-millis" => {
                handshake_timeout_millis =
                    parse_number(&value("--handshake-timeout-millis")?, argument.as_str())?
            }
            "--max-inflight-connections" => {
                max_inflight_connections =
                    parse_number(&value("--max-inflight-connections")?, argument.as_str())?
            }
            _ => return Err(format!("unknown restore crash owner option {argument:?}")),
        }
    }

    if etcd_endpoints.is_empty() {
        return Err("at least one --etcd-endpoint is required".to_owned());
    }
    if lease_ttl_seconds <= 0 {
        return Err("--lease-ttl-seconds must be positive".to_owned());
    }
    if handshake_timeout_millis == 0 || handshake_timeout_millis > 60_000 {
        return Err("--handshake-timeout-millis must be within 1..=60000".to_owned());
    }
    if max_inflight_connections == 0 || max_inflight_connections > 4_096 {
        return Err("--max-inflight-connections must be within 1..=4096".to_owned());
    }
    if object_access_key_id.is_some() != object_secret_access_key.is_some() {
        return Err(
            "--object-access-key-id and --object-secret-access-key must be set together".to_owned(),
        );
    }
    if object_session_token.is_some() && object_access_key_id.is_none() {
        return Err("--object-session-token requires object credentials".to_owned());
    }
    Ok(ServeConfig {
        etcd_endpoints,
        etcd_key_prefix,
        lease_ttl_seconds,
        root_id: root_id.ok_or_else(|| "--root-id is required".to_owned())?,
        node_id: node_id.ok_or_else(|| "--node-id is required".to_owned())?,
        advertise_endpoint: advertise_endpoint
            .ok_or_else(|| "--advertise-endpoint is required".to_owned())?,
        bind: bind.ok_or_else(|| "--bind is required".to_owned())?,
        metadata_reopen: metadata_reopen
            .ok_or_else(|| "--metadata-reopen is required".to_owned())?,
        object_bucket: object_bucket.ok_or_else(|| "--object-bucket is required".to_owned())?,
        object_endpoint,
        object_root,
        object_region,
        object_access_key_id,
        object_secret_access_key,
        object_session_token,
        arm_file: arm_file.ok_or_else(|| "--arm-file is required".to_owned())?,
        evidence_file: evidence_file.ok_or_else(|| "--evidence-file is required".to_owned())?,
        handshake_timeout_millis,
        max_inflight_connections,
    })
}

fn parse_number<T>(value: &str, option: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {option} {value:?}: {error}"))
}

fn parse_fixed_hex<const N: usize>(value: &str, field: &str) -> Result<[u8; N], String> {
    if value.len() != N * 2 || !value.is_ascii() {
        return Err(format!("{field} must contain exactly {} hex digits", N * 2));
    }
    let mut decoded = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] =
            (decode_hex_nibble(pair[0], field)? << 4) | decode_hex_nibble(pair[1], field)?;
    }
    Ok(decoded)
}

fn decode_hex_nibble(value: u8, field: &str) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(format!("{field} contains a non-hex byte")),
    }
}

fn active_shard_placements(
    control: &dyn ControlStore,
    seed: &RootPlacement,
) -> Result<Vec<RootPlacement>, String> {
    let mut placements = control
        .list_root_placements()
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|placement| {
            placement.logical_shard_id == seed.logical_shard_id
                && placement.lifecycle == RootPlacementLifecycle::Active
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

fn bootstrap_request_id(domain: &[u8], root_id: RootId) -> RequestId {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore-crash-owner.bootstrap\0");
    hasher.update(domain);
    hasher.update(root_id.as_bytes());
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
    let mut request = [0_u8; FIXED_ID_BYTES];
    request.copy_from_slice(&digest[..FIXED_ID_BYTES]);
    RequestId::from_bytes(request)
}

#[derive(Debug)]
struct AdmittedRecoveryObjectStore {
    inner: BoundArtifactStore<S3ArtifactStore>,
    admission: ProviderAdmissionReceipt,
}

impl ArtifactObjectStore for AdmittedRecoveryObjectStore {
    fn object_namespace(&self) -> Option<nokv_types::ObjectNamespaceId> {
        Some(self.inner.namespace_id())
    }

    fn capabilities(&self) -> ArtifactStoreCapabilities {
        self.inner.capabilities()
    }

    fn provider_handle_identity(&self) -> ProviderHandleIdentity {
        self.inner.provider_handle_identity()
    }

    fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
        Some(&self.admission)
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        self.inner.create_immutable(key, bytes)
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.read(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        self.inner.delete(key)
    }
}

fn recovery_object_store(
    config: &ServeConfig,
    namespace: nokv_types::ObjectNamespaceId,
) -> Result<Arc<dyn ArtifactObjectStore>, String> {
    let raw = S3ArtifactStore::new(S3ArtifactStoreOptions {
        bucket: config.object_bucket.clone(),
        root: config.object_root.clone(),
        region: config.object_region.clone(),
        endpoint: config.object_endpoint.clone(),
        access_key_id: config.object_access_key_id.clone(),
        secret_access_key: config.object_secret_access_key.clone(),
        session_token: config.object_session_token.clone(),
        virtual_host_style: false,
        skip_signature: false,
    })
    .map_err(|error| error.to_string())?;
    let profile = ProviderAdmissionProfile::single_put(DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE)
        .map_err(|error| error.to_string())?;
    let admission = admit_artifact_provider(&raw, profile).map_err(|error| error.to_string())?;
    let inner = BoundArtifactStore::open(raw, namespace).map_err(|error| error.to_string())?;
    Ok(Arc::new(AdmittedRecoveryObjectStore { inner, admission }))
}

fn run_server(config: ServeConfig) -> Result<(), String> {
    let arm = read_arm(&config.arm_file)?;
    let configured_root = RootIdentity(*config.root_id.as_bytes());
    if arm.root_id != configured_root {
        return Err("restore crash arm root_id does not match --root-id".to_owned());
    }
    let options = EtcdControlStoreOptions::new(config.etcd_endpoints.clone())
        .with_key_prefix(config.etcd_key_prefix.clone())
        .with_lease_ttl_seconds(config.lease_ttl_seconds);
    let control: Arc<dyn ControlStore> =
        Arc::new(EtcdControlStore::connect(options).map_err(|error| error.to_string())?);
    let placement = control
        .get_root_placement(&config.root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "root placement does not exist".to_owned())?;
    let placements = active_shard_placements(control.as_ref(), &placement)?;
    for placement in &placements {
        let binding = control
            .get_root_agent_binding(&placement.root_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "every served root requires an Agent binding".to_owned())?;
        if binding.root_id != placement.root_id {
            return Err("root Agent binding key/value identity mismatch".to_owned());
        }
    }
    let namespace = control
        .get_root_object_namespace_binding(&config.root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "root object namespace is not bound".to_owned())?
        .object_namespace_id;
    for placement in &placements {
        let binding = control
            .get_root_object_namespace_binding(&placement.root_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "every served root requires an object namespace".to_owned())?;
        if binding.object_namespace_id != namespace {
            return Err("one owner cannot serve roots from different object namespaces".to_owned());
        }
    }
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "logical shard does not exist".to_owned())?;
    let recovery_objects = recovery_object_store(&config, namespace)?;
    let registry = Arc::new(RootOwnerRegistry::new());
    let owner = bootstrap_shard(
        Arc::clone(&control),
        Arc::clone(&registry),
        recovery_objects,
        ShardBoot {
            shard_id: placement.logical_shard_id,
            open: OpenMode::Existing(config.metadata_reopen),
            lease: LeaseMode::Acquire {
                owner: NodeId::new(config.node_id)
                    .map_err(|error| format!("invalid --node-id: {error:?}"))?,
                endpoint: config.advertise_endpoint,
                previous_epoch: shard.owner_epoch,
            },
            recovery: RecoveryPublication {
                checkpoint: shard.checkpoint,
                log: shard.log,
                durable_lsn: shard.durable_lsn,
            },
            roots: placements
                .iter()
                .map(|placement| RootAttach {
                    root_id: placement.root_id,
                    object_namespace_id: namespace,
                    install_id: bootstrap_request_id(b"install", placement.root_id),
                    bind_object_namespace_id: bootstrap_request_id(
                        b"bind-object-namespace",
                        placement.root_id,
                    ),
                    activate_id: bootstrap_request_id(b"activate", placement.root_id),
                })
                .collect(),
        },
    )
    .map_err(|error| error.to_string())?;

    let target_route = owner
        .routes()
        .iter()
        .copied()
        .find(|route| route.root_id == configured_root)
        .ok_or_else(|| "bootstrapped owner omitted the armed root route".to_owned())?;
    let barrier: Arc<dyn RestoreInitializationBarrier> =
        Arc::new(CrashBarrier::new(arm.clone(), config.evidence_file));
    let executor: Arc<dyn WorkspaceRequestExecutor> = Arc::new(
        MetadataWorkspaceRequestExecutor::new(Arc::clone(owner.meta()))
            .with_restore_initialization_barrier(arm.operation_id, barrier),
    );
    registry
        .install(target_route, executor)
        .map_err(|error| error.to_string())?;

    let renew_seconds = u64::try_from(config.lease_ttl_seconds)
        .map_err(|_| "etcd lease TTL must be positive".to_owned())?
        .saturating_div(3)
        .max(1);
    let server = WorkspaceServer::new(
        ServerOptions {
            bind: config.bind,
            handshake_timeout: Duration::from_millis(config.handshake_timeout_millis),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(renew_seconds),
            max_inflight_connections: config.max_inflight_connections,
        },
        registry,
        vec![owner],
    )
    .map_err(|error| error.to_string())?;
    let server_result = server.run();
    let release_result = server.release_ownership();
    match (server_result, release_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(server), Ok(())) => Err(format!("RPC server stopped: {server}")),
        (Ok(()), Err(release)) => Err(format!("owner release failed: {release}")),
        (Err(server), Err(release)) => Err(format!(
            "RPC server stopped: {server}; owner release failed: {release}"
        )),
    }
}

fn usage() -> &'static str {
    "usage:\n  nokv-restore-crash-owner arm --etcd-endpoint URL --root-id HEX32 --run-id ID --source-workbench NAME --snapshot-id N --destination-workbench NAME --output PATH\n  nokv-restore-crash-owner inspect --etcd-endpoint URL --root-id HEX32 --operation-id HEX32\n  nokv-restore-crash-owner serve --etcd-endpoint URL [--etcd-endpoint URL ...] --etcd-key-prefix PREFIX --lease-ttl-seconds N --root-id HEX32 --node-id ID --advertise-endpoint HOST:PORT --bind HOST:PORT --metadata-reopen PATH --object-bucket BUCKET [--object-endpoint URL] [--object-root PREFIX] [--object-region REGION] [--object-access-key-id ID --object-secret-access-key SECRET] --arm-file PATH --evidence-file PATH"
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("arm") => run_arm(parse_arm_config(arguments)?),
        Some("inspect") => run_inspect(parse_inspect_config(arguments)?),
        Some("serve") => run_server(parse_serve_config(arguments)?),
        Some("--help" | "-h") => {
            println!("{}", usage());
            Ok(())
        }
        Some(command) => Err(format!("unknown command {command:?}; {}", usage())),
        None => Err(usage().to_owned()),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("nokv-restore-crash-owner: {error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_protocol::{
        CommitIdentity, Digest, LogicalShardIdentity, RootRoute, WorkspaceIdentity,
    };
    use nokv_server::{
        RestoreInitializationBarrierPhase, RestoreManifestBindingEvidence,
        RestoreManifestPublicationEvidence,
    };

    fn sample_arm() -> CrashArm {
        let root_id = RootIdentity([0x11; FIXED_ID_BYTES]);
        let source_workbench = WorkbenchName::new("source-run").unwrap();
        let source_workspace_incarnation_id = WorkspaceIdentity([0x22; FIXED_ID_BYTES]);
        let snapshot_id = 7;
        let destination_workbench = WorkbenchName::new("restored-run").unwrap();
        let identities = RestoreWorkflowIdentities::derive_snapshot(
            root_id,
            &source_workbench,
            source_workspace_incarnation_id,
            snapshot_id,
            &destination_workbench,
        );
        CrashArm {
            schema: ARM_SCHEMA.to_owned(),
            run_id: "gate-run-1".to_owned(),
            root_id,
            source_workbench,
            source_workspace_incarnation_id,
            snapshot_id,
            destination_workbench,
            destination_workspace_incarnation_id: identities.destination_workspace_incarnation_id,
            operation_id: identities.operation_id,
        }
    }

    fn sample_manifest(fill: u8) -> RestoreManifestBindingEvidence {
        let identity = nokv_protocol::RestoreManifestIdentity {
            publication_operation_id: OperationIdentity([fill; FIXED_ID_BYTES]),
            artifact_revision_id: nokv_protocol::ArtifactRevisionIdentity(
                [fill.wrapping_add(1); FIXED_ID_BYTES],
            ),
        };
        RestoreManifestBindingEvidence {
            expected: identity,
            actual: RestoreManifestPublicationEvidence {
                identity,
                workspace_incarnation_id: WorkspaceIdentity([0x33; FIXED_ID_BYTES]),
                body_digest_uri: format!("sha256:{}", "44".repeat(32)),
                manifest_digest_uri: format!("sha256:{}", "55".repeat(32)),
                logical_size: 1,
                content_type: "application/json".to_owned(),
            },
        }
    }

    fn sample_evidence() -> RestoreInitializationBarrierEvidence {
        let arm = sample_arm();
        RestoreInitializationBarrierEvidence {
            route: RootRoute {
                root_id: RootIdentity([0x11; FIXED_ID_BYTES]),
                logical_shard_id: LogicalShardIdentity([0x66; FIXED_ID_BYTES]),
                object_namespace_id: nokv_protocol::ObjectNamespaceIdentity([0x77; FIXED_ID_BYTES]),
                placement_generation: 1,
                owner_epoch: 2,
            },
            operation_id: arm.operation_id,
            durable_read_version: 9,
            phase: RestoreInitializationBarrierPhase::DestinationBuilding,
            initialization_digest: Digest([0x88; 32]),
            destination_workspace_incarnation_id: WorkspaceIdentity([0x33; FIXED_ID_BYTES]),
            destination_commit_id: CommitIdentity([0x99; 32]),
            run_manifest: sample_manifest(0xa0),
            restore_manifest: sample_manifest(0xb0),
            built_commit_members: 0,
            sealed_revisions: 0,
        }
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "nokv-restore-crash-owner-{label}-{}-{}",
            std::process::id(),
            REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }

    #[test]
    fn arm_validation_is_exact_and_rejects_zero_identities() {
        let mut arm = sample_arm();
        arm.validate().unwrap();
        arm.schema = "nokv.restore-crash.arm.v2".to_owned();
        assert!(arm.validate().is_err());
        arm = sample_arm();
        arm.operation_id = OperationIdentity([0; FIXED_ID_BYTES]);
        assert!(arm.validate().is_err());
        arm = sample_arm();
        arm.run_id = "contains space".to_owned();
        assert!(arm.validate().is_err());
    }

    #[test]
    fn evidence_write_is_create_new_and_exactly_bound_to_the_arm() {
        let directory = temporary_directory("evidence");
        let path = directory.join("barrier.json");
        let arm = sample_arm();
        let evidence = sample_evidence();
        let envelope = CrashEvidenceEnvelope {
            schema: EVIDENCE_SCHEMA,
            run_id: &arm.run_id,
            root_id: arm.root_id,
            operation_id: arm.operation_id,
            evidence: &evidence,
        };
        write_create_new_synced(&path, &envelope).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(decoded["schema"], EVIDENCE_SCHEMA);
        assert_eq!(decoded["run_id"], arm.run_id);
        assert_eq!(decoded["evidence"]["durable_read_version"], 9);
        assert_eq!(decoded["evidence"]["built_commit_members"], 0);
        assert_eq!(decoded["evidence"]["sealed_revisions"], 0);
        assert!(write_create_new_synced(&path, &envelope).is_err());
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn serve_parser_requires_reopen_and_exact_root_width() {
        let arguments = vec![
            "--etcd-endpoint",
            "http://127.0.0.1:2379",
            "--root-id",
            "11111111111111111111111111111111",
            "--node-id",
            "fault-owner",
            "--advertise-endpoint",
            "127.0.0.1:5401",
            "--bind",
            "127.0.0.1:5401",
            "--metadata-reopen",
            "/tmp/nokv-meta",
            "--object-bucket",
            "fault-bucket",
            "--object-endpoint",
            "http://127.0.0.1:9000",
            "--arm-file",
            "/tmp/arm.json",
            "--evidence-file",
            "/tmp/evidence.json",
        ]
        .into_iter()
        .map(str::to_owned);
        let config = parse_serve_config(arguments).unwrap();
        assert_eq!(config.root_id.as_bytes(), &[0x11; FIXED_ID_BYTES]);
        assert_eq!(config.lease_ttl_seconds, DEFAULT_LEASE_TTL_SECONDS);

        let invalid = vec![
            "--etcd-endpoint".to_owned(),
            "http://127.0.0.1:2379".to_owned(),
            "--root-id".to_owned(),
            "11".to_owned(),
        ];
        assert!(parse_serve_config(invalid).is_err());
    }
}
