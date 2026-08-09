/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Custom CLI, MCP adapter, and shard-owner process for NoKV Agent workspaces.

mod backend;
mod cli;
mod connection;
mod object_store;
mod owner_session;
mod owner_session_journal;
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
use nokv_types::{NormalizedRelativePath, RootLayoutProfile, WorkbenchId};
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
        Command::Provision { logical_shard_id } => {
            require_qualified_root_layout(&invocation)?;
            run_provision(&invocation, logical_shard_id)
        }
        Command::Serve => {
            require_qualified_root_layout(&invocation)?;
            run_server(&invocation)
        }
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

fn require_qualified_root_layout(invocation: &Invocation) -> Result<(), String> {
    if invocation.root_layout == RootLayoutProfile::SingleShardRoot {
        Ok(())
    } else {
        Err(format!(
            "root layout {:?} is NOT QUALIFIED by this runtime",
            invocation.root_layout
        ))
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
  provision creates the logical shard, metadata authority, immutable root affinity, and activates it
  --root-id HEX32 --etcd-endpoint URL --node-id ID
  --advertise-endpoint HOST:PORT --bind HOST:PORT
  --root-layout single-shard-root is the default LingTai contract
  --root-layout partitioned-root is parsed but explicitly NOT QUALIFIED
  --metadata-profile holt-local-v1 selects the resolved metadata runtime profile
  --metadata-create PATH requires --owner-session-create JOURNAL outside PATH
  --metadata-reopen PATH requires --owner-session-resume JOURNAL for exact recovery admission
  all fresh, successor, prepared-create, exact-resume, and prepared-resume transitions are
  currently NOT QUALIFIED and fail before control, journal, locator, provider, or registry effects
  qualification requires a durable planned exact incarnation before CAS and closed unknown-outcome recovery
  a version-2 Releasing journal is release-only: restart retries its exact lease and never reopens
  the pinned Holt revision lacks actual-held directory/lock identity; Holt provision is allowed,
  but production Holt Serving remains NOT QUALIFIED until that reviewed API is pinned and wired

EXPERIMENTAL FOUNDATIONDB METADATA:
  --metadata-profile foundationdb-v1 requires the non-default foundationdb-provider feature
  --fdb-cluster-file PATH --fdb-namespace NAME [--fdb-stable-cluster-id ID]
  [--fdb-transaction-budget-bytes BYTES --fdb-transaction-timeout-ms MILLIS]
  --metadata-fdb-create or --metadata-fdb-reopen selects an exact open mode
  foundationdb-v1 currently fails before provision/Serving as NOT QUALIFIED; no fallback occurs

OBJECT DATA:
  --object-bucket NAME [--object-endpoint URL] [--object-root PREFIX]
  [--hot-cache-dir PATH --hot-cache-bytes BYTES]

NoKV exposes SDK, CLI, and MCP operations. It does not expose FUSE or POSIX."
    );
}

fn resolve_runtime_descriptor(
    server: &cli::ServerConfig,
) -> Result<nokv_server::RuntimeDescriptor, String> {
    let foundationdb_options_present = server.foundationdb.cluster_file.is_some()
        || server.foundationdb.stable_cluster_id.is_some()
        || server.foundationdb.namespace.is_some()
        || server.foundationdb.transaction_budget_bytes
            != cli::DEFAULT_FDB_TRANSACTION_BUDGET_BYTES
        || server.foundationdb.transaction_timeout_ms != cli::DEFAULT_FDB_TRANSACTION_TIMEOUT_MS;
    if server.metadata_profile.as_str() == nokv_server::HOLT_LOCAL_METADATA_PROFILE_ID
        && foundationdb_options_present
    {
        return Err(
            "FoundationDB options cannot be used with metadata profile holt-local-v1".to_owned(),
        );
    }

    let descriptors =
        vec![nokv_server::holt_runtime_descriptor().map_err(|error| error.to_string())?];
    #[cfg(feature = "foundationdb-provider")]
    let mut descriptors = descriptors;

    if server.metadata_profile.as_str() == nokv_server::FOUNDATIONDB_METADATA_PROFILE_ID {
        #[cfg(not(feature = "foundationdb-provider"))]
        return Err(
            "metadata profile foundationdb-v1 requires the non-default foundationdb-provider feature"
                .to_owned(),
        );
        #[cfg(feature = "foundationdb-provider")]
        {
            use nokv_server::{
                foundationdb_runtime_descriptor, FoundationDbRuntimeConfig,
                FoundationDbTransactionPolicy,
            };

            let cluster_file = server
                .foundationdb
                .cluster_file
                .as_ref()
                .ok_or_else(|| "foundationdb-v1 requires --fdb-cluster-file".to_owned())?;
            let namespace = server
                .foundationdb
                .namespace
                .as_deref()
                .ok_or_else(|| "foundationdb-v1 requires --fdb-namespace".to_owned())?;
            let policy = FoundationDbTransactionPolicy {
                transaction_budget_bytes: server.foundationdb.transaction_budget_bytes,
                transaction_timeout_ms: server.foundationdb.transaction_timeout_ms,
            };
            let config = match server.foundationdb.stable_cluster_id.as_deref() {
                Some(stable_id) => FoundationDbRuntimeConfig::with_explicit_stable_id(
                    cluster_file,
                    stable_id,
                    namespace,
                    policy,
                ),
                None => {
                    FoundationDbRuntimeConfig::from_cluster_file(cluster_file, namespace, policy)
                }
            }
            .map_err(|error| error.to_string())?;
            descriptors
                .push(foundationdb_runtime_descriptor(&config).map_err(|error| error.to_string())?);
        }
    }

    let registry = nokv_server::RuntimeDescriptorRegistry::new(descriptors)
        .map_err(|error| error.to_string())?;
    registry
        .descriptor(&server.metadata_profile)
        .cloned()
        .map_err(|error| error.to_string())
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
trait ExactOwnerReleaseControl {
    fn release_exact_owner(
        &self,
        lease: &nokv_control::LogicalShardLease,
    ) -> Result<nokv_control::OwnerReleaseOutcome, nokv_control::ControlError>;
}

#[cfg(feature = "etcd")]
impl<T> ExactOwnerReleaseControl for T
where
    T: nokv_control::ControlStore + ?Sized,
{
    fn release_exact_owner(
        &self,
        lease: &nokv_control::LogicalShardLease,
    ) -> Result<nokv_control::OwnerReleaseOutcome, nokv_control::ControlError> {
        self.release_owner(lease)
    }
}

#[cfg(feature = "etcd")]
fn reconcile_releasing_owner<C, F>(
    control: &C,
    lease: &nokv_control::LogicalShardLease,
    remove_exact_journal: F,
) -> Result<(), String>
where
    C: ExactOwnerReleaseControl + ?Sized,
    F: FnOnce() -> Result<(), owner_session_journal::OwnerSessionJournalError>,
{
    use nokv_control::OwnerReleaseOutcome;

    match control.release_exact_owner(lease) {
        Ok(OwnerReleaseOutcome::Released(_))
        | Ok(OwnerReleaseOutcome::AlreadyReleased(_))
        | Ok(OwnerReleaseOutcome::Superseded(_)) => remove_exact_journal()
            .map_err(|error| format!("owner release reconciled but journal cleanup failed: {error}")),
        Ok(OwnerReleaseOutcome::OutcomeUnknown) => {
            Err("owner release outcome remains unknown; Releasing journal retained".to_owned())
        }
        Err(error) => Err(format!(
            "owner release retry failed before a terminal outcome; Releasing journal retained: {error}"
        )),
    }
}

#[cfg(feature = "etcd")]
fn render_owner_bootstrap_failure(error: nokv_server::ServerError) -> String {
    use nokv_server::ServerError;

    match error {
        error @ (ServerError::BootstrapOwnerReleasePending { .. }
        | ServerError::BootstrapOwnerReleaseRetryable { .. }) => {
            format!("{error}; durable Releasing journal retained")
        }
        error @ ServerError::BootstrapOwnerReleaseReceiptRejected { .. } => format!(
            "{error}; exact release capability is process-local only and will not survive this process exiting"
        ),
        error @ (ServerError::InvalidOptions(_)
        | ServerError::InvalidRoute(_)
        | ServerError::InvalidBootstrap(_)
        | ServerError::RouteRollback(_)
        | ServerError::Control(_)
        | ServerError::Metadata(_)
        | ServerError::OwnerReleaseReceipt(_)
        | ServerError::OwnerReleasePending { .. }
        | ServerError::OwnerReleaseRetryable { .. }
        | ServerError::BootstrapRollback { .. }
        | ServerError::Protocol(_)
        | ServerError::Bind(_)
        | ServerError::Connection(_)
        | ServerError::FrameTooLarge { .. }
        | ServerError::Executor(_)) => error.to_string(),
    }
}

#[cfg(feature = "etcd")]
fn run_provision(invocation: &Invocation, logical_shard_id: &str) -> Result<(), String> {
    use nokv_control::{LogicalShardId, RootId};
    use nokv_types::RootPlacementLifecycle;

    let cli::RoutingConfig::Etcd(route) = &invocation.client.routing else {
        return Err("provision requires control-backed etcd routing".to_owned());
    };
    let metadata_runtime = resolve_provisioning_metadata_runtime(&invocation.server)?;
    let root_id = RootId::from(
        connection::configured_root_id(&invocation.client).map_err(|error| error.to_string())?,
    );
    let logical_shard_id = LogicalShardId::from(
        connection::parse_logical_shard_id(logical_shard_id).map_err(|error| error.to_string())?,
    );
    let control = connect_control(route)?;
    let outcome = provision::provision_and_activate(
        control.as_ref(),
        root_id,
        logical_shard_id,
        &metadata_runtime,
        invocation.root_layout,
    )
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
        "root_layout": "single-shard-root",
        "root_layout_generation": outcome.placement.layout_generation.get(),
        "root_partition_id": encode_lowercase_hex(outcome.placement.partition_id.as_bytes()),
        "placement_generation": outcome.placement.placement_generation.get(),
        "lifecycle": lifecycle,
        "logical_shard_preexisting": outcome.logical_shard_preexisting,
        "metadata_authority_preexisting": outcome.metadata_authority_preexisting,
        "placement_preexisting": outcome.placement_preexisting,
        "activation_required": outcome.activation_required,
    }))
}

fn resolve_provisioning_metadata_runtime(
    server: &cli::ServerConfig,
) -> Result<nokv_server::RuntimeDescriptor, String> {
    let descriptor = resolve_runtime_descriptor(server)?;
    if let nokv_server::RuntimeQualification::NotQualified(code) = descriptor.qualification() {
        return Err(format!(
            "metadata runtime is not qualified for provisioning ({code:?})"
        ));
    }
    Ok(descriptor)
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
        LifecycleRunner, LifecycleRunnerOptions, LifecycleTransition, OpenIntent, OwnerAdmission,
        RootOwnerBootstrapRequest, RootOwnerRegistry, RuntimeQualification, RuntimeRegistry,
        ServerOptions, WorkspaceServer,
    };
    use nokv_types::{OwnerIncarnationId, RequestId, RootLayoutGeneration, RootPartitionId};
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
    let metadata_config = invocation.server.metadata_store.clone().ok_or_else(|| {
        "serve requires one explicit Holt or FoundationDB metadata create/reopen mode".to_owned()
    })?;
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
    owner_session::OwnerSessionToken::validate_process_binding(&node_id, &endpoint)
        .map_err(|error| error.to_string())?;
    let root_id = RootId::from(
        connection::configured_root_id(&invocation.client).map_err(|error| error.to_string())?,
    );
    let release_only_paths = match (&invocation.server.owner_session, &metadata_config) {
        (
            Some(cli::OwnerSessionConfig::Create(journal_path)),
            cli::MetadataStoreConfig::HoltCreate(metadata_path),
        )
        | (
            Some(cli::OwnerSessionConfig::Resume(journal_path)),
            cli::MetadataStoreConfig::HoltReopen(metadata_path),
        ) => Some((journal_path, metadata_path)),
        _ => None,
    };
    let releasing = match release_only_paths {
        Some((journal_path, metadata_path)) => {
            let preparation = owner_session_journal::OwnerReleasePreparation::new(
                root_id,
                node_id.clone(),
                endpoint.clone(),
                metadata_path,
                journal_path,
            )
            .map_err(|error| error.to_string())?;
            owner_session_journal::OwnerSessionJournal::load_releasing(journal_path, &preparation)
                .map_err(|error| error.to_string())?
        }
        None => None,
    };
    let control = connect_control(route)?;
    if let Some((lease, journal)) = releasing {
        return reconcile_releasing_owner(control.as_ref(), &lease, || journal.remove_if_exact());
    }
    nokv_server::validate_owner_lease_model_before_control_read_v1(control.owner_lease_model())
        .map_err(|error| error.to_string())?;
    let placement = control
        .get_root_placement(&root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "root placement does not exist; provision it before serve".to_owned())?;
    if placement.layout_profile != invocation.root_layout {
        return Err(format!(
            "configured root layout {:?} differs from durable layout {:?}",
            invocation.root_layout, placement.layout_profile
        ));
    }
    if placement.layout_generation
        != RootLayoutGeneration::new(1).expect("one is a valid root layout generation")
        || placement.partition_id != RootPartitionId::SINGLE_SHARD
    {
        return Err(format!(
            "durable root layout fence {:?} is NOT QUALIFIED by the single-shard runtime",
            placement.layout_fence()
        ));
    }
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "logical shard does not exist; provision it before serve".to_owned())?;
    let authority = control
        .get_metadata_authority(&placement.logical_shard_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            "logical shard has no metadata authority; provision it before serve".to_owned()
        })?;
    let recovery = RecoveryPublication {
        checkpoint: shard.checkpoint.clone(),
        log: shard.log.clone(),
        durable_lsn: shard.durable_lsn,
    };

    if let Some(migration) = authority.migration.as_ref() {
        return Err(format!(
            "metadata authority migration is {:?}; owner Serving is refused before owner-session or provider side effects",
            migration.phase
        ));
    }

    let runtime_descriptor = resolve_runtime_descriptor(&invocation.server)?;
    if let RuntimeQualification::NotQualified(code) = runtime_descriptor.qualification() {
        return Err(format!(
            "metadata runtime is not qualified for Serving ({code:?})"
        ));
    }
    enum PreparedOwnerSession {
        Create(Arc<owner_session_journal::OwnerSessionJournal>),
        Resume(Arc<owner_session_journal::OwnerSessionJournal>),
    }

    let (admission, prepared_session, open_intent) = match (
        &invocation.server.owner_session,
        &metadata_config,
    ) {
        (
            Some(cli::OwnerSessionConfig::Create(journal_path)),
            cli::MetadataStoreConfig::HoltCreate(metadata_path),
        ) => {
            nokv_server::validate_owner_admission_transition_v1(
                LifecycleTransition::PreparedFirstCreate,
            )
            .map_err(|code| code.to_string())?;
            let preparation = owner_session_journal::OwnerSessionPreparation::new(
                &placement,
                &authority,
                node_id.clone(),
                endpoint.clone(),
                metadata_path,
                journal_path,
            )
            .map_err(|error| error.to_string())?;
            // Planned admission remains NOT QUALIFIED. This explicit caller
            // identity only keeps the characterized post-gate path type-exact;
            // the planned-admission slice will persist it before any control CAS.
            let owner_incarnation_id = OwnerIncarnationId::from_bytes(
                request_id(b"owner-incarnation", root_id).into_bytes(),
            );
            let (admission, transition) = match preparation
                .reconcile_control_owner(&shard, &authority)
                .map_err(|error| error.to_string())?
            {
                owner_session_journal::PreparedControlOwner::First => (
                    OwnerAdmission::Acquire {
                        owner: node_id.clone(),
                        owner_incarnation_id,
                        endpoint: endpoint.clone(),
                        expected_previous_epoch: None,
                    },
                    LifecycleTransition::PreparedFirstCreate,
                ),
                owner_session_journal::PreparedControlOwner::Successor(previous) => (
                    OwnerAdmission::Acquire {
                        owner: node_id.clone(),
                        owner_incarnation_id,
                        endpoint: endpoint.clone(),
                        expected_previous_epoch: Some(previous),
                    },
                    LifecycleTransition::PreparedSuccessorCreate,
                ),
                owner_session_journal::PreparedControlOwner::ResumeOrSuccessor(lease) => (
                    OwnerAdmission::ResumePreparedOrAcquireSuccessor {
                        lease,
                        endpoint: endpoint.clone(),
                    },
                    LifecycleTransition::PreparedResumeOrSuccessor,
                ),
            };
            nokv_server::validate_owner_admission_transition_v1(transition)
                .map_err(|code| code.to_string())?;
            runtime_descriptor
                .classify_bootstrap(OpenIntent::ReconcilePreparedCreate, transition)
                .map_err(|error| error.to_string())?;
            let (journal, _) = owner_session_journal::OwnerSessionJournal::prepare_create(
                journal_path,
                &preparation,
            )
            .map_err(|error| error.to_string())?;
            (
                admission,
                PreparedOwnerSession::Create(journal),
                OpenIntent::ReconcilePreparedCreate,
            )
        }
        (
            Some(cli::OwnerSessionConfig::Resume(journal_path)),
            cli::MetadataStoreConfig::HoltReopen(metadata_path),
        ) => {
            nokv_server::validate_owner_admission_transition_v1(LifecycleTransition::ExactResume)
                .map_err(|code| code.to_string())?;
            runtime_descriptor
                .classify_bootstrap(OpenIntent::ReopenExisting, LifecycleTransition::ExactResume)
                .map_err(|error| error.to_string())?;
            let (token, journal) = owner_session_journal::OwnerSessionJournal::load_resume(
                journal_path,
                metadata_path,
            )
            .map_err(|error| error.to_string())?;
            token
                .validate_resume(owner_session::OwnerSessionResumeBinding {
                    root_id,
                    layout_profile: invocation.root_layout,
                    owner: &node_id,
                    endpoint: &endpoint,
                    metadata_profile: invocation.server.metadata_profile.clone(),
                    placement: &placement,
                    shard: &shard,
                    authority: &authority,
                })
                .map_err(|error| error.to_string())?;
            (
                OwnerAdmission::Resume {
                    lease: token.lease().clone(),
                },
                PreparedOwnerSession::Resume(journal),
                OpenIntent::ReopenExisting,
            )
        }
        _ => {
            return Err(
                "Holt Serving requires --owner-session-create with --metadata-create or --owner-session-resume with --metadata-reopen; FoundationDB Serving is NOT QUALIFIED"
                    .to_owned(),
            );
        }
    };
    let journal = match &prepared_session {
        PreparedOwnerSession::Create(journal) | PreparedOwnerSession::Resume(journal) => {
            Arc::clone(journal)
        }
    };
    let metadata_path = match &metadata_config {
        cli::MetadataStoreConfig::HoltCreate(path) | cli::MetadataStoreConfig::HoltReopen(path) => {
            path
        }
        cli::MetadataStoreConfig::FoundationDbCreate
        | cli::MetadataStoreConfig::FoundationDbReopen => {
            return Err("FoundationDB Serving is not qualified".to_owned());
        }
    };
    let runtime_factory = nokv_server::holt_file_runtime_factory(metadata_path, journal.clone())
        .map_err(|error| error.to_string())?;
    let runtime_registry =
        RuntimeRegistry::new(vec![runtime_factory]).map_err(|error| error.to_string())?;
    let runtime = runtime_registry
        .resolve(&invocation.server.metadata_profile)
        .map_err(|error| error.to_string())?;
    if runtime.descriptor() != &runtime_descriptor {
        return Err("metadata runtime descriptor changed during stock composition".to_owned());
    }
    let registry = Arc::new(RootOwnerRegistry::new());
    let owner = match bootstrap_root_owner(
        Arc::clone(&control),
        Arc::clone(&registry),
        RootOwnerBootstrapRequest {
            root_id,
            runtime,
            open_intent,
            admission,
            install_request_id: request_id(b"install", root_id),
            activate_request_id: request_id(b"activate", root_id),
            recovery,
        },
    ) {
        Ok(owner) => owner,
        Err(error) => return Err(render_owner_bootstrap_failure(error)),
    };

    let session_file = match prepared_session {
        PreparedOwnerSession::Resume(journal) => Some(journal),
        PreparedOwnerSession::Create(journal) => {
            let current_placement = control
                .get_root_placement(&root_id)
                .map_err(|error| error.to_string());
            let current_authority = control
                .get_metadata_authority(&owner.lease.logical_shard_id)
                .map_err(|error| error.to_string());
            let token = match (current_placement, current_authority) {
                (Ok(Some(current_placement)), Ok(Some(current_authority))) => {
                    owner_session::OwnerSessionToken::from_serving(
                        &current_placement,
                        &owner.serving_record,
                        &owner.lease,
                        &endpoint,
                        &current_authority,
                    )
                    .map_err(|error| error.to_string())
                }
                (Ok(None), _) => {
                    Err("root placement disappeared before owner-session persistence".to_owned())
                }
                (_, Ok(None)) => Err(
                    "metadata authority disappeared before owner-session persistence".to_owned(),
                ),
                (Err(error), _) | (_, Err(error)) => Err(error),
            };
            let persisted = token.and_then(|token| {
                journal
                    .complete_serving(&token)
                    .map_err(|error| error.to_string())
            });
            match persisted {
                Ok(()) => Some(journal),
                Err(primary) => {
                    return release_owner_and_session(
                        &owner.ownership,
                        Some(&journal),
                        Some(primary),
                    );
                }
            }
        }
    };

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
        let lifecycle = owner
            .lifecycle_runner(
                owner_loss,
                lifecycle_objects,
                LifecycleRunnerOptions {
                    poll_interval: Duration::from_millis(
                        invocation.server.lifecycle_interval_millis,
                    ),
                    ..LifecycleRunnerOptions::default()
                },
            )
            .map_err(|error| error.to_string())?;
        Ok((server, lifecycle))
    })();
    let (server, lifecycle) = match runtime {
        Ok(runtime) => runtime,
        Err(primary) => {
            return release_owner_and_session(
                &owner.ownership,
                session_file.as_ref(),
                Some(primary),
            );
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
            return release_owner_and_session(
                &owner.ownership,
                session_file.as_ref(),
                Some(primary),
            );
        }
    };

    let server_result = server.run();
    owner_loss.fail_closed();
    let lifecycle_result = lifecycle_worker.join();
    let release_result = release_owner_and_session(&owner.ownership, session_file.as_ref(), None);

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
        failures.push(error);
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

#[cfg(feature = "etcd")]
fn release_owner_and_session(
    ownership: &nokv_server::ControlBackedRootOwner,
    session_file: Option<&Arc<owner_session_journal::OwnerSessionJournal>>,
    primary: Option<String>,
) -> Result<(), String> {
    let release = ownership.release();
    let mut failures = Vec::new();
    if let Some(primary) = primary {
        failures.push(primary);
    }
    match release {
        Ok(_) => {
            if let Some(session_file) = session_file {
                if let Err(error) = session_file.remove_if_exact() {
                    failures.push(format!("owner-session cleanup failed: {error}"));
                }
            }
        }
        Err(error) => {
            // A failed or ambiguous exact release preserves the durable
            // Releasing token. Restart may reconcile only this release.
            failures.push(format!(
                "owner release failed; Releasing owner-session retained: {error}"
            ));
        }
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
    #[cfg(feature = "etcd")]
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[cfg(feature = "etcd")]
    #[derive(Clone, Copy, Debug)]
    enum CurrentControlDrift {
        MissingPlacement,
        RetiredPlacement,
        ChangedAuthority,
        MigratingAuthority,
    }

    #[cfg(feature = "etcd")]
    struct ReleaseOnlyControl {
        scenario: CurrentControlDrift,
        expected: nokv_control::LogicalShardLease,
        release_calls: AtomicUsize,
    }

    #[cfg(feature = "etcd")]
    impl ExactOwnerReleaseControl for ReleaseOnlyControl {
        fn release_exact_owner(
            &self,
            lease: &nokv_control::LogicalShardLease,
        ) -> Result<nokv_control::OwnerReleaseOutcome, nokv_control::ControlError> {
            assert_eq!(lease, &self.expected, "scenario: {:?}", self.scenario);
            self.release_calls.fetch_add(1, Ordering::SeqCst);
            let mut released = nokv_control::LogicalShardRecord::unassigned(lease.logical_shard_id);
            released.owner_epoch = Some(lease.owner_epoch);
            released.owner_incarnation_id = Some(lease.owner_incarnation_id);
            Ok(nokv_control::OwnerReleaseOutcome::AlreadyReleased(released))
        }
    }

    #[cfg(feature = "etcd")]
    fn release_only_lease() -> nokv_control::LogicalShardLease {
        nokv_control::LogicalShardLease {
            logical_shard_id: nokv_types::LogicalShardId::from_bytes([0x41; 16]),
            owner: nokv_control::NodeId::new("release-only-owner").unwrap(),
            owner_epoch: nokv_control::OwnerEpoch::new(7).unwrap(),
            owner_incarnation_id: nokv_control::OwnerIncarnationId::from_bytes([0x43; 16]),
            lease_id: 19,
            authority: nokv_control::MetadataAuthorityFence {
                logical_shard_id: nokv_types::LogicalShardId::from_bytes([0x41; 16]),
                authority_id: nokv_control::MetadataAuthorityId::from_bytes([0x42; 16]),
                authority_generation: nokv_control::MetadataAuthorityGeneration::new(3).unwrap(),
            },
        }
    }

    #[test]
    fn scoped_artifact_accepts_only_the_five_virtual_sections() {
        let path = scoped_artifact("run-1", "outputs", "result.json").unwrap();
        assert_eq!(path.logical_path(), "outputs/result.json");
        assert!(scoped_artifact("run-1", "tmp", "result.json").is_err());
        assert!(scoped_artifact("run-1", "outputs", "../result.json").is_err());
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn releasing_restart_ignores_current_placement_authority_and_migration_drift() {
        for scenario in [
            CurrentControlDrift::MissingPlacement,
            CurrentControlDrift::RetiredPlacement,
            CurrentControlDrift::ChangedAuthority,
            CurrentControlDrift::MigratingAuthority,
        ] {
            let lease = release_only_lease();
            let control = ReleaseOnlyControl {
                scenario,
                expected: lease.clone(),
                release_calls: AtomicUsize::new(0),
            };
            let cleanup_calls = AtomicUsize::new(0);

            reconcile_releasing_owner(&control, &lease, || {
                cleanup_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();

            assert_eq!(control.release_calls.load(Ordering::SeqCst), 1);
            assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
        }
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn bootstrap_failure_rendering_is_an_exhaustive_closed_match() {
        let code = nokv_server::AdmissionCode::PlannedOwnerAdmissionNotQualifiedV1.to_string();
        let error = nokv_server::ServerError::InvalidBootstrap(code.clone());
        assert_eq!(
            render_owner_bootstrap_failure(error),
            format!("invalid root-owner bootstrap: {code}")
        );
    }

    #[test]
    fn serve_descriptor_preflight_resolves_default_holt_without_a_runtime_factory() {
        let server = cli::ServerConfig::default();
        let descriptor = resolve_runtime_descriptor(&server).unwrap();
        assert_eq!(
            descriptor.profile_id().as_str(),
            nokv_server::HOLT_LOCAL_METADATA_PROFILE_ID
        );
        assert_eq!(
            descriptor.qualification(),
            nokv_server::RuntimeQualification::Qualified
        );
    }

    #[test]
    fn descriptor_preflight_preserves_unknown_profile_errors() {
        let server = cli::ServerConfig {
            metadata_profile: nokv_control::MetadataProviderProfileId::new("external-v1").unwrap(),
            ..cli::ServerConfig::default()
        };
        assert_eq!(
            resolve_runtime_descriptor(&server).unwrap_err(),
            "unknown metadata runtime profile external-v1"
        );
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn foundationdb_descriptor_preflight_never_requires_runtime_resolution() {
        let temporary = tempfile::tempdir().unwrap();
        let cluster_file = temporary.path().join("fdb.cluster");
        std::fs::write(&cluster_file, "nokv:0123456789abcdef@127.0.0.1:4500\n").unwrap();
        let mut server = cli::ServerConfig {
            metadata_profile: nokv_control::MetadataProviderProfileId::new(
                nokv_server::FOUNDATIONDB_METADATA_PROFILE_ID,
            )
            .unwrap(),
            ..cli::ServerConfig::default()
        };
        server.foundationdb.cluster_file = Some(cluster_file);
        server.foundationdb.namespace = Some("openviking/metadata".to_owned());

        let descriptor = resolve_runtime_descriptor(&server).unwrap();
        assert_eq!(
            descriptor.profile_id().as_str(),
            nokv_server::FOUNDATIONDB_METADATA_PROFILE_ID
        );
        assert!(matches!(
            descriptor.qualification(),
            nokv_server::RuntimeQualification::NotQualified(_)
        ));
    }

    #[test]
    fn provision_resolves_the_provisioning_qualification_gate() {
        let server = cli::ServerConfig::default();
        let runtime = resolve_provisioning_metadata_runtime(&server).unwrap();
        assert_eq!(
            runtime.profile_id().as_str(),
            nokv_server::HOLT_LOCAL_METADATA_PROFILE_ID
        );
    }
}
