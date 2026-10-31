/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Custom CLI and shard-owner process for NoKV Agent workspaces.

mod backend;
mod build_info;
mod cli;
mod connection;
mod failure_envelope;
mod identity_admin;
mod object_store;
mod provision;
mod rpc_tls;
mod transfer;
mod workbench_session;

use std::io;
use std::path::Path;
use std::process::ExitCode;
#[cfg(feature = "control")]
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use nokv_agent::{
    execute_tool, tool_definitions, workbench_tool_idempotency, AgentError, ReadRequest, ReadView,
    ScopedPath, SdkWorkbenchToolHandler, Section, WorkbenchBackend, WorkbenchToolHandler,
    WORKBENCH_CONTRACT_SCHEMA,
};
use nokv_types::{ErrorClass, IdempotencyScope, NextAction, NormalizedRelativePath, WorkbenchId};
use serde_json::{json, Value};

use backend::CliWorkbenchBackend;
use cli::{AppendPayload, Command, Invocation, WorkspacePathCommand};
use failure_envelope::CliFailure;
#[cfg(feature = "control")]
use object_store::CliObjectStore;

type CliHandler = SdkWorkbenchToolHandler<CliWorkbenchBackend>;

#[cfg(feature = "control")]
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
        Err(failure) => {
            eprintln!("{}", failure.to_line());
            ExitCode::from(failure.exit_code())
        }
    }
}

fn run() -> Result<(), CliFailure> {
    let invocation = cli::parse(std::env::args().skip(1)).map_err(|error| match error {
        cli::CliError::OperationInput {
            operation_id,
            message,
        } => with_append_details(
            append_input_error(message),
            nokv_protocol::OperationIdentity(operation_id),
        ),
        error => CliFailure::invalid_input(error.to_string()),
    })?;
    match &invocation.command {
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::Schema => Ok(print_schema()?),
        Command::Version { json } => Ok(print_version(*json)?),
        Command::Provision {
            logical_shard_id,
            workbench_root,
            adopt_legacy_object_namespace,
            adopt_legacy_agent_binding,
        } => Ok(run_provision(
            &invocation,
            logical_shard_id,
            workbench_root,
            *adopt_legacy_object_namespace,
            *adopt_legacy_agent_binding,
        )?),
        Command::Serve => Ok(run_server(&invocation)?),
        Command::Identity(command) => {
            let store = connection::open_identity(&invocation.client.routing)
                .map_err(connection_failure)?;
            let now_unix_seconds = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let result = identity_admin::run(store.as_ref(), command, now_unix_seconds)?;
            Ok(print_json(&result)?)
        }
        Command::Workbench { tool, arguments } => {
            // Malformed arguments fail before any connection.
            let arguments: Value = serde_json::from_str(arguments).map_err(|error| {
                CliFailure::invalid_input(format!(
                    "Workbench arguments are not valid JSON: {error}"
                ))
            })?;
            let handler = build_handler(&invocation)?;
            let result = execute_tool(&handler, tool, &arguments).map_err(|error| {
                CliFailure::from_agent(&error, workbench_tool_idempotency(tool))
            })?;
            Ok(print_json(&result)?)
        }
        Command::WorkbenchSession => {
            let admitted = admit_root(&invocation)?;
            let handler = handler_for(
                &invocation,
                admitted.backend.clone(),
                &admitted.workbench_root,
            )?;
            let backend = admitted.backend;
            let input = io::stdin();
            let output = io::stdout();
            workbench_session::serve(
                &handler,
                &SessionMaterializer { backend },
                input.lock(),
                output.lock(),
            )
            .map_err(|error| {
                // A session can lose its output after a mutation applied.
                CliFailure::new(
                    "SessionTransport",
                    format!("Workbench session transport failed: {error}"),
                    ErrorClass::OutcomeUnknown,
                    IdempotencyScope::Unidentified,
                )
            })
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
            replace: _,
            expected_generation,
            content_type,
            operation_id: Some(operation_id),
            expected_workspace_incarnation_id,
        } => run_identified_put(
            &invocation,
            IdentifiedPut {
                operation: "collect",
                workbench,
                section,
                path,
                operation_id: nokv_protocol::OperationIdentity(*operation_id),
                expected_generation: *expected_generation,
                expected_workspace_incarnation_id: *expected_workspace_incarnation_id,
                payload: &AppendPayload::File(source.clone()),
                content_type: content_type.as_deref(),
                block_size: None,
            },
        ),
        Command::Collect {
            workbench,
            section,
            source,
            path,
            replace,
            expected_generation,
            content_type,
            operation_id: None,
            expected_workspace_incarnation_id: _,
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
        Command::OperationStatus { operation_id } => {
            run_operation_status(&invocation, nokv_protocol::OperationIdentity(*operation_id))
        }
        Command::CommitStatus { commit_identity } => {
            let root_id = connection::configured_root_id(&invocation.client)
                .map_err(|error| CliFailure::invalid_input(error.to_string()))?;
            let operation_id = nokv_client::CommitWorkflowIdentities::derive(
                root_id,
                nokv_protocol::CommitIdentity(*commit_identity),
            )
            .operation_id;
            run_operation_status(&invocation, operation_id)
        }
        Command::OperationInspect {
            operation_id,
            limit,
            cursor,
        } => run_operation_inspect(
            &invocation,
            nokv_protocol::OperationIdentity(*operation_id),
            *limit,
            cursor.clone(),
        ),
        Command::OperationRecover {
            operation_id,
            expected_state_digest,
        } => run_operation_recover(
            &invocation,
            nokv_protocol::OperationIdentity(*operation_id),
            *expected_state_digest,
        ),
    }
}

#[derive(Debug)]
struct ConfiguredAgentAdmission {
    root_id: nokv_control::RootId,
    agent_id: nokv_types::AgentId,
}

/// The root and `AgentId` a plaintext client must match before it connects.
/// Over RPC TLS there is none: the owner authorizes the principal itself.
fn configured_agent_admission(
    invocation: &Invocation,
) -> Result<Option<ConfiguredAgentAdmission>, CliFailure> {
    if matches!(invocation.client.routing, cli::RoutingConfig::Static(_)) {
        return Err(CliFailure::invalid_input(
            "Agent-facing commands require durable control routing (--etcd-endpoint or --control-dir); static routes cannot prove the RootId-to-AgentId binding",
        ));
    }
    if invocation.client.tls.is_some() {
        return Ok(None);
    }
    let root_id = nokv_control::RootId::from(
        connection::configured_root_id(&invocation.client)
            .map_err(|error| CliFailure::invalid_input(error.to_string()))?,
    );
    let agent_id = connection::parse_agent_id(invocation.agent_id.as_deref().ok_or_else(|| {
        CliFailure::invalid_input("--agent-id is required for Agent-facing commands")
    })?)
    .map_err(|error| CliFailure::invalid_input(error.to_string()))?;
    Ok(Some(ConfiguredAgentAdmission { root_id, agent_id }))
}

/// A control-plane failure before any workspace request was sent.
fn control_failure(code: &str, message: impl Into<String>, class: ErrorClass) -> CliFailure {
    CliFailure::new(code, message, class, IdempotencyScope::ReadOnly)
}

fn load_root_agent_binding(
    control: &dyn nokv_control::ControlStore,
    root_id: nokv_control::RootId,
) -> Result<nokv_control::RootAgentBinding, CliFailure> {
    let binding = control
        .get_root_agent_binding(&root_id)
        .map_err(|error| control_failure("ControlFailure", error.to_string(), error.class()))?
        .ok_or_else(|| {
            control_failure(
                "RootNotProvisioned",
                format!(
                    "root {root_id:?} has no durable Agent binding in this control store; check --control-dir or --etcd-endpoint, or run provision with its stable --agent-id, adding --adopt-legacy-agent-binding only for a verified legacy root"
                ),
                ErrorClass::OperatorRequired,
            )
        })?;
    if binding.root_id != root_id {
        return Err(control_failure(
            "ControlFailure",
            format!("root {root_id:?} Agent binding key/value identity mismatch"),
            ErrorClass::OperatorRequired,
        ));
    }
    Ok(binding)
}

fn after_agent_root_admission<T>(
    control: &dyn nokv_control::ControlStore,
    root_id: nokv_control::RootId,
    agent_id: nokv_types::AgentId,
    operation: impl FnOnce() -> Result<T, CliFailure>,
) -> Result<T, CliFailure> {
    let binding = load_root_agent_binding(control, root_id)?;
    if binding.agent_id != agent_id {
        // The configured --agent-id names another deployment.
        return Err(control_failure(
            "AgentBindingMismatch",
            nokv_control::ControlError::RootAgentAlreadyBound { root_id }.to_string(),
            ErrorClass::InvalidInput,
        ));
    }
    operation()
}

/// A client failure while connecting or proving capabilities, before the
/// command's own request.
fn connection_failure(error: connection::ConnectionError) -> CliFailure {
    control_failure("ConnectionFailed", error.to_string(), error.class())
}

fn preflight_failure(error: nokv_client::ClientError) -> CliFailure {
    let agent = AgentError::from(backend::map_client_error(error));
    let mut failure = CliFailure::from_agent(&agent, IdempotencyScope::ReadOnly);
    failure.details_mut()["stage"] = json!("preflight");
    failure
}

fn object_failure(error: nokv_object::ObjectError) -> CliFailure {
    control_failure("ObjectProviderFailed", error.to_string(), error.class())
}

/// The object provider admission `provision` and `serve` run before any
/// object write, repeated within the command's retry budget while it fails
/// before it could take effect (an unreachable or inconclusive provider).
/// A provider that lacks a required capability is `operator_required`.
fn admit_object_provider(
    objects: &CliObjectStore,
    invocation: &Invocation,
) -> Result<(), CliFailure> {
    objects
        .admit_within(
            connection::client_options(&invocation.client),
            std::thread::sleep,
        )
        .map_err(|(error, attempts)| {
            let mut failure = control_failure(
                "ProviderAdmissionFailed",
                format!(
                    "{error} after {attempts} attempt{}",
                    if attempts == 1 { "" } else { "s" }
                ),
                error.class(),
            );
            failure.details_mut()["attempts"] = json!(attempts);
            failure
        })
}

#[cfg(feature = "control")]
fn provision_failure(error: provision::ProvisionError) -> CliFailure {
    control_failure(error.code(), error.to_string(), error.class())
}

/// An Agent-facing command's admitted connection to its root, and the
/// Workbench root the control plane binds that root to.
struct AdmittedRoot {
    backend: CliWorkbenchBackend,
    workbench_root: nokv_types::WorkbenchRoot,
}

/// Admit the configured root before any RPC: its Workbench root must be
/// bound, and without RPC TLS its AgentId binding must match.
fn admit_root(invocation: &Invocation) -> Result<AdmittedRoot, CliFailure> {
    let admission = configured_agent_admission(invocation)?;
    #[cfg(feature = "control")]
    {
        let plane = open_control_plane(invocation)?;
        let root_id = nokv_control::RootId::from(
            connection::configured_root_id(&invocation.client)
                .map_err(|error| CliFailure::invalid_input(error.to_string()))?,
        );
        let workbench_root = bound_workbench_root(plane.identity.as_ref(), root_id)?;
        let connect = || {
            let client = connection::connect(&invocation.client, Some(plane.control.clone()))
                .map_err(connection_failure)?;
            build_agent_backend(invocation, client)
        };
        let backend = match admission {
            Some(admission) => after_agent_root_admission(
                plane.control.as_ref(),
                admission.root_id,
                admission.agent_id,
                connect,
            ),
            None => connect(),
        }?;
        Ok(AdmittedRoot {
            backend,
            workbench_root,
        })
    }
    #[cfg(not(feature = "control"))]
    {
        let _ = admission;
        Err(CliFailure::invalid_input(
            "Agent-facing commands require the nokv control feature",
        ))
    }
}

fn build_backend(invocation: &Invocation) -> Result<CliWorkbenchBackend, CliFailure> {
    Ok(admit_root(invocation)?.backend)
}

/// The Workbench root provision bound to `root_id`.
#[cfg(feature = "control")]
fn bound_workbench_root(
    identity: &dyn nokv_control::IdentityStore,
    root_id: nokv_control::RootId,
) -> Result<nokv_types::WorkbenchRoot, CliFailure> {
    identity
        .get_root_presentation(&root_id)
        .map_err(|error| control_failure("ControlFailure", error.to_string(), error.class()))?
        .map(|binding| binding.workbench_root)
        .ok_or_else(|| {
            control_failure(
                "RootNotProvisioned",
                format!(
                    "root {} has no Workbench root in this control store; run provision with --workbench-root",
                    encode_lowercase_hex(root_id.as_bytes())
                ),
                ErrorClass::OperatorRequired,
            )
        })
}

/// The Agent-facing backend: a preflighted client over an object store bound
/// to the root's namespace. The Agent only reads objects; every write goes
/// through the owner, so no write admission runs here.
fn build_agent_backend(
    invocation: &Invocation,
    client: connection::CliWorkspaceClient,
) -> Result<CliWorkbenchBackend, CliFailure> {
    #[cfg(feature = "control")]
    {
        let preflight = client
            .preflight(WORKBENCH_REQUIRED_RPC_CAPABILITIES)
            .map_err(preflight_failure)?;
        let expected_namespace = preflight.value.route.object_namespace_id.into();
        let objects = Arc::new(
            CliObjectStore::build(&invocation.client.object)
                .map_err(CliFailure::invalid_input)?
                .bind(expected_namespace)
                .map_err(object_failure)?,
        );
        Ok(CliWorkbenchBackend::new(
            client,
            objects,
            invocation.client.max_artifact_bytes,
        ))
    }
    #[cfg(not(feature = "control"))]
    {
        let _ = (invocation, client);
        Err(CliFailure::invalid_input(
            "Agent-facing commands require the nokv control feature",
        ))
    }
}

fn build_handler(invocation: &Invocation) -> Result<CliHandler, CliFailure> {
    let admitted = admit_root(invocation)?;
    handler_for(invocation, admitted.backend, &admitted.workbench_root)
}

fn handler_for(
    invocation: &Invocation,
    backend: CliWorkbenchBackend,
    workbench_root: &nokv_types::WorkbenchRoot,
) -> Result<CliHandler, CliFailure> {
    SdkWorkbenchToolHandler::with_limits(
        backend,
        invocation.client.max_artifact_bytes,
        usize::try_from(invocation.client.max_attempts)
            .map_err(|_| CliFailure::invalid_input("--max-attempts does not fit this platform"))?,
        workbench_root.as_str(),
    )
    .map_err(|error| CliFailure::from_agent(&error, IdempotencyScope::ReadOnly))
}

fn build_workspace_path_client(
    invocation: &Invocation,
) -> Result<connection::CliWorkspaceClient, CliFailure> {
    let admission = configured_agent_admission(invocation)?;
    #[cfg(feature = "control")]
    {
        let control = open_control(invocation)?;
        let connect = || {
            let client = connection::connect(&invocation.client, Some(control.clone()))
                .map_err(connection_failure)?;
            client
                .preflight([nokv_protocol::WorkspaceCapability::WorkspacePathV1])
                .map_err(preflight_failure)?;
            Ok(client)
        };
        match admission {
            Some(admission) => after_agent_root_admission(
                control.as_ref(),
                admission.root_id,
                admission.agent_id,
                connect,
            ),
            None => connect(),
        }
    }
    #[cfg(not(feature = "control"))]
    {
        let _ = admission;
        Err(CliFailure::invalid_input(
            "workspace-path commands require the nokv control feature",
        ))
    }
}

fn run_workspace_path(
    invocation: &Invocation,
    command: &WorkspacePathCommand,
) -> Result<(), CliFailure> {
    if let WorkspacePathCommand::Append {
        workbench,
        section,
        path,
        operation_id,
        expected_workspace_incarnation_id,
        payload,
        content_type,
        max_logical_size,
        block_size,
    } = command
    {
        let operation_id = nokv_protocol::OperationIdentity(*operation_id);
        let result = (|| {
            let target =
                canonical_workspace_path(workbench, section, path).map_err(append_input_error)?;
            let (delta, default_content_type) = append_payload(
                payload,
                invocation
                    .client
                    .max_artifact_bytes
                    .min(nokv_client::MAX_APPEND_DELTA_BYTES),
            )
            .map_err(append_input_error)?;
            let content_type = content_type
                .as_ref()
                .map(|value| nokv_protocol::ContentType::new(value.clone()))
                .transpose()
                .map_err(|error| append_input_error(error.to_string()))?;
            let create_content_type = content_type.clone().unwrap_or(
                nokv_protocol::ContentType::new(default_content_type)
                    .map_err(|error| append_input_error(error.to_string()))?,
            );
            let client = build_workspace_path_client(invocation)?;
            let incarnation = client
                .resolve_append_workspace_incarnation(
                    operation_id,
                    &target,
                    expected_workspace_incarnation_id.map(nokv_protocol::WorkspaceIdentity),
                )
                .map_err(append_client_failure)?;
            let mut options = nokv_client::IdempotentAppendOptions::new(
                operation_id,
                target,
                incarnation,
                create_content_type,
            )
            .with_block_size(*block_size);
            if let Some(content_type) = content_type {
                options = options.with_content_type(content_type);
            }
            if let Some(max_logical_size) = max_logical_size {
                options = options.with_max_logical_size(*max_logical_size);
            }
            if let Some(call) = client
                .recover_append_receipt(&mut options, &delta)
                .map_err(append_client_failure)?
            {
                return Ok(print_json(&append_receipt_json(&call))?);
            }
            let backend = build_agent_backend(invocation, client)?;
            let call = backend
                .append_idempotent(options, &delta)
                .map_err(|error| {
                    CliFailure::from_agent(&error.into(), IdempotencyScope::CallerOperation)
                })?;
            Ok(print_json(&append_receipt_json(&call))?)
        })();
        return result.map_err(|failure| with_append_details(failure, operation_id));
    }
    if let WorkspacePathCommand::Put {
        workbench,
        section,
        path,
        operation_id,
        expected_generation,
        expected_workspace_incarnation_id,
        payload,
        content_type,
        block_size,
    } = command
    {
        return run_identified_put(
            invocation,
            IdentifiedPut {
                operation: "put",
                workbench,
                section,
                path,
                operation_id: nokv_protocol::OperationIdentity(*operation_id),
                expected_generation: *expected_generation,
                expected_workspace_incarnation_id: *expected_workspace_incarnation_id,
                payload,
                content_type: content_type.as_deref(),
                block_size: *block_size,
            },
        );
    }
    if let WorkspacePathCommand::Stat {
        workbench,
        section,
        path,
    } = command
    {
        return run_path_stat(invocation, workbench, section, path);
    }
    let (request_id, operation) = match command {
        WorkspacePathCommand::Append { .. }
        | WorkspacePathCommand::Put { .. }
        | WorkspacePathCommand::Stat { .. } => {
            unreachable!("append, put and stat dispatched above")
        }
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
                source: canonical_workspace_path(workbench, section, source)
                    .map_err(CliFailure::invalid_input)?,
                destination: canonical_workspace_path(workbench, section, destination)
                    .map_err(CliFailure::invalid_input)?,
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
                target: canonical_workspace_path(workbench, section, path)
                    .map_err(CliFailure::invalid_input)?,
                expected_generation: *expected_generation,
            }),
        ),
    };
    let client = build_workspace_path_client(invocation)?;
    match operation {
        nokv_protocol::WorkspaceRequest::RenamePath(request) => {
            let call = client
                .rename_path(request_id, request.clone())
                .map_err(|error| request_client_failure(error, request_id))?;
            Ok(print_json(&rename_path_json(request_id, &request, &call)?)?)
        }
        nokv_protocol::WorkspaceRequest::RemovePath(request) => {
            let call = client
                .remove_path(request_id, request.clone())
                .map_err(|error| request_client_failure(error, request_id))?;
            Ok(print_json(&remove_path_json(request_id, &request, &call)?)?)
        }
        _ => unreachable!("workspace-path CLI constructs only path mutations"),
    }
}

/// Report a path's version tokens: the workspace incarnation, which is never
/// reused, the path generation, and the immutable artifact revision, which
/// tells a recreated path from its predecessor even at the same generation.
fn run_path_stat(
    invocation: &Invocation,
    workbench: &str,
    section: &str,
    path: &str,
) -> Result<(), CliFailure> {
    let target =
        canonical_workspace_path(workbench, section, path).map_err(CliFailure::invalid_input)?;
    let client = build_workspace_path_client(invocation)?;
    let read_failure = |error| {
        CliFailure::from_agent(
            &AgentError::from(backend::map_client_error(error)),
            IdempotencyScope::ReadOnly,
        )
    };
    let metadata = match client.get_path(nokv_protocol::GetPathRequest {
        target: target.clone(),
        view: nokv_protocol::WorkspaceReadView::Live,
        expected_read_version: None,
        range: None,
        plan_page: None,
        if_none_match: None,
    }) {
        Ok(call) => call.value.metadata,
        Err(error)
            if error
                .rpc_failure()
                .is_some_and(|failure| failure.code == nokv_protocol::ErrorCode::NotFound) =>
        {
            None
        }
        Err(error) => return Err(read_failure(error)),
    };
    let result = match metadata {
        Some(metadata) => json!({
            "status": "success",
            "operation": "stat",
            "workbench_id": target.workbench.as_str(),
            "path": target.path.as_str(),
            "exists": true,
            "workspace_incarnation_id": encode_lowercase_hex(&metadata.workspace_incarnation_id.0),
            "workspace_revision": metadata.workspace_revision,
            "generation": metadata.generation,
            "artifact_revision_id": encode_lowercase_hex(&metadata.artifact_revision_id.0),
            "logical_size": metadata.descriptor.logical_size,
            "body_digest": metadata.descriptor.body_digest.as_str(),
            "content_type": metadata.descriptor.content_type.as_str(),
        }),
        None => {
            // The path is absent; its workspace still names the incarnation
            // a create-only publication would fence on.
            let workspace = client
                .get_workspace(nokv_protocol::GetWorkspaceRequest {
                    workbench: target.workbench.clone(),
                })
                .map_err(read_failure)?
                .value;
            json!({
                "status": "success",
                "operation": "stat",
                "workbench_id": target.workbench.as_str(),
                "path": target.path.as_str(),
                "exists": false,
                "workspace_incarnation_id": encode_lowercase_hex(&workspace.workspace_incarnation_id.0),
                "workspace_revision": workspace.workspace_revision,
                "generation": Value::Null,
                "artifact_revision_id": Value::Null,
                "logical_size": Value::Null,
                "body_digest": Value::Null,
                "content_type": Value::Null,
            })
        }
    };
    Ok(print_json(&result)?)
}

/// One whole-body publication under a caller-chosen operation identity.
struct IdentifiedPut<'a> {
    operation: &'static str,
    workbench: &'a str,
    section: &'a str,
    path: &'a str,
    operation_id: nokv_protocol::OperationIdentity,
    expected_generation: Option<u64>,
    expected_workspace_incarnation_id: Option<[u8; 16]>,
    payload: &'a AppendPayload,
    content_type: Option<&'a str>,
    block_size: Option<usize>,
}

/// Publish once under the caller's operation id: a retry with the same id
/// and inputs returns the original receipt, from any process.
fn run_identified_put(invocation: &Invocation, put: IdentifiedPut<'_>) -> Result<(), CliFailure> {
    let operation_id = put.operation_id;
    let result = (|| {
        let target = canonical_workspace_path(put.workbench, put.section, put.path)
            .map_err(append_input_error)?;
        let (body, default_content_type) =
            append_payload(put.payload, invocation.client.max_artifact_bytes)
                .map_err(append_input_error)?;
        let content_type =
            nokv_protocol::ContentType::new(put.content_type.unwrap_or(default_content_type))
                .map_err(|error| append_input_error(error.to_string()))?;
        let condition = match put.expected_generation {
            None => nokv_protocol::PublishCondition::CreateOnly,
            Some(expected_generation) => nokv_protocol::PublishCondition::ReplaceOnly {
                expected_generation,
            },
        };
        let mut options = nokv_client::IdempotentPublishOptions::new(
            operation_id,
            target,
            condition,
            content_type,
        );
        if let Some(incarnation) = put.expected_workspace_incarnation_id {
            options = options
                .with_expected_workspace_incarnation(nokv_protocol::WorkspaceIdentity(incarnation));
        }
        if let Some(block_size) = put.block_size {
            options = options.with_block_size(block_size);
        }
        // A replay or a refused identity needs only metadata; the object
        // provider is admitted only when an attempt writes objects.
        let client = build_workspace_path_client(invocation)?;
        if let Some(receipt) = client
            .replay_idempotent_publish(&options, &body)
            .map_err(identified_publish_failure)?
        {
            return Ok(print_json(&identified_put_json(
                put.operation,
                operation_id,
                &receipt,
            ))?);
        }
        let backend = build_agent_backend(invocation, client)?;
        let receipt = backend
            .publish_idempotent(&options, &body)
            .map_err(identified_publish_failure)?;
        Ok(print_json(&identified_put_json(
            put.operation,
            operation_id,
            &receipt,
        ))?)
    })();
    result.map_err(|failure| with_append_details(failure, operation_id))
}

fn identified_publish_failure(error: nokv_client::IdempotentPublishError) -> CliFailure {
    let attempt = error.attempt();
    let mut failure = match error {
        nokv_client::IdempotentPublishError::Client { source, .. } => CliFailure::from_agent(
            &backend::map_client_error(*source).into(),
            IdempotencyScope::CallerOperation,
        ),
        nokv_client::IdempotentPublishError::IdentityConflict { .. } => CliFailure::new(
            "RequestReplayMismatch",
            error.to_string(),
            error.class(),
            IdempotencyScope::CallerOperation,
        ),
        nokv_client::IdempotentPublishError::InProgress { .. } => CliFailure::new(
            "PublicationInProgress",
            error.to_string(),
            error.class(),
            IdempotencyScope::CallerOperation,
        ),
        nokv_client::IdempotentPublishError::Quarantined { .. } => CliFailure::new(
            "Quarantined",
            error.to_string(),
            error.class(),
            IdempotencyScope::CallerOperation,
        ),
    };
    failure.details_mut()["attempt"] = json!(attempt);
    failure
}

fn identified_put_json(
    operation: &str,
    operation_id: nokv_protocol::OperationIdentity,
    receipt: &nokv_client::IdempotentPublishReceipt,
) -> Value {
    let publication = &receipt.publication;
    json!({
        "status": "success",
        "operation": operation,
        "operation_id": encode_lowercase_hex(&operation_id.0),
        "publication_operation_id": encode_lowercase_hex(&publication.operation_id.0),
        "attempt": receipt.attempt,
        "state": "committed",
        "next_action": "none",
        "replayed": receipt.replayed,
        "workbench_id": publication.target.workbench.as_str(),
        "path": publication.target.path.as_str(),
        "workspace_revision": publication.workspace_revision,
        "generation": publication.generation,
        "artifact_revision_id": encode_lowercase_hex(&publication.artifact_revision_id.0),
        "logical_size": publication.logical_size,
        "body_digest": publication.body_digest.as_str(),
        "commit_version": receipt.commit_version,
    })
}

/// A rename or remove failure, resolved through its caller request id.
fn request_client_failure(
    error: nokv_client::ClientError,
    request_id: nokv_protocol::RequestIdentity,
) -> CliFailure {
    let agent = AgentError::from(backend::map_client_error(error));
    let mut failure = CliFailure::from_agent(&agent, IdempotencyScope::CallerRequest);
    failure.details_mut()["request_id"] = json!(encode_lowercase_hex(&request_id.0));
    failure
}

/// A failure of an append or operation command over its caller operation id.
fn append_client_failure(error: nokv_client::ClientError) -> CliFailure {
    let cleanup_unresolved = matches!(
        error,
        nokv_client::ClientError::AppendCleanupUnresolved { .. }
    );
    let failure = CliFailure::from_agent(
        &backend::map_append_client_error(error).into(),
        IdempotencyScope::CallerOperation,
    );
    if cleanup_unresolved {
        failure.with_next_action(NextAction::RetrySameCleanup)
    } else {
        failure
    }
}

fn append_payload(
    payload: &AppendPayload,
    max_bytes: usize,
) -> Result<(Vec<u8>, &'static str), String> {
    let (bytes, content_type) = match payload {
        AppendPayload::Text(text) => {
            if text.len() > max_bytes {
                return Err(format!(
                    "append delta is {} bytes, maximum is {max_bytes}",
                    text.len()
                ));
            }
            (text.as_bytes().to_vec(), "text/plain; charset=utf-8")
        }
        AppendPayload::Base64(encoded) => {
            let max_encoded_bytes = max_bytes.div_ceil(3).saturating_mul(4);
            if encoded.len() > max_encoded_bytes {
                return Err(format!(
                    "append base64 payload exceeds the encoded limit for {max_bytes} decoded bytes"
                ));
            }
            (
                STANDARD
                    .decode(encoded)
                    .map_err(|error| format!("append base64 payload is invalid: {error}"))?,
                "application/octet-stream",
            )
        }
        AppendPayload::File(path) => (
            transfer::read_collect_source(path, max_bytes)?,
            "application/octet-stream",
        ),
    };
    if bytes.len() > max_bytes {
        return Err(format!(
            "append delta is {} bytes, maximum is {max_bytes}",
            bytes.len(),
        ));
    }
    Ok((bytes, content_type))
}

fn append_result_json(result: &nokv_protocol::AppendResult) -> Value {
    json!({
        "operation_id": encode_lowercase_hex(&result.operation_id.0),
        "publication_operation_id": encode_lowercase_hex(&result.publication_operation_id.0),
        "artifact_revision_id": encode_lowercase_hex(&result.artifact_revision_id.0),
        "workbench_id": result.target.workbench.as_str(),
        "path": result.target.path.as_str(),
        "workspace_incarnation_id": encode_lowercase_hex(&result.workspace_incarnation_id.0),
        "workspace_revision": result.workspace_revision,
        "generation": result.generation,
        "logical_size": result.logical_size,
        "body_digest": result.body_digest.as_str(),
    })
}

fn append_receipt_json(call: &nokv_client::ClientCall<nokv_protocol::AppendResult>) -> Value {
    let mut receipt = append_result_json(&call.value);
    receipt["status"] = json!("success");
    receipt["operation"] = json!("append");
    receipt["state"] = json!("committed");
    receipt["next_action"] = json!("none");
    receipt["replayed"] = json!(call.replayed);
    receipt["commit_version"] = json!(call.commit_version);
    receipt
}

fn run_operation_status(
    invocation: &Invocation,
    operation_id: nokv_protocol::OperationIdentity,
) -> Result<(), CliFailure> {
    (|| {
        let client = build_workspace_path_client(invocation)?;
        let call = client
            .get_operation(nokv_protocol::GetOperationRequest { operation_id })
            .map_err(operation_read_failure)?;
        if call.value.token.operation_id != operation_id {
            return Err(CliFailure::from(
                "operation status returned a different identity".to_owned(),
            ));
        }
        let status = if call.value.kind == nokv_protocol::OperationKind::ArtifactAppend {
            append_status_json(&call)?
        } else {
            operation_status_json(&call)?
        };
        Ok(print_json(&status)?)
    })()
    .map_err(|failure| operation_failure(failure, operation_id))
}

fn append_status_json(
    call: &nokv_client::ClientCall<nokv_protocol::OperationStatus>,
) -> Result<Value, CliFailure> {
    let mut result = append_operation_json(&call.value)?;
    result["commit_version"] = json!(call.commit_version);
    result["replayed"] = json!(call.replayed);
    Ok(result)
}

/// Status of one publish, commit, or restore operation with its stored
/// receipt. A receipt is the original durable result, not a re-evaluation of
/// current state.
fn operation_status_json(
    call: &nokv_client::ClientCall<nokv_protocol::OperationStatus>,
) -> Result<Value, CliFailure> {
    let status = &call.value;
    let observation = nokv_client::operation_observation(status).map_err(append_client_failure)?;
    let operation = match status.kind {
        nokv_protocol::OperationKind::ArtifactPublish => "publish",
        nokv_protocol::OperationKind::Commit => "commit",
        nokv_protocol::OperationKind::Restore => "restore",
        nokv_protocol::OperationKind::ArtifactAppend => "append",
    };
    let receipt = match &status.result {
        Some(nokv_protocol::OperationResult::ArtifactPublish(result)) => json!({
            "operation_id": encode_lowercase_hex(&result.operation_id.0),
            "workbench_id": result.target.workbench.as_str(),
            "path": result.target.path.as_str(),
            "workspace_revision": result.workspace_revision,
            "generation": result.generation,
            "artifact_revision_id": encode_lowercase_hex(&result.artifact_revision_id.0),
            "logical_size": result.logical_size,
            "body_digest": result.body_digest.as_str(),
        }),
        Some(nokv_protocol::OperationResult::Commit(result)) => json!({
            "operation_id": encode_lowercase_hex(&result.operation_id.0),
            "commit_identity": encode_lowercase_hex(&result.commit_id.0),
            "workbench_id": result.workbench.as_str(),
            "head_generation": result.head_generation,
            "member_count": result.member_count,
            "member_digest": encode_lowercase_hex(&result.member_digest.0),
        }),
        Some(nokv_protocol::OperationResult::Restore(result)) => json!({
            "operation_id": encode_lowercase_hex(&result.operation_id.0),
            "destination_workbench_id": result.destination.workbench.as_str(),
            "destination_workspace_incarnation_id":
                encode_lowercase_hex(&result.destination.workspace_incarnation_id.0),
            "member_count": result.member_count,
            "member_digest": encode_lowercase_hex(&result.member_digest.0),
            "metadata_rows_copied": result.metadata_rows_copied,
            "object_bytes_copied": result.object_bytes_copied,
        }),
        Some(nokv_protocol::OperationResult::ArtifactAppend(_)) | None => Value::Null,
    };
    let target = match (
        &status.publish_preparation,
        &status.commit_preparation,
        &status.restore_preparation,
    ) {
        (Some(publish), _, _) => json!({
            "workbench_id": publish.target.workbench.as_str(),
            "path": publish.target.path.as_str(),
            "workspace_incarnation_id": encode_lowercase_hex(&publish.workspace_incarnation_id.0),
            "artifact_revision_id": encode_lowercase_hex(&publish.artifact_revision_id.0),
        }),
        (_, Some(commit), _) => json!({
            "workbench_id": commit.request.workbench.as_str(),
            "commit_identity": encode_lowercase_hex(&commit.request.commit_id.0),
        }),
        (_, _, Some(restore)) => json!({
            "source_commit_identity": encode_lowercase_hex(&restore.source_commit.commit_id.0),
        }),
        _ => Value::Null,
    };
    Ok(json!({
        "status": "success",
        "operation": operation,
        "operation_id": encode_lowercase_hex(&status.token.operation_id.0),
        "initiator": status.initiator.map(|initiator| encode_lowercase_hex(&initiator.0)),
        "state": observation.state.as_str(),
        "next_action": observation.next_action.as_str(),
        "observed_state": format!("{:?}", status.state),
        "target": target,
        "progress": status.progress,
        "failure": status.failure.as_ref().map(|failure| json!({
            "code": format!("{:?}", failure.code),
            "message": failure.message,
            "retryable": failure.retryable,
            "conflict": failure.conflict.map(|conflict| format!("{conflict:?}")),
            "current_generation": failure.current_generation,
        })),
        "receipt": receipt,
    }))
}

fn append_operation_json(status: &nokv_protocol::OperationStatus) -> Result<Value, CliFailure> {
    let recovery = nokv_client::append_operation_recovery(status).map_err(append_client_failure)?;
    let preparation = status
        .append_preparation
        .as_ref()
        .ok_or_else(|| CliFailure::from("append status has no preparation".to_owned()))?;
    let receipt = match &status.result {
        Some(nokv_protocol::OperationResult::ArtifactAppend(result)) => {
            Some(append_result_json(result))
        }
        _ => None,
    };
    let attempt_failure = preparation.attempt_failure.as_ref();
    Ok(json!({
        "status": "success",
        "operation": "append",
        "operation_id": encode_lowercase_hex(&status.token.operation_id.0),
        "initiator": status.initiator.map(|initiator| encode_lowercase_hex(&initiator.0)),
        "state": recovery.state.as_str(),
        "next_action": recovery.next_action.as_str(),
        "observed_state": format!("{:?}", status.state),
        "publication_operation_id": encode_lowercase_hex(&preparation.publication_operation_id.0),
        "attempt": preparation.attempt,
        "cleanup_retry_count": preparation.cleanup_retry_count,
        "attempt_phase": preparation.attempt_phase,
        "activity_deadline_ms": preparation.activity_deadline_ms,
        "workspace_incarnation_id": encode_lowercase_hex(&preparation.workspace_incarnation_id.0),
        "workbench_id": preparation.target.workbench.as_str(),
        "path": preparation.target.path.as_str(),
        "progress": status.progress,
        "cause_code": attempt_failure.map(|failure| format!("{:?}", failure.code)),
        "failure_message": attempt_failure.map(|failure| failure.message.as_str()),
        "attempt_failure": attempt_failure.map(|failure| json!({
            "code": format!("{:?}", failure.code),
            "message": failure.message,
            "retryable": failure.retryable,
            "conflict": failure.conflict.map(|conflict| format!("{conflict:?}")),
            "current_generation": failure.current_generation,
        })),
        "receipt": receipt,
    }))
}

fn operation_token_json(token: nokv_protocol::OperationToken) -> Value {
    json!({
        "operation_id": encode_lowercase_hex(&token.operation_id.0),
        "state_digest": encode_lowercase_hex(&token.state_digest.0),
    })
}

fn cleanup_retry_receipt_json(receipt: &nokv_protocol::AppendCleanupRetryResult) -> Value {
    json!({
        "operation_id": encode_lowercase_hex(&receipt.operation_id.0),
        "publication_operation_id": encode_lowercase_hex(&receipt.publication_operation_id.0),
        "cleanup_retry_count": receipt.cleanup_retry_count,
        "expected_state_digest": encode_lowercase_hex(&receipt.expected_state_digest.0),
    })
}

fn append_inspection_json(
    call: &nokv_client::ClientCall<nokv_client::AppendOperationInspection>,
) -> Result<Value, CliFailure> {
    let inspection = &call.value.inspection;
    let mut result = append_operation_json(&inspection.operation)?;
    result["action"] = json!("inspect");
    result["operation_token"] = operation_token_json(inspection.operation.token);
    result["publication_token"] = operation_token_json(inspection.publication_token);
    result["object_namespace_id"] = json!(encode_lowercase_hex(&inspection.object_namespace_id.0));
    result["registered_count"] = json!(inspection.registered_count);
    result["cleanup_cursor"] = json!(inspection.cleanup_cursor);
    result["remaining_count"] = json!(inspection.remaining_count);
    result["entries"] = json!(inspection.entries);
    result["next_cursor"] = json!(call
        .value
        .next_cursor
        .as_ref()
        .map(|cursor| STANDARD.encode(cursor)));
    result["commit_version"] = json!(call.commit_version);
    result["replayed"] = json!(call.replayed);
    Ok(result)
}

fn append_recovery_json(
    call: &nokv_client::ClientCall<nokv_client::AppendRecoveryRequestResult>,
) -> Result<Value, CliFailure> {
    let mut result = append_operation_json(&call.value.operation)?;
    result["action"] = json!("recover");
    result["operation_token"] = operation_token_json(call.value.operation.token);
    result["requested"] = json!(call.value.requested);
    result["recovery_receipt"] = json!(call.value.receipt.as_ref().map(cleanup_retry_receipt_json));
    result["commit_version"] = json!(call.commit_version);
    result["replayed"] = json!(call.replayed);
    Ok(result)
}

fn run_operation_inspect(
    invocation: &Invocation,
    operation_id: nokv_protocol::OperationIdentity,
    limit: u32,
    cursor: Option<Vec<u8>>,
) -> Result<(), CliFailure> {
    (|| {
        let client = build_workspace_path_client(invocation)?;
        let call = client
            .inspect_append_operation(operation_id, nokv_protocol::PageRequest { cursor, limit })
            .map_err(operation_read_failure)?;
        Ok(print_json(&append_inspection_json(&call)?)?)
    })()
    .map_err(|failure| operation_failure(failure, operation_id))
}

fn run_operation_recover(
    invocation: &Invocation,
    operation_id: nokv_protocol::OperationIdentity,
    expected_state_digest: Option<[u8; 32]>,
) -> Result<(), CliFailure> {
    (|| {
        let client = build_workspace_path_client(invocation)?;
        let expected = expected_state_digest.map(|digest| nokv_protocol::OperationToken {
            operation_id,
            state_digest: nokv_protocol::Digest(digest),
        });
        let call = client
            .recover_append_operation(operation_id, expected)
            .map_err(append_client_failure)?;
        Ok(print_json(&append_recovery_json(&call)?)?)
    })()
    .map_err(|failure| operation_failure(failure, operation_id))
}

/// A failed read of an operation changed nothing, so a lost reply is only
/// transient.
fn operation_read_failure(error: nokv_client::ClientError) -> CliFailure {
    CliFailure::from_agent(
        &backend::map_append_client_error(error).into(),
        IdempotencyScope::ReadOnly,
    )
}

/// A failed `operation` command. An absent operation is a point-in-time
/// observation: resubmitting the command that names it, with the same
/// identity and inputs, admits it at most once.
fn operation_failure(
    failure: CliFailure,
    operation_id: nokv_protocol::OperationIdentity,
) -> CliFailure {
    let failure = if failure.class() == ErrorClass::NotFound {
        failure.with_next_action(NextAction::ResubmitSame)
    } else {
        failure
    };
    with_append_details(failure, operation_id)
}

fn append_input_error(message: String) -> CliFailure {
    CliFailure::new(
        "InvalidArgument",
        message,
        ErrorClass::InvalidInput,
        IdempotencyScope::CallerOperation,
    )
}

/// The append-specific details every append and operation failure carries.
/// `details.next_action` predates the top-level field and repeats it, except
/// for a cleanup retry, which names its saved state digest.
fn with_append_details(
    mut failure: CliFailure,
    operation_id: nokv_protocol::OperationIdentity,
) -> CliFailure {
    let next_action = failure.next_action().as_str();
    let details = failure.details_mut();
    if !details.is_object() {
        *details = json!({});
    }
    details["operation_id"] = json!(encode_lowercase_hex(&operation_id.0));
    if details["next_action"].is_null() {
        details["next_action"] = json!(next_action);
    }
    details["cause_code"] = details["code"].clone();
    if details["publication_operation_id"].is_null() {
        details["publication_operation_id"] = Value::Null;
    }
    failure
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

/// Session form of `materialize`: the whole live artifact inline.
struct SessionMaterializer {
    backend: CliWorkbenchBackend,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MaterializeArguments {
    id: String,
    section: String,
    path: String,
}

impl workbench_session::SessionCommands for SessionMaterializer {
    fn materialize(&self, arguments: &Value) -> Result<Value, nokv_agent::AgentError> {
        let arguments: MaterializeArguments =
            serde_json::from_value(arguments.clone()).map_err(|error| {
                nokv_agent::AgentError::invalid_arguments(format!("materialize: {error}"))
            })?;
        let scoped = scoped_artifact(&arguments.id, &arguments.section, &arguments.path)
            .map_err(nokv_agent::AgentError::invalid_arguments)?;
        let artifact = self
            .backend
            .read(ReadRequest {
                path: scoped,
                view: ReadView::Live,
            })
            .map_err(AgentError::from)?
            .ok_or_else(|| {
                nokv_agent::AgentError::backend(
                    "NotFound",
                    format!("{}/{} does not exist", arguments.section, arguments.path),
                    false,
                    json!({}),
                )
            })?;
        Ok(json!({
            "status": "success",
            "size_bytes": artifact.bytes.len(),
            "generation": artifact.metadata.generation,
            "digest_uri": artifact.metadata.digest_uri,
            "base64": STANDARD.encode(&artifact.bytes),
        }))
    }
}

fn materialize(
    backend: &CliWorkbenchBackend,
    workbench: &str,
    section: &str,
    path: &str,
    destination: &Path,
) -> Result<(), CliFailure> {
    let scoped = scoped_artifact(workbench, section, path).map_err(CliFailure::invalid_input)?;
    let artifact = backend
        .read(ReadRequest {
            path: scoped,
            view: ReadView::Live,
        })
        .map_err(|error| CliFailure::from_agent(&error.into(), IdempotencyScope::ReadOnly))?
        .ok_or_else(|| {
            CliFailure::new(
                "NotFound",
                format!("{section}/{path} does not exist"),
                ErrorClass::NotFound,
                IdempotencyScope::ReadOnly,
            )
        })?;
    let written = transfer::write_materialized_file(destination, &artifact.bytes)
        .map_err(CliFailure::invalid_input)?;
    Ok(print_json(&json!({
        "status": "success",
        "destination": written,
        "size_bytes": artifact.bytes.len(),
        "generation": artifact.metadata.generation,
        "digest_uri": artifact.metadata.digest_uri,
    }))?)
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
) -> Result<(), CliFailure> {
    let bytes =
        transfer::read_collect_source(source, max_bytes).map_err(CliFailure::invalid_input)?;
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
    let result = execute_tool(handler, "workbench_put_file", &arguments)
        .map_err(|error| CliFailure::from_agent(&error, IdempotencyScope::Unidentified))?;
    Ok(print_json(&result)?)
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
        let mut identity =
            build_info::identity(WORKBENCH_CONTRACT_SCHEMA, tool_definitions().len());
        identity["cli_contract"] = json!(failure_envelope::CLI_CONTRACT);
        identity["rpc_schema"] = json!(nokv_protocol::WORKSPACE_PROTOCOL_SCHEMA);
        print_json(&identity)
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

const HELP_TEXT: &str = "\
NoKV Agent-workspace CLI

USAGE:
  nokv [connection/object options] workbench <tool> '<json arguments>'
  nokv [connection/object options] workbench-session < requests.jsonl
  nokv [connection/object options] materialize <workbench> <section> <path> <destination>
  nokv [connection/object options] collect <workbench> <section> <source> <path> [--replace] [--expected-generation N] [--content-type TYPE] [--operation-id HEX32 [--expected-workspace-incarnation-id HEX32]]
  nokv [route/agent options] operation status (<HEX32> | --commit-identity <HEX64>)
  nokv [route/agent options] operation inspect <HEX32> [--limit N] [--cursor BASE64]
  nokv [route/agent options] operation recover <HEX32> [--expected-state-digest HEX64]
  nokv [route/agent options] workspace-path rename <workbench> <section> <source> <destination> --expected-generation N --request-id HEX32
  nokv [route/agent options] workspace-path remove <workbench> <section> <path> --expected-generation N --request-id HEX32
  nokv [route/agent options] workspace-path stat <workbench> <section> <path>
  nokv [route/agent/object options] workspace-path put <workbench> <section> <path> --operation-id HEX32 (--text TEXT | --base64 BASE64 | --file PATH) [--expected-generation N] [--expected-workspace-incarnation-id HEX32] [--content-type TYPE] [--block-size N]
  nokv [route/agent/object options] workspace-path append <workbench> <section> <path> --operation-id HEX32 (--text TEXT | --base64 BASE64 | --file PATH) [--content-type TYPE] [--max-logical-size N] [--block-size N] [--expected-workspace-incarnation-id HEX32]
  nokv --root-id HEX32 --agent-id HEX32 (--etcd-endpoint URL | --control-dir PATH) provision <logical-shard-id-hex32> --workbench-root PATH [adoption options]
  nokv [owner options] serve
  nokv (--etcd-endpoint URL | --control-dir PATH) identity <identity command>
  nokv schema
  nokv version [--json]
  nokv --version

AGENT CONTROL ROUTING:
  --root-id HEX32
  --agent-id HEX32 is required by provision, and without RPC TLS by workbench, materialize, collect,
    workspace-path, and operation commands
  AgentId is a durable deployment identity used to prevent root misconfiguration; it is not an authentication credential
  --metadata-address HOST:PORT --logical-shard-id HEX32 --object-namespace-id HEX32
    --placement-generation N --owner-epoch N
  static routing is a point-in-time pin and cannot refresh after placement changes or owner restarts
  use --etcd-endpoint or --control-dir for self-refreshing routing
  Agent-facing commands, provision, and serve require control routing so the RootId-to-AgentId binding can be verified;
  a static pin is rejected there and remains a Rust SDK client option only
  --etcd-endpoint URL [--etcd-endpoint URL ...] for owners and clients on several hosts
  --control-dir PATH for an owner and clients on one host: a local directory whose owner
    session is an OS file lock released when the owner process exits; not for network filesystems

AGENT PRESENTATION:
  provision --workbench-root /agents/TEAM/wb binds the root's presentation root once; workbench,
    workbench-session, materialize, and collect read it from the control plane, so every Agent
    of a root presents it the same way
  the presentation root is durable: it shapes responses and canonical v1 manifest paths
  RootId remains the only storage/routing identity; the presentation root never enters Holt keys
  workspace-path is a custom CLI surface; it does not add to the fixed 18 Workbench tools
  workspace-path rename/remove require an explicit lowercase HEX32 request id for exact cross-process replay
  workspace-path append requires a stable lowercase HEX32 logical operation id
  workspace-path put publishes a whole body under a caller operation id: create-only unless --expected-generation
    pins the current generation; the same id and inputs from any process return the original receipt
  workspace-path stat reports the incarnation, workspace revision, generation and artifact revision of a path
  a failed command writes one JSON line to stderr with its failure class and next_action and exits with that
    class's code (docs/cli-contract.md)
  operation status observes an append, publish, commit or restore by its operation id, without a payload,
    workspace, or object-store connection; --commit-identity names a commit by the identity workbench_commit reports
  operation inspect returns retained staged entries, default 32 per page (maximum 192); reuse its opaque cursor
  save inspect.operation_token.state_digest before operation recover and reuse it after an unknown reply
  operation recover asks the owner to retry quarantined cleanup; it does not resubmit the append payload
  the first append requires an existing workspace; replays resolve its original incarnation
  append delta and default total body limits are 16 MiB; --max-logical-size raises the bound body limit
  append returns the original publication receipt on replay; retain the id and incarnation across restarts
  append limits the delta with --max-artifact-bytes; --max-logical-size optionally limits the resulting body

CLIENT RETRIES:
  --max-attempts N (1..16, default 3): attempts per request, counting the first; an attempt that failed before it
    could take effect (a refused or dropped connection, a retryable owner answer) is repeated, and a request that
    reached a changed owner is re-routed and repeated at once
  --retry-backoff-ms N (default 0) --max-retry-backoff-ms N (default 2000): the pause before the second attempt,
    doubling per further attempt up to the bound; 0 repeats at once, and a backoff above its bound is invalid_input
  a workbench-session that must outlive an owner restart starts with --max-attempts 16 --retry-backoff-ms 250
  provision and serve repeat the object provider's admission probe within the same budget while the provider
    is unreachable; a provider that lacks a required capability fails at once as operator_required
  with a backoff set, an identified write the owner left running when it died is re-observed with the same budget
    until the restarted owner has aborted it and the next attempt publishes; in_progress is answered when the
    budget runs out, or at once without a backoff

OWNER:
  provision binds RootId to an explicit AgentId and a Workbench root, then creates namespace, shard, and affinity
  --adopt-legacy-agent-binding is a one-time explicit migration for a verified legacy root
  --adopt-legacy-object-namespace is a one-time explicit migration after verifying bucket/prefix
  serve validates every active root's Agent binding; it does not require or compare one shard-wide AgentId
  serve mounts a root provisioned on its shard after it started, at --lifecycle-interval-millis and under the same
    checks; a root bound to another tenant or object namespace is not mounted, and the refusal is logged once
  a root bound to a tenant is served only over RPC TLS: a plaintext serve refuses to start
  --root-id HEX32 (--etcd-endpoint URL | --control-dir PATH) --node-id ID
  --append-activity-lease-ms N: serve only, range 1000..=86400000, default 1800000; clock-skew grace is additional
  --advertise-endpoint HOST:PORT --bind HOST:PORT
  --etcd-lease-ttl-seconds N (default 10; owner keepalive runs every max(1, N/3) seconds)
  --handshake-timeout-millis N --max-inflight-connections N
  --metadata-create PATH starts the first standalone local-WAL owner
  --metadata-reopen PATH restarts the same exclusive local-WAL authority after lease loss
  --metadata-recover-log PATH installs or resumes one exact receipt-directed shared-log frontier
  --recovery-publication shared|local-only (default local-only; --metadata-recover-log implies shared)
    local-only keeps the exclusive local WAL as the only recovery authority, publishes nothing,
    and cannot combine with --metadata-recover-log
    shared publishes every applied outbox tail as immutable log segments plus control references
    before a response leaves the shard; without checkpoint compaction the chain is bounded by the
    control record size, and a shard that reaches that bound stops accepting writes

RPC TLS:
  --rpc-tls-cert PEM --rpc-tls-key PEM: with serve, the owner's certificate chain and key. The owner then
    accepts only TLS 1.3 clients whose certificate is a current credential of the tenant bound to its roots
  --rpc-tls-ca PEM --rpc-tls-cert PEM --rpc-tls-key PEM --rpc-tls-server-name NAME: with a command that
    connects to an owner, the roots that sign the owner's certificate, this client's certificate and key,
    and the name the owner's certificate must carry; there is no fallback to plaintext
  a refused credential or a TLS mismatch fails with class operator_required before any request is sent
  every request needs a role on its root: reader reads, writer also writes, and operator also reconciles
    quarantined publications; a refusal is PermissionDenied, class operator_required
  the owner rereads its tenant's records every second: a revoked credential or role stops working within
    that interval plus the request in flight
  over RPC TLS, --agent-id is not needed and the AgentId binding is not checked

CONTROL PLANE TLS:
  --etcd-tls-ca PEM --etcd-tls-cert PEM --etcd-tls-key PEM: with https:// --etcd-endpoint URLs, the roots that
    sign etcd's certificate, and this process's certificate chain and key; the three are given together, and
    https:// endpoints require them
  etcd started with --client-cert-auth authenticates the certificate's Common Name as its user, and that user's
    roles decide which keys this process may read or write: an owner reads and writes only its shard, an Agent
    reads only its root's routing keys (docs/control-plane-credentials.md lists the keys per role)
  a certificate etcd does not accept fails as unauthenticated, and a key its roles do not grant as
    permission_denied, before any request reaches an owner

TENANT IDENTITY:
  identity tenant create <tenant-id-hex32>
  identity principal create <tenant-id-hex32> <principal-id-hex32>
  identity credential add <tenant-id-hex32> <principal-id-hex32> (--certificate PEM | --fingerprint HEX64)
    --expires-at-unix N
  identity credential revoke <tenant-id-hex32> <fingerprint-hex64>
  identity role (grant | revoke) <tenant-id-hex32> <principal-id-hex32> --role reader|writer|operator
    (--tenant-wide | --root HEX32)
  identity root bind <root-id-hex32> <tenant-id-hex32>
  identity show <tenant-id-hex32>
  records live in the control plane; running these commands requires write access to the control store
  a credential is the SHA-256 of a client certificate's DER bytes; --certificate reads the first PEM certificate
  a root belongs to one tenant, and a root-scoped role requires that binding first
  the server does not enforce these records yet: until it does, every deployment remains trusted

OBJECT DATA:
  --object-bucket NAME [--object-endpoint URL] [--object-root PREFIX]
  [--hot-cache-dir PATH --hot-cache-bytes BYTES]

NoKV exposes SDK and CLI operations. It does not expose FUSE or POSIX.";

fn print_help() {
    println!("{HELP_TEXT}");
}

#[cfg(feature = "control")]
fn open_control(
    invocation: &Invocation,
) -> Result<Arc<dyn nokv_control::ControlStore>, CliFailure> {
    connection::open_control(&invocation.client.routing).map_err(connection_failure)
}

#[cfg(feature = "control")]
fn open_control_plane(invocation: &Invocation) -> Result<connection::ControlPlane, CliFailure> {
    connection::open_control_plane(&invocation.client.routing).map_err(connection_failure)
}

#[cfg(feature = "control")]
fn run_provision(
    invocation: &Invocation,
    logical_shard_id: &str,
    workbench_root: &str,
    adopt_legacy_object_namespace: bool,
    adopt_legacy_agent_binding: bool,
) -> Result<(), CliFailure> {
    use nokv_control::{LogicalShardId, RootId, RootPresentationBinding};
    use nokv_types::{RootPlacementLifecycle, WorkbenchRoot};

    let control_error = |error: nokv_control::ControlError| {
        control_failure("ControlFailure", error.to_string(), error.class())
    };
    let root_id = RootId::from(
        connection::configured_root_id(&invocation.client).map_err(connection_failure)?,
    );
    let logical_shard_id = LogicalShardId::from(
        connection::parse_logical_shard_id(logical_shard_id).map_err(connection_failure)?,
    );
    let agent_id = connection::parse_agent_id(
        invocation
            .agent_id
            .as_deref()
            .ok_or_else(|| CliFailure::invalid_input("provision requires --agent-id"))?,
    )
    .map_err(connection_failure)?;
    let workbench_root = WorkbenchRoot::new(workbench_root)
        .map_err(|error| CliFailure::invalid_input(format!("--workbench-root: {error}")))?;
    let connection::ControlPlane { control, identity } = open_control_plane(invocation)?;
    let existing_placement =
        provision::preflight_provision(control.as_ref(), root_id, logical_shard_id)
            .map_err(provision_failure)?;
    let binding_preexisting = provision::ensure_root_agent_binding(
        control.as_ref(),
        root_id,
        agent_id,
        adopt_legacy_agent_binding,
    )
    .map_err(provision_failure)?;
    // Every Agent of the root presents it under this one Workbench root.
    let presentation_preexisting = identity
        .get_root_presentation(&root_id)
        .map_err(control_error)?
        .is_some();
    let presentation = identity
        .bind_root_presentation(RootPresentationBinding {
            root_id,
            workbench_root,
        })
        .map_err(control_error)?;
    // Provision proves the provider semantics the owner's writes will need.
    let objects = CliObjectStore::build(&invocation.client.object)?;
    admit_object_provider(&objects, invocation)?;
    let namespace_id = match control
        .get_root_object_namespace_binding(&root_id)
        .map_err(control_error)?
    {
        Some(binding) => {
            objects
                .clone()
                .bind(binding.object_namespace_id)
                .map_err(object_failure)?;
            binding.object_namespace_id
        }
        None => {
            if existing_placement.is_some() && !adopt_legacy_object_namespace {
                return Err(control_failure(
                    "LegacyObjectNamespaceAdoptionRequired",
                    "legacy root has no durable object namespace binding; verify the configured bucket/prefix, then rerun provision with --adopt-legacy-object-namespace",
                    ErrorClass::OperatorRequired,
                ));
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
    .map_err(provision_failure)?;
    objects.bind(namespace_id).map_err(object_failure)?;
    let lifecycle = match outcome.placement.lifecycle {
        RootPlacementLifecycle::Provisioning => "provisioning",
        RootPlacementLifecycle::Active => "active",
        RootPlacementLifecycle::Draining => "draining",
        RootPlacementLifecycle::Retired => "retired",
    };
    Ok(print_json(&json!({
        "status": "success",
        "root_id": encode_lowercase_hex(root_id.as_bytes()),
        "agent_id": encode_lowercase_hex(agent_id.as_bytes()),
        "binding_preexisting": binding_preexisting,
        "workbench_root": presentation.workbench_root.as_str(),
        "presentation_preexisting": presentation_preexisting,
        "logical_shard_id": encode_lowercase_hex(logical_shard_id.as_bytes()),
        "object_namespace_id": encode_lowercase_hex(namespace_id.as_bytes()),
        "placement_generation": outcome.placement.placement_generation.get(),
        "lifecycle": lifecycle,
        "logical_shard_preexisting": outcome.logical_shard_preexisting,
        "object_namespace_preexisting": outcome.object_namespace_preexisting,
        "placement_preexisting": outcome.placement_preexisting,
        "activation_required": outcome.activation_required,
    }))?)
}

#[cfg(not(feature = "control"))]
fn run_provision(
    _invocation: &Invocation,
    _logical_shard_id: &str,
    _workbench_root: &str,
    _adopt_legacy_object_namespace: bool,
    _adopt_legacy_agent_binding: bool,
) -> Result<(), CliFailure> {
    Err("provision requires the nokv control feature"
        .to_owned()
        .into())
}

#[cfg(feature = "control")]
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

#[cfg(feature = "control")]
fn validate_served_root_agent_bindings(
    control: &dyn nokv_control::ControlStore,
    placements: &[nokv_control::RootPlacement],
) -> Result<(), String> {
    for placement in placements {
        load_root_agent_binding(control, placement.root_id).map_err(|failure| failure.message())?;
    }
    Ok(())
}

/// The one tenant that owns every served root. An owner serving over TLS
/// authenticates clients against that tenant's credentials.
#[cfg(feature = "control")]
fn served_tenant(
    identity: &dyn nokv_control::IdentityStore,
    placements: &[nokv_control::RootPlacement],
) -> Result<nokv_types::TenantId, String> {
    let mut tenant = None;
    for placement in placements {
        let binding = identity
            .get_root_tenant(&placement.root_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                format!(
                    "root {} is not bound to a tenant; run `nokv identity root bind` before \
                     serving with --rpc-tls-cert",
                    encode_lowercase_hex(placement.root_id.as_bytes())
                )
            })?;
        match tenant {
            None => tenant = Some(binding.tenant_id),
            Some(owner_tenant) if owner_tenant != binding.tenant_id => {
                return Err(
                    "one owner serves one tenant, but the roots of this shard are bound to \
                     different tenants"
                        .to_owned(),
                );
            }
            Some(_) => {}
        }
    }
    tenant.ok_or_else(|| "serving over TLS requires at least one root".to_owned())
}

/// A plaintext owner authenticates nobody, so it refuses every root a tenant
/// owns: once a root is bound to a tenant, only a TLS owner serves it.
#[cfg(feature = "control")]
fn refuse_plaintext_for_tenant_roots(
    identity: &dyn nokv_control::IdentityStore,
    placements: &[nokv_control::RootPlacement],
) -> Result<(), String> {
    for placement in placements {
        if let Some(binding) = identity
            .get_root_tenant(&placement.root_id)
            .map_err(|error| error.to_string())?
        {
            return Err(format!(
                "root {} is bound to tenant {}; a tenant-bound root is served only over RPC TLS, \
                 so start serve with --rpc-tls-cert and --rpc-tls-key",
                encode_lowercase_hex(placement.root_id.as_bytes()),
                encode_lowercase_hex(binding.tenant_id.as_bytes())
            ));
        }
    }
    Ok(())
}

/// A fresh idempotency key for one durable root-fence step of this owner
/// process: unique per process, instant and sequence.
#[cfg(feature = "control")]
fn bootstrap_request_id(domain: &[u8], root: nokv_control::RootId) -> nokv_types::RequestId {
    use sha2::{Digest, Sha256};

    static REQUEST_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.owner.bootstrap\0");
    hasher.update(domain);
    hasher.update(root.as_bytes());
    hasher.update(std::process::id().to_be_bytes());
    hasher.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_be_bytes(),
    );
    hasher.update(
        REQUEST_SEQUENCE
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_be_bytes(),
    );
    let digest: [u8; 32] = hasher.finalize().into();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    nokv_types::RequestId::from_bytes(id)
}

/// What a root must satisfy to be served by this owner, checked the same way
/// for the bootstrap roots and for roots provisioned while the owner runs.
#[cfg(feature = "control")]
struct RootMountChecks {
    control: Arc<dyn nokv_control::ControlStore>,
    identity: Arc<dyn nokv_control::IdentityStore>,
    expected_namespace: nokv_types::ObjectNamespaceId,
    /// The tenant a TLS owner serves; `None` for a plaintext owner.
    served_tenant: Option<nokv_types::TenantId>,
}

#[cfg(feature = "control")]
impl RootMountChecks {
    /// Why an Active placement of this shard cannot be mounted, or `None`
    /// when it can. A plaintext owner needs the root's AgentId binding and
    /// refuses a tenant-bound root; a TLS owner mounts only roots of the one
    /// tenant it serves; every root must share the shard's object namespace.
    fn refusal(&self, placement: &nokv_control::RootPlacement) -> Result<Option<String>, String> {
        let root = encode_lowercase_hex(placement.root_id.as_bytes());
        match self
            .control
            .get_root_object_namespace_binding(&placement.root_id)
            .map_err(|error| error.to_string())?
        {
            None => return Ok(Some(format!("root {root} has no object namespace binding"))),
            Some(binding) if binding.object_namespace_id != self.expected_namespace => {
                return Ok(Some(format!(
                    "root {root} is bound to another object namespace than this shard"
                )));
            }
            Some(_) => {}
        }
        match self.served_tenant {
            None => {
                if let Err(failure) =
                    load_root_agent_binding(self.control.as_ref(), placement.root_id)
                {
                    return Ok(Some(failure.message()));
                }
                if let Err(reason) = refuse_plaintext_for_tenant_roots(
                    self.identity.as_ref(),
                    std::slice::from_ref(placement),
                ) {
                    return Ok(Some(reason));
                }
            }
            Some(served) => match self
                .identity
                .get_root_tenant(&placement.root_id)
                .map_err(|error| error.to_string())?
            {
                None => return Ok(Some(format!("root {root} is not bound to a tenant"))),
                Some(binding) if binding.tenant_id != served => {
                    return Ok(Some(format!(
                        "root {root} is bound to tenant {}; this owner serves tenant {}",
                        encode_lowercase_hex(binding.tenant_id.as_bytes()),
                        encode_lowercase_hex(served.as_bytes())
                    )));
                }
                Some(_) => {}
            },
        }
        Ok(None)
    }
}

/// Mount roots provisioned on the owner's shard after it started, every
/// `interval`, until `stopped`, `shutdown`, or owner loss. Each mounted root
/// gets its own lifecycle worker through `start_lifecycle`. A refusal is
/// logged once per reason, and a mount failure is retried on the next cycle.
#[cfg(feature = "control")]
#[allow(clippy::too_many_arguments)]
fn mount_roots_until(
    stopped: &std::sync::atomic::AtomicBool,
    shutdown: &std::sync::atomic::AtomicBool,
    interval: std::time::Duration,
    server: &nokv_server::WorkspaceServer,
    checks: &RootMountChecks,
    seed: &nokv_control::RootPlacement,
    mut mounted: std::collections::BTreeSet<nokv_control::RootId>,
    start_lifecycle: &dyn Fn(nokv_protocol::RootRoute) -> Result<(), String>,
) {
    use std::sync::atomic::Ordering;

    const POLL: std::time::Duration = std::time::Duration::from_millis(50);
    let owner_loss = server.owner_loss_signal();
    let mut refusals = std::collections::BTreeMap::new();
    let stopping = || {
        stopped.load(Ordering::Acquire) || shutdown.load(Ordering::Acquire) || owner_loss.is_lost()
    };
    'cycles: loop {
        let due = std::time::Instant::now() + interval;
        while std::time::Instant::now() < due {
            if stopping() {
                break 'cycles;
            }
            std::thread::sleep(POLL);
        }
        let candidates = match active_shard_placements(checks.control.as_ref(), seed) {
            Ok(placements) => placements,
            Err(reason) => {
                eprintln!("root mounter: cannot list placements: {reason}");
                continue;
            }
        };
        let pending = candidates
            .into_iter()
            .filter(|placement| !mounted.contains(&placement.root_id))
            .collect::<Vec<_>>();
        for placement in &pending {
            let root = encode_lowercase_hex(placement.root_id.as_bytes());
            match checks.refusal(placement) {
                Ok(None) => {}
                Ok(Some(reason)) => {
                    if refusals.insert(placement.root_id, reason.clone()) != Some(reason.clone()) {
                        eprintln!("root {root} not mounted: {reason}");
                    }
                    continue;
                }
                Err(reason) => {
                    eprintln!("root mounter: cannot check root {root}: {reason}");
                    continue;
                }
            }
            let attach = nokv_server::RootAttach {
                root_id: placement.root_id,
                object_namespace_id: checks.expected_namespace,
                install_id: bootstrap_request_id(b"install", placement.root_id),
                bind_object_namespace_id: bootstrap_request_id(
                    b"bind-object-namespace",
                    placement.root_id,
                ),
                activate_id: bootstrap_request_id(b"activate", placement.root_id),
            };
            match server
                .mount_root(placement, attach)
                .map_err(|error| error.to_string())
                .and_then(|route| start_lifecycle(route).map(|()| route))
            {
                Ok(route) => {
                    mounted.insert(placement.root_id);
                    refusals.remove(&placement.root_id);
                    eprintln!(
                        "mounted root {root} at placement generation {} under owner epoch {}",
                        route.placement_generation, route.owner_epoch
                    );
                }
                Err(reason) => eprintln!("root {root} not mounted: {reason}"),
            }
        }
    }
}

#[cfg(feature = "control")]
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

#[cfg(feature = "control")]
fn run_server(invocation: &Invocation) -> Result<(), CliFailure> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use nokv_control::{NodeId, RecoveryPublication, RootId};
    use nokv_server::{
        bootstrap_shard, ArtifactLifecycleCleaner, LeaseMode, LifecycleObjectCleaner,
        LifecycleRunner, LifecycleRunnerOptions, OpenMode, RecoveryPublicationMode, RootAttach,
        RootOwnerRegistry, RpcServerTls, ServerOptions, ShardBoot, TenantAccess, WorkspaceServer,
        DEFAULT_MAX_POLICY_AGE,
    };

    const NON_EXPIRING_SESSION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

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

    let shutdown = install_shutdown_signal()?;
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
    let connection::ControlPlane { control, identity } =
        open_control_plane(invocation).map_err(|failure| failure.message())?;
    let placement = control
        .get_root_placement(&root_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "root placement does not exist; provision it before serve".to_owned())?;
    let placements = active_shard_placements(control.as_ref(), &placement)?;
    // Over TLS the owner authorizes principals by role; the plaintext
    // AgentId binding is not consulted. Without TLS nobody is authenticated,
    // so a root a tenant owns is not served at all.
    if invocation.server.tls.is_none() {
        validate_served_root_agent_bindings(control.as_ref(), &placements)?;
        refuse_plaintext_for_tenant_roots(identity.as_ref(), &placements)?;
    }
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
        return Err(CliFailure::invalid_input(
            "--lifecycle-interval-millis must be within 1..=60000",
        ));
    }
    if invocation.server.handshake_timeout_millis == 0
        || invocation.server.handshake_timeout_millis > 60_000
    {
        return Err(CliFailure::invalid_input(
            "--handshake-timeout-millis must be within 1..=60000",
        ));
    }
    if invocation.server.max_inflight_connections == 0
        || invocation.server.max_inflight_connections > 4_096
    {
        return Err(CliFailure::invalid_input(
            "--max-inflight-connections must be within 1..=4096",
        ));
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
            "one shard owner cannot serve roots bound to different object namespaces"
                .to_owned()
                .into(),
        );
    }
    // Roots mounted after the start pass the same checks as these did.
    let mount_checks = RootMountChecks {
        control: Arc::clone(&control),
        identity: Arc::clone(&identity),
        expected_namespace,
        served_tenant: match &invocation.server.tls {
            None => None,
            Some(_) => Some(served_tenant(identity.as_ref(), &placements)?),
        },
    };
    // TLS is loaded and checked before metadata open or ownership acquisition.
    let rpc_tls = match &invocation.server.tls {
        None => None,
        Some(tls) => {
            let tenant_id = mount_checks
                .served_tenant
                .expect("a TLS owner serves exactly one tenant");
            // On etcd the copy may serve as long as the owner's lease would;
            // past that the owner has lost the right to serve anyway.
            let max_policy_age = match &invocation.client.routing {
                cli::RoutingConfig::Etcd(config) => std::time::Duration::from_secs(
                    u64::try_from(config.lease_ttl_seconds).unwrap_or(1).max(1),
                ),
                _ => DEFAULT_MAX_POLICY_AGE,
            };
            let access = TenantAccess::load(identity, tenant_id, max_policy_age)
                .map_err(|error| error.to_string())?;
            let (chain, key) = rpc_tls::server_identity(tls)?;
            Some(RpcServerTls::new(chain, key, access).map_err(|error| error.to_string())?)
        }
    };
    let objects = Arc::new(
        CliObjectStore::build(&invocation.client.object)?
            .bind(expected_namespace)
            .map_err(|error| error.to_string())?,
    );
    admit_object_provider(&objects, invocation)?;
    let registry = Arc::new(RootOwnerRegistry::new());
    let owner = bootstrap_shard(
        Arc::clone(&control),
        Arc::clone(&registry),
        objects.clone(),
        ShardBoot {
            append_activity_lease_ms: invocation.server.append_activity_lease_ms,
            shard_id: placement.logical_shard_id,
            open: metadata,
            lease: LeaseMode::Acquire {
                owner: node_id,
                endpoint,
                previous_epoch: shard.owner_epoch,
            },
            recovery,
            recovery_publication: match invocation.server.recovery_publication {
                cli::RecoveryPublicationConfig::Shared => RecoveryPublicationMode::Shared,
                cli::RecoveryPublicationConfig::LocalOnly => RecoveryPublicationMode::LocalOnly,
            },
            roots: placements
                .iter()
                .map(|placement| RootAttach {
                    root_id: placement.root_id,
                    object_namespace_id: expected_namespace,
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

    // An expiring owner lease is renewed on the store's cadence. A session
    // that ends only with its process is still checked on a fixed cadence, so
    // a lost lock or a durable record changed outside it fails the owner
    // closed.
    let lease_renew_interval = owner
        .owner_lease_renew_interval()
        .unwrap_or(NON_EXPIRING_SESSION_CHECK_INTERVAL);

    let meta = Arc::clone(owner.meta());
    let recovery = Arc::clone(owner.recovery_publisher());
    let owner_routes = owner.routes().to_vec();
    let server = WorkspaceServer::new(
        ServerOptions {
            bind: invocation.server.bind,
            handshake_timeout: Duration::from_millis(invocation.server.handshake_timeout_millis),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            lease_renew_interval,
            max_inflight_connections: invocation.server.max_inflight_connections,
        },
        Arc::clone(&registry),
        vec![owner],
    )
    .map_err(|error| error.to_string())?;
    let server = match rpc_tls {
        Some(tls) => server.with_rpc_tls(tls),
        None => server,
    };
    let owner_loss = server.owner_loss_signal();
    let lifecycle_objects: Arc<dyn LifecycleObjectCleaner> =
        Arc::new(ArtifactLifecycleCleaner::new(objects));
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
                }
                .into());
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
                return Err(failures.join("; ").into());
            }
        }
    }

    // Roots provisioned on this shard while the owner runs are mounted on
    // the lifecycle cadence; their lifecycle workers join the bootstrap ones.
    let mounted_workers = std::sync::Mutex::new(Vec::new());
    let mounter_stopped = AtomicBool::new(false);
    let server_result = std::thread::scope(|scope| {
        let mounter = std::thread::Builder::new()
            .name("nokv-root-mounter".to_owned())
            .spawn_scoped(scope, || {
                mount_roots_until(
                    &mounter_stopped,
                    shutdown.as_ref(),
                    Duration::from_millis(invocation.server.lifecycle_interval_millis),
                    &server,
                    &mount_checks,
                    &placement,
                    placements
                        .iter()
                        .map(|placement| placement.root_id)
                        .collect(),
                    &|route| {
                        let lifecycle = LifecycleRunner::new(
                            Arc::clone(&meta),
                            Arc::clone(&registry),
                            route,
                            owner_loss.clone(),
                            Arc::clone(&lifecycle_objects),
                            recovery.clone(),
                            LifecycleRunnerOptions {
                                poll_interval: Duration::from_millis(
                                    invocation.server.lifecycle_interval_millis,
                                ),
                                ..LifecycleRunnerOptions::default()
                            },
                        )
                        .map_err(|error| error.to_string())?;
                        let worker_owner_loss = owner_loss.clone();
                        let worker_shutdown = Arc::clone(&shutdown);
                        let worker = std::thread::Builder::new()
                            .name(format!(
                                "nokv-workspace-lifecycle-{}",
                                encode_lowercase_hex(&route.root_id.0)
                            ))
                            .spawn(move || {
                                let result = lifecycle
                                    .run_until_owner_loss_or_shutdown(worker_shutdown.as_ref());
                                if result.is_err() {
                                    worker_owner_loss.fail_closed();
                                }
                                result
                            })
                            .map_err(|error| error.to_string())?;
                        mounted_workers
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(worker);
                        Ok(())
                    },
                )
            })
            .map_err(|error| format!("cannot start root mounter: {error}"));
        let server_result = server.run_until_shutdown(shutdown.as_ref());
        mounter_stopped.store(true, Ordering::Release);
        if let Ok(mounter) = mounter {
            let _ = mounter.join();
        }
        server_result
    });
    if server_result.is_err() {
        owner_loss.fail_closed();
    }

    let mut failures = Vec::new();
    join_lifecycle_workers(lifecycle_workers, &mut failures);
    join_lifecycle_workers(
        mounted_workers
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        &mut failures,
    );
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
        Err(failures.join("; ").into())
    }
}

#[cfg(not(feature = "control"))]
fn run_server(_invocation: &Invocation) -> Result<(), CliFailure> {
    Err("serve requires the nokv control feature".to_owned().into())
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
    fn append_payloads_are_bounded_and_share_explicit_bytes() {
        let text = append_payload(&AppendPayload::Text("event".to_owned()), 5).unwrap();
        let binary = append_payload(&AppendPayload::Base64("ZXZlbnQ=".to_owned()), 5).unwrap();
        assert_eq!(text.0, binary.0);
        assert_eq!(text.1, "text/plain; charset=utf-8");
        assert_eq!(binary.1, "application/octet-stream");
        assert!(append_payload(&AppendPayload::Text("event".to_owned()), 4).is_err());
        assert!(append_payload(&AppendPayload::Base64("!".to_owned()), 5).is_err());
        assert!(append_payload(&AppendPayload::Base64("!".repeat(12)), 5)
            .unwrap_err()
            .contains("encoded limit"));
        assert!(append_payload(&AppendPayload::Base64("ZXZlbnQ=".to_owned()), 4).is_err());
        assert_eq!(
            append_payload(&AppendPayload::Base64("".to_owned()), 0)
                .unwrap()
                .0,
            b""
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delta.bin");
        std::fs::write(&path, b"event").unwrap();
        assert_eq!(
            append_payload(&AppendPayload::File(path), 5).unwrap(),
            binary
        );
    }

    #[test]
    fn append_input_validation_is_typed_with_its_identity() {
        let operation_id = nokv_protocol::OperationIdentity([0xaa; 16]);
        let oversized = append_payload(&AppendPayload::Text("sixsix".to_owned()), 5)
            .map_err(append_input_error)
            .unwrap_err();
        let failure = with_append_details(oversized, operation_id);
        assert_eq!(failure.exit_code(), 2);
        let invalid: Value = serde_json::from_str(&failure.to_line()).unwrap();
        assert_eq!(invalid["code"], "InvalidArgument");
        assert_eq!(invalid["class"], "invalid_input");
        assert_eq!(invalid["details"]["operation_id"], "aa".repeat(16));
        // Repeating an invalid append cannot succeed: change the input.
        assert_eq!(invalid["next_action"], "fix_input");
        assert_eq!(invalid["details"]["next_action"], "fix_input");
    }

    #[test]
    fn append_receipt_keeps_historical_publication_and_error_identity() {
        let call = nokv_client::ClientCall {
            value: nokv_protocol::AppendResult {
                operation_id: nokv_protocol::OperationIdentity([0xaa; 16]),
                publication_operation_id: nokv_protocol::OperationIdentity([0xee; 16]),
                artifact_revision_id: nokv_protocol::ArtifactRevisionIdentity([0xbb; 16]),
                target: canonical_workspace_path("run-42", "logs", "events.jsonl").unwrap(),
                workspace_incarnation_id: nokv_protocol::WorkspaceIdentity([0xdd; 16]),
                workspace_revision: 7,
                generation: 3,
                logical_size: 12,
                body_digest: nokv_protocol::sha256_digest_uri(nokv_protocol::Digest([0xcc; 32])),
            },
            commit_version: None,
            replayed: true,
        };
        let receipt = append_receipt_json(&call);
        assert_eq!(receipt["operation_id"], "aa".repeat(16));
        assert_eq!(receipt["publication_operation_id"], "ee".repeat(16));
        assert_eq!(receipt["artifact_revision_id"], "bb".repeat(16));
        assert_eq!(receipt["generation"], 3);
        assert_eq!(receipt["logical_size"], 12);
        assert_eq!(receipt["workspace_incarnation_id"], "dd".repeat(16));
        assert_eq!(receipt["replayed"], true);
        assert_eq!(receipt["state"], "committed");
        assert_eq!(receipt["next_action"], "none");
        assert!(receipt["commit_version"].is_null());
        let error = nokv_agent::AgentError::backend(
            "AppendUnresolved",
            "reply lost",
            false,
            json!({"state": "Running"}),
        );
        let failure = with_append_details(
            CliFailure::from_agent(&error, IdempotencyScope::CallerOperation),
            call.value.operation_id,
        );
        let result: Value = serde_json::from_str(&failure.to_line()).unwrap();
        assert_eq!(result["details"]["state"], "Running");
        assert_eq!(result["details"]["operation_id"], "aa".repeat(16));
        assert_eq!(result["code"], "AppendUnresolved");
        assert_eq!(result["class"], "outcome_unknown");
        assert_eq!(result["next_action"], "query_same");
    }

    #[test]
    fn an_operation_query_reports_a_lost_reply_as_transient_and_absence_as_resubmittable() {
        let operation_id = nokv_protocol::OperationIdentity([0xaa; 16]);
        let transport = || {
            nokv_client::ClientError::Transport(nokv_client::TransportError::new(
                "connection reset",
                true,
            ))
        };
        let read: Value = serde_json::from_str(
            &operation_failure(operation_read_failure(transport()), operation_id).to_line(),
        )
        .unwrap();
        assert_eq!(read["class"], "transient");
        assert_eq!(read["next_action"], "resubmit_same");
        assert_eq!(read["details"]["next_action"], "resubmit_same");
        // A recovery request is a mutation: its lost reply stays unknown.
        let recover: Value = serde_json::from_str(
            &operation_failure(append_client_failure(transport()), operation_id).to_line(),
        )
        .unwrap();
        assert_eq!(recover["class"], "outcome_unknown");
        assert_eq!(recover["next_action"], "query_same");
        let absent = operation_failure(
            CliFailure::new(
                "NotFound",
                "operation not found",
                ErrorClass::NotFound,
                IdempotencyScope::ReadOnly,
            ),
            operation_id,
        );
        assert_eq!(absent.exit_code(), 3);
        let absent: Value = serde_json::from_str(&absent.to_line()).unwrap();
        assert_eq!(absent["next_action"], "resubmit_same");
        assert_eq!(absent["details"]["operation_id"], "aa".repeat(16));
    }

    #[test]
    fn append_status_does_not_report_pending_or_cleaned_attempts_as_committed() {
        let mut call = nokv_client::ClientCall {
            value: nokv_protocol::OperationStatus {
                initiator: Some(nokv_protocol::PrincipalIdentity([0xa1; 16])),
                token: nokv_protocol::OperationToken {
                    operation_id: nokv_protocol::OperationIdentity([0xaa; 16]),
                    state_digest: nokv_protocol::Digest([0x11; 32]),
                },
                kind: nokv_protocol::OperationKind::ArtifactAppend,
                append_preparation: Some(Box::new(nokv_protocol::AppendPreparation {
                    intent_digest: nokv_protocol::Digest([0x22; 32]),
                    target: canonical_workspace_path("run-42", "logs", "events.jsonl").unwrap(),
                    workspace_incarnation_id: nokv_protocol::WorkspaceIdentity([0xbb; 16]),
                    attempt: 2,
                    publication_operation_id: nokv_protocol::OperationIdentity([0xcc; 16]),
                    artifact_revision_id: nokv_protocol::ArtifactRevisionIdentity([0xdd; 16]),
                    attempt_phase: nokv_protocol::AppendAttemptPhase::Finalizing,
                    activity_deadline_ms: 123_000,
                    attempt_failure: None,
                    cleanup_retry_count: 0,
                })),
                publish_preparation: None,
                commit_preparation: None,
                restore_preparation: None,
                state: nokv_protocol::OperationState::Running,
                progress: nokv_protocol::OperationProgress {
                    completed_rows: 1,
                    total_rows: Some(1),
                    completed_bytes: 4,
                    total_bytes: Some(4),
                },
                result: None,
                failure: None,
            },
            commit_version: None,
            replayed: false,
        };
        let pending = append_status_json(&call).unwrap();
        assert_eq!(pending["state"], "pending");
        assert_eq!(pending["next_action"], "poll");
        assert_eq!(pending["attempt_phase"], "finalizing");
        assert_eq!(pending["attempt"], 2);
        assert_eq!(pending["activity_deadline_ms"], 123_000);
        assert_eq!(pending["operation_id"], "aa".repeat(16));
        assert_eq!(pending["initiator"], "a1".repeat(16));
        assert_eq!(pending["publication_operation_id"], "cc".repeat(16));
        assert!(pending["receipt"].is_null());
        let preparation = call.value.append_preparation.as_mut().unwrap();
        preparation.attempt_phase = nokv_protocol::AppendAttemptPhase::Cleaned;
        preparation.attempt_failure = Some(nokv_protocol::RpcFailure {
            code: nokv_protocol::ErrorCode::OperationFailed,
            message: "append attempt lost its frozen path claim".to_owned(),
            retryable: false,
            conflict: None,
            current_generation: None,
            route_hint: None,
        });
        let cleaned = append_status_json(&call).unwrap();
        assert_eq!(cleaned["state"], "ready_to_retry");
        assert_eq!(cleaned["next_action"], "resubmit_same");
        assert_eq!(cleaned["cause_code"], "OperationFailed");
        assert_eq!(
            cleaned["failure_message"],
            "append attempt lost its frozen path claim"
        );
        assert_eq!(cleaned["attempt_failure"]["code"], "OperationFailed");
        assert!(cleaned["receipt"].is_null());
        call.value.state = nokv_protocol::OperationState::Quarantined;
        call.value
            .append_preparation
            .as_mut()
            .unwrap()
            .attempt_phase = nokv_protocol::AppendAttemptPhase::Quarantined;
        let quarantined = append_status_json(&call).unwrap();
        assert_eq!(quarantined["next_action"], "retry_cleanup");
        assert!(quarantined["receipt"].is_null());

        let inspected = append_inspection_json(&nokv_client::ClientCall {
            value: nokv_client::AppendOperationInspection {
                inspection: nokv_protocol::AppendCleanupInspection {
                    operation: Box::new(call.value.clone()),
                    object_namespace_id: nokv_protocol::ObjectNamespaceIdentity([0xee; 16]),
                    publication_token: nokv_protocol::OperationToken {
                        operation_id: nokv_protocol::OperationIdentity([0xcc; 16]),
                        state_digest: nokv_protocol::Digest([0x33; 32]),
                    },
                    registered_count: 3,
                    cleanup_cursor: 1,
                    remaining_count: 2,
                    entries: vec![nokv_protocol::StagedObject {
                        sequence: 1,
                        object_identity: nokv_protocol::ObjectIdentity::new("objects/retained-1")
                            .unwrap(),
                        expected_length: 4,
                        expected_digest: nokv_protocol::sha256_digest_uri(nokv_protocol::Digest(
                            [0x44; 32],
                        )),
                    }],
                    next_after: Some(1),
                },
                next_cursor: Some(vec![1, 2, 3]),
            },
            commit_version: None,
            replayed: false,
        })
        .unwrap();
        assert_eq!(
            inspected["operation_token"]["state_digest"],
            "11".repeat(32)
        );
        assert_eq!(
            inspected["publication_token"]["operation_id"],
            "cc".repeat(16)
        );
        assert_eq!(inspected["remaining_count"], 2);
        assert_eq!(inspected["cleanup_cursor"], 1);
        assert_eq!(inspected["entries"][0]["expected_length"], 4);
        assert_eq!(inspected["next_cursor"], "AQID");

        // A replayed cleanup receipt can be older than the current operation.
        call.value
            .append_preparation
            .as_mut()
            .unwrap()
            .cleanup_retry_count = 2;
        let recovery = append_recovery_json(&nokv_client::ClientCall {
            value: nokv_client::AppendRecoveryRequestResult {
                operation: call.value.clone(),
                requested: true,
                receipt: Some(nokv_protocol::AppendCleanupRetryResult {
                    operation_id: call.value.token.operation_id,
                    publication_operation_id: nokv_protocol::OperationIdentity([0xcc; 16]),
                    cleanup_retry_count: 1,
                    expected_state_digest: nokv_protocol::Digest([0x55; 32]),
                }),
            },
            commit_version: None,
            replayed: true,
        })
        .unwrap();
        assert_eq!(recovery["cleanup_retry_count"], 2);
        assert_eq!(recovery["recovery_receipt"]["cleanup_retry_count"], 1);
        assert_eq!(
            recovery["recovery_receipt"]["expected_state_digest"],
            "55".repeat(32)
        );
        assert_eq!(recovery["requested"], true);
        assert_eq!(recovery["replayed"], true);
        assert!(recovery["receipt"].is_null());
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

        // The configured --agent-id names another deployment.
        assert_eq!(error.class(), ErrorClass::InvalidInput);
        let error = error.to_line();
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

        assert_eq!(error.class(), ErrorClass::OperatorRequired);
        let error = error.to_line();
        assert!(error.contains("no durable Agent binding"));
        assert!(error.contains("--adopt-legacy-agent-binding"));
        assert!(!rpc_preflight_called.get());
    }

    #[test]
    fn help_text_no_longer_documents_an_mcp_command() {
        assert!(!HELP_TEXT.to_ascii_lowercase().contains("mcp"));
    }

    #[cfg(feature = "control")]
    #[test]
    fn mount_checks_admit_only_roots_this_owner_may_serve() {
        use nokv_control::{
            ControlStore, IdentityStore, InMemoryControlStore, PlacementGeneration, RootPlacement,
            RootPlacementLifecycle, RootTenantBinding, TenantRecord,
        };

        let store = Arc::new(InMemoryControlStore::new());
        let shard = nokv_control::LogicalShardId::from_bytes([2; 16]);
        let namespace = nokv_types::ObjectNamespaceId::from_bytes([8; 16]);
        let other_namespace = nokv_types::ObjectNamespaceId::from_bytes([9; 16]);
        let tenant = nokv_types::TenantId::from_bytes([5; 16]);
        let other_tenant = nokv_types::TenantId::from_bytes([6; 16]);
        store.create_logical_shard(shard).unwrap();
        for tenant_id in [tenant, other_tenant] {
            store.create_tenant(TenantRecord { tenant_id }).unwrap();
        }
        let root = |fill: u8| nokv_control::RootId::from_bytes([fill; 16]);
        let placement = |fill: u8| -> RootPlacement {
            let provisioning = store
                .create_root_placement(RootPlacement {
                    root_id: root(fill),
                    logical_shard_id: shard,
                    placement_generation: PlacementGeneration::new(1).unwrap(),
                    lifecycle: RootPlacementLifecycle::Provisioning,
                })
                .unwrap();
            let active = RootPlacement {
                placement_generation: PlacementGeneration::new(2).unwrap(),
                lifecycle: RootPlacementLifecycle::Active,
                ..provisioning.clone()
            };
            store
                .compare_and_set_root_placement(&provisioning, active)
                .unwrap()
        };
        let bind_namespace = |fill: u8, object_namespace_id| {
            store
                .create_root_object_namespace_binding(nokv_control::RootObjectNamespaceBinding {
                    root_id: root(fill),
                    object_namespace_id,
                })
                .unwrap();
        };
        let bind_agent = |fill: u8| {
            store
                .create_root_agent_binding(nokv_control::RootAgentBinding {
                    root_id: root(fill),
                    agent_id: nokv_types::AgentId::from_bytes([4; 16]),
                })
                .unwrap();
        };
        let bind_tenant = |fill: u8, tenant_id| {
            store
                .bind_root_tenant(RootTenantBinding {
                    root_id: root(fill),
                    tenant_id,
                })
                .unwrap();
        };
        // 1: a plain agent root; 2: no namespace binding; 3: another
        // namespace; 4: no agent binding; 5: bound to the served tenant;
        // 6: bound to another tenant; 7: tenant-bound root of a plaintext owner.
        bind_namespace(1, namespace);
        bind_agent(1);
        bind_namespace(3, other_namespace);
        bind_agent(3);
        bind_namespace(4, namespace);
        bind_namespace(5, namespace);
        bind_agent(5);
        bind_tenant(5, tenant);
        bind_namespace(6, namespace);
        bind_agent(6);
        bind_tenant(6, other_tenant);
        bind_namespace(7, namespace);
        bind_agent(7);
        bind_tenant(7, tenant);
        let placements: Vec<RootPlacement> = (1..=7).map(placement).collect();

        let plaintext = RootMountChecks {
            control: store.clone(),
            identity: store.clone(),
            expected_namespace: namespace,
            served_tenant: None,
        };
        let secured = RootMountChecks {
            served_tenant: Some(tenant),
            control: store.clone(),
            identity: store.clone(),
            expected_namespace: namespace,
        };
        let verdict = |checks: &RootMountChecks, index: usize| {
            checks.refusal(&placements[index - 1]).unwrap()
        };
        assert_eq!(verdict(&plaintext, 1), None);
        assert!(verdict(&plaintext, 2)
            .unwrap()
            .contains("no object namespace binding"));
        assert!(verdict(&plaintext, 3)
            .unwrap()
            .contains("another object namespace"));
        assert!(
            verdict(&plaintext, 4).is_some(),
            "a plaintext owner needs the AgentId binding"
        );
        assert!(verdict(&plaintext, 7)
            .unwrap()
            .contains("served only over RPC TLS"));
        assert_eq!(verdict(&secured, 5), None);
        assert!(verdict(&secured, 6)
            .unwrap()
            .contains("this owner serves tenant"));
        assert!(verdict(&secured, 1)
            .unwrap()
            .contains("not bound to a tenant"));
        assert!(verdict(&secured, 3)
            .unwrap()
            .contains("another object namespace"));
    }

    #[test]
    fn static_agent_route_fails_before_rpc_configuration() {
        let invocation = cli::parse(
            [
                "--root-id",
                "11111111111111111111111111111111",
                "--agent-id",
                "77777777777777777777777777777777",
                "--logical-shard-id",
                "not-a-valid-shard-id",
                "workbench-session",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();

        assert_eq!(invocation.command, Command::WorkbenchSession);

        let error = configured_agent_admission(&invocation).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        let error = error.to_line();
        assert!(error.contains("durable control routing"));
        assert!(!error.contains("logical-shard-id"));
    }

    #[cfg(feature = "control")]
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

    #[test]
    fn a_plaintext_owner_refuses_a_root_that_a_tenant_owns() {
        use nokv_control::{
            ControlStore, IdentityStore, InMemoryControlStore, RootTenantBinding, TenantRecord,
        };
        use nokv_types::{LogicalShardId, ObjectNamespaceId, RootId, TenantId};

        let root = |fill| RootId::from_bytes([fill; nokv_types::FIXED_ID_BYTES]);
        let shard = |fill| LogicalShardId::from_bytes([fill; nokv_types::FIXED_ID_BYTES]);
        let control = InMemoryControlStore::new();
        let namespace = ObjectNamespaceId::from_bytes([3; nokv_types::FIXED_ID_BYTES]);
        provision::provision_and_activate(&control, root(1), shard(9), namespace).unwrap();
        provision::provision_and_activate(&control, root(2), shard(9), namespace).unwrap();
        let seed = control.get_root_placement(&root(1)).unwrap().unwrap();
        let placements = active_shard_placements(&control, &seed).unwrap();
        refuse_plaintext_for_tenant_roots(&control, &placements).unwrap();

        let tenant_id = TenantId::from_bytes([5; nokv_types::FIXED_ID_BYTES]);
        control.create_tenant(TenantRecord { tenant_id }).unwrap();
        control
            .bind_root_tenant(RootTenantBinding {
                root_id: root(2),
                tenant_id,
            })
            .unwrap();
        let error = refuse_plaintext_for_tenant_roots(&control, &placements).unwrap_err();
        assert!(
            error.contains(&encode_lowercase_hex(root(2).as_bytes())),
            "{error}"
        );
        assert!(error.contains("--rpc-tls-cert"), "{error}");
    }
}
