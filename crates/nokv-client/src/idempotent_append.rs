// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! Caller-owned logical appends with durable, predecessor-fenced publication attempts.

use std::{thread, time::Duration};

use nokv_object::{ArtifactObjectStore, DEFAULT_ARTIFACT_BLOCK_SIZE};
use nokv_protocol::{
    stable_append_attempt_identities, AppendAttemptBinding, AppendAttemptPhase, AppendResult,
    ConflictKind, ContentType, Digest, ErrorCode, GetOperationRequest, GetWorkspaceRequest,
    OperationIdentity, OperationKind, OperationResult, OperationState, OperationStatus,
    RootIdentity, RpcFailure, WorkspaceIdentity, WorkspacePath,
};
use sha2::{Digest as _, Sha256};

use crate::{
    ArtifactAppendOptions, ClientCall, ClientError, RouteResolver, RpcTransport, WorkspaceClient,
};

/// Maximum delta accepted by the bounded append product.
pub const MAX_APPEND_DELTA_BYTES: usize = 16 * 1024 * 1024;
/// Default bound for the complete resulting artifact, including rematerialization.
pub const DEFAULT_APPEND_MAX_LOGICAL_SIZE: u64 = 16 * 1024 * 1024;

const APPEND_CLEANUP_POLLS: usize = 12;
const APPEND_CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// An append intent whose identity and payload the caller persists before sending.
///
/// A logical operation may use another immutable publication attempt only after
/// the previous one is durably cleaned. Concurrent or uncertain attempts cannot
/// be replaced. Scheduling budgets do not change the caller's logical identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotentAppendOptions {
    pub operation_id: OperationIdentity,
    pub target: WorkspacePath,
    pub expected_workspace_incarnation_id: WorkspaceIdentity,
    pub create_content_type: ContentType,
    pub content_type: Option<ContentType>,
    pub block_size: usize,
    pub max_logical_size: Option<u64>,
}

impl IdempotentAppendOptions {
    pub fn new(
        operation_id: OperationIdentity,
        target: WorkspacePath,
        expected_workspace_incarnation_id: WorkspaceIdentity,
        create_content_type: ContentType,
    ) -> Self {
        Self {
            operation_id,
            target,
            expected_workspace_incarnation_id,
            create_content_type,
            content_type: None,
            block_size: DEFAULT_ARTIFACT_BLOCK_SIZE,
            max_logical_size: Some(DEFAULT_APPEND_MAX_LOGICAL_SIZE),
        }
    }

    pub fn with_content_type(mut self, content_type: ContentType) -> Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn with_max_logical_size(mut self, max_logical_size: u64) -> Self {
        self.max_logical_size = Some(max_logical_size);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendRecoveryState {
    Committed,
    Pending,
    ReadyToRetry,
    Quarantined,
}

impl AppendRecoveryState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Pending => "pending",
            Self::ReadyToRetry => "ready_to_retry",
            Self::Quarantined => "quarantined",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendNextAction {
    None,
    Poll,
    ResubmitSame,
    OperatorReconcile,
}

impl AppendNextAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Poll => "poll",
            Self::ResubmitSame => "resubmit_same",
            Self::OperatorReconcile => "operator_reconcile",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendRecovery {
    pub state: AppendRecoveryState,
    pub next_action: AppendNextAction,
}

/// Classify a durable append observation without performing a mutation.
///
/// A failed physical attempt is not a failed logical append. Only Cleaned
/// authorizes resubmission under the same logical identity. Unknown transport
/// outcomes are errors, never synthesized observations of absence.
pub fn append_operation_recovery(status: &OperationStatus) -> Result<AppendRecovery, ClientError> {
    if status.kind != OperationKind::ArtifactAppend {
        return Err(append_failure(
            ErrorCode::RequestReplayMismatch,
            "operation identity belongs to another lifecycle",
        ));
    }
    let preparation = status.append_preparation.as_ref().ok_or_else(|| {
        ClientError::ResponseMismatch("logical append has no durable preparation".to_owned())
    })?;
    if status.state == OperationState::Succeeded {
        if preparation.attempt_phase != AppendAttemptPhase::Published
            || !matches!(status.result, Some(OperationResult::ArtifactAppend(_)))
        {
            return Err(ClientError::ResponseMismatch(
                "committed append has no published attempt and receipt".to_owned(),
            ));
        }
        return Ok(AppendRecovery {
            state: AppendRecoveryState::Committed,
            next_action: AppendNextAction::None,
        });
    }
    if status.result.is_some() {
        return Err(ClientError::ResponseMismatch(
            "unfinished append cannot contain a committed receipt".to_owned(),
        ));
    }
    Ok(match preparation.attempt_phase {
        AppendAttemptPhase::Cleaned => AppendRecovery {
            state: AppendRecoveryState::ReadyToRetry,
            next_action: AppendNextAction::ResubmitSame,
        },
        AppendAttemptPhase::Quarantined => AppendRecovery {
            state: AppendRecoveryState::Quarantined,
            next_action: AppendNextAction::OperatorReconcile,
        },
        AppendAttemptPhase::Uploading
        | AppendAttemptPhase::Finalizing
        | AppendAttemptPhase::Aborting
        | AppendAttemptPhase::Cleaning => AppendRecovery {
            state: AppendRecoveryState::Pending,
            next_action: AppendNextAction::Poll,
        },
        AppendAttemptPhase::Published => {
            return Err(ClientError::ResponseMismatch(
                "published attempt has no atomic logical append receipt".to_owned(),
            ));
        }
    })
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// Query one logical append without its delta, current workspace or object provider.
    pub fn get_append_operation(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        let status = self.get_operation(GetOperationRequest { operation_id })?;
        if status.value.token.operation_id != operation_id {
            return Err(ClientError::ResponseMismatch(
                "append status returned a different logical identity".to_owned(),
            ));
        }
        append_operation_recovery(&status.value)?;
        Ok(status)
    }

    /// Resolve the original workspace before considering its current name binding.
    ///
    /// Explicit fences always take precedence. Only an operation with no
    /// observed durable admission obtains its incarnation from the live name.
    pub fn resolve_append_workspace_incarnation(
        &self,
        operation_id: OperationIdentity,
        target: &WorkspacePath,
        expected: Option<WorkspaceIdentity>,
    ) -> Result<WorkspaceIdentity, ClientError> {
        let incarnation = match self.get_append_operation(operation_id) {
            Ok(status) => {
                let preparation = status
                    .value
                    .append_preparation
                    .expect("get_append_operation validates its preparation");
                if preparation.target != *target {
                    return Err(append_unresolved(
                        operation_id,
                        Some(status.value.state),
                        append_failure(
                            ErrorCode::RequestReplayMismatch,
                            "append identity is already bound to another target",
                        ),
                    ));
                }
                preparation.workspace_incarnation_id
            }
            Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => {
                self.get_workspace(GetWorkspaceRequest {
                    workbench: target.workbench.clone(),
                })?
                .value
                .workspace_incarnation_id
            }
            Err(error) => return Err(append_unresolved(operation_id, None, error)),
        };
        if expected.is_some_and(|value| value != incarnation) {
            return Err(append_unresolved(
                operation_id,
                None,
                ClientError::Rpc(RpcFailure {
                    code: ErrorCode::Conflict,
                    message: "append workspace incarnation differs from the explicit fence"
                        .to_owned(),
                    retryable: false,
                    conflict: Some(ConflictKind::WorkspaceIncarnation),
                    current_generation: None,
                    route_hint: None,
                }),
            ));
        }
        Ok(incarnation)
    }

    /// Recover a receipt before constructing an object provider.
    ///
    /// Normalizes and authenticates the caller intent using metadata only.
    /// `None` means the same intent may be submitted: it is either absent or
    /// its predecessor is durably cleaned. That observation is not admission;
    /// `append_artifact_idempotent` still rechecks and atomically fences it.
    /// Pending and quarantined actions return a queryable logical error.
    pub fn recover_append_receipt(
        &self,
        options: &mut IdempotentAppendOptions,
        delta: &[u8],
    ) -> Result<Option<ClientCall<AppendResult>>, ClientError> {
        validate_append_inputs(options, delta)?;
        let intent = append_intent_digest(self.root_id(), options, delta)?;
        let Some(status) = self.observe_append(options)? else {
            return Ok(None);
        };
        validate_append_intent(self.root_id(), &status.value, options, intent)?;
        match append_operation_recovery(&status.value)?.state {
            AppendRecoveryState::Committed => append_receipt(status, options).map(Some),
            AppendRecoveryState::ReadyToRetry => Ok(None),
            AppendRecoveryState::Pending | AppendRecoveryState::Quarantined => {
                Err(pending_append(options, &status.value))
            }
        }
    }

    /// Apply the caller's logical append at most once, recovering safe attempts.
    ///
    /// Committed results replay without accessing Live or the object provider.
    /// Resumption requires the same delta and normalized options. The server
    /// admits a successor and advances its parent atomically only after cleanup
    /// proves that the previous attempt cannot publish. Bounded local retries
    /// return a queryable pending operation instead of allocating a new action.
    pub fn append_artifact_idempotent(
        &self,
        store: &dyn ArtifactObjectStore,
        mut options: IdempotentAppendOptions,
        delta: &[u8],
    ) -> Result<ClientCall<AppendResult>, ClientError> {
        validate_append_inputs(&mut options, delta)?;
        let intent = append_intent_digest(self.root_id(), &options, delta)?;
        let mut observed = self.observe_append(&options)?;
        let mut submissions = 0;

        loop {
            let attempt = match observed.as_ref() {
                None => 0,
                Some(status) => {
                    validate_append_intent(self.root_id(), &status.value, &options, intent)?;
                    match append_operation_recovery(&status.value)?.state {
                        AppendRecoveryState::Committed => {
                            return append_receipt(
                                observed.take().expect("observed committed append"),
                                &options,
                            );
                        }
                        AppendRecoveryState::ReadyToRetry => status
                            .value
                            .append_preparation
                            .as_ref()
                            .expect("validated preparation")
                            .attempt
                            .checked_add(1)
                            .ok_or_else(|| {
                                unresolved(
                                    &options,
                                    Some(status.value.state),
                                    ClientError::InvalidOptions(
                                        "append attempt counter exhausted".to_owned(),
                                    ),
                                )
                            })?,
                        _ => return Err(pending_append(&options, &status.value)),
                    }
                }
            };
            if submissions >= self.max_attempts() {
                return Err(unresolved(
                    &options,
                    observed.as_ref().map(|call| call.value.state),
                    append_failure(ErrorCode::Conflict,
                        "append retry budget exhausted; query or resubmit the same logical identity"),
                ));
            }
            submissions += 1;
            let (publication_id, revision_id) =
                stable_append_attempt_identities(self.root_id(), options.operation_id, attempt);
            let mut publication = ArtifactAppendOptions::new(
                publication_id,
                revision_id,
                options.target.clone(),
                options.create_content_type.clone(),
            )
            .with_block_size(options.block_size);
            publication.content_type = options.content_type.clone();
            publication.max_logical_size = options.max_logical_size;
            let outcome = self.append_artifact_attempt(
                store,
                &publication,
                publication_id,
                revision_id,
                delta,
                Some(options.expected_workspace_incarnation_id),
                Some(intent),
                Some(AppendAttemptBinding {
                    operation_id: options.operation_id,
                    attempt,
                }),
            );
            match outcome {
                Ok(outcome) => {
                    let call = outcome.publication;
                    let receipt = call.value;
                    if receipt.operation_id != publication_id
                        || receipt.artifact_revision_id != revision_id
                        || receipt.target != options.target
                    {
                        return Err(unresolved(
                            &options,
                            None,
                            ClientError::ResponseMismatch(
                                "append publication differs from admitted attempt".to_owned(),
                            ),
                        ));
                    }
                    return Ok(ClientCall {
                        value: AppendResult {
                            operation_id: options.operation_id,
                            publication_operation_id: publication_id,
                            target: receipt.target,
                            workspace_incarnation_id: options.expected_workspace_incarnation_id,
                            workspace_revision: receipt.workspace_revision,
                            generation: receipt.generation,
                            artifact_revision_id: receipt.artifact_revision_id,
                            logical_size: receipt.logical_size,
                            body_digest: receipt.body_digest,
                        },
                        commit_version: call.commit_version,
                        replayed: call.replayed,
                    });
                }
                Err(source) => {
                    // Resolve response-loss ambiguity before classifying the
                    // failed call. Cleanup polling is bounded independently of
                    // the number of new durable attempts this call may admit.
                    observed = self.observe_append(&options)?;
                    if observed.is_none() {
                        if source.rpc_code().is_some_and(|code| {
                            matches!(code, ErrorCode::Conflict | ErrorCode::PreconditionFailed)
                        }) && submissions < self.max_attempts()
                        {
                            continue;
                        }
                        return Err(unresolved(&options, None, source));
                    }
                    for _ in 0..APPEND_CLEANUP_POLLS {
                        let status = &observed.as_ref().expect("observed append").value;
                        validate_append_intent(self.root_id(), status, &options, intent)?;
                        if append_operation_recovery(status)?.state != AppendRecoveryState::Pending
                        {
                            break;
                        }
                        thread::sleep(APPEND_CLEANUP_POLL_INTERVAL);
                        observed = self.observe_append(&options)?;
                        if observed.is_none() {
                            return Err(unresolved(
                                &options,
                                None,
                                ClientError::ResponseMismatch(
                                    "durable append identity disappeared".to_owned(),
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }

    fn observe_append(
        &self,
        options: &IdempotentAppendOptions,
    ) -> Result<Option<ClientCall<OperationStatus>>, ClientError> {
        match self.get_append_operation(options.operation_id) {
            Ok(status) => Ok(Some(status)),
            Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(unresolved(options, None, error)),
        }
    }
}

fn validate_append_inputs(
    options: &mut IdempotentAppendOptions,
    delta: &[u8],
) -> Result<(), ClientError> {
    if delta.len() > MAX_APPEND_DELTA_BYTES {
        return Err(ClientError::InvalidOptions(format!(
            "append delta exceeds the {MAX_APPEND_DELTA_BYTES}-byte product limit"
        )));
    }
    if options.block_size == 0 {
        return Err(ClientError::InvalidOptions(
            "append block size must be positive".to_owned(),
        ));
    }
    let maximum = options
        .max_logical_size
        .get_or_insert(DEFAULT_APPEND_MAX_LOGICAL_SIZE);
    if delta.len() as u64 > *maximum {
        return Err(ClientError::InvalidOptions(
            "append delta exceeds the resulting artifact size limit".to_owned(),
        ));
    }
    Ok(())
}

fn validate_append_intent(
    root: RootIdentity,
    status: &OperationStatus,
    options: &IdempotentAppendOptions,
    intent: Digest,
) -> Result<(), ClientError> {
    let matches = status.token.operation_id == options.operation_id
        && status.kind == OperationKind::ArtifactAppend
        && status
            .append_preparation
            .as_ref()
            .is_some_and(|preparation| {
                let (publication_id, revision_id) = stable_append_attempt_identities(
                    root,
                    options.operation_id,
                    preparation.attempt,
                );
                preparation.intent_digest == intent
                    && preparation.target == options.target
                    && preparation.workspace_incarnation_id
                        == options.expected_workspace_incarnation_id
                    && preparation.publication_operation_id == publication_id
                    && preparation.artifact_revision_id == revision_id
            });
    if !matches {
        return Err(unresolved(
            options,
            None,
            append_failure(
                ErrorCode::RequestReplayMismatch,
                "append logical identity is already bound to a different intent or attempt",
            ),
        ));
    }
    Ok(())
}

fn append_receipt(
    status: ClientCall<OperationStatus>,
    options: &IdempotentAppendOptions,
) -> Result<ClientCall<AppendResult>, ClientError> {
    let Some(OperationResult::ArtifactAppend(result)) = status.value.result else {
        return Err(unresolved(
            options,
            Some(status.value.state),
            ClientError::ResponseMismatch("committed append has no logical receipt".to_owned()),
        ));
    };
    let preparation = status
        .value
        .append_preparation
        .expect("committed status has validated preparation");
    if result.operation_id != options.operation_id
        || result.publication_operation_id != preparation.publication_operation_id
        || result.artifact_revision_id != preparation.artifact_revision_id
        || result.workspace_incarnation_id != options.expected_workspace_incarnation_id
        || result.target != options.target
    {
        return Err(unresolved(
            options,
            Some(status.value.state),
            ClientError::ResponseMismatch(
                "append receipt differs from its durable preparation".to_owned(),
            ),
        ));
    }
    Ok(ClientCall {
        value: result,
        commit_version: status.commit_version,
        replayed: true,
    })
}

fn pending_append(options: &IdempotentAppendOptions, status: &OperationStatus) -> ClientError {
    let recovery = append_operation_recovery(status);
    let message = match recovery {
        Ok(recovery) => format!(
            "append is {}; next action: {}; retain the same logical identity",
            recovery.state.as_str(),
            recovery.next_action.as_str()
        ),
        Err(error) => return unresolved(options, Some(status.state), error),
    };
    unresolved(
        options,
        Some(status.state),
        append_failure(
            if status.state == OperationState::Quarantined {
                ErrorCode::OperationFailed
            } else {
                ErrorCode::Conflict
            },
            message,
        ),
    )
}

fn unresolved(
    options: &IdempotentAppendOptions,
    state: Option<OperationState>,
    source: ClientError,
) -> ClientError {
    append_unresolved(options.operation_id, state, source)
}

fn append_unresolved(
    operation_id: OperationIdentity,
    state: Option<OperationState>,
    source: ClientError,
) -> ClientError {
    ClientError::AppendUnresolved {
        operation_id,
        state,
        source: Box::new(source),
    }
}

fn append_failure(code: ErrorCode, message: impl Into<String>) -> ClientError {
    ClientError::Rpc(RpcFailure {
        code,
        message: message.into(),
        retryable: false,
        conflict: Some(ConflictKind::OperationState),
        current_generation: None,
        route_hint: None,
    })
}

fn append_intent_digest(
    root: RootIdentity,
    options: &IdempotentAppendOptions,
    delta: &[u8],
) -> Result<Digest, ClientError> {
    let block_size = u64::try_from(options.block_size)
        .map_err(|_| ClientError::InvalidOptions("append block size exceeds u64".to_owned()))?;
    let delta_len = u64::try_from(delta.len())
        .map_err(|_| ClientError::InvalidOptions("append delta length exceeds u64".to_owned()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.append.intent.v2\0");
    hasher.update(root.0);
    hasher.update(options.operation_id.0);
    hasher.update(options.expected_workspace_incarnation_id.0);
    for field in [
        options.target.workbench.as_str(),
        options.target.path.as_str(),
        options.create_content_type.as_str(),
    ] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    match &options.content_type {
        Some(content_type) => {
            hasher.update([1]);
            hasher.update((content_type.as_str().len() as u64).to_be_bytes());
            hasher.update(content_type.as_str().as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(block_size.to_be_bytes());
    match options.max_logical_size {
        Some(maximum) => {
            hasher.update([1]);
            hasher.update(maximum.to_be_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(delta_len.to_be_bytes());
    hasher.update(Sha256::digest(delta));
    Ok(Digest(hasher.finalize().into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientOptions, StaticRouteResolver, TransportError};
    use nokv_object::MemoryArtifactStore;
    use nokv_protocol::{
        decode_request, encode_response, AppendPreparation, LogicalShardIdentity,
        ObjectNamespaceIdentity, OperationProgress, OperationToken, RelativePath, RootRoute,
        WorkbenchName, WorkspaceRequest, WorkspaceResult, WorkspaceRpcOutcome,
        WorkspaceRpcResponse,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn options() -> IdempotentAppendOptions {
        IdempotentAppendOptions::new(
            OperationIdentity([1; 16]),
            WorkspacePath {
                workbench: WorkbenchName::new("run-42").unwrap(),
                path: RelativePath::new("logs/events.jsonl").unwrap(),
            },
            WorkspaceIdentity([2; 16]),
            ContentType::new("text/plain").unwrap(),
        )
        .with_block_size(1024)
    }

    fn committed_status(options: &IdempotentAppendOptions) -> OperationStatus {
        let (publication_operation_id, artifact_revision_id) =
            stable_append_attempt_identities(RootIdentity([3; 16]), options.operation_id, 4);
        OperationStatus {
            token: OperationToken {
                operation_id: options.operation_id,
                state_digest: Digest([9; 32]),
            },
            kind: OperationKind::ArtifactAppend,
            append_preparation: Some(Box::new(AppendPreparation {
                intent_digest: append_intent_digest(RootIdentity([3; 16]), options, b"event\n")
                    .unwrap(),
                target: options.target.clone(),
                workspace_incarnation_id: options.expected_workspace_incarnation_id,
                attempt: 4,
                publication_operation_id,
                artifact_revision_id,
                attempt_phase: AppendAttemptPhase::Published,
                attempt_failure: None,
                activity_deadline_ms: 1234,
            })),
            publish_preparation: None,
            commit_preparation: None,
            restore_preparation: None,
            state: OperationState::Succeeded,
            progress: OperationProgress {
                completed_rows: 1,
                total_rows: Some(1),
                completed_bytes: 100,
                total_bytes: Some(100),
            },
            result: Some(OperationResult::ArtifactAppend(AppendResult {
                operation_id: options.operation_id,
                publication_operation_id,
                target: options.target.clone(),
                workspace_incarnation_id: options.expected_workspace_incarnation_id,
                workspace_revision: 7,
                generation: 4,
                artifact_revision_id,
                logical_size: 100,
                body_digest: nokv_protocol::sha256_digest_uri(Digest([4; 32])),
            })),
            failure: None,
        }
    }

    struct StatusOnlyTransport {
        status: OperationStatus,
        calls: Arc<AtomicUsize>,
    }

    impl RpcTransport for StatusOnlyTransport {
        fn round_trip(
            &self,
            _endpoint: std::net::SocketAddr,
            bytes: &[u8],
        ) -> Result<Vec<u8>, TransportError> {
            let request = decode_request(bytes).unwrap();
            // A live-name lookup, metadata read or publication would be an
            // incorrect side effect for a historical receipt or pending query.
            assert!(matches!(
                request.operation,
                WorkspaceRequest::GetOperation(_)
            ));
            self.calls.fetch_add(1, Ordering::SeqCst);
            encode_response(&WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version: None,
                replayed: false,
                outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Operation(
                    self.status.clone(),
                ))),
            })
            .map_err(|error| TransportError::new(error.to_string(), false))
        }
    }

    fn client(
        status: OperationStatus,
    ) -> (
        WorkspaceClient<StatusOnlyTransport, StaticRouteResolver>,
        Arc<AtomicUsize>,
    ) {
        let root_id = RootIdentity([3; 16]);
        let calls = Arc::new(AtomicUsize::new(0));
        let route = RootRoute {
            root_id,
            logical_shard_id: LogicalShardIdentity([4; 16]),
            object_namespace_id: ObjectNamespaceIdentity([5; 16]),
            placement_generation: 1,
            owner_epoch: 1,
        };
        (
            WorkspaceClient::new(
                root_id,
                StatusOnlyTransport {
                    status,
                    calls: calls.clone(),
                },
                StaticRouteResolver::new(route, ([127, 0, 0, 1], 4100).into()).unwrap(),
                ClientOptions::default(),
            )
            .unwrap(),
            calls,
        )
    }

    #[test]
    fn durable_intent_has_frozen_v2_encoding() {
        let actual = append_intent_digest(RootIdentity([3; 16]), &options(), b"event\n").unwrap();
        assert_eq!(
            actual,
            Digest([
                16, 47, 57, 28, 213, 16, 82, 161, 242, 238, 209, 106, 39, 5, 195, 215, 97, 168,
                107, 135, 101, 59, 63, 149, 226, 65, 176, 208, 127, 126, 63, 51
            ])
        );
        let mut implicit = options();
        implicit.max_logical_size = None;
        validate_append_inputs(&mut implicit, b"event\n").unwrap();
        assert_eq!(
            actual,
            append_intent_digest(RootIdentity([3; 16]), &implicit, b"event\n").unwrap()
        );
    }

    #[test]
    fn committed_replay_and_incarnation_resolution_need_only_operation_metadata() {
        let options = options();
        let status = committed_status(&options);
        let expected = status.result.clone();
        let (client, calls) = client(status);
        assert_eq!(
            client
                .resolve_append_workspace_incarnation(options.operation_id, &options.target, None)
                .unwrap(),
            options.expected_workspace_incarnation_id
        );
        let receipt = client
            .append_artifact_idempotent(&MemoryArtifactStore::new(), options, b"event\n")
            .unwrap();
        assert!(receipt.replayed);
        assert_eq!(
            Some(OperationResult::ArtifactAppend(receipt.value)),
            expected
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn receipt_preflight_needs_no_provider_and_normalizes_limits() {
        let mut options = options();
        let status = committed_status(&options);
        let expected = status.result.clone();
        let (client, calls) = client(status);
        options.max_logical_size = None;
        let receipt = client
            .recover_append_receipt(&mut options, b"event\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            Some(OperationResult::ArtifactAppend(receipt.value)),
            expected
        );
        assert_eq!(
            options.max_logical_size,
            Some(DEFAULT_APPEND_MAX_LOGICAL_SIZE)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn only_cleaned_is_a_safe_successor_boundary() {
        let options = options();
        for phase in [
            AppendAttemptPhase::Uploading,
            AppendAttemptPhase::Finalizing,
            AppendAttemptPhase::Aborting,
            AppendAttemptPhase::Cleaning,
            AppendAttemptPhase::Cleaned,
            AppendAttemptPhase::Quarantined,
        ] {
            let mut status = committed_status(&options);
            status.state = if phase == AppendAttemptPhase::Quarantined {
                OperationState::Quarantined
            } else {
                OperationState::Running
            };
            status.result = None;
            let preparation = status.append_preparation.as_mut().unwrap();
            preparation.attempt_phase = phase;
            if matches!(
                phase,
                AppendAttemptPhase::Cleaned | AppendAttemptPhase::Quarantined
            ) {
                preparation.attempt_failure = Some(RpcFailure {
                    code: if phase == AppendAttemptPhase::Quarantined {
                        ErrorCode::Quarantined
                    } else {
                        ErrorCode::OperationFailed
                    },
                    message: "previous publication attempt was safely stopped".to_owned(),
                    retryable: false,
                    conflict: Some(ConflictKind::OperationState),
                    current_generation: None,
                    route_hint: None,
                });
            }
            let recovery = append_operation_recovery(&status).unwrap();
            assert_eq!(
                recovery.next_action == AppendNextAction::ResubmitSame,
                phase == AppendAttemptPhase::Cleaned
            );
            if phase != AppendAttemptPhase::Cleaned {
                let (client, calls) = client(status);
                let error = client
                    .append_artifact_idempotent(
                        &MemoryArtifactStore::new(),
                        options.clone(),
                        b"event\n",
                    )
                    .unwrap_err();
                assert!(
                    matches!(error, ClientError::AppendUnresolved {operation_id, ..} if operation_id == options.operation_id)
                );
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[test]
    fn inconsistent_parent_and_child_success_is_never_a_receipt() {
        let mut status = committed_status(&options());
        status.append_preparation.as_mut().unwrap().attempt_phase = AppendAttemptPhase::Finalizing;
        assert!(matches!(
            append_operation_recovery(&status),
            Err(ClientError::ResponseMismatch(_))
        ));
        status.append_preparation.as_mut().unwrap().attempt_phase = AppendAttemptPhase::Published;
        status.state = OperationState::Running;
        status.result = None;
        assert!(matches!(
            append_operation_recovery(&status),
            Err(ClientError::ResponseMismatch(_))
        ));
    }

    #[test]
    fn every_bound_input_and_child_identity_is_authenticated() {
        let original = options();
        let status = committed_status(&original);
        let root = RootIdentity([3; 16]);
        let mut variants = Vec::new();
        let mut value = original.clone();
        value.target.path = RelativePath::new("other").unwrap();
        variants.push(value);
        let mut value = original.clone();
        value.target.workbench = WorkbenchName::new("other").unwrap();
        variants.push(value);
        let mut value = original.clone();
        value.expected_workspace_incarnation_id = WorkspaceIdentity([8; 16]);
        variants.push(value);
        let mut value = original.clone();
        value.create_content_type = ContentType::new("application/json").unwrap();
        variants.push(value);
        variants.push(
            original
                .clone()
                .with_content_type(ContentType::new("text/plain").unwrap()),
        );
        variants.push(original.clone().with_block_size(2048));
        variants.push(original.clone().with_max_logical_size(32 * 1024 * 1024));
        for value in variants {
            let intent = append_intent_digest(root, &value, b"event\n").unwrap();
            assert_eq!(
                validate_append_intent(root, &status, &value, intent)
                    .unwrap_err()
                    .rpc_code(),
                Some(ErrorCode::RequestReplayMismatch)
            );
        }
        for (scope, delta) in [
            (root, b"other\n".as_slice()),
            (RootIdentity([6; 16]), b"event\n".as_slice()),
        ] {
            let intent = append_intent_digest(scope, &original, delta).unwrap();
            assert_eq!(
                validate_append_intent(scope, &status, &original, intent)
                    .unwrap_err()
                    .rpc_code(),
                Some(ErrorCode::RequestReplayMismatch)
            );
        }
        let intent = append_intent_digest(root, &original, b"event\n").unwrap();
        let mut corrupt = status;
        corrupt.append_preparation.as_mut().unwrap().attempt += 1;
        assert_eq!(
            validate_append_intent(root, &corrupt, &original, intent)
                .unwrap_err()
                .rpc_code(),
            Some(ErrorCode::RequestReplayMismatch)
        );
    }

    #[test]
    fn explicit_incarnation_and_target_mismatches_keep_logical_id_in_errors() {
        let options = options();
        let (client, _) = client(committed_status(&options));
        let error = client
            .resolve_append_workspace_incarnation(
                options.operation_id,
                &options.target,
                Some(WorkspaceIdentity([8; 16])),
            )
            .unwrap_err();
        assert_eq!(error.rpc_code(), Some(ErrorCode::Conflict));
        assert!(
            matches!(error, ClientError::AppendUnresolved {operation_id, ..} if operation_id == options.operation_id)
        );
        let mut target = options.target;
        target.path = RelativePath::new("changed").unwrap();
        let error = client
            .resolve_append_workspace_incarnation(options.operation_id, &target, None)
            .unwrap_err();
        assert_eq!(error.rpc_code(), Some(ErrorCode::RequestReplayMismatch));
    }

    #[test]
    fn oversized_input_is_rejected_before_rpc_or_provider_admission() {
        let options = options();
        let (client, calls) = client(committed_status(&options));
        let error = client
            .append_artifact_idempotent(
                &MemoryArtifactStore::new(),
                options.clone(),
                &vec![0; MAX_APPEND_DELTA_BYTES + 1],
            )
            .unwrap_err();
        assert!(matches!(error, ClientError::InvalidOptions(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let error = client
            .append_artifact_idempotent(
                &MemoryArtifactStore::new(),
                options.with_max_logical_size(0),
                b"x",
            )
            .unwrap_err();
        assert!(matches!(error, ClientError::InvalidOptions(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
