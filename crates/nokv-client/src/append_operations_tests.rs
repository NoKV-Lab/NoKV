// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{ClientOptions, StaticRouteResolver, TransportError};
use nokv_protocol::{
    decode_request, encode_response, sha256_digest_uri, stable_append_attempt_identities,
    AppendAttemptPhase, AppendPreparation, ConflictKind, LogicalShardIdentity, ObjectIdentity,
    ObjectNamespaceIdentity, OperationKind, OperationProgress, OperationState, RelativePath,
    RootRoute, StagedObject, WorkbenchName, WorkspaceIdentity, WorkspacePath,
    WorkspacePreflightResult, WorkspaceRpcOutcome, WorkspaceRpcResponse,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

fn route() -> RootRoute {
    RootRoute {
        root_id: RootIdentity([1; 16]),
        logical_shard_id: LogicalShardIdentity([2; 16]),
        object_namespace_id: ObjectNamespaceIdentity([3; 16]),
        placement_generation: 1,
        owner_epoch: 1,
    }
}

fn status(phase: AppendAttemptPhase, count: u64) -> OperationStatus {
    let operation_id = OperationIdentity([4; 16]);
    let (publication_operation_id, artifact_revision_id) =
        stable_append_attempt_identities(route().root_id, operation_id, 0);
    OperationStatus {
        token: OperationToken {
            operation_id,
            state_digest: Digest([count as u8 + 10; 32]),
        },
        kind: OperationKind::ArtifactAppend,
        append_preparation: Some(Box::new(AppendPreparation {
            intent_digest: Digest([5; 32]),
            target: WorkspacePath {
                workbench: WorkbenchName::new("queue").unwrap(),
                path: RelativePath::new("logs/events.jsonl").unwrap(),
            },
            workspace_incarnation_id: WorkspaceIdentity([6; 16]),
            attempt: 0,
            publication_operation_id,
            artifact_revision_id,
            attempt_phase: phase,
            activity_deadline_ms: 1_000,
            cleanup_retry_count: count,
            attempt_failure: matches!(
                phase,
                AppendAttemptPhase::Quarantined | AppendAttemptPhase::Cleaned
            )
            .then(|| RpcFailure {
                code: ErrorCode::Quarantined,
                message: "provider outcome could not be proved".to_owned(),
                retryable: false,
                conflict: Some(ConflictKind::OperationState),
                current_generation: None,
                route_hint: None,
            }),
        })),
        publish_preparation: None,
        commit_preparation: None,
        restore_preparation: None,
        state: if phase == AppendAttemptPhase::Quarantined {
            OperationState::Quarantined
        } else {
            OperationState::Running
        },
        progress: OperationProgress {
            completed_rows: 2,
            total_rows: Some(4),
            completed_bytes: 8,
            total_bytes: Some(16),
        },
        result: None,
        failure: (phase == AppendAttemptPhase::Quarantined).then(|| RpcFailure {
            code: ErrorCode::Quarantined,
            message: "provider outcome could not be proved".to_owned(),
            retryable: false,
            conflict: Some(ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        }),
    }
}

type ScriptedReply = Result<(WorkspaceResult, bool), TransportError>;

#[derive(Clone, Default)]
struct ScriptedTransport {
    replies: Arc<Mutex<VecDeque<ScriptedReply>>>,
    requests: Arc<Mutex<Vec<WorkspaceRequest>>>,
}

impl RpcTransport for ScriptedTransport {
    fn round_trip(&self, _: std::net::SocketAddr, bytes: &[u8]) -> Result<Vec<u8>, TransportError> {
        let request = decode_request(bytes).unwrap();
        let (result, replayed) = if matches!(request.operation, WorkspaceRequest::Preflight(_)) {
            (
                WorkspaceResult::Preflight(WorkspacePreflightResult::new(
                    request.route,
                    WorkspaceCapability::ALL,
                )),
                false,
            )
        } else {
            self.requests.lock().unwrap().push(request.operation);
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected business RPC")?
        };
        encode_response(&WorkspaceRpcResponse {
            route: request.route,
            request_id: request.request_id,
            commit_version: matches!(result, WorkspaceResult::AppendCleanupRetried(_))
                .then_some(19),
            replayed,
            outcome: WorkspaceRpcOutcome::Success(Box::new(result)),
        })
        .map_err(|error| TransportError::new(error.to_string(), false))
    }
}

fn client(
    transport: &ScriptedTransport,
) -> WorkspaceClient<ScriptedTransport, StaticRouteResolver> {
    WorkspaceClient::new(
        route().root_id,
        transport.clone(),
        StaticRouteResolver::new(route(), ([127, 0, 0, 1], 4100).into()).unwrap(),
        ClientOptions::default(),
    )
    .unwrap()
}

fn retry_receipt(observation: &OperationStatus) -> AppendCleanupRetryResult {
    let preparation = observation.append_preparation.as_ref().unwrap();
    AppendCleanupRetryResult {
        operation_id: observation.token.operation_id,
        publication_operation_id: preparation.publication_operation_id,
        cleanup_retry_count: preparation.cleanup_retry_count + 1,
        expected_state_digest: observation.token.state_digest,
    }
}

fn inspection(observation: OperationStatus, start: u32, end: u32) -> AppendCleanupInspection {
    AppendCleanupInspection {
        publication_token: OperationToken {
            operation_id: observation
                .append_preparation
                .as_ref()
                .unwrap()
                .publication_operation_id,
            state_digest: Digest([44; 32]),
        },
        operation: Box::new(observation),
        object_namespace_id: route().object_namespace_id,
        registered_count: 5,
        cleanup_cursor: 2,
        remaining_count: 3,
        entries: (start..end)
            .map(|sequence| StagedObject {
                sequence,
                object_identity: ObjectIdentity::new(format!("object-{sequence}")).unwrap(),
                expected_length: 4,
                expected_digest: sha256_digest_uri(Digest([7; 32])),
                multipart_token: None,
            })
            .collect(),
        next_after: (end < 5).then(|| end - 1),
    }
}

#[test]
fn retained_ledger_pages_keep_one_exact_token_and_reject_wrong_scope_before_io() {
    let transport = ScriptedTransport::default();
    let observation = status(AppendAttemptPhase::Quarantined, 0);
    transport.replies.lock().unwrap().extend([
        Ok((WorkspaceResult::Operation(observation.clone()), false)),
        Ok((
            WorkspaceResult::AppendCleanupInspection(inspection(observation.clone(), 2, 4)),
            false,
        )),
        Ok((
            WorkspaceResult::AppendCleanupInspection(inspection(observation.clone(), 4, 5)),
            false,
        )),
    ]);
    let client = client(&transport);
    let first = client
        .inspect_append_operation(
            observation.token.operation_id,
            PageRequest {
                limit: 2,
                cursor: None,
            },
        )
        .unwrap();
    let cursor = first.value.next_cursor.unwrap();
    let second = client
        .inspect_append_operation(
            observation.token.operation_id,
            PageRequest {
                limit: 2,
                cursor: Some(cursor.clone()),
            },
        )
        .unwrap();
    assert!(second.value.next_cursor.is_none());
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        matches!(&requests[2], WorkspaceRequest::InspectAppendCleanup(request) if request.token == observation.token && request.start_after == Some(3))
    );
    drop(requests);
    assert!(client
        .inspect_append_operation(
            OperationIdentity([88; 16]),
            PageRequest {
                limit: 2,
                cursor: Some(cursor.clone())
            }
        )
        .is_err());
    let mut damaged = cursor;
    damaged[45] ^= 1;
    assert!(client
        .inspect_append_operation(
            observation.token.operation_id,
            PageRequest {
                limit: 2,
                cursor: Some(damaged)
            }
        )
        .is_err());
    assert_eq!(transport.requests.lock().unwrap().len(), 3);
}

#[test]
fn inspection_rejects_a_truncated_page_even_when_wire_shape_is_valid() {
    let observation = status(AppendAttemptPhase::Quarantined, 0);
    let mut page = inspection(observation.clone(), 3, 5);
    assert!(validate_inspection(&page, observation.token, None, 3).is_err());
    page = inspection(observation.clone(), 2, 5);
    page.operation.token.state_digest = Digest([55; 32]);
    assert!(validate_inspection(&page, observation.token, None, 3).is_err());
}

#[test]
fn explicit_recovery_replays_old_receipt_before_observing_a_new_quarantine() {
    let transport = ScriptedTransport::default();
    let original = status(AppendAttemptPhase::Quarantined, 0);
    let current = status(AppendAttemptPhase::Quarantined, 1);
    let receipt = retry_receipt(&original);
    transport.replies.lock().unwrap().extend([
        Ok((WorkspaceResult::AppendCleanupRetried(receipt.clone()), true)),
        Ok((WorkspaceResult::Operation(current.clone()), false)),
    ]);
    let call = client(&transport)
        .recover_append_operation(original.token.operation_id, Some(original.token))
        .unwrap();
    assert!(call.replayed);
    assert!(call.value.requested);
    assert_eq!(call.value.receipt, Some(receipt));
    assert_eq!(call.value.operation, current);
    assert!(
        matches!(&transport.requests.lock().unwrap()[0], WorkspaceRequest::RetryAppendCleanup(request) if request.token == original.token)
    );
    assert_eq!(transport.requests.lock().unwrap().len(), 2);
}

#[test]
fn unknown_cleanup_ack_retains_the_original_token_without_selecting_a_new_round() {
    let transport = ScriptedTransport::default();
    let observation = status(AppendAttemptPhase::Quarantined, 0);
    transport
        .replies
        .lock()
        .unwrap()
        .push_back(Err(TransportError::new("acknowledgement lost", false)));
    let error = client(&transport)
        .recover_append_operation(observation.token.operation_id, Some(observation.token))
        .unwrap_err();
    assert!(!error.retryable());
    assert!(
        matches!(error, ClientError::AppendCleanupUnresolved { expected_token, receipt: None, .. } if expected_token == observation.token)
    );
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
}

#[test]
fn implicit_recovery_receipt_must_match_the_observed_child_and_next_counter() {
    let observation = status(AppendAttemptPhase::Quarantined, 0);
    for wrong_child in [true, false] {
        let transport = ScriptedTransport::default();
        let mut receipt = retry_receipt(&observation);
        if wrong_child {
            receipt.publication_operation_id = OperationIdentity([99; 16]);
        } else {
            receipt.cleanup_retry_count = 99;
        }
        transport.replies.lock().unwrap().extend([
            Ok((WorkspaceResult::Operation(observation.clone()), false)),
            Ok((WorkspaceResult::AppendCleanupRetried(receipt), false)),
        ]);
        let error = client(&transport)
            .recover_append_operation(observation.token.operation_id, None)
            .unwrap_err();
        assert!(
            matches!(error, ClientError::AppendCleanupUnresolved { source, .. }
            if matches!(*source, ClientError::ResponseMismatch(_)))
        );
        assert_eq!(transport.requests.lock().unwrap().len(), 2);
    }
}

#[test]
fn successful_cleanup_admission_receipt_survives_a_failed_status_read() {
    let transport = ScriptedTransport::default();
    let observation = status(AppendAttemptPhase::Quarantined, 0);
    let receipt = retry_receipt(&observation);
    transport.replies.lock().unwrap().extend([
        Ok((
            WorkspaceResult::AppendCleanupRetried(receipt.clone()),
            false,
        )),
        Err(TransportError::new(
            "owner unavailable after admission",
            false,
        )),
    ]);
    let error = client(&transport)
        .recover_append_operation(observation.token.operation_id, Some(observation.token))
        .unwrap_err();
    assert!(
        matches!(error, ClientError::AppendCleanupUnresolved { expected_token, receipt: Some(actual), .. } if expected_token == observation.token && *actual == receipt)
    );
}

#[test]
fn implicit_recovery_cannot_abort_active_or_cleaned_attempts() {
    for phase in [
        AppendAttemptPhase::Uploading,
        AppendAttemptPhase::Finalizing,
        AppendAttemptPhase::Cleaning,
        AppendAttemptPhase::Cleaned,
    ] {
        let transport = ScriptedTransport::default();
        let observation = status(phase, 0);
        transport
            .replies
            .lock()
            .unwrap()
            .push_back(Ok((WorkspaceResult::Operation(observation.clone()), false)));
        let call = client(&transport)
            .recover_append_operation(observation.token.operation_id, None)
            .unwrap();
        assert!(!call.value.requested);
        assert_eq!(call.value.receipt, None);
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }
}
