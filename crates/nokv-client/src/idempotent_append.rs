// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! Cross-process append identity over the existing publication lifecycle.
//!
//! One operation owns one publication attempt. An admitted but unfinished
//! attempt is never replaced or aborted merely because another caller retries.

use nokv_object::{ArtifactObjectStore, DEFAULT_ARTIFACT_BLOCK_SIZE};
use nokv_protocol::{
    ArtifactRevisionIdentity, ConflictKind, ContentType, Digest, ErrorCode, GetOperationRequest,
    OperationIdentity, OperationKind, OperationResult, OperationState, OperationStatus,
    PublishResult, RootIdentity, RpcFailure, WorkspaceIdentity, WorkspacePath,
};
use sha2::{Digest as _, Sha256};

use crate::{
    ArtifactAppendOptions, ClientCall, ClientError, RouteResolver, RpcTransport, WorkspaceClient,
};

/// An append intent whose identity the caller persists before the first send.
///
/// Content-type inheritance and explicit overrides are distinct intents. The
/// workspace incarnation is an owner-checked fence, not an authentication token.
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
            max_logical_size: None,
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

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// Append at most once for one caller-owned identity and exact intent.
    ///
    /// A successful replay returns the original publication receipt even when
    /// another writer has since replaced or removed the path. It does not
    /// assert that the original revision is still the live head or retained.
    ///
    /// A running, failed, quarantined or uncertain operation is returned as
    /// `AppendUnresolved`. Retrying preserves the identity; it never allocates
    /// another append attempt or interprets a timeout as proof of absence.
    pub fn append_artifact_idempotent(
        &self,
        store: &dyn ArtifactObjectStore,
        options: IdempotentAppendOptions,
        delta: &[u8],
    ) -> Result<ClientCall<PublishResult>, ClientError> {
        let intent = append_intent_digest(self.root_id(), &options, delta)?;
        let revision = append_revision_identity(self.root_id(), &options);
        match self.get_operation(GetOperationRequest {
            operation_id: options.operation_id,
        }) {
            Ok(status) => return append_receipt(status, &options, intent, revision),
            Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => {}
            Err(error) => return Err(unresolved(&options, None, error)),
        }

        // No durable attempt was observed. Racing callers still use the exact
        // same operation/revision: server-side admission decides the winner.
        // Unlike legacy append, there is no sequence of potentially empty
        // attempt IDs that independent callers could fill out of order.
        let mut publication = ArtifactAppendOptions::new(
            options.operation_id,
            revision,
            options.target.clone(),
            options.create_content_type.clone(),
        )
        .with_block_size(options.block_size);
        publication.content_type = options.content_type.clone();
        publication.max_logical_size = options.max_logical_size;
        match self.append_artifact_attempt(
            store,
            &publication,
            options.operation_id,
            revision,
            delta,
            Some(options.expected_workspace_incarnation_id),
            Some(intent),
        ) {
            Ok(outcome) => Ok(outcome.publication),
            Err(source) => {
                // In particular, a Begin race may have lost to the same
                // intent, and Complete may have committed before losing its
                // response. Query before interpreting either as failure.
                match self.get_operation(GetOperationRequest {
                    operation_id: options.operation_id,
                }) {
                    Ok(status) => append_receipt(status, &options, intent, revision),
                    Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => {
                        Err(unresolved(&options, None, source))
                    }
                    Err(error) => Err(unresolved(&options, None, error)),
                }
            }
        }
    }
}

fn append_receipt(
    status: ClientCall<OperationStatus>,
    options: &IdempotentAppendOptions,
    intent: Digest,
    revision: ArtifactRevisionIdentity,
) -> Result<ClientCall<PublishResult>, ClientError> {
    let value = status.value;
    let matches = value.token.operation_id == options.operation_id
        && value.kind == OperationKind::ArtifactPublish
        && value
            .publish_preparation
            .as_ref()
            .is_some_and(|preparation| {
                preparation.append_intent_digest == Some(intent)
                    && preparation.target == options.target
                    && preparation.workspace_incarnation_id
                        == options.expected_workspace_incarnation_id
                    && preparation.artifact_revision_id == revision
            });
    if !matches {
        return Err(unresolved(
            options,
            None,
            append_failure(
                ErrorCode::RequestReplayMismatch,
                "append operation identity is already bound to a different intent or lifecycle",
            ),
        ));
    }
    if value.state != OperationState::Succeeded {
        let message = value.failure.map_or_else(
            || {
                format!(
                    "append publication is {:?}; query or retry this same identity",
                    value.state
                )
            },
            |failure| failure.message,
        );
        return Err(unresolved(
            options,
            Some(value.state),
            append_failure(
                if value.state == OperationState::Running {
                    ErrorCode::Conflict
                } else {
                    ErrorCode::OperationFailed
                },
                message,
            ),
        ));
    }
    let Some(OperationResult::ArtifactPublish(result)) = value.result else {
        return Err(unresolved(
            options,
            Some(value.state),
            ClientError::ResponseMismatch(
                "successful append has no publication receipt".to_owned(),
            ),
        ));
    };
    if result.operation_id != options.operation_id
        || result.artifact_revision_id != revision
        || result.target != options.target
    {
        return Err(unresolved(
            options,
            Some(value.state),
            ClientError::ResponseMismatch(
                "append receipt differs from its bound intent".to_owned(),
            ),
        ));
    }
    Ok(ClientCall {
        value: result,
        commit_version: status.commit_version,
        replayed: true,
    })
}

fn unresolved(
    options: &IdempotentAppendOptions,
    state: Option<OperationState>,
    source: ClientError,
) -> ClientError {
    ClientError::AppendUnresolved {
        operation_id: options.operation_id,
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
    hasher.update(b"nokv.append.intent.v1\0");
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

fn append_revision_identity(
    root: RootIdentity,
    options: &IdempotentAppendOptions,
) -> ArtifactRevisionIdentity {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.append.stable-revision.v1\0");
    hasher.update(root.0);
    hasher.update(options.operation_id.0);
    let digest = hasher.finalize();
    let mut revision = [0; 16];
    revision.copy_from_slice(&digest[..16]);
    ArtifactRevisionIdentity(revision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_protocol::{
        OperationProgress, OperationToken, PublishPreparation, RelativePath, WorkbenchName,
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
    }

    fn successful_status(
        options: &IdempotentAppendOptions,
        delta: &[u8],
    ) -> ClientCall<OperationStatus> {
        let root = RootIdentity([3; 16]);
        let revision = append_revision_identity(root, options);
        ClientCall {
            commit_version: None,
            replayed: false,
            value: OperationStatus {
                token: OperationToken {
                    operation_id: options.operation_id,
                    state_digest: Digest([9; 32]),
                },
                kind: OperationKind::ArtifactPublish,
                publish_preparation: Some(Box::new(PublishPreparation {
                    append_intent_digest: Some(append_intent_digest(root, options, delta).unwrap()),
                    target: options.target.clone(),
                    workspace_incarnation_id: options.expected_workspace_incarnation_id,
                    artifact_revision_id: revision,
                })),
                commit_preparation: None,
                restore_preparation: None,
                state: OperationState::Succeeded,
                progress: OperationProgress {
                    completed_rows: 1,
                    total_rows: Some(1),
                    completed_bytes: delta.len() as u64,
                    total_bytes: Some(delta.len() as u64),
                },
                result: Some(OperationResult::ArtifactPublish(PublishResult {
                    operation_id: options.operation_id,
                    target: options.target.clone(),
                    workspace_revision: 7,
                    generation: 4,
                    artifact_revision_id: revision,
                    logical_size: 100,
                    body_digest: nokv_protocol::sha256_digest_uri(Digest([4; 32])),
                })),
                failure: None,
            },
        }
    }

    #[test]
    fn persisted_append_identity_has_frozen_v1_encoding() {
        // Existing durable intents must remain recoverable after SDK changes.
        // This vector fixes option tags, string lengths, byte order and domains.
        let options = options();
        let root = RootIdentity([3; 16]);
        assert_eq!(
            append_intent_digest(root, &options, b"event\n").unwrap(),
            Digest([
                252, 30, 220, 186, 179, 149, 245, 9, 148, 196, 201, 191, 224, 64, 103, 147, 49,
                197, 154, 223, 18, 234, 168, 87, 99, 0, 52, 10, 45, 175, 183, 3,
            ])
        );
        assert_eq!(
            append_revision_identity(root, &options),
            ArtifactRevisionIdentity([
                138, 32, 231, 5, 12, 133, 71, 175, 10, 237, 44, 92, 128, 156, 4, 168,
            ])
        );
    }

    #[test]
    fn historical_receipt_preserves_original_result_without_live_metadata() {
        let options = options();
        let status = successful_status(&options, b"event\n");
        let expected = status.value.result.clone();
        let receipt = append_receipt(
            status,
            &options,
            append_intent_digest(RootIdentity([3; 16]), &options, b"event\n").unwrap(),
            append_revision_identity(RootIdentity([3; 16]), &options),
        )
        .unwrap();
        assert!(receipt.replayed);
        assert_eq!(
            Some(OperationResult::ArtifactPublish(receipt.value)),
            expected
        );
    }

    #[test]
    fn existing_identity_rejects_every_changed_caller_constraint() {
        let original = options().with_max_logical_size(1024);
        let status = successful_status(&original, b"event\n");
        let mut changed = Vec::new();
        let mut value = original.clone();
        value.target.path = RelativePath::new("logs/other.jsonl").unwrap();
        changed.push(value);
        let mut value = original.clone();
        value.target.workbench = WorkbenchName::new("run-43").unwrap();
        changed.push(value);
        let mut value = original.clone();
        value.expected_workspace_incarnation_id = WorkspaceIdentity([5; 16]);
        changed.push(value);
        let mut value = original.clone();
        value.create_content_type = ContentType::new("application/json").unwrap();
        changed.push(value);
        changed.push(
            original
                .clone()
                .with_content_type(ContentType::new("text/plain").unwrap()),
        );
        changed.push(original.clone().with_block_size(64));
        changed.push(original.clone().with_max_logical_size(2048));
        let mut value = original.clone();
        value.max_logical_size = None;
        changed.push(value);
        for options in changed {
            let error = append_receipt(
                status.clone(),
                &options,
                append_intent_digest(RootIdentity([3; 16]), &options, b"event\n").unwrap(),
                append_revision_identity(RootIdentity([3; 16]), &options),
            )
            .unwrap_err();
            assert_eq!(error.rpc_code(), Some(ErrorCode::RequestReplayMismatch));
            assert!(!error.retryable());
        }
        let error = append_receipt(
            status,
            &original,
            append_intent_digest(RootIdentity([3; 16]), &original, b"other\n").unwrap(),
            append_revision_identity(RootIdentity([3; 16]), &original),
        )
        .unwrap_err();
        assert_eq!(error.rpc_code(), Some(ErrorCode::RequestReplayMismatch));
    }

    #[test]
    fn unfinished_or_failed_operation_never_becomes_a_new_attempt() {
        let options = options();
        for state in [
            OperationState::Running,
            OperationState::Aborting,
            OperationState::Failed,
            OperationState::Quarantined,
        ] {
            let mut status = successful_status(&options, b"event\n");
            status.value.state = state;
            status.value.result = None;
            let error = append_receipt(
                status,
                &options,
                append_intent_digest(RootIdentity([3; 16]), &options, b"event\n").unwrap(),
                append_revision_identity(RootIdentity([3; 16]), &options),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                ClientError::AppendUnresolved {
                    operation_id,
                    state: Some(observed),
                    ..
                } if observed == state && operation_id == options.operation_id
            ));
            assert!(!error.retryable());
        }
    }
}
