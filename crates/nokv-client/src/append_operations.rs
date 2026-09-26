// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only inspection and owner-executed recovery of logical appends.

use nokv_protocol::{
    AppendCleanupInspection, AppendCleanupRetryResult, Digest, ErrorCode,
    InspectAppendCleanupRequest, OperationIdentity, OperationStatus, OperationToken, PageRequest,
    RetryAppendCleanupRequest, RootIdentity, RpcFailure, WorkspaceCapability, WorkspaceRequest,
    WorkspaceResult,
};
use sha2::{Digest as _, Sha256};

use crate::{
    append_operation_recovery, AppendRecoveryState, ClientCall, ClientError, RouteResolver,
    RpcTransport, WorkspaceClient,
};

pub const DEFAULT_APPEND_INSPECTION_LIMIT: u32 = 32;
pub const MAX_APPEND_INSPECTION_LIMIT: u32 = nokv_protocol::MAX_ARTIFACT_PUBLISH_BATCH_ROWS as u32;

// A versioned SDK cursor binds the root, logical state token and last ordinal.
// The checksum detects damaged cursors; authorization remains with the server's
// exact token/namespace/ledger validation, not this unkeyed checksum.
const CURSOR_TAG: &[u8; 8] = b"NKAPIN01";
const CURSOR_PAYLOAD_BYTES: usize = 76;
const CURSOR_BYTES: usize = CURSOR_PAYLOAD_BYTES + 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOperationInspection {
    pub inspection: AppendCleanupInspection,
    /// Continue this exact observation. Restart inspection if its state changes.
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendRecoveryRequestResult {
    /// A fresh observation after admission; not a promise of cleanup completion.
    pub operation: OperationStatus,
    /// True when this request has a durable cleanup retry receipt, including replay.
    pub requested: bool,
    /// Historical admission receipt, distinct from the logical append's receipt.
    pub receipt: Option<AppendCleanupRetryResult>,
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// Inspect retained staged entries without payloads or object credentials.
    ///
    /// Every page binds the complete logical/child observation. A stale cursor
    /// fails rather than combining ledger rows from different cleanup progress
    /// or publication attempts. Retired entries are not historical inventory.
    pub fn inspect_append_operation(
        &self,
        operation_id: OperationIdentity,
        page: PageRequest,
    ) -> Result<ClientCall<AppendOperationInspection>, ClientError> {
        if !(1..=MAX_APPEND_INSPECTION_LIMIT).contains(&page.limit) {
            return Err(invalid_input(format!(
                "append inspection limit must be between 1 and {MAX_APPEND_INSPECTION_LIMIT}"
            )));
        }
        let continuation = page
            .cursor
            .as_deref()
            .map(|cursor| decode_cursor(self.root_id(), operation_id, cursor))
            .transpose()?;
        let namespace = self
            .preflight([WorkspaceCapability::ArtifactAppendRecoveryV1])?
            .value
            .route
            .object_namespace_id;
        let (token, start_after) = match continuation {
            Some((token, after)) => (token, Some(after)),
            None => (self.get_append_operation(operation_id)?.value.token, None),
        };
        let call = self.execute_read(WorkspaceRequest::InspectAppendCleanup(
            InspectAppendCleanupRequest {
                token,
                start_after,
                limit: page.limit,
            },
        ))?;
        call.map(|result| {
            let WorkspaceResult::AppendCleanupInspection(inspection) = result else {
                return Err(ClientError::ResponseMismatch(
                    "expected append cleanup inspection".to_owned(),
                ));
            };
            validate_inspection(&inspection, token, start_after, page.limit)?;
            if inspection.object_namespace_id != namespace {
                return Err(ClientError::ResponseMismatch(
                    "append inspection returned a different object namespace".to_owned(),
                ));
            }
            let next_cursor = inspection
                .next_after
                .map(|after| encode_cursor(self.root_id(), token, after));
            Ok(AppendOperationInspection {
                inspection,
                next_cursor,
            })
        })
    }

    /// Ask the fenced owner to retry quarantined cleanup once.
    ///
    /// Persist the token from inspection and provide it on every retry of the
    /// same recovery request, including after process death. Omitting a token
    /// explicitly starts a request against the current observation; it is a
    /// no-op for active, cleaned or committed attempts. This method never chooses
    /// another token after a race or an unknown acknowledgement. It does not
    /// upload, seal objects itself, admit a successor, or reconstruct the delta.
    pub fn recover_append_operation(
        &self,
        operation_id: OperationIdentity,
        expected_token: Option<OperationToken>,
    ) -> Result<ClientCall<AppendRecoveryRequestResult>, ClientError> {
        if expected_token.is_some_and(|token| token.operation_id != operation_id) {
            return Err(invalid_input(
                "append recovery token belongs to a different logical operation",
            ));
        }
        self.preflight([WorkspaceCapability::ArtifactAppendRecoveryV1])?;
        let (token, expected_receipt) = match expected_token {
            Some(token) => (token, None),
            None => {
                let call = self.get_append_operation(operation_id)?;
                if append_operation_recovery(&call.value)?.state != AppendRecoveryState::Quarantined
                {
                    return call.map(|operation| {
                        Ok(AppendRecoveryRequestResult {
                            operation,
                            requested: false,
                            receipt: None,
                        })
                    });
                }
                let preparation = call.value.append_preparation.as_ref().ok_or_else(|| {
                    ClientError::ResponseMismatch("append status has no preparation".to_owned())
                })?;
                let count = preparation
                    .cleanup_retry_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_input("append cleanup retry counter is exhausted"))?;
                (
                    call.value.token,
                    Some((preparation.publication_operation_id, count)),
                )
            }
        };
        let retry = self
            .execute(
                self.new_request_id(),
                WorkspaceRequest::RetryAppendCleanup(RetryAppendCleanupRequest { token }),
            )
            .and_then(|call| {
                call.map(|result| {
                    let WorkspaceResult::AppendCleanupRetried(receipt) = result else {
                        return Err(ClientError::ResponseMismatch(
                            "expected append cleanup retry receipt".to_owned(),
                        ));
                    };
                    if receipt.operation_id != operation_id
                        || receipt.expected_state_digest != token.state_digest
                        || receipt.publication_operation_id == operation_id
                        || receipt.cleanup_retry_count == 0
                        || expected_receipt.is_some_and(|(child, count)| {
                            receipt.publication_operation_id != child
                                || receipt.cleanup_retry_count != count
                        })
                    {
                        return Err(ClientError::ResponseMismatch(
                            "append cleanup receipt does not match the requested observation"
                                .to_owned(),
                        ));
                    }
                    Ok(receipt)
                })
            })
            .map_err(|source| cleanup_unresolved(token, None, source))?;
        let operation = self
            .get_append_operation(operation_id)
            .map_err(|source| cleanup_unresolved(token, Some(retry.value.clone()), source))?
            .value;
        retry.map(|receipt| {
            Ok(AppendRecoveryRequestResult {
                operation,
                requested: true,
                receipt: Some(receipt),
            })
        })
    }
}

fn validate_inspection(
    inspection: &AppendCleanupInspection,
    token: OperationToken,
    start_after: Option<u32>,
    limit: u32,
) -> Result<(), ClientError> {
    let mismatch = || {
        ClientError::ResponseMismatch(
            "append inspection does not match its exact token and retained ledger interval"
                .to_owned(),
        )
    };
    append_operation_recovery(&inspection.operation)?;
    let preparation = inspection
        .operation
        .append_preparation
        .as_ref()
        .ok_or_else(mismatch)?;
    if inspection.operation.token != token
        || inspection.publication_token.operation_id != preparation.publication_operation_id
        || inspection.cleanup_cursor > inspection.registered_count
        || inspection.remaining_count != inspection.registered_count - inspection.cleanup_cursor
    {
        return Err(mismatch());
    }
    let start = match start_after {
        Some(after) => after.checked_add(1).ok_or_else(mismatch)?,
        None => inspection.cleanup_cursor,
    };
    if start < inspection.cleanup_cursor || start > inspection.registered_count {
        return Err(mismatch());
    }
    let count = (inspection.registered_count - start).min(limit);
    if inspection.entries.len() != count as usize {
        return Err(mismatch());
    }
    for (index, entry) in inspection.entries.iter().enumerate() {
        if entry.sequence != start + index as u32 {
            return Err(mismatch());
        }
    }
    let end = start + count;
    let expected_next = (end < inspection.registered_count).then(|| end - 1);
    if inspection.next_after != expected_next {
        return Err(mismatch());
    }
    Ok(())
}

fn encode_cursor(root: RootIdentity, token: OperationToken, after: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CURSOR_BYTES);
    bytes.extend_from_slice(CURSOR_TAG);
    bytes.extend_from_slice(&root.0);
    bytes.extend_from_slice(&token.operation_id.0);
    bytes.extend_from_slice(&token.state_digest.0);
    bytes.extend_from_slice(&after.to_be_bytes());
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum);
    bytes
}

fn decode_cursor(
    root: RootIdentity,
    operation_id: OperationIdentity,
    bytes: &[u8],
) -> Result<(OperationToken, u32), ClientError> {
    let invalid = || invalid_input("invalid append inspection cursor; restart inspection");
    if bytes.len() != CURSOR_BYTES
        || &bytes[..8] != CURSOR_TAG
        || bytes[8..24] != root.0
        || bytes[24..40] != operation_id.0
        || bytes[CURSOR_PAYLOAD_BYTES..] != Sha256::digest(&bytes[..CURSOR_PAYLOAD_BYTES])[..]
    {
        return Err(invalid());
    }
    Ok((
        OperationToken {
            operation_id,
            state_digest: Digest(bytes[40..72].try_into().map_err(|_| invalid())?),
        },
        u32::from_be_bytes(bytes[72..76].try_into().map_err(|_| invalid())?),
    ))
}

fn invalid_input(message: impl Into<String>) -> ClientError {
    ClientError::Rpc(RpcFailure {
        code: ErrorCode::InvalidArgument,
        message: message.into(),
        retryable: false,
        conflict: None,
        current_generation: None,
        route_hint: None,
    })
}

fn cleanup_unresolved(
    expected_token: OperationToken,
    receipt: Option<AppendCleanupRetryResult>,
    source: ClientError,
) -> ClientError {
    ClientError::AppendCleanupUnresolved {
        operation_id: expected_token.operation_id,
        expected_token,
        receipt: receipt.map(Box::new),
        source: Box::new(source),
    }
}

#[cfg(test)]
#[path = "append_operations_tests.rs"]
mod tests;
