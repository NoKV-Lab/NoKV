// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! Logical append identity, attempts, and historical receipts.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    ArtifactRevisionIdentity, Digest, DigestUri, OperationIdentity, ProtocolError, RootIdentity,
    RpcFailure, WorkspaceIdentity, WorkspacePath,
};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppendAttemptBinding {
    pub operation_id: OperationIdentity,
    pub attempt: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppendAttemptPhase {
    Uploading,
    Finalizing,
    Published,
    Aborting,
    Cleaning,
    Cleaned,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppendPreparation {
    pub intent_digest: Digest,
    pub target: WorkspacePath,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub attempt: u64,
    pub publication_operation_id: OperationIdentity,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub attempt_phase: AppendAttemptPhase,
    pub activity_deadline_ms: u64,
    /// Failure of the current publication attempt, independent of the logical outcome.
    pub attempt_failure: Option<RpcFailure>,
    /// Number of owner cleanup retries accepted for this publication attempt.
    pub cleanup_retry_count: u64,
}

impl AppendPreparation {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        crate::types::require_generation("append.activity_deadline_ms", self.activity_deadline_ms)?;
        if self.cleanup_retry_count != 0
            && matches!(
                self.attempt_phase,
                AppendAttemptPhase::Uploading
                    | AppendAttemptPhase::Finalizing
                    | AppendAttemptPhase::Published
            )
        {
            return Err(ProtocolError::invalid(
                "append.cleanup_retry_count",
                "must be zero before an append attempt enters cleanup",
            ));
        }
        if let Some(failure) = &self.attempt_failure {
            failure.validate()?;
            if matches!(
                self.attempt_phase,
                AppendAttemptPhase::Uploading
                    | AppendAttemptPhase::Finalizing
                    | AppendAttemptPhase::Published
            ) {
                return Err(ProtocolError::invalid(
                    "append.attempt_failure",
                    "is not valid for an active or published attempt",
                ));
            }
        } else if matches!(
            self.attempt_phase,
            AppendAttemptPhase::Cleaned | AppendAttemptPhase::Quarantined
        ) {
            return Err(ProtocolError::invalid(
                "append.attempt_failure",
                "is required for a cleaned or quarantined attempt",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppendResult {
    /// The identity persisted by the caller before its first append.
    pub operation_id: OperationIdentity,
    /// The single publication attempt that committed this append.
    pub publication_operation_id: OperationIdentity,
    pub target: WorkspacePath,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub workspace_revision: u64,
    pub generation: u64,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    pub logical_size: u64,
    pub body_digest: DigestUri,
}

impl AppendResult {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        crate::types::require_generation("append.workspace_revision", self.workspace_revision)?;
        crate::types::require_generation("append.generation", self.generation)?;
        if self.operation_id == self.publication_operation_id {
            return Err(ProtocolError::invalid(
                "append.publication_operation_id",
                "must differ from the logical operation identity",
            ));
        }
        Ok(())
    }
}

/// Read the actual remaining staged ledger at one exact logical-operation state.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InspectAppendCleanupRequest {
    pub token: crate::OperationToken,
    pub start_after: Option<u32>,
    pub limit: u32,
}

impl InspectAppendCleanupRequest {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if !(1..=crate::MAX_ARTIFACT_PUBLISH_BATCH_ROWS as u32).contains(&self.limit) {
            return Err(ProtocolError::invalid(
                "append_cleanup.limit",
                "must be between 1 and 192",
            ));
        }
        Ok(())
    }
}

/// Requeue the quarantined current child. Repeating this exact token returns
/// the original acceptance receipt even after subsequent cleanup progress.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetryAppendCleanupRequest {
    pub token: crate::OperationToken,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppendCleanupInspection {
    pub operation: Box<crate::OperationStatus>,
    pub object_namespace_id: crate::ObjectNamespaceIdentity,
    pub publication_token: crate::OperationToken,
    /// Registered entries, excluding planned objects never admitted to the ledger.
    pub registered_count: u32,
    pub cleanup_cursor: u32,
    pub remaining_count: u32,
    pub entries: Vec<crate::StagedObject>,
    pub next_after: Option<u32>,
}

impl AppendCleanupInspection {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.operation.validate()?;
        let preparation = self.operation.append_preparation.as_ref().ok_or_else(|| {
            ProtocolError::invalid("append_cleanup.operation", "must describe a logical append")
        })?;
        if self.publication_token.operation_id != preparation.publication_operation_id
            || self.registered_count.checked_sub(self.cleanup_cursor) != Some(self.remaining_count)
            || self.entries.len() > crate::MAX_ARTIFACT_PUBLISH_BATCH_ROWS
        {
            return Err(ProtocolError::invalid(
                "append_cleanup",
                "inconsistent ledger bounds or child identity",
            ));
        }
        let mut previous = None;
        for entry in &self.entries {
            entry.validate()?;
            if entry.sequence < self.cleanup_cursor
                || entry.sequence >= self.registered_count
                || previous
                    .is_some_and(|sequence: u32| sequence.checked_add(1) != Some(entry.sequence))
            {
                return Err(ProtocolError::invalid(
                    "append_cleanup.entries",
                    "must be a contiguous remaining ledger page",
                ));
            }
            previous = Some(entry.sequence);
        }
        if let Some(next) = self.next_after {
            if previous != Some(next)
                || next
                    .checked_add(1)
                    .is_none_or(|n| n >= self.registered_count)
            {
                return Err(ProtocolError::invalid(
                    "append_cleanup.next_after",
                    "must identify the last entry of a non-final page",
                ));
            }
        } else if previous.is_some_and(|last| last + 1 != self.registered_count) {
            return Err(ProtocolError::invalid(
                "append_cleanup.entries",
                "a non-final page requires a continuation",
            ));
        }
        Ok(())
    }
}

/// Durable acceptance of one cleanup retry, independent of its eventual outcome.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppendCleanupRetryResult {
    pub operation_id: OperationIdentity,
    pub publication_operation_id: OperationIdentity,
    pub cleanup_retry_count: u64,
    pub expected_state_digest: Digest,
}

impl AppendCleanupRetryResult {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.operation_id == self.publication_operation_id || self.cleanup_retry_count == 0 {
            return Err(ProtocolError::invalid(
                "append_cleanup_retry",
                "requires distinct identities and a positive retry count",
            ));
        }
        Ok(())
    }
}

/// Derive distinct immutable identities for a logical append's numbered attempt.
pub fn stable_append_attempt_identities(
    root: RootIdentity,
    operation_id: OperationIdentity,
    attempt: u64,
) -> (OperationIdentity, ArtifactRevisionIdentity) {
    let derive = |domain: &[u8]| {
        let mut hasher = Sha256::new();
        hasher.update(domain);
        hasher.update(root.0);
        hasher.update(operation_id.0);
        hasher.update(attempt.to_be_bytes());
        let digest = hasher.finalize();
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        identity
    };
    (
        OperationIdentity(derive(b"nokv.append.publication.v2\0")),
        ArtifactRevisionIdentity(derive(b"nokv.append.revision.v2\0")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_cleanup_wire_is_bounded_and_removed_operator_verdict_is_not_decodable() {
        let token = crate::OperationToken {
            operation_id: OperationIdentity([1; 16]),
            state_digest: Digest([2; 32]),
        };
        for limit in [0, 193, u32::MAX] {
            assert!(InspectAppendCleanupRequest {
                token,
                start_after: None,
                limit
            }
            .validate()
            .is_err());
        }
        for limit in [1, 32, 192] {
            let request = InspectAppendCleanupRequest {
                token,
                start_after: Some(17),
                limit,
            };
            request.validate().unwrap();
            let encoded = rmp_serde::to_vec_named(&request).unwrap();
            assert_eq!(
                rmp_serde::from_slice::<InspectAppendCleanupRequest>(&encoded).unwrap(),
                request
            );
        }
        let receipt = AppendCleanupRetryResult {
            operation_id: token.operation_id,
            publication_operation_id: OperationIdentity([3; 16]),
            cleanup_retry_count: 1,
            expected_state_digest: token.state_digest,
        };
        receipt.validate().unwrap();
        let encoded = rmp_serde::to_vec_named(&receipt).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<AppendCleanupRetryResult>(&encoded).unwrap(),
            receipt
        );
        let removed = rmp_serde::to_vec_named("provider_objects_sealed").unwrap();
        let error = rmp_serde::from_slice::<crate::QuarantineResolution>(&removed).unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown variant `provider_objects_sealed`"));
    }

    #[test]
    fn attempt_failure_is_separate_from_logical_outcome_and_validated_by_phase() {
        let mut preparation = AppendPreparation {
            intent_digest: Digest([1; 32]),
            target: WorkspacePath {
                workbench: crate::WorkbenchName::new("append-test").unwrap(),
                path: crate::RelativePath::new("outputs/events.jsonl").unwrap(),
            },
            workspace_incarnation_id: WorkspaceIdentity([2; 16]),
            attempt: 0,
            publication_operation_id: OperationIdentity([3; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([4; 16]),
            attempt_phase: AppendAttemptPhase::Uploading,
            activity_deadline_ms: 1_000,
            attempt_failure: None,
            cleanup_retry_count: 0,
        };
        assert!(preparation.validate().is_ok());
        preparation.cleanup_retry_count = 1;
        for phase in [
            AppendAttemptPhase::Uploading,
            AppendAttemptPhase::Finalizing,
            AppendAttemptPhase::Published,
        ] {
            preparation.attempt_phase = phase;
            assert!(preparation.validate().is_err());
        }
        preparation.cleanup_retry_count = 0;
        preparation.attempt_phase = AppendAttemptPhase::Cleaned;
        assert!(preparation.validate().is_err());
        preparation.attempt_failure = Some(RpcFailure {
            code: crate::ErrorCode::OperationFailed,
            message: "publication activity lease expired".to_owned(),
            retryable: false,
            conflict: Some(crate::ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        });
        for phase in [
            AppendAttemptPhase::Aborting,
            AppendAttemptPhase::Cleaning,
            AppendAttemptPhase::Cleaned,
            AppendAttemptPhase::Quarantined,
        ] {
            preparation.attempt_phase = phase;
            for count in [0, 1, u64::MAX] {
                preparation.cleanup_retry_count = count;
                assert!(preparation.validate().is_ok());
            }
        }
        preparation.cleanup_retry_count = 0;
        preparation.attempt_phase = AppendAttemptPhase::Published;
        assert!(preparation.validate().is_err());
        preparation.attempt_phase = AppendAttemptPhase::Cleaned;
        preparation
            .attempt_failure
            .as_mut()
            .unwrap()
            .message
            .clear();
        assert!(preparation.validate().is_err());
    }
}
