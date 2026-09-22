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
}

impl AppendPreparation {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        crate::types::require_generation("append.activity_deadline_ms", self.activity_deadline_ms)?;
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
        };
        assert!(preparation.validate().is_ok());
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
            assert!(preparation.validate().is_ok());
        }
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
