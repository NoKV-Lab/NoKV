/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Observing a publish, commit, or restore operation by its identity.
//!
//! An append has its own logical lifecycle over physical attempts and is
//! observed through [`crate::append_operation_recovery`].

use nokv_protocol::{OperationKind, OperationResult, OperationState, OperationStatus};
use nokv_types::NextAction;

use crate::ClientError;

/// The caller-facing state of one publish, commit, or restore operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationObservedState {
    Running,
    Succeeded,
    Aborting,
    Failed,
    Quarantined,
}

impl OperationObservedState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Aborting => "aborting",
            Self::Failed => "failed",
            Self::Quarantined => "quarantined",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationObservation {
    pub state: OperationObservedState,
    pub next_action: NextAction,
}

/// Classify one publish, commit, or restore observation without a mutation.
///
/// A durably failed operation cannot run again under its identity: commit and
/// restore identities derive from their content, and a publish identity is
/// bound to its inputs, so the caller changes the intent instead.
pub fn operation_observation(
    status: &OperationStatus,
) -> Result<OperationObservation, ClientError> {
    let receipt_of_kind = matches!(
        (status.kind, &status.result),
        (
            OperationKind::ArtifactPublish,
            Some(OperationResult::ArtifactPublish(_))
        ) | (OperationKind::Commit, Some(OperationResult::Commit(_)))
            | (OperationKind::Restore, Some(OperationResult::Restore(_)))
    );
    if status.kind == OperationKind::ArtifactAppend {
        return Err(ClientError::ResponseMismatch(
            "a logical append is observed through its append recovery".to_owned(),
        ));
    }
    if (status.state == OperationState::Succeeded) != receipt_of_kind {
        return Err(ClientError::ResponseMismatch(
            "operation receipt does not match its state and kind".to_owned(),
        ));
    }
    let (state, next_action) = match status.state {
        OperationState::Running => (OperationObservedState::Running, NextAction::Poll),
        OperationState::Aborting => (OperationObservedState::Aborting, NextAction::Poll),
        OperationState::Succeeded => (OperationObservedState::Succeeded, NextAction::None),
        OperationState::Failed => (OperationObservedState::Failed, NextAction::DoNotRetry),
        OperationState::Quarantined => (OperationObservedState::Quarantined, NextAction::Operator),
    };
    Ok(OperationObservation { state, next_action })
}

#[cfg(test)]
mod tests {
    use nokv_protocol::{
        CommitIdentity, Digest, OperationIdentity, OperationProgress, OperationToken, WorkbenchName,
    };

    use super::*;

    fn status(
        kind: OperationKind,
        state: OperationState,
        result: Option<OperationResult>,
    ) -> OperationStatus {
        OperationStatus {
            append_preparation: None,
            token: OperationToken {
                operation_id: OperationIdentity([1; 16]),
                state_digest: Digest([2; 32]),
            },
            kind,
            publish_preparation: None,
            commit_preparation: None,
            restore_preparation: None,
            state,
            progress: OperationProgress {
                completed_rows: 0,
                total_rows: None,
                completed_bytes: 0,
                total_bytes: None,
            },
            result,
            failure: None,
        }
    }

    fn commit_receipt() -> OperationResult {
        OperationResult::Commit(nokv_protocol::CommitResult {
            operation_id: OperationIdentity([1; 16]),
            commit_id: CommitIdentity([3; 32]),
            workbench: WorkbenchName::new("run").unwrap(),
            head_generation: 1,
            member_count: 0,
            member_digest: Digest([4; 32]),
        })
    }

    #[test]
    fn each_terminal_and_running_state_names_one_next_action() {
        let cases = [
            (OperationState::Running, None, "running", NextAction::Poll),
            (OperationState::Aborting, None, "aborting", NextAction::Poll),
            (
                OperationState::Succeeded,
                Some(commit_receipt()),
                "succeeded",
                NextAction::None,
            ),
            (
                OperationState::Failed,
                None,
                "failed",
                NextAction::DoNotRetry,
            ),
            (
                OperationState::Quarantined,
                None,
                "quarantined",
                NextAction::Operator,
            ),
        ];
        for (state, result, name, next_action) in cases {
            let observation =
                operation_observation(&status(OperationKind::Commit, state, result)).unwrap();
            assert_eq!(observation.state.as_str(), name);
            assert_eq!(observation.next_action, next_action);
        }
    }

    #[test]
    fn a_receipt_must_match_its_state_and_kind() {
        // Success without its receipt, a receipt before success, and another
        // kind's receipt are all malformed observations.
        for (kind, state, result) in [
            (OperationKind::Commit, OperationState::Succeeded, None),
            (
                OperationKind::Commit,
                OperationState::Running,
                Some(commit_receipt()),
            ),
            (
                OperationKind::Restore,
                OperationState::Succeeded,
                Some(commit_receipt()),
            ),
        ] {
            assert!(operation_observation(&status(kind, state, result)).is_err());
        }
        assert!(operation_observation(&status(
            OperationKind::ArtifactAppend,
            OperationState::Running,
            None
        ))
        .is_err());
    }
}
