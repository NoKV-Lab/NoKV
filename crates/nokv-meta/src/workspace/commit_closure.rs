/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Storage-neutral planning and verification for immutable commit closures.
//!
//! Commit construction and destination-creating restore both materialize the
//! same three ordered closures. This module owns their canonical ordering and
//! rolling-seal rules so recovery cannot silently diverge between lifecycles.

use std::fmt;

use nokv_types::{ArtifactRevisionId, CommitId, NormalizedRelativePath, SHA256_BYTES};
use sha2::{Digest, Sha256};

use super::commit_records::{
    advance_commit_member_rolling_digest, commit_member_row_digest, CommitMemberRecord,
    CommitRecordError,
};

/// One planned append to the canonical member closure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitMemberClosureStep {
    pub cursor: NormalizedRelativePath,
    pub count: u64,
    pub digest: [u8; SHA256_BYTES],
    pub row_digest: [u8; SHA256_BYTES],
}

/// One planned append to the sorted unique revision closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitRevisionClosureStep {
    pub cursor: ArtifactRevisionId,
    pub count: u64,
    pub digest: [u8; SHA256_BYTES],
}

/// One planned append to the strictly increasing parent closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitParentClosureStep {
    pub cursor: CommitId,
    pub count: u32,
    pub digest: [u8; SHA256_BYTES],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitClosureError {
    MemberNotCanonical,
    RevisionNotCanonical,
    ParentNotCanonical,
    ParentSelfReference,
    CountOverflow { closure: &'static str },
    SealMismatch { closure: &'static str },
    MemberRecord(CommitRecordError),
}

impl fmt::Display for CommitClosureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemberNotCanonical => {
                formatter.write_str("commit members must be strictly increasing by path")
            }
            Self::RevisionNotCanonical => {
                formatter.write_str("commit revisions must be strictly increasing and unique")
            }
            Self::ParentNotCanonical => {
                formatter.write_str("commit parents must be strictly increasing and unique")
            }
            Self::ParentSelfReference => {
                formatter.write_str("a commit cannot name itself as a parent")
            }
            Self::CountOverflow { closure } => {
                write!(formatter, "{closure} closure count overflow")
            }
            Self::SealMismatch { closure } => {
                write!(
                    formatter,
                    "{closure} closure seal does not match durable progress"
                )
            }
            Self::MemberRecord(error) => write!(formatter, "invalid commit member: {error}"),
        }
    }
}

impl std::error::Error for CommitClosureError {}

pub fn plan_commit_member(
    previous_cursor: Option<&NormalizedRelativePath>,
    previous_count: u64,
    previous_digest: [u8; SHA256_BYTES],
    path: &NormalizedRelativePath,
    member: &CommitMemberRecord,
) -> Result<CommitMemberClosureStep, CommitClosureError> {
    if previous_cursor.is_some_and(|cursor| cursor >= path) {
        return Err(CommitClosureError::MemberNotCanonical);
    }
    let row_digest =
        commit_member_row_digest(path, member).map_err(CommitClosureError::MemberRecord)?;
    let digest = advance_commit_member_rolling_digest(previous_digest, previous_count, row_digest);
    let count = previous_count
        .checked_add(1)
        .ok_or(CommitClosureError::CountOverflow { closure: "member" })?;
    Ok(CommitMemberClosureStep {
        cursor: path.clone(),
        count,
        digest,
        row_digest,
    })
}

pub fn plan_commit_revision(
    previous_cursor: Option<ArtifactRevisionId>,
    previous_count: u64,
    previous_digest: [u8; SHA256_BYTES],
    revision_id: ArtifactRevisionId,
    revision_ref_payload: &[u8],
) -> Result<CommitRevisionClosureStep, CommitClosureError> {
    if previous_cursor.is_some_and(|cursor| cursor >= revision_id) {
        return Err(CommitClosureError::RevisionNotCanonical);
    }
    let digest = advance_commit_revision_rolling_digest(
        previous_digest,
        previous_count,
        revision_id,
        revision_ref_payload,
    );
    let count = previous_count
        .checked_add(1)
        .ok_or(CommitClosureError::CountOverflow {
            closure: "revision",
        })?;
    Ok(CommitRevisionClosureStep {
        cursor: revision_id,
        count,
        digest,
    })
}

pub fn plan_commit_parent(
    child_commit_id: CommitId,
    previous_cursor: Option<CommitId>,
    previous_count: u32,
    previous_digest: [u8; SHA256_BYTES],
    parent_id: CommitId,
) -> Result<CommitParentClosureStep, CommitClosureError> {
    if child_commit_id == parent_id {
        return Err(CommitClosureError::ParentSelfReference);
    }
    if previous_cursor.is_some_and(|cursor| cursor >= parent_id) {
        return Err(CommitClosureError::ParentNotCanonical);
    }
    let digest =
        advance_commit_parent_rolling_digest(previous_digest, u64::from(previous_count), parent_id);
    let count = previous_count
        .checked_add(1)
        .ok_or(CommitClosureError::CountOverflow { closure: "parent" })?;
    Ok(CommitParentClosureStep {
        cursor: parent_id,
        count,
        digest,
    })
}

pub fn verify_commit_closure_seal(
    closure: &'static str,
    progress_count: u64,
    progress_digest: [u8; SHA256_BYTES],
    sealed_count: u64,
    sealed_digest: [u8; SHA256_BYTES],
) -> Result<(), CommitClosureError> {
    if progress_count == sealed_count && progress_digest == sealed_digest {
        Ok(())
    } else {
        Err(CommitClosureError::SealMismatch { closure })
    }
}

/// Advance the sorted unique revision closure using the strict ref payload.
pub fn advance_commit_revision_rolling_digest(
    previous: [u8; SHA256_BYTES],
    sequence: u64,
    revision_id: ArtifactRevisionId,
    revision_ref_payload: &[u8],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.commit.revisions.v1\0");
    hasher.update(previous);
    hasher.update(sequence.to_be_bytes());
    hasher.update(revision_id.as_bytes());
    hasher.update((revision_ref_payload.len() as u32).to_be_bytes());
    hasher.update(revision_ref_payload);
    hasher.finalize().into()
}

/// Advance the strictly increasing direct-parent closure.
pub fn advance_commit_parent_rolling_digest(
    previous: [u8; SHA256_BYTES],
    sequence: u64,
    parent_id: CommitId,
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.commit.parents.v1\0");
    hasher.update(previous);
    hasher.update(sequence.to_be_bytes());
    hasher.update(parent_id.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use nokv_types::Generation;

    use super::*;

    fn path(value: &str) -> NormalizedRelativePath {
        NormalizedRelativePath::new(value).unwrap()
    }

    fn member(revision: u8) -> CommitMemberRecord {
        CommitMemberRecord {
            artifact_revision_id: ArtifactRevisionId::from_bytes([revision; 16]),
            path_generation: Generation::new(1).unwrap(),
            body_digest_uri: format!("sha256:{}", "11".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            logical_size: 1,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: None,
            manifest_id: None,
            typed_projection: Vec::new(),
        }
    }

    #[test]
    fn member_planner_is_canonical_and_replay_deterministic() {
        let first = plan_commit_member(None, 0, [0; SHA256_BYTES], &path("a"), &member(1)).unwrap();
        assert_eq!(first.count, 1);
        assert_eq!(
            plan_commit_member(None, 0, [0; SHA256_BYTES], &path("a"), &member(1)).unwrap(),
            first
        );
        assert_eq!(
            plan_commit_member(
                Some(&first.cursor),
                first.count,
                first.digest,
                &path("a"),
                &member(2),
            ),
            Err(CommitClosureError::MemberNotCanonical)
        );
    }

    #[test]
    fn revision_and_parent_planners_reject_duplicate_or_self_edges() {
        let revision = ArtifactRevisionId::from_bytes([2; 16]);
        let revision_step =
            plan_commit_revision(None, 0, [0; SHA256_BYTES], revision, &[3]).unwrap();
        assert_eq!(
            plan_commit_revision(
                Some(revision_step.cursor),
                revision_step.count,
                revision_step.digest,
                revision,
                &[3],
            ),
            Err(CommitClosureError::RevisionNotCanonical)
        );

        let child = CommitId::from_bytes([9; SHA256_BYTES]);
        assert_eq!(
            plan_commit_parent(child, None, 0, [0; SHA256_BYTES], child),
            Err(CommitClosureError::ParentSelfReference)
        );
    }

    #[test]
    fn seal_verifier_binds_count_and_digest() {
        assert_eq!(
            verify_commit_closure_seal("member", 2, [3; SHA256_BYTES], 2, [3; SHA256_BYTES]),
            Ok(())
        );
        assert_eq!(
            verify_commit_closure_seal("member", 2, [3; SHA256_BYTES], 1, [3; SHA256_BYTES]),
            Err(CommitClosureError::SealMismatch { closure: "member" })
        );
    }
}
