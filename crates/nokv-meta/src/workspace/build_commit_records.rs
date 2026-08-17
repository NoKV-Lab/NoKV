/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable progress records for immutable commit construction and retirement.
//!
//! These records are deliberately complete recovery descriptions. A worker may
//! disappear after any successful command; its replacement reconstructs the
//! next bounded command from metadata and never depends on an in-memory path or
//! revision set.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, BuildCommitPhase, CommitId, CommitRetirePhase, ConsumerEpoch, Generation,
    NormalizedRelativePath, OperationId, ReadVersion, WorkbenchId, WorkspaceIncarnationId,
    SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::commit_records::{
    CommitRecordError, WorkbenchCommitHeadRecord, MAX_COMMIT_DIGEST_URI_BYTES,
    MAX_COMMIT_LINEAGE_BYTES, MAX_COMMIT_PRODUCER_BYTES, MAX_PARENT_COMMITS,
};
use super::publication_records::MAX_CONTENT_TYPE_BYTES;

/// Current value format for build-commit operations.
pub const BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION: u8 = 6;
/// Current value format for commit-retirement operations.
pub const COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION: u8 = 6;
const LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION: u8 = 5;
/// Maximum reconciliation evidence retained with a terminal operation.
pub const MAX_COMMIT_OPERATION_ERROR_BYTES: usize = 4 * 1024;

/// Stable success payload retained by command dedupe and the operation row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildCommitResult {
    pub commit_id: CommitId,
    pub head_generation: Generation,
}

/// Exact path claim frozen before the first commit operation row is created.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitManifestCondition {
    CreateOnly,
    ReplaceOnly { expected_generation: Generation },
}

/// Immutable revision descriptor installed by the commit-owned publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitManifestBinding {
    pub logical_size: u64,
    pub body_digest_uri: String,
    pub manifest_digest_uri: String,
    pub content_type: String,
}

/// Stable class of a build or retirement failure.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOperationErrorKind {
    HeadConflict = 1,
    SourceConflict = 2,
    AbortedByCaller = 3,
    ClosureMismatch = 4,
    InvariantViolation = 5,
}

impl TryFrom<u8> for CommitOperationErrorKind {
    type Error = CommitOperationRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::HeadConflict),
            2 => Ok(Self::SourceConflict),
            3 => Ok(Self::AbortedByCaller),
            4 => Ok(Self::ClosureMismatch),
            5 => Ok(Self::InvariantViolation),
            value => Err(CommitOperationRecordError::UnknownDiscriminant {
                type_name: "CommitOperationErrorKind",
                value,
            }),
        }
    }
}

impl From<CommitOperationErrorKind> for u8 {
    fn from(value: CommitOperationErrorKind) -> Self {
        value as u8
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitOperationTerminalError {
    pub kind: CommitOperationErrorKind,
    pub message: String,
}

/// Recoverable construction progress for one root-global immutable commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildCommitOperationRecord {
    pub operation_id: OperationId,
    pub identity_digest: [u8; SHA256_BYTES],
    pub initialization_digest: [u8; SHA256_BYTES],
    pub workbench_id: WorkbenchId,
    pub source_workspace_incarnation_id: WorkspaceIncarnationId,
    pub source_read_version: ReadVersion,
    pub commit_id: CommitId,
    pub expected_head: Option<WorkbenchCommitHeadRecord>,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    /// Opaque Agent projection-input digest, excluding only the durable time.
    pub projection_input_digest: [u8; SHA256_BYTES],
    pub tree_manifest_revision_id: ArtifactRevisionId,
    /// Original caller authorization. A retry may not upgrade a create-only
    /// commit into a replacement after the live head has advanced.
    pub replace: bool,
    /// Exact path claim used by the commit-owned manifest publication.
    pub run_manifest_condition: CommitManifestCondition,
    /// First owner-observed Unix time for the canonical run-manifest envelope.
    /// This is durable operation state, not caller identity input; every retry
    /// must reuse this value even when the wall clock has advanced.
    pub committed_at_unix_seconds: u64,
    /// Set atomically with the commit-owned revision ref when the canonical
    /// run-manifest upload reaches `Published`. The canonical path is still
    /// absent (or still names the previous head) while this binding is present.
    pub commit_staged_run_manifest: Option<CommitManifestBinding>,
    pub producer: Option<String>,
    pub lineage_projection: Vec<u8>,
    /// Strictly increasing root-global parent identities.
    pub parent_commits: Vec<CommitId>,
    pub phase: BuildCommitPhase,

    /// Last frozen workspace path durably copied into `CommitMember`.
    pub member_cursor: Option<NormalizedRelativePath>,
    pub member_count: u64,
    pub member_digest: [u8; SHA256_BYTES],
    /// The workspace path scan reaches EOF before Generic index ownership is
    /// copied from the same frozen read version.
    pub path_members_complete: bool,
    /// Last non-root Generic index scope copied into the commit closure.
    pub generic_index_cursor: Option<NormalizedRelativePath>,
    pub generic_index_count: u64,
    pub generic_index_digest: [u8; SHA256_BYTES],
    pub generic_indexes_complete: bool,
    /// Last Generic index member whose temporary build reference was
    /// atomically transferred to the immutable commit owner.
    pub generic_index_ref_cursor: Option<NormalizedRelativePath>,
    pub generic_index_ref_count: u64,
    pub generic_index_ref_digest: [u8; SHA256_BYTES],
    pub generic_index_refs_complete: bool,
    pub members_complete: bool,

    /// Number of unique commit-owned `RevisionRef` rows created.
    pub revision_ref_count: u64,
    /// Last revision included in the canonical revision seal.
    pub revision_cursor: Option<ArtifactRevisionId>,
    pub revision_seal_count: u64,
    pub revision_digest: [u8; SHA256_BYTES],
    pub revisions_complete: bool,

    /// Number of the canonical parent prefix already attached.
    pub parent_cursor: u32,
    pub parent_digest: [u8; SHA256_BYTES],
    pub parents_complete: bool,

    /// Bounded cleanup progress. Rows are removed from the smallest remaining
    /// key, so counts are durable cursors even though deleted keys disappear.
    pub cleanup_member_count: u64,
    pub cleanup_generic_index_count: u64,
    pub cleanup_revision_count: u64,
    pub cleanup_parent_count: u32,
    pub history_hold_released: bool,
    pub result: Option<BuildCommitResult>,
    pub terminal_error: Option<CommitOperationTerminalError>,
}

/// Recoverable release progress after the exact zero-consumer retirement claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRetireOperationRecord {
    pub operation_id: OperationId,
    pub identity_digest: [u8; SHA256_BYTES],
    pub commit_id: CommitId,
    pub claimed_consumer_epoch: ConsumerEpoch,
    pub member_count: u64,
    pub member_digest: [u8; SHA256_BYTES],
    pub revision_count: u64,
    pub revision_digest: [u8; SHA256_BYTES],
    pub parent_commits: Vec<CommitId>,
    pub parent_digest: [u8; SHA256_BYTES],
    pub generic_index_count: u64,
    pub generic_index_digest: [u8; SHA256_BYTES],
    pub phase: CommitRetirePhase,
    pub released_generic_index_count: u64,
    pub released_generic_index_digest: [u8; SHA256_BYTES],
    pub released_member_count: u64,
    pub released_member_digest: [u8; SHA256_BYTES],
    pub released_revision_count: u64,
    pub released_revision_digest: [u8; SHA256_BYTES],
    pub released_parent_count: u32,
    pub released_parent_digest: [u8; SHA256_BYTES],
    pub terminal_error: Option<CommitOperationTerminalError>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitOperationRecordError {
    UnsupportedValueVersion {
        actual: u8,
        expected: u8,
    },
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
    },
    InvalidOptionalTag {
        field: &'static str,
        value: u8,
    },
    InvalidUtf8 {
        field: &'static str,
    },
    InvalidWorkbenchId {
        reason: String,
    },
    InvalidPath {
        reason: String,
    },
    EmptyField {
        field: &'static str,
    },
    ContainsNul {
        field: &'static str,
        index: usize,
    },
    FieldTooLong {
        field: &'static str,
        length: usize,
        max: usize,
    },
    ParentsNotCanonical,
    IdentityDigestMismatch,
    InitializationDigestMismatch,
    InvalidPhasePayload {
        phase: &'static str,
        reason: &'static str,
    },
    CursorOutOfRange {
        field: &'static str,
        cursor: u64,
        count: u64,
    },
    ZeroScalar {
        field: &'static str,
    },
    CommitRecord(CommitRecordError),
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
}

impl fmt::Display for CommitOperationRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported commit-operation value version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidWorkbenchId { reason } => {
                write!(formatter, "invalid workbench id: {reason}")
            }
            Self::InvalidPath { reason } => write!(formatter, "invalid path cursor: {reason}"),
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::ContainsNul { field, index } => {
                write!(formatter, "{field} contains NUL at byte offset {index}")
            }
            Self::FieldTooLong { field, length, max } => {
                write!(formatter, "{field} is {length} bytes, maximum is {max}")
            }
            Self::ParentsNotCanonical => {
                formatter.write_str("parent commits must be strictly increasing and unique")
            }
            Self::IdentityDigestMismatch => {
                formatter.write_str("commit operation identity digest does not match its fields")
            }
            Self::InitializationDigestMismatch => formatter
                .write_str("commit operation initialization digest does not match its fields"),
            Self::InvalidPhasePayload { phase, reason } => {
                write!(
                    formatter,
                    "invalid {phase} commit-operation payload: {reason}"
                )
            }
            Self::CursorOutOfRange {
                field,
                cursor,
                count,
            } => write!(formatter, "{field} cursor {cursor} exceeds count {count}"),
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::CommitRecord(error) => error.fmt(formatter),
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(
                    formatter,
                    "commit-operation value has {count} trailing bytes"
                )
            }
        }
    }
}

impl std::error::Error for CommitOperationRecordError {}

impl From<CommitRecordError> for CommitOperationRecordError {
    fn from(error: CommitRecordError) -> Self {
        Self::CommitRecord(error)
    }
}

impl BuildCommitOperationRecord {
    /// Assign both stable operation digests after every immutable field is set.
    pub fn seal_digests(&mut self) {
        self.identity_digest = self.canonical_identity_digest();
        self.initialization_digest = self.canonical_initialization_digest();
    }

    pub fn canonical_identity_digest(&self) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.build-commit.identity.v1\0");
        hasher.update(self.operation_id.as_bytes());
        hasher.update(self.source_workspace_incarnation_id.as_bytes());
        hasher.update(self.commit_id.as_bytes());
        hash_bytes(&mut hasher, self.workbench_id.as_bytes());
        hasher.finalize().into()
    }

    pub fn canonical_initialization_digest(&self) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.build-commit.initialization.v5\0");
        hasher.update(self.source_read_version.get().to_be_bytes());
        hash_optional_head(&mut hasher, self.expected_head);
        hash_bytes(&mut hasher, self.content_digest_uri.as_bytes());
        hash_bytes(&mut hasher, self.manifest_digest_uri.as_bytes());
        hasher.update(self.projection_input_digest);
        hasher.update(self.tree_manifest_revision_id.as_bytes());
        hasher.update([u8::from(self.replace)]);
        hash_manifest_condition(&mut hasher, self.run_manifest_condition);
        hasher.update(self.committed_at_unix_seconds.to_be_bytes());
        match self.producer.as_deref() {
            None => hasher.update([0]),
            Some(producer) => {
                hasher.update([1]);
                hash_bytes(&mut hasher, producer.as_bytes());
            }
        }
        hash_bytes(&mut hasher, &self.lineage_projection);
        hasher.update((self.parent_commits.len() as u32).to_be_bytes());
        for parent in &self.parent_commits {
            hasher.update(parent.as_bytes());
        }
        hasher.finalize().into()
    }

    pub fn validate(&self) -> Result<(), CommitOperationRecordError> {
        validate_common_fields(
            &self.parent_commits,
            &self.content_digest_uri,
            &self.manifest_digest_uri,
            self.producer.as_deref(),
            &self.lineage_projection,
        )?;
        if self.committed_at_unix_seconds == 0 {
            return Err(CommitOperationRecordError::ZeroScalar {
                field: "committed_at_unix_seconds",
            });
        }
        if self.identity_digest != self.canonical_identity_digest() {
            return Err(CommitOperationRecordError::IdentityDigestMismatch);
        }
        if self.initialization_digest != self.canonical_initialization_digest() {
            return Err(CommitOperationRecordError::InitializationDigestMismatch);
        }
        if self.parent_commits.binary_search(&self.commit_id).is_ok() {
            return invalid_build(self.phase, "a commit cannot be its own parent");
        }
        if let Some(manifest) = &self.commit_staged_run_manifest {
            if manifest.logical_size == 0 {
                return Err(CommitOperationRecordError::ZeroScalar {
                    field: "commit_manifest.logical_size",
                });
            }
            validate_required(
                "commit_manifest.body_digest_uri",
                &manifest.body_digest_uri,
                MAX_COMMIT_DIGEST_URI_BYTES,
            )?;
            validate_required(
                "commit_manifest.manifest_digest_uri",
                &manifest.manifest_digest_uri,
                MAX_COMMIT_DIGEST_URI_BYTES,
            )?;
            validate_required(
                "commit_manifest.content_type",
                &manifest.content_type,
                MAX_CONTENT_TYPE_BYTES,
            )?;
            if manifest.content_type != "application/json" {
                return invalid_build(
                    self.phase,
                    "commit-staged run manifest must use application/json",
                );
            }
        }
        if self.member_count == 0 && self.member_cursor.is_some() {
            return invalid_build(self.phase, "member cursor requires non-zero member count");
        }
        if self.member_count > 0 && self.member_cursor.is_none() {
            return invalid_build(self.phase, "non-zero member count requires a cursor");
        }
        if (self.member_count == 0) != (self.member_digest == [0; SHA256_BYTES]) {
            return invalid_build(
                self.phase,
                "member count and initial rolling digest are inconsistent",
            );
        }
        if self.generic_index_count == 0 && self.generic_index_cursor.is_some() {
            return invalid_build(
                self.phase,
                "Generic index cursor requires non-zero Generic index progress",
            );
        }
        if (self.generic_index_count == 0) != (self.generic_index_digest == [0; SHA256_BYTES]) {
            return invalid_build(
                self.phase,
                "Generic index count and rolling digest are inconsistent",
            );
        }
        if (self.generic_index_count > 0 || self.generic_indexes_complete)
            && !self.path_members_complete
        {
            return invalid_build(
                self.phase,
                "Generic index copying requires the path member scan to reach EOF",
            );
        }
        if self.generic_index_ref_count > self.generic_index_count {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "generic_index_ref",
                cursor: self.generic_index_ref_count,
                count: self.generic_index_count,
            });
        }
        if self.generic_index_ref_count == 0 && self.generic_index_ref_cursor.is_some() {
            return invalid_build(
                self.phase,
                "Generic index ref cursor requires non-zero transfer progress",
            );
        }
        if (self.generic_index_ref_count == 0)
            != (self.generic_index_ref_digest == [0; SHA256_BYTES])
        {
            return invalid_build(
                self.phase,
                "Generic index ref count and rolling digest are inconsistent",
            );
        }
        if self.generic_index_ref_count > 0 && !self.generic_indexes_complete {
            return invalid_build(
                self.phase,
                "Generic index ref transfer requires the copied closure seal",
            );
        }
        if self.generic_index_refs_complete
            && (self.generic_index_ref_count != self.generic_index_count
                || self.generic_index_ref_digest != self.generic_index_digest)
        {
            return invalid_build(
                self.phase,
                "Generic index ref transfer must cover the copied closure",
            );
        }
        if self.members_complete
            != (self.path_members_complete
                && self.generic_indexes_complete
                && self.generic_index_refs_complete)
        {
            return invalid_build(
                self.phase,
                "member completion must include both path and Generic index closures",
            );
        }
        if self.revision_seal_count == 0 && self.revision_cursor.is_some() {
            return invalid_build(self.phase, "revision cursor requires non-zero seal count");
        }
        if self.revision_seal_count > 0 && self.revision_cursor.is_none() {
            return invalid_build(self.phase, "non-zero revision seal count requires a cursor");
        }
        if (self.revision_seal_count == 0) != (self.revision_digest == [0; SHA256_BYTES]) {
            return invalid_build(
                self.phase,
                "revision seal count and initial rolling digest are inconsistent",
            );
        }
        if self.revision_seal_count > self.revision_ref_count {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "revision_seal",
                cursor: self.revision_seal_count,
                count: self.revision_ref_count,
            });
        }
        if self.commit_staged_run_manifest.is_some() && self.revision_ref_count == 0 {
            return invalid_build(
                self.phase,
                "commit-staged run manifest requires its commit-owned revision ref",
            );
        }
        if u64::from(self.parent_cursor) > self.parent_commits.len() as u64 {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "parent",
                cursor: u64::from(self.parent_cursor),
                count: self.parent_commits.len() as u64,
            });
        }
        if (self.parent_cursor == 0) != (self.parent_digest == [0; SHA256_BYTES]) {
            return invalid_build(
                self.phase,
                "parent cursor and initial rolling digest are inconsistent",
            );
        }
        if self.cleanup_member_count > self.member_count {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "cleanup_member",
                cursor: self.cleanup_member_count,
                count: self.member_count,
            });
        }
        if self.cleanup_generic_index_count > self.generic_index_count {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "cleanup_generic_index",
                cursor: self.cleanup_generic_index_count,
                count: self.generic_index_count,
            });
        }
        if self.cleanup_revision_count > self.revision_ref_count {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "cleanup_revision",
                cursor: self.cleanup_revision_count,
                count: self.revision_ref_count,
            });
        }
        if self.cleanup_parent_count > self.parent_cursor {
            return Err(CommitOperationRecordError::CursorOutOfRange {
                field: "cleanup_parent",
                cursor: u64::from(self.cleanup_parent_count),
                count: u64::from(self.parent_cursor),
            });
        }
        if self.members_complete && self.member_digest == [0; SHA256_BYTES] && self.member_count > 0
        {
            return invalid_build(self.phase, "non-empty member closure has a zero digest");
        }
        if self.revisions_complete
            && (self.revision_seal_count != self.revision_ref_count || self.revision_ref_count == 0)
        {
            return invalid_build(
                self.phase,
                "revision closure must seal every ref and include the tree manifest",
            );
        }
        if self.parents_complete && self.parent_cursor as usize != self.parent_commits.len() {
            return invalid_build(self.phase, "parent closure is not fully attached");
        }
        if !self.members_complete
            && (self.revision_seal_count != 0
                || self.revision_cursor.is_some()
                || self.revisions_complete)
        {
            return invalid_build(
                self.phase,
                "revision sealing cannot begin before the member closure reaches EOF",
            );
        }
        if self.cleanup_revision_count > 0 && self.cleanup_parent_count != self.parent_cursor {
            return invalid_build(
                self.phase,
                "revision cleanup requires every attached parent consumer released",
            );
        }
        if self.cleanup_member_count > 0 && self.cleanup_revision_count != self.revision_ref_count {
            return invalid_build(
                self.phase,
                "member cleanup requires every commit revision ref released",
            );
        }
        if self.cleanup_generic_index_count > 0
            && self.cleanup_revision_count != self.revision_ref_count
        {
            return invalid_build(
                self.phase,
                "Generic index cleanup requires every commit revision ref released",
            );
        }
        if self.cleanup_member_count > 0
            && self.cleanup_generic_index_count != self.generic_index_count
        {
            return invalid_build(
                self.phase,
                "member cleanup requires every Generic index owner released",
            );
        }

        match self.phase {
            BuildCommitPhase::Building => {
                require_no_terminal(
                    self.result,
                    &self.terminal_error,
                    self.history_hold_released,
                )?;
                require_no_cleanup(self)?;
            }
            BuildCommitPhase::Sealing => {
                require_no_terminal(
                    self.result,
                    &self.terminal_error,
                    self.history_hold_released,
                )?;
                if !(self.members_complete && self.revisions_complete && self.parents_complete) {
                    return invalid_build(self.phase, "all three closure seals are required");
                }
                require_no_cleanup(self)?;
            }
            BuildCommitPhase::Complete => {
                if self.result.is_none()
                    || self.terminal_error.is_some()
                    || !self.history_hold_released
                    || !(self.members_complete && self.revisions_complete && self.parents_complete)
                {
                    return invalid_build(
                        self.phase,
                        "complete requires all closure seals, a result, and a released history hold",
                    );
                }
                require_no_cleanup(self)?;
                if self.result.map(|result| result.commit_id) != Some(self.commit_id) {
                    return invalid_build(self.phase, "result commit id does not match the build");
                }
            }
            BuildCommitPhase::Aborting => {
                if self.result.is_some()
                    || self.terminal_error.is_none()
                    || self.history_hold_released
                {
                    return invalid_build(
                        self.phase,
                        "abort cleanup requires terminal evidence and a live hold",
                    );
                }
                require_no_cleanup(self)?;
            }
            BuildCommitPhase::Cleaning => {
                if self.result.is_some()
                    || self.terminal_error.is_none()
                    || self.history_hold_released
                {
                    return invalid_build(
                        self.phase,
                        "abort cleanup requires terminal evidence and a live hold",
                    );
                }
            }
            BuildCommitPhase::Cleaned => {
                if self.result.is_some()
                    || self.terminal_error.is_none()
                    || !self.history_hold_released
                    || self.cleanup_member_count != self.member_count
                    || self.cleanup_generic_index_count != self.generic_index_count
                    || self.cleanup_revision_count != self.revision_ref_count
                    || self.cleanup_parent_count != self.parent_cursor
                {
                    return invalid_build(
                        self.phase,
                        "cleaned requires every owned row and the hold released",
                    );
                }
            }
            BuildCommitPhase::Quarantined => {
                if self.result.is_some() || self.terminal_error.is_none() {
                    return invalid_build(self.phase, "quarantine requires terminal evidence");
                }
            }
        }
        validate_terminal_error(self.terminal_error.as_ref())
    }

    pub fn encode(&self) -> Result<Vec<u8>, CommitOperationRecordError> {
        self.validate()?;
        let mut out = vec![BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION];
        out.extend_from_slice(self.operation_id.as_bytes());
        out.extend_from_slice(&self.identity_digest);
        out.extend_from_slice(&self.initialization_digest);
        put_bytes(&mut out, self.workbench_id.as_bytes());
        out.extend_from_slice(self.source_workspace_incarnation_id.as_bytes());
        out.extend_from_slice(&self.source_read_version.get().to_be_bytes());
        out.extend_from_slice(self.commit_id.as_bytes());
        put_optional_head(&mut out, self.expected_head);
        put_bytes(&mut out, self.content_digest_uri.as_bytes());
        put_bytes(&mut out, self.manifest_digest_uri.as_bytes());
        out.extend_from_slice(&self.projection_input_digest);
        out.extend_from_slice(self.tree_manifest_revision_id.as_bytes());
        out.push(u8::from(self.replace));
        put_manifest_condition(&mut out, self.run_manifest_condition);
        out.extend_from_slice(&self.committed_at_unix_seconds.to_be_bytes());
        put_optional_manifest_binding(&mut out, self.commit_staged_run_manifest.as_ref());
        put_optional_bytes(&mut out, self.producer.as_deref().map(str::as_bytes));
        put_bytes(&mut out, &self.lineage_projection);
        put_parents(&mut out, &self.parent_commits);
        out.push(self.phase.into());
        put_optional_path(&mut out, self.member_cursor.as_ref());
        out.extend_from_slice(&self.member_count.to_be_bytes());
        out.extend_from_slice(&self.member_digest);
        out.push(u8::from(self.path_members_complete));
        put_optional_path(&mut out, self.generic_index_cursor.as_ref());
        out.extend_from_slice(&self.generic_index_count.to_be_bytes());
        out.extend_from_slice(&self.generic_index_digest);
        out.push(u8::from(self.generic_indexes_complete));
        put_optional_path(&mut out, self.generic_index_ref_cursor.as_ref());
        out.extend_from_slice(&self.generic_index_ref_count.to_be_bytes());
        out.extend_from_slice(&self.generic_index_ref_digest);
        out.push(u8::from(self.generic_index_refs_complete));
        out.push(u8::from(self.members_complete));
        out.extend_from_slice(&self.revision_ref_count.to_be_bytes());
        put_optional_revision(&mut out, self.revision_cursor);
        out.extend_from_slice(&self.revision_seal_count.to_be_bytes());
        out.extend_from_slice(&self.revision_digest);
        out.push(u8::from(self.revisions_complete));
        out.extend_from_slice(&self.parent_cursor.to_be_bytes());
        out.extend_from_slice(&self.parent_digest);
        out.push(u8::from(self.parents_complete));
        out.extend_from_slice(&self.cleanup_member_count.to_be_bytes());
        out.extend_from_slice(&self.cleanup_generic_index_count.to_be_bytes());
        out.extend_from_slice(&self.cleanup_revision_count.to_be_bytes());
        out.extend_from_slice(&self.cleanup_parent_count.to_be_bytes());
        out.push(u8::from(self.history_hold_released));
        put_optional_result(&mut out, self.result);
        put_terminal_error(&mut out, self.terminal_error.as_ref());
        Ok(out)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitOperationRecordError> {
        let mut decoder = Decoder::new(encoded);
        let value_version = decoder.require_version(&[
            LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION,
            BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION,
        ])?;
        let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
        let identity_digest = decoder.fixed("identity_digest")?;
        let initialization_digest = decoder.fixed("initialization_digest")?;
        let workbench_id = WorkbenchId::new(decoder.string("workbench_id")?).map_err(|error| {
            CommitOperationRecordError::InvalidWorkbenchId {
                reason: error.to_string(),
            }
        })?;
        let source_workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes(decoder.fixed("source_workspace")?);
        let source_read_version =
            ReadVersion::new(decoder.u64("source_read_version")?).map_err(|_| {
                CommitOperationRecordError::ZeroScalar {
                    field: "source_read_version",
                }
            })?;
        let commit_id = CommitId::from_bytes(decoder.fixed("commit_id")?);
        let expected_head = decoder.optional_head()?;
        let content_digest_uri = decoder.string("content_digest_uri")?;
        let manifest_digest_uri = decoder.string("manifest_digest_uri")?;
        let projection_input_digest = decoder.fixed("projection_input_digest")?;
        let tree_manifest_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("tree_manifest_revision_id")?);
        let replace = decoder.boolean("replace")?;
        let run_manifest_condition = decoder.manifest_condition()?;
        let committed_at_unix_seconds = decoder.u64("committed_at_unix_seconds")?;
        let commit_staged_run_manifest = decoder.optional_manifest_binding()?;
        let producer = decoder.optional_string("producer")?;
        let lineage_projection = decoder.bytes("lineage_projection")?;
        let parent_commits = decoder.parents()?;
        let phase = decode_enum(decoder.u8("phase")?)?;
        let member_cursor = decoder.optional_path()?;
        let member_count = decoder.u64("member_count")?;
        let member_digest = decoder.fixed("member_digest")?;
        let (
            path_members_complete,
            generic_index_cursor,
            generic_index_count,
            generic_index_digest,
            generic_indexes_complete,
            generic_index_ref_cursor,
            generic_index_ref_count,
            generic_index_ref_digest,
            generic_index_refs_complete,
        ) = if value_version == BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION {
            (
                decoder.boolean("path_members_complete")?,
                decoder.optional_path()?,
                decoder.u64("generic_index_count")?,
                decoder.fixed("generic_index_digest")?,
                decoder.boolean("generic_indexes_complete")?,
                decoder.optional_path()?,
                decoder.u64("generic_index_ref_count")?,
                decoder.fixed("generic_index_ref_digest")?,
                decoder.boolean("generic_index_refs_complete")?,
            )
        } else {
            (
                false,
                None,
                0,
                [0; SHA256_BYTES],
                false,
                None,
                0,
                [0; SHA256_BYTES],
                false,
            )
        };
        let members_complete = decoder.boolean("members_complete")?;
        let (path_members_complete, generic_indexes_complete, generic_index_refs_complete) =
            if value_version == LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION {
                (members_complete, members_complete, members_complete)
            } else {
                (
                    path_members_complete,
                    generic_indexes_complete,
                    generic_index_refs_complete,
                )
            };
        let revision_ref_count = decoder.u64("revision_ref_count")?;
        let revision_cursor = decoder.optional_revision()?;
        let revision_seal_count = decoder.u64("revision_seal_count")?;
        let revision_digest = decoder.fixed("revision_digest")?;
        let revisions_complete = decoder.boolean("revisions_complete")?;
        let parent_cursor = decoder.u32("parent_cursor")?;
        let parent_digest = decoder.fixed("parent_digest")?;
        let parents_complete = decoder.boolean("parents_complete")?;
        let cleanup_member_count = decoder.u64("cleanup_member_count")?;
        let cleanup_generic_index_count =
            if value_version == BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION {
                decoder.u64("cleanup_generic_index_count")?
            } else {
                0
            };
        let cleanup_revision_count = decoder.u64("cleanup_revision_count")?;
        let cleanup_parent_count = decoder.u32("cleanup_parent_count")?;
        let history_hold_released = decoder.boolean("history_hold_released")?;
        let result = decoder.optional_result()?;
        let terminal_error = decoder.terminal_error()?;
        decoder.finish()?;
        let record = Self {
            operation_id,
            identity_digest,
            initialization_digest,
            workbench_id,
            source_workspace_incarnation_id,
            source_read_version,
            commit_id,
            expected_head,
            content_digest_uri,
            manifest_digest_uri,
            projection_input_digest,
            tree_manifest_revision_id,
            replace,
            run_manifest_condition,
            committed_at_unix_seconds,
            commit_staged_run_manifest,
            producer,
            lineage_projection,
            parent_commits,
            phase,
            member_cursor,
            member_count,
            member_digest,
            path_members_complete,
            generic_index_cursor,
            generic_index_count,
            generic_index_digest,
            generic_indexes_complete,
            generic_index_ref_cursor,
            generic_index_ref_count,
            generic_index_ref_digest,
            generic_index_refs_complete,
            members_complete,
            revision_ref_count,
            revision_cursor,
            revision_seal_count,
            revision_digest,
            revisions_complete,
            parent_cursor,
            parent_digest,
            parents_complete,
            cleanup_member_count,
            cleanup_generic_index_count,
            cleanup_revision_count,
            cleanup_parent_count,
            history_hold_released,
            result,
            terminal_error,
        };
        record.validate()?;
        Ok(record)
    }
}

impl CommitRetireOperationRecord {
    pub fn seal_identity(&mut self) {
        self.identity_digest = self.canonical_identity_digest();
    }

    pub fn canonical_identity_digest(&self) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.commit-retire.identity.v1\0");
        hasher.update(self.operation_id.as_bytes());
        hasher.update(self.commit_id.as_bytes());
        hasher.update(self.claimed_consumer_epoch.get().to_be_bytes());
        hasher.finalize().into()
    }

    pub fn validate(&self) -> Result<(), CommitOperationRecordError> {
        validate_parents(&self.parent_commits)?;
        if (self.generic_index_count == 0) != (self.generic_index_digest == [0; SHA256_BYTES]) {
            return invalid_retire(
                self.phase,
                "Generic index count and immutable digest are inconsistent",
            );
        }
        if self.identity_digest != self.canonical_identity_digest() {
            return Err(CommitOperationRecordError::IdentityDigestMismatch);
        }
        validate_release_cursor(
            "released_member",
            self.released_member_count,
            self.member_count,
        )?;
        validate_release_cursor(
            "released_revision",
            self.released_revision_count,
            self.revision_count,
        )?;
        validate_release_cursor(
            "released_parent",
            u64::from(self.released_parent_count),
            self.parent_commits.len() as u64,
        )?;
        validate_release_cursor(
            "released_generic_index",
            self.released_generic_index_count,
            self.generic_index_count,
        )?;
        for (count, digest, reason) in [
            (
                self.released_generic_index_count,
                self.released_generic_index_digest,
                "released Generic index count and rolling digest are inconsistent",
            ),
            (
                self.released_member_count,
                self.released_member_digest,
                "released member count and rolling digest are inconsistent",
            ),
            (
                self.released_revision_count,
                self.released_revision_digest,
                "released revision count and rolling digest are inconsistent",
            ),
            (
                u64::from(self.released_parent_count),
                self.released_parent_digest,
                "released parent count and rolling digest are inconsistent",
            ),
        ] {
            if (count == 0) != (digest == [0; SHA256_BYTES]) {
                return invalid_retire(self.phase, reason);
            }
        }
        match self.phase {
            CommitRetirePhase::Claiming => {
                if self.released_member_count != 0
                    || self.released_generic_index_count != 0
                    || self.released_revision_count != 0
                    || self.released_parent_count != 0
                    || self.released_member_digest != [0; SHA256_BYTES]
                    || self.released_generic_index_digest != [0; SHA256_BYTES]
                    || self.released_revision_digest != [0; SHA256_BYTES]
                    || self.released_parent_digest != [0; SHA256_BYTES]
                    || self.terminal_error.is_some()
                {
                    return invalid_retire(self.phase, "claiming cannot contain release progress");
                }
            }
            CommitRetirePhase::Releasing => {
                if self.terminal_error.is_some() {
                    return invalid_retire(self.phase, "releasing cannot be terminal");
                }
                validate_retire_release_order(self)?;
            }
            CommitRetirePhase::Complete => {
                if self.released_member_count != self.member_count
                    || self.released_generic_index_count != self.generic_index_count
                    || self.released_revision_count != self.revision_count
                    || self.released_parent_count as usize != self.parent_commits.len()
                    || self.released_member_digest != self.member_digest
                    || self.released_generic_index_digest != self.generic_index_digest
                    || self.released_revision_digest != self.revision_digest
                    || self.released_parent_digest != self.parent_digest
                    || self.terminal_error.is_some()
                {
                    return invalid_retire(
                        self.phase,
                        "complete requires every exact closure seal",
                    );
                }
            }
            CommitRetirePhase::Quarantined => {
                if self.terminal_error.is_none() {
                    return invalid_retire(self.phase, "quarantine requires terminal evidence");
                }
            }
        }
        validate_terminal_error(self.terminal_error.as_ref())
    }

    pub fn encode(&self) -> Result<Vec<u8>, CommitOperationRecordError> {
        self.validate()?;
        let mut out = vec![COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION];
        out.extend_from_slice(self.operation_id.as_bytes());
        out.extend_from_slice(&self.identity_digest);
        out.extend_from_slice(self.commit_id.as_bytes());
        out.extend_from_slice(&self.claimed_consumer_epoch.get().to_be_bytes());
        out.extend_from_slice(&self.member_count.to_be_bytes());
        out.extend_from_slice(&self.member_digest);
        out.extend_from_slice(&self.revision_count.to_be_bytes());
        out.extend_from_slice(&self.revision_digest);
        put_parents(&mut out, &self.parent_commits);
        out.extend_from_slice(&self.parent_digest);
        out.extend_from_slice(&self.generic_index_count.to_be_bytes());
        out.extend_from_slice(&self.generic_index_digest);
        out.push(self.phase.into());
        out.extend_from_slice(&self.released_generic_index_count.to_be_bytes());
        out.extend_from_slice(&self.released_generic_index_digest);
        out.extend_from_slice(&self.released_member_count.to_be_bytes());
        out.extend_from_slice(&self.released_member_digest);
        out.extend_from_slice(&self.released_revision_count.to_be_bytes());
        out.extend_from_slice(&self.released_revision_digest);
        out.extend_from_slice(&self.released_parent_count.to_be_bytes());
        out.extend_from_slice(&self.released_parent_digest);
        put_terminal_error(&mut out, self.terminal_error.as_ref());
        Ok(out)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitOperationRecordError> {
        let mut decoder = Decoder::new(encoded);
        let value_version = decoder.require_version(&[
            LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION,
            COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION,
        ])?;
        let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
        let identity_digest = decoder.fixed("identity_digest")?;
        let commit_id = CommitId::from_bytes(decoder.fixed("commit_id")?);
        let claimed_consumer_epoch = ConsumerEpoch::new(decoder.u64("claimed_consumer_epoch")?);
        let member_count = decoder.u64("member_count")?;
        let member_digest = decoder.fixed("member_digest")?;
        let revision_count = decoder.u64("revision_count")?;
        let revision_digest = decoder.fixed("revision_digest")?;
        let parent_commits = decoder.parents()?;
        let parent_digest = decoder.fixed("parent_digest")?;
        let (generic_index_count, generic_index_digest) =
            if value_version == COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION {
                (
                    decoder.u64("generic_index_count")?,
                    decoder.fixed("generic_index_digest")?,
                )
            } else {
                (0, [0; SHA256_BYTES])
            };
        let phase = decode_enum(decoder.u8("phase")?)?;
        let (released_generic_index_count, released_generic_index_digest) =
            if value_version == COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION {
                (
                    decoder.u64("released_generic_index_count")?,
                    decoder.fixed("released_generic_index_digest")?,
                )
            } else {
                (0, [0; SHA256_BYTES])
            };
        let released_member_count = decoder.u64("released_member_count")?;
        let released_member_digest = decoder.fixed("released_member_digest")?;
        let released_revision_count = decoder.u64("released_revision_count")?;
        let released_revision_digest = decoder.fixed("released_revision_digest")?;
        let released_parent_count = decoder.u32("released_parent_count")?;
        let released_parent_digest = decoder.fixed("released_parent_digest")?;
        let terminal_error = decoder.terminal_error()?;
        decoder.finish()?;
        let record = Self {
            operation_id,
            identity_digest,
            commit_id,
            claimed_consumer_epoch,
            member_count,
            member_digest,
            revision_count,
            revision_digest,
            parent_commits,
            parent_digest,
            generic_index_count,
            generic_index_digest,
            phase,
            released_generic_index_count,
            released_generic_index_digest,
            released_member_count,
            released_member_digest,
            released_revision_count,
            released_revision_digest,
            released_parent_count,
            released_parent_digest,
            terminal_error,
        };
        record.validate()?;
        Ok(record)
    }
}

fn validate_common_fields(
    parents: &[CommitId],
    content_digest_uri: &str,
    manifest_digest_uri: &str,
    producer: Option<&str>,
    lineage_projection: &[u8],
) -> Result<(), CommitOperationRecordError> {
    validate_parents(parents)?;
    validate_required(
        "content_digest_uri",
        content_digest_uri,
        MAX_COMMIT_DIGEST_URI_BYTES,
    )?;
    validate_required(
        "manifest_digest_uri",
        manifest_digest_uri,
        MAX_COMMIT_DIGEST_URI_BYTES,
    )?;
    if let Some(producer) = producer {
        validate_required("producer", producer, MAX_COMMIT_PRODUCER_BYTES)?;
    }
    validate_len(
        "lineage_projection",
        lineage_projection.len(),
        MAX_COMMIT_LINEAGE_BYTES,
    )
}

fn validate_parents(parents: &[CommitId]) -> Result<(), CommitOperationRecordError> {
    if parents.len() > MAX_PARENT_COMMITS as usize {
        return Err(CommitOperationRecordError::FieldTooLong {
            field: "parent_commits",
            length: parents.len(),
            max: MAX_PARENT_COMMITS as usize,
        });
    }
    if parents.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CommitOperationRecordError::ParentsNotCanonical);
    }
    Ok(())
}

fn validate_required(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), CommitOperationRecordError> {
    if value.is_empty() {
        return Err(CommitOperationRecordError::EmptyField { field });
    }
    validate_len(field, value.len(), max)?;
    if let Some(index) = value.as_bytes().iter().position(|byte| *byte == 0) {
        return Err(CommitOperationRecordError::ContainsNul { field, index });
    }
    Ok(())
}

fn validate_len(
    field: &'static str,
    length: usize,
    max: usize,
) -> Result<(), CommitOperationRecordError> {
    if length > max {
        Err(CommitOperationRecordError::FieldTooLong { field, length, max })
    } else {
        Ok(())
    }
}

fn validate_terminal_error(
    error: Option<&CommitOperationTerminalError>,
) -> Result<(), CommitOperationRecordError> {
    if let Some(error) = error {
        validate_required(
            "terminal_error",
            &error.message,
            MAX_COMMIT_OPERATION_ERROR_BYTES,
        )?;
    }
    Ok(())
}

fn validate_release_cursor(
    field: &'static str,
    cursor: u64,
    count: u64,
) -> Result<(), CommitOperationRecordError> {
    if cursor > count {
        Err(CommitOperationRecordError::CursorOutOfRange {
            field,
            cursor,
            count,
        })
    } else {
        Ok(())
    }
}

fn require_no_terminal(
    result: Option<BuildCommitResult>,
    error: &Option<CommitOperationTerminalError>,
    hold_released: bool,
) -> Result<(), CommitOperationRecordError> {
    if result.is_some() || error.is_some() || hold_released {
        Err(CommitOperationRecordError::InvalidPhasePayload {
            phase: "active build",
            reason: "active construction cannot contain terminal payloads",
        })
    } else {
        Ok(())
    }
}

fn require_no_cleanup(
    operation: &BuildCommitOperationRecord,
) -> Result<(), CommitOperationRecordError> {
    if operation.cleanup_member_count == 0
        && operation.cleanup_generic_index_count == 0
        && operation.cleanup_revision_count == 0
        && operation.cleanup_parent_count == 0
    {
        Ok(())
    } else {
        invalid_build(
            operation.phase,
            "publication-side phases cannot contain cleanup progress",
        )
    }
}

fn validate_retire_release_order(
    operation: &CommitRetireOperationRecord,
) -> Result<(), CommitOperationRecordError> {
    if operation.released_revision_count > 0
        && (operation.released_generic_index_count != operation.generic_index_count
            || operation.released_generic_index_digest != operation.generic_index_digest)
    {
        return invalid_retire(
            operation.phase,
            "revision release requires the exact Generic index seal",
        );
    }
    if operation.released_parent_count > 0
        && (operation.released_revision_count != operation.revision_count
            || operation.released_revision_digest != operation.revision_digest)
    {
        return invalid_retire(
            operation.phase,
            "parent release requires the exact revision seal",
        );
    }
    if operation.released_member_count > 0
        && (operation.released_parent_count as usize != operation.parent_commits.len()
            || operation.released_parent_digest != operation.parent_digest)
    {
        return invalid_retire(
            operation.phase,
            "member release requires the exact parent seal",
        );
    }
    Ok(())
}

fn invalid_build<T>(
    phase: BuildCommitPhase,
    reason: &'static str,
) -> Result<T, CommitOperationRecordError> {
    Err(CommitOperationRecordError::InvalidPhasePayload {
        phase: match phase {
            BuildCommitPhase::Building => "Building",
            BuildCommitPhase::Sealing => "Sealing",
            BuildCommitPhase::Complete => "Complete",
            BuildCommitPhase::Aborting => "Aborting",
            BuildCommitPhase::Cleaning => "Cleaning",
            BuildCommitPhase::Cleaned => "Cleaned",
            BuildCommitPhase::Quarantined => "Quarantined",
        },
        reason,
    })
}

fn invalid_retire<T>(
    phase: CommitRetirePhase,
    reason: &'static str,
) -> Result<T, CommitOperationRecordError> {
    Err(CommitOperationRecordError::InvalidPhasePayload {
        phase: match phase {
            CommitRetirePhase::Claiming => "Claiming",
            CommitRetirePhase::Releasing => "Releasing",
            CommitRetirePhase::Complete => "Complete",
            CommitRetirePhase::Quarantined => "Quarantined",
        },
        reason,
    })
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u32).to_be_bytes());
    hasher.update(bytes);
}

fn hash_optional_head(hasher: &mut Sha256, head: Option<WorkbenchCommitHeadRecord>) {
    match head {
        None => hasher.update([0]),
        Some(head) => {
            hasher.update([1]);
            hasher.update(head.commit_id.as_bytes());
            hasher.update(head.head_generation.get().to_be_bytes());
        }
    }
}

fn hash_manifest_condition(hasher: &mut Sha256, condition: CommitManifestCondition) {
    match condition {
        CommitManifestCondition::CreateOnly => hasher.update([1]),
        CommitManifestCondition::ReplaceOnly {
            expected_generation,
        } => {
            hasher.update([2]);
            hasher.update(expected_generation.get().to_be_bytes());
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn put_optional_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => out.push(0),
        Some(bytes) => {
            out.push(1);
            put_bytes(out, bytes);
        }
    }
}

fn put_optional_head(out: &mut Vec<u8>, head: Option<WorkbenchCommitHeadRecord>) {
    match head {
        None => out.push(0),
        Some(head) => {
            out.push(1);
            out.extend_from_slice(head.commit_id.as_bytes());
            out.extend_from_slice(&head.head_generation.get().to_be_bytes());
        }
    }
}

fn put_manifest_condition(out: &mut Vec<u8>, condition: CommitManifestCondition) {
    match condition {
        CommitManifestCondition::CreateOnly => out.push(1),
        CommitManifestCondition::ReplaceOnly {
            expected_generation,
        } => {
            out.push(2);
            out.extend_from_slice(&expected_generation.get().to_be_bytes());
        }
    }
}

fn put_optional_manifest_binding(out: &mut Vec<u8>, binding: Option<&CommitManifestBinding>) {
    match binding {
        None => out.push(0),
        Some(binding) => {
            out.push(1);
            out.extend_from_slice(&binding.logical_size.to_be_bytes());
            put_bytes(out, binding.body_digest_uri.as_bytes());
            put_bytes(out, binding.manifest_digest_uri.as_bytes());
            put_bytes(out, binding.content_type.as_bytes());
        }
    }
}

fn put_optional_path(out: &mut Vec<u8>, path: Option<&NormalizedRelativePath>) {
    put_optional_bytes(out, path.map(|path| path.as_str().as_bytes()));
}

fn put_optional_revision(out: &mut Vec<u8>, revision: Option<ArtifactRevisionId>) {
    match revision {
        None => out.push(0),
        Some(revision) => {
            out.push(1);
            out.extend_from_slice(revision.as_bytes());
        }
    }
}

fn put_parents(out: &mut Vec<u8>, parents: &[CommitId]) {
    out.extend_from_slice(&(parents.len() as u32).to_be_bytes());
    for parent in parents {
        out.extend_from_slice(parent.as_bytes());
    }
}

fn put_optional_result(out: &mut Vec<u8>, result: Option<BuildCommitResult>) {
    match result {
        None => out.push(0),
        Some(result) => {
            out.push(1);
            out.extend_from_slice(result.commit_id.as_bytes());
            out.extend_from_slice(&result.head_generation.get().to_be_bytes());
        }
    }
}

fn put_terminal_error(out: &mut Vec<u8>, error: Option<&CommitOperationTerminalError>) {
    match error {
        None => out.push(0),
        Some(error) => {
            out.push(1);
            out.push(error.kind.into());
            put_bytes(out, error.message.as_bytes());
        }
    }
}

fn decode_enum<T>(value: u8) -> Result<T, CommitOperationRecordError>
where
    T: TryFrom<u8, Error = nokv_types::UnknownDurableDiscriminant>,
{
    T::try_from(value).map_err(|error| CommitOperationRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn require_version(&mut self, supported: &[u8]) -> Result<u8, CommitOperationRecordError> {
        let actual = self.u8("value_format_version")?;
        if supported.contains(&actual) {
            Ok(actual)
        } else {
            Err(CommitOperationRecordError::UnsupportedValueVersion {
                actual,
                expected: *supported
                    .last()
                    .expect("every commit-operation decoder supports at least one version"),
            })
        }
    }

    fn take(
        &mut self,
        field: &'static str,
        length: usize,
    ) -> Result<&'a [u8], CommitOperationRecordError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(CommitOperationRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], CommitOperationRecordError> {
        Ok(self
            .take(field, N)?
            .try_into()
            .expect("take returned the requested fixed width"))
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, CommitOperationRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, CommitOperationRecordError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, CommitOperationRecordError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn bytes(&mut self, field: &'static str) -> Result<Vec<u8>, CommitOperationRecordError> {
        let length = self.u32(field)? as usize;
        Ok(self.take(field, length)?.to_vec())
    }

    fn string(&mut self, field: &'static str) -> Result<String, CommitOperationRecordError> {
        String::from_utf8(self.bytes(field)?)
            .map_err(|_| CommitOperationRecordError::InvalidUtf8 { field })
    }

    fn boolean(&mut self, field: &'static str) -> Result<bool, CommitOperationRecordError> {
        match self.u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(CommitOperationRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_string(
        &mut self,
        field: &'static str,
    ) -> Result<Option<String>, CommitOperationRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.string(field).map(Some),
            value => Err(CommitOperationRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_head(
        &mut self,
    ) -> Result<Option<WorkbenchCommitHeadRecord>, CommitOperationRecordError> {
        match self.u8("expected_head")? {
            0 => Ok(None),
            1 => {
                let commit_id = CommitId::from_bytes(self.fixed("expected_head_commit")?);
                let head_generation = Generation::new(self.u64("expected_head_generation")?)
                    .map_err(|_| CommitOperationRecordError::ZeroScalar {
                        field: "expected_head_generation",
                    })?;
                Ok(Some(WorkbenchCommitHeadRecord {
                    commit_id,
                    head_generation,
                }))
            }
            value => Err(CommitOperationRecordError::InvalidOptionalTag {
                field: "expected_head",
                value,
            }),
        }
    }

    fn manifest_condition(
        &mut self,
    ) -> Result<CommitManifestCondition, CommitOperationRecordError> {
        match self.u8("run_manifest_condition")? {
            1 => Ok(CommitManifestCondition::CreateOnly),
            2 => Generation::new(self.u64("run_manifest_expected_generation")?)
                .map(|expected_generation| CommitManifestCondition::ReplaceOnly {
                    expected_generation,
                })
                .map_err(|_| CommitOperationRecordError::ZeroScalar {
                    field: "run_manifest_expected_generation",
                }),
            value => Err(CommitOperationRecordError::UnknownDiscriminant {
                type_name: "CommitManifestCondition",
                value,
            }),
        }
    }

    fn optional_manifest_binding(
        &mut self,
    ) -> Result<Option<CommitManifestBinding>, CommitOperationRecordError> {
        match self.u8("commit_staged_run_manifest")? {
            0 => Ok(None),
            1 => Ok(Some(CommitManifestBinding {
                logical_size: self.u64("commit_manifest.logical_size")?,
                body_digest_uri: self.string("commit_manifest.body_digest_uri")?,
                manifest_digest_uri: self.string("commit_manifest.manifest_digest_uri")?,
                content_type: self.string("commit_manifest.content_type")?,
            })),
            value => Err(CommitOperationRecordError::InvalidOptionalTag {
                field: "commit_staged_run_manifest",
                value,
            }),
        }
    }

    fn optional_path(
        &mut self,
    ) -> Result<Option<NormalizedRelativePath>, CommitOperationRecordError> {
        match self.u8("member_cursor")? {
            0 => Ok(None),
            1 => NormalizedRelativePath::new(self.string("member_cursor")?)
                .map(Some)
                .map_err(|error| CommitOperationRecordError::InvalidPath {
                    reason: error.to_string(),
                }),
            value => Err(CommitOperationRecordError::InvalidOptionalTag {
                field: "member_cursor",
                value,
            }),
        }
    }

    fn optional_revision(
        &mut self,
    ) -> Result<Option<ArtifactRevisionId>, CommitOperationRecordError> {
        match self.u8("revision_cursor")? {
            0 => Ok(None),
            1 => Ok(Some(ArtifactRevisionId::from_bytes(
                self.fixed("revision_cursor")?,
            ))),
            value => Err(CommitOperationRecordError::InvalidOptionalTag {
                field: "revision_cursor",
                value,
            }),
        }
    }

    fn parents(&mut self) -> Result<Vec<CommitId>, CommitOperationRecordError> {
        let count = self.u32("parent_count")?;
        if count > MAX_PARENT_COMMITS {
            return Err(CommitOperationRecordError::FieldTooLong {
                field: "parent_commits",
                length: count as usize,
                max: MAX_PARENT_COMMITS as usize,
            });
        }
        let mut parents = Vec::with_capacity(count as usize);
        for _ in 0..count {
            parents.push(CommitId::from_bytes(self.fixed("parent_commit")?));
        }
        Ok(parents)
    }

    fn optional_result(&mut self) -> Result<Option<BuildCommitResult>, CommitOperationRecordError> {
        match self.u8("result")? {
            0 => Ok(None),
            1 => {
                let commit_id = CommitId::from_bytes(self.fixed("result_commit_id")?);
                let head_generation = Generation::new(self.u64("result_head_generation")?)
                    .map_err(|_| CommitOperationRecordError::ZeroScalar {
                        field: "result_head_generation",
                    })?;
                Ok(Some(BuildCommitResult {
                    commit_id,
                    head_generation,
                }))
            }
            value => Err(CommitOperationRecordError::InvalidOptionalTag {
                field: "result",
                value,
            }),
        }
    }

    fn terminal_error(
        &mut self,
    ) -> Result<Option<CommitOperationTerminalError>, CommitOperationRecordError> {
        match self.u8("terminal_error")? {
            0 => Ok(None),
            1 => Ok(Some(CommitOperationTerminalError {
                kind: CommitOperationErrorKind::try_from(self.u8("terminal_error_kind")?)?,
                message: self.string("terminal_error_message")?,
            })),
            value => Err(CommitOperationRecordError::InvalidOptionalTag {
                field: "terminal_error",
                value,
            }),
        }
    }

    fn finish(self) -> Result<(), CommitOperationRecordError> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(CommitOperationRecordError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy_v5_build_bytes(record: &BuildCommitOperationRecord) -> Vec<u8> {
        let mut encoded = record.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        decoder
            .require_version(&[BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION])
            .unwrap();
        decoder.fixed::<16>("operation_id").unwrap();
        decoder.fixed::<SHA256_BYTES>("identity_digest").unwrap();
        decoder
            .fixed::<SHA256_BYTES>("initialization_digest")
            .unwrap();
        decoder.string("workbench_id").unwrap();
        decoder.fixed::<16>("source_workspace").unwrap();
        decoder.u64("source_read_version").unwrap();
        decoder.fixed::<SHA256_BYTES>("commit_id").unwrap();
        decoder.optional_head().unwrap();
        decoder.string("content_digest_uri").unwrap();
        decoder.string("manifest_digest_uri").unwrap();
        decoder
            .fixed::<SHA256_BYTES>("projection_input_digest")
            .unwrap();
        decoder.fixed::<16>("tree_manifest_revision_id").unwrap();
        decoder.boolean("replace").unwrap();
        decoder.manifest_condition().unwrap();
        decoder.u64("committed_at_unix_seconds").unwrap();
        decoder.optional_manifest_binding().unwrap();
        decoder.optional_string("producer").unwrap();
        decoder.bytes("lineage_projection").unwrap();
        decoder.parents().unwrap();
        decoder.u8("phase").unwrap();
        decoder.optional_path().unwrap();
        decoder.u64("member_count").unwrap();
        decoder.fixed::<SHA256_BYTES>("member_digest").unwrap();
        let generic_start = decoder.offset;
        decoder.boolean("path_members_complete").unwrap();
        decoder.optional_path().unwrap();
        decoder.u64("generic_index_count").unwrap();
        decoder
            .fixed::<SHA256_BYTES>("generic_index_digest")
            .unwrap();
        decoder.boolean("generic_indexes_complete").unwrap();
        decoder.optional_path().unwrap();
        decoder.u64("generic_index_ref_count").unwrap();
        decoder
            .fixed::<SHA256_BYTES>("generic_index_ref_digest")
            .unwrap();
        decoder.boolean("generic_index_refs_complete").unwrap();
        let generic_end = decoder.offset;
        decoder.boolean("members_complete").unwrap();
        decoder.u64("revision_ref_count").unwrap();
        decoder.optional_revision().unwrap();
        decoder.u64("revision_seal_count").unwrap();
        decoder.fixed::<SHA256_BYTES>("revision_digest").unwrap();
        decoder.boolean("revisions_complete").unwrap();
        decoder.u32("parent_cursor").unwrap();
        decoder.fixed::<SHA256_BYTES>("parent_digest").unwrap();
        decoder.boolean("parents_complete").unwrap();
        decoder.u64("cleanup_member_count").unwrap();
        let cleanup_start = decoder.offset;
        decoder.u64("cleanup_generic_index_count").unwrap();
        let cleanup_end = decoder.offset;
        encoded.drain(cleanup_start..cleanup_end);
        encoded.drain(generic_start..generic_end);
        encoded[0] = LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION;
        encoded
    }

    fn legacy_v5_retire_bytes(record: &CommitRetireOperationRecord) -> Vec<u8> {
        let mut encoded = record.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        decoder
            .require_version(&[COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION])
            .unwrap();
        decoder.fixed::<16>("operation_id").unwrap();
        decoder.fixed::<SHA256_BYTES>("identity_digest").unwrap();
        decoder.fixed::<SHA256_BYTES>("commit_id").unwrap();
        decoder.u64("claimed_consumer_epoch").unwrap();
        decoder.u64("member_count").unwrap();
        decoder.fixed::<SHA256_BYTES>("member_digest").unwrap();
        decoder.u64("revision_count").unwrap();
        decoder.fixed::<SHA256_BYTES>("revision_digest").unwrap();
        decoder.parents().unwrap();
        decoder.fixed::<SHA256_BYTES>("parent_digest").unwrap();
        let closure_start = decoder.offset;
        decoder.u64("generic_index_count").unwrap();
        decoder
            .fixed::<SHA256_BYTES>("generic_index_digest")
            .unwrap();
        let closure_end = decoder.offset;
        decoder.u8("phase").unwrap();
        let released_start = decoder.offset;
        decoder.u64("released_generic_index_count").unwrap();
        decoder
            .fixed::<SHA256_BYTES>("released_generic_index_digest")
            .unwrap();
        let released_end = decoder.offset;
        encoded.drain(released_start..released_end);
        encoded.drain(closure_start..closure_end);
        encoded[0] = LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION;
        encoded
    }

    fn build_record() -> BuildCommitOperationRecord {
        let mut record = BuildCommitOperationRecord {
            operation_id: OperationId::from_bytes([1; 16]),
            identity_digest: [0; SHA256_BYTES],
            initialization_digest: [0; SHA256_BYTES],
            workbench_id: WorkbenchId::new("agent-run").unwrap(),
            source_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([2; 16]),
            source_read_version: ReadVersion::new(7).unwrap(),
            commit_id: CommitId::from_bytes([3; SHA256_BYTES]),
            expected_head: None,
            content_digest_uri: "sha256:content".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            projection_input_digest: [0x0f; SHA256_BYTES],
            tree_manifest_revision_id: ArtifactRevisionId::from_bytes([4; 16]),
            replace: false,
            run_manifest_condition: CommitManifestCondition::CreateOnly,
            committed_at_unix_seconds: 1_700_000_000,
            commit_staged_run_manifest: None,
            producer: Some("agent".to_owned()),
            lineage_projection: vec![5, 6],
            parent_commits: vec![],
            phase: BuildCommitPhase::Building,
            member_cursor: None,
            member_count: 0,
            member_digest: [0; SHA256_BYTES],
            path_members_complete: false,
            generic_index_cursor: None,
            generic_index_count: 0,
            generic_index_digest: [0; SHA256_BYTES],
            generic_indexes_complete: false,
            generic_index_ref_cursor: None,
            generic_index_ref_count: 0,
            generic_index_ref_digest: [0; SHA256_BYTES],
            generic_index_refs_complete: false,
            members_complete: false,
            revision_ref_count: 0,
            revision_cursor: None,
            revision_seal_count: 0,
            revision_digest: [0; SHA256_BYTES],
            revisions_complete: false,
            parent_cursor: 0,
            parent_digest: [0; SHA256_BYTES],
            parents_complete: false,
            cleanup_member_count: 0,
            cleanup_generic_index_count: 0,
            cleanup_revision_count: 0,
            cleanup_parent_count: 0,
            history_hold_released: false,
            result: None,
            terminal_error: None,
        };
        record.seal_digests();
        record
    }

    #[test]
    fn build_operation_round_trips_and_rejects_trailing_bytes() {
        let record = build_record();
        let encoded = record.encode().unwrap();
        assert_eq!(
            BuildCommitOperationRecord::decode(&encoded).unwrap(),
            record
        );

        let projection_start = encoded
            .windows(SHA256_BYTES)
            .position(|window| window == &record.projection_input_digest[..])
            .expect("the encoded record contains the projection digest");
        let truncated = &encoded[..projection_start + SHA256_BYTES - 1];
        assert_eq!(
            BuildCommitOperationRecord::decode(truncated),
            Err(CommitOperationRecordError::Truncated {
                field: "projection_input_digest",
                needed: SHA256_BYTES,
                remaining: SHA256_BYTES - 1,
            })
        );

        let mut corrupt = encoded;
        corrupt.push(0);
        assert!(matches!(
            BuildCommitOperationRecord::decode(&corrupt),
            Err(CommitOperationRecordError::TrailingBytes { count: 1 })
        ));

        let mut previous = record.encode().unwrap();
        previous[0] = LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION - 1;
        assert_eq!(
            BuildCommitOperationRecord::decode(&previous),
            Err(CommitOperationRecordError::UnsupportedValueVersion {
                actual: LEGACY_COMMIT_OPERATION_VALUE_FORMAT_VERSION - 1,
                expected: BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION,
            })
        );
    }

    #[test]
    fn build_and_retire_v5_dual_decode_without_claiming_generic_closures() {
        let build = build_record();
        assert_eq!(
            BuildCommitOperationRecord::decode(&legacy_v5_build_bytes(&build)).unwrap(),
            build
        );

        let mut retire = CommitRetireOperationRecord {
            operation_id: OperationId::from_bytes([7; 16]),
            identity_digest: [0; SHA256_BYTES],
            commit_id: CommitId::from_bytes([8; SHA256_BYTES]),
            claimed_consumer_epoch: ConsumerEpoch::new(9),
            member_count: 0,
            member_digest: [0; SHA256_BYTES],
            revision_count: 0,
            revision_digest: [0; SHA256_BYTES],
            parent_commits: vec![],
            parent_digest: [0; SHA256_BYTES],
            generic_index_count: 0,
            generic_index_digest: [0; SHA256_BYTES],
            phase: CommitRetirePhase::Complete,
            released_generic_index_count: 0,
            released_generic_index_digest: [0; SHA256_BYTES],
            released_member_count: 0,
            released_member_digest: [0; SHA256_BYTES],
            released_revision_count: 0,
            released_revision_digest: [0; SHA256_BYTES],
            released_parent_count: 0,
            released_parent_digest: [0; SHA256_BYTES],
            terminal_error: None,
        };
        retire.seal_identity();
        assert_eq!(
            CommitRetireOperationRecord::decode(&legacy_v5_retire_bytes(&retire)).unwrap(),
            retire
        );
    }

    #[test]
    fn immutable_initialization_is_digest_bound() {
        let mut record = build_record();
        record.manifest_digest_uri.push('x');
        assert_eq!(
            record.validate(),
            Err(CommitOperationRecordError::InitializationDigestMismatch)
        );
    }

    #[test]
    fn durable_commit_time_is_initialization_digest_bound() {
        let mut record = build_record();
        record.committed_at_unix_seconds += 1;
        assert_eq!(
            record.validate(),
            Err(CommitOperationRecordError::InitializationDigestMismatch)
        );
    }

    #[test]
    fn exact_commit_admission_is_initialization_digest_bound() {
        let record = build_record();
        assert_eq!(
            record.initialization_digest,
            [
                188, 106, 112, 71, 171, 40, 19, 4, 117, 145, 187, 135, 28, 96, 181, 127, 254, 8,
                172, 2, 229, 99, 83, 36, 90, 27, 120, 202, 199, 93, 172, 26,
            ]
        );

        let mut replace = record.clone();
        replace.replace = true;
        assert_eq!(
            replace.validate(),
            Err(CommitOperationRecordError::InitializationDigestMismatch)
        );

        let mut projection = record.clone();
        projection.projection_input_digest[0] ^= 0xff;
        assert_eq!(
            projection.validate(),
            Err(CommitOperationRecordError::InitializationDigestMismatch)
        );

        let mut condition = record;
        condition.run_manifest_condition = CommitManifestCondition::ReplaceOnly {
            expected_generation: Generation::new(7).unwrap(),
        };
        assert_eq!(
            condition.validate(),
            Err(CommitOperationRecordError::InitializationDigestMismatch)
        );
    }

    #[test]
    fn retire_operation_requires_all_seals_before_complete() {
        let mut record = CommitRetireOperationRecord {
            operation_id: OperationId::from_bytes([7; 16]),
            identity_digest: [0; SHA256_BYTES],
            commit_id: CommitId::from_bytes([8; SHA256_BYTES]),
            claimed_consumer_epoch: ConsumerEpoch::new(9),
            member_count: 2,
            member_digest: [1; SHA256_BYTES],
            revision_count: 3,
            revision_digest: [2; SHA256_BYTES],
            parent_commits: vec![CommitId::from_bytes([3; SHA256_BYTES])],
            parent_digest: [4; SHA256_BYTES],
            generic_index_count: 0,
            generic_index_digest: [0; SHA256_BYTES],
            phase: CommitRetirePhase::Complete,
            released_generic_index_count: 0,
            released_generic_index_digest: [0; SHA256_BYTES],
            released_member_count: 2,
            released_member_digest: [1; SHA256_BYTES],
            released_revision_count: 3,
            released_revision_digest: [2; SHA256_BYTES],
            released_parent_count: 1,
            released_parent_digest: [4; SHA256_BYTES],
            terminal_error: None,
        };
        record.seal_identity();
        let encoded = record.encode().unwrap();
        assert_eq!(
            CommitRetireOperationRecord::decode(&encoded).unwrap(),
            record
        );
        record.released_revision_count = 2;
        assert!(record.validate().is_err());
    }
}
