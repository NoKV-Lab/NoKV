/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable closure records for destination-creating workspace restores.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommitId, Generation, NormalizedRelativePath, OperationId, ReadVersion,
    RestorePhase, RestoreSourceKind, SnapshotId, WorkbenchId, WorkspaceIncarnationId,
    WorkspaceRevision, SHA256_BYTES,
};

use super::commit_closure::advance_commit_parent_rolling_digest;
use super::commit_records::MAX_COMMIT_DIGEST_URI_BYTES;

/// Current value format for restore-operation payloads.
pub const RESTORE_OPERATION_VALUE_FORMAT_VERSION: u8 = 6;
const LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION: u8 = 5;
/// Restore-member rows retain their v4 layout; operation v6 owns the Generic
/// index closure while operation v5 remains dual-decodable.
pub const RESTORE_MEMBER_VALUE_FORMAT_VERSION: u8 = 4;

/// Maximum reconciliation text retained by a failed restore.
pub const MAX_RESTORE_TERMINAL_ERROR_BYTES: usize = 4 * 1024;
/// Maximum number of ordered members in one restore closure.
pub const MAX_RESTORE_MEMBERS: u64 = 16 * 1024 * 1024;
/// Maximum canonical Workbench restore-manifest body bound by one operation.
pub const MAX_RESTORE_MANIFEST_BYTES: usize = 1024 * 1024;
/// Exact content type of the single Workbench restore-manifest projection.
pub const RESTORE_MANIFEST_CONTENT_TYPE: &str = "application/json";

/// Caller-computed object-plane descriptor bound when restore begins.
///
/// The body bytes stay out of the metadata store. This descriptor proves that the one
/// restore-staging publication is the Workbench projection selected before
/// any source rows are copied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreManifestDescriptor {
    pub body_digest_uri: String,
    pub logical_size: u64,
    pub content_type: String,
}

/// Caller-reserved identity of one object-first manifest publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreManifestIdentity {
    pub publication_operation_id: OperationId,
    pub artifact_revision_id: ArtifactRevisionId,
}

/// Immutable source commit facts frozen by restore admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreSourceCommitSeal {
    pub commit_id: CommitId,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    pub tree_manifest_revision_id: ArtifactRevisionId,
    pub member_count: u64,
    pub member_digest: [u8; SHA256_BYTES],
    pub unique_revision_count: u64,
    pub revision_digest: [u8; SHA256_BYTES],
    pub parent_digest: [u8; SHA256_BYTES],
    pub generic_index_count: u64,
    pub generic_index_digest: [u8; SHA256_BYTES],
}

/// Exact object-first publication identity for a destination-owned manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreManifestPublication {
    pub publication_operation_id: OperationId,
    pub workspace_incarnation_id: WorkspaceIncarnationId,
    pub artifact_revision_id: ArtifactRevisionId,
    pub body_digest_uri: String,
    pub manifest_digest_uri: String,
    pub logical_size: u64,
    pub content_type: String,
}

/// Both destination-owned Workbench projections staged atomically while the
/// destination is hidden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreDestinationManifests {
    pub run_manifest: RestoreManifestPublication,
    pub restore_manifest: RestoreManifestPublication,
}

/// Destination authority bound only after the source closure is sealed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreDestinationBinding {
    pub destination_commit_id: CommitId,
    pub effective_content_digest_uri: String,
    pub destination_projection_input_digest: [u8; SHA256_BYTES],
    pub run_manifest_identity: RestoreManifestIdentity,
    pub restore_manifest_identity: RestoreManifestIdentity,
    pub manifests: Option<RestoreDestinationManifests>,
}

/// Durable cursors and seals for the destination immutable commit closure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreCommitClosureProgress {
    /// Canonical `PathCurrent` scan cursor. This ordering is independent of
    /// the append-ordered `RestoreMember` copy ledger.
    pub member_cursor: Option<NormalizedRelativePath>,
    /// Number of final destination paths materialized as `CommitMember` rows.
    pub member_count: u64,
    /// Canonical `CommitMember` rolling digest, never the `RestoreMember`
    /// rolling digest.
    pub member_digest: [u8; SHA256_BYTES],
    pub path_members_complete: bool,
    pub generic_index_cursor: Option<NormalizedRelativePath>,
    pub generic_index_count: u64,
    pub generic_index_digest: [u8; SHA256_BYTES],
    pub generic_indexes_complete: bool,
    pub member_seal: Option<[u8; SHA256_BYTES]>,
    /// Number of unique revision refs materialized while members are built.
    pub revision_ref_count: u64,
    /// Last sorted revision ref included by the independent sealing scan.
    pub revision_cursor: Option<ArtifactRevisionId>,
    pub revision_seal_count: u64,
    pub revision_digest: [u8; SHA256_BYTES],
    pub revision_seal: Option<[u8; SHA256_BYTES]>,
    /// The single-parent seal must bind exactly the frozen source commit.
    pub parent_digest: [u8; SHA256_BYTES],
    pub parent_seal: Option<[u8; SHA256_BYTES]>,
    /// Reverse cleanup cursors for rows created before visibility publication.
    pub cleanup_member_count: u64,
    pub cleanup_generic_index_count: u64,
    pub cleanup_revision_count: u64,
}

/// Commit provenance is absent only for terminal restore-operation v4 rows
/// retained for historical status inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreCommitProvenanceV5 {
    pub source_commit: RestoreSourceCommitSeal,
    pub destination_committed_at_unix_seconds: u64,
    pub destination_binding: Option<RestoreDestinationBinding>,
    pub closure: RestoreCommitClosureProgress,
    pub destination_head_generation: Option<Generation>,
}

/// Versioned commit provenance carried by a restore operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreCommitProvenance {
    MissingLegacyV4,
    V5(Box<RestoreCommitProvenanceV5>),
}

impl RestoreManifestDescriptor {
    pub fn validate(&self) -> Result<(), RestoreRecordError> {
        if self.logical_size == 0 || self.logical_size > MAX_RESTORE_MANIFEST_BYTES as u64 {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "logical_size must be within the restore-manifest body bound",
            });
        }
        if self.content_type != RESTORE_MANIFEST_CONTENT_TYPE {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "content_type must be application/json",
            });
        }
        if self.body_digest_uri.len() != 71
            || !self.body_digest_uri.starts_with("sha256:")
            || !self.body_digest_uri[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "body_digest_uri must be canonical lowercase SHA-256",
            });
        }
        Ok(())
    }
}

impl RestoreSourceCommitSeal {
    fn validate(&self, phase: RestorePhase) -> Result<(), RestoreRecordError> {
        validate_commit_digest_field("source_commit.content_digest_uri", &self.content_digest_uri)?;
        validate_commit_digest_field(
            "source_commit.manifest_digest_uri",
            &self.manifest_digest_uri,
        )?;
        validate_bounded_count("source_commit.member_count", self.member_count)?;
        validate_bounded_count(
            "source_commit.unique_revision_count",
            self.unique_revision_count,
        )?;
        validate_bounded_count(
            "source_commit.generic_index_count",
            self.generic_index_count,
        )?;
        if (self.member_count == 0) != (self.member_digest == [0; SHA256_BYTES])
            || (self.unique_revision_count == 0) != (self.revision_digest == [0; SHA256_BYTES])
            || (self.generic_index_count == 0) != (self.generic_index_digest == [0; SHA256_BYTES])
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "source commit count and immutable closure digest are inconsistent",
            });
        }
        Ok(())
    }
}

impl RestoreManifestPublication {
    fn validate(&self) -> Result<(), RestoreRecordError> {
        validate_digest_uri("manifest.body_digest_uri", &self.body_digest_uri)?;
        validate_digest_uri("manifest.manifest_digest_uri", &self.manifest_digest_uri)?;
        if self.logical_size == 0 {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "published logical_size must be non-zero",
            });
        }
        if self.content_type != RESTORE_MANIFEST_CONTENT_TYPE {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "published content_type must be application/json",
            });
        }
        Ok(())
    }
}

impl RestoreDestinationManifests {
    fn validate(&self) -> Result<(), RestoreRecordError> {
        self.run_manifest.validate()?;
        self.restore_manifest.validate()?;
        if self.restore_manifest.logical_size > MAX_RESTORE_MANIFEST_BYTES as u64 {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "published restore manifest exceeds its body bound",
            });
        }
        if self.run_manifest.publication_operation_id
            == self.restore_manifest.publication_operation_id
            || self.run_manifest.artifact_revision_id == self.restore_manifest.artifact_revision_id
        {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "run and restore manifests must use distinct publication operations and revisions",
            });
        }
        Ok(())
    }
}

impl RestoreDestinationBinding {
    fn validate(&self) -> Result<(), RestoreRecordError> {
        validate_digest_uri(
            "destination_binding.effective_content_digest_uri",
            &self.effective_content_digest_uri,
        )?;
        if self.destination_projection_input_digest == [0; SHA256_BYTES] {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: RestorePhase::SourceSealed,
                reason: "destination projection input digest must be non-zero",
            });
        }
        if self.run_manifest_identity.publication_operation_id
            == self.restore_manifest_identity.publication_operation_id
            || self.run_manifest_identity.artifact_revision_id
                == self.restore_manifest_identity.artifact_revision_id
        {
            return Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "run and restore manifest identities must use distinct publication operations and revisions",
            });
        }
        if let Some(manifests) = &self.manifests {
            manifests.validate()?;
            for (identity, publication) in [
                (&self.run_manifest_identity, &manifests.run_manifest),
                (&self.restore_manifest_identity, &manifests.restore_manifest),
            ] {
                if identity.publication_operation_id != publication.publication_operation_id
                    || identity.artifact_revision_id != publication.artifact_revision_id
                {
                    return Err(RestoreRecordError::InvalidManifestDescriptor {
                        reason: "published destination manifest must match its expected identity",
                    });
                }
            }
        }
        Ok(())
    }
}

impl RestoreCommitClosureProgress {
    fn validate(&self, phase: RestorePhase) -> Result<(), RestoreRecordError> {
        for (field, count) in [
            ("commit_member_count", self.member_count),
            ("commit_revision_ref_count", self.revision_ref_count),
            ("commit_revision_seal_count", self.revision_seal_count),
        ] {
            validate_bounded_count(field, count)?;
        }
        if self.member_cursor.is_some() != (self.member_count > 0) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit member cursor must match non-empty member progress",
            });
        }
        if (self.member_count == 0) != (self.member_digest == [0; SHA256_BYTES]) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit member count and rolling digest are inconsistent",
            });
        }
        if self.generic_index_count == 0 && self.generic_index_cursor.is_some() {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit Generic index cursor requires non-empty progress",
            });
        }
        if (self.generic_index_count == 0) != (self.generic_index_digest == [0; SHA256_BYTES]) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit Generic index count and digest are inconsistent",
            });
        }
        if (self.generic_index_count > 0 || self.generic_indexes_complete)
            && !self.path_members_complete
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit Generic index build requires path EOF",
            });
        }
        if self
            .member_seal
            .is_some_and(|seal| seal != self.member_digest)
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit member seal must equal the rolling digest",
            });
        }
        if self.member_seal.is_some()
            && (!self.path_members_complete || !self.generic_indexes_complete)
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit member seal requires both destination member closures",
            });
        }
        if self.revision_cursor.is_some() != (self.revision_seal_count > 0) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit revision cursor must match non-empty sealing progress",
            });
        }
        if self.revision_seal_count > self.revision_ref_count {
            return Err(RestoreRecordError::CursorOutOfRange {
                field: "commit revision seal",
                cursor: self.revision_seal_count,
                member_count: self.revision_ref_count,
            });
        }
        if (self.revision_seal_count == 0) != (self.revision_digest == [0; SHA256_BYTES]) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit revision seal count and rolling digest are inconsistent",
            });
        }
        if self.revision_seal.is_some_and(|seal| {
            seal != self.revision_digest || self.revision_seal_count != self.revision_ref_count
        }) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit revision seal requires the complete revision closure",
            });
        }
        if self
            .parent_seal
            .is_some_and(|seal| seal != self.parent_digest)
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase,
                reason: "commit parent seal must equal the single-parent digest",
            });
        }
        if self.cleanup_member_count > self.member_count {
            return Err(RestoreRecordError::CursorOutOfRange {
                field: "commit member cleanup",
                cursor: self.cleanup_member_count,
                member_count: self.member_count,
            });
        }
        if self.cleanup_generic_index_count > self.generic_index_count {
            return Err(RestoreRecordError::CursorOutOfRange {
                field: "commit Generic index cleanup",
                cursor: self.cleanup_generic_index_count,
                member_count: self.generic_index_count,
            });
        }
        if self.cleanup_revision_count > self.revision_ref_count {
            return Err(RestoreRecordError::CursorOutOfRange {
                field: "commit revision cleanup",
                cursor: self.cleanup_revision_count,
                member_count: self.revision_ref_count,
            });
        }
        Ok(())
    }
}

/// Frozen source identity for a same-root restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreSource {
    Snapshot {
        snapshot_id: SnapshotId,
        read_version: ReadVersion,
    },
    Commit {
        commit_id: CommitId,
    },
}

impl RestoreSource {
    pub const fn kind(self) -> RestoreSourceKind {
        match self {
            Self::Snapshot { .. } => RestoreSourceKind::Snapshot,
            Self::Commit { .. } => RestoreSourceKind::Commit,
        }
    }
}

/// Stable terminal result retained for response-loss replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreResult {
    pub destination_workspace_incarnation_id: WorkspaceIncarnationId,
    pub destination_workspace_revision: WorkspaceRevision,
    pub member_count: u64,
    pub member_digest: [u8; SHA256_BYTES],
}

/// Immutable destination commit receipt projected only from a terminal v5
/// restore operation. Callers must not reconstruct this from the later live
/// Workbench head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreDestinationCommitReceipt {
    pub destination_commit_id: CommitId,
    pub destination_head_generation: Generation,
}

/// Stable class of a terminal restore failure.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreTerminalErrorKind {
    SourceExpired = 1,
    SourceConflict = 2,
    DestinationConflict = 3,
    InitializationMismatch = 4,
    AbortedByCaller = 5,
    InvariantViolation = 6,
    CleanupFailed = 7,
}

impl TryFrom<u8> for RestoreTerminalErrorKind {
    type Error = RestoreRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::SourceExpired),
            2 => Ok(Self::SourceConflict),
            3 => Ok(Self::DestinationConflict),
            4 => Ok(Self::InitializationMismatch),
            5 => Ok(Self::AbortedByCaller),
            6 => Ok(Self::InvariantViolation),
            7 => Ok(Self::CleanupFailed),
            value => Err(RestoreRecordError::UnknownDiscriminant {
                type_name: "RestoreTerminalErrorKind",
                value,
            }),
        }
    }
}

impl From<RestoreTerminalErrorKind> for u8 {
    fn from(value: RestoreTerminalErrorKind) -> Self {
        value as u8
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreTerminalError {
    pub kind: RestoreTerminalErrorKind,
    pub message: String,
    pub evidence_digest: Option<[u8; SHA256_BYTES]>,
}

/// Recoverable restore operation and its ordered closure progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreOperationRecord {
    pub operation_id: OperationId,
    pub identity_digest: [u8; SHA256_BYTES],
    /// Bound exactly once when the already-published staging manifest is
    /// accepted. It remains absent through source copy and sealing.
    pub initialization_digest: Option<[u8; SHA256_BYTES]>,
    pub source_workbench_id: WorkbenchId,
    pub source_workspace_incarnation_id: WorkspaceIncarnationId,
    pub source: RestoreSource,
    pub destination_workbench_id: WorkbenchId,
    pub destination_workspace_incarnation_id: WorkspaceIncarnationId,
    /// Present for every v5 row and absent only on historical terminal v4
    /// status rows whose publication authority was never recorded.
    pub destination_restore_manifest_identity: Option<RestoreManifestIdentity>,
    pub restore_manifest: RestoreManifestDescriptor,
    pub commit_provenance: RestoreCommitProvenance,
    pub phase: RestorePhase,
    /// Last raw source path folded into the complete frozen source scan.
    pub source_cursor: Option<NormalizedRelativePath>,
    /// The raw path scan reaches EOF before Generic index pointers are copied.
    pub source_paths_eof: bool,
    pub source_generic_index_cursor: Option<NormalizedRelativePath>,
    pub source_generic_index_count: u64,
    pub source_generic_index_rolling_digest: [u8; SHA256_BYTES],
    pub source_generic_index_seal: Option<[u8; SHA256_BYTES]>,
    pub source_generic_indexes_match_base_commit: Option<bool>,
    pub source_eof: bool,
    pub source_member_count: u64,
    pub source_member_rolling_digest: [u8; SHA256_BYTES],
    pub source_member_seal: Option<[u8; SHA256_BYTES]>,
    /// Whether the sealed raw snapshot closure exactly equals the frozen base
    /// commit. Snapshot restores may legitimately persist `Some(false)`.
    pub source_matches_base_commit: Option<bool>,
    /// Dense ordinary destination closure. RestoreMember rows use this
    /// sequence and exclude both source-owned and destination-owned manifests.
    pub next_member_sequence: u64,
    pub member_rolling_digest: [u8; SHA256_BYTES],
    /// Present after the source closure reaches EOF.
    pub member_seal: Option<[u8; SHA256_BYTES]>,
    /// Number of contiguous members removed by abort cleanup.
    pub cleanup_member_cursor: u64,
    pub cleanup_generic_index_cursor: u64,
    pub result: Option<RestoreResult>,
    pub terminal_error: Option<RestoreTerminalError>,
}

/// One exact destination row in canonical source order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreMemberRecord {
    pub destination_path: NormalizedRelativePath,
    pub artifact_revision_id: ArtifactRevisionId,
    pub path_generation: Generation,
    pub row_digest: [u8; SHA256_BYTES],
}

/// Validated restore operation transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreTransition {
    BeginCopying,
    SealSource {
        source_member_seal: [u8; SHA256_BYTES],
    },
    BindDestination {
        binding: RestoreDestinationBinding,
    },
    BeginDestinationBuilding {
        initialization_digest: [u8; SHA256_BYTES],
        manifests: RestoreDestinationManifests,
    },
    BeginDestinationSealing {
        member_seal: [u8; SHA256_BYTES],
    },
    MarkReady {
        revision_seal: [u8; SHA256_BYTES],
        parent_seal: [u8; SHA256_BYTES],
    },
    Complete {
        result: RestoreResult,
        destination_head_generation: Generation,
    },
    BeginAbort {
        terminal_error: RestoreTerminalError,
    },
    BeginCleaning,
    FinishCleanup,
    Quarantine {
        terminal_error: RestoreTerminalError,
    },
}

/// Strict restore payload encode, decode, or state-machine failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreRecordError {
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
    FieldTooLong {
        field: &'static str,
        length: usize,
        max: usize,
    },
    ZeroScalar {
        field: &'static str,
    },
    CountOutOfRange {
        field: &'static str,
        value: u64,
        max: u64,
    },
    CursorOutOfRange {
        field: &'static str,
        cursor: u64,
        member_count: u64,
    },
    LegacyNonterminalRequiresUpgrade {
        phase: RestorePhase,
    },
    LegacyProvenanceMissing {
        phase: RestorePhase,
    },
    InvalidPhasePayload {
        phase: RestorePhase,
        reason: &'static str,
    },
    InvalidManifestDescriptor {
        reason: &'static str,
    },
    InvalidPhaseTransition {
        from: RestorePhase,
        to: RestorePhase,
    },
    PhaseMismatch {
        expected: RestorePhase,
        actual: RestorePhase,
    },
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
}

impl fmt::Display for RestoreRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported restore value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidWorkbenchId { reason } => {
                write!(formatter, "invalid destination workbench id: {reason}")
            }
            Self::InvalidPath { reason } => write!(formatter, "invalid restore path: {reason}"),
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::FieldTooLong { field, length, max } => {
                write!(formatter, "{field} is {length} bytes, maximum is {max}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::CountOutOfRange { field, value, max } => {
                write!(formatter, "{field} {value} exceeds maximum {max}")
            }
            Self::CursorOutOfRange {
                field,
                cursor,
                member_count,
            } => write!(
                formatter,
                "restore {field} cursor {cursor} exceeds count {member_count}"
            ),
            Self::LegacyNonterminalRequiresUpgrade { phase } => write!(
                formatter,
                "restore-operation v4 phase {phase:?} is nonterminal and blocks schema upgrade"
            ),
            Self::LegacyProvenanceMissing { phase } => write!(
                formatter,
                "restore-operation v4 phase {phase:?} has no destination commit provenance"
            ),
            Self::InvalidPhasePayload { phase, reason } => {
                write!(formatter, "invalid {phase:?} restore payload: {reason}")
            }
            Self::InvalidManifestDescriptor { reason } => {
                write!(formatter, "invalid restore manifest descriptor: {reason}")
            }
            Self::InvalidPhaseTransition { from, to } => {
                write!(formatter, "invalid restore transition {from:?} -> {to:?}")
            }
            Self::PhaseMismatch { expected, actual } => {
                write!(
                    formatter,
                    "restore phase mismatch: expected {expected:?}, actual {actual:?}"
                )
            }
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(formatter, "restore value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for RestoreRecordError {}

impl RestoreOperationRecord {
    pub fn validate(&self) -> Result<(), RestoreRecordError> {
        if self.operation_id.as_bytes() != &self.identity_digest[..OperationId::BYTE_WIDTH] {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "operation id must equal the identity digest prefix",
            });
        }
        validate_bounded_count("source_member_count", self.source_member_count)?;
        validate_bounded_count(
            "source_generic_index_count",
            self.source_generic_index_count,
        )?;
        validate_bounded_count("next_member_sequence", self.next_member_sequence)?;
        if self.cleanup_member_cursor > self.next_member_sequence {
            return Err(RestoreRecordError::CursorOutOfRange {
                field: "materialized member cleanup",
                cursor: self.cleanup_member_cursor,
                member_count: self.next_member_sequence,
            });
        }
        if self.cleanup_generic_index_cursor > self.source_generic_index_count {
            return Err(RestoreRecordError::CursorOutOfRange {
                field: "materialized Generic index cleanup",
                cursor: self.cleanup_generic_index_cursor,
                member_count: self.source_generic_index_count,
            });
        }
        self.restore_manifest.validate()?;
        if self.source_workbench_id == self.destination_workbench_id {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source and destination workbenches must differ",
            });
        }
        if self.source_cursor.is_some() != (self.source_member_count > 0) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source cursor presence must match non-empty raw source progress",
            });
        }
        if self.source_generic_index_count == 0 && self.source_generic_index_cursor.is_some() {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source Generic index cursor requires non-empty progress",
            });
        }
        if (self.source_generic_index_count == 0)
            != (self.source_generic_index_rolling_digest == [0; SHA256_BYTES])
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source Generic index count and digest are inconsistent",
            });
        }
        if (self.source_generic_index_count > 0 || self.source_generic_index_seal.is_some())
            && !self.source_paths_eof
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source Generic index copying requires path EOF",
            });
        }
        if self
            .source_generic_index_seal
            .is_some_and(|seal| seal != self.source_generic_index_rolling_digest)
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source Generic index seal must equal its rolling digest",
            });
        }
        if self.source_eof != (self.source_paths_eof && self.source_generic_index_seal.is_some()) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source EOF requires both path and Generic index closures",
            });
        }
        if self.next_member_sequence > self.source_member_count {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "materialized closure cannot contain rows absent from the raw source",
            });
        }
        if (self.source_member_count == 0)
            != (self.source_member_rolling_digest == [0; SHA256_BYTES])
            || (self.next_member_sequence == 0) != (self.member_rolling_digest == [0; SHA256_BYTES])
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "restore closure counts and rolling digests are inconsistent",
            });
        }
        if self
            .source_member_seal
            .is_some_and(|seal| seal != self.source_member_rolling_digest)
            || self
                .member_seal
                .is_some_and(|seal| seal != self.member_rolling_digest)
        {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "restore closure seals must equal their rolling digests",
            });
        }
        if let Some(error) = &self.terminal_error {
            validate_terminal_error(error)?;
        }
        if let Some(result) = &self.result {
            if result.destination_workspace_incarnation_id
                != self.destination_workspace_incarnation_id
                || result.member_count != self.next_member_sequence
                || Some(result.member_digest) != self.member_seal
            {
                return Err(RestoreRecordError::InvalidPhasePayload {
                    phase: self.phase,
                    reason: "terminal result does not match the sealed destination closure",
                });
            }
        }
        match &self.commit_provenance {
            RestoreCommitProvenance::MissingLegacyV4 => validate_legacy_restore_phase(self),
            RestoreCommitProvenance::V5(provenance) => {
                let destination_restore_manifest_identity = self
                    .destination_restore_manifest_identity
                    .as_ref()
                    .ok_or(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "v5 restore requires the expected restore-manifest identity",
                    })?;
                if destination_restore_manifest_identity.publication_operation_id
                    == self.operation_id
                {
                    return Err(RestoreRecordError::InvalidManifestDescriptor {
                        reason: "restore and manifest publication operations must differ",
                    });
                }
                let RestoreCommitProvenanceV5 {
                    source_commit,
                    destination_committed_at_unix_seconds,
                    destination_binding,
                    closure,
                    destination_head_generation,
                } = provenance.as_ref();
                source_commit.validate(self.phase)?;
                if matches!(
                    self.source,
                    RestoreSource::Commit { commit_id } if commit_id != source_commit.commit_id
                ) {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "commit restore selector must equal the frozen source commit",
                    });
                }
                let source_matches_base_commit = self.source_member_count
                    == source_commit.member_count
                    && self.source_member_rolling_digest == source_commit.member_digest;
                if self.source_member_seal.is_some() != self.source_matches_base_commit.is_some()
                    || self
                        .source_matches_base_commit
                        .is_some_and(|matches| matches != source_matches_base_commit)
                {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason:
                            "source/base-commit match projection must equal the sealed raw closure",
                    });
                }
                if matches!(self.source, RestoreSource::Commit { .. })
                    && self.source_matches_base_commit == Some(false)
                {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason:
                            "commit-source restore must exactly match its immutable commit closure",
                    });
                }
                let source_generic_indexes_match_base_commit = self.source_generic_index_count
                    == source_commit.generic_index_count
                    && self.source_generic_index_rolling_digest
                        == source_commit.generic_index_digest;
                if self.source_generic_index_seal.is_some()
                    != self.source_generic_indexes_match_base_commit.is_some()
                    || self
                        .source_generic_indexes_match_base_commit
                        .is_some_and(|matches| matches != source_generic_indexes_match_base_commit)
                {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason:
                            "source/base-commit Generic index match must equal the sealed closure",
                    });
                }
                if matches!(self.source, RestoreSource::Commit { .. })
                    && self.source_generic_indexes_match_base_commit == Some(false)
                {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason:
                            "commit-source restore must exactly match its Generic index closure",
                    });
                }
                if *destination_committed_at_unix_seconds == 0 {
                    return Err(RestoreRecordError::ZeroScalar {
                        field: "destination_committed_at_unix_seconds",
                    });
                }
                if let Some(binding) = destination_binding {
                    binding.validate()?;
                    if binding.destination_commit_id == source_commit.commit_id {
                        return Err(RestoreRecordError::InvalidPhasePayload {
                            phase: self.phase,
                            reason: "destination commit must not reuse the source commit identity",
                        });
                    }
                    if self.source_matches_base_commit.is_some_and(|matches| {
                        matches
                            != (binding.effective_content_digest_uri
                                == source_commit.content_digest_uri)
                    }) {
                        return Err(RestoreRecordError::InvalidPhasePayload {
                            phase: self.phase,
                            reason: "effective content digest must preserve clean source content and distinguish dirty materialization",
                        });
                    }
                    if binding.restore_manifest_identity != *destination_restore_manifest_identity {
                        return Err(RestoreRecordError::InvalidManifestDescriptor {
                            reason: "late bind must repeat the begin restore-manifest identity",
                        });
                    }
                    if binding.run_manifest_identity.publication_operation_id == self.operation_id {
                        return Err(RestoreRecordError::InvalidManifestDescriptor {
                            reason: "restore and manifest publication operations must differ",
                        });
                    }
                    if let Some(manifests) = &binding.manifests {
                        if manifests.run_manifest.workspace_incarnation_id
                            != self.destination_workspace_incarnation_id
                            || manifests.restore_manifest.workspace_incarnation_id
                                != self.destination_workspace_incarnation_id
                        {
                            return Err(RestoreRecordError::InvalidManifestDescriptor {
                                reason: "published destination manifests must target the hidden destination incarnation",
                            });
                        }
                        if manifests.restore_manifest.body_digest_uri
                            != self.restore_manifest.body_digest_uri
                            || manifests.restore_manifest.logical_size
                                != self.restore_manifest.logical_size
                            || manifests.restore_manifest.content_type
                                != self.restore_manifest.content_type
                        {
                            return Err(RestoreRecordError::InvalidManifestDescriptor {
                                reason:
                                    "published restore manifest must match its begin descriptor",
                            });
                        }
                    }
                }
                closure.validate(self.phase)?;
                let expected_parent_digest = advance_commit_parent_rolling_digest(
                    [0; SHA256_BYTES],
                    0,
                    source_commit.commit_id,
                );
                if closure.parent_digest != expected_parent_digest {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "destination commit parent digest must bind only the source commit",
                    });
                }
                if self.phase == RestorePhase::Complete {
                    if destination_head_generation.is_none() {
                        return Err(RestoreRecordError::InvalidPhasePayload {
                            phase: self.phase,
                            reason: "complete restore requires the destination head generation",
                        });
                    }
                } else if destination_head_generation.is_some() {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "destination head generation is terminal-only",
                    });
                }
                validate_restore_phase(self, destination_binding.as_ref(), closure)
            }
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, RestoreRecordError> {
        self.validate()?;
        if matches!(
            self.commit_provenance,
            RestoreCommitProvenance::MissingLegacyV4
        ) {
            return Err(RestoreRecordError::LegacyProvenanceMissing { phase: self.phase });
        }
        let mut encoded = Vec::new();
        encoded.push(RESTORE_OPERATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.operation_id.as_bytes());
        encoded.extend_from_slice(&self.identity_digest);
        put_optional_fixed(&mut encoded, self.initialization_digest.as_ref());
        put_bytes(&mut encoded, self.source_workbench_id.as_bytes());
        encoded.extend_from_slice(self.source_workspace_incarnation_id.as_bytes());
        put_source(&mut encoded, self.source);
        put_bytes(&mut encoded, self.destination_workbench_id.as_bytes());
        encoded.extend_from_slice(self.destination_workspace_incarnation_id.as_bytes());
        let destination_restore_manifest_identity = self
            .destination_restore_manifest_identity
            .as_ref()
            .expect("validated v5 restore has its restore-manifest identity");
        put_manifest_identity(&mut encoded, destination_restore_manifest_identity);
        put_bytes(
            &mut encoded,
            self.restore_manifest.body_digest_uri.as_bytes(),
        );
        encoded.extend_from_slice(&self.restore_manifest.logical_size.to_be_bytes());
        put_bytes(&mut encoded, self.restore_manifest.content_type.as_bytes());
        put_commit_provenance(&mut encoded, &self.commit_provenance)?;
        encoded.push(self.phase.into());
        put_optional_path(&mut encoded, self.source_cursor.as_ref());
        encoded.push(u8::from(self.source_paths_eof));
        put_optional_path(&mut encoded, self.source_generic_index_cursor.as_ref());
        encoded.extend_from_slice(&self.source_generic_index_count.to_be_bytes());
        encoded.extend_from_slice(&self.source_generic_index_rolling_digest);
        put_optional_fixed(&mut encoded, self.source_generic_index_seal.as_ref());
        put_optional_boolean(&mut encoded, self.source_generic_indexes_match_base_commit);
        encoded.push(u8::from(self.source_eof));
        encoded.extend_from_slice(&self.source_member_count.to_be_bytes());
        encoded.extend_from_slice(&self.source_member_rolling_digest);
        put_optional_fixed(&mut encoded, self.source_member_seal.as_ref());
        put_optional_boolean(&mut encoded, self.source_matches_base_commit);
        encoded.extend_from_slice(&self.next_member_sequence.to_be_bytes());
        encoded.extend_from_slice(&self.member_rolling_digest);
        put_optional_fixed(&mut encoded, self.member_seal.as_ref());
        encoded.extend_from_slice(&self.cleanup_member_cursor.to_be_bytes());
        encoded.extend_from_slice(&self.cleanup_generic_index_cursor.to_be_bytes());
        put_optional_result(&mut encoded, self.result.as_ref());
        put_optional_terminal_error(&mut encoded, self.terminal_error.as_ref())?;
        Ok(encoded)
    }

    pub fn destination_commit_receipt(
        &self,
    ) -> Result<RestoreDestinationCommitReceipt, RestoreRecordError> {
        self.validate()?;
        if self.phase != RestorePhase::Complete {
            return Err(RestoreRecordError::PhaseMismatch {
                expected: RestorePhase::Complete,
                actual: self.phase,
            });
        }
        let RestoreCommitProvenance::V5(provenance) = &self.commit_provenance else {
            return Err(RestoreRecordError::LegacyProvenanceMissing { phase: self.phase });
        };
        let destination_head_generation = provenance.destination_head_generation.ok_or(
            RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "complete restore requires the destination head generation",
            },
        )?;
        Ok(RestoreDestinationCommitReceipt {
            destination_commit_id: provenance
                .destination_binding
                .as_ref()
                .ok_or(RestoreRecordError::InvalidPhasePayload {
                    phase: self.phase,
                    reason: "complete restore requires bound destination authority",
                })?
                .destination_commit_id,
            destination_head_generation,
        })
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RestoreRecordError> {
        match encoded.first().copied() {
            Some(RESTORE_OPERATION_VALUE_FORMAT_VERSION) => {
                Self::decode_versioned(encoded, RESTORE_OPERATION_VALUE_FORMAT_VERSION)
            }
            Some(LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION) => {
                Self::decode_versioned(encoded, LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION)
            }
            Some(RESTORE_MEMBER_VALUE_FORMAT_VERSION) => decode_legacy_v4_operation(encoded),
            Some(actual) => Err(RestoreRecordError::UnsupportedValueVersion {
                actual,
                expected: RESTORE_OPERATION_VALUE_FORMAT_VERSION,
            }),
            None => Err(RestoreRecordError::Truncated {
                field: "value_format_version",
                needed: 1,
                remaining: 0,
            }),
        }
    }

    fn decode_versioned(encoded: &[u8], value_version: u8) -> Result<Self, RestoreRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version(value_version)?;
        let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
        let identity_digest = decoder.fixed("identity_digest")?;
        let initialization_digest = decoder.optional_fixed("initialization_digest")?;
        let source_workbench_id = decoder.workbench_id("source_workbench_id")?;
        let source_workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes(decoder.fixed("source_workspace_incarnation_id")?);
        let source = decoder.source()?;
        let destination_workbench_id = decoder.workbench_id("destination_workbench_id")?;
        let destination_workspace_incarnation_id = WorkspaceIncarnationId::from_bytes(
            decoder.fixed("destination_workspace_incarnation_id")?,
        );
        let destination_restore_manifest_identity =
            Some(decoder.manifest_identity("destination_restore_manifest_identity")?);
        let restore_manifest = RestoreManifestDescriptor {
            body_digest_uri: decoder.string("restore_manifest.body_digest_uri")?,
            logical_size: decoder.u64("restore_manifest.logical_size")?,
            content_type: decoder.string("restore_manifest.content_type")?,
        };
        let commit_provenance = decoder.commit_provenance(value_version)?;
        let phase = decode_durable_enum(decoder.u8("phase")?)?;
        let source_cursor = decoder.optional_path("source_cursor")?;
        let (
            source_paths_eof,
            source_generic_index_cursor,
            source_generic_index_count,
            source_generic_index_rolling_digest,
            source_generic_index_seal,
            source_generic_indexes_match_base_commit,
        ) = if value_version == RESTORE_OPERATION_VALUE_FORMAT_VERSION {
            (
                decoder.boolean("source_paths_eof")?,
                decoder.optional_path("source_generic_index_cursor")?,
                decoder.u64("source_generic_index_count")?,
                decoder.fixed("source_generic_index_rolling_digest")?,
                decoder.optional_fixed("source_generic_index_seal")?,
                decoder.optional_boolean("source_generic_indexes_match_base_commit")?,
            )
        } else {
            (false, None, 0, [0; SHA256_BYTES], None, None)
        };
        let source_eof = decoder.boolean("source_eof")?;
        let (source_paths_eof, source_generic_index_seal, source_generic_indexes_match_base_commit) =
            if value_version == LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION {
                (
                    source_eof,
                    source_eof.then_some([0; SHA256_BYTES]),
                    source_eof.then_some(true),
                )
            } else {
                (
                    source_paths_eof,
                    source_generic_index_seal,
                    source_generic_indexes_match_base_commit,
                )
            };
        let source_member_count = decoder.u64("source_member_count")?;
        let source_member_rolling_digest = decoder.fixed("source_member_rolling_digest")?;
        let source_member_seal = decoder.optional_fixed("source_member_seal")?;
        let source_matches_base_commit = decoder.optional_boolean("source_matches_base_commit")?;
        let next_member_sequence = decoder.u64("next_member_sequence")?;
        let member_rolling_digest = decoder.fixed("member_rolling_digest")?;
        let member_seal = decoder.optional_fixed("member_seal")?;
        let cleanup_member_cursor = decoder.u64("cleanup_member_cursor")?;
        let cleanup_generic_index_cursor =
            if value_version == RESTORE_OPERATION_VALUE_FORMAT_VERSION {
                decoder.u64("cleanup_generic_index_cursor")?
            } else {
                0
            };
        let result = decoder.optional_result("result")?;
        let terminal_error = decoder.optional_terminal_error("terminal_error")?;
        decoder.finish()?;
        let record = Self {
            operation_id,
            identity_digest,
            initialization_digest,
            source_workbench_id,
            source_workspace_incarnation_id,
            source,
            destination_workbench_id,
            destination_workspace_incarnation_id,
            destination_restore_manifest_identity,
            restore_manifest,
            commit_provenance,
            phase,
            source_cursor,
            source_paths_eof,
            source_generic_index_cursor,
            source_generic_index_count,
            source_generic_index_rolling_digest,
            source_generic_index_seal,
            source_generic_indexes_match_base_commit,
            source_eof,
            source_member_count,
            source_member_rolling_digest,
            source_member_seal,
            source_matches_base_commit,
            next_member_sequence,
            member_rolling_digest,
            member_seal,
            cleanup_member_cursor,
            cleanup_generic_index_cursor,
            result,
            terminal_error,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn apply(
        &self,
        expected: RestorePhase,
        transition: RestoreTransition,
    ) -> Result<Self, RestoreRecordError> {
        self.validate()?;
        if self.phase != expected {
            return Err(RestoreRecordError::PhaseMismatch {
                expected,
                actual: self.phase,
            });
        }
        let mut next = self.clone();
        match transition {
            RestoreTransition::BeginCopying if expected == RestorePhase::Preparing => {
                next.phase = RestorePhase::Copying;
            }
            RestoreTransition::SealSource { source_member_seal }
                if expected == RestorePhase::Copying && self.source_eof =>
            {
                if source_member_seal != self.source_member_rolling_digest {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "raw source seal does not match its rolling digest",
                    });
                }
                next.phase = RestorePhase::SourceSealed;
                next.source_member_seal = Some(source_member_seal);
                next.member_seal = Some(next.member_rolling_digest);
                let RestoreCommitProvenance::V5(provenance) = &next.commit_provenance else {
                    return Err(RestoreRecordError::LegacyProvenanceMissing { phase: expected });
                };
                next.source_matches_base_commit = Some(
                    next.source_member_count == provenance.source_commit.member_count
                        && source_member_seal == provenance.source_commit.member_digest,
                );
            }
            RestoreTransition::BindDestination { binding }
                if expected == RestorePhase::SourceSealed =>
            {
                if binding.manifests.is_some() {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "late destination bind cannot claim unpublished manifests",
                    });
                }
                let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
                    return Err(RestoreRecordError::LegacyProvenanceMissing { phase: expected });
                };
                if provenance.destination_binding.is_some() {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "destination authority was already bound",
                    });
                }
                provenance.destination_binding = Some(binding);
            }
            RestoreTransition::BeginDestinationBuilding {
                initialization_digest,
                manifests,
            } if expected == RestorePhase::SourceSealed && self.initialization_digest.is_none() => {
                let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
                    return Err(RestoreRecordError::LegacyProvenanceMissing { phase: expected });
                };
                let Some(binding) = &mut provenance.destination_binding else {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "destination authority must be bound before commit construction",
                    });
                };
                if binding.manifests.is_some() {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "destination manifests were already installed",
                    });
                }
                binding.manifests = Some(manifests);
                next.phase = RestorePhase::DestinationBuilding;
                next.initialization_digest = Some(initialization_digest);
            }
            RestoreTransition::BeginDestinationSealing { member_seal }
                if expected == RestorePhase::DestinationBuilding =>
            {
                let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
                    return Err(RestoreRecordError::LegacyProvenanceMissing { phase: expected });
                };
                let closure = &mut provenance.closure;
                if member_seal != closure.member_digest {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "destination commit member seal does not match its rolling digest",
                    });
                }
                closure.member_seal = Some(member_seal);
                next.phase = RestorePhase::DestinationSealing;
            }
            RestoreTransition::MarkReady {
                revision_seal,
                parent_seal,
            } if expected == RestorePhase::DestinationSealing => {
                let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
                    return Err(RestoreRecordError::LegacyProvenanceMissing { phase: expected });
                };
                let closure = &mut provenance.closure;
                if revision_seal != closure.revision_digest
                    || closure.revision_seal_count != closure.revision_ref_count
                    || parent_seal != closure.parent_digest
                {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "destination commit closure seals do not match durable progress",
                    });
                }
                closure.revision_seal = Some(revision_seal);
                closure.parent_seal = Some(parent_seal);
                next.phase = RestorePhase::Ready;
            }
            RestoreTransition::Complete {
                result,
                destination_head_generation,
            } if expected == RestorePhase::Ready => {
                let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
                    return Err(RestoreRecordError::LegacyProvenanceMissing { phase: expected });
                };
                next.phase = RestorePhase::Complete;
                next.result = Some(result);
                provenance.destination_head_generation = Some(destination_head_generation);
            }
            RestoreTransition::BeginAbort { terminal_error }
                if matches!(
                    expected,
                    RestorePhase::Preparing
                        | RestorePhase::Copying
                        | RestorePhase::SourceSealed
                        | RestorePhase::DestinationBuilding
                        | RestorePhase::DestinationSealing
                        | RestorePhase::Ready
                ) =>
            {
                next.phase = RestorePhase::Aborting;
                next.member_seal = self.member_seal;
                next.terminal_error = Some(terminal_error);
            }
            RestoreTransition::BeginCleaning if expected == RestorePhase::Aborting => {
                next.phase = RestorePhase::Cleaning;
            }
            RestoreTransition::FinishCleanup
                if expected == RestorePhase::Cleaning
                    && self.cleanup_member_cursor == self.next_member_sequence =>
            {
                if !commit_cleanup_complete(&self.commit_provenance) {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "destination commit scaffolding cleanup is incomplete",
                    });
                }
                next.phase = RestorePhase::Cleaned;
            }
            RestoreTransition::Quarantine { terminal_error }
                if matches!(expected, RestorePhase::Aborting | RestorePhase::Cleaning) =>
            {
                next.phase = RestorePhase::Quarantined;
                next.terminal_error = Some(terminal_error);
            }
            transition => {
                return Err(RestoreRecordError::InvalidPhaseTransition {
                    from: expected,
                    to: transition.target_phase(),
                });
            }
        }
        next.validate()?;
        Ok(next)
    }
}

impl RestoreTransition {
    fn target_phase(&self) -> RestorePhase {
        match self {
            Self::BeginCopying => RestorePhase::Copying,
            Self::SealSource { .. } => RestorePhase::SourceSealed,
            Self::BindDestination { .. } => RestorePhase::SourceSealed,
            Self::BeginDestinationBuilding { .. } => RestorePhase::DestinationBuilding,
            Self::BeginDestinationSealing { .. } => RestorePhase::DestinationSealing,
            Self::MarkReady { .. } => RestorePhase::Ready,
            Self::Complete { .. } => RestorePhase::Complete,
            Self::BeginAbort { .. } => RestorePhase::Aborting,
            Self::BeginCleaning => RestorePhase::Cleaning,
            Self::FinishCleanup => RestorePhase::Cleaned,
            Self::Quarantine { .. } => RestorePhase::Quarantined,
        }
    }
}

impl RestoreMemberRecord {
    pub fn encode(&self) -> Vec<u8> {
        let path = self.destination_path.as_str().as_bytes();
        let mut encoded = Vec::with_capacity(1 + 4 + path.len() + 16 + 8 + SHA256_BYTES);
        encoded.push(RESTORE_MEMBER_VALUE_FORMAT_VERSION);
        put_bytes(&mut encoded, path);
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        encoded.extend_from_slice(&self.path_generation.get().to_be_bytes());
        encoded.extend_from_slice(&self.row_digest);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RestoreRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version(RESTORE_MEMBER_VALUE_FORMAT_VERSION)?;
        let destination_path = decoder.path("destination_path")?;
        let artifact_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("artifact_revision_id")?);
        let path_generation = decoder.generation("path_generation")?;
        let row_digest = decoder.fixed("row_digest")?;
        decoder.finish()?;
        Ok(Self {
            destination_path,
            artifact_revision_id,
            path_generation,
            row_digest,
        })
    }
}

fn validate_restore_phase(
    record: &RestoreOperationRecord,
    destination_binding: Option<&RestoreDestinationBinding>,
    closure: &RestoreCommitClosureProgress,
) -> Result<(), RestoreRecordError> {
    let destination_manifests = destination_binding.and_then(|binding| binding.manifests.as_ref());
    let fail = |reason| {
        Err(RestoreRecordError::InvalidPhasePayload {
            phase: record.phase,
            reason,
        })
    };
    match record.phase {
        RestorePhase::Preparing => {
            if record.initialization_digest.is_some()
                || record.source_eof
                || record.source_paths_eof
                || record.source_generic_index_cursor.is_some()
                || record.source_generic_index_count != 0
                || record.source_generic_index_rolling_digest != [0; SHA256_BYTES]
                || record.source_generic_index_seal.is_some()
                || record.source_generic_indexes_match_base_commit.is_some()
                || record.source_member_count != 0
                || record.source_member_rolling_digest != [0; SHA256_BYTES]
                || record.source_member_seal.is_some()
                || record.source_matches_base_commit.is_some()
                || record.next_member_sequence != 0
                || record.member_seal.is_some()
                || destination_binding.is_some()
                || !commit_closure_is_pristine(closure)
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("preparing restore cannot have closure or terminal progress");
            }
        }
        RestorePhase::Copying => {
            if record.initialization_digest.is_some()
                || record.source_member_seal.is_some()
                || record.source_matches_base_commit.is_some()
                || record.member_seal.is_some()
                || destination_binding.is_some()
                || !commit_closure_is_pristine(closure)
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("copying restore cannot have a seal, cleanup, or terminal payload");
            }
        }
        RestorePhase::SourceSealed => {
            if record.initialization_digest.is_some()
                || !record.source_eof
                || record.source_member_seal != Some(record.source_member_rolling_digest)
                || record.source_matches_base_commit.is_none()
                || record.source_generic_index_seal
                    != Some(record.source_generic_index_rolling_digest)
                || record.source_generic_indexes_match_base_commit.is_none()
                || record.member_seal != Some(record.member_rolling_digest)
                || destination_manifests.is_some()
                || !commit_closure_is_pristine(closure)
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail(
                    "sealed source requires raw and materialized seals plus optional late binding",
                );
            }
        }
        RestorePhase::DestinationBuilding => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.source_member_seal != Some(record.source_member_rolling_digest)
                || record.source_matches_base_commit.is_none()
                || record.source_generic_index_seal
                    != Some(record.source_generic_index_rolling_digest)
                || record.source_generic_indexes_match_base_commit.is_none()
                || record.member_seal != Some(record.member_rolling_digest)
                || destination_binding.is_none()
                || destination_manifests.is_none()
                || closure.member_seal.is_some()
                || closure.revision_seal_count != 0
                || closure.revision_cursor.is_some()
                || closure.revision_seal.is_some()
                || closure.parent_seal.is_some()
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || closure.cleanup_member_count != 0
                || closure.cleanup_generic_index_count != 0
                || closure.cleanup_revision_count != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail(
                    "destination build requires staged manifests and unsealed commit progress",
                );
            }
        }
        RestorePhase::DestinationSealing => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.source_member_seal != Some(record.source_member_rolling_digest)
                || record.source_matches_base_commit.is_none()
                || record.source_generic_index_seal
                    != Some(record.source_generic_index_rolling_digest)
                || record.source_generic_indexes_match_base_commit.is_none()
                || record.member_seal != Some(record.member_rolling_digest)
                || destination_binding.is_none()
                || destination_manifests.is_none()
                || closure.member_seal != Some(closure.member_digest)
                || closure.revision_seal.is_some()
                || closure.parent_seal.is_some()
                || !commit_closure_matches_materialized(record, closure)
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || closure.cleanup_member_count != 0
                || closure.cleanup_generic_index_count != 0
                || closure.cleanup_revision_count != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("destination sealing requires the complete member seal only");
            }
        }
        RestorePhase::Ready => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.source_member_seal != Some(record.source_member_rolling_digest)
                || record.source_matches_base_commit.is_none()
                || record.source_generic_index_seal
                    != Some(record.source_generic_index_rolling_digest)
                || record.source_generic_indexes_match_base_commit.is_none()
                || record.member_seal != Some(record.member_rolling_digest)
                || destination_binding.is_none()
                || destination_manifests.is_none()
                || !commit_closure_is_sealed(closure)
                || !commit_closure_matches_materialized(record, closure)
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("ready restore requires all destination commit closures to be sealed");
            }
        }
        RestorePhase::Complete => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.source_member_seal != Some(record.source_member_rolling_digest)
                || record.source_matches_base_commit.is_none()
                || record.source_generic_index_seal
                    != Some(record.source_generic_index_rolling_digest)
                || record.source_generic_indexes_match_base_commit.is_none()
                || record.member_seal != Some(record.member_rolling_digest)
                || destination_binding.is_none()
                || destination_manifests.is_none()
                || !commit_closure_is_sealed(closure)
                || !commit_closure_matches_materialized(record, closure)
                || record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || record.result.is_none()
                || record.terminal_error.is_some()
            {
                return fail("complete restore requires one matching result and no cleanup");
            }
        }
        RestorePhase::Aborting => {
            if record.cleanup_member_cursor != 0
                || record.cleanup_generic_index_cursor != 0
                || closure.cleanup_member_count != 0
                || closure.cleanup_generic_index_count != 0
                || closure.cleanup_revision_count != 0
                || record.result.is_some()
                || record.terminal_error.is_none()
            {
                return fail("aborting restore requires a reason before cleanup starts");
            }
        }
        RestorePhase::Cleaning => {
            if record.result.is_some() || record.terminal_error.is_none() {
                return fail("cleaning restore requires a terminal error and no result");
            }
        }
        RestorePhase::Cleaned => {
            if record.cleanup_member_cursor != record.next_member_sequence
                || record.cleanup_generic_index_cursor != record.source_generic_index_count
                || !commit_cleanup_complete(&record.commit_provenance)
                || record.result.is_some()
                || record.terminal_error.is_none()
            {
                return fail("cleaned restore must remove its complete member closure");
            }
        }
        RestorePhase::Quarantined => {
            if record.result.is_some() || record.terminal_error.is_none() {
                return fail("quarantined restore requires reconciliation evidence");
            }
        }
    }
    Ok(())
}

fn validate_legacy_restore_phase(
    record: &RestoreOperationRecord,
) -> Result<(), RestoreRecordError> {
    if record.destination_restore_manifest_identity.is_some()
        || record.source_matches_base_commit.is_some()
    {
        return Err(RestoreRecordError::InvalidPhasePayload {
            phase: record.phase,
            reason: "legacy v4 status cannot claim v5 provenance",
        });
    }
    match record.phase {
        RestorePhase::Complete => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.source_member_seal != Some(record.source_member_rolling_digest)
                || record.member_seal != Some(record.member_rolling_digest)
                || record.cleanup_member_cursor != 0
                || record.result.is_none()
                || record.terminal_error.is_some()
            {
                return Err(RestoreRecordError::InvalidPhasePayload {
                    phase: record.phase,
                    reason: "legacy complete restore payload is inconsistent",
                });
            }
            Ok(())
        }
        RestorePhase::Cleaned => {
            if record.cleanup_member_cursor != record.next_member_sequence
                || record.result.is_some()
                || record.terminal_error.is_none()
            {
                return Err(RestoreRecordError::InvalidPhasePayload {
                    phase: record.phase,
                    reason: "legacy cleaned restore payload is inconsistent",
                });
            }
            Ok(())
        }
        phase => Err(RestoreRecordError::LegacyNonterminalRequiresUpgrade { phase }),
    }
}

fn commit_closure_is_pristine(closure: &RestoreCommitClosureProgress) -> bool {
    closure.member_cursor.is_none()
        && closure.member_count == 0
        && closure.member_digest == [0; SHA256_BYTES]
        && !closure.path_members_complete
        && closure.generic_index_cursor.is_none()
        && closure.generic_index_count == 0
        && closure.generic_index_digest == [0; SHA256_BYTES]
        && !closure.generic_indexes_complete
        && closure.member_seal.is_none()
        && closure.revision_ref_count == 0
        && closure.revision_cursor.is_none()
        && closure.revision_seal_count == 0
        && closure.revision_digest == [0; SHA256_BYTES]
        && closure.revision_seal.is_none()
        && closure.parent_seal.is_none()
        && closure.cleanup_member_count == 0
        && closure.cleanup_generic_index_count == 0
        && closure.cleanup_revision_count == 0
}

fn commit_closure_is_sealed(closure: &RestoreCommitClosureProgress) -> bool {
    closure.path_members_complete
        && closure.generic_indexes_complete
        && closure.member_seal == Some(closure.member_digest)
        && closure.revision_seal_count == closure.revision_ref_count
        && closure.revision_seal == Some(closure.revision_digest)
        && closure.parent_seal == Some(closure.parent_digest)
}

fn commit_closure_matches_materialized(
    record: &RestoreOperationRecord,
    closure: &RestoreCommitClosureProgress,
) -> bool {
    record
        .next_member_sequence
        .checked_add(2)
        .is_some_and(|final_count| closure.member_count == final_count)
        && closure.revision_ref_count >= 2
        && closure.revision_ref_count <= closure.member_count
        && closure.generic_index_count == record.source_generic_index_count
        && closure.generic_index_digest == record.source_generic_index_rolling_digest
}

fn commit_cleanup_complete(provenance: &RestoreCommitProvenance) -> bool {
    match provenance {
        RestoreCommitProvenance::MissingLegacyV4 => true,
        RestoreCommitProvenance::V5(provenance) => {
            provenance.closure.cleanup_member_count == provenance.closure.member_count
                && provenance.closure.cleanup_generic_index_count
                    == provenance.closure.generic_index_count
                && provenance.closure.cleanup_revision_count
                    == provenance.closure.revision_ref_count
        }
    }
}

fn validate_terminal_error(error: &RestoreTerminalError) -> Result<(), RestoreRecordError> {
    if error.message.is_empty() {
        return Err(RestoreRecordError::EmptyField {
            field: "terminal_error.message",
        });
    }
    if error.message.len() > MAX_RESTORE_TERMINAL_ERROR_BYTES {
        return Err(RestoreRecordError::FieldTooLong {
            field: "terminal_error.message",
            length: error.message.len(),
            max: MAX_RESTORE_TERMINAL_ERROR_BYTES,
        });
    }
    Ok(())
}

fn validate_bounded_count(field: &'static str, value: u64) -> Result<(), RestoreRecordError> {
    if value > MAX_RESTORE_MEMBERS {
        Err(RestoreRecordError::CountOutOfRange {
            field,
            value,
            max: MAX_RESTORE_MEMBERS,
        })
    } else {
        Ok(())
    }
}

fn validate_digest_uri(_field: &'static str, value: &str) -> Result<(), RestoreRecordError> {
    if value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(RestoreRecordError::InvalidManifestDescriptor {
            reason: "digest URI must be canonical lowercase SHA-256",
        })
    }
}

fn validate_commit_digest_field(
    field: &'static str,
    value: &str,
) -> Result<(), RestoreRecordError> {
    if value.is_empty() {
        return Err(RestoreRecordError::EmptyField { field });
    }
    if value.len() > MAX_COMMIT_DIGEST_URI_BYTES {
        return Err(RestoreRecordError::FieldTooLong {
            field,
            length: value.len(),
            max: MAX_COMMIT_DIGEST_URI_BYTES,
        });
    }
    if value.as_bytes().contains(&0) {
        return Err(RestoreRecordError::InvalidManifestDescriptor {
            reason: "source commit digest must not contain NUL",
        });
    }
    Ok(())
}

fn put_source(encoded: &mut Vec<u8>, source: RestoreSource) {
    encoded.push(source.kind().into());
    match source {
        RestoreSource::Snapshot {
            snapshot_id,
            read_version,
        } => {
            encoded.extend_from_slice(&snapshot_id.get().to_be_bytes());
            encoded.extend_from_slice(&read_version.get().to_be_bytes());
        }
        RestoreSource::Commit { commit_id } => {
            encoded.extend_from_slice(commit_id.as_bytes());
        }
    }
}

fn put_commit_provenance(
    encoded: &mut Vec<u8>,
    provenance: &RestoreCommitProvenance,
) -> Result<(), RestoreRecordError> {
    let RestoreCommitProvenance::V5(provenance) = provenance else {
        return Err(RestoreRecordError::LegacyProvenanceMissing {
            phase: RestorePhase::Complete,
        });
    };
    let RestoreCommitProvenanceV5 {
        source_commit,
        destination_committed_at_unix_seconds,
        destination_binding,
        closure,
        destination_head_generation,
    } = provenance.as_ref();
    encoded.extend_from_slice(source_commit.commit_id.as_bytes());
    put_bytes(encoded, source_commit.content_digest_uri.as_bytes());
    put_bytes(encoded, source_commit.manifest_digest_uri.as_bytes());
    encoded.extend_from_slice(source_commit.tree_manifest_revision_id.as_bytes());
    encoded.extend_from_slice(&source_commit.member_count.to_be_bytes());
    encoded.extend_from_slice(&source_commit.member_digest);
    encoded.extend_from_slice(&source_commit.unique_revision_count.to_be_bytes());
    encoded.extend_from_slice(&source_commit.revision_digest);
    encoded.extend_from_slice(&source_commit.parent_digest);
    encoded.extend_from_slice(&source_commit.generic_index_count.to_be_bytes());
    encoded.extend_from_slice(&source_commit.generic_index_digest);
    encoded.extend_from_slice(&destination_committed_at_unix_seconds.to_be_bytes());
    match destination_binding {
        None => encoded.push(0),
        Some(binding) => {
            encoded.push(1);
            encoded.extend_from_slice(binding.destination_commit_id.as_bytes());
            put_bytes(encoded, binding.effective_content_digest_uri.as_bytes());
            encoded.extend_from_slice(&binding.destination_projection_input_digest);
            put_manifest_identity(encoded, &binding.run_manifest_identity);
            put_manifest_identity(encoded, &binding.restore_manifest_identity);
            match &binding.manifests {
                None => encoded.push(0),
                Some(manifests) => {
                    encoded.push(1);
                    put_manifest_publication(encoded, &manifests.run_manifest);
                    put_manifest_publication(encoded, &manifests.restore_manifest);
                }
            }
        }
    }
    put_optional_path(encoded, closure.member_cursor.as_ref());
    encoded.extend_from_slice(&closure.member_count.to_be_bytes());
    encoded.extend_from_slice(&closure.member_digest);
    encoded.push(u8::from(closure.path_members_complete));
    put_optional_path(encoded, closure.generic_index_cursor.as_ref());
    encoded.extend_from_slice(&closure.generic_index_count.to_be_bytes());
    encoded.extend_from_slice(&closure.generic_index_digest);
    encoded.push(u8::from(closure.generic_indexes_complete));
    put_optional_fixed(encoded, closure.member_seal.as_ref());
    encoded.extend_from_slice(&closure.revision_ref_count.to_be_bytes());
    put_optional_fixed(
        encoded,
        closure
            .revision_cursor
            .as_ref()
            .map(ArtifactRevisionId::as_bytes),
    );
    encoded.extend_from_slice(&closure.revision_seal_count.to_be_bytes());
    encoded.extend_from_slice(&closure.revision_digest);
    put_optional_fixed(encoded, closure.revision_seal.as_ref());
    encoded.extend_from_slice(&closure.parent_digest);
    put_optional_fixed(encoded, closure.parent_seal.as_ref());
    encoded.extend_from_slice(&closure.cleanup_member_count.to_be_bytes());
    encoded.extend_from_slice(&closure.cleanup_generic_index_count.to_be_bytes());
    encoded.extend_from_slice(&closure.cleanup_revision_count.to_be_bytes());
    match destination_head_generation {
        None => encoded.push(0),
        Some(generation) => {
            encoded.push(1);
            encoded.extend_from_slice(&generation.get().to_be_bytes());
        }
    }
    Ok(())
}

fn put_manifest_publication(encoded: &mut Vec<u8>, publication: &RestoreManifestPublication) {
    encoded.extend_from_slice(publication.publication_operation_id.as_bytes());
    encoded.extend_from_slice(publication.workspace_incarnation_id.as_bytes());
    encoded.extend_from_slice(publication.artifact_revision_id.as_bytes());
    put_bytes(encoded, publication.body_digest_uri.as_bytes());
    put_bytes(encoded, publication.manifest_digest_uri.as_bytes());
    encoded.extend_from_slice(&publication.logical_size.to_be_bytes());
    put_bytes(encoded, publication.content_type.as_bytes());
}

fn put_manifest_identity(encoded: &mut Vec<u8>, identity: &RestoreManifestIdentity) {
    encoded.extend_from_slice(identity.publication_operation_id.as_bytes());
    encoded.extend_from_slice(identity.artifact_revision_id.as_bytes());
}

fn put_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    encoded.extend_from_slice(
        &u32::try_from(value.len())
            .expect("validated restore field length fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(value);
}

fn put_optional_path(encoded: &mut Vec<u8>, path: Option<&NormalizedRelativePath>) {
    match path {
        None => encoded.push(0),
        Some(path) => {
            encoded.push(1);
            put_bytes(encoded, path.as_str().as_bytes());
        }
    }
}

fn put_optional_fixed<const N: usize>(encoded: &mut Vec<u8>, value: Option<&[u8; N]>) {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(value);
        }
    }
}

fn put_optional_boolean(encoded: &mut Vec<u8>, value: Option<bool>) {
    encoded.push(match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    });
}

fn put_optional_result(encoded: &mut Vec<u8>, result: Option<&RestoreResult>) {
    match result {
        None => encoded.push(0),
        Some(result) => {
            encoded.push(1);
            encoded.extend_from_slice(result.destination_workspace_incarnation_id.as_bytes());
            encoded.extend_from_slice(&result.destination_workspace_revision.get().to_be_bytes());
            encoded.extend_from_slice(&result.member_count.to_be_bytes());
            encoded.extend_from_slice(&result.member_digest);
        }
    }
}

fn put_optional_terminal_error(
    encoded: &mut Vec<u8>,
    error: Option<&RestoreTerminalError>,
) -> Result<(), RestoreRecordError> {
    match error {
        None => encoded.push(0),
        Some(error) => {
            validate_terminal_error(error)?;
            encoded.push(1);
            encoded.push(error.kind.into());
            put_bytes(encoded, error.message.as_bytes());
            put_optional_fixed(encoded, error.evidence_digest.as_ref());
        }
    }
    Ok(())
}

fn decode_durable_enum<T>(value: u8) -> Result<T, RestoreRecordError>
where
    T: TryFrom<u8, Error = nokv_types::UnknownDurableDiscriminant>,
{
    T::try_from(value).map_err(|error| RestoreRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

fn decode_legacy_v4_operation(
    encoded: &[u8],
) -> Result<RestoreOperationRecord, RestoreRecordError> {
    let mut decoder = Decoder::new(encoded);
    decoder.require_value_version(RESTORE_MEMBER_VALUE_FORMAT_VERSION)?;
    let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
    let identity_digest = decoder.fixed("identity_digest")?;
    let initialization_digest = decoder.optional_fixed("initialization_digest")?;
    let source_workbench_id = decoder.workbench_id("source_workbench_id")?;
    let source_workspace_incarnation_id =
        WorkspaceIncarnationId::from_bytes(decoder.fixed("source_workspace_incarnation_id")?);
    let source = decoder.source()?;
    let destination_workbench_id = decoder.workbench_id("destination_workbench_id")?;
    let destination_workspace_incarnation_id =
        WorkspaceIncarnationId::from_bytes(decoder.fixed("destination_workspace_incarnation_id")?);
    let restore_manifest = RestoreManifestDescriptor {
        body_digest_uri: decoder.string("restore_manifest.body_digest_uri")?,
        logical_size: decoder.u64("restore_manifest.logical_size")?,
        content_type: decoder.string("restore_manifest.content_type")?,
    };
    let phase = decode_durable_enum(decoder.u8("phase")?)?;
    if !matches!(phase, RestorePhase::Complete | RestorePhase::Cleaned) {
        return Err(RestoreRecordError::LegacyNonterminalRequiresUpgrade { phase });
    }
    let source_cursor = decoder.optional_path("source_cursor")?;
    let source_eof = decoder.boolean("source_eof")?;
    let next_member_sequence = decoder.u64("next_member_sequence")?;
    let member_rolling_digest = decoder.fixed("member_rolling_digest")?;
    let member_seal = decoder.optional_fixed("member_seal")?;
    let cleanup_member_cursor = decoder.u64("cleanup_member_cursor")?;
    let result = decoder.optional_result("result")?;
    let terminal_error = decoder.optional_terminal_error("terminal_error")?;
    decoder.finish()?;
    let record = RestoreOperationRecord {
        operation_id,
        identity_digest,
        initialization_digest,
        source_workbench_id,
        source_workspace_incarnation_id,
        source,
        destination_workbench_id,
        destination_workspace_incarnation_id,
        destination_restore_manifest_identity: None,
        restore_manifest,
        commit_provenance: RestoreCommitProvenance::MissingLegacyV4,
        phase,
        source_cursor,
        source_paths_eof: source_eof,
        source_generic_index_cursor: None,
        source_generic_index_count: 0,
        source_generic_index_rolling_digest: [0; SHA256_BYTES],
        source_generic_index_seal: source_eof.then_some([0; SHA256_BYTES]),
        source_generic_indexes_match_base_commit: None,
        source_eof,
        source_member_count: next_member_sequence,
        source_member_rolling_digest: member_rolling_digest,
        source_member_seal: member_seal,
        source_matches_base_commit: None,
        next_member_sequence,
        member_rolling_digest,
        member_seal,
        cleanup_member_cursor,
        cleanup_generic_index_cursor: 0,
        result,
        terminal_error,
    };
    record.validate()?;
    Ok(record)
}

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn require_value_version(&mut self, expected: u8) -> Result<(), RestoreRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == expected {
            Ok(())
        } else {
            Err(RestoreRecordError::UnsupportedValueVersion { actual, expected })
        }
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], RestoreRecordError> {
        let bytes = self.take(field, N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, RestoreRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, RestoreRecordError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, RestoreRecordError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn boolean(&mut self, field: &'static str) -> Result<bool, RestoreRecordError> {
        match self.u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(RestoreRecordError::UnknownDiscriminant {
                type_name: field,
                value,
            }),
        }
    }

    fn generation(&mut self, field: &'static str) -> Result<Generation, RestoreRecordError> {
        Generation::new(self.u64(field)?).map_err(|_| RestoreRecordError::ZeroScalar { field })
    }

    fn bytes(&mut self, field: &'static str) -> Result<&'a [u8], RestoreRecordError> {
        let length = self.u32(field)? as usize;
        self.take(field, length)
    }

    fn source(&mut self) -> Result<RestoreSource, RestoreRecordError> {
        let kind: RestoreSourceKind = decode_durable_enum(self.u8("source_kind")?)?;
        match kind {
            RestoreSourceKind::Snapshot => {
                let snapshot_id = SnapshotId::new(self.u64("source_snapshot_id")?);
                let read_version =
                    ReadVersion::new(self.u64("source_read_version")?).map_err(|_| {
                        RestoreRecordError::ZeroScalar {
                            field: "source_read_version",
                        }
                    })?;
                Ok(RestoreSource::Snapshot {
                    snapshot_id,
                    read_version,
                })
            }
            RestoreSourceKind::Commit => Ok(RestoreSource::Commit {
                commit_id: CommitId::from_bytes(self.fixed("source_commit_id")?),
            }),
        }
    }

    fn commit_provenance(
        &mut self,
        value_version: u8,
    ) -> Result<RestoreCommitProvenance, RestoreRecordError> {
        let source_commit = RestoreSourceCommitSeal {
            commit_id: CommitId::from_bytes(self.fixed("source_commit.commit_id")?),
            content_digest_uri: self.string("source_commit.content_digest_uri")?,
            manifest_digest_uri: self.string("source_commit.manifest_digest_uri")?,
            tree_manifest_revision_id: ArtifactRevisionId::from_bytes(
                self.fixed("source_commit.tree_manifest_revision_id")?,
            ),
            member_count: self.u64("source_commit.member_count")?,
            member_digest: self.fixed("source_commit.member_digest")?,
            unique_revision_count: self.u64("source_commit.unique_revision_count")?,
            revision_digest: self.fixed("source_commit.revision_digest")?,
            parent_digest: self.fixed("source_commit.parent_digest")?,
            generic_index_count: if value_version == RESTORE_OPERATION_VALUE_FORMAT_VERSION {
                self.u64("source_commit.generic_index_count")?
            } else {
                0
            },
            generic_index_digest: if value_version == RESTORE_OPERATION_VALUE_FORMAT_VERSION {
                self.fixed("source_commit.generic_index_digest")?
            } else {
                [0; SHA256_BYTES]
            },
        };
        let destination_committed_at_unix_seconds =
            self.u64("destination_committed_at_unix_seconds")?;
        let destination_binding = match self.u8("destination_binding")? {
            0 => None,
            1 => Some(RestoreDestinationBinding {
                destination_commit_id: CommitId::from_bytes(
                    self.fixed("destination_binding.destination_commit_id")?,
                ),
                effective_content_digest_uri: self
                    .string("destination_binding.effective_content_digest_uri")?,
                destination_projection_input_digest: self
                    .fixed("destination_binding.destination_projection_input_digest")?,
                run_manifest_identity: self
                    .manifest_identity("destination_binding.run_manifest_identity")?,
                restore_manifest_identity: self
                    .manifest_identity("destination_binding.restore_manifest_identity")?,
                manifests: match self.u8("destination_binding.manifests")? {
                    0 => None,
                    1 => Some(RestoreDestinationManifests {
                        run_manifest: self.manifest_publication("run_manifest")?,
                        restore_manifest: self.manifest_publication("restore_manifest")?,
                    }),
                    value => {
                        return Err(RestoreRecordError::InvalidOptionalTag {
                            field: "destination_binding.manifests",
                            value,
                        });
                    }
                },
            }),
            value => {
                return Err(RestoreRecordError::InvalidOptionalTag {
                    field: "destination_binding",
                    value,
                });
            }
        };
        let member_cursor = self.optional_path("commit_closure.member_cursor")?;
        let member_count = self.u64("commit_closure.member_count")?;
        let member_digest = self.fixed("commit_closure.member_digest")?;
        let (
            path_members_complete,
            generic_index_cursor,
            generic_index_count,
            generic_index_digest,
            generic_indexes_complete,
        ) = if value_version == RESTORE_OPERATION_VALUE_FORMAT_VERSION {
            (
                self.boolean("commit_closure.path_members_complete")?,
                self.optional_path("commit_closure.generic_index_cursor")?,
                self.u64("commit_closure.generic_index_count")?,
                self.fixed("commit_closure.generic_index_digest")?,
                self.boolean("commit_closure.generic_indexes_complete")?,
            )
        } else {
            (false, None, 0, [0; SHA256_BYTES], false)
        };
        let member_seal = self.optional_fixed("commit_closure.member_seal")?;
        let (path_members_complete, generic_indexes_complete) =
            if value_version == LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION {
                (member_seal.is_some(), member_seal.is_some())
            } else {
                (path_members_complete, generic_indexes_complete)
            };
        let revision_ref_count = self.u64("commit_closure.revision_ref_count")?;
        let revision_cursor = self
            .optional_fixed("commit_closure.revision_cursor")?
            .map(ArtifactRevisionId::from_bytes);
        let revision_seal_count = self.u64("commit_closure.revision_seal_count")?;
        let revision_digest = self.fixed("commit_closure.revision_digest")?;
        let revision_seal = self.optional_fixed("commit_closure.revision_seal")?;
        let parent_digest = self.fixed("commit_closure.parent_digest")?;
        let parent_seal = self.optional_fixed("commit_closure.parent_seal")?;
        let cleanup_member_count = self.u64("commit_closure.cleanup_member_count")?;
        let cleanup_generic_index_count = if value_version == RESTORE_OPERATION_VALUE_FORMAT_VERSION
        {
            self.u64("commit_closure.cleanup_generic_index_count")?
        } else {
            0
        };
        let cleanup_revision_count = self.u64("commit_closure.cleanup_revision_count")?;
        let destination_head_generation = match self.u8("destination_head_generation")? {
            0 => None,
            1 => Some(self.generation("destination_head_generation")?),
            value => {
                return Err(RestoreRecordError::InvalidOptionalTag {
                    field: "destination_head_generation",
                    value,
                });
            }
        };
        Ok(RestoreCommitProvenance::V5(Box::new(
            RestoreCommitProvenanceV5 {
                source_commit,
                destination_committed_at_unix_seconds,
                destination_binding,
                closure: RestoreCommitClosureProgress {
                    member_cursor,
                    member_count,
                    member_digest,
                    path_members_complete,
                    generic_index_cursor,
                    generic_index_count,
                    generic_index_digest,
                    generic_indexes_complete,
                    member_seal,
                    revision_ref_count,
                    revision_cursor,
                    revision_seal_count,
                    revision_digest,
                    revision_seal,
                    parent_digest,
                    parent_seal,
                    cleanup_member_count,
                    cleanup_generic_index_count,
                    cleanup_revision_count,
                },
                destination_head_generation,
            },
        )))
    }

    fn manifest_publication(
        &mut self,
        field: &'static str,
    ) -> Result<RestoreManifestPublication, RestoreRecordError> {
        Ok(RestoreManifestPublication {
            publication_operation_id: OperationId::from_bytes(self.fixed(field)?),
            workspace_incarnation_id: WorkspaceIncarnationId::from_bytes(self.fixed(field)?),
            artifact_revision_id: ArtifactRevisionId::from_bytes(self.fixed(field)?),
            body_digest_uri: self.string(field)?,
            manifest_digest_uri: self.string(field)?,
            logical_size: self.u64(field)?,
            content_type: self.string(field)?,
        })
    }

    fn manifest_identity(
        &mut self,
        field: &'static str,
    ) -> Result<RestoreManifestIdentity, RestoreRecordError> {
        Ok(RestoreManifestIdentity {
            publication_operation_id: OperationId::from_bytes(self.fixed(field)?),
            artifact_revision_id: ArtifactRevisionId::from_bytes(self.fixed(field)?),
        })
    }

    fn workbench_id(&mut self, field: &'static str) -> Result<WorkbenchId, RestoreRecordError> {
        let bytes = self.bytes(field)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| RestoreRecordError::InvalidUtf8 { field })?;
        WorkbenchId::new(value).map_err(|error| RestoreRecordError::InvalidWorkbenchId {
            reason: error.to_string(),
        })
    }

    fn string(&mut self, field: &'static str) -> Result<String, RestoreRecordError> {
        let bytes = self.bytes(field)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| RestoreRecordError::InvalidUtf8 { field })
    }

    fn path(&mut self, field: &'static str) -> Result<NormalizedRelativePath, RestoreRecordError> {
        let bytes = self.bytes(field)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| RestoreRecordError::InvalidUtf8 { field })?;
        NormalizedRelativePath::new(value).map_err(|error| RestoreRecordError::InvalidPath {
            reason: error.to_string(),
        })
    }

    fn optional_path(
        &mut self,
        field: &'static str,
    ) -> Result<Option<NormalizedRelativePath>, RestoreRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.path(field).map(Some),
            value => Err(RestoreRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<Option<[u8; N]>, RestoreRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.fixed(field).map(Some),
            value => Err(RestoreRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_boolean(
        &mut self,
        field: &'static str,
    ) -> Result<Option<bool>, RestoreRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => Ok(Some(false)),
            2 => Ok(Some(true)),
            value => Err(RestoreRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_result(
        &mut self,
        field: &'static str,
    ) -> Result<Option<RestoreResult>, RestoreRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => Ok(Some(RestoreResult {
                destination_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes(
                    self.fixed("result.destination_workspace_incarnation_id")?,
                ),
                destination_workspace_revision: WorkspaceRevision::new(
                    self.u64("result.destination_workspace_revision")?,
                ),
                member_count: self.u64("result.member_count")?,
                member_digest: self.fixed("result.member_digest")?,
            })),
            value => Err(RestoreRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_terminal_error(
        &mut self,
        field: &'static str,
    ) -> Result<Option<RestoreTerminalError>, RestoreRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => {
                let kind = RestoreTerminalErrorKind::try_from(self.u8("terminal_error.kind")?)?;
                let message_bytes = self.bytes("terminal_error.message")?;
                let message = std::str::from_utf8(message_bytes)
                    .map_err(|_| RestoreRecordError::InvalidUtf8 {
                        field: "terminal_error.message",
                    })?
                    .to_owned();
                let error = RestoreTerminalError {
                    kind,
                    message,
                    evidence_digest: self.optional_fixed("terminal_error.evidence_digest")?,
                };
                validate_terminal_error(&error)?;
                Ok(Some(error))
            }
            value => Err(RestoreRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], RestoreRecordError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(RestoreRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn finish(self) -> Result<(), RestoreRecordError> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(RestoreRecordError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::super::commit_records::advance_commit_member_rolling_digest;
    use super::*;

    fn digest_uri(byte: u8) -> String {
        format!("sha256:{}", format!("{byte:02x}").repeat(SHA256_BYTES))
    }

    fn manifest_identity(operation_fill: u8, revision_fill: u8) -> RestoreManifestIdentity {
        RestoreManifestIdentity {
            publication_operation_id: OperationId::from_bytes([operation_fill; 16]),
            artifact_revision_id: ArtifactRevisionId::from_bytes([revision_fill; 16]),
        }
    }

    fn manifests() -> RestoreDestinationManifests {
        RestoreDestinationManifests {
            run_manifest: RestoreManifestPublication {
                publication_operation_id: OperationId::from_bytes([0x41; 16]),
                workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([7; 16]),
                artifact_revision_id: ArtifactRevisionId::from_bytes([0x31; 16]),
                body_digest_uri: digest_uri(0x41),
                manifest_digest_uri: digest_uri(0x42),
                logical_size: 129,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            restore_manifest: RestoreManifestPublication {
                publication_operation_id: OperationId::from_bytes([0x42; 16]),
                workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([7; 16]),
                artifact_revision_id: ArtifactRevisionId::from_bytes([0x32; 16]),
                body_digest_uri: digest_uri(0xab),
                manifest_digest_uri: digest_uri(0x43),
                logical_size: 128,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
        }
    }

    fn binding() -> RestoreDestinationBinding {
        RestoreDestinationBinding {
            destination_commit_id: CommitId::from_bytes([4; SHA256_BYTES]),
            effective_content_digest_uri: digest_uri(0x51),
            destination_projection_input_digest: [0x44; SHA256_BYTES],
            run_manifest_identity: manifest_identity(0x41, 0x31),
            restore_manifest_identity: manifest_identity(0x42, 0x32),
            manifests: None,
        }
    }

    fn operation(phase: RestorePhase) -> RestoreOperationRecord {
        let mut identity_digest = [2; SHA256_BYTES];
        identity_digest[..OperationId::BYTE_WIDTH].fill(1);
        RestoreOperationRecord {
            operation_id: OperationId::from_bytes([1; 16]),
            identity_digest,
            initialization_digest: None,
            source_workbench_id: WorkbenchId::new("source").unwrap(),
            source_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([4; 16]),
            source: RestoreSource::Snapshot {
                snapshot_id: SnapshotId::new(5),
                read_version: ReadVersion::new(6).unwrap(),
            },
            destination_workbench_id: WorkbenchId::new("fork").unwrap(),
            destination_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([7; 16]),
            destination_restore_manifest_identity: Some(manifest_identity(0x42, 0x32)),
            restore_manifest: RestoreManifestDescriptor {
                body_digest_uri: digest_uri(0xab),
                logical_size: 128,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            commit_provenance: RestoreCommitProvenance::V5(Box::new(RestoreCommitProvenanceV5 {
                source_commit: RestoreSourceCommitSeal {
                    commit_id: CommitId::from_bytes([3; SHA256_BYTES]),
                    content_digest_uri: digest_uri(0x11),
                    manifest_digest_uri: digest_uri(0x12),
                    tree_manifest_revision_id: ArtifactRevisionId::from_bytes([0x21; 16]),
                    member_count: 1,
                    member_digest: [0x22; SHA256_BYTES],
                    unique_revision_count: 1,
                    revision_digest: [0x23; SHA256_BYTES],
                    parent_digest: [0; SHA256_BYTES],
                    generic_index_count: 0,
                    generic_index_digest: [0; SHA256_BYTES],
                },
                destination_committed_at_unix_seconds: 7,
                destination_binding: None,
                closure: RestoreCommitClosureProgress {
                    member_cursor: None,
                    member_count: 0,
                    member_digest: [0; SHA256_BYTES],
                    path_members_complete: false,
                    generic_index_cursor: None,
                    generic_index_count: 0,
                    generic_index_digest: [0; SHA256_BYTES],
                    generic_indexes_complete: false,
                    member_seal: None,
                    revision_ref_count: 0,
                    revision_cursor: None,
                    revision_seal_count: 0,
                    revision_digest: [0; SHA256_BYTES],
                    revision_seal: None,
                    parent_digest: advance_commit_parent_rolling_digest(
                        [0; SHA256_BYTES],
                        0,
                        CommitId::from_bytes([3; SHA256_BYTES]),
                    ),
                    parent_seal: None,
                    cleanup_member_count: 0,
                    cleanup_generic_index_count: 0,
                    cleanup_revision_count: 0,
                },
                destination_head_generation: None,
            })),
            phase,
            source_cursor: None,
            source_paths_eof: false,
            source_generic_index_cursor: None,
            source_generic_index_count: 0,
            source_generic_index_rolling_digest: [0; SHA256_BYTES],
            source_generic_index_seal: None,
            source_generic_indexes_match_base_commit: None,
            source_eof: false,
            source_member_count: 0,
            source_member_rolling_digest: [0; SHA256_BYTES],
            source_member_seal: None,
            source_matches_base_commit: None,
            next_member_sequence: 0,
            member_rolling_digest: [0; SHA256_BYTES],
            member_seal: None,
            cleanup_member_cursor: 0,
            cleanup_generic_index_cursor: 0,
            result: None,
            terminal_error: None,
        }
    }

    fn seal_empty_generic_index_source(record: &mut RestoreOperationRecord) {
        record.source_paths_eof = true;
        record.source_generic_index_seal = Some([0; SHA256_BYTES]);
        record.source_generic_indexes_match_base_commit = Some(true);
        record.source_eof = true;
    }

    fn bound_source_operation() -> RestoreOperationRecord {
        let mut record = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        record.source_cursor = Some(NormalizedRelativePath::new("outputs/result").unwrap());
        seal_empty_generic_index_source(&mut record);
        record.source_member_count = 1;
        record.source_member_rolling_digest = [7; SHA256_BYTES];
        record.next_member_sequence = 1;
        record.member_rolling_digest = [8; SHA256_BYTES];
        record = record
            .apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    source_member_seal: [7; SHA256_BYTES],
                },
            )
            .unwrap();
        record = record
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination { binding: binding() },
            )
            .unwrap();
        record
    }

    fn destination_building_operation() -> RestoreOperationRecord {
        bound_source_operation()
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: manifests(),
                },
            )
            .unwrap()
    }

    fn terminal_error() -> RestoreTerminalError {
        RestoreTerminalError {
            kind: RestoreTerminalErrorKind::AbortedByCaller,
            message: "cancelled".to_owned(),
            evidence_digest: Some([9; SHA256_BYTES]),
        }
    }

    fn legacy_v4_operation(phase: RestorePhase) -> Vec<u8> {
        let mut encoded = Vec::new();
        let mut identity_digest = [2; SHA256_BYTES];
        identity_digest[..OperationId::BYTE_WIDTH].fill(1);
        encoded.push(RESTORE_MEMBER_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(OperationId::from_bytes([1; 16]).as_bytes());
        encoded.extend_from_slice(&identity_digest);
        match phase {
            RestorePhase::Complete => put_optional_fixed(&mut encoded, Some(&[3; SHA256_BYTES])),
            RestorePhase::Cleaned => put_optional_fixed::<SHA256_BYTES>(&mut encoded, None),
            _ => put_optional_fixed::<SHA256_BYTES>(&mut encoded, None),
        }
        put_bytes(&mut encoded, b"source");
        encoded.extend_from_slice(&[4; 16]);
        put_source(
            &mut encoded,
            RestoreSource::Snapshot {
                snapshot_id: SnapshotId::new(5),
                read_version: ReadVersion::new(6).unwrap(),
            },
        );
        put_bytes(&mut encoded, b"fork");
        encoded.extend_from_slice(&[7; 16]);
        put_bytes(&mut encoded, digest_uri(0xab).as_bytes());
        encoded.extend_from_slice(&128_u64.to_be_bytes());
        put_bytes(&mut encoded, RESTORE_MANIFEST_CONTENT_TYPE.as_bytes());
        encoded.push(phase.into());
        put_optional_path(
            &mut encoded,
            Some(&NormalizedRelativePath::new("input/a").unwrap()),
        );
        encoded.push(u8::from(phase == RestorePhase::Complete));
        encoded.extend_from_slice(&1_u64.to_be_bytes());
        encoded.extend_from_slice(&[8; SHA256_BYTES]);
        match phase {
            RestorePhase::Complete => put_optional_fixed(&mut encoded, Some(&[8; SHA256_BYTES])),
            _ => put_optional_fixed::<SHA256_BYTES>(&mut encoded, None),
        }
        encoded.extend_from_slice(
            &(if phase == RestorePhase::Cleaned {
                1_u64
            } else {
                0_u64
            })
            .to_be_bytes(),
        );
        if phase == RestorePhase::Complete {
            put_optional_result(
                &mut encoded,
                Some(&RestoreResult {
                    destination_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes(
                        [7; 16],
                    ),
                    destination_workspace_revision: WorkspaceRevision::new(1),
                    member_count: 1,
                    member_digest: [8; SHA256_BYTES],
                }),
            );
            put_optional_terminal_error(&mut encoded, None).unwrap();
        } else {
            put_optional_result(&mut encoded, None);
            put_optional_terminal_error(&mut encoded, Some(&terminal_error())).unwrap();
        }
        encoded
    }

    fn legacy_v5_preparing_operation() -> Vec<u8> {
        let record = operation(RestorePhase::Preparing);
        let RestoreCommitProvenance::V5(provenance) = &record.commit_provenance else {
            unreachable!();
        };
        let source_commit = &provenance.source_commit;
        let closure = &provenance.closure;
        let mut encoded = Vec::new();
        encoded.push(LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(record.operation_id.as_bytes());
        encoded.extend_from_slice(&record.identity_digest);
        put_optional_fixed(&mut encoded, record.initialization_digest.as_ref());
        put_bytes(&mut encoded, record.source_workbench_id.as_bytes());
        encoded.extend_from_slice(record.source_workspace_incarnation_id.as_bytes());
        put_source(&mut encoded, record.source);
        put_bytes(&mut encoded, record.destination_workbench_id.as_bytes());
        encoded.extend_from_slice(record.destination_workspace_incarnation_id.as_bytes());
        put_manifest_identity(
            &mut encoded,
            record
                .destination_restore_manifest_identity
                .as_ref()
                .unwrap(),
        );
        put_bytes(
            &mut encoded,
            record.restore_manifest.body_digest_uri.as_bytes(),
        );
        encoded.extend_from_slice(&record.restore_manifest.logical_size.to_be_bytes());
        put_bytes(
            &mut encoded,
            record.restore_manifest.content_type.as_bytes(),
        );
        encoded.extend_from_slice(source_commit.commit_id.as_bytes());
        put_bytes(&mut encoded, source_commit.content_digest_uri.as_bytes());
        put_bytes(&mut encoded, source_commit.manifest_digest_uri.as_bytes());
        encoded.extend_from_slice(source_commit.tree_manifest_revision_id.as_bytes());
        encoded.extend_from_slice(&source_commit.member_count.to_be_bytes());
        encoded.extend_from_slice(&source_commit.member_digest);
        encoded.extend_from_slice(&source_commit.unique_revision_count.to_be_bytes());
        encoded.extend_from_slice(&source_commit.revision_digest);
        encoded.extend_from_slice(&source_commit.parent_digest);
        encoded.extend_from_slice(
            &provenance
                .destination_committed_at_unix_seconds
                .to_be_bytes(),
        );
        encoded.push(0); // destination binding
        put_optional_path(&mut encoded, closure.member_cursor.as_ref());
        encoded.extend_from_slice(&closure.member_count.to_be_bytes());
        encoded.extend_from_slice(&closure.member_digest);
        put_optional_fixed(&mut encoded, closure.member_seal.as_ref());
        encoded.extend_from_slice(&closure.revision_ref_count.to_be_bytes());
        put_optional_fixed(
            &mut encoded,
            closure
                .revision_cursor
                .as_ref()
                .map(ArtifactRevisionId::as_bytes),
        );
        encoded.extend_from_slice(&closure.revision_seal_count.to_be_bytes());
        encoded.extend_from_slice(&closure.revision_digest);
        put_optional_fixed(&mut encoded, closure.revision_seal.as_ref());
        encoded.extend_from_slice(&closure.parent_digest);
        put_optional_fixed(&mut encoded, closure.parent_seal.as_ref());
        encoded.extend_from_slice(&closure.cleanup_member_count.to_be_bytes());
        encoded.extend_from_slice(&closure.cleanup_revision_count.to_be_bytes());
        encoded.push(0); // destination head generation
        encoded.push(record.phase.into());
        put_optional_path(&mut encoded, record.source_cursor.as_ref());
        encoded.push(u8::from(record.source_eof));
        encoded.extend_from_slice(&record.source_member_count.to_be_bytes());
        encoded.extend_from_slice(&record.source_member_rolling_digest);
        put_optional_fixed(&mut encoded, record.source_member_seal.as_ref());
        put_optional_boolean(&mut encoded, record.source_matches_base_commit);
        encoded.extend_from_slice(&record.next_member_sequence.to_be_bytes());
        encoded.extend_from_slice(&record.member_rolling_digest);
        put_optional_fixed(&mut encoded, record.member_seal.as_ref());
        encoded.extend_from_slice(&record.cleanup_member_cursor.to_be_bytes());
        put_optional_result(&mut encoded, record.result.as_ref());
        put_optional_terminal_error(&mut encoded, record.terminal_error.as_ref()).unwrap();
        encoded
    }

    fn v5_phase_offset(encoded: &[u8]) -> usize {
        let mut decoder = Decoder::new(encoded);
        decoder
            .require_value_version(RESTORE_OPERATION_VALUE_FORMAT_VERSION)
            .unwrap();
        let _: [u8; OperationId::BYTE_WIDTH] = decoder.fixed("operation_id").unwrap();
        let _: [u8; SHA256_BYTES] = decoder.fixed("identity_digest").unwrap();
        let _: Option<[u8; SHA256_BYTES]> =
            decoder.optional_fixed("initialization_digest").unwrap();
        decoder.workbench_id("source_workbench_id").unwrap();
        let _: [u8; nokv_types::FIXED_ID_BYTES] =
            decoder.fixed("source_workspace_incarnation_id").unwrap();
        decoder.source().unwrap();
        decoder.workbench_id("destination_workbench_id").unwrap();
        let _: [u8; nokv_types::FIXED_ID_BYTES] = decoder
            .fixed("destination_workspace_incarnation_id")
            .unwrap();
        decoder
            .manifest_identity("destination_restore_manifest_identity")
            .unwrap();
        decoder.string("restore_manifest.body_digest_uri").unwrap();
        decoder.u64("restore_manifest.logical_size").unwrap();
        decoder.string("restore_manifest.content_type").unwrap();
        decoder
            .commit_provenance(RESTORE_OPERATION_VALUE_FORMAT_VERSION)
            .unwrap();
        decoder.offset
    }

    #[test]
    fn restore_operation_v6_has_frozen_golden_digest_and_strict_envelope() {
        let record = operation(RestorePhase::Preparing);
        let encoded = record.encode().unwrap();
        assert_eq!(encoded[0], RESTORE_OPERATION_VALUE_FORMAT_VERSION);
        assert_eq!(encoded.len(), 952);
        assert_eq!(
            <[u8; SHA256_BYTES]>::from(Sha256::digest(&encoded)),
            [
                226, 8, 70, 170, 145, 81, 148, 158, 122, 225, 7, 132, 154, 68, 14, 27, 180, 242,
                123, 163, 69, 205, 160, 15, 163, 103, 182, 254, 215, 125, 78, 92,
            ]
        );
        assert_eq!(RestoreOperationRecord::decode(&encoded).unwrap(), record);
        assert_eq!(
            record.destination_commit_receipt(),
            Err(RestoreRecordError::PhaseMismatch {
                expected: RestorePhase::Complete,
                actual: RestorePhase::Preparing,
            })
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            RestoreOperationRecord::decode(&trailing),
            Err(RestoreRecordError::TrailingBytes { count: 1 })
        );
    }

    #[test]
    fn restore_operation_v5_dual_decode_does_not_claim_a_generic_index_closure() {
        let encoded = legacy_v5_preparing_operation();
        assert_eq!(encoded[0], LEGACY_RESTORE_OPERATION_VALUE_FORMAT_VERSION);
        let decoded = RestoreOperationRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, operation(RestorePhase::Preparing));
        assert_eq!(decoded.source_generic_index_count, 0);
        assert!(decoded.source_generic_index_seal.is_none());
        let RestoreCommitProvenance::V5(provenance) = &decoded.commit_provenance else {
            unreachable!();
        };
        assert_eq!(provenance.source_commit.generic_index_count, 0);
        assert_eq!(provenance.closure.generic_index_count, 0);
        assert!(!provenance.closure.path_members_complete);
        assert!(!provenance.closure.generic_indexes_complete);
        assert_eq!(
            decoded.encode().unwrap()[0],
            RESTORE_OPERATION_VALUE_FORMAT_VERSION
        );
    }

    #[test]
    fn legacy_v4_terminal_status_is_read_only_and_nonterminal_blocks_upgrade() {
        for phase in [RestorePhase::Complete, RestorePhase::Cleaned] {
            let decoded = RestoreOperationRecord::decode(&legacy_v4_operation(phase)).unwrap();
            assert_eq!(decoded.phase, phase);
            assert_eq!(
                decoded.commit_provenance,
                RestoreCommitProvenance::MissingLegacyV4
            );
            assert_eq!(
                decoded.encode(),
                Err(RestoreRecordError::LegacyProvenanceMissing { phase })
            );
            if phase == RestorePhase::Complete {
                assert_eq!(
                    decoded.destination_commit_receipt(),
                    Err(RestoreRecordError::LegacyProvenanceMissing { phase })
                );
            }
        }

        let nonterminal = legacy_v4_operation(RestorePhase::Preparing);
        assert_eq!(
            RestoreOperationRecord::decode(&nonterminal),
            Err(RestoreRecordError::LegacyNonterminalRequiresUpgrade {
                phase: RestorePhase::Preparing,
            })
        );
        let mut future = operation(RestorePhase::Preparing).encode().unwrap();
        future[0] = RESTORE_OPERATION_VALUE_FORMAT_VERSION + 1;
        assert_eq!(
            RestoreOperationRecord::decode(&future),
            Err(RestoreRecordError::UnsupportedValueVersion {
                actual: RESTORE_OPERATION_VALUE_FORMAT_VERSION + 1,
                expected: RESTORE_OPERATION_VALUE_FORMAT_VERSION,
            })
        );
    }

    #[test]
    fn v5_rejects_noncanonical_closure_combinations() {
        let mut record = operation(RestorePhase::Preparing);
        let RestoreCommitProvenance::V5(provenance) = &mut record.commit_provenance else {
            unreachable!();
        };
        let closure = &mut provenance.closure;
        closure.revision_ref_count = 1;
        closure.revision_seal_count = 1;
        closure.revision_cursor = Some(ArtifactRevisionId::from_bytes([7; 16]));
        closure.revision_digest = [7; SHA256_BYTES];
        closure.revision_seal = Some([8; SHA256_BYTES]);
        assert!(matches!(
            record.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "commit revision seal requires the complete revision closure",
                ..
            })
        ));
    }

    #[test]
    fn v5_late_destination_bind_is_required_exact_and_fail_closed() {
        let mut sealed = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        sealed.source_cursor = Some(NormalizedRelativePath::new("outputs/result").unwrap());
        seal_empty_generic_index_source(&mut sealed);
        sealed.source_member_count = 1;
        sealed.source_member_rolling_digest = [7; SHA256_BYTES];
        sealed.next_member_sequence = 1;
        sealed.member_rolling_digest = [8; SHA256_BYTES];
        sealed = sealed
            .apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    source_member_seal: [7; SHA256_BYTES],
                },
            )
            .unwrap();

        assert!(matches!(
            sealed.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: manifests(),
                },
            ),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "destination authority must be bound before commit construction",
                ..
            })
        ));

        let mut duplicate_revisions = binding();
        duplicate_revisions
            .restore_manifest_identity
            .artifact_revision_id = duplicate_revisions
            .run_manifest_identity
            .artifact_revision_id;
        assert!(matches!(
            sealed.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination {
                    binding: duplicate_revisions,
                },
            ),
            Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "run and restore manifest identities must use distinct publication operations and revisions",
            })
        ));

        let mut missing_projection = binding();
        missing_projection.destination_projection_input_digest = [0; SHA256_BYTES];
        assert!(matches!(
            sealed.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination {
                    binding: missing_projection,
                },
            ),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "destination projection input digest must be non-zero",
                ..
            })
        ));

        let mut dirty_reuses_base_content = binding();
        dirty_reuses_base_content.effective_content_digest_uri = digest_uri(0x11);
        assert!(matches!(
            sealed.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination {
                    binding: dirty_reuses_base_content,
                },
            ),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "effective content digest must preserve clean source content and distinguish dirty materialization",
                ..
            })
        ));

        let bound = sealed
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination { binding: binding() },
            )
            .unwrap();
        let mut different = binding();
        different.destination_commit_id = CommitId::from_bytes([5; SHA256_BYTES]);
        assert!(matches!(
            bound.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination { binding: different },
            ),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "destination authority was already bound",
                ..
            })
        ));
    }

    #[test]
    fn snapshot_may_diverge_from_base_commit_but_commit_source_may_not() {
        let mut snapshot = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        snapshot.source_cursor = Some(NormalizedRelativePath::new("outputs/result").unwrap());
        seal_empty_generic_index_source(&mut snapshot);
        snapshot.source_member_count = 1;
        snapshot.source_member_rolling_digest = [7; SHA256_BYTES];
        snapshot.next_member_sequence = 1;
        snapshot.member_rolling_digest = [8; SHA256_BYTES];
        let sealed_snapshot = snapshot
            .apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    source_member_seal: [7; SHA256_BYTES],
                },
            )
            .unwrap();
        assert_eq!(sealed_snapshot.source_matches_base_commit, Some(false));

        let mut commit = snapshot;
        commit.source = RestoreSource::Commit {
            commit_id: CommitId::from_bytes([3; SHA256_BYTES]),
        };
        assert!(matches!(
            commit.apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    source_member_seal: [7; SHA256_BYTES],
                },
            ),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "commit-source restore must exactly match its immutable commit closure",
                ..
            })
        ));

        let mut clean = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        clean.source_cursor = Some(NormalizedRelativePath::new("outputs/result").unwrap());
        seal_empty_generic_index_source(&mut clean);
        clean.source_member_count = 1;
        clean.source_member_rolling_digest = [0x22; SHA256_BYTES];
        clean.next_member_sequence = 1;
        clean.member_rolling_digest = [8; SHA256_BYTES];
        clean = clean
            .apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    source_member_seal: [0x22; SHA256_BYTES],
                },
            )
            .unwrap();
        assert_eq!(clean.source_matches_base_commit, Some(true));
        assert!(matches!(
            clean.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination { binding: binding() },
            ),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "effective content digest must preserve clean source content and distinguish dirty materialization",
                ..
            })
        ));
        let mut preserves_base = binding();
        preserves_base.effective_content_digest_uri = digest_uri(0x11);
        assert!(clean
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination {
                    binding: preserves_base,
                },
            )
            .is_ok());
    }

    #[test]
    fn v5_rejects_each_closure_cursor_and_seal_mismatch() {
        let base = operation(RestorePhase::Preparing);

        let mut raw_cursor = base.clone();
        raw_cursor.source_member_count = 1;
        raw_cursor.source_member_rolling_digest = [1; SHA256_BYTES];
        assert!(matches!(
            raw_cursor.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "source cursor presence must match non-empty raw source progress",
                ..
            })
        ));

        let mut raw_seal = base.clone();
        raw_seal.source_cursor = Some(NormalizedRelativePath::new("input/a").unwrap());
        raw_seal.source_member_count = 1;
        raw_seal.source_member_rolling_digest = [1; SHA256_BYTES];
        raw_seal.source_member_seal = Some([2; SHA256_BYTES]);
        assert!(matches!(
            raw_seal.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "restore closure seals must equal their rolling digests",
                ..
            })
        ));

        let mut destination_only_member = base.clone();
        destination_only_member.next_member_sequence = 1;
        destination_only_member.member_rolling_digest = [3; SHA256_BYTES];
        assert!(matches!(
            destination_only_member.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "materialized closure cannot contain rows absent from the raw source",
                ..
            })
        ));

        let mut materialized_seal = base.clone();
        materialized_seal.source_cursor = Some(NormalizedRelativePath::new("input/a").unwrap());
        materialized_seal.source_member_count = 1;
        materialized_seal.source_member_rolling_digest = [2; SHA256_BYTES];
        materialized_seal.next_member_sequence = 1;
        materialized_seal.member_rolling_digest = [3; SHA256_BYTES];
        materialized_seal.member_seal = Some([4; SHA256_BYTES]);
        assert!(matches!(
            materialized_seal.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "restore closure seals must equal their rolling digests",
                ..
            })
        ));

        let mut commit_member_cursor = base.clone();
        let RestoreCommitProvenance::V5(provenance) = &mut commit_member_cursor.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.member_count = 1;
        provenance.closure.member_digest = [5; SHA256_BYTES];
        assert!(matches!(
            commit_member_cursor.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "commit member cursor must match non-empty member progress",
                ..
            })
        ));

        let mut commit_member_seal = base.clone();
        let RestoreCommitProvenance::V5(provenance) = &mut commit_member_seal.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.member_cursor =
            Some(NormalizedRelativePath::new("output/result").unwrap());
        provenance.closure.member_count = 1;
        provenance.closure.member_digest = [5; SHA256_BYTES];
        provenance.closure.member_seal = Some([6; SHA256_BYTES]);
        assert!(matches!(
            commit_member_seal.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "commit member seal must equal the rolling digest",
                ..
            })
        ));

        let mut revision_cursor = base.clone();
        let RestoreCommitProvenance::V5(provenance) = &mut revision_cursor.commit_provenance else {
            unreachable!();
        };
        provenance.closure.revision_ref_count = 1;
        provenance.closure.revision_seal_count = 1;
        provenance.closure.revision_digest = [7; SHA256_BYTES];
        assert!(matches!(
            revision_cursor.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "commit revision cursor must match non-empty sealing progress",
                ..
            })
        ));

        let mut parent_seal = base;
        let RestoreCommitProvenance::V5(provenance) = &mut parent_seal.commit_provenance else {
            unreachable!();
        };
        provenance.closure.parent_seal = Some([8; SHA256_BYTES]);
        assert!(matches!(
            parent_seal.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "commit parent seal must equal the single-parent digest",
                ..
            })
        ));
    }

    #[test]
    fn destination_building_and_sealing_accept_only_their_own_progress() {
        let building = destination_building_operation();
        assert_eq!(
            RestoreOperationRecord::decode(&building.encode().unwrap()).unwrap(),
            building
        );

        let mut prematurely_sealed = building.clone();
        let RestoreCommitProvenance::V5(provenance) = &mut prematurely_sealed.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.member_seal = Some([0; SHA256_BYTES]);
        assert!(matches!(
            prematurely_sealed.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                phase: RestorePhase::DestinationBuilding,
                reason: "commit member seal requires both destination member closures",
            })
        ));

        let mut building = building;
        let parent_digest = match &mut building.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => {
                let closure = &mut provenance.closure;
                closure.member_cursor =
                    Some(NormalizedRelativePath::new("metadata/run_manifest.json").unwrap());
                closure.member_count = 3;
                closure.member_digest = [9; SHA256_BYTES];
                closure.revision_ref_count = 2;
                closure.path_members_complete = true;
                closure.generic_indexes_complete = true;
                closure.parent_digest
            }
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        };
        let sealing = building
            .apply(
                RestorePhase::DestinationBuilding,
                RestoreTransition::BeginDestinationSealing {
                    member_seal: [9; SHA256_BYTES],
                },
            )
            .unwrap();
        assert_eq!(
            RestoreOperationRecord::decode(&sealing.encode().unwrap()).unwrap(),
            sealing
        );

        let mut partial_revision_scan = sealing;
        match &mut partial_revision_scan.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => {
                provenance.closure.revision_cursor =
                    Some(ArtifactRevisionId::from_bytes([0x31; 16]));
                provenance.closure.revision_seal_count = 1;
                provenance.closure.revision_digest = [10; SHA256_BYTES];
            }
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        }
        assert!(partial_revision_scan.encode().is_ok());

        let RestoreCommitProvenance::V5(provenance) = &mut partial_revision_scan.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.parent_seal = Some(parent_digest);
        assert!(matches!(
            partial_revision_scan.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                phase: RestorePhase::DestinationSealing,
                reason: "destination sealing requires the complete member seal only",
            })
        ));
    }

    #[test]
    fn actual_destination_manifests_install_atomically_and_exactly_once() {
        let bound = bound_source_operation();

        let mut wrong_identity = manifests();
        wrong_identity.run_manifest.publication_operation_id = OperationId::from_bytes([0x43; 16]);
        assert!(matches!(
            bound.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: wrong_identity,
                },
            ),
            Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "published destination manifest must match its expected identity",
            })
        ));

        let mut wrong_incarnation = manifests();
        wrong_incarnation.restore_manifest.workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes([8; 16]);
        assert!(matches!(
            bound.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: wrong_incarnation,
                },
            ),
            Err(RestoreRecordError::InvalidManifestDescriptor {
                reason:
                    "published destination manifests must target the hidden destination incarnation",
            })
        ));

        let mut wrong_descriptor = manifests();
        wrong_descriptor.restore_manifest.logical_size += 1;
        assert!(matches!(
            bound.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: wrong_descriptor,
                },
            ),
            Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "published restore manifest must match its begin descriptor",
            })
        ));

        let building = bound
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: manifests(),
                },
            )
            .unwrap();
        assert_eq!(building.next_member_sequence, 1);
        assert_eq!(building.member_seal, Some([8; SHA256_BYTES]));
        assert!(matches!(
            building.apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: manifests(),
                },
            ),
            Err(RestoreRecordError::PhaseMismatch {
                expected: RestorePhase::SourceSealed,
                actual: RestorePhase::DestinationBuilding,
            })
        ));
    }

    #[test]
    fn run_and_restore_manifest_publications_have_distinct_size_contracts() {
        let mut large_run = manifests();
        large_run.run_manifest.logical_size = MAX_RESTORE_MANIFEST_BYTES as u64 + 1;
        assert!(large_run.validate().is_ok());

        let mut large_restore = manifests();
        large_restore.restore_manifest.logical_size = MAX_RESTORE_MANIFEST_BYTES as u64 + 1;
        assert_eq!(
            large_restore.validate(),
            Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "published restore manifest exceeds its body bound",
            })
        );
    }

    #[test]
    fn empty_materialized_workspace_still_requires_two_commit_members_and_revisions() {
        let mut record = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        record.source_cursor =
            Some(NormalizedRelativePath::new("metadata/run_manifest.json").unwrap());
        seal_empty_generic_index_source(&mut record);
        record.source_member_count = 1;
        record.source_member_rolling_digest = [7; SHA256_BYTES];
        record = record
            .apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    source_member_seal: [7; SHA256_BYTES],
                },
            )
            .unwrap()
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BindDestination { binding: binding() },
            )
            .unwrap()
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [3; SHA256_BYTES],
                    manifests: manifests(),
                },
            )
            .unwrap();
        assert_eq!(record.next_member_sequence, 0);
        assert_eq!(record.member_seal, Some([0; SHA256_BYTES]));
        let RestoreCommitProvenance::V5(provenance) = &mut record.commit_provenance else {
            unreachable!();
        };
        provenance.closure.member_cursor =
            Some(NormalizedRelativePath::new("metadata/run_manifest.json").unwrap());
        provenance.closure.member_count = 2;
        provenance.closure.member_digest = [9; SHA256_BYTES];
        provenance.closure.revision_ref_count = 2;
        provenance.closure.path_members_complete = true;
        provenance.closure.generic_indexes_complete = true;
        assert!(record
            .apply(
                RestorePhase::DestinationBuilding,
                RestoreTransition::BeginDestinationSealing {
                    member_seal: [9; SHA256_BYTES],
                },
            )
            .is_ok());
    }

    #[test]
    fn destination_commit_digest_is_independent_of_manifest_append_order() {
        fn roll(rows: [[u8; SHA256_BYTES]; 3]) -> [u8; SHA256_BYTES] {
            rows.into_iter()
                .enumerate()
                .fold([0; SHA256_BYTES], |digest, (sequence, row)| {
                    advance_commit_member_rolling_digest(digest, sequence as u64, row)
                })
        }

        let ordinary = [1; SHA256_BYTES];
        let run_manifest = [2; SHA256_BYTES];
        let restore_manifest = [3; SHA256_BYTES];
        // RestoreMember is append ordered: copied ordinary member, then the
        // two destination-owned publications.
        let publication_order_digest = roll([ordinary, run_manifest, restore_manifest]);
        // CommitMember is path ordered: metadata/restore_manifest.json,
        // metadata/run_manifest.json, then outputs/result.
        let canonical_commit_digest = roll([restore_manifest, run_manifest, ordinary]);
        assert_ne!(publication_order_digest, canonical_commit_digest);

        let mut building = destination_building_operation();
        let RestoreCommitProvenance::V5(provenance) = &mut building.commit_provenance else {
            unreachable!();
        };
        provenance.closure.member_cursor =
            Some(NormalizedRelativePath::new("outputs/result").unwrap());
        provenance.closure.member_count = 3;
        provenance.closure.member_digest = canonical_commit_digest;
        provenance.closure.revision_ref_count = 2;
        provenance.closure.path_members_complete = true;
        provenance.closure.generic_indexes_complete = true;

        let sealing = building
            .apply(
                RestorePhase::DestinationBuilding,
                RestoreTransition::BeginDestinationSealing {
                    member_seal: canonical_commit_digest,
                },
            )
            .unwrap();
        assert!(sealing.encode().is_ok());
    }

    #[test]
    fn operation_identity_must_match_the_full_digest_prefix() {
        let mut record = operation(RestorePhase::Preparing);
        record.operation_id = OperationId::from_bytes([9; 16]);
        assert!(matches!(
            record.encode(),
            Err(RestoreRecordError::InvalidPhasePayload {
                reason: "operation id must equal the identity digest prefix",
                ..
            })
        ));
    }

    #[test]
    fn complete_state_machine_path_is_validated() {
        let mut record = destination_building_operation();
        let parent_seal = match &mut record.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => {
                let closure = &mut provenance.closure;
                closure.member_cursor =
                    Some(NormalizedRelativePath::new("metadata/run_manifest.json").unwrap());
                closure.member_count = 3;
                closure.member_digest = [9; SHA256_BYTES];
                closure.revision_ref_count = 2;
                closure.path_members_complete = true;
                closure.generic_indexes_complete = true;
                closure.parent_digest
            }
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        };
        record = record
            .apply(
                RestorePhase::DestinationBuilding,
                RestoreTransition::BeginDestinationSealing {
                    member_seal: [9; SHA256_BYTES],
                },
            )
            .unwrap();
        match &mut record.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => {
                let closure = &mut provenance.closure;
                closure.revision_cursor = Some(ArtifactRevisionId::from_bytes([0x32; 16]));
                closure.revision_seal_count = 2;
                closure.revision_digest = [11; SHA256_BYTES];
            }
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        }
        record = record
            .apply(
                RestorePhase::DestinationSealing,
                RestoreTransition::MarkReady {
                    revision_seal: [11; SHA256_BYTES],
                    parent_seal,
                },
            )
            .unwrap();
        let result = RestoreResult {
            destination_workspace_incarnation_id: record.destination_workspace_incarnation_id,
            destination_workspace_revision: WorkspaceRevision::new(1),
            member_count: 1,
            member_digest: [8; SHA256_BYTES],
        };
        record = record
            .apply(
                RestorePhase::Ready,
                RestoreTransition::Complete {
                    result,
                    destination_head_generation: Generation::new(1).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(record.phase, RestorePhase::Complete);
        assert_eq!(
            record.destination_commit_receipt().unwrap(),
            RestoreDestinationCommitReceipt {
                destination_commit_id: CommitId::from_bytes([4; SHA256_BYTES]),
                destination_head_generation: Generation::new(1).unwrap(),
            }
        );
        assert_eq!(
            RestoreOperationRecord::decode(&record.encode().unwrap()).unwrap(),
            record
        );
    }

    #[test]
    fn abort_cleanup_requires_complete_cursor() {
        let mut record = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        record.source_cursor = Some(NormalizedRelativePath::new("input/a").unwrap());
        record.source_member_count = 1;
        record.source_member_rolling_digest = [7; SHA256_BYTES];
        record.next_member_sequence = 1;
        record.member_rolling_digest = [8; SHA256_BYTES];
        record = record
            .apply(
                RestorePhase::Copying,
                RestoreTransition::BeginAbort {
                    terminal_error: terminal_error(),
                },
            )
            .unwrap()
            .apply(RestorePhase::Aborting, RestoreTransition::BeginCleaning)
            .unwrap();
        assert!(matches!(
            record.apply(RestorePhase::Cleaning, RestoreTransition::FinishCleanup),
            Err(RestoreRecordError::InvalidPhaseTransition { .. })
        ));
        record.cleanup_member_cursor = 1;
        record = record
            .apply(RestorePhase::Cleaning, RestoreTransition::FinishCleanup)
            .unwrap();
        assert_eq!(record.phase, RestorePhase::Cleaned);
    }

    #[test]
    fn member_round_trip_and_generation_zero_fail_closed() {
        let record = RestoreMemberRecord {
            destination_path: NormalizedRelativePath::new("logs/worker-0").unwrap(),
            artifact_revision_id: ArtifactRevisionId::from_bytes([3; 16]),
            path_generation: Generation::new(4).unwrap(),
            row_digest: [5; SHA256_BYTES],
        };
        let encoded = record.encode();
        assert_eq!(RestoreMemberRecord::decode(&encoded).unwrap(), record);

        let mut invalid = encoded;
        let generation_offset = 1 + 4 + "logs/worker-0".len() + 16;
        invalid[generation_offset..generation_offset + 8].fill(0);
        assert_eq!(
            RestoreMemberRecord::decode(&invalid),
            Err(RestoreRecordError::ZeroScalar {
                field: "path_generation",
            })
        );
    }

    #[test]
    fn unknown_source_and_phase_fail_closed() {
        let record = operation(RestorePhase::Preparing);
        let mut encoded = record.encode().unwrap();
        let source_kind_offset = 1 + 16 + SHA256_BYTES + 1 + 4 + "source".len() + 16;
        encoded[source_kind_offset] = 0xff;
        assert_eq!(
            RestoreOperationRecord::decode(&encoded),
            Err(RestoreRecordError::UnknownDiscriminant {
                type_name: "RestoreSourceKind",
                value: 0xff,
            })
        );

        let mut encoded = record.encode().unwrap();
        let phase_offset = v5_phase_offset(&encoded);
        encoded[phase_offset] = 0xff;
        assert_eq!(
            RestoreOperationRecord::decode(&encoded),
            Err(RestoreRecordError::UnknownDiscriminant {
                type_name: "RestorePhase",
                value: 0xff,
            })
        );
    }

    #[test]
    fn manifest_descriptor_is_durable_and_strict() {
        let record = operation(RestorePhase::Preparing);
        let decoded = RestoreOperationRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(decoded.restore_manifest, record.restore_manifest);

        let mut invalid = record;
        invalid.restore_manifest.body_digest_uri = "sha256:ABC".to_owned();
        assert_eq!(
            invalid.encode(),
            Err(RestoreRecordError::InvalidManifestDescriptor {
                reason: "body_digest_uri must be canonical lowercase SHA-256",
            })
        );
    }
}
