// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//! Strict durable payloads for object-first artifact publication.
//!
//! These payloads deliberately omit `created_version` and `modified_version`.
//! The metadata executor supplies those fields through the `CurrentValue`
//! envelope. Publication workers must CAS that exact envelope when applying a
//! [`PublishTransition`]; the transition helper validates the state-machine
//! edge, while the executor's CAS makes `Finalizing` and `Aborting` single
//! winners.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, Generation, NormalizedRelativePath, OperationId, OwnerEpoch, PublishPhase,
    StagedCleanupState, StagedProviderState, WorkbenchId, WorkspaceIncarnationId,
    WorkspaceRevision, SHA256_BYTES,
};

/// Durable value format for publication-owned payloads.
pub const PUBLISH_VALUE_FORMAT_VERSION: u8 = 4;

/// Hard safety bound for one publish operation's staged-object ledger.
pub const MAX_STAGED_OBJECTS: u32 = 1_048_576;
/// Hard safety bound for one immutable revision manifest.
pub const MAX_MANIFEST_ROWS: u32 = 1_048_576;
/// Maximum distinct physical owner revisions retained by one child revision.
pub const MAX_DEPENDENCY_COUNT: u8 = 64;
/// Maximum sealed revision-dependency graph depth.
pub const MAX_DEPENDENCY_DEPTH: u8 = 8;
/// Maximum append-segment sequence admitted by the durable publication codec.
pub const MAX_APPEND_SEGMENTS: u32 = 1_048_576;

pub const MAX_MULTIPART_ID_BYTES: usize = 1_024;
pub const MAX_DIGEST_URI_BYTES: usize = 256;
pub const MAX_TERMINAL_ERROR_BYTES: usize = 4_096;

const OBJECT_KEY_BYTES: usize = 137;
const MAX_PUBLISH_OPERATION_RECORD_BYTES: usize = 16 * 1_024;
const MAX_STAGED_OBJECT_RECORD_BYTES: usize = 4 * 1_024;
const MAX_MANIFEST_ROW_BYTES: usize = 2 * 1_024;

/// Path-level publication predicate sealed into the operation identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishClaim {
    /// The normalized path must not exist.
    CreateOnly,
    /// The normalized path must exist at the exact generation.
    ReplaceOnly { expected_generation: Generation },
    /// The path and immutable base revision must still match before append.
    Append {
        expected_generation: Generation,
        base_revision_id: ArtifactRevisionId,
        append_offset: u64,
    },
}

/// Durable authority under which one publication may mutate a workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishAuthority {
    /// Normal publication into the current visible workspace incarnation.
    Visible,
    /// A dependency-free canonical run manifest retained by one commit build.
    /// Publication creates the immutable revision and commit-owned ref only;
    /// the build later installs `PathCurrent` with its typed head atomically.
    CommitStaging { commit_operation_id: OperationId },
    /// The one reserved restore manifest published while a destination remains
    /// hidden under its owning restore operation.
    RestoreStaging { restore_operation_id: OperationId },
}

/// Deterministic result retained for exact response-loss replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishResult {
    pub path_generation: Generation,
    pub workspace_revision: WorkspaceRevision,
    pub logical_size: u64,
    pub body_digest_uri: String,
}

/// Stable class of a terminal publication failure.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishTerminalErrorKind {
    UploadFailed = 1,
    VerificationFailed = 2,
    PublishConflict = 3,
    AbortedByCaller = 4,
    ProviderAmbiguous = 5,
    InvariantViolation = 6,
    CleanupFailed = 7,
    OwnerEpochSuperseded = 8,
    ActivityLeaseExpired = 9,
}

impl TryFrom<u8> for PublishTerminalErrorKind {
    type Error = PublishRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::UploadFailed),
            2 => Ok(Self::VerificationFailed),
            3 => Ok(Self::PublishConflict),
            4 => Ok(Self::AbortedByCaller),
            5 => Ok(Self::ProviderAmbiguous),
            6 => Ok(Self::InvariantViolation),
            7 => Ok(Self::CleanupFailed),
            8 => Ok(Self::OwnerEpochSuperseded),
            9 => Ok(Self::ActivityLeaseExpired),
            value => Err(PublishRecordError::UnknownDiscriminant {
                type_name: "PublishTerminalErrorKind",
                value,
            }),
        }
    }
}

impl From<PublishTerminalErrorKind> for u8 {
    fn from(value: PublishTerminalErrorKind) -> Self {
        value as u8
    }
}

/// Stable terminal error plus bounded reconciliation evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishTerminalError {
    pub kind: PublishTerminalErrorKind,
    pub message: String,
    pub evidence_digest: Option<[u8; SHA256_BYTES]>,
}

/// Durable publish operation payload.
///
/// Both full identity digests are retained so a 16-byte operation-id collision
/// and an exact-identity initialization mismatch remain distinguishable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishOperationRecord {
    pub operation_id: OperationId,
    pub identity_digest: [u8; SHA256_BYTES],
    pub initialization_digest: [u8; SHA256_BYTES],
    /// Root-owner epoch that durably admitted this operation. A strictly newer
    /// owner may recover immediately; this initiating owner remains subject to
    /// the durable activity lease below.
    pub initiating_owner_epoch: OwnerEpoch,
    /// Extend-only durable activity lease for the initiating owner. A worker
    /// in the same epoch may claim an orphan only after this deadline; a newer
    /// fenced owner does not wait for it.
    pub activity_deadline_ms: u64,
    pub authority: PublishAuthority,
    pub workbench_id: WorkbenchId,
    pub workspace_incarnation_id: WorkspaceIncarnationId,
    pub path: NormalizedRelativePath,
    pub artifact_revision_id: ArtifactRevisionId,
    pub claim: PublishClaim,
    pub phase: PublishPhase,

    /// Exact final staged-ledger closure, known before any provider write.
    pub staged_object_count: u32,
    pub staged_object_seal: [u8; SHA256_BYTES],
    /// Number and rolling digest of staged rows durably created so far.
    pub staged_object_cursor: u32,
    pub staged_object_rolling_digest: [u8; SHA256_BYTES],
    /// Contiguous staged rows durably confirmed uploaded so far.
    pub uploaded_object_cursor: u32,
    pub uploaded_object_rolling_digest: [u8; SHA256_BYTES],

    /// Exact final immutable-manifest closure.
    pub manifest_row_count: u32,
    pub manifest_seal: [u8; SHA256_BYTES],
    /// Number, rolling digest, and final key of manifest rows staged so far.
    pub manifest_cursor: u32,
    pub manifest_rolling_digest: [u8; SHA256_BYTES],
    pub manifest_last_position: Option<ManifestPosition>,

    /// Sealed, bounded physical-owner dependency closure.
    pub dependency_count: u8,
    pub dependency_depth: u8,
    pub dependency_digest: [u8; SHA256_BYTES],

    /// Number of staged-object and manifest rows durably removed by cleanup.
    pub cleanup_staged_object_cursor: u32,
    pub cleanup_manifest_cursor: u32,

    /// Required only when cleanup takes over a `Finalizing` operation.
    pub publication_absence_proof: Option<[u8; SHA256_BYTES]>,
    /// Present only in `Published`.
    pub result: Option<PublishResult>,
    /// Present throughout `Aborting`/`Cleaning` and their terminal states.
    pub terminal_error: Option<PublishTerminalError>,
}

/// Last ordered manifest key included in the durable rolling closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestPosition {
    pub object_index: u64,
}

/// One exact object owned by a publish operation before metadata visibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedObjectRecord {
    pub artifact_revision_id: ArtifactRevisionId,
    pub object_sequence: u32,
    pub object_key: String,
    pub multipart_upload_id: Option<Vec<u8>>,
    pub expected_length: u64,
    pub expected_digest_uri: String,
    pub provider_state: StagedProviderState,
    pub cleanup_state: StagedCleanupState,
}

/// Optional immutable append-delta coordinate carried by one manifest row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendSegment {
    pub segment_sequence: u32,
    pub segment_offset: u64,
}

/// One direct immutable-object range in an artifact revision manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactManifestRow {
    /// The revision whose permanent object namespace physically owns the bytes.
    pub physical_owner_revision_id: ArtifactRevisionId,
    /// Owner-local index in that revision's permanent object namespace.
    ///
    /// This is intentionally independent from the child manifest position:
    /// append rebases logical manifest positions but preserves physical keys.
    pub physical_object_index: u64,
    pub object_key: String,
    /// Start offset in the complete logical artifact.
    pub logical_offset: u64,
    /// Start offset inside the immutable physical object.
    pub offset: u64,
    pub length: u64,
    pub digest_uri: String,
    pub append_segment: Option<AppendSegment>,
}

/// Validated state-machine action applied against an exact observed phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishTransition {
    BeginFinalization,
    Publish {
        result: PublishResult,
    },
    BeginAbort {
        terminal_error: PublishTerminalError,
    },
    BeginCleaning,
    FinishCleanup,
    Quarantine {
        terminal_error: PublishTerminalError,
    },
}

/// Strict format, invariant, or state-machine failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishRecordError {
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
    InvalidPath {
        reason: String,
    },
    InvalidWorkbenchId {
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
    InvalidAsciiField {
        field: &'static str,
    },
    RecordTooLarge {
        record: &'static str,
        length: usize,
        max: usize,
    },
    CountOutOfRange {
        field: &'static str,
        value: u64,
        max: u64,
    },
    ZeroScalar {
        field: &'static str,
    },
    CursorOutOfRange {
        field: &'static str,
        cursor: u32,
        count: u32,
    },
    InvalidProgress {
        field: &'static str,
        reason: &'static str,
    },
    InvalidDependencyShape {
        count: u8,
        depth: u8,
    },
    InvalidPhasePayload {
        phase: PublishPhase,
        reason: &'static str,
    },
    PhaseMismatch {
        expected: PublishPhase,
        actual: PublishPhase,
    },
    InvalidPhaseTransition {
        from: PublishPhase,
        to: PublishPhase,
    },
    InvalidStagedStateCombination {
        provider: StagedProviderState,
        cleanup: StagedCleanupState,
    },
    InvalidObjectKey {
        reason: &'static str,
    },
    ObjectKeyRevisionMismatch,
    ObjectKeyIndexMismatch {
        expected: u64,
        actual: u64,
    },
    RangeOverflow {
        field: &'static str,
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

impl fmt::Display for PublishRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported publication value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidPath { reason } => write!(formatter, "invalid publication path: {reason}"),
            Self::InvalidWorkbenchId { reason } => {
                write!(formatter, "invalid publication workbench id: {reason}")
            }
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::FieldTooLong { field, length, max } => {
                write!(formatter, "{field} is {length} bytes, maximum is {max}")
            }
            Self::InvalidAsciiField { field } => {
                write!(formatter, "{field} must contain visible ASCII without NUL")
            }
            Self::RecordTooLarge {
                record,
                length,
                max,
            } => write!(formatter, "{record} is {length} bytes, maximum is {max}"),
            Self::CountOutOfRange { field, value, max } => {
                write!(formatter, "{field} {value} exceeds maximum {max}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::CursorOutOfRange {
                field,
                cursor,
                count,
            } => write!(formatter, "{field} {cursor} exceeds sealed count {count}"),
            Self::InvalidProgress { field, reason } => {
                write!(formatter, "invalid {field} progress: {reason}")
            }
            Self::InvalidDependencyShape { count, depth } => write!(
                formatter,
                "dependency count/depth pair ({count}, {depth}) is not a sealed v1 closure"
            ),
            Self::InvalidPhasePayload { phase, reason } => {
                write!(formatter, "invalid {phase:?} payload: {reason}")
            }
            Self::PhaseMismatch { expected, actual } => {
                write!(
                    formatter,
                    "publish phase mismatch: expected {expected:?}, actual {actual:?}"
                )
            }
            Self::InvalidPhaseTransition { from, to } => {
                write!(formatter, "invalid publish transition {from:?} -> {to:?}")
            }
            Self::InvalidStagedStateCombination { provider, cleanup } => write!(
                formatter,
                "invalid staged-object state pair {provider:?}/{cleanup:?}"
            ),
            Self::InvalidObjectKey { reason } => write!(formatter, "invalid object key: {reason}"),
            Self::ObjectKeyRevisionMismatch => {
                formatter.write_str("object key revision does not match its physical owner")
            }
            Self::ObjectKeyIndexMismatch { expected, actual } => write!(
                formatter,
                "object key index {actual} does not match physical object index {expected}"
            ),
            Self::RangeOverflow { field } => write!(formatter, "{field} range overflows u64"),
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(formatter, "publication value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for PublishRecordError {}

impl PublishOperationRecord {
    pub fn validate(&self) -> Result<(), PublishRecordError> {
        if self.activity_deadline_ms == 0 {
            return Err(PublishRecordError::ZeroScalar {
                field: "activity_deadline_ms",
            });
        }
        if self.staged_object_count > MAX_STAGED_OBJECTS {
            return Err(PublishRecordError::CountOutOfRange {
                field: "staged_object_count",
                value: u64::from(self.staged_object_count),
                max: u64::from(MAX_STAGED_OBJECTS),
            });
        }
        if self.manifest_row_count > MAX_MANIFEST_ROWS {
            return Err(PublishRecordError::CountOutOfRange {
                field: "manifest_row_count",
                value: u64::from(self.manifest_row_count),
                max: u64::from(MAX_MANIFEST_ROWS),
            });
        }
        validate_dependency_shape(self.dependency_count, self.dependency_depth)?;
        require_cursor(
            "staged_object_cursor",
            self.staged_object_cursor,
            self.staged_object_count,
        )?;
        require_cursor(
            "uploaded_object_cursor",
            self.uploaded_object_cursor,
            self.staged_object_cursor,
        )?;
        require_cursor(
            "manifest_cursor",
            self.manifest_cursor,
            self.manifest_row_count,
        )?;
        require_cursor(
            "cleanup_staged_object_cursor",
            self.cleanup_staged_object_cursor,
            self.staged_object_cursor,
        )?;
        require_cursor(
            "cleanup_manifest_cursor",
            self.cleanup_manifest_cursor,
            self.manifest_cursor,
        )?;
        validate_progress_digest(
            "staged_object",
            self.staged_object_count,
            self.staged_object_seal,
            self.staged_object_cursor,
            self.staged_object_rolling_digest,
        )?;
        validate_progress_digest(
            "uploaded_object",
            self.staged_object_count,
            self.staged_object_seal,
            self.uploaded_object_cursor,
            self.uploaded_object_rolling_digest,
        )?;
        validate_progress_digest(
            "manifest",
            self.manifest_row_count,
            self.manifest_seal,
            self.manifest_cursor,
            self.manifest_rolling_digest,
        )?;
        match (self.manifest_cursor, self.manifest_last_position) {
            (0, None) | (1.., Some(_)) => {}
            (0, Some(_)) => {
                return Err(PublishRecordError::InvalidProgress {
                    field: "manifest_last_position",
                    reason: "must be absent at cursor zero",
                });
            }
            (1.., None) => {
                return Err(PublishRecordError::InvalidProgress {
                    field: "manifest_last_position",
                    reason: "must identify the final row at a non-zero cursor",
                });
            }
        }
        validate_claim(&self.claim)?;

        if let Some(result) = &self.result {
            validate_publish_result(result)?;
        }
        if let Some(error) = &self.terminal_error {
            validate_terminal_error(error)?;
        }

        match self.phase {
            PublishPhase::Uploading => {
                require_phase(
                    self,
                    self.cleanup_staged_object_cursor == 0 && self.cleanup_manifest_cursor == 0,
                    "cleanup cursors must be zero before abort wins",
                )?;
                require_phase(
                    self,
                    self.publication_absence_proof.is_none(),
                    "publication-absence proof is only retained on the abort branch",
                )?;
                require_phase(self, self.result.is_none(), "result is terminal-only")?;
                require_phase(
                    self,
                    self.terminal_error.is_none(),
                    "terminal error is abort-branch-only",
                )?;
            }
            PublishPhase::Finalizing => {
                require_phase(
                    self,
                    self.has_complete_publication_closure(),
                    "staged, uploaded, and manifest seals must be complete before finalization",
                )?;
                require_phase(
                    self,
                    self.cleanup_staged_object_cursor == 0 && self.cleanup_manifest_cursor == 0,
                    "cleanup cursors must be zero during finalization",
                )?;
                require_phase(
                    self,
                    self.publication_absence_proof.is_none(),
                    "publication-absence proof belongs to takeover",
                )?;
                require_phase(self, self.result.is_none(), "result is not published yet")?;
                require_phase(
                    self,
                    self.terminal_error.is_none(),
                    "terminal error belongs to abort",
                )?;
            }
            PublishPhase::Published => {
                require_phase(
                    self,
                    self.has_complete_publication_closure(),
                    "published operation must retain the exact publication closure",
                )?;
                require_phase(
                    self,
                    self.cleanup_staged_object_cursor == 0 && self.cleanup_manifest_cursor == 0,
                    "published operation cannot own cleanup cursors",
                )?;
                require_phase(
                    self,
                    self.publication_absence_proof.is_none(),
                    "published operation cannot carry absence proof",
                )?;
                require_phase(self, self.result.is_some(), "published result is required")?;
                require_phase(
                    self,
                    self.terminal_error.is_none(),
                    "published operation cannot carry terminal error",
                )?;
            }
            PublishPhase::Aborting => {
                require_phase(
                    self,
                    self.cleanup_staged_object_cursor == 0 && self.cleanup_manifest_cursor == 0,
                    "cleanup starts only after the Cleaning transition",
                )?;
                require_phase(
                    self,
                    self.result.is_none(),
                    "abort cannot carry success result",
                )?;
                require_phase(
                    self,
                    self.terminal_error.is_some(),
                    "abort reason is required",
                )?;
            }
            PublishPhase::Cleaning => {
                require_phase(
                    self,
                    self.result.is_none(),
                    "cleanup cannot carry success result",
                )?;
                require_phase(
                    self,
                    self.terminal_error.is_some(),
                    "cleanup must retain the terminal error",
                )?;
            }
            PublishPhase::Cleaned => {
                require_phase(
                    self,
                    self.cleanup_staged_object_cursor == self.staged_object_cursor
                        && self.cleanup_manifest_cursor == self.manifest_cursor,
                    "cleaned operation must seal every durable staging row",
                )?;
                require_phase(
                    self,
                    self.result.is_none(),
                    "cleaned operation cannot carry success result",
                )?;
                require_phase(
                    self,
                    self.terminal_error.is_some(),
                    "cleaned operation must retain the terminal error",
                )?;
            }
            PublishPhase::Quarantined => {
                require_phase(
                    self,
                    self.result.is_none(),
                    "quarantined operation cannot carry success result",
                )?;
                require_phase(
                    self,
                    self.terminal_error.is_some(),
                    "quarantined operation must retain the terminal error",
                )?;
                require_phase(
                    self,
                    self.terminal_error
                        .as_ref()
                        .is_some_and(|error| error.evidence_digest.is_some()),
                    "quarantine requires reconciliation evidence",
                )?;
            }
        }
        Ok(())
    }

    pub fn has_complete_publication_closure(&self) -> bool {
        self.staged_object_cursor == self.staged_object_count
            && self.staged_object_rolling_digest == self.staged_object_seal
            && self.uploaded_object_cursor == self.staged_object_count
            && self.uploaded_object_rolling_digest == self.staged_object_seal
            && self.manifest_cursor == self.manifest_row_count
            && self.manifest_rolling_digest == self.manifest_seal
    }

    pub fn encode(&self) -> Result<Vec<u8>, PublishRecordError> {
        self.validate()?;
        let path = self.path.as_str().as_bytes();
        let mut encoded = Vec::with_capacity(256 + path.len());
        encoded.push(PUBLISH_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.operation_id.as_bytes());
        encoded.extend_from_slice(&self.identity_digest);
        encoded.extend_from_slice(&self.initialization_digest);
        encoded.extend_from_slice(&self.initiating_owner_epoch.get().to_be_bytes());
        encoded.extend_from_slice(&self.activity_deadline_ms.to_be_bytes());
        match self.authority {
            PublishAuthority::Visible => encoded.push(1),
            PublishAuthority::CommitStaging {
                commit_operation_id,
            } => {
                encoded.push(3);
                encoded.extend_from_slice(commit_operation_id.as_bytes());
            }
            PublishAuthority::RestoreStaging {
                restore_operation_id,
            } => {
                encoded.push(2);
                encoded.extend_from_slice(restore_operation_id.as_bytes());
            }
        }
        push_bytes(&mut encoded, "workbench_id", self.workbench_id.as_bytes())?;
        encoded.extend_from_slice(self.workspace_incarnation_id.as_bytes());
        push_bytes(&mut encoded, "path", path)?;
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        encode_claim(&mut encoded, &self.claim);
        encoded.push(self.phase.into());
        encoded.extend_from_slice(&self.staged_object_count.to_be_bytes());
        encoded.extend_from_slice(&self.staged_object_seal);
        encoded.extend_from_slice(&self.staged_object_cursor.to_be_bytes());
        encoded.extend_from_slice(&self.staged_object_rolling_digest);
        encoded.extend_from_slice(&self.uploaded_object_cursor.to_be_bytes());
        encoded.extend_from_slice(&self.uploaded_object_rolling_digest);
        encoded.extend_from_slice(&self.manifest_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.manifest_seal);
        encoded.extend_from_slice(&self.manifest_cursor.to_be_bytes());
        encoded.extend_from_slice(&self.manifest_rolling_digest);
        match self.manifest_last_position {
            None => encoded.push(0),
            Some(position) => {
                encoded.push(1);
                encoded.extend_from_slice(&position.object_index.to_be_bytes());
            }
        }
        encoded.push(self.dependency_count);
        encoded.push(self.dependency_depth);
        encoded.extend_from_slice(&self.dependency_digest);
        encoded.extend_from_slice(&self.cleanup_staged_object_cursor.to_be_bytes());
        encoded.extend_from_slice(&self.cleanup_manifest_cursor.to_be_bytes());
        push_optional_fixed(&mut encoded, &self.publication_absence_proof);
        push_optional_result(&mut encoded, self.result.as_ref())?;
        push_optional_terminal_error(&mut encoded, self.terminal_error.as_ref())?;
        require_record_size(
            "PublishOperationRecord",
            encoded.len(),
            MAX_PUBLISH_OPERATION_RECORD_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublishRecordError> {
        require_record_size(
            "PublishOperationRecord",
            encoded.len(),
            MAX_PUBLISH_OPERATION_RECORD_BYTES,
        )?;
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
        let identity_digest = decoder.fixed("identity_digest")?;
        let initialization_digest = decoder.fixed("initialization_digest")?;
        let initiating_owner_epoch = OwnerEpoch::new(decoder.u64("initiating_owner_epoch")?)
            .map_err(|_| PublishRecordError::ZeroScalar {
                field: "initiating_owner_epoch",
            })?;
        let activity_deadline_ms = decoder.u64("activity_deadline_ms")?;
        let authority = match decoder.u8("authority")? {
            1 => PublishAuthority::Visible,
            2 => PublishAuthority::RestoreStaging {
                restore_operation_id: OperationId::from_bytes(
                    decoder.fixed("authority.restore_operation_id")?,
                ),
            },
            3 => PublishAuthority::CommitStaging {
                commit_operation_id: OperationId::from_bytes(
                    decoder.fixed("authority.commit_operation_id")?,
                ),
            },
            value => {
                return Err(PublishRecordError::UnknownDiscriminant {
                    type_name: "PublishAuthority",
                    value,
                });
            }
        };
        let workbench_bytes = decoder.bytes("workbench_id", WorkbenchId::MAX_BYTES)?;
        let workbench_value =
            String::from_utf8(workbench_bytes).map_err(|_| PublishRecordError::InvalidUtf8 {
                field: "workbench_id",
            })?;
        let workbench_id = WorkbenchId::new(workbench_value).map_err(|error| {
            PublishRecordError::InvalidWorkbenchId {
                reason: error.to_string(),
            }
        })?;
        let workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes(decoder.fixed("workspace_incarnation_id")?);
        let path_bytes = decoder.bytes("path", NormalizedRelativePath::MAX_BYTES)?;
        let path_string = String::from_utf8(path_bytes)
            .map_err(|_| PublishRecordError::InvalidUtf8 { field: "path" })?;
        let path = NormalizedRelativePath::new(path_string).map_err(|error| {
            PublishRecordError::InvalidPath {
                reason: error.to_string(),
            }
        })?;
        let artifact_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("artifact_revision_id")?);
        let claim = decode_claim(&mut decoder)?;
        let phase = decode_publish_phase(decoder.u8("phase")?)?;
        let staged_object_count = decoder.u32("staged_object_count")?;
        let staged_object_seal = decoder.fixed("staged_object_seal")?;
        let staged_object_cursor = decoder.u32("staged_object_cursor")?;
        let staged_object_rolling_digest = decoder.fixed("staged_object_rolling_digest")?;
        let uploaded_object_cursor = decoder.u32("uploaded_object_cursor")?;
        let uploaded_object_rolling_digest = decoder.fixed("uploaded_object_rolling_digest")?;
        let manifest_row_count = decoder.u32("manifest_row_count")?;
        let manifest_seal = decoder.fixed("manifest_seal")?;
        let manifest_cursor = decoder.u32("manifest_cursor")?;
        let manifest_rolling_digest = decoder.fixed("manifest_rolling_digest")?;
        let manifest_last_position = match decoder.u8("manifest_last_position.tag")? {
            0 => None,
            1 => Some(ManifestPosition {
                object_index: decoder.u64("manifest_last_position.object_index")?,
            }),
            value => {
                return Err(PublishRecordError::InvalidOptionalTag {
                    field: "manifest_last_position",
                    value,
                });
            }
        };
        let dependency_count = decoder.u8("dependency_count")?;
        let dependency_depth = decoder.u8("dependency_depth")?;
        let dependency_digest = decoder.fixed("dependency_digest")?;
        let cleanup_staged_object_cursor = decoder.u32("cleanup_staged_object_cursor")?;
        let cleanup_manifest_cursor = decoder.u32("cleanup_manifest_cursor")?;
        let publication_absence_proof = decoder.optional_fixed("publication_absence_proof")?;
        let result = decode_optional_result(&mut decoder)?;
        let terminal_error = decode_optional_terminal_error(&mut decoder)?;
        decoder.finish()?;

        let record = Self {
            operation_id,
            identity_digest,
            initialization_digest,
            initiating_owner_epoch,
            activity_deadline_ms,
            authority,
            workbench_id,
            workspace_incarnation_id,
            path,
            artifact_revision_id,
            claim,
            phase,
            staged_object_count,
            staged_object_seal,
            staged_object_cursor,
            staged_object_rolling_digest,
            uploaded_object_cursor,
            uploaded_object_rolling_digest,
            manifest_row_count,
            manifest_seal,
            manifest_cursor,
            manifest_rolling_digest,
            manifest_last_position,
            dependency_count,
            dependency_depth,
            dependency_digest,
            cleanup_staged_object_cursor,
            cleanup_manifest_cursor,
            publication_absence_proof,
            result,
            terminal_error,
        };
        record.validate()?;
        Ok(record)
    }

    /// Applies one legal transition against an exact observed phase.
    ///
    /// Callers persist the returned state by comparing the complete enclosing
    /// `CurrentValue`. Thus two callers observing `Uploading` cannot both win
    /// `BeginFinalization` and `BeginAbort`.
    pub fn apply_transition(
        &mut self,
        expected_phase: PublishPhase,
        transition: PublishTransition,
    ) -> Result<(), PublishRecordError> {
        self.validate()?;
        if self.phase != expected_phase {
            return Err(PublishRecordError::PhaseMismatch {
                expected: expected_phase,
                actual: self.phase,
            });
        }

        let mut next = self.clone();
        match transition {
            PublishTransition::BeginFinalization if expected_phase == PublishPhase::Uploading => {
                if !next.has_complete_publication_closure() {
                    return Err(PublishRecordError::InvalidPhasePayload {
                        phase: expected_phase,
                        reason:
                            "staged, uploaded, and manifest seals must be complete before finalization",
                    });
                }
                next.phase = PublishPhase::Finalizing;
            }
            PublishTransition::Publish { result } if expected_phase == PublishPhase::Finalizing => {
                validate_publish_result(&result)?;
                next.phase = PublishPhase::Published;
                next.result = Some(result);
            }
            PublishTransition::BeginAbort { terminal_error }
                if expected_phase == PublishPhase::Uploading =>
            {
                validate_terminal_error(&terminal_error)?;
                next.phase = PublishPhase::Aborting;
                next.terminal_error = Some(terminal_error);
            }
            PublishTransition::BeginCleaning if expected_phase == PublishPhase::Aborting => {
                next.phase = PublishPhase::Cleaning;
            }
            PublishTransition::FinishCleanup if expected_phase == PublishPhase::Cleaning => {
                if next.cleanup_staged_object_cursor != next.staged_object_cursor
                    || next.cleanup_manifest_cursor != next.manifest_cursor
                {
                    return Err(PublishRecordError::InvalidPhasePayload {
                        phase: expected_phase,
                        reason: "all durable staging rows must be removed before Cleaned",
                    });
                }
                next.phase = PublishPhase::Cleaned;
            }
            PublishTransition::Quarantine { terminal_error }
                if expected_phase == PublishPhase::Cleaning =>
            {
                validate_terminal_error(&terminal_error)?;
                if terminal_error.evidence_digest.is_none() {
                    return Err(PublishRecordError::InvalidPhasePayload {
                        phase: PublishPhase::Quarantined,
                        reason: "quarantine requires reconciliation evidence",
                    });
                }
                next.phase = PublishPhase::Quarantined;
                next.terminal_error = Some(terminal_error);
            }
            transition => {
                return Err(PublishRecordError::InvalidPhaseTransition {
                    from: expected_phase,
                    to: transition.target_phase(),
                })
            }
        }
        next.validate()?;
        *self = next;
        Ok(())
    }

    /// Applies the metadata-service-owned `Finalizing -> Aborting` edge. The
    /// caller must derive `publication_absence_proof` from the exact
    /// `Finalizing` payload that is CASed by the same metadata command.
    pub(super) fn take_over_finalization(
        &mut self,
        publication_absence_proof: [u8; SHA256_BYTES],
        terminal_error: PublishTerminalError,
    ) -> Result<(), PublishRecordError> {
        self.validate()?;
        if self.phase != PublishPhase::Finalizing {
            return Err(PublishRecordError::PhaseMismatch {
                expected: PublishPhase::Finalizing,
                actual: self.phase,
            });
        }
        validate_terminal_error(&terminal_error)?;
        let mut next = self.clone();
        next.phase = PublishPhase::Aborting;
        next.publication_absence_proof = Some(publication_absence_proof);
        next.terminal_error = Some(terminal_error);
        next.validate()?;
        *self = next;
        Ok(())
    }
}

impl PublishTransition {
    fn target_phase(&self) -> PublishPhase {
        match self {
            Self::BeginFinalization => PublishPhase::Finalizing,
            Self::Publish { .. } => PublishPhase::Published,
            Self::BeginAbort { .. } => PublishPhase::Aborting,
            Self::BeginCleaning => PublishPhase::Cleaning,
            Self::FinishCleanup => PublishPhase::Cleaned,
            Self::Quarantine { .. } => PublishPhase::Quarantined,
        }
    }
}

impl StagedObjectRecord {
    pub fn validate(&self) -> Result<(), PublishRecordError> {
        if self.object_sequence >= MAX_STAGED_OBJECTS {
            return Err(PublishRecordError::CountOutOfRange {
                field: "object_sequence",
                value: u64::from(self.object_sequence),
                max: u64::from(MAX_STAGED_OBJECTS - 1),
            });
        }
        validate_object_key(&self.object_key, self.artifact_revision_id)?;
        if let Some(upload_id) = &self.multipart_upload_id {
            validate_opaque_bytes("multipart_upload_id", upload_id, MAX_MULTIPART_ID_BYTES)?;
        }
        if self.expected_length == 0 {
            return Err(PublishRecordError::ZeroScalar {
                field: "expected_length",
            });
        }
        validate_digest_uri("expected_digest_uri", &self.expected_digest_uri)?;
        validate_staged_state_pair(self.provider_state, self.cleanup_state)
    }

    pub fn encode(&self) -> Result<Vec<u8>, PublishRecordError> {
        self.validate()?;
        let mut encoded = Vec::with_capacity(256);
        encoded.push(PUBLISH_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        encoded.extend_from_slice(&self.object_sequence.to_be_bytes());
        push_string(&mut encoded, "object_key", &self.object_key)?;
        push_optional_bytes(
            &mut encoded,
            "multipart_upload_id",
            self.multipart_upload_id.as_deref(),
        )?;
        encoded.extend_from_slice(&self.expected_length.to_be_bytes());
        push_string(
            &mut encoded,
            "expected_digest_uri",
            &self.expected_digest_uri,
        )?;
        encoded.push(self.provider_state.into());
        encoded.push(self.cleanup_state.into());
        require_record_size(
            "StagedObjectRecord",
            encoded.len(),
            MAX_STAGED_OBJECT_RECORD_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublishRecordError> {
        require_record_size(
            "StagedObjectRecord",
            encoded.len(),
            MAX_STAGED_OBJECT_RECORD_BYTES,
        )?;
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let artifact_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("artifact_revision_id")?);
        let object_sequence = decoder.u32("object_sequence")?;
        let object_key = decoder.string("object_key", OBJECT_KEY_BYTES)?;
        let multipart_upload_id =
            decoder.optional_bytes("multipart_upload_id", MAX_MULTIPART_ID_BYTES)?;
        let expected_length = decoder.u64("expected_length")?;
        let expected_digest_uri = decoder.string("expected_digest_uri", MAX_DIGEST_URI_BYTES)?;
        let provider_state = decode_provider_state(decoder.u8("provider_state")?)?;
        let cleanup_state = decode_cleanup_state(decoder.u8("cleanup_state")?)?;
        decoder.finish()?;
        let record = Self {
            artifact_revision_id,
            object_sequence,
            object_key,
            multipart_upload_id,
            expected_length,
            expected_digest_uri,
            provider_state,
            cleanup_state,
        };
        record.validate()?;
        Ok(record)
    }
}

impl ArtifactManifestRow {
    pub fn validate(&self) -> Result<(), PublishRecordError> {
        let actual_object_index =
            validate_object_key(&self.object_key, self.physical_owner_revision_id)?;
        if actual_object_index != self.physical_object_index {
            return Err(PublishRecordError::ObjectKeyIndexMismatch {
                expected: self.physical_object_index,
                actual: actual_object_index,
            });
        }
        if self.length == 0 {
            return Err(PublishRecordError::ZeroScalar { field: "length" });
        }
        self.logical_offset
            .checked_add(self.length)
            .ok_or(PublishRecordError::RangeOverflow {
                field: "logical range",
            })?;
        self.offset
            .checked_add(self.length)
            .ok_or(PublishRecordError::RangeOverflow {
                field: "object range",
            })?;
        validate_digest_uri("digest_uri", &self.digest_uri)?;
        if let Some(segment) = self.append_segment {
            if segment.segment_sequence >= MAX_APPEND_SEGMENTS {
                return Err(PublishRecordError::CountOutOfRange {
                    field: "append segment_sequence",
                    value: u64::from(segment.segment_sequence),
                    max: u64::from(MAX_APPEND_SEGMENTS - 1),
                });
            }
            segment.segment_offset.checked_add(self.length).ok_or(
                PublishRecordError::RangeOverflow {
                    field: "append segment range",
                },
            )?;
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, PublishRecordError> {
        self.validate()?;
        let mut encoded = Vec::with_capacity(256);
        encoded.push(PUBLISH_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.physical_owner_revision_id.as_bytes());
        encoded.extend_from_slice(&self.physical_object_index.to_be_bytes());
        push_string(&mut encoded, "object_key", &self.object_key)?;
        encoded.extend_from_slice(&self.logical_offset.to_be_bytes());
        encoded.extend_from_slice(&self.offset.to_be_bytes());
        encoded.extend_from_slice(&self.length.to_be_bytes());
        push_string(&mut encoded, "digest_uri", &self.digest_uri)?;
        match self.append_segment {
            None => encoded.push(0),
            Some(segment) => {
                encoded.push(1);
                encoded.extend_from_slice(&segment.segment_sequence.to_be_bytes());
                encoded.extend_from_slice(&segment.segment_offset.to_be_bytes());
            }
        }
        require_record_size("ArtifactManifestRow", encoded.len(), MAX_MANIFEST_ROW_BYTES)?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublishRecordError> {
        require_record_size("ArtifactManifestRow", encoded.len(), MAX_MANIFEST_ROW_BYTES)?;
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let physical_owner_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("physical_owner_revision_id")?);
        let physical_object_index = decoder.u64("physical_object_index")?;
        let object_key = decoder.string("object_key", OBJECT_KEY_BYTES)?;
        let logical_offset = decoder.u64("logical_offset")?;
        let offset = decoder.u64("offset")?;
        let length = decoder.u64("length")?;
        let digest_uri = decoder.string("digest_uri", MAX_DIGEST_URI_BYTES)?;
        let append_segment = match decoder.u8("append_segment tag")? {
            0 => None,
            1 => Some(AppendSegment {
                segment_sequence: decoder.u32("append segment_sequence")?,
                segment_offset: decoder.u64("append segment_offset")?,
            }),
            value => {
                return Err(PublishRecordError::InvalidOptionalTag {
                    field: "append_segment",
                    value,
                })
            }
        };
        decoder.finish()?;
        let record = Self {
            physical_owner_revision_id,
            physical_object_index,
            object_key,
            logical_offset,
            offset,
            length,
            digest_uri,
            append_segment,
        };
        record.validate()?;
        Ok(record)
    }
}

fn validate_claim(claim: &PublishClaim) -> Result<(), PublishRecordError> {
    match claim {
        PublishClaim::CreateOnly | PublishClaim::ReplaceOnly { .. } => Ok(()),
        PublishClaim::Append { .. } => Ok(()),
    }
}

fn validate_publish_result(result: &PublishResult) -> Result<(), PublishRecordError> {
    if result.workspace_revision == WorkspaceRevision::ZERO {
        return Err(PublishRecordError::ZeroScalar {
            field: "result.workspace_revision",
        });
    }
    validate_digest_uri("result.body_digest_uri", &result.body_digest_uri)
}

fn validate_terminal_error(error: &PublishTerminalError) -> Result<(), PublishRecordError> {
    validate_text(
        "terminal_error.message",
        &error.message,
        MAX_TERMINAL_ERROR_BYTES,
    )
}

fn validate_dependency_shape(count: u8, depth: u8) -> Result<(), PublishRecordError> {
    let valid = count <= MAX_DEPENDENCY_COUNT
        && depth <= MAX_DEPENDENCY_DEPTH
        && ((count == 0 && depth == 0) || (count > 0 && depth > 0));
    if valid {
        Ok(())
    } else {
        Err(PublishRecordError::InvalidDependencyShape { count, depth })
    }
}

fn require_phase(
    record: &PublishOperationRecord,
    condition: bool,
    reason: &'static str,
) -> Result<(), PublishRecordError> {
    if condition {
        Ok(())
    } else {
        Err(PublishRecordError::InvalidPhasePayload {
            phase: record.phase,
            reason,
        })
    }
}

fn require_cursor(field: &'static str, cursor: u32, count: u32) -> Result<(), PublishRecordError> {
    if cursor <= count {
        Ok(())
    } else {
        Err(PublishRecordError::CursorOutOfRange {
            field,
            cursor,
            count,
        })
    }
}

fn validate_progress_digest(
    field: &'static str,
    count: u32,
    seal: [u8; SHA256_BYTES],
    cursor: u32,
    rolling_digest: [u8; SHA256_BYTES],
) -> Result<(), PublishRecordError> {
    if cursor == 0 && rolling_digest != [0; SHA256_BYTES] {
        return Err(PublishRecordError::InvalidProgress {
            field,
            reason: "cursor zero requires the zero rolling digest",
        });
    }
    if count == 0 && seal != [0; SHA256_BYTES] {
        return Err(PublishRecordError::InvalidProgress {
            field,
            reason: "empty closure requires the zero seal",
        });
    }
    if cursor == count && rolling_digest != seal {
        return Err(PublishRecordError::InvalidProgress {
            field,
            reason: "complete cursor must match the final seal",
        });
    }
    Ok(())
}

fn validate_staged_state_pair(
    provider: StagedProviderState,
    cleanup: StagedCleanupState,
) -> Result<(), PublishRecordError> {
    let valid = matches!(
        (provider, cleanup),
        (
            StagedProviderState::Planned
                | StagedProviderState::Uploading
                | StagedProviderState::Uploaded,
            StagedCleanupState::Owned
        ) | (
            StagedProviderState::Planned
                | StagedProviderState::Uploading
                | StagedProviderState::Uploaded
                | StagedProviderState::AbortPending,
            StagedCleanupState::DeletePending
        ) | (StagedProviderState::Aborted, StagedCleanupState::Deleted)
            | (
                StagedProviderState::Ambiguous,
                StagedCleanupState::Quarantined
            )
    );
    if valid {
        Ok(())
    } else {
        Err(PublishRecordError::InvalidStagedStateCombination { provider, cleanup })
    }
}

fn validate_object_key(
    object_key: &str,
    expected_revision: ArtifactRevisionId,
) -> Result<u64, PublishRecordError> {
    if object_key.len() != OBJECT_KEY_BYTES {
        return Err(PublishRecordError::InvalidObjectKey {
            reason: "artifact object key has the wrong byte length",
        });
    }
    let mut components = object_key.split('/');
    if components.next() != Some("nokv") || components.next() != Some("artifacts") {
        return Err(PublishRecordError::InvalidObjectKey {
            reason: "artifact object key must start with nokv/artifacts/",
        });
    }
    let shard = components.next();
    let root = components.next();
    let revision = components.next();
    let blocks = components.next();
    let object_index = components.next();
    if components.next().is_some()
        || !shard.is_some_and(|value| is_lower_hex(value, 32))
        || !root.is_some_and(|value| is_lower_hex(value, 32))
        || !revision.is_some_and(|value| is_lower_hex(value, 32))
        || blocks != Some("blocks")
        || !object_index.is_some_and(|value| is_lower_hex(value, 16))
    {
        return Err(PublishRecordError::InvalidObjectKey {
            reason: "artifact object key components are malformed",
        });
    }
    if revision != Some(lower_hex(expected_revision.as_bytes()).as_str()) {
        return Err(PublishRecordError::ObjectKeyRevisionMismatch);
    }
    let object_index = object_index.expect("canonical object index was validated");
    u64::from_str_radix(object_index, 16).map_err(|_| PublishRecordError::InvalidObjectKey {
        reason: "artifact object key index exceeds u64",
    })
}

fn is_lower_hex(value: &str, width: usize) -> bool {
    value.len() == width
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn validate_digest_uri(field: &'static str, value: &str) -> Result<(), PublishRecordError> {
    validate_visible_ascii(field, value, MAX_DIGEST_URI_BYTES)
}

fn validate_visible_ascii(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), PublishRecordError> {
    validate_text(field, value, max)?;
    if value.bytes().all(|byte| matches!(byte, 0x21..=0x7e)) {
        Ok(())
    } else {
        Err(PublishRecordError::InvalidAsciiField { field })
    }
}

fn validate_text(field: &'static str, value: &str, max: usize) -> Result<(), PublishRecordError> {
    if value.is_empty() {
        return Err(PublishRecordError::EmptyField { field });
    }
    if value.len() > max {
        return Err(PublishRecordError::FieldTooLong {
            field,
            length: value.len(),
            max,
        });
    }
    if value.contains('\0') {
        return Err(PublishRecordError::InvalidAsciiField { field });
    }
    Ok(())
}

fn validate_opaque_bytes(
    field: &'static str,
    value: &[u8],
    max: usize,
) -> Result<(), PublishRecordError> {
    if value.is_empty() {
        return Err(PublishRecordError::EmptyField { field });
    }
    if value.len() > max {
        return Err(PublishRecordError::FieldTooLong {
            field,
            length: value.len(),
            max,
        });
    }
    Ok(())
}

fn require_record_size(
    record: &'static str,
    length: usize,
    max: usize,
) -> Result<(), PublishRecordError> {
    if length <= max {
        Ok(())
    } else {
        Err(PublishRecordError::RecordTooLarge {
            record,
            length,
            max,
        })
    }
}

fn encode_claim(target: &mut Vec<u8>, claim: &PublishClaim) {
    match claim {
        PublishClaim::CreateOnly => target.push(1),
        PublishClaim::ReplaceOnly {
            expected_generation,
        } => {
            target.push(2);
            target.extend_from_slice(&expected_generation.get().to_be_bytes());
        }
        PublishClaim::Append {
            expected_generation,
            base_revision_id,
            append_offset,
        } => {
            target.push(3);
            target.extend_from_slice(&expected_generation.get().to_be_bytes());
            target.extend_from_slice(base_revision_id.as_bytes());
            target.extend_from_slice(&append_offset.to_be_bytes());
        }
    }
}

fn decode_claim(decoder: &mut Decoder<'_>) -> Result<PublishClaim, PublishRecordError> {
    match decoder.u8("claim")? {
        1 => Ok(PublishClaim::CreateOnly),
        2 => Ok(PublishClaim::ReplaceOnly {
            expected_generation: decoder.generation("claim.expected_generation")?,
        }),
        3 => Ok(PublishClaim::Append {
            expected_generation: decoder.generation("claim.expected_generation")?,
            base_revision_id: ArtifactRevisionId::from_bytes(
                decoder.fixed("claim.base_revision_id")?,
            ),
            append_offset: decoder.u64("claim.append_offset")?,
        }),
        value => Err(PublishRecordError::UnknownDiscriminant {
            type_name: "PublishClaim",
            value,
        }),
    }
}

fn push_optional_result(
    target: &mut Vec<u8>,
    result: Option<&PublishResult>,
) -> Result<(), PublishRecordError> {
    match result {
        None => target.push(0),
        Some(result) => {
            target.push(1);
            target.extend_from_slice(&result.path_generation.get().to_be_bytes());
            target.extend_from_slice(&result.workspace_revision.get().to_be_bytes());
            target.extend_from_slice(&result.logical_size.to_be_bytes());
            push_string(target, "result.body_digest_uri", &result.body_digest_uri)?;
        }
    }
    Ok(())
}

fn decode_optional_result(
    decoder: &mut Decoder<'_>,
) -> Result<Option<PublishResult>, PublishRecordError> {
    match decoder.u8("result tag")? {
        0 => Ok(None),
        1 => Ok(Some(PublishResult {
            path_generation: decoder.generation("result.path_generation")?,
            workspace_revision: WorkspaceRevision::new(decoder.u64("result.workspace_revision")?),
            logical_size: decoder.u64("result.logical_size")?,
            body_digest_uri: decoder.string("result.body_digest_uri", MAX_DIGEST_URI_BYTES)?,
        })),
        value => Err(PublishRecordError::InvalidOptionalTag {
            field: "result",
            value,
        }),
    }
}

fn push_optional_terminal_error(
    target: &mut Vec<u8>,
    terminal_error: Option<&PublishTerminalError>,
) -> Result<(), PublishRecordError> {
    match terminal_error {
        None => target.push(0),
        Some(error) => {
            target.push(1);
            target.push(error.kind.into());
            push_string(target, "terminal_error.message", &error.message)?;
            push_optional_fixed(target, &error.evidence_digest);
        }
    }
    Ok(())
}

fn decode_optional_terminal_error(
    decoder: &mut Decoder<'_>,
) -> Result<Option<PublishTerminalError>, PublishRecordError> {
    match decoder.u8("terminal_error tag")? {
        0 => Ok(None),
        1 => Ok(Some(PublishTerminalError {
            kind: PublishTerminalErrorKind::try_from(decoder.u8("terminal_error.kind")?)?,
            message: decoder.string("terminal_error.message", MAX_TERMINAL_ERROR_BYTES)?,
            evidence_digest: decoder.optional_fixed("terminal_error.evidence_digest")?,
        })),
        value => Err(PublishRecordError::InvalidOptionalTag {
            field: "terminal_error",
            value,
        }),
    }
}

fn decode_publish_phase(value: u8) -> Result<PublishPhase, PublishRecordError> {
    PublishPhase::try_from(value).map_err(|error| PublishRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

fn decode_provider_state(value: u8) -> Result<StagedProviderState, PublishRecordError> {
    StagedProviderState::try_from(value).map_err(|error| PublishRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

fn decode_cleanup_state(value: u8) -> Result<StagedCleanupState, PublishRecordError> {
    StagedCleanupState::try_from(value).map_err(|error| PublishRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

fn push_string(
    target: &mut Vec<u8>,
    field: &'static str,
    value: &str,
) -> Result<(), PublishRecordError> {
    push_bytes(target, field, value.as_bytes())
}

fn push_bytes(
    target: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
) -> Result<(), PublishRecordError> {
    let length = u32::try_from(value.len()).map_err(|_| PublishRecordError::FieldTooLong {
        field,
        length: value.len(),
        max: u32::MAX as usize,
    })?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value);
    Ok(())
}

fn push_optional_bytes(
    target: &mut Vec<u8>,
    field: &'static str,
    value: Option<&[u8]>,
) -> Result<(), PublishRecordError> {
    match value {
        None => target.push(0),
        Some(value) => {
            target.push(1);
            push_bytes(target, field, value)?;
        }
    }
    Ok(())
}

fn push_optional_fixed<const N: usize>(target: &mut Vec<u8>, value: &Option<[u8; N]>) {
    match value {
        None => target.push(0),
        Some(value) => {
            target.push(1);
            target.extend_from_slice(value);
        }
    }
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn require_value_version(&mut self) -> Result<(), PublishRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == PUBLISH_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(PublishRecordError::UnsupportedValueVersion {
                actual,
                expected: PUBLISH_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, PublishRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, PublishRecordError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, PublishRecordError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn generation(&mut self, field: &'static str) -> Result<Generation, PublishRecordError> {
        Generation::new(self.u64(field)?).map_err(|_| PublishRecordError::ZeroScalar { field })
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], PublishRecordError> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(field, N)?);
        Ok(value)
    }

    fn bytes(&mut self, field: &'static str, max: usize) -> Result<Vec<u8>, PublishRecordError> {
        let length = self.u32(field)? as usize;
        if length > max {
            return Err(PublishRecordError::FieldTooLong { field, length, max });
        }
        Ok(self.take(field, length)?.to_vec())
    }

    fn string(&mut self, field: &'static str, max: usize) -> Result<String, PublishRecordError> {
        String::from_utf8(self.bytes(field, max)?)
            .map_err(|_| PublishRecordError::InvalidUtf8 { field })
    }

    fn optional_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Option<Vec<u8>>, PublishRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.bytes(field, max).map(Some),
            value => Err(PublishRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<Option<[u8; N]>, PublishRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.fixed(field).map(Some),
            value => Err(PublishRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], PublishRecordError> {
        let remaining = self.encoded.len().saturating_sub(self.offset);
        let Some(end) = self.offset.checked_add(length) else {
            return Err(PublishRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        };
        if end > self.encoded.len() {
            return Err(PublishRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let value = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), PublishRecordError> {
        let count = self.encoded.len() - self.offset;
        if count == 0 {
            Ok(())
        } else {
            Err(PublishRecordError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation(value: u64) -> Generation {
        Generation::new(value).unwrap()
    }

    fn revision(fill: u8) -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes([fill; 16])
    }

    fn object_key(revision_fill: u8) -> String {
        format!(
            "nokv/artifacts/{}/{}/{}/blocks/{}",
            "11".repeat(16),
            "33".repeat(16),
            format!("{revision_fill:02x}").repeat(16),
            "0000000000000002"
        )
    }

    fn terminal_error() -> PublishTerminalError {
        PublishTerminalError {
            kind: PublishTerminalErrorKind::UploadFailed,
            message: "provider rejected upload".to_owned(),
            evidence_digest: None,
        }
    }

    fn result() -> PublishResult {
        PublishResult {
            path_generation: generation(8),
            workspace_revision: WorkspaceRevision::new(9),
            logical_size: 8_192,
            body_digest_uri: "sha256:abc".to_owned(),
        }
    }

    fn uploading_operation() -> PublishOperationRecord {
        PublishOperationRecord {
            operation_id: OperationId::from_bytes([0x10; 16]),
            identity_digest: [0x11; SHA256_BYTES],
            initialization_digest: [0x12; SHA256_BYTES],
            initiating_owner_epoch: OwnerEpoch::new(5).unwrap(),
            activity_deadline_ms: 50_000,
            authority: PublishAuthority::Visible,
            workbench_id: WorkbenchId::new("agent-run").unwrap(),
            workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([0x13; 16]),
            path: NormalizedRelativePath::new("outputs/result.bin").unwrap(),
            artifact_revision_id: revision(0x14),
            claim: PublishClaim::Append {
                expected_generation: generation(7),
                base_revision_id: revision(0x15),
                append_offset: 4_096,
            },
            phase: PublishPhase::Uploading,
            staged_object_count: 1,
            staged_object_seal: [0x16; SHA256_BYTES],
            staged_object_cursor: 0,
            staged_object_rolling_digest: [0; SHA256_BYTES],
            uploaded_object_cursor: 0,
            uploaded_object_rolling_digest: [0; SHA256_BYTES],
            manifest_row_count: 2,
            manifest_seal: [0x17; SHA256_BYTES],
            manifest_cursor: 0,
            manifest_rolling_digest: [0; SHA256_BYTES],
            manifest_last_position: None,
            dependency_count: 1,
            dependency_depth: 1,
            dependency_digest: [0x18; SHA256_BYTES],
            cleanup_staged_object_cursor: 0,
            cleanup_manifest_cursor: 0,
            publication_absence_proof: None,
            result: None,
            terminal_error: None,
        }
    }

    fn complete_publication_closure(record: &mut PublishOperationRecord) {
        record.staged_object_cursor = record.staged_object_count;
        record.staged_object_rolling_digest = record.staged_object_seal;
        record.uploaded_object_cursor = record.staged_object_count;
        record.uploaded_object_rolling_digest = record.staged_object_seal;
        record.manifest_cursor = record.manifest_row_count;
        record.manifest_rolling_digest = record.manifest_seal;
        record.manifest_last_position =
            (record.manifest_row_count > 0).then_some(ManifestPosition { object_index: 11 });
    }

    fn assert_every_proper_prefix_is_truncated<T>(
        encoded: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, PublishRecordError>,
    ) {
        for length in 0..encoded.len() {
            assert!(
                matches!(
                    decode(&encoded[..length]),
                    Err(PublishRecordError::Truncated { .. })
                ),
                "prefix length {length} was not rejected as truncated: {:?}",
                decode(&encoded[..length]).err()
            );
        }
    }

    fn assert_trailing_byte_is_rejected<T: fmt::Debug>(
        mut encoded: Vec<u8>,
        decode: impl Fn(&[u8]) -> Result<T, PublishRecordError>,
    ) {
        encoded.push(0);
        assert_eq!(
            decode(&encoded).unwrap_err(),
            PublishRecordError::TrailingBytes { count: 1 }
        );
    }

    #[test]
    fn publish_operation_codec_has_frozen_golden_bytes() {
        let mut record = uploading_operation();
        complete_publication_closure(&mut record);
        record.phase = PublishPhase::Published;
        record.result = Some(result());

        let expected = [
            &[PUBLISH_VALUE_FORMAT_VERSION][..],
            &[0x10; 16],
            &[0x11; SHA256_BYTES],
            &[0x12; SHA256_BYTES],
            &5_u64.to_be_bytes(),
            &50_000_u64.to_be_bytes(),
            &[1],
            &9_u32.to_be_bytes(),
            b"agent-run",
            &[0x13; 16],
            &18_u32.to_be_bytes(),
            b"outputs/result.bin",
            &[0x14; 16],
            &[3],
            &7_u64.to_be_bytes(),
            &[0x15; 16],
            &4_096_u64.to_be_bytes(),
            &[3],
            &1_u32.to_be_bytes(),
            &[0x16; SHA256_BYTES],
            &1_u32.to_be_bytes(),
            &[0x16; SHA256_BYTES],
            &1_u32.to_be_bytes(),
            &[0x16; SHA256_BYTES],
            &2_u32.to_be_bytes(),
            &[0x17; SHA256_BYTES],
            &2_u32.to_be_bytes(),
            &[0x17; SHA256_BYTES],
            &[1],
            &11_u64.to_be_bytes(),
            &[1, 1],
            &[0x18; SHA256_BYTES],
            &0_u32.to_be_bytes(),
            &0_u32.to_be_bytes(),
            &[0],
            &[1],
            &8_u64.to_be_bytes(),
            &9_u64.to_be_bytes(),
            &8_192_u64.to_be_bytes(),
            &10_u32.to_be_bytes(),
            b"sha256:abc",
            &[0],
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(PublishOperationRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, PublishOperationRecord::decode);
        assert_trailing_byte_is_rejected(expected, PublishOperationRecord::decode);
    }

    #[test]
    fn staged_object_codec_has_frozen_golden_bytes() {
        let record = StagedObjectRecord {
            artifact_revision_id: revision(0x22),
            object_sequence: 7,
            object_key: object_key(0x22),
            multipart_upload_id: Some(b"upload-1".to_vec()),
            expected_length: 4_096,
            expected_digest_uri: "sha256:block".to_owned(),
            provider_state: StagedProviderState::Uploaded,
            cleanup_state: StagedCleanupState::Owned,
        };
        let key = object_key(0x22);
        let expected = [
            &[PUBLISH_VALUE_FORMAT_VERSION][..],
            &[0x22; 16],
            &7_u32.to_be_bytes(),
            &(key.len() as u32).to_be_bytes(),
            key.as_bytes(),
            &[1],
            &8_u32.to_be_bytes(),
            b"upload-1",
            &4_096_u64.to_be_bytes(),
            &12_u32.to_be_bytes(),
            b"sha256:block",
            &[3, 1],
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(StagedObjectRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, StagedObjectRecord::decode);
        assert_trailing_byte_is_rejected(expected, StagedObjectRecord::decode);
    }

    #[test]
    fn artifact_manifest_codec_has_frozen_golden_bytes() {
        let record = ArtifactManifestRow {
            physical_owner_revision_id: revision(0x44),
            physical_object_index: 2,
            object_key: object_key(0x44),
            logical_offset: 3,
            offset: 5,
            length: 9,
            digest_uri: "xxh3-64:beef".to_owned(),
            append_segment: Some(AppendSegment {
                segment_sequence: 7,
                segment_offset: 11,
            }),
        };
        let key = object_key(0x44);
        let expected = [
            &[PUBLISH_VALUE_FORMAT_VERSION][..],
            &[0x44; 16],
            &2_u64.to_be_bytes(),
            &(key.len() as u32).to_be_bytes(),
            key.as_bytes(),
            &3_u64.to_be_bytes(),
            &5_u64.to_be_bytes(),
            &9_u64.to_be_bytes(),
            &12_u32.to_be_bytes(),
            b"xxh3-64:beef",
            &[1],
            &7_u32.to_be_bytes(),
            &11_u64.to_be_bytes(),
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(ArtifactManifestRow::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, ArtifactManifestRow::decode);
        assert_trailing_byte_is_rejected(expected, ArtifactManifestRow::decode);

        let mut previous_version = record.encode().unwrap();
        previous_version[0] = PUBLISH_VALUE_FORMAT_VERSION - 1;
        assert_eq!(
            ArtifactManifestRow::decode(&previous_version).unwrap_err(),
            PublishRecordError::UnsupportedValueVersion {
                actual: PUBLISH_VALUE_FORMAT_VERSION - 1,
                expected: PUBLISH_VALUE_FORMAT_VERSION,
            }
        );
    }

    #[test]
    fn successful_publication_transition_chain_is_exact() {
        let mut record = uploading_operation();
        complete_publication_closure(&mut record);
        record
            .apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginFinalization,
            )
            .unwrap();
        record
            .apply_transition(
                PublishPhase::Finalizing,
                PublishTransition::Publish { result: result() },
            )
            .unwrap();

        assert_eq!(record.phase, PublishPhase::Published);
        assert_eq!(record.result, Some(result()));
        assert!(record.terminal_error.is_none());
        assert!(record.publication_absence_proof.is_none());
    }

    #[test]
    fn abort_cleanup_transition_chain_is_exact() {
        let mut record = uploading_operation();
        record.staged_object_cursor = 1;
        record.staged_object_rolling_digest = record.staged_object_seal;
        record.manifest_cursor = 1;
        record.manifest_rolling_digest = [0x19; SHA256_BYTES];
        record.manifest_last_position = Some(ManifestPosition { object_index: 0 });
        record
            .apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginAbort {
                    terminal_error: terminal_error(),
                },
            )
            .unwrap();
        record
            .apply_transition(PublishPhase::Aborting, PublishTransition::BeginCleaning)
            .unwrap();
        record.cleanup_staged_object_cursor = 1;
        record.cleanup_manifest_cursor = 1;
        record
            .apply_transition(PublishPhase::Cleaning, PublishTransition::FinishCleanup)
            .unwrap();

        assert_eq!(record.phase, PublishPhase::Cleaned);
        assert_eq!(record.cleanup_staged_object_cursor, 1);
        assert_eq!(record.cleanup_manifest_cursor, 1);
        assert_eq!(record.terminal_error, Some(terminal_error()));
    }

    #[test]
    fn metadata_owned_finalizing_takeover_retains_absence_proof() {
        let mut record = uploading_operation();
        complete_publication_closure(&mut record);
        record
            .apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginFinalization,
            )
            .unwrap();
        record
            .take_over_finalization([0xaa; SHA256_BYTES], terminal_error())
            .unwrap();

        assert_eq!(record.phase, PublishPhase::Aborting);
        assert_eq!(record.publication_absence_proof, Some([0xaa; SHA256_BYTES]));
    }

    #[test]
    fn finalizing_and_aborting_are_single_winner_from_uploading() {
        let mut finalize_winner = uploading_operation();
        complete_publication_closure(&mut finalize_winner);
        finalize_winner
            .apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginFinalization,
            )
            .unwrap();
        assert_eq!(
            finalize_winner
                .apply_transition(
                    PublishPhase::Uploading,
                    PublishTransition::BeginAbort {
                        terminal_error: terminal_error(),
                    },
                )
                .unwrap_err(),
            PublishRecordError::PhaseMismatch {
                expected: PublishPhase::Uploading,
                actual: PublishPhase::Finalizing,
            }
        );

        let mut abort_winner = uploading_operation();
        abort_winner
            .apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginAbort {
                    terminal_error: terminal_error(),
                },
            )
            .unwrap();
        assert_eq!(
            abort_winner
                .apply_transition(
                    PublishPhase::Uploading,
                    PublishTransition::BeginFinalization,
                )
                .unwrap_err(),
            PublishRecordError::PhaseMismatch {
                expected: PublishPhase::Uploading,
                actual: PublishPhase::Aborting,
            }
        );
    }

    #[test]
    fn illegal_phase_edges_and_incomplete_cursors_are_rejected_without_mutation() {
        let mut record = uploading_operation();
        let original = record.clone();
        assert!(matches!(
            record.apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginFinalization,
            ),
            Err(PublishRecordError::InvalidPhasePayload { .. })
        ));
        assert_eq!(record, original);

        assert_eq!(
            record
                .apply_transition(PublishPhase::Uploading, PublishTransition::FinishCleanup,)
                .unwrap_err(),
            PublishRecordError::InvalidPhaseTransition {
                from: PublishPhase::Uploading,
                to: PublishPhase::Cleaned,
            }
        );
        assert_eq!(record, original);
    }

    #[test]
    fn quarantine_requires_reconciliation_evidence() {
        let mut record = uploading_operation();
        record
            .apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginAbort {
                    terminal_error: terminal_error(),
                },
            )
            .unwrap();
        record
            .apply_transition(PublishPhase::Aborting, PublishTransition::BeginCleaning)
            .unwrap();
        let before = record.clone();
        assert!(matches!(
            record.apply_transition(
                PublishPhase::Cleaning,
                PublishTransition::Quarantine {
                    terminal_error: terminal_error(),
                },
            ),
            Err(PublishRecordError::InvalidPhasePayload {
                phase: PublishPhase::Quarantined,
                ..
            })
        ));
        assert_eq!(record, before);

        let quarantined = PublishTerminalError {
            kind: PublishTerminalErrorKind::ProviderAmbiguous,
            message: "provider outcome is ambiguous".to_owned(),
            evidence_digest: Some([0xee; SHA256_BYTES]),
        };
        record
            .apply_transition(
                PublishPhase::Cleaning,
                PublishTransition::Quarantine {
                    terminal_error: quarantined.clone(),
                },
            )
            .unwrap();
        assert_eq!(record.phase, PublishPhase::Quarantined);
        assert_eq!(record.terminal_error, Some(quarantined));
    }

    #[test]
    fn count_cursor_and_dependency_bounds_fail_closed() {
        let mut operation = uploading_operation();
        operation.staged_object_count = MAX_STAGED_OBJECTS + 1;
        assert!(matches!(
            operation.encode(),
            Err(PublishRecordError::CountOutOfRange {
                field: "staged_object_count",
                ..
            })
        ));

        let mut operation = uploading_operation();
        operation.staged_object_cursor = 2;
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::CursorOutOfRange {
                field: "staged_object_cursor",
                cursor: 2,
                count: 1,
            }
        );

        let mut operation = uploading_operation();
        operation.dependency_count = MAX_DEPENDENCY_COUNT + 1;
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::InvalidDependencyShape {
                count: MAX_DEPENDENCY_COUNT + 1,
                depth: 1,
            }
        );

        let mut operation = uploading_operation();
        operation.dependency_count = 0;
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::InvalidDependencyShape { count: 0, depth: 1 }
        );
    }

    #[test]
    fn progress_digests_and_manifest_position_fail_closed() {
        let mut operation = uploading_operation();
        operation.staged_object_rolling_digest = [0x99; SHA256_BYTES];
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::InvalidProgress {
                field: "staged_object",
                reason: "cursor zero requires the zero rolling digest",
            }
        );

        let mut operation = uploading_operation();
        operation.staged_object_cursor = 1;
        operation.staged_object_rolling_digest = [0x99; SHA256_BYTES];
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::InvalidProgress {
                field: "staged_object",
                reason: "complete cursor must match the final seal",
            }
        );

        let mut operation = uploading_operation();
        operation.manifest_cursor = 1;
        operation.manifest_rolling_digest = [0x99; SHA256_BYTES];
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::InvalidProgress {
                field: "manifest_last_position",
                reason: "must identify the final row at a non-zero cursor",
            }
        );

        let mut operation = uploading_operation();
        operation.manifest_last_position = Some(ManifestPosition { object_index: 0 });
        assert_eq!(
            operation.encode().unwrap_err(),
            PublishRecordError::InvalidProgress {
                field: "manifest_last_position",
                reason: "must be absent at cursor zero",
            }
        );
    }

    #[test]
    fn staged_object_and_manifest_invariants_fail_closed() {
        let mut staged = StagedObjectRecord {
            artifact_revision_id: revision(0x22),
            object_sequence: 0,
            object_key: object_key(0x22),
            multipart_upload_id: None,
            expected_length: 1,
            expected_digest_uri: "sha256:a".to_owned(),
            provider_state: StagedProviderState::Uploaded,
            cleanup_state: StagedCleanupState::Owned,
        };
        staged.object_key = object_key(0x23);
        assert_eq!(
            staged.encode().unwrap_err(),
            PublishRecordError::ObjectKeyRevisionMismatch
        );

        staged.object_key = format!(
            "objects/{}/{}/{}/{}/{}",
            "11".repeat(16),
            "33".repeat(16),
            "22".repeat(16),
            "0000000000000000",
            "0000000000000002"
        );
        assert!(matches!(
            staged.encode(),
            Err(PublishRecordError::InvalidObjectKey { .. })
        ));

        staged.object_key = object_key(0x22);
        staged.cleanup_state = StagedCleanupState::Deleted;
        assert!(matches!(
            staged.encode(),
            Err(PublishRecordError::InvalidStagedStateCombination { .. })
        ));

        let manifest = ArtifactManifestRow {
            physical_owner_revision_id: revision(0x44),
            physical_object_index: 2,
            object_key: object_key(0x44),
            logical_offset: 0,
            offset: u64::MAX,
            length: 1,
            digest_uri: "sha256:a".to_owned(),
            append_segment: None,
        };
        assert_eq!(
            manifest.encode().unwrap_err(),
            PublishRecordError::RangeOverflow {
                field: "object range"
            }
        );

        let mut mismatched_index = manifest;
        mismatched_index.offset = 0;
        mismatched_index.physical_object_index = 3;
        assert_eq!(
            mismatched_index.encode().unwrap_err(),
            PublishRecordError::ObjectKeyIndexMismatch {
                expected: 3,
                actual: 2,
            }
        );
    }

    #[test]
    fn every_codec_rejects_unknown_versions_and_enums() {
        let mut operation = uploading_operation().encode().unwrap();
        operation[0] = PUBLISH_VALUE_FORMAT_VERSION + 1;
        assert_eq!(
            PublishOperationRecord::decode(&operation).unwrap_err(),
            PublishRecordError::UnsupportedValueVersion {
                actual: PUBLISH_VALUE_FORMAT_VERSION + 1,
                expected: PUBLISH_VALUE_FORMAT_VERSION,
            }
        );

        let mut operation = uploading_operation().encode().unwrap();
        let phase_offset = 1
            + 16
            + SHA256_BYTES
            + SHA256_BYTES
            + 8
            + 8
            + 1
            + 4
            + "agent-run".len()
            + 16
            + 4
            + 18
            + 16
            + 1
            + 8
            + 16
            + 8;
        operation[phase_offset] = 0xff;
        assert_eq!(
            PublishOperationRecord::decode(&operation).unwrap_err(),
            PublishRecordError::UnknownDiscriminant {
                type_name: "PublishPhase",
                value: 0xff,
            }
        );

        let staged = StagedObjectRecord {
            artifact_revision_id: revision(0x22),
            object_sequence: 0,
            object_key: object_key(0x22),
            multipart_upload_id: None,
            expected_length: 1,
            expected_digest_uri: "sha256:a".to_owned(),
            provider_state: StagedProviderState::Uploaded,
            cleanup_state: StagedCleanupState::Owned,
        };
        let mut encoded = staged.encode().unwrap();
        let provider_offset = encoded.len() - 2;
        encoded[provider_offset] = 0xff;
        assert_eq!(
            StagedObjectRecord::decode(&encoded).unwrap_err(),
            PublishRecordError::UnknownDiscriminant {
                type_name: "StagedProviderState",
                value: 0xff,
            }
        );

        let mut encoded = staged.encode().unwrap();
        let cleanup_offset = encoded.len() - 1;
        encoded[cleanup_offset] = 0xff;
        assert_eq!(
            StagedObjectRecord::decode(&encoded).unwrap_err(),
            PublishRecordError::UnknownDiscriminant {
                type_name: "StagedCleanupState",
                value: 0xff,
            }
        );
    }

    #[test]
    fn unknown_claim_error_and_optional_discriminants_fail_closed() {
        let mut create = uploading_operation();
        create.claim = PublishClaim::CreateOnly;
        let mut encoded = create.encode().unwrap();
        let claim_offset = 1
            + 16
            + SHA256_BYTES
            + SHA256_BYTES
            + 8
            + 8
            + 1
            + 4
            + "agent-run".len()
            + 16
            + 4
            + 18
            + 16;
        encoded[claim_offset] = 0xff;
        assert_eq!(
            PublishOperationRecord::decode(&encoded).unwrap_err(),
            PublishRecordError::UnknownDiscriminant {
                type_name: "PublishClaim",
                value: 0xff,
            }
        );

        let mut aborting = uploading_operation();
        aborting.phase = PublishPhase::Aborting;
        aborting.terminal_error = Some(terminal_error());
        let mut encoded = aborting.encode().unwrap();
        let terminal_kind_offset = encoded.len() - (1 + 4 + terminal_error().message.len() + 1);
        encoded[terminal_kind_offset] = 0xff;
        assert_eq!(
            PublishOperationRecord::decode(&encoded).unwrap_err(),
            PublishRecordError::UnknownDiscriminant {
                type_name: "PublishTerminalErrorKind",
                value: 0xff,
            }
        );

        let manifest = ArtifactManifestRow {
            physical_owner_revision_id: revision(0x44),
            physical_object_index: 2,
            object_key: object_key(0x44),
            logical_offset: 0,
            offset: 0,
            length: 1,
            digest_uri: "sha256:a".to_owned(),
            append_segment: None,
        };
        let mut encoded = manifest.encode().unwrap();
        let tag_offset = encoded.len() - 1;
        encoded[tag_offset] = 2;
        assert_eq!(
            ArtifactManifestRow::decode(&encoded).unwrap_err(),
            PublishRecordError::InvalidOptionalTag {
                field: "append_segment",
                value: 2,
            }
        );
    }

    #[test]
    fn oversized_fields_and_records_fail_before_allocation_or_acceptance() {
        let staged = StagedObjectRecord {
            artifact_revision_id: revision(0x22),
            object_sequence: 0,
            object_key: object_key(0x22),
            multipart_upload_id: Some(vec![0; MAX_MULTIPART_ID_BYTES + 1]),
            expected_length: 1,
            expected_digest_uri: "sha256:a".to_owned(),
            provider_state: StagedProviderState::Uploading,
            cleanup_state: StagedCleanupState::Owned,
        };
        assert!(matches!(
            staged.encode(),
            Err(PublishRecordError::FieldTooLong {
                field: "multipart_upload_id",
                ..
            })
        ));

        let oversized = vec![0; MAX_PUBLISH_OPERATION_RECORD_BYTES + 1];
        assert_eq!(
            PublishOperationRecord::decode(&oversized).unwrap_err(),
            PublishRecordError::RecordTooLarge {
                record: "PublishOperationRecord",
                length: MAX_PUBLISH_OPERATION_RECORD_BYTES + 1,
                max: MAX_PUBLISH_OPERATION_RECORD_BYTES,
            }
        );
    }
}
