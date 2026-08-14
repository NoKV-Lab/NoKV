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

/// Only supported value format for restore-owned payloads.
pub const RESTORE_VALUE_FORMAT_VERSION: u8 = 4;

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
    pub restore_manifest: RestoreManifestDescriptor,
    pub phase: RestorePhase,
    /// Last source path copied into the contiguous member closure.
    pub source_cursor: Option<NormalizedRelativePath>,
    pub source_eof: bool,
    pub next_member_sequence: u64,
    pub member_rolling_digest: [u8; SHA256_BYTES],
    /// Present after the source closure reaches EOF.
    pub member_seal: Option<[u8; SHA256_BYTES]>,
    /// Number of contiguous members removed by abort cleanup.
    pub cleanup_member_cursor: u64,
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
        member_seal: [u8; SHA256_BYTES],
    },
    MarkReady {
        initialization_digest: [u8; SHA256_BYTES],
    },
    Complete {
        result: RestoreResult,
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
        cursor: u64,
        member_count: u64,
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
                cursor,
                member_count,
            } => write!(
                formatter,
                "restore cleanup cursor {cursor} exceeds member count {member_count}"
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
        if self.next_member_sequence > MAX_RESTORE_MEMBERS {
            return Err(RestoreRecordError::CountOutOfRange {
                field: "next_member_sequence",
                value: self.next_member_sequence,
                max: MAX_RESTORE_MEMBERS,
            });
        }
        if self.cleanup_member_cursor > self.next_member_sequence {
            return Err(RestoreRecordError::CursorOutOfRange {
                cursor: self.cleanup_member_cursor,
                member_count: self.next_member_sequence,
            });
        }
        self.restore_manifest.validate()?;
        if self.source_workbench_id == self.destination_workbench_id {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source and destination workbenches must differ",
            });
        }
        if self.source_cursor.is_some() != (self.next_member_sequence > 0) {
            return Err(RestoreRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "source cursor presence must match non-empty member progress",
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
        validate_restore_phase(self)
    }

    pub fn encode(&self) -> Result<Vec<u8>, RestoreRecordError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.push(RESTORE_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.operation_id.as_bytes());
        encoded.extend_from_slice(&self.identity_digest);
        put_optional_fixed(&mut encoded, self.initialization_digest.as_ref());
        put_bytes(&mut encoded, self.source_workbench_id.as_bytes());
        encoded.extend_from_slice(self.source_workspace_incarnation_id.as_bytes());
        put_source(&mut encoded, self.source);
        put_bytes(&mut encoded, self.destination_workbench_id.as_bytes());
        encoded.extend_from_slice(self.destination_workspace_incarnation_id.as_bytes());
        put_bytes(
            &mut encoded,
            self.restore_manifest.body_digest_uri.as_bytes(),
        );
        encoded.extend_from_slice(&self.restore_manifest.logical_size.to_be_bytes());
        put_bytes(&mut encoded, self.restore_manifest.content_type.as_bytes());
        encoded.push(self.phase.into());
        put_optional_path(&mut encoded, self.source_cursor.as_ref());
        encoded.push(u8::from(self.source_eof));
        encoded.extend_from_slice(&self.next_member_sequence.to_be_bytes());
        encoded.extend_from_slice(&self.member_rolling_digest);
        put_optional_fixed(&mut encoded, self.member_seal.as_ref());
        encoded.extend_from_slice(&self.cleanup_member_cursor.to_be_bytes());
        put_optional_result(&mut encoded, self.result.as_ref());
        put_optional_terminal_error(&mut encoded, self.terminal_error.as_ref())?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RestoreRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
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
        let restore_manifest = RestoreManifestDescriptor {
            body_digest_uri: decoder.string("restore_manifest.body_digest_uri")?,
            logical_size: decoder.u64("restore_manifest.logical_size")?,
            content_type: decoder.string("restore_manifest.content_type")?,
        };
        let phase = decode_durable_enum(decoder.u8("phase")?)?;
        let source_cursor = decoder.optional_path("source_cursor")?;
        let source_eof = decoder.boolean("source_eof")?;
        let next_member_sequence = decoder.u64("next_member_sequence")?;
        let member_rolling_digest = decoder.fixed("member_rolling_digest")?;
        let member_seal = decoder.optional_fixed("member_seal")?;
        let cleanup_member_cursor = decoder.u64("cleanup_member_cursor")?;
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
            restore_manifest,
            phase,
            source_cursor,
            source_eof,
            next_member_sequence,
            member_rolling_digest,
            member_seal,
            cleanup_member_cursor,
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
            RestoreTransition::SealSource { member_seal }
                if expected == RestorePhase::Copying && self.source_eof =>
            {
                if member_seal != self.member_rolling_digest {
                    return Err(RestoreRecordError::InvalidPhasePayload {
                        phase: expected,
                        reason: "source seal does not match the rolling member digest",
                    });
                }
                next.phase = RestorePhase::SourceSealed;
                next.member_seal = Some(member_seal);
            }
            RestoreTransition::MarkReady {
                initialization_digest,
            } if expected == RestorePhase::SourceSealed && self.initialization_digest.is_none() => {
                next.phase = RestorePhase::Ready;
                next.initialization_digest = Some(initialization_digest);
            }
            RestoreTransition::Complete { result } if expected == RestorePhase::Ready => {
                next.phase = RestorePhase::Complete;
                next.result = Some(result);
            }
            RestoreTransition::BeginAbort { terminal_error }
                if matches!(
                    expected,
                    RestorePhase::Preparing
                        | RestorePhase::Copying
                        | RestorePhase::SourceSealed
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
        encoded.push(RESTORE_VALUE_FORMAT_VERSION);
        put_bytes(&mut encoded, path);
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        encoded.extend_from_slice(&self.path_generation.get().to_be_bytes());
        encoded.extend_from_slice(&self.row_digest);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RestoreRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
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

fn validate_restore_phase(record: &RestoreOperationRecord) -> Result<(), RestoreRecordError> {
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
                || record.next_member_sequence != 0
                || record.member_seal.is_some()
                || record.cleanup_member_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("preparing restore cannot have closure or terminal progress");
            }
        }
        RestorePhase::Copying => {
            if record.initialization_digest.is_some()
                || record.member_seal.is_some()
                || record.cleanup_member_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("copying restore cannot have a seal, cleanup, or terminal payload");
            }
        }
        RestorePhase::SourceSealed => {
            if record.initialization_digest.is_some()
                || !record.source_eof
                || record.member_seal != Some(record.member_rolling_digest)
                || record.cleanup_member_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail("sealed source requires the exact closure and no terminal payload");
            }
        }
        RestorePhase::Ready => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.member_seal != Some(record.member_rolling_digest)
                || record.cleanup_member_cursor != 0
                || record.result.is_some()
                || record.terminal_error.is_some()
            {
                return fail(
                    "ready restore requires a bound initialization and exact source closure",
                );
            }
        }
        RestorePhase::Complete => {
            if record.initialization_digest.is_none()
                || !record.source_eof
                || record.member_seal != Some(record.member_rolling_digest)
                || record.cleanup_member_cursor != 0
                || record.result.is_none()
                || record.terminal_error.is_some()
            {
                return fail("complete restore requires one matching result and no cleanup");
            }
        }
        RestorePhase::Aborting => {
            if record.cleanup_member_cursor != 0
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

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn require_value_version(&mut self) -> Result<(), RestoreRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == RESTORE_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(RestoreRecordError::UnsupportedValueVersion {
                actual,
                expected: RESTORE_VALUE_FORMAT_VERSION,
            })
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
    use super::*;

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
            restore_manifest: RestoreManifestDescriptor {
                body_digest_uri: format!("sha256:{}", "ab".repeat(32)),
                logical_size: 128,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            phase,
            source_cursor: None,
            source_eof: false,
            next_member_sequence: 0,
            member_rolling_digest: [0; SHA256_BYTES],
            member_seal: None,
            cleanup_member_cursor: 0,
            result: None,
            terminal_error: None,
        }
    }

    fn terminal_error() -> RestoreTerminalError {
        RestoreTerminalError {
            kind: RestoreTerminalErrorKind::AbortedByCaller,
            message: "cancelled".to_owned(),
            evidence_digest: Some([9; SHA256_BYTES]),
        }
    }

    #[test]
    fn preparing_operation_round_trip_and_strict_envelope() {
        let record = operation(RestorePhase::Preparing);
        let encoded = record.encode().unwrap();
        assert_eq!(RestoreOperationRecord::decode(&encoded).unwrap(), record);
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            RestoreOperationRecord::decode(&trailing),
            Err(RestoreRecordError::TrailingBytes { count: 1 })
        );
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
        let mut record = operation(RestorePhase::Preparing)
            .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)
            .unwrap();
        record.source_cursor = Some(NormalizedRelativePath::new("outputs/result").unwrap());
        record.source_eof = true;
        record.next_member_sequence = 1;
        record.member_rolling_digest = [8; SHA256_BYTES];
        record = record
            .apply(
                RestorePhase::Copying,
                RestoreTransition::SealSource {
                    member_seal: [8; SHA256_BYTES],
                },
            )
            .unwrap()
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::MarkReady {
                    initialization_digest: [3; SHA256_BYTES],
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
            .apply(RestorePhase::Ready, RestoreTransition::Complete { result })
            .unwrap();
        assert_eq!(record.phase, RestorePhase::Complete);
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
        record.next_member_sequence = 1;
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
