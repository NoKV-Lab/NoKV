/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable object-deletion progress for one epoch-fenced revision GC claim.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommitVersion, GcPhase, GenericIndexGenerationId, OperationId, ReadVersion,
    ReferenceEpoch, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::publish_operation_records::ManifestPosition;

/// Only supported value format for GC-operation payloads.
pub const GC_VALUE_FORMAT_VERSION: u8 = 1;

/// Maximum provider reconciliation evidence retained by a GC operation.
pub const MAX_GC_EVIDENCE_BYTES: usize = 16 * 1024;

/// Monotonic root-scoped barrier used to advance the metadata commit clock for
/// a quiescent GC candidate. The surrounding CurrentValue owns its exact
/// commit version; this payload prevents the mutation from being a server-side
/// synthetic no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcHistoryBarrierRecord {
    pub generation: u64,
}

/// Recoverable provider-deletion cursor for one exact revision epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcOperationRecord {
    pub operation_id: OperationId,
    pub identity_digest: [u8; SHA256_BYTES],
    pub artifact_revision_id: ArtifactRevisionId,
    pub reference_epoch: ReferenceEpoch,
    pub last_zero_ref_version: CommitVersion,
    pub safe_history_floor: ReadVersion,
    pub expected_manifest_row_count: u64,
    pub expected_manifest_digest: [u8; SHA256_BYTES],
    pub expected_dependency_count: u32,
    pub expected_dependency_digest: [u8; SHA256_BYTES],
    pub phase: GcPhase,
    /// Last contiguous manifest position inspected from authoritative metadata.
    pub manifest_cursor: Option<ManifestPosition>,
    /// Number of contiguous manifest rows inspected so far.
    pub scanned_manifest_row_count: u64,
    /// Publication-compatible rolling digest of every inspected manifest row.
    pub manifest_rolling_digest: [u8; SHA256_BYTES],
    /// Number of target-owned objects confirmed absent at the provider.
    pub deleted_object_count: u64,
    /// Rolling digest of target-owned object absence confirmations.
    pub object_rolling_digest: [u8; SHA256_BYTES],
    /// Canonical proof over the complete confirmed-absent manifest closure.
    pub object_absence_digest: Option<[u8; SHA256_BYTES]>,
    pub retry_count: u32,
    pub quarantine_evidence: Option<Vec<u8>>,
}

/// Recoverable metadata-payload collection for one exact Generic index
/// generation epoch. The immutable header remains outside this record as the
/// permanent generation identity tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexGcOperationRecord {
    pub operation_id: OperationId,
    pub identity_digest: [u8; SHA256_BYTES],
    pub generation_id: GenericIndexGenerationId,
    pub reference_epoch: ReferenceEpoch,
    pub last_zero_reference_version: CommitVersion,
    pub safe_history_floor: ReadVersion,
    pub expected_capability_digest: [u8; SHA256_BYTES],
    pub expected_row_count: u64,
    pub expected_row_digest: [u8; SHA256_BYTES],
    pub phase: GenericIndexGcPhase,
    /// Last contiguous generation-row sequence validated and removed.
    pub row_cursor: Option<u64>,
    pub scanned_row_count: u64,
    pub row_rolling_digest: [u8; SHA256_BYTES],
    pub rows_complete: bool,
    /// Last append-receipt first sequence validated and removed.
    pub receipt_cursor: Option<u64>,
    pub deleted_receipt_count: u64,
    pub receipts_complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenericIndexGcPhase {
    Retiring,
    Retired,
}

impl From<GenericIndexGcPhase> for u8 {
    fn from(value: GenericIndexGcPhase) -> Self {
        match value {
            GenericIndexGcPhase::Retiring => 1,
            GenericIndexGcPhase::Retired => 2,
        }
    }
}

impl TryFrom<u8> for GenericIndexGcPhase {
    type Error = GcRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Retiring),
            2 => Ok(Self::Retired),
            value => Err(GcRecordError::UnknownDiscriminant {
                type_name: "GenericIndexGcPhase",
                value,
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcTransition {
    BeginDeleting,
    AdvanceDeletion {
        manifest_cursor: ManifestPosition,
        scanned_manifest_row_count: u64,
        manifest_rolling_digest: [u8; SHA256_BYTES],
        deleted_object_count: u64,
        object_rolling_digest: [u8; SHA256_BYTES],
    },
    Complete {
        object_absence_digest: [u8; SHA256_BYTES],
    },
    Retry,
    Quarantine {
        evidence: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcRecordError {
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
    FieldTooLong {
        field: &'static str,
        length: usize,
        max: usize,
    },
    IdentityDigestMismatch,
    GenericIndexGcIdentityDigestMismatch,
    ZeroReferenceEpoch,
    UnsafeHistoryFloor {
        last_zero: u64,
        floor: u64,
    },
    NonMonotonicCursor,
    NonMonotonicScanCount {
        current: u64,
        next: u64,
    },
    DeletedObjectCountRegressed {
        current: u64,
        next: u64,
    },
    DeletedObjectCountExceedsScan {
        deleted: u64,
        scanned: u64,
    },
    ScannedManifestCountExceedsExpected {
        scanned: u64,
        expected: u64,
    },
    ObjectAbsenceDigestMismatch,
    RetryCountOverflow,
    ZeroBarrierGeneration,
    InvalidPhasePayload {
        phase: GcPhase,
        reason: &'static str,
    },
    InvalidPhaseTransition {
        from: GcPhase,
        to: GcPhase,
    },
    InvalidGenericIndexGcProgress {
        reason: &'static str,
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

impl fmt::Display for GcRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported GC value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::FieldTooLong { field, length, max } => {
                write!(formatter, "{field} is {length} bytes, maximum is {max}")
            }
            Self::IdentityDigestMismatch => {
                formatter.write_str("GC operation identity digest does not match immutable fields")
            }
            Self::GenericIndexGcIdentityDigestMismatch => formatter.write_str(
                "Generic index GC operation identity digest does not match immutable fields",
            ),
            Self::ZeroReferenceEpoch => {
                formatter.write_str("GC operation reference epoch must be non-zero")
            }
            Self::UnsafeHistoryFloor { last_zero, floor } => write!(
                formatter,
                "GC safe history floor {floor} must be newer than last-zero version {last_zero}"
            ),
            Self::NonMonotonicCursor => {
                formatter.write_str("GC manifest cursor must move strictly forward")
            }
            Self::NonMonotonicScanCount { current, next } => {
                write!(
                    formatter,
                    "GC scanned manifest-row count must advance beyond {current}, got {next}"
                )
            }
            Self::DeletedObjectCountRegressed { current, next } => write!(
                formatter,
                "GC deleted-object count must not regress from {current} to {next}"
            ),
            Self::DeletedObjectCountExceedsScan { deleted, scanned } => write!(
                formatter,
                "GC deleted-object count {deleted} exceeds scanned manifest-row count {scanned}"
            ),
            Self::ScannedManifestCountExceedsExpected { scanned, expected } => write!(
                formatter,
                "GC scanned manifest-row count {scanned} exceeds expected closure {expected}"
            ),
            Self::ObjectAbsenceDigestMismatch => {
                formatter.write_str("GC terminal object-absence digest does not match its closure")
            }
            Self::RetryCountOverflow => formatter.write_str("GC retry count overflow"),
            Self::ZeroBarrierGeneration => {
                formatter.write_str("GC history-barrier generation must be non-zero")
            }
            Self::InvalidPhasePayload { phase, reason } => {
                write!(formatter, "invalid {phase:?} GC payload: {reason}")
            }
            Self::InvalidPhaseTransition { from, to } => {
                write!(formatter, "invalid GC transition {from:?} -> {to:?}")
            }
            Self::InvalidGenericIndexGcProgress { reason } => {
                write!(formatter, "invalid Generic index GC progress: {reason}")
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
                write!(formatter, "GC value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for GcRecordError {}

impl GcHistoryBarrierRecord {
    pub fn encode(self) -> Result<Vec<u8>, GcRecordError> {
        if self.generation == 0 {
            return Err(GcRecordError::ZeroBarrierGeneration);
        }
        let mut encoded = Vec::with_capacity(1 + 8);
        encoded.push(GC_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.generation.to_be_bytes());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GcRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let generation = decoder.u64("generation")?;
        decoder.finish()?;
        if generation == 0 {
            return Err(GcRecordError::ZeroBarrierGeneration);
        }
        Ok(Self { generation })
    }
}

impl GcOperationRecord {
    /// Seal the immutable claim identity after assigning its fields.
    pub fn seal_identity(&mut self) {
        self.identity_digest = self.canonical_identity_digest();
    }

    pub fn canonical_identity_digest(&self) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.gc.operation.identity\0");
        hasher.update(self.operation_id.as_bytes());
        hasher.update(self.artifact_revision_id.as_bytes());
        hasher.update(self.reference_epoch.get().to_be_bytes());
        hasher.update(self.last_zero_ref_version.get().to_be_bytes());
        hasher.update(self.safe_history_floor.get().to_be_bytes());
        hasher.update(self.expected_manifest_row_count.to_be_bytes());
        hasher.update(self.expected_manifest_digest);
        hasher.update(self.expected_dependency_count.to_be_bytes());
        hasher.update(self.expected_dependency_digest);
        hasher.finalize().into()
    }

    pub fn canonical_object_absence_digest(&self) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.gc.object-absence-closure\0");
        hasher.update(self.identity_digest);
        hasher.update(self.scanned_manifest_row_count.to_be_bytes());
        hasher.update(self.manifest_rolling_digest);
        hasher.update(self.deleted_object_count.to_be_bytes());
        hasher.update(self.object_rolling_digest);
        hasher.finalize().into()
    }

    pub fn validate(&self) -> Result<(), GcRecordError> {
        if self.identity_digest != self.canonical_identity_digest() {
            return Err(GcRecordError::IdentityDigestMismatch);
        }
        if self.reference_epoch == ReferenceEpoch::ZERO {
            return Err(GcRecordError::ZeroReferenceEpoch);
        }
        if self.safe_history_floor.get() <= self.last_zero_ref_version.get() {
            return Err(GcRecordError::UnsafeHistoryFloor {
                last_zero: self.last_zero_ref_version.get(),
                floor: self.safe_history_floor.get(),
            });
        }
        if let Some(evidence) = &self.quarantine_evidence {
            if evidence.len() > MAX_GC_EVIDENCE_BYTES {
                return Err(GcRecordError::FieldTooLong {
                    field: "quarantine_evidence",
                    length: evidence.len(),
                    max: MAX_GC_EVIDENCE_BYTES,
                });
            }
        }
        if self.manifest_cursor.is_some() != (self.scanned_manifest_row_count > 0) {
            return Err(GcRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "manifest cursor presence must match non-zero manifest scan progress",
            });
        }
        if self.deleted_object_count > self.scanned_manifest_row_count {
            return Err(GcRecordError::DeletedObjectCountExceedsScan {
                deleted: self.deleted_object_count,
                scanned: self.scanned_manifest_row_count,
            });
        }
        if self.scanned_manifest_row_count > self.expected_manifest_row_count {
            return Err(GcRecordError::ScannedManifestCountExceedsExpected {
                scanned: self.scanned_manifest_row_count,
                expected: self.expected_manifest_row_count,
            });
        }
        if self.expected_manifest_row_count == 0
            && self.expected_manifest_digest != [0; SHA256_BYTES]
        {
            return Err(GcRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "empty expected manifest requires the zero publication closure",
            });
        }
        if self.scanned_manifest_row_count == 0
            && (self.manifest_rolling_digest != [0; SHA256_BYTES]
                || self.deleted_object_count != 0
                || self.object_rolling_digest != [0; SHA256_BYTES])
        {
            return Err(GcRecordError::InvalidPhasePayload {
                phase: self.phase,
                reason: "zero manifest scan progress requires zero counts and rolling digests",
            });
        }
        match self.phase {
            GcPhase::Queued | GcPhase::Claimed => {
                if self.manifest_cursor.is_some()
                    || self.scanned_manifest_row_count != 0
                    || self.manifest_rolling_digest != [0; SHA256_BYTES]
                    || self.deleted_object_count != 0
                    || self.object_rolling_digest != [0; SHA256_BYTES]
                    || self.object_absence_digest.is_some()
                    || self.quarantine_evidence.is_some()
                {
                    return Err(GcRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "unstarted GC cannot carry deletion or terminal progress",
                    });
                }
            }
            GcPhase::Deleting => {
                if self.object_absence_digest.is_some() || self.quarantine_evidence.is_some() {
                    return Err(GcRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "deleting GC cannot carry terminal proof or quarantine evidence",
                    });
                }
            }
            GcPhase::Deleted => {
                if self.object_absence_digest.is_none() || self.quarantine_evidence.is_some() {
                    return Err(GcRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "deleted GC requires one absence proof and no quarantine evidence",
                    });
                }
                if self.scanned_manifest_row_count != self.expected_manifest_row_count
                    || self.manifest_rolling_digest != self.expected_manifest_digest
                {
                    return Err(GcRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "deleted GC requires the complete sealed manifest closure",
                    });
                }
                if self.object_absence_digest != Some(self.canonical_object_absence_digest()) {
                    return Err(GcRecordError::ObjectAbsenceDigestMismatch);
                }
            }
            GcPhase::Quarantined => {
                if self.object_absence_digest.is_some()
                    || self.quarantine_evidence.as_ref().is_none_or(Vec::is_empty)
                {
                    return Err(GcRecordError::InvalidPhasePayload {
                        phase: self.phase,
                        reason: "quarantined GC requires evidence and no absence proof",
                    });
                }
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GcRecordError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.push(GC_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.operation_id.as_bytes());
        encoded.extend_from_slice(&self.identity_digest);
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        encoded.extend_from_slice(&self.reference_epoch.get().to_be_bytes());
        encoded.extend_from_slice(&self.last_zero_ref_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.safe_history_floor.get().to_be_bytes());
        encoded.extend_from_slice(&self.expected_manifest_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.expected_manifest_digest);
        encoded.extend_from_slice(&self.expected_dependency_count.to_be_bytes());
        encoded.extend_from_slice(&self.expected_dependency_digest);
        encoded.push(self.phase.into());
        put_optional_position(&mut encoded, self.manifest_cursor);
        encoded.extend_from_slice(&self.scanned_manifest_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.manifest_rolling_digest);
        encoded.extend_from_slice(&self.deleted_object_count.to_be_bytes());
        encoded.extend_from_slice(&self.object_rolling_digest);
        put_optional_fixed(&mut encoded, self.object_absence_digest.as_ref());
        encoded.extend_from_slice(&self.retry_count.to_be_bytes());
        put_optional_bytes(&mut encoded, self.quarantine_evidence.as_deref())?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GcRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
        let identity_digest = decoder.fixed("identity_digest")?;
        let artifact_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("artifact_revision_id")?);
        let reference_epoch = ReferenceEpoch::new(decoder.u64("reference_epoch")?);
        let last_zero_ref_version = CommitVersion::new(decoder.u64("last_zero_ref_version")?)
            .map_err(|_| GcRecordError::InvalidPhasePayload {
                phase: GcPhase::Queued,
                reason: "last-zero version must be non-zero",
            })?;
        let safe_history_floor =
            ReadVersion::new(decoder.u64("safe_history_floor")?).map_err(|_| {
                GcRecordError::InvalidPhasePayload {
                    phase: GcPhase::Queued,
                    reason: "safe history floor must be non-zero",
                }
            })?;
        let expected_manifest_row_count = decoder.u64("expected_manifest_row_count")?;
        let expected_manifest_digest = decoder.fixed("expected_manifest_digest")?;
        let expected_dependency_count = decoder.u32("expected_dependency_count")?;
        let expected_dependency_digest = decoder.fixed("expected_dependency_digest")?;
        let phase = decode_durable_enum(decoder.u8("phase")?)?;
        let manifest_cursor = decoder.optional_position("manifest_cursor")?;
        let scanned_manifest_row_count = decoder.u64("scanned_manifest_row_count")?;
        let manifest_rolling_digest = decoder.fixed("manifest_rolling_digest")?;
        let deleted_object_count = decoder.u64("deleted_object_count")?;
        let object_rolling_digest = decoder.fixed("object_rolling_digest")?;
        let object_absence_digest = decoder.optional_fixed("object_absence_digest")?;
        let retry_count = decoder.u32("retry_count")?;
        let quarantine_evidence = decoder.optional_bytes("quarantine_evidence")?;
        decoder.finish()?;
        let record = Self {
            operation_id,
            identity_digest,
            artifact_revision_id,
            reference_epoch,
            last_zero_ref_version,
            safe_history_floor,
            expected_manifest_row_count,
            expected_manifest_digest,
            expected_dependency_count,
            expected_dependency_digest,
            phase,
            manifest_cursor,
            scanned_manifest_row_count,
            manifest_rolling_digest,
            deleted_object_count,
            object_rolling_digest,
            object_absence_digest,
            retry_count,
            quarantine_evidence,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn apply(&self, transition: GcTransition) -> Result<Self, GcRecordError> {
        self.validate()?;
        let mut next = self.clone();
        match transition {
            GcTransition::BeginDeleting if self.phase == GcPhase::Claimed => {
                next.phase = GcPhase::Deleting;
            }
            GcTransition::AdvanceDeletion {
                manifest_cursor,
                scanned_manifest_row_count,
                manifest_rolling_digest,
                deleted_object_count,
                object_rolling_digest,
            } if self.phase == GcPhase::Deleting => {
                if self
                    .manifest_cursor
                    .is_some_and(|current| manifest_cursor <= current)
                {
                    return Err(GcRecordError::NonMonotonicCursor);
                }
                if scanned_manifest_row_count <= self.scanned_manifest_row_count {
                    return Err(GcRecordError::NonMonotonicScanCount {
                        current: self.scanned_manifest_row_count,
                        next: scanned_manifest_row_count,
                    });
                }
                if deleted_object_count < self.deleted_object_count {
                    return Err(GcRecordError::DeletedObjectCountRegressed {
                        current: self.deleted_object_count,
                        next: deleted_object_count,
                    });
                }
                next.manifest_cursor = Some(manifest_cursor);
                next.scanned_manifest_row_count = scanned_manifest_row_count;
                next.manifest_rolling_digest = manifest_rolling_digest;
                next.deleted_object_count = deleted_object_count;
                next.object_rolling_digest = object_rolling_digest;
            }
            GcTransition::Complete {
                object_absence_digest,
            } if self.phase == GcPhase::Deleting => {
                next.phase = GcPhase::Deleted;
                next.object_absence_digest = Some(object_absence_digest);
            }
            GcTransition::Retry if matches!(self.phase, GcPhase::Claimed | GcPhase::Deleting) => {
                next.retry_count = self
                    .retry_count
                    .checked_add(1)
                    .ok_or(GcRecordError::RetryCountOverflow)?;
            }
            GcTransition::Quarantine { evidence }
                if matches!(self.phase, GcPhase::Claimed | GcPhase::Deleting) =>
            {
                next.phase = GcPhase::Quarantined;
                next.quarantine_evidence = Some(evidence);
            }
            transition => {
                return Err(GcRecordError::InvalidPhaseTransition {
                    from: self.phase,
                    to: transition.target_phase(self.phase),
                });
            }
        }
        next.validate()?;
        Ok(next)
    }
}

impl GenericIndexGcOperationRecord {
    pub fn seal_identity(&mut self) {
        self.identity_digest = self.canonical_identity_digest();
    }

    pub fn canonical_identity_digest(&self) -> [u8; SHA256_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.gc.generic-index.operation.identity\0");
        hasher.update(self.operation_id.as_bytes());
        hasher.update(self.generation_id.as_bytes());
        hasher.update(self.reference_epoch.get().to_be_bytes());
        hasher.update(self.last_zero_reference_version.get().to_be_bytes());
        hasher.update(self.safe_history_floor.get().to_be_bytes());
        hasher.update(self.expected_capability_digest);
        hasher.update(self.expected_row_count.to_be_bytes());
        hasher.update(self.expected_row_digest);
        hasher.finalize().into()
    }

    pub fn validate(&self) -> Result<(), GcRecordError> {
        if self.identity_digest != self.canonical_identity_digest() {
            return Err(GcRecordError::GenericIndexGcIdentityDigestMismatch);
        }
        if self.reference_epoch == ReferenceEpoch::ZERO {
            return Err(GcRecordError::ZeroReferenceEpoch);
        }
        if self.safe_history_floor.get() <= self.last_zero_reference_version.get() {
            return Err(GcRecordError::UnsafeHistoryFloor {
                last_zero: self.last_zero_reference_version.get(),
                floor: self.safe_history_floor.get(),
            });
        }
        if self.scanned_row_count > self.expected_row_count {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "scanned row count exceeds the immutable generation closure",
            });
        }
        if self.row_cursor.is_some() != (self.scanned_row_count > 0) {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "row cursor presence must match non-zero scan progress",
            });
        }
        if self.row_cursor.is_some_and(|cursor| {
            cursor
                .checked_add(1)
                .is_none_or(|count| count != self.scanned_row_count)
        }) {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "row cursor must identify the contiguous zero-based row closure",
            });
        }
        if self.scanned_row_count == 0 && self.row_rolling_digest != [0; SHA256_BYTES] {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "zero row progress requires the canonical empty rolling digest",
            });
        }
        if self.rows_complete
            && (self.scanned_row_count != self.expected_row_count
                || self.row_rolling_digest != self.expected_row_digest)
        {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "completed rows must match the immutable generation closure",
            });
        }
        if !self.rows_complete
            && (self.expected_row_count == 0 || self.scanned_row_count == self.expected_row_count)
        {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "an exhausted row closure must be marked complete",
            });
        }
        if self.receipt_cursor.is_some() != (self.deleted_receipt_count > 0) {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "receipt cursor presence must match non-zero receipt progress",
            });
        }
        if !self.rows_complete
            && (self.receipt_cursor.is_some()
                || self.deleted_receipt_count != 0
                || self.receipts_complete)
        {
            return Err(GcRecordError::InvalidGenericIndexGcProgress {
                reason: "append receipts cannot be collected before row closure verification",
            });
        }
        match self.phase {
            GenericIndexGcPhase::Retiring if self.receipts_complete => {
                return Err(GcRecordError::InvalidGenericIndexGcProgress {
                    reason: "a fully collected operation must be retired",
                });
            }
            GenericIndexGcPhase::Retired if !self.rows_complete || !self.receipts_complete => {
                return Err(GcRecordError::InvalidGenericIndexGcProgress {
                    reason: "a retired operation requires complete row and receipt cleanup",
                });
            }
            GenericIndexGcPhase::Retiring | GenericIndexGcPhase::Retired => {}
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GcRecordError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.push(GC_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.operation_id.as_bytes());
        encoded.extend_from_slice(&self.identity_digest);
        encoded.extend_from_slice(self.generation_id.as_bytes());
        encoded.extend_from_slice(&self.reference_epoch.get().to_be_bytes());
        encoded.extend_from_slice(&self.last_zero_reference_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.safe_history_floor.get().to_be_bytes());
        encoded.extend_from_slice(&self.expected_capability_digest);
        encoded.extend_from_slice(&self.expected_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.expected_row_digest);
        encoded.push(self.phase.into());
        put_optional_u64(&mut encoded, self.row_cursor);
        encoded.extend_from_slice(&self.scanned_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.row_rolling_digest);
        encoded.push(u8::from(self.rows_complete));
        put_optional_u64(&mut encoded, self.receipt_cursor);
        encoded.extend_from_slice(&self.deleted_receipt_count.to_be_bytes());
        encoded.push(u8::from(self.receipts_complete));
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GcRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let operation_id = OperationId::from_bytes(decoder.fixed("operation_id")?);
        let identity_digest = decoder.fixed("identity_digest")?;
        let generation_id = GenericIndexGenerationId::from_bytes(decoder.fixed("generation_id")?);
        let reference_epoch = ReferenceEpoch::new(decoder.u64("reference_epoch")?);
        let last_zero_reference_version =
            CommitVersion::new(decoder.u64("last_zero_reference_version")?).map_err(|_| {
                GcRecordError::InvalidGenericIndexGcProgress {
                    reason: "last-zero reference version must be non-zero",
                }
            })?;
        let safe_history_floor =
            ReadVersion::new(decoder.u64("safe_history_floor")?).map_err(|_| {
                GcRecordError::InvalidGenericIndexGcProgress {
                    reason: "safe history floor must be non-zero",
                }
            })?;
        let expected_capability_digest = decoder.fixed("expected_capability_digest")?;
        let expected_row_count = decoder.u64("expected_row_count")?;
        let expected_row_digest = decoder.fixed("expected_row_digest")?;
        let phase = GenericIndexGcPhase::try_from(decoder.u8("phase")?)?;
        let row_cursor = decoder.optional_u64("row_cursor")?;
        let scanned_row_count = decoder.u64("scanned_row_count")?;
        let row_rolling_digest = decoder.fixed("row_rolling_digest")?;
        let rows_complete = decoder.boolean("rows_complete")?;
        let receipt_cursor = decoder.optional_u64("receipt_cursor")?;
        let deleted_receipt_count = decoder.u64("deleted_receipt_count")?;
        let receipts_complete = decoder.boolean("receipts_complete")?;
        decoder.finish()?;
        let record = Self {
            operation_id,
            identity_digest,
            generation_id,
            reference_epoch,
            last_zero_reference_version,
            safe_history_floor,
            expected_capability_digest,
            expected_row_count,
            expected_row_digest,
            phase,
            row_cursor,
            scanned_row_count,
            row_rolling_digest,
            rows_complete,
            receipt_cursor,
            deleted_receipt_count,
            receipts_complete,
        };
        record.validate()?;
        Ok(record)
    }
}

impl GcTransition {
    fn target_phase(&self, current: GcPhase) -> GcPhase {
        match self {
            Self::BeginDeleting | Self::AdvanceDeletion { .. } => GcPhase::Deleting,
            Self::Retry => current,
            Self::Complete { .. } => GcPhase::Deleted,
            Self::Quarantine { .. } => GcPhase::Quarantined,
        }
    }
}

fn put_optional_position(encoded: &mut Vec<u8>, position: Option<ManifestPosition>) {
    match position {
        None => encoded.push(0),
        Some(position) => {
            encoded.push(1);
            encoded.extend_from_slice(&position.object_index.to_be_bytes());
        }
    }
}

fn put_optional_u64(encoded: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&value.to_be_bytes());
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

fn put_optional_bytes(encoded: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), GcRecordError> {
    match value {
        None => encoded.push(0),
        Some(value) => {
            if value.len() > MAX_GC_EVIDENCE_BYTES {
                return Err(GcRecordError::FieldTooLong {
                    field: "quarantine_evidence",
                    length: value.len(),
                    max: MAX_GC_EVIDENCE_BYTES,
                });
            }
            encoded.push(1);
            encoded.extend_from_slice(
                &u32::try_from(value.len())
                    .expect("validated GC evidence length fits u32")
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(value);
        }
    }
    Ok(())
}

fn decode_durable_enum<T>(value: u8) -> Result<T, GcRecordError>
where
    T: TryFrom<u8, Error = nokv_types::UnknownDurableDiscriminant>,
{
    T::try_from(value).map_err(|error| GcRecordError::UnknownDiscriminant {
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

    fn require_value_version(&mut self) -> Result<(), GcRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == GC_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(GcRecordError::UnsupportedValueVersion {
                actual,
                expected: GC_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], GcRecordError> {
        let bytes = self.take(field, N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, GcRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, GcRecordError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, GcRecordError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn optional_position(
        &mut self,
        field: &'static str,
    ) -> Result<Option<ManifestPosition>, GcRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => Ok(Some(ManifestPosition {
                object_index: self.u64("manifest_cursor.object_index")?,
            })),
            value => Err(GcRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_u64(&mut self, field: &'static str) -> Result<Option<u64>, GcRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.u64(field).map(Some),
            value => Err(GcRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn boolean(&mut self, field: &'static str) -> Result<bool, GcRecordError> {
        match self.u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(GcRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<Option<[u8; N]>, GcRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.fixed(field).map(Some),
            value => Err(GcRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_bytes(&mut self, field: &'static str) -> Result<Option<Vec<u8>>, GcRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => {
                let length = self.u32(field)? as usize;
                if length > MAX_GC_EVIDENCE_BYTES {
                    return Err(GcRecordError::FieldTooLong {
                        field,
                        length,
                        max: MAX_GC_EVIDENCE_BYTES,
                    });
                }
                Ok(Some(self.take(field, length)?.to_vec()))
            }
            value => Err(GcRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], GcRecordError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(GcRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn finish(self) -> Result<(), GcRecordError> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(GcRecordError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(phase: GcPhase) -> GcOperationRecord {
        let mut operation = GcOperationRecord {
            operation_id: OperationId::from_bytes([1; 16]),
            identity_digest: [0; SHA256_BYTES],
            artifact_revision_id: ArtifactRevisionId::from_bytes([3; 16]),
            reference_epoch: ReferenceEpoch::new(4),
            last_zero_ref_version: CommitVersion::new(5).unwrap(),
            safe_history_floor: ReadVersion::new(6).unwrap(),
            expected_manifest_row_count: 2,
            expected_manifest_digest: [7; SHA256_BYTES],
            expected_dependency_count: 1,
            expected_dependency_digest: [8; SHA256_BYTES],
            phase,
            manifest_cursor: None,
            scanned_manifest_row_count: 0,
            manifest_rolling_digest: [0; SHA256_BYTES],
            deleted_object_count: 0,
            object_rolling_digest: [0; SHA256_BYTES],
            object_absence_digest: None,
            retry_count: 0,
            quarantine_evidence: None,
        };
        operation.seal_identity();
        operation
    }

    #[test]
    fn claimed_record_round_trip_and_strict_envelope() {
        let record = operation(GcPhase::Claimed);
        let encoded = record.encode().unwrap();
        assert_eq!(GcOperationRecord::decode(&encoded).unwrap(), record);
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            GcOperationRecord::decode(&trailing),
            Err(GcRecordError::TrailingBytes { count: 1 })
        );
    }

    #[test]
    fn deletion_progress_is_strictly_monotonic() {
        let record = operation(GcPhase::Claimed)
            .apply(GcTransition::BeginDeleting)
            .unwrap()
            .apply(GcTransition::AdvanceDeletion {
                manifest_cursor: ManifestPosition { object_index: 2 },
                scanned_manifest_row_count: 1,
                manifest_rolling_digest: [4; SHA256_BYTES],
                deleted_object_count: 1,
                object_rolling_digest: [5; SHA256_BYTES],
            })
            .unwrap();
        assert!(matches!(
            record.apply(GcTransition::AdvanceDeletion {
                manifest_cursor: ManifestPosition { object_index: 2 },
                scanned_manifest_row_count: 2,
                manifest_rolling_digest: [5; SHA256_BYTES],
                deleted_object_count: 2,
                object_rolling_digest: [6; SHA256_BYTES],
            }),
            Err(GcRecordError::NonMonotonicCursor)
        ));
        assert!(matches!(
            record.apply(GcTransition::AdvanceDeletion {
                manifest_cursor: ManifestPosition { object_index: 3 },
                scanned_manifest_row_count: 1,
                manifest_rolling_digest: [5; SHA256_BYTES],
                deleted_object_count: 2,
                object_rolling_digest: [6; SHA256_BYTES],
            }),
            Err(GcRecordError::NonMonotonicScanCount { .. })
        ));
    }

    #[test]
    fn complete_and_quarantine_are_terminal_and_exclusive() {
        let deleting = operation(GcPhase::Claimed)
            .apply(GcTransition::BeginDeleting)
            .unwrap()
            .apply(GcTransition::AdvanceDeletion {
                manifest_cursor: ManifestPosition { object_index: 3 },
                scanned_manifest_row_count: 2,
                manifest_rolling_digest: [7; SHA256_BYTES],
                deleted_object_count: 1,
                object_rolling_digest: [6; SHA256_BYTES],
            })
            .unwrap();
        let object_absence_digest = deleting.canonical_object_absence_digest();
        let deleted = deleting
            .apply(GcTransition::Complete {
                object_absence_digest,
            })
            .unwrap();
        assert_eq!(deleted.phase, GcPhase::Deleted);
        assert_eq!(
            GcOperationRecord::decode(&deleted.encode().unwrap()).unwrap(),
            deleted
        );

        let quarantined = operation(GcPhase::Claimed)
            .apply(GcTransition::Quarantine {
                evidence: b"provider outcome ambiguous".to_vec(),
            })
            .unwrap();
        assert_eq!(quarantined.phase, GcPhase::Quarantined);
        assert!(quarantined.object_absence_digest.is_none());
    }

    #[test]
    fn borrowed_only_batch_advances_scan_without_deletion_count() {
        let deleting = operation(GcPhase::Claimed)
            .apply(GcTransition::BeginDeleting)
            .unwrap()
            .apply(GcTransition::AdvanceDeletion {
                manifest_cursor: ManifestPosition { object_index: 1 },
                scanned_manifest_row_count: 1,
                manifest_rolling_digest: [9; SHA256_BYTES],
                deleted_object_count: 0,
                object_rolling_digest: [0; SHA256_BYTES],
            })
            .unwrap();
        assert_eq!(deleting.scanned_manifest_row_count, 1);
        assert_eq!(deleting.deleted_object_count, 0);
    }

    #[test]
    fn unknown_phase_fails_closed() {
        let mut encoded = operation(GcPhase::Claimed).encode().unwrap();
        let phase_offset =
            1 + 16 + SHA256_BYTES + 16 + 8 + 8 + 8 + 8 + SHA256_BYTES + 4 + SHA256_BYTES;
        encoded[phase_offset] = 0xff;
        assert_eq!(
            GcOperationRecord::decode(&encoded),
            Err(GcRecordError::UnknownDiscriminant {
                type_name: "GcPhase",
                value: 0xff,
            })
        );
    }

    #[test]
    fn generic_index_gc_progress_round_trips_and_requires_a_strict_floor() {
        let mut operation = GenericIndexGcOperationRecord {
            operation_id: OperationId::from_bytes([9; 16]),
            identity_digest: [0; SHA256_BYTES],
            generation_id: GenericIndexGenerationId::from_bytes([8; 16]),
            reference_epoch: ReferenceEpoch::new(4),
            last_zero_reference_version: CommitVersion::new(5).unwrap(),
            safe_history_floor: ReadVersion::new(6).unwrap(),
            expected_capability_digest: [7; SHA256_BYTES],
            expected_row_count: 2,
            expected_row_digest: [6; SHA256_BYTES],
            phase: GenericIndexGcPhase::Retiring,
            row_cursor: None,
            scanned_row_count: 0,
            row_rolling_digest: [0; SHA256_BYTES],
            rows_complete: false,
            receipt_cursor: None,
            deleted_receipt_count: 0,
            receipts_complete: false,
        };
        operation.seal_identity();
        let encoded = operation.encode().unwrap();
        assert_eq!(
            GenericIndexGcOperationRecord::decode(&encoded).unwrap(),
            operation
        );

        operation.safe_history_floor = ReadVersion::new(5).unwrap();
        operation.seal_identity();
        assert!(matches!(
            operation.validate(),
            Err(GcRecordError::UnsafeHistoryFloor { .. })
        ));
    }
}
