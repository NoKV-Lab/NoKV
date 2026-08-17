/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Epoch-fenced artifact-revision garbage collection.
//!
//! Authoritative manifest rows drive physical deletion. Object-provider
//! listing is deliberately absent from this service.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommandDigest, CommitVersion, GcClaimState, GcPhase,
    GenericIndexGenerationId, GenericIndexGenerationState, LogicalShardId, OperationId,
    OperationKind, ReadVersion, ReferenceEpoch, RevisionState, RootId, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::codec::{
    artifact_manifest_key, artifact_manifest_prefix, artifact_revision_key,
    decode_artifact_manifest_key, decode_gc_candidate_key, decode_generic_index_append_receipt_key,
    decode_generic_index_generation_key, decode_generic_index_row_key,
    decode_revision_dependency_ref_key, gc_candidate_key, gc_candidate_prefix,
    gc_history_barrier_key, generic_index_append_receipt_key, generic_index_append_receipt_prefix,
    generic_index_generation_key, generic_index_generation_prefix,
    generic_index_generation_ref_prefix, generic_index_row_key, generic_index_row_prefix,
    history_hold_prefix, object_block_key, operation_key, revision_dependency_ref_prefix,
    SCHEMA_ID,
};
use super::engine::{
    CommandMutation, CommandPredicate, HistoryProjection, MetaError, MetaShard, MetadataCommand,
    MetadataCommandResult, RootFenceAction,
};
use super::gc_records::{
    GcHistoryBarrierRecord, GcOperationRecord, GcRecordError, GcTransition,
    GenericIndexGcOperationRecord, GenericIndexGcPhase,
};
use super::generic_index_records::{
    advance_generic_index_row_rolling_digest, generic_index_capability_digest,
    generic_index_row_digest, GenericIndexAppendReceiptRecord, GenericIndexGenerationRecord,
    GenericIndexRecordError, GenericIndexRowRecord,
};
use super::keyspace::MetadataFamily;
use super::namespace::{RootReadContext, RootWriteContext};
use super::publication_records::{
    ArtifactRevisionRecord, GcCandidateRecord, PublicationRecordCodecError, RevisionRefRecord,
    MAX_DEPENDENCY_COUNT, MAX_QUARANTINE_EVIDENCE_BYTES,
};
use super::publish_operation_records::{ArtifactManifestRow, ManifestPosition, PublishRecordError};
use super::snapshot_records::{HistoryHoldRecord, SnapshotRecordError};

const GC_RESULT_FORMAT_VERSION: u8 = 1;
const GC_RESULT_OPERATION: u8 = 1;
const GC_RESULT_STALE_CANDIDATE: u8 = 2;
const GC_RESULT_HISTORY_BARRIER: u8 = 3;
const GC_RESULT_GENERIC_INDEX_OPERATION: u8 = 4;
const MAX_META_SCAN_ROWS: usize = 256;
// Complete mutates revision/candidate/operation plus, for each dependency,
// its reference, owner revision, and at most one newly-zero candidate.
const _: () = assert!(MAX_DEPENDENCY_COUNT as usize * 3 + 3 <= MAX_META_SCAN_ROWS);

/// Maximum number of manifest rows admitted by one recoverable delete batch.
pub const MAX_GC_BATCH_ROWS: usize = 192;
/// Maximum candidate rows returned by one page.
pub const MAX_GC_CANDIDATE_PAGE_SIZE: usize = 255;
/// Maximum Generic index payload rows or append receipts deleted by one
/// metadata command.
pub const MAX_GENERIC_INDEX_GC_BATCH_ROWS: usize = MAX_GC_BATCH_ROWS;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GcCandidateCursor {
    pub artifact_revision_id: ArtifactRevisionId,
    pub reference_epoch: ReferenceEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcCandidateEntry {
    pub cursor: GcCandidateCursor,
    pub candidate: GcCandidateRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcCandidatePage {
    pub entries: Vec<GcCandidateEntry>,
    pub next_cursor: Option<GcCandidateCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GenericIndexGenerationGcCandidateCursor {
    pub generation_id: GenericIndexGenerationId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexGenerationGcCandidate {
    pub cursor: GenericIndexGenerationGcCandidateCursor,
    pub reference_epoch: ReferenceEpoch,
    pub last_zero_reference_version: CommitVersion,
    pub capability_digest: [u8; SHA256_BYTES],
    pub row_count: u64,
    pub row_digest: [u8; SHA256_BYTES],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexGenerationGcCandidatePage {
    pub entries: Vec<GenericIndexGenerationGcCandidate>,
    /// Cursor over scanned headers, not only eligible entries. An empty page
    /// can therefore still carry forward progress past retained generations.
    pub next_cursor: Option<GenericIndexGenerationGcCandidateCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcManifestEntry {
    pub position: ManifestPosition,
    pub row: ArtifactManifestRow,
    /// Only rows physically owned by the collected revision may be deleted.
    pub delete_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcManifestBatch {
    pub entries: Vec<GcManifestEntry>,
    pub end_of_manifest: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcObjectAbsence {
    pub position: ManifestPosition,
    pub object_key: String,
    /// Required for target-owned rows and forbidden for borrowed rows.
    pub absence_digest: Option<[u8; SHA256_BYTES]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimGcRequest {
    pub context: RootWriteContext,
    pub artifact_revision_id: ArtifactRevisionId,
    pub reference_epoch: ReferenceEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginGcDeletionRequest {
    pub context: RootWriteContext,
    pub expected_operation: GcOperationRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdvanceGcDeletionBatchRequest {
    pub context: RootWriteContext,
    pub expected_operation: GcOperationRecord,
    pub confirmations: Vec<GcObjectAbsence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteGcRequest {
    pub context: RootWriteContext,
    pub expected_operation: GcOperationRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineGcRequest {
    pub context: RootWriteContext,
    pub expected_operation: GcOperationRecord,
    pub evidence: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClearStaleGcCandidateRequest {
    pub context: RootWriteContext,
    pub artifact_revision_id: ArtifactRevisionId,
    pub reference_epoch: ReferenceEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimGenericIndexGenerationGcRequest {
    pub context: RootWriteContext,
    pub generation_id: GenericIndexGenerationId,
    pub reference_epoch: ReferenceEpoch,
    pub capability_digest: [u8; SHA256_BYTES],
    pub row_count: u64,
    pub row_digest: [u8; SHA256_BYTES],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectGenericIndexGenerationGcBatchRequest {
    pub context: RootWriteContext,
    pub expected_operation: GenericIndexGcOperationRecord,
    pub batch_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcCommandOutcome {
    pub commit_version: CommitVersion,
    pub operation: GcOperationRecord,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcCandidateClearOutcome {
    pub commit_version: CommitVersion,
    pub artifact_revision_id: ArtifactRevisionId,
    pub reference_epoch: ReferenceEpoch,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcHistoryBarrierOutcome {
    pub commit_version: CommitVersion,
    pub barrier: GcHistoryBarrierRecord,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexGenerationGcOutcome {
    pub commit_version: CommitVersion,
    pub operation: GenericIndexGcOperationRecord,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcError {
    Meta(MetaError),
    OperationCodec(GcRecordError),
    PublicationRecordCodec(PublicationRecordCodecError),
    ManifestCodec(PublishRecordError),
    SnapshotRecordCodec(SnapshotRecordError),
    GenericIndexRecordCodec(GenericIndexRecordError),
    InvalidPageSize {
        requested: usize,
        max: usize,
    },
    CorruptKey {
        family: &'static str,
    },
    RevisionNotFound {
        revision: ArtifactRevisionId,
    },
    CandidateNotFound {
        revision: ArtifactRevisionId,
        epoch: ReferenceEpoch,
    },
    OperationNotFound {
        operation_id: OperationId,
    },
    GenericIndexGenerationNotFound {
        generation_id: GenericIndexGenerationId,
    },
    GenericIndexGenerationNotClaimable {
        state: GenericIndexGenerationState,
        reference_count: u64,
    },
    GenericIndexGenerationSealMismatch {
        reason: &'static str,
    },
    GenericIndexReferenceRowsPresent,
    GenericIndexPayloadClosureMismatch {
        reason: String,
    },
    RevisionNotClaimable {
        state: RevisionState,
        strong_reference_count: u64,
    },
    CandidateNotClaimable {
        state: GcClaimState,
    },
    ReferenceEpochMismatch {
        expected: ReferenceEpoch,
        actual: ReferenceEpoch,
    },
    LastZeroVersionMismatch,
    UnsafeHistoryFloor {
        last_zero: u64,
        floor: u64,
    },
    InvalidOperationPhase {
        expected: GcPhase,
        actual: GcPhase,
    },
    StateMismatch {
        reason: String,
    },
    EmptyBatch,
    BatchTooLarge {
        count: usize,
        max: usize,
    },
    ManifestBatchMismatch {
        reason: String,
    },
    ManifestClosureMismatch {
        reason: String,
    },
    MissingObjectAbsence {
        position: ManifestPosition,
    },
    UnexpectedObjectAbsence {
        position: ManifestPosition,
    },
    CountOverflow {
        field: &'static str,
    },
    ReferenceEpochOverflow {
        revision: ArtifactRevisionId,
    },
    ReferenceCountUnderflow {
        revision: ArtifactRevisionId,
    },
    DependencyClosureMismatch {
        reason: String,
    },
    DependencyOwnerNotFound {
        revision: ArtifactRevisionId,
    },
    DependencyOwnerUnavailable {
        revision: ArtifactRevisionId,
        state: RevisionState,
    },
    CandidateNotStale,
    HistoryHoldActive,
    BarrierGenerationOverflow,
    InvalidQuarantineEvidence {
        length: usize,
        max: usize,
    },
    ConcurrentMutation,
    DeterministicResultMismatch {
        reason: String,
    },
}

impl fmt::Display for GcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta(error) => error.fmt(formatter),
            Self::OperationCodec(error) => write!(formatter, "invalid GC operation: {error}"),
            Self::PublicationRecordCodec(error) => {
                write!(formatter, "invalid GC publication record: {error}")
            }
            Self::ManifestCodec(error) => write!(formatter, "invalid GC manifest row: {error}"),
            Self::SnapshotRecordCodec(error) => {
                write!(formatter, "invalid GC history hold: {error}")
            }
            Self::GenericIndexRecordCodec(error) => {
                write!(formatter, "invalid Generic index GC record: {error}")
            }
            Self::InvalidPageSize { requested, max } => {
                write!(formatter, "GC page size {requested} is outside 1..={max}")
            }
            Self::CorruptKey { family } => write!(formatter, "malformed {family} key"),
            Self::RevisionNotFound { revision } => {
                write!(formatter, "GC revision {:02x?} was not found", revision.as_bytes())
            }
            Self::CandidateNotFound { revision, epoch } => write!(
                formatter,
                "GC candidate {:02x?}/{} was not found",
                revision.as_bytes(),
                epoch.get()
            ),
            Self::OperationNotFound { operation_id } => write!(
                formatter,
                "GC operation {:02x?} was not found",
                operation_id.as_bytes()
            ),
            Self::GenericIndexGenerationNotFound { generation_id } => write!(
                formatter,
                "Generic index GC generation {:02x?} was not found",
                generation_id.as_bytes()
            ),
            Self::GenericIndexGenerationNotClaimable {
                state,
                reference_count,
            } => write!(
                formatter,
                "Generic index generation is not GC-claimable in {state:?} with {reference_count} references"
            ),
            Self::GenericIndexGenerationSealMismatch { reason } => {
                write!(formatter, "Generic index generation seal mismatch: {reason}")
            }
            Self::GenericIndexReferenceRowsPresent => formatter
                .write_str("Generic index generation has strong-reference rows during GC"),
            Self::GenericIndexPayloadClosureMismatch { reason } => {
                write!(formatter, "Generic index payload closure mismatch: {reason}")
            }
            Self::RevisionNotClaimable {
                state,
                strong_reference_count,
            } => write!(
                formatter,
                "revision is not GC-claimable in {state:?} with {strong_reference_count} strong references"
            ),
            Self::CandidateNotClaimable { state } => {
                write!(formatter, "GC candidate is not claimable in {state:?}")
            }
            Self::ReferenceEpochMismatch { expected, actual } => write!(
                formatter,
                "GC reference epoch mismatch: expected {}, found {}",
                expected.get(),
                actual.get()
            ),
            Self::LastZeroVersionMismatch => {
                formatter.write_str("GC candidate and revision last-zero versions do not match")
            }
            Self::UnsafeHistoryFloor { last_zero, floor } => write!(
                formatter,
                "GC history floor {floor} is not newer than last-zero version {last_zero}"
            ),
            Self::InvalidOperationPhase { expected, actual } => {
                write!(formatter, "expected GC phase {expected:?}, found {actual:?}")
            }
            Self::StateMismatch { reason } => {
                write!(formatter, "GC state records are inconsistent: {reason}")
            }
            Self::EmptyBatch => formatter.write_str("GC deletion batch must not be empty"),
            Self::BatchTooLarge { count, max } => {
                write!(formatter, "GC deletion batch has {count} rows, maximum is {max}")
            }
            Self::ManifestBatchMismatch { reason } => {
                write!(formatter, "GC manifest batch mismatch: {reason}")
            }
            Self::ManifestClosureMismatch { reason } => {
                write!(formatter, "GC manifest closure mismatch: {reason}")
            }
            Self::MissingObjectAbsence { position } => write!(
                formatter,
                "GC target-owned object at {} lacks absence evidence",
                position.object_index
            ),
            Self::UnexpectedObjectAbsence { position } => write!(
                formatter,
                "GC borrowed object at {} must not carry deletion evidence",
                position.object_index
            ),
            Self::CountOverflow { field } => write!(formatter, "GC {field} overflow"),
            Self::ReferenceEpochOverflow { revision } => write!(
                formatter,
                "reference epoch overflow for revision {:02x?}",
                revision.as_bytes()
            ),
            Self::ReferenceCountUnderflow { revision } => write!(
                formatter,
                "strong reference count underflow for revision {:02x?}",
                revision.as_bytes()
            ),
            Self::DependencyClosureMismatch { reason } => {
                write!(formatter, "GC dependency closure mismatch: {reason}")
            }
            Self::DependencyOwnerNotFound { revision } => write!(
                formatter,
                "GC dependency owner {:02x?} was not found",
                revision.as_bytes()
            ),
            Self::DependencyOwnerUnavailable { revision, state } => write!(
                formatter,
                "GC dependency owner {:02x?} is {state:?}",
                revision.as_bytes()
            ),
            Self::CandidateNotStale => {
                formatter.write_str("GC candidate still matches the current revision epoch")
            }
            Self::HistoryHoldActive => {
                formatter.write_str("GC history barrier is blocked by an active history hold")
            }
            Self::BarrierGenerationOverflow => {
                formatter.write_str("GC history-barrier generation overflow")
            }
            Self::InvalidQuarantineEvidence { length, max } => write!(
                formatter,
                "GC quarantine evidence length {length} is outside 1..={max}"
            ),
            Self::ConcurrentMutation => {
                formatter.write_str("GC metadata changed concurrently; rebuild the request")
            }
            Self::DeterministicResultMismatch { reason } => {
                write!(formatter, "GC replay result mismatch: {reason}")
            }
        }
    }
}

impl std::error::Error for GcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(source) => Some(source),
            Self::OperationCodec(source) => Some(source),
            Self::PublicationRecordCodec(source) => Some(source),
            Self::ManifestCodec(source) => Some(source),
            Self::SnapshotRecordCodec(source) => Some(source),
            Self::GenericIndexRecordCodec(source) => Some(source),
            _ => None,
        }
    }
}

impl From<MetaError> for GcError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<GcRecordError> for GcError {
    fn from(error: GcRecordError) -> Self {
        Self::OperationCodec(error)
    }
}

impl From<PublicationRecordCodecError> for GcError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::PublicationRecordCodec(error)
    }
}

impl From<PublishRecordError> for GcError {
    fn from(error: PublishRecordError) -> Self {
        Self::ManifestCodec(error)
    }
}

impl From<SnapshotRecordError> for GcError {
    fn from(error: SnapshotRecordError) -> Self {
        Self::SnapshotRecordCodec(error)
    }
}

impl From<GenericIndexRecordError> for GcError {
    fn from(error: GenericIndexRecordError) -> Self {
        Self::GenericIndexRecordCodec(error)
    }
}

#[derive(Clone, Copy)]
pub struct GcService<'a> {
    store: &'a MetaShard,
}

impl<'a> GcService<'a> {
    pub const fn new(store: &'a MetaShard) -> Self {
        Self { store }
    }

    /// Minimum retained history version. Every existing hold, including one
    /// being released, remains a pin until its row is atomically removed.
    pub fn safe_history_floor(&self, context: RootReadContext) -> Result<ReadVersion, GcError> {
        let prefix = history_hold_prefix(context.root_id);
        let mut floor = context.read_version;
        let mut start_after = None::<Vec<u8>>;
        loop {
            let page = self.store.scan_prefix_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::HistoryHold,
                &prefix,
                context.read_version,
                start_after.as_deref(),
                MAX_META_SCAN_ROWS,
            )?;
            if page.is_empty() {
                break;
            }
            for item in &page {
                let hold = HistoryHoldRecord::decode(&item.value)?;
                floor = floor.min(hold.read_version);
            }
            start_after = page.last().map(|item| item.key.clone());
            if page.len() < MAX_META_SCAN_ROWS {
                break;
            }
        }
        Ok(floor)
    }

    /// Advance a quiescent root's durable metadata clock through a real,
    /// root-scoped GC barrier mutation. Active history holds reject the
    /// command; callers cannot use the barrier to jump over retained history.
    pub fn advance_history_barrier(
        &self,
        context: RootWriteContext,
    ) -> Result<GcHistoryBarrierOutcome, GcError> {
        let input_digest = history_barrier_input_digest(context.root_id, context.read_version);
        if let Some(outcome) = replay_history_barrier_outcome(self.store, context, input_digest)? {
            return Ok(outcome);
        }
        require_current_write_frontier(self.store, context)?;
        let hold_prefix = history_hold_prefix(context.root_id);
        if !self
            .store
            .scan_prefix_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::HistoryHold,
                &hold_prefix,
                context.read_version,
                None,
                1,
            )?
            .is_empty()
        {
            return Err(GcError::HistoryHoldActive);
        }

        let key = gc_history_barrier_key(context.root_id);
        let current_payload = read_current(self.store, context, MetadataFamily::GcBarrier, &key)?;
        let current_generation = current_payload
            .as_deref()
            .map(GcHistoryBarrierRecord::decode)
            .transpose()?
            .map_or(0, |record| record.generation);
        let barrier = GcHistoryBarrierRecord {
            generation: current_generation
                .checked_add(1)
                .ok_or(GcError::BarrierGenerationOverflow)?,
        };
        let deterministic_result = encode_history_barrier_result(input_digest, barrier);
        let mut command = base_command(context, deterministic_result);
        command.predicates.push(CommandPredicate::PrefixEmpty {
            family: MetadataFamily::HistoryHold,
            prefix: hold_prefix,
        });
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::GcBarrier,
            key: key.clone(),
            expected: current_payload,
        });
        command.mutations.push(CommandMutation::Put {
            family: MetadataFamily::GcBarrier,
            key: key.clone(),
            value: barrier.encode()?,
        });
        if current_generation != 0 {
            command.history_projection.push(HistoryProjection {
                family: MetadataFamily::GcBarrier,
                key,
            });
        }
        let result = execute_command(self.store, &command.seal())?;
        decode_history_barrier_outcome(result, input_digest)
    }

    pub fn list_candidates(
        &self,
        context: RootReadContext,
        start_after: Option<GcCandidateCursor>,
        page_size: usize,
    ) -> Result<GcCandidatePage, GcError> {
        validate_page_size(page_size, MAX_GC_CANDIDATE_PAGE_SIZE)?;
        let prefix = gc_candidate_prefix(context.root_id);
        let start_key = start_after.map(|cursor| {
            gc_candidate_key(
                context.root_id,
                cursor.artifact_revision_id,
                cursor.reference_epoch,
            )
        });
        let mut rows = self.store.scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GcCandidate,
            &prefix,
            context.read_version,
            start_key.as_deref(),
            page_size + 1,
        )?;
        let has_more = rows.len() > page_size;
        if has_more {
            rows.pop();
        }
        let mut entries = Vec::with_capacity(rows.len());
        for item in rows {
            let (artifact_revision_id, reference_epoch) =
                decode_gc_candidate_key(context.root_id, &item.key).ok_or(GcError::CorruptKey {
                    family: "GcCandidate",
                })?;
            entries.push(GcCandidateEntry {
                cursor: GcCandidateCursor {
                    artifact_revision_id,
                    reference_epoch,
                },
                candidate: GcCandidateRecord::decode(&item.value)?,
            });
        }
        let next_cursor = has_more
            .then(|| entries.last().map(|entry| entry.cursor))
            .flatten();
        Ok(GcCandidatePage {
            entries,
            next_cursor,
        })
    }

    pub fn list_generic_index_generation_candidates(
        &self,
        context: RootReadContext,
        start_after: Option<GenericIndexGenerationGcCandidateCursor>,
        page_size: usize,
    ) -> Result<GenericIndexGenerationGcCandidatePage, GcError> {
        validate_page_size(page_size, MAX_GC_CANDIDATE_PAGE_SIZE)?;
        let prefix = generic_index_generation_prefix(context.root_id);
        let start_key = start_after
            .map(|cursor| generic_index_generation_key(context.root_id, cursor.generation_id));
        let mut rows = self.store.scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &prefix,
            context.read_version,
            start_key.as_deref(),
            page_size + 1,
        )?;
        let has_more = rows.len() > page_size;
        if has_more {
            rows.pop();
        }
        let next_cursor = if has_more {
            let last = rows.last().expect("a non-empty bounded page has a cursor");
            let generation_id = decode_generic_index_generation_key(context.root_id, &last.key)
                .ok_or(GcError::CorruptKey {
                    family: "GenericIndexGeneration(header)",
                })?;
            Some(GenericIndexGenerationGcCandidateCursor { generation_id })
        } else {
            None
        };
        let safe_history_floor = self.safe_history_floor(context)?;
        let mut entries = Vec::with_capacity(rows.len());
        for item in rows {
            let generation_id = decode_generic_index_generation_key(context.root_id, &item.key)
                .ok_or(GcError::CorruptKey {
                    family: "GenericIndexGeneration(header)",
                })?;
            let generation = GenericIndexGenerationRecord::decode(&item.value)?;
            let Some(last_zero_reference_version) = generation.last_zero_reference_version else {
                continue;
            };
            if generation.state != GenericIndexGenerationState::Sealed
                || generation.reference_count != 0
                || safe_history_floor.get() <= last_zero_reference_version.get()
            {
                continue;
            }
            entries.push(GenericIndexGenerationGcCandidate {
                cursor: GenericIndexGenerationGcCandidateCursor { generation_id },
                reference_epoch: generation.reference_epoch,
                last_zero_reference_version,
                capability_digest: generic_index_capability_digest(&generation.capabilities)?,
                row_count: generation.appended_row_count,
                row_digest: generation.rolling_row_digest,
            });
        }
        Ok(GenericIndexGenerationGcCandidatePage {
            entries,
            next_cursor,
        })
    }

    pub fn claim(&self, request: ClaimGcRequest) -> Result<GcCommandOutcome, GcError> {
        let input_digest = claim_input_digest(
            request.context.root_id,
            request.artifact_revision_id,
            request.reference_epoch,
        );
        if let Some(outcome) = replay_operation_outcome(
            self.store,
            request.context,
            input_digest,
            gc_operation_id(
                request.context.root_id,
                request.artifact_revision_id,
                request.reference_epoch,
            ),
        )? {
            return Ok(outcome);
        }

        let revision_key =
            artifact_revision_key(request.context.root_id, request.artifact_revision_id);
        let revision_payload = read_current(
            self.store,
            request.context,
            MetadataFamily::ArtifactRevision,
            &revision_key,
        )?
        .ok_or(GcError::RevisionNotFound {
            revision: request.artifact_revision_id,
        })?;
        let revision = ArtifactRevisionRecord::decode(&revision_payload)?;
        if revision.state != RevisionState::Available || revision.strong_reference_count != 0 {
            return Err(GcError::RevisionNotClaimable {
                state: revision.state,
                strong_reference_count: revision.strong_reference_count,
            });
        }
        if revision.reference_epoch != request.reference_epoch {
            return Err(GcError::ReferenceEpochMismatch {
                expected: request.reference_epoch,
                actual: revision.reference_epoch,
            });
        }

        let candidate_key = gc_candidate_key(
            request.context.root_id,
            request.artifact_revision_id,
            request.reference_epoch,
        );
        let candidate_payload = read_current(
            self.store,
            request.context,
            MetadataFamily::GcCandidate,
            &candidate_key,
        )?
        .ok_or(GcError::CandidateNotFound {
            revision: request.artifact_revision_id,
            epoch: request.reference_epoch,
        })?;
        let candidate = GcCandidateRecord::decode(&candidate_payload)?;
        if candidate.claim_state != GcClaimState::Candidate
            || candidate.quarantine_evidence.is_some()
        {
            return Err(GcError::CandidateNotClaimable {
                state: candidate.claim_state,
            });
        }
        if revision.last_zero_ref_version != Some(candidate.last_zero_ref_version) {
            return Err(GcError::LastZeroVersionMismatch);
        }

        let read_context = read_context(request.context);
        let safe_history_floor = self.safe_history_floor(read_context)?;
        if safe_history_floor.get() <= candidate.last_zero_ref_version.get() {
            return Err(GcError::UnsafeHistoryFloor {
                last_zero: candidate.last_zero_ref_version.get(),
                floor: safe_history_floor.get(),
            });
        }
        let expected_manifest_digest = parse_sha256_digest_uri(&revision.manifest_digest_uri)?;
        let operation_id = gc_operation_id(
            request.context.root_id,
            request.artifact_revision_id,
            request.reference_epoch,
        );
        let mut operation = GcOperationRecord {
            operation_id,
            identity_digest: [0; SHA256_BYTES],
            artifact_revision_id: request.artifact_revision_id,
            reference_epoch: request.reference_epoch,
            last_zero_ref_version: candidate.last_zero_ref_version,
            safe_history_floor,
            expected_manifest_row_count: revision.block_count,
            expected_manifest_digest,
            expected_dependency_count: revision.dependency_count,
            expected_dependency_digest: revision.dependency_digest,
            phase: GcPhase::Claimed,
            manifest_cursor: None,
            scanned_manifest_row_count: 0,
            manifest_rolling_digest: [0; SHA256_BYTES],
            deleted_object_count: 0,
            object_rolling_digest: [0; SHA256_BYTES],
            object_absence_digest: None,
            retry_count: candidate.retry_count,
            quarantine_evidence: None,
        };
        operation.seal_identity();
        operation.validate()?;

        let mut next_revision = revision;
        next_revision.state = RevisionState::Deleting;
        let mut next_candidate = candidate;
        next_candidate.claim_state = GcClaimState::Claimed;
        let deterministic_result = encode_operation_result(input_digest, &operation)?;
        let mut command = base_command(request.context, deterministic_result);
        replace_exact(
            &mut command,
            MetadataFamily::ArtifactRevision,
            revision_key,
            revision_payload,
            next_revision.encode()?,
        );
        replace_exact(
            &mut command,
            MetadataFamily::GcCandidate,
            candidate_key,
            candidate_payload,
            next_candidate.encode()?,
        );
        put_absent(
            &mut command,
            MetadataFamily::Operation,
            operation_key(request.context.root_id, OperationKind::Gc, operation_id),
            operation.encode()?,
        );
        execute_operation_command(self.store, command.seal(), input_digest, operation_id)
    }

    pub fn begin_deletion(
        &self,
        request: BeginGcDeletionRequest,
    ) -> Result<GcCommandOutcome, GcError> {
        let input_digest =
            operation_input_digest(2, request.context.root_id, &request.expected_operation, &[])?;
        if let Some(outcome) = replay_operation_outcome(
            self.store,
            request.context,
            input_digest,
            request.expected_operation.operation_id,
        )? {
            return Ok(outcome);
        }
        require_phase(&request.expected_operation, GcPhase::Claimed)?;
        let triple = load_gc_triple(self.store, request.context, &request.expected_operation)?;
        let next_operation = request
            .expected_operation
            .apply(GcTransition::BeginDeleting)?;
        let deterministic_result = encode_operation_result(input_digest, &next_operation)?;
        let mut command = base_command(request.context, deterministic_result);
        predicate_triple(&mut command, &triple);
        replace_exact(
            &mut command,
            MetadataFamily::Operation,
            triple.operation_key,
            triple.operation_payload,
            next_operation.encode()?,
        );
        execute_operation_command(
            self.store,
            command.seal(),
            input_digest,
            next_operation.operation_id,
        )
    }

    /// Read the next authoritative manifest page that a provider worker must
    /// process. The page never originates from provider listing.
    pub fn scan_manifest_batch(
        &self,
        context: RootWriteContext,
        expected_operation: &GcOperationRecord,
        page_size: usize,
    ) -> Result<GcManifestBatch, GcError> {
        validate_page_size(page_size, MAX_GC_BATCH_ROWS)?;
        require_current_write_frontier(self.store, context)?;
        require_phase(expected_operation, GcPhase::Deleting)?;
        load_gc_triple(self.store, context, expected_operation)?;
        let mut entries = load_manifest_rows(
            self.store,
            read_context(context),
            context.logical_shard_id,
            expected_operation,
            page_size + 1,
        )?;
        let end_of_manifest = entries.len() <= page_size;
        if !end_of_manifest {
            entries.pop();
        }
        Ok(GcManifestBatch {
            entries,
            end_of_manifest,
        })
    }

    pub fn advance_deletion_batch(
        &self,
        request: AdvanceGcDeletionBatchRequest,
    ) -> Result<GcCommandOutcome, GcError> {
        validate_batch_size(request.confirmations.len())?;
        let extra_digest = confirmations_digest(&request.confirmations)?;
        let input_digest = operation_input_digest(
            3,
            request.context.root_id,
            &request.expected_operation,
            &extra_digest,
        )?;
        if let Some(outcome) = replay_operation_outcome(
            self.store,
            request.context,
            input_digest,
            request.expected_operation.operation_id,
        )? {
            return Ok(outcome);
        }
        require_phase(&request.expected_operation, GcPhase::Deleting)?;
        let triple = load_gc_triple(self.store, request.context, &request.expected_operation)?;
        let rows = load_manifest_rows(
            self.store,
            read_context(request.context),
            request.context.logical_shard_id,
            &request.expected_operation,
            request.confirmations.len(),
        )?;
        if rows.len() != request.confirmations.len() {
            return Err(GcError::ManifestBatchMismatch {
                reason: "confirmation count exceeds remaining authoritative manifest rows"
                    .to_owned(),
            });
        }

        let mut manifest_digest = request.expected_operation.manifest_rolling_digest;
        let mut object_digest = request.expected_operation.object_rolling_digest;
        let mut deleted_count = request.expected_operation.deleted_object_count;
        for (entry, confirmation) in rows.iter().zip(&request.confirmations) {
            if confirmation.position != entry.position {
                return Err(GcError::ManifestBatchMismatch {
                    reason: "confirmation positions are not the next contiguous manifest keys"
                        .to_owned(),
                });
            }
            if confirmation.object_key != entry.row.object_key {
                return Err(GcError::ManifestBatchMismatch {
                    reason: "confirmation object key differs from authoritative manifest"
                        .to_owned(),
                });
            }
            manifest_digest = advance_manifest_digest(manifest_digest, entry.position, &entry.row)?;
            if entry.delete_required {
                let absence_digest =
                    confirmation
                        .absence_digest
                        .ok_or(GcError::MissingObjectAbsence {
                            position: entry.position,
                        })?;
                object_digest = advance_object_absence_digest(
                    object_digest,
                    entry.position,
                    &entry.row,
                    absence_digest,
                )?;
                deleted_count = deleted_count.checked_add(1).ok_or(GcError::CountOverflow {
                    field: "deleted-object count",
                })?;
            } else if confirmation.absence_digest.is_some() {
                return Err(GcError::UnexpectedObjectAbsence {
                    position: entry.position,
                });
            }
        }
        let scanned_manifest_row_count = request
            .expected_operation
            .scanned_manifest_row_count
            .checked_add(
                u64::try_from(rows.len()).map_err(|_| GcError::CountOverflow {
                    field: "scanned manifest-row count",
                })?,
            )
            .ok_or(GcError::CountOverflow {
                field: "scanned manifest-row count",
            })?;
        let manifest_cursor = rows
            .last()
            .expect("non-empty deletion batch was validated")
            .position;
        let next_operation = request
            .expected_operation
            .apply(GcTransition::AdvanceDeletion {
                manifest_cursor,
                scanned_manifest_row_count,
                manifest_rolling_digest: manifest_digest,
                deleted_object_count: deleted_count,
                object_rolling_digest: object_digest,
            })?;
        let deterministic_result = encode_operation_result(input_digest, &next_operation)?;
        let mut command = base_command(request.context, deterministic_result);
        predicate_triple(&mut command, &triple);
        replace_exact(
            &mut command,
            MetadataFamily::Operation,
            triple.operation_key,
            triple.operation_payload,
            next_operation.encode()?,
        );
        execute_operation_command(
            self.store,
            command.seal(),
            input_digest,
            next_operation.operation_id,
        )
    }

    pub fn complete(&self, request: CompleteGcRequest) -> Result<GcCommandOutcome, GcError> {
        let input_digest =
            operation_input_digest(4, request.context.root_id, &request.expected_operation, &[])?;
        if let Some(outcome) = replay_operation_outcome(
            self.store,
            request.context,
            input_digest,
            request.expected_operation.operation_id,
        )? {
            return Ok(outcome);
        }
        require_phase(&request.expected_operation, GcPhase::Deleting)?;
        let triple = load_gc_triple(self.store, request.context, &request.expected_operation)?;
        validate_complete_manifest(
            self.store,
            request.context,
            &request.expected_operation,
            &triple.revision,
        )?;
        let dependency_updates = load_dependency_updates(
            self.store,
            request.context,
            &request.expected_operation,
            &triple.revision,
        )?;

        let object_absence_digest = request.expected_operation.canonical_object_absence_digest();
        let next_operation = request.expected_operation.apply(GcTransition::Complete {
            object_absence_digest,
        })?;
        let mut next_revision = triple.revision.clone();
        next_revision.state = RevisionState::Deleted;
        let mut next_candidate = triple.candidate.clone();
        next_candidate.claim_state = GcClaimState::Complete;
        next_candidate.quarantine_evidence = None;

        let deterministic_result = encode_operation_result(input_digest, &next_operation)?;
        let mut command = base_command(request.context, deterministic_result);
        replace_exact(
            &mut command,
            MetadataFamily::ArtifactRevision,
            triple.revision_key,
            triple.revision_payload,
            next_revision.encode()?,
        );
        replace_exact(
            &mut command,
            MetadataFamily::GcCandidate,
            triple.candidate_key,
            triple.candidate_payload,
            next_candidate.encode()?,
        );
        replace_exact(
            &mut command,
            MetadataFamily::Operation,
            triple.operation_key,
            triple.operation_payload,
            next_operation.encode()?,
        );
        for update in dependency_updates {
            delete_exact(
                &mut command,
                MetadataFamily::RevisionRef,
                update.reference_key,
                update.reference_payload,
            );
            replace_exact(
                &mut command,
                MetadataFamily::ArtifactRevision,
                update.owner_key,
                update.owner_payload,
                update.next_owner.encode()?,
            );
            if let Some((key, candidate)) = update.next_candidate {
                put_absent(
                    &mut command,
                    MetadataFamily::GcCandidate,
                    key,
                    candidate.encode()?,
                );
            }
        }
        execute_operation_command(
            self.store,
            command.seal(),
            input_digest,
            next_operation.operation_id,
        )
    }

    pub fn quarantine(&self, request: QuarantineGcRequest) -> Result<GcCommandOutcome, GcError> {
        if request.evidence.is_empty() || request.evidence.len() > MAX_QUARANTINE_EVIDENCE_BYTES {
            return Err(GcError::InvalidQuarantineEvidence {
                length: request.evidence.len(),
                max: MAX_QUARANTINE_EVIDENCE_BYTES,
            });
        }
        let input_digest = operation_input_digest(
            5,
            request.context.root_id,
            &request.expected_operation,
            &request.evidence,
        )?;
        if let Some(outcome) = replay_operation_outcome(
            self.store,
            request.context,
            input_digest,
            request.expected_operation.operation_id,
        )? {
            return Ok(outcome);
        }
        if !matches!(
            request.expected_operation.phase,
            GcPhase::Claimed | GcPhase::Deleting
        ) {
            return Err(GcError::InvalidOperationPhase {
                expected: GcPhase::Deleting,
                actual: request.expected_operation.phase,
            });
        }
        let triple = load_gc_triple(self.store, request.context, &request.expected_operation)?;
        let next_operation = request.expected_operation.apply(GcTransition::Quarantine {
            evidence: request.evidence.clone(),
        })?;
        let mut next_revision = triple.revision.clone();
        next_revision.state = RevisionState::Quarantined;
        let mut next_candidate = triple.candidate.clone();
        next_candidate.claim_state = GcClaimState::Quarantined;
        next_candidate.quarantine_evidence = Some(request.evidence);

        let deterministic_result = encode_operation_result(input_digest, &next_operation)?;
        let mut command = base_command(request.context, deterministic_result);
        replace_exact(
            &mut command,
            MetadataFamily::ArtifactRevision,
            triple.revision_key,
            triple.revision_payload,
            next_revision.encode()?,
        );
        replace_exact(
            &mut command,
            MetadataFamily::GcCandidate,
            triple.candidate_key,
            triple.candidate_payload,
            next_candidate.encode()?,
        );
        replace_exact(
            &mut command,
            MetadataFamily::Operation,
            triple.operation_key,
            triple.operation_payload,
            next_operation.encode()?,
        );
        execute_operation_command(
            self.store,
            command.seal(),
            input_digest,
            next_operation.operation_id,
        )
    }

    pub fn claim_generic_index_generation(
        &self,
        request: ClaimGenericIndexGenerationGcRequest,
    ) -> Result<GenericIndexGenerationGcOutcome, GcError> {
        let input_digest = generic_index_gc_claim_input_digest(&request);
        let operation_id = generic_index_gc_operation_id(
            request.context.root_id,
            request.generation_id,
            request.reference_epoch,
        );
        if let Some(outcome) = replay_generic_index_gc_outcome(
            self.store,
            request.context,
            input_digest,
            operation_id,
        )? {
            return Ok(outcome);
        }
        let command = plan_generic_index_generation_gc_claim(self.store, &request)?;
        execute_generic_index_gc_command(self.store, command, input_digest, operation_id)
    }

    pub fn collect_generic_index_generation_batch(
        &self,
        request: CollectGenericIndexGenerationGcBatchRequest,
    ) -> Result<GenericIndexGenerationGcOutcome, GcError> {
        validate_page_size(request.batch_size, MAX_GENERIC_INDEX_GC_BATCH_ROWS)?;
        request.expected_operation.validate()?;
        if request.expected_operation.phase != GenericIndexGcPhase::Retiring {
            return Err(GcError::StateMismatch {
                reason: "Generic index GC operation is not retiring".to_owned(),
            });
        }
        let input_digest = generic_index_gc_collect_input_digest(&request)?;
        if let Some(outcome) = replay_generic_index_gc_outcome(
            self.store,
            request.context,
            input_digest,
            request.expected_operation.operation_id,
        )? {
            return Ok(outcome);
        }
        let loaded =
            load_generic_index_gc_state(self.store, request.context, &request.expected_operation)?;
        let mut next_operation = request.expected_operation.clone();
        let mut next_generation = None;
        let deletions: Vec<(Vec<u8>, Vec<u8>)>;

        if !next_operation.rows_complete {
            let prefix =
                generic_index_row_prefix(request.context.root_id, next_operation.generation_id);
            let start_after = next_operation.row_cursor.map(|sequence| {
                generic_index_row_key(
                    request.context.root_id,
                    next_operation.generation_id,
                    sequence,
                )
            });
            let mut rows = self.store.scan_prefix_at(
                request.context.root_id,
                request.context.placement_generation,
                request.context.owner_epoch,
                MetadataFamily::GenericIndexGeneration,
                &prefix,
                request.context.read_version,
                start_after.as_deref(),
                request.batch_size + 1,
            )?;
            let rows_complete = rows.len() <= request.batch_size;
            if !rows_complete {
                rows.pop();
            }
            let mut rolling_digest = next_operation.row_rolling_digest;
            let mut expected_sequence = next_operation.scanned_row_count;
            for item in &rows {
                let sequence = decode_generic_index_row_key(
                    request.context.root_id,
                    next_operation.generation_id,
                    &item.key,
                )
                .ok_or(GcError::CorruptKey {
                    family: "GenericIndexGeneration(row)",
                })?;
                if sequence != expected_sequence {
                    return Err(GcError::GenericIndexPayloadClosureMismatch {
                        reason: format!(
                            "expected contiguous row sequence {expected_sequence}, found {sequence}"
                        ),
                    });
                }
                let row = GenericIndexRowRecord::decode(&item.value)?;
                rolling_digest = advance_generic_index_row_rolling_digest(
                    rolling_digest,
                    generic_index_row_digest(sequence, &row)?,
                );
                expected_sequence =
                    expected_sequence
                        .checked_add(1)
                        .ok_or(GcError::CountOverflow {
                            field: "Generic index GC row count",
                        })?;
            }
            if expected_sequence > next_operation.expected_row_count {
                return Err(GcError::GenericIndexPayloadClosureMismatch {
                    reason: "generation contains more rows than its immutable header seal"
                        .to_owned(),
                });
            }
            if rows_complete
                && (expected_sequence != next_operation.expected_row_count
                    || rolling_digest != next_operation.expected_row_digest)
            {
                return Err(GcError::GenericIndexPayloadClosureMismatch {
                    reason: "validated rows do not match the immutable count/digest seal"
                        .to_owned(),
                });
            }
            if let Some(last) = rows.last() {
                next_operation.row_cursor = decode_generic_index_row_key(
                    request.context.root_id,
                    next_operation.generation_id,
                    &last.key,
                );
            }
            next_operation.scanned_row_count = expected_sequence;
            next_operation.row_rolling_digest = rolling_digest;
            next_operation.rows_complete = rows_complete;
            deletions = rows
                .into_iter()
                .map(|item| (item.key, item.value))
                .collect();
        } else {
            let prefix = generic_index_append_receipt_prefix(
                request.context.root_id,
                next_operation.generation_id,
            );
            let start_after = next_operation.receipt_cursor.map(|first_sequence| {
                generic_index_append_receipt_key(
                    request.context.root_id,
                    next_operation.generation_id,
                    first_sequence,
                )
            });
            let mut receipts = self.store.scan_prefix_at(
                request.context.root_id,
                request.context.placement_generation,
                request.context.owner_epoch,
                MetadataFamily::GenericIndexGeneration,
                &prefix,
                request.context.read_version,
                start_after.as_deref(),
                request.batch_size + 1,
            )?;
            let receipts_complete = receipts.len() <= request.batch_size;
            if !receipts_complete {
                receipts.pop();
            }
            let mut previous = next_operation.receipt_cursor;
            for item in &receipts {
                let first_sequence = decode_generic_index_append_receipt_key(
                    request.context.root_id,
                    next_operation.generation_id,
                    &item.key,
                )
                .ok_or(GcError::CorruptKey {
                    family: "GenericIndexGeneration(append-receipt)",
                })?;
                if previous.is_some_and(|cursor| first_sequence <= cursor) {
                    return Err(GcError::GenericIndexPayloadClosureMismatch {
                        reason: "append receipts are not strictly ordered".to_owned(),
                    });
                }
                let receipt = GenericIndexAppendReceiptRecord::decode(&item.value)?;
                if receipt.first_sequence != first_sequence
                    || receipt.resulting_row_count > next_operation.expected_row_count
                {
                    return Err(GcError::GenericIndexPayloadClosureMismatch {
                        reason: "append receipt does not belong to the sealed row closure"
                            .to_owned(),
                    });
                }
                previous = Some(first_sequence);
            }
            next_operation.receipt_cursor = previous;
            next_operation.deleted_receipt_count = next_operation
                .deleted_receipt_count
                .checked_add(
                    u64::try_from(receipts.len()).map_err(|_| GcError::CountOverflow {
                        field: "Generic index GC receipt count",
                    })?,
                )
                .ok_or(GcError::CountOverflow {
                    field: "Generic index GC receipt count",
                })?;
            next_operation.receipts_complete = receipts_complete;
            if receipts_complete {
                next_operation.phase = GenericIndexGcPhase::Retired;
                let mut retired = loaded.generation.clone();
                retired.state = GenericIndexGenerationState::Retired;
                next_generation = Some(retired);
            }
            deletions = receipts
                .into_iter()
                .map(|item| (item.key, item.value))
                .collect();
        }
        next_operation.validate()?;

        let deterministic_result = encode_generic_index_gc_result(input_digest, &next_operation)?;
        let mut command = base_command(request.context, deterministic_result);
        if let Some(next_generation) = next_generation {
            replace_exact(
                &mut command,
                MetadataFamily::GenericIndexGeneration,
                loaded.generation_key,
                loaded.generation_payload,
                next_generation.encode()?,
            );
        } else {
            predicate_generic_index_gc_state(&mut command, &loaded);
        }
        command.predicates.push(CommandPredicate::PrefixEmpty {
            family: MetadataFamily::GenericIndexGeneration,
            prefix: generic_index_generation_ref_prefix(
                request.context.root_id,
                next_operation.generation_id,
            ),
        });
        replace_exact(
            &mut command,
            MetadataFamily::Operation,
            loaded.operation_key,
            loaded.operation_payload,
            next_operation.encode()?,
        );
        for (key, payload) in deletions {
            delete_exact(
                &mut command,
                MetadataFamily::GenericIndexGeneration,
                key,
                payload,
            );
        }
        execute_generic_index_gc_command(
            self.store,
            command.seal(),
            input_digest,
            next_operation.operation_id,
        )
    }

    pub fn clear_stale_candidate(
        &self,
        request: ClearStaleGcCandidateRequest,
    ) -> Result<GcCandidateClearOutcome, GcError> {
        let input_digest = stale_candidate_input_digest(
            request.context.root_id,
            request.artifact_revision_id,
            request.reference_epoch,
        );
        if let Some(outcome) = replay_candidate_clear_outcome(
            self.store,
            request.context,
            input_digest,
            request.artifact_revision_id,
            request.reference_epoch,
        )? {
            return Ok(outcome);
        }
        let revision_key =
            artifact_revision_key(request.context.root_id, request.artifact_revision_id);
        let revision_payload = read_current(
            self.store,
            request.context,
            MetadataFamily::ArtifactRevision,
            &revision_key,
        )?
        .ok_or(GcError::RevisionNotFound {
            revision: request.artifact_revision_id,
        })?;
        let revision = ArtifactRevisionRecord::decode(&revision_payload)?;
        let candidate_key = gc_candidate_key(
            request.context.root_id,
            request.artifact_revision_id,
            request.reference_epoch,
        );
        let candidate_payload = read_current(
            self.store,
            request.context,
            MetadataFamily::GcCandidate,
            &candidate_key,
        )?
        .ok_or(GcError::CandidateNotFound {
            revision: request.artifact_revision_id,
            epoch: request.reference_epoch,
        })?;
        let candidate = GcCandidateRecord::decode(&candidate_payload)?;
        if candidate.claim_state != GcClaimState::Candidate
            || candidate.quarantine_evidence.is_some()
        {
            return Err(GcError::CandidateNotClaimable {
                state: candidate.claim_state,
            });
        }
        if revision.reference_epoch == request.reference_epoch {
            return Err(GcError::CandidateNotStale);
        }

        let deterministic_result = encode_candidate_clear_result(
            input_digest,
            request.artifact_revision_id,
            request.reference_epoch,
        );
        let mut command = base_command(request.context, deterministic_result);
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::ArtifactRevision,
            key: revision_key,
            expected: Some(revision_payload),
        });
        delete_exact(
            &mut command,
            MetadataFamily::GcCandidate,
            candidate_key,
            candidate_payload,
        );
        execute_candidate_clear_command(
            self.store,
            command.seal(),
            input_digest,
            request.artifact_revision_id,
            request.reference_epoch,
        )
    }
}

/// Stable operation id that lets recovery map a claimed epoch-keyed candidate
/// back to its sole GC operation without a second candidate-owned pointer.
pub fn gc_operation_id(
    root_id: RootId,
    artifact_revision_id: ArtifactRevisionId,
    reference_epoch: ReferenceEpoch,
) -> OperationId {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.gc.operation-id\0");
    hasher.update(root_id.as_bytes());
    hasher.update(artifact_revision_id.as_bytes());
    hasher.update(reference_epoch.get().to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    OperationId::from_bytes(bytes)
}

/// Stable recovery identity for one exact Generic index generation epoch.
pub fn generic_index_gc_operation_id(
    root_id: RootId,
    generation_id: GenericIndexGenerationId,
    reference_epoch: ReferenceEpoch,
) -> OperationId {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.gc.generic-index.operation-id\0");
    hasher.update(root_id.as_bytes());
    hasher.update(generation_id.as_bytes());
    hasher.update(reference_epoch.get().to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    OperationId::from_bytes(bytes)
}

fn plan_generic_index_generation_gc_claim(
    store: &MetaShard,
    request: &ClaimGenericIndexGenerationGcRequest,
) -> Result<MetadataCommand, GcError> {
    require_current_write_frontier(store, request.context)?;
    let generation_key =
        generic_index_generation_key(request.context.root_id, request.generation_id);
    let generation_payload = read_current(
        store,
        request.context,
        MetadataFamily::GenericIndexGeneration,
        &generation_key,
    )?
    .ok_or(GcError::GenericIndexGenerationNotFound {
        generation_id: request.generation_id,
    })?;
    let generation = GenericIndexGenerationRecord::decode(&generation_payload)?;
    if generation.state != GenericIndexGenerationState::Sealed || generation.reference_count != 0 {
        return Err(GcError::GenericIndexGenerationNotClaimable {
            state: generation.state,
            reference_count: generation.reference_count,
        });
    }
    if generation.reference_epoch != request.reference_epoch {
        return Err(GcError::ReferenceEpochMismatch {
            expected: request.reference_epoch,
            actual: generation.reference_epoch,
        });
    }
    let last_zero_reference_version = generation.last_zero_reference_version.ok_or(
        GcError::GenericIndexGenerationSealMismatch {
            reason: "zero-reference generation lacks its last-zero version",
        },
    )?;
    if generic_index_capability_digest(&generation.capabilities)? != request.capability_digest {
        return Err(GcError::GenericIndexGenerationSealMismatch {
            reason: "capability digest differs from the caller's immutable seal",
        });
    }
    if generation.appended_row_count != request.row_count
        || generation.declared_row_count != request.row_count
        || generation.rolling_row_digest != request.row_digest
    {
        return Err(GcError::GenericIndexGenerationSealMismatch {
            reason: "row count or digest differs from the caller's immutable seal",
        });
    }
    ensure_generic_index_reference_prefix_empty(
        store,
        read_context(request.context),
        request.generation_id,
    )?;
    let safe_history_floor =
        GcService::new(store).safe_history_floor(read_context(request.context))?;
    if safe_history_floor.get() <= last_zero_reference_version.get() {
        return Err(GcError::UnsafeHistoryFloor {
            last_zero: last_zero_reference_version.get(),
            floor: safe_history_floor.get(),
        });
    }

    let operation_id = generic_index_gc_operation_id(
        request.context.root_id,
        request.generation_id,
        request.reference_epoch,
    );
    let mut operation = GenericIndexGcOperationRecord {
        operation_id,
        identity_digest: [0; SHA256_BYTES],
        generation_id: request.generation_id,
        reference_epoch: request.reference_epoch,
        last_zero_reference_version,
        safe_history_floor,
        expected_capability_digest: request.capability_digest,
        expected_row_count: request.row_count,
        expected_row_digest: request.row_digest,
        phase: GenericIndexGcPhase::Retiring,
        row_cursor: None,
        scanned_row_count: 0,
        row_rolling_digest: [0; SHA256_BYTES],
        rows_complete: request.row_count == 0,
        receipt_cursor: None,
        deleted_receipt_count: 0,
        receipts_complete: false,
    };
    operation.seal_identity();
    operation.validate()?;

    let mut retiring = generation;
    retiring.state = GenericIndexGenerationState::Retiring;
    let input_digest = generic_index_gc_claim_input_digest(request);
    let deterministic_result = encode_generic_index_gc_result(input_digest, &operation)?;
    let mut command = base_command(request.context, deterministic_result);
    command.predicates.push(CommandPredicate::PrefixEmpty {
        family: MetadataFamily::GenericIndexGeneration,
        prefix: generic_index_generation_ref_prefix(request.context.root_id, request.generation_id),
    });
    replace_exact(
        &mut command,
        MetadataFamily::GenericIndexGeneration,
        generation_key,
        generation_payload,
        retiring.encode()?,
    );
    put_absent(
        &mut command,
        MetadataFamily::Operation,
        operation_key(request.context.root_id, OperationKind::Gc, operation_id),
        operation.encode()?,
    );
    Ok(command.seal())
}

fn validate_page_size(requested: usize, max: usize) -> Result<(), GcError> {
    if requested == 0 || requested > max {
        Err(GcError::InvalidPageSize { requested, max })
    } else {
        Ok(())
    }
}

fn validate_batch_size(count: usize) -> Result<(), GcError> {
    if count == 0 {
        Err(GcError::EmptyBatch)
    } else if count > MAX_GC_BATCH_ROWS {
        Err(GcError::BatchTooLarge {
            count,
            max: MAX_GC_BATCH_ROWS,
        })
    } else {
        Ok(())
    }
}

fn require_current_write_frontier(
    store: &MetaShard,
    context: RootWriteContext,
) -> Result<(), GcError> {
    if store.current_read_version()? == context.read_version {
        Ok(())
    } else {
        Err(GcError::ConcurrentMutation)
    }
}

fn require_phase(operation: &GcOperationRecord, expected: GcPhase) -> Result<(), GcError> {
    operation.validate()?;
    if operation.phase == expected {
        Ok(())
    } else {
        Err(GcError::InvalidOperationPhase {
            expected,
            actual: operation.phase,
        })
    }
}

fn read_context(context: RootWriteContext) -> RootReadContext {
    RootReadContext {
        root_id: context.root_id,
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        read_version: context.read_version,
    }
}

fn read_current(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, GcError> {
    store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            family,
            key,
            context.read_version,
        )
        .map_err(Into::into)
}

fn ensure_generic_index_reference_prefix_empty(
    store: &MetaShard,
    context: RootReadContext,
    generation_id: GenericIndexGenerationId,
) -> Result<(), GcError> {
    let prefix = generic_index_generation_ref_prefix(context.root_id, generation_id);
    if store
        .scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &prefix,
            context.read_version,
            None,
            1,
        )?
        .is_empty()
    {
        Ok(())
    } else {
        Err(GcError::GenericIndexReferenceRowsPresent)
    }
}

struct LoadedGenericIndexGcState {
    generation_key: Vec<u8>,
    generation_payload: Vec<u8>,
    generation: GenericIndexGenerationRecord,
    operation_key: Vec<u8>,
    operation_payload: Vec<u8>,
}

fn load_generic_index_gc_state(
    store: &MetaShard,
    context: RootWriteContext,
    expected_operation: &GenericIndexGcOperationRecord,
) -> Result<LoadedGenericIndexGcState, GcError> {
    expected_operation.validate()?;
    require_current_write_frontier(store, context)?;
    let generation_key =
        generic_index_generation_key(context.root_id, expected_operation.generation_id);
    let generation_payload = read_current(
        store,
        context,
        MetadataFamily::GenericIndexGeneration,
        &generation_key,
    )?
    .ok_or(GcError::GenericIndexGenerationNotFound {
        generation_id: expected_operation.generation_id,
    })?;
    let generation = GenericIndexGenerationRecord::decode(&generation_payload)?;
    if generation.state != GenericIndexGenerationState::Retiring
        || generation.reference_count != 0
        || generation.reference_epoch != expected_operation.reference_epoch
        || generation.last_zero_reference_version
            != Some(expected_operation.last_zero_reference_version)
        || generic_index_capability_digest(&generation.capabilities)?
            != expected_operation.expected_capability_digest
        || generation.declared_row_count != expected_operation.expected_row_count
        || generation.appended_row_count != expected_operation.expected_row_count
        || generation.rolling_row_digest != expected_operation.expected_row_digest
    {
        return Err(GcError::StateMismatch {
            reason: "Generic index generation header differs from the claimed epoch and seal"
                .to_owned(),
        });
    }
    ensure_generic_index_reference_prefix_empty(
        store,
        read_context(context),
        expected_operation.generation_id,
    )?;
    let operation_key = operation_key(
        context.root_id,
        OperationKind::Gc,
        expected_operation.operation_id,
    );
    let operation_payload =
        read_current(store, context, MetadataFamily::Operation, &operation_key)?.ok_or(
            GcError::OperationNotFound {
                operation_id: expected_operation.operation_id,
            },
        )?;
    let operation = GenericIndexGcOperationRecord::decode(&operation_payload)?;
    if operation != *expected_operation {
        return Err(GcError::ConcurrentMutation);
    }
    Ok(LoadedGenericIndexGcState {
        generation_key,
        generation_payload,
        generation,
        operation_key,
        operation_payload,
    })
}

fn predicate_generic_index_gc_state(
    command: &mut MetadataCommand,
    state: &LoadedGenericIndexGcState,
) {
    command.predicates.push(CommandPredicate::Value {
        family: MetadataFamily::GenericIndexGeneration,
        key: state.generation_key.clone(),
        expected: Some(state.generation_payload.clone()),
    });
}

struct LoadedGcTriple {
    revision_key: Vec<u8>,
    revision_payload: Vec<u8>,
    revision: ArtifactRevisionRecord,
    candidate_key: Vec<u8>,
    candidate_payload: Vec<u8>,
    candidate: GcCandidateRecord,
    operation_key: Vec<u8>,
    operation_payload: Vec<u8>,
}

fn load_gc_triple(
    store: &MetaShard,
    context: RootWriteContext,
    expected_operation: &GcOperationRecord,
) -> Result<LoadedGcTriple, GcError> {
    expected_operation.validate()?;
    let revision_key =
        artifact_revision_key(context.root_id, expected_operation.artifact_revision_id);
    let revision_payload = read_current(
        store,
        context,
        MetadataFamily::ArtifactRevision,
        &revision_key,
    )?
    .ok_or(GcError::RevisionNotFound {
        revision: expected_operation.artifact_revision_id,
    })?;
    let revision = ArtifactRevisionRecord::decode(&revision_payload)?;
    let candidate_key = gc_candidate_key(
        context.root_id,
        expected_operation.artifact_revision_id,
        expected_operation.reference_epoch,
    );
    let candidate_payload =
        read_current(store, context, MetadataFamily::GcCandidate, &candidate_key)?.ok_or(
            GcError::CandidateNotFound {
                revision: expected_operation.artifact_revision_id,
                epoch: expected_operation.reference_epoch,
            },
        )?;
    let candidate = GcCandidateRecord::decode(&candidate_payload)?;
    let operation_key = operation_key(
        context.root_id,
        OperationKind::Gc,
        expected_operation.operation_id,
    );
    let operation_payload =
        read_current(store, context, MetadataFamily::Operation, &operation_key)?.ok_or(
            GcError::OperationNotFound {
                operation_id: expected_operation.operation_id,
            },
        )?;
    let operation = GcOperationRecord::decode(&operation_payload)?;
    if operation != *expected_operation {
        return Err(GcError::ConcurrentMutation);
    }
    validate_synchronized_state(&revision, &candidate, &operation)?;
    Ok(LoadedGcTriple {
        revision_key,
        revision_payload,
        revision,
        candidate_key,
        candidate_payload,
        candidate,
        operation_key,
        operation_payload,
    })
}

fn validate_synchronized_state(
    revision: &ArtifactRevisionRecord,
    candidate: &GcCandidateRecord,
    operation: &GcOperationRecord,
) -> Result<(), GcError> {
    if revision.reference_epoch != operation.reference_epoch {
        return Err(GcError::StateMismatch {
            reason: "revision epoch differs from operation epoch".to_owned(),
        });
    }
    if revision.strong_reference_count != 0
        || revision.last_zero_ref_version != Some(operation.last_zero_ref_version)
        || candidate.last_zero_ref_version != operation.last_zero_ref_version
    {
        return Err(GcError::StateMismatch {
            reason: "zero-reference lifetime differs across revision, candidate, and operation"
                .to_owned(),
        });
    }
    if revision.block_count != operation.expected_manifest_row_count
        || parse_sha256_digest_uri(&revision.manifest_digest_uri)?
            != operation.expected_manifest_digest
        || revision.dependency_count != operation.expected_dependency_count
        || revision.dependency_digest != operation.expected_dependency_digest
    {
        return Err(GcError::StateMismatch {
            reason: "revision closure differs from the claimed operation seal".to_owned(),
        });
    }
    match operation.phase {
        GcPhase::Claimed | GcPhase::Deleting => {
            if revision.state != RevisionState::Deleting
                || candidate.claim_state != GcClaimState::Claimed
                || candidate.quarantine_evidence.is_some()
                || candidate.retry_count != operation.retry_count
            {
                return Err(GcError::StateMismatch {
                    reason: "active claim lifecycle is not synchronized".to_owned(),
                });
            }
        }
        GcPhase::Deleted => {
            if revision.state != RevisionState::Deleted
                || candidate.claim_state != GcClaimState::Complete
                || candidate.quarantine_evidence.is_some()
            {
                return Err(GcError::StateMismatch {
                    reason: "completed GC lifecycle is not synchronized".to_owned(),
                });
            }
        }
        GcPhase::Quarantined => {
            if revision.state != RevisionState::Quarantined
                || candidate.claim_state != GcClaimState::Quarantined
                || candidate.quarantine_evidence.as_deref()
                    != operation.quarantine_evidence.as_deref()
            {
                return Err(GcError::StateMismatch {
                    reason: "quarantined GC lifecycle is not synchronized".to_owned(),
                });
            }
        }
        GcPhase::Queued => {
            return Err(GcError::StateMismatch {
                reason: "epoch candidates, not operation rows, are the GC queue".to_owned(),
            });
        }
    }
    Ok(())
}

fn predicate_triple(command: &mut MetadataCommand, triple: &LoadedGcTriple) {
    for (family, key, payload) in [
        (
            MetadataFamily::ArtifactRevision,
            &triple.revision_key,
            &triple.revision_payload,
        ),
        (
            MetadataFamily::GcCandidate,
            &triple.candidate_key,
            &triple.candidate_payload,
        ),
    ] {
        command.predicates.push(CommandPredicate::Value {
            family,
            key: key.clone(),
            expected: Some(payload.clone()),
        });
    }
}

fn load_manifest_rows(
    store: &MetaShard,
    context: RootReadContext,
    logical_shard_id: LogicalShardId,
    operation: &GcOperationRecord,
    limit: usize,
) -> Result<Vec<GcManifestEntry>, GcError> {
    let prefix = artifact_manifest_prefix(context.root_id, operation.artifact_revision_id);
    let start_after = operation.manifest_cursor.map(|position| {
        artifact_manifest_key(
            context.root_id,
            operation.artifact_revision_id,
            position.object_index,
        )
    });
    let rows = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::ArtifactManifest,
        &prefix,
        context.read_version,
        start_after.as_deref(),
        limit,
    )?;
    rows.into_iter()
        .map(|item| {
            let object_index = decode_artifact_manifest_key(
                context.root_id,
                operation.artifact_revision_id,
                &item.key,
            )
            .ok_or(GcError::CorruptKey {
                family: "ArtifactManifest",
            })?;
            let position = ManifestPosition { object_index };
            let row = ArtifactManifestRow::decode(&item.value)?;
            let expected_object_key = object_block_key(
                logical_shard_id,
                context.root_id,
                row.physical_owner_revision_id,
                row.physical_object_index,
            );
            if row.object_key != expected_object_key {
                return Err(GcError::ManifestBatchMismatch {
                    reason: "manifest object key does not match shard/root/physical-owner/owner-local-index"
                        .to_owned(),
                });
            }
            Ok(GcManifestEntry {
                position,
                delete_required: row.physical_owner_revision_id == operation.artifact_revision_id,
                row,
            })
        })
        .collect()
}

fn validate_complete_manifest(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &GcOperationRecord,
    revision: &ArtifactRevisionRecord,
) -> Result<(), GcError> {
    if operation.scanned_manifest_row_count != operation.expected_manifest_row_count
        || operation.scanned_manifest_row_count != revision.block_count
    {
        return Err(GcError::ManifestClosureMismatch {
            reason: "scanned row count does not equal the sealed revision block count".to_owned(),
        });
    }
    if operation.manifest_rolling_digest != operation.expected_manifest_digest {
        return Err(GcError::ManifestClosureMismatch {
            reason: "rolling manifest digest does not equal the publication seal".to_owned(),
        });
    }
    if !load_manifest_rows(
        store,
        read_context(context),
        context.logical_shard_id,
        operation,
        1,
    )?
    .is_empty()
    {
        return Err(GcError::ManifestClosureMismatch {
            reason: "authoritative manifest has rows beyond the durable cursor".to_owned(),
        });
    }
    Ok(())
}

struct DependencyUpdate {
    reference_key: Vec<u8>,
    reference_payload: Vec<u8>,
    owner_key: Vec<u8>,
    owner_payload: Vec<u8>,
    next_owner: ArtifactRevisionRecord,
    next_candidate: Option<(Vec<u8>, GcCandidateRecord)>,
}

fn load_dependency_updates(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &GcOperationRecord,
    revision: &ArtifactRevisionRecord,
) -> Result<Vec<DependencyUpdate>, GcError> {
    let prefix = revision_dependency_ref_prefix(context.root_id, operation.artifact_revision_id);
    let rows = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::RevisionRef,
        &prefix,
        context.read_version,
        None,
        MAX_DEPENDENCY_COUNT as usize + 1,
    )?;
    if rows.len()
        != usize::try_from(operation.expected_dependency_count).map_err(|_| {
            GcError::DependencyClosureMismatch {
                reason: "dependency count does not fit the platform".to_owned(),
            }
        })?
        || revision.dependency_count != operation.expected_dependency_count
    {
        return Err(GcError::DependencyClosureMismatch {
            reason: "dependency reference count differs from the claimed seal".to_owned(),
        });
    }
    let mut owners = Vec::with_capacity(rows.len());
    for item in &rows {
        let owner = decode_revision_dependency_ref_key(
            context.root_id,
            operation.artifact_revision_id,
            &item.key,
        )
        .ok_or(GcError::CorruptKey {
            family: "RevisionRef(RevisionDependency)",
        })?;
        if owner == operation.artifact_revision_id {
            return Err(GcError::DependencyClosureMismatch {
                reason: "revision cannot own a dependency reference to itself".to_owned(),
            });
        }
        owners.push(owner);
    }
    if dependency_digest(&owners)? != operation.expected_dependency_digest {
        return Err(GcError::DependencyClosureMismatch {
            reason: "dependency owner digest differs from the claimed seal".to_owned(),
        });
    }

    let next_commit_version = CommitVersion::new(context.read_version.get().checked_add(1).ok_or(
        GcError::CountOverflow {
            field: "commit version",
        },
    )?)
    .map_err(|_| GcError::CountOverflow {
        field: "commit version",
    })?;
    let mut updates = Vec::with_capacity(rows.len());
    for (item, owner_id) in rows.into_iter().zip(owners) {
        let reference = RevisionRefRecord::decode(&item.value)?;
        let owner_key = artifact_revision_key(context.root_id, owner_id);
        let owner_payload =
            read_current(store, context, MetadataFamily::ArtifactRevision, &owner_key)?
                .ok_or(GcError::DependencyOwnerNotFound { revision: owner_id })?;
        let owner = ArtifactRevisionRecord::decode(&owner_payload)?;
        if owner.state != RevisionState::Available {
            return Err(GcError::DependencyOwnerUnavailable {
                revision: owner_id,
                state: owner.state,
            });
        }
        if reference.reference_epoch_at_add > owner.reference_epoch {
            return Err(GcError::DependencyClosureMismatch {
                reason: "dependency reference epoch is ahead of its owner".to_owned(),
            });
        }
        let next_epoch = ReferenceEpoch::new(
            owner
                .reference_epoch
                .get()
                .checked_add(1)
                .ok_or(GcError::ReferenceEpochOverflow { revision: owner_id })?,
        );
        let next_count = owner
            .strong_reference_count
            .checked_sub(1)
            .ok_or(GcError::ReferenceCountUnderflow { revision: owner_id })?;
        let mut next_owner = owner;
        next_owner.reference_epoch = next_epoch;
        next_owner.strong_reference_count = next_count;
        next_owner.last_zero_ref_version = (next_count == 0).then_some(next_commit_version);
        let next_candidate = (next_count == 0).then(|| {
            (
                gc_candidate_key(context.root_id, owner_id, next_epoch),
                GcCandidateRecord {
                    last_zero_ref_version: next_commit_version,
                    claim_state: GcClaimState::Candidate,
                    retry_count: 0,
                    quarantine_evidence: None,
                },
            )
        });
        updates.push(DependencyUpdate {
            reference_key: item.key,
            reference_payload: item.value,
            owner_key,
            owner_payload,
            next_owner,
            next_candidate,
        });
    }
    Ok(updates)
}

fn advance_manifest_digest(
    previous: [u8; SHA256_BYTES],
    position: ManifestPosition,
    row: &ArtifactManifestRow,
) -> Result<[u8; SHA256_BYTES], GcError> {
    let encoded = row.encode()?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.manifest-row.v2\0");
    hasher.update(previous);
    hasher.update(position.object_index.to_be_bytes());
    hash_bytes(&mut hasher, &encoded)?;
    Ok(hasher.finalize().into())
}

fn advance_object_absence_digest(
    previous: [u8; SHA256_BYTES],
    position: ManifestPosition,
    row: &ArtifactManifestRow,
    absence_digest: [u8; SHA256_BYTES],
) -> Result<[u8; SHA256_BYTES], GcError> {
    let encoded = row.encode()?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.gc.object-absence\0");
    hasher.update(previous);
    hasher.update(position.object_index.to_be_bytes());
    hash_bytes(&mut hasher, &encoded)?;
    hasher.update(absence_digest);
    Ok(hasher.finalize().into())
}

fn dependency_digest(owners: &[ArtifactRevisionId]) -> Result<[u8; SHA256_BYTES], GcError> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.dependencies.v1\0");
    let count = u32::try_from(owners.len()).map_err(|_| GcError::CountOverflow {
        field: "dependency count",
    })?;
    hasher.update(count.to_be_bytes());
    for owner in owners {
        hasher.update(owner.as_bytes());
    }
    Ok(hasher.finalize().into())
}

fn parse_sha256_digest_uri(value: &str) -> Result<[u8; SHA256_BYTES], GcError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(GcError::ManifestClosureMismatch {
            reason: "revision manifest digest is not a canonical sha256 URI".to_owned(),
        });
    };
    if hex.len() != SHA256_BYTES * 2 {
        return Err(GcError::ManifestClosureMismatch {
            reason: "revision manifest digest has the wrong width".to_owned(),
        });
    }
    let mut digest = [0; SHA256_BYTES];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        digest[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(digest)
}

fn hex_nibble(value: u8) -> Result<u8, GcError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(GcError::ManifestClosureMismatch {
            reason: "revision manifest digest is not lowercase hexadecimal".to_owned(),
        }),
    }
}

fn base_command(context: RootWriteContext, deterministic_result: Vec<u8>) -> MetadataCommand {
    MetadataCommand {
        schema_id: SCHEMA_ID.to_owned(),
        root_id: context.root_id,
        logical_shard_id: context.logical_shard_id,
        object_namespace_id: Some(context.object_namespace_id),
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        request_id: context.request_id,
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: context.read_version,
        root_fence_action: RootFenceAction::RequireActive,
        predicates: Vec::new(),
        mutations: Vec::new(),
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result,
    }
}

fn replace_exact(
    command: &mut MetadataCommand,
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Vec<u8>,
    value: Vec<u8>,
) {
    command.predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: Some(expected),
    });
    command.mutations.push(CommandMutation::Put {
        family,
        key: key.clone(),
        value,
    });
    command
        .history_projection
        .push(HistoryProjection { family, key });
}

fn put_absent(command: &mut MetadataCommand, family: MetadataFamily, key: Vec<u8>, value: Vec<u8>) {
    command.predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: None,
    });
    command
        .mutations
        .push(CommandMutation::Put { family, key, value });
}

fn delete_exact(
    command: &mut MetadataCommand,
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Vec<u8>,
) {
    command.predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: Some(expected),
    });
    command.mutations.push(CommandMutation::Delete {
        family,
        key: key.clone(),
    });
    command
        .history_projection
        .push(HistoryProjection { family, key });
}

fn execute_operation_command(
    store: &MetaShard,
    command: MetadataCommand,
    input_digest: [u8; SHA256_BYTES],
    operation_id: OperationId,
) -> Result<GcCommandOutcome, GcError> {
    let result = execute_command(store, &command)?;
    decode_operation_outcome(result, input_digest, operation_id)
}

fn execute_generic_index_gc_command(
    store: &MetaShard,
    command: MetadataCommand,
    input_digest: [u8; SHA256_BYTES],
    operation_id: OperationId,
) -> Result<GenericIndexGenerationGcOutcome, GcError> {
    let result = execute_command(store, &command)?;
    decode_generic_index_gc_outcome(result, input_digest, operation_id)
}

fn execute_candidate_clear_command(
    store: &MetaShard,
    command: MetadataCommand,
    input_digest: [u8; SHA256_BYTES],
    revision: ArtifactRevisionId,
    epoch: ReferenceEpoch,
) -> Result<GcCandidateClearOutcome, GcError> {
    let result = execute_command(store, &command)?;
    decode_candidate_clear_outcome(result, input_digest, revision, epoch)
}

fn execute_command(
    store: &MetaShard,
    command: &MetadataCommand,
) -> Result<MetadataCommandResult, GcError> {
    match store.execute(command) {
        Ok(result) => Ok(result),
        Err(
            MetaError::PredicateFailed
            | MetaError::WriteConflict
            | MetaError::WriteReadVersionMismatch { .. },
        ) => Err(GcError::ConcurrentMutation),
        Err(error) => Err(GcError::Meta(error)),
    }
}

fn replay_operation_outcome(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    operation_id: OperationId,
) -> Result<Option<GcCommandOutcome>, GcError> {
    let Some(replay) = store.lookup_request_result(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        context.request_id,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_operation_outcome(
        replay,
        input_digest,
        operation_id,
    )?))
}

fn replay_generic_index_gc_outcome(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    operation_id: OperationId,
) -> Result<Option<GenericIndexGenerationGcOutcome>, GcError> {
    let Some(replay) = store.lookup_request_result(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        context.request_id,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_generic_index_gc_outcome(
        replay,
        input_digest,
        operation_id,
    )?))
}

fn replay_candidate_clear_outcome(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    revision: ArtifactRevisionId,
    epoch: ReferenceEpoch,
) -> Result<Option<GcCandidateClearOutcome>, GcError> {
    let Some(replay) = store.lookup_request_result(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        context.request_id,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_candidate_clear_outcome(
        replay,
        input_digest,
        revision,
        epoch,
    )?))
}

fn replay_history_barrier_outcome(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
) -> Result<Option<GcHistoryBarrierOutcome>, GcError> {
    let Some(replay) = store.lookup_request_result(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        context.request_id,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_history_barrier_outcome(replay, input_digest)?))
}

fn encode_operation_result(
    input_digest: [u8; SHA256_BYTES],
    operation: &GcOperationRecord,
) -> Result<Vec<u8>, GcError> {
    let operation = operation.encode()?;
    let mut encoded = Vec::with_capacity(2 + SHA256_BYTES + 4 + operation.len());
    encoded.push(GC_RESULT_FORMAT_VERSION);
    encoded.push(GC_RESULT_OPERATION);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(
        &u32::try_from(operation.len())
            .expect("bounded GC operation length fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&operation);
    Ok(encoded)
}

fn encode_generic_index_gc_result(
    input_digest: [u8; SHA256_BYTES],
    operation: &GenericIndexGcOperationRecord,
) -> Result<Vec<u8>, GcError> {
    let operation = operation.encode()?;
    let mut encoded = Vec::with_capacity(2 + SHA256_BYTES + 4 + operation.len());
    encoded.push(GC_RESULT_FORMAT_VERSION);
    encoded.push(GC_RESULT_GENERIC_INDEX_OPERATION);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(
        &u32::try_from(operation.len())
            .expect("bounded Generic index GC operation length fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&operation);
    Ok(encoded)
}

fn encode_candidate_clear_result(
    input_digest: [u8; SHA256_BYTES],
    revision: ArtifactRevisionId,
    epoch: ReferenceEpoch,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(2 + SHA256_BYTES + 16 + 8);
    encoded.push(GC_RESULT_FORMAT_VERSION);
    encoded.push(GC_RESULT_STALE_CANDIDATE);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(revision.as_bytes());
    encoded.extend_from_slice(&epoch.get().to_be_bytes());
    encoded
}

fn encode_history_barrier_result(
    input_digest: [u8; SHA256_BYTES],
    barrier: GcHistoryBarrierRecord,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(2 + SHA256_BYTES + 8);
    encoded.push(GC_RESULT_FORMAT_VERSION);
    encoded.push(GC_RESULT_HISTORY_BARRIER);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(&barrier.generation.to_be_bytes());
    encoded
}

fn decode_operation_outcome(
    result: MetadataCommandResult,
    expected_input_digest: [u8; SHA256_BYTES],
    expected_operation_id: OperationId,
) -> Result<GcCommandOutcome, GcError> {
    let mut decoder = ResultDecoder::new(&result.deterministic_result);
    decoder.header(GC_RESULT_OPERATION, expected_input_digest)?;
    let operation_length = decoder.u32("operation length")? as usize;
    let operation = GcOperationRecord::decode(decoder.take("operation", operation_length)?)?;
    decoder.finish()?;
    if operation.operation_id != expected_operation_id {
        return Err(GcError::DeterministicResultMismatch {
            reason: "operation id differs from the requested claim".to_owned(),
        });
    }
    Ok(GcCommandOutcome {
        commit_version: result.commit_version,
        operation,
        replayed: result.replayed,
    })
}

fn decode_generic_index_gc_outcome(
    result: MetadataCommandResult,
    expected_input_digest: [u8; SHA256_BYTES],
    expected_operation_id: OperationId,
) -> Result<GenericIndexGenerationGcOutcome, GcError> {
    let mut decoder = ResultDecoder::new(&result.deterministic_result);
    decoder.header(GC_RESULT_GENERIC_INDEX_OPERATION, expected_input_digest)?;
    let operation_length = decoder.u32("Generic index GC operation length")? as usize;
    let operation = GenericIndexGcOperationRecord::decode(
        decoder.take("Generic index GC operation", operation_length)?,
    )?;
    decoder.finish()?;
    if operation.operation_id != expected_operation_id {
        return Err(GcError::DeterministicResultMismatch {
            reason: "Generic index GC operation id differs from the request".to_owned(),
        });
    }
    Ok(GenericIndexGenerationGcOutcome {
        commit_version: result.commit_version,
        operation,
        replayed: result.replayed,
    })
}

fn decode_candidate_clear_outcome(
    result: MetadataCommandResult,
    expected_input_digest: [u8; SHA256_BYTES],
    expected_revision: ArtifactRevisionId,
    expected_epoch: ReferenceEpoch,
) -> Result<GcCandidateClearOutcome, GcError> {
    let mut decoder = ResultDecoder::new(&result.deterministic_result);
    decoder.header(GC_RESULT_STALE_CANDIDATE, expected_input_digest)?;
    let revision = ArtifactRevisionId::from_bytes(decoder.fixed("revision")?);
    let epoch = ReferenceEpoch::new(decoder.u64("reference epoch")?);
    decoder.finish()?;
    if revision != expected_revision || epoch != expected_epoch {
        return Err(GcError::DeterministicResultMismatch {
            reason: "cleared candidate identity differs from the request".to_owned(),
        });
    }
    Ok(GcCandidateClearOutcome {
        commit_version: result.commit_version,
        artifact_revision_id: revision,
        reference_epoch: epoch,
        replayed: result.replayed,
    })
}

fn decode_history_barrier_outcome(
    result: MetadataCommandResult,
    expected_input_digest: [u8; SHA256_BYTES],
) -> Result<GcHistoryBarrierOutcome, GcError> {
    let mut decoder = ResultDecoder::new(&result.deterministic_result);
    decoder.header(GC_RESULT_HISTORY_BARRIER, expected_input_digest)?;
    let barrier = GcHistoryBarrierRecord {
        generation: decoder.u64("barrier generation")?,
    };
    decoder.finish()?;
    barrier.encode()?;
    Ok(GcHistoryBarrierOutcome {
        commit_version: result.commit_version,
        barrier,
        replayed: result.replayed,
    })
}

fn history_barrier_input_digest(root_id: RootId, read_version: ReadVersion) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(7, root_id);
    hasher.update(read_version.get().to_be_bytes());
    hasher.finalize().into()
}

fn claim_input_digest(
    root_id: RootId,
    revision: ArtifactRevisionId,
    epoch: ReferenceEpoch,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(1, root_id);
    hasher.update(revision.as_bytes());
    hasher.update(epoch.get().to_be_bytes());
    hasher.finalize().into()
}

fn generic_index_gc_claim_input_digest(
    request: &ClaimGenericIndexGenerationGcRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(8, request.context.root_id);
    hasher.update(request.generation_id.as_bytes());
    hasher.update(request.reference_epoch.get().to_be_bytes());
    hasher.update(request.capability_digest);
    hasher.update(request.row_count.to_be_bytes());
    hasher.update(request.row_digest);
    hasher.finalize().into()
}

fn generic_index_gc_collect_input_digest(
    request: &CollectGenericIndexGenerationGcBatchRequest,
) -> Result<[u8; SHA256_BYTES], GcError> {
    let operation = request.expected_operation.encode()?;
    let mut hasher = input_hasher(9, request.context.root_id);
    hash_bytes(&mut hasher, &operation)?;
    hasher.update(
        u64::try_from(request.batch_size)
            .expect("bounded Generic index GC batch size fits u64")
            .to_be_bytes(),
    );
    Ok(hasher.finalize().into())
}

fn stale_candidate_input_digest(
    root_id: RootId,
    revision: ArtifactRevisionId,
    epoch: ReferenceEpoch,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(6, root_id);
    hasher.update(revision.as_bytes());
    hasher.update(epoch.get().to_be_bytes());
    hasher.finalize().into()
}

fn operation_input_digest(
    kind: u8,
    root_id: RootId,
    operation: &GcOperationRecord,
    extra: &[u8],
) -> Result<[u8; SHA256_BYTES], GcError> {
    let operation = operation.encode()?;
    let mut hasher = input_hasher(kind, root_id);
    hash_bytes(&mut hasher, &operation)?;
    hash_bytes(&mut hasher, extra)?;
    Ok(hasher.finalize().into())
}

fn confirmations_digest(confirmations: &[GcObjectAbsence]) -> Result<[u8; SHA256_BYTES], GcError> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.gc.confirmations\0");
    let count = u32::try_from(confirmations.len()).map_err(|_| GcError::CountOverflow {
        field: "confirmation count",
    })?;
    hasher.update(count.to_be_bytes());
    for confirmation in confirmations {
        hasher.update(confirmation.position.object_index.to_be_bytes());
        hash_bytes(&mut hasher, confirmation.object_key.as_bytes())?;
        match confirmation.absence_digest {
            None => hasher.update([0]),
            Some(digest) => {
                hasher.update([1]);
                hasher.update(digest);
            }
        }
    }
    Ok(hasher.finalize().into())
}

fn input_hasher(kind: u8, root_id: RootId) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.gc.request\0");
    hasher.update([kind]);
    hasher.update(root_id.as_bytes());
    hasher
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), GcError> {
    let length = u32::try_from(bytes.len()).map_err(|_| GcError::CountOverflow {
        field: "canonical byte length",
    })?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

struct ResultDecoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ResultDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn header(
        &mut self,
        expected_kind: u8,
        expected_input_digest: [u8; SHA256_BYTES],
    ) -> Result<(), GcError> {
        let version = self.u8("result version")?;
        let kind = self.u8("result kind")?;
        let input_digest = self.fixed("input digest")?;
        if version != GC_RESULT_FORMAT_VERSION
            || kind != expected_kind
            || input_digest != expected_input_digest
        {
            return Err(GcError::DeterministicResultMismatch {
                reason: "result version, kind, or input digest differs".to_owned(),
            });
        }
        Ok(())
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], GcError> {
        let bytes = self.take(field, N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, GcError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, GcError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, GcError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], GcError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(GcError::DeterministicResultMismatch {
                reason: format!("truncated {field}: need {length} bytes, have {remaining}"),
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn finish(self) -> Result<(), GcError> {
        let trailing = self.input.len().saturating_sub(self.offset);
        if trailing == 0 {
            Ok(())
        } else {
            Err(GcError::DeterministicResultMismatch {
                reason: format!("GC result has {trailing} trailing bytes"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use nokv_types::{
        GenericIndexGenerationId, GenericIndexGenerationState, GenericIndexReferenceKind,
        HistoryHoldState, NormalizedRelativePath, OwnerEpoch, PlacementGeneration, RequestId,
        RootActivationState, SnapshotId, FIXED_ID_BYTES,
    };

    use super::super::codec::{
        generic_index_append_receipt_key, generic_index_append_receipt_prefix,
        generic_index_generation_key, generic_index_generation_ref_key,
        generic_index_generation_ref_prefix, generic_index_row_key, generic_index_row_prefix,
        revision_dependency_ref_key, snapshot_history_hold_key,
    };
    use super::super::generic_index_records::{
        advance_generic_index_row_rolling_digest, generic_index_capability_digest,
        generic_index_row_digest, GenericIndexAppendReceiptRecord, GenericIndexGenerationRecord,
        GenericIndexGenerationRefRecord, GenericIndexRowBinding, GenericIndexRowRecord,
    };
    use super::super::AppendSegment;
    use super::*;

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn root() -> RootId {
        RootId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(7).unwrap()
    }

    fn owner_epoch() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn revision(value: u8) -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes([value; FIXED_ID_BYTES])
    }

    fn request_id(counter: &mut u128) -> RequestId {
        let value = *counter;
        *counter += 1;
        RequestId::from_bytes(value.to_be_bytes())
    }

    fn fence_command(
        store: &MetaShard,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch: owner_epoch(),
            request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal()
    }

    fn initialize_store(store: &MetaShard, counter: &mut u128) {
        store.advance_owner_epoch(None, owner_epoch()).unwrap();
        store
            .execute(&fence_command(
                store,
                request_id(counter),
                RootFenceAction::Install,
            ))
            .unwrap();
        store
            .execute(&fence_command(
                store,
                request_id(counter),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
    }

    fn write_context(store: &MetaShard, counter: &mut u128) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            nokv_types::ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner_epoch(),
            request_id(counter),
        )
        .unwrap()
    }

    fn read_context(store: &MetaShard) -> RootReadContext {
        RootReadContext::current(store, root(), placement(), owner_epoch()).unwrap()
    }

    fn sha256_uri(digest: [u8; SHA256_BYTES]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut value = String::with_capacity(7 + SHA256_BYTES * 2);
        value.push_str("sha256:");
        for byte in digest {
            value.push(HEX[usize::from(byte >> 4)] as char);
            value.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        value
    }

    #[derive(Clone)]
    struct Fixture {
        target: ArtifactRevisionId,
        dependency_owner: ArtifactRevisionId,
        epoch: ReferenceEpoch,
        rows: Vec<GcManifestEntry>,
        manifest_digest: [u8; SHA256_BYTES],
    }

    fn seed_fixture(
        store: &MetaShard,
        counter: &mut u128,
        history_hold: bool,
        stale_candidate: bool,
    ) -> Fixture {
        let target = revision(0x10);
        let dependency_owner = revision(0x20);
        let epoch = ReferenceEpoch::new(2);
        let base_row_0 = ArtifactManifestRow {
            physical_owner_revision_id: dependency_owner,
            physical_object_index: 0,
            object_key: object_block_key(shard(), root(), dependency_owner, 0),
            logical_offset: 0,
            offset: 0,
            length: 8,
            digest_uri: sha256_uri([0x31; SHA256_BYTES]),
            append_segment: None,
        };
        let base_row_1 = ArtifactManifestRow {
            physical_owner_revision_id: dependency_owner,
            physical_object_index: 1,
            object_key: object_block_key(shard(), root(), dependency_owner, 1),
            logical_offset: 8,
            offset: 0,
            length: 8,
            digest_uri: sha256_uri([0x32; SHA256_BYTES]),
            append_segment: None,
        };
        let delta_row_0 = ArtifactManifestRow {
            physical_owner_revision_id: target,
            physical_object_index: 0,
            object_key: object_block_key(shard(), root(), target, 0),
            logical_offset: 16,
            offset: 0,
            length: 8,
            digest_uri: sha256_uri([0x33; SHA256_BYTES]),
            append_segment: Some(AppendSegment {
                segment_sequence: 0,
                segment_offset: 0,
            }),
        };
        let delta_row_1 = ArtifactManifestRow {
            physical_owner_revision_id: target,
            physical_object_index: 1,
            object_key: object_block_key(shard(), root(), target, 1),
            logical_offset: 24,
            offset: 0,
            length: 8,
            digest_uri: sha256_uri([0x34; SHA256_BYTES]),
            append_segment: Some(AppendSegment {
                segment_sequence: 0,
                segment_offset: 8,
            }),
        };
        let rows = vec![
            GcManifestEntry {
                position: ManifestPosition { object_index: 0 },
                row: base_row_0,
                delete_required: false,
            },
            GcManifestEntry {
                position: ManifestPosition { object_index: 1 },
                row: base_row_1,
                delete_required: false,
            },
            GcManifestEntry {
                position: ManifestPosition { object_index: 2 },
                row: delta_row_0,
                delete_required: true,
            },
            GcManifestEntry {
                position: ManifestPosition { object_index: 3 },
                row: delta_row_1,
                delete_required: true,
            },
        ];
        let mut manifest_digest = [0; SHA256_BYTES];
        for entry in &rows {
            manifest_digest =
                advance_manifest_digest(manifest_digest, entry.position, &entry.row).unwrap();
        }
        let mut owner_manifest_digest = [0; SHA256_BYTES];
        for entry in rows.iter().take(2) {
            owner_manifest_digest =
                advance_manifest_digest(owner_manifest_digest, entry.position, &entry.row).unwrap();
        }
        let target_dependency_digest = dependency_digest(&[dependency_owner]).unwrap();
        let empty_dependency_digest = dependency_digest(&[]).unwrap();
        let last_zero = CommitVersion::new(2).unwrap();
        let target_record = ArtifactRevisionRecord {
            logical_size: 32,
            body_digest_uri: sha256_uri([0x41; SHA256_BYTES]),
            manifest_digest_uri: sha256_uri(manifest_digest),
            block_count: 4,
            dependency_count: 1,
            dependency_depth: 1,
            dependency_digest: target_dependency_digest,
            content_type: "application/octet-stream".to_owned(),
            state: RevisionState::Available,
            reference_epoch: epoch,
            strong_reference_count: 0,
            last_zero_ref_version: Some(last_zero),
        };
        let owner_record = ArtifactRevisionRecord {
            logical_size: 16,
            body_digest_uri: sha256_uri([0x42; SHA256_BYTES]),
            manifest_digest_uri: sha256_uri(owner_manifest_digest),
            block_count: 2,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: empty_dependency_digest,
            content_type: "application/octet-stream".to_owned(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        };
        let candidate = GcCandidateRecord {
            last_zero_ref_version: last_zero,
            claim_state: GcClaimState::Candidate,
            retry_count: 0,
            quarantine_evidence: None,
        };
        let context = write_context(store, counter);
        let mut command = base_command(context, Vec::new());
        put_absent(
            &mut command,
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(root(), target),
            target_record.encode().unwrap(),
        );
        put_absent(
            &mut command,
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(root(), dependency_owner),
            owner_record.encode().unwrap(),
        );
        put_absent(
            &mut command,
            MetadataFamily::GcCandidate,
            gc_candidate_key(root(), target, epoch),
            candidate.encode().unwrap(),
        );
        for entry in &rows {
            put_absent(
                &mut command,
                MetadataFamily::ArtifactManifest,
                artifact_manifest_key(root(), target, entry.position.object_index),
                entry.row.encode().unwrap(),
            );
        }
        for entry in rows.iter().take(2) {
            put_absent(
                &mut command,
                MetadataFamily::ArtifactManifest,
                artifact_manifest_key(root(), dependency_owner, entry.position.object_index),
                entry.row.encode().unwrap(),
            );
        }
        put_absent(
            &mut command,
            MetadataFamily::RevisionRef,
            revision_dependency_ref_key(root(), target, dependency_owner),
            RevisionRefRecord {
                reference_epoch_at_add: ReferenceEpoch::new(1),
            }
            .encode()
            .unwrap(),
        );
        if history_hold {
            put_absent(
                &mut command,
                MetadataFamily::HistoryHold,
                snapshot_history_hold_key(root(), SnapshotId::new(9)),
                HistoryHoldRecord {
                    read_version: ReadVersion::new(2).unwrap(),
                    source_snapshot_id: None,
                    state: HistoryHoldState::Releasing,
                }
                .encode(),
            );
        }
        if stale_candidate {
            put_absent(
                &mut command,
                MetadataFamily::GcCandidate,
                gc_candidate_key(root(), target, ReferenceEpoch::new(1)),
                GcCandidateRecord {
                    last_zero_ref_version: CommitVersion::new(1).unwrap(),
                    claim_state: GcClaimState::Candidate,
                    retry_count: 0,
                    quarantine_evidence: None,
                }
                .encode()
                .unwrap(),
            );
        }
        store.execute(&command.seal()).unwrap();
        Fixture {
            target,
            dependency_owner,
            epoch,
            rows,
            manifest_digest,
        }
    }

    fn read_payload(store: &MetaShard, family: MetadataFamily, key: &[u8]) -> Option<Vec<u8>> {
        let context = read_context(store);
        store
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                family,
                key,
                context.read_version,
            )
            .unwrap()
    }

    #[derive(Clone)]
    struct GenericIndexGcFixture {
        generation_id: GenericIndexGenerationId,
        reference_epoch: ReferenceEpoch,
        capability_digest: [u8; SHA256_BYTES],
        row_count: u64,
        row_digest: [u8; SHA256_BYTES],
        rows: Vec<GenericIndexRowRecord>,
    }

    fn seed_generic_index_gc_fixture(
        store: &MetaShard,
        counter: &mut u128,
        row_count: u64,
        receipt_count: u64,
    ) -> GenericIndexGcFixture {
        assert!(receipt_count <= row_count);
        let generation_id = GenericIndexGenerationId::from_bytes([0x71; FIXED_ID_BYTES]);
        let reference_epoch = ReferenceEpoch::new(2);
        let capabilities = Vec::new();
        let capability_digest = generic_index_capability_digest(&capabilities).unwrap();
        let rows = (0..row_count)
            .map(|sequence| GenericIndexRowRecord {
                relative_path: Some(
                    NormalizedRelativePath::new(format!("entry-{sequence:03}")).unwrap(),
                ),
                binding: GenericIndexRowBinding::Unbound,
                values: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut row_digest = [0; SHA256_BYTES];
        let mut resulting_digests = Vec::with_capacity(rows.len());
        for (sequence, row) in rows.iter().enumerate() {
            row_digest = advance_generic_index_row_rolling_digest(
                row_digest,
                generic_index_row_digest(sequence as u64, row).unwrap(),
            );
            resulting_digests.push(row_digest);
        }

        let context = write_context(store, counter);
        let commit_version =
            CommitVersion::new(context.read_version.get().checked_add(1).unwrap()).unwrap();
        let generation = GenericIndexGenerationRecord {
            capabilities,
            declared_row_count: row_count,
            appended_row_count: row_count,
            rolling_row_digest: row_digest,
            reference_count: 0,
            reference_epoch,
            last_zero_reference_version: Some(commit_version),
            state: GenericIndexGenerationState::Sealed,
        };
        let mut command = base_command(context, Vec::new());
        put_absent(
            &mut command,
            MetadataFamily::GenericIndexGeneration,
            generic_index_generation_key(root(), generation_id),
            generation.encode().unwrap(),
        );
        for (sequence, row) in rows.iter().enumerate() {
            put_absent(
                &mut command,
                MetadataFamily::GenericIndexGeneration,
                generic_index_row_key(root(), generation_id, sequence as u64),
                row.encode().unwrap(),
            );
        }
        for first_sequence in 0..receipt_count {
            put_absent(
                &mut command,
                MetadataFamily::GenericIndexGeneration,
                generic_index_append_receipt_key(root(), generation_id, first_sequence),
                GenericIndexAppendReceiptRecord {
                    first_sequence,
                    row_count: 1,
                    commit_version,
                    input_digest: [first_sequence as u8; SHA256_BYTES],
                    resulting_row_count: first_sequence + 1,
                    resulting_row_digest: resulting_digests[first_sequence as usize],
                }
                .encode()
                .unwrap(),
            );
        }
        store.execute(&command.seal()).unwrap();
        GenericIndexGcFixture {
            generation_id,
            reference_epoch,
            capability_digest,
            row_count,
            row_digest,
            rows,
        }
    }

    fn generic_index_gc_claim_request(
        store: &MetaShard,
        counter: &mut u128,
        fixture: &GenericIndexGcFixture,
    ) -> ClaimGenericIndexGenerationGcRequest {
        ClaimGenericIndexGenerationGcRequest {
            context: write_context(store, counter),
            generation_id: fixture.generation_id,
            reference_epoch: fixture.reference_epoch,
            capability_digest: fixture.capability_digest,
            row_count: fixture.row_count,
            row_digest: fixture.row_digest,
        }
    }

    fn claim_and_begin(
        service: GcService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        fixture: &Fixture,
    ) -> GcOperationRecord {
        let claimed = service
            .claim(ClaimGcRequest {
                context: write_context(store, counter),
                artifact_revision_id: fixture.target,
                reference_epoch: fixture.epoch,
            })
            .unwrap();
        service
            .begin_deletion(BeginGcDeletionRequest {
                context: write_context(store, counter),
                expected_operation: claimed.operation,
            })
            .unwrap()
            .operation
    }

    #[test]
    fn claim_replays_and_reference_race_has_one_winner() {
        let mut counter = 1;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, false, false);
        let service = GcService::new(&store);
        let first_context = write_context(&store, &mut counter);
        let racing_context = write_context(&store, &mut counter);
        let request = ClaimGcRequest {
            context: first_context,
            artifact_revision_id: fixture.target,
            reference_epoch: fixture.epoch,
        };
        let first = service.claim(request.clone()).unwrap();
        let replay = service.claim(request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);
        assert_eq!(replay.operation, first.operation);
        assert_eq!(first.operation.phase, GcPhase::Claimed);
        assert_eq!(
            first.operation.operation_id,
            gc_operation_id(root(), fixture.target, fixture.epoch)
        );

        let raced = service.claim(ClaimGcRequest {
            context: racing_context,
            artifact_revision_id: fixture.target,
            reference_epoch: fixture.epoch,
        });
        assert_eq!(raced, Err(GcError::ConcurrentMutation));
        let revision = ArtifactRevisionRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), fixture.target),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(revision.state, RevisionState::Deleting);
    }

    #[test]
    fn releasing_history_hold_blocks_claim() {
        let mut counter = 100;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, true, false);
        let service = GcService::new(&store);
        assert_eq!(
            service.safe_history_floor(read_context(&store)).unwrap(),
            ReadVersion::new(2).unwrap()
        );
        assert_eq!(
            service.claim(ClaimGcRequest {
                context: write_context(&store, &mut counter),
                artifact_revision_id: fixture.target,
                reference_epoch: fixture.epoch,
            }),
            Err(GcError::UnsafeHistoryFloor {
                last_zero: 2,
                floor: 2,
            })
        );
    }

    #[test]
    fn gc_exact_replay_rejects_a_corrupted_recovery_binding() {
        let mut counter = 1;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, false, false);
        let service = GcService::new(&store);
        let context = write_context(&store, &mut counter);
        let request = ClaimGcRequest {
            context,
            artifact_revision_id: fixture.target,
            reference_epoch: fixture.epoch,
        };
        service.claim(request.clone()).unwrap();
        let dedupe = store
            .lookup_request(root(), placement(), owner_epoch(), context.request_id)
            .unwrap()
            .unwrap();
        store
            .replace_recovery_header_for_test(
                dedupe.recovery_lsn,
                Some(b"tampered GC recovery header".to_vec()),
            )
            .unwrap();

        assert!(matches!(
            service.claim(request),
            Err(GcError::Meta(MetaError::CorruptRecord { .. }))
        ));
    }

    #[test]
    fn root_history_barrier_is_real_replayable_and_hold_fenced() {
        let mut counter = 150;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let service = GcService::new(&store);

        let first_context = write_context(&store, &mut counter);
        let before = first_context.read_version;
        let first = service.advance_history_barrier(first_context).unwrap();
        assert_eq!(first.barrier.generation, 1);
        assert_eq!(first.commit_version.get(), before.get() + 1);
        let replay = service.advance_history_barrier(first_context).unwrap();
        assert!(replay.replayed);
        assert_eq!(
            replay,
            GcHistoryBarrierOutcome {
                replayed: true,
                ..first
            }
        );

        let second = service
            .advance_history_barrier(write_context(&store, &mut counter))
            .unwrap();
        assert_eq!(second.barrier.generation, 2);
        assert!(second.commit_version > first.commit_version);

        seed_fixture(&store, &mut counter, true, false);
        assert_eq!(
            service.advance_history_barrier(write_context(&store, &mut counter)),
            Err(GcError::HistoryHoldActive)
        );
    }

    #[test]
    fn manifest_fault_is_atomic_and_complete_releases_dependencies() {
        let mut counter = 200;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, false, false);
        let service = GcService::new(&store);
        let deleting = claim_and_begin(service, &store, &mut counter, &fixture);
        let batch = service
            .scan_manifest_batch(
                write_context(&store, &mut counter),
                &deleting,
                MAX_GC_BATCH_ROWS,
            )
            .unwrap();
        assert!(batch.end_of_manifest);
        assert_eq!(batch.entries, fixture.rows);

        let mut bad_confirmations = fixture
            .rows
            .iter()
            .map(|entry| GcObjectAbsence {
                position: entry.position,
                object_key: entry.row.object_key.clone(),
                absence_digest: entry.delete_required.then_some([0x51; SHA256_BYTES]),
            })
            .collect::<Vec<_>>();
        bad_confirmations[2].object_key = "wrong-object-key".to_owned();
        assert!(matches!(
            service.advance_deletion_batch(AdvanceGcDeletionBatchRequest {
                context: write_context(&store, &mut counter),
                expected_operation: deleting.clone(),
                confirmations: bad_confirmations.clone(),
            }),
            Err(GcError::ManifestBatchMismatch { .. })
        ));
        let persisted = GcOperationRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Gc, deleting.operation_id),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(persisted, deleting);

        bad_confirmations[2].object_key = fixture.rows[2].row.object_key.clone();
        let advanced = service
            .advance_deletion_batch(AdvanceGcDeletionBatchRequest {
                context: write_context(&store, &mut counter),
                expected_operation: deleting,
                confirmations: bad_confirmations,
            })
            .unwrap()
            .operation;
        assert_eq!(advanced.scanned_manifest_row_count, 4);
        assert_eq!(advanced.deleted_object_count, 2);
        assert_eq!(advanced.manifest_rolling_digest, fixture.manifest_digest);

        let complete_request = CompleteGcRequest {
            context: write_context(&store, &mut counter),
            expected_operation: advanced,
        };
        let completed = service.complete(complete_request.clone()).unwrap();
        let replay = service.complete(complete_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(completed.operation.phase, GcPhase::Deleted);
        assert_eq!(replay.operation, completed.operation);

        let target = ArtifactRevisionRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), fixture.target),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(target.state, RevisionState::Deleted);
        let target_candidate = GcCandidateRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::GcCandidate,
                &gc_candidate_key(root(), fixture.target, fixture.epoch),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(target_candidate.claim_state, GcClaimState::Complete);
        assert!(read_payload(
            &store,
            MetadataFamily::RevisionRef,
            &revision_dependency_ref_key(root(), fixture.target, fixture.dependency_owner),
        )
        .is_none());
        let dependency_owner = ArtifactRevisionRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), fixture.dependency_owner),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(dependency_owner.strong_reference_count, 0);
        assert_eq!(dependency_owner.reference_epoch, ReferenceEpoch::new(2));
        assert_eq!(
            dependency_owner.last_zero_ref_version,
            Some(completed.commit_version)
        );
        assert!(read_payload(
            &store,
            MetadataFamily::GcCandidate,
            &gc_candidate_key(
                root(),
                fixture.dependency_owner,
                dependency_owner.reference_epoch,
            ),
        )
        .is_some());
        assert!(read_payload(
            &store,
            MetadataFamily::ArtifactManifest,
            &artifact_manifest_key(root(), fixture.target, 0),
        )
        .is_some());
    }

    #[test]
    fn append_child_then_base_gc_separates_manifest_positions_from_physical_indexes() {
        let mut counter = 260;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, false, false);
        let service = GcService::new(&store);

        let child_deleting = claim_and_begin(service, &store, &mut counter, &fixture);
        let child_batch = service
            .scan_manifest_batch(
                write_context(&store, &mut counter),
                &child_deleting,
                MAX_GC_BATCH_ROWS,
            )
            .unwrap();
        assert!(child_batch.end_of_manifest);
        assert_eq!(
            child_batch
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.position.object_index,
                        entry.row.physical_object_index,
                        entry.delete_required,
                    )
                })
                .collect::<Vec<_>>(),
            vec![(0, 0, false), (1, 1, false), (2, 0, true), (3, 1, true)],
        );
        let child_confirmations = child_batch
            .entries
            .iter()
            .map(|entry| GcObjectAbsence {
                position: entry.position,
                object_key: entry.row.object_key.clone(),
                absence_digest: entry.delete_required.then_some([0x61; SHA256_BYTES]),
            })
            .collect::<Vec<_>>();
        let child_advanced = service
            .advance_deletion_batch(AdvanceGcDeletionBatchRequest {
                context: write_context(&store, &mut counter),
                expected_operation: child_deleting,
                confirmations: child_confirmations,
            })
            .unwrap()
            .operation;
        assert_eq!(child_advanced.deleted_object_count, 2);
        let child_completed = service
            .complete(CompleteGcRequest {
                context: write_context(&store, &mut counter),
                expected_operation: child_advanced,
            })
            .unwrap();

        let base_record = ArtifactRevisionRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), fixture.dependency_owner),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(base_record.strong_reference_count, 0);
        assert_eq!(base_record.reference_epoch, ReferenceEpoch::new(2));
        assert_eq!(
            base_record.last_zero_ref_version,
            Some(child_completed.commit_version)
        );
        assert!(fixture.rows[..2].iter().all(|entry| {
            read_payload(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_key(
                    root(),
                    fixture.dependency_owner,
                    entry.position.object_index,
                ),
            )
            .is_some()
        }));

        service
            .advance_history_barrier(write_context(&store, &mut counter))
            .unwrap();
        let base_claimed = service
            .claim(ClaimGcRequest {
                context: write_context(&store, &mut counter),
                artifact_revision_id: fixture.dependency_owner,
                reference_epoch: base_record.reference_epoch,
            })
            .unwrap()
            .operation;
        let base_deleting = service
            .begin_deletion(BeginGcDeletionRequest {
                context: write_context(&store, &mut counter),
                expected_operation: base_claimed,
            })
            .unwrap()
            .operation;
        let base_batch = service
            .scan_manifest_batch(
                write_context(&store, &mut counter),
                &base_deleting,
                MAX_GC_BATCH_ROWS,
            )
            .unwrap();
        assert_eq!(
            base_batch
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.position.object_index,
                        entry.row.physical_object_index,
                        entry.delete_required,
                    )
                })
                .collect::<Vec<_>>(),
            vec![(0, 0, true), (1, 1, true)],
        );
        let base_confirmations = base_batch
            .entries
            .iter()
            .map(|entry| GcObjectAbsence {
                position: entry.position,
                object_key: entry.row.object_key.clone(),
                absence_digest: Some([0x62; SHA256_BYTES]),
            })
            .collect();
        let base_advanced = service
            .advance_deletion_batch(AdvanceGcDeletionBatchRequest {
                context: write_context(&store, &mut counter),
                expected_operation: base_deleting,
                confirmations: base_confirmations,
            })
            .unwrap()
            .operation;
        assert_eq!(base_advanced.deleted_object_count, 2);
        let base_completed = service
            .complete(CompleteGcRequest {
                context: write_context(&store, &mut counter),
                expected_operation: base_advanced,
            })
            .unwrap();
        assert_eq!(base_completed.operation.phase, GcPhase::Deleted);
    }

    #[test]
    fn stale_candidate_cleanup_is_safe_and_replayable() {
        let mut counter = 300;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, false, true);
        let service = GcService::new(&store);
        let stale_request = ClearStaleGcCandidateRequest {
            context: write_context(&store, &mut counter),
            artifact_revision_id: fixture.target,
            reference_epoch: ReferenceEpoch::new(1),
        };
        let cleared = service
            .clear_stale_candidate(stale_request.clone())
            .unwrap();
        let replay = service.clear_stale_candidate(stale_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, cleared.commit_version);
        assert!(read_payload(
            &store,
            MetadataFamily::GcCandidate,
            &gc_candidate_key(root(), fixture.target, ReferenceEpoch::new(1)),
        )
        .is_none());
        assert_eq!(
            service.clear_stale_candidate(ClearStaleGcCandidateRequest {
                context: write_context(&store, &mut counter),
                artifact_revision_id: fixture.target,
                reference_epoch: fixture.epoch,
            }),
            Err(GcError::CandidateNotStale)
        );
    }

    #[test]
    fn candidate_pages_preserve_epoch_key_order() {
        let mut counter = 400;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        seed_fixture(&store, &mut counter, false, true);
        let service = GcService::new(&store);
        let first = service
            .list_candidates(read_context(&store), None, 1)
            .unwrap();
        assert_eq!(first.entries.len(), 1);
        assert!(first.next_cursor.is_some());
        let second = service
            .list_candidates(read_context(&store), first.next_cursor, 1)
            .unwrap();
        assert_eq!(second.entries.len(), 1);
        assert!(second.next_cursor.is_none());
        assert!(first.entries[0].cursor < second.entries[0].cursor);
    }

    #[test]
    fn quarantine_and_replay_survive_file_reopen() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("gc-metadata");
        let mut counter = 500;
        let store = crate::workspace::test_support::initialize_file(&database, shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_fixture(&store, &mut counter, false, false);
        let service = GcService::new(&store);
        let deleting = claim_and_begin(service, &store, &mut counter, &fixture);
        let quarantine_request = QuarantineGcRequest {
            context: write_context(&store, &mut counter),
            expected_operation: deleting,
            evidence: b"provider delete outcome is ambiguous".to_vec(),
        };
        let quarantined = service.quarantine(quarantine_request.clone()).unwrap();
        assert_eq!(quarantined.operation.phase, GcPhase::Quarantined);
        drop(store);

        let reopened = crate::workspace::test_support::open_file(&database, shard()).unwrap();
        let service = GcService::new(&reopened);
        let replay = service.quarantine(quarantine_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation, quarantined.operation);
        let revision = ArtifactRevisionRecord::decode(
            &read_payload(
                &reopened,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), fixture.target),
            )
            .unwrap(),
        )
        .unwrap();
        let candidate = GcCandidateRecord::decode(
            &read_payload(
                &reopened,
                MetadataFamily::GcCandidate,
                &gc_candidate_key(root(), fixture.target, fixture.epoch),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(revision.state, RevisionState::Quarantined);
        assert_eq!(candidate.claim_state, GcClaimState::Quarantined);
        assert_eq!(
            candidate.quarantine_evidence,
            quarantined.operation.quarantine_evidence
        );
    }

    #[test]
    fn generic_index_gc_requires_floor_strictly_newer_than_last_zero() {
        let mut counter = 700;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_generic_index_gc_fixture(&store, &mut counter, 0, 0);
        let service = GcService::new(&store);

        let generation = GenericIndexGenerationRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::GenericIndexGeneration,
                &generic_index_generation_key(root(), fixture.generation_id),
            )
            .unwrap(),
        )
        .unwrap();
        let last_zero = generation.last_zero_reference_version.unwrap();
        assert_eq!(read_context(&store).read_version.get(), last_zero.get());
        assert!(service
            .list_generic_index_generation_candidates(read_context(&store), None, 8)
            .unwrap()
            .entries
            .is_empty());
        assert_eq!(
            service.claim_generic_index_generation(generic_index_gc_claim_request(
                &store,
                &mut counter,
                &fixture,
            )),
            Err(GcError::UnsafeHistoryFloor {
                last_zero: last_zero.get(),
                floor: last_zero.get(),
            })
        );

        service
            .advance_history_barrier(write_context(&store, &mut counter))
            .unwrap();
        let candidates = service
            .list_generic_index_generation_candidates(read_context(&store), None, 8)
            .unwrap();
        assert_eq!(candidates.entries.len(), 1);
        assert_eq!(
            candidates.entries[0].cursor.generation_id,
            fixture.generation_id
        );
        assert_eq!(
            candidates.entries[0].reference_epoch,
            fixture.reference_epoch
        );
        assert_eq!(candidates.entries[0].row_digest, fixture.row_digest);
        let claimed = service
            .claim_generic_index_generation(generic_index_gc_claim_request(
                &store,
                &mut counter,
                &fixture,
            ))
            .unwrap();
        assert_eq!(claimed.operation.phase, GenericIndexGcPhase::Retiring);
    }

    #[test]
    fn generic_index_gc_validates_and_collects_rows_then_receipts_in_batches() {
        let mut counter = 800;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_generic_index_gc_fixture(&store, &mut counter, 5, 5);
        let service = GcService::new(&store);
        service
            .advance_history_barrier(write_context(&store, &mut counter))
            .unwrap();
        let mut operation = service
            .claim_generic_index_generation(generic_index_gc_claim_request(
                &store,
                &mut counter,
                &fixture,
            ))
            .unwrap()
            .operation;

        let first_batch = CollectGenericIndexGenerationGcBatchRequest {
            context: write_context(&store, &mut counter),
            expected_operation: operation,
            batch_size: 2,
        };
        let first = service
            .collect_generic_index_generation_batch(first_batch.clone())
            .unwrap();
        let replay = service
            .collect_generic_index_generation_batch(first_batch)
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);
        assert_eq!(replay.operation, first.operation);
        operation = first.operation;

        let mut calls = 1;
        while operation.phase != GenericIndexGcPhase::Retired {
            operation = service
                .collect_generic_index_generation_batch(
                    CollectGenericIndexGenerationGcBatchRequest {
                        context: write_context(&store, &mut counter),
                        expected_operation: operation,
                        batch_size: 2,
                    },
                )
                .unwrap()
                .operation;
            calls += 1;
        }
        assert_eq!(calls, 6);
        assert_eq!(operation.scanned_row_count, 5);
        assert_eq!(operation.deleted_receipt_count, 5);
        assert!(operation.rows_complete);
        assert!(operation.receipts_complete);
        assert!(store
            .scan_prefix_at(
                root(),
                placement(),
                owner_epoch(),
                MetadataFamily::GenericIndexGeneration,
                &generic_index_row_prefix(root(), fixture.generation_id),
                read_context(&store).read_version,
                None,
                1,
            )
            .unwrap()
            .is_empty());
        assert!(store
            .scan_prefix_at(
                root(),
                placement(),
                owner_epoch(),
                MetadataFamily::GenericIndexGeneration,
                &generic_index_append_receipt_prefix(root(), fixture.generation_id),
                read_context(&store).read_version,
                None,
                1,
            )
            .unwrap()
            .is_empty());
        assert!(store
            .scan_prefix_at(
                root(),
                placement(),
                owner_epoch(),
                MetadataFamily::GenericIndexGeneration,
                &generic_index_generation_ref_prefix(root(), fixture.generation_id),
                read_context(&store).read_version,
                None,
                1,
            )
            .unwrap()
            .is_empty());
        let tombstone = GenericIndexGenerationRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::GenericIndexGeneration,
                &generic_index_generation_key(root(), fixture.generation_id),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(tombstone.state, GenericIndexGenerationState::Retired);
        assert_eq!(tombstone.reference_epoch, fixture.reference_epoch);
        assert_eq!(tombstone.rolling_row_digest, fixture.row_digest);
    }

    #[test]
    fn generic_index_gc_stale_e2_claim_fails_after_reference_aba_to_e4() {
        let mut counter = 900;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_generic_index_gc_fixture(&store, &mut counter, 0, 0);
        let service = GcService::new(&store);
        service
            .advance_history_barrier(write_context(&store, &mut counter))
            .unwrap();

        let claim_request = generic_index_gc_claim_request(&store, &mut counter, &fixture);
        let mut stale_plan =
            plan_generic_index_generation_gc_claim(&store, &claim_request).unwrap();
        let generation_key = generic_index_generation_key(root(), fixture.generation_id);
        let reference_key = generic_index_generation_ref_key(
            root(),
            fixture.generation_id,
            GenericIndexReferenceKind::Current,
            [0x44; SHA256_BYTES],
        );
        let e2_payload = read_payload(
            &store,
            MetadataFamily::GenericIndexGeneration,
            &generation_key,
        )
        .unwrap();
        let mut e3 = GenericIndexGenerationRecord::decode(&e2_payload).unwrap();
        e3.reference_epoch = ReferenceEpoch::new(3);
        e3.reference_count = 1;
        e3.last_zero_reference_version = None;
        let context = write_context(&store, &mut counter);
        let mut add = base_command(context, Vec::new());
        replace_exact(
            &mut add,
            MetadataFamily::GenericIndexGeneration,
            generation_key.clone(),
            e2_payload,
            e3.encode().unwrap(),
        );
        put_absent(
            &mut add,
            MetadataFamily::GenericIndexGeneration,
            reference_key.clone(),
            GenericIndexGenerationRefRecord {
                kind: GenericIndexReferenceKind::Current,
                owner_digest: [0x44; SHA256_BYTES],
                reference_epoch_at_add: ReferenceEpoch::new(3),
            }
            .encode()
            .unwrap(),
        );
        store.execute(&add.seal()).unwrap();

        let e3_payload = read_payload(
            &store,
            MetadataFamily::GenericIndexGeneration,
            &generation_key,
        )
        .unwrap();
        let reference_payload = read_payload(
            &store,
            MetadataFamily::GenericIndexGeneration,
            &reference_key,
        )
        .unwrap();
        let context = write_context(&store, &mut counter);
        let mut e4 = GenericIndexGenerationRecord::decode(&e3_payload).unwrap();
        e4.reference_epoch = ReferenceEpoch::new(4);
        e4.reference_count = 0;
        e4.last_zero_reference_version =
            Some(CommitVersion::new(context.read_version.get().checked_add(1).unwrap()).unwrap());
        let mut remove = base_command(context, Vec::new());
        replace_exact(
            &mut remove,
            MetadataFamily::GenericIndexGeneration,
            generation_key.clone(),
            e3_payload,
            e4.encode().unwrap(),
        );
        delete_exact(
            &mut remove,
            MetadataFamily::GenericIndexGeneration,
            reference_key,
            reference_payload,
        );
        store.execute(&remove.seal()).unwrap();

        let fresh = write_context(&store, &mut counter);
        stale_plan.read_version = fresh.read_version;
        stale_plan.request_id = fresh.request_id;
        stale_plan.command_digest = CommandDigest::from_bytes([0; SHA256_BYTES]);
        assert_eq!(
            store.execute(&stale_plan.seal()),
            Err(MetaError::PredicateFailed)
        );
        let persisted = GenericIndexGenerationRecord::decode(
            &read_payload(
                &store,
                MetadataFamily::GenericIndexGeneration,
                &generation_key,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(persisted.reference_epoch, ReferenceEpoch::new(4));
        assert_eq!(persisted.state, GenericIndexGenerationState::Sealed);
    }

    #[test]
    fn generic_index_gc_fails_closed_on_seal_and_payload_corruption() {
        let mut counter = 1_000;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        initialize_store(&store, &mut counter);
        let fixture = seed_generic_index_gc_fixture(&store, &mut counter, 2, 2);
        let service = GcService::new(&store);
        service
            .advance_history_barrier(write_context(&store, &mut counter))
            .unwrap();

        let mut wrong_seal = generic_index_gc_claim_request(&store, &mut counter, &fixture);
        wrong_seal.row_digest = [0x55; SHA256_BYTES];
        assert!(matches!(
            service.claim_generic_index_generation(wrong_seal),
            Err(GcError::GenericIndexGenerationSealMismatch { .. })
        ));

        let corrupt_key = generic_index_row_key(root(), fixture.generation_id, 1);
        let corrupt_payload =
            read_payload(&store, MetadataFamily::GenericIndexGeneration, &corrupt_key).unwrap();
        let mut corrupt_row = fixture.rows[1].clone();
        corrupt_row.relative_path = Some(NormalizedRelativePath::new("changed").unwrap());
        let context = write_context(&store, &mut counter);
        let mut corrupt = base_command(context, Vec::new());
        replace_exact(
            &mut corrupt,
            MetadataFamily::GenericIndexGeneration,
            corrupt_key.clone(),
            corrupt_payload,
            corrupt_row.encode().unwrap(),
        );
        store.execute(&corrupt.seal()).unwrap();

        let claimed = service
            .claim_generic_index_generation(generic_index_gc_claim_request(
                &store,
                &mut counter,
                &fixture,
            ))
            .unwrap();
        assert!(matches!(
            service.collect_generic_index_generation_batch(
                CollectGenericIndexGenerationGcBatchRequest {
                    context: write_context(&store, &mut counter),
                    expected_operation: claimed.operation,
                    batch_size: 2,
                }
            ),
            Err(GcError::GenericIndexPayloadClosureMismatch { .. })
        ));
        assert!(read_payload(
            &store,
            MetadataFamily::GenericIndexGeneration,
            &generic_index_row_key(root(), fixture.generation_id, 0),
        )
        .is_some());
        assert!(read_payload(
            &store,
            MetadataFamily::GenericIndexGeneration,
            &generic_index_row_key(root(), fixture.generation_id, 1),
        )
        .is_some());
    }
}
