/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable object-first publication commands for NoKV workspaces.
//!
//! This module composes typed publication records into the sole
//! [`MetadataCommand`] executor and never writes a transaction store directly.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use nokv_types::{
    ArtifactRevisionId, BuildCommitPhase, CommandDigest, CommitVersion, GcClaimState, Generation,
    LogicalShardId, ObjectNamespaceId, OperationId, OperationKind, OwnerEpoch, PlacementGeneration,
    PublishPhase, ReadVersion, ReferenceEpoch, RequestId, RestorePhase, RevisionState, RootId,
    StagedCleanupState, StagedProviderState, WorkspaceRevision, WorkspaceState, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::build_commit_records::{
    BuildCommitOperationRecord, CommitManifestBinding, CommitManifestCondition,
    CommitOperationRecordError,
};
use super::codec::{
    artifact_manifest_key, artifact_manifest_prefix, artifact_revision_claim_key,
    artifact_revision_key, commit_revision_ref_key, gc_candidate_key, object_block_key,
    operation_key, path_current_key, path_revision_ref_key, revision_dependency_ref_key,
    staged_object_key, staged_object_prefix, workspace_current_key, SCHEMA_ID,
};
use super::commit::RUN_MANIFEST_PATH;
use super::engine::{
    CommandMutation, CommandPredicate, EventProjection, HistoryProjection, MetaError, MetaShard,
    MetadataCommand, MetadataCommandResult, RootFenceAction,
};
use super::event_projection::change_event_projection;
use super::keyspace::MetadataFamily;
use super::publication_records::{
    ArtifactRevisionClaimRecord, ArtifactRevisionRecord, GcCandidateRecord, PathEntry,
    PublicationRecordCodecError, RevisionRefRecord, WorkspaceRecord,
};
use super::publish_operation_records::{
    ArtifactManifestRow, ManifestPosition, PublishAuthority, PublishClaim, PublishOperationRecord,
    PublishRecordError, PublishResult, PublishTerminalError, PublishTerminalErrorKind,
    PublishTransition, StagedObjectRecord, MAX_DEPENDENCY_COUNT, MAX_MANIFEST_ROWS,
    MAX_STAGED_OBJECTS,
};
use super::query_records::{
    secondary_index_key, ChangeEventKind, ChangeEventRecord, QueryRecordError,
    SecondaryIndexRecord, TypedProjection,
};
use super::restore::RESTORE_MANIFEST_PATH;
use super::restore_records::{
    RestoreCommitProvenance, RestoreDestinationBinding, RestoreManifestIdentity,
    RestoreManifestPublication, RestoreOperationRecord, RestoreRecordError,
    RESTORE_MANIFEST_CONTENT_TYPE,
};

const MAX_COMMAND_ITEMS: usize = 256;
/// Maximum rows admitted by one recoverable publication batch.
pub const MAX_PUBLICATION_BATCH_ROWS: usize = 192;
const _: () = assert!(MAX_PUBLICATION_BATCH_ROWS + 2 <= MAX_COMMAND_ITEMS);
const _: () = assert!(
    2 * super::query_records::MAX_TYPED_PROJECTION_FIELDS + 2 * MAX_DEPENDENCY_COUNT as usize + 8
        <= MAX_COMMAND_ITEMS
);

/// Root-owner and request fence shared by one publication command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationContext {
    pub root_id: RootId,
    pub logical_shard_id: LogicalShardId,
    pub object_namespace_id: ObjectNamespaceId,
    pub placement_generation: PlacementGeneration,
    pub owner_epoch: OwnerEpoch,
    pub request_id: RequestId,
    /// Exact current metadata clock observed before command construction.
    pub read_version: ReadVersion,
}

/// Atomic creation of one empty publish operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginPublishRequest {
    pub context: PublicationContext,
    pub operation: PublishOperationRecord,
}

/// One exact staged-ledger state change or provider-cleanup proof pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedObjectUpdate {
    pub expected: StagedObjectRecord,
    pub next: StagedObjectRecord,
}

/// Bounded append to the durable staged-object ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageObjectsBatchRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub staged_objects: Vec<StagedObjectRecord>,
}

/// Bounded contiguous confirmation of uploaded staged objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkObjectsUploadedBatchRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub staged_object_updates: Vec<StagedObjectUpdate>,
}

/// CAS transition of one exact observed publish operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransitionPublishRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub transition: PublishTransition,
}

/// Initiating-owner, extend-only activity heartbeat for one exact nonterminal
/// publish operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatPublishRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub activity_deadline_ms: u64,
}

/// Fenced takeover of one exact nonterminal publish operation.
///
/// A newer owner may abort immediately; the initiating owner must present an
/// observed clock beyond the durable activity deadline plus skew grace. For
/// `Finalizing`, this service derives and persists the publication-absence
/// proof from the exact operation payload that the same metadata command
/// CASes, so callers cannot manufacture that proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TakeOverOrphanedPublishRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub observed_now_ms: u64,
    pub maximum_clock_skew_ms: u64,
    pub terminal_error: PublishTerminalError,
}

/// One ordered immutable row and its authoritative manifest-key coordinates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRowInput {
    pub object_index: u64,
    pub row: ArtifactManifestRow,
}

/// Bounded append to the invisible immutable manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageManifestBatchRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub manifest_rows: Vec<ManifestRowInput>,
    /// Strictly increasing, distinct physical owner revision ids.
    pub dependency_owner_revision_ids: Vec<ArtifactRevisionId>,
}

/// Bounded cleanup of aborted publication-owned staging rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupPublishBatchRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    /// Contiguous rows starting at `cleanup_staged_object_cursor`. Each next
    /// state is durable proof that provider cleanup completed.
    pub staged_object_updates: Vec<StagedObjectUpdate>,
}

/// Operator verdict about the provider-side state of one quarantined publish.
///
/// The operator verifies at the provider before calling; the metadata command
/// enforces the machine-checkable half of each verdict atomically against the
/// authoritative `ArtifactRevision` row so a wrong verdict fails loudly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineReconcileResolution {
    /// Every staged provider key was verified absent and the artifact revision
    /// was never published. Reconciliation releases the revision identity for
    /// a fresh begin.
    RevisionUnpublished,
    /// The artifact revision is already published, so the staged provider
    /// keys are the published revision's live objects and must not be touched.
    /// Only this operation's private bookkeeping rows are removed.
    RevisionPublished,
}

/// Bounded removal of one quarantined operation's durable staging rows.
///
/// The operation stays `Quarantined` while batches run, so an abandoned
/// reconciliation keeps the fail-closed default and remains resumable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileQuarantinedPublishBatchRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub resolution: QuarantineReconcileResolution,
    /// Exact durable staged rows starting at `cleanup_staged_object_cursor`.
    /// Empty once the staged cursor is sealed and manifest pages remain.
    pub staged_object_rows: Vec<StagedObjectRecord>,
}

/// Atomic operator resolution of one fully-swept quarantined operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinishReconcileQuarantinedPublishRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub resolution: QuarantineReconcileResolution,
    /// Operator-supplied audit reason retained in the terminal error.
    pub reason: String,
    /// Digest of the operator's provider verification transcript.
    pub operator_evidence_digest: [u8; SHA256_BYTES],
}

/// Caller-visible metadata projected into both the revision and path records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedArtifact {
    pub logical_size: u64,
    pub body_digest_uri: String,
    pub manifest_digest_uri: String,
    pub content_type: String,
    pub producer: Option<String>,
    pub manifest_id: Option<String>,
    pub typed_index_projection: Vec<u8>,
}

/// Atomic metadata-last publication of one fully-uploaded revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizePublishRequest {
    pub context: PublicationContext,
    pub expected_operation: PublishOperationRecord,
    pub artifact: PublishedArtifact,
    /// Strictly increasing, distinct physical owner revision ids.
    pub dependency_owner_revision_ids: Vec<ArtifactRevisionId>,
}

/// Decoded durable result of begin or transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishCommandOutcome {
    pub commit_version: CommitVersion,
    pub operation: PublishOperationRecord,
    pub replayed: bool,
}

/// Decoded durable result of final metadata publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizePublishOutcome {
    pub commit_version: CommitVersion,
    pub operation: PublishOperationRecord,
    pub result: PublishResult,
    pub replayed: bool,
}

/// Typed publication-domain failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationError {
    Meta(MetaError),
    OperationCodec(PublishRecordError),
    RecordCodec(PublicationRecordCodecError),
    QueryRecord(QueryRecordError),
    CommitOperationCodec(CommitOperationRecordError),
    RestoreCodec(RestoreRecordError),
    InvalidOperationSeal {
        seal: &'static str,
    },
    InvalidOperationPhase {
        expected: PublishPhase,
        actual: PublishPhase,
    },
    /// The durable operation row no longer matches the caller's observed
    /// predecessor. Re-observe it before diagnosing contextual authority.
    ConcurrentMutation,
    InitiatingOwnerEpochMismatch {
        expected: OwnerEpoch,
        actual: OwnerEpoch,
    },
    OwnerTakeoverNotAllowed {
        initiating: OwnerEpoch,
        current: OwnerEpoch,
    },
    HeartbeatOwnerMismatch {
        initiating: OwnerEpoch,
        current: OwnerEpoch,
    },
    ActivityDeadlineNotExtended {
        current: u64,
        requested: u64,
    },
    ActivityDeadlineNotFuture {
        clock: u64,
        requested: u64,
    },
    ActivityLeaseNotExpired {
        deadline_ms: u64,
        lease_clock_ms: u64,
        maximum_clock_skew_ms: u64,
    },
    HeartbeatPhase {
        actual: PublishPhase,
    },
    OwnerTakeoverPhase {
        actual: PublishPhase,
    },
    EmptyBatch {
        batch: &'static str,
    },
    BatchTooLarge {
        batch: &'static str,
        count: usize,
        max: usize,
    },
    BatchExceedsPlannedCount {
        batch: &'static str,
        cursor: u32,
        count: usize,
        planned: u32,
    },
    StagedObjectCountMismatch {
        expected: u32,
        actual: usize,
    },
    StagedObjectSequenceMismatch {
        expected: u32,
        actual: u32,
    },
    StagedObjectRevisionMismatch,
    StagedObjectKeyMismatch {
        sequence: u32,
    },
    InvalidStagedObjectTransition {
        sequence: u32,
    },
    StagedObjectNotUploaded {
        sequence: u32,
    },
    ManifestCountMismatch {
        expected: u32,
        actual: usize,
    },
    ManifestOrder,
    ManifestPositionMismatch,
    ManifestLogicalOffsetMismatch {
        expected: u64,
        actual: u64,
    },
    ManifestDigestMismatch,
    ManifestPhysicalOwnerMissing {
        owner: ArtifactRevisionId,
    },
    ManifestOwnershipMismatch {
        object_index: u64,
        reason: &'static str,
    },
    DuplicateStagedObjectKey {
        sequence: u32,
    },
    DependencyCountMismatch {
        expected: u8,
        actual: usize,
    },
    DependencyOrder,
    DependencyDigestMismatch,
    DependencyDepthMismatch {
        expected: u8,
        actual: u8,
    },
    WorkspaceNotFound,
    WorkspaceUnavailable,
    WorkspaceIncarnationMismatch,
    ReservedManifestAuthorityRequired,
    CommitOperationMissing,
    CommitAuthorityMismatch,
    CommitManifestClosureMismatch,
    RestoreOperationMissing,
    RestoreAuthorityMismatch,
    RestoreManifestPathRequired,
    RestoreManifestClosureMismatch,
    WorkspaceRevisionOverflow,
    PathAlreadyExists,
    PathNotFound,
    PathGenerationMismatch {
        expected: u64,
        actual: u64,
    },
    AppendBaseRevisionMismatch,
    PathGenerationOverflow,
    RevisionAlreadyExists,
    RevisionClaimHeld {
        revision: ArtifactRevisionId,
        operation_id: OperationId,
    },
    ReconcileResolutionMismatch {
        resolution: QuarantineReconcileResolution,
        revision_published: bool,
    },
    ReconcileManifestRowsRemain {
        remaining: u32,
    },
    RevisionNotFound {
        revision: ArtifactRevisionId,
    },
    RevisionUnavailable {
        revision: ArtifactRevisionId,
    },
    RevisionIdentityCollision,
    RevisionReferenceMissing,
    RevisionReferenceEpochAhead,
    ReferenceCountUnderflow {
        revision: ArtifactRevisionId,
    },
    ReferenceCountOverflow {
        revision: ArtifactRevisionId,
    },
    ReferenceEpochOverflow {
        revision: ArtifactRevisionId,
    },
    CommitVersionOverflow,
    LengthOverflow {
        field: &'static str,
        length: usize,
    },
    CommandItemLimit {
        field: &'static str,
        count: usize,
        max: usize,
    },
    DuplicateCommandKey {
        family: MetadataFamily,
    },
    OperationInputMismatch,
    ReplayResultMismatch,
    MetadataLastFinalizationRequired,
}

impl fmt::Display for PublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta(error) => error.fmt(formatter),
            Self::OperationCodec(error) => error.fmt(formatter),
            Self::RecordCodec(error) => error.fmt(formatter),
            Self::QueryRecord(error) => error.fmt(formatter),
            Self::CommitOperationCodec(error) => error.fmt(formatter),
            Self::RestoreCodec(error) => error.fmt(formatter),
            Self::InvalidOperationSeal { seal } => {
                write!(formatter, "publish operation {seal} seal mismatch")
            }
            Self::InvalidOperationPhase { expected, actual } => write!(
                formatter,
                "publish operation phase must be {expected:?}, found {actual:?}"
            ),
            Self::ConcurrentMutation => {
                formatter.write_str("publication state changed concurrently")
            }
            Self::InitiatingOwnerEpochMismatch { expected, actual } => write!(
                formatter,
                "publish initiating owner epoch must be {}, found {}",
                expected.get(),
                actual.get()
            ),
            Self::OwnerTakeoverNotAllowed {
                initiating,
                current,
            } => write!(
                formatter,
                "publish owner takeover cannot use epoch {} older than initiating epoch {}",
                current.get(),
                initiating.get(),
            ),
            Self::HeartbeatOwnerMismatch {
                initiating,
                current,
            } => write!(
                formatter,
                "publish heartbeat requires initiating owner epoch {}, found {}",
                initiating.get(),
                current.get()
            ),
            Self::ActivityDeadlineNotExtended { current, requested } => write!(
                formatter,
                "publish activity deadline {requested} does not extend current deadline {current}"
            ),
            Self::ActivityDeadlineNotFuture { clock, requested } => write!(
                formatter,
                "publish activity deadline {requested} is not newer than lease clock {clock}"
            ),
            Self::ActivityLeaseNotExpired {
                deadline_ms,
                lease_clock_ms,
                maximum_clock_skew_ms,
            } => write!(
                formatter,
                "publish activity deadline {deadline_ms} plus clock-skew grace {maximum_clock_skew_ms} is newer than lease clock {lease_clock_ms}"
            ),
            Self::HeartbeatPhase { actual } => write!(
                formatter,
                "publish phase {actual:?} is not eligible for activity heartbeat"
            ),
            Self::OwnerTakeoverPhase { actual } => write!(
                formatter,
                "publish phase {actual:?} is not eligible for owner takeover"
            ),
            Self::EmptyBatch { batch } => write!(formatter, "{batch} batch must not be empty"),
            Self::BatchTooLarge { batch, count, max } => {
                write!(
                    formatter,
                    "{batch} batch has {count} rows, maximum is {max}"
                )
            }
            Self::BatchExceedsPlannedCount {
                batch,
                cursor,
                count,
                planned,
            } => write!(
                formatter,
                "{batch} batch at cursor {cursor} with {count} rows exceeds planned count {planned}"
            ),
            Self::StagedObjectCountMismatch { expected, actual } => write!(
                formatter,
                "staged-object count mismatch: expected {expected}, found {actual}"
            ),
            Self::StagedObjectSequenceMismatch { expected, actual } => write!(
                formatter,
                "staged-object sequence mismatch: expected {expected}, found {actual}"
            ),
            Self::StagedObjectRevisionMismatch => {
                formatter.write_str("staged object belongs to a different artifact revision")
            }
            Self::StagedObjectKeyMismatch { sequence } => write!(
                formatter,
                "staged object {sequence} does not use its canonical shard/root/revision key"
            ),
            Self::InvalidStagedObjectTransition { sequence } => {
                write!(
                    formatter,
                    "invalid staged-object transition at sequence {sequence}"
                )
            }
            Self::StagedObjectNotUploaded { sequence } => {
                write!(
                    formatter,
                    "staged object {sequence} is not uploaded and owned"
                )
            }
            Self::ManifestCountMismatch { expected, actual } => write!(
                formatter,
                "manifest-row count mismatch: expected {expected}, found {actual}"
            ),
            Self::ManifestOrder => {
                formatter.write_str("manifest rows must be strictly ordered by object index")
            }
            Self::ManifestPositionMismatch => formatter
                .write_str("manifest batch does not continue after the durable manifest position"),
            Self::ManifestLogicalOffsetMismatch { expected, actual } => write!(
                formatter,
                "manifest logical offset mismatch: expected {expected}, found {actual}"
            ),
            Self::ManifestDigestMismatch => {
                formatter.write_str("manifest-row digest does not match operation seal")
            }
            Self::ManifestPhysicalOwnerMissing { owner } => write!(
                formatter,
                "manifest physical owner {:02x?} is not the new revision or a sealed dependency",
                owner.as_bytes()
            ),
            Self::ManifestOwnershipMismatch {
                object_index,
                reason,
            } => write!(
                formatter,
                "manifest row {object_index} does not match its physical object ownership: {reason}"
            ),
            Self::DuplicateStagedObjectKey { sequence } => {
                write!(
                    formatter,
                    "staged object {sequence} repeats an object key in its batch"
                )
            }
            Self::DependencyCountMismatch { expected, actual } => write!(
                formatter,
                "dependency count mismatch: expected {expected}, found {actual}"
            ),
            Self::DependencyOrder => {
                formatter.write_str("dependency owner revisions must be strictly increasing")
            }
            Self::DependencyDigestMismatch => {
                formatter.write_str("dependency digest does not match operation seal")
            }
            Self::DependencyDepthMismatch { expected, actual } => write!(
                formatter,
                "dependency depth mismatch: expected {expected}, derived {actual}"
            ),
            Self::WorkspaceNotFound => formatter.write_str("workspace marker not found"),
            Self::WorkspaceUnavailable => formatter.write_str("workspace is not visible"),
            Self::WorkspaceIncarnationMismatch => {
                formatter.write_str("workspace marker names a different incarnation")
            }
            Self::ReservedManifestAuthorityRequired => formatter.write_str(
                "canonical Workbench manifests require their lifecycle-owned staging authority",
            ),
            Self::CommitOperationMissing => {
                formatter.write_str("owning commit operation is missing")
            }
            Self::CommitAuthorityMismatch => {
                formatter.write_str("publication does not match its commit staging authority")
            }
            Self::CommitManifestClosureMismatch => formatter.write_str(
                "run manifest artifact does not match the commit-owned staging closure",
            ),
            Self::RestoreOperationMissing => {
                formatter.write_str("owning restore operation is missing")
            }
            Self::RestoreAuthorityMismatch => {
                formatter.write_str("publication does not match the hidden restore authority")
            }
            Self::RestoreManifestPathRequired => write!(
                formatter,
                "restore-staging publication must create {RUN_MANIFEST_PATH} or {RESTORE_MANIFEST_PATH}"
            ),
            Self::RestoreManifestClosureMismatch => formatter.write_str(
                "destination manifest artifact does not match the sealed restore authority",
            ),
            Self::WorkspaceRevisionOverflow => formatter.write_str("workspace revision overflow"),
            Self::PathAlreadyExists => formatter.write_str("path already exists"),
            Self::PathNotFound => formatter.write_str("path does not exist"),
            Self::PathGenerationMismatch { expected, actual } => {
                write!(
                    formatter,
                    "path generation mismatch: expected {expected}, found {actual}"
                )
            }
            Self::AppendBaseRevisionMismatch => {
                formatter.write_str("append base revision does not match current path")
            }
            Self::PathGenerationOverflow => formatter.write_str("path generation overflow"),
            Self::RevisionAlreadyExists => {
                formatter.write_str("artifact revision identity already exists")
            }
            Self::RevisionClaimHeld {
                revision,
                operation_id,
            } => write!(
                formatter,
                "artifact revision {:02x?} is claimed by in-flight publish operation {:02x?}",
                revision.as_bytes(),
                operation_id.as_bytes()
            ),
            Self::ReconcileResolutionMismatch {
                resolution,
                revision_published,
            } => write!(
                formatter,
                "operator resolution {resolution:?} contradicts the authoritative revision state \
                 (published: {revision_published})"
            ),
            Self::ReconcileManifestRowsRemain { remaining } => write!(
                formatter,
                "quarantined operation still owns {remaining} manifest rows under a published \
                 revision; those rows are the published manifest and reconciliation refuses to \
                 touch them"
            ),
            Self::RevisionNotFound { revision } => {
                write!(
                    formatter,
                    "artifact revision {:02x?} not found",
                    revision.as_bytes()
                )
            }
            Self::RevisionUnavailable { revision } => write!(
                formatter,
                "artifact revision {:02x?} is not Available",
                revision.as_bytes()
            ),
            Self::RevisionIdentityCollision => {
                formatter.write_str("new revision collides with a referenced revision")
            }
            Self::RevisionReferenceMissing => {
                formatter.write_str("current path strong-reference row is missing")
            }
            Self::RevisionReferenceEpochAhead => {
                formatter.write_str("reference row epoch is newer than its artifact revision")
            }
            Self::ReferenceCountUnderflow { revision } => write!(
                formatter,
                "artifact revision {:02x?} strong-reference count underflow",
                revision.as_bytes()
            ),
            Self::ReferenceCountOverflow { revision } => write!(
                formatter,
                "artifact revision {:02x?} strong-reference count overflow",
                revision.as_bytes()
            ),
            Self::ReferenceEpochOverflow { revision } => write!(
                formatter,
                "artifact revision {:02x?} reference epoch overflow",
                revision.as_bytes()
            ),
            Self::CommitVersionOverflow => formatter.write_str("metadata commit version overflow"),
            Self::LengthOverflow { field, length } => {
                write!(formatter, "{field} length {length} exceeds u32")
            }
            Self::CommandItemLimit { field, count, max } => {
                write!(formatter, "{field} count {count} exceeds {max}")
            }
            Self::DuplicateCommandKey { family } => {
                write!(formatter, "duplicate command key in {family:?}")
            }
            Self::OperationInputMismatch => formatter
                .write_str("publish operation id was reused with different immutable inputs"),
            Self::ReplayResultMismatch => {
                formatter.write_str("stored deterministic publication result is inconsistent")
            }
            Self::MetadataLastFinalizationRequired => formatter.write_str(
                "Published is installed only by finalize_publish with metadata-last closure",
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct Loaded<T> {
    payload: Vec<u8>,
    record: T,
}

#[derive(Clone, Debug)]
struct OwnerRevisionUpdate {
    loaded: Loaded<ArtifactRevisionRecord>,
    next: ArtifactRevisionRecord,
    add_dependency: bool,
}

#[derive(Default)]
struct CommandPlan {
    predicates: Vec<CommandPredicate>,
    mutations: Vec<CommandMutation>,
    history: Vec<HistoryProjection>,
    events: Vec<EventProjection>,
    exact_keys: BTreeSet<(MetadataFamily, Vec<u8>)>,
}

impl CommandPlan {
    fn assert_value(
        &mut self,
        family: MetadataFamily,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
    ) -> Result<(), PublicationError> {
        if !self.exact_keys.insert((family, key.clone())) {
            return Err(PublicationError::DuplicateCommandKey { family });
        }
        self.predicates.push(CommandPredicate::Value {
            family,
            key,
            expected,
        });
        Ok(())
    }

    fn prefix_empty(&mut self, family: MetadataFamily, prefix: Vec<u8>) {
        self.predicates
            .push(CommandPredicate::PrefixEmpty { family, prefix });
    }

    fn put_absent(
        &mut self,
        family: MetadataFamily,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), PublicationError> {
        self.assert_value(family, key.clone(), None)?;
        self.mutations
            .push(CommandMutation::Put { family, key, value });
        Ok(())
    }

    fn replace(
        &mut self,
        family: MetadataFamily,
        key: Vec<u8>,
        expected: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), PublicationError> {
        self.assert_value(family, key.clone(), Some(expected))?;
        self.mutations.push(CommandMutation::Put {
            family,
            key: key.clone(),
            value,
        });
        self.history.push(HistoryProjection { family, key });
        Ok(())
    }

    fn delete(
        &mut self,
        family: MetadataFamily,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> Result<(), PublicationError> {
        self.assert_value(family, key.clone(), Some(expected))?;
        self.mutations.push(CommandMutation::Delete {
            family,
            key: key.clone(),
        });
        self.history.push(HistoryProjection { family, key });
        Ok(())
    }

    fn validate_bounds(&self) -> Result<(), PublicationError> {
        for (field, count) in [
            ("predicates", self.predicates.len()),
            ("mutations", self.mutations.len()),
            ("history projections", self.history.len()),
            ("event projections", self.events.len()),
        ] {
            if count > MAX_COMMAND_ITEMS {
                return Err(PublicationError::CommandItemLimit {
                    field,
                    count,
                    max: MAX_COMMAND_ITEMS,
                });
            }
        }
        Ok(())
    }
}

fn secondary_index_rows(
    root_id: RootId,
    workspace_incarnation_id: nokv_types::WorkspaceIncarnationId,
    path: &nokv_types::NormalizedRelativePath,
    generation: Generation,
    projection: &TypedProjection,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, PublicationError> {
    let value = SecondaryIndexRecord {
        path_generation: generation,
        compact_projection: projection.clone(),
    }
    .encode()?;
    Ok(projection
        .fields()
        .iter()
        .map(|(field, scalar)| {
            (
                secondary_index_key(root_id, field, scalar, workspace_incarnation_id, path),
                value.clone(),
            )
        })
        .collect())
}

fn validate_begin_request(request: &BeginPublishRequest) -> Result<(), PublicationError> {
    validate_operation_seals(&request.operation)?;
    if request.operation.initiating_owner_epoch != request.context.owner_epoch {
        return Err(PublicationError::InitiatingOwnerEpochMismatch {
            expected: request.context.owner_epoch,
            actual: request.operation.initiating_owner_epoch,
        });
    }
    if request.operation.phase != PublishPhase::Uploading {
        return Err(PublicationError::InvalidOperationPhase {
            expected: PublishPhase::Uploading,
            actual: request.operation.phase,
        });
    }
    if request.operation.staged_object_cursor != 0
        || request.operation.uploaded_object_cursor != 0
        || request.operation.manifest_cursor != 0
        || request.operation.manifest_last_position.is_some()
        || request.operation.cleanup_staged_object_cursor != 0
        || request.operation.cleanup_manifest_cursor != 0
    {
        return Err(PublicationError::InvalidOperationSeal {
            seal: "initial cursor",
        });
    }
    Ok(())
}

fn require_operation_phase(
    operation: &PublishOperationRecord,
    expected: PublishPhase,
) -> Result<(), PublicationError> {
    if operation.phase == expected {
        Ok(())
    } else {
        Err(PublicationError::InvalidOperationPhase {
            expected,
            actual: operation.phase,
        })
    }
}

fn validate_batch_size(batch: &'static str, count: usize) -> Result<(), PublicationError> {
    if count == 0 {
        return Err(PublicationError::EmptyBatch { batch });
    }
    if count > MAX_PUBLICATION_BATCH_ROWS {
        return Err(PublicationError::BatchTooLarge {
            batch,
            count,
            max: MAX_PUBLICATION_BATCH_ROWS,
        });
    }
    Ok(())
}

fn checked_batch_cursor(
    batch: &'static str,
    cursor: u32,
    count: usize,
    planned: u32,
) -> Result<u32, PublicationError> {
    let count_u32 =
        u32::try_from(count).map_err(|_| PublicationError::BatchExceedsPlannedCount {
            batch,
            cursor,
            count,
            planned,
        })?;
    let next = cursor
        .checked_add(count_u32)
        .ok_or(PublicationError::BatchExceedsPlannedCount {
            batch,
            cursor,
            count,
            planned,
        })?;
    if next > planned {
        return Err(PublicationError::BatchExceedsPlannedCount {
            batch,
            cursor,
            count,
            planned,
        });
    }
    Ok(next)
}

fn validate_operation_seals(operation: &PublishOperationRecord) -> Result<(), PublicationError> {
    operation.validate()?;
    if operation.identity_digest != publish_identity_digest(operation) {
        return Err(PublicationError::InvalidOperationSeal { seal: "identity" });
    }
    if operation.initialization_digest != publish_initialization_digest(operation) {
        return Err(PublicationError::InvalidOperationSeal {
            seal: "initialization",
        });
    }
    Ok(())
}

fn validate_cleanup_update(
    expected: &StagedObjectRecord,
    next: &StagedObjectRecord,
) -> Result<(), PublicationError> {
    expected.validate()?;
    next.validate()?;
    if expected.artifact_revision_id != next.artifact_revision_id
        || expected.object_sequence != next.object_sequence
        || expected.object_key != next.object_key
        || expected.multipart_upload_id != next.multipart_upload_id
        || expected.expected_length != next.expected_length
        || expected.expected_digest_uri != next.expected_digest_uri
        || next.provider_state != StagedProviderState::Aborted
        || next.cleanup_state != StagedCleanupState::Deleted
    {
        return Err(PublicationError::InvalidStagedObjectTransition {
            sequence: expected.object_sequence,
        });
    }
    Ok(())
}

fn validate_staged_update(
    expected: &StagedObjectRecord,
    next: &StagedObjectRecord,
) -> Result<(), PublicationError> {
    expected.validate()?;
    next.validate()?;
    if expected == next
        || expected.artifact_revision_id != next.artifact_revision_id
        || expected.object_sequence != next.object_sequence
        || expected.object_key != next.object_key
        || expected.expected_length != next.expected_length
        || expected.expected_digest_uri != next.expected_digest_uri
        || expected
            .multipart_upload_id
            .as_ref()
            .is_some_and(|id| next.multipart_upload_id.as_ref() != Some(id))
        || !valid_provider_transition(expected.provider_state, next.provider_state)
        || !valid_cleanup_transition(expected.cleanup_state, next.cleanup_state)
    {
        return Err(PublicationError::InvalidStagedObjectTransition {
            sequence: expected.object_sequence,
        });
    }
    Ok(())
}

fn valid_provider_transition(expected: StagedProviderState, next: StagedProviderState) -> bool {
    expected == next
        || matches!(
            (expected, next),
            (
                StagedProviderState::Planned,
                StagedProviderState::Uploading
                    | StagedProviderState::Uploaded
                    | StagedProviderState::AbortPending
                    | StagedProviderState::Ambiguous
            ) | (
                StagedProviderState::Uploading,
                StagedProviderState::Uploaded
                    | StagedProviderState::AbortPending
                    | StagedProviderState::Ambiguous
            ) | (
                StagedProviderState::Uploaded,
                StagedProviderState::AbortPending
                    | StagedProviderState::Aborted
                    | StagedProviderState::Ambiguous
            ) | (
                StagedProviderState::AbortPending,
                StagedProviderState::Aborted | StagedProviderState::Ambiguous
            )
        )
}

fn valid_cleanup_transition(expected: StagedCleanupState, next: StagedCleanupState) -> bool {
    expected == next
        || matches!(
            (expected, next),
            (
                StagedCleanupState::Owned,
                StagedCleanupState::DeletePending | StagedCleanupState::Quarantined
            ) | (
                StagedCleanupState::DeletePending,
                StagedCleanupState::Deleted | StagedCleanupState::Quarantined
            )
        )
}

fn validate_manifest_order(rows: &[ManifestRowInput]) -> Result<(), PublicationError> {
    if rows.len() > MAX_MANIFEST_ROWS as usize {
        return Err(PublicationError::ManifestCountMismatch {
            expected: MAX_MANIFEST_ROWS,
            actual: rows.len(),
        });
    }
    for input in rows {
        input.row.validate()?;
    }
    for pair in rows.windows(2) {
        if pair[0].object_index >= pair[1].object_index {
            return Err(PublicationError::ManifestOrder);
        }
        let expected = pair[0]
            .row
            .logical_offset
            .checked_add(pair[0].row.length)
            .expect("validated manifest logical range cannot overflow");
        if pair[1].row.logical_offset != expected {
            return Err(PublicationError::ManifestLogicalOffsetMismatch {
                expected,
                actual: pair[1].row.logical_offset,
            });
        }
    }
    Ok(())
}

fn validate_dependency_seal(
    operation: &PublishOperationRecord,
    owners: &[ArtifactRevisionId],
) -> Result<(), PublicationError> {
    let actual =
        u8::try_from(owners.len()).map_err(|_| PublicationError::DependencyCountMismatch {
            expected: operation.dependency_count,
            actual: owners.len(),
        })?;
    if actual != operation.dependency_count {
        return Err(PublicationError::DependencyCountMismatch {
            expected: operation.dependency_count,
            actual: owners.len(),
        });
    }
    if dependency_owner_digest(owners)? != operation.dependency_digest {
        return Err(PublicationError::DependencyDigestMismatch);
    }
    Ok(())
}

fn validate_dependency_order(owners: &[ArtifactRevisionId]) -> Result<(), PublicationError> {
    if owners.len() > usize::from(MAX_DEPENDENCY_COUNT) {
        return Err(PublicationError::DependencyCountMismatch {
            expected: MAX_DEPENDENCY_COUNT,
            actual: owners.len(),
        });
    }
    if owners.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PublicationError::DependencyOrder);
    }
    Ok(())
}

fn commit_manifest_claim_matches(expected: CommitManifestCondition, actual: &PublishClaim) -> bool {
    match (expected, actual) {
        (CommitManifestCondition::CreateOnly, PublishClaim::CreateOnly) => true,
        (
            CommitManifestCondition::ReplaceOnly {
                expected_generation,
            },
            PublishClaim::ReplaceOnly {
                expected_generation: actual_generation,
            },
        ) => expected_generation == *actual_generation,
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreManifestKind {
    Run,
    Restore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationAuthorityPurpose {
    Forward,
    Cleanup,
}

fn transition_authority_purpose(transition: &PublishTransition) -> PublicationAuthorityPurpose {
    match transition {
        PublishTransition::BeginFinalization | PublishTransition::Publish { .. } => {
            PublicationAuthorityPurpose::Forward
        }
        PublishTransition::BeginAbort { .. }
        | PublishTransition::BeginCleaning
        | PublishTransition::FinishCleanup
        | PublishTransition::Quarantine { .. } => PublicationAuthorityPurpose::Cleanup,
    }
}

fn restore_manifest_kind(
    path: &nokv_types::NormalizedRelativePath,
) -> Result<RestoreManifestKind, PublicationError> {
    match path.as_str() {
        RUN_MANIFEST_PATH => Ok(RestoreManifestKind::Run),
        RESTORE_MANIFEST_PATH => Ok(RestoreManifestKind::Restore),
        _ => Err(PublicationError::RestoreManifestPathRequired),
    }
}

fn restore_destination_binding(
    restore: &RestoreOperationRecord,
) -> Result<&RestoreDestinationBinding, PublicationError> {
    let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
        return Err(PublicationError::RestoreAuthorityMismatch);
    };
    provenance
        .destination_binding
        .as_ref()
        .ok_or(PublicationError::RestoreAuthorityMismatch)
}

fn expected_restore_manifest_identity(
    binding: &RestoreDestinationBinding,
    kind: RestoreManifestKind,
) -> RestoreManifestIdentity {
    match kind {
        RestoreManifestKind::Run => binding.run_manifest_identity,
        RestoreManifestKind::Restore => binding.restore_manifest_identity,
    }
}

fn actual_restore_manifest_publication(
    binding: &RestoreDestinationBinding,
    kind: RestoreManifestKind,
) -> Option<&RestoreManifestPublication> {
    binding.manifests.as_ref().map(|manifests| match kind {
        RestoreManifestKind::Run => &manifests.run_manifest,
        RestoreManifestKind::Restore => &manifests.restore_manifest,
    })
}

fn validate_path_claim(
    claim: &PublishClaim,
    current: Option<&Loaded<PathEntry>>,
) -> Result<Generation, PublicationError> {
    match (claim, current) {
        (PublishClaim::CreateOnly, None) => {
            Generation::new(1).map_err(|_| PublicationError::PathGenerationOverflow)
        }
        (PublishClaim::CreateOnly, Some(_)) => Err(PublicationError::PathAlreadyExists),
        (PublishClaim::ReplaceOnly { .. } | PublishClaim::Append { .. }, None) => {
            Err(PublicationError::PathNotFound)
        }
        (
            PublishClaim::ReplaceOnly {
                expected_generation,
            },
            Some(current),
        ) => next_generation(*expected_generation, &current.record),
        (
            PublishClaim::Append {
                expected_generation,
                base_revision_id,
                ..
            },
            Some(current),
        ) => {
            let generation = next_generation(*expected_generation, &current.record)?;
            if current.record.artifact_revision_id != *base_revision_id {
                return Err(PublicationError::AppendBaseRevisionMismatch);
            }
            Ok(generation)
        }
    }
}

fn next_generation(
    expected: Generation,
    current: &PathEntry,
) -> Result<Generation, PublicationError> {
    if current.generation != expected {
        return Err(PublicationError::PathGenerationMismatch {
            expected: expected.get(),
            actual: current.generation.get(),
        });
    }
    Generation::new(
        expected
            .get()
            .checked_add(1)
            .ok_or(PublicationError::PathGenerationOverflow)?,
    )
    .map_err(|_| PublicationError::PathGenerationOverflow)
}

fn publish_identity_digest(operation: &PublishOperationRecord) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.operation.identity.v3\0");
    hasher.update(operation.operation_id.as_bytes());
    hasher.update(operation.initiating_owner_epoch.get().to_be_bytes());
    match operation.authority {
        PublishAuthority::Visible => hasher.update([1]),
        PublishAuthority::CommitStaging {
            commit_operation_id,
        } => {
            hasher.update([3]);
            hasher.update(commit_operation_id.as_bytes());
        }
        PublishAuthority::RestoreStaging {
            restore_operation_id,
        } => {
            hasher.update([2]);
            hasher.update(restore_operation_id.as_bytes());
        }
    }
    hasher.update(
        u32::try_from(operation.workbench_id.as_bytes().len())
            .expect("workbench id length fits u32")
            .to_be_bytes(),
    );
    hasher.update(operation.workbench_id.as_bytes());
    hasher.update(operation.workspace_incarnation_id.as_bytes());
    let path = operation.path.as_str().as_bytes();
    hasher.update(
        u32::try_from(path.len())
            .expect("normalized path length fits u32")
            .to_be_bytes(),
    );
    hasher.update(path);
    hasher.update(operation.artifact_revision_id.as_bytes());
    match &operation.claim {
        PublishClaim::CreateOnly => hasher.update([1]),
        PublishClaim::ReplaceOnly {
            expected_generation,
        } => {
            hasher.update([2]);
            hasher.update(expected_generation.get().to_be_bytes());
        }
        PublishClaim::Append {
            expected_generation,
            base_revision_id,
            append_offset,
        } => {
            hasher.update([3]);
            hasher.update(expected_generation.get().to_be_bytes());
            hasher.update(base_revision_id.as_bytes());
            hasher.update(append_offset.to_be_bytes());
        }
    }
    hasher.finalize().into()
}

fn publish_initialization_digest(operation: &PublishOperationRecord) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.operation.initialization.v3\0");
    hasher.update(operation.initiating_owner_epoch.get().to_be_bytes());
    match operation.authority {
        PublishAuthority::Visible => hasher.update([1]),
        PublishAuthority::CommitStaging {
            commit_operation_id,
        } => {
            hasher.update([3]);
            hasher.update(commit_operation_id.as_bytes());
        }
        PublishAuthority::RestoreStaging {
            restore_operation_id,
        } => {
            hasher.update([2]);
            hasher.update(restore_operation_id.as_bytes());
        }
    }
    hasher.update(
        u32::try_from(operation.workbench_id.as_bytes().len())
            .expect("workbench id length fits u32")
            .to_be_bytes(),
    );
    hasher.update(operation.workbench_id.as_bytes());
    hasher.update(operation.staged_object_count.to_be_bytes());
    hasher.update(operation.staged_object_seal);
    hasher.update(operation.manifest_row_count.to_be_bytes());
    hasher.update(operation.manifest_seal);
    hasher.update([operation.dependency_count, operation.dependency_depth]);
    hasher.update(operation.dependency_digest);
    hasher.finalize().into()
}

fn sha256_digest_uri(digest: [u8; SHA256_BYTES]) -> String {
    let mut value = String::with_capacity(7 + SHA256_BYTES * 2);
    value.push_str("sha256:");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        value.push(HEX[usize::from(byte >> 4)] as char);
        value.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    value
}

fn hash_count(hasher: &mut Sha256, count: usize) -> Result<(), PublicationError> {
    let count = u32::try_from(count).map_err(|_| PublicationError::LengthOverflow {
        field: "canonical collection",
        length: count,
    })?;
    hasher.update(count.to_be_bytes());
    Ok(())
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), PublicationError> {
    let length = u32::try_from(bytes.len()).map_err(|_| PublicationError::LengthOverflow {
        field: "canonical bytes",
        length: bytes.len(),
    })?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn finalization_takeover_absence_proof(
    context: PublicationContext,
    finalizing_payload: &[u8],
) -> Result<[u8; SHA256_BYTES], PublicationError> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.finalization-takeover-absence.v1\0");
    hasher.update(context.root_id.as_bytes());
    hasher.update(context.logical_shard_id.as_bytes());
    hasher.update(context.placement_generation.get().to_be_bytes());
    hasher.update(context.owner_epoch.get().to_be_bytes());
    hasher.update(context.request_id.as_bytes());
    hasher.update(context.read_version.get().to_be_bytes());
    hash_bytes(&mut hasher, finalizing_payload)?;
    Ok(hasher.finalize().into())
}

fn reconcile_resolution_label(resolution: QuarantineReconcileResolution) -> &'static str {
    match resolution {
        QuarantineReconcileResolution::RevisionUnpublished => "revision-unpublished",
        QuarantineReconcileResolution::RevisionPublished => "revision-published",
    }
}

fn reconcile_terminal_message(resolution: QuarantineReconcileResolution, reason: &str) -> String {
    format!(
        "operator reconciliation ({}): {reason}",
        reconcile_resolution_label(resolution)
    )
}

/// Deterministic audit chain binding the operator verdict, the operator's
/// provider verification transcript, and the original quarantine evidence.
fn reconcile_evidence_digest(
    resolution: QuarantineReconcileResolution,
    original_evidence_digest: Option<[u8; SHA256_BYTES]>,
    operator_evidence_digest: [u8; SHA256_BYTES],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.reconcile-evidence.v1\0");
    hasher.update([match resolution {
        QuarantineReconcileResolution::RevisionUnpublished => 1,
        QuarantineReconcileResolution::RevisionPublished => 2,
    }]);
    match original_evidence_digest {
        None => hasher.update([0]),
        Some(digest) => {
            hasher.update([1]);
            hasher.update(digest);
        }
    }
    hasher.update(operator_evidence_digest);
    hasher.finalize().into()
}

fn decode_operation_outcome(
    result: MetadataCommandResult,
    operation_id: nokv_types::OperationId,
) -> Result<PublishCommandOutcome, PublicationError> {
    let operation = PublishOperationRecord::decode(&result.deterministic_result)?;
    if operation.operation_id != operation_id {
        return Err(PublicationError::ReplayResultMismatch);
    }
    Ok(PublishCommandOutcome {
        commit_version: result.commit_version,
        operation,
        replayed: result.replayed,
    })
}

impl std::error::Error for PublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(source) => Some(source),
            Self::OperationCodec(source) => Some(source),
            Self::RecordCodec(source) => Some(source),
            Self::QueryRecord(source) => Some(source),
            Self::CommitOperationCodec(source) => Some(source),
            Self::RestoreCodec(source) => Some(source),
            _ => None,
        }
    }
}

impl From<MetaError> for PublicationError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<PublishRecordError> for PublicationError {
    fn from(error: PublishRecordError) -> Self {
        Self::OperationCodec(error)
    }
}

impl From<PublicationRecordCodecError> for PublicationError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::RecordCodec(error)
    }
}

impl From<QueryRecordError> for PublicationError {
    fn from(error: QueryRecordError) -> Self {
        Self::QueryRecord(error)
    }
}

impl From<CommitOperationRecordError> for PublicationError {
    fn from(error: CommitOperationRecordError) -> Self {
        Self::CommitOperationCodec(error)
    }
}

impl From<RestoreRecordError> for PublicationError {
    fn from(error: RestoreRecordError) -> Self {
        Self::RestoreCodec(error)
    }
}

/// Publication command facade over the authoritative metadata executor.
#[derive(Clone, Copy)]
pub struct PublicationService<'a> {
    store: &'a MetaShard,
}

impl<'a> PublicationService<'a> {
    pub const fn new(store: &'a MetaShard) -> Self {
        Self { store }
    }
}

/// Seals identity and initialization digests after immutable operation inputs
/// and row/dependency seals have been assigned.
pub fn seal_publish_operation(operation: &mut PublishOperationRecord) {
    operation.identity_digest = publish_identity_digest(operation);
    operation.initialization_digest = publish_initialization_digest(operation);
}

/// Canonical digest of immutable staged-object ownership fields.
pub fn staged_object_ledger_digest(
    staged_objects: &[StagedObjectRecord],
) -> Result<[u8; SHA256_BYTES], PublicationError> {
    let mut digest = [0; SHA256_BYTES];
    for (index, staged) in staged_objects.iter().enumerate() {
        let expected_sequence =
            u32::try_from(index).map_err(|_| PublicationError::StagedObjectCountMismatch {
                expected: MAX_STAGED_OBJECTS,
                actual: staged_objects.len(),
            })?;
        if staged.object_sequence != expected_sequence {
            return Err(PublicationError::StagedObjectSequenceMismatch {
                expected: expected_sequence,
                actual: staged.object_sequence,
            });
        }
        digest = advance_staged_object_rolling_digest(digest, staged)?;
    }
    Ok(digest)
}

/// Advances the staged-ledger closure by one canonical immutable row.
pub fn advance_staged_object_rolling_digest(
    previous: [u8; SHA256_BYTES],
    staged: &StagedObjectRecord,
) -> Result<[u8; SHA256_BYTES], PublicationError> {
    staged.validate()?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.staged-row.v1\0");
    hasher.update(previous);
    hasher.update(staged.artifact_revision_id.as_bytes());
    hasher.update(staged.object_sequence.to_be_bytes());
    hash_bytes(&mut hasher, staged.object_key.as_bytes())?;
    hasher.update(staged.expected_length.to_be_bytes());
    hash_bytes(&mut hasher, staged.expected_digest_uri.as_bytes())?;
    Ok(hasher.finalize().into())
}

/// Canonical digest of ordered manifest keys and strict row payloads.
pub fn manifest_rows_digest(
    rows: &[ManifestRowInput],
) -> Result<[u8; SHA256_BYTES], PublicationError> {
    validate_manifest_order(rows)?;
    if let Some(first) = rows.first() {
        if first.row.logical_offset != 0 {
            return Err(PublicationError::ManifestLogicalOffsetMismatch {
                expected: 0,
                actual: first.row.logical_offset,
            });
        }
    }
    let mut digest = [0; SHA256_BYTES];
    for input in rows {
        digest = advance_manifest_rolling_digest(digest, input)?;
    }
    Ok(digest)
}

/// Advances the manifest closure by one ordered key and strict row payload.
pub fn advance_manifest_rolling_digest(
    previous: [u8; SHA256_BYTES],
    input: &ManifestRowInput,
) -> Result<[u8; SHA256_BYTES], PublicationError> {
    input.row.validate()?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.manifest-row.v2\0");
    hasher.update(previous);
    hasher.update(input.object_index.to_be_bytes());
    let encoded = input.row.encode()?;
    hash_bytes(&mut hasher, &encoded)?;
    Ok(hasher.finalize().into())
}

/// Canonical digest of the sorted, distinct dependency owner set.
pub fn dependency_owner_digest(
    owners: &[ArtifactRevisionId],
) -> Result<[u8; SHA256_BYTES], PublicationError> {
    validate_dependency_order(owners)?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.publish.dependencies.v1\0");
    hash_count(&mut hasher, owners.len())?;
    for owner in owners {
        hasher.update(owner.as_bytes());
    }
    Ok(hasher.finalize().into())
}

impl PublicationService<'_> {
    pub fn begin_publish(
        &self,
        request: BeginPublishRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_begin_request(&request)?;
        let operation_key = operation_key(
            request.context.root_id,
            OperationKind::Publish,
            request.operation.operation_id,
        );
        if let Some(payload) =
            self.read_payload(request.context, MetadataFamily::Operation, &operation_key)?
        {
            let operation = PublishOperationRecord::decode(&payload)?;
            validate_operation_seals(&operation)?;
            let mut replay_candidate = request.operation.clone();
            if matches!(operation.authority, PublishAuthority::RestoreStaging { .. })
                && operation.phase == PublishPhase::Published
            {
                // The owner epoch is server-derived admission state, not a
                // caller-selected part of a restore manifest identity. A
                // successor may borrow only the stored epoch of an already
                // terminal RestoreStaging row, then must still match every
                // other sealed input and pass the live restore/path checks
                // below. Visible and CommitStaging receipts deliberately
                // remain epoch-bound because they lack equivalent live
                // replay authority validation.
                replay_candidate.initiating_owner_epoch = operation.initiating_owner_epoch;
                seal_publish_operation(&mut replay_candidate);
            }
            if operation.operation_id != replay_candidate.operation_id
                || operation.identity_digest != replay_candidate.identity_digest
                || operation.initialization_digest != replay_candidate.initialization_digest
            {
                return Err(PublicationError::OperationInputMismatch);
            }
            // A durable publish receipt is not authority by itself. Restore
            // cleanup may have won after the client lost the response, while
            // successful initialization/complete must remain replayable only
            // when the live path and exact destination binding still agree.
            self.validate_existing_publish_replay_authority(request.context, &operation)?;
            return Ok(PublishCommandOutcome {
                commit_version: CommitVersion::new(request.context.read_version.get())
                    .expect("publication contexts always have a non-zero read version"),
                operation,
                replayed: true,
            });
        }
        let lease_clock = self.store.lease_clock_high_water()?;
        if request.operation.activity_deadline_ms <= lease_clock {
            return Err(PublicationError::ActivityDeadlineNotFuture {
                clock: lease_clock,
                requested: request.operation.activity_deadline_ms,
            });
        }
        // Take the revision-scoped exclusive claim before planning: staged
        // rows derive permanent object keys from the revision id alone, so a
        // second operation owning the same revision could later destroy the
        // first operation's published objects during abort cleanup. Same-
        // operation replays return above through the operation row.
        let claim_key = artifact_revision_claim_key(
            request.context.root_id,
            request.operation.artifact_revision_id,
        );
        if let Some(payload) = self.read_payload(
            request.context,
            MetadataFamily::ArtifactRevision,
            &claim_key,
        )? {
            let claim = ArtifactRevisionClaimRecord::decode(&payload)?;
            return Err(PublicationError::RevisionClaimHeld {
                revision: request.operation.artifact_revision_id,
                operation_id: claim.operation_id,
            });
        }
        let operation_payload = request.operation.encode()?;
        let mut plan = CommandPlan::default();
        plan.put_absent(
            MetadataFamily::Operation,
            operation_key,
            operation_payload.clone(),
        )?;
        plan.put_absent(
            MetadataFamily::ArtifactRevision,
            claim_key,
            ArtifactRevisionClaimRecord {
                operation_id: request.operation.operation_id,
            }
            .encode()?,
        )?;
        plan.prefix_empty(
            MetadataFamily::StagedObject,
            staged_object_prefix(request.context.root_id, request.operation.operation_id),
        );
        plan.prefix_empty(
            MetadataFamily::ArtifactManifest,
            artifact_manifest_prefix(
                request.context.root_id,
                request.operation.artifact_revision_id,
            ),
        );
        plan.assert_value(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(
                request.context.root_id,
                request.operation.artifact_revision_id,
            ),
            None,
        )?;

        let result = self.execute_plan_before_lease_deadline(
            request.context,
            plan,
            operation_payload,
            request.operation.activity_deadline_ms,
            PublicationAuthorityPurpose::Forward,
        )?;
        decode_operation_outcome(result, request.operation.operation_id)
    }

    pub fn stage_objects_batch(
        &self,
        request: StageObjectsBatchRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        require_operation_phase(&request.expected_operation, PublishPhase::Uploading)?;
        validate_batch_size("stage objects", request.staged_objects.len())?;
        let next_cursor = checked_batch_cursor(
            "stage objects",
            request.expected_operation.staged_object_cursor,
            request.staged_objects.len(),
            request.expected_operation.staged_object_count,
        )?;

        let mut next_operation = request.expected_operation.clone();
        let mut rolling_digest = next_operation.staged_object_rolling_digest;
        let mut keys = BTreeSet::new();
        for (offset, staged) in request.staged_objects.iter().enumerate() {
            let sequence = next_operation
                .staged_object_cursor
                .checked_add(u32::try_from(offset).map_err(|_| {
                    PublicationError::BatchTooLarge {
                        batch: "stage objects",
                        count: request.staged_objects.len(),
                        max: MAX_PUBLICATION_BATCH_ROWS,
                    }
                })?)
                .ok_or(PublicationError::BatchExceedsPlannedCount {
                    batch: "stage objects",
                    cursor: next_operation.staged_object_cursor,
                    count: request.staged_objects.len(),
                    planned: next_operation.staged_object_count,
                })?;
            if staged.object_sequence != sequence {
                return Err(PublicationError::StagedObjectSequenceMismatch {
                    expected: sequence,
                    actual: staged.object_sequence,
                });
            }
            if staged.artifact_revision_id != next_operation.artifact_revision_id {
                return Err(PublicationError::StagedObjectRevisionMismatch);
            }
            let expected_object_key = object_block_key(
                request.context.logical_shard_id,
                request.context.root_id,
                next_operation.artifact_revision_id,
                u64::from(sequence),
            );
            if staged.object_key != expected_object_key {
                return Err(PublicationError::StagedObjectKeyMismatch { sequence });
            }
            if staged.provider_state != StagedProviderState::Planned
                || staged.cleanup_state != StagedCleanupState::Owned
            {
                return Err(PublicationError::InvalidStagedObjectTransition { sequence });
            }
            if !keys.insert(staged.object_key.as_str()) {
                return Err(PublicationError::DuplicateStagedObjectKey { sequence });
            }
            rolling_digest = advance_staged_object_rolling_digest(rolling_digest, staged)?;
        }
        next_operation.staged_object_cursor = next_cursor;
        next_operation.staged_object_rolling_digest = rolling_digest;
        let expected_payload = request.expected_operation.encode()?;
        let next_payload = next_operation.encode()?;

        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        for staged in &request.staged_objects {
            plan.put_absent(
                MetadataFamily::StagedObject,
                staged_object_key(
                    request.context.root_id,
                    next_operation.operation_id,
                    u64::from(staged.object_sequence),
                ),
                staged.encode()?,
            )?;
        }
        let result = self.execute_plan(
            request.context,
            plan,
            next_payload,
            PublicationAuthorityPurpose::Forward,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    pub fn mark_objects_uploaded_batch(
        &self,
        request: MarkObjectsUploadedBatchRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        require_operation_phase(&request.expected_operation, PublishPhase::Uploading)?;
        validate_batch_size("mark objects uploaded", request.staged_object_updates.len())?;
        let next_cursor = checked_batch_cursor(
            "mark objects uploaded",
            request.expected_operation.uploaded_object_cursor,
            request.staged_object_updates.len(),
            request.expected_operation.staged_object_cursor,
        )?;

        let mut next_operation = request.expected_operation.clone();
        let mut rolling_digest = next_operation.uploaded_object_rolling_digest;
        let mut plan = CommandPlan::default();
        for (offset, update) in request.staged_object_updates.iter().enumerate() {
            let sequence = next_operation
                .uploaded_object_cursor
                .checked_add(u32::try_from(offset).map_err(|_| {
                    PublicationError::BatchTooLarge {
                        batch: "mark objects uploaded",
                        count: request.staged_object_updates.len(),
                        max: MAX_PUBLICATION_BATCH_ROWS,
                    }
                })?)
                .ok_or(PublicationError::BatchExceedsPlannedCount {
                    batch: "mark objects uploaded",
                    cursor: next_operation.uploaded_object_cursor,
                    count: request.staged_object_updates.len(),
                    planned: next_operation.staged_object_cursor,
                })?;
            validate_staged_update(&update.expected, &update.next)?;
            if update.expected.object_sequence != sequence {
                return Err(PublicationError::StagedObjectSequenceMismatch {
                    expected: sequence,
                    actual: update.expected.object_sequence,
                });
            }
            if update.expected.artifact_revision_id != next_operation.artifact_revision_id {
                return Err(PublicationError::StagedObjectRevisionMismatch);
            }
            if update.next.provider_state != StagedProviderState::Uploaded
                || update.next.cleanup_state != StagedCleanupState::Owned
            {
                return Err(PublicationError::StagedObjectNotUploaded { sequence });
            }
            rolling_digest = advance_staged_object_rolling_digest(rolling_digest, &update.next)?;
            plan.replace(
                MetadataFamily::StagedObject,
                staged_object_key(
                    request.context.root_id,
                    next_operation.operation_id,
                    u64::from(sequence),
                ),
                update.expected.encode()?,
                update.next.encode()?,
            )?;
        }
        next_operation.uploaded_object_cursor = next_cursor;
        next_operation.uploaded_object_rolling_digest = rolling_digest;
        let expected_payload = request.expected_operation.encode()?;
        let next_payload = next_operation.encode()?;
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        let result = self.execute_plan(
            request.context,
            plan,
            next_payload,
            PublicationAuthorityPurpose::Forward,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    pub fn stage_manifest_batch(
        &self,
        request: StageManifestBatchRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        require_operation_phase(&request.expected_operation, PublishPhase::Uploading)?;
        validate_batch_size("stage manifest", request.manifest_rows.len())?;
        if request.expected_operation.staged_object_cursor
            != request.expected_operation.staged_object_count
            || request.expected_operation.staged_object_rolling_digest
                != request.expected_operation.staged_object_seal
            || request.expected_operation.uploaded_object_cursor
                != request.expected_operation.staged_object_count
            || request.expected_operation.uploaded_object_rolling_digest
                != request.expected_operation.staged_object_seal
        {
            return Err(PublicationError::InvalidOperationSeal {
                seal: "uploaded-object closure",
            });
        }
        validate_dependency_seal(
            &request.expected_operation,
            &request.dependency_owner_revision_ids,
        )?;
        for (offset, input) in request.manifest_rows.iter().enumerate() {
            let expected_object_index = u64::from(request.expected_operation.manifest_cursor)
                .checked_add(
                    u64::try_from(offset)
                        .expect("publication batches are bounded below the u64 range"),
                )
                .ok_or(PublicationError::ManifestPositionMismatch)?;
            if input.object_index != expected_object_index {
                return Err(PublicationError::ManifestPositionMismatch);
            }
            let expected_object_key = object_block_key(
                request.context.logical_shard_id,
                request.context.root_id,
                input.row.physical_owner_revision_id,
                input.row.physical_object_index,
            );
            if input.row.object_key != expected_object_key {
                return Err(PublicationError::ManifestOwnershipMismatch {
                    object_index: input.object_index,
                    reason: "object key does not match the canonical physical owner coordinates",
                });
            }
        }
        validate_manifest_order(&request.manifest_rows)?;
        let next_cursor = checked_batch_cursor(
            "stage manifest",
            request.expected_operation.manifest_cursor,
            request.manifest_rows.len(),
            request.expected_operation.manifest_row_count,
        )?;
        let first = request
            .manifest_rows
            .first()
            .expect("non-empty batch was validated");
        if request
            .expected_operation
            .manifest_last_position
            .is_some_and(|last| first.object_index <= last.object_index)
        {
            return Err(PublicationError::ManifestPositionMismatch);
        }
        let previous_row = if let Some(last) = request.expected_operation.manifest_last_position {
            let key = artifact_manifest_key(
                request.context.root_id,
                request.expected_operation.artifact_revision_id,
                last.object_index,
            );
            let payload = self
                .read_payload(request.context, MetadataFamily::ArtifactManifest, &key)?
                .ok_or(PublicationError::ManifestPositionMismatch)?;
            Some(ArtifactManifestRow::decode(&payload)?)
        } else {
            None
        };
        let expected_logical_offset = previous_row.as_ref().map_or(0, |row| {
            row.logical_offset
                .checked_add(row.length)
                .expect("validated manifest logical range cannot overflow")
        });
        if first.row.logical_offset != expected_logical_offset {
            return Err(PublicationError::ManifestLogicalOffsetMismatch {
                expected: expected_logical_offset,
                actual: first.row.logical_offset,
            });
        }
        let dependencies = request
            .dependency_owner_revision_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut next_operation = request.expected_operation.clone();
        let mut rolling_digest = next_operation.manifest_rolling_digest;
        let mut previous_row = previous_row;
        for input in &request.manifest_rows {
            let owner = input.row.physical_owner_revision_id;
            if owner != next_operation.artifact_revision_id && !dependencies.contains(&owner) {
                return Err(PublicationError::ManifestPhysicalOwnerMissing { owner });
            }
            if input.row.offset != 0 {
                return Err(PublicationError::ManifestOwnershipMismatch {
                    object_index: input.object_index,
                    reason: "packed object ranges are unsupported",
                });
            }
            let target_owned = owner == next_operation.artifact_revision_id;
            let previous_target_owned = previous_row.as_ref().is_some_and(|row| {
                row.physical_owner_revision_id == next_operation.artifact_revision_id
            });
            if !target_owned && previous_target_owned {
                return Err(PublicationError::ManifestOwnershipMismatch {
                    object_index: input.object_index,
                    reason: "borrowed rows cannot follow child-owned rows",
                });
            }
            if target_owned {
                let expected_physical_index = if previous_target_owned {
                    previous_row
                        .as_ref()
                        .expect("the previous target-owned row is present")
                        .physical_object_index
                        .checked_add(1)
                        .ok_or(PublicationError::ManifestOwnershipMismatch {
                            object_index: input.object_index,
                            reason: "physical object index overflows u64",
                        })?
                } else {
                    0
                };
                if input.row.physical_object_index != expected_physical_index {
                    return Err(PublicationError::ManifestOwnershipMismatch {
                        object_index: input.object_index,
                        reason: "child-owned physical object indexes must be contiguous from zero",
                    });
                }
                let sequence = u32::try_from(input.row.physical_object_index).map_err(|_| {
                    PublicationError::ManifestOwnershipMismatch {
                        object_index: input.object_index,
                        reason: "physical object index exceeds the staged-object sequence range",
                    }
                })?;
                if sequence >= next_operation.staged_object_count {
                    return Err(PublicationError::ManifestOwnershipMismatch {
                        object_index: input.object_index,
                        reason: "child-owned row has no staged object",
                    });
                }
                let staged_key = staged_object_key(
                    request.context.root_id,
                    next_operation.operation_id,
                    u64::from(sequence),
                );
                let staged_payload = self
                    .read_payload(request.context, MetadataFamily::StagedObject, &staged_key)?
                    .ok_or(PublicationError::ManifestOwnershipMismatch {
                        object_index: input.object_index,
                        reason: "child-owned row has no durable staged object",
                    })?;
                let staged = StagedObjectRecord::decode(&staged_payload)?;
                let expected_object_key = object_block_key(
                    request.context.logical_shard_id,
                    request.context.root_id,
                    input.row.physical_owner_revision_id,
                    input.row.physical_object_index,
                );
                let mismatch_reason = if staged.artifact_revision_id
                    != next_operation.artifact_revision_id
                    || staged.object_sequence != sequence
                {
                    Some("staged object identity differs from the child-owned row")
                } else if staged.object_key != expected_object_key {
                    Some("staged object key does not match the canonical child-owned coordinates")
                } else if staged.object_key != input.row.object_key {
                    Some("staged object key differs from the child-owned row")
                } else if staged.expected_length != input.row.length {
                    Some("staged object length differs from the child-owned row")
                } else if staged.expected_digest_uri != input.row.digest_uri {
                    Some("staged object digest differs from the child-owned row")
                } else if staged.provider_state != StagedProviderState::Uploaded
                    || staged.cleanup_state != StagedCleanupState::Owned
                {
                    Some("staged object is not durably uploaded and owned")
                } else {
                    None
                };
                if let Some(reason) = mismatch_reason {
                    return Err(PublicationError::ManifestOwnershipMismatch {
                        object_index: input.object_index,
                        reason,
                    });
                }
            }
            rolling_digest = advance_manifest_rolling_digest(rolling_digest, input)?;
            previous_row = Some(input.row.clone());
        }
        if next_cursor == next_operation.manifest_row_count {
            let target_owned_count = match previous_row.as_ref() {
                Some(row)
                    if row.physical_owner_revision_id == next_operation.artifact_revision_id =>
                {
                    row.physical_object_index.checked_add(1).ok_or(
                        PublicationError::ManifestOwnershipMismatch {
                            object_index: request
                                .manifest_rows
                                .last()
                                .expect("non-empty batch was validated")
                                .object_index,
                            reason: "physical object count overflows u64",
                        },
                    )?
                }
                _ => 0,
            };
            if target_owned_count != u64::from(next_operation.staged_object_count) {
                return Err(PublicationError::ManifestOwnershipMismatch {
                    object_index: request
                        .manifest_rows
                        .last()
                        .expect("non-empty batch was validated")
                        .object_index,
                    reason: "child-owned row count differs from staged-object count",
                });
            }
        }
        let last = request
            .manifest_rows
            .last()
            .expect("non-empty batch was validated");
        next_operation.manifest_cursor = next_cursor;
        next_operation.manifest_rolling_digest = rolling_digest;
        next_operation.manifest_last_position = Some(ManifestPosition {
            object_index: last.object_index,
        });
        let expected_payload = request.expected_operation.encode()?;
        let next_payload = next_operation.encode()?;

        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        plan.assert_value(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(request.context.root_id, next_operation.artifact_revision_id),
            None,
        )?;
        for input in &request.manifest_rows {
            plan.put_absent(
                MetadataFamily::ArtifactManifest,
                artifact_manifest_key(
                    request.context.root_id,
                    next_operation.artifact_revision_id,
                    input.object_index,
                ),
                input.row.encode()?,
            )?;
        }
        let result = self.execute_plan(
            request.context,
            plan,
            next_payload,
            PublicationAuthorityPurpose::Forward,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    pub fn transition_publish(
        &self,
        request: TransitionPublishRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        if matches!(request.transition, PublishTransition::Publish { .. }) {
            return Err(PublicationError::MetadataLastFinalizationRequired);
        }
        let authority_purpose = transition_authority_purpose(&request.transition);
        self.require_current_operation(request.context, &request.expected_operation)?;
        let operation_key = operation_key(
            request.context.root_id,
            OperationKind::Publish,
            request.expected_operation.operation_id,
        );
        let expected_operation_payload = request.expected_operation.encode()?;
        let mut next_operation = request.expected_operation.clone();

        next_operation
            .apply_transition(request.expected_operation.phase, request.transition.clone())?;

        let next_operation_payload = next_operation.encode()?;
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key,
            expected_operation_payload,
            next_operation_payload.clone(),
        )?;
        // Cleanup completion is the terminal owner transition for an aborted
        // operation: its provider objects are durably gone, so the revision
        // identity becomes claimable again in the same command.
        if matches!(request.transition, PublishTransition::FinishCleanup) {
            self.release_revision_claim(
                request.context,
                request.expected_operation.artifact_revision_id,
                request.expected_operation.operation_id,
                &mut plan,
            )?;
        }

        let result = self.execute_plan(
            request.context,
            plan,
            next_operation_payload,
            authority_purpose,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    /// Extend one nonterminal publish lease. Only the owner epoch that began
    /// the operation may heartbeat, and the deadline is strictly monotonic.
    pub fn heartbeat_publish(
        &self,
        request: HeartbeatPublishRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        if request.context.owner_epoch != request.expected_operation.initiating_owner_epoch {
            return Err(PublicationError::HeartbeatOwnerMismatch {
                initiating: request.expected_operation.initiating_owner_epoch,
                current: request.context.owner_epoch,
            });
        }
        if !matches!(
            request.expected_operation.phase,
            PublishPhase::Uploading | PublishPhase::Finalizing
        ) {
            return Err(PublicationError::HeartbeatPhase {
                actual: request.expected_operation.phase,
            });
        }
        if request.activity_deadline_ms <= request.expected_operation.activity_deadline_ms {
            return Err(PublicationError::ActivityDeadlineNotExtended {
                current: request.expected_operation.activity_deadline_ms,
                requested: request.activity_deadline_ms,
            });
        }
        self.require_current_operation(request.context, &request.expected_operation)?;
        let lease_clock = self.store.lease_clock_high_water()?;
        if request.activity_deadline_ms <= lease_clock {
            return Err(PublicationError::ActivityDeadlineNotFuture {
                clock: lease_clock,
                requested: request.activity_deadline_ms,
            });
        }

        let expected_payload = request.expected_operation.encode()?;
        let mut next_operation = request.expected_operation;
        next_operation.activity_deadline_ms = request.activity_deadline_ms;
        let next_payload = next_operation.encode()?;
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        let result = self.execute_plan_before_lease_deadline(
            request.context,
            plan,
            next_payload,
            request.activity_deadline_ms,
            PublicationAuthorityPurpose::Forward,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    /// Atomically aborts an orphaned publish. A newer owner may take over
    /// immediately; the initiating owner must first durably observe the
    /// extend-only activity lease expired. A `Finalizing` operation is safe to
    /// take over because final publication and its `Published` operation
    /// transition are one metadata command; the exact `Finalizing` CAS
    /// performed here cannot win if publication already committed.
    pub fn take_over_orphaned_publish(
        &self,
        request: TakeOverOrphanedPublishRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        if !matches!(
            request.expected_operation.phase,
            PublishPhase::Uploading | PublishPhase::Finalizing
        ) {
            return Err(PublicationError::OwnerTakeoverPhase {
                actual: request.expected_operation.phase,
            });
        }
        if request.context.owner_epoch.get()
            < request.expected_operation.initiating_owner_epoch.get()
        {
            return Err(PublicationError::OwnerTakeoverNotAllowed {
                initiating: request.expected_operation.initiating_owner_epoch,
                current: request.context.owner_epoch,
            });
        }
        if request.context.owner_epoch == request.expected_operation.initiating_owner_epoch {
            let lease_clock = self.store.observe_lease_clock(
                request.context.root_id,
                request.context.placement_generation,
                request.context.owner_epoch,
                request.observed_now_ms,
            )?;
            let expiry_threshold = request
                .expected_operation
                .activity_deadline_ms
                .saturating_add(request.maximum_clock_skew_ms);
            if lease_clock < expiry_threshold {
                return Err(PublicationError::ActivityLeaseNotExpired {
                    deadline_ms: request.expected_operation.activity_deadline_ms,
                    lease_clock_ms: lease_clock,
                    maximum_clock_skew_ms: request.maximum_clock_skew_ms,
                });
            }
        }

        let expected_payload = request.expected_operation.encode()?;
        let mut next_operation = request.expected_operation.clone();
        match request.expected_operation.phase {
            PublishPhase::Uploading => next_operation.apply_transition(
                PublishPhase::Uploading,
                PublishTransition::BeginAbort {
                    terminal_error: request.terminal_error,
                },
            )?,
            PublishPhase::Finalizing => {
                let proof =
                    finalization_takeover_absence_proof(request.context, &expected_payload)?;
                next_operation.take_over_finalization(proof, request.terminal_error)?;
            }
            actual => return Err(PublicationError::OwnerTakeoverPhase { actual }),
        }

        let next_payload = next_operation.encode()?;
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        let result = self.execute_plan(
            request.context,
            plan,
            next_payload,
            PublicationAuthorityPurpose::Cleanup,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    pub fn cleanup_publish_batch(
        &self,
        request: CleanupPublishBatchRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        require_operation_phase(&request.expected_operation, PublishPhase::Cleaning)?;

        let mut next_operation = request.expected_operation.clone();
        let mut plan = CommandPlan::default();
        if next_operation.cleanup_staged_object_cursor < next_operation.staged_object_cursor {
            validate_batch_size(
                "cleanup staged objects",
                request.staged_object_updates.len(),
            )?;
            let next_cursor = checked_batch_cursor(
                "cleanup staged objects",
                next_operation.cleanup_staged_object_cursor,
                request.staged_object_updates.len(),
                next_operation.staged_object_cursor,
            )?;
            for (offset, update) in request.staged_object_updates.iter().enumerate() {
                let sequence = next_operation
                    .cleanup_staged_object_cursor
                    .checked_add(u32::try_from(offset).map_err(|_| {
                        PublicationError::BatchTooLarge {
                            batch: "cleanup staged objects",
                            count: request.staged_object_updates.len(),
                            max: MAX_PUBLICATION_BATCH_ROWS,
                        }
                    })?)
                    .ok_or(PublicationError::BatchExceedsPlannedCount {
                        batch: "cleanup staged objects",
                        cursor: next_operation.cleanup_staged_object_cursor,
                        count: request.staged_object_updates.len(),
                        planned: next_operation.staged_object_cursor,
                    })?;
                validate_cleanup_update(&update.expected, &update.next)?;
                if update.expected.object_sequence != sequence {
                    return Err(PublicationError::StagedObjectSequenceMismatch {
                        expected: sequence,
                        actual: update.expected.object_sequence,
                    });
                }
                if update.expected.artifact_revision_id != next_operation.artifact_revision_id {
                    return Err(PublicationError::StagedObjectRevisionMismatch);
                }
                plan.delete(
                    MetadataFamily::StagedObject,
                    staged_object_key(
                        request.context.root_id,
                        next_operation.operation_id,
                        u64::from(sequence),
                    ),
                    update.expected.encode()?,
                )?;
            }
            next_operation.cleanup_staged_object_cursor = next_cursor;
        } else {
            if !request.staged_object_updates.is_empty() {
                return Err(PublicationError::BatchExceedsPlannedCount {
                    batch: "cleanup staged objects",
                    cursor: next_operation.cleanup_staged_object_cursor,
                    count: request.staged_object_updates.len(),
                    planned: next_operation.staged_object_cursor,
                });
            }
            let remaining = next_operation
                .manifest_cursor
                .checked_sub(next_operation.cleanup_manifest_cursor)
                .expect("operation validation orders manifest cleanup cursors");
            if remaining == 0 {
                return Err(PublicationError::EmptyBatch {
                    batch: "cleanup manifest",
                });
            }
            let limit = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(MAX_PUBLICATION_BATCH_ROWS);
            let prefix = artifact_manifest_prefix(
                request.context.root_id,
                next_operation.artifact_revision_id,
            );
            let rows = self.store.scan_prefix_at(
                request.context.root_id,
                request.context.placement_generation,
                request.context.owner_epoch,
                MetadataFamily::ArtifactManifest,
                &prefix,
                request.context.read_version,
                None,
                limit,
            )?;
            if rows.is_empty() {
                return Err(PublicationError::ManifestCountMismatch {
                    expected: next_operation.manifest_cursor,
                    actual: usize::try_from(next_operation.cleanup_manifest_cursor)
                        .unwrap_or(usize::MAX),
                });
            }
            for row in &rows {
                plan.delete(
                    MetadataFamily::ArtifactManifest,
                    row.key.clone(),
                    row.value.clone(),
                )?;
            }
            next_operation.cleanup_manifest_cursor = next_operation
                .cleanup_manifest_cursor
                .checked_add(u32::try_from(rows.len()).map_err(|_| {
                    PublicationError::BatchTooLarge {
                        batch: "cleanup manifest",
                        count: rows.len(),
                        max: MAX_PUBLICATION_BATCH_ROWS,
                    }
                })?)
                .ok_or(PublicationError::BatchExceedsPlannedCount {
                    batch: "cleanup manifest",
                    cursor: next_operation.cleanup_manifest_cursor,
                    count: rows.len(),
                    planned: next_operation.manifest_cursor,
                })?;
        }

        let expected_payload = request.expected_operation.encode()?;
        let next_payload = next_operation.encode()?;
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        let result = self.execute_plan(
            request.context,
            plan,
            next_payload,
            PublicationAuthorityPurpose::Cleanup,
        )?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    /// Remove one bounded page of a quarantined operation's durable staging
    /// rows under an explicit operator verdict. Staged-object rows are removed
    /// first, exactly like aborted cleanup; manifest pages follow. The phase
    /// stays `Quarantined` so an abandoned reconciliation remains fail-closed
    /// and resumable, and every batch re-pins the verdict's revision predicate.
    pub fn reconcile_quarantined_publish_batch(
        &self,
        request: ReconcileQuarantinedPublishBatchRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        require_operation_phase(&request.expected_operation, PublishPhase::Quarantined)?;

        let mut next_operation = request.expected_operation.clone();
        let mut plan = CommandPlan::default();
        self.predicate_reconcile_revision_state(
            request.context,
            &next_operation,
            request.resolution,
            &mut plan,
        )?;
        if next_operation.cleanup_staged_object_cursor < next_operation.staged_object_cursor {
            validate_batch_size("reconcile staged objects", request.staged_object_rows.len())?;
            let next_cursor = checked_batch_cursor(
                "reconcile staged objects",
                next_operation.cleanup_staged_object_cursor,
                request.staged_object_rows.len(),
                next_operation.staged_object_cursor,
            )?;
            for (offset, expected) in request.staged_object_rows.iter().enumerate() {
                expected.validate()?;
                let sequence = next_operation
                    .cleanup_staged_object_cursor
                    .checked_add(u32::try_from(offset).map_err(|_| {
                        PublicationError::BatchTooLarge {
                            batch: "reconcile staged objects",
                            count: request.staged_object_rows.len(),
                            max: MAX_PUBLICATION_BATCH_ROWS,
                        }
                    })?)
                    .ok_or(PublicationError::BatchExceedsPlannedCount {
                        batch: "reconcile staged objects",
                        cursor: next_operation.cleanup_staged_object_cursor,
                        count: request.staged_object_rows.len(),
                        planned: next_operation.staged_object_cursor,
                    })?;
                if expected.object_sequence != sequence {
                    return Err(PublicationError::StagedObjectSequenceMismatch {
                        expected: sequence,
                        actual: expected.object_sequence,
                    });
                }
                if expected.artifact_revision_id != next_operation.artifact_revision_id {
                    return Err(PublicationError::StagedObjectRevisionMismatch);
                }
                plan.delete(
                    MetadataFamily::StagedObject,
                    staged_object_key(
                        request.context.root_id,
                        next_operation.operation_id,
                        u64::from(sequence),
                    ),
                    expected.encode()?,
                )?;
            }
            next_operation.cleanup_staged_object_cursor = next_cursor;
        } else {
            if !request.staged_object_rows.is_empty() {
                return Err(PublicationError::BatchExceedsPlannedCount {
                    batch: "reconcile staged objects",
                    cursor: next_operation.cleanup_staged_object_cursor,
                    count: request.staged_object_rows.len(),
                    planned: next_operation.staged_object_cursor,
                });
            }
            let remaining = next_operation
                .manifest_cursor
                .checked_sub(next_operation.cleanup_manifest_cursor)
                .expect("operation validation orders manifest cleanup cursors");
            if remaining == 0 {
                return Err(PublicationError::EmptyBatch {
                    batch: "reconcile manifest",
                });
            }
            // Under a published revision the manifest prefix holds the
            // published revision's live manifest. This operation legally never
            // retains un-swept manifest rows in that case, so a remainder
            // means the invariant derivation is wrong somewhere: stop loudly
            // instead of deleting published data.
            if matches!(
                request.resolution,
                QuarantineReconcileResolution::RevisionPublished
            ) {
                return Err(PublicationError::ReconcileManifestRowsRemain { remaining });
            }
            let limit = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(MAX_PUBLICATION_BATCH_ROWS);
            let prefix = artifact_manifest_prefix(
                request.context.root_id,
                next_operation.artifact_revision_id,
            );
            let rows = self.store.scan_prefix_at(
                request.context.root_id,
                request.context.placement_generation,
                request.context.owner_epoch,
                MetadataFamily::ArtifactManifest,
                &prefix,
                request.context.read_version,
                None,
                limit,
            )?;
            if rows.is_empty() {
                return Err(PublicationError::ManifestCountMismatch {
                    expected: next_operation.manifest_cursor,
                    actual: usize::try_from(next_operation.cleanup_manifest_cursor)
                        .unwrap_or(usize::MAX),
                });
            }
            for row in &rows {
                plan.delete(
                    MetadataFamily::ArtifactManifest,
                    row.key.clone(),
                    row.value.clone(),
                )?;
            }
            next_operation.cleanup_manifest_cursor = next_operation
                .cleanup_manifest_cursor
                .checked_add(u32::try_from(rows.len()).map_err(|_| {
                    PublicationError::BatchTooLarge {
                        batch: "reconcile manifest",
                        count: rows.len(),
                        max: MAX_PUBLICATION_BATCH_ROWS,
                    }
                })?)
                .ok_or(PublicationError::BatchExceedsPlannedCount {
                    batch: "reconcile manifest",
                    cursor: next_operation.cleanup_manifest_cursor,
                    count: rows.len(),
                    planned: next_operation.manifest_cursor,
                })?;
        }

        let expected_payload = request.expected_operation.encode()?;
        let next_payload = next_operation.encode()?;
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        let result = self.execute_reconcile_plan(request.context, plan, next_payload)?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    /// Atomically resolve one fully-swept quarantined operation: CAS the
    /// operator terminal error in, transition `Quarantined -> Cleaned`, and
    /// release the revision claim when this operation owns it. The verdict's
    /// revision predicate is pinned by the same command.
    pub fn finish_reconcile_quarantined_publish(
        &self,
        request: FinishReconcileQuarantinedPublishRequest,
    ) -> Result<PublishCommandOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        self.require_current_operation(request.context, &request.expected_operation)?;
        require_operation_phase(&request.expected_operation, PublishPhase::Quarantined)?;

        let mut plan = CommandPlan::default();
        self.predicate_reconcile_revision_state(
            request.context,
            &request.expected_operation,
            request.resolution,
            &mut plan,
        )?;
        let original_evidence_digest = request
            .expected_operation
            .terminal_error
            .as_ref()
            .and_then(|error| error.evidence_digest);
        let terminal_error = PublishTerminalError {
            kind: PublishTerminalErrorKind::OperatorReconciled,
            message: reconcile_terminal_message(request.resolution, &request.reason),
            evidence_digest: Some(reconcile_evidence_digest(
                request.resolution,
                original_evidence_digest,
                request.operator_evidence_digest,
            )),
        };
        let mut next_operation = request.expected_operation.clone();
        next_operation.reconcile_quarantine(terminal_error)?;

        let expected_payload = request.expected_operation.encode()?;
        let next_payload = next_operation.encode()?;
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                next_operation.operation_id,
            ),
            expected_payload,
            next_payload.clone(),
        )?;
        // Reconciliation is the operator path the quarantine fail-closed
        // default was waiting for: with the staged liability durably resolved,
        // the revision identity becomes claimable again in the same command.
        self.release_revision_claim(
            request.context,
            request.expected_operation.artifact_revision_id,
            request.expected_operation.operation_id,
            &mut plan,
        )?;
        let result = self.execute_reconcile_plan(request.context, plan, next_payload)?;
        decode_operation_outcome(result, next_operation.operation_id)
    }

    /// Pin the authoritative revision row to the operator's verdict, refusing
    /// loudly on contradiction. The predicate makes the verdict atomic with
    /// every reconciliation mutation.
    fn predicate_reconcile_revision_state(
        &self,
        context: PublicationContext,
        operation: &PublishOperationRecord,
        resolution: QuarantineReconcileResolution,
        plan: &mut CommandPlan,
    ) -> Result<(), PublicationError> {
        let revision_key = artifact_revision_key(context.root_id, operation.artifact_revision_id);
        let payload =
            self.read_payload(context, MetadataFamily::ArtifactRevision, &revision_key)?;
        match (resolution, payload) {
            (QuarantineReconcileResolution::RevisionUnpublished, None) => {
                plan.assert_value(MetadataFamily::ArtifactRevision, revision_key, None)?;
            }
            (QuarantineReconcileResolution::RevisionPublished, Some(payload)) => {
                plan.assert_value(
                    MetadataFamily::ArtifactRevision,
                    revision_key,
                    Some(payload),
                )?;
            }
            (resolution, payload) => {
                return Err(PublicationError::ReconcileResolutionMismatch {
                    resolution,
                    revision_published: payload.is_some(),
                });
            }
        }
        Ok(())
    }

    fn load_restore_publication_authority(
        &self,
        context: PublicationContext,
        operation: &PublishOperationRecord,
        restore_operation_id: OperationId,
    ) -> Result<
        (
            Vec<u8>,
            Vec<u8>,
            RestoreOperationRecord,
            RestoreManifestKind,
        ),
        PublicationError,
    > {
        let kind = restore_manifest_kind(&operation.path)?;
        let empty_dependency_digest = dependency_owner_digest(&[])?;
        if !matches!(operation.claim, PublishClaim::CreateOnly)
            || operation.dependency_count != 0
            || operation.dependency_depth != 0
            || operation.dependency_digest != empty_dependency_digest
        {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        let restore_key = operation_key(
            context.root_id,
            OperationKind::Restore,
            restore_operation_id,
        );
        let restore_payload = self
            .store
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::Operation,
                &restore_key,
                context.read_version,
            )?
            .ok_or(PublicationError::RestoreOperationMissing)?;
        let restore = RestoreOperationRecord::decode(&restore_payload)?;
        if restore.operation_id != restore_operation_id
            || restore.destination_workbench_id != operation.workbench_id
            || restore.destination_workspace_incarnation_id != operation.workspace_incarnation_id
        {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        let binding = restore_destination_binding(&restore)?;
        let expected_identity = expected_restore_manifest_identity(binding, kind);
        if expected_identity.publication_operation_id != operation.operation_id
            || expected_identity.artifact_revision_id != operation.artifact_revision_id
        {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        Ok((restore_key, restore_payload, restore, kind))
    }

    fn validate_restore_workspace_state(
        workspace: &WorkspaceRecord,
        restore: &RestoreOperationRecord,
    ) -> Result<(), PublicationError> {
        let hidden = matches!(
            restore.phase,
            RestorePhase::SourceSealed
                | RestorePhase::DestinationBuilding
                | RestorePhase::DestinationSealing
                | RestorePhase::Ready
        );
        let authorized = if hidden {
            workspace.state == WorkspaceState::Staging
                && workspace.owning_operation_id == Some(restore.operation_id)
        } else if restore.phase == RestorePhase::Complete {
            workspace.state == WorkspaceState::Visible && workspace.owning_operation_id.is_none()
        } else {
            false
        };
        if authorized {
            Ok(())
        } else {
            Err(PublicationError::RestoreAuthorityMismatch)
        }
    }

    fn validate_restore_terminal_publication_replay(
        &self,
        context: PublicationContext,
        operation: &PublishOperationRecord,
        restore: &RestoreOperationRecord,
        kind: RestoreManifestKind,
    ) -> Result<(), PublicationError> {
        if operation.phase != PublishPhase::Published {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        let result = operation
            .result
            .as_ref()
            .ok_or(PublicationError::ReplayResultMismatch)?;
        let binding = restore_destination_binding(restore)?;
        match restore.phase {
            RestorePhase::SourceSealed => {
                if binding.manifests.is_some() {
                    return Err(PublicationError::RestoreAuthorityMismatch);
                }
            }
            RestorePhase::DestinationBuilding
            | RestorePhase::DestinationSealing
            | RestorePhase::Ready
            | RestorePhase::Complete => {
                let actual = actual_restore_manifest_publication(binding, kind)
                    .ok_or(PublicationError::RestoreAuthorityMismatch)?;
                if actual.publication_operation_id != operation.operation_id
                    || actual.workspace_incarnation_id != operation.workspace_incarnation_id
                    || actual.artifact_revision_id != operation.artifact_revision_id
                    || actual.body_digest_uri != result.body_digest_uri
                    || actual.manifest_digest_uri != sha256_digest_uri(operation.manifest_seal)
                    || actual.logical_size != result.logical_size
                    || actual.content_type != RESTORE_MANIFEST_CONTENT_TYPE
                {
                    return Err(PublicationError::RestoreAuthorityMismatch);
                }
            }
            _ => return Err(PublicationError::RestoreAuthorityMismatch),
        }

        let path_key = path_current_key(
            context.root_id,
            operation.workspace_incarnation_id,
            &operation.path,
        );
        let path = self
            .read_publication_record(
                context,
                MetadataFamily::PathCurrent,
                &path_key,
                PathEntry::decode,
            )?
            .ok_or(PublicationError::RestoreAuthorityMismatch)?;
        let expected_manifest_digest_uri = sha256_digest_uri(operation.manifest_seal);
        if path.record.generation != result.path_generation
            || path.record.artifact_revision_id != operation.artifact_revision_id
            || path.record.body_digest_uri != result.body_digest_uri
            || path.record.manifest_digest_uri != expected_manifest_digest_uri
            || path.record.logical_size != result.logical_size
            || path.record.dependency_count != 0
            || path.record.dependency_depth != 0
            || path.record.content_type != RESTORE_MANIFEST_CONTENT_TYPE
            || path.record.producer.is_some()
            || path.record.manifest_id.is_some()
            || !TypedProjection::decode_stored(&path.record.typed_index_projection)?
                .fields()
                .is_empty()
        {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        if kind == RestoreManifestKind::Restore
            && (path.record.body_digest_uri != restore.restore_manifest.body_digest_uri
                || path.record.logical_size != restore.restore_manifest.logical_size
                || path.record.content_type != restore.restore_manifest.content_type)
        {
            return Err(PublicationError::RestoreManifestClosureMismatch);
        }

        let revision_key = artifact_revision_key(context.root_id, operation.artifact_revision_id);
        let revision = self
            .read_publication_record(
                context,
                MetadataFamily::ArtifactRevision,
                &revision_key,
                ArtifactRevisionRecord::decode,
            )?
            .ok_or(PublicationError::RestoreAuthorityMismatch)?;
        if revision.record.logical_size != path.record.logical_size
            || revision.record.body_digest_uri != path.record.body_digest_uri
            || revision.record.manifest_digest_uri != path.record.manifest_digest_uri
            || revision.record.block_count != u64::from(operation.manifest_row_count)
            || revision.record.dependency_count != 0
            || revision.record.dependency_depth != 0
            || revision.record.content_type != path.record.content_type
            || revision.record.state != RevisionState::Available
            || revision.record.strong_reference_count == 0
        {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        let path_ref_key = path_revision_ref_key(
            context.root_id,
            operation.workspace_incarnation_id,
            &operation.path,
            operation.artifact_revision_id,
        );
        let path_ref = self
            .read_publication_record(
                context,
                MetadataFamily::RevisionRef,
                &path_ref_key,
                RevisionRefRecord::decode,
            )?
            .ok_or(PublicationError::RestoreAuthorityMismatch)?;
        if path_ref.record.reference_epoch_at_add > revision.record.reference_epoch {
            return Err(PublicationError::RestoreAuthorityMismatch);
        }
        Ok(())
    }

    fn validate_existing_publish_replay_authority(
        &self,
        context: PublicationContext,
        operation: &PublishOperationRecord,
    ) -> Result<(), PublicationError> {
        let PublishAuthority::RestoreStaging {
            restore_operation_id,
        } = operation.authority
        else {
            return Ok(());
        };
        self.require_current_restore_replay_version(context)?;
        let (_, _, restore, kind) =
            self.load_restore_publication_authority(context, operation, restore_operation_id)?;
        let workspace_key = workspace_current_key(context.root_id, &operation.workbench_id);
        let workspace = self
            .read_publication_record(
                context,
                MetadataFamily::WorkspaceCurrent,
                &workspace_key,
                WorkspaceRecord::decode,
            )?
            .ok_or(PublicationError::WorkspaceNotFound)?;
        if workspace.record.incarnation_id != operation.workspace_incarnation_id {
            return Err(PublicationError::WorkspaceIncarnationMismatch);
        }
        Self::validate_restore_workspace_state(&workspace.record, &restore)?;
        let replay = match operation.phase {
            PublishPhase::Published => self
                .validate_restore_terminal_publication_replay(context, operation, &restore, kind),
            PublishPhase::Uploading | PublishPhase::Finalizing
                if restore.phase == RestorePhase::SourceSealed
                    && restore_destination_binding(&restore)?.manifests.is_none() =>
            {
                Ok(())
            }
            PublishPhase::Uploading
            | PublishPhase::Finalizing
            | PublishPhase::Aborting
            | PublishPhase::Cleaning
            | PublishPhase::Cleaned
            | PublishPhase::Quarantined => Err(PublicationError::RestoreAuthorityMismatch),
        };
        replay?;
        // This read-only replay path has no MetadataCommand CAS. Re-read the
        // commit clock so a concurrent cleanup cannot make a historical
        // SourceSealed/path snapshot look like live success.
        self.require_current_restore_replay_version(context)
    }

    fn require_current_restore_replay_version(
        &self,
        context: PublicationContext,
    ) -> Result<(), PublicationError> {
        let current = self.store.current_read_version()?;
        if current == context.read_version {
            Ok(())
        } else {
            Err(MetaError::WriteReadVersionMismatch {
                requested: context.read_version.get(),
                current: current.get(),
            }
            .into())
        }
    }

    /// Execute a reconciliation plan without publication-authority predicates.
    ///
    /// A quarantined operation's workspace incarnation, commit build, or
    /// restore may be long retired, so `predicate_publication_authority` would
    /// wrongly make such operations unresolvable. Operator reconciliation is
    /// fenced by the exact operation-row CAS, the verdict's revision-row
    /// predicate, and the active root fence instead.
    fn execute_reconcile_plan(
        &self,
        context: PublicationContext,
        plan: CommandPlan,
        deterministic_result: Vec<u8>,
    ) -> Result<MetadataCommandResult, PublicationError> {
        plan.validate_bounds()?;
        let command = MetadataCommand {
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
            predicates: plan.predicates,
            mutations: plan.mutations,
            history_projection: plan.history,
            event_projection: plan.events,
            deterministic_result,
        }
        .seal();
        self.store.execute(&command).map_err(Into::into)
    }

    fn predicate_publication_authority(
        &self,
        context: PublicationContext,
        operation: &PublishOperationRecord,
        purpose: PublicationAuthorityPurpose,
        plan: &mut CommandPlan,
    ) -> Result<(), PublicationError> {
        let workspace_key = workspace_current_key(context.root_id, &operation.workbench_id);
        let workspace = self
            .read_publication_record(
                context,
                MetadataFamily::WorkspaceCurrent,
                &workspace_key,
                WorkspaceRecord::decode,
            )?
            .ok_or(PublicationError::WorkspaceNotFound)?;
        if workspace.record.incarnation_id != operation.workspace_incarnation_id {
            return Err(PublicationError::WorkspaceIncarnationMismatch);
        }
        match operation.authority {
            PublishAuthority::Visible => {
                if matches!(
                    operation.path.as_str(),
                    RUN_MANIFEST_PATH | RESTORE_MANIFEST_PATH
                ) {
                    return Err(PublicationError::ReservedManifestAuthorityRequired);
                }
                if workspace.record.state != WorkspaceState::Visible
                    || workspace.record.owning_operation_id.is_some()
                {
                    return Err(PublicationError::WorkspaceUnavailable);
                }
            }
            PublishAuthority::CommitStaging {
                commit_operation_id,
            } => {
                if operation.path.as_str() != RUN_MANIFEST_PATH
                    || matches!(operation.claim, PublishClaim::Append { .. })
                    || operation.dependency_count != 0
                    || operation.dependency_depth != 0
                {
                    return Err(PublicationError::CommitAuthorityMismatch);
                }
                if workspace.record.state != WorkspaceState::Visible
                    || workspace.record.owning_operation_id.is_some()
                {
                    return Err(PublicationError::WorkspaceUnavailable);
                }
                let commit_key = operation_key(
                    context.root_id,
                    OperationKind::BuildCommit,
                    commit_operation_id,
                );
                let commit_payload = self
                    .store
                    .read_at(
                        context.root_id,
                        context.placement_generation,
                        context.owner_epoch,
                        MetadataFamily::Operation,
                        &commit_key,
                        context.read_version,
                    )?
                    .ok_or(PublicationError::CommitOperationMissing)?;
                let commit = BuildCommitOperationRecord::decode(&commit_payload)?;
                if commit.phase != BuildCommitPhase::Building
                    || commit.operation_id != commit_operation_id
                    || commit.workbench_id != operation.workbench_id
                    || commit.source_workspace_incarnation_id != operation.workspace_incarnation_id
                    || commit.tree_manifest_revision_id != operation.artifact_revision_id
                    || commit.commit_staged_run_manifest.is_some()
                    || !commit_manifest_claim_matches(
                        commit.run_manifest_condition,
                        &operation.claim,
                    )
                    || commit.member_count != 0
                    || commit.member_cursor.is_some()
                {
                    return Err(PublicationError::CommitAuthorityMismatch);
                }
                if !plan
                    .exact_keys
                    .contains(&(MetadataFamily::Operation, commit_key.clone()))
                {
                    plan.assert_value(MetadataFamily::Operation, commit_key, Some(commit_payload))?;
                }
            }
            PublishAuthority::RestoreStaging {
                restore_operation_id,
            } => {
                if workspace.record.state != WorkspaceState::Staging
                    || workspace.record.owning_operation_id != Some(restore_operation_id)
                {
                    return Err(PublicationError::RestoreAuthorityMismatch);
                }
                let (restore_key, restore_payload, restore, _) = self
                    .load_restore_publication_authority(context, operation, restore_operation_id)?;
                let binding = restore_destination_binding(&restore)?;
                match purpose {
                    PublicationAuthorityPurpose::Forward => {
                        if restore.phase != RestorePhase::SourceSealed
                            || binding.manifests.is_some()
                        {
                            return Err(PublicationError::RestoreAuthorityMismatch);
                        }
                    }
                    PublicationAuthorityPurpose::Cleanup => {
                        if !matches!(
                            restore.phase,
                            RestorePhase::Aborting | RestorePhase::Cleaning
                        ) || binding.manifests.is_some()
                            || !matches!(
                                operation.phase,
                                PublishPhase::Aborting
                                    | PublishPhase::Cleaning
                                    | PublishPhase::Cleaned
                                    | PublishPhase::Quarantined
                            )
                        {
                            return Err(PublicationError::RestoreAuthorityMismatch);
                        }
                        // A partial publisher may shed only its private
                        // staging liability. Any visible path or available
                        // revision proves publication won and must instead be
                        // cleaned by the owning restore lifecycle.
                        plan.assert_value(
                            MetadataFamily::PathCurrent,
                            path_current_key(
                                context.root_id,
                                operation.workspace_incarnation_id,
                                &operation.path,
                            ),
                            None,
                        )?;
                        plan.assert_value(
                            MetadataFamily::ArtifactRevision,
                            artifact_revision_key(context.root_id, operation.artifact_revision_id),
                            None,
                        )?;
                    }
                }
                if !plan
                    .exact_keys
                    .contains(&(MetadataFamily::Operation, restore_key.clone()))
                {
                    plan.assert_value(
                        MetadataFamily::Operation,
                        restore_key,
                        Some(restore_payload),
                    )?;
                }
            }
        }
        if !plan
            .exact_keys
            .contains(&(MetadataFamily::WorkspaceCurrent, workspace_key.clone()))
        {
            plan.assert_value(
                MetadataFamily::WorkspaceCurrent,
                workspace_key,
                Some(workspace.payload),
            )?;
        }
        Ok(())
    }

    /// Release the operation's in-flight revision claim inside `plan`.
    ///
    /// An absent claim is tolerated so operations begun before claims existed
    /// still finalize and clean up; a claim held by a different operation is
    /// left untouched because it guards that operation's own lifecycle. A
    /// quarantined operation deliberately keeps its claim: its provider-side
    /// object state is unresolved, so the revision identity stays fail-closed
    /// until `finish_reconcile_quarantined_publish` releases it under an
    /// operator verdict.
    fn release_revision_claim(
        &self,
        context: PublicationContext,
        revision: ArtifactRevisionId,
        operation_id: OperationId,
        plan: &mut CommandPlan,
    ) -> Result<(), PublicationError> {
        let claim_key = artifact_revision_claim_key(context.root_id, revision);
        if let Some(payload) =
            self.read_payload(context, MetadataFamily::ArtifactRevision, &claim_key)?
        {
            let claim = ArtifactRevisionClaimRecord::decode(&payload)?;
            if claim.operation_id == operation_id {
                plan.delete(MetadataFamily::ArtifactRevision, claim_key, payload)?;
            }
        }
        Ok(())
    }

    fn execute_plan(
        &self,
        context: PublicationContext,
        plan: CommandPlan,
        deterministic_result: Vec<u8>,
        authority_purpose: PublicationAuthorityPurpose,
    ) -> Result<MetadataCommandResult, PublicationError> {
        let command = self.seal_plan(context, plan, deterministic_result, authority_purpose)?;
        self.store.execute(&command).map_err(Into::into)
    }

    fn execute_plan_before_lease_deadline(
        &self,
        context: PublicationContext,
        plan: CommandPlan,
        deterministic_result: Vec<u8>,
        activity_deadline_ms: u64,
        authority_purpose: PublicationAuthorityPurpose,
    ) -> Result<MetadataCommandResult, PublicationError> {
        let command = self.seal_plan(context, plan, deterministic_result, authority_purpose)?;
        match self
            .store
            .execute_before_lease_deadline(&command, activity_deadline_ms)
        {
            Ok(result) => Ok(result),
            Err(MetaError::LeaseDeadlineReached {
                lease_clock_ms,
                requested_deadline_ms,
            }) => Err(PublicationError::ActivityDeadlineNotFuture {
                clock: lease_clock_ms,
                requested: requested_deadline_ms,
            }),
            Err(error) => Err(error.into()),
        }
    }

    fn seal_plan(
        &self,
        context: PublicationContext,
        mut plan: CommandPlan,
        deterministic_result: Vec<u8>,
        authority_purpose: PublicationAuthorityPurpose,
    ) -> Result<MetadataCommand, PublicationError> {
        let operation = PublishOperationRecord::decode(&deterministic_result)?;
        self.predicate_publication_authority(context, &operation, authority_purpose, &mut plan)?;
        plan.validate_bounds()?;
        Ok(MetadataCommand {
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
            predicates: plan.predicates,
            mutations: plan.mutations,
            history_projection: plan.history,
            event_projection: plan.events,
            deterministic_result,
        }
        .seal())
    }

    /// Establish operation-state precedence over contextual authority and row
    /// validation at one exact read version.
    fn require_current_operation(
        &self,
        context: PublicationContext,
        expected: &PublishOperationRecord,
    ) -> Result<(), PublicationError> {
        let key = operation_key(
            context.root_id,
            OperationKind::Publish,
            expected.operation_id,
        );
        let current = self.read_payload(context, MetadataFamily::Operation, &key)?;
        let expected_payload = expected.encode()?;
        if current.as_deref() != Some(expected_payload.as_slice()) {
            return Err(PublicationError::ConcurrentMutation);
        }
        Ok(())
    }

    fn read_publication_record<T>(
        &self,
        context: PublicationContext,
        family: MetadataFamily,
        key: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, PublicationRecordCodecError>,
    ) -> Result<Option<Loaded<T>>, PublicationError> {
        self.store
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                family,
                key,
                context.read_version,
            )?
            .map(|payload| {
                let record = decode(&payload)?;
                Ok(Loaded { payload, record })
            })
            .transpose()
    }

    pub fn finalize_publish(
        &self,
        request: FinalizePublishRequest,
    ) -> Result<FinalizePublishOutcome, PublicationError> {
        validate_operation_seals(&request.expected_operation)?;
        if request.expected_operation.phase != PublishPhase::Finalizing {
            return Err(PublicationError::InvalidOperationPhase {
                expected: PublishPhase::Finalizing,
                actual: request.expected_operation.phase,
            });
        }
        self.require_current_operation(request.context, &request.expected_operation)?;
        let expected_commit_version = CommitVersion::new(
            request
                .context
                .read_version
                .get()
                .checked_add(1)
                .ok_or(PublicationError::CommitVersionOverflow)?,
        )
        .map_err(|_| PublicationError::CommitVersionOverflow)?;

        validate_dependency_seal(
            &request.expected_operation,
            &request.dependency_owner_revision_ids,
        )?;
        if request
            .dependency_owner_revision_ids
            .binary_search(&request.expected_operation.artifact_revision_id)
            .is_ok()
        {
            return Err(PublicationError::RevisionIdentityCollision);
        }
        let expected_manifest_uri = sha256_digest_uri(request.expected_operation.manifest_seal);
        if request.artifact.manifest_digest_uri != expected_manifest_uri {
            return Err(PublicationError::ManifestDigestMismatch);
        }
        let sealed_logical_size =
            if let Some(last) = request.expected_operation.manifest_last_position {
                let key = artifact_manifest_key(
                    request.context.root_id,
                    request.expected_operation.artifact_revision_id,
                    last.object_index,
                );
                let payload = self
                    .read_payload(request.context, MetadataFamily::ArtifactManifest, &key)?
                    .ok_or(PublicationError::ManifestPositionMismatch)?;
                let last_row = ArtifactManifestRow::decode(&payload)?;
                last_row
                    .logical_offset
                    .checked_add(last_row.length)
                    .expect("validated manifest logical range cannot overflow")
            } else {
                0
            };
        if request.artifact.logical_size != sealed_logical_size {
            return Err(PublicationError::ManifestLogicalOffsetMismatch {
                expected: sealed_logical_size,
                actual: request.artifact.logical_size,
            });
        }
        let next_projection = TypedProjection::decode(&request.artifact.typed_index_projection)?;
        if let PublishAuthority::RestoreStaging {
            restore_operation_id,
        } = request.expected_operation.authority
        {
            let (_, _, restore, kind) = self.load_restore_publication_authority(
                request.context,
                &request.expected_operation,
                restore_operation_id,
            )?;
            if restore.phase != RestorePhase::SourceSealed
                || restore_destination_binding(&restore)?.manifests.is_some()
                || request.expected_operation.dependency_count != 0
                || request.expected_operation.dependency_depth != 0
                || request.artifact.logical_size == 0
                || request.artifact.content_type != RESTORE_MANIFEST_CONTENT_TYPE
                || request.artifact.producer.is_some()
                || request.artifact.manifest_id.is_some()
                || !next_projection.fields().is_empty()
                || (kind == RestoreManifestKind::Restore
                    && (request.artifact.logical_size != restore.restore_manifest.logical_size
                        || request.artifact.body_digest_uri
                            != restore.restore_manifest.body_digest_uri
                        || request.artifact.content_type != restore.restore_manifest.content_type))
            {
                return Err(PublicationError::RestoreManifestClosureMismatch);
            }
        }
        if matches!(
            request.expected_operation.authority,
            PublishAuthority::CommitStaging { .. }
        ) {
            return self.finalize_commit_staging_publish(request, next_projection);
        }

        let operation_key = operation_key(
            request.context.root_id,
            OperationKind::Publish,
            request.expected_operation.operation_id,
        );
        let expected_operation_payload = request.expected_operation.encode()?;
        let workspace_key = workspace_current_key(
            request.context.root_id,
            &request.expected_operation.workbench_id,
        );
        let workspace = self
            .read_publication_record(
                request.context,
                MetadataFamily::WorkspaceCurrent,
                &workspace_key,
                WorkspaceRecord::decode,
            )?
            .ok_or(PublicationError::WorkspaceNotFound)?;
        if workspace.record.incarnation_id != request.expected_operation.workspace_incarnation_id {
            return Err(PublicationError::WorkspaceIncarnationMismatch);
        }
        let (next_workspace_revision, next_workspace) = match request.expected_operation.authority {
            PublishAuthority::Visible => {
                if workspace.record.state != WorkspaceState::Visible
                    || workspace.record.owning_operation_id.is_some()
                {
                    return Err(PublicationError::WorkspaceUnavailable);
                }
                let revision = WorkspaceRevision::new(
                    workspace
                        .record
                        .workspace_revision
                        .get()
                        .checked_add(1)
                        .ok_or(PublicationError::WorkspaceRevisionOverflow)?,
                );
                (
                    revision,
                    Some(WorkspaceRecord {
                        workspace_revision: revision,
                        ..workspace.record
                    }),
                )
            }
            PublishAuthority::CommitStaging { .. } => {
                unreachable!("commit staging returns before visible path planning")
            }
            PublishAuthority::RestoreStaging {
                restore_operation_id,
            } => {
                if workspace.record.state != WorkspaceState::Staging
                    || workspace.record.owning_operation_id != Some(restore_operation_id)
                {
                    return Err(PublicationError::RestoreAuthorityMismatch);
                }
                // Restore staging does not mutate WorkspaceCurrent. Persist
                // the exact revision that complete_restore will install so
                // the terminal publish result remains both non-zero and
                // replay-stable.
                let revision = WorkspaceRevision::new(
                    workspace
                        .record
                        .workspace_revision
                        .get()
                        .checked_add(1)
                        .ok_or(PublicationError::WorkspaceRevisionOverflow)?,
                );
                (revision, None)
            }
        };

        let path_key = path_current_key(
            request.context.root_id,
            request.expected_operation.workspace_incarnation_id,
            &request.expected_operation.path,
        );
        let current_path = self.read_publication_record(
            request.context,
            MetadataFamily::PathCurrent,
            &path_key,
            PathEntry::decode,
        )?;
        let current_projection = current_path
            .as_ref()
            .map(|current| TypedProjection::decode_stored(&current.record.typed_index_projection))
            .transpose()?;
        let next_path_generation =
            validate_path_claim(&request.expected_operation.claim, current_path.as_ref())?;
        let mut current_index_rows = match (&current_path, &current_projection) {
            (Some(current), Some(projection)) => secondary_index_rows(
                request.context.root_id,
                request.expected_operation.workspace_incarnation_id,
                &request.expected_operation.path,
                current.record.generation,
                projection,
            )?,
            (None, None) => BTreeMap::new(),
            _ => unreachable!("path and decoded projection presence agree"),
        };
        let mut next_index_rows = secondary_index_rows(
            request.context.root_id,
            request.expected_operation.workspace_incarnation_id,
            &request.expected_operation.path,
            next_path_generation,
            &next_projection,
        )?;

        let revision_key = artifact_revision_key(
            request.context.root_id,
            request.expected_operation.artifact_revision_id,
        );
        if self
            .read_payload(
                request.context,
                MetadataFamily::ArtifactRevision,
                &revision_key,
            )?
            .is_some()
        {
            return Err(PublicationError::RevisionAlreadyExists);
        }

        let mut owner_records =
            BTreeMap::<ArtifactRevisionId, Loaded<ArtifactRevisionRecord>>::new();
        for owner in &request.dependency_owner_revision_ids {
            let loaded = self.load_available_revision(request.context, *owner)?;
            owner_records.insert(*owner, loaded);
        }
        let derived_dependency_depth = if owner_records.is_empty() {
            0
        } else {
            owner_records
                .values()
                .map(|loaded| loaded.record.dependency_depth)
                .max()
                .expect("nonempty dependency set has a maximum")
                .checked_add(1)
                .ok_or(PublicationError::DependencyDepthMismatch {
                    expected: request.expected_operation.dependency_depth,
                    actual: u8::MAX,
                })?
        };
        if derived_dependency_depth != request.expected_operation.dependency_depth {
            return Err(PublicationError::DependencyDepthMismatch {
                expected: request.expected_operation.dependency_depth,
                actual: derived_dependency_depth,
            });
        }
        let old_path_revision = current_path
            .as_ref()
            .map(|loaded| loaded.record.artifact_revision_id);
        if old_path_revision == Some(request.expected_operation.artifact_revision_id) {
            return Err(PublicationError::RevisionIdentityCollision);
        }
        if let Some(old_revision) = old_path_revision {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                owner_records.entry(old_revision)
            {
                entry.insert(self.load_available_revision(request.context, old_revision)?);
            }
        }

        let mut old_path_ref = None;
        if let Some(old_revision) = old_path_revision {
            let key = path_revision_ref_key(
                request.context.root_id,
                request.expected_operation.workspace_incarnation_id,
                &request.expected_operation.path,
                old_revision,
            );
            let loaded = self
                .read_publication_record(
                    request.context,
                    MetadataFamily::RevisionRef,
                    &key,
                    RevisionRefRecord::decode,
                )?
                .ok_or(PublicationError::RevisionReferenceMissing)?;
            let owner = owner_records
                .get(&old_revision)
                .expect("old path revision was loaded");
            if loaded.record.reference_epoch_at_add > owner.record.reference_epoch {
                return Err(PublicationError::RevisionReferenceEpochAhead);
            }
            old_path_ref = Some((key, loaded));
        }

        let mut owner_updates = BTreeMap::<ArtifactRevisionId, OwnerRevisionUpdate>::new();
        for (revision, loaded) in owner_records {
            let add_dependency = request
                .dependency_owner_revision_ids
                .binary_search(&revision)
                .is_ok();
            let remove_path = old_path_revision == Some(revision);
            let new_epoch_raw = loaded
                .record
                .reference_epoch
                .get()
                .checked_add(1)
                .ok_or(PublicationError::ReferenceEpochOverflow { revision })?;
            let new_epoch = ReferenceEpoch::new(new_epoch_raw);
            let with_add = if add_dependency {
                loaded
                    .record
                    .strong_reference_count
                    .checked_add(1)
                    .ok_or(PublicationError::ReferenceCountOverflow { revision })?
            } else {
                loaded.record.strong_reference_count
            };
            let new_count = if remove_path {
                with_add
                    .checked_sub(1)
                    .ok_or(PublicationError::ReferenceCountUnderflow { revision })?
            } else {
                with_add
            };
            let mut next = loaded.record.clone();
            next.reference_epoch = new_epoch;
            next.strong_reference_count = new_count;
            next.last_zero_ref_version = (new_count == 0).then_some(expected_commit_version);
            owner_updates.insert(
                revision,
                OwnerRevisionUpdate {
                    loaded,
                    next,
                    add_dependency,
                },
            );
        }

        let new_revision = ArtifactRevisionRecord {
            logical_size: request.artifact.logical_size,
            body_digest_uri: request.artifact.body_digest_uri.clone(),
            manifest_digest_uri: request.artifact.manifest_digest_uri.clone(),
            block_count: u64::from(request.expected_operation.manifest_row_count),
            dependency_count: u32::from(request.expected_operation.dependency_count),
            dependency_depth: request.expected_operation.dependency_depth,
            dependency_digest: request.expected_operation.dependency_digest,
            content_type: request.artifact.content_type.clone(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        };
        let next_path = PathEntry {
            generation: next_path_generation,
            artifact_revision_id: request.expected_operation.artifact_revision_id,
            body_digest_uri: request.artifact.body_digest_uri.clone(),
            manifest_digest_uri: request.artifact.manifest_digest_uri.clone(),
            logical_size: request.artifact.logical_size,
            dependency_count: u32::from(request.expected_operation.dependency_count),
            dependency_depth: request.expected_operation.dependency_depth,
            content_type: request.artifact.content_type.clone(),
            producer: request.artifact.producer.clone(),
            manifest_id: request.artifact.manifest_id.clone(),
            typed_index_projection: request.artifact.typed_index_projection.clone(),
        };
        let publish_result = PublishResult {
            path_generation: next_path_generation,
            workspace_revision: next_workspace_revision,
            logical_size: request.artifact.logical_size,
            body_digest_uri: request.artifact.body_digest_uri.clone(),
        };
        let mut published_operation = request.expected_operation.clone();
        published_operation.apply_transition(
            PublishPhase::Finalizing,
            PublishTransition::Publish {
                result: publish_result.clone(),
            },
        )?;
        let published_operation_payload = published_operation.encode()?;

        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key,
            expected_operation_payload,
            published_operation_payload.clone(),
        )?;
        if let Some(next_workspace) = next_workspace {
            plan.replace(
                MetadataFamily::WorkspaceCurrent,
                workspace_key,
                workspace.payload,
                next_workspace.encode()?,
            )?;
        }
        match current_path {
            None => plan.put_absent(MetadataFamily::PathCurrent, path_key, next_path.encode()?)?,
            Some(current) => plan.replace(
                MetadataFamily::PathCurrent,
                path_key,
                current.payload,
                next_path.encode()?,
            )?,
        }
        let index_keys = current_index_rows
            .keys()
            .chain(next_index_rows.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in index_keys {
            match (
                current_index_rows.remove(&key),
                next_index_rows.remove(&key),
            ) {
                (Some(expected), Some(next)) if expected == next => {
                    plan.assert_value(MetadataFamily::SecondaryIndex, key, Some(expected))?;
                }
                (Some(expected), Some(next)) => {
                    plan.replace(MetadataFamily::SecondaryIndex, key, expected, next)?;
                }
                (Some(expected), None) => {
                    plan.delete(MetadataFamily::SecondaryIndex, key, expected)?;
                }
                (None, Some(next)) => {
                    plan.put_absent(MetadataFamily::SecondaryIndex, key, next)?;
                }
                (None, None) => unreachable!("secondary-index union key has one value"),
            }
        }
        plan.put_absent(
            MetadataFamily::ArtifactRevision,
            revision_key,
            new_revision.encode()?,
        )?;
        self.release_revision_claim(
            request.context,
            request.expected_operation.artifact_revision_id,
            request.expected_operation.operation_id,
            &mut plan,
        )?;
        let new_path_ref_key = path_revision_ref_key(
            request.context.root_id,
            request.expected_operation.workspace_incarnation_id,
            &request.expected_operation.path,
            request.expected_operation.artifact_revision_id,
        );
        plan.put_absent(
            MetadataFamily::RevisionRef,
            new_path_ref_key,
            RevisionRefRecord {
                reference_epoch_at_add: ReferenceEpoch::new(1),
            }
            .encode()?,
        )?;
        if let Some((key, loaded)) = old_path_ref {
            plan.delete(MetadataFamily::RevisionRef, key, loaded.payload)?;
        }
        for (revision, update) in owner_updates {
            let revision_key = artifact_revision_key(request.context.root_id, revision);
            plan.replace(
                MetadataFamily::ArtifactRevision,
                revision_key,
                update.loaded.payload,
                update.next.encode()?,
            )?;
            if update.add_dependency {
                plan.put_absent(
                    MetadataFamily::RevisionRef,
                    revision_dependency_ref_key(
                        request.context.root_id,
                        request.expected_operation.artifact_revision_id,
                        revision,
                    ),
                    RevisionRefRecord {
                        reference_epoch_at_add: update.next.reference_epoch,
                    }
                    .encode()?,
                )?;
            }
            if update.next.strong_reference_count == 0 {
                plan.put_absent(
                    MetadataFamily::GcCandidate,
                    gc_candidate_key(
                        request.context.root_id,
                        revision,
                        update.next.reference_epoch,
                    ),
                    GcCandidateRecord {
                        last_zero_ref_version: expected_commit_version,
                        claim_state: GcClaimState::Candidate,
                        retry_count: 0,
                        quarantine_evidence: None,
                    }
                    .encode()?,
                )?;
            }
        }
        if matches!(
            request.expected_operation.authority,
            PublishAuthority::Visible
        ) {
            plan.events
                .push(change_event_projection(&ChangeEventRecord {
                    workbench_id: request.expected_operation.workbench_id.clone(),
                    workspace_incarnation_id: request.expected_operation.workspace_incarnation_id,
                    kind: ChangeEventKind::ArtifactPublished,
                    artifact_revision_id: Some(request.expected_operation.artifact_revision_id),
                    commit_id: None,
                    operation_id: Some(request.expected_operation.operation_id),
                    path: Some(request.expected_operation.path.clone()),
                    before: current_projection.unwrap_or_default(),
                    after: next_projection,
                })?);
        }

        let result = self.execute_plan(
            request.context,
            plan,
            published_operation_payload,
            PublicationAuthorityPurpose::Forward,
        )?;
        let outcome = decode_operation_outcome(result, published_operation.operation_id)?;
        let result = outcome
            .operation
            .result
            .clone()
            .ok_or(PublicationError::ReplayResultMismatch)?;
        if outcome.operation.phase != PublishPhase::Published || result != publish_result {
            return Err(PublicationError::ReplayResultMismatch);
        }
        Ok(FinalizePublishOutcome {
            commit_version: outcome.commit_version,
            operation: outcome.operation,
            result,
            replayed: outcome.replayed,
        })
    }

    /// Finalize a run-manifest upload without publishing its canonical path.
    ///
    /// The immutable revision and its commit-owned strong ref are created in
    /// the same command that marks the publish operation successful and sets
    /// the build's durable manifest binding. `CommitService::finalize_build` later
    /// consumes this exact state and installs the path with the typed head.
    fn finalize_commit_staging_publish(
        &self,
        request: FinalizePublishRequest,
        projection: TypedProjection,
    ) -> Result<FinalizePublishOutcome, PublicationError> {
        let PublishAuthority::CommitStaging {
            commit_operation_id,
        } = request.expected_operation.authority
        else {
            unreachable!("commit staging helper is called only for commit authority")
        };
        if request.expected_operation.path.as_str() != RUN_MANIFEST_PATH
            || request.expected_operation.staged_object_count != 1
            || request.expected_operation.manifest_row_count != 1
            || request.expected_operation.dependency_count != 0
            || request.expected_operation.dependency_depth != 0
            || !request.dependency_owner_revision_ids.is_empty()
            || request.artifact.content_type != "application/json"
            || request.artifact.producer.is_some()
            || request.artifact.manifest_id.is_some()
            || !projection.fields().is_empty()
        {
            return Err(PublicationError::CommitManifestClosureMismatch);
        }

        let manifest_payload = self
            .read_payload(
                request.context,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_key(
                    request.context.root_id,
                    request.expected_operation.artifact_revision_id,
                    0,
                ),
            )?
            .ok_or(PublicationError::CommitManifestClosureMismatch)?;
        let manifest = ArtifactManifestRow::decode(&manifest_payload)?;
        let staged_payload = self
            .read_payload(
                request.context,
                MetadataFamily::StagedObject,
                &staged_object_key(
                    request.context.root_id,
                    request.expected_operation.operation_id,
                    0,
                ),
            )?
            .ok_or(PublicationError::CommitManifestClosureMismatch)?;
        let staged = StagedObjectRecord::decode(&staged_payload)?;
        if manifest.physical_owner_revision_id != request.expected_operation.artifact_revision_id
            || manifest.physical_object_index != 0
            || manifest.logical_offset != 0
            || manifest.offset != 0
            || manifest.length != request.artifact.logical_size
            || manifest.digest_uri != request.artifact.body_digest_uri
            || manifest.append_segment.is_some()
            || staged.object_sequence != 0
            || staged.artifact_revision_id != request.expected_operation.artifact_revision_id
            || staged.object_key != manifest.object_key
            || staged.expected_length != request.artifact.logical_size
            || staged.expected_digest_uri != request.artifact.body_digest_uri
            || staged.provider_state != StagedProviderState::Uploaded
            || staged.cleanup_state != StagedCleanupState::Owned
        {
            return Err(PublicationError::CommitManifestClosureMismatch);
        }

        let workspace_key = workspace_current_key(
            request.context.root_id,
            &request.expected_operation.workbench_id,
        );
        let workspace = self
            .read_publication_record(
                request.context,
                MetadataFamily::WorkspaceCurrent,
                &workspace_key,
                WorkspaceRecord::decode,
            )?
            .ok_or(PublicationError::WorkspaceNotFound)?;
        if workspace.record.incarnation_id != request.expected_operation.workspace_incarnation_id
            || workspace.record.state != WorkspaceState::Visible
            || workspace.record.owning_operation_id.is_some()
        {
            return Err(PublicationError::CommitAuthorityMismatch);
        }

        let path_key = path_current_key(
            request.context.root_id,
            request.expected_operation.workspace_incarnation_id,
            &request.expected_operation.path,
        );
        let current_path = self.read_publication_record(
            request.context,
            MetadataFamily::PathCurrent,
            &path_key,
            PathEntry::decode,
        )?;
        let path_generation =
            validate_path_claim(&request.expected_operation.claim, current_path.as_ref())?;

        let commit_key = operation_key(
            request.context.root_id,
            OperationKind::BuildCommit,
            commit_operation_id,
        );
        let commit_payload = self
            .read_payload(request.context, MetadataFamily::Operation, &commit_key)?
            .ok_or(PublicationError::CommitOperationMissing)?;
        let mut commit = BuildCommitOperationRecord::decode(&commit_payload)?;
        if commit.phase != BuildCommitPhase::Building
            || commit.operation_id != commit_operation_id
            || commit.workbench_id != request.expected_operation.workbench_id
            || commit.source_workspace_incarnation_id
                != request.expected_operation.workspace_incarnation_id
            || commit.tree_manifest_revision_id != request.expected_operation.artifact_revision_id
            || commit.commit_staged_run_manifest.is_some()
            || !commit_manifest_claim_matches(
                commit.run_manifest_condition,
                &request.expected_operation.claim,
            )
            || commit.member_count != 0
            || commit.member_cursor.is_some()
        {
            return Err(PublicationError::CommitAuthorityMismatch);
        }
        commit.commit_staged_run_manifest = Some(CommitManifestBinding {
            logical_size: request.artifact.logical_size,
            body_digest_uri: request.artifact.body_digest_uri.clone(),
            manifest_digest_uri: request.artifact.manifest_digest_uri.clone(),
            content_type: request.artifact.content_type.clone(),
        });
        commit.revision_ref_count = commit.revision_ref_count.checked_add(1).ok_or(
            PublicationError::ReferenceCountOverflow {
                revision: request.expected_operation.artifact_revision_id,
            },
        )?;

        let revision = ArtifactRevisionRecord {
            logical_size: request.artifact.logical_size,
            body_digest_uri: request.artifact.body_digest_uri.clone(),
            manifest_digest_uri: request.artifact.manifest_digest_uri.clone(),
            block_count: 1,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; SHA256_BYTES],
            content_type: request.artifact.content_type.clone(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        };
        let publish_result = PublishResult {
            path_generation,
            // Commit staging does not mutate WorkspaceCurrent. Persist the
            // exact revision that the later atomic path/head switch will
            // install so the terminal publish result remains both non-zero
            // and replay-stable.
            workspace_revision: WorkspaceRevision::new(
                workspace
                    .record
                    .workspace_revision
                    .get()
                    .checked_add(1)
                    .ok_or(PublicationError::WorkspaceRevisionOverflow)?,
            ),
            logical_size: request.artifact.logical_size,
            body_digest_uri: request.artifact.body_digest_uri.clone(),
        };
        let mut published_operation = request.expected_operation.clone();
        published_operation.apply_transition(
            PublishPhase::Finalizing,
            PublishTransition::Publish {
                result: publish_result.clone(),
            },
        )?;
        let published_payload = published_operation.encode()?;
        let expected_publish_payload = request.expected_operation.encode()?;

        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::Publish,
                request.expected_operation.operation_id,
            ),
            expected_publish_payload,
            published_payload.clone(),
        )?;
        plan.replace(
            MetadataFamily::Operation,
            commit_key,
            commit_payload,
            commit.encode()?,
        )?;
        plan.assert_value(
            MetadataFamily::PathCurrent,
            path_key,
            current_path.map(|loaded| loaded.payload),
        )?;
        plan.put_absent(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(
                request.context.root_id,
                request.expected_operation.artifact_revision_id,
            ),
            revision.encode()?,
        )?;
        self.release_revision_claim(
            request.context,
            request.expected_operation.artifact_revision_id,
            request.expected_operation.operation_id,
            &mut plan,
        )?;
        plan.put_absent(
            MetadataFamily::RevisionRef,
            commit_revision_ref_key(
                request.context.root_id,
                commit.commit_id,
                request.expected_operation.artifact_revision_id,
            ),
            RevisionRefRecord {
                reference_epoch_at_add: ReferenceEpoch::new(1),
            }
            .encode()?,
        )?;

        let result = self.execute_plan(
            request.context,
            plan,
            published_payload,
            PublicationAuthorityPurpose::Forward,
        )?;
        let outcome = decode_operation_outcome(result, published_operation.operation_id)?;
        let result = outcome
            .operation
            .result
            .clone()
            .ok_or(PublicationError::ReplayResultMismatch)?;
        if outcome.operation.phase != PublishPhase::Published || result != publish_result {
            return Err(PublicationError::ReplayResultMismatch);
        }
        Ok(FinalizePublishOutcome {
            commit_version: outcome.commit_version,
            operation: outcome.operation,
            result,
            replayed: outcome.replayed,
        })
    }

    fn read_payload(
        &self,
        context: PublicationContext,
        family: MetadataFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PublicationError> {
        self.store
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

    fn load_available_revision(
        &self,
        context: PublicationContext,
        revision: ArtifactRevisionId,
    ) -> Result<Loaded<ArtifactRevisionRecord>, PublicationError> {
        let key = artifact_revision_key(context.root_id, revision);
        let loaded = self
            .read_publication_record(
                context,
                MetadataFamily::ArtifactRevision,
                &key,
                ArtifactRevisionRecord::decode,
            )?
            .ok_or(PublicationError::RevisionNotFound { revision })?;
        if loaded.record.state != RevisionState::Available {
            return Err(PublicationError::RevisionUnavailable { revision });
        }
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use nokv_types::{
        CommitId, NormalizedRelativePath, OperationId, RootActivationState, WorkbenchId,
        WorkspaceIncarnationId, FIXED_ID_BYTES,
    };

    use super::super::codec::{commit_key, workbench_commit_head_key};
    use super::super::commit::{BeginBuildCommitRequest, BuildCommitStepRequest, CommitService};
    use super::super::commit_closure::advance_commit_parent_rolling_digest;
    use super::super::commit_records::{CommitRecord, WorkbenchCommitHeadRecord};
    use super::super::namespace::{
        create_visible_workspace, get_visible_path_at, RootReadContext, RootWriteContext,
    };
    use super::super::object_block_key;
    use super::super::publish_operation_records::{
        AppendSegment, PublishTerminalError, PublishTerminalErrorKind,
    };
    use super::super::query_records::{QueryFieldId, QueryScalar};
    use super::super::remove::{remove_path, RemovePathRequest};
    use super::super::restore_records::{
        RestoreCommitClosureProgress, RestoreCommitProvenance, RestoreCommitProvenanceV5,
        RestoreDestinationBinding, RestoreDestinationManifests, RestoreManifestDescriptor,
        RestoreManifestIdentity, RestoreManifestPublication, RestoreSource,
        RestoreSourceCommitSeal, RestoreTerminalError, RestoreTerminalErrorKind, RestoreTransition,
        RESTORE_MANIFEST_CONTENT_TYPE,
    };
    use super::*;

    const TEST_BATCH_ROWS: usize = 300;

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn root() -> RootId {
        RootId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(7).unwrap()
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn successor_owner() -> OwnerEpoch {
        OwnerEpoch::new(2).unwrap()
    }

    fn request(value: u128) -> RequestId {
        RequestId::from_bytes(value.to_be_bytes())
    }

    fn operation_id(value: u128) -> OperationId {
        OperationId::from_bytes(value.to_be_bytes())
    }

    fn revision(value: u128) -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes(value.to_be_bytes())
    }

    fn incarnation(value: u128) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes(value.to_be_bytes())
    }

    fn path(value: &str) -> NormalizedRelativePath {
        NormalizedRelativePath::new(value).unwrap()
    }

    fn workbench() -> WorkbenchId {
        WorkbenchId::new("publication-tests").unwrap()
    }

    fn next_request(counter: &mut u128) -> RequestId {
        let value = *counter;
        *counter += 1;
        request(value)
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
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
            placement_generation: placement(),
            owner_epoch: owner(),
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

    fn ready_store(counter: &mut u128) -> MetaShard {
        prepare_store(
            crate::workspace::test_support::memory(shard()).unwrap(),
            counter,
        )
    }

    fn ready_capturing_store(
        counter: &mut u128,
    ) -> (
        MetaShard,
        Arc<crate::workspace::test_support::CommitCaptureStore>,
    ) {
        let inner = crate::workspace::test_support::memory_txn_store().unwrap();
        let (wrapped, capture) = crate::workspace::test_support::capture_txn_store(inner);
        let store = MetaShard::initialize(wrapped, shard()).unwrap();
        (prepare_store(store, counter), capture)
    }

    fn open_capturing_file_store(
        path: &std::path::Path,
    ) -> (
        MetaShard,
        Arc<crate::workspace::test_support::CommitCaptureStore>,
    ) {
        let inner = crate::workspace::test_support::open_file_txn_store(path).unwrap();
        let (wrapped, capture) = crate::workspace::test_support::capture_txn_store(inner);
        let store = MetaShard::open(wrapped, shard()).unwrap();
        (store, capture)
    }

    fn ready_file_store(path: &std::path::Path, counter: &mut u128) -> MetaShard {
        prepare_store(
            crate::workspace::test_support::initialize_file(path, shard()).unwrap(),
            counter,
        )
    }

    fn prepare_store(store: MetaShard, counter: &mut u128) -> MetaShard {
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(
                &store,
                next_request(counter),
                RootFenceAction::Install,
            ))
            .unwrap();
        store
            .execute(&fence_command(
                &store,
                next_request(counter),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
        create_visible_workspace(
            &store,
            RootWriteContext::current(
                &store,
                root(),
                shard(),
                ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
                placement(),
                owner(),
                next_request(counter),
            )
            .unwrap(),
            &workbench(),
            incarnation(9),
        )
        .unwrap();
        store
    }

    fn publication_context(store: &MetaShard, counter: &mut u128) -> PublicationContext {
        publication_context_for_owner(store, counter, owner())
    }

    fn commit_context(store: &MetaShard, counter: &mut u128) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            next_request(counter),
        )
        .unwrap()
    }

    fn publication_context_for_owner(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
    ) -> PublicationContext {
        PublicationContext {
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement_generation: placement(),
            owner_epoch,
            request_id: next_request(counter),
            read_version: store.current_read_version().unwrap(),
        }
    }

    fn publication_context_at(
        owner_epoch: OwnerEpoch,
        request_id: RequestId,
        read_version: ReadVersion,
    ) -> PublicationContext {
        PublicationContext {
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement_generation: placement(),
            owner_epoch,
            request_id,
            read_version,
        }
    }

    fn staged_rows(
        artifact_revision_id: ArtifactRevisionId,
        count: usize,
    ) -> Vec<StagedObjectRecord> {
        (0..count)
            .map(|index| {
                let sequence = u32::try_from(index).unwrap();
                StagedObjectRecord {
                    artifact_revision_id,
                    object_sequence: sequence,
                    object_key: object_block_key(
                        shard(),
                        root(),
                        artifact_revision_id,
                        u64::from(sequence),
                    ),
                    multipart_upload_id: None,
                    expected_length: 1,
                    expected_digest_uri: format!("sha256:{sequence:064x}"),
                    provider_state: StagedProviderState::Planned,
                    cleanup_state: StagedCleanupState::Owned,
                }
            })
            .collect()
    }

    fn uploaded_rows(staged: &[StagedObjectRecord]) -> Vec<StagedObjectRecord> {
        staged
            .iter()
            .cloned()
            .map(|mut row| {
                row.provider_state = StagedProviderState::Uploaded;
                row
            })
            .collect()
    }

    fn manifest_rows(staged: &[StagedObjectRecord]) -> Vec<ManifestRowInput> {
        let mut logical_offset = 0_u64;
        staged
            .iter()
            .map(|staged| {
                let row = ManifestRowInput {
                    object_index: u64::from(staged.object_sequence),
                    row: ArtifactManifestRow {
                        physical_owner_revision_id: staged.artifact_revision_id,
                        physical_object_index: u64::from(staged.object_sequence),
                        object_key: staged.object_key.clone(),
                        logical_offset,
                        offset: 0,
                        length: staged.expected_length,
                        digest_uri: staged.expected_digest_uri.clone(),
                        append_segment: None,
                    },
                };
                logical_offset = logical_offset
                    .checked_add(staged.expected_length)
                    .expect("test manifest logical length fits u64");
                row
            })
            .collect()
    }

    fn publish_operation(
        operation_id: OperationId,
        artifact_revision_id: ArtifactRevisionId,
        path: NormalizedRelativePath,
        claim: PublishClaim,
        staged: &[StagedObjectRecord],
        manifest: &[ManifestRowInput],
    ) -> PublishOperationRecord {
        let dependencies = Vec::new();
        let mut operation = PublishOperationRecord {
            operation_id,
            identity_digest: [0; SHA256_BYTES],
            initialization_digest: [0; SHA256_BYTES],
            initiating_owner_epoch: owner(),
            activity_deadline_ms: 1_000_000,
            authority: PublishAuthority::Visible,
            workbench_id: workbench(),
            workspace_incarnation_id: incarnation(9),
            path,
            artifact_revision_id,
            claim,
            phase: PublishPhase::Uploading,
            staged_object_count: u32::try_from(staged.len()).unwrap(),
            staged_object_seal: staged_object_ledger_digest(staged).unwrap(),
            staged_object_cursor: 0,
            staged_object_rolling_digest: [0; SHA256_BYTES],
            uploaded_object_cursor: 0,
            uploaded_object_rolling_digest: [0; SHA256_BYTES],
            manifest_row_count: u32::try_from(manifest.len()).unwrap(),
            manifest_seal: manifest_rows_digest(manifest).unwrap(),
            manifest_cursor: 0,
            manifest_rolling_digest: [0; SHA256_BYTES],
            manifest_last_position: None,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: dependency_owner_digest(&dependencies).unwrap(),
            cleanup_staged_object_cursor: 0,
            cleanup_manifest_cursor: 0,
            publication_absence_proof: None,
            result: None,
            terminal_error: None,
        };
        seal_publish_operation(&mut operation);
        operation
    }

    fn published_artifact(operation: &PublishOperationRecord) -> PublishedArtifact {
        PublishedArtifact {
            logical_size: u64::from(operation.manifest_row_count),
            body_digest_uri: format!(
                "sha256:body-{:032x}",
                u128::from_be_bytes(*operation.artifact_revision_id.as_bytes())
            ),
            manifest_digest_uri: sha256_digest_uri(operation.manifest_seal),
            content_type: "application/octet-stream".to_owned(),
            producer: Some("publication-test".to_owned()),
            manifest_id: Some("manifest-test".to_owned()),
            typed_index_projection: TypedProjection::empty().encode().unwrap(),
        }
    }

    fn canonical_digest_uri(fill: u8) -> String {
        format!("sha256:{}", format!("{fill:02x}").repeat(SHA256_BYTES))
    }

    fn restore_manifest_identity(
        publication_operation_id: OperationId,
        artifact_revision_id: ArtifactRevisionId,
    ) -> RestoreManifestIdentity {
        RestoreManifestIdentity {
            publication_operation_id,
            artifact_revision_id,
        }
    }

    fn restore_destination_binding() -> RestoreDestinationBinding {
        RestoreDestinationBinding {
            destination_commit_id: CommitId::from_bytes([0x31; SHA256_BYTES]),
            effective_content_digest_uri: canonical_digest_uri(0x11),
            destination_projection_input_digest: [0x32; SHA256_BYTES],
            run_manifest_identity: restore_manifest_identity(
                operation_id(90_001),
                revision(90_001),
            ),
            restore_manifest_identity: restore_manifest_identity(
                operation_id(90_002),
                revision(90_002),
            ),
            manifests: None,
        }
    }

    fn sealed_bound_restore_operation() -> RestoreOperationRecord {
        let restore_operation_id = operation_id(90_000);
        let source_commit_id = CommitId::from_bytes([0x21; SHA256_BYTES]);
        let mut identity_digest = [0x90; SHA256_BYTES];
        identity_digest[..OperationId::BYTE_WIDTH].copy_from_slice(restore_operation_id.as_bytes());
        let binding = restore_destination_binding();
        let record = RestoreOperationRecord {
            operation_id: restore_operation_id,
            identity_digest,
            initialization_digest: None,
            source_workbench_id: WorkbenchId::new("restore-source").unwrap(),
            source_workspace_incarnation_id: incarnation(90_010),
            source: RestoreSource::Snapshot {
                snapshot_id: nokv_types::SnapshotId::new(90_011),
                read_version: ReadVersion::new(1).unwrap(),
            },
            destination_workbench_id: workbench(),
            destination_workspace_incarnation_id: incarnation(9),
            destination_restore_manifest_identity: Some(binding.restore_manifest_identity),
            restore_manifest: RestoreManifestDescriptor {
                body_digest_uri: canonical_digest_uri(0x42),
                logical_size: 128,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            commit_provenance: RestoreCommitProvenance::V5(Box::new(RestoreCommitProvenanceV5 {
                source_commit: RestoreSourceCommitSeal {
                    commit_id: source_commit_id,
                    content_digest_uri: canonical_digest_uri(0x11),
                    manifest_digest_uri: canonical_digest_uri(0x12),
                    tree_manifest_revision_id: revision(90_010),
                    member_count: 0,
                    member_digest: [0; SHA256_BYTES],
                    unique_revision_count: 0,
                    revision_digest: [0; SHA256_BYTES],
                    parent_digest: [0; SHA256_BYTES],
                    generic_index_count: 0,
                    generic_index_digest: [0; SHA256_BYTES],
                },
                destination_committed_at_unix_seconds: 1,
                destination_binding: Some(binding),
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
                        source_commit_id,
                    ),
                    parent_seal: None,
                    cleanup_member_count: 0,
                    cleanup_generic_index_count: 0,
                    cleanup_revision_count: 0,
                },
                destination_head_generation: None,
            })),
            phase: RestorePhase::SourceSealed,
            source_cursor: None,
            source_paths_eof: true,
            source_generic_index_cursor: None,
            source_generic_index_count: 0,
            source_generic_index_rolling_digest: [0; SHA256_BYTES],
            source_generic_index_seal: Some([0; SHA256_BYTES]),
            source_generic_indexes_match_base_commit: Some(true),
            source_eof: true,
            source_member_count: 0,
            source_member_rolling_digest: [0; SHA256_BYTES],
            source_member_seal: Some([0; SHA256_BYTES]),
            source_matches_base_commit: Some(true),
            next_member_sequence: 0,
            member_rolling_digest: [0; SHA256_BYTES],
            member_seal: Some([0; SHA256_BYTES]),
            cleanup_member_cursor: 0,
            cleanup_generic_index_cursor: 0,
            result: None,
            terminal_error: None,
        };
        record.validate().unwrap();
        record
    }

    fn install_restore_publication_authority(
        store: &MetaShard,
        counter: &mut u128,
        restore: &RestoreOperationRecord,
    ) {
        let context = publication_context(store, counter);
        let workspace_key = workspace_current_key(root(), &workbench());
        let workspace_payload = payload_at(
            store,
            MetadataFamily::WorkspaceCurrent,
            &workspace_key,
            context.read_version,
        )
        .unwrap();
        let workspace = WorkspaceRecord::decode(&workspace_payload).unwrap();
        let staging = WorkspaceRecord {
            state: WorkspaceState::Staging,
            owning_operation_id: Some(restore.operation_id),
            ..workspace
        };
        let restore_key = operation_key(root(), OperationKind::Restore, restore.operation_id);
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::WorkspaceCurrent,
            workspace_key,
            workspace_payload,
            staging.encode().unwrap(),
        )
        .unwrap();
        plan.put_absent(
            MetadataFamily::Operation,
            restore_key,
            restore.encode().unwrap(),
        )
        .unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(context.object_namespace_id),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: plan.predicates,
                    mutations: plan.mutations,
                    history_projection: plan.history,
                    event_projection: plan.events,
                    deterministic_result: restore.encode().unwrap(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn replace_restore_operation(
        store: &MetaShard,
        counter: &mut u128,
        expected: &RestoreOperationRecord,
        next: &RestoreOperationRecord,
    ) {
        let context = publication_context(store, counter);
        let key = operation_key(root(), OperationKind::Restore, expected.operation_id);
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            key,
            expected.encode().unwrap(),
            next.encode().unwrap(),
        )
        .unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(context.object_namespace_id),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: plan.predicates,
                    mutations: plan.mutations,
                    history_projection: plan.history,
                    event_projection: plan.events,
                    deterministic_result: next.encode().unwrap(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn install_publish_operation_for_test(
        store: &MetaShard,
        counter: &mut u128,
        operation: &PublishOperationRecord,
    ) {
        let context = publication_context(store, counter);
        let key = operation_key(root(), OperationKind::Publish, operation.operation_id);
        let mut plan = CommandPlan::default();
        plan.put_absent(MetadataFamily::Operation, key, operation.encode().unwrap())
            .unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(context.object_namespace_id),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: plan.predicates,
                    mutations: plan.mutations,
                    history_projection: plan.history,
                    event_projection: plan.events,
                    deterministic_result: operation.encode().unwrap(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn replace_publish_operation_for_test(
        store: &MetaShard,
        counter: &mut u128,
        expected: &PublishOperationRecord,
        next: &PublishOperationRecord,
    ) {
        let context = publication_context(store, counter);
        let key = operation_key(root(), OperationKind::Publish, expected.operation_id);
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Operation,
            key,
            expected.encode().unwrap(),
            next.encode().unwrap(),
        )
        .unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(context.object_namespace_id),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: plan.predicates,
                    mutations: plan.mutations,
                    history_projection: plan.history,
                    event_projection: plan.events,
                    deterministic_result: next.encode().unwrap(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn delete_restore_manifest_path_for_test(
        store: &MetaShard,
        counter: &mut u128,
        manifest_path: &str,
    ) {
        let context = publication_context(store, counter);
        let key = path_current_key(root(), incarnation(9), &path(manifest_path));
        let payload = payload_at(
            store,
            MetadataFamily::PathCurrent,
            &key,
            context.read_version,
        )
        .unwrap();
        let mut plan = CommandPlan::default();
        plan.delete(MetadataFamily::PathCurrent, key, payload)
            .unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(context.object_namespace_id),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: plan.predicates,
                    mutations: plan.mutations,
                    history_projection: plan.history,
                    event_projection: plan.events,
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn move_restore_into_abort_for_publication_test(
        store: &MetaShard,
        counter: &mut u128,
        restore: &RestoreOperationRecord,
        cleaning: bool,
    ) -> RestoreOperationRecord {
        let aborting = restore
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginAbort {
                    terminal_error: RestoreTerminalError {
                        kind: RestoreTerminalErrorKind::AbortedByCaller,
                        message: "publisher cleanup owns the remaining staging rows".to_owned(),
                        evidence_digest: None,
                    },
                },
            )
            .unwrap();
        replace_restore_operation(store, counter, restore, &aborting);
        if cleaning {
            let next = aborting
                .apply(RestorePhase::Aborting, RestoreTransition::BeginCleaning)
                .unwrap();
            replace_restore_operation(store, counter, &aborting, &next);
            next
        } else {
            aborting
        }
    }

    fn load_staged_object_for_test(
        store: &MetaShard,
        operation: &PublishOperationRecord,
        sequence: u64,
    ) -> StagedObjectRecord {
        StagedObjectRecord::decode(
            &payload_at(
                store,
                MetadataFamily::StagedObject,
                &staged_object_key(root(), operation.operation_id, sequence),
                store.current_read_version().unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn restore_staging_publish_operation(
        identity: RestoreManifestIdentity,
        manifest_path: &str,
        body_digest_uri: String,
        logical_size: u64,
    ) -> (
        PublishOperationRecord,
        Vec<StagedObjectRecord>,
        Vec<ManifestRowInput>,
    ) {
        let mut staged = staged_rows(identity.artifact_revision_id, 1);
        staged[0].expected_length = logical_size;
        staged[0].expected_digest_uri = body_digest_uri;
        let manifest = manifest_rows(&staged);
        let mut operation = publish_operation(
            identity.publication_operation_id,
            identity.artifact_revision_id,
            path(manifest_path),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        operation.authority = PublishAuthority::RestoreStaging {
            restore_operation_id: operation_id(90_000),
        };
        seal_publish_operation(&mut operation);
        (operation, staged, manifest)
    }

    fn drive_restore_staging_publish(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        operation: PublishOperationRecord,
        staged: &[StagedObjectRecord],
        manifest: &[ManifestRowInput],
        content_type: &str,
    ) -> (PublishOperationRecord, FinalizePublishOutcome) {
        let initial = operation.clone();
        let operation = begin_operation(service, store, counter, operation);
        let operation = stage_all(service, store, counter, operation, staged, manifest);
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let outcome = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(store, counter),
                expected_operation: operation.clone(),
                artifact: PublishedArtifact {
                    logical_size: staged[0].expected_length,
                    body_digest_uri: staged[0].expected_digest_uri.clone(),
                    manifest_digest_uri: sha256_digest_uri(operation.manifest_seal),
                    content_type: content_type.to_owned(),
                    producer: None,
                    manifest_id: None,
                    typed_index_projection: TypedProjection::empty().encode().unwrap(),
                },
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();
        (initial, outcome)
    }

    fn budget_projection(scalar_bytes: usize) -> TypedProjection {
        budget_projection_with_prefix("budget", scalar_bytes)
    }

    fn budget_projection_with_prefix(prefix: &str, scalar_bytes: usize) -> TypedProjection {
        TypedProjection::new(
            (0..super::super::query_records::MAX_TYPED_PROJECTION_FIELDS)
                .map(|index| {
                    (
                        QueryFieldId::new(format!("{prefix}.field.{index:02}")).unwrap(),
                        QueryScalar::String("x".repeat(scalar_bytes)),
                    )
                })
                .collect(),
        )
        .unwrap()
    }

    fn begin_operation(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        operation: PublishOperationRecord,
    ) -> PublishOperationRecord {
        service
            .begin_publish(BeginPublishRequest {
                context: publication_context(store, counter),
                operation,
            })
            .unwrap()
            .operation
    }

    fn stage_all(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        mut operation: PublishOperationRecord,
        staged: &[StagedObjectRecord],
        manifest: &[ManifestRowInput],
    ) -> PublishOperationRecord {
        operation = stage_uploaded_objects(service, store, counter, operation, staged);
        for batch in manifest.chunks(MAX_PUBLICATION_BATCH_ROWS) {
            operation = service
                .stage_manifest_batch(StageManifestBatchRequest {
                    context: publication_context(store, counter),
                    expected_operation: operation,
                    manifest_rows: batch.to_vec(),
                    dependency_owner_revision_ids: Vec::new(),
                })
                .unwrap()
                .operation;
        }
        operation
    }

    fn stage_uploaded_objects(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        mut operation: PublishOperationRecord,
        staged: &[StagedObjectRecord],
    ) -> PublishOperationRecord {
        for batch in staged.chunks(MAX_PUBLICATION_BATCH_ROWS) {
            operation = service
                .stage_objects_batch(StageObjectsBatchRequest {
                    context: publication_context(store, counter),
                    expected_operation: operation,
                    staged_objects: batch.to_vec(),
                })
                .unwrap()
                .operation;
        }
        let uploaded = uploaded_rows(staged);
        for (expected, next) in staged
            .chunks(MAX_PUBLICATION_BATCH_ROWS)
            .zip(uploaded.chunks(MAX_PUBLICATION_BATCH_ROWS))
        {
            let updates = expected
                .iter()
                .cloned()
                .zip(next.iter().cloned())
                .map(|(expected, next)| StagedObjectUpdate { expected, next })
                .collect();
            operation = service
                .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                    context: publication_context(store, counter),
                    expected_operation: operation,
                    staged_object_updates: updates,
                })
                .unwrap()
                .operation;
        }
        operation
    }

    fn rejected_manifest_join(
        staged: Vec<StagedObjectRecord>,
        manifest: Vec<ManifestRowInput>,
    ) -> PublicationError {
        rejected_manifest_join_with_plan(staged, manifest.clone(), manifest)
    }

    fn rejected_manifest_join_with_plan(
        staged: Vec<StagedObjectRecord>,
        planned_manifest: Vec<ManifestRowInput>,
        manifest: Vec<ManifestRowInput>,
    ) -> PublicationError {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = staged
            .first()
            .expect("join rejection fixture has staged rows")
            .artifact_revision_id;
        let operation = publish_operation(
            operation_id(700),
            revision_id,
            path("outputs/rejected-manifest.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &planned_manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_uploaded_objects(&service, &store, &mut counter, operation, &staged);
        let before = store.current_read_version().unwrap();
        let error = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                manifest_rows: manifest,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap_err();
        assert_eq!(store.current_read_version().unwrap(), before);
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            0,
        );
        error
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_full(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        operation_id: OperationId,
        artifact_revision_id: ArtifactRevisionId,
        path: NormalizedRelativePath,
        claim: PublishClaim,
        row_count: usize,
    ) -> FinalizePublishOutcome {
        publish_full_with_projection(
            service,
            store,
            counter,
            operation_id,
            artifact_revision_id,
            path,
            claim,
            row_count,
            TypedProjection::empty(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_full_with_projection(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        operation_id: OperationId,
        artifact_revision_id: ArtifactRevisionId,
        path: NormalizedRelativePath,
        claim: PublishClaim,
        row_count: usize,
        projection: TypedProjection,
    ) -> FinalizePublishOutcome {
        let staged = staged_rows(artifact_revision_id, row_count);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id,
            artifact_revision_id,
            path,
            claim,
            &staged,
            &manifest,
        );
        let operation = begin_operation(service, store, counter, operation);
        let operation = stage_all(service, store, counter, operation, &staged, &manifest);
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let mut artifact = published_artifact(&operation);
        artifact.typed_index_projection = projection.encode().unwrap();
        service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(store, counter),
                artifact,
                expected_operation: operation,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_with_dependencies_and_projection(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        operation_id: OperationId,
        artifact_revision_id: ArtifactRevisionId,
        path: NormalizedRelativePath,
        claim: PublishClaim,
        dependencies: &[ArtifactRevisionId],
        projection: TypedProjection,
    ) -> FinalizePublishOutcome {
        let staged = staged_rows(artifact_revision_id, 1);
        let manifest = manifest_rows(&staged);
        let mut operation = publish_operation(
            operation_id,
            artifact_revision_id,
            path,
            claim,
            &staged,
            &manifest,
        );
        operation.dependency_count = u8::try_from(dependencies.len()).unwrap();
        operation.dependency_depth = u8::from(!dependencies.is_empty());
        operation.dependency_digest = dependency_owner_digest(dependencies).unwrap();
        seal_publish_operation(&mut operation);
        let operation = begin_operation(service, store, counter, operation);
        let operation = stage_uploaded_objects(service, store, counter, operation, &staged);
        let operation = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(store, counter),
                expected_operation: operation,
                manifest_rows: manifest,
                dependency_owner_revision_ids: dependencies.to_vec(),
            })
            .unwrap()
            .operation;
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let mut artifact = published_artifact(&operation);
        artifact.typed_index_projection = projection.encode().unwrap();
        service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(store, counter),
                artifact,
                expected_operation: operation,
                dependency_owner_revision_ids: dependencies.to_vec(),
            })
            .unwrap()
    }

    fn maximum_path(discriminator: usize) -> NormalizedRelativePath {
        let prefix = format!("p{discriminator:03}");
        let value = format!(
            "{prefix}{}",
            "x".repeat(NormalizedRelativePath::MAX_BYTES - prefix.len())
        );
        let path = NormalizedRelativePath::new(value).unwrap();
        assert_eq!(path.byte_len(), NormalizedRelativePath::MAX_BYTES);
        path
    }

    fn seed_maximum_dependencies(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
    ) -> Vec<ArtifactRevisionId> {
        (0..usize::from(MAX_DEPENDENCY_COUNT))
            .map(|index| {
                let identity = 20_000 + index as u128;
                let revision_id = revision(identity);
                publish_full(
                    service,
                    store,
                    counter,
                    operation_id(identity),
                    revision_id,
                    path(&format!("dependencies/{index:02}.bin")),
                    PublishClaim::CreateOnly,
                    1,
                );
                revision_id
            })
            .collect()
    }

    fn count_prefix(store: &MetaShard, family: MetadataFamily, prefix: &[u8]) -> usize {
        let version = store.current_read_version().unwrap();
        let mut start_after = None;
        let mut count = 0;
        loop {
            let page = store
                .scan_prefix_at(
                    root(),
                    placement(),
                    owner(),
                    family,
                    prefix,
                    version,
                    start_after.as_deref(),
                    MAX_COMMAND_ITEMS,
                )
                .unwrap();
            if page.is_empty() {
                return count;
            }
            count += page.len();
            start_after = page.last().map(|row| row.key.clone());
            if page.len() < MAX_COMMAND_ITEMS {
                return count;
            }
        }
    }

    fn payload_at(
        store: &MetaShard,
        family: MetadataFamily,
        key: &[u8],
        read_version: ReadVersion,
    ) -> Option<Vec<u8>> {
        store
            .read_at(root(), placement(), owner(), family, key, read_version)
            .unwrap()
    }

    #[test]
    fn begin_publish_resumes_exact_operation_identity_and_rejects_reuse() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(650);
        let staged = staged_rows(revision_id, 2);
        let manifest = manifest_rows(&staged);
        let initial = publish_operation(
            operation_id(650),
            revision_id,
            path("outputs/resume.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let begun = begin_operation(&service, &store, &mut counter, initial.clone());
        let progressed = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: begun,
                staged_objects: staged[..1].to_vec(),
            })
            .unwrap()
            .operation;

        let resumed = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: initial.clone(),
            })
            .unwrap();
        assert!(resumed.replayed);
        assert_eq!(resumed.operation, progressed);

        let mut mismatched = initial;
        mismatched.artifact_revision_id = revision(651);
        seal_publish_operation(&mut mismatched);
        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: mismatched,
            }),
            Err(PublicationError::OperationInputMismatch)
        );
    }

    #[test]
    fn successor_epoch_cannot_borrow_visible_or_commit_staging_operation_identity() {
        for (index, authority) in [
            PublishAuthority::Visible,
            PublishAuthority::CommitStaging {
                commit_operation_id: operation_id(91_100),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let revision_id = revision(91_101 + u128::try_from(index).unwrap());
            let staged = staged_rows(revision_id, 1);
            let manifest = manifest_rows(&staged);
            let mut durable = publish_operation(
                operation_id(91_101 + u128::try_from(index).unwrap()),
                revision_id,
                path("outputs/epoch-bound.bin"),
                PublishClaim::CreateOnly,
                &staged,
                &manifest,
            );
            durable.authority = authority;
            seal_publish_operation(&mut durable);
            install_publish_operation_for_test(&store, &mut counter, &durable);

            store
                .advance_owner_epoch(Some(owner()), successor_owner())
                .unwrap();
            let mut successor_candidate = durable.clone();
            successor_candidate.initiating_owner_epoch = successor_owner();
            seal_publish_operation(&mut successor_candidate);
            assert_eq!(
                PublicationService::new(&store).begin_publish(BeginPublishRequest {
                    context: publication_context_for_owner(
                        &store,
                        &mut counter,
                        successor_owner(),
                    ),
                    operation: successor_candidate,
                }),
                Err(PublicationError::OperationInputMismatch),
                "only a restore-staging terminal receipt may cross an owner epoch",
            );
        }
    }

    #[test]
    fn absent_publish_operation_never_accepts_an_old_initiating_owner() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        store
            .advance_owner_epoch(Some(owner()), successor_owner())
            .unwrap();
        let revision_id = revision(91_110);
        let staged = staged_rows(revision_id, 1);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(91_110),
            revision_id,
            path("outputs/absent-old-owner.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        assert_eq!(
            PublicationService::new(&store).begin_publish(BeginPublishRequest {
                context: publication_context_for_owner(&store, &mut counter, successor_owner(),),
                operation,
            }),
            Err(PublicationError::InitiatingOwnerEpochMismatch {
                expected: successor_owner(),
                actual: owner(),
            }),
        );
    }

    #[test]
    fn transition_publish_rejects_direct_publish_without_metadata_side_effects() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(652);
        let publish_path = path("outputs/direct-publish-bypass.bin");
        let staged = staged_rows(revision_id, 1);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(652),
            revision_id,
            publish_path.clone(),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let finalizing = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let artifact = published_artifact(&finalizing);
        let operation_key = operation_key(root(), OperationKind::Publish, finalizing.operation_id);
        let path_key = path_current_key(root(), finalizing.workspace_incarnation_id, &publish_path);
        let revision_key = artifact_revision_key(root(), revision_id);
        let claim_key = artifact_revision_claim_key(root(), revision_id);
        let version_before = store.current_read_version().unwrap();
        let operation_before = payload_at(
            &store,
            MetadataFamily::Operation,
            &operation_key,
            version_before,
        );
        let path_before = payload_at(
            &store,
            MetadataFamily::PathCurrent,
            &path_key,
            version_before,
        );
        let revision_before = payload_at(
            &store,
            MetadataFamily::ArtifactRevision,
            &revision_key,
            version_before,
        );
        let claim_before = payload_at(
            &store,
            MetadataFamily::ArtifactRevision,
            &claim_key,
            version_before,
        );
        assert!(path_before.is_none());
        assert!(revision_before.is_none());
        assert!(claim_before.is_some());

        assert_eq!(
            service.transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: finalizing.clone(),
                transition: PublishTransition::Publish {
                    result: PublishResult {
                        path_generation: Generation::new(1).unwrap(),
                        workspace_revision: WorkspaceRevision::new(1),
                        logical_size: artifact.logical_size,
                        body_digest_uri: artifact.body_digest_uri.clone(),
                    },
                },
            }),
            Err(PublicationError::MetadataLastFinalizationRequired),
        );
        assert_eq!(store.current_read_version().unwrap(), version_before);
        assert_eq!(
            payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key,
                version_before,
            ),
            operation_before,
        );
        assert_eq!(
            payload_at(
                &store,
                MetadataFamily::PathCurrent,
                &path_key,
                version_before,
            ),
            path_before,
        );
        assert_eq!(
            payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &revision_key,
                version_before,
            ),
            revision_before,
        );
        assert_eq!(
            payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &claim_key,
                version_before,
            ),
            claim_before,
        );

        let outcome = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: finalizing,
                artifact,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();
        assert_eq!(outcome.operation.phase, PublishPhase::Published);
        assert!(read_revision_claim(&store, revision_id).is_none());
        let published_version = store.current_read_version().unwrap();
        assert!(payload_at(
            &store,
            MetadataFamily::PathCurrent,
            &path_key,
            published_version,
        )
        .is_some());
        assert!(payload_at(
            &store,
            MetadataFamily::ArtifactRevision,
            &revision_key,
            published_version,
        )
        .is_some());
    }

    #[test]
    fn staged_admission_rejects_noncanonical_object_coordinates_atomically() {
        let revision_id = revision(705);
        let canonical_staged = staged_rows(revision_id, 1);
        let canonical_manifest = manifest_rows(&canonical_staged);
        let malformed_keys = [
            object_block_key(
                LogicalShardId::from_bytes([0x55; FIXED_ID_BYTES]),
                root(),
                revision_id,
                0,
            ),
            object_block_key(
                shard(),
                RootId::from_bytes([0x66; FIXED_ID_BYTES]),
                revision_id,
                0,
            ),
            object_block_key(shard(), root(), revision(706), 0),
            object_block_key(shard(), root(), revision_id, 1),
        ];

        for (case, malformed_key) in malformed_keys.into_iter().enumerate() {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let operation = publish_operation(
                operation_id(705 + u128::try_from(case).unwrap()),
                revision_id,
                path("outputs/noncanonical-staged.bin"),
                PublishClaim::CreateOnly,
                &canonical_staged,
                &canonical_manifest,
            );
            let operation = begin_operation(&service, &store, &mut counter, operation);
            let mut malformed_staged = canonical_staged.clone();
            malformed_staged[0].object_key.clone_from(&malformed_key);
            let mut agreeing_manifest = canonical_manifest.clone();
            agreeing_manifest[0]
                .row
                .object_key
                .clone_from(&malformed_key);
            assert_eq!(
                malformed_staged[0].object_key, agreeing_manifest[0].row.object_key,
                "fixture {case} must model a staged/manifest pair that agrees on the bad key"
            );

            let before = store.current_read_version().unwrap();
            assert_eq!(
                service
                    .stage_objects_batch(StageObjectsBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation.clone(),
                        staged_objects: malformed_staged,
                    })
                    .unwrap_err(),
                PublicationError::StagedObjectKeyMismatch { sequence: 0 },
                "malformed coordinate case {case}"
            );
            assert_eq!(store.current_read_version().unwrap(), before);
            assert_eq!(
                count_prefix(
                    &store,
                    MetadataFamily::StagedObject,
                    &staged_object_prefix(root(), operation.operation_id),
                ),
                0
            );
            let persisted = payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Publish, operation.operation_id),
                before,
            )
            .expect("publish operation remains durable");
            assert_eq!(
                PublishOperationRecord::decode(&persisted).unwrap(),
                operation
            );
        }
    }

    #[test]
    fn manifest_batches_require_cursor_contiguous_object_indexes_atomically() {
        let revision_id = revision(707);
        let staged = staged_rows(revision_id, 3);
        let manifest = manifest_rows(&staged);

        {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let operation = publish_operation(
                operation_id(707),
                revision_id,
                path("outputs/starting-gap.bin"),
                PublishClaim::CreateOnly,
                &staged,
                &manifest,
            );
            let operation = begin_operation(&service, &store, &mut counter, operation);
            let operation =
                stage_uploaded_objects(&service, &store, &mut counter, operation, &staged);
            let mut starting_gap = manifest.clone();
            for input in &mut starting_gap {
                input.object_index += 1;
            }

            let before = store.current_read_version().unwrap();
            assert_eq!(
                service
                    .stage_manifest_batch(StageManifestBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation.clone(),
                        manifest_rows: starting_gap,
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .unwrap_err(),
                PublicationError::ManifestPositionMismatch
            );
            assert_eq!(store.current_read_version().unwrap(), before);
            assert_eq!(
                count_prefix(
                    &store,
                    MetadataFamily::ArtifactManifest,
                    &artifact_manifest_prefix(root(), revision_id),
                ),
                0
            );
            let persisted = payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Publish, operation.operation_id),
                before,
            )
            .expect("publish operation remains durable");
            assert_eq!(
                PublishOperationRecord::decode(&persisted).unwrap(),
                operation
            );
        }

        {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let operation = publish_operation(
                operation_id(708),
                revision_id,
                path("outputs/middle-gap.bin"),
                PublishClaim::CreateOnly,
                &staged,
                &manifest,
            );
            let operation = begin_operation(&service, &store, &mut counter, operation);
            let operation =
                stage_uploaded_objects(&service, &store, &mut counter, operation, &staged);
            let operation = service
                .stage_manifest_batch(StageManifestBatchRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    manifest_rows: manifest[..1].to_vec(),
                    dependency_owner_revision_ids: Vec::new(),
                })
                .unwrap()
                .operation;
            let mut middle_gap = manifest[1..].to_vec();
            middle_gap[1].object_index += 1;

            let before = store.current_read_version().unwrap();
            assert_eq!(
                service
                    .stage_manifest_batch(StageManifestBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation.clone(),
                        manifest_rows: middle_gap,
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .unwrap_err(),
                PublicationError::ManifestPositionMismatch
            );
            assert_eq!(store.current_read_version().unwrap(), before);
            assert_eq!(
                count_prefix(
                    &store,
                    MetadataFamily::ArtifactManifest,
                    &artifact_manifest_prefix(root(), revision_id),
                ),
                1
            );
            let persisted = payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Publish, operation.operation_id),
                before,
            )
            .expect("publish operation remains durable");
            assert_eq!(
                PublishOperationRecord::decode(&persisted).unwrap(),
                operation
            );
        }
    }

    #[test]
    fn manifest_join_rejects_unmatched_child_owned_rows_atomically() {
        let revision_id = revision(710);

        let staged = staged_rows(revision_id, 1);
        let planned_manifest = manifest_rows(&staged);
        for wrong_key in [
            object_block_key(
                LogicalShardId::from_bytes([0x55; FIXED_ID_BYTES]),
                root(),
                revision_id,
                0,
            ),
            object_block_key(
                shard(),
                RootId::from_bytes([0x66; FIXED_ID_BYTES]),
                revision_id,
                0,
            ),
            object_block_key(shard(), root(), revision(711), 0),
            object_block_key(shard(), root(), revision_id, 1),
        ] {
            let mut wrong_manifest = manifest_rows(&staged);
            wrong_manifest[0].row.object_key = wrong_key;
            assert_eq!(
                rejected_manifest_join_with_plan(
                    staged.clone(),
                    planned_manifest.clone(),
                    wrong_manifest,
                ),
                PublicationError::ManifestOwnershipMismatch {
                    object_index: 0,
                    reason: "object key does not match the canonical physical owner coordinates",
                }
            );
        }

        let mut wrong_digest = manifest_rows(&staged);
        wrong_digest[0].row.digest_uri = format!("sha256:{:064x}", 99);
        assert_eq!(
            rejected_manifest_join(staged.clone(), wrong_digest),
            PublicationError::ManifestOwnershipMismatch {
                object_index: 0,
                reason: "staged object digest differs from the child-owned row",
            }
        );

        let mut wrong_length = manifest_rows(&staged);
        wrong_length[0].row.length = 2;
        assert_eq!(
            rejected_manifest_join(staged.clone(), wrong_length),
            PublicationError::ManifestOwnershipMismatch {
                object_index: 0,
                reason: "staged object length differs from the child-owned row",
            }
        );

        let mut packed_range = manifest_rows(&staged);
        packed_range[0].row.offset = 1;
        assert_eq!(
            rejected_manifest_join(staged.clone(), packed_range),
            PublicationError::ManifestOwnershipMismatch {
                object_index: 0,
                reason: "packed object ranges are unsupported",
            }
        );

        let staged = staged_rows(revision_id, 2);
        let mut duplicate_physical_index = manifest_rows(&staged);
        duplicate_physical_index[1].row.physical_object_index = 0;
        duplicate_physical_index[1].row.object_key = staged[0].object_key.clone();
        assert_eq!(
            rejected_manifest_join(staged.clone(), duplicate_physical_index),
            PublicationError::ManifestOwnershipMismatch {
                object_index: 1,
                reason: "child-owned physical object indexes must be contiguous from zero",
            }
        );

        let incomplete_manifest = manifest_rows(&staged[..1]);
        assert_eq!(
            rejected_manifest_join(staged, incomplete_manifest),
            PublicationError::ManifestOwnershipMismatch {
                object_index: 0,
                reason: "child-owned row count differs from staged-object count",
            }
        );
    }

    #[test]
    fn manifest_join_requires_each_durable_staged_row_to_remain_uploaded_and_owned() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(711);
        let staged = staged_rows(revision_id, 1);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(711),
            revision_id,
            path("outputs/staged-state-mismatch.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_uploaded_objects(&service, &store, &mut counter, operation, &staged);
        let staged_key = staged_object_key(root(), operation.operation_id, 0);
        let mut persisted = uploaded_rows(&staged)[0].clone();

        for next in [
            StagedObjectRecord {
                provider_state: StagedProviderState::Uploading,
                ..persisted.clone()
            },
            StagedObjectRecord {
                provider_state: StagedProviderState::Uploaded,
                cleanup_state: StagedCleanupState::DeletePending,
                ..persisted.clone()
            },
        ] {
            store
                .execute(
                    &MetadataCommand {
                        schema_id: SCHEMA_ID.to_owned(),
                        root_id: root(),
                        logical_shard_id: shard(),
                        object_namespace_id: Some(ObjectNamespaceId::from_bytes(
                            [10; FIXED_ID_BYTES],
                        )),
                        placement_generation: placement(),
                        owner_epoch: owner(),
                        request_id: next_request(&mut counter),
                        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                        read_version: store.current_read_version().unwrap(),
                        root_fence_action: RootFenceAction::RequireActive,
                        predicates: vec![CommandPredicate::Value {
                            family: MetadataFamily::StagedObject,
                            key: staged_key.clone(),
                            expected: Some(persisted.encode().unwrap()),
                        }],
                        mutations: vec![CommandMutation::Put {
                            family: MetadataFamily::StagedObject,
                            key: staged_key.clone(),
                            value: next.encode().unwrap(),
                        }],
                        history_projection: vec![HistoryProjection {
                            family: MetadataFamily::StagedObject,
                            key: staged_key.clone(),
                        }],
                        event_projection: Vec::new(),
                        deterministic_result: Vec::new(),
                    }
                    .seal(),
                )
                .unwrap();
            persisted = next;

            let before = store.current_read_version().unwrap();
            assert_eq!(
                service
                    .stage_manifest_batch(StageManifestBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation.clone(),
                        manifest_rows: manifest.clone(),
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .unwrap_err(),
                PublicationError::ManifestOwnershipMismatch {
                    object_index: 0,
                    reason: "staged object is not durably uploaded and owned",
                }
            );
            assert_eq!(store.current_read_version().unwrap(), before);
            assert_eq!(
                count_prefix(
                    &store,
                    MetadataFamily::ArtifactManifest,
                    &artifact_manifest_prefix(root(), revision_id),
                ),
                0,
            );
        }
    }

    #[test]
    fn append_manifest_join_accepts_borrowed_prefix_and_child_owned_tail_across_batches() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let base_revision_id = revision(720);
        let child_revision_id = revision(721);
        let staged = staged_rows(child_revision_id, 2);
        let mut manifest = (0_u64..2)
            .map(|index| ManifestRowInput {
                object_index: index,
                row: ArtifactManifestRow {
                    physical_owner_revision_id: base_revision_id,
                    physical_object_index: index,
                    object_key: object_block_key(shard(), root(), base_revision_id, index),
                    logical_offset: index,
                    offset: 0,
                    length: 1,
                    digest_uri: format!("sha256:{:064x}", index + 100),
                    append_segment: None,
                },
            })
            .collect::<Vec<_>>();
        manifest.extend(manifest_rows(&staged).into_iter().enumerate().map(
            |(index, mut input)| {
                input.object_index += 2;
                input.row.logical_offset += 2;
                input.row.append_segment = Some(AppendSegment {
                    segment_sequence: 0,
                    segment_offset: u64::try_from(index).unwrap(),
                });
                input
            },
        ));
        let dependencies = vec![base_revision_id];
        let mut operation = publish_operation(
            operation_id(721),
            child_revision_id,
            path("outputs/cross-batch-append.bin"),
            PublishClaim::Append {
                expected_generation: Generation::new(1).unwrap(),
                base_revision_id,
                append_offset: 2,
            },
            &staged,
            &manifest,
        );
        operation.dependency_count = 1;
        operation.dependency_depth = 1;
        operation.dependency_digest = dependency_owner_digest(&dependencies).unwrap();
        seal_publish_operation(&mut operation);
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_uploaded_objects(&service, &store, &mut counter, operation, &staged);
        let operation = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                manifest_rows: manifest[..2].to_vec(),
                dependency_owner_revision_ids: dependencies.clone(),
            })
            .unwrap()
            .operation;
        assert_eq!(operation.manifest_cursor, 2);
        let operation = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                manifest_rows: manifest[2..].to_vec(),
                dependency_owner_revision_ids: dependencies,
            })
            .unwrap()
            .operation;
        assert_eq!(operation.manifest_cursor, 4);
        assert_eq!(operation.manifest_rolling_digest, operation.manifest_seal);
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), child_revision_id),
            ),
            4,
        );
    }

    #[test]
    fn commit_staging_keeps_manifest_hidden_until_path_and_head_switch_atomically() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let publication = PublicationService::new(&store);
        let commits = CommitService::new(&store);
        let commit_operation_id = operation_id(700);
        let publish_operation_id = operation_id(701);
        let manifest_revision_id = revision(700);
        let commit_id = CommitId::from_bytes([7; SHA256_BYTES]);
        let run_manifest_path = path(RUN_MANIFEST_PATH);

        let begun = commits
            .begin_build(BeginBuildCommitRequest {
                context: commit_context(&store, &mut counter),
                operation_id: commit_operation_id,
                workbench_id: workbench(),
                expected_source_workspace_incarnation_id: incarnation(9),
                commit_id,
                content_digest_uri: format!("sha256:{:064x}", 1),
                manifest_digest_uri: format!("sha256:{:064x}", 2),
                projection_input_digest: [0x18; SHA256_BYTES],
                tree_manifest_revision_id: manifest_revision_id,
                replace: false,
                run_manifest_condition: CommitManifestCondition::CreateOnly,
                committed_at_unix_seconds: 1_700_000_000,
                expected_head_generation: None,
                producer: None,
                lineage_projection: Vec::new(),
                parent_commits: Vec::new(),
            })
            .unwrap();
        assert!(begun.operation.commit_staged_run_manifest.is_none());

        let staged = staged_rows(manifest_revision_id, 1);
        let manifest = manifest_rows(&staged);
        let mut operation = publish_operation(
            publish_operation_id,
            manifest_revision_id,
            run_manifest_path.clone(),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        operation.authority = PublishAuthority::CommitStaging {
            commit_operation_id,
        };
        seal_publish_operation(&mut operation);
        let operation = begin_operation(&publication, &store, &mut counter, operation);
        let operation = stage_all(
            &publication,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = publication
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let staged_manifest = PublishedArtifact {
            logical_size: staged[0].expected_length,
            body_digest_uri: staged[0].expected_digest_uri.clone(),
            manifest_digest_uri: sha256_digest_uri(operation.manifest_seal),
            content_type: "application/json".to_owned(),
            producer: None,
            manifest_id: None,
            typed_index_projection: TypedProjection::empty().encode().unwrap(),
        };
        let staged_outcome = publication
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                artifact: staged_manifest,
                expected_operation: operation,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();
        assert_eq!(staged_outcome.operation.phase, PublishPhase::Published);
        // The commit-staging finalize path must release the revision claim in
        // the same command that created the hidden Available revision.
        assert_eq!(read_revision_claim(&store, manifest_revision_id), None);

        let staged_version = store.current_read_version().unwrap();
        assert!(get_visible_path_at(
            &store,
            RootReadContext::current(&store, root(), placement(), owner()).unwrap(),
            &workbench(),
            &run_manifest_path,
        )
        .unwrap()
        .is_none());
        let head_key = workbench_commit_head_key(root(), incarnation(9));
        assert!(payload_at(
            &store,
            MetadataFamily::WorkbenchCommitHead,
            &head_key,
            staged_version,
        )
        .is_none());
        let staged_revision = ArtifactRevisionRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), manifest_revision_id),
                staged_version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(staged_revision.strong_reference_count, 1);
        let build = BuildCommitOperationRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::BuildCommit, commit_operation_id),
                staged_version,
            )
            .unwrap(),
        )
        .unwrap();
        assert!(build.commit_staged_run_manifest.is_some());
        assert_eq!(build.revision_ref_count, 1);

        loop {
            let outcome = commits
                .build_members(BuildCommitStepRequest {
                    context: commit_context(&store, &mut counter),
                    operation_id: commit_operation_id,
                    limit: 7,
                })
                .unwrap();
            if outcome.operation.members_complete {
                break;
            }
        }
        loop {
            let outcome = commits
                .seal_revisions(BuildCommitStepRequest {
                    context: commit_context(&store, &mut counter),
                    operation_id: commit_operation_id,
                    limit: 7,
                })
                .unwrap();
            if outcome.operation.revisions_complete {
                break;
            }
        }
        commits
            .attach_parents(BuildCommitStepRequest {
                context: commit_context(&store, &mut counter),
                operation_id: commit_operation_id,
                limit: 7,
            })
            .unwrap();
        commits
            .begin_sealing(commit_context(&store, &mut counter), commit_operation_id)
            .unwrap();

        let before_switch = store.current_read_version().unwrap();
        let path_key = path_current_key(root(), incarnation(9), &run_manifest_path);
        let commit_key = commit_key(root(), commit_id);
        assert!(payload_at(
            &store,
            MetadataFamily::PathCurrent,
            &path_key,
            before_switch,
        )
        .is_none());
        assert!(payload_at(
            &store,
            MetadataFamily::WorkbenchCommitHead,
            &head_key,
            before_switch,
        )
        .is_none());
        assert!(payload_at(&store, MetadataFamily::Commit, &commit_key, before_switch,).is_none());

        let completed = commits
            .finalize_build(commit_context(&store, &mut counter), commit_operation_id)
            .unwrap();
        assert_eq!(completed.operation.phase, BuildCommitPhase::Complete);
        let switch_version = store.current_read_version().unwrap();
        assert_eq!(switch_version.get(), completed.commit_version.get());
        assert!(payload_at(
            &store,
            MetadataFamily::PathCurrent,
            &path_key,
            before_switch,
        )
        .is_none());
        assert!(payload_at(
            &store,
            MetadataFamily::WorkbenchCommitHead,
            &head_key,
            before_switch,
        )
        .is_none());

        let visible = PathEntry::decode(
            &payload_at(
                &store,
                MetadataFamily::PathCurrent,
                &path_key,
                switch_version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(visible.generation, Generation::new(1).unwrap());
        assert_eq!(visible.artifact_revision_id, manifest_revision_id);
        let head = WorkbenchCommitHeadRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::WorkbenchCommitHead,
                &head_key,
                switch_version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(head.commit_id, commit_id);
        assert_eq!(head.head_generation, Generation::new(1).unwrap());
        let sealed = CommitRecord::decode(
            &payload_at(&store, MetadataFamily::Commit, &commit_key, switch_version).unwrap(),
        )
        .unwrap();
        assert_eq!(sealed.tree_manifest_revision_id, manifest_revision_id);
        assert_eq!(sealed.member_count, 1);
        assert_eq!(sealed.unique_revision_count, 1);
        let visible_revision = ArtifactRevisionRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), manifest_revision_id),
                switch_version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(visible_revision.strong_reference_count, 2);
    }

    #[test]
    fn large_publication_batches_resume_and_create_atomically() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(100);
        let staged = staged_rows(revision_id, TEST_BATCH_ROWS);
        let manifest = manifest_rows(&staged);
        let initial = publish_operation(
            operation_id(100),
            revision_id,
            path("outputs/large.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let mut operation = begin_operation(&service, &store, &mut counter, initial);

        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::StagedObject,
                &staged_object_prefix(root(), operation.operation_id),
            ),
            0
        );
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            0
        );

        let replay_context = publication_context(&store, &mut counter);
        let replay_request = StageObjectsBatchRequest {
            context: replay_context,
            expected_operation: operation.clone(),
            staged_objects: staged[..MAX_PUBLICATION_BATCH_ROWS].to_vec(),
        };
        let first = service.stage_objects_batch(replay_request.clone()).unwrap();
        let replay = service.stage_objects_batch(replay_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);
        assert_eq!(replay.operation, first.operation);
        operation = first.operation;

        operation = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                staged_objects: staged[MAX_PUBLICATION_BATCH_ROWS..].to_vec(),
            })
            .unwrap()
            .operation;
        assert!(matches!(
            service.transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation.clone(),
                transition: PublishTransition::BeginFinalization,
            }),
            Err(PublicationError::OperationCodec(
                PublishRecordError::InvalidPhasePayload { .. }
            ))
        ));

        let uploaded = uploaded_rows(&staged);
        for (expected, next) in staged
            .chunks(MAX_PUBLICATION_BATCH_ROWS)
            .zip(uploaded.chunks(MAX_PUBLICATION_BATCH_ROWS))
        {
            operation = service
                .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    staged_object_updates: expected
                        .iter()
                        .cloned()
                        .zip(next.iter().cloned())
                        .map(|(expected, next)| StagedObjectUpdate { expected, next })
                        .collect(),
                })
                .unwrap()
                .operation;
        }

        operation = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                manifest_rows: manifest[..MAX_PUBLICATION_BATCH_ROWS].to_vec(),
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap()
            .operation;
        assert!(matches!(
            service.transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation.clone(),
                transition: PublishTransition::BeginFinalization,
            }),
            Err(PublicationError::OperationCodec(
                PublishRecordError::InvalidPhasePayload { .. }
            ))
        ));
        operation = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                manifest_rows: manifest[MAX_PUBLICATION_BATCH_ROWS..].to_vec(),
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap()
            .operation;
        assert!(operation.has_complete_publication_closure());
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::StagedObject,
                &staged_object_prefix(root(), operation.operation_id),
            ),
            TEST_BATCH_ROWS
        );
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            TEST_BATCH_ROWS
        );

        operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let finalized = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                artifact: published_artifact(&operation),
                expected_operation: operation,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();
        assert_eq!(finalized.operation.phase, PublishPhase::Published);
        assert_eq!(
            finalized.result.path_generation,
            Generation::new(1).unwrap()
        );
        let visible = get_visible_path_at(
            &store,
            RootReadContext::current(&store, root(), placement(), owner()).unwrap(),
            &workbench(),
            &path("outputs/large.bin"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(visible.artifact_revision_id, revision_id);
        assert_eq!(visible.logical_size, TEST_BATCH_ROWS as u64);
    }

    #[test]
    fn replace_creates_zero_reference_gc_candidate() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let artifact_path = path("outputs/replace.bin");
        let old_revision = revision(200);
        publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(200),
            old_revision,
            artifact_path.clone(),
            PublishClaim::CreateOnly,
            1,
        );
        let new_revision = revision(201);
        let replaced = publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(201),
            new_revision,
            artifact_path.clone(),
            PublishClaim::ReplaceOnly {
                expected_generation: Generation::new(1).unwrap(),
            },
            1,
        );
        let version = store.current_read_version().unwrap();
        let old = store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), old_revision),
                version,
            )
            .unwrap()
            .map(|payload| ArtifactRevisionRecord::decode(&payload).unwrap())
            .unwrap();
        assert_eq!(old.strong_reference_count, 0);
        assert_eq!(old.reference_epoch, ReferenceEpoch::new(2));
        assert_eq!(old.last_zero_ref_version, Some(replaced.commit_version));
        let candidate = store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::GcCandidate,
                &gc_candidate_key(root(), old_revision, ReferenceEpoch::new(2)),
                version,
            )
            .unwrap()
            .map(|payload| GcCandidateRecord::decode(&payload).unwrap())
            .unwrap();
        assert_eq!(candidate.last_zero_ref_version, replaced.commit_version);
        assert_eq!(candidate.claim_state, GcClaimState::Candidate);
        let visible = get_visible_path_at(
            &store,
            RootReadContext::current(&store, root(), placement(), owner()).unwrap(),
            &workbench(),
            &artifact_path,
        )
        .unwrap()
        .unwrap();
        assert_eq!(visible.generation, Generation::new(2).unwrap());
        assert_eq!(visible.artifact_revision_id, new_revision);
    }

    #[test]
    fn dependency_free_append_rematerialization_releases_old_head_for_gc() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let artifact_path = path("outputs/rematerialized-append.bin");
        let base_revision = revision(205);
        publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(205),
            base_revision,
            artifact_path.clone(),
            PublishClaim::CreateOnly,
            1,
        );

        let squashed_revision = revision(206);
        let appended = publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(206),
            squashed_revision,
            artifact_path.clone(),
            PublishClaim::Append {
                expected_generation: Generation::new(1).unwrap(),
                base_revision_id: base_revision,
                append_offset: 1,
            },
            2,
        );
        let version = store.current_read_version().unwrap();
        let base = ArtifactRevisionRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), base_revision),
                version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(base.strong_reference_count, 0);
        assert_eq!(base.reference_epoch, ReferenceEpoch::new(2));
        assert_eq!(base.last_zero_ref_version, Some(appended.commit_version));
        let candidate = GcCandidateRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::GcCandidate,
                &gc_candidate_key(root(), base_revision, ReferenceEpoch::new(2)),
                version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(candidate.claim_state, GcClaimState::Candidate);

        let squashed = ArtifactRevisionRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), squashed_revision),
                version,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(squashed.dependency_count, 0);
        assert_eq!(squashed.dependency_depth, 0);
        assert_eq!(
            squashed.dependency_digest,
            dependency_owner_digest(&[]).unwrap()
        );
        for object_index in 0..2 {
            let row = ArtifactManifestRow::decode(
                &payload_at(
                    &store,
                    MetadataFamily::ArtifactManifest,
                    &artifact_manifest_key(root(), squashed_revision, object_index),
                    version,
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(row.physical_owner_revision_id, squashed_revision);
            assert_eq!(row.physical_object_index, object_index);
            assert!(row.append_segment.is_none());
        }
        let visible = get_visible_path_at(
            &store,
            RootReadContext::current(&store, root(), placement(), owner()).unwrap(),
            &workbench(),
            &artifact_path,
        )
        .unwrap()
        .unwrap();
        assert_eq!(visible.generation, Generation::new(2).unwrap());
        assert_eq!(visible.artifact_revision_id, squashed_revision);
    }

    #[test]
    fn main_holt_projection_boundary_remains_full_lifecycle_compatible() {
        let directory = tempdir().unwrap();
        let database_path = directory.path().join("metadata-budget-compatibility");
        let mut counter = 1;
        let store = ready_file_store(&database_path, &mut counter);
        let service = PublicationService::new(&store);
        let projection = budget_projection(998);
        assert_eq!(projection.encode().unwrap().len(), 61_203);

        let replaced_path = path("outputs/boundary-replace.bin");
        publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(215),
            revision(215),
            replaced_path.clone(),
            PublishClaim::CreateOnly,
            1,
            projection.clone(),
        );
        let removed_path = path("outputs/boundary-remove.bin");
        publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(216),
            revision(216),
            removed_path.clone(),
            PublishClaim::CreateOnly,
            1,
            projection,
        );
        drop(store);

        let (store, capture) = open_capturing_file_store(&database_path);
        let service = PublicationService::new(&store);
        let replaced = publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(217),
            revision(217),
            replaced_path,
            PublishClaim::ReplaceOnly {
                expected_generation: Generation::new(1).unwrap(),
            },
            1,
            TypedProjection::empty(),
        );
        assert_eq!(replaced.result.path_generation, Generation::new(2).unwrap());
        let replace_bytes =
            capture.with_last_commit(crate::workspace::test_support::transaction_bytes);
        assert_eq!(replace_bytes, 11_799_467);

        let removed = remove_path(
            &store,
            RemovePathRequest {
                context: RootWriteContext::current(
                    &store,
                    root(),
                    shard(),
                    ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
                    placement(),
                    owner(),
                    next_request(&mut counter),
                )
                .unwrap(),
                workbench_id: workbench(),
                path: removed_path,
                expected_generation: Generation::new(1).unwrap(),
            },
        )
        .unwrap();
        assert_eq!(removed.removed_artifact_revision_id, revision(216));
        let remove_bytes =
            capture.with_last_commit(crate::workspace::test_support::transaction_bytes);
        assert_eq!(remove_bytes, 11_793_108);
    }

    #[test]
    fn main_max_path_and_dependency_projection_boundary_remains_compatible() {
        let directory = tempdir().unwrap();
        let database_path = directory.path().join("metadata-max-domain-compatibility");
        let mut counter = 1;
        let store = ready_file_store(&database_path, &mut counter);
        let service = PublicationService::new(&store);
        let dependencies = seed_maximum_dependencies(&service, &store, &mut counter);
        assert_eq!(dependencies.len(), usize::from(MAX_DEPENDENCY_COUNT));
        let projection = budget_projection(932);
        assert_eq!(projection.encode().unwrap().len(), 57_243);

        let replaced_path = maximum_path(100);
        publish_with_dependencies_and_projection(
            &service,
            &store,
            &mut counter,
            operation_id(40_000),
            revision(40_000),
            replaced_path.clone(),
            PublishClaim::CreateOnly,
            &dependencies,
            projection.clone(),
        );
        let removed_path = maximum_path(101);
        publish_with_dependencies_and_projection(
            &service,
            &store,
            &mut counter,
            operation_id(40_001),
            revision(40_001),
            removed_path.clone(),
            PublishClaim::CreateOnly,
            &dependencies,
            projection,
        );
        drop(store);

        let store = crate::workspace::test_support::open_file(&database_path, shard()).unwrap();
        let service = PublicationService::new(&store);
        let replaced = publish_with_dependencies_and_projection(
            &service,
            &store,
            &mut counter,
            operation_id(40_002),
            revision(40_002),
            replaced_path,
            PublishClaim::ReplaceOnly {
                expected_generation: Generation::new(1).unwrap(),
            },
            &[],
            TypedProjection::empty(),
        );
        assert_eq!(replaced.result.path_generation, Generation::new(2).unwrap());
        let removed = remove_path(
            &store,
            RemovePathRequest {
                context: RootWriteContext::current(
                    &store,
                    root(),
                    shard(),
                    ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
                    placement(),
                    owner(),
                    next_request(&mut counter),
                )
                .unwrap(),
                workbench_id: workbench(),
                path: removed_path,
                expected_generation: Generation::new(1).unwrap(),
            },
        )
        .unwrap();
        assert_eq!(removed.removed_artifact_revision_id, revision(40_001));
    }

    #[test]
    fn maximum_event_sized_disjoint_sixty_field_republish_pins_transaction_bytes() {
        let mut counter = 1;
        let (store, capture) = ready_capturing_store(&mut counter);
        let service = PublicationService::new(&store);
        let artifact_path = path("outputs/disjoint-republish.bin");
        let old_projection = budget_projection_with_prefix("oldset", 488);
        let next_projection = budget_projection_with_prefix("newset", 488);
        assert_eq!(old_projection.fields().len(), 60);
        assert_eq!(next_projection.fields().len(), 60);
        assert_eq!(old_projection.encode().unwrap().len(), 30_603);
        assert_eq!(next_projection.encode().unwrap().len(), 30_603);
        assert!(old_projection
            .fields()
            .keys()
            .all(|field| !next_projection.fields().contains_key(field)));
        let event = ChangeEventRecord {
            workbench_id: workbench(),
            workspace_incarnation_id: incarnation(9),
            kind: ChangeEventKind::ArtifactPublished,
            artifact_revision_id: Some(revision(220)),
            commit_id: None,
            operation_id: Some(operation_id(220)),
            path: Some(artifact_path.clone()),
            before: old_projection.clone(),
            after: next_projection.clone(),
        }
        .encode()
        .unwrap();
        assert_eq!(event.len(), 61_323);
        let oversized_before = budget_projection_with_prefix("oldmax", 489);
        let oversized_after = budget_projection_with_prefix("newmax", 489);
        let oversized_event = ChangeEventRecord {
            workbench_id: workbench(),
            workspace_incarnation_id: incarnation(9),
            kind: ChangeEventKind::ArtifactPublished,
            artifact_revision_id: Some(revision(220)),
            commit_id: None,
            operation_id: Some(operation_id(220)),
            path: Some(artifact_path.clone()),
            before: oversized_before,
            after: oversized_after,
        }
        .encode()
        .unwrap();
        assert_eq!(oversized_event.len(), 61_443);
        assert!(event.len() <= 60 * 1024);
        assert!(oversized_event.len() > 60 * 1024);
        publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(219),
            revision(219),
            artifact_path.clone(),
            PublishClaim::CreateOnly,
            1,
            old_projection,
        );
        let replaced = publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(220),
            revision(220),
            artifact_path,
            PublishClaim::ReplaceOnly {
                expected_generation: Generation::new(1).unwrap(),
            },
            1,
            next_projection,
        );
        assert_eq!(replaced.result.path_generation, Generation::new(2).unwrap());

        assert_eq!(
            capture.with_last_commit(crate::workspace::test_support::transaction_bytes),
            9_860_740
        );
    }

    #[test]
    fn multi_megabyte_body_size_is_not_metadata_transaction_bytes() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(216);
        let mut staged = staged_rows(revision_id, 1);
        staged[0].expected_length = 3_600_000;
        let manifest = manifest_rows(&staged);
        let operation = begin_operation(
            &service,
            &store,
            &mut counter,
            publish_operation(
                operation_id(216),
                revision_id,
                path("outputs/large-body.bin"),
                PublishClaim::CreateOnly,
                &staged,
                &manifest,
            ),
        );
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let mut artifact = published_artifact(&operation);
        artifact.logical_size = 3_600_000;
        let finalized = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                artifact,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();

        assert_eq!(finalized.result.logical_size, 3_600_000);
        assert_eq!(finalized.operation.phase, PublishPhase::Published);
    }

    #[test]
    fn maximum_valid_projection_union_rejects_before_metadata_side_effects() {
        let directory = tempdir().unwrap();
        let database_path = directory.path().join("metadata-budget-rejection");
        let mut counter = 1;
        let store = ready_file_store(&database_path, &mut counter);
        let service = PublicationService::new(&store);
        let artifact_path = path("outputs/disjoint-projections.bin");
        let old_projection = budget_projection_with_prefix("oldget", 998);
        let next_projection = budget_projection_with_prefix("newget", 993);
        assert_eq!(old_projection.encode().unwrap().len(), 61_203);
        assert_eq!(next_projection.encode().unwrap().len(), 60_903);
        assert!(old_projection
            .fields()
            .keys()
            .all(|field| !next_projection.fields().contains_key(field)));
        publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(218),
            revision(218),
            artifact_path.clone(),
            PublishClaim::CreateOnly,
            1,
            old_projection,
        );

        let revision_id = revision(217);
        let staged = staged_rows(revision_id, 1);
        let manifest = manifest_rows(&staged);
        let operation = begin_operation(
            &service,
            &store,
            &mut counter,
            publish_operation(
                operation_id(217),
                revision_id,
                artifact_path.clone(),
                PublishClaim::ReplaceOnly {
                    expected_generation: Generation::new(1).unwrap(),
                },
                &staged,
                &manifest,
            ),
        );
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let mut artifact = published_artifact(&operation);
        artifact.typed_index_projection = next_projection.encode().unwrap();
        let version_before = store.current_read_version().unwrap();
        let recovery_before = store.recovery_state().unwrap();
        let path_key = path_current_key(root(), incarnation(9), &artifact_path);
        let path_before = payload_at(
            &store,
            MetadataFamily::PathCurrent,
            &path_key,
            version_before,
        )
        .unwrap();
        let error = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation.clone(),
                artifact,
                dependency_owner_revision_ids: Vec::new(),
            })
            .expect_err("maximum valid projection union must be rejected before commit");
        assert!(matches!(
            error,
            PublicationError::Meta(MetaError::InvalidCommand { ref reason })
                if reason == "event projection exceeds size bound"
        ));
        assert_eq!(store.current_read_version().unwrap(), version_before);
        assert_eq!(store.recovery_state().unwrap(), recovery_before);
        assert_eq!(
            payload_at(
                &store,
                MetadataFamily::PathCurrent,
                &path_key,
                version_before,
            )
            .unwrap(),
            path_before
        );
        assert!(payload_at(
            &store,
            MetadataFamily::ArtifactRevision,
            &artifact_revision_key(root(), revision_id),
            version_before,
        )
        .is_none());
        assert!(read_revision_claim(&store, revision_id).is_some());
        let durable_operation = PublishOperationRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Publish, operation.operation_id),
                version_before,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(durable_operation.phase, PublishPhase::Finalizing);
    }

    #[test]
    fn secondary_indexes_switch_atomically_and_survive_reopen() {
        let directory = tempdir().unwrap();
        let database_path = directory.path().join("metadata");
        let mut counter = 1;
        let store = ready_file_store(&database_path, &mut counter);
        let service = PublicationService::new(&store);
        let artifact_path = path("outputs/indexed.bin");
        let stage_field = QueryFieldId::new("run.stage").unwrap();
        let owner_field = QueryFieldId::new("run.owner").unwrap();
        let old_projection = TypedProjection::new(BTreeMap::from([
            (stage_field.clone(), QueryScalar::Unsigned(1)),
            (owner_field.clone(), QueryScalar::String("alpha".to_owned())),
        ]))
        .unwrap();
        let created = publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(220),
            revision(220),
            artifact_path.clone(),
            PublishClaim::CreateOnly,
            1,
            old_projection.clone(),
        );
        let old_stage_key = secondary_index_key(
            root(),
            &stage_field,
            &QueryScalar::Unsigned(1),
            incarnation(9),
            &artifact_path,
        );
        let old_owner_key = secondary_index_key(
            root(),
            &owner_field,
            &QueryScalar::String("alpha".to_owned()),
            incarnation(9),
            &artifact_path,
        );
        let old_read = ReadVersion::new(created.commit_version.get()).unwrap();
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::SecondaryIndex,
                &old_stage_key,
                old_read,
            )
            .unwrap()
            .is_some());

        let new_projection = TypedProjection::new(BTreeMap::from([(
            stage_field.clone(),
            QueryScalar::Unsigned(2),
        )]))
        .unwrap();
        let replaced = publish_full_with_projection(
            &service,
            &store,
            &mut counter,
            operation_id(221),
            revision(221),
            artifact_path.clone(),
            PublishClaim::ReplaceOnly {
                expected_generation: Generation::new(1).unwrap(),
            },
            1,
            new_projection.clone(),
        );
        let new_stage_key = secondary_index_key(
            root(),
            &stage_field,
            &QueryScalar::Unsigned(2),
            incarnation(9),
            &artifact_path,
        );
        let current_read = ReadVersion::new(replaced.commit_version.get()).unwrap();
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::SecondaryIndex,
                &old_stage_key,
                current_read,
            )
            .unwrap()
            .is_none());
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::SecondaryIndex,
                &old_owner_key,
                current_read,
            )
            .unwrap()
            .is_none());
        let current_payload = store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::SecondaryIndex,
                &new_stage_key,
                current_read,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            SecondaryIndexRecord::decode(&current_payload).unwrap(),
            SecondaryIndexRecord {
                path_generation: Generation::new(2).unwrap(),
                compact_projection: new_projection,
            }
        );
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::SecondaryIndex,
                &old_stage_key,
                old_read,
            )
            .unwrap()
            .is_some());
        drop(store);

        let reopened = crate::workspace::test_support::open_file(&database_path, shard()).unwrap();
        assert_eq!(
            reopened
                .read_at(
                    root(),
                    placement(),
                    owner(),
                    MetadataFamily::SecondaryIndex,
                    &new_stage_key,
                    current_read,
                )
                .unwrap(),
            Some(current_payload)
        );
    }

    #[test]
    fn append_base_mismatch_fails_without_changing_the_path() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let artifact_path = path("outputs/append.bin");
        let base_revision = revision(300);
        publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(300),
            base_revision,
            artifact_path.clone(),
            PublishClaim::CreateOnly,
            1,
        );
        let staged = Vec::new();
        let manifest = Vec::new();
        let operation = publish_operation(
            operation_id(301),
            revision(301),
            artifact_path.clone(),
            PublishClaim::Append {
                expected_generation: Generation::new(1).unwrap(),
                base_revision_id: revision(999),
                append_offset: 1,
            },
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        assert_eq!(
            service.finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                artifact: published_artifact(&operation),
                expected_operation: operation,
                dependency_owner_revision_ids: Vec::new(),
            }),
            Err(PublicationError::AppendBaseRevisionMismatch)
        );
        let visible = get_visible_path_at(
            &store,
            RootReadContext::current(&store, root(), placement(), owner()).unwrap(),
            &workbench(),
            &artifact_path,
        )
        .unwrap()
        .unwrap();
        assert_eq!(visible.generation, Generation::new(1).unwrap());
        assert_eq!(visible.artifact_revision_id, base_revision);
    }

    #[test]
    fn newer_owner_takeover_and_finalize_have_one_durable_winner() {
        let staged = Vec::new();
        let manifest = Vec::new();

        {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let operation = publish_operation(
                operation_id(400),
                revision(400),
                path("outputs/race.bin"),
                PublishClaim::CreateOnly,
                &staged,
                &manifest,
            );
            let operation = begin_operation(&service, &store, &mut counter, operation);
            let operation = service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    transition: PublishTransition::BeginFinalization,
                })
                .unwrap()
                .operation;
            let finalized = service
                .finalize_publish(FinalizePublishRequest {
                    context: publication_context(&store, &mut counter),
                    artifact: published_artifact(&operation),
                    expected_operation: operation.clone(),
                    dependency_owner_revision_ids: Vec::new(),
                })
                .unwrap();
            assert_eq!(finalized.operation.phase, PublishPhase::Published);

            store
                .advance_owner_epoch(Some(owner()), successor_owner())
                .unwrap();
            let takeover = service.take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                context: publication_context_for_owner(&store, &mut counter, successor_owner()),
                expected_operation: operation,
                observed_now_ms: 0,
                maximum_clock_skew_ms: 30_000,
                terminal_error: PublishTerminalError {
                    kind: PublishTerminalErrorKind::AbortedByCaller,
                    message: "prior owner was fenced".to_owned(),
                    evidence_digest: None,
                },
            });
            assert!(matches!(
                takeover,
                Err(PublicationError::ConcurrentMutation)
            ));
            assert!(get_visible_path_at(
                &store,
                RootReadContext::current(&store, root(), placement(), successor_owner()).unwrap(),
                &workbench(),
                &path("outputs/race.bin"),
            )
            .unwrap()
            .is_some());
        }

        {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let operation = publish_operation(
                operation_id(401),
                revision(401),
                path("outputs/race-abort.bin"),
                PublishClaim::CreateOnly,
                &staged,
                &manifest,
            );
            let operation = begin_operation(&service, &store, &mut counter, operation);
            let operation = service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    transition: PublishTransition::BeginFinalization,
                })
                .unwrap()
                .operation;
            let same_owner = service.take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation.clone(),
                observed_now_ms: 999_999,
                maximum_clock_skew_ms: 30_000,
                terminal_error: PublishTerminalError {
                    kind: PublishTerminalErrorKind::AbortedByCaller,
                    message: "not durably expired".to_owned(),
                    evidence_digest: None,
                },
            });
            assert_eq!(
                same_owner,
                Err(PublicationError::ActivityLeaseNotExpired {
                    deadline_ms: 1_000_000,
                    lease_clock_ms: 999_999,
                    maximum_clock_skew_ms: 30_000,
                })
            );

            store
                .advance_owner_epoch(Some(owner()), successor_owner())
                .unwrap();
            let shared_version = store.current_read_version().unwrap();
            let takeover_context = publication_context_at(
                successor_owner(),
                next_request(&mut counter),
                shared_version,
            );
            let finalize_context = publication_context_at(
                successor_owner(),
                next_request(&mut counter),
                shared_version,
            );
            let abort_winner = service
                .take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                    context: takeover_context,
                    expected_operation: operation.clone(),
                    observed_now_ms: 0,
                    maximum_clock_skew_ms: 30_000,
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::AbortedByCaller,
                        message: "prior owner was fenced".to_owned(),
                        evidence_digest: None,
                    },
                })
                .unwrap();
            assert_eq!(abort_winner.operation.phase, PublishPhase::Aborting);
            assert!(abort_winner.operation.publication_absence_proof.is_some());
            let finalize_loser = service.finalize_publish(FinalizePublishRequest {
                context: finalize_context,
                artifact: published_artifact(&operation),
                expected_operation: operation,
                dependency_owner_revision_ids: Vec::new(),
            });
            assert!(matches!(
                finalize_loser,
                Err(PublicationError::Meta(
                    MetaError::WriteReadVersionMismatch { .. }
                ))
            ));
            assert_eq!(
                get_visible_path_at(
                    &store,
                    RootReadContext::current(&store, root(), placement(), successor_owner(),)
                        .unwrap(),
                    &workbench(),
                    &path("outputs/race-abort.bin"),
                )
                .unwrap(),
                None
            );
        }
    }

    #[test]
    fn stale_publication_driver_is_classified_as_concurrent_progress() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let staged = staged_rows(revision(402), 2);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(402),
            revision(402),
            path("outputs/concurrent-progress.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let stale = begin_operation(&service, &store, &mut counter, operation);
        service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: stale.clone(),
                staged_objects: staged[..1].to_vec(),
            })
            .unwrap();

        let error = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: stale,
                staged_objects: staged[..1].to_vec(),
            })
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "publication state changed concurrently",
            "a stale driver must re-observe the durable operation before contextual authority"
        );
    }

    #[test]
    fn stale_upload_marker_is_classified_before_noop_transition_validation() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let staged = staged_rows(revision(404), 1);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(404),
            revision(404),
            path("outputs/concurrent-upload.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let stale = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                staged_objects: staged.clone(),
            })
            .unwrap()
            .operation;
        let mut uploaded = staged[0].clone();
        uploaded.provider_state = StagedProviderState::Uploaded;
        service
            .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: stale.clone(),
                staged_object_updates: vec![StagedObjectUpdate {
                    expected: staged[0].clone(),
                    next: uploaded.clone(),
                }],
            })
            .unwrap();

        assert_eq!(
            service.mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: stale,
                staged_object_updates: vec![StagedObjectUpdate {
                    expected: uploaded.clone(),
                    next: uploaded,
                }],
            }),
            Err(PublicationError::ConcurrentMutation)
        );
    }

    #[test]
    fn stale_publication_finalizer_joins_the_durable_winner() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let staged = Vec::new();
        let manifest = Vec::new();
        let operation = publish_operation(
            operation_id(403),
            revision(403),
            path("outputs/concurrent-finalize.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let finalizing = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let artifact = published_artifact(&finalizing);
        let winner = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                artifact: artifact.clone(),
                expected_operation: finalizing.clone(),
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();
        assert_eq!(winner.operation.phase, PublishPhase::Published);

        assert_eq!(
            service.finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                artifact,
                expected_operation: finalizing,
                dependency_owner_revision_ids: Vec::new(),
            }),
            Err(PublicationError::ConcurrentMutation)
        );
    }

    #[test]
    fn begin_publish_binds_operation_to_admitting_owner_epoch() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let mut operation = publish_operation(
            operation_id(402),
            revision(402),
            path("outputs/wrong-owner.bin"),
            PublishClaim::CreateOnly,
            &[],
            &[],
        );
        operation.initiating_owner_epoch = successor_owner();
        seal_publish_operation(&mut operation);

        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation,
            }),
            Err(PublicationError::InitiatingOwnerEpochMismatch {
                expected: owner(),
                actual: successor_owner(),
            })
        );
    }

    #[test]
    fn activity_heartbeat_is_initiator_only_extend_only_and_fences_expiry() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let operation = begin_operation(
            &service,
            &store,
            &mut counter,
            publish_operation(
                operation_id(403),
                revision(403),
                path("outputs/leased-upload.bin"),
                PublishClaim::CreateOnly,
                &[],
                &[],
            ),
        );

        let heartbeated = service
            .heartbeat_publish(HeartbeatPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                activity_deadline_ms: 2_000_000,
            })
            .unwrap()
            .operation;
        assert_eq!(heartbeated.activity_deadline_ms, 2_000_000);
        assert_eq!(heartbeated.phase, PublishPhase::Uploading);

        assert_eq!(
            service.heartbeat_publish(HeartbeatPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: heartbeated.clone(),
                activity_deadline_ms: 2_000_000,
            }),
            Err(PublicationError::ActivityDeadlineNotExtended {
                current: 2_000_000,
                requested: 2_000_000,
            })
        );
        assert_eq!(
            service.heartbeat_publish(HeartbeatPublishRequest {
                context: publication_context_for_owner(&store, &mut counter, successor_owner(),),
                expected_operation: heartbeated.clone(),
                activity_deadline_ms: 3_000_000,
            }),
            Err(PublicationError::HeartbeatOwnerMismatch {
                initiating: owner(),
                current: successor_owner(),
            })
        );

        assert_eq!(
            service.take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: heartbeated.clone(),
                observed_now_ms: 2_029_999,
                maximum_clock_skew_ms: 30_000,
                terminal_error: PublishTerminalError {
                    kind: PublishTerminalErrorKind::ActivityLeaseExpired,
                    message: "durable activity lease expired".to_owned(),
                    evidence_digest: None,
                },
            }),
            Err(PublicationError::ActivityLeaseNotExpired {
                deadline_ms: 2_000_000,
                lease_clock_ms: 2_029_999,
                maximum_clock_skew_ms: 30_000,
            })
        );
        let taken_over = service
            .take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: heartbeated,
                observed_now_ms: 2_030_000,
                maximum_clock_skew_ms: 30_000,
                terminal_error: PublishTerminalError {
                    kind: PublishTerminalErrorKind::ActivityLeaseExpired,
                    message: "durable activity lease expired".to_owned(),
                    evidence_digest: None,
                },
            })
            .unwrap()
            .operation;
        assert_eq!(taken_over.phase, PublishPhase::Aborting);
        assert_eq!(
            taken_over.terminal_error.unwrap().kind,
            PublishTerminalErrorKind::ActivityLeaseExpired
        );
    }

    #[test]
    fn aborted_cleanup_removes_staged_and_manifest_rows_in_order() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(500);
        let staged = staged_rows(revision_id, 3);
        let uploaded = uploaded_rows(&staged);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(500),
            revision_id,
            path("outputs/cleanup.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginAbort {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::AbortedByCaller,
                        message: "caller cancelled".to_owned(),
                        evidence_digest: None,
                    },
                },
            })
            .unwrap()
            .operation;
        let mut operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        let cleanup_updates = uploaded
            .iter()
            .cloned()
            .map(|expected| {
                let mut next = expected.clone();
                next.provider_state = StagedProviderState::Aborted;
                next.cleanup_state = StagedCleanupState::Deleted;
                StagedObjectUpdate { expected, next }
            })
            .collect();
        operation = service
            .cleanup_publish_batch(CleanupPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                staged_object_updates: cleanup_updates,
            })
            .unwrap()
            .operation;
        assert_eq!(operation.cleanup_staged_object_cursor, 3);
        assert_eq!(operation.cleanup_manifest_cursor, 0);
        operation = service
            .cleanup_publish_batch(CleanupPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                staged_object_updates: Vec::new(),
            })
            .unwrap()
            .operation;
        assert_eq!(operation.cleanup_manifest_cursor, 3);
        operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::FinishCleanup,
            })
            .unwrap()
            .operation;
        assert_eq!(operation.phase, PublishPhase::Cleaned);
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::StagedObject,
                &staged_object_prefix(root(), operation.operation_id),
            ),
            0
        );
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            0
        );
    }

    fn read_revision_claim(store: &MetaShard, revision_id: ArtifactRevisionId) -> Option<Vec<u8>> {
        let version = store.current_read_version().unwrap();
        store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::ArtifactRevision,
                &artifact_revision_claim_key(root(), revision_id),
                version,
            )
            .unwrap()
    }

    #[test]
    fn begin_publish_rejects_second_operation_for_same_revision_until_cleanup_completes() {
        // Staged rows derive permanent object keys from the revision id alone,
        // so two operations owning one revision could destroy each other's
        // published objects during abort cleanup (risk report P1-A). The
        // begin-time exclusive claim must reject the second operation and must
        // be released only when the first operation's cleanup completes.
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(910);
        let staged = staged_rows(revision_id, 1);
        let manifest = manifest_rows(&staged);
        // Both operations would stage rows naming identical provider keys.
        assert_eq!(
            staged[0].object_key,
            object_block_key(shard(), root(), revision_id, 0)
        );

        let operation_a = publish_operation(
            operation_id(910),
            revision_id,
            path("outputs/duplicate-revision-claim.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let begun_a = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: operation_a.clone(),
            })
            .expect("operation A begins for revision R");
        assert!(!begun_a.replayed);
        let claim_payload =
            read_revision_claim(&store, revision_id).expect("begin creates the revision claim");
        assert_eq!(
            ArtifactRevisionClaimRecord::decode(&claim_payload).unwrap(),
            ArtifactRevisionClaimRecord {
                operation_id: operation_id(910),
            }
        );

        let operation_b = publish_operation(
            operation_id(911),
            revision_id,
            path("outputs/duplicate-revision-claim.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: operation_b.clone(),
            }),
            Err(PublicationError::RevisionClaimHeld {
                revision: revision_id,
                operation_id: operation_id(910),
            })
        );

        // Exact same-operation replay is unaffected by the held claim.
        let replayed = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: operation_a,
            })
            .expect("operation A replays its begin");
        assert!(replayed.replayed);

        // The claim survives abort and cleaning: provider objects for the
        // revision may still exist until cleanup finishes.
        let aborting = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: begun_a.operation,
                transition: PublishTransition::BeginAbort {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::AbortedByCaller,
                        message: "caller cancelled".to_owned(),
                        evidence_digest: None,
                    },
                },
            })
            .unwrap()
            .operation;
        let cleaning = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: aborting,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: operation_b.clone(),
            }),
            Err(PublicationError::RevisionClaimHeld {
                revision: revision_id,
                operation_id: operation_id(910),
            })
        );

        let cleaned = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: cleaning,
                transition: PublishTransition::FinishCleanup,
            })
            .unwrap()
            .operation;
        assert_eq!(cleaned.phase, PublishPhase::Cleaned);
        assert_eq!(read_revision_claim(&store, revision_id), None);

        // With the previous owner's cleanup durably complete, the revision
        // identity is claimable again.
        let begun_b = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: operation_b,
            })
            .expect("operation B begins after operation A's cleanup completes");
        assert!(!begun_b.replayed);
    }

    fn overwrite_revision_claim(
        store: &MetaShard,
        counter: &mut u128,
        revision_id: ArtifactRevisionId,
        next: Option<ArtifactRevisionClaimRecord>,
    ) {
        let claim_key = artifact_revision_claim_key(root(), revision_id);
        let current = read_revision_claim(store, revision_id).expect("claim row exists");
        let mutation = match &next {
            Some(record) => CommandMutation::Put {
                family: MetadataFamily::ArtifactRevision,
                key: claim_key.clone(),
                value: record.encode().unwrap(),
            },
            None => CommandMutation::Delete {
                family: MetadataFamily::ArtifactRevision,
                key: claim_key.clone(),
            },
        };
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: request(*counter),
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: vec![CommandPredicate::Value {
                        family: MetadataFamily::ArtifactRevision,
                        key: claim_key.clone(),
                        expected: Some(current),
                    }],
                    mutations: vec![mutation],
                    history_projection: vec![HistoryProjection {
                        family: MetadataFamily::ArtifactRevision,
                        key: claim_key,
                    }],
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .expect("raw claim overwrite for legacy simulation");
        *counter += 1;
    }

    #[test]
    fn finish_cleanup_preserves_foreign_claim_and_tolerates_absent_claim() {
        // release_revision_claim must delete only the claim its own operation
        // holds: a foreign claim guards that operation's lifecycle, and an
        // absent claim means the operation predates claims entirely.
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);

        let finish_cleanup = |service: &PublicationService<'_>, counter: &mut u128, operation| {
            let aborting = service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, counter),
                    expected_operation: operation,
                    transition: PublishTransition::BeginAbort {
                        terminal_error: PublishTerminalError {
                            kind: PublishTerminalErrorKind::AbortedByCaller,
                            message: "caller cancelled".to_owned(),
                            evidence_digest: None,
                        },
                    },
                })
                .unwrap()
                .operation;
            let cleaning = service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, counter),
                    expected_operation: aborting,
                    transition: PublishTransition::BeginCleaning,
                })
                .unwrap()
                .operation;
            service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, counter),
                    expected_operation: cleaning,
                    transition: PublishTransition::FinishCleanup,
                })
                .unwrap()
                .operation
        };

        // Foreign claim: swap ownership to another operation id before the
        // owner's cleanup finishes; the claim must survive it untouched.
        let foreign_revision = revision(930);
        let begun = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: publish_operation(
                    operation_id(930),
                    foreign_revision,
                    path("outputs/foreign-claim.bin"),
                    PublishClaim::CreateOnly,
                    &[],
                    &[],
                ),
            })
            .unwrap()
            .operation;
        let foreign_claim = ArtifactRevisionClaimRecord {
            operation_id: operation_id(931),
        };
        overwrite_revision_claim(&store, &mut counter, foreign_revision, Some(foreign_claim));
        let cleaned = finish_cleanup(&service, &mut counter, begun);
        assert_eq!(cleaned.phase, PublishPhase::Cleaned);
        assert_eq!(
            read_revision_claim(&store, foreign_revision)
                .map(|payload| ArtifactRevisionClaimRecord::decode(&payload).unwrap()),
            Some(foreign_claim)
        );

        // Absent claim: a pre-claim legacy operation still cleans up.
        let legacy_revision = revision(940);
        let begun = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: publish_operation(
                    operation_id(940),
                    legacy_revision,
                    path("outputs/legacy-claimless.bin"),
                    PublishClaim::CreateOnly,
                    &[],
                    &[],
                ),
            })
            .unwrap()
            .operation;
        overwrite_revision_claim(&store, &mut counter, legacy_revision, None);
        let cleaned = finish_cleanup(&service, &mut counter, begun);
        assert_eq!(cleaned.phase, PublishPhase::Cleaned);
        assert_eq!(read_revision_claim(&store, legacy_revision), None);
    }

    #[test]
    fn finalize_publish_releases_revision_claim() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(920);

        let outcome = publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(920),
            revision_id,
            path("outputs/claim-release.bin"),
            PublishClaim::CreateOnly,
            1,
        );
        assert_eq!(outcome.operation.phase, PublishPhase::Published);

        // The same command that created the Available revision row released
        // the in-flight claim; only the 32-byte revision row remains.
        assert_eq!(read_revision_claim(&store, revision_id), None);
        let version = store.current_read_version().unwrap();
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), revision_id),
                version,
            )
            .unwrap()
            .is_some());
    }

    fn quarantine_after_abort(
        service: &PublicationService<'_>,
        store: &MetaShard,
        counter: &mut u128,
        operation: PublishOperationRecord,
        evidence: &[u8],
    ) -> PublishOperationRecord {
        let aborting = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter),
                expected_operation: operation,
                transition: PublishTransition::BeginAbort {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::AbortedByCaller,
                        message: "caller cancelled".to_owned(),
                        evidence_digest: None,
                    },
                },
            })
            .unwrap()
            .operation;
        let cleaning = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter),
                expected_operation: aborting,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter),
                expected_operation: cleaning,
                transition: PublishTransition::Quarantine {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::CleanupFailed,
                        message: "provider cleanup outcome is ambiguous".to_owned(),
                        evidence_digest: Some(Sha256::digest(evidence).into()),
                    },
                },
            })
            .unwrap()
            .operation
    }

    fn put_operation_row_raw(
        store: &MetaShard,
        counter: &mut u128,
        operation: &PublishOperationRecord,
    ) {
        let key = operation_key(root(), OperationKind::Publish, operation.operation_id);
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: request(*counter),
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: vec![CommandPredicate::Value {
                        family: MetadataFamily::Operation,
                        key: key.clone(),
                        expected: None,
                    }],
                    mutations: vec![CommandMutation::Put {
                        family: MetadataFamily::Operation,
                        key,
                        value: operation.encode().unwrap(),
                    }],
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .expect("raw operation write for invariant-violation simulation");
        *counter += 1;
    }

    #[test]
    fn operator_reconcile_resolves_quarantined_publish_and_releases_claim() {
        // A quarantined operation keeps its revision claim fail-closed. The
        // operator reconciliation surface must sweep the durable staging rows,
        // transition Quarantined -> Cleaned with a durable operator audit
        // trail, and release the claim so the revision id is usable again.
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(930);
        let staged = staged_rows(revision_id, 3);
        let uploaded = uploaded_rows(&staged);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(930),
            revision_id,
            path("outputs/reconcile.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = quarantine_after_abort(
            &service,
            &store,
            &mut counter,
            operation,
            b"ambiguous provider delete",
        );
        assert_eq!(operation.phase, PublishPhase::Quarantined);
        let original_evidence_digest = operation
            .terminal_error
            .as_ref()
            .and_then(|error| error.evidence_digest)
            .expect("quarantine retains evidence");

        let blocked = publish_operation(
            operation_id(931),
            revision_id,
            path("outputs/reconcile.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: blocked.clone(),
            }),
            Err(PublicationError::RevisionClaimHeld {
                revision: revision_id,
                operation_id: operation_id(930),
            })
        );

        // Staged rows sweep first; the phase and the claim stay fail-closed
        // between batches so an abandoned reconciliation changes nothing.
        let operation = service
            .reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                staged_object_rows: uploaded.clone(),
            })
            .unwrap()
            .operation;
        assert_eq!(operation.phase, PublishPhase::Quarantined);
        assert_eq!(operation.cleanup_staged_object_cursor, 3);
        assert!(read_revision_claim(&store, revision_id).is_some());

        let operation = service
            .reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                staged_object_rows: Vec::new(),
            })
            .unwrap()
            .operation;
        assert_eq!(operation.phase, PublishPhase::Quarantined);
        assert_eq!(operation.cleanup_manifest_cursor, 3);

        let finish_context = publication_context(&store, &mut counter);
        let finished = service
            .finish_reconcile_quarantined_publish(FinishReconcileQuarantinedPublishRequest {
                context: finish_context,
                expected_operation: operation.clone(),
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                reason: "verified every staged key absent at the provider".to_owned(),
                operator_evidence_digest: [7; SHA256_BYTES],
            })
            .unwrap();
        assert!(!finished.replayed);
        let resolved = finished.operation;
        assert_eq!(resolved.phase, PublishPhase::Cleaned);
        let terminal = resolved
            .terminal_error
            .as_ref()
            .expect("reconciled operation retains its terminal error");
        assert_eq!(terminal.kind, PublishTerminalErrorKind::OperatorReconciled);
        assert!(terminal
            .message
            .contains("verified every staged key absent at the provider"));
        assert_eq!(
            terminal.evidence_digest,
            Some(reconcile_evidence_digest(
                QuarantineReconcileResolution::RevisionUnpublished,
                Some(original_evidence_digest),
                [7; SHA256_BYTES],
            ))
        );
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::StagedObject,
                &staged_object_prefix(root(), resolved.operation_id),
            ),
            0
        );
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            0
        );
        assert_eq!(read_revision_claim(&store, revision_id), None);

        // An exact response-loss replay of the finish returns the stored
        // resolution without re-running it.
        let replayed = service
            .finish_reconcile_quarantined_publish(FinishReconcileQuarantinedPublishRequest {
                context: finish_context,
                expected_operation: operation,
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                reason: "verified every staged key absent at the provider".to_owned(),
                operator_evidence_digest: [7; SHA256_BYTES],
            })
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.operation, resolved);

        // The revision identity is claimable again.
        let begun = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: blocked,
            })
            .expect("revision claimable after operator reconciliation");
        assert!(!begun.replayed);
    }

    #[test]
    fn operator_reconcile_republished_revision_removes_bookkeeping_only() {
        // Rolling-upgrade shape: a claimless legacy loser shares its revision
        // id with a claimed winner that published. The loser's staged keys are
        // the winner's live provider objects, so the revision-published
        // verdict must delete only the loser's private bookkeeping rows and
        // must refuse the contradictory verdict loudly.
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(940);

        let staged = staged_rows(revision_id, 2);
        let manifest = manifest_rows(&staged);
        let loser = publish_operation(
            operation_id(940),
            revision_id,
            path("outputs/reconcile-legacy.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let loser = begin_operation(&service, &store, &mut counter, loser);
        overwrite_revision_claim(&store, &mut counter, revision_id, None);
        let loser = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: loser,
                staged_objects: staged.clone(),
            })
            .unwrap()
            .operation;
        let loser = quarantine_after_abort(
            &service,
            &store,
            &mut counter,
            loser,
            b"aborted publish cleanup found its artifact revision published",
        );

        let published = publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(941),
            revision_id,
            path("outputs/reconcile-winner.bin"),
            PublishClaim::CreateOnly,
            1,
        );
        assert_eq!(published.operation.phase, PublishPhase::Published);

        // The absent-objects verdict contradicts the published revision row.
        assert_eq!(
            service.reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: loser.clone(),
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                staged_object_rows: staged.clone(),
            }),
            Err(PublicationError::ReconcileResolutionMismatch {
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                revision_published: true,
            })
        );

        let loser = service
            .reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: loser,
                resolution: QuarantineReconcileResolution::RevisionPublished,
                staged_object_rows: staged.clone(),
            })
            .unwrap()
            .operation;
        assert_eq!(loser.cleanup_staged_object_cursor, 2);
        let resolved = service
            .finish_reconcile_quarantined_publish(FinishReconcileQuarantinedPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: loser,
                resolution: QuarantineReconcileResolution::RevisionPublished,
                reason: "revision was published by operation 941".to_owned(),
                operator_evidence_digest: [9; SHA256_BYTES],
            })
            .unwrap()
            .operation;
        assert_eq!(resolved.phase, PublishPhase::Cleaned);

        // The winner's published revision, manifest, and claim state stay
        // exactly as publication left them; only loser bookkeeping is gone.
        let version = store.current_read_version().unwrap();
        assert!(payload_at(
            &store,
            MetadataFamily::ArtifactRevision,
            &artifact_revision_key(root(), revision_id),
            version,
        )
        .is_some());
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            1
        );
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::StagedObject,
                &staged_object_prefix(root(), resolved.operation_id),
            ),
            0
        );
        assert_eq!(read_revision_claim(&store, revision_id), None);
    }

    #[test]
    fn operator_reconcile_enforces_quarantined_phase_and_sealed_cursors() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(950);
        let staged = staged_rows(revision_id, 1);
        let uploaded = uploaded_rows(&staged);
        let manifest = manifest_rows(&staged);
        let operation = publish_operation(
            operation_id(950),
            revision_id,
            path("outputs/reconcile-guard.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let aborting = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginAbort {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::AbortedByCaller,
                        message: "caller cancelled".to_owned(),
                        evidence_digest: None,
                    },
                },
            })
            .unwrap()
            .operation;
        let cleaning = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: aborting,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;

        // Reconciliation is quarantine-only: normal cleanup owns Cleaning.
        assert_eq!(
            service.reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: cleaning.clone(),
                resolution: QuarantineReconcileResolution::RevisionUnpublished,
                staged_object_rows: uploaded.clone(),
            }),
            Err(PublicationError::InvalidOperationPhase {
                expected: PublishPhase::Quarantined,
                actual: PublishPhase::Cleaning,
            })
        );

        let quarantined = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: cleaning,
                transition: PublishTransition::Quarantine {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::CleanupFailed,
                        message: "provider cleanup outcome is ambiguous".to_owned(),
                        evidence_digest: Some(Sha256::digest(b"ambiguous").into()),
                    },
                },
            })
            .unwrap()
            .operation;

        // The published-revision verdict contradicts the absent revision row.
        assert_eq!(
            service.reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: quarantined.clone(),
                resolution: QuarantineReconcileResolution::RevisionPublished,
                staged_object_rows: uploaded.clone(),
            }),
            Err(PublicationError::ReconcileResolutionMismatch {
                resolution: QuarantineReconcileResolution::RevisionPublished,
                revision_published: false,
            })
        );

        // Finishing before every durable staging row is removed refuses.
        assert!(matches!(
            service.finish_reconcile_quarantined_publish(
                FinishReconcileQuarantinedPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: quarantined,
                    resolution: QuarantineReconcileResolution::RevisionUnpublished,
                    reason: "premature".to_owned(),
                    operator_evidence_digest: [1; SHA256_BYTES],
                }
            ),
            Err(PublicationError::OperationCodec(
                PublishRecordError::InvalidPhasePayload { .. }
            ))
        ));
    }

    #[test]
    fn operator_reconcile_refuses_manifest_rows_under_published_revision() {
        // Legal flows never leave a quarantined operation with un-swept
        // manifest rows once its revision is published: those rows ARE the
        // published manifest. If that derivation is ever wrong, reconciliation
        // must stop loudly instead of deleting published data.
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let revision_id = revision(960);
        let published = publish_full(
            &service,
            &store,
            &mut counter,
            operation_id(960),
            revision_id,
            path("outputs/reconcile-live.bin"),
            PublishClaim::CreateOnly,
            1,
        );
        assert_eq!(published.operation.phase, PublishPhase::Published);

        let staged = staged_rows(revision_id, 1);
        let manifest = manifest_rows(&staged);
        let mut planted = publish_operation(
            operation_id(961),
            revision_id,
            path("outputs/reconcile-planted.bin"),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        planted.phase = PublishPhase::Quarantined;
        planted.manifest_cursor = 1;
        planted.manifest_rolling_digest = planted.manifest_seal;
        planted.manifest_last_position = Some(ManifestPosition { object_index: 0 });
        planted.terminal_error = Some(PublishTerminalError {
            kind: PublishTerminalErrorKind::CleanupFailed,
            message: "provider cleanup outcome is ambiguous".to_owned(),
            evidence_digest: Some(Sha256::digest(b"planted").into()),
        });
        put_operation_row_raw(&store, &mut counter, &planted);

        assert_eq!(
            service.reconcile_quarantined_publish_batch(ReconcileQuarantinedPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: planted.clone(),
                resolution: QuarantineReconcileResolution::RevisionPublished,
                staged_object_rows: Vec::new(),
            }),
            Err(PublicationError::ReconcileManifestRowsRemain { remaining: 1 })
        );
        assert!(matches!(
            service.finish_reconcile_quarantined_publish(
                FinishReconcileQuarantinedPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: planted,
                    resolution: QuarantineReconcileResolution::RevisionPublished,
                    reason: "planted invariant violation".to_owned(),
                    operator_evidence_digest: [2; SHA256_BYTES],
                }
            ),
            Err(PublicationError::OperationCodec(
                PublishRecordError::InvalidPhasePayload { .. }
            ))
        ));

        // The published revision's manifest survives untouched.
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::ArtifactManifest,
                &artifact_manifest_prefix(root(), revision_id),
            ),
            1
        );
    }

    #[test]
    fn append_segment_rows_remain_strictly_ordered_across_batches() {
        let revision_id = revision(600);
        let staged = staged_rows(revision_id, 2);
        let mut rows = manifest_rows(&staged);
        rows[0].row.append_segment = Some(AppendSegment {
            segment_sequence: 0,
            segment_offset: 0,
        });
        rows[1].row.append_segment = Some(AppendSegment {
            segment_sequence: 1,
            segment_offset: 1,
        });
        assert_ne!(manifest_rows_digest(&rows).unwrap(), [0; SHA256_BYTES]);
        rows.swap(0, 1);
        assert_eq!(
            manifest_rows_digest(&rows),
            Err(PublicationError::ManifestOrder)
        );
    }

    #[test]
    fn restore_staging_publishes_both_destination_manifests_in_either_order() {
        for restore_first in [false, true] {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let binding = provenance.destination_binding.as_ref().unwrap();
            install_restore_publication_authority(&store, &mut counter, &restore);
            let restore_payload = restore.encode().unwrap();

            let run = (
                binding.run_manifest_identity,
                RUN_MANIFEST_PATH,
                canonical_digest_uri(0x41),
                127,
            );
            let restore_manifest = (
                binding.restore_manifest_identity,
                RESTORE_MANIFEST_PATH,
                restore.restore_manifest.body_digest_uri.clone(),
                restore.restore_manifest.logical_size,
            );
            let order = if restore_first {
                vec![restore_manifest, run]
            } else {
                vec![run, restore_manifest]
            };

            for (index, (identity, manifest_path, body_digest_uri, logical_size)) in
                order.into_iter().enumerate()
            {
                let (operation, staged, manifest) = restore_staging_publish_operation(
                    identity,
                    manifest_path,
                    body_digest_uri,
                    logical_size,
                );
                let (initial, outcome) = drive_restore_staging_publish(
                    &service,
                    &store,
                    &mut counter,
                    operation,
                    &staged,
                    &manifest,
                    RESTORE_MANIFEST_CONTENT_TYPE,
                );
                assert_eq!(outcome.operation.phase, PublishPhase::Published);
                assert_eq!(outcome.result.path_generation, Generation::new(1).unwrap());
                let replay = service
                    .begin_publish(BeginPublishRequest {
                        context: publication_context(&store, &mut counter),
                        operation: initial,
                    })
                    .unwrap();
                assert!(replay.replayed);
                assert_eq!(replay.operation, outcome.operation);
                assert!(payload_at(
                    &store,
                    MetadataFamily::PathCurrent,
                    &path_current_key(root(), incarnation(9), &path(manifest_path)),
                    store.current_read_version().unwrap(),
                )
                .is_some());
                assert_eq!(
                    payload_at(
                        &store,
                        MetadataFamily::Operation,
                        &operation_key(root(), OperationKind::Restore, restore.operation_id),
                        store.current_read_version().unwrap(),
                    )
                    .unwrap(),
                    restore_payload,
                    "individual manifest {index} must not mutate RestoreOperation",
                );
            }
        }
    }

    #[test]
    fn restore_staging_run_manifest_uses_generic_multiblock_publication_closure() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let restore = sealed_bound_restore_operation();
        let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
            unreachable!("test fixture uses v5 provenance")
        };
        let identity = provenance
            .destination_binding
            .as_ref()
            .unwrap()
            .run_manifest_identity;
        install_restore_publication_authority(&store, &mut counter, &restore);

        let mut staged = staged_rows(identity.artifact_revision_id, 2);
        staged[0].expected_length = 71;
        staged[0].expected_digest_uri = canonical_digest_uri(0x61);
        staged[1].expected_length = 89;
        staged[1].expected_digest_uri = canonical_digest_uri(0x62);
        let manifest = manifest_rows(&staged);
        let mut operation = publish_operation(
            identity.publication_operation_id,
            identity.artifact_revision_id,
            path(RUN_MANIFEST_PATH),
            PublishClaim::CreateOnly,
            &staged,
            &manifest,
        );
        operation.authority = PublishAuthority::RestoreStaging {
            restore_operation_id: restore.operation_id,
        };
        seal_publish_operation(&mut operation);
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        let outcome = service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation.clone(),
                artifact: PublishedArtifact {
                    logical_size: 160,
                    body_digest_uri: canonical_digest_uri(0x63),
                    manifest_digest_uri: sha256_digest_uri(operation.manifest_seal),
                    content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
                    producer: None,
                    manifest_id: None,
                    typed_index_projection: TypedProjection::empty().encode().unwrap(),
                },
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap();
        assert_eq!(outcome.result.logical_size, 160);
        assert_ne!(
            outcome.result.body_digest_uri,
            restore.restore_manifest.body_digest_uri,
        );
        let revision = ArtifactRevisionRecord::decode(
            &payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), identity.artifact_revision_id),
                store.current_read_version().unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(revision.block_count, 2);
    }

    #[test]
    fn restore_staging_rejects_unbound_or_wrong_manifest_identity_without_side_effects() {
        for case in 0..4 {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let mut restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &mut restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let expected_identity = provenance
                .destination_binding
                .as_ref()
                .unwrap()
                .restore_manifest_identity;
            if case == 0 {
                provenance.destination_binding = None;
            }
            restore.validate().unwrap();
            install_restore_publication_authority(&store, &mut counter, &restore);

            let wrong_identity = match case {
                0 => expected_identity,
                1 => restore_manifest_identity(operation_id(90_099), revision(90_002)),
                2 => restore_manifest_identity(operation_id(90_002), revision(90_099)),
                3 => restore_manifest_identity(operation_id(90_099), revision(90_099)),
                _ => unreachable!("case is bounded"),
            };
            let (operation, _, _) = restore_staging_publish_operation(
                wrong_identity,
                RESTORE_MANIFEST_PATH,
                restore.restore_manifest.body_digest_uri.clone(),
                restore.restore_manifest.logical_size,
            );
            let version_before = store.current_read_version().unwrap();
            assert_eq!(
                service.begin_publish(BeginPublishRequest {
                    context: publication_context(&store, &mut counter),
                    operation: operation.clone(),
                }),
                Err(PublicationError::RestoreAuthorityMismatch),
            );
            assert_eq!(store.current_read_version().unwrap(), version_before);
            assert!(payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Publish, operation.operation_id),
                version_before,
            )
            .is_none());
            assert!(payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_claim_key(root(), operation.artifact_revision_id),
                version_before,
            )
            .is_none());
        }
    }

    #[test]
    fn restore_staging_rejects_wrong_path_claim_and_dependencies_before_admission() {
        for case in 0..4 {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let identity = provenance
                .destination_binding
                .as_ref()
                .unwrap()
                .restore_manifest_identity;
            install_restore_publication_authority(&store, &mut counter, &restore);
            let (mut operation, _, _) = restore_staging_publish_operation(
                identity,
                RESTORE_MANIFEST_PATH,
                restore.restore_manifest.body_digest_uri.clone(),
                restore.restore_manifest.logical_size,
            );
            let expected_error = match case {
                0 => {
                    operation.path = path("metadata/not_a_canonical_manifest.json");
                    PublicationError::RestoreManifestPathRequired
                }
                1 => {
                    operation.claim = PublishClaim::ReplaceOnly {
                        expected_generation: Generation::new(1).unwrap(),
                    };
                    PublicationError::RestoreAuthorityMismatch
                }
                2 => {
                    operation.dependency_count = 1;
                    operation.dependency_depth = 1;
                    operation.dependency_digest =
                        dependency_owner_digest(&[revision(99_001)]).unwrap();
                    PublicationError::RestoreAuthorityMismatch
                }
                3 => {
                    operation.dependency_digest = [0x99; SHA256_BYTES];
                    PublicationError::RestoreAuthorityMismatch
                }
                _ => unreachable!("case is bounded"),
            };
            seal_publish_operation(&mut operation);
            let version_before = store.current_read_version().unwrap();
            assert_eq!(
                service.begin_publish(BeginPublishRequest {
                    context: publication_context(&store, &mut counter),
                    operation: operation.clone(),
                }),
                Err(expected_error),
            );
            assert_eq!(store.current_read_version().unwrap(), version_before);
            assert!(payload_at(
                &store,
                MetadataFamily::Operation,
                &operation_key(root(), OperationKind::Publish, operation.operation_id),
                version_before,
            )
            .is_none());
        }
    }

    #[test]
    fn restore_manifest_descriptor_mismatch_never_publishes_hidden_path() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let restore = sealed_bound_restore_operation();
        let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
            unreachable!("test fixture uses v5 provenance")
        };
        let identity = provenance
            .destination_binding
            .as_ref()
            .unwrap()
            .restore_manifest_identity;
        install_restore_publication_authority(&store, &mut counter, &restore);
        let (operation, staged, manifest) = restore_staging_publish_operation(
            identity,
            RESTORE_MANIFEST_PATH,
            canonical_digest_uri(0xee),
            restore.restore_manifest.logical_size,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = stage_all(
            &service,
            &store,
            &mut counter,
            operation,
            &staged,
            &manifest,
        );
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        assert_eq!(
            service.finalize_publish(FinalizePublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation.clone(),
                artifact: PublishedArtifact {
                    logical_size: staged[0].expected_length,
                    body_digest_uri: staged[0].expected_digest_uri.clone(),
                    manifest_digest_uri: sha256_digest_uri(operation.manifest_seal),
                    content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
                    producer: None,
                    manifest_id: None,
                    typed_index_projection: TypedProjection::empty().encode().unwrap(),
                },
                dependency_owner_revision_ids: Vec::new(),
            }),
            Err(PublicationError::RestoreManifestClosureMismatch),
        );
        let current = store.current_read_version().unwrap();
        assert!(payload_at(
            &store,
            MetadataFamily::PathCurrent,
            &path_current_key(root(), incarnation(9), &path(RESTORE_MANIFEST_PATH)),
            current,
        )
        .is_none());
        assert!(payload_at(
            &store,
            MetadataFamily::ArtifactRevision,
            &artifact_revision_key(root(), identity.artifact_revision_id),
            current,
        )
        .is_none());
        assert_eq!(
            RestoreOperationRecord::decode(
                &payload_at(
                    &store,
                    MetadataFamily::Operation,
                    &operation_key(root(), OperationKind::Restore, restore.operation_id),
                    current,
                )
                .unwrap(),
            )
            .unwrap(),
            restore,
        );
    }

    #[test]
    fn restore_terminal_publish_replay_requires_live_progressed_authority() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let restore = sealed_bound_restore_operation();
        let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
            unreachable!("test fixture uses v5 provenance")
        };
        let binding = provenance.destination_binding.as_ref().unwrap().clone();
        install_restore_publication_authority(&store, &mut counter, &restore);

        let (run_initial, run_outcome) = {
            let (operation, staged, manifest) = restore_staging_publish_operation(
                binding.run_manifest_identity,
                RUN_MANIFEST_PATH,
                canonical_digest_uri(0x41),
                127,
            );
            drive_restore_staging_publish(
                &service,
                &store,
                &mut counter,
                operation,
                &staged,
                &manifest,
                RESTORE_MANIFEST_CONTENT_TYPE,
            )
        };
        let (restore_initial, restore_outcome) = {
            let (operation, staged, manifest) = restore_staging_publish_operation(
                binding.restore_manifest_identity,
                RESTORE_MANIFEST_PATH,
                restore.restore_manifest.body_digest_uri.clone(),
                restore.restore_manifest.logical_size,
            );
            drive_restore_staging_publish(
                &service,
                &store,
                &mut counter,
                operation,
                &staged,
                &manifest,
                RESTORE_MANIFEST_CONTENT_TYPE,
            )
        };
        let manifests = RestoreDestinationManifests {
            run_manifest: RestoreManifestPublication {
                publication_operation_id: run_outcome.operation.operation_id,
                workspace_incarnation_id: incarnation(9),
                artifact_revision_id: run_outcome.operation.artifact_revision_id,
                body_digest_uri: run_outcome.result.body_digest_uri.clone(),
                manifest_digest_uri: sha256_digest_uri(run_outcome.operation.manifest_seal),
                logical_size: run_outcome.result.logical_size,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            restore_manifest: RestoreManifestPublication {
                publication_operation_id: restore_outcome.operation.operation_id,
                workspace_incarnation_id: incarnation(9),
                artifact_revision_id: restore_outcome.operation.artifact_revision_id,
                body_digest_uri: restore_outcome.result.body_digest_uri.clone(),
                manifest_digest_uri: sha256_digest_uri(restore_outcome.operation.manifest_seal),
                logical_size: restore_outcome.result.logical_size,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
        };
        let building = restore
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [0x51; SHA256_BYTES],
                    manifests,
                },
            )
            .unwrap();
        replace_restore_operation(&store, &mut counter, &restore, &building);

        let mut mismatched_binding = building.clone();
        let RestoreCommitProvenance::V5(provenance) = &mut mismatched_binding.commit_provenance
        else {
            unreachable!("test fixture uses v5 provenance")
        };
        let binding = provenance.destination_binding.as_mut().unwrap();
        let wrong_run_identity = restore_manifest_identity(operation_id(90_099), revision(90_099));
        binding.run_manifest_identity = wrong_run_identity;
        let actual_run = &mut binding.manifests.as_mut().unwrap().run_manifest;
        actual_run.publication_operation_id = wrong_run_identity.publication_operation_id;
        actual_run.artifact_revision_id = wrong_run_identity.artifact_revision_id;
        mismatched_binding.validate().unwrap();
        replace_restore_operation(&store, &mut counter, &building, &mismatched_binding);
        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: run_initial.clone(),
            }),
            Err(PublicationError::RestoreAuthorityMismatch),
            "progressed replay must reject a different durable manifest binding",
        );
        replace_restore_operation(&store, &mut counter, &mismatched_binding, &building);

        let replay = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: run_initial.clone(),
            })
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation, run_outcome.operation);
        let stale_success_context = publication_context(&store, &mut counter);

        delete_restore_manifest_path_for_test(&store, &mut counter, RUN_MANIFEST_PATH);
        assert_eq!(
            service.begin_publish(BeginPublishRequest {
                context: publication_context(&store, &mut counter),
                operation: run_initial.clone(),
            }),
            Err(PublicationError::RestoreAuthorityMismatch),
            "progressed replay must re-read the live destination path",
        );

        let aborting = building
            .apply(
                RestorePhase::DestinationBuilding,
                RestoreTransition::BeginAbort {
                    terminal_error: RestoreTerminalError {
                        kind: RestoreTerminalErrorKind::AbortedByCaller,
                        message: "cleanup won after response loss".to_owned(),
                        evidence_digest: None,
                    },
                },
            )
            .unwrap();
        replace_restore_operation(&store, &mut counter, &building, &aborting);

        assert!(matches!(
            service.begin_publish(BeginPublishRequest {
                context: stale_success_context,
                operation: restore_initial.clone(),
            }),
            Err(PublicationError::Meta(
                MetaError::WriteReadVersionMismatch { .. }
            ))
        ));

        for initial in [run_initial, restore_initial] {
            assert_eq!(
                service.begin_publish(BeginPublishRequest {
                    context: publication_context(&store, &mut counter),
                    operation: initial,
                }),
                Err(PublicationError::RestoreAuthorityMismatch),
                "an old terminal publication receipt must not survive restore cleanup authority",
            );
        }
    }

    #[test]
    fn successor_replays_only_the_exact_terminal_restore_staging_identity() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let restore = sealed_bound_restore_operation();
        let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
            unreachable!("test fixture uses v5 provenance")
        };
        let binding = provenance.destination_binding.as_ref().unwrap().clone();
        install_restore_publication_authority(&store, &mut counter, &restore);

        let (run_initial, run_outcome) = {
            let (operation, staged, manifest) = restore_staging_publish_operation(
                binding.run_manifest_identity,
                RUN_MANIFEST_PATH,
                canonical_digest_uri(0x41),
                127,
            );
            drive_restore_staging_publish(
                &service,
                &store,
                &mut counter,
                operation,
                &staged,
                &manifest,
                RESTORE_MANIFEST_CONTENT_TYPE,
            )
        };
        let restore_outcome = {
            let (operation, staged, manifest) = restore_staging_publish_operation(
                binding.restore_manifest_identity,
                RESTORE_MANIFEST_PATH,
                restore.restore_manifest.body_digest_uri.clone(),
                restore.restore_manifest.logical_size,
            );
            drive_restore_staging_publish(
                &service,
                &store,
                &mut counter,
                operation,
                &staged,
                &manifest,
                RESTORE_MANIFEST_CONTENT_TYPE,
            )
            .1
        };
        let manifests = RestoreDestinationManifests {
            run_manifest: RestoreManifestPublication {
                publication_operation_id: run_outcome.operation.operation_id,
                workspace_incarnation_id: incarnation(9),
                artifact_revision_id: run_outcome.operation.artifact_revision_id,
                body_digest_uri: run_outcome.result.body_digest_uri.clone(),
                manifest_digest_uri: sha256_digest_uri(run_outcome.operation.manifest_seal),
                logical_size: run_outcome.result.logical_size,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            restore_manifest: RestoreManifestPublication {
                publication_operation_id: restore_outcome.operation.operation_id,
                workspace_incarnation_id: incarnation(9),
                artifact_revision_id: restore_outcome.operation.artifact_revision_id,
                body_digest_uri: restore_outcome.result.body_digest_uri.clone(),
                manifest_digest_uri: sha256_digest_uri(restore_outcome.operation.manifest_seal),
                logical_size: restore_outcome.result.logical_size,
                content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
        };
        let building = restore
            .apply(
                RestorePhase::SourceSealed,
                RestoreTransition::BeginDestinationBuilding {
                    initialization_digest: [0x51; SHA256_BYTES],
                    manifests,
                },
            )
            .unwrap();
        replace_restore_operation(&store, &mut counter, &restore, &building);
        store
            .advance_owner_epoch(Some(owner()), successor_owner())
            .unwrap();

        let mut successor_candidate = run_initial.clone();
        successor_candidate.initiating_owner_epoch = successor_owner();
        seal_publish_operation(&mut successor_candidate);
        let replay = service
            .begin_publish(BeginPublishRequest {
                context: publication_context_for_owner(&store, &mut counter, successor_owner()),
                operation: successor_candidate.clone(),
            })
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation, run_outcome.operation);

        let mut wrong_staged_row_seal = successor_candidate.clone();
        wrong_staged_row_seal.staged_object_seal[0] ^= 0xff;
        seal_publish_operation(&mut wrong_staged_row_seal);
        let mut wrong_manifest_row_seal = successor_candidate.clone();
        wrong_manifest_row_seal.manifest_seal[0] ^= 0xff;
        seal_publish_operation(&mut wrong_manifest_row_seal);
        let mut wrong_revision = successor_candidate.clone();
        wrong_revision.artifact_revision_id = revision(91_200);
        seal_publish_operation(&mut wrong_revision);
        let mut wrong_restore = successor_candidate;
        wrong_restore.authority = PublishAuthority::RestoreStaging {
            restore_operation_id: operation_id(91_201),
        };
        seal_publish_operation(&mut wrong_restore);

        for mismatch in [
            wrong_staged_row_seal,
            wrong_manifest_row_seal,
            wrong_revision,
            wrong_restore,
        ] {
            assert_eq!(
                service.begin_publish(BeginPublishRequest {
                    context: publication_context_for_owner(
                        &store,
                        &mut counter,
                        successor_owner(),
                    ),
                    operation: mismatch,
                }),
                Err(PublicationError::OperationInputMismatch),
                "successor replay must compare every immutable publish input",
            );
        }
    }

    #[test]
    fn failed_restore_staging_publish_phases_never_replay_as_success() {
        for phase in [
            PublishPhase::Aborting,
            PublishPhase::Cleaning,
            PublishPhase::Cleaned,
            PublishPhase::Quarantined,
        ] {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let identity = provenance
                .destination_binding
                .as_ref()
                .unwrap()
                .run_manifest_identity;
            install_restore_publication_authority(&store, &mut counter, &restore);
            let (initial, published) = {
                let (operation, staged, manifest) = restore_staging_publish_operation(
                    identity,
                    RUN_MANIFEST_PATH,
                    canonical_digest_uri(0x41),
                    127,
                );
                drive_restore_staging_publish(
                    &service,
                    &store,
                    &mut counter,
                    operation,
                    &staged,
                    &manifest,
                    RESTORE_MANIFEST_CONTENT_TYPE,
                )
            };
            let mut failed = published.operation.clone();
            failed.phase = phase;
            failed.result = None;
            failed.terminal_error = Some(PublishTerminalError {
                kind: PublishTerminalErrorKind::InvariantViolation,
                message: "terminal failure must not replay as success".to_owned(),
                evidence_digest: (phase == PublishPhase::Quarantined)
                    .then_some([0x91; SHA256_BYTES]),
            });
            if phase == PublishPhase::Cleaned {
                failed.cleanup_staged_object_cursor = failed.staged_object_cursor;
                failed.cleanup_manifest_cursor = failed.manifest_cursor;
            }
            seal_publish_operation(&mut failed);
            failed.validate().unwrap();
            replace_publish_operation_for_test(&store, &mut counter, &published.operation, &failed);

            assert_eq!(
                service.begin_publish(BeginPublishRequest {
                    context: publication_context(&store, &mut counter),
                    operation: initial,
                }),
                Err(PublicationError::RestoreAuthorityMismatch),
                "{phase:?} must fail closed instead of borrowing a stale receipt",
            );
        }
    }

    #[test]
    fn restore_abort_allows_only_exact_partial_publisher_cleanup() {
        for partial_phase in 0..3 {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let identity = provenance
                .destination_binding
                .as_ref()
                .unwrap()
                .run_manifest_identity;
            install_restore_publication_authority(&store, &mut counter, &restore);
            let (operation, staged, manifest) = restore_staging_publish_operation(
                identity,
                RUN_MANIFEST_PATH,
                canonical_digest_uri(0x71),
                96,
            );
            let mut operation = begin_operation(&service, &store, &mut counter, operation);
            operation = service
                .stage_objects_batch(StageObjectsBatchRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    staged_objects: staged.clone(),
                })
                .unwrap()
                .operation;
            if partial_phase >= 1 {
                operation = service
                    .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        staged_object_updates: vec![StagedObjectUpdate {
                            expected: staged[0].clone(),
                            next: uploaded_rows(&staged)[0].clone(),
                        }],
                    })
                    .unwrap()
                    .operation;
            }
            if partial_phase == 2 {
                operation = service
                    .stage_manifest_batch(StageManifestBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        manifest_rows: manifest.clone(),
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .unwrap()
                    .operation;
            }

            let restore_cleanup = move_restore_into_abort_for_publication_test(
                &store,
                &mut counter,
                &restore,
                partial_phase != 0,
            );
            let aborting = service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    transition: PublishTransition::BeginAbort {
                        terminal_error: PublishTerminalError {
                            kind: PublishTerminalErrorKind::AbortedByCaller,
                            message: "restore abort won".to_owned(),
                            evidence_digest: None,
                        },
                    },
                })
                .unwrap()
                .operation;
            let cleaning = service
                .transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: aborting,
                    transition: PublishTransition::BeginCleaning,
                })
                .unwrap()
                .operation;
            let expected_staged = load_staged_object_for_test(&store, &cleaning, 0);
            let mut deleted_staged = expected_staged.clone();
            deleted_staged.provider_state = StagedProviderState::Aborted;
            deleted_staged.cleanup_state = StagedCleanupState::Deleted;
            let mut cleaning = service
                .cleanup_publish_batch(CleanupPublishBatchRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: cleaning,
                    staged_object_updates: vec![StagedObjectUpdate {
                        expected: expected_staged,
                        next: deleted_staged,
                    }],
                })
                .unwrap()
                .operation;
            if cleaning.manifest_cursor > 0 {
                cleaning = service
                    .cleanup_publish_batch(CleanupPublishBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: cleaning,
                        staged_object_updates: Vec::new(),
                    })
                    .unwrap()
                    .operation;
            }
            if partial_phase == 2 {
                // With the publication still Cleaning and its expected record
                // current, a restore that already reached Cleaned is past the
                // automatic publisher-cleanup authority: the transition must
                // fail closed without touching the store.
                let restore_cleaned = restore_cleanup
                    .apply(RestorePhase::Cleaning, RestoreTransition::FinishCleanup)
                    .unwrap();
                replace_restore_operation(&store, &mut counter, &restore_cleanup, &restore_cleaned);
                let version_before = store.current_read_version().unwrap();
                assert_eq!(
                    service.transition_publish(TransitionPublishRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: cleaning.clone(),
                        transition: PublishTransition::FinishCleanup,
                    }),
                    Err(PublicationError::RestoreAuthorityMismatch),
                    "restore Cleaned is past automatic publisher cleanup authority",
                );
                assert_eq!(store.current_read_version().unwrap(), version_before);
                replace_restore_operation(&store, &mut counter, &restore_cleaned, &restore_cleanup);
            }
            let finish_expected = cleaning.clone();
            let finish = TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: cleaning,
                transition: PublishTransition::FinishCleanup,
            };
            let cleaned = service.transition_publish(finish.clone()).unwrap();
            assert_eq!(cleaned.operation.phase, PublishPhase::Cleaned);
            let replay = service.transition_publish(finish).unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.operation, cleaned.operation);

            if partial_phase == 2 {
                let restore_cleaned = restore_cleanup
                    .apply(RestorePhase::Cleaning, RestoreTransition::FinishCleanup)
                    .unwrap();
                replace_restore_operation(&store, &mut counter, &restore_cleanup, &restore_cleaned);
                // A stale expected record is classified as concurrent progress
                // before any contextual authority check runs; the caller must
                // re-observe the durable row rather than reason about
                // authority from an outdated receipt.
                let version_before = store.current_read_version().unwrap();
                assert_eq!(
                    service.transition_publish(TransitionPublishRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: finish_expected,
                        transition: PublishTransition::FinishCleanup,
                    }),
                    Err(PublicationError::ConcurrentMutation),
                    "a stale expected record must be re-observed before authority is judged",
                );
                assert_eq!(store.current_read_version().unwrap(), version_before);
            }

            let current = store.current_read_version().unwrap();
            assert!(payload_at(
                &store,
                MetadataFamily::PathCurrent,
                &path_current_key(root(), incarnation(9), &path(RUN_MANIFEST_PATH)),
                current,
            )
            .is_none());
            assert!(payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), identity.artifact_revision_id),
                current,
            )
            .is_none());
            assert!(read_revision_claim(&store, identity.artifact_revision_id).is_none());
            assert_eq!(
                count_prefix(
                    &store,
                    MetadataFamily::StagedObject,
                    &staged_object_prefix(root(), identity.publication_operation_id),
                ),
                0,
            );
            assert_eq!(
                count_prefix(
                    &store,
                    MetadataFamily::ArtifactManifest,
                    &artifact_manifest_prefix(root(), identity.artifact_revision_id),
                ),
                0,
            );
        }
    }

    #[test]
    fn restore_abort_rejects_every_forward_publication_step_without_mutation() {
        for forward_step in 0..5 {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let identity = provenance
                .destination_binding
                .as_ref()
                .unwrap()
                .run_manifest_identity;
            install_restore_publication_authority(&store, &mut counter, &restore);
            let (initial, staged, manifest) = restore_staging_publish_operation(
                identity,
                RUN_MANIFEST_PATH,
                canonical_digest_uri(0x72),
                97,
            );
            let mut operation = initial.clone();
            if forward_step > 0 {
                operation = begin_operation(&service, &store, &mut counter, operation);
            }
            if forward_step >= 2 {
                operation = service
                    .stage_objects_batch(StageObjectsBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        staged_objects: staged.clone(),
                    })
                    .unwrap()
                    .operation;
            }
            if forward_step >= 3 {
                operation = service
                    .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        staged_object_updates: vec![StagedObjectUpdate {
                            expected: staged[0].clone(),
                            next: uploaded_rows(&staged)[0].clone(),
                        }],
                    })
                    .unwrap()
                    .operation;
            }
            if forward_step >= 4 {
                operation = service
                    .stage_manifest_batch(StageManifestBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        manifest_rows: manifest.clone(),
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .unwrap()
                    .operation;
                operation = service
                    .transition_publish(TransitionPublishRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        transition: PublishTransition::BeginFinalization,
                    })
                    .unwrap()
                    .operation;
            }
            move_restore_into_abort_for_publication_test(
                &store,
                &mut counter,
                &restore,
                forward_step % 2 == 0,
            );
            let version_before = store.current_read_version().unwrap();
            let rejected = match forward_step {
                0 => service
                    .begin_publish(BeginPublishRequest {
                        context: publication_context(&store, &mut counter),
                        operation,
                    })
                    .map(|_| ()),
                1 => service
                    .stage_objects_batch(StageObjectsBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        staged_objects: staged.clone(),
                    })
                    .map(|_| ()),
                2 => service
                    .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        staged_object_updates: vec![StagedObjectUpdate {
                            expected: staged[0].clone(),
                            next: uploaded_rows(&staged)[0].clone(),
                        }],
                    })
                    .map(|_| ()),
                3 => service
                    .stage_manifest_batch(StageManifestBatchRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation,
                        manifest_rows: manifest.clone(),
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .map(|_| ()),
                4 => service
                    .finalize_publish(FinalizePublishRequest {
                        context: publication_context(&store, &mut counter),
                        expected_operation: operation.clone(),
                        artifact: PublishedArtifact {
                            logical_size: 97,
                            body_digest_uri: canonical_digest_uri(0x72),
                            manifest_digest_uri: sha256_digest_uri(operation.manifest_seal),
                            content_type: RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
                            producer: None,
                            manifest_id: None,
                            typed_index_projection: TypedProjection::empty().encode().unwrap(),
                        },
                        dependency_owner_revision_ids: Vec::new(),
                    })
                    .map(|_| ()),
                _ => unreachable!("forward step is bounded"),
            };
            assert!(matches!(
                rejected,
                Err(PublicationError::RestoreAuthorityMismatch)
                    | Err(PublicationError::RestoreManifestClosureMismatch)
            ));
            assert_eq!(store.current_read_version().unwrap(), version_before);
            assert!(payload_at(
                &store,
                MetadataFamily::PathCurrent,
                &path_current_key(root(), incarnation(9), &path(RUN_MANIFEST_PATH)),
                version_before,
            )
            .is_none());
            assert!(payload_at(
                &store,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(root(), identity.artifact_revision_id),
                version_before,
            )
            .is_none());
        }
    }

    #[test]
    fn restore_abort_cleanup_rejects_publishers_outside_late_bound_identity() {
        for wrong_operation in [false, true] {
            let mut counter = 1;
            let store = ready_store(&mut counter);
            let service = PublicationService::new(&store);
            let restore = sealed_bound_restore_operation();
            let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
                unreachable!("test fixture uses v5 provenance")
            };
            let expected = provenance
                .destination_binding
                .as_ref()
                .unwrap()
                .run_manifest_identity;
            install_restore_publication_authority(&store, &mut counter, &restore);
            move_restore_into_abort_for_publication_test(&store, &mut counter, &restore, false);
            let wrong = if wrong_operation {
                restore_manifest_identity(operation_id(90_099), expected.artifact_revision_id)
            } else {
                restore_manifest_identity(expected.publication_operation_id, revision(90_099))
            };
            let (mut operation, _, _) = restore_staging_publish_operation(
                wrong,
                RUN_MANIFEST_PATH,
                canonical_digest_uri(0x73),
                98,
            );
            operation.phase = PublishPhase::Aborting;
            operation.terminal_error = Some(PublishTerminalError {
                kind: PublishTerminalErrorKind::AbortedByCaller,
                message: "planted foreign publisher".to_owned(),
                evidence_digest: None,
            });
            seal_publish_operation(&mut operation);
            put_operation_row_raw(&store, &mut counter, &operation);
            let version_before = store.current_read_version().unwrap();
            assert_eq!(
                service.transition_publish(TransitionPublishRequest {
                    context: publication_context(&store, &mut counter),
                    expected_operation: operation,
                    transition: PublishTransition::BeginCleaning,
                }),
                Err(PublicationError::RestoreAuthorityMismatch),
            );
            assert_eq!(store.current_read_version().unwrap(), version_before);
        }
    }

    #[test]
    fn quarantined_restore_publisher_retains_claim_and_provider_evidence() {
        let mut counter = 1;
        let store = ready_store(&mut counter);
        let service = PublicationService::new(&store);
        let restore = sealed_bound_restore_operation();
        let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
            unreachable!("test fixture uses v5 provenance")
        };
        let identity = provenance
            .destination_binding
            .as_ref()
            .unwrap()
            .restore_manifest_identity;
        install_restore_publication_authority(&store, &mut counter, &restore);
        let (operation, staged, _) = restore_staging_publish_operation(
            identity,
            RESTORE_MANIFEST_PATH,
            restore.restore_manifest.body_digest_uri.clone(),
            restore.restore_manifest.logical_size,
        );
        let operation = begin_operation(&service, &store, &mut counter, operation);
        let operation = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                staged_objects: staged,
            })
            .unwrap()
            .operation;
        move_restore_into_abort_for_publication_test(&store, &mut counter, &restore, true);
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginAbort {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::AbortedByCaller,
                        message: "provider outcome became ambiguous".to_owned(),
                        evidence_digest: None,
                    },
                },
            })
            .unwrap()
            .operation;
        let cleaning = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: operation,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        let quarantined = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: cleaning,
                transition: PublishTransition::Quarantine {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::CleanupFailed,
                        message: "operator verification required".to_owned(),
                        evidence_digest: Some([0x74; SHA256_BYTES]),
                    },
                },
            })
            .unwrap()
            .operation;
        assert_eq!(quarantined.phase, PublishPhase::Quarantined);
        assert!(read_revision_claim(&store, identity.artifact_revision_id).is_some());
        assert_eq!(
            count_prefix(
                &store,
                MetadataFamily::StagedObject,
                &staged_object_prefix(root(), identity.publication_operation_id),
            ),
            1,
        );
        assert!(matches!(
            service.cleanup_publish_batch(CleanupPublishBatchRequest {
                context: publication_context(&store, &mut counter),
                expected_operation: quarantined,
                staged_object_updates: Vec::new(),
            }),
            Err(PublicationError::InvalidOperationPhase {
                expected: PublishPhase::Cleaning,
                actual: PublishPhase::Quarantined,
            })
        ));
        assert!(read_revision_claim(&store, identity.artifact_revision_id).is_some());
    }
}
