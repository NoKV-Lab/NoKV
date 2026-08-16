/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable same-root restore lifecycle.
//!
//! A restore creates a new hidden workspace incarnation, retains one exact
//! snapshot or commit, copies its canonical path closure in bounded commands,
//! applies the Workbench initialization while the destination is still hidden,
//! and publishes the workspace marker last.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommandDigest, CommitId, CommitState, CommitVersion, ConsumerEpoch,
    GcClaimState, Generation, HistoryHoldState, NormalizedRelativePath, OperationId, OperationKind,
    PublishPhase, ReadVersion, ReferenceEpoch, RestorePhase, RevisionState, SnapshotId,
    SnapshotState, WorkbenchId, WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState,
    FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::codec::{
    artifact_revision_key, child_commit_consumer_key, commit_key, commit_member_key,
    commit_member_prefix, commit_revision_ref_key, commit_revision_ref_prefix,
    decode_commit_member_key, decode_path_current_key, gc_candidate_key, lease_commit_consumer_key,
    operation_key, path_child_prefix, path_current_key, path_revision_ref_key,
    restore_history_hold_key, restore_member_key, restore_member_prefix,
    snapshot_commit_consumer_key, snapshot_ref_key, workbench_commit_head_key,
    workbench_head_commit_consumer_key, workspace_current_key, workspace_incarnation_claim_key,
    SCHEMA_ID,
};
use super::commit::RUN_MANIFEST_PATH;
use super::commit_closure::{
    advance_commit_parent_rolling_digest, plan_commit_member, plan_commit_revision,
};
use super::commit_records::{
    add_commit_consumer, advance_commit_member_rolling_digest, commit_member_row_digest,
    remove_commit_consumer, CommitConsumerMutationError, CommitConsumerRecord, CommitMemberRecord,
    CommitRecord, CommitRecordError, WorkbenchCommitHeadRecord,
};
use super::engine::{
    CommandFit, CommandMutation, CommandPredicate, EventProjection, HistoryProjection, MetaError,
    MetaShard, MetadataCommand, MetadataScanItem, RootFenceAction,
};
use super::event_projection::change_event_projection;
use super::keyspace::MetadataFamily;
use super::namespace::RootWriteContext;
use super::publication_records::{
    ArtifactRevisionRecord, GcCandidateRecord, PathEntry, PublicationRecordCodecError,
    RevisionRefRecord, WorkspaceIncarnationClaimRecord, WorkspaceRecord,
};
use super::publish_operation_records::{
    PublishAuthority, PublishClaim, PublishOperationRecord, PublishRecordError,
};
use super::query_records::{
    secondary_index_key, ChangeEventKind, ChangeEventRecord, QueryFieldId, QueryRecordError,
    QueryScalar, SecondaryIndexRecord, TypedProjection,
};
use super::restore_records::{
    RestoreCommitClosureProgress, RestoreCommitProvenance, RestoreCommitProvenanceV5,
    RestoreDestinationBinding, RestoreManifestDescriptor, RestoreManifestIdentity,
    RestoreManifestPublication, RestoreMemberRecord, RestoreOperationRecord, RestoreRecordError,
    RestoreResult, RestoreSource, RestoreSourceCommitSeal, RestoreTerminalError,
    RestoreTerminalErrorKind, RestoreTransition, MAX_RESTORE_TERMINAL_ERROR_BYTES,
};
use super::snapshot_records::{HistoryHoldRecord, SnapshotRecordError, SnapshotRefRecord};

/// Maximum source rows copied by one metadata command.
///
/// Each row may require one path, reference, member, and revision predicate /
/// mutation pair. Forty-eight rows remain below the metadata layer's independent
/// 256-item bounds even when every row names a distinct revision.
pub const MAX_RESTORE_BATCH_MEMBERS: usize = 48;
/// Exact Workbench initialization path published before publication.
pub const RESTORE_MANIFEST_PATH: &str = "metadata/restore_manifest.json";
const RESTORE_OUTCOME_FORMAT: u8 = 1;
const MAX_COMMAND_ITEMS: usize = 256;
const CAPACITY_EXCEEDED_MESSAGE: &str =
    "restore source member exceeds the serving metadata transaction budget";
const CLEANUP_CAPACITY_EXCEEDED_MESSAGE: &str =
    "restore cleanup member exceeds the serving metadata transaction budget";
const QUARANTINED_PUBLICATION_MESSAGE: &str =
    "restore cleanup is blocked by a quarantined destination manifest publication";

/// Caller selection before a snapshot read version is frozen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreSourceSelector {
    Snapshot(SnapshotId),
    Commit(CommitId),
}

/// Canonical hidden initialization applied after the source closure is sealed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreInitialization {
    /// Exact already-published staging path entry. The object body remains in
    /// the object plane and is never copied into metadata.
    pub restore_manifest: PathEntry,
}

/// Atomic creation request for a destination workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginRestoreRequest {
    pub operation_id: OperationId,
    pub source_workbench_id: WorkbenchId,
    pub expected_source_workspace_incarnation_id: WorkspaceIncarnationId,
    pub source: RestoreSourceSelector,
    pub destination_workbench_id: WorkbenchId,
    pub destination_workspace_incarnation_id: WorkspaceIncarnationId,
    pub destination_restore_manifest_identity: RestoreManifestIdentity,
    /// First-writer commit-time proposal. A retry may propose a later clock
    /// value; the durable operation always wins.
    pub destination_committed_at_unix_seconds: u64,
    pub restore_manifest: RestoreManifestDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreOperationRequest {
    pub operation_id: OperationId,
}

/// Exact destination commit and projection authority bound after source seal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindRestoreDestinationRequest {
    pub operation_id: OperationId,
    pub binding: RestoreDestinationBinding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopyRestoreBatchRequest {
    pub operation_id: OperationId,
    pub limit: usize,
}

/// One bounded destination commit-closure step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreClosureBatchRequest {
    pub operation_id: OperationId,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortRestoreRequest {
    pub operation_id: OperationId,
    pub terminal_error: RestoreTerminalError,
}

/// Stable result of one restore lifecycle command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreCommandOutcome {
    pub operation: RestoreOperationRecord,
    pub commit_version: CommitVersion,
    pub replayed: bool,
    pub affected_members: u32,
}

/// Copy progress returned after one bounded source batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyRestoreBatchOutcome {
    pub command: RestoreCommandOutcome,
    pub copied_members: usize,
    pub source_eof: bool,
}

/// Progress returned by one bounded destination commit-member build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildRestoreCommitBatchOutcome {
    pub command: RestoreCommandOutcome,
    pub built_members: usize,
    pub members_complete: bool,
}

/// Progress returned by one bounded destination revision-ref seal scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealRestoreCommitBatchOutcome {
    pub command: RestoreCommandOutcome,
    pub sealed_revisions: usize,
    pub ready: bool,
}

/// Final publication result retained in the terminal operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteRestoreOutcome {
    pub command: RestoreCommandOutcome,
    pub result: RestoreResult,
}

/// Exact commit-owned source run manifest retained for destination projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreSourceRunManifest {
    pub operation: RestoreOperationRecord,
    pub source_commit_id: CommitId,
    pub source_snapshot_read_version: Option<ReadVersion>,
    pub path_entry: PathEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreError {
    SourceWorkspaceMissing {
        workbench_id: WorkbenchId,
    },
    SourceWorkspaceMismatch,
    DestinationExists {
        workbench_id: WorkbenchId,
    },
    DestinationIncarnationClaimed {
        incarnation_id: WorkspaceIncarnationId,
        workbench_id: WorkbenchId,
    },
    DestinationMarkerMismatch,
    SnapshotMissing {
        snapshot_id: SnapshotId,
    },
    SnapshotNotActive {
        snapshot_id: SnapshotId,
        state: SnapshotState,
    },
    SnapshotLeaseExpired {
        snapshot_id: SnapshotId,
        lease_clock_ms: u64,
        lease_deadline_ms: u64,
    },
    SnapshotRetentionMismatch,
    SnapshotCommitProvenanceMissing {
        snapshot_id: SnapshotId,
    },
    CommitMissing {
        commit_id: CommitId,
    },
    CommitNotSealed {
        commit_id: CommitId,
        state: CommitState,
    },
    CommitRetentionMismatch,
    OperationMissing {
        operation_id: OperationId,
    },
    OperationIdentityCollision {
        operation_id: OperationId,
    },
    OperationIdentityMismatch {
        expected: OperationId,
        actual: OperationId,
    },
    RestoreManifestBindingMismatch {
        operation_id: OperationId,
    },
    DestinationBindingMismatch {
        operation_id: OperationId,
    },
    InvalidPhase {
        expected: &'static str,
        actual: RestorePhase,
    },
    InvalidBatchLimit {
        requested: usize,
        max: usize,
    },
    SourceAlreadyExhausted,
    SourceClosureMismatch {
        reason: String,
    },
    ReservedPathInSource,
    CorruptKey {
        family: &'static str,
    },
    CorruptSourceMember {
        family: &'static str,
        path: String,
        detail: String,
    },
    RevisionMissing {
        revision: ArtifactRevisionId,
    },
    RevisionUnavailable {
        revision: ArtifactRevisionId,
        state: RevisionState,
    },
    RevisionReferenceMissing {
        revision: ArtifactRevisionId,
    },
    ReferenceEpochAhead {
        revision: ArtifactRevisionId,
    },
    ReferenceEpochOverflow {
        revision: ArtifactRevisionId,
    },
    ReferenceCountOverflow {
        revision: ArtifactRevisionId,
    },
    ReferenceCountUnderflow {
        revision: ArtifactRevisionId,
    },
    ConsumerEpochOverflow,
    ConsumerCountOverflow,
    ConsumerCountUnderflow,
    WorkspaceRevisionOverflow,
    CommitVersionOverflow,
    ManifestBindingMismatch,
    RestoreManifestMissing,
    ManifestRevisionMismatch,
    PublicationCleanupPending {
        operation_id: OperationId,
        phase: PublishPhase,
    },
    DuplicateCommandKey {
        family: MetadataFamily,
    },
    ConcurrentMutation,
    RequestInputMismatch,
    DeterministicResultMismatch {
        reason: String,
    },
    RecordCodec(PublicationRecordCodecError),
    CommitCodec(CommitRecordError),
    SnapshotCodec(SnapshotRecordError),
    RestoreCodec(RestoreRecordError),
    QueryRecord(QueryRecordError),
    PublishCodec(PublishRecordError),
    Meta(MetaError),
}

impl fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceWorkspaceMissing { workbench_id } => {
                write!(formatter, "source workbench {workbench_id} is not visible")
            }
            Self::SourceWorkspaceMismatch => {
                formatter.write_str("restore source does not belong to the selected workspace")
            }
            Self::DestinationExists { workbench_id } => {
                write!(formatter, "destination workbench {workbench_id} already exists")
            }
            Self::DestinationIncarnationClaimed {
                incarnation_id,
                workbench_id,
            } => write!(
                formatter,
                "destination workspace incarnation {:02x?} is permanently claimed by workbench {workbench_id}",
                incarnation_id.as_bytes()
            ),
            Self::DestinationMarkerMismatch => {
                formatter.write_str("destination workspace marker does not match the restore")
            }
            Self::SnapshotMissing { snapshot_id } => {
                write!(formatter, "snapshot {snapshot_id} does not exist")
            }
            Self::SnapshotNotActive { snapshot_id, state } => {
                write!(formatter, "snapshot {snapshot_id} is {state:?}, not Active")
            }
            Self::SnapshotLeaseExpired {
                snapshot_id,
                lease_clock_ms,
                lease_deadline_ms,
            } => write!(
                formatter,
                "snapshot {snapshot_id} deadline {lease_deadline_ms} is not newer than lease clock {lease_clock_ms}"
            ),
            Self::SnapshotRetentionMismatch => {
                formatter.write_str("snapshot restore retention records are inconsistent")
            }
            Self::SnapshotCommitProvenanceMissing { snapshot_id } => write!(
                formatter,
                "snapshot {snapshot_id} does not retain a sealed source commit"
            ),
            Self::CommitMissing { commit_id } => {
                write!(formatter, "commit {:02x?} does not exist", commit_id.as_bytes())
            }
            Self::CommitNotSealed { commit_id, state } => write!(
                formatter,
                "commit {:02x?} is {state:?}, not Sealed",
                commit_id.as_bytes()
            ),
            Self::CommitRetentionMismatch => {
                formatter.write_str("commit restore consumer is inconsistent")
            }
            Self::OperationMissing { operation_id } => write!(
                formatter,
                "restore operation {:02x?} does not exist",
                operation_id.as_bytes()
            ),
            Self::OperationIdentityCollision { operation_id } => write!(
                formatter,
                "restore operation identity collision at {:02x?}",
                operation_id.as_bytes()
            ),
            Self::OperationIdentityMismatch { expected, actual } => write!(
                formatter,
                "restore operation identity {:02x?} does not match derived v2 identity {:02x?}",
                actual.as_bytes(),
                expected.as_bytes()
            ),
            Self::RestoreManifestBindingMismatch { operation_id } => write!(
                formatter,
                "restore operation {:02x?} is bound to a different manifest descriptor",
                operation_id.as_bytes()
            ),
            Self::DestinationBindingMismatch { operation_id } => write!(
                formatter,
                "restore operation {:02x?} is bound to different destination authority",
                operation_id.as_bytes()
            ),
            Self::InvalidPhase { expected, actual } => {
                write!(formatter, "restore is {actual:?}, expected {expected}")
            }
            Self::InvalidBatchLimit { requested, max } => {
                write!(formatter, "restore batch limit {requested} is outside 1..={max}")
            }
            Self::SourceAlreadyExhausted => {
                formatter.write_str("restore source closure already reached EOF")
            }
            Self::SourceClosureMismatch { reason } => {
                write!(formatter, "restore source closure mismatch: {reason}")
            }
            Self::ReservedPathInSource => write!(
                formatter,
                "restore source contains reserved path {RESTORE_MANIFEST_PATH}"
            ),
            Self::CorruptKey { family } => write!(formatter, "corrupt {family} key"),
            Self::CorruptSourceMember {
                family,
                path,
                detail,
            } => write!(
                formatter,
                "restore source member {path} has a corrupt {family} record: {detail}"
            ),
            Self::RevisionMissing { revision } => write!(
                formatter,
                "artifact revision {:02x?} is missing",
                revision.as_bytes()
            ),
            Self::RevisionUnavailable { revision, state } => write!(
                formatter,
                "artifact revision {:02x?} is {state:?}, not Available",
                revision.as_bytes()
            ),
            Self::RevisionReferenceMissing { revision } => write!(
                formatter,
                "artifact revision {:02x?} is missing its path reference",
                revision.as_bytes()
            ),
            Self::ReferenceEpochAhead { revision } => write!(
                formatter,
                "artifact revision {:02x?} has a reference from a future epoch",
                revision.as_bytes()
            ),
            Self::ReferenceEpochOverflow { revision } => write!(
                formatter,
                "artifact revision {:02x?} reference epoch overflow",
                revision.as_bytes()
            ),
            Self::ReferenceCountOverflow { revision } => write!(
                formatter,
                "artifact revision {:02x?} reference count overflow",
                revision.as_bytes()
            ),
            Self::ReferenceCountUnderflow { revision } => write!(
                formatter,
                "artifact revision {:02x?} reference count underflow",
                revision.as_bytes()
            ),
            Self::ConsumerEpochOverflow => formatter.write_str("consumer epoch overflow"),
            Self::ConsumerCountOverflow => formatter.write_str("consumer count overflow"),
            Self::ConsumerCountUnderflow => formatter.write_str("consumer count underflow"),
            Self::WorkspaceRevisionOverflow => formatter.write_str("workspace revision overflow"),
            Self::CommitVersionOverflow => formatter.write_str("metadata commit version overflow"),
            Self::ManifestBindingMismatch => formatter.write_str(
                "published restore manifest does not match the descriptor bound by its operation",
            ),
            Self::RestoreManifestMissing => {
                formatter.write_str("restore manifest was not published to the staging workspace")
            }
            Self::ManifestRevisionMismatch => {
                formatter.write_str("restore manifest revision descriptor does not match its entry")
            }
            Self::PublicationCleanupPending {
                operation_id,
                phase,
            } => write!(
                formatter,
                "destination manifest publication {:02x?} is {phase:?}; restore cleanup must wait for its terminal state",
                operation_id.as_bytes()
            ),
            Self::DuplicateCommandKey { family } => {
                write!(formatter, "duplicate command key in {family:?}")
            }
            Self::ConcurrentMutation => formatter.write_str("restore state changed concurrently"),
            Self::RequestInputMismatch => {
                formatter.write_str("request id was reused with different restore inputs")
            }
            Self::DeterministicResultMismatch { reason } => {
                write!(formatter, "invalid restore command result: {reason}")
            }
            Self::RecordCodec(source) => source.fmt(formatter),
            Self::CommitCodec(source) => source.fmt(formatter),
            Self::SnapshotCodec(source) => source.fmt(formatter),
            Self::RestoreCodec(source) => source.fmt(formatter),
            Self::QueryRecord(source) => source.fmt(formatter),
            Self::PublishCodec(source) => source.fmt(formatter),
            Self::Meta(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for RestoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RecordCodec(source) => Some(source),
            Self::CommitCodec(source) => Some(source),
            Self::SnapshotCodec(source) => Some(source),
            Self::RestoreCodec(source) => Some(source),
            Self::QueryRecord(source) => Some(source),
            Self::PublishCodec(source) => Some(source),
            Self::Meta(source) => Some(source),
            _ => None,
        }
    }
}

impl From<PublicationRecordCodecError> for RestoreError {
    fn from(source: PublicationRecordCodecError) -> Self {
        Self::RecordCodec(source)
    }
}

impl From<CommitRecordError> for RestoreError {
    fn from(source: CommitRecordError) -> Self {
        Self::CommitCodec(source)
    }
}

impl From<SnapshotRecordError> for RestoreError {
    fn from(source: SnapshotRecordError) -> Self {
        Self::SnapshotCodec(source)
    }
}

impl From<RestoreRecordError> for RestoreError {
    fn from(source: RestoreRecordError) -> Self {
        Self::RestoreCodec(source)
    }
}

impl From<MetaError> for RestoreError {
    fn from(source: MetaError) -> Self {
        Self::Meta(source)
    }
}

impl From<QueryRecordError> for RestoreError {
    fn from(source: QueryRecordError) -> Self {
        Self::QueryRecord(source)
    }
}

impl From<PublishRecordError> for RestoreError {
    fn from(source: PublishRecordError) -> Self {
        Self::PublishCodec(source)
    }
}

#[derive(Clone, Debug)]
struct Loaded<T> {
    payload: Vec<u8>,
    record: T,
}

#[derive(Clone, Debug)]
struct SourceMember {
    path: nokv_types::NormalizedRelativePath,
    entry: PathEntry,
    row_digest: [u8; SHA256_BYTES],
}

#[derive(Clone, Debug)]
struct ScannedSourceMember {
    path: NormalizedRelativePath,
    row_digest: [u8; SHA256_BYTES],
    materialized: Option<SourceMember>,
}

#[derive(Clone, Debug)]
struct SourcePage {
    members: Vec<ScannedSourceMember>,
    eof: bool,
}

struct VerifiedRestoreManifestPublication {
    publication: RestoreManifestPublication,
    path_key: Vec<u8>,
    path: Loaded<PathEntry>,
    publish_key: Vec<u8>,
    publish: Loaded<PublishOperationRecord>,
    revision_key: Vec<u8>,
    revision: Loaded<ArtifactRevisionRecord>,
}

#[derive(Clone, Debug)]
struct RestoreCleanupPublicationObservation {
    key: Vec<u8>,
    payload: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreCleanupPublicationDisposition {
    Ready,
    Pending {
        operation_id: OperationId,
        phase: PublishPhase,
    },
    Quarantined,
}

#[derive(Clone, Debug)]
struct RestoreCleanupPublications {
    observations: Vec<RestoreCleanupPublicationObservation>,
    disposition: RestoreCleanupPublicationDisposition,
}

impl RestoreCleanupPublications {
    fn add_predicates(&self, plan: &mut CommandPlan) -> Result<(), RestoreError> {
        for observed in &self.observations {
            plan.assert_value(
                MetadataFamily::Operation,
                observed.key.clone(),
                observed.payload.clone(),
            )?;
        }
        Ok(())
    }

    fn evidence_digest(&self) -> [u8; SHA256_BYTES] {
        let mut digest = Sha256::new();
        digest.update(b"nokv.restore.cleanup-publication-evidence.v1\0");
        for observed in &self.observations {
            digest.update(
                u64::try_from(observed.key.len())
                    .expect("metadata keys fit u64")
                    .to_be_bytes(),
            );
            digest.update(&observed.key);
            match &observed.payload {
                Some(payload) => {
                    digest.update([1]);
                    digest.update(
                        u64::try_from(payload.len())
                            .expect("metadata values fit u64")
                            .to_be_bytes(),
                    );
                    digest.update(payload);
                }
                None => digest.update([0]),
            }
        }
        digest.finalize().into()
    }
}

impl VerifiedRestoreManifestPublication {
    fn add_predicates(&self, plan: &mut CommandPlan) -> Result<(), RestoreError> {
        for (family, key, payload) in [
            (
                MetadataFamily::PathCurrent,
                self.path_key.clone(),
                self.path.payload.clone(),
            ),
            (
                MetadataFamily::Operation,
                self.publish_key.clone(),
                self.publish.payload.clone(),
            ),
            (
                MetadataFamily::ArtifactRevision,
                self.revision_key.clone(),
                self.revision.payload.clone(),
            ),
        ] {
            if !plan.exact_keys.contains(&(family, key.clone())) {
                plan.assert_value(family, key, Some(payload))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CommandPlan {
    predicates: Vec<CommandPredicate>,
    mutations: Vec<CommandMutation>,
    history: Vec<HistoryProjection>,
    events: Vec<EventProjection>,
    exact_keys: BTreeSet<(MetadataFamily, Vec<u8>)>,
}

struct PlannedRestoreCommand {
    command: MetadataCommand,
    operation: RestoreOperationRecord,
}

struct PlannedCopiedMember {
    sequence: u64,
    path: nokv_types::NormalizedRelativePath,
    entry_payload: Vec<u8>,
    revision: ArtifactRevisionId,
    revision_ref_payload: Vec<u8>,
    restore_member_key: Vec<u8>,
    restore_member_payload: Vec<u8>,
    secondary_index_rows: BTreeMap<Vec<u8>, Vec<u8>>,
    revision_after_copy: ArtifactRevisionRecord,
}

struct PlannedCopyBatch {
    command: PlannedRestoreCommand,
    members: Vec<PlannedCopiedMember>,
    staging_payload: Vec<u8>,
}

impl CommandPlan {
    fn assert_value(
        &mut self,
        family: MetadataFamily,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
    ) -> Result<(), RestoreError> {
        if !self.exact_keys.insert((family, key.clone())) {
            return Err(RestoreError::DuplicateCommandKey { family });
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
    ) -> Result<(), RestoreError> {
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
    ) -> Result<(), RestoreError> {
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
    ) -> Result<(), RestoreError> {
        self.assert_value(family, key.clone(), Some(expected))?;
        self.mutations.push(CommandMutation::Delete {
            family,
            key: key.clone(),
        });
        self.history.push(HistoryProjection { family, key });
        Ok(())
    }
}

/// Atomically claim a hidden destination and retain the exact source.
pub fn begin_restore(
    store: &MetaShard,
    context: RootWriteContext,
    request: &BeginRestoreRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    request.restore_manifest.validate()?;
    let request_digest = begin_request_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, request_digest, None)? {
        return Ok(outcome);
    }

    let identity_digest = restore_selector_identity_digest(
        context.root_id,
        &request.source_workbench_id,
        request.expected_source_workspace_incarnation_id,
        request.source,
        &request.destination_workbench_id,
        request.destination_workspace_incarnation_id,
    )?;
    let operation_id = operation_id_from_identity(identity_digest);
    if request.operation_id != operation_id {
        return Err(RestoreError::OperationIdentityMismatch {
            expected: operation_id,
            actual: request.operation_id,
        });
    }
    let operation_key = operation_key(context.root_id, OperationKind::Restore, operation_id);
    if let Some(existing) = read_record(
        store,
        context,
        MetadataFamily::Operation,
        &operation_key,
        RestoreOperationRecord::decode,
    )? {
        if existing.record.identity_digest != identity_digest {
            return Err(RestoreError::OperationIdentityCollision { operation_id });
        }
        let begin_provenance_matches = matches!(
            &existing.record.commit_provenance,
            RestoreCommitProvenance::V5(_)
        );
        if existing.record.source_workbench_id != request.source_workbench_id
            || existing.record.source_workspace_incarnation_id
                != request.expected_source_workspace_incarnation_id
            || !restore_source_matches_selector(existing.record.source, request.source)
            || existing.record.destination_workbench_id != request.destination_workbench_id
            || existing.record.restore_manifest != request.restore_manifest
            || existing.record.destination_workspace_incarnation_id
                != request.destination_workspace_incarnation_id
            || existing.record.destination_restore_manifest_identity
                != Some(request.destination_restore_manifest_identity)
            || !begin_provenance_matches
        {
            return Err(RestoreError::RestoreManifestBindingMismatch { operation_id });
        }
        return Ok(RestoreCommandOutcome {
            operation: existing.record,
            commit_version: commit_version_from_read(context.read_version)?,
            replayed: true,
            affected_members: 0,
        });
    }
    let source_workspace = load_visible_workspace(store, context, &request.source_workbench_id)?;
    if source_workspace.record.incarnation_id != request.expected_source_workspace_incarnation_id {
        return Err(RestoreError::SourceWorkspaceMismatch);
    }
    let (source, source_snapshot, source_commit, lease_guard) = match request.source {
        RestoreSourceSelector::Snapshot(snapshot_id) => {
            let key = snapshot_ref_key(
                context.root_id,
                source_workspace.record.incarnation_id,
                snapshot_id,
            );
            let loaded = read_record(
                store,
                context,
                MetadataFamily::SnapshotRef,
                &key,
                SnapshotRefRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotMissing { snapshot_id })?;
            if loaded.record.state != SnapshotState::Active {
                return Err(RestoreError::SnapshotNotActive {
                    snapshot_id,
                    state: loaded.record.state,
                });
            }
            let clock = store.lease_clock_high_water()?;
            if loaded.record.lease_deadline_ms <= clock {
                return Err(RestoreError::SnapshotLeaseExpired {
                    snapshot_id,
                    lease_clock_ms: clock,
                    lease_deadline_ms: loaded.record.lease_deadline_ms,
                });
            }
            let lease_deadline_ms = loaded.record.lease_deadline_ms;
            (
                RestoreSource::Snapshot {
                    snapshot_id,
                    read_version: loaded.record.read_version,
                },
                Some((key, loaded)),
                None,
                Some((snapshot_id, lease_deadline_ms)),
            )
        }
        RestoreSourceSelector::Commit(commit_id) => {
            let key = commit_key(context.root_id, commit_id);
            let loaded = read_record(
                store,
                context,
                MetadataFamily::Commit,
                &key,
                CommitRecord::decode,
            )?
            .ok_or(RestoreError::CommitMissing { commit_id })?;
            if loaded.record.state != CommitState::Sealed {
                return Err(RestoreError::CommitNotSealed {
                    commit_id,
                    state: loaded.record.state,
                });
            }
            if loaded.record.source_workspace_incarnation_id
                != source_workspace.record.incarnation_id
            {
                return Err(RestoreError::SourceWorkspaceMismatch);
            }
            (
                RestoreSource::Commit { commit_id },
                None,
                Some((key, loaded)),
                None,
            )
        }
    };
    let source_commit_record = match (&source, &source_snapshot, &source_commit) {
        (RestoreSource::Snapshot { snapshot_id, .. }, Some((_, snapshot)), None) => {
            let commit_id = snapshot.record.source_commit_id.ok_or(
                RestoreError::SnapshotCommitProvenanceMissing {
                    snapshot_id: *snapshot_id,
                },
            )?;
            let loaded = read_record(
                store,
                context,
                MetadataFamily::Commit,
                &commit_key(context.root_id, commit_id),
                CommitRecord::decode,
            )?
            .ok_or(RestoreError::CommitMissing { commit_id })?;
            if loaded.record.state != CommitState::Sealed {
                return Err(RestoreError::CommitNotSealed {
                    commit_id,
                    state: loaded.record.state,
                });
            }
            let snapshot_consumer = read_record(
                store,
                context,
                MetadataFamily::CommitConsumer,
                &snapshot_commit_consumer_key(context.root_id, commit_id, *snapshot_id),
                CommitConsumerRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotRetentionMismatch)?;
            if loaded.record.consumer_count == 0
                || snapshot_consumer.record.consumer_epoch_at_add > loaded.record.consumer_epoch
            {
                return Err(RestoreError::SnapshotRetentionMismatch);
            }
            loaded
        }
        (RestoreSource::Commit { .. }, None, Some((_, commit))) => commit.clone(),
        _ => {
            return Err(RestoreError::SourceClosureMismatch {
                reason: "restore source retention shape is inconsistent".to_owned(),
            })
        }
    };
    if source_commit_record.record.source_workspace_incarnation_id
        != source_workspace.record.incarnation_id
    {
        return Err(RestoreError::SourceWorkspaceMismatch);
    }

    let destination_key = workspace_current_key(context.root_id, &request.destination_workbench_id);
    let previous_destination = read_record(
        store,
        context,
        MetadataFamily::WorkspaceCurrent,
        &destination_key,
        WorkspaceRecord::decode,
    )?;
    if let Some(previous) = &previous_destination {
        if previous.record.state != WorkspaceState::Retired {
            return Err(RestoreError::DestinationExists {
                workbench_id: request.destination_workbench_id.clone(),
            });
        }
        let previous_operation_id = previous
            .record
            .owning_operation_id
            .ok_or(RestoreError::DestinationMarkerMismatch)?;
        let previous_operation = load_operation(store, context, previous_operation_id)?;
        if previous_operation.record.phase != RestorePhase::Cleaned
            || previous_operation
                .record
                .destination_workspace_incarnation_id
                != previous.record.incarnation_id
            || previous_operation.record.destination_workbench_id
                != request.destination_workbench_id
        {
            return Err(RestoreError::DestinationMarkerMismatch);
        }
        if !store
            .scan_prefix_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::PathCurrent,
                &path_child_prefix(context.root_id, previous.record.incarnation_id, None),
                context.read_version,
                None,
                1,
            )?
            .is_empty()
        {
            return Err(RestoreError::DestinationMarkerMismatch);
        }
    }
    let destination_claim_key = workspace_incarnation_claim_key(
        context.root_id,
        request.destination_workspace_incarnation_id,
    );
    if let Some(claim) = read_record(
        store,
        context,
        MetadataFamily::WorkspaceIncarnationClaim,
        &destination_claim_key,
        WorkspaceIncarnationClaimRecord::decode,
    )? {
        return Err(RestoreError::DestinationIncarnationClaimed {
            incarnation_id: request.destination_workspace_incarnation_id,
            workbench_id: claim.record.workbench_id,
        });
    }
    let source_commit_id = match source {
        RestoreSource::Snapshot { .. } => match &source_snapshot {
            Some((_, snapshot)) => snapshot.record.source_commit_id.ok_or(
                RestoreError::SnapshotCommitProvenanceMissing {
                    snapshot_id: match source {
                        RestoreSource::Snapshot { snapshot_id, .. } => snapshot_id,
                        RestoreSource::Commit { .. } => unreachable!(),
                    },
                },
            )?,
            None => unreachable!("snapshot restore retained its snapshot row"),
        },
        RestoreSource::Commit { commit_id } => commit_id,
    };
    let parent_digest =
        advance_commit_parent_rolling_digest([0; SHA256_BYTES], 0, source_commit_id);
    let operation = RestoreOperationRecord {
        operation_id,
        identity_digest,
        initialization_digest: None,
        source_workbench_id: request.source_workbench_id.clone(),
        source_workspace_incarnation_id: source_workspace.record.incarnation_id,
        source,
        destination_workbench_id: request.destination_workbench_id.clone(),
        destination_workspace_incarnation_id: request.destination_workspace_incarnation_id,
        destination_restore_manifest_identity: Some(request.destination_restore_manifest_identity),
        restore_manifest: request.restore_manifest.clone(),
        commit_provenance: RestoreCommitProvenance::V5(Box::new(RestoreCommitProvenanceV5 {
            source_commit: RestoreSourceCommitSeal {
                commit_id: source_commit_id,
                content_digest_uri: source_commit_record.record.content_digest_uri.clone(),
                manifest_digest_uri: source_commit_record.record.manifest_digest_uri.clone(),
                tree_manifest_revision_id: source_commit_record.record.tree_manifest_revision_id,
                member_count: source_commit_record.record.member_count,
                member_digest: source_commit_record.record.member_digest,
                unique_revision_count: source_commit_record.record.unique_revision_count,
                revision_digest: source_commit_record.record.revision_digest,
                parent_digest: source_commit_record.record.parent_digest,
            },
            destination_committed_at_unix_seconds: request.destination_committed_at_unix_seconds,
            destination_binding: None,
            closure: RestoreCommitClosureProgress {
                member_cursor: None,
                member_count: 0,
                member_digest: [0; SHA256_BYTES],
                member_seal: None,
                revision_ref_count: 0,
                revision_cursor: None,
                revision_seal_count: 0,
                revision_digest: [0; SHA256_BYTES],
                revision_seal: None,
                parent_digest,
                parent_seal: None,
                cleanup_member_count: 0,
                cleanup_revision_count: 0,
            },
            destination_head_generation: None,
        })),
        phase: RestorePhase::Preparing,
        source_cursor: None,
        source_eof: false,
        source_member_count: 0,
        source_member_rolling_digest: [0; SHA256_BYTES],
        source_member_seal: None,
        source_matches_base_commit: None,
        next_member_sequence: 0,
        member_rolling_digest: [0; SHA256_BYTES],
        member_seal: None,
        cleanup_member_cursor: 0,
        result: None,
        terminal_error: None,
    };
    let operation_payload = operation.encode()?;
    let staging_workspace = WorkspaceRecord {
        incarnation_id: request.destination_workspace_incarnation_id,
        workspace_revision: WorkspaceRevision::ZERO,
        state: WorkspaceState::Staging,
        owning_operation_id: Some(operation_id),
    };
    let mut plan = CommandPlan::default();
    plan.put_absent(
        MetadataFamily::WorkspaceIncarnationClaim,
        destination_claim_key,
        WorkspaceIncarnationClaimRecord {
            workbench_id: request.destination_workbench_id.clone(),
        }
        .encode()?,
    )?;
    plan.put_absent(
        MetadataFamily::Operation,
        operation_key,
        operation_payload.clone(),
    )?;
    match previous_destination {
        None => plan.put_absent(
            MetadataFamily::WorkspaceCurrent,
            destination_key,
            staging_workspace.encode()?,
        )?,
        Some(previous) => plan.replace(
            MetadataFamily::WorkspaceCurrent,
            destination_key,
            previous.payload,
            staging_workspace.encode()?,
        )?,
    }
    plan.assert_value(
        MetadataFamily::WorkspaceCurrent,
        workspace_current_key(context.root_id, &request.source_workbench_id),
        Some(source_workspace.payload),
    )?;
    plan.prefix_empty(
        MetadataFamily::PathCurrent,
        path_child_prefix(
            context.root_id,
            request.destination_workspace_incarnation_id,
            None,
        ),
    );

    match (source_snapshot, source_commit) {
        (Some((snapshot_key, loaded)), None) => {
            let mut next = loaded.record.clone();
            next.consumer_count = next
                .consumer_count
                .checked_add(1)
                .ok_or(RestoreError::ConsumerCountOverflow)?;
            next.consumer_epoch = increment_consumer_epoch(next.consumer_epoch)?;
            plan.replace(
                MetadataFamily::SnapshotRef,
                snapshot_key,
                loaded.payload,
                next.encode()?,
            )?;
            plan.put_absent(
                MetadataFamily::HistoryHold,
                restore_history_hold_key(context.root_id, operation_id),
                HistoryHoldRecord {
                    read_version: next.read_version,
                    source_snapshot_id: Some(match source {
                        RestoreSource::Snapshot { snapshot_id, .. } => snapshot_id,
                        RestoreSource::Commit { .. } => unreachable!(),
                    }),
                    state: HistoryHoldState::Active,
                }
                .encode(),
            )?;
        }
        (None, Some((commit_key, loaded))) => {
            let mut next = loaded.record.clone();
            next.consumer_count = next
                .consumer_count
                .checked_add(1)
                .ok_or(RestoreError::ConsumerCountOverflow)?;
            next.consumer_epoch = increment_consumer_epoch(next.consumer_epoch)?;
            next.last_zero_consumer_version = None;
            plan.replace(
                MetadataFamily::Commit,
                commit_key,
                loaded.payload,
                next.encode()?,
            )?;
            plan.put_absent(
                MetadataFamily::CommitConsumer,
                lease_commit_consumer_key(
                    context.root_id,
                    match source {
                        RestoreSource::Commit { commit_id } => commit_id,
                        RestoreSource::Snapshot { .. } => unreachable!(),
                    },
                    operation_id,
                ),
                CommitConsumerRecord {
                    consumer_epoch_at_add: next.consumer_epoch,
                }
                .encode(),
            )?;
        }
        _ => unreachable!("one restore source is loaded"),
    }

    execute_plan(
        store,
        context,
        plan,
        request_digest,
        operation_payload,
        lease_guard,
        operation_id,
        0,
    )
}

/// Change a newly-created restore from `Preparing` to `Copying`.
pub fn start_restore_copy(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let input_digest = operation_input_digest(b"start-copy", request.operation_id);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Preparing, "Preparing")?;
    validate_source_retention(store, context, &loaded.record)?;
    let next = loaded
        .record
        .apply(RestorePhase::Preparing, RestoreTransition::BeginCopying)?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

/// Copy one canonical source page into the hidden destination.
pub fn copy_restore_batch(
    store: &MetaShard,
    context: RootWriteContext,
    request: CopyRestoreBatchRequest,
) -> Result<CopyRestoreBatchOutcome, RestoreError> {
    if !(1..=MAX_RESTORE_BATCH_MEMBERS).contains(&request.limit) {
        return Err(RestoreError::InvalidBatchLimit {
            requested: request.limit,
            max: MAX_RESTORE_BATCH_MEMBERS,
        });
    }
    let input_digest = batch_input_digest(b"copy", request.operation_id, request.limit);
    if let Some(command) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(CopyRestoreBatchOutcome {
            copied_members: command.affected_members as usize,
            source_eof: command.operation.source_eof,
            command,
        });
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Copying, "Copying")?;
    if loaded.record.source_eof {
        return Err(RestoreError::SourceAlreadyExhausted);
    }
    let page = scan_source_members(store, context, &loaded.record, request.limit)?;
    let mut command_items = 2_usize;
    let mut revisions = BTreeSet::new();
    let mut admitted_raw = 0_usize;
    let mut admitted_materialized = 0_usize;
    for scanned in &page.members {
        let Some(member) = scanned.materialized.as_ref() else {
            admitted_raw += 1;
            continue;
        };
        let projection = TypedProjection::decode(&member.entry.typed_index_projection)?;
        let new_revision = revisions.insert(member.entry.artifact_revision_id);
        let additional = 3 + projection.fields().len() + usize::from(new_revision);
        if command_items.saturating_add(additional) > MAX_COMMAND_ITEMS {
            break;
        }
        command_items += additional;
        admitted_raw += 1;
        admitted_materialized += 1;
        if admitted_materialized == request.limit {
            break;
        }
    }
    if page.members.is_empty() {
        let planned = plan_copy_batch(store, context, &loaded, &[], page.eof, input_digest)?;
        let command = execute_restore_command(
            store,
            &planned.command.command,
            input_digest,
            None,
            request.operation_id,
        )?;
        return Ok(CopyRestoreBatchOutcome {
            copied_members: command.affected_members as usize,
            source_eof: command.operation.source_eof,
            command,
        });
    }

    let mut selected = None;
    for count in (1..=admitted_raw).rev() {
        let planned = plan_copy_batch(
            store,
            context,
            &loaded,
            &page.members[..count],
            page.eof && count == page.members.len(),
            input_digest,
        )?;
        if !command_fits(store, &planned.command.command)?
            || !every_copied_member_can_be_cleaned(store, context, &planned, input_digest)?
        {
            continue;
        }
        selected = Some(planned);
        break;
    }

    let Some(planned) = selected else {
        let (plan, operation, operation_payload) =
            plan_restore_abort(store, context, loaded, capacity_exceeded_error())?;
        let command = build_restore_command(context, plan, input_digest, operation_payload, 0);
        let outcome =
            execute_restore_command(store, &command, input_digest, None, request.operation_id)?;
        debug_assert_eq!(outcome.operation, operation);
        return Ok(CopyRestoreBatchOutcome {
            copied_members: 0,
            source_eof: false,
            command: outcome,
        });
    };
    let command = execute_restore_command(
        store,
        &planned.command.command,
        input_digest,
        None,
        request.operation_id,
    )?;
    Ok(CopyRestoreBatchOutcome {
        copied_members: command.affected_members as usize,
        source_eof: command.operation.source_eof,
        command,
    })
}

fn plan_copy_batch(
    store: &MetaShard,
    context: RootWriteContext,
    loaded: &Loaded<RestoreOperationRecord>,
    scanned_members: &[ScannedSourceMember],
    source_eof: bool,
    input_digest: [u8; SHA256_BYTES],
) -> Result<PlannedCopyBatch, RestoreError> {
    let members = scanned_members
        .iter()
        .filter_map(|member| member.materialized.as_ref())
        .collect::<Vec<_>>();
    let raw_additional =
        u64::try_from(scanned_members.len()).expect("restore raw batch length fits u64");
    let next_source_count = loaded
        .record
        .source_member_count
        .checked_add(raw_additional)
        .ok_or(RestoreError::SourceClosureMismatch {
            reason: "raw source member count overflow".to_owned(),
        })?;
    let additional = u64::try_from(members.len()).expect("restore batch length fits u64");
    let next_sequence = loaded
        .record
        .next_member_sequence
        .checked_add(additional)
        .ok_or(RestoreError::SourceClosureMismatch {
            reason: "member sequence overflow".to_owned(),
        })?;
    if next_sequence > super::restore_records::MAX_RESTORE_MEMBERS
        || next_source_count > super::restore_records::MAX_RESTORE_MEMBERS
    {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "source exceeds the restore member limit".to_owned(),
        });
    }

    let mut revision_occurrences = BTreeMap::<ArtifactRevisionId, u64>::new();
    for member in &members {
        let count = revision_occurrences
            .entry(member.entry.artifact_revision_id)
            .or_default();
        *count = count
            .checked_add(1)
            .ok_or(RestoreError::ReferenceCountOverflow {
                revision: member.entry.artifact_revision_id,
            })?;
    }
    let mut revision_updates = BTreeMap::<
        ArtifactRevisionId,
        (Loaded<ArtifactRevisionRecord>, ArtifactRevisionRecord),
    >::new();
    for (revision, count) in revision_occurrences {
        let loaded_revision = load_available_revision(store, context, revision)?;
        for member in members
            .iter()
            .filter(|member| member.entry.artifact_revision_id == revision)
        {
            if loaded_revision.record.logical_size != member.entry.logical_size
                || loaded_revision.record.body_digest_uri != member.entry.body_digest_uri
                || loaded_revision.record.manifest_digest_uri != member.entry.manifest_digest_uri
                || loaded_revision.record.dependency_count != member.entry.dependency_count
                || loaded_revision.record.dependency_depth != member.entry.dependency_depth
                || loaded_revision.record.content_type != member.entry.content_type
            {
                return Err(RestoreError::SourceClosureMismatch {
                    reason: format!(
                        "source path {} does not match artifact revision {:02x?}",
                        member.path,
                        revision.as_bytes()
                    ),
                });
            }
        }
        let mut next_revision = loaded_revision.record.clone();
        next_revision.reference_epoch =
            increment_reference_epoch(next_revision.reference_epoch, revision)?;
        next_revision.strong_reference_count = next_revision
            .strong_reference_count
            .checked_add(count)
            .ok_or(RestoreError::ReferenceCountOverflow { revision })?;
        next_revision.last_zero_ref_version = None;
        revision_updates.insert(revision, (loaded_revision, next_revision));
    }

    let mut next = loaded.record.clone();
    let mut rolling = next.member_rolling_digest;
    for (offset, member) in members.iter().enumerate() {
        let sequence = next
            .next_member_sequence
            .checked_add(u64::try_from(offset).expect("batch offset fits u64"))
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: "member sequence overflow".to_owned(),
            })?;
        rolling = advance_commit_member_rolling_digest(rolling, sequence, member.row_digest);
    }
    let mut source_rolling = next.source_member_rolling_digest;
    for (offset, member) in scanned_members.iter().enumerate() {
        let sequence = next
            .source_member_count
            .checked_add(u64::try_from(offset).expect("raw batch offset fits u64"))
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: "raw source member sequence overflow".to_owned(),
            })?;
        source_rolling =
            advance_commit_member_rolling_digest(source_rolling, sequence, member.row_digest);
    }
    if let Some(last) = scanned_members.last() {
        next.source_cursor = Some(last.path.clone());
    }
    next.source_eof = source_eof;
    next.source_member_count = next_source_count;
    next.source_member_rolling_digest = source_rolling;
    next.next_member_sequence = next_sequence;
    next.member_rolling_digest = rolling;
    next.validate()?;
    let next_payload = next.encode()?;

    let staging = load_staging_workspace(store, context, &next)?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        loaded.payload.clone(),
        next_payload.clone(),
    )?;
    plan.assert_value(
        MetadataFamily::WorkspaceCurrent,
        workspace_current_key(context.root_id, &next.destination_workbench_id),
        Some(staging.payload.clone()),
    )?;

    let mut copied = Vec::with_capacity(members.len());
    for (offset, member) in members.iter().enumerate() {
        let sequence = loaded
            .record
            .next_member_sequence
            .checked_add(u64::try_from(offset).expect("batch offset fits u64"))
            .expect("sequence bound was checked");
        let entry_payload = member.entry.encode()?;
        plan.put_absent(
            MetadataFamily::PathCurrent,
            path_current_key(
                context.root_id,
                next.destination_workspace_incarnation_id,
                &member.path,
            ),
            entry_payload.clone(),
        )?;
        let revision_after_copy = revision_updates
            .get(&member.entry.artifact_revision_id)
            .expect("every source revision has one update")
            .1
            .clone();
        let revision_ref_payload = RevisionRefRecord {
            reference_epoch_at_add: revision_after_copy.reference_epoch,
        }
        .encode()?;
        plan.put_absent(
            MetadataFamily::RevisionRef,
            path_revision_ref_key(
                context.root_id,
                next.destination_workspace_incarnation_id,
                &member.path,
                member.entry.artifact_revision_id,
            ),
            revision_ref_payload.clone(),
        )?;
        let restore_member_key = restore_member_key(context.root_id, next.operation_id, sequence);
        let restore_member_payload = RestoreMemberRecord {
            destination_path: member.path.clone(),
            artifact_revision_id: member.entry.artifact_revision_id,
            path_generation: member.entry.generation,
            row_digest: member.row_digest,
        }
        .encode();
        plan.put_absent(
            MetadataFamily::RestoreMember,
            restore_member_key.clone(),
            restore_member_payload.clone(),
        )?;
        let index_rows = secondary_index_rows(
            context.root_id,
            next.destination_workspace_incarnation_id,
            &member.path,
            &member.entry,
        )?;
        for (key, value) in &index_rows {
            plan.put_absent(MetadataFamily::SecondaryIndex, key.clone(), value.clone())?;
        }
        copied.push(PlannedCopiedMember {
            sequence,
            path: member.path.clone(),
            entry_payload,
            revision: member.entry.artifact_revision_id,
            revision_ref_payload,
            restore_member_key,
            restore_member_payload,
            secondary_index_rows: index_rows,
            revision_after_copy,
        });
    }
    for (revision, (loaded_revision, next_revision)) in revision_updates {
        plan.replace(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(context.root_id, revision),
            loaded_revision.payload,
            next_revision.encode()?,
        )?;
    }
    let affected_members =
        u32::try_from(members.len()).expect("bounded restore batch length fits u32");
    let command =
        build_restore_command(context, plan, input_digest, next_payload, affected_members);
    Ok(PlannedCopyBatch {
        command: PlannedRestoreCommand {
            command,
            operation: next,
        },
        members: copied,
        staging_payload: staging.payload,
    })
}

fn command_fits(store: &MetaShard, command: &MetadataCommand) -> Result<bool, RestoreError> {
    match store.command_fit(command, None) {
        Ok(CommandFit::Fits) => Ok(true),
        Ok(CommandFit::Exceeds { .. }) => Ok(false),
        Err(source) => Err(RestoreError::Meta(source)),
    }
}

fn capacity_exceeded_error() -> RestoreTerminalError {
    RestoreTerminalError {
        kind: RestoreTerminalErrorKind::InvariantViolation,
        message: CAPACITY_EXCEEDED_MESSAGE.to_owned(),
        evidence_digest: None,
    }
}

fn maximum_cleanup_terminal_error() -> RestoreTerminalError {
    RestoreTerminalError {
        kind: RestoreTerminalErrorKind::CleanupFailed,
        message: "x".repeat(MAX_RESTORE_TERMINAL_ERROR_BYTES),
        evidence_digest: Some([0; SHA256_BYTES]),
    }
}

fn every_copied_member_can_be_cleaned(
    store: &MetaShard,
    context: RootWriteContext,
    batch: &PlannedCopyBatch,
    input_digest: [u8; SHA256_BYTES],
) -> Result<bool, RestoreError> {
    for member in &batch.members {
        let command = plan_single_member_cleanup_command(context, batch, member, input_digest)?;
        if !command_fits(store, &command.command)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn plan_single_member_cleanup_command(
    context: RootWriteContext,
    batch: &PlannedCopyBatch,
    member: &PlannedCopiedMember,
    input_digest: [u8; SHA256_BYTES],
) -> Result<PlannedRestoreCommand, RestoreError> {
    let mut cleaning = maximum_cleanup_operation(&batch.command.operation)?;
    cleaning.cleanup_member_cursor = member.sequence;
    cleaning.validate()?;
    let cleaning_payload = cleaning.encode()?;
    let mut next = cleaning.clone();
    next.cleanup_member_cursor =
        member
            .sequence
            .checked_add(1)
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: "cleanup cursor overflow".to_owned(),
            })?;
    next.validate()?;
    let next_payload = next.encode()?;

    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        cleaning_payload,
        next_payload.clone(),
    )?;
    plan.assert_value(
        MetadataFamily::WorkspaceCurrent,
        workspace_current_key(context.root_id, &next.destination_workbench_id),
        Some(batch.staging_payload.clone()),
    )?;
    for (key, value) in &member.secondary_index_rows {
        plan.delete(MetadataFamily::SecondaryIndex, key.clone(), value.clone())?;
    }
    plan.delete(
        MetadataFamily::PathCurrent,
        path_current_key(
            context.root_id,
            next.destination_workspace_incarnation_id,
            &member.path,
        ),
        member.entry_payload.clone(),
    )?;
    plan.delete(
        MetadataFamily::RevisionRef,
        path_revision_ref_key(
            context.root_id,
            next.destination_workspace_incarnation_id,
            &member.path,
            member.revision,
        ),
        member.revision_ref_payload.clone(),
    )?;

    // Exercise the exact, heavier zero-reference cleanup branch. Any real
    // non-zero cleanup writes a strict subset of these rows with the same
    // fixed-width revision values.
    let mut revision_before = member.revision_after_copy.clone();
    revision_before.strong_reference_count = 1;
    revision_before.last_zero_ref_version = None;
    let mut revision_after = revision_before.clone();
    revision_after.reference_epoch =
        increment_reference_epoch(revision_after.reference_epoch, member.revision)?;
    revision_after.strong_reference_count = 0;
    let zero_version = next_commit_version(context.read_version)?;
    revision_after.last_zero_ref_version = Some(zero_version);
    plan.replace(
        MetadataFamily::ArtifactRevision,
        artifact_revision_key(context.root_id, member.revision),
        revision_before.encode()?,
        revision_after.encode()?,
    )?;
    plan.put_absent(
        MetadataFamily::GcCandidate,
        gc_candidate_key(
            context.root_id,
            member.revision,
            revision_after.reference_epoch,
        ),
        GcCandidateRecord {
            last_zero_ref_version: zero_version,
            claim_state: GcClaimState::Candidate,
            retry_count: 0,
            quarantine_evidence: None,
        }
        .encode()?,
    )?;
    plan.delete(
        MetadataFamily::RestoreMember,
        member.restore_member_key.clone(),
        member.restore_member_payload.clone(),
    )?;
    Ok(PlannedRestoreCommand {
        command: build_restore_command(context, plan, input_digest, next_payload, 1),
        operation: next,
    })
}

fn maximum_cleanup_operation(
    copied: &RestoreOperationRecord,
) -> Result<RestoreOperationRecord, RestoreError> {
    // A member admitted by an early copy page must remain cleanable after the
    // restore reaches its largest later operation shape. Model the maximum
    // source cursor plus the seal and initialization digests before deriving
    // the worst legal abort and zero-reference cleanup transaction.
    let mut ready = copied.clone();
    ready.source_cursor = Some(
        NormalizedRelativePath::new("x".repeat(NormalizedRelativePath::MAX_BYTES))
            .expect("the path type accepts its documented maximum byte length"),
    );
    ready.source_eof = true;
    // Preserve the independently accumulated raw-source closure. It may
    // contain one or two provenance manifests that are deliberately absent
    // from the ordinary RestoreMember ledger. Collapsing it to the
    // materialized closure would make a commit source appear to diverge from
    // its immutable CommitRecord and under-size snapshot cleanup records.
    let source_member_seal = ready.source_member_rolling_digest;
    ready = ready.apply(
        RestorePhase::Copying,
        RestoreTransition::SealSource { source_member_seal },
    )?;
    let RestoreCommitProvenance::V5(provenance) = &ready.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let restore_identity = ready.destination_restore_manifest_identity.ok_or(
        RestoreError::RestoreManifestBindingMismatch {
            operation_id: ready.operation_id,
        },
    )?;
    let mut destination_commit_bytes = provenance.source_commit.commit_id.into_bytes();
    destination_commit_bytes[0] ^= 0xff;
    let destination_commit_id = CommitId::from_bytes(destination_commit_bytes);
    let mut run_operation_bytes = *restore_identity.publication_operation_id.as_bytes();
    run_operation_bytes[0] ^= 0xff;
    if run_operation_bytes == *ready.operation_id.as_bytes() {
        run_operation_bytes[1] ^= 0xff;
    }
    let run_publication_operation_id = OperationId::from_bytes(run_operation_bytes);
    let mut run_revision_bytes = *restore_identity.artifact_revision_id.as_bytes();
    run_revision_bytes[0] ^= 0xff;
    let run_revision_id = ArtifactRevisionId::from_bytes(run_revision_bytes);
    let effective_content_digest_uri = if ready.source_matches_base_commit == Some(true) {
        provenance.source_commit.content_digest_uri.clone()
    } else {
        commit_member_tree_digest_uri(ready.member_rolling_digest)
    };
    let binding = RestoreDestinationBinding {
        destination_commit_id,
        effective_content_digest_uri,
        destination_projection_input_digest: [0x7a; SHA256_BYTES],
        run_manifest_identity: RestoreManifestIdentity {
            publication_operation_id: run_publication_operation_id,
            artifact_revision_id: run_revision_id,
        },
        restore_manifest_identity: restore_identity,
        manifests: None,
    };
    ready = ready.apply(
        RestorePhase::SourceSealed,
        RestoreTransition::BindDestination { binding },
    )?;
    let manifests = super::restore_records::RestoreDestinationManifests {
        run_manifest: RestoreManifestPublication {
            publication_operation_id: run_publication_operation_id,
            workspace_incarnation_id: ready.destination_workspace_incarnation_id,
            artifact_revision_id: run_revision_id,
            body_digest_uri: commit_member_tree_digest_uri([0x41; SHA256_BYTES]),
            manifest_digest_uri: commit_member_tree_digest_uri([0x42; SHA256_BYTES]),
            logical_size: 1,
            content_type: "application/json".to_owned(),
        },
        restore_manifest: RestoreManifestPublication {
            publication_operation_id: restore_identity.publication_operation_id,
            workspace_incarnation_id: ready.destination_workspace_incarnation_id,
            artifact_revision_id: restore_identity.artifact_revision_id,
            body_digest_uri: ready.restore_manifest.body_digest_uri.clone(),
            manifest_digest_uri: commit_member_tree_digest_uri([0x43; SHA256_BYTES]),
            logical_size: ready.restore_manifest.logical_size,
            content_type: ready.restore_manifest.content_type.clone(),
        },
    };
    ready = ready.apply(
        RestorePhase::SourceSealed,
        RestoreTransition::BeginDestinationBuilding {
            initialization_digest: [0x44; SHA256_BYTES],
            manifests,
        },
    )?;
    let aborting = ready.apply(
        RestorePhase::DestinationBuilding,
        RestoreTransition::BeginAbort {
            terminal_error: maximum_cleanup_terminal_error(),
        },
    )?;
    aborting
        .apply(RestorePhase::Aborting, RestoreTransition::BeginCleaning)
        .map_err(Into::into)
}

/// Re-read the frozen source and ordered member ledger, then seal the exact
/// closure in one operation CAS.
pub fn seal_restore_source(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let input_digest = operation_input_digest(b"seal-source", request.operation_id);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Copying, "Copying")?;
    if !loaded.record.source_eof {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "source has not reached EOF".to_owned(),
        });
    }
    verify_source_and_members(store, context, &loaded.record)?;
    let next = loaded.record.apply(
        RestorePhase::Copying,
        RestoreTransition::SealSource {
            source_member_seal: loaded.record.source_member_rolling_digest,
        },
    )?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

/// Bind the caller-authoritative destination commit and both destination-owned
/// projections after the complete source closure is durable.
pub fn bind_restore_destination(
    store: &MetaShard,
    context: RootWriteContext,
    request: &BindRestoreDestinationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let input_digest = bind_destination_input_digest(request);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::SourceSealed, "SourceSealed")?;
    let RestoreCommitProvenance::V5(provenance) = &loaded.record.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    if let Some(existing) = &provenance.destination_binding {
        if existing != &request.binding {
            return Err(RestoreError::DestinationBindingMismatch {
                operation_id: request.operation_id,
            });
        }
        return Ok(RestoreCommandOutcome {
            operation: loaded.record,
            commit_version: commit_version_from_read(context.read_version)?,
            replayed: true,
            affected_members: 0,
        });
    }
    let next = loaded.record.apply(
        RestorePhase::SourceSealed,
        RestoreTransition::BindDestination {
            binding: request.binding.clone(),
        },
    )?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    plan.assert_value(
        MetadataFamily::Commit,
        commit_key(context.root_id, request.binding.destination_commit_id),
        None,
    )?;
    plan.prefix_empty(
        MetadataFamily::CommitMember,
        commit_member_prefix(context.root_id, request.binding.destination_commit_id),
    );
    plan.prefix_empty(
        MetadataFamily::RevisionRef,
        commit_revision_ref_prefix(context.root_id, request.binding.destination_commit_id),
    );
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

/// Read the immutable source commit-owned run manifest held by an active
/// restore. The caller cannot substitute a path, revision, or read view.
///
/// Snapshot restores deliberately use the base commit member rather than the
/// possibly newer snapshot path: the base run manifest is the authority used
/// to construct the destination-owned projection. The snapshot read version
/// is returned only so the projection layer can label its frozen source view.
pub fn read_restore_source_run_manifest(
    store: &MetaShard,
    context: RootWriteContext,
    operation_id: OperationId,
) -> Result<RestoreSourceRunManifest, RestoreError> {
    let loaded = load_operation(store, context, operation_id)?;
    if !matches!(
        loaded.record.phase,
        RestorePhase::SourceSealed
            | RestorePhase::DestinationBuilding
            | RestorePhase::DestinationSealing
            | RestorePhase::Ready
    ) {
        return Err(RestoreError::InvalidPhase {
            expected: "SourceSealed, DestinationBuilding, DestinationSealing, or Ready",
            actual: loaded.record.phase,
        });
    }
    validate_source_retention(store, context, &loaded.record)?;
    let RestoreCommitProvenance::V5(provenance) = &loaded.record.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let source_commit_id = provenance.source_commit.commit_id;
    let path = NormalizedRelativePath::new(RUN_MANIFEST_PATH)
        .expect("canonical run manifest path is normalized");
    let member = read_record(
        store,
        context,
        MetadataFamily::CommitMember,
        &commit_member_key(context.root_id, source_commit_id, &path),
        CommitMemberRecord::decode,
    )?
    .ok_or(RestoreError::SourceClosureMismatch {
        reason: "source commit does not retain its run manifest member".to_owned(),
    })?;
    if member.record.artifact_revision_id != provenance.source_commit.tree_manifest_revision_id {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "source run manifest revision does not match the immutable base commit"
                .to_owned(),
        });
    }
    let path_entry = path_entry_from_commit_member(&member.record);
    validate_destination_manifest_entry(&path_entry)?;
    validate_manifest_revision(store, context, &path_entry)?;
    let source_snapshot_read_version = match loaded.record.source {
        RestoreSource::Snapshot { read_version, .. } => Some(read_version),
        RestoreSource::Commit { .. } => None,
    };
    Ok(RestoreSourceRunManifest {
        operation: loaded.record,
        source_commit_id,
        source_snapshot_read_version,
        path_entry,
    })
}

/// Install both already-published destination-owned manifests while the
/// destination remains hidden, then begin bounded commit-closure construction.
pub fn apply_restore_initialization(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let loaded = load_operation(store, context, request.operation_id)?;
    if loaded.record.phase == RestorePhase::DestinationBuilding {
        let initialization_digest = loaded.record.initialization_digest.ok_or_else(|| {
            RestoreError::SourceClosureMismatch {
                reason: "destination build is missing its initialization digest".to_owned(),
            }
        })?;
        let input_digest = initialization_input_digest(request.operation_id, initialization_digest);
        if let Some(outcome) =
            replay_outcome(store, context, input_digest, Some(request.operation_id))?
        {
            return Ok(outcome);
        }
        return Ok(RestoreCommandOutcome {
            operation: loaded.record,
            commit_version: commit_version_from_read(context.read_version)?,
            replayed: true,
            affected_members: 0,
        });
    }
    require_phase(&loaded.record, RestorePhase::SourceSealed, "SourceSealed")?;
    let RestoreCommitProvenance::V5(provenance) = &loaded.record.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let binding = provenance.destination_binding.as_ref().ok_or(
        RestoreError::DestinationBindingMismatch {
            operation_id: request.operation_id,
        },
    )?;
    if binding.manifests.is_some() {
        return Err(RestoreError::DestinationBindingMismatch {
            operation_id: request.operation_id,
        });
    }
    let run = load_restore_manifest_publication(
        store,
        context,
        &loaded.record,
        RUN_MANIFEST_PATH,
        binding.run_manifest_identity,
    )?;
    let restore = load_restore_manifest_publication(
        store,
        context,
        &loaded.record,
        RESTORE_MANIFEST_PATH,
        binding.restore_manifest_identity,
    )?;
    let initialization = RestoreInitialization {
        restore_manifest: restore.path.record.clone(),
    };
    validate_initialization(&initialization, &loaded.record)?;
    let manifests = super::restore_records::RestoreDestinationManifests {
        run_manifest: run.publication.clone(),
        restore_manifest: restore.publication.clone(),
    };
    let initialization_digest = restore_initialization_digest(&loaded.record, &manifests);
    let input_digest = initialization_input_digest(request.operation_id, initialization_digest);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let next = loaded.record.apply(
        RestorePhase::SourceSealed,
        RestoreTransition::BeginDestinationBuilding {
            initialization_digest,
            manifests,
        },
    )?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    run.add_predicates(&mut plan)?;
    restore.add_predicates(&mut plan)?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

/// Build a bounded canonical page of the hidden destination commit closure.
/// CommitRecord remains absent throughout this phase.
pub fn build_restore_commit_members(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreClosureBatchRequest,
) -> Result<BuildRestoreCommitBatchOutcome, RestoreError> {
    if !(1..=MAX_RESTORE_BATCH_MEMBERS).contains(&request.limit) {
        return Err(RestoreError::InvalidBatchLimit {
            requested: request.limit,
            max: MAX_RESTORE_BATCH_MEMBERS,
        });
    }
    let input_digest = batch_input_digest(
        b"build-destination-commit-members",
        request.operation_id,
        request.limit,
    );
    if let Some(command) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(BuildRestoreCommitBatchOutcome {
            built_members: command.affected_members as usize,
            members_complete: command.operation.phase == RestorePhase::DestinationSealing,
            command,
        });
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    if loaded.record.phase == RestorePhase::DestinationSealing {
        return Ok(BuildRestoreCommitBatchOutcome {
            command: RestoreCommandOutcome {
                operation: loaded.record,
                commit_version: commit_version_from_read(context.read_version)?,
                replayed: true,
                affected_members: 0,
            },
            built_members: 0,
            members_complete: true,
        });
    }
    require_phase(
        &loaded.record,
        RestorePhase::DestinationBuilding,
        "DestinationBuilding",
    )?;
    let (destination_commit_id, member_cursor) = match &loaded.record.commit_provenance {
        RestoreCommitProvenance::V5(provenance) => {
            let binding = provenance.destination_binding.as_ref().ok_or(
                RestoreError::DestinationBindingMismatch {
                    operation_id: request.operation_id,
                },
            )?;
            (
                binding.destination_commit_id,
                provenance.closure.member_cursor.clone(),
            )
        }
        RestoreCommitProvenance::MissingLegacyV4 => {
            return Err(RestoreError::CommitRetentionMismatch);
        }
    };
    let prefix = path_child_prefix(
        context.root_id,
        loaded.record.destination_workspace_incarnation_id,
        None,
    );
    let marker = member_cursor.as_ref().map(|path| {
        path_current_key(
            context.root_id,
            loaded.record.destination_workspace_incarnation_id,
            path,
        )
    });
    let mut rows = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::PathCurrent,
        &prefix,
        context.read_version,
        marker.as_deref(),
        request.limit + 1,
    )?;
    let has_more = rows.len() > request.limit;
    if has_more {
        rows.truncate(request.limit);
    }

    let mut selected = None;
    for count in (0..=rows.len()).rev() {
        if count == 0 && !rows.is_empty() {
            break;
        }
        let planned = plan_restore_commit_member_batch(
            store,
            context,
            &loaded,
            destination_commit_id,
            &rows[..count],
            !has_more && count == rows.len(),
            input_digest,
        )?;
        if command_fits(store, &planned.command)? {
            selected = Some((planned, count));
            break;
        }
    }
    let Some((planned, built_members)) = selected else {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "one destination commit member exceeds the metadata command budget".to_owned(),
        });
    };
    let command = execute_restore_command(
        store,
        &planned.command,
        input_digest,
        None,
        request.operation_id,
    )?;
    debug_assert_eq!(command.operation, planned.operation);
    Ok(BuildRestoreCommitBatchOutcome {
        members_complete: command.operation.phase == RestorePhase::DestinationSealing,
        command,
        built_members,
    })
}

/// Re-scan a bounded page of sorted destination commit RevisionRef rows and
/// move to Ready only after the complete revision and single-parent seals are
/// verified. CommitRecord is still absent.
pub fn seal_restore_commit_revisions(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreClosureBatchRequest,
) -> Result<SealRestoreCommitBatchOutcome, RestoreError> {
    if !(1..=MAX_RESTORE_BATCH_MEMBERS).contains(&request.limit) {
        return Err(RestoreError::InvalidBatchLimit {
            requested: request.limit,
            max: MAX_RESTORE_BATCH_MEMBERS,
        });
    }
    let input_digest = batch_input_digest(
        b"seal-destination-commit-revisions",
        request.operation_id,
        request.limit,
    );
    if let Some(command) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(SealRestoreCommitBatchOutcome {
            sealed_revisions: command.affected_members as usize,
            ready: command.operation.phase == RestorePhase::Ready,
            command,
        });
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    if loaded.record.phase == RestorePhase::Ready {
        return Ok(SealRestoreCommitBatchOutcome {
            command: RestoreCommandOutcome {
                operation: loaded.record,
                commit_version: commit_version_from_read(context.read_version)?,
                replayed: true,
                affected_members: 0,
            },
            sealed_revisions: 0,
            ready: true,
        });
    }
    require_phase(
        &loaded.record,
        RestorePhase::DestinationSealing,
        "DestinationSealing",
    )?;
    let (destination_commit_id, revision_cursor) = match &loaded.record.commit_provenance {
        RestoreCommitProvenance::V5(provenance) => {
            let binding = provenance.destination_binding.as_ref().ok_or(
                RestoreError::DestinationBindingMismatch {
                    operation_id: request.operation_id,
                },
            )?;
            (
                binding.destination_commit_id,
                provenance.closure.revision_cursor,
            )
        }
        RestoreCommitProvenance::MissingLegacyV4 => {
            return Err(RestoreError::CommitRetentionMismatch);
        }
    };
    let prefix = commit_revision_ref_prefix(context.root_id, destination_commit_id);
    let marker = revision_cursor
        .map(|revision| commit_revision_ref_key(context.root_id, destination_commit_id, revision));
    let mut rows = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::RevisionRef,
        &prefix,
        context.read_version,
        marker.as_deref(),
        request.limit + 1,
    )?;
    let has_more = rows.len() > request.limit;
    if has_more {
        rows.truncate(request.limit);
    }
    let mut next = loaded.record.clone();
    {
        let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
            unreachable!("phase validation excludes legacy operations");
        };
        for row in &rows {
            let revision = decode_restore_commit_revision_ref_key(&prefix, &row.key)?;
            RevisionRefRecord::decode(&row.value)?;
            let step = plan_commit_revision(
                provenance.closure.revision_cursor,
                provenance.closure.revision_seal_count,
                provenance.closure.revision_digest,
                revision,
                &row.value,
            )
            .map_err(|error| RestoreError::SourceClosureMismatch {
                reason: error.to_string(),
            })?;
            provenance.closure.revision_cursor = Some(step.cursor);
            provenance.closure.revision_seal_count = step.count;
            provenance.closure.revision_digest = step.digest;
        }
    }
    if !has_more {
        verify_destination_commit_scaffolding(store, context, &next)?;
        let (revision_seal, parent_seal) = match &next.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => {
                if provenance.closure.revision_seal_count != provenance.closure.revision_ref_count {
                    return Err(RestoreError::SourceClosureMismatch {
                        reason: "destination revision-ref seal count is incomplete".to_owned(),
                    });
                }
                (
                    provenance.closure.revision_digest,
                    provenance.closure.parent_digest,
                )
            }
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        };
        next = next.apply(
            RestorePhase::DestinationSealing,
            RestoreTransition::MarkReady {
                revision_seal,
                parent_seal,
            },
        )?;
    } else {
        next.validate()?;
    }
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    plan.assert_value(
        MetadataFamily::Commit,
        commit_key(context.root_id, destination_commit_id),
        None,
    )?;
    let command = execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        u32::try_from(rows.len()).expect("bounded revision page fits u32"),
    )?;
    Ok(SealRestoreCommitBatchOutcome {
        sealed_revisions: rows.len(),
        ready: command.operation.phase == RestorePhase::Ready,
        command,
    })
}

/// Atomically publish the sealed destination commit, its first Workbench head,
/// the visible workspace marker, and the parent-child retention handoff.
pub fn complete_restore(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<CompleteRestoreOutcome, RestoreError> {
    let input_digest = operation_input_digest(b"complete", request.operation_id);
    if let Some(command) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        let result =
            command
                .operation
                .result
                .clone()
                .ok_or(RestoreError::DeterministicResultMismatch {
                    reason: "complete replay does not contain a result".to_owned(),
                })?;
        return Ok(CompleteRestoreOutcome { command, result });
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Ready, "Ready")?;
    validate_source_retention(store, context, &loaded.record)?;
    verify_destination_commit_scaffolding(store, context, &loaded.record)?;
    let (
        source_commit_id,
        source_manifest_digest_uri,
        binding,
        closure,
        destination_committed_at_unix_seconds,
    ) = match &loaded.record.commit_provenance {
        RestoreCommitProvenance::V5(provenance) => (
            provenance.source_commit.commit_id,
            provenance.source_commit.manifest_digest_uri.clone(),
            provenance
                .destination_binding
                .as_ref()
                .ok_or(RestoreError::DestinationBindingMismatch {
                    operation_id: request.operation_id,
                })?
                .clone(),
            provenance.closure.clone(),
            provenance.destination_committed_at_unix_seconds,
        ),
        RestoreCommitProvenance::MissingLegacyV4 => {
            return Err(RestoreError::CommitRetentionMismatch);
        }
    };
    let manifests = binding
        .manifests
        .as_ref()
        .ok_or(RestoreError::DestinationBindingMismatch {
            operation_id: request.operation_id,
        })?;
    let staging = load_staging_workspace(store, context, &loaded.record)?;
    let workspace_revision = staging
        .record
        .workspace_revision
        .get()
        .checked_add(1)
        .map(WorkspaceRevision::new)
        .ok_or(RestoreError::WorkspaceRevisionOverflow)?;
    let result = RestoreResult {
        destination_workspace_incarnation_id: loaded.record.destination_workspace_incarnation_id,
        destination_workspace_revision: workspace_revision,
        member_count: loaded.record.next_member_sequence,
        member_digest: loaded.record.member_seal.ok_or_else(|| {
            RestoreError::SourceClosureMismatch {
                reason: "ready restore is missing its source seal".to_owned(),
            }
        })?,
    };
    let next = loaded.record.apply(
        RestorePhase::Ready,
        RestoreTransition::Complete {
            result: result.clone(),
            destination_head_generation: Generation::new(1).expect("one is non-zero"),
        },
    )?;
    let next_payload = next.encode()?;
    let visible = WorkspaceRecord {
        incarnation_id: staging.record.incarnation_id,
        workspace_revision,
        state: WorkspaceState::Visible,
        owning_operation_id: None,
    };
    let head_generation = Generation::new(1).expect("one is non-zero");
    let destination_commit = CommitRecord {
        source_workspace_incarnation_id: next.destination_workspace_incarnation_id,
        content_digest_uri: binding.effective_content_digest_uri.clone(),
        manifest_digest_uri: source_manifest_digest_uri,
        tree_manifest_revision_id: manifests.run_manifest.artifact_revision_id,
        tree_digest_uri: commit_member_tree_digest_uri(closure.member_digest),
        member_count: closure.member_count,
        member_digest: closure.member_digest,
        unique_revision_count: closure.revision_ref_count,
        revision_digest: closure.revision_digest,
        parent_commits: vec![source_commit_id],
        parent_digest: closure.parent_digest,
        producer: None,
        lineage_projection: Vec::new(),
        consumer_count: 1,
        consumer_epoch: ConsumerEpoch::new(1),
        last_zero_consumer_version: None,
        state: CommitState::Sealed,
    };
    destination_commit.validate()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    plan.replace(
        MetadataFamily::WorkspaceCurrent,
        workspace_current_key(context.root_id, &next.destination_workbench_id),
        staging.payload,
        visible.encode()?,
    )?;
    plan.put_absent(
        MetadataFamily::Commit,
        commit_key(context.root_id, binding.destination_commit_id),
        destination_commit.encode()?,
    )?;
    plan.put_absent(
        MetadataFamily::WorkbenchCommitHead,
        workbench_commit_head_key(context.root_id, next.destination_workspace_incarnation_id),
        WorkbenchCommitHeadRecord {
            commit_id: binding.destination_commit_id,
            head_generation,
        }
        .encode(),
    )?;
    plan.put_absent(
        MetadataFamily::CommitConsumer,
        workbench_head_commit_consumer_key(
            context.root_id,
            binding.destination_commit_id,
            next.destination_workspace_incarnation_id,
        ),
        CommitConsumerRecord {
            consumer_epoch_at_add: ConsumerEpoch::new(1),
        }
        .encode(),
    )?;
    handoff_source_retention_to_child(
        store,
        context,
        &loaded.record,
        binding.destination_commit_id,
        &mut plan,
    )?;
    let (run_publication, restore_publication) =
        verify_destination_manifest_publications(store, context, &loaded.record)?;
    run_publication.add_predicates(&mut plan)?;
    restore_publication.add_predicates(&mut plan)?;
    plan.events
        .push(change_event_projection(&ChangeEventRecord {
            workbench_id: next.destination_workbench_id.clone(),
            workspace_incarnation_id: result.destination_workspace_incarnation_id,
            kind: ChangeEventKind::WorkspaceRestored,
            artifact_revision_id: None,
            commit_id: Some(binding.destination_commit_id),
            operation_id: Some(request.operation_id),
            path: None,
            before: TypedProjection::empty(),
            after: TypedProjection::new(BTreeMap::from([
                (
                    QueryFieldId::new("restore.member_count")?,
                    QueryScalar::Unsigned(result.member_count),
                ),
                (
                    QueryFieldId::new("restore.member_digest")?,
                    QueryScalar::Bytes(result.member_digest.to_vec()),
                ),
                (
                    QueryFieldId::new("restore.workspace_revision")?,
                    QueryScalar::Unsigned(result.destination_workspace_revision.get()),
                ),
                (
                    QueryFieldId::new("restore.destination_committed_at")?,
                    QueryScalar::Unsigned(destination_committed_at_unix_seconds),
                ),
            ]))?,
        })?);
    let command = execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )?;
    Ok(CompleteRestoreOutcome { command, result })
}

/// Win the terminal race against publication and persist the exact abort
/// reason. Source retention remains attached until cleanup is proven complete.
pub fn abort_restore(
    store: &MetaShard,
    context: RootWriteContext,
    request: &AbortRestoreRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let input_digest = abort_input_digest(request.operation_id, &request.terminal_error)?;
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    if !matches!(
        loaded.record.phase,
        RestorePhase::Preparing
            | RestorePhase::Copying
            | RestorePhase::SourceSealed
            | RestorePhase::DestinationBuilding
            | RestorePhase::DestinationSealing
            | RestorePhase::Ready
    ) {
        return Err(RestoreError::InvalidPhase {
            expected:
                "Preparing, Copying, SourceSealed, DestinationBuilding, DestinationSealing, or Ready",
            actual: loaded.record.phase,
        });
    }
    let (plan, _next, next_payload) =
        plan_restore_abort(store, context, loaded, request.terminal_error.clone())?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

fn plan_restore_abort(
    store: &MetaShard,
    context: RootWriteContext,
    loaded: Loaded<RestoreOperationRecord>,
    terminal_error: RestoreTerminalError,
) -> Result<(CommandPlan, RestoreOperationRecord, Vec<u8>), RestoreError> {
    let next = loaded.record.apply(
        loaded.record.phase,
        RestoreTransition::BeginAbort { terminal_error },
    )?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    Ok((plan, next, next_payload))
}

/// Transfer an aborted restore into its durable cleanup phase.
pub fn start_restore_cleanup(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let input_digest = operation_input_digest(b"start-cleanup", request.operation_id);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Aborting, "Aborting")?;
    let next = loaded
        .record
        .apply(RestorePhase::Aborting, RestoreTransition::BeginCleaning)?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

/// Delete one bounded contiguous range of staged destination members.
pub fn cleanup_restore_batch(
    store: &MetaShard,
    context: RootWriteContext,
    request: CopyRestoreBatchRequest,
) -> Result<CopyRestoreBatchOutcome, RestoreError> {
    if !(1..=MAX_RESTORE_BATCH_MEMBERS).contains(&request.limit) {
        return Err(RestoreError::InvalidBatchLimit {
            requested: request.limit,
            max: MAX_RESTORE_BATCH_MEMBERS,
        });
    }
    let input_digest = batch_input_digest(b"cleanup", request.operation_id, request.limit);
    if let Some(command) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(CopyRestoreBatchOutcome {
            copied_members: command.affected_members as usize,
            source_eof: command.operation.cleanup_member_cursor
                == command.operation.next_member_sequence,
            command,
        });
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Cleaning, "Cleaning")?;
    if restore_commit_cleanup_pending(&loaded.record) {
        return cleanup_restore_commit_scaffolding_batch(
            store,
            context,
            request,
            loaded,
            input_digest,
        );
    }
    if loaded.record.cleanup_member_cursor == loaded.record.next_member_sequence {
        return Ok(CopyRestoreBatchOutcome {
            command: RestoreCommandOutcome {
                operation: loaded.record,
                commit_version: commit_version_from_read(context.read_version)?,
                replayed: true,
                affected_members: 0,
            },
            copied_members: 0,
            source_eof: true,
        });
    }
    let remaining = loaded
        .record
        .next_member_sequence
        .checked_sub(loaded.record.cleanup_member_cursor)
        .expect("validated cleanup cursor cannot exceed the member count");
    let requested_count = usize::try_from(remaining.min(request.limit as u64))
        .expect("bounded cleanup count fits usize");
    let mut members = Vec::with_capacity(requested_count);
    let mut path_changes = Vec::with_capacity(requested_count);
    let mut command_items = 2_usize;
    let mut revisions = BTreeSet::new();
    for offset in 0..requested_count {
        let sequence = loaded
            .record
            .cleanup_member_cursor
            .checked_add(u64::try_from(offset).expect("batch offset fits u64"))
            .expect("validated member count bounds the cleanup sequence");
        let key = restore_member_key(context.root_id, request.operation_id, sequence);
        let member = read_record(
            store,
            context,
            MetadataFamily::RestoreMember,
            &key,
            RestoreMemberRecord::decode,
        )?
        .ok_or_else(|| RestoreError::SourceClosureMismatch {
            reason: format!("restore member sequence {sequence} is missing"),
        })?;
        let current = load_path(
            store,
            context,
            loaded.record.destination_workspace_incarnation_id,
            &member.record.destination_path,
        )?;
        let additional = match &current {
            None => 1,
            Some(path) => {
                let projection = TypedProjection::decode(&path.record.typed_index_projection)?;
                let new_revision = revisions.insert(path.record.artifact_revision_id);
                3 + projection.fields().len() + if new_revision { 2 } else { 0 }
            }
        };
        if command_items.saturating_add(additional) > MAX_COMMAND_ITEMS {
            break;
        }
        command_items += additional;
        path_changes.push(PathChange {
            path: member.record.destination_path.clone(),
            before: current,
            after: None,
        });
        members.push((key, member));
    }
    let mut selected = None;
    for count in (1..=members.len()).rev() {
        let planned = plan_cleanup_batch(
            store,
            context,
            &loaded,
            &members[..count],
            &path_changes[..count],
            input_digest,
        )?;
        if command_fits(store, &planned.command)? {
            selected = Some(planned);
            break;
        }
    }
    let Some(planned) = selected else {
        let planned = plan_cleanup_capacity_quarantine(store, context, loaded, input_digest)?;
        let command = execute_restore_command(
            store,
            &planned.command,
            input_digest,
            None,
            request.operation_id,
        )?;
        return Ok(CopyRestoreBatchOutcome {
            copied_members: 0,
            source_eof: false,
            command,
        });
    };
    let command = execute_restore_command(
        store,
        &planned.command,
        input_digest,
        None,
        request.operation_id,
    )?;
    Ok(CopyRestoreBatchOutcome {
        copied_members: command.affected_members as usize,
        source_eof: command.operation.cleanup_member_cursor
            == command.operation.next_member_sequence,
        command,
    })
}

fn restore_commit_cleanup_pending(operation: &RestoreOperationRecord) -> bool {
    match &operation.commit_provenance {
        RestoreCommitProvenance::MissingLegacyV4 => false,
        RestoreCommitProvenance::V5(provenance) => {
            provenance.closure.cleanup_revision_count != provenance.closure.revision_ref_count
                || provenance.closure.cleanup_member_count != provenance.closure.member_count
        }
    }
}

fn cleanup_restore_commit_scaffolding_batch(
    store: &MetaShard,
    context: RootWriteContext,
    request: CopyRestoreBatchRequest,
    loaded: Loaded<RestoreOperationRecord>,
    input_digest: [u8; SHA256_BYTES],
) -> Result<CopyRestoreBatchOutcome, RestoreError> {
    let (destination_commit_id, cleanup_revisions, remaining) =
        match &loaded.record.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => {
                let binding = provenance.destination_binding.as_ref().ok_or(
                    RestoreError::DestinationBindingMismatch {
                        operation_id: request.operation_id,
                    },
                )?;
                if provenance.closure.cleanup_revision_count < provenance.closure.revision_ref_count
                {
                    (
                        binding.destination_commit_id,
                        true,
                        provenance
                            .closure
                            .revision_ref_count
                            .checked_sub(provenance.closure.cleanup_revision_count)
                            .expect("validated cleanup cursor"),
                    )
                } else {
                    (
                        binding.destination_commit_id,
                        false,
                        provenance
                            .closure
                            .member_count
                            .checked_sub(provenance.closure.cleanup_member_count)
                            .expect("validated cleanup cursor"),
                    )
                }
            }
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        };
    let limit = usize::try_from(remaining.min(request.limit as u64))
        .expect("bounded cleanup count fits usize");
    let (family, prefix) = if cleanup_revisions {
        (
            MetadataFamily::RevisionRef,
            commit_revision_ref_prefix(context.root_id, destination_commit_id),
        )
    } else {
        (
            MetadataFamily::CommitMember,
            commit_member_prefix(context.root_id, destination_commit_id),
        )
    };
    let rows = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        family,
        &prefix,
        context.read_version,
        None,
        limit,
    )?;
    if rows.is_empty() {
        return Err(RestoreError::SourceClosureMismatch {
            reason: if cleanup_revisions {
                "destination commit revision refs are missing before cleanup completed".to_owned()
            } else {
                "destination commit members are missing before cleanup completed".to_owned()
            },
        });
    }
    let mut next = loaded.record.clone();
    let mut plan = CommandPlan::default();
    if cleanup_revisions {
        for row in &rows {
            let revision_id = decode_restore_commit_revision_ref_key(&prefix, &row.key)?;
            let reference = RevisionRefRecord::decode(&row.value)?;
            let revision = load_available_revision(store, context, revision_id)?;
            if reference.reference_epoch_at_add > revision.record.reference_epoch {
                return Err(RestoreError::ReferenceEpochAhead {
                    revision: revision_id,
                });
            }
            let mut next_revision = revision.record.clone();
            next_revision.reference_epoch =
                increment_reference_epoch(next_revision.reference_epoch, revision_id)?;
            next_revision.strong_reference_count = next_revision
                .strong_reference_count
                .checked_sub(1)
                .ok_or(RestoreError::ReferenceCountUnderflow {
                    revision: revision_id,
                })?;
            let zero_version = next_commit_version(context.read_version)?;
            next_revision.last_zero_ref_version =
                (next_revision.strong_reference_count == 0).then_some(zero_version);
            plan.replace(
                MetadataFamily::ArtifactRevision,
                artifact_revision_key(context.root_id, revision_id),
                revision.payload,
                next_revision.encode()?,
            )?;
            plan.delete(
                MetadataFamily::RevisionRef,
                row.key.clone(),
                row.value.clone(),
            )?;
            if next_revision.strong_reference_count == 0 {
                plan.put_absent(
                    MetadataFamily::GcCandidate,
                    gc_candidate_key(context.root_id, revision_id, next_revision.reference_epoch),
                    GcCandidateRecord {
                        last_zero_ref_version: zero_version,
                        claim_state: GcClaimState::Candidate,
                        retry_count: 0,
                        quarantine_evidence: None,
                    }
                    .encode()?,
                )?;
            }
        }
        let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
            unreachable!();
        };
        provenance.closure.cleanup_revision_count = provenance
            .closure
            .cleanup_revision_count
            .checked_add(u64::try_from(rows.len()).expect("bounded batch fits u64"))
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: "destination revision cleanup cursor overflow".to_owned(),
            })?;
    } else {
        for row in &rows {
            let path = decode_commit_member_key(context.root_id, destination_commit_id, &row.key)
                .ok_or(RestoreError::CorruptKey {
                family: "CommitMember(destination)",
            })?;
            CommitMemberRecord::decode(&row.value).map_err(|error| {
                RestoreError::CorruptSourceMember {
                    family: "CommitMember(destination)",
                    path: path.as_str().to_owned(),
                    detail: error.to_string(),
                }
            })?;
            plan.delete(
                MetadataFamily::CommitMember,
                row.key.clone(),
                row.value.clone(),
            )?;
        }
        let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
            unreachable!();
        };
        provenance.closure.cleanup_member_count = provenance
            .closure
            .cleanup_member_count
            .checked_add(u64::try_from(rows.len()).expect("bounded batch fits u64"))
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: "destination member cleanup cursor overflow".to_owned(),
            })?;
    }
    next.validate()?;
    let next_payload = next.encode()?;
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    let command = execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        u32::try_from(rows.len()).expect("bounded cleanup page fits u32"),
    )?;
    Ok(CopyRestoreBatchOutcome {
        copied_members: rows.len(),
        source_eof: !restore_commit_cleanup_pending(&command.operation)
            && command.operation.cleanup_member_cursor == command.operation.next_member_sequence,
        command,
    })
}

fn plan_cleanup_capacity_quarantine(
    store: &MetaShard,
    context: RootWriteContext,
    loaded: Loaded<RestoreOperationRecord>,
    input_digest: [u8; SHA256_BYTES],
) -> Result<PlannedRestoreCommand, RestoreError> {
    let next = loaded.record.apply(
        RestorePhase::Cleaning,
        RestoreTransition::Quarantine {
            terminal_error: RestoreTerminalError {
                kind: RestoreTerminalErrorKind::CleanupFailed,
                message: CLEANUP_CAPACITY_EXCEEDED_MESSAGE.to_owned(),
                evidence_digest: None,
            },
        },
    )?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    Ok(PlannedRestoreCommand {
        command: build_restore_command(context, plan, input_digest, next_payload, 0),
        operation: next,
    })
}

fn plan_cleanup_batch(
    store: &MetaShard,
    context: RootWriteContext,
    loaded: &Loaded<RestoreOperationRecord>,
    members: &[(Vec<u8>, Loaded<RestoreMemberRecord>)],
    path_changes: &[PathChange],
    input_digest: [u8; SHA256_BYTES],
) -> Result<PlannedRestoreCommand, RestoreError> {
    let count = members.len();
    let mut next = loaded.record.clone();
    next.cleanup_member_cursor = next
        .cleanup_member_cursor
        .checked_add(u64::try_from(count).expect("batch count fits u64"))
        .expect("cleanup cursor is bounded by member count");
    next.validate()?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        loaded.payload.clone(),
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    apply_path_changes(
        store,
        context,
        next.destination_workspace_incarnation_id,
        path_changes.to_vec(),
        &mut plan,
    )?;
    for (key, member) in members {
        plan.delete(
            MetadataFamily::RestoreMember,
            key.clone(),
            member.payload.clone(),
        )?;
    }
    let affected_members = u32::try_from(count).expect("bounded batch count fits u32");
    Ok(PlannedRestoreCommand {
        command: build_restore_command(context, plan, input_digest, next_payload, affected_members),
        operation: next,
    })
}

fn inspect_restore_cleanup_publications(
    store: &MetaShard,
    context: RootWriteContext,
    restore: &RestoreOperationRecord,
) -> Result<RestoreCleanupPublications, RestoreError> {
    let RestoreCommitProvenance::V5(provenance) = &restore.commit_provenance else {
        return Ok(RestoreCleanupPublications {
            observations: Vec::new(),
            disposition: RestoreCleanupPublicationDisposition::Ready,
        });
    };
    let Some(binding) = provenance.destination_binding.as_ref() else {
        // RestoreStaging forward admission requires the late destination
        // binding. No publisher can legally exist before it is installed.
        return Ok(RestoreCleanupPublications {
            observations: Vec::new(),
            disposition: RestoreCleanupPublicationDisposition::Ready,
        });
    };
    let expected = [
        (RUN_MANIFEST_PATH, binding.run_manifest_identity),
        (RESTORE_MANIFEST_PATH, binding.restore_manifest_identity),
    ];
    let mut observations = Vec::with_capacity(expected.len());
    let mut pending = None;
    let mut quarantined = false;
    for (expected_path, identity) in expected {
        let key = operation_key(
            context.root_id,
            OperationKind::Publish,
            identity.publication_operation_id,
        );
        let payload = read_payload(store, context, MetadataFamily::Operation, &key)?;
        let phase = match payload.as_deref() {
            None => None,
            Some(payload) => {
                let publish = PublishOperationRecord::decode(payload)?;
                let path = NormalizedRelativePath::new(expected_path)
                    .expect("reserved destination manifest path is normalized");
                if publish.operation_id != identity.publication_operation_id
                    || publish.authority
                        != (PublishAuthority::RestoreStaging {
                            restore_operation_id: restore.operation_id,
                        })
                    || publish.workbench_id != restore.destination_workbench_id
                    || publish.workspace_incarnation_id
                        != restore.destination_workspace_incarnation_id
                    || publish.path != path
                    || publish.artifact_revision_id != identity.artifact_revision_id
                    || publish.claim != PublishClaim::CreateOnly
                {
                    return Err(RestoreError::ManifestBindingMismatch);
                }
                Some(publish.phase)
            }
        };
        match phase {
            None | Some(PublishPhase::Published | PublishPhase::Cleaned) => {}
            Some(PublishPhase::Quarantined) => quarantined = true,
            Some(
                phase @ (PublishPhase::Uploading
                | PublishPhase::Finalizing
                | PublishPhase::Aborting
                | PublishPhase::Cleaning),
            ) => {
                pending.get_or_insert((identity.publication_operation_id, phase));
            }
        }
        observations.push(RestoreCleanupPublicationObservation { key, payload });
    }
    // Keep the restore in Cleaning while any publisher can still make
    // automatic cleanup progress. Moving to Quarantined first would revoke
    // that publisher's cleanup authority and strand a mixed
    // Quarantined+nonterminal pair.
    let disposition = if let Some((operation_id, phase)) = pending {
        RestoreCleanupPublicationDisposition::Pending {
            operation_id,
            phase,
        }
    } else if quarantined {
        RestoreCleanupPublicationDisposition::Quarantined
    } else {
        RestoreCleanupPublicationDisposition::Ready
    };
    Ok(RestoreCleanupPublications {
        observations,
        disposition,
    })
}

fn quarantine_restore_for_publication_cleanup(
    store: &MetaShard,
    context: RootWriteContext,
    loaded: Loaded<RestoreOperationRecord>,
    publications: &RestoreCleanupPublications,
    input_digest: [u8; SHA256_BYTES],
) -> Result<RestoreCommandOutcome, RestoreError> {
    let next = loaded.record.apply(
        RestorePhase::Cleaning,
        RestoreTransition::Quarantine {
            terminal_error: RestoreTerminalError {
                kind: RestoreTerminalErrorKind::CleanupFailed,
                message: QUARANTINED_PUBLICATION_MESSAGE.to_owned(),
                evidence_digest: Some(publications.evidence_digest()),
            },
        },
    )?;
    let next_payload = next.encode()?;
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    publications.add_predicates(&mut plan)?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        next.operation_id,
        0,
    )
}

/// Retire the hidden marker and release source retention only after the member
/// ledger is empty and its cleanup cursor proves the full closure was removed.
pub fn finish_restore_cleanup(
    store: &MetaShard,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let input_digest = operation_input_digest(b"finish-cleanup", request.operation_id);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    let loaded = load_operation(store, context, request.operation_id)?;
    require_phase(&loaded.record, RestorePhase::Cleaning, "Cleaning")?;
    if loaded.record.cleanup_member_cursor != loaded.record.next_member_sequence {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "cleanup cursor has not consumed the full member closure".to_owned(),
        });
    }
    let publications = inspect_restore_cleanup_publications(store, context, &loaded.record)?;
    match publications.disposition {
        RestoreCleanupPublicationDisposition::Ready => {}
        RestoreCleanupPublicationDisposition::Pending {
            operation_id,
            phase,
        } => {
            return Err(RestoreError::PublicationCleanupPending {
                operation_id,
                phase,
            });
        }
        RestoreCleanupPublicationDisposition::Quarantined => {
            return quarantine_restore_for_publication_cleanup(
                store,
                context,
                loaded,
                &publications,
                input_digest,
            );
        }
    }
    let staging = load_staging_workspace(store, context, &loaded.record)?;
    let manifest_paths = [
        NormalizedRelativePath::new(RUN_MANIFEST_PATH)
            .expect("canonical run manifest path is normalized"),
        restore_manifest_path(),
    ];
    let manifests = manifest_paths
        .iter()
        .map(|path| {
            load_path(
                store,
                context,
                loaded.record.destination_workspace_incarnation_id,
                path,
            )
            .map(|loaded| (path.clone(), loaded))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let next = loaded
        .record
        .apply(RestorePhase::Cleaning, RestoreTransition::FinishCleanup)?;
    let next_payload = next.encode()?;
    let retired = WorkspaceRecord {
        incarnation_id: staging.record.incarnation_id,
        workspace_revision: staging.record.workspace_revision,
        state: WorkspaceState::Retired,
        owning_operation_id: Some(request.operation_id),
    };
    let mut plan = CommandPlan::default();
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    plan.replace(
        MetadataFamily::WorkspaceCurrent,
        workspace_current_key(context.root_id, &next.destination_workbench_id),
        staging.payload,
        retired.encode()?,
    )?;
    plan.prefix_empty(
        MetadataFamily::RestoreMember,
        restore_member_prefix(context.root_id, request.operation_id),
    );
    let destination_commit_id = match &loaded.record.commit_provenance {
        RestoreCommitProvenance::V5(provenance) => provenance
            .destination_binding
            .as_ref()
            .map(|binding| binding.destination_commit_id),
        RestoreCommitProvenance::MissingLegacyV4 => None,
    };
    if let Some(destination_commit_id) = destination_commit_id {
        plan.prefix_empty(
            MetadataFamily::CommitMember,
            commit_member_prefix(context.root_id, destination_commit_id),
        );
        plan.prefix_empty(
            MetadataFamily::RevisionRef,
            commit_revision_ref_prefix(context.root_id, destination_commit_id),
        );
        plan.assert_value(
            MetadataFamily::Commit,
            commit_key(context.root_id, destination_commit_id),
            None,
        )?;
    }
    publications.add_predicates(&mut plan)?;
    apply_path_changes(
        store,
        context,
        next.destination_workspace_incarnation_id,
        manifests
            .into_iter()
            .map(|(path, before)| PathChange {
                path,
                before,
                after: None,
            })
            .collect(),
        &mut plan,
    )?;
    release_source_retention(store, context, &loaded.record, &mut plan)?;
    execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        0,
    )
}

/// Read one exact restore operation through current placement and owner fences.
pub fn get_restore(
    store: &MetaShard,
    context: RootWriteContext,
    operation_id: OperationId,
) -> Result<Option<RestoreOperationRecord>, RestoreError> {
    read_record(
        store,
        context,
        MetadataFamily::Operation,
        &operation_key(context.root_id, OperationKind::Restore, operation_id),
        RestoreOperationRecord::decode,
    )
    .map(|loaded| loaded.map(|loaded| loaded.record))
}

fn scan_source_members(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    limit: usize,
) -> Result<SourcePage, RestoreError> {
    validate_source_retention(store, context, operation)?;
    scan_source_page(
        store,
        context,
        operation,
        operation.source_cursor.as_ref(),
        limit,
    )
}

fn scan_source_page(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    start_after: Option<&nokv_types::NormalizedRelativePath>,
    limit: usize,
) -> Result<SourcePage, RestoreError> {
    let (family, prefix, marker, version) = match operation.source {
        RestoreSource::Snapshot { read_version, .. } => (
            MetadataFamily::PathCurrent,
            path_child_prefix(
                context.root_id,
                operation.source_workspace_incarnation_id,
                None,
            ),
            start_after.map(|path| {
                path_current_key(
                    context.root_id,
                    operation.source_workspace_incarnation_id,
                    path,
                )
            }),
            read_version,
        ),
        RestoreSource::Commit { commit_id } => (
            MetadataFamily::CommitMember,
            commit_member_prefix(context.root_id, commit_id),
            start_after.map(|path| commit_member_key_for_marker(context.root_id, commit_id, path)),
            context.read_version,
        ),
    };
    // Probe window: limit + 1 for the EOF probe + 2 for the at most two
    // skipped provenance rows (run manifest and restore manifest), so a
    // page never comes back empty with eof=false and the copy cursor
    // always advances.
    let mut scanned = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        family,
        &prefix,
        version,
        marker.as_deref(),
        limit.saturating_add(3),
    )?;
    let maximum_raw = limit.saturating_add(2);
    let has_more = scanned.len() > maximum_raw;
    if has_more {
        scanned.truncate(maximum_raw);
    }
    let members = scanned
        .into_iter()
        .map(|item| decode_source_member(context.root_id, operation, item))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SourcePage {
        members,
        eof: !has_more,
    })
}

fn decode_source_member(
    root_id: nokv_types::RootId,
    operation: &RestoreOperationRecord,
    item: MetadataScanItem,
) -> Result<ScannedSourceMember, RestoreError> {
    let (family, path, entry) = match operation.source {
        RestoreSource::Snapshot { .. } => {
            let path = decode_path_current_key(
                root_id,
                operation.source_workspace_incarnation_id,
                &item.key,
            )
            .ok_or(RestoreError::CorruptKey {
                family: "PathCurrent",
            })?;
            let entry = PathEntry::decode(&item.value).map_err(|error| {
                RestoreError::CorruptSourceMember {
                    family: "PathCurrent",
                    path: path.as_str().to_owned(),
                    detail: error.to_string(),
                }
            })?;
            ("PathCurrent", path, entry)
        }
        RestoreSource::Commit { commit_id } => {
            let path = decode_commit_member_key(root_id, commit_id, &item.key).ok_or(
                RestoreError::CorruptKey {
                    family: "CommitMember",
                },
            )?;
            let member = CommitMemberRecord::decode(&item.value).map_err(|error| {
                RestoreError::CorruptSourceMember {
                    family: "CommitMember",
                    path: path.as_str().to_owned(),
                    detail: error.to_string(),
                }
            })?;
            let entry = path_entry_from_commit_member(&member);
            ("CommitMember", path, entry)
        }
    };
    let canonical_member = CommitMemberRecord {
        artifact_revision_id: entry.artifact_revision_id,
        path_generation: entry.generation,
        body_digest_uri: entry.body_digest_uri.clone(),
        manifest_digest_uri: entry.manifest_digest_uri.clone(),
        logical_size: entry.logical_size,
        dependency_count: entry.dependency_count,
        dependency_depth: entry.dependency_depth,
        content_type: entry.content_type.clone(),
        producer: entry.producer.clone(),
        manifest_id: entry.manifest_id.clone(),
        typed_projection: entry.typed_index_projection.clone(),
    };
    let row_digest = commit_member_row_digest(&path, &canonical_member)?;
    // Source-owned provenance is part of the frozen raw closure but is never
    // materialized. The hidden destination publishes both manifests under its
    // own identity before its immutable commit closure is built.
    if matches!(path.as_str(), RESTORE_MANIFEST_PATH | RUN_MANIFEST_PATH) {
        return Ok(ScannedSourceMember {
            path,
            row_digest,
            materialized: None,
        });
    }
    TypedProjection::decode(&entry.typed_index_projection).map_err(|error| {
        RestoreError::CorruptSourceMember {
            family,
            path: path.as_str().to_owned(),
            detail: error.to_string(),
        }
    })?;
    Ok(ScannedSourceMember {
        path: path.clone(),
        row_digest,
        materialized: Some(SourceMember {
            path,
            entry,
            row_digest,
        }),
    })
}

fn verify_source_and_members(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<(), RestoreError> {
    validate_source_retention(store, context, operation)?;
    let mut cursor = None;
    let mut source_sequence = 0_u64;
    let mut source_rolling = [0; SHA256_BYTES];
    let mut materialized_sequence = 0_u64;
    let mut materialized_rolling = [0; SHA256_BYTES];
    loop {
        // 253 keeps scan_source_page's limit + 3 probe window within the
        // metadata layer's 256-item scan bound.
        let page = scan_source_page(store, context, operation, cursor.as_ref(), 253)?;
        for source in page.members {
            source_rolling = advance_commit_member_rolling_digest(
                source_rolling,
                source_sequence,
                source.row_digest,
            );
            source_sequence = source_sequence.checked_add(1).ok_or_else(|| {
                RestoreError::SourceClosureMismatch {
                    reason: "raw source member count overflow".to_owned(),
                }
            })?;
            cursor = Some(source.path.clone());

            let Some(materialized) = source.materialized else {
                continue;
            };
            let member_key = restore_member_key(
                context.root_id,
                operation.operation_id,
                materialized_sequence,
            );
            let restored = read_record(
                store,
                context,
                MetadataFamily::RestoreMember,
                &member_key,
                RestoreMemberRecord::decode,
            )?
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: format!("restore member sequence {materialized_sequence} is missing"),
            })?;
            if restored.record.destination_path != materialized.path
                || restored.record.artifact_revision_id != materialized.entry.artifact_revision_id
                || restored.record.path_generation != materialized.entry.generation
                || restored.record.row_digest != materialized.row_digest
            {
                return Err(RestoreError::SourceClosureMismatch {
                    reason: format!(
                        "restore member sequence {materialized_sequence} does not match its source"
                    ),
                });
            }
            materialized_rolling = advance_commit_member_rolling_digest(
                materialized_rolling,
                materialized_sequence,
                materialized.row_digest,
            );
            materialized_sequence = materialized_sequence.checked_add(1).ok_or_else(|| {
                RestoreError::SourceClosureMismatch {
                    reason: "materialized member count overflow".to_owned(),
                }
            })?;
        }
        if page.eof {
            break;
        }
    }
    if source_sequence != operation.source_member_count
        || source_rolling != operation.source_member_rolling_digest
        || materialized_sequence != operation.next_member_sequence
        || materialized_rolling != operation.member_rolling_digest
        || cursor != operation.source_cursor
    {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "rescanned source does not match persisted count, cursor, and digest"
                .to_owned(),
        });
    }
    let member_prefix = restore_member_prefix(context.root_id, operation.operation_id);
    let last_member_key = materialized_sequence
        .checked_sub(1)
        .map(|last| restore_member_key(context.root_id, operation.operation_id, last));
    if !store
        .scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::RestoreMember,
            &member_prefix,
            context.read_version,
            last_member_key.as_deref(),
            1,
        )?
        .is_empty()
    {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "member ledger contains rows past the sealed source count".to_owned(),
        });
    }
    Ok(())
}

fn validate_source_retention(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<(), RestoreError> {
    let RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let frozen_commit = &provenance.source_commit;
    match operation.source {
        RestoreSource::Snapshot {
            snapshot_id,
            read_version,
        } => {
            let snapshot_key = snapshot_ref_key(
                context.root_id,
                operation.source_workspace_incarnation_id,
                snapshot_id,
            );
            let snapshot = read_record(
                store,
                context,
                MetadataFamily::SnapshotRef,
                &snapshot_key,
                SnapshotRefRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotMissing { snapshot_id })?;
            let hold_key = restore_history_hold_key(context.root_id, operation.operation_id);
            let hold = read_record(
                store,
                context,
                MetadataFamily::HistoryHold,
                &hold_key,
                HistoryHoldRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotRetentionMismatch)?;
            let source_commit_id = snapshot
                .record
                .source_commit_id
                .ok_or(RestoreError::SnapshotRetentionMismatch)?;
            if source_commit_id != frozen_commit.commit_id {
                return Err(RestoreError::SnapshotRetentionMismatch);
            }
            let commit = load_commit(store, context, source_commit_id)?;
            let snapshot_consumer = read_record(
                store,
                context,
                MetadataFamily::CommitConsumer,
                &snapshot_commit_consumer_key(context.root_id, source_commit_id, snapshot_id),
                CommitConsumerRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotRetentionMismatch)?;
            if snapshot.record.state != SnapshotState::Active
                || snapshot.record.consumer_count == 0
                || snapshot.record.read_version != read_version
                || commit.record.state != CommitState::Sealed
                || commit.record.source_workspace_incarnation_id
                    != operation.source_workspace_incarnation_id
                || commit.record.consumer_count == 0
                || snapshot_consumer.record.consumer_epoch_at_add > commit.record.consumer_epoch
                || !source_commit_matches_seal(&commit.record, frozen_commit)
                || hold.record.read_version != read_version
                || hold.record.source_snapshot_id != Some(snapshot_id)
                || hold.record.state != HistoryHoldState::Active
            {
                return Err(RestoreError::SnapshotRetentionMismatch);
            }
        }
        RestoreSource::Commit { commit_id } => {
            let commit = load_commit(store, context, commit_id)?;
            let consumer_key =
                lease_commit_consumer_key(context.root_id, commit_id, operation.operation_id);
            let consumer = read_record(
                store,
                context,
                MetadataFamily::CommitConsumer,
                &consumer_key,
                CommitConsumerRecord::decode,
            )?
            .ok_or(RestoreError::CommitRetentionMismatch)?;
            if commit.record.state != CommitState::Sealed
                || commit.record.source_workspace_incarnation_id
                    != operation.source_workspace_incarnation_id
                || commit.record.consumer_count == 0
                || commit_id != frozen_commit.commit_id
                || consumer.record.consumer_epoch_at_add > commit.record.consumer_epoch
                || !source_commit_matches_seal(&commit.record, frozen_commit)
            {
                return Err(RestoreError::CommitRetentionMismatch);
            }
        }
    }
    Ok(())
}

fn source_commit_matches_seal(commit: &CommitRecord, seal: &RestoreSourceCommitSeal) -> bool {
    commit.content_digest_uri == seal.content_digest_uri
        && commit.manifest_digest_uri == seal.manifest_digest_uri
        && commit.tree_manifest_revision_id == seal.tree_manifest_revision_id
        && commit.member_count == seal.member_count
        && commit.member_digest == seal.member_digest
        && commit.unique_revision_count == seal.unique_revision_count
        && commit.revision_digest == seal.revision_digest
        && commit.parent_digest == seal.parent_digest
}

fn path_entry_from_commit_member(member: &CommitMemberRecord) -> PathEntry {
    PathEntry {
        generation: member.path_generation,
        artifact_revision_id: member.artifact_revision_id,
        body_digest_uri: member.body_digest_uri.clone(),
        manifest_digest_uri: member.manifest_digest_uri.clone(),
        logical_size: member.logical_size,
        dependency_count: member.dependency_count,
        dependency_depth: member.dependency_depth,
        content_type: member.content_type.clone(),
        producer: member.producer.clone(),
        manifest_id: member.manifest_id.clone(),
        typed_index_projection: member.typed_projection.clone(),
    }
}

fn commit_member_key_for_marker(
    root: nokv_types::RootId,
    commit: CommitId,
    path: &nokv_types::NormalizedRelativePath,
) -> Vec<u8> {
    super::codec::commit_member_key(root, commit, path)
}

#[derive(Clone, Debug)]
struct PathChange {
    path: nokv_types::NormalizedRelativePath,
    before: Option<Loaded<PathEntry>>,
    after: Option<PathEntry>,
}

fn secondary_index_rows(
    root_id: nokv_types::RootId,
    workspace: WorkspaceIncarnationId,
    path: &nokv_types::NormalizedRelativePath,
    entry: &PathEntry,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, RestoreError> {
    let projection = TypedProjection::decode(&entry.typed_index_projection)?;
    let payload = SecondaryIndexRecord {
        path_generation: entry.generation,
        compact_projection: projection.clone(),
    }
    .encode()?;
    Ok(projection
        .fields()
        .iter()
        .map(|(field, scalar)| {
            (
                secondary_index_key(root_id, field, scalar, workspace, path),
                payload.clone(),
            )
        })
        .collect())
}

fn apply_secondary_index_change(
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    change: &PathChange,
    plan: &mut CommandPlan,
) -> Result<(), RestoreError> {
    let mut before = change
        .before
        .as_ref()
        .map(|loaded| {
            secondary_index_rows(context.root_id, workspace, &change.path, &loaded.record)
        })
        .transpose()?
        .unwrap_or_default();
    let mut after = change
        .after
        .as_ref()
        .map(|entry| secondary_index_rows(context.root_id, workspace, &change.path, entry))
        .transpose()?
        .unwrap_or_default();
    let keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        match (before.remove(&key), after.remove(&key)) {
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
    Ok(())
}

#[derive(Clone, Debug)]
enum ReferenceChange {
    Add {
        key: Vec<u8>,
        revision: ArtifactRevisionId,
    },
    Remove {
        key: Vec<u8>,
        revision: ArtifactRevisionId,
        payload: Vec<u8>,
        record: RevisionRefRecord,
    },
}

#[derive(Clone, Copy, Debug, Default)]
struct ReferenceDelta {
    count_delta: i64,
    touched: bool,
}

fn apply_path_changes(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    changes: Vec<PathChange>,
    plan: &mut CommandPlan,
) -> Result<(), RestoreError> {
    let mut unique_paths = BTreeSet::new();
    let mut reference_changes = Vec::new();
    let mut deltas = BTreeMap::<ArtifactRevisionId, ReferenceDelta>::new();
    for change in &changes {
        if !unique_paths.insert(change.path.clone()) {
            return Err(RestoreError::SourceClosureMismatch {
                reason: "path update set contains a duplicate path".to_owned(),
            });
        }
        apply_secondary_index_change(context, workspace, change, plan)?;
        let before_revision = change
            .before
            .as_ref()
            .map(|loaded| loaded.record.artifact_revision_id);
        let after_revision = change
            .after
            .as_ref()
            .map(|entry| entry.artifact_revision_id);
        if before_revision == after_revision {
            continue;
        }
        if let Some(revision) = before_revision {
            let key = path_revision_ref_key(context.root_id, workspace, &change.path, revision);
            let loaded = read_record(
                store,
                context,
                MetadataFamily::RevisionRef,
                &key,
                RevisionRefRecord::decode,
            )?
            .ok_or(RestoreError::RevisionReferenceMissing { revision })?;
            reference_changes.push(ReferenceChange::Remove {
                key,
                revision,
                payload: loaded.payload,
                record: loaded.record,
            });
            let delta = deltas.entry(revision).or_default();
            delta.count_delta = delta
                .count_delta
                .checked_sub(1)
                .ok_or(RestoreError::ReferenceCountUnderflow { revision })?;
            delta.touched = true;
        }
        if let Some(revision) = after_revision {
            reference_changes.push(ReferenceChange::Add {
                key: path_revision_ref_key(context.root_id, workspace, &change.path, revision),
                revision,
            });
            let delta = deltas.entry(revision).or_default();
            delta.count_delta = delta
                .count_delta
                .checked_add(1)
                .ok_or(RestoreError::ReferenceCountOverflow { revision })?;
            delta.touched = true;
        }
    }

    let expected_commit_version = next_commit_version(context.read_version)?;
    let mut revision_updates = BTreeMap::<
        ArtifactRevisionId,
        (Loaded<ArtifactRevisionRecord>, ArtifactRevisionRecord),
    >::new();
    for (revision, delta) in deltas {
        if !delta.touched {
            continue;
        }
        let loaded = load_available_revision(store, context, revision)?;
        let mut next = loaded.record.clone();
        next.reference_epoch = increment_reference_epoch(next.reference_epoch, revision)?;
        next.strong_reference_count =
            apply_reference_delta(next.strong_reference_count, delta.count_delta, revision)?;
        next.last_zero_ref_version =
            (next.strong_reference_count == 0).then_some(expected_commit_version);
        revision_updates.insert(revision, (loaded, next));
    }

    for change in changes {
        let key = path_current_key(context.root_id, workspace, &change.path);
        match (change.before, change.after) {
            (None, None) => {}
            (None, Some(after)) => {
                plan.put_absent(MetadataFamily::PathCurrent, key, after.encode()?)?;
            }
            (Some(before), None) => {
                plan.delete(MetadataFamily::PathCurrent, key, before.payload)?;
            }
            (Some(before), Some(after)) => {
                let after_payload = after.encode()?;
                if before.payload == after_payload {
                    plan.assert_value(MetadataFamily::PathCurrent, key, Some(before.payload))?;
                } else {
                    plan.replace(
                        MetadataFamily::PathCurrent,
                        key,
                        before.payload,
                        after_payload,
                    )?;
                }
            }
        }
    }
    for reference in reference_changes {
        match reference {
            ReferenceChange::Add { key, revision } => {
                let epoch = revision_updates
                    .get(&revision)
                    .expect("every added reference updates its owner")
                    .1
                    .reference_epoch;
                plan.put_absent(
                    MetadataFamily::RevisionRef,
                    key,
                    RevisionRefRecord {
                        reference_epoch_at_add: epoch,
                    }
                    .encode()?,
                )?;
            }
            ReferenceChange::Remove {
                key,
                revision,
                payload,
                record,
            } => {
                let owner = revision_updates
                    .get(&revision)
                    .expect("every removed reference updates its owner");
                if record.reference_epoch_at_add > owner.0.record.reference_epoch {
                    return Err(RestoreError::ReferenceEpochAhead { revision });
                }
                plan.delete(MetadataFamily::RevisionRef, key, payload)?;
            }
        }
    }
    for (revision, (loaded, next)) in revision_updates {
        plan.replace(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(context.root_id, revision),
            loaded.payload,
            next.encode()?,
        )?;
        if next.strong_reference_count == 0 {
            plan.put_absent(
                MetadataFamily::GcCandidate,
                gc_candidate_key(context.root_id, revision, next.reference_epoch),
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
    Ok(())
}

fn apply_reference_delta(
    current: u64,
    delta: i64,
    revision: ArtifactRevisionId,
) -> Result<u64, RestoreError> {
    if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or(RestoreError::ReferenceCountOverflow { revision })
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or(RestoreError::ReferenceCountUnderflow { revision })
    }
}

fn handoff_source_retention_to_child(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    destination_commit_id: CommitId,
    plan: &mut CommandPlan,
) -> Result<(), RestoreError> {
    let RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let source_commit_id = provenance.source_commit.commit_id;
    let source_key = commit_key(context.root_id, source_commit_id);
    let source = read_record(
        store,
        context,
        MetadataFamily::Commit,
        &source_key,
        CommitRecord::decode,
    )?
    .ok_or(RestoreError::CommitMissing {
        commit_id: source_commit_id,
    })?;
    if source.record.state != CommitState::Sealed
        || !source_commit_matches_seal(&source.record, &provenance.source_commit)
    {
        return Err(RestoreError::CommitRetentionMismatch);
    }
    let child_key =
        child_commit_consumer_key(context.root_id, source_commit_id, destination_commit_id);
    if read_payload(store, context, MetadataFamily::CommitConsumer, &child_key)?.is_some() {
        return Err(RestoreError::CommitRetentionMismatch);
    }

    let next_source = match operation.source {
        RestoreSource::Snapshot { .. } => {
            let next =
                add_commit_consumer(&source.record).map_err(map_commit_consumer_mutation_error)?;
            release_source_retention(store, context, operation, plan)?;
            next
        }
        RestoreSource::Commit { commit_id } => {
            if commit_id != source_commit_id {
                return Err(RestoreError::CommitRetentionMismatch);
            }
            let lease_key =
                lease_commit_consumer_key(context.root_id, commit_id, operation.operation_id);
            let lease = read_record(
                store,
                context,
                MetadataFamily::CommitConsumer,
                &lease_key,
                CommitConsumerRecord::decode,
            )?
            .ok_or(RestoreError::CommitRetentionMismatch)?;
            if lease.record.consumer_epoch_at_add > source.record.consumer_epoch {
                return Err(RestoreError::CommitRetentionMismatch);
            }
            let removed =
                remove_commit_consumer(&source.record, next_commit_version(context.read_version)?)
                    .map_err(map_commit_consumer_mutation_error)?;
            let next = add_commit_consumer(&removed).map_err(map_commit_consumer_mutation_error)?;
            plan.delete(MetadataFamily::CommitConsumer, lease_key, lease.payload)?;
            next
        }
    };
    plan.replace(
        MetadataFamily::Commit,
        source_key,
        source.payload,
        next_source.encode()?,
    )?;
    plan.put_absent(
        MetadataFamily::CommitConsumer,
        child_key,
        CommitConsumerRecord {
            consumer_epoch_at_add: next_source.consumer_epoch,
        }
        .encode(),
    )?;
    Ok(())
}

fn map_commit_consumer_mutation_error(error: CommitConsumerMutationError) -> RestoreError {
    match error {
        CommitConsumerMutationError::CountOverflow => RestoreError::ConsumerCountOverflow,
        CommitConsumerMutationError::CountUnderflow => RestoreError::ConsumerCountUnderflow,
        CommitConsumerMutationError::EpochOverflow => RestoreError::ConsumerEpochOverflow,
    }
}

fn commit_member_tree_digest_uri(digest: [u8; SHA256_BYTES]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut uri = String::with_capacity("sha256:".len() + SHA256_BYTES * 2);
    uri.push_str("sha256:");
    for byte in digest {
        uri.push(char::from(HEX[usize::from(byte >> 4)]));
        uri.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    uri
}

fn release_source_retention(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    plan: &mut CommandPlan,
) -> Result<(), RestoreError> {
    match operation.source {
        RestoreSource::Snapshot {
            snapshot_id,
            read_version,
        } => {
            let key = snapshot_ref_key(
                context.root_id,
                operation.source_workspace_incarnation_id,
                snapshot_id,
            );
            let loaded = read_record(
                store,
                context,
                MetadataFamily::SnapshotRef,
                &key,
                SnapshotRefRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotMissing { snapshot_id })?;
            let hold_key = restore_history_hold_key(context.root_id, operation.operation_id);
            let hold = read_record(
                store,
                context,
                MetadataFamily::HistoryHold,
                &hold_key,
                HistoryHoldRecord::decode,
            )?
            .ok_or(RestoreError::SnapshotRetentionMismatch)?;
            if loaded.record.state != SnapshotState::Active
                || loaded.record.read_version != read_version
                || loaded.record.consumer_count == 0
                || hold.record.read_version != read_version
                || hold.record.source_snapshot_id != Some(snapshot_id)
                || hold.record.state != HistoryHoldState::Active
            {
                return Err(RestoreError::SnapshotRetentionMismatch);
            }
            let mut next = loaded.record.clone();
            next.consumer_count = next
                .consumer_count
                .checked_sub(1)
                .ok_or(RestoreError::ConsumerCountUnderflow)?;
            next.consumer_epoch = increment_consumer_epoch(next.consumer_epoch)?;
            plan.replace(
                MetadataFamily::SnapshotRef,
                key,
                loaded.payload,
                next.encode()?,
            )?;
            plan.delete(MetadataFamily::HistoryHold, hold_key, hold.payload)?;
        }
        RestoreSource::Commit { commit_id } => {
            let key = commit_key(context.root_id, commit_id);
            let loaded = read_record(
                store,
                context,
                MetadataFamily::Commit,
                &key,
                CommitRecord::decode,
            )?
            .ok_or(RestoreError::CommitMissing { commit_id })?;
            let consumer_key =
                lease_commit_consumer_key(context.root_id, commit_id, operation.operation_id);
            let consumer = read_record(
                store,
                context,
                MetadataFamily::CommitConsumer,
                &consumer_key,
                CommitConsumerRecord::decode,
            )?
            .ok_or(RestoreError::CommitRetentionMismatch)?;
            if loaded.record.state != CommitState::Sealed
                || loaded.record.consumer_count == 0
                || consumer.record.consumer_epoch_at_add > loaded.record.consumer_epoch
            {
                return Err(RestoreError::CommitRetentionMismatch);
            }
            let mut next = loaded.record.clone();
            next.consumer_count = next
                .consumer_count
                .checked_sub(1)
                .ok_or(RestoreError::ConsumerCountUnderflow)?;
            next.consumer_epoch = increment_consumer_epoch(next.consumer_epoch)?;
            next.last_zero_consumer_version =
                (next.consumer_count == 0).then_some(next_commit_version(context.read_version)?);
            plan.replace(MetadataFamily::Commit, key, loaded.payload, next.encode()?)?;
            plan.delete(
                MetadataFamily::CommitConsumer,
                consumer_key,
                consumer.payload,
            )?;
        }
    }
    Ok(())
}

fn load_visible_workspace(
    store: &MetaShard,
    context: RootWriteContext,
    workbench_id: &WorkbenchId,
) -> Result<Loaded<WorkspaceRecord>, RestoreError> {
    let key = workspace_current_key(context.root_id, workbench_id);
    let loaded = read_record(
        store,
        context,
        MetadataFamily::WorkspaceCurrent,
        &key,
        WorkspaceRecord::decode,
    )?
    .ok_or_else(|| RestoreError::SourceWorkspaceMissing {
        workbench_id: workbench_id.clone(),
    })?;
    if loaded.record.state != WorkspaceState::Visible || loaded.record.owning_operation_id.is_some()
    {
        return Err(RestoreError::SourceWorkspaceMissing {
            workbench_id: workbench_id.clone(),
        });
    }
    Ok(loaded)
}

fn load_staging_workspace(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<Loaded<WorkspaceRecord>, RestoreError> {
    let key = workspace_current_key(context.root_id, &operation.destination_workbench_id);
    let loaded = read_record(
        store,
        context,
        MetadataFamily::WorkspaceCurrent,
        &key,
        WorkspaceRecord::decode,
    )?
    .ok_or(RestoreError::DestinationMarkerMismatch)?;
    if loaded.record.state != WorkspaceState::Staging
        || loaded.record.incarnation_id != operation.destination_workspace_incarnation_id
        || loaded.record.owning_operation_id != Some(operation.operation_id)
    {
        return Err(RestoreError::DestinationMarkerMismatch);
    }
    Ok(loaded)
}

fn predicate_staging_workspace(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    plan: &mut CommandPlan,
) -> Result<(), RestoreError> {
    let loaded = load_staging_workspace(store, context, operation)?;
    plan.assert_value(
        MetadataFamily::WorkspaceCurrent,
        workspace_current_key(context.root_id, &operation.destination_workbench_id),
        Some(loaded.payload),
    )
}

fn load_operation(
    store: &MetaShard,
    context: RootWriteContext,
    operation_id: OperationId,
) -> Result<Loaded<RestoreOperationRecord>, RestoreError> {
    read_record(
        store,
        context,
        MetadataFamily::Operation,
        &operation_key(context.root_id, OperationKind::Restore, operation_id),
        RestoreOperationRecord::decode,
    )?
    .ok_or(RestoreError::OperationMissing { operation_id })
}

fn load_path(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    path: &nokv_types::NormalizedRelativePath,
) -> Result<Option<Loaded<PathEntry>>, RestoreError> {
    read_record(
        store,
        context,
        MetadataFamily::PathCurrent,
        &path_current_key(context.root_id, workspace, path),
        PathEntry::decode,
    )
}

fn load_available_revision(
    store: &MetaShard,
    context: RootWriteContext,
    revision: ArtifactRevisionId,
) -> Result<Loaded<ArtifactRevisionRecord>, RestoreError> {
    let loaded = read_record(
        store,
        context,
        MetadataFamily::ArtifactRevision,
        &artifact_revision_key(context.root_id, revision),
        ArtifactRevisionRecord::decode,
    )?
    .ok_or(RestoreError::RevisionMissing { revision })?;
    if loaded.record.state != RevisionState::Available {
        return Err(RestoreError::RevisionUnavailable {
            revision,
            state: loaded.record.state,
        });
    }
    Ok(loaded)
}

fn load_commit(
    store: &MetaShard,
    context: RootWriteContext,
    commit_id: CommitId,
) -> Result<Loaded<CommitRecord>, RestoreError> {
    let loaded = read_record(
        store,
        context,
        MetadataFamily::Commit,
        &commit_key(context.root_id, commit_id),
        CommitRecord::decode,
    )?
    .ok_or(RestoreError::CommitMissing { commit_id })?;
    if loaded.record.state != CommitState::Sealed {
        return Err(RestoreError::CommitNotSealed {
            commit_id,
            state: loaded.record.state,
        });
    }
    Ok(loaded)
}

fn read_record<T, E>(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<Option<Loaded<T>>, RestoreError>
where
    RestoreError: From<E>,
{
    read_payload(store, context, family, key)?
        .map(|payload| {
            let record = decode(&payload).map_err(RestoreError::from)?;
            Ok(Loaded { payload, record })
        })
        .transpose()
}

fn read_payload(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, RestoreError> {
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

fn require_phase(
    operation: &RestoreOperationRecord,
    expected: RestorePhase,
    expected_name: &'static str,
) -> Result<(), RestoreError> {
    if operation.phase == expected {
        Ok(())
    } else {
        Err(RestoreError::InvalidPhase {
            expected: expected_name,
            actual: operation.phase,
        })
    }
}

fn plan_restore_commit_member_batch(
    store: &MetaShard,
    context: RootWriteContext,
    loaded: &Loaded<RestoreOperationRecord>,
    destination_commit_id: CommitId,
    rows: &[MetadataScanItem],
    source_eof: bool,
    input_digest: [u8; SHA256_BYTES],
) -> Result<PlannedRestoreCommand, RestoreError> {
    let mut next = loaded.record.clone();
    let mut plan = CommandPlan::default();
    let mut batch_revisions = BTreeSet::new();
    plan.assert_value(
        MetadataFamily::Commit,
        commit_key(context.root_id, destination_commit_id),
        None,
    )?;

    for row in rows {
        let path = decode_path_current_key(
            context.root_id,
            next.destination_workspace_incarnation_id,
            &row.key,
        )
        .ok_or(RestoreError::CorruptKey {
            family: "PathCurrent(destination)",
        })?;
        let entry = PathEntry::decode(&row.value)?;
        TypedProjection::decode(&entry.typed_index_projection)?;
        let member = CommitMemberRecord {
            artifact_revision_id: entry.artifact_revision_id,
            path_generation: entry.generation,
            body_digest_uri: entry.body_digest_uri.clone(),
            manifest_digest_uri: entry.manifest_digest_uri.clone(),
            logical_size: entry.logical_size,
            dependency_count: entry.dependency_count,
            dependency_depth: entry.dependency_depth,
            content_type: entry.content_type.clone(),
            producer: entry.producer.clone(),
            manifest_id: entry.manifest_id.clone(),
            typed_projection: entry.typed_index_projection.clone(),
        };
        let step = {
            let RestoreCommitProvenance::V5(provenance) = &next.commit_provenance else {
                return Err(RestoreError::CommitRetentionMismatch);
            };
            plan_commit_member(
                provenance.closure.member_cursor.as_ref(),
                provenance.closure.member_count,
                provenance.closure.member_digest,
                &path,
                &member,
            )
            .map_err(|error| RestoreError::SourceClosureMismatch {
                reason: error.to_string(),
            })?
        };
        plan.assert_value(
            MetadataFamily::PathCurrent,
            row.key.clone(),
            Some(row.value.clone()),
        )?;
        plan.put_absent(
            MetadataFamily::CommitMember,
            commit_member_key(context.root_id, destination_commit_id, &path),
            member.encode()?,
        )?;
        {
            let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
                unreachable!("phase validation excludes legacy operations");
            };
            provenance.closure.member_cursor = Some(step.cursor);
            provenance.closure.member_count = step.count;
            provenance.closure.member_digest = step.digest;
        }

        if !batch_revisions.insert(entry.artifact_revision_id) {
            continue;
        }
        let ref_key = commit_revision_ref_key(
            context.root_id,
            destination_commit_id,
            entry.artifact_revision_id,
        );
        if let Some(reference) = read_record(
            store,
            context,
            MetadataFamily::RevisionRef,
            &ref_key,
            RevisionRefRecord::decode,
        )? {
            let revision = load_available_revision(store, context, entry.artifact_revision_id)?;
            if reference.record.reference_epoch_at_add > revision.record.reference_epoch {
                return Err(RestoreError::ReferenceEpochAhead {
                    revision: entry.artifact_revision_id,
                });
            }
            plan.assert_value(
                MetadataFamily::RevisionRef,
                ref_key,
                Some(reference.payload),
            )?;
            plan.assert_value(
                MetadataFamily::ArtifactRevision,
                artifact_revision_key(context.root_id, entry.artifact_revision_id),
                Some(revision.payload),
            )?;
            continue;
        }
        let revision = load_available_revision(store, context, entry.artifact_revision_id)?;
        if revision.record.logical_size != entry.logical_size
            || revision.record.body_digest_uri != entry.body_digest_uri
            || revision.record.manifest_digest_uri != entry.manifest_digest_uri
            || revision.record.dependency_count != entry.dependency_count
            || revision.record.dependency_depth != entry.dependency_depth
            || revision.record.content_type != entry.content_type
        {
            return Err(RestoreError::ManifestRevisionMismatch);
        }
        let next_epoch =
            increment_reference_epoch(revision.record.reference_epoch, entry.artifact_revision_id)?;
        let mut next_revision = revision.record.clone();
        next_revision.reference_epoch = next_epoch;
        next_revision.strong_reference_count = next_revision
            .strong_reference_count
            .checked_add(1)
            .ok_or(RestoreError::ReferenceCountOverflow {
                revision: entry.artifact_revision_id,
            })?;
        next_revision.last_zero_ref_version = None;
        plan.replace(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(context.root_id, entry.artifact_revision_id),
            revision.payload,
            next_revision.encode()?,
        )?;
        plan.put_absent(
            MetadataFamily::RevisionRef,
            ref_key,
            RevisionRefRecord {
                reference_epoch_at_add: next_epoch,
            }
            .encode()?,
        )?;
        let RestoreCommitProvenance::V5(provenance) = &mut next.commit_provenance else {
            unreachable!("phase validation excludes legacy operations");
        };
        provenance.closure.revision_ref_count = provenance
            .closure
            .revision_ref_count
            .checked_add(1)
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: "destination revision-ref count overflow".to_owned(),
            })?;
    }

    if source_eof {
        let (run, restore) = verify_destination_manifest_publications(store, context, &next)?;
        run.add_predicates(&mut plan)?;
        restore.add_predicates(&mut plan)?;
        let member_seal = match &next.commit_provenance {
            RestoreCommitProvenance::V5(provenance) => provenance.closure.member_digest,
            RestoreCommitProvenance::MissingLegacyV4 => unreachable!(),
        };
        next = next.apply(
            RestorePhase::DestinationBuilding,
            RestoreTransition::BeginDestinationSealing { member_seal },
        )?;
    } else {
        next.validate()?;
    }
    let next_payload = next.encode()?;
    plan.replace(
        MetadataFamily::Operation,
        operation_key(context.root_id, OperationKind::Restore, next.operation_id),
        loaded.payload.clone(),
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    Ok(PlannedRestoreCommand {
        command: build_restore_command(
            context,
            plan,
            input_digest,
            next_payload,
            u32::try_from(rows.len()).expect("bounded member page fits u32"),
        ),
        operation: next,
    })
}

fn verify_destination_manifest_publications(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<
    (
        VerifiedRestoreManifestPublication,
        VerifiedRestoreManifestPublication,
    ),
    RestoreError,
> {
    let RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let binding = provenance.destination_binding.as_ref().ok_or(
        RestoreError::DestinationBindingMismatch {
            operation_id: operation.operation_id,
        },
    )?;
    let manifests = binding
        .manifests
        .as_ref()
        .ok_or(RestoreError::DestinationBindingMismatch {
            operation_id: operation.operation_id,
        })?;
    let run = load_restore_manifest_publication(
        store,
        context,
        operation,
        RUN_MANIFEST_PATH,
        binding.run_manifest_identity,
    )?;
    let restore = load_restore_manifest_publication(
        store,
        context,
        operation,
        RESTORE_MANIFEST_PATH,
        binding.restore_manifest_identity,
    )?;
    if run.publication != manifests.run_manifest
        || restore.publication != manifests.restore_manifest
    {
        return Err(RestoreError::ManifestBindingMismatch);
    }
    Ok((run, restore))
}

fn verify_destination_commit_scaffolding(
    store: &MetaShard,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<(), RestoreError> {
    let RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
        return Err(RestoreError::CommitRetentionMismatch);
    };
    let binding = provenance.destination_binding.as_ref().ok_or(
        RestoreError::DestinationBindingMismatch {
            operation_id: operation.operation_id,
        },
    )?;
    if read_payload(
        store,
        context,
        MetadataFamily::Commit,
        &commit_key(context.root_id, binding.destination_commit_id),
    )?
    .is_some()
    {
        return Err(RestoreError::DestinationBindingMismatch {
            operation_id: operation.operation_id,
        });
    }
    let member_marker = provenance
        .closure
        .member_cursor
        .as_ref()
        .map(|path| commit_member_key(context.root_id, binding.destination_commit_id, path));
    if !store
        .scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::CommitMember,
            &commit_member_prefix(context.root_id, binding.destination_commit_id),
            context.read_version,
            member_marker.as_deref(),
            1,
        )?
        .is_empty()
    {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "destination commit member closure contains rows past its seal".to_owned(),
        });
    }
    let path_marker = provenance.closure.member_cursor.as_ref().map(|path| {
        path_current_key(
            context.root_id,
            operation.destination_workspace_incarnation_id,
            path,
        )
    });
    if !store
        .scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::PathCurrent,
            &path_child_prefix(
                context.root_id,
                operation.destination_workspace_incarnation_id,
                None,
            ),
            context.read_version,
            path_marker.as_deref(),
            1,
        )?
        .is_empty()
    {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "hidden destination contains a path past the commit member seal".to_owned(),
        });
    }
    verify_destination_manifest_publications(store, context, operation)?;
    Ok(())
}

fn decode_restore_commit_revision_ref_key(
    prefix: &[u8],
    key: &[u8],
) -> Result<ArtifactRevisionId, RestoreError> {
    if !key.starts_with(prefix) || key.len() != prefix.len() + FIXED_ID_BYTES {
        return Err(RestoreError::CorruptKey {
            family: "RevisionRef(Commit)",
        });
    }
    Ok(ArtifactRevisionId::from_bytes(
        key[prefix.len()..]
            .try_into()
            .expect("validated revision-id suffix width"),
    ))
}

fn load_restore_manifest_publication(
    store: &MetaShard,
    context: RootWriteContext,
    restore: &RestoreOperationRecord,
    path: &str,
    expected: RestoreManifestIdentity,
) -> Result<VerifiedRestoreManifestPublication, RestoreError> {
    let path = NormalizedRelativePath::new(path).expect("reserved manifest path is normalized");
    let path_key = path_current_key(
        context.root_id,
        restore.destination_workspace_incarnation_id,
        &path,
    );
    let loaded_path = read_record(
        store,
        context,
        MetadataFamily::PathCurrent,
        &path_key,
        PathEntry::decode,
    )?
    .ok_or(RestoreError::RestoreManifestMissing)?;
    validate_destination_manifest_entry(&loaded_path.record)?;
    if loaded_path.record.artifact_revision_id != expected.artifact_revision_id {
        return Err(RestoreError::ManifestBindingMismatch);
    }

    let publish_key = operation_key(
        context.root_id,
        OperationKind::Publish,
        expected.publication_operation_id,
    );
    let publish = read_record(
        store,
        context,
        MetadataFamily::Operation,
        &publish_key,
        PublishOperationRecord::decode,
    )?
    .ok_or(RestoreError::ManifestBindingMismatch)?;
    let result = publish
        .record
        .result
        .as_ref()
        .ok_or(RestoreError::ManifestBindingMismatch)?;
    if publish.record.phase != PublishPhase::Published
        || publish.record.authority
            != (PublishAuthority::RestoreStaging {
                restore_operation_id: restore.operation_id,
            })
        || publish.record.workbench_id != restore.destination_workbench_id
        || publish.record.workspace_incarnation_id != restore.destination_workspace_incarnation_id
        || publish.record.path != path
        || publish.record.artifact_revision_id != expected.artifact_revision_id
        || publish.record.claim != PublishClaim::CreateOnly
        || result.path_generation != loaded_path.record.generation
        || result.logical_size != loaded_path.record.logical_size
        || result.body_digest_uri != loaded_path.record.body_digest_uri
    {
        return Err(RestoreError::ManifestBindingMismatch);
    }

    let revision_key = artifact_revision_key(context.root_id, expected.artifact_revision_id);
    let revision = validate_manifest_revision(store, context, &loaded_path.record)?;
    Ok(VerifiedRestoreManifestPublication {
        publication: RestoreManifestPublication {
            publication_operation_id: expected.publication_operation_id,
            workspace_incarnation_id: restore.destination_workspace_incarnation_id,
            artifact_revision_id: expected.artifact_revision_id,
            body_digest_uri: loaded_path.record.body_digest_uri.clone(),
            manifest_digest_uri: loaded_path.record.manifest_digest_uri.clone(),
            logical_size: loaded_path.record.logical_size,
            content_type: loaded_path.record.content_type.clone(),
        },
        path_key,
        path: loaded_path,
        publish_key,
        publish,
        revision_key,
        revision,
    })
}

fn validate_destination_manifest_entry(manifest: &PathEntry) -> Result<(), RestoreError> {
    manifest.encode()?;
    // Commit's virtual run-manifest member historically seals an empty byte
    // string for the semantically empty projection.  This is immutable source
    // data, so read it through the durable-record decoder instead of requiring
    // a newly published PathEntry's canonical projection encoding.
    let projection = TypedProjection::decode_stored(&manifest.typed_index_projection)?;
    if manifest.logical_size == 0
        || manifest.content_type != "application/json"
        || manifest.dependency_count != 0
        || manifest.dependency_depth != 0
        || manifest.manifest_id.is_some()
        || !projection.fields().is_empty()
    {
        return Err(RestoreError::ManifestBindingMismatch);
    }
    Ok(())
}

fn validate_initialization(
    initialization: &RestoreInitialization,
    operation: &RestoreOperationRecord,
) -> Result<(), RestoreError> {
    let manifest = &initialization.restore_manifest;
    manifest.encode()?;
    TypedProjection::decode(&manifest.typed_index_projection)?;
    if manifest.logical_size != operation.restore_manifest.logical_size
        || manifest.body_digest_uri != operation.restore_manifest.body_digest_uri
        || manifest.content_type != operation.restore_manifest.content_type
        || !TypedProjection::decode(&manifest.typed_index_projection)?
            .fields()
            .is_empty()
    {
        return Err(RestoreError::ManifestBindingMismatch);
    }
    Ok(())
}

fn validate_manifest_revision(
    store: &MetaShard,
    context: RootWriteContext,
    manifest: &PathEntry,
) -> Result<Loaded<ArtifactRevisionRecord>, RestoreError> {
    let revision = load_available_revision(store, context, manifest.artifact_revision_id)?;
    if revision.record.logical_size != manifest.logical_size
        || revision.record.body_digest_uri != manifest.body_digest_uri
        || revision.record.manifest_digest_uri != manifest.manifest_digest_uri
        || revision.record.content_type != manifest.content_type
        || revision.record.dependency_count != manifest.dependency_count
        || revision.record.dependency_depth != manifest.dependency_depth
    {
        return Err(RestoreError::ManifestRevisionMismatch);
    }
    Ok(revision)
}

fn restore_manifest_path() -> nokv_types::NormalizedRelativePath {
    nokv_types::NormalizedRelativePath::new(RESTORE_MANIFEST_PATH)
        .expect("constant restore-manifest path is normalized")
}

fn increment_reference_epoch(
    epoch: ReferenceEpoch,
    revision: ArtifactRevisionId,
) -> Result<ReferenceEpoch, RestoreError> {
    epoch
        .get()
        .checked_add(1)
        .map(ReferenceEpoch::new)
        .ok_or(RestoreError::ReferenceEpochOverflow { revision })
}

fn increment_consumer_epoch(epoch: ConsumerEpoch) -> Result<ConsumerEpoch, RestoreError> {
    epoch
        .get()
        .checked_add(1)
        .map(ConsumerEpoch::new)
        .ok_or(RestoreError::ConsumerEpochOverflow)
}

fn next_commit_version(read_version: ReadVersion) -> Result<CommitVersion, RestoreError> {
    read_version
        .get()
        .checked_add(1)
        .and_then(|value| CommitVersion::new(value).ok())
        .ok_or(RestoreError::CommitVersionOverflow)
}

fn commit_version_from_read(read_version: ReadVersion) -> Result<CommitVersion, RestoreError> {
    CommitVersion::new(read_version.get()).map_err(|_| RestoreError::CommitVersionOverflow)
}

#[allow(clippy::too_many_arguments)]
fn execute_plan(
    store: &MetaShard,
    context: RootWriteContext,
    plan: CommandPlan,
    input_digest: [u8; SHA256_BYTES],
    operation_payload: Vec<u8>,
    lease_guard: Option<(SnapshotId, u64)>,
    operation_id: OperationId,
    affected_members: u32,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let command = build_restore_command(
        context,
        plan,
        input_digest,
        operation_payload,
        affected_members,
    );
    execute_restore_command(store, &command, input_digest, lease_guard, operation_id)
}

fn build_restore_command(
    context: RootWriteContext,
    plan: CommandPlan,
    input_digest: [u8; SHA256_BYTES],
    operation_payload: Vec<u8>,
    affected_members: u32,
) -> MetadataCommand {
    let deterministic_result =
        encode_restore_outcome(input_digest, &operation_payload, affected_members);
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
        predicates: plan.predicates,
        mutations: plan.mutations,
        history_projection: plan.history,
        event_projection: plan.events,
        deterministic_result,
    }
    .seal()
}

fn execute_restore_command(
    store: &MetaShard,
    command: &MetadataCommand,
    input_digest: [u8; SHA256_BYTES],
    lease_guard: Option<(SnapshotId, u64)>,
    operation_id: OperationId,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let executed = match lease_guard {
        Some((_, deadline)) => store.execute_before_lease_deadline(command, deadline),
        None => store.execute(command),
    };
    let result = match executed {
        Ok(result) => result,
        Err(MetaError::LeaseDeadlineReached {
            lease_clock_ms,
            requested_deadline_ms,
        }) => {
            return Err(RestoreError::SnapshotLeaseExpired {
                snapshot_id: lease_guard
                    .map(|(snapshot_id, _)| snapshot_id)
                    .expect("lease deadline errors require a snapshot lease guard"),
                lease_clock_ms,
                lease_deadline_ms: requested_deadline_ms,
            });
        }
        Err(MetaError::PredicateFailed | MetaError::WriteConflict) => {
            return Err(RestoreError::ConcurrentMutation);
        }
        Err(MetaError::RequestIdReused) => {
            return Err(RestoreError::RequestInputMismatch);
        }
        Err(source) => return Err(RestoreError::Meta(source)),
    };
    let (operation, decoded_affected) = decode_restore_outcome(
        &result.deterministic_result,
        input_digest,
        Some(operation_id),
    )?;
    Ok(RestoreCommandOutcome {
        operation,
        commit_version: result.commit_version,
        replayed: result.replayed,
        affected_members: decoded_affected,
    })
}

fn replay_outcome(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    operation_id: Option<OperationId>,
) -> Result<Option<RestoreCommandOutcome>, RestoreError> {
    let Some(replay) = store.lookup_request_result(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        context.request_id,
    )?
    else {
        return Ok(None);
    };
    let (operation, affected_members) =
        decode_restore_outcome(&replay.deterministic_result, input_digest, operation_id)?;
    Ok(Some(RestoreCommandOutcome {
        operation,
        commit_version: replay.commit_version,
        replayed: true,
        affected_members,
    }))
}

fn encode_restore_outcome(
    input_digest: [u8; SHA256_BYTES],
    operation_payload: &[u8],
    affected_members: u32,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + SHA256_BYTES + 4 + operation_payload.len() + 4);
    encoded.push(RESTORE_OUTCOME_FORMAT);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(
        &u32::try_from(operation_payload.len())
            .expect("restore operation payload fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(operation_payload);
    encoded.extend_from_slice(&affected_members.to_be_bytes());
    encoded
}

fn decode_restore_outcome(
    encoded: &[u8],
    expected_input_digest: [u8; SHA256_BYTES],
    expected_operation_id: Option<OperationId>,
) -> Result<(RestoreOperationRecord, u32), RestoreError> {
    if encoded.len() < 1 + SHA256_BYTES + 4 + 4 {
        return Err(RestoreError::DeterministicResultMismatch {
            reason: "restore outcome is truncated".to_owned(),
        });
    }
    if encoded[0] != RESTORE_OUTCOME_FORMAT {
        return Err(RestoreError::DeterministicResultMismatch {
            reason: "restore outcome format is unknown".to_owned(),
        });
    }
    if encoded[1..1 + SHA256_BYTES] != expected_input_digest {
        return Err(RestoreError::RequestInputMismatch);
    }
    let length_offset = 1 + SHA256_BYTES;
    let payload_length = u32::from_be_bytes(
        encoded[length_offset..length_offset + 4]
            .try_into()
            .expect("fixed outcome length bytes"),
    ) as usize;
    let payload_start = length_offset + 4;
    let payload_end = payload_start.checked_add(payload_length).ok_or_else(|| {
        RestoreError::DeterministicResultMismatch {
            reason: "restore outcome payload length overflow".to_owned(),
        }
    })?;
    if encoded.len() != payload_end + 4 {
        return Err(RestoreError::DeterministicResultMismatch {
            reason: "restore outcome length does not match its envelope".to_owned(),
        });
    }
    let operation = RestoreOperationRecord::decode(&encoded[payload_start..payload_end])?;
    if expected_operation_id.is_some_and(|expected| expected != operation.operation_id) {
        return Err(RestoreError::DeterministicResultMismatch {
            reason: "restore outcome belongs to another operation".to_owned(),
        });
    }
    let affected_members = u32::from_be_bytes(
        encoded[payload_end..]
            .try_into()
            .expect("fixed affected-members bytes"),
    );
    Ok((operation, affected_members))
}

fn restore_selector_identity_digest(
    root_id: nokv_types::RootId,
    source_workbench_id: &WorkbenchId,
    source_workspace_incarnation_id: WorkspaceIncarnationId,
    source: RestoreSourceSelector,
    destination_workbench_id: &WorkbenchId,
    destination_workspace_incarnation_id: WorkspaceIncarnationId,
) -> Result<[u8; SHA256_BYTES], RestoreError> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.operation.v2\0");
    hasher.update(root_id.as_bytes());
    hash_u32_bytes(&mut hasher, source_workbench_id.as_bytes())?;
    hasher.update(source_workspace_incarnation_id.as_bytes());
    hasher.update([match source {
        RestoreSourceSelector::Snapshot(_) => 1,
        RestoreSourceSelector::Commit(_) => 2,
    }]);
    match source {
        RestoreSourceSelector::Snapshot(snapshot_id) => {
            hasher.update(snapshot_id.get().to_be_bytes());
        }
        RestoreSourceSelector::Commit(commit_id) => {
            hasher.update(commit_id.as_bytes());
        }
    }
    hash_u32_bytes(&mut hasher, destination_workbench_id.as_bytes())?;
    hasher.update(destination_workspace_incarnation_id.as_bytes());
    Ok(hasher.finalize().into())
}

/// Derive the frozen cross-layer v2 restore operation identity.
pub fn restore_operation_id(
    root_id: nokv_types::RootId,
    source_workbench_id: &WorkbenchId,
    source_workspace_incarnation_id: WorkspaceIncarnationId,
    source: RestoreSourceSelector,
    destination_workbench_id: &WorkbenchId,
    destination_workspace_incarnation_id: WorkspaceIncarnationId,
) -> Result<OperationId, RestoreError> {
    restore_selector_identity_digest(
        root_id,
        source_workbench_id,
        source_workspace_incarnation_id,
        source,
        destination_workbench_id,
        destination_workspace_incarnation_id,
    )
    .map(operation_id_from_identity)
}

fn restore_source_matches_selector(
    durable: RestoreSource,
    requested: RestoreSourceSelector,
) -> bool {
    match (durable, requested) {
        (
            RestoreSource::Snapshot {
                snapshot_id: durable,
                ..
            },
            RestoreSourceSelector::Snapshot(requested),
        ) => durable == requested,
        (
            RestoreSource::Commit { commit_id: durable },
            RestoreSourceSelector::Commit(requested),
        ) => durable == requested,
        _ => false,
    }
}

fn restore_initialization_digest(
    operation: &RestoreOperationRecord,
    manifests: &super::restore_records::RestoreDestinationManifests,
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.initialization.v4\0");
    hasher.update(operation.identity_digest);
    hash_manifest_publication(&mut hasher, &manifests.run_manifest);
    hash_manifest_publication(&mut hasher, &manifests.restore_manifest);
    hasher.finalize().into()
}

fn operation_id_from_identity(identity_digest: [u8; SHA256_BYTES]) -> OperationId {
    OperationId::from_bytes(
        identity_digest[..OperationId::BYTE_WIDTH]
            .try_into()
            .expect("operation identity prefix has fixed width"),
    )
}

fn begin_request_digest(
    root_id: nokv_types::RootId,
    request: &BeginRestoreRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.begin.v5\0");
    hasher.update(root_id.as_bytes());
    hasher.update(request.operation_id.as_bytes());
    hash_digest_bytes(&mut hasher, request.source_workbench_id.as_bytes());
    hasher.update(request.expected_source_workspace_incarnation_id.as_bytes());
    match request.source {
        RestoreSourceSelector::Snapshot(snapshot_id) => {
            hasher.update([1]);
            hasher.update(snapshot_id.get().to_be_bytes());
        }
        RestoreSourceSelector::Commit(commit_id) => {
            hasher.update([2]);
            hasher.update(commit_id.as_bytes());
        }
    }
    hash_digest_bytes(&mut hasher, request.destination_workbench_id.as_bytes());
    hasher.update(request.destination_workspace_incarnation_id.as_bytes());
    hasher.update(
        request
            .destination_restore_manifest_identity
            .publication_operation_id
            .as_bytes(),
    );
    hasher.update(
        request
            .destination_restore_manifest_identity
            .artifact_revision_id
            .as_bytes(),
    );
    hash_digest_bytes(
        &mut hasher,
        request.restore_manifest.body_digest_uri.as_bytes(),
    );
    hasher.update(request.restore_manifest.logical_size.to_be_bytes());
    hash_digest_bytes(
        &mut hasher,
        request.restore_manifest.content_type.as_bytes(),
    );
    hasher.finalize().into()
}

fn operation_input_digest(domain: &[u8], operation_id: OperationId) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.command.v1\0");
    hash_digest_bytes(&mut hasher, domain);
    hasher.update(operation_id.as_bytes());
    hasher.finalize().into()
}

fn bind_destination_input_digest(request: &BindRestoreDestinationRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.bind-destination.v1\0");
    hasher.update(request.operation_id.as_bytes());
    hasher.update(request.binding.destination_commit_id.as_bytes());
    hash_digest_bytes(
        &mut hasher,
        request.binding.effective_content_digest_uri.as_bytes(),
    );
    hasher.update(request.binding.destination_projection_input_digest);
    hash_manifest_identity(&mut hasher, &request.binding.run_manifest_identity);
    hash_manifest_identity(&mut hasher, &request.binding.restore_manifest_identity);
    match &request.binding.manifests {
        None => hasher.update([0]),
        Some(manifests) => {
            hasher.update([1]);
            hash_manifest_publication(&mut hasher, &manifests.run_manifest);
            hash_manifest_publication(&mut hasher, &manifests.restore_manifest);
        }
    }
    hasher.finalize().into()
}

fn hash_manifest_identity(hasher: &mut Sha256, identity: &RestoreManifestIdentity) {
    hasher.update(identity.publication_operation_id.as_bytes());
    hasher.update(identity.artifact_revision_id.as_bytes());
}

fn hash_manifest_publication(hasher: &mut Sha256, publication: &RestoreManifestPublication) {
    hasher.update(publication.publication_operation_id.as_bytes());
    hasher.update(publication.workspace_incarnation_id.as_bytes());
    hasher.update(publication.artifact_revision_id.as_bytes());
    hash_digest_bytes(hasher, publication.body_digest_uri.as_bytes());
    hash_digest_bytes(hasher, publication.manifest_digest_uri.as_bytes());
    hasher.update(publication.logical_size.to_be_bytes());
    hash_digest_bytes(hasher, publication.content_type.as_bytes());
}

fn batch_input_digest(
    domain: &[u8],
    operation_id: OperationId,
    limit: usize,
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.batch.v1\0");
    hash_digest_bytes(&mut hasher, domain);
    hasher.update(operation_id.as_bytes());
    hasher.update(
        u64::try_from(limit)
            .expect("restore batch limit fits u64")
            .to_be_bytes(),
    );
    hasher.finalize().into()
}

fn initialization_input_digest(
    operation_id: OperationId,
    initialization_digest: [u8; SHA256_BYTES],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.apply-initialization.v1\0");
    hasher.update(operation_id.as_bytes());
    hasher.update(initialization_digest);
    hasher.finalize().into()
}

fn abort_input_digest(
    operation_id: OperationId,
    terminal_error: &RestoreTerminalError,
) -> Result<[u8; SHA256_BYTES], RestoreError> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.abort.v1\0");
    hasher.update(operation_id.as_bytes());
    hasher.update([u8::from(terminal_error.kind)]);
    hash_u32_bytes(&mut hasher, terminal_error.message.as_bytes())?;
    match terminal_error.evidence_digest {
        None => hasher.update([0]),
        Some(digest) => {
            hasher.update([1]);
            hasher.update(digest);
        }
    }
    Ok(hasher.finalize().into())
}

fn hash_u32_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), RestoreError> {
    let length = u32::try_from(bytes.len()).map_err(|_| RestoreError::SourceClosureMismatch {
        reason: "canonical restore field exceeds u32".to_owned(),
    })?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn hash_digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(
        u32::try_from(bytes.len())
            .expect("bounded restore digest field fits u32")
            .to_be_bytes(),
    );
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use nokv_types::{
        Generation, LogicalShardId, OwnerEpoch, PlacementGeneration, RequestId,
        RootActivationState, RootId, FIXED_ID_BYTES,
    };

    use super::super::namespace::{
        create_visible_workspace, get_visible_path_at, get_visible_workspace_at, RootReadContext,
    };
    use super::super::remove::{remove_path, RemovePathRequest};
    use super::super::snapshot::{
        mint_snapshot, retire_snapshot, MintSnapshotRequest, RetireSnapshotRequest,
        SnapshotSelector,
    };
    use super::*;

    fn sha256_digest_uri(digest: [u8; SHA256_BYTES]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut value = String::with_capacity(7 + SHA256_BYTES * 2);
        value.push_str("sha256:");
        for byte in digest {
            value.push(HEX[usize::from(byte >> 4)] as char);
            value.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        value
    }

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn root() -> RootId {
        RootId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(7).unwrap()
    }

    fn owner(value: u64) -> OwnerEpoch {
        OwnerEpoch::new(value).unwrap()
    }

    fn request(counter: &mut u128) -> RequestId {
        *counter += 1;
        RequestId::from_bytes(counter.to_be_bytes())
    }

    fn incarnation(fill: u8) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn revision(fill: u8) -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn workbench(value: &str) -> WorkbenchId {
        WorkbenchId::new(value).unwrap()
    }

    fn derived_operation_identity(request: &BeginRestoreRequest) -> OperationId {
        restore_operation_id(
            root(),
            &request.source_workbench_id,
            request.expected_source_workspace_incarnation_id,
            request.source,
            &request.destination_workbench_id,
            request.destination_workspace_incarnation_id,
        )
        .unwrap()
    }

    fn bind_operation_identity(mut request: BeginRestoreRequest) -> BeginRestoreRequest {
        request.operation_id = derived_operation_identity(&request);
        request
    }

    fn write_context(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
    ) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            nokv_types::ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner_epoch,
            request(counter),
        )
        .unwrap()
    }

    fn read_context(store: &MetaShard, owner_epoch: OwnerEpoch) -> RootReadContext {
        RootReadContext {
            root_id: root(),
            placement_generation: placement(),
            owner_epoch,
            read_version: store.current_read_version().unwrap(),
        }
    }

    fn fence_command(
        store: &MetaShard,
        request_id: RequestId,
        owner_epoch: OwnerEpoch,
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
            owner_epoch,
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

    fn activate_root(store: &MetaShard, counter: &mut u128, owner_epoch: OwnerEpoch) {
        store.advance_owner_epoch(None, owner_epoch).unwrap();
        store
            .execute(&fence_command(
                store,
                request(counter),
                owner_epoch,
                RootFenceAction::Install,
            ))
            .unwrap();
        store
            .execute(&fence_command(
                store,
                request(counter),
                owner_epoch,
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
    }

    fn put_absent(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        rows: Vec<(MetadataFamily, Vec<u8>, Vec<u8>)>,
    ) {
        let mut predicates = Vec::with_capacity(rows.len());
        let mut mutations = Vec::with_capacity(rows.len());
        for (family, key, value) in rows {
            predicates.push(CommandPredicate::Value {
                family,
                key: key.clone(),
                expected: None,
            });
            mutations.push(CommandMutation::Put { family, key, value });
        }
        let command = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch,
            request_id: request(counter),
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: RootFenceAction::RequireActive,
            predicates,
            mutations,
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&command).unwrap();
    }

    fn replace_present(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        let context = write_context(store, counter, owner_epoch);
        let expected = read_payload(store, context, family, &key)
            .unwrap()
            .expect("test row exists");
        let command = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch,
            request_id: context.request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: context.read_version,
            root_fence_action: RootFenceAction::RequireActive,
            predicates: vec![CommandPredicate::Value {
                family,
                key: key.clone(),
                expected: Some(expected),
            }],
            mutations: vec![CommandMutation::Put {
                family,
                key: key.clone(),
                value,
            }],
            history_projection: vec![HistoryProjection { family, key }],
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&command).unwrap();
    }

    fn delete_present(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        key: Vec<u8>,
    ) {
        let context = write_context(store, counter, owner_epoch);
        let expected = read_payload(store, context, family, &key)
            .unwrap()
            .expect("test row exists");
        let command = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch,
            request_id: context.request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: context.read_version,
            root_fence_action: RootFenceAction::RequireActive,
            predicates: vec![CommandPredicate::Value {
                family,
                key: key.clone(),
                expected: Some(expected),
            }],
            mutations: vec![CommandMutation::Delete {
                family,
                key: key.clone(),
            }],
            history_projection: vec![HistoryProjection { family, key }],
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&command).unwrap();
    }

    fn artifact_record(
        logical_size: u64,
        body_digest_uri: String,
        content_type: &str,
        strong_reference_count: u64,
        last_zero_ref_version: Option<CommitVersion>,
    ) -> ArtifactRevisionRecord {
        ArtifactRevisionRecord {
            logical_size,
            body_digest_uri,
            manifest_digest_uri: "sha256:manifest".to_owned(),
            block_count: 1,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; SHA256_BYTES],
            content_type: content_type.to_owned(),
            state: RevisionState::Available,
            reference_epoch: if strong_reference_count == 0 {
                ReferenceEpoch::ZERO
            } else {
                ReferenceEpoch::new(1)
            },
            strong_reference_count,
            last_zero_ref_version,
        }
    }

    struct SeededSource {
        source_workbench: WorkbenchId,
        source_incarnation: WorkspaceIncarnationId,
        source_commit_id: CommitId,
        source_revision: ArtifactRevisionId,
        manifest_revision: ArtifactRevisionId,
        initialization: RestoreInitialization,
        paths: Vec<nokv_types::NormalizedRelativePath>,
    }

    fn seed_source(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        path_count: usize,
    ) -> SeededSource {
        let source_workbench = workbench("source");
        let source_incarnation = incarnation(10);
        create_visible_workspace(
            store,
            write_context(store, counter, owner_epoch),
            &source_workbench,
            source_incarnation,
        )
        .unwrap();
        let source_revision = revision(20);
        let manifest_revision = revision(21);
        let manifest_body = br#"{"schema":"nokv.workbench.restore_manifest.v1"}"#.to_vec();
        let manifest_digest = sha256_digest_uri(Sha256::digest(&manifest_body).into());
        let current_version =
            CommitVersion::new(store.current_read_version().unwrap().get()).unwrap();
        put_absent(
            store,
            counter,
            owner_epoch,
            vec![
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), source_revision),
                    artifact_record(
                        1,
                        "sha256:source".to_owned(),
                        "text/plain",
                        path_count as u64,
                        None,
                    )
                    .encode()
                    .unwrap(),
                ),
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), manifest_revision),
                    artifact_record(
                        manifest_body.len() as u64,
                        manifest_digest.clone(),
                        "application/json",
                        0,
                        Some(current_version),
                    )
                    .encode()
                    .unwrap(),
                ),
            ],
        );

        let mut paths = Vec::with_capacity(path_count);
        for index in 0..path_count {
            paths.push(
                nokv_types::NormalizedRelativePath::new(format!("outputs/item-{index:04}"))
                    .unwrap(),
            );
        }
        for chunk in paths.chunks(100) {
            let mut rows = Vec::with_capacity(chunk.len() * 2);
            for path in chunk {
                rows.push((
                    MetadataFamily::PathCurrent,
                    path_current_key(root(), source_incarnation, path),
                    PathEntry {
                        generation: Generation::new(1).unwrap(),
                        artifact_revision_id: source_revision,
                        body_digest_uri: "sha256:source".to_owned(),
                        manifest_digest_uri: "sha256:manifest".to_owned(),
                        logical_size: 1,
                        dependency_count: 0,
                        dependency_depth: 0,
                        content_type: "text/plain".to_owned(),
                        producer: Some("test".to_owned()),
                        manifest_id: None,
                        typed_index_projection: TypedProjection::new(BTreeMap::from([(
                            QueryFieldId::new("source.class").unwrap(),
                            QueryScalar::String("seed".to_owned()),
                        )]))
                        .unwrap()
                        .encode()
                        .unwrap(),
                    }
                    .encode()
                    .unwrap(),
                ));
                rows.push((
                    MetadataFamily::RevisionRef,
                    path_revision_ref_key(root(), source_incarnation, path, source_revision),
                    RevisionRefRecord {
                        reference_epoch_at_add: ReferenceEpoch::new(1),
                    }
                    .encode()
                    .unwrap(),
                ));
            }
            put_absent(store, counter, owner_epoch, rows);
        }
        let source_commit_id = CommitId::from_bytes([0xc0; SHA256_BYTES]);
        drive_commit_wire_calls(
            store,
            counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            source_commit_id,
            operation(0xe1),
            operation(0xe2),
            revision(22),
            br#"{"schema":"nokv.workbench.run_manifest.v1"}"#,
        );
        SeededSource {
            source_workbench,
            source_incarnation,
            source_commit_id,
            source_revision,
            manifest_revision,
            initialization: RestoreInitialization {
                restore_manifest: PathEntry {
                    generation: Generation::new(1).unwrap(),
                    artifact_revision_id: manifest_revision,
                    body_digest_uri: manifest_digest,
                    manifest_digest_uri: "sha256:manifest".to_owned(),
                    logical_size: manifest_body.len() as u64,
                    dependency_count: 0,
                    dependency_depth: 0,
                    content_type: "application/json".to_owned(),
                    producer: Some("restore".to_owned()),
                    manifest_id: None,
                    typed_index_projection: TypedProjection::empty().encode().unwrap(),
                },
            },
            paths,
        }
    }

    fn mint_source_snapshot(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source: &SeededSource,
    ) -> SnapshotId {
        let snapshot_id = SnapshotId::new(42);
        let minted = mint_snapshot(
            store,
            write_context(store, counter, owner_epoch),
            &MintSnapshotRequest {
                workbench_id: source.source_workbench.clone(),
                snapshot_id,
                alias: None,
                lease_deadline_ms: 1_000_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(
            minted.snapshot.record.source_commit_id,
            Some(source.source_commit_id)
        );
        snapshot_id
    }

    fn replace_source_projection(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source: &SeededSource,
        projection: TypedProjection,
    ) {
        let encoded_projection = projection.encode().unwrap();
        for path in &source.paths {
            let context = write_context(store, counter, owner_epoch);
            let loaded = load_path(store, context, source.source_incarnation, path)
                .unwrap()
                .expect("seeded source path exists");
            let mut next = loaded.record;
            next.typed_index_projection = encoded_projection.clone();
            let key = path_current_key(root(), source.source_incarnation, path);
            let command = MetadataCommand {
                schema_id: SCHEMA_ID.to_owned(),
                root_id: root(),
                logical_shard_id: shard(),
                object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                    [10; FIXED_ID_BYTES],
                )),
                placement_generation: placement(),
                owner_epoch,
                request_id: context.request_id,
                command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                read_version: context.read_version,
                root_fence_action: RootFenceAction::RequireActive,
                predicates: vec![CommandPredicate::Value {
                    family: MetadataFamily::PathCurrent,
                    key: key.clone(),
                    expected: Some(loaded.payload),
                }],
                mutations: vec![CommandMutation::Put {
                    family: MetadataFamily::PathCurrent,
                    key: key.clone(),
                    value: next.encode().unwrap(),
                }],
                history_projection: vec![HistoryProjection {
                    family: MetadataFamily::PathCurrent,
                    key,
                }],
                event_projection: Vec::new(),
                deterministic_result: Vec::new(),
            }
            .seal();
            store.execute(&command).unwrap();
        }
    }

    fn begin_snapshot_restore(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source: &SeededSource,
        snapshot_id: SnapshotId,
        destination: &str,
        destination_incarnation: WorkspaceIncarnationId,
    ) -> RestoreCommandOutcome {
        let request = snapshot_restore_request(
            source,
            snapshot_id,
            destination,
            destination_incarnation,
            0xd1,
        );
        begin_restore(store, write_context(store, counter, owner_epoch), &request).unwrap()
    }

    fn snapshot_restore_request(
        source: &SeededSource,
        snapshot_id: SnapshotId,
        destination: &str,
        destination_incarnation: WorkspaceIncarnationId,
        _destination_commit_fill: u8,
    ) -> BeginRestoreRequest {
        bind_operation_identity(BeginRestoreRequest {
            operation_id: OperationId::from_bytes([0; 16]),
            source_workbench_id: source.source_workbench.clone(),
            expected_source_workspace_incarnation_id: source.source_incarnation,
            source: RestoreSourceSelector::Snapshot(snapshot_id),
            destination_workbench_id: workbench(destination),
            destination_workspace_incarnation_id: destination_incarnation,
            destination_restore_manifest_identity: RestoreManifestIdentity {
                publication_operation_id: OperationId::from_bytes([0xf2; 16]),
                artifact_revision_id: revision(0xe4),
            },
            destination_committed_at_unix_seconds: 1,
            restore_manifest: restore_manifest_descriptor(&source.initialization),
        })
    }

    fn publish_destination_manifests_for_test(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        operation_id: OperationId,
    ) {
        const RUN_BODY: &[u8] = br#"{"schema":"nokv.workbench.run_manifest.v1"}"#;
        const RESTORE_BODY: &[u8] = br#"{"schema":"nokv.workbench.restore_manifest.v1"}"#;
        let operation = get_restore(
            store,
            write_context(store, counter, owner_epoch),
            operation_id,
        )
        .unwrap()
        .unwrap();
        let RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
            unreachable!();
        };
        let binding = provenance.destination_binding.as_ref().unwrap();
        for (path, identity, body) in [
            (RUN_MANIFEST_PATH, binding.run_manifest_identity, RUN_BODY),
            (
                RESTORE_MANIFEST_PATH,
                binding.restore_manifest_identity,
                RESTORE_BODY,
            ),
        ] {
            let (staged, manifest) = single_object_rows(identity.artifact_revision_id, body);
            let publish = wire_publish_operation(
                identity.publication_operation_id,
                identity.artifact_revision_id,
                &operation.destination_workbench_id,
                operation.destination_workspace_incarnation_id,
                NormalizedRelativePath::new(path).unwrap(),
                PublishAuthority::RestoreStaging {
                    restore_operation_id: operation_id,
                },
                owner_epoch,
                &staged,
                &manifest,
            );
            let manifest_digest_uri = sha256_digest_uri(publish.manifest_seal);
            drive_publish_wire_calls(
                store,
                counter,
                owner_epoch,
                publish,
                &staged,
                &manifest,
                PublishedArtifact {
                    logical_size: body.len() as u64,
                    body_digest_uri: body_digest_uri(body),
                    manifest_digest_uri,
                    content_type: "application/json".to_owned(),
                    producer: None,
                    manifest_id: None,
                    typed_index_projection: TypedProjection::empty().encode().unwrap(),
                },
            );
        }
    }

    fn prepare_cleanup_with_uploading_run_publication(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        destination: &str,
        destination_incarnation: WorkspaceIncarnationId,
    ) -> (
        SeededSource,
        SnapshotId,
        OperationId,
        PublishOperationRecord,
    ) {
        const RUN_BODY: &[u8] = br#"{"schema":"nokv.workbench.run_manifest.v1"}"#;
        let source = seed_source(store, counter, owner_epoch, 1);
        let snapshot_id = mint_source_snapshot(store, counter, owner_epoch, &source);
        let begun = begin_snapshot_restore(
            store,
            counter,
            owner_epoch,
            &source,
            snapshot_id,
            destination,
            destination_incarnation,
        );
        let operation_id = begun.operation.operation_id;
        start_restore_copy(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        copy_all(store, counter, owner_epoch, operation_id);
        let sealed = seal_restore_source(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        let binding = destination_binding_for_test(&sealed.operation, 0xd8);
        let run_identity = binding.run_manifest_identity;
        bind_restore_destination(
            store,
            write_context(store, counter, owner_epoch),
            &BindRestoreDestinationRequest {
                operation_id,
                binding,
            },
        )
        .unwrap();

        let (staged, manifest) = single_object_rows(run_identity.artifact_revision_id, RUN_BODY);
        let publish = wire_publish_operation(
            run_identity.publication_operation_id,
            run_identity.artifact_revision_id,
            &workbench(destination),
            destination_incarnation,
            NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap(),
            PublishAuthority::RestoreStaging {
                restore_operation_id: operation_id,
            },
            owner_epoch,
            &staged,
            &manifest,
        );
        let uploading = PublicationService::new(store)
            .begin_publish(BeginPublishRequest {
                context: publication_context(store, counter, owner_epoch),
                operation: publish,
            })
            .unwrap()
            .operation;

        abort_restore(
            store,
            write_context(store, counter, owner_epoch),
            &AbortRestoreRequest {
                operation_id,
                terminal_error: RestoreTerminalError {
                    kind: RestoreTerminalErrorKind::AbortedByCaller,
                    message: "cancelled with a live destination publisher".to_owned(),
                    evidence_digest: None,
                },
            },
        )
        .unwrap();
        start_restore_cleanup(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        loop {
            let cleanup = cleanup_restore_batch(
                store,
                write_context(store, counter, owner_epoch),
                CopyRestoreBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap();
            if cleanup.source_eof {
                break;
            }
        }
        (source, snapshot_id, operation_id, uploading)
    }

    fn restore_manifest_descriptor(
        initialization: &RestoreInitialization,
    ) -> RestoreManifestDescriptor {
        RestoreManifestDescriptor {
            body_digest_uri: initialization.restore_manifest.body_digest_uri.clone(),
            logical_size: initialization.restore_manifest.logical_size,
            content_type: initialization.restore_manifest.content_type.clone(),
        }
    }

    fn destination_binding_for_test(
        operation: &RestoreOperationRecord,
        _destination_commit_fill: u8,
    ) -> RestoreDestinationBinding {
        let RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
            unreachable!();
        };
        let restore_manifest_identity = operation.destination_restore_manifest_identity.unwrap();
        let mut run_operation = *restore_manifest_identity
            .publication_operation_id
            .as_bytes();
        run_operation[0] ^= 0x80;
        if run_operation == *operation.operation_id.as_bytes() {
            run_operation[1] ^= 0x80;
        }
        let mut run_revision = *restore_manifest_identity.artifact_revision_id.as_bytes();
        run_revision[0] ^= 0x80;
        let effective_content_digest_uri = if operation.source_matches_base_commit == Some(true) {
            provenance.source_commit.content_digest_uri.clone()
        } else {
            commit_member_tree_digest_uri(operation.member_rolling_digest)
        };
        let destination_commit_id = workbench_commit_identity_for_test(
            &operation.destination_workbench_id,
            &effective_content_digest_uri,
            &provenance.source_commit.manifest_digest_uri,
        );
        assert_ne!(destination_commit_id, provenance.source_commit.commit_id);
        RestoreDestinationBinding {
            destination_commit_id,
            effective_content_digest_uri,
            destination_projection_input_digest: [0x72; SHA256_BYTES],
            run_manifest_identity: RestoreManifestIdentity {
                publication_operation_id: OperationId::from_bytes(run_operation),
                artifact_revision_id: ArtifactRevisionId::from_bytes(run_revision),
            },
            restore_manifest_identity,
            manifests: None,
        }
    }

    fn workbench_commit_identity_for_test(
        workbench_id: &WorkbenchId,
        content_digest_uri: &str,
        manifest_digest_uri: &str,
    ) -> CommitId {
        let mut identity = Sha256::new();
        identity.update(b"nokv.workbench.commit_identity.v1\0");
        for value in [
            workbench_id.as_bytes(),
            content_digest_uri.as_bytes(),
            manifest_digest_uri.as_bytes(),
        ] {
            identity.update(
                u64::try_from(value.len())
                    .expect("test identity input fits u64")
                    .to_be_bytes(),
            );
            identity.update(value);
        }
        CommitId::from_bytes(identity.finalize().into())
    }

    fn detach_commit_head_for_retirement_test(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        workspace_incarnation_id: WorkspaceIncarnationId,
        commit_id: CommitId,
    ) {
        let context = write_context(store, counter, owner_epoch);
        let head_key = workbench_commit_head_key(root(), workspace_incarnation_id);
        let head_payload = read_payload(
            store,
            context,
            MetadataFamily::WorkbenchCommitHead,
            &head_key,
        )
        .unwrap()
        .unwrap();
        let head = WorkbenchCommitHeadRecord::decode(&head_payload).unwrap();
        assert_eq!(head.commit_id, commit_id);
        let commit_key_bytes = commit_key(root(), commit_id);
        let commit_payload =
            read_payload(store, context, MetadataFamily::Commit, &commit_key_bytes)
                .unwrap()
                .unwrap();
        let commit = CommitRecord::decode(&commit_payload).unwrap();
        let next =
            remove_commit_consumer(&commit, next_commit_version(context.read_version).unwrap())
                .unwrap();
        let consumer_key =
            workbench_head_commit_consumer_key(root(), commit_id, workspace_incarnation_id);
        let consumer_payload = read_payload(
            store,
            context,
            MetadataFamily::CommitConsumer,
            &consumer_key,
        )
        .unwrap()
        .unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                        [10; FIXED_ID_BYTES],
                    )),
                    placement_generation: placement(),
                    owner_epoch,
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: vec![
                        CommandPredicate::Value {
                            family: MetadataFamily::Commit,
                            key: commit_key_bytes.clone(),
                            expected: Some(commit_payload),
                        },
                        CommandPredicate::Value {
                            family: MetadataFamily::WorkbenchCommitHead,
                            key: head_key.clone(),
                            expected: Some(head_payload),
                        },
                        CommandPredicate::Value {
                            family: MetadataFamily::CommitConsumer,
                            key: consumer_key.clone(),
                            expected: Some(consumer_payload),
                        },
                    ],
                    mutations: vec![
                        CommandMutation::Put {
                            family: MetadataFamily::Commit,
                            key: commit_key_bytes.clone(),
                            value: next.encode().unwrap(),
                        },
                        CommandMutation::Delete {
                            family: MetadataFamily::WorkbenchCommitHead,
                            key: head_key.clone(),
                        },
                        CommandMutation::Delete {
                            family: MetadataFamily::CommitConsumer,
                            key: consumer_key.clone(),
                        },
                    ],
                    history_projection: vec![
                        HistoryProjection {
                            family: MetadataFamily::Commit,
                            key: commit_key_bytes,
                        },
                        HistoryProjection {
                            family: MetadataFamily::WorkbenchCommitHead,
                            key: head_key,
                        },
                        HistoryProjection {
                            family: MetadataFamily::CommitConsumer,
                            key: consumer_key,
                        },
                    ],
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn copy_all(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        operation_id: OperationId,
    ) {
        loop {
            let outcome = copy_restore_batch(
                store,
                write_context(store, counter, owner_epoch),
                CopyRestoreBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap();
            if outcome.source_eof {
                break;
            }
        }
    }

    fn drive_to_ready(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        operation_id: OperationId,
        initialization: &RestoreInitialization,
    ) {
        start_restore_copy(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        copy_all(store, counter, owner_epoch, operation_id);
        seal_restore_source(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        drive_sealed_to_ready(store, counter, owner_epoch, operation_id, initialization);
    }

    fn drive_sealed_to_ready(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        operation_id: OperationId,
        initialization: &RestoreInitialization,
    ) {
        let sealed = get_restore(
            store,
            write_context(store, counter, owner_epoch),
            operation_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            restore_manifest_descriptor(initialization),
            sealed.restore_manifest
        );
        bind_restore_destination(
            store,
            write_context(store, counter, owner_epoch),
            &BindRestoreDestinationRequest {
                operation_id,
                binding: destination_binding_for_test(&sealed, 0xd8),
            },
        )
        .unwrap();
        publish_destination_manifests_for_test(store, counter, owner_epoch, operation_id);
        apply_restore_initialization(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        loop {
            let built = build_restore_commit_members(
                store,
                write_context(store, counter, owner_epoch),
                RestoreClosureBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap();
            if built.members_complete {
                break;
            }
        }
        loop {
            let sealed = seal_restore_commit_revisions(
                store,
                write_context(store, counter, owner_epoch),
                RestoreClosureBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap();
            if sealed.ready {
                break;
            }
        }
    }

    #[test]
    fn restore_operation_v2_matches_the_sdk_golden_vector() {
        let operation_id = restore_operation_id(
            RootId::from_bytes([1; 16]),
            &WorkbenchId::new("source-run").unwrap(),
            WorkspaceIncarnationId::from_bytes([2; 16]),
            RestoreSourceSelector::Snapshot(SnapshotId::new(7)),
            &WorkbenchId::new("restored-run").unwrap(),
            WorkspaceIncarnationId::from_bytes([
                0xd6, 0x67, 0x1c, 0x11, 0x22, 0xc9, 0xf8, 0x73, 0x9e, 0x03, 0xfe, 0xc7, 0x75, 0x20,
                0xaa, 0xed,
            ]),
        )
        .unwrap();
        assert_eq!(
            operation_id,
            OperationId::from_bytes([
                0x9b, 0x21, 0xea, 0xbe, 0x08, 0xd0, 0x35, 0x6d, 0x55, 0x16, 0x8e, 0xf1, 0x62, 0x15,
                0x6e, 0xe6,
            ])
        );
    }

    #[test]
    fn restore_cannot_reuse_an_existing_workspace_incarnation() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 0);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let destination = workbench("duplicate-incarnation-destination");

        assert_eq!(
            begin_restore(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                &bind_operation_identity(BeginRestoreRequest {
                    operation_id: OperationId::from_bytes([0; 16]),
                    source_workbench_id: source.source_workbench.clone(),
                    expected_source_workspace_incarnation_id: source.source_incarnation,
                    source: RestoreSourceSelector::Snapshot(snapshot_id),
                    destination_workbench_id: destination.clone(),
                    destination_workspace_incarnation_id: source.source_incarnation,
                    destination_restore_manifest_identity: RestoreManifestIdentity {
                        publication_operation_id: OperationId::from_bytes([0xf2; 16]),
                        artifact_revision_id: source.manifest_revision,
                    },
                    destination_committed_at_unix_seconds: 1,
                    restore_manifest: restore_manifest_descriptor(&source.initialization),
                }),
            ),
            Err(RestoreError::DestinationIncarnationClaimed {
                incarnation_id: source.source_incarnation,
                workbench_id: source.source_workbench.clone(),
            })
        );
        assert_eq!(
            get_visible_workspace_at(&store, read_context(&store, owner_epoch), &destination)
                .unwrap(),
            None
        );
    }

    #[test]
    fn begin_retry_reuses_first_writer_time_and_defers_destination_authority() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 1);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let request = snapshot_restore_request(
            &source,
            snapshot_id,
            "first-writer-time",
            incarnation(30),
            0xd8,
        );
        let mut wrong_operation_id = request.clone();
        wrong_operation_id.operation_id = OperationId::from_bytes([0xee; 16]);
        assert_eq!(
            begin_restore(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                &wrong_operation_id,
            ),
            Err(RestoreError::OperationIdentityMismatch {
                expected: request.operation_id,
                actual: wrong_operation_id.operation_id,
            })
        );
        assert!(get_restore(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            request.operation_id,
        )
        .unwrap()
        .is_none());
        let begin_context = write_context(&store, &mut counter, owner_epoch);
        let begun = begin_restore(&store, begin_context, &request).unwrap();

        let restored = get_restore(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            begun.operation.operation_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            restored.destination_restore_manifest_identity,
            Some(request.destination_restore_manifest_identity)
        );
        assert_eq!(restored.restore_manifest, request.restore_manifest);
        let RestoreCommitProvenance::V5(pre_bind) = &restored.commit_provenance else {
            unreachable!();
        };
        assert_eq!(pre_bind.destination_committed_at_unix_seconds, 1);
        assert!(pre_bind.destination_binding.is_none());

        let mut later_clock_retry = request.clone();
        later_clock_retry.destination_committed_at_unix_seconds = 99;
        assert_eq!(
            derived_operation_identity(&later_clock_retry),
            derived_operation_identity(&request)
        );
        assert_eq!(
            begin_request_digest(root(), &later_clock_retry),
            begin_request_digest(root(), &request)
        );
        let replay = begin_restore(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &later_clock_retry,
        )
        .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation, begun.operation);
        let RestoreCommitProvenance::V5(provenance) = &replay.operation.commit_provenance else {
            unreachable!();
        };
        assert_eq!(provenance.destination_committed_at_unix_seconds, 1);

        assert!(matches!(
            &replay.operation.commit_provenance,
            RestoreCommitProvenance::V5(provenance)
                if provenance.destination_binding.is_none()
        ));

        let mut wrong_restore_identity = request;
        wrong_restore_identity
            .destination_restore_manifest_identity
            .artifact_revision_id = revision(0xf3);
        assert_eq!(
            derived_operation_identity(&wrong_restore_identity),
            derived_operation_identity(&later_clock_retry)
        );
        assert_ne!(
            begin_request_digest(root(), &wrong_restore_identity),
            begin_request_digest(root(), &later_clock_retry)
        );
        assert_eq!(
            begin_restore(&store, begin_context, &wrong_restore_identity),
            Err(RestoreError::RequestInputMismatch)
        );
    }

    #[test]
    fn restore_exact_replay_rejects_a_missing_recovery_binding() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 1);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let request_value = snapshot_restore_request(
            &source,
            snapshot_id,
            "recovery-binding",
            incarnation(30),
            0xd8,
        );
        let context = write_context(&store, &mut counter, owner_epoch);
        begin_restore(&store, context, &request_value).unwrap();
        let dedupe = store
            .lookup_request(root(), placement(), owner_epoch, context.request_id)
            .unwrap()
            .unwrap();
        store
            .replace_recovery_header_for_test(dedupe.recovery_lsn, None)
            .unwrap();

        assert!(matches!(
            begin_restore(&store, context, &request_value),
            Err(RestoreError::Meta(MetaError::CorruptRecord { .. }))
        ));
    }

    #[test]
    fn mutated_snapshot_seals_against_base_commit_then_late_binds_exact_destination() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 1);
        replace_source_projection(
            &store,
            &mut counter,
            owner_epoch,
            &source,
            TypedProjection::new(BTreeMap::from([(
                QueryFieldId::new("source.class").unwrap(),
                QueryScalar::String("mutated-after-base-commit".to_owned()),
            )]))
            .unwrap(),
        );
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let begun = begin_snapshot_restore(
            &store,
            &mut counter,
            owner_epoch,
            &source,
            snapshot_id,
            "mutated-snapshot-restore",
            incarnation(31),
        );
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        copy_all(
            &store,
            &mut counter,
            owner_epoch,
            begun.operation.operation_id,
        );
        let sealed = seal_restore_source(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        assert_eq!(sealed.operation.source_matches_base_commit, Some(false));
        assert_eq!(
            sealed.operation.member_seal,
            Some(sealed.operation.member_rolling_digest)
        );

        let binding = destination_binding_for_test(&sealed.operation, 0xd8);
        let bind_request = BindRestoreDestinationRequest {
            operation_id: begun.operation.operation_id,
            binding: binding.clone(),
        };
        let bound = bind_restore_destination(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &bind_request,
        )
        .unwrap();
        assert!(!bound.replayed);
        let RestoreCommitProvenance::V5(provenance) = &bound.operation.commit_provenance else {
            unreachable!();
        };
        assert_eq!(provenance.destination_binding.as_ref(), Some(&binding));

        let replay = bind_restore_destination(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &bind_request,
        )
        .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation, bound.operation);

        let mut different = bind_request;
        different.binding.destination_projection_input_digest = [0x76; SHA256_BYTES];
        assert_ne!(
            bind_destination_input_digest(&different),
            bind_destination_input_digest(&BindRestoreDestinationRequest {
                operation_id: begun.operation.operation_id,
                binding,
            })
        );
        assert_eq!(
            bind_restore_destination(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                &different,
            ),
            Err(RestoreError::DestinationBindingMismatch {
                operation_id: begun.operation.operation_id,
            })
        );
    }

    #[test]
    fn snapshot_begin_rejects_missing_or_ahead_commit_consumer() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 1);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let request = snapshot_restore_request(
            &source,
            snapshot_id,
            "retention-begin",
            incarnation(30),
            0xd9,
        );
        let consumer_key =
            snapshot_commit_consumer_key(root(), source.source_commit_id, snapshot_id);
        let original = read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &consumer_key,
        )
        .unwrap()
        .unwrap();
        replace_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::CommitConsumer,
            consumer_key.clone(),
            CommitConsumerRecord {
                consumer_epoch_at_add: ConsumerEpoch::new(u64::MAX),
            }
            .encode(),
        );
        assert_eq!(
            begin_restore(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                &request,
            ),
            Err(RestoreError::SnapshotRetentionMismatch)
        );

        replace_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::CommitConsumer,
            consumer_key.clone(),
            original,
        );
        delete_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::CommitConsumer,
            consumer_key,
        );
        assert_eq!(
            begin_restore(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                &request,
            ),
            Err(RestoreError::SnapshotRetentionMismatch)
        );
    }

    #[test]
    fn snapshot_resume_rejects_commit_id_or_immutable_seal_drift() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 1);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let request = snapshot_restore_request(
            &source,
            snapshot_id,
            "retention-resume",
            incarnation(30),
            0xda,
        );
        let begun = begin_restore(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &request,
        )
        .unwrap();
        let snapshot_key = snapshot_ref_key(root(), source.source_incarnation, snapshot_id);
        let snapshot = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::SnapshotRef,
            &snapshot_key,
            SnapshotRefRecord::decode,
        )
        .unwrap()
        .unwrap();
        let mut drifted_snapshot = snapshot.record.clone();
        drifted_snapshot.source_commit_id = Some(CommitId::from_bytes([0xee; SHA256_BYTES]));
        replace_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::SnapshotRef,
            snapshot_key.clone(),
            drifted_snapshot.encode().unwrap(),
        );
        assert_eq!(
            start_restore_copy(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                RestoreOperationRequest {
                    operation_id: begun.operation.operation_id,
                },
            ),
            Err(RestoreError::SnapshotRetentionMismatch)
        );

        replace_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::SnapshotRef,
            snapshot_key,
            snapshot.record.encode().unwrap(),
        );
        let commit_key = commit_key(root(), source.source_commit_id);
        let commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key,
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        let mut drifted_commit = commit.record;
        drifted_commit.member_digest = [0xef; SHA256_BYTES];
        replace_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::Commit,
            commit_key,
            drifted_commit.encode().unwrap(),
        );
        assert_eq!(
            start_restore_copy(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                RestoreOperationRequest {
                    operation_id: begun.operation.operation_id,
                },
            ),
            Err(RestoreError::SnapshotRetentionMismatch)
        );
    }

    #[test]
    fn held_source_run_manifest_read_is_commit_owned_and_survives_snapshot_lease_expiry() {
        let mut counter = 950_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 1);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let begun = begin_snapshot_restore(
            &store,
            &mut counter,
            owner_epoch,
            &source,
            snapshot_id,
            "held-run-manifest",
            incarnation(39),
        );
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        copy_all(
            &store,
            &mut counter,
            owner_epoch,
            begun.operation.operation_id,
        );
        seal_restore_source(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();

        let held = read_restore_source_run_manifest(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            begun.operation.operation_id,
        )
        .unwrap();
        assert_eq!(held.source_commit_id, source.source_commit_id);
        let RestoreCommitProvenance::V5(provenance) = &held.operation.commit_provenance else {
            unreachable!();
        };
        assert_eq!(
            held.path_entry.artifact_revision_id,
            provenance.source_commit.tree_manifest_revision_id
        );
        assert!(held.source_snapshot_read_version.is_some());

        store
            .observe_lease_clock(root(), placement(), owner_epoch, 2_000_000)
            .unwrap();
        assert!(read_restore_source_run_manifest(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            begun.operation.operation_id,
        )
        .is_ok());

        delete_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::CommitConsumer,
            snapshot_commit_consumer_key(root(), source.source_commit_id, snapshot_id),
        );
        assert_eq!(
            read_restore_source_run_manifest(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                begun.operation.operation_id,
            ),
            Err(RestoreError::SnapshotRetentionMismatch)
        );
    }

    #[test]
    fn empty_ordinary_restore_commits_exactly_two_destination_manifests() {
        let mut counter = 975_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 0);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let destination = workbench("empty-restored");
        let destination_incarnation = incarnation(40);
        let completed = drive_snapshot_restore_to_visible(
            &store,
            &mut counter,
            owner_epoch,
            &source.source_workbench,
            source.source_incarnation,
            snapshot_id,
            &destination,
            destination_incarnation,
            operation(0x73),
            revision(0x74),
            1,
        );
        assert_eq!(completed.result.member_count, 0);
        let receipt = completed
            .command
            .operation
            .destination_commit_receipt()
            .unwrap();
        let commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), receipt.destination_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(commit.record.member_count, 2);
        assert_eq!(commit.record.unique_revision_count, 2);
        let head = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::WorkbenchCommitHead,
            &workbench_commit_head_key(root(), destination_incarnation),
            WorkbenchCommitHeadRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(head.record.commit_id, receipt.destination_commit_id);
        assert_eq!(head.record.head_generation, Generation::new(1).unwrap());
        assert!(
            get_visible_workspace_at(&store, read_context(&store, owner_epoch), &destination,)
                .unwrap()
                .is_some()
        );
        for path in [RUN_MANIFEST_PATH, RESTORE_MANIFEST_PATH] {
            assert!(get_visible_path_at(
                &store,
                read_context(&store, owner_epoch),
                &destination,
                &NormalizedRelativePath::new(path).unwrap(),
            )
            .unwrap()
            .is_some());
        }
    }

    #[test]
    fn retiring_restored_destination_releases_its_parent_child_consumer() {
        let mut counter = 980_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let source = seed_source(&store, &mut counter, owner_epoch, 0);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, owner_epoch, &source);
        let destination = workbench("retired-restore");
        let destination_incarnation = incarnation(41);
        let completed = drive_snapshot_restore_to_visible(
            &store,
            &mut counter,
            owner_epoch,
            &source.source_workbench,
            source.source_incarnation,
            snapshot_id,
            &destination,
            destination_incarnation,
            operation(0x76),
            revision(0x77),
            1,
        );
        let destination_commit_id = completed
            .command
            .operation
            .destination_commit_receipt()
            .unwrap()
            .destination_commit_id;
        let parent_consumer_key =
            child_commit_consumer_key(root(), source.source_commit_id, destination_commit_id);
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &parent_consumer_key,
        )
        .unwrap()
        .is_some());
        let parent_before_retire = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), source.source_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();

        // Commit retirement requires every external consumer to be detached.
        // This helper mirrors commit.rs' retirement test boundary and is not a
        // production workspace-lifecycle shortcut.
        detach_commit_head_for_retirement_test(
            &store,
            &mut counter,
            owner_epoch,
            destination_incarnation,
            destination_commit_id,
        );
        let zero_consumer_destination = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), destination_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(zero_consumer_destination.record.consumer_count, 0);
        let retire_operation_id = operation(0x78);
        let service = CommitService::new(&store);
        let mut retired = service
            .begin_retirement(BeginCommitRetirementRequest {
                context: write_context(&store, &mut counter, owner_epoch),
                operation_id: retire_operation_id,
                commit_id: destination_commit_id,
                expected_consumer_epoch: zero_consumer_destination.record.consumer_epoch,
            })
            .unwrap();
        while retired.operation.phase != CommitRetirePhase::Complete {
            retired = service
                .release_retired_commit(BuildCommitStepRequest {
                    context: write_context(&store, &mut counter, owner_epoch),
                    operation_id: retire_operation_id,
                    limit: MAX_COMMIT_MEMBER_BATCH_ROWS,
                })
                .unwrap();
        }
        let destination_commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), destination_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(destination_commit.record.state, CommitState::Retired);
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &parent_consumer_key,
        )
        .unwrap()
        .is_none());
        let parent_after_retire = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), source.source_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parent_after_retire.record.consumer_count + 1,
            parent_before_retire.record.consumer_count
        );
        assert_eq!(
            parent_after_retire.record.consumer_epoch.get(),
            parent_before_retire.record.consumer_epoch.get() + 1
        );
        assert!(parent_after_retire
            .record
            .last_zero_consumer_version
            .is_none());
    }

    #[test]
    fn large_snapshot_restore_survives_reopen_and_replays_publication() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("restore-meta");
        let mut counter = 0_u128;
        let initial_owner = owner(1);
        let store = crate::workspace::test_support::initialize_file(&path, shard()).unwrap();
        activate_root(&store, &mut counter, initial_owner);
        let source = seed_source(&store, &mut counter, initial_owner, 300);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, initial_owner, &source);
        let begin_context = write_context(&store, &mut counter, initial_owner);
        let begin_request = bind_operation_identity(BeginRestoreRequest {
            operation_id: OperationId::from_bytes([0; 16]),
            source_workbench_id: source.source_workbench.clone(),
            expected_source_workspace_incarnation_id: source.source_incarnation,
            source: RestoreSourceSelector::Snapshot(snapshot_id),
            destination_workbench_id: workbench("destination"),
            destination_workspace_incarnation_id: incarnation(30),
            destination_restore_manifest_identity: RestoreManifestIdentity {
                publication_operation_id: OperationId::from_bytes([0xf2; 16]),
                artifact_revision_id: revision(0xe4),
            },
            destination_committed_at_unix_seconds: 1,
            restore_manifest: restore_manifest_descriptor(&source.initialization),
        });
        let begun = begin_restore(&store, begin_context, &begin_request).unwrap();
        let begin_replay = begin_restore(&store, begin_context, &begin_request).unwrap();
        assert!(begin_replay.replayed);
        assert_eq!(begin_replay.operation, begun.operation);
        assert!(begun.operation.initialization_digest.is_none());
        let mut later_clock_retry = begin_request.clone();
        later_clock_retry.destination_committed_at_unix_seconds = 99;
        assert_eq!(
            begin_request_digest(root(), &later_clock_retry),
            begin_request_digest(root(), &begin_request)
        );
        let later_clock_replay = begin_restore(
            &store,
            write_context(&store, &mut counter, initial_owner),
            &later_clock_retry,
        )
        .unwrap();
        assert!(later_clock_replay.replayed);
        assert_eq!(later_clock_replay.operation, begun.operation);
        let RestoreCommitProvenance::V5(provenance) =
            &later_clock_replay.operation.commit_provenance
        else {
            unreachable!();
        };
        assert_eq!(provenance.destination_committed_at_unix_seconds, 1);
        let operation_id = begun.operation.operation_id;
        let mut wrong_descriptor = begin_request.clone();
        wrong_descriptor.restore_manifest.body_digest_uri = format!("sha256:{}", "cd".repeat(32));
        assert_ne!(
            begin_request_digest(root(), &wrong_descriptor),
            begin_request_digest(root(), &begin_request)
        );
        assert_eq!(derived_operation_identity(&wrong_descriptor), operation_id);
        assert_eq!(
            begin_restore(
                &store,
                write_context(&store, &mut counter, initial_owner),
                &wrong_descriptor,
            ),
            Err(RestoreError::RestoreManifestBindingMismatch { operation_id })
        );
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, initial_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        copy_restore_batch(
            &store,
            write_context(&store, &mut counter, initial_owner),
            CopyRestoreBatchRequest {
                operation_id,
                limit: MAX_RESTORE_BATCH_MEMBERS,
            },
        )
        .unwrap();
        let index_key = secondary_index_key(
            root(),
            &QueryFieldId::new("source.class").unwrap(),
            &QueryScalar::String("seed".to_owned()),
            incarnation(30),
            &source.paths[0],
        );
        let staged_index = store
            .read_at(
                root(),
                placement(),
                initial_owner,
                MetadataFamily::SecondaryIndex,
                &index_key,
                store.current_read_version().unwrap(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            SecondaryIndexRecord::decode(&staged_index)
                .unwrap()
                .path_generation,
            Generation::new(1).unwrap()
        );
        assert!(get_visible_workspace_at(
            &store,
            read_context(&store, initial_owner),
            &workbench("destination")
        )
        .unwrap()
        .is_none());
        drop(store);

        let store = crate::workspace::test_support::open_file(&path, shard()).unwrap();
        assert_eq!(
            store
                .read_at(
                    root(),
                    placement(),
                    initial_owner,
                    MetadataFamily::SecondaryIndex,
                    &index_key,
                    store.current_read_version().unwrap(),
                )
                .unwrap(),
            Some(staged_index)
        );
        copy_all(&store, &mut counter, initial_owner, operation_id);
        let sealed = seal_restore_source(
            &store,
            write_context(&store, &mut counter, initial_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert_eq!(sealed.operation.next_member_sequence, 300);
        let mut wrong_initialization = source.initialization.clone();
        wrong_initialization.restore_manifest.body_digest_uri =
            format!("sha256:{}", "ef".repeat(32));
        assert_eq!(
            validate_initialization(&wrong_initialization, &sealed.operation),
            Err(RestoreError::ManifestBindingMismatch)
        );
        drive_sealed_to_ready(
            &store,
            &mut counter,
            initial_owner,
            operation_id,
            &source.initialization,
        );
        assert!(get_visible_workspace_at(
            &store,
            read_context(&store, initial_owner),
            &workbench("destination")
        )
        .unwrap()
        .is_none());
        let complete_context = write_context(&store, &mut counter, initial_owner);
        let completed = complete_restore(
            &store,
            complete_context,
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        let source_commit_after_complete = read_record(
            &store,
            write_context(&store, &mut counter, initial_owner),
            MetadataFamily::Commit,
            &commit_key(root(), source.source_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        let complete_replay = complete_restore(
            &store,
            complete_context,
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        let source_commit_after_replay = read_record(
            &store,
            write_context(&store, &mut counter, initial_owner),
            MetadataFamily::Commit,
            &commit_key(root(), source.source_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert!(complete_replay.command.replayed);
        assert_eq!(complete_replay.result, completed.result);
        assert_eq!(
            source_commit_after_replay.record,
            source_commit_after_complete.record
        );
        assert_eq!(completed.result.member_count, 300);
        store
            .observe_lease_clock(root(), placement(), initial_owner, 2_000_000)
            .unwrap();
        let terminal_begin_replay = begin_restore(
            &store,
            write_context(&store, &mut counter, initial_owner),
            &begin_request,
        )
        .unwrap();
        assert!(terminal_begin_replay.replayed);
        assert_eq!(
            terminal_begin_replay.operation.phase,
            RestorePhase::Complete
        );
        assert!(matches!(
            abort_restore(
                &store,
                write_context(&store, &mut counter, initial_owner),
                &AbortRestoreRequest {
                    operation_id,
                    terminal_error: RestoreTerminalError {
                        kind:
                            super::super::restore_records::RestoreTerminalErrorKind::AbortedByCaller,
                        message: "too late".to_owned(),
                        evidence_digest: None,
                    },
                },
            ),
            Err(RestoreError::InvalidPhase {
                actual: RestorePhase::Complete,
                ..
            })
        ));
        let manifest = get_visible_path_at(
            &store,
            read_context(&store, initial_owner),
            &workbench("destination"),
            &restore_manifest_path(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(manifest.artifact_revision_id, revision(0xe4));
        let hold = read_payload(
            &store,
            write_context(&store, &mut counter, initial_owner),
            MetadataFamily::HistoryHold,
            &restore_history_hold_key(root(), operation_id),
        )
        .unwrap();
        assert!(hold.is_none());
    }

    #[test]
    fn owner_change_fences_old_worker_and_new_owner_resumes() {
        let mut counter = 1000_u128;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        let first_owner = owner(1);
        activate_root(&store, &mut counter, first_owner);
        let source = seed_source(&store, &mut counter, first_owner, 3);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, first_owner, &source);
        let begun = begin_snapshot_restore(
            &store,
            &mut counter,
            first_owner,
            &source,
            snapshot_id,
            "owner-move",
            incarnation(31),
        );
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, first_owner),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        let stale = write_context(&store, &mut counter, first_owner);
        let second_owner = owner(2);
        store
            .advance_owner_epoch(Some(first_owner), second_owner)
            .unwrap();
        assert!(matches!(
            copy_restore_batch(
                &store,
                stale,
                CopyRestoreBatchRequest {
                    operation_id: begun.operation.operation_id,
                    limit: 2,
                }
            ),
            Err(RestoreError::Meta(MetaError::OwnerEpochMismatch { .. }))
        ));
        copy_all(
            &store,
            &mut counter,
            second_owner,
            begun.operation.operation_id,
        );
        let operation = get_restore(
            &store,
            write_context(&store, &mut counter, second_owner),
            begun.operation.operation_id,
        )
        .unwrap()
        .unwrap();
        assert!(operation.source_eof);
    }

    #[test]
    fn large_snapshot_member_copies_replays_and_releases_retention_after_abort() {
        let mut counter = 1900_u128;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        let current_owner = owner(1);
        activate_root(&store, &mut counter, current_owner);
        let source = seed_source(&store, &mut counter, current_owner, 1);
        let projection = TypedProjection::new(
            (0..super::super::query_records::MAX_TYPED_PROJECTION_FIELDS)
                .map(|index| {
                    (
                        QueryFieldId::new(format!("capacity.field_{index:02}")).unwrap(),
                        QueryScalar::String("x".repeat(998)),
                    )
                })
                .collect(),
        )
        .unwrap();
        // Keep the fixture at the largest valid stored projection shape.
        assert_eq!(projection.encode().unwrap().len(), 61_323);
        replace_source_projection(&store, &mut counter, current_owner, &source, projection);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, current_owner, &source);
        let begun = begin_snapshot_restore(
            &store,
            &mut counter,
            current_owner,
            &source,
            snapshot_id,
            "capacity-abort",
            incarnation(31),
        );
        let operation_id = begun.operation.operation_id;
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();

        let copy_context = write_context(&store, &mut counter, current_owner);
        let request = CopyRestoreBatchRequest {
            operation_id,
            limit: MAX_RESTORE_BATCH_MEMBERS,
        };
        let copied = copy_restore_batch(&store, copy_context, request).unwrap();
        assert_eq!(copied.command.operation.phase, RestorePhase::Copying);
        assert_eq!(copied.copied_members, 1);
        let replay = copy_restore_batch(&store, copy_context, request).unwrap();
        assert!(replay.command.replayed);
        assert_eq!(replay.command.operation, copied.command.operation);

        abort_restore(
            &store,
            write_context(&store, &mut counter, current_owner),
            &AbortRestoreRequest {
                operation_id,
                terminal_error: RestoreTerminalError {
                    kind: RestoreTerminalErrorKind::AbortedByCaller,
                    message: "cancelled after large-member copy".to_owned(),
                    evidence_digest: None,
                },
            },
        )
        .unwrap();

        start_restore_cleanup(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        loop {
            let outcome = cleanup_restore_batch(
                &store,
                write_context(&store, &mut counter, current_owner),
                CopyRestoreBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap();
            if outcome.source_eof {
                break;
            }
        }
        let cleaned = finish_restore_cleanup(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert_eq!(cleaned.operation.phase, RestorePhase::Cleaned);
        let snapshot = read_record(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::SnapshotRef,
            &snapshot_ref_key(root(), source.source_incarnation, snapshot_id),
            SnapshotRefRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(snapshot.record.consumer_count, 0);
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::HistoryHold,
            &restore_history_hold_key(root(), operation_id),
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn cleanup_restore_batch_shrinks_to_the_fully_derived_byte_budget() {
        let terminal = maximum_cleanup_terminal_error();
        assert_eq!(terminal.message.len(), MAX_RESTORE_TERMINAL_ERROR_BYTES);
        assert!(terminal.evidence_digest.is_some());
        let mut counter = 1950_u128;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        let current_owner = owner(1);
        activate_root(&store, &mut counter, current_owner);
        let source = seed_source(&store, &mut counter, current_owner, 2);
        let projection = TypedProjection::new(
            (0..super::super::query_records::MAX_TYPED_PROJECTION_FIELDS)
                .map(|index| {
                    (
                        QueryFieldId::new(format!("cleanup.field_{index:02}")).unwrap(),
                        QueryScalar::String("y".repeat(700)),
                    )
                })
                .collect(),
        )
        .unwrap();
        // Freeze the payload size so this remains a byte-budget fixture.
        assert_eq!(projection.encode().unwrap().len(), 43_383);
        replace_source_projection(&store, &mut counter, current_owner, &source, projection);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, current_owner, &source);
        let begun = begin_snapshot_restore(
            &store,
            &mut counter,
            current_owner,
            &source,
            snapshot_id,
            "cleanup-budget",
            incarnation(35),
        );
        let operation_id = begun.operation.operation_id;
        let mut outcome = start_restore_copy(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        while !outcome.operation.source_eof {
            outcome = copy_restore_batch(
                &store,
                write_context(&store, &mut counter, current_owner),
                CopyRestoreBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap()
            .command;
        }
        let maximum_cleanup = maximum_cleanup_operation(&outcome.operation).unwrap();
        assert_eq!(maximum_cleanup.phase, RestorePhase::Cleaning);
        assert_eq!(
            maximum_cleanup
                .source_cursor
                .as_ref()
                .unwrap()
                .as_str()
                .len(),
            NormalizedRelativePath::MAX_BYTES
        );
        assert!(maximum_cleanup.member_seal.is_some());
        assert!(maximum_cleanup.initialization_digest.is_some());
        let maximum_error = maximum_cleanup.terminal_error.as_ref().unwrap();
        assert_eq!(
            maximum_error.message.len(),
            MAX_RESTORE_TERMINAL_ERROR_BYTES
        );
        assert!(maximum_error.evidence_digest.is_some());
        abort_restore(
            &store,
            write_context(&store, &mut counter, current_owner),
            &AbortRestoreRequest {
                operation_id,
                terminal_error: RestoreTerminalError {
                    kind: RestoreTerminalErrorKind::AbortedByCaller,
                    message: "cancelled".to_owned(),
                    evidence_digest: None,
                },
            },
        )
        .unwrap();
        start_restore_cleanup(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        let first = cleanup_restore_batch(
            &store,
            write_context(&store, &mut counter, current_owner),
            CopyRestoreBatchRequest {
                operation_id,
                limit: MAX_RESTORE_BATCH_MEMBERS,
            },
        )
        .unwrap();
        assert_eq!(first.copied_members, 1);
        let mut cleanup = first;
        while !cleanup.source_eof {
            cleanup = cleanup_restore_batch(
                &store,
                write_context(&store, &mut counter, current_owner),
                CopyRestoreBatchRequest {
                    operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap();
        }
        let cleaned = finish_restore_cleanup(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert_eq!(cleaned.operation.phase, RestorePhase::Cleaned);
    }

    #[test]
    fn finish_cleanup_waits_for_live_manifest_publisher_then_accepts_cleaned() {
        let mut counter = 1_975_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let (_source, _snapshot_id, operation_id, uploading) =
            prepare_cleanup_with_uploading_run_publication(
                &store,
                &mut counter,
                owner_epoch,
                "publisher-pending",
                incarnation(36),
            );

        assert_eq!(
            finish_restore_cleanup(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                RestoreOperationRequest { operation_id },
            ),
            Err(RestoreError::PublicationCleanupPending {
                operation_id: uploading.operation_id,
                phase: PublishPhase::Uploading,
            })
        );
        assert_eq!(
            get_restore(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                operation_id,
            )
            .unwrap()
            .unwrap()
            .phase,
            RestorePhase::Cleaning
        );

        let service = PublicationService::new(&store);
        let aborting = service
            .take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                context: publication_context(&store, &mut counter, owner_epoch),
                expected_operation: uploading,
                observed_now_ms: 2_000_000,
                maximum_clock_skew_ms: 0,
                terminal_error: PublishTerminalError {
                    kind: PublishTerminalErrorKind::ActivityLeaseExpired,
                    message: "publisher lease expired during restore cleanup".to_owned(),
                    evidence_digest: None,
                },
            })
            .unwrap()
            .operation;
        let cleaning = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter, owner_epoch),
                expected_operation: aborting,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        let cleaned_publication = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter, owner_epoch),
                expected_operation: cleaning,
                transition: PublishTransition::FinishCleanup,
            })
            .unwrap()
            .operation;
        assert_eq!(cleaned_publication.phase, PublishPhase::Cleaned);

        let cleaned = finish_restore_cleanup(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert_eq!(cleaned.operation.phase, RestorePhase::Cleaned);
    }

    #[test]
    fn finish_cleanup_rejects_manifest_publisher_identity_drift() {
        let mut counter = 1_980_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let (source, _snapshot_id, operation_id, uploading) =
            prepare_cleanup_with_uploading_run_publication(
                &store,
                &mut counter,
                owner_epoch,
                "publisher-drift",
                incarnation(37),
            );
        let expected_key = operation_key(root(), OperationKind::Publish, uploading.operation_id);
        let mut wrong = uploading;
        wrong.operation_id = operation(0xaa);
        seal_publish_operation(&mut wrong);
        replace_present(
            &store,
            &mut counter,
            owner_epoch,
            MetadataFamily::Operation,
            expected_key,
            wrong.encode().unwrap(),
        );

        assert_eq!(
            finish_restore_cleanup(
                &store,
                write_context(&store, &mut counter, owner_epoch),
                RestoreOperationRequest { operation_id },
            ),
            Err(RestoreError::ManifestBindingMismatch)
        );
        let snapshot = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::SnapshotRef,
            &snapshot_ref_key(root(), source.source_incarnation, SnapshotId::new(42)),
            SnapshotRefRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(snapshot.record.consumer_count, 1);
    }

    #[test]
    fn quarantined_manifest_publisher_quarantines_restore_without_releasing_source() {
        let mut counter = 1_985_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);
        let (source, snapshot_id, operation_id, uploading) =
            prepare_cleanup_with_uploading_run_publication(
                &store,
                &mut counter,
                owner_epoch,
                "publisher-quarantined",
                incarnation(38),
            );
        let service = PublicationService::new(&store);
        let aborting = service
            .take_over_orphaned_publish(TakeOverOrphanedPublishRequest {
                context: publication_context(&store, &mut counter, owner_epoch),
                expected_operation: uploading,
                observed_now_ms: 2_000_000,
                maximum_clock_skew_ms: 0,
                terminal_error: PublishTerminalError {
                    kind: PublishTerminalErrorKind::ActivityLeaseExpired,
                    message: "publisher lease expired during restore cleanup".to_owned(),
                    evidence_digest: None,
                },
            })
            .unwrap()
            .operation;
        let cleaning = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter, owner_epoch),
                expected_operation: aborting,
                transition: PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        let quarantined_publication = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(&store, &mut counter, owner_epoch),
                expected_operation: cleaning,
                transition: PublishTransition::Quarantine {
                    terminal_error: PublishTerminalError {
                        kind: PublishTerminalErrorKind::CleanupFailed,
                        message: "provider cleanup requires reconciliation".to_owned(),
                        evidence_digest: Some([0x91; SHA256_BYTES]),
                    },
                },
            })
            .unwrap()
            .operation;
        assert_eq!(quarantined_publication.phase, PublishPhase::Quarantined);

        let quarantined = finish_restore_cleanup(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert_eq!(quarantined.operation.phase, RestorePhase::Quarantined);
        assert!(quarantined
            .operation
            .terminal_error
            .as_ref()
            .unwrap()
            .evidence_digest
            .is_some());
        let snapshot = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::SnapshotRef,
            &snapshot_ref_key(root(), source.source_incarnation, snapshot_id),
            SnapshotRefRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(snapshot.record.consumer_count, 1);
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::HistoryHold,
            &restore_history_hold_key(root(), operation_id),
        )
        .unwrap()
        .is_some());
    }

    #[test]
    fn abort_wins_ready_race_and_cleanup_releases_every_reference() {
        let mut counter = 2000_u128;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        let current_owner = owner(1);
        activate_root(&store, &mut counter, current_owner);
        let source = seed_source(&store, &mut counter, current_owner, 5);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, current_owner, &source);
        let begun = begin_snapshot_restore(
            &store,
            &mut counter,
            current_owner,
            &source,
            snapshot_id,
            "aborted",
            incarnation(32),
        );
        let operation_id = begun.operation.operation_id;
        drive_to_ready(
            &store,
            &mut counter,
            current_owner,
            operation_id,
            &source.initialization,
        );
        let publication_context = write_context(&store, &mut counter, current_owner);
        abort_restore(
            &store,
            write_context(&store, &mut counter, current_owner),
            &AbortRestoreRequest {
                operation_id,
                terminal_error: RestoreTerminalError {
                    kind: super::super::restore_records::RestoreTerminalErrorKind::AbortedByCaller,
                    message: "cancelled".to_owned(),
                    evidence_digest: None,
                },
            },
        )
        .unwrap();
        assert!(complete_restore(
            &store,
            publication_context,
            RestoreOperationRequest { operation_id },
        )
        .is_err());
        start_restore_cleanup(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        loop {
            let outcome = cleanup_restore_batch(
                &store,
                write_context(&store, &mut counter, current_owner),
                CopyRestoreBatchRequest {
                    operation_id,
                    limit: 2,
                },
            )
            .unwrap();
            if outcome.source_eof {
                break;
            }
        }
        let cleaned = finish_restore_cleanup(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert_eq!(cleaned.operation.phase, RestorePhase::Cleaned);
        assert!(get_visible_workspace_at(
            &store,
            read_context(&store, current_owner),
            &workbench("aborted")
        )
        .unwrap()
        .is_none());
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::HistoryHold,
            &restore_history_hold_key(root(), operation_id),
        )
        .unwrap()
        .is_none());
        let source_revision = read_record(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::ArtifactRevision,
            &artifact_revision_key(root(), source.source_revision),
            ArtifactRevisionRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            source_revision.record.strong_reference_count,
            source.paths.len() as u64 + 1
        );
        let removed_index_key = secondary_index_key(
            root(),
            &QueryFieldId::new("source.class").unwrap(),
            &QueryScalar::String("seed".to_owned()),
            incarnation(32),
            &source.paths[0],
        );
        assert!(store
            .read_at(
                root(),
                placement(),
                current_owner,
                MetadataFamily::SecondaryIndex,
                &removed_index_key,
                store.current_read_version().unwrap(),
            )
            .unwrap()
            .is_none());
        let second_snapshot_id = SnapshotId::new(43);
        mint_snapshot(
            &store,
            write_context(&store, &mut counter, current_owner),
            &MintSnapshotRequest {
                workbench_id: source.source_workbench.clone(),
                snapshot_id: second_snapshot_id,
                alias: None,
                lease_deadline_ms: 1_000_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();
        let replacement = begin_snapshot_restore(
            &store,
            &mut counter,
            current_owner,
            &source,
            second_snapshot_id,
            "aborted",
            incarnation(34),
        );
        assert_eq!(
            replacement.operation.destination_workspace_incarnation_id,
            incarnation(34)
        );
        assert_eq!(replacement.operation.phase, RestorePhase::Preparing);
    }

    #[test]
    fn commit_source_consumer_is_exact_and_released_on_publication() {
        let mut counter = 3000_u128;
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        let current_owner = owner(1);
        activate_root(&store, &mut counter, current_owner);
        let source = seed_source(&store, &mut counter, current_owner, 3);
        let commit_id = source.source_commit_id;
        let initial_commit = read_record(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::Commit,
            &commit_key(root(), commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(initial_commit.record.consumer_count, 1);
        let begun = begin_restore(
            &store,
            write_context(&store, &mut counter, current_owner),
            &bind_operation_identity(BeginRestoreRequest {
                operation_id: OperationId::from_bytes([0; 16]),
                source_workbench_id: source.source_workbench.clone(),
                expected_source_workspace_incarnation_id: source.source_incarnation,
                source: RestoreSourceSelector::Commit(commit_id),
                destination_workbench_id: workbench("from-commit"),
                destination_workspace_incarnation_id: incarnation(33),
                destination_restore_manifest_identity: RestoreManifestIdentity {
                    publication_operation_id: OperationId::from_bytes([0xf2; 16]),
                    artifact_revision_id: revision(0xe4),
                },
                destination_committed_at_unix_seconds: 1,
                restore_manifest: restore_manifest_descriptor(&source.initialization),
            }),
        )
        .unwrap();
        let operation_id = begun.operation.operation_id;
        drive_to_ready(
            &store,
            &mut counter,
            current_owner,
            operation_id,
            &source.initialization,
        );
        let completed = complete_restore(
            &store,
            write_context(&store, &mut counter, current_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        let commit = read_record(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::Commit,
            &commit_key(root(), commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(commit.record.consumer_count, 2);
        assert_eq!(
            commit.record.consumer_epoch.get(),
            initial_commit.record.consumer_epoch.get() + 3
        );
        assert!(commit.record.last_zero_consumer_version.is_none());
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::CommitConsumer,
            &lease_commit_consumer_key(root(), commit_id, operation_id),
        )
        .unwrap()
        .is_none());
        let destination_commit_id = completed
            .command
            .operation
            .destination_commit_receipt()
            .unwrap()
            .destination_commit_id;
        let child = read_record(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::CommitConsumer,
            &child_commit_consumer_key(root(), commit_id, destination_commit_id),
            CommitConsumerRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            child.record.consumer_epoch_at_add,
            commit.record.consumer_epoch
        );
    }

    // ------------------------------------------------------------------
    // Issue #429 scenario: the exact server-driven wire sequence over a
    // real store. Unlike `seed_source`, every source row below is written
    // by the production publication/commit services, never synthesized.
    // ------------------------------------------------------------------

    use nokv_types::{
        BuildCommitPhase, CommitRetirePhase, NormalizedRelativePath, PublishPhase,
        SnapshotAliasName, StagedCleanupState, StagedProviderState,
    };

    use super::super::build_commit_records::CommitManifestCondition;
    use super::super::codec::object_block_key;
    use super::super::commit::{
        BeginBuildCommitRequest, BeginCommitRetirementRequest, BuildCommitStepRequest,
        CommitService, MAX_COMMIT_MEMBER_BATCH_ROWS, MAX_COMMIT_PARENT_BATCH_ROWS,
        MAX_COMMIT_REVISION_BATCH_ROWS, RUN_MANIFEST_PATH,
    };
    use super::super::publication::{
        dependency_owner_digest, manifest_rows_digest, seal_publish_operation,
        staged_object_ledger_digest, BeginPublishRequest, FinalizePublishRequest, ManifestRowInput,
        MarkObjectsUploadedBatchRequest, PublicationContext, PublicationService, PublishedArtifact,
        StageManifestBatchRequest, StageObjectsBatchRequest, StagedObjectUpdate,
        TakeOverOrphanedPublishRequest, TransitionPublishRequest,
    };
    use super::super::publish_operation_records::{
        ArtifactManifestRow, PublishAuthority, PublishClaim, PublishOperationRecord,
        PublishTerminalError, PublishTerminalErrorKind, PublishTransition, StagedObjectRecord,
    };

    fn operation(fill: u8) -> OperationId {
        OperationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn body_digest_uri(body: &[u8]) -> String {
        sha256_digest_uri(Sha256::digest(body).into())
    }

    fn publication_context(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
    ) -> PublicationContext {
        PublicationContext {
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement_generation: placement(),
            owner_epoch,
            request_id: request(counter),
            read_version: store.current_read_version().unwrap(),
        }
    }

    fn single_object_rows(
        artifact_revision_id: ArtifactRevisionId,
        body: &[u8],
    ) -> (Vec<StagedObjectRecord>, Vec<ManifestRowInput>) {
        let staged = vec![StagedObjectRecord {
            artifact_revision_id,
            object_sequence: 0,
            object_key: object_block_key(shard(), root(), artifact_revision_id, 0),
            multipart_upload_id: None,
            expected_length: body.len() as u64,
            expected_digest_uri: body_digest_uri(body),
            provider_state: StagedProviderState::Planned,
            cleanup_state: StagedCleanupState::Owned,
        }];
        let manifest = vec![ManifestRowInput {
            object_index: 0,
            row: ArtifactManifestRow {
                physical_owner_revision_id: artifact_revision_id,
                physical_object_index: 0,
                object_key: staged[0].object_key.clone(),
                logical_offset: 0,
                offset: 0,
                length: staged[0].expected_length,
                digest_uri: staged[0].expected_digest_uri.clone(),
                append_segment: None,
            },
        }];
        (staged, manifest)
    }

    #[allow(clippy::too_many_arguments)]
    fn wire_publish_operation(
        operation_id: OperationId,
        artifact_revision_id: ArtifactRevisionId,
        target_workbench: &WorkbenchId,
        target_incarnation: WorkspaceIncarnationId,
        publish_path: NormalizedRelativePath,
        authority: PublishAuthority,
        owner_epoch: OwnerEpoch,
        staged: &[StagedObjectRecord],
        manifest: &[ManifestRowInput],
    ) -> PublishOperationRecord {
        let mut operation = PublishOperationRecord {
            operation_id,
            identity_digest: [0; SHA256_BYTES],
            initialization_digest: [0; SHA256_BYTES],
            initiating_owner_epoch: owner_epoch,
            activity_deadline_ms: 1_000_000,
            authority,
            workbench_id: target_workbench.clone(),
            workspace_incarnation_id: target_incarnation,
            path: publish_path,
            artifact_revision_id,
            claim: PublishClaim::CreateOnly,
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
            dependency_digest: dependency_owner_digest(&[]).unwrap(),
            cleanup_staged_object_cursor: 0,
            cleanup_manifest_cursor: 0,
            publication_absence_proof: None,
            result: None,
            terminal_error: None,
        };
        seal_publish_operation(&mut operation);
        operation
    }

    /// Drives the five publication wire calls the server executes for one
    /// artifact: Begin, StageObjects, MarkUploaded, StageManifest, Complete.
    fn drive_publish_wire_calls(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        operation: PublishOperationRecord,
        staged: &[StagedObjectRecord],
        manifest: &[ManifestRowInput],
        artifact: PublishedArtifact,
    ) -> super::super::publication::FinalizePublishOutcome {
        let service = PublicationService::new(store);
        let operation = service
            .begin_publish(BeginPublishRequest {
                context: publication_context(store, counter, owner_epoch),
                operation,
            })
            .unwrap()
            .operation;
        let operation = service
            .stage_objects_batch(StageObjectsBatchRequest {
                context: publication_context(store, counter, owner_epoch),
                expected_operation: operation,
                staged_objects: staged.to_vec(),
            })
            .unwrap()
            .operation;
        let uploads = staged
            .iter()
            .cloned()
            .map(|expected| {
                let mut next = expected.clone();
                next.provider_state = StagedProviderState::Uploaded;
                StagedObjectUpdate { expected, next }
            })
            .collect();
        let operation = service
            .mark_objects_uploaded_batch(MarkObjectsUploadedBatchRequest {
                context: publication_context(store, counter, owner_epoch),
                expected_operation: operation,
                staged_object_updates: uploads,
            })
            .unwrap()
            .operation;
        let operation = service
            .stage_manifest_batch(StageManifestBatchRequest {
                context: publication_context(store, counter, owner_epoch),
                expected_operation: operation,
                manifest_rows: manifest.to_vec(),
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap()
            .operation;
        let operation = service
            .transition_publish(TransitionPublishRequest {
                context: publication_context(store, counter, owner_epoch),
                expected_operation: operation,
                transition: PublishTransition::BeginFinalization,
            })
            .unwrap()
            .operation;
        service
            .finalize_publish(FinalizePublishRequest {
                context: publication_context(store, counter, owner_epoch),
                artifact,
                expected_operation: operation,
                dependency_owner_revision_ids: Vec::new(),
            })
            .unwrap()
    }

    /// Publishes one text file through the five production wire calls under
    /// the Visible authority, exactly as workbench_put_file does.
    #[allow(clippy::too_many_arguments)]
    fn publish_text_file(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        target_workbench: &WorkbenchId,
        target_incarnation: WorkspaceIncarnationId,
        path: &str,
        body: &[u8],
        file_revision: ArtifactRevisionId,
        publish_operation_id: OperationId,
    ) {
        let (staged, manifest) = single_object_rows(file_revision, body);
        let publish = wire_publish_operation(
            publish_operation_id,
            file_revision,
            target_workbench,
            target_incarnation,
            NormalizedRelativePath::new(path).unwrap(),
            PublishAuthority::Visible,
            owner_epoch,
            &staged,
            &manifest,
        );
        let manifest_digest_uri = sha256_digest_uri(publish.manifest_seal);
        drive_publish_wire_calls(
            store,
            counter,
            owner_epoch,
            publish,
            &staged,
            &manifest,
            PublishedArtifact {
                logical_size: body.len() as u64,
                body_digest_uri: body_digest_uri(body),
                manifest_digest_uri,
                content_type: "text/plain".to_owned(),
                producer: Some("issue-429".to_owned()),
                manifest_id: None,
                typed_index_projection: TypedProjection::empty().encode().unwrap(),
            },
        );
    }

    /// Drives the real commit wire sequence: begin_build, the CommitStaging
    /// run-manifest publication, then build/seal/attach/sealing/finalize.
    #[allow(clippy::too_many_arguments)]
    fn drive_commit_wire_calls(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source_workbench: &WorkbenchId,
        source_incarnation: WorkspaceIncarnationId,
        commit_id: CommitId,
        commit_operation_id: OperationId,
        run_manifest_publish_operation_id: OperationId,
        tree_manifest_revision: ArtifactRevisionId,
        run_manifest_body: &[u8],
    ) {
        let commits = CommitService::new(store);
        let existing_head = read_record(
            store,
            write_context(store, counter, owner_epoch),
            MetadataFamily::WorkbenchCommitHead,
            &workbench_commit_head_key(root(), source_incarnation),
            WorkbenchCommitHeadRecord::decode,
        )
        .unwrap();
        let existing_run_manifest = get_visible_path_at(
            store,
            read_context(store, owner_epoch),
            source_workbench,
            &NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap(),
        )
        .unwrap();
        let run_manifest_condition = match existing_run_manifest {
            Some(path) => CommitManifestCondition::ReplaceOnly {
                expected_generation: path.generation,
            },
            None => CommitManifestCondition::CreateOnly,
        };
        let parent_commits = existing_head
            .as_ref()
            .map(|head| vec![head.record.commit_id])
            .unwrap_or_default();
        let begun = commits
            .begin_build(BeginBuildCommitRequest {
                context: write_context(store, counter, owner_epoch),
                operation_id: commit_operation_id,
                workbench_id: source_workbench.clone(),
                expected_source_workspace_incarnation_id: source_incarnation,
                commit_id,
                content_digest_uri: format!("sha256:{:064x}", 0x429),
                manifest_digest_uri: body_digest_uri(run_manifest_body),
                projection_input_digest: [0; SHA256_BYTES],
                tree_manifest_revision_id: tree_manifest_revision,
                replace: existing_head.is_some(),
                run_manifest_condition,
                committed_at_unix_seconds: 1_700_000_000,
                expected_head_generation: existing_head
                    .as_ref()
                    .map(|head| head.record.head_generation),
                producer: None,
                lineage_projection: Vec::new(),
                parent_commits,
            })
            .unwrap();
        assert!(begun.operation.commit_staged_run_manifest.is_none());

        let (staged, manifest) = single_object_rows(tree_manifest_revision, run_manifest_body);
        let mut publish = wire_publish_operation(
            run_manifest_publish_operation_id,
            tree_manifest_revision,
            source_workbench,
            source_incarnation,
            NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap(),
            PublishAuthority::CommitStaging {
                commit_operation_id,
            },
            owner_epoch,
            &staged,
            &manifest,
        );
        publish.claim = match run_manifest_condition {
            CommitManifestCondition::CreateOnly => PublishClaim::CreateOnly,
            CommitManifestCondition::ReplaceOnly {
                expected_generation,
            } => PublishClaim::ReplaceOnly {
                expected_generation,
            },
        };
        seal_publish_operation(&mut publish);
        let manifest_digest_uri = sha256_digest_uri(publish.manifest_seal);
        drive_publish_wire_calls(
            store,
            counter,
            owner_epoch,
            publish,
            &staged,
            &manifest,
            PublishedArtifact {
                logical_size: run_manifest_body.len() as u64,
                body_digest_uri: body_digest_uri(run_manifest_body),
                manifest_digest_uri,
                content_type: "application/json".to_owned(),
                producer: None,
                manifest_id: None,
                typed_index_projection: TypedProjection::empty().encode().unwrap(),
            },
        );

        loop {
            let outcome = commits
                .build_members(BuildCommitStepRequest {
                    context: write_context(store, counter, owner_epoch),
                    operation_id: commit_operation_id,
                    limit: MAX_COMMIT_MEMBER_BATCH_ROWS,
                })
                .unwrap();
            if outcome.operation.members_complete {
                break;
            }
        }
        loop {
            let outcome = commits
                .seal_revisions(BuildCommitStepRequest {
                    context: write_context(store, counter, owner_epoch),
                    operation_id: commit_operation_id,
                    limit: MAX_COMMIT_REVISION_BATCH_ROWS,
                })
                .unwrap();
            if outcome.operation.revisions_complete {
                break;
            }
        }
        commits
            .attach_parents(BuildCommitStepRequest {
                context: write_context(store, counter, owner_epoch),
                operation_id: commit_operation_id,
                limit: MAX_COMMIT_PARENT_BATCH_ROWS,
            })
            .unwrap();
        commits
            .begin_sealing(
                write_context(store, counter, owner_epoch),
                commit_operation_id,
            )
            .unwrap();
        let completed = commits
            .finalize_build(
                write_context(store, counter, owner_epoch),
                commit_operation_id,
            )
            .unwrap();
        assert_eq!(completed.operation.phase, BuildCommitPhase::Complete);
    }

    /// Drives one snapshot-source restore end to end exactly as the client
    /// workflow does: begin, start copy, bounded copy batches, seal, the
    /// RestoreStaging restore-manifest publication, initialization, complete.
    #[allow(clippy::too_many_arguments)]
    fn drive_snapshot_restore_to_visible(
        store: &MetaShard,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source_workbench: &WorkbenchId,
        source_incarnation: WorkspaceIncarnationId,
        snapshot_id: SnapshotId,
        destination_workbench: &WorkbenchId,
        destination_incarnation: WorkspaceIncarnationId,
        restore_manifest_publish_operation_id: OperationId,
        restore_manifest_revision: ArtifactRevisionId,
        copy_limit: usize,
    ) -> CompleteRestoreOutcome {
        let restore_manifest_body: &[u8] = br#"{"schema":"nokv.workbench.restore_manifest.v1"}"#;
        let begun = begin_restore(
            store,
            write_context(store, counter, owner_epoch),
            &bind_operation_identity(BeginRestoreRequest {
                operation_id: OperationId::from_bytes([0; 16]),
                source_workbench_id: source_workbench.clone(),
                expected_source_workspace_incarnation_id: source_incarnation,
                source: RestoreSourceSelector::Snapshot(snapshot_id),
                destination_workbench_id: destination_workbench.clone(),
                destination_workspace_incarnation_id: destination_incarnation,
                destination_restore_manifest_identity: RestoreManifestIdentity {
                    publication_operation_id: restore_manifest_publish_operation_id,
                    artifact_revision_id: restore_manifest_revision,
                },
                destination_committed_at_unix_seconds: 1,
                restore_manifest: RestoreManifestDescriptor {
                    body_digest_uri: body_digest_uri(restore_manifest_body),
                    logical_size: restore_manifest_body.len() as u64,
                    content_type: "application/json".to_owned(),
                },
            }),
        )
        .unwrap();
        let restore_operation_id = begun.operation.operation_id;

        start_restore_copy(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: restore_operation_id,
            },
        )
        .unwrap();
        loop {
            let outcome = copy_restore_batch(
                store,
                write_context(store, counter, owner_epoch),
                CopyRestoreBatchRequest {
                    operation_id: restore_operation_id,
                    limit: copy_limit,
                },
            )
            .unwrap_or_else(|error| panic!("copy_restore_batch failed: {error}"));
            if outcome.source_eof {
                break;
            }
        }
        seal_restore_source(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: restore_operation_id,
            },
        )
        .unwrap_or_else(|error| panic!("seal_restore_source failed: {error}"));

        let sealed = get_restore(
            store,
            write_context(store, counter, owner_epoch),
            restore_operation_id,
        )
        .unwrap()
        .unwrap();
        let binding = destination_binding_for_test(&sealed, 0xd9);
        bind_restore_destination(
            store,
            write_context(store, counter, owner_epoch),
            &BindRestoreDestinationRequest {
                operation_id: restore_operation_id,
                binding: binding.clone(),
            },
        )
        .unwrap_or_else(|error| panic!("bind_restore_destination failed: {error}"));

        let run_manifest_body: &[u8] =
            br#"{"schema":"nokv.workbench.run_manifest.v1","restored":true}"#;
        let mut restore_publication_revision = None;
        for (path, identity, body) in [
            (
                RUN_MANIFEST_PATH,
                binding.run_manifest_identity,
                run_manifest_body,
            ),
            (
                RESTORE_MANIFEST_PATH,
                binding.restore_manifest_identity,
                restore_manifest_body,
            ),
        ] {
            let (staged, manifest) = single_object_rows(identity.artifact_revision_id, body);
            let publish = wire_publish_operation(
                identity.publication_operation_id,
                identity.artifact_revision_id,
                destination_workbench,
                destination_incarnation,
                NormalizedRelativePath::new(path).unwrap(),
                PublishAuthority::RestoreStaging {
                    restore_operation_id,
                },
                owner_epoch,
                &staged,
                &manifest,
            );
            let manifest_digest_uri = sha256_digest_uri(publish.manifest_seal);
            let published = drive_publish_wire_calls(
                store,
                counter,
                owner_epoch,
                publish,
                &staged,
                &manifest,
                PublishedArtifact {
                    logical_size: body.len() as u64,
                    body_digest_uri: body_digest_uri(body),
                    manifest_digest_uri,
                    content_type: "application/json".to_owned(),
                    producer: None,
                    manifest_id: None,
                    typed_index_projection: TypedProjection::empty().encode().unwrap(),
                },
            );
            if path == RESTORE_MANIFEST_PATH {
                restore_publication_revision = Some(published.result.workspace_revision);
            }
        }

        apply_restore_initialization(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: restore_operation_id,
            },
        )
        .unwrap_or_else(|error| panic!("apply_restore_initialization failed: {error}"));
        loop {
            let built = build_restore_commit_members(
                store,
                write_context(store, counter, owner_epoch),
                RestoreClosureBatchRequest {
                    operation_id: restore_operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap_or_else(|error| panic!("build_restore_commit_members failed: {error}"));
            if built.members_complete {
                break;
            }
        }
        loop {
            let sealed = seal_restore_commit_revisions(
                store,
                write_context(store, counter, owner_epoch),
                RestoreClosureBatchRequest {
                    operation_id: restore_operation_id,
                    limit: MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .unwrap_or_else(|error| panic!("seal_restore_commit_revisions failed: {error}"));
            if sealed.ready {
                break;
            }
        }
        let completed = complete_restore(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: restore_operation_id,
            },
        )
        .unwrap_or_else(|error| panic!("complete_restore failed: {error}"));
        assert_eq!(
            completed.result.destination_workspace_revision.get(),
            restore_publication_revision.unwrap().get()
        );
        completed
    }

    /// Issue #429: create -> put_file(input/note.txt) -> commit ->
    /// snapshot("frozen") -> restore into a new destination workbench.
    ///
    /// Every step runs the same production service the server executor
    /// drives, in the same order. The restore must complete and the
    /// destination must expose input/note.txt at the source revision.
    #[test]
    fn snapshot_restore_after_real_commit_completes_and_copies_source_paths() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);

        // workbench_create
        let source_workbench = workbench("minimal");
        let source_incarnation = incarnation(10);
        create_visible_workspace(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &source_workbench,
            source_incarnation,
        )
        .unwrap();

        // workbench_put_file: section "input", path "note.txt", small text.
        let note_path = NormalizedRelativePath::new("input/note.txt").unwrap();
        let note_body: &[u8] = b"issue 429 reproduction note";
        let note_revision = revision(0x11);
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            "input/note.txt",
            note_body,
            note_revision,
            operation(0x21),
        );

        // workbench_commit: run-manifest CommitStaging publish + full build.
        let run_manifest_body: &[u8] =
            br#"{"schema":"nokv.workbench.run_manifest.v1","paths":["input/note.txt"]}"#;
        let source_commit_id = CommitId::from_bytes([0x42; SHA256_BYTES]);
        drive_commit_wire_calls(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            source_commit_id,
            operation(0x31),
            operation(0x32),
            revision(0x12),
            run_manifest_body,
        );

        // workbench_snapshot: alias "frozen", one-day lease.
        let snapshot_id = SnapshotId::new(429);
        mint_snapshot(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &MintSnapshotRequest {
                workbench_id: source_workbench.clone(),
                snapshot_id,
                alias: Some(SnapshotAliasName::new("frozen").unwrap()),
                lease_deadline_ms: 86_400_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();

        // workbench_restore into destination "minimal-restored".
        let destination_workbench = workbench("minimal-restored");
        let destination_incarnation = incarnation(30);
        let completed = drive_snapshot_restore_to_visible(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            snapshot_id,
            &destination_workbench,
            destination_incarnation,
            operation(0x41),
            revision(0x13),
            MAX_RESTORE_BATCH_MEMBERS,
        );
        assert_eq!(
            completed.result.destination_workspace_incarnation_id,
            destination_incarnation
        );
        // The snapshot closure excludes the commit-produced run manifest,
        // so exactly one member (input/note.txt) is copied.
        assert_eq!(completed.result.member_count, 1);
        assert_eq!(
            completed.result.destination_workspace_revision,
            WorkspaceRevision::new(1)
        );
        let receipt = completed
            .command
            .operation
            .destination_commit_receipt()
            .unwrap();
        let destination_commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), receipt.destination_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        let source_commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), source_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        let RestoreCommitProvenance::V5(provenance) =
            &completed.command.operation.commit_provenance
        else {
            unreachable!();
        };
        let binding = provenance.destination_binding.as_ref().unwrap();
        let manifests = binding.manifests.as_ref().unwrap();
        assert_eq!(
            destination_commit.record.content_digest_uri,
            binding.effective_content_digest_uri
        );
        assert_eq!(
            destination_commit.record.manifest_digest_uri,
            source_commit.record.manifest_digest_uri
        );
        assert_eq!(
            destination_commit.record.tree_manifest_revision_id,
            manifests.run_manifest.artifact_revision_id
        );
        assert_eq!(
            destination_commit.record.parent_commits,
            vec![source_commit_id]
        );
        assert_eq!(source_commit.record.consumer_count, 3);
        let source_child = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &child_commit_consumer_key(root(), source_commit_id, receipt.destination_commit_id),
            CommitConsumerRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            source_child.record.consumer_epoch_at_add,
            source_commit.record.consumer_epoch
        );
        assert_eq!(
            receipt.destination_commit_id,
            workbench_commit_identity_for_test(
                &destination_workbench,
                &destination_commit.record.content_digest_uri,
                &destination_commit.record.manifest_digest_uri,
            )
        );

        // The destination is visible and contains the restored note.
        let destination = get_visible_workspace_at(
            &store,
            read_context(&store, owner_epoch),
            &destination_workbench,
        )
        .unwrap()
        .expect("restored destination workbench is visible");
        assert_eq!(destination.incarnation_id, destination_incarnation);
        let restored_note = get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &destination_workbench,
            &note_path,
        )
        .unwrap()
        .expect("restored destination contains input/note.txt");
        assert_eq!(restored_note.artifact_revision_id, note_revision);
        assert_eq!(restored_note.body_digest_uri, body_digest_uri(note_body));
        assert_eq!(restored_note.logical_size, note_body.len() as u64);

        // Source-owned provenance is not copied. The restore publishes a new
        // destination-owned run manifest and restore manifest instead.
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &destination_workbench,
            &NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap(),
        )
        .unwrap()
        .is_some());
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &destination_workbench,
            &NormalizedRelativePath::new(RESTORE_MANIFEST_PATH).unwrap(),
        )
        .unwrap()
        .is_some());
    }

    /// The run-manifest row sorts between input/ and output/, so a one-row
    /// copy limit forces it onto page boundaries of its own; the skip must
    /// keep the cursor advancing and the copy/seal closures identical.
    #[test]
    fn snapshot_restore_skips_run_manifest_across_page_boundaries() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);

        let source_workbench = workbench("paged");
        let source_incarnation = incarnation(10);
        create_visible_workspace(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &source_workbench,
            source_incarnation,
        )
        .unwrap();
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            "input/a.txt",
            b"first",
            revision(0x11),
            operation(0x21),
        );
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            "output/z.txt",
            b"last",
            revision(0x12),
            operation(0x22),
        );
        drive_commit_wire_calls(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            CommitId::from_bytes([0x43; SHA256_BYTES]),
            operation(0x31),
            operation(0x32),
            revision(0x13),
            br#"{"schema":"nokv.workbench.run_manifest.v1","paths":["input/a.txt","output/z.txt"]}"#,
        );
        let snapshot_id = SnapshotId::new(430);
        mint_snapshot(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &MintSnapshotRequest {
                workbench_id: source_workbench.clone(),
                snapshot_id,
                alias: None,
                lease_deadline_ms: 86_400_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();

        let destination_workbench = workbench("paged-restored");
        let destination_incarnation = incarnation(30);
        let completed = drive_snapshot_restore_to_visible(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            snapshot_id,
            &destination_workbench,
            destination_incarnation,
            operation(0x41),
            revision(0x14),
            1,
        );
        assert_eq!(completed.result.member_count, 2);
        for (path, file_revision) in [
            ("input/a.txt", revision(0x11)),
            ("output/z.txt", revision(0x12)),
        ] {
            let restored = get_visible_path_at(
                &store,
                read_context(&store, owner_epoch),
                &destination_workbench,
                &NormalizedRelativePath::new(path).unwrap(),
            )
            .unwrap()
            .unwrap_or_else(|| panic!("restored destination is missing {path}"));
            assert_eq!(restored.artifact_revision_id, file_revision);
        }
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &destination_workbench,
            &NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap(),
        )
        .unwrap()
        .is_some());
    }

    /// A restored workbench must remain snapshot-restorable after ordinary
    /// dirty mutations without an intervening recommit. Its restore-created
    /// head supplies lineage while the snapshot closure supplies current
    /// content.
    #[test]
    fn snapshot_restore_chains_from_a_restored_workbench() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);

        let first = workbench("chain-a");
        let first_incarnation = incarnation(10);
        create_visible_workspace(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &first,
            first_incarnation,
        )
        .unwrap();
        let note_revision = revision(0x11);
        let note_body: &[u8] = b"chained restore evidence";
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &first,
            first_incarnation,
            "input/note.txt",
            note_body,
            note_revision,
            operation(0x21),
        );
        let first_commit_id = CommitId::from_bytes([0x45; SHA256_BYTES]);
        drive_commit_wire_calls(
            &store,
            &mut counter,
            owner_epoch,
            &first,
            first_incarnation,
            first_commit_id,
            operation(0x31),
            operation(0x32),
            revision(0x12),
            br#"{"schema":"nokv.workbench.run_manifest.v1","paths":["input/note.txt"]}"#,
        );
        let first_snapshot = SnapshotId::new(432);
        mint_snapshot(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &MintSnapshotRequest {
                workbench_id: first.clone(),
                snapshot_id: first_snapshot,
                alias: None,
                lease_deadline_ms: 86_400_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();

        let second = workbench("chain-b");
        let second_incarnation = incarnation(30);
        let first_completed = drive_snapshot_restore_to_visible(
            &store,
            &mut counter,
            owner_epoch,
            &first,
            first_incarnation,
            first_snapshot,
            &second,
            second_incarnation,
            operation(0x41),
            revision(0x13),
            MAX_RESTORE_BATCH_MEMBERS,
        );

        let second_base_commit_id = first_completed
            .command
            .operation
            .destination_commit_receipt()
            .unwrap()
            .destination_commit_id;

        // Mutate the restored workbench without recommitting: publish the
        // renamed destination and another dirty path, then remove the old
        // source name. The existing restore-created head remains the snapshot
        // lineage authority while the snapshot closure is deliberately dirty.
        let renamed_revision = revision(0x14);
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &second,
            second_incarnation,
            "renamed/note.txt",
            note_body,
            renamed_revision,
            operation(0x51),
        );
        let dirty_revision = revision(0x15);
        let dirty_body: &[u8] = b"dirty after restore without recommit";
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &second,
            second_incarnation,
            "output/dirty.txt",
            dirty_body,
            dirty_revision,
            operation(0x52),
        );
        remove_path(
            &store,
            RemovePathRequest {
                context: write_context(&store, &mut counter, owner_epoch),
                workbench_id: second.clone(),
                path: NormalizedRelativePath::new("input/note.txt").unwrap(),
                expected_generation: Generation::new(1).unwrap(),
            },
        )
        .unwrap();
        let second_snapshot = SnapshotId::new(433);
        mint_snapshot(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &MintSnapshotRequest {
                workbench_id: second.clone(),
                snapshot_id: second_snapshot,
                alias: None,
                lease_deadline_ms: 86_400_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();

        let third = workbench("chain-c");
        let third_incarnation = incarnation(50);
        let completed = drive_snapshot_restore_to_visible(
            &store,
            &mut counter,
            owner_epoch,
            &second,
            second_incarnation,
            second_snapshot,
            &third,
            third_incarnation,
            operation(0x61),
            revision(0x16),
            1,
        );
        assert_eq!(
            completed.command.operation.source_matches_base_commit,
            Some(false)
        );
        assert_eq!(completed.result.member_count, 2);
        let restored_note = get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &third,
            &NormalizedRelativePath::new("renamed/note.txt").unwrap(),
        )
        .unwrap()
        .expect("chained destination contains renamed/note.txt");
        assert_eq!(restored_note.artifact_revision_id, renamed_revision);
        assert_eq!(restored_note.body_digest_uri, body_digest_uri(note_body));
        let restored_dirty = get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &third,
            &NormalizedRelativePath::new("output/dirty.txt").unwrap(),
        )
        .unwrap()
        .expect("chained destination contains dirty output");
        assert_eq!(restored_dirty.artifact_revision_id, dirty_revision);
        assert_eq!(restored_dirty.body_digest_uri, body_digest_uri(dirty_body));
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &third,
            &NormalizedRelativePath::new("input/note.txt").unwrap(),
        )
        .unwrap()
        .is_none());
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &third,
            &NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap(),
        )
        .unwrap()
        .is_some());
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &third,
            &NormalizedRelativePath::new(RESTORE_MANIFEST_PATH).unwrap(),
        )
        .unwrap()
        .is_some());

        let third_receipt = completed
            .command
            .operation
            .destination_commit_receipt()
            .unwrap();
        let third_commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), third_receipt.destination_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        let second_base_commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), second_base_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            third_commit.record.parent_commits,
            vec![second_base_commit_id]
        );
        assert_ne!(
            third_commit.record.content_digest_uri,
            second_base_commit.record.content_digest_uri
        );
        let third_head = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::WorkbenchCommitHead,
            &workbench_commit_head_key(root(), third_incarnation),
            WorkbenchCommitHeadRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            third_head.record.commit_id,
            third_receipt.destination_commit_id
        );
        assert_eq!(
            third_head.record.head_generation,
            Generation::new(1).unwrap()
        );

        // A, B, and C retain independent mutable path state.
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &first,
            &NormalizedRelativePath::new("input/note.txt").unwrap(),
        )
        .unwrap()
        .is_some());
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &second,
            &NormalizedRelativePath::new("input/note.txt").unwrap(),
        )
        .unwrap()
        .is_none());
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &third,
            third_incarnation,
            "output/c-only.txt",
            b"only-c",
            revision(0x17),
            operation(0x71),
        );
        assert!(get_visible_path_at(
            &store,
            read_context(&store, owner_epoch),
            &second,
            &NormalizedRelativePath::new("output/c-only.txt").unwrap(),
        )
        .unwrap()
        .is_none());

        retire_snapshot(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &RetireSnapshotRequest {
                workbench_id: second.clone(),
                selector: SnapshotSelector::Id(second_snapshot),
                retire_annotation: None,
            },
        )
        .unwrap();
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &snapshot_commit_consumer_key(root(), second_base_commit_id, second_snapshot),
        )
        .unwrap()
        .is_none());
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &child_commit_consumer_key(root(), first_commit_id, second_base_commit_id),
        )
        .unwrap()
        .is_some());
        let second_to_third_child = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::CommitConsumer,
            &child_commit_consumer_key(
                root(),
                second_base_commit_id,
                third_receipt.destination_commit_id,
            ),
            CommitConsumerRecord::decode,
        )
        .unwrap()
        .expect("C must retain B's base commit after B's snapshot retires");
        let second_after_snapshot_retire = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), second_base_commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(second_after_snapshot_retire.record.consumer_count, 2);
        assert_eq!(
            second_after_snapshot_retire.record.consumer_epoch.get(),
            second_to_third_child.record.consumer_epoch_at_add.get() + 1
        );
        assert!(second_after_snapshot_retire
            .record
            .last_zero_consumer_version
            .is_none());
    }

    /// A genuinely undecodable stored projection must surface as a typed
    /// error naming the member path and family, never a raw codec string.
    #[test]
    fn corrupt_source_projection_reports_member_path_and_family() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);

        let source_workbench = workbench("corrupt");
        let source_incarnation = incarnation(10);
        create_visible_workspace(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &source_workbench,
            source_incarnation,
        )
        .unwrap();
        let bad_path = nokv_types::NormalizedRelativePath::new("input/bad.txt").unwrap();
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            bad_path.as_str(),
            b"bad",
            revision(0x11),
            operation(0x21),
        );
        drive_commit_wire_calls(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            CommitId::from_bytes([0x43; SHA256_BYTES]),
            operation(0x31),
            operation(0x32),
            revision(0x12),
            br#"{"schema":"nokv.workbench.run_manifest.v1","paths":["input/bad.txt"]}"#,
        );
        let context = write_context(&store, &mut counter, owner_epoch);
        let loaded = load_path(&store, context, source_incarnation, &bad_path)
            .unwrap()
            .unwrap();
        let mut bad_entry = loaded.record;
        bad_entry.typed_index_projection = vec![0xff, 0xff];
        let key = path_current_key(root(), source_incarnation, &bad_path);
        let command = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch,
            request_id: context.request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: context.read_version,
            root_fence_action: RootFenceAction::RequireActive,
            predicates: vec![CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: key.clone(),
                expected: Some(loaded.payload),
            }],
            mutations: vec![CommandMutation::Put {
                family: MetadataFamily::PathCurrent,
                key: key.clone(),
                value: bad_entry.encode().unwrap(),
            }],
            history_projection: vec![HistoryProjection {
                family: MetadataFamily::PathCurrent,
                key,
            }],
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&command).unwrap();
        let snapshot_id = SnapshotId::new(431);
        mint_snapshot(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &MintSnapshotRequest {
                workbench_id: source_workbench.clone(),
                snapshot_id,
                alias: None,
                lease_deadline_ms: 86_400_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();

        let begun = begin_restore(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &bind_operation_identity(BeginRestoreRequest {
                operation_id: OperationId::from_bytes([0; 16]),
                source_workbench_id: source_workbench.clone(),
                expected_source_workspace_incarnation_id: source_incarnation,
                source: RestoreSourceSelector::Snapshot(snapshot_id),
                destination_workbench_id: workbench("corrupt-restored"),
                destination_workspace_incarnation_id: incarnation(30),
                destination_restore_manifest_identity: RestoreManifestIdentity {
                    publication_operation_id: operation(0x15),
                    artifact_revision_id: revision(0x14),
                },
                destination_committed_at_unix_seconds: 1,
                restore_manifest: RestoreManifestDescriptor {
                    body_digest_uri: format!("sha256:{:064x}", 0xcafe),
                    logical_size: 2,
                    content_type: "application/json".to_owned(),
                },
            }),
        )
        .unwrap();
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        let error = copy_restore_batch(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            CopyRestoreBatchRequest {
                operation_id: begun.operation.operation_id,
                limit: MAX_RESTORE_BATCH_MEMBERS,
            },
        )
        .unwrap_err();
        let RestoreError::CorruptSourceMember {
            family,
            path,
            detail,
        } = &error
        else {
            panic!("expected CorruptSourceMember, got {error:?}");
        };
        assert_eq!(*family, "PathCurrent");
        assert_eq!(path, "input/bad.txt");
        assert!(detail.contains("value format version"), "detail: {detail}");
        assert!(error
            .to_string()
            .starts_with("restore source member input/bad.txt has a corrupt PathCurrent record"));
    }

    /// A sealed source commit's raw closure includes its run manifest, while
    /// materialization deliberately omits that source-owned projection.
    #[test]
    fn commit_source_restore_seals_but_does_not_materialize_the_source_run_manifest() {
        let mut counter = 0_u128;
        let owner_epoch = owner(1);
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store, &mut counter, owner_epoch);

        let source_workbench = workbench("commit-source");
        let source_incarnation = incarnation(10);
        create_visible_workspace(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &source_workbench,
            source_incarnation,
        )
        .unwrap();
        publish_text_file(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            "input/note.txt",
            b"commit source",
            revision(0x11),
            operation(0x21),
        );
        let commit_id = CommitId::from_bytes([0x44; SHA256_BYTES]);
        drive_commit_wire_calls(
            &store,
            &mut counter,
            owner_epoch,
            &source_workbench,
            source_incarnation,
            commit_id,
            operation(0x31),
            operation(0x32),
            revision(0x12),
            br#"{"schema":"nokv.workbench.run_manifest.v1","paths":["input/note.txt"]}"#,
        );

        let begun = begin_restore(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            &bind_operation_identity(BeginRestoreRequest {
                operation_id: OperationId::from_bytes([0; 16]),
                source_workbench_id: source_workbench.clone(),
                expected_source_workspace_incarnation_id: source_incarnation,
                source: RestoreSourceSelector::Commit(commit_id),
                destination_workbench_id: workbench("commit-restored"),
                destination_workspace_incarnation_id: incarnation(30),
                destination_restore_manifest_identity: RestoreManifestIdentity {
                    publication_operation_id: operation(0x15),
                    artifact_revision_id: revision(0x14),
                },
                destination_committed_at_unix_seconds: 1,
                restore_manifest: RestoreManifestDescriptor {
                    body_digest_uri: format!("sha256:{:064x}", 0xcafe),
                    logical_size: 2,
                    content_type: "application/json".to_owned(),
                },
            }),
        )
        .unwrap();
        start_restore_copy(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        let copied = copy_restore_batch(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            CopyRestoreBatchRequest {
                operation_id: begun.operation.operation_id,
                limit: MAX_RESTORE_BATCH_MEMBERS,
            },
        )
        .unwrap();
        assert!(copied.source_eof);
        assert_eq!(copied.copied_members, 1);
        assert_eq!(copied.command.operation.source_member_count, 2);
        assert_eq!(copied.command.operation.next_member_sequence, 1);

        let source_commit = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::Commit,
            &commit_key(root(), commit_id),
            CommitRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            (
                copied.command.operation.source_member_count,
                copied.command.operation.source_member_rolling_digest,
            ),
            (
                source_commit.record.member_count,
                source_commit.record.member_digest,
            ),
            "commit-source raw restore closure must exactly match the immutable commit",
        );

        let sealed = seal_restore_source(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            RestoreOperationRequest {
                operation_id: begun.operation.operation_id,
            },
        )
        .unwrap();
        assert_eq!(sealed.operation.phase, RestorePhase::SourceSealed);
        assert_eq!(
            sealed.operation.source_member_seal,
            Some(source_commit.record.member_digest)
        );
        let materialized = read_record(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::RestoreMember,
            &restore_member_key(root(), begun.operation.operation_id, 0),
            RestoreMemberRecord::decode,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            materialized.record.destination_path.as_str(),
            "input/note.txt"
        );
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, owner_epoch),
            MetadataFamily::RestoreMember,
            &restore_member_key(root(), begun.operation.operation_id, 1),
        )
        .unwrap()
        .is_none());
    }
}
