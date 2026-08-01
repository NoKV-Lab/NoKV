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
    GcClaimState, HistoryHoldState, OperationId, OperationKind, ReadVersion, ReferenceEpoch,
    RestorePhase, RevisionState, SnapshotId, SnapshotState, WorkbenchId, WorkspaceIncarnationId,
    WorkspaceRevision, WorkspaceState, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::codec::{
    artifact_revision_key, commit_key, commit_member_prefix, decode_commit_member_key,
    decode_path_current_key, gc_candidate_key, lease_commit_consumer_key, operation_key,
    path_child_prefix, path_current_key, path_revision_ref_key, restore_history_hold_key,
    restore_member_key, restore_member_prefix, snapshot_ref_key, workspace_current_key, SCHEMA_ID,
};
use super::commit_records::{
    advance_commit_member_rolling_digest, commit_member_row_digest, CommitConsumerRecord,
    CommitMemberRecord, CommitRecord, CommitRecordError,
};
use super::engine::{
    AgentMetadataError, AgentMetadataStore, CommandMutation, CommandPredicate, EventProjection,
    HistoryProjection, MetadataCommand, MetadataFamily, MetadataScanItem, RootFenceAction,
};
use super::namespace::RootWriteContext;
use super::publication_records::{
    ArtifactRevisionRecord, GcCandidateRecord, PathEntry, PublicationRecordCodecError,
    RevisionRefRecord, WorkspaceRecord,
};
use super::query::change_event_projection;
use super::query_records::{
    secondary_index_key, ChangeEventKind, ChangeEventRecord, QueryFieldId, QueryRecordError,
    QueryScalar, SecondaryIndexRecord, TypedProjection,
};
use super::restore_records::{
    RestoreManifestDescriptor, RestoreMemberRecord, RestoreOperationRecord, RestoreRecordError,
    RestoreResult, RestoreSource, RestoreTerminalError, RestoreTransition,
};
use super::snapshot_records::{HistoryHoldRecord, SnapshotRecordError, SnapshotRefRecord};

/// Maximum source rows copied by one metadata command.
///
/// Each row may require one path, reference, member, and revision predicate /
/// mutation pair. Forty-eight rows remain below the engine's independent
/// 256-item bounds even when every row names a distinct revision.
pub const MAX_RESTORE_BATCH_MEMBERS: usize = 48;
/// Exact Workbench initialization path published before publication.
pub const RESTORE_MANIFEST_PATH: &str = "metadata/restore_manifest.json";
const RESTORE_OUTCOME_FORMAT: u8 = 1;
const MAX_COMMAND_ITEMS: usize = 256;

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
    /// the object plane and is never copied into Holt.
    pub restore_manifest: PathEntry,
}

/// Atomic creation request for a destination workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginRestoreRequest {
    pub source_workbench_id: WorkbenchId,
    pub expected_source_workspace_incarnation_id: WorkspaceIncarnationId,
    pub source: RestoreSourceSelector,
    pub destination_workbench_id: WorkbenchId,
    pub destination_workspace_incarnation_id: WorkspaceIncarnationId,
    pub restore_manifest: RestoreManifestDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreOperationRequest {
    pub operation_id: OperationId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopyRestoreBatchRequest {
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

/// Final publication result retained in the terminal operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteRestoreOutcome {
    pub command: RestoreCommandOutcome,
    pub result: RestoreResult,
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
    RestoreManifestBindingMismatch {
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
    Engine(AgentMetadataError),
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
            Self::RestoreManifestBindingMismatch { operation_id } => write!(
                formatter,
                "restore operation {:02x?} is bound to a different manifest descriptor",
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
            Self::Engine(source) => source.fmt(formatter),
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
            Self::Engine(source) => Some(source),
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

impl From<AgentMetadataError> for RestoreError {
    fn from(source: AgentMetadataError) -> Self {
        Self::Engine(source)
    }
}

impl From<QueryRecordError> for RestoreError {
    fn from(source: QueryRecordError) -> Self {
        Self::QueryRecord(source)
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
    store: &AgentMetadataStore,
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
        if existing.record.source_workbench_id != request.source_workbench_id
            || existing.record.source_workspace_incarnation_id
                != request.expected_source_workspace_incarnation_id
            || !restore_source_matches_selector(existing.record.source, request.source)
            || existing.record.destination_workbench_id != request.destination_workbench_id
            || existing.record.restore_manifest != request.restore_manifest
            || existing.record.destination_workspace_incarnation_id
                != request.destination_workspace_incarnation_id
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
    let operation = RestoreOperationRecord {
        operation_id,
        identity_digest,
        initialization_digest: None,
        source_workbench_id: request.source_workbench_id.clone(),
        source_workspace_incarnation_id: source_workspace.record.incarnation_id,
        source,
        destination_workbench_id: request.destination_workbench_id.clone(),
        destination_workspace_incarnation_id: request.destination_workspace_incarnation_id,
        restore_manifest: request.restore_manifest.clone(),
        phase: RestorePhase::Preparing,
        source_cursor: None,
        source_eof: false,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    let (mut members, scanned_eof) =
        scan_source_members(store, context, &loaded.record, request.limit)?;
    let scanned_count = members.len();
    let mut command_items = 2_usize;
    let mut revisions = BTreeSet::new();
    let mut admitted = 0_usize;
    for member in &members {
        let projection = TypedProjection::decode(&member.entry.typed_index_projection)?;
        let new_revision = revisions.insert(member.entry.artifact_revision_id);
        let additional = 3 + projection.fields().len() + usize::from(new_revision);
        if command_items.saturating_add(additional) > MAX_COMMAND_ITEMS {
            break;
        }
        command_items += additional;
        admitted += 1;
    }
    members.truncate(admitted);
    let source_eof = scanned_eof && admitted == scanned_count;
    let additional = u64::try_from(members.len()).expect("restore batch length fits u64");
    let next_sequence = loaded
        .record
        .next_member_sequence
        .checked_add(additional)
        .ok_or(RestoreError::SourceClosureMismatch {
            reason: "member sequence overflow".to_owned(),
        })?;
    if next_sequence > super::restore_records::MAX_RESTORE_MEMBERS {
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
    if let Some(last) = members.last() {
        next.source_cursor = Some(last.path.clone());
    }
    next.source_eof = source_eof;
    next.next_member_sequence = next_sequence;
    next.member_rolling_digest = rolling;
    next.validate()?;
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
    for (offset, member) in members.iter().enumerate() {
        let sequence = loaded
            .record
            .next_member_sequence
            .checked_add(u64::try_from(offset).expect("batch offset fits u64"))
            .expect("sequence bound was checked");
        plan.put_absent(
            MetadataFamily::PathCurrent,
            path_current_key(
                context.root_id,
                next.destination_workspace_incarnation_id,
                &member.path,
            ),
            member.entry.encode()?,
        )?;
        let next_epoch = revision_updates
            .get(&member.entry.artifact_revision_id)
            .expect("every source revision has one update")
            .1
            .reference_epoch;
        plan.put_absent(
            MetadataFamily::RevisionRef,
            path_revision_ref_key(
                context.root_id,
                next.destination_workspace_incarnation_id,
                &member.path,
                member.entry.artifact_revision_id,
            ),
            RevisionRefRecord {
                reference_epoch_at_add: next_epoch,
            }
            .encode()?,
        )?;
        plan.put_absent(
            MetadataFamily::RestoreMember,
            restore_member_key(context.root_id, request.operation_id, sequence),
            RestoreMemberRecord {
                destination_path: member.path.clone(),
                artifact_revision_id: member.entry.artifact_revision_id,
                path_generation: member.entry.generation,
                row_digest: member.row_digest,
            }
            .encode(),
        )?;
        let index_rows = secondary_index_rows(
            context.root_id,
            next.destination_workspace_incarnation_id,
            &member.path,
            &member.entry,
        )?;
        for (key, value) in index_rows {
            plan.put_absent(MetadataFamily::SecondaryIndex, key, value)?;
        }
    }
    for (revision, (loaded_revision, next_revision)) in revision_updates {
        plan.replace(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(context.root_id, revision),
            loaded_revision.payload,
            next_revision.encode()?,
        )?;
    }
    let copied_members = u32::try_from(members.len()).expect("bounded batch length fits u32");
    let command = execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        copied_members,
    )?;
    Ok(CopyRestoreBatchOutcome {
        copied_members: command.affected_members as usize,
        source_eof: command.operation.source_eof,
        command,
    })
}

/// Re-read the frozen source and ordered member ledger, then seal the exact
/// closure in one operation CAS.
pub fn seal_restore_source(
    store: &AgentMetadataStore,
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
            member_seal: loaded.record.member_rolling_digest,
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

/// Publish the stable restore manifest while the destination workspace remains
/// hidden. The operation becomes `Ready` in the same command as the path
/// change and its strong-reference updates.
pub fn apply_restore_initialization(
    store: &AgentMetadataStore,
    context: RootWriteContext,
    request: RestoreOperationRequest,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let loaded = load_operation(store, context, request.operation_id)?;
    let manifest_path = restore_manifest_path();
    let manifest = load_path(
        store,
        context,
        loaded.record.destination_workspace_incarnation_id,
        &manifest_path,
    )?
    .ok_or(RestoreError::RestoreManifestMissing)?;
    let initialization = RestoreInitialization {
        restore_manifest: manifest.record.clone(),
    };
    validate_initialization(&initialization, &loaded.record)?;
    let initialization_digest = restore_initialization_digest(&loaded.record, &initialization)?;
    let input_digest = initialization_input_digest(request.operation_id, initialization_digest);
    if let Some(outcome) = replay_outcome(store, context, input_digest, Some(request.operation_id))?
    {
        return Ok(outcome);
    }
    require_phase(&loaded.record, RestorePhase::SourceSealed, "SourceSealed")?;
    let revision = validate_manifest_revision(store, context, &initialization.restore_manifest)?;
    let next = loaded.record.apply(
        RestorePhase::SourceSealed,
        RestoreTransition::MarkReady {
            initialization_digest,
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
        MetadataFamily::PathCurrent,
        path_current_key(
            context.root_id,
            next.destination_workspace_incarnation_id,
            &manifest_path,
        ),
        Some(manifest.payload),
    )?;
    plan.assert_value(
        MetadataFamily::ArtifactRevision,
        artifact_revision_key(
            context.root_id,
            initialization.restore_manifest.artifact_revision_id,
        ),
        Some(revision.payload),
    )?;
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

/// Publish the destination marker and release the exact source retention in
/// one bounded command.
pub fn complete_restore(
    store: &AgentMetadataStore,
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
        },
    )?;
    let next_payload = next.encode()?;
    let visible = WorkspaceRecord {
        incarnation_id: staging.record.incarnation_id,
        workspace_revision,
        state: WorkspaceState::Visible,
        owning_operation_id: None,
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
        visible.encode()?,
    )?;
    release_source_retention(store, context, &loaded.record, &mut plan)?;
    plan.events
        .push(change_event_projection(&ChangeEventRecord {
            workspace_incarnation_id: result.destination_workspace_incarnation_id,
            kind: ChangeEventKind::WorkspaceRestored,
            artifact_revision_id: None,
            commit_id: None,
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
    store: &AgentMetadataStore,
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
            | RestorePhase::Ready
    ) {
        return Err(RestoreError::InvalidPhase {
            expected: "Preparing, Copying, SourceSealed, or Ready",
            actual: loaded.record.phase,
        });
    }
    let next = loaded.record.apply(
        loaded.record.phase,
        RestoreTransition::BeginAbort {
            terminal_error: request.terminal_error.clone(),
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

/// Transfer an aborted restore into its durable cleanup phase.
pub fn start_restore_cleanup(
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
        operation_key(
            context.root_id,
            OperationKind::Restore,
            request.operation_id,
        ),
        loaded.payload,
        next_payload.clone(),
    )?;
    predicate_staging_workspace(store, context, &next, &mut plan)?;
    apply_path_changes(
        store,
        context,
        next.destination_workspace_incarnation_id,
        path_changes,
        &mut plan,
    )?;
    for (key, member) in members {
        plan.delete(MetadataFamily::RestoreMember, key, member.payload)?;
    }
    let affected_members = u32::try_from(count).expect("bounded batch count fits u32");
    let command = execute_plan(
        store,
        context,
        plan,
        input_digest,
        next_payload,
        None,
        request.operation_id,
        affected_members,
    )?;
    Ok(CopyRestoreBatchOutcome {
        copied_members: command.affected_members as usize,
        source_eof: command.operation.cleanup_member_cursor
            == command.operation.next_member_sequence,
        command,
    })
}

/// Retire the hidden marker and release source retention only after the member
/// ledger is empty and its cleanup cursor proves the full closure was removed.
pub fn finish_restore_cleanup(
    store: &AgentMetadataStore,
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
    let staging = load_staging_workspace(store, context, &loaded.record)?;
    let manifest_path = restore_manifest_path();
    let manifest = load_path(
        store,
        context,
        loaded.record.destination_workspace_incarnation_id,
        &manifest_path,
    )?;
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
    apply_path_changes(
        store,
        context,
        next.destination_workspace_incarnation_id,
        vec![PathChange {
            path: manifest_path,
            before: manifest,
            after: None,
        }],
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    limit: usize,
) -> Result<(Vec<SourceMember>, bool), RestoreError> {
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
    store: &AgentMetadataStore,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
    start_after: Option<&nokv_types::NormalizedRelativePath>,
    limit: usize,
) -> Result<(Vec<SourceMember>, bool), RestoreError> {
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
    let scanned = store.scan_prefix_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        family,
        &prefix,
        version,
        marker.as_deref(),
        limit.saturating_add(1),
    )?;
    let eof = scanned.len() <= limit;
    let mut members = Vec::with_capacity(scanned.len().min(limit));
    for item in scanned.into_iter().take(limit) {
        members.push(decode_source_member(context.root_id, operation, item)?);
    }
    Ok((members, eof))
}

fn decode_source_member(
    root_id: nokv_types::RootId,
    operation: &RestoreOperationRecord,
    item: MetadataScanItem,
) -> Result<SourceMember, RestoreError> {
    let (path, entry) = match operation.source {
        RestoreSource::Snapshot { .. } => {
            let path = decode_path_current_key(
                root_id,
                operation.source_workspace_incarnation_id,
                &item.key,
            )
            .ok_or(RestoreError::CorruptKey {
                family: "PathCurrent",
            })?;
            let entry = PathEntry::decode(&item.value)?;
            (path, entry)
        }
        RestoreSource::Commit { commit_id } => {
            let path = decode_commit_member_key(root_id, commit_id, &item.key).ok_or(
                RestoreError::CorruptKey {
                    family: "CommitMember",
                },
            )?;
            let member = CommitMemberRecord::decode(&item.value)?;
            let entry = path_entry_from_commit_member(&member);
            (path, entry)
        }
    };
    if path.as_str() == RESTORE_MANIFEST_PATH {
        return Err(RestoreError::ReservedPathInSource);
    }
    TypedProjection::decode(&entry.typed_index_projection)?;
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
    Ok(SourceMember {
        path,
        entry,
        row_digest,
    })
}

fn verify_source_and_members(
    store: &AgentMetadataStore,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<(), RestoreError> {
    validate_source_retention(store, context, operation)?;
    let mut cursor = None;
    let mut sequence = 0_u64;
    let mut rolling = [0; SHA256_BYTES];
    loop {
        let (members, eof) = scan_source_page(store, context, operation, cursor.as_ref(), 255)?;
        for source in members {
            let member_key = restore_member_key(context.root_id, operation.operation_id, sequence);
            let restored = read_record(
                store,
                context,
                MetadataFamily::RestoreMember,
                &member_key,
                RestoreMemberRecord::decode,
            )?
            .ok_or_else(|| RestoreError::SourceClosureMismatch {
                reason: format!("restore member sequence {sequence} is missing"),
            })?;
            if restored.record.destination_path != source.path
                || restored.record.artifact_revision_id != source.entry.artifact_revision_id
                || restored.record.path_generation != source.entry.generation
                || restored.record.row_digest != source.row_digest
            {
                return Err(RestoreError::SourceClosureMismatch {
                    reason: format!("restore member sequence {sequence} does not match its source"),
                });
            }
            rolling = advance_commit_member_rolling_digest(rolling, sequence, source.row_digest);
            sequence =
                sequence
                    .checked_add(1)
                    .ok_or_else(|| RestoreError::SourceClosureMismatch {
                        reason: "source member count overflow".to_owned(),
                    })?;
            cursor = Some(source.path);
        }
        if eof {
            break;
        }
    }
    if sequence != operation.next_member_sequence
        || rolling != operation.member_rolling_digest
        || cursor != operation.source_cursor
    {
        return Err(RestoreError::SourceClosureMismatch {
            reason: "rescanned source does not match persisted count, cursor, and digest"
                .to_owned(),
        });
    }
    let member_prefix = restore_member_prefix(context.root_id, operation.operation_id);
    let last_member_key = sequence
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
    if let RestoreSource::Commit { commit_id } = operation.source {
        let commit = load_commit(store, context, commit_id)?;
        if commit.record.member_count != sequence || commit.record.member_digest != rolling {
            return Err(RestoreError::SourceClosureMismatch {
                reason: "commit member seal does not match its canonical member rows".to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_source_retention(
    store: &AgentMetadataStore,
    context: RootWriteContext,
    operation: &RestoreOperationRecord,
) -> Result<(), RestoreError> {
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
            if snapshot.record.state != SnapshotState::Active
                || snapshot.record.consumer_count == 0
                || snapshot.record.read_version != read_version
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
                || commit.record.consumer_count == 0
                || consumer.record.consumer_epoch_at_add > commit.record.consumer_epoch
            {
                return Err(RestoreError::CommitRetentionMismatch);
            }
        }
    }
    Ok(())
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
    store: &AgentMetadataStore,
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

fn release_source_retention(
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
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
    store: &AgentMetadataStore,
    context: RootWriteContext,
    manifest: &PathEntry,
) -> Result<Loaded<ArtifactRevisionRecord>, RestoreError> {
    let revision = load_available_revision(store, context, manifest.artifact_revision_id)?;
    if revision.record.logical_size != manifest.logical_size
        || revision.record.body_digest_uri != manifest.body_digest_uri
        || revision.record.content_type != manifest.content_type
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
    store: &AgentMetadataStore,
    context: RootWriteContext,
    plan: CommandPlan,
    input_digest: [u8; SHA256_BYTES],
    operation_payload: Vec<u8>,
    lease_guard: Option<(SnapshotId, u64)>,
    operation_id: OperationId,
    affected_members: u32,
) -> Result<RestoreCommandOutcome, RestoreError> {
    let deterministic_result =
        encode_restore_outcome(input_digest, &operation_payload, affected_members);
    let command = MetadataCommand {
        schema_id: SCHEMA_ID.to_owned(),
        root_id: context.root_id,
        logical_shard_id: context.logical_shard_id,
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
    let executed = match lease_guard {
        Some((_, deadline)) => store.execute_before_lease_deadline(&command, deadline),
        None => store.execute(&command),
    };
    let result = match executed {
        Ok(result) => result,
        Err(AgentMetadataError::LeaseDeadlineReached {
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
        Err(AgentMetadataError::PredicateFailed | AgentMetadataError::WriteConflict) => {
            return Err(RestoreError::ConcurrentMutation);
        }
        Err(AgentMetadataError::RequestIdReused) => {
            return Err(RestoreError::RequestInputMismatch);
        }
        Err(source) => return Err(RestoreError::Engine(source)),
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
    store: &AgentMetadataStore,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    operation_id: Option<OperationId>,
) -> Result<Option<RestoreCommandOutcome>, RestoreError> {
    let Some(replay) = store.lookup_request(
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
    initialization: &RestoreInitialization,
) -> Result<[u8; SHA256_BYTES], RestoreError> {
    let encoded = initialization.restore_manifest.encode()?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.restore.initialization.v3\0");
    hasher.update(operation.identity_digest);
    hasher.update(1_u32.to_be_bytes());
    hasher.update([1]);
    hash_u32_bytes(&mut hasher, RESTORE_MANIFEST_PATH.as_bytes())?;
    hash_u32_bytes(&mut hasher, &encoded)?;
    Ok(hasher.finalize().into())
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
    hasher.update(b"nokv.restore.begin.v4\0");
    hasher.update(root_id.as_bytes());
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
    use super::super::snapshot::{mint_snapshot, MintSnapshotRequest};
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

    fn write_context(
        store: &AgentMetadataStore,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
    ) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            placement(),
            owner_epoch,
            request(counter),
        )
        .unwrap()
    }

    fn read_context(store: &AgentMetadataStore, owner_epoch: OwnerEpoch) -> RootReadContext {
        RootReadContext {
            root_id: root(),
            placement_generation: placement(),
            owner_epoch,
            read_version: store.current_read_version().unwrap(),
        }
    }

    fn fence_command(
        store: &AgentMetadataStore,
        request_id: RequestId,
        owner_epoch: OwnerEpoch,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
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

    fn activate_root(store: &AgentMetadataStore, counter: &mut u128, owner_epoch: OwnerEpoch) {
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
        store: &AgentMetadataStore,
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
        source_revision: ArtifactRevisionId,
        manifest_revision: ArtifactRevisionId,
        initialization: RestoreInitialization,
        paths: Vec<nokv_types::NormalizedRelativePath>,
    }

    fn seed_source(
        store: &AgentMetadataStore,
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
        SeededSource {
            source_workbench,
            source_incarnation,
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
        store: &AgentMetadataStore,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source: &SeededSource,
    ) -> SnapshotId {
        let snapshot_id = SnapshotId::new(42);
        mint_snapshot(
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
        snapshot_id
    }

    fn begin_snapshot_restore(
        store: &AgentMetadataStore,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        source: &SeededSource,
        snapshot_id: SnapshotId,
        destination: &str,
        destination_incarnation: WorkspaceIncarnationId,
    ) -> RestoreCommandOutcome {
        begin_restore(
            store,
            write_context(store, counter, owner_epoch),
            &BeginRestoreRequest {
                source_workbench_id: source.source_workbench.clone(),
                expected_source_workspace_incarnation_id: source.source_incarnation,
                source: RestoreSourceSelector::Snapshot(snapshot_id),
                destination_workbench_id: workbench(destination),
                destination_workspace_incarnation_id: destination_incarnation,
                restore_manifest: restore_manifest_descriptor(&source.initialization),
            },
        )
        .unwrap()
    }

    fn publish_restore_manifest_for_test(
        store: &AgentMetadataStore,
        counter: &mut u128,
        owner_epoch: OwnerEpoch,
        operation_id: OperationId,
        initialization: &RestoreInitialization,
    ) {
        let context = write_context(store, counter, owner_epoch);
        let loaded = load_operation(store, context, operation_id).unwrap();
        let mut plan = CommandPlan::default();
        predicate_staging_workspace(store, context, &loaded.record, &mut plan).unwrap();
        apply_path_changes(
            store,
            context,
            loaded.record.destination_workspace_incarnation_id,
            vec![PathChange {
                path: restore_manifest_path(),
                before: None,
                after: Some(initialization.restore_manifest.clone()),
            }],
            &mut plan,
        )
        .unwrap();
        execute_plan(
            store,
            context,
            plan,
            operation_input_digest(b"test-manifest-publication", operation_id),
            loaded.payload,
            None,
            operation_id,
            0,
        )
        .unwrap();
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

    fn copy_all(
        store: &AgentMetadataStore,
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
        store: &AgentMetadataStore,
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
        publish_restore_manifest_for_test(
            store,
            counter,
            owner_epoch,
            operation_id,
            initialization,
        );
        apply_restore_initialization(
            store,
            write_context(store, counter, owner_epoch),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
    }

    #[test]
    fn large_snapshot_restore_survives_reopen_and_replays_publication() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("restore-meta");
        let mut counter = 0_u128;
        let initial_owner = owner(1);
        let store = AgentMetadataStore::create_file(&path, shard()).unwrap();
        activate_root(&store, &mut counter, initial_owner);
        let source = seed_source(&store, &mut counter, initial_owner, 300);
        let snapshot_id = mint_source_snapshot(&store, &mut counter, initial_owner, &source);
        let begin_context = write_context(&store, &mut counter, initial_owner);
        let begin_request = BeginRestoreRequest {
            source_workbench_id: source.source_workbench.clone(),
            expected_source_workspace_incarnation_id: source.source_incarnation,
            source: RestoreSourceSelector::Snapshot(snapshot_id),
            destination_workbench_id: workbench("destination"),
            destination_workspace_incarnation_id: incarnation(30),
            restore_manifest: restore_manifest_descriptor(&source.initialization),
        };
        let begun = begin_restore(&store, begin_context, &begin_request).unwrap();
        let begin_replay = begin_restore(&store, begin_context, &begin_request).unwrap();
        assert!(begin_replay.replayed);
        assert_eq!(begin_replay.operation, begun.operation);
        assert!(begun.operation.initialization_digest.is_none());
        let operation_id = begun.operation.operation_id;
        let mut wrong_descriptor = begin_request.clone();
        wrong_descriptor.restore_manifest.body_digest_uri = format!("sha256:{}", "cd".repeat(32));
        assert!(matches!(
            begin_restore(
                &store,
                write_context(&store, &mut counter, initial_owner),
                &wrong_descriptor,
            ),
            Err(RestoreError::RestoreManifestBindingMismatch {
                operation_id: actual,
            }) if actual == operation_id
        ));
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

        let store = AgentMetadataStore::reopen_file(&path, shard()).unwrap();
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
        publish_restore_manifest_for_test(
            &store,
            &mut counter,
            initial_owner,
            operation_id,
            &source.initialization,
        );
        apply_restore_initialization(
            &store,
            write_context(&store, &mut counter, initial_owner),
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
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
        let complete_replay = complete_restore(
            &store,
            complete_context,
            RestoreOperationRequest { operation_id },
        )
        .unwrap();
        assert!(complete_replay.command.replayed);
        assert_eq!(complete_replay.result, completed.result);
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
        assert_eq!(manifest.artifact_revision_id, source.manifest_revision);
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
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
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
            Err(RestoreError::Engine(
                AgentMetadataError::OwnerEpochMismatch { .. }
            ))
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
    fn abort_wins_ready_race_and_cleanup_releases_every_reference() {
        let mut counter = 2000_u128;
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
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
            source.paths.len() as u64
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
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        let current_owner = owner(1);
        activate_root(&store, &mut counter, current_owner);
        let source = seed_source(&store, &mut counter, current_owner, 3);
        let commit_id = CommitId::from_bytes([44; SHA256_BYTES]);
        let mut rolling = [0; SHA256_BYTES];
        let mut member_rows = Vec::new();
        for (sequence, path) in source.paths.iter().enumerate() {
            let member = CommitMemberRecord {
                artifact_revision_id: source.source_revision,
                path_generation: Generation::new(1).unwrap(),
                body_digest_uri: "sha256:source".to_owned(),
                manifest_digest_uri: "sha256:manifest".to_owned(),
                logical_size: 1,
                dependency_count: 0,
                dependency_depth: 0,
                content_type: "text/plain".to_owned(),
                producer: Some("test".to_owned()),
                manifest_id: None,
                typed_projection: TypedProjection::empty().encode().unwrap(),
            };
            let row_digest = commit_member_row_digest(path, &member).unwrap();
            rolling = advance_commit_member_rolling_digest(rolling, sequence as u64, row_digest);
            member_rows.push((
                MetadataFamily::CommitMember,
                super::super::codec::commit_member_key(root(), commit_id, path),
                member.encode().unwrap(),
            ));
        }
        let current_version =
            CommitVersion::new(store.current_read_version().unwrap().get()).unwrap();
        let commit = CommitRecord {
            source_workspace_incarnation_id: source.source_incarnation,
            content_digest_uri: "sha256:content".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            tree_manifest_revision_id: source.source_revision,
            tree_digest_uri: "sha256:tree".to_owned(),
            member_count: source.paths.len() as u64,
            member_digest: rolling,
            unique_revision_count: 1,
            revision_digest: [1; SHA256_BYTES],
            parent_commits: Vec::new(),
            parent_digest: [0; SHA256_BYTES],
            producer: Some("test".to_owned()),
            lineage_projection: Vec::new(),
            consumer_count: 0,
            consumer_epoch: ConsumerEpoch::ZERO,
            last_zero_consumer_version: Some(current_version),
            state: CommitState::Sealed,
        };
        member_rows.push((
            MetadataFamily::Commit,
            commit_key(root(), commit_id),
            commit.encode().unwrap(),
        ));
        put_absent(&store, &mut counter, current_owner, member_rows);
        let begun = begin_restore(
            &store,
            write_context(&store, &mut counter, current_owner),
            &BeginRestoreRequest {
                source_workbench_id: source.source_workbench.clone(),
                expected_source_workspace_incarnation_id: source.source_incarnation,
                source: RestoreSourceSelector::Commit(commit_id),
                destination_workbench_id: workbench("from-commit"),
                destination_workspace_incarnation_id: incarnation(33),
                restore_manifest: restore_manifest_descriptor(&source.initialization),
            },
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
        complete_restore(
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
        assert_eq!(commit.record.consumer_count, 0);
        assert!(commit.record.last_zero_consumer_version.is_some());
        assert!(read_payload(
            &store,
            write_context(&store, &mut counter, current_owner),
            MetadataFamily::CommitConsumer,
            &lease_commit_consumer_key(root(), commit_id, operation_id),
        )
        .unwrap()
        .is_none());
    }
}
