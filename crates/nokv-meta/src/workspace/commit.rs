/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Holt-native immutable commit construction, consumption, and retirement.
//!
//! Every potentially large closure is materialized or released by bounded
//! commands. The operation row is the sole durable cursor and the sole
//! publication-versus-cleanup ownership fence.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use nokv_types::{
    ArtifactRevisionId, BuildCommitPhase, CommitId, CommitRetirePhase, CommitState, CommitVersion,
    ConsumerEpoch, GcClaimState, Generation, HistoryHoldState, NormalizedRelativePath, OperationId,
    OperationKind, ReferenceEpoch, RevisionState, RootId, TagName, WorkbenchId,
    WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState, FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::build_commit_records::{
    BuildCommitOperationRecord, BuildCommitResult, CommitManifestCondition,
    CommitOperationRecordError, CommitOperationTerminalError, CommitRetireOperationRecord,
};
use super::codec::{
    artifact_revision_key, build_commit_history_hold_key, child_commit_consumer_key, commit_key,
    commit_member_key, commit_member_prefix, commit_revision_ref_key, commit_revision_ref_prefix,
    decode_commit_member_key, gc_candidate_key, operation_key, path_current_key,
    path_revision_ref_key, tag_commit_consumer_key, tag_key, workbench_commit_head_key,
    workbench_head_commit_consumer_key, workspace_current_key, SCHEMA_ID,
};
use super::commit_records::{
    advance_commit_member_rolling_digest, commit_member_row_digest, CommitConsumerRecord,
    CommitMemberRecord, CommitRecord, CommitRecordError, TagRecord, WorkbenchCommitHeadRecord,
};
use super::engine::{
    AgentMetadataError, AgentMetadataStore, CommandMutation, CommandPredicate, EventProjection,
    HistoryProjection, MetadataCommand, MetadataCommandResult, MetadataFamily, RootFenceAction,
    MAX_COMMAND_ITEMS,
};
use super::event_projection::change_event_projection;
use super::namespace::{
    get_visible_path_at, get_visible_workspace_at, scan_visible_paths_at, NamespaceError,
    RootReadContext, RootWriteContext,
};
use super::publication_records::{
    ArtifactRevisionRecord, GcCandidateRecord, PathEntry, PublicationRecordCodecError,
    RevisionRefRecord, WorkspaceRecord,
};
use super::query_records::{ChangeEventKind, ChangeEventRecord, QueryRecordError, TypedProjection};
use super::snapshot_records::{HistoryHoldRecord, SnapshotRecordError};

/// Canonical Workbench projection installed only with its typed commit head.
pub const RUN_MANIFEST_PATH: &str = "metadata/run_manifest.json";
/// Member batches reserve three command items per path plus the operation row.
pub const MAX_COMMIT_MEMBER_BATCH_ROWS: usize = 80;
/// Revision seal/release scans reserve a lookahead row.
pub const MAX_COMMIT_REVISION_BATCH_ROWS: usize = 80;
/// Direct parent closure bound from the schema.
pub const MAX_COMMIT_PARENT_BATCH_ROWS: usize = 64;
/// Member release batches need only member rows plus the operation.
pub const MAX_COMMIT_RETIRE_MEMBER_BATCH_ROWS: usize = 192;

const _: () = assert!(MAX_COMMIT_MEMBER_BATCH_ROWS * 3 < MAX_COMMAND_ITEMS);
const _: () = assert!(MAX_COMMIT_REVISION_BATCH_ROWS * 3 + 2 <= MAX_COMMAND_ITEMS);
const _: () = assert!(MAX_COMMIT_PARENT_BATCH_ROWS * 2 < MAX_COMMAND_ITEMS);

#[derive(Clone, Debug)]
pub struct BeginBuildCommitRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub workbench_id: WorkbenchId,
    /// Exact source incarnation selected by the caller. A workbench name may
    /// be rebound after retirement, so the name alone is never sufficient
    /// input identity for a durable commit operation.
    pub expected_source_workspace_incarnation_id: WorkspaceIncarnationId,
    pub commit_id: CommitId,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    pub projection_input_digest: [u8; SHA256_BYTES],
    pub tree_manifest_revision_id: ArtifactRevisionId,
    pub replace: bool,
    pub run_manifest_condition: CommitManifestCondition,
    /// Owner-observed proposal used only when this operation is first created.
    /// Once present, the durable operation timestamp is authoritative and an
    /// exact logical retry may carry a later proposal.
    pub committed_at_unix_seconds: u64,
    /// `None` requires an empty head; `Some` is an exact head-generation CAS.
    pub expected_head_generation: Option<Generation>,
    pub producer: Option<String>,
    pub lineage_projection: Vec<u8>,
    pub parent_commits: Vec<CommitId>,
}

#[derive(Clone, Copy, Debug)]
pub struct BuildCommitStepRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct AbortBuildCommitRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub terminal_error: CommitOperationTerminalError,
}

#[derive(Clone, Copy, Debug)]
pub struct BeginCommitRetirementRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub commit_id: CommitId,
    pub expected_consumer_epoch: ConsumerEpoch,
}

#[derive(Clone, Debug)]
pub struct SetCommitTagRequest {
    pub context: RootWriteContext,
    pub workbench_id: WorkbenchId,
    pub tag: TagName,
    pub commit_id: CommitId,
}

#[derive(Clone, Debug)]
pub struct DeleteCommitTagRequest {
    pub context: RootWriteContext,
    pub workbench_id: WorkbenchId,
    pub tag: TagName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildCommitOutcome {
    pub commit_version: CommitVersion,
    pub operation: BuildCommitOperationRecord,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetireCommitOutcome {
    pub commit_version: CommitVersion,
    pub operation: CommitRetireOperationRecord,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TagMutationOutcome {
    pub commit_version: CommitVersion,
    pub tag: Option<TagRecord>,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitError {
    Metadata(AgentMetadataError),
    Namespace(NamespaceError),
    CommitCodec(CommitRecordError),
    OperationCodec(CommitOperationRecordError),
    PublicationCodec(PublicationRecordCodecError),
    SnapshotCodec(SnapshotRecordError),
    QueryRecord(QueryRecordError),
    WorkspaceNotFound,
    OperationNotFound,
    CommitNotFound {
        commit_id: CommitId,
    },
    CommitAlreadyExists,
    CommitUnavailable {
        commit_id: CommitId,
        state: CommitState,
    },
    RevisionNotFound {
        revision_id: ArtifactRevisionId,
    },
    RevisionUnavailable {
        revision_id: ArtifactRevisionId,
    },
    PhaseMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    InvalidBatchLimit {
        requested: usize,
        max: usize,
    },
    ClosureMismatch {
        closure: &'static str,
        expected_count: u64,
        actual_count: u64,
    },
    CorruptKey {
        family: &'static str,
    },
    HeadConflict,
    TagNotFound,
    ConsumerMissing,
    ConsumerCountUnderflow,
    CounterOverflow {
        field: &'static str,
    },
    ReplayResultMismatch,
    OperationInputMismatch,
    DuplicateCommandKey {
        family: MetadataFamily,
    },
}

impl fmt::Display for CommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(error) => error.fmt(formatter),
            Self::Namespace(error) => error.fmt(formatter),
            Self::CommitCodec(error) => error.fmt(formatter),
            Self::OperationCodec(error) => error.fmt(formatter),
            Self::PublicationCodec(error) => error.fmt(formatter),
            Self::SnapshotCodec(error) => error.fmt(formatter),
            Self::QueryRecord(error) => error.fmt(formatter),
            Self::WorkspaceNotFound => formatter.write_str("visible workbench was not found"),
            Self::OperationNotFound => formatter.write_str("commit operation was not found"),
            Self::CommitNotFound { commit_id } => {
                write!(
                    formatter,
                    "commit {:02x?} was not found",
                    commit_id.as_bytes()
                )
            }
            Self::CommitAlreadyExists => formatter.write_str("commit id already exists"),
            Self::CommitUnavailable { commit_id, state } => write!(
                formatter,
                "commit {:02x?} is not consumable in state {state:?}",
                commit_id.as_bytes()
            ),
            Self::RevisionNotFound { revision_id } => write!(
                formatter,
                "artifact revision {:02x?} was not found",
                revision_id.as_bytes()
            ),
            Self::RevisionUnavailable { revision_id } => write!(
                formatter,
                "artifact revision {:02x?} is not available",
                revision_id.as_bytes()
            ),
            Self::PhaseMismatch { expected, actual } => {
                write!(formatter, "expected phase {expected}, found {actual}")
            }
            Self::InvalidBatchLimit { requested, max } => {
                write!(formatter, "batch limit {requested} is outside 1..={max}")
            }
            Self::ClosureMismatch {
                closure,
                expected_count,
                actual_count,
            } => write!(
                formatter,
                "{closure} closure count mismatch: expected {expected_count}, found {actual_count}"
            ),
            Self::CorruptKey { family } => write!(formatter, "malformed {family} key"),
            Self::HeadConflict => formatter.write_str("workbench commit head moved"),
            Self::TagNotFound => formatter.write_str("commit tag was not found"),
            Self::ConsumerMissing => formatter.write_str("exact commit consumer row is missing"),
            Self::ConsumerCountUnderflow => {
                formatter.write_str("commit consumer count would underflow")
            }
            Self::CounterOverflow { field } => write!(formatter, "{field} counter overflow"),
            Self::ReplayResultMismatch => {
                formatter.write_str("stored deterministic commit result is inconsistent")
            }
            Self::OperationInputMismatch => formatter
                .write_str("commit operation id was reused with different immutable inputs"),
            Self::DuplicateCommandKey { family } => {
                write!(formatter, "duplicate command key in {family:?}")
            }
        }
    }
}

impl std::error::Error for CommitError {}

impl From<AgentMetadataError> for CommitError {
    fn from(error: AgentMetadataError) -> Self {
        Self::Metadata(error)
    }
}

impl From<NamespaceError> for CommitError {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}

impl From<CommitRecordError> for CommitError {
    fn from(error: CommitRecordError) -> Self {
        Self::CommitCodec(error)
    }
}

impl From<CommitOperationRecordError> for CommitError {
    fn from(error: CommitOperationRecordError) -> Self {
        Self::OperationCodec(error)
    }
}

impl From<PublicationRecordCodecError> for CommitError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::PublicationCodec(error)
    }
}

impl From<SnapshotRecordError> for CommitError {
    fn from(error: SnapshotRecordError) -> Self {
        Self::SnapshotCodec(error)
    }
}

impl From<QueryRecordError> for CommitError {
    fn from(error: QueryRecordError) -> Self {
        Self::QueryRecord(error)
    }
}

#[derive(Clone, Copy)]
pub struct CommitService<'a> {
    store: &'a AgentMetadataStore,
}

impl<'a> CommitService<'a> {
    pub const fn new(store: &'a AgentMetadataStore) -> Self {
        Self { store }
    }

    pub fn begin_build(
        &self,
        request: BeginBuildCommitRequest,
    ) -> Result<BuildCommitOutcome, CommitError> {
        let operation_key = operation_key(
            request.context.root_id,
            OperationKind::BuildCommit,
            request.operation_id,
        );
        if let Some(payload) =
            self.read_payload(request.context, MetadataFamily::Operation, &operation_key)?
        {
            let operation = BuildCommitOperationRecord::decode(&payload)?;
            let expected_head_generation = operation.expected_head.map(|head| head.head_generation);
            if operation.operation_id != request.operation_id
                || operation.workbench_id != request.workbench_id
                || operation.source_workspace_incarnation_id
                    != request.expected_source_workspace_incarnation_id
                || operation.commit_id != request.commit_id
                || operation.content_digest_uri != request.content_digest_uri
                || operation.manifest_digest_uri != request.manifest_digest_uri
                || operation.projection_input_digest != request.projection_input_digest
                || operation.tree_manifest_revision_id != request.tree_manifest_revision_id
                || operation.replace != request.replace
                || operation.run_manifest_condition != request.run_manifest_condition
                || expected_head_generation != request.expected_head_generation
                || operation.producer != request.producer
                || operation.lineage_projection != request.lineage_projection
                || operation.parent_commits != request.parent_commits
            {
                return Err(CommitError::OperationInputMismatch);
            }
            return Ok(BuildCommitOutcome {
                commit_version: CommitVersion::new(request.context.read_version.get())
                    .expect("root write contexts always have a non-zero read version"),
                operation,
                replayed: true,
            });
        }
        let read = read_context(request.context);
        let workspace = get_visible_workspace_at(self.store, read, &request.workbench_id)?
            .ok_or(CommitError::WorkspaceNotFound)?;
        if workspace.incarnation_id != request.expected_source_workspace_incarnation_id {
            return Err(CommitError::OperationInputMismatch);
        }
        let head_key = workbench_commit_head_key(request.context.root_id, workspace.incarnation_id);
        let expected_head = self
            .read_commit_record(
                request.context,
                MetadataFamily::WorkbenchCommitHead,
                &head_key,
                WorkbenchCommitHeadRecord::decode,
            )?
            .map(|loaded| loaded.record);
        let head_matches = match (request.expected_head_generation, expected_head) {
            (None, None) => true,
            (Some(expected), Some(current)) => expected == current.head_generation,
            _ => false,
        };
        if !head_matches {
            return Err(CommitError::HeadConflict);
        }
        if expected_head.is_some() && !request.replace {
            return Err(CommitError::HeadConflict);
        }
        let run_manifest_path = NormalizedRelativePath::new(RUN_MANIFEST_PATH)
            .expect("canonical run manifest path is normalized");
        let current_run_manifest =
            get_visible_path_at(self.store, read, &request.workbench_id, &run_manifest_path)?;
        let condition_matches = match (
            request.run_manifest_condition,
            current_run_manifest.as_ref().map(|entry| entry.generation),
        ) {
            (CommitManifestCondition::CreateOnly, None) => true,
            (
                CommitManifestCondition::ReplaceOnly {
                    expected_generation,
                },
                Some(current_generation),
            ) => expected_generation == current_generation,
            _ => false,
        };
        if !condition_matches {
            return Err(CommitError::HeadConflict);
        }

        let mut operation = BuildCommitOperationRecord {
            operation_id: request.operation_id,
            identity_digest: [0; SHA256_BYTES],
            initialization_digest: [0; SHA256_BYTES],
            workbench_id: request.workbench_id,
            source_workspace_incarnation_id: workspace.incarnation_id,
            source_read_version: request.context.read_version,
            commit_id: request.commit_id,
            expected_head,
            content_digest_uri: request.content_digest_uri,
            manifest_digest_uri: request.manifest_digest_uri,
            projection_input_digest: request.projection_input_digest,
            tree_manifest_revision_id: request.tree_manifest_revision_id,
            replace: request.replace,
            run_manifest_condition: request.run_manifest_condition,
            committed_at_unix_seconds: request.committed_at_unix_seconds,
            commit_staged_run_manifest: None,
            producer: request.producer,
            lineage_projection: request.lineage_projection,
            parent_commits: request.parent_commits,
            phase: BuildCommitPhase::Building,
            member_cursor: None,
            member_count: 0,
            member_digest: [0; SHA256_BYTES],
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
            cleanup_revision_count: 0,
            cleanup_parent_count: 0,
            history_hold_released: false,
            result: None,
            terminal_error: None,
        };
        operation.seal_digests();
        let operation_payload = operation.encode()?;
        let hold_key = build_commit_history_hold_key(request.context.root_id, request.operation_id);
        let hold = HistoryHoldRecord {
            read_version: request.context.read_version,
            source_snapshot_id: None,
            state: HistoryHoldState::Active,
        }
        .encode();

        let mut plan = CommandPlan::default();
        plan.put_absent(
            MetadataFamily::Operation,
            operation_key,
            operation_payload.clone(),
        )?;
        plan.put_absent(MetadataFamily::HistoryHold, hold_key, hold)?;
        plan.assert_value(
            MetadataFamily::Commit,
            commit_key(request.context.root_id, request.commit_id),
            None,
        )?;
        plan.prefix_empty(
            MetadataFamily::CommitMember,
            commit_member_prefix(request.context.root_id, request.commit_id),
        );
        plan.prefix_empty(
            MetadataFamily::RevisionRef,
            commit_revision_ref_prefix(request.context.root_id, request.commit_id),
        );
        let result = self.execute_plan(request.context, plan, operation_payload)?;
        decode_build_outcome(result, request.operation_id)
    }

    pub fn build_members(
        &self,
        request: BuildCommitStepRequest,
    ) -> Result<BuildCommitOutcome, CommitError> {
        validate_limit(request.limit, MAX_COMMIT_MEMBER_BATCH_ROWS)?;
        let loaded = self.load_build_operation(request.context, request.operation_id)?;
        require_build_phase(&loaded.record, BuildCommitPhase::Building)?;
        if loaded.record.members_complete {
            return self.noop_build(request.context, loaded);
        }
        let source_context = RootReadContext {
            root_id: request.context.root_id,
            placement_generation: request.context.placement_generation,
            owner_epoch: request.context.owner_epoch,
            read_version: loaded.record.source_read_version,
        };
        let page = scan_visible_paths_at(
            self.store,
            source_context,
            &loaded.record.workbench_id,
            loaded.record.member_cursor.as_ref(),
            request.limit,
        )?;
        let mut next = loaded.record.clone();
        let mut plan = CommandPlan::default();
        let mut new_revisions =
            BTreeMap::<ArtifactRevisionId, Loaded<ArtifactRevisionRecord>>::new();
        let mut observed_revisions = BTreeSet::new();
        let run_manifest_path = NormalizedRelativePath::new(RUN_MANIFEST_PATH)
            .expect("canonical run manifest path is normalized");
        let mut virtual_manifest_pending = loaded.record.commit_staged_run_manifest.is_some()
            && loaded
                .record
                .member_cursor
                .as_ref()
                .is_none_or(|cursor| cursor < &run_manifest_path);
        let staged_manifest_member =
            if let Some(binding) = loaded.record.commit_staged_run_manifest.as_ref() {
                let revision =
                    self.load_revision(request.context, loaded.record.tree_manifest_revision_id)?;
                if revision.record.content_type != "application/json"
                    || revision.record.logical_size != binding.logical_size
                    || revision.record.body_digest_uri != binding.body_digest_uri
                    || revision.record.manifest_digest_uri != binding.manifest_digest_uri
                    || revision.record.content_type != binding.content_type
                    || revision.record.dependency_count != 0
                    || revision.record.dependency_depth != 0
                {
                    return Err(CommitError::ClosureMismatch {
                        closure: "commit-staged run manifest",
                        expected_count: 1,
                        actual_count: 0,
                    });
                }
                let source = get_visible_path_at(
                    self.store,
                    source_context,
                    &loaded.record.workbench_id,
                    &run_manifest_path,
                )?;
                let generation = match source {
                    None => Generation::new(1).expect("one is non-zero"),
                    Some(source) => Generation::new(source.generation.get().checked_add(1).ok_or(
                        CommitError::CounterOverflow {
                            field: "run_manifest_path_generation",
                        },
                    )?)
                    .expect("checked generation successor is non-zero"),
                };
                Some(CommitMemberRecord {
                    artifact_revision_id: loaded.record.tree_manifest_revision_id,
                    path_generation: generation,
                    body_digest_uri: revision.record.body_digest_uri,
                    manifest_digest_uri: revision.record.manifest_digest_uri,
                    logical_size: revision.record.logical_size,
                    dependency_count: revision.record.dependency_count,
                    dependency_depth: revision.record.dependency_depth,
                    content_type: revision.record.content_type,
                    producer: None,
                    manifest_id: None,
                    typed_projection: Vec::new(),
                })
            } else {
                None
            };

        let mut consumed_source_rows = 0_usize;
        for visible in &page.entries {
            if next.member_count - loaded.record.member_count >= request.limit as u64 {
                break;
            }
            if virtual_manifest_pending && run_manifest_path < visible.path {
                self.plan_commit_member(
                    request.context,
                    &run_manifest_path,
                    staged_manifest_member
                        .as_ref()
                        .expect("pending virtual manifest has a staged descriptor"),
                    &mut next,
                    &mut new_revisions,
                    &mut observed_revisions,
                    &mut plan,
                )?;
                virtual_manifest_pending = false;
                if next.member_count - loaded.record.member_count >= request.limit as u64 {
                    break;
                }
            }
            if visible.path == run_manifest_path
                && loaded.record.commit_staged_run_manifest.is_some()
            {
                if virtual_manifest_pending {
                    self.plan_commit_member(
                        request.context,
                        &run_manifest_path,
                        staged_manifest_member
                            .as_ref()
                            .expect("pending virtual manifest has a staged descriptor"),
                        &mut next,
                        &mut new_revisions,
                        &mut observed_revisions,
                        &mut plan,
                    )?;
                    virtual_manifest_pending = false;
                }
                consumed_source_rows += 1;
                continue;
            }
            let member = CommitMemberRecord {
                artifact_revision_id: visible.entry.artifact_revision_id,
                path_generation: visible.entry.generation,
                body_digest_uri: visible.entry.body_digest_uri.clone(),
                manifest_digest_uri: visible.entry.manifest_digest_uri.clone(),
                logical_size: visible.entry.logical_size,
                dependency_count: visible.entry.dependency_count,
                dependency_depth: visible.entry.dependency_depth,
                content_type: visible.entry.content_type.clone(),
                producer: visible.entry.producer.clone(),
                manifest_id: visible.entry.manifest_id.clone(),
                typed_projection: visible.entry.typed_index_projection.clone(),
            };
            self.plan_commit_member(
                request.context,
                &visible.path,
                &member,
                &mut next,
                &mut new_revisions,
                &mut observed_revisions,
                &mut plan,
            )?;
            consumed_source_rows += 1;
        }
        if virtual_manifest_pending
            && page.next_marker.is_none()
            && consumed_source_rows == page.entries.len()
            && next.member_count - loaded.record.member_count < request.limit as u64
        {
            self.plan_commit_member(
                request.context,
                &run_manifest_path,
                staged_manifest_member
                    .as_ref()
                    .expect("pending virtual manifest has a staged descriptor"),
                &mut next,
                &mut new_revisions,
                &mut observed_revisions,
                &mut plan,
            )?;
            virtual_manifest_pending = false;
        }

        if page.next_marker.is_none()
            && consumed_source_rows == page.entries.len()
            && !virtual_manifest_pending
        {
            self.plan_revision_attachment(
                request.context,
                next.commit_id,
                next.tree_manifest_revision_id,
                &mut next,
                &mut new_revisions,
                &mut plan,
            )?;
            next.members_complete = true;
        }
        replace_operation(&mut plan, request.context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_build_outcome(result, request.operation_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_commit_member(
        &self,
        context: RootWriteContext,
        path: &NormalizedRelativePath,
        member: &CommitMemberRecord,
        operation: &mut BuildCommitOperationRecord,
        new_revisions: &mut BTreeMap<ArtifactRevisionId, Loaded<ArtifactRevisionRecord>>,
        observed_revisions: &mut BTreeSet<ArtifactRevisionId>,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        let row_digest = commit_member_row_digest(path, member)?;
        operation.member_digest = advance_commit_member_rolling_digest(
            operation.member_digest,
            operation.member_count,
            row_digest,
        );
        operation.member_count = checked_add(operation.member_count, 1, "member_count")?;
        operation.member_cursor = Some(path.clone());
        plan.put_absent(
            MetadataFamily::CommitMember,
            commit_member_key(context.root_id, operation.commit_id, path),
            member.encode()?,
        )?;
        if observed_revisions.insert(member.artifact_revision_id) {
            self.plan_revision_attachment(
                context,
                operation.commit_id,
                member.artifact_revision_id,
                operation,
                new_revisions,
                plan,
            )?;
        }
        Ok(())
    }

    fn plan_revision_attachment(
        &self,
        context: RootWriteContext,
        commit_id: CommitId,
        revision_id: ArtifactRevisionId,
        operation: &mut BuildCommitOperationRecord,
        batch_updates: &mut BTreeMap<ArtifactRevisionId, Loaded<ArtifactRevisionRecord>>,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        if batch_updates.contains_key(&revision_id) {
            return Ok(());
        }
        let ref_key = commit_revision_ref_key(context.root_id, commit_id, revision_id);
        if let Some(reference_payload) =
            self.read_payload(context, MetadataFamily::RevisionRef, &ref_key)?
        {
            let reference = RevisionRefRecord::decode(&reference_payload)?;
            let revision = self.load_revision(context, revision_id)?;
            if reference.reference_epoch_at_add > revision.record.reference_epoch {
                return Err(CommitError::ClosureMismatch {
                    closure: "existing commit revision ref epoch",
                    expected_count: revision.record.reference_epoch.get(),
                    actual_count: reference.reference_epoch_at_add.get(),
                });
            }
            return Ok(());
        }
        let loaded = self.load_revision(context, revision_id)?;
        let next_epoch = ReferenceEpoch::new(checked_add(
            loaded.record.reference_epoch.get(),
            1,
            "reference_epoch",
        )?);
        let mut next_revision = loaded.record.clone();
        next_revision.reference_epoch = next_epoch;
        next_revision.strong_reference_count = checked_add(
            next_revision.strong_reference_count,
            1,
            "strong_reference_count",
        )?;
        next_revision.last_zero_ref_version = None;
        plan.replace(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(context.root_id, revision_id),
            loaded.payload.clone(),
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
        operation.revision_ref_count =
            checked_add(operation.revision_ref_count, 1, "revision_ref_count")?;
        batch_updates.insert(
            revision_id,
            Loaded {
                payload: next_revision.encode()?,
                record: next_revision,
            },
        );
        Ok(())
    }

    pub fn seal_revisions(
        &self,
        request: BuildCommitStepRequest,
    ) -> Result<BuildCommitOutcome, CommitError> {
        validate_limit(request.limit, MAX_COMMIT_REVISION_BATCH_ROWS)?;
        let loaded = self.load_build_operation(request.context, request.operation_id)?;
        require_build_phase(&loaded.record, BuildCommitPhase::Building)?;
        if !loaded.record.members_complete {
            return Err(CommitError::PhaseMismatch {
                expected: "complete member closure",
                actual: "building members",
            });
        }
        if loaded.record.revisions_complete {
            return self.noop_build(request.context, loaded);
        }
        let prefix = commit_revision_ref_prefix(request.context.root_id, loaded.record.commit_id);
        let marker = loaded.record.revision_cursor.map(|revision| {
            commit_revision_ref_key(request.context.root_id, loaded.record.commit_id, revision)
        });
        let mut rows = self.store.scan_prefix_at(
            request.context.root_id,
            request.context.placement_generation,
            request.context.owner_epoch,
            MetadataFamily::RevisionRef,
            &prefix,
            request.context.read_version,
            marker.as_deref(),
            request.limit + 1,
        )?;
        let has_more = rows.len() > request.limit;
        if has_more {
            rows.truncate(request.limit);
        }
        let mut next = loaded.record.clone();
        for row in rows {
            let revision = decode_commit_revision_ref_key(&prefix, &row.key)?;
            RevisionRefRecord::decode(&row.value)?;
            next.revision_digest = advance_commit_revision_rolling_digest(
                next.revision_digest,
                next.revision_seal_count,
                revision,
                &row.value,
            );
            next.revision_seal_count =
                checked_add(next.revision_seal_count, 1, "revision_seal_count")?;
            if next.revision_seal_count > next.revision_ref_count {
                return Err(CommitError::ClosureMismatch {
                    closure: "revision",
                    expected_count: next.revision_ref_count,
                    actual_count: next.revision_seal_count,
                });
            }
            next.revision_cursor = Some(revision);
        }
        if !has_more {
            if next.revision_seal_count != next.revision_ref_count {
                return Err(CommitError::ClosureMismatch {
                    closure: "revision",
                    expected_count: next.revision_ref_count,
                    actual_count: next.revision_seal_count,
                });
            }
            next.revisions_complete = true;
        }
        let mut plan = CommandPlan::default();
        replace_operation(&mut plan, request.context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_build_outcome(result, request.operation_id)
    }

    pub fn attach_parents(
        &self,
        request: BuildCommitStepRequest,
    ) -> Result<BuildCommitOutcome, CommitError> {
        validate_limit(request.limit, MAX_COMMIT_PARENT_BATCH_ROWS)?;
        let loaded = self.load_build_operation(request.context, request.operation_id)?;
        require_build_phase(&loaded.record, BuildCommitPhase::Building)?;
        if loaded.record.parents_complete {
            return self.noop_build(request.context, loaded);
        }
        let start = loaded.record.parent_cursor as usize;
        let end = (start + request.limit).min(loaded.record.parent_commits.len());
        let mut next = loaded.record.clone();
        let mut plan = CommandPlan::default();
        for index in start..end {
            let parent_id = next.parent_commits[index];
            if parent_id == next.commit_id {
                return Err(CommitError::ClosureMismatch {
                    closure: "parent self-reference",
                    expected_count: 0,
                    actual_count: 1,
                });
            }
            let consumer_key =
                child_commit_consumer_key(request.context.root_id, parent_id, next.commit_id);
            if self
                .read_payload(
                    request.context,
                    MetadataFamily::CommitConsumer,
                    &consumer_key,
                )?
                .is_some()
            {
                return Err(CommitError::ClosureMismatch {
                    closure: "parent consumer",
                    expected_count: next.parent_cursor.into(),
                    actual_count: (index + 1) as u64,
                });
            }
            let parent = self.load_commit(request.context, parent_id)?;
            require_sealed(&parent.record, parent_id)?;
            let next_parent = add_consumer(&parent.record)?;
            plan.replace(
                MetadataFamily::Commit,
                commit_key(request.context.root_id, parent_id),
                parent.payload,
                next_parent.encode()?,
            )?;
            plan.put_absent(
                MetadataFamily::CommitConsumer,
                consumer_key,
                CommitConsumerRecord {
                    consumer_epoch_at_add: next_parent.consumer_epoch,
                }
                .encode(),
            )?;
            next.parent_digest = advance_commit_parent_rolling_digest(
                next.parent_digest,
                u64::from(next.parent_cursor),
                parent_id,
            );
            next.parent_cursor =
                next.parent_cursor
                    .checked_add(1)
                    .ok_or(CommitError::CounterOverflow {
                        field: "parent_cursor",
                    })?;
        }
        next.parents_complete = end == next.parent_commits.len();
        replace_operation(&mut plan, request.context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_build_outcome(result, request.operation_id)
    }

    pub fn begin_sealing(
        &self,
        context: RootWriteContext,
        operation_id: OperationId,
    ) -> Result<BuildCommitOutcome, CommitError> {
        let loaded = self.load_build_operation(context, operation_id)?;
        require_build_phase(&loaded.record, BuildCommitPhase::Building)?;
        if !(loaded.record.members_complete
            && loaded.record.revisions_complete
            && loaded.record.parents_complete)
        {
            return Err(CommitError::PhaseMismatch {
                expected: "three complete closure seals",
                actual: "incomplete build closure",
            });
        }
        let mut verified_parent_digest = [0; SHA256_BYTES];
        let mut plan = CommandPlan::default();
        for (sequence, parent_id) in loaded.record.parent_commits.iter().copied().enumerate() {
            let key =
                child_commit_consumer_key(context.root_id, parent_id, loaded.record.commit_id);
            let payload = self
                .read_payload(context, MetadataFamily::CommitConsumer, &key)?
                .ok_or(CommitError::ConsumerMissing)?;
            CommitConsumerRecord::decode(&payload)?;
            plan.assert_value(MetadataFamily::CommitConsumer, key, Some(payload))?;
            verified_parent_digest = advance_commit_parent_rolling_digest(
                verified_parent_digest,
                sequence as u64,
                parent_id,
            );
        }
        if loaded.record.parent_cursor as usize != loaded.record.parent_commits.len()
            || verified_parent_digest != loaded.record.parent_digest
        {
            return Err(CommitError::ClosureMismatch {
                closure: "parent",
                expected_count: loaded.record.parent_commits.len() as u64,
                actual_count: u64::from(loaded.record.parent_cursor),
            });
        }
        let mut next = loaded.record.clone();
        next.phase = BuildCommitPhase::Sealing;
        replace_operation(&mut plan, context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(context, plan, payload)?;
        decode_build_outcome(result, operation_id)
    }

    pub fn finalize_build(
        &self,
        context: RootWriteContext,
        operation_id: OperationId,
    ) -> Result<BuildCommitOutcome, CommitError> {
        let loaded = self.load_build_operation(context, operation_id)?;
        require_build_phase(&loaded.record, BuildCommitPhase::Sealing)?;
        let operation = &loaded.record;
        let current_workspace =
            get_visible_workspace_at(self.store, read_context(context), &operation.workbench_id)?
                .ok_or(CommitError::WorkspaceNotFound)?;
        if current_workspace.incarnation_id != operation.source_workspace_incarnation_id {
            return Err(CommitError::HeadConflict);
        }
        let workspace_key = workspace_current_key(context.root_id, &operation.workbench_id);
        let workspace_payload = self
            .read_payload(context, MetadataFamily::WorkspaceCurrent, &workspace_key)?
            .ok_or(CommitError::WorkspaceNotFound)?;
        let commit_key_bytes = commit_key(context.root_id, operation.commit_id);
        if self
            .read_payload(context, MetadataFamily::Commit, &commit_key_bytes)?
            .is_some()
        {
            return Err(CommitError::CommitAlreadyExists);
        }

        let head_key =
            workbench_commit_head_key(context.root_id, operation.source_workspace_incarnation_id);
        let current_head = self.read_commit_record(
            context,
            MetadataFamily::WorkbenchCommitHead,
            &head_key,
            WorkbenchCommitHeadRecord::decode,
        )?;
        if current_head.as_ref().map(|loaded| loaded.record) != operation.expected_head {
            return Err(CommitError::HeadConflict);
        }
        let next_head_generation = match operation.expected_head {
            None => Generation::new(1).expect("one is non-zero"),
            Some(head) => Generation::new(head.head_generation.get().checked_add(1).ok_or(
                CommitError::CounterOverflow {
                    field: "head_generation",
                },
            )?)
            .expect("checked successor is non-zero"),
        };
        let next_commit_version = next_commit_version(context)?;
        let new_commit = CommitRecord {
            source_workspace_incarnation_id: operation.source_workspace_incarnation_id,
            content_digest_uri: operation.content_digest_uri.clone(),
            manifest_digest_uri: operation.manifest_digest_uri.clone(),
            tree_manifest_revision_id: operation.tree_manifest_revision_id,
            tree_digest_uri: member_tree_digest_uri(operation.member_digest),
            member_count: operation.member_count,
            member_digest: operation.member_digest,
            unique_revision_count: operation.revision_ref_count,
            revision_digest: operation.revision_digest,
            parent_commits: operation.parent_commits.clone(),
            parent_digest: operation.parent_digest,
            producer: operation.producer.clone(),
            lineage_projection: operation.lineage_projection.clone(),
            consumer_count: 1,
            consumer_epoch: ConsumerEpoch::new(1),
            last_zero_consumer_version: None,
            state: CommitState::Sealed,
        };
        let new_head = WorkbenchCommitHeadRecord {
            commit_id: operation.commit_id,
            head_generation: next_head_generation,
        };
        let new_head_consumer_key = workbench_head_commit_consumer_key(
            context.root_id,
            operation.commit_id,
            operation.source_workspace_incarnation_id,
        );
        let hold_key = build_commit_history_hold_key(context.root_id, operation_id);
        let hold_payload = self
            .read_payload(context, MetadataFamily::HistoryHold, &hold_key)?
            .ok_or(CommitError::OperationNotFound)?;
        let hold = HistoryHoldRecord::decode(&hold_payload)?;
        if hold.read_version != operation.source_read_version
            || hold.source_snapshot_id.is_some()
            || hold.state != HistoryHoldState::Active
        {
            return Err(CommitError::ClosureMismatch {
                closure: "history hold",
                expected_count: operation.source_read_version.get(),
                actual_count: hold.read_version.get(),
            });
        }

        let mut plan = CommandPlan::default();
        if operation.commit_staged_run_manifest.is_some() {
            self.plan_commit_staged_run_manifest_publication(
                context,
                operation,
                workspace_key,
                workspace_payload,
                next_commit_version,
                &mut plan,
            )?;
        } else {
            plan.assert_value(
                MetadataFamily::WorkspaceCurrent,
                workspace_key,
                Some(workspace_payload),
            )?;
        }
        plan.put_absent(
            MetadataFamily::Commit,
            commit_key_bytes,
            new_commit.encode()?,
        )?;
        plan.put_absent(
            MetadataFamily::CommitConsumer,
            new_head_consumer_key,
            CommitConsumerRecord {
                consumer_epoch_at_add: ConsumerEpoch::new(1),
            }
            .encode(),
        )?;
        match current_head {
            None => plan.put_absent(
                MetadataFamily::WorkbenchCommitHead,
                head_key,
                new_head.encode(),
            )?,
            Some(current) => {
                plan.replace(
                    MetadataFamily::WorkbenchCommitHead,
                    head_key,
                    current.payload,
                    new_head.encode(),
                )?;
                let old_id = current.record.commit_id;
                let old_commit = self.load_commit(context, old_id)?;
                require_sealed(&old_commit.record, old_id)?;
                let old_consumer_key = workbench_head_commit_consumer_key(
                    context.root_id,
                    old_id,
                    operation.source_workspace_incarnation_id,
                );
                let old_consumer = self
                    .read_payload(context, MetadataFamily::CommitConsumer, &old_consumer_key)?
                    .ok_or(CommitError::ConsumerMissing)?;
                CommitConsumerRecord::decode(&old_consumer)?;
                let next_old = remove_consumer(&old_commit.record, next_commit_version)?;
                plan.replace(
                    MetadataFamily::Commit,
                    commit_key(context.root_id, old_id),
                    old_commit.payload,
                    next_old.encode()?,
                )?;
                plan.delete(
                    MetadataFamily::CommitConsumer,
                    old_consumer_key,
                    old_consumer,
                )?;
            }
        }
        plan.delete(MetadataFamily::HistoryHold, hold_key, hold_payload)?;
        let mut next = operation.clone();
        next.phase = BuildCommitPhase::Complete;
        next.history_hold_released = true;
        next.result = Some(BuildCommitResult {
            commit_id: operation.commit_id,
            head_generation: next_head_generation,
        });
        replace_operation(&mut plan, context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(context, plan, payload)?;
        decode_build_outcome(result, operation_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_commit_staged_run_manifest_publication(
        &self,
        context: RootWriteContext,
        operation: &BuildCommitOperationRecord,
        workspace_key: Vec<u8>,
        workspace_payload: Vec<u8>,
        next_commit_version: CommitVersion,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        let workspace = WorkspaceRecord::decode(&workspace_payload)?;
        if workspace.incarnation_id != operation.source_workspace_incarnation_id
            || workspace.state != WorkspaceState::Visible
            || workspace.owning_operation_id.is_some()
        {
            return Err(CommitError::HeadConflict);
        }
        let next_workspace_revision =
            WorkspaceRevision::new(workspace.workspace_revision.get().checked_add(1).ok_or(
                CommitError::CounterOverflow {
                    field: "workspace_revision",
                },
            )?);
        let next_workspace = WorkspaceRecord {
            workspace_revision: next_workspace_revision,
            ..workspace
        };
        plan.replace(
            MetadataFamily::WorkspaceCurrent,
            workspace_key,
            workspace_payload,
            next_workspace.encode()?,
        )?;

        let path = NormalizedRelativePath::new(RUN_MANIFEST_PATH)
            .expect("canonical run manifest path is normalized");
        let source_context = RootReadContext {
            root_id: context.root_id,
            placement_generation: context.placement_generation,
            owner_epoch: context.owner_epoch,
            read_version: operation.source_read_version,
        };
        let source_path =
            get_visible_path_at(self.store, source_context, &operation.workbench_id, &path)?;
        let path_key = path_current_key(
            context.root_id,
            operation.source_workspace_incarnation_id,
            &path,
        );
        let current_path = self.read_commit_record(
            context,
            MetadataFamily::PathCurrent,
            &path_key,
            PathEntry::decode,
        )?;
        if current_path.as_ref().map(|loaded| &loaded.record) != source_path.as_ref() {
            return Err(CommitError::HeadConflict);
        }
        if current_path
            .as_ref()
            .is_some_and(|current| !current.record.typed_index_projection.is_empty())
        {
            return Err(CommitError::ClosureMismatch {
                closure: "run manifest secondary index",
                expected_count: 0,
                actual_count: 1,
            });
        }

        let member_key = commit_member_key(context.root_id, operation.commit_id, &path);
        let member_payload = self
            .read_payload(context, MetadataFamily::CommitMember, &member_key)?
            .ok_or(CommitError::ClosureMismatch {
                closure: "run manifest commit member",
                expected_count: 1,
                actual_count: 0,
            })?;
        let member = CommitMemberRecord::decode(&member_payload)?;
        let binding =
            operation
                .commit_staged_run_manifest
                .as_ref()
                .ok_or(CommitError::ClosureMismatch {
                    closure: "run manifest durable binding",
                    expected_count: 1,
                    actual_count: 0,
                })?;
        if member.artifact_revision_id != operation.tree_manifest_revision_id
            || member.logical_size != binding.logical_size
            || member.body_digest_uri != binding.body_digest_uri
            || member.manifest_digest_uri != binding.manifest_digest_uri
            || member.dependency_count != 0
            || member.dependency_depth != 0
            || member.content_type != binding.content_type
            || member.content_type != "application/json"
            || member.producer.is_some()
            || member.manifest_id.is_some()
            || !member.typed_projection.is_empty()
        {
            return Err(CommitError::ClosureMismatch {
                closure: "run manifest commit member",
                expected_count: 1,
                actual_count: 0,
            });
        }
        plan.assert_value(
            MetadataFamily::CommitMember,
            member_key,
            Some(member_payload),
        )?;

        let staged_revision = self.load_revision(context, operation.tree_manifest_revision_id)?;
        if staged_revision.record.body_digest_uri != member.body_digest_uri
            || staged_revision.record.logical_size != member.logical_size
            || staged_revision.record.content_type != member.content_type
            || staged_revision.record.manifest_digest_uri != binding.manifest_digest_uri
            || staged_revision.record.dependency_count != 0
            || staged_revision.record.dependency_depth != 0
        {
            return Err(CommitError::ClosureMismatch {
                closure: "run manifest revision descriptor",
                expected_count: 1,
                actual_count: 0,
            });
        }
        let expected_generation = match current_path.as_ref() {
            None => Generation::new(1).expect("one is non-zero"),
            Some(current) => {
                Generation::new(current.record.generation.get().checked_add(1).ok_or(
                    CommitError::CounterOverflow {
                        field: "run_manifest_path_generation",
                    },
                )?)
                .expect("checked generation successor is non-zero")
            }
        };
        if member.path_generation != expected_generation {
            return Err(CommitError::ClosureMismatch {
                closure: "run manifest path generation",
                expected_count: expected_generation.get(),
                actual_count: member.path_generation.get(),
            });
        }

        let commit_ref_key = commit_revision_ref_key(
            context.root_id,
            operation.commit_id,
            operation.tree_manifest_revision_id,
        );
        let commit_ref_payload = self
            .read_payload(context, MetadataFamily::RevisionRef, &commit_ref_key)?
            .ok_or(CommitError::ClosureMismatch {
                closure: "run manifest commit ref",
                expected_count: 1,
                actual_count: 0,
            })?;
        let commit_ref = RevisionRefRecord::decode(&commit_ref_payload)?;
        if commit_ref.reference_epoch_at_add > staged_revision.record.reference_epoch {
            return Err(CommitError::ClosureMismatch {
                closure: "run manifest commit ref epoch",
                expected_count: staged_revision.record.reference_epoch.get(),
                actual_count: commit_ref.reference_epoch_at_add.get(),
            });
        }
        plan.assert_value(
            MetadataFamily::RevisionRef,
            commit_ref_key,
            Some(commit_ref_payload),
        )?;

        let next_revision_epoch = ReferenceEpoch::new(
            staged_revision
                .record
                .reference_epoch
                .get()
                .checked_add(1)
                .ok_or(CommitError::CounterOverflow {
                    field: "reference_epoch",
                })?,
        );
        let mut next_staged_revision = staged_revision.record.clone();
        next_staged_revision.reference_epoch = next_revision_epoch;
        next_staged_revision.strong_reference_count = next_staged_revision
            .strong_reference_count
            .checked_add(1)
            .ok_or(CommitError::CounterOverflow {
                field: "strong_reference_count",
            })?;
        next_staged_revision.last_zero_ref_version = None;
        plan.replace(
            MetadataFamily::ArtifactRevision,
            artifact_revision_key(context.root_id, operation.tree_manifest_revision_id),
            staged_revision.payload,
            next_staged_revision.encode()?,
        )?;
        plan.put_absent(
            MetadataFamily::RevisionRef,
            path_revision_ref_key(
                context.root_id,
                operation.source_workspace_incarnation_id,
                &path,
                operation.tree_manifest_revision_id,
            ),
            RevisionRefRecord {
                reference_epoch_at_add: next_revision_epoch,
            }
            .encode()?,
        )?;

        let next_path = PathEntry {
            generation: member.path_generation,
            artifact_revision_id: member.artifact_revision_id,
            body_digest_uri: member.body_digest_uri,
            manifest_digest_uri: member.manifest_digest_uri,
            logical_size: member.logical_size,
            dependency_count: member.dependency_count,
            dependency_depth: member.dependency_depth,
            content_type: member.content_type,
            producer: member.producer,
            manifest_id: member.manifest_id,
            typed_index_projection: member.typed_projection,
        };
        match current_path {
            None => plan.put_absent(MetadataFamily::PathCurrent, path_key, next_path.encode()?)?,
            Some(current) => {
                if current.record.artifact_revision_id == operation.tree_manifest_revision_id {
                    return Err(CommitError::ClosureMismatch {
                        closure: "run manifest revision identity",
                        expected_count: 0,
                        actual_count: 1,
                    });
                }
                plan.replace(
                    MetadataFamily::PathCurrent,
                    path_key,
                    current.payload,
                    next_path.encode()?,
                )?;
                let old_ref_key = path_revision_ref_key(
                    context.root_id,
                    operation.source_workspace_incarnation_id,
                    &path,
                    current.record.artifact_revision_id,
                );
                let old_ref_payload = self
                    .read_payload(context, MetadataFamily::RevisionRef, &old_ref_key)?
                    .ok_or(CommitError::ClosureMismatch {
                        closure: "old run manifest path ref",
                        expected_count: 1,
                        actual_count: 0,
                    })?;
                let old_ref = RevisionRefRecord::decode(&old_ref_payload)?;
                let old_revision =
                    self.load_revision(context, current.record.artifact_revision_id)?;
                if old_ref.reference_epoch_at_add > old_revision.record.reference_epoch {
                    return Err(CommitError::ClosureMismatch {
                        closure: "old run manifest path ref epoch",
                        expected_count: old_revision.record.reference_epoch.get(),
                        actual_count: old_ref.reference_epoch_at_add.get(),
                    });
                }
                let next_old = remove_revision_reference(
                    &old_revision.record,
                    next_commit_version,
                    current.record.artifact_revision_id,
                )?;
                plan.delete(MetadataFamily::RevisionRef, old_ref_key, old_ref_payload)?;
                plan.replace(
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(context.root_id, current.record.artifact_revision_id),
                    old_revision.payload,
                    next_old.encode()?,
                )?;
                if next_old.strong_reference_count == 0 {
                    plan.put_absent(
                        MetadataFamily::GcCandidate,
                        gc_candidate_key(
                            context.root_id,
                            current.record.artifact_revision_id,
                            next_old.reference_epoch,
                        ),
                        GcCandidateRecord {
                            last_zero_ref_version: next_commit_version,
                            claim_state: GcClaimState::Candidate,
                            retry_count: 0,
                            quarantine_evidence: None,
                        }
                        .encode()?,
                    )?;
                }
            }
        }

        plan.events
            .push(change_event_projection(&ChangeEventRecord {
                workbench_id: operation.workbench_id.clone(),
                workspace_incarnation_id: operation.source_workspace_incarnation_id,
                kind: ChangeEventKind::CommitAdvanced,
                artifact_revision_id: Some(operation.tree_manifest_revision_id),
                commit_id: Some(operation.commit_id),
                operation_id: Some(operation.operation_id),
                path: Some(path),
                before: TypedProjection::default(),
                after: TypedProjection::default(),
            })?);
        Ok(())
    }

    pub fn abort_build(
        &self,
        request: AbortBuildCommitRequest,
    ) -> Result<BuildCommitOutcome, CommitError> {
        let loaded = self.load_build_operation(request.context, request.operation_id)?;
        if !matches!(
            loaded.record.phase,
            BuildCommitPhase::Building | BuildCommitPhase::Sealing
        ) {
            return Err(CommitError::PhaseMismatch {
                expected: "Building or unpublished Sealing",
                actual: build_phase_name(loaded.record.phase),
            });
        }
        let mut next = loaded.record.clone();
        next.phase = BuildCommitPhase::Aborting;
        next.terminal_error = Some(request.terminal_error);
        let mut plan = CommandPlan::default();
        plan.assert_value(
            MetadataFamily::Commit,
            commit_key(request.context.root_id, next.commit_id),
            None,
        )?;
        let head_key = workbench_commit_head_key(
            request.context.root_id,
            next.source_workspace_incarnation_id,
        );
        if let Some(head) = self.read_commit_record(
            request.context,
            MetadataFamily::WorkbenchCommitHead,
            &head_key,
            WorkbenchCommitHeadRecord::decode,
        )? {
            if head.record.commit_id == next.commit_id {
                return Err(CommitError::HeadConflict);
            }
            plan.assert_value(
                MetadataFamily::WorkbenchCommitHead,
                head_key,
                Some(head.payload),
            )?;
        } else {
            plan.assert_value(MetadataFamily::WorkbenchCommitHead, head_key, None)?;
        }
        replace_operation(&mut plan, request.context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_build_outcome(result, request.operation_id)
    }

    pub fn cleanup_build(
        &self,
        request: BuildCommitStepRequest,
    ) -> Result<BuildCommitOutcome, CommitError> {
        validate_limit(request.limit, MAX_COMMIT_REVISION_BATCH_ROWS)?;
        let loaded = self.load_build_operation(request.context, request.operation_id)?;
        match loaded.record.phase {
            BuildCommitPhase::Aborting => {
                let mut next = loaded.record.clone();
                next.phase = BuildCommitPhase::Cleaning;
                let mut plan = CommandPlan::default();
                replace_operation(&mut plan, request.context.root_id, &loaded, &next)?;
                let payload = next.encode()?;
                let result = self.execute_plan(request.context, plan, payload)?;
                return decode_build_outcome(result, request.operation_id);
            }
            BuildCommitPhase::Cleaning => {}
            _ => {
                return Err(CommitError::PhaseMismatch {
                    expected: "Aborting or Cleaning",
                    actual: build_phase_name(loaded.record.phase),
                });
            }
        }

        let mut next = loaded.record.clone();
        let mut plan = CommandPlan::default();
        if next.cleanup_parent_count < next.parent_cursor {
            let remaining = next.parent_cursor - next.cleanup_parent_count;
            let count = remaining.min(request.limit as u32);
            let next_commit = next_commit_version(request.context)?;
            for _ in 0..count {
                let index = usize::try_from(next.parent_cursor - next.cleanup_parent_count - 1)
                    .expect("u32 fits usize");
                let parent_id = next.parent_commits[index];
                let consumer_key =
                    child_commit_consumer_key(request.context.root_id, parent_id, next.commit_id);
                let consumer = self
                    .read_payload(
                        request.context,
                        MetadataFamily::CommitConsumer,
                        &consumer_key,
                    )?
                    .ok_or(CommitError::ConsumerMissing)?;
                CommitConsumerRecord::decode(&consumer)?;
                let parent = self.load_commit(request.context, parent_id)?;
                require_sealed(&parent.record, parent_id)?;
                let updated = remove_consumer(&parent.record, next_commit)?;
                plan.replace(
                    MetadataFamily::Commit,
                    commit_key(request.context.root_id, parent_id),
                    parent.payload,
                    updated.encode()?,
                )?;
                plan.delete(MetadataFamily::CommitConsumer, consumer_key, consumer)?;
                next.cleanup_parent_count = next.cleanup_parent_count.checked_add(1).ok_or(
                    CommitError::CounterOverflow {
                        field: "cleanup_parent_count",
                    },
                )?;
            }
        } else if next.cleanup_revision_count < next.revision_ref_count {
            self.plan_build_revision_cleanup(request.context, request.limit, &mut next, &mut plan)?;
        } else if next.cleanup_member_count < next.member_count {
            let prefix = commit_member_prefix(request.context.root_id, next.commit_id);
            let rows = self.store.scan_prefix_at(
                request.context.root_id,
                request.context.placement_generation,
                request.context.owner_epoch,
                MetadataFamily::CommitMember,
                &prefix,
                request.context.read_version,
                None,
                request.limit,
            )?;
            if rows.is_empty() {
                return Err(CommitError::ClosureMismatch {
                    closure: "abort member cleanup",
                    expected_count: next.member_count,
                    actual_count: next.cleanup_member_count,
                });
            }
            for row in rows {
                decode_commit_member_key(request.context.root_id, next.commit_id, &row.key).ok_or(
                    CommitError::CorruptKey {
                        family: "CommitMember",
                    },
                )?;
                CommitMemberRecord::decode(&row.value)?;
                plan.delete(MetadataFamily::CommitMember, row.key, row.value)?;
                next.cleanup_member_count =
                    checked_add(next.cleanup_member_count, 1, "cleanup_member_count")?;
            }
            if next.cleanup_member_count > next.member_count {
                return Err(CommitError::ClosureMismatch {
                    closure: "abort member cleanup",
                    expected_count: next.member_count,
                    actual_count: next.cleanup_member_count,
                });
            }
        } else {
            let member_rows = self.scan_prefix(
                request.context,
                MetadataFamily::CommitMember,
                &commit_member_prefix(request.context.root_id, next.commit_id),
                1,
            )?;
            let revision_rows = self.scan_prefix(
                request.context,
                MetadataFamily::RevisionRef,
                &commit_revision_ref_prefix(request.context.root_id, next.commit_id),
                1,
            )?;
            if !member_rows.is_empty() || !revision_rows.is_empty() {
                return Err(CommitError::ClosureMismatch {
                    closure: "abort cleanup residue",
                    expected_count: 0,
                    actual_count: (member_rows.len() + revision_rows.len()) as u64,
                });
            }
            let hold_key =
                build_commit_history_hold_key(request.context.root_id, request.operation_id);
            let hold = self
                .read_payload(request.context, MetadataFamily::HistoryHold, &hold_key)?
                .ok_or(CommitError::OperationNotFound)?;
            HistoryHoldRecord::decode(&hold)?;
            plan.delete(MetadataFamily::HistoryHold, hold_key, hold)?;
            next.phase = BuildCommitPhase::Cleaned;
            next.history_hold_released = true;
        }
        replace_operation(&mut plan, request.context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_build_outcome(result, request.operation_id)
    }

    fn plan_build_revision_cleanup(
        &self,
        context: RootWriteContext,
        limit: usize,
        operation: &mut BuildCommitOperationRecord,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        let prefix = commit_revision_ref_prefix(context.root_id, operation.commit_id);
        let rows = self.scan_prefix(context, MetadataFamily::RevisionRef, &prefix, limit)?;
        if rows.is_empty() {
            return Err(CommitError::ClosureMismatch {
                closure: "abort revision cleanup",
                expected_count: operation.revision_ref_count,
                actual_count: operation.cleanup_revision_count,
            });
        }
        let zero_version = next_commit_version(context)?;
        for row in rows {
            let revision_id = decode_commit_revision_ref_key(&prefix, &row.key)?;
            let reference = RevisionRefRecord::decode(&row.value)?;
            let revision = self.load_revision(context, revision_id)?;
            if reference.reference_epoch_at_add > revision.record.reference_epoch {
                return Err(CommitError::ClosureMismatch {
                    closure: "revision reference epoch",
                    expected_count: revision.record.reference_epoch.get(),
                    actual_count: reference.reference_epoch_at_add.get(),
                });
            }
            let updated = remove_revision_reference(&revision.record, zero_version, revision_id)?;
            plan.replace(
                MetadataFamily::ArtifactRevision,
                artifact_revision_key(context.root_id, revision_id),
                revision.payload,
                updated.encode()?,
            )?;
            plan.delete(MetadataFamily::RevisionRef, row.key, row.value)?;
            if updated.strong_reference_count == 0 {
                plan.put_absent(
                    MetadataFamily::GcCandidate,
                    gc_candidate_key(context.root_id, revision_id, updated.reference_epoch),
                    GcCandidateRecord {
                        last_zero_ref_version: zero_version,
                        claim_state: GcClaimState::Candidate,
                        retry_count: 0,
                        quarantine_evidence: None,
                    }
                    .encode()?,
                )?;
            }
            operation.cleanup_revision_count = checked_add(
                operation.cleanup_revision_count,
                1,
                "cleanup_revision_count",
            )?;
        }
        if operation.cleanup_revision_count > operation.revision_ref_count {
            return Err(CommitError::ClosureMismatch {
                closure: "abort revision cleanup",
                expected_count: operation.revision_ref_count,
                actual_count: operation.cleanup_revision_count,
            });
        }
        Ok(())
    }

    pub fn begin_retirement(
        &self,
        request: BeginCommitRetirementRequest,
    ) -> Result<RetireCommitOutcome, CommitError> {
        let commit = self.load_commit(request.context, request.commit_id)?;
        require_sealed(&commit.record, request.commit_id)?;
        if commit.record.consumer_count != 0
            || commit.record.consumer_epoch != request.expected_consumer_epoch
        {
            return Err(CommitError::ClosureMismatch {
                closure: "zero-consumer retirement claim",
                expected_count: request.expected_consumer_epoch.get(),
                actual_count: commit.record.consumer_epoch.get(),
            });
        }
        let mut retiring_commit = commit.record.clone();
        retiring_commit.state = CommitState::Retiring;
        let mut operation = CommitRetireOperationRecord {
            operation_id: request.operation_id,
            identity_digest: [0; SHA256_BYTES],
            commit_id: request.commit_id,
            claimed_consumer_epoch: request.expected_consumer_epoch,
            member_count: commit.record.member_count,
            member_digest: commit.record.member_digest,
            revision_count: commit.record.unique_revision_count,
            revision_digest: commit.record.revision_digest,
            parent_commits: commit.record.parent_commits.clone(),
            parent_digest: commit.record.parent_digest,
            phase: CommitRetirePhase::Claiming,
            released_member_count: 0,
            released_member_digest: [0; SHA256_BYTES],
            released_revision_count: 0,
            released_revision_digest: [0; SHA256_BYTES],
            released_parent_count: 0,
            released_parent_digest: [0; SHA256_BYTES],
            terminal_error: None,
        };
        operation.seal_identity();
        let payload = operation.encode()?;
        let operation_key = operation_key(
            request.context.root_id,
            OperationKind::CommitRetire,
            request.operation_id,
        );
        let mut plan = CommandPlan::default();
        plan.put_absent(MetadataFamily::Operation, operation_key, payload.clone())?;
        plan.replace(
            MetadataFamily::Commit,
            commit_key(request.context.root_id, request.commit_id),
            commit.payload,
            retiring_commit.encode()?,
        )?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_retire_outcome(result, request.operation_id)
    }

    pub fn release_retired_commit(
        &self,
        request: BuildCommitStepRequest,
    ) -> Result<RetireCommitOutcome, CommitError> {
        validate_limit(request.limit, MAX_COMMIT_RETIRE_MEMBER_BATCH_ROWS)?;
        let loaded = self.load_retire_operation(request.context, request.operation_id)?;
        match loaded.record.phase {
            CommitRetirePhase::Claiming => {
                let mut next = loaded.record.clone();
                next.phase = CommitRetirePhase::Releasing;
                let mut plan = CommandPlan::default();
                replace_retire_operation(&mut plan, request.context.root_id, &loaded, &next)?;
                let payload = next.encode()?;
                let result = self.execute_plan(request.context, plan, payload)?;
                return decode_retire_outcome(result, request.operation_id);
            }
            CommitRetirePhase::Releasing => {}
            _ => {
                return Err(CommitError::PhaseMismatch {
                    expected: "Claiming or Releasing",
                    actual: retire_phase_name(loaded.record.phase),
                });
            }
        }
        let commit = self.load_commit(request.context, loaded.record.commit_id)?;
        if commit.record.state != CommitState::Retiring
            || commit.record.consumer_count != 0
            || commit.record.consumer_epoch != loaded.record.claimed_consumer_epoch
        {
            return Err(CommitError::CommitUnavailable {
                commit_id: loaded.record.commit_id,
                state: commit.record.state,
            });
        }
        let mut next = loaded.record.clone();
        let mut plan = CommandPlan::default();
        plan.assert_value(
            MetadataFamily::Commit,
            commit_key(request.context.root_id, next.commit_id),
            Some(commit.payload.clone()),
        )?;

        if next.released_revision_count < next.revision_count {
            self.plan_retire_revision_release(
                request.context,
                request.limit.min(MAX_COMMIT_REVISION_BATCH_ROWS),
                &mut next,
                &mut plan,
            )?;
        } else if (next.released_parent_count as usize) < next.parent_commits.len() {
            self.plan_retire_parent_release(
                request.context,
                request.limit.min(MAX_COMMIT_PARENT_BATCH_ROWS),
                &mut next,
                &mut plan,
            )?;
        } else if next.released_member_count < next.member_count {
            self.plan_retire_member_release(
                request.context,
                request.limit.min(MAX_COMMIT_RETIRE_MEMBER_BATCH_ROWS),
                &mut next,
                &mut plan,
            )?;
        } else {
            verify_retire_seals(&next)?;
            let member_rows = self.scan_prefix(
                request.context,
                MetadataFamily::CommitMember,
                &commit_member_prefix(request.context.root_id, next.commit_id),
                1,
            )?;
            let revision_rows = self.scan_prefix(
                request.context,
                MetadataFamily::RevisionRef,
                &commit_revision_ref_prefix(request.context.root_id, next.commit_id),
                1,
            )?;
            if !member_rows.is_empty() || !revision_rows.is_empty() {
                return Err(CommitError::ClosureMismatch {
                    closure: "retirement residue",
                    expected_count: 0,
                    actual_count: (member_rows.len() + revision_rows.len()) as u64,
                });
            }
            let mut retired = commit.record.clone();
            retired.state = CommitState::Retired;
            plan.replace_existing_predicate(
                MetadataFamily::Commit,
                commit_key(request.context.root_id, next.commit_id),
                commit.payload,
                retired.encode()?,
            )?;
            next.phase = CommitRetirePhase::Complete;
        }
        replace_retire_operation(&mut plan, request.context.root_id, &loaded, &next)?;
        let payload = next.encode()?;
        let result = self.execute_plan(request.context, plan, payload)?;
        decode_retire_outcome(result, request.operation_id)
    }

    fn plan_retire_revision_release(
        &self,
        context: RootWriteContext,
        limit: usize,
        operation: &mut CommitRetireOperationRecord,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        let prefix = commit_revision_ref_prefix(context.root_id, operation.commit_id);
        let rows = self.scan_prefix(context, MetadataFamily::RevisionRef, &prefix, limit)?;
        if rows.is_empty() {
            return Err(CommitError::ClosureMismatch {
                closure: "retire revision",
                expected_count: operation.revision_count,
                actual_count: operation.released_revision_count,
            });
        }
        let zero_version = next_commit_version(context)?;
        for row in rows {
            let revision_id = decode_commit_revision_ref_key(&prefix, &row.key)?;
            let reference = RevisionRefRecord::decode(&row.value)?;
            let revision = self.load_revision(context, revision_id)?;
            if reference.reference_epoch_at_add > revision.record.reference_epoch {
                return Err(CommitError::ClosureMismatch {
                    closure: "revision reference epoch",
                    expected_count: revision.record.reference_epoch.get(),
                    actual_count: reference.reference_epoch_at_add.get(),
                });
            }
            operation.released_revision_digest = advance_commit_revision_rolling_digest(
                operation.released_revision_digest,
                operation.released_revision_count,
                revision_id,
                &row.value,
            );
            operation.released_revision_count = checked_add(
                operation.released_revision_count,
                1,
                "released_revision_count",
            )?;
            if operation.released_revision_count > operation.revision_count {
                return Err(CommitError::ClosureMismatch {
                    closure: "retire revision",
                    expected_count: operation.revision_count,
                    actual_count: operation.released_revision_count,
                });
            }
            let updated = remove_revision_reference(&revision.record, zero_version, revision_id)?;
            plan.replace(
                MetadataFamily::ArtifactRevision,
                artifact_revision_key(context.root_id, revision_id),
                revision.payload,
                updated.encode()?,
            )?;
            plan.delete(MetadataFamily::RevisionRef, row.key, row.value)?;
            if updated.strong_reference_count == 0 {
                plan.put_absent(
                    MetadataFamily::GcCandidate,
                    gc_candidate_key(context.root_id, revision_id, updated.reference_epoch),
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
        Ok(())
    }

    fn plan_retire_parent_release(
        &self,
        context: RootWriteContext,
        limit: usize,
        operation: &mut CommitRetireOperationRecord,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        let start = operation.released_parent_count as usize;
        let end = (start + limit).min(operation.parent_commits.len());
        let zero_version = next_commit_version(context)?;
        for index in start..end {
            let parent_id = operation.parent_commits[index];
            let consumer_key =
                child_commit_consumer_key(context.root_id, parent_id, operation.commit_id);
            let consumer = self
                .read_payload(context, MetadataFamily::CommitConsumer, &consumer_key)?
                .ok_or(CommitError::ConsumerMissing)?;
            CommitConsumerRecord::decode(&consumer)?;
            let parent = self.load_commit(context, parent_id)?;
            require_sealed(&parent.record, parent_id)?;
            let updated = remove_consumer(&parent.record, zero_version)?;
            plan.replace(
                MetadataFamily::Commit,
                commit_key(context.root_id, parent_id),
                parent.payload,
                updated.encode()?,
            )?;
            plan.delete(MetadataFamily::CommitConsumer, consumer_key, consumer)?;
            operation.released_parent_digest = advance_commit_parent_rolling_digest(
                operation.released_parent_digest,
                u64::from(operation.released_parent_count),
                parent_id,
            );
            operation.released_parent_count = operation
                .released_parent_count
                .checked_add(1)
                .ok_or(CommitError::CounterOverflow {
                    field: "released_parent_count",
                })?;
        }
        Ok(())
    }

    fn plan_retire_member_release(
        &self,
        context: RootWriteContext,
        limit: usize,
        operation: &mut CommitRetireOperationRecord,
        plan: &mut CommandPlan,
    ) -> Result<(), CommitError> {
        let prefix = commit_member_prefix(context.root_id, operation.commit_id);
        let rows = self.scan_prefix(context, MetadataFamily::CommitMember, &prefix, limit)?;
        if rows.is_empty() {
            return Err(CommitError::ClosureMismatch {
                closure: "retire member",
                expected_count: operation.member_count,
                actual_count: operation.released_member_count,
            });
        }
        for row in rows {
            let path = decode_commit_member_key(context.root_id, operation.commit_id, &row.key)
                .ok_or(CommitError::CorruptKey {
                    family: "CommitMember",
                })?;
            let member = CommitMemberRecord::decode(&row.value)?;
            let row_digest = commit_member_row_digest(&path, &member)?;
            operation.released_member_digest = advance_commit_member_rolling_digest(
                operation.released_member_digest,
                operation.released_member_count,
                row_digest,
            );
            operation.released_member_count =
                checked_add(operation.released_member_count, 1, "released_member_count")?;
            if operation.released_member_count > operation.member_count {
                return Err(CommitError::ClosureMismatch {
                    closure: "retire member",
                    expected_count: operation.member_count,
                    actual_count: operation.released_member_count,
                });
            }
            plan.delete(MetadataFamily::CommitMember, row.key, row.value)?;
        }
        Ok(())
    }

    pub fn set_tag(&self, request: SetCommitTagRequest) -> Result<TagMutationOutcome, CommitError> {
        let workspace = get_visible_workspace_at(
            self.store,
            read_context(request.context),
            &request.workbench_id,
        )?
        .ok_or(CommitError::WorkspaceNotFound)?;
        let key = tag_key(
            request.context.root_id,
            workspace.incarnation_id,
            &request.tag,
        );
        let current = self.read_commit_record(
            request.context,
            MetadataFamily::Tag,
            &key,
            TagRecord::decode,
        )?;
        let generation = match current.as_ref() {
            None => Generation::new(1).expect("one is non-zero"),
            Some(current) => {
                Generation::new(current.record.tag_generation.get().checked_add(1).ok_or(
                    CommitError::CounterOverflow {
                        field: "tag_generation",
                    },
                )?)
                .expect("checked successor is non-zero")
            }
        };
        let next_tag = TagRecord {
            commit_id: request.commit_id,
            tag_generation: generation,
        };
        let mut plan = CommandPlan::default();
        if current.as_ref().map(|loaded| loaded.record.commit_id) != Some(request.commit_id) {
            let target = self.load_commit(request.context, request.commit_id)?;
            require_sealed(&target.record, request.commit_id)?;
            let target_next = add_consumer(&target.record)?;
            plan.replace(
                MetadataFamily::Commit,
                commit_key(request.context.root_id, request.commit_id),
                target.payload,
                target_next.encode()?,
            )?;
            plan.put_absent(
                MetadataFamily::CommitConsumer,
                tag_commit_consumer_key(
                    request.context.root_id,
                    request.commit_id,
                    workspace.incarnation_id,
                    &request.tag,
                ),
                CommitConsumerRecord {
                    consumer_epoch_at_add: target_next.consumer_epoch,
                }
                .encode(),
            )?;
            if let Some(current) = &current {
                let old_id = current.record.commit_id;
                let old = self.load_commit(request.context, old_id)?;
                require_sealed(&old.record, old_id)?;
                let consumer_key = tag_commit_consumer_key(
                    request.context.root_id,
                    old_id,
                    workspace.incarnation_id,
                    &request.tag,
                );
                let consumer = self
                    .read_payload(
                        request.context,
                        MetadataFamily::CommitConsumer,
                        &consumer_key,
                    )?
                    .ok_or(CommitError::ConsumerMissing)?;
                CommitConsumerRecord::decode(&consumer)?;
                let old_next = remove_consumer(&old.record, next_commit_version(request.context)?)?;
                plan.replace(
                    MetadataFamily::Commit,
                    commit_key(request.context.root_id, old_id),
                    old.payload,
                    old_next.encode()?,
                )?;
                plan.delete(MetadataFamily::CommitConsumer, consumer_key, consumer)?;
            }
        } else if current.is_some() {
            let consumer_key = tag_commit_consumer_key(
                request.context.root_id,
                request.commit_id,
                workspace.incarnation_id,
                &request.tag,
            );
            let consumer = self
                .read_payload(
                    request.context,
                    MetadataFamily::CommitConsumer,
                    &consumer_key,
                )?
                .ok_or(CommitError::ConsumerMissing)?;
            CommitConsumerRecord::decode(&consumer)?;
        }
        match current {
            None => plan.put_absent(MetadataFamily::Tag, key, next_tag.encode())?,
            Some(current) => {
                plan.replace(MetadataFamily::Tag, key, current.payload, next_tag.encode())?
            }
        }
        let deterministic = encode_tag_result(Some(next_tag));
        let result = self.execute_plan(request.context, plan, deterministic)?;
        decode_tag_outcome(result)
    }

    pub fn delete_tag(
        &self,
        request: DeleteCommitTagRequest,
    ) -> Result<TagMutationOutcome, CommitError> {
        let workspace = get_visible_workspace_at(
            self.store,
            read_context(request.context),
            &request.workbench_id,
        )?
        .ok_or(CommitError::WorkspaceNotFound)?;
        let key = tag_key(
            request.context.root_id,
            workspace.incarnation_id,
            &request.tag,
        );
        let current = self
            .read_commit_record(
                request.context,
                MetadataFamily::Tag,
                &key,
                TagRecord::decode,
            )?
            .ok_or(CommitError::TagNotFound)?;
        let old_id = current.record.commit_id;
        let old = self.load_commit(request.context, old_id)?;
        require_sealed(&old.record, old_id)?;
        let consumer_key = tag_commit_consumer_key(
            request.context.root_id,
            old_id,
            workspace.incarnation_id,
            &request.tag,
        );
        let consumer = self
            .read_payload(
                request.context,
                MetadataFamily::CommitConsumer,
                &consumer_key,
            )?
            .ok_or(CommitError::ConsumerMissing)?;
        CommitConsumerRecord::decode(&consumer)?;
        let updated = remove_consumer(&old.record, next_commit_version(request.context)?)?;
        let mut plan = CommandPlan::default();
        plan.replace(
            MetadataFamily::Commit,
            commit_key(request.context.root_id, old_id),
            old.payload,
            updated.encode()?,
        )?;
        plan.delete(MetadataFamily::CommitConsumer, consumer_key, consumer)?;
        plan.delete(MetadataFamily::Tag, key, current.payload)?;
        let deterministic = encode_tag_result(None);
        let result = self.execute_plan(request.context, plan, deterministic)?;
        decode_tag_outcome(result)
    }

    fn load_build_operation(
        &self,
        context: RootWriteContext,
        operation_id: OperationId,
    ) -> Result<Loaded<BuildCommitOperationRecord>, CommitError> {
        self.read_commit_record(
            context,
            MetadataFamily::Operation,
            &operation_key(context.root_id, OperationKind::BuildCommit, operation_id),
            BuildCommitOperationRecord::decode,
        )?
        .ok_or(CommitError::OperationNotFound)
    }

    fn load_retire_operation(
        &self,
        context: RootWriteContext,
        operation_id: OperationId,
    ) -> Result<Loaded<CommitRetireOperationRecord>, CommitError> {
        self.read_commit_record(
            context,
            MetadataFamily::Operation,
            &operation_key(context.root_id, OperationKind::CommitRetire, operation_id),
            CommitRetireOperationRecord::decode,
        )?
        .ok_or(CommitError::OperationNotFound)
    }

    fn load_commit(
        &self,
        context: RootWriteContext,
        commit_id: CommitId,
    ) -> Result<Loaded<CommitRecord>, CommitError> {
        self.read_commit_record(
            context,
            MetadataFamily::Commit,
            &commit_key(context.root_id, commit_id),
            CommitRecord::decode,
        )?
        .ok_or(CommitError::CommitNotFound { commit_id })
    }

    fn load_revision(
        &self,
        context: RootWriteContext,
        revision_id: ArtifactRevisionId,
    ) -> Result<Loaded<ArtifactRevisionRecord>, CommitError> {
        let loaded = self
            .read_commit_record(
                context,
                MetadataFamily::ArtifactRevision,
                &artifact_revision_key(context.root_id, revision_id),
                ArtifactRevisionRecord::decode,
            )?
            .ok_or(CommitError::RevisionNotFound { revision_id })?;
        if loaded.record.state != RevisionState::Available {
            return Err(CommitError::RevisionUnavailable { revision_id });
        }
        Ok(loaded)
    }

    fn read_commit_record<T, E>(
        &self,
        context: RootWriteContext,
        family: MetadataFamily,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<T, E>,
    ) -> Result<Option<Loaded<T>>, CommitError>
    where
        CommitError: From<E>,
    {
        self.read_payload(context, family, key)?
            .map(|payload| {
                let record = decode(&payload).map_err(CommitError::from)?;
                Ok(Loaded { payload, record })
            })
            .transpose()
    }

    fn read_payload(
        &self,
        context: RootWriteContext,
        family: MetadataFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, CommitError> {
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

    fn scan_prefix(
        &self,
        context: RootWriteContext,
        family: MetadataFamily,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<super::engine::MetadataScanItem>, CommitError> {
        self.store
            .scan_prefix_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                family,
                prefix,
                context.read_version,
                None,
                limit,
            )
            .map_err(Into::into)
    }

    fn noop_build(
        &self,
        context: RootWriteContext,
        loaded: Loaded<BuildCommitOperationRecord>,
    ) -> Result<BuildCommitOutcome, CommitError> {
        let payload = loaded.record.encode()?;
        let result = self.execute_plan(context, CommandPlan::default(), payload)?;
        decode_build_outcome(result, loaded.record.operation_id)
    }

    fn execute_plan(
        &self,
        context: RootWriteContext,
        plan: CommandPlan,
        deterministic_result: Vec<u8>,
    ) -> Result<MetadataCommandResult, CommitError> {
        let command = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: context.root_id,
            logical_shard_id: context.logical_shard_id,
            placement_generation: context.placement_generation,
            owner_epoch: context.owner_epoch,
            request_id: context.request_id,
            command_digest: nokv_types::CommandDigest::from_bytes([0; SHA256_BYTES]),
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
}

#[derive(Clone, Debug)]
struct Loaded<T> {
    payload: Vec<u8>,
    record: T,
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
    ) -> Result<(), CommitError> {
        if !self.exact_keys.insert((family, key.clone())) {
            return Err(CommitError::DuplicateCommandKey { family });
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
    ) -> Result<(), CommitError> {
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
    ) -> Result<(), CommitError> {
        self.assert_value(family, key.clone(), Some(expected))?;
        self.mutations.push(CommandMutation::Put {
            family,
            key: key.clone(),
            value,
        });
        self.history.push(HistoryProjection { family, key });
        Ok(())
    }

    /// Add a replacement after the exact predicate was installed separately.
    fn replace_existing_predicate(
        &mut self,
        family: MetadataFamily,
        key: Vec<u8>,
        expected: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), CommitError> {
        if !self.exact_keys.contains(&(family, key.clone())) {
            return self.replace(family, key, expected, value);
        }
        let predicate_matches = self.predicates.iter().any(|predicate| {
            matches!(
                predicate,
                CommandPredicate::Value {
                    family: predicate_family,
                    key: predicate_key,
                    expected: Some(predicate_expected),
                } if *predicate_family == family
                    && predicate_key == &key
                    && predicate_expected == &expected
            )
        });
        if !predicate_matches {
            return Err(CommitError::DuplicateCommandKey { family });
        }
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
    ) -> Result<(), CommitError> {
        self.assert_value(family, key.clone(), Some(expected))?;
        self.mutations.push(CommandMutation::Delete {
            family,
            key: key.clone(),
        });
        self.history.push(HistoryProjection { family, key });
        Ok(())
    }
}

fn replace_operation(
    plan: &mut CommandPlan,
    root: RootId,
    loaded: &Loaded<BuildCommitOperationRecord>,
    next: &BuildCommitOperationRecord,
) -> Result<(), CommitError> {
    plan.replace(
        MetadataFamily::Operation,
        operation_key(root, OperationKind::BuildCommit, loaded.record.operation_id),
        loaded.payload.clone(),
        next.encode()?,
    )
}

fn replace_retire_operation(
    plan: &mut CommandPlan,
    root: RootId,
    loaded: &Loaded<CommitRetireOperationRecord>,
    next: &CommitRetireOperationRecord,
) -> Result<(), CommitError> {
    plan.replace(
        MetadataFamily::Operation,
        operation_key(
            root,
            OperationKind::CommitRetire,
            loaded.record.operation_id,
        ),
        loaded.payload.clone(),
        next.encode()?,
    )
}

fn read_context(context: RootWriteContext) -> RootReadContext {
    RootReadContext {
        root_id: context.root_id,
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        read_version: context.read_version,
    }
}

fn next_commit_version(context: RootWriteContext) -> Result<CommitVersion, CommitError> {
    CommitVersion::new(context.read_version.get().checked_add(1).ok_or(
        CommitError::CounterOverflow {
            field: "commit_version",
        },
    )?)
    .map_err(|_| CommitError::CounterOverflow {
        field: "commit_version",
    })
}

fn checked_add(value: u64, increment: u64, field: &'static str) -> Result<u64, CommitError> {
    value
        .checked_add(increment)
        .ok_or(CommitError::CounterOverflow { field })
}

fn validate_limit(requested: usize, max: usize) -> Result<(), CommitError> {
    if (1..=max).contains(&requested) {
        Ok(())
    } else {
        Err(CommitError::InvalidBatchLimit { requested, max })
    }
}

fn require_build_phase(
    operation: &BuildCommitOperationRecord,
    expected: BuildCommitPhase,
) -> Result<(), CommitError> {
    if operation.phase == expected {
        Ok(())
    } else {
        Err(CommitError::PhaseMismatch {
            expected: build_phase_name(expected),
            actual: build_phase_name(operation.phase),
        })
    }
}

const fn build_phase_name(phase: BuildCommitPhase) -> &'static str {
    match phase {
        BuildCommitPhase::Building => "Building",
        BuildCommitPhase::Sealing => "Sealing",
        BuildCommitPhase::Complete => "Complete",
        BuildCommitPhase::Aborting => "Aborting",
        BuildCommitPhase::Cleaning => "Cleaning",
        BuildCommitPhase::Cleaned => "Cleaned",
        BuildCommitPhase::Quarantined => "Quarantined",
    }
}

const fn retire_phase_name(phase: CommitRetirePhase) -> &'static str {
    match phase {
        CommitRetirePhase::Claiming => "Claiming",
        CommitRetirePhase::Releasing => "Releasing",
        CommitRetirePhase::Complete => "Complete",
        CommitRetirePhase::Quarantined => "Quarantined",
    }
}

fn require_sealed(record: &CommitRecord, commit_id: CommitId) -> Result<(), CommitError> {
    if record.state == CommitState::Sealed {
        Ok(())
    } else {
        Err(CommitError::CommitUnavailable {
            commit_id,
            state: record.state,
        })
    }
}

fn add_consumer(record: &CommitRecord) -> Result<CommitRecord, CommitError> {
    let mut next = record.clone();
    next.consumer_count = checked_add(next.consumer_count, 1, "consumer_count")?;
    next.consumer_epoch =
        ConsumerEpoch::new(checked_add(next.consumer_epoch.get(), 1, "consumer_epoch")?);
    next.last_zero_consumer_version = None;
    Ok(next)
}

fn remove_consumer(
    record: &CommitRecord,
    zero_version: CommitVersion,
) -> Result<CommitRecord, CommitError> {
    let mut next = record.clone();
    next.consumer_count = next
        .consumer_count
        .checked_sub(1)
        .ok_or(CommitError::ConsumerCountUnderflow)?;
    next.consumer_epoch =
        ConsumerEpoch::new(checked_add(next.consumer_epoch.get(), 1, "consumer_epoch")?);
    next.last_zero_consumer_version = (next.consumer_count == 0).then_some(zero_version);
    Ok(next)
}

fn remove_revision_reference(
    record: &ArtifactRevisionRecord,
    zero_version: CommitVersion,
    revision_id: ArtifactRevisionId,
) -> Result<ArtifactRevisionRecord, CommitError> {
    let mut next = record.clone();
    next.strong_reference_count =
        next.strong_reference_count
            .checked_sub(1)
            .ok_or(CommitError::ClosureMismatch {
                closure: "revision reference count",
                expected_count: 1,
                actual_count: 0,
            })?;
    next.reference_epoch = ReferenceEpoch::new(checked_add(
        next.reference_epoch.get(),
        1,
        "reference_epoch",
    )?);
    next.last_zero_ref_version = (next.strong_reference_count == 0).then_some(zero_version);
    if next.state != RevisionState::Available {
        return Err(CommitError::RevisionUnavailable { revision_id });
    }
    Ok(next)
}

fn decode_commit_revision_ref_key(
    prefix: &[u8],
    key: &[u8],
) -> Result<ArtifactRevisionId, CommitError> {
    if key.len() != prefix.len() + FIXED_ID_BYTES || !key.starts_with(prefix) {
        return Err(CommitError::CorruptKey {
            family: "RevisionRef(Commit)",
        });
    }
    Ok(ArtifactRevisionId::from_bytes(
        key[prefix.len()..]
            .try_into()
            .expect("validated revision-id suffix width"),
    ))
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

/// Bind the public tree digest to the exact frozen `CommitMember` closure.
///
/// The rolling member digest already commits to every normalized path and the
/// complete encoded member payload in canonical key order. Encoding that
/// digest as a SHA-256 URI avoids a second unbounded scan during finalization.
fn member_tree_digest_uri(member_digest: [u8; SHA256_BYTES]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut uri = String::with_capacity("sha256:".len() + SHA256_BYTES * 2);
    uri.push_str("sha256:");
    for byte in member_digest {
        uri.push(char::from(HEX[usize::from(byte >> 4)]));
        uri.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    uri
}

fn verify_retire_seals(operation: &CommitRetireOperationRecord) -> Result<(), CommitError> {
    for (name, expected, actual) in [
        (
            "retire member",
            operation.member_count,
            operation.released_member_count,
        ),
        (
            "retire revision",
            operation.revision_count,
            operation.released_revision_count,
        ),
        (
            "retire parent",
            operation.parent_commits.len() as u64,
            u64::from(operation.released_parent_count),
        ),
    ] {
        if expected != actual {
            return Err(CommitError::ClosureMismatch {
                closure: name,
                expected_count: expected,
                actual_count: actual,
            });
        }
    }
    if operation.member_digest != operation.released_member_digest
        || operation.revision_digest != operation.released_revision_digest
        || operation.parent_digest != operation.released_parent_digest
    {
        return Err(CommitError::ClosureMismatch {
            closure: "retire digest",
            expected_count: 3,
            actual_count: 0,
        });
    }
    Ok(())
}

fn decode_build_outcome(
    result: MetadataCommandResult,
    operation_id: OperationId,
) -> Result<BuildCommitOutcome, CommitError> {
    let operation = BuildCommitOperationRecord::decode(&result.deterministic_result)?;
    if operation.operation_id != operation_id {
        return Err(CommitError::ReplayResultMismatch);
    }
    Ok(BuildCommitOutcome {
        commit_version: result.commit_version,
        operation,
        replayed: result.replayed,
    })
}

fn decode_retire_outcome(
    result: MetadataCommandResult,
    operation_id: OperationId,
) -> Result<RetireCommitOutcome, CommitError> {
    let operation = CommitRetireOperationRecord::decode(&result.deterministic_result)?;
    if operation.operation_id != operation_id {
        return Err(CommitError::ReplayResultMismatch);
    }
    Ok(RetireCommitOutcome {
        commit_version: result.commit_version,
        operation,
        replayed: result.replayed,
    })
}

fn encode_tag_result(tag: Option<TagRecord>) -> Vec<u8> {
    match tag {
        None => vec![0],
        Some(tag) => {
            let mut result = vec![1];
            result.extend_from_slice(&tag.encode());
            result
        }
    }
}

fn decode_tag_outcome(result: MetadataCommandResult) -> Result<TagMutationOutcome, CommitError> {
    let tag = match result.deterministic_result.split_first() {
        Some((0, [])) => None,
        Some((1, payload)) => Some(TagRecord::decode(payload)?),
        _ => return Err(CommitError::ReplayResultMismatch),
    };
    Ok(TagMutationOutcome {
        commit_version: result.commit_version,
        tag,
        replayed: result.replayed,
    })
}

#[cfg(test)]
mod tests {
    use nokv_types::{
        CommandDigest, LogicalShardId, NormalizedRelativePath, OwnerEpoch, PlacementGeneration,
        RequestId, RootActivationState, WorkspaceIncarnationId,
    };
    use tempfile::tempdir;

    use super::super::namespace::create_visible_workspace;
    use super::super::path_current_key;
    use super::super::publication_records::PathEntry;
    use super::*;

    const LARGE_CLOSURE_ROWS: usize = 300;

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

    fn request(value: u128) -> RequestId {
        RequestId::from_bytes(value.to_be_bytes())
    }

    fn operation(value: u128) -> OperationId {
        OperationId::from_bytes(value.to_be_bytes())
    }

    fn revision(value: u128) -> ArtifactRevisionId {
        ArtifactRevisionId::from_bytes(value.to_be_bytes())
    }

    fn commit(value: u8) -> CommitId {
        CommitId::from_bytes([value; SHA256_BYTES])
    }

    fn incarnation() -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([9; FIXED_ID_BYTES])
    }

    fn workbench() -> WorkbenchId {
        WorkbenchId::new("commit-tests").unwrap()
    }

    fn next_request(counter: &mut u128) -> RequestId {
        let value = *counter;
        *counter += 1;
        request(value)
    }

    fn write_context(store: &AgentMetadataStore, counter: &mut u128) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            placement(),
            owner(),
            next_request(counter),
        )
        .unwrap()
    }

    fn fence_command(
        store: &AgentMetadataStore,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
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

    fn initialize(store: &AgentMetadataStore, counter: &mut u128) {
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(
                store,
                next_request(counter),
                RootFenceAction::Install {
                    layout_profile: nokv_types::RootLayoutProfile::SingleShardRoot,
                    layout_generation: nokv_types::RootLayoutGeneration::new(1).unwrap(),
                    partition_id: nokv_types::RootPartitionId::SINGLE_SHARD,
                },
            ))
            .unwrap();
        store
            .execute(&fence_command(
                store,
                next_request(counter),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
        create_visible_workspace(
            store,
            write_context(store, counter),
            &workbench(),
            incarnation(),
        )
        .unwrap();
    }

    fn ready_store(counter: &mut u128) -> AgentMetadataStore {
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        initialize(&store, counter);
        store
    }

    fn raw_put(
        store: &AgentMetadataStore,
        counter: &mut u128,
        records: Vec<(MetadataFamily, Vec<u8>, Vec<u8>)>,
    ) {
        let context = write_context(store, counter);
        let predicates = records
            .iter()
            .map(|(family, key, _)| CommandPredicate::Value {
                family: *family,
                key: key.clone(),
                expected: None,
            })
            .collect();
        let mutations = records
            .into_iter()
            .map(|(family, key, value)| CommandMutation::Put { family, key, value })
            .collect();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: context.root_id,
                    logical_shard_id: context.logical_shard_id,
                    placement_generation: context.placement_generation,
                    owner_epoch: context.owner_epoch,
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates,
                    mutations,
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: b"seed".to_vec(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn artifact(index: usize) -> ArtifactRevisionRecord {
        ArtifactRevisionRecord {
            logical_size: index as u64 + 1,
            body_digest_uri: format!("sha256:body-{index:04}"),
            manifest_digest_uri: format!("sha256:manifest-{index:04}"),
            block_count: 1,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; SHA256_BYTES],
            content_type: "application/octet-stream".to_owned(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        }
    }

    fn path_entry(index: usize, revision_id: ArtifactRevisionId) -> PathEntry {
        PathEntry {
            generation: Generation::new(1).unwrap(),
            artifact_revision_id: revision_id,
            body_digest_uri: format!("sha256:body-{index:04}"),
            manifest_digest_uri: format!("sha256:manifest-{index:04}"),
            logical_size: index as u64 + 1,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: Some("commit-test".to_owned()),
            manifest_id: Some(format!("manifest-{index:04}")),
            typed_index_projection: (index as u64).to_be_bytes().to_vec(),
        }
    }

    fn seed_paths(store: &AgentMetadataStore, counter: &mut u128, count: usize) {
        for start in (0..count).step_by(MAX_COMMIT_MEMBER_BATCH_ROWS) {
            let mut records = Vec::new();
            for index in start..(start + MAX_COMMIT_MEMBER_BATCH_ROWS).min(count) {
                let revision_id = revision(10_000 + index as u128);
                let path = NormalizedRelativePath::new(format!("data/{index:04}.bin")).unwrap();
                records.push((
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision_id),
                    artifact(index).encode().unwrap(),
                ));
                records.push((
                    MetadataFamily::PathCurrent,
                    path_current_key(root(), incarnation(), &path),
                    path_entry(index, revision_id).encode().unwrap(),
                ));
            }
            raw_put(store, counter, records);
        }
    }

    fn begin_request(
        context: RootWriteContext,
        operation_id: OperationId,
        commit_id: CommitId,
        tree_revision: ArtifactRevisionId,
        parents: Vec<CommitId>,
    ) -> BeginBuildCommitRequest {
        BeginBuildCommitRequest {
            context,
            operation_id,
            workbench_id: workbench(),
            expected_source_workspace_incarnation_id: incarnation(),
            commit_id,
            content_digest_uri: "sha256:content".to_owned(),
            manifest_digest_uri: "sha256:run-manifest".to_owned(),
            projection_input_digest: [0x17; SHA256_BYTES],
            tree_manifest_revision_id: tree_revision,
            replace: false,
            run_manifest_condition: CommitManifestCondition::CreateOnly,
            committed_at_unix_seconds: 1_700_000_000,
            expected_head_generation: None,
            producer: Some("test-agent".to_owned()),
            lineage_projection: vec![1, 2, 3],
            parent_commits: parents,
        }
    }

    fn finish_build(
        store: &AgentMetadataStore,
        counter: &mut u128,
        operation_id: OperationId,
    ) -> BuildCommitOutcome {
        let service = CommitService::new(store);
        loop {
            let outcome = service
                .build_members(BuildCommitStepRequest {
                    context: write_context(store, counter),
                    operation_id,
                    limit: 37,
                })
                .unwrap();
            if outcome.operation.members_complete {
                break;
            }
        }
        loop {
            let outcome = service
                .seal_revisions(BuildCommitStepRequest {
                    context: write_context(store, counter),
                    operation_id,
                    limit: 31,
                })
                .unwrap();
            if outcome.operation.revisions_complete {
                break;
            }
        }
        service
            .attach_parents(BuildCommitStepRequest {
                context: write_context(store, counter),
                operation_id,
                limit: MAX_COMMIT_PARENT_BATCH_ROWS,
            })
            .unwrap();
        service
            .begin_sealing(write_context(store, counter), operation_id)
            .unwrap();
        service
            .finalize_build(write_context(store, counter), operation_id)
            .unwrap()
    }

    fn read_current(
        store: &AgentMetadataStore,
        family: MetadataFamily,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        store
            .read_at(
                root(),
                placement(),
                owner(),
                family,
                key,
                store.current_read_version().unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn builds_and_retires_closures_larger_than_one_holt_command() {
        let mut counter = 100_u128;
        let store = ready_store(&mut counter);
        seed_paths(&store, &mut counter, LARGE_CLOSURE_ROWS);
        let service = CommitService::new(&store);
        let operation_id = operation(1);
        let begin_context = write_context(&store, &mut counter);
        let request = begin_request(
            begin_context,
            operation_id,
            commit(1),
            revision(10_000),
            vec![],
        );
        let begun = service.begin_build(request.clone()).unwrap();
        assert_eq!(
            begun.operation.source_read_version,
            begin_context.read_version
        );
        let replay = service.begin_build(request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation, begun.operation);

        let first_batch_context = write_context(&store, &mut counter);
        let first_batch_request = BuildCommitStepRequest {
            context: first_batch_context,
            operation_id,
            limit: 37,
        };
        let first_batch = service.build_members(first_batch_request).unwrap();
        let first_batch_replay = service.build_members(first_batch_request).unwrap();
        assert!(first_batch_replay.replayed);
        assert_eq!(first_batch_replay.operation, first_batch.operation);

        let complete = finish_build(&store, &mut counter, operation_id);
        assert_eq!(complete.operation.phase, BuildCommitPhase::Complete);
        assert_eq!(complete.operation.member_count, LARGE_CLOSURE_ROWS as u64);
        assert_eq!(
            complete.operation.revision_ref_count,
            LARGE_CLOSURE_ROWS as u64
        );
        let sealed = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(1)),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(sealed.member_count, LARGE_CLOSURE_ROWS as u64);
        assert_eq!(sealed.unique_revision_count, LARGE_CLOSURE_ROWS as u64);
        assert_eq!(
            sealed.tree_digest_uri,
            member_tree_digest_uri(sealed.member_digest)
        );
        assert_eq!(
            store
                .scan_prefix_at(
                    root(),
                    placement(),
                    owner(),
                    MetadataFamily::CommitMember,
                    &commit_member_prefix(root(), commit(1)),
                    store.current_read_version().unwrap(),
                    None,
                    0,
                )
                .unwrap()
                .len(),
            256
        );

        detach_head(&store, &mut counter, commit(1));
        let zero = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(1)),
            )
            .unwrap(),
        )
        .unwrap();
        let retire_id = operation(2);
        service
            .begin_retirement(BeginCommitRetirementRequest {
                context: write_context(&store, &mut counter),
                operation_id: retire_id,
                commit_id: commit(1),
                expected_consumer_epoch: zero.consumer_epoch,
            })
            .unwrap();
        loop {
            let outcome = service
                .release_retired_commit(BuildCommitStepRequest {
                    context: write_context(&store, &mut counter),
                    operation_id: retire_id,
                    limit: 43,
                })
                .unwrap();
            if outcome.operation.phase == CommitRetirePhase::Complete {
                break;
            }
        }
        let retired = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(1)),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(retired.state, CommitState::Retired);
        assert!(store
            .scan_prefix_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::CommitMember,
                &commit_member_prefix(root(), commit(1)),
                store.current_read_version().unwrap(),
                None,
                1,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn begin_retry_reuses_first_durable_commit_time_after_clock_advances() {
        let mut counter = 10_u128;
        let store = ready_store(&mut counter);
        let service = CommitService::new(&store);
        let mut first = begin_request(
            write_context(&store, &mut counter),
            operation(2),
            commit(2),
            revision(20_000),
            vec![],
        );
        first.committed_at_unix_seconds = 1_700_000_000;
        let begun = service.begin_build(first.clone()).unwrap();

        let mut retry = first;
        retry.context = write_context(&store, &mut counter);
        retry.committed_at_unix_seconds = 1_700_000_123;
        let replayed = service.begin_build(retry).unwrap();

        assert!(replayed.replayed);
        assert_eq!(
            replayed.operation.committed_at_unix_seconds,
            begun.operation.committed_at_unix_seconds
        );
        assert_eq!(replayed.operation.committed_at_unix_seconds, 1_700_000_000);
    }

    #[test]
    fn begin_build_binds_replay_to_source_incarnation_and_projection_digest() {
        let mut counter = 20_u128;
        let store = ready_store(&mut counter);
        let service = CommitService::new(&store);
        let replacement_incarnation = WorkspaceIncarnationId::from_bytes([0x99; FIXED_ID_BYTES]);

        let mut initial_mismatch = begin_request(
            write_context(&store, &mut counter),
            operation(3),
            commit(3),
            revision(30_000),
            vec![],
        );
        initial_mismatch.expected_source_workspace_incarnation_id = replacement_incarnation;
        assert!(matches!(
            service.begin_build(initial_mismatch),
            Err(CommitError::OperationInputMismatch)
        ));

        let first = begin_request(
            write_context(&store, &mut counter),
            operation(3),
            commit(3),
            revision(30_000),
            vec![],
        );
        let begun = service.begin_build(first.clone()).unwrap();
        assert!(!begun.replayed);
        assert_eq!(
            begun.operation.source_workspace_incarnation_id,
            incarnation()
        );

        let mut projection_mismatch = first.clone();
        projection_mismatch.context = write_context(&store, &mut counter);
        projection_mismatch.projection_input_digest = [0xee; SHA256_BYTES];
        assert!(matches!(
            service.begin_build(projection_mismatch),
            Err(CommitError::OperationInputMismatch)
        ));

        let mut replay_mismatch = first;
        replay_mismatch.context = write_context(&store, &mut counter);
        replay_mismatch.expected_source_workspace_incarnation_id = replacement_incarnation;
        assert!(matches!(
            service.begin_build(replay_mismatch),
            Err(CommitError::OperationInputMismatch)
        ));
    }

    fn detach_head(store: &AgentMetadataStore, counter: &mut u128, commit_id: CommitId) {
        let context = write_context(store, counter);
        let head_key = workbench_commit_head_key(root(), incarnation());
        let head_payload =
            read_current(store, MetadataFamily::WorkbenchCommitHead, &head_key).unwrap();
        let head = WorkbenchCommitHeadRecord::decode(&head_payload).unwrap();
        assert_eq!(head.commit_id, commit_id);
        let commit_key_bytes = commit_key(root(), commit_id);
        let commit_payload =
            read_current(store, MetadataFamily::Commit, &commit_key_bytes).unwrap();
        let commit_record = CommitRecord::decode(&commit_payload).unwrap();
        let next_version = next_commit_version(context).unwrap();
        let next = remove_consumer(&commit_record, next_version).unwrap();
        let consumer_key = workbench_head_commit_consumer_key(root(), commit_id, incarnation());
        let consumer = read_current(store, MetadataFamily::CommitConsumer, &consumer_key).unwrap();
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(),
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
                            expected: Some(consumer),
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

    #[test]
    fn partial_build_resumes_after_file_store_reopen() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("commit-reopen.holt");
        let mut counter = 1_000_u128;
        {
            let store = AgentMetadataStore::create_file(&database, shard()).unwrap();
            initialize(&store, &mut counter);
            seed_paths(&store, &mut counter, 97);
            let service = CommitService::new(&store);
            service
                .begin_build(begin_request(
                    write_context(&store, &mut counter),
                    operation(11),
                    commit(11),
                    revision(10_000),
                    vec![],
                ))
                .unwrap();
            let partial = service
                .build_members(BuildCommitStepRequest {
                    context: write_context(&store, &mut counter),
                    operation_id: operation(11),
                    limit: 17,
                })
                .unwrap();
            assert_eq!(partial.operation.member_count, 17);
            assert!(!partial.operation.members_complete);
        }

        let store = AgentMetadataStore::reopen_file(&database, shard()).unwrap();
        let complete = finish_build(&store, &mut counter, operation(11));
        assert_eq!(complete.operation.member_count, 97);
        assert_eq!(complete.operation.phase, BuildCommitPhase::Complete);
    }

    #[test]
    fn prepare_only_reopen_reuses_first_commit_time_after_later_proposal() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("commit-time-reopen.holt");
        let mut counter = 1_100_u128;
        let first_time = 1_700_000_000;
        {
            let store = AgentMetadataStore::create_file(&database, shard()).unwrap();
            initialize(&store, &mut counter);
            let service = CommitService::new(&store);
            let mut request = begin_request(
                write_context(&store, &mut counter),
                operation(12),
                commit(12),
                revision(10_001),
                vec![],
            );
            request.committed_at_unix_seconds = first_time;
            let begun = service.begin_build(request).unwrap();
            assert_eq!(begun.operation.committed_at_unix_seconds, first_time);
        }

        let store = AgentMetadataStore::reopen_file(&database, shard()).unwrap();
        let service = CommitService::new(&store);
        let mut retry = begin_request(
            write_context(&store, &mut counter),
            operation(12),
            commit(12),
            revision(10_001),
            vec![],
        );
        retry.committed_at_unix_seconds = first_time + 120;
        let replayed = service.begin_build(retry).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.operation.committed_at_unix_seconds, first_time);
    }

    #[test]
    fn begin_build_enforces_exact_head_generation_and_replays_response_loss() {
        let mut counter = 1_250_u128;
        let store = ready_store(&mut counter);
        raw_put(
            &store,
            &mut counter,
            vec![
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision(70_001)),
                    artifact(1).encode().unwrap(),
                ),
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision(70_002)),
                    artifact(2).encode().unwrap(),
                ),
            ],
        );
        let service = CommitService::new(&store);
        service
            .begin_build(begin_request(
                write_context(&store, &mut counter),
                operation(71),
                commit(71),
                revision(70_001),
                vec![],
            ))
            .unwrap();
        finish_build(&store, &mut counter, operation(71));

        let create_on_existing = begin_request(
            write_context(&store, &mut counter),
            operation(72),
            commit(72),
            revision(70_002),
            vec![],
        );
        assert_eq!(
            service.begin_build(create_on_existing).unwrap_err(),
            CommitError::HeadConflict
        );

        let mut stale_replace = begin_request(
            write_context(&store, &mut counter),
            operation(73),
            commit(73),
            revision(70_002),
            vec![],
        );
        stale_replace.replace = true;
        stale_replace.expected_head_generation = Some(Generation::new(2).unwrap());
        assert_eq!(
            service.begin_build(stale_replace).unwrap_err(),
            CommitError::HeadConflict
        );

        let context = write_context(&store, &mut counter);
        let mut exact_replace =
            begin_request(context, operation(74), commit(74), revision(70_002), vec![]);
        exact_replace.replace = true;
        exact_replace.expected_head_generation = Some(Generation::new(1).unwrap());
        let begun = service.begin_build(exact_replace.clone()).unwrap();
        let replayed = service.begin_build(exact_replace).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.operation, begun.operation);
    }

    #[test]
    fn completed_commit_replays_after_a_replacement_advances_the_live_head() {
        for original_replace in [false, true] {
            let mut counter = 1_400_u128;
            let store = ready_store(&mut counter);
            seed_paths(&store, &mut counter, 1);
            let service = CommitService::new(&store);

            let mut first = begin_request(
                write_context(&store, &mut counter),
                operation(81),
                commit(81),
                revision(10_000),
                vec![],
            );
            first.replace = original_replace;
            let original = first.clone();
            service.begin_build(first).unwrap();
            let completed_first = finish_build(&store, &mut counter, operation(81));
            let first_result = completed_first.operation.result.unwrap();
            assert_eq!(first_result.head_generation, Generation::new(1).unwrap());

            let mut replacement = begin_request(
                write_context(&store, &mut counter),
                operation(82),
                commit(82),
                revision(10_000),
                vec![commit(81)],
            );
            replacement.replace = true;
            replacement.expected_head_generation = Some(Generation::new(1).unwrap());
            service.begin_build(replacement).unwrap();
            let completed_replacement = finish_build(&store, &mut counter, operation(82));
            assert_eq!(
                completed_replacement
                    .operation
                    .result
                    .unwrap()
                    .head_generation,
                Generation::new(2).unwrap()
            );

            let head = WorkbenchCommitHeadRecord::decode(
                &read_current(
                    &store,
                    MetadataFamily::WorkbenchCommitHead,
                    &workbench_commit_head_key(root(), incarnation()),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(head.commit_id, commit(82));

            let mut replay = original;
            replay.context = write_context(&store, &mut counter);
            replay.committed_at_unix_seconds += 600;
            let replayed = service.begin_build(replay).unwrap();
            assert!(replayed.replayed);
            assert_eq!(replayed.operation.phase, BuildCommitPhase::Complete);
            assert_eq!(replayed.operation.result, Some(first_result));

            let unchanged_head = WorkbenchCommitHeadRecord::decode(
                &read_current(
                    &store,
                    MetadataFamily::WorkbenchCommitHead,
                    &workbench_commit_head_key(root(), incarnation()),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(unchanged_head.commit_id, commit(82));
            assert_eq!(unchanged_head.head_generation, Generation::new(2).unwrap());
        }
    }

    #[test]
    fn child_commit_parent_consumer_is_sealed_and_released_by_retirement() {
        let mut counter = 1_500_u128;
        let store = ready_store(&mut counter);
        raw_put(
            &store,
            &mut counter,
            vec![
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision(60_001)),
                    artifact(1).encode().unwrap(),
                ),
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision(60_002)),
                    artifact(2).encode().unwrap(),
                ),
            ],
        );
        let service = CommitService::new(&store);
        service
            .begin_build(begin_request(
                write_context(&store, &mut counter),
                operation(31),
                commit(31),
                revision(60_001),
                vec![],
            ))
            .unwrap();
        finish_build(&store, &mut counter, operation(31));

        let mut child_request = begin_request(
            write_context(&store, &mut counter),
            operation(32),
            commit(32),
            revision(60_002),
            vec![commit(31)],
        );
        child_request.replace = true;
        child_request.expected_head_generation = Some(Generation::new(1).unwrap());
        service.begin_build(child_request).unwrap();
        finish_build(&store, &mut counter, operation(32));
        let child_consumer = child_commit_consumer_key(root(), commit(31), commit(32));
        assert!(read_current(&store, MetadataFamily::CommitConsumer, &child_consumer).is_some());
        let parent = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(31)),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(parent.consumer_count, 1);

        detach_head(&store, &mut counter, commit(32));
        let child = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(32)),
            )
            .unwrap(),
        )
        .unwrap();
        service
            .begin_retirement(BeginCommitRetirementRequest {
                context: write_context(&store, &mut counter),
                operation_id: operation(33),
                commit_id: commit(32),
                expected_consumer_epoch: child.consumer_epoch,
            })
            .unwrap();
        loop {
            let outcome = service
                .release_retired_commit(BuildCommitStepRequest {
                    context: write_context(&store, &mut counter),
                    operation_id: operation(33),
                    limit: 7,
                })
                .unwrap();
            if outcome.operation.phase == CommitRetirePhase::Complete {
                break;
            }
        }
        assert!(read_current(&store, MetadataFamily::CommitConsumer, &child_consumer).is_none());
        let released_parent = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(31)),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(released_parent.consumer_count, 0);
        assert!(released_parent.last_zero_consumer_version.is_some());
    }

    #[test]
    fn head_publication_and_retirement_consumer_races_have_one_winner() {
        let mut counter = 2_000_u128;
        let store = ready_store(&mut counter);
        raw_put(
            &store,
            &mut counter,
            vec![
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision(50_001)),
                    artifact(1).encode().unwrap(),
                ),
                (
                    MetadataFamily::ArtifactRevision,
                    artifact_revision_key(root(), revision(50_002)),
                    artifact(2).encode().unwrap(),
                ),
            ],
        );
        let service = CommitService::new(&store);
        service
            .begin_build(begin_request(
                write_context(&store, &mut counter),
                operation(21),
                commit(21),
                revision(50_001),
                vec![],
            ))
            .unwrap();
        service
            .begin_build(begin_request(
                write_context(&store, &mut counter),
                operation(22),
                commit(22),
                revision(50_002),
                vec![],
            ))
            .unwrap();
        let winner = finish_build(&store, &mut counter, operation(21));
        assert_eq!(winner.operation.phase, BuildCommitPhase::Complete);
        let tag_name = TagName::new("checkpoint").unwrap();
        let tagged = service
            .set_tag(SetCommitTagRequest {
                context: write_context(&store, &mut counter),
                workbench_id: workbench(),
                tag: tag_name.clone(),
                commit_id: commit(21),
            })
            .unwrap();
        assert_eq!(tagged.tag.unwrap().tag_generation.get(), 1);
        let deleted = service
            .delete_tag(DeleteCommitTagRequest {
                context: write_context(&store, &mut counter),
                workbench_id: workbench(),
                tag: tag_name,
            })
            .unwrap();
        assert_eq!(deleted.tag, None);

        prepare_empty_build_for_sealing(&store, &mut counter, operation(22));
        assert_eq!(
            service
                .finalize_build(write_context(&store, &mut counter), operation(22))
                .unwrap_err(),
            CommitError::HeadConflict
        );
        service
            .abort_build(AbortBuildCommitRequest {
                context: write_context(&store, &mut counter),
                operation_id: operation(22),
                terminal_error: CommitOperationTerminalError {
                    kind: super::super::CommitOperationErrorKind::HeadConflict,
                    message: "lost the exact head race".to_owned(),
                },
            })
            .unwrap();
        loop {
            let outcome = service
                .cleanup_build(BuildCommitStepRequest {
                    context: write_context(&store, &mut counter),
                    operation_id: operation(22),
                    limit: 7,
                })
                .unwrap();
            if outcome.operation.phase == BuildCommitPhase::Cleaned {
                break;
            }
        }

        detach_head(&store, &mut counter, commit(21));
        let zero = CommitRecord::decode(
            &read_current(
                &store,
                MetadataFamily::Commit,
                &commit_key(root(), commit(21)),
            )
            .unwrap(),
        )
        .unwrap();
        service
            .begin_build(begin_request(
                write_context(&store, &mut counter),
                operation(24),
                commit(24),
                revision(50_002),
                vec![commit(21)],
            ))
            .unwrap();
        let child_members = service
            .build_members(BuildCommitStepRequest {
                context: write_context(&store, &mut counter),
                operation_id: operation(24),
                limit: 7,
            })
            .unwrap();
        assert!(child_members.operation.members_complete);
        let child_revisions = service
            .seal_revisions(BuildCommitStepRequest {
                context: write_context(&store, &mut counter),
                operation_id: operation(24),
                limit: 7,
            })
            .unwrap();
        assert!(child_revisions.operation.revisions_complete);

        let retire_context = write_context(&store, &mut counter);
        let competing_retire_context = write_context(&store, &mut counter);
        let tag_context = write_context(&store, &mut counter);
        let child_context = write_context(&store, &mut counter);
        service
            .begin_retirement(BeginCommitRetirementRequest {
                context: retire_context,
                operation_id: operation(23),
                commit_id: commit(21),
                expected_consumer_epoch: zero.consumer_epoch,
            })
            .unwrap();
        assert!(matches!(
            service.begin_retirement(BeginCommitRetirementRequest {
                context: competing_retire_context,
                operation_id: operation(25),
                commit_id: commit(21),
                expected_consumer_epoch: zero.consumer_epoch,
            }),
            Err(CommitError::Metadata(
                AgentMetadataError::WriteReadVersionMismatch { .. }
            ))
        ));
        assert!(matches!(
            service.set_tag(SetCommitTagRequest {
                context: tag_context,
                workbench_id: workbench(),
                tag: TagName::new("late-tag").unwrap(),
                commit_id: commit(21),
            }),
            Err(CommitError::Metadata(
                AgentMetadataError::WriteReadVersionMismatch { .. }
            ))
        ));
        assert!(matches!(
            service.attach_parents(BuildCommitStepRequest {
                context: child_context,
                operation_id: operation(24),
                limit: 7,
            }),
            Err(CommitError::Metadata(
                AgentMetadataError::WriteReadVersionMismatch { .. }
            ))
        ));

        service
            .abort_build(AbortBuildCommitRequest {
                context: write_context(&store, &mut counter),
                operation_id: operation(24),
                terminal_error: CommitOperationTerminalError {
                    kind: super::super::CommitOperationErrorKind::AbortedByCaller,
                    message: "retirement won the child-consumer race".to_owned(),
                },
            })
            .unwrap();
        loop {
            let outcome = service
                .cleanup_build(BuildCommitStepRequest {
                    context: write_context(&store, &mut counter),
                    operation_id: operation(24),
                    limit: 7,
                })
                .unwrap();
            if outcome.operation.phase == BuildCommitPhase::Cleaned {
                break;
            }
        }
    }

    fn prepare_empty_build_for_sealing(
        store: &AgentMetadataStore,
        counter: &mut u128,
        operation_id: OperationId,
    ) {
        let service = CommitService::new(store);
        let members = service
            .build_members(BuildCommitStepRequest {
                context: write_context(store, counter),
                operation_id,
                limit: 7,
            })
            .unwrap();
        assert!(members.operation.members_complete);
        let revisions = service
            .seal_revisions(BuildCommitStepRequest {
                context: write_context(store, counter),
                operation_id,
                limit: 7,
            })
            .unwrap();
        assert!(revisions.operation.revisions_complete);
        service
            .attach_parents(BuildCommitStepRequest {
                context: write_context(store, counter),
                operation_id,
                limit: 7,
            })
            .unwrap();
        service
            .begin_sealing(write_context(store, counter), operation_id)
            .unwrap();
    }
}
