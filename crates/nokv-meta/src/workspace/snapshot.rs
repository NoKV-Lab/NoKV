/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Fenced leased-snapshot lifecycle over workspace-native metadata records.

use std::collections::BTreeMap;
use std::fmt;

use nokv_types::{
    CommandDigest, CommitId, CommitState, CommitVersion, ConsumerEpoch, Generation,
    HistoryHoldState, OperationId, SnapshotAliasName, SnapshotId, SnapshotState, WorkbenchId,
    WorkspaceIncarnationId, WorkspaceState, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::codec::{
    commit_key, restore_history_hold_key, snapshot_alias_key, snapshot_commit_consumer_key,
    snapshot_history_hold_key, snapshot_id_claim_key, snapshot_ref_key, workbench_commit_head_key,
    workbench_head_commit_consumer_key, workspace_current_key, SCHEMA_ID,
};
use super::commit_records::{
    add_commit_consumer, remove_commit_consumer, CommitConsumerMutationError, CommitConsumerRecord,
    CommitRecord, CommitRecordError, WorkbenchCommitHeadRecord,
};
use super::engine::{
    CommandMutation, CommandPredicate, EventProjection, HistoryProjection, MetaError, MetaShard,
    MetadataCommand, RootFenceAction,
};
use super::event_projection::change_event_projection;
use super::keyspace::MetadataFamily;
use super::namespace::{RootReadContext, RootWriteContext};
use super::publication_records::{PublicationRecordCodecError, WorkspaceRecord};
use super::query_records::{
    ChangeEventKind, ChangeEventRecord, QueryFieldId, QueryRecordError, QueryScalar,
    TypedProjection,
};
use super::snapshot_records::{
    HistoryHoldRecord, SnapshotAliasRecord, SnapshotRecordError, SnapshotRefRecord,
};

const SNAPSHOT_RESULT_VERSION: u8 = 1;

/// Numeric or exact alias selection for one snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotSelector {
    Id(SnapshotId),
    Alias(SnapshotAliasName),
}

/// One resolved durable snapshot and the alias generation that selected it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSnapshot {
    pub snapshot_id: SnapshotId,
    pub record: SnapshotRefRecord,
    pub alias_record: Option<SnapshotAliasRecord>,
}

/// Durable result of one snapshot lifecycle command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotWriteOutcome {
    pub snapshot: ResolvedSnapshot,
    pub commit_version: CommitVersion,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MintSnapshotRequest {
    pub workbench_id: WorkbenchId,
    pub snapshot_id: SnapshotId,
    pub alias: Option<SnapshotAliasName>,
    pub lease_deadline_ms: u64,
    pub annotation: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewSnapshotRequest {
    pub workbench_id: WorkbenchId,
    pub selector: SnapshotSelector,
    pub lease_deadline_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetireSnapshotRequest {
    pub workbench_id: WorkbenchId,
    pub selector: SnapshotSelector,
    pub retire_annotation: Option<Vec<u8>>,
}

enum TerminalTransition {
    ExplicitRetire { annotation: Option<Vec<u8>> },
    ExpiryClaim,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimExpiredSnapshotRequest {
    pub workbench_id: WorkbenchId,
    pub snapshot_id: SnapshotId,
    pub observed_now_ms: u64,
    pub maximum_clock_skew_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinishSnapshotReapRequest {
    pub workbench_id: WorkbenchId,
    pub snapshot_id: SnapshotId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachSnapshotConsumerRequest {
    pub workbench_id: WorkbenchId,
    pub selector: SnapshotSelector,
    pub restore_operation_id: OperationId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseSnapshotConsumerRequest {
    pub workbench_id: WorkbenchId,
    pub snapshot_id: SnapshotId,
    pub restore_operation_id: OperationId,
}

/// Snapshot lifecycle failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    WorkspaceMissing {
        workbench_id: WorkbenchId,
    },
    WorkspaceNotCommitted {
        workbench_id: WorkbenchId,
    },
    SourceCommitMissing,
    SourceCommitUnavailable {
        state: CommitState,
    },
    SourceCommitBindingMismatch,
    SourceCommitConsumerMissing,
    SourceCommitConsumerMismatch,
    SnapshotAlreadyExists {
        snapshot_id: SnapshotId,
    },
    SnapshotIdAlreadyClaimed {
        snapshot_id: SnapshotId,
    },
    SnapshotMissing {
        snapshot_id: SnapshotId,
    },
    AliasMissing {
        alias: SnapshotAliasName,
    },
    AliasProjectionMismatch,
    SnapshotNotActive {
        snapshot_id: SnapshotId,
        state: SnapshotState,
    },
    SnapshotNotReapClaimed {
        snapshot_id: SnapshotId,
        state: SnapshotState,
    },
    LeaseDeadlineNotExtended {
        current: u64,
        requested: u64,
    },
    LeaseDeadlineNotFuture {
        clock: u64,
        requested: u64,
    },
    SnapshotNotExpired {
        deadline_ms: u64,
        lease_clock_ms: u64,
        maximum_clock_skew_ms: u64,
    },
    ForkRetentionActive {
        snapshot_id: SnapshotId,
        consumer_count: u64,
    },
    ConsumerMissing {
        restore_operation_id: OperationId,
    },
    ConsumerCountOverflow,
    ConsumerEpochOverflow,
    CommitConsumerCountOverflow,
    CommitConsumerCountUnderflow,
    CommitConsumerEpochOverflow,
    CommitVersionOverflow,
    AliasGenerationOverflow,
    RequestInputMismatch,
    DeterministicResultMismatch {
        reason: String,
    },
    ConcurrentMutation,
    WorkspaceCodec(PublicationRecordCodecError),
    CommitCodec(CommitRecordError),
    SnapshotCodec(SnapshotRecordError),
    QueryRecord(QueryRecordError),
    Meta(MetaError),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkspaceMissing { workbench_id } => {
                write!(formatter, "workbench {workbench_id} does not exist")
            }
            Self::WorkspaceNotCommitted { workbench_id } => {
                write!(formatter, "workbench {workbench_id} has no committed head")
            }
            Self::SourceCommitMissing => {
                formatter.write_str("snapshot source commit is missing")
            }
            Self::SourceCommitUnavailable { state } => {
                write!(formatter, "snapshot source commit is not sealed: {state:?}")
            }
            Self::SourceCommitBindingMismatch => formatter
                .write_str("snapshot source commit does not belong to the visible workbench"),
            Self::SourceCommitConsumerMissing => formatter
                .write_str("snapshot source commit is missing its exact consumer row"),
            Self::SourceCommitConsumerMismatch => formatter
                .write_str("snapshot source commit consumer epoch is inconsistent"),
            Self::SnapshotAlreadyExists { snapshot_id } => {
                write!(formatter, "snapshot {snapshot_id} already exists")
            }
            Self::SnapshotIdAlreadyClaimed { snapshot_id } => {
                write!(
                    formatter,
                    "snapshot id {snapshot_id} was already used in this root"
                )
            }
            Self::SnapshotMissing { snapshot_id } => {
                write!(formatter, "snapshot {snapshot_id} does not exist")
            }
            Self::AliasMissing { alias } => write!(formatter, "snapshot alias {alias} is missing"),
            Self::AliasProjectionMismatch => {
                formatter.write_str("snapshot alias projection does not match its selected record")
            }
            Self::SnapshotNotActive { snapshot_id, state } => {
                write!(formatter, "snapshot {snapshot_id} is {state:?}, not Active")
            }
            Self::SnapshotNotReapClaimed { snapshot_id, state } => {
                write!(
                    formatter,
                    "snapshot {snapshot_id} is {state:?}, not ReapClaimed"
                )
            }
            Self::LeaseDeadlineNotExtended { current, requested } => write!(
                formatter,
                "snapshot lease deadline {requested} does not extend current deadline {current}"
            ),
            Self::LeaseDeadlineNotFuture { clock, requested } => write!(
                formatter,
                "snapshot lease deadline {requested} is not newer than lease clock {clock}"
            ),
            Self::SnapshotNotExpired {
                deadline_ms,
                lease_clock_ms,
                maximum_clock_skew_ms,
            } => write!(
                formatter,
                "snapshot deadline {deadline_ms} plus clock-skew grace {maximum_clock_skew_ms} is newer than lease clock {lease_clock_ms}"
            ),
            Self::ForkRetentionActive {
                snapshot_id,
                consumer_count,
            } => write!(
                formatter,
                "snapshot {snapshot_id} has {consumer_count} active fork consumers"
            ),
            Self::ConsumerMissing {
                restore_operation_id,
            } => write!(
                formatter,
                "snapshot consumer for restore operation {} is missing",
                hex_id(restore_operation_id.as_bytes())
            ),
            Self::ConsumerCountOverflow => {
                formatter.write_str("snapshot consumer count overflow")
            }
            Self::ConsumerEpochOverflow => {
                formatter.write_str("snapshot consumer epoch overflow")
            }
            Self::CommitConsumerCountOverflow => {
                formatter.write_str("source commit consumer count overflow")
            }
            Self::CommitConsumerCountUnderflow => {
                formatter.write_str("source commit consumer count would underflow")
            }
            Self::CommitConsumerEpochOverflow => {
                formatter.write_str("source commit consumer epoch overflow")
            }
            Self::CommitVersionOverflow => {
                formatter.write_str("snapshot commit version overflow")
            }
            Self::AliasGenerationOverflow => {
                formatter.write_str("snapshot alias generation overflow")
            }
            Self::RequestInputMismatch => {
                formatter.write_str("request id was reused with different snapshot inputs")
            }
            Self::DeterministicResultMismatch { reason } => {
                write!(formatter, "invalid snapshot command result: {reason}")
            }
            Self::ConcurrentMutation => {
                formatter.write_str("snapshot state changed concurrently")
            }
            Self::WorkspaceCodec(source) => source.fmt(formatter),
            Self::CommitCodec(source) => source.fmt(formatter),
            Self::SnapshotCodec(source) => source.fmt(formatter),
            Self::QueryRecord(source) => source.fmt(formatter),
            Self::Meta(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WorkspaceCodec(source) => Some(source),
            Self::CommitCodec(source) => Some(source),
            Self::SnapshotCodec(source) => Some(source),
            Self::QueryRecord(source) => Some(source),
            Self::Meta(source) => Some(source),
            _ => None,
        }
    }
}

impl From<MetaError> for SnapshotError {
    fn from(source: MetaError) -> Self {
        Self::Meta(source)
    }
}

impl From<SnapshotRecordError> for SnapshotError {
    fn from(source: SnapshotRecordError) -> Self {
        Self::SnapshotCodec(source)
    }
}

impl From<CommitRecordError> for SnapshotError {
    fn from(source: CommitRecordError) -> Self {
        Self::CommitCodec(source)
    }
}

impl From<QueryRecordError> for SnapshotError {
    fn from(source: QueryRecordError) -> Self {
        Self::QueryRecord(source)
    }
}

/// Mint a new leased MVCC snapshot, its history hold, and one exact sealed
/// source-commit consumer atomically.
pub fn mint_snapshot(
    store: &MetaShard,
    context: RootWriteContext,
    request: &MintSnapshotRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = mint_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let clock = store.lease_clock_high_water()?;
    if request.lease_deadline_ms <= clock {
        return Err(SnapshotError::LeaseDeadlineNotFuture {
            clock,
            requested: request.lease_deadline_ms,
        });
    }
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let source = load_committed_snapshot_source(store, context, &workspace)?;
    let snapshot_key = snapshot_ref_key(
        context.root_id,
        workspace.record.incarnation_id,
        request.snapshot_id,
    );
    if read_current(store, context, MetadataFamily::SnapshotRef, &snapshot_key)?.is_some() {
        return Err(SnapshotError::SnapshotAlreadyExists {
            snapshot_id: request.snapshot_id,
        });
    }
    let claim_key = snapshot_id_claim_key(context.root_id, request.snapshot_id);
    if read_current(store, context, MetadataFamily::SnapshotRef, &claim_key)?.is_some() {
        return Err(SnapshotError::SnapshotIdAlreadyClaimed {
            snapshot_id: request.snapshot_id,
        });
    }
    let hold_key = snapshot_history_hold_key(context.root_id, request.snapshot_id);
    if read_current(store, context, MetadataFamily::HistoryHold, &hold_key)?.is_some() {
        return Err(SnapshotError::SnapshotIdAlreadyClaimed {
            snapshot_id: request.snapshot_id,
        });
    }

    let alias_plan = request
        .alias
        .as_ref()
        .map(|alias| {
            plan_alias_mint(
                store,
                context,
                workspace.record.incarnation_id,
                alias,
                request.snapshot_id,
            )
        })
        .transpose()?;
    let record = SnapshotRefRecord {
        read_version: context.read_version,
        source_commit_id: Some(source.commit_id),
        alias: request.alias.clone(),
        lease_deadline_ms: request.lease_deadline_ms,
        state: SnapshotState::Active,
        consumer_count: 0,
        consumer_epoch: ConsumerEpoch::ZERO,
        annotation: request.annotation.clone(),
        retire_annotation: None,
    };
    let record_payload = record.encode()?;
    let hold_payload = HistoryHoldRecord {
        read_version: context.read_version,
        source_snapshot_id: None,
        state: HistoryHoldState::Active,
    }
    .encode();

    let resolved = ResolvedSnapshot {
        snapshot_id: request.snapshot_id,
        record,
        alias_record: alias_plan.as_ref().map(|plan| plan.next.clone()),
    };
    let deterministic_result = encode_snapshot_result(input_digest, &resolved)?;
    let mut command = base_command(context, deterministic_result);
    predicate_workspace(&mut command, &workspace);
    predicate_exact(
        &mut command,
        MetadataFamily::WorkbenchCommitHead,
        source.head_key,
        source.head_payload,
    );
    predicate_exact(
        &mut command,
        MetadataFamily::CommitConsumer,
        source.head_consumer_key,
        source.head_consumer_payload,
    );
    replace_exact(
        &mut command,
        MetadataFamily::Commit,
        source.commit_key,
        source.commit_payload,
        source.retained_commit.encode()?,
    );
    put_absent(
        &mut command,
        MetadataFamily::CommitConsumer,
        snapshot_commit_consumer_key(context.root_id, source.commit_id, request.snapshot_id),
        CommitConsumerRecord {
            consumer_epoch_at_add: source.retained_commit.consumer_epoch,
        }
        .encode(),
    );
    put_absent(
        &mut command,
        MetadataFamily::SnapshotRef,
        snapshot_key,
        record_payload,
    );
    put_absent(
        &mut command,
        MetadataFamily::SnapshotRef,
        claim_key,
        workspace.record.incarnation_id.as_bytes().to_vec(),
    );
    put_absent(
        &mut command,
        MetadataFamily::HistoryHold,
        hold_key,
        hold_payload,
    );
    if let Some(alias_plan) = alias_plan {
        replace_optional(
            &mut command,
            MetadataFamily::SnapshotAlias,
            alias_plan.key,
            alias_plan.expected,
            alias_plan.next.encode(),
        );
    }
    command.event_projection.push(snapshot_change_event(
        &request.workbench_id,
        workspace.record.incarnation_id,
        ChangeEventKind::SnapshotMinted,
        None,
        &resolved,
        None,
    )?);
    execute_snapshot_command(
        store,
        command.seal(),
        input_digest,
        Some(request.lease_deadline_ms),
    )
}

/// Resolve one snapshot at an exact MVCC read version.
pub fn get_snapshot_at(
    store: &MetaShard,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    selector: &SnapshotSelector,
) -> Result<Option<ResolvedSnapshot>, SnapshotError> {
    let Some(workspace) = load_visible_workspace_at(store, context, workbench_id)? else {
        return Ok(None);
    };
    load_snapshot_at(store, context, workspace.incarnation_id, selector)
}

/// Extend an active snapshot lease. Expired but unclaimed snapshots may revive.
pub fn renew_snapshot(
    store: &MetaShard,
    context: RootWriteContext,
    request: &RenewSnapshotRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = renew_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let clock = store.lease_clock_high_water()?;
    if request.lease_deadline_ms <= clock {
        return Err(SnapshotError::LeaseDeadlineNotFuture {
            clock,
            requested: request.lease_deadline_ms,
        });
    }
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let loaded = load_snapshot_for_write(
        store,
        context,
        workspace.record.incarnation_id,
        &request.selector,
    )?;
    require_active(&loaded.resolved)?;
    if request.lease_deadline_ms <= loaded.resolved.record.lease_deadline_ms {
        return Err(SnapshotError::LeaseDeadlineNotExtended {
            current: loaded.resolved.record.lease_deadline_ms,
            requested: request.lease_deadline_ms,
        });
    }
    let mut next = loaded.resolved.clone();
    next.record.lease_deadline_ms = request.lease_deadline_ms;
    let deterministic_result = encode_snapshot_result(input_digest, &next)?;
    let mut command = base_command(context, deterministic_result);
    predicate_workspace(&mut command, &workspace);
    replace_exact(
        &mut command,
        MetadataFamily::SnapshotRef,
        loaded.snapshot_key,
        loaded.snapshot_payload,
        next.record.encode()?,
    );
    predicate_alias_without_mutation(&mut command, loaded.alias_plan.as_ref());
    command.event_projection.push(snapshot_change_event(
        &request.workbench_id,
        workspace.record.incarnation_id,
        ChangeEventKind::SnapshotRenewed,
        Some(&loaded.resolved),
        &next,
        None,
    )?);
    execute_snapshot_command(
        store,
        command.seal(),
        input_digest,
        Some(request.lease_deadline_ms),
    )
}

/// Explicitly retire an active snapshot and release its history hold.
pub fn retire_snapshot(
    store: &MetaShard,
    context: RootWriteContext,
    request: &RetireSnapshotRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = retire_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let loaded = load_snapshot_for_write(
        store,
        context,
        workspace.record.incarnation_id,
        &request.selector,
    )?;
    require_active(&loaded.resolved)?;
    require_no_consumers(&loaded.resolved)?;
    terminalize_snapshot(
        store,
        context,
        input_digest,
        workspace,
        loaded,
        TerminalTransition::ExplicitRetire {
            annotation: request.retire_annotation.clone(),
        },
    )
}

/// Claim one expired snapshot after persisted clock-skew grace and release its
/// history hold. The claim is the point after which renewal is forbidden.
pub fn claim_expired_snapshot(
    store: &MetaShard,
    context: RootWriteContext,
    request: &ClaimExpiredSnapshotRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = claim_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let lease_clock = store.observe_lease_clock(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        request.observed_now_ms,
    )?;
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let loaded = load_snapshot_for_write(
        store,
        context,
        workspace.record.incarnation_id,
        &SnapshotSelector::Id(request.snapshot_id),
    )?;
    require_active(&loaded.resolved)?;
    require_no_consumers(&loaded.resolved)?;
    let reaping_threshold = loaded
        .resolved
        .record
        .lease_deadline_ms
        .saturating_add(request.maximum_clock_skew_ms);
    if lease_clock < reaping_threshold {
        return Err(SnapshotError::SnapshotNotExpired {
            deadline_ms: loaded.resolved.record.lease_deadline_ms,
            lease_clock_ms: lease_clock,
            maximum_clock_skew_ms: request.maximum_clock_skew_ms,
        });
    }
    terminalize_snapshot(
        store,
        context,
        input_digest,
        workspace,
        loaded,
        TerminalTransition::ExpiryClaim,
    )
}

/// Mark a claimed snapshot as fully reaped after history pruning observes its
/// released hold.
pub fn finish_snapshot_reap(
    store: &MetaShard,
    context: RootWriteContext,
    request: &FinishSnapshotReapRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = finish_reap_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let loaded = load_snapshot_for_write(
        store,
        context,
        workspace.record.incarnation_id,
        &SnapshotSelector::Id(request.snapshot_id),
    )?;
    if loaded.resolved.record.state != SnapshotState::ReapClaimed {
        return Err(SnapshotError::SnapshotNotReapClaimed {
            snapshot_id: request.snapshot_id,
            state: loaded.resolved.record.state,
        });
    }
    let mut next = loaded.resolved.clone();
    next.record.state = SnapshotState::Reaped;
    if let Some(alias) = &mut next.alias_record {
        alias.snapshot_state = SnapshotState::Reaped;
    }
    let deterministic_result = encode_snapshot_result(input_digest, &next)?;
    let mut command = base_command(context, deterministic_result);
    predicate_workspace(&mut command, &workspace);
    predicate_absent(
        &mut command,
        MetadataFamily::HistoryHold,
        snapshot_history_hold_key(context.root_id, request.snapshot_id),
    );
    replace_exact(
        &mut command,
        MetadataFamily::SnapshotRef,
        loaded.snapshot_key,
        loaded.snapshot_payload,
        next.record.encode()?,
    );
    apply_alias_replacement(&mut command, loaded.alias_plan, next.alias_record.as_ref());
    command.event_projection.push(snapshot_change_event(
        &request.workbench_id,
        workspace.record.incarnation_id,
        ChangeEventKind::SnapshotReaped,
        Some(&loaded.resolved),
        &next,
        None,
    )?);
    execute_snapshot_command(store, command.seal(), input_digest, None)
}

/// Attach one restore consumer to a live snapshot and create its exact source
/// history hold in the same command.
pub fn attach_snapshot_consumer(
    store: &MetaShard,
    context: RootWriteContext,
    request: &AttachSnapshotConsumerRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = attach_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let clock = store.lease_clock_high_water()?;
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let loaded = load_snapshot_for_write(
        store,
        context,
        workspace.record.incarnation_id,
        &request.selector,
    )?;
    require_active(&loaded.resolved)?;
    if loaded.resolved.record.lease_deadline_ms <= clock {
        return Err(SnapshotError::LeaseDeadlineNotFuture {
            clock,
            requested: loaded.resolved.record.lease_deadline_ms,
        });
    }
    let hold_key = restore_history_hold_key(context.root_id, request.restore_operation_id);
    if read_current(store, context, MetadataFamily::HistoryHold, &hold_key)?.is_some() {
        return Err(SnapshotError::ConcurrentMutation);
    }
    let mut next = loaded.resolved.clone();
    next.record.consumer_count = next
        .record
        .consumer_count
        .checked_add(1)
        .ok_or(SnapshotError::ConsumerCountOverflow)?;
    next.record.consumer_epoch = increment_consumer_epoch(next.record.consumer_epoch)?;
    let hold = HistoryHoldRecord {
        read_version: next.record.read_version,
        source_snapshot_id: Some(next.snapshot_id),
        state: HistoryHoldState::Active,
    };
    let deterministic_result = encode_snapshot_result(input_digest, &next)?;
    let mut command = base_command(context, deterministic_result);
    predicate_workspace(&mut command, &workspace);
    replace_exact(
        &mut command,
        MetadataFamily::SnapshotRef,
        loaded.snapshot_key,
        loaded.snapshot_payload,
        next.record.encode()?,
    );
    predicate_alias_without_mutation(&mut command, loaded.alias_plan.as_ref());
    put_absent(
        &mut command,
        MetadataFamily::HistoryHold,
        hold_key,
        hold.encode(),
    );
    command.event_projection.push(snapshot_change_event(
        &request.workbench_id,
        workspace.record.incarnation_id,
        ChangeEventKind::SnapshotConsumerAttached,
        Some(&loaded.resolved),
        &next,
        Some(request.restore_operation_id),
    )?);
    execute_snapshot_command(
        store,
        command.seal(),
        input_digest,
        Some(next.record.lease_deadline_ms),
    )
}

/// Release one restore consumer and its exact source history hold atomically.
pub fn release_snapshot_consumer(
    store: &MetaShard,
    context: RootWriteContext,
    request: &ReleaseSnapshotConsumerRequest,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let input_digest = release_input_digest(context.root_id, request);
    if let Some(outcome) = replay_outcome(store, context, input_digest)? {
        return Ok(outcome);
    }
    let workspace = load_visible_workspace(store, context, &request.workbench_id)?;
    let loaded = load_snapshot_for_write(
        store,
        context,
        workspace.record.incarnation_id,
        &SnapshotSelector::Id(request.snapshot_id),
    )?;
    require_active(&loaded.resolved)?;
    let hold_key = restore_history_hold_key(context.root_id, request.restore_operation_id);
    let hold_payload = read_current(store, context, MetadataFamily::HistoryHold, &hold_key)?
        .ok_or(SnapshotError::ConsumerMissing {
            restore_operation_id: request.restore_operation_id,
        })?;
    let hold = HistoryHoldRecord::decode(&hold_payload)?;
    if hold.read_version != loaded.resolved.record.read_version
        || hold.source_snapshot_id != Some(request.snapshot_id)
        || hold.state != HistoryHoldState::Active
    {
        return Err(SnapshotError::DeterministicResultMismatch {
            reason: "restore history hold does not match its source snapshot".to_owned(),
        });
    }
    let mut next = loaded.resolved.clone();
    next.record.consumer_count = next.record.consumer_count.checked_sub(1).ok_or(
        SnapshotError::DeterministicResultMismatch {
            reason: "history hold exists while snapshot consumer count is zero".to_owned(),
        },
    )?;
    next.record.consumer_epoch = increment_consumer_epoch(next.record.consumer_epoch)?;
    let deterministic_result = encode_snapshot_result(input_digest, &next)?;
    let mut command = base_command(context, deterministic_result);
    predicate_workspace(&mut command, &workspace);
    replace_exact(
        &mut command,
        MetadataFamily::SnapshotRef,
        loaded.snapshot_key,
        loaded.snapshot_payload,
        next.record.encode()?,
    );
    predicate_alias_without_mutation(&mut command, loaded.alias_plan.as_ref());
    delete_exact(
        &mut command,
        MetadataFamily::HistoryHold,
        hold_key,
        hold_payload,
    );
    command.event_projection.push(snapshot_change_event(
        &request.workbench_id,
        workspace.record.incarnation_id,
        ChangeEventKind::SnapshotConsumerReleased,
        Some(&loaded.resolved),
        &next,
        Some(request.restore_operation_id),
    )?);
    execute_snapshot_command(store, command.seal(), input_digest, None)
}

fn terminalize_snapshot(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
    workspace: LoadedWorkspace,
    loaded: LoadedSnapshot,
    transition: TerminalTransition,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let (next_state, event_kind, retire_annotation) = match transition {
        TerminalTransition::ExplicitRetire { annotation } => (
            SnapshotState::Retired,
            ChangeEventKind::SnapshotRetired,
            annotation,
        ),
        TerminalTransition::ExpiryClaim => (
            SnapshotState::ReapClaimed,
            ChangeEventKind::SnapshotReapClaimed,
            None,
        ),
    };
    let hold_key = snapshot_history_hold_key(context.root_id, loaded.resolved.snapshot_id);
    let hold_payload = read_current(store, context, MetadataFamily::HistoryHold, &hold_key)?
        .ok_or_else(|| SnapshotError::DeterministicResultMismatch {
            reason: "active snapshot is missing its history hold".to_owned(),
        })?;
    let hold = HistoryHoldRecord::decode(&hold_payload)?;
    if hold.read_version != loaded.resolved.record.read_version
        || hold.source_snapshot_id.is_some()
        || hold.state != HistoryHoldState::Active
    {
        return Err(SnapshotError::DeterministicResultMismatch {
            reason: "snapshot history hold does not match the active snapshot".to_owned(),
        });
    }
    let mut next = loaded.resolved.clone();
    next.record.state = next_state;
    next.record.retire_annotation = retire_annotation;
    if let Some(alias) = &mut next.alias_record {
        alias.snapshot_state = next_state;
    }
    let deterministic_result = encode_snapshot_result(input_digest, &next)?;
    let mut command = base_command(context, deterministic_result);
    predicate_workspace(&mut command, &workspace);
    plan_snapshot_commit_release(
        store,
        context,
        workspace.record.incarnation_id,
        &loaded.resolved,
        &mut command,
    )?;
    replace_exact(
        &mut command,
        MetadataFamily::SnapshotRef,
        loaded.snapshot_key,
        loaded.snapshot_payload,
        next.record.encode()?,
    );
    delete_exact(
        &mut command,
        MetadataFamily::HistoryHold,
        hold_key,
        hold_payload,
    );
    apply_alias_replacement(&mut command, loaded.alias_plan, next.alias_record.as_ref());
    command.event_projection.push(snapshot_change_event(
        &workspace.workbench_id,
        workspace.record.incarnation_id,
        event_kind,
        Some(&loaded.resolved),
        &next,
        None,
    )?);
    execute_snapshot_command(store, command.seal(), input_digest, None)
}

fn require_active(snapshot: &ResolvedSnapshot) -> Result<(), SnapshotError> {
    if snapshot.record.state == SnapshotState::Active {
        Ok(())
    } else {
        Err(SnapshotError::SnapshotNotActive {
            snapshot_id: snapshot.snapshot_id,
            state: snapshot.record.state,
        })
    }
}

fn require_no_consumers(snapshot: &ResolvedSnapshot) -> Result<(), SnapshotError> {
    if snapshot.record.consumer_count == 0 {
        Ok(())
    } else {
        Err(SnapshotError::ForkRetentionActive {
            snapshot_id: snapshot.snapshot_id,
            consumer_count: snapshot.record.consumer_count,
        })
    }
}

fn increment_consumer_epoch(epoch: ConsumerEpoch) -> Result<ConsumerEpoch, SnapshotError> {
    epoch
        .get()
        .checked_add(1)
        .map(ConsumerEpoch::new)
        .ok_or(SnapshotError::ConsumerEpochOverflow)
}

#[derive(Clone)]
struct LoadedWorkspace {
    workbench_id: WorkbenchId,
    key: Vec<u8>,
    payload: Vec<u8>,
    record: WorkspaceRecord,
}

struct LoadedSnapshotSourceCommit {
    commit_id: CommitId,
    head_key: Vec<u8>,
    head_payload: Vec<u8>,
    head_consumer_key: Vec<u8>,
    head_consumer_payload: Vec<u8>,
    commit_key: Vec<u8>,
    commit_payload: Vec<u8>,
    retained_commit: CommitRecord,
}

fn load_committed_snapshot_source(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: &LoadedWorkspace,
) -> Result<LoadedSnapshotSourceCommit, SnapshotError> {
    let head_key = workbench_commit_head_key(context.root_id, workspace.record.incarnation_id);
    let head_payload = read_current(
        store,
        context,
        MetadataFamily::WorkbenchCommitHead,
        &head_key,
    )?
    .ok_or_else(|| SnapshotError::WorkspaceNotCommitted {
        workbench_id: workspace.workbench_id.clone(),
    })?;
    let head = WorkbenchCommitHeadRecord::decode(&head_payload)?;
    let commit_key = commit_key(context.root_id, head.commit_id);
    let commit_payload = read_current(store, context, MetadataFamily::Commit, &commit_key)?
        .ok_or(SnapshotError::SourceCommitMissing)?;
    let commit = CommitRecord::decode(&commit_payload)?;
    require_snapshot_source_commit(&commit, workspace.record.incarnation_id)?;

    let head_consumer_key = workbench_head_commit_consumer_key(
        context.root_id,
        head.commit_id,
        workspace.record.incarnation_id,
    );
    let head_consumer_payload = read_current(
        store,
        context,
        MetadataFamily::CommitConsumer,
        &head_consumer_key,
    )?
    .ok_or(SnapshotError::SourceCommitConsumerMissing)?;
    let head_consumer = CommitConsumerRecord::decode(&head_consumer_payload)?;
    validate_commit_consumer(&commit, head_consumer)?;
    let retained_commit = add_commit_consumer(&commit).map_err(map_commit_consumer_mutation)?;
    Ok(LoadedSnapshotSourceCommit {
        commit_id: head.commit_id,
        head_key,
        head_payload,
        head_consumer_key,
        head_consumer_payload,
        commit_key,
        commit_payload,
        retained_commit,
    })
}

fn plan_snapshot_commit_release(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    snapshot: &ResolvedSnapshot,
    command: &mut MetadataCommand,
) -> Result<(), SnapshotError> {
    let Some(commit_id) = snapshot.record.source_commit_id else {
        return Ok(());
    };
    let commit_key = commit_key(context.root_id, commit_id);
    let commit_payload = read_current(store, context, MetadataFamily::Commit, &commit_key)?
        .ok_or(SnapshotError::SourceCommitMissing)?;
    let commit = CommitRecord::decode(&commit_payload)?;
    require_snapshot_source_commit(&commit, workspace)?;

    let consumer_key =
        snapshot_commit_consumer_key(context.root_id, commit_id, snapshot.snapshot_id);
    let consumer_payload = read_current(
        store,
        context,
        MetadataFamily::CommitConsumer,
        &consumer_key,
    )?
    .ok_or(SnapshotError::SourceCommitConsumerMissing)?;
    let consumer = CommitConsumerRecord::decode(&consumer_payload)?;
    validate_commit_consumer(&commit, consumer)?;
    let zero_version = CommitVersion::new(
        context
            .read_version
            .get()
            .checked_add(1)
            .ok_or(SnapshotError::CommitVersionOverflow)?,
    )
    .map_err(|_| SnapshotError::CommitVersionOverflow)?;
    let released =
        remove_commit_consumer(&commit, zero_version).map_err(map_commit_consumer_mutation)?;
    replace_exact(
        command,
        MetadataFamily::Commit,
        commit_key,
        commit_payload,
        released.encode()?,
    );
    delete_exact(
        command,
        MetadataFamily::CommitConsumer,
        consumer_key,
        consumer_payload,
    );
    Ok(())
}

fn require_snapshot_source_commit(
    commit: &CommitRecord,
    workspace: WorkspaceIncarnationId,
) -> Result<(), SnapshotError> {
    if commit.state != CommitState::Sealed {
        return Err(SnapshotError::SourceCommitUnavailable {
            state: commit.state,
        });
    }
    if commit.source_workspace_incarnation_id != workspace {
        return Err(SnapshotError::SourceCommitBindingMismatch);
    }
    Ok(())
}

fn validate_commit_consumer(
    commit: &CommitRecord,
    consumer: CommitConsumerRecord,
) -> Result<(), SnapshotError> {
    if consumer.consumer_epoch_at_add == ConsumerEpoch::ZERO
        || consumer.consumer_epoch_at_add > commit.consumer_epoch
        || commit.consumer_count == 0
    {
        return Err(SnapshotError::SourceCommitConsumerMismatch);
    }
    Ok(())
}

fn map_commit_consumer_mutation(error: CommitConsumerMutationError) -> SnapshotError {
    match error {
        CommitConsumerMutationError::CountOverflow => SnapshotError::CommitConsumerCountOverflow,
        CommitConsumerMutationError::CountUnderflow => SnapshotError::CommitConsumerCountUnderflow,
        CommitConsumerMutationError::EpochOverflow => SnapshotError::CommitConsumerEpochOverflow,
    }
}

#[derive(Clone)]
struct AliasPlan {
    key: Vec<u8>,
    expected: Vec<u8>,
}

struct LoadedSnapshot {
    snapshot_key: Vec<u8>,
    snapshot_payload: Vec<u8>,
    resolved: ResolvedSnapshot,
    alias_plan: Option<AliasPlan>,
}

fn load_visible_workspace(
    store: &MetaShard,
    context: RootWriteContext,
    workbench_id: &WorkbenchId,
) -> Result<LoadedWorkspace, SnapshotError> {
    let key = workspace_current_key(context.root_id, workbench_id);
    let payload = read_current(store, context, MetadataFamily::WorkspaceCurrent, &key)?
        .ok_or_else(|| SnapshotError::WorkspaceMissing {
            workbench_id: workbench_id.clone(),
        })?;
    let record = WorkspaceRecord::decode(&payload).map_err(SnapshotError::WorkspaceCodec)?;
    if record.state != WorkspaceState::Visible {
        return Err(SnapshotError::WorkspaceMissing {
            workbench_id: workbench_id.clone(),
        });
    }
    Ok(LoadedWorkspace {
        workbench_id: workbench_id.clone(),
        key,
        payload,
        record,
    })
}

fn load_visible_workspace_at(
    store: &MetaShard,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
) -> Result<Option<WorkspaceRecord>, SnapshotError> {
    let key = workspace_current_key(context.root_id, workbench_id);
    let Some(payload) = store.read_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::WorkspaceCurrent,
        &key,
        context.read_version,
    )?
    else {
        return Ok(None);
    };
    let record = WorkspaceRecord::decode(&payload).map_err(SnapshotError::WorkspaceCodec)?;
    Ok((record.state == WorkspaceState::Visible).then_some(record))
}

fn load_snapshot_for_write(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    selector: &SnapshotSelector,
) -> Result<LoadedSnapshot, SnapshotError> {
    let read_context = RootReadContext {
        root_id: context.root_id,
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        read_version: context.read_version,
    };
    let resolved = load_snapshot_at(store, read_context, workspace, selector)?.ok_or_else(
        || match selector {
            SnapshotSelector::Id(snapshot_id) => SnapshotError::SnapshotMissing {
                snapshot_id: *snapshot_id,
            },
            SnapshotSelector::Alias(alias) => SnapshotError::AliasMissing {
                alias: alias.clone(),
            },
        },
    )?;
    let snapshot_key = snapshot_ref_key(context.root_id, workspace, resolved.snapshot_id);
    let snapshot_payload =
        read_current(store, context, MetadataFamily::SnapshotRef, &snapshot_key)?.ok_or(
            SnapshotError::SnapshotMissing {
                snapshot_id: resolved.snapshot_id,
            },
        )?;
    let alias_plan = resolved
        .record
        .alias
        .as_ref()
        .zip(resolved.alias_record.as_ref())
        .map(|(alias, alias_record)| AliasPlan {
            key: snapshot_alias_key(context.root_id, workspace, alias),
            expected: alias_record.encode(),
        });
    Ok(LoadedSnapshot {
        snapshot_key,
        snapshot_payload,
        resolved,
        alias_plan,
    })
}

fn load_snapshot_at(
    store: &MetaShard,
    context: RootReadContext,
    workspace: WorkspaceIncarnationId,
    selector: &SnapshotSelector,
) -> Result<Option<ResolvedSnapshot>, SnapshotError> {
    let (snapshot_id, selected_alias) = match selector {
        SnapshotSelector::Id(snapshot_id) => (*snapshot_id, None),
        SnapshotSelector::Alias(alias) => {
            let alias_key = snapshot_alias_key(context.root_id, workspace, alias);
            let Some(alias_payload) = store.read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::SnapshotAlias,
                &alias_key,
                context.read_version,
            )?
            else {
                return Ok(None);
            };
            let alias_record = SnapshotAliasRecord::decode(&alias_payload)?;
            (alias_record.snapshot_id, Some(alias_record))
        }
    };
    let snapshot_key = snapshot_ref_key(context.root_id, workspace, snapshot_id);
    let Some(snapshot_payload) = store.read_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::SnapshotRef,
        &snapshot_key,
        context.read_version,
    )?
    else {
        return if selected_alias.is_some() {
            Err(SnapshotError::AliasProjectionMismatch)
        } else {
            Ok(None)
        };
    };
    let record = SnapshotRefRecord::decode(&snapshot_payload)?;
    let alias_record = match selected_alias {
        Some(alias_record) => Some(alias_record),
        None => match &record.alias {
            None => None,
            Some(alias) => {
                let alias_key = snapshot_alias_key(context.root_id, workspace, alias);
                store
                    .read_at(
                        context.root_id,
                        context.placement_generation,
                        context.owner_epoch,
                        MetadataFamily::SnapshotAlias,
                        &alias_key,
                        context.read_version,
                    )?
                    .map(|payload| SnapshotAliasRecord::decode(&payload))
                    .transpose()?
                    .filter(|alias_record| alias_record.snapshot_id == snapshot_id)
            }
        },
    };
    if let Some(alias_record) = &alias_record {
        if alias_record.snapshot_id != snapshot_id || alias_record.snapshot_state != record.state {
            return Err(SnapshotError::AliasProjectionMismatch);
        }
    }
    if let SnapshotSelector::Alias(alias) = selector {
        if record.alias.as_ref() != Some(alias) {
            return Err(SnapshotError::AliasProjectionMismatch);
        }
    }
    Ok(Some(ResolvedSnapshot {
        snapshot_id,
        record,
        alias_record,
    }))
}

fn plan_alias_mint(
    store: &MetaShard,
    context: RootWriteContext,
    workspace: WorkspaceIncarnationId,
    alias: &SnapshotAliasName,
    snapshot_id: SnapshotId,
) -> Result<AliasMintPlan, SnapshotError> {
    let key = snapshot_alias_key(context.root_id, workspace, alias);
    let expected = read_current(store, context, MetadataFamily::SnapshotAlias, &key)?;
    let generation = match &expected {
        None => Generation::new(1).expect("one is a valid alias generation"),
        Some(payload) => {
            let current = SnapshotAliasRecord::decode(payload)?;
            Generation::new(
                current
                    .alias_generation
                    .get()
                    .checked_add(1)
                    .ok_or(SnapshotError::AliasGenerationOverflow)?,
            )
            .map_err(|_| SnapshotError::AliasGenerationOverflow)?
        }
    };
    Ok(AliasMintPlan {
        key,
        expected,
        next: SnapshotAliasRecord {
            snapshot_id,
            alias_generation: generation,
            snapshot_state: SnapshotState::Active,
        },
    })
}

struct AliasMintPlan {
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
    next: SnapshotAliasRecord,
}

fn read_current(
    store: &MetaShard,
    context: RootWriteContext,
    family: MetadataFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, SnapshotError> {
    store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            family,
            key,
            context.read_version,
        )
        .map_err(SnapshotError::Meta)
}

fn predicate_workspace(command: &mut MetadataCommand, workspace: &LoadedWorkspace) {
    command.predicates.push(CommandPredicate::Value {
        family: MetadataFamily::WorkspaceCurrent,
        key: workspace.key.clone(),
        expected: Some(workspace.payload.clone()),
    });
}

fn predicate_exact(
    command: &mut MetadataCommand,
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Vec<u8>,
) {
    command.predicates.push(CommandPredicate::Value {
        family,
        key,
        expected: Some(expected),
    });
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

fn predicate_absent(command: &mut MetadataCommand, family: MetadataFamily, key: Vec<u8>) {
    command.predicates.push(CommandPredicate::Value {
        family,
        key,
        expected: None,
    });
}

fn replace_optional(
    command: &mut MetadataCommand,
    family: MetadataFamily,
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
    value: Vec<u8>,
) {
    command.predicates.push(CommandPredicate::Value {
        family,
        key: key.clone(),
        expected: expected.clone(),
    });
    command.mutations.push(CommandMutation::Put {
        family,
        key: key.clone(),
        value,
    });
    if expected.is_some() {
        command
            .history_projection
            .push(HistoryProjection { family, key });
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

fn predicate_alias_without_mutation(command: &mut MetadataCommand, plan: Option<&AliasPlan>) {
    if let Some(plan) = plan {
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::SnapshotAlias,
            key: plan.key.clone(),
            expected: Some(plan.expected.clone()),
        });
    }
}

fn apply_alias_replacement(
    command: &mut MetadataCommand,
    plan: Option<AliasPlan>,
    next: Option<&SnapshotAliasRecord>,
) {
    if let (Some(plan), Some(next)) = (plan, next) {
        replace_exact(
            command,
            MetadataFamily::SnapshotAlias,
            plan.key,
            plan.expected,
            next.encode(),
        );
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

fn execute_snapshot_command(
    store: &MetaShard,
    command: MetadataCommand,
    input_digest: [u8; SHA256_BYTES],
    lease_deadline_ms: Option<u64>,
) -> Result<SnapshotWriteOutcome, SnapshotError> {
    let executed = match lease_deadline_ms {
        Some(deadline) => store.execute_before_lease_deadline(&command, deadline),
        None => store.execute(&command),
    };
    let result = match executed {
        Ok(result) => result,
        Err(MetaError::LeaseDeadlineReached {
            lease_clock_ms,
            requested_deadline_ms,
        }) => {
            return Err(SnapshotError::LeaseDeadlineNotFuture {
                clock: lease_clock_ms,
                requested: requested_deadline_ms,
            });
        }
        Err(
            MetaError::PredicateFailed
            | MetaError::WriteConflict
            | MetaError::WriteReadVersionMismatch { .. },
        ) => {
            return Err(SnapshotError::ConcurrentMutation);
        }
        Err(source) => return Err(SnapshotError::Meta(source)),
    };
    let snapshot = decode_snapshot_result(&result.deterministic_result, input_digest)?;
    Ok(SnapshotWriteOutcome {
        snapshot,
        commit_version: result.commit_version,
        replayed: result.replayed,
    })
}

fn replay_outcome(
    store: &MetaShard,
    context: RootWriteContext,
    input_digest: [u8; SHA256_BYTES],
) -> Result<Option<SnapshotWriteOutcome>, SnapshotError> {
    let Some(replay) = store.lookup_request_result(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        context.request_id,
    )?
    else {
        return Ok(None);
    };
    let snapshot = decode_snapshot_result(&replay.deterministic_result, input_digest)?;
    Ok(Some(SnapshotWriteOutcome {
        snapshot,
        commit_version: replay.commit_version,
        replayed: true,
    }))
}

fn encode_snapshot_result(
    input_digest: [u8; SHA256_BYTES],
    snapshot: &ResolvedSnapshot,
) -> Result<Vec<u8>, SnapshotError> {
    let record = snapshot.record.encode()?;
    let mut encoded = Vec::new();
    encoded.push(SNAPSHOT_RESULT_VERSION);
    encoded.extend_from_slice(&input_digest);
    encoded.extend_from_slice(&snapshot.snapshot_id.get().to_be_bytes());
    encoded.extend_from_slice(
        &u32::try_from(record.len())
            .expect("snapshot record size fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&record);
    match &snapshot.alias_record {
        None => encoded.push(0),
        Some(alias) => {
            encoded.push(1);
            encoded.extend_from_slice(&alias.encode());
        }
    }
    Ok(encoded)
}

fn decode_snapshot_result(
    encoded: &[u8],
    expected_input_digest: [u8; SHA256_BYTES],
) -> Result<ResolvedSnapshot, SnapshotError> {
    let mut decoder = ResultDecoder::new(encoded);
    let version = decoder.u8("result_version")?;
    if version != SNAPSHOT_RESULT_VERSION {
        return Err(SnapshotError::DeterministicResultMismatch {
            reason: format!(
                "unsupported result version {version}, expected {SNAPSHOT_RESULT_VERSION}"
            ),
        });
    }
    let input_digest = decoder.fixed("input_digest")?;
    if input_digest != expected_input_digest {
        return Err(SnapshotError::RequestInputMismatch);
    }
    let snapshot_id = SnapshotId::new(decoder.u64("snapshot_id")?);
    let record_length = decoder.u32("snapshot_record_length")? as usize;
    let record = SnapshotRefRecord::decode(decoder.take("snapshot_record", record_length)?)?;
    let alias_record = match decoder.u8("alias_record")? {
        0 => None,
        1 => {
            let length = 1 + 8 + 8 + 1;
            Some(SnapshotAliasRecord::decode(
                decoder.take("alias_record", length)?,
            )?)
        }
        value => {
            return Err(SnapshotError::DeterministicResultMismatch {
                reason: format!("invalid alias result tag {value}"),
            });
        }
    };
    decoder.finish()?;
    if let Some(alias) = &alias_record {
        if record.alias.is_none()
            || alias.snapshot_id != snapshot_id
            || alias.snapshot_state != record.state
        {
            return Err(SnapshotError::DeterministicResultMismatch {
                reason: "snapshot result alias does not match its snapshot record".to_owned(),
            });
        }
    }
    Ok(ResolvedSnapshot {
        snapshot_id,
        record,
        alias_record,
    })
}

struct ResultDecoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ResultDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], SnapshotError> {
        let bytes = self.take(field, N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, SnapshotError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, SnapshotError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, SnapshotError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], SnapshotError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(SnapshotError::DeterministicResultMismatch {
                reason: format!("truncated {field}: need {length} bytes, have {remaining}"),
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn finish(self) -> Result<(), SnapshotError> {
        let trailing = self.input.len().saturating_sub(self.offset);
        if trailing == 0 {
            Ok(())
        } else {
            Err(SnapshotError::DeterministicResultMismatch {
                reason: format!("snapshot result has {trailing} trailing bytes"),
            })
        }
    }
}

fn mint_input_digest(
    root_id: nokv_types::RootId,
    request: &MintSnapshotRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(1, root_id, &request.workbench_id);
    hasher.update(request.snapshot_id.get().to_be_bytes());
    hash_optional_alias(&mut hasher, request.alias.as_ref());
    hasher.update(request.lease_deadline_ms.to_be_bytes());
    hash_bytes(&mut hasher, &request.annotation);
    hasher.finalize().into()
}

fn renew_input_digest(
    root_id: nokv_types::RootId,
    request: &RenewSnapshotRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(2, root_id, &request.workbench_id);
    hash_selector(&mut hasher, &request.selector);
    hasher.update(request.lease_deadline_ms.to_be_bytes());
    hasher.finalize().into()
}

fn retire_input_digest(
    root_id: nokv_types::RootId,
    request: &RetireSnapshotRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(3, root_id, &request.workbench_id);
    hash_selector(&mut hasher, &request.selector);
    hash_optional_bytes(&mut hasher, request.retire_annotation.as_deref());
    hasher.finalize().into()
}

fn claim_input_digest(
    root_id: nokv_types::RootId,
    request: &ClaimExpiredSnapshotRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(4, root_id, &request.workbench_id);
    hasher.update(request.snapshot_id.get().to_be_bytes());
    hasher.update(request.observed_now_ms.to_be_bytes());
    hasher.update(request.maximum_clock_skew_ms.to_be_bytes());
    hasher.finalize().into()
}

fn finish_reap_input_digest(
    root_id: nokv_types::RootId,
    request: &FinishSnapshotReapRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(5, root_id, &request.workbench_id);
    hasher.update(request.snapshot_id.get().to_be_bytes());
    hasher.finalize().into()
}

fn attach_input_digest(
    root_id: nokv_types::RootId,
    request: &AttachSnapshotConsumerRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(6, root_id, &request.workbench_id);
    hash_selector(&mut hasher, &request.selector);
    hasher.update(request.restore_operation_id.as_bytes());
    hasher.finalize().into()
}

fn release_input_digest(
    root_id: nokv_types::RootId,
    request: &ReleaseSnapshotConsumerRequest,
) -> [u8; SHA256_BYTES] {
    let mut hasher = input_hasher(7, root_id, &request.workbench_id);
    hasher.update(request.snapshot_id.get().to_be_bytes());
    hasher.update(request.restore_operation_id.as_bytes());
    hasher.finalize().into()
}

fn input_hasher(kind: u8, root_id: nokv_types::RootId, workbench_id: &WorkbenchId) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.snapshot.command\0");
    hasher.update([kind]);
    hasher.update(root_id.as_bytes());
    hash_bytes(&mut hasher, workbench_id.as_bytes());
    hasher
}

fn hash_selector(hasher: &mut Sha256, selector: &SnapshotSelector) {
    match selector {
        SnapshotSelector::Id(snapshot_id) => {
            hasher.update([1]);
            hasher.update(snapshot_id.get().to_be_bytes());
        }
        SnapshotSelector::Alias(alias) => {
            hasher.update([2]);
            hash_bytes(hasher, alias.as_bytes());
        }
    }
}

fn hash_optional_alias(hasher: &mut Sha256, alias: Option<&SnapshotAliasName>) {
    match alias {
        None => hasher.update([0]),
        Some(alias) => {
            hasher.update([1]);
            hash_bytes(hasher, alias.as_bytes());
        }
    }
}

fn hash_optional_bytes(hasher: &mut Sha256, value: Option<&[u8]>) {
    match value {
        None => hasher.update([0]),
        Some(value) => {
            hasher.update([1]);
            hash_bytes(hasher, value);
        }
    }
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn snapshot_change_event(
    workbench_id: &WorkbenchId,
    workspace_incarnation_id: WorkspaceIncarnationId,
    kind: ChangeEventKind,
    before: Option<&ResolvedSnapshot>,
    after: &ResolvedSnapshot,
    restore_operation_id: Option<OperationId>,
) -> Result<EventProjection, SnapshotError> {
    Ok(change_event_projection(&ChangeEventRecord {
        workbench_id: workbench_id.clone(),
        workspace_incarnation_id,
        kind,
        artifact_revision_id: None,
        commit_id: None,
        operation_id: restore_operation_id,
        path: None,
        before: before
            .map(|snapshot| snapshot_event_fields(snapshot, None))
            .transpose()?
            .unwrap_or_default(),
        after: snapshot_event_fields(after, restore_operation_id)?,
    })?)
}

fn snapshot_event_fields(
    snapshot: &ResolvedSnapshot,
    restore_operation_id: Option<OperationId>,
) -> Result<TypedProjection, QueryRecordError> {
    let mut fields = BTreeMap::from([
        (
            QueryFieldId::new("snapshot.consumer_count")?,
            QueryScalar::Unsigned(snapshot.record.consumer_count),
        ),
        (
            QueryFieldId::new("snapshot.consumer_epoch")?,
            QueryScalar::Unsigned(snapshot.record.consumer_epoch.get()),
        ),
        (
            QueryFieldId::new("snapshot.id")?,
            QueryScalar::Unsigned(snapshot.snapshot_id.get()),
        ),
        (
            QueryFieldId::new("snapshot.lease_deadline_ms")?,
            QueryScalar::Unsigned(snapshot.record.lease_deadline_ms),
        ),
        (
            QueryFieldId::new("snapshot.state")?,
            QueryScalar::Unsigned(u64::from(u8::from(snapshot.record.state))),
        ),
    ]);
    if let Some(operation_id) = restore_operation_id {
        fields.insert(
            QueryFieldId::new("snapshot.restore_operation_id")?,
            QueryScalar::Bytes(operation_id.as_bytes().to_vec()),
        );
    }
    TypedProjection::new(fields)
}

fn hex_id(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use nokv_types::{
        ArtifactRevisionId, CommitId, CommitState, LogicalShardId, OwnerEpoch, PlacementGeneration,
        ReadVersion, RequestId, RootActivationState, RootId, FIXED_ID_BYTES,
    };

    use super::super::codec::{
        commit_key, snapshot_commit_consumer_key, workbench_commit_head_key,
        workbench_head_commit_consumer_key,
    };
    use super::super::namespace::create_visible_workspace;
    use super::super::snapshot_query::list_snapshots_at;
    use super::super::{CommitConsumerRecord, CommitRecord, WorkbenchCommitHeadRecord};
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

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn operation(fill: u8) -> OperationId {
        OperationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn incarnation(fill: u8) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn commit(fill: u8) -> CommitId {
        CommitId::from_bytes([fill; SHA256_BYTES])
    }

    fn workbench(value: &str) -> WorkbenchId {
        WorkbenchId::new(value).unwrap()
    }

    fn alias(value: &str) -> SnapshotAliasName {
        SnapshotAliasName::new(value).unwrap()
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

    fn activate_root(store: &MetaShard) {
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(store, request(1), RootFenceAction::Install))
            .unwrap();
        store
            .execute(&fence_command(
                store,
                request(2),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
    }

    fn ready_store() -> MetaShard {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate_root(&store);
        store
    }

    fn write_context(store: &MetaShard, request_fill: u8) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            nokv_types::ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            request(request_fill),
        )
        .unwrap()
    }

    fn read_context(store: &MetaShard) -> RootReadContext {
        RootReadContext::current(store, root(), placement(), owner()).unwrap()
    }

    fn create_workspace(
        store: &MetaShard,
        request_fill: u8,
        name: &WorkbenchId,
        incarnation_fill: u8,
    ) -> WorkspaceIncarnationId {
        let incarnation = incarnation(incarnation_fill);
        create_visible_workspace(store, write_context(store, request_fill), name, incarnation)
            .unwrap();
        install_committed_head(
            store,
            request_fill.wrapping_add(128),
            incarnation,
            commit(incarnation_fill),
        );
        incarnation
    }

    fn install_committed_head(
        store: &MetaShard,
        request_fill: u8,
        workspace: WorkspaceIncarnationId,
        commit_id: CommitId,
    ) {
        let context = write_context(store, request_fill);
        let commit_record = sealed_commit_record(workspace);
        let head = WorkbenchCommitHeadRecord {
            commit_id,
            head_generation: Generation::new(1).unwrap(),
        };
        let mut command = base_command(context, Vec::new());
        put_absent(
            &mut command,
            MetadataFamily::Commit,
            commit_key(root(), commit_id),
            commit_record.encode().unwrap(),
        );
        put_absent(
            &mut command,
            MetadataFamily::CommitConsumer,
            workbench_head_commit_consumer_key(root(), commit_id, workspace),
            CommitConsumerRecord {
                consumer_epoch_at_add: ConsumerEpoch::new(1),
            }
            .encode(),
        );
        put_absent(
            &mut command,
            MetadataFamily::WorkbenchCommitHead,
            workbench_commit_head_key(root(), workspace),
            head.encode(),
        );
        store.execute(&command.seal()).unwrap();
    }

    fn sealed_commit_record(workspace: WorkspaceIncarnationId) -> CommitRecord {
        CommitRecord {
            source_workspace_incarnation_id: workspace,
            content_digest_uri: format!("sha256:{}", "11".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            tree_manifest_revision_id: ArtifactRevisionId::from_bytes([0x33; FIXED_ID_BYTES]),
            tree_digest_uri: format!("sha256:{}", "44".repeat(32)),
            member_count: 0,
            member_digest: [0; SHA256_BYTES],
            unique_revision_count: 1,
            revision_digest: [0x55; SHA256_BYTES],
            parent_commits: Vec::new(),
            parent_digest: [0; SHA256_BYTES],
            producer: Some("snapshot-test".to_owned()),
            lineage_projection: Vec::new(),
            consumer_count: 1,
            consumer_epoch: ConsumerEpoch::new(1),
            last_zero_consumer_version: None,
            state: CommitState::Sealed,
        }
    }

    fn switch_committed_head(
        store: &MetaShard,
        request_fill: u8,
        workspace: WorkspaceIncarnationId,
        next_commit_id: CommitId,
    ) {
        let context = write_context(store, request_fill);
        let head_key = workbench_commit_head_key(root(), workspace);
        let head_payload = read_family(store, MetadataFamily::WorkbenchCommitHead, &head_key)
            .expect("test head exists");
        let head = WorkbenchCommitHeadRecord::decode(&head_payload).unwrap();
        let old_commit_key = commit_key(root(), head.commit_id);
        let old_commit_payload = read_family(store, MetadataFamily::Commit, &old_commit_key)
            .expect("test commit exists");
        let old_commit = CommitRecord::decode(&old_commit_payload).unwrap();
        let old_consumer_key =
            workbench_head_commit_consumer_key(root(), head.commit_id, workspace);
        let old_consumer_payload =
            read_family(store, MetadataFamily::CommitConsumer, &old_consumer_key)
                .expect("test head consumer exists");
        let zero_version = CommitVersion::new(context.read_version.get() + 1).unwrap();
        let released = remove_commit_consumer(&old_commit, zero_version).unwrap();
        let next_commit = sealed_commit_record(workspace);
        let next_head = WorkbenchCommitHeadRecord {
            commit_id: next_commit_id,
            head_generation: Generation::new(head.head_generation.get() + 1).unwrap(),
        };
        let mut command = base_command(context, Vec::new());
        replace_exact(
            &mut command,
            MetadataFamily::WorkbenchCommitHead,
            head_key,
            head_payload,
            next_head.encode(),
        );
        replace_exact(
            &mut command,
            MetadataFamily::Commit,
            old_commit_key,
            old_commit_payload,
            released.encode().unwrap(),
        );
        delete_exact(
            &mut command,
            MetadataFamily::CommitConsumer,
            old_consumer_key,
            old_consumer_payload,
        );
        put_absent(
            &mut command,
            MetadataFamily::Commit,
            commit_key(root(), next_commit_id),
            next_commit.encode().unwrap(),
        );
        put_absent(
            &mut command,
            MetadataFamily::CommitConsumer,
            workbench_head_commit_consumer_key(root(), next_commit_id, workspace),
            CommitConsumerRecord {
                consumer_epoch_at_add: ConsumerEpoch::new(1),
            }
            .encode(),
        );
        store.execute(&command.seal()).unwrap();
    }

    fn read_commit(store: &MetaShard, commit_id: CommitId) -> CommitRecord {
        CommitRecord::decode(
            &read_family(
                store,
                MetadataFamily::Commit,
                &commit_key(root(), commit_id),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn install_legacy_v1_snapshot(
        store: &MetaShard,
        request_fill: u8,
        workspace: WorkspaceIncarnationId,
        snapshot_id: SnapshotId,
        lease_deadline_ms: u64,
    ) {
        let context = write_context(store, request_fill);
        let mut payload = Vec::new();
        payload.push(1);
        payload.extend_from_slice(&context.read_version.get().to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&lease_deadline_ms.to_be_bytes());
        payload.push(u8::from(SnapshotState::Active));
        payload.extend_from_slice(&0_u64.to_be_bytes());
        payload.extend_from_slice(&0_u64.to_be_bytes());
        payload.extend_from_slice(&0_u32.to_be_bytes());
        payload.push(0);
        SnapshotRefRecord::decode(&payload).unwrap();

        let mut command = base_command(context, Vec::new());
        put_absent(
            &mut command,
            MetadataFamily::SnapshotRef,
            snapshot_ref_key(root(), workspace, snapshot_id),
            payload,
        );
        put_absent(
            &mut command,
            MetadataFamily::SnapshotRef,
            snapshot_id_claim_key(root(), snapshot_id),
            workspace.as_bytes().to_vec(),
        );
        put_absent(
            &mut command,
            MetadataFamily::HistoryHold,
            snapshot_history_hold_key(root(), snapshot_id),
            HistoryHoldRecord {
                read_version: context.read_version,
                source_snapshot_id: None,
                state: HistoryHoldState::Active,
            }
            .encode(),
        );
        store.execute(&command.seal()).unwrap();
    }

    fn mint(
        store: &MetaShard,
        request_fill: u8,
        name: &WorkbenchId,
        snapshot_id: u64,
        alias_name: Option<&str>,
        deadline_ms: u64,
    ) -> SnapshotWriteOutcome {
        mint_snapshot(
            store,
            write_context(store, request_fill),
            &MintSnapshotRequest {
                workbench_id: name.clone(),
                snapshot_id: SnapshotId::new(snapshot_id),
                alias: alias_name.map(alias),
                lease_deadline_ms: deadline_ms,
                annotation: format!("snapshot-{snapshot_id}").into_bytes(),
            },
        )
        .unwrap()
    }

    fn get(store: &MetaShard, name: &WorkbenchId, selector: SnapshotSelector) -> ResolvedSnapshot {
        get_snapshot_at(store, read_context(store), name, &selector)
            .unwrap()
            .unwrap()
    }

    fn read_family(store: &MetaShard, family: MetadataFamily, key: &[u8]) -> Option<Vec<u8>> {
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

    #[test]
    fn uncommitted_mint_fails_without_retention_side_effects() {
        let store = ready_store();
        let name = workbench("uncommitted");
        let workspace = incarnation(3);
        create_visible_workspace(&store, write_context(&store, 3), &name, workspace).unwrap();

        assert_eq!(
            mint_snapshot(
                &store,
                write_context(&store, 4),
                &MintSnapshotRequest {
                    workbench_id: name.clone(),
                    snapshot_id: SnapshotId::new(11),
                    alias: Some(alias("must-not-exist")),
                    lease_deadline_ms: 100,
                    annotation: Vec::new(),
                },
            ),
            Err(SnapshotError::WorkspaceNotCommitted { workbench_id: name })
        );
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::SnapshotRef,
                &snapshot_ref_key(root(), workspace, SnapshotId::new(11)),
            ),
            None
        );
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::HistoryHold,
                &snapshot_history_hold_key(root(), SnapshotId::new(11)),
            ),
            None
        );
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::SnapshotRef,
                &snapshot_id_claim_key(root(), SnapshotId::new(11)),
            ),
            None
        );
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::SnapshotRef,
                &snapshot_alias_key(root(), workspace, &alias("must-not-exist")),
            ),
            None
        );
        assert!(store
            .scan_prefix_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::CommitConsumer,
                root().as_bytes(),
                store.current_read_version().unwrap(),
                None,
                8,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn mint_freezes_commit_and_retire_releases_it_once() {
        let store = ready_store();
        let name = workbench("committed");
        let workspace = create_workspace(&store, 3, &name, 3);
        let source_commit = commit(3);
        let successor_commit = commit(4);
        let mint_context = write_context(&store, 4);
        let minted = mint_snapshot(
            &store,
            mint_context,
            &MintSnapshotRequest {
                workbench_id: name.clone(),
                snapshot_id: SnapshotId::new(11),
                alias: None,
                lease_deadline_ms: 1_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();

        assert_eq!(minted.snapshot.record.source_commit_id, Some(source_commit));
        let retained = read_commit(&store, source_commit);
        assert_eq!(retained.consumer_count, 2);
        assert_eq!(retained.consumer_epoch, ConsumerEpoch::new(2));
        let consumer_key = snapshot_commit_consumer_key(root(), source_commit, SnapshotId::new(11));
        assert_eq!(
            CommitConsumerRecord::decode(
                &read_family(&store, MetadataFamily::CommitConsumer, &consumer_key).unwrap(),
            )
            .unwrap()
            .consumer_epoch_at_add,
            ConsumerEpoch::new(2)
        );

        let renewed = renew_snapshot(
            &store,
            write_context(&store, 5),
            &RenewSnapshotRequest {
                workbench_id: name.clone(),
                selector: SnapshotSelector::Id(SnapshotId::new(11)),
                lease_deadline_ms: 2_000,
            },
        )
        .unwrap();
        assert_eq!(
            renewed.snapshot.record.source_commit_id,
            Some(source_commit)
        );
        assert_eq!(read_commit(&store, source_commit), retained);

        switch_committed_head(&store, 6, workspace, successor_commit);
        let snapshot_only_retained = read_commit(&store, source_commit);
        assert_eq!(snapshot_only_retained.consumer_count, 1);
        assert_eq!(snapshot_only_retained.consumer_epoch, ConsumerEpoch::new(3));
        assert_eq!(snapshot_only_retained.last_zero_consumer_version, None);

        let retire_context = write_context(&store, 7);
        let retire_request = RetireSnapshotRequest {
            workbench_id: name,
            selector: SnapshotSelector::Id(SnapshotId::new(11)),
            retire_annotation: None,
        };
        let retired = retire_snapshot(&store, retire_context, &retire_request).unwrap();
        assert!(!retired.replayed);
        let released = read_commit(&store, source_commit);
        assert_eq!(released.consumer_count, 0);
        assert_eq!(released.consumer_epoch, ConsumerEpoch::new(4));
        assert_eq!(
            released.last_zero_consumer_version,
            Some(CommitVersion::new(retire_context.read_version.get() + 1).unwrap())
        );
        assert_eq!(
            read_family(&store, MetadataFamily::CommitConsumer, &consumer_key),
            None
        );

        let replay = retire_snapshot(&store, write_context(&store, 7), &retire_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, retired.commit_version);
        assert_eq!(read_commit(&store, source_commit), released);
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::WorkbenchCommitHead,
                &workbench_commit_head_key(root(), workspace),
            )
            .map(|payload| WorkbenchCommitHeadRecord::decode(&payload)
                .unwrap()
                .commit_id),
            Some(successor_commit)
        );
    }

    #[test]
    fn stale_head_cas_cannot_freeze_a_mixed_commit() {
        let store = ready_store();
        let name = workbench("head-race");
        let workspace = create_workspace(&store, 3, &name, 3);
        let old_commit = commit(3);
        let new_commit = commit(4);
        let stale_context = write_context(&store, 4);
        let request_value = MintSnapshotRequest {
            workbench_id: name,
            snapshot_id: SnapshotId::new(11),
            alias: None,
            lease_deadline_ms: 1_000,
            annotation: Vec::new(),
        };

        switch_committed_head(&store, 5, workspace, new_commit);
        assert_eq!(
            mint_snapshot(&store, stale_context, &request_value),
            Err(SnapshotError::ConcurrentMutation)
        );
        assert_eq!(read_commit(&store, old_commit).consumer_count, 0);

        let retry = mint_snapshot(&store, write_context(&store, 4), &request_value).unwrap();
        assert_eq!(retry.snapshot.record.source_commit_id, Some(new_commit));
        assert_eq!(read_commit(&store, old_commit).consumer_count, 0);
        assert_eq!(read_commit(&store, new_commit).consumer_count, 2);
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::CommitConsumer,
                &snapshot_commit_consumer_key(root(), old_commit, SnapshotId::new(11)),
            ),
            None
        );
        assert!(read_family(
            &store,
            MetadataFamily::CommitConsumer,
            &snapshot_commit_consumer_key(root(), new_commit, SnapshotId::new(11)),
        )
        .is_some());
    }

    #[test]
    fn legacy_v1_snapshot_renews_lists_retires_and_reaps_without_guessing_head() {
        let store = ready_store();
        let name = workbench("legacy");
        let workspace = incarnation(3);
        create_visible_workspace(&store, write_context(&store, 3), &name, workspace).unwrap();
        install_legacy_v1_snapshot(&store, 4, workspace, SnapshotId::new(11), 1_000);
        install_legacy_v1_snapshot(&store, 5, workspace, SnapshotId::new(12), 100);

        let listed = list_snapshots_at(&store, read_context(&store), &name, None, 10).unwrap();
        assert_eq!(listed.snapshots.len(), 2);
        assert!(listed
            .snapshots
            .iter()
            .all(|snapshot| snapshot.record.source_commit_id.is_none()));

        let renewed = renew_snapshot(
            &store,
            write_context(&store, 6),
            &RenewSnapshotRequest {
                workbench_id: name.clone(),
                selector: SnapshotSelector::Id(SnapshotId::new(11)),
                lease_deadline_ms: 2_000,
            },
        )
        .unwrap();
        assert_eq!(renewed.snapshot.record.source_commit_id, None);
        assert_eq!(renewed.snapshot.record.encode().unwrap()[0], 2);
        let retired = retire_snapshot(
            &store,
            write_context(&store, 7),
            &RetireSnapshotRequest {
                workbench_id: name.clone(),
                selector: SnapshotSelector::Id(SnapshotId::new(11)),
                retire_annotation: None,
            },
        )
        .unwrap();
        assert_eq!(retired.snapshot.record.state, SnapshotState::Retired);
        assert_eq!(retired.snapshot.record.source_commit_id, None);

        let claimed = claim_expired_snapshot(
            &store,
            write_context(&store, 8),
            &ClaimExpiredSnapshotRequest {
                workbench_id: name.clone(),
                snapshot_id: SnapshotId::new(12),
                observed_now_ms: 150,
                maximum_clock_skew_ms: 50,
            },
        )
        .unwrap();
        assert_eq!(claimed.snapshot.record.state, SnapshotState::ReapClaimed);
        assert_eq!(claimed.snapshot.record.source_commit_id, None);
        let reaped = finish_snapshot_reap(
            &store,
            write_context(&store, 9),
            &FinishSnapshotReapRequest {
                workbench_id: name,
                snapshot_id: SnapshotId::new(12),
            },
        )
        .unwrap();
        assert_eq!(reaped.snapshot.record.state, SnapshotState::Reaped);
        assert_eq!(reaped.snapshot.record.source_commit_id, None);
    }

    #[test]
    fn mint_exact_replay_survives_expiry_and_rejects_changed_input() {
        let store = ready_store();
        let name = workbench("replay");
        let workspace = create_workspace(&store, 3, &name, 3);
        let context = write_context(&store, 4);
        let request_value = MintSnapshotRequest {
            workbench_id: name.clone(),
            snapshot_id: SnapshotId::new(11),
            alias: Some(alias("checkpoint")),
            lease_deadline_ms: 100,
            annotation: b"exact".to_vec(),
        };

        let created = mint_snapshot(&store, context, &request_value).unwrap();
        assert!(!created.replayed);
        assert_eq!(created.snapshot.record.read_version, context.read_version);
        assert_eq!(
            created
                .snapshot
                .alias_record
                .as_ref()
                .unwrap()
                .alias_generation
                .get(),
            1
        );
        assert!(HistoryHoldRecord::decode(
            &read_family(
                &store,
                MetadataFamily::HistoryHold,
                &snapshot_history_hold_key(root(), SnapshotId::new(11)),
            )
            .unwrap(),
        )
        .is_ok());
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::SnapshotRef,
                &snapshot_id_claim_key(root(), SnapshotId::new(11)),
            ),
            Some(workspace.as_bytes().to_vec())
        );

        store
            .observe_lease_clock(root(), placement(), owner(), 200)
            .unwrap();
        let replay = mint_snapshot(&store, write_context(&store, 4), &request_value).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, created.commit_version);
        assert_eq!(replay.snapshot, created.snapshot);

        let mut changed = request_value.clone();
        changed.annotation = b"changed".to_vec();
        assert_eq!(
            mint_snapshot(&store, write_context(&store, 4), &changed),
            Err(SnapshotError::RequestInputMismatch)
        );
        assert_eq!(
            get(&store, &name, SnapshotSelector::Alias(alias("checkpoint")),).snapshot_id,
            SnapshotId::new(11)
        );
    }

    #[test]
    fn snapshot_exact_replay_rejects_a_missing_recovery_binding() {
        let store = ready_store();
        let name = workbench("recovery-binding");
        create_workspace(&store, 3, &name, 3);
        let request_value = MintSnapshotRequest {
            workbench_id: name,
            snapshot_id: SnapshotId::new(11),
            alias: None,
            lease_deadline_ms: 100,
            annotation: b"exact".to_vec(),
        };
        mint_snapshot(&store, write_context(&store, 4), &request_value).unwrap();
        let dedupe = store
            .lookup_request(root(), placement(), owner(), request(4))
            .unwrap()
            .unwrap();
        store
            .replace_recovery_header_for_test(dedupe.recovery_lsn, None)
            .unwrap();

        assert!(matches!(
            mint_snapshot(&store, write_context(&store, 4), &request_value),
            Err(SnapshotError::Meta(MetaError::CorruptRecord { .. }))
        ));
    }

    #[test]
    fn alias_remint_selects_latest_snapshot_exactly() {
        let store = ready_store();
        let name = workbench("aliases");
        create_workspace(&store, 3, &name, 3);
        mint(&store, 4, &name, 1, Some("latest"), 1_000);
        mint(&store, 5, &name, 2, Some("latest"), 1_000);

        let latest = get(&store, &name, SnapshotSelector::Alias(alias("latest")));
        assert_eq!(latest.snapshot_id, SnapshotId::new(2));
        assert_eq!(
            latest.alias_record.as_ref().unwrap().alias_generation.get(),
            2
        );
        assert_eq!(
            get(&store, &name, SnapshotSelector::Id(SnapshotId::new(1))).alias_record,
            None
        );

        retire_snapshot(
            &store,
            write_context(&store, 6),
            &RetireSnapshotRequest {
                workbench_id: name.clone(),
                selector: SnapshotSelector::Id(SnapshotId::new(1)),
                retire_annotation: None,
            },
        )
        .unwrap();
        assert_eq!(
            get(&store, &name, SnapshotSelector::Alias(alias("latest")),).snapshot_id,
            SnapshotId::new(2)
        );

        retire_snapshot(
            &store,
            write_context(&store, 7),
            &RetireSnapshotRequest {
                workbench_id: name.clone(),
                selector: SnapshotSelector::Alias(alias("latest")),
                retire_annotation: None,
            },
        )
        .unwrap();
        let terminal = get(&store, &name, SnapshotSelector::Alias(alias("latest")));
        assert_eq!(terminal.snapshot_id, SnapshotId::new(2));
        assert_eq!(terminal.record.state, SnapshotState::Retired);
        assert_eq!(
            terminal.alias_record.unwrap().snapshot_state,
            SnapshotState::Retired
        );
    }

    #[test]
    fn expired_but_unclaimed_snapshot_can_renew_past_durable_clock() {
        let store = ready_store();
        let name = workbench("renew");
        create_workspace(&store, 3, &name, 3);
        mint(&store, 4, &name, 1, None, 100);
        store
            .observe_lease_clock(root(), placement(), owner(), 150)
            .unwrap();

        let renewed = renew_snapshot(
            &store,
            write_context(&store, 5),
            &RenewSnapshotRequest {
                workbench_id: name.clone(),
                selector: SnapshotSelector::Id(SnapshotId::new(1)),
                lease_deadline_ms: 200,
            },
        )
        .unwrap();
        assert_eq!(renewed.snapshot.record.lease_deadline_ms, 200);
        assert_eq!(renewed.snapshot.record.state, SnapshotState::Active);

        store
            .observe_lease_clock(root(), placement(), owner(), 250)
            .unwrap();
        assert_eq!(
            renew_snapshot(
                &store,
                write_context(&store, 6),
                &RenewSnapshotRequest {
                    workbench_id: name,
                    selector: SnapshotSelector::Id(SnapshotId::new(1)),
                    lease_deadline_ms: 225,
                },
            ),
            Err(SnapshotError::LeaseDeadlineNotFuture {
                clock: 250,
                requested: 225,
            })
        );
    }

    #[test]
    fn attached_consumer_blocks_retire_until_exact_hold_is_released() {
        let store = ready_store();
        let name = workbench("consumer");
        create_workspace(&store, 3, &name, 3);
        mint(&store, 4, &name, 1, Some("source"), 1_000);
        let attach_context = write_context(&store, 5);
        let attach_request = AttachSnapshotConsumerRequest {
            workbench_id: name.clone(),
            selector: SnapshotSelector::Alias(alias("source")),
            restore_operation_id: operation(9),
        };

        let attached = attach_snapshot_consumer(&store, attach_context, &attach_request).unwrap();
        assert_eq!(attached.snapshot.record.consumer_count, 1);
        assert_eq!(
            attached.snapshot.record.consumer_epoch,
            ConsumerEpoch::new(1)
        );
        let attach_replay =
            attach_snapshot_consumer(&store, write_context(&store, 5), &attach_request).unwrap();
        assert!(attach_replay.replayed);
        assert_eq!(attach_replay.commit_version, attached.commit_version);

        let hold_key = restore_history_hold_key(root(), operation(9));
        let hold = HistoryHoldRecord::decode(
            &read_family(&store, MetadataFamily::HistoryHold, &hold_key).unwrap(),
        )
        .unwrap();
        assert_eq!(hold.read_version, attached.snapshot.record.read_version);
        assert_eq!(hold.source_snapshot_id, Some(SnapshotId::new(1)));
        assert_eq!(hold.state, HistoryHoldState::Active);

        assert_eq!(
            retire_snapshot(
                &store,
                write_context(&store, 6),
                &RetireSnapshotRequest {
                    workbench_id: name.clone(),
                    selector: SnapshotSelector::Id(SnapshotId::new(1)),
                    retire_annotation: None,
                },
            ),
            Err(SnapshotError::ForkRetentionActive {
                snapshot_id: SnapshotId::new(1),
                consumer_count: 1,
            })
        );

        let released = release_snapshot_consumer(
            &store,
            write_context(&store, 7),
            &ReleaseSnapshotConsumerRequest {
                workbench_id: name.clone(),
                snapshot_id: SnapshotId::new(1),
                restore_operation_id: operation(9),
            },
        )
        .unwrap();
        assert_eq!(released.snapshot.record.consumer_count, 0);
        assert_eq!(
            released.snapshot.record.consumer_epoch,
            ConsumerEpoch::new(2)
        );
        assert_eq!(
            read_family(&store, MetadataFamily::HistoryHold, &hold_key),
            None
        );

        let retired = retire_snapshot(
            &store,
            write_context(&store, 8),
            &RetireSnapshotRequest {
                workbench_id: name,
                selector: SnapshotSelector::Id(SnapshotId::new(1)),
                retire_annotation: Some(br#"{"metadata":null,"reason":"done"}"#.to_vec()),
            },
        )
        .unwrap();
        assert_eq!(retired.snapshot.record.state, SnapshotState::Retired);
        assert_eq!(
            retired.snapshot.record.retire_annotation.as_deref(),
            Some(br#"{"metadata":null,"reason":"done"}"#.as_slice())
        );
    }

    #[test]
    fn reaper_observes_clock_skew_grace_then_finishes_without_hold() {
        let store = ready_store();
        let name = workbench("reap");
        create_workspace(&store, 3, &name, 3);
        mint(&store, 4, &name, 1, Some("daily"), 100);

        assert_eq!(
            claim_expired_snapshot(
                &store,
                write_context(&store, 5),
                &ClaimExpiredSnapshotRequest {
                    workbench_id: name.clone(),
                    snapshot_id: SnapshotId::new(1),
                    observed_now_ms: 149,
                    maximum_clock_skew_ms: 50,
                },
            ),
            Err(SnapshotError::SnapshotNotExpired {
                deadline_ms: 100,
                lease_clock_ms: 149,
                maximum_clock_skew_ms: 50,
            })
        );

        let claim_context = write_context(&store, 6);
        let claim_request = ClaimExpiredSnapshotRequest {
            workbench_id: name.clone(),
            snapshot_id: SnapshotId::new(1),
            observed_now_ms: 150,
            maximum_clock_skew_ms: 50,
        };
        let claimed = claim_expired_snapshot(&store, claim_context, &claim_request).unwrap();
        assert_eq!(claimed.snapshot.record.state, SnapshotState::ReapClaimed);
        let released_commit = read_commit(&store, commit(3));
        assert_eq!(released_commit.consumer_count, 1);
        assert_eq!(released_commit.consumer_epoch, ConsumerEpoch::new(3));
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::CommitConsumer,
                &snapshot_commit_consumer_key(root(), commit(3), SnapshotId::new(1)),
            ),
            None
        );
        let replay =
            claim_expired_snapshot(&store, write_context(&store, 6), &claim_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, claimed.commit_version);
        assert_eq!(read_commit(&store, commit(3)), released_commit);
        assert_eq!(
            read_family(
                &store,
                MetadataFamily::HistoryHold,
                &snapshot_history_hold_key(root(), SnapshotId::new(1)),
            ),
            None
        );
        assert!(matches!(
            renew_snapshot(
                &store,
                write_context(&store, 7),
                &RenewSnapshotRequest {
                    workbench_id: name.clone(),
                    selector: SnapshotSelector::Id(SnapshotId::new(1)),
                    lease_deadline_ms: 300,
                },
            ),
            Err(SnapshotError::SnapshotNotActive {
                state: SnapshotState::ReapClaimed,
                ..
            })
        ));

        let reaped = finish_snapshot_reap(
            &store,
            write_context(&store, 8),
            &FinishSnapshotReapRequest {
                workbench_id: name.clone(),
                snapshot_id: SnapshotId::new(1),
            },
        )
        .unwrap();
        assert_eq!(reaped.snapshot.record.state, SnapshotState::Reaped);
        let by_alias = get(&store, &name, SnapshotSelector::Alias(alias("daily")));
        assert_eq!(by_alias.snapshot_id, SnapshotId::new(1));
        assert_eq!(by_alias.record.state, SnapshotState::Reaped);
    }

    #[test]
    fn snapshot_id_claim_is_root_global_across_workspaces() {
        let store = ready_store();
        let first = workbench("first");
        let second = workbench("second");
        create_workspace(&store, 3, &first, 3);
        create_workspace(&store, 4, &second, 4);
        mint(&store, 5, &first, 42, None, 1_000);
        retire_snapshot(
            &store,
            write_context(&store, 6),
            &RetireSnapshotRequest {
                workbench_id: first,
                selector: SnapshotSelector::Id(SnapshotId::new(42)),
                retire_annotation: None,
            },
        )
        .unwrap();

        assert_eq!(
            mint_snapshot(
                &store,
                write_context(&store, 7),
                &MintSnapshotRequest {
                    workbench_id: second,
                    snapshot_id: SnapshotId::new(42),
                    alias: None,
                    lease_deadline_ms: 1_000,
                    annotation: Vec::new(),
                },
            ),
            Err(SnapshotError::SnapshotIdAlreadyClaimed {
                snapshot_id: SnapshotId::new(42),
            })
        );
    }

    #[test]
    fn file_reopen_preserves_snapshot_alias_claim_clock_and_replay() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("snapshot-metadata");
        let name = workbench("durable");
        let mint_context;
        let mint_request;
        let created;
        {
            let store = crate::workspace::test_support::initialize_file(&path, shard()).unwrap();
            activate_root(&store);
            create_workspace(&store, 3, &name, 3);
            mint_context = write_context(&store, 4);
            mint_request = MintSnapshotRequest {
                workbench_id: name.clone(),
                snapshot_id: SnapshotId::new(77),
                alias: Some(alias("saved")),
                lease_deadline_ms: 1_000,
                annotation: b"persisted".to_vec(),
            };
            created = mint_snapshot(&store, mint_context, &mint_request).unwrap();
            store
                .observe_lease_clock(root(), placement(), owner(), 321)
                .unwrap();
        }

        let reopened = crate::workspace::test_support::open_file(&path, shard()).unwrap();
        assert_eq!(reopened.lease_clock_high_water().unwrap(), 321);
        assert_eq!(
            get(&reopened, &name, SnapshotSelector::Alias(alias("saved")),),
            created.snapshot
        );
        assert_eq!(
            read_family(
                &reopened,
                MetadataFamily::SnapshotRef,
                &snapshot_id_claim_key(root(), SnapshotId::new(77)),
            ),
            Some(incarnation(3).as_bytes().to_vec())
        );

        let replay = mint_snapshot(&reopened, write_context(&reopened, 4), &mint_request).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, created.commit_version);
        assert_eq!(replay.snapshot, created.snapshot);
        assert_eq!(
            mint_context.read_version,
            ReadVersion::new(created.commit_version.get() - 1).unwrap()
        );
    }
}
