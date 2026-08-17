/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Recoverable publication of immutable Generic namespace-index generations.
//!
//! Registration freezes one visible workspace incarnation and read version,
//! appends an ordered metadata-only row closure under a never-reused
//! generation id, and swaps the exact current pointer only after the closure
//! is sealed. Staging rows are never query-visible.

use std::collections::BTreeSet;
use std::fmt;

use nokv_types::{
    CommandDigest, CommitVersion, Generation, GenericIndexGenerationId,
    GenericIndexGenerationState, GenericIndexReferenceKind, GenericIndexRegistrationPhase,
    HistoryHoldState, NormalizedRelativePath, OperationId, OperationKind, ReferenceEpoch,
    WorkbenchId, WorkspaceIncarnationId, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::codec::{
    decode_generic_index_row_key, generic_index_append_receipt_key,
    generic_index_append_receipt_prefix, generic_index_current_key, generic_index_generation_key,
    generic_index_generation_ref_key, generic_index_generation_ref_prefix, generic_index_row_key,
    generic_index_row_prefix, operation_key, path_child_prefix,
    register_generic_index_history_hold_key, workspace_current_key, SCHEMA_ID,
};
use super::engine::{
    CommandFit, CommandMutation, CommandPredicate, HistoryProjection, MetaError, MetaShard,
    MetadataCommand, MetadataCommandResult, RootFenceAction,
};
use super::generic_index_records::{
    advance_generic_index_row_rolling_digest, empty_generic_index_row_digest,
    generic_index_append_input_digest, generic_index_capability_digest,
    generic_index_current_owner_digest, generic_index_registration_owner_digest,
    generic_index_row_digest, verify_generic_index_generation_seal,
    GenericIndexAppendReceiptRecord, GenericIndexArtifactBinding, GenericIndexCurrentRecord,
    GenericIndexFieldCapability, GenericIndexFieldValues, GenericIndexGenerationRecord,
    GenericIndexGenerationRefRecord, GenericIndexRecordError,
    GenericIndexRegistrationOperationRecord, GenericIndexRowBinding, GenericIndexRowRecord,
};
use super::keyspace::MetadataFamily;
use super::namespace::{
    get_visible_path_at, get_visible_workspace_at, NamespaceError, RootReadContext,
    RootWriteContext,
};
use super::publication_records::{PathEntry, PublicationRecordCodecError};
use super::query_records::QueryFieldId;
use super::snapshot_records::{HistoryHoldRecord, SnapshotRecordError};

/// One append command may use at most 240 row mutations, leaving room for the
/// operation, generation, receipt, predicates, history, dedupe, and recovery
/// envelopes inside the independent metadata-command item bound.
pub const MAX_GENERIC_INDEX_APPEND_BATCH_ROWS: usize = 240;
/// One abort cleanup command removes at most this many generation-owned rows
/// and receipts before advancing its durable cleanup cursor by deletion.
pub const MAX_GENERIC_INDEX_CLEANUP_BATCH_ROWS: usize = 120;

/// Immutable identity copied into every owner of a sealed generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GenericIndexGenerationSeal {
    pub generation_id: GenericIndexGenerationId,
    pub capability_digest: [u8; SHA256_BYTES],
    pub row_count: u64,
    pub row_digest: [u8; SHA256_BYTES],
}

/// Exact compare-and-swap delta for one generation reference mutation.
///
/// Callers merge this into their own lifecycle command so the owner row,
/// generation counter, durable cursor, and any visibility pointer move in one
/// metadata commit.
#[derive(Clone, Debug)]
pub(crate) struct GenericIndexReferenceDelta {
    pub predicates: Vec<CommandPredicate>,
    pub mutations: Vec<CommandMutation>,
    pub history: Vec<HistoryProjection>,
}

#[derive(Clone, Debug)]
pub struct BeginGenericIndexRegistrationRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub generation_id: GenericIndexGenerationId,
    pub workbench_id: WorkbenchId,
    pub expected_workspace_incarnation_id: WorkspaceIncarnationId,
    /// `None` registers the exact Workbench root.
    pub index_path: Option<NormalizedRelativePath>,
    /// `None` is create-only; `Some` is an exact current-pointer generation
    /// compare-and-swap. There is deliberately no upsert condition.
    pub expected_current_generation: Option<Generation>,
    pub capabilities: Vec<GenericIndexFieldCapability>,
    pub declared_row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexRowInput {
    /// Relative to the registration root. `None` denotes that root itself.
    pub relative_path: Option<NormalizedRelativePath>,
    pub values: Vec<GenericIndexFieldValues>,
}

#[derive(Clone, Debug)]
pub struct AppendGenericIndexRowsRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub first_sequence: u64,
    pub rows: Vec<GenericIndexRowInput>,
}

#[derive(Clone, Copy, Debug)]
pub struct FinalizeGenericIndexRegistrationRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
}

#[derive(Clone, Copy, Debug)]
pub struct AbortGenericIndexRegistrationRequest {
    pub context: RootWriteContext,
    pub operation_id: OperationId,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexRegistrationOutcome {
    pub operation: GenericIndexRegistrationOperationRecord,
    pub commit_version: CommitVersion,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendGenericIndexRowsOutcome {
    pub command: GenericIndexRegistrationOutcome,
    pub receipt: GenericIndexAppendReceiptRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortGenericIndexRegistrationOutcome {
    pub command: GenericIndexRegistrationOutcome,
    pub removed_rows: usize,
    pub removed_receipts: usize,
    pub cleanup_complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GenericIndexError {
    Meta(MetaError),
    Namespace(NamespaceError),
    Record(GenericIndexRecordError),
    WorkspaceCodec(PublicationRecordCodecError),
    HoldCodec(SnapshotRecordError),
    WorkspaceMissing,
    WorkspaceIncarnationMismatch,
    RegistrationRootIsArtifact,
    RegistrationRootMissing,
    OperationMissing,
    OperationInputMismatch,
    GenerationAlreadyExists,
    GenerationMissing,
    GenerationMismatch,
    CurrentPointerConflict,
    CurrentPointerMissing,
    CurrentReferenceMissing,
    RegistrationReferenceMissing,
    HistoryHoldMissing,
    HistoryHoldMismatch,
    InvalidPhase {
        expected: &'static str,
        actual: GenericIndexRegistrationPhase,
    },
    InvalidBatchLimit {
        requested: usize,
        max: usize,
    },
    EmptyAppendBatch,
    AppendSequenceMismatch {
        expected: u64,
        actual: u64,
    },
    AppendExceedsDeclaredRows,
    RowsNotStrictlyOrdered,
    UndeclaredField {
        field: QueryFieldId,
    },
    RowPathMissing {
        relative_path: Option<NormalizedRelativePath>,
    },
    PathJoinInvalid {
        reason: String,
    },
    CounterOverflow {
        field: &'static str,
    },
    ResourceExhausted {
        actual: usize,
        maximum: usize,
    },
    ReplayResultMismatch,
    CorruptKey {
        family: &'static str,
    },
}

impl fmt::Display for GenericIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta(error) => error.fmt(formatter),
            Self::Namespace(error) => error.fmt(formatter),
            Self::Record(error) => error.fmt(formatter),
            Self::WorkspaceCodec(error) => error.fmt(formatter),
            Self::HoldCodec(error) => error.fmt(formatter),
            Self::WorkspaceMissing => formatter.write_str("visible Workbench was not found"),
            Self::WorkspaceIncarnationMismatch => {
                formatter.write_str("visible Workbench incarnation changed")
            }
            Self::RegistrationRootIsArtifact => {
                formatter.write_str("Generic index registration root resolves to an artifact")
            }
            Self::RegistrationRootMissing => formatter.write_str(
                "Generic index registration root is not a virtual or implicit directory",
            ),
            Self::OperationMissing => formatter.write_str("Generic index operation is missing"),
            Self::OperationInputMismatch => {
                formatter.write_str("Generic index operation input does not match")
            }
            Self::GenerationAlreadyExists => {
                formatter.write_str("Generic index generation id was already used")
            }
            Self::GenerationMissing => formatter.write_str("Generic index generation is missing"),
            Self::GenerationMismatch => {
                formatter.write_str("Generic index generation does not match its operation")
            }
            Self::CurrentPointerConflict => {
                formatter.write_str("Generic index current-pointer condition failed")
            }
            Self::CurrentPointerMissing => {
                formatter.write_str("Generic index current pointer is missing")
            }
            Self::CurrentReferenceMissing => {
                formatter.write_str("Generic index current reference is missing")
            }
            Self::RegistrationReferenceMissing => {
                formatter.write_str("Generic index registration reference is missing")
            }
            Self::HistoryHoldMissing => {
                formatter.write_str("Generic index registration history hold is missing")
            }
            Self::HistoryHoldMismatch => {
                formatter.write_str("Generic index registration history hold is inconsistent")
            }
            Self::InvalidPhase { expected, actual } => write!(
                formatter,
                "Generic index operation phase is {actual:?}, expected {expected}"
            ),
            Self::InvalidBatchLimit { requested, max } => write!(
                formatter,
                "Generic index batch limit {requested} is outside 1..={max}"
            ),
            Self::EmptyAppendBatch => formatter.write_str("Generic index append batch is empty"),
            Self::AppendSequenceMismatch { expected, actual } => write!(
                formatter,
                "Generic index append begins at {actual}, expected {expected}"
            ),
            Self::AppendExceedsDeclaredRows => {
                formatter.write_str("Generic index append exceeds the declared row closure")
            }
            Self::RowsNotStrictlyOrdered => formatter
                .write_str("Generic index rows must be strictly ordered and unique by path"),
            Self::UndeclaredField { field } => {
                write!(formatter, "Generic index row uses undeclared field {field}")
            }
            Self::RowPathMissing { relative_path } => write!(
                formatter,
                "Generic index row path {:?} is absent at the frozen source version",
                relative_path.as_ref().map(NormalizedRelativePath::as_str)
            ),
            Self::PathJoinInvalid { reason } => {
                write!(formatter, "Generic index row path is invalid: {reason}")
            }
            Self::CounterOverflow { field } => write!(formatter, "{field} overflows"),
            Self::ResourceExhausted { actual, maximum } => write!(
                formatter,
                "Generic index command needs {actual} units, provider maximum is {maximum}"
            ),
            Self::ReplayResultMismatch => {
                formatter.write_str("Generic index deterministic replay result does not match")
            }
            Self::CorruptKey { family } => write!(formatter, "corrupt {family} key"),
        }
    }
}

impl std::error::Error for GenericIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Meta(error) => Some(error),
            Self::Namespace(error) => Some(error),
            Self::Record(error) => Some(error),
            Self::WorkspaceCodec(error) => Some(error),
            Self::HoldCodec(error) => Some(error),
            _ => None,
        }
    }
}

impl From<MetaError> for GenericIndexError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<NamespaceError> for GenericIndexError {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}

impl From<GenericIndexRecordError> for GenericIndexError {
    fn from(error: GenericIndexRecordError) -> Self {
        Self::Record(error)
    }
}

impl From<PublicationRecordCodecError> for GenericIndexError {
    fn from(error: PublicationRecordCodecError) -> Self {
        Self::WorkspaceCodec(error)
    }
}

impl From<SnapshotRecordError> for GenericIndexError {
    fn from(error: SnapshotRecordError) -> Self {
        Self::HoldCodec(error)
    }
}

#[derive(Clone, Copy)]
pub struct GenericIndexRegistrationService<'a> {
    store: &'a MetaShard,
}

impl<'a> GenericIndexRegistrationService<'a> {
    pub const fn new(store: &'a MetaShard) -> Self {
        Self { store }
    }

    pub fn get(
        &self,
        context: RootReadContext,
        operation_id: OperationId,
    ) -> Result<Option<GenericIndexRegistrationOperationRecord>, GenericIndexError> {
        self.store
            .read_at(
                context.root_id,
                context.placement_generation,
                context.owner_epoch,
                MetadataFamily::Operation,
                &operation_key(
                    context.root_id,
                    OperationKind::RegisterGenericIndex,
                    operation_id,
                ),
                context.read_version,
            )?
            .map(|payload| {
                GenericIndexRegistrationOperationRecord::decode(&payload).map_err(Into::into)
            })
            .transpose()
    }

    pub fn begin(
        &self,
        request: BeginGenericIndexRegistrationRequest,
    ) -> Result<GenericIndexRegistrationOutcome, GenericIndexError> {
        let capability_digest = generic_index_capability_digest(&request.capabilities)?;
        let request_digest = registration_request_digest(&request, capability_digest);
        let operation_key = operation_key(
            request.context.root_id,
            OperationKind::RegisterGenericIndex,
            request.operation_id,
        );
        if let Some(loaded) = self.read_record(
            request.context,
            MetadataFamily::Operation,
            &operation_key,
            GenericIndexRegistrationOperationRecord::decode,
        )? {
            if loaded.record.request_digest != request_digest
                || loaded.record.generation_id != request.generation_id
                || loaded.record.workspace_incarnation_id
                    != request.expected_workspace_incarnation_id
                || loaded.record.index_path != request.index_path
                || loaded.record.expected_current_generation != request.expected_current_generation
                || loaded.record.capability_digest != capability_digest
                || loaded.record.declared_row_count != request.declared_row_count
            {
                return Err(GenericIndexError::OperationInputMismatch);
            }
            return Ok(observed_outcome(loaded.record));
        }

        let generation_key =
            generic_index_generation_key(request.context.root_id, request.generation_id);
        if self
            .read_payload(
                request.context,
                MetadataFamily::GenericIndexGeneration,
                &generation_key,
            )?
            .is_some()
        {
            return Err(GenericIndexError::GenerationAlreadyExists);
        }

        let read_context = read_context(request.context);
        let workspace = get_visible_workspace_at(self.store, read_context, &request.workbench_id)?
            .ok_or(GenericIndexError::WorkspaceMissing)?;
        if workspace.incarnation_id != request.expected_workspace_incarnation_id {
            return Err(GenericIndexError::WorkspaceIncarnationMismatch);
        }
        self.require_registration_directory(
            read_context,
            &request.workbench_id,
            workspace.incarnation_id,
            request.index_path.as_ref(),
        )?;

        let current_key = generic_index_current_key(
            request.context.root_id,
            workspace.incarnation_id,
            request.index_path.as_ref(),
        );
        let current = self.read_record(
            request.context,
            MetadataFamily::GenericIndexCurrent,
            &current_key,
            GenericIndexCurrentRecord::decode,
        )?;
        if current
            .as_ref()
            .map(|current| current.record.pointer_generation)
            != request.expected_current_generation
        {
            return Err(GenericIndexError::CurrentPointerConflict);
        }

        let transition_version = next_commit_version(request.context)?;
        let operation = GenericIndexRegistrationOperationRecord {
            workspace_incarnation_id: workspace.incarnation_id,
            index_path: request.index_path.clone(),
            generation_id: request.generation_id,
            request_digest,
            source_read_version: request.context.read_version,
            last_transition_version: transition_version,
            expected_current_generation: request.expected_current_generation,
            capability_digest,
            declared_row_count: request.declared_row_count,
            appended_row_count: 0,
            rolling_row_digest: empty_generic_index_row_digest(),
            phase: GenericIndexRegistrationPhase::Appending,
            published_pointer_generation: None,
            terminal_error: None,
        };
        let generation = GenericIndexGenerationRecord {
            capabilities: request.capabilities,
            declared_row_count: request.declared_row_count,
            appended_row_count: 0,
            rolling_row_digest: empty_generic_index_row_digest(),
            reference_count: 1,
            reference_epoch: ReferenceEpoch::new(1),
            last_zero_reference_version: None,
            state: GenericIndexGenerationState::Building,
        };
        let registration_owner = generic_index_registration_owner_digest(request.operation_id);
        let reference_key = generic_index_generation_ref_key(
            request.context.root_id,
            request.generation_id,
            GenericIndexReferenceKind::Registration,
            registration_owner,
        );
        let reference = GenericIndexGenerationRefRecord {
            kind: GenericIndexReferenceKind::Registration,
            owner_digest: registration_owner,
            reference_epoch_at_add: generation.reference_epoch,
        };
        let hold_key =
            register_generic_index_history_hold_key(request.context.root_id, request.operation_id);
        let hold = HistoryHoldRecord {
            read_version: request.context.read_version,
            source_snapshot_id: None,
            state: HistoryHoldState::Active,
        };

        let mut plan = CommandPlan::default();
        plan.assert_value(
            MetadataFamily::WorkspaceCurrent,
            workspace_current_key(request.context.root_id, &request.workbench_id),
            Some(workspace.encode()?),
        );
        plan.assert_value(
            MetadataFamily::GenericIndexCurrent,
            current_key,
            current.map(|current| current.payload),
        );
        plan.prefix_empty(
            MetadataFamily::GenericIndexGeneration,
            generic_index_row_prefix(request.context.root_id, request.generation_id),
        );
        plan.prefix_empty(
            MetadataFamily::GenericIndexGeneration,
            generic_index_generation_ref_prefix(request.context.root_id, request.generation_id),
        );
        plan.prefix_empty(
            MetadataFamily::GenericIndexGeneration,
            generic_index_append_receipt_prefix(request.context.root_id, request.generation_id),
        );
        plan.put_absent(
            MetadataFamily::Operation,
            operation_key,
            operation.encode()?,
        );
        plan.put_absent(
            MetadataFamily::GenericIndexGeneration,
            generation_key,
            generation.encode()?,
        );
        plan.put_absent(
            MetadataFamily::GenericIndexGeneration,
            reference_key,
            reference.encode()?,
        );
        plan.put_absent(MetadataFamily::HistoryHold, hold_key, hold.encode());
        decode_outcome(self.execute_plan(request.context, plan, operation.encode()?)?)
    }

    fn require_registration_directory(
        &self,
        context: RootReadContext,
        workbench_id: &WorkbenchId,
        workspace_incarnation_id: WorkspaceIncarnationId,
        path: Option<&NormalizedRelativePath>,
    ) -> Result<(), GenericIndexError> {
        let Some(path) = path else {
            return Ok(());
        };
        if get_visible_path_at(self.store, context, workbench_id, path)?.is_some() {
            return Err(GenericIndexError::RegistrationRootIsArtifact);
        }
        if is_virtual_section(path)
            || !self
                .store
                .scan_prefix_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    MetadataFamily::PathCurrent,
                    &path_child_prefix(context.root_id, workspace_incarnation_id, Some(path)),
                    context.read_version,
                    None,
                    1,
                )?
                .is_empty()
        {
            Ok(())
        } else {
            Err(GenericIndexError::RegistrationRootMissing)
        }
    }
}

#[derive(Clone)]
struct Loaded<T> {
    payload: Vec<u8>,
    record: T,
}

#[derive(Clone, Default)]
struct CommandPlan {
    predicates: Vec<CommandPredicate>,
    mutations: Vec<CommandMutation>,
    history: Vec<HistoryProjection>,
}

impl CommandPlan {
    fn assert_value(&mut self, family: MetadataFamily, key: Vec<u8>, expected: Option<Vec<u8>>) {
        self.predicates.push(CommandPredicate::Value {
            family,
            key,
            expected,
        });
    }

    fn prefix_empty(&mut self, family: MetadataFamily, prefix: Vec<u8>) {
        self.predicates
            .push(CommandPredicate::PrefixEmpty { family, prefix });
    }

    fn put_absent(&mut self, family: MetadataFamily, key: Vec<u8>, value: Vec<u8>) {
        self.assert_value(family, key.clone(), None);
        self.mutations
            .push(CommandMutation::Put { family, key, value });
    }

    fn replace(&mut self, family: MetadataFamily, key: Vec<u8>, previous: Vec<u8>, value: Vec<u8>) {
        self.assert_value(family, key.clone(), Some(previous));
        self.history.push(HistoryProjection {
            family,
            key: key.clone(),
        });
        self.mutations
            .push(CommandMutation::Put { family, key, value });
    }

    fn delete(&mut self, family: MetadataFamily, key: Vec<u8>, previous: Vec<u8>) {
        self.assert_value(family, key.clone(), Some(previous));
        self.history.push(HistoryProjection {
            family,
            key: key.clone(),
        });
        self.mutations.push(CommandMutation::Delete { family, key });
    }
}

impl GenericIndexRegistrationService<'_> {
    pub fn abort(
        &self,
        request: AbortGenericIndexRegistrationRequest,
    ) -> Result<AbortGenericIndexRegistrationOutcome, GenericIndexError> {
        validate_limit(request.limit, MAX_GENERIC_INDEX_CLEANUP_BATCH_ROWS)?;
        let loaded_operation = self.load_operation(request.context, request.operation_id)?;
        if loaded_operation.record.phase == GenericIndexRegistrationPhase::Cleaned {
            return Ok(AbortGenericIndexRegistrationOutcome {
                command: observed_outcome(loaded_operation.record),
                removed_rows: 0,
                removed_receipts: 0,
                cleanup_complete: true,
            });
        }
        if !matches!(
            loaded_operation.record.phase,
            GenericIndexRegistrationPhase::Appending
                | GenericIndexRegistrationPhase::Aborting
                | GenericIndexRegistrationPhase::Cleaning
        ) {
            return Err(GenericIndexError::InvalidPhase {
                expected: "Appending, Aborting, or Cleaning",
                actual: loaded_operation.record.phase,
            });
        }
        let hold = self.load_hold(request.context, request.operation_id)?;
        verify_hold(&loaded_operation.record, &hold.record)?;
        let loaded_generation =
            self.load_generation(request.context, loaded_operation.record.generation_id)?;
        verify_build_generation(&loaded_operation.record, &loaded_generation.record)?;
        let registration_owner = generic_index_registration_owner_digest(request.operation_id);
        let registration_ref_key = generic_index_generation_ref_key(
            request.context.root_id,
            loaded_operation.record.generation_id,
            GenericIndexReferenceKind::Registration,
            registration_owner,
        );
        let registration_ref = self
            .read_record(
                request.context,
                MetadataFamily::GenericIndexGeneration,
                &registration_ref_key,
                GenericIndexGenerationRefRecord::decode,
            )?
            .ok_or(GenericIndexError::RegistrationReferenceMissing)?;
        if registration_ref.record.kind != GenericIndexReferenceKind::Registration
            || registration_ref.record.owner_digest != registration_owner
            || registration_ref.record.reference_epoch_at_add
                > loaded_generation.record.reference_epoch
            || loaded_generation.record.reference_count != 1
        {
            return Err(GenericIndexError::GenerationMismatch);
        }

        let row_prefix = generic_index_row_prefix(
            request.context.root_id,
            loaded_operation.record.generation_id,
        );
        let mut row_page = self.store.scan_prefix_at(
            request.context.root_id,
            request.context.placement_generation,
            request.context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &row_prefix,
            request.context.read_version,
            None,
            request.limit + 1,
        )?;
        let rows_have_more = row_page.len() > request.limit;
        if rows_have_more {
            row_page.truncate(request.limit);
        }
        // Append receipts are compact response-loss authority and deliberately
        // survive Abort. Only materialized staging rows are cleaned here.
        let cleanup_complete = !rows_have_more && row_page.len() < request.limit;

        let mut next_operation = loaded_operation.record.clone();
        next_operation.last_transition_version = next_commit_version(request.context)?;
        next_operation.phase = if cleanup_complete {
            GenericIndexRegistrationPhase::Cleaned
        } else {
            GenericIndexRegistrationPhase::Cleaning
        };
        let mut plan = CommandPlan::default();
        for row in &row_page {
            let sequence = decode_generic_index_row_key(
                request.context.root_id,
                loaded_operation.record.generation_id,
                &row.key,
            )
            .ok_or(GenericIndexError::CorruptKey {
                family: "GenericIndexGeneration(row)",
            })?;
            if sequence >= loaded_operation.record.appended_row_count {
                return Err(GenericIndexError::CorruptKey {
                    family: "GenericIndexGeneration(row)",
                });
            }
            GenericIndexRowRecord::decode(&row.value)?;
            plan.delete(
                MetadataFamily::GenericIndexGeneration,
                row.key.clone(),
                row.value.clone(),
            );
        }
        if cleanup_complete {
            let mut next_generation = loaded_generation.record.clone();
            next_generation.reference_count = 0;
            next_generation.reference_epoch =
                ReferenceEpoch::new(next_generation.reference_epoch.get().checked_add(1).ok_or(
                    GenericIndexError::CounterOverflow {
                        field: "reference_epoch",
                    },
                )?);
            next_generation.last_zero_reference_version =
                Some(next_commit_version(request.context)?);
            next_generation.state = GenericIndexGenerationState::Retired;
            plan.replace(
                MetadataFamily::GenericIndexGeneration,
                generic_index_generation_key(
                    request.context.root_id,
                    loaded_operation.record.generation_id,
                ),
                loaded_generation.payload,
                next_generation.encode()?,
            );
            plan.delete(
                MetadataFamily::GenericIndexGeneration,
                registration_ref_key,
                registration_ref.payload,
            );
            plan.delete(
                MetadataFamily::HistoryHold,
                register_generic_index_history_hold_key(
                    request.context.root_id,
                    request.operation_id,
                ),
                hold.payload,
            );
        }
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::RegisterGenericIndex,
                request.operation_id,
            ),
            loaded_operation.payload,
            next_operation.encode()?,
        );
        let command =
            decode_outcome(self.execute_plan(request.context, plan, next_operation.encode()?)?)?;
        Ok(AbortGenericIndexRegistrationOutcome {
            command,
            removed_rows: row_page.len(),
            removed_receipts: 0,
            cleanup_complete,
        })
    }

    pub fn finalize(
        &self,
        request: FinalizeGenericIndexRegistrationRequest,
    ) -> Result<GenericIndexRegistrationOutcome, GenericIndexError> {
        let loaded_operation = self.load_operation(request.context, request.operation_id)?;
        if loaded_operation.record.phase == GenericIndexRegistrationPhase::Complete {
            return Ok(observed_outcome(loaded_operation.record));
        }
        require_phase(
            &loaded_operation.record,
            GenericIndexRegistrationPhase::Appending,
            "Appending",
        )?;
        if loaded_operation.record.appended_row_count != loaded_operation.record.declared_row_count
        {
            return Err(GenericIndexError::GenerationMismatch);
        }
        let hold = self.load_hold(request.context, request.operation_id)?;
        verify_hold(&loaded_operation.record, &hold.record)?;
        let loaded_generation =
            self.load_generation(request.context, loaded_operation.record.generation_id)?;
        verify_build_generation(&loaded_operation.record, &loaded_generation.record)?;
        if loaded_generation.record.appended_row_count
            != loaded_generation.record.declared_row_count
        {
            return Err(GenericIndexError::GenerationMismatch);
        }

        let current_key = generic_index_current_key(
            request.context.root_id,
            loaded_operation.record.workspace_incarnation_id,
            loaded_operation.record.index_path.as_ref(),
        );
        let current = self.read_record(
            request.context,
            MetadataFamily::GenericIndexCurrent,
            &current_key,
            GenericIndexCurrentRecord::decode,
        )?;
        if current
            .as_ref()
            .map(|current| current.record.pointer_generation)
            != loaded_operation.record.expected_current_generation
        {
            return Err(GenericIndexError::CurrentPointerConflict);
        }
        let pointer_generation = match loaded_operation.record.expected_current_generation {
            None => Generation::new(1).expect("one is a non-zero generation"),
            Some(previous) => Generation::new(previous.get().checked_add(1).ok_or(
                GenericIndexError::CounterOverflow {
                    field: "pointer_generation",
                },
            )?)
            .expect("checked successor is non-zero"),
        };
        let next_current = GenericIndexCurrentRecord {
            generation_id: loaded_operation.record.generation_id,
            pointer_generation,
            capability_digest: loaded_operation.record.capability_digest,
            row_count: loaded_operation.record.declared_row_count,
            row_digest: loaded_operation.record.rolling_row_digest,
        };

        let registration_owner = generic_index_registration_owner_digest(request.operation_id);
        let registration_ref_key = generic_index_generation_ref_key(
            request.context.root_id,
            loaded_operation.record.generation_id,
            GenericIndexReferenceKind::Registration,
            registration_owner,
        );
        let registration_ref = self
            .read_record(
                request.context,
                MetadataFamily::GenericIndexGeneration,
                &registration_ref_key,
                GenericIndexGenerationRefRecord::decode,
            )?
            .ok_or(GenericIndexError::RegistrationReferenceMissing)?;
        if registration_ref.record.kind != GenericIndexReferenceKind::Registration
            || registration_ref.record.owner_digest != registration_owner
            || registration_ref.record.reference_epoch_at_add
                > loaded_generation.record.reference_epoch
            || loaded_generation.record.reference_count != 1
        {
            return Err(GenericIndexError::GenerationMismatch);
        }

        let current_owner = generic_index_current_owner_digest(
            loaded_operation.record.workspace_incarnation_id,
            loaded_operation.record.index_path.as_ref(),
        );
        let current_ref_key = generic_index_generation_ref_key(
            request.context.root_id,
            loaded_operation.record.generation_id,
            GenericIndexReferenceKind::Current,
            current_owner,
        );
        if self
            .read_payload(
                request.context,
                MetadataFamily::GenericIndexGeneration,
                &current_ref_key,
            )?
            .is_some()
        {
            return Err(GenericIndexError::GenerationMismatch);
        }
        let next_epoch = ReferenceEpoch::new(
            loaded_generation
                .record
                .reference_epoch
                .get()
                .checked_add(1)
                .ok_or(GenericIndexError::CounterOverflow {
                    field: "reference_epoch",
                })?,
        );
        let mut next_generation = loaded_generation.record.clone();
        next_generation.reference_epoch = next_epoch;
        next_generation.state = GenericIndexGenerationState::Sealed;
        let current_ref = GenericIndexGenerationRefRecord {
            kind: GenericIndexReferenceKind::Current,
            owner_digest: current_owner,
            reference_epoch_at_add: next_epoch,
        };
        verify_generic_index_generation_seal(
            &next_generation,
            next_current.capability_digest,
            next_current.row_count,
            next_current.row_digest,
        )?;

        let mut next_operation = loaded_operation.record.clone();
        next_operation.last_transition_version = next_commit_version(request.context)?;
        next_operation.phase = GenericIndexRegistrationPhase::Complete;
        next_operation.published_pointer_generation = Some(pointer_generation);
        let mut plan = CommandPlan::default();
        if let Some(old_current) = current {
            self.plan_remove_old_current_reference(
                request.context,
                &old_current.record,
                current_owner,
                &mut plan,
            )?;
            plan.replace(
                MetadataFamily::GenericIndexCurrent,
                current_key,
                old_current.payload,
                next_current.encode()?,
            );
        } else {
            plan.put_absent(
                MetadataFamily::GenericIndexCurrent,
                current_key,
                next_current.encode()?,
            );
        }
        plan.replace(
            MetadataFamily::GenericIndexGeneration,
            generic_index_generation_key(
                request.context.root_id,
                loaded_operation.record.generation_id,
            ),
            loaded_generation.payload,
            next_generation.encode()?,
        );
        plan.delete(
            MetadataFamily::GenericIndexGeneration,
            registration_ref_key,
            registration_ref.payload,
        );
        plan.put_absent(
            MetadataFamily::GenericIndexGeneration,
            current_ref_key,
            current_ref.encode()?,
        );
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::RegisterGenericIndex,
                request.operation_id,
            ),
            loaded_operation.payload,
            next_operation.encode()?,
        );
        plan.delete(
            MetadataFamily::HistoryHold,
            register_generic_index_history_hold_key(request.context.root_id, request.operation_id),
            hold.payload,
        );
        match self.execute_plan(request.context, plan, next_operation.encode()?) {
            Ok(result) => decode_outcome(result),
            Err(GenericIndexError::Meta(MetaError::PredicateFailed | MetaError::WriteConflict)) => {
                Err(GenericIndexError::CurrentPointerConflict)
            }
            Err(error) => Err(error),
        }
    }

    pub fn append(
        &self,
        request: AppendGenericIndexRowsRequest,
    ) -> Result<AppendGenericIndexRowsOutcome, GenericIndexError> {
        if request.rows.is_empty() {
            return Err(GenericIndexError::EmptyAppendBatch);
        }
        validate_limit(request.rows.len(), MAX_GENERIC_INDEX_APPEND_BATCH_ROWS)?;
        if request
            .rows
            .windows(2)
            .any(|pair| pair[0].relative_path >= pair[1].relative_path)
        {
            return Err(GenericIndexError::RowsNotStrictlyOrdered);
        }
        let input_rows = request
            .rows
            .iter()
            .map(|input| {
                let row = GenericIndexRowRecord {
                    relative_path: input.relative_path.clone(),
                    binding: input
                        .relative_path
                        .as_ref()
                        .map_or(GenericIndexRowBinding::Directory, |_| {
                            GenericIndexRowBinding::Unbound
                        }),
                    values: input.values.clone(),
                };
                row.validate()?;
                Ok(row)
            })
            .collect::<Result<Vec<_>, GenericIndexError>>()?;
        let input_digest = generic_index_append_input_digest(request.first_sequence, &input_rows)?;

        let loaded_operation = self.load_operation(request.context, request.operation_id)?;
        let receipt_key = generic_index_append_receipt_key(
            request.context.root_id,
            loaded_operation.record.generation_id,
            request.first_sequence,
        );
        if let Some(receipt) = self.read_record(
            request.context,
            MetadataFamily::GenericIndexGeneration,
            &receipt_key,
            GenericIndexAppendReceiptRecord::decode,
        )? {
            if receipt.record.first_sequence != request.first_sequence
                || usize::try_from(receipt.record.row_count).ok() != Some(request.rows.len())
                || receipt.record.input_digest != input_digest
                || receipt.record.resulting_row_count
                    != request
                        .first_sequence
                        .checked_add(request.rows.len() as u64)
                        .ok_or(GenericIndexError::CounterOverflow {
                            field: "append_sequence",
                        })?
            {
                return Err(GenericIndexError::OperationInputMismatch);
            }
            let mut replay_operation = loaded_operation.record;
            replay_operation.appended_row_count = receipt.record.resulting_row_count;
            replay_operation.rolling_row_digest = receipt.record.resulting_row_digest;
            replay_operation.phase = GenericIndexRegistrationPhase::Appending;
            replay_operation.published_pointer_generation = None;
            replay_operation.terminal_error = None;
            replay_operation.last_transition_version = receipt.record.commit_version;
            return Ok(AppendGenericIndexRowsOutcome {
                command: GenericIndexRegistrationOutcome {
                    operation: replay_operation,
                    commit_version: receipt.record.commit_version,
                    replayed: true,
                },
                receipt: receipt.record,
            });
        }

        require_phase(
            &loaded_operation.record,
            GenericIndexRegistrationPhase::Appending,
            "Appending",
        )?;
        let hold = self.load_hold(request.context, request.operation_id)?;
        verify_hold(&loaded_operation.record, &hold.record)?;
        let loaded_generation =
            self.load_generation(request.context, loaded_operation.record.generation_id)?;
        verify_build_generation(&loaded_operation.record, &loaded_generation.record)?;

        let rows = self.bind_rows_at_frozen_source(
            request.context,
            &loaded_operation.record,
            &loaded_generation.record,
            &request.rows,
        )?;
        if request.first_sequence != loaded_operation.record.appended_row_count {
            return Err(GenericIndexError::AppendSequenceMismatch {
                expected: loaded_operation.record.appended_row_count,
                actual: request.first_sequence,
            });
        }
        let resulting_count = request
            .first_sequence
            .checked_add(rows.len() as u64)
            .ok_or(GenericIndexError::CounterOverflow {
                field: "appended_row_count",
            })?;
        if resulting_count > loaded_operation.record.declared_row_count {
            return Err(GenericIndexError::AppendExceedsDeclaredRows);
        }
        if request.first_sequence > 0 {
            let previous_key = generic_index_row_key(
                request.context.root_id,
                loaded_operation.record.generation_id,
                request.first_sequence - 1,
            );
            let previous = self
                .read_record(
                    request.context,
                    MetadataFamily::GenericIndexGeneration,
                    &previous_key,
                    GenericIndexRowRecord::decode,
                )?
                .ok_or(GenericIndexError::GenerationMismatch)?;
            if previous.record.relative_path >= rows[0].relative_path {
                return Err(GenericIndexError::RowsNotStrictlyOrdered);
            }
        }

        let mut rolling = loaded_operation.record.rolling_row_digest;
        for (offset, row) in rows.iter().enumerate() {
            let sequence = request.first_sequence + offset as u64;
            rolling = advance_generic_index_row_rolling_digest(
                rolling,
                generic_index_row_digest(sequence, row)?,
            );
        }
        let transition_version = next_commit_version(request.context)?;
        let receipt = GenericIndexAppendReceiptRecord {
            first_sequence: request.first_sequence,
            row_count: u32::try_from(rows.len()).map_err(|_| {
                GenericIndexError::CounterOverflow {
                    field: "append_row_count",
                }
            })?,
            commit_version: transition_version,
            input_digest,
            resulting_row_count: resulting_count,
            resulting_row_digest: rolling,
        };
        let mut next_generation = loaded_generation.record.clone();
        next_generation.appended_row_count = resulting_count;
        next_generation.rolling_row_digest = rolling;
        let mut next_operation = loaded_operation.record.clone();
        next_operation.last_transition_version = transition_version;
        next_operation.appended_row_count = resulting_count;
        next_operation.rolling_row_digest = rolling;

        let mut plan = CommandPlan::default();
        for (offset, row) in rows.iter().enumerate() {
            plan.put_absent(
                MetadataFamily::GenericIndexGeneration,
                generic_index_row_key(
                    request.context.root_id,
                    loaded_operation.record.generation_id,
                    request.first_sequence + offset as u64,
                ),
                row.encode()?,
            );
        }
        plan.put_absent(
            MetadataFamily::GenericIndexGeneration,
            receipt_key,
            receipt.encode()?,
        );
        plan.replace(
            MetadataFamily::GenericIndexGeneration,
            generic_index_generation_key(
                request.context.root_id,
                loaded_operation.record.generation_id,
            ),
            loaded_generation.payload,
            next_generation.encode()?,
        );
        plan.replace(
            MetadataFamily::Operation,
            operation_key(
                request.context.root_id,
                OperationKind::RegisterGenericIndex,
                request.operation_id,
            ),
            loaded_operation.payload,
            next_operation.encode()?,
        );
        let command =
            decode_outcome(self.execute_plan(request.context, plan, next_operation.encode()?)?)?;
        Ok(AppendGenericIndexRowsOutcome { command, receipt })
    }

    fn read_payload(
        &self,
        context: RootWriteContext,
        family: MetadataFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, GenericIndexError> {
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

    fn read_record<T>(
        &self,
        context: RootWriteContext,
        family: MetadataFamily,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<T, GenericIndexRecordError>,
    ) -> Result<Option<Loaded<T>>, GenericIndexError> {
        self.read_payload(context, family, key)?
            .map(|payload| {
                let record = decode(&payload)?;
                Ok(Loaded { payload, record })
            })
            .transpose()
    }

    fn load_operation(
        &self,
        context: RootWriteContext,
        operation_id: OperationId,
    ) -> Result<Loaded<GenericIndexRegistrationOperationRecord>, GenericIndexError> {
        self.read_record(
            context,
            MetadataFamily::Operation,
            &operation_key(
                context.root_id,
                OperationKind::RegisterGenericIndex,
                operation_id,
            ),
            GenericIndexRegistrationOperationRecord::decode,
        )?
        .ok_or(GenericIndexError::OperationMissing)
    }

    fn load_generation(
        &self,
        context: RootWriteContext,
        generation_id: GenericIndexGenerationId,
    ) -> Result<Loaded<GenericIndexGenerationRecord>, GenericIndexError> {
        self.read_record(
            context,
            MetadataFamily::GenericIndexGeneration,
            &generic_index_generation_key(context.root_id, generation_id),
            GenericIndexGenerationRecord::decode,
        )?
        .ok_or(GenericIndexError::GenerationMissing)
    }

    fn load_hold(
        &self,
        context: RootWriteContext,
        operation_id: OperationId,
    ) -> Result<Loaded<HistoryHoldRecord>, GenericIndexError> {
        let key = register_generic_index_history_hold_key(context.root_id, operation_id);
        let payload = self
            .read_payload(context, MetadataFamily::HistoryHold, &key)?
            .ok_or(GenericIndexError::HistoryHoldMissing)?;
        let record = HistoryHoldRecord::decode(&payload)?;
        Ok(Loaded { payload, record })
    }

    fn execute_plan(
        &self,
        context: RootWriteContext,
        plan: CommandPlan,
        deterministic_result: Vec<u8>,
    ) -> Result<MetadataCommandResult, GenericIndexError> {
        let command = build_command(context, plan, deterministic_result);
        match self.store.command_fit(&command, None)? {
            CommandFit::Fits => self.store.execute(&command).map_err(Into::into),
            CommandFit::Exceeds {
                actual, maximum, ..
            } => Err(GenericIndexError::ResourceExhausted { actual, maximum }),
        }
    }

    fn bind_rows_at_frozen_source(
        &self,
        context: RootWriteContext,
        operation: &GenericIndexRegistrationOperationRecord,
        generation: &GenericIndexGenerationRecord,
        inputs: &[GenericIndexRowInput],
    ) -> Result<Vec<GenericIndexRowRecord>, GenericIndexError> {
        let source_context = RootReadContext {
            root_id: context.root_id,
            placement_generation: context.placement_generation,
            owner_epoch: context.owner_epoch,
            read_version: operation.source_read_version,
        };
        let declared = generation
            .capabilities
            .iter()
            .map(|capability| capability.field.clone())
            .collect::<BTreeSet<_>>();
        inputs
            .iter()
            .map(|input| {
                for values in &input.values {
                    if !declared.contains(&values.field) {
                        return Err(GenericIndexError::UndeclaredField {
                            field: values.field.clone(),
                        });
                    }
                }
                let full_path =
                    join_index_path(operation.index_path.as_ref(), input.relative_path.as_ref())?;
                let binding = self.resolve_row_binding(
                    source_context,
                    operation.workspace_incarnation_id,
                    full_path.as_ref(),
                )?;
                let row = GenericIndexRowRecord {
                    relative_path: input.relative_path.clone(),
                    binding,
                    values: input.values.clone(),
                };
                row.validate()?;
                Ok(row)
            })
            .collect()
    }

    fn resolve_row_binding(
        &self,
        context: RootReadContext,
        workspace_incarnation_id: WorkspaceIncarnationId,
        full_path: Option<&NormalizedRelativePath>,
    ) -> Result<GenericIndexRowBinding, GenericIndexError> {
        let Some(full_path) = full_path else {
            return Ok(GenericIndexRowBinding::Directory);
        };
        let exact = self.store.read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::PathCurrent,
            &super::codec::path_current_key(context.root_id, workspace_incarnation_id, full_path),
            context.read_version,
        )?;
        if let Some(payload) = exact {
            let entry = PathEntry::decode(&payload)?;
            return Ok(GenericIndexRowBinding::Artifact(
                GenericIndexArtifactBinding {
                    artifact_revision_id: entry.artifact_revision_id,
                    path_generation: entry.generation,
                },
            ));
        }
        if is_virtual_section(full_path)
            || !self
                .store
                .scan_prefix_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    MetadataFamily::PathCurrent,
                    &path_child_prefix(context.root_id, workspace_incarnation_id, Some(full_path)),
                    context.read_version,
                    None,
                    1,
                )?
                .is_empty()
        {
            return Ok(GenericIndexRowBinding::Directory);
        }
        // A registered row never creates namespace state. An absent source
        // path keeps the historical path-keyed contract: it becomes visible
        // only if that path later resolves to a current namespace node.
        Ok(GenericIndexRowBinding::Unbound)
    }

    fn plan_remove_old_current_reference(
        &self,
        context: RootWriteContext,
        current: &GenericIndexCurrentRecord,
        current_owner: [u8; SHA256_BYTES],
        plan: &mut CommandPlan,
    ) -> Result<(), GenericIndexError> {
        let delta = plan_remove_generic_index_reference(
            self.store,
            context,
            GenericIndexGenerationSeal {
                generation_id: current.generation_id,
                capability_digest: current.capability_digest,
                row_count: current.row_count,
                row_digest: current.row_digest,
            },
            GenericIndexReferenceKind::Current,
            current_owner,
        )?;
        plan.predicates.extend(delta.predicates);
        plan.mutations.extend(delta.mutations);
        plan.history.extend(delta.history);
        Ok(())
    }
}

pub(crate) fn plan_add_generic_index_reference(
    store: &MetaShard,
    context: RootWriteContext,
    seal: GenericIndexGenerationSeal,
    kind: GenericIndexReferenceKind,
    owner_digest: [u8; SHA256_BYTES],
) -> Result<GenericIndexReferenceDelta, GenericIndexError> {
    let generation_key = generic_index_generation_key(context.root_id, seal.generation_id);
    let generation_payload = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &generation_key,
            context.read_version,
        )?
        .ok_or(GenericIndexError::GenerationMissing)?;
    let generation = GenericIndexGenerationRecord::decode(&generation_payload)?;
    verify_generic_index_generation_seal(
        &generation,
        seal.capability_digest,
        seal.row_count,
        seal.row_digest,
    )?;
    let reference_key =
        generic_index_generation_ref_key(context.root_id, seal.generation_id, kind, owner_digest);
    if store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &reference_key,
            context.read_version,
        )?
        .is_some()
    {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let next_epoch = ReferenceEpoch::new(generation.reference_epoch.get().checked_add(1).ok_or(
        GenericIndexError::CounterOverflow {
            field: "reference_epoch",
        },
    )?);
    let mut next_generation = generation;
    next_generation.reference_epoch = next_epoch;
    next_generation.reference_count = next_generation.reference_count.checked_add(1).ok_or(
        GenericIndexError::CounterOverflow {
            field: "reference_count",
        },
    )?;
    next_generation.last_zero_reference_version = None;
    let next_payload = next_generation.encode()?;
    let reference = GenericIndexGenerationRefRecord {
        kind,
        owner_digest,
        reference_epoch_at_add: next_epoch,
    }
    .encode()?;
    Ok(GenericIndexReferenceDelta {
        predicates: vec![
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key.clone(),
                expected: Some(generation_payload.clone()),
            },
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: reference_key.clone(),
                expected: None,
            },
        ],
        mutations: vec![
            CommandMutation::Put {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key.clone(),
                value: next_payload,
            },
            CommandMutation::Put {
                family: MetadataFamily::GenericIndexGeneration,
                key: reference_key,
                value: reference,
            },
        ],
        history: vec![HistoryProjection {
            family: MetadataFamily::GenericIndexGeneration,
            key: generation_key,
        }],
    })
}

pub(crate) fn plan_remove_generic_index_reference(
    store: &MetaShard,
    context: RootWriteContext,
    seal: GenericIndexGenerationSeal,
    kind: GenericIndexReferenceKind,
    owner_digest: [u8; SHA256_BYTES],
) -> Result<GenericIndexReferenceDelta, GenericIndexError> {
    let generation_key = generic_index_generation_key(context.root_id, seal.generation_id);
    let generation_payload = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &generation_key,
            context.read_version,
        )?
        .ok_or(GenericIndexError::GenerationMissing)?;
    let generation = GenericIndexGenerationRecord::decode(&generation_payload)?;
    verify_generic_index_generation_seal(
        &generation,
        seal.capability_digest,
        seal.row_count,
        seal.row_digest,
    )?;
    if generation.reference_count == 0 {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let reference_key =
        generic_index_generation_ref_key(context.root_id, seal.generation_id, kind, owner_digest);
    let reference_payload = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &reference_key,
            context.read_version,
        )?
        .ok_or(GenericIndexError::CurrentReferenceMissing)?;
    let reference = GenericIndexGenerationRefRecord::decode(&reference_payload)?;
    if reference.kind != kind
        || reference.owner_digest != owner_digest
        || reference.reference_epoch_at_add > generation.reference_epoch
    {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let next_epoch = ReferenceEpoch::new(generation.reference_epoch.get().checked_add(1).ok_or(
        GenericIndexError::CounterOverflow {
            field: "reference_epoch",
        },
    )?);
    let mut next_generation = generation;
    next_generation.reference_epoch = next_epoch;
    next_generation.reference_count -= 1;
    if next_generation.reference_count == 0 {
        next_generation.last_zero_reference_version = Some(next_commit_version(context)?);
    }
    let next_payload = next_generation.encode()?;
    Ok(GenericIndexReferenceDelta {
        predicates: vec![
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key.clone(),
                expected: Some(generation_payload.clone()),
            },
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: reference_key.clone(),
                expected: Some(reference_payload.clone()),
            },
        ],
        mutations: vec![
            CommandMutation::Put {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key.clone(),
                value: next_payload,
            },
            CommandMutation::Delete {
                family: MetadataFamily::GenericIndexGeneration,
                key: reference_key,
            },
        ],
        history: vec![
            HistoryProjection {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key,
            },
            HistoryProjection {
                family: MetadataFamily::GenericIndexGeneration,
                key: reference_key_for_history(
                    context.root_id,
                    seal.generation_id,
                    kind,
                    owner_digest,
                ),
            },
        ],
    })
}

pub(crate) fn plan_transfer_generic_index_reference(
    store: &MetaShard,
    context: RootWriteContext,
    seal: GenericIndexGenerationSeal,
    from_kind: GenericIndexReferenceKind,
    from_owner_digest: [u8; SHA256_BYTES],
    to_kind: GenericIndexReferenceKind,
    to_owner_digest: [u8; SHA256_BYTES],
) -> Result<GenericIndexReferenceDelta, GenericIndexError> {
    if from_kind == to_kind && from_owner_digest == to_owner_digest {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let generation_key = generic_index_generation_key(context.root_id, seal.generation_id);
    let generation_payload = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &generation_key,
            context.read_version,
        )?
        .ok_or(GenericIndexError::GenerationMissing)?;
    let generation = GenericIndexGenerationRecord::decode(&generation_payload)?;
    verify_generic_index_generation_seal(
        &generation,
        seal.capability_digest,
        seal.row_count,
        seal.row_digest,
    )?;
    if generation.reference_count == 0 {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let from_key = generic_index_generation_ref_key(
        context.root_id,
        seal.generation_id,
        from_kind,
        from_owner_digest,
    );
    let from_payload = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &from_key,
            context.read_version,
        )?
        .ok_or(GenericIndexError::CurrentReferenceMissing)?;
    let from = GenericIndexGenerationRefRecord::decode(&from_payload)?;
    if from.kind != from_kind
        || from.owner_digest != from_owner_digest
        || from.reference_epoch_at_add > generation.reference_epoch
    {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let to_key = generic_index_generation_ref_key(
        context.root_id,
        seal.generation_id,
        to_kind,
        to_owner_digest,
    );
    if store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &to_key,
            context.read_version,
        )?
        .is_some()
    {
        return Err(GenericIndexError::GenerationMismatch);
    }
    let next_epoch = ReferenceEpoch::new(generation.reference_epoch.get().checked_add(1).ok_or(
        GenericIndexError::CounterOverflow {
            field: "reference_epoch",
        },
    )?);
    let mut next_generation = generation;
    next_generation.reference_epoch = next_epoch;
    let next_payload = next_generation.encode()?;
    let to_payload = GenericIndexGenerationRefRecord {
        kind: to_kind,
        owner_digest: to_owner_digest,
        reference_epoch_at_add: next_epoch,
    }
    .encode()?;
    Ok(GenericIndexReferenceDelta {
        predicates: vec![
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key.clone(),
                expected: Some(generation_payload),
            },
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: from_key.clone(),
                expected: Some(from_payload),
            },
            CommandPredicate::Value {
                family: MetadataFamily::GenericIndexGeneration,
                key: to_key.clone(),
                expected: None,
            },
        ],
        mutations: vec![
            CommandMutation::Put {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key.clone(),
                value: next_payload,
            },
            CommandMutation::Delete {
                family: MetadataFamily::GenericIndexGeneration,
                key: from_key.clone(),
            },
            CommandMutation::Put {
                family: MetadataFamily::GenericIndexGeneration,
                key: to_key,
                value: to_payload,
            },
        ],
        history: vec![
            HistoryProjection {
                family: MetadataFamily::GenericIndexGeneration,
                key: generation_key,
            },
            HistoryProjection {
                family: MetadataFamily::GenericIndexGeneration,
                key: from_key,
            },
        ],
    })
}

fn reference_key_for_history(
    root_id: nokv_types::RootId,
    generation_id: GenericIndexGenerationId,
    kind: GenericIndexReferenceKind,
    owner_digest: [u8; SHA256_BYTES],
) -> Vec<u8> {
    generic_index_generation_ref_key(root_id, generation_id, kind, owner_digest)
}

fn build_command(
    context: RootWriteContext,
    plan: CommandPlan,
    deterministic_result: Vec<u8>,
) -> MetadataCommand {
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
        event_projection: Vec::new(),
        deterministic_result,
    }
    .seal()
}

fn decode_outcome(
    result: MetadataCommandResult,
) -> Result<GenericIndexRegistrationOutcome, GenericIndexError> {
    let operation = GenericIndexRegistrationOperationRecord::decode(&result.deterministic_result)?;
    Ok(GenericIndexRegistrationOutcome {
        operation,
        commit_version: result.commit_version,
        replayed: result.replayed,
    })
}

fn observed_outcome(
    operation: GenericIndexRegistrationOperationRecord,
) -> GenericIndexRegistrationOutcome {
    let commit_version = operation.last_transition_version;
    GenericIndexRegistrationOutcome {
        operation,
        commit_version,
        replayed: true,
    }
}

fn registration_request_digest(
    request: &BeginGenericIndexRegistrationRequest,
    capability_digest: [u8; SHA256_BYTES],
) -> CommandDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.generic-index.registration.v1\0");
    hasher.update(request.operation_id.as_bytes());
    hasher.update(request.generation_id.as_bytes());
    hasher.update(request.workbench_id.as_bytes());
    hasher.update(request.expected_workspace_incarnation_id.as_bytes());
    hash_optional_path(&mut hasher, request.index_path.as_ref());
    match request.expected_current_generation {
        None => hasher.update([0]),
        Some(generation) => {
            hasher.update([1]);
            hasher.update(generation.get().to_be_bytes());
        }
    }
    hasher.update(capability_digest);
    hasher.update(request.declared_row_count.to_be_bytes());
    CommandDigest::from_bytes(hasher.finalize().into())
}

fn hash_optional_path(hasher: &mut Sha256, path: Option<&NormalizedRelativePath>) {
    match path {
        None => hasher.update([0]),
        Some(path) => {
            hasher.update([1]);
            hasher.update((path.byte_len() as u64).to_be_bytes());
            hasher.update(path.as_str().as_bytes());
        }
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

fn next_commit_version(context: RootWriteContext) -> Result<CommitVersion, GenericIndexError> {
    CommitVersion::new(context.read_version.get().checked_add(1).ok_or(
        GenericIndexError::CounterOverflow {
            field: "commit_version",
        },
    )?)
    .map_err(|_| GenericIndexError::CounterOverflow {
        field: "commit_version",
    })
}

fn validate_limit(requested: usize, max: usize) -> Result<(), GenericIndexError> {
    if (1..=max).contains(&requested) {
        Ok(())
    } else {
        Err(GenericIndexError::InvalidBatchLimit { requested, max })
    }
}

fn require_phase(
    operation: &GenericIndexRegistrationOperationRecord,
    expected: GenericIndexRegistrationPhase,
    expected_name: &'static str,
) -> Result<(), GenericIndexError> {
    if operation.phase == expected {
        Ok(())
    } else {
        Err(GenericIndexError::InvalidPhase {
            expected: expected_name,
            actual: operation.phase,
        })
    }
}

fn verify_hold(
    operation: &GenericIndexRegistrationOperationRecord,
    hold: &HistoryHoldRecord,
) -> Result<(), GenericIndexError> {
    if hold.read_version != operation.source_read_version
        || hold.source_snapshot_id.is_some()
        || hold.state != HistoryHoldState::Active
    {
        Err(GenericIndexError::HistoryHoldMismatch)
    } else {
        Ok(())
    }
}

fn verify_build_generation(
    operation: &GenericIndexRegistrationOperationRecord,
    generation: &GenericIndexGenerationRecord,
) -> Result<(), GenericIndexError> {
    if generation.state != GenericIndexGenerationState::Building
        || generation.declared_row_count != operation.declared_row_count
        || generation.appended_row_count != operation.appended_row_count
        || generation.rolling_row_digest != operation.rolling_row_digest
        || generic_index_capability_digest(&generation.capabilities)? != operation.capability_digest
    {
        Err(GenericIndexError::GenerationMismatch)
    } else {
        Ok(())
    }
}

fn join_index_path(
    index_path: Option<&NormalizedRelativePath>,
    relative_path: Option<&NormalizedRelativePath>,
) -> Result<Option<NormalizedRelativePath>, GenericIndexError> {
    match (index_path, relative_path) {
        (None, None) => Ok(None),
        (Some(path), None) | (None, Some(path)) => Ok(Some(path.clone())),
        (Some(index), Some(relative)) => {
            NormalizedRelativePath::new(format!("{}/{}", index.as_str(), relative.as_str()))
                .map(Some)
                .map_err(|error| GenericIndexError::PathJoinInvalid {
                    reason: error.to_string(),
                })
        }
    }
}

fn is_virtual_section(path: &NormalizedRelativePath) -> bool {
    path.component_count() == 1
        && matches!(
            path.as_str(),
            "input" | "scripts" | "outputs" | "logs" | "metadata"
        )
}

#[cfg(test)]
mod tests {
    use nokv_types::{
        ArtifactRevisionId, LogicalShardId, ObjectNamespaceId, OwnerEpoch, PlacementGeneration,
        RequestId, RootActivationState, RootId, FIXED_ID_BYTES,
    };

    use super::super::codec::path_current_key;
    use super::super::generic_index_records::GenericIndexOperator;
    use super::super::namespace::create_visible_workspace;
    use super::super::query_records::{QueryScalar, TypedProjection};
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

    fn generation(fill: u8) -> GenericIndexGenerationId {
        GenericIndexGenerationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn incarnation(fill: u8) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn revision(value: usize) -> ArtifactRevisionId {
        let mut bytes = [0; FIXED_ID_BYTES];
        bytes[..8].copy_from_slice(&(value as u64).to_be_bytes());
        ArtifactRevisionId::from_bytes(bytes)
    }

    fn workbench() -> WorkbenchId {
        WorkbenchId::new("generic-index-tests").unwrap()
    }

    fn field() -> QueryFieldId {
        QueryFieldId::new("custom.value").unwrap()
    }

    fn capability() -> GenericIndexFieldCapability {
        GenericIndexFieldCapability {
            field: field(),
            operators: vec![
                GenericIndexOperator::Equal,
                GenericIndexOperator::NotEqual,
                GenericIndexOperator::In,
                GenericIndexOperator::Greater,
                GenericIndexOperator::GreaterOrEqual,
                GenericIndexOperator::Less,
                GenericIndexOperator::LessOrEqual,
                GenericIndexOperator::Exists,
                GenericIndexOperator::NotExists,
            ],
            sortable: true,
            facetable: true,
        }
    }

    fn write_context(store: &MetaShard, request_fill: u8) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            request(request_fill),
        )
        .unwrap()
    }

    fn read_context(store: &MetaShard) -> RootReadContext {
        RootReadContext::current(store, root(), placement(), owner()).unwrap()
    }

    fn fence_command(
        store: &MetaShard,
        request_fill: u8,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id: request(request_fill),
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

    fn ready_store(path_count: usize) -> (MetaShard, WorkspaceIncarnationId) {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(&store, 1, RootFenceAction::Install))
            .unwrap();
        store
            .execute(&fence_command(
                &store,
                2,
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
        let workspace_incarnation = incarnation(3);
        create_visible_workspace(
            &store,
            write_context(&store, 3),
            &workbench(),
            workspace_incarnation,
        )
        .unwrap();
        for (chunk_index, chunk) in (0..path_count).collect::<Vec<_>>().chunks(200).enumerate() {
            let context = write_context(&store, 4 + chunk_index as u8);
            let records = chunk
                .iter()
                .map(|index| {
                    let path = row_path(*index);
                    let entry = artifact_entry(*index);
                    (
                        path_current_key(root(), workspace_incarnation, &path),
                        entry.encode().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let predicates = records
                .iter()
                .map(|(key, _)| CommandPredicate::Value {
                    family: MetadataFamily::PathCurrent,
                    key: key.clone(),
                    expected: None,
                })
                .collect();
            let mutations = records
                .into_iter()
                .map(|(key, value)| CommandMutation::Put {
                    family: MetadataFamily::PathCurrent,
                    key,
                    value,
                })
                .collect();
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
                        request_id: context.request_id,
                        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                        read_version: context.read_version,
                        root_fence_action: RootFenceAction::RequireActive,
                        predicates,
                        mutations,
                        history_projection: Vec::new(),
                        event_projection: Vec::new(),
                        deterministic_result: b"generic-index-path-fixture".to_vec(),
                    }
                    .seal(),
                )
                .unwrap();
        }
        (store, workspace_incarnation)
    }

    fn artifact_entry(index: usize) -> PathEntry {
        PathEntry {
            generation: Generation::new(1).unwrap(),
            artifact_revision_id: revision(index + 1),
            body_digest_uri: format!("sha256:{:064x}", index + 1),
            manifest_digest_uri: format!("sha256:{:064x}", index + 2),
            logical_size: index as u64,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: None,
            manifest_id: None,
            typed_index_projection: TypedProjection::empty().encode().unwrap(),
        }
    }

    fn row_path(index: usize) -> NormalizedRelativePath {
        NormalizedRelativePath::new(format!("outputs/item-{index:04}.json")).unwrap()
    }

    fn row_input(index: usize, large: bool) -> GenericIndexRowInput {
        let values = if large {
            vec![QueryScalar::String("x".repeat(59 * 1_024))]
        } else {
            vec![
                QueryScalar::String(index.to_string()),
                QueryScalar::Unsigned(index as u64),
                QueryScalar::String(index.to_string()),
            ]
        };
        GenericIndexRowInput {
            relative_path: Some(row_path(index)),
            values: vec![GenericIndexFieldValues {
                field: field(),
                values,
            }],
        }
    }

    struct BeginFixture {
        request_fill: u8,
        operation_fill: u8,
        generation_fill: u8,
        declared_row_count: u64,
        expected_current_generation: Option<Generation>,
    }

    fn begin(
        service: GenericIndexRegistrationService<'_>,
        store: &MetaShard,
        workspace_incarnation: WorkspaceIncarnationId,
        fixture: BeginFixture,
    ) -> GenericIndexRegistrationOutcome {
        service
            .begin(BeginGenericIndexRegistrationRequest {
                context: write_context(store, fixture.request_fill),
                operation_id: operation(fixture.operation_fill),
                generation_id: generation(fixture.generation_fill),
                workbench_id: workbench(),
                expected_workspace_incarnation_id: workspace_incarnation,
                index_path: None,
                expected_current_generation: fixture.expected_current_generation,
                capabilities: vec![capability()],
                declared_row_count: fixture.declared_row_count,
            })
            .unwrap()
    }

    fn current_payload(
        store: &MetaShard,
        workspace_incarnation: WorkspaceIncarnationId,
    ) -> Option<Vec<u8>> {
        let context = read_context(store);
        store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::GenericIndexCurrent,
                &generic_index_current_key(root(), workspace_incarnation, None),
                context.read_version,
            )
            .unwrap()
    }

    #[test]
    fn zero_row_finalize_and_terminal_replay_preserve_exact_commit() {
        let (store, workspace_incarnation) = ready_store(0);
        let service = GenericIndexRegistrationService::new(&store);
        let begun = begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 20,
                operation_fill: 30,
                generation_fill: 40,
                declared_row_count: 0,
                expected_current_generation: None,
            },
        );
        assert_eq!(
            begun.operation.phase,
            GenericIndexRegistrationPhase::Appending
        );
        assert_eq!(
            begun.operation.last_transition_version,
            begun.commit_version
        );
        assert_eq!(current_payload(&store, workspace_incarnation), None);

        let finalize_request = FinalizeGenericIndexRegistrationRequest {
            context: write_context(&store, 21),
            operation_id: operation(30),
        };
        let finalized = service.finalize(finalize_request).unwrap();
        let response_loss_replay = service.finalize(finalize_request).unwrap();
        assert!(!finalized.replayed);
        assert!(response_loss_replay.replayed);
        assert_eq!(
            response_loss_replay.commit_version,
            finalized.commit_version
        );
        assert_eq!(response_loss_replay.operation, finalized.operation);

        let later_replay = service
            .finalize(FinalizeGenericIndexRegistrationRequest {
                context: write_context(&store, 22),
                operation_id: operation(30),
            })
            .unwrap();
        assert!(later_replay.replayed);
        assert_eq!(later_replay.commit_version, finalized.commit_version);
        assert_eq!(
            later_replay.operation.last_transition_version,
            finalized.commit_version
        );
        assert_eq!(
            service.get(read_context(&store), operation(30)).unwrap(),
            Some(finalized.operation)
        );
        let current = GenericIndexCurrentRecord::decode(
            &current_payload(&store, workspace_incarnation).unwrap(),
        )
        .unwrap();
        assert_eq!(current.row_count, 0);
        assert_eq!(current.pointer_generation, Generation::new(1).unwrap());
    }

    #[test]
    fn append_freezes_directory_unbound_and_artifact_binding_states() {
        let (store, workspace_incarnation) = ready_store(1);
        let service = GenericIndexRegistrationService::new(&store);
        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 20,
                operation_fill: 30,
                generation_fill: 40,
                declared_row_count: 4,
                expected_current_generation: None,
            },
        );
        let values = || {
            vec![GenericIndexFieldValues {
                field: field(),
                values: vec![QueryScalar::String("value".to_owned())],
            }]
        };
        service
            .append(AppendGenericIndexRowsRequest {
                context: write_context(&store, 21),
                operation_id: operation(30),
                first_sequence: 0,
                rows: vec![
                    GenericIndexRowInput {
                        relative_path: None,
                        values: values(),
                    },
                    GenericIndexRowInput {
                        relative_path: Some(NormalizedRelativePath::new("outputs").unwrap()),
                        values: values(),
                    },
                    GenericIndexRowInput {
                        relative_path: Some(
                            NormalizedRelativePath::new("outputs/absent.json").unwrap(),
                        ),
                        values: values(),
                    },
                    row_input(0, false),
                ],
            })
            .unwrap();

        let context = read_context(&store);
        let bindings = (0..4)
            .map(|sequence| {
                let payload = store
                    .read_at(
                        root(),
                        placement(),
                        owner(),
                        MetadataFamily::GenericIndexGeneration,
                        &generic_index_row_key(root(), generation(40), sequence),
                        context.read_version,
                    )
                    .unwrap()
                    .unwrap();
                GenericIndexRowRecord::decode(&payload).unwrap().binding
            })
            .collect::<Vec<_>>();
        assert_eq!(bindings[0], GenericIndexRowBinding::Directory);
        assert_eq!(bindings[1], GenericIndexRowBinding::Directory);
        assert_eq!(bindings[2], GenericIndexRowBinding::Unbound);
        assert!(matches!(bindings[3], GenericIndexRowBinding::Artifact(_)));
    }

    #[test]
    fn append_273_and_520_rows_page_and_replay_exact_historical_outcomes() {
        for count in [273_usize, 520] {
            let (store, workspace_incarnation) = ready_store(count);
            let service = GenericIndexRegistrationService::new(&store);
            let operation_fill = if count == 273 { 31 } else { 32 };
            begin(
                service,
                &store,
                workspace_incarnation,
                BeginFixture {
                    request_fill: 20,
                    operation_fill,
                    generation_fill: 41,
                    declared_row_count: count as u64,
                    expected_current_generation: None,
                },
            );
            let rows = (0..count)
                .map(|index| row_input(index, false))
                .collect::<Vec<_>>();
            let mut first = 0_usize;
            let mut request_fill = 21_u8;
            let mut first_outcome = None;
            while first < rows.len() {
                let end = (first + MAX_GENERIC_INDEX_APPEND_BATCH_ROWS).min(rows.len());
                let append_request = AppendGenericIndexRowsRequest {
                    context: write_context(&store, request_fill),
                    operation_id: operation(operation_fill),
                    first_sequence: first as u64,
                    rows: rows[first..end].to_vec(),
                };
                let outcome = service.append(append_request.clone()).unwrap();
                assert_eq!(
                    outcome.receipt.commit_version,
                    outcome.command.commit_version
                );
                if first == 0 {
                    let response_loss_replay = service.append(append_request).unwrap();
                    assert!(response_loss_replay.command.replayed);
                    assert_eq!(response_loss_replay.receipt, outcome.receipt);
                    assert_eq!(
                        response_loss_replay.command.commit_version,
                        outcome.command.commit_version
                    );
                    assert_eq!(
                        response_loss_replay.command.operation,
                        outcome.command.operation
                    );
                    first_outcome = Some(outcome.clone());
                }
                first = end;
                request_fill += 1;
            }

            let historical = service
                .append(AppendGenericIndexRowsRequest {
                    context: write_context(&store, request_fill),
                    operation_id: operation(operation_fill),
                    first_sequence: 0,
                    rows: rows[..MAX_GENERIC_INDEX_APPEND_BATCH_ROWS.min(count)].to_vec(),
                })
                .unwrap();
            let first_outcome = first_outcome.unwrap();
            assert!(historical.command.replayed);
            assert_eq!(
                historical.command.commit_version,
                first_outcome.command.commit_version
            );
            assert_eq!(
                historical.command.operation,
                first_outcome.command.operation
            );

            if count == 273 {
                let mut cleanup_calls = 0;
                loop {
                    request_fill += 1;
                    let outcome = service
                        .abort(AbortGenericIndexRegistrationRequest {
                            context: write_context(&store, request_fill),
                            operation_id: operation(operation_fill),
                            limit: MAX_GENERIC_INDEX_CLEANUP_BATCH_ROWS,
                        })
                        .unwrap();
                    assert!(outcome.removed_rows + outcome.removed_receipts <= 120);
                    cleanup_calls += 1;
                    if outcome.cleanup_complete {
                        assert_eq!(
                            outcome.command.operation.phase,
                            GenericIndexRegistrationPhase::Cleaned
                        );
                        break;
                    }
                }
                assert!(cleanup_calls >= 3);
                request_fill += 1;
                let after_abort = service
                    .append(AppendGenericIndexRowsRequest {
                        context: write_context(&store, request_fill),
                        operation_id: operation(operation_fill),
                        first_sequence: 0,
                        rows: rows[..MAX_GENERIC_INDEX_APPEND_BATCH_ROWS].to_vec(),
                    })
                    .unwrap();
                assert_eq!(
                    after_abort.command.commit_version,
                    first_outcome.command.commit_version
                );
                assert_eq!(
                    after_abort.command.operation,
                    first_outcome.command.operation
                );
                assert_eq!(current_payload(&store, workspace_incarnation), None);
            } else {
                request_fill += 1;
                let finalized = service
                    .finalize(FinalizeGenericIndexRegistrationRequest {
                        context: write_context(&store, request_fill),
                        operation_id: operation(operation_fill),
                    })
                    .unwrap();
                request_fill += 1;
                let after_finalize = service
                    .append(AppendGenericIndexRowsRequest {
                        context: write_context(&store, request_fill),
                        operation_id: operation(operation_fill),
                        first_sequence: 0,
                        rows: rows[..MAX_GENERIC_INDEX_APPEND_BATCH_ROWS].to_vec(),
                    })
                    .unwrap();
                assert_eq!(
                    after_finalize.command.commit_version,
                    first_outcome.command.commit_version
                );
                assert_eq!(
                    after_finalize.command.operation.phase,
                    GenericIndexRegistrationPhase::Appending
                );
                assert_eq!(finalized.operation.appended_row_count, 520);
                let mut mismatched = rows[..MAX_GENERIC_INDEX_APPEND_BATCH_ROWS].to_vec();
                mismatched[0].values[0].values[0] = QueryScalar::String("different".to_owned());
                request_fill += 1;
                assert!(matches!(
                    service.append(AppendGenericIndexRowsRequest {
                        context: write_context(&store, request_fill),
                        operation_id: operation(operation_fill),
                        first_sequence: 0,
                        rows: mismatched,
                    }),
                    Err(GenericIndexError::OperationInputMismatch)
                ));
            }
        }
    }

    #[test]
    fn stale_concurrent_finalize_cannot_replace_a_newer_pointer() {
        let (store, workspace_incarnation) = ready_store(0);
        let service = GenericIndexRegistrationService::new(&store);
        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 20,
                operation_fill: 30,
                generation_fill: 40,
                declared_row_count: 0,
                expected_current_generation: None,
            },
        );
        service
            .finalize(FinalizeGenericIndexRegistrationRequest {
                context: write_context(&store, 21),
                operation_id: operation(30),
            })
            .unwrap();

        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 22,
                operation_fill: 31,
                generation_fill: 41,
                declared_row_count: 0,
                expected_current_generation: Some(Generation::new(1).unwrap()),
            },
        );
        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 23,
                operation_fill: 32,
                generation_fill: 42,
                declared_row_count: 0,
                expected_current_generation: Some(Generation::new(1).unwrap()),
            },
        );
        service
            .finalize(FinalizeGenericIndexRegistrationRequest {
                context: write_context(&store, 24),
                operation_id: operation(31),
            })
            .unwrap();
        let current_after_winner = current_payload(&store, workspace_incarnation).unwrap();
        assert!(matches!(
            service.finalize(FinalizeGenericIndexRegistrationRequest {
                context: write_context(&store, 25),
                operation_id: operation(32),
            }),
            Err(GenericIndexError::CurrentPointerConflict)
        ));
        assert_eq!(
            current_payload(&store, workspace_incarnation),
            Some(current_after_winner)
        );
    }

    #[test]
    fn abort_rejects_a_malformed_generation_row_key_before_cleanup() {
        let (store, workspace_incarnation) = ready_store(1);
        let service = GenericIndexRegistrationService::new(&store);
        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 20,
                operation_fill: 30,
                generation_fill: 40,
                declared_row_count: 1,
                expected_current_generation: None,
            },
        );
        service
            .append(AppendGenericIndexRowsRequest {
                context: write_context(&store, 21),
                operation_id: operation(30),
                first_sequence: 0,
                rows: vec![row_input(0, false)],
            })
            .unwrap();
        let mut malformed_key = generic_index_row_prefix(root(), generation(40));
        malformed_key.push(0xff);
        let context = write_context(&store, 22);
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES])),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: context.request_id,
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: context.read_version,
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates: vec![CommandPredicate::Value {
                        family: MetadataFamily::GenericIndexGeneration,
                        key: malformed_key.clone(),
                        expected: None,
                    }],
                    mutations: vec![CommandMutation::Put {
                        family: MetadataFamily::GenericIndexGeneration,
                        key: malformed_key,
                        value: b"malformed-row".to_vec(),
                    }],
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: b"malformed-row-fixture".to_vec(),
                }
                .seal(),
            )
            .unwrap();
        assert!(matches!(
            service.abort(AbortGenericIndexRegistrationRequest {
                context: write_context(&store, 23),
                operation_id: operation(30),
                limit: MAX_GENERIC_INDEX_CLEANUP_BATCH_ROWS,
            }),
            Err(GenericIndexError::CorruptKey {
                family: "GenericIndexGeneration(row)"
            })
        ));
    }

    #[test]
    fn oversized_append_is_preapply_and_abort_preserves_old_current_bytes() {
        let (store, workspace_incarnation) = ready_store(240);
        let service = GenericIndexRegistrationService::new(&store);
        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 20,
                operation_fill: 30,
                generation_fill: 40,
                declared_row_count: 0,
                expected_current_generation: None,
            },
        );
        service
            .finalize(FinalizeGenericIndexRegistrationRequest {
                context: write_context(&store, 21),
                operation_id: operation(30),
            })
            .unwrap();
        let old_current = current_payload(&store, workspace_incarnation).unwrap();

        begin(
            service,
            &store,
            workspace_incarnation,
            BeginFixture {
                request_fill: 22,
                operation_fill: 31,
                generation_fill: 41,
                declared_row_count: 240,
                expected_current_generation: Some(Generation::new(1).unwrap()),
            },
        );
        let large_rows = (0..240)
            .map(|index| row_input(index, true))
            .collect::<Vec<_>>();
        assert!(matches!(
            service.append(AppendGenericIndexRowsRequest {
                context: write_context(&store, 23),
                operation_id: operation(31),
                first_sequence: 0,
                rows: large_rows,
            }),
            Err(GenericIndexError::ResourceExhausted { .. })
        ));
        assert_eq!(
            current_payload(&store, workspace_incarnation),
            Some(old_current.clone())
        );

        let abort_request = AbortGenericIndexRegistrationRequest {
            context: write_context(&store, 24),
            operation_id: operation(31),
            limit: MAX_GENERIC_INDEX_CLEANUP_BATCH_ROWS,
        };
        let aborted = service.abort(abort_request).unwrap();
        let response_loss_replay = service.abort(abort_request).unwrap();
        assert!(aborted.cleanup_complete);
        assert_eq!(
            aborted.command.operation.phase,
            GenericIndexRegistrationPhase::Cleaned
        );
        assert!(response_loss_replay.command.replayed);
        assert_eq!(
            response_loss_replay.command.commit_version,
            aborted.command.commit_version
        );
        assert_eq!(
            response_loss_replay.command.operation,
            aborted.command.operation
        );
        assert_eq!(
            current_payload(&store, workspace_incarnation),
            Some(old_current)
        );
    }
}
