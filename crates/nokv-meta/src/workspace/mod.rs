//! Authoritative NoKV workspace metadata schema and transaction-store binding.
//!
//! This is the only supported durable namespace layout.

mod build_commit_records;
mod codec;
mod commit;
mod commit_closure;
mod commit_records;
mod engine;
mod event_projection;
mod gc;
mod gc_records;
mod generic_index;
mod generic_index_records;
mod keyspace;
mod namespace;
mod publication;
mod publication_records;
mod publish_operation_records;
mod query;
mod query_records;
#[cfg(feature = "metadata-read-stats")]
mod read_stats;
mod records;
mod recovery;
mod remove;
mod rename;
mod restore;
mod restore_records;
mod secondary_index;
mod snapshot;
mod snapshot_query;
mod snapshot_records;
#[cfg(test)]
pub(crate) mod test_support;

pub use build_commit_records::{
    BuildCommitOperationRecord, BuildCommitResult, CommitManifestBinding, CommitManifestCondition,
    CommitOperationErrorKind, CommitOperationRecordError, CommitOperationTerminalError,
    CommitRetireOperationRecord, BUILD_COMMIT_OPERATION_VALUE_FORMAT_VERSION,
    COMMIT_RETIRE_OPERATION_VALUE_FORMAT_VERSION, MAX_COMMIT_OPERATION_ERROR_BYTES,
};
pub use codec::{
    artifact_manifest_key, artifact_manifest_prefix, artifact_revision_claim_key,
    artifact_revision_key, build_commit_history_hold_key, child_commit_consumer_key,
    commit_generic_index_member_key, commit_generic_index_member_prefix, commit_key,
    commit_member_key, commit_member_prefix, commit_prefix, commit_revision_ref_key,
    commit_revision_ref_prefix, decode_artifact_manifest_key,
    decode_commit_generic_index_member_key, decode_commit_key, decode_commit_member_key,
    decode_gc_candidate_key, decode_generic_index_append_receipt_key,
    decode_generic_index_current_key, decode_generic_index_generation_key,
    decode_generic_index_generation_ref_key, decode_generic_index_row_key, decode_operation_key,
    decode_path_current_key, decode_revision_dependency_ref_key, decode_snapshot_ref_key,
    decode_workspace_current_key, gc_candidate_key, gc_candidate_prefix, gc_history_barrier_key,
    generic_index_append_receipt_key, generic_index_append_receipt_prefix,
    generic_index_current_key, generic_index_current_prefix, generic_index_generation_key,
    generic_index_generation_prefix, generic_index_generation_ref_key,
    generic_index_generation_ref_prefix, generic_index_row_key, generic_index_row_prefix,
    history_hold_prefix, lease_commit_consumer_key, object_block_key, operation_key,
    operation_prefix, path_child_prefix, path_current_key, path_revision_ref_key,
    register_generic_index_history_hold_key, restore_history_hold_key, restore_member_key,
    restore_member_prefix, revision_dependency_ref_key, revision_dependency_ref_prefix,
    snapshot_alias_key, snapshot_commit_consumer_key, snapshot_history_hold_key,
    snapshot_id_claim_key, snapshot_ref_key, snapshot_ref_prefix, staged_object_key,
    staged_object_prefix, tag_commit_consumer_key, tag_key, workbench_commit_head_key,
    workbench_head_commit_consumer_key, workspace_current_key, workspace_current_prefix,
    PATH_COMPONENT_DELIMITER, PATH_EXACT_TERMINATOR, SCHEMA_ID, VALUE_FORMAT_VERSION,
    WORKSPACE_FORMAT_VERSION,
};
pub use commit::{
    AbortBuildCommitRequest, BeginBuildCommitRequest, BeginCommitRetirementRequest,
    BuildCommitOutcome, BuildCommitStepRequest, CommitError, CommitService, DeleteCommitTagRequest,
    RetireCommitOutcome, SetCommitTagRequest, TagMutationOutcome, MAX_COMMIT_MEMBER_BATCH_ROWS,
    MAX_COMMIT_PARENT_BATCH_ROWS, MAX_COMMIT_RETIRE_MEMBER_BATCH_ROWS,
    MAX_COMMIT_REVISION_BATCH_ROWS,
};
pub use commit_closure::{
    advance_commit_parent_rolling_digest, advance_commit_revision_rolling_digest,
    plan_commit_member, plan_commit_parent, plan_commit_revision, verify_commit_closure_seal,
    CommitClosureError, CommitMemberClosureStep, CommitParentClosureStep,
    CommitRevisionClosureStep,
};
pub use commit_records::{
    advance_commit_member_rolling_digest, commit_member_row_digest, CommitConsumerRecord,
    CommitMemberRecord, CommitRecord, CommitRecordError, TagRecord, WorkbenchCommitHeadRecord,
    COMMIT_AUXILIARY_VALUE_FORMAT_VERSION, COMMIT_RECORD_VALUE_FORMAT_VERSION,
    MAX_COMMIT_DIGEST_URI_BYTES, MAX_COMMIT_LINEAGE_BYTES, MAX_COMMIT_MEMBER_PROJECTION_BYTES,
    MAX_COMMIT_PRODUCER_BYTES, MAX_PARENT_COMMITS,
};
#[cfg(feature = "metadata-read-stats")]
pub use engine::MetadataReadStatsSession;
pub use engine::{
    store_limits, CommandMutation, CommandPredicate, EventProjection, HistoryProjection, MetaError,
    MetaShard, MetadataCommand, MetadataCommandResult, MetadataScanItem, RecoveryFsckReport,
    RootFenceAction,
};
pub use gc::{
    gc_operation_id, generic_index_gc_operation_id, AdvanceGcDeletionBatchRequest,
    BeginGcDeletionRequest, ClaimGcRequest, ClaimGenericIndexGenerationGcRequest,
    ClearStaleGcCandidateRequest, CollectGenericIndexGenerationGcBatchRequest, CompleteGcRequest,
    GcCandidateClearOutcome, GcCandidateCursor, GcCandidateEntry, GcCandidatePage,
    GcCommandOutcome, GcError, GcHistoryBarrierOutcome, GcManifestBatch, GcManifestEntry,
    GcObjectAbsence, GcService, GenericIndexGenerationGcCandidate,
    GenericIndexGenerationGcCandidateCursor, GenericIndexGenerationGcCandidatePage,
    GenericIndexGenerationGcOutcome, QuarantineGcRequest, MAX_GC_BATCH_ROWS,
    MAX_GC_CANDIDATE_PAGE_SIZE, MAX_GENERIC_INDEX_GC_BATCH_ROWS,
};
pub use gc_records::{
    GcHistoryBarrierRecord, GcOperationRecord, GcRecordError, GcTransition,
    GenericIndexGcOperationRecord, GenericIndexGcPhase, GC_VALUE_FORMAT_VERSION,
    MAX_GC_EVIDENCE_BYTES,
};
pub use generic_index::{
    AbortGenericIndexRegistrationOutcome, AbortGenericIndexRegistrationRequest,
    AppendGenericIndexRowsOutcome, AppendGenericIndexRowsRequest,
    BeginGenericIndexRegistrationRequest, FinalizeGenericIndexRegistrationRequest,
    GenericIndexError, GenericIndexRegistrationOutcome, GenericIndexRegistrationService,
    GenericIndexRowInput, MAX_GENERIC_INDEX_APPEND_BATCH_ROWS,
    MAX_GENERIC_INDEX_CLEANUP_BATCH_ROWS,
};
pub use generic_index_records::{
    advance_commit_generic_index_member_rolling_digest, advance_generic_index_row_rolling_digest,
    commit_generic_index_member_row_digest, empty_generic_index_row_digest,
    generic_index_append_batch_digest, generic_index_append_input_digest,
    generic_index_build_commit_owner_digest, generic_index_capability_digest,
    generic_index_commit_owner_digest, generic_index_current_owner_digest,
    generic_index_registration_owner_digest, generic_index_restore_owner_digest,
    generic_index_row_digest, verify_generic_index_generation_seal, CommitGenericIndexMemberRecord,
    GenericIndexAppendReceiptRecord, GenericIndexArtifactBinding, GenericIndexCurrentRecord,
    GenericIndexFieldCapability, GenericIndexFieldValues, GenericIndexGenerationRecord,
    GenericIndexGenerationRefRecord, GenericIndexOperator, GenericIndexRecordError,
    GenericIndexRegistrationOperationRecord, GenericIndexRowBinding, GenericIndexRowRecord,
    GENERIC_INDEX_VALUE_FORMAT_VERSION, MAX_GENERIC_INDEX_APPEND_ROWS, MAX_GENERIC_INDEX_FIELDS,
    MAX_GENERIC_INDEX_ROW_BYTES, MAX_GENERIC_INDEX_ROW_FIELDS,
    MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES, MAX_GENERIC_INDEX_VALUES_PER_FIELD,
};
pub use keyspace::{keyspaces, KeyspaceDef, MetadataFamily};
pub use namespace::{
    create_visible_workspace, get_current_visible_workspace_path, get_path_at_visible_workspace,
    get_visible_path_at, get_visible_workspace_at, get_visible_workspace_path_at,
    list_paths_at_visible_workspace, CreateVisibleWorkspaceResult, NamespaceError, RootReadContext,
    RootWriteContext, VisiblePathChild, VisiblePathListPage, VisibleWorkspacePathRead,
    MAX_VISIBLE_PATH_LIST_PAGE_SIZE,
};
pub use publication::{
    advance_manifest_rolling_digest, advance_staged_object_rolling_digest, dependency_owner_digest,
    manifest_rows_digest, seal_publish_operation, staged_object_ledger_digest, BeginPublishRequest,
    CleanupPublishBatchRequest, FinalizePublishOutcome, FinalizePublishRequest,
    FinishReconcileQuarantinedPublishRequest, HeartbeatPublishRequest, ManifestRowInput,
    MarkObjectsUploadedBatchRequest, PublicationContext, PublicationError, PublicationService,
    PublishCommandOutcome, PublishedArtifact, QuarantineReconcileResolution,
    ReconcileQuarantinedPublishBatchRequest, StageManifestBatchRequest, StageObjectsBatchRequest,
    StagedObjectUpdate, TakeOverOrphanedPublishRequest, TransitionPublishRequest,
    MAX_PUBLICATION_BATCH_ROWS,
};
pub use publication_records::{
    ArtifactRevisionClaimRecord, ArtifactRevisionRecord, GcCandidateRecord, PathEntry,
    PublicationRecordCodecError, RevisionRefRecord, WorkspaceIncarnationClaimRecord,
    WorkspaceRecord, MAX_CONTENT_TYPE_BYTES, MAX_DEPENDENCY_COUNT as MAX_REVISION_DEPENDENCIES,
    MAX_DEPENDENCY_DEPTH as MAX_REVISION_DEPENDENCY_DEPTH, MAX_DIGEST_URI_BYTES,
    MAX_INDEX_PROJECTION_BYTES, MAX_MANIFEST_ID_BYTES, MAX_PRODUCER_BYTES,
    MAX_QUARANTINE_EVIDENCE_BYTES, PUBLICATION_VALUE_FORMAT_VERSION,
};
pub use publish_operation_records::{
    AppendSegment, ArtifactManifestRow, ManifestPosition, PublishAuthority, PublishClaim,
    PublishOperationRecord, PublishRecordError, PublishResult, PublishTerminalError,
    PublishTerminalErrorKind, PublishTransition, StagedObjectRecord, MAX_APPEND_SEGMENTS,
    MAX_MANIFEST_ROWS, MAX_MULTIPART_ID_BYTES, MAX_STAGED_OBJECTS, MAX_TERMINAL_ERROR_BYTES,
    PUBLISH_VALUE_FORMAT_VERSION,
};
pub use query::{
    aggregate_paths_at, catalog_fields_at, find_workspaces_at, get_workspace_at, read_changes_at,
    search_paths_at, AggregateFunction, AggregateGroup, AggregatePage, AggregateRequest,
    AggregateSpec, CatalogField, CatalogPage, CatalogPathMatch, CatalogRequest, ChangeEvent,
    ChangePage, ChangePageRequest, CommittedFilter, FacetBucket, FacetResult, FindWorkspacesPage,
    FindWorkspacesRequest, GenericArtifactMetadata, GenericNamespaceHit, GenericNamespaceKind,
    PresentationPathRoot, QueryError, QueryOperand, QueryOperator, QueryPredicate, QueryProfile,
    QueryScope, QuerySort, QuerySortDirection, SearchHit, SearchPage, SearchRequest,
    WorkspaceDiscovery, MAX_FACET_BUCKETS_PER_FIELD, MAX_QUERY_AGGREGATES, MAX_QUERY_CURSOR_BYTES,
    MAX_QUERY_FACET_FIELDS, MAX_QUERY_GROUP_FIELDS, MAX_QUERY_IN_VALUES, MAX_QUERY_PAGE_SIZE,
    MAX_QUERY_PREDICATES, MAX_QUERY_PROJECTION_FIELDS, MAX_QUERY_SORT_FIELDS,
};
pub use query_records::{
    decode_path_index_locator_key, decode_secondary_index_key, decode_secondary_index_row_key,
    encode_ordered_index_scalar, path_index_digest, path_index_generation, path_index_locator_key,
    secondary_index_field_prefix, secondary_index_key, secondary_index_value_prefix,
    ChangeEventKind, ChangeEventRecord, FiniteFloat, PathIndexLocatorRecord, PathIndexLocatorState,
    QueryFieldId, QueryRecordError, QueryScalar, QueryScalarType, SecondaryIndexRecord,
    TypedProjection, CHANGE_EVENT_VALUE_FORMAT_VERSION, MAX_QUERY_FIELD_ID_BYTES,
    MAX_QUERY_SCALAR_BYTES, MAX_TYPED_PROJECTION_BYTES, MAX_TYPED_PROJECTION_FIELDS,
    PATH_INDEX_LOCATOR_VALUE_FORMAT_VERSION, QUERY_RECORD_VALUE_FORMAT_VERSION,
    SECONDARY_INDEX_VALUE_FORMAT_VERSION,
};
#[cfg(feature = "metadata-read-stats")]
pub use read_stats::{MetadataReadStats, MetadataReadStatsSessionError};
pub use records::{
    CommandDedupeRecord, CurrentValue, HistoryValue, LocalRecoveryReceipt, RecordCodecError,
    RootFence,
};
pub use recovery::{
    RecoveryCodecError, RecoveryMutationV1, RecoveryOutboxRecord, RecoveryOutboxSegment,
    RecoveryResultV1, RecoveryState, MAX_RECOVERY_SEGMENT_BYTES, MAX_RECOVERY_SEGMENT_RECORDS,
    RECOVERY_CHAIN_DIGEST_BYTES, RECOVERY_OUTBOX_VALUE_FORMAT_VERSION,
};
pub use remove::{remove_path, RemovePathError, RemovePathOutcome, RemovePathRequest};
pub use rename::{rename_path, RenamePathError, RenamePathOutcome, RenamePathRequest};
pub use restore::{
    abort_restore, apply_restore_initialization, begin_restore, bind_restore_destination,
    build_restore_commit_members, cleanup_restore_batch, complete_restore, copy_restore_batch,
    finish_restore_cleanup, get_restore, read_restore_source_run_manifest, restore_operation_id,
    seal_restore_commit_revisions, seal_restore_source, start_restore_cleanup, start_restore_copy,
    AbortRestoreRequest, BeginRestoreRequest, BindRestoreDestinationRequest,
    BuildRestoreCommitBatchOutcome, CompleteRestoreOutcome, CopyRestoreBatchOutcome,
    CopyRestoreBatchRequest, RestoreClosureBatchRequest, RestoreCommandOutcome, RestoreError,
    RestoreInitialization, RestoreOperationRequest, RestoreSourceRunManifest,
    RestoreSourceSelector, SealRestoreCommitBatchOutcome, MAX_RESTORE_BATCH_MEMBERS,
    RESTORE_MANIFEST_PATH,
};
pub use restore_records::{
    RestoreCommitClosureProgress, RestoreCommitProvenance, RestoreCommitProvenanceV5,
    RestoreDestinationBinding, RestoreDestinationCommitReceipt, RestoreDestinationManifests,
    RestoreManifestDescriptor, RestoreManifestIdentity, RestoreManifestPublication,
    RestoreMemberRecord, RestoreOperationRecord, RestoreRecordError, RestoreResult, RestoreSource,
    RestoreSourceCommitSeal, RestoreTerminalError, RestoreTerminalErrorKind, RestoreTransition,
    MAX_RESTORE_MANIFEST_BYTES, MAX_RESTORE_MEMBERS, MAX_RESTORE_TERMINAL_ERROR_BYTES,
    RESTORE_MANIFEST_CONTENT_TYPE, RESTORE_MEMBER_VALUE_FORMAT_VERSION,
    RESTORE_OPERATION_VALUE_FORMAT_VERSION,
};
pub use secondary_index::{
    cleanup_secondary_index_page, CleanupSecondaryIndexPageOutcome,
    CleanupSecondaryIndexPageRequest, SecondaryIndexCleanupCursor, SecondaryIndexCleanupError,
    SecondaryIndexCleanupPhase, MAX_SECONDARY_INDEX_CLEANUP_PAGE_SIZE,
};
pub use snapshot::{
    attach_snapshot_consumer, claim_expired_snapshot, finish_snapshot_reap, get_snapshot_at,
    mint_snapshot, release_snapshot_consumer, renew_snapshot, retire_snapshot,
    AttachSnapshotConsumerRequest, ClaimExpiredSnapshotRequest, FinishSnapshotReapRequest,
    MintSnapshotRequest, ReleaseSnapshotConsumerRequest, RenewSnapshotRequest, ResolvedSnapshot,
    RetireSnapshotRequest, SnapshotError, SnapshotSelector, SnapshotWriteOutcome,
};
pub use snapshot_query::{
    list_snapshots_at, SnapshotListError, SnapshotListPage, MAX_SNAPSHOT_PAGE_SIZE,
};
pub use snapshot_records::{
    HistoryHoldRecord, SnapshotAliasRecord, SnapshotRecordError, SnapshotRefRecord,
    MAX_SNAPSHOT_ANNOTATION_BYTES, MAX_SNAPSHOT_RETIRE_ANNOTATION_BYTES,
    SNAPSHOT_VALUE_FORMAT_VERSION,
};
