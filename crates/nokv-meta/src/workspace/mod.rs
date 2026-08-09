//! Authoritative NoKV workspace metadata schema and provider facade.
//!
//! This is the only supported durable namespace layout.

mod authority;
mod build_commit_records;
mod codec;
mod commit;
mod commit_receipt;
mod commit_records;
mod commit_recovery_fence;
mod engine;
mod event_projection;
mod fsck;
mod gc;
mod gc_records;
mod namespace;
pub(crate) mod provider;
mod provider_catalog;
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
mod restore;
mod restore_records;
mod snapshot;
mod snapshot_query;
mod snapshot_records;

pub use authority::{
    workspace_metadata_contract_digest, AcknowledgedMetadataFrontier, MetadataAuthorityState,
    MetadataStoreIdentity,
};
pub use build_commit_records::{
    BuildCommitOperationRecord, BuildCommitResult, CommitManifestBinding, CommitManifestCondition,
    CommitOperationErrorKind, CommitOperationRecordError, CommitOperationTerminalError,
    CommitRetireOperationRecord, COMMIT_OPERATION_VALUE_FORMAT_VERSION,
    MAX_COMMIT_OPERATION_ERROR_BYTES,
};
pub use codec::{
    artifact_manifest_key, artifact_manifest_prefix, artifact_revision_key,
    build_commit_history_hold_key, child_commit_consumer_key, commit_key, commit_member_key,
    commit_member_prefix, commit_prefix, commit_revision_ref_key, commit_revision_ref_prefix,
    decode_artifact_manifest_key, decode_commit_key, decode_commit_member_key,
    decode_gc_candidate_key, decode_operation_key, decode_path_current_key,
    decode_revision_dependency_ref_key, decode_snapshot_ref_key, decode_workspace_current_key,
    gc_candidate_key, gc_candidate_prefix, gc_history_barrier_key, history_hold_prefix,
    lease_commit_consumer_key, object_block_key, operation_key, operation_prefix,
    path_child_prefix, path_current_key, path_revision_ref_key, restore_history_hold_key,
    restore_member_key, restore_member_prefix, revision_dependency_ref_key,
    revision_dependency_ref_prefix, snapshot_alias_key, snapshot_history_hold_key,
    snapshot_id_claim_key, snapshot_ref_key, snapshot_ref_prefix, staged_object_key,
    staged_object_prefix, tag_commit_consumer_key, tag_key, workbench_commit_head_key,
    workbench_head_commit_consumer_key, workspace_current_key, workspace_current_prefix,
    ARTIFACT_MANIFEST_TREE, ARTIFACT_REVISION_TREE, CHANGE_EVENT_TREE, COMMAND_DEDUPE_TREE,
    COMMIT_CONSUMER_TREE, COMMIT_MEMBER_TREE, COMMIT_TREE, GC_BARRIER_TREE, GC_CANDIDATE_TREE,
    HISTORY_HOLD_TREE, HISTORY_TREE, OPERATION_TREE, PATH_COMPONENT_DELIMITER, PATH_CURRENT_TREE,
    PATH_EXACT_TERMINATOR, RECOVERY_OUTBOX_TREE, RESTORE_MEMBER_TREE, REVISION_REF_TREE,
    ROOT_FENCE_TREE, SCHEMA_ID, SECONDARY_INDEX_TREE, SNAPSHOT_ALIAS_TREE, SNAPSHOT_REF_TREE,
    STAGED_OBJECT_TREE, SYSTEM_TREE, TAG_TREE, VALUE_FORMAT_VERSION, WORKBENCH_COMMIT_HEAD_TREE,
    WORKSPACE_CURRENT_TREE,
};
pub use commit::{
    advance_commit_parent_rolling_digest, advance_commit_revision_rolling_digest,
    AbortBuildCommitRequest, BeginBuildCommitRequest, BeginCommitRetirementRequest,
    BuildCommitOutcome, BuildCommitStepRequest, CommitError, CommitService, DeleteCommitTagRequest,
    RetireCommitOutcome, SetCommitTagRequest, TagMutationOutcome, MAX_COMMIT_MEMBER_BATCH_ROWS,
    MAX_COMMIT_PARENT_BATCH_ROWS, MAX_COMMIT_RETIRE_MEMBER_BATCH_ROWS,
    MAX_COMMIT_REVISION_BATCH_ROWS,
};
pub use commit_receipt::{
    ClaimedMetadataCommitReceiptPersistCommandV1, ClaimedMetadataCommitReceiptPoisonCommandV1,
    ClaimedMetadataCommitReceiptResolveCommandV1, MetadataAuthorityCommitActionV1,
    MetadataCommandCommitClassV1, MetadataCommitPurposeV1, MetadataCommitReceiptDirtySourceV1,
    MetadataCommitReceiptErrorV1, MetadataCommitReceiptMutationBackendResultV1,
    MetadataCommitReceiptMutationNotDispatchedV1, MetadataCommitReceiptPersistBackendResultV1,
    MetadataCommitReceiptPersistCommandV1, MetadataCommitReceiptPersistErrorV1,
    MetadataCommitReceiptPersistNotDispatchedV1, MetadataCommitReceiptPersistOutcomeV1,
    MetadataCommitReceiptPoisonCommandV1, MetadataCommitReceiptPoisonOutcomeV1,
    MetadataCommitReceiptPoisonReasonV1, MetadataCommitReceiptQualificationV1,
    MetadataCommitReceiptResolveCommandV1, MetadataCommitReceiptResolveOutcomeV1,
    MetadataCommitReceiptStateV1, MetadataCommitReceiptStoreV1, MetadataCommitResolutionBasisV1,
    MetadataCommitResolutionV1, MetadataFrontierPointV1, MetadataRuntimeCommitBundleV1,
    PlannedMetadataCommitV1,
};
pub use commit_records::{
    advance_commit_member_rolling_digest, commit_member_row_digest, CommitConsumerRecord,
    CommitMemberRecord, CommitRecord, CommitRecordError, TagRecord, WorkbenchCommitHeadRecord,
    COMMIT_VALUE_FORMAT_VERSION, MAX_COMMIT_DIGEST_URI_BYTES, MAX_COMMIT_LINEAGE_BYTES,
    MAX_COMMIT_MEMBER_PROJECTION_BYTES, MAX_COMMIT_PRODUCER_BYTES, MAX_PARENT_COMMITS,
};
pub use commit_recovery_fence::{
    ClaimedMetadataPendingRecoveryOpenCommandV1, MetadataCommitRecoveryFenceFactoryV1,
    MetadataOldDispatchExcludedProviderV1, MetadataOldDispatchExclusionInstallationV1,
    MetadataPendingRecoveryOpenBackendResultV1, MetadataPendingRecoveryOpenCommandV1,
    MetadataPendingRecoveryOpenErrorV1, MetadataPendingRecoveryOpenNotDispatchedV1,
    MetadataPendingRecoveryOpenOutcomeV1, MetadataPendingRecoveryOpenWitnessV1,
};
pub(crate) use engine::canonical_provider_requirement_values;
#[cfg(feature = "metadata-read-stats")]
pub use engine::MetadataReadStatsSession;
pub use engine::{
    canonical_provider_schema_v1, AgentMetadataError, AgentMetadataStore, CommandMutation,
    CommandPredicate, EventProjection, HistoryProjection, MetadataCommand, MetadataCommandResult,
    MetadataFamily, MetadataScanItem, MetadataStoreCreateModeV1, RootFenceAction,
};
pub use fsck::{
    run_metadata_fsck, MetadataFsckFamilyCoverage, MetadataFsckFinding, MetadataFsckFindingKind,
    MetadataFsckLimits, MetadataFsckReport, MetadataFsckRequest, MetadataFsckStatus,
    METADATA_FSCK_REPORT_SCHEMA_VERSION,
};
pub use gc::{
    gc_operation_id, AdvanceGcDeletionBatchRequest, BeginGcDeletionRequest, ClaimGcRequest,
    ClearStaleGcCandidateRequest, CompleteGcRequest, GcCandidateClearOutcome, GcCandidateCursor,
    GcCandidateEntry, GcCandidatePage, GcCommandOutcome, GcError, GcHistoryBarrierOutcome,
    GcManifestBatch, GcManifestEntry, GcObjectAbsence, GcService, QuarantineGcRequest,
    MAX_GC_BATCH_ROWS, MAX_GC_CANDIDATE_PAGE_SIZE,
};
pub use gc_records::{
    GcHistoryBarrierRecord, GcOperationRecord, GcRecordError, GcTransition,
    GC_VALUE_FORMAT_VERSION, MAX_GC_EVIDENCE_BYTES,
};
pub use namespace::{
    create_visible_workspace, get_current_visible_workspace_path, get_path_at_visible_workspace,
    get_visible_path_at, get_visible_workspace_at, get_visible_workspace_path_at,
    list_paths_at_visible_workspace, CreateVisibleWorkspaceResult, NamespaceError, RootReadContext,
    RootWriteContext, VisiblePathChild, VisiblePathListPage, VisibleWorkspacePathRead,
    MAX_VISIBLE_PATH_LIST_PAGE_SIZE,
};
pub use nokv_types::{
    MetadataMigrationTargetBinding, MetadataRecoveryFrontier, SourceQuiesceReceipt,
    TargetActivationToken,
};
pub use publication::{
    advance_manifest_rolling_digest, advance_staged_object_rolling_digest, dependency_owner_digest,
    manifest_rows_digest, seal_publish_operation, staged_object_ledger_digest, BeginPublishRequest,
    CleanupPublishBatchRequest, FinalizePublishOutcome, FinalizePublishRequest,
    HeartbeatPublishRequest, ManifestRowInput, MarkObjectsUploadedBatchRequest, PublicationContext,
    PublicationError, PublicationService, PublishCommandOutcome, PublishedArtifact,
    StageManifestBatchRequest, StageObjectsBatchRequest, StagedObjectUpdate,
    TakeOverOrphanedPublishRequest, TransitionPublishRequest, MAX_PUBLICATION_BATCH_ROWS,
};
pub use publication_records::{
    ArtifactRevisionRecord, GcCandidateRecord, PathEntry, PublicationRecordCodecError,
    RevisionRefRecord, WorkspaceIncarnationClaimRecord, WorkspaceRecord, MAX_CONTENT_TYPE_BYTES,
    MAX_DEPENDENCY_COUNT as MAX_REVISION_DEPENDENCIES,
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
    AggregateSpec, CatalogField, CatalogPage, CatalogRequest, ChangeEvent, ChangePage,
    ChangePageRequest, CommittedFilter, FacetBucket, FacetResult, FindWorkspacesPage,
    FindWorkspacesRequest, QueryError, QueryOperand, QueryOperator, QueryPredicate, QueryScope,
    QuerySort, QuerySortDirection, SearchHit, SearchPage, SearchRequest, WorkspaceDiscovery,
    MAX_FACET_BUCKETS_PER_FIELD, MAX_QUERY_AGGREGATES, MAX_QUERY_CURSOR_BYTES,
    MAX_QUERY_FACET_FIELDS, MAX_QUERY_GROUP_FIELDS, MAX_QUERY_IN_VALUES, MAX_QUERY_PAGE_SIZE,
    MAX_QUERY_PREDICATES, MAX_QUERY_PROJECTION_FIELDS, MAX_QUERY_SORT_FIELDS,
};
pub use query_records::{
    encode_ordered_index_scalar, secondary_index_field_prefix, secondary_index_key,
    ChangeEventKind, ChangeEventRecord, FiniteFloat, QueryFieldId, QueryRecordError, QueryScalar,
    QueryScalarType, SecondaryIndexRecord, TypedProjection, CHANGE_EVENT_VALUE_FORMAT_VERSION,
    MAX_QUERY_FIELD_ID_BYTES, MAX_QUERY_SCALAR_BYTES, MAX_TYPED_PROJECTION_BYTES,
    MAX_TYPED_PROJECTION_FIELDS, QUERY_RECORD_VALUE_FORMAT_VERSION,
};
#[cfg(feature = "metadata-read-stats")]
pub use read_stats::{
    MetadataReadStats, MetadataReadStatsDeltaError, MetadataReadStatsSessionError,
};
pub use records::{
    CommandDedupeRecord, CurrentValue, HistoryValue, RecordCodecError, RootFence,
    ROOT_FENCE_VALUE_FORMAT_VERSION,
};
pub use recovery::{
    RecoveryCodecError, RecoveryMutationV1, RecoveryOutboxRecord, RecoveryResultV1, RecoveryState,
    RECOVERY_CHAIN_DIGEST_BYTES, RECOVERY_OUTBOX_VALUE_FORMAT_VERSION,
};
pub use remove::{remove_path, RemovePathError, RemovePathOutcome, RemovePathRequest};
pub use restore::{
    abort_restore, apply_restore_initialization, begin_restore, cleanup_restore_batch,
    complete_restore, copy_restore_batch, finish_restore_cleanup, get_restore, seal_restore_source,
    start_restore_cleanup, start_restore_copy, AbortRestoreRequest, BeginRestoreRequest,
    CompleteRestoreOutcome, CopyRestoreBatchOutcome, CopyRestoreBatchRequest,
    RestoreCommandOutcome, RestoreError, RestoreInitialization, RestoreOperationRequest,
    RestoreSourceSelector, MAX_RESTORE_BATCH_MEMBERS, RESTORE_MANIFEST_PATH,
};
pub use restore_records::{
    RestoreManifestDescriptor, RestoreMemberRecord, RestoreOperationRecord, RestoreRecordError,
    RestoreResult, RestoreSource, RestoreTerminalError, RestoreTerminalErrorKind,
    RestoreTransition, MAX_RESTORE_MANIFEST_BYTES, MAX_RESTORE_MEMBERS,
    MAX_RESTORE_TERMINAL_ERROR_BYTES, RESTORE_MANIFEST_CONTENT_TYPE, RESTORE_VALUE_FORMAT_VERSION,
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
