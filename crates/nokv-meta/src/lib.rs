//! Metadata model execution for NoKV.
//!
//! This crate owns the Holt-friendly metadata layout, the storage-neutral
//! metadata command contract, the Holt-backed metastore, and the in-process
//! metadata service. It does not own object-store provider behavior, FUSE, or
//! client path ergonomics.

pub mod backup;
pub mod command;
pub mod gc;
pub mod holtstore;
pub mod layout;
pub mod log;
pub mod service;

pub use backup::{MetadataBackupOptions, MetadataBackupWorker, MetadataBackupWorkerState};
pub use command::{
    CommandKind, CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCheckpointStore,
    MetadataCommand, MetadataError, MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider,
    Mutation, MutationOp, Predicate, PredicateRef, ReadPurpose, ScanItem, ScanRequest, Value,
    Version, WatchProjection,
};
pub use gc::{
    HistoryGcOptions, HistoryGcWorker, HistoryGcWorkerState, ObjectGcOptions, ObjectGcWorker,
    ObjectGcWorkerState,
};
pub use holtstore::HoltMetadataStore;
pub use log::{
    metadata_log_replay_entries, MetadataLogEntry, MetadataLogError, MetadataLogSegment,
    METADATA_LOG_ZERO_DIGEST,
};
pub use service::{
    BodyReadPlan, CheckpointHandle, CheckpointShard, CloneHandle, CreateInDirPathBatch,
    CreatedPreparedArtifact, DanglingBlock, DentryWithAttr, FsckReport, MetadError,
    MetadataArchiveConfig, MetadataBackupOutcome, MetadataLogArchiveConfig,
    MetadataLogRestoreOutcome, MetadataLogSegmentArchiveOutcome, MetadataLogSegmentPointer,
    MetadataLogSyncConfig, MetadataLogSyncSnapshot, MetadataRestoreOutcome, MetadataServiceStats,
    NamespaceAggregateGroup, NamespaceAggregateMeasure, NamespaceAggregateOp,
    NamespaceAggregateOutputMeasure, NamespaceAggregateRequest, NamespaceAggregateResult,
    NamespaceAggregateSample, NamespaceAggregateSort, NamespaceAggregateValue,
    NamespaceBodyDescriptor, NamespaceCard, NamespaceCardKind, NamespaceFacetSummary,
    NamespaceFacetValue, NamespaceFieldSource, NamespaceFieldSourceKind, NamespaceFieldValue,
    NamespaceFilterCapability, NamespaceFindField, NamespaceFindRequest, NamespaceFindResult,
    NamespaceGrepMatch, NamespaceGrepRequest, NamespaceGrepResult, NamespaceInclude,
    NamespaceIndexField, NamespaceIndexRegistration, NamespaceIndexRow, NamespaceIndexValue,
    NamespaceListOptions, NamespaceListPage, NamespacePredicate, NamespacePredicateOp,
    NamespacePredicateValue, NamespaceQueryCatalog, NamespaceReadFormat, NamespaceReadItem,
    NamespaceReadOptions, NamespaceReadPage, NamespaceRecordCount, NamespaceRecordType,
    NamespaceSchema, NamespaceSort, NamespaceSortDirection, NamespaceSortField, NoKvFs,
    ObjectTransferStats, OpenPathReadPlan, OpenPathReadPlanRequest, PendingObjectCleanupOutcome,
    PreparedArtifact, PublishArtifact, PublishArtifactRange, PublishArtifactSession,
    PublishArtifactStagedSession, ReadDirPlusPage, RecordCountProvenance, RenameReplaceResult,
    SubtreeDelta, SubtreeDeltaKind, UpdateAttr, XattrSetMode, DEFAULT_SNAPSHOT_LEASE_MS,
};
