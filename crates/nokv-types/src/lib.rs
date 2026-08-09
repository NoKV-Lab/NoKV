//! Storage-neutral Agent workspace domain model.
//!
//! This crate owns root, workbench, path, artifact, commit, snapshot, operation,
//! and lifecycle identities. It does not own metadata layout, routing
//! implementation, object providers, or wire encoding.

pub mod workspace;

pub use workspace::{
    ArtifactRevisionId, BuildCommitPhase, CommandDigest, CommitConsumerKind, CommitId,
    CommitRetirePhase, CommitState, CommitVersion, ConsistencyDomainId, ConsumerEpoch,
    DurableNameError, GcClaimState, GcPhase, Generation, HistoryHoldKind, HistoryHoldState,
    LogicalShardId, MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRevision,
    MetadataContractDigest, MetadataMigrationTargetBinding, MetadataRecoveryFrontier,
    NormalizedRelativePath, NormalizedRelativePathError, OperationId, OperationKind, OwnerEpoch,
    OwnerIncarnationId, PlacementGeneration, PublishPhase, ReadVersion, ReferenceEpoch,
    ReferenceKind, RequestId, RestorePhase, RestoreSourceKind, RevisionState, RootActivationState,
    RootId, RootLayoutGeneration, RootLayoutProfile, RootPartitionId, RootPlacementLifecycle,
    SnapshotAliasName, SnapshotId, SnapshotState, SourceQuiesceReceipt, StagedCleanupState,
    StagedProviderState, TagName, TargetActivationToken, UnknownDurableDiscriminant, WorkbenchId,
    WorkbenchIdError, WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState, ZeroValueError,
    FIXED_ID_BYTES, SHA256_BYTES,
};
