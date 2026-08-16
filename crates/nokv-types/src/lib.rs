//! Storage-neutral Agent workspace domain model.
//!
//! This crate owns root, workbench, path, artifact, commit, snapshot, operation,
//! and lifecycle identities. It does not own metadata layout, routing
//! implementation, object providers, or wire encoding.

pub mod workspace;

pub use workspace::{
    AgentId, ArtifactRevisionId, BuildCommitPhase, CommandDigest, CommitConsumerKind, CommitId,
    CommitRetirePhase, CommitState, CommitVersion, ConsumerEpoch, DurableNameError, GcClaimState,
    GcPhase, Generation, HistoryHoldKind, HistoryHoldState, LogicalShardId, NormalizedRelativePath,
    NormalizedRelativePathError, ObjectNamespaceId, OperationId, OperationKind, OwnerEpoch,
    PlacementGeneration, PublishPhase, ReadVersion, ReferenceEpoch, ReferenceKind, RequestId,
    RestorePhase, RestoreSourceKind, RevisionState, RootActivationState, RootId,
    RootPlacementLifecycle, SnapshotAliasName, SnapshotId, SnapshotState, StagedCleanupState,
    StagedProviderState, TagName, UnknownDurableDiscriminant, WorkbenchId, WorkbenchIdError,
    WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState, ZeroValueError, FIXED_ID_BYTES,
    SHA256_BYTES,
};
