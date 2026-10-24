//! Storage-neutral Agent workspace domain model.
//!
//! This crate owns root, workbench, path, artifact, commit, snapshot, operation,
//! and lifecycle identities, and the caller-facing failure classes. It does not own metadata layout, routing
//! implementation, object providers, or wire encoding.

pub mod failure;
pub mod workspace;

pub use failure::{ErrorClass, IdempotencyScope, NextAction};
pub use workspace::time_ordered_identity;
pub use workspace::{
    AgentId, ArtifactRevisionId, BuildCommitPhase, CommandDigest, CommitConsumerKind, CommitId,
    CommitRetirePhase, CommitState, CommitVersion, ConsumerEpoch, CredentialFingerprint,
    DurableNameError, GcClaimState, GcPhase, Generation, GenericIndexGenerationId,
    GenericIndexGenerationState, GenericIndexReferenceKind, GenericIndexRegistrationPhase,
    HistoryHoldKind, HistoryHoldState, LogicalShardId, NormalizedRelativePath,
    NormalizedRelativePathError, ObjectNamespaceId, OperationId, OperationKind, OwnerEpoch,
    PlacementGeneration, PrincipalId, PublishPhase, ReadVersion, ReferenceEpoch, ReferenceKind,
    RequestId, RestorePhase, RestoreSourceKind, RevisionState, Role, RoleScope,
    RootActivationState, RootId, RootPlacementLifecycle, SnapshotAliasName, SnapshotId,
    SnapshotState, StagedCleanupState, StagedProviderState, TagName, TenantId,
    UnknownDurableDiscriminant, WorkbenchId, WorkbenchIdError, WorkbenchRoot, WorkbenchRootError,
    WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState, ZeroValueError, FIXED_ID_BYTES,
    MAX_INLINE_ARTIFACT_BODY_BYTES, SHA256_BYTES, TIME_ORDERED_PREFIX_BYTES,
};
