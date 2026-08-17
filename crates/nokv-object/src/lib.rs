//! Immutable Agent artifact storage for NoKV.
//!
//! Permanent keys are owned by an artifact revision and remain independent of
//! object-provider endpoints and physical shard owners. This crate uploads and
//! verifies immutable blocks, plans strict range reads, exposes S3-compatible
//! and in-memory durable stores, and provides local soft caches. Namespace
//! visibility, revision reachability, metadata transactions, and GC policy live
//! in `nokv-meta`.

mod admission;
mod artifact;
mod cache;
pub(crate) mod chunked_immutable_blob;
mod digest;
mod local_hot;
mod namespace;
#[cfg(test)]
mod provider_admission_test;
mod recovery_checkpoint;
mod recovery_log_segment;
mod store;
mod tiered;

pub use admission::{
    admit_artifact_provider, AdmittedCreateMode, ProviderAdmissionCapability,
    ProviderAdmissionError, ProviderAdmissionProfile, ProviderAdmissionReceipt,
    ProviderHandleIdentity, MAX_SINGLE_PUT_ADMISSION_BYTES, PROVIDER_ADMISSION_CONTRACT_VERSION,
};
pub use artifact::{
    cleanup_staged_artifact, plan_artifact_read, plan_artifact_upload, read_artifact,
    read_artifact_range, read_artifact_window, upload_artifact_from_plan, verify_artifact_bytes,
    ArtifactBlock, ArtifactBlockRead, ArtifactCleanupFailure, ArtifactCleanupOutcome,
    ArtifactKeyspace, ArtifactManifest, ArtifactReadOutcome, ArtifactReadPlan, ArtifactReadStats,
    ArtifactReadWindow, ArtifactUpload, ArtifactUploadFailure, ArtifactUploadOptions,
    ArtifactUploadPlan, ArtifactUploadStats, StagedArtifactObjects, DEFAULT_ARTIFACT_BLOCK_SIZE,
};
pub use cache::{
    ArtifactBlockCache, ArtifactCacheStats, MemoryArtifactCache, MemoryArtifactCacheOptions,
};
pub use local_hot::{LocalHotTier, LocalHotTierOptions, LocalHotTierStats};
pub use namespace::{
    ensure_object_namespace, load_object_namespace, verify_object_namespace, BoundArtifactStore,
    OBJECT_NAMESPACE_MARKER_KEY,
};
pub use recovery_checkpoint::{
    plan_recovery_checkpoint_blob, read_recovery_checkpoint_blob,
    write_recovery_checkpoint_blob_from_plan, RecoveryCheckpointBlob, RecoveryCheckpointBlobPlan,
    RecoveryCheckpointBlobReceipt, RecoveryCheckpointBoundary, RecoveryCheckpointError,
    RecoveryCheckpointIdentity, RecoveryCheckpointWrite, DEFAULT_RECOVERY_CHECKPOINT_CHUNK_SIZE,
    MAX_RECOVERY_CHECKPOINT_CHUNKS, MAX_RECOVERY_CHECKPOINT_ENVELOPE_BYTES,
};
pub use recovery_log_segment::{
    plan_recovery_log_segment, read_recovery_log_segment, write_recovery_log_segment_from_plan,
    RecoveryLogSegment, RecoveryLogSegmentError, RecoveryLogSegmentIdentity,
    RecoveryLogSegmentPlan, RecoveryLogSegmentReceipt, RecoveryLogSegmentWrite,
    DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE, MAX_RECOVERY_LOG_SEGMENT_BYTES,
    MAX_RECOVERY_LOG_SEGMENT_CHUNKS,
};
pub use store::{
    ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome, MemoryArtifactStore,
    MemoryArtifactStoreStats, ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange,
    S3ArtifactStore, S3ArtifactStoreOptions, DEFAULT_S3_MULTIPART_CONCURRENCY,
    DEFAULT_S3_MULTIPART_PART_SIZE,
};
pub use tiered::{TieredArtifactStore, TieredArtifactStoreOptions, TieredArtifactStoreStats};

#[cfg(test)]
mod tests;
