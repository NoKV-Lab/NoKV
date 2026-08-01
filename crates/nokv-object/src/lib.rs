//! Immutable Agent artifact storage for NoKV.
//!
//! Permanent keys are owned by an artifact revision and remain independent of
//! object-provider endpoints and physical shard owners. This crate uploads and
//! verifies immutable blocks, plans strict range reads, exposes S3-compatible
//! and in-memory durable stores, and provides local soft caches. Namespace
//! visibility, revision reachability, metadata transactions, and GC policy live
//! in `nokv-meta`.

mod artifact;
mod cache;
mod digest;
mod local_hot;
mod store;
mod tiered;

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
pub use store::{
    ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome, MemoryArtifactStore,
    MemoryArtifactStoreStats, ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange,
    S3ArtifactStore, S3ArtifactStoreOptions, DEFAULT_S3_MULTIPART_CONCURRENCY,
    DEFAULT_S3_MULTIPART_PART_SIZE,
};
pub use tiered::{TieredArtifactStore, TieredArtifactStoreOptions, TieredArtifactStoreStats};

#[cfg(test)]
mod tests;
