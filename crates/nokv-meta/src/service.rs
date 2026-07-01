//! In-process NoKV metadata service.
//!
//! This crate owns the first Rust-native service semantics over the
//! storage-neutral metadata command contract. It compiles namespace operations
//! into `MetadataCommand`s and stores file bodies through an object-store
//! boundary. It does not own Holt trees, Raft replication, FUSE, or protobuf.

mod agent;
mod allocator;
mod backup;
mod checkpoint;
mod clone;
mod command;
mod fsck;
mod gc;
mod lifecycle;
mod lock;
mod log_archive;
mod log_sync;
mod namespace;
mod publish;
mod read;
mod rollback;
mod snapshot;
mod watch;
mod xattr;

pub use self::backup::{MetadataArchiveConfig, MetadataBackupOutcome, MetadataRestoreOutcome};
pub use self::checkpoint::{CheckpointHandle, CheckpointShard};
pub use self::fsck::{DanglingBlock, FsckReport};
pub use self::log_archive::{
    MetadataLogArchiveConfig, MetadataLogRestoreOutcome, MetadataLogSegmentArchiveOutcome,
};
pub use self::log_sync::{
    MetadataLogSegmentPointer, MetadataLogSyncConfig, MetadataLogSyncSnapshot,
};
pub use self::snapshot::DEFAULT_SNAPSHOT_LEASE_MS;

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use self::lock::AdvisoryLockTable;
use crate::command::{
    CommandKind, CommitResult, DelimitedScanItem, DelimitedScanRequest, HistoryPruneOutcome,
    HistoryPruneRequest, KeyScanRequest, MetadataCommand, MetadataError, MetadataStore,
    MetadataStoreStats, MetadataStoreStatsProvider, Mutation, MutationOp, Predicate, PredicateRef,
    ReadPurpose, ScanRequest, Value, Version, WatchProjection,
};
use crate::layout::{
    allocator_key, chunk_manifest_key, chunk_manifest_prefix, decode_allocator_state,
    decode_body_descriptor, decode_chunk_manifest, decode_dentry_projection, decode_inode_attr,
    decode_object_gc_record, decode_path_index_catalog, decode_path_index_row, decode_snapshot_pin,
    decode_watch_event, dentry_key, dentry_mount_prefix, dentry_prefix, encode_allocator_state,
    encode_body_descriptor, encode_chunk_manifest, encode_dentry_projection, encode_inode_attr,
    encode_object_gc_record, encode_path_index_catalog, encode_path_index_row, encode_snapshot_pin,
    encode_watch_event, gc_object_key, gc_queue_prefix, inode_key, path_index_catalog_key,
    path_index_key, path_index_prefix, path_index_row_key, path_index_row_prefix, snapshot_pin_key,
    snapshot_pin_prefix, watch_log_key, watch_log_prefix, xattr_key, xattr_prefix,
    PathIndexCatalogRecord, PathIndexFieldRecord, PathIndexRowRecord, PathIndexValueRecord,
    PATH_INDEX_DELIMITER,
};
use nokv_object::{
    plan_chunk_manifest_reads, BlockReadOptions, ChunkStore, ChunkWriteOptions, ChunkWriteRange,
    ChunkedWrite, MemoryBlockCache, ObjectCleanupOutcome, ObjectError, ObjectKey, ObjectReadBlock,
    ObjectStore, StagedObjectSet, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokv_types::{
    parse_absolute_path, AdvisoryLock, BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName,
    DentryProjection, DentryRecord, FileType, InodeAttr, InodeId, ModelError, MountId,
    ObjectGcRecord, PathError, PathMetadata, ReadLease, RecordFamily, SliceManifest, SnapshotPin,
    SpecialNodeSpec, WatchCursor, WatchEvent, WatchEventKind, WatchRecord,
};
use sha2::{Digest, Sha256};

pub use agent::{
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
    NamespaceSchema, NamespaceSort, NamespaceSortDirection, NamespaceSortField,
    RecordCountProvenance,
};

const BODY_SUMMARY_CHUNK_INDEX: u64 = u64::MAX;
const ALLOCATOR_VERSION_RESERVATION: u64 = 1024;
const ALLOCATOR_INODE_RESERVATION: u64 = 1024;
const BODY_DIGEST_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const PATH_RESOLUTION_CACHE_MAX_ENTRIES: usize = 4096;
const PATH_INDEX_LOOKUP_CACHE_MAX_ENTRIES: usize = 4096;
const PATH_INDEX_VALIDATION_CACHE_MAX_ENTRIES: usize = 4096;
const PATH_CACHE_SHARD_COUNT: usize = 64;
const PATH_RESOLUTION_CACHE_MAX_ENTRIES_PER_SHARD: usize =
    PATH_RESOLUTION_CACHE_MAX_ENTRIES / PATH_CACHE_SHARD_COUNT;
const PATH_INDEX_LOOKUP_CACHE_MAX_ENTRIES_PER_SHARD: usize =
    PATH_INDEX_LOOKUP_CACHE_MAX_ENTRIES / PATH_CACHE_SHARD_COUNT;
const PATH_INDEX_VALIDATION_CACHE_MAX_ENTRIES_PER_SHARD: usize =
    PATH_INDEX_VALIDATION_CACHE_MAX_ENTRIES / PATH_CACHE_SHARD_COUNT;
pub(crate) const DEFAULT_READ_LEASE_MS: u64 = 3_600_000;

// Families folded into the fallback allocator rebuild when the durable
// `allocator_key` System record is absent. Each row contributes its commit
// version to the recovered high-water (`last_commit_version`), and the Inode /
// Dentry arms additionally fold any locally-owned inode id.
//
// `CommandDedupe` is deliberately EXCLUDED. Two reasons, either of which is
// sufficient:
//   1. Encoding: a dedupe row's value is a header-less 24-byte result payload
//      (`encode_dedupe_result`), not the standard `[version:8][kind:1][..]`
//      shape every other family uses. The scan path decodes every row through
//      `decode_current_value`, which rejects it ("unknown kind"), so scanning
//      `CommandDedupe` here crashes the fallback rebuild on any populated store.
//   2. Redundancy: the family is keyed by `request_id` and carries no inode, so
//      it can never raise the inode high-water; and every committed command that
//      wrote a dedupe row also wrote Inode/Dentry/Gc/Watch/etc. records at the
//      SAME commit version, all of which are still scanned below. So its commit
//      version is already covered and dropping it cannot lower the recovered
//      `last_commit_version`.
// `CommandDedupe` is the ONLY family with a non-standard value encoding; every
// other family below writes through `encode_current_value`, so the scan can
// decode them and recovery stays correct.
const ALLOCATOR_RECOVERY_FAMILIES: [RecordFamily; 12] = [
    RecordFamily::System,
    RecordFamily::Mount,
    RecordFamily::Inode,
    RecordFamily::Dentry,
    RecordFamily::Parent,
    RecordFamily::Xattr,
    RecordFamily::ChunkManifest,
    RecordFamily::Session,
    RecordFamily::PathIndex,
    RecordFamily::Watch,
    RecordFamily::Snapshot,
    RecordFamily::Gc,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AllocatorState {
    // These values are durable reservation upper bounds. Recovery may skip
    // unused ids after a crash, but must never reuse a visible version/inode.
    last_commit_version: u64,
    next_inode: u64,
    // Monotonic identity of the allocation authority. `1` while a single owner
    // holds the inode space; a control plane bumps it on ownership transfer so a
    // stale owner can be fenced. Recovery folds it with fetch_max (never regresses).
    epoch: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct PathResolutionCacheKey {
    root: u64,
    version: u64,
    components_key: Vec<u8>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct PathIndexLookupCacheKey {
    read_version: u64,
    index_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PathIndexLookupCacheValue {
    entry: DentryWithAttr,
    dentry_version: Version,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct PathIndexValidationCacheKey {
    read_version: u64,
    index_version: u64,
    index_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StagedArtifactBody {
    body: BodyDescriptor,
    chunks: Vec<ChunkManifest>,
    old_chunks: Vec<ChunkManifest>,
    staged: StagedObjectSet,
}

struct ReplaceProjectionCommit<'a> {
    kind: CommandKind,
    projection: &'a DentryProjection,
    chunks: &'a [ChunkManifest],
    old_chunks: &'a [ChunkManifest],
    dentry_version: Version,
    old_generation: Option<u64>,
    version: Version,
    path_index: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateInDirPathBatch {
    pub parent_path: String,
    pub names: Vec<DentryName>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryWithAttr {
    pub dentry: DentryRecord,
    pub attr: InodeAttr,
    pub body: Option<BodyDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadDirPlusPage {
    pub entries: Vec<DentryWithAttr>,
    pub next_cursor: Option<DentryName>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifact {
    pub parent: InodeId,
    pub name: DentryName,
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub bytes: Vec<u8>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifactRange {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifactSession {
    pub parent: InodeId,
    pub name: DentryName,
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub size: u64,
    pub ranges: Vec<PublishArtifactRange>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifactStagedSession {
    pub parent: InodeId,
    pub name: DentryName,
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub size: u64,
    pub chunks: Vec<ChunkManifest>,
    pub staged: StagedObjectSet,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub mtime_ms: Option<u64>,
    pub ctime_ms: Option<u64>,
}

impl UpdateAttr {
    fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.uid.is_none()
            && self.gid.is_none()
            && self.size.is_none()
            && self.mtime_ms.is_none()
            && self.ctime_ms.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifact {
    pub parent: InodeId,
    pub name: DentryName,
    pub path: Option<String>,
    pub inode: InodeId,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedPreparedArtifact {
    pub entry: DentryWithAttr,
    pub prepared: PreparedArtifact,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectTransferStats {
    pub object_puts: u64,
    pub object_put_bytes: u64,
    pub object_gets: u64,
    pub object_get_bytes: u64,
    pub coalesced_gets: u64,
    pub coalesced_get_bytes: u64,
    pub cache_hits: u64,
    pub cache_hit_bytes: u64,
    pub prefetch_enqueued: u64,
    pub prefetch_dropped: u64,
    pub prefetch_completed: u64,
    pub prefetch_failed: u64,
    pub prefetch_object_gets: u64,
    pub prefetch_object_get_bytes: u64,
    pub prefetch_cache_hits: u64,
    pub prefetch_cache_hit_bytes: u64,
    pub read_plan_cache_hits: u64,
    pub read_plan_cache_misses: u64,
    pub object_writeback_enqueued: u64,
    pub object_writeback_inline: u64,
    pub object_writeback_completed: u64,
    pub object_writeback_failed: u64,
    pub object_writeback_staged_bytes: u64,
    pub object_writeback_uploaded_bytes: u64,
    pub object_writeback_queue_wait_ns: u64,
    pub object_writeback_queue_max_wait_ns: u64,
    pub object_writeback_upload_ns: u64,
    pub object_writeback_upload_max_ns: u64,
    pub object_writeback_collect_ns: u64,
    pub object_writeback_digest_ns: u64,
    pub object_writeback_store_put_ns: u64,
    pub object_writeback_cache_put_ns: u64,
    pub manifest_chunks: u64,
    pub manifest_blocks: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataServiceStats {
    pub path_index_lookup_total: u64,
    pub path_index_hit_total: u64,
    pub path_index_miss_total: u64,
    pub path_index_stale_total: u64,
    pub path_index_scan_stale_total: u64,
    pub path_index_fallback_total: u64,
    pub create_files_batch_total: u64,
    pub create_files_entry_total: u64,
    pub create_dirs_batch_total: u64,
    pub create_dirs_entry_total: u64,
    pub read_dir_plus_total: u64,
    pub read_dir_plus_entry_total: u64,
    pub read_dir_plus_projection_hit_total: u64,
    pub metadata_log_segments_archived_total: u64,
    pub metadata_log_entries_archived_total: u64,
    pub metadata_log_archive_bytes_total: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingObjectCleanupOutcome {
    pub scanned: usize,
    pub blocked_by_snapshots: usize,
    pub blocked_by_read_leases: usize,
    pub attempted: usize,
    pub deleted: usize,
    pub missing: usize,
    pub records_removed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BodyReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PathReadPlan {
    pub metadata: PathMetadata,
    pub plan: BodyReadPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenPathReadPlan {
    pub metadata: PathMetadata,
    pub lease: ReadLease,
    pub plan: BodyReadPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenPathReadPlanRequest {
    pub path: String,
    pub offset: u64,
    pub len: usize,
    pub expected_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameReplaceResult {
    pub entry: DentryWithAttr,
    pub replaced: Option<DentryWithAttr>,
}

/// Handle to a writable copy-on-write fork produced by [`NoKvFs::clone_subtree`].
///
/// `root` is the new namespace root: it sees every file and directory the source
/// subtree had at clone time and shares the source's object blocks (no data copy)
/// until the fork diverges on write. `snapshot_id` is a retained snapshot pin on
/// the source that holds the GC retention floor down so the shared base blocks are
/// protected while the fork references them. Retire it with
/// [`NoKvFs::retire_snapshot`] once the fork's own divergent state no longer needs
/// the source's base objects (typically when the fork is deleted).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneHandle {
    pub root: InodeId,
    pub snapshot_id: u64,
}

/// How a path differs between two subtrees, as reported by
/// [`NoKvFs::diff_subtrees`]. Directions are relative to the diff arguments
/// `diff_subtrees(a_root, b_root)`: `Added` exists only under `b_root`, `Removed`
/// exists only under `a_root`, and `Modified` exists under both but the content or
/// type differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubtreeDeltaKind {
    Added,
    Removed,
    Modified,
}

/// A single path-level difference between two subtrees. `path` is relative to the
/// subtree roots (e.g. `/a`, `/dir/b`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubtreeDelta {
    pub path: String,
    pub kind: SubtreeDeltaKind,
    /// Content digest (e.g. `sha256:…`) of the changed side — the `b`-side body for
    /// `Added`/`Modified`, the `a`-side body for `Removed`. `None` for directories
    /// or bodiless nodes. Makes the diff content-addressed, not just nominal.
    pub digest: Option<String>,
    /// Net byte-size change: `+size` for `Added`, `-size` for `Removed`,
    /// `b.size - a.size` for `Modified`.
    pub size_delta: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XattrSetMode {
    Any,
    Create,
    Replace,
}

#[derive(Debug)]
pub enum MetadError {
    Model(ModelError),
    Metadata(MetadataError),
    Object(ObjectError),
    PublishArtifactFailed {
        source: Box<MetadError>,
        staged: StagedObjectSet,
    },
    Codec(String),
    BodySizeMismatch {
        descriptor: u64,
        bytes: u64,
    },
    InvalidPreparedArtifact(String),
    InvalidQuery(String),
    StaleBodyGeneration {
        expected: u64,
        current: u64,
    },
    LockConflict(AdvisoryLock),
    AllocatorExhausted,
    InvalidPath(String),
    NotFound,
    NotFile,
    NotDirectory,
    DirectoryNotEmpty,
    CannotRemoveRoot,
    MissingBodyDescriptor,
    InvalidOwnerEpoch,
    StaleOwnerEpoch {
        owner_epoch: u64,
        required_epoch: u64,
    },
    /// The owner's lease deadline has passed without a successful renewal, so it
    /// self-fenced. The caller should re-resolve the shard owner and retry.
    LeaseExpired {
        now_ms: u64,
        deadline_ms: u64,
    },
    /// The addressed shard is not owned by this node. The caller should
    /// re-resolve the shard owner (via the control plane / shard map) and retry
    /// against `endpoint` when present.
    NotOwner {
        shard_id: String,
        endpoint: Option<String>,
    },
    /// A rename/hardlink/clone would cross a shard boundary: the two endpoints
    /// live in different shards' namespaces, so it cannot be a single in-shard
    /// commit. Surfaced as `EXDEV` to userspace, matching POSIX cross-device
    /// link/rename semantics, rather than a misleading `NotFound` from resolving
    /// the destination inside the source shard.
    CrossShard {
        source_shard: u16,
        dest_shard: u16,
    },
    /// The target dentry is a cross-shard graft point: its child inode is owned
    /// by another shard (`child.shard_index() != self.shard_index()`), so its
    /// contents live in the child shard, not here. A plain remove/rename on the
    /// parent shard would see a locally-empty subtree and silently orphan the
    /// entire child namespace. Reject and steer the caller to the lifecycle path
    /// (`unregister-graft`). Surfaced as `EBUSY` (the entry is a live mount
    /// point), NOT `EXDEV` — there is no copy+unlink fallback that would be
    /// correct here.
    GraftPoint,
    /// The command was durably committed to the local engine, but archiving its
    /// logical-log segment failed, so durability could not be acknowledged.
    /// `committed` distinguishes "applied locally, not durable" from a plain
    /// failure so a client does not blindly retry data that actually landed.
    SyncLogArchiveFailed {
        committed: bool,
        message: String,
    },
}

pub struct NoKvFs<M, O> {
    mount: MountId,
    /// Stable index of the shard this service owns. Encoded into the high bits
    /// of every inode it mints (so inodes are globally unique across shards and
    /// self-routing). The default/root shard is index 0 — unchanged behavior.
    shard_index: u16,
    metadata: M,
    objects: O,
    allocator_gate: Mutex<()>,
    backup_gate: Mutex<()>,
    /// Serializes owner-epoch installs/observes against in-flight commits.
    /// Commits hold a read guard across their fence check + durable apply;
    /// epoch changes take the write guard. This closes the TOCTOU where a
    /// failover epoch bump could land between a commit's check and its apply,
    /// letting a stale owner commit one more time.
    epoch_fence: RwLock<()>,
    path_resolution_cache: Vec<Mutex<BTreeMap<PathResolutionCacheKey, InodeId>>>,
    path_index_lookup_cache:
        Vec<Mutex<BTreeMap<PathIndexLookupCacheKey, PathIndexLookupCacheValue>>>,
    path_index_validation_cache: Vec<Mutex<BTreeMap<PathIndexValidationCacheKey, DentryWithAttr>>>,
    advisory_locks: Mutex<AdvisoryLockTable>,
    clock: AtomicU64,
    reserved_version: AtomicU64,
    next_inode: AtomicU64,
    reserved_next_inode: AtomicU64,
    /// Identity of this node's allocation authority (see [`AllocatorState::epoch`]).
    /// Persisted with every reservation and recovered with fetch_max so it never
    /// regresses; the seam a control plane bumps to fence a stale owner.
    epoch: AtomicU64,
    /// Lowest control-plane owner epoch allowed to commit through this service.
    required_owner_epoch: AtomicU64,
    /// Wall-clock deadline (ms since epoch) past which this owner refuses to
    /// commit, regardless of control-plane reachability. `0` = disabled
    /// (single-node dev, or owners with auto-renewal turned off). Refreshed on
    /// every successful lease renewal; a partitioned owner that stops renewing
    /// self-fences here even though it can never observe a bumped epoch.
    lease_deadline_ms: AtomicU64,
    /// Test/simulation clock override (ms since epoch). `0` = use the system
    /// clock. Lets lease-deadline fencing be exercised deterministically.
    clock_override_ms: AtomicU64,
    metadata_log_sync: Mutex<Option<log_sync::MetadataLogSyncState>>,
    metadata_log_segments_archived_total: AtomicU64,
    metadata_log_entries_archived_total: AtomicU64,
    metadata_log_archive_bytes_total: AtomicU64,
    block_cache: MemoryBlockCache,
    block_cache_enabled: AtomicBool,
    watch_logging_enabled: AtomicBool,
    object_puts: AtomicU64,
    object_put_bytes: AtomicU64,
    object_gets: AtomicU64,
    object_get_bytes: AtomicU64,
    coalesced_gets: AtomicU64,
    coalesced_get_bytes: AtomicU64,
    cache_hits: AtomicU64,
    cache_hit_bytes: AtomicU64,
    manifest_chunks: AtomicU64,
    manifest_blocks: AtomicU64,
    path_index_lookup_total: AtomicU64,
    path_index_hit_total: AtomicU64,
    path_index_miss_total: AtomicU64,
    path_index_stale_total: AtomicU64,
    path_index_scan_stale_total: AtomicU64,
    path_index_fallback_total: AtomicU64,
    create_files_batch_total: AtomicU64,
    create_files_entry_total: AtomicU64,
    create_dirs_batch_total: AtomicU64,
    create_dirs_entry_total: AtomicU64,
    read_dir_plus_total: AtomicU64,
    read_dir_plus_entry_total: AtomicU64,
    read_dir_plus_projection_hit_total: AtomicU64,
}

fn new_path_resolution_cache_shards() -> Vec<Mutex<BTreeMap<PathResolutionCacheKey, InodeId>>> {
    (0..PATH_CACHE_SHARD_COUNT)
        .map(|_| Mutex::new(BTreeMap::new()))
        .collect()
}

fn new_path_index_lookup_cache_shards(
) -> Vec<Mutex<BTreeMap<PathIndexLookupCacheKey, PathIndexLookupCacheValue>>> {
    (0..PATH_CACHE_SHARD_COUNT)
        .map(|_| Mutex::new(BTreeMap::new()))
        .collect()
}

fn new_path_index_validation_cache_shards(
) -> Vec<Mutex<BTreeMap<PathIndexValidationCacheKey, DentryWithAttr>>> {
    (0..PATH_CACHE_SHARD_COUNT)
        .map(|_| Mutex::new(BTreeMap::new()))
        .collect()
}

fn path_cache_shard_index<T: Hash>(key: &T) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % PATH_CACHE_SHARD_COUNT
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    fn resized_body_digest_uri(
        &self,
        inode: InodeId,
        old_body: Option<&BodyDescriptor>,
        new_size: u64,
        read_version: Version,
    ) -> Result<String, MetadError> {
        let mut hasher = Sha256::new();
        let mut offset = 0_u64;
        let old_size = old_body.map(|body| body.size).unwrap_or(0);
        let old_prefix_len = old_size.min(new_size);

        if let Some(body) = old_body {
            while offset < old_prefix_len {
                let requested = usize::try_from((old_prefix_len - offset).min(
                    u64::try_from(BODY_DIGEST_CHUNK_SIZE).map_err(|_| ObjectError::InvalidRange)?,
                ))
                .map_err(|_| ObjectError::InvalidRange)?;
                let bytes =
                    self.read_file_at_version(inode, body, offset, requested, read_version)?;
                if bytes.is_empty() {
                    return Err(ObjectError::InvalidRange.into());
                }
                hasher.update(&bytes);
                offset = offset
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| ObjectError::InvalidRange)?)
                    .ok_or(ObjectError::InvalidRange)?;
            }
        }

        let mut zero_remaining = new_size.saturating_sub(old_prefix_len);
        if zero_remaining > 0 {
            let zeros = vec![0_u8; BODY_DIGEST_CHUNK_SIZE];
            while zero_remaining > 0 {
                let len = usize::try_from(zero_remaining.min(
                    u64::try_from(BODY_DIGEST_CHUNK_SIZE).map_err(|_| ObjectError::InvalidRange)?,
                ))
                .map_err(|_| ObjectError::InvalidRange)?;
                hasher.update(&zeros[..len]);
                zero_remaining -= u64::try_from(len).map_err(|_| ObjectError::InvalidRange)?;
            }
        }

        let digest = hasher.finalize();
        Ok(format!("sha256:{digest:x}"))
    }
}

impl<M, O> NoKvFs<M, O> where M: MetadataStore + MetadataStoreStatsProvider {}

fn projection(
    parent: InodeId,
    name: DentryName,
    attr: InodeAttr,
    body: Option<BodyDescriptor>,
) -> DentryProjection {
    DentryProjection {
        dentry: DentryRecord {
            parent,
            name,
            child: attr.inode,
            child_type: attr.file_type,
            attr_generation: attr.generation,
        },
        attr,
        body,
    }
}

fn recover_allocator_state<M: MetadataStore>(
    metadata: &M,
    mount: MountId,
    shard_index: u16,
) -> Result<AllocatorState, MetadError> {
    let max_read = Version::new(u64::MAX)?;
    if let Some(value) = metadata.get(
        RecordFamily::System,
        &allocator_key(mount),
        max_read,
        ReadPurpose::UserStrong,
    )? {
        let (last_commit_version, next_inode, epoch) =
            decode_allocator_state(&value.0).map_err(|err| MetadError::Codec(err.to_string()))?;
        Version::new(last_commit_version)?;
        InodeId::new(next_inode)?;
        return Ok(AllocatorState {
            last_commit_version,
            next_inode,
            epoch,
        });
    }

    let mut last_commit_version = 1_u64;
    let mut max_inode = InodeId::ROOT_RAW;
    // Only inodes minted by THIS shard may raise the local allocator floor.
    // Foreign inodes embedded in this shard's records (a cross-shard graft's
    // target dir, or any other cross-shard reference) live in another shard's
    // subspace; folding them here would poison this shard's allocator and let it
    // hand out ids it doesn't own. For shard 0 every owned id has shard_index 0
    // (`compose(0, x) == x`), so this guard is a no-op and the single-shard
    // recovery path is unchanged. Version/generation folding stays unconditional
    // because the commit clock is shared across the mount.
    let fold_owned_inode = |max_inode: &mut u64, raw: u64| {
        if InodeId::new(raw)
            .map(|inode| inode.shard_index() == shard_index)
            .unwrap_or(false)
        {
            *max_inode = (*max_inode).max(raw);
        }
    };
    for family in ALLOCATOR_RECOVERY_FAMILIES {
        let rows = metadata.scan(ScanRequest {
            family,
            prefix: Vec::new(),
            start_after: None,
            version: max_read,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        for row in rows {
            last_commit_version = last_commit_version.max(row.version.get());
            match family {
                RecordFamily::Inode => {
                    let attr = decode_inode_attr(&row.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?;
                    last_commit_version = last_commit_version.max(attr.generation);
                    fold_owned_inode(&mut max_inode, attr.inode.get());
                }
                RecordFamily::Dentry => {
                    let projection = decode_dentry_projection(&row.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?;
                    last_commit_version = last_commit_version
                        .max(projection.attr.generation)
                        .max(projection.dentry.attr_generation);
                    fold_owned_inode(&mut max_inode, projection.attr.inode.get());
                    fold_owned_inode(&mut max_inode, projection.dentry.child.get());
                }
                _ => {}
            }
        }
    }

    let next_inode = max_inode
        .checked_add(1)
        .ok_or(MetadError::AllocatorExhausted)?;
    Ok(AllocatorState {
        last_commit_version,
        next_inode,
        // No durable record: bootstrap the single-owner epoch.
        epoch: 1,
    })
}

fn reservation_upper_bound(required: u64, reservation: u64) -> u64 {
    required.saturating_add(reservation)
}

fn directory_attr(inode: InodeId, mode: u32, uid: u32, gid: u32, version: u64) -> InodeAttr {
    let now_ms = current_time_ms();
    InodeAttr {
        inode,
        file_type: FileType::Directory,
        mode,
        uid,
        gid,
        rdev: 0,
        nlink: FileType::Directory.initial_link_count(),
        size: 0,
        generation: version,
        mtime_ms: now_ms,
        ctime_ms: now_ms,
    }
}

fn current_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

fn body_digest_uri(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn delete_mutation(family: RecordFamily, key: Vec<u8>) -> Mutation {
    Mutation {
        family,
        key,
        op: MutationOp::Delete,
        value: None,
    }
}

fn put_projection_mutation(
    family: RecordFamily,
    key: Vec<u8>,
    projection: &DentryProjection,
) -> Mutation {
    Mutation {
        family,
        key,
        op: MutationOp::Put,
        value: Some(Value(encode_dentry_projection(projection))),
    }
}

fn ensure_unique_names(names: &[DentryName]) -> Result<(), MetadError> {
    let mut seen = HashSet::with_capacity(names.len());
    for name in names {
        if !seen.insert(name.as_bytes()) {
            return Err(MetadError::InvalidPath(format!(
                "duplicate dentry name {} in batched create",
                String::from_utf8_lossy(name.as_bytes())
            )));
        }
    }
    Ok(())
}

fn canonical_path(components: &[DentryName]) -> Result<String, MetadError> {
    if components.is_empty() {
        return Ok("/".to_owned());
    }
    let mut out = String::new();
    for component in components {
        out.push('/');
        out.push_str(
            std::str::from_utf8(component.as_bytes()).map_err(|_| {
                MetadError::InvalidPath("path indexes require utf-8 paths".to_owned())
            })?,
        );
    }
    Ok(out)
}

fn create_watch_kind(kind: CommandKind) -> WatchEventKind {
    match kind {
        CommandKind::PublishArtifact => WatchEventKind::PublishArtifact,
        CommandKind::CreateFile
        | CommandKind::CreateFiles
        | CommandKind::CreateDir
        | CommandKind::CreateSymlink
        | CommandKind::CreateSpecialNode
        | CommandKind::Link => WatchEventKind::Create,
        _ => WatchEventKind::UpdateAttr,
    }
}

fn validate_prepared_artifact(
    prepared: &PreparedArtifact,
    body: &BodyDescriptor,
    chunks: &[ChunkManifest],
) -> Result<(), MetadError> {
    if body.generation != prepared.generation {
        return Err(MetadError::InvalidPreparedArtifact(format!(
            "body generation {} does not match prepared generation {}",
            body.generation, prepared.generation
        )));
    }
    if body.chunk_size == 0 || body.block_size == 0 {
        return Err(ObjectError::InvalidChunkLayout.into());
    }
    if body.size == 0 {
        if !chunks.is_empty() {
            return Err(MetadError::InvalidPreparedArtifact(
                "empty body must not contain chunk manifests".to_owned(),
            ));
        }
        return Ok(());
    }
    let last_chunk = (body.size - 1) / body.chunk_size;
    // A self-contained generation (base_generation == 0) must cover every chunk
    // of the file; a delta/sparse generation stores only the chunks it rewrote
    // (untouched chunks fall through to the base on read).
    if body.base_generation == 0 && chunks.len() as u64 != last_chunk + 1 {
        return Err(MetadError::InvalidPreparedArtifact(format!(
            "chunk manifest count {} does not match expected {}",
            chunks.len(),
            last_chunk + 1
        )));
    }
    let mut seen_chunks = HashSet::new();
    for chunk in chunks.iter() {
        let chunk_index = chunk.chunk_index;
        if chunk_index > last_chunk {
            return Err(MetadError::InvalidPreparedArtifact(format!(
                "chunk manifest index {chunk_index} exceeds last chunk {last_chunk}"
            )));
        }
        if !seen_chunks.insert(chunk_index) {
            return Err(MetadError::InvalidPreparedArtifact(format!(
                "duplicate chunk manifest index {chunk_index}"
            )));
        }
        let expected_offset = chunk_index
            .checked_mul(body.chunk_size)
            .ok_or(ObjectError::InvalidRange)?;
        if chunk.logical_offset != expected_offset {
            return Err(MetadError::InvalidPreparedArtifact(format!(
                "chunk {} starts at {} but expected {expected_offset}",
                chunk.chunk_index, chunk.logical_offset
            )));
        }
        let expected_len = body.chunk_size.min(body.size - expected_offset);
        if chunk.len != expected_len {
            return Err(MetadError::InvalidPreparedArtifact(format!(
                "chunk {} length {} does not match expected {expected_len}",
                chunk.chunk_index, chunk.len
            )));
        }
        let chunk_end = chunk
            .logical_offset
            .checked_add(chunk.len)
            .ok_or(ObjectError::InvalidRange)?;
        let mut seen_slices = HashSet::new();
        for slice in &chunk.slices {
            if slice.len == 0 {
                return Err(MetadError::InvalidPreparedArtifact(
                    "slice descriptor must not be empty".to_owned(),
                ));
            }
            if !seen_slices.insert(slice.slice_id) {
                return Err(MetadError::InvalidPreparedArtifact(format!(
                    "duplicate slice id {} in chunk {}",
                    slice.slice_id, chunk.chunk_index
                )));
            }
            let slice_end = slice
                .logical_offset
                .checked_add(slice.len)
                .ok_or(ObjectError::InvalidRange)?;
            if slice_end > chunk_end || slice.logical_offset < chunk.logical_offset {
                return Err(MetadError::InvalidPreparedArtifact(
                    "slice descriptor is outside chunk range".to_owned(),
                ));
            }
            validate_slice_block_coverage(chunk.chunk_index, body.block_size, slice, slice_end)?;
        }
    }
    Ok(())
}

fn validate_slice_block_coverage(
    chunk_index: u64,
    block_size: u64,
    slice: &SliceManifest,
    slice_end: u64,
) -> Result<(), MetadError> {
    if slice.blocks.is_empty() {
        return Err(MetadError::InvalidPreparedArtifact(format!(
            "slice {} in chunk {chunk_index} has no blocks",
            slice.slice_id
        )));
    }
    let mut intervals = Vec::with_capacity(slice.blocks.len());
    for block in &slice.blocks {
        if block.object_key.is_empty() || block.digest_uri.is_empty() {
            return Err(MetadError::InvalidPreparedArtifact(
                "block descriptor is missing object identity".to_owned(),
            ));
        }
        if block.len == 0 {
            return Err(MetadError::InvalidPreparedArtifact(
                "block descriptor must not be empty".to_owned(),
            ));
        }
        if block.len > block_size {
            return Err(MetadError::InvalidPreparedArtifact(format!(
                "block descriptor length {} exceeds configured block size {block_size}",
                block.len
            )));
        }
        block
            .object_offset
            .checked_add(block.len)
            .ok_or(ObjectError::InvalidRange)?;
        let block_end = block
            .logical_offset
            .checked_add(block.len)
            .ok_or(ObjectError::InvalidRange)?;
        if block_end > slice_end || block.logical_offset < slice.logical_offset {
            return Err(MetadError::InvalidPreparedArtifact(
                "block descriptor is outside slice range".to_owned(),
            ));
        }
        intervals.push((block.logical_offset, block_end));
    }
    intervals.sort_unstable();
    let mut expected = slice.logical_offset;
    for (start, end) in intervals {
        if start != expected {
            return Err(MetadError::InvalidPreparedArtifact(format!(
                "slice {} in chunk {chunk_index} has a block coverage gap",
                slice.slice_id
            )));
        }
        expected = end;
    }
    if expected != slice_end {
        return Err(MetadError::InvalidPreparedArtifact(format!(
            "slice {} in chunk {chunk_index} is not fully covered by blocks",
            slice.slice_id
        )));
    }
    Ok(())
}

fn validate_artifact_ranges(request: &PublishArtifactSession) -> Result<(), MetadError> {
    let mut ranges = request
        .ranges
        .iter()
        .filter(|range| !range.bytes.is_empty())
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| range.offset);
    let mut previous_end = 0_u64;
    for range in ranges {
        let len = u64::try_from(range.bytes.len()).map_err(|_| ObjectError::InvalidRange)?;
        let end = range
            .offset
            .checked_add(len)
            .ok_or(ObjectError::InvalidRange)?;
        if end > request.size {
            return Err(MetadError::InvalidPreparedArtifact(
                "dirty range exceeds session body size".to_owned(),
            ));
        }
        if range.offset < previous_end {
            return Err(MetadError::InvalidPreparedArtifact(
                "dirty ranges must not overlap".to_owned(),
            ));
        }
        previous_end = end;
    }
    Ok(())
}

fn merge_session_chunks(
    size: u64,
    old_chunks: Vec<ChunkManifest>,
    dirty_chunks: Vec<ChunkManifest>,
) -> Result<Vec<ChunkManifest>, MetadError> {
    let mut chunks = BTreeMap::<u64, ChunkManifest>::new();
    if size > 0 {
        let last_chunk = (size - 1) / DEFAULT_CHUNK_SIZE;
        for chunk_index in 0..=last_chunk {
            ensure_manifest_chunk(&mut chunks, chunk_index, size);
        }
    }
    for old_chunk in old_chunks {
        append_chunk_manifest_slices(&mut chunks, old_chunk, size)?;
    }
    for dirty_chunk in dirty_chunks {
        append_chunk_manifest_slices(&mut chunks, dirty_chunk, size)?;
    }
    Ok(chunks.into_values().collect())
}

/// Coalesce each chunk's accumulated slices into the minimal newest-wins set.
/// Used at compaction so slice count does not grow without bound across
/// compaction cycles (the chain-collapse re-materialize alone keeps every
/// superseded slice). Metadata-only: the planner emits sub-ranges of existing
/// block objects, so the coalesced manifest borrows them without copying bytes.
fn compact_chunk_slices(chunks: Vec<ChunkManifest>) -> Result<Vec<ChunkManifest>, MetadError> {
    chunks.into_iter().map(coalesce_chunk_slices).collect()
}

fn coalesce_chunk_slices(chunk: ChunkManifest) -> Result<ChunkManifest, MetadError> {
    if chunk.slices.len() <= 1 {
        return Ok(chunk);
    }
    let plan = plan_chunk_manifest_reads(
        std::slice::from_ref(&chunk),
        chunk.logical_offset,
        usize::try_from(chunk.len).map_err(|_| ObjectError::InvalidRange)?,
    )?;
    let blocks = plan
        .blocks
        .into_iter()
        .map(|read| BlockDescriptor {
            object_key: read.object_key,
            logical_offset: chunk.logical_offset + read.output_offset as u64,
            object_offset: read.object_offset,
            len: read.len as u64,
            digest_uri: read.digest_uri,
        })
        .collect::<Vec<_>>();
    let mut coalesced = ChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        slices: Vec::new(),
    };
    append_contiguous_slices(&mut coalesced, blocks)?;
    Ok(coalesced)
}

fn append_chunk_manifest_slices(
    chunks: &mut BTreeMap<u64, ChunkManifest>,
    manifest: ChunkManifest,
    size: u64,
) -> Result<(), MetadError> {
    for slice in manifest.slices {
        let mut blocks = Vec::new();
        for block in slice.blocks {
            let Some(block) = clip_block_to_size(block, size)? else {
                continue;
            };
            blocks.push(block);
        }
        if blocks.is_empty() {
            continue;
        }
        let chunk_index = slice.logical_offset / DEFAULT_CHUNK_SIZE;
        let chunk = ensure_manifest_chunk(chunks, chunk_index, size);
        append_contiguous_slices(chunk, blocks)?;
    }
    Ok(())
}

fn ensure_manifest_chunk(
    chunks: &mut BTreeMap<u64, ChunkManifest>,
    chunk_index: u64,
    size: u64,
) -> &mut ChunkManifest {
    chunks.entry(chunk_index).or_insert_with(|| {
        let logical_offset = chunk_index.saturating_mul(DEFAULT_CHUNK_SIZE);
        let len = if logical_offset >= size {
            0
        } else {
            DEFAULT_CHUNK_SIZE.min(size - logical_offset)
        };
        ChunkManifest {
            chunk_index,
            logical_offset,
            len,
            slices: Vec::new(),
        }
    })
}

fn next_slice_id(chunk: &ChunkManifest) -> u64 {
    chunk
        .slices
        .iter()
        .map(|slice| slice.slice_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn append_contiguous_slices(
    chunk: &mut ChunkManifest,
    mut blocks: Vec<BlockDescriptor>,
) -> Result<(), MetadError> {
    if blocks.is_empty() {
        return Ok(());
    }
    blocks.sort_by_key(|block| block.logical_offset);
    let mut current = Vec::new();
    let mut current_end = None;
    for block in blocks {
        if block.len == 0 {
            return Err(MetadError::InvalidPreparedArtifact(
                "block descriptor must not be empty".to_owned(),
            ));
        }
        let block_end = block
            .logical_offset
            .checked_add(block.len)
            .ok_or(ObjectError::InvalidRange)?;
        match current_end {
            Some(end) if block.logical_offset == end => {
                current.push(block);
                current_end = Some(block_end);
            }
            Some(_) => {
                push_slice_from_contiguous_blocks(chunk, std::mem::take(&mut current))?;
                current.push(block);
                current_end = Some(block_end);
            }
            None => {
                current.push(block);
                current_end = Some(block_end);
            }
        }
    }
    push_slice_from_contiguous_blocks(chunk, current)?;
    Ok(())
}

fn push_slice_from_contiguous_blocks(
    chunk: &mut ChunkManifest,
    blocks: Vec<BlockDescriptor>,
) -> Result<(), MetadError> {
    let Some(first) = blocks.first() else {
        return Ok(());
    };
    let logical_offset = first.logical_offset;
    let end = blocks
        .iter()
        .map(|block| block.logical_offset.saturating_add(block.len))
        .max()
        .unwrap_or(logical_offset);
    let slice_id = next_slice_id(chunk);
    chunk.slices.push(SliceManifest {
        slice_id,
        logical_offset,
        len: end.saturating_sub(logical_offset),
        blocks,
    });
    Ok(())
}

fn clip_block_to_size(
    mut block: BlockDescriptor,
    size: u64,
) -> Result<Option<BlockDescriptor>, MetadError> {
    if block.logical_offset >= size {
        return Ok(None);
    }
    let max_len = size - block.logical_offset;
    block.len = block.len.min(max_len);
    if block.len == 0 {
        return Ok(None);
    }
    block
        .logical_offset
        .checked_add(block.len)
        .ok_or(ObjectError::InvalidRange)?;
    Ok(Some(block))
}

fn chunk_object_keys(chunks: &[ChunkManifest]) -> HashSet<String> {
    chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .slices
                .iter()
                .flat_map(|slice| slice.blocks.iter().map(|block| block.object_key.clone()))
        })
        .collect()
}

fn manifest_block_count(chunks: &[ChunkManifest]) -> u64 {
    chunks
        .iter()
        .flat_map(|chunk| chunk.slices.iter())
        .map(|slice| slice.blocks.len() as u64)
        .sum()
}

fn watch_cursor_from_key(key: &[u8]) -> Result<WatchCursor, MetadError> {
    let cursor_len = std::mem::size_of::<u64>() * 2;
    if key.len() < cursor_len {
        return Err(MetadError::Codec(
            "watch log key is missing cursor suffix".to_owned(),
        ));
    }
    let offset = key.len() - cursor_len;
    Ok(WatchCursor {
        version: u64::from_be_bytes(
            key[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("watch version has fixed width"),
        ),
        event_id: u64::from_be_bytes(
            key[offset + std::mem::size_of::<u64>()..]
                .try_into()
                .expect("watch event id has fixed width"),
        ),
    })
}

fn chunk_index_from_manifest_key(key: &[u8]) -> Result<u64, MetadError> {
    if key.len() < std::mem::size_of::<u64>() {
        return Err(MetadError::Codec(
            "chunk manifest key is truncated".to_owned(),
        ));
    }
    Ok(u64::from_be_bytes(
        key[key.len() - std::mem::size_of::<u64>()..]
            .try_into()
            .expect("chunk index has fixed width"),
    ))
}

fn predecessor(version: Version) -> Result<Version, MetadataError> {
    Version::new(version.get().saturating_sub(1))
}

fn request_id(prefix: &[u8], mount: MountId, inode: InodeId, version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 24);
    out.extend_from_slice(prefix);
    out.extend_from_slice(&mount.get().to_be_bytes());
    out.extend_from_slice(&inode.get().to_be_bytes());
    out.extend_from_slice(&version.get().to_be_bytes());
    out
}

fn allocator_reservation_request_id(
    mount: MountId,
    commit_version: Version,
    reserved_version: u64,
    reserved_next_inode: u64,
) -> Vec<u8> {
    let prefix = b"reserve-allocator";
    let mut out = Vec::with_capacity(prefix.len() + 32);
    out.extend_from_slice(prefix);
    out.extend_from_slice(&mount.get().to_be_bytes());
    out.extend_from_slice(&commit_version.get().to_be_bytes());
    out.extend_from_slice(&reserved_version.to_be_bytes());
    out.extend_from_slice(&reserved_next_inode.to_be_bytes());
    out
}

fn kind_name(kind: CommandKind) -> &'static [u8] {
    match kind {
        CommandKind::ReserveAllocator => b"reserve-allocator",
        CommandKind::CreateFile => b"create-file",
        CommandKind::CreateFiles => b"create-files",
        CommandKind::CreateDir => b"create-dir",
        CommandKind::CreateSymlink => b"create-symlink",
        CommandKind::CreateSpecialNode => b"create-special-node",
        CommandKind::UpdateAttr => b"update-attr",
        CommandKind::SetXattr => b"set-xattr",
        CommandKind::RemoveXattr => b"remove-xattr",
        CommandKind::Rename => b"rename",
        CommandKind::RenameReplace => b"rename-replace",
        CommandKind::Link => b"link",
        CommandKind::RemoveFile => b"remove-file",
        CommandKind::RemoveEmptyDir => b"remove-empty-dir",
        CommandKind::PublishArtifact => b"publish-artifact",
        CommandKind::ReplaceArtifact => b"replace-artifact",
        CommandKind::SnapshotSubtree => b"snapshot-subtree",
        CommandKind::RetireSnapshot => b"retire-snapshot",
        CommandKind::RenewSnapshot => b"renew-snapshot",
        CommandKind::WatchSubtree => b"watch-subtree",
        CommandKind::CleanupObjects => b"cleanup-objects",
        CommandKind::RegisterNamespaceIndex => b"register-namespace-index",
    }
}

impl From<DentryProjection> for DentryWithAttr {
    fn from(projection: DentryProjection) -> Self {
        Self {
            dentry: projection.dentry,
            attr: projection.attr,
            body: projection.body,
        }
    }
}

impl From<MetadataError> for MetadError {
    fn from(err: MetadataError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ModelError> for MetadError {
    fn from(err: ModelError) -> Self {
        Self::Model(err)
    }
}

impl From<PathError> for MetadError {
    fn from(err: PathError) -> Self {
        Self::InvalidPath(err.to_string())
    }
}

impl From<ObjectError> for MetadError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl MetadError {
    pub fn staged_objects(&self) -> Option<&StagedObjectSet> {
        match self {
            Self::PublishArtifactFailed { staged, .. } => Some(staged),
            Self::Object(ObjectError::StagedWriteFailed { staged, .. }) => Some(staged),
            _ => None,
        }
    }
}

impl fmt::Display for MetadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(err) => write!(f, "model error: {err}"),
            Self::Metadata(err) => write!(f, "metadata error: {err}"),
            Self::Object(err) => write!(f, "object error: {err}"),
            Self::PublishArtifactFailed { source, staged } => write!(
                f,
                "artifact publish failed after staging {} objects: {source}",
                staged.len()
            ),
            Self::Codec(err) => write!(f, "codec error: {err}"),
            Self::BodySizeMismatch { descriptor, bytes } => write!(
                f,
                "body descriptor size {descriptor} does not match uploaded bytes {bytes}"
            ),
            Self::InvalidPreparedArtifact(err) => {
                write!(f, "invalid prepared artifact: {err}")
            }
            Self::InvalidQuery(err) => write!(f, "invalid namespace query: {err}"),
            Self::StaleBodyGeneration { expected, current } => write!(
                f,
                "body generation {expected} is stale; current generation is {current}"
            ),
            Self::LockConflict(lock) => write!(
                f,
                "advisory lock conflicts with {:?} lock on inode {} range {}..={} owned by {}",
                lock.kind,
                lock.inode.get(),
                lock.start,
                lock.end,
                lock.owner
            ),
            Self::AllocatorExhausted => write!(f, "inode allocator is exhausted"),
            Self::InvalidPath(err) => write!(f, "invalid path: {err}"),
            Self::NotFound => write!(f, "metadata entry not found"),
            Self::NotFile => write!(f, "metadata entry is not a file"),
            Self::NotDirectory => write!(f, "metadata entry is not a directory"),
            Self::DirectoryNotEmpty => write!(f, "directory is not empty"),
            Self::CannotRemoveRoot => write!(f, "root directory cannot be removed"),
            Self::MissingBodyDescriptor => write!(f, "file is missing body descriptor"),
            Self::InvalidOwnerEpoch => write!(f, "owner epoch must be non-zero"),
            Self::StaleOwnerEpoch {
                owner_epoch,
                required_epoch,
            } => write!(
                f,
                "owner epoch {owner_epoch} is stale; required owner epoch is {required_epoch}"
            ),
            Self::LeaseExpired {
                now_ms,
                deadline_ms,
            } => write!(
                f,
                "owner lease expired: now {now_ms}ms is past deadline {deadline_ms}ms"
            ),
            Self::NotOwner { shard_id, endpoint } => match endpoint {
                Some(endpoint) => write!(
                    f,
                    "shard {shard_id} is not owned here; current owner endpoint is {endpoint}"
                ),
                None => write!(f, "shard {shard_id} is not owned here"),
            },
            Self::CrossShard {
                source_shard,
                dest_shard,
            } => write!(
                f,
                "cross-shard operation from shard {source_shard} to shard {dest_shard} is not supported (EXDEV)"
            ),
            Self::GraftPoint => write!(
                f,
                "path is a cross-shard graft point; use unregister-graft"
            ),
            Self::SyncLogArchiveFailed { committed, message } => write!(
                f,
                "metadata sync log archive failed (committed={committed}): {message}"
            ),
        }
    }
}

impl std::error::Error for MetadError {}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
