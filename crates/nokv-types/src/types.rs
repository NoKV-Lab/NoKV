use std::fmt;
use std::num::NonZeroU64;

use crate::DentryName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MountId(NonZeroU64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InodeId(NonZeroU64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    ZeroMountId,
    ZeroInodeId,
    /// A shard-local inode number was 0 or exceeded the local-bits range, so it
    /// cannot be composed into a globally-unique inode id.
    InodeLocalOutOfRange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
    NamedPipe,
    CharDevice,
    BlockDevice,
    Socket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordFamily {
    System,
    Mount,
    Inode,
    Dentry,
    Parent,
    Xattr,
    ChunkManifest,
    Session,
    PathIndex,
    Watch,
    Snapshot,
    Gc,
    CommandDedupe,
    History,
    /// Durable lifecycle and exact base-reference owner for a
    /// metadata-materialized copy-on-write fork. Keyed by fork-root inode.
    ForkBinding,
    /// Inverse index for borrowed PathIndex projections in a materialized fork,
    /// keyed by destination parent inode and dentry name.
    ForkShadow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodeAttr {
    pub inode: InodeId,
    pub file_type: FileType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub nlink: u32,
    pub size: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpecialNodeSpec {
    pub file_type: FileType,
    pub mode: u32,
    pub rdev: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdvisoryLockKind {
    Read,
    Write,
    Unlock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdvisoryLock {
    pub inode: InodeId,
    pub owner: u64,
    pub start: u64,
    pub end: u64,
    pub kind: AdvisoryLockKind,
    pub pid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdvisoryLockRequest {
    pub inode: InodeId,
    pub owner: u64,
    pub start: u64,
    pub end: u64,
    pub kind: AdvisoryLockKind,
    pub pid: u32,
    pub wait: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryRecord {
    pub parent: InodeId,
    pub name: DentryName,
    pub child: InodeId,
    pub child_type: FileType,
    pub attr_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryProjection {
    pub dentry: DentryRecord,
    pub attr: InodeAttr,
    pub body: Option<BodyDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathMetadata {
    pub attr: InodeAttr,
    pub body: Option<BodyDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    /// Generation to fall through to for chunks this (sparse) generation did
    /// not rewrite. `0` means self-contained (a fresh write or a compacted
    /// generation that re-materialized every chunk).
    pub base_generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkManifest {
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub slices: Vec<SliceManifest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SliceManifest {
    pub slice_id: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub blocks: Vec<BlockDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockDescriptor {
    pub object_key: String,
    pub logical_offset: u64,
    pub object_offset: u64,
    pub len: u64,
    pub digest_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectGcRecord {
    pub inode: InodeId,
    pub generation: u64,
    pub object_key: String,
    pub size: u64,
    pub digest_uri: String,
    pub enqueue_version: u64,
    pub enqueue_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPin {
    pub snapshot_id: u64,
    pub root: InodeId,
    pub read_version: u64,
    pub created_version: u64,
    /// Wall-clock deadline (unix ms) after which this pin no longer protects its
    /// snapshot from GC. Holders renew to extend it; an abandoned pin expires so
    /// a crashed client can never block reclamation forever.
    pub lease_expires_unix_ms: u64,
}

/// A read capability for one immutable artifact generation — the result of
/// opening a file for reading. It pins **nothing** durably: it is a client-side
/// token naming `(inode, generation)` plus the MVCC `read_version` observed at
/// open. Range reads carry `generation` and validate it against the file's
/// *current* attr (a CAS). The live generation's blocks are never GC-reclaimed,
/// so a read against it is always safe; if the artifact was superseded (a new
/// generation published, an immutable-CoW rewrite), the CAS fails fast with
/// `StaleBodyGeneration` and the caller re-opens — never a silent stale read.
///
/// This is the formal `open()` boundary for the read path: it freezes the layout
/// a reader sees (generation == the immutable `BodyDescriptor`/block map) so a
/// differently-parallelized consumer can issue arbitrary range reads against one
/// consistent view, with zero metadata written on open. To read a *superseded*
/// (historical) generation that may be reclaimed, take a durable snapshot pin
/// instead — that is the only thing that holds an old version against GC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadLease {
    pub inode: InodeId,
    pub generation: u64,
    pub read_version: u64,
    /// Wall-clock freshness hint (unix ms). Advisory only — the lease holds no
    /// durable state, so expiry just suggests the caller re-open to re-validate
    /// the generation; it does not gate GC.
    pub lease_expires_unix_ms: u64,
}

/// Lifecycle record for a metadata-materialized copy-on-write fork.
///
/// The destination has its own inode and dentry tree; reads do not redirect to
/// the source namespace. File manifests may still reference source object blocks,
/// so `base_ref_set_id` owns the exact durable references that protect those
/// shared blocks, while ForkShadow indexes retain the corresponding borrowed
/// namespace projections. `snapshot_id` and `pinned_read_version` record the
/// construction checkpoint only; the snapshot lease is not the fork's lifetime
/// holder after the destination is attached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForkBindingState {
    Preparing,
    Complete,
    /// The destination is no longer visible. Exact base-object references are
    /// being released asynchronously before the durable binding is removed.
    Releasing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkBinding {
    pub fork_root: InodeId,
    pub source_root: InodeId,
    /// Historical version used to materialize the destination metadata.
    pub pinned_read_version: u64,
    /// Construction checkpoint identity retained for provenance and retries.
    pub snapshot_id: u64,
    pub created_version: u64,
    /// Unique incarnation of the exact base-reference set owned by this fork.
    /// Unlike `operation_digest`, a retried destination incarnation never
    /// reuses this identifier.
    pub base_ref_set_id: u64,
    /// Stable digest of `(mount, source, snapshot, read-version, destination)`.
    /// Zero is reserved for non-restore forks.
    pub operation_digest: [u8; 32],
    /// Digest of the canonical detached-tree initialization. It is checked
    /// independently from the public operation id so manifest content may carry
    /// that stable id without creating a hashing cycle.
    pub initialization_digest: [u8; 32],
    pub state: ForkBindingState,
    pub destination_path: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct WatchCursor {
    pub version: u64,
    pub event_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchRecord {
    pub cursor: WatchCursor,
    pub event: WatchEvent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchEventKind {
    Create,
    Remove,
    Rename,
    UpdateAttr,
    PublishArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub parent: Option<InodeId>,
    pub name: Option<DentryName>,
    pub inode: InodeId,
    pub version: u64,
}

impl FileType {
    pub fn is_special_node(self) -> bool {
        matches!(
            self,
            Self::NamedPipe | Self::CharDevice | Self::BlockDevice | Self::Socket
        )
    }

    pub fn initial_link_count(self) -> u32 {
        match self {
            Self::Directory => 2,
            Self::File
            | Self::Symlink
            | Self::NamedPipe
            | Self::CharDevice
            | Self::BlockDevice
            | Self::Socket => 1,
        }
    }
}

impl MountId {
    pub fn new(id: u64) -> Result<Self, ModelError> {
        NonZeroU64::new(id).map(Self).ok_or(ModelError::ZeroMountId)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl InodeId {
    pub const ROOT_RAW: u64 = 1;

    /// High bits of an inode id reserved for the owning shard index, so an inode
    /// is globally unique across shards *and* self-routing: the owning shard is
    /// recoverable directly from the id (`shard_index`), which is what lets
    /// bare-inode RPCs route without a lookup.
    pub const SHARD_BITS: u32 = 16;
    /// Low bits available for a shard-local inode number.
    pub const LOCAL_BITS: u32 = u64::BITS - Self::SHARD_BITS;
    /// Largest representable shard index.
    pub const MAX_SHARD_INDEX: u16 = u16::MAX;
    /// Largest representable shard-local inode number.
    pub const MAX_LOCAL: u64 = (1u64 << Self::LOCAL_BITS) - 1;

    pub fn new(id: u64) -> Result<Self, ModelError> {
        NonZeroU64::new(id).map(Self).ok_or(ModelError::ZeroInodeId)
    }

    pub fn root() -> Self {
        Self(NonZeroU64::new(Self::ROOT_RAW).expect("root inode id is non-zero"))
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    /// Compose a globally-unique inode id from a shard index and a shard-local
    /// inode number. Shard 0 is the default/root shard, for which this is the
    /// identity on `local` — so existing single-shard ids are unchanged and the
    /// root stays global id 1. `local` must be in `1..=MAX_LOCAL`.
    pub fn compose(shard_index: u16, local: u64) -> Result<Self, ModelError> {
        if local == 0 || local > Self::MAX_LOCAL {
            return Err(ModelError::InodeLocalOutOfRange);
        }
        let raw = ((shard_index as u64) << Self::LOCAL_BITS) | local;
        Self::new(raw)
    }

    /// The shard index encoded in the high bits of this inode id.
    pub fn shard_index(self) -> u16 {
        (self.get() >> Self::LOCAL_BITS) as u16
    }

    /// The shard-local inode number in the low bits.
    pub fn local(self) -> u64 {
        self.get() & Self::MAX_LOCAL
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMountId => write!(f, "mount id must be non-zero"),
            Self::ZeroInodeId => write!(f, "inode id must be non-zero"),
            Self::InodeLocalOutOfRange => {
                write!(f, "shard-local inode number is zero or out of range")
            }
        }
    }
}

impl std::error::Error for ModelError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_reject_zero() {
        assert_eq!(MountId::new(0), Err(ModelError::ZeroMountId));
        assert_eq!(InodeId::new(0), Err(ModelError::ZeroInodeId));
    }

    #[test]
    fn root_inode_is_one() {
        assert_eq!(InodeId::root().get(), 1);
    }
}
