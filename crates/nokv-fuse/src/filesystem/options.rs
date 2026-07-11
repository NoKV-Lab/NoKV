use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use fuser::MountOption;
use nokv_meta::ObjectTransferStats;
use nokv_object::{
    BlockCachePolicy, BlockCacheStats, FileReadPipelineOptions, LocalObjectStoreStats,
    ObjectPrefetchOptions, ObjectPrefetchStats, ObjectWritebackStats, TieredObjectStoreStats,
    WritebackCacheStats, DEFAULT_S3_MULTIPART_CONCURRENCY,
};
use nokv_types::InodeId;

use crate::invalidation::FuseInvalidationOptions;

const DEFAULT_WRITEBACK_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_WRITEBACK_CACHE_ITEMS: usize = 16 * 1024;
const DEFAULT_WRITEBACK_QUEUE_CAPACITY: usize = 256;
const DEFAULT_WRITEBACK_WORKERS: usize = DEFAULT_S3_MULTIPART_CONCURRENCY;
const DEFAULT_WRITEBACK_UPLOAD_WORKERS_PER_REQUEST: usize = DEFAULT_WRITEBACK_WORKERS;
#[cfg(target_os = "macos")]
const MACOS_DIRECT_IO_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct FuseOptions {
    pub entry_ttl: Duration,
    pub attr_ttl: Duration,
    pub fs_name: String,
    pub threads: usize,
    pub kernel_cache: bool,
    pub direct_io: bool,
    pub block_cache: BlockCachePolicy,
    pub prefetch: FusePrefetchOptions,
    pub read_pipeline: FileReadPipelineOptions,
    pub writeback: FuseWritebackOptions,
    pub view: FuseView,
    /// Absolute live namespace path that owns `view` when mounting a snapshot.
    /// Required for `FuseView::Snapshot`; snapshot RPCs route and bind through
    /// this path while all targets remain snapshot-root-relative.
    pub snapshot_root_path: Option<String>,
    pub access: FuseAccessMode,
    pub invalidation: FuseInvalidationOptions,
    pub stats_bind: Option<SocketAddr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FusePrefetchOptions {
    pub enabled: bool,
    pub queue_capacity: usize,
    pub workers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FuseWritebackOptions {
    pub cache_enabled: bool,
    pub root: PathBuf,
    pub max_bytes: u64,
    pub max_items: usize,
    pub queue_capacity: usize,
    pub workers: usize,
    pub upload_workers_per_request: usize,
    /// Opt-in "ack-on-buffer": `flush`/`release` acknowledge once the write
    /// blocks are durably staged in the writeback cache and a record is fsync'd
    /// to the publish journal; the object-store upload + manifest commit complete
    /// in the background. Trades durability for write-close latency, so it is off
    /// by default and requires `cache_enabled`.
    pub async_publish: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FuseObjectPipelineStats {
    pub block_cache: Option<BlockCacheStats>,
    pub prefetch: Option<ObjectPrefetchStats>,
    pub writeback_cache: Option<WritebackCacheStats>,
    pub writeback: Option<ObjectWritebackStats>,
    pub tiered_object: Option<TieredObjectStoreStats>,
    pub local_hot: Option<LocalObjectStoreStats>,
    pub fuse_read_requests: u64,
    pub fuse_read_request_bytes: u64,
    pub foreground_object_gets: u64,
    pub foreground_object_get_bytes: u64,
    pub foreground_coalesced_gets: u64,
    pub foreground_coalesced_get_bytes: u64,
    pub foreground_cache_hits: u64,
    pub foreground_cache_hit_bytes: u64,
    pub foreground_block_cache_hits: u64,
    pub foreground_block_cache_hit_bytes: u64,
    pub foreground_read_window_hits: u64,
    pub foreground_read_window_hit_bytes: u64,
    pub read_plan_cache_hits: u64,
    pub read_plan_cache_misses: u64,
}

impl FuseObjectPipelineStats {
    pub fn object_transfer_stats(&self) -> ObjectTransferStats {
        let prefetch = self.prefetch.unwrap_or_default();
        let writeback = self.writeback.unwrap_or_default();
        ObjectTransferStats {
            object_puts: 0,
            object_put_bytes: 0,
            object_gets: self
                .foreground_object_gets
                .saturating_add(prefetch.object_gets),
            object_get_bytes: self
                .foreground_object_get_bytes
                .saturating_add(prefetch.object_get_bytes),
            coalesced_gets: self.foreground_coalesced_gets,
            coalesced_get_bytes: self.foreground_coalesced_get_bytes,
            cache_hits: self
                .foreground_cache_hits
                .saturating_add(prefetch.cache_hits),
            cache_hit_bytes: self
                .foreground_cache_hit_bytes
                .saturating_add(prefetch.cache_hit_bytes),
            prefetch_enqueued: prefetch.enqueued,
            prefetch_dropped: prefetch.dropped,
            prefetch_completed: prefetch.completed,
            prefetch_failed: prefetch.failed,
            prefetch_object_gets: prefetch.object_gets,
            prefetch_object_get_bytes: prefetch.object_get_bytes,
            prefetch_cache_hits: prefetch.cache_hits,
            prefetch_cache_hit_bytes: prefetch.cache_hit_bytes,
            read_plan_cache_hits: self.read_plan_cache_hits,
            read_plan_cache_misses: self.read_plan_cache_misses,
            object_writeback_enqueued: writeback.enqueued,
            object_writeback_inline: writeback.inline,
            object_writeback_completed: writeback.completed,
            object_writeback_failed: writeback.failed,
            object_writeback_staged_bytes: writeback.staged_bytes,
            object_writeback_uploaded_bytes: writeback.uploaded_bytes,
            object_writeback_queue_wait_ns: writeback.queue_wait_ns,
            object_writeback_queue_max_wait_ns: writeback.queue_max_wait_ns,
            object_writeback_upload_ns: writeback.upload_ns,
            object_writeback_upload_max_ns: writeback.upload_max_ns,
            object_writeback_collect_ns: writeback.collect_ns,
            object_writeback_digest_ns: writeback.digest_ns,
            object_writeback_store_put_ns: writeback.store_put_ns,
            object_writeback_cache_put_ns: writeback.cache_put_ns,
            manifest_chunks: 0,
            manifest_blocks: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuseView {
    Live,
    Snapshot { snapshot_id: u64, root: InodeId },
}

impl FuseView {
    pub(super) fn root(self) -> InodeId {
        match self {
            Self::Live => InodeId::root(),
            Self::Snapshot { root, .. } => root,
        }
    }

    pub(super) fn is_read_only(self) -> bool {
        matches!(self, Self::Snapshot { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuseAccessMode {
    ReadWrite,
    ReadOnly,
}

impl FuseAccessMode {
    pub(super) fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

impl Default for FuseOptions {
    fn default() -> Self {
        Self {
            entry_ttl: Duration::from_secs(1),
            attr_ttl: Duration::from_secs(1),
            fs_name: "nokv".to_owned(),
            threads: default_threads(),
            kernel_cache: true,
            direct_io: false,
            block_cache: BlockCachePolicy::default(),
            prefetch: FusePrefetchOptions::default(),
            read_pipeline: FileReadPipelineOptions::default(),
            writeback: FuseWritebackOptions::default(),
            view: FuseView::Live,
            snapshot_root_path: None,
            access: FuseAccessMode::ReadWrite,
            invalidation: FuseInvalidationOptions::default(),
            stats_bind: None,
        }
    }
}

impl Default for FusePrefetchOptions {
    fn default() -> Self {
        let defaults = ObjectPrefetchOptions::default();
        Self {
            enabled: true,
            queue_capacity: defaults.queue_capacity,
            workers: defaults.workers,
        }
    }
}

impl From<FusePrefetchOptions> for ObjectPrefetchOptions {
    fn from(options: FusePrefetchOptions) -> Self {
        Self {
            queue_capacity: options.queue_capacity,
            workers: options.workers,
        }
    }
}

impl Default for FuseWritebackOptions {
    fn default() -> Self {
        Self {
            cache_enabled: false,
            root: std::env::temp_dir().join(format!("nokv-writeback-{}", std::process::id())),
            max_bytes: DEFAULT_WRITEBACK_CACHE_BYTES,
            max_items: DEFAULT_WRITEBACK_CACHE_ITEMS,
            queue_capacity: DEFAULT_WRITEBACK_QUEUE_CAPACITY,
            workers: DEFAULT_WRITEBACK_WORKERS,
            upload_workers_per_request: DEFAULT_WRITEBACK_UPLOAD_WORKERS_PER_REQUEST,
            async_publish: false,
        }
    }
}

pub(super) fn mount_options(options: &FuseOptions) -> Vec<MountOption> {
    let mut mount_options = vec![MountOption::FSName(options.fs_name.clone())];
    if options.access.is_read_only() || options.view.is_read_only() {
        mount_options.push(MountOption::RO);
    } else {
        mount_options.push(MountOption::RW);
    }
    #[cfg(target_os = "macos")]
    {
        mount_options.push(MountOption::CUSTOM("fstypename=nokv".to_owned()));
        mount_options.push(MountOption::CUSTOM(format!("volname={}", options.fs_name)));
        // NoKV does not persist Finder/resource-fork metadata yet, so suppress the
        // AppleDouble sidecars macFUSE would otherwise materialise as visible `._`
        // files. We do NOT pass `noapplexattr`: that makes macFUSE reject every
        // `com.apple.*` xattr with EPERM, which breaks `cp` (macOS tags files with
        // `com.apple.provenance`, and `cp` copies it). Our xattr store accepts
        // arbitrary names, so those flow through the normal setxattr path instead.
        mount_options.push(MountOption::CUSTOM("noappledouble".to_owned()));
        if options.direct_io {
            mount_options.push(MountOption::CUSTOM("direct_io".to_owned()));
            mount_options.push(MountOption::CUSTOM(format!(
                "iosize={MACOS_DIRECT_IO_SIZE}"
            )));
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        mount_options.push(MountOption::Subtype("nokv".to_owned()));
        mount_options.push(MountOption::NoAtime);
    }
    mount_options
}

#[cfg(target_os = "linux")]
fn default_threads() -> usize {
    4
}

#[cfg(not(target_os = "linux"))]
fn default_threads() -> usize {
    1
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FuseStatfs {
    pub(super) blocks: u64,
    pub(super) bfree: u64,
    pub(super) bavail: u64,
    pub(super) files: u64,
    pub(super) ffree: u64,
    pub(super) bsize: u32,
    pub(super) namelen: u32,
    pub(super) frsize: u32,
}

pub(super) const STATFS_BLOCK_SIZE: u32 = 4096;
pub(super) const STATFS_TOTAL_BYTES: u64 = 1 << 40;
pub(super) const STATFS_TOTAL_FILES: u64 = 1 << 32;
pub(super) const STATFS_NAME_MAX: u32 = 255;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_fuse_threads_match_parallel_read_baseline() {
        #[cfg(target_os = "linux")]
        assert_eq!(FuseOptions::default().threads, 4);
        #[cfg(not(target_os = "linux"))]
        assert_eq!(FuseOptions::default().threads, 1);
    }

    #[test]
    fn default_writeback_parallelism_favors_request_fanout() {
        let options = FuseWritebackOptions::default();
        assert!(!options.cache_enabled);
        assert_eq!(options.workers, DEFAULT_WRITEBACK_WORKERS);
        assert_eq!(
            options.upload_workers_per_request,
            DEFAULT_WRITEBACK_UPLOAD_WORKERS_PER_REQUEST
        );
    }
}
