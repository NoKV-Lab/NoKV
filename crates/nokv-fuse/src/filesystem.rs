use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

#[cfg(test)]
use fuser::FopenFlags;
use fuser::{
    AccessFlags, BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileHandle,
    FileType as FuseFileType, Filesystem, Generation, INodeNo, OpenAccMode, OpenFlags, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
    WriteFlags,
};
use nokv_client::MetadataClient;
#[cfg(any(test, target_os = "linux"))]
use nokv_meta::RenameReplaceResult;
#[cfg(test)]
use nokv_meta::{DentryWithAttr, PublishArtifactStagedSession, ReadDirPlusPage};
use nokv_meta::{MetadError, UpdateAttr, XattrSetMode};
#[cfg(test)]
use nokv_object::{manifest_digest_uri, ObjectError, ObjectReadBlock, PendingChunkedWrite};
use nokv_object::{FileReadPipeline, ObjectReadPlanCache, ObjectStore};
#[cfg(test)]
use nokv_types::{AdvisoryLockRequest, DentryName, InodeId};
use nokv_types::{FileType, InodeAttr, SpecialNodeSpec};

use crate::attr::fuse_file_type;
use crate::backend::{ClientFuseBackend, FuseBackend, FuseBackendError};
use crate::invalidation::{FuseInvalidationWorker, InvalidationRegistry};

mod directory;
mod errors;
mod handle;
mod locks;
mod options;
mod posix;
mod publish_journal;
mod publisher;
mod write_session;
mod xattr;

pub use options::{
    FuseAccessMode, FuseObjectPipelineStats, FuseOptions, FusePrefetchOptions, FuseView,
    FuseWritebackOptions,
};

use directory::{
    child_index_from_offset, child_offset, DirectoryHandle, FUSE_DOT_DOT_OFFSET, FUSE_DOT_OFFSET,
};
use errors::errno;
use locks::{
    advisory_lock_kind_to_fuse, fuse_lock_type, fuse_rename_mode, FuseLockRequest, FuseRenameMode,
};
use options::mount_options;
#[cfg(test)]
use options::{STATFS_BLOCK_SIZE, STATFS_NAME_MAX};
use posix::{
    access_allowed, copy_file_range_size, dentry_name, file_type_from_mknod_mode, resolve_lseek,
    system_time_ms, time_or_now_ms, validate_access_mask,
};
#[cfg(test)]
pub(crate) use write_session::BufferedWriteRange;
pub(crate) use write_session::PendingBufferedRange;
use write_session::WriteHandle;
#[cfg(test)]
use write_session::{
    has_buffered_upload_ready, push_buffered_write, select_unstaged_blocks,
    staged_range_block_count, take_buffered_upload_ranges, PendingBufferedUpload,
    FUSE_WRITEBACK_UPLOAD_THRESHOLD,
};
use xattr::{
    reply_xattr_data, xattr_missing_error, xattr_name, xattr_set_mode, xattr_unsupported_error,
};

const READ_HANDLE_PLAN_CACHE_CAPACITY: usize = 512;

pub(crate) struct NoKvFuse<B: FuseBackend> {
    backend: Arc<B>,
    options: FuseOptions,
    read_stats: Arc<FuseReadStats>,
    parents: RwLock<HashMap<u64, u64>>,
    names: RwLock<HashMap<u64, Vec<u8>>>,
    attrs: Arc<RwLock<HashMap<u64, InodeAttr>>>,
    next_handle: AtomicU64,
    read_handles: RwLock<HashMap<u64, Arc<ReadHandle>>>,
    write_handles: RwLock<HashMap<u64, WriteHandle<B::Prepared>>>,
    directory_handles: RwLock<HashMap<u64, DirectoryHandle>>,
    invalidation: Arc<InvalidationRegistry>,
    /// Opt-in async write-back runtime (journal + read-after-write tracker +
    /// background publisher). `None` unless `--writeback-async-publish` is on, in
    /// which case the synchronous `publish_handle` tail is bypassed at `release`.
    writeback_publisher: Option<publisher::WritebackPublisher<B>>,
}

#[derive(Debug, Default)]
struct FuseReadStats {
    requests: AtomicU64,
    request_bytes: AtomicU64,
}

pub struct FuseMount {
    session: fuser::BackgroundSession,
    stats: Arc<dyn Fn() -> io::Result<FuseObjectPipelineStats> + Send + Sync>,
    _invalidation_worker: Option<FuseInvalidationWorker>,
    _stats_server: Option<FuseStatsServer>,
}

impl FuseMount {
    pub fn object_pipeline_stats(&self) -> io::Result<FuseObjectPipelineStats> {
        (self.stats)()
    }

    pub fn join(self) -> io::Result<()> {
        self.session.join()
    }
}

#[derive(Debug)]
struct ReadHandle {
    attr: InodeAttr,
    state: Mutex<ReadHandleState>,
}

#[derive(Debug)]
struct ReadHandleState {
    reader: FileReadPipeline,
    read_plans: ObjectReadPlanCache,
}

impl ReadHandleState {
    fn new(reader: FileReadPipeline) -> Self {
        Self {
            reader,
            read_plans: ObjectReadPlanCache::new(READ_HANDLE_PLAN_CACHE_CAPACITY),
        }
    }
}

pub fn mount_client<O>(
    metadata: MetadataClient,
    objects: O,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<()>
where
    O: ObjectStore + Send + Sync + 'static,
{
    spawn_mount_client(metadata, objects, mountpoint, options).and_then(FuseMount::join)
}

pub fn spawn_mount_client<O>(
    metadata: MetadataClient,
    objects: O,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<FuseMount>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let backend = ClientFuseBackend::new(metadata, objects, &options)
        .map_err(|err| io::Error::other(err.to_string()))?;
    spawn_mount_backend(backend, mountpoint, options)
}

fn spawn_mount_backend<B>(
    backend: B,
    mountpoint: impl AsRef<Path>,
    options: FuseOptions,
) -> io::Result<FuseMount>
where
    B: FuseBackend,
{
    let mut config = Config::default();
    let mount_options = mount_options(&options);
    config.mount_options = mount_options;
    config.n_threads = Some(options.threads);
    let backend = Arc::new(backend);
    let stats_backend = Arc::clone(&backend);
    let mut fuse = NoKvFuse::from_shared_backend(Arc::clone(&backend), options.clone());
    fuse.enable_async_publish()?;
    let stats_read = Arc::clone(&fuse.read_stats);
    let registry = fuse.invalidation_registry();
    let local_invalidation = fuse.local_invalidation();
    let session = fuser::spawn_mount2(fuse, mountpoint, &config)?;
    let stats: FuseStatsProvider = Arc::new(move || {
        let mut stats = stats_backend
            .object_pipeline_stats()
            .map_err(|err| io::Error::other(err.to_string()))?;
        merge_fuse_read_stats(&mut stats, &stats_read);
        Ok(stats)
    });
    let stats_server = options
        .stats_bind
        .map(|bind| FuseStatsServer::spawn(bind, Arc::clone(&stats)))
        .transpose()?;
    let invalidation_worker = if options.view == FuseView::Live {
        Some(FuseInvalidationWorker::spawn(
            backend,
            session.notifier(),
            registry,
            Some(local_invalidation),
            options.invalidation,
        ))
    } else {
        None
    };
    Ok(FuseMount {
        session,
        stats,
        _invalidation_worker: invalidation_worker,
        _stats_server: stats_server,
    })
}

struct FuseStatsServer {
    shutdown: Arc<AtomicBool>,
    local_addr: SocketAddr,
    handle: Option<JoinHandle<()>>,
}

type FuseStatsProvider = Arc<dyn Fn() -> io::Result<FuseObjectPipelineStats> + Send + Sync>;

impl FuseStatsServer {
    fn spawn(bind: SocketAddr, stats: FuseStatsProvider) -> io::Result<Self> {
        let listener = TcpListener::bind(bind)?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = thread::Builder::new()
            .name("nokv-fuse-stats".to_owned())
            .spawn(move || serve_fuse_stats(listener, stats, thread_shutdown))
            .map_err(io::Error::other)?;
        Ok(Self {
            shutdown,
            local_addr,
            handle: Some(handle),
        })
    }
}

impl Drop for FuseStatsServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.local_addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve_fuse_stats(listener: TcpListener, stats: FuseStatsProvider, shutdown: Arc<AtomicBool>) {
    for stream in listener.incoming() {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let Ok(mut stream) = stream else {
            continue;
        };
        let _ = handle_fuse_stats_stream(&mut stream, stats.as_ref());
    }
}

fn handle_fuse_stats_stream(
    stream: &mut TcpStream,
    stats: &(dyn Fn() -> io::Result<FuseObjectPipelineStats> + Send + Sync),
) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    let mut request = [0_u8; 1024];
    let read = stream.read(&mut request)?;
    let line = std::str::from_utf8(&request[..read])
        .unwrap_or_default()
        .lines()
        .next()
        .unwrap_or_default();
    match line
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["GET", "/healthz"] => write_http_response(stream, "200 OK", "text/plain", "ok\n"),
        ["GET", "/stats"] => {
            let body = fuse_stats_json(&stats()?);
            write_http_response(stream, "200 OK", "application/json", &body)
        }
        _ => write_http_response(stream, "404 Not Found", "text/plain", "not found\n"),
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn fuse_stats_json(stats: &FuseObjectPipelineStats) -> String {
    let objects = stats.object_transfer_stats();
    let tiered = stats.tiered_object.unwrap_or_default();
    let local_hot = stats.local_hot.unwrap_or_default();
    format!(
        "{{\"ready\":true,\"source\":\"fuse\",\"block_cache_enabled\":{},\"fuse_read_requests\":{},\"fuse_read_request_bytes\":{},\"object_puts\":{},\"object_put_bytes\":{},\"object_gets\":{},\"object_get_bytes\":{},\"coalesced_gets\":{},\"coalesced_get_bytes\":{},\"cache_hits\":{},\"cache_hit_bytes\":{},\"block_cache_hits\":{},\"block_cache_hit_bytes\":{},\"read_window_hits\":{},\"read_window_hit_bytes\":{},\"prefetch_enqueued\":{},\"prefetch_dropped\":{},\"prefetch_completed\":{},\"prefetch_failed\":{},\"prefetch_object_gets\":{},\"prefetch_object_get_bytes\":{},\"prefetch_cache_hits\":{},\"prefetch_cache_hit_bytes\":{},\"read_plan_cache_hits\":{},\"read_plan_cache_misses\":{},\"object_writeback_enqueued\":{},\"object_writeback_inline\":{},\"object_writeback_completed\":{},\"object_writeback_failed\":{},\"object_writeback_staged_bytes\":{},\"object_writeback_uploaded_bytes\":{},\"object_writeback_queue_wait_ns\":{},\"object_writeback_queue_max_wait_ns\":{},\"object_writeback_upload_ns\":{},\"object_writeback_upload_max_ns\":{},\"object_writeback_collect_ns\":{},\"object_writeback_digest_ns\":{},\"object_writeback_store_put_ns\":{},\"object_writeback_cache_put_ns\":{},\"tiered_hot_put_ns\":{},\"tiered_pending_cold_put_ns\":{},\"tiered_cold_put_enqueue_ns\":{},\"local_hot_puts\":{},\"local_hot_put_bytes\":{},\"local_hot_put_total_ns\":{},\"local_hot_put_prepare_ns\":{},\"local_hot_put_write_ns\":{},\"local_hot_put_sync_ns\":{},\"local_hot_put_rename_ns\":{},\"local_hot_put_record_ns\":{},\"manifest_chunks\":{},\"manifest_blocks\":{}}}\n",
        stats.block_cache.is_some(),
        stats.fuse_read_requests,
        stats.fuse_read_request_bytes,
        objects.object_puts,
        objects.object_put_bytes,
        objects.object_gets,
        objects.object_get_bytes,
        objects.coalesced_gets,
        objects.coalesced_get_bytes,
        objects.cache_hits,
        objects.cache_hit_bytes,
        stats.foreground_block_cache_hits,
        stats.foreground_block_cache_hit_bytes,
        stats.foreground_read_window_hits,
        stats.foreground_read_window_hit_bytes,
        objects.prefetch_enqueued,
        objects.prefetch_dropped,
        objects.prefetch_completed,
        objects.prefetch_failed,
        objects.prefetch_object_gets,
        objects.prefetch_object_get_bytes,
        objects.prefetch_cache_hits,
        objects.prefetch_cache_hit_bytes,
        objects.read_plan_cache_hits,
        objects.read_plan_cache_misses,
        objects.object_writeback_enqueued,
        objects.object_writeback_inline,
        objects.object_writeback_completed,
        objects.object_writeback_failed,
        objects.object_writeback_staged_bytes,
        objects.object_writeback_uploaded_bytes,
        objects.object_writeback_queue_wait_ns,
        objects.object_writeback_queue_max_wait_ns,
        objects.object_writeback_upload_ns,
        objects.object_writeback_upload_max_ns,
        objects.object_writeback_collect_ns,
        objects.object_writeback_digest_ns,
        objects.object_writeback_store_put_ns,
        objects.object_writeback_cache_put_ns,
        tiered.hot_put_ns,
        tiered.pending_cold_put_ns,
        tiered.cold_put_enqueue_ns,
        local_hot.puts,
        local_hot.put_bytes,
        local_hot.put_total_ns,
        local_hot.put_prepare_ns,
        local_hot.put_write_ns,
        local_hot.put_sync_ns,
        local_hot.put_rename_ns,
        local_hot.put_record_ns,
        objects.manifest_chunks,
        objects.manifest_blocks,
    )
}

fn merge_fuse_read_stats(stats: &mut FuseObjectPipelineStats, read_stats: &FuseReadStats) {
    stats.fuse_read_requests = read_stats.requests.load(Ordering::Relaxed);
    stats.fuse_read_request_bytes = read_stats.request_bytes.load(Ordering::Relaxed);
}

impl<B> Filesystem for NoKvFuse<B>
where
    B: FuseBackend,
{
    fn forget(&self, _req: &Request, ino: INodeNo, _nlookup: u64) {
        if let Ok(inode) = self.metadata_inode(ino) {
            self.forget_inode_cache(inode);
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if let Err(err) = self.wait_pending_publish_dentry(parent, &name) {
            reply.error(err);
            return;
        }
        match self.service_lookup_plus(parent, &name) {
            Ok(Some(entry)) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        // No async-publish barrier here: the kernel issues a getattr while
        // closing a just-written file, and barriering it would make the writer
        // wait out its own deferred publish (a self-inflicted stall). A *reader*
        // resolves the path through `lookup` first, which does barrier, so
        // read-after-write across handles stays consistent without this one.
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(attr) => {
                self.remember_attr(&attr);
                let attr = self.attr_with_pending_size(attr);
                reply.attr(&self.options.attr_ttl, &self.view_file_attr(&attr))
            }
            Err(err) => reply.error(err),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let mut published_handle = false;
        if let Some(fh) = fh {
            if let Err(err) = self.update_handle_attrs(fh, mode, uid, gid) {
                reply.error(err);
                return;
            }
            if let Some(size) = size {
                if let Err(err) = self.truncate_handle(fh, size) {
                    reply.error(err);
                    return;
                }
            }
            if size.is_some() {
                if let Err(err) = self.publish_handle(fh) {
                    reply.error(err);
                    return;
                }
                published_handle = true;
            }
        }
        if published_handle && mtime.is_none() && ctime.is_none() {
            match self.service_get_attr(inode) {
                Ok(Some(attr)) => {
                    self.remember_attr(&attr);
                    reply.attr(&self.options.attr_ttl, &self.view_file_attr(&attr));
                }
                Ok(None) => reply.error(Errno::ENOENT),
                Err(err) => reply.error(errno(err)),
            }
            return;
        }
        let changes = UpdateAttr {
            mode: (!published_handle).then_some(mode).flatten(),
            uid: (!published_handle).then_some(uid).flatten(),
            gid: (!published_handle).then_some(gid).flatten(),
            size: (!published_handle).then_some(size).flatten(),
            mtime_ms: mtime.map(time_or_now_ms),
            ctime_ms: ctime.map(system_time_ms),
        };
        if inode == self.options.view.root() {
            match self.backend.update_root_attrs(changes) {
                Ok(attr) => {
                    self.remember_attr(&attr);
                    reply.attr(&self.options.attr_ttl, &self.view_file_attr(&attr));
                }
                Err(err) => reply.error(errno(err)),
            }
            return;
        }
        let parent = self.parent_of(inode);
        let name = match self.name_of(inode) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.update_attrs(parent, &name, changes) {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.attr(&self.options.attr_ttl, &self.view_file_attr(&entry.attr));
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.service_read_symlink(inode) {
            Ok(target) => reply.data(&target),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if let Err(err) = self.wait_pending_publish(inode) {
            reply.error(err);
            return;
        }
        // The read-open path trusts the local attr cache (`read_open_attr`); after
        // waiting out an async publish, drop the now-stale entry so the open
        // resolves the just-committed generation.
        if self.writeback_publisher.is_some() {
            self.forget_attr_cache(inode);
        }
        let attr = match flags.acc_mode() {
            OpenAccMode::O_RDONLY => self.read_open_attr(inode),
            OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR => {
                if self.read_only() {
                    reply.error(Errno::EROFS);
                    return;
                }
                self.live_open_attr(inode)
            }
        };
        let attr = match attr {
            Ok(attr) if attr.file_type == FileType::File => attr,
            Ok(_) => {
                reply.error(Errno::EISDIR);
                return;
            }
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match flags.acc_mode() {
            OpenAccMode::O_RDONLY => match self.allocate_read_handle(attr) {
                Ok(fh) => reply.opened(fh, self.read_open_flags()),
                Err(err) => reply.error(err),
            },
            OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR => {
                let parent = self.parent_of(attr.inode);
                match self.open_write_handle(&attr, parent) {
                    Ok(fh) => reply.opened(fh, self.write_open_flags()),
                    Err(err) => reply.error(err),
                }
            }
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        self.record_fuse_read_request(size);
        if self.writeback_publisher.is_some() {
            // A read with no read/write handle falls through to the metadata
            // service below; block it behind any in-flight async publish so it
            // observes the committed generation, not the pre-write one.
            match self.metadata_inode(ino) {
                Ok(inode) => {
                    if let Err(err) = self.wait_pending_publish(inode) {
                        reply.error(err);
                        return;
                    }
                }
                Err(err) => {
                    reply.error(err);
                    return;
                }
            }
        }
        match self.read_from_read_handle(fh, offset, size) {
            Ok(Some(bytes)) => {
                reply.data(&bytes);
                return;
            }
            Ok(None) => {}
            Err(err) => {
                reply.error(err);
                return;
            }
        }
        match self.read_from_handle(fh, offset, size) {
            Ok(Some(bytes)) => {
                reply.data(&bytes);
                return;
            }
            Ok(None) => {}
            Err(err) => {
                reply.error(err);
                return;
            }
        }
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_read_file(inode, offset, size as usize)
                .map_err(errno)
        }) {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self
            .metadata_inode(ino)
            .and_then(|inode| self.allocate_directory_handle(inode))
        {
            Ok(handle) => reply.opened(handle, self.read_open_flags()),
            Err(err) => reply.error(err),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if let Err(err) = self.directory_handle_attr(fh) {
            reply.error(err);
            return;
        }
        let parent = self.parent_of(inode);
        if self.add_dirent(
            &mut reply,
            offset,
            FUSE_DOT_OFFSET,
            inode,
            FuseFileType::Directory,
            ".",
        ) || self.add_dirent(
            &mut reply,
            offset,
            FUSE_DOT_DOT_OFFSET,
            parent,
            FuseFileType::Directory,
            "..",
        ) {
            reply.ok();
            return;
        }
        let Some(mut index) = child_index_from_offset(offset) else {
            reply.ok();
            return;
        };
        loop {
            let entry = match self.directory_child(fh, index) {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) => {
                    reply.error(err);
                    return;
                }
            };
            let entry_offset = child_offset(index);
            if offset >= entry_offset {
                index = index.saturating_add(1);
                continue;
            }
            if reply.add(
                self.fuse_ino(entry.attr.inode),
                entry_offset,
                fuse_file_type(entry.attr.file_type),
                OsStr::from_bytes(entry.dentry.name.as_bytes()),
            ) {
                break;
            }
            index = index.saturating_add(1);
        }
        reply.ok();
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let attr = match self.directory_handle_attr(fh) {
            Ok(attr) => attr,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if attr.file_type != FileType::Directory {
            reply.error(Errno::ENOTDIR);
            return;
        };
        let parent = self.parent_of(inode);
        let parent_attr = self
            .service_get_attr(parent)
            .ok()
            .flatten()
            .unwrap_or_else(|| attr.clone());
        if self.add_dirent_plus(&mut reply, offset, FUSE_DOT_OFFSET, ".", &attr)
            || self.add_dirent_plus(&mut reply, offset, FUSE_DOT_DOT_OFFSET, "..", &parent_attr)
        {
            reply.ok();
            return;
        }
        let Some(mut index) = child_index_from_offset(offset) else {
            reply.ok();
            return;
        };
        loop {
            let entry = match self.directory_child(fh, index) {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) => {
                    reply.error(err);
                    return;
                }
            };
            let entry_offset = child_offset(index);
            if offset >= entry_offset {
                index = index.saturating_add(1);
                continue;
            }
            if reply.add(
                self.fuse_ino(entry.attr.inode),
                entry_offset,
                OsStr::from_bytes(entry.dentry.name.as_bytes()),
                &self.options.entry_ttl,
                &self.view_file_attr(&entry.attr),
                Generation(entry.attr.generation),
            ) {
                break;
            }
            index = index.saturating_add(1);
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        match self.release_directory_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.sync_directory_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let stat = self.statfs_snapshot();
        reply.statfs(
            stat.blocks,
            stat.bfree,
            stat.bavail,
            stat.files,
            stat.ffree,
            stat.bsize,
            stat.namelen,
            stat.frsize,
        );
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        if position != 0 {
            reply.error(xattr_unsupported_error());
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match xattr_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let mode = match xattr_set_mode(flags) {
            Ok(mode) => mode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.set_xattr(inode, name, value.to_vec(), mode) {
            Ok(()) => reply.ok(),
            Err(FuseBackendError::Metadata(MetadError::Metadata(
                nokv_meta::MetadataError::PredicateFailed,
            ))) if mode == XattrSetMode::Create => reply.error(Errno::EEXIST),
            Err(FuseBackendError::Metadata(MetadError::Metadata(
                nokv_meta::MetadataError::PredicateFailed,
            ))) if mode == XattrSetMode::Replace => reply.error(xattr_missing_error()),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match xattr_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.get_xattr(inode, name) {
            Ok(Some(value)) => reply_xattr_data(value.as_slice(), size, reply),
            Ok(None) => reply.error(xattr_missing_error()),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.list_xattr(inode) {
            Ok(names) => {
                let mut encoded = Vec::new();
                for name in names {
                    encoded.extend_from_slice(&name);
                    encoded.push(0);
                }
                reply_xattr_data(&encoded, size, reply);
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match xattr_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.remove_xattr(inode, name) {
            Ok(()) => reply.ok(),
            Err(FuseBackendError::Metadata(MetadError::Metadata(
                nokv_meta::MetadataError::PredicateFailed,
            ))) => reply.error(xattr_missing_error()),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn access(&self, req: &Request, ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if let Err(err) = validate_access_mask(mask) {
            reply.error(err);
            return;
        }
        match self.metadata_inode(ino).and_then(|inode| {
            self.service_get_attr(inode)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        }) {
            Ok(_) if self.read_only() && mask.contains(AccessFlags::W_OK) => {
                reply.error(Errno::EROFS)
            }
            Ok(attr) if access_allowed(&attr, req.uid(), req.gid(), mask) => reply.ok(),
            Ok(_) => reply.error(Errno::EACCES),
            Err(err) => reply.error(err),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .backend
            .create_dir(parent, name, mode & !umask, req.uid(), req.gid())
        {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let file_type = match file_type_from_mknod_mode(mode) {
            Ok(file_type) => file_type,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let mode = mode & !umask & 0o7777;
        let result = if file_type == FileType::File {
            self.backend
                .create_file(parent, name, mode, req.uid(), req.gid())
        } else {
            self.backend.create_special_node(
                parent,
                name,
                SpecialNodeSpec {
                    file_type,
                    mode,
                    rdev,
                    uid: req.uid(),
                    gid: req.gid(),
                },
            )
        };
        match result {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(link_name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let target = target.as_os_str().as_bytes().to_vec();
        match self
            .backend
            .create_symlink(parent, name, target, 0o777, req.uid(), req.gid())
        {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .backend
            .create_file_prepared(parent, name, mode & !umask, req.uid(), req.gid())
        {
            Ok((entry, prepared)) => {
                match self.allocate_write_handle_with_session(
                    &entry.attr,
                    parent,
                    entry.dentry.name.clone(),
                    Some(prepared),
                ) {
                    Ok(handle) => {
                        self.remember_entry(&entry);
                        reply.created(
                            &self.options.entry_ttl,
                            &self.view_file_attr(&entry.attr),
                            Generation(entry.attr.generation),
                            handle,
                            self.write_open_flags(),
                        );
                    }
                    Err(err) => reply.error(err),
                }
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.remove_file(parent, &name) {
            Ok(entry) => {
                self.forget_inode_cache(entry.attr.inode);
                reply.ok();
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.remove_empty_dir(parent, &name) {
            Ok(entry) => {
                self.forget_inode_cache(entry.attr.inode);
                reply.ok();
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let inode = match self.metadata_inode(ino) {
            Ok(inode) => inode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let new_parent = match self.metadata_inode(newparent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let new_name = match dentry_name(newname) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.link(inode, new_parent, new_name) {
            Ok(entry) => {
                self.remember_entry(&entry);
                reply.entry(
                    &self.options.entry_ttl,
                    &self.view_file_attr(&entry.attr),
                    Generation(entry.attr.generation),
                );
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let mode = match fuse_rename_mode(flags) {
            Ok(mode) => mode,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        let parent = match self.metadata_inode(parent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let newparent = match self.metadata_inode(newparent) {
            Ok(parent) => parent,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let name = match dentry_name(name) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let newname = match dentry_name(newname) {
            Ok(name) => name,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let result = match mode {
            FuseRenameMode::ReplaceIfTargetExists => {
                self.rename_entry(parent, &name, newparent, newname)
            }
            #[cfg(target_os = "linux")]
            FuseRenameMode::NoReplace => self
                .backend
                .rename(parent, &name, newparent, newname)
                .map(|entry| RenameReplaceResult {
                    entry,
                    replaced: None,
                }),
        };
        match result {
            Ok(result) => {
                let entry_inode = result.entry.attr.inode;
                self.remember_entry(&result.entry);
                if let Some(replaced) = result.replaced.filter(|old| old.attr.inode != entry_inode)
                {
                    self.forget_inode_cache(replaced.attr.inode);
                }
                reply.ok();
            }
            Err(err) => reply.error(errno(err)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        match self.write_to_handle(fh, offset, data) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(err),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.ok();
            return;
        }
        match self.publish_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            match self.release_read_handle(fh) {
                Ok(_) => reply.ok(),
                Err(err) => reply.error(err),
            }
            return;
        }
        match self.release_read_handle(fh) {
            Ok(true) => {
                reply.ok();
                return;
            }
            Ok(false) => {}
            Err(err) => {
                reply.error(err);
                return;
            }
        }
        match self.release_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let request = match self.advisory_lock_request(FuseLockRequest {
            ino,
            owner: lock_owner,
            start,
            end,
            typ,
            pid,
            wait: false,
        }) {
            Ok(request) => request,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.get_advisory_lock(request) {
            Ok(Some(lock)) => match advisory_lock_kind_to_fuse(lock.kind) {
                Ok(typ) => reply.locked(lock.start, lock.end, typ, lock.pid),
                Err(err) => reply.error(err),
            },
            Ok(None) => reply.locked(start, end, fuse_lock_type(libc::F_UNLCK), 0),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn setlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let request = match self.advisory_lock_request(FuseLockRequest {
            ino,
            owner: lock_owner,
            start,
            end,
            typ,
            pid,
            wait: sleep,
        }) {
            Ok(request) => request,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.backend.set_advisory_lock(request) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno(err)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.ok();
            return;
        }
        match self.publish_handle(fh) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        match self.ensure_fallocated_range(ino, fh, offset, length, mode) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        _ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        if self.read_only() {
            reply.error(Errno::EROFS);
            return;
        }
        if !flags.is_empty() {
            reply.error(Errno::EOPNOTSUPP);
            return;
        }
        let size = copy_file_range_size(len);
        if size == 0 {
            reply.written(0);
            return;
        }
        let bytes = match self.read_for_copy(ino_in, fh_in, offset_in, size) {
            Ok(bytes) => bytes,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if bytes.is_empty() {
            reply.written(0);
            return;
        }
        match self.write_to_handle(fh_out, offset_out, &bytes) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(err),
        }
    }

    fn lseek(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let size = match self.lseek_file_size(ino, fh) {
            Ok(size) => size,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match resolve_lseek(size, offset, whence) {
            Ok(offset) => reply.offset(offset),
            Err(err) => reply.error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::FuseBackendResult;
    use nokv_object::{
        BlockCacheStats, FileReadPipelineStats, FileWritePipeline, ObjectPrefetchStats,
        ObjectWritebackStats, WritebackCacheStats, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
    };
    use nokv_types::{WatchCursor, WatchRecord};

    #[test]
    fn parent_cache_defaults_to_root_and_remembers_lookup_parent() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        let child = InodeId::new(99).unwrap();
        assert_eq!(fuse.parent_of(child), InodeId::root());
        fuse.remember_parent(child, InodeId::new(9).unwrap());
        assert_eq!(fuse.parent_of(child), InodeId::new(9).unwrap());
    }

    #[test]
    fn snapshot_inode_cache_reconstructs_only_root_relative_components() {
        let root = InodeId::new(42).unwrap();
        let options = FuseOptions {
            view: FuseView::Snapshot {
                snapshot_id: 7,
                root,
            },
            snapshot_root_path: Some("/workbenches/source".to_owned()),
            ..FuseOptions::default()
        };
        let fuse = NoKvFuse::from_backend(UnsupportedTestBackend::default(), options);
        let directory = InodeId::new(43).unwrap();
        let file = InodeId::new(44).unwrap();
        fuse.remember_parent(directory, root);
        fuse.remember_name(directory, &DentryName::new("nested").unwrap());
        fuse.remember_parent(file, directory);
        fuse.remember_name(file, &DentryName::new("checkpoint.bin").unwrap());

        let components = fuse.snapshot_path_components(file).unwrap();
        assert_eq!(
            components
                .iter()
                .map(|component| component.as_bytes())
                .collect::<Vec<_>>(),
            vec![b"nested".as_slice(), b"checkpoint.bin".as_slice()]
        );
        assert!(fuse
            .snapshot_path_components(InodeId::new(99).unwrap())
            .is_err());
    }

    #[test]
    fn statfs_snapshot_reports_nonzero_capacity_and_name_limit() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());

        let stat = fuse.statfs_snapshot();

        assert_eq!(stat.bsize, STATFS_BLOCK_SIZE);
        assert_eq!(stat.frsize, STATFS_BLOCK_SIZE);
        assert_eq!(stat.namelen, STATFS_NAME_MAX);
        assert!(stat.blocks > 0);
        assert!(stat.bfree <= stat.blocks);
        assert!(stat.bavail <= stat.bfree);
        assert!(stat.files > 0);
        assert!(stat.ffree <= stat.files);
    }

    #[test]
    fn open_flags_follow_fuse_cache_options() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        assert_eq!(fuse.read_open_flags(), FopenFlags::FOPEN_KEEP_CACHE);
        assert_eq!(fuse.write_open_flags(), FopenFlags::empty());

        let fuse = NoKvFuse::from_backend(
            UnsupportedTestBackend::default(),
            FuseOptions {
                kernel_cache: false,
                ..FuseOptions::default()
            },
        );
        assert_eq!(fuse.read_open_flags(), FopenFlags::empty());
        assert_eq!(fuse.write_open_flags(), FopenFlags::empty());

        let fuse = NoKvFuse::from_backend(
            UnsupportedTestBackend::default(),
            FuseOptions {
                direct_io: true,
                ..FuseOptions::default()
            },
        );
        assert_eq!(fuse.read_open_flags(), FopenFlags::FOPEN_DIRECT_IO);
        assert_eq!(fuse.write_open_flags(), FopenFlags::FOPEN_DIRECT_IO);
    }

    #[test]
    fn object_pipeline_stats_default_for_backend_without_object_pipeline() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        assert_eq!(
            fuse.object_pipeline_stats().unwrap(),
            FuseObjectPipelineStats::default()
        );
    }

    #[test]
    fn object_pipeline_stats_include_fuse_read_requests() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());

        fuse.record_fuse_read_request(4096);
        fuse.record_fuse_read_request(8192);

        let stats = fuse.object_pipeline_stats().unwrap();
        assert_eq!(stats.fuse_read_requests, 2);
        assert_eq!(stats.fuse_read_request_bytes, 12_288);
    }

    #[test]
    fn object_pipeline_stats_convert_to_object_transfer_stats() {
        let stats = FuseObjectPipelineStats {
            block_cache: Some(BlockCacheStats {
                hits: 2,
                hit_bytes: 20,
                ..BlockCacheStats::default()
            }),
            prefetch: Some(ObjectPrefetchStats {
                enqueued: 3,
                dropped: 1,
                completed: 2,
                failed: 1,
                object_gets: 4,
                object_get_bytes: 40,
                cache_hits: 5,
                cache_hit_bytes: 50,
            }),
            writeback_cache: Some(WritebackCacheStats {
                staged: 6,
                staged_bytes: 60,
                ..WritebackCacheStats::default()
            }),
            writeback: Some(ObjectWritebackStats {
                enqueued: 7,
                inline: 1,
                completed: 3,
                failed: 4,
                staged_bytes: 70,
                uploaded_bytes: 80,
                queue_wait_ns: 90,
                queue_max_wait_ns: 91,
                upload_ns: 100,
                upload_max_ns: 101,
                collect_ns: 102,
                digest_ns: 103,
                store_put_ns: 104,
                cache_put_ns: 105,
            }),
            tiered_object: Some(nokv_object::TieredObjectStoreStats {
                hot_put_ns: 106,
                pending_cold_put_ns: 107,
                cold_put_enqueue_ns: 108,
                ..nokv_object::TieredObjectStoreStats::default()
            }),
            local_hot: Some(nokv_object::LocalObjectStoreStats {
                puts: 2,
                put_bytes: 8192,
                put_total_ns: 109,
                put_prepare_ns: 110,
                put_write_ns: 111,
                put_sync_ns: 112,
                put_rename_ns: 113,
                put_record_ns: 114,
                ..nokv_object::LocalObjectStoreStats::default()
            }),
            fuse_read_requests: 0,
            fuse_read_request_bytes: 0,
            foreground_object_gets: 1,
            foreground_object_get_bytes: 10,
            foreground_coalesced_gets: 2,
            foreground_coalesced_get_bytes: 20,
            foreground_cache_hits: 2,
            foreground_cache_hit_bytes: 20,
            foreground_block_cache_hits: 2,
            foreground_block_cache_hit_bytes: 20,
            foreground_read_window_hits: 0,
            foreground_read_window_hit_bytes: 0,
            read_plan_cache_hits: 8,
            read_plan_cache_misses: 9,
        };

        let object = stats.object_transfer_stats();

        assert_eq!(object.object_gets, 5);
        assert_eq!(object.object_get_bytes, 50);
        assert_eq!(object.coalesced_gets, 2);
        assert_eq!(object.coalesced_get_bytes, 20);
        assert_eq!(object.cache_hits, 7);
        assert_eq!(object.cache_hit_bytes, 70);
        assert_eq!(object.prefetch_enqueued, 3);
        assert_eq!(object.prefetch_object_gets, 4);
        assert_eq!(object.read_plan_cache_hits, 8);
        assert_eq!(object.read_plan_cache_misses, 9);
        assert_eq!(object.object_writeback_enqueued, 7);
        assert_eq!(object.object_writeback_inline, 1);
        assert_eq!(object.object_writeback_completed, 3);
        assert_eq!(object.object_writeback_failed, 4);
        assert_eq!(object.object_writeback_staged_bytes, 70);
        assert_eq!(object.object_writeback_uploaded_bytes, 80);
        assert_eq!(object.object_writeback_queue_wait_ns, 90);
        assert_eq!(object.object_writeback_queue_max_wait_ns, 91);
        assert_eq!(object.object_writeback_upload_ns, 100);
        assert_eq!(object.object_writeback_upload_max_ns, 101);
        assert_eq!(object.object_writeback_collect_ns, 102);
        assert_eq!(object.object_writeback_digest_ns, 103);
        assert_eq!(object.object_writeback_store_put_ns, 104);
        assert_eq!(object.object_writeback_cache_put_ns, 105);
    }

    #[test]
    fn fuse_stats_json_reports_mount_object_counters() {
        let body = fuse_stats_json(&FuseObjectPipelineStats {
            foreground_object_gets: 3,
            foreground_object_get_bytes: 4096,
            foreground_cache_hits: 2,
            foreground_cache_hit_bytes: 2048,
            foreground_read_window_hits: 2,
            foreground_read_window_hit_bytes: 2048,
            read_plan_cache_misses: 1,
            tiered_object: Some(nokv_object::TieredObjectStoreStats {
                hot_put_ns: 12,
                ..nokv_object::TieredObjectStoreStats::default()
            }),
            local_hot: Some(nokv_object::LocalObjectStoreStats {
                puts: 1,
                put_total_ns: 34,
                put_write_ns: 21,
                ..nokv_object::LocalObjectStoreStats::default()
            }),
            ..FuseObjectPipelineStats::default()
        });

        assert!(body.contains("\"source\":\"fuse\""));
        assert!(body.contains("\"fuse_read_requests\":0"));
        assert!(body.contains("\"fuse_read_request_bytes\":0"));
        assert!(body.contains("\"object_gets\":3"));
        assert!(body.contains("\"object_get_bytes\":4096"));
        assert!(body.contains("\"cache_hits\":2"));
        assert!(body.contains("\"read_window_hits\":2"));
        assert!(body.contains("\"read_plan_cache_misses\":1"));
        assert!(body.contains("\"tiered_hot_put_ns\":12"));
        assert!(body.contains("\"local_hot_puts\":1"));
        assert!(body.contains("\"local_hot_put_total_ns\":34"));
        assert!(body.contains("\"local_hot_put_write_ns\":21"));
    }

    #[test]
    fn staged_range_block_count_matches_chunked_object_boundaries() {
        assert_eq!(staged_range_block_count(0, 0).unwrap(), 0);
        assert_eq!(staged_range_block_count(0, 1).unwrap(), 1);
        assert_eq!(
            staged_range_block_count(0, DEFAULT_BLOCK_SIZE + 1).unwrap(),
            2
        );
        assert_eq!(
            staged_range_block_count(DEFAULT_CHUNK_SIZE - 1, 2).unwrap(),
            2
        );
    }

    #[test]
    fn manifest_digest_is_stable_and_manifest_scoped() {
        let chunks = vec![nokv_object::StoredChunk {
            chunk_index: 0,
            logical_offset: 0,
            len: 4,
            blocks: vec![nokv_object::StoredBlock {
                object_key: "blocks/1/2/3/0/0".to_owned(),
                logical_offset: 0,
                object_offset: 0,
                len: 4,
                digest_uri: "sha256:block".to_owned(),
            }],
        }];

        let first = manifest_digest_uri(4, 3, &chunks);
        let second = manifest_digest_uri(4, 3, &chunks);
        assert_eq!(first, second);
        assert!(first.starts_with("manifest-sha256:"));
        assert_ne!(first, manifest_digest_uri(4, 4, &chunks));

        let mut changed = chunks.clone();
        changed[0].blocks[0].digest_uri = "sha256:changed".to_owned();
        assert_ne!(first, manifest_digest_uri(4, 3, &changed));
    }

    #[test]
    fn buffered_writes_coalesce_until_upload_threshold_is_ready() {
        let mut buffered = Vec::new();
        push_buffered_write(
            &mut buffered,
            0,
            &vec![1; FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2],
        );
        assert!(!has_buffered_upload_ready(&buffered, false));
        assert!(has_buffered_upload_ready(&buffered, true));
        assert!(take_buffered_upload_ranges(&mut buffered, false)
            .unwrap()
            .is_empty());

        push_buffered_write(
            &mut buffered,
            (FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2) as u64,
            &vec![2; FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2],
        );
        assert!(has_buffered_upload_ready(&buffered, false));
        let upload = take_buffered_upload_ranges(&mut buffered, false).unwrap();

        assert!(buffered.is_empty());
        assert_eq!(upload.len(), 1);
        assert_eq!(upload[0].offset, 0);
        assert_eq!(upload[0].bytes.len(), FUSE_WRITEBACK_UPLOAD_THRESHOLD);
        assert_eq!(upload[0].bytes[0], 1);
        assert_eq!(upload[0].bytes[FUSE_WRITEBACK_UPLOAD_THRESHOLD / 2], 2);
    }

    #[test]
    fn buffered_upload_keeps_tail_until_flush() {
        let mut buffered = Vec::new();
        push_buffered_write(
            &mut buffered,
            0,
            &vec![7; FUSE_WRITEBACK_UPLOAD_THRESHOLD + 17],
        );

        assert!(has_buffered_upload_ready(&buffered, false));
        let upload = take_buffered_upload_ranges(&mut buffered, false).unwrap();
        assert_eq!(upload.len(), 1);
        assert_eq!(upload[0].bytes.len(), FUSE_WRITEBACK_UPLOAD_THRESHOLD);
        assert_eq!(buffered.len(), 1);
        assert_eq!(buffered[0].offset, FUSE_WRITEBACK_UPLOAD_THRESHOLD as u64);
        assert_eq!(buffered[0].bytes.len(), 17);

        assert!(!has_buffered_upload_ready(&buffered, false));
        assert!(has_buffered_upload_ready(&buffered, true));
        let tail = take_buffered_upload_ranges(&mut buffered, true).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].offset, FUSE_WRITEBACK_UPLOAD_THRESHOLD as u64);
        assert_eq!(tail[0].bytes.len(), 17);
        assert!(buffered.is_empty());
    }

    #[test]
    fn buffered_write_overwrites_in_place_for_replayed_offset() {
        // FUSE writeback can re-dispatch an earlier, still-buffered offset. The
        // overlapping write must overwrite in place, not append a duplicate range.
        let mut buffered = Vec::new();
        push_buffered_write(&mut buffered, 0, &[1, 1, 1, 1, 1, 1, 1, 1]);
        push_buffered_write(&mut buffered, 2, &[9, 9]);
        assert_eq!(buffered.len(), 1, "must stay a single coalesced range");
        assert_eq!(buffered[0].offset, 0);
        assert_eq!(buffered[0].bytes, vec![1, 1, 9, 9, 1, 1, 1, 1]);
    }

    #[test]
    fn buffered_write_preserves_holes_without_fabricating_zeros() {
        // Two disjoint ranges with a genuine gap, then a write touching both ends.
        // The gap must remain a hole, never be emitted as fabricated zero bytes.
        let mut buffered = Vec::new();
        push_buffered_write(&mut buffered, 0, &[1, 1]);
        push_buffered_write(&mut buffered, 100, &[2, 2]);
        // Write that abuts the first range and the second; the [2,100) hole stays.
        push_buffered_write(&mut buffered, 2, &[3, 3]);
        assert_eq!(buffered.len(), 2);
        assert_eq!(buffered[0].offset, 0);
        assert_eq!(buffered[0].bytes, vec![1, 1, 3, 3]);
        assert_eq!(buffered[1].offset, 100);
        assert_eq!(buffered[1].bytes, vec![2, 2]);
        let total: usize = buffered.iter().map(|r| r.bytes.len()).sum();
        assert_eq!(total, 6, "no zero-fill bytes introduced for the hole");
    }

    #[test]
    fn select_unstaged_skips_already_staged_offsets() {
        let block = DEFAULT_BLOCK_SIZE;
        let mut staged = std::collections::HashSet::new();

        // First stage of two distinct blocks: both are new, and they stay as
        // separate block-sized ranges instead of being stitched into a larger
        // buffer that the object layer would split again.
        let first = vec![BufferedWriteRange {
            offset: 0,
            bytes: {
                let mut b = vec![7_u8; block];
                b.extend(vec![8_u8; block]);
                b
            },
        }];
        let out = select_unstaged_blocks(first, &mut staged).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].logical_offset, 0);
        assert_eq!(out[0].bytes.len(), block);
        assert_eq!(out[1].logical_offset, block as u64);
        assert_eq!(out[1].bytes.len(), block);
        assert_eq!(staged.len(), 2);

        // A FUSE re-dispatch re-sends both already-staged offsets plus one new
        // block past the high-water mark. Only the new block survives; the
        // re-dispatched offsets are skipped on offset identity alone (no hashing).
        let replay = vec![
            BufferedWriteRange {
                offset: 0,
                bytes: vec![7_u8; block],
            },
            BufferedWriteRange {
                offset: block as u64,
                bytes: vec![8_u8; block],
            },
            BufferedWriteRange {
                offset: 2 * block as u64,
                bytes: vec![9_u8; block],
            },
        ];
        let out = select_unstaged_blocks(replay, &mut staged).unwrap();
        assert_eq!(out.len(), 1, "only the not-yet-staged offset survives");
        assert_eq!(out[0].logical_offset, 2 * block as u64);
        assert_eq!(out[0].bytes.as_slice(), vec![9_u8; block].as_slice());
        assert_eq!(staged.len(), 3);
    }

    #[test]
    fn select_unstaged_moves_single_block_without_copying() {
        let mut staged = std::collections::HashSet::new();
        let bytes = vec![11_u8; DEFAULT_BLOCK_SIZE];
        let ptr = bytes.as_ptr();

        let out =
            select_unstaged_blocks(vec![BufferedWriteRange { offset: 0, bytes }], &mut staged)
                .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].logical_offset, 0);
        assert_eq!(out[0].bytes.len(), DEFAULT_BLOCK_SIZE);
        assert_eq!(out[0].bytes.as_ptr(), ptr);
    }

    #[test]
    fn failed_pending_writeback_restores_dirty_range_for_retry() {
        let backend = UnsupportedTestBackend::default();
        let fuse = NoKvFuse::from_backend(backend.clone(), FuseOptions::default());
        let inode = InodeId::new(7).unwrap();
        let fh = fuse
            .allocate_write_handle(
                &InodeAttr {
                    inode,
                    file_type: FileType::File,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    nlink: 1,
                    size: 0,
                    generation: 1,
                    mtime_ms: 1,
                    ctime_ms: 1,
                },
                InodeId::root(),
                DentryName::new("checkpoint.bin").unwrap(),
            )
            .unwrap();

        {
            let mut handles = fuse.write_handles.write().unwrap();
            let handle = handles.get_mut(&fh.0).unwrap();
            handle.prepared = Some(());
            handle.size = 10;
            handle.dirty = true;
            handle.writer = Some(
                FileWritePipeline::new(nokv_object::ChunkWriteOptions {
                    manifest_id: "fuse/1/7".to_owned(),
                    mount: 1,
                    inode: inode.get(),
                    generation: 1,
                    chunk_size: DEFAULT_CHUNK_SIZE,
                    block_size: DEFAULT_BLOCK_SIZE,
                })
                .unwrap(),
            );
            handle.pending_uploads.push(PendingBufferedUpload {
                pending: PendingChunkedWrite::ready(Err(ObjectError::Backend(
                    "temporary object outage".to_owned(),
                ))),
                ranges: vec![PendingBufferedRange::from_buffered_owned(
                    BufferedWriteRange {
                        offset: 0,
                        bytes: b"checkpoint".to_vec(),
                    },
                )],
            });
        }

        assert_eq!(
            fuse.drain_handle_uploads(fh).unwrap_err().code(),
            Errno::EIO.code()
        );
        let handles = fuse.write_handles.read().unwrap();
        let handle = handles.get(&fh.0).unwrap();
        assert!(handle.pending_uploads.is_empty());
        assert_eq!(handle.buffered.len(), 1);
        assert_eq!(handle.buffered[0].offset, 0);
        assert_eq!(handle.buffered[0].bytes, b"checkpoint");
        assert!(handle.writer.as_ref().unwrap().staged_chunks().is_empty());
        drop(handles);

        fuse.publish_handle(fh).unwrap();
        let published_digests = backend.published_digests();
        assert_eq!(published_digests.len(), 1);
        assert!(
            published_digests[0].starts_with("manifest-sha256:"),
            "{published_digests:?}"
        );
        assert_eq!(fuse.cached_attr(inode).unwrap().size, 10);

        let handles = fuse.write_handles.read().unwrap();
        let handle = handles.get(&fh.0).unwrap();
        assert!(handle.buffered.is_empty());
        assert!(handle.pending_uploads.is_empty());
        assert!(handle.writer.is_none());
        assert!(!handle.dirty);
        assert_eq!(handle.base_size, 10);
    }

    #[test]
    fn attr_reflects_in_flight_write_handle_size() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        let inode = InodeId::new(7).unwrap();
        let committed = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 1,
            size: 0,
            generation: 1,
            mtime_ms: 1,
            ctime_ms: 1,
        };
        let fh = fuse
            .allocate_write_handle(
                &committed,
                InodeId::root(),
                DentryName::new("growing.bin").unwrap(),
            )
            .unwrap();

        // Before any dirty write, the committed size is reported unchanged.
        assert_eq!(fuse.attr_with_pending_size(committed.clone()).size, 0);
        assert_eq!(fuse.pending_write_size(inode), None);

        {
            let mut handles = fuse.write_handles.write().unwrap();
            let handle = handles.get_mut(&fh.0).unwrap();
            handle.size = 4096;
            handle.dirty = true;
        }

        // While a write is in flight, attrs report the larger in-flight size so the
        // kernel does not treat the freshly written bytes as a hole to zero-fill.
        assert_eq!(fuse.pending_write_size(inode), Some(4096));
        assert_eq!(fuse.attr_with_pending_size(committed).size, 4096);
    }

    #[test]
    fn read_from_write_handle_overlays_pending_upload_without_waiting() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        let inode = InodeId::new(7).unwrap();
        let fh = fuse
            .allocate_write_handle(
                &InodeAttr {
                    inode,
                    file_type: FileType::File,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    nlink: 1,
                    size: 0,
                    generation: 1,
                    mtime_ms: 1,
                    ctime_ms: 1,
                },
                InodeId::root(),
                DentryName::new("checkpoint.bin").unwrap(),
            )
            .unwrap();

        {
            let mut handles = fuse.write_handles.write().unwrap();
            let handle = handles.get_mut(&fh.0).unwrap();
            handle.size = 10;
            handle.dirty = true;
            handle.pending_uploads.push(PendingBufferedUpload {
                pending: PendingChunkedWrite::ready(Err(ObjectError::Backend(
                    "upload should not be awaited by read".to_owned(),
                ))),
                ranges: vec![PendingBufferedRange::from_buffered_owned(
                    BufferedWriteRange {
                        offset: 0,
                        bytes: b"checkpoint".to_vec(),
                    },
                )],
            });
        }

        assert_eq!(
            fuse.read_from_handle(fh, 0, 10).unwrap().unwrap(),
            b"checkpoint"
        );
        let handles = fuse.write_handles.read().unwrap();
        assert_eq!(handles.get(&fh.0).unwrap().pending_uploads.len(), 1);
    }

    #[test]
    fn read_handle_uses_pipeline_and_is_released() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        let fh = fuse.allocate_read_handle(test_file_attr(7)).unwrap();

        let first = fuse.read_from_read_handle(fh, 0, 4).unwrap().unwrap();
        let second = fuse.read_from_read_handle(fh, 4, 4).unwrap().unwrap();

        assert_eq!(first, b"abcd");
        assert_eq!(second, b"efgh");
        assert!(fuse.release_read_handle(fh).unwrap());
        assert!(!fuse.release_read_handle(fh).unwrap());
        assert!(fuse.read_from_read_handle(fh, 0, 4).unwrap().is_none());
    }

    #[test]
    fn read_handle_pipeline_state_is_per_handle() {
        let backend = UnsupportedTestBackend::default();
        let fuse = NoKvFuse::from_backend(backend.clone(), FuseOptions::default());
        let first_fh = fuse.allocate_read_handle(test_file_attr(7)).unwrap();
        let second_fh = fuse.allocate_read_handle(test_file_attr(8)).unwrap();

        assert_eq!(
            fuse.read_from_read_handle(first_fh, 0, 4).unwrap().unwrap(),
            b"abcd"
        );
        assert_eq!(
            fuse.read_from_read_handle(first_fh, 4, 4).unwrap().unwrap(),
            b"efgh"
        );
        assert_eq!(
            fuse.read_from_read_handle(second_fh, 0, 4)
                .unwrap()
                .unwrap(),
            b"abcd"
        );
        assert_eq!(
            fuse.read_from_read_handle(second_fh, 4, 4)
                .unwrap()
                .unwrap(),
            b"efgh"
        );

        let stats = backend.read_stats();
        assert_eq!(stats.len(), 4);
        assert_eq!(stats[1].0, 7);
        assert_eq!(stats[1].1.reads, 2);
        assert_eq!(stats[1].1.sequential_reads, 2);
        assert_eq!(stats[3].0, 8);
        assert_eq!(stats[3].1.reads, 2);
        assert_eq!(stats[3].1.sequential_reads, 2);
    }

    #[test]
    fn read_handle_bypasses_pipeline_for_non_stateful_reads() {
        let backend = UnsupportedTestBackend::default();
        let fuse = NoKvFuse::from_backend(backend.clone(), FuseOptions::default());
        let fh = fuse.allocate_read_handle(test_file_attr(7)).unwrap();

        assert_eq!(
            fuse.read_from_read_handle(fh, 4, 4).unwrap().unwrap(),
            b"efgh"
        );
        assert!(
            backend.read_stats().is_empty(),
            "a first middle-of-file read should use the concurrent direct path"
        );

        assert_eq!(
            fuse.read_from_read_handle(fh, 8, 4).unwrap().unwrap(),
            b"ijkl"
        );
        let stats = backend.read_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].0, 7);
        assert_eq!(stats[0].1.reads, 1);
        assert_eq!(stats[0].1.sequential_reads, 1);
    }

    #[test]
    fn read_handle_skips_pipeline_when_prefetch_disabled() {
        let backend = UnsupportedTestBackend::default();
        let fuse = NoKvFuse::from_backend(
            backend.clone(),
            FuseOptions {
                prefetch: FusePrefetchOptions {
                    enabled: false,
                    ..FusePrefetchOptions::default()
                },
                ..FuseOptions::default()
            },
        );
        let fh = fuse.allocate_read_handle(test_file_attr(7)).unwrap();

        assert_eq!(
            fuse.read_from_read_handle(fh, 0, 4).unwrap().unwrap(),
            b"abcd"
        );
        assert_eq!(
            fuse.read_from_read_handle(fh, 4, 4).unwrap().unwrap(),
            b"efgh"
        );
        assert!(
            backend.read_stats().is_empty(),
            "no-prefetch reads should bypass FileReadPipeline state"
        );
    }

    #[test]
    fn read_handle_clamps_read_past_eof_to_short_read() {
        // A read that starts before EOF (size 16) but whose length runs past it
        // must come back as a short read, not a backend range error. Exercised on
        // both the prefetch-disabled and pipeline paths.
        for prefetch_enabled in [false, true] {
            let fuse = NoKvFuse::from_backend(
                UnsupportedTestBackend::default(),
                FuseOptions {
                    prefetch: FusePrefetchOptions {
                        enabled: prefetch_enabled,
                        ..FusePrefetchOptions::default()
                    },
                    ..FuseOptions::default()
                },
            );
            let fh = fuse.allocate_read_handle(test_file_attr(7)).unwrap();
            // offset 14 + size 8 would reach 22 > size 16 without clamping.
            let bytes = fuse.read_from_read_handle(fh, 14, 8).unwrap().unwrap();
            assert_eq!(bytes, b"op", "prefetch_enabled={prefetch_enabled}");
            // A read fully at/after EOF is an empty short read, never an error.
            assert!(fuse
                .read_from_read_handle(fh, 16, 8)
                .unwrap()
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn attr_cache_tracks_entries_and_inode_forgets() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        let attr = test_file_attr(7);
        let name = DentryName::new("checkpoint.bin").unwrap();
        let entry = DentryWithAttr {
            dentry: nokv_types::DentryRecord {
                parent: InodeId::root(),
                name: name.clone(),
                child: attr.inode,
                child_type: FileType::File,
                attr_generation: attr.generation,
            },
            attr: attr.clone(),
            body: None,
        };

        fuse.remember_entry(&entry);
        assert_eq!(fuse.cached_attr(attr.inode), Some(attr.clone()));
        assert_eq!(fuse.name_of(attr.inode).unwrap(), name);

        fuse.forget_attr_cache(attr.inode);
        assert_eq!(fuse.cached_attr(attr.inode), None);
        assert_eq!(fuse.name_of(attr.inode).unwrap(), entry.dentry.name);

        fuse.remember_attr(&attr);
        fuse.forget_inode_cache(attr.inode);
        assert_eq!(fuse.cached_attr(attr.inode), None);
        assert_eq!(
            fuse.name_of(attr.inode).unwrap_err().code(),
            Errno::EIO.code()
        );
    }

    #[test]
    fn local_invalidation_drops_cached_attr() {
        let fuse =
            NoKvFuse::from_backend(UnsupportedTestBackend::default(), FuseOptions::default());
        let attr = test_file_attr(9);
        fuse.remember_attr(&attr);

        let invalidate = fuse.local_invalidation();
        invalidate(crate::invalidation::InvalidationTarget {
            parent: Some(InodeId::root()),
            name: Some(DentryName::new("shard-0009.bin").unwrap()),
            inode: attr.inode,
        });

        assert_eq!(fuse.cached_attr(attr.inode), None);
    }

    fn test_file_attr(raw_inode: u64) -> InodeAttr {
        InodeAttr {
            inode: InodeId::new(raw_inode).unwrap(),
            file_type: FileType::File,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 1,
            size: 16,
            generation: 1,
            mtime_ms: 1,
            ctime_ms: 1,
        }
    }

    #[test]
    fn rename_flags_select_supported_modes() {
        assert_eq!(
            fuse_rename_mode(RenameFlags::empty()).unwrap(),
            FuseRenameMode::ReplaceIfTargetExists
        );

        #[cfg(target_os = "linux")]
        {
            assert_eq!(
                fuse_rename_mode(RenameFlags::RENAME_NOREPLACE).unwrap(),
                FuseRenameMode::NoReplace
            );
            assert_eq!(
                fuse_rename_mode(RenameFlags::RENAME_EXCHANGE)
                    .unwrap_err()
                    .code(),
                Errno::EINVAL.code()
            );
            assert_eq!(
                fuse_rename_mode(RenameFlags::RENAME_NOREPLACE | RenameFlags::RENAME_EXCHANGE)
                    .unwrap_err()
                    .code(),
                Errno::EINVAL.code()
            );
        }

        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(
                fuse_rename_mode(RenameFlags::from_bits_retain(1))
                    .unwrap_err()
                    .code(),
                Errno::EINVAL.code()
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mount_options_disable_appledouble_sidecars() {
        let options = mount_options(&FuseOptions::default());
        let debug = format!("{options:?}");
        assert!(debug.contains("noappledouble"));
        assert!(!debug.contains("direct_io"));
        assert!(!debug.contains("iosize="));
        // `noapplexattr` must NOT be set: it makes macFUSE reject `com.apple.*`
        // xattrs with EPERM, which breaks `cp` (it copies `com.apple.provenance`).
        assert!(!debug.contains("noapplexattr"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_direct_io_sets_mount_io_size() {
        let options = mount_options(&FuseOptions {
            direct_io: true,
            ..FuseOptions::default()
        });
        let debug = format!("{options:?}");
        assert!(debug.contains("direct_io"));
        assert!(debug.contains("iosize=1048576"));
    }

    #[derive(Clone, Default)]
    struct UnsupportedTestBackend {
        published_digests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        read_stats: std::sync::Arc<std::sync::Mutex<Vec<(u64, FileReadPipelineStats)>>>,
    }

    impl UnsupportedTestBackend {
        fn published_digests(&self) -> Vec<String> {
            self.published_digests.lock().unwrap().clone()
        }

        fn read_stats(&self) -> Vec<(u64, FileReadPipelineStats)> {
            self.read_stats.lock().unwrap().clone()
        }
    }

    impl FuseBackend for UnsupportedTestBackend {
        type Prepared = ();

        fn prepared_generation(&self, _prepared: &Self::Prepared) -> u64 {
            1
        }

        fn prepared_record_fields(
            &self,
            _prepared: &Self::Prepared,
        ) -> crate::backend::PreparedRecordFields {
            crate::backend::PreparedRecordFields {
                mount: 1,
                generation: 1,
                mtime_ms: 0,
                ctime_ms: 0,
                replace: false,
                dentry_version: None,
                old_generation: None,
            }
        }

        fn prepared_is_replace(&self, _prepared: &Self::Prepared) -> bool {
            false
        }

        fn rebind_prepared_dentry_version(&self, _prepared: &mut Self::Prepared, _version: u64) {}

        fn watch_subtree(&self, _scope: InodeId) -> FuseBackendResult<Option<WatchCursor>> {
            unsupported()
        }

        fn replay_watch(
            &self,
            _scope: InodeId,
            _cursor: WatchCursor,
            _limit: usize,
        ) -> FuseBackendResult<Vec<WatchRecord>> {
            unsupported()
        }

        fn get_attr(&self, _inode: InodeId) -> FuseBackendResult<Option<InodeAttr>> {
            unsupported()
        }

        fn get_attr_at_snapshot(
            &self,
            _snapshot_id: u64,
            _path_components: &[DentryName],
        ) -> FuseBackendResult<Option<InodeAttr>> {
            unsupported()
        }

        fn lookup_plus(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<Option<DentryWithAttr>> {
            unsupported()
        }

        fn current_dentry_version(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<Option<u64>> {
            unsupported()
        }

        fn lookup_plus_at_snapshot(
            &self,
            _snapshot_id: u64,
            _parent_components: &[DentryName],
            _name: &DentryName,
        ) -> FuseBackendResult<Option<DentryWithAttr>> {
            unsupported()
        }

        fn read_dir_plus_page(
            &self,
            _inode: InodeId,
            _after: Option<&DentryName>,
            _limit: usize,
        ) -> FuseBackendResult<ReadDirPlusPage> {
            unsupported()
        }

        fn read_dir_plus_at_snapshot(
            &self,
            _snapshot_id: u64,
            _path_components: &[DentryName],
        ) -> FuseBackendResult<Vec<DentryWithAttr>> {
            unsupported()
        }

        fn rename(
            &self,
            _parent: InodeId,
            _name: &DentryName,
            _new_parent: InodeId,
            _new_name: DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn rename_replace(
            &self,
            _parent: InodeId,
            _name: &DentryName,
            _new_parent: InodeId,
            _new_name: DentryName,
        ) -> FuseBackendResult<RenameReplaceResult> {
            unsupported()
        }

        fn read_file(
            &self,
            _inode: InodeId,
            _offset: u64,
            _len: usize,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn read_file_with_known_attr(
            &self,
            attr: &InodeAttr,
            offset: u64,
            len: usize,
        ) -> FuseBackendResult<Vec<u8>> {
            let data = b"abcdefghijklmnop";
            if len == 0 || offset >= attr.size {
                return Ok(Vec::new());
            }
            let start = usize::try_from(offset).map_err(|_| ObjectError::InvalidRange)?;
            let end = start.checked_add(len).ok_or(ObjectError::InvalidRange)?;
            if end > data.len() {
                return Err(ObjectError::InvalidRange.into());
            }
            Ok(data[start..end].to_vec())
        }

        fn read_file_with_known_attr_pipeline(
            &self,
            attr: &InodeAttr,
            offset: u64,
            len: usize,
            pipeline: &mut FileReadPipeline,
            _read_plans: &mut ObjectReadPlanCache,
        ) -> FuseBackendResult<Vec<u8>> {
            let inode = attr.inode;
            let store = nokv_object::MemoryObjectStore::new();
            let key = nokv_object::ObjectKey::new("blocks/test/read").unwrap();
            store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
            let blocks = [ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                digest_uri: "sha256:test".to_owned(),
                object_offset: offset,
                object_len: 16,
                len,
                output_offset: 0,
            }];
            let outcome = pipeline
                .read_blocks_with_options::<_, nokv_object::MemoryBlockCache>(
                    &store,
                    None,
                    nokv_object::FileReadRequest {
                        file_size: 16,
                        offset,
                        output_len: len,
                        blocks: &blocks,
                    },
                    nokv_object::BlockReadOptions::default(),
                )
                .unwrap();
            self.read_stats
                .lock()
                .unwrap()
                .push((inode.get(), pipeline.stats()));
            Ok(outcome.blocks.bytes)
        }

        fn read_file_at_snapshot(
            &self,
            _snapshot_id: u64,
            _path_components: &[DentryName],
            _offset: u64,
            _len: usize,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn read_symlink(&self, _inode: InodeId) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn read_symlink_at_snapshot(
            &self,
            _snapshot_id: u64,
            _path_components: &[DentryName],
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn update_root_attrs(&self, _changes: UpdateAttr) -> FuseBackendResult<InodeAttr> {
            unsupported()
        }

        fn update_attrs(
            &self,
            _parent: InodeId,
            _name: &DentryName,
            _changes: UpdateAttr,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn set_xattr(
            &self,
            _inode: InodeId,
            _name: &[u8],
            _value: Vec<u8>,
            _mode: XattrSetMode,
        ) -> FuseBackendResult<()> {
            unsupported()
        }

        fn get_xattr(&self, _inode: InodeId, _name: &[u8]) -> FuseBackendResult<Option<Vec<u8>>> {
            unsupported()
        }

        fn list_xattr(&self, _inode: InodeId) -> FuseBackendResult<Vec<Vec<u8>>> {
            unsupported()
        }

        fn remove_xattr(&self, _inode: InodeId, _name: &[u8]) -> FuseBackendResult<()> {
            unsupported()
        }

        fn get_advisory_lock(
            &self,
            _request: AdvisoryLockRequest,
        ) -> FuseBackendResult<Option<nokv_types::AdvisoryLock>> {
            unsupported()
        }

        fn set_advisory_lock(&self, _request: AdvisoryLockRequest) -> FuseBackendResult<()> {
            unsupported()
        }

        fn create_dir(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn create_file(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn create_file_prepared(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<(DentryWithAttr, Self::Prepared)> {
            unsupported()
        }

        fn create_symlink(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _target: Vec<u8>,
            _mode: u32,
            _uid: u32,
            _gid: u32,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn create_special_node(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _spec: SpecialNodeSpec,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn link(
            &self,
            _inode: InodeId,
            _new_parent: InodeId,
            _new_name: DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn remove_file(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn remove_empty_dir(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<DentryWithAttr> {
            unsupported()
        }

        fn prepare_artifact_replace(
            &self,
            _parent: InodeId,
            _name: DentryName,
        ) -> FuseBackendResult<Self::Prepared> {
            unsupported()
        }

        fn new_write_pipeline(
            &self,
            _prepared: &Self::Prepared,
            manifest_id: &str,
        ) -> FuseBackendResult<FileWritePipeline> {
            FileWritePipeline::new(nokv_object::ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: 1,
                inode: 7,
                generation: 1,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            })
            .map_err(FuseBackendError::Object)
        }

        fn stage_prepared_artifact_shared_ranges_async(
            &self,
            _prepared: &Self::Prepared,
            manifest_id: &str,
            ranges: &[PendingBufferedRange],
            block_index_base: u64,
        ) -> FuseBackendResult<PendingChunkedWrite> {
            let mut blocks = Vec::new();
            let mut size = 0_u64;
            for (index, range) in ranges.iter().enumerate() {
                if range.is_empty() {
                    continue;
                }
                let len = range.len() as u64;
                size = size.max(range.offset.saturating_add(len));
                blocks.push(nokv_object::StoredBlock {
                    object_key: format!(
                        "blocks/test/{}",
                        block_index_base.saturating_add(index as u64)
                    ),
                    logical_offset: range.offset,
                    object_offset: 0,
                    len,
                    digest_uri: format!("sha256:test-{index}"),
                });
            }
            Ok(PendingChunkedWrite::ready(Ok(nokv_object::ChunkedWrite {
                manifest_id: manifest_id.to_owned(),
                size,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
                chunks: vec![nokv_object::StoredChunk {
                    chunk_index: 0,
                    logical_offset: 0,
                    len: size,
                    blocks,
                }],
                object_puts: ranges.len(),
                object_put_bytes: ranges.iter().map(|range| range.len() as u64).sum(),
            })))
        }

        fn restage_cached_blocks(
            &self,
            _prepared: &Self::Prepared,
            _manifest_id: &str,
            _blocks: &[crate::backend::RecoveredBlock],
            _block_index_base: u64,
        ) -> FuseBackendResult<PendingChunkedWrite> {
            unsupported()
        }

        fn prepared_from_record_fields(
            &self,
            _parent: InodeId,
            _name: DentryName,
            _inode: InodeId,
            _fields: crate::backend::PreparedRecordFields,
        ) -> Self::Prepared {
        }

        fn purge_cache_orphans(
            &self,
            _live_file_names: &std::collections::HashSet<String>,
        ) -> FuseBackendResult<usize> {
            Ok(0)
        }

        fn sync_writeback_root(&self) -> FuseBackendResult<()> {
            Ok(())
        }

        fn cleanup_staged_objects(
            &self,
            _staged: &nokv_object::StagedObjectSet,
        ) -> FuseBackendResult<()> {
            Ok(())
        }

        fn read_session_object_blocks(
            &self,
            _output_len: usize,
            _blocks: &[ObjectReadBlock],
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }

        fn publish_prepared_artifact_staged_session(
            &self,
            _prepared: Self::Prepared,
            request: PublishArtifactStagedSession,
        ) -> FuseBackendResult<RenameReplaceResult> {
            self.published_digests
                .lock()
                .unwrap()
                .push(request.digest_uri.clone());
            let inode = InodeId::new(7).unwrap();
            let attr = InodeAttr {
                inode,
                file_type: FileType::File,
                mode: request.mode,
                uid: request.uid,
                gid: request.gid,
                rdev: 0,
                nlink: 1,
                size: request.size,
                generation: 1,
                mtime_ms: 1,
                ctime_ms: 1,
            };
            Ok(RenameReplaceResult {
                entry: DentryWithAttr {
                    dentry: nokv_types::DentryRecord {
                        parent: request.parent,
                        name: request.name,
                        child: inode,
                        child_type: FileType::File,
                        attr_generation: attr.generation,
                    },
                    attr,
                    body: None,
                },
                replaced: None,
            })
        }
    }

    fn unsupported<T>() -> FuseBackendResult<T> {
        Err(FuseBackendError::Metadata(MetadError::Codec(
            "test backend".to_owned(),
        )))
    }
}
