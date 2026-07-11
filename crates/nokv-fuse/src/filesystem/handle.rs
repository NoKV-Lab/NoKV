// File-handle I/O and lifecycle helpers for NoKvFuse, split out of
// filesystem.rs to keep that file focused on the Filesystem trait impl.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use fuser::{
    Errno, FileAttr, FileHandle, FileType as FuseFileType, FopenFlags, Generation, INodeNo,
    ReplyDirectory, ReplyDirectoryPlus,
};
use nokv_meta::{
    DentryWithAttr, MetadError, PublishArtifactStagedSession, ReadDirPlusPage, RenameReplaceResult,
};
use nokv_object::{
    chunk_manifests_from_stored_chunks, manifest_digest_uri, DirtyChunkExtent, FileReadPipeline,
    ObjectError, ObjectReadBlock, PendingChunkedWrite, StagedObjectSet, StoredChunk,
};
use nokv_types::{AdvisoryLockRequest, DentryName, FileType, InodeAttr, InodeId};

use crate::attr::file_attr;
use crate::backend::{FuseBackend, FuseBackendError};
use crate::invalidation::{InvalidationRegistry, InvalidationTarget, LocalInvalidation};

use super::directory::{DirectoryHandle, FUSE_READDIR_PAGE_SIZE};
use super::errors::errno;
use super::locks::{advisory_lock_kind_from_fuse, FuseLockRequest};
#[cfg(test)]
use super::merge_fuse_read_stats;
#[cfg(test)]
use super::options::FuseObjectPipelineStats;
use super::options::{FuseOptions, FuseView};
use super::options::{
    FuseStatfs, STATFS_BLOCK_SIZE, STATFS_NAME_MAX, STATFS_TOTAL_BYTES, STATFS_TOTAL_FILES,
};
use super::posix::{inode_id, resolve_fallocate_size};
use super::publish_journal::{CacheFileRef, PendingPublishRecord};
use super::publisher::PendingPublish;
use super::write_session::{
    cleanup_written_objects, forget_staged_blocks, fuse_manifest_id, has_buffered_upload_ready,
    push_buffered_write, select_unstaged_blocks, take_buffered_upload_ranges, BufferedWriteRange,
    PendingBufferedRange, PendingBufferedUpload, WriteHandle, WriteStageReservation,
};
use super::{FuseReadStats, NoKvFuse, ReadHandle, ReadHandleState};

impl<B> NoKvFuse<B>
where
    B: FuseBackend,
{
    #[cfg(test)]
    pub(crate) fn from_backend(backend: B, options: FuseOptions) -> Self {
        Self::from_shared_backend(Arc::new(backend), options)
    }

    pub(crate) fn from_shared_backend(backend: Arc<B>, options: FuseOptions) -> Self {
        let mut parents = HashMap::new();
        parents.insert(options.view.root().get(), options.view.root().get());
        let invalidation = Arc::new(InvalidationRegistry::default());
        let fuse = Self {
            backend,
            options,
            read_stats: Arc::new(FuseReadStats::default()),
            parents: RwLock::new(parents),
            names: RwLock::new(HashMap::new()),
            attrs: Arc::new(RwLock::new(HashMap::new())),
            next_handle: AtomicU64::new(1),
            read_handles: RwLock::new(HashMap::new()),
            write_handles: RwLock::new(HashMap::new()),
            directory_handles: RwLock::new(HashMap::new()),
            invalidation,
            writeback_publisher: None,
        };
        fuse.register_watch_scope(fuse.options.view.root());
        fuse
    }

    /// Enable opt-in async write-back: open the publish journal under the
    /// writeback root and spawn the background publisher over this mount's
    /// backend. A no-op unless `options.writeback.async_publish` is set. Fallible
    /// (journal I/O), so it is driven from `spawn_mount_backend` rather than the
    /// infallible constructor.
    pub(crate) fn enable_async_publish(&mut self) -> std::io::Result<()> {
        if !self.options.writeback.async_publish {
            return Ok(());
        }
        let backend = Arc::clone(&self.backend);
        let root = self.options.writeback.root.clone();
        self.writeback_publisher = Some(super::publisher::WritebackPublisher::recover(
            backend, &root,
        )?);
        Ok(())
    }

    /// Read-after-write barrier (async write-back only): block until any in-flight
    /// async publish for `inode` commits, surfacing a failed publish as the
    /// handler's error. A no-op when async publish is off, or when nothing is
    /// pending for the inode. Only the background worker resolves the wait, and it
    /// never re-enters a FUSE handler, so a blocked handler cannot deadlock.
    pub(super) fn wait_pending_publish(&self, inode: InodeId) -> Result<(), Errno> {
        match self.writeback_publisher.as_ref() {
            Some(publisher) => publisher.tracker().wait_for(inode),
            None => Ok(()),
        }
    }

    /// `lookup` variant of [`wait_pending_publish`]: blocks on the publish behind
    /// `(parent, name)`, which the caller has not yet resolved to an inode.
    pub(super) fn wait_pending_publish_dentry(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<(), Errno> {
        match self.writeback_publisher.as_ref() {
            Some(publisher) => publisher.tracker().wait_for_dentry(parent, name),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(super) fn writeback_tracker(&self) -> Option<Arc<super::publisher::PendingPublishTracker>> {
        self.writeback_publisher
            .as_ref()
            .map(|publisher| Arc::clone(publisher.tracker()))
    }

    pub(super) fn remember_parent(&self, child: InodeId, parent: InodeId) {
        if let Ok(mut parents) = self.parents.write() {
            parents.insert(child.get(), parent.get());
        }
    }

    pub(super) fn remember_name(&self, child: InodeId, name: &DentryName) {
        if let Ok(mut names) = self.names.write() {
            names.insert(child.get(), name.as_bytes().to_vec());
        }
    }

    pub(super) fn remember_entry(&self, entry: &DentryWithAttr) {
        self.remember_parent(entry.attr.inode, entry.dentry.parent);
        self.remember_name(entry.attr.inode, &entry.dentry.name);
        self.remember_attr(&entry.attr);
        if entry.attr.file_type == FileType::Directory {
            self.register_watch_scope(entry.attr.inode);
        }
    }

    pub(super) fn remember_attr(&self, attr: &InodeAttr) {
        if let Ok(mut attrs) = self.attrs.write() {
            attrs.insert(attr.inode.get(), attr.clone());
        }
    }

    pub(super) fn cached_attr(&self, inode: InodeId) -> Option<InodeAttr> {
        self.attrs
            .read()
            .ok()
            .and_then(|attrs| attrs.get(&inode.get()).cloned())
    }

    pub(super) fn forget_attr_cache(&self, inode: InodeId) {
        if let Ok(mut attrs) = self.attrs.write() {
            attrs.remove(&inode.get());
        }
    }

    pub(super) fn forget_inode_cache(&self, inode: InodeId) {
        if inode == self.options.view.root() {
            return;
        }
        self.forget_attr_cache(inode);
        if let Ok(mut parents) = self.parents.write() {
            parents.remove(&inode.get());
        }
        if let Ok(mut names) = self.names.write() {
            names.remove(&inode.get());
        }
    }

    pub(super) fn snapshot_path_components(
        &self,
        inode: InodeId,
    ) -> Result<Vec<DentryName>, FuseBackendError> {
        let root = self.options.view.root();
        if inode == root {
            return Ok(Vec::new());
        }
        let parents = self.parents.read().map_err(|err| {
            FuseBackendError::Metadata(MetadError::Codec(format!(
                "snapshot parent cache lock poisoned: {err}"
            )))
        })?;
        let names = self.names.read().map_err(|err| {
            FuseBackendError::Metadata(MetadError::Codec(format!(
                "snapshot name cache lock poisoned: {err}"
            )))
        })?;
        let mut current = inode.get();
        let mut reversed = Vec::new();
        let mut seen = HashSet::new();
        while current != root.get() {
            if !seen.insert(current) {
                return Err(FuseBackendError::Metadata(MetadError::Codec(
                    "snapshot inode path cache contains a parent cycle".to_owned(),
                )));
            }
            let parent = parents
                .get(&current)
                .copied()
                .ok_or(FuseBackendError::Metadata(MetadError::NotFound))?;
            let name = names
                .get(&current)
                .ok_or(FuseBackendError::Metadata(MetadError::NotFound))?;
            reversed.push(DentryName::new(name.clone()).map_err(|err| {
                FuseBackendError::Metadata(MetadError::InvalidPath(err.to_string()))
            })?);
            current = parent;
        }
        reversed.reverse();
        Ok(reversed)
    }

    pub(super) fn statfs_snapshot(&self) -> FuseStatfs {
        let blocks = STATFS_TOTAL_BYTES / u64::from(STATFS_BLOCK_SIZE);
        FuseStatfs {
            blocks,
            bfree: blocks,
            bavail: blocks,
            files: STATFS_TOTAL_FILES,
            ffree: STATFS_TOTAL_FILES,
            bsize: STATFS_BLOCK_SIZE,
            namelen: STATFS_NAME_MAX,
            frsize: STATFS_BLOCK_SIZE,
        }
    }

    pub(super) fn register_watch_scope(&self, scope: InodeId) {
        if self.options.view != FuseView::Live {
            return;
        }
        if let Ok(Some(cursor)) = self.backend.watch_subtree(scope) {
            self.invalidation.register_scope(scope, cursor);
        }
    }

    #[cfg(test)]
    pub(crate) fn object_pipeline_stats(&self) -> Result<FuseObjectPipelineStats, Errno> {
        let mut stats = self.backend.object_pipeline_stats().map_err(errno)?;
        merge_fuse_read_stats(&mut stats, &self.read_stats);
        Ok(stats)
    }

    pub(super) fn record_fuse_read_request(&self, size: u32) {
        self.read_stats.requests.fetch_add(1, Ordering::Relaxed);
        self.read_stats
            .request_bytes
            .fetch_add(u64::from(size), Ordering::Relaxed);
    }

    pub(super) fn invalidation_registry(&self) -> Arc<InvalidationRegistry> {
        Arc::clone(&self.invalidation)
    }

    pub(super) fn local_invalidation(&self) -> LocalInvalidation {
        let attr_cache = Arc::clone(&self.attrs);
        let local: LocalInvalidation = Arc::new(move |target: InvalidationTarget| {
            if let Ok(mut attrs) = attr_cache.write() {
                attrs.remove(&target.inode.get());
            }
        });
        local
    }

    pub(super) fn parent_of(&self, inode: InodeId) -> InodeId {
        if inode == self.options.view.root() {
            return inode;
        }
        let raw = self
            .parents
            .read()
            .ok()
            .and_then(|parents| parents.get(&inode.get()).copied())
            .unwrap_or(InodeId::ROOT_RAW);
        InodeId::new(raw).unwrap_or_else(|_| InodeId::root())
    }

    pub(super) fn name_of(&self, inode: InodeId) -> Result<DentryName, Errno> {
        let raw = self
            .names
            .read()
            .ok()
            .and_then(|names| names.get(&inode.get()).cloned())
            .ok_or(Errno::EIO)?;
        DentryName::new(raw).map_err(|_| Errno::EIO)
    }

    pub(super) fn metadata_inode(&self, ino: INodeNo) -> Result<InodeId, Errno> {
        if ino.0 == InodeId::ROOT_RAW {
            return Ok(self.options.view.root());
        }
        inode_id(ino)
    }

    pub(super) fn advisory_lock_request(
        &self,
        request: FuseLockRequest,
    ) -> Result<AdvisoryLockRequest, Errno> {
        Ok(AdvisoryLockRequest {
            inode: self.metadata_inode(request.ino)?,
            owner: request.owner.0,
            start: request.start,
            end: request.end,
            kind: advisory_lock_kind_from_fuse(request.typ)?,
            pid: request.pid,
            wait: request.wait,
        })
    }

    pub(super) fn fuse_ino(&self, inode: InodeId) -> INodeNo {
        if inode == self.options.view.root() {
            INodeNo(InodeId::ROOT_RAW)
        } else {
            INodeNo(inode.get())
        }
    }

    pub(super) fn view_file_attr(&self, attr: &InodeAttr) -> FileAttr {
        let mut out = file_attr(attr);
        out.ino = self.fuse_ino(attr.inode);
        out
    }

    pub(super) fn read_open_flags(&self) -> FopenFlags {
        if self.options.direct_io {
            FopenFlags::FOPEN_DIRECT_IO
        } else if self.options.kernel_cache {
            FopenFlags::FOPEN_KEEP_CACHE
        } else {
            FopenFlags::empty()
        }
    }

    pub(super) fn write_open_flags(&self) -> FopenFlags {
        if self.options.direct_io {
            FopenFlags::FOPEN_DIRECT_IO
        } else {
            FopenFlags::empty()
        }
    }

    pub(super) fn read_only(&self) -> bool {
        self.options.access.is_read_only() || self.options.view.is_read_only()
    }

    pub(super) fn service_get_attr(
        &self,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.get_attr(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                let components = self.snapshot_path_components(inode)?;
                self.backend.get_attr_at_snapshot(snapshot_id, &components)
            }
        }
    }

    pub(super) fn live_open_attr(&self, inode: InodeId) -> Result<InodeAttr, Errno> {
        let attr = self
            .service_get_attr(inode)
            .map_err(errno)?
            .ok_or(Errno::ENOENT)?;
        self.remember_attr(&attr);
        Ok(attr)
    }

    pub(super) fn read_open_attr(&self, inode: InodeId) -> Result<InodeAttr, Errno> {
        if let Some(attr) = self.cached_attr(inode) {
            return Ok(attr);
        }
        self.live_open_attr(inode)
    }

    pub(super) fn service_lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.lookup_plus(parent, name),
            FuseView::Snapshot { snapshot_id, .. } => {
                let parent_components = self.snapshot_path_components(parent)?;
                self.backend
                    .lookup_plus_at_snapshot(snapshot_id, &parent_components, name)
            }
        }
    }

    pub(super) fn service_read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_dir_plus_page(inode, after, limit),
            FuseView::Snapshot { snapshot_id, .. } => {
                let requested = limit.max(1);
                let components = self.snapshot_path_components(inode)?;
                let rows = self
                    .backend
                    .read_dir_plus_at_snapshot(snapshot_id, &components)?;
                let mut entries = rows
                    .into_iter()
                    .filter(|entry| {
                        after.is_none_or(|cursor| entry.dentry.name.as_bytes() > cursor.as_bytes())
                    })
                    .take(requested.saturating_add(1))
                    .collect::<Vec<_>>();
                let has_more = entries.len() > requested;
                entries.truncate(requested);
                let next_cursor = if has_more {
                    entries.last().map(|entry| entry.dentry.name.clone())
                } else {
                    None
                };
                Ok(ReadDirPlusPage {
                    entries,
                    next_cursor,
                })
            }
        }
    }

    pub(super) fn rename_entry(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, FuseBackendError> {
        if self.backend.lookup_plus(new_parent, &new_name)?.is_some() {
            self.backend
                .rename_replace(parent, name, new_parent, new_name)
        } else {
            self.backend
                .rename(parent, name, new_parent, new_name)
                .map(|entry| RenameReplaceResult {
                    entry,
                    replaced: None,
                })
        }
    }

    pub(super) fn service_read_file(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_file(inode, offset, len),
            FuseView::Snapshot { snapshot_id, .. } => {
                let components = self.snapshot_path_components(inode)?;
                self.backend
                    .read_file_at_snapshot(snapshot_id, &components, offset, len)
            }
        }
    }

    pub(super) fn service_read_file_with_known_attr_pipeline(
        &self,
        attr: &InodeAttr,
        offset: u64,
        len: usize,
        pipeline: &mut FileReadPipeline,
        read_plans: &mut nokv_object::ObjectReadPlanCache,
    ) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self
                .backend
                .read_file_with_known_attr_pipeline(attr, offset, len, pipeline, read_plans),
            FuseView::Snapshot { snapshot_id, .. } => {
                let components = self.snapshot_path_components(attr.inode)?;
                self.backend
                    .read_file_at_snapshot(snapshot_id, &components, offset, len)
            }
        }
    }

    pub(super) fn service_read_file_with_known_attr(
        &self,
        attr: &InodeAttr,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_file_with_known_attr(attr, offset, len),
            FuseView::Snapshot { snapshot_id, .. } => {
                let components = self.snapshot_path_components(attr.inode)?;
                self.backend
                    .read_file_at_snapshot(snapshot_id, &components, offset, len)
            }
        }
    }

    pub(super) fn service_read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, FuseBackendError> {
        match self.options.view {
            FuseView::Live => self.backend.read_symlink(inode),
            FuseView::Snapshot { snapshot_id, .. } => {
                let components = self.snapshot_path_components(inode)?;
                self.backend
                    .read_symlink_at_snapshot(snapshot_id, &components)
            }
        }
    }

    pub(super) fn add_dirent(
        &self,
        reply: &mut ReplyDirectory,
        request_offset: u64,
        entry_offset: u64,
        inode: InodeId,
        kind: FuseFileType,
        name: &str,
    ) -> bool {
        request_offset < entry_offset && reply.add(self.fuse_ino(inode), entry_offset, kind, name)
    }

    pub(super) fn add_dirent_plus(
        &self,
        reply: &mut ReplyDirectoryPlus,
        request_offset: u64,
        entry_offset: u64,
        name: &str,
        attr: &InodeAttr,
    ) -> bool {
        request_offset < entry_offset
            && reply.add(
                self.fuse_ino(attr.inode),
                entry_offset,
                name,
                &self.options.entry_ttl,
                &self.view_file_attr(attr),
                Generation(attr.generation),
            )
    }

    pub(super) fn allocate_directory_handle(&self, inode: InodeId) -> Result<FileHandle, Errno> {
        let attr = self
            .service_get_attr(inode)
            .map_err(errno)?
            .ok_or(Errno::ENOENT)?;
        if attr.file_type != FileType::Directory {
            return Err(Errno::ENOTDIR);
        }
        let raw = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.directory_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .insert(
                raw,
                DirectoryHandle {
                    inode,
                    attr,
                    entries: Vec::new(),
                    next_cursor: None,
                    exhausted: false,
                },
            );
        Ok(FileHandle(raw))
    }

    pub(super) fn directory_handle_attr(&self, fh: FileHandle) -> Result<InodeAttr, Errno> {
        self.directory_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| handle.attr.clone())
            .ok_or(Errno::EBADF)
    }

    pub(super) fn directory_child(
        &self,
        fh: FileHandle,
        child_index: usize,
    ) -> Result<Option<DentryWithAttr>, Errno> {
        loop {
            let fetch = {
                let handles = self.directory_handles.read().map_err(|_| Errno::EIO)?;
                let handle = handles.get(&fh.0).ok_or(Errno::EBADF)?;
                if let Some(entry) = handle.entries.get(child_index) {
                    return Ok(Some(entry.clone()));
                }
                if handle.exhausted {
                    return Ok(None);
                }
                (handle.inode, handle.next_cursor.clone())
            };
            let page = self
                .service_read_dir_plus_page(fetch.0, fetch.1.as_ref(), FUSE_READDIR_PAGE_SIZE)
                .map_err(errno)?;
            for entry in &page.entries {
                self.remember_entry(entry);
            }
            let mut handles = self.directory_handles.write().map_err(|_| Errno::EIO)?;
            let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
            if handle.entries.get(child_index).is_some() || handle.next_cursor != fetch.1 {
                continue;
            }
            handle.entries.extend(page.entries);
            handle.next_cursor = page.next_cursor;
            handle.exhausted = handle.next_cursor.is_none();
        }
    }

    pub(super) fn release_directory_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        self.directory_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .remove(&fh.0);
        Ok(())
    }

    pub(super) fn sync_directory_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        self.directory_handle_attr(fh).map(|_| ())
    }

    pub(super) fn allocate_read_handle(&self, attr: InodeAttr) -> Result<FileHandle, Errno> {
        let raw = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.read_handles.write().map_err(|_| Errno::EIO)?.insert(
            raw,
            Arc::new(ReadHandle {
                attr,
                state: Mutex::new(ReadHandleState::new(FileReadPipeline::new(
                    self.options.read_pipeline,
                ))),
            }),
        );
        Ok(FileHandle(raw))
    }

    pub(super) fn read_from_read_handle(
        &self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<u8>>, Errno> {
        let Some(handle) = self
            .read_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .cloned()
        else {
            return Ok(None);
        };
        // Clamp the requested length to what the handle's known size can serve
        // before EITHER backend path. A read that starts before EOF but extends
        // past `attr.size` must be a short read, not a backend range error
        // (mirrors the write-handle read path in this file).
        if offset >= handle.attr.size {
            return Ok(Some(Vec::new()));
        }
        let len = u64::from(size)
            .min(handle.attr.size - offset)
            .try_into()
            .map_err(|_| Errno::EINVAL)?;
        if !self.options.prefetch.enabled {
            let bytes = self
                .service_read_file_with_known_attr(&handle.attr, offset, len)
                .map_err(errno)?;
            return Ok(Some(bytes));
        }
        let mut state = handle.state.lock().map_err(|_| Errno::EIO)?;
        if !state.reader.needs_stateful_pipeline(offset, len) {
            state
                .reader
                .observe_unpipelined_read(offset, len)
                .map_err(errno)?;
            drop(state);
            let bytes = self
                .service_read_file_with_known_attr(&handle.attr, offset, len)
                .map_err(errno)?;
            return Ok(Some(bytes));
        }
        let ReadHandleState { reader, read_plans } = &mut *state;
        let bytes = self
            .service_read_file_with_known_attr_pipeline(
                &handle.attr,
                offset,
                len,
                reader,
                read_plans,
            )
            .map_err(errno)?;
        Ok(Some(bytes))
    }

    pub(super) fn read_for_copy(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, Errno> {
        if let Some(bytes) = self.read_from_read_handle(fh, offset, size)? {
            return Ok(bytes);
        }
        if let Some(bytes) = self.read_from_handle(fh, offset, size)? {
            return Ok(bytes);
        }
        let inode = self.metadata_inode(ino)?;
        self.service_read_file(inode, offset, size as usize)
            .map_err(errno)
    }

    pub(super) fn release_read_handle(&self, fh: FileHandle) -> Result<bool, Errno> {
        Ok(self
            .read_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .remove(&fh.0)
            .is_some())
    }

    pub(super) fn lseek_file_size(&self, ino: INodeNo, fh: FileHandle) -> Result<u64, Errno> {
        if let Some(size) = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| handle.size)
        {
            return Ok(size);
        }
        if let Some(size) = self
            .read_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .map(|handle| handle.attr.size)
        {
            return Ok(size);
        }
        let inode = self.metadata_inode(ino)?;
        match self.service_get_attr(inode).map_err(errno)? {
            Some(attr) if attr.file_type == FileType::File => Ok(attr.size),
            Some(_) => Err(Errno::EINVAL),
            None => Err(Errno::ENOENT),
        }
    }

    /// Largest size of any open write handle for `inode` whose buffered/dirty
    /// data has not yet been published. Used so attribute replies report the
    /// in-flight size of a file being written, not the smaller committed size.
    ///
    /// This matters on macOS: if `getattr` reported the stale committed size
    /// while an application is still appending, the kernel's cluster writeback
    /// treats the bytes between the reported size and the write offset as a hole
    /// and flushes zero-filled pages over them, corrupting the just-written data.
    pub(super) fn pending_write_size(&self, inode: InodeId) -> Option<u64> {
        let handles = self.write_handles.read().ok()?;
        handles
            .values()
            .filter(|handle| handle.inode == inode && handle.dirty)
            .map(|handle| handle.size)
            .max()
    }

    pub(super) fn attr_with_pending_size(&self, mut attr: InodeAttr) -> InodeAttr {
        if attr.file_type == FileType::File {
            if let Some(size) = self.pending_write_size(attr.inode) {
                attr.size = attr.size.max(size);
            }
        }
        attr
    }

    pub(super) fn ensure_fallocated_range(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> Result<(), Errno> {
        let current_size = self.lseek_file_size(ino, fh)?;
        let size = resolve_fallocate_size(current_size, offset, length, mode)?;
        let has_write_handle = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .contains_key(&fh.0);
        if !has_write_handle {
            return Err(Errno::EBADF);
        }
        let Some(size) = size else {
            return Ok(());
        };
        if size <= current_size {
            return Ok(());
        }
        self.truncate_handle(fh, size)?;
        self.publish_handle(fh)
    }

    pub(super) fn allocate_handle(
        &self,
        handle: WriteHandle<B::Prepared>,
    ) -> Result<FileHandle, Errno> {
        let raw = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .insert(raw, handle);
        Ok(FileHandle(raw))
    }

    pub(super) fn open_write_handle(
        &self,
        attr: &InodeAttr,
        parent: InodeId,
    ) -> Result<FileHandle, Errno> {
        let name = self.name_of(attr.inode)?;
        self.allocate_write_handle(attr, parent, name)
    }

    pub(super) fn allocate_write_handle(
        &self,
        attr: &InodeAttr,
        parent: InodeId,
        name: DentryName,
    ) -> Result<FileHandle, Errno> {
        self.allocate_write_handle_with_session(attr, parent, name, None)
    }

    pub(super) fn allocate_write_handle_with_session(
        &self,
        attr: &InodeAttr,
        parent: InodeId,
        name: DentryName,
        prepared: Option<B::Prepared>,
    ) -> Result<FileHandle, Errno> {
        let writer = prepared
            .as_ref()
            .map(|prepared| {
                self.backend
                    .new_write_pipeline(prepared, &fuse_manifest_id(parent, attr.inode))
                    .map_err(errno)
            })
            .transpose()?;
        self.allocate_handle(WriteHandle {
            inode: attr.inode,
            parent,
            name,
            prepared,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            base_size: attr.size,
            size: attr.size,
            writer,
            buffered: Vec::new(),
            pending_uploads: Vec::new(),
            staged_block_offsets: HashSet::new(),
            dirty: false,
        })
    }

    pub(super) fn write_to_handle(
        &self,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, Errno> {
        let end = offset
            .checked_add(u64::try_from(data.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        if data.is_empty() {
            return Ok(0);
        }
        let inode = {
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
            if handle.prepared.is_none() {
                handle.prepared = Some(
                    self.backend
                        .prepare_artifact_replace(handle.parent, handle.name.clone())
                        .map_err(errno)?,
                );
            }
            let prepared = handle.prepared.clone().ok_or(Errno::EIO)?;
            if handle.writer.is_none() {
                handle.writer = Some(
                    self.backend
                        .new_write_pipeline(
                            &prepared,
                            &fuse_manifest_id(handle.parent, handle.inode),
                        )
                        .map_err(errno)?,
                );
            }
            push_buffered_write(&mut handle.buffered, offset, data);
            handle.size = handle.size.max(end);
            handle.dirty = true;
            handle.inode
        };
        self.forget_attr_cache(inode);
        self.flush_handle_buffers(fh, false)?;
        Ok(data.len())
    }

    pub(super) fn truncate_handle(&self, fh: FileHandle, size: u64) -> Result<(), Errno> {
        let inode = {
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let handle = handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
            if handle.prepared.is_none() {
                handle.prepared = Some(
                    self.backend
                        .prepare_artifact_replace(handle.parent, handle.name.clone())
                        .map_err(errno)?,
                );
            }
            if handle.writer.is_none() {
                let prepared = handle.prepared.as_ref().ok_or(Errno::EIO)?;
                handle.writer = Some(
                    self.backend
                        .new_write_pipeline(
                            prepared,
                            &fuse_manifest_id(handle.parent, handle.inode),
                        )
                        .map_err(errno)?,
                );
            }
            handle.size = size;
            handle.dirty = true;
            handle.inode
        };
        self.forget_attr_cache(inode);
        Ok(())
    }

    pub(super) fn update_handle_attrs(
        &self,
        fh: FileHandle,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<bool, Errno> {
        let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
        let Some(handle) = handles.get_mut(&fh.0) else {
            return Ok(false);
        };
        if let Some(mode) = mode {
            handle.mode = mode;
        }
        if let Some(uid) = uid {
            handle.uid = uid;
        }
        if let Some(gid) = gid {
            handle.gid = gid;
        }
        Ok(true)
    }

    pub(super) fn read_from_handle(
        &self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Option<Vec<u8>>, Errno> {
        let Some(handle) = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .cloned()
        else {
            return Ok(None);
        };
        if offset >= handle.size {
            return Ok(Some(Vec::new()));
        }
        let output_len = u64::from(size)
            .min(handle.size.saturating_sub(offset))
            .try_into()
            .map_err(|_| Errno::EINVAL)?;
        let mut bytes = vec![0_u8; output_len];
        if offset < handle.base_size {
            let base_len = u64::try_from(output_len)
                .map_err(|_| Errno::EINVAL)?
                .min(handle.base_size - offset) as usize;
            let base = self
                .service_read_file(handle.inode, offset, base_len)
                .map_err(errno)?;
            bytes[..base.len()].copy_from_slice(&base);
        }
        if let Some(writer) = &handle.writer {
            self.overlay_dirty_extents(&mut bytes, offset, writer.dirty_extents())?;
        }
        for upload in &handle.pending_uploads {
            self.overlay_pending_ranges(&mut bytes, offset, &upload.ranges)?;
        }
        self.overlay_buffered_ranges(&mut bytes, offset, &handle.buffered)?;
        Ok(Some(bytes))
    }

    pub(super) fn cleanup_completed_pending(
        &self,
        pending: PendingChunkedWrite,
    ) -> Result<(), Errno> {
        match pending.wait() {
            Ok(written) => cleanup_written_objects(&*self.backend, &written),
            Err(err) => {
                let _ = pending.discard_writeback_cache();
                Err(self.cleanup_object_error(err))
            }
        }
    }

    pub(super) fn cleanup_object_error(&self, err: ObjectError) -> Errno {
        if let ObjectError::StagedWriteFailed { staged, .. } = &err {
            let _ = self.backend.cleanup_staged_objects(staged);
        }
        errno(err)
    }

    pub(super) fn drain_handle_uploads(&self, fh: FileHandle) -> Result<(), Errno> {
        loop {
            let uploads = {
                let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
                let Some(handle) = handles.get_mut(&fh.0) else {
                    return Err(Errno::EBADF);
                };
                std::mem::take(&mut handle.pending_uploads)
            };
            if uploads.is_empty() {
                return Ok(());
            }
            let mut completed = Vec::with_capacity(uploads.len());
            let mut failed_ranges = Vec::new();
            let mut first_error = None;
            for upload in uploads {
                match upload.pending.wait() {
                    Ok(written) => completed.push(written),
                    Err(err) => {
                        let errno = self.cleanup_object_error(err);
                        let _ = upload.pending.discard_writeback_cache();
                        if first_error.is_none() {
                            first_error = Some(errno);
                        }
                        failed_ranges.extend(
                            upload
                                .ranges
                                .into_iter()
                                .map(PendingBufferedRange::into_buffered),
                        );
                    }
                }
            }
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let Some(handle) = handles.get_mut(&fh.0) else {
                drop(handles);
                for written in completed {
                    cleanup_written_objects(&*self.backend, &written)?;
                }
                return Err(Errno::EBADF);
            };
            let Some(writer) = handle.writer.as_mut() else {
                drop(handles);
                for written in completed {
                    cleanup_written_objects(&*self.backend, &written)?;
                }
                return Err(Errno::EIO);
            };
            for written in completed {
                writer.record_write(written).map_err(errno)?;
            }
            if !failed_ranges.is_empty() {
                // These blocks never landed durably: forget their staged digest so
                // the retried write re-stages them instead of being deduped away.
                forget_staged_blocks(&mut handle.staged_block_offsets, &failed_ranges)?;
                let current = std::mem::take(&mut handle.buffered);
                handle.buffered = failed_ranges;
                for range in current {
                    push_buffered_write(&mut handle.buffered, range.offset, &range.bytes);
                }
            }
            if let Some(err) = first_error {
                return Err(err);
            }
        }
    }

    pub(super) fn flush_handle_buffers(&self, fh: FileHandle, force: bool) -> Result<(), Errno> {
        loop {
            let reservation = {
                let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
                let Some(handle) = handles.get_mut(&fh.0) else {
                    return Err(Errno::EBADF);
                };
                if !has_buffered_upload_ready(&handle.buffered, force) {
                    return Ok(());
                }
                let taken = take_buffered_upload_ranges(&mut handle.buffered, force)?;
                if taken.is_empty() {
                    return Ok(());
                }
                let ranges = select_unstaged_blocks(taken, &mut handle.staged_block_offsets)?;
                if ranges.is_empty() {
                    // Every block in this flush is byte-for-byte identical to one
                    // already staged in this session (FUSE writeback re-dispatching
                    // the same pages); nothing new to upload. On a forced flush we
                    // still need to drain any remaining retained tail, so loop.
                    if force {
                        continue;
                    }
                    return Ok(());
                }
                let prepared = handle.prepared.clone().ok_or(Errno::EIO)?;
                let writer = handle.writer.as_mut().ok_or(Errno::EIO)?;
                let upload = writer.prepare_upload(ranges).map_err(errno)?;
                let Some(upload) = upload else {
                    if force {
                        continue;
                    }
                    return Ok(());
                };
                WriteStageReservation {
                    prepared,
                    manifest_id: writer.options().manifest_id.clone(),
                    block_index_base: upload.block_index_base,
                    ranges: upload.ranges,
                }
            };
            let pending_ranges = reservation
                .ranges
                .into_iter()
                .map(PendingBufferedRange::from_chunk_range)
                .collect::<Vec<_>>();
            let pending = match self.backend.stage_prepared_artifact_shared_ranges_async(
                &reservation.prepared,
                &reservation.manifest_id,
                &pending_ranges,
                reservation.block_index_base,
            ) {
                Ok(pending) => pending,
                Err(err) => {
                    self.restore_buffered_ranges(
                        fh,
                        pending_ranges
                            .into_iter()
                            .map(PendingBufferedRange::into_buffered)
                            .collect(),
                    );
                    return Err(errno(err));
                }
            };
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let Some(handle) = handles.get_mut(&fh.0) else {
                drop(handles);
                self.cleanup_completed_pending(pending)?;
                return Err(Errno::EBADF);
            };
            if handle
                .prepared
                .as_ref()
                .map(|prepared| self.backend.prepared_generation(prepared))
                != Some(self.backend.prepared_generation(&reservation.prepared))
            {
                drop(handles);
                self.cleanup_completed_pending(pending)?;
                return Err(Errno::ESTALE);
            }
            handle.pending_uploads.push(PendingBufferedUpload {
                pending,
                ranges: pending_ranges,
            });
            if !force {
                return Ok(());
            }
        }
    }

    pub(super) fn restore_buffered_ranges(&self, fh: FileHandle, ranges: Vec<BufferedWriteRange>) {
        let Ok(mut handles) = self.write_handles.write() else {
            return;
        };
        let Some(handle) = handles.get_mut(&fh.0) else {
            return;
        };
        // The staging submission for these ranges failed, so their blocks were not
        // durably staged: forget the digests recorded during dedup so the retry
        // re-stages them instead of skipping them as "unchanged".
        let _ = forget_staged_blocks(&mut handle.staged_block_offsets, &ranges);
        let current = std::mem::take(&mut handle.buffered);
        handle.buffered = ranges;
        for range in current {
            push_buffered_write(&mut handle.buffered, range.offset, &range.bytes);
        }
    }

    pub(super) fn overlay_dirty_extents(
        &self,
        output: &mut [u8],
        output_offset: u64,
        extents: &[DirtyChunkExtent],
    ) -> Result<(), Errno> {
        if output.is_empty() {
            return Ok(());
        }
        let output_limit = output_offset
            .checked_add(u64::try_from(output.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        for extent in extents {
            for chunk in &extent.chunks {
                for block in &chunk.blocks {
                    let block_end = block
                        .logical_offset
                        .checked_add(block.len)
                        .ok_or(Errno::EINVAL)?;
                    let start = block.logical_offset.max(output_offset);
                    let end = block_end.min(output_limit);
                    if start >= end {
                        continue;
                    }
                    let object_offset = block
                        .object_offset
                        .checked_add(start - block.logical_offset)
                        .ok_or(Errno::EINVAL)?;
                    let len = usize::try_from(end - start).map_err(|_| Errno::EINVAL)?;
                    let bytes = self
                        .backend
                        .read_session_object_blocks(
                            len,
                            &[ObjectReadBlock {
                                object_key: block.object_key.clone(),
                                digest_uri: "sha256:test".to_owned(),
                                object_offset,
                                object_len: block.len,
                                len,
                                output_offset: 0,
                            }],
                        )
                        .map_err(errno)?;
                    let output_start =
                        usize::try_from(start - output_offset).map_err(|_| Errno::EINVAL)?;
                    let output_end = output_start.checked_add(bytes.len()).ok_or(Errno::EINVAL)?;
                    output[output_start..output_end].copy_from_slice(&bytes);
                }
            }
        }
        Ok(())
    }

    pub(super) fn overlay_buffered_ranges(
        &self,
        output: &mut [u8],
        output_offset: u64,
        ranges: &[BufferedWriteRange],
    ) -> Result<(), Errno> {
        if output.is_empty() {
            return Ok(());
        }
        let output_limit = output_offset
            .checked_add(u64::try_from(output.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        for range in ranges {
            let range_end = range
                .offset
                .checked_add(u64::try_from(range.bytes.len()).map_err(|_| Errno::EINVAL)?)
                .ok_or(Errno::EINVAL)?;
            let start = range.offset.max(output_offset);
            let end = range_end.min(output_limit);
            if start >= end {
                continue;
            }
            let source_start = usize::try_from(start - range.offset).map_err(|_| Errno::EINVAL)?;
            let source_end = usize::try_from(end - range.offset).map_err(|_| Errno::EINVAL)?;
            let output_start = usize::try_from(start - output_offset).map_err(|_| Errno::EINVAL)?;
            let output_end = usize::try_from(end - output_offset).map_err(|_| Errno::EINVAL)?;
            output[output_start..output_end]
                .copy_from_slice(&range.bytes[source_start..source_end]);
        }
        Ok(())
    }

    pub(super) fn overlay_pending_ranges(
        &self,
        output: &mut [u8],
        output_offset: u64,
        ranges: &[PendingBufferedRange],
    ) -> Result<(), Errno> {
        if output.is_empty() {
            return Ok(());
        }
        let output_limit = output_offset
            .checked_add(u64::try_from(output.len()).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        for range in ranges {
            let range_end = range
                .offset
                .checked_add(u64::try_from(range.len()).map_err(|_| Errno::EINVAL)?)
                .ok_or(Errno::EINVAL)?;
            let start = range.offset.max(output_offset);
            let end = range_end.min(output_limit);
            if start >= end {
                continue;
            }
            let source_start = usize::try_from(start - range.offset).map_err(|_| Errno::EINVAL)?;
            let source_end = usize::try_from(end - range.offset).map_err(|_| Errno::EINVAL)?;
            let output_start = usize::try_from(start - output_offset).map_err(|_| Errno::EINVAL)?;
            let output_end = usize::try_from(end - output_offset).map_err(|_| Errno::EINVAL)?;
            output[output_start..output_end]
                .copy_from_slice(&range.as_slice()[source_start..source_end]);
        }
        Ok(())
    }

    pub(super) fn publish_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        let has_write_handle = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .contains_key(&fh.0);
        if !has_write_handle {
            return Ok(());
        }
        self.flush_handle_buffers(fh, true)?;
        // Opt-in async write-back: ack once the blocks are durably staged + a
        // journal record is fsync'd, deferring the object-store drain + manifest
        // commit to the background publisher. Off by default (synchronous tail).
        if self.writeback_publisher.is_some() {
            return self.stage_and_enqueue_publish(fh);
        }
        self.drain_handle_uploads(fh)?;
        let Some(snapshot) = self
            .write_handles
            .read()
            .map_err(|_| Errno::EIO)?
            .get(&fh.0)
            .cloned()
        else {
            return Ok(());
        };
        if !snapshot.dirty {
            return Ok(());
        }
        let writer = snapshot.writer.as_ref().ok_or(Errno::EIO)?;
        let prepared = snapshot.prepared.clone().ok_or(Errno::EIO)?;
        let result = publish_staged_session(
            &*self.backend,
            prepared,
            snapshot.parent,
            snapshot.name.clone(),
            fuse_manifest_id(snapshot.parent, snapshot.inode),
            snapshot.size,
            snapshot.mode,
            snapshot.uid,
            snapshot.gid,
            writer.staged_chunks(),
            writer.staged_objects(),
        )?;
        let entry_inode = result.entry.attr.inode;
        self.remember_entry(&result.entry);
        if let Some(replaced) = result.replaced.filter(|old| old.attr.inode != entry_inode) {
            self.forget_inode_cache(replaced.attr.inode);
        }
        if let Some(handle) = self
            .write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .get_mut(&fh.0)
        {
            handle.base_size = snapshot.size;
            handle.size = snapshot.size;
            handle.prepared = None;
            handle.writer = None;
            handle.staged_block_offsets.clear();
            handle.dirty = false;
        }
        Ok(())
    }

    /// The async-publish ack path: durably record the pending publish + hand the
    /// staged-block uploads to the background publisher, then reset the handle and
    /// return — without draining the object-store uploads or committing the
    /// manifest. The fsync of the journal record is the durability anchor the ack
    /// rests on; everything after it completes in the worker.
    fn stage_and_enqueue_publish(&self, fh: FileHandle) -> Result<(), Errno> {
        let Some(publisher) = self.writeback_publisher.as_ref() else {
            return Err(Errno::EIO);
        };
        // Snapshot + detach the session state under the write lock: take the
        // in-flight uploads so the worker owns them, and capture the scalars
        // needed to publish (and to recover after a crash).
        let staged = {
            let mut handles = self.write_handles.write().map_err(|_| Errno::EIO)?;
            let Some(handle) = handles.get_mut(&fh.0) else {
                return Err(Errno::EBADF);
            };
            if !handle.dirty {
                return Ok(());
            }
            let prepared = handle.prepared.clone().ok_or(Errno::EIO)?;
            let uploads = std::mem::take(&mut handle.pending_uploads);
            StagedPublish {
                prepared,
                inode: handle.inode,
                parent: handle.parent,
                name: handle.name.clone(),
                size: handle.size,
                mode: handle.mode,
                uid: handle.uid,
                gid: handle.gid,
                uploads,
            }
        };
        let fields = self.backend.prepared_record_fields(&staged.prepared);
        let manifest_id = fuse_manifest_id(staged.parent, staged.inode);
        let cache_files = staged
            .uploads
            .iter()
            .flat_map(|upload| upload.pending.cache_entries())
            .map(|(logical_offset, cache_key, file_name, len)| CacheFileRef {
                logical_offset,
                cache_key,
                file_name,
                len,
            })
            .collect();
        let record = PendingPublishRecord {
            inode: staged.inode.get(),
            parent: staged.parent.get(),
            name: staged.name.as_bytes().to_vec(),
            mount: fields.mount,
            generation: fields.generation,
            mtime_ms: fields.mtime_ms,
            ctime_ms: fields.ctime_ms,
            replace: fields.replace,
            dentry_version: fields.dentry_version.unwrap_or(0),
            old_generation: fields.old_generation.unwrap_or(0),
            object_gc_claim_version: fields.object_gc_claim_version,
            size: staged.size,
            mode: staged.mode,
            uid: staged.uid,
            gid: staged.gid,
            manifest_id: manifest_id.clone(),
            cache_files,
        };
        // Make every staged block's directory entry durable in one batched fsync,
        // then fsync the journal record. Together these are the ack's durability
        // anchor: only after both may we return.
        self.backend.sync_writeback_root().map_err(errno)?;
        publisher
            .journal()
            .append_publish(&record)
            .map_err(|_| Errno::EIO)?;
        publisher
            .tracker()
            .begin(staged.inode, fields.generation, staged.parent, &staged.name);
        publisher.enqueue(PendingPublish {
            prepared: staged.prepared,
            parent: staged.parent,
            name: staged.name,
            manifest_id,
            inode: staged.inode,
            generation: fields.generation,
            size: staged.size,
            mode: staged.mode,
            uid: staged.uid,
            gid: staged.gid,
            uploads: staged.uploads,
        });
        // Reset the session: the write is acked and now owned by the worker.
        if let Some(handle) = self
            .write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .get_mut(&fh.0)
        {
            handle.base_size = staged.size;
            handle.size = staged.size;
            handle.prepared = None;
            handle.writer = None;
            handle.staged_block_offsets.clear();
            handle.dirty = false;
        }
        Ok(())
    }

    pub(super) fn release_handle(&self, fh: FileHandle) -> Result<(), Errno> {
        self.publish_handle(fh)?;
        self.write_handles
            .write()
            .map_err(|_| Errno::EIO)?
            .remove(&fh.0);
        Ok(())
    }
}

/// The detached write-session state captured at an async-publish ack: handed off
/// to the background publisher so the handle can reset immediately.
struct StagedPublish<P> {
    prepared: P,
    inode: InodeId,
    parent: InodeId,
    name: DentryName,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    uploads: Vec<PendingBufferedUpload>,
}

/// Commit a staged write session as a published artifact generation. This is the
/// metadata half of a write close, shared by the synchronous `publish_handle`
/// path and the background write-back [`publisher`](super::publisher) worker so
/// both build the manifest, re-sync the replace CAS, and commit identically.
///
/// `staged_chunks`/`staged_objects` are the fully-uploaded result of the session
/// (from the handle's writer online, or a writer rebuilt from the upload results
/// in the worker). The caller owns any post-commit cache updates — this function
/// touches no `NoKvFuse` state, so it is callable with only an `Arc<B>`.
#[allow(clippy::too_many_arguments)]
pub(super) fn publish_staged_session<B>(
    backend: &B,
    mut prepared: B::Prepared,
    parent: InodeId,
    name: DentryName,
    manifest_id: String,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    staged_chunks: &[StoredChunk],
    staged_objects: &StagedObjectSet,
) -> Result<RenameReplaceResult, Errno>
where
    B: FuseBackend,
{
    // The replace CAS is guarded by the dentry version captured when the handle
    // prepared. A `setattr` (chmod/utimes/truncate) on this still-open file
    // commits through `update_attrs`, which advances the dentry version
    // out-of-band and would otherwise strand this publish with a stale guard
    // (PredicateFailed -> EIO). Re-read the live dentry version and rebind the
    // guard to it; the pinned generation (and the staged object keys derived
    // from it) is left untouched, so this only re-syncs the CAS to the truth the
    // metadata store already committed.
    if backend.prepared_is_replace(&prepared) {
        if let Some(version) = backend
            .current_dentry_version(parent, &name)
            .map_err(errno)?
        {
            backend.rebind_prepared_dentry_version(&mut prepared, version);
        }
    }
    let digest_uri =
        manifest_digest_uri(size, backend.prepared_generation(&prepared), staged_chunks);
    let chunks = chunk_manifests_from_stored_chunks(staged_chunks);
    backend
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent,
                name,
                producer: "nokv-fuse".to_owned(),
                digest_uri,
                content_type: "application/octet-stream".to_owned(),
                manifest_id,
                size,
                chunks,
                staged: staged_objects.clone(),
                mode,
                uid,
                gid,
            },
        )
        .map_err(errno)
}
