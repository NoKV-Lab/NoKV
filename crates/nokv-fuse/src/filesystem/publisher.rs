//! Background write-back publisher: drains acked-on-buffer writes to the object
//! store + metadata in a worker thread, off the `flush`/`release` critical path.
//!
//! In opt-in async-publish mode, `release_handle` durably stages the write blocks
//! in the writeback cache, fsyncs a [`PendingPublishRecord`] to the publish
//! journal, and hands a [`PendingPublish`] to this worker instead of blocking on
//! the object-store upload + manifest commit. The worker then, per item:
//!
//! 1. **Idempotency gen-check** — if the inode's published generation already
//!    covers this record (a crash after publish but before the journal
//!    tombstone), skip the re-publish; just evict + tombstone.
//! 2. **Drain uploads** — wait the staged-block uploads and rebuild the session
//!    manifest from their results, exactly as the synchronous close does.
//! 3. **Publish** — commit the manifest via the shared
//!    [`publish_staged_session`](super::handle::publish_staged_session).
//! 4. **Evict + tombstone** — drop the now-redundant cache copies and retire the
//!    journal record.
//! 5. **Complete the tracker** — *always*, even on failure (delivering the error
//!    to any read-after-write barrier) so a waiter never hangs.
//!
//! A failed publish keeps its journal record + cache blocks for mount-time
//! recovery to re-drive; the gen-check makes that replay idempotent.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use fuser::Errno;
use nokv_types::{DentryName, InodeId};

use crate::backend::{FuseBackend, PreparedRecordFields, RecoveredBlock};

use super::errno;
use super::handle::publish_staged_session;
use super::publish_journal::{PendingPublishRecord, PublishJournal};
use super::write_session::PendingBufferedUpload;

/// One acked-on-buffer write awaiting background publish. Carries everything the
/// worker needs to commit without the in-memory `WriteHandle`: the prepared
/// artifact, the staged-block uploads to drain, and the manifest identity.
pub(super) struct PendingPublish<P> {
    pub(super) prepared: P,
    pub(super) parent: InodeId,
    pub(super) name: DentryName,
    pub(super) manifest_id: String,
    pub(super) inode: InodeId,
    pub(super) generation: u64,
    pub(super) size: u64,
    pub(super) mode: u32,
    pub(super) uid: u32,
    pub(super) gid: u32,
    pub(super) uploads: Vec<PendingBufferedUpload>,
}

// --------------------------------------------------------------------------- //
// Read-after-write barrier state
// --------------------------------------------------------------------------- //

/// Tracks the in-flight async publishes so read-side handlers can block until a
/// just-acked write is durably committed (read-after-write consistency on the
/// same mount). The worker is the *only* thread that resolves an entry, so a
/// FUSE read waiting here can never deadlock against the worker.
#[derive(Default)]
pub(super) struct PendingPublishTracker {
    inner: Mutex<PendingPublishInner>,
    ready: Condvar,
}

#[derive(Default)]
struct PendingPublishInner {
    by_inode: HashMap<u64, PendingPublishState>,
    /// `(parent, name) -> inode`, so a `lookup` (which has no inode yet) can find
    /// the pending publish to wait on.
    by_dentry: HashMap<(u64, Vec<u8>), u64>,
}

struct PendingPublishState {
    generation: u64,
    parent: u64,
    name: Vec<u8>,
    /// `None` while the publish is in flight; `Some(Err(..))` once it has failed
    /// (a successful publish removes the entry instead, so waiters read fresh).
    result: Option<Result<(), Errno>>,
}

impl PendingPublishTracker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Mark an inode as having an in-flight publish at `generation`. A re-write of
    /// the same inode before the prior publish drains overwrites the entry with
    /// the newer generation, so a barrier always waits for the latest write.
    pub(super) fn begin(
        &self,
        inode: InodeId,
        generation: u64,
        parent: InodeId,
        name: &DentryName,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        inner
            .by_dentry
            .insert((parent.get(), name.as_bytes().to_vec()), inode.get());
        inner.by_inode.insert(
            inode.get(),
            PendingPublishState {
                generation,
                parent: parent.get(),
                name: name.as_bytes().to_vec(),
                result: None,
            },
        );
    }

    /// Resolve the publish of `generation` for `inode`. A no-op if the tracked
    /// generation has moved on (a newer write superseded this one). On success
    /// the entry is removed so later reads fall through to fresh metadata; on
    /// failure the error is retained so barriers surface it instead of hanging.
    pub(super) fn complete(&self, inode: InodeId, generation: u64, result: Result<(), Errno>) {
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        let matched = inner
            .by_inode
            .get(&inode.get())
            .filter(|state| state.generation == generation)
            .map(|state| (state.parent, state.name.clone()));
        if let Some((parent, name)) = matched {
            match result {
                Ok(()) => {
                    inner.by_inode.remove(&inode.get());
                    let key = (parent, name);
                    if inner.by_dentry.get(&key) == Some(&inode.get()) {
                        inner.by_dentry.remove(&key);
                    }
                }
                Err(err) => {
                    if let Some(state) = inner.by_inode.get_mut(&inode.get()) {
                        state.result = Some(Err(err));
                    }
                }
            }
        }
        self.ready.notify_all();
    }

    /// Block until `inode`'s in-flight publish (if any) resolves. Returns the
    /// publish error on failure, `Ok(())` once committed (or if nothing pending).
    pub(super) fn wait_for(&self, inode: InodeId) -> Result<(), Errno> {
        self.wait_inode(inode.get())
    }

    /// Block until the publish behind `(parent, name)` resolves, for `lookup`
    /// which has no inode in hand yet.
    pub(super) fn wait_for_dentry(&self, parent: InodeId, name: &DentryName) -> Result<(), Errno> {
        let inode = {
            let inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
            inner
                .by_dentry
                .get(&(parent.get(), name.as_bytes().to_vec()))
                .copied()
        };
        match inode {
            Some(inode) => self.wait_inode(inode),
            None => Ok(()),
        }
    }

    fn wait_inode(&self, inode: u64) -> Result<(), Errno> {
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        loop {
            match inner.by_inode.get(&inode) {
                None => return Ok(()),
                Some(state) => match state.result {
                    Some(result) => return result,
                    None => {
                        inner = self
                            .ready
                            .wait(inner)
                            .unwrap_or_else(|err| err.into_inner());
                    }
                },
            }
        }
    }
}

// --------------------------------------------------------------------------- //
// Background worker
// --------------------------------------------------------------------------- //

/// The publisher worker thread + its queue handle. `Drop` stops the worker and
/// joins it, draining any backlog first so an unmount never abandons acked work.
pub(super) struct PublisherWorker<B: FuseBackend> {
    queue: Arc<PublisherQueue<B::Prepared>>,
    handle: Option<JoinHandle<()>>,
}

struct PublisherQueue<P> {
    state: Mutex<PublisherQueueState<P>>,
    signal: Condvar,
}

struct PublisherQueueState<P> {
    items: VecDeque<PendingPublish<P>>,
    stop: bool,
}

impl<B: FuseBackend> PublisherWorker<B> {
    pub(super) fn spawn(
        backend: Arc<B>,
        journal: Arc<PublishJournal>,
        tracker: Arc<PendingPublishTracker>,
    ) -> Self {
        let queue = Arc::new(PublisherQueue {
            state: Mutex::new(PublisherQueueState {
                items: VecDeque::new(),
                stop: false,
            }),
            signal: Condvar::new(),
        });
        let worker_queue = Arc::clone(&queue);
        let handle = thread::Builder::new()
            .name("nokv-fuse-publisher".to_owned())
            .spawn(move || run_worker(&*backend, &journal, &tracker, &worker_queue))
            .ok();
        Self { queue, handle }
    }

    pub(super) fn enqueue(&self, item: PendingPublish<B::Prepared>) {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        state.items.push_back(item);
        self.queue.signal.notify_one();
    }

    fn stop(&mut self) {
        {
            let mut state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            state.stop = true;
            self.queue.signal.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl<B: FuseBackend> Drop for PublisherWorker<B> {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The async-publish runtime owned by a mount: the durable journal, the
/// read-after-write tracker, and the background worker. Dropped — and so drained
/// + joined — when the mount ends.
pub(super) struct WritebackPublisher<B: FuseBackend> {
    pub(super) journal: Arc<PublishJournal>,
    pub(super) tracker: Arc<PendingPublishTracker>,
    worker: PublisherWorker<B>,
}

impl<B: FuseBackend> WritebackPublisher<B> {
    /// Open the journal, replay any publishes left pending by a prior crash, and
    /// spawn the worker to re-drive them. Recovery, in order:
    ///
    /// 1. **Replay** the journal for un-tombstoned records.
    /// 2. **Re-stage** each record's cached blocks + rebuild its prepared
    ///    artifact; a record whose cache is gone is dropped (accepted data loss —
    ///    never wedge).
    /// 3. **Compact** the journal to exactly the records being re-driven, before
    ///    the worker starts appending tombstones (single-threaded, no race).
    /// 4. **Spawn** the worker and hand it the recovered publishes. Its
    ///    idempotency gen-check makes "crashed after publish, before tombstone" a
    ///    no-op.
    /// 5. **Purge** cache files referenced by no live record (staged by a crash
    ///    before the record existed — unrecoverable garbage).
    ///
    /// An empty journal makes this a plain start.
    pub(super) fn recover(backend: Arc<B>, root: &Path) -> io::Result<Self> {
        let journal = Arc::new(PublishJournal::open(root)?);
        let tracker = Arc::new(PendingPublishTracker::new());

        let mut items: Vec<PendingPublish<B::Prepared>> = Vec::new();
        let mut live_records: Vec<PendingPublishRecord> = Vec::new();
        let mut live_files: HashSet<String> = HashSet::new();
        for record in journal.replay()? {
            // A record that cannot be re-staged (cache gone) yields None: accept
            // the loss by dropping it (compact omits it) rather than wedge.
            if let Some(item) = reconstruct_pending(&*backend, &record) {
                tracker.begin(item.inode, item.generation, item.parent, &item.name);
                for cache_file in &record.cache_files {
                    live_files.insert(cache_file.file_name.clone());
                }
                items.push(item);
                live_records.push(record);
            }
        }
        journal.compact(root, &live_records)?;

        let worker = PublisherWorker::spawn(
            Arc::clone(&backend),
            Arc::clone(&journal),
            Arc::clone(&tracker),
        );
        for item in items {
            worker.enqueue(item);
        }
        let _ = backend.purge_cache_orphans(&live_files);

        Ok(Self {
            journal,
            tracker,
            worker,
        })
    }

    pub(super) fn tracker(&self) -> &Arc<PendingPublishTracker> {
        &self.tracker
    }

    pub(super) fn journal(&self) -> &PublishJournal {
        &self.journal
    }

    pub(super) fn enqueue(&self, item: PendingPublish<B::Prepared>) {
        self.worker.enqueue(item);
    }
}

fn run_worker<B: FuseBackend>(
    backend: &B,
    journal: &PublishJournal,
    tracker: &PendingPublishTracker,
    queue: &PublisherQueue<B::Prepared>,
) {
    loop {
        let item = {
            let mut state = match queue.state.lock() {
                Ok(state) => state,
                Err(err) => err.into_inner(),
            };
            loop {
                if let Some(item) = state.items.pop_front() {
                    break Some(item);
                }
                // Drain-on-stop: only exit once the backlog is empty.
                if state.stop {
                    break None;
                }
                state = match queue.signal.wait(state) {
                    Ok(state) => state,
                    Err(err) => err.into_inner(),
                };
            }
        };
        match item {
            Some(item) => process_publish(backend, journal, tracker, item),
            None => return,
        }
    }
}

fn process_publish<B: FuseBackend>(
    backend: &B,
    journal: &PublishJournal,
    tracker: &PendingPublishTracker,
    item: PendingPublish<B::Prepared>,
) {
    let inode = item.inode;
    let generation = item.generation;
    let result = drive_publish(backend, journal, item);
    // Always complete so a read-after-write barrier never hangs: success lets
    // the waiter read fresh metadata; failure delivers the error.
    tracker.complete(inode, generation, result);
}

fn drive_publish<B: FuseBackend>(
    backend: &B,
    journal: &PublishJournal,
    item: PendingPublish<B::Prepared>,
) -> Result<(), Errno> {
    let PendingPublish {
        prepared,
        parent,
        name,
        manifest_id,
        inode,
        generation,
        size,
        mode,
        uid,
        gid,
        uploads,
    } = item;

    // Idempotency: a crash after the manifest commit but before the journal
    // tombstone leaves a pending record whose generation is already live.
    // Re-publishing would fail the dentry-version CAS, so skip straight to
    // evicting the redundant cache blocks + retiring the record.
    let published = backend
        .get_attr(inode)
        .map_err(errno)?
        .map_or(0, |attr| attr.generation);
    if published >= generation {
        discard_upload_caches(&uploads);
        journal
            .append_tombstone(inode.get(), generation)
            .map_err(|_| Errno::EIO)?;
        return Ok(());
    }

    // Drain the staged-block uploads, rebuilding the session manifest from their
    // results exactly as the synchronous close does via `drain_handle_uploads`.
    // A wait/publish error returns here without evicting or tombstoning, so the
    // journal record + cache blocks survive for mount-time recovery to re-drive.
    let mut writer = backend
        .new_write_pipeline(&prepared, &manifest_id)
        .map_err(errno)?;
    for upload in &uploads {
        let written = upload.pending.wait().map_err(errno)?;
        writer.record_write(written).map_err(errno)?;
    }

    publish_staged_session(
        backend,
        prepared,
        parent,
        name,
        manifest_id,
        size,
        mode,
        uid,
        gid,
        writer.staged_chunks(),
        writer.staged_objects(),
    )?;

    // Committed: the blocks are durable in the object store and the manifest
    // references them, so the local cache copies are now redundant.
    discard_upload_caches(&uploads);
    journal
        .append_tombstone(inode.get(), generation)
        .map_err(|_| Errno::EIO)?;
    Ok(())
}

fn discard_upload_caches(uploads: &[PendingBufferedUpload]) {
    for upload in uploads {
        let _ = upload.pending.discard_writeback_cache();
    }
}

/// Rebuild the in-flight publish for a journal record by re-staging its cached
/// blocks and reconstructing its prepared artifact. Returns `None` (so recovery
/// drops the record, accepting the loss) when the record is malformed or its
/// cache blocks can no longer be re-staged — e.g. the cache was wiped.
fn reconstruct_pending<B: FuseBackend>(
    backend: &B,
    record: &PendingPublishRecord,
) -> Option<PendingPublish<B::Prepared>> {
    let parent = InodeId::new(record.parent).ok()?;
    let inode = InodeId::new(record.inode).ok()?;
    let name = DentryName::new(record.name.clone()).ok()?;
    let fields = PreparedRecordFields {
        mount: record.mount,
        generation: record.generation,
        mtime_ms: record.mtime_ms,
        ctime_ms: record.ctime_ms,
        replace: record.replace,
        dentry_version: (record.dentry_version != 0).then_some(record.dentry_version),
        old_generation: (record.old_generation != 0).then_some(record.old_generation),
    };
    let prepared = backend.prepared_from_record_fields(parent, name.clone(), inode, fields);
    let blocks = record
        .cache_files
        .iter()
        .map(|cache_file| RecoveredBlock {
            logical_offset: cache_file.logical_offset,
            cache_key: cache_file.cache_key.clone(),
            file_name: cache_file.file_name.clone(),
            len: cache_file.len,
        })
        .collect::<Vec<_>>();
    let pending = backend
        .restage_cached_blocks(&prepared, &record.manifest_id, &blocks, 0)
        .ok()?;
    Some(PendingPublish {
        prepared,
        parent,
        name,
        manifest_id: record.manifest_id.clone(),
        inode,
        generation: record.generation,
        size: record.size,
        mode: record.mode,
        uid: record.uid,
        gid: record.gid,
        uploads: vec![PendingBufferedUpload {
            pending,
            ranges: Vec::new(),
        }],
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use nokv_meta::{
        DentryWithAttr, MetadError, PublishArtifactStagedSession, ReadDirPlusPage,
        RenameReplaceResult, UpdateAttr, XattrSetMode,
    };
    use nokv_object::{
        ChunkWriteOptions, ChunkedWrite, FileReadPipeline, FileWritePipeline, ObjectReadBlock,
        ObjectReadPlanCache, PendingChunkedWrite, StoredBlock, StoredChunk, DEFAULT_BLOCK_SIZE,
        DEFAULT_CHUNK_SIZE,
    };
    use nokv_types::{
        AdvisoryLock, AdvisoryLockRequest, DentryName, DentryRecord, FileType, InodeAttr, InodeId,
        SpecialNodeSpec, WatchCursor, WatchRecord,
    };

    use crate::backend::{FuseBackend, FuseBackendError, FuseBackendResult};

    use super::super::publish_journal::{CacheFileRef, PendingPublishRecord, PublishJournal};
    use super::super::write_session::{PendingBufferedRange, PendingBufferedUpload};
    use super::{PendingPublish, PendingPublishTracker, PublisherWorker, WritebackPublisher};

    fn unsupported<T>() -> FuseBackendResult<T> {
        Err(FuseBackendError::Metadata(MetadError::Codec(
            "publisher test backend".to_owned(),
        )))
    }

    fn inode(raw: u64) -> InodeId {
        InodeId::new(raw).unwrap()
    }

    fn name(raw: &[u8]) -> DentryName {
        DentryName::new(raw.to_vec()).unwrap()
    }

    #[derive(Clone)]
    struct MockPrepared {
        inode: u64,
        generation: u64,
        replace: bool,
        dentry_version: Option<u64>,
    }

    #[derive(Default)]
    struct MockState {
        /// Pre-seeded `inode -> published generation` for the idempotency check.
        published: HashMap<u64, u64>,
        /// `(inode, generation)` of every committed publish, in order.
        commits: Vec<(u64, u64)>,
        /// When set, the next publish call fails with this many remaining failures.
        fail_publishes: usize,
        /// When true, `restage_cached_blocks` fails (stands in for a wiped cache).
        fail_restage: bool,
    }

    #[derive(Clone, Default)]
    struct MockBackend {
        state: Arc<Mutex<MockState>>,
    }

    impl MockBackend {
        fn set_published(&self, ino: u64, generation: u64) {
            self.state.lock().unwrap().published.insert(ino, generation);
        }

        fn fail_next_publishes(&self, count: usize) {
            self.state.lock().unwrap().fail_publishes = count;
        }

        fn fail_restage(&self) {
            self.state.lock().unwrap().fail_restage = true;
        }

        fn commits(&self) -> Vec<(u64, u64)> {
            self.state.lock().unwrap().commits.clone()
        }
    }

    impl FuseBackend for MockBackend {
        type Prepared = MockPrepared;

        fn prepared_generation(&self, prepared: &Self::Prepared) -> u64 {
            prepared.generation
        }

        fn prepared_record_fields(
            &self,
            prepared: &Self::Prepared,
        ) -> crate::backend::PreparedRecordFields {
            crate::backend::PreparedRecordFields {
                mount: 1,
                generation: prepared.generation,
                mtime_ms: 0,
                ctime_ms: 0,
                replace: prepared.replace,
                dentry_version: prepared.dentry_version,
                old_generation: None,
            }
        }

        fn prepared_is_replace(&self, prepared: &Self::Prepared) -> bool {
            prepared.replace
        }

        fn rebind_prepared_dentry_version(&self, prepared: &mut Self::Prepared, version: u64) {
            prepared.dentry_version = Some(version);
        }

        fn get_attr(&self, ino: InodeId) -> FuseBackendResult<Option<InodeAttr>> {
            let generation = self
                .state
                .lock()
                .unwrap()
                .published
                .get(&ino.get())
                .copied();
            Ok(generation.map(|generation| InodeAttr {
                inode: ino,
                file_type: FileType::File,
                mode: 0o644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                size: 0,
                generation,
                mtime_ms: 0,
                ctime_ms: 0,
            }))
        }

        fn current_dentry_version(
            &self,
            _parent: InodeId,
            _name: &DentryName,
        ) -> FuseBackendResult<Option<u64>> {
            Ok(None)
        }

        fn new_write_pipeline(
            &self,
            prepared: &Self::Prepared,
            manifest_id: &str,
        ) -> FuseBackendResult<FileWritePipeline> {
            FileWritePipeline::new(ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: 1,
                inode: prepared.inode,
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            })
            .map_err(FuseBackendError::Object)
        }

        fn publish_prepared_artifact_staged_session(
            &self,
            prepared: Self::Prepared,
            request: PublishArtifactStagedSession,
        ) -> FuseBackendResult<RenameReplaceResult> {
            let mut state = self.state.lock().unwrap();
            if state.fail_publishes > 0 {
                state.fail_publishes -= 1;
                return Err(FuseBackendError::Metadata(MetadError::Codec(
                    "injected publish failure".to_owned(),
                )));
            }
            state.commits.push((prepared.inode, prepared.generation));
            state.published.insert(prepared.inode, prepared.generation);
            let attr = InodeAttr {
                inode: inode(prepared.inode),
                file_type: FileType::File,
                mode: request.mode,
                uid: request.uid,
                gid: request.gid,
                rdev: 0,
                nlink: 1,
                size: request.size,
                generation: prepared.generation,
                mtime_ms: 0,
                ctime_ms: 0,
            };
            Ok(RenameReplaceResult {
                entry: DentryWithAttr {
                    dentry: DentryRecord {
                        parent: request.parent,
                        name: request.name,
                        child: inode(prepared.inode),
                        child_type: FileType::File,
                        attr_generation: prepared.generation,
                    },
                    attr,
                    body: None,
                },
                replaced: None,
            })
        }

        // --- unused by the publisher worker ---------------------------------- //
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
            _attr: &InodeAttr,
            _offset: u64,
            _len: usize,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }
        fn read_file_with_known_attr_pipeline(
            &self,
            _attr: &InodeAttr,
            _offset: u64,
            _len: usize,
            _pipeline: &mut FileReadPipeline,
            _read_plans: &mut ObjectReadPlanCache,
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
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
        ) -> FuseBackendResult<Option<AdvisoryLock>> {
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
        fn stage_prepared_artifact_shared_ranges_async(
            &self,
            _prepared: &Self::Prepared,
            _manifest_id: &str,
            _ranges: &[PendingBufferedRange],
            _block_index_base: u64,
        ) -> FuseBackendResult<PendingChunkedWrite> {
            unsupported()
        }

        fn restage_cached_blocks(
            &self,
            prepared: &Self::Prepared,
            manifest_id: &str,
            blocks: &[crate::backend::RecoveredBlock],
            _block_index_base: u64,
        ) -> FuseBackendResult<PendingChunkedWrite> {
            if self.state.lock().unwrap().fail_restage {
                return Err(FuseBackendError::Object(nokv_object::ObjectError::Backend(
                    "writeback cache file is missing on reinsert".to_owned(),
                )));
            }
            let stored = blocks
                .iter()
                .enumerate()
                .map(|(index, block)| StoredBlock {
                    object_key: format!(
                        "blocks/1/{}/{}/0/{index}",
                        prepared.inode, prepared.generation
                    ),
                    logical_offset: block.logical_offset,
                    object_offset: 0,
                    len: block.len,
                    digest_uri: format!("xxh3-64:recovered-{index}"),
                })
                .collect::<Vec<_>>();
            let size = blocks.iter().map(|block| block.len).sum();
            Ok(PendingChunkedWrite::ready(Ok(ChunkedWrite {
                manifest_id: manifest_id.to_owned(),
                size,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
                chunks: vec![StoredChunk {
                    chunk_index: 0,
                    logical_offset: 0,
                    len: size,
                    blocks: stored,
                }],
                object_puts: blocks.len(),
                object_put_bytes: size,
            })))
        }

        fn prepared_from_record_fields(
            &self,
            _parent: InodeId,
            _name: DentryName,
            inode: InodeId,
            fields: crate::backend::PreparedRecordFields,
        ) -> Self::Prepared {
            MockPrepared {
                inode: inode.get(),
                generation: fields.generation,
                replace: fields.replace,
                dentry_version: fields.dentry_version,
            }
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
            unsupported()
        }
        fn read_session_object_blocks(
            &self,
            _output_len: usize,
            _blocks: &[ObjectReadBlock],
        ) -> FuseBackendResult<Vec<u8>> {
            unsupported()
        }
    }

    fn ready_upload(ino: u64, generation: u64) -> PendingBufferedUpload {
        let written = ChunkedWrite {
            manifest_id: format!("fuse/1/{ino}"),
            size: 4096,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
            chunks: vec![StoredChunk {
                chunk_index: 0,
                logical_offset: 0,
                len: 4096,
                blocks: vec![StoredBlock {
                    object_key: format!("blocks/1/{ino}/{generation}/0/0"),
                    logical_offset: 0,
                    object_offset: 0,
                    len: 4096,
                    digest_uri: "xxh3-64:0123456789abcdef".to_owned(),
                }],
            }],
            object_puts: 1,
            object_put_bytes: 4096,
        };
        PendingBufferedUpload {
            pending: PendingChunkedWrite::ready(Ok(written)),
            ranges: Vec::new(),
        }
    }

    fn sample_record(ino: u64, generation: u64) -> PendingPublishRecord {
        PendingPublishRecord {
            inode: ino,
            parent: 1,
            name: b"file.bin".to_vec(),
            mount: 1,
            generation,
            mtime_ms: 0,
            ctime_ms: 0,
            replace: false,
            dentry_version: 0,
            old_generation: 0,
            size: 4096,
            mode: 0o644,
            uid: 0,
            gid: 0,
            manifest_id: format!("fuse/1/{ino}"),
            cache_files: vec![CacheFileRef {
                logical_offset: 0,
                cache_key: format!("fuse/1/{ino}:{generation}:0:4096"),
                file_name: format!("abc-{generation:016x}.writeback"),
                len: 4096,
            }],
        }
    }

    fn pending_publish(ino: u64, generation: u64) -> PendingPublish<MockPrepared> {
        PendingPublish {
            prepared: MockPrepared {
                inode: ino,
                generation,
                replace: false,
                dentry_version: None,
            },
            parent: inode(1),
            name: name(b"file.bin"),
            manifest_id: format!("fuse/1/{ino}"),
            inode: inode(ino),
            generation,
            size: 4096,
            mode: 0o644,
            uid: 0,
            gid: 0,
            uploads: vec![ready_upload(ino, generation)],
        }
    }

    fn wait_until<F: Fn() -> bool>(predicate: F) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        predicate()
    }

    #[test]
    fn worker_publishes_once_and_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(PublishJournal::open(dir.path()).unwrap());
        journal.append_publish(&sample_record(5, 2)).unwrap();
        let backend = MockBackend::default();
        let tracker = Arc::new(PendingPublishTracker::new());
        let worker = PublisherWorker::spawn(
            Arc::new(backend.clone()),
            Arc::clone(&journal),
            Arc::clone(&tracker),
        );

        tracker.begin(inode(5), 2, inode(1), &name(b"file.bin"));
        worker.enqueue(pending_publish(5, 2));

        assert!(wait_until(|| backend.commits() == vec![(5, 2)]));
        // The read-after-write barrier resolves once the publish lands.
        assert!(tracker.wait_for(inode(5)).is_ok());
        drop(worker);
        // Tombstoned: replay returns nothing pending.
        assert!(journal.replay().unwrap().is_empty());
    }

    #[test]
    fn worker_skips_publish_when_generation_already_live() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(PublishJournal::open(dir.path()).unwrap());
        journal.append_publish(&sample_record(7, 3)).unwrap();
        let backend = MockBackend::default();
        // Crash-after-publish-before-tombstone: the generation is already live.
        backend.set_published(7, 3);
        let tracker = Arc::new(PendingPublishTracker::new());
        let worker = PublisherWorker::spawn(
            Arc::new(backend.clone()),
            Arc::clone(&journal),
            Arc::clone(&tracker),
        );

        tracker.begin(inode(7), 3, inode(1), &name(b"file.bin"));
        worker.enqueue(pending_publish(7, 3));

        // The barrier still resolves, and no duplicate publish is issued.
        assert!(tracker.wait_for(inode(7)).is_ok());
        assert!(wait_until(|| journal.replay().unwrap().is_empty()));
        assert!(backend.commits().is_empty());
        drop(worker);
    }

    #[test]
    fn worker_keeps_record_and_surfaces_error_on_publish_failure() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(PublishJournal::open(dir.path()).unwrap());
        journal.append_publish(&sample_record(9, 4)).unwrap();
        let backend = MockBackend::default();
        backend.fail_next_publishes(1);
        let tracker = Arc::new(PendingPublishTracker::new());
        let worker = PublisherWorker::spawn(
            Arc::new(backend.clone()),
            Arc::clone(&journal),
            Arc::clone(&tracker),
        );

        tracker.begin(inode(9), 4, inode(1), &name(b"file.bin"));
        worker.enqueue(pending_publish(9, 4));

        // The barrier surfaces the failure instead of hanging.
        assert!(wait_until(|| tracker.wait_for(inode(9)).is_err()));
        // The journal record survives for mount-time recovery to re-drive.
        let live = journal.replay().unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].inode, 9);
        assert!(backend.commits().is_empty());
        drop(worker);
    }

    #[test]
    fn stop_drains_enqueued_backlog() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(PublishJournal::open(dir.path()).unwrap());
        let backend = MockBackend::default();
        let tracker = Arc::new(PendingPublishTracker::new());
        let worker = PublisherWorker::spawn(
            Arc::new(backend.clone()),
            Arc::clone(&journal),
            Arc::clone(&tracker),
        );

        for ino in 10..20 {
            journal.append_publish(&sample_record(ino, 1)).unwrap();
            tracker.begin(inode(ino), 1, inode(1), &name(b"file.bin"));
            worker.enqueue(pending_publish(ino, 1));
        }
        // Drop joins after draining the whole backlog.
        drop(worker);

        let mut commits = backend.commits();
        commits.sort_unstable();
        let expected: Vec<(u64, u64)> = (10..20).map(|ino| (ino, 1)).collect();
        assert_eq!(commits, expected);
        assert!(journal.replay().unwrap().is_empty());
    }

    #[test]
    fn tracker_supersedes_older_generation() {
        let tracker = PendingPublishTracker::new();
        tracker.begin(inode(3), 1, inode(1), &name(b"f"));
        tracker.begin(inode(3), 2, inode(1), &name(b"f"));
        // Completing the stale generation must not release the barrier.
        tracker.complete(inode(3), 1, Ok(()));
        tracker.complete(inode(3), 2, Ok(()));
        assert!(tracker.wait_for(inode(3)).is_ok());
        assert!(tracker.wait_for_dentry(inode(1), &name(b"f")).is_ok());
    }

    #[test]
    fn read_barrier_blocks_until_publish_completes() {
        use crate::filesystem::{FuseOptions, FuseWritebackOptions, NoKvFuse};

        let dir = tempfile::tempdir().unwrap();
        let options = FuseOptions {
            writeback: FuseWritebackOptions {
                cache_enabled: true,
                async_publish: true,
                root: dir.path().to_path_buf(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut fuse = NoKvFuse::from_backend(MockBackend::default(), options);
        fuse.enable_async_publish().unwrap();
        let tracker = fuse.writeback_tracker().unwrap();

        // Nothing pending: the barrier returns immediately.
        assert!(fuse.wait_pending_publish(inode(5)).is_ok());

        // A pending publish parks the barrier until the worker resolves it. The
        // only thread that resolves it is the test (standing in for the worker),
        // so a parked FUSE handler cannot deadlock.
        tracker.begin(inode(5), 1, inode(1), &name(b"f"));
        thread::scope(|scope| {
            let blocked = scope.spawn(|| fuse.wait_pending_publish(inode(5)));
            thread::sleep(Duration::from_millis(30));
            assert!(!blocked.is_finished(), "barrier released before publish");
            tracker.complete(inode(5), 1, Ok(()));
            assert!(blocked.join().unwrap().is_ok());
        });

        // A failed publish surfaces as an error, not a hang.
        tracker.begin(inode(6), 1, inode(1), &name(b"g"));
        tracker.complete(inode(6), 1, Err(fuser::Errno::EIO));
        assert!(fuse.wait_pending_publish(inode(6)).is_err());
    }

    #[test]
    fn read_barrier_is_noop_without_async_publish() {
        use crate::filesystem::{FuseOptions, NoKvFuse};

        let fuse = NoKvFuse::from_backend(MockBackend::default(), FuseOptions::default());
        assert!(fuse.writeback_tracker().is_none());
        assert!(fuse.wait_pending_publish(inode(5)).is_ok());
        assert!(fuse
            .wait_pending_publish_dentry(inode(1), &name(b"f"))
            .is_ok());
    }

    #[test]
    fn recover_redrives_pending_publish_from_journal() {
        let dir = tempfile::tempdir().unwrap();
        {
            // A prior run acked a write but crashed before the worker published.
            let journal = PublishJournal::open(dir.path()).unwrap();
            journal.append_publish(&sample_record(5, 2)).unwrap();
        }
        let backend = MockBackend::default();
        let publisher = WritebackPublisher::recover(Arc::new(backend.clone()), dir.path()).unwrap();
        assert!(wait_until(|| backend.commits() == vec![(5, 2)]));
        // The read-after-write barrier for the recovered inode resolves once the
        // re-driven publish lands.
        assert!(publisher.tracker().wait_for(inode(5)).is_ok());
        drop(publisher);
        let journal = PublishJournal::open(dir.path()).unwrap();
        assert!(journal.replay().unwrap().is_empty());
    }

    #[test]
    fn recover_is_idempotent_for_already_published_generation() {
        let dir = tempfile::tempdir().unwrap();
        {
            let journal = PublishJournal::open(dir.path()).unwrap();
            journal.append_publish(&sample_record(7, 3)).unwrap();
        }
        let backend = MockBackend::default();
        // The prior run had already committed generation 3 before crashing.
        backend.set_published(7, 3);
        let publisher = WritebackPublisher::recover(Arc::new(backend.clone()), dir.path()).unwrap();
        drop(publisher);
        // Idempotent: no duplicate publish, and the record is retired.
        assert!(backend.commits().is_empty());
        let journal = PublishJournal::open(dir.path()).unwrap();
        assert!(journal.replay().unwrap().is_empty());
    }

    #[test]
    fn recover_accepts_loss_when_cache_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        {
            let journal = PublishJournal::open(dir.path()).unwrap();
            journal.append_publish(&sample_record(9, 4)).unwrap();
        }
        let backend = MockBackend::default();
        // The cache was wiped between runs: the blocks cannot be re-staged.
        backend.fail_restage();
        let publisher = WritebackPublisher::recover(Arc::new(backend.clone()), dir.path()).unwrap();
        drop(publisher);
        // Never wedge: the un-recoverable record is dropped, not retried forever.
        assert!(backend.commits().is_empty());
        let journal = PublishJournal::open(dir.path()).unwrap();
        assert!(journal.replay().unwrap().is_empty());
    }
}
