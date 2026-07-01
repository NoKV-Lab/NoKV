use std::collections::HashSet;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use nokv_control::ControlStore;
use nokv_meta::{DentryWithAttr, ObjectTransferStats, RenameReplaceResult};
use nokv_object::{
    BlockCache, BlockReadOptions, ChunkStore, ChunkWriteOptions, ChunkedWrite, DataFabricReadStats,
    LayoutReadExecutor, LayoutReadRequest, ObjectBlockCache, ObjectError, ObjectPrefetchOptions,
    ObjectPrefetchRequest, ObjectPrefetcher, ObjectReadBlock, ObjectReadPlan, ObjectReadPlanCache,
    ObjectReadPlanKey, ObjectStore, StagedObjectSet, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokv_types::{BodyDescriptor, ChunkManifest, FileType, InodeId, MountId};

use crate::read_cache::ReadPipelineCache;
use crate::service::{
    ClientPreparedArtifact, MetadataClient, PathLayoutOpen, PathLayoutOpenRequest,
};
use crate::{ArtifactMetadata, ClientError, NamespaceRead};

const MAX_READ_PIPELINES: usize = 1024;
const MAX_READ_PLAN_CACHE_ENTRIES: usize = 4096;
const MAX_BATCH_READ_WORKERS: usize = 32;
const MIN_RANGE_READAHEAD_BYTES: usize = DEFAULT_BLOCK_SIZE / 4;

struct BatchReadTask {
    index: usize,
    offset: u64,
    pipeline_key: String,
    open: PathLayoutOpen,
}

#[derive(Clone, Debug)]
struct RangeReadTask {
    index: usize,
    offset: u64,
    len: usize,
    end: u64,
}

#[derive(Clone, Debug)]
struct RangeReadWindow {
    offset: u64,
    end: u64,
    ranges: Vec<RangeReadTask>,
}

struct RangeBatchWindowTask {
    window: RangeReadWindow,
    open: PathLayoutOpen,
}

struct RangeBatchRequestTask {
    index: usize,
    path: String,
    reads: Vec<Option<Vec<u8>>>,
    windows: Vec<RangeBatchWindowTask>,
}

struct RangeBatchPackedRequestTask {
    index: usize,
    path: String,
    packed: Vec<u8>,
    offsets: Vec<usize>,
    windows: Vec<RangeBatchWindowTask>,
}

struct RangeBatchIntoRequestTask {
    index: usize,
    path: String,
    output_offset: usize,
    output_len: usize,
    offsets: Vec<usize>,
    windows: Vec<RangeBatchWindowTask>,
}

#[derive(Clone, Debug)]
struct PreparedRangeBatchIntoRequestTask {
    index: usize,
    path: String,
    output_offset: usize,
    output_len: usize,
    offsets: Vec<usize>,
    windows: Vec<RangeReadWindow>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PreparedRangeBatchOpenTarget {
    request_index: usize,
    window_index: usize,
}

struct PreparedRangeBatchWindowTask<'a> {
    window: &'a RangeReadWindow,
    open: PathLayoutOpen,
}

struct PreparedRangeBatchIntoTask<'a> {
    plan: &'a PreparedRangeBatchIntoRequestTask,
    windows: Vec<PreparedRangeBatchWindowTask<'a>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RangeBatchOutputRegion {
    offset: usize,
    len: usize,
    index: usize,
}

struct ScatterPackedRangePlan {
    output_start: usize,
    expands_physical_reads: bool,
    plan: ObjectReadPlan,
}

struct PlannedObjectReadIntoRequest<'a> {
    pipeline_key: &'a str,
    inode: InodeId,
    generation: u64,
    file_size: u64,
    offset: u64,
    plan: &'a ObjectReadPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PathReadRange {
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathRangeReadRequest {
    pub path: String,
    pub ranges: Vec<PathReadRange>,
    pub expected_generation: Option<u64>,
    pub max_gap_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct PreparedPathRangeBatch {
    tasks: Vec<PreparedRangeBatchIntoRequestTask>,
    open_requests: Vec<PathLayoutOpenRequest>,
    open_targets: Vec<PreparedRangeBatchOpenTarget>,
    request_offsets: Vec<usize>,
    request_lengths: Vec<usize>,
    range_count: usize,
    output_len: usize,
}

pub struct NoKvFsClient<O> {
    metadata: MetadataClient,
    objects: Arc<O>,
    block_cache: ObjectBlockCache,
    prefetcher: ObjectPrefetcher<Arc<O>>,
    read_pipelines: Mutex<ReadPipelineCache>,
    read_plans: Mutex<ObjectReadPlanCache>,
    block_cache_enabled: bool,
    object_puts: AtomicU64,
    object_put_bytes: AtomicU64,
    object_gets: AtomicU64,
    object_get_bytes: AtomicU64,
    coalesced_gets: AtomicU64,
    coalesced_get_bytes: AtomicU64,
    cache_hits: AtomicU64,
    cache_hit_bytes: AtomicU64,
    read_plan_cache_hits: AtomicU64,
    read_plan_cache_misses: AtomicU64,
    manifest_chunks: AtomicU64,
    manifest_blocks: AtomicU64,
    data_fabric_stats: Mutex<DataFabricReadStats>,
}

impl<O> NoKvFsClient<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    pub fn new(metadata: MetadataClient, objects: O) -> Self {
        Self::with_block_cache(metadata, objects, ObjectBlockCache::default())
    }

    pub fn with_block_cache(
        metadata: MetadataClient,
        objects: O,
        block_cache: ObjectBlockCache,
    ) -> Self {
        let objects = Arc::new(objects);
        let prefetcher = ObjectPrefetcher::new(
            Arc::clone(&objects),
            block_cache.clone(),
            ObjectPrefetchOptions::default(),
        );
        Self {
            metadata,
            objects,
            block_cache,
            prefetcher,
            read_pipelines: Mutex::new(ReadPipelineCache::new(MAX_READ_PIPELINES)),
            read_plans: Mutex::new(ObjectReadPlanCache::new(MAX_READ_PLAN_CACHE_ENTRIES)),
            block_cache_enabled: true,
            object_puts: AtomicU64::new(0),
            object_put_bytes: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            object_get_bytes: AtomicU64::new(0),
            coalesced_gets: AtomicU64::new(0),
            coalesced_get_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_hit_bytes: AtomicU64::new(0),
            read_plan_cache_hits: AtomicU64::new(0),
            read_plan_cache_misses: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
            data_fabric_stats: Mutex::new(DataFabricReadStats::default()),
        }
    }

    pub fn connect(address: SocketAddr, objects: O) -> Self {
        Self::new(MetadataClient::connect(address), objects)
    }

    /// Build a client whose metadata RPCs route to the owning shard's endpoint,
    /// resolved from the control plane. Use this against a multi-shard fleet;
    /// [`NoKvFsClient::connect`] stays single-shard for direct/dev deployments.
    pub fn connect_fleet(
        control: Arc<dyn ControlStore>,
        mount: MountId,
        objects: O,
    ) -> Result<Self, ClientError> {
        Ok(Self::new(MetadataClient::fleet(control, mount)?, objects))
    }

    pub fn metadata(&self) -> &MetadataClient {
        &self.metadata
    }

    pub fn set_block_cache_enabled(&mut self, enabled: bool) {
        self.block_cache_enabled = enabled;
    }

    pub fn block_cache_enabled(&self) -> bool {
        self.block_cache_enabled
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        let prefetch = self.prefetcher.stats().unwrap_or_default();
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_put_bytes: self.object_put_bytes.load(Ordering::Relaxed),
            object_gets: self
                .object_gets
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.object_gets),
            object_get_bytes: self
                .object_get_bytes
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.object_get_bytes),
            coalesced_gets: self.coalesced_gets.load(Ordering::Relaxed),
            coalesced_get_bytes: self.coalesced_get_bytes.load(Ordering::Relaxed),
            cache_hits: self
                .cache_hits
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.cache_hits),
            cache_hit_bytes: self
                .cache_hit_bytes
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.cache_hit_bytes),
            prefetch_enqueued: prefetch.enqueued,
            prefetch_dropped: prefetch.dropped,
            prefetch_completed: prefetch.completed,
            prefetch_failed: prefetch.failed,
            prefetch_object_gets: prefetch.object_gets,
            prefetch_object_get_bytes: prefetch.object_get_bytes,
            prefetch_cache_hits: prefetch.cache_hits,
            prefetch_cache_hit_bytes: prefetch.cache_hit_bytes,
            read_plan_cache_hits: self.read_plan_cache_hits.load(Ordering::Relaxed),
            read_plan_cache_misses: self.read_plan_cache_misses.load(Ordering::Relaxed),
            object_writeback_enqueued: 0,
            object_writeback_inline: 0,
            object_writeback_completed: 0,
            object_writeback_failed: 0,
            object_writeback_staged_bytes: 0,
            object_writeback_uploaded_bytes: 0,
            object_writeback_queue_wait_ns: 0,
            object_writeback_queue_max_wait_ns: 0,
            object_writeback_upload_ns: 0,
            object_writeback_upload_max_ns: 0,
            object_writeback_collect_ns: 0,
            object_writeback_digest_ns: 0,
            object_writeback_store_put_ns: 0,
            object_writeback_cache_put_ns: 0,
            manifest_chunks: self.manifest_chunks.load(Ordering::Relaxed),
            manifest_blocks: self.manifest_blocks.load(Ordering::Relaxed),
        }
    }

    pub fn data_fabric_stats(&self) -> Result<DataFabricReadStats, ClientError> {
        self.data_fabric_stats
            .lock()
            .map(|stats| *stats)
            .map_err(|err| ClientError::Protocol(format!("data fabric stats lock poisoned: {err}")))
    }

    pub fn cat(&self, path: &str) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, 0, file_len(entry.attr.size)?)
    }

    pub fn cat_snapshot(&self, snapshot_id: u64, path: &str) -> Result<Vec<u8>, ClientError> {
        self.metadata.read_artifact_at_snapshot(snapshot_id, path)
    }

    pub fn read_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        self.metadata
            .read_file_path_at_snapshot(snapshot_id, path, offset, len)
    }

    pub fn read(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, offset, len)
    }

    pub fn read_path(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<NamespaceRead, ClientError> {
        let open = self
            .metadata
            .open_path_read_plan(path, offset, len, expected_generation)?;
        let generation = open.lease.generation;
        let bytes = self.read_planned_object_blocks(
            &read_pipeline_key(path, generation),
            open.metadata.attr.inode,
            generation,
            open.metadata.attr.size,
            offset,
            &open.plan,
        )?;
        Ok(NamespaceRead {
            metadata: open.metadata,
            bytes,
        })
    }

    pub fn read_paths(
        &self,
        requests: &[PathLayoutOpenRequest],
    ) -> Result<Vec<NamespaceRead>, ClientError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let opens = self.metadata.open_path_read_plan_batch(requests)?;
        if opens.len() != requests.len() {
            return Err(ClientError::Protocol(format!(
                "metadata returned {} batch read plans for {} requests",
                opens.len(),
                requests.len()
            )));
        }
        let mut seen_pipeline_keys = HashSet::with_capacity(opens.len());
        let mut has_duplicate_pipeline = false;
        let tasks = opens
            .into_iter()
            .zip(requests)
            .enumerate()
            .map(|(index, (open, request))| {
                let pipeline_key = read_pipeline_key(&request.path, open.lease.generation);
                if !seen_pipeline_keys.insert(pipeline_key.clone()) {
                    has_duplicate_pipeline = true;
                }
                BatchReadTask {
                    index,
                    offset: request.offset,
                    pipeline_key,
                    open,
                }
            })
            .collect::<Vec<_>>();
        if has_duplicate_pipeline || tasks.len() == 1 {
            return self.read_path_batch_tasks_sequential(tasks);
        }
        self.read_path_batch_tasks_parallel(tasks)
    }

    fn read_path_batch_tasks_sequential(
        &self,
        tasks: Vec<BatchReadTask>,
    ) -> Result<Vec<NamespaceRead>, ClientError> {
        tasks
            .into_iter()
            .map(|task| self.read_path_batch_task(task).map(|(_, read)| read))
            .collect()
    }

    fn read_path_batch_tasks_parallel(
        &self,
        tasks: Vec<BatchReadTask>,
    ) -> Result<Vec<NamespaceRead>, ClientError> {
        let mut reads = (0..tasks.len()).map(|_| None).collect::<Vec<_>>();
        let mut tasks = tasks.into_iter();
        loop {
            let chunk = tasks
                .by_ref()
                .take(MAX_BATCH_READ_WORKERS)
                .collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            let results = thread::scope(|scope| {
                let handles = chunk
                    .into_iter()
                    .map(|task| scope.spawn(move || self.read_path_batch_task(task)))
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle.join().unwrap_or_else(|_| {
                            Err(ClientError::Protocol(
                                "batch path read worker panicked".to_owned(),
                            ))
                        })
                    })
                    .collect::<Vec<_>>()
            });
            for result in results {
                let (index, read) = result?;
                reads[index] = Some(read);
            }
        }
        reads
            .into_iter()
            .map(|read| {
                read.ok_or_else(|| {
                    ClientError::Protocol("batch path read did not produce every result".to_owned())
                })
            })
            .collect()
    }

    fn read_path_batch_task(
        &self,
        task: BatchReadTask,
    ) -> Result<(usize, NamespaceRead), ClientError> {
        let generation = task.open.lease.generation;
        let bytes = self.read_planned_object_blocks(
            &task.pipeline_key,
            task.open.metadata.attr.inode,
            generation,
            task.open.metadata.attr.size,
            task.offset,
            &task.open.plan,
        )?;
        Ok((
            task.index,
            NamespaceRead {
                metadata: task.open.metadata,
                bytes,
            },
        ))
    }

    pub fn read_path_ranges(
        &self,
        path: &str,
        ranges: &[(u64, usize)],
        expected_generation: Option<u64>,
        max_gap_bytes: u64,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let mut reads = (0..ranges.len()).map(|_| None).collect::<Vec<_>>();
        let mut tasks = Vec::with_capacity(ranges.len());
        for (index, (offset, len)) in ranges.iter().copied().enumerate() {
            if len == 0 {
                reads[index] = Some(Vec::new());
                continue;
            }
            let end = checked_read_range_end(offset, len)?;
            tasks.push(RangeReadTask {
                index,
                offset,
                len,
                end,
            });
        }
        if tasks.is_empty() {
            return collect_range_reads(reads);
        }
        let windows = coalesce_range_read_tasks(tasks, max_gap_bytes);
        let mut effective_generation = expected_generation;
        for window_index in 0..windows.len() {
            let window = &windows[window_index];
            let window_len = range_read_window_len(window)?;
            let open = self.metadata.open_path_read_plan(
                path,
                window.offset,
                window_len,
                effective_generation,
            )?;
            let generation = open.lease.generation;
            if effective_generation.is_none() {
                effective_generation = Some(generation);
            }
            self.prefetch_range_read_window(
                open.metadata.attr.inode,
                generation,
                windows.get(window_index + 1),
            );
            let bytes = self.read_planned_object_blocks(
                &read_pipeline_key(path, generation),
                open.metadata.attr.inode,
                generation,
                open.metadata.attr.size,
                window.offset,
                &open.plan,
            )?;
            fill_range_reads(&mut reads, window, &bytes)?;
        }
        collect_range_reads(reads)
    }

    pub fn read_path_ranges_batch(
        &self,
        requests: &[PathRangeReadRequest],
    ) -> Result<Vec<Vec<Vec<u8>>>, ClientError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut tasks = requests
            .iter()
            .enumerate()
            .map(|(index, request)| RangeBatchRequestTask {
                index,
                path: request.path.clone(),
                reads: (0..request.ranges.len()).map(|_| None).collect(),
                windows: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut open_requests = Vec::new();
        let mut open_targets = Vec::new();
        for (request_index, request) in requests.iter().enumerate() {
            let mut range_tasks = Vec::with_capacity(request.ranges.len());
            for (range_index, range) in request.ranges.iter().copied().enumerate() {
                if range.len == 0 {
                    tasks[request_index].reads[range_index] = Some(Vec::new());
                    continue;
                }
                let end = checked_read_range_end(range.offset, range.len)?;
                range_tasks.push(RangeReadTask {
                    index: range_index,
                    offset: range.offset,
                    len: range.len,
                    end,
                });
            }
            for window in coalesce_range_read_tasks(range_tasks, request.max_gap_bytes) {
                let window_len = range_read_window_len(&window)?;
                let mut open = PathLayoutOpenRequest::new(&request.path, window.offset, window_len);
                open.expected_generation = request.expected_generation;
                open_requests.push(open);
                open_targets.push((request_index, window));
            }
        }
        if !open_requests.is_empty() {
            let opens = self.metadata.open_path_read_plan_batch(&open_requests)?;
            if opens.len() != open_targets.len() {
                return Err(ClientError::Protocol(format!(
                    "metadata returned {} batch range read plans for {} windows",
                    opens.len(),
                    open_targets.len()
                )));
            }
            for ((request_index, window), open) in open_targets.into_iter().zip(opens) {
                tasks[request_index]
                    .windows
                    .push(RangeBatchWindowTask { window, open });
            }
        }
        self.read_range_batch_request_tasks(tasks)
    }

    pub fn read_path_ranges_batch_packed(
        &self,
        requests: &[PathRangeReadRequest],
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut tasks = requests
            .iter()
            .enumerate()
            .map(|(index, request)| {
                let mut offsets = Vec::with_capacity(request.ranges.len());
                let mut total_len = 0_usize;
                for range in &request.ranges {
                    offsets.push(total_len);
                    total_len = total_len.checked_add(range.len).ok_or_else(|| {
                        ClientError::Protocol("packed range read length exceeds usize".to_owned())
                    })?;
                }
                Ok(RangeBatchPackedRequestTask {
                    index,
                    path: request.path.clone(),
                    packed: vec![0_u8; total_len],
                    offsets,
                    windows: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, ClientError>>()?;
        let mut open_requests = Vec::new();
        let mut open_targets = Vec::new();
        for (request_index, request) in requests.iter().enumerate() {
            let mut range_tasks = Vec::with_capacity(request.ranges.len());
            for (range_index, range) in request.ranges.iter().copied().enumerate() {
                if range.len == 0 {
                    continue;
                }
                let end = checked_read_range_end(range.offset, range.len)?;
                range_tasks.push(RangeReadTask {
                    index: range_index,
                    offset: range.offset,
                    len: range.len,
                    end,
                });
            }
            for window in coalesce_range_read_tasks(range_tasks, request.max_gap_bytes) {
                let window_len = range_read_window_len(&window)?;
                let mut open = PathLayoutOpenRequest::new(&request.path, window.offset, window_len);
                open.expected_generation = request.expected_generation;
                open_requests.push(open);
                open_targets.push((request_index, window));
            }
        }
        if !open_requests.is_empty() {
            let opens = self.metadata.open_path_read_plan_batch(&open_requests)?;
            if opens.len() != open_targets.len() {
                return Err(ClientError::Protocol(format!(
                    "metadata returned {} packed batch range read plans for {} windows",
                    opens.len(),
                    open_targets.len()
                )));
            }
            for ((request_index, window), open) in open_targets.into_iter().zip(opens) {
                tasks[request_index]
                    .windows
                    .push(RangeBatchWindowTask { window, open });
            }
        }
        self.read_range_batch_packed_request_tasks(tasks)
    }

    pub fn prepare_path_ranges_batch(
        &self,
        requests: &[PathRangeReadRequest],
    ) -> Result<PreparedPathRangeBatch, ClientError> {
        PreparedPathRangeBatch::new(requests)
    }

    pub fn read_path_ranges_batch_into(
        &self,
        requests: &[PathRangeReadRequest],
        output: &mut [u8],
        request_offsets: &[usize],
    ) -> Result<Vec<usize>, ClientError> {
        if requests.len() != request_offsets.len() {
            return Err(ClientError::Protocol(format!(
                "packed range read into got {} output offsets for {} requests",
                request_offsets.len(),
                requests.len()
            )));
        }
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut tasks = requests
            .iter()
            .zip(request_offsets)
            .enumerate()
            .map(|(index, (request, output_offset))| {
                let mut offsets = Vec::with_capacity(request.ranges.len());
                let mut total_len = 0_usize;
                for range in &request.ranges {
                    offsets.push(total_len);
                    total_len = total_len.checked_add(range.len).ok_or_else(|| {
                        ClientError::Protocol(
                            "packed range read into length exceeds usize".to_owned(),
                        )
                    })?;
                }
                let output_end = output_offset.checked_add(total_len).ok_or_else(|| {
                    ClientError::Protocol(
                        "packed range read into output end exceeds usize".to_owned(),
                    )
                })?;
                if output_end > output.len() {
                    return Err(ClientError::Protocol(
                        "packed range read into output buffer is too small".to_owned(),
                    ));
                }
                Ok(RangeBatchIntoRequestTask {
                    index,
                    path: request.path.clone(),
                    output_offset: *output_offset,
                    output_len: total_len,
                    offsets,
                    windows: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, ClientError>>()?;
        validate_range_batch_output_regions(
            tasks.iter().map(|task| RangeBatchOutputRegion {
                offset: task.output_offset,
                len: task.output_len,
                index: task.index,
            }),
            output.len(),
        )?;

        let mut open_requests = Vec::new();
        let mut open_targets = Vec::new();
        for (request_index, request) in requests.iter().enumerate() {
            let mut range_tasks = Vec::with_capacity(request.ranges.len());
            for (range_index, range) in request.ranges.iter().copied().enumerate() {
                if range.len == 0 {
                    continue;
                }
                let end = checked_read_range_end(range.offset, range.len)?;
                range_tasks.push(RangeReadTask {
                    index: range_index,
                    offset: range.offset,
                    len: range.len,
                    end,
                });
            }
            for window in coalesce_range_read_tasks(range_tasks, request.max_gap_bytes) {
                let window_len = range_read_window_len(&window)?;
                let mut open = PathLayoutOpenRequest::new(&request.path, window.offset, window_len);
                open.expected_generation = request.expected_generation;
                open_requests.push(open);
                open_targets.push((request_index, window));
            }
        }
        if !open_requests.is_empty() {
            let opens = self.metadata.open_path_read_plan_batch(&open_requests)?;
            if opens.len() != open_targets.len() {
                return Err(ClientError::Protocol(format!(
                    "metadata returned {} packed into batch range read plans for {} windows",
                    opens.len(),
                    open_targets.len()
                )));
            }
            for ((request_index, window), open) in open_targets.into_iter().zip(opens) {
                tasks[request_index]
                    .windows
                    .push(RangeBatchWindowTask { window, open });
            }
        }
        self.read_range_batch_into_request_tasks(tasks, output)
    }

    pub fn read_prepared_path_ranges_batch_into(
        &self,
        plan: &PreparedPathRangeBatch,
        output: &mut [u8],
    ) -> Result<Vec<usize>, ClientError> {
        if output.len() < plan.output_len {
            return Err(ClientError::Protocol(
                "packed range read into output buffer is too small".to_owned(),
            ));
        }
        if plan.tasks.is_empty() {
            return Ok(Vec::new());
        }
        let mut task_windows = plan
            .tasks
            .iter()
            .map(|task| Vec::with_capacity(task.windows.len()))
            .collect::<Vec<_>>();
        if !plan.open_requests.is_empty() {
            let opens = self
                .metadata
                .open_path_read_plan_batch(&plan.open_requests)?;
            if opens.len() != plan.open_targets.len() {
                return Err(ClientError::Protocol(format!(
                    "metadata returned {} prepared range read plans for {} windows",
                    opens.len(),
                    plan.open_targets.len()
                )));
            }
            for (target, open) in plan.open_targets.iter().copied().zip(opens) {
                let task = plan.tasks.get(target.request_index).ok_or_else(|| {
                    ClientError::Protocol("prepared range read target is out of bounds".to_owned())
                })?;
                let window = task.windows.get(target.window_index).ok_or_else(|| {
                    ClientError::Protocol("prepared range read target is out of bounds".to_owned())
                })?;
                task_windows
                    .get_mut(target.request_index)
                    .ok_or_else(|| {
                        ClientError::Protocol(
                            "prepared range read target is out of bounds".to_owned(),
                        )
                    })?
                    .push(PreparedRangeBatchWindowTask { window, open });
            }
        }
        self.read_prepared_range_batch_into_request_tasks(plan, task_windows, output)
    }

    fn read_range_batch_request_tasks(
        &self,
        tasks: Vec<RangeBatchRequestTask>,
    ) -> Result<Vec<Vec<Vec<u8>>>, ClientError> {
        let mut outputs = (0..tasks.len()).map(|_| None).collect::<Vec<_>>();
        let mut active = Vec::new();
        let mut seen_paths = HashSet::with_capacity(tasks.len());
        let mut has_duplicate_path = false;
        for task in tasks {
            if task.windows.is_empty() {
                let index = task.index;
                outputs[index] = Some(collect_range_reads(task.reads)?);
                continue;
            }
            if !seen_paths.insert(task.path.clone()) {
                has_duplicate_path = true;
            }
            active.push(task);
        }
        if active.len() == 1 || has_duplicate_path {
            for task in active {
                let (index, reads) = self.read_range_batch_request_task(task)?;
                outputs[index] = Some(reads);
            }
        } else {
            let mut active = active.into_iter();
            loop {
                let chunk = active
                    .by_ref()
                    .take(MAX_BATCH_READ_WORKERS)
                    .collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                let results = thread::scope(|scope| {
                    let handles = chunk
                        .into_iter()
                        .map(|task| scope.spawn(move || self.read_range_batch_request_task(task)))
                        .collect::<Vec<_>>();
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle.join().unwrap_or_else(|_| {
                                Err(ClientError::Protocol(
                                    "batch range read worker panicked".to_owned(),
                                ))
                            })
                        })
                        .collect::<Vec<_>>()
                });
                for result in results {
                    let (index, reads) = result?;
                    outputs[index] = Some(reads);
                }
            }
        }
        outputs
            .into_iter()
            .map(|reads| {
                reads.ok_or_else(|| {
                    ClientError::Protocol(
                        "batch range read did not produce every result".to_owned(),
                    )
                })
            })
            .collect()
    }

    fn read_range_batch_request_task(
        &self,
        mut task: RangeBatchRequestTask,
    ) -> Result<(usize, Vec<Vec<u8>>), ClientError> {
        for window_index in 0..task.windows.len() {
            let window_task = &task.windows[window_index];
            let generation = window_task.open.lease.generation;
            self.prefetch_range_read_window(
                window_task.open.metadata.attr.inode,
                generation,
                task.windows.get(window_index + 1).map(|next| &next.window),
            );
            let bytes = self.read_planned_object_blocks(
                &read_pipeline_key(&task.path, generation),
                window_task.open.metadata.attr.inode,
                generation,
                window_task.open.metadata.attr.size,
                window_task.window.offset,
                &window_task.open.plan,
            )?;
            fill_range_reads(&mut task.reads, &window_task.window, &bytes)?;
        }
        Ok((task.index, collect_range_reads(task.reads)?))
    }

    fn read_range_batch_packed_request_tasks(
        &self,
        tasks: Vec<RangeBatchPackedRequestTask>,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let mut outputs = (0..tasks.len()).map(|_| None).collect::<Vec<_>>();
        let mut active = Vec::new();
        let mut seen_paths = HashSet::with_capacity(tasks.len());
        let mut has_duplicate_path = false;
        for task in tasks {
            if task.windows.is_empty() {
                let index = task.index;
                outputs[index] = Some(task.packed);
                continue;
            }
            if !seen_paths.insert(task.path.clone()) {
                has_duplicate_path = true;
            }
            active.push(task);
        }
        if active.len() == 1 || has_duplicate_path {
            for task in active {
                let (index, packed) = self.read_range_batch_packed_request_task(task)?;
                outputs[index] = Some(packed);
            }
        } else {
            let mut active = active.into_iter();
            loop {
                let chunk = active
                    .by_ref()
                    .take(MAX_BATCH_READ_WORKERS)
                    .collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                let results = thread::scope(|scope| {
                    let handles = chunk
                        .into_iter()
                        .map(|task| {
                            scope.spawn(move || self.read_range_batch_packed_request_task(task))
                        })
                        .collect::<Vec<_>>();
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle.join().unwrap_or_else(|_| {
                                Err(ClientError::Protocol(
                                    "packed batch range read worker panicked".to_owned(),
                                ))
                            })
                        })
                        .collect::<Vec<_>>()
                });
                for result in results {
                    let (index, packed) = result?;
                    outputs[index] = Some(packed);
                }
            }
        }
        outputs
            .into_iter()
            .map(|packed| {
                packed.ok_or_else(|| {
                    ClientError::Protocol(
                        "packed batch range read did not produce every result".to_owned(),
                    )
                })
            })
            .collect()
    }

    fn read_range_batch_packed_request_task(
        &self,
        mut task: RangeBatchPackedRequestTask,
    ) -> Result<(usize, Vec<u8>), ClientError> {
        for window_index in 0..task.windows.len() {
            let window_task = &task.windows[window_index];
            let generation = window_task.open.lease.generation;
            self.prefetch_range_read_window(
                window_task.open.metadata.attr.inode,
                generation,
                task.windows.get(window_index + 1).map(|next| &next.window),
            );
            if let Some(scatter) = scatter_packed_range_plan(
                &task.offsets,
                &window_task.window,
                &window_task.open.plan,
            )? {
                let output_end = scatter.output_end()?;
                if scatter.expands_physical_reads
                    && self.try_fill_scatter_from_block_cache(
                        &scatter,
                        &mut task.packed[scatter.output_start..output_end],
                    )?
                {
                    continue;
                }
                if scatter.expands_physical_reads {
                    let bytes = self.read_planned_object_blocks(
                        &read_pipeline_key(&task.path, generation),
                        window_task.open.metadata.attr.inode,
                        generation,
                        window_task.open.metadata.attr.size,
                        window_task.window.offset,
                        &window_task.open.plan,
                    )?;
                    fill_packed_range_reads(
                        &mut task.packed,
                        &task.offsets,
                        &window_task.window,
                        &bytes,
                    )?;
                    continue;
                }
                let pipeline_key = read_pipeline_key(&task.path, generation);
                self.read_planned_object_blocks_into(
                    PlannedObjectReadIntoRequest {
                        pipeline_key: &pipeline_key,
                        inode: window_task.open.metadata.attr.inode,
                        generation,
                        file_size: window_task.open.metadata.attr.size,
                        offset: window_task.window.offset,
                        plan: &scatter.plan,
                    },
                    &mut task.packed[scatter.output_start..output_end],
                )?;
                continue;
            }
            let bytes = self.read_planned_object_blocks(
                &read_pipeline_key(&task.path, generation),
                window_task.open.metadata.attr.inode,
                generation,
                window_task.open.metadata.attr.size,
                window_task.window.offset,
                &window_task.open.plan,
            )?;
            fill_packed_range_reads(&mut task.packed, &task.offsets, &window_task.window, &bytes)?;
        }
        Ok((task.index, task.packed))
    }

    fn read_range_batch_into_request_tasks(
        &self,
        tasks: Vec<RangeBatchIntoRequestTask>,
        output: &mut [u8],
    ) -> Result<Vec<usize>, ClientError> {
        let mut outputs = (0..tasks.len()).map(|_| None).collect::<Vec<_>>();
        let mut active = Vec::new();
        let mut seen_paths = HashSet::with_capacity(tasks.len());
        let mut has_duplicate_path = false;
        for task in tasks {
            if task.windows.is_empty() {
                let index = task.index;
                outputs[index] = Some(task.output_len);
                continue;
            }
            if !seen_paths.insert(task.path.clone()) {
                has_duplicate_path = true;
            }
            active.push(task);
        }
        if active.len() == 1 || has_duplicate_path {
            for task in active {
                let start = task.output_offset;
                let end = start.checked_add(task.output_len).ok_or_else(|| {
                    ClientError::Protocol(
                        "packed range read into output end exceeds usize".to_owned(),
                    )
                })?;
                let (index, len) =
                    self.read_range_batch_into_request_task(task, &mut output[start..end])?;
                outputs[index] = Some(len);
            }
        } else {
            active.sort_by_key(|task| task.output_offset);
            let mut active = active.into_iter();
            loop {
                let chunk = active
                    .by_ref()
                    .take(MAX_BATCH_READ_WORKERS)
                    .collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                let results = thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(chunk.len());
                    let mut remaining = &mut output[..];
                    let mut remaining_offset = 0_usize;
                    for task in chunk {
                        let gap = task
                            .output_offset
                            .checked_sub(remaining_offset)
                            .ok_or_else(|| {
                                ClientError::Protocol(
                                    "packed range read into output regions are out of order"
                                        .to_owned(),
                                )
                            })?;
                        let (_, after_gap) = remaining.split_at_mut(gap);
                        let (task_output, after_task) = after_gap.split_at_mut(task.output_len);
                        remaining = after_task;
                        remaining_offset = task
                            .output_offset
                            .checked_add(task.output_len)
                            .ok_or_else(|| {
                                ClientError::Protocol(
                                    "packed range read into output end exceeds usize".to_owned(),
                                )
                            })?;
                        handles.push(scope.spawn(move || {
                            self.read_range_batch_into_request_task(task, task_output)
                        }));
                    }
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle.join().unwrap_or_else(|_| {
                                Err(ClientError::Protocol(
                                    "packed into batch range read worker panicked".to_owned(),
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, ClientError>>()
                })?;
                for (index, len) in results {
                    outputs[index] = Some(len);
                }
            }
        }
        outputs
            .into_iter()
            .map(|len| {
                len.ok_or_else(|| {
                    ClientError::Protocol(
                        "packed range read into did not produce every result".to_owned(),
                    )
                })
            })
            .collect()
    }

    fn read_prepared_range_batch_into_request_tasks<'a>(
        &self,
        plan: &'a PreparedPathRangeBatch,
        task_windows: Vec<Vec<PreparedRangeBatchWindowTask<'a>>>,
        output: &mut [u8],
    ) -> Result<Vec<usize>, ClientError> {
        if task_windows.len() != plan.tasks.len() {
            return Err(ClientError::Protocol(
                "prepared range read window table has the wrong length".to_owned(),
            ));
        }
        let mut outputs = (0..plan.tasks.len()).map(|_| None).collect::<Vec<_>>();
        let mut active = Vec::new();
        let mut seen_paths = HashSet::with_capacity(plan.tasks.len());
        let mut has_duplicate_path = false;
        for (task, windows) in plan.tasks.iter().zip(task_windows) {
            if windows.is_empty() {
                outputs[task.index] = Some(task.output_len);
                continue;
            }
            if !seen_paths.insert(task.path.as_str()) {
                has_duplicate_path = true;
            }
            active.push(PreparedRangeBatchIntoTask {
                plan: task,
                windows,
            });
        }
        if active.len() == 1 || has_duplicate_path {
            for task in active {
                let start = task.plan.output_offset;
                let end = start.checked_add(task.plan.output_len).ok_or_else(|| {
                    ClientError::Protocol(
                        "packed range read into output end exceeds usize".to_owned(),
                    )
                })?;
                let (index, len) = self
                    .read_prepared_range_batch_into_request_task(task, &mut output[start..end])?;
                outputs[index] = Some(len);
            }
        } else {
            active.sort_by_key(|task| task.plan.output_offset);
            let mut active = active.into_iter();
            loop {
                let chunk = active
                    .by_ref()
                    .take(MAX_BATCH_READ_WORKERS)
                    .collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                let results = thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(chunk.len());
                    let mut remaining = &mut output[..];
                    let mut remaining_offset = 0_usize;
                    for task in chunk {
                        let gap = task
                            .plan
                            .output_offset
                            .checked_sub(remaining_offset)
                            .ok_or_else(|| {
                                ClientError::Protocol(
                                    "packed range read into output regions are out of order"
                                        .to_owned(),
                                )
                            })?;
                        let (_, after_gap) = remaining.split_at_mut(gap);
                        let (task_output, after_task) =
                            after_gap.split_at_mut(task.plan.output_len);
                        remaining = after_task;
                        remaining_offset = task
                            .plan
                            .output_offset
                            .checked_add(task.plan.output_len)
                            .ok_or_else(|| {
                                ClientError::Protocol(
                                    "packed range read into output end exceeds usize".to_owned(),
                                )
                            })?;
                        handles.push(scope.spawn(move || {
                            self.read_prepared_range_batch_into_request_task(task, task_output)
                        }));
                    }
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle.join().unwrap_or_else(|_| {
                                Err(ClientError::Protocol(
                                    "prepared packed into batch range read worker panicked"
                                        .to_owned(),
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, ClientError>>()
                })?;
                for (index, len) in results {
                    outputs[index] = Some(len);
                }
            }
        }
        outputs
            .into_iter()
            .map(|len| {
                len.ok_or_else(|| {
                    ClientError::Protocol(
                        "packed range read into did not produce every result".to_owned(),
                    )
                })
            })
            .collect()
    }

    fn read_range_batch_into_request_task(
        &self,
        task: RangeBatchIntoRequestTask,
        output: &mut [u8],
    ) -> Result<(usize, usize), ClientError> {
        if output.len() != task.output_len {
            return Err(ClientError::Protocol(
                "packed range read into output slice has the wrong length".to_owned(),
            ));
        }
        for window_index in 0..task.windows.len() {
            let window_task = &task.windows[window_index];
            self.read_range_batch_window_into(
                &task.path,
                &task.offsets,
                &window_task.window,
                &window_task.open,
                task.windows.get(window_index + 1).map(|next| &next.window),
                output,
            )?;
        }
        Ok((task.index, task.output_len))
    }

    fn read_prepared_range_batch_into_request_task(
        &self,
        task: PreparedRangeBatchIntoTask<'_>,
        output: &mut [u8],
    ) -> Result<(usize, usize), ClientError> {
        if output.len() != task.plan.output_len {
            return Err(ClientError::Protocol(
                "packed range read into output slice has the wrong length".to_owned(),
            ));
        }
        for window_index in 0..task.windows.len() {
            let window_task = &task.windows[window_index];
            self.read_range_batch_window_into(
                &task.plan.path,
                &task.plan.offsets,
                window_task.window,
                &window_task.open,
                task.windows.get(window_index + 1).map(|next| next.window),
                output,
            )?;
        }
        Ok((task.plan.index, task.plan.output_len))
    }

    fn read_range_batch_window_into(
        &self,
        path: &str,
        offsets: &[usize],
        window: &RangeReadWindow,
        open: &PathLayoutOpen,
        next_window: Option<&RangeReadWindow>,
        output: &mut [u8],
    ) -> Result<(), ClientError> {
        let generation = open.lease.generation;
        self.prefetch_range_read_window(open.metadata.attr.inode, generation, next_window);
        if let Some(scatter) = scatter_packed_range_plan(offsets, window, &open.plan)? {
            let output_end = scatter.output_end()?;
            if scatter.expands_physical_reads
                && self.try_fill_scatter_from_block_cache(
                    &scatter,
                    &mut output[scatter.output_start..output_end],
                )?
            {
                return Ok(());
            }
            if scatter.expands_physical_reads {
                let bytes = self.read_planned_object_blocks(
                    &read_pipeline_key(path, generation),
                    open.metadata.attr.inode,
                    generation,
                    open.metadata.attr.size,
                    window.offset,
                    &open.plan,
                )?;
                fill_packed_range_reads(output, offsets, window, &bytes)?;
                return Ok(());
            }
            let pipeline_key = read_pipeline_key(path, generation);
            self.read_planned_object_blocks_into(
                PlannedObjectReadIntoRequest {
                    pipeline_key: &pipeline_key,
                    inode: open.metadata.attr.inode,
                    generation,
                    file_size: open.metadata.attr.size,
                    offset: window.offset,
                    plan: &scatter.plan,
                },
                &mut output[scatter.output_start..output_end],
            )?;
            return Ok(());
        }
        let bytes = self.read_planned_object_blocks(
            &read_pipeline_key(path, generation),
            open.metadata.attr.inode,
            generation,
            open.metadata.attr.size,
            window.offset,
            &open.plan,
        )?;
        fill_packed_range_reads(output, offsets, window, &bytes)
    }

    fn prefetch_range_read_window(
        &self,
        inode: InodeId,
        generation: u64,
        window: Option<&RangeReadWindow>,
    ) {
        let Some(window) = window else {
            return;
        };
        let Ok(len) = range_read_window_len(window) else {
            return;
        };
        if !should_prefetch_range_read_window(len) {
            return;
        }
        self.prefetch_read_blocks(inode, generation, window.offset, len);
    }

    fn read_entry(
        &self,
        path: &str,
        entry: &DentryWithAttr,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        if entry.attr.file_type != FileType::File {
            return Err(ClientError::Metadata(nokv_meta::MetadError::NotFile));
        }
        if len == 0 || offset >= entry.attr.size {
            return Ok(Vec::new());
        }
        let body = entry.body.as_ref().ok_or_else(|| {
            ClientError::Protocol(format!("file {path} is missing body descriptor"))
        })?;
        let len = bounded_read_len(entry.attr.size - offset, len)?;
        let plan = self.cached_read_body_plan(entry.attr.inode, body.generation, offset, len)?;
        self.read_planned_object_blocks(
            &read_pipeline_key(path, body.generation),
            entry.attr.inode,
            body.generation,
            entry.attr.size,
            offset,
            &plan,
        )
    }

    fn read_planned_object_blocks(
        &self,
        pipeline_key: &str,
        inode: InodeId,
        generation: u64,
        file_size: u64,
        offset: u64,
        plan: &ObjectReadPlan,
    ) -> Result<Vec<u8>, ClientError> {
        if plan.output_len == 0 {
            return Ok(Vec::new());
        }
        let cache = if self.block_cache_enabled {
            Some(&self.block_cache)
        } else {
            None
        };
        let mut pipeline = {
            let mut pipelines = self.read_pipelines.lock().map_err(|err| {
                ClientError::Protocol(format!("read pipeline lock poisoned: {err}"))
            })?;
            pipelines.take(pipeline_key)
        };
        let executor = LayoutReadExecutor::new(self.objects.as_ref());
        let read_options = if self.block_cache_enabled {
            BlockReadOptions::default().with_read_coordinator(self.prefetcher.read_coordinator())
        } else {
            BlockReadOptions::default()
        };
        let outcome = executor
            .read_plan_with_options(&mut pipeline, cache, file_size, offset, plan, read_options)
            .map_err(ClientError::Object)?;
        {
            let mut pipelines = self.read_pipelines.lock().map_err(|err| {
                ClientError::Protocol(format!("read pipeline lock poisoned: {err}"))
            })?;
            pipelines.insert(pipeline_key.to_owned(), pipeline);
        }
        if let Some(hint) = outcome.readahead {
            self.prefetch_read_blocks(inode, generation, hint.offset, hint.len);
        }
        if self.block_cache_enabled {
            if let Some(request) = outcome.cache_warmup {
                let _ = self.prefetcher.submit(request);
            }
        }
        let stats = outcome.stats;
        self.record_object_read_stats(stats)?;
        Ok(outcome.bytes)
    }

    fn read_planned_object_blocks_into(
        &self,
        request: PlannedObjectReadIntoRequest<'_>,
        output: &mut [u8],
    ) -> Result<(), ClientError> {
        if request.plan.output_len == 0 {
            return Ok(());
        }
        let cache = if self.block_cache_enabled {
            Some(&self.block_cache)
        } else {
            None
        };
        let mut pipeline = {
            let mut pipelines = self.read_pipelines.lock().map_err(|err| {
                ClientError::Protocol(format!("read pipeline lock poisoned: {err}"))
            })?;
            pipelines.take(request.pipeline_key)
        };
        let executor = LayoutReadExecutor::new(self.objects.as_ref());
        let read_options = if self.block_cache_enabled {
            BlockReadOptions::default().with_read_coordinator(self.prefetcher.read_coordinator())
        } else {
            BlockReadOptions::default()
        };
        let outcome = executor
            .read_plan_into_with_options(
                &mut pipeline,
                cache,
                LayoutReadRequest {
                    file_size: request.file_size,
                    offset: request.offset,
                    plan: request.plan,
                },
                output,
                read_options,
            )
            .map_err(ClientError::Object)?;
        {
            let mut pipelines = self.read_pipelines.lock().map_err(|err| {
                ClientError::Protocol(format!("read pipeline lock poisoned: {err}"))
            })?;
            pipelines.insert(request.pipeline_key.to_owned(), pipeline);
        }
        if let Some(hint) = outcome.readahead {
            self.prefetch_read_blocks(request.inode, request.generation, hint.offset, hint.len);
        }
        if self.block_cache_enabled {
            if let Some(request) = outcome.cache_warmup {
                let _ = self.prefetcher.submit(request);
            }
        }
        self.record_object_read_stats(outcome.stats)?;
        Ok(())
    }

    fn try_fill_scatter_from_block_cache(
        &self,
        scatter: &ScatterPackedRangePlan,
        output: &mut [u8],
    ) -> Result<bool, ClientError> {
        if !self.block_cache_enabled || scatter.plan.blocks.is_empty() {
            return Ok(false);
        }
        // The scatter blocks cover only the byte ranges backed by data; gaps in
        // the window projection are sparse-file holes the plan never touches. Zero
        // the whole slice first (mirroring `read_object_blocks_into_with_cache_options`)
        // so a hole-spanning range reads as zeros, not stale bytes left over in a
        // reused staging buffer.
        output.fill(0);
        let mut cache_hits = 0_u64;
        let mut cache_hit_bytes = 0_u64;
        for block in &scatter.plan.blocks {
            let output_end = block.output_offset.checked_add(block.len).ok_or_else(|| {
                ClientError::Protocol("scatter cache read output end exceeds usize".to_owned())
            })?;
            if output_end > output.len() {
                return Err(ClientError::Protocol(
                    "scatter cache read output slice is out of bounds".to_owned(),
                ));
            }
            let Some(bytes) = self
                .block_cache
                .get_block_range(block.object_key.as_str(), block.object_offset, block.len)
                .map_err(ClientError::Object)?
            else {
                return Ok(false);
            };
            if bytes.len() != block.len {
                return Err(ClientError::Protocol(
                    "scatter cache read returned wrong length".to_owned(),
                ));
            }
            output[block.output_offset..output_end].copy_from_slice(&bytes);
            cache_hits = cache_hits.saturating_add(1);
            cache_hit_bytes = cache_hit_bytes.saturating_add(bytes.len() as u64);
        }
        self.record_object_read_stats(DataFabricReadStats {
            planned_blocks: scatter.plan.blocks.len() as u64,
            cache_hits,
            cache_hit_bytes,
            ..DataFabricReadStats::default()
        })?;
        Ok(true)
    }

    fn record_object_read_stats(&self, stats: DataFabricReadStats) -> Result<(), ClientError> {
        self.record_data_fabric_stats(stats)?;
        self.object_gets
            .fetch_add(stats.object_gets, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(stats.object_get_bytes, Ordering::Relaxed);
        self.coalesced_gets
            .fetch_add(stats.coalesced_ranges, Ordering::Relaxed);
        self.coalesced_get_bytes
            .fetch_add(stats.coalesced_range_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(stats.cache_hits, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(stats.cache_hit_bytes, Ordering::Relaxed);
        Ok(())
    }

    fn record_data_fabric_stats(&self, update: DataFabricReadStats) -> Result<(), ClientError> {
        let mut stats = self.data_fabric_stats.lock().map_err(|err| {
            ClientError::Protocol(format!("data fabric stats lock poisoned: {err}"))
        })?;
        stats.saturating_add_assign(update);
        Ok(())
    }

    fn prefetch_read_blocks(&self, inode: InodeId, generation: u64, offset: u64, len: usize) {
        if !self.block_cache_enabled || len == 0 {
            return;
        }
        let Ok(plan) = self.metadata.read_body_plan(inode, generation, offset, len) else {
            return;
        };
        let key = ObjectReadPlanKey::new(inode.get(), generation, offset, len);
        let _ = self.cache_read_body_plan(key, plan.clone());
        let _ = self
            .prefetcher
            .submit(ObjectPrefetchRequest::new(plan.output_len, plan.blocks));
    }

    fn cached_read_body_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<ObjectReadPlan, ClientError> {
        let key = ObjectReadPlanKey::new(inode.get(), generation, offset, len);
        if let Some(plan) = self.cached_read_body_plan_for_key(&key)? {
            self.read_plan_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(plan);
        }
        self.read_plan_cache_misses.fetch_add(1, Ordering::Relaxed);
        let plan = self
            .metadata
            .read_body_plan(inode, generation, offset, len)?;
        self.cache_read_body_plan(key, plan.clone())?;
        Ok(plan)
    }

    fn cached_read_body_plan_for_key(
        &self,
        key: &ObjectReadPlanKey,
    ) -> Result<Option<ObjectReadPlan>, ClientError> {
        self.read_plans
            .lock()
            .map_err(|err| ClientError::Protocol(format!("read plan cache lock poisoned: {err}")))
            .map(|mut plans| plans.get(key))
    }

    fn cache_read_body_plan(
        &self,
        key: ObjectReadPlanKey,
        plan: ObjectReadPlan,
    ) -> Result<(), ClientError> {
        self.read_plans
            .lock()
            .map_err(|err| ClientError::Protocol(format!("read plan cache lock poisoned: {err}")))?
            .insert(key, plan);
        Ok(())
    }

    pub fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, false)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_body(&prepared, &bytes, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result.entry),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_from_reader<R: Read>(
        &self,
        path: &str,
        reader: R,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, false)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_reader(&prepared, reader, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result.entry),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_replace(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, true)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_body(&prepared, &bytes, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_replace_from_reader<R: Read>(
        &self,
        path: &str,
        reader: R,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, true)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_reader(&prepared, reader, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    fn stage_artifact_body(
        &self,
        prepared: &ClientPreparedArtifact,
        bytes: &[u8],
        metadata: ArtifactMetadata,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        let written = match self.objects.write_bytes(
            bytes,
            ChunkWriteOptions {
                manifest_id: metadata.manifest_id.clone(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        ) {
            Ok(written) => written,
            Err(err) => {
                cleanup_staged_write_error(&self.objects, &err)?;
                return Err(ClientError::Object(err));
            }
        };
        self.finish_staged_artifact(prepared, metadata, written)
    }

    fn stage_artifact_reader<R: Read>(
        &self,
        prepared: &ClientPreparedArtifact,
        reader: R,
        metadata: ArtifactMetadata,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        let written = match self.objects.write_reader(
            reader,
            ChunkWriteOptions {
                manifest_id: metadata.manifest_id.clone(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        ) {
            Ok(written) => written,
            Err(err) => {
                cleanup_staged_write_error(&self.objects, &err)?;
                return Err(ClientError::Object(err));
            }
        };
        self.finish_staged_artifact(prepared, metadata, written)
    }

    fn finish_staged_artifact(
        &self,
        prepared: &ClientPreparedArtifact,
        metadata: ArtifactMetadata,
        written: ChunkedWrite,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.object_put_bytes
            .fetch_add(written.object_put_bytes, Ordering::Relaxed);
        self.manifest_chunks
            .fetch_add(written.chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            written
                .chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        let staged = written.staged_objects()?;
        let chunks = written.chunk_manifests();
        Ok((
            BodyDescriptor {
                producer: metadata.producer,
                digest_uri: metadata.digest_uri,
                size: written.size,
                content_type: metadata.content_type,
                manifest_id: written.manifest_id,
                generation: prepared.generation,
                // Fresh client-side write: self-contained, no fallthrough base.
                base_generation: 0,
                chunk_size: written.chunk_size,
                block_size: written.block_size,
            },
            chunks,
            staged,
        ))
    }
}

impl PathReadRange {
    pub fn new(offset: u64, len: usize) -> Self {
        Self { offset, len }
    }
}

impl PathRangeReadRequest {
    pub fn new(path: impl Into<String>, ranges: Vec<PathReadRange>) -> Self {
        Self {
            path: path.into(),
            ranges,
            expected_generation: None,
            max_gap_bytes: 0,
        }
    }

    pub fn with_expected_generation(mut self, generation: u64) -> Self {
        self.expected_generation = Some(generation);
        self
    }

    pub fn with_max_gap_bytes(mut self, max_gap_bytes: u64) -> Self {
        self.max_gap_bytes = max_gap_bytes;
        self
    }
}

impl PreparedPathRangeBatch {
    pub fn new(requests: &[PathRangeReadRequest]) -> Result<Self, ClientError> {
        let mut request_offsets = Vec::with_capacity(requests.len());
        let mut cursor = 0_usize;
        for request in requests {
            request_offsets.push(cursor);
            cursor = cursor
                .checked_add(path_range_request_output_len(request)?)
                .ok_or_else(|| {
                    ClientError::Protocol(
                        "prepared range batch output length exceeds usize".to_owned(),
                    )
                })?;
        }
        Self::with_request_offsets(requests, &request_offsets)
    }

    pub fn request_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn range_count(&self) -> usize {
        self.range_count
    }

    pub fn output_len(&self) -> usize {
        self.output_len
    }

    pub fn request_layout(&self) -> Vec<(usize, usize)> {
        self.request_offsets
            .iter()
            .copied()
            .zip(self.request_lengths.iter().copied())
            .collect()
    }

    fn with_request_offsets(
        requests: &[PathRangeReadRequest],
        request_offsets: &[usize],
    ) -> Result<Self, ClientError> {
        if requests.len() != request_offsets.len() {
            return Err(ClientError::Protocol(format!(
                "packed range read into got {} output offsets for {} requests",
                request_offsets.len(),
                requests.len()
            )));
        }
        let mut tasks = Vec::with_capacity(requests.len());
        let mut open_requests = Vec::new();
        let mut open_targets = Vec::new();
        let mut request_lengths = Vec::with_capacity(requests.len());
        let mut range_count = 0_usize;
        let mut output_len = 0_usize;
        for (index, (request, output_offset)) in requests.iter().zip(request_offsets).enumerate() {
            let mut offsets = Vec::with_capacity(request.ranges.len());
            let mut total_len = 0_usize;
            let mut range_tasks = Vec::with_capacity(request.ranges.len());
            for (range_index, range) in request.ranges.iter().copied().enumerate() {
                offsets.push(total_len);
                total_len = total_len.checked_add(range.len).ok_or_else(|| {
                    ClientError::Protocol("packed range read into length exceeds usize".to_owned())
                })?;
                range_count = range_count.checked_add(1).ok_or_else(|| {
                    ClientError::Protocol("prepared range count exceeds usize".to_owned())
                })?;
                if range.len == 0 {
                    continue;
                }
                let end = checked_read_range_end(range.offset, range.len)?;
                range_tasks.push(RangeReadTask {
                    index: range_index,
                    offset: range.offset,
                    len: range.len,
                    end,
                });
            }
            let output_end = output_offset.checked_add(total_len).ok_or_else(|| {
                ClientError::Protocol("packed range read into output end exceeds usize".to_owned())
            })?;
            output_len = output_len.max(output_end);
            let windows = coalesce_range_read_tasks(range_tasks, request.max_gap_bytes);
            for (window_index, window) in windows.iter().enumerate() {
                let window_len = range_read_window_len(window)?;
                let mut open =
                    PathLayoutOpenRequest::new(request.path.as_str(), window.offset, window_len);
                open.expected_generation = request.expected_generation;
                open_requests.push(open);
                open_targets.push(PreparedRangeBatchOpenTarget {
                    request_index: index,
                    window_index,
                });
            }
            request_lengths.push(total_len);
            tasks.push(PreparedRangeBatchIntoRequestTask {
                index,
                path: request.path.clone(),
                output_offset: *output_offset,
                output_len: total_len,
                offsets,
                windows,
            });
        }
        validate_range_batch_output_regions(
            tasks.iter().map(|task| RangeBatchOutputRegion {
                offset: task.output_offset,
                len: task.output_len,
                index: task.index,
            }),
            output_len,
        )?;
        Ok(Self {
            tasks,
            open_requests,
            open_targets,
            request_offsets: request_offsets.to_vec(),
            request_lengths,
            range_count,
            output_len,
        })
    }
}

impl ScatterPackedRangePlan {
    fn output_end(&self) -> Result<usize, ClientError> {
        self.output_start
            .checked_add(self.plan.output_len)
            .ok_or_else(|| {
                ClientError::Protocol("scatter packed read output end exceeds usize".to_owned())
            })
    }
}

fn cleanup_staged_write_error<O: ObjectStore>(
    objects: &O,
    err: &ObjectError,
) -> Result<(), ClientError> {
    if let ObjectError::StagedWriteFailed { staged, .. } = err {
        objects.delete_staged(staged).map_err(ClientError::Object)?;
    }
    Ok(())
}

fn coalesce_range_read_tasks(
    mut tasks: Vec<RangeReadTask>,
    max_gap_bytes: u64,
) -> Vec<RangeReadWindow> {
    tasks.sort_by(|left, right| {
        left.offset
            .cmp(&right.offset)
            .then_with(|| left.end.cmp(&right.end))
            .then_with(|| left.index.cmp(&right.index))
    });
    let mut windows = Vec::new();
    for task in tasks {
        let Some(window) = windows.last_mut() else {
            windows.push(RangeReadWindow {
                offset: task.offset,
                end: task.end,
                ranges: vec![task],
            });
            continue;
        };
        if task.offset <= window.end.saturating_add(max_gap_bytes) {
            window.end = window.end.max(task.end);
            window.ranges.push(task);
        } else {
            windows.push(RangeReadWindow {
                offset: task.offset,
                end: task.end,
                ranges: vec![task],
            });
        }
    }
    windows
}

fn range_read_window_len(window: &RangeReadWindow) -> Result<usize, ClientError> {
    usize::try_from(window.end - window.offset)
        .map_err(|_| ClientError::Protocol("coalesced range read length exceeds usize".to_owned()))
}

fn should_prefetch_range_read_window(len: usize) -> bool {
    len >= MIN_RANGE_READAHEAD_BYTES
}

fn validate_range_batch_output_regions(
    regions: impl IntoIterator<Item = RangeBatchOutputRegion>,
    output_len: usize,
) -> Result<(), ClientError> {
    let mut ordered = Vec::new();
    for region in regions {
        let end = region.offset.checked_add(region.len).ok_or_else(|| {
            ClientError::Protocol("packed range read into output end exceeds usize".to_owned())
        })?;
        if end > output_len {
            return Err(ClientError::Protocol(
                "packed range read into output buffer is too small".to_owned(),
            ));
        }
        if region.len > 0 {
            ordered.push((region.offset, end, region.index));
        }
    }
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut previous_end = 0_usize;
    for (start, end, _) in ordered {
        if start < previous_end {
            return Err(ClientError::Protocol(
                "packed range read into output regions must not overlap".to_owned(),
            ));
        }
        previous_end = end;
    }
    Ok(())
}

fn path_range_request_output_len(request: &PathRangeReadRequest) -> Result<usize, ClientError> {
    request.ranges.iter().try_fold(0_usize, |total, range| {
        total.checked_add(range.len).ok_or_else(|| {
            ClientError::Protocol("prepared range batch output length exceeds usize".to_owned())
        })
    })
}

fn scatter_packed_range_plan(
    offsets: &[usize],
    window: &RangeReadWindow,
    plan: &ObjectReadPlan,
) -> Result<Option<ScatterPackedRangePlan>, ClientError> {
    let Some((output_start, output_len)) = dense_packed_window_output(offsets, window)? else {
        return Ok(None);
    };
    let mut blocks = Vec::new();
    for block in &plan.blocks {
        let block_start = block.output_offset;
        let block_end = block_start.checked_add(block.len).ok_or_else(|| {
            ClientError::Protocol("scatter packed read block end exceeds usize".to_owned())
        })?;
        for range in &window.ranges {
            let range_window_start =
                usize::try_from(range.offset - window.offset).map_err(|_| {
                    ClientError::Protocol(
                        "scatter packed read range offset exceeds usize".to_owned(),
                    )
                })?;
            let range_window_end = range_window_start.checked_add(range.len).ok_or_else(|| {
                ClientError::Protocol("scatter packed read range end exceeds usize".to_owned())
            })?;
            let overlap_start = block_start.max(range_window_start);
            let overlap_end = block_end.min(range_window_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let overlap_len = overlap_end - overlap_start;
            let source_delta = overlap_start - block_start;
            let range_delta = overlap_start - range_window_start;
            let target_start = offsets
                .get(range.index)
                .copied()
                .ok_or_else(|| {
                    ClientError::Protocol("packed range read offset is missing".to_owned())
                })?
                .checked_sub(output_start)
                .ok_or_else(|| {
                    ClientError::Protocol(
                        "scatter packed read output offset is before window output".to_owned(),
                    )
                })?
                .checked_add(range_delta)
                .ok_or_else(|| {
                    ClientError::Protocol(
                        "scatter packed read output offset exceeds usize".to_owned(),
                    )
                })?;
            let target_end = target_start.checked_add(overlap_len).ok_or_else(|| {
                ClientError::Protocol("scatter packed read output end exceeds usize".to_owned())
            })?;
            if target_end > output_len {
                return Err(ClientError::Protocol(
                    "scatter packed read output slice is out of bounds".to_owned(),
                ));
            }
            append_scatter_read_block(
                &mut blocks,
                ObjectReadBlock {
                    object_key: block.object_key.clone(),
                    digest_uri: block.digest_uri.clone(),
                    object_offset: block
                        .object_offset
                        .checked_add(u64::try_from(source_delta).map_err(|_| {
                            ClientError::Protocol(
                                "scatter packed read source delta exceeds u64".to_owned(),
                            )
                        })?)
                        .ok_or_else(|| {
                            ClientError::Protocol(
                                "scatter packed read object offset exceeds u64".to_owned(),
                            )
                        })?,
                    object_len: block.object_len,
                    len: overlap_len,
                    output_offset: target_start,
                },
            )?;
        }
    }
    Ok(Some(ScatterPackedRangePlan {
        output_start,
        expands_physical_reads: blocks.len() > plan.blocks.len(),
        plan: ObjectReadPlan::new(output_len, blocks),
    }))
}

fn append_scatter_read_block(
    blocks: &mut Vec<ObjectReadBlock>,
    block: ObjectReadBlock,
) -> Result<(), ClientError> {
    if let Some(previous) = blocks.last_mut() {
        let previous_object_end = previous
            .object_offset
            .checked_add(previous.len as u64)
            .ok_or_else(|| {
                ClientError::Protocol(
                    "scatter packed read previous object end exceeds u64".to_owned(),
                )
            })?;
        let previous_output_end = previous
            .output_offset
            .checked_add(previous.len)
            .ok_or_else(|| {
                ClientError::Protocol(
                    "scatter packed read previous output end exceeds usize".to_owned(),
                )
            })?;
        if previous.object_key == block.object_key
            && previous.digest_uri == block.digest_uri
            && previous.object_len == block.object_len
            && previous_object_end == block.object_offset
            && previous_output_end == block.output_offset
        {
            previous.len = previous.len.checked_add(block.len).ok_or_else(|| {
                ClientError::Protocol("scatter packed read merged len exceeds usize".to_owned())
            })?;
            return Ok(());
        }
    }
    blocks.push(block);
    Ok(())
}

fn dense_packed_window_output(
    offsets: &[usize],
    window: &RangeReadWindow,
) -> Result<Option<(usize, usize)>, ClientError> {
    if window.ranges.is_empty() {
        return Ok(None);
    }
    let mut regions = Vec::with_capacity(window.ranges.len());
    for range in &window.ranges {
        let start = offsets.get(range.index).copied().ok_or_else(|| {
            ClientError::Protocol("packed range read offset is missing".to_owned())
        })?;
        let end = start.checked_add(range.len).ok_or_else(|| {
            ClientError::Protocol("packed range read output end exceeds usize".to_owned())
        })?;
        regions.push((start, end));
    }
    regions.sort_by_key(|region| region.0);
    let output_start = regions[0].0;
    let mut previous_end = output_start;
    for (start, end) in regions {
        if start != previous_end {
            return Ok(None);
        }
        previous_end = end;
    }
    let output_len = previous_end.checked_sub(output_start).ok_or_else(|| {
        ClientError::Protocol("packed range read output length underflow".to_owned())
    })?;
    Ok(Some((output_start, output_len)))
}

fn fill_range_reads(
    reads: &mut [Option<Vec<u8>>],
    window: &RangeReadWindow,
    bytes: &[u8],
) -> Result<(), ClientError> {
    for range in &window.ranges {
        let start = usize::try_from(range.offset - window.offset)
            .map_err(|_| ClientError::Protocol("range read offset exceeds usize".to_owned()))?;
        if start >= bytes.len() {
            reads[range.index] = Some(Vec::new());
            continue;
        }
        let end = start
            .checked_add(range.len)
            .ok_or_else(|| ClientError::Protocol("range read end exceeds usize".to_owned()))?
            .min(bytes.len());
        reads[range.index] = Some(bytes[start..end].to_vec());
    }
    Ok(())
}

fn fill_packed_range_reads(
    packed: &mut [u8],
    offsets: &[usize],
    window: &RangeReadWindow,
    bytes: &[u8],
) -> Result<(), ClientError> {
    for range in &window.ranges {
        let input_start = usize::try_from(range.offset - window.offset)
            .map_err(|_| ClientError::Protocol("range read offset exceeds usize".to_owned()))?;
        let input_end = input_start.checked_add(range.len).ok_or_else(|| {
            ClientError::Protocol("packed range read input end exceeds usize".to_owned())
        })?;
        if input_end > bytes.len() {
            return Err(ClientError::Protocol(
                "packed range read requires full range coverage".to_owned(),
            ));
        }
        let output_start = *offsets.get(range.index).ok_or_else(|| {
            ClientError::Protocol("packed range read offset is missing".to_owned())
        })?;
        let output_end = output_start.checked_add(range.len).ok_or_else(|| {
            ClientError::Protocol("packed range read output end exceeds usize".to_owned())
        })?;
        let output = packed.get_mut(output_start..output_end).ok_or_else(|| {
            ClientError::Protocol("packed range read output slice is out of bounds".to_owned())
        })?;
        output.copy_from_slice(&bytes[input_start..input_end]);
    }
    Ok(())
}

fn collect_range_reads(reads: Vec<Option<Vec<u8>>>) -> Result<Vec<Vec<u8>>, ClientError> {
    reads
        .into_iter()
        .map(|read| {
            read.ok_or_else(|| {
                ClientError::Protocol("range read did not produce every result".to_owned())
            })
        })
        .collect()
}

fn checked_read_range_end(offset: u64, len: usize) -> Result<u64, ClientError> {
    let len = u64::try_from(len)
        .map_err(|_| ClientError::Protocol("range read length exceeds u64".to_owned()))?;
    offset
        .checked_add(len)
        .ok_or_else(|| ClientError::Protocol("range read end overflows u64".to_owned()))
}

fn file_len(size: u64) -> Result<usize, ClientError> {
    usize::try_from(size)
        .map_err(|_| ClientError::Protocol("file is too large for this client".to_owned()))
}

fn read_pipeline_key(path: &str, generation: u64) -> String {
    format!("{path}#{generation}")
}

fn bounded_read_len(available: u64, requested: usize) -> Result<usize, ClientError> {
    let requested = u64::try_from(requested)
        .map_err(|_| ClientError::Protocol("read length exceeds u64".to_owned()))?;
    let len = available.min(requested);
    usize::try_from(len).map_err(|_| ClientError::Protocol("read length exceeds usize".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::{SocketAddr, TcpListener};
    use std::thread;

    use nokv_object::MemoryObjectStore;
    use nokv_protocol::{decode_request, encode_envelope, MetadataRpcEnvelope, MetadataRpcRequest};

    use crate::framed::{read_frame, write_frame, FRAMED_RPC_MAGIC};

    use super::*;

    fn serve_body_read_plans(bodies: Vec<Vec<u8>>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            for body in bodies {
                let (request_id, flags, request) = read_frame(&mut stream).unwrap();
                let request = decode_request(&request).expect("framed request is metadata rpc");
                assert!(matches!(request, MetadataRpcRequest::ReadBodyPlan { .. }));
                write_frame(&mut stream, request_id, flags, &body).unwrap();
            }
        });
        addr
    }

    fn response_body(json: &str) -> Vec<u8> {
        let envelope: MetadataRpcEnvelope = serde_json::from_str(json).unwrap();
        encode_envelope(&envelope).unwrap()
    }

    #[test]
    fn file_client_caches_body_read_plan_after_miss() {
        let read_plan = response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":0,"object_len":6,"len":6,"output_offset":0}]}}}"#,
        );
        let addr = serve_body_read_plans(vec![read_plan.clone(), read_plan]);
        let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
        let inode = InodeId::new(42).unwrap();

        let first = client.cached_read_body_plan(inode, 7, 0, 6).unwrap();
        let second = client.cached_read_body_plan(inode, 7, 0, 6).unwrap();

        assert_eq!(first.output_len, 6);
        assert_eq!(first, second);
        let stats = client.object_stats();
        assert_eq!(stats.read_plan_cache_misses, 1);
        assert_eq!(stats.read_plan_cache_hits, 1);
    }
}
