//! NoKV workload benchmark harness.
//!
//! This binary intentionally reports workload shape and durability caveats with
//! every result. It runs a real `metad` process boundary with the service client.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nokv_client::{
    ArtifactMetadata, DataFabricReadStats, MetadataClient, NoKvFsClient, PathLayoutOpenRequest,
    PathRangeReadRequest, PathReadRange,
};
use nokv_control::{ControlStore, InMemoryControlStore, ShardId};
use nokv_meta::{
    DentryWithAttr, HistoryGcOptions, MetadataServiceStats, MetadataStoreStats, ObjectGcOptions,
    ObjectTransferStats, RenameReplaceResult,
};
use nokv_object::{
    ConfiguredObjectStore, HotFillMode, LocalObjectStoreOptions, LocalObjectStoreStats,
    ObjectStoreConfig, S3ObjectStoreOptions, TieredObjectStoreOptions, TieredObjectStoreStats,
    TieredPutPolicy,
};
use nokv_server::{Server, ServerOptions, ServerShardOwnerOptions, ServerSharedLogOptions};
use nokv_types::{MountId, PathMetadata, DEFAULT_SHARD_INDEX};

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_MODE_FILE: u32 = 0o644;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Profile {
    Smoke,
    Standard,
    Long,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    All,
    MetadataSmoke,
    MdtestEasy,
    MdtestHard,
    MetadataNegativeLookup,
    ArtifactIndexLookup,
    MetadataConcurrentRead,
    MetadataDurabilityBatch,
    MetadataShardRouting,
    CheckpointPublish,
    TrainingRead,
    NativeLayoutRead,
    AiDatasetBatchRead,
    AiShardRangeRead,
    MlperfDlio,
    DemoDataset,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    profile: Profile,
    workload: Workload,
    root: PathBuf,
    object_backend: ObjectBackendKind,
    s3: S3ObjectStoreOptions,
    object_concurrency: usize,
    hot_object_root: Option<PathBuf>,
    hot_object_max_bytes: Option<u64>,
    hot_fill_mode: HotFillMode,
    read_repeats: usize,
    range_stride: usize,
    range_coalesce_gap_bytes: u64,
    block_cache: bool,
    checkpoint_bytes: Option<usize>,
    sample_bytes: Option<usize>,
    keep: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectBackendKind {
    S3,
    RustFs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkloadShape {
    dirs: usize,
    files_per_dir: usize,
    shared_files: usize,
    checkpoints: usize,
    checkpoint_bytes: usize,
    dataset_dirs: usize,
    dataset_files_per_dir: usize,
    dataset_file_bytes: usize,
}

#[derive(Clone, Debug)]
struct ResultRow {
    workload: &'static str,
    profile: Profile,
    operations: usize,
    seconds: f64,
    ops_per_second: f64,
    mb_per_second: f64,
    samples_per_second: f64,
    object_puts: u64,
    object_put_bytes: u64,
    object_gets: u64,
    object_get_bytes: u64,
    coalesced_gets: u64,
    coalesced_get_bytes: u64,
    cache_hits: u64,
    cache_hit_bytes: u64,
    cache_hit_rate: f64,
    prefetch_enqueued: u64,
    prefetch_dropped: u64,
    prefetch_completed: u64,
    prefetch_failed: u64,
    prefetch_object_gets: u64,
    prefetch_object_get_bytes: u64,
    prefetch_cache_hits: u64,
    prefetch_cache_hit_bytes: u64,
    read_plan_cache_hits: u64,
    read_plan_cache_misses: u64,
    object_writeback_enqueued: u64,
    object_writeback_inline: u64,
    object_writeback_completed: u64,
    object_writeback_failed: u64,
    object_writeback_staged_bytes: u64,
    object_writeback_uploaded_bytes: u64,
    object_writeback_queue_wait_ns: u64,
    object_writeback_queue_max_wait_ns: u64,
    object_writeback_upload_ns: u64,
    object_writeback_upload_max_ns: u64,
    object_writeback_collect_ns: u64,
    object_writeback_digest_ns: u64,
    object_writeback_store_put_ns: u64,
    object_writeback_cache_put_ns: u64,
    manifest_chunks: u64,
    manifest_blocks: u64,
    data_fabric_planned_blocks: u64,
    data_fabric_local_nvme_hits: u64,
    data_fabric_object_fallbacks: u64,
    data_fabric_object_gets: u64,
    data_fabric_object_get_bytes: u64,
    data_fabric_coalesced_ranges: u64,
    data_fabric_coalesced_range_bytes: u64,
    data_fabric_cache_hits: u64,
    data_fabric_cache_hit_bytes: u64,
    tiered_hot_gets: u64,
    tiered_hot_hits: u64,
    tiered_hot_misses: u64,
    tiered_hot_errors: u64,
    tiered_cold_gets: u64,
    tiered_cold_get_bytes: u64,
    tiered_cold_puts: u64,
    tiered_cold_put_errors: u64,
    tiered_hot_puts: u64,
    tiered_hot_put_errors: u64,
    tiered_hot_fills: u64,
    tiered_hot_fill_enqueued: u64,
    tiered_hot_fill_coalesced: u64,
    tiered_hot_fill_errors: u64,
    tiered_cold_deletes: u64,
    tiered_hot_deletes: u64,
    tiered_hot_delete_errors: u64,
    tiered_hot_put_ns: u64,
    tiered_pending_cold_put_ns: u64,
    tiered_cold_put_enqueue_ns: u64,
    local_hot_resident_objects: u64,
    local_hot_resident_bytes: u64,
    local_hot_max_bytes: u64,
    local_hot_evictions: u64,
    local_hot_eviction_bytes: u64,
    local_hot_admission_rejections: u64,
    local_hot_puts: u64,
    local_hot_put_bytes: u64,
    local_hot_put_total_ns: u64,
    local_hot_put_prepare_ns: u64,
    local_hot_put_write_ns: u64,
    local_hot_put_sync_ns: u64,
    local_hot_put_rename_ns: u64,
    local_hot_put_record_ns: u64,
    metadata_commits: u64,
    metadata_dedupe_hits: u64,
    metadata_predicates: u64,
    metadata_prefix_empty_predicates: u64,
    metadata_gets: u64,
    metadata_get_user_strong: u64,
    metadata_get_write_plan_local: u64,
    metadata_get_snapshot: u64,
    metadata_scans: u64,
    metadata_scan_user_strong: u64,
    metadata_scan_write_plan_local: u64,
    metadata_scan_snapshot: u64,
    metadata_scan_visited: u64,
    metadata_scan_returned: u64,
    metadata_history_lookups: u64,
    metadata_current_puts: u64,
    metadata_current_deletes: u64,
    metadata_history_writes: u64,
    metadata_watch_writes: u64,
    metadata_dedupe_writes: u64,
    metadata_commit_prepare_ns: u64,
    metadata_atomic_applies: u64,
    metadata_atomic_apply_commands: u64,
    metadata_atomic_apply_max_batch: u64,
    metadata_atomic_apply_ns: u64,
    metadata_log_segments_archived: u64,
    metadata_log_entries_archived: u64,
    metadata_log_archive_bytes: u64,
    path_index_lookups: u64,
    path_index_hits: u64,
    path_index_misses: u64,
    path_index_stale: u64,
    path_index_scan_stale: u64,
    path_index_fallback: u64,
    path_index_hit_rate: f64,
    create_files_batches: u64,
    create_files_entries: u64,
    create_dirs_batches: u64,
    create_dirs_entries: u64,
    read_dir_plus_calls: u64,
    read_dir_plus_entries: u64,
    read_dir_plus_projection_hits: u64,
    read_dir_plus_projection_hit_rate: f64,
    object_concurrency: usize,
    read_repeats: usize,
    block_cache: bool,
    phase: &'static str,
    checksum: u64,
    shape: String,
    caveat: String,
}

#[derive(Clone, Debug)]
struct RowInput {
    workload: &'static str,
    profile: Profile,
    operations: usize,
    seconds: f64,
    bytes: u64,
    samples: usize,
    stats: BenchStats,
    object_concurrency: usize,
    read_repeats: usize,
    block_cache: bool,
    checksum: u64,
    shape: String,
    caveat: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BenchStats {
    object: ObjectTransferStats,
    data_fabric: DataFabricReadStats,
    tiered_object: TieredObjectStoreStats,
    local_hot: LocalObjectStoreStats,
    metadata_store: MetadataStoreStats,
    metadata_service: MetadataServiceStats,
}

#[derive(Clone, Debug)]
struct NativeReadRequest {
    path: String,
    offset: u64,
    len: usize,
    generation: u64,
}

#[derive(Clone, Copy, Debug)]
struct NativeReadRange {
    offset: u64,
    len: usize,
}

#[derive(Clone, Debug)]
struct ShardRangeReadRequest {
    path: String,
    generation: u64,
    ranges: Vec<NativeReadRange>,
}

#[derive(Debug)]
enum BenchError {
    MissingValue(&'static str),
    UnknownOption(String),
    InvalidProfile(String),
    InvalidWorkload(String),
    Io(String),
    Client(String),
}

trait BenchClient: Sync {
    fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), BenchError>;
    fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError>;
    fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        paths
            .iter()
            .map(|path| self.mkdir(path, mode, uid, gid))
            .collect()
    }
    fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError>;
    fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        paths
            .iter()
            .map(|path| self.create_file(path, mode, uid, gid))
            .collect()
    }
    fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, BenchError>;
    fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, BenchError>;
    fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, BenchError>;
    fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, BenchError>;
    fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError>;
    fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError>;
    fn cat(&self, path: &str) -> Result<Vec<u8>, BenchError>;
    fn read_path(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<Vec<u8>, BenchError>;
    fn read_paths(&self, requests: &[NativeReadRequest]) -> Result<Vec<Vec<u8>>, BenchError> {
        requests
            .iter()
            .map(|request| {
                self.read_path(
                    &request.path,
                    request.offset,
                    request.len,
                    Some(request.generation),
                )
            })
            .collect()
    }
    fn read_ranges(
        &self,
        path: &str,
        ranges: &[NativeReadRange],
        expected_generation: Option<u64>,
        max_gap_bytes: u64,
    ) -> Result<Vec<Vec<u8>>, BenchError> {
        let _ = max_gap_bytes;
        ranges
            .iter()
            .map(|range| self.read_path(path, range.offset, range.len, expected_generation))
            .collect()
    }
    fn read_range_batches(
        &self,
        requests: &[ShardRangeReadRequest],
        max_gap_bytes: u64,
    ) -> Result<Vec<Vec<Vec<u8>>>, BenchError> {
        requests
            .iter()
            .map(|request| {
                self.read_ranges(
                    &request.path,
                    &request.ranges,
                    Some(request.generation),
                    max_gap_bytes,
                )
            })
            .collect()
    }
    fn stats(&self) -> Result<BenchStats, BenchError>;
}

struct ServiceBenchClient {
    client: NoKvFsClient<ConfiguredObjectStore>,
    objects: ConfiguredObjectStore,
    stats_addr: SocketAddr,
    include_server_object_stats: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataDurabilityMode {
    LocalOnly,
    SyncSharedLog,
}

impl BenchClient for ServiceBenchClient {
    fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), BenchError> {
        self.client
            .metadata()
            .bootstrap_root(mode, uid, gid)
            .map_err(from_client)
    }

    fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError> {
        self.client
            .metadata()
            .mkdir(path, mode, uid, gid)
            .map_err(from_client)
    }

    fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client
            .metadata()
            .mkdirs(paths, mode, uid, gid)
            .map_err(from_client)?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(from_client)
    }

    fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError> {
        self.client
            .metadata()
            .create_file(path, mode, uid, gid)
            .map_err(from_client)
    }

    fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client
            .metadata()
            .create_files(paths, mode, uid, gid)
            .map_err(from_client)?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(from_client)
    }

    fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, BenchError> {
        NoKvFsClient::put_artifact(&self.client, path, bytes, metadata).map_err(from_client)
    }

    fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, BenchError> {
        self.client
            .metadata()
            .rename_replace(source, destination)
            .map_err(from_client)
    }

    fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, BenchError> {
        self.client.metadata().lookup(path).map_err(from_client)
    }

    fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, BenchError> {
        self.client.metadata().stat_path(path).map_err(from_client)
    }

    fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client.metadata().list(path).map_err(from_client)
    }

    fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client
            .metadata()
            .list_indexed(path)
            .map_err(from_client)
    }

    fn cat(&self, path: &str) -> Result<Vec<u8>, BenchError> {
        NoKvFsClient::cat(&self.client, path).map_err(from_client)
    }

    fn read_path(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<Vec<u8>, BenchError> {
        NoKvFsClient::read_path(&self.client, path, offset, len, expected_generation)
            .map(|read| read.bytes)
            .map_err(from_client)
    }

    fn read_paths(&self, requests: &[NativeReadRequest]) -> Result<Vec<Vec<u8>>, BenchError> {
        let requests = requests
            .iter()
            .map(|request| {
                PathLayoutOpenRequest::new(&request.path, request.offset, request.len)
                    .with_expected_generation(request.generation)
            })
            .collect::<Vec<_>>();
        NoKvFsClient::read_paths(&self.client, &requests)
            .map(|reads| reads.into_iter().map(|read| read.bytes).collect())
            .map_err(from_client)
    }

    fn read_ranges(
        &self,
        path: &str,
        ranges: &[NativeReadRange],
        expected_generation: Option<u64>,
        max_gap_bytes: u64,
    ) -> Result<Vec<Vec<u8>>, BenchError> {
        let ranges = ranges
            .iter()
            .map(|range| (range.offset, range.len))
            .collect::<Vec<_>>();
        NoKvFsClient::read_path_ranges(
            &self.client,
            path,
            &ranges,
            expected_generation,
            max_gap_bytes,
        )
        .map_err(from_client)
    }

    fn read_range_batches(
        &self,
        requests: &[ShardRangeReadRequest],
        max_gap_bytes: u64,
    ) -> Result<Vec<Vec<Vec<u8>>>, BenchError> {
        let requests = requests
            .iter()
            .map(|request| {
                PathRangeReadRequest::new(
                    &request.path,
                    request
                        .ranges
                        .iter()
                        .map(|range| PathReadRange::new(range.offset, range.len))
                        .collect(),
                )
                .with_expected_generation(request.generation)
                .with_max_gap_bytes(max_gap_bytes)
            })
            .collect::<Vec<_>>();
        NoKvFsClient::read_path_ranges_batch(&self.client, &requests).map_err(from_client)
    }

    fn stats(&self) -> Result<BenchStats, BenchError> {
        let mut stats = fetch_server_stats(self.stats_addr)?;
        let client_object_stats = NoKvFsClient::object_stats(&self.client);
        stats.object = if self.include_server_object_stats {
            add_object_transfer_stats(stats.object, client_object_stats)
        } else {
            client_object_stats
        };
        stats.data_fabric = NoKvFsClient::data_fabric_stats(&self.client).map_err(from_client)?;
        stats.tiered_object = self
            .objects
            .tiered_stats()
            .map_err(from_client)?
            .unwrap_or_default();
        stats.local_hot = self
            .objects
            .local_hot_stats()
            .map_err(from_client)?
            .unwrap_or_default();
        Ok(stats)
    }
}

fn add_object_transfer_stats(
    left: ObjectTransferStats,
    right: ObjectTransferStats,
) -> ObjectTransferStats {
    ObjectTransferStats {
        object_puts: left.object_puts.saturating_add(right.object_puts),
        object_put_bytes: left.object_put_bytes.saturating_add(right.object_put_bytes),
        object_gets: left.object_gets.saturating_add(right.object_gets),
        object_get_bytes: left.object_get_bytes.saturating_add(right.object_get_bytes),
        coalesced_gets: left.coalesced_gets.saturating_add(right.coalesced_gets),
        coalesced_get_bytes: left
            .coalesced_get_bytes
            .saturating_add(right.coalesced_get_bytes),
        cache_hits: left.cache_hits.saturating_add(right.cache_hits),
        cache_hit_bytes: left.cache_hit_bytes.saturating_add(right.cache_hit_bytes),
        prefetch_enqueued: left
            .prefetch_enqueued
            .saturating_add(right.prefetch_enqueued),
        prefetch_dropped: left.prefetch_dropped.saturating_add(right.prefetch_dropped),
        prefetch_completed: left
            .prefetch_completed
            .saturating_add(right.prefetch_completed),
        prefetch_failed: left.prefetch_failed.saturating_add(right.prefetch_failed),
        prefetch_object_gets: left
            .prefetch_object_gets
            .saturating_add(right.prefetch_object_gets),
        prefetch_object_get_bytes: left
            .prefetch_object_get_bytes
            .saturating_add(right.prefetch_object_get_bytes),
        prefetch_cache_hits: left
            .prefetch_cache_hits
            .saturating_add(right.prefetch_cache_hits),
        prefetch_cache_hit_bytes: left
            .prefetch_cache_hit_bytes
            .saturating_add(right.prefetch_cache_hit_bytes),
        read_plan_cache_hits: left
            .read_plan_cache_hits
            .saturating_add(right.read_plan_cache_hits),
        read_plan_cache_misses: left
            .read_plan_cache_misses
            .saturating_add(right.read_plan_cache_misses),
        object_writeback_enqueued: left
            .object_writeback_enqueued
            .saturating_add(right.object_writeback_enqueued),
        object_writeback_inline: left
            .object_writeback_inline
            .saturating_add(right.object_writeback_inline),
        object_writeback_completed: left
            .object_writeback_completed
            .saturating_add(right.object_writeback_completed),
        object_writeback_failed: left
            .object_writeback_failed
            .saturating_add(right.object_writeback_failed),
        object_writeback_staged_bytes: left
            .object_writeback_staged_bytes
            .saturating_add(right.object_writeback_staged_bytes),
        object_writeback_uploaded_bytes: left
            .object_writeback_uploaded_bytes
            .saturating_add(right.object_writeback_uploaded_bytes),
        object_writeback_queue_wait_ns: left
            .object_writeback_queue_wait_ns
            .saturating_add(right.object_writeback_queue_wait_ns),
        object_writeback_queue_max_wait_ns: left
            .object_writeback_queue_max_wait_ns
            .max(right.object_writeback_queue_max_wait_ns),
        object_writeback_upload_ns: left
            .object_writeback_upload_ns
            .saturating_add(right.object_writeback_upload_ns),
        object_writeback_upload_max_ns: left
            .object_writeback_upload_max_ns
            .max(right.object_writeback_upload_max_ns),
        object_writeback_collect_ns: left
            .object_writeback_collect_ns
            .saturating_add(right.object_writeback_collect_ns),
        object_writeback_digest_ns: left
            .object_writeback_digest_ns
            .saturating_add(right.object_writeback_digest_ns),
        object_writeback_store_put_ns: left
            .object_writeback_store_put_ns
            .saturating_add(right.object_writeback_store_put_ns),
        object_writeback_cache_put_ns: left
            .object_writeback_cache_put_ns
            .saturating_add(right.object_writeback_cache_put_ns),
        manifest_chunks: left.manifest_chunks.saturating_add(right.manifest_chunks),
        manifest_blocks: left.manifest_blocks.saturating_add(right.manifest_blocks),
    }
}

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!(
            "\nUsage: nokv-bench [--profile smoke|standard|long] \
             [--workload all|metadata-smoke|mdtest-easy|mdtest-hard|metadata-negative-lookup|artifact-index-lookup|metadata-concurrent-read|metadata-durability-batch|metadata-shard-routing|checkpoint-publish|training-read|native-layout-read|ai-dataset-batch-read|ai-shard-range-read|mlperf-dlio|demo-dataset] \
             [--root PATH] [--object-backend s3|rustfs] \
             [--hot-object-root PATH] [--hot-object-max-bytes N] [--hot-fill-mode inline|background] \
             [--object-concurrency N] [--checkpoint-bytes N] [--sample-bytes N] \
             [--read-repeats N] [--range-stride N] [--range-coalesce-gap-bytes N] \
             [--block-cache on|off] [--keep]"
        );
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), BenchError> {
    let config = parse(args)?;
    let shape = shape(&config);
    fs::create_dir_all(&config.root).map_err(from_io)?;

    println!("{}", csv_header());
    for workload in expand_workloads(config.workload) {
        let labels = boundary_labels(&config, workload_name(workload));
        for row in run_one(&config, &shape, workload)? {
            println!("{}", csv_row(&row, &labels));
        }
    }

    if !config.keep {
        fs::remove_dir_all(&config.root).map_err(from_io)?;
    }
    Ok(())
}

fn run_one(
    config: &Config,
    shape: &WorkloadShape,
    workload: Workload,
) -> Result<Vec<ResultRow>, BenchError> {
    if workload == Workload::MetadataDurabilityBatch {
        return bench_metadata_durability_batch(config, shape);
    }
    if workload == Workload::MetadataShardRouting {
        return bench_metadata_shard_routing(config, shape);
    }
    let label = workload_name(workload);
    let client = client_for(config, label)?;
    client.bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let row = match workload {
        Workload::MdtestEasy => bench_mdtest_easy(client.as_ref(), config, shape)?,
        Workload::MdtestHard => bench_mdtest_hard(client.as_ref(), config, shape)?,
        Workload::MetadataNegativeLookup => {
            bench_metadata_negative_lookup(client.as_ref(), config, shape)?
        }
        Workload::ArtifactIndexLookup => {
            bench_artifact_index_lookup(client.as_ref(), config, shape)?
        }
        Workload::MetadataConcurrentRead => {
            bench_metadata_concurrent_read(client.as_ref(), config, shape)?
        }
        Workload::MetadataDurabilityBatch => unreachable!("durability batch uses two clients"),
        Workload::MetadataShardRouting => {
            unreachable!("shard routing builds its own in-process fleet")
        }
        Workload::CheckpointPublish => bench_checkpoint_publish(client.as_ref(), config, shape)?,
        Workload::TrainingRead => bench_training_read(client.as_ref(), config, shape)?,
        Workload::NativeLayoutRead => {
            return bench_native_layout_read(client.as_ref(), config, shape)
        }
        Workload::AiDatasetBatchRead => {
            return bench_ai_dataset_batch_read(client.as_ref(), config, shape)
        }
        Workload::AiShardRangeRead => {
            return bench_ai_shard_range_read(client.as_ref(), config, shape)
        }
        Workload::MlperfDlio => bench_mlperf_dlio(client.as_ref(), config, shape)?,
        Workload::DemoDataset => bench_demo_dataset(client.as_ref(), config, shape)?,
        Workload::MetadataSmoke => unreachable!("metadata-smoke expands before execution"),
        Workload::All => unreachable!("all expands before execution"),
    };
    Ok(vec![row])
}

fn bench_mdtest_easy(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    client.mkdir("/mdtest-easy", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let dir_paths = (0..shape.dirs)
        .map(|dir| format!("/mdtest-easy/dir-{dir:05}"))
        .collect::<Vec<_>>();
    for entry in client.mkdirs(&dir_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    let mut file_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for dir in 0..shape.dirs {
        let dir_path = format!("/mdtest-easy/dir-{dir:05}");
        checksum = checksum.wrapping_add(dir as u64);
        file_paths
            .extend((0..shape.files_per_dir).map(|file| format!("{dir_path}/file-{file:05}")));
    }
    for entry in client.create_files(&file_paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    let operations = shape.dirs + shape.dirs * shape.files_per_dir + 1;
    Ok(row(RowInput {
        workload: "mdtest-easy",
        profile: config.profile,
        operations,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dirs={} files_per_dir={} file_body=metadata-only",
            shape.dirs, shape.files_per_dir
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_mdtest_hard(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/mdtest-hard", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    let paths = (0..shape.shared_files)
        .map(|file| format!("/mdtest-hard/file-{file:06}"))
        .collect::<Vec<_>>();
    for entry in client.create_files(&paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    Ok(row(RowInput {
        workload: "mdtest-hard",
        profile: config.profile,
        operations: shape.shared_files,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "shared_dir_files={} file_body=metadata-only",
            shape.shared_files
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_metadata_negative_lookup(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir(
        "/metadata-negative-lookup",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    let dir_paths = (0..shape.dirs)
        .map(|dir| format!("/metadata-negative-lookup/dir-{dir:05}"))
        .collect::<Vec<_>>();
    let mut checksum = 0_u64;
    for entry in client.mkdirs(&dir_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }

    let mut present_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for dir in 0..shape.dirs {
        let dir_path = format!("/metadata-negative-lookup/dir-{dir:05}");
        present_paths
            .extend((0..shape.files_per_dir).map(|file| format!("{dir_path}/present-{file:05}")));
    }
    for entry in client.create_files(&present_paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }

    let before = client.stats()?;
    let start = Instant::now();
    let mut probes = 0_usize;
    for dir in 0..shape.dirs {
        for file in 0..shape.files_per_dir {
            let path = format!("/metadata-negative-lookup/dir-{dir:05}/missing-{file:05}");
            if client.lookup(&path)?.is_some() {
                return Err(BenchError::Client(format!(
                    "negative lookup unexpectedly found {path}"
                )));
            }
            checksum = checksum.wrapping_add(((dir as u64) << 32) ^ file as u64);
            probes += 1;
        }
    }

    Ok(row(RowInput {
        workload: "metadata-negative-lookup",
        profile: config.profile,
        operations: probes,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dirs={} missing_per_dir={} setup_present_per_dir={} file_body=metadata-only",
            shape.dirs, shape.files_per_dir, shape.files_per_dir
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_artifact_index_lookup(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir(
        "/artifact-index",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    let run_paths = (0..shape.dirs)
        .map(|run| format!("/artifact-index/run-{run:05}"))
        .collect::<Vec<_>>();
    client.mkdirs(&run_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;

    let mut artifact_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for run in 0..shape.dirs {
        let run_path = format!("/artifact-index/run-{run:05}");
        for artifact in 0..shape.files_per_dir {
            let path = format!("{run_path}/artifact-{artifact:05}.bin");
            let manifest_id = format!("artifact-index/run-{run:05}/artifact-{artifact:05}");
            client.put_artifact(
                &path,
                vec![artifact as u8],
                artifact_metadata("artifact-index", &manifest_id),
            )?;
            artifact_paths.push(path);
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    for path in &artifact_paths {
        let metadata = client.stat_path(path)?.ok_or_else(|| {
            BenchError::Client(format!("artifact stat missed indexed path {path}"))
        })?;
        checksum = checksum
            .wrapping_add(metadata.attr.inode.get())
            .wrapping_add(metadata.attr.generation);
    }
    for run_path in &run_paths {
        let entries = client.list_indexed(run_path)?;
        if entries.len() != shape.files_per_dir {
            return Err(BenchError::Client(format!(
                "indexed list for {run_path} returned {} entries, expected {}",
                entries.len(),
                shape.files_per_dir
            )));
        }
        checksum = checksum.wrapping_add(entries.len() as u64);
    }

    Ok(row(RowInput {
        workload: "artifact-index-lookup",
        profile: config.profile,
        operations: artifact_paths.len() + run_paths.len(),
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "runs={} artifacts_per_run={} timed_ops=stat_path_plus_indexed_list",
            shape.dirs, shape.files_per_dir
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_metadata_concurrent_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir(
        "/metadata-concurrent-read",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    let run_paths = (0..shape.dirs)
        .map(|run| format!("/metadata-concurrent-read/run-{run:05}"))
        .collect::<Vec<_>>();
    client.mkdirs(&run_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;

    let mut artifact_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for run in 0..shape.dirs {
        let run_path = format!("/metadata-concurrent-read/run-{run:05}");
        for artifact in 0..shape.files_per_dir {
            let path = format!("{run_path}/artifact-{artifact:05}.bin");
            let manifest_id =
                format!("metadata-concurrent-read/run-{run:05}/artifact-{artifact:05}");
            client.put_artifact(
                &path,
                vec![artifact as u8],
                artifact_metadata("metadata-concurrent-read", &manifest_id),
            )?;
            artifact_paths.push(path);
        }
    }

    for path in &artifact_paths {
        client
            .stat_path(path)?
            .ok_or_else(|| BenchError::Client(format!("warmup stat missed indexed path {path}")))?;
    }
    for run_path in &run_paths {
        let entries = client.list_indexed(run_path)?;
        if entries.len() != shape.files_per_dir {
            return Err(BenchError::Client(format!(
                "warmup indexed list for {run_path} returned {} entries, expected {}",
                entries.len(),
                shape.files_per_dir
            )));
        }
    }

    let stat_ops = artifact_paths.len() * config.read_repeats;
    let list_ops = run_paths.len() * config.read_repeats;
    let operations = stat_ops + list_ops;
    let before = client.stats()?;
    let start = Instant::now();
    let checksum = run_parallel(operations, config.object_concurrency, |op| {
        if op < stat_ops {
            let path = &artifact_paths[op % artifact_paths.len()];
            let metadata = client.stat_path(path)?.ok_or_else(|| {
                BenchError::Client(format!("concurrent stat missed indexed path {path}"))
            })?;
            Ok(metadata
                .attr
                .inode
                .get()
                .wrapping_add(metadata.attr.generation))
        } else {
            let index = op - stat_ops;
            let run_path = &run_paths[index % run_paths.len()];
            let entries = client.list_indexed(run_path)?;
            if entries.len() != shape.files_per_dir {
                return Err(BenchError::Client(format!(
                    "concurrent indexed list for {run_path} returned {} entries, expected {}",
                    entries.len(),
                    shape.files_per_dir
                )));
            }
            Ok(entries.len() as u64)
        }
    })?;

    Ok(row(RowInput {
        workload: "metadata-concurrent-read",
        profile: config.profile,
        operations,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "runs={} artifacts_per_run={} read_repeats={} workers={} timed_ops=concurrent_stat_path_plus_indexed_list",
            shape.dirs, shape.files_per_dir, config.read_repeats, config.object_concurrency
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_metadata_durability_batch(
    config: &Config,
    shape: &WorkloadShape,
) -> Result<Vec<ResultRow>, BenchError> {
    let local = service_client_for_with_metadata_durability(
        config,
        "metadata-durability-batch-local-only",
        MetadataDurabilityMode::LocalOnly,
    )?;
    local.bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let local = bench_metadata_durability_batch_phase(
        local.as_ref(),
        config,
        shape,
        MetadataDurabilityMode::LocalOnly,
    )?;

    let sync = service_client_for_with_metadata_durability(
        config,
        "metadata-durability-batch-sync-shared-log",
        MetadataDurabilityMode::SyncSharedLog,
    )?;
    sync.bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let sync = bench_metadata_durability_batch_phase(
        sync.as_ref(),
        config,
        shape,
        MetadataDurabilityMode::SyncSharedLog,
    )?;

    Ok(vec![local, sync])
}

fn bench_metadata_durability_batch_phase(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
    mode: MetadataDurabilityMode,
) -> Result<ResultRow, BenchError> {
    let root = "/metadata-durability-batch";
    client.mkdir(root, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let dir_paths = (0..shape.dirs)
        .map(|dir| format!("{root}/shard-{dir:05}"))
        .collect::<Vec<_>>();
    client.mkdirs(&dir_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;

    let mut file_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for file in 0..shape.files_per_dir {
        for dir_path in &dir_paths {
            file_paths.push(format!("{dir_path}/sample-{file:05}.meta"));
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let entries = client.create_files(&file_paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)?;
    let seconds = start.elapsed().as_secs_f64();
    if entries.len() != file_paths.len() {
        return Err(BenchError::Client(format!(
            "metadata durability batch created {} entries, expected {}",
            entries.len(),
            file_paths.len()
        )));
    }
    let checksum = entries
        .iter()
        .fold(0_u64, |acc, entry| acc.wrapping_add(entry.attr.inode.get()));
    black_box(checksum);

    let mut row = row(RowInput {
        workload: "metadata-durability-batch",
        profile: config.profile,
        operations: file_paths.len(),
        seconds,
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dirs={} files_per_dir={} timed_ops=batch_create_files_across_dirs file_body=metadata-only",
            shape.dirs, shape.files_per_dir
        ),
        caveat: metadata_durability_caveat(config, mode),
    });
    row.phase = metadata_durability_phase(mode);
    Ok(row)
}

/// Number of subtree shards the distributed-metadata routing workload fans across
/// (plus the implicit default shard 0). Kept small and fixed so the in-process
/// fleet is bounded and the row is reproducible across runs/profiles. Overridable
/// at runtime via `NOKV_BENCH_SHARD_ROUTING_SHARDS` for the scaling sweep.
const SHARD_ROUTING_SHARDS: usize = 4;

/// Env var: number of subtree shards the routing fleet fans across. Defaults to
/// [`SHARD_ROUTING_SHARDS`] so unset runs (and the unit tests) keep today's shape.
const SHARD_ROUTING_SHARDS_ENV: &str = "NOKV_BENCH_SHARD_ROUTING_SHARDS";

/// Env var: number of worker threads driving the *fleet* phase of the routing
/// bench. Defaults to 1, which is the original strictly-sequential single-client
/// path — so the baseline behavior is unchanged unless this is set. For the
/// scaling demo set it equal to the shard count.
const SHARD_ROUTING_CONCURRENCY_ENV: &str = "NOKV_BENCH_SHARD_ROUTING_CONCURRENCY";

/// Read a positive-`usize` env override, falling back to `default` when the var is
/// unset, empty, unparseable, or zero. Keeping the fallback total means a typo
/// degrades to the documented default rather than silently disabling the phase.
fn shard_routing_env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|v| *v > 0)
            .unwrap_or(default),
        Err(_) => default,
    }
}

/// An in-process metadata fleet for the routing microbench: one `Server` per
/// shard (the default shard 0 plus `SHARD_ROUTING_SHARDS` subtree shards), all
/// sharing one `InMemoryControlStore`, fronted by a fleet `MetadataClient` that
/// resolves each request to its owning shard's endpoint. Each member's serve loop
/// runs on its own thread; the per-shard stats addresses let the bench read back
/// how many commits each shard actually absorbed.
struct ShardRoutingFleet {
    client: MetadataClient,
    /// Retained so concurrent drivers can mint one *independent* fleet
    /// `MetadataClient` per worker from the same control plane (see
    /// [`ShardRoutingFleet::new_client`]).
    control: Arc<dyn ControlStore>,
    mount: MountId,
    stats_addrs: Vec<SocketAddr>,
    prefixes: Vec<String>,
}

impl ShardRoutingFleet {
    /// Build a fleet of `shard_count` subtree shards (`/shard-00`..) plus the
    /// default shard. `root` scopes each member's meta dir. With `shard_count == 0`
    /// this is the single-shard baseline: just the default shard owns everything.
    fn build(root: &std::path::Path, shard_count: usize) -> Result<Self, BenchError> {
        let control = Arc::new(InMemoryControlStore::new());

        let mut stats_addrs = Vec::with_capacity(shard_count + 1);
        let mut prefixes = Vec::with_capacity(shard_count + 1);

        // Register every shard identity BEFORE any owner acquires so each slot
        // reads its index/prefix from the control record.
        nokv_control::register_shard(
            control.as_ref(),
            ShardId::new("mount-1:/"),
            "/",
            DEFAULT_SHARD_INDEX,
        )
        .map_err(from_client)?;
        for shard in 0..shard_count {
            let prefix = format!("/shard-{shard:02}");
            let shard_index = (shard + 1) as u16;
            nokv_control::register_shard(
                control.as_ref(),
                ShardId::new(format!("mount-1:{prefix}")),
                prefix.clone(),
                shard_index,
            )
            .map_err(from_client)?;
        }

        // Bring up the default shard owner.
        let default_addr = spawn_routing_member(root, Arc::clone(&control), "mount-1:/")?;
        stats_addrs.push(default_addr);
        prefixes.push("/".to_owned());
        // Bring up each subtree shard owner.
        for shard in 0..shard_count {
            let prefix = format!("/shard-{shard:02}");
            let addr =
                spawn_routing_member(root, Arc::clone(&control), &format!("mount-1:{prefix}"))?;
            stats_addrs.push(addr);
            prefixes.push(prefix);
        }

        let mount = MountId::new(1).unwrap();
        let control = Arc::clone(&control) as Arc<dyn ControlStore>;
        let client = MetadataClient::fleet(Arc::clone(&control), mount).map_err(from_client)?;
        Ok(Self {
            client,
            control,
            mount,
            stats_addrs,
            prefixes,
        })
    }

    /// Mint a fresh fleet `MetadataClient` against the same in-memory control
    /// plane and the already-running shard members. Each concurrent worker takes
    /// its own client so the per-connection write mutex and the per-client
    /// connection pool are not shared — the shared-client path serializes every
    /// request behind one `Mutex<TcpStream>` per shard, which would cap fleet
    /// throughput regardless of shard count.
    fn new_client(&self) -> Result<MetadataClient, BenchError> {
        MetadataClient::fleet(Arc::clone(&self.control), self.mount).map_err(from_client)
    }

    /// Sum of `commit_total` across every shard's metadata store — the server-side
    /// corroboration that the client's routed creates actually committed somewhere.
    fn total_commits(&self) -> Result<u64, BenchError> {
        let mut total: u64 = 0;
        for addr in &self.stats_addrs {
            total = total.saturating_add(fetch_server_stats(*addr)?.metadata_store.commit_total);
        }
        Ok(total)
    }

    /// Per-shard `commit_total`, indexed the same as `prefixes`/`stats_addrs`
    /// (slot 0 is the default shard). This is the measured request distribution the
    /// row reports, proving the fleet client fanned writes across the shards rather
    /// than funneling them to one node.
    fn per_shard_commits(&self) -> Result<Vec<u64>, BenchError> {
        self.stats_addrs
            .iter()
            .map(|addr| Ok(fetch_server_stats(*addr)?.metadata_store.commit_total))
            .collect()
    }
}

/// Open one routing-fleet member: a fresh `Server` owning `shard_id`, served on a
/// background thread off a bound localhost port. Its `NodeId` is its bind addr, so
/// the control record's endpoint resolves straight back to it for the fleet
/// client. The workload is metadata-only, so each member opens its own (never
/// contacted) fake-S3 object store from its config. Returns the bind address
/// (also the stats/HTTP address).
fn spawn_routing_member(
    root: &std::path::Path,
    control: Arc<InMemoryControlStore>,
    shard_id: &str,
) -> Result<SocketAddr, BenchError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let bind = listener.local_addr().map_err(from_io)?;
    let sanitized = shard_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    let options = ServerOptions {
        bind,
        mount: MountId::new(1).expect("mount id is non-zero"),
        meta_path: root.join(format!("meta-{sanitized}")),
        metadata_checkpoint_archive_prefix: None,
        object: ObjectStoreConfig::s3(S3ObjectStoreOptions {
            bucket: "unused".to_owned(),
            root: "/".to_owned(),
            region: "auto".to_owned(),
            endpoint: Some("http://127.0.0.1:1".to_owned()),
            access_key_id: Some("unused".to_owned()),
            secret_access_key: Some("unused".to_owned()),
            session_token: None,
            virtual_host_style: false,
            skip_signature: true,
        }),
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
        object_gc: ObjectGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
            read_lease_grace: ObjectGcOptions::default().read_lease_grace,
        },
        history_gc: HistoryGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
        },
        control: None,
    };
    // No renewal worker, so nothing races the measurement.
    let server = Server::open_with_control(
        options,
        control as Arc<dyn ControlStore>,
        vec![ServerShardOwnerOptions::fresh(shard_id, bind.to_string()).with_renewal(None)],
    )
    .map_err(from_client)?;
    thread::spawn(move || {
        let _ = server.serve(listener);
    });
    Ok(bind)
}

/// The path set the routing workload drives, split into the order it must be
/// created in.
struct ShardRoutingPaths {
    /// Top-level `/shard-NN` directories — one per subtree shard. Each is the
    /// shard's own prefix directory and must exist in that shard's namespace before
    /// anything lands under it (the prefix is not auto-created on acquire).
    shard_dirs: Vec<String>,
    /// `/shard-NN/dir-XXXXX` subdirectories, spread across shards by prefix.
    dir_paths: Vec<String>,
    /// `/shard-NN/dir-XXXXX/file-YYYYY` leaf files.
    file_paths: Vec<String>,
}

/// Build the path set the routing workload drives: `dirs` subdirectories (each
/// holding `files_per_dir` files) spread evenly across `shard_count` subtree
/// shards by their top-level prefix, so the fleet client must fan them across
/// shards. The same set is replayed against the single-shard baseline (where
/// every path routes to shard 0) so the two rows are directly comparable.
fn shard_routing_paths(shape: &WorkloadShape, shard_count: usize) -> ShardRoutingPaths {
    let shard_dirs = (0..shard_count)
        .map(|shard| format!("/shard-{shard:02}"))
        .collect::<Vec<_>>();
    let mut dir_paths = Vec::new();
    let mut file_paths = Vec::new();
    for dir in 0..shape.dirs {
        let shard = dir % shard_count;
        let dir_path = format!("/shard-{shard:02}/dir-{dir:05}");
        dir_paths.push(dir_path.clone());
        for file in 0..shape.files_per_dir {
            file_paths.push(format!("{dir_path}/file-{file:05}"));
        }
    }
    ShardRoutingPaths {
        shard_dirs,
        dir_paths,
        file_paths,
    }
}

/// Deliverable 2 — distributed-metadata routing microbench. Emits two comparable
/// rows over one path set: a single-shard baseline (every path served by shard 0)
/// and an `N`-shard fleet (paths fanned across shards by prefix, each request
/// routed to its owning shard's endpoint by the fleet client). The fleet row's
/// `shape` carries the measured per-shard request distribution and the routing
/// overhead vs the baseline so the two are read together.
fn bench_metadata_shard_routing(
    config: &Config,
    shape: &WorkloadShape,
) -> Result<Vec<ResultRow>, BenchError> {
    let shard_count = shard_routing_env_usize(SHARD_ROUTING_SHARDS_ENV, SHARD_ROUTING_SHARDS);
    let concurrency = shard_routing_env_usize(SHARD_ROUTING_CONCURRENCY_ENV, 1);
    let paths = shard_routing_paths(shape, shard_count);
    // ops = shard-dir mkdirs + subdir mkdirs + file creates + file lookups.
    let total_ops = paths.shard_dirs.len() + paths.dir_paths.len() + paths.file_paths.len() * 2;

    // --- Single-shard baseline: one server owns "/", so every path routes to
    // shard 0. This is the apples-to-apples reference the fleet row is measured
    // against (same path set, same op mix, one node).
    let baseline_root = config.root.join("metadata-shard-routing-baseline");
    let baseline = ShardRoutingFleet::build(&baseline_root, 0)?;
    baseline
        .client
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_client)?;
    // The baseline always stays sequential (concurrency=1): it is the one-node
    // reference the fleet's aggregate ops/s is measured against, so it must not
    // itself benefit from worker parallelism.
    let (baseline_seconds, baseline_checksum) = drive_shard_routing(&baseline.client, &paths)?;
    let baseline_commits = baseline.total_commits()?;
    let baseline_per_op_us = baseline_seconds * 1_000_000.0 / total_ops.max(1) as f64;
    let baseline_ops_per_sec = shard_routing_ops_per_sec(total_ops, baseline_seconds);
    let mut baseline_row = shard_routing_row(
        config,
        shape,
        total_ops,
        baseline_seconds,
        baseline_checksum,
        baseline_commits,
        format!(
            "topology=single-shard shards=1 concurrency=1 dirs={} files_per_dir={} paths={} op_mix=mkdir+create+lookup baseline_per_op_us={baseline_per_op_us:.3} fleet_ops_per_sec={baseline_ops_per_sec:.0} file_body=metadata-only",
            shape.dirs,
            shape.files_per_dir,
            paths.file_paths.len(),
        ),
        "single-node baseline: one in-process Holt metadata server owns every path (shard 0); sequential reference for the fleet routing row".to_owned(),
    );
    baseline_row.phase = "single-shard-baseline";

    // --- N-shard fleet: the same path set spread across `shard_count` subtree
    // shards, each served by its own in-process server; the fleet client resolves
    // every request to the owning shard's endpoint.
    let fleet_root = config.root.join("metadata-shard-routing-fleet");
    let fleet = ShardRoutingFleet::build(&fleet_root, shard_count)?;
    fleet
        .client
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_client)?;
    // concurrency==1 keeps the original single-client sequential path verbatim;
    // >1 fans the file create/lookup phases across that many workers, each with
    // its own fleet client, so routed throughput can scale with shard count.
    let (fleet_seconds, fleet_checksum) = if concurrency <= 1 {
        drive_shard_routing(&fleet.client, &paths)?
    } else {
        drive_shard_routing_concurrent(&fleet, &paths, concurrency)?
    };
    let fleet_commits = fleet.total_commits()?;
    let per_shard = fleet.per_shard_commits()?;

    // Server-side request distribution across the subtree shards (slot 0 is the
    // default shard, which only absorbs the root bootstrap here since every path
    // is under a /shard-NN prefix).
    let subtree_commits: Vec<u64> = per_shard.iter().skip(1).copied().collect();
    let max_shard = subtree_commits.iter().copied().max().unwrap_or(0);
    let min_shard = subtree_commits.iter().copied().min().unwrap_or(0);
    let routing_overhead_pct = if baseline_seconds > 0.0 {
        (fleet_seconds - baseline_seconds) / baseline_seconds * 100.0
    } else {
        0.0
    };
    let fleet_per_op_us = fleet_seconds * 1_000_000.0 / total_ops.max(1) as f64;
    let fleet_ops_per_sec = shard_routing_ops_per_sec(total_ops, fleet_seconds);
    // Even fan-out check: every subtree shard absorbed the same commit count.
    let even_fanout = subtree_commits.len() > 1 && max_shard == min_shard;
    let distribution = per_shard
        .iter()
        .zip(&fleet.prefixes)
        .map(|(commits, prefix)| format!("{prefix}={commits}"))
        .collect::<Vec<_>>()
        .join(",");

    let client_mode = if concurrency <= 1 {
        "shared-single-client-sequential"
    } else {
        "per-worker-fleet-clients"
    };
    let mut fleet_row = shard_routing_row(
        config,
        shape,
        total_ops,
        fleet_seconds,
        fleet_checksum,
        fleet_commits,
        format!(
            "topology=fleet shards={shard_count} concurrency={concurrency} client_mode={client_mode} dirs={} files_per_dir={} paths={} op_mix=mkdir+create+lookup per_shard_commits=[{distribution}] subtree_commit_skew_max_minus_min={} even_fanout={even_fanout} fleet_per_op_us={fleet_per_op_us:.3} fleet_ops_per_sec={fleet_ops_per_sec:.0} routing_overhead_vs_single_shard_pct={routing_overhead_pct:.2} file_body=metadata-only",
            shape.dirs,
            shape.files_per_dir,
            paths.file_paths.len(),
            max_shard.saturating_sub(min_shard),
        ),
        format!(
            "SINGLE-BOX, IN-PROCESS routing only: {shard_count} subtree shards plus the default shard, each an in-process Holt metadata server on a localhost TCP port, all sharing ONE in-memory control store; driven by {concurrency} worker thread(s) ({client_mode}). per-shard counts are server-side commit_total. NOT a multi-machine cluster, NOT a real network, NO etcd/quorum — this shows how routed metadata throughput scales with shard parallelism ON ONE MULTI-CORE BOX (expect a plateau near the physical core count), not cross-machine scaling"
        ),
    );
    // The exact shard count lives in `shape` (`shards=N`); the phase label stays a
    // stable static so downstream CSV consumers can group on it.
    fleet_row.phase = "fleet-multi-shard";

    Ok(vec![baseline_row, fleet_row])
}

/// Drive the routing op mix against `client`: create the top-level `/shard-NN`
/// directories, then every subdirectory, then every file, then look every file
/// back up (so each path is both written and routed-for-read). Returns the
/// elapsed wall time and a checksum of the touched inodes (folded so the work is
/// not optimized away). Everything is issued one-by-one (not batched) so each
/// path routes independently — batch create coalesces by parent and would mask
/// the per-request routing this workload exists to measure.
fn drive_shard_routing(
    client: &MetadataClient,
    paths: &ShardRoutingPaths,
) -> Result<(f64, u64), BenchError> {
    let start = Instant::now();
    let mut checksum = 0_u64;
    for dir in paths.shard_dirs.iter().chain(paths.dir_paths.iter()) {
        let entry = client
            .mkdir(dir, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    for path in &paths.file_paths {
        let entry = client
            .create_file(path, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    for path in &paths.file_paths {
        let entry = client
            .lookup(path)
            .map_err(from_client)?
            .ok_or_else(|| BenchError::Client(format!("shard-routing lookup missed {path}")))?;
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    black_box(checksum);
    Ok((start.elapsed().as_secs_f64(), checksum))
}

/// Concurrent variant of [`drive_shard_routing`] for the fleet scaling sweep.
/// Directories are still created single-threaded (a file create needs its parent
/// dir present, and the dirs themselves fan across shards by prefix so they are a
/// negligible fraction of the op mix), then the file creates and the file lookups
/// are each split into `workers` disjoint contiguous slices of `file_paths` and
/// driven in parallel. Crucially every worker gets its *own* fleet client
/// (`fleet.new_client()`) so threads do not serialize on a shared connection's
/// write mutex — that is what lets routed throughput actually rise with shard
/// count on a multi-core box. The whole mkdir+create+lookup span is timed so the
/// returned seconds/`total_ops` are defined identically to the sequential path,
/// keeping the baseline and fleet rows directly comparable.
fn drive_shard_routing_concurrent(
    fleet: &ShardRoutingFleet,
    paths: &ShardRoutingPaths,
    concurrency: usize,
) -> Result<(f64, u64), BenchError> {
    let workers = concurrency.max(1).min(paths.file_paths.len().max(1));
    let start = Instant::now();

    // Phase 1: directories, single-threaded (parents must exist before files).
    let mut checksum = 0_u64;
    for dir in paths.shard_dirs.iter().chain(paths.dir_paths.iter()) {
        let entry = fleet
            .client
            .mkdir(dir, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }

    // One independent fleet client per worker, reused across both the create and
    // the lookup phase so we pay the route-resolution + connect cost once.
    let clients: Vec<MetadataClient> = (0..workers)
        .map(|_| fleet.new_client())
        .collect::<Result<_, _>>()?;

    // Phase 2: file creates, partitioned across workers.
    let create_sum = run_shard_routing_phase(&clients, &paths.file_paths, |client, path| {
        let entry = client
            .create_file(path, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        Ok(entry.attr.inode.get())
    })?;

    // Phase 3: file lookups, same partition (each path written then routed-for-read).
    let lookup_sum = run_shard_routing_phase(&clients, &paths.file_paths, |client, path| {
        let entry = client
            .lookup(path)
            .map_err(from_client)?
            .ok_or_else(|| BenchError::Client(format!("shard-routing lookup missed {path}")))?;
        Ok(entry.attr.inode.get())
    })?;

    checksum = checksum.wrapping_add(create_sum).wrapping_add(lookup_sum);
    black_box(checksum);
    Ok((start.elapsed().as_secs_f64(), checksum))
}

/// Run one routing phase (create or lookup) over `paths`, splitting them into
/// `clients.len()` disjoint contiguous slices — one worker thread per client —
/// and folding each touched inode into a wrapping checksum. Contiguous slicing
/// (rather than round-robin) keeps a worker's slice spanning several `/shard-NN`
/// prefixes, so every worker still drives every shard and the fan-out the row
/// reports stays even. The first error aborts the scope and is surfaced.
fn run_shard_routing_phase<F>(
    clients: &[MetadataClient],
    paths: &[String],
    op: F,
) -> Result<u64, BenchError>
where
    F: Fn(&MetadataClient, &str) -> Result<u64, BenchError> + Sync,
{
    let workers = clients.len().max(1);
    let chunk = paths.len().div_ceil(workers).max(1);
    let checksum = AtomicU64::new(0);
    let error: Mutex<Option<BenchError>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for (client, slice) in clients.iter().zip(paths.chunks(chunk)) {
            let checksum = &checksum;
            let error = &error;
            let op = &op;
            scope.spawn(move || {
                let mut local = 0_u64;
                for path in slice {
                    match op(client, path) {
                        Ok(value) => local = local.wrapping_add(value),
                        Err(err) => {
                            *error.lock().expect("shard-routing error lock") = Some(err);
                            return;
                        }
                    }
                }
                checksum.fetch_add(local, Ordering::Relaxed);
            });
        }
    });
    if let Some(err) = error.into_inner().expect("shard-routing error lock") {
        return Err(err);
    }
    Ok(checksum.load(Ordering::Relaxed))
}

/// Aggregate ops/s for a routing phase: `operations` total ops over `seconds`
/// wall-clock. Returns 0.0 for a zero/degenerate duration so the row never
/// divides by zero.
fn shard_routing_ops_per_sec(operations: usize, seconds: f64) -> f64 {
    if seconds > 0.0 {
        operations as f64 / seconds
    } else {
        0.0
    }
}

/// Assemble a result row for a shard-routing phase. The routing microbench is
/// metadata-only and has no object/data-fabric traffic, so all object/tier
/// counters are zero; the load-bearing numbers are `ops_per_second`,
/// `metadata_commits`, and the per-shard distribution carried in `shape`.
#[allow(clippy::too_many_arguments)]
fn shard_routing_row(
    config: &Config,
    _shape: &WorkloadShape,
    operations: usize,
    seconds: f64,
    checksum: u64,
    metadata_commits: u64,
    shape_desc: String,
    caveat: String,
) -> ResultRow {
    let mut row = row(RowInput {
        workload: "metadata-shard-routing",
        profile: config.profile,
        operations,
        seconds,
        bytes: 0,
        samples: 0,
        stats: BenchStats::default(),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: shape_desc,
        caveat,
    });
    // The aggregate commit count is gathered from the fleet's own per-shard stats
    // endpoints rather than a single server, so set it on the row directly.
    row.metadata_commits = metadata_commits;
    row
}

fn bench_checkpoint_publish(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/checkpoints", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let first = checkpoint_payload(0, shape.checkpoint_bytes);
    client.put_artifact(
        "/checkpoints/latest.ckpt",
        first,
        artifact_metadata("checkpoint", "checkpoints/latest-initial"),
    )?;

    let before = client.stats()?;
    let start = Instant::now();
    let stage_checksum = run_parallel(shape.checkpoints, config.object_concurrency, |step| {
        let stage_path = format!("/checkpoints/.stage-{step:06}");
        let manifest_id = format!("checkpoints/stage-{step:06}");
        let bytes = checkpoint_payload(step, shape.checkpoint_bytes);
        let staged = client.put_artifact(
            &stage_path,
            bytes,
            artifact_metadata("checkpoint", &manifest_id),
        )?;
        Ok(staged.attr.inode.get())
    })?;

    let mut checksum = stage_checksum;
    for step in 0..shape.checkpoints {
        let stage_path = format!("/checkpoints/.stage-{step:06}");
        let result = client.rename_replace(&stage_path, "/checkpoints/latest.ckpt")?;
        checksum = checksum
            .wrapping_add(result.entry.attr.inode.get())
            .wrapping_add(
                result
                    .replaced
                    .map(|entry| entry.attr.inode.get())
                    .unwrap_or(0),
            );
    }
    Ok(row(RowInput {
        workload: "checkpoint-publish",
        profile: config.profile,
        operations: shape.checkpoints * 2,
        seconds: start.elapsed().as_secs_f64(),
        bytes: (shape.checkpoints * shape.checkpoint_bytes) as u64,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "iterations={} payload_bytes={} ops=count_parallel_put_plus_atomic_replace",
            shape.checkpoints, shape.checkpoint_bytes
        ),
        caveat: object_caveat(config, "object put plus metadata rename-replace"),
    }))
}

fn bench_training_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/dataset", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let shard_paths = (0..shape.dataset_dirs)
        .map(|shard| format!("/dataset/shard-{shard:04}"))
        .collect::<Vec<_>>();
    client.mkdirs(&shard_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/dataset/shard-{shard:04}");
        for file in 0..shape.dataset_files_per_dir {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let manifest_id = format!("dataset/shard-{shard:04}/sample-{file:05}.bin");
            let payload = dataset_payload(shard, file, shape.dataset_file_bytes);
            client.put_artifact(&path, payload, artifact_metadata("dataset", &manifest_id))?;
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let checksum = run_parallel(shape.dataset_dirs, config.object_concurrency, |shard| {
        let shard_path = format!("/dataset/shard-{shard:04}");
        let entries = client.list(&shard_path)?;
        let mut checksum = entries.len() as u64;
        if let Some(first) = entries.first() {
            let name = String::from_utf8_lossy(first.dentry.name.as_bytes());
            let path = format!("{shard_path}/{name}");
            for _ in 0..config.read_repeats {
                let bytes = client.cat(&path)?;
                checksum =
                    checksum.wrapping_add(bytes.iter().map(|byte| *byte as u64).sum::<u64>());
            }
        }
        Ok(checksum)
    })?;
    black_box(checksum);
    Ok(row(RowInput {
        workload: "training-read",
        profile: config.profile,
        operations: shape.dataset_dirs * (1 + config.read_repeats),
        seconds: start.elapsed().as_secs_f64(),
        bytes: (shape.dataset_dirs * shape.dataset_file_bytes * config.read_repeats) as u64,
        samples: shape.dataset_dirs * config.read_repeats,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dataset_dirs={} files_per_dir={} sample_bytes={} timed_ops=list_plus_repeated_read_per_dir",
            shape.dataset_dirs, shape.dataset_files_per_dir, shape.dataset_file_bytes
        ),
        caveat: object_caveat(config, "warm object reads after deterministic dataset seed"),
    }))
}

fn bench_native_layout_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<Vec<ResultRow>, BenchError> {
    client.mkdir(
        "/native-layout-read",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    client.mkdir(
        "/native-layout-read/samples",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;

    let checkpoint_path = "/native-layout-read/checkpoint.bin";
    let checkpoint = client.put_artifact(
        checkpoint_path,
        checkpoint_payload(0, shape.checkpoint_bytes),
        artifact_metadata(
            "native-layout-read-checkpoint",
            "native-layout-read/checkpoint",
        ),
    )?;
    let mut requests = vec![NativeReadRequest {
        path: checkpoint_path.to_owned(),
        offset: 0,
        len: shape.checkpoint_bytes,
        generation: body_generation(&checkpoint),
    }];

    let samples_per_shard = shape.dataset_files_per_dir.clamp(1, 16);
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/native-layout-read/samples/shard-{shard:04}");
        client.mkdir(&shard_path, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
        for file in 0..samples_per_shard {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let manifest_id =
                format!("native-layout-read/samples/shard-{shard:04}/sample-{file:05}");
            let entry = client.put_artifact(
                &path,
                dataset_payload(shard, file, shape.dataset_file_bytes),
                artifact_metadata("native-layout-read-sample", &manifest_id),
            )?;
            if file == (shard * 7 + config.read_repeats) % samples_per_shard {
                requests.push(NativeReadRequest {
                    path,
                    offset: 0,
                    len: shape.dataset_file_bytes,
                    generation: body_generation(&entry),
                });
            }
        }
    }
    let sample_requests = requests.len().saturating_sub(1);
    if sample_requests > 1 {
        let rotate = config.read_repeats % sample_requests;
        requests[1..].rotate_left(rotate);
    }

    let cold = bench_native_layout_read_phase(client, config, shape, "cold", &requests)?;
    let warm = bench_native_layout_read_phase(client, config, shape, "warm", &requests)?;
    Ok(vec![cold, warm])
}

fn bench_native_layout_read_phase(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
    phase: &'static str,
    requests: &[NativeReadRequest],
) -> Result<ResultRow, BenchError> {
    let before = client.stats()?;
    let start = Instant::now();
    let checksum = run_parallel(requests.len(), config.object_concurrency, |index| {
        let request = &requests[index];
        let bytes = client.read_path(
            &request.path,
            request.offset,
            request.len,
            Some(request.generation),
        )?;
        Ok(bytes.iter().map(|byte| *byte as u64).sum::<u64>())
    })?;
    black_box(checksum);
    let mut row = row(RowInput {
        workload: "native-layout-read",
        profile: config.profile,
        operations: requests.len(),
        seconds: start.elapsed().as_secs_f64(),
        bytes: requests.iter().map(|request| request.len as u64).sum(),
        samples: requests.len(),
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "phase={} large_sequential=1 checkpoint_bytes={} shuffled_samples={} sample_bytes={} full_file_layout_reads=true",
            phase,
            shape.checkpoint_bytes,
            requests.len().saturating_sub(1),
            shape.dataset_file_bytes
        ),
        caveat: object_caveat(
            config,
            "native layout-open read_path over data-fabric executor; cold phase should populate full-object hot tier, warm phase should expose local hot hits"
        ),
    });
    row.phase = phase;
    Ok(row)
}

fn bench_ai_dataset_batch_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<Vec<ResultRow>, BenchError> {
    client.mkdir(
        "/ai-dataset-batch-read",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    client.mkdir(
        "/ai-dataset-batch-read/samples",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;

    let samples_per_shard = shape.dataset_files_per_dir.clamp(1, 32);
    let mut requests = Vec::with_capacity(shape.dataset_dirs * samples_per_shard);
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/ai-dataset-batch-read/samples/shard-{shard:04}");
        client.mkdir(&shard_path, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
        for file in 0..samples_per_shard {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let manifest_id =
                format!("ai-dataset-batch-read/samples/shard-{shard:04}/sample-{file:05}");
            let entry = client.put_artifact(
                &path,
                dataset_payload(shard, file, shape.dataset_file_bytes),
                artifact_metadata("ai-dataset-batch-read-sample", &manifest_id),
            )?;
            requests.push(NativeReadRequest {
                path,
                offset: 0,
                len: shape.dataset_file_bytes,
                generation: body_generation(&entry),
            });
        }
    }
    if requests.len() > 1 {
        let rotate = config.read_repeats % requests.len();
        requests.rotate_left(rotate);
    }

    let cold = bench_ai_dataset_batch_read_phase(client, config, shape, "cold", &requests)?;
    let warm = bench_ai_dataset_batch_read_phase(client, config, shape, "warm", &requests)?;
    Ok(vec![cold, warm])
}

fn bench_ai_dataset_batch_read_phase(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
    phase: &'static str,
    requests: &[NativeReadRequest],
) -> Result<ResultRow, BenchError> {
    let batch_size = config.object_concurrency.max(1);
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    for chunk in requests.chunks(batch_size) {
        let reads = client.read_paths(chunk)?;
        checksum = checksum.saturating_add(
            reads
                .iter()
                .map(|bytes| bytes.iter().map(|byte| *byte as u64).sum::<u64>())
                .sum::<u64>(),
        );
    }
    black_box(checksum);
    let mut row = row(RowInput {
        workload: "ai-dataset-batch-read",
        profile: config.profile,
        operations: requests.len(),
        seconds: start.elapsed().as_secs_f64(),
        bytes: requests.iter().map(|request| request.len as u64).sum(),
        samples: requests.len(),
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "phase={} batch_size={} dataset_dirs={} samples_per_dir={} sample_bytes={} layout_plan_batch=true",
            phase,
            batch_size,
            shape.dataset_dirs,
            shape.dataset_files_per_dir.clamp(1, 32),
            shape.dataset_file_bytes
        ),
        caveat: object_caveat(
            config,
            "AI dataset batch read through SDK batch layout-open plus parallel object reads; cold phase should expose object fallbacks, warm phase should expose local hot hits",
        ),
    });
    row.phase = phase;
    Ok(row)
}

fn bench_ai_shard_range_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<Vec<ResultRow>, BenchError> {
    client.mkdir(
        "/ai-shard-range-read",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    client.mkdir(
        "/ai-shard-range-read/shards",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;

    let samples_per_shard = shape.dataset_files_per_dir.clamp(1, 256);
    let mut requests = Vec::with_capacity(shape.dataset_dirs);
    for shard in 0..shape.dataset_dirs {
        let path = format!("/ai-shard-range-read/shards/shard-{shard:04}.bin");
        let manifest_id = format!("ai-shard-range-read/shards/shard-{shard:04}");
        let shard_bytes = samples_per_shard
            .checked_mul(shape.dataset_file_bytes)
            .ok_or_else(|| BenchError::Client("shard payload size overflows usize".to_owned()))?;
        let mut payload = Vec::with_capacity(shard_bytes);
        let mut ranges = Vec::with_capacity(samples_per_shard);
        for sample in 0..samples_per_shard {
            let offset = u64::try_from(payload.len())
                .map_err(|_| BenchError::Client("shard offset exceeds u64".to_owned()))?;
            payload.extend(dataset_payload(shard, sample, shape.dataset_file_bytes));
            if sample % config.range_stride == 0 {
                ranges.push(NativeReadRange {
                    offset,
                    len: shape.dataset_file_bytes,
                });
            }
        }
        let entry = client.put_artifact(
            &path,
            payload,
            artifact_metadata("ai-shard-range-read-shard", &manifest_id),
        )?;
        requests.push(ShardRangeReadRequest {
            path,
            generation: body_generation(&entry),
            ranges,
        });
    }
    if requests.len() > 1 {
        let rotate = config.read_repeats % requests.len();
        requests.rotate_left(rotate);
    }

    let cold = bench_ai_shard_range_read_phase(client, config, shape, "cold", &requests)?;
    let warm = bench_ai_shard_range_read_phase(client, config, shape, "warm", &requests)?;
    Ok(vec![cold, warm])
}

fn bench_ai_shard_range_read_phase(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
    phase: &'static str,
    requests: &[ShardRangeReadRequest],
) -> Result<ResultRow, BenchError> {
    let before = client.stats()?;
    let start = Instant::now();
    let batch_size = config.object_concurrency.max(1);
    let mut checksum = 0_u64;
    for chunk in requests.chunks(batch_size) {
        let reads = client.read_range_batches(chunk, config.range_coalesce_gap_bytes)?;
        checksum = checksum.saturating_add(
            reads
                .iter()
                .flat_map(|request_reads| request_reads.iter())
                .map(|bytes| bytes.iter().map(|byte| *byte as u64).sum::<u64>())
                .sum::<u64>(),
        );
    }
    black_box(checksum);
    let samples = requests
        .iter()
        .map(|request| request.ranges.len())
        .sum::<usize>();
    let bytes = requests
        .iter()
        .flat_map(|request| request.ranges.iter())
        .map(|range| range.len as u64)
        .sum::<u64>();
    let mut row = row(RowInput {
        workload: "ai-shard-range-read",
        profile: config.profile,
        operations: samples,
        seconds: start.elapsed().as_secs_f64(),
        bytes,
        samples,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "phase={} shard_count={} batch_size={} samples_per_shard={} selected_samples_per_shard={} sample_bytes={} range_stride={} max_gap_bytes={} range_coalescing=true range_batch_open=true",
            phase,
            requests.len(),
            batch_size,
            shape.dataset_files_per_dir.clamp(1, 256),
            requests.first().map(|request| request.ranges.len()).unwrap_or(0),
            shape.dataset_file_bytes,
            config.range_stride,
            config.range_coalesce_gap_bytes
        ),
        caveat: object_caveat(
            config,
            "AI shard range read through SDK batch coalesced range reads; warm phase should expose local hot hits",
        ),
    });
    row.phase = phase;
    Ok(row)
}

fn bench_mlperf_dlio(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/mlperf-dlio", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    client.mkdir(
        "/mlperf-dlio/dataset",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    client.mkdir(
        "/mlperf-dlio/checkpoints",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;

    let shard_paths = (0..shape.dataset_dirs)
        .map(|shard| format!("/mlperf-dlio/dataset/shard-{shard:04}"))
        .collect::<Vec<_>>();
    client.mkdirs(&shard_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/mlperf-dlio/dataset/shard-{shard:04}");
        for file in 0..shape.dataset_files_per_dir {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let manifest_id = format!("mlperf-dlio/dataset/shard-{shard:04}/sample-{file:05}");
            let sample_payload = dataset_payload(shard, file, shape.dataset_file_bytes);
            client.put_artifact(
                &path,
                sample_payload,
                artifact_metadata("mlperf-dlio-dataset", &manifest_id),
            )?;
        }
    }

    client.put_artifact(
        "/mlperf-dlio/checkpoints/latest.ckpt",
        checkpoint_payload(0, shape.checkpoint_bytes),
        artifact_metadata(
            "mlperf-dlio-checkpoint",
            "mlperf-dlio/checkpoints/latest-initial",
        ),
    )?;

    let checkpoint_steps = shape.checkpoints.max(1) / 4;
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = run_parallel(shape.dataset_dirs, config.object_concurrency, |shard| {
        let shard_path = format!("/mlperf-dlio/dataset/shard-{shard:04}");
        let entries = client.list(&shard_path)?;
        let mut checksum = entries.len() as u64;
        if let Some(first) = entries.first() {
            let name = String::from_utf8_lossy(first.dentry.name.as_bytes());
            let path = format!("{shard_path}/{name}");
            for _ in 0..config.read_repeats {
                let bytes = client.cat(&path)?;
                checksum = checksum.wrapping_add(bytes.len() as u64);
            }
        }
        Ok(checksum)
    })?;
    let stage_checksum = run_parallel(checkpoint_steps, config.object_concurrency, |step| {
        let stage_path = format!("/mlperf-dlio/checkpoints/.stage-{step:06}");
        let manifest_id = format!("mlperf-dlio/checkpoints/stage-{step:06}");
        let entry = client.put_artifact(
            &stage_path,
            checkpoint_payload(step, shape.checkpoint_bytes),
            artifact_metadata("mlperf-dlio-checkpoint", &manifest_id),
        )?;
        Ok(entry.attr.inode.get())
    })?;
    checksum = checksum.wrapping_add(stage_checksum);
    for step in 0..checkpoint_steps {
        let stage_path = format!("/mlperf-dlio/checkpoints/.stage-{step:06}");
        let result = client.rename_replace(&stage_path, "/mlperf-dlio/checkpoints/latest.ckpt")?;
        checksum = checksum.wrapping_add(result.entry.attr.inode.get());
    }
    black_box(checksum);
    Ok(row(RowInput {
        workload: "mlperf-dlio",
        profile: config.profile,
        operations: shape.dataset_dirs * (1 + config.read_repeats) + checkpoint_steps * 2,
        seconds: start.elapsed().as_secs_f64(),
        bytes: (shape.dataset_dirs * shape.dataset_file_bytes * config.read_repeats
            + checkpoint_steps * shape.checkpoint_bytes) as u64,
        samples: shape.dataset_dirs * config.read_repeats,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dlio_style_generated dataset_dirs={} files_per_dir={} sample_bytes={} checkpoint_steps={} checkpoint_bytes={}",
            shape.dataset_dirs,
            shape.dataset_files_per_dir,
            shape.dataset_file_bytes,
            checkpoint_steps,
            shape.checkpoint_bytes
        ),
        caveat: object_caveat(config, "MLPerf Storage/DLIO-style generated training read plus checkpoint write"),
    }))
}

fn bench_demo_dataset(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/demo-dataset", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let classes = shape.dataset_dirs.clamp(1, 8);
    let samples_per_class = shape.dataset_files_per_dir.clamp(1, 32);
    let sample_bytes = shape.dataset_file_bytes.clamp(128, 4096);
    let payload = vec![23_u8; sample_bytes];
    let class_paths = (0..classes)
        .map(|class| format!("/demo-dataset/class-{class:03}"))
        .collect::<Vec<_>>();
    client.mkdirs(&class_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    for class in 0..classes {
        let class_path = format!("/demo-dataset/class-{class:03}");
        for sample in 0..samples_per_class {
            let path = format!("{class_path}/sample-{sample:05}.bin");
            let manifest_id = format!("demo-dataset/class-{class:03}/sample-{sample:05}");
            client.put_artifact(
                &path,
                payload.clone(),
                artifact_metadata("demo-dataset", &manifest_id),
            )?;
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    for class in 0..classes {
        let class_path = format!("/demo-dataset/class-{class:03}");
        let entries = client.list(&class_path)?;
        for entry in entries.iter().take(2) {
            let name = String::from_utf8_lossy(entry.dentry.name.as_bytes());
            let path = format!("{class_path}/{name}");
            checksum = checksum.wrapping_add(client.cat(&path)?.len() as u64);
        }
    }
    Ok(row(RowInput {
        workload: "demo-dataset",
        profile: config.profile,
        operations: classes * 3,
        seconds: start.elapsed().as_secs_f64(),
        bytes: (classes * 2 * sample_bytes) as u64,
        samples: classes * 2,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "public_dataset_demo_shape classes={} samples_per_class={} sample_bytes={} timed=list_plus_two_reads_per_class",
            classes, samples_per_class, sample_bytes
        ),
        caveat: object_caveat(config, "small public-dataset-shaped demo without external download"),
    }))
}

fn row(input: RowInput) -> ResultRow {
    let cache_total = input.stats.object.object_gets + input.stats.object.cache_hits;
    let path_index_lookups = input.stats.metadata_service.path_index_lookup_total;
    let path_index_hits = input.stats.metadata_service.path_index_hit_total;
    let read_dir_plus_entries = input.stats.metadata_service.read_dir_plus_entry_total;
    let read_dir_plus_projection_hits = input
        .stats
        .metadata_service
        .read_dir_plus_projection_hit_total;
    ResultRow {
        workload: input.workload,
        profile: input.profile,
        operations: input.operations,
        seconds: input.seconds,
        ops_per_second: input.operations as f64 / input.seconds.max(f64::MIN_POSITIVE),
        mb_per_second: input.bytes as f64 / 1_048_576_f64 / input.seconds.max(f64::MIN_POSITIVE),
        samples_per_second: input.samples as f64 / input.seconds.max(f64::MIN_POSITIVE),
        object_puts: input.stats.object.object_puts,
        object_put_bytes: input.stats.object.object_put_bytes,
        object_gets: input.stats.object.object_gets,
        object_get_bytes: input.stats.object.object_get_bytes,
        coalesced_gets: input.stats.object.coalesced_gets,
        coalesced_get_bytes: input.stats.object.coalesced_get_bytes,
        cache_hits: input.stats.object.cache_hits,
        cache_hit_bytes: input.stats.object.cache_hit_bytes,
        cache_hit_rate: if cache_total == 0 {
            0.0
        } else {
            input.stats.object.cache_hits as f64 / cache_total as f64
        },
        prefetch_enqueued: input.stats.object.prefetch_enqueued,
        prefetch_dropped: input.stats.object.prefetch_dropped,
        prefetch_completed: input.stats.object.prefetch_completed,
        prefetch_failed: input.stats.object.prefetch_failed,
        prefetch_object_gets: input.stats.object.prefetch_object_gets,
        prefetch_object_get_bytes: input.stats.object.prefetch_object_get_bytes,
        prefetch_cache_hits: input.stats.object.prefetch_cache_hits,
        prefetch_cache_hit_bytes: input.stats.object.prefetch_cache_hit_bytes,
        read_plan_cache_hits: input.stats.object.read_plan_cache_hits,
        read_plan_cache_misses: input.stats.object.read_plan_cache_misses,
        object_writeback_enqueued: input.stats.object.object_writeback_enqueued,
        object_writeback_inline: input.stats.object.object_writeback_inline,
        object_writeback_completed: input.stats.object.object_writeback_completed,
        object_writeback_failed: input.stats.object.object_writeback_failed,
        object_writeback_staged_bytes: input.stats.object.object_writeback_staged_bytes,
        object_writeback_uploaded_bytes: input.stats.object.object_writeback_uploaded_bytes,
        object_writeback_queue_wait_ns: input.stats.object.object_writeback_queue_wait_ns,
        object_writeback_queue_max_wait_ns: input.stats.object.object_writeback_queue_max_wait_ns,
        object_writeback_upload_ns: input.stats.object.object_writeback_upload_ns,
        object_writeback_upload_max_ns: input.stats.object.object_writeback_upload_max_ns,
        object_writeback_collect_ns: input.stats.object.object_writeback_collect_ns,
        object_writeback_digest_ns: input.stats.object.object_writeback_digest_ns,
        object_writeback_store_put_ns: input.stats.object.object_writeback_store_put_ns,
        object_writeback_cache_put_ns: input.stats.object.object_writeback_cache_put_ns,
        manifest_chunks: input.stats.object.manifest_chunks,
        manifest_blocks: input.stats.object.manifest_blocks,
        data_fabric_planned_blocks: input.stats.data_fabric.planned_blocks,
        data_fabric_local_nvme_hits: input.stats.data_fabric.local_nvme_hits,
        data_fabric_object_fallbacks: input.stats.data_fabric.object_fallbacks,
        data_fabric_object_gets: input.stats.data_fabric.object_gets,
        data_fabric_object_get_bytes: input.stats.data_fabric.object_get_bytes,
        data_fabric_coalesced_ranges: input.stats.data_fabric.coalesced_ranges,
        data_fabric_coalesced_range_bytes: input.stats.data_fabric.coalesced_range_bytes,
        data_fabric_cache_hits: input.stats.data_fabric.cache_hits,
        data_fabric_cache_hit_bytes: input.stats.data_fabric.cache_hit_bytes,
        tiered_hot_gets: input.stats.tiered_object.hot_gets,
        tiered_hot_hits: input.stats.tiered_object.hot_hits,
        tiered_hot_misses: input.stats.tiered_object.hot_misses,
        tiered_hot_errors: input.stats.tiered_object.hot_errors,
        tiered_cold_gets: input.stats.tiered_object.cold_gets,
        tiered_cold_get_bytes: input.stats.tiered_object.cold_get_bytes,
        tiered_cold_puts: input.stats.tiered_object.cold_puts,
        tiered_cold_put_errors: input.stats.tiered_object.cold_put_errors,
        tiered_hot_puts: input.stats.tiered_object.hot_puts,
        tiered_hot_put_errors: input.stats.tiered_object.hot_put_errors,
        tiered_hot_fills: input.stats.tiered_object.hot_fills,
        tiered_hot_fill_enqueued: input.stats.tiered_object.hot_fill_enqueued,
        tiered_hot_fill_coalesced: input.stats.tiered_object.hot_fill_coalesced,
        tiered_hot_fill_errors: input.stats.tiered_object.hot_fill_errors,
        tiered_cold_deletes: input.stats.tiered_object.cold_deletes,
        tiered_hot_deletes: input.stats.tiered_object.hot_deletes,
        tiered_hot_delete_errors: input.stats.tiered_object.hot_delete_errors,
        tiered_hot_put_ns: input.stats.tiered_object.hot_put_ns,
        tiered_pending_cold_put_ns: input.stats.tiered_object.pending_cold_put_ns,
        tiered_cold_put_enqueue_ns: input.stats.tiered_object.cold_put_enqueue_ns,
        local_hot_resident_objects: input.stats.local_hot.resident_objects,
        local_hot_resident_bytes: input.stats.local_hot.resident_bytes,
        local_hot_max_bytes: input.stats.local_hot.max_bytes.unwrap_or(0),
        local_hot_evictions: input.stats.local_hot.evictions,
        local_hot_eviction_bytes: input.stats.local_hot.eviction_bytes,
        local_hot_admission_rejections: input.stats.local_hot.admission_rejections,
        local_hot_puts: input.stats.local_hot.puts,
        local_hot_put_bytes: input.stats.local_hot.put_bytes,
        local_hot_put_total_ns: input.stats.local_hot.put_total_ns,
        local_hot_put_prepare_ns: input.stats.local_hot.put_prepare_ns,
        local_hot_put_write_ns: input.stats.local_hot.put_write_ns,
        local_hot_put_sync_ns: input.stats.local_hot.put_sync_ns,
        local_hot_put_rename_ns: input.stats.local_hot.put_rename_ns,
        local_hot_put_record_ns: input.stats.local_hot.put_record_ns,
        metadata_commits: input.stats.metadata_store.commit_total,
        metadata_dedupe_hits: input.stats.metadata_store.dedupe_hit_total,
        metadata_predicates: input.stats.metadata_store.predicate_total,
        metadata_prefix_empty_predicates: input.stats.metadata_store.prefix_empty_predicate_total,
        metadata_gets: input.stats.metadata_store.get_total,
        metadata_get_user_strong: input.stats.metadata_store.get_user_strong_total,
        metadata_get_write_plan_local: input.stats.metadata_store.get_write_plan_local_total,
        metadata_get_snapshot: input.stats.metadata_store.get_snapshot_total,
        metadata_scans: input.stats.metadata_store.scan_total,
        metadata_scan_user_strong: input.stats.metadata_store.scan_user_strong_total,
        metadata_scan_write_plan_local: input.stats.metadata_store.scan_write_plan_local_total,
        metadata_scan_snapshot: input.stats.metadata_store.scan_snapshot_total,
        metadata_scan_visited: input.stats.metadata_store.scan_key_visited_total,
        metadata_scan_returned: input.stats.metadata_store.scan_key_returned_total,
        metadata_history_lookups: input.stats.metadata_store.history_lookup_total,
        metadata_current_puts: input.stats.metadata_store.current_put_total,
        metadata_current_deletes: input.stats.metadata_store.current_delete_total,
        metadata_history_writes: input.stats.metadata_store.history_write_total,
        metadata_watch_writes: input.stats.metadata_store.watch_write_total,
        metadata_dedupe_writes: input.stats.metadata_store.dedupe_write_total,
        metadata_commit_prepare_ns: input.stats.metadata_store.commit_prepare_ns_total,
        metadata_atomic_applies: input.stats.metadata_store.atomic_apply_total,
        metadata_atomic_apply_commands: input.stats.metadata_store.atomic_apply_command_total,
        metadata_atomic_apply_max_batch: input.stats.metadata_store.atomic_apply_max_batch,
        metadata_atomic_apply_ns: input.stats.metadata_store.atomic_apply_ns_total,
        metadata_log_segments_archived: input
            .stats
            .metadata_service
            .metadata_log_segments_archived_total,
        metadata_log_entries_archived: input
            .stats
            .metadata_service
            .metadata_log_entries_archived_total,
        metadata_log_archive_bytes: input
            .stats
            .metadata_service
            .metadata_log_archive_bytes_total,
        path_index_lookups,
        path_index_hits,
        path_index_misses: input.stats.metadata_service.path_index_miss_total,
        path_index_stale: input.stats.metadata_service.path_index_stale_total,
        path_index_scan_stale: input.stats.metadata_service.path_index_scan_stale_total,
        path_index_fallback: input.stats.metadata_service.path_index_fallback_total,
        path_index_hit_rate: ratio(path_index_hits, path_index_lookups),
        create_files_batches: input.stats.metadata_service.create_files_batch_total,
        create_files_entries: input.stats.metadata_service.create_files_entry_total,
        create_dirs_batches: input.stats.metadata_service.create_dirs_batch_total,
        create_dirs_entries: input.stats.metadata_service.create_dirs_entry_total,
        read_dir_plus_calls: input.stats.metadata_service.read_dir_plus_total,
        read_dir_plus_entries,
        read_dir_plus_projection_hits,
        read_dir_plus_projection_hit_rate: ratio(
            read_dir_plus_projection_hits,
            read_dir_plus_entries,
        ),
        object_concurrency: input.object_concurrency,
        read_repeats: input.read_repeats,
        block_cache: input.block_cache,
        phase: "n/a",
        checksum: input.checksum,
        shape: input.shape,
        caveat: input.caveat,
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Canonical boundary/tier labels prepended to every L1 service-path row so its
/// output is never silently compared against an L2 mount measurement.
struct BoundaryLabels {
    boundary: &'static str,
    system: &'static str,
    metadata_tier: String,
    object_backend: String,
    cache_state: &'static str,
    concurrency: usize,
    tool: &'static str,
}

fn boundary_labels(config: &Config, workload: &str) -> BoundaryLabels {
    let hot_object_root = hot_object_root_for(config, workload);
    BoundaryLabels {
        boundary: "L1",
        system: "nokv",
        metadata_tier: "nokv-l1-service".to_owned(),
        object_backend: if hot_object_root.is_some() {
            format!(
                "{}+local-hot+put={}",
                object_backend_name(config.object_backend),
                tiered_put_policy_label(tiered_put_policy_for_workload(workload))
            )
        } else {
            object_backend_name(config.object_backend).to_owned()
        },
        cache_state: "n/a",
        concurrency: config.object_concurrency,
        tool: "native",
    }
}

fn csv_header() -> &'static str {
    "boundary,system,metadata_tier,object_backend,cache_state,concurrency,tool,profile,workload,phase,operations,seconds,ops_per_second,throughput_MiB_s,p50_us,p99_us,cost_breakdown,samples_per_second,object_puts,object_put_bytes,object_gets,object_get_bytes,coalesced_gets,coalesced_get_bytes,cache_hits,cache_hit_bytes,cache_hit_rate,prefetch_enqueued,prefetch_dropped,prefetch_completed,prefetch_failed,prefetch_object_gets,prefetch_object_get_bytes,prefetch_cache_hits,prefetch_cache_hit_bytes,read_plan_cache_hits,read_plan_cache_misses,object_writeback_enqueued,object_writeback_inline,object_writeback_completed,object_writeback_failed,object_writeback_staged_bytes,object_writeback_uploaded_bytes,object_writeback_queue_wait_ns,object_writeback_queue_max_wait_ns,object_writeback_upload_ns,object_writeback_upload_max_ns,object_writeback_collect_ns,object_writeback_digest_ns,object_writeback_store_put_ns,object_writeback_cache_put_ns,manifest_chunks,manifest_blocks,data_fabric_planned_blocks,data_fabric_local_nvme_hits,data_fabric_object_fallbacks,data_fabric_object_gets,data_fabric_object_get_bytes,data_fabric_coalesced_ranges,data_fabric_coalesced_range_bytes,data_fabric_cache_hits,data_fabric_cache_hit_bytes,tiered_hot_gets,tiered_hot_hits,tiered_hot_misses,tiered_hot_errors,tiered_cold_gets,tiered_cold_get_bytes,tiered_cold_puts,tiered_cold_put_errors,tiered_hot_puts,tiered_hot_put_errors,tiered_hot_fills,tiered_hot_fill_enqueued,tiered_hot_fill_coalesced,tiered_hot_fill_errors,tiered_cold_deletes,tiered_hot_deletes,tiered_hot_delete_errors,tiered_hot_put_ns,tiered_pending_cold_put_ns,tiered_cold_put_enqueue_ns,local_hot_resident_objects,local_hot_resident_bytes,local_hot_max_bytes,local_hot_evictions,local_hot_eviction_bytes,local_hot_admission_rejections,local_hot_puts,local_hot_put_bytes,local_hot_put_total_ns,local_hot_put_prepare_ns,local_hot_put_write_ns,local_hot_put_sync_ns,local_hot_put_rename_ns,local_hot_put_record_ns,metadata_commits,metadata_dedupe_hits,metadata_predicates,metadata_prefix_empty_predicates,metadata_gets,metadata_get_user_strong,metadata_get_write_plan_local,metadata_get_snapshot,metadata_scans,metadata_scan_user_strong,metadata_scan_write_plan_local,metadata_scan_snapshot,metadata_scan_visited,metadata_scan_returned,metadata_history_lookups,metadata_current_puts,metadata_current_deletes,metadata_history_writes,metadata_watch_writes,metadata_dedupe_writes,metadata_commit_prepare_ns,metadata_atomic_applies,metadata_atomic_apply_commands,metadata_atomic_apply_max_batch,metadata_atomic_apply_ns,metadata_log_segments_archived,metadata_log_entries_archived,metadata_log_archive_bytes,path_index_lookups,path_index_hits,path_index_misses,path_index_stale,path_index_scan_stale,path_index_fallback,path_index_hit_rate,create_files_batches,create_files_entries,create_dirs_batches,create_dirs_entries,read_dir_plus_calls,read_dir_plus_entries,read_dir_plus_projection_hits,read_dir_plus_projection_hit_rate,object_concurrency,read_repeats,block_cache,checksum,shape,caveat"
}

fn csv_row(row: &ResultRow, labels: &BoundaryLabels) -> String {
    [
        labels.boundary.to_owned(),
        labels.system.to_owned(),
        labels.metadata_tier.clone(),
        labels.object_backend.clone(),
        labels.cache_state.to_owned(),
        labels.concurrency.to_string(),
        labels.tool.to_owned(),
        profile_name(row.profile).to_owned(),
        row.workload.to_owned(),
        // L1 service-path rows have no per-op phase or per-op latency; emit the
        // canonical columns so cross-boundary consumers read them by name.
        row.phase.to_owned(),
        row.operations.to_string(),
        format!("{:.6}", row.seconds),
        format!("{:.2}", row.ops_per_second),
        format!("{:.4}", row.mb_per_second),
        "0.00".to_owned(),
        "0.00".to_owned(),
        String::new(),
        format!("{:.2}", row.samples_per_second),
        row.object_puts.to_string(),
        row.object_put_bytes.to_string(),
        row.object_gets.to_string(),
        row.object_get_bytes.to_string(),
        row.coalesced_gets.to_string(),
        row.coalesced_get_bytes.to_string(),
        row.cache_hits.to_string(),
        row.cache_hit_bytes.to_string(),
        format!("{:.4}", row.cache_hit_rate),
        row.prefetch_enqueued.to_string(),
        row.prefetch_dropped.to_string(),
        row.prefetch_completed.to_string(),
        row.prefetch_failed.to_string(),
        row.prefetch_object_gets.to_string(),
        row.prefetch_object_get_bytes.to_string(),
        row.prefetch_cache_hits.to_string(),
        row.prefetch_cache_hit_bytes.to_string(),
        row.read_plan_cache_hits.to_string(),
        row.read_plan_cache_misses.to_string(),
        row.object_writeback_enqueued.to_string(),
        row.object_writeback_inline.to_string(),
        row.object_writeback_completed.to_string(),
        row.object_writeback_failed.to_string(),
        row.object_writeback_staged_bytes.to_string(),
        row.object_writeback_uploaded_bytes.to_string(),
        row.object_writeback_queue_wait_ns.to_string(),
        row.object_writeback_queue_max_wait_ns.to_string(),
        row.object_writeback_upload_ns.to_string(),
        row.object_writeback_upload_max_ns.to_string(),
        row.object_writeback_collect_ns.to_string(),
        row.object_writeback_digest_ns.to_string(),
        row.object_writeback_store_put_ns.to_string(),
        row.object_writeback_cache_put_ns.to_string(),
        row.manifest_chunks.to_string(),
        row.manifest_blocks.to_string(),
        row.data_fabric_planned_blocks.to_string(),
        row.data_fabric_local_nvme_hits.to_string(),
        row.data_fabric_object_fallbacks.to_string(),
        row.data_fabric_object_gets.to_string(),
        row.data_fabric_object_get_bytes.to_string(),
        row.data_fabric_coalesced_ranges.to_string(),
        row.data_fabric_coalesced_range_bytes.to_string(),
        row.data_fabric_cache_hits.to_string(),
        row.data_fabric_cache_hit_bytes.to_string(),
        row.tiered_hot_gets.to_string(),
        row.tiered_hot_hits.to_string(),
        row.tiered_hot_misses.to_string(),
        row.tiered_hot_errors.to_string(),
        row.tiered_cold_gets.to_string(),
        row.tiered_cold_get_bytes.to_string(),
        row.tiered_cold_puts.to_string(),
        row.tiered_cold_put_errors.to_string(),
        row.tiered_hot_puts.to_string(),
        row.tiered_hot_put_errors.to_string(),
        row.tiered_hot_fills.to_string(),
        row.tiered_hot_fill_enqueued.to_string(),
        row.tiered_hot_fill_coalesced.to_string(),
        row.tiered_hot_fill_errors.to_string(),
        row.tiered_cold_deletes.to_string(),
        row.tiered_hot_deletes.to_string(),
        row.tiered_hot_delete_errors.to_string(),
        row.tiered_hot_put_ns.to_string(),
        row.tiered_pending_cold_put_ns.to_string(),
        row.tiered_cold_put_enqueue_ns.to_string(),
        row.local_hot_resident_objects.to_string(),
        row.local_hot_resident_bytes.to_string(),
        row.local_hot_max_bytes.to_string(),
        row.local_hot_evictions.to_string(),
        row.local_hot_eviction_bytes.to_string(),
        row.local_hot_admission_rejections.to_string(),
        row.local_hot_puts.to_string(),
        row.local_hot_put_bytes.to_string(),
        row.local_hot_put_total_ns.to_string(),
        row.local_hot_put_prepare_ns.to_string(),
        row.local_hot_put_write_ns.to_string(),
        row.local_hot_put_sync_ns.to_string(),
        row.local_hot_put_rename_ns.to_string(),
        row.local_hot_put_record_ns.to_string(),
        row.metadata_commits.to_string(),
        row.metadata_dedupe_hits.to_string(),
        row.metadata_predicates.to_string(),
        row.metadata_prefix_empty_predicates.to_string(),
        row.metadata_gets.to_string(),
        row.metadata_get_user_strong.to_string(),
        row.metadata_get_write_plan_local.to_string(),
        row.metadata_get_snapshot.to_string(),
        row.metadata_scans.to_string(),
        row.metadata_scan_user_strong.to_string(),
        row.metadata_scan_write_plan_local.to_string(),
        row.metadata_scan_snapshot.to_string(),
        row.metadata_scan_visited.to_string(),
        row.metadata_scan_returned.to_string(),
        row.metadata_history_lookups.to_string(),
        row.metadata_current_puts.to_string(),
        row.metadata_current_deletes.to_string(),
        row.metadata_history_writes.to_string(),
        row.metadata_watch_writes.to_string(),
        row.metadata_dedupe_writes.to_string(),
        row.metadata_commit_prepare_ns.to_string(),
        row.metadata_atomic_applies.to_string(),
        row.metadata_atomic_apply_commands.to_string(),
        row.metadata_atomic_apply_max_batch.to_string(),
        row.metadata_atomic_apply_ns.to_string(),
        row.metadata_log_segments_archived.to_string(),
        row.metadata_log_entries_archived.to_string(),
        row.metadata_log_archive_bytes.to_string(),
        row.path_index_lookups.to_string(),
        row.path_index_hits.to_string(),
        row.path_index_misses.to_string(),
        row.path_index_stale.to_string(),
        row.path_index_scan_stale.to_string(),
        row.path_index_fallback.to_string(),
        format!("{:.4}", row.path_index_hit_rate),
        row.create_files_batches.to_string(),
        row.create_files_entries.to_string(),
        row.create_dirs_batches.to_string(),
        row.create_dirs_entries.to_string(),
        row.read_dir_plus_calls.to_string(),
        row.read_dir_plus_entries.to_string(),
        row.read_dir_plus_projection_hits.to_string(),
        format!("{:.4}", row.read_dir_plus_projection_hit_rate),
        row.object_concurrency.to_string(),
        row.read_repeats.to_string(),
        row.block_cache.to_string(),
        row.checksum.to_string(),
        csv_field(&row.shape),
        csv_field(&row.caveat),
    ]
    .join(",")
}

fn tiered_object_stats_delta(
    before: TieredObjectStoreStats,
    after: TieredObjectStoreStats,
) -> TieredObjectStoreStats {
    TieredObjectStoreStats {
        hot_gets: after.hot_gets.saturating_sub(before.hot_gets),
        hot_hits: after.hot_hits.saturating_sub(before.hot_hits),
        hot_misses: after.hot_misses.saturating_sub(before.hot_misses),
        hot_errors: after.hot_errors.saturating_sub(before.hot_errors),
        cold_gets: after.cold_gets.saturating_sub(before.cold_gets),
        cold_get_bytes: after.cold_get_bytes.saturating_sub(before.cold_get_bytes),
        cold_puts: after.cold_puts.saturating_sub(before.cold_puts),
        cold_put_errors: after.cold_put_errors.saturating_sub(before.cold_put_errors),
        hot_puts: after.hot_puts.saturating_sub(before.hot_puts),
        hot_put_errors: after.hot_put_errors.saturating_sub(before.hot_put_errors),
        hot_fills: after.hot_fills.saturating_sub(before.hot_fills),
        hot_fill_enqueued: after
            .hot_fill_enqueued
            .saturating_sub(before.hot_fill_enqueued),
        hot_fill_coalesced: after
            .hot_fill_coalesced
            .saturating_sub(before.hot_fill_coalesced),
        hot_fill_errors: after.hot_fill_errors.saturating_sub(before.hot_fill_errors),
        cold_deletes: after.cold_deletes.saturating_sub(before.cold_deletes),
        hot_deletes: after.hot_deletes.saturating_sub(before.hot_deletes),
        hot_delete_errors: after
            .hot_delete_errors
            .saturating_sub(before.hot_delete_errors),
        hot_put_ns: after.hot_put_ns.saturating_sub(before.hot_put_ns),
        pending_cold_put_ns: after
            .pending_cold_put_ns
            .saturating_sub(before.pending_cold_put_ns),
        cold_put_enqueue_ns: after
            .cold_put_enqueue_ns
            .saturating_sub(before.cold_put_enqueue_ns),
    }
}

fn local_hot_stats_delta(
    before: LocalObjectStoreStats,
    after: LocalObjectStoreStats,
) -> LocalObjectStoreStats {
    LocalObjectStoreStats {
        resident_objects: after.resident_objects,
        resident_bytes: after.resident_bytes,
        max_bytes: after.max_bytes,
        evictions: after.evictions.saturating_sub(before.evictions),
        eviction_bytes: after.eviction_bytes.saturating_sub(before.eviction_bytes),
        admission_rejections: after
            .admission_rejections
            .saturating_sub(before.admission_rejections),
        puts: after.puts.saturating_sub(before.puts),
        put_bytes: after.put_bytes.saturating_sub(before.put_bytes),
        put_total_ns: after.put_total_ns.saturating_sub(before.put_total_ns),
        put_prepare_ns: after.put_prepare_ns.saturating_sub(before.put_prepare_ns),
        put_write_ns: after.put_write_ns.saturating_sub(before.put_write_ns),
        put_sync_ns: after.put_sync_ns.saturating_sub(before.put_sync_ns),
        put_rename_ns: after.put_rename_ns.saturating_sub(before.put_rename_ns),
        put_record_ns: after.put_record_ns.saturating_sub(before.put_record_ns),
    }
}

fn stats_delta(before: BenchStats, after: BenchStats) -> BenchStats {
    let atomic_apply_delta = after
        .metadata_store
        .atomic_apply_total
        .saturating_sub(before.metadata_store.atomic_apply_total);
    let object_writeback_delta = after
        .object
        .object_writeback_enqueued
        .saturating_sub(before.object.object_writeback_enqueued)
        .saturating_add(
            after
                .object
                .object_writeback_inline
                .saturating_sub(before.object.object_writeback_inline),
        );
    BenchStats {
        object: ObjectTransferStats {
            object_puts: after
                .object
                .object_puts
                .saturating_sub(before.object.object_puts),
            object_put_bytes: after
                .object
                .object_put_bytes
                .saturating_sub(before.object.object_put_bytes),
            object_gets: after
                .object
                .object_gets
                .saturating_sub(before.object.object_gets),
            object_get_bytes: after
                .object
                .object_get_bytes
                .saturating_sub(before.object.object_get_bytes),
            coalesced_gets: after
                .object
                .coalesced_gets
                .saturating_sub(before.object.coalesced_gets),
            coalesced_get_bytes: after
                .object
                .coalesced_get_bytes
                .saturating_sub(before.object.coalesced_get_bytes),
            cache_hits: after
                .object
                .cache_hits
                .saturating_sub(before.object.cache_hits),
            cache_hit_bytes: after
                .object
                .cache_hit_bytes
                .saturating_sub(before.object.cache_hit_bytes),
            prefetch_enqueued: after
                .object
                .prefetch_enqueued
                .saturating_sub(before.object.prefetch_enqueued),
            prefetch_dropped: after
                .object
                .prefetch_dropped
                .saturating_sub(before.object.prefetch_dropped),
            prefetch_completed: after
                .object
                .prefetch_completed
                .saturating_sub(before.object.prefetch_completed),
            prefetch_failed: after
                .object
                .prefetch_failed
                .saturating_sub(before.object.prefetch_failed),
            prefetch_object_gets: after
                .object
                .prefetch_object_gets
                .saturating_sub(before.object.prefetch_object_gets),
            prefetch_object_get_bytes: after
                .object
                .prefetch_object_get_bytes
                .saturating_sub(before.object.prefetch_object_get_bytes),
            prefetch_cache_hits: after
                .object
                .prefetch_cache_hits
                .saturating_sub(before.object.prefetch_cache_hits),
            prefetch_cache_hit_bytes: after
                .object
                .prefetch_cache_hit_bytes
                .saturating_sub(before.object.prefetch_cache_hit_bytes),
            read_plan_cache_hits: after
                .object
                .read_plan_cache_hits
                .saturating_sub(before.object.read_plan_cache_hits),
            read_plan_cache_misses: after
                .object
                .read_plan_cache_misses
                .saturating_sub(before.object.read_plan_cache_misses),
            object_writeback_enqueued: after
                .object
                .object_writeback_enqueued
                .saturating_sub(before.object.object_writeback_enqueued),
            object_writeback_inline: after
                .object
                .object_writeback_inline
                .saturating_sub(before.object.object_writeback_inline),
            object_writeback_completed: after
                .object
                .object_writeback_completed
                .saturating_sub(before.object.object_writeback_completed),
            object_writeback_failed: after
                .object
                .object_writeback_failed
                .saturating_sub(before.object.object_writeback_failed),
            object_writeback_staged_bytes: after
                .object
                .object_writeback_staged_bytes
                .saturating_sub(before.object.object_writeback_staged_bytes),
            object_writeback_uploaded_bytes: after
                .object
                .object_writeback_uploaded_bytes
                .saturating_sub(before.object.object_writeback_uploaded_bytes),
            object_writeback_queue_wait_ns: after
                .object
                .object_writeback_queue_wait_ns
                .saturating_sub(before.object.object_writeback_queue_wait_ns),
            object_writeback_queue_max_wait_ns: if object_writeback_delta == 0 {
                0
            } else {
                after.object.object_writeback_queue_max_wait_ns
            },
            object_writeback_upload_ns: after
                .object
                .object_writeback_upload_ns
                .saturating_sub(before.object.object_writeback_upload_ns),
            object_writeback_upload_max_ns: if object_writeback_delta == 0 {
                0
            } else {
                after.object.object_writeback_upload_max_ns
            },
            object_writeback_collect_ns: after
                .object
                .object_writeback_collect_ns
                .saturating_sub(before.object.object_writeback_collect_ns),
            object_writeback_digest_ns: after
                .object
                .object_writeback_digest_ns
                .saturating_sub(before.object.object_writeback_digest_ns),
            object_writeback_store_put_ns: after
                .object
                .object_writeback_store_put_ns
                .saturating_sub(before.object.object_writeback_store_put_ns),
            object_writeback_cache_put_ns: after
                .object
                .object_writeback_cache_put_ns
                .saturating_sub(before.object.object_writeback_cache_put_ns),
            manifest_chunks: after
                .object
                .manifest_chunks
                .saturating_sub(before.object.manifest_chunks),
            manifest_blocks: after
                .object
                .manifest_blocks
                .saturating_sub(before.object.manifest_blocks),
        },
        data_fabric: DataFabricReadStats {
            planned_blocks: after
                .data_fabric
                .planned_blocks
                .saturating_sub(before.data_fabric.planned_blocks),
            local_nvme_hits: after
                .data_fabric
                .local_nvme_hits
                .saturating_sub(before.data_fabric.local_nvme_hits),
            object_fallbacks: after
                .data_fabric
                .object_fallbacks
                .saturating_sub(before.data_fabric.object_fallbacks),
            object_gets: after
                .data_fabric
                .object_gets
                .saturating_sub(before.data_fabric.object_gets),
            object_get_bytes: after
                .data_fabric
                .object_get_bytes
                .saturating_sub(before.data_fabric.object_get_bytes),
            coalesced_ranges: after
                .data_fabric
                .coalesced_ranges
                .saturating_sub(before.data_fabric.coalesced_ranges),
            coalesced_range_bytes: after
                .data_fabric
                .coalesced_range_bytes
                .saturating_sub(before.data_fabric.coalesced_range_bytes),
            cache_hits: after
                .data_fabric
                .cache_hits
                .saturating_sub(before.data_fabric.cache_hits),
            cache_hit_bytes: after
                .data_fabric
                .cache_hit_bytes
                .saturating_sub(before.data_fabric.cache_hit_bytes),
        },
        tiered_object: tiered_object_stats_delta(before.tiered_object, after.tiered_object),
        local_hot: local_hot_stats_delta(before.local_hot, after.local_hot),
        metadata_store: MetadataStoreStats {
            get_total: after
                .metadata_store
                .get_total
                .saturating_sub(before.metadata_store.get_total),
            get_user_strong_total: after
                .metadata_store
                .get_user_strong_total
                .saturating_sub(before.metadata_store.get_user_strong_total),
            get_write_plan_local_total: after
                .metadata_store
                .get_write_plan_local_total
                .saturating_sub(before.metadata_store.get_write_plan_local_total),
            get_snapshot_total: after
                .metadata_store
                .get_snapshot_total
                .saturating_sub(before.metadata_store.get_snapshot_total),
            scan_total: after
                .metadata_store
                .scan_total
                .saturating_sub(before.metadata_store.scan_total),
            scan_user_strong_total: after
                .metadata_store
                .scan_user_strong_total
                .saturating_sub(before.metadata_store.scan_user_strong_total),
            scan_write_plan_local_total: after
                .metadata_store
                .scan_write_plan_local_total
                .saturating_sub(before.metadata_store.scan_write_plan_local_total),
            scan_snapshot_total: after
                .metadata_store
                .scan_snapshot_total
                .saturating_sub(before.metadata_store.scan_snapshot_total),
            scan_cache_hit_total: after
                .metadata_store
                .scan_cache_hit_total
                .saturating_sub(before.metadata_store.scan_cache_hit_total),
            scan_key_visited_total: after
                .metadata_store
                .scan_key_visited_total
                .saturating_sub(before.metadata_store.scan_key_visited_total),
            scan_key_returned_total: after
                .metadata_store
                .scan_key_returned_total
                .saturating_sub(before.metadata_store.scan_key_returned_total),
            history_lookup_total: after
                .metadata_store
                .history_lookup_total
                .saturating_sub(before.metadata_store.history_lookup_total),
            active_snapshot_pin_total: after.metadata_store.active_snapshot_pin_total,
            commit_total: after
                .metadata_store
                .commit_total
                .saturating_sub(before.metadata_store.commit_total),
            dedupe_hit_total: after
                .metadata_store
                .dedupe_hit_total
                .saturating_sub(before.metadata_store.dedupe_hit_total),
            predicate_total: after
                .metadata_store
                .predicate_total
                .saturating_sub(before.metadata_store.predicate_total),
            prefix_empty_predicate_total: after
                .metadata_store
                .prefix_empty_predicate_total
                .saturating_sub(before.metadata_store.prefix_empty_predicate_total),
            current_put_total: after
                .metadata_store
                .current_put_total
                .saturating_sub(before.metadata_store.current_put_total),
            current_delete_total: after
                .metadata_store
                .current_delete_total
                .saturating_sub(before.metadata_store.current_delete_total),
            history_write_total: after
                .metadata_store
                .history_write_total
                .saturating_sub(before.metadata_store.history_write_total),
            watch_write_total: after
                .metadata_store
                .watch_write_total
                .saturating_sub(before.metadata_store.watch_write_total),
            dedupe_write_total: after
                .metadata_store
                .dedupe_write_total
                .saturating_sub(before.metadata_store.dedupe_write_total),
            commit_prepare_ns_total: after
                .metadata_store
                .commit_prepare_ns_total
                .saturating_sub(before.metadata_store.commit_prepare_ns_total),
            atomic_apply_total: atomic_apply_delta,
            atomic_apply_command_total: after
                .metadata_store
                .atomic_apply_command_total
                .saturating_sub(before.metadata_store.atomic_apply_command_total),
            atomic_apply_max_batch: if atomic_apply_delta == 0 {
                0
            } else {
                after.metadata_store.atomic_apply_max_batch
            },
            atomic_apply_ns_total: after
                .metadata_store
                .atomic_apply_ns_total
                .saturating_sub(before.metadata_store.atomic_apply_ns_total),
        },
        metadata_service: MetadataServiceStats {
            path_index_lookup_total: after
                .metadata_service
                .path_index_lookup_total
                .saturating_sub(before.metadata_service.path_index_lookup_total),
            path_index_hit_total: after
                .metadata_service
                .path_index_hit_total
                .saturating_sub(before.metadata_service.path_index_hit_total),
            path_index_miss_total: after
                .metadata_service
                .path_index_miss_total
                .saturating_sub(before.metadata_service.path_index_miss_total),
            path_index_stale_total: after
                .metadata_service
                .path_index_stale_total
                .saturating_sub(before.metadata_service.path_index_stale_total),
            path_index_scan_stale_total: after
                .metadata_service
                .path_index_scan_stale_total
                .saturating_sub(before.metadata_service.path_index_scan_stale_total),
            path_index_fallback_total: after
                .metadata_service
                .path_index_fallback_total
                .saturating_sub(before.metadata_service.path_index_fallback_total),
            create_files_batch_total: after
                .metadata_service
                .create_files_batch_total
                .saturating_sub(before.metadata_service.create_files_batch_total),
            create_files_entry_total: after
                .metadata_service
                .create_files_entry_total
                .saturating_sub(before.metadata_service.create_files_entry_total),
            create_dirs_batch_total: after
                .metadata_service
                .create_dirs_batch_total
                .saturating_sub(before.metadata_service.create_dirs_batch_total),
            create_dirs_entry_total: after
                .metadata_service
                .create_dirs_entry_total
                .saturating_sub(before.metadata_service.create_dirs_entry_total),
            read_dir_plus_total: after
                .metadata_service
                .read_dir_plus_total
                .saturating_sub(before.metadata_service.read_dir_plus_total),
            read_dir_plus_entry_total: after
                .metadata_service
                .read_dir_plus_entry_total
                .saturating_sub(before.metadata_service.read_dir_plus_entry_total),
            read_dir_plus_projection_hit_total: after
                .metadata_service
                .read_dir_plus_projection_hit_total
                .saturating_sub(before.metadata_service.read_dir_plus_projection_hit_total),
            metadata_log_segments_archived_total: after
                .metadata_service
                .metadata_log_segments_archived_total
                .saturating_sub(before.metadata_service.metadata_log_segments_archived_total),
            metadata_log_entries_archived_total: after
                .metadata_service
                .metadata_log_entries_archived_total
                .saturating_sub(before.metadata_service.metadata_log_entries_archived_total),
            metadata_log_archive_bytes_total: after
                .metadata_service
                .metadata_log_archive_bytes_total
                .saturating_sub(before.metadata_service.metadata_log_archive_bytes_total),
            restore_to_fork_requests_total: after
                .metadata_service
                .restore_to_fork_requests_total
                .saturating_sub(before.metadata_service.restore_to_fork_requests_total),
            restore_to_fork_success_total: after
                .metadata_service
                .restore_to_fork_success_total
                .saturating_sub(before.metadata_service.restore_to_fork_success_total),
            restore_to_fork_failure_total: after
                .metadata_service
                .restore_to_fork_failure_total
                .saturating_sub(before.metadata_service.restore_to_fork_failure_total),
            restore_to_fork_elapsed_ns_total: after
                .metadata_service
                .restore_to_fork_elapsed_ns_total
                .saturating_sub(before.metadata_service.restore_to_fork_elapsed_ns_total),
            restore_to_fork_elapsed_ns_max: if after.metadata_service.restore_to_fork_requests_total
                == before.metadata_service.restore_to_fork_requests_total
            {
                0
            } else {
                after.metadata_service.restore_to_fork_elapsed_ns_max
            },
        },
    }
}

fn metadata_only_caveat(config: &Config) -> String {
    format!(
        "metadata-only on Holt metadata service, object_backend={}, {}",
        object_backend_name(config.object_backend),
        local_metadata_caveat()
    )
}

fn metadata_durability_phase(mode: MetadataDurabilityMode) -> &'static str {
    match mode {
        MetadataDurabilityMode::LocalOnly => "local-only",
        MetadataDurabilityMode::SyncSharedLog => "sync-shared-log",
    }
}

fn metadata_durability_caveat(config: &Config, mode: MetadataDurabilityMode) -> String {
    match mode {
        MetadataDurabilityMode::LocalOnly => {
            "single Holt metadata server; ACK after local metadata commit; timed op is batch create across dirs".to_owned()
        }
        MetadataDurabilityMode::SyncSharedLog => format!(
            "single Holt metadata server plus in-memory control owner; ACK after local metadata commit, grouped shared-log segment archive, and LogRef publish; object_backend={}; no etcd quorum",
            object_backend_name(config.object_backend)
        ),
    }
}

fn object_caveat(config: &Config, path: &str) -> String {
    let cache = if config.block_cache {
        "cache=on"
    } else {
        "cache=off"
    };
    match config.object_backend {
        ObjectBackendKind::RustFs => {
            format!(
                "{path}, Holt metadata service, RustFS S3-compatible backend over configured endpoint, object_concurrency={}, read_repeats={}, {cache}, {}",
                config.object_concurrency,
                config.read_repeats,
                local_metadata_caveat()
            )
        }
        ObjectBackendKind::S3 => {
            format!(
                "{path}, Holt metadata service, generic S3-compatible backend over configured endpoint, object_concurrency={}, read_repeats={}, {cache}, {}",
                config.object_concurrency,
                config.read_repeats,
                local_metadata_caveat()
            )
        }
    }
}

fn local_metadata_caveat() -> &'static str {
    "single Holt metadata server"
}

fn client_for(config: &Config, workload: &str) -> Result<Box<dyn BenchClient>, BenchError> {
    service_client_for(config, workload)
}

fn service_client_for(config: &Config, workload: &str) -> Result<Box<dyn BenchClient>, BenchError> {
    service_client_for_with_metadata_durability(config, workload, MetadataDurabilityMode::LocalOnly)
}

fn service_client_for_with_metadata_durability(
    config: &Config,
    workload: &str,
    durability: MetadataDurabilityMode,
) -> Result<Box<dyn BenchClient>, BenchError> {
    let meta = config.root.join(workload).join("meta");
    let object = object_config_for(config, workload);
    let objects = object.clone().open().map_err(from_client)?;
    let client_objects = objects.clone();
    let listener = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let bind = listener.local_addr().map_err(from_io)?;
    let options = ServerOptions {
        bind,
        mount: MountId::new(1).expect("mount id is non-zero"),
        meta_path: meta,
        metadata_checkpoint_archive_prefix: None,
        object,
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
        object_gc: ObjectGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
            read_lease_grace: ObjectGcOptions::default().read_lease_grace,
        },
        history_gc: HistoryGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
        },
        control: None,
    };
    let server = match durability {
        MetadataDurabilityMode::LocalOnly => Server::open(options),
        MetadataDurabilityMode::SyncSharedLog => Server::open_with_control(
            options,
            Arc::new(InMemoryControlStore::new()),
            vec![
                ServerShardOwnerOptions::fresh("mount-1:/", format!("bench-{workload}"))
                    .with_renewal(None)
                    .with_shared_log(Some(ServerSharedLogOptions::new(format!(
                        "metadata/bench/{workload}/shared-log"
                    )))),
            ],
        ),
    }
    .map_err(from_client)?;
    thread::spawn(move || {
        let _ = server.serve(listener);
    });
    let mut client = NoKvFsClient::connect(bind, objects);
    client.set_block_cache_enabled(config.block_cache);
    Ok(Box::new(ServiceBenchClient {
        client,
        objects: client_objects,
        stats_addr: bind,
        include_server_object_stats: durability == MetadataDurabilityMode::SyncSharedLog,
    }))
}

fn fetch_server_stats(address: SocketAddr) -> Result<BenchStats, BenchError> {
    let mut stream = TcpStream::connect(address).map_err(from_io)?;
    stream
        .write_all(b"GET /stats HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(from_io)?;
    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(from_io)?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| BenchError::Client("stats response missing body".to_owned()))?;
    Ok(BenchStats {
        object: ObjectTransferStats {
            object_puts: json_u64(body, "object_puts")?,
            object_put_bytes: json_u64(body, "object_put_bytes")?,
            object_gets: json_u64(body, "object_gets")?,
            object_get_bytes: json_u64(body, "object_get_bytes")?,
            coalesced_gets: json_u64(body, "coalesced_gets")?,
            coalesced_get_bytes: json_u64(body, "coalesced_get_bytes")?,
            cache_hits: json_u64(body, "cache_hits")?,
            cache_hit_bytes: json_u64(body, "cache_hit_bytes")?,
            prefetch_enqueued: json_u64(body, "prefetch_enqueued")?,
            prefetch_dropped: json_u64(body, "prefetch_dropped")?,
            prefetch_completed: json_u64(body, "prefetch_completed")?,
            prefetch_failed: json_u64(body, "prefetch_failed")?,
            prefetch_object_gets: json_u64(body, "prefetch_object_gets")?,
            prefetch_object_get_bytes: json_u64(body, "prefetch_object_get_bytes")?,
            prefetch_cache_hits: json_u64(body, "prefetch_cache_hits")?,
            prefetch_cache_hit_bytes: json_u64(body, "prefetch_cache_hit_bytes")?,
            read_plan_cache_hits: json_u64(body, "read_plan_cache_hits")?,
            read_plan_cache_misses: json_u64(body, "read_plan_cache_misses")?,
            object_writeback_enqueued: json_u64_or_zero(body, "object_writeback_enqueued"),
            object_writeback_inline: json_u64_or_zero(body, "object_writeback_inline"),
            object_writeback_completed: json_u64_or_zero(body, "object_writeback_completed"),
            object_writeback_failed: json_u64_or_zero(body, "object_writeback_failed"),
            object_writeback_staged_bytes: json_u64_or_zero(body, "object_writeback_staged_bytes"),
            object_writeback_uploaded_bytes: json_u64_or_zero(
                body,
                "object_writeback_uploaded_bytes",
            ),
            object_writeback_queue_wait_ns: json_u64_or_zero(
                body,
                "object_writeback_queue_wait_ns",
            ),
            object_writeback_queue_max_wait_ns: json_u64_or_zero(
                body,
                "object_writeback_queue_max_wait_ns",
            ),
            object_writeback_upload_ns: json_u64_or_zero(body, "object_writeback_upload_ns"),
            object_writeback_upload_max_ns: json_u64_or_zero(
                body,
                "object_writeback_upload_max_ns",
            ),
            object_writeback_collect_ns: json_u64_or_zero(body, "object_writeback_collect_ns"),
            object_writeback_digest_ns: json_u64_or_zero(body, "object_writeback_digest_ns"),
            object_writeback_store_put_ns: json_u64_or_zero(body, "object_writeback_store_put_ns"),
            object_writeback_cache_put_ns: json_u64_or_zero(body, "object_writeback_cache_put_ns"),
            manifest_chunks: json_u64(body, "manifest_chunks")?,
            manifest_blocks: json_u64(body, "manifest_blocks")?,
        },
        data_fabric: DataFabricReadStats::default(),
        tiered_object: TieredObjectStoreStats::default(),
        local_hot: LocalObjectStoreStats::default(),
        metadata_store: MetadataStoreStats {
            get_total: json_u64(body, "get_total")?,
            get_user_strong_total: json_u64(body, "get_user_strong_total")?,
            get_write_plan_local_total: json_u64(body, "get_write_plan_local_total")?,
            get_snapshot_total: json_u64(body, "get_snapshot_total")?,
            scan_total: json_u64(body, "scan_total")?,
            scan_user_strong_total: json_u64(body, "scan_user_strong_total")?,
            scan_write_plan_local_total: json_u64(body, "scan_write_plan_local_total")?,
            scan_snapshot_total: json_u64(body, "scan_snapshot_total")?,
            scan_cache_hit_total: json_u64(body, "scan_cache_hit_total")?,
            scan_key_visited_total: json_u64(body, "scan_key_visited_total")?,
            scan_key_returned_total: json_u64(body, "scan_key_returned_total")?,
            history_lookup_total: json_u64(body, "history_lookup_total")?,
            active_snapshot_pin_total: json_u64(body, "active_snapshot_pin_total")?,
            commit_total: json_u64(body, "commit_total")?,
            dedupe_hit_total: json_u64(body, "dedupe_hit_total")?,
            predicate_total: json_u64(body, "predicate_total")?,
            prefix_empty_predicate_total: json_u64(body, "prefix_empty_predicate_total")?,
            current_put_total: json_u64(body, "current_put_total")?,
            current_delete_total: json_u64(body, "current_delete_total")?,
            history_write_total: json_u64(body, "history_write_total")?,
            watch_write_total: json_u64(body, "watch_write_total")?,
            dedupe_write_total: json_u64(body, "dedupe_write_total")?,
            commit_prepare_ns_total: json_u64(body, "commit_prepare_ns_total")?,
            atomic_apply_total: json_u64(body, "atomic_apply_total")?,
            atomic_apply_command_total: json_u64(body, "atomic_apply_command_total")?,
            atomic_apply_max_batch: json_u64(body, "atomic_apply_max_batch")?,
            atomic_apply_ns_total: json_u64(body, "atomic_apply_ns_total")?,
        },
        metadata_service: MetadataServiceStats {
            path_index_lookup_total: json_u64(body, "path_index_lookup_total")?,
            path_index_hit_total: json_u64(body, "path_index_hit_total")?,
            path_index_miss_total: json_u64(body, "path_index_miss_total")?,
            path_index_stale_total: json_u64(body, "path_index_stale_total")?,
            path_index_scan_stale_total: json_u64(body, "path_index_scan_stale_total")?,
            path_index_fallback_total: json_u64(body, "path_index_fallback_total")?,
            create_files_batch_total: json_u64(body, "create_files_batch_total")?,
            create_files_entry_total: json_u64(body, "create_files_entry_total")?,
            create_dirs_batch_total: json_u64(body, "create_dirs_batch_total")?,
            create_dirs_entry_total: json_u64(body, "create_dirs_entry_total")?,
            read_dir_plus_total: json_u64(body, "read_dir_plus_total")?,
            read_dir_plus_entry_total: json_u64(body, "read_dir_plus_entry_total")?,
            read_dir_plus_projection_hit_total: json_u64(
                body,
                "read_dir_plus_projection_hit_total",
            )?,
            metadata_log_segments_archived_total: json_u64(
                body,
                "metadata_log_segments_archived_total",
            )?,
            metadata_log_entries_archived_total: json_u64(
                body,
                "metadata_log_entries_archived_total",
            )?,
            metadata_log_archive_bytes_total: json_u64(body, "metadata_log_archive_bytes_total")?,
            restore_to_fork_requests_total: json_u64(body, "restore_to_fork_requests_total")?,
            restore_to_fork_success_total: json_u64(body, "restore_to_fork_success_total")?,
            restore_to_fork_failure_total: json_u64(body, "restore_to_fork_failure_total")?,
            restore_to_fork_elapsed_ns_total: json_u64(body, "restore_to_fork_elapsed_ns_total")?,
            restore_to_fork_elapsed_ns_max: json_u64(body, "restore_to_fork_elapsed_ns_max")?,
        },
    })
}

fn json_u64(body: &str, key: &str) -> Result<u64, BenchError> {
    let needle = format!("\"{key}\":");
    let start = body
        .find(&needle)
        .ok_or_else(|| BenchError::Client(format!("stats response missing {key}")))?
        + needle.len();
    let digits = body[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return Err(BenchError::Client(format!(
            "stats response has non-numeric {key}"
        )));
    }
    digits
        .parse()
        .map_err(|err| BenchError::Client(format!("invalid stats value for {key}: {err}")))
}

fn json_u64_or_zero(body: &str, key: &str) -> u64 {
    json_u64(body, key).unwrap_or(0)
}

fn artifact_metadata(producer: &str, manifest_id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: producer.to_owned(),
        digest_uri: format!("sha256:{}", stable_id_hash(manifest_id)),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: DEFAULT_MODE_FILE,
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
    }
}

fn body_generation(entry: &DentryWithAttr) -> u64 {
    entry
        .body
        .as_ref()
        .map(|body| body.generation)
        .unwrap_or(entry.attr.generation)
}

fn stable_id_hash(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!("{hash:016x}")
}

fn checkpoint_payload(seed: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| ((seed + offset) % 251) as u8)
        .collect()
}

fn dataset_payload(shard: usize, file: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| ((shard * 31 + file * 17 + offset) % 251) as u8)
        .collect()
}

fn run_parallel<F>(total: usize, concurrency: usize, worker: F) -> Result<u64, BenchError>
where
    F: Fn(usize) -> Result<u64, BenchError> + Sync,
{
    if total == 0 {
        return Ok(0);
    }
    let workers = concurrency.max(1).min(total);
    let next = AtomicUsize::new(0);
    let checksum = AtomicU64::new(0);
    let error = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                if error.lock().expect("error lock").is_some() {
                    break;
                }
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    break;
                }
                match worker(index) {
                    Ok(value) => {
                        checksum.fetch_add(value, Ordering::Relaxed);
                    }
                    Err(err) => {
                        *error.lock().expect("error lock") = Some(err);
                        break;
                    }
                }
            });
        }
    });
    if let Some(err) = error.into_inner().expect("error lock") {
        return Err(err);
    }
    Ok(checksum.load(Ordering::Relaxed))
}

fn parse(args: Vec<String>) -> Result<Config, BenchError> {
    let mut profile = Profile::Smoke;
    let mut workload = Workload::All;
    let mut root = default_root();
    let mut object_backend = ObjectBackendKind::RustFs;
    let mut s3 = S3ObjectStoreOptions::new("");
    let mut object_concurrency = 1_usize;
    let mut hot_object_root = None;
    let mut hot_object_max_bytes = None;
    let mut hot_fill_mode = HotFillMode::Inline;
    let mut read_repeats = 1_usize;
    let mut range_stride = 1_usize;
    let mut range_coalesce_gap_bytes = 0_u64;
    let mut block_cache = true;
    let mut checkpoint_bytes = None;
    let mut sample_bytes = None;
    let mut keep = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--profile" => {
                index += 1;
                profile = parse_profile(value(&args, index, "--profile")?)?;
            }
            "--workload" => {
                index += 1;
                workload = parse_workload(value(&args, index, "--workload")?)?;
            }
            "--root" => {
                index += 1;
                root = PathBuf::from(value(&args, index, "--root")?);
            }
            "--object-backend" => {
                index += 1;
                object_backend = parse_object_backend(value(&args, index, "--object-backend")?)?;
            }
            "--s3-bucket" => {
                index += 1;
                s3.bucket = value(&args, index, "--s3-bucket")?.to_owned();
            }
            "--s3-endpoint" => {
                index += 1;
                s3.endpoint = Some(value(&args, index, "--s3-endpoint")?.to_owned());
            }
            "--s3-region" => {
                index += 1;
                s3.region = value(&args, index, "--s3-region")?.to_owned();
            }
            "--s3-access-key-id" => {
                index += 1;
                s3.access_key_id = Some(value(&args, index, "--s3-access-key-id")?.to_owned());
            }
            "--s3-secret-access-key" => {
                index += 1;
                s3.secret_access_key =
                    Some(value(&args, index, "--s3-secret-access-key")?.to_owned());
            }
            "--s3-session-token" => {
                index += 1;
                s3.session_token = Some(value(&args, index, "--s3-session-token")?.to_owned());
            }
            "--s3-root" => {
                index += 1;
                s3.root = value(&args, index, "--s3-root")?.to_owned();
            }
            "--s3-virtual-host-style" => {
                s3.virtual_host_style = true;
            }
            "--s3-skip-signature" => {
                s3.skip_signature = true;
            }
            "--object-concurrency" => {
                index += 1;
                object_concurrency = parse_positive_usize(
                    value(&args, index, "--object-concurrency")?,
                    "--object-concurrency",
                )?;
            }
            "--hot-object-root" => {
                index += 1;
                hot_object_root = Some(PathBuf::from(value(&args, index, "--hot-object-root")?));
            }
            "--hot-object-max-bytes" => {
                index += 1;
                hot_object_max_bytes = Some(parse_positive_u64(
                    value(&args, index, "--hot-object-max-bytes")?,
                    "--hot-object-max-bytes",
                )?);
            }
            "--hot-fill-mode" => {
                index += 1;
                hot_fill_mode = parse_hot_fill_mode(value(&args, index, "--hot-fill-mode")?)?;
            }
            "--checkpoint-bytes" => {
                index += 1;
                checkpoint_bytes = Some(parse_positive_usize(
                    value(&args, index, "--checkpoint-bytes")?,
                    "--checkpoint-bytes",
                )?);
            }
            "--sample-bytes" => {
                index += 1;
                sample_bytes = Some(parse_positive_usize(
                    value(&args, index, "--sample-bytes")?,
                    "--sample-bytes",
                )?);
            }
            "--read-repeats" => {
                index += 1;
                read_repeats =
                    parse_positive_usize(value(&args, index, "--read-repeats")?, "--read-repeats")?;
            }
            "--range-stride" => {
                index += 1;
                range_stride =
                    parse_positive_usize(value(&args, index, "--range-stride")?, "--range-stride")?;
            }
            "--range-coalesce-gap-bytes" => {
                index += 1;
                range_coalesce_gap_bytes = parse_u64(
                    value(&args, index, "--range-coalesce-gap-bytes")?,
                    "--range-coalesce-gap-bytes",
                )?;
            }
            "--block-cache" => {
                index += 1;
                block_cache = parse_block_cache(value(&args, index, "--block-cache")?)?;
            }
            "--keep" => keep = true,
            "--help" | "-h" => {
                return Err(BenchError::UnknownOption("--help".to_owned()));
            }
            option => return Err(BenchError::UnknownOption(option.to_owned())),
        }
        index += 1;
    }
    if object_backend == ObjectBackendKind::RustFs {
        apply_rustfs_defaults(&mut s3);
    }
    Ok(Config {
        profile,
        workload,
        root,
        object_backend,
        s3,
        object_concurrency,
        hot_object_root,
        hot_object_max_bytes,
        hot_fill_mode,
        read_repeats,
        range_stride,
        range_coalesce_gap_bytes,
        block_cache,
        checkpoint_bytes,
        sample_bytes,
        keep,
    })
}

fn object_config_for(config: &Config, workload: &str) -> ObjectStoreConfig {
    let mut options = config.s3.clone();
    if config.object_backend == ObjectBackendKind::RustFs {
        apply_rustfs_defaults(&mut options);
    }
    if options.root == "/" {
        options.root = format!("/nokv-bench/{workload}");
    }
    if let Some(hot_root) = hot_object_root_for(config, workload) {
        let hot = match config.hot_object_max_bytes {
            Some(max_bytes) => LocalObjectStoreOptions::new(hot_root).with_max_bytes(max_bytes),
            None => LocalObjectStoreOptions::new(hot_root),
        };
        ObjectStoreConfig::tiered_local_with_options(
            hot,
            options,
            TieredObjectStoreOptions {
                put_policy: tiered_put_policy_for_workload(workload),
                populate_hot_on_get: true,
                hot_fill_mode: config.hot_fill_mode,
                pending_cold_put_root: None,
            },
        )
    } else {
        ObjectStoreConfig::s3(options)
    }
}

fn tiered_put_policy_for_workload(workload: &str) -> TieredPutPolicy {
    match workload {
        "native-layout-read" | "ai-dataset-batch-read" | "ai-shard-range-read" => {
            TieredPutPolicy::ColdOnly
        }
        _ => TieredPutPolicy::HotThenBackgroundCold,
    }
}

fn tiered_put_policy_label(policy: TieredPutPolicy) -> &'static str {
    match policy {
        TieredPutPolicy::ColdOnly => "cold-only",
        TieredPutPolicy::ColdThenHot => "cold-then-hot",
        TieredPutPolicy::HotThenBackgroundCold => "hot-background",
    }
}

fn hot_object_root_for(config: &Config, workload: &str) -> Option<PathBuf> {
    config
        .hot_object_root
        .as_ref()
        .map(|root| root.join(workload))
        .or_else(|| {
            matches!(
                workload,
                "native-layout-read" | "ai-dataset-batch-read" | "ai-shard-range-read"
            )
            .then(|| config.root.join(workload).join("hot-objects"))
        })
}

fn parse_object_backend(raw: &str) -> Result<ObjectBackendKind, BenchError> {
    match raw {
        "s3" => Ok(ObjectBackendKind::S3),
        "rustfs" => Ok(ObjectBackendKind::RustFs),
        _ => Err(BenchError::UnknownOption(format!("--object-backend {raw}"))),
    }
}

fn parse_positive_usize(raw: &str, option: &'static str) -> Result<usize, BenchError> {
    raw.parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| BenchError::UnknownOption(format!("{option} {raw}")))
}

fn parse_positive_u64(raw: &str, option: &'static str) -> Result<u64, BenchError> {
    raw.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| BenchError::UnknownOption(format!("{option} {raw}")))
}

fn parse_u64(raw: &str, option: &'static str) -> Result<u64, BenchError> {
    raw.parse::<u64>()
        .map_err(|_| BenchError::UnknownOption(format!("{option} {raw}")))
}

fn parse_hot_fill_mode(raw: &str) -> Result<HotFillMode, BenchError> {
    match raw {
        "inline" => Ok(HotFillMode::Inline),
        "background" => Ok(HotFillMode::Background),
        _ => Err(BenchError::UnknownOption(format!("--hot-fill-mode {raw}"))),
    }
}

fn parse_block_cache(raw: &str) -> Result<bool, BenchError> {
    parse_on_off(raw).map_err(|_| BenchError::UnknownOption(format!("--block-cache {raw}")))
}

fn parse_on_off(raw: &str) -> Result<bool, BenchError> {
    match raw {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        _ => Err(BenchError::UnknownOption(raw.to_owned())),
    }
}

fn value<'a>(
    args: &'a [String],
    index: usize,
    option: &'static str,
) -> Result<&'a str, BenchError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(BenchError::MissingValue(option))
}

fn parse_profile(raw: &str) -> Result<Profile, BenchError> {
    match raw {
        "smoke" => Ok(Profile::Smoke),
        "standard" => Ok(Profile::Standard),
        "long" => Ok(Profile::Long),
        _ => Err(BenchError::InvalidProfile(raw.to_owned())),
    }
}

fn parse_workload(raw: &str) -> Result<Workload, BenchError> {
    match raw {
        "all" => Ok(Workload::All),
        "metadata-smoke" => Ok(Workload::MetadataSmoke),
        "mdtest-easy" => Ok(Workload::MdtestEasy),
        "mdtest-hard" => Ok(Workload::MdtestHard),
        "metadata-negative-lookup" => Ok(Workload::MetadataNegativeLookup),
        "artifact-index-lookup" => Ok(Workload::ArtifactIndexLookup),
        "metadata-concurrent-read" => Ok(Workload::MetadataConcurrentRead),
        "metadata-durability-batch" => Ok(Workload::MetadataDurabilityBatch),
        "metadata-shard-routing" => Ok(Workload::MetadataShardRouting),
        "checkpoint-publish" => Ok(Workload::CheckpointPublish),
        "training-read" => Ok(Workload::TrainingRead),
        "native-layout-read" => Ok(Workload::NativeLayoutRead),
        "ai-dataset-batch-read" => Ok(Workload::AiDatasetBatchRead),
        "ai-shard-range-read" => Ok(Workload::AiShardRangeRead),
        "mlperf-dlio" => Ok(Workload::MlperfDlio),
        "demo-dataset" => Ok(Workload::DemoDataset),
        _ => Err(BenchError::InvalidWorkload(raw.to_owned())),
    }
}

fn expand_workloads(workload: Workload) -> Vec<Workload> {
    match workload {
        Workload::All => vec![
            Workload::MdtestEasy,
            Workload::MdtestHard,
            Workload::MetadataNegativeLookup,
            Workload::ArtifactIndexLookup,
            Workload::MetadataConcurrentRead,
            Workload::MetadataDurabilityBatch,
            Workload::MetadataShardRouting,
            Workload::CheckpointPublish,
            Workload::TrainingRead,
            Workload::NativeLayoutRead,
            Workload::AiDatasetBatchRead,
            Workload::AiShardRangeRead,
            Workload::MlperfDlio,
            Workload::DemoDataset,
        ],
        Workload::MetadataSmoke => vec![
            Workload::MdtestEasy,
            Workload::MdtestHard,
            Workload::MetadataNegativeLookup,
            Workload::MetadataConcurrentRead,
            Workload::MetadataDurabilityBatch,
            Workload::MetadataShardRouting,
        ],
        other => vec![other],
    }
}

fn shape(config: &Config) -> WorkloadShape {
    let mut shape = match config.profile {
        Profile::Smoke => WorkloadShape {
            dirs: 8,
            files_per_dir: 64,
            shared_files: 512,
            checkpoints: 128,
            checkpoint_bytes: 4096,
            dataset_dirs: 8,
            dataset_files_per_dir: 64,
            dataset_file_bytes: 512,
        },
        Profile::Standard => WorkloadShape {
            dirs: 32,
            files_per_dir: 256,
            shared_files: 8192,
            checkpoints: 1024,
            checkpoint_bytes: 1024 * 1024,
            dataset_dirs: 32,
            dataset_files_per_dir: 256,
            dataset_file_bytes: 16 * 1024,
        },
        Profile::Long => WorkloadShape {
            dirs: 64,
            files_per_dir: 1024,
            shared_files: 65536,
            checkpoints: 4096,
            checkpoint_bytes: 8 * 1024 * 1024,
            dataset_dirs: 64,
            dataset_files_per_dir: 1024,
            dataset_file_bytes: 256 * 1024,
        },
    };
    if let Some(bytes) = config.checkpoint_bytes {
        shape.checkpoint_bytes = bytes;
    }
    if let Some(bytes) = config.sample_bytes {
        shape.dataset_file_bytes = bytes;
    }
    shape
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::All => "all",
        Workload::MetadataSmoke => "metadata-smoke",
        Workload::MdtestEasy => "mdtest-easy",
        Workload::MdtestHard => "mdtest-hard",
        Workload::MetadataNegativeLookup => "metadata-negative-lookup",
        Workload::ArtifactIndexLookup => "artifact-index-lookup",
        Workload::MetadataConcurrentRead => "metadata-concurrent-read",
        Workload::MetadataDurabilityBatch => "metadata-durability-batch",
        Workload::MetadataShardRouting => "metadata-shard-routing",
        Workload::CheckpointPublish => "checkpoint-publish",
        Workload::TrainingRead => "training-read",
        Workload::NativeLayoutRead => "native-layout-read",
        Workload::AiDatasetBatchRead => "ai-dataset-batch-read",
        Workload::AiShardRangeRead => "ai-shard-range-read",
        Workload::MlperfDlio => "mlperf-dlio",
        Workload::DemoDataset => "demo-dataset",
    }
}

fn profile_name(profile: Profile) -> &'static str {
    match profile {
        Profile::Smoke => "smoke",
        Profile::Standard => "standard",
        Profile::Long => "long",
    }
}

fn object_backend_name(backend: ObjectBackendKind) -> &'static str {
    match backend {
        ObjectBackendKind::S3 => "s3",
        ObjectBackendKind::RustFs => "rustfs",
    }
}

fn apply_rustfs_defaults(options: &mut S3ObjectStoreOptions) {
    if options.bucket.is_empty() {
        options.bucket = "nokv".to_owned();
    }
    if options.region.is_empty() || options.region == "us-east-1" {
        options.region = "auto".to_owned();
    }
    if options.endpoint.is_none() {
        options.endpoint = Some("http://127.0.0.1:9000".to_owned());
    }
    if options.access_key_id.is_none() {
        options.access_key_id = Some("rustfsadmin".to_owned());
    }
    if options.secret_access_key.is_none() {
        options.secret_access_key = Some("rustfsadmin".to_owned());
    }
}

fn default_root() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    env::temp_dir().join(format!("nokv-bench-{now}"))
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn from_io(err: impl Error) -> BenchError {
    BenchError::Io(err.to_string())
}

fn from_client(err: impl Error) -> BenchError {
    BenchError::Client(err.to_string())
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(option) => write!(f, "{option} requires a value"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::InvalidProfile(profile) => write!(f, "invalid profile {profile}"),
            Self::InvalidWorkload(workload) => write!(f, "invalid workload {workload}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Client(err) => write!(f, "{err}"),
        }
    }
}

impl Error for BenchError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(value: &str) -> String {
        value.to_owned()
    }

    #[test]
    fn parse_defaults_to_smoke_all_with_temp_root() {
        let config = parse(Vec::new()).unwrap();
        assert_eq!(config.profile, Profile::Smoke);
        assert_eq!(config.workload, Workload::All);
        assert_eq!(config.object_backend, ObjectBackendKind::RustFs);
        assert_eq!(config.s3.bucket, "nokv");
        assert_eq!(config.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert!(!config.keep);
        assert!(config.root.to_string_lossy().contains("nokv-bench"));
    }

    #[test]
    fn parse_profile_workload_root_and_keep() {
        let config = parse(vec![
            s("--profile"),
            s("standard"),
            s("--workload"),
            s("training-read"),
            s("--root"),
            s("/tmp/nokv-bench"),
            s("--keep"),
        ])
        .unwrap();
        assert_eq!(config.profile, Profile::Standard);
        assert_eq!(config.workload, Workload::TrainingRead);
        assert_eq!(config.root, PathBuf::from("/tmp/nokv-bench"));
        assert!(config.keep);
        assert_eq!(config.object_concurrency, 1);
        assert_eq!(config.read_repeats, 1);
        assert_eq!(config.range_stride, 1);
        assert_eq!(config.range_coalesce_gap_bytes, 0);
        assert!(config.block_cache);
    }

    #[test]
    fn parse_rustfs_object_options() {
        let config = parse(vec![
            s("--object-backend"),
            s("rustfs"),
            s("--s3-bucket"),
            s("nokv"),
            s("--s3-endpoint"),
            s("http://127.0.0.1:9000"),
            s("--s3-access-key-id"),
            s("access"),
            s("--s3-secret-access-key"),
            s("secret"),
        ])
        .unwrap();
        assert_eq!(config.object_backend, ObjectBackendKind::RustFs);
        assert_eq!(config.s3.bucket, "nokv");
        assert_eq!(config.s3.region, "auto");
        assert_eq!(config.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[test]
    fn parse_s3_does_not_inherit_rustfs_endpoint() {
        let config = parse(vec![
            s("--s3-bucket"),
            s("training-artifacts"),
            s("--object-backend"),
            s("s3"),
        ])
        .unwrap();
        assert_eq!(config.object_backend, ObjectBackendKind::S3);
        assert_eq!(config.s3.bucket, "training-artifacts");
        assert_eq!(config.s3.region, "us-east-1");
        assert_eq!(config.s3.endpoint, None);
    }

    #[test]
    fn parse_object_size_concurrency_and_cache_options() {
        let config = parse(vec![
            s("--object-concurrency"),
            s("8"),
            s("--checkpoint-bytes"),
            s("1048576"),
            s("--sample-bytes"),
            s("65536"),
            s("--read-repeats"),
            s("3"),
            s("--range-stride"),
            s("2"),
            s("--range-coalesce-gap-bytes"),
            s("512"),
            s("--block-cache"),
            s("off"),
        ])
        .unwrap();
        assert_eq!(config.object_concurrency, 8);
        assert_eq!(config.checkpoint_bytes, Some(1_048_576));
        assert_eq!(config.sample_bytes, Some(65_536));
        assert_eq!(config.read_repeats, 3);
        assert_eq!(config.range_stride, 2);
        assert_eq!(config.range_coalesce_gap_bytes, 512);
        assert!(!config.block_cache);
    }

    #[test]
    fn parse_metadata_negative_lookup_workload() {
        let config = parse(vec![s("--workload"), s("metadata-negative-lookup")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataNegativeLookup);
    }

    #[test]
    fn parse_artifact_index_lookup_workload() {
        let config = parse(vec![s("--workload"), s("artifact-index-lookup")]).unwrap();
        assert_eq!(config.workload, Workload::ArtifactIndexLookup);
    }

    #[test]
    fn parse_metadata_concurrent_read_workload() {
        let config = parse(vec![s("--workload"), s("metadata-concurrent-read")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataConcurrentRead);
    }

    #[test]
    fn parse_metadata_durability_batch_workload() {
        let config = parse(vec![s("--workload"), s("metadata-durability-batch")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataDurabilityBatch);
    }

    #[test]
    fn parse_native_layout_read_workload_and_hot_root() {
        let config = parse(vec![
            s("--workload"),
            s("native-layout-read"),
            s("--hot-object-root"),
            s("/tmp/nokv-hot"),
            s("--hot-object-max-bytes"),
            s("1048576"),
            s("--hot-fill-mode"),
            s("background"),
        ])
        .unwrap();
        assert_eq!(config.workload, Workload::NativeLayoutRead);
        assert_eq!(config.hot_object_root, Some(PathBuf::from("/tmp/nokv-hot")));
        assert_eq!(config.hot_object_max_bytes, Some(1_048_576));
        assert_eq!(config.hot_fill_mode, HotFillMode::Background);
    }

    #[test]
    fn shape_applies_object_size_overrides() {
        let config = parse(vec![
            s("--profile"),
            s("standard"),
            s("--checkpoint-bytes"),
            s("8192"),
            s("--sample-bytes"),
            s("4096"),
        ])
        .unwrap();
        let shape = shape(&config);
        assert_eq!(shape.checkpoint_bytes, 8192);
        assert_eq!(shape.dataset_file_bytes, 4096);
    }

    #[test]
    fn stats_json_parser_reads_metadata_fields() {
        let body = r#"{"object_puts":41,"object_put_bytes":42,"object_gets":43,"object_get_bytes":44,"coalesced_gets":45,"coalesced_get_bytes":46,"cache_hits":47,"cache_hit_bytes":48,"prefetch_enqueued":49,"prefetch_dropped":50,"prefetch_completed":51,"prefetch_failed":52,"prefetch_object_gets":53,"prefetch_object_get_bytes":54,"prefetch_cache_hits":55,"prefetch_cache_hit_bytes":56,"read_plan_cache_hits":57,"read_plan_cache_misses":58,"object_writeback_enqueued":59,"object_writeback_inline":60,"object_writeback_completed":62,"object_writeback_failed":63,"object_writeback_staged_bytes":64,"object_writeback_uploaded_bytes":65,"object_writeback_queue_wait_ns":66,"object_writeback_queue_max_wait_ns":67,"object_writeback_upload_ns":68,"object_writeback_upload_max_ns":69,"object_writeback_collect_ns":70,"object_writeback_digest_ns":71,"object_writeback_store_put_ns":72,"object_writeback_cache_put_ns":73,"manifest_chunks":74,"manifest_blocks":75,"tiered_hot_put_ns":76,"tiered_pending_cold_put_ns":77,"tiered_cold_put_enqueue_ns":78,"local_hot_puts":79,"local_hot_put_bytes":80,"local_hot_put_total_ns":81,"local_hot_put_prepare_ns":82,"local_hot_put_write_ns":83,"local_hot_put_sync_ns":84,"local_hot_put_rename_ns":85,"local_hot_put_record_ns":86,"metadata_store":{"get_total":2,"get_user_strong_total":32,"get_write_plan_local_total":33,"get_snapshot_total":34,"scan_total":3,"scan_user_strong_total":35,"scan_write_plan_local_total":36,"scan_snapshot_total":37,"scan_key_visited_total":4,"scan_key_returned_total":5,"history_lookup_total":40,"active_snapshot_pin_total":0,"commit_total":6,"dedupe_hit_total":7,"predicate_total":8,"prefix_empty_predicate_total":9,"current_put_total":10,"current_delete_total":11,"history_write_total":12,"watch_write_total":13,"dedupe_write_total":14,"commit_prepare_ns_total":15,"atomic_apply_total":16,"atomic_apply_command_total":17,"atomic_apply_max_batch":18,"atomic_apply_ns_total":19},"metadata_service":{"path_index_lookup_total":30,"path_index_hit_total":31,"path_index_miss_total":32,"path_index_stale_total":33,"path_index_scan_stale_total":34,"path_index_fallback_total":35,"create_files_batch_total":36,"create_files_entry_total":37,"create_dirs_batch_total":38,"create_dirs_entry_total":39,"read_dir_plus_total":40,"read_dir_plus_entry_total":41,"read_dir_plus_projection_hit_total":42,"metadata_log_segments_archived_total":43,"metadata_log_entries_archived_total":44,"metadata_log_archive_bytes_total":45,"restore_to_fork_requests_total":46,"restore_to_fork_success_total":47,"restore_to_fork_failure_total":48,"restore_to_fork_elapsed_ns_total":49,"restore_to_fork_elapsed_ns_max":50}}"#;

        assert_eq!(json_u64(body, "object_put_bytes").unwrap(), 42);
        assert_eq!(json_u64(body, "object_get_bytes").unwrap(), 44);
        assert_eq!(json_u64(body, "coalesced_gets").unwrap(), 45);
        assert_eq!(json_u64(body, "coalesced_get_bytes").unwrap(), 46);
        assert_eq!(json_u64(body, "cache_hit_bytes").unwrap(), 48);
        assert_eq!(json_u64(body, "prefetch_enqueued").unwrap(), 49);
        assert_eq!(json_u64(body, "prefetch_object_get_bytes").unwrap(), 54);
        assert_eq!(json_u64(body, "prefetch_cache_hit_bytes").unwrap(), 56);
        assert_eq!(json_u64(body, "read_plan_cache_hits").unwrap(), 57);
        assert_eq!(json_u64(body, "read_plan_cache_misses").unwrap(), 58);
        assert_eq!(json_u64_or_zero(body, "object_writeback_enqueued"), 59);
        assert_eq!(json_u64_or_zero(body, "object_writeback_inline"), 60);
        assert_eq!(json_u64_or_zero(body, "object_writeback_completed"), 62);
        assert_eq!(json_u64_or_zero(body, "object_writeback_failed"), 63);
        assert_eq!(json_u64_or_zero(body, "object_writeback_staged_bytes"), 64);
        assert_eq!(
            json_u64_or_zero(body, "object_writeback_uploaded_bytes"),
            65
        );
        assert_eq!(json_u64_or_zero(body, "object_writeback_queue_wait_ns"), 66);
        assert_eq!(
            json_u64_or_zero(body, "object_writeback_queue_max_wait_ns"),
            67
        );
        assert_eq!(json_u64_or_zero(body, "object_writeback_upload_ns"), 68);
        assert_eq!(json_u64_or_zero(body, "object_writeback_upload_max_ns"), 69);
        assert_eq!(json_u64_or_zero(body, "object_writeback_collect_ns"), 70);
        assert_eq!(json_u64_or_zero(body, "object_writeback_digest_ns"), 71);
        assert_eq!(json_u64_or_zero(body, "object_writeback_store_put_ns"), 72);
        assert_eq!(json_u64_or_zero(body, "object_writeback_cache_put_ns"), 73);
        assert_eq!(json_u64_or_zero(body, "tiered_hot_put_ns"), 76);
        assert_eq!(json_u64_or_zero(body, "tiered_pending_cold_put_ns"), 77);
        assert_eq!(json_u64_or_zero(body, "tiered_cold_put_enqueue_ns"), 78);
        assert_eq!(json_u64_or_zero(body, "local_hot_puts"), 79);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_bytes"), 80);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_total_ns"), 81);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_prepare_ns"), 82);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_write_ns"), 83);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_sync_ns"), 84);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_rename_ns"), 85);
        assert_eq!(json_u64_or_zero(body, "local_hot_put_record_ns"), 86);
        assert_eq!(json_u64_or_zero(body, "missing_writeback_total"), 0);
        assert_eq!(json_u64(body, "commit_total").unwrap(), 6);
        assert_eq!(json_u64(body, "get_write_plan_local_total").unwrap(), 33);
        assert_eq!(json_u64(body, "scan_write_plan_local_total").unwrap(), 36);
        assert_eq!(json_u64(body, "scan_key_visited_total").unwrap(), 4);
        assert_eq!(json_u64(body, "history_lookup_total").unwrap(), 40);
        assert_eq!(json_u64(body, "atomic_apply_total").unwrap(), 16);
        assert_eq!(json_u64(body, "atomic_apply_command_total").unwrap(), 17);
        assert_eq!(json_u64(body, "atomic_apply_max_batch").unwrap(), 18);
        assert_eq!(json_u64(body, "atomic_apply_ns_total").unwrap(), 19);
        assert_eq!(json_u64(body, "path_index_hit_total").unwrap(), 31);
        assert_eq!(json_u64(body, "path_index_scan_stale_total").unwrap(), 34);
        assert_eq!(json_u64(body, "create_files_batch_total").unwrap(), 36);
        assert_eq!(json_u64(body, "create_dirs_entry_total").unwrap(), 39);
        assert_eq!(
            json_u64(body, "read_dir_plus_projection_hit_total").unwrap(),
            42
        );
        assert_eq!(
            json_u64(body, "metadata_log_segments_archived_total").unwrap(),
            43
        );
        assert_eq!(
            json_u64(body, "metadata_log_entries_archived_total").unwrap(),
            44
        );
        assert_eq!(
            json_u64(body, "metadata_log_archive_bytes_total").unwrap(),
            45
        );
        assert_eq!(
            json_u64(body, "restore_to_fork_requests_total").unwrap(),
            46
        );
        assert_eq!(json_u64(body, "restore_to_fork_success_total").unwrap(), 47);
        assert_eq!(json_u64(body, "restore_to_fork_failure_total").unwrap(), 48);
        assert_eq!(
            json_u64(body, "restore_to_fork_elapsed_ns_total").unwrap(),
            49
        );
        assert_eq!(
            json_u64(body, "restore_to_fork_elapsed_ns_max").unwrap(),
            50
        );
        assert!(json_u64(body, "missing_total").is_err());
    }

    #[test]
    fn csv_row_reports_hot_path_attribution() {
        let row = row(RowInput {
            workload: "mdtest-easy",
            profile: Profile::Smoke,
            operations: 10,
            seconds: 0.5,
            bytes: 0,
            samples: 0,
            stats: BenchStats {
                object: ObjectTransferStats {
                    object_writeback_enqueued: 3,
                    object_writeback_inline: 1,
                    object_writeback_completed: 2,
                    object_writeback_failed: 1,
                    object_writeback_staged_bytes: 4096,
                    object_writeback_uploaded_bytes: 2048,
                    object_writeback_queue_wait_ns: 10,
                    object_writeback_queue_max_wait_ns: 7,
                    object_writeback_upload_ns: 20,
                    object_writeback_upload_max_ns: 12,
                    object_writeback_collect_ns: 5,
                    object_writeback_digest_ns: 6,
                    object_writeback_store_put_ns: 7,
                    object_writeback_cache_put_ns: 8,
                    ..ObjectTransferStats::default()
                },
                metadata_store: MetadataStoreStats {
                    commit_total: 4,
                    dedupe_hit_total: 1,
                    predicate_total: 8,
                    prefix_empty_predicate_total: 2,
                    history_lookup_total: 5,
                    current_put_total: 12,
                    atomic_apply_total: 2,
                    atomic_apply_command_total: 4,
                    atomic_apply_max_batch: 3,
                    ..MetadataStoreStats::default()
                },
                metadata_service: MetadataServiceStats {
                    path_index_lookup_total: 4,
                    path_index_hit_total: 3,
                    path_index_miss_total: 1,
                    path_index_scan_stale_total: 2,
                    read_dir_plus_total: 2,
                    read_dir_plus_entry_total: 8,
                    read_dir_plus_projection_hit_total: 6,
                    ..MetadataServiceStats::default()
                },
                data_fabric: DataFabricReadStats {
                    planned_blocks: 9,
                    local_nvme_hits: 7,
                    object_fallbacks: 2,
                    object_gets: 2,
                    object_get_bytes: 8192,
                    coalesced_ranges: 1,
                    coalesced_range_bytes: 4096,
                    cache_hits: 3,
                    cache_hit_bytes: 12288,
                },
                tiered_object: TieredObjectStoreStats {
                    hot_gets: 9,
                    hot_hits: 7,
                    hot_misses: 2,
                    cold_gets: 2,
                    cold_get_bytes: 8192,
                    hot_put_ns: 13,
                    pending_cold_put_ns: 14,
                    cold_put_enqueue_ns: 15,
                    hot_fills: 2,
                    hot_fill_enqueued: 2,
                    hot_fill_coalesced: 1,
                    ..TieredObjectStoreStats::default()
                },
                local_hot: LocalObjectStoreStats {
                    resident_objects: 4,
                    resident_bytes: 16384,
                    max_bytes: Some(32768),
                    evictions: 1,
                    eviction_bytes: 4096,
                    admission_rejections: 1,
                    puts: 2,
                    put_bytes: 8192,
                    put_total_ns: 21,
                    put_prepare_ns: 22,
                    put_write_ns: 23,
                    put_sync_ns: 24,
                    put_rename_ns: 25,
                    put_record_ns: 26,
                },
            },
            object_concurrency: 1,
            read_repeats: 1,
            block_cache: true,
            checksum: 99,
            shape: "shape".to_owned(),
            caveat: "caveat".to_owned(),
        });

        assert_eq!(row.metadata_dedupe_hits, 1);
        assert_eq!(row.metadata_prefix_empty_predicates, 2);
        assert_eq!(row.metadata_history_lookups, 5);
        assert_eq!(row.metadata_atomic_applies, 2);
        assert_eq!(row.metadata_atomic_apply_commands, 4);
        assert_eq!(row.metadata_atomic_apply_max_batch, 3);
        assert_eq!(row.path_index_lookups, 4);
        assert_eq!(row.path_index_misses, 1);
        assert_eq!(row.path_index_scan_stale, 2);
        assert_eq!(row.path_index_hit_rate, 0.75);
        assert_eq!(row.read_dir_plus_calls, 2);
        assert_eq!(row.read_dir_plus_projection_hit_rate, 0.75);
        assert_eq!(row.object_writeback_enqueued, 3);
        assert_eq!(row.object_writeback_staged_bytes, 4096);
        assert_eq!(row.object_writeback_upload_max_ns, 12);
        assert_eq!(row.object_writeback_collect_ns, 5);
        assert_eq!(row.object_writeback_digest_ns, 6);
        assert_eq!(row.object_writeback_store_put_ns, 7);
        assert_eq!(row.object_writeback_cache_put_ns, 8);
        assert_eq!(row.data_fabric_planned_blocks, 9);
        assert_eq!(row.data_fabric_local_nvme_hits, 7);
        assert_eq!(row.data_fabric_object_fallbacks, 2);
        assert_eq!(row.data_fabric_coalesced_ranges, 1);
        assert_eq!(row.data_fabric_cache_hit_bytes, 12288);
        assert_eq!(row.tiered_hot_gets, 9);
        assert_eq!(row.tiered_hot_hits, 7);
        assert_eq!(row.tiered_hot_misses, 2);
        assert_eq!(row.tiered_cold_gets, 2);
        assert_eq!(row.tiered_cold_get_bytes, 8192);
        assert_eq!(row.tiered_hot_fills, 2);
        assert_eq!(row.tiered_hot_fill_enqueued, 2);
        assert_eq!(row.tiered_hot_fill_coalesced, 1);
        assert_eq!(row.tiered_hot_put_ns, 13);
        assert_eq!(row.tiered_pending_cold_put_ns, 14);
        assert_eq!(row.tiered_cold_put_enqueue_ns, 15);
        assert_eq!(row.local_hot_resident_objects, 4);
        assert_eq!(row.local_hot_resident_bytes, 16384);
        assert_eq!(row.local_hot_max_bytes, 32768);
        assert_eq!(row.local_hot_evictions, 1);
        assert_eq!(row.local_hot_admission_rejections, 1);
        assert_eq!(row.local_hot_puts, 2);
        assert_eq!(row.local_hot_put_bytes, 8192);
        assert_eq!(row.local_hot_put_total_ns, 21);
        assert_eq!(row.local_hot_put_write_ns, 23);

        let header = csv_header();
        let labels = BoundaryLabels {
            boundary: "L1",
            system: "nokv",
            metadata_tier: "nokv-l1-service".to_owned(),
            object_backend: "rustfs".to_owned(),
            cache_state: "n/a",
            concurrency: 1,
            tool: "native",
        };
        let record = csv_row(&row, &labels);
        assert!(header.starts_with("boundary,system,metadata_tier,"));
        assert!(header.contains("metadata_prefix_empty_predicates"));
        assert!(header.contains("metadata_history_lookups"));
        assert!(header.contains("metadata_atomic_apply_max_batch"));
        assert!(header.contains("path_index_hit_rate"));
        assert!(header.contains("path_index_scan_stale"));
        assert!(header.contains("read_dir_plus_projection_hit_rate"));
        assert!(header.contains("object_writeback_queue_max_wait_ns"));
        assert!(header.contains("object_writeback_store_put_ns"));
        assert!(header.contains("data_fabric_local_nvme_hits"));
        assert!(header.contains("tiered_hot_hits"));
        assert!(header.contains("tiered_cold_gets"));
        assert!(header.contains("tiered_hot_fills"));
        assert!(header.contains("tiered_hot_fill_coalesced"));
        assert!(header.contains("tiered_hot_put_ns"));
        assert!(header.contains("tiered_pending_cold_put_ns"));
        assert!(header.contains("local_hot_resident_bytes"));
        assert!(header.contains("local_hot_admission_rejections"));
        assert!(header.contains("local_hot_put_total_ns"));
        assert!(header.contains("local_hot_put_write_ns"));
        assert_eq!(header.split(',').count(), record.split(',').count());
    }

    #[test]
    fn stats_delta_clears_atomic_apply_max_when_window_has_no_apply() {
        let before = BenchStats {
            metadata_store: MetadataStoreStats {
                atomic_apply_total: 3,
                atomic_apply_command_total: 16,
                atomic_apply_max_batch: 8,
                ..MetadataStoreStats::default()
            },
            ..BenchStats::default()
        };
        let after = before;

        let delta = stats_delta(before, after);

        assert_eq!(delta.metadata_store.atomic_apply_total, 0);
        assert_eq!(delta.metadata_store.atomic_apply_command_total, 0);
        assert_eq!(delta.metadata_store.atomic_apply_max_batch, 0);
    }

    #[test]
    fn stats_delta_reports_object_writeback_window() {
        let before = BenchStats {
            object: ObjectTransferStats {
                object_writeback_enqueued: 3,
                object_writeback_completed: 2,
                object_writeback_staged_bytes: 1024,
                object_writeback_uploaded_bytes: 512,
                object_writeback_queue_wait_ns: 30,
                object_writeback_queue_max_wait_ns: 25,
                object_writeback_upload_ns: 80,
                object_writeback_upload_max_ns: 50,
                object_writeback_collect_ns: 10,
                object_writeback_digest_ns: 20,
                object_writeback_store_put_ns: 30,
                object_writeback_cache_put_ns: 40,
                ..ObjectTransferStats::default()
            },
            ..BenchStats::default()
        };
        let after = BenchStats {
            object: ObjectTransferStats {
                object_writeback_enqueued: 5,
                object_writeback_completed: 4,
                object_writeback_staged_bytes: 4096,
                object_writeback_uploaded_bytes: 2048,
                object_writeback_queue_wait_ns: 100,
                object_writeback_queue_max_wait_ns: 60,
                object_writeback_upload_ns: 240,
                object_writeback_upload_max_ns: 90,
                object_writeback_collect_ns: 30,
                object_writeback_digest_ns: 70,
                object_writeback_store_put_ns: 90,
                object_writeback_cache_put_ns: 120,
                ..ObjectTransferStats::default()
            },
            ..BenchStats::default()
        };

        let delta = stats_delta(before, after);

        assert_eq!(delta.object.object_writeback_enqueued, 2);
        assert_eq!(delta.object.object_writeback_completed, 2);
        assert_eq!(delta.object.object_writeback_staged_bytes, 3072);
        assert_eq!(delta.object.object_writeback_uploaded_bytes, 1536);
        assert_eq!(delta.object.object_writeback_queue_wait_ns, 70);
        assert_eq!(delta.object.object_writeback_queue_max_wait_ns, 60);
        assert_eq!(delta.object.object_writeback_upload_ns, 160);
        assert_eq!(delta.object.object_writeback_upload_max_ns, 90);
        assert_eq!(delta.object.object_writeback_collect_ns, 20);
        assert_eq!(delta.object.object_writeback_digest_ns, 50);
        assert_eq!(delta.object.object_writeback_store_put_ns, 60);
        assert_eq!(delta.object.object_writeback_cache_put_ns, 80);

        let idle_delta = stats_delta(after, after);
        assert_eq!(idle_delta.object.object_writeback_queue_max_wait_ns, 0);
        assert_eq!(idle_delta.object.object_writeback_upload_max_ns, 0);
    }

    #[test]
    fn stats_delta_reports_data_fabric_window() {
        let before = BenchStats {
            data_fabric: DataFabricReadStats {
                planned_blocks: 10,
                local_nvme_hits: 3,
                object_fallbacks: 7,
                object_gets: 7,
                object_get_bytes: 1024,
                coalesced_ranges: 2,
                cache_hits: 1,
                ..DataFabricReadStats::default()
            },
            ..BenchStats::default()
        };
        let after = BenchStats {
            data_fabric: DataFabricReadStats {
                planned_blocks: 18,
                local_nvme_hits: 9,
                object_fallbacks: 9,
                object_gets: 9,
                object_get_bytes: 4096,
                coalesced_ranges: 3,
                cache_hits: 5,
                ..DataFabricReadStats::default()
            },
            ..BenchStats::default()
        };

        let delta = stats_delta(before, after);

        assert_eq!(delta.data_fabric.planned_blocks, 8);
        assert_eq!(delta.data_fabric.local_nvme_hits, 6);
        assert_eq!(delta.data_fabric.object_fallbacks, 2);
        assert_eq!(delta.data_fabric.object_get_bytes, 3072);
        assert_eq!(delta.data_fabric.coalesced_ranges, 1);
        assert_eq!(delta.data_fabric.cache_hits, 4);
    }

    #[test]
    fn stats_delta_reports_tiered_object_window() {
        let before = BenchStats {
            tiered_object: TieredObjectStoreStats {
                hot_gets: 10,
                hot_hits: 3,
                hot_misses: 7,
                cold_gets: 7,
                cold_get_bytes: 1024,
                hot_fills: 6,
                hot_fill_enqueued: 4,
                hot_fill_coalesced: 1,
                hot_fill_errors: 1,
                hot_put_ns: 10,
                pending_cold_put_ns: 20,
                cold_put_enqueue_ns: 30,
                ..TieredObjectStoreStats::default()
            },
            ..BenchStats::default()
        };
        let after = BenchStats {
            tiered_object: TieredObjectStoreStats {
                hot_gets: 18,
                hot_hits: 9,
                hot_misses: 9,
                cold_gets: 9,
                cold_get_bytes: 4096,
                hot_fills: 8,
                hot_fill_enqueued: 7,
                hot_fill_coalesced: 3,
                hot_fill_errors: 2,
                hot_put_ns: 15,
                pending_cold_put_ns: 27,
                cold_put_enqueue_ns: 41,
                ..TieredObjectStoreStats::default()
            },
            ..BenchStats::default()
        };

        let delta = stats_delta(before, after);

        assert_eq!(delta.tiered_object.hot_gets, 8);
        assert_eq!(delta.tiered_object.hot_hits, 6);
        assert_eq!(delta.tiered_object.hot_misses, 2);
        assert_eq!(delta.tiered_object.cold_gets, 2);
        assert_eq!(delta.tiered_object.cold_get_bytes, 3072);
        assert_eq!(delta.tiered_object.hot_fills, 2);
        assert_eq!(delta.tiered_object.hot_fill_enqueued, 3);
        assert_eq!(delta.tiered_object.hot_fill_coalesced, 2);
        assert_eq!(delta.tiered_object.hot_fill_errors, 1);
        assert_eq!(delta.tiered_object.hot_put_ns, 5);
        assert_eq!(delta.tiered_object.pending_cold_put_ns, 7);
        assert_eq!(delta.tiered_object.cold_put_enqueue_ns, 11);
    }

    #[test]
    fn stats_delta_reports_local_hot_window() {
        let before = BenchStats {
            local_hot: LocalObjectStoreStats {
                resident_objects: 2,
                resident_bytes: 1024,
                max_bytes: Some(4096),
                evictions: 3,
                eviction_bytes: 512,
                admission_rejections: 1,
                puts: 4,
                put_bytes: 1024,
                put_total_ns: 100,
                put_prepare_ns: 10,
                put_write_ns: 20,
                put_sync_ns: 30,
                put_rename_ns: 40,
                put_record_ns: 50,
            },
            ..BenchStats::default()
        };
        let after = BenchStats {
            local_hot: LocalObjectStoreStats {
                resident_objects: 4,
                resident_bytes: 2048,
                max_bytes: Some(4096),
                evictions: 5,
                eviction_bytes: 1536,
                admission_rejections: 2,
                puts: 9,
                put_bytes: 4096,
                put_total_ns: 190,
                put_prepare_ns: 25,
                put_write_ns: 55,
                put_sync_ns: 30,
                put_rename_ns: 70,
                put_record_ns: 95,
            },
            ..BenchStats::default()
        };

        let delta = stats_delta(before, after);

        assert_eq!(delta.local_hot.resident_objects, 4);
        assert_eq!(delta.local_hot.resident_bytes, 2048);
        assert_eq!(delta.local_hot.max_bytes, Some(4096));
        assert_eq!(delta.local_hot.evictions, 2);
        assert_eq!(delta.local_hot.eviction_bytes, 1024);
        assert_eq!(delta.local_hot.admission_rejections, 1);
        assert_eq!(delta.local_hot.puts, 5);
        assert_eq!(delta.local_hot.put_bytes, 3072);
        assert_eq!(delta.local_hot.put_total_ns, 90);
        assert_eq!(delta.local_hot.put_prepare_ns, 15);
        assert_eq!(delta.local_hot.put_write_ns, 35);
        assert_eq!(delta.local_hot.put_sync_ns, 0);
        assert_eq!(delta.local_hot.put_rename_ns, 30);
        assert_eq!(delta.local_hot.put_record_ns, 45);
    }

    #[test]
    fn workload_all_expands_to_industry_and_training_paths() {
        assert_eq!(
            expand_workloads(Workload::All),
            vec![
                Workload::MdtestEasy,
                Workload::MdtestHard,
                Workload::MetadataNegativeLookup,
                Workload::ArtifactIndexLookup,
                Workload::MetadataConcurrentRead,
                Workload::MetadataDurabilityBatch,
                Workload::MetadataShardRouting,
                Workload::CheckpointPublish,
                Workload::TrainingRead,
                Workload::NativeLayoutRead,
                Workload::AiDatasetBatchRead,
                Workload::AiShardRangeRead,
                Workload::MlperfDlio,
                Workload::DemoDataset
            ]
        );
    }

    #[test]
    fn native_layout_read_defaults_to_tiered_hot_root() {
        let config = parse(vec![
            s("--root"),
            s("/tmp/nokv-bench"),
            s("--workload"),
            s("native-layout-read"),
        ])
        .unwrap();
        let object = object_config_for(&config, "native-layout-read");

        assert_eq!(
            object.local_hot_root(),
            Some(PathBuf::from("/tmp/nokv-bench/native-layout-read/hot-objects").as_path())
        );
        assert_eq!(
            object.tiered_options(),
            Some(TieredObjectStoreOptions {
                put_policy: TieredPutPolicy::ColdOnly,
                populate_hot_on_get: true,
                ..TieredObjectStoreOptions::default()
            })
        );
        let labels = boundary_labels(&config, "native-layout-read");
        assert_eq!(labels.object_backend, "rustfs+local-hot+put=cold-only");
    }

    #[test]
    fn checkpoint_publish_with_hot_root_uses_hot_first_put_policy() {
        let config = parse(vec![
            s("--root"),
            s("/tmp/nokv-bench"),
            s("--workload"),
            s("checkpoint-publish"),
            s("--hot-object-root"),
            s("/tmp/nokv-hot"),
        ])
        .unwrap();
        let object = object_config_for(&config, "checkpoint-publish");

        assert_eq!(
            object.local_hot_root(),
            Some(PathBuf::from("/tmp/nokv-hot/checkpoint-publish").as_path())
        );
        assert_eq!(
            object.tiered_options(),
            Some(TieredObjectStoreOptions {
                put_policy: TieredPutPolicy::HotThenBackgroundCold,
                populate_hot_on_get: true,
                ..TieredObjectStoreOptions::default()
            })
        );
        let labels = boundary_labels(&config, "checkpoint-publish");
        assert_eq!(labels.object_backend, "rustfs+local-hot+put=hot-background");
    }

    #[test]
    fn ai_dataset_batch_read_defaults_to_tiered_hot_root() {
        let config = parse(vec![
            s("--root"),
            s("/tmp/nokv-bench"),
            s("--workload"),
            s("ai-dataset-batch-read"),
        ])
        .unwrap();
        let object = object_config_for(&config, "ai-dataset-batch-read");

        assert_eq!(
            object.local_hot_root(),
            Some(PathBuf::from("/tmp/nokv-bench/ai-dataset-batch-read/hot-objects").as_path())
        );
        let labels = boundary_labels(&config, "ai-dataset-batch-read");
        assert_eq!(labels.object_backend, "rustfs+local-hot+put=cold-only");
    }

    #[test]
    fn ai_shard_range_read_defaults_to_tiered_hot_root() {
        let config = parse(vec![
            s("--root"),
            s("/tmp/nokv-bench"),
            s("--workload"),
            s("ai-shard-range-read"),
        ])
        .unwrap();
        assert_eq!(config.workload, Workload::AiShardRangeRead);
        let object = object_config_for(&config, "ai-shard-range-read");

        assert_eq!(
            object.local_hot_root(),
            Some(PathBuf::from("/tmp/nokv-bench/ai-shard-range-read/hot-objects").as_path())
        );
        let labels = boundary_labels(&config, "ai-shard-range-read");
        assert_eq!(labels.object_backend, "rustfs+local-hot+put=cold-only");
    }

    #[test]
    fn metadata_smoke_expands_to_metadata_only_paths() {
        assert_eq!(
            expand_workloads(Workload::MetadataSmoke),
            vec![
                Workload::MdtestEasy,
                Workload::MdtestHard,
                Workload::MetadataNegativeLookup,
                Workload::MetadataConcurrentRead,
                Workload::MetadataDurabilityBatch,
                Workload::MetadataShardRouting
            ]
        );
    }

    #[test]
    fn parses_metadata_shard_routing_workload() {
        let config = parse(vec![s("--workload"), s("metadata-shard-routing")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataShardRouting);
        // It is its own row (not expanded into other workloads).
        assert_eq!(
            expand_workloads(Workload::MetadataShardRouting),
            vec![Workload::MetadataShardRouting]
        );
    }

    #[test]
    fn shard_routing_paths_spread_evenly_across_shards() {
        let shape = WorkloadShape {
            dirs: 8,
            files_per_dir: 4,
            shared_files: 0,
            checkpoints: 0,
            checkpoint_bytes: 0,
            dataset_dirs: 0,
            dataset_files_per_dir: 0,
            dataset_file_bytes: 0,
        };
        let paths = shard_routing_paths(&shape, SHARD_ROUTING_SHARDS);
        // One top-level dir per subtree shard, `dirs` subdirs, dirs*files leaves.
        assert_eq!(paths.shard_dirs.len(), SHARD_ROUTING_SHARDS);
        assert_eq!(paths.dir_paths.len(), shape.dirs);
        assert_eq!(paths.file_paths.len(), shape.dirs * shape.files_per_dir);
        // The subdirs distribute round-robin across the shard prefixes, so each
        // shard owns the same count (8 dirs / 4 shards = 2 each).
        for shard in 0..SHARD_ROUTING_SHARDS {
            let prefix = format!("/shard-{shard:02}/");
            let count = paths
                .dir_paths
                .iter()
                .filter(|path| path.starts_with(&prefix))
                .count();
            assert_eq!(count, shape.dirs / SHARD_ROUTING_SHARDS);
        }
    }

    #[test]
    fn rejects_removed_local_object_options() {
        assert!(matches!(
            parse(vec![s("--object-backend"), s("local")]),
            Err(BenchError::UnknownOption(_))
        ));
        assert!(matches!(
            parse(vec![s("--objects"), s("/tmp/objects")]),
            Err(BenchError::UnknownOption(_))
        ));
    }
}
