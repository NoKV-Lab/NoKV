//! Holt-backed metadata store for NoKV.
//!
//! This crate owns the mapping from storage-engine-neutral metadata commands to
//! Holt family trees. It does not own filesystem semantics, object storage,
//! Raft replication, FUSE, or protobuf types.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::command::{
    metadata_commands_conflict, CommitResult, DelimitedScanItem, DelimitedScanRequest,
    HistoryPruneOutcome, HistoryPruneRequest, KeyScanRequest, MetadataCheckpointStore,
    MetadataCommand, MetadataError, MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider,
    MutationOp, Predicate, ReadItem, ReadPurpose, ScanItem, ScanRequest, Value, Version,
};
use crate::layout::{history_index_key, history_index_prefix, history_key, history_prefix};
use holt::{
    CheckpointImage, DBAtomicBatch, Durability, Error as HoltError, KeyRangeEntryRef,
    KeyScanOutcome, RangeEntry, RecordVersion,
};
use holt::{Tree, TreeConfig, DB};
use nokv_types::RecordFamily;

mod codec;
mod families;
use codec::*;
use families::*;

const HISTORY_INDEX_COMPLETE_KEY: &[u8] = b"\0history-key-index-v1-complete";
const HISTORY_INDEX_PROGRESS_KEY: &[u8] = b"\0history-key-index-v1-progress";
const HISTORY_INDEX_COMPLETE_VALUE: &[u8] = b"1";
const HISTORY_INDEX_BACKFILL_BATCH_SIZE: usize = 1_024;
const HISTORY_PRUNE_CONFLICT_RETRIES: usize = 4;

#[derive(Clone)]
pub struct HoltMetadataStore {
    db: DB,
    stats: Arc<HoltMetadataStoreCounters>,
    history_retention: Arc<HistoryRetentionState>,
}

#[derive(Default)]
struct HistoryRetentionState {
    /// Ordinary commands share the read side from planning through durable
    /// apply. Snapshot pins and fork-base bindings take the write side, making
    /// their lifetime transition indivisible from the counters used by planners.
    planning_fence: RwLock<()>,
    active_snapshot_pins: AtomicU64,
    active_fork_bindings: AtomicU64,
    /// Monotonic process-local sequence for durable holder transitions. It is
    /// protected by `planning_fence` and serves as the seqlock/CAS token for a
    /// service-computed retention floor.
    epoch: AtomicU64,
    /// Highest snapshot-visible commit applied through this live store handle.
    /// This deliberately starts at zero on open: no pre-open command can still
    /// hold this handle's planning fence, while the service restores its durable
    /// version allocator before accepting work. Scanning the unbounded dedupe
    /// tree at readiness would turn restart latency into O(command history).
    max_applied_commit_version: AtomicU64,
    #[cfg(test)]
    after_retention_apply_before_state: std::sync::Mutex<Option<TestHook>>,
    #[cfg(test)]
    before_ordinary_planning_fence: std::sync::Mutex<Option<TestHook>>,
}

#[cfg(test)]
type TestHook = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct HistoryRetentionDelta {
    snapshot_pins: i64,
    fork_bindings: i64,
}

impl HistoryRetentionDelta {
    fn is_zero(self) -> bool {
        self.snapshot_pins == 0 && self.fork_bindings == 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RecoveredHistoryRetentionState {
    active_snapshot_pins: u64,
    active_fork_bindings: u64,
}

#[derive(Default)]
struct HoltMetadataStoreCounters {
    get_total: AtomicU64,
    get_user_strong_total: AtomicU64,
    get_write_plan_local_total: AtomicU64,
    get_snapshot_total: AtomicU64,
    scan_total: AtomicU64,
    scan_user_strong_total: AtomicU64,
    scan_write_plan_local_total: AtomicU64,
    scan_snapshot_total: AtomicU64,
    scan_cache_hit_total: AtomicU64,
    scan_key_visited_total: AtomicU64,
    scan_key_returned_total: AtomicU64,
    history_lookup_total: AtomicU64,
    commit_total: AtomicU64,
    dedupe_hit_total: AtomicU64,
    predicate_total: AtomicU64,
    prefix_empty_predicate_total: AtomicU64,
    current_put_total: AtomicU64,
    current_delete_total: AtomicU64,
    history_write_total: AtomicU64,
    watch_write_total: AtomicU64,
    dedupe_write_total: AtomicU64,
    commit_prepare_ns_total: AtomicU64,
    atomic_apply_total: AtomicU64,
    atomic_apply_command_total: AtomicU64,
    atomic_apply_max_batch: AtomicU64,
    atomic_apply_ns_total: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MutationGuard {
    Always,
    PutIfAbsent,
    CompareAndPut(RecordVersion),
    DeleteIfVersion(RecordVersion),
}

#[derive(Clone, Debug)]
struct PlannedMutation {
    mutation: crate::command::Mutation,
    guard: MutationGuard,
}

#[derive(Clone, Debug)]
struct VersionGuard {
    family: RecordFamily,
    key: Vec<u8>,
    version: RecordVersion,
}

#[derive(Clone, Debug)]
struct PrefixEmptyGuard {
    family: RecordFamily,
    prefix: Vec<u8>,
}

struct CommandPlan {
    mutations: Vec<PlannedMutation>,
    history_records: Vec<(RecordFamily, Vec<u8>, Vec<u8>)>,
    version_guards: Vec<VersionGuard>,
    prefix_empty_guards: Vec<PrefixEmptyGuard>,
    retain_history: bool,
    history_retention_delta: HistoryRetentionDelta,
    adds_history_retention: bool,
}

struct PendingPlannedCommand {
    index: usize,
    command: MetadataCommand,
    plan: CommandPlan,
}

#[derive(Clone, Debug)]
struct PlannedCommandStats {
    predicate_count: u64,
    prefix_empty_predicate_count: u64,
    current_put_count: u64,
    current_delete_count: u64,
    history_write_count: u64,
    watch_write_count: u64,
    history_retention_delta: HistoryRetentionDelta,
    advances_history_version: bool,
    result: CommitResult,
    dedupe_result: Vec<u8>,
}

enum PreparedCommand {
    DedupeHit(CommitResult),
    Planned(CommandPlan),
}

struct CurrentRecord {
    record_version: RecordVersion,
    metadata_version: Version,
    value: Option<Vec<u8>>,
}

struct HistoryIndexPruneUpdate {
    key: Vec<u8>,
    record_version: RecordVersion,
    latest_retained: Option<u64>,
}

struct HistoryPruneDeletePlan {
    records: Vec<(Vec<u8>, RecordVersion)>,
    index_updates: Vec<HistoryIndexPruneUpdate>,
}

type CurrentScanCandidate = (Vec<u8>, Vec<u8>);

impl HistoryRetentionState {
    fn new(recovered: RecoveredHistoryRetentionState) -> Self {
        Self {
            planning_fence: RwLock::new(()),
            active_snapshot_pins: AtomicU64::new(recovered.active_snapshot_pins),
            active_fork_bindings: AtomicU64::new(recovered.active_fork_bindings),
            epoch: AtomicU64::new(1),
            max_applied_commit_version: AtomicU64::new(0),
            #[cfg(test)]
            after_retention_apply_before_state: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_ordinary_planning_fence: std::sync::Mutex::new(None),
        }
    }

    fn install_recovered(&self, recovered: RecoveredHistoryRetentionState) {
        self.active_snapshot_pins
            .store(recovered.active_snapshot_pins, Ordering::Release);
        self.active_fork_bindings
            .store(recovered.active_fork_bindings, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        // A checkpoint install replaces the DB while holding the exclusive
        // planning fence, so there is no surviving pre-install plan to order.
        self.max_applied_commit_version.store(0, Ordering::Release);
    }

    fn has_active_hold(&self) -> bool {
        self.active_snapshot_pins.load(Ordering::Acquire) > 0
            || self.active_fork_bindings.load(Ordering::Acquire) > 0
    }

    fn apply_delta(&self, delta: HistoryRetentionDelta) {
        if delta.is_zero() {
            return;
        }
        apply_counter_delta(&self.active_snapshot_pins, delta.snapshot_pins);
        apply_counter_delta(&self.active_fork_bindings, delta.fork_bindings);
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    fn set_after_retention_apply_before_state_hook(&self, hook: TestHook) {
        *self
            .after_retention_apply_before_state
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn run_after_retention_apply_before_state_hook(&self) {
        let hook = self
            .after_retention_apply_before_state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_before_ordinary_planning_fence_hook(&self, hook: TestHook) {
        *self
            .before_ordinary_planning_fence
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn run_before_ordinary_planning_fence_hook(&self) {
        let hook = self
            .before_ordinary_planning_fence
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

fn apply_counter_delta(counter: &AtomicU64, delta: i64) {
    if delta > 0 {
        counter.fetch_add(delta as u64, Ordering::AcqRel);
    } else if delta < 0 {
        let decrement = delta.unsigned_abs();
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(decrement))
        });
    }
}

impl HoltMetadataStore {
    pub fn open_memory() -> Result<Self, MetadataError> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        let mut config = TreeConfig::new(path.as_ref());
        // A successful metadata RPC is a durability promise. Holt's default
        // async WAL mode leaves a small ACK-to-flusher window in which SIGKILL
        // can discard a committed namespace change or restore journal state.
        config.durability = Durability::Wal { sync: true };
        Self::open(config)
    }

    pub fn open(config: TreeConfig) -> Result<Self, MetadataError> {
        let db = DB::open(config).map_err(to_backend_error)?;
        let recovered = recover_history_retention_state(&db)?;
        let store = Self {
            db,
            stats: Arc::new(HoltMetadataStoreCounters::default()),
            history_retention: Arc::new(HistoryRetentionState::new(recovered)),
        };
        // Do not create trees in a brand-new database: checkpoint installation
        // requires a pristine DB. Existing stores, however, are migrated before
        // they can serve reads.
        match store.db.open_tree(HISTORY_TREE) {
            Ok(_) => store.ensure_history_key_index()?,
            Err(HoltError::TreeNotFound { .. }) => {}
            Err(err) => return Err(to_backend_error(err)),
        }
        Ok(store)
    }

    pub fn checkpoint(&self) -> Result<(), MetadataError> {
        self.db.checkpoint().map_err(to_backend_error)?;
        self.reclaim_unreachable_storage()?;
        Ok(())
    }

    pub fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        self.db
            .export_checkpoint()
            .map(CheckpointImage::into_bytes)
            .map_err(to_backend_error)
    }

    pub fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        let checkpoint = CheckpointImage::from_bytes(image.to_vec());
        checkpoint.validate().map_err(to_backend_error)?;
        let _fence = self
            .history_retention
            .planning_fence
            .write()
            .unwrap_or_else(|err| err.into_inner());
        self.db
            .install_checkpoint(&checkpoint)
            .map_err(to_backend_error)?;
        self.ensure_history_key_index()?;
        self.history_retention
            .install_recovered(recover_history_retention_state(&self.db)?);
        Ok(())
    }

    pub fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        self.db.gc().map_err(to_backend_error)
    }

    fn current_tree(&self, family: RecordFamily) -> Result<Tree, MetadataError> {
        self.db
            .open_or_create_tree(current_tree_name(family))
            .map_err(to_backend_error)
    }

    fn history_tree(&self) -> Result<Tree, MetadataError> {
        self.db
            .open_or_create_tree(HISTORY_TREE)
            .map_err(to_backend_error)
    }

    fn history_key_index_tree(&self) -> Result<Tree, MetadataError> {
        self.db
            .open_or_create_tree(HISTORY_KEY_INDEX_TREE)
            .map_err(to_backend_error)
    }

    fn ensure_metadata_trees(&self) -> Result<(), MetadataError> {
        for name in METADATA_TREE_NAMES {
            self.db
                .open_or_create_tree(name)
                .map_err(to_backend_error)?;
        }
        Ok(())
    }

    /// Build the derived history candidate index for stores created before the
    /// index existed. Each batch persists both its entries and the last consumed
    /// history key, so reopening after a crash resumes without rescanning earlier
    /// batches. The completion marker makes the steady-state check one point read.
    fn ensure_history_key_index(&self) -> Result<(), MetadataError> {
        let history = self.history_tree()?;
        let index = self.history_key_index_tree()?;
        if index
            .get(HISTORY_INDEX_COMPLETE_KEY)
            .map_err(to_backend_error)?
            .is_some()
        {
            return Ok(());
        }

        loop {
            let progress = index
                .get(HISTORY_INDEX_PROGRESS_KEY)
                .map_err(to_backend_error)?;
            let mut range = history.range();
            if let Some(progress) = progress.as_deref() {
                range = range.start_after(progress);
            }

            let mut updates = BTreeMap::<Vec<u8>, u64>::new();
            let mut last_key = None;
            let mut scanned = 0_usize;
            for entry in range {
                let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? else {
                    continue;
                };
                let index_key = history_index_key_from_record_key(&key)?;
                let (version, _) = decode_current_value(&value)?;
                updates
                    .entry(index_key)
                    .and_modify(|latest| *latest = (*latest).max(version.get()))
                    .or_insert(version.get());
                last_key = Some(key);
                scanned += 1;
                if scanned >= HISTORY_INDEX_BACKFILL_BATCH_SIZE {
                    break;
                }
            }

            let Some(last_key) = last_key else {
                self.db
                    .atomic(|batch| {
                        batch.put(
                            HISTORY_KEY_INDEX_TREE,
                            HISTORY_INDEX_COMPLETE_KEY,
                            HISTORY_INDEX_COMPLETE_VALUE,
                        );
                        batch.delete(HISTORY_KEY_INDEX_TREE, HISTORY_INDEX_PROGRESS_KEY);
                    })
                    .map_err(to_backend_error)?;
                return Ok(());
            };

            for (key, latest) in &mut updates {
                if let Some(existing) = index.get(key).map_err(to_backend_error)? {
                    *latest = (*latest).max(decode_history_index_version(&existing)?);
                }
            }
            self.db
                .atomic(|batch| {
                    for (key, latest) in &updates {
                        batch.put(HISTORY_KEY_INDEX_TREE, key, &latest.to_be_bytes());
                    }
                    batch.put(
                        HISTORY_KEY_INDEX_TREE,
                        HISTORY_INDEX_PROGRESS_KEY,
                        &last_key,
                    );
                })
                .map_err(to_backend_error)?;
        }
    }

    /// Snapshot scans enumerate the union of current keys and keys with retained
    /// history, then run the same point-visibility resolver used by snapshot get.
    /// `start_after` and `limit` are deliberately applied to visible rows rather
    /// than candidate rows: an invisible post-snapshot key must not consume a
    /// page slot.
    fn scan_snapshot_rows(
        &self,
        request: &ScanRequest,
    ) -> Result<(Vec<ScanItem>, usize), MetadataError> {
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let mut out = Vec::new();
        let visited = self.visit_snapshot_visible(request, |item| {
            out.push(item);
            out.len() < limit
        })?;
        Ok((out, visited))
    }

    /// Merge the ordered current and retained-history candidate streams without
    /// materializing either prefix. Equal keys are emitted once, preferring the
    /// captured current value. The visitor controls early termination, which lets
    /// normal and delimited snapshot pages stop as soon as their visible limit is
    /// satisfied.
    fn visit_snapshot_visible(
        &self,
        request: &ScanRequest,
        mut visitor: impl FnMut(ScanItem) -> bool,
    ) -> Result<usize, MetadataError> {
        self.ensure_history_key_index()?;
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let index = self.history_key_index_tree()?;

        let current_snapshot = current
            .snapshot(&request.prefix)
            .map_err(to_backend_error)?;
        let mut current_range = current_snapshot.view().range();
        if let Some(start_after) = request.start_after.as_deref() {
            current_range = current_range.start_after(start_after);
        }
        let mut current_iter = current_range.into_iter();

        let index_prefix = history_index_prefix(request.family, &request.prefix);
        let index_snapshot = index.snapshot(&index_prefix).map_err(to_backend_error)?;
        let mut index_range = index_snapshot.view().range();
        let index_start_after;
        if let Some(start_after) = request.start_after.as_deref() {
            index_start_after = history_index_key(request.family, start_after);
            index_range = index_range.start_after(&index_start_after);
        }
        let mut index_iter = index_range.into_iter();

        let context = VisibleReadContext {
            family: request.family,
            version: request.version,
            purpose: request.purpose,
            history: &history,
            stats: &self.stats,
        };
        let mut current_head = next_current_candidate(&mut current_iter)?;
        let mut index_head = next_history_index_candidate(request.family, &mut index_iter)?;
        let mut visited = 0_usize;
        while current_head.is_some() || index_head.is_some() {
            let (key, current_value) = match (&current_head, &index_head) {
                (Some((current_key, _)), Some(index_key)) => match current_key.cmp(index_key) {
                    std::cmp::Ordering::Less => {
                        let (key, value) = current_head.take().expect("head is present");
                        current_head = next_current_candidate(&mut current_iter)?;
                        (key, Some(value))
                    }
                    std::cmp::Ordering::Greater => {
                        let key = index_head.take().expect("head is present");
                        index_head = next_history_index_candidate(request.family, &mut index_iter)?;
                        (key, None)
                    }
                    std::cmp::Ordering::Equal => {
                        let (key, value) = current_head.take().expect("head is present");
                        index_head.take();
                        current_head = next_current_candidate(&mut current_iter)?;
                        index_head = next_history_index_candidate(request.family, &mut index_iter)?;
                        (key, Some(value))
                    }
                },
                (Some(_), None) => {
                    let (key, value) = current_head.take().expect("head is present");
                    current_head = next_current_candidate(&mut current_iter)?;
                    (key, Some(value))
                }
                (None, Some(_)) => {
                    let key = index_head.take().expect("head is present");
                    index_head = next_history_index_candidate(request.family, &mut index_iter)?;
                    (key, None)
                }
                (None, None) => break,
            };
            visited += 1;
            if let Some((version, value)) =
                decode_visible_value(&key, current_value.as_deref(), &context)?
            {
                if !visitor(ScanItem {
                    key,
                    value: Value(value),
                    version,
                }) {
                    break;
                }
            }
        }
        Ok(visited)
    }

    fn current_live_record(
        &self,
        family: RecordFamily,
        key: &[u8],
    ) -> Result<Option<(RecordVersion, Version, Vec<u8>)>, MetadataError> {
        let Some(record) = self.current_record(family, key)? else {
            return Ok(None);
        };
        Ok(record
            .value
            .map(|value| (record.record_version, record.metadata_version, value)))
    }

    fn current_record(
        &self,
        family: RecordFamily,
        key: &[u8],
    ) -> Result<Option<CurrentRecord>, MetadataError> {
        let Some(record) = self
            .current_tree(family)?
            .get_record(key)
            .map_err(to_backend_error)?
        else {
            return Ok(None);
        };
        let (version, value) = decode_current_value(&record.value)?;
        Ok(Some(CurrentRecord {
            record_version: record.version,
            metadata_version: version,
            value,
        }))
    }
}

impl MetadataCheckpointStore for HoltMetadataStore {
    fn checkpoint(&self) -> Result<(), MetadataError> {
        HoltMetadataStore::checkpoint(self)
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        HoltMetadataStore::export_checkpoint_image(self)
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        HoltMetadataStore::install_checkpoint_image(self, image)
    }

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        HoltMetadataStore::reclaim_unreachable_storage(self)
    }
}

impl MetadataStoreStatsProvider for HoltMetadataStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        let mut stats = self.stats.snapshot();
        stats.active_snapshot_pin_total = self
            .history_retention
            .active_snapshot_pins
            .load(Ordering::Acquire);
        stats
    }
}

impl MetadataStore for HoltMetadataStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.stats.get_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_get_purpose(purpose);
        read_visible(
            &self.current_tree(family)?,
            family,
            key,
            version,
            purpose,
            &self.history_tree()?,
            &self.stats,
        )
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_scan_purpose(request.purpose);
        if request.purpose == ReadPurpose::Snapshot {
            let (out, visited) = self.scan_snapshot_rows(&request)?;
            self.stats
                .scan_key_visited_total
                .fetch_add(visited as u64, Ordering::Relaxed);
            self.stats
                .scan_key_returned_total
                .fetch_add(out.len() as u64, Ordering::Relaxed);
            return Ok(out);
        }
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let start_after = request.start_after.as_deref();
        let mut out = Vec::new();
        let mut visited_total = 0_u64;
        let mut returned_total = 0_u64;
        let context = VisibleReadContext {
            family: request.family,
            version: request.version,
            purpose: request.purpose,
            history: &history,
            stats: &self.stats,
        };

        let snapshot = current
            .snapshot(&request.prefix)
            .map_err(to_backend_error)?;
        let mut range = snapshot.view().range();
        if let Some(start_after) = start_after {
            range = range.start_after(start_after);
        }
        for entry in range {
            let outcome = push_visible_scan_item(entry, &context, &mut out, limit, start_after)?;
            visited_total += outcome.visited as u64;
            returned_total += outcome.returned as u64;
            if outcome.done {
                break;
            }
        }
        self.stats
            .scan_key_visited_total
            .fetch_add(visited_total, Ordering::Relaxed);
        self.stats
            .scan_key_returned_total
            .fetch_add(returned_total, Ordering::Relaxed);
        Ok(out)
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_scan_purpose(request.purpose);
        if request.purpose == ReadPurpose::Snapshot {
            let limit = if request.limit == 0 {
                usize::MAX
            } else {
                request.limit
            };
            let mut out = Vec::new();
            let visited = self.visit_snapshot_visible(
                &ScanRequest {
                    family: request.family,
                    prefix: request.prefix.clone(),
                    // A delimited cursor names the collapsed key/common prefix,
                    // not an underlying raw key. Apply it after collapsing so
                    // children below an already-returned prefix are not repeated.
                    start_after: None,
                    version: request.version,
                    limit: 0,
                    purpose: request.purpose,
                },
                |item| {
                    let suffix = item.key.get(request.prefix.len()..).unwrap_or_default();
                    let collapsed = if let Some(offset) =
                        suffix.iter().position(|byte| *byte == request.delimiter)
                    {
                        DelimitedScanItem::CommonPrefix(
                            item.key[..request.prefix.len() + offset + 1].to_vec(),
                        )
                    } else {
                        DelimitedScanItem::Key(item)
                    };
                    let marker = match &collapsed {
                        DelimitedScanItem::Key(item) => item.key.as_slice(),
                        DelimitedScanItem::CommonPrefix(prefix) => prefix.as_slice(),
                    };
                    if request
                        .start_after
                        .as_deref()
                        .is_some_and(|start_after| marker <= start_after)
                        || out.last() == Some(&collapsed)
                    {
                        return true;
                    }
                    out.push(collapsed);
                    out.len() < limit
                },
            )?;
            self.stats
                .scan_key_visited_total
                .fetch_add(visited as u64, Ordering::Relaxed);
            self.stats
                .scan_key_returned_total
                .fetch_add(out.len() as u64, Ordering::Relaxed);
            return Ok(out);
        }
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let start_after = request.start_after.as_deref();
        let mut out = Vec::new();
        let mut visited_total = 0_u64;
        let mut returned_total = 0_u64;
        let context = VisibleReadContext {
            family: request.family,
            version: request.version,
            purpose: request.purpose,
            history: &history,
            stats: &self.stats,
        };

        let snapshot = current
            .snapshot(&request.prefix)
            .map_err(to_backend_error)?;
        let mut range = snapshot.view().range().delimiter(request.delimiter);
        if let Some(start_after) = start_after {
            range = range.start_after(start_after);
        }
        for entry in range {
            let outcome =
                push_visible_delimited_scan_item(entry, &context, &mut out, limit, start_after)?;
            visited_total += outcome.visited as u64;
            returned_total += outcome.returned as u64;
            if outcome.done {
                break;
            }
        }
        self.stats
            .scan_key_visited_total
            .fetch_add(visited_total, Ordering::Relaxed);
        self.stats
            .scan_key_returned_total
            .fetch_add(returned_total, Ordering::Relaxed);
        Ok(out)
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_scan_purpose(request.purpose);
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let mut range = current.range_keys().prefix(&request.prefix);
        if let Some(start_after) = request.start_after.as_deref() {
            range = range.start_after(start_after);
        }
        let mut out = Vec::new();
        let outcome = range
            .visit_with_outcome(limit, |entry| {
                if let KeyRangeEntryRef::Key { key, .. } = entry {
                    out.push(key.to_vec());
                }
                Ok(())
            })
            .map_err(to_backend_error)?;
        self.stats.record_key_scan_outcome(outcome);
        Ok(out)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        if commands.iter().any(command_mutates_history_retention) {
            let _fence = self
                .history_retention
                .planning_fence
                .write()
                .unwrap_or_else(|err| err.into_inner());
            return self.commit_independent_batch_fenced(commands);
        }
        let _fence = self
            .history_retention
            .planning_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        self.commit_independent_batch_fenced(commands)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        if command_mutates_history_retention(&command) {
            let _fence = self
                .history_retention
                .planning_fence
                .write()
                .unwrap_or_else(|err| err.into_inner());
            return self.commit_metadata_fenced(command);
        }
        #[cfg(test)]
        self.history_retention
            .run_before_ordinary_planning_fence_hook();
        let _fence = self
            .history_retention
            .planning_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        self.commit_metadata_fenced(command)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.current_tree(RecordFamily::CommandDedupe)?
            .get(request_id)
            .map_err(to_backend_error)?
            .as_deref()
            .map(decode_dedupe_result)
            .transpose()
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        Ok(self.history_retention.epoch.load(Ordering::Acquire))
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        let _fence = self
            .history_retention
            .planning_fence
            .write()
            .unwrap_or_else(|err| err.into_inner());
        if self.history_retention.epoch.load(Ordering::Acquire) != request.retention_epoch {
            return Err(MetadataError::PredicateFailed);
        }
        for attempt in 0..HISTORY_PRUNE_CONFLICT_RETRIES {
            match self.prune_history_once(request) {
                Err(MetadataError::PredicateFailed)
                    if attempt + 1 < HISTORY_PRUNE_CONFLICT_RETRIES =>
                {
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("history prune retry loop always returns")
    }
}

impl HoltMetadataStore {
    fn commit_independent_batch_fenced(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        let mut results = vec![None; commands.len()];
        let mut pending = Vec::new();
        for (index, command) in commands.iter().cloned().enumerate() {
            if pending.iter().any(|pending: &PendingPlannedCommand| {
                metadata_commands_conflict(&pending.command, &command)
            }) {
                self.commit_pending_batch(&mut pending, &mut results);
            }
            match self.prepare_command(&command) {
                Ok(PreparedCommand::DedupeHit(result)) => results[index] = Some(Ok(result)),
                Ok(PreparedCommand::Planned(plan)) => {
                    if !plan.history_retention_delta.is_zero() || plan.adds_history_retention {
                        self.commit_pending_batch(&mut pending, &mut results);
                        results[index] = Some(self.commit_planned_command_fenced(command, plan));
                    } else {
                        pending.push(PendingPlannedCommand {
                            index,
                            command,
                            plan,
                        });
                    }
                }
                Err(err) => results[index] = Some(Err(err)),
            }
        }
        self.commit_pending_batch(&mut pending, &mut results);
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(MetadataError::Backend(
                        "holt batch result was not recorded".to_owned(),
                    ))
                })
            })
            .collect()
    }

    fn commit_metadata_fenced(
        &self,
        command: MetadataCommand,
    ) -> Result<CommitResult, MetadataError> {
        match self.prepare_command(&command)? {
            PreparedCommand::DedupeHit(result) => Ok(result),
            PreparedCommand::Planned(plan) => self.commit_planned_command_fenced(command, plan),
        }
    }

    fn prune_history_once(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.ensure_history_key_index()?;
        let remove_limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let history = self.history_tree()?;
        let index = self.history_key_index_tree()?;
        let mut outcome = HistoryPruneOutcome::default();
        let mut records_to_remove = Vec::new();
        let mut current_prefix = Vec::new();
        let mut kept_anchor_below_floor = false;

        for entry in history.range() {
            let RangeEntry::Key {
                key,
                value,
                version: record_version,
            } = entry.map_err(to_backend_error)?
            else {
                continue;
            };
            let prefix = history_user_prefix(&key)?;
            if prefix != current_prefix.as_slice() {
                current_prefix.clear();
                current_prefix.extend_from_slice(prefix);
                kept_anchor_below_floor = false;
            }
            let (version, _) = decode_current_value(&value)?;
            outcome.scanned += 1;
            let remove = match request.retain_from {
                None => true,
                Some(floor) if version >= floor => {
                    outcome.retained_by_snapshots += 1;
                    false
                }
                Some(_) if !kept_anchor_below_floor => {
                    kept_anchor_below_floor = true;
                    outcome.retained_by_snapshots += 1;
                    false
                }
                Some(_) => true,
            };
            if remove {
                records_to_remove.push((key, record_version));
                if records_to_remove.len() >= remove_limit {
                    break;
                }
            }
        }

        outcome.removed =
            self.delete_history_records_and_update_index(&history, &index, &records_to_remove)?;
        Ok(outcome)
    }

    fn prepare_command(&self, command: &MetadataCommand) -> Result<PreparedCommand, MetadataError> {
        command.validate()?;
        if let Some(result) = self.dedupe_result(&command.request_id)? {
            self.stats.dedupe_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(PreparedCommand::DedupeHit(result));
        }

        let prepare_start = Instant::now();
        let plan = self.plan_command(command)?;
        self.stats.commit_prepare_ns_total.fetch_add(
            prepare_start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(PreparedCommand::Planned(plan))
    }

    fn dedupe_result(&self, request_id: &[u8]) -> Result<Option<CommitResult>, MetadataError> {
        self.current_tree(RecordFamily::CommandDedupe)?
            .get(request_id)
            .map_err(to_backend_error)?
            .as_deref()
            .map(decode_dedupe_result)
            .transpose()
    }

    fn commit_pending_batch(
        &self,
        pending: &mut Vec<PendingPlannedCommand>,
        results: &mut [Option<Result<CommitResult, MetadataError>>],
    ) {
        if pending.is_empty() {
            return;
        }
        let batch = std::mem::take(pending);
        match self.commit_planned_batch(&batch) {
            Ok(Some(committed)) => {
                for (item, result) in batch.into_iter().zip(committed) {
                    results[item.index] = Some(Ok(result));
                }
            }
            Ok(None) => {
                for item in batch {
                    results[item.index] = Some(self.commit_metadata_fenced(item.command));
                }
            }
            Err(err) => {
                for item in batch {
                    results[item.index] = Some(Err(err.clone()));
                }
            }
        }
    }

    fn commit_planned_batch(
        &self,
        batch_items: &[PendingPlannedCommand],
    ) -> Result<Option<Vec<CommitResult>>, MetadataError> {
        self.ensure_metadata_trees()?;
        self.ensure_history_key_index()?;
        let stats = batch_items
            .iter()
            .map(|item| planned_command_stats(&item.command, &item.plan))
            .collect::<Vec<_>>();
        let atomic_start = Instant::now();
        let committed = self
            .db
            .atomic(|batch| {
                for (item, stats) in batch_items.iter().zip(&stats) {
                    enqueue_planned_command(batch, &item.command, &item.plan, &stats.dedupe_result);
                }
            })
            .map_err(to_backend_error)?;
        self.stats
            .record_atomic_apply(batch_items.len(), atomic_start.elapsed());
        if !committed {
            return Ok(None);
        }

        for stats in &stats {
            self.record_committed_stats(stats);
        }
        Ok(Some(stats.into_iter().map(|stats| stats.result).collect()))
    }

    fn plan_command(&self, command: &MetadataCommand) -> Result<CommandPlan, MetadataError> {
        let mut mutations = command
            .mutations
            .iter()
            .cloned()
            .map(|mutation| PlannedMutation {
                mutation,
                guard: MutationGuard::Always,
            })
            .collect::<Vec<_>>();
        let mut version_guards = Vec::new();
        let mut prefix_empty_guards = Vec::new();

        for predicate in &command.predicates {
            match predicate.predicate {
                Predicate::Exists => {
                    let (record_version, _, _) = self
                        .current_live_record(predicate.family, &predicate.key)?
                        .ok_or(MetadataError::PredicateFailed)?;
                    apply_record_version_guard(
                        &mut mutations,
                        &mut version_guards,
                        predicate.family,
                        &predicate.key,
                        record_version,
                    )?;
                }
                Predicate::NotExists => {
                    let index = mutation_index(&mutations, predicate.family, &predicate.key)
                        .ok_or(MetadataError::PredicateFailed)?;
                    if mutations[index].mutation.op != MutationOp::Put {
                        return Err(MetadataError::PredicateFailed);
                    }
                    match self.current_record(predicate.family, &predicate.key)? {
                        None => {
                            set_mutation_guard(&mut mutations[index], MutationGuard::PutIfAbsent)?;
                        }
                        Some(record) if record.value.is_none() => {
                            set_mutation_guard(
                                &mut mutations[index],
                                MutationGuard::CompareAndPut(record.record_version),
                            )?;
                        }
                        Some(_) => return Err(MetadataError::PredicateFailed),
                    }
                }
                Predicate::PrefixEmpty => {
                    let count = self
                        .current_tree(predicate.family)?
                        .prefix_count(&predicate.key, 1)
                        .map_err(to_backend_error)?;
                    self.stats.record_key_scan_outcome(KeyScanOutcome {
                        stats: count.stats,
                        cache_hit: count.cache_hit,
                    });
                    if count.count > 0 {
                        return Err(MetadataError::PredicateFailed);
                    }
                    prefix_empty_guards.push(PrefixEmptyGuard {
                        family: predicate.family,
                        prefix: predicate.key.clone(),
                    });
                }
                Predicate::VersionEquals(expected) => {
                    let (record_version, actual, _) = self
                        .current_live_record(predicate.family, &predicate.key)?
                        .ok_or(MetadataError::PredicateFailed)?;
                    if actual != expected {
                        return Err(MetadataError::PredicateFailed);
                    }
                    apply_record_version_guard(
                        &mut mutations,
                        &mut version_guards,
                        predicate.family,
                        &predicate.key,
                        record_version,
                    )?;
                }
            }
        }

        let retain_history = self.has_active_history_retention();
        let (history_retention_delta, adds_history_retention) =
            self.history_retention_delta(&mutations)?;
        let mut history_records = Vec::new();
        if retain_history {
            for planned in &mutations {
                if !family_requires_history(planned.mutation.family) {
                    continue;
                }
                if let Some(current) = self
                    .current_tree(planned.mutation.family)?
                    .get(&planned.mutation.key)
                    .map_err(to_backend_error)?
                {
                    history_records.push((
                        planned.mutation.family,
                        planned.mutation.key.clone(),
                        current,
                    ));
                }
            }
        }

        Ok(CommandPlan {
            mutations,
            history_records,
            version_guards,
            prefix_empty_guards,
            retain_history,
            history_retention_delta,
            adds_history_retention,
        })
    }

    fn commit_planned_command_fenced(
        &self,
        command: MetadataCommand,
        plan: CommandPlan,
    ) -> Result<CommitResult, MetadataError> {
        if plan.adds_history_retention
            && command.read_version.get()
                < self
                    .history_retention
                    .max_applied_commit_version
                    .load(Ordering::Acquire)
        {
            return Err(MetadataError::PredicateFailed);
        }
        self.ensure_metadata_trees()?;
        self.ensure_history_key_index()?;
        let stats = planned_command_stats(&command, &plan);
        let atomic_start = Instant::now();
        let committed = self
            .db
            .atomic(|batch| {
                enqueue_planned_command(batch, &command, &plan, &stats.dedupe_result);
            })
            .map_err(to_backend_error)?;
        self.stats.record_atomic_apply(1, atomic_start.elapsed());
        if !committed {
            if let Some(encoded) = self.dedupe_result(&command.request_id)? {
                self.stats.dedupe_hit_total.fetch_add(1, Ordering::Relaxed);
                return Ok(encoded);
            }
            return Err(MetadataError::PredicateFailed);
        }

        #[cfg(test)]
        if plan.adds_history_retention {
            self.history_retention
                .run_after_retention_apply_before_state_hook();
        }
        self.record_committed_stats(&stats);
        Ok(stats.result)
    }

    fn record_committed_stats(&self, stats: &PlannedCommandStats) {
        self.history_retention
            .apply_delta(stats.history_retention_delta);
        if stats.advances_history_version {
            self.history_retention
                .max_applied_commit_version
                .fetch_max(stats.result.commit_version.get(), Ordering::AcqRel);
        }
        self.stats.commit_total.fetch_add(1, Ordering::Relaxed);
        self.stats
            .predicate_total
            .fetch_add(stats.predicate_count, Ordering::Relaxed);
        self.stats
            .prefix_empty_predicate_total
            .fetch_add(stats.prefix_empty_predicate_count, Ordering::Relaxed);
        self.stats
            .current_put_total
            .fetch_add(stats.current_put_count, Ordering::Relaxed);
        self.stats
            .current_delete_total
            .fetch_add(stats.current_delete_count, Ordering::Relaxed);
        self.stats
            .history_write_total
            .fetch_add(stats.history_write_count, Ordering::Relaxed);
        self.stats
            .watch_write_total
            .fetch_add(stats.watch_write_count, Ordering::Relaxed);
        self.stats
            .dedupe_write_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn history_retention_delta(
        &self,
        mutations: &[PlannedMutation],
    ) -> Result<(HistoryRetentionDelta, bool), MetadataError> {
        let mut final_states = Vec::<(RecordFamily, Vec<u8>, bool)>::new();
        for planned in mutations
            .iter()
            .filter(|planned| is_history_retention_family(planned.mutation.family))
        {
            let new_active = planned.mutation.op == MutationOp::Put;
            if let Some((_, _, active)) = final_states.iter_mut().find(|(family, key, _)| {
                *family == planned.mutation.family
                    && key.as_slice() == planned.mutation.key.as_slice()
            }) {
                *active = new_active;
            } else {
                final_states.push((
                    planned.mutation.family,
                    planned.mutation.key.clone(),
                    new_active,
                ));
            }
        }

        let mut delta = HistoryRetentionDelta::default();
        let mut adds_history_retention = false;
        for (family, key, new_active) in final_states {
            let old_active = self
                .current_record(family, &key)?
                .is_some_and(|record| record.value.is_some());
            adds_history_retention |= !old_active && new_active;
            let family_delta = i64::from(new_active) - i64::from(old_active);
            match family {
                RecordFamily::Snapshot => delta.snapshot_pins += family_delta,
                RecordFamily::ForkBinding => delta.fork_bindings += family_delta,
                _ => unreachable!("retention family filter accepts only durable holds"),
            }
        }
        Ok((delta, adds_history_retention))
    }

    fn has_active_history_retention(&self) -> bool {
        self.history_retention.has_active_hold()
    }

    fn delete_history_records_and_update_index(
        &self,
        history: &Tree,
        index: &Tree,
        records: &[(Vec<u8>, RecordVersion)],
    ) -> Result<usize, MetadataError> {
        if records.is_empty() {
            return Ok(0);
        }

        let plan = self.plan_history_record_deletion(history, index, records)?;
        self.apply_history_record_deletion(&plan)?;
        Ok(plan.records.len())
    }

    fn plan_history_record_deletion(
        &self,
        history: &Tree,
        index: &Tree,
        records: &[(Vec<u8>, RecordVersion)],
    ) -> Result<HistoryPruneDeletePlan, MetadataError> {
        let removed_keys = records
            .iter()
            .map(|(key, _)| key.as_slice())
            .collect::<HashSet<_>>();
        let affected_prefixes = records
            .iter()
            .map(|(key, _)| history_user_prefix(key).map(Vec::from))
            .collect::<Result<HashSet<_>, _>>()?;
        let mut index_updates = Vec::with_capacity(affected_prefixes.len());
        for history_prefix in affected_prefixes {
            let index_key = history_index_key_from_user_prefix(&history_prefix)?;
            let index_record = index
                .get_record(&index_key)
                .map_err(to_backend_error)?
                .ok_or_else(|| {
                    MetadataError::Backend(
                        "history index is missing a retained history key".to_owned(),
                    )
                })?;
            let mut latest_retained = None::<u64>;
            for entry in history.range().prefix(&history_prefix) {
                let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? else {
                    continue;
                };
                if removed_keys.contains(key.as_slice()) {
                    continue;
                }
                let (version, _) = decode_current_value(&value)?;
                latest_retained =
                    Some(latest_retained.map_or(version.get(), |latest| latest.max(version.get())));
            }
            index_updates.push(HistoryIndexPruneUpdate {
                key: index_key,
                record_version: index_record.version,
                latest_retained,
            });
        }

        Ok(HistoryPruneDeletePlan {
            records: records.to_vec(),
            index_updates,
        })
    }

    fn apply_history_record_deletion(
        &self,
        plan: &HistoryPruneDeletePlan,
    ) -> Result<(), MetadataError> {
        let committed = self
            .db
            .atomic(|batch| {
                for (key, version) in &plan.records {
                    batch.delete_if_version(HISTORY_TREE, key, *version);
                }
                for update in &plan.index_updates {
                    if let Some(latest_retained) = update.latest_retained {
                        batch.compare_and_put(
                            HISTORY_KEY_INDEX_TREE,
                            &update.key,
                            update.record_version,
                            &latest_retained.to_be_bytes(),
                        );
                    } else {
                        batch.delete_if_version(
                            HISTORY_KEY_INDEX_TREE,
                            &update.key,
                            update.record_version,
                        );
                    }
                }
            })
            .map_err(to_backend_error)?;
        if !committed {
            return Err(MetadataError::PredicateFailed);
        }
        Ok(())
    }
}

impl HoltMetadataStoreCounters {
    fn record_get_purpose(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.get_user_strong_total,
            ReadPurpose::WritePlanLocal => &self.get_write_plan_local_total,
            ReadPurpose::Snapshot => &self.get_snapshot_total,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_scan_purpose(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.scan_user_strong_total,
            ReadPurpose::WritePlanLocal => &self.scan_write_plan_local_total,
            ReadPurpose::Snapshot => &self.scan_snapshot_total,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_key_scan_outcome(&self, outcome: KeyScanOutcome) {
        if outcome.cache_hit {
            self.scan_cache_hit_total.fetch_add(1, Ordering::Relaxed);
        }
        self.scan_key_visited_total
            .fetch_add(outcome.stats.visited, Ordering::Relaxed);
        self.scan_key_returned_total.fetch_add(
            outcome.stats.returned + outcome.stats.rollup,
            Ordering::Relaxed,
        );
    }

    fn record_atomic_apply(&self, command_count: usize, elapsed: std::time::Duration) {
        self.atomic_apply_total.fetch_add(1, Ordering::Relaxed);
        self.atomic_apply_command_total
            .fetch_add(command_count as u64, Ordering::Relaxed);
        self.atomic_apply_max_batch
            .fetch_max(command_count as u64, Ordering::Relaxed);
        self.atomic_apply_ns_total.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> MetadataStoreStats {
        MetadataStoreStats {
            get_total: self.get_total.load(Ordering::Relaxed),
            get_user_strong_total: self.get_user_strong_total.load(Ordering::Relaxed),
            get_write_plan_local_total: self.get_write_plan_local_total.load(Ordering::Relaxed),
            get_snapshot_total: self.get_snapshot_total.load(Ordering::Relaxed),
            scan_total: self.scan_total.load(Ordering::Relaxed),
            scan_user_strong_total: self.scan_user_strong_total.load(Ordering::Relaxed),
            scan_write_plan_local_total: self.scan_write_plan_local_total.load(Ordering::Relaxed),
            scan_snapshot_total: self.scan_snapshot_total.load(Ordering::Relaxed),
            scan_cache_hit_total: self.scan_cache_hit_total.load(Ordering::Relaxed),
            scan_key_visited_total: self.scan_key_visited_total.load(Ordering::Relaxed),
            scan_key_returned_total: self.scan_key_returned_total.load(Ordering::Relaxed),
            history_lookup_total: self.history_lookup_total.load(Ordering::Relaxed),
            active_snapshot_pin_total: 0,
            commit_total: self.commit_total.load(Ordering::Relaxed),
            dedupe_hit_total: self.dedupe_hit_total.load(Ordering::Relaxed),
            predicate_total: self.predicate_total.load(Ordering::Relaxed),
            prefix_empty_predicate_total: self.prefix_empty_predicate_total.load(Ordering::Relaxed),
            current_put_total: self.current_put_total.load(Ordering::Relaxed),
            current_delete_total: self.current_delete_total.load(Ordering::Relaxed),
            history_write_total: self.history_write_total.load(Ordering::Relaxed),
            watch_write_total: self.watch_write_total.load(Ordering::Relaxed),
            dedupe_write_total: self.dedupe_write_total.load(Ordering::Relaxed),
            commit_prepare_ns_total: self.commit_prepare_ns_total.load(Ordering::Relaxed),
            atomic_apply_total: self.atomic_apply_total.load(Ordering::Relaxed),
            atomic_apply_command_total: self.atomic_apply_command_total.load(Ordering::Relaxed),
            atomic_apply_max_batch: self.atomic_apply_max_batch.load(Ordering::Relaxed),
            atomic_apply_ns_total: self.atomic_apply_ns_total.load(Ordering::Relaxed),
        }
    }
}

fn recover_history_retention_state(
    db: &DB,
) -> Result<RecoveredHistoryRetentionState, MetadataError> {
    Ok(RecoveredHistoryRetentionState {
        active_snapshot_pins: count_active_records(db, SNAPSHOT_CURRENT_TREE)?,
        active_fork_bindings: count_active_records(db, FORK_BINDING_CURRENT_TREE)?,
    })
}

fn count_active_records(db: &DB, tree_name: &str) -> Result<u64, MetadataError> {
    let tree = match db.open_tree(tree_name) {
        Ok(tree) => tree,
        Err(HoltError::TreeNotFound { .. }) => return Ok(0),
        Err(err) => return Err(to_backend_error(err)),
    };
    let mut total = 0_u64;
    for entry in tree.range() {
        let RangeEntry::Key { value, .. } = entry.map_err(to_backend_error)? else {
            continue;
        };
        if decode_current_value(&value)?.1.is_some() {
            total += 1;
        }
    }
    Ok(total)
}

fn is_history_retention_family(family: RecordFamily) -> bool {
    matches!(family, RecordFamily::Snapshot | RecordFamily::ForkBinding)
}

fn command_mutates_history_retention(command: &MetadataCommand) -> bool {
    command
        .mutations
        .iter()
        .any(|mutation| is_history_retention_family(mutation.family))
}

fn command_advances_history_version(command: &MetadataCommand) -> bool {
    command.mutations.iter().any(|mutation| {
        family_requires_history(mutation.family) && !is_history_retention_family(mutation.family)
    })
}

fn read_visible(
    current: &Tree,
    family: RecordFamily,
    key: &[u8],
    version: Version,
    purpose: ReadPurpose,
    history: &Tree,
    stats: &HoltMetadataStoreCounters,
) -> Result<Option<ReadItem>, MetadataError> {
    let encoded = current.get(key).map_err(to_backend_error)?;
    let context = VisibleReadContext {
        family,
        version,
        purpose,
        history,
        stats,
    };
    decode_visible_value(key, encoded.as_deref(), &context).map(|value| {
        value.map(|(version, bytes)| ReadItem {
            value: Value(bytes),
            version,
        })
    })
}

fn planned_command_stats(command: &MetadataCommand, plan: &CommandPlan) -> PlannedCommandStats {
    let history_tombstone_count = plan
        .mutations
        .iter()
        .filter(|planned| {
            planned.mutation.op == MutationOp::Delete
                && plan.retain_history
                && family_requires_history(planned.mutation.family)
        })
        .count() as u64;
    let result = CommitResult {
        commit_version: command.commit_version,
        applied_mutations: plan.mutations.len(),
        watch_events: command.watch.len(),
    };
    let dedupe_result = encode_dedupe_result(&result);
    PlannedCommandStats {
        predicate_count: command.predicates.len() as u64,
        prefix_empty_predicate_count: command
            .predicates
            .iter()
            .filter(|predicate| matches!(predicate.predicate, Predicate::PrefixEmpty))
            .count() as u64,
        current_put_count: plan
            .mutations
            .iter()
            .filter(|planned| planned.mutation.op == MutationOp::Put)
            .count() as u64,
        current_delete_count: plan
            .mutations
            .iter()
            .filter(|planned| planned.mutation.op == MutationOp::Delete)
            .count() as u64,
        history_write_count: plan.history_records.len() as u64 + history_tombstone_count,
        watch_write_count: command.watch.len() as u64,
        history_retention_delta: plan.history_retention_delta,
        advances_history_version: command_advances_history_version(command),
        result,
        dedupe_result,
    }
}

fn enqueue_planned_command(
    batch: &mut DBAtomicBatch,
    command: &MetadataCommand,
    plan: &CommandPlan,
    dedupe_result: &[u8],
) {
    for (family, key, current) in &plan.history_records {
        if let Ok((old_version, _)) = decode_current_value(current) {
            batch.put(
                HISTORY_TREE,
                &history_key(*family, key, old_version.get()),
                current,
            );
            batch.put(
                HISTORY_KEY_INDEX_TREE,
                &history_index_key(*family, key),
                &old_version.get().to_be_bytes(),
            );
        }
    }
    for planned in &plan.mutations {
        if planned.mutation.op == MutationOp::Delete
            && plan.retain_history
            && family_requires_history(planned.mutation.family)
        {
            batch.put(
                HISTORY_TREE,
                &history_key(
                    planned.mutation.family,
                    &planned.mutation.key,
                    command.commit_version.get(),
                ),
                &encode_tombstone_value(command.commit_version),
            );
            batch.put(
                HISTORY_KEY_INDEX_TREE,
                &history_index_key(planned.mutation.family, &planned.mutation.key),
                &command.commit_version.get().to_be_bytes(),
            );
        }
    }
    for guard in &plan.version_guards {
        batch.assert_version(current_tree_name(guard.family), &guard.key, guard.version);
    }
    for guard in &plan.prefix_empty_guards {
        batch.assert_prefix_empty(current_tree_name(guard.family), &guard.prefix);
    }
    for planned in &plan.mutations {
        match (planned.mutation.op, planned.guard) {
            (MutationOp::Put, MutationGuard::Always) => {
                let value = planned
                    .mutation
                    .value
                    .as_ref()
                    .expect("validated put mutation has a value");
                batch.put(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    &encode_current_value(command.commit_version, &value.0),
                );
            }
            (MutationOp::Put, MutationGuard::PutIfAbsent) => {
                let value = planned
                    .mutation
                    .value
                    .as_ref()
                    .expect("validated put mutation has a value");
                batch.put_if_absent(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    &encode_current_value(command.commit_version, &value.0),
                );
            }
            (MutationOp::Put, MutationGuard::CompareAndPut(version)) => {
                let value = planned
                    .mutation
                    .value
                    .as_ref()
                    .expect("validated put mutation has a value");
                batch.compare_and_put(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    version,
                    &encode_current_value(command.commit_version, &value.0),
                );
            }
            (MutationOp::Put, MutationGuard::DeleteIfVersion(_)) => {
                unreachable!("put mutation cannot use delete guard")
            }
            (MutationOp::Delete, MutationGuard::Always) => {
                batch.delete(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                );
            }
            (MutationOp::Delete, MutationGuard::DeleteIfVersion(version)) => {
                batch.delete_if_version(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    version,
                );
            }
            (MutationOp::Delete, MutationGuard::PutIfAbsent)
            | (MutationOp::Delete, MutationGuard::CompareAndPut(_)) => {
                unreachable!("delete mutation cannot use put guard")
            }
        }
    }
    for (ordinal, event) in command.watch.iter().enumerate() {
        let key = watch_event_key(&event.key, command.commit_version, ordinal);
        batch.put(
            WATCH_CURRENT_TREE,
            &key,
            &encode_current_value(command.commit_version, &event.event),
        );
    }
    batch.put_if_absent(
        current_tree_name(RecordFamily::CommandDedupe),
        &command.request_id,
        dedupe_result,
    );
}

fn decode_visible_value(
    key: &[u8],
    encoded: Option<&[u8]>,
    context: &VisibleReadContext<'_>,
) -> Result<Option<(Version, Vec<u8>)>, MetadataError> {
    if let Some(encoded) = encoded {
        let (current_version, current_value) = decode_current_value(encoded)?;
        if current_version <= context.version {
            return Ok(current_value.map(|value| (current_version, value)));
        }
    } else if context.purpose != ReadPurpose::Snapshot {
        return Ok(None);
    }
    context
        .stats
        .history_lookup_total
        .fetch_add(1, Ordering::Relaxed);
    for entry in context
        .history
        .range()
        .prefix(&history_prefix(context.family, key))
    {
        let RangeEntry::Key { value, .. } = entry.map_err(to_backend_error)? else {
            continue;
        };
        let (history_version, history_value) = decode_current_value(&value)?;
        if history_version <= context.version {
            return Ok(history_value.map(|value| (history_version, value)));
        }
    }
    Ok(None)
}

struct VisibleReadContext<'a> {
    family: RecordFamily,
    version: Version,
    purpose: ReadPurpose,
    history: &'a Tree,
    stats: &'a HoltMetadataStoreCounters,
}

struct ScanPushOutcome {
    done: bool,
    visited: usize,
    returned: usize,
}

fn push_visible_scan_item(
    entry: Result<RangeEntry, holt::Error>,
    context: &VisibleReadContext<'_>,
    out: &mut Vec<ScanItem>,
    limit: usize,
    start_after: Option<&[u8]>,
) -> Result<ScanPushOutcome, MetadataError> {
    let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? else {
        return Ok(ScanPushOutcome {
            done: false,
            visited: 0,
            returned: 0,
        });
    };
    if start_after.is_some_and(|start_after| key.as_slice() <= start_after) {
        return Ok(ScanPushOutcome {
            done: false,
            visited: 1,
            returned: 0,
        });
    }
    let mut returned = 0_usize;
    if let Some((commit, visible)) = decode_visible_value(&key, Some(&value), context)? {
        out.push(ScanItem {
            key,
            value: Value(visible),
            version: commit,
        });
        returned = 1;
    }
    Ok(ScanPushOutcome {
        done: out.len() >= limit,
        visited: 1,
        returned,
    })
}

fn push_visible_delimited_scan_item(
    entry: Result<RangeEntry, holt::Error>,
    context: &VisibleReadContext<'_>,
    out: &mut Vec<DelimitedScanItem>,
    limit: usize,
    start_after: Option<&[u8]>,
) -> Result<ScanPushOutcome, MetadataError> {
    match entry.map_err(to_backend_error)? {
        RangeEntry::Key { key, value, .. } => {
            if start_after.is_some_and(|start_after| key.as_slice() <= start_after) {
                return Ok(ScanPushOutcome {
                    done: false,
                    visited: 1,
                    returned: 0,
                });
            }
            let mut returned = 0_usize;
            if let Some((commit, visible)) = decode_visible_value(&key, Some(&value), context)? {
                out.push(DelimitedScanItem::Key(ScanItem {
                    key,
                    value: Value(visible),
                    version: commit,
                }));
                returned = 1;
            }
            Ok(ScanPushOutcome {
                done: out.len() >= limit,
                visited: 1,
                returned,
            })
        }
        RangeEntry::CommonPrefix(prefix) => {
            if start_after.is_some_and(|start_after| prefix.as_slice() <= start_after) {
                return Ok(ScanPushOutcome {
                    done: false,
                    visited: 1,
                    returned: 0,
                });
            }
            out.push(DelimitedScanItem::CommonPrefix(prefix));
            Ok(ScanPushOutcome {
                done: out.len() >= limit,
                visited: 1,
                returned: 1,
            })
        }
        _ => Ok(ScanPushOutcome {
            done: false,
            visited: 0,
            returned: 0,
        }),
    }
}

fn next_current_candidate(
    iterator: &mut impl Iterator<Item = Result<RangeEntry, HoltError>>,
) -> Result<Option<CurrentScanCandidate>, MetadataError> {
    for entry in iterator {
        if let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? {
            return Ok(Some((key, value)));
        }
    }
    Ok(None)
}

fn next_history_index_candidate(
    family: RecordFamily,
    iterator: &mut impl Iterator<Item = Result<RangeEntry, HoltError>>,
) -> Result<Option<Vec<u8>>, MetadataError> {
    for entry in iterator {
        if let RangeEntry::Key { key, .. } = entry.map_err(to_backend_error)? {
            return history_index_user_key(family, &key).map(Some);
        }
    }
    Ok(None)
}

fn history_index_key_from_record_key(history_record_key: &[u8]) -> Result<Vec<u8>, MetadataError> {
    let user_prefix = history_user_prefix(history_record_key)?;
    history_index_key_from_user_prefix(user_prefix)
}

fn history_index_key_from_user_prefix(user_prefix: &[u8]) -> Result<Vec<u8>, MetadataError> {
    if user_prefix.len() < 5 {
        return Err(MetadataError::Backend(
            "history key has a truncated family/user-key header".to_owned(),
        ));
    }
    let user_key_len = u32::from_be_bytes(
        user_prefix[1..5]
            .try_into()
            .expect("history user-key length has fixed width"),
    ) as usize;
    if user_prefix.len() != 5 + user_key_len {
        return Err(MetadataError::Backend(
            "history key user-key length does not match its payload".to_owned(),
        ));
    }
    let mut out = Vec::with_capacity(1 + user_key_len);
    out.push(user_prefix[0]);
    out.extend_from_slice(&user_prefix[5..]);
    Ok(out)
}

fn history_index_user_key(
    family: RecordFamily,
    index_key: &[u8],
) -> Result<Vec<u8>, MetadataError> {
    let expected_prefix = history_index_prefix(family, &[]);
    let Some(user_key) = index_key.strip_prefix(expected_prefix.as_slice()) else {
        return Err(MetadataError::Backend(
            "history index key belongs to the wrong record family".to_owned(),
        ));
    };
    Ok(user_key.to_vec())
}

fn decode_history_index_version(encoded: &[u8]) -> Result<u64, MetadataError> {
    let bytes: [u8; 8] = encoded
        .try_into()
        .map_err(|_| MetadataError::Backend("history index version is malformed".to_owned()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn to_backend_error(err: impl std::fmt::Display) -> MetadataError {
    MetadataError::Backend(err.to_string())
}

fn mutation_index(
    mutations: &[PlannedMutation],
    family: RecordFamily,
    key: &[u8],
) -> Option<usize> {
    mutations
        .iter()
        .position(|planned| planned.mutation.family == family && planned.mutation.key == key)
}

fn apply_record_version_guard(
    mutations: &mut [PlannedMutation],
    version_guards: &mut Vec<VersionGuard>,
    family: RecordFamily,
    key: &[u8],
    record_version: RecordVersion,
) -> Result<(), MetadataError> {
    if let Some(index) = mutation_index(mutations, family, key) {
        let guard = match mutations[index].mutation.op {
            MutationOp::Put => MutationGuard::CompareAndPut(record_version),
            MutationOp::Delete => MutationGuard::DeleteIfVersion(record_version),
        };
        set_mutation_guard(&mut mutations[index], guard)?;
    } else {
        version_guards.push(VersionGuard {
            family,
            key: key.to_vec(),
            version: record_version,
        });
    }
    Ok(())
}

fn set_mutation_guard(
    planned: &mut PlannedMutation,
    guard: MutationGuard,
) -> Result<(), MetadataError> {
    match (planned.guard, guard) {
        (MutationGuard::Always, guard) => {
            planned.guard = guard;
            Ok(())
        }
        (current, requested) if current == requested => Ok(()),
        _ => Err(MetadataError::Backend(
            "metadata command has conflicting mutation guards".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests;
