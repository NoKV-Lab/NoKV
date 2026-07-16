use super::*;
use crate::command::{MetadataCheckpointStore, ReadItem, ScanItem};
use crate::holtstore::HoltMetadataStore;
use crate::layout::object_gc_quarantine_prefix;
use crate::{MetadataLogEntry, MetadataLogSegment, METADATA_LOG_ZERO_DIGEST};
use nokv_object::{MemoryObjectStore, ObjectBytes};
use nokv_types::{AdvisoryLockKind, AdvisoryLockRequest};
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, Condvar};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct RestoreInverseProbeStore {
    inner: HoltMetadataStore,
    inverse_prefix: Vec<u8>,
    inverse_gets: Arc<AtomicU64>,
}

impl RestoreInverseProbeStore {
    fn new(mount: MountId) -> Self {
        let inverse_prefix = restore::restore_control_keyspaces(mount)
            .into_iter()
            .find_map(|(name, prefix)| (name == "staging_inode_inverse").then_some(prefix))
            .expect("restore staging inverse keyspace is registered");
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            inverse_prefix,
            inverse_gets: Arc::new(AtomicU64::new(0)),
        }
    }

    fn inverse_gets(&self) -> u64 {
        self.inverse_gets.load(Ordering::SeqCst)
    }
}

impl MetadataStore for RestoreInverseProbeStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        if family == RecordFamily::System && key.starts_with(&self.inverse_prefix) {
            self.inverse_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

#[derive(Clone)]
struct RestoreBorrowerManifestReadFaultStore {
    inner: HoltMetadataStore,
    fail_key: Arc<Mutex<Option<Vec<u8>>>>,
    failures: Arc<AtomicU64>,
}

impl RestoreBorrowerManifestReadFaultStore {
    fn new() -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            fail_key: Arc::new(Mutex::new(None)),
            failures: Arc::new(AtomicU64::new(0)),
        }
    }

    fn fail_next_restore_manifest_read(&self, key: Vec<u8>) {
        let replaced = self.fail_key.lock().unwrap().replace(key);
        assert!(
            replaced.is_none(),
            "a restore manifest read fault is already armed"
        );
    }

    fn failures(&self) -> u64 {
        self.failures.load(Ordering::SeqCst)
    }
}

impl MetadataStore for RestoreBorrowerManifestReadFaultStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        let should_fail =
            if family == RecordFamily::ChunkManifest && purpose == ReadPurpose::RestoreStaging {
                let mut fail_key = self.fail_key.lock().unwrap();
                if fail_key.as_deref() == Some(key) {
                    fail_key.take();
                    true
                } else {
                    false
                }
            } else {
                false
            };
        if should_fail {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(MetadataError::Backend(
                "injected restore borrower manifest read failure".to_owned(),
            ));
        }
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

/// Pause one non-empty exact-reference inverse scan after Holt has produced its
/// snapshot. A concurrent release can then attempt to delete that inverse and
/// its canonical owner, reproducing the scan/point-get TOCTOU seen by the live
/// crash matrix.
#[derive(Clone)]
struct RestoreBaseInverseScanBarrierStore {
    inner: HoltMetadataStore,
    inverse_prefix: Vec<u8>,
    armed: Arc<AtomicBool>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl RestoreBaseInverseScanBarrierStore {
    fn new(mount: MountId) -> Self {
        let inverse_prefix = restore::restore_control_keyspaces(mount)
            .into_iter()
            .find_map(|(name, prefix)| (name == "base_inverse").then_some(prefix))
            .expect("restore base inverse keyspace is registered");
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            inverse_prefix,
            armed: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn wait_until_blocked(&self) {
        self.entered.wait();
    }

    fn release_blocked(&self) {
        self.release.wait();
    }
}

impl MetadataStore for RestoreBaseInverseScanBarrierStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        let matches = request.family == RecordFamily::System
            && request.prefix.starts_with(&self.inverse_prefix);
        let rows = self.inner.scan(request)?;
        if matches
            && !rows.is_empty()
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.entered.wait();
            self.release.wait();
        }
        Ok(rows)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for RestoreBaseInverseScanBarrierStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

#[derive(Clone)]
struct RestorePostApplyFaultStore<M = HoltMetadataStore> {
    inner: M,
    state: Arc<Mutex<RestorePostApplyFaultState>>,
}

#[derive(Default)]
struct RestorePostApplyFaultState {
    fault: Option<RestorePostApplyFault>,
    fail_checkpoint_after_install: bool,
    applied_by_request_id: BTreeMap<Vec<u8>, usize>,
    applied_mutation_counts: Vec<(Vec<u8>, usize)>,
}

struct RestorePostApplyFault {
    request_prefix: Vec<u8>,
    remaining_matches: usize,
}

impl RestorePostApplyFaultStore<HoltMetadataStore> {
    fn new() -> Self {
        Self::wrapping(HoltMetadataStore::open_memory().unwrap())
    }
}

impl<M> RestorePostApplyFaultStore<M> {
    fn wrapping(inner: M) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(RestorePostApplyFaultState::default())),
        }
    }

    fn fail_after_apply(&self, request_prefix: &[u8], zero_based_match: usize) {
        self.state.lock().unwrap().fault = Some(RestorePostApplyFault {
            request_prefix: request_prefix.to_vec(),
            remaining_matches: zero_based_match,
        });
    }

    fn clear_fault(&self) {
        self.state.lock().unwrap().fault = None;
    }

    fn fail_checkpoint_after_install(&self) {
        self.state.lock().unwrap().fail_checkpoint_after_install = true;
    }

    fn applied_with_prefix(&self, request_prefix: &[u8]) -> usize {
        self.state
            .lock()
            .unwrap()
            .applied_by_request_id
            .iter()
            .filter_map(|(request_id, count)| {
                request_id.starts_with(request_prefix).then_some(*count)
            })
            .sum()
    }

    fn applied_mutation_counts_with_prefix(&self, request_prefix: &[u8]) -> Vec<usize> {
        self.state
            .lock()
            .unwrap()
            .applied_mutation_counts
            .iter()
            .filter_map(|(request_id, count)| {
                request_id.starts_with(request_prefix).then_some(*count)
            })
            .collect()
    }

    fn applied_request_counts_with_prefix(&self, request_prefix: &[u8]) -> Vec<usize> {
        self.state
            .lock()
            .unwrap()
            .applied_by_request_id
            .iter()
            .filter_map(|(request_id, count)| {
                request_id.starts_with(request_prefix).then_some(*count)
            })
            .collect()
    }
}

impl<M> MetadataStore for RestorePostApplyFaultStore<M>
where
    M: MetadataStore,
{
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn scan_delimited(
        &self,
        request: crate::command::DelimitedScanRequest,
    ) -> Result<Vec<crate::command::DelimitedScanItem>, MetadataError> {
        self.inner.scan_delimited(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let request_id = command.request_id.clone();
        let mutation_count = command.mutations.len();
        let result = self.inner.commit_metadata(command);
        if result.is_ok() {
            let should_fail = {
                let mut state = self.state.lock().unwrap();
                *state
                    .applied_by_request_id
                    .entry(request_id.clone())
                    .or_default() += 1;
                state
                    .applied_mutation_counts
                    .push((request_id.clone(), mutation_count));
                match state.fault.as_mut() {
                    Some(fault) if request_id.starts_with(&fault.request_prefix) => {
                        if fault.remaining_matches == 0 {
                            state.fault = None;
                            true
                        } else {
                            fault.remaining_matches -= 1;
                            false
                        }
                    }
                    _ => false,
                }
            };
            if should_fail {
                return Err(MetadataError::Backend(
                    "injected restore crash after durable apply".to_owned(),
                ));
            }
        }
        result
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl<M> MetadataCheckpointStore for RestorePostApplyFaultStore<M>
where
    M: MetadataCheckpointStore,
{
    fn checkpoint(&self) -> Result<(), MetadataError> {
        self.inner.checkpoint()
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        self.inner.export_checkpoint_image()
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        self.inner.install_checkpoint_image(image)?;
        if std::mem::take(&mut self.state.lock().unwrap().fail_checkpoint_after_install) {
            return Err(MetadataError::Backend(
                "injected checkpoint install error after apply".to_owned(),
            ));
        }
        Ok(())
    }

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        self.inner.reclaim_unreachable_storage()
    }
}

impl<M> MetadataStoreStatsProvider for RestorePostApplyFaultStore<M>
where
    M: MetadataStoreStatsProvider,
{
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

/// Holt intentionally caps one value at 64 KiB, while the MetadataStore
/// contract permits other backends to expose larger manifest, restore-control,
/// and object-GC values. Keep those test-only values out-of-line while
/// delegating physical keys and versions to Holt so release CAS, ACK
/// reconciliation, and reopen exercise the real service state machine.
#[derive(Clone)]
struct OversizedManifestStore {
    inner: RestorePostApplyFaultStore,
    manifests: Arc<Mutex<BTreeMap<Vec<u8>, ScanItem>>>,
    system_rows: Arc<Mutex<BTreeMap<Vec<u8>, ScanItem>>>,
    gc_rows: Arc<Mutex<BTreeMap<Vec<u8>, ScanItem>>>,
}

impl OversizedManifestStore {
    fn new() -> Self {
        Self {
            inner: RestorePostApplyFaultStore::new(),
            manifests: Arc::new(Mutex::new(BTreeMap::new())),
            system_rows: Arc::new(Mutex::new(BTreeMap::new())),
            gc_rows: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn install_manifest(&self, item: ScanItem) {
        self.manifests
            .lock()
            .unwrap()
            .insert(item.key.clone(), item);
    }

    fn fail_after_apply(&self, request_prefix: &[u8], zero_based_match: usize) {
        self.inner
            .fail_after_apply(request_prefix, zero_based_match);
    }

    fn clear_fault(&self) {
        self.inner.clear_fault();
    }

    fn applied_with_prefix(&self, request_prefix: &[u8]) -> usize {
        self.inner.applied_with_prefix(request_prefix)
    }

    fn applied_mutation_counts_with_prefix(&self, request_prefix: &[u8]) -> Vec<usize> {
        self.inner
            .applied_mutation_counts_with_prefix(request_prefix)
    }

    fn applied_request_counts_with_prefix(&self, request_prefix: &[u8]) -> Vec<usize> {
        self.inner
            .applied_request_counts_with_prefix(request_prefix)
    }
}

impl MetadataStore for OversizedManifestStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        if family == RecordFamily::ChunkManifest {
            if let Some(item) = self.manifests.lock().unwrap().get(key) {
                if item.version <= version {
                    return Ok(Some(ReadItem {
                        value: item.value.clone(),
                        version: item.version,
                    }));
                }
            }
        } else if family == RecordFamily::System {
            if let Some(item) = self.system_rows.lock().unwrap().get(key) {
                if item.version <= version {
                    return Ok(Some(ReadItem {
                        value: item.value.clone(),
                        version: item.version,
                    }));
                }
            }
        } else if family == RecordFamily::Gc {
            if let Some(item) = self.gc_rows.lock().unwrap().get(key) {
                if item.version <= version {
                    return Ok(Some(ReadItem {
                        value: item.value.clone(),
                        version: item.version,
                    }));
                }
            }
        }
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        let mut rows = self.inner.scan(request.clone())?;
        if request.family == RecordFamily::ChunkManifest {
            for manifest in self.manifests.lock().unwrap().values() {
                if manifest.version <= request.version {
                    if let Some(row) = rows.iter_mut().find(|row| row.key == manifest.key) {
                        *row = manifest.clone();
                    }
                }
            }
        } else if request.family == RecordFamily::System {
            for item in self.system_rows.lock().unwrap().values() {
                if item.version <= request.version {
                    if let Some(row) = rows.iter_mut().find(|row| row.key == item.key) {
                        *row = item.clone();
                    }
                }
            }
        } else if request.family == RecordFamily::Gc {
            for item in self.gc_rows.lock().unwrap().values() {
                if item.version <= request.version {
                    if let Some(row) = rows.iter_mut().find(|row| row.key == item.key) {
                        *row = item.clone();
                    }
                }
            }
        }
        Ok(rows)
    }

    fn commit_metadata(&self, mut command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let request_id = command.request_id.clone();
        let mut oversized_manifests = Vec::new();
        let mut oversized_system_rows = Vec::new();
        let mut oversized_gc_rows = Vec::new();
        let mut deleted = Vec::new();
        for mutation in &mut command.mutations {
            if !matches!(
                mutation.family,
                RecordFamily::ChunkManifest | RecordFamily::System | RecordFamily::Gc
            ) {
                continue;
            }
            match mutation.op {
                MutationOp::Put
                    if mutation
                        .value
                        .as_ref()
                        .is_some_and(|value| value.0.len() > u16::MAX as usize) =>
                {
                    let value = mutation
                        .value
                        .replace(Value(Vec::new()))
                        .expect("oversized Put has a value");
                    let item = ScanItem {
                        key: mutation.key.clone(),
                        value,
                        version: command.commit_version,
                    };
                    if mutation.family == RecordFamily::ChunkManifest {
                        oversized_manifests.push(item);
                    } else if mutation.family == RecordFamily::System {
                        oversized_system_rows.push(item);
                    } else {
                        oversized_gc_rows.push(item);
                    }
                }
                MutationOp::Delete => deleted.push((mutation.family, mutation.key.clone())),
                MutationOp::Put => {}
            }
        }
        let result = self.inner.commit_metadata(command);
        let applied = result.is_ok()
            || self
                .inner
                .committed_request_result(&request_id)
                .ok()
                .flatten()
                .is_some();
        if applied {
            let mut manifests = self.manifests.lock().unwrap();
            let mut system_rows = self.system_rows.lock().unwrap();
            let mut gc_rows = self.gc_rows.lock().unwrap();
            for (family, key) in deleted {
                if family == RecordFamily::ChunkManifest {
                    manifests.remove(&key);
                } else if family == RecordFamily::System {
                    system_rows.remove(&key);
                } else {
                    gc_rows.remove(&key);
                }
            }
            for item in oversized_manifests {
                manifests.insert(item.key.clone(), item);
            }
            for item in oversized_system_rows {
                system_rows.insert(item.key.clone(), item);
            }
            for item in oversized_gc_rows {
                gc_rows.insert(item.key.clone(), item);
            }
        }
        result
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataCheckpointStore for OversizedManifestStore {
    fn checkpoint(&self) -> Result<(), MetadataError> {
        self.inner.checkpoint()
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        self.inner.export_checkpoint_image()
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        self.inner.install_checkpoint_image(image)
    }

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        self.inner.reclaim_unreachable_storage()
    }
}

impl MetadataStoreStatsProvider for OversizedManifestStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

/// Keep one PathIndex row out-of-line so preflight can cover metadata backends
/// whose value limit is larger than Holt's 64 KiB test backend.
#[derive(Clone)]
struct OversizedPathIndexStore {
    inner: RestorePostApplyFaultStore,
    rows: Arc<Mutex<Vec<(RecordFamily, ScanItem)>>>,
}

impl OversizedPathIndexStore {
    fn new() -> Self {
        Self {
            inner: RestorePostApplyFaultStore::new(),
            rows: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn install_row(&self, row: ScanItem) {
        self.install_override(RecordFamily::PathIndex, row);
    }

    fn install_override(&self, family: RecordFamily, row: ScanItem) {
        let mut rows = self.rows.lock().unwrap();
        if let Some((_, existing)) = rows.iter_mut().find(|(candidate_family, candidate)| {
            *candidate_family == family && candidate.key == row.key
        }) {
            *existing = row;
        } else {
            rows.push((family, row));
        }
    }

    fn applied_with_prefix(&self, request_prefix: &[u8]) -> usize {
        self.inner.applied_with_prefix(request_prefix)
    }
}

impl MetadataStore for OversizedPathIndexStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        if let Some((_, row)) = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|(candidate_family, row)| {
                *candidate_family == family && row.key == key && row.version <= version
            })
        {
            return Ok(Some(ReadItem {
                value: row.value.clone(),
                version: row.version,
            }));
        }
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        let mut rows = self.inner.scan(request.clone())?;
        for (family, oversized) in self.rows.lock().unwrap().iter() {
            if *family == request.family && oversized.version <= request.version {
                if let Some(row) = rows.iter_mut().find(|row| row.key == oversized.key) {
                    *row = oversized.clone();
                }
            }
        }
        Ok(rows)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for OversizedPathIndexStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

#[derive(Clone)]
struct SnapshotCommitBarrierStore {
    inner: HoltMetadataStore,
    kind: CommandKind,
    request_prefix: Option<Vec<u8>>,
    rejected_kind: Option<CommandKind>,
    remaining: Arc<AtomicU64>,
    predicate_failures: Arc<AtomicU64>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl SnapshotCommitBarrierStore {
    fn new(kind: CommandKind, blocked_commits: u64, parties: usize) -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            kind,
            request_prefix: None,
            rejected_kind: None,
            remaining: Arc::new(AtomicU64::new(blocked_commits)),
            predicate_failures: Arc::new(AtomicU64::new(0)),
            entered: Arc::new(Barrier::new(parties)),
            release: Arc::new(Barrier::new(parties)),
        }
    }

    fn wait_until_blocked(&self) {
        self.entered.wait();
    }

    fn arm(&self, blocked_commits: u64) {
        self.remaining.store(blocked_commits, Ordering::SeqCst);
    }

    fn predicate_failures(&self) -> u64 {
        self.predicate_failures.load(Ordering::SeqCst)
    }

    fn rejecting(mut self, kind: CommandKind) -> Self {
        self.rejected_kind = Some(kind);
        self
    }

    fn matching_request_prefix(mut self, prefix: &[u8]) -> Self {
        self.request_prefix = Some(prefix.to_vec());
        self
    }

    fn release_blocked(&self) {
        self.release.wait();
    }
}

impl MetadataStore for SnapshotCommitBarrierStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        if command.kind == self.kind
            && self
                .request_prefix
                .as_ref()
                .is_none_or(|prefix| command.request_id.starts_with(prefix))
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            self.entered.wait();
            self.release.wait();
        }
        if self.rejected_kind == Some(command.kind)
            && self
                .request_prefix
                .as_ref()
                .is_none_or(|prefix| command.request_id.starts_with(prefix))
        {
            return Err(MetadataError::PredicateFailed);
        }
        let result = self.inner.commit_metadata(command);
        if matches!(&result, Err(MetadataError::PredicateFailed)) {
            self.predicate_failures.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for SnapshotCommitBarrierStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

/// Metadata store wrapper that pauses one matching command only after Holt has
/// durably applied it. This deterministically exposes apply-vs-log ordering
/// races without relying on scheduler timing.
#[derive(Clone)]
struct PostCommitBarrierStore {
    inner: HoltMetadataStore,
    request_id: Vec<u8>,
    remaining: Arc<AtomicU64>,
    applied: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl PostCommitBarrierStore {
    fn new(request_id: &[u8]) -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            request_id: request_id.to_vec(),
            remaining: Arc::new(AtomicU64::new(1)),
            applied: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        }
    }

    fn wait_until_applied(&self) {
        self.applied.wait();
    }

    fn release_after_apply(&self) {
        self.release.wait();
    }
}

impl MetadataStore for PostCommitBarrierStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let should_block = command.request_id == self.request_id
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
        let result = self.inner.commit_metadata(command);
        if should_block && result.is_ok() {
            self.applied.wait();
            self.release.wait();
        }
        result
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataCheckpointStore for PostCommitBarrierStore {
    fn checkpoint(&self) -> Result<(), MetadataError> {
        self.inner.checkpoint()
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        self.inner.export_checkpoint_image()
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        self.inner.install_checkpoint_image(image)
    }

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        self.inner.reclaim_unreachable_storage()
    }
}

impl MetadataStoreStatsProvider for PostCommitBarrierStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

#[derive(Clone)]
struct PostCommitErrorStore {
    inner: HoltMetadataStore,
    kind: CommandKind,
    remaining: Arc<AtomicU64>,
    readback_failures: Arc<AtomicU64>,
    readback_mismatches: Arc<AtomicU64>,
    batch_backend_indices: Arc<Mutex<Option<Vec<usize>>>>,
}

impl PostCommitErrorStore {
    fn new(kind: CommandKind) -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            kind,
            remaining: Arc::new(AtomicU64::new(1)),
            readback_failures: Arc::new(AtomicU64::new(0)),
            readback_mismatches: Arc::new(AtomicU64::new(0)),
            batch_backend_indices: Arc::new(Mutex::new(None)),
        }
    }

    fn new_disarmed(kind: CommandKind) -> Self {
        let store = Self::new(kind);
        store.remaining.store(0, Ordering::SeqCst);
        store
    }

    fn arm(&self) {
        self.remaining.store(1, Ordering::SeqCst);
    }

    fn fail_next_readbacks(&self, count: u64) {
        self.readback_failures.store(count, Ordering::SeqCst);
    }

    fn clear_readback_failures(&self) {
        self.readback_failures.store(0, Ordering::SeqCst);
    }

    fn mismatch_next_readbacks(&self, count: u64) {
        self.readback_mismatches.store(count, Ordering::SeqCst);
    }

    fn clear_readback_mismatches(&self) {
        self.readback_mismatches.store(0, Ordering::SeqCst);
    }

    fn fail_next_batch_results(&self, indices: Vec<usize>) {
        *self.batch_backend_indices.lock().unwrap() = Some(indices);
    }
}

impl MetadataStore for PostCommitErrorStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let kind = command.kind;
        let result = self.inner.commit_metadata(command);
        if kind == self.kind
            && result.is_ok()
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(MetadataError::Backend(
                "injected journal acknowledgement failure".to_owned(),
            ));
        }
        result
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        let mut results = self.inner.commit_independent_batch(commands);
        if let Some(indices) = self.batch_backend_indices.lock().unwrap().take() {
            for index in indices {
                if matches!(results.get(index), Some(Ok(_))) {
                    results[index] = Err(MetadataError::Backend(
                        "injected batch journal acknowledgement failure".to_owned(),
                    ));
                }
            }
        }
        results
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        if self
            .readback_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(MetadataError::Backend(
                "injected authoritative readback failure".to_owned(),
            ));
        }
        let result = self.inner.committed_request_result(request_id)?;
        if self
            .readback_mismatches
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Ok(result.map(|mut result| {
                result.applied_mutations = result.applied_mutations.saturating_add(1);
                result
            }));
        }
        Ok(result)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for PostCommitErrorStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

impl MetadataCheckpointStore for PostCommitErrorStore {
    fn checkpoint(&self) -> Result<(), MetadataError> {
        self.inner.checkpoint()
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        self.inner.export_checkpoint_image()
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        self.inner.install_checkpoint_image(image)
    }

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        self.inner.reclaim_unreachable_storage()
    }
}

#[derive(Clone)]
struct SnapshotPredicateOnceStore {
    inner: HoltMetadataStore,
    remaining_failures: Arc<AtomicU64>,
}

impl SnapshotPredicateOnceStore {
    fn new() -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            remaining_failures: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl MetadataStore for SnapshotPredicateOnceStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        if command.kind == CommandKind::SnapshotSubtree
            && self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(MetadataError::PredicateFailed);
        }
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for SnapshotPredicateOnceStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

#[derive(Clone)]
struct PurposeTrackingStore {
    inner: HoltMetadataStore,
    counts: Arc<PurposeCounts>,
}

#[derive(Default)]
struct PurposeCounts {
    user_strong_gets: AtomicU64,
    write_plan_gets: AtomicU64,
    snapshot_gets: AtomicU64,
    user_strong_scans: AtomicU64,
    write_plan_scans: AtomicU64,
    snapshot_scans: AtomicU64,
    dentry_scans: AtomicU64,
    unbounded_scans: AtomicU64,
    restore_index_unbounded_scans: AtomicU64,
    largest_bounded_scan: AtomicU64,
    largest_restore_index_scan: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PurposeCountSnapshot {
    user_strong_gets: u64,
    write_plan_gets: u64,
    snapshot_gets: u64,
    user_strong_scans: u64,
    write_plan_scans: u64,
    snapshot_scans: u64,
    dentry_scans: u64,
    unbounded_scans: u64,
    restore_index_unbounded_scans: u64,
    largest_bounded_scan: u64,
    largest_restore_index_scan: u64,
}

impl PurposeTrackingStore {
    fn new() -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            counts: Arc::new(PurposeCounts::default()),
        }
    }

    fn counts(&self) -> PurposeCountSnapshot {
        PurposeCountSnapshot {
            user_strong_gets: self.counts.user_strong_gets.load(Ordering::Relaxed),
            write_plan_gets: self.counts.write_plan_gets.load(Ordering::Relaxed),
            snapshot_gets: self.counts.snapshot_gets.load(Ordering::Relaxed),
            user_strong_scans: self.counts.user_strong_scans.load(Ordering::Relaxed),
            write_plan_scans: self.counts.write_plan_scans.load(Ordering::Relaxed),
            snapshot_scans: self.counts.snapshot_scans.load(Ordering::Relaxed),
            dentry_scans: self.counts.dentry_scans.load(Ordering::Relaxed),
            unbounded_scans: self.counts.unbounded_scans.load(Ordering::Relaxed),
            restore_index_unbounded_scans: self
                .counts
                .restore_index_unbounded_scans
                .load(Ordering::Relaxed),
            largest_bounded_scan: self.counts.largest_bounded_scan.load(Ordering::Relaxed),
            largest_restore_index_scan: self
                .counts
                .largest_restore_index_scan
                .load(Ordering::Relaxed),
        }
    }

    fn reset_scan_bounds(&self) {
        self.counts.unbounded_scans.store(0, Ordering::Relaxed);
        self.counts
            .restore_index_unbounded_scans
            .store(0, Ordering::Relaxed);
        self.counts.largest_bounded_scan.store(0, Ordering::Relaxed);
        self.counts
            .largest_restore_index_scan
            .store(0, Ordering::Relaxed);
    }

    fn record_get(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.counts.user_strong_gets,
            ReadPurpose::WritePlanLocal | ReadPurpose::RestoreStaging => {
                &self.counts.write_plan_gets
            }
            ReadPurpose::Snapshot => &self.counts.snapshot_gets,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_scan(&self, family: RecordFamily, purpose: ReadPurpose, limit: usize) {
        match purpose {
            ReadPurpose::UserStrong => &self.counts.user_strong_scans,
            ReadPurpose::WritePlanLocal | ReadPurpose::RestoreStaging => {
                &self.counts.write_plan_scans
            }
            ReadPurpose::Snapshot => &self.counts.snapshot_scans,
        }
        .fetch_add(1, Ordering::Relaxed);
        if family == RecordFamily::Dentry {
            self.counts.dentry_scans.fetch_add(1, Ordering::Relaxed);
        }
        if limit == 0 {
            self.counts.unbounded_scans.fetch_add(1, Ordering::Relaxed);
            if matches!(
                family,
                RecordFamily::System | RecordFamily::PathIndex | RecordFamily::Dentry
            ) {
                self.counts
                    .restore_index_unbounded_scans
                    .fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.counts
                .largest_bounded_scan
                .fetch_max(limit as u64, Ordering::Relaxed);
            if matches!(
                family,
                RecordFamily::System | RecordFamily::PathIndex | RecordFamily::Dentry
            ) {
                self.counts
                    .largest_restore_index_scan
                    .fetch_max(limit as u64, Ordering::Relaxed);
            }
        }
    }
}

impl MetadataStore for PurposeTrackingStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.record_get(purpose);
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.record_scan(request.family, request.purpose, request.limit);
        self.inner.scan(request)
    }

    fn scan_delimited(
        &self,
        request: crate::command::DelimitedScanRequest,
    ) -> Result<Vec<crate::command::DelimitedScanItem>, MetadataError> {
        self.record_scan(request.family, request.purpose, request.limit);
        self.inner.scan_delimited(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for PurposeTrackingStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

#[derive(Clone)]
struct PausingObjectGcStore {
    inner: HoltMetadataStore,
    gate: Arc<(Mutex<PausingObjectGcState>, Condvar)>,
}

#[derive(Default)]
struct PausingObjectGcState {
    armed: bool,
    reached: bool,
    released: bool,
}

impl PausingObjectGcStore {
    fn new() -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            gate: Arc::new((Mutex::new(PausingObjectGcState::default()), Condvar::new())),
        }
    }

    fn arm(&self) {
        let (lock, _) = &*self.gate;
        *lock.lock().unwrap() = PausingObjectGcState {
            armed: true,
            reached: false,
            released: false,
        };
    }

    fn wait_until_reached(&self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !state.reached {
            let now = Instant::now();
            assert!(now < deadline, "timed out waiting for durable GC claim");
            let (next, timed_out) = changed.wait_timeout(state, deadline - now).unwrap();
            state = next;
            assert!(
                !timed_out.timed_out() || state.reached,
                "timed out waiting for durable GC claim"
            );
        }
    }

    fn release(&self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().unwrap();
        state.released = true;
        changed.notify_all();
    }

    fn pause_after_deleting_claim(&self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().unwrap();
        if !state.armed {
            return;
        }
        state.armed = false;
        state.reached = true;
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).unwrap();
        }
    }
}

impl MetadataStore for PausingObjectGcStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let deleting_claim = command.primary_family == RecordFamily::System
            && command.primary_key == object_gc_claim_key(MountId::new(1).unwrap())
            && command.mutations.iter().any(|mutation| {
                mutation.family == RecordFamily::System
                    && mutation.key == command.primary_key
                    && mutation
                        .value
                        .as_ref()
                        .is_some_and(|value| value.0.first() == Some(&2))
            });
        let result = self.inner.commit_metadata(command);
        if deleting_claim && result.is_ok() {
            self.pause_after_deleting_claim();
        }
        result
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

/// Simulates a remote DELETE that takes effect but loses its acknowledgement.
/// The retry observes the object as missing, matching an idempotent S3 DELETE.
#[derive(Clone)]
struct DeleteAckLostObjectStore {
    inner: MemoryObjectStore,
    lose_next_delete_ack: Arc<AtomicBool>,
    delete_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl DeleteAckLostObjectStore {
    fn new(inner: MemoryObjectStore) -> Self {
        Self {
            inner,
            lose_next_delete_ack: Arc::new(AtomicBool::new(true)),
            delete_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn delete_calls(&self) -> usize {
        self.delete_calls.load(Ordering::SeqCst)
    }
}

impl ObjectStore for DeleteAckLostObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<nokv_object::ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(
        &self,
        key: &ObjectKey,
        range: Option<nokv_object::ObjectRange>,
    ) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<nokv_object::ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        let deleted = self.inner.delete(key)?;
        if self.lose_next_delete_ack.swap(false, Ordering::SeqCst) {
            return Err(ObjectError::Backend(
                "injected lost DELETE acknowledgement".to_owned(),
            ));
        }
        Ok(deleted)
    }
}

/// Pause one object HEAD so restore fsck can be exercised across an unrelated
/// metadata commit while its physical-reclaim fence remains held.
#[derive(Clone)]
struct HeadBarrierObjectStore {
    inner: MemoryObjectStore,
    armed: Arc<AtomicBool>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl HeadBarrierObjectStore {
    fn new(inner: MemoryObjectStore) -> Self {
        Self {
            inner,
            armed: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn wait_until_head(&self) {
        self.entered.wait();
    }

    fn release_head(&self) {
        self.release.wait();
    }
}

impl ObjectStore for HeadBarrierObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<nokv_object::ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(
        &self,
        key: &ObjectKey,
        range: Option<nokv_object::ObjectRange>,
    ) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<nokv_object::ObjectInfo>, ObjectError> {
        let info = self.inner.head(key)?;
        if self
            .armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.entered.wait();
            self.release.wait();
        }
        Ok(info)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

/// Pause after the current active-marker read so an allocator high-water
/// reservation can replace the current-only System allocator before metrics
/// reads its fence projection.
#[derive(Clone)]
struct AllocatorFenceBarrierStore {
    inner: HoltMetadataStore,
    active_key: Vec<u8>,
    armed: Arc<AtomicBool>,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl AllocatorFenceBarrierStore {
    fn new(mount: MountId) -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            active_key: restore::restore_active_key(mount),
            armed: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn wait_until_active_read(&self) {
        self.entered.wait();
    }

    fn release_active_read(&self) {
        self.release.wait();
    }
}

impl MetadataStore for AllocatorFenceBarrierStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        let item = self.inner.get_versioned(family, key, version, purpose)?;
        if family == RecordFamily::System
            && key == self.active_key
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.entered.wait();
            self.release.wait();
        }
        Ok(item)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.inner.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
        self.inner.history_retention_epoch()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

fn service() -> NoKvFs<HoltMetadataStore, MemoryObjectStore> {
    service_with_objects().0
}

fn service_with_objects() -> (
    NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    MemoryObjectStore,
) {
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    (service, objects)
}

fn enqueue_gc_candidate<M, O>(service: &NoKvFs<M, O>, mut record: ObjectGcRecord) -> Vec<u8>
where
    M: MetadataStore,
    O: ObjectStore,
{
    let version = service.next_version().unwrap();
    record.enqueue_version = version.get();
    record.enqueue_unix_ms = service.now_ms();
    let key = gc_object_key(
        service.mount,
        version.get(),
        record.inode,
        record.generation,
        0,
        0,
    );
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-enqueue-gc-candidate",
                service.mount,
                record.inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::Gc,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Gc,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Gc,
                key: key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_object_gc_record(&record))),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    key
}

fn leave_object_gc_deleting<M, O>(service: &NoKvFs<M, O>, gc_row: &ScanItem) -> Vec<u8>
where
    M: MetadataStore,
    O: ObjectStore,
{
    let claim_key = object_gc_claim_key(service.mount);
    let claim = service
        .metadata
        .get_versioned(
            RecordFamily::System,
            &claim_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-leave-object-gc-deleting",
                service.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: claim_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: claim_key.clone(),
                    predicate: Predicate::VersionEquals(claim.version),
                },
                PredicateRef {
                    family: RecordFamily::Gc,
                    key: gc_row.key.clone(),
                    predicate: Predicate::VersionEquals(gc_row.version),
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: claim_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(
                    encode_object_gc_claim(&ObjectGcClaim::Deleting {
                        owner_epoch: service.epoch.load(Ordering::Relaxed),
                        operation_token: claim.version.get(),
                        gc_record_key: gc_row.key.clone(),
                        gc_record_version: gc_row.version.get(),
                    })
                    .unwrap(),
                )),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    claim_key
}

fn delete_object_gc_claim<M, O>(service: &NoKvFs<M, O>)
where
    M: MetadataStore,
    O: ObjectStore,
{
    let key = object_gc_claim_key(service.mount);
    let claim = service
        .metadata
        .get_versioned(
            RecordFamily::System,
            &key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-delete-object-gc-claim",
                service.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: key.clone(),
                predicate: Predicate::VersionEquals(claim.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, key)],
            watch: Vec::new(),
        })
        .unwrap();
}

fn artifact_request(name: DentryName, manifest_id: &str, bytes: &[u8]) -> PublishArtifact {
    PublishArtifact {
        parent: InodeId::root(),
        name,
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:test".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        bytes: bytes.to_vec(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

fn publish_path_artifact<M: MetadataStore, O: ObjectStore>(
    service: &NoKvFs<M, O>,
    path: &str,
    manifest_id: &str,
    bytes: &[u8],
) -> DentryWithAttr {
    let prepared = service.prepare_artifact_create_path(path).unwrap();
    service
        .publish_prepared_artifact_session(
            prepared.clone(),
            PublishArtifactSession {
                parent: prepared.parent,
                name: prepared.name,
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest_id.to_owned(),
                size: bytes.len() as u64,
                ranges: vec![PublishArtifactRange {
                    offset: 0,
                    bytes: bytes.to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap()
        .entry
}

/// Supersede an existing artifact in `parent` (replace -> a fresh generation).
fn republish_path_artifact<O: ObjectStore>(
    service: &NoKvFs<HoltMetadataStore, O>,
    parent: InodeId,
    name: &str,
    manifest_id: &str,
    bytes: &[u8],
) -> DentryWithAttr {
    let prepared = service
        .prepare_artifact_replace(parent, DentryName::new(name.as_bytes().to_vec()).unwrap())
        .unwrap();
    service
        .publish_prepared_artifact_session(
            prepared.clone(),
            PublishArtifactSession {
                parent: prepared.parent,
                name: prepared.name,
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest_id.to_owned(),
                size: bytes.len() as u64,
                ranges: vec![PublishArtifactRange {
                    offset: 0,
                    bytes: bytes.to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap()
        .entry
}

#[test]
fn publish_multichunk_artifact_succeeds() {
    let service = service();
    // 128 MiB spans two 64 MiB chunks: the multi-chunk publish path the FUSE
    // bigfile workload hits (and currently EIOs on via InvalidPreparedArtifact).
    let size = 128 * 1024 * 1024_usize;
    let bytes = vec![0u8; size];
    let prepared = service.prepare_artifact_create_path("/big.bin").unwrap();
    let result = service.publish_prepared_artifact_session(
        prepared.clone(),
        PublishArtifactSession {
            parent: prepared.parent,
            name: prepared.name,
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fuse/big".to_owned(),
            size: size as u64,
            ranges: vec![PublishArtifactRange { offset: 0, bytes }],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(
        result.is_ok(),
        "multi-chunk publish failed: {:?}",
        result.err()
    );
}

fn block_key(inode: InodeId, generation: u64, chunk: u64, block: u64) -> ObjectKey {
    ObjectKey::new(format!(
        "blocks/1/{}/{}/{}/{}",
        inode.get(),
        generation,
        chunk,
        block
    ))
    .unwrap()
}

fn body_descriptor(generation: u64, size: u64) -> BodyDescriptor {
    BodyDescriptor {
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:test".to_owned(),
        size,
        content_type: "application/octet-stream".to_owned(),
        manifest_id: format!("manifest-{generation}"),
        generation,
        base_generation: 0,
        chunk_size: DEFAULT_CHUNK_SIZE,
        block_size: DEFAULT_BLOCK_SIZE as u64,
    }
}

fn one_chunk_manifest(inode: InodeId, generation: u64, len: u64) -> ChunkManifest {
    ChunkManifest {
        chunk_index: 0,
        logical_offset: 0,
        len,
        slices: vec![SliceManifest {
            slice_id: 1,
            logical_offset: 0,
            len,
            blocks: vec![BlockDescriptor {
                object_key: block_key(inode, generation, 0, 0).as_str().to_owned(),
                logical_offset: 0,
                object_offset: 0,
                len,
                digest_uri: "sha256:block".to_owned(),
            }],
        }],
    }
}

#[test]
fn create_dir_then_lookup_and_readdir_use_dentry_projection() {
    let service = service();
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();

    let lookup = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, created);

    let entries = service.read_dir_plus(InodeId::root()).unwrap();
    assert_eq!(entries, vec![created]);
    let stats = service.metadata_service_stats();
    assert_eq!(stats.read_dir_plus_total, 1);
    assert_eq!(stats.read_dir_plus_entry_total, 1);
    assert_eq!(stats.read_dir_plus_projection_hit_total, 1);
}

#[test]
fn write_planning_reads_are_marked_local_while_user_reads_stay_strong() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let file_name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .create_file(InodeId::root(), file_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    let dir_name = DentryName::new(b"runs".to_vec()).unwrap();
    let dir = service
        .create_dir(InodeId::root(), dir_name, 0o755, 1000, 1000)
        .unwrap();

    let before_lookup = metadata.counts();
    assert!(service
        .lookup_plus(InodeId::root(), &file_name)
        .unwrap()
        .is_some());
    let after_lookup = metadata.counts();
    assert!(after_lookup.user_strong_gets > before_lookup.user_strong_gets);
    assert_eq!(after_lookup.write_plan_gets, before_lookup.write_plan_gets);

    service
        .remove_file(InodeId::root(), &file_name)
        .expect("remove file");
    let after_remove = metadata.counts();
    assert_eq!(after_remove.user_strong_gets, after_lookup.user_strong_gets);
    assert!(after_remove.write_plan_gets > after_lookup.write_plan_gets);

    let snapshot = service
        .snapshot_subtree(dir.attr.inode)
        .expect("snapshot subtree");
    let after_snapshot = metadata.counts();
    assert_eq!(
        after_snapshot.user_strong_gets,
        after_remove.user_strong_gets
    );
    assert!(after_snapshot.write_plan_gets > after_remove.write_plan_gets);

    assert!(service
        .get_attr_at_snapshot("/runs", snapshot.snapshot_id, &[])
        .unwrap()
        .is_some());
    assert!(service
        .read_dir_plus_at_snapshot("/runs", snapshot.snapshot_id, &[])
        .unwrap()
        .is_empty());
    let after_snapshot_reads = metadata.counts();
    assert!(
        after_snapshot_reads.user_strong_gets > after_snapshot.user_strong_gets,
        "root-path binding is resolved with strong reads before snapshot-purpose reads"
    );
    assert!(after_snapshot_reads.snapshot_gets > after_snapshot.snapshot_gets);
    assert!(after_snapshot_reads.snapshot_scans > after_snapshot.snapshot_scans);
}

#[test]
fn xattr_round_trips_lists_replaces_and_removes() {
    let service = service();
    let entry = service
        .create_file(
            InodeId::root(),
            DentryName::new(b"note.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();

    service
        .set_xattr(
            entry.attr.inode,
            b"user.comment",
            b"first".to_vec(),
            XattrSetMode::Create,
        )
        .unwrap();
    assert_eq!(
        service
            .get_xattr(entry.attr.inode, b"user.comment")
            .unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        service.list_xattr(entry.attr.inode).unwrap(),
        vec![b"user.comment".to_vec()]
    );
    assert!(matches!(
        service.set_xattr(
            entry.attr.inode,
            b"user.comment",
            b"duplicate".to_vec(),
            XattrSetMode::Create,
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));

    service
        .set_xattr(
            entry.attr.inode,
            b"user.comment",
            b"second".to_vec(),
            XattrSetMode::Replace,
        )
        .unwrap();
    assert_eq!(
        service
            .get_xattr(entry.attr.inode, b"user.comment")
            .unwrap(),
        Some(b"second".to_vec())
    );
    assert!(matches!(
        service.set_xattr(
            entry.attr.inode,
            b"user.missing",
            b"value".to_vec(),
            XattrSetMode::Replace,
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));

    service
        .remove_xattr(entry.attr.inode, b"user.comment")
        .unwrap();
    assert_eq!(
        service
            .get_xattr(entry.attr.inode, b"user.comment")
            .unwrap(),
        None
    );
    assert!(service.list_xattr(entry.attr.inode).unwrap().is_empty());
    assert!(matches!(
        service.remove_xattr(entry.attr.inode, b"user.comment"),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
}

#[test]
fn path_methods_resolve_current_namespace_on_server_side() {
    let service = service();
    let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let artifact = service
        .create_file_path("/runs/checkpoint.bin", 0o644, 1000, 1000)
        .unwrap();

    assert_eq!(service.lookup_path("/runs").unwrap(), Some(runs.clone()));
    assert_eq!(
        service.lookup_path("/runs/checkpoint.bin").unwrap(),
        Some(artifact.clone())
    );
    assert_eq!(service.read_dir_plus_path("/runs").unwrap(), vec![artifact]);
    assert!(matches!(
        service.create_file_path("relative", 0o644, 1000, 1000),
        Err(MetadError::InvalidPath(_))
    ));
}

#[test]
fn plain_path_create_uses_canonical_namespace_without_path_index() {
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let artifact = service
        .create_file_path("/runs/checkpoint.bin", 0o644, 1000, 1000)
        .unwrap();
    let components = parse_absolute_path("/runs/checkpoint.bin").unwrap();
    let key = path_index_key(MountId::new(1).unwrap(), &components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());

    let before = service.metadata_service_stats();
    assert_eq!(
        service.lookup_path("/runs/checkpoint.bin").unwrap(),
        Some(artifact)
    );
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        0
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        0
    );
}

#[test]
fn prepared_artifact_path_publish_writes_and_uses_validated_path_index() {
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let prepared = service
        .prepare_artifact_create_path("/runs/checkpoint.bin")
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let artifact = service
        .publish_prepared_artifact(
            prepared.clone(),
            body,
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;
    let components = parse_absolute_path("/runs/checkpoint.bin").unwrap();
    let key = path_index_key(MountId::new(1).unwrap(), &components);
    let indexed = metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .expect("artifact path index entry");
    let projection = decode_dentry_projection(&indexed.0).unwrap();
    assert_eq!(DentryWithAttr::from(projection), artifact);

    let before = service.metadata_service_stats();
    let metadata = service
        .stat_path("/runs/checkpoint.bin")
        .unwrap()
        .expect("artifact stat");
    assert_eq!(metadata.attr, artifact.attr);
    assert_eq!(metadata.body, artifact.body);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        1
    );
    assert_eq!(after.path_index_hit_total - before.path_index_hit_total, 1);
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        0
    );

    let before = service.metadata_service_stats();
    assert_eq!(service.stat_path("/runs/missing.bin").unwrap(), None);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        1
    );
    assert_eq!(
        after.path_index_miss_total - before.path_index_miss_total,
        1
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        1
    );
}

#[test]
fn artifact_path_rename_moves_live_path_index() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let archive = service
        .create_dir_path("/archive", 0o755, 1000, 1000)
        .unwrap();
    let artifact = publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    let old_components = parse_absolute_path("/runs/a.bin").unwrap();
    let new_components = parse_absolute_path("/archive/a.bin").unwrap();
    let old_key = path_index_key(MountId::new(1).unwrap(), &old_components);
    let new_key = path_index_key(MountId::new(1).unwrap(), &new_components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    let renamed = service
        .rename_path("/runs/a.bin", "/archive/a.bin")
        .unwrap();
    let old_index = metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    let new_index = metadata
        .get(
            RecordFamily::PathIndex,
            &new_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .expect("renamed artifact path index");

    assert!(old_index.is_none());
    assert_eq!(renamed.attr.inode, artifact.attr.inode);
    let indexed = decode_dentry_projection(&new_index.0).unwrap();
    assert_eq!(indexed.dentry.parent, archive.attr.inode);
    assert_eq!(indexed.dentry.name.as_bytes(), b"a.bin");
    assert_eq!(indexed.attr.inode, artifact.attr.inode);
}

#[test]
fn plain_directory_path_rename_does_not_create_path_index() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let source_components = parse_absolute_path("/runs").unwrap();
    let destination_components = parse_absolute_path("/archive").unwrap();
    let source_key = path_index_key(MountId::new(1).unwrap(), &source_components);
    let destination_key = path_index_key(MountId::new(1).unwrap(), &destination_components);
    let before = metadata.metadata_store_stats();

    let renamed = service.rename_path("/runs", "/archive").unwrap();
    let after = metadata.metadata_store_stats();

    assert_eq!(renamed.attr.inode, runs.attr.inode);
    assert_eq!(after.current_put_total - before.current_put_total, 1);
    assert_eq!(after.current_delete_total - before.current_delete_total, 1);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &source_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &destination_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
}

#[test]
fn artifact_path_remove_deletes_live_path_index() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    let components = parse_absolute_path("/runs/a.bin").unwrap();
    let key = path_index_key(MountId::new(1).unwrap(), &components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    service.remove_file_path("/runs/a.bin").unwrap();

    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
}

#[test]
fn path_resolution_cache_reuses_parent_directory_for_indexed_stats() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    publish_path_artifact(&service, "/runs/b.bin", "runs/b.bin", b"b");
    service.clear_read_path_caches_for_test();

    let before_store = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();
    assert!(service.stat_path("/runs/a.bin").unwrap().is_some());
    let after_first_store = metadata.metadata_store_stats();
    assert!(service.stat_path("/runs/b.bin").unwrap().is_some());
    let after_second_store = metadata.metadata_store_stats();
    let after_service = service.metadata_service_stats();

    let first_gets = after_first_store.get_total - before_store.get_total;
    let second_gets = after_second_store.get_total - after_first_store.get_total;
    assert_eq!(first_gets, 3);
    assert_eq!(second_gets, 2);
    assert_eq!(
        after_service.path_index_hit_total - before_service.path_index_hit_total,
        2
    );
    assert_eq!(
        after_service.path_index_fallback_total - before_service.path_index_fallback_total,
        0
    );
}

#[test]
fn validated_path_index_cache_reuses_stat_validation_for_indexed_list() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let first = publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    let second = publish_path_artifact(&service, "/runs/b.bin", "runs/b.bin", b"b");
    service.clear_read_path_caches_for_test();

    assert!(service.stat_path("/runs/a.bin").unwrap().is_some());
    assert!(service.stat_path("/runs/b.bin").unwrap().is_some());

    let before_store = metadata.metadata_store_stats();
    let page = service.list_indexed_path_page("/runs", None, 10).unwrap();
    let after_store = metadata.metadata_store_stats();

    assert_eq!(page.entries, vec![first, second]);
    assert_eq!(page.next_cursor, None);
    assert_eq!(after_store.get_total - before_store.get_total, 0);
}

#[test]
fn validated_path_index_lookup_cache_reuses_repeated_stat_result() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let artifact = publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    service.clear_read_path_caches_for_test();

    let first = service
        .stat_path("/runs/a.bin")
        .unwrap()
        .expect("first stat");
    assert_eq!(first.attr, artifact.attr);

    let before_store = metadata.metadata_store_stats();
    let second = service
        .stat_path("/runs/a.bin")
        .unwrap()
        .expect("second stat");
    let after_store = metadata.metadata_store_stats();

    assert_eq!(second.attr, artifact.attr);
    assert_eq!(after_store.get_total - before_store.get_total, 0);
}

#[test]
fn namespace_find_body_facets_do_not_require_body_projection() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.json", "runs/a.json", br#"{"loss":0.4}"#);
    publish_path_artifact(&service, "/runs/b.log", "runs/b.log", b"loss=0.3\n");

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: Vec::new(),
            sort: Vec::new(),
            include: Vec::new(),
            facets: vec![NamespaceFindField::body_content_type()],
            cursor: None,
            limit: 10,
        })
        .unwrap();

    assert_eq!(result.match_count, 2);
    assert!(result.matches.iter().all(|card| card.body.is_none()));
    assert_eq!(result.facets.len(), 1);
    assert_eq!(
        result.facets[0].field,
        NamespaceFindField::body_content_type()
    );
    assert_eq!(result.facets[0].values[0].count, 2);
}

#[test]
fn namespace_find_tolerates_exists_predicate_payloads() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.json", "runs/a.json", br#"{"loss":0.4}"#);

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::body_content_type(),
                op: NamespacePredicateOp::Exists,
                value: Some(NamespacePredicateValue::String("ignored".to_owned())),
            }],
            sort: Vec::new(),
            include: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: 10,
        })
        .unwrap();

    assert_eq!(result.match_count, 1);
    assert_eq!(result.matches[0].path, "/runs/a.json");
}

#[test]
fn namespace_grep_cursor_resumes_at_next_unemitted_match() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/runs/train.log",
        "runs/train.log",
        b"loss=1\nloss=2\n",
    );

    let first = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs/train.log".to_owned(),
            pattern: "loss".to_owned(),
            patterns: Vec::new(),
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 1,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(first.matches.len(), 1);
    assert_eq!(first.matches[0].line_number, 1);
    assert_eq!(first.pattern, "loss");
    assert!(first.patterns.is_empty());

    let second = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs/train.log".to_owned(),
            pattern: "loss".to_owned(),
            patterns: Vec::new(),
            recursive: false,
            name_glob: None,
            cursor: first.next_cursor,
            limit: 1,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(second.matches.len(), 1);
    assert_eq!(second.matches[0].line_number, 2);
}

#[test]
fn namespace_grep_multiple_patterns_match_any() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.log", "runs/a.log", b"alpha metric\n");
    publish_path_artifact(
        &service,
        "/runs/b.log",
        "runs/b.log",
        b"nothing here\nbeta metric\n",
    );

    let result = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: String::new(),
            patterns: vec!["alpha".to_owned(), "beta".to_owned()],
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();

    assert_eq!(result.matches.len(), 2);
    assert_eq!(result.matches[0].path, "/runs/a.log");
    assert_eq!(result.matches[0].line_number, 1);
    assert_eq!(result.matches[1].path, "/runs/b.log");
    assert_eq!(result.matches[1].line_number, 2);
    assert_eq!(result.patterns, vec!["alpha", "beta"]);
}

#[test]
fn namespace_grep_multiple_patterns_match_cjk() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/runs/notes.txt",
        "runs/notes.txt",
        "今日营养记录\n普通一行\n食谱更新完成\n".as_bytes(),
    );

    let result = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs/notes.txt".to_owned(),
            pattern: String::new(),
            patterns: vec!["营养".to_owned(), "食谱".to_owned()],
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();

    let lines = result
        .matches
        .iter()
        .map(|entry| entry.line_number)
        .collect::<Vec<_>>();
    assert_eq!(lines, vec![1, 3]);
}

/// `patterns` adds OR alternatives to `pattern` (union semantics); a non-empty
/// `pattern` must keep matching when `patterns` is also provided instead of
/// being silently dropped.
#[test]
fn namespace_grep_unions_pattern_with_patterns() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/runs/notes.txt",
        "runs/notes.txt",
        "食谱更新完成\n无关的一行\n食材已备齐\n新 recipe 上线\n".as_bytes(),
    );

    let result = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs/notes.txt".to_owned(),
            pattern: "食谱".to_owned(),
            patterns: vec!["食材".to_owned(), "recipe".to_owned()],
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();

    let lines = result
        .matches
        .iter()
        .map(|entry| entry.line_number)
        .collect::<Vec<_>>();
    assert_eq!(lines, vec![1, 3, 4]);
    // The echo reports the request fields verbatim.
    assert_eq!(result.pattern, "食谱");
    assert_eq!(result.patterns, vec!["食材", "recipe"]);
}

/// The workbench pipe-split forwards `pattern: "a|b"` together with
/// `patterns: ["a", "b"]`. Any line containing the literal "a|b" also contains
/// each split alternative, so union semantics must return the same match set
/// as the split alternatives alone.
#[test]
fn namespace_grep_piped_pattern_union_matches_split_alternatives() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/runs/mixed.log",
        "runs/mixed.log",
        b"alpha metric\nliteral alpha|beta row\nbeta metric\nnothing\n",
    );
    let request = |pattern: &str, patterns: Vec<String>| NamespaceGrepRequest {
        path: "/runs/mixed.log".to_owned(),
        pattern: pattern.to_owned(),
        patterns,
        recursive: false,
        name_glob: None,
        cursor: None,
        limit: 10,
        max_files: None,
        max_bytes: None,
    };

    let split_only = service
        .grep_paths(request("", vec!["alpha".to_owned(), "beta".to_owned()]))
        .unwrap();
    let piped_union = service
        .grep_paths(request(
            "alpha|beta",
            vec!["alpha".to_owned(), "beta".to_owned()],
        ))
        .unwrap();

    let lines = |result: &NamespaceGrepResult| {
        result
            .matches
            .iter()
            .map(|entry| (entry.path.clone(), entry.line_number))
            .collect::<Vec<_>>()
    };
    assert_eq!(lines(&piped_union), lines(&split_only));
    assert_eq!(
        lines(&split_only),
        vec![
            ("/runs/mixed.log".to_owned(), 1),
            ("/runs/mixed.log".to_owned(), 2),
            ("/runs/mixed.log".to_owned(), 3),
        ]
    );
}

#[test]
fn namespace_grep_name_glob_skips_unmatched_files_without_reading() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.md", "runs/a.md", b"needle in md\n");
    publish_path_artifact(&service, "/runs/b.log", "runs/b.log", b"needle in log\n");

    let result = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: "needle".to_owned(),
            patterns: Vec::new(),
            recursive: false,
            name_glob: Some("*.md".to_owned()),
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();

    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "/runs/a.md");
    assert_eq!(result.files_scanned, 1);
    assert_eq!(result.bytes_read, b"needle in md\n".len());
}

#[test]
fn namespace_grep_name_glob_matches_cjk_substring() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/runs/营养日志.txt",
        "runs/nutrition.txt",
        "记录一\n".as_bytes(),
    );
    publish_path_artifact(
        &service,
        "/runs/训练日志.txt",
        "runs/training.txt",
        "记录二\n".as_bytes(),
    );

    let result = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: "记录".to_owned(),
            patterns: Vec::new(),
            recursive: false,
            name_glob: Some("*营养*".to_owned()),
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();

    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "/runs/营养日志.txt");
    assert_eq!(result.files_scanned, 1);
}

#[test]
fn namespace_grep_rejects_more_than_sixteen_patterns() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let err = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: String::new(),
            patterns: (0..17).map(|index| format!("p{index}")).collect(),
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap_err();

    assert!(matches!(err, MetadError::InvalidQuery(message) if message.contains("16")));
}

#[test]
fn namespace_grep_rejects_empty_pattern_entry() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let err = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: String::new(),
            patterns: vec!["ok".to_owned(), String::new()],
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap_err();

    assert!(matches!(err, MetadError::InvalidQuery(message) if message.contains("empty")));
}

#[test]
fn namespace_grep_rejects_missing_pattern_and_patterns() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let err = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: String::new(),
            patterns: Vec::new(),
            recursive: false,
            name_glob: None,
            cursor: None,
            limit: 10,
            max_files: None,
            max_bytes: None,
        })
        .unwrap_err();

    assert!(matches!(err, MetadError::InvalidQuery(message) if message.contains("pattern")));
}

#[test]
fn namespace_read_structured_offset_without_cursor_skips_items() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.json", "runs/a.json", br#"["a","b","c"]"#);

    let page = service
        .read_page(
            "/runs/a.json",
            NamespaceReadOptions {
                format: NamespaceReadFormat::Structured,
                cursor: None,
                offset: 1,
                limit: 1,
                expected_generation: None,
            },
        )
        .unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].index, 1);
    assert_eq!(page.items[0].value_json, r#""b""#);
    assert!(page.truncated);
}

#[test]
fn namespace_read_bytes_honors_returned_cursor() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"abcdef");

    let first = service
        .read_page(
            "/runs/a.bin",
            NamespaceReadOptions {
                format: NamespaceReadFormat::Bytes,
                cursor: None,
                offset: 0,
                limit: 2,
                expected_generation: None,
            },
        )
        .unwrap();
    assert_eq!(first.bytes.as_deref(), Some(b"ab".as_slice()));

    let second = service
        .read_page(
            "/runs/a.bin",
            NamespaceReadOptions {
                format: NamespaceReadFormat::Bytes,
                cursor: first.next_cursor,
                offset: 0,
                limit: 2,
                expected_generation: None,
            },
        )
        .unwrap();
    assert_eq!(second.bytes.as_deref(), Some(b"cd".as_slice()));
}

#[test]
fn register_namespace_index_rejects_rows_outside_registered_path() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let err = service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.status"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: false,
                facetable: true,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/archive/a.json".to_owned(),
                values: Vec::new(),
            }],
        })
        .unwrap_err();

    assert!(
        matches!(err, MetadError::InvalidQuery(message) if message.contains("outside registered namespace"))
    );
}

#[test]
fn register_namespace_index_uses_metadata_predicate_fence() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let before = metadata.metadata_store_stats();
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.status"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: false,
                facetable: true,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/runs/a.json".to_owned(),
                values: vec![NamespaceIndexValue {
                    field: NamespaceFindField::new("run.status"),
                    value: NamespacePredicateValue::String("completed".to_owned()),
                }],
            }],
        })
        .unwrap();
    let after = metadata.metadata_store_stats();

    assert_eq!(
        after.predicate_total - before.predicate_total,
        2,
        "catalog registration fences both the canonical catalog and absent restore membership"
    );
}

#[test]
fn stale_path_index_falls_back_to_canonical_namespace() {
    let service = service();
    let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let archive = service
        .create_dir_path("/archive", 0o755, 1000, 1000)
        .unwrap();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create_path("/runs/checkpoint.bin")
        .unwrap();
    let artifact = service
        .publish_prepared_artifact(
            prepared.clone(),
            body_descriptor(prepared.generation, 6),
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;

    assert!(service.stat_path("/runs/checkpoint.bin").unwrap().is_some());

    service
        .rename(runs.attr.inode, &name, archive.attr.inode, name.clone())
        .unwrap();

    let before = service.metadata_service_stats();
    assert_eq!(service.stat_path("/runs/checkpoint.bin").unwrap(), None);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        1
    );
    assert_eq!(
        after.path_index_stale_total - before.path_index_stale_total,
        1
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        1
    );

    let mut moved_artifact = artifact;
    moved_artifact.dentry.parent = archive.attr.inode;

    let before = service.metadata_service_stats();
    let metadata = service
        .stat_path("/archive/checkpoint.bin")
        .unwrap()
        .expect("moved artifact stat");
    assert_eq!(metadata.attr, moved_artifact.attr);
    assert_eq!(metadata.body, moved_artifact.body);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_miss_total - before.path_index_miss_total,
        1
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        1
    );
}

#[test]
fn path_index_page_lists_immediate_indexed_children_with_holt_delimiter() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let epoch = service
        .create_dir_path("/runs/epoch", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/runs/plain.txt", 0o644, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/runs/top.bin", "runs/top.bin", b"top");
    publish_path_artifact(
        &service,
        "/runs/epoch/ckpt.bin",
        "runs/epoch/ckpt.bin",
        b"ckpt",
    );

    let before = metadata.metadata_store_stats();
    let first = service.list_indexed_path_page("/runs", None, 1).unwrap();
    let after_first = metadata.metadata_store_stats();
    assert_eq!(first.entries, vec![epoch]);
    assert_eq!(
        first.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"epoch".as_slice())
    );
    assert_eq!(
        after_first.scan_key_returned_total - before.scan_key_returned_total,
        2
    );

    let second = service
        .list_indexed_path_page("/runs", first.next_cursor.as_ref(), 10)
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].dentry.name.as_bytes(), b"top.bin");
    assert_eq!(second.entries[0].attr.file_type, FileType::File);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn path_index_page_skips_stale_rows_without_truncating_visible_children() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/archive", 0o755, 1000, 1000)
        .unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/runs/aaa", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/runs/aaa/stale.bin",
        "runs/aaa/stale.bin",
        b"stale",
    );
    service.rename_path("/runs/aaa", "/archive/aaa").unwrap();
    let first_valid = publish_path_artifact(&service, "/runs/bbb.bin", "runs/bbb.bin", b"bbb");
    let second_valid = publish_path_artifact(&service, "/runs/ccc.bin", "runs/ccc.bin", b"ccc");

    let before_store = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();
    let first = service.list_indexed_path_page("/runs", None, 1).unwrap();
    let after_first_store = metadata.metadata_store_stats();
    let after_first_service = service.metadata_service_stats();
    assert_eq!(first.entries, vec![first_valid]);
    assert_eq!(
        first.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"bbb.bin".as_slice())
    );
    assert!(
        after_first_store.scan_key_returned_total - before_store.scan_key_returned_total > 2,
        "stale index row should force an extra delimiter scan page"
    );
    assert_eq!(
        after_first_service.read_dir_plus_entry_total - before_service.read_dir_plus_entry_total,
        1
    );
    assert_eq!(
        after_first_service.read_dir_plus_projection_hit_total
            - before_service.read_dir_plus_projection_hit_total,
        1
    );
    assert!(
        after_first_service.path_index_scan_stale_total
            - before_service.path_index_scan_stale_total
            >= 1,
        "stale derived path-index rows should be reported separately from live entries"
    );

    let second = service
        .list_indexed_path_page("/runs", first.next_cursor.as_ref(), 1)
        .unwrap();
    assert_eq!(second.entries, vec![second_valid]);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn directory_rename_leaves_descendant_path_index_as_derived_stale_cache() {
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let prepared = service
        .prepare_artifact_create_path("/runs/checkpoint.bin")
        .unwrap();
    let artifact = service
        .publish_prepared_artifact(
            prepared.clone(),
            body_descriptor(prepared.generation, 6),
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;
    let old_components = parse_absolute_path("/runs/checkpoint.bin").unwrap();
    let old_key = path_index_key(MountId::new(1).unwrap(), &old_components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    service.rename_path("/runs", "/archive").unwrap();

    let renamed_dir_key = path_index_key(
        MountId::new(1).unwrap(),
        &parse_absolute_path("/runs").unwrap(),
    );
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &renamed_dir_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());
    assert!(matches!(
        service.lookup_path("/runs/checkpoint.bin"),
        Err(MetadError::NotFound)
    ));
    assert_eq!(
        service.lookup_path("/archive/checkpoint.bin").unwrap(),
        Some(artifact)
    );
}

#[test]
fn create_file_publishes_metadata_without_body_descriptor() {
    let service = service();
    let name = DentryName::new(b"empty.txt".to_vec()).unwrap();
    let created = service
        .create_file(InodeId::root(), name.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(created.attr.file_type, FileType::File);
    assert_eq!(created.attr.size, 0);
    assert!(created.body.is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(created)
    );
}

#[test]
fn new_file_attrs_use_wall_clock_timestamps() {
    let service = service();
    let before = current_time_ms().saturating_sub(1_000);

    let created = service
        .create_file(
            InodeId::root(),
            DentryName::new(b"empty.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert!(created.attr.mtime_ms >= before);
    assert!(created.attr.ctime_ms >= before);
    assert!(created.attr.mtime_ms > created.attr.generation);

    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"artifact.bin".to_vec()).unwrap(),
            "artifact",
            b"body",
        ))
        .unwrap();
    assert!(published.attr.mtime_ms >= before);
    assert!(published.attr.ctime_ms >= before);
    assert!(published.attr.mtime_ms > published.attr.generation);
}

#[test]
fn create_symlink_round_trips_target_and_unlinks_like_file() {
    let service = service();
    let name = DentryName::new(b"latest".to_vec()).unwrap();
    let created = service
        .create_symlink(
            InodeId::root(),
            name.clone(),
            b"runs/42/checkpoint.bin".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();

    assert_eq!(created.attr.file_type, FileType::Symlink);
    assert_eq!(created.attr.size, 22);
    assert_eq!(
        service.read_symlink(created.attr.inode).unwrap(),
        b"runs/42/checkpoint.bin"
    );
    assert_eq!(
        created.body.as_ref().unwrap().digest_uri,
        "sha256:15a533489b90109ab69bd64dabcc260602c854b6b4a472b20aefa0eabcee3a24"
    );
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(created.clone())
    );

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed.attr.file_type, FileType::Symlink);
    assert_eq!(service.lookup_plus(InodeId::root(), &name).unwrap(), None);
}

#[test]
fn create_special_node_persists_type_and_rdev_without_body() {
    let service = service();
    let fifo_name = DentryName::new(b"events.fifo".to_vec()).unwrap();
    let fifo = service
        .create_special_node(
            InodeId::root(),
            fifo_name.clone(),
            SpecialNodeSpec {
                file_type: FileType::NamedPipe,
                mode: 0o644,
                rdev: 0,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    assert_eq!(fifo.attr.file_type, FileType::NamedPipe);
    assert_eq!(fifo.attr.rdev, 0);
    assert_eq!(fifo.attr.size, 0);
    assert!(fifo.body.is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &fifo_name).unwrap(),
        Some(fifo.clone())
    );

    let char_name = DentryName::new(b"accelerator0".to_vec()).unwrap();
    let char_device = service
        .create_special_node(
            InodeId::root(),
            char_name.clone(),
            SpecialNodeSpec {
                file_type: FileType::CharDevice,
                mode: 0o660,
                rdev: 0x1234,
                uid: 0,
                gid: 44,
            },
        )
        .unwrap();
    assert_eq!(char_device.attr.file_type, FileType::CharDevice);
    assert_eq!(char_device.attr.rdev, 0x1234);
    assert!(char_device.body.is_none());
    assert!(service
        .read_dir_plus(InodeId::root())
        .unwrap()
        .iter()
        .any(|entry| entry.attr == char_device.attr));

    let removed = service.remove_file(InodeId::root(), &char_name).unwrap();
    assert_eq!(removed.attr.file_type, FileType::CharDevice);
    assert_eq!(
        service.lookup_plus(InodeId::root(), &char_name).unwrap(),
        None
    );
}

#[test]
fn advisory_locks_detect_conflicts_and_support_partial_unlock() {
    let service = service();
    let name = DentryName::new(b"locked.bin".to_vec()).unwrap();
    let file = service
        .create_file(InodeId::root(), name, 0o644, 1000, 1000)
        .unwrap();
    let inode = file.attr.inode;
    let read_owner = 11;
    let write_owner = 22;

    service
        .set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: read_owner,
            start: 0,
            end: 99,
            kind: AdvisoryLockKind::Read,
            pid: 1100,
            wait: false,
        })
        .unwrap();
    service
        .set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: 33,
            start: 20,
            end: 30,
            kind: AdvisoryLockKind::Read,
            pid: 3300,
            wait: false,
        })
        .unwrap();

    let conflict = service
        .get_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 50,
            end: 60,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        })
        .unwrap()
        .unwrap();
    assert_eq!(conflict.owner, read_owner);
    assert_eq!(conflict.kind, AdvisoryLockKind::Read);
    assert!(matches!(
        service.set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 50,
            end: 60,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        }),
        Err(MetadError::LockConflict(_))
    ));

    service
        .set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: read_owner,
            start: 40,
            end: 70,
            kind: AdvisoryLockKind::Unlock,
            pid: 1100,
            wait: false,
        })
        .unwrap();
    assert!(service
        .get_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 50,
            end: 60,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        })
        .unwrap()
        .is_none());
    assert!(service
        .get_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 10,
            end: 20,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        })
        .unwrap()
        .is_some());
}

#[test]
fn snapshot_preserves_symlink_target() {
    let service = service();
    let name = DentryName::new(b"latest".to_vec()).unwrap();
    service
        .create_symlink(
            InodeId::root(),
            name.clone(),
            b"runs/old".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    service.remove_file(InodeId::root(), &name).unwrap();
    service
        .create_symlink(
            InodeId::root(),
            name.clone(),
            b"runs/new".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();

    assert_eq!(
        service
            .read_symlink_at_snapshot("/", snapshot.snapshot_id, std::slice::from_ref(&name))
            .unwrap(),
        b"runs/old"
    );
}

#[test]
fn update_attrs_truncates_and_extends_sparse_file() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint-v1", b"abcdef"))
        .unwrap();

    let shrunk = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                size: Some(3),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert_eq!(shrunk.attr.inode, published.attr.inode);
    assert_eq!(shrunk.attr.size, 3);
    assert_eq!(service.read_file(shrunk.attr.inode, 0, 8).unwrap(), b"abc");
    assert_eq!(
        shrunk.body.as_ref().unwrap().digest_uri,
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );

    let grown = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                size: Some(6),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert_eq!(grown.attr.size, 6);
    assert_eq!(
        service.read_file(grown.attr.inode, 0, 8).unwrap(),
        b"abc\0\0\0"
    );
    assert_eq!(
        grown.body.as_ref().unwrap().digest_uri,
        "sha256:dd0b251b2bf91037a1e4fc8416a24ae00bcb9a8c252dc7e2361f2fc015f51c16"
    );
}

#[test]
fn attr_only_update_preserves_body_generation_and_readability() {
    // `cp` preserves metadata, so it chmods a file it just wrote. An attribute-
    // only `update_attrs` (no size change) must not advance `attr.generation`:
    // the body summary / chunk manifests are keyed by generation and reads
    // resolve the body via `attr.generation`, so bumping it would point the
    // dentry at a generation that has no body, surfacing as MissingBodyDescriptor
    // on the next read (the cp corruption this regression guards).
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint-v1", b"abcdef"))
        .unwrap();
    let body_generation = published.body.as_ref().unwrap().generation;
    assert_eq!(published.attr.generation, body_generation);

    let chmodded = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                mode: Some(0o600),
                ..UpdateAttr::default()
            },
        )
        .unwrap();

    assert_eq!(chmodded.attr.mode, 0o600);
    assert_eq!(chmodded.attr.size, published.attr.size);
    // Generation is the content version; an attribute-only change keeps it.
    assert_eq!(chmodded.attr.generation, body_generation);
    assert_eq!(chmodded.body.as_ref().unwrap().generation, body_generation);
    // The body is still resolvable and intact after the metadata-only update.
    assert_eq!(
        service.read_file(chmodded.attr.inode, 0, 6).unwrap(),
        b"abcdef"
    );

    // A size change still advances the generation (new body content).
    let resized = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                size: Some(3),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert!(resized.attr.generation > body_generation);
    assert_eq!(
        resized.attr.generation,
        resized.body.as_ref().unwrap().generation
    );
    assert_eq!(service.read_file(resized.attr.inode, 0, 8).unwrap(), b"abc");
}

#[test]
fn replace_publish_refreshes_stale_dentry_version_after_attr_update() {
    // Reproduces the cp setattr-mid-write -> release publish CAS: a write handle
    // prepares an artifact-replace (pinning the dentry version), then a `setattr`
    // (here a chmod via update_attrs) advances the dentry version out-of-band.
    // Publishing with the stale pinned version must fail the CAS (PredicateFailed
    // -> EIO), and re-reading the live version via `current_dentry_version` (what
    // publish_handle now does) before publishing must make it succeed without
    // losing the body.
    let service = service();
    let name = DentryName::new(b"y.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "y-v1", b"abcdef"))
        .unwrap();

    // The write handle's prepared-replace, capturing the current dentry version.
    let mut prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let pinned_version = prepared.dentry_version.unwrap();

    // An intervening chmod advances the dentry version, stranding `prepared`.
    service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                mode: Some(0o600),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    let current_version = service
        .current_dentry_version(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_ne!(
        current_version, pinned_version,
        "chmod must advance the dentry version"
    );

    let new_body = body_descriptor(prepared.generation, 3);
    let new_chunks = vec![one_chunk_manifest(prepared.inode, prepared.generation, 3)];

    // Publishing with the stale pinned version fails the CAS, exactly the cp EIO.
    let stale = service.publish_prepared_artifact(
        prepared.clone(),
        new_body.clone(),
        new_chunks.clone(),
        0o600,
        1000,
        1000,
    );
    assert!(
        matches!(
            stale,
            Err(MetadError::Metadata(MetadataError::PredicateFailed))
        ),
        "stale dentry version must fail the replace CAS, got {stale:?}"
    );

    // Rebinding the guard to the live version (the publish_handle refresh) lets the
    // replace CAS pass and commit the new body.
    prepared.dentry_version = Some(current_version);
    let published = service
        .publish_prepared_artifact(prepared, new_body, new_chunks, 0o600, 1000, 1000)
        .unwrap()
        .entry;
    assert_eq!(published.attr.size, 3);
    assert_eq!(published.attr.mode, 0o600);
    let committed = service
        .stat_path("/y.bin")
        .unwrap()
        .expect("artifact still resolvable after refreshed publish");
    assert_eq!(committed.attr.inode, published.attr.inode);
    assert_eq!(committed.attr.size, 3);
    assert_eq!(committed.body.as_ref().unwrap().size, 3);
}

#[test]
fn update_root_attrs_changes_root_inode_without_dentry_projection() {
    let service = service();
    let updated = service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o700),
            uid: Some(42),
            gid: Some(43),
            ..UpdateAttr::default()
        })
        .unwrap();

    assert_eq!(updated.mode, 0o700);
    assert_eq!(updated.uid, 42);
    assert_eq!(updated.gid, 43);
    assert_eq!(service.get_attr(InodeId::root()).unwrap().unwrap(), updated);
}

#[test]
fn history_writes_are_snapshot_retention_driven() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();

    let before_hot = metadata.metadata_store_stats();
    service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o700),
            ..UpdateAttr::default()
        })
        .unwrap();
    let after_hot = metadata.metadata_store_stats();
    assert_eq!(
        after_hot.history_write_total - before_hot.history_write_total,
        0
    );

    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    assert_eq!(metadata.metadata_store_stats().active_snapshot_pin_total, 1);
    let snapshot_attr = service
        .get_attr_at_snapshot("/", snapshot.snapshot_id, &[])
        .unwrap()
        .unwrap();
    let before_retained = metadata.metadata_store_stats();
    service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o750),
            ..UpdateAttr::default()
        })
        .unwrap();
    let after_retained = metadata.metadata_store_stats();

    assert_eq!(
        after_retained.history_write_total - before_retained.history_write_total,
        1
    );
    assert_eq!(
        service
            .get_attr_at_snapshot("/", snapshot.snapshot_id, &[])
            .unwrap()
            .unwrap(),
        snapshot_attr
    );
    assert_eq!(
        service.get_attr(InodeId::root()).unwrap().unwrap().mode,
        0o750
    );

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    assert_eq!(metadata.metadata_store_stats().active_snapshot_pin_total, 0);
    let before_retired_hot = metadata.metadata_store_stats();
    service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o755),
            ..UpdateAttr::default()
        })
        .unwrap();
    let after_retired_hot = metadata.metadata_store_stats();
    assert_eq!(
        after_retired_hot.history_write_total - before_retired_hot.history_write_total,
        0
    );
}

#[test]
fn create_file_hot_path_write_attribution_is_bounded() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let before = metadata.metadata_store_stats();

    service
        .create_file(
            InodeId::root(),
            DentryName::new(b"empty.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 2);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
}

#[test]
fn create_files_in_dir_coalesces_into_one_metadata_command() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let before = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();

    let entries = service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    let after_service = service.metadata_service_stats();
    assert_eq!(entries.len(), 2);
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 4);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
    assert_eq!(
        after_service.create_files_batch_total - before_service.create_files_batch_total,
        1
    );
    assert_eq!(
        after_service.create_files_entry_total - before_service.create_files_entry_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 2);
}

#[test]
fn create_dirs_in_dir_coalesces_into_one_metadata_command() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let before = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();

    let entries = service
        .create_dirs_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a".to_vec()).unwrap(),
                DentryName::new(b"b".to_vec()).unwrap(),
            ],
            0o755,
            1000,
            1000,
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    let after_service = service.metadata_service_stats();
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .all(|entry| entry.attr.file_type == FileType::Directory));
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 4);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
    assert_eq!(
        after_service.create_dirs_batch_total - before_service.create_dirs_batch_total,
        1
    );
    assert_eq!(
        after_service.create_dirs_entry_total - before_service.create_dirs_entry_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 2);
}

#[test]
fn remove_files_in_dir_coalesces_into_one_holt_apply() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
                DentryName::new(b"keep.bin".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();
    let before = metadata.metadata_store_stats();

    let removed = service
        .remove_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
            ],
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    assert_eq!(removed.len(), 2);
    assert!(removed.iter().all(Result::is_ok));
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.current_delete_total - before.current_delete_total, 4);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].dentry.name.as_bytes(), b"keep.bin");
}

#[test]
fn remove_empty_dirs_in_dir_coalesces_into_one_holt_apply() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_batches_in_dir_path(vec![CreateInDirPathBatch {
            parent_path: "/runs".to_owned(),
            names: vec![
                DentryName::new(b"a".to_vec()).unwrap(),
                DentryName::new(b"b".to_vec()).unwrap(),
                DentryName::new(b"keep".to_vec()).unwrap(),
            ],
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        }])
        .remove(0)
        .unwrap();
    let before = metadata.metadata_store_stats();

    let removed = service
        .remove_empty_dirs_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a".to_vec()).unwrap(),
                DentryName::new(b"b".to_vec()).unwrap(),
            ],
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    assert_eq!(removed.len(), 2);
    assert!(removed[0].is_ok());
    assert!(removed[1].is_ok());
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.current_delete_total - before.current_delete_total, 4);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    let names = listed
        .iter()
        .map(|entry| entry.dentry.name.as_bytes())
        .collect::<Vec<_>>();
    assert_eq!(names, vec![b"keep".as_slice()]);
}

#[test]
fn read_dir_plus_page_returns_cursor_without_materializing_full_directory() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
                DentryName::new(b"c.bin".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();
    let runs = service.lookup_path("/runs").unwrap().unwrap();

    let before_store = metadata.metadata_store_stats();
    let first = service
        .read_dir_plus_page(runs.attr.inode, None, 2)
        .unwrap();
    let after_first_store = metadata.metadata_store_stats();
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"a.bin".as_slice(), b"b.bin".as_slice()]
    );
    assert_eq!(
        first.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"b.bin".as_slice())
    );
    assert_eq!(
        after_first_store.scan_key_returned_total - before_store.scan_key_returned_total,
        3
    );

    let before_service = service.metadata_service_stats();
    let second = service
        .read_dir_plus_page(runs.attr.inode, first.next_cursor.as_ref(), 2)
        .unwrap();
    let after_service = service.metadata_service_stats();
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"c.bin".as_slice()]
    );
    assert_eq!(second.next_cursor, None);
    assert_eq!(
        after_service.read_dir_plus_entry_total - before_service.read_dir_plus_entry_total,
        1
    );
    assert_eq!(
        after_service.read_dir_plus_projection_hit_total
            - before_service.read_dir_plus_projection_hit_total,
        1
    );
}

#[test]
fn publish_artifact_stores_body_then_publishes_metadata() {
    let service = service();
    let name = DentryName::new(b"checkpoint.json".to_vec()).unwrap();
    let before_publish = service.object_stats();
    let published = service
        .publish_artifact(PublishArtifact {
            content_type: "application/json".to_owned(),
            ..artifact_request(name.clone(), "runs/1/checkpoint.json", b"{\"x\":1}")
        })
        .unwrap();
    assert_eq!(
        service.object_stats().object_put_bytes,
        before_publish.object_put_bytes + 7
    );

    let lookup = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, published);
    assert_eq!(lookup.attr.size, 7);
    assert_eq!(
        lookup.body.as_ref().unwrap().manifest_id,
        "runs/1/checkpoint.json"
    );

    let bytes = service
        .read_artifact(InodeId::root(), &name)
        .expect("read artifact body");
    assert_eq!(bytes, b"{\"x\":1}");

    let body = service
        .body_descriptor(published.attr.inode)
        .expect("read body descriptor")
        .expect("body descriptor exists");
    assert_eq!(body.manifest_id, "runs/1/checkpoint.json");
    assert_eq!(body.generation, published.attr.generation);
    let range = service
        .read_file(published.attr.inode, 2, 3)
        .expect("read file range");
    assert_eq!(range, b"x\":");
    let before_cache = service.object_stats();
    let cached = service
        .read_file(published.attr.inode, 2, 3)
        .expect("read cached file range");
    assert_eq!(cached, b"x\":");
    let after_cache = service.object_stats();
    assert!(after_cache.cache_hits > before_cache.cache_hits);
    assert_eq!(
        after_cache.cache_hit_bytes,
        before_cache.cache_hit_bytes + 3
    );
}

#[test]
fn read_file_uses_one_attr_read_for_body_and_manifest_plan() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            "checkpoint/body",
            b"abcdef",
        ))
        .unwrap();

    let before = metadata.counts();
    assert_eq!(
        service.read_file(published.attr.inode, 1, 3).unwrap(),
        b"bcd"
    );
    let after = metadata.counts();
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        3,
        "read_file should read inode, body summary, and one chunk manifest"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn read_artifact_uses_dentry_projection_body_descriptor() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/body", b"abcdef"))
        .unwrap();

    let before = metadata.counts();
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"abcdef"
    );
    let after = metadata.counts();
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        2,
        "read_artifact should read dentry projection and one chunk manifest"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn open_path_read_plan_uses_dentry_projection_body_descriptor() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            "checkpoint/body",
            b"abcdef",
        ))
        .unwrap();

    let before = metadata.counts();
    let open = service
        .open_path_read_plan("/checkpoint.bin", 1, 3, Some(published.attr.generation))
        .unwrap();
    let after = metadata.counts();
    assert_eq!(open.metadata.attr.inode, published.attr.inode);
    assert_eq!(open.plan.output_len, 3);
    assert_eq!(open.plan.blocks.len(), 1);
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        2,
        "open_path_read_plan should read dentry projection and one chunk manifest"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn open_path_read_plan_batch_uses_one_read_version() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let first = service
        .publish_artifact(artifact_request(
            DentryName::new(b"sample-0.bin".to_vec()).unwrap(),
            "dataset/sample-0",
            b"abcdefgh",
        ))
        .unwrap();
    let second = service
        .publish_artifact(artifact_request(
            DentryName::new(b"sample-1.bin".to_vec()).unwrap(),
            "dataset/sample-1",
            b"uvwxyz",
        ))
        .unwrap();

    let before = metadata.counts();
    let opens = service
        .open_path_read_plan_batch(&[
            OpenPathReadPlanRequest {
                path: "/sample-0.bin".to_owned(),
                offset: 1,
                len: 3,
                expected_generation: Some(first.attr.generation),
            },
            OpenPathReadPlanRequest {
                path: "/sample-1.bin".to_owned(),
                offset: 2,
                len: 2,
                expected_generation: Some(second.attr.generation),
            },
        ])
        .unwrap();
    let after = metadata.counts();

    assert_eq!(opens.len(), 2);
    assert_eq!(opens[0].metadata.attr.inode, first.attr.inode);
    assert_eq!(opens[1].metadata.attr.inode, second.attr.inode);
    assert_eq!(opens[0].lease.read_version, opens[1].lease.read_version);
    assert_eq!(opens[0].plan.output_len, 3);
    assert_eq!(opens[1].plan.output_len, 2);
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        4,
        "batch open should read each dentry projection and chunk manifest once"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn open_path_read_plan_returns_zero_write_lease_and_projected_plan() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            "checkpoint/body",
            b"abcdef",
        ))
        .unwrap();

    let before_counts = metadata.counts();
    let before_commits = service.metadata_store_stats().commit_total;
    let open = service
        .open_path_read_plan("/checkpoint.bin", 1, 3, Some(published.attr.generation))
        .unwrap();
    let after_counts = metadata.counts();

    assert_eq!(open.metadata.attr.inode, published.attr.inode);
    assert_eq!(open.lease.inode, published.attr.inode);
    assert_eq!(open.lease.generation, published.attr.generation);
    assert!(open.lease.read_version >= published.attr.generation);
    assert_eq!(open.plan.output_len, 3);
    assert_eq!(open.plan.blocks.len(), 1);
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before_commits,
        "layout-open must not persist read state"
    );
    assert_eq!(
        after_counts.user_strong_gets - before_counts.user_strong_gets,
        2,
        "layout-open should read dentry projection and one chunk manifest"
    );
    assert_eq!(after_counts.write_plan_gets, before_counts.write_plan_gets);
}

#[test]
fn read_file_plan_returns_ranges_without_fetching_objects() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name, "checkpoint/body", b"hello metadata"))
        .unwrap();
    let before = service.object_stats();
    let plan = service
        .read_file_plan(published.attr.inode, published.attr.generation, 6, 6)
        .unwrap();
    assert_eq!(plan.output_len, 6);
    assert_eq!(plan.blocks.len(), 1);
    assert_eq!(plan.blocks[0].object_offset, 6);
    assert_eq!(plan.blocks[0].len, 6);
    assert_eq!(plan.blocks[0].output_offset, 0);
    assert!(plan.blocks[0].digest_uri.starts_with("xxh3-64:"));
    assert_eq!(service.object_stats().object_gets, before.object_gets);

    let stale = service
        .read_file_plan(published.attr.inode, published.attr.generation - 1, 0, 1)
        .unwrap_err();
    assert!(
        matches!(stale, MetadError::StaleBodyGeneration { .. }),
        "unexpected error: {stale:?}"
    );
}

#[test]
fn prepared_artifact_publish_commits_manifest_without_object_fetch() {
    let service = service();
    let name = DentryName::new(b"metadata.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let result = service
        .publish_prepared_artifact(
            prepared.clone(),
            body,
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert_eq!(result.replaced, None);
    assert_eq!(result.entry.attr.inode, prepared.inode);
    let lookup = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, result.entry);
    let plan = service
        .read_file_plan(prepared.inode, prepared.generation, 1, 3)
        .unwrap();
    assert_eq!(plan.output_len, 3);
    assert_eq!(plan.blocks[0].object_offset, 1);
}

#[test]
fn prepared_artifact_publish_rejects_foreign_block_identity_before_commit() {
    let service = service();
    let name = DentryName::new(b"foreign-block.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let foreign_keys = [
        format!(
            "blocks/2/{}/{}/0/0",
            prepared.inode.get(),
            prepared.generation
        ),
        format!("blocks/1/999/{}/0/0", prepared.generation),
        format!(
            "blocks/1/{}/{}/0/0",
            prepared.inode.get(),
            prepared.generation + 1
        ),
        format!(
            "blocks/1/{}/{}/1/0",
            prepared.inode.get(),
            prepared.generation
        ),
        format!(
            "blocks/1/{}/{}/0",
            prepared.inode.get(),
            prepared.generation
        ),
    ];

    for object_key in foreign_keys {
        let mut chunk = one_chunk_manifest(prepared.inode, prepared.generation, 6);
        chunk.slices[0].blocks[0].object_key = object_key;
        let err = service
            .publish_prepared_artifact(
                prepared.clone(),
                body.clone(),
                vec![chunk],
                0o644,
                1000,
                1000,
            )
            .unwrap_err();
        assert!(
            matches!(err, MetadError::InvalidPreparedArtifact(_)),
            "unexpected error: {err:?}"
        );
        assert!(service
            .lookup_plus(InodeId::root(), &prepared.name)
            .unwrap()
            .is_none());
    }

    let mut valid_nonzero_block = one_chunk_manifest(prepared.inode, prepared.generation, 6);
    valid_nonzero_block.slices[0].blocks[0].object_key =
        block_key(prepared.inode, prepared.generation, 0, 17)
            .as_str()
            .to_owned();
    service
        .publish_prepared_artifact(prepared, body, vec![valid_nonzero_block], 0o644, 1000, 1000)
        .unwrap();
}

#[test]
fn prepared_artifact_publish_rejects_duplicate_chunk_range() {
    let service = service();
    let name = DentryName::new(b"duplicate-chunk.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 12);
    let chunks = vec![
        one_chunk_manifest(prepared.inode, prepared.generation, 6),
        one_chunk_manifest(prepared.inode, prepared.generation, 6),
    ];

    let err = service
        .publish_prepared_artifact(prepared, body, chunks, 0o644, 1000, 1000)
        .unwrap_err();
    assert!(
        matches!(err, MetadError::InvalidPreparedArtifact(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_publish_rejects_slice_block_gap() {
    let service = service();
    let name = DentryName::new(b"slice-gap.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let mut chunk = one_chunk_manifest(prepared.inode, prepared.generation, 6);
    chunk.slices[0].blocks[0].len = 3;

    let err = service
        .publish_prepared_artifact(prepared, body, vec![chunk], 0o644, 1000, 1000)
        .unwrap_err();
    assert!(
        matches!(err, MetadError::InvalidPreparedArtifact(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_publish_rejects_block_larger_than_manifest_block_size() {
    let service = service();
    let name = DentryName::new(b"oversized-block.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let mut body = body_descriptor(prepared.generation, 6);
    body.block_size = 3;
    let chunk = one_chunk_manifest(prepared.inode, prepared.generation, 6);

    let err = service
        .publish_prepared_artifact(prepared, body, vec![chunk], 0o644, 1000, 1000)
        .unwrap_err();
    assert!(
        matches!(err, MetadError::InvalidPreparedArtifact(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_replace_rejects_stale_dentry_version() {
    let service = service();
    let name = DentryName::new(b"replace-metadata.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "old", b"old"))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    service
        .replace_artifact(artifact_request(name, "newer", b"newer"))
        .unwrap();
    let err = service
        .publish_prepared_artifact(
            prepared.clone(),
            body_descriptor(prepared.generation, 6),
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap_err();
    assert!(
        matches!(err, MetadError::Metadata(MetadataError::PredicateFailed)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_replace_retry_is_idempotent() {
    let service = service();
    let name = DentryName::new(b"retry-metadata.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "old", b"old"))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let chunks = vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)];
    let first = service
        .publish_prepared_artifact(
            prepared.clone(),
            body.clone(),
            chunks.clone(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert!(first.replaced.is_some());
    let second = service
        .publish_prepared_artifact(prepared, body, chunks, 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(second.entry, first.entry);
    assert_eq!(second.replaced, None);
}

#[test]
fn prepared_artifact_session_uploads_only_dirty_ranges_and_reuses_old_blocks() {
    let service = service();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "artifact.bin",
            b"abcdefghij",
        ))
        .unwrap();
    let before = service.object_stats();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let before_scan = service.metadata_store_stats();
    let replaced = service
        .publish_prepared_artifact_session(
            prepared,
            PublishArtifactSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "artifact.bin".to_owned(),
                size: 10,
                ranges: vec![PublishArtifactRange {
                    offset: 3,
                    bytes: b"XYZ".to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    let after = service.object_stats();
    let after_scan = service.metadata_store_stats();
    assert_eq!(after.object_puts, before.object_puts + 1);
    assert_eq!(after.object_put_bytes, before.object_put_bytes + 3);
    assert_eq!(
        after_scan.scan_key_visited_total,
        before_scan.scan_key_visited_total
    );
    assert_eq!(
        after_scan.scan_key_returned_total,
        before_scan.scan_key_returned_total
    );
    assert_eq!(replaced.entry.attr.inode, published.attr.inode);
    assert_eq!(
        service.read_file(published.attr.inode, 0, 10).unwrap(),
        b"abcXYZghij"
    );
    let gc = service.cleanup_pending_objects(10).unwrap();
    assert_eq!(gc.attempted, 0);
}

#[test]
fn prepared_artifact_session_splits_noncontiguous_dirty_blocks() {
    let service = service();
    let name = DentryName::new(b"sparse-dirty.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "sparse-dirty-v1",
            b"abcdefghijklmnop",
        ))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();

    service
        .publish_prepared_artifact_session(
            prepared,
            PublishArtifactSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "sparse-dirty-v2".to_owned(),
                size: 16,
                ranges: vec![
                    PublishArtifactRange {
                        offset: 2,
                        bytes: b"XY".to_vec(),
                    },
                    PublishArtifactRange {
                        offset: 10,
                        bytes: b"UV".to_vec(),
                    },
                ],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    assert_eq!(
        service.read_file(published.attr.inode, 0, 16).unwrap(),
        b"abXYefghijUVmnop"
    );
}

#[test]
fn prepared_artifact_session_replace_exact_retry_does_not_restage_or_reapply() {
    let service = service();
    let name = DentryName::new(b"session-retry.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(
            name.clone(),
            "session-retry-v1",
            b"original bytes",
        ))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let session = PublishArtifactSession {
        parent: prepared.parent,
        name: prepared.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:session-retry".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "session-retry-v2".to_owned(),
        size: 12,
        ranges: vec![PublishArtifactRange {
            offset: 0,
            bytes: b"retry bytes!".to_vec(),
        }],
        mode: 0o640,
        uid: 1001,
        gid: 1002,
    };

    let first = service
        .publish_prepared_artifact_session(prepared.clone(), session.clone())
        .unwrap();
    let object_puts = service.object_puts.load(Ordering::Relaxed);
    let stats = service.metadata_store_stats();
    let retry = service
        .publish_prepared_artifact_session(prepared, session)
        .unwrap();

    assert_eq!(retry.entry, first.entry);
    assert!(retry.replaced.is_none());
    assert_eq!(service.object_puts.load(Ordering::Relaxed), object_puts);
    let retry_stats = service.metadata_store_stats();
    assert_eq!(retry_stats.commit_total, stats.commit_total);
    assert_eq!(retry_stats.dedupe_write_total, stats.dedupe_write_total);
    assert_eq!(retry_stats.current_put_total, stats.current_put_total);
    assert_eq!(retry_stats.current_delete_total, stats.current_delete_total);
    assert_eq!(retry_stats.history_write_total, stats.history_write_total);
    assert_eq!(retry_stats.watch_write_total, stats.watch_write_total);
    assert_eq!(
        service.read_file(first.entry.attr.inode, 0, 12).unwrap(),
        b"retry bytes!"
    );
}

#[test]
fn prepared_artifact_staged_session_preserves_dirty_slice_overlay() {
    let service = service();
    let name = DentryName::new(b"staged-dirty.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "staged-dirty-v1",
            b"abcdefghijklmnop",
        ))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "staged-dirty-v2",
            &[
                PublishArtifactRange {
                    offset: 2,
                    bytes: b"XY".to_vec(),
                },
                PublishArtifactRange {
                    offset: 10,
                    bytes: b"UV".to_vec(),
                },
            ],
            0,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();
    let chunks = written.chunk_manifests();

    service
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "staged-dirty-v2".to_owned(),
                size: 16,
                chunks,
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    assert_eq!(
        service.read_file(published.attr.inode, 0, 16).unwrap(),
        b"abXYefghijUVmnop"
    );
    let metadata = service.lookup_path("/staged-dirty.bin").unwrap().unwrap();
    let body = metadata.body.as_ref().unwrap();
    let manifests = service
        .chunk_manifests_for_body_at_version(
            published.attr.inode,
            body,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    assert_eq!(manifests[0].slices.len(), 3);
    assert_eq!(manifests[0].slices[1].logical_offset, 2);
    assert_eq!(manifests[0].slices[2].logical_offset, 10);
}

#[test]
fn stat_path_sees_append_after_read_during_prepared_publish_window() {
    // Regression for the "concurrent read + append silently drops the last
    // append" visibility bug: prepare pre-allocates the commit version (the
    // clock bump), so a stat between prepare and publish caches the pre-append
    // dentry under the exact version the publish then applies at. The publish
    // never advances the clock past that version, so without apply-time cache
    // purging the poisoned entry is served for the process lifetime.
    let service = service();
    service.create_dir_path("/w", 0o755, 1000, 1000).unwrap();

    let staged_session =
        |prepared: &PreparedArtifact, written: &ChunkedWrite, manifest_id: &str, size: u64| {
            PublishArtifactStagedSession {
                parent: prepared.parent,
                name: prepared.name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "text/plain".to_owned(),
                manifest_id: manifest_id.to_owned(),
                size,
                chunks: written.chunk_manifests(),
                staged: written.staged_objects().unwrap(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            }
        };

    let prepared = service.prepare_artifact_create_path("/w/log.txt").unwrap();
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "log-v1",
            &[PublishArtifactRange {
                offset: 0,
                bytes: b"seg0|".to_vec(),
            }],
            0,
        )
        .unwrap();
    let session = staged_session(&prepared, &written, "log-v1", 5);
    service
        .publish_prepared_artifact_staged_session(prepared, session)
        .unwrap();

    // Append: the replace prepare allocates the commit version V.
    let prepared = service.prepare_artifact_replace_path("/w/log.txt").unwrap();
    // A read inside the staging window resolves at read_version == V and
    // caches the pre-append entry under it.
    let before = service.stat_path("/w/log.txt").unwrap().unwrap();
    assert_eq!(before.attr.size, 5);
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "log-v2",
            &[PublishArtifactRange {
                offset: 5,
                bytes: b"seg1|".to_vec(),
            }],
            0,
        )
        .unwrap();
    let session = staged_session(&prepared, &written, "log-v2", 10);
    service
        .publish_prepared_artifact_staged_session(prepared, session)
        .unwrap();

    // No later write bumps the clock here: the applied publish itself must
    // have invalidated the poisoned entry.
    let after = service.stat_path("/w/log.txt").unwrap().unwrap();
    assert_eq!(after.attr.size, 10);
    assert_eq!(
        service
            .lookup_path("/w/log.txt")
            .unwrap()
            .unwrap()
            .attr
            .size,
        10
    );
}

#[test]
fn delta_publish_writes_only_dirty_chunks_and_preserves_base() {
    let service = service();
    let name = DentryName::new(b"multi.bin".to_vec()).unwrap();

    // Generation 1: a two-chunk file (a few bytes in chunk 0 and chunk 1).
    let create = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let inode = create.inode;
    let g1 = create.generation;
    let written = service
        .stage_prepared_artifact_ranges(
            &create,
            "multi-v1",
            &[
                PublishArtifactRange {
                    offset: 0,
                    bytes: b"aa".to_vec(),
                },
                PublishArtifactRange {
                    offset: DEFAULT_CHUNK_SIZE,
                    bytes: b"bb".to_vec(),
                },
            ],
            0,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();
    let chunks = written.chunk_manifests();
    service
        .publish_prepared_artifact_staged_session(
            create,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "multi-v1".to_owned(),
                size: DEFAULT_CHUNK_SIZE + 2,
                chunks,
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    // Generation 2: overwrite only chunk 0 — a delta over generation 1.
    let replace = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let g2 = replace.generation;
    assert_eq!(replace.old_generation, Some(g1));
    let written2 = service
        .stage_prepared_artifact_ranges(
            &replace,
            "multi-v2",
            &[PublishArtifactRange {
                offset: 0,
                bytes: b"XY".to_vec(),
            }],
            0,
        )
        .unwrap();
    let staged2 = written2.staged_objects().unwrap();
    let chunks2 = written2.chunk_manifests();
    service
        .publish_prepared_artifact_staged_session(
            replace,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "multi-v2".to_owned(),
                size: DEFAULT_CHUNK_SIZE + 2,
                chunks: chunks2,
                staged: staged2,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    let version = service.read_version().unwrap();
    let body = service
        .lookup_path("/multi.bin")
        .unwrap()
        .unwrap()
        .body
        .unwrap();
    assert_eq!(body.generation, g2);
    // The delta falls through to generation 1 for untouched chunks.
    assert_eq!(body.base_generation, g1);

    // O(write): generation 2 stores ONLY the dirty chunk (chunk 0), not chunk 1.
    assert!(service
        .chain_chunk_manifest(inode, &[g2], 0, version, ReadPurpose::UserStrong)
        .unwrap()
        .is_some());
    assert!(service
        .chain_chunk_manifest(inode, &[g2], 1, version, ReadPurpose::UserStrong)
        .unwrap()
        .is_none());

    // The base generation is preserved intact — not eagerly deleted.
    assert!(service
        .chain_chunk_manifest(inode, &[g1], 0, version, ReadPurpose::UserStrong)
        .unwrap()
        .is_some());
    assert!(service
        .chain_chunk_manifest(inode, &[g1], 1, version, ReadPurpose::UserStrong)
        .unwrap()
        .is_some());

    // Reads resolve across the chain: chunk 0 from the delta, chunk 1 inherited.
    assert_eq!(service.read_file(inode, 0, 2).unwrap(), b"XY");
    assert_eq!(
        service.read_file(inode, DEFAULT_CHUNK_SIZE, 2).unwrap(),
        b"bb"
    );
}

fn overwrite_staged(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    prepared: PreparedArtifact,
    name: &DentryName,
    manifest_id: &str,
    offset: u64,
    bytes: &[u8],
    size: u64,
) {
    let parent = prepared.parent;
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            manifest_id,
            &[PublishArtifactRange {
                offset,
                bytes: bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();
    let chunks = written.chunk_manifests();
    service
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent,
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest_id.to_owned(),
                size,
                chunks,
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
}

#[test]
fn final_unlink_cleans_the_full_owner_generation_chain_and_snapshot_protects_objects() {
    let (service, objects) = service_with_objects();
    let workspace = service
        .create_dir_path("/unlink-chain", 0o755, 1000, 1000)
        .unwrap();
    let name = dname(b"artifact.bin");
    let base = publish_path_artifact(
        &service,
        "/unlink-chain/artifact.bin",
        "unlink-chain/base",
        b"base",
    );
    let base_body = base.body.as_ref().unwrap();
    let base_generation = base_body.generation;
    let base_block = block_key(base.attr.inode, base_generation, 0, 0);

    let append = service
        .prepare_artifact_replace(workspace.attr.inode, name.clone())
        .unwrap();
    let append_generation = append.generation;
    overwrite_staged(
        &service,
        append,
        &name,
        "unlink-chain/append",
        base_body.size,
        b"+delta",
        base_body.size + 6,
    );
    let appended = service
        .lookup_path("/unlink-chain/artifact.bin")
        .unwrap()
        .unwrap();
    assert_eq!(
        appended.body.as_ref().unwrap().base_generation,
        base_generation
    );
    let append_block = block_key(appended.attr.inode, append_generation, 0, 0);
    assert!(objects.head(&base_block).unwrap().is_some());
    assert!(objects.head(&append_block).unwrap().is_some());

    let snapshot = service.snapshot_subtree_path("/unlink-chain").unwrap();
    service.remove_file(workspace.attr.inode, &name).unwrap();

    for generation in [base_generation, append_generation] {
        let rows = service
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::ChunkManifest,
                prefix: chunk_manifest_prefix(service.mount, appended.attr.inode, generation),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();
        assert!(
            rows.is_empty(),
            "final unlink left generation {generation} manifests: {rows:?}"
        );
    }
    let queued = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .into_iter()
        .map(|row| decode_object_gc_record(&row.value.0).unwrap().object_key)
        .collect::<HashSet<_>>();
    assert!(queued.contains(base_block.as_str()));
    assert!(queued.contains(append_block.as_str()));

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 0, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&base_block).unwrap().is_some());
    assert!(objects.head(&append_block).unwrap().is_some());

    assert!(service
        .retire_snapshot_path("/unlink-chain", snapshot.snapshot_id)
        .unwrap());
    for _ in 0..8 {
        service.cleanup_pending_objects(100).unwrap();
        if objects.head(&base_block).unwrap().is_none()
            && objects.head(&append_block).unwrap().is_none()
        {
            break;
        }
    }
    assert!(objects.head(&base_block).unwrap().is_none());
    assert!(objects.head(&append_block).unwrap().is_none());
}

#[test]
fn rename_replace_cleans_the_full_victim_generation_chain() {
    let service = service();
    let victim = publish_path_artifact(&service, "/victim.bin", "rename-chain/base", b"victim");
    let victim_body = victim.body.as_ref().unwrap();
    let base_generation = victim_body.generation;
    let base_block = block_key(victim.attr.inode, base_generation, 0, 0);
    let name = dname(b"victim.bin");
    let append = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let append_generation = append.generation;
    overwrite_staged(
        &service,
        append,
        &name,
        "rename-chain/append",
        victim_body.size,
        b"+delta",
        victim_body.size + 6,
    );
    let append_block = block_key(victim.attr.inode, append_generation, 0, 0);
    let source = publish_path_artifact(
        &service,
        "/source.bin",
        "rename-chain/source",
        b"replacement",
    );

    let outcome = service
        .rename_replace_path("/source.bin", "/victim.bin")
        .unwrap();
    assert_eq!(
        outcome.replaced.as_ref().unwrap().attr.inode,
        victim.attr.inode
    );
    assert_eq!(
        service
            .lookup_path("/victim.bin")
            .unwrap()
            .unwrap()
            .attr
            .inode,
        source.attr.inode
    );
    for generation in [base_generation, append_generation] {
        assert!(
            service
                .metadata
                .scan(ScanRequest {
                    family: RecordFamily::ChunkManifest,
                    prefix: chunk_manifest_prefix(service.mount, victim.attr.inode, generation),
                    start_after: None,
                    version: service.read_version().unwrap(),
                    limit: 0,
                    purpose: ReadPurpose::UserStrong,
                })
                .unwrap()
                .is_empty(),
            "rename-replace left victim generation {generation} manifests"
        );
    }
    let queued = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .into_iter()
        .map(|row| decode_object_gc_record(&row.value.0).unwrap().object_key)
        .collect::<HashSet<_>>();
    assert!(queued.contains(base_block.as_str()));
    assert!(queued.contains(append_block.as_str()));
}

#[test]
fn final_unlink_enqueues_a_compaction_retained_old_generation_object() {
    let (service, objects) = service_with_objects();
    let name = dname(b"compacted.bin");
    let create = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let inode = create.inode;
    let base_generation = create.generation;
    let written = service
        .stage_prepared_artifact_ranges(
            &create,
            "final-unlink-compaction/base",
            &[
                PublishArtifactRange {
                    offset: 0,
                    bytes: b"aa".to_vec(),
                },
                PublishArtifactRange {
                    offset: DEFAULT_CHUNK_SIZE,
                    bytes: b"bb".to_vec(),
                },
            ],
            0,
        )
        .unwrap();
    service
        .publish_prepared_artifact_staged_session(
            create,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:compaction-base".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "final-unlink-compaction/base".to_owned(),
                size: DEFAULT_CHUNK_SIZE + 2,
                chunks: written.chunk_manifests(),
                staged: written.staged_objects().unwrap(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    let retained_old_block = block_key(inode, base_generation, 1, 0);

    let mut compacted_generation = None;
    for index in 1..=12 {
        let replace = service
            .prepare_artifact_replace(InodeId::root(), name.clone())
            .unwrap();
        overwrite_staged(
            &service,
            replace,
            &name,
            &format!("final-unlink-compaction/{index}"),
            0,
            &[b'a' + index as u8, b'a' + index as u8],
            DEFAULT_CHUNK_SIZE + 2,
        );
        let body = service
            .lookup_path("/compacted.bin")
            .unwrap()
            .unwrap()
            .body
            .unwrap();
        if body.generation != base_generation && body.base_generation == 0 {
            compacted_generation = Some(body.generation);
            break;
        }
    }
    let compacted_generation = compacted_generation.expect("delta chain must compact");
    assert!(objects.head(&retained_old_block).unwrap().is_some());
    assert!(service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(service.mount, inode, base_generation),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .is_empty());
    let compacted_manifests = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(service.mount, inode, compacted_generation),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();
    assert!(compacted_manifests.iter().any(|row| {
        chunk_index_from_manifest_key(&row.key)
            .ok()
            .filter(|chunk| *chunk != BODY_SUMMARY_CHUNK_INDEX)
            .and_then(|_| decode_chunk_manifest(&row.value.0).ok())
            .is_some_and(|manifest| {
                manifest
                    .slices
                    .iter()
                    .flat_map(|slice| &slice.blocks)
                    .any(|block| block.object_key == retained_old_block.as_str())
            })
    }));

    service.remove_file(InodeId::root(), &name).unwrap();
    let queued = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .into_iter()
        .map(|row| decode_object_gc_record(&row.value.0).unwrap().object_key)
        .collect::<HashSet<_>>();
    assert!(
        queued.contains(retained_old_block.as_str()),
        "final unlink must enqueue an old-generation object retained by the compacted manifest"
    );
    // Staging the delta chain may install short read leases while it merges
    // prior slices. Advance past that grace period, then prove this is not only
    // a metadata-queue assertion: the regular durable GC worker deletes the
    // retained old-generation object after the final namespace reference dies.
    service.set_clock_override_ms(
        service
            .now_ms()
            .saturating_add(DEFAULT_READ_LEASE_MS)
            .saturating_add(1),
    );
    for _ in 0..64 {
        service.cleanup_pending_objects(256).unwrap();
        if objects.head(&retained_old_block).unwrap().is_none() {
            break;
        }
    }
    assert!(
        objects.head(&retained_old_block).unwrap().is_none(),
        "GC must physically delete the compacted manifest's retained old-generation object"
    );
}

#[test]
fn final_unlink_skips_clone_and_restore_borrowed_objects() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let source = publish_path_artifact(
        &service,
        "/source/data.bin",
        "final-unlink-borrowed/source",
        b"borrowed",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);

    let clone = service
        .clone_subtree_path_into("/source", "/clone")
        .unwrap();
    let clone_file = service.lookup_path("/clone/data.bin").unwrap().unwrap();
    let append = service
        .prepare_artifact_replace(clone.root, dname(b"data.bin"))
        .unwrap();
    let append_generation = append.generation;
    overwrite_staged(
        &service,
        append,
        &dname(b"data.bin"),
        "final-unlink-borrowed/clone-delta",
        source_body.size,
        b"+owned",
        source_body.size + 6,
    );
    let clone_owned_block = block_key(clone_file.attr.inode, append_generation, 0, 0);
    service.remove_file_path("/clone/data.bin").unwrap();

    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();
    service.remove_file_path("/restored/data.bin").unwrap();

    let queued = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .into_iter()
        .map(|row| decode_object_gc_record(&row.value.0).unwrap().object_key)
        .collect::<HashSet<_>>();
    assert!(queued.contains(clone_owned_block.as_str()));
    assert!(
        !queued.contains(source_block.as_str()),
        "clone/restore borrowers must not enqueue the source-owned object"
    );
}

#[test]
fn delta_chain_compacts_to_self_contained_at_depth_threshold() {
    let service = service();
    let name = DentryName::new(b"hot.bin".to_vec()).unwrap();
    let create = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let inode = create.inode;
    overwrite_staged(&service, create, &name, "hot-0", 0, b"AAAA", 4);

    // Overwrite the same region many times. Each delta extends the fall-through
    // chain by one; at the depth threshold the publish must re-materialize a
    // self-contained generation (base_generation == 0) instead of growing the
    // chain without bound. Every read must stay correct throughout.
    let mut saw_compaction = false;
    for i in 1..=12u32 {
        let replace = service
            .prepare_artifact_replace(InodeId::root(), name.clone())
            .unwrap();
        let byte = b'A' + (i % 16) as u8;
        let want = [byte; 4];
        overwrite_staged(&service, replace, &name, &format!("hot-{i}"), 0, &want, 4);
        assert_eq!(service.read_file(inode, 0, 4).unwrap(), want.to_vec());
        let body = service
            .lookup_path("/hot.bin")
            .unwrap()
            .unwrap()
            .body
            .unwrap();
        if body.base_generation == 0 {
            saw_compaction = true;
            // Compaction must coalesce the hot chunk's accumulated slices, not
            // just collapse the chain — otherwise slice count grows unbounded
            // across compaction cycles. The fully-overwritten chunk collapses to
            // a single newest-wins slice.
            let chunk0 = service
                .chain_chunk_manifest(
                    inode,
                    &[body.generation],
                    0,
                    service.read_version().unwrap(),
                    ReadPurpose::UserStrong,
                )
                .unwrap()
                .unwrap();
            assert_eq!(chunk0.slices.len(), 1);
        }
    }
    assert!(
        saw_compaction,
        "deep delta chain must compact to a self-contained generation"
    );
}

#[test]
fn compacting_staged_publish_exact_retry_uses_stable_terminal_identity() {
    let service = service();
    let name = DentryName::new(b"compaction-retry.bin".to_vec()).unwrap();
    let create = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let inode = create.inode;
    overwrite_staged(&service, create, &name, "compaction-retry-0", 0, b"0000", 4);
    for index in 1..8 {
        let prepared = service
            .prepare_artifact_replace(InodeId::root(), name.clone())
            .unwrap();
        let bytes = [b'0' + index; 4];
        overwrite_staged(
            &service,
            prepared,
            &name,
            &format!("compaction-retry-{index}"),
            0,
            &bytes,
            4,
        );
    }

    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "compaction-retry-8",
            &[PublishArtifactRange {
                offset: 0,
                bytes: b"8888".to_vec(),
            }],
            0,
        )
        .unwrap();
    let session = PublishArtifactStagedSession {
        parent: prepared.parent,
        name: prepared.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:compaction-retry".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "compaction-retry-8".to_owned(),
        size: 4,
        chunks: written.chunk_manifests(),
        staged: written.staged_objects().unwrap(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };
    let first = service
        .publish_prepared_artifact_staged_session(prepared.clone(), session.clone())
        .unwrap();
    assert_eq!(first.entry.body.as_ref().unwrap().base_generation, 0);
    let stats = service.metadata_store_stats();

    let retry = service
        .publish_prepared_artifact_staged_session(prepared, session)
        .unwrap();

    assert_eq!(retry.entry, first.entry);
    assert!(retry.replaced.is_none());
    let retry_stats = service.metadata_store_stats();
    assert_eq!(retry_stats.commit_total, stats.commit_total);
    assert_eq!(retry_stats.dedupe_write_total, stats.dedupe_write_total);
    assert_eq!(retry_stats.current_put_total, stats.current_put_total);
    assert_eq!(retry_stats.current_delete_total, stats.current_delete_total);
    assert_eq!(retry_stats.history_write_total, stats.history_write_total);
    assert_eq!(retry_stats.watch_write_total, stats.watch_write_total);
    assert_eq!(service.read_file(inode, 0, 4).unwrap(), b"8888");
}

#[test]
fn chain_collapse_gc_is_snapshot_safe() {
    let service = service();
    let name = DentryName::new(b"snap.bin".to_vec()).unwrap();
    let create = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let inode = create.inode;
    overwrite_staged(&service, create, &name, "snap-0", 0, b"AAAA", 4);

    // Pin generation 1, then overwrite enough to trigger a chain-collapse
    // compaction. The compaction enqueues the superseded chain blocks for GC.
    let pin = service.snapshot_subtree(InodeId::root()).unwrap();
    for i in 1..=10u32 {
        let replace = service
            .prepare_artifact_replace(InodeId::root(), name.clone())
            .unwrap();
        let byte = b'A' + (i % 16) as u8;
        overwrite_staged(
            &service,
            replace,
            &name,
            &format!("snap-{i}"),
            0,
            &[byte; 4],
            4,
        );
    }

    // The snapshot still resolves generation-1 content, and a GC pass must NOT
    // delete any block the snapshot can still reach — the version retention
    // floor blocks reclamation of everything enqueued after the snapshot.
    assert_eq!(
        service
            .read_file_at_snapshot("/", pin.snapshot_id, std::slice::from_ref(&name), 0, 4)
            .unwrap(),
        b"AAAA"
    );
    let blocked = service.cleanup_pending_objects(1024).unwrap();
    assert!(
        blocked.blocked_by_snapshots > 0,
        "snapshot must block reclamation of still-reachable chain blocks"
    );
    assert_eq!(blocked.deleted, 0);
    // Snapshot read still works after the (blocked) GC pass.
    assert_eq!(
        service
            .read_file_at_snapshot("/", pin.snapshot_id, std::slice::from_ref(&name), 0, 4)
            .unwrap(),
        b"AAAA"
    );

    // Retiring the snapshot raises the floor; the superseded chain blocks now
    // reclaim — proving the whole chain (not just its top) was enqueued.
    assert!(service.retire_snapshot(pin.snapshot_id).unwrap());
    let reclaimed = service.cleanup_pending_objects(1024).unwrap();
    assert!(
        reclaimed.deleted > 0,
        "retiring the snapshot must let superseded chain blocks reclaim"
    );

    // The live file reads correctly throughout (last write was i=10 -> 'K').
    assert_eq!(service.read_file(inode, 0, 4).unwrap(), b"KKKK");
}

#[test]
fn replace_artifact_preserves_inode_and_returns_old_body() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();

    assert_eq!(replaced.entry.attr.inode, first.attr.inode);
    assert!(replaced.entry.attr.generation > first.attr.generation);
    assert_eq!(replaced.replaced, Some(first.clone()));
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(replaced.entry.clone())
    );
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
    assert_eq!(
        replaced.replaced.unwrap().body.unwrap().manifest_id,
        "checkpoint/old"
    );
}

#[test]
fn get_attr_reads_root_inode() {
    let service = service();
    let root = service.get_attr(InodeId::root()).unwrap().unwrap();
    assert_eq!(root.inode, InodeId::root());
    assert_eq!(root.file_type, FileType::Directory);
}

#[test]
fn remove_file_deletes_namespace_and_returns_old_body() {
    let service = service();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed, published);
    assert_eq!(removed.body.as_ref().unwrap().manifest_id, "artifact.bin");
    assert!(service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .is_none());
    assert!(service.get_attr(removed.attr.inode).unwrap().is_none());
}

#[test]
fn hardlink_updates_link_count_and_defers_body_gc_until_last_unlink() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let link_name = DentryName::new(b"artifact.link".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();
    let body = published.body.clone().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);

    let linked = service
        .link(published.attr.inode, InodeId::root(), link_name.clone())
        .unwrap();
    assert_eq!(linked.attr.inode, published.attr.inode);
    assert_eq!(linked.attr.nlink, 2);
    assert_eq!(
        service
            .lookup_plus(InodeId::root(), &name)
            .unwrap()
            .unwrap()
            .attr
            .nlink,
        2
    );
    assert_eq!(
        service
            .lookup_plus(InodeId::root(), &link_name)
            .unwrap()
            .unwrap()
            .attr
            .nlink,
        2
    );

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed.attr.inode, published.attr.inode);
    assert!(service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .is_none());
    let remaining = service
        .lookup_plus(InodeId::root(), &link_name)
        .unwrap()
        .unwrap();
    assert_eq!(remaining.attr.nlink, 1);
    assert_eq!(
        service
            .get_attr(published.attr.inode)
            .unwrap()
            .unwrap()
            .nlink,
        1
    );
    assert_eq!(
        service.read_artifact(InodeId::root(), &link_name).unwrap(),
        b"old"
    );
    assert!(objects.head(&object).unwrap().is_some());
    assert_eq!(
        service.cleanup_pending_objects(100).unwrap(),
        PendingObjectCleanupOutcome::default()
    );

    let removed_last = service.remove_file(InodeId::root(), &link_name).unwrap();
    assert_eq!(removed_last.attr.inode, published.attr.inode);
    assert!(service.get_attr(published.attr.inode).unwrap().is_none());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&object).unwrap().is_none());
}

#[test]
fn hardlink_rejects_directories() {
    let service = service();
    let dir = service
        .create_dir(
            InodeId::root(),
            DentryName::new(b"dir".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    let err = service
        .link(
            dir.attr.inode,
            InodeId::root(),
            DentryName::new(b"dir-link".to_vec()).unwrap(),
        )
        .unwrap_err();
    assert!(matches!(err, MetadError::NotFile));
}

#[test]
fn remove_file_queues_old_body_for_object_cleanup() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();
    let body = published.body.clone().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    assert!(objects.head(&object).unwrap().is_some());

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed, published);
    assert!(objects.head(&object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.scanned, 1);
    assert_eq!(cleanup.attempted, 1);
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.missing, 0);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_none());
    assert_eq!(
        service.cleanup_pending_objects(100).unwrap(),
        PendingObjectCleanupOutcome::default()
    );
}

#[test]
fn gc_uses_the_canonical_nonzero_object_block_index() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"sparse-gc.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let inode = prepared.inode;
    let generation = prepared.generation;
    let block_index = 7_u64;
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "sparse-gc",
            &[PublishArtifactRange {
                offset: 0,
                bytes: b"sparse".to_vec(),
            }],
            block_index,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();
    let chunks = written.chunk_manifests();
    service
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "sparse-gc".to_owned(),
                size: 6,
                chunks,
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    let object = block_key(inode, generation, 0, block_index);
    assert!(objects.head(&object).unwrap().is_some());
    service.remove_file(InodeId::root(), &name).unwrap();

    let rows = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();
    assert_eq!(rows.len(), 1);
    let key = decode_object_gc_record_key(service.mount, &rows[0].key).unwrap();
    assert_eq!(key.chunk_index, 0);
    assert_eq!(key.block_index, block_index);

    let cleanup = service.cleanup_pending_objects(10).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_none());
    assert!(service
        .metadata
        .scan_keys(KeyScanRequest {
            family: RecordFamily::System,
            prefix: object_gc_quarantine_prefix(service.mount),
            start_after: None,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .is_empty());
}

#[test]
fn durable_object_gc_claim_codec_round_trips_and_rejects_trailing_bytes() {
    let mount = MountId::new(1).unwrap();
    let deleting = ObjectGcClaim::Deleting {
        owner_epoch: 7,
        operation_token: 11,
        gc_record_key: gc_object_key(mount, 10, InodeId::new(2).unwrap(), 3, 4, 5),
        gc_record_version: 13,
    };
    assert_eq!(
        decode_object_gc_claim(
            mount,
            &encode_object_gc_claim(&ObjectGcClaim::Open).unwrap()
        )
        .unwrap(),
        ObjectGcClaim::Open
    );
    assert_eq!(
        decode_object_gc_claim(mount, &encode_object_gc_claim(&deleting).unwrap()).unwrap(),
        deleting
    );
    let mut malformed = encode_object_gc_claim(&deleting).unwrap();
    malformed.push(0);
    assert!(matches!(
        decode_object_gc_claim(mount, &malformed),
        Err(MetadError::Codec(_))
    ));
}

#[test]
fn durable_object_gc_claim_codec_rejects_zero_identity_fields() {
    let mount = MountId::new(1).unwrap();
    let gc_record_key = gc_object_key(mount, 10, InodeId::new(2).unwrap(), 3, 4, 5);
    for invalid in [
        ObjectGcClaim::Deleting {
            owner_epoch: 0,
            operation_token: 11,
            gc_record_key: gc_record_key.clone(),
            gc_record_version: 13,
        },
        ObjectGcClaim::Deleting {
            owner_epoch: 7,
            operation_token: 0,
            gc_record_key: gc_record_key.clone(),
            gc_record_version: 13,
        },
        ObjectGcClaim::Deleting {
            owner_epoch: 7,
            operation_token: 11,
            gc_record_key: gc_record_key.clone(),
            gc_record_version: 0,
        },
    ] {
        assert!(matches!(
            decode_object_gc_claim(mount, &encode_object_gc_claim(&invalid).unwrap()),
            Err(MetadError::Codec(_))
        ));
    }
}

#[test]
fn durable_object_gc_claim_codec_rejects_non_local_or_malformed_gc_keys() {
    let mount = MountId::new(1).unwrap();
    let valid_key = gc_object_key(mount, 10, InodeId::new(2).unwrap(), 3, 4, 5);
    let mut invalid_keys = vec![
        gc_object_key(
            MountId::new(2).unwrap(),
            10,
            InodeId::new(2).unwrap(),
            3,
            4,
            5,
        ),
        gc_queue_prefix(mount),
    ];
    for field in [1, 2, 3] {
        let mut key = valid_key.clone();
        key[field * 8..(field + 1) * 8].fill(0);
        invalid_keys.push(key);
    }

    for gc_record_key in invalid_keys {
        let invalid = ObjectGcClaim::Deleting {
            owner_epoch: 7,
            operation_token: 11,
            gc_record_key,
            gc_record_version: 13,
        };
        assert!(matches!(
            decode_object_gc_claim(mount, &encode_object_gc_claim(&invalid).unwrap()),
            Err(MetadError::Codec(_))
        ));
    }
}

#[test]
fn durable_gc_claim_blocks_new_object_reference_planning_while_deleting() {
    let metadata = PausingObjectGcStore::new();
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"old".to_vec()).unwrap(),
            "old",
            b"old",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    let resize_name = DentryName::new(b"resize".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(
            resize_name.clone(),
            "resize",
            b"resize-body",
        ))
        .unwrap();
    service.remove_file_path("/old").unwrap();

    metadata.arm();
    let cleaner = {
        let service = Arc::clone(&service);
        std::thread::spawn(move || service.cleanup_pending_objects(1))
    };
    metadata.wait_until_reached();

    assert!(matches!(
        service.prepare_artifact_create(InodeId::root(), DentryName::new(b"new".to_vec()).unwrap()),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(matches!(
        service.publish_artifact(artifact_request(
            DentryName::new(b"new-publish".to_vec()).unwrap(),
            "new-publish",
            b"new"
        )),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(matches!(
        service.create_symlink(
            InodeId::root(),
            DentryName::new(b"new-link".to_vec()).unwrap(),
            b"target".to_vec(),
            0o777,
            1000,
            1000
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(matches!(
        service.create_symlink(
            InodeId::root(),
            DentryName::new(b"invalid-link".to_vec()).unwrap(),
            Vec::new(),
            0o777,
            1000,
            1000
        ),
        Err(MetadError::InvalidPath(_))
    ));
    assert!(matches!(
        service.update_attrs(
            InodeId::root(),
            &resize_name,
            UpdateAttr {
                size: Some(1),
                ..UpdateAttr::default()
            }
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(matches!(
        service.publish_checkpoint(
            InodeId::root(),
            vec![CheckpointShard {
                name: DentryName::new(b"checkpoint-shard".to_vec()).unwrap(),
                bytes: b"checkpoint".to_vec(),
            }],
            1000,
            1000
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(matches!(
        service.snapshot_subtree(InodeId::root()),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    let attr_only = service
        .update_attrs(
            InodeId::root(),
            &resize_name,
            UpdateAttr {
                mode: Some(0o600),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert_eq!(attr_only.attr.mode, 0o600);
    metadata.release();
    let cleaned = cleaner.join().unwrap().unwrap();
    assert_eq!(cleaned.deleted, 1);
    assert!(objects.head(&object).unwrap().is_none());
}

#[test]
fn gc_recheck_preserves_an_object_referenced_by_the_current_manifest() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"live".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "live", b"live"))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: published.attr.inode,
            generation: body.generation,
            object_key: object.as_str().to_owned(),
            size: body.size,
            digest_uri: body.digest_uri.clone(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );

    let claim_key = object_gc_claim_key(service.mount);
    let claim_version_before = service
        .metadata
        .get_versioned(
            RecordFamily::System,
            &claim_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap()
        .version;

    let cleanup = service.cleanup_pending_objects(1).unwrap();
    assert_eq!(cleanup.blocked_by_snapshots, 1);
    assert_eq!(cleanup.deleted, 0);
    assert_eq!(cleanup.records_removed, 0);
    assert!(objects.head(&object).unwrap().is_some());
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"live"
    );

    // The first pass advances the durable cursor, the second reaches the tail
    // and resets it, and the third sees the protected row again. None of those
    // advisory scans may rotate the global object-reference epoch.
    assert_eq!(service.cleanup_pending_objects(1).unwrap().scanned, 0);
    assert_eq!(
        service
            .cleanup_pending_objects(1)
            .unwrap()
            .blocked_by_snapshots,
        1
    );
    let claim_version_after = service
        .metadata
        .get_versioned(
            RecordFamily::System,
            &claim_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap()
        .version;
    assert_eq!(claim_version_after, claim_version_before);
}

#[test]
fn snapshot_mint_retries_after_an_intervening_object_delete_epoch() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::SnapshotSubtree, 1, 2);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"snapshot-race".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "snapshot-race", b"old"))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);

    let mint_service = Arc::clone(&service);
    let mint = std::thread::spawn(move || mint_service.snapshot_subtree(InodeId::root()));
    store.wait_until_blocked();
    service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(service.cleanup_pending_objects(1).unwrap().deleted, 1);
    assert!(objects.head(&object).unwrap().is_none());
    store.release_blocked();

    let snapshot = mint.join().unwrap().unwrap();
    assert!(matches!(
        service.read_artifact_path_at_snapshot("/", snapshot.snapshot_id, "/snapshot-race"),
        Err(MetadError::NotFound)
    ));
}

#[test]
fn reopen_resumes_a_durable_object_gc_claim_after_crash() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"old".to_vec()).unwrap(),
            "old",
            b"old",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file_path("/old").unwrap();
    let gc_row = metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .pop()
        .unwrap();
    let claim_key = leave_object_gc_deleting(&service, &gc_row);
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    assert!(objects.head(&object).unwrap().is_some());
    let deferred_claim = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_object_gc_claim(reopened.mount, &deferred_claim.0).unwrap(),
        ObjectGcClaim::Deleting { .. }
    ));

    let cleanup = reopened.cleanup_pending_objects(1).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_none());
    let open_claim = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_object_gc_claim(reopened.mount, &open_claim.0).unwrap(),
        ObjectGcClaim::Open
    );
}

#[test]
fn history_cleanup_does_not_invalidate_a_prepared_object_upload() {
    let (service, _) = service_with_objects();
    let name = DentryName::new(b"upload-during-history-prune.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();

    service.cleanup_history(100).unwrap();
    assert_eq!(
        service
            .refresh_prepared_artifact_object_gc_epoch(prepared.clone())
            .unwrap(),
        prepared
    );

    let bytes = b"history-prune-independent";
    service
        .publish_prepared_artifact_session(
            prepared,
            PublishArtifactSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:history-prune-independent".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "history-prune-independent".to_owned(),
                size: bytes.len() as u64,
                ranges: vec![PublishArtifactRange {
                    offset: 0,
                    bytes: bytes.to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        bytes
    );
}

#[test]
fn prepared_publish_rejects_an_intervening_object_gc_epoch_and_refreshes_identity() {
    let (service, objects) = service_with_objects();
    let victim_name = DentryName::new(b"gc-victim".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(
            victim_name.clone(),
            "gc-victim",
            b"victim",
        ))
        .unwrap();
    service.remove_file(InodeId::root(), &victim_name).unwrap();

    let name = DentryName::new(b"late-after-gc".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let bytes = b"late-after-gc";
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "late-after-gc",
            &[PublishArtifactRange {
                offset: 0,
                bytes: bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();

    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 1);
    let error = service
        .publish_prepared_artifact_staged_session(
            prepared.clone(),
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:late-after-gc".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "late-after-gc".to_owned(),
                size: bytes.len() as u64,
                chunks: written.chunk_manifests(),
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap_err();
    let staged = match error {
        MetadError::PublishArtifactFailed { source, staged } => {
            assert!(matches!(
                *source,
                MetadError::StalePreparedArtifactObjectGcEpoch { .. }
            ));
            staged
        }
        error => panic!("unexpected prepared publish error: {error:?}"),
    };
    assert!(service.lookup_path("/late-after-gc").unwrap().is_none());
    let cleanup = service.cleanup_staged_objects(&staged).unwrap();
    assert_eq!(cleanup.deleted, staged.len());
    for object in staged.objects() {
        assert!(objects.head(&object.key).unwrap().is_none());
    }

    let refreshed = service
        .refresh_prepared_artifact_object_gc_epoch(prepared.clone())
        .unwrap();
    assert!(refreshed.generation > prepared.generation);
    assert_ne!(
        refreshed.object_gc_claim_version,
        prepared.object_gc_claim_version
    );
}

#[test]
fn failover_claim_rotation_rejects_old_prepared_upload_and_allows_cleanup() {
    let (source, objects) = service_with_objects();
    let name = dname(b"prepared-before-failover.bin");
    let prepared = source
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let bytes = b"staged by the failed owner";
    let written = source
        .stage_prepared_artifact_ranges(
            &prepared,
            "prepared-before-failover",
            &[PublishArtifactRange {
                offset: 0,
                bytes: bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let staged_before_failover = written.staged_objects().unwrap();
    let staged_key = staged_before_failover.objects()[0].key.clone();
    assert!(objects.head(&staged_key).unwrap().is_some());

    let checkpoint = MetadataArchiveConfig::new("meta/prepared-failover", 2);
    source.backup_metadata(&checkpoint).unwrap();
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    // Match controlled startup: install the acquired epoch before restoring the
    // old owner's checkpoint, recover any crash-left deletion, then rotate Open.
    recovered.install_owner_epoch(2).unwrap();
    recovered.restore_metadata(&checkpoint).unwrap().unwrap();
    recovered.recover_object_gc_claim().unwrap();
    recovered.rotate_object_gc_claim_for_failover().unwrap();

    let error = recovered
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:prepared-before-failover".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "prepared-before-failover".to_owned(),
                size: bytes.len() as u64,
                chunks: written.chunk_manifests(),
                staged: staged_before_failover,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap_err();
    let staged = match error {
        MetadError::PublishArtifactFailed { source, staged } => {
            assert!(matches!(
                *source,
                MetadError::StalePreparedArtifactObjectGcEpoch { .. }
                    | MetadError::Metadata(MetadataError::PredicateFailed)
            ));
            staged
        }
        other => panic!("unexpected old prepared publish error: {other:?}"),
    };
    assert!(recovered
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .is_none());
    recovered.cleanup_staged_objects(&staged).unwrap();
    assert!(objects.head(&staged_key).unwrap().is_none());
}

#[test]
fn failover_claim_rotation_confirms_a_lost_backend_ack_by_exact_readback() {
    let metadata = PostCommitErrorStore::new_disarmed(CommandKind::CleanupObjects);
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), dname(b"old-token.bin"))
        .unwrap();
    let old_claim_version = prepared.object_gc_claim_version;

    metadata.arm();
    service.rotate_object_gc_claim_for_failover().unwrap();
    let refreshed = service
        .refresh_prepared_artifact_object_gc_epoch(prepared)
        .unwrap();
    assert_ne!(refreshed.object_gc_claim_version, old_claim_version);
}

#[test]
fn blocked_head_gc_row_does_not_starve_later_reclaimable_candidate() {
    let (service, objects) = service_with_objects();
    let live_name = DentryName::new(b"live-head".to_vec()).unwrap();
    let live = service
        .publish_artifact(artifact_request(live_name, "live-head", b"live"))
        .unwrap();
    let live_body = live.body.as_ref().unwrap();
    let live_object = block_key(live.attr.inode, live_body.generation, 0, 0);
    enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: live.attr.inode,
            generation: live_body.generation,
            object_key: live_object.as_str().to_owned(),
            size: live_body.size,
            digest_uri: live_body.digest_uri.clone(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );

    let reclaimable = ObjectKey::new("blocks/1/900/1/0/0").unwrap();
    objects.put(&reclaimable, b"stale".to_vec()).unwrap();
    enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: InodeId::new(900).unwrap(),
            generation: 1,
            object_key: reclaimable.as_str().to_owned(),
            size: 5,
            digest_uri: "sha256:stale".to_owned(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );

    let first = service.cleanup_pending_objects(1).unwrap();
    assert_eq!(first.scanned, 1);
    assert_eq!(first.blocked_by_snapshots, 1);
    assert_eq!(first.deleted, 0);
    assert!(objects.head(&live_object).unwrap().is_some());
    assert!(objects.head(&reclaimable).unwrap().is_some());

    let second = service.cleanup_pending_objects(1).unwrap();
    assert_eq!(second.scanned, 1);
    assert_eq!(second.deleted, 1);
    assert!(objects.head(&live_object).unwrap().is_some());
    assert!(objects.head(&reclaimable).unwrap().is_none());
}

#[test]
fn local_gc_row_key_cannot_delete_an_object_owned_by_another_mount() {
    let (service, objects) = service_with_objects();
    let foreign_object = ObjectKey::new("blocks/2/902/1/0/0").unwrap();
    objects.put(&foreign_object, b"foreign".to_vec()).unwrap();
    let row_key = enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: InodeId::new(902).unwrap(),
            generation: 1,
            object_key: foreign_object.as_str().to_owned(),
            size: 7,
            digest_uri: "sha256:foreign".to_owned(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );
    assert_eq!(row_key.len(), 48);
    assert!(row_key.starts_with(&gc_queue_prefix(service.mount)));

    let outcome = service.cleanup_pending_objects(1).unwrap();
    assert_eq!(outcome.attempted, 0);
    assert_eq!(outcome.deleted, 0);
    assert_eq!(outcome.records_removed, 1);
    assert!(objects.head(&foreign_object).unwrap().is_some());
    assert!(service
        .metadata
        .get(
            RecordFamily::Gc,
            &row_key,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert_eq!(
        service
            .metadata
            .scan_keys(KeyScanRequest {
                family: RecordFamily::System,
                prefix: object_gc_quarantine_prefix(service.mount),
                start_after: None,
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn gc_row_key_cannot_delete_a_different_block_of_the_same_generation() {
    let (service, objects) = service_with_objects();
    let inode = InodeId::new(904).unwrap();
    let object = ObjectKey::new("blocks/1/904/1/0/0").unwrap();
    objects.put(&object, b"protected".to_vec()).unwrap();

    let version = service.next_version().unwrap();
    let row_key = gc_object_key(service.mount, version.get(), inode, 1, 0, 1);
    let record = ObjectGcRecord {
        inode,
        generation: 1,
        object_key: object.as_str().to_owned(),
        size: 9,
        digest_uri: "sha256:protected".to_owned(),
        enqueue_version: version.get(),
        enqueue_unix_ms: service.now_ms(),
    };
    service
        .commit_metadata(MetadataCommand {
            request_id: b"mismatched-block-gc-row".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::Gc,
            primary_key: row_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Gc,
                key: row_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Gc,
                key: row_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_object_gc_record(&record))),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let outcome = service.cleanup_pending_objects(1).unwrap();
    assert_eq!(outcome.attempted, 0);
    assert_eq!(outcome.deleted, 0);
    assert_eq!(outcome.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_some());
    assert!(service
        .metadata
        .get(
            RecordFamily::Gc,
            &row_key,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert_eq!(
        service
            .metadata
            .scan_keys(KeyScanRequest {
                family: RecordFamily::System,
                prefix: object_gc_quarantine_prefix(service.mount),
                start_after: None,
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn crash_left_gc_claim_quarantines_a_foreign_object_key_without_deleting_it() {
    let (service, objects) = service_with_objects();
    let metadata = service.metadata.clone();
    let foreign_object = ObjectKey::new("blocks/2/903/1/0/0").unwrap();
    objects.put(&foreign_object, b"foreign".to_vec()).unwrap();
    let row_key = enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: InodeId::new(903).unwrap(),
            generation: 1,
            object_key: foreign_object.as_str().to_owned(),
            size: 7,
            digest_uri: "sha256:foreign".to_owned(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );
    let row = metadata
        .get_versioned(
            RecordFamily::Gc,
            &row_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    leave_object_gc_deleting(
        &service,
        &ScanItem {
            key: row_key.clone(),
            value: row.value,
            version: row.version,
        },
    );
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    let outcome = reopened.recover_object_gc_claim().unwrap();
    assert_eq!(outcome.attempted, 0);
    assert_eq!(outcome.deleted, 0);
    assert_eq!(outcome.records_removed, 1);
    assert!(objects.head(&foreign_object).unwrap().is_some());
    assert!(metadata
        .get(
            RecordFamily::Gc,
            &row_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert_eq!(
        metadata
            .scan_keys(KeyScanRequest {
                family: RecordFamily::System,
                prefix: object_gc_quarantine_prefix(reopened.mount),
                start_after: None,
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
    let claim = metadata
        .get(
            RecordFamily::System,
            &object_gc_claim_key(reopened.mount),
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_object_gc_claim(reopened.mount, &claim.0).unwrap(),
        ObjectGcClaim::Open
    );
}

#[test]
fn delete_error_keeps_the_durable_claim_closed_until_same_claim_recovery() {
    let backing = MemoryObjectStore::new();
    let objects = DeleteAckLostObjectStore::new(backing.clone());
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"lost-delete-ack.bin".to_vec()).unwrap(),
            "lost-delete-ack",
            b"payload",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service
        .remove_file(
            InodeId::root(),
            &DentryName::new(b"lost-delete-ack.bin".to_vec()).unwrap(),
        )
        .unwrap();

    assert!(matches!(
        service.cleanup_pending_objects(1),
        Err(MetadError::Object(ObjectError::Backend(message)))
            if message == "injected lost DELETE acknowledgement"
    ));
    assert_eq!(objects.delete_calls(), 1);
    assert!(backing.head(&object).unwrap().is_none());
    let claim_key = object_gc_claim_key(service.mount);
    let deleting = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_object_gc_claim(service.mount, &deleting.0).unwrap(),
        ObjectGcClaim::Deleting { .. }
    ));
    assert!(matches!(
        service.begin_object_reference_mutation(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert_eq!(
        metadata
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(service.mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );

    let recovered = service.recover_object_gc_claim().unwrap();
    assert_eq!(objects.delete_calls(), 2);
    assert_eq!(recovered.attempted, 1);
    assert_eq!(recovered.missing, 1);
    assert_eq!(recovered.records_removed, 1);
    let open = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_object_gc_claim(service.mount, &open.0).unwrap(),
        ObjectGcClaim::Open
    );
    assert!(metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap()
        .is_empty());
}

#[test]
fn malformed_gc_row_is_quarantined_without_starving_a_valid_candidate() {
    let (service, objects) = service_with_objects();
    let mut malformed_key = gc_queue_prefix(service.mount);
    malformed_key.extend_from_slice(&0_u64.to_be_bytes());
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: b"malformed-gc-row".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::Gc,
            primary_key: malformed_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Gc,
                key: malformed_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Gc,
                key: malformed_key,
                op: MutationOp::Put,
                value: Some(Value(b"not-an-object-gc-record".to_vec())),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let object = ObjectKey::new("blocks/1/901/1/0/0").unwrap();
    objects.put(&object, b"stale".to_vec()).unwrap();
    enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: InodeId::new(901).unwrap(),
            generation: 1,
            object_key: object.as_str().to_owned(),
            size: 5,
            digest_uri: "sha256:stale".to_owned(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );

    let outcome = service.cleanup_pending_objects(10).unwrap();
    assert_eq!(outcome.scanned, 2);
    assert_eq!(outcome.records_removed, 2);
    assert_eq!(outcome.deleted, 1);
    assert!(objects.head(&object).unwrap().is_none());
    assert_eq!(
        service
            .metadata
            .scan_keys(KeyScanRequest {
                family: RecordFamily::System,
                prefix: object_gc_quarantine_prefix(service.mount),
                start_after: None,
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn failover_durability_marker_survives_disable_and_reopen_and_blocks_object_gc() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"ha-victim".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "ha-victim", b"victim"))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();

    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "metadata-log",
            "shard-0",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    service.disable_sync_metadata_log().unwrap();
    let marker_key = failover_durability_required_key(service.mount);
    assert_eq!(
        metadata
            .get(
                RecordFamily::System,
                &marker_key,
                service.read_version().unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        FAILOVER_DURABILITY_REQUIRED_MARKER
    );
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    let outcome = reopened.cleanup_pending_objects(1).unwrap();
    assert_eq!(outcome.scanned, 1);
    assert_eq!(outcome.blocked_by_failover_durability, 1);
    assert_eq!(outcome.attempted, 0);
    assert_eq!(outcome.records_removed, 0);
    assert!(objects.head(&object).unwrap().is_some());
    assert_eq!(
        metadata
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(reopened.mount),
                start_after: None,
                version: reopened.read_version().unwrap(),
                limit: 1,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn failover_marker_committing_first_invalidates_shared_gc_claim_without_delete() {
    let metadata = SnapshotCommitBarrierStore::new(CommandKind::CleanupObjects, 0, 2);
    let memory = MemoryObjectStore::new();
    let objects = DeleteAckLostObjectStore::new(memory.clone());
    let cleaner = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
    ));
    cleaner.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"marker-first".to_vec()).unwrap();
    let published = cleaner
        .publish_artifact(artifact_request(name.clone(), "marker-first", b"victim"))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    cleaner.remove_file(InodeId::root(), &name).unwrap();
    let marker = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();

    metadata.arm(1);
    let cleanup = {
        let cleaner = Arc::clone(&cleaner);
        std::thread::spawn(move || cleaner.cleanup_pending_objects(1))
    };
    metadata.wait_until_blocked();
    marker.require_failover_durability().unwrap();
    metadata.release_blocked();

    let outcome = cleanup.join().unwrap().unwrap();
    assert_eq!(outcome.attempted, 0);
    assert_eq!(objects.delete_calls(), 0);
    assert!(memory.head(&object).unwrap().is_some());
    assert_eq!(metadata.predicate_failures(), 1);
    cleaner.refresh_allocator_state().unwrap();
    let blocked = cleaner.cleanup_pending_objects(1).unwrap();
    assert_eq!(blocked.blocked_by_failover_durability, 1);
    assert_eq!(objects.delete_calls(), 0);
    let marker_key = failover_durability_required_key(cleaner.mount);
    assert_eq!(
        metadata
            .get(
                RecordFamily::System,
                &marker_key,
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        FAILOVER_DURABILITY_REQUIRED_MARKER
    );
}

#[test]
fn shared_gc_claim_committing_first_invalidates_preplanned_marker_cas() {
    let metadata = SnapshotCommitBarrierStore::new(CommandKind::CleanupObjects, 0, 2);
    let objects = MemoryObjectStore::new();
    let cleaner = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    cleaner.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"claim-first".to_vec()).unwrap();
    let published = cleaner
        .publish_artifact(artifact_request(name.clone(), "claim-first", b"victim"))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    cleaner.remove_file(InodeId::root(), &name).unwrap();
    let marker = Arc::new(
        NoKvFs::open_existing(
            MountId::new(1).unwrap(),
            metadata.clone(),
            objects.clone(),
            0,
        )
        .unwrap(),
    );

    metadata.arm(1);
    let marker_commit = {
        let marker = Arc::clone(&marker);
        std::thread::spawn(move || marker.require_failover_durability())
    };
    metadata.wait_until_blocked();
    let outcome = cleaner.cleanup_pending_objects(1).unwrap();
    assert_eq!(outcome.deleted, 1);
    assert!(objects.head(&object).unwrap().is_none());
    metadata.release_blocked();
    marker_commit.join().unwrap().unwrap();

    assert_eq!(
        metadata.predicate_failures(),
        1,
        "the marker command planned against the old Open claim must not cross the Deleting transition"
    );
    assert_eq!(
        metadata
            .get(
                RecordFamily::System,
                &failover_durability_required_key(cleaner.mount),
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .unwrap()
            .0,
        FAILOVER_DURABILITY_REQUIRED_MARKER
    );
}

#[test]
fn startup_recovery_keeps_interrupted_claim_closed_under_failover_marker() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"interrupted-ha-victim".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "interrupted-ha-victim",
            b"victim",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "metadata-log",
            "shard-0",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    service.disable_sync_metadata_log().unwrap();
    let gc_row = metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .pop()
        .unwrap();
    let claim_key = leave_object_gc_deleting(&service, &gc_row);
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    assert!(matches!(
        reopened.recover_object_gc_claim(),
        Err(MetadError::ObjectGcRecoveryRequiresIntervention { .. })
    ));
    let claim = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_object_gc_claim(reopened.mount, &claim.0).unwrap(),
        ObjectGcClaim::Deleting { .. }
    ));
    assert!(objects.head(&object).unwrap().is_some());
    assert_eq!(
        metadata
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(reopened.mount),
                start_after: None,
                version: reopened.read_version().unwrap(),
                limit: 1,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn enabling_failover_durability_fences_a_crash_left_deleting_claim_before_recovery() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"pre-marker-interrupted".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "pre-marker-interrupted",
            b"victim",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();
    let gc_row = metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .pop()
        .unwrap();
    let claim_key = leave_object_gc_deleting(&service, &gc_row);
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    reopened
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "metadata-log",
            "shard-0",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    reopened.disable_sync_metadata_log().unwrap();
    let deleting = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_object_gc_claim(reopened.mount, &deleting.0).unwrap(),
        ObjectGcClaim::Deleting { .. }
    ));

    assert!(matches!(
        reopened.recover_object_gc_claim(),
        Err(MetadError::ObjectGcRecoveryRequiresIntervention { .. })
    ));
    let still_deleting = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_object_gc_claim(reopened.mount, &still_deleting.0).unwrap(),
        ObjectGcClaim::Deleting { .. }
    ));
    assert!(objects.head(&object).unwrap().is_some());
    assert_eq!(
        metadata
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(reopened.mount),
                start_after: None,
                version: reopened.read_version().unwrap(),
                limit: 1,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn failover_recovery_does_not_reopen_after_external_delete_completed() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"deleted-before-crash".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "deleted-before-crash",
            b"victim",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();
    let gc_row = metadata
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .pop()
        .unwrap();
    let claim_key = leave_object_gc_deleting(&service, &gc_row);
    assert!(objects.delete(&object).unwrap());
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "metadata-log",
            "shard-0",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    service.disable_sync_metadata_log().unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    assert!(matches!(
        reopened.recover_object_gc_claim(),
        Err(MetadError::ObjectGcRecoveryRequiresIntervention { .. })
    ));
    assert!(objects.head(&object).unwrap().is_none());
    let claim = metadata
        .get(
            RecordFamily::System,
            &claim_key,
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_object_gc_claim(reopened.mount, &claim.0).unwrap(),
        ObjectGcClaim::Deleting { .. }
    ));
    assert_eq!(
        metadata
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(reopened.mount),
                start_after: None,
                version: reopened.read_version().unwrap(),
                limit: 1,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn reopen_missing_claim_is_initialized_before_enabling_failover_durability() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    delete_object_gc_claim(&service);
    drop(service);

    let reopened =
        NoKvFs::open_existing(MountId::new(1).unwrap(), metadata.clone(), objects, 0).unwrap();
    assert!(metadata
        .get(
            RecordFamily::System,
            &object_gc_claim_key(reopened.mount),
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    reopened
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "metadata-log",
            "shard-0",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    let claim = metadata
        .get(
            RecordFamily::System,
            &object_gc_claim_key(reopened.mount),
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_object_gc_claim(reopened.mount, &claim.0).unwrap(),
        ObjectGcClaim::Open
    );
    assert!(metadata
        .get(
            RecordFamily::System,
            &failover_durability_required_key(reopened.mount),
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());
}

#[test]
fn reopen_missing_claim_is_initialized_by_cleanup_recovery() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"legacy-gc-victim".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "legacy-gc-victim",
            b"victim",
        ))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();
    delete_object_gc_claim(&service);
    drop(service);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    let outcome = reopened.cleanup_pending_objects(1).unwrap();
    assert_eq!(outcome.deleted, 1);
    assert!(objects.head(&object).unwrap().is_none());
    let claim = metadata
        .get(
            RecordFamily::System,
            &object_gc_claim_key(reopened.mount),
            reopened.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_object_gc_claim(reopened.mount, &claim.0).unwrap(),
        ObjectGcClaim::Open
    );
}

#[test]
fn read_lease_grace_blocks_recent_object_gc_records() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let new_body = replaced.entry.body.clone().unwrap();
    let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);

    let blocked = service
        .cleanup_pending_objects_with_grace(100, std::time::Duration::from_secs(3_600))
        .unwrap();
    assert_eq!(blocked.scanned, 1);
    assert_eq!(blocked.blocked_by_snapshots, 0);
    assert_eq!(blocked.blocked_by_read_leases, 1);
    assert_eq!(blocked.attempted, 0);
    assert_eq!(blocked.records_removed, 0);
    assert!(objects.head(&old_object).unwrap().is_some());
    assert!(objects.head(&new_object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert!(objects.head(&new_object).unwrap().is_some());
}

#[test]
fn replace_artifact_cleanup_deletes_only_old_generation() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let new_body = replaced.entry.body.clone().unwrap();
    let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);
    assert!(objects.head(&old_object).unwrap().is_some());
    assert!(objects.head(&new_object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert!(objects.head(&new_object).unwrap().is_some());
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
}

#[test]
fn snapshot_after_replace_does_not_block_old_object_cleanup() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();

    assert_eq!(
        service
            .read_artifact_path_at_snapshot("/", snapshot.snapshot_id, "/checkpoint.bin")
            .unwrap(),
        b"new-body"
    );
    assert!(objects.head(&old_object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.scanned, 1);
    assert_eq!(cleanup.blocked_by_snapshots, 0);
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
    assert_eq!(
        replaced.entry.body.unwrap().generation,
        snapshot.read_version
    );
}

#[test]
fn snapshot_preserves_old_artifact_and_blocks_object_gc_until_retired() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();

    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let new_body = replaced.entry.body.clone().unwrap();
    let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);

    assert_eq!(
        service
            .read_artifact_path_at_snapshot("/", snapshot.snapshot_id, "/checkpoint.bin")
            .unwrap(),
        b"old"
    );
    assert_eq!(
        service
            .get_attr_at_snapshot("/", snapshot.snapshot_id, std::slice::from_ref(&name))
            .unwrap(),
        Some(first.attr.clone())
    );
    assert_eq!(
        service
            .read_file_at_snapshot("/", snapshot.snapshot_id, std::slice::from_ref(&name), 0, 3,)
            .unwrap(),
        b"old"
    );
    assert_eq!(
        service
            .read_dir_plus_at_snapshot("/", snapshot.snapshot_id, &[])
            .unwrap(),
        vec![first.clone()]
    );
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
    let blocked = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(blocked.scanned, 1);
    assert_eq!(blocked.blocked_by_snapshots, 1);
    assert_eq!(blocked.attempted, 0);
    assert!(objects.head(&old_object).unwrap().is_some());
    assert!(objects.head(&new_object).unwrap().is_some());

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    assert!(!service.retire_snapshot(snapshot.snapshot_id).unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert!(objects.head(&new_object).unwrap().is_some());
}

#[test]
fn snapshot_path_reads_are_rooted_at_snapshot_subtree_and_support_ranges() {
    let service = service();
    let scope = service
        .create_dir_path("/scope", 0o755, 1000, 1000)
        .unwrap();
    let nested = service
        .create_dir_path("/scope/nested", 0o755, 1000, 1000)
        .unwrap();
    let outside = service
        .create_dir_path("/outside", 0o755, 1000, 1000)
        .unwrap();
    let name = DentryName::new(b"model.bin".to_vec()).unwrap();
    let inside_old = service
        .publish_artifact(PublishArtifact {
            parent: nested.attr.inode,
            name: name.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:inside-old".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "inside-old".to_owned(),
            bytes: b"inside-old".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: outside.attr.inode,
            name: name.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:outside".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "outside".to_owned(),
            bytes: b"outside".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/scope").unwrap();
    service
        .replace_artifact(PublishArtifact {
            parent: nested.attr.inode,
            name: name.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:inside-new".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "inside-new".to_owned(),
            bytes: b"inside-new".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let root = service
        .stat_path_at_snapshot("/scope", snapshot.snapshot_id, "/")
        .unwrap()
        .unwrap();
    assert_eq!(root.attr.inode, scope.attr.inode);
    assert_eq!(
        service
            .read_dir_plus_path_at_snapshot("/scope", snapshot.snapshot_id, "/")
            .unwrap(),
        vec![nested.clone()]
    );
    let file = service
        .stat_path_at_snapshot("/scope", snapshot.snapshot_id, "/nested/model.bin")
        .unwrap()
        .unwrap();
    assert_eq!(file.attr.generation, inside_old.attr.generation);
    assert_eq!(file.body, inside_old.body);
    assert_eq!(
        service
            .read_file_path_at_snapshot("/scope", snapshot.snapshot_id, "/nested/model.bin", 7, 3,)
            .unwrap(),
        b"old"
    );
    assert!(matches!(
        service.read_file_path_at_snapshot(
            "/scope",
            snapshot.snapshot_id,
            "/outside/model.bin",
            0,
            7,
        ),
        Err(MetadError::NotFound)
    ));
}

#[test]
fn snapshot_path_list_pages_include_entries_deleted_after_snapshot() {
    let service = service();
    for name in [
        b"a.txt".as_slice(),
        b"b.txt".as_slice(),
        b"c.txt".as_slice(),
    ] {
        service
            .create_file(
                InodeId::root(),
                DentryName::new(name.to_vec()).unwrap(),
                0o644,
                1000,
                1000,
            )
            .unwrap();
    }
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    service
        .rename(
            InodeId::root(),
            &DentryName::new(b"b.txt".to_vec()).unwrap(),
            InodeId::root(),
            DentryName::new(b"z.txt".to_vec()).unwrap(),
        )
        .unwrap();
    service
        .remove_file(
            InodeId::root(),
            &DentryName::new(b"c.txt".to_vec()).unwrap(),
        )
        .unwrap();

    let first = service
        .read_dir_plus_path_at_snapshot_page("/", snapshot.snapshot_id, "/", None, 2)
        .unwrap();
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"a.txt".as_slice(), b"b.txt".as_slice()]
    );
    let cursor = first.next_cursor.unwrap();
    assert_eq!(cursor.as_bytes(), b"b.txt");

    let second = service
        .read_dir_plus_path_at_snapshot_page("/", snapshot.snapshot_id, "/", Some(&cursor), 2)
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].dentry.name.as_bytes(), b"c.txt");
    assert!(second.next_cursor.is_none());
}

#[test]
fn history_cleanup_keeps_snapshot_reads_until_snapshot_retired() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();

    let retained = service.cleanup_history(100).unwrap();
    assert!(retained.retained_by_snapshots > 0);
    assert_eq!(
        service
            .read_artifact_path_at_snapshot("/", snapshot.snapshot_id, "/checkpoint.bin")
            .unwrap(),
        b"old"
    );

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    let pruned = service.cleanup_history(100).unwrap();
    assert!(pruned.removed > 0);
    assert_eq!(
        service
            .metadata
            .get(
                RecordFamily::Dentry,
                &dentry_key(service.mount, InodeId::root(), &name),
                Version::new(snapshot.read_version).unwrap(),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        None
    );
}

#[test]
fn remove_empty_dir_rejects_non_empty_directory() {
    let service = service();
    let dir = DentryName::new(b"runs".to_vec()).unwrap();
    let child = DentryName::new(b"1".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir(created.attr.inode, child, 0o755, 1000, 1000)
        .unwrap();

    let err = service.remove_empty_dir(InodeId::root(), &dir).unwrap_err();
    assert!(matches!(err, MetadError::DirectoryNotEmpty));
    assert!(service
        .lookup_plus(InodeId::root(), &dir)
        .unwrap()
        .is_some());
}

#[test]
fn remove_empty_dir_deletes_empty_directory() {
    let service = service();
    let dir = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
        .unwrap();

    let removed = service.remove_empty_dir(InodeId::root(), &dir).unwrap();
    assert_eq!(removed, created);
    assert!(service
        .lookup_plus(InodeId::root(), &dir)
        .unwrap()
        .is_none());
    assert!(service.get_attr(created.attr.inode).unwrap().is_none());
}

#[test]
fn remove_empty_dir_allows_directory_after_last_child_removed() {
    let service = service();
    let dir = DentryName::new(b"runs".to_vec()).unwrap();
    let child = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: created.attr.inode,
            name: child.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "runs/checkpoint.bin".to_owned(),
            bytes: b"payload".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    service.remove_file(created.attr.inode, &child).unwrap();
    let removed = service.remove_empty_dir(InodeId::root(), &dir).unwrap();

    assert_eq!(removed, created);
    assert!(service
        .lookup_plus(InodeId::root(), &dir)
        .unwrap()
        .is_none());
}

#[test]
fn rename_moves_dentry_without_changing_inode() {
    let service = service();
    let old_name = DentryName::new(b"old".to_vec()).unwrap();
    let new_name = DentryName::new(b"new".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
        .unwrap();

    let renamed = service
        .rename(
            InodeId::root(),
            &old_name,
            InodeId::root(),
            new_name.clone(),
        )
        .unwrap();
    assert_eq!(renamed.attr.inode, created.attr.inode);
    assert!(service
        .lookup_plus(InodeId::root(), &old_name)
        .unwrap()
        .is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &new_name).unwrap(),
        Some(renamed)
    );
}

#[test]
fn rename_replace_returns_replaced_file_body() {
    let service = service();
    let source_name = DentryName::new(b"stage".to_vec()).unwrap();
    let final_name = DentryName::new(b"final".to_vec()).unwrap();
    let source = service
        .publish_artifact(artifact_request(source_name.clone(), "stage", b"new"))
        .unwrap();
    let old = service
        .publish_artifact(artifact_request(final_name.clone(), "final-old", b"old"))
        .unwrap();

    let result = service
        .rename_replace(
            InodeId::root(),
            &source_name,
            InodeId::root(),
            final_name.clone(),
        )
        .unwrap();
    assert_eq!(result.entry.attr.inode, source.attr.inode);
    assert_eq!(result.replaced, Some(old.clone()));
    assert!(service
        .lookup_plus(InodeId::root(), &source_name)
        .unwrap()
        .is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &final_name).unwrap(),
        Some(result.entry)
    );
    assert!(service.get_attr(old.attr.inode).unwrap().is_none());
}

#[test]
fn watch_replay_returns_typed_events_after_cursor() {
    let service = service();
    let cursor = service.watch_subtree(InodeId::root()).unwrap();
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();

    let events = service.replay_watch(InodeId::root(), cursor, 100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind, WatchEventKind::Create);
    assert_eq!(events[0].event.parent, Some(InodeId::root()));
    assert_eq!(events[0].event.name, Some(name.clone()));
    assert_eq!(events[0].event.inode, created.attr.inode);

    let next_name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(
            next_name.clone(),
            "checkpoint.bin",
            b"body",
        ))
        .unwrap();
    let resumed = service
        .replay_watch(InodeId::root(), events[0].cursor, 100)
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].event.kind, WatchEventKind::PublishArtifact);
    assert_eq!(resumed[0].event.name, Some(next_name));
}

#[test]
fn watch_replay_resumes_from_cursor_without_scanning_old_events() {
    let service = service();
    let cursor = service.watch_subtree(InodeId::root()).unwrap();
    for name in ["a", "b", "c"] {
        service
            .create_dir(
                InodeId::root(),
                DentryName::new(name.as_bytes().to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
    }

    let before_first = service.metadata_store_stats();
    let first = service.replay_watch(InodeId::root(), cursor, 1).unwrap();
    let after_first = service.metadata_store_stats();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].event.name.as_ref().map(DentryName::as_bytes),
        Some(b"a".as_slice())
    );
    assert_eq!(
        after_first.scan_key_visited_total - before_first.scan_key_visited_total,
        1
    );
    assert_eq!(
        after_first.scan_key_returned_total - before_first.scan_key_returned_total,
        1
    );

    let before_second = service.metadata_store_stats();
    let second = service
        .replay_watch(InodeId::root(), first[0].cursor, 1)
        .unwrap();
    let after_second = service.metadata_store_stats();
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].event.name.as_ref().map(DentryName::as_bytes),
        Some(b"b".as_slice())
    );
    assert_eq!(
        after_second.scan_key_visited_total - before_second.scan_key_visited_total,
        1
    );
    assert_eq!(
        after_second.scan_key_returned_total - before_second.scan_key_returned_total,
        1
    );
}

#[test]
fn rename_replay_notifies_old_and_new_parent_scopes() {
    let service = service();
    let old_parent_name = DentryName::new(b"old-parent".to_vec()).unwrap();
    let new_parent_name = DentryName::new(b"new-parent".to_vec()).unwrap();
    let old_parent = service
        .create_dir(InodeId::root(), old_parent_name, 0o755, 1000, 1000)
        .unwrap();
    let new_parent = service
        .create_dir(InodeId::root(), new_parent_name, 0o755, 1000, 1000)
        .unwrap();
    let file_name = DentryName::new(b"artifact".to_vec()).unwrap();
    let source = service
        .create_file(old_parent.attr.inode, file_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    let old_cursor = service.watch_subtree(old_parent.attr.inode).unwrap();
    let new_cursor = service.watch_subtree(new_parent.attr.inode).unwrap();

    service
        .rename(
            old_parent.attr.inode,
            &file_name,
            new_parent.attr.inode,
            file_name.clone(),
        )
        .unwrap();

    let old_events = service
        .replay_watch(old_parent.attr.inode, old_cursor, 100)
        .unwrap();
    assert_eq!(old_events.len(), 1);
    assert_eq!(old_events[0].event.kind, WatchEventKind::Remove);
    assert_eq!(old_events[0].event.inode, source.attr.inode);

    let new_events = service
        .replay_watch(new_parent.attr.inode, new_cursor, 100)
        .unwrap();
    assert_eq!(new_events.len(), 1);
    assert_eq!(new_events[0].event.kind, WatchEventKind::Rename);
    assert_eq!(new_events[0].event.name, Some(file_name));
    assert_eq!(new_events[0].event.inode, source.attr.inode);
}

#[test]
fn watch_replay_survives_service_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let cursor = service.watch_subtree(InodeId::root()).unwrap();
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    let events = reopened.replay_watch(InodeId::root(), cursor, 100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind, WatchEventKind::Create);
    assert_eq!(events[0].event.name, Some(name));
    assert_eq!(events[0].event.inode, created.attr.inode);
}

#[test]
fn open_existing_recovers_inode_and_version_allocators() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let first = service
        .create_dir(
            InodeId::root(),
            DentryName::new(b"first".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    let second = reopened
        .create_dir(
            InodeId::root(),
            DentryName::new(b"second".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    assert!(second.attr.inode > first.attr.inode);
    assert!(second.attr.generation > first.attr.generation);
}

#[test]
fn refresh_allocator_state_advances_live_read_version_after_external_commit() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let original = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    original.bootstrap_root(0o755, 1000, 1000).unwrap();

    let external = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    let external_file = external
        .create_file_path("/external.bin", 0o644, 1000, 1000)
        .unwrap();
    assert!(original.stat_path("/external.bin").unwrap().is_none());

    original.refresh_allocator_state().unwrap();
    let visible = original
        .stat_path("/external.bin")
        .unwrap()
        .expect("external commit should be visible after refresh");
    assert_eq!(visible.attr, external_file.attr);
    let local_file = original
        .create_file_path("/after-refresh.bin", 0o644, 1000, 1000)
        .unwrap();
    assert!(local_file.attr.inode > external_file.attr.inode);
    assert!(local_file.attr.generation > external_file.attr.generation);
}

#[test]
fn open_existing_recovers_after_dentry_only_rename() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let old_name = DentryName::new(b"old".to_vec()).unwrap();
    let new_name = DentryName::new(b"new".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
        .unwrap();
    let renamed = service
        .rename(
            InodeId::root(),
            &old_name,
            InodeId::root(),
            new_name.clone(),
        )
        .unwrap();
    assert_eq!(renamed.attr.inode, created.attr.inode);
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    assert!(reopened
        .lookup_plus(InodeId::root(), &old_name)
        .unwrap()
        .is_none());
    assert_eq!(
        reopened.lookup_plus(InodeId::root(), &new_name).unwrap(),
        Some(renamed)
    );
    assert_eq!(reopened.read_dir_plus(InodeId::root()).unwrap().len(), 1);
}

#[test]
fn open_existing_does_not_reuse_removed_inode() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let first_name = DentryName::new(b"first".to_vec()).unwrap();
    let second_name = DentryName::new(b"second".to_vec()).unwrap();
    let first = service
        .create_file(InodeId::root(), first_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    service.remove_file(InodeId::root(), &first_name).unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    let second = reopened
        .create_file(InodeId::root(), second_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert!(second.attr.inode > first.attr.inode);
    assert!(second.attr.generation > first.attr.generation);
    assert!(reopened
        .lookup_plus(InodeId::root(), &first_name)
        .unwrap()
        .is_none());
    assert_eq!(
        reopened.lookup_plus(InodeId::root(), &second_name).unwrap(),
        Some(second)
    );
}

#[test]
fn pending_object_gc_survives_service_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();
    let body = published.body.clone().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();
    drop(service);

    let reopened =
        NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects.clone(), 0).unwrap();
    let cleanup = reopened.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_none());
}

#[test]
fn snapshot_pin_survives_service_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    assert_eq!(
        reopened.snapshot_pin(snapshot.snapshot_id).unwrap(),
        Some(snapshot)
    );
    assert_eq!(reopened.metadata_store_stats().active_snapshot_pin_total, 1);
}

#[test]
fn failed_publish_returns_staged_objects_for_cleanup_and_does_not_reuse_identity() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "first", b"first"))
        .unwrap();
    let err = service
        .publish_artifact(artifact_request(name.clone(), "duplicate", b"duplicate"))
        .unwrap_err();
    let staged = match err {
        MetadError::PublishArtifactFailed { source, staged } => {
            assert!(matches!(
                *source,
                MetadError::Metadata(MetadataError::PredicateFailed)
            ));
            staged
        }
        err => panic!("unexpected publish error: {err:?}"),
    };
    assert_eq!(staged.len(), 1);
    for object in staged.objects() {
        assert!(objects.head(&object.key).unwrap().is_some());
    }
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(first.clone())
    );

    let cleanup = service.cleanup_staged_objects(&staged).unwrap();
    assert_eq!(cleanup.attempted, staged.len());
    assert_eq!(cleanup.deleted, staged.len());
    assert_eq!(cleanup.missing, 0);
    for object in staged.objects() {
        assert!(objects.head(&object.key).unwrap().is_none());
    }
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    let next_name = DentryName::new(b"next.bin".to_vec()).unwrap();
    let next = reopened
        .publish_artifact(artifact_request(next_name, "next", b"next"))
        .unwrap();

    assert!(next.attr.inode.get() > first.attr.inode.get() + 1);
    assert!(next.attr.generation > first.attr.generation + 1);
}

fn dname(raw: &[u8]) -> DentryName {
    DentryName::new(raw.to_vec()).unwrap()
}

fn block_count_for(objects: &MemoryObjectStore, inode: InodeId, generation: u64) -> usize {
    // Count the published blocks the base file owns under its (inode, generation).
    let mut count = 0;
    let mut block = 0;
    while objects
        .head(&block_key(inode, generation, 0, block))
        .unwrap()
        .is_some()
    {
        count += 1;
        block += 1;
    }
    count
}

#[test]
fn clone_subtree_shares_base_blocks_diverges_on_write_and_keeps_gc_safe() {
    let (service, objects) = service_with_objects();
    // 1. Base namespace: /base with files a ("AAA..") and b ("BBB..").
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let a = publish_path_artifact(&service, "/base/a", "base/a", &vec![b'A'; 4096]);
    let b = publish_path_artifact(&service, "/base/b", "base/b", &vec![b'B'; 4096]);
    let a_gen = a.body.as_ref().unwrap().generation;
    let b_gen = b.body.as_ref().unwrap().generation;
    let a_block = block_key(a.attr.inode, a_gen, 0, 0);
    let b_block = block_key(b.attr.inode, b_gen, 0, 0);
    assert!(objects.head(&a_block).unwrap().is_some());
    assert!(objects.head(&b_block).unwrap().is_some());
    let objects_after_base = objects.object_count();

    // 2. Writable O(1)-ish fork of /base.
    let fork = service.clone_subtree_path("/base").unwrap();
    assert_ne!(fork.root, base.attr.inode);

    // 3. Sharing: the fork sees the base content, with NO duplicate blocks.
    let fork_a = service
        .lookup_plus(fork.root, &dname(b"a"))
        .unwrap()
        .unwrap();
    let fork_b = service
        .lookup_plus(fork.root, &dname(b"b"))
        .unwrap()
        .unwrap();
    assert_ne!(
        fork_a.attr.inode, a.attr.inode,
        "fork must use a fresh inode"
    );
    // Shared files keep the source's content generation (the CoW sharing signal).
    assert_eq!(fork_a.attr.generation, a_gen);
    assert_eq!(fork_b.attr.generation, b_gen);
    assert_eq!(fork_b.body.as_ref().unwrap().generation, b_gen);
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"a")).unwrap(),
        vec![b'A'; 4096]
    );
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"b")).unwrap(),
        vec![b'B'; 4096]
    );
    // Zero-copy: clone added metadata only, not object blocks.
    assert_eq!(
        objects.object_count(),
        objects_after_base,
        "clone must share base blocks, not copy them"
    );
    // The fork's a/b manifests reference the SAME object keys as the base.
    assert_eq!(
        service
            .read_file_plan(fork_a.attr.inode, fork_a.attr.generation, 0, 4096)
            .unwrap()
            .blocks[0]
            .object_key,
        a_block.as_str()
    );

    // 4. Divergence: rewrite a in the fork and add a new file c.
    service
        .replace_artifact(PublishArtifact {
            parent: fork.root,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:zzz".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fork/a".to_owned(),
            bytes: vec![b'Z'; 4096],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: fork.root,
            name: dname(b"c"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:ccc".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fork/c".to_owned(),
            bytes: vec![b'C'; 4096],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    // 4a. Fork now sees a="ZZZ..", b="BBB..", c present.
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"a")).unwrap(),
        vec![b'Z'; 4096]
    );
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"b")).unwrap(),
        vec![b'B'; 4096]
    );
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"c")).unwrap(),
        vec![b'C'; 4096]
    );
    // 4b. Base is unchanged: a="AAA..", no c.
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"a"))
            .unwrap(),
        vec![b'A'; 4096]
    );
    assert!(service
        .lookup_plus(base.attr.inode, &dname(b"c"))
        .unwrap()
        .is_none());

    // 6. Diff reports exactly { modified: a, added: c }; b (shared) is not reported.
    let mut diff = service.diff_subtrees(base.attr.inode, fork.root).unwrap();
    diff.sort_by(|left, right| left.path.cmp(&right.path));
    let summary: Vec<(&str, SubtreeDeltaKind)> =
        diff.iter().map(|d| (d.path.as_str(), d.kind)).collect();
    assert_eq!(
        summary,
        vec![
            ("/a", SubtreeDeltaKind::Modified),
            ("/c", SubtreeDeltaKind::Added),
        ]
    );
    // The enriched diff carries the changed file's content digest.
    assert!(diff
        .iter()
        .find(|d| d.path == "/a")
        .unwrap()
        .digest
        .is_some());

    // 5. GC safety: reclaim must NOT touch base blocks the fork's divergent write
    // abandoned but the base still references; they are owned by the base inode and
    // protected by the fork's retained snapshot pin.
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(reclaim.deleted, 0, "no base block may be reclaimed yet");
    assert!(objects.head(&a_block).unwrap().is_some());
    assert!(objects.head(&b_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"a"))
            .unwrap(),
        vec![b'A'; 4096]
    );

    // Drop the fork: remove its files and retire its snapshot pin. The fork-only
    // blocks (the divergent a' and the new c) then become reclaimable, while the
    // base's blocks remain because the base still references them.
    let fork_a_diverged = service
        .lookup_plus(fork.root, &dname(b"a"))
        .unwrap()
        .unwrap();
    let fork_c = service
        .lookup_plus(fork.root, &dname(b"c"))
        .unwrap()
        .unwrap();
    let fork_a_block = block_key(
        fork_a_diverged.attr.inode,
        fork_a_diverged.body.as_ref().unwrap().generation,
        0,
        0,
    );
    let fork_c_block = block_key(
        fork_c.attr.inode,
        fork_c.body.as_ref().unwrap().generation,
        0,
        0,
    );
    service.remove_file(fork.root, &dname(b"a")).unwrap();
    service.remove_file(fork.root, &dname(b"b")).unwrap();
    service.remove_file(fork.root, &dname(b"c")).unwrap();
    assert!(service.retire_snapshot(fork.snapshot_id).unwrap());
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert!(reclaim.deleted >= 2, "fork-only blocks must be reclaimable");
    assert!(objects.head(&fork_a_block).unwrap().is_none());
    assert!(objects.head(&fork_c_block).unwrap().is_none());
    // Base remains fully intact and readable.
    assert!(objects.head(&a_block).unwrap().is_some());
    assert!(objects.head(&b_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"a"))
            .unwrap(),
        vec![b'A'; 4096]
    );
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"b"))
            .unwrap(),
        vec![b'B'; 4096]
    );
    assert_eq!(block_count_for(&objects, a.attr.inode, a_gen), 1);
}

#[test]
fn clone_subtree_copies_nested_dirs_and_diff_reports_removed() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/base/sub", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/base/sub/deep", "base/deep", b"deep-bytes");
    publish_path_artifact(&service, "/base/top", "base/top", b"top-bytes");

    let fork = service.clone_subtree_path("/base").unwrap();
    // Nested structure is reproduced under fresh inodes.
    let sub = service
        .lookup_plus(fork.root, &dname(b"sub"))
        .unwrap()
        .unwrap();
    assert_eq!(sub.attr.file_type, FileType::Directory);
    assert_eq!(
        service
            .read_artifact(sub.attr.inode, &dname(b"deep"))
            .unwrap(),
        b"deep-bytes"
    );

    // Identical subtree => no deltas.
    let base = service.resolve_directory_path("/base").unwrap();
    assert!(service.diff_subtrees(base, fork.root).unwrap().is_empty());

    // Remove a nested file in the fork => Removed delta at the nested path,
    // direction base -> fork.
    service
        .remove_file(sub.attr.inode, &dname(b"deep"))
        .unwrap();
    let removed = service.diff_subtrees(base, fork.root).unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].path, "/sub/deep");
    assert_eq!(removed[0].kind, SubtreeDeltaKind::Removed);
    assert_eq!(removed[0].size_delta, -(b"deep-bytes".len() as i64));
    // Reversed direction reports it as Added, with the net size flipped.
    let added = service.diff_subtrees(fork.root, base).unwrap();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].path, "/sub/deep");
    assert_eq!(added[0].kind, SubtreeDeltaKind::Added);
    assert_eq!(added[0].size_delta, b"deep-bytes".len() as i64);
}

#[test]
fn clone_subtree_path_rejects_non_directory() {
    let service = service();
    publish_path_artifact(&service, "/file.bin", "file", b"bytes");
    assert!(matches!(
        service.clone_subtree_path("/file.bin"),
        Err(MetadError::NotDirectory)
    ));
}

#[test]
fn clone_link_rejects_an_expired_reaped_pin_before_exposing_the_fork() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::CreateDir, 1, 2)
        .matching_request_prefix(b"clone-subtree-link");
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(PublishArtifact {
            parent: base.attr.inode,
            name: dname(b"data.bin"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:clone-link-race".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "clone-link-race".to_owned(),
            bytes: b"payload".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);

    let clone_service = Arc::clone(&service);
    let clone = std::thread::spawn(move || clone_service.clone_subtree_path_into("/base", "/fork"));
    store.wait_until_blocked();
    assert!(service.object_gc_gate.try_lock().is_err());

    let pins = store
        .scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();
    assert_eq!(pins.len(), 1);
    let pin = decode_snapshot_pin(&pins[0].value.0).unwrap();

    service
        .remove_file(base.attr.inode, &dname(b"data.bin"))
        .unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );

    let cleanup_started = Arc::new(Barrier::new(2));
    let cleanup_service = Arc::clone(&service);
    let cleanup_thread_started = Arc::clone(&cleanup_started);
    let cleanup = std::thread::spawn(move || {
        cleanup_thread_started.wait();
        cleanup_service.cleanup_pending_objects(100)
    });
    cleanup_started.wait();
    store.release_blocked();

    assert!(matches!(
        clone.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    let cleanup = cleanup.join().unwrap().unwrap();
    assert_eq!(cleanup.deleted, 1, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&object).unwrap().is_none());
    assert!(service.lookup_path("/fork").unwrap().is_none());
}

#[test]
fn fork_binding_survives_pin_reaping_and_a_hardlink_escaping_the_fork_root() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/data.bin",
        "fork-retention/source",
        b"shared bytes",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);

    let fork = service.clone_subtree_path_into("/base", "/fork").unwrap();
    let fork_file = service
        .lookup_plus(fork.root, &dname(b"data.bin"))
        .unwrap()
        .unwrap();
    service
        .link(fork_file.attr.inode, InodeId::root(), dname(b"escaped.bin"))
        .unwrap();
    service.remove_file(fork.root, &dname(b"data.bin")).unwrap();
    service.remove_empty_dir_path("/fork").unwrap();

    // The source can stop naming its object and the construction pin can expire;
    // neither event retires the durable binding. The escaped hardlink remains a
    // current borrowed reference even though the fork root itself is gone.
    service.remove_file_path("/base/data.bin").unwrap();
    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    assert!(service.snapshot_pin(fork.snapshot_id).unwrap().is_none());
    assert_eq!(
        service
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::ForkBinding,
                prefix: crate::layout::fork_binding_prefix(service.mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .len(),
        1
    );
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 0, "cleanup outcome: {cleanup:?}");
    assert!(cleanup.blocked_by_snapshots >= 1);
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(InodeId::root(), &dname(b"escaped.bin"))
            .unwrap(),
        b"shared bytes"
    );

    let error = service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap_err();
    assert!(
        matches!(
            error,
            MetadError::ForkRetentionActive {
                snapshot_id,
                fork_root,
                borrower,
            } if snapshot_id == fork.snapshot_id
                && fork_root == fork.root
                && borrower == fork_file.attr.inode
        ),
        "unexpected retirement error: {error:?}"
    );
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(InodeId::root(), &dname(b"escaped.bin"))
            .unwrap(),
        b"shared bytes"
    );

    // Retirement becomes valid after every borrowed fork reference, including
    // links outside the original fork root, has been removed.
    service
        .remove_file(InodeId::root(), &dname(b"escaped.bin"))
        .unwrap();
    assert!(service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&source_block).unwrap().is_none());
}

#[test]
fn detached_fork_binding_is_a_legal_link_and_rename_root() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/base/data.bin",
        "detached-namespace/source",
        b"shared bytes",
    );
    let fork = service.clone_subtree_path("/base").unwrap();
    assert!(
        !service.materialization_orphan_slow_path_enabled(),
        "a durable detached ForkBinding must restore the healthy fast path"
    );
    let reachability_scans = service.namespace_reachability_scan_count();
    let fork_file = service
        .lookup_plus(fork.root, &dname(b"data.bin"))
        .unwrap()
        .unwrap();

    let renamed = service
        .rename(
            fork.root,
            &dname(b"data.bin"),
            fork.root,
            dname(b"renamed.bin"),
        )
        .unwrap();
    assert_eq!(renamed.attr.inode, fork_file.attr.inode);
    let escaped = service
        .link(fork_file.attr.inode, InodeId::root(), dname(b"escaped.bin"))
        .unwrap();
    assert_eq!(escaped.attr.inode, fork_file.attr.inode);
    assert_eq!(
        service
            .read_artifact(InodeId::root(), &dname(b"escaped.bin"))
            .unwrap(),
        b"shared bytes"
    );
    assert_eq!(
        service.namespace_reachability_scan_count(),
        reachability_scans,
        "healthy detached-root rename/link must not scan namespace reachability"
    );
}

#[test]
fn reopen_recovers_an_unbound_materialization_into_the_slow_path() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/base/data.bin",
        "reopen-orphan/source",
        b"borrowed",
    );
    let pin = service.snapshot_subtree(base.attr.inode).unwrap();
    let orphan_root = {
        let _object_gc_gate = service.object_gc_gate.lock().unwrap();
        service
            .materialize_subtree_at(base.attr.inode, Version::new(pin.read_version).unwrap())
            .unwrap()
    };
    let orphan = service
        .lookup_plus(orphan_root, &dname(b"data.bin"))
        .unwrap()
        .unwrap();
    assert!(service.materialization_orphan_slow_path_enabled());

    let reopened =
        NoKvFs::open_existing(service.mount, metadata, objects, service.shard_index()).unwrap();
    assert!(
        reopened.materialization_orphan_slow_path_enabled(),
        "reopen must recover an unbound detached tree before serving"
    );
    assert_eq!(reopened.namespace_reachability_scan_count(), 1);
    assert!(matches!(
        reopened.link(orphan.attr.inode, InodeId::root(), dname(b"reopened-link")),
        Err(MetadError::NotFound)
    ));
    assert!(matches!(
        reopened.rename(
            orphan_root,
            &dname(b"data.bin"),
            InodeId::root(),
            dname(b"reopened-rename")
        ),
        Err(MetadError::NotFound)
    ));
    assert!(reopened
        .lookup_plus(InodeId::root(), &dname(b"reopened-link"))
        .unwrap()
        .is_none());
    assert!(reopened
        .lookup_plus(InodeId::root(), &dname(b"reopened-rename"))
        .unwrap()
        .is_none());
}

#[test]
fn open_existing_allows_empty_namespace_until_bootstrap_then_uses_fast_path() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();

    assert!(
        service.materialization_orphan_slow_path_enabled(),
        "an empty namespace remains fail-closed until its root is bootstrapped"
    );
    assert_eq!(service.namespace_reachability_scan_count(), 0);

    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    assert!(
        !service.materialization_orphan_slow_path_enabled(),
        "root bootstrap must prove the pristine namespace healthy"
    );
    assert_eq!(service.namespace_reachability_scan_count(), 1);

    let source = service
        .create_file(InodeId::root(), dname(b"source.bin"), 0o644, 1000, 1000)
        .unwrap();
    let scans_after_bootstrap = service.namespace_reachability_scan_count();
    service
        .link(source.attr.inode, InodeId::root(), dname(b"linked.bin"))
        .unwrap();
    assert_eq!(
        service.namespace_reachability_scan_count(),
        scans_after_bootstrap,
        "healthy link after bootstrap must not run namespace reachability"
    );
}

#[test]
fn open_existing_rejects_missing_root_with_unbound_materialization_records() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let writer = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    let orphan = InodeId::new(InodeId::ROOT_RAW + 1).unwrap();
    let version = writer.next_version().unwrap();
    let attr = directory_attr(orphan, 0o755, 1000, 1000, version.get());
    writer
        .commit_metadata(MetadataCommand {
            request_id: request_id(b"test-rootless-orphan", writer.mount, orphan, version),
            kind: CommandKind::CreateDir,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(writer.mount, orphan),
            predicates: Vec::new(),
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key: inode_key(writer.mount, orphan),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&attr))),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let reopened = NoKvFs::open_existing(writer.mount, metadata, objects, writer.shard_index());
    assert!(matches!(
        reopened,
        Err(MetadError::Codec(message))
            if message == "mount root is missing while namespace records still exist"
    ));
}

#[test]
fn fork_binding_retains_object_backed_symlink_after_source_deletion() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = service
        .create_symlink(
            base.attr.inode,
            dname(b"latest"),
            b"runs/42/checkpoint.bin".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);
    let fork = service.clone_subtree_path_into("/base", "/fork").unwrap();
    let fork_symlink = service
        .lookup_plus(fork.root, &dname(b"latest"))
        .unwrap()
        .unwrap();

    service.remove_file_path("/base/latest").unwrap();
    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        service.read_symlink(fork_symlink.attr.inode).unwrap(),
        b"runs/42/checkpoint.bin"
    );

    assert!(matches!(
        service.retire_snapshot_path("/base", fork.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            fork_root,
            borrower,
        }) if snapshot_id == fork.snapshot_id
            && fork_root == fork.root
            && borrower == fork_symlink.attr.inode
    ));
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        service.read_symlink(fork_symlink.attr.inode).unwrap(),
        b"runs/42/checkpoint.bin"
    );

    service.remove_file(fork.root, &dname(b"latest")).unwrap();
    assert!(service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap());
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 1);
    assert!(objects.head(&source_block).unwrap().is_none());
}

#[test]
fn fork_binding_can_retire_after_the_fork_rewrites_onto_self_owned_blocks() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/data.bin",
        "fork-rewrite/source",
        b"borrowed bytes",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);
    let fork = service.clone_subtree_path_into("/base", "/fork").unwrap();

    let rewritten = service
        .replace_artifact(PublishArtifact {
            parent: fork.root,
            name: dname(b"data.bin"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:fork-rewrite".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fork-rewrite/self-owned".to_owned(),
            bytes: b"self-owned bytes".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let rewritten_body = rewritten.entry.body.as_ref().unwrap();
    let rewritten_block = block_key(rewritten.entry.attr.inode, rewritten_body.generation, 0, 0);
    service
        .create_file(
            InodeId::root(),
            dname(b"unrelated-empty"),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    service.remove_file_path("/base/data.bin").unwrap();
    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );

    assert!(service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap());
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 1);
    assert!(objects.head(&source_block).unwrap().is_none());
    assert!(objects.head(&rewritten_block).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/fork/data.bin"),
        b"self-owned bytes"
    );
}

#[test]
fn unrelated_cross_shard_graft_does_not_block_safe_fork_retirement() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let fork = service.clone_subtree_path_into("/base", "/fork").unwrap();
    let foreign = InodeId::compose(1, 42).unwrap();
    service
        .create_graft(
            InodeId::root(),
            dname(b"foreign"),
            foreign,
            0o755,
            1000,
            1000,
        )
        .unwrap();

    assert!(service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap());
    assert!(!service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap());
    assert_eq!(
        service
            .lookup_plus(InodeId::root(), &dname(b"foreign"))
            .unwrap()
            .unwrap()
            .attr
            .inode,
        foreign
    );
}

#[test]
fn fork_binding_cannot_retire_when_append_still_inherits_borrowed_blocks() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/data.bin",
        "fork-append/source",
        b"borrowed base",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);
    let fork = service.clone_subtree_path_into("/base", "/fork").unwrap();
    let fork_file = service
        .lookup_plus(fork.root, &dname(b"data.bin"))
        .unwrap()
        .unwrap();

    let prepared = service
        .prepare_artifact_replace(fork.root, dname(b"data.bin"))
        .unwrap();
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "fork-append/delta",
            &[PublishArtifactRange {
                offset: source_body.size,
                bytes: b" + self-owned delta".to_vec(),
            }],
            0,
        )
        .unwrap();
    service
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent: fork.root,
                name: dname(b"data.bin"),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:fork-append".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "fork-append/delta".to_owned(),
                size: source_body.size + b" + self-owned delta".len() as u64,
                chunks: written.chunk_manifests(),
                staged: written.staged_objects().unwrap(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    service.remove_file_path("/base/data.bin").unwrap();
    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/fork/data.bin"),
        b"borrowed base + self-owned delta"
    );

    assert!(matches!(
        service.retire_snapshot_path("/base", fork.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            fork_root,
            borrower,
        }) if snapshot_id == fork.snapshot_id
            && fork_root == fork.root
            && borrower == fork_file.attr.inode
    ));
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/fork/data.bin"),
        b"borrowed base + self-owned delta"
    );
}

#[test]
fn fork_binding_retirement_fails_closed_on_corrupt_dentry_projection_body() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/data.bin",
        "fork-corruption/source",
        b"must remain retained",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);
    let fork = service.clone_subtree_path_into("/base", "/fork").unwrap();

    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );

    let dentry = dentry_key(service.mount, fork.root, &dname(b"data.bin"));
    let current = service.read_version().unwrap();
    let row = service
        .metadata
        .get_versioned(
            RecordFamily::Dentry,
            &dentry,
            current,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let mut projection = decode_dentry_projection(&row.value.0).unwrap();
    projection
        .body
        .as_mut()
        .unwrap()
        .manifest_id
        .push_str("/forged");
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"inject-corrupt-dentry-projection",
                service.mount,
                projection.attr.inode,
                version,
            ),
            kind: CommandKind::UpdateAttr,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::VersionEquals(row.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: dentry,
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(&projection))),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    service.remove_file_path("/base/data.bin").unwrap();

    let error = service
        .retire_snapshot_path("/base", fork.snapshot_id)
        .unwrap_err();
    assert!(
        matches!(&error, MetadError::Codec(message) if message.contains("body descriptor")),
        "unexpected retirement error: {error:?}"
    );
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&source_block).unwrap().is_some());
}

#[test]
fn detached_fork_binding_protects_after_source_deletion_and_pin_reaping() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/data.bin",
        "detached-retention/source",
        b"detached bytes",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);
    let fork = service.clone_subtree_path("/base").unwrap();

    service.remove_file_path("/base/data.bin").unwrap();
    service.remove_empty_dir_path("/base").unwrap();
    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 0, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(fork.root, &dname(b"data.bin"))
            .unwrap(),
        b"detached bytes"
    );

    // A deleted source path cannot accidentally release the retention root.
    // The unbound service primitive remains able to retire it after the detached
    // fork has dropped its last borrowed reference.
    assert!(matches!(
        service.retire_snapshot_path("/base", fork.snapshot_id),
        Err(MetadError::NotFound)
    ));
    service.remove_file(fork.root, &dname(b"data.bin")).unwrap();
    assert!(service.retire_snapshot(fork.snapshot_id).unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&source_block).unwrap().is_none());
}

#[test]
fn fork_binding_can_be_retired_through_the_sources_new_path_after_rename() {
    let (service, _objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/base/data.bin",
        "renamed-retention/source",
        b"renamed bytes",
    );
    let fork = service.clone_subtree_path("/base").unwrap();
    service
        .rename_path("/base", "/renamed")
        .expect("source directory rename");
    service.remove_file(fork.root, &dname(b"data.bin")).unwrap();

    let pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    assert!(matches!(
        service.retire_snapshot_path("/base", fork.snapshot_id),
        Err(MetadError::NotFound)
    ));
    assert!(service
        .retire_snapshot_path("/renamed", fork.snapshot_id)
        .unwrap());
    assert!(!service.retire_snapshot(fork.snapshot_id).unwrap());
}

fn read_artifact_at_path<M: MetadataStore, O: ObjectStore>(
    service: &NoKvFs<M, O>,
    path: &str,
) -> Vec<u8> {
    let (parent, name) = service.resolve_parent_path(path).unwrap();
    service.read_artifact(parent, &name).unwrap()
}

#[test]
fn failed_rollback_orphan_does_not_block_unrelated_fork_retirement() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RenameReplace, 1, 2)
        .matching_request_prefix(b"rollback-subtree-swap");
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();

    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:a1".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-orphan/a1".to_owned(),
            bytes: b"A1".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/ws").unwrap();
    let ws_inode = ws.attr.inode;
    let snapshot_id = snapshot.snapshot_id;

    let rollback_service = Arc::clone(&service);
    let rollback =
        std::thread::spawn(move || rollback_service.rollback_subtree(ws_inode, snapshot_id));
    store.wait_until_blocked();
    assert!(
        service.materialization_orphan_slow_path_enabled(),
        "materialization must enter slow mode before its detached tree can commit"
    );

    // Change a swap-guarded dentry after rollback materialized its detached tree.
    // The graft must fail, leaving that tree unreachable and without a binding.
    // The rollback commit is deliberately paused while the service-local
    // commit-to-log ordering gate is held. Model the conflicting write from a
    // separately recovered owner so it can reach the shared store and invalidate
    // the already-planned swap without deadlocking on that process-local gate.
    let concurrent =
        NoKvFs::open_existing(service.mount, store.clone(), objects, service.shard_index())
            .unwrap();
    concurrent
        .replace_artifact(PublishArtifact {
            parent: ws_inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:a2".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-orphan/a2".to_owned(),
            bytes: b"A2".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    store.release_blocked();
    assert!(matches!(
        rollback.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    service.refresh_allocator_state().unwrap();
    assert_eq!(
        service.read_artifact(ws_inode, &dname(b"a")).unwrap(),
        b"A2"
    );
    assert!(service
        .versioned_fork_bindings_at(service.read_version().unwrap(), ReadPurpose::UserStrong)
        .unwrap()
        .iter()
        .all(|binding| binding.binding.snapshot_id != snapshot_id));

    // A real detached clone remains a retention root through its binding. Once
    // its borrower is removed, the failed rollback's unbound orphan must not keep
    // this otherwise-unrelated binding alive forever.
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: base.attr.inode,
            name: dname(b"data"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:base".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-orphan/base".to_owned(),
            bytes: b"base".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let fork = service.clone_subtree_path("/base").unwrap();
    let fork_file = service
        .lookup_plus(fork.root, &dname(b"data"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.retire_snapshot(fork.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            fork_root,
            borrower,
        }) if snapshot_id == fork.snapshot_id
            && fork_root == fork.root
            && borrower == fork_file.attr.inode
    ));
    service.remove_file(fork.root, &dname(b"data")).unwrap();
    assert!(service.retire_snapshot(fork.snapshot_id).unwrap());
}

#[test]
fn failed_rollback_orphan_cannot_race_retirement_or_be_resurrected() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RetireSnapshot, 1, 2)
        .rejecting(CommandKind::RenameReplace);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();

    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    let original = service
        .publish_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:orphan-a1".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-resurrection/a1".to_owned(),
            bytes: b"A1".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let original_body = original.body.as_ref().unwrap();
    let original_block = block_key(original.attr.inode, original_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree_path("/ws").unwrap();
    service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:orphan-a2".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-resurrection/a2".to_owned(),
            bytes: b"A2".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    // Reject only the atomic graft. All prior materialization commits remain,
    // leaving an inode + dentry tree with neither a mount path nor ForkBinding.
    assert!(matches!(
        service.rollback_subtree(ws.attr.inode, snapshot.snapshot_id),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(
        service.materialization_orphan_slow_path_enabled(),
        "failed materialization must leave inode exposure fail-closed"
    );
    let (orphan_row, orphan) = service
        .metadata
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_mount_prefix(service.mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .into_iter()
        .find_map(|row| {
            let projection = decode_dentry_projection(&row.value.0).unwrap();
            projection
                .body
                .as_ref()
                .is_some_and(|body| body.manifest_id == "rollback-resurrection/a1")
                .then_some((row, projection))
        })
        .expect("failed rollback leaves its materialized child");
    let orphan_parent = orphan.dentry.parent;
    let orphan_inode = orphan.attr.inode;
    let orphan_name = orphan.dentry.name.clone();
    assert_ne!(orphan_parent, ws.attr.inode);
    assert!(service
        .versioned_fork_bindings_at(service.read_version().unwrap(), ReadPurpose::UserStrong)
        .unwrap()
        .iter()
        .all(|binding| binding.binding.snapshot_id != snapshot.snapshot_id));

    // Hold an unrelated retirement after its mount-wide proof but before its
    // binding CAS. A racing hardlink must wait on object_gc_gate; after the CAS
    // removes the last legal detached root, it must re-prove and reject the
    // unbound rollback orphan instead of exposing it under the mount root.
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        service.as_ref(),
        "/base/data",
        "rollback-resurrection/base",
        b"base",
    );
    let fork = service.clone_subtree_path("/base").unwrap();
    service.remove_file(fork.root, &dname(b"data")).unwrap();

    let retire_service = Arc::clone(&service);
    let retire = std::thread::spawn(move || retire_service.retire_snapshot(fork.snapshot_id));
    store.wait_until_blocked();
    assert!(service.object_gc_gate.try_lock().is_err());

    let (link_tx, link_rx) = std::sync::mpsc::channel();
    let link_service = Arc::clone(&service);
    let racing_link = std::thread::spawn(move || {
        let result =
            link_service.link(orphan_inode, InodeId::root(), dname(b"racing-resurrection"));
        link_tx.send(result).unwrap();
    });
    let early_link = link_rx.recv_timeout(Duration::from_millis(100));
    store.release_blocked();
    assert!(retire.join().unwrap().unwrap());
    let link_result = match early_link {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => link_rx.recv().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("racing hardlink thread disconnected")
        }
    };
    racing_link.join().unwrap();
    assert!(
        matches!(link_result, Err(MetadError::NotFound)),
        "unbound orphan hardlink result: {link_result:?}"
    );
    assert!(service
        .lookup_plus(InodeId::root(), &dname(b"racing-resurrection"))
        .unwrap()
        .is_none());

    // Release the failed rollback's construction snapshot as well, allowing A1
    // to be reclaimed. The orphan metadata is now a dangling borrower; neither
    // inode-addressed operation may make it reachable again.
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert!(cleanup.deleted >= 1, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&original_block).unwrap().is_none());

    let orphan_inode_key = inode_key(service.mount, orphan_inode);
    let before_inode = service
        .metadata
        .get_versioned(
            RecordFamily::Inode,
            &orphan_inode_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.link(
            orphan_inode,
            InodeId::root(),
            dname(b"hardlink-resurrection")
        ),
        Err(MetadError::NotFound)
    ));
    assert!(matches!(
        service.rename(
            orphan_parent,
            &orphan_name,
            InodeId::root(),
            dname(b"rename-resurrection")
        ),
        Err(MetadError::NotFound)
    ));

    let live = service
        .lookup_plus(ws.attr.inode, &dname(b"a"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.link(
            live.attr.inode,
            orphan_parent,
            dname(b"orphan-destination-link")
        ),
        Err(MetadError::NotFound)
    ));
    assert!(matches!(
        service.rename(
            ws.attr.inode,
            &dname(b"a"),
            orphan_parent,
            dname(b"orphan-destination-rename")
        ),
        Err(MetadError::NotFound)
    ));

    let after_dentry = service
        .metadata
        .get_versioned(
            RecordFamily::Dentry,
            &orphan_row.key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let after_inode = service
        .metadata
        .get_versioned(
            RecordFamily::Inode,
            &orphan_inode_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    assert_eq!(after_dentry.version, orphan_row.version);
    assert_eq!(after_dentry.value, orphan_row.value);
    assert_eq!(after_inode.version, before_inode.version);
    assert_eq!(after_inode.value, before_inode.value);
    assert!(service
        .lookup_plus(InodeId::root(), &dname(b"hardlink-resurrection"))
        .unwrap()
        .is_none());
    assert!(service
        .lookup_plus(InodeId::root(), &dname(b"rename-resurrection"))
        .unwrap()
        .is_none());
    assert!(service
        .lookup_plus(orphan_parent, &dname(b"orphan-destination-link"))
        .unwrap()
        .is_none());
    assert!(service
        .lookup_plus(orphan_parent, &dname(b"orphan-destination-rename"))
        .unwrap()
        .is_none());
    assert!(objects.head(&original_block).unwrap().is_none());
    assert_eq!(
        service.read_artifact(ws.attr.inode, &dname(b"a")).unwrap(),
        b"A2"
    );
}

#[test]
fn rollback_binding_survives_pin_reaping_without_an_auxiliary_clone() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    let original = publish_path_artifact(&service, "/ws/a", "rollback-hold/a1", b"A1");
    let original_body = original.body.as_ref().unwrap();
    let original_block = block_key(original.attr.inode, original_body.generation, 0, 0);
    let snap = service.snapshot_subtree_path("/ws").unwrap();
    let diverged = service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:a2".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-hold/a2".to_owned(),
            bytes: b"A2".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap()
        .entry;
    let diverged_block = block_key(
        diverged.attr.inode,
        diverged.body.as_ref().unwrap().generation,
        0,
        0,
    );

    service
        .rollback_subtree_path("/ws", snap.snapshot_id)
        .unwrap();
    let restored = service
        .lookup_plus(ws.attr.inode, &dname(b"a"))
        .unwrap()
        .unwrap();
    assert_ne!(restored.attr.inode, original.attr.inode);
    let binding = service
        .versioned_fork_bindings_at(service.read_version().unwrap(), ReadPurpose::UserStrong)
        .unwrap()
        .into_iter()
        .find(|binding| binding.binding.snapshot_id == snap.snapshot_id)
        .expect("rollback installs durable retention");
    assert_eq!(binding.binding.source_root, ws.attr.inode);
    assert_ne!(binding.binding.fork_root, ws.attr.inode);
    assert!(service
        .get_attr(binding.binding.fork_root)
        .unwrap()
        .is_none());

    let pin = service.snapshot_pin(snap.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 0, "cleanup outcome: {cleanup:?}");
    assert!(cleanup.blocked_by_snapshots >= 1);
    assert!(objects.head(&original_block).unwrap().is_some());
    assert!(objects.head(&diverged_block).unwrap().is_some());
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), b"A1");
    assert!(matches!(
        service.retire_snapshot_path("/ws", snap.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            fork_root,
            borrower,
        }) if snapshot_id == snap.snapshot_id
            && fork_root == binding.binding.fork_root
            && borrower == restored.attr.inode
    ));

    let current = service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:a3".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-hold/a3".to_owned(),
            bytes: b"A3".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap()
        .entry;
    let current_block = block_key(
        current.attr.inode,
        current.body.as_ref().unwrap().generation,
        0,
        0,
    );
    assert!(service
        .retire_snapshot_path("/ws", snap.snapshot_id)
        .unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert!(cleanup.deleted >= 2, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&original_block).unwrap().is_none());
    assert!(objects.head(&diverged_block).unwrap().is_none());
    assert!(objects.head(&current_block).unwrap().is_some());
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), b"A3");
}

#[test]
fn rollback_binding_protects_an_owner_gc_row_enqueued_after_the_swap() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(&service, "/ws/a", "rollback-late-owner", b"shared");
    let source_body = source.body.as_ref().unwrap();
    let source_block = block_key(source.attr.inode, source_body.generation, 0, 0);
    let snap = service.snapshot_subtree_path("/ws").unwrap();
    service
        .link(source.attr.inode, InodeId::root(), dname(b"outside"))
        .unwrap();
    service.remove_file(ws.attr.inode, &dname(b"a")).unwrap();
    assert_eq!(service.cleanup_pending_objects(100).unwrap().attempted, 0);

    service
        .rollback_subtree_path("/ws", snap.snapshot_id)
        .unwrap();
    let restored = service
        .lookup_plus(ws.attr.inode, &dname(b"a"))
        .unwrap()
        .unwrap();
    let pin = service.snapshot_pin(snap.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );

    service
        .remove_file(InodeId::root(), &dname(b"outside"))
        .unwrap();
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 0, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&source_block).unwrap().is_some());
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), b"shared");
    assert!(matches!(
        service.retire_snapshot_path("/ws", snap.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            borrower,
            ..
        }) if snapshot_id == snap.snapshot_id && borrower == restored.attr.inode
    ));

    service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:self-owned".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-late-owner/self-owned".to_owned(),
            bytes: b"self-owned".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    assert!(service
        .retire_snapshot_path("/ws", snap.snapshot_id)
        .unwrap());
    assert!(service.cleanup_pending_objects(100).unwrap().deleted >= 1);
    assert!(objects.head(&source_block).unwrap().is_none());
}

#[test]
fn rollback_preserves_the_gc_row_for_a_restored_delta_base() {
    let (service, objects) = service_with_objects();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    let original = publish_path_artifact(&service, "/ws/a", "rollback-delta/a1", b"A1");
    let original_body = original.body.as_ref().unwrap();
    let original_generation = original_body.generation;
    let original_block = block_key(original.attr.inode, original_generation, 0, 0);
    let snap = service.snapshot_subtree_path("/ws").unwrap();

    let prepared = service
        .prepare_artifact_replace(ws.attr.inode, dname(b"a"))
        .unwrap();
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "rollback-delta/a2",
            &[PublishArtifactRange {
                offset: original_body.size,
                bytes: b"-delta".to_vec(),
            }],
            0,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();
    let chunks = written.chunk_manifests();
    service
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent: ws.attr.inode,
                name: dname(b"a"),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:a2".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "rollback-delta/a2".to_owned(),
                size: original_body.size + 6,
                chunks,
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    let appended = service.lookup_path("/ws/a").unwrap().unwrap();
    assert_eq!(
        appended.body.as_ref().unwrap().base_generation,
        original_generation
    );
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), b"A1-delta");

    service.remove_file(ws.attr.inode, &dname(b"a")).unwrap();
    let gc_records = || {
        service
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(service.mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap()
            .into_iter()
            .map(|row| decode_object_gc_record(&row.value.0).unwrap())
            .collect::<Vec<_>>()
    };
    let original_candidates_before_rollback = gc_records()
        .iter()
        .filter(|record| record.object_key == original_block.as_str())
        .count();
    assert_eq!(
        original_candidates_before_rollback, 1,
        "final unlink must enqueue every owner object in the delta generation chain"
    );

    service
        .rollback_subtree_path("/ws", snap.snapshot_id)
        .unwrap();
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), b"A1");
    let original_candidates_after_rollback = gc_records()
        .iter()
        .filter(|record| record.object_key == original_block.as_str())
        .count();
    assert_eq!(
        original_candidates_after_rollback,
        original_candidates_before_rollback + 1,
        "rollback must proactively make every restored owner object reclaimable"
    );

    let pin = service.snapshot_pin(snap.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert_eq!(
        service.reclaim_expired_snapshot_pins(100).unwrap().reaped,
        1
    );
    assert_eq!(service.cleanup_pending_objects(100).unwrap().deleted, 0);
    assert!(objects.head(&original_block).unwrap().is_some());

    service.remove_file(ws.attr.inode, &dname(b"a")).unwrap();
    assert!(service
        .retire_snapshot_path("/ws", snap.snapshot_id)
        .unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert!(cleanup.deleted >= 2, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&original_block).unwrap().is_none());
}

#[test]
fn rollback_on_a_clone_root_keeps_both_retention_bindings() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/base/a", "layered/base", b"base");
    let clone = service.clone_subtree_path_into("/base", "/fork").unwrap();
    let snap = service.snapshot_subtree_path("/fork").unwrap();
    service
        .replace_artifact(PublishArtifact {
            parent: clone.root,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:diverged".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "layered/diverged".to_owned(),
            bytes: b"diverged".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .rollback_subtree_path("/fork", snap.snapshot_id)
        .unwrap();

    let bindings = service
        .versioned_fork_bindings_at(service.read_version().unwrap(), ReadPurpose::UserStrong)
        .unwrap();
    let clone_binding = bindings
        .iter()
        .find(|binding| binding.binding.snapshot_id == clone.snapshot_id)
        .unwrap();
    let rollback_binding = bindings
        .iter()
        .find(|binding| binding.binding.snapshot_id == snap.snapshot_id)
        .unwrap();
    assert_eq!(clone_binding.binding.fork_root, clone.root);
    assert_eq!(rollback_binding.binding.source_root, clone.root);
    assert_ne!(rollback_binding.binding.fork_root, clone.root);
    assert_ne!(clone_binding.key, rollback_binding.key);
    assert!(matches!(
        service.retire_snapshot(clone.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            ..
        }) if snapshot_id == clone.snapshot_id
    ));
    assert!(matches!(
        service.retire_snapshot(snap.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            ..
        }) if snapshot_id == snap.snapshot_id
    ));
    assert_eq!(read_artifact_at_path(&service, "/fork/a"), b"base");
}

#[test]
fn rollback_rejects_hardlinks_in_snapshot_or_current_tree() {
    let snapshot_linked = service();
    snapshot_linked
        .create_dir_path("/ws", 0o755, 1000, 1000)
        .unwrap();
    let file = publish_path_artifact(&snapshot_linked, "/ws/a", "hardlink-snapshot", b"body");
    snapshot_linked
        .link(file.attr.inode, InodeId::root(), dname(b"outside"))
        .unwrap();
    let snap = snapshot_linked.snapshot_subtree_path("/ws").unwrap();
    let error = snapshot_linked
        .rollback_subtree_path("/ws", snap.snapshot_id)
        .unwrap_err();
    assert!(matches!(
        &error,
        MetadError::InvalidPath(message) if message.contains("hardlink-free")
    ));
    assert_eq!(
        snapshot_linked
            .read_artifact(InodeId::root(), &dname(b"outside"))
            .unwrap(),
        b"body"
    );

    let current_linked = service();
    let ws = current_linked
        .create_dir_path("/ws", 0o755, 1000, 1000)
        .unwrap();
    let file = publish_path_artifact(&current_linked, "/ws/a", "hardlink-current", b"body");
    let snap = current_linked.snapshot_subtree_path("/ws").unwrap();
    current_linked
        .link(file.attr.inode, InodeId::root(), dname(b"outside"))
        .unwrap();
    let error = current_linked
        .rollback_subtree(ws.attr.inode, snap.snapshot_id)
        .unwrap_err();
    assert!(matches!(
        &error,
        MetadError::InvalidPath(message) if message.contains("hardlink-free")
    ));
    assert_eq!(read_artifact_at_path(&current_linked, "/ws/a"), b"body");
    assert_eq!(
        current_linked
            .read_artifact(InodeId::root(), &dname(b"outside"))
            .unwrap(),
        b"body"
    );
}

#[test]
fn rollback_subtree_restores_snapshot_shares_blocks_and_reclaims_delta() {
    let (service, objects) = service_with_objects();
    // 1. Build /ws with files a="A1", b="B1", sub/c="C1" (real object data).
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/ws/sub", 0o755, 1000, 1000)
        .unwrap();
    let a = publish_path_artifact(&service, "/ws/a", "ws/a", &vec![b'1'; 4096]);
    let b = publish_path_artifact(&service, "/ws/b", "ws/b", &vec![b'2'; 4096]);
    let c = publish_path_artifact(&service, "/ws/sub/c", "ws/sub/c", &vec![b'3'; 4096]);
    let a_gen = a.body.as_ref().unwrap().generation;
    let b_gen = b.body.as_ref().unwrap().generation;
    let c_gen = c.body.as_ref().unwrap().generation;
    let a1_block = block_key(a.attr.inode, a_gen, 0, 0);
    let b1_block = block_key(b.attr.inode, b_gen, 0, 0);
    let c1_block = block_key(c.attr.inode, c_gen, 0, 0);
    assert!(objects.head(&a1_block).unwrap().is_some());
    assert!(objects.head(&b1_block).unwrap().is_some());
    assert!(objects.head(&c1_block).unwrap().is_some());

    // 2. Snapshot /ws.
    let snap = service.snapshot_subtree_path("/ws").unwrap();

    // 3. Diverge /ws: rewrite a->"A2", add d="D1", delete b.
    service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:a2".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "ws/a2".to_owned(),
            bytes: vec![b'4'; 4096],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let d = publish_path_artifact(&service, "/ws/d", "ws/d", &vec![b'5'; 4096]);
    service.remove_file(ws.attr.inode, &dname(b"b")).unwrap();
    // Capture the delta's private blocks so we can assert their fate.
    let a_diverged = service
        .lookup_plus(ws.attr.inode, &dname(b"a"))
        .unwrap()
        .unwrap();
    let a2_block = block_key(
        a_diverged.attr.inode,
        a_diverged.body.as_ref().unwrap().generation,
        0,
        0,
    );
    let d1_block = block_key(d.attr.inode, d.body.as_ref().unwrap().generation, 0, 0);
    assert!(objects.head(&a2_block).unwrap().is_some());
    assert!(objects.head(&d1_block).unwrap().is_some());
    // Pre-rollback /ws is the diverged state.
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'4'; 4096]);
    assert!(service
        .lookup_plus(ws.attr.inode, &dname(b"b"))
        .unwrap()
        .is_none());

    // 4. Roll /ws back to the snapshot.
    service
        .rollback_subtree_path("/ws", snap.snapshot_id)
        .unwrap();

    // 5. /ws now exactly matches the snapshot: a="A1", b="B1" (restored), sub/c="C1",
    //    and d is gone. The target keeps its inode identity.
    assert_eq!(
        service.resolve_directory_path("/ws").unwrap(),
        ws.attr.inode,
        "rollback keeps the target root's identity"
    );
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'1'; 4096]);
    assert_eq!(read_artifact_at_path(&service, "/ws/b"), vec![b'2'; 4096]);
    assert_eq!(
        read_artifact_at_path(&service, "/ws/sub/c"),
        vec![b'3'; 4096]
    );
    assert!(
        service
            .lookup_plus(ws.attr.inode, &dname(b"d"))
            .unwrap()
            .is_none(),
        "the delta-only file d must be gone after rollback"
    );

    // 6. The rolled-back /ws is identical to a fresh clone of the snapshot: an empty
    //    diff in both directions.
    let reference = service
        .clone_subtree_path_into("/ws", "/reference")
        .unwrap();
    assert!(service
        .diff_subtrees(ws.attr.inode, reference.root)
        .unwrap()
        .is_empty());
    assert!(service
        .diff_subtrees(reference.root, ws.attr.inode)
        .unwrap()
        .is_empty());

    // Remove the reference fork itself. Its binding still cannot be retired:
    // rollback propagated the same borrowed keys onto fresh /ws inodes, which
    // the mount-global retirement proof must discover even though they were
    // never descendants of `reference.root`.
    let reference_sub = service
        .lookup_plus(reference.root, &dname(b"sub"))
        .unwrap()
        .unwrap();
    service
        .remove_file(reference_sub.attr.inode, &dname(b"c"))
        .unwrap();
    service
        .remove_empty_dir(reference.root, &dname(b"sub"))
        .unwrap();
    service.remove_file(reference.root, &dname(b"a")).unwrap();
    service.remove_file(reference.root, &dname(b"b")).unwrap();
    service.remove_empty_dir_path("/reference").unwrap();

    // 7. Both durable bindings fail closed while /ws still borrows the restored
    //    blocks. The rollback binding is intentionally conservative: its old
    //    mount-wide floor also delays the discarded delta until the borrowers
    //    are rewritten.
    assert!(matches!(
        service.retire_snapshot(snap.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            ..
        }) if snapshot_id == snap.snapshot_id
    ));
    assert!(matches!(
        service.retire_snapshot(reference.snapshot_id),
        Err(MetadError::ForkRetentionActive {
            snapshot_id,
            fork_root,
            ..
        }) if snapshot_id == reference.snapshot_id && fork_root == reference.root
    ));
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(reclaim.deleted, 0, "cleanup outcome: {reclaim:?}");
    assert!(objects.head(&a2_block).unwrap().is_some());
    assert!(objects.head(&d1_block).unwrap().is_some());
    assert!(
        objects.head(&a1_block).unwrap().is_some(),
        "A1 must survive"
    );
    assert!(
        objects.head(&b1_block).unwrap().is_some(),
        "B1 must survive"
    );
    assert!(
        objects.head(&c1_block).unwrap().is_some(),
        "C1 must survive"
    );
    // Restored content is still readable from the shared blocks after reclaim.
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'1'; 4096]);
    assert_eq!(read_artifact_at_path(&service, "/ws/b"), vec![b'2'; 4096]);
    assert_eq!(
        read_artifact_at_path(&service, "/ws/sub/c"),
        vec![b'3'; 4096]
    );

    // Once every rollback borrower owns a fresh generation, each binding can be
    // retired independently and both the old snapshot blocks and discarded delta
    // become reclaimable.
    let restored_sub = service
        .lookup_plus(ws.attr.inode, &dname(b"sub"))
        .unwrap()
        .unwrap();
    let rewrite = |parent: InodeId, name: &[u8], manifest: &str, byte: u8| {
        service
            .replace_artifact(PublishArtifact {
                parent,
                name: dname(name),
                producer: "unit-test".to_owned(),
                digest_uri: format!("sha256:{manifest}"),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest.to_owned(),
                bytes: vec![byte; 4096],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap()
            .entry
    };
    let a3 = rewrite(ws.attr.inode, b"a", "ws/a3", b'6');
    let b3 = rewrite(ws.attr.inode, b"b", "ws/b3", b'7');
    let c3 = rewrite(restored_sub.attr.inode, b"c", "ws/c3", b'8');
    let a3_block = block_key(a3.attr.inode, a3.body.as_ref().unwrap().generation, 0, 0);
    let b3_block = block_key(b3.attr.inode, b3.body.as_ref().unwrap().generation, 0, 0);
    let c3_block = block_key(c3.attr.inode, c3.body.as_ref().unwrap().generation, 0, 0);
    assert!(service.retire_snapshot(snap.snapshot_id).unwrap());
    assert!(service.retire_snapshot(reference.snapshot_id).unwrap());
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert!(reclaim.deleted >= 5, "cleanup outcome: {reclaim:?}");
    for old in [&a1_block, &b1_block, &c1_block, &a2_block, &d1_block] {
        assert!(objects.head(old).unwrap().is_none(), "old block {old:?}");
    }
    for current in [&a3_block, &b3_block, &c3_block] {
        assert!(
            objects.head(current).unwrap().is_some(),
            "current block {current:?}"
        );
    }
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'6'; 4096]);
    assert_eq!(read_artifact_at_path(&service, "/ws/b"), vec![b'7'; 4096]);
    assert_eq!(
        read_artifact_at_path(&service, "/ws/sub/c"),
        vec![b'8'; 4096]
    );
}

#[test]
fn rollback_subtree_rejects_an_expired_snapshot_before_materializing() {
    let service = service();
    service.set_clock_override_ms(1_000);
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/ws/a", "rollback-expired", b"original");
    let pin = service
        .snapshot_subtree_with_lease(ws.attr.inode, 500)
        .unwrap();

    service.set_clock_override_ms(1_500);
    assert!(matches!(
        service.rollback_subtree(ws.attr.inode, pin.snapshot_id),
        Err(MetadError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms: 1_500,
            now_ms: 1_500,
        }) if snapshot_id == pin.snapshot_id
    ));
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), b"original");
}

#[test]
fn rollback_holds_the_gc_gate_until_restored_references_are_live() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RenameReplace, 1, 2);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    let ws_inode = ws.attr.inode;
    let original = service
        .publish_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:original".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-race/original".to_owned(),
            bytes: b"original".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let original_body = original.body.as_ref().unwrap();
    let original_object = block_key(original.attr.inode, original_body.generation, 0, 0);
    let pin = service
        .snapshot_subtree_with_lease(ws.attr.inode, 500)
        .unwrap();
    service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:delta".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "rollback-race/delta".to_owned(),
            bytes: b"delta".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let rollback_service = Arc::clone(&service);
    let rollback =
        std::thread::spawn(move || rollback_service.rollback_subtree(ws_inode, pin.snapshot_id));
    store.wait_until_blocked();
    assert!(service.object_gc_gate.try_lock().is_err());

    let deadline_ms = start_ms + 500;
    service.set_clock_override_ms(deadline_ms);
    let cleanup_started = Arc::new(Barrier::new(2));
    let cleanup_service = Arc::clone(&service);
    let cleanup_thread_started = Arc::clone(&cleanup_started);
    let cleanup = std::thread::spawn(move || {
        cleanup_thread_started.wait();
        cleanup_service.cleanup_pending_objects(100)
    });
    cleanup_started.wait();
    store.release_blocked();

    rollback.join().unwrap().unwrap();
    cleanup.join().unwrap().unwrap();
    assert!(objects.head(&original_object).unwrap().is_some());
    assert_eq!(
        service.read_artifact(ws_inode, &dname(b"a")).unwrap(),
        b"original"
    );
}

#[test]
fn rollback_subtree_rejects_foreign_or_missing_snapshot() {
    let service = service();
    service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/other", 0o755, 1000, 1000)
        .unwrap();
    let other_root = service.resolve_directory_path("/other").unwrap();
    let snap = service.snapshot_subtree_path("/other").unwrap();

    // A snapshot of /other cannot roll back /ws.
    assert!(matches!(
        service.rollback_subtree_path("/ws", snap.snapshot_id),
        Err(MetadError::InvalidPath(_))
    ));
    // An unknown snapshot id is not found.
    assert!(matches!(
        service.rollback_subtree_path("/ws", snap.snapshot_id + 9_999),
        Err(MetadError::NotFound)
    ));
    // The rejected target is untouched and the legitimate one still works.
    assert!(service
        .rollback_subtree(other_root, snap.snapshot_id)
        .is_ok());
}

#[test]
fn metadata_backup_then_restore_into_fresh_store_recovers_namespace() {
    let (service, objects) = service_with_objects();
    // Build a small namespace; file bodies land in the shared object store.
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/data", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "m-a", b"alpha-body");
    publish_path_artifact(&service, "/data/b.bin", "m-b", b"bravo-body-2");
    let want_runs = service.lookup_path("/runs/a.bin").unwrap();
    let want_data = service.lookup_path("/data/b.bin").unwrap();
    assert!(want_runs.is_some());

    let config = MetadataArchiveConfig::new("meta/checkpoints", 3);
    let backup = service.backup_metadata(&config).unwrap();
    assert!(backup.image_bytes > 0);
    assert!(backup.checkpoint_key.starts_with("meta/checkpoints/ckpt/"));

    // Simulate total loss of the metadata node: a brand-new empty Holt store,
    // pointed at the SAME object store (the clone shares the backing map).
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    // The fresh node has no namespace at all (not even a root) until restore.
    let outcome = recovered.restore_metadata(&config).unwrap();
    assert_eq!(
        outcome.as_ref().map(|o| o.checkpoint_key.as_str()),
        Some(backup.checkpoint_key.as_str())
    );

    // Namespace entries (dentry + attr + body descriptor) are identical after
    // restore, and a subsequent create allocates a fresh, non-colliding inode.
    assert_eq!(recovered.lookup_path("/runs/a.bin").unwrap(), want_runs);
    assert_eq!(recovered.lookup_path("/data/b.bin").unwrap(), want_data);
    let fresh = publish_path_artifact(&recovered, "/runs/c.bin", "m-c", b"charlie");
    assert_ne!(fresh.attr.inode, want_runs.unwrap().attr.inode);
}

#[test]
fn checkpoint_install_error_permanently_poisons_the_service_instance() {
    let (source, objects) = service_with_objects();
    source
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let config = MetadataArchiveConfig::new("meta/poisoned-checkpoint-install", 2);
    source.backup_metadata(&config).unwrap();

    let metadata = RestorePostApplyFaultStore::new();
    metadata.fail_checkpoint_after_install();
    let recovered = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects);
    assert!(matches!(
        recovered.restore_metadata(&config),
        Err(MetadError::Metadata(MetadataError::Backend(message)))
            if message.contains("checkpoint install error after apply")
    ));

    for error in [
        recovered.lookup_path("/source").unwrap_err(),
        recovered.restore_metrics().unwrap_err(),
        recovered.fsck_restore_state(false).unwrap_err(),
        recovered
            .create_dir_path("/must-not-serve", 0o755, 1000, 1000)
            .unwrap_err(),
        recovered.restore_metadata(&config).unwrap_err(),
    ] {
        assert!(matches!(
            error,
            MetadError::MetadataCheckpointInstallUncertain
        ));
    }
}

#[test]
fn restore_metadata_without_archive_returns_none() {
    let (service, _objects) = service_with_objects();
    let config = MetadataArchiveConfig::new("meta/empty", 3);
    assert!(service.restore_metadata(&config).unwrap().is_none());
}

#[test]
fn restore_rejects_a_pre_fence_archive_before_installing_its_image() {
    let (service, objects) = service_with_objects();
    service
        .create_dir_path("/unsafe", 0o755, 1000, 1000)
        .unwrap();
    let config = MetadataArchiveConfig::new("meta/legacy", 3);
    let backup = service.backup_metadata(&config).unwrap();

    // Downgrade only CURRENT to the legacy format. The referenced image is
    // actually valid and fenced, so an empty target proves rejection happened
    // from the pointer proof before install_checkpoint_image could mutate it.
    let current_key = ObjectKey::new("meta/legacy/CURRENT").unwrap();
    let current = String::from_utf8(objects.get(&current_key, None).unwrap()).unwrap();
    let legacy = current
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if line.starts_with("object_gc_failover_fenced\t") {
                None
            } else if index == 0 {
                Some("nokv-metadata-archive\t1".to_owned())
            } else {
                Some(line.to_owned())
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    objects.put(&current_key, legacy.into_bytes()).unwrap();

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    assert!(matches!(
        recovered.restore_metadata(&config),
        Err(MetadError::MetadataArchiveMissingObjectGcFence { checkpoint_key })
            if checkpoint_key == backup.checkpoint_key
    ));
    assert!(recovered.get_attr(InodeId::root()).unwrap().is_none());
}

#[test]
fn metadata_backup_retains_only_keep_last_checkpoints() {
    let (service, objects) = service_with_objects();
    let config = MetadataArchiveConfig::new("meta/ck", 2);
    let b1 = service.backup_metadata(&config).unwrap();
    let _b2 = service.backup_metadata(&config).unwrap();
    let b3 = service.backup_metadata(&config).unwrap();
    // keep_last=2: the third backup prunes exactly the first checkpoint object.
    assert_eq!(b3.pruned, 1);
    let pruned = ObjectKey::new(b1.checkpoint_key.clone()).unwrap();
    assert!(objects.head(&pruned).unwrap().is_none());
    let live = ObjectKey::new(b3.checkpoint_key.clone()).unwrap();
    assert!(objects.head(&live).unwrap().is_some());
    // Restore (into a fresh store) always selects the latest checkpoint.
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    let restored = recovered.restore_metadata(&config).unwrap().unwrap();
    assert_eq!(restored.checkpoint_key, b3.checkpoint_key);
}

fn log_test_command(request_id: &[u8], commit_version: u64) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::CreateFile,
        read_version: Version::new(commit_version - 1).unwrap(),
        commit_version: Version::new(commit_version).unwrap(),
        primary_family: RecordFamily::Dentry,
        primary_key: b"log-primary".to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: b"log-primary".to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: b"log-primary".to_vec(),
            op: MutationOp::Put,
            value: Some(Value(b"log-value".to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn log_test_entry(
    request_id: &[u8],
    lsn: u64,
    commit_version: u64,
    prev_digest: [u8; 32],
) -> MetadataLogEntry {
    MetadataLogEntry::seal(
        "mount-1:/runs",
        1,
        lsn,
        log_test_command(request_id, commit_version),
        CommitResult {
            commit_version: Version::new(commit_version).unwrap(),
            applied_mutations: 1,
            watch_events: 0,
        },
        prev_digest,
    )
    .unwrap()
}

fn snapshot_segment_keys(snapshot: &MetadataLogSyncSnapshot) -> Vec<String> {
    snapshot
        .segments
        .iter()
        .map(|segment| segment.segment_key.clone())
        .collect()
}

fn log_replay_command(
    request_id: &[u8],
    key: &[u8],
    value: &[u8],
    read_version: u64,
    commit_version: u64,
) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::RegisterNamespaceIndex,
        read_version: Version::new(read_version).unwrap(),
        commit_version: Version::new(commit_version).unwrap(),
        primary_family: RecordFamily::System,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::System,
            key: key.to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::System,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(value.to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn ordered_log_put_command(
    request_id: &[u8],
    key: &[u8],
    value: &[u8],
    read_version: u64,
    commit_version: u64,
) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::RegisterNamespaceIndex,
        read_version: Version::new(read_version).unwrap(),
        commit_version: Version::new(commit_version).unwrap(),
        primary_family: RecordFamily::System,
        primary_key: key.to_vec(),
        predicates: Vec::new(),
        mutations: vec![Mutation {
            family: RecordFamily::System,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(value.to_vec())),
        }],
        watch: Vec::new(),
    }
}

#[test]
fn metadata_log_segment_archive_round_trips_through_object_store() {
    let (service, objects) = service_with_objects();
    let first = log_test_entry(b"req-log-1", 1, 11, METADATA_LOG_ZERO_DIGEST);
    let second = log_test_entry(b"req-log-2", 2, 12, first.digest);
    let segment = MetadataLogSegment::seal(vec![first, second]).unwrap();
    let config = MetadataLogArchiveConfig::new("meta/shared-log");

    let archived = service
        .archive_metadata_log_segment(&config, &segment)
        .unwrap();

    assert!(archived.segment_key.starts_with("meta/shared-log/log/"));
    assert!(archived
        .segment_key
        .contains("/00000000000000000001-00000000000000000002-"));
    assert!(archived.segment_key.ends_with(".segment"));
    assert_eq!(archived.first_lsn, 1);
    assert_eq!(archived.last_lsn, 2);
    assert_eq!(
        archived.encoded_bytes,
        segment.encode().unwrap().len() as u64
    );
    assert!(objects
        .head(&ObjectKey::new(archived.segment_key.clone()).unwrap())
        .unwrap()
        .is_some());

    let loaded = service
        .load_metadata_log_segment(&archived.segment_key)
        .unwrap();
    assert_eq!(loaded, segment);
}

#[test]
fn metadata_log_archive_config_rejects_keys_outside_the_exact_shard_prefix() {
    let config = MetadataLogArchiveConfig::new("meta/shared-log/mount_1__");
    let valid = concat!(
        "meta/shared-log/mount_1__/log/",
        "00000000000000000001-00000000000000000001-",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.segment"
    );
    config.validate_segment_key(valid).unwrap();

    for invalid in [
        concat!(
            "meta/shared-log/mount_2__/log/",
            "00000000000000000001-00000000000000000001-",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.segment"
        ),
        concat!(
            "meta/shared-log/mount_1__/log/nested/",
            "00000000000000000001-00000000000000000001-",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.segment"
        ),
        "meta/shared-log/mount_1__/log/",
    ] {
        assert!(matches!(
            config.validate_segment_key(invalid),
            Err(MetadError::Codec(_))
        ));
    }
}

#[test]
fn metadata_log_segment_load_rejects_corrupted_object() {
    let (service, objects) = service_with_objects();
    let first = log_test_entry(b"req-log-1", 1, 11, METADATA_LOG_ZERO_DIGEST);
    let segment = MetadataLogSegment::seal(vec![first]).unwrap();
    let config = MetadataLogArchiveConfig::new("meta/shared-log");
    let archived = service
        .archive_metadata_log_segment(&config, &segment)
        .unwrap();
    let key = ObjectKey::new(archived.segment_key.clone()).unwrap();
    objects.put(&key, b"corrupted-segment".to_vec()).unwrap();

    assert!(matches!(
        service.load_metadata_log_segment(&archived.segment_key),
        Err(MetadError::Codec(_))
    ));
}

#[test]
fn restore_metadata_with_archived_log_segments_replays_after_checkpoint() {
    let (service, objects) = service_with_objects();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-log-replay", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();

    let key = b"log-replay-system-key".to_vec();
    let value = b"after-checkpoint".to_vec();
    let commit_version = checkpoint.commit_version + 1;
    let command = log_replay_command(
        b"req-log-replay",
        &key,
        &value,
        checkpoint.commit_version,
        commit_version,
    );
    let result = service.commit_metadata(command.clone()).unwrap();
    let entry =
        MetadataLogEntry::seal("mount-1:/", 1, 1, command, result, METADATA_LOG_ZERO_DIGEST)
            .unwrap();
    let segment = MetadataLogSegment::seal(vec![entry.clone()]).unwrap();
    let log_config = MetadataLogArchiveConfig::new("meta/shared-log-replay");
    let archived = service
        .archive_metadata_log_segment(&log_config, &segment)
        .unwrap();

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    let outcome = recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            std::slice::from_ref(&archived.segment_key),
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.checkpoint.checkpoint_key, checkpoint.checkpoint_key);
    assert_eq!(outcome.replayed_entries, 1);
    assert_eq!(outcome.durable_lsn, 1);
    assert_eq!(outcome.last_digest, entry.digest);
    assert!(recovered.read_version().unwrap().get() >= commit_version);
    assert_eq!(
        recovered
            .metadata
            .get(
                RecordFamily::System,
                &key,
                Version::new(commit_version).unwrap(),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(value))
    );
}

#[test]
fn log_replay_recovers_partial_materialization_as_an_unbound_orphan() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/base/data.bin",
        "replay-orphan/source",
        b"borrowed",
    );
    let pin = service.snapshot_subtree(base.attr.inode).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/replay-orphan-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    // The checkpoint is healthy. Only the subsequent detached materialization
    // commits enter the retained log tail; no ForkBinding commit follows them.
    let checkpoint_config = MetadataArchiveConfig::new("meta/replay-orphan-checkpoint", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    assert!(!service.materialization_orphan_slow_path_enabled());
    let orphan_root = {
        let _object_gc_gate = service.object_gc_gate.lock().unwrap();
        service
            .materialize_subtree_at(base.attr.inode, Version::new(pin.read_version).unwrap())
            .unwrap()
    };
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert!(snapshot.durable_lsn > checkpoint.log_lsn);
    assert!(!snapshot.segments.is_empty());
    let segments = snapshot
        .segments
        .iter()
        .map(|pointer| service.load_metadata_log_segment(&pointer.segment_key))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(segments
        .iter()
        .flat_map(|segment| &segment.entries)
        .flat_map(|entry| &entry.command.mutations)
        .all(|mutation| mutation.family != RecordFamily::ForkBinding));

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    let outcome = recovered
        .restore_metadata_with_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segments,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();
    assert_eq!(outcome.replayed_entries, 2);
    assert!(
        recovered.materialization_orphan_slow_path_enabled(),
        "partial materialization replay must remain fail-closed"
    );
    let orphan = recovered
        .lookup_plus(orphan_root, &dname(b"data.bin"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        recovered.link(orphan.attr.inode, InodeId::root(), dname(b"replayed-link")),
        Err(MetadError::NotFound)
    ));
    assert!(matches!(
        recovered.rename(
            orphan_root,
            &dname(b"data.bin"),
            InodeId::root(),
            dname(b"replayed-rename")
        ),
        Err(MetadError::NotFound)
    ));
}

#[test]
fn restore_metadata_with_log_segments_rejects_chain_gap_before_restore() {
    let (service, objects) = service_with_objects();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-log-gap", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();

    let command = log_replay_command(
        b"req-log-gap",
        b"log-gap-system-key",
        b"value",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    let result = service.commit_metadata(command.clone()).unwrap();
    let entry =
        MetadataLogEntry::seal("mount-1:/", 1, 2, command, result, METADATA_LOG_ZERO_DIGEST)
            .unwrap();
    let segment = MetadataLogSegment::seal(vec![entry]).unwrap();
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );

    let err = recovered
        .restore_metadata_with_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &[segment],
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap_err();

    assert!(matches!(err, MetadError::Codec(message) if message.contains("lsn gap")));
}

#[test]
fn sync_metadata_log_archives_commit_before_recovery_ack() {
    let (service, objects) = service_with_objects();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-sync-log", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let key = b"sync-log-system-key".to_vec();
    let value = b"sync-after-checkpoint".to_vec();
    let commit_version = checkpoint.commit_version + 1;
    let command = log_replay_command(
        b"req-sync-log",
        &key,
        &value,
        checkpoint.commit_version,
        commit_version,
    );
    let result = service.commit_metadata(command).unwrap();
    assert_eq!(result.commit_version.get(), commit_version);
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 1);
    assert_eq!(snapshot.segments.len(), 1);
    let segment_pointer = snapshot.segments.last().unwrap();
    assert!(segment_pointer
        .segment_key
        .starts_with("meta/sync-log/log/"));

    let loaded = service
        .load_metadata_log_segment(&segment_pointer.segment_key)
        .unwrap();
    assert_eq!(loaded.first_lsn, 1);
    assert_eq!(loaded.last_lsn, 1);
    assert_eq!(loaded.last_digest, snapshot.last_digest);

    let segment_keys = snapshot_segment_keys(&snapshot);
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segment_keys,
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        recovered
            .metadata
            .get(
                RecordFamily::System,
                &key,
                Version::new(commit_version).unwrap(),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(value))
    );
}

#[test]
fn log_replay_does_not_fold_a_foreign_graft_inode_into_the_local_allocator() {
    let (service, objects) = service_with_objects();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-graft-log-replay", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/graft-log-replay",
            "mount-1:/",
            1,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        ))
        .unwrap();

    let foreign_inode = InodeId::compose(1, 42).unwrap();
    service
        .create_graft(
            InodeId::root(),
            dname(b"dataset"),
            foreign_inode,
            0o755,
            1000,
            1000,
        )
        .unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.segments.len(), 1);

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    )
    .with_shard_index(0);
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &snapshot_segment_keys(&snapshot),
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();

    let graft = recovered
        .lookup_plus(InodeId::root(), &dname(b"dataset"))
        .unwrap();
    assert_eq!(graft.unwrap().dentry.child, foreign_inode);
    let local = recovered
        .create_file(
            InodeId::root(),
            dname(b"local-after-replay"),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert_eq!(
        local.attr.inode.shard_index(),
        0,
        "a foreign graft target replayed from the log must not poison the local allocator"
    );
}

#[test]
fn concurrent_metadata_apply_and_log_archive_preserve_one_recovery_order() {
    const FIRST_REQUEST: &[u8] = b"req-ordered-log-first";
    const SECOND_REQUEST: &[u8] = b"req-ordered-log-second";
    let store = PostCommitBarrierStore::new(FIRST_REQUEST);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-ordered-sync-log", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/ordered-sync-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let key = b"ordered-sync-log-key".to_vec();
    let first = ordered_log_put_command(
        FIRST_REQUEST,
        &key,
        b"first",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    let second = ordered_log_put_command(
        SECOND_REQUEST,
        &key,
        b"second",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 2,
    );

    let first_service = Arc::clone(&service);
    let first_commit = std::thread::spawn(move || first_service.commit_metadata(first));
    store.wait_until_applied();

    let (second_tx, second_rx) = std::sync::mpsc::sync_channel(1);
    let second_service = Arc::clone(&service);
    let second_commit = std::thread::spawn(move || {
        let result = second_service.commit_metadata(second);
        second_tx.send(result).unwrap();
    });
    let early = second_rx.recv_timeout(Duration::from_millis(100));
    store.release_after_apply();
    first_commit.join().unwrap().unwrap();
    assert!(matches!(
        early,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    second_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    second_commit.join().unwrap();

    assert_eq!(
        service
            .metadata
            .get(
                RecordFamily::System,
                &key,
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"second".to_vec()))
    );
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
    assert_eq!(snapshot.segments.len(), 2);
    let first_segment = service
        .load_metadata_log_segment(&snapshot.segments[0].segment_key)
        .unwrap();
    let second_segment = service
        .load_metadata_log_segment(&snapshot.segments[1].segment_key)
        .unwrap();
    assert_eq!(first_segment.entries[0].command.request_id, FIRST_REQUEST);
    assert_eq!(second_segment.entries[0].command.request_id, SECOND_REQUEST);
    assert_eq!(first_segment.last_digest, second_segment.prev_digest);

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    let outcome = recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &snapshot_segment_keys(&snapshot),
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();
    assert_eq!(outcome.replayed_entries, 2);
    assert_eq!(
        recovered
            .metadata
            .get(
                RecordFamily::System,
                &key,
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"second".to_vec()))
    );
}

#[test]
fn metadata_commits_remain_concurrent_while_sync_log_is_disabled() {
    const FIRST_REQUEST: &[u8] = b"req-no-sync-first";
    let store = PostCommitBarrierStore::new(FIRST_REQUEST);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let base = service.read_version().unwrap().get();
    let key = b"no-sync-concurrent-key";
    let first = ordered_log_put_command(FIRST_REQUEST, key, b"first", base, base + 1);
    let second = ordered_log_put_command(b"req-no-sync-second", key, b"second", base + 1, base + 2);

    let first_service = Arc::clone(&service);
    let first_commit = std::thread::spawn(move || first_service.commit_metadata(first));
    store.wait_until_applied();

    let (second_tx, second_rx) = std::sync::mpsc::sync_channel(1);
    let second_service = Arc::clone(&service);
    let second_commit = std::thread::spawn(move || {
        second_tx
            .send(second_service.commit_metadata(second))
            .unwrap();
    });
    let second_result = second_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("a disabled sync log must not serialize unrelated metadata commits");
    store.release_after_apply();

    second_result.unwrap();
    second_commit.join().unwrap();
    first_commit.join().unwrap().unwrap();
    assert_eq!(
        service
            .metadata
            .get(
                RecordFamily::System,
                key,
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"second".to_vec()))
    );
}

#[test]
fn sync_metadata_log_snapshot_keeps_durable_tail_after_checkpoint_prune() {
    let (service, _objects) = service_with_objects();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-log-prune",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    service
        .create_dir_path("/before-checkpoint", 0o755, 1000, 1000)
        .unwrap();
    let first = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(first.segments.len(), 1);
    assert_eq!(first.segments[0].last_lsn, first.durable_lsn);
    assert_eq!(first.segments[0].last_digest, first.last_digest);

    service.prune_sync_metadata_log_segments(first.durable_lsn);
    let after_first_prune = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(after_first_prune.durable_lsn, first.durable_lsn);
    assert_eq!(after_first_prune.last_digest, first.last_digest);
    assert!(after_first_prune.segments.is_empty());

    service
        .create_dir_path("/after-checkpoint", 0o755, 1000, 1000)
        .unwrap();
    let second = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(second.segments.len(), 1);
    assert_eq!(second.segments[0].first_lsn, first.durable_lsn + 1);
    assert_eq!(second.segments[0].last_lsn, second.durable_lsn);
    assert_eq!(second.segments[0].last_digest, second.last_digest);
    let continued = service
        .load_metadata_log_segment(&second.segments[0].segment_key)
        .unwrap();
    assert_eq!(continued.prev_digest, first.last_digest);

    service.prune_sync_metadata_log_segments(second.durable_lsn);
    let after_second_prune = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(after_second_prune.durable_lsn, second.durable_lsn);
    assert_eq!(after_second_prune.last_digest, second.last_digest);
    assert!(after_second_prune.segments.is_empty());
}

#[test]
fn sync_metadata_log_prune_never_deletes_pointer_outside_archive_prefix() {
    let (service, objects) = service_with_objects();
    let unrelated = ObjectKey::new("unrelated/keep-me").unwrap();
    objects.put(&unrelated, b"keep".to_vec()).unwrap();
    service
        .enable_sync_metadata_log(
            MetadataLogSyncConfig::new("meta/sync-log-delete-scope", "mount-1:/", 1, 1, [7; 32])
                .with_segments(vec![MetadataLogSegmentPointer {
                    segment_key: unrelated.as_str().to_owned(),
                    first_lsn: 1,
                    last_lsn: 1,
                    last_digest: [7; 32],
                }]),
        )
        .unwrap();

    let outcome = service.prune_sync_metadata_log_segments(1);

    assert_eq!(outcome.pointers_pruned, 1);
    assert_eq!(outcome.objects_deleted, 0);
    assert_eq!(outcome.objects_missing, 0);
    assert_eq!(outcome.delete_failures, 1);
    assert!(objects.head(&unrelated).unwrap().is_some());
    assert!(service
        .sync_metadata_log_snapshot()
        .unwrap()
        .segments
        .is_empty());
}

#[test]
fn sync_metadata_log_rejects_duplicate_enable_without_replacing_tail() {
    let (service, _objects) = service_with_objects();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-log-original",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    service
        .create_dir_path("/before-duplicate-enable", 0o755, 1000, 1000)
        .unwrap();
    let before = service.sync_metadata_log_snapshot().unwrap();

    let error = service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-log-replacement",
            "mount-1:/",
            2,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::Codec(message) if message.contains("already enabled")
    ));
    assert_eq!(service.sync_metadata_log_snapshot().unwrap(), before);

    service
        .create_dir_path("/after-duplicate-enable", 0o755, 1000, 1000)
        .unwrap();
    let after = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(after.epoch, 1);
    assert_eq!(after.durable_lsn, before.durable_lsn + 1);
    assert!(after
        .segments
        .last()
        .unwrap()
        .segment_key
        .starts_with("meta/sync-log-original/log/"));
}

#[test]
fn sync_metadata_log_preflights_lsn_capacity_before_metadata_apply() {
    let (service, _objects) = service_with_objects();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-log-exhausted",
            "mount-1:/",
            1,
            u64::MAX - 1,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let error = service
        .create_dir_path("/must-not-apply", 0o755, 1000, 1000)
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::SyncLogArchiveFailed {
            committed: false,
            message,
        } if message.contains("LSN is exhausted before commit")
    ));
    assert!(service.lookup_path("/must-not-apply").unwrap().is_none());
    assert_eq!(
        service.sync_metadata_log_snapshot().unwrap().durable_lsn,
        u64::MAX - 1
    );
}

#[test]
fn restore_metadata_with_sync_log_advances_allocator_after_replay() {
    let (service, objects) = service_with_objects();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-sync-allocator", 2);
    service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-allocator-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let post_checkpoint = service
        .create_dir_path("/runs/post-checkpoint", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    let segment_keys = snapshot_segment_keys(&snapshot);

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segment_keys,
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        recovered
            .lookup_path("/runs/post-checkpoint")
            .unwrap()
            .unwrap()
            .attr
            .inode,
        post_checkpoint.attr.inode
    );

    let after_failover = recovered
        .create_dir_path("/after-failover", 0o755, 1000, 1000)
        .unwrap();
    assert!(
        after_failover.attr.inode.get() > post_checkpoint.attr.inode.get(),
        "failover replay must advance allocator past replayed namespace state"
    );
    assert_eq!(
        recovered
            .lookup_path("/runs/post-checkpoint")
            .unwrap()
            .unwrap()
            .attr
            .inode,
        post_checkpoint.attr.inode
    );
}

#[test]
fn prepare_only_allocator_reservation_replays_before_failover_reuse() {
    let (service, objects) = service_with_objects();
    // Initialize the durable object-GC claim before the checkpoint so the
    // prepare-only workload below changes only allocator reservations.
    service
        .prepare_artifact_create(InodeId::root(), dname(b"warmup.bin"))
        .unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-prepare-allocator", 2);
    service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/prepare-allocator-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let mut max_prepared_inode = 0;
    let mut max_prepared_generation = 0;
    for _ in 0..(ALLOCATOR_INODE_RESERVATION + 4) {
        let prepared = service
            .prepare_artifact_create(InodeId::root(), dname(b"never-published.bin"))
            .unwrap();
        max_prepared_inode = max_prepared_inode.max(prepared.inode.get());
        max_prepared_generation = max_prepared_generation.max(prepared.generation);
    }

    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert!(snapshot.durable_lsn > 0);
    let segments = snapshot
        .segments
        .iter()
        .map(|segment| {
            service
                .load_metadata_log_segment(&segment.segment_key)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(segments
        .iter()
        .flat_map(|segment| &segment.entries)
        .any(|entry| { entry.command.kind == CommandKind::ReserveAllocator }));

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &snapshot_segment_keys(&snapshot),
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();

    let after_failover = recovered
        .prepare_artifact_create(InodeId::root(), dname(b"after-failover.bin"))
        .unwrap();
    assert!(
        after_failover.inode.get() > max_prepared_inode,
        "recovery must not reuse an inode from a prepare-only reservation"
    );
    assert!(
        after_failover.generation > max_prepared_generation,
        "recovery must not reuse a generation from a prepare-only reservation"
    );
}

#[test]
fn failed_allocator_archive_keeps_local_watermark_slow_until_pending_flush() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing);
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    // Initialize the object-GC claim before enabling the log so the only logged
    // command in this workload is the allocator reservation at the boundary.
    service
        .prepare_artifact_create(InodeId::root(), dname(b"claim-warmup.bin"))
        .unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/allocator-pending-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    while service.clock.load(Ordering::Relaxed).saturating_add(1)
        <= service.reserved_version.load(Ordering::Relaxed)
        && service.next_inode.load(Ordering::Relaxed).saturating_add(1)
            <= service.reserved_next_inode.load(Ordering::Relaxed)
    {
        service
            .prepare_artifact_create(InodeId::root(), dname(b"boundary.bin"))
            .unwrap();
    }
    let reserved_version_before = service.reserved_version.load(Ordering::Relaxed);
    let reserved_inode_before = service.reserved_next_inode.load(Ordering::Relaxed);
    objects.fail_puts_containing("meta/allocator-pending-log/log/");

    assert!(matches!(
        service.prepare_artifact_create(InodeId::root(), dname(b"first-fails.bin")),
        Err(MetadError::SyncLogArchiveFailed {
            committed: true,
            ..
        })
    ));
    assert_eq!(
        service.reserved_version.load(Ordering::Relaxed),
        reserved_version_before,
        "a locally applied but unarchived reservation must not open the fast path"
    );
    assert_eq!(
        service.reserved_next_inode.load(Ordering::Relaxed),
        reserved_inode_before,
        "inode reservation watermark must stay behind until pending archive flush"
    );
    assert_eq!(service.sync_metadata_log_snapshot().unwrap().durable_lsn, 0);

    objects.clear_faults();
    service
        .prepare_artifact_create(InodeId::root(), dname(b"second-flushes.bin"))
        .unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(
        snapshot.durable_lsn, 2,
        "the retry must archive the prior applied reservation before its own reservation"
    );
    let kinds = snapshot
        .segments
        .iter()
        .map(|segment| {
            service
                .load_metadata_log_segment(&segment.segment_key)
                .unwrap()
        })
        .flat_map(|segment| segment.entries.into_iter())
        .map(|entry| entry.command.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![CommandKind::ReserveAllocator, CommandKind::ReserveAllocator]
    );
}

#[test]
fn sync_metadata_log_archives_independent_batch_as_one_segment() {
    let (service, objects) = service_with_objects();
    service.create_dir_path("/left", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/right", 0o755, 1000, 1000)
        .unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-sync-batch-log", 2);
    service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-batch-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let results = service.create_file_batches_in_dir_path(vec![
        CreateInDirPathBatch {
            parent_path: "/left".to_owned(),
            names: vec![DentryName::new(b"a.bin".to_vec()).unwrap()],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
        CreateInDirPathBatch {
            parent_path: "/right".to_owned(),
            names: vec![DentryName::new(b"b.bin".to_vec()).unwrap()],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ]);

    assert_eq!(results.len(), 2);
    for result in &results {
        assert_eq!(result.as_ref().unwrap().len(), 1);
    }
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
    assert_eq!(snapshot.segments.len(), 1);
    let segment = service
        .load_metadata_log_segment(&snapshot.segments.last().unwrap().segment_key)
        .unwrap();
    assert_eq!(segment.first_lsn, 1);
    assert_eq!(segment.last_lsn, 2);
    assert_eq!(segment.entries.len(), 2);

    let segment_keys = snapshot_segment_keys(&snapshot);
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    let outcome = recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segment_keys,
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.replayed_entries, 2);
    assert!(recovered.lookup_path("/left/a.bin").unwrap().is_some());
    assert!(recovered.lookup_path("/right/b.bin").unwrap().is_some());
}

#[test]
fn post_commit_backend_readback_preserves_single_commit_log_order() {
    let store = PostCommitErrorStore::new_disarmed(CommandKind::RegisterNamespaceIndex);
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), store.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-post-commit-readback", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/post-commit-readback-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    store.arm();
    let first = ordered_log_put_command(
        b"req-post-commit-first",
        b"post-commit-first",
        b"first",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    service.commit_metadata(first).unwrap();
    let second = ordered_log_put_command(
        b"req-post-commit-second",
        b"post-commit-second",
        b"second",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 2,
    );
    service.commit_metadata(second).unwrap();

    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
    let request_ids = snapshot
        .segments
        .iter()
        .map(|pointer| {
            service
                .load_metadata_log_segment(&pointer.segment_key)
                .unwrap()
        })
        .flat_map(|segment| segment.entries.into_iter())
        .map(|entry| entry.command.request_id)
        .collect::<Vec<_>>();
    assert_eq!(
        request_ids,
        vec![
            b"req-post-commit-first".to_vec(),
            b"req-post-commit-second".to_vec()
        ]
    );

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &snapshot_segment_keys(&snapshot),
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        recovered
            .metadata
            .get(
                RecordFamily::System,
                b"post-commit-second",
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"second".to_vec()))
    );
}

#[test]
fn readback_error_blocks_later_apply_and_both_checkpoint_publication_paths() {
    let store = PostCommitErrorStore::new_disarmed(CommandKind::RegisterNamespaceIndex);
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), store.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-readback-block", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/readback-block-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    store.arm();
    store.fail_next_readbacks(4);
    let first = ordered_log_put_command(
        b"req-readback-block-first",
        b"readback-block-first",
        b"first",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    assert!(matches!(
        service.commit_metadata(first),
        Err(MetadError::Codec(message)) if message.contains("readback failed")
    ));

    let blocked = ordered_log_put_command(
        b"req-readback-block-second",
        b"readback-block-second",
        b"second",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 2,
    );
    assert!(matches!(
        service.commit_metadata(blocked),
        Err(MetadError::Codec(message)) if message.contains("readback failed")
    ));
    assert_eq!(
        service
            .metadata
            .get(
                RecordFamily::System,
                b"readback-block-second",
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        None
    );

    assert!(matches!(
        service.backup_metadata(&checkpoint_config),
        Err(MetadError::Codec(message)) if message.contains("readback failed")
    ));
    let controlled = MetadataArchiveConfig::new("meta/controlled-readback-block", 2);
    assert!(matches!(
        service.prepare_immutable_metadata_backup(&controlled),
        Err(MetadError::Codec(message)) if message.contains("readback failed")
    ));
    assert_eq!(service.sync_metadata_log_snapshot().unwrap().durable_lsn, 0);

    store.clear_readback_failures();
    let third = ordered_log_put_command(
        b"req-readback-block-third",
        b"readback-block-third",
        b"third",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 3,
    );
    service.commit_metadata(third).unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
    let request_ids = snapshot
        .segments
        .iter()
        .map(|pointer| {
            service
                .load_metadata_log_segment(&pointer.segment_key)
                .unwrap()
        })
        .flat_map(|segment| segment.entries.into_iter())
        .map(|entry| entry.command.request_id)
        .collect::<Vec<_>>();
    assert_eq!(
        request_ids,
        vec![
            b"req-readback-block-first".to_vec(),
            b"req-readback-block-third".to_vec()
        ]
    );
}

#[test]
fn mismatched_readback_result_blocks_later_apply_until_exact_result_is_visible() {
    let store = PostCommitErrorStore::new_disarmed(CommandKind::RegisterNamespaceIndex);
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-readback-mismatch", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/readback-mismatch-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    store.arm();
    store.mismatch_next_readbacks(2);
    let first = ordered_log_put_command(
        b"req-readback-mismatch-first",
        b"readback-mismatch-first",
        b"first",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    assert!(matches!(
        service.commit_metadata(first),
        Err(MetadError::Codec(message)) if message.contains("result mismatch")
    ));
    let blocked = ordered_log_put_command(
        b"req-readback-mismatch-blocked",
        b"readback-mismatch-blocked",
        b"blocked",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 2,
    );
    assert!(matches!(
        service.commit_metadata(blocked),
        Err(MetadError::Codec(message)) if message.contains("result mismatch")
    ));
    assert!(service
        .metadata
        .get(
            RecordFamily::System,
            b"readback-mismatch-blocked",
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());

    store.clear_readback_mismatches();
    let trigger = ordered_log_put_command(
        b"req-readback-mismatch-trigger",
        b"readback-mismatch-trigger",
        b"trigger",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 3,
    );
    service.commit_metadata(trigger).unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
}

#[test]
fn batch_post_commit_backend_readback_archives_full_batch_in_input_order() {
    let store = PostCommitErrorStore::new_disarmed(CommandKind::RegisterNamespaceIndex);
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), store.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-batch-readback", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/batch-readback-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let commands = vec![
        ordered_log_put_command(
            b"req-batch-readback-first",
            b"batch-readback-first",
            b"first",
            checkpoint.commit_version,
            checkpoint.commit_version + 1,
        ),
        ordered_log_put_command(
            b"req-batch-readback-second",
            b"batch-readback-second",
            b"second",
            checkpoint.commit_version,
            checkpoint.commit_version + 2,
        ),
    ];
    store.fail_next_batch_results(vec![0, 1]);
    let results = service.commit_independent_metadata_batch(&commands);
    assert!(results.iter().all(Result::is_ok));

    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
    assert_eq!(snapshot.segments.len(), 1);
    let segment = service
        .load_metadata_log_segment(&snapshot.segments[0].segment_key)
        .unwrap();
    assert_eq!(
        segment
            .entries
            .iter()
            .map(|entry| entry.command.request_id.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"req-batch-readback-first".as_slice(),
            b"req-batch-readback-second".as_slice()
        ]
    );

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    let outcome = recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &snapshot_segment_keys(&snapshot),
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();
    assert_eq!(outcome.replayed_entries, 2);
    assert_eq!(
        recovered
            .metadata
            .get(
                RecordFamily::System,
                b"batch-readback-second",
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"second".to_vec()))
    );
}

#[test]
fn unresolved_early_batch_subgroup_freezes_later_success_and_mixed_failure() {
    let store = PostCommitErrorStore::new_disarmed(CommandKind::RegisterNamespaceIndex);
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), store.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-subgroup-readback", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/subgroup-readback-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    let first = ordered_log_put_command(
        b"req-subgroup-first",
        b"subgroup-shared-key",
        b"first",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    // Same key forces Holt to flush the first atomic subgroup before planning
    // and applying this later successful subgroup.
    let second = ordered_log_put_command(
        b"req-subgroup-second",
        b"subgroup-shared-key",
        b"second",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 2,
    );
    let mut rejected = ordered_log_put_command(
        b"req-subgroup-rejected",
        b"subgroup-rejected-key",
        b"rejected",
        checkpoint.commit_version,
        checkpoint.commit_version + 3,
    );
    rejected.predicates.push(PredicateRef {
        family: RecordFamily::System,
        key: b"missing-subgroup-predicate-key".to_vec(),
        predicate: Predicate::Exists,
    });
    store.fail_next_batch_results(vec![0]);
    store.fail_next_readbacks(1);
    let results =
        service.commit_independent_metadata_batch(&[first.clone(), second.clone(), rejected]);
    assert!(results
        .iter()
        .all(|result| matches!(result, Err(MetadError::Codec(message)) if message.contains("readback failed"))));
    assert_eq!(service.sync_metadata_log_snapshot().unwrap().durable_lsn, 0);
    assert_eq!(
        service
            .metadata
            .get(
                RecordFamily::System,
                b"subgroup-shared-key",
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"second".to_vec())),
        "the later subgroup applied but must not be acknowledged or archived early"
    );

    store.clear_readback_failures();
    let trigger = ordered_log_put_command(
        b"req-subgroup-trigger",
        b"subgroup-trigger-key",
        b"trigger",
        checkpoint.commit_version + 2,
        checkpoint.commit_version + 4,
    );
    service.commit_metadata(trigger).unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 3);
    assert_eq!(snapshot.segments.len(), 2);
    let resolved_group = service
        .load_metadata_log_segment(&snapshot.segments[0].segment_key)
        .unwrap();
    assert_eq!(
        resolved_group
            .entries
            .iter()
            .map(|entry| entry.command.request_id.as_slice())
            .collect::<Vec<_>>(),
        vec![first.request_id.as_slice(), second.request_id.as_slice()],
        "the true committed subset must archive once in original execution order"
    );
}

use nokv_object::{ObjectInfo, ObjectRange};
use std::sync::atomic::AtomicUsize;

/// An [`ObjectStore`] wrapper that injects PUT failures to simulate a crash at a
/// chosen point (e.g. after the checkpoint object is written but before the
/// `CURRENT` pointer is swapped). Reads and deletes always pass through.
#[derive(Clone)]
struct FaultObjectStore {
    inner: MemoryObjectStore,
    fail_put_substring: Arc<Mutex<Option<String>>>,
    injected_put_failures: Arc<AtomicUsize>,
    get_keys: Arc<Mutex<Vec<String>>>,
}

impl FaultObjectStore {
    fn new(inner: MemoryObjectStore) -> Self {
        Self {
            inner,
            fail_put_substring: Arc::new(Mutex::new(None)),
            injected_put_failures: Arc::new(AtomicUsize::new(0)),
            get_keys: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn fail_puts_containing(&self, substring: &str) {
        *self.fail_put_substring.lock().unwrap() = Some(substring.to_owned());
    }

    fn clear_faults(&self) {
        *self.fail_put_substring.lock().unwrap() = None;
    }

    fn injected_put_failures(&self) -> usize {
        self.injected_put_failures.load(Ordering::Relaxed)
    }

    fn clear_get_keys(&self) {
        self.get_keys.lock().unwrap().clear();
    }

    fn got_key(&self, key: &str) -> bool {
        self.get_keys.lock().unwrap().iter().any(|got| got == key)
    }
}

/// Records successful physical PUTs and can report one of them as failed only
/// after the bytes reached the backing store. This models process loss at the
/// restore initialization PUT-after barrier without changing production code.
#[derive(Clone)]
struct RestorePostPutFaultStore {
    inner: MemoryObjectStore,
    state: Arc<Mutex<RestorePostPutFaultState>>,
}

#[derive(Default)]
struct RestorePostPutFaultState {
    fail_after_matches: Option<usize>,
    put_keys: Vec<ObjectKey>,
}

impl RestorePostPutFaultStore {
    fn new(inner: MemoryObjectStore) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(RestorePostPutFaultState::default())),
        }
    }

    fn fail_after_put(&self, zero_based_match: usize) {
        self.state.lock().unwrap().fail_after_matches = Some(zero_based_match);
    }

    fn clear_put_history(&self) {
        self.state.lock().unwrap().put_keys.clear();
    }

    fn put_keys(&self) -> Vec<ObjectKey> {
        self.state.lock().unwrap().put_keys.clone()
    }
}

impl ObjectStore for RestorePostPutFaultStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let info = self.inner.put(key, bytes)?;
        let should_fail = {
            let mut state = self.state.lock().unwrap();
            state.put_keys.push(key.clone());
            match state.fail_after_matches.as_mut() {
                Some(remaining) if *remaining == 0 => {
                    state.fail_after_matches = None;
                    true
                }
                Some(remaining) => {
                    *remaining -= 1;
                    false
                }
                None => false,
            }
        };
        if should_fail {
            return Err(ObjectError::Backend(
                "injected restore crash after object PUT".to_owned(),
            ));
        }
        Ok(info)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

impl ObjectStore for FaultObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        if let Some(substring) = self.fail_put_substring.lock().unwrap().clone() {
            if key.as_str().contains(&substring) {
                self.injected_put_failures.fetch_add(1, Ordering::Relaxed);
                return Err(ObjectError::Backend("injected put fault".to_owned()));
            }
        }
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.get_keys.lock().unwrap().push(key.as_str().to_owned());
        self.inner.get(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

#[test]
fn post_commit_backend_with_archive_failure_retains_exact_pending_segment() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing);
    let store = PostCommitErrorStore::new_disarmed(CommandKind::RegisterNamespaceIndex);
    let service = NoKvFs::new(MountId::new(1).unwrap(), store.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-backend-archive-failure", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/backend-archive-failure-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();

    objects.fail_puts_containing("meta/backend-archive-failure-log/log/");
    store.arm();
    let first = ordered_log_put_command(
        b"req-backend-archive-first",
        b"backend-archive-first",
        b"first",
        checkpoint.commit_version,
        checkpoint.commit_version + 1,
    );
    assert!(matches!(
        service.commit_metadata(first),
        Err(MetadError::SyncLogArchiveFailed {
            committed: true,
            ..
        })
    ));
    assert_eq!(service.sync_metadata_log_snapshot().unwrap().durable_lsn, 0);

    objects.clear_faults();
    let second = ordered_log_put_command(
        b"req-backend-archive-second",
        b"backend-archive-second",
        b"second",
        checkpoint.commit_version + 1,
        checkpoint.commit_version + 2,
    );
    service.commit_metadata(second).unwrap();
    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 2);
    let request_ids = snapshot
        .segments
        .iter()
        .map(|pointer| {
            service
                .load_metadata_log_segment(&pointer.segment_key)
                .unwrap()
        })
        .flat_map(|segment| segment.entries.into_iter())
        .map(|entry| entry.command.request_id)
        .collect::<Vec<_>>();
    assert_eq!(
        request_ids,
        vec![
            b"req-backend-archive-first".to_vec(),
            b"req-backend-archive-second".to_vec()
        ]
    );
}

#[test]
fn publish_remains_readable_when_sync_log_ack_fails_after_metadata_commit() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing.clone());
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/sync-log-ack",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    objects.fail_puts_containing("meta/sync-log-ack/log/");

    let name = dname(b"committed.bin");
    let payload = b"metadata committed before the archive acknowledgement";
    let error = service
        .publish_artifact(artifact_request(name.clone(), "committed", payload))
        .unwrap_err();
    let staged = match error {
        MetadError::PublishArtifactFailed { source, staged } => {
            assert!(matches!(
                *source,
                MetadError::SyncLogArchiveFailed {
                    committed: true,
                    ..
                }
            ));
            staged
        }
        other => panic!("unexpected publish error: {other:?}"),
    };

    let committed = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .expect("the metadata transaction committed");
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        payload
    );
    assert_eq!(staged.len(), 1);
    assert_eq!(
        staged.objects()[0].key,
        block_key(
            committed.attr.inode,
            committed.body.as_ref().unwrap().generation,
            0,
            0,
        )
    );
    assert!(backing.head(&staged.objects()[0].key).unwrap().is_some());
}

#[test]
fn checkpoint_keeps_referenced_blocks_after_committed_sync_log_failure() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing.clone());
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/checkpoint-sync-log-ack",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    objects.fail_puts_containing("meta/checkpoint-sync-log-ack/log/");

    let payload = b"checkpoint shard remains reachable after log ACK failure".to_vec();
    let error = service
        .publish_checkpoint(
            InodeId::root(),
            vec![CheckpointShard {
                name: dname(b"rank-0.ckpt"),
                bytes: payload.clone(),
            }],
            1000,
            1000,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::SyncLogArchiveFailed {
            committed: true,
            ..
        }
    ));

    let committed = service
        .lookup_path("/rank-0.ckpt")
        .unwrap()
        .expect("checkpoint metadata was atomically committed");
    assert_eq!(
        service
            .read_artifact(InodeId::root(), &dname(b"rank-0.ckpt"))
            .unwrap(),
        payload
    );
    let body = committed.body.expect("checkpoint has a staged object body");
    let object = block_key(committed.attr.inode, body.generation, 0, 0);
    assert!(backing.head(&object).unwrap().is_some());
}

#[test]
fn checkpoint_keeps_blocks_after_uncertain_backend_ack_failure() {
    let metadata = PostCommitErrorStore::new(CommandKind::PublishArtifact);
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let payload = b"checkpoint visible despite a lost backend acknowledgement".to_vec();

    let error = service
        .publish_checkpoint(
            InodeId::root(),
            vec![CheckpointShard {
                name: dname(b"uncertain-rank.ckpt"),
                bytes: payload.clone(),
            }],
            1000,
            1000,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::Metadata(MetadataError::Backend(message))
            if message.contains("journal acknowledgement")
    ));

    let committed = service
        .lookup_path("/uncertain-rank.ckpt")
        .unwrap()
        .expect("the injected backend applied before losing its ACK");
    assert_eq!(
        service
            .read_artifact(InodeId::root(), &dname(b"uncertain-rank.ckpt"))
            .unwrap(),
        payload
    );
    let body = committed.body.expect("checkpoint has an object body");
    assert!(objects
        .head(&block_key(committed.attr.inode, body.generation, 0, 0,))
        .unwrap()
        .is_some());
}

#[test]
fn pending_sync_log_blocks_single_and_batch_apply_until_prior_commit_is_archived() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing);
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/batch", 0o755, 1000, 1000)
        .unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/ck-pending-sync-log", 2);
    service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/pending-sync-log",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    objects.fail_puts_containing("meta/pending-sync-log/log/");

    let first_error = service
        .create_dir_path("/first", 0o755, 1000, 1000)
        .unwrap_err();
    assert!(matches!(
        first_error,
        MetadError::SyncLogArchiveFailed {
            committed: true,
            ..
        }
    ));
    assert!(service.lookup_path("/first").unwrap().is_some());

    let blocked_single = service
        .create_dir_path("/second", 0o755, 1000, 1000)
        .unwrap_err();
    assert!(matches!(
        blocked_single,
        MetadError::SyncLogArchiveFailed {
            committed: false,
            ..
        }
    ));
    assert!(service.lookup_path("/second").unwrap().is_none());

    let blocked = service.create_file_batches_in_dir_path(vec![CreateInDirPathBatch {
        parent_path: "/batch".to_owned(),
        names: vec![dname(b"third.bin")],
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }]);
    assert!(matches!(
        &blocked[0],
        Err(MetadError::SyncLogArchiveFailed {
            committed: false,
            ..
        })
    ));
    assert!(service.lookup_path("/batch/third.bin").unwrap().is_none());
    assert_eq!(service.sync_metadata_log_snapshot().unwrap().durable_lsn, 0);

    objects.clear_faults();
    service
        .create_dir_path("/second", 0o755, 1000, 1000)
        .unwrap();
    let retried = service.create_file_batches_in_dir_path(vec![CreateInDirPathBatch {
        parent_path: "/batch".to_owned(),
        names: vec![dname(b"third.bin")],
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }]);
    assert_eq!(retried[0].as_ref().unwrap().len(), 1);

    let snapshot = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(snapshot.durable_lsn, 3);
    assert_eq!(snapshot.segments.len(), 3);
    let first_segment = service
        .load_metadata_log_segment(&snapshot.segments[0].segment_key)
        .unwrap();
    let second_segment = service
        .load_metadata_log_segment(&snapshot.segments[1].segment_key)
        .unwrap();
    let third_segment = service
        .load_metadata_log_segment(&snapshot.segments[2].segment_key)
        .unwrap();
    assert_eq!((first_segment.first_lsn, first_segment.last_lsn), (1, 1));
    assert_eq!((second_segment.first_lsn, second_segment.last_lsn), (2, 2));
    assert_eq!(first_segment.last_digest, second_segment.prev_digest);
    assert_eq!((third_segment.first_lsn, third_segment.last_lsn), (3, 3));
    assert_eq!(second_segment.last_digest, third_segment.prev_digest);

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    let outcome = recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &snapshot_segment_keys(&snapshot),
            0,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap()
        .unwrap();
    assert_eq!(outcome.replayed_entries, 3);
    assert!(recovered.lookup_path("/first").unwrap().is_some());
    assert!(recovered.lookup_path("/second").unwrap().is_some());
    assert!(recovered.lookup_path("/batch/third.bin").unwrap().is_some());
}

#[test]
fn immutable_checkpoint_restores_only_its_exact_control_identity() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing.clone());
    let source = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    source.bootstrap_root(0o755, 1000, 1000).unwrap();
    source
        .create_dir_path("/controlled", 0o755, 1000, 1000)
        .unwrap();
    let config = MetadataArchiveConfig::new("meta/exact", 4);
    let prepared = source.prepare_immutable_metadata_backup(&config).unwrap();
    let identity = MetadataCheckpointIdentity {
        checkpoint_key: prepared.checkpoint_key.clone(),
        image_bytes: prepared.image_bytes,
        image_digest: prepared.image_digest.clone(),
    };
    assert!(backing
        .head(&ObjectKey::new("meta/exact/CURRENT").unwrap())
        .unwrap()
        .is_none());

    // Drift standalone CURRENT to a different image. Controlled restore must
    // still install only the exact control identity above.
    source.create_dir_path("/drift", 0o755, 1000, 1000).unwrap();
    source.backup_metadata(&config).unwrap();
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    let restored = recovered
        .restore_metadata_checkpoint(&config, &identity)
        .unwrap();
    assert_eq!(restored.checkpoint_key, prepared.checkpoint_key);
    assert!(recovered.lookup_path("/controlled").unwrap().is_some());
    assert!(recovered.lookup_path("/drift").unwrap().is_none());
}

#[test]
fn exact_checkpoint_rejects_missing_proof_before_fetching_image() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing.clone());
    let source = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    source.bootstrap_root(0o755, 1000, 1000).unwrap();
    let config = MetadataArchiveConfig::new("meta/proof", 4);
    let prepared = source.prepare_immutable_metadata_backup(&config).unwrap();
    let identity = MetadataCheckpointIdentity {
        checkpoint_key: prepared.checkpoint_key.clone(),
        image_bytes: prepared.image_bytes,
        image_digest: prepared.image_digest,
    };
    let proof_key = ObjectKey::new(format!("{}.proof", identity.checkpoint_key)).unwrap();
    assert!(backing.delete(&proof_key).unwrap());
    objects.clear_get_keys();

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    assert!(matches!(
        recovered.restore_metadata_checkpoint(&config, &identity),
        Err(MetadError::MetadataArchiveMissingObjectGcFence { checkpoint_key })
            if checkpoint_key == identity.checkpoint_key
    ));
    assert!(!objects.got_key(&identity.checkpoint_key));
    assert!(recovered.get_attr(InodeId::root()).unwrap().is_none());
}

#[test]
fn exact_checkpoint_rejects_cross_prefix_or_same_key_digest_mismatch() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing);
    let source = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    source.bootstrap_root(0o755, 1000, 1000).unwrap();
    let config = MetadataArchiveConfig::new("meta/owned", 4);
    let prepared = source.prepare_immutable_metadata_backup(&config).unwrap();
    let identity = MetadataCheckpointIdentity {
        checkpoint_key: prepared.checkpoint_key.clone(),
        image_bytes: prepared.image_bytes,
        image_digest: prepared.image_digest,
    };
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    assert!(matches!(
        recovered.restore_metadata_checkpoint(
            &MetadataArchiveConfig::new("meta/other", 4),
            &identity,
        ),
        Err(MetadError::Codec(message)) if message.contains("does not match archive prefix")
    ));

    let mismatched = MetadataCheckpointIdentity {
        checkpoint_key: identity.checkpoint_key.clone(),
        image_bytes: identity.image_bytes,
        image_digest: format!("sha256:{}", "0".repeat(64)),
    };
    objects.clear_get_keys();
    assert!(matches!(
        recovered.restore_metadata_checkpoint(&config, &mismatched),
        Err(MetadError::Codec(message)) if message.contains("does not match archive prefix")
    ));
    assert!(!objects.got_key(&identity.checkpoint_key));
}

#[test]
fn exact_checkpoint_rejects_tampered_same_key_image_before_install() {
    let backing = MemoryObjectStore::new();
    let source = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        backing.clone(),
    );
    source.bootstrap_root(0o755, 1000, 1000).unwrap();
    source
        .create_dir_path("/must-not-install", 0o755, 1000, 1000)
        .unwrap();
    let config = MetadataArchiveConfig::new("meta/tamper", 4);
    let prepared = source.prepare_immutable_metadata_backup(&config).unwrap();
    let identity = MetadataCheckpointIdentity {
        checkpoint_key: prepared.checkpoint_key.clone(),
        image_bytes: prepared.image_bytes,
        image_digest: prepared.image_digest,
    };
    let image_key = ObjectKey::new(identity.checkpoint_key.clone()).unwrap();
    let mut tampered = backing.get(&image_key, None).unwrap();
    tampered[0] ^= 0xff;
    assert_eq!(tampered.len() as u64, identity.image_bytes);
    backing.put(&image_key, tampered).unwrap();

    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        backing,
    );
    assert!(matches!(
        recovered.restore_metadata_checkpoint(&config, &identity),
        Err(MetadError::Codec(message)) if message.contains("image digest mismatch")
    ));
    assert!(recovered.get_attr(InodeId::root()).unwrap().is_none());
}

#[test]
fn backup_archive_crash_between_checkpoint_and_pointer_is_consistent() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing.clone());
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "m-a", b"alpha");

    let config = MetadataArchiveConfig::new("meta/ck", 4);
    // First backup completes: CURRENT -> checkpoint #1 (captures only /runs/a.bin).
    let good = service.backup_metadata(&config).unwrap();

    // Add /runs/b.bin, then crash the second backup at the pointer swap: the
    // checkpoint object is written, but the CURRENT manifest PUT fails.
    publish_path_artifact(&service, "/runs/b.bin", "m-b", b"bravo");
    objects.fail_puts_containing("/CURRENT");
    let err = service.backup_metadata(&config).unwrap_err();
    assert!(matches!(err, MetadError::Object(_)));
    assert_eq!(objects.injected_put_failures(), 1);
    objects.clear_faults();

    // CURRENT still names the first, complete checkpoint — never the orphaned
    // second one. Restore into a fresh node recovers the pre-crash state.
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        backing.clone(),
    );
    let restored = recovered.restore_metadata(&config).unwrap().unwrap();
    assert_eq!(restored.checkpoint_key, good.checkpoint_key);
    assert!(recovered.lookup_path("/runs/a.bin").unwrap().is_some());
    assert!(
        recovered.lookup_path("/runs/b.bin").unwrap().is_none(),
        "restore must not expose the torn (uncommitted) checkpoint"
    );

    // With the fault cleared, the archive recovers forward cleanly.
    publish_path_artifact(&service, "/runs/c.bin", "m-c", b"charlie");
    let next = service.backup_metadata(&config).unwrap();
    assert_ne!(next.checkpoint_key, good.checkpoint_key);
}

#[test]
fn object_gc_converges_under_create_delete_churn() {
    let (service, objects) = service_with_objects();
    // Churn: create many small files; delete the even rounds (their blocks must
    // be reclaimed) and keep the odd rounds (their blocks must never be deleted).
    let mut live_keys = Vec::new();
    for round in 0..20u32 {
        let name = DentryName::new(format!("churn-{round}.bin").into_bytes()).unwrap();
        let published = service
            .publish_artifact(artifact_request(
                name.clone(),
                &format!("m{round}"),
                b"payload",
            ))
            .unwrap();
        let body = published.body.clone().unwrap();
        let key = block_key(published.attr.inode, body.generation, 0, 0);
        if round % 2 == 0 {
            service.remove_file(InodeId::root(), &name).unwrap();
        } else {
            live_keys.push(key);
        }
    }

    // Drive GC to convergence with a small per-iteration limit so the queue is
    // drained across several batches rather than one sweep.
    let mut total_deleted = 0;
    let mut guard = 0;
    loop {
        let outcome = service.cleanup_pending_objects(4).unwrap();
        total_deleted += outcome.deleted;
        if outcome.scanned == 0 {
            break;
        }
        guard += 1;
        assert!(guard < 1000, "object GC did not converge");
    }

    // Exactly the 10 deleted files were reclaimed, and the queue is now empty.
    assert_eq!(total_deleted, 10);
    assert_eq!(
        service.cleanup_pending_objects(100).unwrap(),
        PendingObjectCleanupOutcome::default()
    );
    // Every kept file's block survived: owns_block_object_key never over-deleted.
    for key in &live_keys {
        assert!(
            objects.head(key).unwrap().is_some(),
            "live block was wrongly GC'd: {}",
            key.as_str()
        );
    }
}

#[test]
fn fsck_detects_dangling_block_after_out_of_band_object_loss() {
    let (service, objects) = service_with_objects();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let a = publish_path_artifact(&service, "/runs/a.bin", "m-a", b"alpha-body");
    publish_path_artifact(&service, "/runs/b.bin", "m-b", b"bravo-body");

    // A healthy namespace has no dangling references.
    let clean = service.fsck_dangling_blocks(0).unwrap();
    assert!(
        clean.is_consistent(),
        "unexpected dangling: {:?}",
        clean.dangling
    );
    assert_eq!(clean.files_scanned, 2);
    assert!(clean.blocks_checked >= 2);

    // Delete one file's backing object out-of-band: drift that object-first
    // ordering cannot prevent once the metadata is already committed.
    let body = a.body.clone().unwrap();
    let lost = block_key(a.attr.inode, body.generation, 0, 0);
    assert!(objects.delete(&lost).unwrap());

    // fsck flags exactly that reference, and nothing else.
    let report = service.fsck_dangling_blocks(0).unwrap();
    assert!(!report.is_consistent());
    assert_eq!(report.dangling.len(), 1);
    assert_eq!(report.dangling[0].inode, a.attr.inode.get());
    assert_eq!(report.dangling[0].object_key, lost.as_str());
}

/// Set up `/runs/a.bin`, snapshot `/runs` with `lease_ms`, then free the block so
/// it is GC-enqueued *after* the snapshot's read version (i.e. protected while
/// the pin is live). Returns the freed block's object key.
fn snapshot_then_free_block(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    lease_ms: u64,
) -> (SnapshotPin, ObjectKey) {
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let published = publish_path_artifact(service, "/runs/a.bin", "m-a", b"payload");
    let body = published.body.clone().unwrap();
    let block = block_key(published.attr.inode, body.generation, 0, 0);
    let runs = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(runs, lease_ms).unwrap();
    service.remove_file_path("/runs/a.bin").unwrap();
    (pin, block)
}

#[test]
fn expired_snapshot_pin_does_not_block_object_gc() {
    let (service, objects) = service_with_objects();
    // Lease of 0 ms: the pin is expired the moment GC inspects it.
    let (_pin, block) = snapshot_then_free_block(&service, 0);
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.blocked_by_snapshots, 0);
    assert_eq!(cleanup.deleted, 1);
    assert!(objects.head(&block).unwrap().is_none());
}

#[test]
fn live_snapshot_pin_blocks_object_gc_until_retired() {
    let (service, objects) = service_with_objects();
    let (pin, block) = snapshot_then_free_block(&service, 3_600_000);

    // A live pin protects the freed block.
    let blocked = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(blocked.blocked_by_snapshots, 1);
    assert_eq!(blocked.deleted, 0);
    assert!(objects.head(&block).unwrap().is_some());

    // Retiring it releases the protection.
    assert!(service.retire_snapshot(pin.snapshot_id).unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert!(objects.head(&block).unwrap().is_none());
}

#[test]
fn renew_snapshot_rejects_expiry_at_the_deadline() {
    let (service, _objects) = service_with_objects();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let runs = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(runs, 500).unwrap();

    service.set_clock_override_ms(1_500);
    assert!(matches!(
        service.renew_snapshot(pin.snapshot_id, 3_600_000),
        Err(MetadError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms: 1_500,
            now_ms: 1_500,
        }) if snapshot_id == pin.snapshot_id
    ));

    assert_eq!(
        service
            .renew_snapshot(pin.snapshot_id + 9_999, 1_000)
            .unwrap(),
        SnapshotRenewOutcome::Missing {
            snapshot_id: pin.snapshot_id + 9_999,
        }
    );
}

#[test]
fn renew_snapshot_cannot_revive_a_pin_after_gc_crosses_its_expiry() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RenewSnapshot, 1, 2)
        .rejecting(CommandKind::RetireSnapshot);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let start_ms = service.now_ms();
    service.set_clock_override_ms(start_ms);
    let name = DentryName::new(b"renew-gc-race.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "renew-gc-race", b"payload"))
        .unwrap();
    let body = published.body.as_ref().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    let pin = service
        .snapshot_subtree_with_lease(InodeId::root(), 500)
        .unwrap();
    let snapshot_id = pin.snapshot_id;
    service.remove_file(InodeId::root(), &name).unwrap();

    let renew_service = Arc::clone(&service);
    let renew = std::thread::spawn(move || renew_service.renew_snapshot(snapshot_id, 10_000));
    store.wait_until_blocked();

    let deadline_ms = start_ms + 500;
    service.set_clock_override_ms(deadline_ms);
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.snapshot_reap.conflicted, 1);
    assert_eq!(cleanup.deleted, 1, "cleanup outcome: {cleanup:?}");
    assert!(objects.head(&object).unwrap().is_none());
    store.release_blocked();

    assert!(matches!(
        renew.join().unwrap(),
        Err(MetadError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms,
            now_ms,
        }) if snapshot_id == pin.snapshot_id
            && lease_expires_unix_ms == deadline_ms
            && now_ms == deadline_ms
    ));
    assert_eq!(
        service
            .snapshot_pin(pin.snapshot_id)
            .unwrap()
            .unwrap()
            .lease_expires_unix_ms,
        deadline_ms
    );
}

#[test]
fn renew_snapshot_is_extend_only_and_never_shortens_protection() {
    let (service, _objects) = service_with_objects();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let runs = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(runs, 1_000).unwrap();

    // Grant a long lease.
    let SnapshotRenewOutcome::Renewed {
        pin: long,
        extended: true,
    } = service.renew_snapshot(pin.snapshot_id, 3_600_000).unwrap()
    else {
        panic!("expected an extended live pin")
    };

    // A shorter renew must NOT shorten the protection already granted: renew is
    // extend-only (Iceberg / S3 Object Lock semantics). Shrinking protection is
    // expressed by `retire`, never by a shorter renew silently dropping it.
    let SnapshotRenewOutcome::Renewed {
        pin: after_short,
        extended: false,
    } = service.renew_snapshot(pin.snapshot_id, 1_000).unwrap()
    else {
        panic!("expected an unchanged live pin")
    };
    assert_eq!(
        after_short.lease_expires_unix_ms, long.lease_expires_unix_ms,
        "a shorter renew must never shorten protection"
    );

    // A longer renew still extends protection.
    let SnapshotRenewOutcome::Renewed {
        pin: after_long,
        extended: true,
    } = service.renew_snapshot(pin.snapshot_id, 7_200_000).unwrap()
    else {
        panic!("expected an extended live pin")
    };
    assert!(
        after_long.lease_expires_unix_ms > long.lease_expires_unix_ms,
        "a longer renew extends protection"
    );
}

#[test]
fn concurrent_snapshot_renewals_preserve_the_longest_successful_lease() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RenewSnapshot, 2, 2);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(MountId::new(1).unwrap(), store, objects));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let root = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(root, 1_000).unwrap();

    let short_service = Arc::clone(&service);
    let short = std::thread::spawn(move || short_service.renew_snapshot(pin.snapshot_id, 5_000));
    let long_service = Arc::clone(&service);
    let long = std::thread::spawn(move || long_service.renew_snapshot(pin.snapshot_id, 10_000));

    let short = short.join().unwrap().unwrap();
    let long = long.join().unwrap().unwrap();
    assert!(matches!(short, SnapshotRenewOutcome::Renewed { .. }));
    assert!(matches!(long, SnapshotRenewOutcome::Renewed { .. }));
    assert_eq!(
        service
            .snapshot_pin(pin.snapshot_id)
            .unwrap()
            .unwrap()
            .lease_expires_unix_ms,
        11_000
    );
}

#[test]
fn sixteen_concurrent_snapshot_renewals_converge_on_the_longest_lease() {
    const WRITERS: usize = 16;
    let store =
        SnapshotCommitBarrierStore::new(CommandKind::RenewSnapshot, WRITERS as u64, WRITERS);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store,
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let root = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(root, 1_000).unwrap();

    let mut writers = Vec::new();
    for index in 0..WRITERS {
        let service = Arc::clone(&service);
        let lease_ms = 2_000 + index as u64 * 1_000;
        writers.push(std::thread::spawn(move || {
            service.renew_snapshot(pin.snapshot_id, lease_ms)
        }));
    }
    for writer in writers {
        assert!(matches!(
            writer.join().unwrap().unwrap(),
            SnapshotRenewOutcome::Renewed { .. }
        ));
    }
    assert_eq!(
        service
            .snapshot_pin(pin.snapshot_id)
            .unwrap()
            .unwrap()
            .lease_expires_unix_ms,
        18_000
    );
}

#[test]
fn stale_reaper_scan_cannot_delete_a_newer_pin_version() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RetireSnapshot, 1, 2);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects,
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let root = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(root, 500).unwrap();

    service.set_clock_override_ms(1_500);
    let reaper_service = Arc::clone(&service);
    let reaper = std::thread::spawn(move || reaper_service.reclaim_expired_snapshot_pins(100));
    store.wait_until_blocked();

    // A deterministic clock rewind models a stale reaper candidate whose record
    // was replaced before its delete applied. The version fence, not wall time,
    // is the invariant under test.
    service.set_clock_override_ms(1_400);
    assert!(matches!(
        service.renew_snapshot(pin.snapshot_id, 10_000).unwrap(),
        SnapshotRenewOutcome::Renewed { extended: true, .. }
    ));
    store.release_blocked();

    let outcome = reaper.join().unwrap().unwrap();
    assert_eq!(outcome.expired_candidates, 1);
    assert_eq!(outcome.reaped, 0);
    assert_eq!(outcome.conflicted, 1);
    assert!(service.snapshot_pin(pin.snapshot_id).unwrap().is_some());
}

#[test]
fn one_reaper_conflict_does_not_block_other_expired_pins() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RetireSnapshot, 1, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/b", 0o755, 1000, 1000).unwrap();
    let a = service.resolve_directory_path("/a").unwrap();
    let b = service.resolve_directory_path("/b").unwrap();
    let renewed = service.snapshot_subtree_with_lease(a, 500).unwrap();
    let expired = service.snapshot_subtree_with_lease(b, 500).unwrap();

    service.set_clock_override_ms(1_500);
    let reaper_service = Arc::clone(&service);
    let reaper = std::thread::spawn(move || reaper_service.reclaim_expired_snapshot_pins(100));
    store.wait_until_blocked();
    service.set_clock_override_ms(1_400);
    assert!(matches!(
        service.renew_snapshot(renewed.snapshot_id, 10_000).unwrap(),
        SnapshotRenewOutcome::Renewed { extended: true, .. }
    ));
    store.release_blocked();

    let outcome = reaper.join().unwrap().unwrap();
    assert_eq!(outcome.expired_candidates, 2);
    assert_eq!(outcome.reaped, 1);
    assert_eq!(outcome.conflicted, 1);
    assert!(service.snapshot_pin(renewed.snapshot_id).unwrap().is_some());
    assert!(service.snapshot_pin(expired.snapshot_id).unwrap().is_none());
}

#[test]
fn uncontended_reaper_page_uses_one_atomic_commit() {
    let (service, _objects) = service_with_objects();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/b", 0o755, 1000, 1000).unwrap();
    let a = service.resolve_directory_path("/a").unwrap();
    let b = service.resolve_directory_path("/b").unwrap();
    service.snapshot_subtree_with_lease(a, 500).unwrap();
    service.snapshot_subtree_with_lease(b, 500).unwrap();
    service.set_clock_override_ms(1_500);

    let before = service.metadata_store_stats().commit_total;
    let outcome = service.reclaim_expired_snapshot_pins(100).unwrap();
    let commits = service.metadata_store_stats().commit_total - before;
    assert_eq!(outcome.expired_candidates, 2);
    assert_eq!(outcome.reaped, 2);
    assert_eq!(outcome.conflicted, 0);
    assert_eq!(commits, 1);
}

#[test]
fn snapshot_path_operations_reject_a_different_root() {
    let (service, _objects) = service_with_objects();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/b", 0o755, 1000, 1000).unwrap();
    let root = service.resolve_directory_path("/a").unwrap();
    let pin = service.snapshot_subtree_with_lease(root, 10_000).unwrap();

    assert!(matches!(
        service.stat_path_at_snapshot("/b", pin.snapshot_id, "/"),
        Err(MetadError::SnapshotRootMismatch {
            snapshot_id,
            expected_root,
            actual_root,
            ..
        }) if snapshot_id == pin.snapshot_id && expected_root != root && actual_root == Some(root)
    ));
    assert!(matches!(
        service.renew_snapshot_path("/b", pin.snapshot_id, 20_000),
        Err(MetadError::SnapshotRootMismatch { .. })
    ));

    service.set_clock_override_ms(pin.lease_expires_unix_ms);
    assert!(matches!(
        service.stat_path_at_snapshot("/a", pin.snapshot_id, "/"),
        Err(MetadError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms,
            now_ms,
        }) if snapshot_id == pin.snapshot_id
            && lease_expires_unix_ms == pin.lease_expires_unix_ms
            && now_ms == pin.lease_expires_unix_ms
    ));
}

#[test]
fn snapshot_component_reads_are_root_bound_even_when_empty_or_zero_length() {
    let (service, _objects) = service_with_objects();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/b", 0o755, 1000, 1000).unwrap();
    service
        .create_file_path("/a/inside", 0o644, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/b/outside", 0o644, 1000, 1000)
        .unwrap();
    let pin = service
        .snapshot_subtree_path_with_lease("/a", 10_000)
        .unwrap();
    let inside = DentryName::new("inside").unwrap();
    let outside = DentryName::new("outside").unwrap();

    assert!(service
        .get_attr_at_snapshot("/a", pin.snapshot_id, std::slice::from_ref(&inside))
        .unwrap()
        .is_some());
    assert!(service
        .get_attr_at_snapshot("/a", pin.snapshot_id, std::slice::from_ref(&outside))
        .unwrap()
        .is_none());
    assert!(matches!(
        service.get_attr_at_snapshot("/b", pin.snapshot_id, &[]),
        Err(MetadError::SnapshotRootMismatch { .. })
    ));
    assert!(matches!(
        service.read_file_at_snapshot("/b", pin.snapshot_id, std::slice::from_ref(&outside), 0, 0,),
        Err(MetadError::SnapshotRootMismatch { .. })
    ));
}

#[test]
fn snapshot_ids_are_shard_qualified_and_foreign_ids_fail_as_root_mismatch() {
    let source = service().with_shard_index(1);
    source
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let pin = source.snapshot_subtree_path("/source").unwrap();
    assert_eq!(pin.snapshot_id >> 48, 1);

    let destination = service().with_shard_index(2);
    destination
        .create_dir_path("/destination", 0o755, 1000, 1000)
        .unwrap();
    assert!(matches!(
        destination.stat_path_at_snapshot("/destination", pin.snapshot_id, "/"),
        Err(MetadError::SnapshotRootMismatch {
            snapshot_id,
            actual_root: None,
            actual_shard: 1,
            ..
        }) if snapshot_id == pin.snapshot_id
    ));
}

#[test]
fn restore_to_fork_reinitializes_a_missing_object_gc_claim_before_its_hold_cas() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/source/empty.txt", 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();

    // Model an upgraded metadata image whose root and snapshot predate the
    // durable object-GC claim record.
    delete_object_gc_claim(&service);

    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    assert_eq!(restored.state, RestoreState::Complete);
    assert_eq!(
        service
            .lookup_path("/destination/empty.txt")
            .unwrap()
            .unwrap()
            .attr
            .file_type,
        FileType::File
    );

    let retry = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    assert_eq!(retry, restored);
}

#[test]
fn restore_rejects_a_source_path_that_did_not_bind_the_pin_root_at_snapshot_version() {
    let metadata = RestorePostApplyFaultStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/source/item.bin", "source/item", b"payload");
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let source_root = service.resolve_directory_path("/source").unwrap();
    service.rename_path("/source", "/renamed").unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/renamed",
        snapshot.snapshot_id,
        "/destination",
    );

    assert!(matches!(
        service.restore_subtree_path_to_fork(
            "/renamed",
            snapshot.snapshot_id,
            "/destination",
        ),
        Err(MetadError::SnapshotRootMismatch {
            snapshot_id,
            expected_root,
            actual_root: None,
            ..
        }) if snapshot_id == snapshot.snapshot_id && expected_root == source_root
    ));
    assert_eq!(metadata.applied_with_prefix(b"restore-install-hold"), 0);
    assert!(service.lookup_path("/destination").unwrap().is_none());

    let version = service.read_version().unwrap();
    for key in [
        restore::restore_active_key(service.mount_id()),
        restore::restore_operation_key(service.mount_id(), &operation_digest),
        restore::restore_destination_claim_key(service.mount_id(), "/destination"),
    ] {
        assert!(service
            .metadata_store()
            .get(
                RecordFamily::System,
                &key,
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none());
    }
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::ForkBinding,
            prefix: Vec::new(),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
}

#[test]
fn restore_to_fork_records_success_failure_retry_and_latency_metrics() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();

    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let retry = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    assert_eq!(retry, restored);
    assert!(matches!(
        service.restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/source",),
        Err(MetadError::RestoreDestinationConflict { .. })
    ));

    let stats = service.metadata_service_stats();
    assert_eq!(stats.restore_to_fork_requests_total, 3);
    assert_eq!(stats.restore_to_fork_success_total, 2);
    assert_eq!(stats.restore_to_fork_failure_total, 1);
    assert!(stats.restore_to_fork_elapsed_ns_total >= stats.restore_to_fork_elapsed_ns_max);
}

#[test]
fn restore_to_fork_applies_initialization_while_the_destination_is_detached() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/metadata/checkpoints.jsonl",
        "source-checkpoints",
        b"inherited\n",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let manifest = br#"{"schema":"nokv.workbench.restore_manifest.v1"}"#.to_vec();

    service
        .restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            RestoreInitialization {
                remove_relative_paths: vec!["metadata/checkpoints.jsonl".to_owned()],
                files: vec![RestoreInitializationFile {
                    relative_path: "metadata/restore_manifest.json".to_owned(),
                    bytes: manifest.clone(),
                    content_type: "application/json".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap();

    assert!(service
        .lookup_path("/destination/metadata/checkpoints.jsonl")
        .unwrap()
        .is_none());
    let metadata = service
        .lookup_path("/destination/metadata")
        .unwrap()
        .unwrap();
    assert_eq!(
        service
            .read_artifact(
                metadata.attr.inode,
                &DentryName::new("restore_manifest.json").unwrap(),
            )
            .unwrap(),
        manifest
    );
}

#[test]
fn restore_resource_preflight_rejects_oversized_initialization_command_without_a_hold() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );

    // The canonical initialization is below its 8 MiB request limit, but the
    // content type is encoded in both the dentry projection and body summary,
    // making the single publish MetadataCommand exceed 8 MiB.
    let error = service
        .restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            RestoreInitialization {
                remove_relative_paths: Vec::new(),
                files: vec![RestoreInitializationFile {
                    relative_path: "metadata/restore_manifest.json".to_owned(),
                    bytes: Vec::new(),
                    content_type: "x".repeat(4_300_000),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::RestoreResourceLimit { resource, .. }
            if resource == "restore initialization publish bytes"
    ));

    let version = service.read_version().unwrap();
    for key in [
        restore::restore_active_key(service.mount_id()),
        restore::restore_operation_key(service.mount_id(), &operation_digest),
        restore::restore_destination_claim_key(service.mount_id(), "/destination"),
    ] {
        assert!(
            service
                .metadata_store()
                .get(
                    RecordFamily::System,
                    &key,
                    version,
                    ReadPurpose::WritePlanLocal,
                )
                .unwrap()
                .is_none(),
            "resource preflight must not leave durable restore state"
        );
    }
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::ForkBinding,
            prefix: Vec::new(),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
    assert!(service.lookup_path("/destination").unwrap().is_none());
}

#[test]
fn restore_custom_index_command_shape_is_rejected_before_the_durable_hold() {
    const OVERSIZED_VALUE_BYTES: usize = 3_000_000;

    let metadata = OversizedPathIndexStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/source/item.bin", "preflight/source", b"x");
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("budget.value"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: false,
                facetable: false,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/source/item.bin".to_owned(),
                values: vec![NamespaceIndexValue {
                    field: NamespaceFindField::new("budget.value"),
                    value: NamespacePredicateValue::String("small".to_owned()),
                }],
            }],
        })
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let row_key = path_index_row_key(service.mount_id(), "/source", "/source/item.bin");
    let row = service
        .metadata_store()
        .get_versioned(
            RecordFamily::PathIndex,
            &row_key,
            service.read_version().unwrap(),
            ReadPurpose::Snapshot,
        )
        .unwrap()
        .unwrap();
    metadata.install_row(ScanItem {
        key: row_key,
        value: Value(encode_path_index_row(&PathIndexRowRecord {
            path: "/source/item.bin".to_owned(),
            values: vec![(
                "budget.value".to_owned(),
                PathIndexValueRecord::String("v".repeat(OVERSIZED_VALUE_BYTES)),
            )],
        })),
        version: row.version,
    });
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );

    for _ in 0..2 {
        let error = service
            .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
            .unwrap_err();
        assert!(matches!(
            error,
            MetadError::RestoreResourceLimit {
                resource,
                limit: 8_388_608,
                actual,
            } if resource == "restore index materialization batch bytes" && actual > 8_388_608
        ));
    }
    assert_eq!(metadata.applied_with_prefix(b"restore-install-hold"), 0);
    assert_eq!(metadata.applied_with_prefix(b"restore-begin-discard"), 0);
    let version = service.read_version().unwrap();
    for key in [
        restore::restore_active_key(service.mount_id()),
        restore::restore_operation_key(service.mount_id(), &operation_digest),
        restore::restore_destination_claim_key(service.mount_id(), "/destination"),
    ] {
        assert!(service
            .metadata_store()
            .get(
                RecordFamily::System,
                &key,
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none());
    }
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::ForkBinding,
            prefix: Vec::new(),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
    assert!(service.lookup_path("/destination").unwrap().is_none());
}

#[test]
fn restore_canonical_index_command_shape_is_rejected_before_the_durable_hold() {
    const LARGE_BODY_FIELD_BYTES: usize = 4_193_000;

    let metadata = OversizedPathIndexStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let published = publish_path_artifact(
        &service,
        "/source/item.bin",
        "canonical-preflight/source",
        b"x",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let version = service.read_version().unwrap();
    let source = service.resolve_directory_path("/source").unwrap();
    let name = DentryName::new("item.bin").unwrap();
    let dentry_key = dentry_key(service.mount_id(), source, &name);
    let path_components = parse_absolute_path("/source/item.bin").unwrap();
    let path_key = path_index_key(service.mount_id(), &path_components);
    let generation = published.body.as_ref().unwrap().generation;
    let body_key = chunk_manifest_key(
        service.mount_id(),
        published.attr.inode,
        generation,
        BODY_SUMMARY_CHUNK_INDEX,
    );
    let dentry_version = service
        .metadata_store()
        .get_versioned(
            RecordFamily::Dentry,
            &dentry_key,
            version,
            ReadPurpose::Snapshot,
        )
        .unwrap()
        .unwrap()
        .version;
    let path_version = service
        .metadata_store()
        .get_versioned(
            RecordFamily::PathIndex,
            &path_key,
            version,
            ReadPurpose::Snapshot,
        )
        .unwrap()
        .unwrap()
        .version;
    let body_version = service
        .metadata_store()
        .get_versioned(
            RecordFamily::ChunkManifest,
            &body_key,
            version,
            ReadPurpose::Snapshot,
        )
        .unwrap()
        .unwrap()
        .version;
    let mut body = published.body.clone().unwrap();
    body.producer = "p".repeat(LARGE_BODY_FIELD_BYTES);
    let projection = DentryProjection {
        dentry: published.dentry,
        attr: published.attr,
        body: Some(body.clone()),
    };
    for (family, key, value, row_version) in [
        (
            RecordFamily::Dentry,
            dentry_key,
            encode_dentry_projection(&projection),
            dentry_version,
        ),
        (
            RecordFamily::PathIndex,
            path_key,
            encode_dentry_projection(&projection),
            path_version,
        ),
        (
            RecordFamily::ChunkManifest,
            body_key,
            encode_body_descriptor(&body),
            body_version,
        ),
    ] {
        metadata.install_override(
            family,
            ScanItem {
                key,
                value: Value(value),
                version: row_version,
            },
        );
    }
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );

    let error = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::RestoreResourceLimit {
            resource,
            limit: 8_388_608,
            actual,
        } if resource == "restore index materialization batch bytes" && actual > 8_388_608
    ));
    assert_eq!(metadata.applied_with_prefix(b"restore-install-hold"), 0);
    let version = service.read_version().unwrap();
    for key in [
        restore::restore_active_key(service.mount_id()),
        restore::restore_operation_key(service.mount_id(), &operation_digest),
        restore::restore_destination_claim_key(service.mount_id(), "/destination"),
    ] {
        assert!(service
            .metadata_store()
            .get(
                RecordFamily::System,
                &key,
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none());
    }
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::ForkBinding,
            prefix: Vec::new(),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
}

#[test]
fn restore_initialization_member_is_packed_into_canonical_index_preflight() {
    const SOURCE_FILES: usize = 62;
    const LARGE_CONTENT_TYPE_BYTES: usize = 4_190_000;

    let metadata = RestorePostApplyFaultStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    for index in 0..SOURCE_FILES {
        publish_path_artifact(
            &service,
            &format!("/source/item-{index:02}.bin"),
            &format!("canonical-init/{index:02}"),
            b"x",
        );
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );

    let error = service
        .restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            RestoreInitialization {
                remove_relative_paths: Vec::new(),
                files: vec![RestoreInitializationFile {
                    relative_path: "new.bin".to_owned(),
                    bytes: Vec::new(),
                    content_type: "x".repeat(LARGE_CONTENT_TYPE_BYTES),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::RestoreResourceLimit {
            resource,
            limit: 8_388_608,
            actual,
        } if resource == "restore index materialization batch bytes" && actual > 8_388_608
    ));
    assert_eq!(metadata.applied_with_prefix(b"restore-install-hold"), 0);
    let version = service.read_version().unwrap();
    for key in [
        restore::restore_active_key(service.mount_id()),
        restore::restore_operation_key(service.mount_id(), &operation_digest),
        restore::restore_destination_claim_key(service.mount_id(), "/destination"),
    ] {
        assert!(service
            .metadata_store()
            .get(
                RecordFamily::System,
                &key,
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none());
    }
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::ForkBinding,
            prefix: Vec::new(),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
}

#[test]
fn restore_large_initialization_projection_passes_individual_command_preflight() {
    const LARGE_CONTENT_TYPE_BYTES: usize = 4_190_000;

    let metadata = RestorePostApplyFaultStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    metadata.fail_after_apply(b"restore-install-hold", 0);

    let error = service
        .restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            RestoreInitialization {
                remove_relative_paths: Vec::new(),
                files: vec![RestoreInitializationFile {
                    relative_path: "new.bin".to_owned(),
                    bytes: Vec::new(),
                    content_type: "x".repeat(LARGE_CONTENT_TYPE_BYTES),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MetadError::Metadata(MetadataError::Backend(message))
            if message == "injected restore crash after durable apply"
    ));
    assert_eq!(metadata.applied_with_prefix(b"restore-install-hold"), 1);
}

#[test]
fn restore_indexes_preserve_historical_snapshot_across_publish_rename_delete_and_release() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/source/a.bin", "source/a", b"a-v1");
    publish_path_artifact(&service, "/source/b.bin", "source/b", b"b-v1");
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.kind"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: vec![
                NamespaceIndexRow {
                    path: "/source/a.bin".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.kind"),
                        value: NamespacePredicateValue::String("a".to_owned()),
                    }],
                },
                NamespaceIndexRow {
                    path: "/source/b.bin".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.kind"),
                        value: NamespacePredicateValue::String("b".to_owned()),
                    }],
                },
            ],
        })
        .unwrap();
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/restored")
        .unwrap();
    let (restored_operation, _) = load_completed_restore_operation_and_seal(
        &service,
        "/source",
        source_snapshot.snapshot_id,
        "/restored",
    );

    let restored_root = service.resolve_directory_path("/restored").unwrap();
    let indexed = service
        .list_indexed_path_page("/restored", None, 10)
        .unwrap();
    assert_eq!(
        indexed
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        vec![b"a.bin".to_vec(), b"b.bin".to_vec()]
    );
    let found = service
        .find_paths(NamespaceFindRequest {
            path: "/restored".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::new("run.kind"),
                op: NamespacePredicateOp::Eq,
                value: Some(NamespacePredicateValue::String("a".to_owned())),
            }],
            sort: Vec::new(),
            include: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(found.match_count, 1);
    assert_eq!(found.matches[0].path, "/restored/a.bin");

    let historical = service.snapshot_subtree_path("/restored").unwrap();
    republish_path_artifact(&service, restored_root, "a.bin", "restored/a-v2", b"a-v2");
    service
        .rename_path("/restored/a.bin", "/restored/renamed.bin")
        .unwrap();
    service.remove_file_path("/restored/b.bin").unwrap();

    service
        .restore_subtree_path_to_fork("/restored", historical.snapshot_id, "/historical")
        .unwrap();
    assert!(service.lookup_path("/historical/a.bin").unwrap().is_some());
    assert!(service.lookup_path("/historical/b.bin").unwrap().is_some());
    assert!(service
        .lookup_path("/historical/renamed.bin")
        .unwrap()
        .is_none());
    let found = service
        .find_paths(NamespaceFindRequest {
            path: "/historical".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::new("run.kind"),
                op: NamespacePredicateOp::Eq,
                value: Some(NamespacePredicateValue::String("b".to_owned())),
            }],
            sort: Vec::new(),
            include: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(found.match_count, 1);
    assert_eq!(found.matches[0].path, "/historical/b.bin");

    service.remove_file_path("/restored/renamed.bin").unwrap();
    service.remove_empty_dir_path("/restored").unwrap();
    assert!(service
        .restore_indexed_children_page(
            restored_root,
            None,
            10,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .entries
        .is_empty());
    let snapshot_version = Version::new(historical.read_version).unwrap();
    let historical_indexed = service
        .restore_indexed_children_page(
            restored_root,
            None,
            10,
            snapshot_version,
            ReadPurpose::Snapshot,
        )
        .unwrap();
    assert_eq!(historical_indexed.entries.len(), 2);
    let historical_custom = service
        .restore_custom_index_at_path(
            "/restored",
            restored_root,
            snapshot_version,
            ReadPurpose::Snapshot,
        )
        .unwrap()
        .unwrap();
    assert_eq!(historical_custom.rows.len(), 2);

    assert!(service.retire_snapshot(historical.snapshot_id).unwrap());
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
    for _ in 0..128 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    let metrics = service.restore_metrics().unwrap();
    assert_eq!(metrics.releasing, 0);
    for keyspace in ["index_source_member", "index_mvcc_source_member"] {
        let mut prefix = restore_index::restore_index_private_keyspaces(service.mount_id())
            .into_iter()
            .find_map(|(name, prefix)| (name == keyspace).then_some(prefix))
            .unwrap();
        prefix.extend_from_slice(&restored_operation.ref_set_id.to_be_bytes());
        assert!(
            service
                .metadata_store()
                .scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix,
                    start_after: None,
                    version: service.read_version().unwrap(),
                    limit: 1,
                    purpose: ReadPurpose::WritePlanLocal,
                })
                .unwrap()
                .is_empty(),
            "released restore left {keyspace} rows"
        );
    }
}

#[test]
fn restore_release_discovers_and_reclaims_post_attach_namespace() {
    let (service, objects) = service_with_objects();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/original.bin",
        "restore/original",
        b"original bytes",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());

    let dynamic_dir = service
        .create_dir_path("/destination/new", 0o755, 1000, 1000)
        .unwrap();
    let dynamic_file = publish_path_artifact(
        &service,
        "/destination/new/payload.bin",
        "restore/dynamic-payload",
        b"post-attach bytes",
    );
    let checkpoint = service
        .publish_checkpoint(
            dynamic_dir.attr.inode,
            vec![CheckpointShard {
                name: DentryName::new("checkpoint.bin").unwrap(),
                bytes: b"post-attach checkpoint bytes".to_vec(),
            }],
            1000,
            1000,
        )
        .unwrap();
    let checkpoint_file = service
        .lookup_path("/destination/new/checkpoint.bin")
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.shards[0].1, checkpoint_file.attr.inode);
    service
        .set_xattr(
            dynamic_file.attr.inode,
            b"user.restore-test",
            b"present".to_vec(),
            XattrSetMode::Create,
        )
        .unwrap();
    let dynamic_body = dynamic_file.body.as_ref().unwrap();
    let dynamic_object = block_key(dynamic_file.attr.inode, dynamic_body.generation, 0, 0);
    let checkpoint_body = checkpoint_file.body.as_ref().unwrap();
    let checkpoint_object = block_key(checkpoint_file.attr.inode, checkpoint_body.generation, 0, 0);
    assert!(objects.head(&dynamic_object).unwrap().is_some());
    assert!(objects.head(&checkpoint_object).unwrap().is_some());

    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        source_snapshot.snapshot_id,
        "/destination",
    );
    let operation = restore::decode_restore_operation(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_operation_key(service.mount_id(), &operation_digest),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap()
            .0,
    )
    .unwrap();
    for inode in [
        dynamic_dir.attr.inode,
        dynamic_file.attr.inode,
        checkpoint_file.attr.inode,
    ] {
        let member = service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_staging_member_key(
                    service.mount_id(),
                    operation.ref_set_id,
                    inode,
                ),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();
        let member = restore::decode_restore_staging_member(&member.0).unwrap();
        assert!(member.source_inode.is_none());
        assert!(!member.canonical_index_complete);
    }

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    assert!(service.restore_staging_slow_path_enabled());
    assert!(service
        .get_attr(restored.destination_root)
        .unwrap()
        .is_none());
    assert!(service.get_attr(dynamic_file.attr.inode).unwrap().is_none());
    for _ in 0..512 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    assert!(!service.restore_staging_slow_path_enabled());
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Inode,
            &inode_key(service.mount_id(), dynamic_dir.attr.inode),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Inode,
            &inode_key(service.mount_id(), dynamic_file.attr.inode),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Inode,
            &inode_key(service.mount_id(), checkpoint_file.attr.inode),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(service.mount_id(), dynamic_dir.attr.inode),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &path_index_key(
                service.mount_id(),
                &parse_absolute_path("/destination/new/payload.bin").unwrap(),
            ),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Xattr,
            prefix: xattr_prefix(service.mount_id(), dynamic_file.attr.inode),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::System,
            &restore::restore_staging_inode_key(service.mount_id(), dynamic_file.attr.inode,),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());

    for _ in 0..64 {
        service.cleanup_pending_objects(64).unwrap();
        if objects.head(&dynamic_object).unwrap().is_none()
            && objects.head(&checkpoint_object).unwrap().is_none()
        {
            break;
        }
    }
    assert!(objects.head(&dynamic_object).unwrap().is_none());
    assert!(objects.head(&checkpoint_object).unwrap().is_none());
    assert_ne!(restored.destination_root, dynamic_dir.attr.inode);
}

#[test]
fn restore_release_pages_fragmented_manifest_across_byte_budget_ack_loss_and_reopen() {
    const BLOCK_COUNT: usize = 4_200;
    const ITEM_BUDGET_BLOCK_CAP: u64 = 4_087;

    let metadata = OversizedManifestStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let source_file = publish_path_artifact(
        &service,
        "/source/fragments.bin",
        "restore/fragmented-release",
        b"source",
    );
    let source_body = source_file.body.as_ref().unwrap();
    let source_object = block_key(source_file.attr.inode, source_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    let destination_file = service
        .lookup_path("/destination/fragments.bin")
        .unwrap()
        .unwrap();
    let generation = destination_file.body.as_ref().unwrap().generation;
    let manifest_key = chunk_manifest_key(
        service.mount_id(),
        destination_file.attr.inode,
        generation,
        0,
    );
    let manifest_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::ChunkManifest,
            &manifest_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let long_digest = format!("sha256:{}", "a".repeat(2_300));
    let mut object_keys = Vec::with_capacity(BLOCK_COUNT);
    let mut blocks = Vec::with_capacity(BLOCK_COUNT);
    for index in 0..BLOCK_COUNT {
        let object_key = block_key(destination_file.attr.inode, generation, 0, index as u64);
        let bytes = if index == 0 {
            vec![(index % 251) as u8; 4]
        } else {
            vec![(index % 251) as u8]
        };
        objects.put(&object_key, bytes).unwrap();
        blocks.push(BlockDescriptor {
            object_key: object_key.as_str().to_owned(),
            logical_offset: index as u64,
            object_offset: 0,
            len: 1,
            digest_uri: long_digest.clone(),
        });
        object_keys.push(object_key);
    }
    let duplicate_object_key = object_keys[0].clone();
    let fragmented = ChunkManifest {
        chunk_index: 0,
        logical_offset: 0,
        len: BLOCK_COUNT as u64,
        slices: vec![
            SliceManifest {
                slice_id: 1,
                logical_offset: 0,
                len: BLOCK_COUNT as u64,
                blocks,
            },
            // Slice overlays may legally reuse one immutable object through a
            // different logical/object subrange. Release identity is the
            // canonical key plus digest, not the range-specific descriptor.
            SliceManifest {
                slice_id: 2,
                logical_offset: 1,
                len: 2,
                blocks: vec![BlockDescriptor {
                    object_key: duplicate_object_key.as_str().to_owned(),
                    logical_offset: 1,
                    object_offset: 1,
                    len: 2,
                    digest_uri: long_digest.clone(),
                }],
            },
        ],
    };
    plan_chunk_manifest_reads(std::slice::from_ref(&fragmented), 0, BLOCK_COUNT)
        .expect("range-distinct duplicate descriptors are a valid slice overlay");
    metadata.install_manifest(ScanItem {
        key: manifest_key.clone(),
        value: Value(encode_chunk_manifest(&fragmented)),
        version: manifest_item.version,
    });

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service.remove_file_path("/source/fragments.bin").unwrap();
    service.remove_empty_dir_path("/source").unwrap();
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    metadata.fail_after_apply(b"restore-release-member-manifests", 0);

    let member_key = restore::restore_staging_member_key(
        service.mount_id(),
        operation.ref_set_id,
        destination_file.attr.inode,
    );
    let first_cursor = loop {
        service.cleanup_restore_releases(1).unwrap();
        let member = service
            .metadata_store()
            .get(
                RecordFamily::System,
                &member_key,
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .map(|value| restore::decode_restore_staging_member(&value.0).unwrap());
        if let Some(member) = member.filter(|member| !member.manifest_cursor.is_empty()) {
            break member;
        }
    };
    assert_eq!(
        metadata.applied_with_prefix(b"restore-release-member-manifests"),
        1
    );
    assert_eq!(first_cursor.manifest_cursor, manifest_key);
    assert!(first_cursor.manifest_block_cursor > 0);
    assert!(first_cursor.manifest_block_cursor < BLOCK_COUNT as u64);
    assert!(
        first_cursor.manifest_block_cursor < ITEM_BUDGET_BLOCK_CAP,
        "the 8 MiB byte bound, not only the 4096-item bound, must split this manifest"
    );
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::ChunkManifest,
            &manifest_key,
            service.read_version().unwrap(),
            ReadPurpose::RestoreStaging,
        )
        .unwrap()
        .is_some());
    let report = service.fsck_restore_state(true).unwrap();
    assert!(
        report.is_consistent(),
        "mid-cursor restore fsck report: {report:#?}"
    );
    assert!(service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_quarantine_prefix(service.mount_id()),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
    drop(service);

    metadata.clear_fault();
    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    let reopened_cursor = restore::decode_restore_staging_member(
        &reopened
            .metadata_store()
            .get(
                RecordFamily::System,
                &member_key,
                reopened.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap()
            .0,
    )
    .unwrap();
    assert_eq!(
        reopened_cursor.manifest_cursor,
        first_cursor.manifest_cursor
    );
    assert_eq!(
        reopened_cursor.manifest_block_cursor,
        first_cursor.manifest_block_cursor
    );
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(
        report.is_consistent(),
        "reopened mid-cursor restore fsck report: {report:#?}"
    );

    for _ in 0..512 {
        reopened.cleanup_restore_releases(1).unwrap();
        if reopened.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    let metrics = reopened.restore_metrics().unwrap();
    assert_eq!(metrics.releasing, 0);
    assert_eq!(metrics.operation_count(), 0);
    assert_eq!(metrics.staging_rows, 0);
    assert_eq!(metrics.exact_reference_rows, 0);
    assert_eq!(metrics.index_rows, 0);
    assert_eq!(metrics.release_backlog, 0);
    assert!(reopened
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_quarantine_prefix(reopened.mount_id()),
            start_after: None,
            version: reopened.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());

    let destination_prefix = format!(
        "blocks/{}/{}/{}/0/",
        reopened.mount_id().get(),
        destination_file.attr.inode.get(),
        generation,
    );
    let mut queued_by_object = BTreeMap::<String, usize>::new();
    for row in reopened
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(reopened.mount_id()),
            start_after: None,
            version: reopened.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
    {
        let record = decode_object_gc_record(&row.value.0).unwrap();
        if record.object_key.starts_with(&destination_prefix) {
            *queued_by_object.entry(record.object_key).or_default() += 1;
        }
    }
    assert_eq!(queued_by_object.len(), BLOCK_COUNT);
    assert!(queued_by_object.values().all(|count| *count == 1));
    assert_eq!(
        queued_by_object.get(duplicate_object_key.as_str()),
        Some(&1),
        "range-specific duplicate descriptors must produce one canonical GC candidate"
    );

    // The object-GC worker deliberately uses a two-transaction durable claim
    // per object. Its claim/reopen behavior is covered independently; running
    // 8,400 claim transitions here would dominate the restore suite. After
    // proving every fragmented block is queued exactly once, compact the
    // test queue in bounded CAS batches and leave one row for the real worker
    // to drain, so the final no-leak assertion still crosses the worker path.
    loop {
        let rows = reopened
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(reopened.mount_id()),
                start_after: None,
                version: reopened.read_version().unwrap(),
                limit: 256,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap();
        if rows.len() <= 1 {
            break;
        }
        for row in &rows[1..] {
            let record = decode_object_gc_record(&row.value.0).unwrap();
            objects
                .delete(&ObjectKey::new(record.object_key).unwrap())
                .unwrap();
        }
        let version = reopened.next_version().unwrap();
        reopened
            .commit_metadata(MetadataCommand {
                request_id: request_id(
                    b"test-compact-fragmented-release-gc",
                    reopened.mount_id(),
                    InodeId::root(),
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).unwrap(),
                commit_version: version,
                primary_family: RecordFamily::Gc,
                primary_key: rows[1].key.clone(),
                predicates: rows[1..]
                    .iter()
                    .map(|row| PredicateRef {
                        family: RecordFamily::Gc,
                        key: row.key.clone(),
                        predicate: Predicate::VersionEquals(row.version),
                    })
                    .collect(),
                mutations: rows[1..]
                    .iter()
                    .map(|row| delete_mutation(RecordFamily::Gc, row.key.clone()))
                    .collect(),
                watch: Vec::new(),
            })
            .unwrap();
    }
    for _ in 0..8 {
        reopened.cleanup_pending_objects(1).unwrap();
        if reopened
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(reopened.mount_id()),
                start_after: None,
                version: reopened.read_version().unwrap(),
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .is_empty()
        {
            break;
        }
    }
    let remaining_gc = reopened
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(reopened.mount_id()),
            start_after: None,
            version: reopened.read_version().unwrap(),
            limit: 16,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert!(
        remaining_gc.is_empty(),
        "remaining GC records: {:#?}",
        remaining_gc
            .iter()
            .map(|row| decode_object_gc_record(&row.value.0))
            .collect::<Vec<_>>()
    );
    assert!(objects.head(&source_object).unwrap().is_none());
    assert!(object_keys
        .iter()
        .all(|object_key| objects.head(object_key).unwrap().is_none()));
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(
        report.is_consistent(),
        "final restore fsck report: {report:#?}"
    );
}

#[test]
fn restore_release_finds_a_live_snapshot_and_borrower_beyond_first_pages() {
    let service = service();
    service
        .create_dir_path("/pin-padding", 0o755, 1000, 1000)
        .unwrap();
    for _ in 0..64 {
        service.snapshot_subtree_path("/pin-padding").unwrap();
    }

    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/paged-pin",
        b"paged snapshot retention",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();

    let holder = service
        .create_dir_path("/snapshot-holder", 0o755, 1000, 1000)
        .unwrap();
    let padding_names = (0..64)
        .map(|index| DentryName::new(format!("aa-padding-{index:03}").into_bytes()).unwrap())
        .collect::<Vec<_>>();
    service
        .create_files_in_dir_path("/snapshot-holder", padding_names, 0o644, 1000, 1000)
        .unwrap();
    service
        .rename_path(
            "/destination/shared.bin",
            "/snapshot-holder/zz-borrower.bin",
        )
        .unwrap();
    let holder_snapshot = service.snapshot_subtree_path("/snapshot-holder").unwrap();
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
    service.remove_file_path("/source/shared.bin").unwrap();
    service.remove_empty_dir_path("/source").unwrap();
    service.cleanup_pending_objects(64).unwrap();

    service
        .remove_file_path("/snapshot-holder/zz-borrower.bin")
        .unwrap();
    service.remove_empty_dir_path("/destination").unwrap();
    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(
        service.restore_metrics().unwrap().releasing,
        1,
        "a historical escaped borrower beyond the first pin and dentry pages must retain the ref-set"
    );
    let historical_entries = service
        .read_dir_plus_at_snapshot("/snapshot-holder", holder_snapshot.snapshot_id, &[])
        .unwrap();
    assert!(historical_entries
        .iter()
        .any(|entry| entry.dentry.name.as_bytes() == b"zz-borrower.bin"));
    let borrower_name = DentryName::new("zz-borrower.bin").unwrap();
    assert_eq!(
        service
            .read_file_at_snapshot(
                "/snapshot-holder",
                holder_snapshot.snapshot_id,
                std::slice::from_ref(&borrower_name),
                0,
                b"paged snapshot retention".len(),
            )
            .unwrap(),
        b"paged snapshot retention"
    );
    let historical_indexed = service
        .restore_indexed_children_page(
            holder.attr.inode,
            None,
            128,
            Version::new(holder_snapshot.read_version).unwrap(),
            ReadPurpose::Snapshot,
        )
        .unwrap();
    assert!(historical_indexed
        .entries
        .iter()
        .any(|entry| entry.dentry.name.as_bytes() == b"zz-borrower.bin"));

    assert!(service
        .retire_snapshot(holder_snapshot.snapshot_id)
        .unwrap());
    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::System,
            &restore::restore_staging_inode_key(service.mount_id(), restored.destination_root),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn restore_release_finds_a_live_fork_binding_beyond_the_first_page() {
    let service = service();
    service
        .create_dir_path("/binding-padding-source", 0o755, 1000, 1000)
        .unwrap();
    for index in 0..64 {
        service
            .clone_subtree_path_into(
                "/binding-padding-source",
                &format!("/binding-padding-{index:03}"),
            )
            .unwrap();
    }

    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/paged-binding",
        b"paged binding retention",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();
    let holder = service
        .clone_subtree_path_into("/destination", "/generic-holder")
        .unwrap();

    let holder_pin = service.snapshot_pin(holder.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(holder_pin.lease_expires_unix_ms);
    service.reclaim_expired_snapshot_pins(1_024).unwrap();
    assert!(service.snapshot_pin(holder.snapshot_id).unwrap().is_none());

    service.remove_file_path("/destination/shared.bin").unwrap();
    service.remove_empty_dir_path("/destination").unwrap();
    for _ in 0..4 {
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(
        service.restore_metrics().unwrap().releasing,
        1,
        "the live ForkBinding beyond the first 64-row page must retain the ref-set"
    );

    service
        .remove_file_path("/generic-holder/shared.bin")
        .unwrap();
    service.remove_empty_dir_path("/generic-holder").unwrap();
    assert!(service.retire_snapshot(holder.snapshot_id).unwrap());
    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
}

#[test]
fn complete_restore_can_replace_another_complete_restore_and_release_both() {
    let service = service();
    for source in ["/outer-source", "/nested-source"] {
        service.create_dir_path(source, 0o755, 1000, 1000).unwrap();
    }
    publish_path_artifact(
        &service,
        "/outer-source/outer.bin",
        "restore/replace-complete-outer",
        b"outer bytes",
    );
    publish_path_artifact(
        &service,
        "/nested-source/nested.bin",
        "restore/replace-complete-nested",
        b"nested bytes",
    );
    let outer_snapshot = service.snapshot_subtree_path("/outer-source").unwrap();
    let outer = service
        .restore_subtree_path_to_fork("/outer-source", outer_snapshot.snapshot_id, "/outer")
        .unwrap();
    let nested_snapshot = service.snapshot_subtree_path("/nested-source").unwrap();
    let nested = service
        .restore_subtree_path_to_fork("/nested-source", nested_snapshot.snapshot_id, "/nested")
        .unwrap();
    assert!(service.retire_snapshot(outer_snapshot.snapshot_id).unwrap());
    assert!(service
        .retire_snapshot(nested_snapshot.snapshot_id)
        .unwrap());

    service.rename_replace_path("/nested", "/outer").unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    assert_eq!(service.restore_metrics().unwrap().complete, 1);
    let surviving_root = service.resolve_directory_path("/outer").unwrap();
    assert_eq!(
        service
            .read_artifact(
                surviving_root,
                &DentryName::new(b"nested.bin".to_vec()).unwrap(),
            )
            .unwrap(),
        b"nested bytes"
    );
    assert!(service.lookup_path("/outer/outer.bin").unwrap().is_none());

    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    assert_eq!(service.restore_metrics().unwrap().complete, 1);
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Inode,
            &inode_key(service.mount_id(), outer.destination_root),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Inode,
            &inode_key(service.mount_id(), nested.destination_root),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_some());

    service.remove_file_path("/outer/nested.bin").unwrap();
    service.remove_empty_dir_path("/outer").unwrap();
    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().complete, 0);
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    let report = service.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn nested_restore_replace_releases_shared_object_after_escaped_borrower_is_removed() {
    let (service, objects) = service_with_objects();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let source = publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/nested-replace-escaped",
        b"shared restore bytes",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_object = block_key(source.attr.inode, source_body.generation, 0, 0);
    let inherited_checkpoint = publish_path_artifact(
        &service,
        "/source/metadata/checkpoints.jsonl",
        "restore/nested-replace-checkpoint-alias",
        b"source checkpoint alias",
    );
    let inherited_checkpoint_body = inherited_checkpoint.body.as_ref().unwrap();
    let inherited_checkpoint_object = block_key(
        inherited_checkpoint.attr.inode,
        inherited_checkpoint_body.generation,
        0,
        0,
    );

    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork_initialized(
            "/source",
            source_snapshot.snapshot_id,
            "/destination",
            RestoreInitialization {
                remove_relative_paths: vec!["metadata/checkpoints.jsonl".to_owned()],
                files: vec![RestoreInitializationFile {
                    relative_path: "metadata/restore_manifest.json".to_owned(),
                    bytes: b"outer restore manifest".to_vec(),
                    content_type: "application/json".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap();
    assert!(service
        .lookup_path("/destination/metadata/checkpoints.jsonl")
        .unwrap()
        .is_none());
    let outer_manifest = service
        .lookup_path("/destination/metadata/restore_manifest.json")
        .unwrap()
        .unwrap();
    let outer_manifest_body = outer_manifest.body.as_ref().unwrap();
    let outer_manifest_object = block_key(
        outer_manifest.attr.inode,
        outer_manifest_body.generation,
        0,
        0,
    );
    let nested_snapshot = service.snapshot_subtree_path("/destination").unwrap();
    service
        .restore_subtree_path_to_fork_initialized(
            "/destination",
            nested_snapshot.snapshot_id,
            "/nested",
            RestoreInitialization {
                remove_relative_paths: Vec::new(),
                files: vec![RestoreInitializationFile {
                    relative_path: "metadata/restore_manifest.json".to_owned(),
                    bytes: b"nested restore manifest".to_vec(),
                    content_type: "application/json".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap();
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
    assert!(service
        .retire_snapshot(nested_snapshot.snapshot_id)
        .unwrap());

    service.remove_file_path("/source/shared.bin").unwrap();
    service
        .remove_file_path("/source/metadata/checkpoints.jsonl")
        .unwrap();
    service.remove_empty_dir_path("/source/metadata").unwrap();
    service.remove_empty_dir_path("/source").unwrap();
    service
        .rename_path("/destination/shared.bin", "/escaped.bin")
        .unwrap();
    service
        .rename_replace_path("/nested", "/destination")
        .unwrap();
    service.rename_path("/destination", "/replacement").unwrap();
    service.remove_file_path("/replacement/shared.bin").unwrap();
    service
        .remove_file_path("/replacement/metadata/restore_manifest.json")
        .unwrap();
    service
        .remove_empty_dir_path("/replacement/metadata")
        .unwrap();
    service.remove_empty_dir_path("/replacement").unwrap();

    for _ in 0..512 {
        service.cleanup_restore_releases(64).unwrap();
        service.cleanup_pending_objects(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 1
            && objects.head(&outer_manifest_object).unwrap().is_none()
        {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    assert!(objects.head(&outer_manifest_object).unwrap().is_none());
    let intermediate_restore_fsck = service.fsck_restore_state(true).unwrap();
    assert!(
        intermediate_restore_fsck.is_consistent(),
        "intermediate restore fsck: {intermediate_restore_fsck:?}"
    );
    let intermediate_fsck = service.fsck_object_references(FsckMode::Full, 0).unwrap();
    assert!(
        intermediate_fsck.is_consistent(),
        "intermediate object fsck: {intermediate_fsck:?}"
    );
    assert!(objects.head(&source_object).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/escaped.bin"),
        b"shared restore bytes"
    );

    let metadata = service.metadata_store().clone();
    drop(service);
    let service =
        NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects.clone(), 0).unwrap();
    let reopened_restore_fsck = service.fsck_restore_state(true).unwrap();
    assert!(
        reopened_restore_fsck.is_consistent(),
        "reopened restore fsck: {reopened_restore_fsck:?}"
    );
    let reopened_object_fsck = service.fsck_object_references(FsckMode::Full, 0).unwrap();
    assert!(
        reopened_object_fsck.is_consistent(),
        "reopened object fsck: {reopened_object_fsck:?}"
    );

    service.remove_file_path("/escaped.bin").unwrap();
    for _ in 0..1_024 {
        service.cleanup_restore_releases(64).unwrap();
        service.cleanup_pending_objects(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0
            && objects.head(&source_object).unwrap().is_none()
        {
            break;
        }
    }
    let final_metrics = service.restore_metrics().unwrap();
    assert_eq!(final_metrics.releasing, 0);
    assert_eq!(final_metrics.staging_rows, 0);
    assert_eq!(final_metrics.exact_reference_rows, 0);
    assert_eq!(final_metrics.index_rows, 0);
    assert_eq!(final_metrics.release_backlog, 0);
    assert!(objects.head(&source_object).unwrap().is_none());
    assert!(objects
        .head(&inherited_checkpoint_object)
        .unwrap()
        .is_none());
    let report = service.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
    let object_report = service.fsck_object_references(FsckMode::Full, 0).unwrap();
    assert!(
        object_report.is_consistent(),
        "final object fsck report: {object_report:#?}"
    );
}

#[test]
fn outer_restore_release_cascades_into_a_nested_complete_restore() {
    let service = service();
    service
        .create_dir_path("/outer-source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/nested-source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/nested-source/payload.bin",
        "restore/nested-release",
        b"nested restore bytes",
    );
    let outer_snapshot = service.snapshot_subtree_path("/outer-source").unwrap();
    let outer = service
        .restore_subtree_path_to_fork("/outer-source", outer_snapshot.snapshot_id, "/outer")
        .unwrap();
    let nested_snapshot = service.snapshot_subtree_path("/nested-source").unwrap();
    let nested = service
        .restore_subtree_path_to_fork(
            "/nested-source",
            nested_snapshot.snapshot_id,
            "/outer/nested",
        )
        .unwrap();
    assert!(service.retire_snapshot(outer_snapshot.snapshot_id).unwrap());
    assert!(service
        .retire_snapshot(nested_snapshot.snapshot_id)
        .unwrap());

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/outer")
        .unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    assert_eq!(service.restore_metrics().unwrap().complete, 1);
    assert!(
        service.get_attr(nested.destination_root).unwrap().is_none(),
        "slow-path visibility must hide the nested Complete root as soon as its outer root detaches"
    );

    for _ in 0..32 {
        service.cleanup_restore_releases(1).unwrap();
        if service.restore_metrics().unwrap().releasing == 2 {
            break;
        }
    }
    assert_eq!(
        service.restore_metrics().unwrap().releasing,
        2,
        "outer release must durably cascade the nested Complete root"
    );
    assert_eq!(service.restore_metrics().unwrap().complete, 0);
    assert!(service.lookup_path("/outer/nested").unwrap().is_none());

    for _ in 0..512 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    for inode in [outer.destination_root, nested.destination_root] {
        assert!(service
            .metadata_store()
            .get(
                RecordFamily::Inode,
                &inode_key(service.mount_id(), inode),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none());
    }
}

#[test]
fn outer_restore_release_invalidates_a_preplanned_nested_restore_write() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::SetXattr, 0, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/outer-source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/nested-source", 0o755, 1000, 1000)
        .unwrap();
    let outer_snapshot = service.snapshot_subtree_path("/outer-source").unwrap();
    service
        .restore_subtree_path_to_fork("/outer-source", outer_snapshot.snapshot_id, "/outer")
        .unwrap();
    let nested_snapshot = service.snapshot_subtree_path("/nested-source").unwrap();
    let nested = service
        .restore_subtree_path_to_fork(
            "/nested-source",
            nested_snapshot.snapshot_id,
            "/outer/nested",
        )
        .unwrap();
    store.arm(1);

    let writer = Arc::clone(&service);
    let write = std::thread::spawn(move || {
        writer.set_xattr(
            nested.destination_root,
            b"user.preplanned-nested",
            b"must not commit".to_vec(),
            XattrSetMode::Create,
        )
    });
    store.wait_until_blocked();

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/outer")
        .unwrap();
    assert!(service.get_attr(nested.destination_root).unwrap().is_none());
    store.release_blocked();

    assert!(matches!(
        write.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Xattr,
            &xattr_key(
                service.mount_id(),
                nested.destination_root,
                b"user.preplanned-nested",
            ),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn detached_restore_rejects_guessed_writes_at_the_attach_boundary() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::CreateDir, 1, 2)
        .matching_request_prefix(b"restore-attach-destination");
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        service.as_ref(),
        "/source/original.bin",
        "restore/attach-race",
        b"original bytes",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );

    let restoring = Arc::clone(&service);
    let restore_thread = std::thread::spawn(move || {
        restoring.restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
    });
    store.wait_until_blocked();

    let operation = restore::decode_restore_operation(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_operation_key(service.mount_id(), &operation_digest),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap()
            .0,
    )
    .unwrap();
    assert_eq!(
        operation.state,
        restore::RestoreOperationState::ReadyToAttach
    );
    let guessed_name = DentryName::new("guessed.txt").unwrap();
    assert!(matches!(
        service.create_file(
            operation.destination_root,
            guessed_name.clone(),
            0o644,
            1000,
            1000,
        ),
        Err(MetadError::RestoreInProgress)
    ));
    assert!(matches!(
        service.set_xattr(
            operation.destination_root,
            b"user.guessed",
            b"forbidden".to_vec(),
            XattrSetMode::Create,
        ),
        Err(MetadError::RestoreInProgress)
    ));
    let objects_before_publish = objects.object_count();
    assert!(matches!(
        service.publish_artifact(PublishArtifact {
            parent: operation.destination_root,
            name: DentryName::new("guessed.bin").unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "restore/forbidden".to_owned(),
            bytes: b"must not upload".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        }),
        Err(MetadError::RestoreInProgress)
    ));
    assert_eq!(objects.object_count(), objects_before_publish);
    assert!(matches!(
        service.publish_checkpoint(
            operation.destination_root,
            vec![CheckpointShard {
                name: DentryName::new("guessed-checkpoint.bin").unwrap(),
                bytes: b"must not upload".to_vec(),
            }],
            1000,
            1000,
        ),
        Err(MetadError::RestoreInProgress)
    ));
    assert_eq!(objects.object_count(), objects_before_publish);

    store.release_blocked();
    let outcome = restore_thread.join().unwrap().unwrap();
    assert_eq!(outcome.destination_root, operation.destination_root);
    assert!(service
        .lookup_plus(outcome.destination_root, &guessed_name)
        .unwrap()
        .is_none());
}

#[test]
fn restore_attach_reproves_a_concurrently_occupied_destination_as_a_conflict() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::CreateDir, 1, 2)
        .matching_request_prefix(b"restore-attach-destination");
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/source/empty.txt", 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let snapshot_id = snapshot.snapshot_id;

    let restoring = Arc::clone(&service);
    let restore_thread = std::thread::spawn(move || {
        restoring.restore_subtree_path_to_fork("/source", snapshot_id, "/destination")
    });
    store.wait_until_blocked();

    service
        .create_dir_path("/destination", 0o755, 1000, 1000)
        .unwrap();
    store.release_blocked();
    assert!(matches!(
        restore_thread.join().unwrap(),
        Err(MetadError::RestoreDestinationConflict { destination })
            if destination == "/destination"
    ));
    assert_eq!(store.predicate_failures(), 1);

    let interrupted = durable_restore_operation(&service, snapshot_id);
    assert_eq!(
        interrupted.state,
        restore::RestoreOperationState::ReadyToAttach
    );
    service.remove_empty_dir_path("/destination").unwrap();

    let outcome = service
        .restore_subtree_path_to_fork("/source", snapshot_id, "/destination")
        .unwrap();
    assert_eq!(outcome.destination_root, interrupted.destination_root);
    assert_eq!(outcome.state, RestoreState::Complete);
    assert!(service.fsck_restore_state(false).unwrap().is_consistent());
}

#[test]
fn restore_release_cas_rejects_a_preplanned_complete_write() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::CreateFile, 0, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        service.as_ref(),
        "/source/original.bin",
        "restore/release-race",
        b"original bytes",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    store.arm(1);

    let writer = Arc::clone(&service);
    let create = std::thread::spawn(move || {
        writer.create_file(
            restored.destination_root,
            DentryName::new("raced.txt").unwrap(),
            0o644,
            1000,
            1000,
        )
    });
    store.wait_until_blocked();
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    store.release_blocked();

    assert!(matches!(
        create.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Dentry,
            &dentry_key(
                service.mount_id(),
                restored.destination_root,
                &DentryName::new("raced.txt").unwrap(),
            ),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn restore_enrollment_invalidates_an_ordinary_preplanned_xattr_write() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::SetXattr, 0, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let ordinary = service
        .create_file_path("/ordinary.bin", 0o644, 1000, 1000)
        .unwrap();
    let ordinary_inode = ordinary.attr.inode;
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    store.arm(1);

    let writer = Arc::clone(&service);
    let write = std::thread::spawn(move || {
        writer.set_xattr(
            ordinary_inode,
            b"user.preplanned",
            b"must not commit".to_vec(),
            XattrSetMode::Create,
        )
    });
    store.wait_until_blocked();

    service
        .rename_path("/ordinary.bin", "/destination/ordinary.bin")
        .unwrap();
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    store.release_blocked();

    assert!(matches!(
        write.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Xattr,
            &xattr_key(service.mount_id(), ordinary_inode, b"user.preplanned",),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn allocator_reservation_does_not_invalidate_a_preplanned_ordinary_write() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::SetXattr, 0, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let ordinary = service
        .create_file_path("/ordinary-before-reservation.bin", 0o644, 1000, 1000)
        .unwrap();
    let activation_key = restore::restore_activation_fence_key(service.mount_id());
    let activation_before = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &activation_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let allocator = allocator_key(service.mount_id());
    let allocator_before = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &allocator,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();

    store.arm(1);
    let writer = Arc::clone(&service);
    let inode = ordinary.attr.inode;
    let write = std::thread::spawn(move || {
        writer.set_xattr(
            inode,
            b"user.preplanned-before-reservation",
            b"must commit".to_vec(),
            XattrSetMode::Create,
        )
    });
    store.wait_until_blocked();

    for _ in 0..=ALLOCATOR_VERSION_RESERVATION {
        service.next_version().unwrap();
        let current = service
            .metadata_store()
            .get_versioned(
                RecordFamily::System,
                &allocator,
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();
        if current.version != allocator_before.version {
            break;
        }
    }
    let allocator_after = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &allocator,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    assert_ne!(allocator_after.version, allocator_before.version);
    let activation_after = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &activation_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    assert_eq!(activation_after.version, activation_before.version);

    store.release_blocked();
    write.join().unwrap().unwrap();
    assert_eq!(
        service
            .get_xattr(inode, b"user.preplanned-before-reservation")
            .unwrap(),
        Some(b"must commit".to_vec())
    );
}

#[test]
fn first_restore_hold_invalidates_a_preplanned_ordinary_write() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::SetXattr, 0, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let ordinary = service
        .create_file_path("/ordinary-before-restore.bin", 0o644, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    store.arm(1);

    let writer = Arc::clone(&service);
    let ordinary_inode = ordinary.attr.inode;
    let write = std::thread::spawn(move || {
        writer.set_xattr(
            ordinary_inode,
            b"user.preplanned-before-restore",
            b"must not commit".to_vec(),
            XattrSetMode::Create,
        )
    });
    store.wait_until_blocked();

    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    store.release_blocked();

    assert!(matches!(
        write.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Xattr,
            &xattr_key(
                service.mount_id(),
                ordinary_inode,
                b"user.preplanned-before-restore",
            ),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn restore_release_cas_rejects_a_preplanned_checkpoint_publish() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::PublishArtifact, 0, 2)
        .matching_request_prefix(b"publish-checkpoint");
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    store.arm(1);

    let writer = Arc::clone(&service);
    let publish = std::thread::spawn(move || {
        writer.publish_checkpoint(
            restored.destination_root,
            vec![CheckpointShard {
                name: DentryName::new("raced-checkpoint.bin").unwrap(),
                bytes: b"raced checkpoint bytes".to_vec(),
            }],
            1000,
            1000,
        )
    });
    store.wait_until_blocked();

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    store.release_blocked();

    assert!(matches!(
        publish.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Dentry,
            &dentry_key(
                service.mount_id(),
                restored.destination_root,
                &DentryName::new("raced-checkpoint.bin").unwrap(),
            ),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert_eq!(objects.object_count(), 0);
}

#[test]
fn restore_release_cas_rejects_a_preplanned_dynamic_catalog_registration() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RegisterNamespaceIndex, 0, 2)
        .matching_request_prefix(b"register-namespace-index");
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    service
        .create_dir_path("/destination/dynamic", 0o755, 1000, 1000)
        .unwrap();
    store.arm(1);

    let writer = Arc::clone(&service);
    let registration = std::thread::spawn(move || {
        writer.register_namespace_index(NamespaceIndexRegistration {
            path: "/destination/dynamic".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.kind"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: Vec::new(),
        })
    });
    store.wait_until_blocked();

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    store.release_blocked();

    assert!(matches!(
        registration.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &path_index_catalog_key(service.mount_id(), "/destination/dynamic"),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn complete_restore_root_can_replace_an_empty_ordinary_directory() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/original.bin",
        "restore/move-original",
        b"original bytes",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let victim = service
        .create_dir_path("/ordinary-victim", 0o755, 1000, 1000)
        .unwrap();

    let moved = service
        .rename_replace_path("/destination", "/ordinary-victim")
        .unwrap();
    assert_eq!(moved.entry.attr.inode, restored.destination_root);
    assert!(service.lookup_path("/destination").unwrap().is_none());
    assert_eq!(
        service.resolve_directory_path("/ordinary-victim").unwrap(),
        restored.destination_root
    );
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::Inode,
            &inode_key(service.mount_id(), victim.attr.inode),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert_eq!(service.restore_metrics().unwrap().complete, 1);
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    service
        .create_file_path("/ordinary-victim/after-move", 0o644, 1000, 1000)
        .unwrap();
}

#[test]
fn restore_index_pagination_and_mvcc_folding_cross_internal_scan_pages() {
    const ENTRY_COUNT: usize = 273;
    const API_PAGE: usize = 13;

    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let mut custom_rows = Vec::with_capacity(ENTRY_COUNT);
    for index in 0..ENTRY_COUNT {
        let path = format!("/source/item-{index:03}.bin");
        publish_path_artifact(
            &service,
            &path,
            &format!("paged-index/{index:03}"),
            &[index as u8],
        );
        custom_rows.push(NamespaceIndexRow {
            path,
            values: vec![NamespaceIndexValue {
                field: NamespaceFindField::new("run.ordinal"),
                value: NamespacePredicateValue::U64(index as u64),
            }],
        });
    }
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.ordinal"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: custom_rows,
        })
        .unwrap();
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/restored")
        .unwrap();
    let restored_root = service.resolve_directory_path("/restored").unwrap();

    let mut current_names = Vec::new();
    let mut cursor = None;
    loop {
        let page = service
            .list_indexed_path_page("/restored", cursor.as_ref(), API_PAGE)
            .unwrap();
        assert!(page.entries.len() <= API_PAGE);
        current_names.extend(
            page.entries
                .iter()
                .map(|entry| entry.dentry.name.as_bytes().to_vec()),
        );
        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(cursor
            .as_ref()
            .is_none_or(|previous: &DentryName| previous.as_bytes() < next.as_bytes()));
        cursor = Some(next);
    }
    assert_eq!(current_names.len(), ENTRY_COUNT);
    assert!(current_names.windows(2).all(|pair| pair[0] < pair[1]));

    let historical = service.snapshot_subtree_path("/restored").unwrap();
    for revision in 0..ENTRY_COUNT {
        republish_path_artifact(
            &service,
            restored_root,
            "item-000.bin",
            &format!("paged-history/{revision:03}"),
            &[revision as u8],
        );
    }
    let snapshot_version = Version::new(historical.read_version).unwrap();
    let mut historical_names = Vec::new();
    let mut cursor = None;
    loop {
        let page = service
            .restore_indexed_children_page(
                restored_root,
                cursor.as_ref(),
                API_PAGE,
                snapshot_version,
                ReadPurpose::Snapshot,
            )
            .unwrap();
        historical_names.extend(
            page.entries
                .iter()
                .map(|entry| entry.dentry.name.as_bytes().to_vec()),
        );
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(historical_names, current_names);
    let historical_custom = service
        .restore_custom_index_at_path(
            "/restored",
            restored_root,
            snapshot_version,
            ReadPurpose::Snapshot,
        )
        .unwrap()
        .unwrap();
    assert_eq!(historical_custom.rows.len(), ENTRY_COUNT);
}

#[test]
fn restore_index_materialization_and_seal_never_request_unbounded_pages() {
    const ENTRY_COUNT: usize = 273;

    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let mut rows = Vec::with_capacity(ENTRY_COUNT);
    for index in 0..ENTRY_COUNT {
        let path = format!("/source/item-{index:03}.bin");
        publish_path_artifact(
            &service,
            &path,
            &format!("bounded-index/{index:03}"),
            &[index as u8],
        );
        rows.push(NamespaceIndexRow {
            path,
            values: vec![NamespaceIndexValue {
                field: NamespaceFindField::new("run.ordinal"),
                value: NamespacePredicateValue::U64(index as u64),
            }],
        });
    }
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.ordinal"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows,
        })
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();

    metadata.reset_scan_bounds();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let counts = metadata.counts();
    assert_eq!(counts.restore_index_unbounded_scans, 0);
    assert!(
        counts.largest_restore_index_scan <= 256,
        "restore index requested a {}-row page",
        counts.largest_restore_index_scan
    );
    let restored = service.resolve_directory_path("/restored").unwrap();
    let custom = service
        .restore_custom_index_at_path(
            "/restored",
            restored,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .unwrap();
    assert_eq!(custom.rows.len(), ENTRY_COUNT);
    let counts = metadata.counts();
    assert_eq!(counts.restore_index_unbounded_scans, 0);
    assert!(
        counts.largest_restore_index_scan <= 256,
        "restore index query requested a {}-row physical page",
        counts.largest_restore_index_scan
    );
}

fn query_custom_index_without_dentry_scan(
    service: &NoKvFs<PurposeTrackingStore, MemoryObjectStore>,
    metadata: &PurposeTrackingStore,
    path: &str,
) -> Option<restore_index::RestoreCustomIndex> {
    let root = service.resolve_directory_path(path).unwrap();
    let before = metadata.counts();
    let index = service
        .restore_custom_index_at_path(
            path,
            root,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    let after = metadata.counts();
    assert_eq!(
        after.dentry_scans, before.dentry_scans,
        "custom index query for {path} enumerated the namespace"
    );
    index
}

#[test]
fn custom_index_query_fast_paths_do_not_enumerate_unindexed_or_canonical_namespaces() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();

    service
        .create_dir_path("/unindexed", 0o755, 1000, 1000)
        .unwrap();
    let unindexed = service.resolve_directory_path("/unindexed").unwrap();
    let names = (0..1_025)
        .map(|index| DentryName::new(format!("item-{index:04}").into_bytes()).unwrap())
        .collect::<Vec<_>>();
    service
        .create_files_in_dir(unindexed, names, 0o644, 1000, 1000)
        .unwrap();
    assert!(query_custom_index_without_dentry_scan(&service, &metadata, "/unindexed").is_none());

    service
        .create_dir_path("/canonical", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/canonical/indexed.bin",
        "canonical/indexed",
        b"indexed",
    );
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/canonical".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.kind"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/canonical/indexed.bin".to_owned(),
                values: vec![NamespaceIndexValue {
                    field: NamespaceFindField::new("run.kind"),
                    value: NamespacePredicateValue::String("canonical".to_owned()),
                }],
            }],
        })
        .unwrap();
    let canonical =
        query_custom_index_without_dentry_scan(&service, &metadata, "/canonical").unwrap();
    assert_eq!(canonical.rows.len(), 1);
    assert_eq!(canonical.rows[0].path, "/canonical/indexed.bin");
}

#[test]
fn restore_overlay_queries_are_index_row_driven_across_nested_and_generic_forks() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/source/a.bin", "row-driven/a", b"a");
    publish_path_artifact(&service, "/source/b.bin", "row-driven/b", b"b");
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.kind"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: vec![
                NamespaceIndexRow {
                    path: "/source/a.bin".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.kind"),
                        value: NamespacePredicateValue::String("a".to_owned()),
                    }],
                },
                NamespaceIndexRow {
                    path: "/source/b.bin".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.kind"),
                        value: NamespacePredicateValue::String("b".to_owned()),
                    }],
                },
            ],
        })
        .unwrap();

    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/restored")
        .unwrap();
    let restored =
        query_custom_index_without_dentry_scan(&service, &metadata, "/restored").unwrap();
    assert_eq!(restored.rows.len(), 2);

    // Post-restore namespace growth is deliberately unindexed. The scan
    // assertion proves query cost is independent of that growth, so the
    // creation-time one-million-entry restore limit cannot become a read-time
    // failure after the destination grows beyond it.
    let restored_root = service.resolve_directory_path("/restored").unwrap();
    let names = (0..1_025)
        .map(|index| DentryName::new(format!("post-restore-{index:04}").into_bytes()).unwrap())
        .collect::<Vec<_>>();
    for batch in names.chunks(128) {
        service
            .create_files_in_dir(restored_root, batch.to_vec(), 0o644, 1000, 1000)
            .unwrap();
    }
    let grown = query_custom_index_without_dentry_scan(&service, &metadata, "/restored").unwrap();
    assert_eq!(grown.rows.len(), 2);

    let restored_snapshot = service.snapshot_subtree_path("/restored").unwrap();
    service
        .restore_subtree_path_to_fork("/restored", restored_snapshot.snapshot_id, "/nested")
        .unwrap();
    let nested = query_custom_index_without_dentry_scan(&service, &metadata, "/nested").unwrap();
    assert_eq!(nested.rows.len(), 2);

    service
        .clone_subtree_path_into("/restored", "/generic")
        .unwrap();
    let generic = query_custom_index_without_dentry_scan(&service, &metadata, "/generic").unwrap();
    assert_eq!(generic.rows.len(), 2);
}

#[test]
fn restore_index_catalog_replacement_over_command_budget_is_atomic() {
    const ENTRY_COUNT: usize = 520;

    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let mut rows = Vec::with_capacity(ENTRY_COUNT);
    for index in 0..ENTRY_COUNT {
        let path = format!("/source/item-{index:03}.bin");
        publish_path_artifact(
            &service,
            &path,
            &format!("budget-index/{index:03}"),
            &[index as u8],
        );
        rows.push(NamespaceIndexRow {
            path,
            values: vec![NamespaceIndexValue {
                field: NamespaceFindField::new("run.ordinal"),
                value: NamespacePredicateValue::U64(index as u64),
            }],
        });
    }
    let fields = vec![NamespaceIndexField {
        field: NamespaceFindField::new("run.ordinal"),
        operators: vec![NamespacePredicateOp::Eq],
        sortable: true,
        facetable: true,
    }];
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: fields.clone(),
            rows,
        })
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let error = service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/restored".to_owned(),
            fields,
            rows: Vec::new(),
        })
        .unwrap_err();
    assert!(matches!(
        error,
        MetadError::RestoreResourceLimit {
            resource,
            limit: 4096,
            actual,
        } if resource.contains("restore index") && actual > 4096
    ));

    let read_version = service.read_version().unwrap();
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &path_index_catalog_key(service.mount_id(), "/restored"),
            read_version,
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    let restored = service.resolve_directory_path("/restored").unwrap();
    let inherited = service
        .restore_custom_index_at_path("/restored", restored, read_version, ReadPurpose::UserStrong)
        .unwrap()
        .unwrap();
    assert_eq!(inherited.rows.len(), ENTRY_COUNT);
}

fn assert_restore_command_limit(error: MetadError, expected_resource: &str, expected_limit: u64) {
    match error {
        MetadError::RestoreResourceLimit {
            resource,
            limit,
            actual,
        } => {
            assert_eq!(resource, expected_resource);
            assert_eq!(limit, expected_limit);
            assert!(actual > limit, "resource limit must report an overflow");
        }
        other => panic!("expected restore resource limit, got {other}"),
    }
}

fn seed_restored_hardlink_fanout(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    parent: InodeId,
    name: &DentryName,
    additional_links: usize,
) -> DentryProjection {
    let version = service.next_version().unwrap();
    let read_version = predecessor(version).unwrap();
    let dentry = dentry_key(service.mount_id(), parent, name);
    let dentry_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::Dentry,
            &dentry,
            read_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let original = decode_dentry_projection(&dentry_item.value.0).unwrap();
    let inode_key = inode_key(service.mount_id(), original.attr.inode);
    let inode_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::Inode,
            &inode_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let mut attr = decode_inode_attr(&inode_item.value.0).unwrap();
    attr.nlink = u32::try_from(additional_links + 1).unwrap();
    attr.generation = version.get();
    attr.ctime_ms = current_time_ms();

    let mut updated_original = original.clone();
    updated_original.attr = attr.clone();
    updated_original.dentry.attr_generation = attr.generation;
    let restore_index = service
        .restore_index_link_plan(&[(original.clone(), updated_original.clone())], version)
        .unwrap();
    let mut predicates = vec![
        PredicateRef {
            family: RecordFamily::Dentry,
            key: dentry.clone(),
            predicate: Predicate::VersionEquals(dentry_item.version),
        },
        PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key.clone(),
            predicate: Predicate::VersionEquals(inode_item.version),
        },
    ];
    predicates.extend(restore_index.predicates);
    let mut mutations = vec![
        Mutation {
            family: RecordFamily::Inode,
            key: inode_key,
            op: MutationOp::Put,
            value: Some(Value(encode_inode_attr(&attr))),
        },
        put_projection_mutation(RecordFamily::Dentry, dentry.clone(), &updated_original),
    ];
    for index in 0..additional_links {
        let link_name = DentryName::new(format!("fanout-{index:04}").into_bytes()).unwrap();
        let linked = projection(
            parent,
            link_name.clone(),
            attr.clone(),
            original.body.clone(),
        );
        mutations.push(put_projection_mutation(
            RecordFamily::Dentry,
            dentry_key(service.mount_id(), parent, &link_name),
            &linked,
        ));
    }
    mutations.extend(restore_index.mutations);
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-seed-restore-hardlink-fanout",
                service.mount_id(),
                original.attr.inode,
                version,
            ),
            kind: CommandKind::Link,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry,
            predicates,
            mutations,
            watch: Vec::new(),
        })
        .unwrap();
    updated_original
}

#[test]
fn restore_namespace_hardlink_fanout_item_limits_are_atomic() {
    const ADDITIONAL_LINKS: usize = 2_050;

    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/primary.bin",
        "budget/primary",
        b"primary",
    );
    publish_path_artifact(
        &service,
        "/source/replacement.bin",
        "budget/replacement",
        b"replacement",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();
    let restored_root = service.resolve_directory_path("/restored").unwrap();
    let primary_name = DentryName::new("primary.bin").unwrap();
    let seeded =
        seed_restored_hardlink_fanout(&service, restored_root, &primary_name, ADDITIONAL_LINKS);
    let before_primary = service
        .lookup_path("/restored/primary.bin")
        .unwrap()
        .unwrap();
    let before_replacement = service
        .lookup_path("/restored/replacement.bin")
        .unwrap()
        .unwrap();
    let before_sample = service
        .lookup_path("/restored/fanout-2049")
        .unwrap()
        .unwrap();
    let before_index = service
        .list_indexed_path_page("/restored", None, 10)
        .unwrap();
    assert_eq!(before_primary.attr.nlink, (ADDITIONAL_LINKS + 1) as u32);

    let error = service
        .link(
            seeded.attr.inode,
            restored_root,
            DentryName::new("overflow-link.bin").unwrap(),
        )
        .unwrap_err();
    assert_restore_command_limit(error, "restore namespace link items", 4_096);
    assert!(service
        .lookup_path("/restored/overflow-link.bin")
        .unwrap()
        .is_none());
    assert_eq!(
        service.lookup_path("/restored/primary.bin").unwrap(),
        Some(before_primary.clone())
    );
    assert_eq!(
        service.lookup_path("/restored/fanout-2049").unwrap(),
        Some(before_sample.clone())
    );
    assert_eq!(
        service.get_attr(seeded.attr.inode).unwrap(),
        Some(seeded.attr.clone())
    );
    assert_eq!(
        service
            .list_indexed_path_page("/restored", None, 10)
            .unwrap(),
        before_index
    );

    let error = service
        .remove_file_path("/restored/primary.bin")
        .unwrap_err();
    assert_restore_command_limit(error, "restore namespace remove file items", 4_096);
    assert_eq!(
        service.lookup_path("/restored/primary.bin").unwrap(),
        Some(before_primary.clone())
    );
    assert_eq!(
        service.lookup_path("/restored/fanout-2049").unwrap(),
        Some(before_sample.clone())
    );

    let (existing, dentry_version) = service
        .lookup_plus_for_write_plan(restored_root, &primary_name)
        .unwrap()
        .unwrap();
    let version = service.next_version().unwrap();
    let mut published_attr = existing.attr.clone();
    published_attr.generation = version.get();
    published_attr.ctime_ms = current_time_ms();
    published_attr.mtime_ms = published_attr.ctime_ms;
    let mut published_body = existing.body.clone().unwrap();
    published_body.generation = version.get();
    published_body.base_generation = 0;
    published_body.manifest_id = "budget/replaced".to_owned();
    let published = projection(
        restored_root,
        primary_name.clone(),
        published_attr,
        Some(published_body),
    );
    let error = service
        .commit_replace_projection_with_chunks(ReplaceProjectionCommit {
            request_id: None,
            kind: CommandKind::ReplaceArtifact,
            projection: &published,
            chunks: &[],
            old_chunks: &[],
            dentry_version,
            old_generation: None,
            version,
            path_index: Some(path_index_key(
                service.mount_id(),
                &parse_absolute_path("/restored/primary.bin").unwrap(),
            )),
            object_reference: None,
        })
        .unwrap_err();
    assert_restore_command_limit(error, "restore namespace publish replace items", 4_096);
    assert_eq!(
        service.lookup_path("/restored/primary.bin").unwrap(),
        Some(before_primary.clone())
    );
    assert_eq!(
        service
            .list_indexed_path_page("/restored", None, 10)
            .unwrap(),
        before_index
    );

    let error = service
        .rename_replace_path("/restored/replacement.bin", "/restored/primary.bin")
        .unwrap_err();
    assert_restore_command_limit(error, "restore namespace rename items", 4_096);
    assert_eq!(
        service.lookup_path("/restored/primary.bin").unwrap(),
        Some(before_primary)
    );
    assert_eq!(
        service.lookup_path("/restored/replacement.bin").unwrap(),
        Some(before_replacement)
    );
    assert_eq!(
        service.lookup_path("/restored/fanout-2049").unwrap(),
        Some(before_sample)
    );
    assert_eq!(
        service
            .list_indexed_path_page("/restored", None, 10)
            .unwrap(),
        before_index
    );
}

fn install_large_body_for_restore_budget(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    path: &str,
    producer_bytes: usize,
) {
    let components = parse_absolute_path(path).unwrap();
    let (name, parent_components) = components.split_last().unwrap();
    let parent = service
        .resolve_directory_path(&canonical_path(parent_components).unwrap())
        .unwrap();
    let version = service.next_version().unwrap();
    let read_version = predecessor(version).unwrap();
    let dentry_key = dentry_key(service.mount_id(), parent, name);
    let dentry_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::Dentry,
            &dentry_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let old_projection = decode_dentry_projection(&dentry_item.value.0).unwrap();
    let inode_key = inode_key(service.mount_id(), old_projection.attr.inode);
    let inode_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::Inode,
            &inode_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let index_key = path_index_key(service.mount_id(), &components);
    let index_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::PathIndex,
            &index_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let mut attr = old_projection.attr.clone();
    attr.size = 0;
    attr.generation = version.get();
    attr.mtime_ms = current_time_ms();
    attr.ctime_ms = attr.mtime_ms;
    let body = BodyDescriptor {
        producer: "x".repeat(producer_bytes),
        digest_uri: "sha256:large-command-budget".to_owned(),
        size: 0,
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "large-command-budget".to_owned(),
        generation: version.get(),
        base_generation: 0,
        chunk_size: DEFAULT_CHUNK_SIZE,
        block_size: DEFAULT_BLOCK_SIZE as u64,
    };
    let updated = projection(parent, name.clone(), attr.clone(), Some(body.clone()));
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-install-large-restore-body",
                service.mount_id(),
                attr.inode,
                version,
            ),
            kind: CommandKind::ReplaceArtifact,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry_key.clone(),
                    predicate: Predicate::VersionEquals(dentry_item.version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key.clone(),
                    predicate: Predicate::VersionEquals(inode_item.version),
                },
                PredicateRef {
                    family: RecordFamily::PathIndex,
                    key: index_key.clone(),
                    predicate: Predicate::VersionEquals(index_item.version),
                },
            ],
            mutations: vec![
                Mutation {
                    family: RecordFamily::Inode,
                    key: inode_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_inode_attr(&attr))),
                },
                put_projection_mutation(RecordFamily::Dentry, dentry_key, &updated),
                put_projection_mutation(RecordFamily::PathIndex, index_key, &updated),
                Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(
                        service.mount_id(),
                        attr.inode,
                        body.generation,
                        BODY_SUMMARY_CHUNK_INDEX,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_body_descriptor(&body))),
                },
            ],
            watch: Vec::new(),
        })
        .unwrap();
}

#[test]
fn restore_namespace_rename_full_command_byte_limit_is_atomic() {
    const CATALOG_COUNT: usize = 27;
    const LARGE_VALUE_BYTES: usize = 50_000;

    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let mut catalog_paths = vec!["/source".to_owned()];
    let mut parent_path = "/source".to_owned();
    for index in 1..CATALOG_COUNT {
        parent_path.push_str(&format!("/d{index:02}"));
        service
            .create_dir_path(&parent_path, 0o755, 1000, 1000)
            .unwrap();
        catalog_paths.push(parent_path.clone());
    }
    let source_path = format!("{parent_path}/large.bin");
    publish_path_artifact(&service, &source_path, "budget/large", b"x");
    install_large_body_for_restore_budget(&service, &source_path, LARGE_VALUE_BYTES);
    for catalog_path in catalog_paths {
        service
            .register_namespace_index(NamespaceIndexRegistration {
                path: catalog_path,
                fields: vec![NamespaceIndexField {
                    field: NamespaceFindField::new("budget.value"),
                    operators: vec![NamespacePredicateOp::Eq],
                    sortable: false,
                    facetable: false,
                }],
                rows: vec![NamespaceIndexRow {
                    path: source_path.clone(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("budget.value"),
                        value: NamespacePredicateValue::String("v".repeat(LARGE_VALUE_BYTES)),
                    }],
                }],
            })
            .unwrap();
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();
    let restored_source_path = source_path.replacen("/source", "/restored", 1);
    let restored_destination_path = format!("{restored_source_path}-renamed");
    let before = service.lookup_path(&restored_source_path).unwrap().unwrap();
    let restored_root = service.resolve_directory_path("/restored").unwrap();
    let before_index = service
        .restore_custom_index_at_path(
            "/restored",
            restored_root,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap();

    let error = service
        .rename_path(&restored_source_path, &restored_destination_path)
        .unwrap_err();
    assert_restore_command_limit(error, "restore namespace rename bytes", 8 * 1024 * 1024);
    assert_eq!(
        service.lookup_path(&restored_source_path).unwrap(),
        Some(before)
    );
    assert!(service
        .lookup_path(&restored_destination_path)
        .unwrap()
        .is_none());
    assert_eq!(
        service
            .restore_custom_index_at_path(
                "/restored",
                restored_root,
                service.read_version().unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        before_index
    );
}

#[test]
fn ordinary_namespace_commands_keep_their_existing_budget_and_scan_behavior() {
    const ENTRY_COUNT: usize = 1_400;

    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let ordinary = service
        .create_dir_path("/ordinary", 0o755, 1000, 1000)
        .unwrap();
    let names = (0..ENTRY_COUNT)
        .map(|index| DentryName::new(format!("item-{index:04}").into_bytes()).unwrap())
        .collect::<Vec<_>>();
    let before = metadata.counts();

    let created = service
        .create_files_in_dir(ordinary.attr.inode, names, 0o644, 1000, 1000)
        .unwrap();

    assert_eq!(created.len(), ENTRY_COUNT);
    let after = metadata.counts();
    assert_eq!(after.user_strong_scans, before.user_strong_scans);
    assert_eq!(after.write_plan_scans, before.write_plan_scans);
    assert_eq!(after.snapshot_scans, before.snapshot_scans);
    assert!(service
        .lookup_plus(ordinary.attr.inode, &DentryName::new("item-1399").unwrap())
        .unwrap()
        .is_some());
}

fn replace_path_artifact(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    path: &str,
    manifest_id: &str,
    bytes: &[u8],
) -> DentryWithAttr {
    let prepared = service.prepare_artifact_replace_path(path).unwrap();
    let session = PublishArtifactSession {
        parent: prepared.parent,
        name: prepared.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:test".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        size: bytes.len() as u64,
        ranges: vec![PublishArtifactRange {
            offset: 0,
            bytes: bytes.to_vec(),
        }],
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };
    service
        .publish_prepared_artifact_session(prepared, session)
        .unwrap()
        .entry
}

#[test]
fn path_replace_of_inherited_restore_entry_does_not_leak_a_canonical_index_row() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/inherited.bin",
        "path-replace/source",
        b"source",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let destination_components = parse_absolute_path("/destination/inherited.bin").unwrap();
    let destination_index = path_index_key(service.mount_id(), &destination_components);
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &destination_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());

    replace_path_artifact(
        &service,
        "/destination/inherited.bin",
        "path-replace/destination",
        b"destination-v2",
    );
    assert_eq!(
        read_artifact_at_path(&service, "/destination/inherited.bin"),
        b"destination-v2"
    );
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &destination_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    for _ in 0..512 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &destination_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(service.fsck_restore_state(true).unwrap().is_consistent());
}

#[test]
fn path_replace_of_a_later_hardlink_keeps_its_canonical_index_row() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/inherited.bin",
        "path-replace-link/source",
        b"source",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let destination = service.resolve_directory_path("/destination").unwrap();
    let inherited = service
        .lookup_path("/destination/inherited.bin")
        .unwrap()
        .unwrap();
    service
        .link(
            inherited.attr.inode,
            destination,
            DentryName::new("later-link.bin").unwrap(),
        )
        .unwrap();
    let link_components = parse_absolute_path("/destination/later-link.bin").unwrap();
    let link_index = path_index_key(service.mount_id(), &link_components);
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &link_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());

    let replaced = replace_path_artifact(
        &service,
        "/destination/later-link.bin",
        "path-replace-link/destination",
        b"hardlink-v2",
    );
    let canonical = service
        .metadata_store()
        .get(
            RecordFamily::PathIndex,
            &link_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .expect("later canonical hardlink must receive a PathIndex row");
    let canonical = decode_dentry_projection(&canonical.0).unwrap();
    assert_eq!(
        canonical,
        DentryProjection {
            dentry: replaced.dentry.clone(),
            attr: replaced.attr.clone(),
            body: replaced.body.clone(),
        }
    );
    assert_eq!(
        read_artifact_at_path(&service, "/destination/inherited.bin"),
        b"hardlink-v2"
    );
    let indexed = service
        .list_indexed_path_page("/destination", None, 10)
        .unwrap();
    assert_eq!(indexed.entries.len(), 2);
    assert!(indexed
        .entries
        .iter()
        .any(|entry| entry.dentry.name.as_bytes() == b"later-link.bin"));
    assert!(service.fsck_restore_state(true).unwrap().is_consistent());
}

#[test]
fn complete_restore_prepared_terminal_retries_survive_prune_epoch_and_log_replay() {
    let metadata = RestorePostApplyFaultStore::new();
    let objects = FaultObjectStore::new(MemoryObjectStore::new());
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/inherited.bin",
        "prepared-terminal/source",
        b"source-v1",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/prepared-terminal-checkpoint", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/prepared-terminal-log",
            "mount-1:/",
            1,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        ))
        .unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();

    let prepared_create = service
        .prepare_artifact_create_path("/destination/created.bin")
        .unwrap();
    let create_bytes = b"created-after-restore";
    let create_written = service
        .stage_prepared_artifact_ranges(
            &prepared_create,
            "prepared-terminal/create",
            &[PublishArtifactRange {
                offset: 0,
                bytes: create_bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let create_session = PublishArtifactStagedSession {
        parent: prepared_create.parent,
        name: prepared_create.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:prepared-terminal-create".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "prepared-terminal/create".to_owned(),
        size: create_bytes.len() as u64,
        chunks: create_written.chunk_manifests(),
        staged: create_written.staged_objects().unwrap(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    let prepared_replace = service
        .prepare_artifact_replace_path("/destination/inherited.bin")
        .unwrap();
    let replace_bytes = b"destination-v2";
    let replace_written = service
        .stage_prepared_artifact_ranges(
            &prepared_replace,
            "prepared-terminal/replace",
            &[PublishArtifactRange {
                offset: 0,
                bytes: replace_bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let replace_session = PublishArtifactStagedSession {
        parent: prepared_replace.parent,
        name: prepared_replace.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:prepared-terminal-replace".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "prepared-terminal/replace".to_owned(),
        size: replace_bytes.len() as u64,
        chunks: replace_written.chunk_manifests(),
        staged: replace_written.staged_objects().unwrap(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    let publish_before = metadata.applied_with_prefix(b"publish-artifact");
    metadata.fail_after_apply(b"publish-artifact", 0);
    let first_create = service
        .publish_prepared_artifact_staged_session(prepared_create.clone(), create_session.clone())
        .unwrap();
    assert_eq!(
        metadata.applied_with_prefix(b"publish-artifact"),
        publish_before + 1
    );

    let replace_before = metadata.applied_with_prefix(b"replace-artifact");
    metadata.fail_after_apply(b"replace-artifact", 0);
    let first_replace = service
        .publish_prepared_artifact_staged_session(prepared_replace.clone(), replace_session.clone())
        .unwrap();
    assert_eq!(
        metadata.applied_with_prefix(b"replace-artifact"),
        replace_before + 1
    );

    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
    assert_eq!(service.history_retention_floor().unwrap(), None);
    let historical_dentry = dentry_key(
        service.mount_id(),
        prepared_replace.parent,
        &prepared_replace.name,
    );
    let prepared_read_version = predecessor(Version::new(prepared_replace.generation).unwrap())
        .expect("prepared generation has a predecessor");
    for _ in 0..128 {
        service.cleanup_history(4_096).unwrap();
        if metadata
            .get_versioned(
                RecordFamily::Dentry,
                &historical_dentry,
                prepared_read_version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none()
        {
            break;
        }
    }
    assert!(metadata
        .get_versioned(
            RecordFamily::Dentry,
            &historical_dentry,
            prepared_read_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    service.rotate_object_gc_claim_for_failover().unwrap();

    let log_before_retry = service.sync_metadata_log_snapshot().unwrap();
    let stats_before_retry = service.metadata_store_stats();
    let restore_before_retry = service.restore_metrics().unwrap();
    let path_cache_epoch_before_retry = service.path_cache_epoch.load(Ordering::Acquire);
    let retry_create = service
        .publish_prepared_artifact_staged_session(prepared_create.clone(), create_session.clone())
        .unwrap();
    let retry_replace = service
        .publish_prepared_artifact_staged_session(prepared_replace.clone(), replace_session.clone())
        .unwrap();
    assert_eq!(retry_create.entry, first_create.entry);
    assert_eq!(retry_replace.entry, first_replace.entry);
    assert!(retry_create.replaced.is_none());
    assert!(retry_replace.replaced.is_none());
    assert_eq!(
        metadata.applied_with_prefix(b"publish-artifact"),
        publish_before + 1
    );
    assert_eq!(
        metadata.applied_with_prefix(b"replace-artifact"),
        replace_before + 1
    );
    assert_eq!(
        service.sync_metadata_log_snapshot().unwrap(),
        log_before_retry
    );
    let stats_after_retry = service.metadata_store_stats();
    assert_eq!(
        stats_after_retry.dedupe_write_total,
        stats_before_retry.dedupe_write_total
    );
    assert_eq!(
        stats_after_retry.current_put_total,
        stats_before_retry.current_put_total
    );
    assert_eq!(
        stats_after_retry.current_delete_total,
        stats_before_retry.current_delete_total
    );
    assert_eq!(
        stats_after_retry.history_write_total,
        stats_before_retry.history_write_total
    );
    assert_eq!(
        stats_after_retry.watch_write_total,
        stats_before_retry.watch_write_total
    );
    assert_eq!(service.restore_metrics().unwrap(), restore_before_retry);
    assert_eq!(
        service.path_cache_epoch.load(Ordering::Acquire),
        path_cache_epoch_before_retry,
        "terminal readback without metadata apply must not invalidate path caches"
    );
    assert_eq!(
        read_artifact_at_path(&service, "/destination/created.bin"),
        create_bytes
    );
    assert_eq!(
        read_artifact_at_path(&service, "/destination/inherited.bin"),
        replace_bytes
    );

    let inherited_index = path_index_key(
        service.mount_id(),
        &parse_absolute_path("/destination/inherited.bin").unwrap(),
    );
    let created_index = path_index_key(
        service.mount_id(),
        &parse_absolute_path("/destination/created.bin").unwrap(),
    );
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &inherited_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &created_index,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_some());
    let indexed = service
        .list_indexed_path_page("/destination", None, 16)
        .unwrap();
    assert_eq!(indexed.entries.len(), 2);
    assert!(service.fsck_restore_state(true).unwrap().is_consistent());

    let mut different_payload = create_session.clone();
    different_payload.mode = 0o600;
    assert!(matches!(
        service.publish_prepared_artifact_staged_session(
            prepared_create.clone(),
            different_payload,
        ),
        Err(MetadError::PublishArtifactFailed { source, .. })
            if matches!(*source, MetadError::StalePreparedArtifactObjectGcEpoch { .. })
    ));
    assert_eq!(
        service.sync_metadata_log_snapshot().unwrap(),
        log_before_retry
    );

    let log = service.sync_metadata_log_snapshot().unwrap();
    let segment_keys = snapshot_segment_keys(&log);
    let prepared_log_entries = log
        .segments
        .iter()
        .map(|pointer| {
            service
                .load_metadata_log_segment(&pointer.segment_key)
                .unwrap()
        })
        .flat_map(|segment| segment.entries)
        .filter(|entry| {
            entry.command.commit_version.get() == prepared_create.generation
                || entry.command.commit_version.get() == prepared_replace.generation
        })
        .collect::<Vec<_>>();
    assert_eq!(prepared_log_entries.len(), 2);
    assert!(prepared_log_entries.iter().all(|entry| {
        is_prepared_artifact_request_id(entry.command.kind, &entry.command.request_id)
    }));
    drop(service);

    let recovered_metadata = HoltMetadataStore::open_memory().unwrap();
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        recovered_metadata,
        objects.clone(),
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segment_keys,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();
    recovered
        .enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                "meta/prepared-terminal-log",
                "mount-1:/",
                2,
                log.durable_lsn,
                log.last_digest,
            )
            .with_segments(log.segments.clone())
            .with_durable_recovery_baseline(checkpoint.log_lsn, checkpoint.log_digest)
            .with_authoritative_recovery_baseline(),
        )
        .unwrap();
    let recovered_log_before = recovered.sync_metadata_log_snapshot().unwrap();
    assert_eq!(
        recovered
            .publish_prepared_artifact_staged_session(prepared_create, create_session)
            .unwrap()
            .entry,
        first_create.entry
    );
    assert_eq!(
        recovered
            .publish_prepared_artifact_staged_session(prepared_replace, replace_session)
            .unwrap()
            .entry,
        first_replace.entry
    );
    assert_eq!(
        recovered.sync_metadata_log_snapshot().unwrap(),
        recovered_log_before
    );
    assert_eq!(
        read_artifact_at_path(&recovered, "/destination/created.bin"),
        create_bytes
    );
    assert_eq!(
        read_artifact_at_path(&recovered, "/destination/inherited.bin"),
        replace_bytes
    );
    let report = recovered.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn complete_restore_prepared_retry_archives_pending_log_without_reapply() {
    let metadata = RestorePostApplyFaultStore::new();
    let objects = FaultObjectStore::new(MemoryObjectStore::new());
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/inherited.bin",
        "prepared-pending/source",
        b"source",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/prepared-pending-checkpoint", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/prepared-pending-log",
            "mount-1:/",
            1,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        ))
        .unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();
    let prepared = service
        .prepare_artifact_create_path("/destination/pending.bin")
        .unwrap();
    let bytes = b"pending-log-terminal";
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "prepared-pending/create",
            &[PublishArtifactRange {
                offset: 0,
                bytes: bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let session = PublishArtifactStagedSession {
        parent: prepared.parent,
        name: prepared.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:prepared-pending".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "prepared-pending/create".to_owned(),
        size: bytes.len() as u64,
        chunks: written.chunk_manifests(),
        staged: written.staged_objects().unwrap(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };
    let log_before = service.sync_metadata_log_snapshot().unwrap();
    let applies_before = metadata.applied_with_prefix(b"publish-artifact");

    objects.fail_puts_containing("meta/prepared-pending-log/log/");
    assert!(matches!(
        service.publish_prepared_artifact_staged_session(prepared.clone(), session.clone()),
        Err(MetadError::PublishArtifactFailed { source, .. })
            if matches!(*source, MetadError::SyncLogArchiveFailed { committed: true, .. })
    ));
    assert_eq!(
        metadata.applied_with_prefix(b"publish-artifact"),
        applies_before + 1
    );
    assert_eq!(
        service.sync_metadata_log_snapshot().unwrap().durable_lsn,
        log_before.durable_lsn
    );

    objects.clear_faults();
    let retry = service
        .publish_prepared_artifact_staged_session(prepared.clone(), session.clone())
        .unwrap();
    assert_eq!(retry.entry.attr.generation, prepared.generation);
    assert_eq!(
        metadata.applied_with_prefix(b"publish-artifact"),
        applies_before + 1,
        "retry must archive the pending command, not apply it again"
    );
    let archived = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(archived.durable_lsn, log_before.durable_lsn + 1);
    let matching_entries = archived
        .segments
        .iter()
        .map(|pointer| {
            service
                .load_metadata_log_segment(&pointer.segment_key)
                .unwrap()
        })
        .flat_map(|segment| segment.entries)
        .filter(|entry| entry.command.commit_version.get() == prepared.generation)
        .collect::<Vec<_>>();
    assert_eq!(matching_entries.len(), 1);
    assert!(is_prepared_artifact_request_id(
        matching_entries[0].command.kind,
        &matching_entries[0].command.request_id,
    ));
    let stable_log = archived.clone();
    service
        .publish_prepared_artifact_staged_session(prepared, session)
        .unwrap();
    assert_eq!(service.sync_metadata_log_snapshot().unwrap(), stable_log);
    assert_eq!(
        read_artifact_at_path(&service, "/destination/pending.bin"),
        bytes
    );
    assert!(service.fsck_restore_state(true).unwrap().is_consistent());

    let segment_keys = snapshot_segment_keys(&archived);
    drop(service);
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects,
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segment_keys,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        read_artifact_at_path(&recovered, "/destination/pending.bin"),
        bytes
    );
    let report = recovered.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn late_prepared_terminal_requires_complete_lsn_chain_after_checkpoint() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/destination", 0o755, 1000, 1000)
        .unwrap();

    let checkpoint_config = MetadataArchiveConfig::new("meta/late-prepared-checkpoint", 3);
    let initial = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/late-prepared-log",
            "mount-1:/",
            1,
            initial.log_lsn,
            initial.log_digest,
        ))
        .unwrap();

    let prepared = service
        .prepare_artifact_create_path("/destination/late.bin")
        .unwrap();
    let bytes = b"late-prepared-payload";
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "late-prepared",
            &[PublishArtifactRange {
                offset: 0,
                bytes: bytes.to_vec(),
            }],
            0,
        )
        .unwrap();
    let session = PublishArtifactStagedSession {
        parent: prepared.parent,
        name: prepared.name.clone(),
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:late-prepared".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: "late-prepared".to_owned(),
        size: bytes.len() as u64,
        chunks: written.chunk_manifests(),
        staged: written.staged_objects().unwrap(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    for index in 0..4 {
        service
            .create_dir_path(&format!("/advance-{index}"), 0o755, 1000, 1000)
            .unwrap();
    }
    let baseline = service.backup_metadata(&checkpoint_config).unwrap();
    assert!(prepared.generation < baseline.commit_version);

    service
        .create_dir_path("/after-baseline", 0o755, 1000, 1000)
        .unwrap();
    let first = service
        .publish_prepared_artifact_staged_session(prepared.clone(), session.clone())
        .unwrap();
    let published_log = service.sync_metadata_log_snapshot().unwrap();
    assert_eq!(service.prepared_terminal_proof_cache_len(), Some(1));

    let prepared_pointer = published_log
        .segments
        .iter()
        .find(|pointer| {
            service
                .load_metadata_log_segment(&pointer.segment_key)
                .unwrap()
                .entries
                .iter()
                .any(|entry| {
                    entry.command.commit_version.get() == prepared.generation
                        && is_prepared_artifact_request_id(
                            entry.command.kind,
                            &entry.command.request_id,
                        )
                })
        })
        .cloned()
        .expect("prepared publish segment");
    let prior_active_pointer = published_log
        .segments
        .iter()
        .find(|pointer| {
            pointer.first_lsn > baseline.log_lsn && pointer.last_lsn < prepared_pointer.first_lsn
        })
        .cloned()
        .expect("active segment before prepared publish");

    for pointer in [&prior_active_pointer, &prepared_pointer] {
        let key = ObjectKey::new(pointer.segment_key.clone()).unwrap();
        let original = objects.get(&key, None).unwrap();
        objects.put(&key, b"corrupt-log-segment".to_vec()).unwrap();
        assert!(service
            .publish_prepared_artifact_staged_session(prepared.clone(), session.clone())
            .is_err());
        assert_eq!(service.prepared_terminal_proof_cache_len(), Some(1));
        objects.put(&key, original).unwrap();
    }

    let log_before_bad_ack = service.sync_metadata_log_snapshot().unwrap();
    let proof_count_before_bad_ack = service.prepared_terminal_proof_cache_len();
    let mut wrong_digest = log_before_bad_ack.last_digest;
    wrong_digest[0] ^= 0xff;
    assert!(service
        .acknowledge_published_metadata_checkpoint(log_before_bad_ack.durable_lsn, wrong_digest,)
        .is_err());
    assert_eq!(
        service.sync_metadata_log_snapshot().unwrap(),
        log_before_bad_ack
    );
    assert_eq!(
        service.prepared_terminal_proof_cache_len(),
        proof_count_before_bad_ack
    );

    service
        .acknowledge_published_metadata_checkpoint(baseline.log_lsn, baseline.log_digest)
        .unwrap();
    assert_eq!(service.prepared_terminal_proof_cache_len(), Some(1));

    let checkpoint_only = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    checkpoint_only
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &[],
            baseline.log_lsn,
            baseline.log_digest,
        )
        .unwrap()
        .unwrap();
    assert!(checkpoint_only
        .lookup_path("/destination/late.bin")
        .unwrap()
        .is_none());

    let tail_segment_keys = published_log
        .segments
        .iter()
        .filter(|pointer| pointer.first_lsn > baseline.log_lsn)
        .map(|pointer| pointer.segment_key.clone())
        .collect::<Vec<_>>();
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &tail_segment_keys,
            baseline.log_lsn,
            baseline.log_digest,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        read_artifact_at_path(&recovered, "/destination/late.bin"),
        bytes
    );

    service.disable_sync_metadata_log().unwrap();
    service
        .enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                "meta/late-prepared-log",
                "mount-1:/",
                2,
                published_log.durable_lsn,
                published_log.last_digest,
            )
            .with_segments(published_log.segments.clone())
            .with_durable_recovery_baseline(baseline.log_lsn, baseline.log_digest)
            .with_authoritative_recovery_baseline(),
        )
        .unwrap();
    assert_eq!(service.prepared_terminal_proof_cache_len(), Some(0));
    assert_eq!(
        service
            .publish_prepared_artifact_staged_session(prepared.clone(), session.clone())
            .unwrap()
            .entry,
        first.entry
    );

    service.disable_sync_metadata_log().unwrap();
    objects
        .delete(&ObjectKey::new(prepared_pointer.segment_key).unwrap())
        .unwrap();
    service
        .enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                "meta/late-prepared-log",
                "mount-1:/",
                3,
                published_log.durable_lsn,
                published_log.last_digest,
            )
            .with_segments(published_log.segments)
            .with_durable_recovery_baseline(baseline.log_lsn, baseline.log_digest)
            .with_authoritative_recovery_baseline(),
        )
        .unwrap();
    assert!(service
        .publish_prepared_artifact_staged_session(prepared, session)
        .is_err());
}

fn restore_test_initialization() -> RestoreInitialization {
    RestoreInitialization {
        remove_relative_paths: Vec::new(),
        files: vec![RestoreInitializationFile {
            relative_path: "metadata/restore_manifest.json".to_owned(),
            bytes: br#"{"schema":"nokv.workbench.restore_manifest.v1"}"#.to_vec(),
            content_type: "application/json".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        }],
    }
}

fn retry_restore_until_complete<M, O>(
    service: &NoKvFs<M, O>,
    snapshot_id: u64,
    initialization: &RestoreInitialization,
) -> RestoreOutcome
where
    M: MetadataStore,
    O: ObjectStore,
{
    for _ in 0..4096 {
        match service.restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot_id,
            "/destination",
            initialization.clone(),
        ) {
            Ok(outcome) => return outcome,
            Err(MetadError::RestoreInProgress) => {}
            Err(error) => panic!("restore retry failed: {error}"),
        }
    }
    panic!("bounded restore retry did not converge")
}

fn durable_restore_operation<M, O>(
    service: &NoKvFs<M, O>,
    snapshot_id: u64,
) -> restore::RestoreOperation
where
    M: MetadataStore,
    O: ObjectStore,
{
    let digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot_id,
        "/destination",
    );
    restore::decode_restore_operation(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_operation_key(service.mount_id(), &digest),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .expect("restore operation must be durable")
            .0,
    )
    .unwrap()
}

fn replace_durable_restore_operation<M, O>(
    service: &NoKvFs<M, O>,
    durable_key_digest: [u8; 32],
    operation: &restore::RestoreOperation,
) where
    M: MetadataStore,
    O: ObjectStore,
{
    let operation_key = restore::restore_operation_key(service.mount_id(), &durable_key_digest);
    let operation_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &operation_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .expect("restore operation must be durable");
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-replace-restore-operation",
                service.mount_id(),
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: operation_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(operation_item.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: operation_key,
                op: MutationOp::Put,
                value: Some(Value(restore::encode_restore_operation(operation).unwrap())),
            }],
            watch: Vec::new(),
        })
        .unwrap();
}

#[test]
fn restore_operation_identity_rejects_a_corrupted_snapshot_id_on_durable_read_paths() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/source/empty.txt", 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );
    let mut corrupted = durable_restore_operation(&service, snapshot.snapshot_id);
    corrupted.snapshot_id = corrupted.snapshot_id.checked_add(1).unwrap();
    replace_durable_restore_operation(&service, operation_digest, &corrupted);

    assert!(matches!(
        service.restore_subtree_path_to_fork(
            "/source",
            snapshot.snapshot_id,
            "/destination",
        ),
        Err(MetadError::Codec(message))
            if message.contains("request identity")
    ));
    assert!(matches!(
        service.is_complete_restore_root(restored.destination_root),
        Err(MetadError::Codec(message))
            if message.contains("request identity")
    ));
    assert!(matches!(
        service.recover_restore_staging_visibility(),
        Err(MetadError::Codec(message))
            if message.contains("request identity")
    ));

    let report = service.fsck_restore_state(false).unwrap();
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.code == "operation_identity_mismatch"));
}

#[test]
fn restore_operation_identity_rejects_a_corrupted_value_digest() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/source/empty.txt", 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );
    let mut corrupted = durable_restore_operation(&service, snapshot.snapshot_id);
    corrupted.operation_digest = [0xa5; 32];
    assert_ne!(corrupted.operation_digest, operation_digest);
    replace_durable_restore_operation(&service, operation_digest, &corrupted);

    assert!(matches!(
        service.restore_subtree_path_to_fork(
            "/source",
            snapshot.snapshot_id,
            "/destination",
        ),
        Err(MetadError::Codec(message))
            if message.contains("durable key")
    ));
    assert!(matches!(
        service.is_complete_restore_root(restored.destination_root),
        Err(MetadError::Codec(message))
            if message.contains("durable key")
    ));
    assert!(matches!(
        service.recover_restore_staging_visibility(),
        Err(MetadError::Codec(message))
            if message.contains("durable key")
    ));

    let report = service.fsck_restore_state(false).unwrap();
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.code == "operation_identity_mismatch"));
}

#[test]
fn concurrent_identical_restore_requests_create_one_root_ref_set_and_manifest_put() {
    let metadata = RestorePostApplyFaultStore::new();
    let objects = RestorePostPutFaultStore::new(MemoryObjectStore::new());
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let initialization = restore_test_initialization();
    objects.clear_put_history();
    let start = Arc::new(Barrier::new(17));
    let mut calls = Vec::new();
    for _ in 0..16 {
        let service = Arc::clone(&service);
        let start = Arc::clone(&start);
        let initialization = initialization.clone();
        calls.push(std::thread::spawn(move || {
            start.wait();
            service.restore_subtree_path_to_fork_initialized(
                "/source",
                snapshot.snapshot_id,
                "/destination",
                initialization,
            )
        }));
    }
    start.wait();
    let outcomes = calls
        .into_iter()
        .map(|call| call.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert!(outcomes.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(outcomes[0].state, RestoreState::Complete);
    assert_eq!(
        metadata.applied_with_prefix(b"restore-install-hold"),
        1,
        "sixteen simultaneous callers must install one durable hold/root"
    );
    assert_eq!(
        metadata.applied_with_prefix(b"restore-start-base-refs"),
        1,
        "one ref-set build must own the terminal operation"
    );
    assert_eq!(metadata.applied_with_prefix(b"restore-seal-base-refs"), 1);
    assert_eq!(
        metadata.applied_with_prefix(b"restore-init-upload-intent"),
        1
    );
    assert_eq!(metadata.applied_with_prefix(b"restore-init-publish"), 1);
    let put_keys = objects.put_keys();
    assert_eq!(
        put_keys.len(),
        1,
        "the deterministic restore manifest must be uploaded once"
    );
    assert!(put_keys[0].as_str().starts_with("blocks/"));

    let metrics = service.restore_metrics().unwrap();
    assert_eq!(metrics.complete, 1);
    for keyspace in ["operation", "destination_claim", "root_index", "base_seal"] {
        assert_eq!(
            metrics.control_rows.get(keyspace),
            Some(&1),
            "restore must leave exactly one {keyspace} row"
        );
    }
    assert!(service.fsck_restore_state(true).unwrap().is_consistent());
}

fn exercise_restore_post_apply_crash(
    request_prefix: &[u8],
    zero_based_match: usize,
    empty_entry_count: usize,
    artifact_count: usize,
    expected_state: restore::RestoreOperationState,
    rebuild_expected: bool,
) {
    let metadata = RestorePostApplyFaultStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    if empty_entry_count > 0 {
        let names = (0..empty_entry_count)
            .map(|index| DentryName::new(format!("empty-{index:04}.bin")).unwrap())
            .collect();
        service
            .create_files_in_dir_path("/source", names, 0o644, 1000, 1000)
            .unwrap();
    }
    for index in 0..artifact_count {
        publish_path_artifact(
            &service,
            &format!("/source/artifact-{index:03}.bin"),
            &format!("restore/crash-reference-{index:03}"),
            format!("payload-{index:03}").as_bytes(),
        );
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    metadata.fail_after_apply(request_prefix, zero_based_match);

    let error = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap_err();
    assert!(
        matches!(
            &error,
            MetadError::Metadata(MetadataError::Backend(message))
                if message == "injected restore crash after durable apply"
        ),
        "unexpected crash result for {} occurrence {zero_based_match}: {error:?}",
        String::from_utf8_lossy(request_prefix),
    );
    assert_eq!(
        metadata.applied_with_prefix(request_prefix),
        zero_based_match + 1
    );
    let crashed = durable_restore_operation(&service, snapshot.snapshot_id);
    assert_eq!(crashed.state, expected_state);
    if expected_state == restore::RestoreOperationState::Complete {
        assert!(service.lookup_path("/destination").unwrap().is_some());
    } else {
        assert!(service.lookup_path("/destination").unwrap().is_none());
        assert!(service
            .get_attr(crashed.destination_root)
            .unwrap()
            .is_none());
    }
    drop(service);

    metadata.clear_fault();
    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0).unwrap();
    let reopened_operation = durable_restore_operation(&reopened, snapshot.snapshot_id);
    assert_eq!(
        reopened_operation.state, expected_state,
        "reopen must not advance the restore state machine"
    );
    if expected_state != restore::RestoreOperationState::Complete {
        assert!(reopened.lookup_path("/destination").unwrap().is_none());
    }
    let outcome = retry_restore_until_complete(
        &reopened,
        snapshot.snapshot_id,
        &RestoreInitialization::default(),
    );
    assert_eq!(outcome.state, RestoreState::Complete);
    if rebuild_expected {
        assert_ne!(
            outcome.destination_root, crashed.destination_root,
            "Preparing recovery must discard and rebuild its detached root"
        );
    } else {
        assert_eq!(
            outcome.destination_root, crashed.destination_root,
            "sealed ReadyToAttach/Complete recovery must retain its durable root"
        );
    }
    assert!(reopened.lookup_path("/destination").unwrap().is_some());
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_post_apply_crashes_rebuild_hold_and_four_materialization_batches() {
    exercise_restore_post_apply_crash(
        b"restore-install-hold",
        0,
        1,
        0,
        restore::RestoreOperationState::Preparing,
        true,
    );
    for batch in 0..4 {
        exercise_restore_post_apply_crash(
            b"restore-materialize-batch",
            batch,
            257,
            0,
            restore::RestoreOperationState::Preparing,
            true,
        );
    }
}

#[test]
fn restore_post_apply_crashes_rebuild_index_and_reference_seals() {
    // Reference-page apply-then-ACK loss is authoritatively reconciled from
    // the durable build cursor; the adaptive byte-budget test below covers
    // that path through reopen. Seal commits still surface the injected error
    // and require the explicit Preparing cleanup/rebuild contract.
    exercise_restore_post_apply_crash(
        b"restore-seal-index",
        0,
        3,
        0,
        restore::RestoreOperationState::Preparing,
        true,
    );
    exercise_restore_post_apply_crash(
        b"restore-seal-base-refs",
        0,
        0,
        130,
        restore::RestoreOperationState::Preparing,
        true,
    );
}

#[test]
fn restore_reopen_only_attaches_sealed_ready_state_on_explicit_retry() {
    exercise_restore_post_apply_crash(
        b"restore-ready-to-attach",
        0,
        3,
        0,
        restore::RestoreOperationState::ReadyToAttach,
        false,
    );
}

#[test]
fn restore_attach_ack_loss_reopens_at_the_exact_terminal_outcome() {
    exercise_restore_post_apply_crash(
        b"restore-attach-destination",
        0,
        3,
        0,
        restore::RestoreOperationState::Complete,
        false,
    );
}

#[test]
fn restore_attach_publication_failure_stays_committed_until_exact_retry() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "metadata/restore-attach-publication-failure",
            "mount-1:/",
            1,
            0,
            METADATA_LOG_ZERO_DIGEST,
        ))
        .unwrap();
    let attach_publication_failures = Arc::new(AtomicU64::new(0));
    let observed_failures = Arc::clone(&attach_publication_failures);
    service
        .install_sync_metadata_log_publication_hook(move |_| {
            let operation = metadata
                .get(
                    RecordFamily::System,
                    &restore::restore_operation_key(MountId::new(1).unwrap(), &operation_digest),
                    Version::new(u64::MAX).unwrap(),
                    ReadPurpose::WritePlanLocal,
                )
                .map_err(|error| error.to_string())?
                .map(|value| restore::decode_restore_operation(&value.0))
                .transpose()
                .map_err(|error| error.to_string())?;
            if operation.is_some_and(|operation| {
                operation.state == restore::RestoreOperationState::Complete
            }) {
                observed_failures.fetch_add(1, Ordering::SeqCst);
                return Err("injected attach control publication failure".to_owned());
            }
            Ok(())
        })
        .unwrap();
    live_test_barrier::reset_test_attach_applied_calls();

    let error = service
        .with_immediate_sync_metadata_log_publication(|| {
            service.restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        })
        .unwrap_err();
    assert!(matches!(
        &error,
        MetadError::SyncLogArchiveFailed {
            committed: true,
            message,
        } if message.contains("injected attach control publication failure")
    ));
    assert_eq!(attach_publication_failures.load(Ordering::SeqCst), 1);
    assert_eq!(live_test_barrier::test_attach_applied_calls(), 0);
    assert_eq!(
        durable_restore_operation(&service, snapshot.snapshot_id).state,
        restore::RestoreOperationState::Complete
    );
    assert!(service.lookup_path("/destination").unwrap().is_some());

    let retry = service
        .with_immediate_sync_metadata_log_publication(|| {
            service.restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        })
        .unwrap();
    assert_eq!(retry.state, RestoreState::Complete);
    assert_eq!(attach_publication_failures.load(Ordering::SeqCst), 1);
    assert_eq!(live_test_barrier::test_attach_applied_calls(), 0);
}

#[test]
fn restore_initialization_post_put_crash_rebuilds_without_reusing_the_orphan() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = RestorePostPutFaultStore::new(MemoryObjectStore::new());
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let initialization = restore_test_initialization();
    objects.clear_put_history();
    objects.fail_after_put(0);

    let error = service
        .restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            initialization.clone(),
        )
        .unwrap_err();
    assert!(
        matches!(
            &error,
            MetadError::Object(ObjectError::StagedWriteFailed { source, staged })
                if source.contains("injected restore crash after object PUT") && staged.is_empty()
        ),
        "unexpected initialization crash result: {error:?}"
    );
    let crashed = durable_restore_operation(&service, snapshot.snapshot_id);
    assert_eq!(crashed.state, restore::RestoreOperationState::Preparing);
    let orphan = objects.put_keys().into_iter().next().unwrap();
    assert!(objects.head(&orphan).unwrap().is_some());
    drop(service);

    let reopened =
        NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects.clone(), 0).unwrap();
    assert_eq!(
        durable_restore_operation(&reopened, snapshot.snapshot_id).state,
        restore::RestoreOperationState::Preparing
    );
    assert!(reopened.lookup_path("/destination").unwrap().is_none());
    let outcome = retry_restore_until_complete(&reopened, snapshot.snapshot_id, &initialization);
    assert_ne!(outcome.destination_root, crashed.destination_root);
    let puts = objects.put_keys();
    assert_eq!(puts.len(), 2);
    assert_ne!(puts[0], puts[1]);
    assert!(objects.head(&orphan).unwrap().is_none());
    assert!(objects.head(&puts[1]).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&reopened, "/destination/metadata/restore_manifest.json"),
        br#"{"schema":"nokv.workbench.restore_manifest.v1"}"#
    );
    assert!(reopened.fsck_restore_state(true).unwrap().is_consistent());
}

#[test]
fn restore_materialization_rejects_gc_epoch_rotation_then_rebuilds_exact_refs() {
    let metadata = SnapshotCommitBarrierStore::new(CommandKind::CreateFiles, 1, 2)
        .matching_request_prefix(b"restore-materialize-batch");
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let source = publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/gc-epoch-race",
        b"borrowed across GC epoch",
    );
    let source_body = source.body.as_ref().unwrap();
    let shared_object = block_key(source.attr.inode, source_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree_path("/source").unwrap();

    let restore = {
        let service = Arc::clone(&service);
        std::thread::spawn(move || {
            service.restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        })
    };
    metadata.wait_until_blocked();
    service.rotate_object_gc_claim_for_failover().unwrap();
    metadata.release_blocked();
    assert!(matches!(
        restore.join().unwrap(),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
    let interrupted = durable_restore_operation(&service, snapshot.snapshot_id);
    assert_eq!(interrupted.state, restore::RestoreOperationState::Preparing);
    assert!(service.lookup_path("/destination").unwrap().is_none());

    let completed = retry_restore_until_complete(
        &service,
        snapshot.snapshot_id,
        &RestoreInitialization::default(),
    );
    assert_ne!(completed.destination_root, interrupted.destination_root);
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service.remove_file_path("/source/shared.bin").unwrap();
    service.remove_empty_dir_path("/source").unwrap();
    for _ in 0..64 {
        service.cleanup_pending_objects(1).unwrap();
    }
    assert!(objects.head(&shared_object).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/destination/shared.bin"),
        b"borrowed across GC epoch"
    );

    service.remove_file_path("/destination/shared.bin").unwrap();
    service.remove_empty_dir_path("/destination").unwrap();
    for _ in 0..256 {
        service.cleanup_restore_releases(1).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    for _ in 0..256 {
        service.cleanup_pending_objects(1).unwrap();
        if objects.head(&shared_object).unwrap().is_none() {
            break;
        }
    }
    assert!(objects.head(&shared_object).unwrap().is_none());
    assert!(service.fsck_restore_state(true).unwrap().is_consistent());
}

fn load_completed_restore_operation_and_seal(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    source_path: &str,
    snapshot_id: u64,
    destination_path: &str,
) -> (restore::RestoreOperation, restore_gc::RestoreBaseSeal) {
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        source_path,
        snapshot_id,
        destination_path,
    );
    let version = service.read_version().unwrap();
    let operation = restore::decode_restore_operation(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_operation_key(service.mount_id(), &operation_digest),
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .expect("completed restore operation must be durable")
            .0,
    )
    .unwrap();
    assert_eq!(operation.state, restore::RestoreOperationState::Complete);
    let seal = restore_gc::decode_restore_base_seal(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_base_seal_key(service.mount_id(), operation.ref_set_id),
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .expect("completed restore must have a sealed exact-reference set")
            .0,
    )
    .unwrap();
    (operation, seal)
}

#[test]
fn restore_exact_references_cross_multiple_durable_batches() {
    const REFERENCE_COUNT: usize = 130;

    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    for index in 0..REFERENCE_COUNT {
        publish_path_artifact(
            &service,
            &format!("/source/artifact-{index:03}.bin"),
            &format!("restore/reference-batch-{index:03}"),
            format!("payload-{index:03}").as_bytes(),
        );
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let (_, seal) = load_completed_restore_operation_and_seal(
        &service,
        "/source",
        snapshot.snapshot_id,
        "/restored",
    );
    assert_eq!(seal.reference_count, REFERENCE_COUNT as u64);
    let metrics = service.restore_metrics().unwrap();
    for keyspace in ["base_owner", "base_inverse", "base_inverse_owner"] {
        assert_eq!(
            metrics.control_rows.get(keyspace),
            Some(&REFERENCE_COUNT),
            "every exact-reference projection must cross the 64-row batch boundary"
        );
    }
    assert_eq!(
        read_artifact_at_path(&service, "/restored/artifact-129.bin"),
        b"payload-129"
    );
    let report = service.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_release_batches_retained_reference_cursor_across_pages() {
    const REFERENCE_COUNT: usize = 65;

    let metadata = RestorePostApplyFaultStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let mut source_objects = Vec::with_capacity(REFERENCE_COUNT);
    for index in 0..REFERENCE_COUNT {
        let artifact = publish_path_artifact(
            &service,
            &format!("/source/artifact-{index:03}.bin"),
            &format!("restore/retained-reference-{index:03}"),
            format!("payload-{index:03}").as_bytes(),
        );
        let body = artifact.body.as_ref().unwrap();
        source_objects.push(block_key(artifact.attr.inode, body.generation, 0, 0));
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    let seal = restore_gc::decode_restore_base_seal(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_base_seal_key(service.mount_id(), operation.ref_set_id),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .expect("completed restore must have a sealed exact-reference set")
            .0,
    )
    .unwrap();
    assert_eq!(seal.reference_count, REFERENCE_COUNT as u64);

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service
        .create_dir_path("/escaped", 0o755, 1000, 1000)
        .unwrap();
    for index in 0..REFERENCE_COUNT {
        service
            .rename_path(
                &format!("/destination/artifact-{index:03}.bin"),
                &format!("/escaped/artifact-{index:03}.bin"),
            )
            .unwrap();
        service
            .remove_file_path(&format!("/source/artifact-{index:03}.bin"))
            .unwrap();
    }
    service.remove_empty_dir_path("/source").unwrap();
    for _ in 0..4 {
        service.cleanup_pending_objects(REFERENCE_COUNT).unwrap();
    }
    assert!(source_objects
        .iter()
        .all(|object_key| objects.head(object_key).unwrap().is_some()));

    service.remove_empty_dir_path("/destination").unwrap();
    let mount = service.mount_id();
    let release_key = restore::restore_release_job_key(mount, operation.ref_set_id);
    let load_job = || {
        service
            .metadata_store()
            .get_versioned(
                RecordFamily::System,
                &release_key,
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .map(|item| {
                (
                    restore::decode_restore_release_job(&item.value.0).unwrap(),
                    item.version,
                )
            })
            .expect("detached restore root must create a release job")
    };
    let owner_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_base_owner_prefix(mount, operation.ref_set_id),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(owner_rows.len(), REFERENCE_COUNT);
    let (initial_job, initial_job_version) = load_job();
    assert_eq!(
        initial_job.phase,
        restore::RestoreReleasePhase::ExactReferences
    );
    assert!(initial_job.cursor.is_empty());

    let update_before = metadata.applied_with_prefix(b"restore-update-release-job");
    let reference_release_before = metadata.applied_with_prefix(b"restore-release-reference");
    let commits_before = service.metadata_store_stats().commit_total;
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(
        metadata.applied_with_prefix(b"restore-update-release-job") - update_before,
        1,
        "one full page of retained references must persist one release-job cursor"
    );
    assert_eq!(
        metadata.applied_with_prefix(b"restore-release-reference"),
        reference_release_before,
        "retained rows must not issue per-object release commands"
    );
    assert_eq!(
        service.metadata_store_stats().commit_total - commits_before,
        2,
        "one retained-reference page writes one job cursor and one mount worker cursor"
    );
    let (first_page_job, first_page_version) = load_job();
    assert_eq!(
        first_page_job.phase,
        restore::RestoreReleasePhase::ExactReferences
    );
    assert_eq!(first_page_job.cursor, owner_rows[63].key);
    assert!(first_page_version > initial_job_version);

    let commits_before = service.metadata_store_stats().commit_total;
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(
        metadata.applied_with_prefix(b"restore-update-release-job") - update_before,
        2,
        "the retained reference beyond the first page must advance the release job"
    );
    assert_eq!(
        metadata.applied_with_prefix(b"restore-release-reference"),
        reference_release_before
    );
    assert_eq!(
        service.metadata_store_stats().commit_total - commits_before,
        2
    );
    let (second_page_job, second_page_version) = load_job();
    assert_eq!(second_page_job.phase, restore::RestoreReleasePhase::Members);
    assert!(second_page_job.cursor.is_empty());
    assert!(second_page_version > first_page_version);

    let retained_owner_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_base_owner_prefix(mount, operation.ref_set_id),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(retained_owner_rows.len(), REFERENCE_COUNT);
    assert!(source_objects
        .iter()
        .all(|object_key| objects.head(object_key).unwrap().is_some()));
    for index in 0..REFERENCE_COUNT {
        assert_eq!(
            read_artifact_at_path(&service, &format!("/escaped/artifact-{index:03}.bin")),
            format!("payload-{index:03}").as_bytes()
        );
    }
}

#[test]
fn restore_release_batches_nonretained_references_across_pages() {
    const REFERENCE_COUNT: usize = 65;

    let metadata = RestorePostApplyFaultStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let mut source_objects = Vec::with_capacity(REFERENCE_COUNT);
    for index in 0..REFERENCE_COUNT {
        let artifact = publish_path_artifact(
            &service,
            &format!("/source/artifact-{index:03}.bin"),
            &format!("restore/nonretained-reference-{index:03}"),
            format!("payload-{index:03}").as_bytes(),
        );
        let body = artifact.body.as_ref().unwrap();
        source_objects.push(block_key(artifact.attr.inode, body.generation, 0, 0));
    }
    let expected_objects = source_objects
        .iter()
        .map(|key| key.as_str().to_owned())
        .collect::<HashSet<_>>();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);

    let mount = service.mount_id();
    let release_key = restore::restore_release_job_key(mount, operation.ref_set_id);
    let load_job = || {
        restore::decode_restore_release_job(
            &service
                .metadata_store()
                .get(
                    RecordFamily::System,
                    &release_key,
                    service.read_version().unwrap(),
                    ReadPurpose::WritePlanLocal,
                )
                .unwrap()
                .expect("detached restore root must create a release job")
                .0,
        )
        .unwrap()
    };
    let scan_owner_rows = || {
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore::restore_base_owner_prefix(mount, operation.ref_set_id),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
    };
    let queued_objects = || {
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .into_iter()
            .map(|row| decode_object_gc_record(&row.value.0).unwrap().object_key)
            .filter(|object_key| expected_objects.contains(object_key))
            .collect::<Vec<_>>()
    };

    let owner_rows = scan_owner_rows();
    assert_eq!(owner_rows.len(), REFERENCE_COUNT);
    let apply_before = metadata.applied_with_prefix(b"restore-release-reference-batch");
    let mutation_counts_before = metadata
        .applied_mutation_counts_with_prefix(b"restore-release-reference-batch")
        .len();
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(
        metadata.applied_with_prefix(b"restore-release-reference-batch") - apply_before,
        1,
        "one cleanup pass must release the first 64 references in one command"
    );
    let mutation_counts =
        metadata.applied_mutation_counts_with_prefix(b"restore-release-reference-batch");
    assert_eq!(
        mutation_counts[mutation_counts_before],
        1 + 3 * restore::RESTORE_BATCH_ENTRIES + restore::RESTORE_BATCH_ENTRIES,
        "the first page contains one job cursor, 64 exact-reference triples, and 64 GC rows"
    );
    let remaining = scan_owner_rows();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, owner_rows[64].key);
    let first_job = load_job();
    assert_eq!(
        first_job.phase,
        restore::RestoreReleasePhase::ExactReferences
    );
    assert_eq!(first_job.cursor, owner_rows[63].key);
    let first_queued = queued_objects();
    assert_eq!(first_queued.len(), restore::RESTORE_BATCH_ENTRIES);
    assert_eq!(
        first_queued.iter().collect::<HashSet<_>>().len(),
        restore::RESTORE_BATCH_ENTRIES,
        "the first page must enqueue each canonical object once"
    );

    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(
        metadata.applied_with_prefix(b"restore-release-reference-batch") - apply_before,
        2,
        "the final reference must be released by one second batch"
    );
    let mutation_counts =
        metadata.applied_mutation_counts_with_prefix(b"restore-release-reference-batch");
    assert_eq!(
        mutation_counts[mutation_counts_before + 1],
        1 + 3 + 1,
        "the tail page contains one job cursor, one exact-reference triple, and one GC row"
    );
    assert!(scan_owner_rows().is_empty());
    let final_job = load_job();
    assert_eq!(final_job.phase, restore::RestoreReleasePhase::Members);
    assert!(final_job.cursor.is_empty());
    let queued = queued_objects();
    assert_eq!(queued.len(), REFERENCE_COUNT);
    assert_eq!(queued.iter().collect::<HashSet<_>>().len(), REFERENCE_COUNT);
    assert!(source_objects
        .iter()
        .all(|object_key| objects.head(object_key).unwrap().is_some()));
}

#[test]
fn restore_release_retries_a_borrower_manifest_backend_read_without_quarantine() {
    let metadata = RestoreBorrowerManifestReadFaultStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let source = publish_path_artifact(
        &service,
        "/source/artifact.bin",
        "restore/retry-borrower-manifest-source",
        b"source generation",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_object = block_key(source.attr.inode, source_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    let borrower = service
        .lookup_path("/destination/artifact.bin")
        .unwrap()
        .expect("restored borrower must be visible");

    // Keep the borrower inode reachable after detaching the restore root, but
    // move it to a new generation that no longer references the restored base
    // object. Release must read this manifest before it can drop the exact ref.
    let prepared = service
        .prepare_artifact_replace_path("/destination/artifact.bin")
        .unwrap();
    let replacement = service
        .publish_prepared_artifact_session(
            prepared.clone(),
            PublishArtifactSession {
                parent: prepared.parent,
                name: prepared.name,
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "restore/retry-borrower-manifest-replacement".to_owned(),
                size: b"replacement generation".len() as u64,
                ranges: vec![PublishArtifactRange {
                    offset: 0,
                    bytes: b"replacement generation".to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap()
        .entry;
    assert_eq!(replacement.attr.inode, borrower.attr.inode);
    let replacement_body = replacement.body.as_ref().unwrap();
    let borrower_manifest_key = chunk_manifest_key(
        service.mount_id(),
        replacement.attr.inode,
        replacement_body.generation,
        0,
    );
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::ChunkManifest,
            &borrower_manifest_key,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service
        .create_dir_path("/escaped", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_path("/destination/artifact.bin", "/escaped/artifact.bin")
        .unwrap();
    service.remove_empty_dir_path("/destination").unwrap();

    let mount = service.mount_id();
    let release_key = restore::restore_release_job_key(mount, operation.ref_set_id);
    let load_job = || {
        service
            .metadata_store()
            .get_versioned(
                RecordFamily::System,
                &release_key,
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .map(|item| {
                (
                    restore::decode_restore_release_job(&item.value.0).unwrap(),
                    item.version,
                )
            })
            .expect("detached restore root must create a release job")
    };
    let owner_rows = || {
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore::restore_base_owner_prefix(mount, operation.ref_set_id),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
    };
    let quarantines = || {
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore::restore_release_quarantine_prefix(mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
    };

    let initial_owners = owner_rows();
    assert_eq!(initial_owners.len(), 1);
    let (initial_job, initial_job_version) = load_job();
    assert_eq!(
        initial_job.phase,
        restore::RestoreReleasePhase::ExactReferences
    );
    assert!(initial_job.cursor.is_empty());
    assert!(quarantines().is_empty());

    metadata.fail_next_restore_manifest_read(borrower_manifest_key);
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(metadata.failures(), 1);
    let (failed_job, failed_job_version) = load_job();
    assert_eq!(failed_job.phase, initial_job.phase);
    assert_eq!(failed_job.cursor, initial_job.cursor);
    assert_eq!(failed_job_version, initial_job_version);
    let failed_owners = owner_rows();
    assert_eq!(failed_owners.len(), 1);
    assert_eq!(failed_owners[0].key, initial_owners[0].key);
    assert_eq!(failed_owners[0].version, initial_owners[0].version);
    assert!(quarantines().is_empty());

    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(metadata.failures(), 1);
    assert!(owner_rows().is_empty());
    let (released_job, released_job_version) = load_job();
    assert_eq!(released_job.phase, restore::RestoreReleasePhase::Members);
    assert!(released_job.cursor.is_empty());
    assert!(released_job_version > failed_job_version);
    assert!(quarantines().is_empty());
    let queued = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert!(queued.into_iter().any(|row| {
        decode_object_gc_record(&row.value.0)
            .is_ok_and(|record| record.object_key == source_object.as_str())
    }));
    assert_eq!(
        read_artifact_at_path(&service, "/escaped/artifact.bin"),
        b"replacement generation"
    );
    assert!(objects.head(&source_object).unwrap().is_some());
}

#[test]
fn restore_release_deduplicates_gc_for_distinct_borrowers_of_one_object() {
    let metadata = RestorePostApplyFaultStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/shared.bin",
        "restore/release-shared-object",
        b"shared object bytes",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_object = block_key(source.attr.inode, source_body.generation, 0, 0);
    service.create_dir_path("/both", 0o755, 1000, 1000).unwrap();
    service
        .clone_subtree_path_into("/base", "/both/one")
        .unwrap();
    service
        .clone_subtree_path_into("/base", "/both/two")
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/both").unwrap();
    service
        .restore_subtree_path_to_fork("/both", snapshot.snapshot_id, "/restored")
        .unwrap();
    assert_eq!(
        service.restore_metrics().unwrap().control_rows["base_owner"],
        2
    );
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/restored")
        .unwrap();
    let mutation_counts_before = metadata
        .applied_mutation_counts_with_prefix(b"restore-release-reference-batch")
        .len();
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    let mutation_counts =
        metadata.applied_mutation_counts_with_prefix(b"restore-release-reference-batch");
    assert_eq!(
        mutation_counts[mutation_counts_before],
        1 + 3 * 2 + 1,
        "two borrower triples sharing one canonical object need one GC mutation"
    );
    let metrics = service.restore_metrics().unwrap();
    for keyspace in ["base_owner", "base_inverse", "base_inverse_owner"] {
        assert_eq!(metrics.control_rows.get(keyspace), Some(&0));
    }
    let queued = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(service.mount_id()),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .into_iter()
        .filter_map(|row| {
            decode_object_gc_record(&row.value.0)
                .ok()
                .filter(|record| record.object_key == source_object.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(queued.len(), 1);
    assert!(objects.head(&source_object).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/both/one/shared.bin"),
        b"shared object bytes"
    );
    assert_eq!(
        read_artifact_at_path(&service, "/both/two/shared.bin"),
        b"shared object bytes"
    );
}

#[test]
fn restore_release_defers_gc_until_a_cross_page_object_is_validated() {
    const BORROWER_COUNT: usize = 65;

    let metadata = HoltMetadataStore::open_memory().unwrap();
    let backing = MemoryObjectStore::new();
    let objects = DeleteAckLostObjectStore::new(backing);
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/shared.bin",
        "restore/cross-page-shared-object",
        b"shared across an exact-reference page boundary",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_object = block_key(source.attr.inode, source_body.generation, 0, 0);
    enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: source.attr.inode,
            generation: source_body.generation,
            object_key: source_object.as_str().to_owned(),
            size: source_body.size,
            digest_uri: source_body.digest_uri.clone(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );
    service.create_dir_path("/both", 0o755, 1000, 1000).unwrap();
    for index in 0..BORROWER_COUNT {
        service
            .clone_subtree_path_into("/base", &format!("/both/borrower-{index:03}"))
            .unwrap();
    }
    let snapshot = service.snapshot_subtree_path("/both").unwrap();
    service
        .restore_subtree_path_to_fork("/both", snapshot.snapshot_id, "/restored")
        .unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/both",
        snapshot.snapshot_id,
        "/restored",
    );
    let operation = restore::decode_restore_operation(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_operation_key(service.mount_id(), &operation_digest),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .expect("restore operation must be durable")
            .0,
    )
    .unwrap();
    assert_eq!(
        service.restore_metrics().unwrap().control_rows["base_owner"],
        BORROWER_COUNT
    );
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/restored")
        .unwrap();

    // Remove every ordinary namespace holder before corrupting the final exact
    // inverse. The pre-existing candidate predates every clone retention floor,
    // so the same cleanup call must rely on the exact-reference continuation
    // proof when it inspects that candidate immediately after the release page.
    for index in 0..BORROWER_COUNT {
        service
            .remove_file_path(&format!("/both/borrower-{index:03}/shared.bin"))
            .unwrap();
        service
            .remove_empty_dir_path(&format!("/both/borrower-{index:03}"))
            .unwrap();
    }
    service.remove_empty_dir_path("/both").unwrap();
    service.remove_file_path("/base/shared.bin").unwrap();
    service.remove_empty_dir_path("/base").unwrap();
    let mount = service.mount_id();
    let owner_prefix = restore::restore_base_owner_prefix(mount, operation.ref_set_id);
    let owner_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: owner_prefix.clone(),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(owner_rows.len(), BORROWER_COUNT);
    let last_reference =
        restore_gc::decode_restore_base_reference(&owner_rows[BORROWER_COUNT - 1].value.0).unwrap();
    assert!(owner_rows.iter().all(|row| {
        restore_gc::decode_restore_base_reference(&row.value.0)
            .is_ok_and(|reference| reference.object_key == source_object.as_str())
    }));
    let object_digest: [u8; 32] = Sha256::digest(source_object.as_str().as_bytes()).into();
    let last_inverse_key = restore::restore_base_inverse_key(
        mount,
        &object_digest,
        operation.ref_set_id,
        last_reference.borrower_inode,
        last_reference.borrower_generation,
    );
    let last_inverse = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &last_inverse_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .expect("the final borrower must have an inverse before corruption");
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-delete-cross-page-restore-inverse",
                mount,
                last_reference.borrower_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: last_inverse_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: last_inverse_key.clone(),
                predicate: Predicate::VersionEquals(last_inverse.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, last_inverse_key)],
            watch: Vec::new(),
        })
        .unwrap();

    let queued_for_object = || {
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .into_iter()
            .filter(|row| {
                decode_object_gc_record(&row.value.0)
                    .is_ok_and(|record| record.object_key == source_object.as_str())
            })
            .count()
    };

    assert!(queued_for_object() >= 1);
    let first = service.cleanup_pending_objects(64).unwrap();
    assert_eq!(first.restore_release_jobs_processed, 1);
    assert_eq!(first.deleted, 0, "first cleanup outcome: {first:?}");
    assert_eq!(objects.delete_calls(), 0);
    assert!(first.scanned >= 1, "first cleanup outcome: {first:?}");
    assert!(
        first.blocked_by_snapshots >= 1,
        "first cleanup outcome: {first:?}"
    );
    assert!(queued_for_object() >= 1);
    assert!(objects.head(&source_object).unwrap().is_some());
    let remaining = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: owner_prefix,
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, owner_rows[BORROWER_COUNT - 1].key);

    let second = service.cleanup_pending_objects(64).unwrap();
    assert_eq!(second.restore_release_jobs_processed, 1);
    assert_eq!(second.deleted, 0, "second cleanup outcome: {second:?}");
    assert_eq!(second.restore_release_quarantine, 1);
    assert_eq!(second.restore_release_mount_wide_quarantine, 0);
    assert_eq!(objects.delete_calls(), 0);
    assert!(second.scanned >= 1, "second cleanup outcome: {second:?}");
    assert!(
        second.blocked_by_snapshots >= 1,
        "second cleanup outcome: {second:?}"
    );
    assert!(queued_for_object() >= 1);
    assert!(objects.head(&source_object).unwrap().is_some());
    let quarantines = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_object_quarantine_prefix(mount, &object_digest),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(quarantines.len(), 1);
    let quarantine =
        restore::validate_restore_release_quarantine_row(mount, &quarantines[0]).unwrap();
    assert_eq!(
        quarantine.scope,
        restore::RestoreReleaseQuarantineScope::Object(object_digest)
    );
    assert_eq!(quarantine.original_key, owner_rows[BORROWER_COUNT - 1].key);
    assert!(quarantine.reason.contains("no inverse"));
}

#[test]
fn restore_release_mount_wide_quarantines_a_rekeyed_cross_page_owner_before_gc() {
    const BORROWER_COUNT: usize = 65;

    let metadata = HoltMetadataStore::open_memory().unwrap();
    let backing = MemoryObjectStore::new();
    let objects = DeleteAckLostObjectStore::new(backing);
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let source = publish_path_artifact(
        &service,
        "/base/shared.bin",
        "restore/rekeyed-cross-page-owner",
        b"shared across a rekeyed exact-reference page boundary",
    );
    let source_body = source.body.as_ref().unwrap();
    let source_object = block_key(source.attr.inode, source_body.generation, 0, 0);
    enqueue_gc_candidate(
        &service,
        ObjectGcRecord {
            inode: source.attr.inode,
            generation: source_body.generation,
            object_key: source_object.as_str().to_owned(),
            size: source_body.size,
            digest_uri: source_body.digest_uri.clone(),
            enqueue_version: 0,
            enqueue_unix_ms: 0,
        },
    );
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    for index in 0..BORROWER_COUNT {
        service
            .clone_subtree_path_into("/base", &format!("/source/borrower-{index:03}"))
            .unwrap();
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    assert_eq!(
        service.restore_metrics().unwrap().control_rows["base_owner"],
        BORROWER_COUNT
    );
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();

    for index in 0..BORROWER_COUNT {
        service
            .remove_file_path(&format!("/source/borrower-{index:03}/shared.bin"))
            .unwrap();
        service
            .remove_empty_dir_path(&format!("/source/borrower-{index:03}"))
            .unwrap();
    }
    service.remove_empty_dir_path("/source").unwrap();
    service.remove_file_path("/base/shared.bin").unwrap();
    service.remove_empty_dir_path("/base").unwrap();

    let mount = service.mount_id();
    let owner_prefix = restore::restore_base_owner_prefix(mount, operation.ref_set_id);
    let owner_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: owner_prefix.clone(),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(owner_rows.len(), BORROWER_COUNT);
    let last_owner = &owner_rows[BORROWER_COUNT - 1];
    let last_reference = restore_gc::decode_restore_base_reference(&last_owner.value.0).unwrap();
    let source_object_digest: [u8; 32] = Sha256::digest(source_object.as_str().as_bytes()).into();
    assert!(owner_rows.iter().all(|row| {
        restore_gc::decode_restore_base_reference(&row.value.0)
            .is_ok_and(|reference| reference.object_key == source_object.as_str())
    }));

    // Move the 65th owner to a syntactically valid, larger object digest while
    // retaining its encoded reference to object A. Also remove A's matching
    // target-first inverse, so only mount-wide fail-closed quarantine can keep
    // the pre-existing A candidate from being deleted after the first 64 rows.
    let rekeyed_object_digest = [u8::MAX; 32];
    assert!(source_object_digest < rekeyed_object_digest);
    let rekeyed_owner_key = restore::restore_base_owner_key(
        mount,
        operation.ref_set_id,
        &rekeyed_object_digest,
        last_reference.borrower_inode,
        last_reference.borrower_generation,
    );
    assert!(rekeyed_owner_key > last_owner.key);
    let inverse_key = restore::restore_base_inverse_key(
        mount,
        &source_object_digest,
        operation.ref_set_id,
        last_reference.borrower_inode,
        last_reference.borrower_generation,
    );
    let inverse = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &inverse_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .expect("the rekeyed owner must begin with an A inverse");
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-rekey-cross-page-restore-owner",
                mount,
                last_reference.borrower_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: rekeyed_owner_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: last_owner.key.clone(),
                    predicate: Predicate::VersionEquals(last_owner.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: rekeyed_owner_key.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::VersionEquals(inverse.version),
                },
            ],
            mutations: vec![
                delete_mutation(RecordFamily::System, last_owner.key.clone()),
                Mutation {
                    family: RecordFamily::System,
                    key: rekeyed_owner_key.clone(),
                    op: MutationOp::Put,
                    value: Some(last_owner.value.clone()),
                },
                delete_mutation(RecordFamily::System, inverse_key),
            ],
            watch: Vec::new(),
        })
        .unwrap();

    let boundary_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: owner_prefix.clone(),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(boundary_rows.len(), BORROWER_COUNT);
    assert_eq!(boundary_rows[BORROWER_COUNT - 1].key, rekeyed_owner_key);

    let outcome = service.cleanup_pending_objects(64).unwrap();
    assert_eq!(outcome.restore_release_jobs_processed, 1);
    assert_eq!(outcome.restore_release_quarantine, 1);
    assert_eq!(outcome.restore_release_mount_wide_quarantine, 1);
    assert_eq!(outcome.deleted, 0, "cleanup outcome: {outcome:?}");
    assert_eq!(objects.delete_calls(), 0);
    assert!(
        outcome.blocked_by_snapshots >= 1,
        "cleanup outcome: {outcome:?}"
    );
    assert!(objects.head(&source_object).unwrap().is_some());

    let remaining = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: owner_prefix,
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, rekeyed_owner_key);
    let quarantines = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_mount_wide_quarantine_prefix(mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(quarantines.len(), 1);
    let quarantine =
        restore::validate_restore_release_quarantine_row(mount, &quarantines[0]).unwrap();
    assert_eq!(
        quarantine.scope,
        restore::RestoreReleaseQuarantineScope::MountWide
    );
    assert_eq!(quarantine.original_key, rekeyed_owner_key);
    assert!(quarantine.reason.contains("changed identity"));
}

#[test]
fn restore_exact_references_adapt_to_byte_budget_ack_loss_and_reopen() {
    const REFERENCE_COUNT: usize = 64;
    const REFERENCES_PER_DIRECTORY: usize = 32;
    // 134 KiB is deliberately close to the production boundary: 61 such
    // references plus owner/inverse/inverse-owner proof exceed 8 MiB, while
    // either 32-entry materialization command remains comfortably bounded.
    const DIGEST_PADDING: usize = 134 * 1024;

    let metadata = OversizedManifestStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    for directory in 0..2 {
        service
            .create_dir_path(&format!("/source/group-{directory}"), 0o755, 1000, 1000)
            .unwrap();
    }
    let large_digest = format!("sha256:{}", "d".repeat(DIGEST_PADDING));
    for index in 0..REFERENCE_COUNT {
        let directory = index / REFERENCES_PER_DIRECTORY;
        let path = format!("/source/group-{directory}/artifact-{index:02}.bin");
        let prepared = service.prepare_artifact_create_path(&path).unwrap();
        let written = service
            .stage_prepared_artifact_ranges(
                &prepared,
                &format!("restore/adaptive-reference-{index:02}"),
                &[PublishArtifactRange {
                    offset: 0,
                    bytes: vec![index as u8],
                }],
                0,
            )
            .unwrap();
        let staged = written.staged_objects().unwrap();
        let mut chunks = written.chunk_manifests();
        chunks[0].slices[0].blocks[0].digest_uri = large_digest.clone();
        service
            .publish_prepared_artifact_staged_session(
                prepared.clone(),
                PublishArtifactStagedSession {
                    parent: prepared.parent,
                    name: prepared.name,
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:body".to_owned(),
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: format!("restore/adaptive-reference-{index:02}"),
                    size: 1,
                    chunks,
                    staged,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            )
            .unwrap();
    }
    let snapshot = service.snapshot_subtree_path("/source").unwrap();

    // The first persisted page only advances across empty staging members;
    // lose the ACK for the following, byte-packed reference page.
    metadata.fail_after_apply(b"restore-persist-base-refs", 1);
    let outcome = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    assert_eq!(outcome.state, RestoreState::Complete);
    let mutation_counts =
        metadata.applied_mutation_counts_with_prefix(b"restore-persist-base-refs");
    let reference_counts = mutation_counts
        .iter()
        .map(|count| count.saturating_sub(1) / 3)
        .collect::<Vec<_>>();
    assert_eq!(reference_counts.iter().sum::<usize>(), REFERENCE_COUNT);
    assert!(
        reference_counts.iter().copied().max().unwrap_or(0) < 61,
        "the 61-reference runtime page must be shortened by the 8 MiB packer: {reference_counts:?}"
    );
    assert!(
        reference_counts.iter().filter(|count| **count > 0).count() >= 2,
        "large references must span multiple durable pages: {reference_counts:?}"
    );
    assert_eq!(
        metadata.applied_with_prefix(b"restore-persist-base-refs"),
        mutation_counts.len(),
        "an apply-then-ACK-loss page must be recognized exactly once"
    );
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    let destination_root = operation.destination_root;
    drop(service);

    metadata.clear_fault();
    let reopened =
        NoKvFs::open_existing(MountId::new(1).unwrap(), metadata.clone(), objects, 0).unwrap();
    let reopened_operation = durable_restore_operation(&reopened, snapshot.snapshot_id);
    assert_eq!(reopened_operation.destination_root, destination_root);
    assert_eq!(
        reopened_operation.state,
        restore::RestoreOperationState::Complete
    );
    let version = reopened.read_version().unwrap();
    let seal = restore_gc::decode_restore_base_seal(
        &reopened
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_base_seal_key(reopened.mount_id(), reopened_operation.ref_set_id),
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .expect("adaptive reference set must be sealed")
            .0,
    )
    .unwrap();
    assert_eq!(seal.reference_count, REFERENCE_COUNT as u64);
    let inverse_prefix = restore::restore_control_keyspaces(reopened.mount_id())
        .into_iter()
        .find_map(|(name, prefix)| (name == "base_inverse").then_some(prefix))
        .expect("base inverse keyspace is registered");
    for prefix in [
        restore::restore_base_owner_prefix(reopened.mount_id(), reopened_operation.ref_set_id),
        inverse_prefix,
        restore::restore_base_inverse_owner_prefix(
            reopened.mount_id(),
            reopened_operation.ref_set_id,
        ),
    ] {
        let rows = reopened
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix,
                start_after: None,
                version,
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap();
        assert_eq!(rows.len(), REFERENCE_COUNT);
        assert_eq!(
            rows.iter()
                .map(|row| row.key.as_slice())
                .collect::<HashSet<_>>()
                .len(),
            REFERENCE_COUNT,
            "exact-reference projection contains a duplicate key"
        );
    }
    assert_eq!(
        read_artifact_at_path(&reopened, "/destination/group-1/artifact-63.bin"),
        vec![63]
    );
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_release_exact_reference_batch_adapts_to_byte_budget_ack_loss_and_reopen() {
    const REFERENCE_COUNT: usize = 64;
    const REFERENCES_PER_DIRECTORY: usize = 32;
    const DIGEST_PADDING: usize = 134 * 1024;
    const RELEASE_REQUEST_PREFIX: &[u8] = b"restore-release-reference-batch";

    let metadata = OversizedManifestStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    for directory in 0..2 {
        service
            .create_dir_path(&format!("/source/group-{directory}"), 0o755, 1000, 1000)
            .unwrap();
    }
    let large_digest = format!("sha256:{}", "r".repeat(DIGEST_PADDING));
    let mut expected_objects = HashSet::with_capacity(REFERENCE_COUNT);
    for index in 0..REFERENCE_COUNT {
        let directory = index / REFERENCES_PER_DIRECTORY;
        let path = format!("/source/group-{directory}/artifact-{index:02}.bin");
        let prepared = service.prepare_artifact_create_path(&path).unwrap();
        let written = service
            .stage_prepared_artifact_ranges(
                &prepared,
                &format!("restore/adaptive-release-reference-{index:02}"),
                &[PublishArtifactRange {
                    offset: 0,
                    bytes: vec![index as u8],
                }],
                0,
            )
            .unwrap();
        let staged = written.staged_objects().unwrap();
        let mut chunks = written.chunk_manifests();
        chunks[0].slices[0].blocks[0].digest_uri = large_digest.clone();
        service
            .publish_prepared_artifact_staged_session(
                prepared.clone(),
                PublishArtifactStagedSession {
                    parent: prepared.parent,
                    name: prepared.name,
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:body".to_owned(),
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: format!("restore/adaptive-release-reference-{index:02}"),
                    size: 1,
                    chunks,
                    staged,
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            )
            .unwrap();
        let artifact = service.lookup_path(&path).unwrap().unwrap();
        let body = artifact.body.as_ref().unwrap();
        assert!(expected_objects.insert(
            block_key(artifact.attr.inode, body.generation, 0, 0)
                .as_str()
                .to_owned()
        ));
    }

    let projection_counts = |service: &NoKvFs<OversizedManifestStore, MemoryObjectStore>,
                             ref_set_id: u64| {
        let version = service.read_version().unwrap();
        let count_prefix = |prefix| {
            service
                .metadata_store()
                .scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix,
                    start_after: None,
                    version,
                    limit: 0,
                    purpose: ReadPurpose::WritePlanLocal,
                })
                .unwrap()
                .len()
        };
        let inverse_prefix = restore::restore_control_keyspaces(service.mount_id())
            .into_iter()
            .find_map(|(name, prefix)| (name == "base_inverse").then_some(prefix))
            .expect("base inverse keyspace is registered");
        let inverse_count = service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: inverse_prefix,
                start_after: None,
                version,
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .into_iter()
            .filter(|row| {
                restore_gc::decode_restore_base_inverse(&row.value.0)
                    .is_ok_and(|inverse| inverse.ref_set_id == ref_set_id)
            })
            .count();
        [
            count_prefix(restore::restore_base_owner_prefix(
                service.mount_id(),
                ref_set_id,
            )),
            inverse_count,
            count_prefix(restore::restore_base_inverse_owner_prefix(
                service.mount_id(),
                ref_set_id,
            )),
        ]
    };
    let queued_exact_objects = |service: &NoKvFs<OversizedManifestStore, MemoryObjectStore>| {
        let mut queued = BTreeMap::<String, usize>::new();
        for row in service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::Gc,
                prefix: gc_queue_prefix(service.mount_id()),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
        {
            let record = decode_object_gc_record(&row.value.0).unwrap();
            if expected_objects.contains(&record.object_key) {
                *queued.entry(record.object_key).or_default() += 1;
            }
        }
        queued
    };

    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation = durable_restore_operation(&service, snapshot.snapshot_id);
    let ref_set_id = operation.ref_set_id;
    assert_eq!(
        projection_counts(&service, ref_set_id),
        [REFERENCE_COUNT; 3]
    );
    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);

    metadata.fail_after_apply(RELEASE_REQUEST_PREFIX, 0);
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    let first_mutation_counts =
        metadata.applied_mutation_counts_with_prefix(RELEASE_REQUEST_PREFIX);
    assert_eq!(first_mutation_counts.len(), 1);
    let first_mutation_count = first_mutation_counts[0];
    assert_eq!(first_mutation_count.saturating_sub(1) % 4, 0);
    let first_reference_count = first_mutation_count.saturating_sub(1) / 4;
    assert!(first_reference_count > 0);
    assert!(
        first_reference_count < 61,
        "the 61-reference release page must be shortened by the 8 MiB packer"
    );
    assert!(first_reference_count < REFERENCE_COUNT);
    assert_eq!(
        metadata.applied_request_counts_with_prefix(RELEASE_REQUEST_PREFIX),
        vec![1],
        "the apply-then-ACK-loss release request must not be applied twice"
    );
    let expected_remaining = REFERENCE_COUNT - first_reference_count;
    assert_eq!(
        projection_counts(&service, ref_set_id),
        [expected_remaining; 3]
    );
    let first_queued = queued_exact_objects(&service);
    assert_eq!(first_queued.len(), first_reference_count);
    assert!(first_queued.values().all(|count| *count == 1));
    assert!(expected_objects.iter().all(|object_key| {
        objects
            .head(&ObjectKey::new(object_key.clone()).unwrap())
            .unwrap()
            .is_some()
    }));
    drop(service);

    metadata.clear_fault();
    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        metadata.clone(),
        objects.clone(),
        0,
    )
    .unwrap();
    assert_eq!(reopened.restore_metrics().unwrap().releasing, 1);
    assert_eq!(
        projection_counts(&reopened, ref_set_id),
        [expected_remaining; 3],
        "reopen must resume from the ACK-reconciled release frontier"
    );
    for _ in 0..1_024 {
        if reopened.restore_metrics().unwrap().releasing == 0 {
            break;
        }
        reopened.cleanup_restore_releases(1).unwrap();
    }
    let final_metrics = reopened.restore_metrics().unwrap();
    assert_eq!(final_metrics.releasing, 0);
    assert_eq!(final_metrics.operation_count(), 0);
    assert_eq!(projection_counts(&reopened, ref_set_id), [0; 3]);

    let mutation_counts = metadata.applied_mutation_counts_with_prefix(RELEASE_REQUEST_PREFIX);
    let released_reference_counts = mutation_counts
        .iter()
        .map(|count| {
            assert_eq!(count.saturating_sub(1) % 4, 0);
            count.saturating_sub(1) / 4
        })
        .collect::<Vec<_>>();
    assert_eq!(
        released_reference_counts.iter().sum::<usize>(),
        REFERENCE_COUNT
    );
    assert!(
        released_reference_counts.len() >= 2,
        "134 KiB exact references must span multiple release batches: {released_reference_counts:?}"
    );
    assert!(
        released_reference_counts.iter().copied().max().unwrap_or(0) < 61,
        "every release batch must remain under the 8 MiB bound: {released_reference_counts:?}"
    );
    let request_apply_counts = metadata.applied_request_counts_with_prefix(RELEASE_REQUEST_PREFIX);
    assert_eq!(request_apply_counts.len(), mutation_counts.len());
    assert!(
        request_apply_counts.iter().all(|count| *count == 1),
        "every release request id must apply exactly once: {request_apply_counts:?}"
    );
    assert_eq!(
        metadata.applied_with_prefix(RELEASE_REQUEST_PREFIX),
        mutation_counts.len()
    );
    let final_queued = queued_exact_objects(&reopened);
    assert_eq!(final_queued.len(), REFERENCE_COUNT);
    assert!(final_queued.values().all(|count| *count == 1));
    assert!(expected_objects.iter().all(|object_key| {
        objects
            .head(&ObjectKey::new(object_key.clone()).unwrap())
            .unwrap()
            .is_some()
    }));
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_reference_page_serializes_inverse_validation_with_release() {
    let mount = MountId::new(1).unwrap();
    let metadata = RestoreBaseInverseScanBarrierStore::new(mount);
    let objects = MemoryObjectStore::new();
    let service = Arc::new(NoKvFs::new(mount, metadata.clone(), objects.clone()));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/release-validation-race",
        b"shared across the release race",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/first")
        .unwrap();
    service.remove_file_path("/first/shared.bin").unwrap();
    service.remove_empty_dir_path("/first").unwrap();
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);

    metadata.arm();
    let restore_service = Arc::clone(&service);
    let snapshot_id = snapshot.snapshot_id;
    let restore = std::thread::spawn(move || {
        restore_service.restore_subtree_path_to_fork("/source", snapshot_id, "/second")
    });
    metadata.wait_until_blocked();
    assert!(
        service.object_gc_gate.try_lock().is_err(),
        "reference validation must hold object_gc_gate before reading an inverse"
    );

    let release_service = Arc::clone(&service);
    let release_started = Arc::new(Barrier::new(2));
    let release_thread_started = Arc::clone(&release_started);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = std::thread::spawn(move || {
        release_thread_started.wait();
        let result = release_service.cleanup_restore_releases(64);
        release_tx.send(result).unwrap();
    });
    release_started.wait();
    assert!(
        release_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "release must not delete an inverse between validation's scan and owner read"
    );

    metadata.release_blocked();
    let outcome = restore.join().unwrap().unwrap();
    assert_eq!(outcome.state, RestoreState::Complete);
    assert!(service.lookup_path("/second").unwrap().is_some());
    release_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    release.join().unwrap();

    for _ in 0..256 {
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    drop(service);

    let reopened = NoKvFs::open_existing(mount, metadata, objects, 0).unwrap();
    assert_eq!(
        read_artifact_at_path(&reopened, "/second/shared.bin"),
        b"shared across the release race"
    );
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_reference_validation_rejects_a_durable_owner_orphan() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/durable-owner-orphan",
        b"fail closed on a real owner orphan",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/first")
        .unwrap();
    let (operation, _) = load_completed_restore_operation_and_seal(
        &service,
        "/source",
        snapshot.snapshot_id,
        "/first",
    );
    let owner = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_base_owner_prefix(service.mount_id(), operation.ref_set_id),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: b"test-delete-restore-base-owner".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: owner.key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: owner.key.clone(),
                predicate: Predicate::VersionEquals(owner.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, owner.key)],
            watch: Vec::new(),
        })
        .unwrap();

    assert!(matches!(
        service.restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/second"),
        Err(MetadError::Codec(message))
            if message == "restore base inverse has no canonical owner"
    ));
    assert_eq!(
        read_artifact_at_path(&service, "/first/shared.bin"),
        b"fail closed on a real owner orphan"
    );
    let report = service.fsck_restore_state(false).unwrap();
    assert!(!report.is_consistent());
    assert!(report.issues.iter().any(|issue| {
        issue.code == "orphan_or_mismatched_base_inverse" && issue.message.contains("owner")
    }));
}

#[test]
fn restore_overlay_release_does_not_repeat_historical_retention_walks() {
    const FILE_COUNT: usize = 65;
    const OVERLAY_TRANSITION_REQUEST_PREFIX: &[u8] = b"restore-enter-release-overlay";

    let mount = MountId::new(1).unwrap();
    let purpose_tracking = PurposeTrackingStore::new();
    let metadata = RestorePostApplyFaultStore::wrapping(purpose_tracking.clone());
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(mount, metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let names = (0..FILE_COUNT)
        .map(|index| DentryName::new(format!("indexed-{index:03}.csv")).unwrap())
        .collect::<Vec<_>>();
    service
        .create_files_in_dir_path("/source", names, 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();
    let operation_digest =
        restore::restore_operation_digest(mount, "/source", snapshot.snapshot_id, "/restored");
    let operation = restore::decode_restore_operation(
        &service
            .metadata_store()
            .get(
                RecordFamily::System,
                &restore::restore_operation_key(mount, &operation_digest),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap()
            .0,
    )
    .unwrap();
    for index in 0..FILE_COUNT {
        service
            .remove_file_path(&format!("/restored/indexed-{index:03}.csv"))
            .unwrap();
    }
    service.remove_empty_dir_path("/restored").unwrap();
    metadata.fail_after_apply(OVERLAY_TRANSITION_REQUEST_PREFIX, 0);

    {
        let load_job = || {
            service
                .metadata_store()
                .get_versioned(
                    RecordFamily::System,
                    &restore::restore_release_job_key(mount, operation.ref_set_id),
                    service.read_version().unwrap(),
                    ReadPurpose::WritePlanLocal,
                )
                .unwrap()
                .map(|item| {
                    (
                        restore::decode_restore_release_job(&item.value.0).unwrap(),
                        item.version,
                    )
                })
        };
        for _ in 0..4096 {
            if load_job()
                .as_ref()
                .is_some_and(|(job, _)| job.phase == restore::RestoreReleasePhase::Overlay)
            {
                break;
            }
            service.cleanup_restore_releases(1).unwrap();
        }
        let (overlay, _) = load_job().expect("release must reach the durable Overlay phase");
        assert_eq!(overlay.phase, restore::RestoreReleasePhase::Overlay);
        assert!(
            service.restore_metrics().unwrap().index_rows > FILE_COUNT,
            "the overlay regression must cross a 64-row release page"
        );
        assert_eq!(
            metadata.applied_with_prefix(OVERLAY_TRANSITION_REQUEST_PREFIX),
            1,
            "the durable Overlay transition must survive its lost acknowledgement"
        );
    }
    drop(service);

    metadata.clear_fault();
    let service = NoKvFs::open_existing(mount, metadata.clone(), objects, 0).unwrap();
    let load_job = || {
        service
            .metadata_store()
            .get_versioned(
                RecordFamily::System,
                &restore::restore_release_job_key(mount, operation.ref_set_id),
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .map(|item| {
                (
                    restore::decode_restore_release_job(&item.value.0).unwrap(),
                    item.version,
                )
            })
    };
    let (overlay, _) = load_job().expect("Overlay phase must survive reopen");
    assert_eq!(overlay.phase, restore::RestoreReleasePhase::Overlay);
    let before = purpose_tracking.counts();
    let initial_cursor = overlay.cursor;
    let mut advanced = false;
    for _ in 0..4 {
        service.cleanup_restore_releases(1).unwrap();
        match load_job() {
            None => {
                advanced = true;
                break;
            }
            Some((job, _)) if job.cursor != initial_cursor => {
                advanced = true;
                break;
            }
            Some(_) => {}
        }
    }
    assert!(advanced, "the overlay release cursor did not advance");
    let after = purpose_tracking.counts();
    assert_eq!(after.snapshot_gets, before.snapshot_gets);
    assert_eq!(after.snapshot_scans, before.snapshot_scans);

    for _ in 0..256 {
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
        service.cleanup_restore_releases(1).unwrap();
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    let after_finish = purpose_tracking.counts();
    assert_eq!(after_finish.snapshot_gets, before.snapshot_gets);
    assert_eq!(after_finish.snapshot_scans, before.snapshot_scans);
    let report = service.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_fsck_keeps_one_version_and_reports_damage_across_concurrent_repair() {
    let objects = HeadBarrierObjectStore::new(MemoryObjectStore::new());
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let source = publish_path_artifact(
        service.as_ref(),
        "/source/shared.bin",
        "restore/fsck-point-in-time",
        b"point-in-time restore fsck",
    );
    let source_body = source.body.as_ref().unwrap();
    let object_key = block_key(source.attr.inode, source_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    assert!(objects.delete(&object_key).unwrap());
    let damaged_version = service.read_version().unwrap();
    let graph_sequence = service.restore_graph_sequence.load(Ordering::Acquire);

    objects.arm();
    let fsck_service = Arc::clone(&service);
    let fsck = std::thread::spawn(move || fsck_service.fsck_restore_state(true));
    objects.wait_until_head();

    // Repair the physical object after HEAD captured its absence, then commit
    // unrelated metadata before the restore-private scans finish. A correct
    // point-in-time proof must return the captured damage instead of rejecting
    // the concurrent commit through the old mount-global epoch check.
    objects
        .put(&object_key, b"point-in-time restore fsck".to_vec())
        .unwrap();
    let epoch_before_repair = service.path_cache_epoch.load(Ordering::Acquire);
    let cursor_key = restore::restore_init_upload_tombstone_cursor_key(service.mount_id());
    let cursor_prefix = restore::restore_init_upload_tombstone_prefix(service.mount_id());
    for index in 0..16 {
        let version = service.next_version().unwrap();
        service
            .commit_metadata(MetadataCommand {
                request_id: format!("test-rotate-restore-scheduler-cursor-{index}").into_bytes(),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).unwrap(),
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: cursor_key.clone(),
                predicates: Vec::new(),
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: cursor_key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(if index % 2 == 0 {
                        cursor_prefix.clone()
                    } else {
                        Vec::new()
                    })),
                }],
                watch: Vec::new(),
            })
            .unwrap();
    }
    service
        .create_dir_path("/unrelated", 0o755, 1000, 1000)
        .unwrap();
    let repaired_version = service.read_version().unwrap();
    assert!(
        service.path_cache_epoch.load(Ordering::Acquire) > epoch_before_repair,
        "concurrent commit did not exercise the old global-epoch rejection path"
    );
    assert_eq!(
        service.restore_graph_sequence.load(Ordering::Acquire),
        graph_sequence,
        "unrelated writes and scheduler cursor movement changed the restore graph sequence"
    );

    objects.release_head();
    let report = fsck.join().unwrap().unwrap();
    assert_eq!(report.metrics.read_version, damaged_version.get());
    assert!(!report.is_consistent());
    assert!(report
        .dangling_borrowed_objects
        .iter()
        .any(|dangling| dangling.object_key == object_key.as_str()));

    let repaired = service.fsck_restore_state(true).unwrap();
    assert!(
        repaired.is_consistent(),
        "repaired restore fsck: {repaired:#?}"
    );
    assert!(repaired.metrics.read_version >= repaired_version.get());
}

#[test]
fn restore_metrics_samples_allocator_fence_after_concurrent_reservation() {
    let mount = MountId::new(1).unwrap();
    let metadata = AllocatorFenceBarrierStore::new(mount);
    let service = Arc::new(NoKvFs::new(
        mount,
        metadata.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let allocator_key = allocator_key(mount);
    let max_version = Version::new(u64::MAX).unwrap();
    let before = metadata
        .get_versioned(
            RecordFamily::System,
            &allocator_key,
            max_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let graph_sequence = service.restore_graph_sequence.load(Ordering::SeqCst);

    metadata.arm();
    let metrics_service = Arc::clone(&service);
    let metrics = std::thread::spawn(move || metrics_service.restore_metrics());
    metadata.wait_until_active_read();

    let reservation_boundary = service.reserved_version.load(Ordering::Relaxed);
    while service.clock.load(Ordering::Relaxed) <= reservation_boundary {
        service.next_version().unwrap();
    }
    let after = metadata
        .get_versioned(
            RecordFamily::System,
            &allocator_key,
            max_version,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    assert!(after.version > before.version);
    assert_eq!(
        service.restore_graph_sequence.load(Ordering::SeqCst),
        graph_sequence,
        "ordinary allocator high-water reservation changed the restore graph sequence"
    );

    metadata.release_active_read();
    let metrics = metrics.join().unwrap().unwrap();
    assert_eq!(metrics.complete, 1);
    assert!(metrics.active_marker);
    assert!(metrics.allocator_v2_fenced);
}

#[test]
fn restore_fsck_retries_when_the_graph_changes_after_a_scan_page() {
    let mount = MountId::new(1).unwrap();
    let metadata = RestoreBaseInverseScanBarrierStore::new(mount);
    let service = Arc::new(NoKvFs::new(
        mount,
        metadata.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        service.as_ref(),
        "/source/artifact.bin",
        "restore/fsck-reader-first",
        b"reader-first graph rewrite",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let activation_key = restore::restore_activation_fence_key(mount);
    let activation = metadata
        .get_versioned(
            RecordFamily::System,
            &activation_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    metadata.arm();
    let fsck_service = Arc::clone(&service);
    let fsck = std::thread::spawn(move || fsck_service.fsck_restore_state(false));
    metadata.wait_until_blocked();

    let rewrite_version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: b"test-rewrite-restore-activation-during-fsck".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(rewrite_version).unwrap(),
            commit_version: rewrite_version,
            primary_family: RecordFamily::System,
            primary_key: activation_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: activation_key.clone(),
                predicate: Predicate::VersionEquals(activation.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: activation_key,
                op: MutationOp::Put,
                value: Some(activation.value),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    metadata.release_blocked();

    let report = fsck.join().unwrap().unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
    assert!(report.metrics.read_version >= rewrite_version.get());
}

#[test]
fn restore_fsck_point_checks_missing_index_inverse_rows() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/artifact.bin",
        "restore/fsck-missing-index-inverse",
        b"indexed artifact",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let inverse_prefix = restore_index::restore_index_private_keyspaces(service.mount_id())
        .into_iter()
        .find_map(|(name, prefix)| (name == "index_parent_inverse").then_some(prefix))
        .unwrap();
    let inverse = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: inverse_prefix,
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: b"test-delete-restore-index-inverse-before-fsck".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: inverse.key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: inverse.key.clone(),
                predicate: Predicate::VersionEquals(inverse.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, inverse.key)],
            watch: Vec::new(),
        })
        .unwrap();

    let report = service.fsck_restore_state(false).unwrap();
    assert!(!report.is_consistent());
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.code == "index_closure_mismatch"));
}

#[test]
fn restore_fsck_rejects_a_graph_apply_before_the_writer_returns() {
    const REQUEST_ID: &[u8] = b"test-restore-graph-post-apply-fence";

    let metadata = PostCommitBarrierStore::new(REQUEST_ID);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        service.as_ref(),
        "/source/artifact.bin",
        "restore/fsck-post-apply-fence",
        b"indexed artifact",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let inverse_prefix = restore_index::restore_index_private_keyspaces(service.mount_id())
        .into_iter()
        .find_map(|(name, prefix)| (name == "index_parent_inverse").then_some(prefix))
        .unwrap();
    let inverse = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: inverse_prefix,
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let version = service.next_version().unwrap();
    let command = MetadataCommand {
        request_id: REQUEST_ID.to_vec(),
        kind: CommandKind::CleanupObjects,
        read_version: predecessor(version).unwrap(),
        commit_version: version,
        primary_family: RecordFamily::System,
        primary_key: inverse.key.clone(),
        predicates: vec![PredicateRef {
            family: RecordFamily::System,
            key: inverse.key.clone(),
            predicate: Predicate::VersionEquals(inverse.version),
        }],
        mutations: vec![delete_mutation(RecordFamily::System, inverse.key)],
        watch: Vec::new(),
    };
    let writer_service = Arc::clone(&service);
    let writer = std::thread::spawn(move || writer_service.commit_metadata(command));
    metadata.wait_until_applied();

    assert_eq!(service.restore_graph_writers.load(Ordering::Acquire), 1);
    let error = service.fsck_restore_state(false).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("restore fsck raced a restore graph write"),
        "unexpected restore fsck error: {error}"
    );
    let error = service.restore_metrics().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("restore metrics raced a restore graph write"),
        "unexpected restore metrics error: {error}"
    );

    metadata.release_after_apply();
    writer.join().unwrap().unwrap();
    assert_eq!(service.restore_graph_writers.load(Ordering::Acquire), 0);
    let report = service.fsck_restore_state(false).unwrap();
    assert!(!report.is_consistent());
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.code == "index_closure_mismatch"));
}

#[test]
fn restore_fsck_uses_bounded_pages_for_large_private_graphs() {
    const ENTRY_COUNT: usize = 300;
    const FSCK_PAGE_ROWS: u64 = 256;

    let metadata = PurposeTrackingStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let names = (0..ENTRY_COUNT)
        .map(|index| DentryName::new(format!("empty-{index:03}.bin")).unwrap())
        .collect::<Vec<_>>();
    service
        .create_files_in_dir_path("/source", names, 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    metadata.reset_scan_bounds();
    let report = service.fsck_restore_state(false).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
    let counts = metadata.counts();
    assert_eq!(counts.unbounded_scans, 0);
    assert!(
        counts.largest_bounded_scan <= FSCK_PAGE_ROWS,
        "restore fsck requested a {}-row page",
        counts.largest_bounded_scan
    );
    assert!(counts.write_plan_scans > 1);
}

#[test]
fn restore_fsck_point_checks_missing_index_source_members() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/source/artifact.bin", 0o644, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/restored")
        .unwrap();

    let source_prefix = restore_index::restore_index_private_keyspaces(service.mount_id())
        .into_iter()
        .find_map(|(name, prefix)| (name == "index_source_member").then_some(prefix))
        .unwrap();
    let source = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: source_prefix,
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: b"test-delete-restore-index-source-member".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: source.key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: source.key.clone(),
                predicate: Predicate::VersionEquals(source.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, source.key)],
            watch: Vec::new(),
        })
        .unwrap();

    let report = service.fsck_restore_state(false).unwrap();
    assert!(!report.is_consistent());
    assert!(report.issues.iter().any(|issue| {
        issue.code == "index_closure_mismatch" && issue.message.contains("source member")
    }));
}

#[test]
fn restore_exact_references_allow_distinct_borrowers_of_one_canonical_object() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/base/shared.bin",
        "shared-base",
        b"shared object bytes",
    );
    service.create_dir_path("/both", 0o755, 1000, 1000).unwrap();
    service
        .clone_subtree_path_into("/base", "/both/one")
        .unwrap();
    service
        .clone_subtree_path_into("/base", "/both/two")
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/both").unwrap();

    service
        .restore_subtree_path_to_fork("/both", snapshot.snapshot_id, "/restored")
        .unwrap();

    let one = service.lookup_path("/restored/one").unwrap().unwrap();
    let two = service.lookup_path("/restored/two").unwrap().unwrap();
    assert_eq!(
        service
            .read_artifact(one.attr.inode, &DentryName::new("shared.bin").unwrap())
            .unwrap(),
        b"shared object bytes"
    );
    assert_eq!(
        service
            .read_artifact(two.attr.inode, &DentryName::new("shared.bin").unwrap())
            .unwrap(),
        b"shared object bytes"
    );
    let (_, seal) = load_completed_restore_operation_and_seal(
        &service,
        "/both",
        snapshot.snapshot_id,
        "/restored",
    );
    assert_eq!(seal.reference_count, 2);
    let metrics = service.restore_metrics().unwrap();
    for keyspace in ["base_owner", "base_inverse", "base_inverse_owner"] {
        assert_eq!(metrics.control_rows.get(keyspace), Some(&2));
    }
}

#[test]
fn restore_exact_references_reject_inconsistent_identity_for_one_object_key() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/base/shared.bin",
        "restore/inconsistent-object-base",
        b"shared object bytes",
    );
    service.create_dir_path("/both", 0o755, 1000, 1000).unwrap();
    service
        .clone_subtree_path_into("/base", "/both/one")
        .unwrap();
    service
        .clone_subtree_path_into("/base", "/both/two")
        .unwrap();

    let second = service
        .lookup_path("/both/two/shared.bin")
        .unwrap()
        .unwrap();
    let second_body = second.body.as_ref().unwrap();
    let manifest_key = chunk_manifest_key(
        service.mount_id(),
        second.attr.inode,
        second_body.generation,
        0,
    );
    let manifest_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::ChunkManifest,
            &manifest_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let mut manifest = decode_chunk_manifest(&manifest_item.value.0).unwrap();
    manifest.slices[0].blocks[0].digest_uri = "sha256:inconsistent".to_owned();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: b"test-corrupt-shared-object-identity".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::ChunkManifest,
            primary_key: manifest_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::ChunkManifest,
                key: manifest_key.clone(),
                predicate: Predicate::VersionEquals(manifest_item.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::ChunkManifest,
                key: manifest_key,
                op: MutationOp::Put,
                value: Some(Value(encode_chunk_manifest(&manifest))),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let snapshot = service.snapshot_subtree_path("/both").unwrap();
    assert!(matches!(
        service.restore_subtree_path_to_fork(
            "/both",
            snapshot.snapshot_id,
            "/destination",
        ),
        Err(MetadError::Codec(message))
            if message.contains("inconsistent object identity")
    ));
    assert!(service.lookup_path("/destination").unwrap().is_none());
}

#[test]
fn restore_exact_references_outlive_source_for_delta_sparse_and_symlink_borrowers() {
    let (service, objects) = service_with_objects();
    let source = service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();

    let append_base = publish_path_artifact(
        &service,
        "/source/append.bin",
        "restore/append-base",
        b"append-base",
    );
    let append_base_body = append_base.body.as_ref().unwrap();
    let append_base_object = block_key(append_base.attr.inode, append_base_body.generation, 0, 0);
    let append_prepared = service
        .prepare_artifact_replace(source.attr.inode, dname(b"append.bin"))
        .unwrap();
    let append_generation = append_prepared.generation;
    let append_written = service
        .stage_prepared_artifact_ranges(
            &append_prepared,
            "restore/append-delta",
            &[PublishArtifactRange {
                offset: append_base_body.size,
                bytes: b"+delta".to_vec(),
            }],
            0,
        )
        .unwrap();
    service
        .publish_prepared_artifact_staged_session(
            append_prepared,
            PublishArtifactStagedSession {
                parent: source.attr.inode,
                name: dname(b"append.bin"),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:restore-append".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "restore/append-delta".to_owned(),
                size: append_base_body.size + 6,
                chunks: append_written.chunk_manifests(),
                staged: append_written.staged_objects().unwrap(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    let append = service.lookup_path("/source/append.bin").unwrap().unwrap();
    assert_eq!(
        append.body.as_ref().unwrap().base_generation,
        append_base_body.generation
    );
    let append_delta_object = block_key(append.attr.inode, append_generation, 0, 0);

    let sparse_base = publish_path_artifact(
        &service,
        "/source/sparse.bin",
        "restore/sparse-base",
        b"sparse-base",
    );
    let sparse_base_body = sparse_base.body.as_ref().unwrap();
    let sparse_base_object = block_key(sparse_base.attr.inode, sparse_base_body.generation, 0, 0);
    let sparse_tail_offset = DEFAULT_CHUNK_SIZE + 5;
    let sparse_prepared = service
        .prepare_artifact_replace(source.attr.inode, dname(b"sparse.bin"))
        .unwrap();
    let sparse_generation = sparse_prepared.generation;
    let sparse_written = service
        .stage_prepared_artifact_ranges(
            &sparse_prepared,
            "restore/sparse-delta",
            &[PublishArtifactRange {
                offset: sparse_tail_offset,
                bytes: b"tail".to_vec(),
            }],
            0,
        )
        .unwrap();
    service
        .publish_prepared_artifact_staged_session(
            sparse_prepared,
            PublishArtifactStagedSession {
                parent: source.attr.inode,
                name: dname(b"sparse.bin"),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:restore-sparse".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "restore/sparse-delta".to_owned(),
                size: sparse_tail_offset + 4,
                chunks: sparse_written.chunk_manifests(),
                staged: sparse_written.staged_objects().unwrap(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    let sparse = service.lookup_path("/source/sparse.bin").unwrap().unwrap();
    assert_eq!(
        sparse.body.as_ref().unwrap().base_generation,
        sparse_base_body.generation
    );
    let sparse_delta_object = block_key(sparse.attr.inode, sparse_generation, 1, 0);

    let symlink_target = vec![b'x'; 8 * 1024];
    let symlink = service
        .create_symlink(
            source.attr.inode,
            dname(b"latest"),
            symlink_target.clone(),
            0o777,
            1000,
            1000,
        )
        .unwrap();
    let symlink_body = symlink.body.as_ref().unwrap();
    let symlink_object = block_key(symlink.attr.inode, symlink_body.generation, 0, 0);
    let source_objects = [
        append_base_object.clone(),
        append_delta_object.clone(),
        sparse_base_object.clone(),
        sparse_delta_object.clone(),
        symlink_object.clone(),
    ];
    assert!(source_objects
        .iter()
        .all(|object| objects.head(object).unwrap().is_some()));

    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let outcome = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let (_, seal) = load_completed_restore_operation_and_seal(
        &service,
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );
    assert_eq!(seal.reference_count, source_objects.len() as u64);
    let metrics = service.restore_metrics().unwrap();
    for keyspace in ["base_owner", "base_inverse", "base_inverse_owner"] {
        assert_eq!(
            metrics.control_rows.get(keyspace),
            Some(&source_objects.len())
        );
    }
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::ForkBinding,
            &fork_binding_key(service.mount_id(), outcome.destination_root),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    assert_eq!(service.history_retention_floor().unwrap(), None);
    service.remove_file_path("/source/append.bin").unwrap();
    service.remove_file_path("/source/sparse.bin").unwrap();
    service.remove_file_path("/source/latest").unwrap();
    service.remove_empty_dir_path("/source").unwrap();
    for _ in 0..64 {
        service.cleanup_pending_objects(64).unwrap();
    }
    assert!(source_objects
        .iter()
        .all(|object| objects.head(object).unwrap().is_some()));

    assert_eq!(
        read_artifact_at_path(&service, "/destination/append.bin"),
        b"append-base+delta"
    );
    let destination_sparse = service
        .lookup_path("/destination/sparse.bin")
        .unwrap()
        .unwrap();
    assert_eq!(
        service
            .read_file(destination_sparse.attr.inode, 0, b"sparse-base".len())
            .unwrap(),
        b"sparse-base"
    );
    assert_eq!(
        service
            .read_file(destination_sparse.attr.inode, sparse_tail_offset, 4)
            .unwrap(),
        b"tail"
    );
    let destination_symlink = service.lookup_path("/destination/latest").unwrap().unwrap();
    assert_eq!(
        service
            .read_symlink(destination_symlink.attr.inode)
            .unwrap(),
        symlink_target
    );

    let destination_append = service
        .lookup_path("/destination/append.bin")
        .unwrap()
        .unwrap();
    service
        .link(
            destination_append.attr.inode,
            InodeId::root(),
            dname(b"escaped-append.bin"),
        )
        .unwrap();
    service.remove_file_path("/destination/append.bin").unwrap();
    service.remove_file_path("/destination/sparse.bin").unwrap();
    service.remove_file_path("/destination/latest").unwrap();
    service.remove_empty_dir_path("/destination").unwrap();
    for _ in 0..32 {
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    for _ in 0..64 {
        service.cleanup_pending_objects(64).unwrap();
    }
    assert!(objects.head(&append_base_object).unwrap().is_some());
    assert!(objects.head(&append_delta_object).unwrap().is_some());
    assert_eq!(
        read_artifact_at_path(&service, "/escaped-append.bin"),
        b"append-base+delta"
    );

    service.remove_file_path("/escaped-append.bin").unwrap();
    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
    for _ in 0..256 {
        service.cleanup_pending_objects(64).unwrap();
        if source_objects
            .iter()
            .all(|object| objects.head(object).unwrap().is_none())
        {
            break;
        }
    }
    assert!(source_objects
        .iter()
        .all(|object| objects.head(object).unwrap().is_none()));
}

#[test]
fn restore_exact_reference_graph_replays_from_checkpoint_log_and_releases_after_reopen() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata, objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let source_file = publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore/replay-exact-reference",
        b"checkpoint-log exact reference",
    );
    let source_body = source_file.body.as_ref().unwrap();
    let shared_object = block_key(source_file.attr.inode, source_body.generation, 0, 0);
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    let checkpoint_config = MetadataArchiveConfig::new("meta/restore-ref-replay-checkpoint", 2);
    let checkpoint = service.backup_metadata(&checkpoint_config).unwrap();
    service
        .enable_sync_metadata_log(MetadataLogSyncConfig::new(
            "meta/restore-ref-replay-log",
            "mount-1:/",
            1,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        ))
        .unwrap();

    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
    assert_eq!(service.history_retention_floor().unwrap(), None);
    service.remove_file_path("/source/shared.bin").unwrap();
    service.remove_empty_dir_path("/source").unwrap();
    for _ in 0..8 {
        service.cleanup_pending_objects(1).unwrap();
    }
    assert!(
        objects.head(&shared_object).unwrap().is_some(),
        "the Complete exact-reference graph must protect the deleted source object"
    );
    let log = service.sync_metadata_log_snapshot().unwrap();
    assert!(log.durable_lsn > checkpoint.log_lsn);
    assert!(!log.segments.is_empty());
    let segment_keys = snapshot_segment_keys(&log);
    drop(service);

    let recovered_metadata = HoltMetadataStore::open_memory().unwrap();
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        recovered_metadata.clone(),
        objects.clone(),
    );
    recovered
        .restore_metadata_with_archived_log_segments(
            &checkpoint_config,
            "mount-1:/",
            &segment_keys,
            checkpoint.log_lsn,
            checkpoint.log_digest,
        )
        .unwrap()
        .unwrap();
    recovered
        .enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                "meta/restore-ref-replay-log",
                "mount-1:/",
                2,
                log.durable_lsn,
                log.last_digest,
            )
            .with_segments(log.segments.clone()),
        )
        .unwrap();
    assert!(recovered.lookup_path("/source").unwrap().is_none());
    assert_eq!(
        read_artifact_at_path(&recovered, "/destination/shared.bin"),
        b"checkpoint-log exact reference"
    );
    assert_eq!(recovered.history_retention_floor().unwrap(), None);
    let report = recovered.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
    let recovered_metrics = recovered.restore_metrics().unwrap();
    assert_eq!(recovered_metrics.complete, 1);
    for keyspace in ["base_owner", "base_inverse", "base_inverse_owner"] {
        assert_eq!(recovered_metrics.control_rows.get(keyspace), Some(&1));
    }
    assert!(objects.head(&shared_object).unwrap().is_some());

    let moved = recovered
        .rename_path("/destination", "/moved-destination")
        .unwrap();
    assert_eq!(
        moved.attr.inode,
        recovered
            .resolve_directory_path("/moved-destination")
            .unwrap()
    );
    assert_eq!(recovered.restore_metrics().unwrap().complete, 1);
    recovered
        .remove_file_path("/moved-destination/shared.bin")
        .unwrap();
    recovered
        .remove_empty_dir_path("/moved-destination")
        .unwrap();
    assert_eq!(recovered.restore_metrics().unwrap().releasing, 1);
    assert_eq!(recovered.cleanup_restore_releases(1).unwrap(), 1);
    assert_eq!(
        recovered.restore_metrics().unwrap().releasing,
        1,
        "one bounded release page must leave resumable durable work"
    );
    let release_log = recovered.sync_metadata_log_snapshot().unwrap();
    drop(recovered);

    let reopened = NoKvFs::open_existing(
        MountId::new(1).unwrap(),
        recovered_metadata,
        objects.clone(),
        0,
    )
    .unwrap();
    reopened
        .enable_sync_metadata_log(
            MetadataLogSyncConfig::new(
                "meta/restore-ref-replay-log",
                "mount-1:/",
                2,
                release_log.durable_lsn,
                release_log.last_digest,
            )
            .with_segments(release_log.segments),
        )
        .unwrap();
    for _ in 0..512 {
        reopened.cleanup_restore_releases(1).unwrap();
        if reopened.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(reopened.restore_metrics().unwrap().releasing, 0);
    let cleanup = reopened.cleanup_pending_objects(1).unwrap();
    assert_eq!(cleanup.blocked_by_failover_durability, 1);
    assert!(
        objects.head(&shared_object).unwrap().is_some(),
        "checkpoint-recoverable metadata must keep external DELETE fail-closed"
    );
    let queued = reopened
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(reopened.mount_id()),
            start_after: None,
            version: reopened.read_version().unwrap(),
            limit: 64,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert!(queued.iter().any(|row| {
        decode_object_gc_record(&row.value.0)
            .is_ok_and(|record| record.object_key == shared_object.as_str())
    }));
    let report = reopened.fsck_restore_state(true).unwrap();
    assert!(report.is_consistent(), "restore fsck report: {report:#?}");
}

#[test]
fn restore_release_waits_for_a_live_snapshot_of_the_restored_namespace() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore-snapshot-holder",
        b"snapshot-retained bytes",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();

    let destination_snapshot = service.snapshot_subtree_path("/destination").unwrap();
    service.remove_file_path("/destination/shared.bin").unwrap();
    service.remove_empty_dir_path("/destination").unwrap();

    for _ in 0..4 {
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(
        service.restore_metrics().unwrap().releasing,
        1,
        "a live snapshot must retain the complete restore ref-set after the live root disappears"
    );
    let cursor_key = restore::restore_release_cursor_key(service.mount_id());
    let before_idle = service.metadata_store_stats();
    let cursor_version = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &cursor_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap()
        .version;
    assert_eq!(service.cleanup_restore_releases(64).unwrap(), 1);
    assert_eq!(service.cleanup_restore_releases(64).unwrap(), 1);
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before_idle.commit_total,
        "a snapshot-blocked release must not rewrite an unchanged worker cursor"
    );
    assert_eq!(
        service
            .metadata_store()
            .get_versioned(
                RecordFamily::System,
                &cursor_key,
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap()
            .version,
        cursor_version
    );

    assert!(service
        .retire_snapshot(destination_snapshot.snapshot_id)
        .unwrap());
    for _ in 0..128 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
}

#[test]
fn restore_release_serializes_with_snapshot_renewal_across_the_old_expiry() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RenewSnapshot, 1, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore-renew-release-race",
        b"renewed snapshot bytes",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();
    let destination_snapshot = service
        .snapshot_subtree_path_with_lease("/destination", 500)
        .unwrap();
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
    service.remove_file_path("/destination/shared.bin").unwrap();
    service.remove_empty_dir_path("/destination").unwrap();

    let renew_service = Arc::clone(&service);
    let renew = std::thread::spawn(move || {
        renew_service.renew_snapshot(destination_snapshot.snapshot_id, 10_000)
    });
    store.wait_until_blocked();
    service.set_clock_override_ms(destination_snapshot.lease_expires_unix_ms);
    assert!(matches!(
        service.restore_snapshot_gate.try_write(),
        Err(std::sync::TryLockError::WouldBlock)
    ));

    let (release_started_tx, release_started_rx) = std::sync::mpsc::channel();
    let (release_done_tx, release_done_rx) = std::sync::mpsc::channel();
    let release_service = Arc::clone(&service);
    let release = std::thread::spawn(move || {
        release_started_tx.send(()).unwrap();
        let outcome = release_service.cleanup_restore_releases(64);
        release_done_tx.send(outcome).unwrap();
    });
    release_started_rx.recv().unwrap();
    assert!(
        release_done_rx
            .recv_timeout(Duration::from_millis(250))
            .is_err(),
        "restore release must wait for the in-flight renewal proof and CAS"
    );

    store.release_blocked();
    let SnapshotRenewOutcome::Renewed {
        pin: renewed,
        extended: true,
    } = renew.join().unwrap().unwrap()
    else {
        panic!("expected the pre-expiry renewal to commit")
    };
    assert_eq!(renewed.lease_expires_unix_ms, 11_000);
    release_done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    release.join().unwrap();
    assert_eq!(
        service.restore_metrics().unwrap().releasing,
        1,
        "release must observe the successfully renewed pin after it acquires the gate"
    );

    assert!(service
        .retire_snapshot(destination_snapshot.snapshot_id)
        .unwrap());
    for _ in 0..256 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
}

#[test]
fn restore_release_waits_for_a_generic_fork_binding_after_its_pin_expires() {
    let service = service();
    service.set_clock_override_ms(1_000);
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "restore-fork-binding-holder",
        b"fork-binding-retained bytes",
    );
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/destination")
        .unwrap();
    let fork = service
        .clone_subtree_path_into("/destination", "/generic-holder")
        .unwrap();

    let fork_pin = service.snapshot_pin(fork.snapshot_id).unwrap().unwrap();
    service.set_clock_override_ms(fork_pin.lease_expires_unix_ms);
    service.reclaim_expired_snapshot_pins(100).unwrap();
    assert!(service.snapshot_pin(fork.snapshot_id).unwrap().is_none());
    assert!(service
        .versioned_fork_bindings_at(service.read_version().unwrap(), ReadPurpose::UserStrong)
        .unwrap()
        .iter()
        .any(|binding| {
            binding.binding.snapshot_id == fork.snapshot_id
                && binding.binding.source_root == restored.destination_root
                && binding.binding.fork_root == fork.root
        }));

    service.remove_file_path("/destination/shared.bin").unwrap();
    service.remove_empty_dir_path("/destination").unwrap();
    for _ in 0..4 {
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(
        service.restore_metrics().unwrap().releasing,
        1,
        "the durable ForkBinding must retain the ref-set after its construction pin expires"
    );

    service
        .remove_file_path("/generic-holder/shared.bin")
        .unwrap();
    service.remove_empty_dir_path("/generic-holder").unwrap();
    assert!(service.retire_snapshot(fork.snapshot_id).unwrap());
    for _ in 0..128 {
        service.cleanup_restore_releases(64).unwrap();
        if service.restore_metrics().unwrap().releasing == 0 {
            break;
        }
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 0);
}

#[test]
fn restore_indexes_flow_through_generic_fork_after_source_delete_and_nested_restore() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/source/a.bin", "generic-index/a", b"a");
    publish_path_artifact(&service, "/source/b.bin", "generic-index/b", b"b");
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/source".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.kind"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: vec![
                NamespaceIndexRow {
                    path: "/source/a.bin".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.kind"),
                        value: NamespacePredicateValue::String("a".to_owned()),
                    }],
                },
                NamespaceIndexRow {
                    path: "/source/b.bin".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.kind"),
                        value: NamespacePredicateValue::String("b".to_owned()),
                    }],
                },
            ],
        })
        .unwrap();
    let source_snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", source_snapshot.snapshot_id, "/restored")
        .unwrap();
    let generic = service
        .clone_subtree_path_into("/restored", "/generic")
        .unwrap();

    service.remove_file_path("/restored/a.bin").unwrap();
    service.remove_file_path("/restored/b.bin").unwrap();
    service.remove_empty_dir_path("/restored").unwrap();
    for _ in 0..4 {
        service.cleanup_restore_releases(64).unwrap();
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);

    let indexed = service
        .list_indexed_path_page("/generic", None, 10)
        .unwrap();
    assert_eq!(
        indexed
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        vec![b"a.bin".to_vec(), b"b.bin".to_vec()]
    );
    let found = service
        .find_paths(NamespaceFindRequest {
            path: "/generic".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::new("run.kind"),
                op: NamespacePredicateOp::Eq,
                value: Some(NamespacePredicateValue::String("b".to_owned())),
            }],
            sort: Vec::new(),
            include: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(found.match_count, 1);
    assert_eq!(found.matches[0].path, "/generic/b.bin");

    let generic_snapshot = service.snapshot_subtree_path("/generic").unwrap();
    service
        .restore_subtree_path_to_fork("/generic", generic_snapshot.snapshot_id, "/nested")
        .unwrap();
    let nested_indexed = service.list_indexed_path_page("/nested", None, 10).unwrap();
    assert_eq!(nested_indexed.entries.len(), 2);
    let nested_found = service
        .find_paths(NamespaceFindRequest {
            path: "/nested".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::new("run.kind"),
                op: NamespacePredicateOp::Eq,
                value: Some(NamespacePredicateValue::String("a".to_owned())),
            }],
            sort: Vec::new(),
            include: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(nested_found.match_count, 1);
    assert_eq!(nested_found.matches[0].path, "/nested/a.bin");

    service.remove_file_path("/nested/a.bin").unwrap();
    service.remove_file_path("/nested/b.bin").unwrap();
    service.remove_empty_dir_path("/nested").unwrap();
    service.remove_file_path("/generic/a.bin").unwrap();
    service.remove_file_path("/generic/b.bin").unwrap();
    service.remove_empty_dir_path("/generic").unwrap();
    assert!(service.retire_snapshot(generic.snapshot_id).unwrap());
    assert!(service
        .retire_snapshot(generic_snapshot.snapshot_id)
        .unwrap());
    assert!(service
        .retire_snapshot(source_snapshot.snapshot_id)
        .unwrap());
}

#[test]
fn snapshot_retirement_cannot_remove_a_preparing_restore_history_floor() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RegisterNamespaceIndex, 0, 1)
        .matching_request_prefix(b"restore-init-publish")
        .rejecting(CommandKind::PublishArtifact);
    let service = NoKvFs::new(MountId::new(1).unwrap(), store, MemoryObjectStore::new());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restore = service.restore_subtree_path_to_fork_initialized(
        "/source",
        snapshot.snapshot_id,
        "/destination",
        RestoreInitialization {
            remove_relative_paths: Vec::new(),
            files: vec![RestoreInitializationFile {
                relative_path: "metadata/restore_manifest.json".to_owned(),
                bytes: b"{}".to_vec(),
                content_type: "application/json".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            }],
        },
    );
    assert!(matches!(
        restore,
        Err(MetadError::PublishArtifactFailed { .. })
    ));
    assert!(matches!(
        service.retire_snapshot(snapshot.snapshot_id),
        Err(MetadError::RestoreInProgress)
    ));
}

#[test]
fn ordinary_and_complete_restore_reads_do_not_probe_the_system_keyspace() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let source = service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/source/file.txt", "visibility/read", b"read");

    assert!(!service.restore_staging_slow_path_enabled());
    let before = service.metadata_store_stats();
    assert!(service.get_attr(source.attr.inode).unwrap().is_some());
    let after = service.metadata_store_stats();
    assert_eq!(after.get_total - before.get_total, 1);
    assert_eq!(
        after.get_write_plan_local_total - before.get_write_plan_local_total,
        0,
        "an ordinary inode read must not probe the restore staging inverse"
    );

    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    assert!(!service.restore_staging_slow_path_enabled());
    let before_retry = service.metadata_store_stats();
    let retry = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let after_retry = service.metadata_store_stats();
    assert_eq!(retry, restored);
    assert_eq!(
        after_retry.scan_total - before_retry.scan_total,
        0,
        "an exact terminal retry must not rescan restore-private keyspaces"
    );
    let before = service.metadata_store_stats();
    assert!(service
        .get_attr(restored.destination_root)
        .unwrap()
        .is_some());
    let after = service.metadata_store_stats();
    assert_eq!(after.get_total - before.get_total, 1);
    assert_eq!(
        after.get_write_plan_local_total - before.get_write_plan_local_total,
        0,
        "a completed restore must use the ordinary inode read hot path"
    );

    let reopened = NoKvFs::open_existing(service.mount, metadata, objects, 0).unwrap();
    assert!(!reopened.restore_staging_slow_path_enabled());
    reopened.observe_required_owner_epoch(2).unwrap();
    assert!(!reopened.restore_staging_slow_path_enabled());
    reopened.install_owner_epoch(2).unwrap();
    assert!(!reopened.restore_staging_slow_path_enabled());
    let before = reopened.metadata_store_stats();
    assert!(reopened
        .get_attr(restored.destination_root)
        .unwrap()
        .is_some());
    let after = reopened.metadata_store_stats();
    assert_eq!(after.get_total - before.get_total, 1);
    assert_eq!(
        after.get_write_plan_local_total - before.get_write_plan_local_total,
        0,
        "reopen/failover recovery must restore the completed fast path"
    );
}

#[test]
fn concurrent_ordinary_writes_before_restore_do_not_probe_staging_inverses() {
    const WRITERS: usize = 8;
    const WRITES_PER_WRITER: usize = 32;

    let mount = MountId::new(1).unwrap();
    let metadata = RestoreInverseProbeStore::new(mount);
    let service = Arc::new(NoKvFs::new(
        mount,
        metadata.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let start = Arc::new(Barrier::new(WRITERS));
    let writers = (0..WRITERS)
        .map(|writer| {
            let service = Arc::clone(&service);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for index in 0..WRITES_PER_WRITER {
                    service
                        .set_xattr(
                            InodeId::root(),
                            format!("user.concurrent-{writer}-{index}").as_bytes(),
                            vec![writer as u8, index as u8],
                            XattrSetMode::Any,
                        )
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    assert_eq!(
        metadata.inverse_gets(),
        0,
        "a mount which has never activated restore must use the stable activation fence",
    );
    assert!(metadata
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: metadata.inverse_prefix.clone(),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap()
        .is_empty());
}

#[test]
fn concurrent_non_member_writes_after_restore_never_observe_inverse_sentinels() {
    const WRITERS: usize = 8;
    const WRITES_PER_WRITER: usize = 32;

    let service = Arc::new(service());
    let ordinary = service
        .create_file_path("/ordinary.bin", 0o644, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();

    let start = Arc::new(Barrier::new(WRITERS));
    let writers = (0..WRITERS)
        .map(|writer| {
            let service = Arc::clone(&service);
            let start = Arc::clone(&start);
            let inode = ordinary.attr.inode;
            std::thread::spawn(move || {
                start.wait();
                for index in 0..WRITES_PER_WRITER {
                    service
                        .set_xattr(
                            inode,
                            format!("user.post-restore-{writer}-{index}").as_bytes(),
                            vec![writer as u8, index as u8],
                            XattrSetMode::Any,
                        )
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    assert!(service
        .metadata_store()
        .get(
            RecordFamily::System,
            &restore::restore_staging_inode_key(service.mount_id(), ordinary.attr.inode),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
}

#[test]
fn restore_visibility_recovery_fails_closed_without_a_complete_marker() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let restored = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );
    let operation = service
        .metadata_store()
        .get(
            RecordFamily::System,
            &restore::restore_operation_key(service.mount_id(), &operation_digest),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .map(|value| restore::decode_restore_operation(&value.0).unwrap())
        .unwrap();
    let marker_key =
        restore_index::restore_index_complete_key(service.mount_id(), operation.ref_set_id);
    let marker = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &marker_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();

    // Model a corrupt/replaced image without creating an exposure window in
    // the test process itself.
    service.mark_restore_staging_possible();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-remove-restore-complete-marker",
                service.mount_id(),
                restored.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: marker_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: marker_key.clone(),
                predicate: Predicate::VersionEquals(marker.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, marker_key)],
            watch: Vec::new(),
        })
        .unwrap();

    assert!(matches!(
        service.recover_restore_staging_visibility(),
        Err(MetadError::Codec(message))
            if message == "complete restore operation has no visibility marker"
    ));
    assert!(service.restore_staging_slow_path_enabled());
    assert!(service
        .get_attr(restored.destination_root)
        .unwrap()
        .is_none());
}

#[test]
fn reopen_and_owner_failover_keep_crash_left_restore_staging_hidden() {
    let metadata = SnapshotCommitBarrierStore::new(CommandKind::RegisterNamespaceIndex, 0, 1)
        .matching_request_prefix(b"restore-init-publish")
        .rejecting(CommandKind::PublishArtifact);
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let operation_digest = restore::restore_operation_digest(
        service.mount_id(),
        "/source",
        snapshot.snapshot_id,
        "/destination",
    );
    let restore = service.restore_subtree_path_to_fork_initialized(
        "/source",
        snapshot.snapshot_id,
        "/destination",
        RestoreInitialization {
            remove_relative_paths: Vec::new(),
            files: vec![RestoreInitializationFile {
                relative_path: "metadata/restore_manifest.json".to_owned(),
                bytes: b"{}".to_vec(),
                content_type: "application/json".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            }],
        },
    );
    assert!(matches!(
        restore,
        Err(MetadError::PublishArtifactFailed { .. })
    ));
    let operation = service
        .metadata_store()
        .get(
            RecordFamily::System,
            &restore::restore_operation_key(service.mount_id(), &operation_digest),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .map(|value| restore::decode_restore_operation(&value.0).unwrap())
        .unwrap();
    assert_eq!(operation.state, restore::RestoreOperationState::Preparing);
    assert!(service.restore_staging_slow_path_enabled());
    assert!(service
        .get_attr(operation.destination_root)
        .unwrap()
        .is_none());

    let reopened = NoKvFs::open_existing(service.mount, metadata, objects, 0).unwrap();
    assert!(reopened.restore_staging_slow_path_enabled());
    assert!(reopened
        .get_attr(operation.destination_root)
        .unwrap()
        .is_none());
    reopened.observe_required_owner_epoch(2).unwrap();
    assert!(reopened.restore_staging_slow_path_enabled());
    reopened.install_owner_epoch(2).unwrap();
    assert!(reopened.restore_staging_slow_path_enabled());
    assert!(reopened
        .get_attr(operation.destination_root)
        .unwrap()
        .is_none());
}

#[test]
fn identical_retry_discards_a_preparing_restore_and_rebuilds_it() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing);
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/source/metadata", 0o755, 1000, 1000)
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let initialization = RestoreInitialization {
        remove_relative_paths: Vec::new(),
        files: vec![RestoreInitializationFile {
            relative_path: "metadata/restore_manifest.json".to_owned(),
            bytes: b"durable manifest".to_vec(),
            content_type: "application/json".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        }],
    };
    objects.fail_puts_containing("blocks/");
    assert!(matches!(
        service.restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            initialization.clone(),
        ),
        Err(MetadError::Object(_))
    ));

    objects.clear_faults();
    let mut cleanup_pages = 0_usize;
    let restored = loop {
        match service.restore_subtree_path_to_fork_initialized(
            "/source",
            snapshot.snapshot_id,
            "/destination",
            initialization.clone(),
        ) {
            Ok(outcome) => break outcome,
            Err(MetadError::RestoreInProgress) => {
                cleanup_pages += 1;
                assert!(
                    cleanup_pages <= 64,
                    "bounded restore cleanup failed to converge"
                );
            }
            Err(error) => panic!("identical restore retry failed: {error}"),
        }
    };
    assert!(cleanup_pages > 0);
    assert_eq!(restored.state, RestoreState::Complete);
    assert!(service
        .lookup_path("/destination/metadata/restore_manifest.json")
        .unwrap()
        .is_some());
}

#[test]
fn restore_init_tombstone_cursor_reaches_rows_beyond_one_page() {
    let (service, objects) = service_with_objects();
    let mount = service.mount_id();
    let mut tombstone_rows = Vec::new();
    let mut object_keys = Vec::new();
    for index in 0..3_u64 {
        let inode = InodeId::new(100 + index).unwrap();
        let generation = 200 + index;
        let mut operation_digest = [0_u8; 32];
        operation_digest[31] = index as u8 + 1;
        let tombstone = restore::RestoreInitUploadTombstone {
            operation_digest,
            initialization_digest: [index as u8 + 10; 32],
            inode,
            generation,
            size: 1,
            relative_path: format!("metadata/restore-{index}.json"),
        };
        let key =
            restore::restore_init_upload_tombstone_key(mount, &operation_digest, inode, generation);
        tombstone_rows.push((
            key,
            restore::encode_restore_init_upload_tombstone(&tombstone).unwrap(),
        ));
        let object_key = block_key(inode, generation, 0, 0);
        objects.put(&object_key, vec![index as u8]).unwrap();
        object_keys.push(object_key);
    }
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-restore-init-tombstones",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: tombstone_rows[0].0.clone(),
            predicates: tombstone_rows
                .iter()
                .map(|(key, _)| PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                })
                .collect(),
            mutations: tombstone_rows
                .iter()
                .map(|(key, value)| Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(value.clone())),
                })
                .collect(),
            watch: Vec::new(),
        })
        .unwrap();

    let first = service.cleanup_pending_objects(2).unwrap();
    assert_eq!(first.restore_init_tombstones_scanned, 2);
    assert!(objects.head(&object_keys[0]).unwrap().is_none());
    assert!(objects.head(&object_keys[1]).unwrap().is_none());
    assert!(objects.head(&object_keys[2]).unwrap().is_some());

    let second = service.cleanup_pending_objects(2).unwrap();
    assert_eq!(second.restore_init_tombstones_scanned, 1);
    assert!(objects.head(&object_keys[2]).unwrap().is_none());
    assert_eq!(
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore::restore_init_upload_tombstone_prefix(mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .len(),
        3,
        "automatic cleanup must retain the permanent late-PUT ledger"
    );
}

#[test]
fn restore_release_cursor_does_not_starve_jobs_beyond_one_page() {
    let service = service();
    let mount = service.mount_id();
    let job_keys = (1..=3_u64)
        .map(|ref_set_id| restore::restore_release_job_key(mount, ref_set_id))
        .collect::<Vec<_>>();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-malformed-restore-release-jobs",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: job_keys[0].clone(),
            predicates: job_keys
                .iter()
                .map(|key| PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                })
                .collect(),
            mutations: job_keys
                .iter()
                .enumerate()
                .map(|(index, key)| Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![index as u8])),
                })
                .collect(),
            watch: Vec::new(),
        })
        .unwrap();

    let append_malformed_job = || {
        let version = service.next_version().unwrap();
        let key = restore::restore_release_job_key(mount, version.get());
        service
            .commit_metadata(MetadataCommand {
                request_id: request_id(
                    b"test-append-malformed-restore-release-job",
                    mount,
                    InodeId::root(),
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).unwrap(),
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: key.clone(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                }],
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xff])),
                }],
                watch: Vec::new(),
            })
            .unwrap();
        key
    };

    assert_eq!(service.cleanup_restore_releases(2).unwrap(), 2);
    let first_quarantine = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_quarantine_prefix(mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(first_quarantine.len(), 2);
    let first_appended = append_malformed_job();

    assert_eq!(service.cleanup_restore_releases(2).unwrap(), 1);
    let quarantine = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_quarantine_prefix(mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(quarantine.len(), 3);
    let second_appended = append_malformed_job();
    assert_eq!(
        service.cleanup_restore_releases(2).unwrap(),
        2,
        "a frozen cycle must revisit old jobs while newer job ids keep arriving"
    );
    assert!(first_appended > job_keys[2]);
    assert!(second_appended > first_appended);
    assert_eq!(
        service.cleanup_restore_releases(0).unwrap(),
        1,
        "a zero caller limit has an explicit one-job effective budget"
    );
    for row in &quarantine {
        let decoded = restore::validate_restore_release_quarantine_row(mount, row).unwrap();
        assert_eq!(decoded.family, RecordFamily::System);
        assert_eq!(
            decoded.scope,
            restore::RestoreReleaseQuarantineScope::Diagnostic
        );
        assert!(job_keys.contains(&decoded.original_key));
    }
    assert_eq!(
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore::restore_release_job_prefix(mount),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .len(),
        5,
        "malformed active jobs stay durable for fsck repair"
    );
}

#[test]
fn restore_release_cursor_ack_loss_reopens_with_frozen_cycle() {
    let mount = MountId::new(1).unwrap();
    let metadata = RestorePostApplyFaultStore::new();
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(mount, metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let job_keys = (1..=3_u64)
        .map(|ref_set_id| restore::restore_release_job_key(mount, ref_set_id))
        .collect::<Vec<_>>();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-release-cursor-ack-loss-jobs",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: job_keys[0].clone(),
            predicates: job_keys
                .iter()
                .map(|key| PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                })
                .collect(),
            mutations: job_keys
                .iter()
                .map(|key| Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xfe])),
                })
                .collect(),
            watch: Vec::new(),
        })
        .unwrap();

    metadata.fail_after_apply(b"restore-release-worker-cursor", 0);
    assert!(matches!(
        service.cleanup_restore_releases(2),
        Err(MetadError::Metadata(MetadataError::Backend(message)))
            if message == "injected restore crash after durable apply"
    ));
    let cursor_key = restore::restore_release_cursor_key(mount);
    let cursor_before_reopen = metadata
        .get_versioned(
            RecordFamily::System,
            &cursor_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let frozen =
        restore::decode_restore_release_worker_cursor(mount, &cursor_before_reopen.value.0)
            .unwrap();
    assert_eq!(frozen.start_after, job_keys[1]);

    metadata.clear_fault();
    drop(service);
    let reopened = NoKvFs::open_existing(mount, metadata.clone(), objects, 0).unwrap();
    let append_version = reopened.next_version().unwrap();
    let appended_key = restore::restore_release_job_key(mount, append_version.get());
    assert!(append_version.get() > frozen.cycle_high_water);
    reopened
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-release-cursor-post-reopen-append",
                mount,
                InodeId::root(),
                append_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(append_version).unwrap(),
            commit_version: append_version,
            primary_family: RecordFamily::System,
            primary_key: appended_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: appended_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: appended_key,
                op: MutationOp::Put,
                value: Some(Value(vec![0xfd])),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    assert_eq!(reopened.cleanup_restore_releases(2).unwrap(), 1);
    assert_eq!(
        reopened.cleanup_restore_releases(2).unwrap(),
        2,
        "reopen must preserve the old cycle boundary and revisit its first page"
    );
    assert!(metadata.applied_with_prefix(b"restore-release-worker-cursor") >= 3);
}

#[test]
fn restore_release_cursor_wraps_before_late_old_ref_sets() {
    let service = service();
    let mount = service.mount_id();
    for index in 0..32_u8 {
        service
            .set_xattr(
                InodeId::root(),
                format!("user.release-frontier-{index}").as_bytes(),
                vec![index],
                XattrSetMode::Any,
            )
            .unwrap();
    }
    let job_keys = [10_u64, 20, 30]
        .into_iter()
        .map(|ref_set_id| restore::restore_release_job_key(mount, ref_set_id))
        .collect::<Vec<_>>();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-old-ref-set-release-jobs",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: job_keys[0].clone(),
            predicates: job_keys
                .iter()
                .map(|key| PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                })
                .collect(),
            mutations: job_keys
                .iter()
                .map(|key| Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xfc])),
                })
                .collect(),
            watch: Vec::new(),
        })
        .unwrap();
    assert_eq!(service.cleanup_restore_releases(2).unwrap(), 2);
    let cursor_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &restore::restore_release_cursor_key(mount),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let frozen =
        restore::decode_restore_release_worker_cursor(mount, &cursor_item.value.0).unwrap();
    assert!(frozen.cycle_high_water > 22);
    assert_eq!(frozen.start_after, job_keys[1]);

    let late_keys = [21_u64, 22]
        .into_iter()
        .map(|ref_set_id| restore::restore_release_job_key(mount, ref_set_id))
        .collect::<Vec<_>>();
    let late_version = service.next_version().unwrap();
    assert!(late_version.get() > frozen.cycle_high_water);
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-late-old-ref-set-release-jobs",
                mount,
                InodeId::root(),
                late_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(late_version).unwrap(),
            commit_version: late_version,
            primary_family: RecordFamily::System,
            primary_key: late_keys[0].clone(),
            predicates: late_keys
                .iter()
                .map(|key| PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                })
                .collect(),
            mutations: late_keys
                .iter()
                .map(|key| Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xfb])),
                })
                .collect(),
            watch: Vec::new(),
        })
        .unwrap();

    assert_eq!(
        service.cleanup_restore_releases(2).unwrap(),
        2,
        "new rows for old ref-set ids must end the old cycle before its cursor chases them"
    );
    let quarantine = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_release_quarantine_prefix(mount),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(quarantine.len(), 2, "the first page was revisited promptly");
}

#[test]
fn restore_release_cursor_raises_frontier_after_blocked_row_before_boundary() {
    let service = service();
    let mount = service.mount_id();
    for index in 0..24_u8 {
        service
            .set_xattr(
                InodeId::root(),
                format!("user.release-mixed-{index}").as_bytes(),
                vec![index],
                XattrSetMode::Any,
            )
            .unwrap();
    }
    let blocked_key = restore::restore_release_job_key(mount, 10);
    let blocked_version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-release-mixed-blocked-row",
                mount,
                InodeId::root(),
                blocked_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(blocked_version).unwrap(),
            commit_version: blocked_version,
            primary_family: RecordFamily::System,
            primary_key: blocked_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: blocked_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: blocked_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(vec![0xfa])),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    let cursor_key = restore::restore_release_cursor_key(mount);
    let cursor_version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-release-mixed-cursor",
                mount,
                InodeId::root(),
                cursor_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(cursor_version).unwrap(),
            commit_version: cursor_version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: cursor_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cursor_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(
                    restore::encode_restore_release_worker_cursor(
                        mount,
                        &restore::RestoreReleaseWorkerCursor {
                            cycle_high_water: blocked_version.get(),
                            start_after: Vec::new(),
                        },
                    )
                    .unwrap(),
                )),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    let changed_key = restore::restore_release_job_key(mount, 20);
    let changed_version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-release-mixed-changed-row",
                mount,
                InodeId::root(),
                changed_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(changed_version).unwrap(),
            commit_version: changed_version,
            primary_family: RecordFamily::System,
            primary_key: changed_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: changed_key,
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: restore::restore_release_job_key(mount, 20),
                op: MutationOp::Put,
                value: Some(Value(vec![0xf9])),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    assert_eq!(service.cleanup_restore_releases(64).unwrap(), 1);
    let raised = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &cursor_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let raised = restore::decode_restore_release_worker_cursor(mount, &raised.value.0).unwrap();
    assert!(raised.cycle_high_water >= changed_version.get());
    assert_eq!(raised.start_after, Vec::<u8>::new());
    assert_eq!(
        service.cleanup_restore_releases(64).unwrap(),
        2,
        "a blocked older row must not pin a newer row-version boundary"
    );
}

#[test]
fn restore_release_worker_moves_noncanonical_job_key_out_of_scan_path() {
    let service = service();
    let mount = service.mount_id();
    let mut invalid_key = restore::restore_release_job_prefix(mount);
    invalid_key.extend_from_slice(&0_u64.to_be_bytes());
    let valid_key = restore::restore_release_job_key(mount, 1);
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-noncanonical-release-job-key",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: invalid_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: invalid_key.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: valid_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![
                Mutation {
                    family: RecordFamily::System,
                    key: invalid_key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xf8])),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: valid_key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![0xf7])),
                },
            ],
            watch: Vec::new(),
        })
        .unwrap();

    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert!(service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &invalid_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_none());
    let cursor = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &restore::restore_release_cursor_key(mount),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    restore::decode_restore_release_worker_cursor_at_version(
        mount,
        &cursor.value.0,
        service.read_version().unwrap(),
    )
    .unwrap();
    assert_eq!(service.cleanup_restore_releases(1).unwrap(), 1);
    assert!(service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &valid_key,
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_some());
}

#[test]
fn idle_restore_release_cursor_does_not_write_metadata() {
    let service = service();
    let mount = service.mount_id();
    let cursor_key = restore::restore_release_cursor_key(mount);
    let high_water = service.read_version().unwrap().get();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-install-idle-release-cursor",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: cursor_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cursor_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(
                    restore::encode_restore_release_worker_cursor(
                        mount,
                        &restore::RestoreReleaseWorkerCursor {
                            cycle_high_water: high_water,
                            start_after: Vec::new(),
                        },
                    )
                    .unwrap(),
                )),
            }],
            watch: Vec::new(),
        })
        .unwrap();
    let before = service.read_version().unwrap();
    let cursor_version = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &cursor_key,
            before,
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap()
        .version;
    assert_eq!(service.cleanup_restore_releases(64).unwrap(), 0);
    assert_eq!(service.cleanup_restore_releases(64).unwrap(), 0);
    assert_eq!(service.read_version().unwrap(), before);
    assert_eq!(
        service
            .metadata_store()
            .get_versioned(
                RecordFamily::System,
                &cursor_key,
                before,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap()
            .version,
        cursor_version
    );
}

#[test]
fn future_restore_release_cursor_fails_fsck_cleanup_and_downgrade() {
    let service = service();
    let mount = service.mount_id();
    let cursor_key = restore::restore_release_cursor_key(mount);
    let read_version = service.read_version().unwrap();
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-install-future-release-cursor",
                mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: cursor_key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cursor_key,
                op: MutationOp::Put,
                value: Some(Value(
                    restore::encode_restore_release_worker_cursor(
                        mount,
                        &restore::RestoreReleaseWorkerCursor {
                            cycle_high_water: read_version.get() + 1_000,
                            start_after: Vec::new(),
                        },
                    )
                    .unwrap(),
                )),
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let report = service.fsck_restore_state(false).unwrap();
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.code == "restore_cursor_invalid"));
    assert!(matches!(
        service.cleanup_restore_releases(1),
        Err(MetadError::Codec(message))
            if message == "restore release worker cursor is ahead of metadata"
    ));
    assert!(matches!(
        service.drain_restore_to_fork_v1(
            RestoreCapabilityDisabled::acknowledged(),
            RestoreWritersQuiesced::acknowledged(),
        ),
        Err(RestoreDowngradeError::Metadata(MetadError::Codec(message)))
            if message == "restore release worker cursor is ahead of metadata"
    ));
}

#[test]
fn restore_inverse_owner_registry_prevents_target_first_orphan_finalization() {
    let service = service();
    service
        .create_dir_path("/source", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/source/shared.bin",
        "inverse-closure",
        b"borrowed bytes",
    );
    let snapshot = service.snapshot_subtree_path("/source").unwrap();
    let outcome = service
        .restore_subtree_path_to_fork("/source", snapshot.snapshot_id, "/destination")
        .unwrap();
    let mount = service.mount_id();
    let operation_digest =
        restore::restore_operation_digest(mount, "/source", snapshot.snapshot_id, "/destination");
    let operation_item = service
        .metadata_store()
        .get_versioned(
            RecordFamily::System,
            &restore::restore_operation_key(mount, &operation_digest),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .unwrap();
    let operation = restore::decode_restore_operation(&operation_item.value.0).unwrap();
    assert_eq!(operation.destination_root, outcome.destination_root);

    let owner_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_base_owner_prefix(mount, operation.ref_set_id),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(owner_rows.len(), 1);
    let inverse_owner_rows = service
        .metadata_store()
        .scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore::restore_base_inverse_owner_prefix(mount, operation.ref_set_id),
            start_after: None,
            version: service.read_version().unwrap(),
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })
        .unwrap();
    assert_eq!(inverse_owner_rows.len(), 1);

    // Simulate a damaged owner-first row while leaving the target-first inverse
    // and its ref-set registry. The registry must keep the operation durable;
    // otherwise finalization would erase the only identity fsck can use to
    // repair the orphan inverse.
    let owner = &owner_rows[0];
    let version = service.next_version().unwrap();
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-drop-restore-base-owner",
                mount,
                outcome.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: owner.key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: owner.key.clone(),
                predicate: Predicate::VersionEquals(owner.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, owner.key.clone())],
            watch: Vec::new(),
        })
        .unwrap();

    service
        .create_dir_path("/replacement", 0o755, 1000, 1000)
        .unwrap();
    service
        .rename_replace_path("/replacement", "/destination")
        .unwrap();
    for _ in 0..4 {
        service.cleanup_restore_releases(1).unwrap();
    }
    assert_eq!(service.restore_metrics().unwrap().releasing, 1);
    assert!(service
        .metadata_store()
        .get(
            RecordFamily::System,
            &restore::restore_operation_key(mount, &operation_digest),
            service.read_version().unwrap(),
            ReadPurpose::WritePlanLocal,
        )
        .unwrap()
        .is_some());
    assert_eq!(
        service
            .metadata_store()
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore::restore_base_inverse_owner_prefix(mount, operation.ref_set_id,),
                start_after: None,
                version: service.read_version().unwrap(),
                limit: 0,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn snapshot_renew_reports_a_concurrent_root_rebind() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::RenewSnapshot, 1, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    let root = service.resolve_directory_path("/a").unwrap();
    let pin = service.snapshot_subtree_with_lease(root, 10_000).unwrap();

    let renew_service = Arc::clone(&service);
    let renew = std::thread::spawn(move || {
        renew_service.renew_snapshot_path("/a", pin.snapshot_id, 20_000)
    });
    store.wait_until_blocked();
    service.rename_path("/a", "/moved").unwrap();
    store.release_blocked();

    assert!(matches!(
        renew.join().unwrap(),
        Err(MetadError::SnapshotBindingChanged { root_path }) if root_path == "/a"
    ));
}

#[test]
fn snapshot_mint_rejects_a_concurrent_root_rebind() {
    let store = SnapshotCommitBarrierStore::new(CommandKind::SnapshotSubtree, 1, 2);
    let service = Arc::new(NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    ));
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();

    let mint_service = Arc::clone(&service);
    let mint =
        std::thread::spawn(move || mint_service.snapshot_subtree_path_with_lease("/a", 10_000));
    store.wait_until_blocked();
    service.rename_path("/a", "/moved").unwrap();
    store.release_blocked();

    assert!(matches!(
        mint.join().unwrap(),
        Err(MetadError::SnapshotBindingChanged { root_path }) if root_path == "/a"
    ));
    assert_eq!(service.metadata_store_stats().active_snapshot_pin_total, 0);
}

#[test]
fn snapshot_mint_retries_a_stable_binding_after_a_planning_conflict() {
    let store = SnapshotPredicateOnceStore::new();
    let service = NoKvFs::new(MountId::new(1).unwrap(), store, MemoryObjectStore::new());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();

    let pin = service
        .snapshot_subtree_path_with_lease("/a", 10_000)
        .unwrap();

    assert_eq!(service.metadata_store_stats().active_snapshot_pin_total, 1);
    assert_eq!(service.snapshot_pin(pin.snapshot_id).unwrap(), Some(pin));
}

#[test]
fn gc_reaps_expired_snapshot_pins_but_keeps_live_ones() {
    let (service, _objects) = service_with_objects();
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/b", 0o755, 1000, 1000).unwrap();
    let a = service.resolve_directory_path("/a").unwrap();
    let b = service.resolve_directory_path("/b").unwrap();
    let expired = service.snapshot_subtree_with_lease(a, 0).unwrap();
    let live = service.snapshot_subtree_with_lease(b, 3_600_000).unwrap();

    // An object-GC pass reaps expired pins as housekeeping, keeping live ones.
    service.cleanup_pending_objects(100).unwrap();
    assert!(service.snapshot_pin(expired.snapshot_id).unwrap().is_none());
    assert!(service.snapshot_pin(live.snapshot_id).unwrap().is_some());
}

#[test]
fn clone_is_batched_per_dir_and_diff_is_o_tree() {
    // Pins the measured complexity: clone is batched per source directory (one
    // commit per directory, NOT one per entry — well below the JuiceFS-class
    // per-entry cost), while diff still walks the whole tree (O(tree)) — a one-file
    // change costs the same full-tree walk, so diff is not yet O(changes) (tracked
    // future work).
    let (service, _objects) = service_with_objects();
    let dirs = 6usize;
    let files = 6usize;
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    for d in 0..dirs {
        service
            .create_dir_path(&format!("/base/d{d}"), 0o755, 1000, 1000)
            .unwrap();
        for f in 0..files {
            publish_path_artifact(
                &service,
                &format!("/base/d{d}/f{f}.bin"),
                &format!("m{d}-{f}"),
                b"x",
            );
        }
    }
    let entries = dirs * (1 + files); // each d{d} directory + its files

    // CLONE: batched per source directory — one commit per directory, NOT one per
    // entry — so commit count stays far below the entry count.
    let before = service.metadata_store_stats().commit_total;
    service.clone_subtree_path_into("/base", "/fork").unwrap();
    let clone_commits = service.metadata_store_stats().commit_total - before;
    assert!(
        clone_commits < entries as u64,
        "clone batches per directory, not per entry: entries={entries} commits={clone_commits}"
    );
    assert!(
        clone_commits >= dirs as u64,
        "clone still commits at least once per directory: dirs={dirs} commits={clone_commits}"
    );

    // DIFF (clean): scans scale with the directory count → O(tree).
    let before = service.metadata_store_stats().scan_total;
    let clean = service.diff_subtrees_path("/base", "/fork").unwrap();
    let scans_clean = service.metadata_store_stats().scan_total - before;
    assert!(clean.is_empty(), "a fresh clone diffs clean: {clean:?}");
    assert!(
        scans_clean >= dirs as u64,
        "diff walks every directory: dirs={dirs} scans={scans_clean}"
    );

    // DIFF after ONE change: still the full-tree walk → NOT O(changes).
    publish_path_artifact(&service, "/fork/d0/added.bin", "m-added", b"yy");
    let before = service.metadata_store_stats().scan_total;
    let dirty = service.diff_subtrees_path("/base", "/fork").unwrap();
    let scans_dirty = service.metadata_store_stats().scan_total - before;
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].kind, SubtreeDeltaKind::Added);
    assert!(
        scans_dirty >= scans_clean,
        "diff cost does not shrink with change count (O(tree), not O(changes)): \
         clean={scans_clean} dirty={scans_dirty}"
    );
}

#[test]
#[ignore = "scale bench; run: cargo test -p nokv-meta --release -- --ignored bench_clone_and_diff_scale --nocapture"]
fn bench_clone_and_diff_scale() {
    use std::time::Instant;
    // The constant behind the O(entries) clone / O(tree) diff, in release. Tells us
    // whether the best-of-N demo (clone N forks of a node_modules-scale tree, diff
    // each) is viable as-is or needs the clone-commit batching first.
    eprintln!("\nentries     clone_ms   us/entry   diff_clean_ms   diff_1change_ms");
    for &(dirs, files) in &[
        (10usize, 10usize),
        (50, 20),
        (100, 50),
        (200, 80),
        (300, 100),
    ] {
        let entries = dirs * (1 + files);
        let (service, _objects) = service_with_objects();
        service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
        for d in 0..dirs {
            service
                .create_dir_path(&format!("/base/d{d}"), 0o755, 1000, 1000)
                .unwrap();
            for f in 0..files {
                publish_path_artifact(
                    &service,
                    &format!("/base/d{d}/f{f}.bin"),
                    &format!("m{d}-{f}"),
                    b"x",
                );
            }
        }

        let t = Instant::now();
        service.clone_subtree_path_into("/base", "/fork").unwrap();
        let clone_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let clean = service.diff_subtrees_path("/base", "/fork").unwrap();
        let diff_clean_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert!(clean.is_empty());

        publish_path_artifact(&service, "/fork/d0/added.bin", "m-added", b"yy");
        let t = Instant::now();
        let dirty = service.diff_subtrees_path("/base", "/fork").unwrap();
        let diff_1change_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(dirty.len(), 1);

        eprintln!(
            "{entries:7}   {clone_ms:8.2}   {:8.2}   {diff_clean_ms:13.2}   {diff_1change_ms:15.2}",
            clone_ms * 1000.0 / entries as f64
        );
    }
}

#[test]
fn publish_checkpoint_is_atomic_multi_shard_and_range_readable() {
    let (service, _objects) = service_with_objects();
    let ckpt = service.create_dir_path("/ckpt", 0o755, 1000, 1000).unwrap();
    let shards: Vec<CheckpointShard> = (0..5u8)
        .map(|i| CheckpointShard {
            name: DentryName::new(format!("shard{i}").into_bytes()).unwrap(),
            bytes: vec![b'A' + i; 100 + 50 * i as usize],
        })
        .collect();

    // ATOMIC: all 5 shards land together — far fewer commits than 5 separate
    // publishes (one batched commit, not one-per-shard).
    let before = service.metadata_store_stats().commit_total;
    let handle = service
        .publish_checkpoint(ckpt.attr.inode, shards, 1000, 1000)
        .unwrap();
    let commits = service.metadata_store_stats().commit_total - before;
    assert_eq!(handle.shards.len(), 5);
    assert!(
        commits <= 2,
        "checkpoint shards must commit atomically in one batched command, not per shard: commits={commits}"
    );

    // All shards visible after the single publish.
    for i in 0..5u8 {
        assert!(service
            .lookup_path(&format!("/ckpt/shard{i}"))
            .unwrap()
            .is_some());
    }

    // RESHARD-ON-READ: an arbitrary byte range of a shard returns the right bytes
    // (what a differently-parallelized restore reads — a plain range read).
    let s1 = service.lookup_path("/ckpt/shard1").unwrap().unwrap();
    assert_eq!(s1.attr.size, 150);
    assert_eq!(
        service.read_file(s1.attr.inode, 40, 60).unwrap(),
        vec![b'B'; 60]
    );

    // CoW version pin: snapshot the checkpoint dir = a parallelism-agnostic version.
    let pin = service.snapshot_subtree(ckpt.attr.inode).unwrap();
    assert!(service.snapshot_pin(pin.snapshot_id).unwrap().is_some());
}

#[test]
fn open_read_is_zero_write_and_generation_cas_catches_supersede() {
    let (service, _objects) = service_with_objects();
    let data = service.create_dir_path("/data", 0o755, 1000, 1000).unwrap();
    let v1 = publish_path_artifact(&service, "/data/ckpt.bin", "ckpt", b"AAAA");

    // open_read writes ZERO metadata and captures the current (generation, version).
    let before = service.metadata_store_stats().commit_total;
    let lease = service.open_read(v1.attr.inode).unwrap();
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before,
        "read-mode open must create zero metadata state"
    );
    assert_eq!(lease.inode, v1.attr.inode);
    assert_eq!(lease.generation, v1.attr.generation);

    // The leased generation is the reshard-on-read substrate: an arbitrary byte
    // range read against it succeeds (a differently-parallelized consumer's read).
    let plan = service
        .read_file_plan(lease.inode, lease.generation, 1, 2)
        .unwrap();
    assert_eq!(plan.output_len, 2);

    // Supersede the artifact (immutable CoW rewrite -> a new generation).
    let v2 = republish_path_artifact(&service, data.attr.inode, "ckpt.bin", "ckpt", b"BBBBBB");
    assert_ne!(v2.attr.generation, v1.attr.generation);

    // The stale lease's generation no longer matches the live attr: the CAS in
    // read_file_plan fails fast instead of returning stale/reclaimed bytes.
    assert!(matches!(
        service.read_file_plan(lease.inode, lease.generation, 0, 4),
        Err(MetadError::StaleBodyGeneration { .. })
    ));
    // open_read_expecting(old gen) rejects too; a fresh open observes the new gen.
    assert!(matches!(
        service.open_read_expecting(v1.attr.inode, Some(v1.attr.generation)),
        Err(MetadError::StaleBodyGeneration { .. })
    ));
    let lease2 = service.open_read(v1.attr.inode).unwrap();
    assert_eq!(lease2.generation, v2.attr.generation);
    assert!(lease2.read_version >= lease.read_version);
}

/// Externally persist a durable allocator record (simulating a control-plane
/// epoch bump or another incarnation writing the System record).
fn commit_allocator_record(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    version: u64,
    next_inode: u64,
    epoch: u64,
) {
    let commit_version = Version::new(version).unwrap();
    let key = allocator_key(MountId::new(1).unwrap());
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-alloc-epoch",
                MountId::new(1).unwrap(),
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::ReserveAllocator,
            read_version: predecessor(commit_version).unwrap(),
            commit_version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: Vec::new(),
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_allocator_state(version, next_inode, epoch))),
            }],
            watch: Vec::new(),
        })
        .unwrap();
}

#[test]
fn allocator_epoch_recovers_monotonically_via_fetch_max() {
    let service = service();
    assert_eq!(
        service.allocator_epoch(),
        1,
        "a single owner starts at epoch 1"
    );

    // A control plane bumps the durable epoch (ownership transfer / new incarnation).
    commit_allocator_record(&service, 100, 500, 5);
    service.refresh_allocator_state().unwrap();
    assert_eq!(
        service.allocator_epoch(),
        5,
        "refresh folds in the higher durable epoch"
    );

    // A record carrying a LOWER epoch (a stale incarnation) must never lower it:
    // recovery is fetch_max, so the allocation-authority epoch never regresses —
    // a stale owner can't re-persist itself as current.
    commit_allocator_record(&service, 200, 600, 2);
    service.refresh_allocator_state().unwrap();
    assert_eq!(
        service.allocator_epoch(),
        5,
        "epoch must be monotonic across refresh (fetch_max, not store)"
    );
}

#[test]
fn owner_epoch_fence_rejects_single_metadata_commit() {
    let service = service();
    service.observe_required_owner_epoch(2).unwrap();
    let before = service.metadata_store_stats();

    let err = service
        .create_dir_path("/stale-owner", 0o755, 1000, 1000)
        .unwrap_err();

    assert!(matches!(
        err,
        MetadError::StaleOwnerEpoch {
            owner_epoch: 1,
            required_epoch: 2
        }
    ));
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before.commit_total,
        "stale-owner commit must be rejected before durable metadata apply"
    );
}

#[test]
fn owner_epoch_fence_rejects_independent_batch_commit() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service.observe_required_owner_epoch(2).unwrap();
    let before = service.metadata_store_stats();

    let results = service.create_file_batches_in_dir_path(vec![CreateInDirPathBatch {
        parent_path: "/runs".to_owned(),
        names: vec![
            DentryName::new(b"a.bin".to_vec()).unwrap(),
            DentryName::new(b"b.bin".to_vec()).unwrap(),
        ],
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }]);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0].as_ref().unwrap_err(),
        MetadError::StaleOwnerEpoch {
            owner_epoch: 1,
            required_epoch: 2
        }
    ));
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before.commit_total,
        "stale-owner batch must be rejected before durable metadata apply"
    );
}

#[test]
fn installed_owner_epoch_allows_new_owner_commit() {
    let service = service();
    service.observe_required_owner_epoch(5).unwrap();
    assert!(matches!(
        service.create_dir_path("/blocked", 0o755, 1000, 1000),
        Err(MetadError::StaleOwnerEpoch {
            owner_epoch: 1,
            required_epoch: 5
        })
    ));

    service.install_owner_epoch(5).unwrap();
    let created = service
        .create_dir_path("/new-owner", 0o755, 1000, 1000)
        .unwrap();

    assert_eq!(created.dentry.name.as_bytes(), b"new-owner");
    assert_eq!(service.allocator_epoch(), 5);
    assert_eq!(service.required_owner_epoch(), 5);
}

#[test]
fn lease_deadline_fences_commit_when_passed() {
    let service = service();
    service.set_clock_override_ms(1_000);
    service.set_lease_deadline(5_000);
    // Within the lease window the commit succeeds.
    service
        .create_dir_path("/within-lease", 0o755, 1000, 1000)
        .unwrap();

    // The clock advances past the deadline with no renewal: the owner
    // self-fences here even though no higher epoch was ever observed (the
    // partition split-brain case the epoch fence alone cannot catch).
    service.set_clock_override_ms(6_000);
    let err = service
        .create_dir_path("/after-deadline", 0o755, 1000, 1000)
        .unwrap_err();
    assert!(matches!(
        err,
        MetadError::LeaseExpired {
            now_ms: 6_000,
            deadline_ms: 5_000
        }
    ));
}

#[test]
fn lease_deadline_fences_commit_at_exact_deadline() {
    let service = service();
    service.set_clock_override_ms(1_000);
    service.set_lease_deadline(5_000);
    // A commit strictly inside the window still succeeds.
    service
        .create_dir_path("/before-deadline", 0o755, 1000, 1000)
        .unwrap();

    // At exactly the deadline the control plane already considers the lease
    // expired, so the owner must reject rather than racing the handoff.
    service.set_clock_override_ms(5_000);
    let err = service
        .create_dir_path("/at-deadline", 0o755, 1000, 1000)
        .unwrap_err();
    assert!(matches!(
        err,
        MetadError::LeaseExpired {
            now_ms: 5_000,
            deadline_ms: 5_000
        }
    ));
}

#[test]
fn lease_deadline_fences_independent_batch_commit() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service.set_clock_override_ms(1_000);
    service.set_lease_deadline(2_000);
    service.set_clock_override_ms(3_000);

    let results = service.create_file_batches_in_dir_path(vec![CreateInDirPathBatch {
        parent_path: "/runs".to_owned(),
        names: vec![DentryName::new(b"a.bin".to_vec()).unwrap()],
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }]);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0].as_ref().unwrap_err(),
        MetadError::LeaseExpired {
            now_ms: 3_000,
            deadline_ms: 2_000
        }
    ));
}

#[test]
fn lease_deadline_zero_disables_self_fence() {
    let service = service();
    // No deadline armed (0): single-node/manual owners are never time-fenced.
    assert_eq!(service.lease_deadline_ms(), 0);
    service.set_clock_override_ms(1_000_000);
    service
        .create_dir_path("/no-deadline", 0o755, 1000, 1000)
        .unwrap();
}

#[test]
fn with_shard_index_mints_inodes_in_shard_subspace() {
    let shard3 = service().with_shard_index(3);
    assert_eq!(shard3.shard_index(), 3);
    // A newly minted inode carries this shard's index in its high bits, so it is
    // globally unique across shards and self-routing.
    let dir = shard3.create_dir_path("/d", 0o755, 1000, 1000).unwrap();
    assert_eq!(dir.attr.inode.shard_index(), 3);
    // The default shard is the identity (no high bits).
    let shard0 = service().with_shard_index(0);
    let dir0 = shard0.create_dir_path("/d", 0o755, 1000, 1000).unwrap();
    assert_eq!(dir0.attr.inode.shard_index(), 0);
}

#[test]
fn same_shard_rename_and_link_are_unaffected_by_cross_shard_fence() {
    // On a non-default shard, every inode carries this shard's index, so the
    // cross-shard fence is a no-op: same-shard rename and hardlink still work.
    let service = service().with_shard_index(2);
    let dir = service.create_dir_path("/d", 0o755, 1000, 1000).unwrap();
    let old_name = DentryName::new(b"a".to_vec()).unwrap();
    let new_name = DentryName::new(b"b".to_vec()).unwrap();
    let created = service
        .create_file(dir.attr.inode, old_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(created.attr.inode.shard_index(), 2);

    // Rename within the shard succeeds and keeps the inode.
    let renamed = service
        .rename(dir.attr.inode, &old_name, dir.attr.inode, new_name.clone())
        .unwrap();
    assert_eq!(renamed.attr.inode, created.attr.inode);

    // Hardlink within the shard succeeds and bumps nlink.
    let link_name = DentryName::new(b"b.link".to_vec()).unwrap();
    let linked = service
        .link(created.attr.inode, dir.attr.inode, link_name.clone())
        .unwrap();
    assert_eq!(linked.attr.inode, created.attr.inode);
    assert_eq!(linked.attr.nlink, 2);
}

#[test]
fn inode_rename_to_foreign_shard_parent_is_cross_shard_no_op() {
    // This service owns shard 1; a `new_parent` carrying shard index 0 addresses a
    // foreign namespace. The rename must reject with `CrossShard` before any
    // mutation, not resolve the foreign parent as `NotFound`.
    let service = service().with_shard_index(1);
    let dir = service.create_dir_path("/d", 0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"a".to_vec()).unwrap();
    let created = service
        .create_file(dir.attr.inode, name.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(dir.attr.inode.shard_index(), 1);

    // A directory inode minted by shard 0 (the default shard): foreign to shard 1.
    let foreign_parent = InodeId::compose(0, 99).unwrap();
    assert_eq!(foreign_parent.shard_index(), 0);
    let new_name = DentryName::new(b"moved".to_vec()).unwrap();

    let err = service
        .rename(dir.attr.inode, &name, foreign_parent, new_name)
        .unwrap_err();
    assert!(
        matches!(
            err,
            MetadError::CrossShard {
                source_shard: 1,
                dest_shard: 0
            }
        ),
        "expected CrossShard, got {err:?}"
    );

    // No namespace change: the source dentry still resolves to the same inode.
    assert_eq!(
        service
            .lookup_plus(dir.attr.inode, &name)
            .unwrap()
            .unwrap()
            .attr
            .inode,
        created.attr.inode
    );
}

#[test]
fn inode_link_to_foreign_shard_parent_is_cross_shard_no_op() {
    // Hardlinking a shard-1 inode into a shard-0 directory crosses a boundary and
    // must reject with `CrossShard` before bumping nlink.
    let service = service().with_shard_index(1);
    let dir = service.create_dir_path("/d", 0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"a".to_vec()).unwrap();
    let created = service
        .create_file(dir.attr.inode, name.clone(), 0o644, 1000, 1000)
        .unwrap();
    let before_nlink = created.attr.nlink;

    let foreign_parent = InodeId::compose(0, 7).unwrap();
    let link_name = DentryName::new(b"x.link".to_vec()).unwrap();

    let err = service
        .link(created.attr.inode, foreign_parent, link_name)
        .unwrap_err();
    assert!(
        matches!(
            err,
            MetadError::CrossShard {
                source_shard: 1,
                dest_shard: 0
            }
        ),
        "expected CrossShard, got {err:?}"
    );

    // No mutation: nlink is unchanged.
    assert_eq!(
        service
            .lookup_plus(dir.attr.inode, &name)
            .unwrap()
            .unwrap()
            .attr
            .nlink,
        before_nlink
    );
}

/// Build a shard service over a freshly held in-memory store, with its root
/// bootstrapped at the global root inode. Returns the store handle so a test can
/// drive `recover_allocator_state` against it directly. `shard_index` seeds the
/// inode allocator into the shard's high-bit subspace, exactly like a fleet node.
fn shard_service(
    shard_index: u16,
) -> (
    NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    HoltMetadataStore,
) {
    let store = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        store.clone(),
        MemoryObjectStore::new(),
    )
    .with_shard_index(shard_index);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    (service, store)
}

#[test]
fn cross_shard_graft_is_traversable_without_inode_record() {
    // Two independent shards, each bootstrapping its namespace root at the global
    // root inode (== 1). Shard 1 owns the `/dataset` subtree; shard 0 only needs a
    // graft dentry pointing at it so FUSE traversal `lookup(root, "dataset")`
    // (which routes by the parent inode 1 -> shard 0) resolves instead of ENOENT.
    let (shard0, _store0) = shard_service(0);
    let (shard1, _store1) = shard_service(1);
    let dataset = DentryName::new(b"dataset".to_vec()).unwrap();

    // The subtree dir is created on its owning shard with a real inode that
    // carries shard 1's index in its high bits.
    let subtree = shard1
        .create_dir(InodeId::root(), dataset.clone(), 0o755, 1000, 1000)
        .unwrap();
    let foreign_inode = subtree.attr.inode;
    assert_eq!(foreign_inode.shard_index(), 1);

    // Shard 0 installs the graft: dentry only, pointing at the foreign inode.
    let graft = shard0
        .create_graft(
            InodeId::root(),
            dataset.clone(),
            foreign_inode,
            0o755,
            1000,
            1000,
        )
        .unwrap();
    assert_eq!(graft.dentry.child, foreign_inode);
    assert_eq!(graft.attr.inode, foreign_inode);
    assert_eq!(graft.dentry.child_type, FileType::Directory);

    // FUSE-style lookup by parent inode on shard 0 now resolves to the foreign
    // subtree inode, with the embedded directory attr served from the projection.
    let looked_up = shard0
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .expect("graft dentry must resolve on the parent shard");
    assert_eq!(looked_up.dentry.child, foreign_inode);
    assert_eq!(looked_up.attr.inode, foreign_inode);
    assert_eq!(looked_up.attr.file_type, FileType::Directory);

    // readdir on the parent shard includes exactly the graft entry.
    let entries = shard0.read_dir_plus(InodeId::root()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name, dataset);
    assert_eq!(entries[0].dentry.child, foreign_inode);

    // The allocator-safety invariant: shard 0 holds NO Inode record for the
    // foreign inode. `get_attr` fetches `inode_key`, so it must be absent — the
    // graft is a pure dentry projection.
    assert!(
        shard0.get_attr(foreign_inode).unwrap().is_none(),
        "graft must not write an Inode record for the foreign child"
    );
}

/// A minimal read-only `MetadataStore` that serves a fixed set of records to the
/// allocator recovery fold. It has NO durable allocator System record, so
/// recovery always takes the scan-and-fold FALLBACK path — the path the
/// shard-index guard lives on. It serves exactly the Inode/Dentry rows under
/// test and nothing else, isolating the guard logic from any other family.
/// (The fallback path is also covered against a real, fully-populated Holt store
/// by `fallback_recovery_survives_command_dedupe_rows_on_real_store`.)
struct FixedRecoveryStore {
    rows: Vec<ScanItem>,
    row_family: Vec<RecordFamily>,
}

impl FixedRecoveryStore {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            row_family: Vec::new(),
        }
    }

    fn push(&mut self, family: RecordFamily, key: Vec<u8>, value: Vec<u8>, version: u64) {
        // Recovery never inspects the key bytes (only the family it scanned and
        // the decoded value), so the stored key is kept verbatim.
        self.rows.push(ScanItem {
            key,
            value: Value(value),
            version: Version::new(version).unwrap(),
        });
        self.row_family.push(family);
    }
}

impl MetadataStore for FixedRecoveryStore {
    fn get_versioned(
        &self,
        _family: RecordFamily,
        _key: &[u8],
        _version: Version,
        _purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        // No durable allocator record -> recovery falls through to the scan path.
        Ok(None)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        Ok(self
            .rows
            .iter()
            .zip(self.row_family.iter())
            .filter(|(_, family)| **family == request.family)
            .map(|(row, _)| row.clone())
            .collect())
    }

    fn commit_metadata(&self, _command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        unreachable!("recovery is read-only")
    }

    fn committed_request_result(
        &self,
        _request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        unreachable!("recovery is read-only")
    }

    fn prune_history(
        &self,
        _request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        unreachable!("recovery does not prune")
    }
}

#[test]
fn cross_shard_graft_does_not_poison_parent_allocator() {
    // After a graft, a fallback allocator rebuild on the parent shard must not be
    // dragged up to the foreign child's id. The foreign inode lives in shard 1's
    // subspace (>> shard 0's), so folding it would make shard 0 hand out ids it
    // does not own.
    let (shard0, _store0) = shard_service(0);
    let (shard1, _store1) = shard_service(1);
    let dataset = DentryName::new(b"dataset".to_vec()).unwrap();

    let subtree = shard1
        .create_dir(InodeId::root(), dataset.clone(), 0o755, 1000, 1000)
        .unwrap();
    let foreign_inode = subtree.attr.inode;
    assert_eq!(foreign_inode.shard_index(), 1);
    let graft = shard0
        .create_graft(InodeId::root(), dataset, foreign_inode, 0o755, 1000, 1000)
        .unwrap();

    // Reconstruct, from real encoded records, the exact rows a fallback rebuild
    // of shard 0's allocator would scan: shard 0's own root Inode record, and the
    // graft's Dentry projection (which embeds the FOREIGN child + attr, and which
    // — by the graft invariant — is the ONLY record carrying that foreign id;
    // there is no Inode record for it).
    let root_attr = shard0
        .get_attr(InodeId::root())
        .unwrap()
        .expect("shard 0 root inode record exists");
    let graft_projection = DentryProjection {
        dentry: graft.dentry.clone(),
        attr: graft.attr.clone(),
        body: None,
    };

    let build_store = || {
        let mut store = FixedRecoveryStore::new();
        store.push(
            RecordFamily::Inode,
            inode_key(MountId::new(1).unwrap(), InodeId::root()),
            encode_inode_attr(&root_attr),
            root_attr.generation,
        );
        store.push(
            RecordFamily::Dentry,
            dentry_key(
                MountId::new(1).unwrap(),
                InodeId::root(),
                &graft.dentry.name,
            ),
            encode_dentry_projection(&graft_projection),
            graft.attr.generation,
        );
        store
    };

    // Shard-aware fallback recovery AS shard 0: the foreign graft child (shard
    // index 1) is excluded from the high-water, so next_inode stays in shard 0's
    // subspace and does NOT jump to foreign_inode + 1. Shard 0 minted no local
    // inodes here, so the high-water stays at the root => next_inode = ROOT + 1.
    let recovered = recover_allocator_state(&build_store(), MountId::new(1).unwrap(), 0).unwrap();
    assert!(
        recovered.next_inode <= foreign_inode.get(),
        "shard 0 allocator was poisoned by the foreign graft child: \
         next_inode={} foreign_inode={}",
        recovered.next_inode,
        foreign_inode.get()
    );
    assert_eq!(recovered.next_inode, InodeId::ROOT_RAW + 1);

    // Control case proving the guard is shard-scoped, not a blanket skip: with the
    // SAME records, recovering AS shard 1 DOES fold the (now-owned) child and
    // lands at foreign_inode + 1.
    let as_shard1 = recover_allocator_state(&build_store(), MountId::new(1).unwrap(), 1).unwrap();
    assert_eq!(as_shard1.next_inode, foreign_inode.get() + 1);
}

/// Delete the durable allocator `System` record so the next recovery is forced
/// down the scan-and-fold FALLBACK path on a real, populated store.
fn drop_allocator_record(service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>) {
    let commit_version = service.next_version().unwrap();
    let key = allocator_key(service.mount_id());
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-drop-allocator",
                service.mount_id(),
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::ReserveAllocator,
            read_version: predecessor(commit_version).unwrap(),
            commit_version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: Vec::new(),
            mutations: vec![delete_mutation(RecordFamily::System, key)],
            watch: Vec::new(),
        })
        .unwrap();
    assert!(
        service
            .metadata_store()
            .get(
                RecordFamily::System,
                &allocator_key(service.mount_id()),
                Version::new(u64::MAX).unwrap(),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .is_none(),
        "allocator System record must be gone so recovery takes the fallback path"
    );
}

#[test]
fn fallback_recovery_survives_command_dedupe_rows_on_real_store() {
    // Regression: the fallback scan used to fold `RecordFamily::CommandDedupe`,
    // whose values are header-less dedupe-result payloads the scan codec cannot
    // decode ("unknown kind"). On any store that had taken real commits — which
    // populate the dedupe tree — the fallback rebuild therefore PANICKED. This
    // exercises the fixed path against a genuine `HoltMetadataStore`.
    let (service, store) = shard_service(0);

    // Several commits, each of which writes a `CommandDedupe` row keyed by its
    // request id. Mix dirs and files so multiple families carry the high-water.
    let dir = service
        .create_dir(
            InodeId::root(),
            DentryName::new(b"dir".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    let mut last_file_inode = dir.attr.inode;
    for n in 0..5 {
        let entry = service
            .create_file(
                dir.attr.inode,
                DentryName::new(format!("f{n}").into_bytes()).unwrap(),
                0o644,
                1000,
                1000,
            )
            .unwrap();
        last_file_inode = entry.attr.inode;
    }
    // The live allocator floor the durable record would have carried.
    let live_next_inode = service.next_inode().unwrap().get() + 1;
    let live_commit_version = service.read_version().unwrap().get();
    assert!(last_file_inode.get() < live_next_inode);

    // Sanity: the dedupe tree is genuinely populated, so a fallback that still
    // scanned it would hit the undecodable rows. The dedupe family stores
    // header-less result payloads and is INTENTIONALLY not standard-scannable
    // (that is the whole bug), so prove population through its dedicated lookup
    // path instead of a raw `scan`.
    let probe_version = service.next_version().unwrap();
    let probe_request = request_id(
        b"dedupe-probe",
        service.mount_id(),
        InodeId::root(),
        probe_version,
    );
    service
        .commit_metadata(MetadataCommand {
            request_id: probe_request.clone(),
            kind: CommandKind::UpdateAttr,
            read_version: predecessor(probe_version).unwrap(),
            commit_version: probe_version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(service.mount_id(), InodeId::root()),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(service.mount_id(), InodeId::root()),
                predicate: Predicate::Exists,
            }],
            mutations: Vec::new(),
            watch: Vec::new(),
        })
        .unwrap();
    assert!(
        store
            .committed_request_result(&probe_request)
            .unwrap()
            .is_some(),
        "a committed command must leave a CommandDedupe row"
    );

    // Force the fallback: remove the durable allocator record.
    drop_allocator_record(&service);

    // The fix: recovery scans the standard-encoded families and SKIPS
    // CommandDedupe, so this returns instead of panicking on "unknown kind".
    let recovered = recover_allocator_state(&store, service.mount_id(), 0).unwrap();

    // It must not regress below any minted inode / observed commit version.
    assert!(
        recovered.next_inode > last_file_inode.get(),
        "fallback next_inode {} must cover the last minted inode {}",
        recovered.next_inode,
        last_file_inode.get()
    );
    assert!(
        recovered.next_inode <= live_next_inode,
        "fallback next_inode {} must not exceed the durable floor {} (reservation skips ids on crash, never on a clean fold)",
        recovered.next_inode,
        live_next_inode
    );
    assert!(
        recovered.last_commit_version <= live_commit_version,
        "recovered commit version {} must not exceed the live clock {}",
        recovered.last_commit_version,
        live_commit_version
    );
    assert!(
        recovered.last_commit_version >= dir.attr.generation,
        "recovered commit version {} must cover committed generations (e.g. {})",
        recovered.last_commit_version,
        dir.attr.generation
    );

    // And the recovered floor must let the shard be reopened and keep minting
    // ids above everything it already handed out — the end-to-end contract.
    let reopened =
        NoKvFs::open_existing(service.mount_id(), store, MemoryObjectStore::new(), 0).unwrap();
    let minted = reopened
        .create_file(
            dir.attr.inode,
            DentryName::new(b"after-recovery".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert!(
        minted.attr.inode.get() > last_file_inode.get(),
        "a reopened shard must mint ids above the pre-crash high-water"
    );
    assert_eq!(minted.attr.inode.shard_index(), 0);
}

#[test]
fn fallback_allocator_recovery_folds_fork_binding_commit_version() {
    let (service, store) = shard_service(0);
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let fork = service.clone_subtree_path("/base").unwrap();
    let binding = service
        .versioned_fork_bindings_at(service.read_version().unwrap(), ReadPurpose::UserStrong)
        .unwrap()
        .into_iter()
        .find(|binding| binding.binding.fork_root == fork.root)
        .expect("detached clone publishes a durable fork binding");
    assert_eq!(binding.binding.created_version, binding.version.get());

    // The binding commit only leaves a ForkBinding row (the dedupe value has a
    // special encoding and is intentionally excluded from fallback scans).
    // Removing the allocator record simulates recovery when that durable fast
    // path is unavailable.
    drop_allocator_record(&service);
    let recovered = recover_allocator_state(&store, service.mount_id(), 0).unwrap();
    assert!(
        recovered.last_commit_version >= binding.binding.created_version,
        "fallback clock {} must cover binding commit {}",
        recovered.last_commit_version,
        binding.binding.created_version
    );

    let reopened =
        NoKvFs::open_existing(service.mount_id(), store, MemoryObjectStore::new(), 0).unwrap();
    assert!(
        reopened.next_version().unwrap().get() > binding.binding.created_version,
        "reopened allocator must not reuse a visible ForkBinding version"
    );
}

/// Install `/dataset` as a cross-shard graft on shard 0 pointing at a real
/// subtree dir owned by shard 1, returning both shards and the graft name. The
/// child subtree dir already holds a file so any blind emptiness check on the
/// parent would be wrong.
fn grafted_pair() -> (
    NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    DentryName,
    InodeId,
) {
    let (shard0, _store0) = shard_service(0);
    let (shard1, _store1) = shard_service(1);
    let dataset = DentryName::new(b"dataset".to_vec()).unwrap();

    let subtree = shard1
        .create_dir(InodeId::root(), dataset.clone(), 0o755, 1000, 1000)
        .unwrap();
    let foreign_inode = subtree.attr.inode;
    assert_eq!(foreign_inode.shard_index(), 1);
    // Populate the child subtree so its contents live on shard 1, invisible to
    // shard 0's dentry subspace.
    shard1
        .create_file(
            foreign_inode,
            DentryName::new(b"inside.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    shard0
        .create_graft(
            InodeId::root(),
            dataset.clone(),
            foreign_inode,
            0o755,
            1000,
            1000,
        )
        .unwrap();
    (shard0, shard1, dataset, foreign_inode)
}

#[test]
fn create_graft_rejects_same_shard_target() {
    // A graft must point at a FOREIGN (child-shard) inode. Pointing it at an
    // inode this shard owns would write a projection-only dentry with no backing
    // Inode record here — a dangling entry. The control-plane mints such inodes
    // with this shard's index, so reject them up front.
    let (shard0, _store0) = shard_service(0);
    let same_shard_dir = shard0
        .create_dir(
            InodeId::root(),
            DentryName::new(b"local".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    assert_eq!(same_shard_dir.attr.inode.shard_index(), 0);

    let err = shard0
        .create_graft(
            InodeId::root(),
            DentryName::new(b"bad-graft".to_vec()).unwrap(),
            same_shard_dir.attr.inode,
            0o755,
            1000,
            1000,
        )
        .unwrap_err();
    assert!(matches!(err, MetadError::InvalidPath(_)), "got {err:?}");
    // No dentry was written for the rejected graft.
    assert!(shard0
        .lookup_plus(
            InodeId::root(),
            &DentryName::new(b"bad-graft".to_vec()).unwrap()
        )
        .unwrap()
        .is_none());
}

#[test]
fn remove_graft_is_idempotent_when_dentry_already_gone() {
    // First teardown removes the graft dentry and returns the removed entry.
    let (shard0, _shard1, dataset, foreign_inode) = grafted_pair();
    let removed = shard0.remove_graft(InodeId::root(), &dataset).unwrap();
    assert_eq!(
        removed.expect("first remove returns the entry").attr.inode,
        foreign_inode
    );
    assert!(shard0
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .is_none());

    // A racing/re-driven second teardown must be a no-op success, not an error:
    // the desired post-state (dentry absent) already holds.
    let second = shard0.remove_graft(InodeId::root(), &dataset).unwrap();
    assert!(
        second.is_none(),
        "idempotent re-run must return Ok(None), got {second:?}"
    );
}

#[test]
fn remove_empty_dir_rejects_graft_point() {
    let (shard0, shard1, dataset, foreign_inode) = grafted_pair();

    // rmdir of the graft must be rejected, NOT silently succeed against the
    // locally-empty (foreign) subtree.
    assert!(matches!(
        shard0.remove_empty_dir(InodeId::root(), &dataset),
        Err(MetadError::GraftPoint)
    ));

    // The graft dentry still resolves on the parent and the child subtree + its
    // contents are untouched on shard 1 (no orphaning).
    assert!(shard0
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .is_some());
    assert!(shard1.get_attr(foreign_inode).unwrap().is_some());
    let inside = shard1
        .lookup_plus(
            foreign_inode,
            &DentryName::new(b"inside.txt".to_vec()).unwrap(),
        )
        .unwrap();
    assert!(inside.is_some(), "child subtree contents must survive");
}

#[test]
fn remove_file_rejects_graft_point() {
    let (shard0, _shard1, dataset, _foreign) = grafted_pair();
    // `unlink` of the graft reports the actionable graft-point error (ahead of
    // the generic is-a-directory error) and does not touch the dentry.
    assert!(matches!(
        shard0.remove_file(InodeId::root(), &dataset),
        Err(MetadError::GraftPoint)
    ));
    assert!(shard0
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .is_some());
}

#[test]
fn rename_rejects_graft_point_source_and_destination() {
    let (shard0, _shard1, dataset, _foreign) = grafted_pair();
    let elsewhere = DentryName::new(b"elsewhere".to_vec()).unwrap();

    // Graft as the rename SOURCE: moving it would detach the projection.
    assert!(matches!(
        shard0.rename(
            InodeId::root(),
            &dataset,
            InodeId::root(),
            elsewhere.clone()
        ),
        Err(MetadError::GraftPoint)
    ));
    // Still in place after the rejected move.
    assert!(shard0
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .is_some());

    // Graft as the rename DESTINATION: create a local file, then try to clobber
    // the graft with it. `rename_replace` reaches the destination-graft guard.
    let victim = DentryName::new(b"victim".to_vec()).unwrap();
    shard0
        .create_file(InodeId::root(), victim.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert!(matches!(
        shard0.rename_replace(InodeId::root(), &victim, InodeId::root(), dataset.clone()),
        Err(MetadError::GraftPoint)
    ));
    assert!(shard0
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .is_some());

    // A graft self-rename (same parent + name) is a harmless no-op and still
    // succeeds — the guard only fires on an actual move.
    let same = shard0
        .rename(InodeId::root(), &dataset, InodeId::root(), dataset.clone())
        .unwrap();
    assert_eq!(same.attr.inode.shard_index(), 1);
}

#[test]
fn normal_empty_dir_removal_still_works_after_graft_guard() {
    // The guard must be inert for a same-shard child. On shard 0 every child is
    // local (`compose(0, x) == x`), so a plain empty-dir removal is unaffected.
    let (shard0, _store0) = shard_service(0);
    let local = DentryName::new(b"local".to_vec()).unwrap();
    let dir = shard0
        .create_dir(InodeId::root(), local.clone(), 0o755, 1000, 1000)
        .unwrap();
    assert_eq!(dir.attr.inode.shard_index(), 0);
    let removed = shard0.remove_empty_dir(InodeId::root(), &local).unwrap();
    assert_eq!(removed.attr.inode, dir.attr.inode);
    assert!(shard0
        .lookup_plus(InodeId::root(), &local)
        .unwrap()
        .is_none());
}

#[test]
fn child_gc_preserves_grafted_subtree_root_and_contents() {
    // A grafted subtree's root dir is created on its OWNING (child) shard by the
    // mkdir half of register_graft, so it has a LIVE local dentry (child root ->
    // "dataset") and a live Inode record. NoKV-FS GC has no logical orphan
    // collector — the reachable passes only reclaim object-block GC-queue
    // entries, expired snapshot pins, prunable history, and unreachable Holt
    // storage frames — none of which can touch a live current-tree record. This
    // locks that the subtree root and its contents survive a full GC sweep on
    // the child shard. (Runs entirely on the child shard; the parent's graft
    // dentry is irrelevant to child-side GC.)
    let (child, store) = shard_service(1);
    let dataset = DentryName::new(b"dataset".to_vec()).unwrap();
    let subtree = child
        .create_dir(InodeId::root(), dataset.clone(), 0o755, 1000, 1000)
        .unwrap();
    let subtree_root = subtree.attr.inode;
    assert_eq!(subtree_root.shard_index(), 1);

    // Populate the subtree: a nested dir and a file with real body content (so
    // the object-block GC path has something to consider).
    let nested = child
        .create_dir(
            subtree_root,
            DentryName::new(b"nested".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    let file_name = DentryName::new(b"keep.txt".to_vec()).unwrap();
    child
        .publish_artifact(PublishArtifact {
            parent: subtree_root,
            name: file_name.clone(),
            producer: "test".to_owned(),
            digest_uri: body_digest_uri(b"hello graft"),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "graft/keep".to_owned(),
            bytes: b"hello graft".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    // Run every reachable GC pass on the child shard.
    child.cleanup_pending_objects(1000).unwrap();
    child.cleanup_history(1000).unwrap();
    // Holt physical-frame GC (folds the WAL into a checkpoint, then reclaims
    // unreachable storage frames). This is the deepest reclaimer in the stack.
    store.checkpoint().unwrap();
    store.reclaim_unreachable_storage().unwrap();
    child.cleanup_pending_objects(1000).unwrap();

    // The subtree root, the nested dir, and the file all survive: they are
    // referenced by live dentries, so no GC pass can reclaim them.
    assert!(
        child.get_attr(subtree_root).unwrap().is_some(),
        "grafted subtree root inode must survive child GC"
    );
    let looked_up_root = child
        .lookup_plus(InodeId::root(), &dataset)
        .unwrap()
        .expect("subtree root dentry must survive");
    assert_eq!(looked_up_root.attr.inode, subtree_root);

    assert!(child.get_attr(nested.attr.inode).unwrap().is_some());
    let kept = child
        .lookup_plus(subtree_root, &file_name)
        .unwrap()
        .expect("file under subtree root must survive");
    // Body still readable end-to-end after GC.
    let bytes = child.read_file(kept.attr.inode, 0, 64).unwrap();
    assert_eq!(bytes, b"hello graft");
}
