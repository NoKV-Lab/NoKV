//! Background garbage collection for NoKV metadata.
//!
//! This module owns only background loops around service-level cleanup APIs.
//! Durable queue formats, object deletes, history pruning rules, and metadata
//! commits stay in the service layer.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::command::{HistoryPruneOutcome, MetadataStore};
use crate::service::{
    NoKvFs, PendingObjectCleanupOutcome, SnapshotReapOutcome, DEFAULT_READ_LEASE_MS,
};
use nokv_object::ObjectStore;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectGcOptions {
    pub interval: Duration,
    pub limit: usize,
    pub run_immediately: bool,
    pub read_lease_grace: Duration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectGcWorkerState {
    pub iterations: u64,
    pub last_outcome: Option<PendingObjectCleanupOutcome>,
    /// Process-lifetime totals retained across empty GC iterations so a renew/
    /// reaper race remains observable after the iteration that detected it.
    pub snapshot_reap: SnapshotReapOutcome,
    pub last_error: Option<String>,
}

pub struct ObjectGcWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    state: Arc<Mutex<ObjectGcWorkerState>>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryGcOptions {
    pub interval: Duration,
    pub limit: usize,
    pub run_immediately: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistoryGcWorkerState {
    pub iterations: u64,
    pub last_outcome: Option<HistoryPruneOutcome>,
    pub last_error: Option<String>,
}

pub struct HistoryGcWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    state: Arc<Mutex<HistoryGcWorkerState>>,
    handle: Option<JoinHandle<()>>,
}

impl Default for ObjectGcOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            limit: DEFAULT_LIMIT,
            run_immediately: true,
            read_lease_grace: Duration::from_millis(DEFAULT_READ_LEASE_MS),
        }
    }
}

impl Default for HistoryGcOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            limit: DEFAULT_LIMIT,
            run_immediately: true,
        }
    }
}

impl ObjectGcWorker {
    pub fn spawn<M, O>(service: Arc<NoKvFs<M, O>>, options: ObjectGcOptions) -> Self
    where
        M: MetadataStore + Send + Sync + 'static,
        O: ObjectStore + Send + Sync + 'static,
    {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = Arc::new(Mutex::new(ObjectGcWorkerState::default()));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let handle = thread::spawn(move || {
            if options.run_immediately {
                run_once(
                    &service,
                    options.limit,
                    options.read_lease_grace,
                    &worker_state,
                );
            }
            loop {
                let (lock, cvar) = &*worker_stop;
                let stopped = match lock.lock() {
                    Ok(stopped) => stopped,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                let (stopped, _) = match cvar.wait_timeout(stopped, options.interval) {
                    Ok(waited) => waited,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                drop(stopped);
                run_once(
                    &service,
                    options.limit,
                    options.read_lease_grace,
                    &worker_state,
                );
            }
        });
        Self {
            stop,
            state,
            handle: Some(handle),
        }
    }

    pub fn state(&self) -> ObjectGcWorkerState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|err| err.into_inner().clone())
    }

    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ObjectGcWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

impl HistoryGcWorker {
    pub fn spawn<M, O>(service: Arc<NoKvFs<M, O>>, options: HistoryGcOptions) -> Self
    where
        M: MetadataStore + Send + Sync + 'static,
        O: ObjectStore + Send + Sync + 'static,
    {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = Arc::new(Mutex::new(HistoryGcWorkerState::default()));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let handle = thread::spawn(move || {
            if options.run_immediately {
                run_history_once(&service, options.limit, &worker_state);
            }
            loop {
                let (lock, cvar) = &*worker_stop;
                let stopped = match lock.lock() {
                    Ok(stopped) => stopped,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                let (stopped, _) = match cvar.wait_timeout(stopped, options.interval) {
                    Ok(waited) => waited,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                drop(stopped);
                run_history_once(&service, options.limit, &worker_state);
            }
        });
        Self {
            stop,
            state,
            handle: Some(handle),
        }
    }

    pub fn state(&self) -> HistoryGcWorkerState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|err| err.into_inner().clone())
    }

    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for HistoryGcWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_once<M, O>(
    service: &NoKvFs<M, O>,
    limit: usize,
    read_lease_grace: Duration,
    state: &Mutex<ObjectGcWorkerState>,
) where
    M: MetadataStore,
    O: ObjectStore,
{
    let result = service.cleanup_pending_objects_with_grace(limit, read_lease_grace);
    if let Ok(mut state) = state.lock() {
        state.iterations += 1;
        match result {
            Ok(outcome) => {
                accumulate_snapshot_reap(&mut state.snapshot_reap, outcome.snapshot_reap);
                state.last_outcome = Some(outcome);
                state.last_error = None;
            }
            Err(err) => {
                state.last_error = Some(err.to_string());
            }
        }
    }
}

fn accumulate_snapshot_reap(total: &mut SnapshotReapOutcome, delta: SnapshotReapOutcome) {
    total.scanned = total.scanned.saturating_add(delta.scanned);
    total.expired_candidates = total
        .expired_candidates
        .saturating_add(delta.expired_candidates);
    total.reaped = total.reaped.saturating_add(delta.reaped);
    total.conflicted = total.conflicted.saturating_add(delta.conflicted);
}

fn run_history_once<M, O>(service: &NoKvFs<M, O>, limit: usize, state: &Mutex<HistoryGcWorkerState>)
where
    M: MetadataStore,
    O: ObjectStore,
{
    let result = service.cleanup_history(limit);
    if let Ok(mut state) = state.lock() {
        state.iterations += 1;
        match result {
            Ok(outcome) => {
                state.last_outcome = Some(outcome);
                state.last_error = None;
            }
            Err(err) => {
                state.last_error = Some(err.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holtstore::HoltMetadataStore;
    use crate::service::PublishArtifact;

    #[test]
    fn snapshot_reap_worker_totals_survive_later_empty_iterations() {
        let mut total = SnapshotReapOutcome::default();
        accumulate_snapshot_reap(
            &mut total,
            SnapshotReapOutcome {
                scanned: 3,
                expired_candidates: 2,
                reaped: 1,
                conflicted: 1,
            },
        );
        accumulate_snapshot_reap(
            &mut total,
            SnapshotReapOutcome {
                scanned: 4,
                ..SnapshotReapOutcome::default()
            },
        );

        assert_eq!(
            total,
            SnapshotReapOutcome {
                scanned: 7,
                expired_candidates: 2,
                reaped: 1,
                conflicted: 1,
            }
        );
    }
    use nokv_object::{MemoryObjectStore, ObjectKey};
    use nokv_types::{DentryName, InodeId, MountId};
    use std::time::Instant;

    fn service_with_objects() -> (
        Arc<NoKvFs<HoltMetadataStore, MemoryObjectStore>>,
        MemoryObjectStore,
    ) {
        let objects = MemoryObjectStore::new();
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects.clone(),
        );
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        (Arc::new(service), objects)
    }

    fn artifact_request(name: DentryName, manifest_id: &str, bytes: &[u8]) -> PublishArtifact {
        PublishArtifact {
            parent: InodeId::root(),
            name,
            producer: "gc-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: manifest_id.to_owned(),
            bytes: bytes.to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        }
    }

    fn block_key(inode: InodeId, generation: u64) -> ObjectKey {
        ObjectKey::new(format!("blocks/1/{}/{}/0/0", inode.get(), generation)).unwrap()
    }

    #[test]
    fn object_worker_stop_before_first_wait_does_not_sleep_interval() {
        let (service, _) = service_with_objects();
        let mut worker = ObjectGcWorker::spawn(
            service,
            ObjectGcOptions {
                interval: Duration::from_secs(3600),
                limit: 100,
                run_immediately: false,
                read_lease_grace: Duration::ZERO,
            },
        );

        let started = Instant::now();
        worker.stop();

        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn history_worker_stop_before_first_wait_does_not_sleep_interval() {
        let (service, _) = service_with_objects();
        let mut worker = HistoryGcWorker::spawn(
            service,
            HistoryGcOptions {
                interval: Duration::from_secs(3600),
                limit: 100,
                run_immediately: false,
            },
        );

        let started = Instant::now();
        worker.stop();

        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn worker_deletes_queued_objects_and_removes_gc_records() {
        let (service, objects) = service_with_objects();
        let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        let published = service
            .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
            .unwrap();
        let body = published.body.clone().unwrap();
        let object = block_key(published.attr.inode, body.generation);
        assert!(objects.head(&object).unwrap().is_some());
        service.remove_file(InodeId::root(), &name).unwrap();

        let mut worker = ObjectGcWorker::spawn(
            Arc::clone(&service),
            ObjectGcOptions {
                interval: Duration::from_millis(10),
                limit: 100,
                run_immediately: true,
                read_lease_grace: Duration::ZERO,
            },
        );
        wait_for(|| objects.head(&object).unwrap().is_none());
        worker.stop();

        let state = worker.state();
        assert!(state.iterations > 0);
        assert_eq!(state.last_error, None);
        assert_eq!(
            service.cleanup_pending_objects(100).unwrap(),
            PendingObjectCleanupOutcome::default()
        );
    }

    #[test]
    fn worker_respects_snapshot_pins_until_retired() {
        let (service, objects) = service_with_objects();
        let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
        let first = service
            .publish_artifact(artifact_request(name.clone(), "checkpoint-old", b"old"))
            .unwrap();
        let old_body = first.body.clone().unwrap();
        let old_object = block_key(first.attr.inode, old_body.generation);
        let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
        service
            .replace_artifact(artifact_request(name, "checkpoint-new", b"new-body"))
            .unwrap();

        let mut worker = ObjectGcWorker::spawn(
            Arc::clone(&service),
            ObjectGcOptions {
                interval: Duration::from_millis(10),
                limit: 100,
                run_immediately: true,
                read_lease_grace: Duration::ZERO,
            },
        );
        wait_for(|| {
            worker
                .state()
                .last_outcome
                .is_some_and(|outcome| outcome.blocked_by_snapshots > 0)
        });
        assert!(objects.head(&old_object).unwrap().is_some());

        assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
        wait_for(|| objects.head(&old_object).unwrap().is_none());
        worker.stop();
        assert_eq!(worker.state().last_error, None);
    }

    #[test]
    fn history_worker_prunes_history_without_snapshot_pins() {
        let (service, _) = service_with_objects();
        let name = DentryName::new(b"history.bin".to_vec()).unwrap();
        service
            .publish_artifact(artifact_request(name.clone(), "history-old", b"old"))
            .unwrap();
        service
            .replace_artifact(artifact_request(name, "history-new", b"new-body"))
            .unwrap();

        let mut worker = HistoryGcWorker::spawn(
            Arc::clone(&service),
            HistoryGcOptions {
                interval: Duration::from_secs(60),
                limit: 100,
                run_immediately: true,
            },
        );
        wait_for(|| worker.state().iterations > 0);
        worker.stop();
        assert_eq!(worker.state().last_error, None);
        assert_eq!(service.cleanup_history(100).unwrap().removed, 0);
    }

    #[test]
    fn history_worker_respects_snapshot_pins_until_retired() {
        let (service, _) = service_with_objects();
        let name = DentryName::new(b"snapshot-history.bin".to_vec()).unwrap();
        service
            .publish_artifact(artifact_request(name.clone(), "history-old", b"old"))
            .unwrap();
        let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
        service
            .replace_artifact(artifact_request(name.clone(), "history-new", b"new-body"))
            .unwrap();

        let mut worker = HistoryGcWorker::spawn(
            Arc::clone(&service),
            HistoryGcOptions {
                interval: Duration::from_secs(60),
                limit: 100,
                run_immediately: true,
            },
        );
        wait_for(|| {
            worker
                .state()
                .last_outcome
                .is_some_and(|outcome| outcome.retained_by_snapshots > 0)
        });
        assert_eq!(
            service
                .read_artifact_path_at_snapshot("/", snapshot.snapshot_id, "/snapshot-history.bin",)
                .unwrap(),
            b"old"
        );

        assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
        worker.stop();
        assert_eq!(worker.state().last_error, None);

        let mut worker = HistoryGcWorker::spawn(
            Arc::clone(&service),
            HistoryGcOptions {
                interval: Duration::from_secs(60),
                limit: 100,
                run_immediately: true,
            },
        );
        wait_for(|| {
            worker
                .state()
                .last_outcome
                .is_some_and(|outcome| outcome.removed > 0)
        });
        worker.stop();
        assert_eq!(worker.state().last_error, None);
    }

    fn wait_for(mut condition: impl FnMut() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("condition did not become true");
    }
}
