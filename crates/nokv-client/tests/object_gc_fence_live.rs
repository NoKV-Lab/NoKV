//! Live RustFS acceptance for the durable object-reference/GC fence.
//!
//! This test is ignored by default because it needs a real S3-compatible
//! endpoint. `scripts/run-object-gc-fence-live-e2e.sh` owns that environment.

use std::collections::BTreeSet;
use std::env;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nokv_client::{ArtifactMetadata, ClientError, ClientPreparedArtifact, NoKvFsClient};
use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
use nokv_object::{
    ChunkStore, ChunkWriteOptions, ObjectBytes, ObjectCapabilities, ObjectError, ObjectInfo,
    ObjectKey, ObjectRange, ObjectStore, ObjectStoreConfig, S3ObjectStore, S3ObjectStoreOptions,
    StagedObjectSet, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokv_server::ServerOptions;
use nokv_types::{BodyDescriptor, ChunkManifest, MountId};

type LiveClient = NoKvFsClient<S3ObjectStore>;

#[derive(Clone)]
struct PausingPutStore {
    inner: S3ObjectStore,
    gate: Arc<(Mutex<PausingPutState>, Condvar)>,
}

#[derive(Default)]
struct PausingPutState {
    armed: bool,
    expected_puts: usize,
    reached: bool,
    released: bool,
    staged_keys: BTreeSet<String>,
}

impl PausingPutStore {
    fn new(inner: S3ObjectStore) -> Self {
        Self {
            inner,
            gate: Arc::new((Mutex::new(PausingPutState::default()), Condvar::new())),
        }
    }

    fn arm(&self, expected_puts: usize) {
        assert!(
            expected_puts > 1,
            "the paused write must span multiple blocks"
        );
        let (lock, _) = &*self.gate;
        *lock.lock().expect("lock put pause") = PausingPutState {
            armed: true,
            expected_puts,
            ..PausingPutState::default()
        };
    }

    fn wait_until_staged(&self) -> BTreeSet<String> {
        let (lock, changed) = &*self.gate;
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut state = lock.lock().expect("lock put pause");
        while !state.reached {
            let now = Instant::now();
            assert!(now < deadline, "timed out waiting for paused RustFS PUT");
            let (next, timed_out) = changed
                .wait_timeout(state, deadline - now)
                .expect("wait for paused RustFS PUT");
            state = next;
            assert!(
                !timed_out.timed_out() || state.reached,
                "timed out waiting for paused RustFS PUT"
            );
        }
        assert_eq!(state.staged_keys.len(), state.expected_puts);
        state.staged_keys.clone()
    }

    fn release(&self) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().expect("lock put pause");
        state.released = true;
        changed.notify_all();
    }

    fn pause_after_put(&self, key: &ObjectKey) {
        let (lock, changed) = &*self.gate;
        let mut state = lock.lock().expect("lock put pause");
        if !state.armed {
            return;
        }
        state.staged_keys.insert(key.as_str().to_owned());
        if state.staged_keys.len() < state.expected_puts {
            return;
        }
        state.armed = false;
        state.reached = true;
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).expect("wait for PUT release");
        }
    }
}

impl ObjectStore for PausingPutStore {
    fn capabilities(&self) -> ObjectCapabilities {
        self.inner.capabilities()
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let result = self.inner.put(key, bytes)?;
        self.pause_after_put(key);
        Ok(result)
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

fn live_options(test_namespace: &str) -> S3ObjectStoreOptions {
    let run_prefix = env::var("NOKV_OBJECT_GC_LIVE_RUN_PREFIX")
        .expect("NOKV_OBJECT_GC_LIVE_RUN_PREFIX must isolate this live run");
    let run_prefix = run_prefix.trim_matches('/');
    assert!(!run_prefix.is_empty(), "live run prefix must not be empty");
    let mut options = S3ObjectStoreOptions::rustfs(
        env::var("NOKV_OBJECT_GC_LIVE_BUCKET")
            .expect("NOKV_OBJECT_GC_LIVE_BUCKET must name the live test bucket"),
        env::var("NOKV_OBJECT_GC_LIVE_ENDPOINT")
            .expect("NOKV_OBJECT_GC_LIVE_ENDPOINT must name the RustFS endpoint"),
        env::var("NOKV_OBJECT_GC_LIVE_ACCESS_KEY")
            .expect("NOKV_OBJECT_GC_LIVE_ACCESS_KEY must be set"),
        env::var("NOKV_OBJECT_GC_LIVE_SECRET_KEY")
            .expect("NOKV_OBJECT_GC_LIVE_SECRET_KEY must be set"),
    );
    options.root = format!("/{run_prefix}/{test_namespace}");
    options
}

fn spawn_live_server(options: S3ObjectStoreOptions, mount: MountId) -> SocketAddr {
    let metadata_dir = tempfile::tempdir().expect("create metadata directory");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind metadata server");
    let bind = listener.local_addr().expect("read metadata server address");
    let server = nokv_server::Server::open(ServerOptions {
        bind,
        mount,
        meta_path: metadata_dir.path().join("meta"),
        metadata_checkpoint_archive_prefix: None,
        object: ObjectStoreConfig::s3(options),
        uid: 1000,
        gid: 1000,
        object_gc: ObjectGcOptions {
            interval: Duration::from_millis(10),
            limit: 128,
            run_immediately: true,
            read_lease_grace: Duration::ZERO,
        },
        history_gc: HistoryGcOptions {
            interval: Duration::from_secs(3600),
            limit: 128,
            run_immediately: false,
        },
        control: None,
    })
    .expect("open metadata server");
    thread::spawn(move || {
        let _metadata_dir = metadata_dir;
        server.serve(listener).expect("serve metadata requests");
    });
    bind
}

fn artifact_metadata(manifest_id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: "object-gc-fence-live-e2e".to_owned(),
        digest_uri: format!("sha256:{manifest_id}"),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

fn stage(
    objects: &S3ObjectStore,
    prepared: &ClientPreparedArtifact,
    bytes: &[u8],
    manifest_id: &str,
) -> (BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet) {
    let written = objects
        .write_bytes(
            bytes,
            ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        )
        .expect("stage artifact body in RustFS");
    let staged = written.staged_objects().expect("collect staged objects");
    let chunks = written.chunk_manifests();
    let body = BodyDescriptor {
        producer: "object-gc-fence-live-e2e".to_owned(),
        digest_uri: format!("sha256:{manifest_id}"),
        size: written.size,
        content_type: "application/octet-stream".to_owned(),
        manifest_id: written.manifest_id,
        generation: prepared.generation,
        base_generation: 0,
        chunk_size: written.chunk_size,
        block_size: written.block_size,
    };
    (body, chunks, staged)
}

fn staged_keys(staged: &StagedObjectSet) -> BTreeSet<String> {
    staged
        .objects()
        .iter()
        .map(|object| object.key.as_str().to_owned())
        .collect()
}

fn multi_block_payload(seed: u8) -> Vec<u8> {
    (0..DEFAULT_BLOCK_SIZE + 257)
        .map(|index| seed.wrapping_add((index as u8).wrapping_mul(31)))
        .collect()
}

fn expected_block_count(payload_len: usize) -> usize {
    payload_len.div_ceil(DEFAULT_BLOCK_SIZE)
}

fn path_object_keys(client: &LiveClient, path: &str) -> BTreeSet<String> {
    let entry = client
        .metadata()
        .lookup(path)
        .expect("lookup object-backed path")
        .expect("object-backed path exists");
    let body = entry.body.expect("path has a body descriptor");
    client
        .metadata()
        .read_body_plan(
            entry.attr.inode,
            body.generation,
            0,
            usize::try_from(entry.attr.size).expect("live test artifact fits usize"),
        )
        .expect("read path object plan")
        .blocks
        .into_iter()
        .map(|block| block.object_key)
        .collect()
}

fn wait_until_objects_are_deleted(objects: &S3ObjectStore, keys: &BTreeSet<String>) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = keys
            .iter()
            .filter(|key| {
                objects
                    .head(&ObjectKey::new((*key).clone()).expect("valid object key"))
                    .expect("query RustFS object")
                    .is_some()
            })
            .count();
        if remaining == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for object GC to delete {remaining} RustFS objects"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn assert_objects_survive_gc_window(
    objects: &S3ObjectStore,
    keys: &BTreeSet<String>,
    window: Duration,
) {
    assert!(!keys.is_empty(), "retention check requires object keys");
    let deadline = Instant::now() + window;
    loop {
        for key in keys {
            assert!(
                objects
                    .head(&ObjectKey::new(key.clone()).expect("valid object key"))
                    .expect("query retained RustFS object")
                    .is_some(),
                "object GC deleted retained RustFS object {key}"
            );
        }
        if Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
#[ignore = "requires a live RustFS endpoint; run scripts/run-object-gc-fence-live-e2e.sh"]
fn rustfs_stale_prepare_is_rejected_then_refreshed_and_restaged() {
    let options = live_options("stale-restage");
    let objects = S3ObjectStore::new(options.clone()).expect("open RustFS object store");
    let address = spawn_live_server(options, MountId::new(1).expect("non-zero mount"));
    let client = LiveClient::connect(address, objects.clone());

    client
        .put_artifact(
            "/gc-source.bin",
            b"old generation queued for collection".to_vec(),
            artifact_metadata("gc-source-old"),
        )
        .expect("publish source generation");
    let source = client
        .metadata()
        .lookup("/gc-source.bin")
        .expect("lookup source")
        .expect("source exists");
    let source_body = source.body.expect("source body descriptor");
    let source_plan = client
        .metadata()
        .read_body_plan(
            source.attr.inode,
            source_body.generation,
            0,
            source.attr.size as usize,
        )
        .expect("read source object plan");
    let old_source_keys = source_plan
        .blocks
        .into_iter()
        .map(|block| block.object_key)
        .collect::<BTreeSet<_>>();
    assert!(!old_source_keys.is_empty(), "source must stage an object");

    let stale = client
        .metadata()
        .prepare_artifact_path("/gc-target.bin", false)
        .expect("prepare target before the delete epoch");
    client
        .put_artifact_replace(
            "/gc-source.bin",
            b"replacement generation remains live".to_vec(),
            artifact_metadata("gc-source-new"),
        )
        .expect("replace source and enqueue its old objects");
    wait_until_objects_are_deleted(&objects, &old_source_keys);

    let payload = multi_block_payload(0x31);
    let (stale_body, stale_chunks, stale_staged) =
        stage(&objects, &stale, &payload, "gc-target-stale");
    let stale_keys = staged_keys(&stale_staged);
    assert_eq!(stale_keys.len(), expected_block_count(payload.len()));
    assert!(
        stale_keys.len() > 1,
        "manual stale stage must be multi-block"
    );
    let err = client
        .metadata()
        .publish_prepared_artifact(stale.clone(), stale_body, stale_chunks, 0o644, 1000, 1000)
        .expect_err("publish with a pre-GC epoch must fail closed");
    assert!(
        matches!(
            err,
            ClientError::Metadata(nokv_meta::MetadError::StalePreparedArtifactObjectGcEpoch { .. })
        ),
        "expected typed stale object-GC epoch, got {err:?}"
    );
    assert!(
        client
            .metadata()
            .lookup("/gc-target.bin")
            .expect("lookup target after rejected publish")
            .is_none(),
        "stale publish must not attach namespace metadata"
    );
    objects
        .delete_staged(&stale_staged)
        .expect("delete stale staged generation");
    wait_until_objects_are_deleted(&objects, &stale_keys);

    let refreshed = client
        .metadata()
        .refresh_prepared_artifact_object_gc_epoch(stale.clone())
        .expect("refresh stale prepared identity");
    assert_ne!(refreshed.generation, stale.generation);
    assert_ne!(
        refreshed.object_gc_claim_version,
        stale.object_gc_claim_version
    );
    let (body, chunks, staged) = stage(&objects, &refreshed, &payload, "gc-target-refreshed");
    let refreshed_keys = staged_keys(&staged);
    assert_eq!(refreshed_keys.len(), expected_block_count(payload.len()));
    assert!(
        refreshed_keys.is_disjoint(&stale_keys),
        "refreshed generation must not reuse stale object keys"
    );
    client
        .metadata()
        .publish_prepared_artifact(refreshed, body, chunks, 0o644, 1000, 1000)
        .expect("publish fully restaged generation");
    assert_eq!(
        path_object_keys(&client, "/gc-target.bin"),
        refreshed_keys,
        "published target must reference the complete refreshed object set"
    );
    assert_eq!(
        client
            .cat("/gc-target.bin")
            .expect("read target from RustFS"),
        payload
    );

    // Exercise the public file-client retry path against the same live
    // endpoint. Pause its first object PUT after prepare/stage, advance GC from
    // another client, then prove the writer cleans that stale generation and
    // transparently performs a full restage under a fresh generation.
    client
        .put_artifact(
            "/auto-gc-source.bin",
            b"old source for automatic restage".to_vec(),
            artifact_metadata("auto-gc-source-old"),
        )
        .expect("publish automatic-restage source generation");
    let auto_source = client
        .metadata()
        .lookup("/auto-gc-source.bin")
        .expect("lookup automatic-restage source")
        .expect("automatic-restage source exists");
    let auto_source_body = auto_source.body.expect("automatic-restage source body");
    let auto_source_keys = client
        .metadata()
        .read_body_plan(
            auto_source.attr.inode,
            auto_source_body.generation,
            0,
            auto_source.attr.size as usize,
        )
        .expect("read automatic-restage source object plan")
        .blocks
        .into_iter()
        .map(|block| block.object_key)
        .collect::<BTreeSet<_>>();

    let pausing_objects = PausingPutStore::new(objects.clone());
    let auto_payload = multi_block_payload(0xa7);
    let auto_block_count = expected_block_count(auto_payload.len());
    pausing_objects.arm(auto_block_count);
    let writer_objects = pausing_objects.clone();
    let expected_auto_payload = auto_payload.clone();
    let writer = thread::spawn(move || {
        let writer = NoKvFsClient::connect(address, writer_objects);
        writer.put_artifact(
            "/auto-gc-target.bin",
            auto_payload,
            artifact_metadata("auto-gc-target"),
        )
    });
    let stale_auto_keys = pausing_objects.wait_until_staged();
    assert_eq!(stale_auto_keys.len(), auto_block_count);

    client
        .put_artifact_replace(
            "/auto-gc-source.bin",
            b"new source survives automatic restage".to_vec(),
            artifact_metadata("auto-gc-source-new"),
        )
        .expect("advance object GC while the file client is paused after staging");
    wait_until_objects_are_deleted(&objects, &auto_source_keys);
    pausing_objects.release();

    writer
        .join()
        .expect("automatic-restage writer thread")
        .expect("file client refreshes and fully restages after stale epoch");
    wait_until_objects_are_deleted(&objects, &stale_auto_keys);
    let fresh_auto_keys = path_object_keys(&client, "/auto-gc-target.bin");
    assert_eq!(fresh_auto_keys.len(), auto_block_count);
    assert!(
        fresh_auto_keys.is_disjoint(&stale_auto_keys),
        "automatic restage must mint a fully disjoint object-key set"
    );
    assert_eq!(
        client
            .cat("/auto-gc-target.bin")
            .expect("read automatically restaged target from RustFS"),
        expected_auto_payload
    );
}

#[test]
#[ignore = "requires a live RustFS endpoint; run scripts/run-object-gc-fence-live-e2e.sh"]
fn rustfs_fork_binding_retains_then_releases_source_objects() {
    let options = live_options("fork-retention");
    let objects = S3ObjectStore::new(options.clone()).expect("open RustFS object store");
    let address = spawn_live_server(options, MountId::new(2).expect("non-zero mount"));
    let client = LiveClient::connect(address, objects.clone());

    client
        .metadata()
        .mkdir("/base", 0o755, 1000, 1000)
        .expect("create clone source root");
    let source_payload = multi_block_payload(0x52);
    client
        .put_artifact(
            "/base/data.bin",
            source_payload.clone(),
            artifact_metadata("fork-retention-source"),
        )
        .expect("publish clone source");
    let source_keys = path_object_keys(&client, "/base/data.bin");
    assert_eq!(
        source_keys.len(),
        expected_block_count(source_payload.len())
    );

    let fork = client
        .metadata()
        .clone_subtree_path("/base", "/fork")
        .expect("create object-sharing fork");
    let borrower = client
        .metadata()
        .lookup("/fork/data.bin")
        .expect("lookup fork borrower")
        .expect("fork borrower exists")
        .attr
        .inode;
    assert_eq!(
        path_object_keys(&client, "/fork/data.bin"),
        source_keys,
        "fresh fork must borrow the source object set"
    );

    client
        .metadata()
        .remove("/base/data.bin")
        .expect("remove source namespace reference");
    // The server has a 10 ms object-GC interval and zero read-lease grace.
    // Observe many background passes before opening the fork for the first time.
    assert_objects_survive_gc_window(&objects, &source_keys, Duration::from_millis(500));
    assert_eq!(
        client
            .cat("/fork/data.bin")
            .expect("freshly read retained fork from RustFS"),
        source_payload
    );

    let error = client
        .metadata()
        .retire_snapshot("/base", fork.snapshot_id)
        .expect_err("borrowed blocks must prevent fork-retention retirement");
    assert!(
        matches!(
            error,
            ClientError::Metadata(nokv_meta::MetadError::ForkRetentionActive {
                snapshot_id,
                fork_root,
                borrower: active_borrower,
            }) if snapshot_id == fork.snapshot_id
                && fork_root == fork.root
                && active_borrower == borrower
        ),
        "expected typed ForkRetentionActive, got {error:?}"
    );
    assert_objects_survive_gc_window(&objects, &source_keys, Duration::from_millis(250));

    let replacement_payload = multi_block_payload(0xd3);
    client
        .put_artifact_replace(
            "/fork/data.bin",
            replacement_payload.clone(),
            artifact_metadata("fork-retention-rewritten"),
        )
        .expect("rewrite borrower onto self-owned objects");
    let replacement_keys = path_object_keys(&client, "/fork/data.bin");
    assert!(
        replacement_keys.is_disjoint(&source_keys),
        "rewritten borrower must no longer reference source objects"
    );
    assert!(
        client
            .metadata()
            .retire_snapshot("/base", fork.snapshot_id)
            .expect("retire releasable fork binding"),
        "fork binding must retire exactly once"
    );

    wait_until_objects_are_deleted(&objects, &source_keys);
    assert_eq!(
        client
            .cat("/fork/data.bin")
            .expect("read rewritten fork after source GC"),
        replacement_payload
    );
    assert_objects_survive_gc_window(&objects, &replacement_keys, Duration::from_millis(100));
}
