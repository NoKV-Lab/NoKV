//! Regression: a read racing an append must never hide the last append.
//!
//! Appends pre-allocate their commit version at prepare time, so a concurrent
//! path-probing read (stat_path / read plans) can cache the pre-append dentry
//! under the exact version the publish then commits at. Since commits never
//! advance the clock past their pre-allocated version, the server would serve
//! that poisoned entry until the next unrelated write: stat_path reported the
//! old size and read_path returned a short read (or MissingBodyDescriptor once
//! chain compaction reclaimed the superseded generations). The server now
//! purges its version-keyed path caches when a commit applies; this hammers
//! the race and asserts every append is visible as soon as it returns.

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nokv_client::{ArtifactMetadata, NoKvFsClient};
use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
use nokv_object::{MemoryObjectStore, ObjectStoreConfig, S3ObjectStoreOptions};
use nokv_server::ServerOptions;
use nokv_types::MountId;

type TestClient = NoKvFsClient<MemoryObjectStore>;

fn spawn_test_server() -> SocketAddr {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = listener.local_addr().unwrap();
    let server = nokv_server::Server::open(ServerOptions {
        bind,
        mount: MountId::new(1).unwrap(),
        meta_path: dir.path().join("meta"),
        metadata_checkpoint_archive_prefix: None,
        object: fake_object_config(),
        uid: 1000,
        gid: 1000,
        object_gc: ObjectGcOptions {
            interval: Duration::from_secs(3600),
            limit: 128,
            run_immediately: false,
            read_lease_grace: ObjectGcOptions::default().read_lease_grace,
        },
        history_gc: HistoryGcOptions {
            interval: Duration::from_secs(3600),
            limit: 128,
            run_immediately: false,
        },
        control: None,
    })
    .unwrap();
    thread::spawn(move || {
        let _dir = dir;
        let _ = server.serve(listener);
    });
    bind
}

fn fake_object_config() -> ObjectStoreConfig {
    ObjectStoreConfig::s3(S3ObjectStoreOptions {
        bucket: "test".to_owned(),
        root: "/".to_owned(),
        region: "auto".to_owned(),
        endpoint: Some("http://127.0.0.1:1".to_owned()),
        access_key_id: Some("test".to_owned()),
        secret_access_key: Some("test".to_owned()),
        session_token: None,
        virtual_host_style: false,
        skip_signature: true,
    })
}

fn artifact_metadata(manifest_id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: "concurrent-read-append-tests".to_owned(),
        digest_uri: format!("sha256:{manifest_id}"),
        content_type: "text/plain".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

#[test]
fn appends_stay_visible_under_concurrent_stat_path_reads() {
    // 8 appends push the delta chain to the compaction threshold, covering the
    // harder symptom (a poisoned entry pointing at a reclaimed generation).
    const ROUNDS: usize = 12;
    const APPENDS: usize = 8;
    let address = spawn_test_server();
    let objects = MemoryObjectStore::new();
    let writer: TestClient = NoKvFsClient::connect(address, objects.clone());
    writer.metadata().mkdir("/w", 0o755, 1000, 1000).unwrap();

    for round in 0..ROUNDS {
        let path = format!("/w/log-{round}.txt");
        writer
            .put_artifact(
                &path,
                b"seg0|".to_vec(),
                artifact_metadata(&format!("r{round}-0")),
            )
            .unwrap();

        // Reader hammers stat_path (a path-index-probing read) so some read
        // lands inside an append's prepare→publish window.
        let stop = Arc::new(AtomicBool::new(false));
        let reader_stop = Arc::clone(&stop);
        let reader_path = path.clone();
        let reader_objects = objects.clone();
        let reader = thread::spawn(move || {
            let client: TestClient = NoKvFsClient::connect(address, reader_objects);
            while !reader_stop.load(Ordering::Relaxed) {
                let _ = client.metadata().stat_path(&reader_path);
            }
        });

        let mut expected = b"seg0|".to_vec();
        for index in 1..=APPENDS {
            let delta = format!("seg{index}|").into_bytes();
            expected.extend_from_slice(&delta);
            writer
                .append_artifact(
                    &path,
                    delta,
                    artifact_metadata(&format!("r{round}-{index}")),
                    None,
                )
                .unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();

        // Observe from a FRESH client: the poisoned state lived in the
        // server's shared caches, not in any client.
        let observer: TestClient = NoKvFsClient::connect(address, objects.clone());
        let stat_size = observer
            .metadata()
            .stat_path(&path)
            .unwrap()
            .map(|m| m.attr.size)
            .unwrap_or(0);
        let lookup_size = observer
            .metadata()
            .lookup(&path)
            .unwrap()
            .map(|e| e.attr.size)
            .unwrap_or(0);
        let read = observer
            .read_path(&path, 0, expected.len() + 64, None)
            .unwrap_or_else(|err| panic!("round {round}: read_path failed: {err:?}"));

        assert_eq!(
            stat_size,
            expected.len() as u64,
            "round {round}: stat_path hides the last append"
        );
        assert_eq!(
            lookup_size,
            expected.len() as u64,
            "round {round}: lookup hides the last append"
        );
        assert_eq!(
            read.bytes, expected,
            "round {round}: read_path returned stale content"
        );
    }
}
