//! A minted snapshot's lease expiry must reach the caller. The wire response
//! (`WireSnapshotPin`) already carries `lease_expires_unix_ms`; this asserts the
//! client surfaces it in `SnapshotOutcome` instead of dropping it, so a caller
//! can renew before the pin is reaped.

use std::net::{SocketAddr, TcpListener};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nokv_client::NoKvFsClient;
use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
use nokv_object::{MemoryObjectStore, ObjectStoreConfig, S3ObjectStoreOptions};
use nokv_server::ServerOptions;
use nokv_types::MountId;

type TestClient = NoKvFsClient<MemoryObjectStore>;

/// Default snapshot lease minted by the service when no explicit lease is given
/// (mirrors `nokv_meta` `DEFAULT_SNAPSHOT_LEASE_MS` = 1h).
const DEFAULT_SNAPSHOT_LEASE_MS: u64 = 3_600_000;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
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

#[test]
fn snapshot_subtree_path_surfaces_the_lease_expiry() {
    let address = spawn_test_server();
    let client: TestClient = NoKvFsClient::connect(address, MemoryObjectStore::new());
    client.metadata().mkdir("/runs", 0o755, 1000, 1000).unwrap();

    let before = now_unix_ms();
    let outcome = client.metadata().snapshot_subtree_path("/runs").unwrap();
    let after = now_unix_ms();

    // Non-zero and consistent with a fresh default lease: the outcome must carry
    // the pin's real expiry, not a dropped 0. Window bounds the mint timestamp.
    assert!(
        outcome.lease_expires_unix_ms >= before + DEFAULT_SNAPSHOT_LEASE_MS,
        "lease expiry {} predates the mint window (>= {})",
        outcome.lease_expires_unix_ms,
        before + DEFAULT_SNAPSHOT_LEASE_MS
    );
    assert!(
        outcome.lease_expires_unix_ms <= after + DEFAULT_SNAPSHOT_LEASE_MS,
        "lease expiry {} postdates the mint window (<= {})",
        outcome.lease_expires_unix_ms,
        after + DEFAULT_SNAPSHOT_LEASE_MS
    );
}
