//! Append/edit write primitives against a real in-process server.
//!
//! The server owns metadata only: block bytes are staged in the client's
//! `MemoryObjectStore`, so every content assertion reads through the client
//! (`cat`). Server-side data reads (e.g. `cat_snapshot`) cannot see
//! client-staged blocks in this harness.

use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use nokv_client::{is_artifact_write_conflict, ArtifactMetadata, NoKvFsClient};
use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
use nokv_object::{MemoryObjectStore, ObjectStoreConfig, S3ObjectStoreOptions};
use nokv_server::ServerOptions;
use nokv_types::MountId;

type TestClient = NoKvFsClient<MemoryObjectStore>;

fn client() -> TestClient {
    NoKvFsClient::connect(spawn_test_server(), MemoryObjectStore::new())
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
        producer: "file-client-append-tests".to_owned(),
        digest_uri: format!("sha256:{manifest_id}"),
        content_type: "text/plain".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

fn body_generation(client: &TestClient, path: &str) -> u64 {
    client
        .metadata()
        .lookup(path)
        .unwrap()
        .unwrap()
        .body
        .unwrap()
        .generation
}

#[test]
fn append_extends_existing_artifact_as_delta() {
    let client = client();
    client
        .put_artifact(
            "/log.txt",
            b"hello ".to_vec(),
            artifact_metadata("base.log"),
        )
        .unwrap();
    let base_generation = body_generation(&client, "/log.txt");

    let outcome = client
        .append_artifact(
            "/log.txt",
            b"world".to_vec(),
            artifact_metadata("delta-1.log"),
            None,
        )
        .unwrap();

    assert!(!outcome.created);
    assert_eq!(outcome.new_size, 11);
    assert!(outcome.generation > base_generation);
    assert_eq!(client.cat("/log.txt").unwrap(), b"hello world");
    let body = client
        .metadata()
        .lookup("/log.txt")
        .unwrap()
        .unwrap()
        .body
        .unwrap();
    assert_eq!(body.generation, outcome.generation);
    // Delta publish: the new body must fall through to the appended-onto
    // generation for the old bytes instead of re-materializing them.
    assert_eq!(body.base_generation, base_generation);
}

#[test]
fn append_with_matching_expected_generation_succeeds() {
    let client = client();
    client
        .put_artifact("/cas.txt", b"one".to_vec(), artifact_metadata("cas-0.log"))
        .unwrap();
    let base_generation = body_generation(&client, "/cas.txt");

    let outcome = client
        .append_artifact(
            "/cas.txt",
            b"two".to_vec(),
            artifact_metadata("cas-1.log"),
            Some(base_generation),
        )
        .unwrap();

    assert!(!outcome.created);
    assert_eq!(outcome.new_size, 6);
    assert_eq!(client.cat("/cas.txt").unwrap(), b"onetwo");
}

#[test]
fn append_creates_missing_artifact() {
    let client = client();

    let outcome = client
        .append_artifact(
            "/fresh.txt",
            b"seed".to_vec(),
            artifact_metadata("fresh.log"),
            None,
        )
        .unwrap();

    assert!(outcome.created);
    assert_eq!(outcome.new_size, 4);
    assert_eq!(outcome.generation, body_generation(&client, "/fresh.txt"));
    assert_eq!(client.cat("/fresh.txt").unwrap(), b"seed");
}

#[test]
fn append_missing_artifact_with_expected_generation_conflicts() {
    let client = client();

    let err = client
        .append_artifact(
            "/ghost.txt",
            b"x".to_vec(),
            artifact_metadata("ghost.log"),
            Some(7),
        )
        .unwrap_err();

    assert!(
        is_artifact_write_conflict(&err),
        "expected write conflict, got {err:?}"
    );
    assert!(client.metadata().lookup("/ghost.txt").unwrap().is_none());
}

#[test]
fn append_with_stale_expected_generation_conflicts() {
    let client = client();
    client
        .put_artifact(
            "/race.txt",
            b"one".to_vec(),
            artifact_metadata("race-0.log"),
        )
        .unwrap();
    let base_generation = body_generation(&client, "/race.txt");
    client
        .append_artifact(
            "/race.txt",
            b"two".to_vec(),
            artifact_metadata("race-1.log"),
            Some(base_generation),
        )
        .unwrap();

    // A second writer holding the pre-append generation must lose the CAS.
    let err = client
        .append_artifact(
            "/race.txt",
            b"three".to_vec(),
            artifact_metadata("race-2.log"),
            Some(base_generation),
        )
        .unwrap_err();

    assert!(
        is_artifact_write_conflict(&err),
        "expected write conflict, got {err:?}"
    );
    assert_eq!(client.cat("/race.txt").unwrap(), b"onetwo");
}

#[test]
fn repeated_appends_stay_readable_across_chain_compaction() {
    let client = client();
    let mut expected = b"seg0|".to_vec();
    client
        .put_artifact("/chain.txt", expected.clone(), artifact_metadata("chain-0"))
        .unwrap();

    // Nine appends push the fall-through chain past the server's compaction
    // depth (8); content must read back whole at every depth.
    let mut compacted = false;
    for index in 1..=9_usize {
        let delta = format!("seg{index}|").into_bytes();
        expected.extend_from_slice(&delta);
        let outcome = client
            .append_artifact(
                "/chain.txt",
                delta,
                artifact_metadata(&format!("chain-{index}")),
                None,
            )
            .unwrap();
        assert_eq!(outcome.new_size as usize, expected.len());
        assert_eq!(client.cat("/chain.txt").unwrap(), expected);
        let body = client
            .metadata()
            .lookup("/chain.txt")
            .unwrap()
            .unwrap()
            .body
            .unwrap();
        if body.base_generation == 0 {
            compacted = true;
        }
    }
    assert!(
        compacted,
        "nine appends must cross the compaction threshold at least once"
    );
}

/// Two writers with independent clients (sharing one object store so either
/// side can read blocks the other staged) race 10 appends each — including the
/// initial create — against one path. Every conflict-shaped error along the
/// way must be classified by [`is_artifact_write_conflict`] so a
/// retry-on-conflict loop converges: the file must end up with exactly the 20
/// complete lines the writers produced.
#[test]
fn concurrent_appends_from_two_writers_keep_every_line() {
    const WRITERS: usize = 2;
    const APPENDS_PER_WRITER: usize = 10;
    let address = spawn_test_server();
    let objects = MemoryObjectStore::new();
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let objects = objects.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let client: TestClient = NoKvFsClient::connect(address, objects);
                barrier.wait();
                for index in 0..APPENDS_PER_WRITER {
                    let line = format!("writer-{writer}-line-{index}\n").into_bytes();
                    let manifest = format!("concurrent-{writer}-{index}");
                    let mut attempts = 0;
                    loop {
                        match client.append_artifact(
                            "/concurrent.txt",
                            line.clone(),
                            artifact_metadata(&manifest),
                            None,
                        ) {
                            Ok(_) => break,
                            Err(err) if is_artifact_write_conflict(&err) && attempts < 100 => {
                                attempts += 1;
                                thread::sleep(Duration::from_millis(1));
                            }
                            Err(err) => {
                                panic!("writer {writer} append {index} failed hard: {err:?}")
                            }
                        }
                    }
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let client: TestClient = NoKvFsClient::connect(address, objects);
    let content = String::from_utf8(client.cat("/concurrent.txt").unwrap()).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(
        lines.len(),
        WRITERS * APPENDS_PER_WRITER,
        "appends were lost; content:\n{content}"
    );
    for writer in 0..WRITERS {
        for index in 0..APPENDS_PER_WRITER {
            let expected = format!("writer-{writer}-line-{index}");
            assert!(
                lines.contains(&expected.as_str()),
                "missing line {expected}; content:\n{content}"
            );
        }
    }
}

#[test]
fn replace_if_generation_matches_current_body() {
    let client = client();
    client
        .put_artifact("/doc.txt", b"v1".to_vec(), artifact_metadata("doc-0"))
        .unwrap();
    let base_generation = body_generation(&client, "/doc.txt");

    let result = client
        .put_artifact_replace_if_generation(
            "/doc.txt",
            b"v2-longer".to_vec(),
            artifact_metadata("doc-1"),
            base_generation,
        )
        .unwrap();

    assert!(result.replaced.is_some());
    assert_eq!(client.cat("/doc.txt").unwrap(), b"v2-longer");
}

#[test]
fn replace_if_generation_rejects_stale_generation() {
    let client = client();
    client
        .put_artifact("/doc.txt", b"v1".to_vec(), artifact_metadata("doc-0"))
        .unwrap();
    let base_generation = body_generation(&client, "/doc.txt");
    client
        .put_artifact_replace_if_generation(
            "/doc.txt",
            b"v2".to_vec(),
            artifact_metadata("doc-1"),
            base_generation,
        )
        .unwrap();

    let err = client
        .put_artifact_replace_if_generation(
            "/doc.txt",
            b"v3".to_vec(),
            artifact_metadata("doc-2"),
            base_generation,
        )
        .unwrap_err();

    assert!(
        is_artifact_write_conflict(&err),
        "expected write conflict, got {err:?}"
    );
    assert_eq!(client.cat("/doc.txt").unwrap(), b"v2");
}
