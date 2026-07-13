use super::*;
use crate::server::tests::{test_options, test_server};
use crate::{ServerShardOwnerOptions, ServerSharedLogOptions};
use nokv_control::{ControlStore, InMemoryControlStore, ShardId};
use nokv_object::{
    ConfiguredObjectStore, HotFillMode, LocalObjectStoreOptions, MemoryObjectStore,
    ObjectStoreConfig, S3ObjectStoreOptions, TieredObjectStoreOptions, TieredPutPolicy,
};
use nokv_protocol::{
    decode_envelope, encode_request, WireBlockDescriptor, WireBodyDescriptor, WireChunkManifest,
    WireDentryWithAttr, WireMetadataError, WireOpenPathReadPlanRequest, WireSliceManifest,
    WireStagedObjectSet, WireUpdateAttr, WireXattrSetMode,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn request_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
    let body = encode_request(&request).unwrap();
    let response = handle_binary_rpc(server, &body).unwrap();
    decode_envelope(&response).unwrap()
}

fn expect_dentry(envelope: MetadataRpcEnvelope) -> WireDentryWithAttr {
    assert!(envelope.ok, "unexpected error envelope: {envelope:?}");
    match envelope.result.unwrap() {
        MetadataRpcResult::Dentry { entry: Some(entry) } => *entry,
        other => panic!("unexpected dentry result: {other:?}"),
    }
}

fn expect_attr(envelope: MetadataRpcEnvelope) -> nokv_protocol::WireInodeAttr {
    assert!(envelope.ok, "unexpected error envelope: {envelope:?}");
    match envelope.result.unwrap() {
        MetadataRpcResult::InodeAttr { attr: Some(attr) } => attr,
        other => panic!("unexpected attr result: {other:?}"),
    }
}

fn fake_s3_options() -> S3ObjectStoreOptions {
    S3ObjectStoreOptions {
        bucket: "test".to_owned(),
        root: "/".to_owned(),
        region: "auto".to_owned(),
        endpoint: Some("http://127.0.0.1:1".to_owned()),
        access_key_id: Some("test".to_owned()),
        secret_access_key: Some("test".to_owned()),
        session_token: None,
        virtual_host_style: false,
        skip_signature: true,
    }
}

fn shared_tiered_object_config(root: &Path) -> ObjectStoreConfig {
    ObjectStoreConfig::tiered_local_with_options(
        LocalObjectStoreOptions::new(root.join("hot")),
        fake_s3_options(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::HotThenBackgroundCold,
            populate_hot_on_get: true,
            hot_fill_mode: HotFillMode::Inline,
            pending_cold_put_root: Some(root.join("pending-cold")),
        },
    )
}

#[test]
fn rpc_creates_and_lists_directory() {
    let server = test_server();
    let created = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDir {
            parent: 1,
            name: "runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(created.dentry.name_hex, "72756e73");

    let envelope = request_envelope(&server, MetadataRpcRequest::ReadDirPlus { parent: 1 });
    let entries = match envelope.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "72756e73");
}

#[test]
fn rpc_write_publishes_sync_shared_log_before_ack() {
    let dir = tempdir().unwrap();
    let mut options = test_options(dir.path());
    options.object = shared_tiered_object_config(dir.path());
    let control = Arc::new(InMemoryControlStore::new());
    let server = Server::open_with_control(
        options,
        control.clone(),
        vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
            .with_renewal(None)
            .with_shared_log(Some(ServerSharedLogOptions::new("meta/rpc-sync-log")))],
    )
    .unwrap();

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    );

    assert!(envelope.ok, "unexpected RPC error: {envelope:?}");
    let record = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
    let log = record.log.expect("RPC ACK should publish a LogRef");
    assert!(record.durable_lsn > 0);
    assert_eq!(log.durable_lsn, record.durable_lsn);
    let segment = log
        .segments
        .last()
        .expect("RPC ACK LogRef should carry at least one segment");
    // Each shard's shared-log archive is isolated under {prefix}/{sanitized-shard-id}.
    assert!(segment
        .segment_key
        .starts_with("meta/rpc-sync-log/mount_1__/log/"));
    assert_eq!(log.digest.len(), 64);
}

#[test]
fn controlled_failover_restores_checkpoint_and_replays_shared_log() {
    let dir = tempdir().unwrap();
    let object = ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()));
    let control = Arc::new(InMemoryControlStore::new());
    let shared_log = Some(ServerSharedLogOptions::new("meta/failover-log"));

    let mut first_options = test_options(&dir.path().join("first"));
    first_options.metadata_checkpoint_archive_prefix = Some("meta/failover-ck".to_owned());
    let first = Server::open_with_objects(
        first_options,
        object.clone(),
        Some((
            control.clone(),
            vec![ServerShardOwnerOptions::fresh("mount-1:/", "node-a")
                .with_renewal(None)
                .with_shared_log(shared_log.clone())],
        )),
    )
    .unwrap();

    expect_dentry(request_envelope(
        &first,
        MetadataRpcRequest::CreateDirPath {
            path: "/before".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let backup = first.run_manual_backup().unwrap();
    assert!(backup.contains("\"checkpoint_key\""));
    let checkpoint_record = control
        .get_shard(&ShardId::new("mount-1:/"))
        .unwrap()
        .checkpoint
        .expect("manual backup should publish checkpoint ref");

    expect_dentry(request_envelope(
        &first,
        MetadataRpcRequest::CreateDirPath {
            path: "/after".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let after_record = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
    assert!(after_record.durable_lsn > checkpoint_record.lsn);
    assert!(after_record.log.is_some());

    let mut second_options = test_options(&dir.path().join("second"));
    second_options.metadata_checkpoint_archive_prefix = Some("meta/failover-ck".to_owned());
    let second = Server::open_with_objects(
        second_options,
        object,
        Some((
            control.clone(),
            vec![ServerShardOwnerOptions::failover("mount-1:/", "node-b", 1)
                .with_renewal(None)
                .with_shared_log(shared_log)],
        )),
    )
    .unwrap();

    let state = second.shard_owner_state().unwrap().unwrap();
    assert_eq!(state.node_id.as_str(), "node-b");
    assert_eq!(state.epoch, 2);
    assert_eq!(state.state, nokv_control::ShardState::Serving);
    assert_eq!(state.durable_lsn, after_record.durable_lsn);
    assert_eq!(state.checkpoint, after_record.checkpoint);
    assert_eq!(state.log, after_record.log);

    expect_dentry(request_envelope(
        &second,
        MetadataRpcRequest::LookupPath {
            path: "/before".to_owned(),
        },
    ));
    expect_dentry(request_envelope(
        &second,
        MetadataRpcRequest::LookupPath {
            path: "/after".to_owned(),
        },
    ));

    expect_dentry(request_envelope(
        &second,
        MetadataRpcRequest::CreateDirPath {
            path: "/post-failover".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let post_record = control.get_shard(&ShardId::new("mount-1:/")).unwrap();
    assert!(post_record.durable_lsn > after_record.durable_lsn);
    expect_dentry(request_envelope(
        &second,
        MetadataRpcRequest::LookupPath {
            path: "/post-failover".to_owned(),
        },
    ));
    expect_dentry(request_envelope(
        &second,
        MetadataRpcRequest::LookupPath {
            path: "/after".to_owned(),
        },
    ));
}

#[test]
fn rpc_supports_remote_fuse_inode_operations() {
    let server = test_server();
    let file = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFile {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    let updated = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::UpdateAttrs {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            changes: WireUpdateAttr {
                size: Some(128),
                ..WireUpdateAttr::default()
            },
        },
    ));
    assert_eq!(updated.attr.inode, file.attr.inode);
    assert_eq!(updated.attr.size, 128);

    let root = expect_attr(request_envelope(
        &server,
        MetadataRpcRequest::UpdateRootAttrs {
            changes: WireUpdateAttr {
                mode: Some(0o700),
                ..WireUpdateAttr::default()
            },
        },
    ));
    assert_eq!(root.mode, 0o700);

    let set_xattr = request_envelope(
        &server,
        MetadataRpcRequest::SetXattr {
            inode: file.attr.inode,
            name_hex: "757365722e636f6d6d656e74".to_owned(),
            value: b"training checkpoint".to_vec(),
            mode: WireXattrSetMode::Create,
        },
    );
    assert!(set_xattr.ok, "unexpected setxattr error: {set_xattr:?}");
    assert!(matches!(set_xattr.result, Some(MetadataRpcResult::Unit)));

    let get_xattr = request_envelope(
        &server,
        MetadataRpcRequest::GetXattr {
            inode: file.attr.inode,
            name_hex: "757365722e636f6d6d656e74".to_owned(),
        },
    );
    match get_xattr.result.unwrap() {
        MetadataRpcResult::XattrValue { value } => {
            assert_eq!(value, Some(b"training checkpoint".to_vec()));
        }
        other => panic!("unexpected getxattr result: {other:?}"),
    }

    let list_xattr = request_envelope(
        &server,
        MetadataRpcRequest::ListXattr {
            inode: file.attr.inode,
        },
    );
    match list_xattr.result.unwrap() {
        MetadataRpcResult::XattrNames { names_hex } => {
            assert_eq!(names_hex, vec!["757365722e636f6d6d656e74"]);
        }
        other => panic!("unexpected listxattr result: {other:?}"),
    }

    let remove_xattr = request_envelope(
        &server,
        MetadataRpcRequest::RemoveXattr {
            inode: file.attr.inode,
            name_hex: "757365722e636f6d6d656e74".to_owned(),
        },
    );
    assert!(
        matches!(remove_xattr.result, Some(MetadataRpcResult::Unit)),
        "unexpected removexattr result: {remove_xattr:?}"
    );

    let special = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateSpecialNode {
            parent: 1,
            name: "accelerator0".to_owned(),
            file_type: "char_device".to_owned(),
            mode: 0o660,
            rdev: 0x1234,
            uid: 0,
            gid: 44,
        },
    ));
    assert_eq!(special.dentry.child_type, "char_device");
    assert_eq!(special.attr.file_type, "char_device");
    assert_eq!(special.attr.rdev, 0x1234);

    let snapshot = request_envelope(
        &server,
        MetadataRpcRequest::SnapshotSubtreePath {
            root_path: "/".to_owned(),
            lease_ms: nokv_meta::DEFAULT_SNAPSHOT_LEASE_MS,
        },
    );
    let snapshot_id = match snapshot.result.unwrap() {
        MetadataRpcResult::Snapshot { snapshot } => snapshot.snapshot_id,
        other => panic!("unexpected snapshot result: {other:?}"),
    };
    let pin = request_envelope(
        &server,
        MetadataRpcRequest::SnapshotPin {
            root_path: "/".to_owned(),
            snapshot_id,
        },
    );
    match pin.result.unwrap() {
        MetadataRpcResult::SnapshotPin {
            snapshot: Some(snapshot),
            server_now_ms,
        } => {
            assert_eq!(snapshot.snapshot_id, snapshot_id);
            assert!(server_now_ms > 0);
        }
        other => panic!("unexpected snapshot pin result: {other:?}"),
    }
}

#[test]
fn rpc_create_file_prepared_returns_entry_and_write_token() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePrepared {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    let (entry, prepared) = match envelope.result.unwrap() {
        MetadataRpcResult::CreatedPreparedArtifact { entry, prepared } => (*entry, prepared),
        other => panic!("unexpected create prepared result: {other:?}"),
    };
    assert_eq!(entry.dentry.name_hex, "636865636b706f696e742e62696e");
    assert_eq!(entry.attr.file_type, "file");
    assert_eq!(prepared.parent, 1);
    assert_eq!(prepared.name, "checkpoint.bin");
    assert_eq!(prepared.inode, entry.attr.inode);
    assert!(prepared.replace);
    assert_eq!(prepared.old_generation, None);
    assert!(prepared.generation > entry.attr.generation);
}

#[test]
fn rpc_supports_remote_fuse_advisory_locks() {
    let server = test_server();
    let file = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFile {
            parent: 1,
            name: "locked.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    let set = request_envelope(
        &server,
        MetadataRpcRequest::SetAdvisoryLock {
            inode: file.attr.inode,
            owner: 7,
            start: 0,
            end: 99,
            kind: "write".to_owned(),
            pid: 700,
            wait: false,
        },
    );
    assert!(set.ok, "unexpected set lock error: {set:?}");

    let get = request_envelope(
        &server,
        MetadataRpcRequest::GetAdvisoryLock {
            inode: file.attr.inode,
            owner: 8,
            start: 10,
            end: 20,
            kind: "read".to_owned(),
            pid: 800,
        },
    );
    match get.result.unwrap() {
        MetadataRpcResult::AdvisoryLock { lock: Some(lock) } => {
            assert_eq!(lock.owner, 7);
            assert_eq!(lock.kind, "write");
        }
        other => panic!("unexpected lock result: {other:?}"),
    }

    let conflict = request_envelope(
        &server,
        MetadataRpcRequest::SetAdvisoryLock {
            inode: file.attr.inode,
            owner: 8,
            start: 10,
            end: 20,
            kind: "read".to_owned(),
            pid: 800,
            wait: false,
        },
    );
    assert!(!conflict.ok);
    assert!(matches!(
        conflict.error_kind,
        Some(WireMetadataError::LockConflict { .. })
    ));
}

#[test]
fn rpc_batch_read_plans_return_per_item_envelopes() {
    let server = test_server();
    publish_synthetic_file(&server, "/artifact.bin", "artifact.bin", false);
    let entry = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::LookupPath {
            path: "/artifact.bin".to_owned(),
        },
    ));
    let inode = entry.attr.inode;
    let generation = entry.body.as_ref().unwrap().generation;

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::ReadBodyPlan {
                    inode,
                    generation,
                    offset: 1,
                    len: 2,
                },
                MetadataRpcRequest::ReadBodyPlan {
                    inode,
                    generation: generation - 1,
                    offset: 0,
                    len: 1,
                },
                MetadataRpcRequest::ReadBodyPlan {
                    inode,
                    generation,
                    offset: 2,
                    len: 2,
                },
            ],
        },
    );

    assert!(envelope.ok, "unexpected batch error: {envelope:?}");
    let MetadataRpcResult::Batch { results } = envelope.result.unwrap() else {
        panic!("unexpected batch read plan result")
    };
    assert_eq!(results.len(), 3);

    let first = &results[0];
    assert!(first.ok, "unexpected first item error: {first:?}");
    let MetadataRpcResult::BodyReadPlan { plan } = first.result.as_ref().unwrap() else {
        panic!("unexpected first item result")
    };
    assert_eq!(plan.output_len, 2);
    assert_eq!(plan.blocks[0].object_offset, 1);

    assert!(!results[1].ok);
    assert!(matches!(
        &results[1].error_kind,
        Some(WireMetadataError::StaleBodyGeneration { .. })
    ));

    let third = &results[2];
    assert!(third.ok, "unexpected third item error: {third:?}");
    let MetadataRpcResult::BodyReadPlan { plan } = third.result.as_ref().unwrap() else {
        panic!("unexpected third item result")
    };
    assert_eq!(plan.output_len, 2);
    assert_eq!(plan.blocks[0].object_offset, 2);
}

#[test]
fn rpc_open_read_plans_return_lease_metadata_and_plan() {
    let server = test_server();
    publish_synthetic_file(&server, "/artifact.bin", "artifact.bin", false);
    let entry = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::LookupPath {
            path: "/artifact.bin".to_owned(),
        },
    ));
    let inode = entry.attr.inode;
    let generation = entry.body.as_ref().unwrap().generation;

    let path = request_envelope(
        &server,
        MetadataRpcRequest::OpenPathReadPlan {
            path: "/artifact.bin".to_owned(),
            offset: 1,
            len: 2,
            expected_generation: Some(generation),
        },
    );
    assert!(path.ok, "unexpected path layout-open error: {path:?}");
    let MetadataRpcResult::OpenPathReadPlan {
        metadata,
        lease,
        plan,
    } = path.result.unwrap()
    else {
        panic!("unexpected path layout-open result")
    };
    assert_eq!(metadata.attr.inode, inode);
    assert_eq!(lease.inode, inode);
    assert_eq!(lease.generation, generation);
    assert_eq!(plan.output_len, 2);
    assert_eq!(plan.blocks[0].object_offset, 1);

    let stale = request_envelope(
        &server,
        MetadataRpcRequest::OpenPathReadPlan {
            path: "/artifact.bin".to_owned(),
            offset: 0,
            len: 1,
            expected_generation: Some(generation - 1),
        },
    );
    assert!(!stale.ok);
    assert!(matches!(
        stale.error_kind,
        Some(WireMetadataError::StaleBodyGeneration { .. })
    ));
}

#[test]
fn rpc_open_read_plan_batch_returns_one_result_per_request() {
    let server = test_server();
    publish_synthetic_file(&server, "/sample-0.bin", "sample-0.bin", false);
    publish_synthetic_file(&server, "/sample-1.bin", "sample-1.bin", false);
    let first = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::LookupPath {
            path: "/sample-0.bin".to_owned(),
        },
    ));
    let second = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::LookupPath {
            path: "/sample-1.bin".to_owned(),
        },
    ));

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: vec![
                WireOpenPathReadPlanRequest {
                    path: "/sample-0.bin".to_owned(),
                    offset: 1,
                    len: 2,
                    expected_generation: Some(first.body.as_ref().unwrap().generation),
                },
                WireOpenPathReadPlanRequest {
                    path: "/sample-1.bin".to_owned(),
                    offset: 2,
                    len: 1,
                    expected_generation: Some(second.body.as_ref().unwrap().generation),
                },
            ],
        },
    );
    assert!(
        envelope.ok,
        "unexpected batch layout-open error: {envelope:?}"
    );
    let MetadataRpcResult::OpenPathReadPlanBatch { plans } = envelope.result.unwrap() else {
        panic!("unexpected batch layout-open result")
    };

    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].metadata.attr.inode, first.attr.inode);
    assert_eq!(plans[0].plan.output_len, 2);
    assert_eq!(plans[0].plan.blocks[0].object_offset, 1);
    assert_eq!(plans[1].metadata.attr.inode, second.attr.inode);
    assert_eq!(plans[1].plan.output_len, 1);
    assert_eq!(plans[1].plan.blocks[0].object_offset, 2);
    assert_eq!(plans[0].lease.read_version, plans[1].lease.read_version);
}

#[test]
fn rpc_path_ops_resolve_on_server_side() {
    let server = test_server();
    let dir = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(dir.dentry.name_hex, "72756e73");
    let file = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePath {
            path: "/runs/checkpoint.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(file.dentry.name_hex, "636865636b706f696e742e62696e");

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match envelope.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "636865636b706f696e742e62696e");
}

#[test]
fn rpc_lists_directory_pages_with_name_cursor() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    for name in ["a.bin", "b.bin", "c.bin"] {
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: format!("/runs/{name}"),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        ));
    }

    let first = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPathPage {
            path: "/runs".to_owned(),
            after_name_hex: None,
            limit: 2,
        },
    );
    let (entries, cursor) = match first.result.unwrap() {
        MetadataRpcResult::DentriesPage {
            entries,
            next_name_hex,
        } => (entries, next_name_hex),
        other => panic!("unexpected page result: {other:?}"),
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].dentry.name_hex, "612e62696e");
    assert_eq!(entries[1].dentry.name_hex, "622e62696e");
    assert_eq!(cursor.as_deref(), Some("622e62696e"));

    let second = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPathPage {
            path: "/runs".to_owned(),
            after_name_hex: cursor,
            limit: 2,
        },
    );
    let (entries, cursor) = match second.result.unwrap() {
        MetadataRpcResult::DentriesPage {
            entries,
            next_name_hex,
        } => (entries, next_name_hex),
        other => panic!("unexpected page result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "632e62696e");
    assert_eq!(cursor, None);
}

#[test]
fn rpc_lists_indexed_path_pages_without_plain_namespace_entries() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePath {
            path: "/runs/plain.txt".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    let prepared = match request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifactPath {
            path: "/runs/metrics.json".to_owned(),
            replace: false,
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let published = request_envelope(
        &server,
        MetadataRpcRequest::PublishPreparedArtifact {
            body: Box::new(WireBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:metrics".to_owned(),
                size: 2,
                content_type: "application/json".to_owned(),
                manifest_id: "metrics.json".to_owned(),
                generation: prepared.generation,
                base_generation: 0,
                chunk_size: 64 * 1024 * 1024,
                block_size: 4 * 1024 * 1024,
            }),
            chunks: vec![WireChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 2,
                slices: vec![WireSliceManifest {
                    slice_id: 1,
                    logical_offset: 0,
                    len: 2,
                    blocks: vec![WireBlockDescriptor {
                        object_key: format!("blocks/1/{}/{}", prepared.inode, prepared.generation),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 2,
                        digest_uri: "sha256:block".to_owned(),
                    }],
                }],
            }],
            prepared,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(published.ok, "unexpected publish error: {published:?}");

    let page = request_envelope(
        &server,
        MetadataRpcRequest::ReadIndexedPathPage {
            path: "/runs".to_owned(),
            after_name_hex: None,
            limit: 100,
        },
    );
    let entries = match page.result.unwrap() {
        MetadataRpcResult::DentriesPage {
            entries,
            next_name_hex,
        } => {
            assert_eq!(next_name_hex, None);
            entries
        }
        other => panic!("unexpected indexed page result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "6d6574726963732e6a736f6e");
}

#[test]
fn rpc_batch_preserves_ordered_per_request_results() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );
    let results = match envelope.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected batch result: {other:?}"),
    };
    assert_eq!(results.len(), 3);
    assert!(results[0].ok);
    assert!(results[1].ok);
    assert!(!results[2].ok);
    assert!(results[2].error.is_some());
    assert_eq!(
        results[2].error_kind,
        Some(WireMetadataError::PredicateFailed)
    );
}

#[test]
fn rpc_batch_coalesces_same_parent_create_dir_paths() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let before = server.service().metadata_store_stats();

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/a".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/b".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );

    let results = match envelope.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected batch result: {other:?}"),
    };
    let after = server.service().metadata_store_stats();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert_eq!(after.commit_total - before.commit_total, 1);
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 2);
}

#[test]
fn rpc_batch_coalesces_same_parent_remove_file_paths() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let created = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/b.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/keep.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );
    let created = match created.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected create batch result: {other:?}"),
    };
    assert!(created.iter().all(|result| result.ok));
    let before = server.service().metadata_store_stats();

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/a.bin".to_owned(),
                },
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/b.bin".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected remove batch result: {other:?}"),
    };
    let after = server.service().metadata_store_stats();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "6b6565702e62696e");
}

#[test]
fn rpc_batch_keeps_duplicate_remove_file_paths_independent() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePath {
            path: "/runs/a.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/a.bin".to_owned(),
                },
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/a.bin".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected remove batch result: {other:?}"),
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].ok);
    assert!(!results[1].ok);
    assert_eq!(results[1].error_kind, Some(WireMetadataError::NotFound));
}

#[test]
fn rpc_batch_coalesces_same_parent_remove_empty_dir_paths() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let created = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/a".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/b".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/keep".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );
    let created = match created.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected create batch result: {other:?}"),
    };
    assert!(created.iter().all(|result| result.ok));
    let before = server.service().metadata_store_stats();

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/a".to_owned(),
                },
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/b".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected rmdir batch result: {other:?}"),
    };
    let after = server.service().metadata_store_stats();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "6b656570");
}

#[test]
fn rpc_batch_keeps_duplicate_remove_empty_dir_paths_independent() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs/a".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/a".to_owned(),
                },
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/a".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected rmdir batch result: {other:?}"),
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].ok);
    assert!(!results[1].ok);
    assert_eq!(results[1].error_kind, Some(WireMetadataError::NotFound));
}

#[test]
fn rpc_batch_coalesces_multi_parent_create_files_into_metadata_batch() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs/a".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs/b".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a/one.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/b/two.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );

    let results = match envelope.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected batch result: {other:?}"),
    };
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert!(server.stats_json().contains("\"atomic_apply_max_batch\":2"));
}

#[test]
fn framed_rpc_coalesces_pipelined_create_frames_into_metadata_batch() {
    let server = Arc::new(test_server());
    server
        .service()
        .create_dir_path("/runs", 0o755, 1000, 1000)
        .unwrap();
    server
        .service()
        .create_dir_path("/runs/a", 0o755, 1000, 1000)
        .unwrap();
    server
        .service()
        .create_dir_path("/runs/b", 0o755, 1000, 1000)
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(FRAMED_RPC_MAGIC).unwrap();
    write_frame(
        &mut client,
        1,
        0,
        &encode_request(&MetadataRpcRequest::CreateFilePath {
            path: "/runs/a/one.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(
        &mut client,
        2,
        0,
        &encode_request(&MetadataRpcRequest::CreateFilePath {
            path: "/runs/b/two.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap(),
    )
    .unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();

    let server_thread = {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            crate::http::handle_stream(server, stream).unwrap();
        })
    };

    let first = read_frame(&mut client).unwrap().unwrap();
    let second = read_frame(&mut client).unwrap().unwrap();
    assert_eq!(first.request_id, 1);
    assert_eq!(second.request_id, 2);
    assert!(decode_envelope(&first.payload).unwrap().ok);
    assert!(decode_envelope(&second.payload).unwrap().ok);
    server_thread.join().unwrap();

    assert!(server.stats_json().contains("\"atomic_apply_max_batch\":2"));
}

#[test]
fn rpc_reports_predicate_errors_without_panicking() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::RemoveEmptyDir {
            parent: 1,
            name: "missing".to_owned(),
        },
    );
    assert!(!envelope.ok);
    assert!(envelope.error.is_some());
}

#[test]
fn rpc_reports_stale_owner_epoch_as_typed_error() {
    let server = test_server();
    server.service().observe_required_owner_epoch(2).unwrap();

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/stale-owner".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    );

    assert!(!envelope.ok);
    assert!(matches!(
        envelope.error_kind,
        Some(WireMetadataError::StaleOwnerEpoch {
            owner_epoch: 1,
            required_epoch: 2
        })
    ));
}

#[test]
fn rpc_rejects_malformed_binary_request() {
    let server = test_server();
    let response = handle_binary_rpc(&server, b"not-msgpack").unwrap();
    let envelope = decode_envelope(&response).unwrap();
    assert!(!envelope.ok);
    assert!(matches!(
        envelope.error_kind,
        Some(WireMetadataError::Protocol { .. })
    ));
}

#[test]
fn framed_rpc_worker_count_is_bounded() {
    let workers = default_framed_rpc_worker_count();
    assert!(workers >= MIN_FRAMED_RPC_WORKERS);
    assert!(workers <= MAX_FRAMED_RPC_WORKERS);
}

#[test]
fn framed_rpc_client_reuses_peer_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let mut request_ids = Vec::new();
        for _ in 0..2 {
            let frame = read_frame(&mut stream).unwrap().unwrap();
            request_ids.push(frame.request_id);
            let response = encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::RetiredSnapshot { retired: false }),
                error: None,
                error_kind: None,
            })
            .unwrap();
            write_frame(&mut stream, frame.request_id, frame.flags, &response).unwrap();
        }
        request_ids
    });

    let client = FramedRpcClient::new(addr);
    let first = client
        .call(&MetadataRpcRequest::RetireSnapshot {
            root_path: "/".to_owned(),
            snapshot_id: 1,
        })
        .unwrap();
    let second = client
        .call(&MetadataRpcRequest::RetireSnapshot {
            root_path: "/".to_owned(),
            snapshot_id: 2,
        })
        .unwrap();
    assert!(first.ok);
    assert!(second.ok);
    assert_eq!(server_thread.join().unwrap(), vec![1, 2]);
}

#[test]
fn rpc_prepares_and_publishes_artifact_manifest() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifact {
            parent: 1,
            name: "artifact.bin".to_owned(),
            replace: false,
        },
    );
    let prepared = match envelope.result.unwrap() {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let request = MetadataRpcRequest::PublishPreparedArtifact {
        body: Box::new(WireBodyDescriptor {
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:body".to_owned(),
            size: 4,
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "artifact.bin".to_owned(),
            generation: prepared.generation,
            base_generation: 0,
            chunk_size: 64 * 1024 * 1024,
            block_size: 4 * 1024 * 1024,
        }),
        chunks: vec![WireChunkManifest {
            chunk_index: 0,
            logical_offset: 0,
            len: 4,
            slices: vec![WireSliceManifest {
                slice_id: 1,
                logical_offset: 0,
                len: 4,
                blocks: vec![WireBlockDescriptor {
                    object_key: format!("blocks/1/{}/{}", prepared.inode, prepared.generation),
                    logical_offset: 0,
                    object_offset: 0,
                    len: 4,
                    digest_uri: "sha256:block".to_owned(),
                }],
            }],
        }],
        prepared,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };
    let envelope = request_envelope(&server, request);
    let result = match envelope.result.unwrap() {
        MetadataRpcResult::RenameReplace { entry, replaced } => (entry, replaced),
        other => panic!("unexpected publish result: {other:?}"),
    };
    assert_eq!(result.0.dentry.name_hex, "61727469666163742e62696e");
    assert!(result.1.is_none());
}

#[test]
fn rpc_staged_session_publish_preserves_old_prefix_on_shrink() {
    let server = test_server();
    let prepared = match request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifact {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            replace: false,
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let old_object_key = format!("blocks/1/{}/{}", prepared.inode, prepared.generation);
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::PublishPreparedArtifact {
            body: Box::new(WireBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:old".to_owned(),
                size: 14,
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint.bin".to_owned(),
                generation: prepared.generation,
                base_generation: 0,
                chunk_size: 64 * 1024 * 1024,
                block_size: 4 * 1024 * 1024,
            }),
            chunks: vec![WireChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 14,
                slices: vec![WireSliceManifest {
                    slice_id: 1,
                    logical_offset: 0,
                    len: 14,
                    blocks: vec![WireBlockDescriptor {
                        object_key: old_object_key.clone(),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 14,
                        digest_uri: "sha256:block-old".to_owned(),
                    }],
                }],
            }],
            prepared,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(envelope.ok, "initial publish failed: {envelope:?}");
    let inode = match envelope.result.unwrap() {
        MetadataRpcResult::RenameReplace { entry, .. } => entry.attr.inode,
        other => panic!("unexpected publish result: {other:?}"),
    };

    let prepared = match request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifact {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            replace: true,
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let new_generation = prepared.generation;
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared,
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:shrunk".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "checkpoint.bin".to_owned(),
            size: 5,
            chunks: Vec::new(),
            staged: WireStagedObjectSet::default(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(envelope.ok, "staged-session publish failed: {envelope:?}");

    let plan = request_envelope(
        &server,
        MetadataRpcRequest::ReadBodyPlan {
            inode,
            generation: new_generation,
            offset: 0,
            len: 10,
        },
    );
    let plan = match plan.result.unwrap() {
        MetadataRpcResult::BodyReadPlan { plan } => plan,
        other => panic!("unexpected body plan result: {other:?}"),
    };
    assert_eq!(plan.output_len, 5);
    assert_eq!(plan.blocks.len(), 1);
    assert_eq!(plan.blocks[0].object_key, old_object_key);
    assert_eq!(plan.blocks[0].object_offset, 0);
    assert_eq!(plan.blocks[0].len, 5);
    assert_eq!(plan.blocks[0].output_offset, 0);
}

/// Publish a metadata-only file body at `path` with a synthetic single-block
/// manifest (no object bytes are written, so this works against the test server's
/// fake object endpoint). The body carries a real generation, which is the
/// copy-on-write sharing signal a clone and diff key on.
fn publish_synthetic_file(server: &Server, path: &str, manifest_id: &str, replace: bool) {
    let prepared = if replace {
        server
            .service()
            .prepare_artifact_replace_path(path)
            .unwrap()
    } else {
        server.service().prepare_artifact_create_path(path).unwrap()
    };
    server
        .service()
        .publish_prepared_artifact(
            prepared.clone(),
            nokv_types::BodyDescriptor {
                producer: "rpc-test".to_owned(),
                digest_uri: format!("sha256:{manifest_id}"),
                size: 4,
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest_id.to_owned(),
                generation: prepared.generation,
                base_generation: 0,
                chunk_size: nokv_object::DEFAULT_CHUNK_SIZE,
                block_size: nokv_object::DEFAULT_BLOCK_SIZE as u64,
            },
            vec![nokv_types::ChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 4,
                slices: vec![nokv_types::SliceManifest {
                    slice_id: 1,
                    logical_offset: 0,
                    len: 4,
                    blocks: vec![nokv_types::BlockDescriptor {
                        object_key: format!("blocks/{manifest_id}"),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 4,
                        digest_uri: format!("sha256:{manifest_id}"),
                    }],
                }],
            }],
            0o644,
            1000,
            1000,
        )
        .unwrap();
}

fn fork_entry_generation(server: &Server, path: &str) -> u64 {
    match request_envelope(
        server,
        MetadataRpcRequest::LookupPath {
            path: path.to_owned(),
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::Dentry { entry: Some(entry) } => entry.body.unwrap().generation,
        other => panic!("unexpected lookup result for {path}: {other:?}"),
    }
}

#[test]
fn rpc_clone_subtree_links_navigable_fork_and_diff_tracks_divergence() {
    let server = test_server();
    // Base namespace: /base with a file body and a nested directory + file.
    let base = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/base".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    publish_synthetic_file(&server, "/base/a", "base-a", false);
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/base/sub".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    publish_synthetic_file(&server, "/base/sub/deep", "base-deep", false);

    // Clone /base -> /fork through the RPC surface.
    let cloned = request_envelope(
        &server,
        MetadataRpcRequest::CloneSubtreePath {
            src_path: "/base".to_owned(),
            dst_path: "/fork".to_owned(),
        },
    );
    assert!(cloned.ok, "unexpected clone error: {cloned:?}");
    let (fork_root, snapshot_id) = match cloned.result.unwrap() {
        MetadataRpcResult::CloneSubtree { root, snapshot_id } => (root, snapshot_id),
        other => panic!("unexpected clone result: {other:?}"),
    };
    assert!(fork_root > base.attr.inode, "fork gets a fresh root inode");
    assert!(snapshot_id > 0, "clone retains a snapshot pin");

    // The fork is a real, navigable directory at /fork: listing it through the
    // path RPC surfaces the base's entries under fresh inodes.
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/fork".to_owned(),
        },
    );
    let mut names: Vec<String> = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries
            .iter()
            .map(|entry| entry.dentry.name_hex.clone())
            .collect(),
        other => panic!("unexpected readdir result: {other:?}"),
    };
    names.sort();
    assert_eq!(names, vec!["61".to_owned(), "737562".to_owned()]); // "a", "sub"

    // Copy-on-write sharing: the fork's file body shares the base body generation
    // (the byte-level sharing signal) before any divergent write.
    let base_a_generation = fork_entry_generation(&server, "/base/a");
    assert_eq!(
        fork_entry_generation(&server, "/fork/a"),
        base_a_generation,
        "fork shares the base body generation"
    );

    // Identical subtree => no deltas.
    let diff = request_envelope(
        &server,
        MetadataRpcRequest::DiffSubtrees {
            a_path: "/base".to_owned(),
            b_path: "/fork".to_owned(),
        },
    );
    match diff.result.unwrap() {
        MetadataRpcResult::SubtreeDeltas { deltas } => {
            assert!(deltas.is_empty(), "fresh clone has no deltas: {deltas:?}");
        }
        other => panic!("unexpected diff result: {other:?}"),
    }

    // Diverge the fork independently: rewrite a (bumping its generation) and add a
    // new file b. The base must stay untouched.
    publish_synthetic_file(&server, "/fork/a", "fork-a", true);
    publish_synthetic_file(&server, "/fork/b", "fork-b", false);
    assert_ne!(
        fork_entry_generation(&server, "/fork/a"),
        base_a_generation,
        "the fork write mints a fresh generation"
    );
    assert_eq!(
        fork_entry_generation(&server, "/base/a"),
        base_a_generation,
        "the base body is unaffected by the fork write"
    );

    // Diff now reports exactly { a: Modified, b: Added }; the shared sub/deep is not
    // reported.
    let diff = request_envelope(
        &server,
        MetadataRpcRequest::DiffSubtrees {
            a_path: "/base".to_owned(),
            b_path: "/fork".to_owned(),
        },
    );
    let mut deltas = match diff.result.unwrap() {
        MetadataRpcResult::SubtreeDeltas { deltas } => deltas,
        other => panic!("unexpected diff result: {other:?}"),
    };
    deltas.sort_by(|left, right| left.path.cmp(&right.path));
    let summary: Vec<(&str, &nokv_protocol::WireSubtreeDeltaKind)> =
        deltas.iter().map(|d| (d.path.as_str(), &d.kind)).collect();
    assert_eq!(
        summary,
        vec![
            ("/a", &nokv_protocol::WireSubtreeDeltaKind::Modified),
            ("/b", &nokv_protocol::WireSubtreeDeltaKind::Added),
        ]
    );
    // The enriched delta carries the changed file's content digest end-to-end.
    assert!(deltas
        .iter()
        .find(|d| d.path == "/a")
        .unwrap()
        .digest
        .is_some());
}

fn dir_names_sorted(server: &Server, path: &str) -> Vec<String> {
    let listed = request_envelope(
        server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: path.to_owned(),
        },
    );
    let mut names: Vec<String> = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries
            .iter()
            .map(|entry| entry.dentry.name_hex.clone())
            .collect(),
        other => panic!("unexpected readdir result for {path}: {other:?}"),
    };
    names.sort();
    names
}

#[test]
fn rpc_snapshot_then_rollback_restores_namespace_to_snapshot() {
    let server = test_server();
    // Base namespace: /base with a file body (a) and a nested directory + file
    // (sub/deep). The snapshot captures this exact state.
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/base".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    publish_synthetic_file(&server, "/base/a", "base-a", false);
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/base/sub".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    publish_synthetic_file(&server, "/base/sub/deep", "base-deep", false);
    let base_a_generation = fork_entry_generation(&server, "/base/a");

    // Pin a snapshot of /base through the RPC surface and capture its id.
    let snapshot = request_envelope(
        &server,
        MetadataRpcRequest::SnapshotSubtreePath {
            root_path: "/base".to_owned(),
            lease_ms: nokv_meta::DEFAULT_SNAPSHOT_LEASE_MS,
        },
    );
    assert!(snapshot.ok, "unexpected snapshot error: {snapshot:?}");
    let (snapshot_id, read_version) = match snapshot.result.unwrap() {
        MetadataRpcResult::Snapshot { snapshot } => (snapshot.snapshot_id, snapshot.read_version),
        other => panic!("unexpected snapshot result: {other:?}"),
    };
    assert!(snapshot_id > 0, "snapshot retains a durable id");
    assert!(read_version > 0, "snapshot captures a read version");

    // Diverge /base after the snapshot: modify a (bumping its generation), add a new
    // file c, and delete sub/deep.
    publish_synthetic_file(&server, "/base/a", "base-a-2", true);
    publish_synthetic_file(&server, "/base/c", "base-c", false);
    let removed = request_envelope(
        &server,
        MetadataRpcRequest::RemoveFilePath {
            path: "/base/sub/deep".to_owned(),
        },
    );
    assert!(removed.ok, "unexpected remove error: {removed:?}");
    assert_ne!(
        fork_entry_generation(&server, "/base/a"),
        base_a_generation,
        "the post-snapshot write mints a fresh generation"
    );
    assert_eq!(
        dir_names_sorted(&server, "/base"),
        vec!["61".to_owned(), "63".to_owned(), "737562".to_owned()]
    ); // a, c, sub
    assert!(
        dir_names_sorted(&server, "/base/sub").is_empty(),
        "deep is deleted before rollback"
    );

    // Roll /base back to the snapshot through the RPC surface.
    let rolled = request_envelope(
        &server,
        MetadataRpcRequest::RollbackSubtreePath {
            target_path: "/base".to_owned(),
            snapshot_id,
        },
    );
    assert!(rolled.ok, "unexpected rollback error: {rolled:?}");
    assert!(matches!(rolled.result.unwrap(), MetadataRpcResult::Unit));

    // The namespace is restored to the snapshot: the modification to a is undone
    // (generation back to the snapshot's), the added c is gone, and the deleted
    // sub/deep is back.
    assert_eq!(
        fork_entry_generation(&server, "/base/a"),
        base_a_generation,
        "rollback restores a to the snapshot generation"
    );
    assert_eq!(
        dir_names_sorted(&server, "/base"),
        vec!["61".to_owned(), "737562".to_owned()],
        "rollback drops the post-snapshot c and keeps a, sub"
    );
    assert_eq!(
        dir_names_sorted(&server, "/base/sub"),
        vec!["64656570".to_owned()],
        "rollback restores the deleted sub/deep"
    );

    // The restored subtree is identical to the snapshot: diffing the live /base
    // against the snapshot view reports no deltas.
    let diff = request_envelope(
        &server,
        MetadataRpcRequest::DiffSubtrees {
            a_path: "/base".to_owned(),
            b_path: "/base".to_owned(),
        },
    );
    match diff.result.unwrap() {
        MetadataRpcResult::SubtreeDeltas { deltas } => {
            assert!(deltas.is_empty(), "restored tree diffs clean: {deltas:?}");
        }
        other => panic!("unexpected diff result: {other:?}"),
    }
}

#[test]
fn rpc_rollback_rejects_unknown_snapshot() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/base".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let rolled = request_envelope(
        &server,
        MetadataRpcRequest::RollbackSubtreePath {
            target_path: "/base".to_owned(),
            snapshot_id: 9999,
        },
    );
    assert!(!rolled.ok, "rollback to an unknown snapshot must fail");
    assert!(matches!(
        rolled.error_kind,
        Some(WireMetadataError::NotFound)
    ));
}

#[test]
fn rpc_clone_subtree_rejects_existing_destination() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/base".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/fork".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let cloned = request_envelope(
        &server,
        MetadataRpcRequest::CloneSubtreePath {
            src_path: "/base".to_owned(),
            dst_path: "/fork".to_owned(),
        },
    );
    assert!(!cloned.ok, "clone onto an occupied path must fail");
    assert!(matches!(
        cloned.error_kind,
        Some(WireMetadataError::PredicateFailed)
    ));
}

/// Phase 4 — per-shard MVCC/snapshot audit. Every test above drives a single
/// default shard; this module proves the multi-shard `Server` routes correctly,
/// keeps each shard's allocator/commit clock independent, scopes subtree reads to
/// the owning shard, and fences requests for shards it does not host.
mod multi_shard {
    use super::*;
    use nokv_types::{InodeId, DEFAULT_SHARD_INDEX};
    use tempfile::TempDir;

    const DATASET_INDEX: u16 = 1;

    /// One `Server` hosting two shards of the same mount: the default `mount-1:/`
    /// (index 0) and `mount-1:/dataset` (index 1). Both identities are registered
    /// in the control store *before* `open_with_control` so each slot reads its
    /// index/prefix from the record. The returned `TempDir` keeps the per-shard
    /// Holt engines alive for the lifetime of the server.
    fn two_shard_server() -> (Server, Arc<InMemoryControlStore>, TempDir) {
        let dir = tempdir().unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        nokv_control::register_shard(
            control.as_ref(),
            ShardId::new("mount-1:/"),
            "/",
            DEFAULT_SHARD_INDEX,
        )
        .unwrap();
        nokv_control::register_shard(
            control.as_ref(),
            ShardId::new("mount-1:/dataset"),
            "/dataset",
            DATASET_INDEX,
        )
        .unwrap();
        let server = Server::open_with_control(
            test_options(dir.path()),
            control.clone(),
            vec![
                ServerShardOwnerOptions::fresh("mount-1:/", "node-a").with_renewal(None),
                ServerShardOwnerOptions::fresh("mount-1:/dataset", "node-a").with_renewal(None),
            ],
        )
        .unwrap();
        (server, control, dir)
    }

    fn create_dir_path(server: &Server, path: &str) -> nokv_protocol::WireInodeAttr {
        expect_dentry(request_envelope(
            server,
            MetadataRpcRequest::CreateDirPath {
                path: path.to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ))
        .attr
    }

    fn create_file_path(server: &Server, path: &str) -> nokv_protocol::WireInodeAttr {
        expect_dentry(request_envelope(
            server,
            MetadataRpcRequest::CreateFilePath {
                path: path.to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        ))
        .attr
    }

    fn route_index(server: &Server, request: &MetadataRpcRequest) -> u16 {
        server.route(request).unwrap().shard_index()
    }

    /// A parent shard self-heals a graft on startup from the durable control
    /// record. Models the crash window in `register_graft`: the subtree dir + the
    /// control `subtree_root_inode` landed, but the parent graft dentry write was
    /// lost. `reconcile_local_grafts` (run at the head of `serve`) re-creates it
    /// against the local parent slot, no RPC.
    #[test]
    fn server_reconciles_missing_graft_on_startup() {
        let (server, control, _dir) = two_shard_server();

        // The subtree dir exists on shard 1 (its inode carries index 1).
        let subtree = create_dir_path(&server, "/dataset");
        let subtree_inode = InodeId::new(subtree.inode).unwrap();
        assert_eq!(subtree_inode.shard_index(), DATASET_INDEX);

        // The durable registration landed, but the parent graft dentry did NOT:
        // an inode-routed lookup of (root, "dataset") on shard 0 misses.
        control
            .set_subtree_root_inode(&ShardId::new("mount-1:/dataset"), Some(subtree_inode.get()))
            .unwrap();
        let before = request_envelope(
            &server,
            MetadataRpcRequest::LookupPlus {
                parent: InodeId::root().get(),
                name: "dataset".to_owned(),
            },
        );
        assert!(
            matches!(
                before.result.unwrap(),
                MetadataRpcResult::Dentry { entry: None }
            ),
            "no parent graft dentry exists before reconcile"
        );

        // Reconcile heals it locally.
        server.reconcile_local_grafts();

        let after = expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::LookupPlus {
                parent: InodeId::root().get(),
                name: "dataset".to_owned(),
            },
        ));
        assert_eq!(
            InodeId::new(after.attr.inode).unwrap(),
            subtree_inode,
            "reconcile re-created the parent graft dentry pointing at the shard-1 subtree inode"
        );

        // Idempotent: a second reconcile is a no-op (the dentry already exists).
        server.reconcile_local_grafts();
        let still = expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::LookupPlus {
                parent: InodeId::root().get(),
                name: "dataset".to_owned(),
            },
        ));
        assert_eq!(InodeId::new(still.attr.inode).unwrap(), subtree_inode);
    }

    /// A path create under `/dataset` is served by shard 1 and the new inode is
    /// tagged with shard index 1; a create outside `/dataset` is served by shard
    /// 0 and tagged with index 0. Routing keys on the path for path ops and on the
    /// encoded shard index for bare-inode ops.
    #[test]
    fn create_routes_by_prefix_and_tags_inode_shard_index() {
        let (server, _control, _dir) = two_shard_server();

        // Shard 1's namespace is independent: its own `/dataset` directory is
        // created inside shard 1 (its root is shard 1's root), then a file under it.
        let dataset_dir = create_dir_path(&server, "/dataset");
        assert_eq!(
            InodeId::new(dataset_dir.inode).unwrap().shard_index(),
            DATASET_INDEX,
            "/dataset is minted by shard 1"
        );
        let dataset_file = create_file_path(&server, "/dataset/checkpoint.bin");
        assert_eq!(
            InodeId::new(dataset_file.inode).unwrap().shard_index(),
            DATASET_INDEX,
            "a file under /dataset is minted by shard 1"
        );

        // A create outside the /dataset subtree falls to the default shard.
        let other_dir = create_dir_path(&server, "/other");
        assert_eq!(
            InodeId::new(other_dir.inode).unwrap().shard_index(),
            DEFAULT_SHARD_INDEX,
            "/other is minted by the default shard 0"
        );

        // route() resolves the same slots the creates landed on.
        assert_eq!(
            route_index(
                &server,
                &MetadataRpcRequest::CreateFilePath {
                    path: "/dataset/another.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }
            ),
            DATASET_INDEX,
            "a /dataset path request routes to shard 1"
        );
        assert_eq!(
            route_index(
                &server,
                &MetadataRpcRequest::CreateDirPath {
                    path: "/other/sub".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                }
            ),
            DEFAULT_SHARD_INDEX,
            "an /other path request routes to shard 0"
        );

        // A bare-inode request routes on the inode's encoded shard index alone:
        // the shard-1 file's inode resolves to the shard-1 slot, the shard-0
        // directory's inode to the shard-0 slot.
        assert_eq!(
            route_index(
                &server,
                &MetadataRpcRequest::GetAttr {
                    inode: dataset_file.inode
                }
            ),
            DATASET_INDEX,
            "a bare-inode request for a shard-1 inode routes to shard 1"
        );
        assert_eq!(
            route_index(
                &server,
                &MetadataRpcRequest::GetAttr {
                    inode: other_dir.inode
                }
            ),
            DEFAULT_SHARD_INDEX,
            "a bare-inode request for a shard-0 inode routes to shard 0"
        );
    }

    #[test]
    fn snapshot_path_lifecycle_routes_to_the_root_owning_shard() {
        let (server, _control, _dir) = two_shard_server();
        let dataset = create_dir_path(&server, "/dataset");
        create_file_path(&server, "/dataset/checkpoint.bin");

        let snapshot = request_envelope(
            &server,
            MetadataRpcRequest::SnapshotSubtreePath {
                root_path: "/dataset".to_owned(),
                lease_ms: 60_000,
            },
        );
        let snapshot = match snapshot.result.unwrap() {
            MetadataRpcResult::Snapshot { snapshot } => snapshot,
            other => panic!("unexpected snapshot result: {other:?}"),
        };
        assert_eq!(
            InodeId::new(snapshot.root).unwrap().shard_index(),
            DATASET_INDEX
        );

        let renewed = request_envelope(
            &server,
            MetadataRpcRequest::RenewSnapshot {
                root_path: "/dataset".to_owned(),
                snapshot_id: snapshot.snapshot_id,
                lease_ms: 120_000,
            },
        );
        assert!(matches!(
            renewed.result.unwrap(),
            MetadataRpcResult::RenewedSnapshot {
                outcome: nokv_protocol::WireSnapshotRenewOutcome::Renewed { .. }
            }
        ));

        let stat = request_envelope(
            &server,
            MetadataRpcRequest::StatPathAtSnapshot {
                root_path: "/dataset".to_owned(),
                snapshot_id: snapshot.snapshot_id,
                path: "/".to_owned(),
            },
        );
        let stat = match stat.result.unwrap() {
            MetadataRpcResult::PathMetadata {
                metadata: Some(metadata),
            } => metadata,
            other => panic!("unexpected snapshot stat result: {other:?}"),
        };
        assert_eq!(stat.attr.inode, dataset.inode);
    }

    #[test]
    fn swapped_snapshot_ids_across_shards_are_typed_root_mismatches() {
        let (server, _control, _dir) = two_shard_server();
        create_dir_path(&server, "/dataset");
        create_dir_path(&server, "/other");

        let mint = |root_path: &str| {
            let envelope = request_envelope(
                &server,
                MetadataRpcRequest::SnapshotSubtreePath {
                    root_path: root_path.to_owned(),
                    lease_ms: 60_000,
                },
            );
            match envelope.result.unwrap() {
                MetadataRpcResult::Snapshot { snapshot } => snapshot,
                other => panic!("unexpected snapshot result: {other:?}"),
            }
        };
        let dataset = mint("/dataset");
        let other = mint("/other");
        assert_eq!(dataset.snapshot_id >> 48, u64::from(DATASET_INDEX));
        assert_eq!(other.snapshot_id >> 48, u64::from(DEFAULT_SHARD_INDEX));

        for (root_path, snapshot_id, foreign_shard) in [
            ("/other", dataset.snapshot_id, DATASET_INDEX),
            ("/dataset", other.snapshot_id, DEFAULT_SHARD_INDEX),
        ] {
            let envelope = request_envelope(
                &server,
                MetadataRpcRequest::StatPathAtSnapshot {
                    root_path: root_path.to_owned(),
                    snapshot_id,
                    path: "/".to_owned(),
                },
            );
            assert!(!envelope.ok);
            assert!(matches!(
                envelope.error_kind,
                Some(WireMetadataError::SnapshotRootMismatch {
                    snapshot_id: actual_id,
                    actual_root: None,
                    actual_shard,
                    ..
                }) if actual_id == snapshot_id && actual_shard == foreign_shard
            ));
        }
    }

    /// Per-shard MVCC independence: the two shards mint inodes from disjoint
    /// high-bit subspaces (so ids never collide), and a commit on one shard does
    /// not advance the other's commit clock or inode high-water. The shard-1 root
    /// directory is its own object, distinct from shard 0's root.
    #[test]
    fn allocators_and_commit_clocks_are_independent() {
        let (server, _control, _dir) = two_shard_server();
        let shard0 = server
            .route(&MetadataRpcRequest::GetAttr { inode: 1 })
            .unwrap();
        let dataset_inode = InodeId::compose(DATASET_INDEX, InodeId::ROOT_RAW).unwrap();
        let shard1 = server
            .route(&MetadataRpcRequest::GetAttr {
                inode: dataset_inode.get(),
            })
            .unwrap();
        assert_eq!(shard0.shard_index(), DEFAULT_SHARD_INDEX);
        assert_eq!(shard1.shard_index(), DATASET_INDEX);

        // Two distinct Holt engines back the two shards.
        assert!(
            !std::ptr::eq(shard0.service(), shard1.service()),
            "each shard owns its own service/engine"
        );

        // Shard 1's own /dataset directory must exist before files land under it.
        create_dir_path(&server, "/dataset");

        // Drive a burst of creates into shard 1 only.
        let shard0_commits_before = shard0.service().metadata_store_stats().commit_total;
        let mut shard1_inodes = Vec::new();
        for i in 0..4 {
            shard1_inodes.push(create_file_path(&server, &format!("/dataset/f{i}.bin")).inode);
        }
        // Every shard-1 inode carries index 1; none carries index 0.
        for inode in &shard1_inodes {
            assert_eq!(InodeId::new(*inode).unwrap().shard_index(), DATASET_INDEX);
        }
        // Shard 0 did not commit while shard 1 was mutated.
        assert_eq!(
            shard0.service().metadata_store_stats().commit_total,
            shard0_commits_before,
            "a commit on shard 1 must not advance shard 0's commit clock"
        );

        // Now drive creates into shard 0 only and confirm the reverse isolation.
        let shard1_commits_before = shard1.service().metadata_store_stats().commit_total;
        let mut shard0_inodes = Vec::new();
        for i in 0..4 {
            shard0_inodes.push(create_file_path(&server, &format!("/g{i}.bin")).inode);
        }
        for inode in &shard0_inodes {
            assert_eq!(
                InodeId::new(*inode).unwrap().shard_index(),
                DEFAULT_SHARD_INDEX
            );
        }
        assert_eq!(
            shard1.service().metadata_store_stats().commit_total,
            shard1_commits_before,
            "a commit on shard 0 must not advance shard 1's commit clock"
        );

        // The two id spaces are disjoint: no shard-0 inode equals any shard-1
        // inode, the defining MVCC-independence property (each allocator hands out
        // ids from its own high-bit subspace, so cross-shard collisions are
        // impossible even with identical local counters).
        for a in &shard0_inodes {
            assert!(
                !shard1_inodes.contains(a),
                "shard-0 inode {a} collided with a shard-1 inode"
            );
        }
    }

    /// A request whose target resolves to a shard this server does NOT host is
    /// rejected with `NotOwner` (a re-resolve hint) instead of being silently
    /// served by the wrong shard. The server hosts indices {0, 1}; a bare-inode
    /// request for an index-5 inode has no local slot.
    #[test]
    fn route_rejects_unhosted_shard_as_not_owner() {
        let (server, _control, _dir) = two_shard_server();
        let foreign = InodeId::compose(5, 7).unwrap();
        // `ShardSlot` is not `Debug`, so match instead of `unwrap_err`.
        match server.route(&MetadataRpcRequest::GetAttr {
            inode: foreign.get(),
        }) {
            Err(ServerError::NotOwner { shard_id, .. }) => assert!(
                shard_id.contains("shard-5"),
                "NotOwner must name the unhosted shard index, got {shard_id}"
            ),
            Ok(slot) => panic!(
                "an inode for an unhosted shard must surface NotOwner, was served by shard {}",
                slot.shard_index()
            ),
            Err(other) => panic!("expected NotOwner, got {other:?}"),
        }

        // The same is true end-to-end through the binary RPC surface: the wrong
        // shard never silently serves it.
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::GetAttr {
                inode: foreign.get(),
            },
        );
        assert!(!envelope.ok, "an unhosted-shard RPC must not succeed");
    }

    /// Subtree-scoped reads are confined to the shard that owns the subtree: a
    /// readdir of `/dataset` sees only shard-1 entries, and a readdir of `/` (the
    /// default shard's root) sees only shard-0 entries — the `/dataset` mount
    /// point is not a shard-0 namespace entry, so the two namespaces never bleed.
    #[test]
    fn subtree_reads_are_scoped_to_owning_shard() {
        let (server, _control, _dir) = two_shard_server();
        // Shard 1's own subtree.
        create_dir_path(&server, "/dataset");
        create_file_path(&server, "/dataset/in-shard-1.bin");
        // Shard 0's subtree (disjoint names so a leak would be obvious).
        create_dir_path(&server, "/runs");
        create_file_path(&server, "/runs/in-shard-0.bin");

        // Listing /dataset surfaces only the shard-1 child, under a shard-1 inode.
        let dataset = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPath {
                path: "/dataset".to_owned(),
            },
        );
        let entries = match dataset.result.unwrap() {
            MetadataRpcResult::Dentries { entries } => entries,
            other => panic!("unexpected /dataset readdir result: {other:?}"),
        };
        assert_eq!(entries.len(), 1, "/dataset sees only its own child");
        assert_eq!(entries[0].dentry.name_hex, "696e2d73686172642d312e62696e"); // "in-shard-1.bin"
        assert_eq!(
            InodeId::new(entries[0].attr.inode).unwrap().shard_index(),
            DATASET_INDEX
        );

        // A lookup under /dataset for a shard-0 name must miss: shard 1's
        // namespace has no /dataset/runs.
        let cross = request_envelope(
            &server,
            MetadataRpcRequest::LookupPath {
                path: "/dataset/runs".to_owned(),
            },
        );
        assert!(matches!(
            cross.result.unwrap(),
            MetadataRpcResult::Dentry { entry: None }
        ));

        // Listing the default shard's root sees only the shard-0 directory we
        // created; the /dataset subtree (a different shard's namespace) is not an
        // entry here.
        let root = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPath {
                path: "/".to_owned(),
            },
        );
        let mut root_names: Vec<String> = match root.result.unwrap() {
            MetadataRpcResult::Dentries { entries } => entries
                .iter()
                .map(|entry| entry.dentry.name_hex.clone())
                .collect(),
            other => panic!("unexpected / readdir result: {other:?}"),
        };
        root_names.sort();
        assert_eq!(root_names, vec!["72756e73".to_owned()]); // only "runs"
    }

    /// A batch layout-open routes on its FIRST entry's path: a batch whose first
    /// path is under `/dataset` lands on shard 1, and a batch whose first path is
    /// elsewhere lands on the default shard. The client guarantees each batch it
    /// sends is single-shard, so first-entry routing reaches the right shard.
    #[test]
    fn open_path_read_plan_batch_routes_on_first_entry_path() {
        let (server, _control, _dir) = two_shard_server();

        let dataset_batch = MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: vec![
                WireOpenPathReadPlanRequest {
                    path: "/dataset/sample-0.bin".to_owned(),
                    offset: 0,
                    len: 1,
                    expected_generation: None,
                },
                WireOpenPathReadPlanRequest {
                    path: "/dataset/sample-1.bin".to_owned(),
                    offset: 0,
                    len: 1,
                    expected_generation: None,
                },
            ],
        };
        assert_eq!(
            route_index(&server, &dataset_batch),
            DATASET_INDEX,
            "a batch whose first path is under /dataset routes to shard 1"
        );

        let default_batch = MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: vec![WireOpenPathReadPlanRequest {
                path: "/runs/sample-0.bin".to_owned(),
                offset: 0,
                len: 1,
                expected_generation: None,
            }],
        };
        assert_eq!(
            route_index(&server, &default_batch),
            DEFAULT_SHARD_INDEX,
            "a batch whose first path is outside /dataset routes to the default shard"
        );

        // An empty batch has no addressable key and falls to the default shard.
        let empty_batch = MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: Vec::new(),
        };
        assert_eq!(route_index(&server, &empty_batch), DEFAULT_SHARD_INDEX);
    }

    /// A clone whose source is in shard 1 but whose destination crosses into
    /// shard 0's subtree (`/dataset/...` -> `/other/...`) must NOT silently
    /// corrupt. The op routes on its source path (shard 1) and the single-shard
    /// service resolves the destination inside shard 1's own namespace, where the
    /// `/other` parent does not exist — so it errors with NotFound rather than
    /// writing into shard 0. (Explicit EXDEV fencing is a later phase; this only
    /// confirms today's behavior is "errors", not "silent cross-shard write".)
    #[test]
    fn cross_boundary_clone_errors_instead_of_corrupting() {
        let (server, _control, _dir) = two_shard_server();
        // Source subtree in shard 1.
        create_dir_path(&server, "/dataset");
        create_file_path(&server, "/dataset/a.bin");
        // Destination parent in shard 0.
        create_dir_path(&server, "/other");

        // The clone routes to shard 1 (src path), which has no /other parent.
        assert_eq!(
            route_index(
                &server,
                &MetadataRpcRequest::CloneSubtreePath {
                    src_path: "/dataset".to_owned(),
                    dst_path: "/other/fork".to_owned(),
                }
            ),
            DATASET_INDEX,
            "a cross-boundary clone routes on its source shard"
        );
        let cloned = request_envelope(
            &server,
            MetadataRpcRequest::CloneSubtreePath {
                src_path: "/dataset".to_owned(),
                dst_path: "/other/fork".to_owned(),
            },
        );
        assert!(
            !cloned.ok,
            "a cross-shard clone must fail, not silently corrupt: {cloned:?}"
        );
        assert!(
            matches!(cloned.error_kind, Some(WireMetadataError::NotFound)),
            "cross-boundary clone errors with NotFound (no /other parent in shard 1), got {:?}",
            cloned.error_kind
        );

        // Shard 0's /other is untouched: the failed clone wrote nothing there.
        let other = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPath {
                path: "/other".to_owned(),
            },
        );
        let entries = match other.result.unwrap() {
            MetadataRpcResult::Dentries { entries } => entries,
            other => panic!("unexpected /other readdir result: {other:?}"),
        };
        assert!(
            entries.is_empty(),
            "the failed cross-shard clone must not have written into shard 0: {entries:?}"
        );
    }
}

/// Phase 8 capstone — end-to-end fleet integration.
///
/// Every test here stands up *real* [`Server`]s on localhost `TcpListener`s and
/// drives them through the *real* fleet client (`NoKvFsClient::connect_fleet` /
/// `MetadataClient::fleet`). Nothing is mocked: requests cross TCP, the server
/// routes each one to its owning shard slot, and the client resolves the owning
/// endpoint per request from one shared `InMemoryControlStore`. This welds the
/// whole multi-shard stack — control plane, per-shard servers, and the routing
/// client — together.
///
/// Determinism: every owner uses `with_renewal(None)` so no background renewal
/// worker races the test, and each lease/owner-epoch transition is driven
/// explicitly. To make a fenced old owner reachable (so the client observes a
/// typed handoff error and transparently re-resolves, instead of a bare transport
/// error against a dead socket), the old server is kept alive and made to observe
/// the bumped epoch via an explicit `renew_shard_owner_lease`.
mod fleet_e2e {
    use super::*;
    use crate::options::ServerOptions;
    use nokv_client::{ClientError, MetadataClient, NoKvFsClient};
    use nokv_control::{ControlStore, NodeId};
    use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokv_types::{DentryName, InodeId, MountId, DEFAULT_SHARD_INDEX};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    const DATASET_INDEX: u16 = 1;
    const MODE_DIR: u32 = 0o755;
    const MODE_FILE: u32 = 0o644;

    fn mount() -> MountId {
        MountId::new(1).unwrap()
    }

    /// A real server kept addressable: the `Arc<Server>` is held by the test (so it
    /// can drive explicit lease/checkpoint/log transitions) while a background
    /// accept loop serves connections off a bound `TcpListener`. This is the
    /// in-crate equivalent of `Server::serve`, but it keeps a handle to the server
    /// instead of consuming it.
    struct RunningServer {
        server: Arc<Server>,
        addr: SocketAddr,
        stop: Arc<AtomicBool>,
        accept: Option<thread::JoinHandle<()>>,
    }

    impl RunningServer {
        /// Bind a fresh localhost port, build the server with `build` (handed the
        /// bind addr so it can be used as the owner `NodeId`/endpoint), and start a
        /// background accept loop. The returned addr is what a client connects to.
        fn spawn(build: impl FnOnce(SocketAddr) -> Server) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let server = Arc::new(build(addr));
            let stop = Arc::new(AtomicBool::new(false));
            let worker_server = Arc::clone(&server);
            let worker_stop = Arc::clone(&stop);
            let accept = thread::spawn(move || {
                for stream in listener.incoming() {
                    if worker_stop.load(Ordering::Relaxed) {
                        break;
                    }
                    match stream {
                        Ok(stream) => {
                            let conn_server = Arc::clone(&worker_server);
                            // One connection per client endpoint (the pipelined
                            // client reuses it); serve it on its own thread so the
                            // accept loop stays responsive.
                            thread::spawn(move || {
                                let _ = crate::http::handle_stream(conn_server, stream);
                            });
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                server,
                addr,
                stop,
                accept: Some(accept),
            }
        }

        fn addr(&self) -> SocketAddr {
            self.addr
        }

        fn server(&self) -> &Arc<Server> {
            &self.server
        }

        /// Stop the accept loop and drop the inner server (releasing its owner
        /// leases) by handing ownership to `Drop`.
        fn shutdown(self) {
            drop(self);
        }
    }

    impl Drop for RunningServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            // Unblock the loop's blocked `accept()` so it observes `stop` and exits.
            let _ = std::net::TcpStream::connect(self.addr);
            if let Some(handle) = self.accept.take() {
                let _ = handle.join();
            }
        }
    }

    fn memory_object_store() -> ConfiguredObjectStore {
        ConfiguredObjectStore::Memory(Arc::new(MemoryObjectStore::new()))
    }

    /// A throwaway S3 config placeholder for the `ServerOptions.object` field. The
    /// fleet members are opened via [`Server::open_with_objects`] with one shared
    /// in-memory store, so this config is never actually opened — it just satisfies
    /// the struct.
    fn placeholder_object_config() -> nokv_object::ObjectStoreConfig {
        nokv_object::ObjectStoreConfig::s3(fake_s3_options())
    }

    /// Build one fleet member from injected `objects` (shared across the fleet so a
    /// restoring owner can read another owner's archived metadata). `make_owner`
    /// receives the bound address so the owner's `NodeId` is its own bind addr —
    /// which is exactly what the control record's `endpoint` becomes, so the fleet
    /// client resolves straight back to this member. A unique `meta_root` keeps
    /// each member's Holt engine isolated; the optional `checkpoint_prefix` enables
    /// the per-shard checkpoint archive.
    fn spawn_member(
        meta_root: &Path,
        objects: ConfiguredObjectStore,
        control: Arc<InMemoryControlStore>,
        checkpoint_prefix: Option<&str>,
        make_owner: impl FnOnce(SocketAddr) -> ServerShardOwnerOptions + Send + 'static,
    ) -> RunningServer {
        let meta_root = meta_root.to_path_buf();
        let prefix = checkpoint_prefix.map(ToOwned::to_owned);
        RunningServer::spawn(move |addr| {
            let mut options = fleet_options(&meta_root);
            options.metadata_checkpoint_archive_prefix = prefix;
            Server::open_with_objects(
                options,
                objects,
                Some((control as Arc<dyn ControlStore>, vec![make_owner(addr)])),
            )
            .unwrap()
        })
    }

    /// Options for an in-process fleet member: a unique per-server meta dir, a
    /// placeholder object config (the real store is injected), and workers turned
    /// down so nothing fires mid-test.
    fn fleet_options(meta_root: &Path) -> ServerOptions {
        ServerOptions {
            bind: crate::options::DEFAULT_SERVER_BIND,
            mount: mount(),
            meta_path: meta_root.join("meta"),
            metadata_checkpoint_archive_prefix: None,
            object: placeholder_object_config(),
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
        }
    }

    /// Register the two shard identities used here: the default shard `mount-1:/`
    /// (index 0) and the subtree shard `mount-1:/dataset` (index 1). Identity must
    /// be registered before any owner acquires so each slot reads its index/prefix
    /// from the record.
    fn register_two_shards(control: &InMemoryControlStore) {
        nokv_control::register_shard(control, ShardId::new("mount-1:/"), "/", DEFAULT_SHARD_INDEX)
            .unwrap();
        nokv_control::register_shard(
            control,
            ShardId::new("mount-1:/dataset"),
            "/dataset",
            DATASET_INDEX,
        )
        .unwrap();
    }

    /// Deliverable 1 (core): two real servers, one shared control store, a real
    /// fleet client. Proves cross-shard routing end-to-end —
    ///   1. a dir+file created under `/dataset/...` is served by shard 1 (owned by
    ///      B) and a file under `/other/...` by shard 0 (owned by A), asserted via
    ///      the shard index encoded in each returned inode;
    ///   2. both are read back through the fleet client, which routes each request
    ///      to the right server.
    #[test]
    fn fleet_routes_creates_and_reads_across_two_real_servers() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let object = memory_object_store();
        let control = Arc::new(InMemoryControlStore::new());
        register_two_shards(&control);

        // Server A owns the default shard (index 0, "/"); its NodeId is its bind
        // addr, so the control record's endpoint resolves straight back to it.
        let server_a = spawn_member(
            dir_a.path(),
            object.clone(),
            Arc::clone(&control),
            None,
            |addr| ServerShardOwnerOptions::fresh("mount-1:/", addr.to_string()).with_renewal(None),
        );

        // Server B owns the /dataset shard (index 1).
        let server_b = spawn_member(
            dir_b.path(),
            object.clone(),
            Arc::clone(&control),
            None,
            |addr| {
                ServerShardOwnerOptions::fresh("mount-1:/dataset", addr.to_string())
                    .with_renewal(None)
            },
        );

        // The fleet client resolves the owning endpoint of every request from the
        // shared control store. No address is hard-coded — routing is the point.
        let client = NoKvFsClient::connect_fleet(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            mount(),
            object,
        )
        .unwrap();
        let metadata = client.metadata();

        // (1) A dir + file under /dataset land on shard 1 (server B).
        let dataset_dir = metadata.mkdir("/dataset", MODE_DIR, 1000, 1000).unwrap();
        assert_eq!(
            dataset_dir.attr.inode.shard_index(),
            DATASET_INDEX,
            "/dataset must be served by shard 1 (its inode carries shard index 1)"
        );
        let dataset_file = metadata
            .create_file("/dataset/checkpoint.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        assert_eq!(
            dataset_file.attr.inode.shard_index(),
            DATASET_INDEX,
            "a file under /dataset must be served by shard 1"
        );

        // A file under /other falls to the default shard 0 (server A).
        let other_file = metadata
            .create_file("/other-file.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        assert_eq!(
            other_file.attr.inode.shard_index(),
            DEFAULT_SHARD_INDEX,
            "a file outside /dataset must be served by the default shard 0"
        );

        // (2) Read both back through the fleet client; each lookup routes to the
        // right server, and the inode's shard index confirms which one answered.
        let looked_up_dataset = metadata
            .lookup("/dataset/checkpoint.bin")
            .unwrap()
            .expect("dataset file resolves through the fleet client");
        assert_eq!(looked_up_dataset.attr.inode, dataset_file.attr.inode);
        assert_eq!(
            looked_up_dataset.attr.inode.shard_index(),
            DATASET_INDEX,
            "the dataset lookup was answered by shard 1"
        );

        let looked_up_other = metadata
            .lookup("/other-file.bin")
            .unwrap()
            .expect("other file resolves through the fleet client");
        assert_eq!(looked_up_other.attr.inode, other_file.attr.inode);
        assert_eq!(
            looked_up_other.attr.inode.shard_index(),
            DEFAULT_SHARD_INDEX,
            "the other lookup was answered by shard 0"
        );

        // Cross-shard isolation: listing /dataset (shard 1) sees only its own
        // child, under a shard-1 inode.
        let dataset_entries = metadata.list("/dataset").unwrap();
        assert_eq!(
            dataset_entries.len(),
            1,
            "/dataset has exactly its one child"
        );
        assert_eq!(
            dataset_entries[0].attr.inode, dataset_file.attr.inode,
            "the /dataset listing is the shard-1 file"
        );

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
    }

    /// Deliverable 1 (handoff): the fleet client transparently re-resolves after a
    /// shard's owner moves.
    ///
    /// Sequence (deterministic, no renewal worker):
    ///   - A owns shard 0, B owns shard 1; the client caches B as shard 1's owner.
    ///   - B' failover-acquires shard 1 on a *new* port (epoch 1 -> 2; the control
    ///     record's endpoint is rewritten to B').
    ///   - The old owner B is made to observe the bumped epoch via an explicit
    ///     `renew_shard_owner_lease` (fencing its commits), so a shard-1 request
    ///     hitting the still-cached B returns a typed handoff error.
    ///   - The client's next `/dataset` request hits B, gets `StaleOwnerEpoch`,
    ///     refreshes the shard map from control, re-resolves to B', and succeeds —
    ///     all inside one client call, transparently. With no checkpoint archive
    ///     configured, B' starts shard 1 fresh (the documented "fresh if nothing
    ///     was checkpointed" path); the stronger restore variant is the next test.
    #[test]
    fn fleet_transparently_reresolves_after_owner_handoff() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let dir_b_prime = tempdir().unwrap();
        let object = memory_object_store();
        let control = Arc::new(InMemoryControlStore::new());
        register_two_shards(&control);

        let server_a = spawn_member(
            dir_a.path(),
            object.clone(),
            Arc::clone(&control),
            None,
            |addr| ServerShardOwnerOptions::fresh("mount-1:/", addr.to_string()).with_renewal(None),
        );
        let server_b = spawn_member(
            dir_b.path(),
            object.clone(),
            Arc::clone(&control),
            None,
            |addr| {
                ServerShardOwnerOptions::fresh("mount-1:/dataset", addr.to_string())
                    .with_renewal(None)
            },
        );
        let b_epoch = control
            .get_shard(&ShardId::new("mount-1:/dataset"))
            .unwrap()
            .epoch;

        let client = NoKvFsClient::connect_fleet(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            mount(),
            object.clone(),
        )
        .unwrap();
        let metadata = client.metadata();

        // Seed shard 1 through B and confirm the client routes there.
        let pre = metadata.mkdir("/dataset", MODE_DIR, 1000, 1000).unwrap();
        assert_eq!(pre.attr.inode.shard_index(), DATASET_INDEX);

        // Bring up B' as a failover owner of shard 1 on a brand-new port. This
        // bumps the shard epoch (1 -> 2) and rewrites the control endpoint to B'.
        let server_b_prime = spawn_member(
            dir_b_prime.path(),
            object.clone(),
            Arc::clone(&control),
            None,
            move |addr| {
                ServerShardOwnerOptions::failover("mount-1:/dataset", addr.to_string(), b_epoch)
                    .with_renewal(None)
            },
        );
        assert_ne!(
            server_b_prime.addr(),
            server_b.addr(),
            "B' must bind a different port so the client's cached B endpoint is stale"
        );
        let after_failover = control
            .get_shard(&ShardId::new("mount-1:/dataset"))
            .unwrap();
        assert_eq!(
            after_failover.epoch,
            b_epoch + 1,
            "failover bumped the epoch"
        );
        assert_eq!(
            after_failover.endpoint.as_deref(),
            Some(server_b_prime.addr().to_string().as_str()),
            "control now points shard 1 at B'"
        );

        // Force the old owner B to observe the bumped epoch and fence itself, so a
        // shard-1 request that still lands on B returns a typed handoff error
        // rather than silently serving orphaned state.
        server_b
            .server()
            .renew_shard_owner_lease()
            .expect_err("the fenced old owner must fail to renew its lease");

        // The first client call after the handoff hits cached B -> StaleOwnerEpoch
        // -> refresh from control -> re-resolve to B' -> served there. The
        // re-resolve is invisible to the caller; it just sees a successful op.
        // (B' is a fresh shard with no checkpoint, so its /dataset directory must
        // be recreated before files land under it — that recreate IS the request
        // that drives the transparent re-resolve.)
        let post_dir = metadata.mkdir("/dataset", MODE_DIR, 1000, 1000).unwrap();
        assert_eq!(
            post_dir.attr.inode.shard_index(),
            DATASET_INDEX,
            "the re-resolved /dataset directory is still a shard-1 inode (served by B')"
        );
        let post = metadata
            .create_file("/dataset/after-handoff.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        assert_eq!(
            post.attr.inode.shard_index(),
            DATASET_INDEX,
            "the post-handoff create is still a shard-1 inode"
        );

        // A follow-up read of the just-created file is served by B' too.
        let reread = metadata
            .lookup("/dataset/after-handoff.bin")
            .unwrap()
            .expect("the post-handoff file resolves through B'");
        assert_eq!(reread.attr.inode, post.attr.inode);

        // The default shard was never disturbed by the shard-1 handoff.
        let other = metadata
            .create_file("/still-on-shard-0.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        assert_eq!(other.attr.inode.shard_index(), DEFAULT_SHARD_INDEX);

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
        server_b_prime.shutdown();
    }

    /// Deliverable 1 (handoff, full restore): the strongest proof — a replacement
    /// owner reconstructs the failed shard's namespace from its checkpoint image +
    /// replayed shared log, and the fleet client keeps serving the *same*
    /// pre-handoff data through the new owner. B' is given an EMPTY meta dir, so
    /// the restore must come from the shared object store, exactly as in production
    /// failover.
    #[test]
    fn fleet_handoff_restores_shard_from_checkpoint_and_keeps_serving() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let dir_b_prime = tempdir().unwrap();
        // One shared in-memory object store backs the whole fleet so B' can read
        // the metadata checkpoint + shared-log segments B wrote.
        let object = memory_object_store();
        let control = Arc::new(InMemoryControlStore::new());
        register_two_shards(&control);

        let checkpoint_prefix = "meta/fleet-ck";
        let shared_log = ServerSharedLogOptions::new("meta/fleet-log");

        // Server A: default shard, plain (its data is not under test here).
        let server_a = spawn_member(
            dir_a.path(),
            object.clone(),
            Arc::clone(&control),
            None,
            |addr| ServerShardOwnerOptions::fresh("mount-1:/", addr.to_string()).with_renewal(None),
        );

        // Server B: owns shard 1 with a checkpoint archive + sync shared log.
        let shared_log_b = shared_log.clone();
        let server_b = spawn_member(
            dir_b.path(),
            object.clone(),
            Arc::clone(&control),
            Some(checkpoint_prefix),
            move |addr| {
                ServerShardOwnerOptions::fresh("mount-1:/dataset", addr.to_string())
                    .with_renewal(None)
                    .with_shared_log(Some(shared_log_b))
            },
        );
        let b_epoch = control
            .get_shard(&ShardId::new("mount-1:/dataset"))
            .unwrap()
            .epoch;

        let client = NoKvFsClient::connect_fleet(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            mount(),
            object.clone(),
        )
        .unwrap();
        let metadata = client.metadata();

        // Seed shard 1 with a directory + file BEFORE any checkpoint, then archive
        // a checkpoint through B, then add a second file AFTER the checkpoint so the
        // restore must replay the shared log to recover it.
        metadata.mkdir("/dataset", MODE_DIR, 1000, 1000).unwrap();
        let before_ck = metadata
            .create_file("/dataset/before-ckpt.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        let backup = server_b.server().run_manual_backup().unwrap();
        assert!(
            backup.contains("\"checkpoint_key\""),
            "B must archive a shard-1 checkpoint: {backup}"
        );
        let after_ck = metadata
            .create_file("/dataset/after-ckpt.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        // Publish the latest sync-log ref so the control record carries the segment
        // chain B' needs to replay the post-checkpoint write.
        server_b.server().publish_latest_metadata_log_ref().unwrap();

        // Hand shard 1 off to B' on a new port. B' has an EMPTY meta dir, so it
        // must reconstruct shard 1's namespace from the checkpoint image + log.
        let shared_log_bp = shared_log.clone();
        let server_b_prime = spawn_member(
            dir_b_prime.path(),
            object.clone(),
            Arc::clone(&control),
            Some(checkpoint_prefix),
            move |addr| {
                ServerShardOwnerOptions::failover("mount-1:/dataset", addr.to_string(), b_epoch)
                    .with_renewal(None)
                    .with_shared_log(Some(shared_log_bp))
            },
        );

        // Fence the old owner so the client's cached B endpoint surfaces a handoff.
        server_b
            .server()
            .renew_shard_owner_lease()
            .expect_err("fenced old owner cannot renew");

        // Through the fleet client (which transparently re-resolves to B'), BOTH
        // the pre-checkpoint and post-checkpoint files are still present — proving
        // B' restored the checkpoint image AND replayed the shared log.
        let restored_before = metadata
            .lookup("/dataset/before-ckpt.bin")
            .unwrap()
            .expect("pre-checkpoint file survives the handoff (restored from checkpoint image)");
        assert_eq!(restored_before.attr.inode, before_ck.attr.inode);
        let restored_after = metadata
            .lookup("/dataset/after-ckpt.bin")
            .unwrap()
            .expect("post-checkpoint file survives the handoff (replayed from shared log)");
        assert_eq!(restored_after.attr.inode, after_ck.attr.inode);

        // B' accepts new writes into the restored namespace and serves them.
        let post = metadata
            .create_file("/dataset/post-handoff.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        assert_eq!(post.attr.inode.shard_index(), DATASET_INDEX);
        let listed = metadata.list("/dataset").unwrap();
        let mut names: Vec<String> = listed
            .iter()
            .map(|entry| String::from_utf8_lossy(entry.dentry.name.as_bytes()).into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "after-ckpt.bin".to_owned(),
                "before-ckpt.bin".to_owned(),
                "post-handoff.bin".to_owned(),
            ],
            "the restored shard-1 namespace carries both replayed files plus the new write"
        );

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
        server_b_prime.shutdown();
    }

    /// A fleet client whose control store has *no* owner for the routed shard fails
    /// fast with a clear error instead of hanging — the negative companion to the
    /// transparent re-resolve path.
    #[test]
    fn fleet_request_to_unowned_shard_fails_fast() {
        let control = Arc::new(InMemoryControlStore::new());
        // /dataset identity is registered but never acquired, so it has no endpoint.
        register_two_shards(&control);
        // The default shard needs an owner so building the client succeeds; point
        // it at an address nothing listens on — it is never contacted.
        nokv_control::assign(
            control.as_ref(),
            ShardId::new("mount-1:/"),
            NodeId::new("127.0.0.1:1"),
        )
        .unwrap();

        let client =
            MetadataClient::fleet(Arc::clone(&control) as Arc<dyn ControlStore>, mount()).unwrap();

        // A /dataset request routes to shard 1, which has no endpoint in any
        // refresh: the client exhausts its bounded retries and returns a Protocol
        // error rather than blocking forever.
        let err = client
            .lookup("/dataset/missing")
            .expect_err("a request to an unowned shard must fail, not hang");
        assert!(
            matches!(err, ClientError::Protocol(_)),
            "expected a routing Protocol error, got {err:?}"
        );
    }

    /// Stand up the two-real-server fleet (default shard 0 on A, `/dataset` shard
    /// 1 on B) plus a real fleet client. Returns the client, both servers (kept
    /// alive), and the shared control store.
    fn graft_fleet(
        dir_a: &std::path::Path,
        dir_b: &std::path::Path,
    ) -> (
        NoKvFsClient<ConfiguredObjectStore>,
        RunningServer,
        RunningServer,
        Arc<InMemoryControlStore>,
    ) {
        let object = memory_object_store();
        let control = Arc::new(InMemoryControlStore::new());
        register_two_shards(&control);
        let server_a = spawn_member(dir_a, object.clone(), Arc::clone(&control), None, |addr| {
            ServerShardOwnerOptions::fresh("mount-1:/", addr.to_string()).with_renewal(None)
        });
        let server_b = spawn_member(dir_b, object.clone(), Arc::clone(&control), None, |addr| {
            ServerShardOwnerOptions::fresh("mount-1:/dataset", addr.to_string()).with_renewal(None)
        });
        let client = NoKvFsClient::connect_fleet(
            Arc::clone(&control) as Arc<dyn ControlStore>,
            mount(),
            object,
        )
        .unwrap();
        (client, server_a, server_b, control)
    }

    fn dataset_shard_record(control: &InMemoryControlStore) -> nokv_control::ShardRecord {
        control
            .get_shard(&ShardId::new("mount-1:/dataset"))
            .unwrap()
    }

    /// register_graft records the child subtree-root inode DURABLY in the control
    /// plane (the atomic registration point) and then installs the parent graft
    /// dentry, so a parent-shard lookup of `/dataset` resolves to the shard-1
    /// subtree inode.
    #[test]
    fn fleet_register_graft_records_subtree_root_in_control_plane() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let (client, server_a, server_b, control) = graft_fleet(dir_a.path(), dir_b.path());
        let metadata = client.metadata();

        let graft = metadata
            .register_graft("/dataset", MODE_DIR, 1000, 1000)
            .unwrap();
        assert_eq!(
            graft.attr.inode.shard_index(),
            DATASET_INDEX,
            "the graft points at the shard-1 subtree inode"
        );

        // Durable registration: the subtree shard's control record carries the
        // subtree-root inode.
        let record = dataset_shard_record(&control);
        assert_eq!(
            record.subtree_root_inode,
            Some(graft.attr.inode.get()),
            "register_graft must record the subtree-root inode in the control plane"
        );

        // The PARENT graft dentry specifically: an inode-routed lookup of
        // (root, "dataset") goes to shard 0 and must resolve to the foreign
        // subtree inode. (A path lookup of "/dataset" would route to shard 1 and
        // see the child's OWN local dir, so it cannot test the graft dentry.)
        let dataset = DentryName::new(b"dataset".to_vec()).unwrap();
        let looked_up = metadata
            .lookup_plus(InodeId::root(), dataset)
            .unwrap()
            .expect("the parent shard resolves the graft dentry by inode");
        assert_eq!(looked_up.attr.inode, graft.attr.inode);
        assert_eq!(looked_up.attr.inode.shard_index(), DATASET_INDEX);

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
    }

    /// reconcile_grafts re-creates a graft dentry that the control plane says
    /// should exist but the parent shard is missing (the crash window between the
    /// durable control write and the parent-dentry write).
    #[test]
    fn fleet_reconcile_recreates_missing_graft_dentry() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let (client, server_a, server_b, _control) = graft_fleet(dir_a.path(), dir_b.path());
        let metadata = client.metadata();

        metadata
            .register_graft("/dataset", MODE_DIR, 1000, 1000)
            .unwrap();

        // Simulate the lost parent-dentry write: remove ONLY the parent graft
        // dentry (the control record still says the graft should exist). The
        // parent dentry is inode-routed (root -> shard 0), distinct from the
        // child's own local "/dataset" dir on shard 1.
        let dataset = DentryName::new(b"dataset".to_vec()).unwrap();
        let removed = metadata
            .remove_graft(InodeId::root(), dataset.clone())
            .unwrap();
        assert!(removed.is_some(), "the graft dentry was present to remove");
        assert!(
            metadata
                .lookup_plus(InodeId::root(), dataset.clone())
                .unwrap()
                .is_none(),
            "the parent graft dentry is gone after the simulated lost write"
        );

        // Reconcile heals it from the durable control record.
        let reconciled = metadata.reconcile_grafts().unwrap();
        assert_eq!(reconciled, vec!["/dataset".to_owned()]);
        let healed = metadata
            .lookup_plus(InodeId::root(), dataset)
            .unwrap()
            .expect("reconcile re-created the graft dentry");
        assert_eq!(healed.attr.inode.shard_index(), DATASET_INDEX);

        // Idempotent: a second pass finds nothing missing.
        assert!(metadata.reconcile_grafts().unwrap().is_empty());

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
    }

    /// unregister_graft removes the parent graft dentry, reaps the (empty) child
    /// subtree on its owning shard, and clears the control-plane registration.
    #[test]
    fn fleet_unregister_removes_graft_and_empty_child_subtree() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let (client, server_a, server_b, control) = graft_fleet(dir_a.path(), dir_b.path());
        let metadata = client.metadata();

        let dataset = DentryName::new(b"dataset".to_vec()).unwrap();
        metadata
            .register_graft("/dataset", MODE_DIR, 1000, 1000)
            .unwrap();
        assert!(metadata
            .lookup_plus(InodeId::root(), dataset.clone())
            .unwrap()
            .is_some());

        metadata.unregister_graft("/dataset").unwrap();

        // Parent graft dentry gone (inode-routed to shard 0).
        assert!(
            metadata
                .lookup_plus(InodeId::root(), dataset.clone())
                .unwrap()
                .is_none(),
            "unregister removes the parent graft dentry"
        );
        // Child subtree dir gone too (path-routed to shard 1).
        assert!(
            metadata.lookup("/dataset").unwrap().is_none(),
            "unregister reaps the child subtree root"
        );
        // Control-plane registration cleared.
        assert_eq!(
            dataset_shard_record(&control).subtree_root_inode,
            None,
            "unregister clears the control-plane subtree-root inode"
        );
        // Child subtree reaped: re-registering mints a FRESH subtree inode (the
        // old root dir no longer exists on shard 1).
        let regraft = metadata
            .register_graft("/dataset", MODE_DIR, 1000, 1000)
            .unwrap();
        assert_eq!(regraft.attr.inode.shard_index(), DATASET_INDEX);

        // Idempotent: unregister again on the now-empty re-graft succeeds.
        metadata.unregister_graft("/dataset").unwrap();
        assert!(metadata.lookup("/dataset").unwrap().is_none());

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
    }

    /// unregister_graft of a NON-EMPTY child subtree returns DirectoryNotEmpty
    /// (recursive delete is out of scope) and leaves the graft fully intact: the
    /// parent dentry, the child subtree, and the control record are all restored.
    #[test]
    fn fleet_unregister_nonempty_graft_returns_directory_not_empty() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let (client, server_a, server_b, control) = graft_fleet(dir_a.path(), dir_b.path());
        let metadata = client.metadata();

        let graft = metadata
            .register_graft("/dataset", MODE_DIR, 1000, 1000)
            .unwrap();
        // Put a file INSIDE the child subtree (lands on shard 1).
        let inside = metadata
            .create_file("/dataset/keep.bin", MODE_FILE, 1000, 1000)
            .unwrap();
        assert_eq!(inside.attr.inode.shard_index(), DATASET_INDEX);

        let err = metadata
            .unregister_graft("/dataset")
            .expect_err("a non-empty child subtree must block unregister");
        assert!(
            matches!(
                err,
                ClientError::Metadata(nokv_meta::MetadError::DirectoryNotEmpty)
            ),
            "expected DirectoryNotEmpty, got {err:?}"
        );

        // Fully intact after the refused unregister: the PARENT graft dentry
        // still resolves (inode-routed), the child file survives, and the control
        // record is restored.
        let dataset = DentryName::new(b"dataset".to_vec()).unwrap();
        let still = metadata
            .lookup_plus(InodeId::root(), dataset)
            .unwrap()
            .expect("parent graft dentry survives a refused unregister");
        assert_eq!(still.attr.inode, graft.attr.inode);
        assert!(
            metadata.lookup("/dataset/keep.bin").unwrap().is_some(),
            "the child subtree contents survive"
        );
        assert_eq!(
            dataset_shard_record(&control).subtree_root_inode,
            Some(graft.attr.inode.get()),
            "the control-plane registration is restored on a refused unregister"
        );

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
    }

    /// reconcile_grafts heals the DE-registration side too: an `unregister_graft`
    /// that crashed after clearing the control record (and reaping the child) but
    /// before removing the parent dentry leaves an orphan parent graft dentry
    /// pointing at a reaped subtree. Reconcile removes it because the control
    /// record's `subtree_root_inode` is now None.
    #[test]
    fn fleet_reconcile_removes_orphan_graft_after_deregistration() {
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let (client, server_a, server_b, control) = graft_fleet(dir_a.path(), dir_b.path());
        let metadata = client.metadata();

        let dataset = DentryName::new(b"dataset".to_vec()).unwrap();
        metadata
            .register_graft("/dataset", MODE_DIR, 1000, 1000)
            .unwrap();
        assert!(metadata
            .lookup_plus(InodeId::root(), dataset.clone())
            .unwrap()
            .is_some());

        // Simulate the crash window in unregister_graft: the control record was
        // de-registered (step 1) but the parent graft dentry write (step 3) never
        // happened. The orphan parent dentry still resolves on shard 0.
        control
            .set_subtree_root_inode(&ShardId::new("mount-1:/dataset"), None)
            .unwrap();
        assert!(
            metadata
                .lookup_plus(InodeId::root(), dataset.clone())
                .unwrap()
                .is_some(),
            "the orphan parent graft dentry is still present before reconcile"
        );

        // Reconcile removes the orphan because the control record says None.
        let reconciled = metadata.reconcile_grafts().unwrap();
        assert_eq!(reconciled, vec!["/dataset".to_owned()]);
        assert!(
            metadata
                .lookup_plus(InodeId::root(), dataset.clone())
                .unwrap()
                .is_none(),
            "reconcile removed the orphan parent graft dentry"
        );

        // Idempotent: a second pass finds nothing to heal.
        assert!(metadata.reconcile_grafts().unwrap().is_empty());

        drop(client);
        server_a.shutdown();
        server_b.shutdown();
    }
}
