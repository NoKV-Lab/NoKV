use super::*;
use crate::command::{
    CommandKind, DelimitedScanItem, DelimitedScanRequest, HistoryPruneRequest, MetadataCommand,
    Mutation, PredicateRef, ScanRequest, Value,
};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

fn version(raw: u64) -> Version {
    Version::new(raw).unwrap()
}

fn put_command(key: &[u8], request_id: &[u8], value: &[u8], commit: u64) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::CreateFile,
        read_version: version(commit - 1),
        commit_version: version(commit),
        primary_family: RecordFamily::Dentry,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(value.to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn replace_command(
    key: &[u8],
    request_id: &[u8],
    value: &[u8],
    read: u64,
    commit: u64,
) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::RenameReplace,
        read_version: version(read),
        commit_version: version(commit),
        primary_family: RecordFamily::Dentry,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            predicate: Predicate::Exists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(value.to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn delete_command(key: &[u8], request_id: &[u8], read: u64, commit: u64) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::RemoveFile,
        read_version: version(read),
        commit_version: version(commit),
        primary_family: RecordFamily::Dentry,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            predicate: Predicate::Exists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::Dentry,
            key: key.to_vec(),
            op: MutationOp::Delete,
            value: None,
        }],
        watch: Vec::new(),
    }
}

fn snapshot_pin_command(request_id: &[u8], commit: u64) -> MetadataCommand {
    retention_put_command(RecordFamily::Snapshot, b"snapshot/1", request_id, commit)
}

const FORK_BASE_HOLD_KEY: &[u8] = b"\0\0\0\0\0\0\0\x01fork-base-hold\0restore-operation-1";

fn fork_base_hold_command(request_id: &[u8], commit: u64) -> MetadataCommand {
    retention_put_command(RecordFamily::System, FORK_BASE_HOLD_KEY, request_id, commit)
}

fn retention_put_command(
    family: RecordFamily,
    key: &[u8],
    request_id: &[u8],
    commit: u64,
) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::SnapshotSubtree,
        read_version: version(commit - 1),
        commit_version: version(commit),
        primary_family: family,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family,
            key: key.to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(b"pin".to_vec())),
        }],
        watch: Vec::new(),
    }
}

fn retention_delete_command(
    family: RecordFamily,
    key: &[u8],
    request_id: &[u8],
    read: u64,
    commit: u64,
) -> MetadataCommand {
    MetadataCommand {
        request_id: request_id.to_vec(),
        kind: CommandKind::RetireSnapshot,
        read_version: version(read),
        commit_version: version(commit),
        primary_family: family,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family,
            key: key.to_vec(),
            predicate: Predicate::VersionEquals(version(read)),
        }],
        mutations: vec![Mutation {
            family,
            key: key.to_vec(),
            op: MutationOp::Delete,
            value: None,
        }],
        watch: Vec::new(),
    }
}

#[test]
fn commit_put_then_get_and_scan() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    let scan = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].key, b"dir/a");
}

#[test]
fn scan_start_after_skips_prior_prefix_keys() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/c", b"req-3", b"value-c", 4))
        .unwrap();

    let before = store.metadata_store_stats();
    let scan = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: Some(b"dir/a".to_vec()),
            version: version(4),
            limit: 1,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();

    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].key, b"dir/b");
    let after = store.metadata_store_stats();
    assert_eq!(
        after.scan_key_visited_total - before.scan_key_visited_total,
        1
    );
    assert_eq!(
        after.scan_key_returned_total - before.scan_key_returned_total,
        1
    );
}

#[test]
fn scan_delimited_uses_engine_common_prefix_rollup() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/sub/b", b"req-2", b"value-b", 3))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/sub/c", b"req-3", b"value-c", 4))
        .unwrap();

    let before = store.metadata_store_stats();
    let scan = store
        .scan_delimited(DelimitedScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            delimiter: b'/',
            version: version(4),
            limit: 10,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();

    assert_eq!(
        scan,
        vec![
            DelimitedScanItem::Key(ScanItem {
                key: b"dir/a".to_vec(),
                value: Value(b"value-a".to_vec()),
                version: version(2),
            }),
            DelimitedScanItem::CommonPrefix(b"dir/sub/".to_vec()),
        ]
    );
    let after = store.metadata_store_stats();
    assert_eq!(
        after.scan_key_returned_total - before.scan_key_returned_total,
        2
    );
}

#[test]
fn scan_keys_uses_key_only_range_with_start_after() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/c", b"req-3", b"value-c", 4))
        .unwrap();

    let before = store.metadata_store_stats();
    let keys = store
        .scan_keys(KeyScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: Some(b"dir/a".to_vec()),
            limit: 1,
            purpose: ReadPurpose::UserStrong,
        })
        .unwrap();

    assert_eq!(keys, vec![b"dir/b".to_vec()]);
    let after = store.metadata_store_stats();
    let visited = after.scan_key_visited_total - before.scan_key_visited_total;
    assert!(
            visited <= 2,
            "bounded key scan should stop at the requested entry or one internal cursor step past it, visited {visited}"
        );
    assert_eq!(
        after.scan_key_returned_total - before.scan_key_returned_total,
        1
    );
}

#[test]
fn repeated_key_scan_records_holt_prefix_cache_hit() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
        .unwrap();

    let request = KeyScanRequest {
        family: RecordFamily::Dentry,
        prefix: b"dir/".to_vec(),
        start_after: None,
        limit: 10,
        purpose: ReadPurpose::UserStrong,
    };
    assert_eq!(
        store.scan_keys(request.clone()).unwrap(),
        vec![b"dir/a".to_vec(), b"dir/b".to_vec()]
    );
    let before = store.metadata_store_stats();
    assert_eq!(
        store.scan_keys(request).unwrap(),
        vec![b"dir/a".to_vec(), b"dir/b".to_vec()]
    );
    let after = store.metadata_store_stats();
    assert_eq!(after.scan_cache_hit_total - before.scan_cache_hit_total, 1);
}

#[test]
fn predicate_failure_does_not_apply_any_mutation() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    let failed = store.commit_metadata(put_command(b"dir/a", b"req-2", b"value-b", 3));
    assert_eq!(failed, Err(MetadataError::PredicateFailed));
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
}

#[test]
fn independent_batch_commits_disjoint_commands() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let results = store.commit_independent_batch(&[
        put_command(b"dir/a", b"req-1", b"value-a", 2),
        put_command(b"dir/b", b"req-2", b"value-b", 3),
    ]);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().commit_version, version(2));
    assert_eq!(results[1].as_ref().unwrap().commit_version, version(3));
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/b",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    let stats = store.metadata_store_stats();
    assert_eq!(stats.commit_total, 2);
    assert_eq!(stats.atomic_apply_total, 1);
    assert_eq!(stats.atomic_apply_command_total, 2);
    assert_eq!(stats.atomic_apply_max_batch, 2);
}

#[test]
fn independent_batch_preserves_conflict_result_boundary() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let results = store.commit_independent_batch(&[
        put_command(b"dir/a", b"req-1", b"value-a", 2),
        put_command(b"dir/a", b"req-2", b"value-b", 3),
        put_command(b"dir/b", b"req-3", b"value-c", 4),
    ]);

    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert_eq!(results[1], Err(MetadataError::PredicateFailed));
    assert!(results[2].is_ok());
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/b",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-c".to_vec()))
    );
    let stats = store.metadata_store_stats();
    assert_eq!(stats.commit_total, 2);
    assert_eq!(stats.atomic_apply_total, 2);
    assert_eq!(stats.atomic_apply_command_total, 2);
    assert_eq!(stats.atomic_apply_max_batch, 1);
}

#[test]
fn independent_batch_isolates_snapshot_retention_changes() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    let results = store.commit_independent_batch(&[
        snapshot_pin_command(b"snapshot-1", 3),
        replace_command(b"dir/a", b"req-2", b"value-b", 2, 4),
    ]);

    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    assert!(store.metadata_store_stats().history_write_total > 0);
}

#[test]
fn independent_batch_orders_fork_base_hold_retention_transitions() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    let results = store.commit_independent_batch(&[
        fork_base_hold_command(b"fork-1", 3),
        replace_command(b"dir/a", b"req-2", b"value-b", 2, 4),
    ]);
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        store
            .history_retention
            .active_fork_base_holds
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );

    let history_before_release = store.metadata_store_stats().history_write_total;
    let results = store.commit_independent_batch(&[
        retention_delete_command(
            RecordFamily::System,
            FORK_BASE_HOLD_KEY,
            b"fork-retire",
            3,
            5,
        ),
        replace_command(b"dir/a", b"req-3", b"value-c", 4, 6),
    ]);
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        store
            .history_retention
            .active_fork_base_holds
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        store.metadata_store_stats().history_write_total,
        history_before_release
    );
}

#[test]
fn independent_batch_rejects_a_pin_older_than_an_earlier_writer() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    let results = store.commit_independent_batch(&[
        replace_command(b"dir/a", b"req-2", b"value-b", 2, 4),
        snapshot_pin_command(b"snapshot-stale", 3),
    ]);
    assert!(results[0].is_ok());
    assert_eq!(results[1], Err(MetadataError::PredicateFailed));
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 0);
    assert!(store
        .get(
            RecordFamily::Snapshot,
            b"snapshot/1",
            version(4),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
}

#[test]
fn snapshot_pin_rejects_a_read_version_older_than_an_applied_writer() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();

    let stale_pin = snapshot_pin_command(b"snapshot-stale", 3);
    assert_eq!(
        store.commit_metadata(stale_pin),
        Err(MetadataError::PredicateFailed)
    );
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 0);

    store
        .commit_metadata(snapshot_pin_command(b"snapshot-retry", 5))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-3", b"value-c", 4, 6))
        .unwrap();
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
}

#[test]
fn fork_base_hold_retains_point_reads_and_snapshot_scans() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(fork_base_hold_command(b"fork-1", 3))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: b"dir/".to_vec(),
                start_after: None,
                version: version(2),
                limit: 10,
                purpose: ReadPurpose::Snapshot,
            })
            .unwrap(),
        vec![ScanItem {
            key: b"dir/a".to_vec(),
            value: Value(b"value-a".to_vec()),
            version: version(2),
        }]
    );
}

#[test]
fn ordinary_fork_binding_does_not_hold_history_retention() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(retention_put_command(
            RecordFamily::ForkBinding,
            b"fork-binding/1",
            b"fork-binding",
            3,
        ))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();

    assert_eq!(
        store
            .history_retention
            .active_fork_base_holds
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(store.metadata_store_stats().history_write_total, 0);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        None
    );
}

#[test]
fn retention_counts_change_only_after_successful_commands() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let pin = snapshot_pin_command(b"snapshot-1", 2);
    store.commit_metadata(pin.clone()).unwrap();
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 1);

    assert_eq!(
        store.commit_metadata(pin).unwrap().commit_version,
        version(2)
    );
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 1);
    assert_eq!(
        store.commit_metadata(snapshot_pin_command(b"snapshot-conflict", 3)),
        Err(MetadataError::PredicateFailed)
    );
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 1);

    assert_eq!(
        store.commit_metadata(retention_delete_command(
            RecordFamily::Snapshot,
            b"snapshot/1",
            b"retire-stale",
            3,
            4,
        )),
        Err(MetadataError::PredicateFailed)
    );
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 1);
    store
        .commit_metadata(retention_delete_command(
            RecordFamily::Snapshot,
            b"snapshot/1",
            b"retire-live",
            2,
            5,
        ))
        .unwrap();
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 0);
}

#[test]
fn retention_apply_and_counter_update_share_one_planning_fence() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    let retention_applied = Arc::new(Barrier::new(2));
    let release_retention = Arc::new(Barrier::new(2));
    let hook_applied = Arc::clone(&retention_applied);
    let hook_release = Arc::clone(&release_retention);
    store
        .history_retention
        .set_after_retention_apply_before_state_hook(Arc::new(move || {
            hook_applied.wait();
            hook_release.wait();
        }));
    let pin_store = store.clone();
    let pin_thread =
        thread::spawn(move || pin_store.commit_metadata(snapshot_pin_command(b"snapshot-1", 3)));

    retention_applied.wait();
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 0);
    assert_eq!(
        store
            .get(
                RecordFamily::Snapshot,
                b"snapshot/1",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap(),
        Some(Value(b"pin".to_vec()))
    );

    let writer_at_fence = Arc::new(Barrier::new(2));
    let writer_hook = Arc::clone(&writer_at_fence);
    store
        .history_retention
        .set_before_ordinary_planning_fence_hook(Arc::new(move || {
            writer_hook.wait();
        }));
    let writer_store = store.clone();
    let writer = thread::spawn(move || {
        writer_store.commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
    });
    writer_at_fence.wait();
    release_retention.wait();

    pin_thread.join().unwrap().unwrap();
    writer.join().unwrap().unwrap();
    assert_eq!(store.metadata_store_stats().active_snapshot_pin_total, 1);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        store
            .scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: b"dir/".to_vec(),
                start_after: None,
                version: version(2),
                limit: 10,
                purpose: ReadPurpose::Snapshot,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn checkpoint_image_round_trips_current_history_and_dedupe() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    let replace = store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();

    let image = store.export_checkpoint_image().unwrap();
    let restored = HoltMetadataStore::open_memory().unwrap();
    restored.install_checkpoint_image(&image).unwrap();

    assert_eq!(
        restored
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    assert_eq!(
        restored
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    assert_eq!(
        restored.committed_request_result(b"req-2").unwrap(),
        Some(replace)
    );
    assert_eq!(restored.metadata_store_stats().active_snapshot_pin_total, 1);
}

#[test]
fn checkpoint_install_and_file_reopen_restore_all_retention_state() {
    let source = HoltMetadataStore::open_memory().unwrap();
    source
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 2))
        .unwrap();
    source
        .commit_metadata(fork_base_hold_command(b"fork-1", 3))
        .unwrap();
    let image = source.export_checkpoint_image().unwrap();

    let restored = HoltMetadataStore::open_memory().unwrap();
    restored.install_checkpoint_image(&image).unwrap();
    assert_eq!(restored.metadata_store_stats().active_snapshot_pin_total, 1);
    assert_eq!(
        restored
            .history_retention
            .active_fork_base_holds
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(
        restored
            .history_retention
            .max_applied_commit_version
            .load(Ordering::Acquire),
        0
    );

    let directory = tempdir().unwrap();
    let path = directory.path().join("metadata");
    {
        let store = HoltMetadataStore::open_file(&path).unwrap();
        store
            .commit_metadata(snapshot_pin_command(b"snapshot-1", 2))
            .unwrap();
        store
            .commit_metadata(fork_base_hold_command(b"fork-1", 3))
            .unwrap();
    }
    let reopened = HoltMetadataStore::open_file(path).unwrap();
    assert_eq!(reopened.metadata_store_stats().active_snapshot_pin_total, 1);
    assert_eq!(
        reopened
            .history_retention
            .active_fork_base_holds
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(
        reopened
            .history_retention
            .max_applied_commit_version
            .load(Ordering::Acquire),
        0
    );

    reopened
        .commit_metadata(put_command(b"dir/a", b"post-open-write", b"value-a", 4))
        .unwrap();
    assert_eq!(
        reopened
            .history_retention
            .max_applied_commit_version
            .load(Ordering::Acquire),
        4
    );
    assert_eq!(
        reopened.commit_metadata(retention_put_command(
            RecordFamily::Snapshot,
            b"snapshot/2",
            b"post-open-stale-pin",
            3,
        )),
        Err(MetadataError::PredicateFailed)
    );
}

#[test]
fn checkpoint_image_round_trips_history_candidate_index() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-a", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/a", b"req-delete", 3, 4))
        .unwrap();

    let image = store.export_checkpoint_image().unwrap();
    let restored = HoltMetadataStore::open_memory().unwrap();
    restored.install_checkpoint_image(&image).unwrap();
    let rows = restored
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"dir/a");
    assert!(restored
        .history_key_index_tree()
        .unwrap()
        .get(HISTORY_INDEX_COMPLETE_KEY)
        .unwrap()
        .is_some());
}

#[test]
fn opening_legacy_store_backfills_missing_history_candidate_index() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("metadata");
    {
        let store = HoltMetadataStore::open_file(&path).unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-a", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
            .unwrap();
        store
            .commit_metadata(delete_command(b"dir/a", b"req-delete", 3, 4))
            .unwrap();
        let index = store.history_key_index_tree().unwrap();
        let keys = index
            .range()
            .into_iter()
            .filter_map(|entry| match entry.unwrap() {
                RangeEntry::Key { key, .. } => Some(key),
                _ => None,
            })
            .collect::<Vec<_>>();
        for key in keys {
            index.delete(&key).unwrap();
        }
        let history = store.history_tree().unwrap();
        let (first_key, first_value) = history
            .range()
            .into_iter()
            .find_map(|entry| match entry.unwrap() {
                RangeEntry::Key { key, value, .. } => Some((key, value)),
                _ => None,
            })
            .unwrap();
        let first_index_key = history_index_key_from_record_key(&first_key).unwrap();
        let (first_version, _) = decode_current_value(&first_value).unwrap();
        index
            .put(&first_index_key, &first_version.get().to_be_bytes())
            .unwrap();
        index.put(HISTORY_INDEX_PROGRESS_KEY, &first_key).unwrap();
    }

    let reopened = HoltMetadataStore::open_file(&path).unwrap();
    let rows = reopened
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"dir/a");
    assert!(reopened
        .history_key_index_tree()
        .unwrap()
        .get(HISTORY_INDEX_COMPLETE_KEY)
        .unwrap()
        .is_some());
    assert!(reopened
        .history_key_index_tree()
        .unwrap()
        .get(HISTORY_INDEX_PROGRESS_KEY)
        .unwrap()
        .is_none());
}

#[test]
fn file_backed_commit_is_wal_durable_before_ack() {
    let directory = tempdir().unwrap();
    let store = HoltMetadataStore::open_file(directory.path().join("metadata")).unwrap();

    store
        .commit_metadata(put_command(b"dir/durable", b"req-durable", b"value", 2))
        .unwrap();

    let journal = store.db.stats().journal.unwrap();
    assert_eq!(journal.pending_work, 0);
    assert_eq!(journal.flushed_work, journal.queued_work);
    assert!(journal.syncs > 0);
}

#[test]
fn storage_reclaim_is_idempotent_after_checkpoint() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();

    store.checkpoint().unwrap();
    store.reclaim_unreachable_storage().unwrap();
    store.reclaim_unreachable_storage().unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
}

#[test]
fn checkpoint_image_rejects_malformed_bytes() {
    let store = HoltMetadataStore::open_memory().unwrap();
    assert!(store.install_checkpoint_image(b"not-a-checkpoint").is_err());

    let mut image = store.export_checkpoint_image().unwrap();
    image.push(1);
    assert!(store.install_checkpoint_image(&image).is_err());
}

#[test]
fn deleted_key_is_hidden_latest_but_visible_to_old_version() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(MetadataCommand {
            request_id: b"req-delete".to_vec(),
            kind: CommandKind::RemoveFile,
            read_version: version(3),
            commit_version: version(4),
            primary_family: RecordFamily::Dentry,
            primary_key: b"dir/a".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                op: MutationOp::Delete,
                value: None,
            }],
            watch: Vec::new(),
        })
        .unwrap();

    let before_latest = store.metadata_store_stats();
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        None
    );
    let after_latest = store.metadata_store_stats();
    assert_eq!(
        after_latest.history_lookup_total - before_latest.history_lookup_total,
        0,
        "live current-missing reads should not scan history"
    );
    let before_snapshot = store.metadata_store_stats();
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
    let after_snapshot = store.metadata_store_stats();
    assert_eq!(
        after_snapshot.history_lookup_total - before_snapshot.history_lookup_total,
        1,
        "snapshot reads must retain historical visibility"
    );
}

#[test]
fn snapshot_scan_includes_key_deleted_after_read_version() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/a", b"req-delete", 3, 4))
        .unwrap();

    let rows = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"dir/a");
    assert_eq!(rows[0].value, Value(b"value-a".to_vec()));
}

#[test]
fn snapshot_scan_delete_recreate_returns_one_historical_candidate() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/a", b"req-delete", 3, 4))
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-recreate", b"value-new", 5))
        .unwrap();

    let rows = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"dir/a");
    assert_eq!(rows[0].value, Value(b"value-a".to_vec()));
}

#[test]
fn snapshot_scan_applies_start_after_and_limit_to_visible_rows() {
    let store = HoltMetadataStore::open_memory().unwrap();
    for (key, request, value, commit) in [
        (b"dir/a".as_slice(), b"req-a".as_slice(), b"a".as_slice(), 2),
        (b"dir/b".as_slice(), b"req-b".as_slice(), b"b".as_slice(), 3),
        (b"dir/c".as_slice(), b"req-c".as_slice(), b"c".as_slice(), 4),
    ] {
        store
            .commit_metadata(put_command(key, request, value, commit))
            .unwrap();
    }
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 5))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/b", b"req-delete-b", 5, 6))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/c", b"req-delete-c", 6, 7))
        .unwrap();

    let before = store.metadata_store_stats();
    let rows = store
        .scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: Some(b"dir/a".to_vec()),
            version: version(4),
            limit: 1,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"dir/b");
    let after = store.metadata_store_stats();
    assert_eq!(
        after.scan_key_visited_total - before.scan_key_visited_total,
        1,
        "streaming merge must stop when the visible page is full"
    );
}

#[test]
fn snapshot_delimited_scan_includes_deleted_historical_prefix() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/removed/file", b"req-file", b"value", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/removed/file", b"req-delete", 3, 4))
        .unwrap();

    let rows = store
        .scan_delimited(DelimitedScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            delimiter: b'/',
            version: version(2),
            limit: 10,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();

    assert_eq!(
        rows,
        vec![DelimitedScanItem::CommonPrefix(b"dir/removed/".to_vec())]
    );
}

#[test]
fn snapshot_delimited_scan_pages_after_historical_common_prefix() {
    let store = HoltMetadataStore::open_memory().unwrap();
    for (key, request, commit) in [
        (b"dir/a/file".as_slice(), b"req-a".as_slice(), 2),
        (b"dir/b/file".as_slice(), b"req-b".as_slice(), 3),
    ] {
        store
            .commit_metadata(put_command(key, request, b"value", commit))
            .unwrap();
    }
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 4))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/a/file", b"delete-a", 4, 5))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/b/file", b"delete-b", 5, 6))
        .unwrap();

    let first = store
        .scan_delimited(DelimitedScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: None,
            delimiter: b'/',
            version: version(3),
            limit: 1,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();
    assert_eq!(
        first,
        vec![DelimitedScanItem::CommonPrefix(b"dir/a/".to_vec())]
    );

    let second = store
        .scan_delimited(DelimitedScanRequest {
            family: RecordFamily::Dentry,
            prefix: b"dir/".to_vec(),
            start_after: Some(b"dir/a/".to_vec()),
            delimiter: b'/',
            version: version(3),
            limit: 1,
            purpose: ReadPurpose::Snapshot,
        })
        .unwrap();
    assert_eq!(
        second,
        vec![DelimitedScanItem::CommonPrefix(b"dir/b/".to_vec())]
    );
}

#[test]
fn not_exists_allows_recreate_after_tombstone() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(MetadataCommand {
            request_id: b"req-delete".to_vec(),
            kind: CommandKind::RemoveFile,
            read_version: version(2),
            commit_version: version(3),
            primary_family: RecordFamily::Dentry,
            primary_key: b"dir/a".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: b"dir/a".to_vec(),
                op: MutationOp::Delete,
                value: None,
            }],
            watch: Vec::new(),
        })
        .unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-2", b"value-b", 4))
        .unwrap();

    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
}

#[test]
fn prefix_empty_predicate_uses_family_prefix() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    let mut command = put_command(b"dir", b"req-2", b"directory", 3);
    command.predicates = vec![PredicateRef {
        family: RecordFamily::Dentry,
        key: b"dir/".to_vec(),
        predicate: Predicate::PrefixEmpty,
    }];
    assert_eq!(
        store.commit_metadata(command),
        Err(MetadataError::PredicateFailed)
    );
}

#[test]
fn duplicate_request_id_returns_original_result() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let first = store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    let duplicate = store
        .commit_metadata(put_command(b"dir/b", b"req-1", b"value-b", 3))
        .unwrap();
    assert_eq!(duplicate, first);
    assert!(store
        .get(
            RecordFamily::Dentry,
            b"dir/b",
            version(3),
            ReadPurpose::UserStrong
        )
        .unwrap()
        .is_none());
}

#[test]
fn concurrent_duplicate_request_id_commits_once() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let left_store = store.clone();
    let left_barrier = Arc::clone(&barrier);
    let left = thread::spawn(move || {
        left_barrier.wait();
        left_store.commit_metadata(put_command(b"dir/a", b"req-shared", b"value-a", 2))
    });
    let right_store = store.clone();
    let right = thread::spawn(move || {
        barrier.wait();
        right_store.commit_metadata(put_command(b"dir/b", b"req-shared", b"value-b", 3))
    });

    let left = left.join().unwrap().unwrap();
    let right = right.join().unwrap().unwrap();
    assert_eq!(left, right);

    let a = store
        .get(
            RecordFamily::Dentry,
            b"dir/a",
            version(3),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    let b = store
        .get(
            RecordFamily::Dentry,
            b"dir/b",
            version(3),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    assert_ne!(a.is_some(), b.is_some());
}

#[test]
fn concurrent_not_exists_commits_one_writer() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let left_store = store.clone();
    let left_barrier = Arc::clone(&barrier);
    let left = thread::spawn(move || {
        left_barrier.wait();
        left_store.commit_metadata(put_command(b"dir/a", b"req-left", b"value-a", 2))
    });
    let right_store = store.clone();
    let right = thread::spawn(move || {
        barrier.wait();
        right_store.commit_metadata(put_command(b"dir/a", b"req-right", b"value-b", 3))
    });

    let outcomes = [left.join().unwrap(), right.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(MetadataError::PredicateFailed)))
            .count(),
        1
    );
    assert!(store
        .get(
            RecordFamily::Dentry,
            b"dir/a",
            version(3),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());
}

#[test]
fn hot_path_skips_history_without_snapshot_retention() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 3))
        .unwrap();

    assert_eq!(store.metadata_store_stats().history_write_total, 0);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        None
    );
    let outcome = store
        .prune_history(HistoryPruneRequest {
            retain_from: None,
            retention_epoch: store.history_retention_epoch().unwrap(),
            limit: 100,
        })
        .unwrap();
    assert_eq!(outcome.removed, 0);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
}

#[test]
fn prune_history_keeps_snapshot_floor_anchor_per_key() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-3", b"value-c", 4, 5))
        .unwrap();

    assert_eq!(store.metadata_store_stats().history_write_total, 2);

    let outcome = store
        .prune_history(HistoryPruneRequest {
            retain_from: Some(version(5)),
            retention_epoch: store.history_retention_epoch().unwrap(),
            limit: 100,
        })
        .unwrap();
    assert_eq!(outcome.scanned, 2);
    assert_eq!(outcome.removed, 1);
    assert_eq!(outcome.retained_by_snapshots, 1);
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(4),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        Some(Value(b"value-b".to_vec()))
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(2),
                ReadPurpose::Snapshot
            )
            .unwrap(),
        None
    );
}

#[test]
fn pruning_last_history_record_removes_candidate_index_key() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-a", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(delete_command(b"dir/a", b"req-delete", 3, 4))
        .unwrap();

    let outcome = store
        .prune_history(HistoryPruneRequest {
            retain_from: None,
            retention_epoch: store.history_retention_epoch().unwrap(),
            limit: 100,
        })
        .unwrap();

    assert_eq!(outcome.removed, 2);
    assert!(store
        .history_key_index_tree()
        .unwrap()
        .get(&history_index_key(RecordFamily::Dentry, b"dir/a"))
        .unwrap()
        .is_none());
}

#[test]
fn fork_shadow_history_scan_and_prune_use_candidate_index() {
    let store = HoltMetadataStore::open_memory().unwrap();
    let key = b"shadow/parent/child";
    let put = MetadataCommand {
        request_id: b"fork-shadow-put".to_vec(),
        kind: CommandKind::CreateFile,
        read_version: version(1),
        commit_version: version(2),
        primary_family: RecordFamily::ForkShadow,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::ForkShadow,
            key: key.to_vec(),
            predicate: Predicate::NotExists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::ForkShadow,
            key: key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(b"shadow-value".to_vec())),
        }],
        watch: Vec::new(),
    };
    store.commit_metadata(put).unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"fork-shadow-snapshot", 3))
        .unwrap();
    let delete = MetadataCommand {
        request_id: b"fork-shadow-delete".to_vec(),
        kind: CommandKind::RemoveFile,
        read_version: version(3),
        commit_version: version(4),
        primary_family: RecordFamily::ForkShadow,
        primary_key: key.to_vec(),
        predicates: vec![PredicateRef {
            family: RecordFamily::ForkShadow,
            key: key.to_vec(),
            predicate: Predicate::Exists,
        }],
        mutations: vec![Mutation {
            family: RecordFamily::ForkShadow,
            key: key.to_vec(),
            op: MutationOp::Delete,
            value: None,
        }],
        watch: Vec::new(),
    };
    store.commit_metadata(delete).unwrap();

    assert_eq!(
        store
            .scan(ScanRequest {
                family: RecordFamily::ForkShadow,
                prefix: b"shadow/parent/".to_vec(),
                start_after: None,
                version: version(3),
                limit: 10,
                purpose: ReadPurpose::Snapshot,
            })
            .unwrap()
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect::<Vec<_>>(),
        vec![(key.to_vec(), Value(b"shadow-value".to_vec()))]
    );

    let outcome = store
        .prune_history(HistoryPruneRequest {
            retain_from: None,
            retention_epoch: store.history_retention_epoch().unwrap(),
            limit: 100,
        })
        .unwrap();
    assert_eq!(outcome.removed, 2);
    assert!(store
        .history_key_index_tree()
        .unwrap()
        .get(&history_index_key(RecordFamily::ForkShadow, key))
        .unwrap()
        .is_none());
}

#[test]
fn stale_prune_plan_cannot_remove_history_added_by_concurrent_write() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-a", b"value-a", 2))
        .unwrap();
    store
        .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
        .unwrap();
    store
        .commit_metadata(replace_command(
            b"dir/a",
            b"req-replace-b",
            b"value-b",
            3,
            4,
        ))
        .unwrap();

    let history = store.history_tree().unwrap();
    let index = store.history_key_index_tree().unwrap();
    let records = history
        .range()
        .into_iter()
        .filter_map(|entry| match entry.unwrap() {
            RangeEntry::Key { key, version, .. } => Some((key, version)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let stale = store
        .plan_history_record_deletion(&history, &index, &records)
        .unwrap();

    store
        .commit_metadata(replace_command(
            b"dir/a",
            b"req-replace-c",
            b"value-c",
            4,
            5,
        ))
        .unwrap();
    assert_eq!(
        store.apply_history_record_deletion(&stale),
        Err(MetadataError::PredicateFailed)
    );
    assert_eq!(history.range().into_iter().count(), 2);

    let retried = store
        .prune_history(HistoryPruneRequest {
            retain_from: None,
            retention_epoch: store.history_retention_epoch().unwrap(),
            limit: 100,
        })
        .unwrap();
    assert_eq!(retried.removed, 2);
    assert!(index
        .get(&history_index_key(RecordFamily::Dentry, b"dir/a"))
        .unwrap()
        .is_none());
}

#[test]
fn stale_retention_epoch_cannot_prune_history_needed_by_a_new_snapshot() {
    let store = HoltMetadataStore::open_memory().unwrap();
    store
        .commit_metadata(put_command(b"dir/a", b"req-a", b"value-a", 2))
        .unwrap();
    let stale_epoch = store.history_retention_epoch().unwrap();

    store
        .commit_metadata(snapshot_pin_command(b"snapshot-new", 3))
        .unwrap();
    store
        .commit_metadata(replace_command(b"dir/a", b"req-replace", b"value-b", 3, 4))
        .unwrap();

    assert_eq!(
        store.prune_history(HistoryPruneRequest {
            retain_from: None,
            retention_epoch: stale_epoch,
            limit: 100,
        }),
        Err(MetadataError::PredicateFailed)
    );
    assert_eq!(
        store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        Some(Value(b"value-a".to_vec()))
    );
}
