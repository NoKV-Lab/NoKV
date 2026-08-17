/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use holt::Durability;
use nokv_meta_store::conformance;
use nokv_meta_store::{
    Check, CheckpointCatalogCommitment, CheckpointError, CheckpointFormatId,
    CheckpointInstallState, Commit, FreshStoreCheckpointInstaller, Key, Keyspace, Mutation,
    ReadBatch, ReadOp, ReadResult, Scan, ScanItem, StoreCheckpointEnvelope, StoreError,
    StoreLimits, TxnStore, UnknownCommit, WholeStoreCheckpointSource, WriteTxn,
};
use tempfile::tempdir;

#[cfg(feature = "read-stats")]
use crate::HoltReadStatsSessionError;
use crate::{
    HoltFreshCheckpointInstaller, HoltOptions, HoltRecoveryStagingAdoptionAuthority,
    HoltRecoveryStagingIdentity, HoltStore, TreeBinding,
};

const FIRST: Keyspace = Keyspace::new(1);
const SECOND: Keyspace = Keyspace::new(2);
const CRASH_CHILD_ENV: &str = "NOKV_META_HOLT_CRASH_CHILD";
const CRASH_PATH_ENV: &str = "NOKV_META_HOLT_CRASH_PATH";
const CRASH_EXIT_CODE: i32 = 91;
const CONFORMANCE_KEYSPACES: [Keyspace; 8] = [
    Keyspace::new(0x7f01),
    Keyspace::new(0x7f02),
    Keyspace::new(0x7f03),
    Keyspace::new(0x7f04),
    Keyspace::new(0x7f05),
    Keyspace::new(0x7f06),
    Keyspace::new(0x7f07),
    Keyspace::new(0x7f08),
];

fn limits() -> StoreLimits {
    StoreLimits {
        max_reads: 64,
        max_checks: 64,
        max_mutations: 128,
        max_key_bytes: 256,
        max_value_bytes: 1024,
        max_read_bytes: 2_048,
        max_transaction_bytes: 2_048,
        max_result_rows: 1024,
        max_result_bytes: 1 << 20,
    }
}

fn memory_store(keyspaces: impl IntoIterator<Item = Keyspace>) -> HoltStore {
    HoltStore::memory(catalog(keyspaces), limits()).expect("open memory HoltStore")
}

fn catalog(keyspaces: impl IntoIterator<Item = Keyspace>) -> Vec<TreeBinding> {
    keyspaces
        .into_iter()
        .map(|keyspace| TreeBinding::new(keyspace, format!("test-tree-{:04x}", keyspace.get())))
        .collect()
}

fn put(store: &HoltStore, keyspace: Keyspace, key: &[u8], value: &[u8]) -> Commit {
    store
        .commit(WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: Key::new(keyspace, key),
                value: value.to_vec(),
            }],
        })
        .expect("commit put")
}

fn get(store: &HoltStore, keyspace: Keyspace, key: &[u8]) -> Option<Vec<u8>> {
    let snapshot = store
        .read(ReadBatch {
            ops: vec![ReadOp::Get(Key::new(keyspace, key))],
        })
        .expect("read point key");
    match snapshot.results.into_iter().next().expect("one result") {
        ReadResult::Get(value) => value,
        ReadResult::Scan(_) => panic!("point read returned scan result"),
    }
}

fn assert_invalid_options(options: HoltOptions) {
    assert!(matches!(
        options.validate(false),
        Err(StoreError::InvalidRequest(_))
    ));
}

fn persist_control_finalize_for_test(path: &Path, identity: &HoltRecoveryStagingIdentity) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("create durable control finalize receipt");
    file.write_all(&identity.encode())
        .expect("write durable control finalize receipt");
    file.sync_all()
        .expect("sync durable control finalize receipt");
    drop(file);
    File::open(path.parent().expect("control receipt parent"))
        .and_then(|directory| directory.sync_all())
        .expect("sync control finalize directory");
}

#[test]
fn whole_store_checkpoint_installs_into_one_fresh_target() {
    let source = memory_store([FIRST, SECOND]);
    assert_eq!(put(&source, FIRST, b"first", b"one"), Commit::Applied);
    assert_eq!(put(&source, SECOND, b"second", b"two"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");

    let directory = tempdir().expect("create checkpoint target parent");
    let target = directory.path().join("restored");
    let restored = HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &target,
        catalog([FIRST, SECOND]),
        limits(),
    ))
    .install(&checkpoint)
    .expect("install checkpoint");
    assert!(crate::checkpoint::complete_marker_path(&target).is_file());

    assert_eq!(get(&restored, FIRST, b"first"), Some(b"one".to_vec()));
    assert_eq!(get(&restored, SECOND, b"second"), Some(b"two".to_vec()));
    let reexported = restored.export_checkpoint().expect("re-export checkpoint");
    assert_eq!(reexported, checkpoint);
}

#[test]
fn checkpoint_install_rejects_format_and_catalog_before_creating_target() {
    let source = memory_store([FIRST]);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create target parent");

    let wrong_format = StoreCheckpointEnvelope::new(
        CheckpointFormatId::new("other.checkpoint.v1").unwrap(),
        checkpoint.catalog_commitment(),
        checkpoint.image().to_vec(),
    )
    .unwrap();
    let format_target = directory.path().join("wrong-format");
    let error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &format_target,
        catalog([FIRST]),
        limits(),
    ))
    .install(&wrong_format)
    {
        Ok(_) => panic!("wrong format checkpoint installed"),
        Err(error) => error,
    };
    assert_eq!(error.state(), CheckpointInstallState::Unchanged);
    assert!(!format_target.exists());

    let wrong_catalog = StoreCheckpointEnvelope::new(
        checkpoint.format_id().clone(),
        CheckpointCatalogCommitment::from_bytes([0x55; 32]),
        checkpoint.image().to_vec(),
    )
    .unwrap();
    let catalog_target = directory.path().join("wrong-catalog");
    let error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &catalog_target,
        catalog([FIRST]),
        limits(),
    ))
    .install(&wrong_catalog)
    {
        Ok(_) => panic!("wrong catalog checkpoint installed"),
        Err(error) => error,
    };
    assert_eq!(error.state(), CheckpointInstallState::Unchanged);
    assert!(!catalog_target.exists());
}

#[test]
fn catalog_commitment_binds_keyspace_to_physical_tree_mapping() {
    let first = BTreeMap::from([(FIRST, "tree-a".to_owned()), (SECOND, "tree-b".to_owned())]);
    let swapped = BTreeMap::from([(FIRST, "tree-b".to_owned()), (SECOND, "tree-a".to_owned())]);

    assert_ne!(
        crate::checkpoint::catalog_commitment(&first),
        crate::checkpoint::catalog_commitment(&swapped)
    );
}

#[test]
fn checkpoint_validation_enforces_the_holt_record_budget() {
    let source = memory_store([FIRST]);
    assert_eq!(put(&source, FIRST, b"a", b"one"), Commit::Applied);
    assert_eq!(put(&source, FIRST, b"b", b"two"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let options = HoltOptions::file("unused", catalog([FIRST]), limits());
    let trees = options.validate(false).expect("validate test catalog");

    let error = crate::checkpoint::validate_holt_checkpoint_image_with_record_limit(
        checkpoint.image(),
        &trees,
        &options.limits,
        1,
    )
    .expect_err("second record must cross the Holt-aligned checkpoint budget");
    assert!(error.contains("record"));
}

#[test]
fn malformed_or_foreign_holt_image_is_rejected_before_target_creation() {
    let source = memory_store([FIRST, SECOND]);
    assert_eq!(put(&source, FIRST, b"key", b"value"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create target parent");

    let mut wrong_count = checkpoint.image().to_vec();
    wrong_count[8..12].copy_from_slice(&3_u32.to_le_bytes());
    let wrong_count = StoreCheckpointEnvelope::new(
        checkpoint.format_id().clone(),
        checkpoint.catalog_commitment(),
        wrong_count,
    )
    .unwrap();
    let count_target = directory.path().join("wrong-count");
    let count_error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &count_target,
        catalog([FIRST, SECOND]),
        limits(),
    ))
    .install(&wrong_count)
    {
        Ok(_) => panic!("wrong family count installed"),
        Err(error) => error,
    };
    assert_eq!(count_error.state(), CheckpointInstallState::Unchanged);
    assert!(!count_target.exists());

    let mut foreign_family = checkpoint.image().to_vec();
    let name_len = u32::from_le_bytes(foreign_family[12..16].try_into().unwrap()) as usize;
    let last_name_byte = 16 + name_len - 1;
    foreign_family[last_name_byte] ^= 1;
    let foreign_family = StoreCheckpointEnvelope::new(
        checkpoint.format_id().clone(),
        checkpoint.catalog_commitment(),
        foreign_family,
    )
    .unwrap();
    let family_target = directory.path().join("foreign-family");
    let family_error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &family_target,
        catalog([FIRST, SECOND]),
        limits(),
    ))
    .install(&foreign_family)
    {
        Ok(_) => panic!("foreign family installed"),
        Err(error) => error,
    };
    assert_eq!(family_error.state(), CheckpointInstallState::Unchanged);
    assert!(!family_target.exists());
}

#[test]
fn partial_install_leaves_durable_sentinel_and_regular_open_refuses_full_catalog() {
    let source = memory_store([FIRST, SECOND]);
    assert_eq!(put(&source, FIRST, b"source", b"complete"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create target parent");
    let target = directory.path().join("partial");
    crate::checkpoint::fail_install_after_partial_apply_for_test(target.clone());

    let error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &target,
        catalog([FIRST, SECOND]),
        limits(),
    ))
    .install(&checkpoint)
    {
        Ok(_) => panic!("partial checkpoint install unexpectedly succeeded"),
        Err(error) => error,
    };

    assert_eq!(error.state(), CheckpointInstallState::Poisoned);
    assert!(crate::checkpoint::install_sentinel_path(&target).is_file());
    assert!(matches!(
        HoltStore::open(HoltOptions::file(
            &target,
            catalog([FIRST, SECOND]),
            limits(),
        )),
        Err(StoreError::Corrupt(reason)) if reason.contains("incomplete checkpoint installation marker")
    ));
}

#[test]
fn finalize_unlink_sync_and_sentinel_restore_failure_remains_restart_visible() {
    let source = memory_store([FIRST]);
    assert_eq!(put(&source, FIRST, b"source", b"complete"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create target parent");
    let target = directory.path().join("finalize-uncertain");
    crate::checkpoint::fail_finalize_after_unlink_and_restore_for_test(target.clone());

    let error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &target,
        catalog([FIRST]),
        limits(),
    ))
    .install(&checkpoint)
    {
        Ok(_) => panic!("uncertain checkpoint finalize unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.state(), CheckpointInstallState::Poisoned);
    assert!(
        !crate::checkpoint::install_sentinel_path(&target).exists(),
        "fault model removes the legacy installing sentinel"
    );
    assert!(
        crate::checkpoint::complete_marker_path(&target).is_file(),
        "verified checkpoint identity must remain durable before installing unlink"
    );
    assert!(matches!(
        HoltStore::open(HoltOptions::file(&target, catalog([FIRST]), limits())),
        Err(StoreError::Corrupt(reason)) if reason.contains("checkpoint")
    ));
    let reconciled =
        HoltFreshCheckpointInstaller::new(HoltOptions::file(&target, catalog([FIRST]), limits()))
            .install(&checkpoint)
            .expect("reconcile exact completed checkpoint identity");
    assert_eq!(
        get(&reconciled, FIRST, b"source"),
        Some(b"complete".to_vec())
    );
}

#[test]
fn ready_and_open_fail_closed_if_install_sentinel_appears() {
    let directory = tempdir().expect("create HoltStore parent");
    let target = directory.path().join("live");
    let store = HoltStore::initialize(HoltOptions::file(&target, catalog([FIRST]), limits()))
        .expect("initialize HoltStore");
    let sentinel = crate::checkpoint::install_sentinel_path(&target);
    std::fs::write(&sentinel, b"incomplete").expect("write sentinel");

    assert!(matches!(store.ready(), Err(StoreError::Corrupt(_))));
    assert!(matches!(
        HoltStore::open(HoltOptions::file(&target, catalog([FIRST]), limits())),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn checkpoint_export_releases_state_lock_and_rechecks_poison() {
    let store = Arc::new(memory_store([FIRST]));
    assert_eq!(put(&store, FIRST, b"seed", b"value"), Commit::Applied);

    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    store.pause_next_checkpoint_export_after_clone(entered_tx, resume_rx);
    let exporting_store = Arc::clone(&store);
    let export = thread::spawn(move || exporting_store.export_checkpoint());
    entered_rx
        .recv()
        .expect("checkpoint export did not release its state guard");
    assert_eq!(
        put(&store, FIRST, b"concurrent", b"commit"),
        Commit::Applied
    );
    resume_tx.send(()).expect("resume checkpoint export");
    assert!(export.join().expect("join checkpoint export").is_ok());

    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    store.pause_next_checkpoint_export_after_clone(entered_tx, resume_rx);
    let exporting_store = Arc::clone(&store);
    let export = thread::spawn(move || exporting_store.export_checkpoint());
    entered_rx
        .recv()
        .expect("second checkpoint export did not release its state guard");
    store.fail_next_atomic_after_apply();
    assert!(matches!(
        store.commit(WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: Key::new(FIRST, b"poison"),
                value: b"applied".to_vec(),
            }],
        }),
        Err(StoreError::OutcomeUnknown {
            state: UnknownCommit::Poisoned,
            ..
        })
    ));
    resume_tx.send(()).expect("resume poisoned export");
    assert!(matches!(
        export.join().expect("join poisoned export"),
        Err(CheckpointError::Unavailable(_))
    ));
}

#[test]
fn installed_checkpoint_target_only_reconciles_the_exact_completed_identity() {
    let source = memory_store([FIRST]);
    assert_eq!(put(&source, FIRST, b"identity", b"first"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create target parent");
    let target = directory.path().join("single-use");
    let restored =
        HoltFreshCheckpointInstaller::new(HoltOptions::file(&target, catalog([FIRST]), limits()))
            .install(&checkpoint)
            .expect("first install");
    drop(restored);

    assert!(matches!(
        HoltStore::open(HoltOptions::file(&target, catalog([FIRST]), limits())),
        Err(StoreError::Corrupt(reason)) if reason.contains("checkpoint")
    ));
    let reconciled =
        HoltFreshCheckpointInstaller::new(HoltOptions::file(&target, catalog([FIRST]), limits()))
            .install(&checkpoint)
            .expect("same durable install intent must reconcile after restart");
    assert_eq!(
        get(&reconciled, FIRST, b"identity"),
        Some(b"first".to_vec())
    );
    drop(reconciled);

    let foreign_source = memory_store([FIRST]);
    assert_eq!(
        put(&foreign_source, FIRST, b"identity", b"foreign"),
        Commit::Applied
    );
    let foreign = foreign_source
        .export_checkpoint()
        .expect("export foreign checkpoint");
    let error = match HoltFreshCheckpointInstaller::new(HoltOptions::file(
        &target,
        catalog([FIRST]),
        limits(),
    ))
    .install(&foreign)
    {
        Ok(_) => panic!("foreign checkpoint identity claimed completed target"),
        Err(error) => error,
    };
    assert_eq!(error.state(), CheckpointInstallState::Poisoned);
}

#[test]
fn installed_store_fails_closed_if_its_completed_identity_marker_is_tampered() {
    let source = memory_store([FIRST]);
    assert_eq!(put(&source, FIRST, b"identity", b"value"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create checkpoint target parent");
    let target = directory.path().join("tampered-complete-marker");
    let restored =
        HoltFreshCheckpointInstaller::new(HoltOptions::file(&target, catalog([FIRST]), limits()))
            .install(&checkpoint)
            .expect("install checkpoint");

    std::fs::write(
        crate::checkpoint::complete_marker_path(&target),
        b"tampered",
    )
    .expect("tamper completed checkpoint marker");
    assert!(matches!(restored.ready(), Err(StoreError::Corrupt(_))));
}

#[test]
fn recovery_staging_reopens_after_pre_control_replay_without_the_checkpoint_image() {
    let source = memory_store([FIRST]);
    assert_eq!(put(&source, FIRST, b"base", b"checkpoint"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create recovery staging parent");
    let target = directory.path().join("recovery-staging");
    let options = HoltOptions::file(&target, catalog([FIRST]), limits());
    let installer = HoltFreshCheckpointInstaller::new(options.clone());
    let identity = installer
        .recovery_staging_identity(&checkpoint)
        .expect("derive exact staging identity before installation");
    let persisted_identity = identity.encode();
    let staging = installer.install(&checkpoint).expect("install checkpoint");
    assert_eq!(
        put(&staging, FIRST, b"pre-control-log", b"replayed"),
        Commit::Applied
    );
    drop(staging);
    drop(checkpoint);

    assert!(matches!(
        HoltStore::open(options.clone()),
        Err(StoreError::Corrupt(reason)) if reason.contains("checkpoint")
    ));

    let foreign_source = memory_store([FIRST]);
    assert_eq!(
        put(&foreign_source, FIRST, b"base", b"foreign"),
        Commit::Applied
    );
    let foreign_checkpoint = foreign_source
        .export_checkpoint()
        .expect("export foreign checkpoint");
    let foreign_identity = HoltFreshCheckpointInstaller::new(options.clone())
        .recovery_staging_identity(&foreign_checkpoint)
        .expect("derive foreign staging identity");
    assert!(matches!(
        HoltStore::open_recovery_staging(options.clone(), &foreign_identity),
        Err(StoreError::Corrupt(_))
    ));

    let identity = HoltRecoveryStagingIdentity::decode(&persisted_identity)
        .expect("decode persisted staging identity");
    let reopened = HoltStore::open_recovery_staging(options, &identity)
        .expect("open exact recovery staging without original image");
    assert_eq!(
        get(&reopened, FIRST, b"pre-control-log"),
        Some(b"replayed".to_vec())
    );
}

#[test]
fn recovery_staging_open_rejects_a_tampered_complete_identity_marker() {
    let source = memory_store([FIRST]);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create staging parent");
    let target = directory.path().join("tampered-staging");
    let options = HoltOptions::file(&target, catalog([FIRST]), limits());
    let installer = HoltFreshCheckpointInstaller::new(options.clone());
    let identity = installer
        .recovery_staging_identity(&checkpoint)
        .expect("derive staging identity");
    drop(installer.install(&checkpoint).expect("install checkpoint"));
    std::fs::write(
        crate::checkpoint::complete_marker_path(&target),
        b"tampered",
    )
    .expect("tamper complete marker");

    assert!(matches!(
        HoltStore::open_recovery_staging(options, &identity),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn durable_control_finalize_precedes_staging_adoption_and_ordinary_reopen() {
    let source = memory_store([FIRST]);
    assert_eq!(put(&source, FIRST, b"base", b"checkpoint"), Commit::Applied);
    let checkpoint = source.export_checkpoint().expect("export checkpoint");
    let directory = tempdir().expect("create adoption parent");
    let target = directory.path().join("adopt-staging");
    let options = HoltOptions::file(&target, catalog([FIRST]), limits());
    let installer = HoltFreshCheckpointInstaller::new(options.clone());
    let identity = installer
        .recovery_staging_identity(&checkpoint)
        .expect("derive staging identity");
    let persisted_identity = identity.encode();
    let staging = installer.install(&checkpoint).expect("install checkpoint");
    assert_eq!(
        put(&staging, FIRST, b"pre-control-log", b"replayed"),
        Commit::Applied
    );
    drop(staging);
    drop(checkpoint);

    assert!(matches!(
        HoltStore::open(options.clone()),
        Err(StoreError::Corrupt(_))
    ));
    let foreign_source = memory_store([FIRST]);
    assert_eq!(
        put(&foreign_source, FIRST, b"base", b"foreign"),
        Commit::Applied
    );
    let foreign_checkpoint = foreign_source
        .export_checkpoint()
        .expect("export foreign checkpoint");
    let foreign_identity = HoltFreshCheckpointInstaller::new(options.clone())
        .recovery_staging_identity(&foreign_checkpoint)
        .expect("derive foreign staging identity");
    persist_control_finalize_for_test(
        &directory.path().join("foreign-control-finalized"),
        &foreign_identity,
    );
    assert!(matches!(
        HoltStore::adopt_recovery_staging(
            options.clone(),
            HoltRecoveryStagingAdoptionAuthority::after_durable_control_finalize(foreign_identity,),
        ),
        Err(StoreError::Corrupt(_))
    ));
    assert!(crate::checkpoint::complete_marker_path(&target).is_file());

    let identity = HoltRecoveryStagingIdentity::decode(&persisted_identity)
        .expect("decode persisted staging identity after simulated restart");
    let control_receipt = directory.path().join("control-finalized");
    persist_control_finalize_for_test(&control_receipt, &identity);
    let authority =
        HoltRecoveryStagingAdoptionAuthority::after_durable_control_finalize(identity.clone());
    let adopted = HoltStore::adopt_recovery_staging(options.clone(), authority)
        .expect("adopt only after durable control finalize");
    assert_eq!(
        get(&adopted, FIRST, b"pre-control-log"),
        Some(b"replayed".to_vec())
    );
    assert_eq!(
        put(&adopted, FIRST, b"post-adoption-log", b"applied"),
        Commit::Applied
    );
    drop(adopted);

    let ordinary = HoltStore::open(options).expect("ordinary open after durable adoption");
    assert_eq!(
        get(&ordinary, FIRST, b"pre-control-log"),
        Some(b"replayed".to_vec())
    );
    assert_eq!(
        get(&ordinary, FIRST, b"post-adoption-log"),
        Some(b"applied".to_vec())
    );
}

#[test]
fn staging_adoption_rename_and_sync_failures_are_exactly_retryable() {
    for fail_after_rename in [false, true] {
        let source = memory_store([FIRST]);
        let checkpoint = source.export_checkpoint().expect("export checkpoint");
        let directory = tempdir().expect("create adoption failure parent");
        let target = directory.path().join(if fail_after_rename {
            "fail-after-rename"
        } else {
            "fail-before-rename"
        });
        let options = HoltOptions::file(&target, catalog([FIRST]), limits());
        let installer = HoltFreshCheckpointInstaller::new(options.clone());
        let identity = installer
            .recovery_staging_identity(&checkpoint)
            .expect("derive staging identity");
        let staging = installer.install(&checkpoint).expect("install checkpoint");
        assert_eq!(
            put(&staging, FIRST, b"pre-control-log", b"replayed"),
            Commit::Applied
        );
        drop(staging);
        persist_control_finalize_for_test(&directory.path().join("control-finalized"), &identity);
        if fail_after_rename {
            crate::checkpoint::fail_next_adoption_after_rename_for_test(target.clone());
        } else {
            crate::checkpoint::fail_next_adoption_before_rename_for_test(target.clone());
        }

        let error = match HoltStore::adopt_recovery_staging(
            options.clone(),
            HoltRecoveryStagingAdoptionAuthority::after_durable_control_finalize(identity.clone()),
        ) {
            Ok(_) => panic!("injected adoption failure unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::Unavailable(_)));
        if !fail_after_rename {
            assert!(matches!(
                HoltStore::open(options.clone()),
                Err(StoreError::Corrupt(_))
            ));
        }

        let adopted = HoltStore::adopt_recovery_staging(
            options.clone(),
            HoltRecoveryStagingAdoptionAuthority::after_durable_control_finalize(identity),
        )
        .expect("retry exact adoption identity");
        assert_eq!(
            get(&adopted, FIRST, b"pre-control-log"),
            Some(b"replayed".to_vec())
        );
        drop(adopted);
        let reopened = HoltStore::open(options).expect("ordinary open after adoption retry");
        assert_eq!(
            get(&reopened, FIRST, b"pre-control-log"),
            Some(b"replayed".to_vec())
        );
        drop(reopened);
        if fail_after_rename {
            let adopted_marker = crate::checkpoint::adopted_marker_path(&target);
            let mut tampered = std::fs::read(&adopted_marker).expect("read adopted marker");
            let last = tampered.last_mut().expect("nonempty adopted marker");
            *last ^= 1;
            std::fs::write(&adopted_marker, tampered).expect("tamper adopted marker");
            assert!(matches!(
                HoltStore::open(HoltOptions::file(&target, catalog([FIRST]), limits())),
                Err(StoreError::Corrupt(_))
            ));
        }
    }
}

#[test]
fn memory_store_applies_atomic_multi_keyspace_writes_and_checks() {
    let store = memory_store([FIRST, SECOND]);
    assert_eq!(put(&store, FIRST, b"guard", b"v1"), Commit::Applied);
    assert_eq!(put(&store, SECOND, b"keep", b"old"), Commit::Applied);

    assert_eq!(
        store
            .commit(WriteTxn {
                checks: vec![
                    Check::Value {
                        key: Key::new(FIRST, b"guard"),
                        expected: b"v1".to_vec(),
                    },
                    Check::Absent {
                        key: Key::new(FIRST, b"new"),
                    },
                    Check::EmptyPrefix {
                        keyspace: SECOND,
                        prefix: b"free/".to_vec(),
                    },
                ],
                mutations: vec![
                    Mutation::Put {
                        key: Key::new(FIRST, b"new"),
                        value: b"created".to_vec(),
                    },
                    Mutation::Put {
                        key: Key::new(SECOND, b"free/row"),
                        value: b"published".to_vec(),
                    },
                ],
            })
            .expect("apply checked transaction"),
        Commit::Applied
    );
    assert_eq!(get(&store, FIRST, b"new"), Some(b"created".to_vec()));
    assert_eq!(
        get(&store, SECOND, b"free/row"),
        Some(b"published".to_vec())
    );

    assert_eq!(
        store
            .commit(WriteTxn {
                checks: vec![Check::Absent {
                    key: Key::new(FIRST, b"guard"),
                }],
                mutations: vec![
                    Mutation::Delete {
                        key: Key::new(SECOND, b"keep"),
                    },
                    Mutation::Put {
                        key: Key::new(FIRST, b"bad"),
                        value: b"bad".to_vec(),
                    },
                ],
            })
            .expect("false check returns a conflict"),
        Commit::Conflict
    );
    assert_eq!(get(&store, SECOND, b"keep"), Some(b"old".to_vec()));
    assert_eq!(get(&store, FIRST, b"bad"), None);
}

#[test]
fn healthy_reads_can_share_one_store() {
    let store = Arc::new(memory_store([FIRST]));
    assert_eq!(put(&store, FIRST, b"key", b"value"), Commit::Applied);

    let (first_entered_tx, first_entered_rx) = mpsc::sync_channel(0);
    let (resume_first_tx, resume_first_rx) = mpsc::sync_channel(0);
    store.pause_next_read_after_lock(first_entered_tx, resume_first_rx);
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || get(&first_store, FIRST, b"key"));
    first_entered_rx
        .recv()
        .expect("first read did not acquire the shared state");

    let (second_done_tx, second_done_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second = thread::spawn(move || {
        let value = get(&second_store, FIRST, b"key");
        second_done_tx
            .send(value)
            .expect("report second read result");
    });
    assert_eq!(
        second_done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("second read waited for an unrelated healthy read"),
        Some(b"value".to_vec())
    );

    resume_first_tx.send(()).expect("resume first read");
    assert_eq!(
        first.join().expect("first read thread panicked"),
        Some(b"value".to_vec())
    );
    second.join().expect("second read thread panicked");
}

#[cfg(feature = "read-stats")]
#[test]
fn read_stats_session_reports_adapter_cursor_work() {
    let store = memory_store([FIRST]);
    for key in [b"p/a".as_slice(), b"p/b".as_slice()] {
        assert_eq!(put(&store, FIRST, key, b"value"), Commit::Applied);
    }

    let session = store.begin_read_stats_session().expect("start read stats");
    let snapshot = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"p/".to_vec(),
                after: None,
                limit: 1,
                max_bytes: 1280,
                delimiter: None,
            })],
        })
        .expect("read one bounded page");
    let ReadResult::Scan(page) = &snapshot.results[0] else {
        panic!("scan returned a point result");
    };
    assert_eq!(page.items.len(), 1);
    assert!(page.more);

    let stats = session.finish().expect("finish read stats");
    assert_eq!(stats.scan_cursors, 1);
    assert!(stats.scan_visited_units >= stats.scan_returned_keys);
    assert!(stats.scan_returned_keys >= 2);
}

#[cfg(feature = "read-stats")]
#[test]
fn read_stats_session_is_store_exclusive_and_thread_bound() {
    let first = memory_store([FIRST]);
    let second = memory_store([FIRST]);

    let active = first
        .begin_read_stats_session()
        .expect("start first session");
    let same_store_error = match first.begin_read_stats_session() {
        Err(error) => error,
        Ok(_) => panic!("second session started for the same store"),
    };
    assert_eq!(
        same_store_error,
        HoltReadStatsSessionError::StoreSessionAlreadyActive
    );
    let same_thread_error = match second.begin_read_stats_session() {
        Err(error) => error,
        Ok(_) => panic!("nested session started on the same thread"),
    };
    assert_eq!(
        same_thread_error,
        HoltReadStatsSessionError::ThreadSessionAlreadyActive
    );
    drop(active);

    second
        .begin_read_stats_session()
        .expect("dropped session released the thread slot")
        .finish()
        .expect("finish replacement session");
}

#[cfg(feature = "read-stats")]
#[test]
fn reopened_file_session_reports_rollups_and_storage_activity() {
    let directory = tempdir().expect("create HoltStore directory");
    let path = directory.path().join("meta");
    {
        let store = HoltStore::initialize(HoltOptions::file(&path, catalog([FIRST]), limits()))
            .expect("initialize file HoltStore");
        for key in [b"p/a/1".as_slice(), b"p/a/2".as_slice(), b"p/b".as_slice()] {
            assert_eq!(put(&store, FIRST, key, b"value"), Commit::Applied);
        }
    }

    let store = HoltStore::open(HoltOptions::file(&path, catalog([FIRST]), limits()))
        .expect("reopen file HoltStore");
    let session = store.begin_read_stats_session().expect("start read stats");
    store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"p/".to_vec(),
                after: None,
                limit: 2,
                max_bytes: 1280,
                delimiter: Some(b'/'),
            })],
        })
        .expect("read delimiter page");
    let stats = session.finish().expect("finish read stats");

    assert_eq!(stats.scan_cursors, 1);
    assert!(stats.scan_common_prefixes >= 1);
    assert!(
        stats.cache_hits
            + stats.cache_misses
            + stats.full_blob_reads
            + stats.read_page_hits
            + stats.read_page_misses
            + stats.read_index_cache_hits
            + stats.read_index_cache_misses
            > 0,
        "file-backed session did not report Holt read activity"
    );
}

#[test]
fn scans_stop_on_rows_bytes_and_delimiter_boundaries() {
    let store = memory_store([FIRST]);
    for (key, value) in [
        (b"p/a/1".as_slice(), b"one".as_slice()),
        (b"p/a/2".as_slice(), b"two".as_slice()),
        (b"p/b".as_slice(), b"bee".as_slice()),
        (b"p/c".as_slice(), b"see".as_slice()),
    ] {
        assert_eq!(put(&store, FIRST, key, value), Commit::Applied);
    }

    let first = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"p/".to_vec(),
                after: None,
                limit: 2,
                max_bytes: 1280,
                delimiter: Some(b'/'),
            })],
        })
        .expect("read first delimiter page");
    let ReadResult::Scan(first) = &first.results[0] else {
        panic!("scan returned a point result");
    };
    assert_eq!(
        first.items,
        vec![
            ScanItem::CommonPrefix(b"p/a/".to_vec()),
            ScanItem::Row {
                key: b"p/b".to_vec(),
                value: b"bee".to_vec(),
            },
        ]
    );
    assert!(first.more);

    let second = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"p/".to_vec(),
                after: Some(b"p/b".to_vec()),
                limit: 2,
                max_bytes: 1280,
                delimiter: Some(b'/'),
            })],
        })
        .expect("read second delimiter page");
    let ReadResult::Scan(second) = &second.results[0] else {
        panic!("scan returned a point result");
    };
    assert_eq!(
        second.items,
        vec![ScanItem::Row {
            key: b"p/c".to_vec(),
            value: b"see".to_vec(),
        }]
    );
    assert!(!second.more);

    for key in [b"q/a".as_slice(), b"q/b".as_slice()] {
        assert_eq!(put(&store, FIRST, key, &[b'x'; 700]), Commit::Applied);
    }
    let byte_page = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"q/".to_vec(),
                after: None,
                limit: 2,
                max_bytes: 1280,
                delimiter: None,
            })],
        })
        .expect("read byte-bounded page");
    let ReadResult::Scan(byte_page) = &byte_page.results[0] else {
        panic!("scan returned a point result");
    };
    assert_eq!(byte_page.items.len(), 1);
    assert!(byte_page.more);

    for key in [b"r/a/1".as_slice(), b"r/a/2".as_slice()] {
        assert_eq!(put(&store, FIRST, key, b"v"), Commit::Applied);
    }
    let folded_eof = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"r/".to_vec(),
                after: None,
                limit: 1,
                max_bytes: 1280,
                delimiter: Some(b'/'),
            })],
        })
        .expect("read folded prefix at end of range");
    let ReadResult::Scan(folded_eof) = &folded_eof.results[0] else {
        panic!("scan returned a point result");
    };
    assert_eq!(
        folded_eof.items,
        vec![ScanItem::CommonPrefix(b"r/a/".to_vec())]
    );
    assert!(!folded_eof.more);

    for key in [b"s/a".as_slice(), b"s/b".as_slice()] {
        assert_eq!(put(&store, FIRST, key, b"v"), Commit::Applied);
    }
    let full_eof = store
        .read(ReadBatch {
            ops: vec![ReadOp::Scan(Scan {
                keyspace: FIRST,
                prefix: b"s/".to_vec(),
                after: None,
                limit: 2,
                max_bytes: 1280,
                delimiter: None,
            })],
        })
        .expect("read a full final page");
    let ReadResult::Scan(full_eof) = &full_eof.results[0] else {
        panic!("scan returned a point result");
    };
    assert_eq!(full_eof.items.len(), 2);
    assert!(!full_eof.more);
}

#[test]
fn file_store_reopens_applied_commits() {
    let directory = tempdir().expect("create HoltStore directory");
    let path = directory.path().join("meta");
    {
        let store =
            HoltStore::initialize(HoltOptions::file(&path, catalog([FIRST, SECOND]), limits()))
                .expect("initialize file HoltStore");
        assert_eq!(put(&store, FIRST, b"durable", b"value"), Commit::Applied);
    }
    let reopened = HoltStore::open(HoltOptions::file(&path, catalog([FIRST, SECOND]), limits()))
        .expect("reopen file HoltStore");
    assert_eq!(get(&reopened, FIRST, b"durable"), Some(b"value".to_vec()));
}

#[test]
fn file_store_replays_applied_commit_after_process_exit() {
    if std::env::var_os(CRASH_CHILD_ENV).as_deref() == Some(std::ffi::OsStr::new("1")) {
        let path = std::env::var_os(CRASH_PATH_ENV).expect("crash fixture path");
        let store =
            HoltStore::initialize(HoltOptions::file(path, catalog([FIRST, SECOND]), limits()))
                .expect("initialize crash fixture");
        assert_eq!(put(&store, FIRST, b"durable", b"value"), Commit::Applied);
        std::process::exit(CRASH_EXIT_CODE);
    }

    let directory = tempdir().expect("create crash-test directory");
    let path = directory.path().join("meta");
    let status = Command::new(std::env::current_exe().expect("resolve current test binary"))
        .args([
            "--exact",
            "tests::file_store_replays_applied_commit_after_process_exit",
            "--nocapture",
        ])
        .env(CRASH_CHILD_ENV, "1")
        .env(CRASH_PATH_ENV, &path)
        .status()
        .expect("run crash fixture");
    assert_eq!(status.code(), Some(CRASH_EXIT_CODE));

    let reopened = HoltStore::open(HoltOptions::file(&path, catalog([FIRST, SECOND]), limits()))
        .expect("reopen store after process exit");
    assert_eq!(get(&reopened, FIRST, b"durable"), Some(b"value".to_vec()));
}

#[test]
fn options_reject_unsupported_profiles_catalogs_and_limits() {
    assert_invalid_options(HoltOptions::memory(catalog([FIRST]), limits()));

    let mut async_wal = HoltOptions::file("unused", catalog([FIRST]), limits());
    async_wal.config.durability = Durability::Wal { sync: false };
    assert_invalid_options(async_wal);

    for catalog in [
        vec![],
        vec![TreeBinding::new(FIRST, "")],
        vec![TreeBinding::new(FIRST, "\0invalid")],
        vec![
            TreeBinding::new(FIRST, "first"),
            TreeBinding::new(FIRST, "second"),
        ],
        vec![
            TreeBinding::new(FIRST, "same"),
            TreeBinding::new(SECOND, "same"),
        ],
    ] {
        assert_invalid_options(HoltOptions::file("unused", catalog, limits()));
    }

    let base = limits();
    for invalid in [
        StoreLimits {
            max_reads: 0,
            ..base
        },
        StoreLimits {
            max_checks: 0,
            ..base
        },
        StoreLimits {
            max_mutations: 0,
            ..base
        },
        StoreLimits {
            max_key_bytes: 0,
            ..base
        },
        StoreLimits {
            max_value_bytes: 0,
            ..base
        },
        StoreLimits {
            max_read_bytes: 0,
            ..base
        },
        StoreLimits {
            max_transaction_bytes: 0,
            ..base
        },
        StoreLimits {
            max_result_rows: 0,
            ..base
        },
        StoreLimits {
            max_result_bytes: 0,
            ..base
        },
        StoreLimits {
            max_key_bytes: u16::MAX as usize,
            ..base
        },
        StoreLimits {
            max_value_bytes: u16::MAX as usize + 1,
            ..base
        },
        StoreLimits {
            max_result_bytes: base.max_key_bytes + base.max_value_bytes - 1,
            ..base
        },
    ] {
        assert_invalid_options(HoltOptions::file("unused", catalog([FIRST]), invalid));
    }
}

#[test]
fn initialize_and_open_enforce_the_exact_physical_catalog() {
    let directory = tempdir().expect("create HoltStore directory");
    let path = directory.path().join("meta");
    drop(
        HoltStore::initialize(HoltOptions::file(&path, catalog([FIRST]), limits()))
            .expect("initialize physical catalog"),
    );

    assert!(matches!(
        HoltStore::initialize(HoltOptions::file(&path, catalog([FIRST]), limits())),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        HoltStore::open(HoltOptions::file(&path, catalog([FIRST, SECOND]), limits(),)),
        Err(StoreError::Corrupt(_))
    ));
    HoltStore::open(HoltOptions::file(&path, catalog([FIRST]), limits()))
        .expect("open exact physical catalog");
}

#[test]
fn opening_a_live_store_names_the_owner_conflict_not_the_access_mode() {
    let directory = tempdir().expect("create HoltStore parent directory");
    let path = directory.path().join("metadata");
    let live = HoltStore::initialize(HoltOptions::file(&path, catalog([FIRST]), limits()))
        .expect("initialize the first owner");

    // The directory is the exclusive local authority, so a second owner aimed
    // at it must be refused while the first is still open.
    let reason = match HoltStore::open(HoltOptions::file(&path, catalog([FIRST]), limits())) {
        Ok(_) => panic!("a second owner opened a live store"),
        Err(StoreError::Unavailable(reason)) => reason,
        Err(other) => panic!("expected an Unavailable refusal, got {other:?}"),
    };
    assert!(
        reason.contains("another live owner already holds this metadata store"),
        "refusal did not name the owner conflict: {reason}"
    );

    // The original blob-store text is retained as the cause, and the refusal
    // must not disturb the live owner.
    assert!(
        reason.contains("incompatible live access mode"),
        "refusal dropped its underlying cause: {reason}"
    );
    drop(live);
    HoltStore::open(HoltOptions::file(&path, catalog([FIRST]), limits()))
        .expect("reopen after the live owner released the store");
}

#[test]
fn path_preflight_refuses_missing_open_and_foreign_directories() {
    let directory = tempdir().expect("create HoltStore parent directory");
    let missing = directory.path().join("missing");
    assert!(matches!(
        HoltStore::open(HoltOptions::file(&missing, catalog([FIRST]), limits(),)),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(!missing.exists(), "open created a missing directory");

    let foreign = directory.path().join("foreign");
    std::fs::create_dir(&foreign).expect("create foreign directory");
    std::fs::write(foreign.join("owner.txt"), b"foreign").expect("write foreign marker");
    assert!(matches!(
        HoltStore::initialize(HoltOptions::file(&foreign, catalog([FIRST]), limits(),)),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        HoltStore::open(HoltOptions::file(&foreign, catalog([FIRST]), limits(),)),
        Err(StoreError::Corrupt(_))
    ));
    assert_eq!(
        std::fs::read(foreign.join("owner.txt")).expect("read foreign marker"),
        b"foreign"
    );
    assert!(!foreign.join("blobs.dat").exists());
    assert!(!foreign.join("journal.wal").exists());
}

#[test]
fn file_profile_disables_unqualified_automatic_vacuum() {
    let directory = tempdir().expect("create HoltStore parent directory");
    let path = directory.path().join("meta");
    let mut options = HoltOptions::file(&path, catalog([FIRST]), limits());
    assert!(!options.config.checkpoint.auto_vacuum);
    options.config.checkpoint.auto_vacuum = true;
    assert!(matches!(
        HoltStore::initialize(options),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(!path.exists(), "invalid profile created a store directory");

    let oversized_path = directory.path().join("oversized");
    let mut oversized_limits = limits();
    oversized_limits.max_transaction_bytes = 16 * 1024 * 1024;
    assert!(matches!(
        HoltStore::initialize(HoltOptions::file(
            &oversized_path,
            catalog([FIRST]),
            oversized_limits,
        )),
        Err(StoreError::InvalidRequest(_))
    ));
    assert!(
        !oversized_path.exists(),
        "oversized WAL profile created a store directory"
    );

    let boundary_path = directory.path().join("boundary");
    let mut boundary_limits = limits();
    boundary_limits.max_transaction_bytes = 16 * 1024 * 1024 - 33 - 128 * 17;
    drop(
        HoltStore::initialize(HoltOptions::file(
            &boundary_path,
            catalog([FIRST]),
            boundary_limits,
        ))
        .expect("initialize profile at the Holt WAL boundary"),
    );
}

/// Power-loss torn-write probe for the file-backed Holt adapter.
///
/// Crash model: the process dies while Holt writes a 512 KiB blob frame to
/// `blobs.dat`. A frame write is not power-loss atomic. Holt must not modify a
/// slot while the durable manifest still references that slot, because WAL
/// redo requires a complete base frame.
///
/// The probe brackets one background checkpoint round with directory
/// snapshots. It rejects an in-place rewrite of a durable slot and verifies
/// that recovery ignores torn bytes in slots that the crash-time manifest does
/// not reference.
#[test]
fn file_store_checkpoint_never_rewrites_durable_slots_in_place() {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::{Duration, Instant};

    const SLOT_BYTES: usize = 0x80000;

    fn snapshot_store_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).expect("create HoltStore snapshot directory");
        for entry in std::fs::read_dir(src).expect("read live HoltStore directory") {
            let entry = entry.expect("read HoltStore directory entry");
            if entry.file_name().to_string_lossy() == "store.lock" {
                continue;
            }
            let target = dst.join(entry.file_name());
            if entry.path().is_dir() {
                snapshot_store_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).expect("copy HoltStore snapshot file");
            }
        }
    }

    fn wait_for_wal_truncation(dir: &Path, header_len: u64) {
        let wal = dir.join("journal.wal");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let length = std::fs::metadata(&wal)
                .map(|metadata| metadata.len())
                .unwrap_or(u64::MAX);
            if length <= header_len {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "checkpoint round did not complete within 30 seconds; WAL has {length} bytes",
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn durable_manifest(dir: &Path) -> BTreeMap<[u8; 16], u64> {
        let mut entries = BTreeMap::new();
        if let Ok(bytes) = std::fs::read(dir.join("manifest.bin")) {
            assert!(bytes.len() >= 24, "manifest.bin header is truncated");
            assert_eq!(&bytes[..8], b"ARTSNMNF", "manifest.bin magic");
            let count =
                u32::from_le_bytes(bytes[10..14].try_into().expect("manifest.bin entry count"))
                    as usize;
            let mut offset = 24;
            for _ in 0..count {
                assert!(
                    bytes.len() >= offset + 24,
                    "manifest.bin entry is truncated"
                );
                let mut guid = [0_u8; 16];
                guid.copy_from_slice(&bytes[offset..offset + 16]);
                let slot = u64::from_le_bytes(
                    bytes[offset + 16..offset + 24]
                        .try_into()
                        .expect("manifest.bin slot"),
                );
                entries.insert(guid, slot);
                offset += 24;
            }
        }
        if let Ok(bytes) = std::fs::read(dir.join("manifest.log")) {
            let mut offset = 0_usize;
            while offset + 9 <= bytes.len() {
                let start = offset;
                assert_eq!(&bytes[start..start + 4], b"MLG1", "manifest.log magic");
                let body_len = u32::from_le_bytes(
                    bytes[start + 4..start + 8]
                        .try_into()
                        .expect("manifest.log body length"),
                ) as usize;
                let record_len = 9 + body_len + 4;
                if bytes.len() - start < record_len {
                    break;
                }
                let body = &bytes[start + 9..start + 9 + body_len];
                match bytes[start + 8] {
                    1 => {
                        let mut guid = [0_u8; 16];
                        guid.copy_from_slice(&body[..16]);
                        let slot = u64::from_le_bytes(
                            body[16..24].try_into().expect("manifest.log set slot"),
                        );
                        entries.insert(guid, slot);
                    }
                    2 => {
                        let mut guid = [0_u8; 16];
                        guid.copy_from_slice(body);
                        entries.remove(&guid);
                    }
                    other => panic!("manifest.log contains unknown operation {other}"),
                }
                offset = start + record_len;
            }
        }
        entries
    }

    fn changed_slots(before: &[u8], after: &[u8]) -> Vec<u64> {
        let overlap = before.len().min(after.len()) / SLOT_BYTES;
        (0..overlap)
            .filter(|slot| {
                before[slot * SLOT_BYTES..(slot + 1) * SLOT_BYTES]
                    != after[slot * SLOT_BYTES..(slot + 1) * SLOT_BYTES]
            })
            .map(|slot| slot as u64)
            .collect()
    }

    fn record(tree: Keyspace, index: usize) -> (Key, Vec<u8>) {
        let key = format!("data/{index:04}.bin");
        let mut value = vec![u8::try_from(index % 251).expect("bounded fill byte"); 256];
        value[..8].copy_from_slice(&(index as u64).to_le_bytes());
        (Key::new(tree, key.as_bytes()), value)
    }

    fn put_records(store: &HoltStore, records: &[(Key, Vec<u8>)]) {
        let mutations = records
            .iter()
            .map(|(key, value)| Mutation::Put {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        assert_eq!(
            store
                .commit(WriteTxn {
                    checks: Vec::new(),
                    mutations,
                })
                .expect("commit checkpoint-probe records"),
            Commit::Applied
        );
    }

    fn checkpoint_limits() -> StoreLimits {
        StoreLimits {
            max_reads: 128,
            max_checks: 128,
            max_mutations: 256,
            max_key_bytes: 256,
            max_value_bytes: 1024,
            max_read_bytes: 1 << 20,
            max_transaction_bytes: 1 << 20,
            max_result_rows: 1024,
            max_result_bytes: 1 << 20,
        }
    }

    fn checkpoint_catalog() -> Vec<TreeBinding> {
        catalog([FIRST, SECOND])
    }

    fn read_expected(store: &HoltStore, records: &[(Key, Vec<u8>)], label: &str) {
        for (key, expected) in records {
            let actual = get(store, key.keyspace, &key.bytes)
                .unwrap_or_else(|| panic!("acked checkpoint-probe record was lost ({label})"));
            assert_eq!(
                &actual, expected,
                "acked checkpoint-probe record was corrupted ({label})"
            );
        }
    }

    let directory = tempdir().expect("create checkpoint-probe directory");
    let database = directory.path().join("torn-frame.holt");
    let store = HoltStore::initialize(HoltOptions::file(
        &database,
        checkpoint_catalog(),
        checkpoint_limits(),
    ))
    .expect("initialize checkpoint-probe HoltStore");
    let wal_header_len = std::fs::metadata(database.join("journal.wal"))
        .expect("read initial WAL metadata")
        .len();

    for tree in [FIRST, SECOND] {
        let seeded = (0..128)
            .map(|index| record(tree, index))
            .collect::<Vec<_>>();
        put_records(&store, &seeded);
    }
    wait_for_wal_truncation(&database, wal_header_len);
    let before = directory.path().join("before");
    snapshot_store_dir(&database, &before);

    let mut acknowledged = Vec::new();
    for index in 500..536 {
        acknowledged.push(record(FIRST, index));
        acknowledged.push(record(SECOND, index));
    }
    put_records(&store, &acknowledged);

    let crash_time = directory.path().join("crash-time");
    snapshot_store_dir(&database, &crash_time);
    assert_eq!(
        std::fs::read(before.join("blobs.dat")).expect("read pre-round blobs"),
        std::fs::read(crash_time.join("blobs.dat")).expect("read crash-time blobs"),
        "crash-time snapshot raced the checkpoint round for blobs.dat",
    );
    assert_eq!(
        std::fs::read(before.join("manifest.bin")).ok(),
        std::fs::read(crash_time.join("manifest.bin")).ok(),
        "crash-time snapshot raced the checkpoint round for manifest.bin",
    );
    assert_eq!(
        std::fs::read(before.join("manifest.log")).ok(),
        std::fs::read(crash_time.join("manifest.log")).ok(),
        "crash-time snapshot raced the checkpoint round for manifest.log",
    );
    assert!(
        std::fs::metadata(crash_time.join("journal.wal"))
            .expect("read crash-time WAL metadata")
            .len()
            > wal_header_len,
        "acknowledged transaction must be present in the crash-time WAL",
    );

    wait_for_wal_truncation(&database, wal_header_len);
    let after = directory.path().join("after");
    snapshot_store_dir(&database, &after);
    drop(store);

    let crash_blobs =
        std::fs::read(crash_time.join("blobs.dat")).expect("read crash-time blob frames");
    let after_blobs = std::fs::read(after.join("blobs.dat")).expect("read post-round blob frames");
    let changed = changed_slots(&crash_blobs, &after_blobs);
    assert!(
        !changed.is_empty(),
        "checkpoint round did not rewrite any existing frame slot"
    );
    let crash_manifest = durable_manifest(&crash_time);
    let after_manifest = durable_manifest(&after);
    let in_place = changed
        .iter()
        .copied()
        .filter(|slot| {
            crash_manifest.iter().any(|(guid, durable_slot)| {
                durable_slot == slot && after_manifest.get(guid) == Some(slot)
            })
        })
        .collect::<Vec<_>>();

    if !in_place.is_empty() {
        let torn = directory.path().join("torn-durable-slot");
        snapshot_store_dir(&crash_time, &torn);
        let mut torn_blobs = crash_blobs.clone();
        for &slot in &in_place {
            const BLOCK_BYTES: usize = 4096;
            let start = slot as usize * SLOT_BYTES;
            for (index, offset) in (start..start + SLOT_BYTES).step_by(BLOCK_BYTES).enumerate() {
                if index % 2 == 0 {
                    torn_blobs[offset..offset + BLOCK_BYTES]
                        .copy_from_slice(&after_blobs[offset..offset + BLOCK_BYTES]);
                }
            }
        }
        std::fs::write(torn.join("blobs.dat"), torn_blobs).expect("write torn durable-slot image");
        let outcome = match HoltStore::open(HoltOptions::file(
            &torn,
            checkpoint_catalog(),
            checkpoint_limits(),
        )) {
            Err(error) => format!("reopen failed: {error}"),
            Ok(reopened) => {
                let lost = acknowledged
                    .iter()
                    .filter(|(key, expected)| {
                        get(&reopened, key.keyspace, &key.bytes).as_ref() != Some(expected)
                    })
                    .count();
                format!(
                    "reopen succeeded but lost {lost}/{} acknowledged records",
                    acknowledged.len()
                )
            }
        };
        panic!(
            "checkpoint rewrote durable manifest slots {in_place:?} in place; a power cut can tear the WAL base frame; torn-image outcome: {outcome}"
        );
    }

    let torn_slots = changed
        .iter()
        .copied()
        .filter(|slot| {
            !crash_manifest
                .values()
                .any(|durable_slot| durable_slot == slot)
        })
        .collect::<Vec<_>>();
    assert!(
        !torn_slots.is_empty(),
        "checkpoint shadow rewrites did not use an unreferenced slot"
    );
    for (label, half_frame) in [("new-prefix", true), ("interleave-4k", false)] {
        let torn = directory.path().join(format!("torn-{label}"));
        snapshot_store_dir(&crash_time, &torn);
        let mut torn_blobs = crash_blobs.clone();
        for &slot in &torn_slots {
            let start = slot as usize * SLOT_BYTES;
            if half_frame {
                let middle = start + SLOT_BYTES / 2;
                torn_blobs[start..middle].copy_from_slice(&after_blobs[start..middle]);
            } else {
                const BLOCK_BYTES: usize = 4096;
                for (index, offset) in (start..start + SLOT_BYTES).step_by(BLOCK_BYTES).enumerate()
                {
                    if index % 2 == 0 {
                        torn_blobs[offset..offset + BLOCK_BYTES]
                            .copy_from_slice(&after_blobs[offset..offset + BLOCK_BYTES]);
                    }
                }
            }
        }
        std::fs::write(torn.join("blobs.dat"), torn_blobs)
            .expect("write torn unreferenced-slot image");

        let reopened = HoltStore::open(HoltOptions::file(
            &torn,
            checkpoint_catalog(),
            checkpoint_limits(),
        ))
        .unwrap_or_else(|error| {
            panic!("reopen with torn unreferenced slots {torn_slots:?} ({label}) failed: {error}")
        });
        read_expected(&reopened, &acknowledged, label);
    }
}

#[test]
fn definitely_not_applied_atomic_error_keeps_store_ready() {
    let store = memory_store([FIRST]);
    assert_eq!(put(&store, FIRST, b"healthy", b"value"), Commit::Applied);

    let txn = WriteTxn {
        checks: vec![],
        mutations: vec![Mutation::Put {
            key: Key::new(FIRST, b"not-applied"),
            value: b"value".to_vec(),
        }],
    };
    store.fail_next_atomic_not_applied();
    let error = store
        .commit(txn.clone())
        .expect_err("classified pre-apply failure must be returned");
    assert!(matches!(error, StoreError::Unavailable(_)));
    assert_eq!(get(&store, FIRST, b"not-applied"), None);
    assert_eq!(get(&store, FIRST, b"healthy"), Some(b"value".to_vec()));
    store
        .ready()
        .expect("definitely-not-applied failure must not poison store");
    assert_eq!(
        store.commit(txn).expect("retry the same transaction"),
        Commit::Applied
    );
    assert_eq!(get(&store, FIRST, b"not-applied"), Some(b"value".to_vec()));
}

#[test]
fn atomic_error_poison_is_sticky_until_file_reopen() {
    let directory = tempdir().expect("create HoltStore directory");
    let path = directory.path().join("meta");
    let store = Arc::new(
        HoltStore::initialize(HoltOptions::file(&path, catalog([FIRST]), limits()))
            .expect("initialize file HoltStore"),
    );
    assert_eq!(
        put(store.as_ref(), FIRST, b"healthy", b"value"),
        Commit::Applied
    );
    conformance::assert_poisoned(
        store.as_ref(),
        Key::new(FIRST, b"healthy"),
        b"value".to_vec(),
        || {
            store.fail_next_atomic_after_apply();
            store.commit(WriteTxn {
                checks: vec![],
                mutations: vec![Mutation::Put {
                    key: Key::new(FIRST, b"uncertain"),
                    value: b"applied".to_vec(),
                }],
            })
        },
    );
    drop(store);

    let reopened = HoltStore::open(HoltOptions::file(&path, catalog([FIRST]), limits()))
        .expect("reopen poisoned file HoltStore");
    assert_eq!(
        get(&reopened, FIRST, b"uncertain"),
        Some(b"applied".to_vec())
    );
}

#[test]
fn operations_overlapping_poison_transition_cannot_return_success() {
    let store = Arc::new(memory_store([FIRST]));
    assert_eq!(put(&store, FIRST, b"healthy", b"value"), Commit::Applied);
    assert_eq!(
        store
            .commit(WriteTxn {
                checks: vec![Check::Value {
                    key: Key::new(FIRST, b"healthy"),
                    expected: b"value".to_vec(),
                }],
                mutations: vec![],
            })
            .expect("validate the healthy check-only transaction"),
        Commit::Applied
    );

    let (atomic_entered_tx, atomic_entered_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    store.pause_next_atomic_before_poison(atomic_entered_tx, resume_rx);

    let commit_store = Arc::clone(&store);
    let commit = thread::spawn(move || {
        commit_store.commit(WriteTxn {
            checks: vec![],
            mutations: vec![Mutation::Put {
                key: Key::new(FIRST, b"uncertain"),
                value: b"applied".to_vec(),
            }],
        })
    });
    atomic_entered_rx
        .recv()
        .expect("fault hook did not reach the poison boundary");

    let (read_entered_tx, read_entered_rx) = mpsc::sync_channel(0);
    store.signal_next_read_entry(read_entered_tx);
    let read_store = Arc::clone(&store);
    let read = thread::spawn(move || {
        read_store.read(ReadBatch {
            ops: vec![ReadOp::Get(Key::new(FIRST, b"healthy"))],
        })
    });
    read_entered_rx
        .recv()
        .expect("overlapping read did not enter the adapter");

    let (commit_entered_tx, commit_entered_rx) = mpsc::sync_channel(0);
    store.signal_next_commit_entry(commit_entered_tx);
    let check_store = Arc::clone(&store);
    let check = thread::spawn(move || {
        check_store.commit(WriteTxn {
            checks: vec![Check::Value {
                key: Key::new(FIRST, b"healthy"),
                expected: b"value".to_vec(),
            }],
            mutations: vec![],
        })
    });
    commit_entered_rx
        .recv()
        .expect("overlapping commit did not enter the adapter");

    resume_tx
        .send(())
        .expect("release the injected poison transition");
    conformance::assert_unknown(
        commit.join().expect("poisoning commit thread panicked"),
        UnknownCommit::Poisoned,
    );
    assert!(
        read.join()
            .expect("overlapping read thread panicked")
            .is_err(),
        "overlapping read succeeded after the poison transition"
    );
    assert!(
        check
            .join()
            .expect("overlapping commit thread panicked")
            .is_err(),
        "overlapping commit succeeded after the poison transition"
    );
}

#[test]
fn invalid_requests_do_not_poison_the_store() {
    let store = memory_store([FIRST]);
    let result = store.read(ReadBatch {
        ops: vec![ReadOp::Get(Key::new(SECOND, b"key"))],
    });
    assert_eq!(
        result,
        Err(StoreError::InvalidRequest(
            "keyspace 0002 is not configured for this HoltStore".to_owned()
        ))
    );
    store
        .ready()
        .expect("invalid request must not poison store");
}

#[test]
fn storage_neutral_conformance_passes_across_file_reopen() {
    let directory = tempdir().expect("create conformance directory");
    let path = directory.path().join("meta");
    conformance::run(
        || {
            HoltStore::initialize(HoltOptions::file(
                &path,
                catalog(CONFORMANCE_KEYSPACES),
                limits(),
            ))
        },
        || {
            HoltStore::open(HoltOptions::file(
                &path,
                catalog(CONFORMANCE_KEYSPACES),
                limits(),
            ))
        },
    );
}

#[test]
fn unknown_states_remain_adapter_visible() {
    let error = StoreError::OutcomeUnknown {
        state: UnknownCommit::Poisoned,
        reason: "test".to_owned(),
    };
    conformance::assert_unknown(Err(error), UnknownCommit::Poisoned);
}
