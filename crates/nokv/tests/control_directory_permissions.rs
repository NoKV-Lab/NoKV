/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A control directory's records include the tenants' authorization policy,
//! and its file permissions are its only access control. Every command
//! refuses one that group or other users can write, before it reads or
//! writes a record. Someone else may have changed its records, so the
//! refusal is `operator_required` (exit 9, `operator`) and names the
//! directory and the fix. A command that writes it also refuses one it
//! cannot create or write, and changes nothing when it refuses.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Command;

use nokv_control::{
    AgentId, ControlStore as _, IdentityStore as _, LocalControlStore, LogicalShardId,
    ObjectNamespaceId, PlacementGeneration, RootAgentBinding, RootId, RootObjectNamespaceBinding,
    RootPlacement, RootPlacementLifecycle, TenantId, TenantRecord,
};
use serde_json::Value;

const ROOT: &str = "11111111111111111111111111111111";
const AGENT: &str = "44444444444444444444444444444444";
const OPERATION: &str = "33333333333333333333333333333333";

fn nokv(control: &Path, arguments: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_nokv"))
        .args(["--root-id", ROOT, "--agent-id", AGENT])
        .args(["--control-dir".as_ref(), control.as_os_str()])
        .args(arguments)
        .output()
        .expect("nokv must execute");
    // A result is one JSON document on stdout; a failure is one JSON line,
    // the last on stderr.
    let success = output.status.success();
    let text = String::from_utf8(if success {
        output.stdout
    } else {
        output.stderr
    })
    .expect("output must be UTF-8");
    let json = if success {
        text.trim()
    } else {
        text.trim().lines().last().unwrap_or("{}")
    };
    let envelope = serde_json::from_str(json)
        .unwrap_or_else(|error| panic!("the result must be JSON ({error}): {text}"));
    (output.status.code().unwrap_or(-1), envelope)
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[test]
fn a_control_directory_other_users_can_write_is_refused_for_an_operator() {
    let parent = tempfile::tempdir().unwrap();
    let control = parent.path().join("control");
    let (exit, envelope) = nokv(
        &control,
        &[
            "identity",
            "tenant",
            "create",
            "55555555555555555555555555555555",
        ],
    );
    assert_eq!(exit, 0, "{envelope}");
    assert_eq!(mode(&control), 0o700);
    assert_eq!(mode(&control.join("identity-state.json")), 0o600);

    fs::set_permissions(&control, fs::Permissions::from_mode(0o770)).unwrap();
    for arguments in [
        // An operator command that writes the identity records.
        &[
            "identity",
            "tenant",
            "create",
            "66666666666666666666666666666666",
        ][..],
        // An Agent command that only reads a route.
        &["operation", "status", OPERATION][..],
    ] {
        let (exit, envelope) = nokv(&control, arguments);
        assert_eq!(exit, 9, "{envelope}");
        assert_eq!(envelope["class"], "operator_required", "{envelope}");
        assert_eq!(envelope["next_action"], "operator", "{envelope}");
        let message = envelope["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(&control.display().to_string())
                && message.contains("group or other users can write it")
                && message.contains("chmod go-w"),
            "{envelope}"
        );
    }

    fs::set_permissions(&control, fs::Permissions::from_mode(0o700)).unwrap();
    let (exit, envelope) = nokv(
        &control,
        &[
            "identity",
            "tenant",
            "create",
            "66666666666666666666666666666666",
        ],
    );
    assert_eq!(exit, 0, "{envelope}");
}

const TENANT_CREATE: [&str; 4] = [
    "identity",
    "tenant",
    "create",
    "77777777777777777777777777777777",
];

/// The user this process runs as: the owner of a directory it created.
fn user_of(created: &Path) -> u32 {
    fs::metadata(created).unwrap().uid()
}

fn assert_refused(exit: i32, envelope: &Value, path: &Path, why: &str) {
    assert_eq!(exit, 9, "{}: {envelope}", path.display());
    assert_eq!(envelope["class"], "operator_required", "{envelope}");
    assert_eq!(envelope["next_action"], "operator", "{envelope}");
    let message = envelope["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(&format!("refuses {}:", path.display())) && message.contains(why),
        "{envelope}"
    );
}

/// A control directory as `provision` leaves it for a new logical shard:
/// `ROOT` is Active on it with its Agent and object namespace bindings. It
/// also holds a tenant, which owns no root.
fn provisioned_control_directory(parent: &Path) -> PathBuf {
    let control = parent.join("control");
    let store = LocalControlStore::open(&control).unwrap();
    let root_id = RootId::from_bytes([0x11; 16]);
    let logical_shard_id = LogicalShardId::from_bytes([0x22; 16]);
    store
        .create_root_agent_binding(RootAgentBinding {
            root_id,
            agent_id: AgentId::from_bytes([0x44; 16]),
        })
        .unwrap();
    store
        .create_root_object_namespace_binding(RootObjectNamespaceBinding {
            root_id,
            object_namespace_id: ObjectNamespaceId::from_bytes([0x55; 16]),
        })
        .unwrap();
    store.create_logical_shard(logical_shard_id).unwrap();
    let provisioning = store
        .create_root_placement(RootPlacement {
            root_id,
            logical_shard_id,
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        })
        .unwrap();
    store
        .compare_and_set_root_placement(
            &provisioning,
            RootPlacement {
                placement_generation: PlacementGeneration::new(2).unwrap(),
                lifecycle: RootPlacementLifecycle::Active,
                ..provisioning.clone()
            },
        )
        .unwrap();
    store
        .create_tenant(TenantRecord {
            tenant_id: TenantId::from_bytes([0x66; 16]),
        })
        .unwrap();
    control
}

/// `nokv serve` for `ROOT` with a new metadata store at `metadata`. The
/// object provider refuses connections, so a serve that passes its control
/// checks fails at the provider instead of serving.
fn serve(control: &Path, metadata: &Path) -> (i32, Value) {
    nokv(
        control,
        &[
            "--node-id",
            "owner-a",
            "--bind",
            "127.0.0.1:0",
            "--advertise-endpoint",
            "127.0.0.1:1",
            "--metadata-create",
            metadata.to_str().unwrap(),
            "--object-bucket",
            "permissions-test",
            "--object-endpoint",
            "http://127.0.0.1:1",
            "--object-skip-signature",
            "--max-attempts",
            "1",
            "serve",
        ],
    )
}

#[test]
fn serve_refuses_an_unsafe_control_directory_before_it_prepares_metadata() {
    let parent = tempfile::tempdir().unwrap();
    let control = provisioned_control_directory(parent.path());
    let metadata = parent.path().join("metadata");

    // Private, serve passes every control check and fails at the provider.
    let (exit, envelope) = serve(&control, &metadata);
    assert!(
        ![0, 2].contains(&exit) && envelope["code"] != "ControlFailure",
        "{envelope}"
    );
    assert!(!metadata.exists(), "{envelope}");

    let shared = "group or other users can write it";
    let mut cases = vec![
        (control.clone(), 0o770, shared),
        (control.join("sessions"), 0o770, shared),
        (control.join("control-state.json"), 0o660, shared),
        (control.join("identity-state.json"), 0o606, shared),
    ];
    if user_of(parent.path()) != 0 {
        // Root may write any directory.
        cases.push((control.clone(), 0o500, "cannot create files in it"));
    }
    for (path, unusable, why) in cases {
        let private = mode(&path);
        fs::set_permissions(&path, fs::Permissions::from_mode(unusable)).unwrap();
        let (exit, envelope) = serve(&control, &metadata);
        fs::set_permissions(&path, fs::Permissions::from_mode(private)).unwrap();
        assert_refused(exit, &envelope, &path, why);
        assert!(!metadata.exists(), "{}: {envelope}", path.display());
    }
}

/// Every entry under `directory`: its mode and, for a file, its contents.
/// The records are JSON, so they compare and print as text.
fn tree(directory: &Path) -> BTreeMap<PathBuf, (u32, Option<String>)> {
    let mut entries = BTreeMap::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            let contents = if metadata.is_dir() {
                pending.push(path.clone());
                None
            } else {
                Some(String::from_utf8_lossy(&fs::read(&path).unwrap()).into_owned())
            };
            entries.insert(
                path.strip_prefix(directory).unwrap().to_path_buf(),
                (metadata.mode() & 0o7777, contents),
            );
        }
    }
    entries
}

/// `nokv provision` of `ROOT` on a new logical shard. The object provider
/// refuses connections, so a provision that passes its control checks writes
/// the Agent binding and the presentation and then fails at the provider.
fn provision(control: &Path) -> (i32, Value) {
    nokv(
        control,
        &[
            "--object-bucket",
            "permissions-test",
            "--object-endpoint",
            "http://127.0.0.1:1",
            "--object-skip-signature",
            "--max-attempts",
            "1",
            "provision",
            "22222222222222222222222222222222",
            "--workbench-root",
            "/agents/permissions/wb",
        ],
    )
}

#[test]
fn provision_refuses_an_unsafe_control_record_before_it_writes_anything() {
    let parent = tempfile::tempdir().unwrap();
    let control = parent.path().join("control");
    // Another shard and a tenant: both record files exist, and provisioning
    // `ROOT` writes to each of them.
    let store = LocalControlStore::open(&control).unwrap();
    store
        .create_logical_shard(LogicalShardId::from_bytes([0x23; 16]))
        .unwrap();
    store
        .create_tenant(TenantRecord {
            tenant_id: TenantId::from_bytes([0x66; 16]),
        })
        .unwrap();
    let before = tree(&control);

    let shared = "group or other users can write it";
    let mut cases = vec![
        (control.join("identity-state.json"), 0o606, shared),
        (control.join("control-state.json"), 0o660, shared),
        (control.join("sessions"), 0o770, shared),
    ];
    if user_of(parent.path()) != 0 {
        // Root may write any directory.
        cases.push((control.clone(), 0o500, "cannot create files in it"));
    }
    for (path, unusable, why) in cases {
        let private = mode(&path);
        fs::set_permissions(&path, fs::Permissions::from_mode(unusable)).unwrap();
        let (exit, envelope) = provision(&control);
        fs::set_permissions(&path, fs::Permissions::from_mode(private)).unwrap();
        assert_refused(exit, &envelope, &path, why);
        assert_eq!(tree(&control), before, "{}: {envelope}", path.display());
    }

    // As it was, the directory passes every control check: provision writes
    // the Agent binding, then fails at the provider.
    let (exit, envelope) = provision(&control);
    assert_eq!(exit, 5, "{envelope}");
    assert_eq!(envelope["code"], "ProviderAdmissionFailed", "{envelope}");
    assert!(store
        .get_root_agent_binding(&RootId::from_bytes([0x11; 16]))
        .unwrap()
        .is_some());
}

#[test]
fn a_write_refuses_a_directory_it_cannot_use_before_creating_anything_in_it() {
    let parent = tempfile::tempdir().unwrap();
    let user = user_of(parent.path());
    let control = parent.path().join("control");
    fs::create_dir(&control).unwrap();
    let mut cases = vec![(
        control.clone(),
        0o770,
        "group or other users can write it".to_owned(),
    )];
    if user != 0 {
        // Root may write any directory.
        let cannot = format!("this process's user (uid {user}) cannot create");
        cases.push((control.clone(), 0o555, format!("{cannot} files in it")));
        cases.push((control.join("nested"), 0o555, format!("{cannot} it")));
    }
    for (directory, unusable, why) in cases {
        fs::set_permissions(&control, fs::Permissions::from_mode(unusable)).unwrap();
        let (exit, envelope) = nokv(&directory, &TENANT_CREATE);
        fs::set_permissions(&control, fs::Permissions::from_mode(0o700)).unwrap();
        assert_refused(exit, &envelope, &directory, &why);
        assert_eq!(fs::read_dir(&control).unwrap().count(), 0, "{envelope}");
    }
}

/// A directory that belongs to neither this process's user nor root and
/// that group and other users cannot write. Root makes one; another user
/// looks for a well-known one, and finds none on some hosts.
fn foreign_directory(scratch: &Path) -> Option<PathBuf> {
    let user = user_of(scratch);
    if user == 0 {
        let directory = scratch.join("foreign");
        fs::create_dir(&directory).unwrap();
        std::os::unix::fs::chown(&directory, Some(65_534), None).unwrap();
        return Some(directory);
    }
    // WindowServer's logs on macOS; apt's download areas on Debian and Ubuntu.
    [
        "/Library/Logs/WindowServer",
        "/var/cache/apt/archives/partial",
        "/var/lib/apt/lists/partial",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| {
        fs::metadata(path).is_ok_and(|metadata| {
            metadata.is_dir()
                && ![user, 0].contains(&metadata.uid())
                && metadata.mode() & 0o022 == 0
        })
    })
}

#[test]
fn a_write_refuses_a_directory_another_user_owns_before_creating_anything() {
    let scratch = tempfile::tempdir().unwrap();
    let Some(directory) = foreign_directory(scratch.path()) else {
        eprintln!("skipped: no directory here belongs to another user");
        return;
    };
    let owner = fs::metadata(&directory).unwrap().uid();
    let (exit, envelope) = nokv(&directory, &TENANT_CREATE);
    assert_refused(
        exit,
        &envelope,
        &directory,
        &format!("belongs to uid {owner}"),
    );
    assert!(!directory.join("sessions").exists(), "{envelope}");
}
