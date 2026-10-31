/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A control directory's records include the tenants' authorization policy,
//! and its file permissions are its only access control. Every command
//! refuses one that group or other users can write, before it reads or
//! writes a record, as a usage error that names the directory and the fix.
#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;

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
fn a_control_directory_other_users_can_write_is_refused_as_invalid_input() {
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
        assert_eq!(exit, 2, "{envelope}");
        assert_eq!(envelope["class"], "invalid_input", "{envelope}");
        assert_eq!(envelope["next_action"], "fix_input", "{envelope}");
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
