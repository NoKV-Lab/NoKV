/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! An `--etcd-endpoint` that cannot name an etcd server is the caller's
//! mistake: every command refuses it as `invalid_input` (exit 2,
//! `fix_input`) before it connects or prepares anything, so a supervisor
//! that follows `next_action` does not restart an owner forever on a typo.
//! A well-formed endpoint that nothing answers stays `transient`.
#![cfg(feature = "etcd")]

use std::process::Command;

use serde_json::Value;

/// Endpoints that cannot name an etcd server: no host, an unclosed IPv6
/// address, a port outside 1..=65535, another scheme, and no scheme.
const MALFORMED: [&str; 5] = [
    "http://",
    "http://[::1",
    "http://127.0.0.1:99999",
    "ftp://127.0.0.1:2379",
    "127.0.0.1:2379",
];

/// `nokv` with `endpoint` as its only etcd endpoint. A failure is one JSON
/// line, the last on stderr.
fn nokv(endpoint: &str, arguments: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_nokv"))
        .args(["--root-id", "11111111111111111111111111111111"])
        .args(["--agent-id", "44444444444444444444444444444444"])
        .args(["--etcd-endpoint", endpoint, "--etcd-lease-ttl-seconds", "1"])
        .args(arguments)
        .output()
        .expect("nokv must execute");
    let stderr = String::from_utf8(output.stderr).expect("stderr must be UTF-8");
    let envelope = serde_json::from_str(stderr.trim().lines().last().unwrap_or("{}"))
        .unwrap_or_else(|error| panic!("the failure must be one JSON line ({error}): {stderr}"));
    (output.status.code().unwrap_or(-1), envelope)
}

fn assert_invalid_endpoint(endpoint: &str, exit: i32, envelope: &Value) {
    assert_eq!(exit, 2, "{endpoint}: {envelope}");
    assert_eq!(envelope["class"], "invalid_input", "{endpoint}: {envelope}");
    assert_eq!(
        envelope["next_action"], "fix_input",
        "{endpoint}: {envelope}"
    );
    let message = envelope["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(&format!("etcd endpoint {endpoint:?}")),
        "{endpoint}: {envelope}"
    );
}

#[test]
fn serve_refuses_a_malformed_etcd_endpoint_before_it_prepares_metadata() {
    let scratch = tempfile::tempdir().unwrap();
    for (index, endpoint) in MALFORMED.into_iter().enumerate() {
        let metadata = scratch.path().join(format!("metadata-{index}"));
        let (exit, envelope) = nokv(
            endpoint,
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
                "endpoint-test",
                "--object-endpoint",
                "http://127.0.0.1:1",
                "--object-skip-signature",
                "serve",
            ],
        );
        assert_invalid_endpoint(endpoint, exit, &envelope);
        assert!(!metadata.exists(), "{endpoint}: {envelope}");
    }
}

#[test]
fn every_other_command_refuses_a_malformed_etcd_endpoint() {
    let commands: [&[&str]; 3] = [
        &["identity", "show", "55555555555555555555555555555555"],
        &[
            "--object-bucket",
            "endpoint-test",
            "--object-endpoint",
            "http://127.0.0.1:1",
            "--object-skip-signature",
            "provision",
            "22222222222222222222222222222222",
            "--workbench-root",
            "/agents/endpoint/wb",
        ],
        &["operation", "status", "33333333333333333333333333333333"],
    ];
    for endpoint in MALFORMED {
        for command in commands {
            let (exit, envelope) = nokv(endpoint, command);
            assert_invalid_endpoint(endpoint, exit, &envelope);
        }
    }
}

#[test]
fn a_well_formed_etcd_endpoint_nothing_answers_stays_transient() {
    // Nothing listens on privileged port 1.
    let (exit, envelope) = nokv(
        "http://127.0.0.1:1",
        &["identity", "show", "55555555555555555555555555555555"],
    );
    assert_eq!(exit, 5, "{envelope}");
    assert_eq!(envelope["class"], "transient", "{envelope}");
    assert_eq!(envelope["next_action"], "resubmit_same", "{envelope}");
}
