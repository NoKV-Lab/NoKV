/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! An `--etcd-endpoint` that cannot name an etcd server is the caller's
//! mistake: every command refuses it as `invalid_input` (exit 2,
//! `fix_input`) before it connects or prepares anything, so a supervisor
//! that follows `next_action` does not restart an owner forever on a typo.
//! One that carries credentials is refused without repeating them. A
//! well-formed endpoint that nothing answers stays `transient`.
#![cfg(feature = "etcd")]

use std::process::{Command, Output};

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

/// `nokv` with `endpoint` as its only etcd endpoint.
fn run(endpoint: &str, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nokv"))
        .args(["--root-id", "11111111111111111111111111111111"])
        .args(["--agent-id", "44444444444444444444444444444444"])
        .args(["--etcd-endpoint", endpoint, "--etcd-lease-ttl-seconds", "1"])
        .args(arguments)
        .output()
        .expect("nokv must execute")
}

/// The exit code and the failure of `run`: one JSON line, the last on
/// stderr.
fn nokv(endpoint: &str, arguments: &[&str]) -> (i32, Value) {
    let output = run(endpoint, arguments);
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

/// Credentials in an endpoint, whatever else is wrong with it, are refused
/// before anything else and appear in no output, including the line an
/// owner exits with.
#[test]
fn an_endpoint_with_credentials_is_refused_without_repeating_them() {
    let scratch = tempfile::tempdir().unwrap();
    let metadata = scratch.path().join("metadata");
    let serve = [
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
    ];
    let commands: [&[&str]; 6] = [
        &["identity", "show", "55555555555555555555555555555555"],
        &[
            "identity",
            "tenant",
            "create",
            "55555555555555555555555555555555",
        ],
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
        &["workbench", "workbench_stat", r#"{"id":"w"}"#],
        &serve,
    ];
    for endpoint in [
        "user:s3cr3t@127.0.0.1:2379",
        "HTTP://user:s3cr3t@127.0.0.1:2379",
        "ftp://user:s3cr3t@127.0.0.1:2379",
        "http://user:s3cr3t@127.0.0.1:2379",
    ] {
        for command in commands {
            let output = run(endpoint, command);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                !stdout.contains("s3cr3t") && !stderr.contains("s3cr3t"),
                "{command:?} repeats the credentials of its etcd endpoint"
            );
            assert_eq!(output.status.code(), Some(2), "{command:?}: {stderr}");
            let envelope: Value =
                serde_json::from_str(stderr.trim().lines().last().unwrap_or("{}")).unwrap();
            assert_eq!(envelope["class"], "invalid_input", "{envelope}");
            assert!(
                envelope["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("carries credentials")),
                "{envelope}"
            );
        }
    }
    assert!(!metadata.exists());
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
