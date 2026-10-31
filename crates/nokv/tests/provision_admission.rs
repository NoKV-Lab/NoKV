/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `nokv provision` against an object provider that refuses connections:
//! the admission probe fails before it can take effect, so the command must
//! say so (`transient`, `resubmit_same`) after spending its retry budget,
//! not report an internal failure that tells the caller to give up.

use std::process::Command;

use serde_json::Value;

const ROOT: &str = "11111111111111111111111111111111";
const AGENT: &str = "44444444444444444444444444444444";
const SHARD: &str = "22222222222222222222222222222222";

fn provision(control: &std::path::Path, retry: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_nokv"))
        .args(["--root-id", ROOT, "--agent-id", AGENT])
        .args(["--control-dir".as_ref(), control.as_os_str()])
        // Nothing listens on privileged port 1, so every probe is refused
        // without leaving the host.
        .args([
            "--object-bucket",
            "admission-test",
            "--object-endpoint",
            "http://127.0.0.1:1",
            "--object-skip-signature",
        ])
        .args(retry)
        .args([
            "provision",
            SHARD,
            "--workbench-root",
            "/agents/admission/wb",
        ])
        .output()
        .expect("nokv provision must execute");
    let stderr = String::from_utf8(output.stderr).expect("stderr must be UTF-8");
    let envelope = serde_json::from_str(stderr.trim().lines().last().unwrap_or("{}"))
        .unwrap_or_else(|error| panic!("the failure must be one JSON line ({error}): {stderr}"));
    (output.status.code().unwrap_or(-1), envelope)
}

#[test]
fn an_unreachable_provider_fails_provision_as_transient_after_the_retry_budget() {
    let control = tempfile::tempdir().unwrap();
    let (exit, envelope) = provision(
        control.path(),
        &["--max-attempts", "3", "--retry-backoff-ms", "5"],
    );
    assert_eq!(exit, 5, "{envelope}");
    assert_eq!(envelope["class"], "transient", "{envelope}");
    assert_eq!(envelope["next_action"], "resubmit_same", "{envelope}");
    assert_eq!(envelope["code"], "ProviderAdmissionFailed", "{envelope}");
    assert_eq!(envelope["details"]["attempts"], 3, "{envelope}");
    assert!(
        envelope["message"]
            .as_str()
            .is_some_and(|message| message.contains("admission")),
        "{envelope}"
    );
}

#[test]
fn a_single_attempt_budget_probes_the_provider_once() {
    let control = tempfile::tempdir().unwrap();
    let (exit, envelope) = provision(control.path(), &["--max-attempts", "1"]);
    assert_eq!(exit, 5, "{envelope}");
    assert_eq!(envelope["details"]["attempts"], 1, "{envelope}");
}
