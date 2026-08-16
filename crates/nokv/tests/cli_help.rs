/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::process::Command;

#[test]
fn static_route_help_exposes_required_fences_and_refresh_limit() {
    let output = Command::new(env!("CARGO_BIN_EXE_nokv"))
        .arg("--help")
        .output()
        .expect("nokv help must execute");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("nokv help must be UTF-8");

    assert!(help.contains("--placement-generation"));
    assert!(help.contains("--owner-epoch"));
    assert!(help.contains("static routing is a point-in-time pin"));
    assert!(help.contains("cannot refresh after placement changes or owner restarts"));
    assert!(help.contains("use --etcd-endpoint for self-refreshing routing"));
}
