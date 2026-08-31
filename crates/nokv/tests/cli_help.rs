/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::process::Command;

#[test]
fn help_exposes_seed_discovery_and_dual_metadata_runtimes() {
    let output = Command::new(env!("CARGO_BIN_EXE_nokv"))
        .arg("--help")
        .output()
        .expect("nokv help must execute");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("nokv help must be UTF-8");

    assert!(help.contains("--seed HOST:PORT [--seed HOST:PORT ...]"));
    assert!(help.contains("clients never connect to the metadata database directly"));
    assert!(help.contains("Holt is one exclusive standalone metadata store"));
    assert!(help.contains("FDB is"));
    assert!(help.contains("NOT QUALIFIED"));
}
