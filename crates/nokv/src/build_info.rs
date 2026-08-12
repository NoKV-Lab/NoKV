/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Build and dependency identity reported by the installed CLI.

use serde_json::{json, Value};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("NOKV_GIT_COMMIT");
pub const CARGO_LOCK_SHA256: &str = env!("NOKV_CARGO_LOCK_SHA256");
pub const HOLT_VERSION: &str = env!("NOKV_HOLT_VERSION");
pub const HOLT_SOURCE: &str = env!("NOKV_HOLT_SOURCE");
pub const HOLT_CHECKSUM: &str = env!("NOKV_HOLT_CHECKSUM");

pub fn identity(workbench_contract_schema: &str, workbench_tool_count: usize) -> Value {
    json!({
        "version": VERSION,
        "git_commit": GIT_COMMIT,
        "cargo_lock_sha256": CARGO_LOCK_SHA256,
        "holt": {
            "version": HOLT_VERSION,
            "source": HOLT_SOURCE,
            "checksum": HOLT_CHECKSUM,
        },
        "workbench_contract_schema": workbench_contract_schema,
        "workbench_tool_count": workbench_tool_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_identity_is_complete() {
        assert_eq!(VERSION, "1.0.0");
        assert!(GIT_COMMIT == "unknown" || GIT_COMMIT.len() == 40);
        assert_eq!(CARGO_LOCK_SHA256.len(), 64);
        assert_eq!(HOLT_VERSION, "0.8.5");
        assert_eq!(
            HOLT_SOURCE,
            "registry+https://github.com/rust-lang/crates.io-index"
        );
        assert_eq!(HOLT_CHECKSUM.len(), 64);
    }
}
