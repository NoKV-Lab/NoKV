/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

const CRATES_IO_SOURCE: &str = "registry+https://github.com/rust-lang/crates.io-index";

#[derive(Debug, PartialEq, Eq)]
struct LockedPackage {
    version: String,
    source: String,
    checksum: String,
}

fn main() {
    println!("cargo:rerun-if-env-changed=NOKV_BUILD_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=NOKV_BUILD_CARGO_LOCK_SHA256");
    println!("cargo:rerun-if-env-changed=NOKV_BUILD_HOLT_VERSION");
    println!("cargo:rerun-if-env-changed=NOKV_BUILD_HOLT_SOURCE");
    println!("cargo:rerun-if-env-changed=NOKV_BUILD_HOLT_CHECKSUM");

    let manifest_directory =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("Cargo sets manifest dir"));
    let repository = manifest_directory.join("../..");
    let lock_path = build_lock_path(&manifest_directory);
    println!("cargo:rerun-if-changed={}", lock_path.display());

    let lock = fs::read(&lock_path).expect("NoKV builds require a source Cargo.lock");
    let lock_sha256 = encode_hex(&Sha256::digest(&lock));
    require_matching_override("NOKV_BUILD_CARGO_LOCK_SHA256", &lock_sha256);

    let lock_text = std::str::from_utf8(&lock).expect("Cargo.lock must be UTF-8");
    let holt = locked_package(lock_text, "holt");
    assert_eq!(
        holt.source, CRATES_IO_SOURCE,
        "Holt must resolve from canonical crates.io"
    );
    assert!(
        is_lower_hex(&holt.checksum, 64),
        "Holt checksum must be 64 lowercase hexadecimal characters"
    );
    require_matching_override("NOKV_BUILD_HOLT_VERSION", &holt.version);
    require_matching_override("NOKV_BUILD_HOLT_SOURCE", &holt.source);
    require_matching_override("NOKV_BUILD_HOLT_CHECKSUM", &holt.checksum);

    let source_commit = if is_workspace_source(&manifest_directory) {
        git_commit(&repository)
    } else {
        cargo_vcs_commit(&manifest_directory)
    };
    let release_commit = env::var("NOKV_BUILD_GIT_COMMIT").ok();
    if let (Some(expected), Some(actual)) = (&release_commit, &source_commit) {
        assert_eq!(
            expected, actual,
            "NOKV_BUILD_GIT_COMMIT does not match the checked-out commit"
        );
    }
    let commit = release_commit
        .or(source_commit)
        .unwrap_or_else(|| "unknown".to_owned());
    assert!(
        commit == "unknown" || is_lower_hex(&commit, 40),
        "NoKV git commit must be 40 lowercase hexadecimal characters"
    );

    emit("NOKV_GIT_COMMIT", &commit);
    emit("NOKV_CARGO_LOCK_SHA256", &lock_sha256);
    emit("NOKV_HOLT_VERSION", &holt.version);
    emit("NOKV_HOLT_SOURCE", &holt.source);
    emit("NOKV_HOLT_CHECKSUM", &holt.checksum);
}

fn build_lock_path(manifest_directory: &Path) -> PathBuf {
    // `cargo package` verifies `nokv` as a standalone source tree with its own
    // generated lockfile; complete workspace sources retain the root lockfile.
    let workspace_lock = manifest_directory.join("../..").join("Cargo.lock");
    if is_workspace_source(manifest_directory) {
        return workspace_lock;
    }
    manifest_directory.join("Cargo.lock")
}

fn is_workspace_source(manifest_directory: &Path) -> bool {
    manifest_directory.ends_with(Path::new("crates/nokv"))
        && manifest_directory
            .join("../..")
            .join("Cargo.lock")
            .is_file()
}

fn locked_package(lock: &str, package_name: &str) -> LockedPackage {
    let matches = lock
        .split("[[package]]")
        .skip(1)
        .filter_map(|section| {
            let name = toml_string(section, "name")?;
            (name == package_name).then(|| LockedPackage {
                version: toml_string(section, "version")
                    .expect("locked package must have a version"),
                source: toml_string(section, "source")
                    .expect("locked registry package must have a source"),
                checksum: toml_string(section, "checksum")
                    .expect("locked registry package must have a checksum"),
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "Cargo.lock must contain exactly one {package_name} package"
    );
    matches.into_iter().next().expect("one package exists")
}

fn toml_string(section: &str, key: &str) -> Option<String> {
    section.lines().find_map(|line| {
        let (candidate, value) = line.split_once('=')?;
        if candidate.trim() != key {
            return None;
        }
        let value = value.trim();
        value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .map(ToOwned::to_owned)
    })
}

fn require_matching_override(name: &str, actual: &str) {
    if let Ok(expected) = env::var(name) {
        assert_eq!(expected, actual, "{name} does not match Cargo.lock");
    }
}

fn git_commit(repository: &Path) -> Option<String> {
    for identity_path in git_identity_paths(repository) {
        println!("cargo:rerun-if-changed={}", identity_path.display());
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repository)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?;
    let commit = commit.trim().to_owned();
    is_lower_hex(&commit, 40).then_some(commit)
}

fn cargo_vcs_commit(manifest_directory: &Path) -> Option<String> {
    let path = manifest_directory.join(".cargo_vcs_info.json");
    println!("cargo:rerun-if-changed={}", path.display());
    let value = fs::read_to_string(path).ok()?;
    vcs_commit(&value)
}

fn vcs_commit(value: &str) -> Option<String> {
    let (_, suffix) = value.split_once("\"sha1\"")?;
    let (_, suffix) = suffix.split_once(':')?;
    let suffix = suffix.trim_start().strip_prefix('"')?;
    let (commit, _) = suffix.split_once('"')?;
    is_lower_hex(commit, 40).then(|| commit.to_owned())
}

fn git_identity_paths(repository: &Path) -> Vec<PathBuf> {
    let mut names = vec!["HEAD".to_owned(), "packed-refs".to_owned()];
    if let Ok(output) = Command::new("git")
        .args(["symbolic-ref", "--quiet", "HEAD"])
        .current_dir(repository)
        .output()
    {
        if output.status.success() {
            if let Ok(reference) = String::from_utf8(output.stdout) {
                names.push(reference.trim().to_owned());
            }
        }
    }
    names
        .into_iter()
        .filter_map(|name| {
            let output = Command::new("git")
                .args(["rev-parse", "--git-path", &name])
                .current_dir(repository)
                .output()
                .ok()?;
            if !output.status.success() {
                return None;
            }
            let value = String::from_utf8(output.stdout).ok()?;
            let path = PathBuf::from(value.trim());
            Some(if path.is_absolute() {
                path
            } else {
                repository.join(path)
            })
        })
        .collect()
}

fn emit(name: &str, value: &str) {
    assert!(
        !value.contains(['\n', '\r', '\0']),
        "build identity cannot contain control characters"
    );
    println!("cargo:rustc-env={name}={value}");
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_exact_locked_package() {
        let lock = r#"
[[package]]
name = "holt"
version = "0.8.4"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
"#;
        assert_eq!(
            locked_package(lock, "holt"),
            LockedPackage {
                version: "0.8.4".to_owned(),
                source: CRATES_IO_SOURCE.to_owned(),
                checksum: "a".repeat(64),
            }
        );
    }

    #[test]
    fn extracts_packaged_vcs_commit() {
        let value = r#"{
  "git": {
    "sha1": "0123456789abcdef0123456789abcdef01234567"
  },
  "path_in_vcs": "crates/nokv"
}"#;
        assert_eq!(
            vcs_commit(value),
            Some("0123456789abcdef0123456789abcdef01234567".to_owned())
        );
        assert_eq!(vcs_commit(r#"{"git":{"sha1":"unknown"}}"#), None);
    }
}
