/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

pub(super) struct EvidenceBundle {
    root: PathBuf,
}

impl EvidenceBundle {
    pub(super) fn create(root: PathBuf) -> Result<Self, String> {
        if root.exists() {
            return Err(format!(
                "evidence directory {} already exists",
                root.display()
            ));
        }
        fs::create_dir_all(&root).map_err(|error| {
            format!(
                "cannot create fresh evidence directory {}: {error}",
                root.display()
            )
        })?;
        for child in ["commands", "owners", "routes", "peers"] {
            fs::create_dir(root.join(child)).map_err(|error| {
                format!("cannot create evidence subdirectory {child:?}: {error}")
            })?;
        }
        Ok(Self { root })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn write_json(
        &self,
        relative: impl AsRef<Path>,
        value: &impl Serialize,
    ) -> Result<(), String> {
        let mut encoded = serde_json::to_vec_pretty(value)
            .map_err(|error| format!("cannot encode evidence JSON: {error}"))?;
        encoded.push(b'\n');
        self.write_bytes(relative, &encoded)
    }

    pub(super) fn write_bytes(
        &self,
        relative: impl AsRef<Path>,
        value: &[u8],
    ) -> Result<(), String> {
        let target = self.resolve(relative.as_ref())?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "cannot create evidence parent {}: {error}",
                    parent.display()
                )
            })?;
        }
        let mut file = File::create(&target)
            .map_err(|error| format!("cannot create evidence {}: {error}", target.display()))?;
        file.write_all(value)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("cannot write evidence {}: {error}", target.display()))
    }

    pub(super) fn finalize(&self, value: &impl Serialize) -> Result<(), String> {
        let target = self.root.join("result.json");
        if target.exists() {
            return Err("terminal qualification result already exists".to_owned());
        }
        let temporary = self
            .root
            .join(format!(".result.json.{}.tmp", std::process::id()));
        let mut encoded = serde_json::to_vec_pretty(value)
            .map_err(|error| format!("cannot encode terminal qualification result: {error}"))?;
        encoded.push(b'\n');
        let mut file = File::options()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| format!("cannot create terminal result: {error}"))?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("cannot write terminal result: {error}"))?;
        fs::rename(&temporary, &target)
            .map_err(|error| format!("cannot publish terminal result atomically: {error}"))?;
        File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("cannot sync evidence directory: {error}"))
    }

    fn resolve(&self, relative: &Path) -> Result<PathBuf, String> {
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(format!(
                "evidence path must be a non-empty relative path: {}",
                relative.display()
            ));
        }
        Ok(self.root.join(relative))
    }
}

pub(super) fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path)
        .map_err(|error| format!("cannot open {} for hashing: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot hash {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(lowercase_hex(&digest.finalize()))
}

pub(super) fn sha256_bytes(value: &[u8]) -> String {
    lowercase_hex(&Sha256::digest(value))
}

pub(super) fn lowercase_hex(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct ResultBody {
        status: &'static str,
    }

    #[test]
    fn terminal_result_exists_only_after_atomic_finalize() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("evidence");
        let bundle = EvidenceBundle::create(root.clone()).unwrap();
        bundle.write_bytes("commands/setup.txt", b"ok\n").unwrap();
        assert!(!root.join("result.json").exists());
        bundle.finalize(&ResultBody { status: "PASS" }).unwrap();
        assert!(root.join("result.json").is_file());
        assert!(bundle.finalize(&ResultBody { status: "PASS" }).is_err());
    }

    #[test]
    fn evidence_paths_cannot_escape_the_bundle() {
        let temporary = tempfile::tempdir().unwrap();
        let bundle = EvidenceBundle::create(temporary.path().join("evidence")).unwrap();
        assert!(bundle.write_bytes("../outside", b"no").is_err());
        assert!(bundle.write_bytes("", b"no").is_err());
    }
}
