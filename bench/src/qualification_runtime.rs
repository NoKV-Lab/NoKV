/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Retained-evidence and child-process runtime for live qualification workloads.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
#[cfg(feature = "fdb-serve-qualification")]
use std::thread;
#[cfg(feature = "fdb-serve-qualification")]
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

pub(crate) struct EvidenceBundle {
    root: PathBuf,
}

impl EvidenceBundle {
    pub(crate) fn create(root: PathBuf) -> Result<Self, String> {
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
        for child in [
            "commands",
            "faults",
            "owners",
            "peers",
            "routes",
            "snapshots",
        ] {
            fs::create_dir(root.join(child)).map_err(|error| {
                format!("cannot create evidence subdirectory {child:?}: {error}")
            })?;
        }
        Ok(Self { root })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn write_json(
        &self,
        relative: impl AsRef<Path>,
        value: &impl Serialize,
    ) -> Result<(), String> {
        let mut encoded = serde_json::to_vec_pretty(value)
            .map_err(|error| format!("cannot encode evidence JSON: {error}"))?;
        encoded.push(b'\n');
        self.write_bytes(relative, &encoded)
    }

    pub(crate) fn write_bytes(
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

    pub(crate) fn finalize(&self, value: &impl Serialize) -> Result<(), String> {
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

pub(crate) fn sha256_file(path: &Path) -> Result<String, String> {
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

pub(crate) fn sha256_bytes(value: &[u8]) -> String {
    lowercase_hex(&Sha256::digest(value))
}

pub(crate) fn lowercase_hex(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

pub(crate) fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProcessExit {
    pub(crate) name: String,
    pub(crate) pid: u32,
    pub(crate) started_unix_millis: u64,
    pub(crate) ended_unix_millis: Option<u64>,
    pub(crate) killed_by_harness: bool,
    pub(crate) success: bool,
    pub(crate) code: Option<i32>,
}

struct ManagedProcess {
    name: String,
    child: Child,
    started_unix_millis: u64,
    ended_unix_millis: Option<u64>,
    killed_by_harness: bool,
    exit: Option<ExitStatus>,
}

#[derive(Default)]
pub(crate) struct ProcessSet {
    children: Vec<ManagedProcess>,
}

impl ProcessSet {
    pub(crate) fn spawn(
        &mut self,
        name: &str,
        command: &mut Command,
        evidence: &EvidenceBundle,
    ) -> Result<(), String> {
        if self.children.iter().any(|child| child.name == name) {
            return Err(format!("qualification process {name:?} already exists"));
        }
        let stdout_path = evidence.root().join(format!("owners/{name}.stdout"));
        let stderr_path = evidence.root().join(format!("owners/{name}.stderr"));
        let stdout = File::create(&stdout_path)
            .map_err(|error| format!("cannot create {}: {error}", stdout_path.display()))?;
        let stderr = File::create(&stderr_path)
            .map_err(|error| format!("cannot create {}: {error}", stderr_path.display()))?;
        let started_unix_millis = unix_millis();
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| format!("cannot start qualification process {name:?}: {error}"))?;
        self.children.push(ManagedProcess {
            name: name.to_owned(),
            child,
            started_unix_millis,
            ended_unix_millis: None,
            killed_by_harness: false,
            exit: None,
        });
        Ok(())
    }

    pub(crate) fn require_running(&mut self, name: &str) -> Result<(), String> {
        let child = self.child_mut(name)?;
        Self::refresh(child, name)?;
        match child.exit {
            None => Ok(()),
            Some(status) => Err(format!(
                "qualification process {name:?} exited early with {:?}",
                status.code()
            )),
        }
    }

    #[cfg(feature = "fdb-serve-qualification")]
    pub(crate) fn wait_for_exit(&mut self, name: &str, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "process exit deadline overflowed".to_owned())?;
        loop {
            let child = self.child_mut(name)?;
            Self::refresh(child, name)?;
            if child.exit.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for qualification process {name:?} to exit"
                ));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    pub(crate) fn terminate(&mut self, name: &str) -> Result<(), String> {
        let child = self.child_mut(name)?;
        Self::refresh(child, name)?;
        if child.exit.is_none() {
            child.killed_by_harness = true;
            child
                .child
                .kill()
                .map_err(|error| format!("cannot terminate process {name:?}: {error}"))?;
            child.exit = Some(
                child
                    .child
                    .wait()
                    .map_err(|error| format!("cannot reap process {name:?}: {error}"))?,
            );
            child.ended_unix_millis = Some(unix_millis());
        }
        Ok(())
    }

    pub(crate) fn reap_all(&mut self) -> Vec<ProcessExit> {
        for child in &mut self.children {
            if child.exit.is_none() {
                child.exit = child.child.try_wait().ok().flatten();
                if child.exit.is_some() {
                    child.ended_unix_millis = Some(unix_millis());
                }
            }
            if child.exit.is_none() {
                child.killed_by_harness = true;
                let _ = child.child.kill();
                child.exit = child.child.wait().ok();
                child.ended_unix_millis = Some(unix_millis());
            }
        }
        self.children
            .iter()
            .map(|child| {
                let status = child.exit.as_ref();
                ProcessExit {
                    name: child.name.clone(),
                    pid: child.child.id(),
                    started_unix_millis: child.started_unix_millis,
                    ended_unix_millis: child.ended_unix_millis,
                    killed_by_harness: child.killed_by_harness,
                    success: status.is_some_and(ExitStatus::success),
                    code: status.and_then(ExitStatus::code),
                }
            })
            .collect()
    }

    fn refresh(child: &mut ManagedProcess, name: &str) -> Result<(), String> {
        if child.exit.is_none() {
            child.exit = child
                .child
                .try_wait()
                .map_err(|error| format!("cannot inspect process {name:?}: {error}"))?;
            if child.exit.is_some() {
                child.ended_unix_millis = Some(unix_millis());
            }
        }
        Ok(())
    }

    fn child_mut(&mut self, name: &str) -> Result<&mut ManagedProcess, String> {
        self.children
            .iter_mut()
            .find(|child| child.name == name)
            .ok_or_else(|| format!("qualification process {name:?} does not exist"))
    }
}

impl Drop for ProcessSet {
    fn drop(&mut self) {
        let _ = self.reap_all();
    }
}

#[cfg(test)]
mod tests {
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
