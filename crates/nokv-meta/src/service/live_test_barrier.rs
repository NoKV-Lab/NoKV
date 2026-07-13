//! Explicit opt-in barriers for checked-in live acceptance tests.
//!
//! These hooks are inert unless the corresponding `NOKV_TEST_*` environment
//! variable is set on the server process. They let the live harness exercise a
//! precise distributed interleaving without weakening or changing the durable
//! protocol used in normal deployments.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::MetadError;

const SNAPSHOT_BARRIER_DIR_ENV: &str = "NOKV_TEST_SNAPSHOT_BARRIER_DIR";
const BARRIER_TIMEOUT_MS_ENV: &str = "NOKV_TEST_BARRIER_TIMEOUT_MS";
const DEFAULT_BARRIER_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) fn snapshot(snapshot_id: u64, phase: &str) -> Result<(), MetadError> {
    let Some(directory) = barrier_directory(SNAPSHOT_BARRIER_DIR_ENV)? else {
        return Ok(());
    };
    wait(directory, format!("{snapshot_id}.{phase}"))
}

fn barrier_directory(variable: &str) -> Result<Option<PathBuf>, MetadError> {
    let Some(raw) = std::env::var_os(variable) else {
        return Ok(None);
    };
    let directory = PathBuf::from(raw);
    if !directory.is_absolute() {
        return Err(MetadError::Codec(format!(
            "{variable} must be an absolute path"
        )));
    }
    std::fs::create_dir_all(&directory).map_err(|err| {
        MetadError::Codec(format!(
            "failed to create live-test barrier directory {}: {err}",
            directory.display()
        ))
    })?;
    Ok(Some(directory))
}

fn wait(directory: PathBuf, stem: String) -> Result<(), MetadError> {
    let arm = directory.join(format!("{stem}.arm"));
    if !arm.exists() {
        return Ok(());
    }
    let ready = directory.join(format!("{stem}.ready"));
    let continue_marker = directory.join(format!("{stem}.continue"));
    match OpenOptions::new().write(true).create_new(true).open(&ready) {
        Ok(mut file) => file.write_all(b"ready\n").map_err(|err| {
            MetadError::Codec(format!(
                "failed to publish live-test barrier {}: {err}",
                ready.display()
            ))
        })?,
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => {
            return Err(MetadError::Codec(format!(
                "failed to publish live-test barrier {}: {err}",
                ready.display()
            )))
        }
    }

    let timeout = std::env::var(BARRIER_TIMEOUT_MS_ENV)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_BARRIER_TIMEOUT);
    let deadline = Instant::now() + timeout;
    while !continue_marker.exists() {
        if Instant::now() >= deadline {
            return Err(MetadError::Codec(format!(
                "live-test barrier {} timed out after {} ms",
                ready.display(),
                timeout.as_millis()
            )));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}
