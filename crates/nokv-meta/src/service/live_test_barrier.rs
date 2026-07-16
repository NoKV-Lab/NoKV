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

#[cfg(test)]
use std::cell::Cell;

use super::MetadError;

const SNAPSHOT_BARRIER_DIR_ENV: &str = "NOKV_TEST_SNAPSHOT_BARRIER_DIR";
const RESTORE_BARRIER_DIR_ENV: &str = "NOKV_TEST_RESTORE_BARRIER_DIR";
const BARRIER_TIMEOUT_MS_ENV: &str = "NOKV_TEST_BARRIER_TIMEOUT_MS";
const DEFAULT_BARRIER_TIMEOUT: Duration = Duration::from_secs(60);
const RESTORE_OPERATION_DIGEST_HEX_LEN: usize = 64;
const MAX_RESTORE_BARRIER_BATCH_INDEX: u64 = 999_999;

#[cfg(test)]
thread_local! {
    static TEST_ATTACH_APPLIED_CALLS: Cell<u64> = const { Cell::new(0) };
}

/// A restore phase that may only be observed after its metadata command has
/// durably applied. Keeping PUT boundaries out of this enum makes it harder to
/// accidentally move an object-store crash point across its write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreAppliedPhase {
    Hold,
    MaterializeBatch(u64),
    ReferenceBatch(u64),
    ReferencesSealed,
    IndexSealed,
    Attach,
    CleanupBatch(u64),
    ReleaseBatch(u64),
}

/// The explicit side of an initialization object PUT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreInitializationPutBoundary {
    Before,
    After,
}

pub(super) fn snapshot(snapshot_id: u64, phase: &str) -> Result<(), MetadError> {
    let Some(directory) = barrier_directory(SNAPSHOT_BARRIER_DIR_ENV)? else {
        return Ok(());
    };
    wait(directory, format!("{snapshot_id}.{phase}"))
}

/// Wait at a restore crash point after the corresponding metadata command has
/// durably applied. Callers must place this after `commit_metadata` succeeds;
/// in particular, `Attach` belongs after apply and before the RPC ACK.
pub(super) fn restore_applied(
    operation_id: &str,
    phase: RestoreAppliedPhase,
) -> Result<(), MetadError> {
    #[cfg(test)]
    if phase == RestoreAppliedPhase::Attach {
        TEST_ATTACH_APPLIED_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    }
    let Some(directory) = barrier_directory(RESTORE_BARRIER_DIR_ENV)? else {
        return Ok(());
    };
    let operation_id = validated_restore_operation_id(operation_id)?;
    wait(
        directory,
        format!("{operation_id}.{}", restore_applied_phase_name(phase)?),
    )
}

#[cfg(test)]
pub(super) fn reset_test_attach_applied_calls() {
    TEST_ATTACH_APPLIED_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(super) fn test_attach_applied_calls() -> u64 {
    TEST_ATTACH_APPLIED_CALLS.with(Cell::get)
}

/// Wait immediately before or after an initialization object PUT. The same
/// deterministic batch index must be used on both sides of one PUT.
pub(super) fn restore_initialization_put(
    operation_id: &str,
    batch_index: u64,
    boundary: RestoreInitializationPutBoundary,
) -> Result<(), MetadError> {
    let Some(directory) = barrier_directory(RESTORE_BARRIER_DIR_ENV)? else {
        return Ok(());
    };
    let operation_id = validated_restore_operation_id(operation_id)?;
    let batch_index = restore_batch_index(batch_index)?;
    let boundary = match boundary {
        RestoreInitializationPutBoundary::Before => "before",
        RestoreInitializationPutBoundary::After => "after",
    };
    wait(
        directory,
        format!("{operation_id}.initialization-put-{boundary}-{batch_index}"),
    )
}

fn validated_restore_operation_id(operation_id: &str) -> Result<&str, MetadError> {
    let Some(digest) = operation_id.strip_prefix("restore-") else {
        return Err(MetadError::Codec(
            "live-test restore barrier operation id must start with restore-".to_owned(),
        ));
    };
    if digest.len() != RESTORE_OPERATION_DIGEST_HEX_LEN
        || !digest
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(MetadError::Codec(
            "live-test restore barrier operation id must contain 64 lowercase hex digits"
                .to_owned(),
        ));
    }
    Ok(operation_id)
}

fn restore_applied_phase_name(phase: RestoreAppliedPhase) -> Result<String, MetadError> {
    Ok(match phase {
        RestoreAppliedPhase::Hold => "hold-applied".to_owned(),
        RestoreAppliedPhase::MaterializeBatch(index) => {
            format!("materialize-batch-{}", restore_batch_index(index)?)
        }
        RestoreAppliedPhase::ReferenceBatch(index) => {
            format!("reference-batch-{}", restore_batch_index(index)?)
        }
        RestoreAppliedPhase::ReferencesSealed => "references-sealed".to_owned(),
        RestoreAppliedPhase::IndexSealed => "index-sealed".to_owned(),
        RestoreAppliedPhase::Attach => "attach-applied".to_owned(),
        RestoreAppliedPhase::CleanupBatch(index) => {
            format!("cleanup-batch-{}", restore_batch_index(index)?)
        }
        RestoreAppliedPhase::ReleaseBatch(index) => {
            format!("release-batch-{}", restore_batch_index(index)?)
        }
    })
}

fn restore_batch_index(index: u64) -> Result<String, MetadError> {
    if index > MAX_RESTORE_BARRIER_BATCH_INDEX {
        return Err(MetadError::Codec(format!(
            "live-test restore barrier batch index {index} exceeds {MAX_RESTORE_BARRIER_BATCH_INDEX}"
        )));
    }
    Ok(format!("{index:06}"))
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

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};
    use std::thread;

    use tempfile::TempDir;

    use super::*;

    const OPERATION_ID: &str =
        "restore-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct RestoreBarrierEnvironment {
        _guard: MutexGuard<'static, ()>,
        directory: TempDir,
        previous_directory: Option<std::ffi::OsString>,
        previous_timeout: Option<std::ffi::OsString>,
    }

    impl RestoreBarrierEnvironment {
        fn enabled() -> Self {
            let guard = ENV_LOCK.lock().expect("restore barrier env lock");
            let directory = tempfile::tempdir().expect("restore barrier tempdir");
            let previous_directory = std::env::var_os(RESTORE_BARRIER_DIR_ENV);
            let previous_timeout = std::env::var_os(BARRIER_TIMEOUT_MS_ENV);
            std::env::set_var(RESTORE_BARRIER_DIR_ENV, directory.path());
            std::env::remove_var(BARRIER_TIMEOUT_MS_ENV);
            Self {
                _guard: guard,
                directory,
                previous_directory,
                previous_timeout,
            }
        }

        fn path(&self, phase: &str, suffix: &str) -> PathBuf {
            self.directory
                .path()
                .join(format!("{OPERATION_ID}.{phase}.{suffix}"))
        }
    }

    impl Drop for RestoreBarrierEnvironment {
        fn drop(&mut self) {
            restore_environment_variable(RESTORE_BARRIER_DIR_ENV, &self.previous_directory);
            restore_environment_variable(BARRIER_TIMEOUT_MS_ENV, &self.previous_timeout);
        }
    }

    fn restore_environment_variable(variable: &str, previous: &Option<std::ffi::OsString>) {
        match previous {
            Some(value) => std::env::set_var(variable, value),
            None => std::env::remove_var(variable),
        }
    }

    #[test]
    fn restore_phase_names_match_live_harness_contract() {
        let cases = [
            (RestoreAppliedPhase::Hold, "hold-applied"),
            (
                RestoreAppliedPhase::MaterializeBatch(0),
                "materialize-batch-000000",
            ),
            (
                RestoreAppliedPhase::MaterializeBatch(12),
                "materialize-batch-000012",
            ),
            (
                RestoreAppliedPhase::ReferenceBatch(1),
                "reference-batch-000001",
            ),
            (RestoreAppliedPhase::ReferencesSealed, "references-sealed"),
            (RestoreAppliedPhase::IndexSealed, "index-sealed"),
            (RestoreAppliedPhase::Attach, "attach-applied"),
            (RestoreAppliedPhase::CleanupBatch(0), "cleanup-batch-000000"),
            (
                RestoreAppliedPhase::ReleaseBatch(999_999),
                "release-batch-999999",
            ),
        ];
        for (phase, expected) in cases {
            assert_eq!(restore_applied_phase_name(phase).unwrap(), expected);
        }
    }

    #[test]
    fn restore_applied_publishes_ready_and_waits_for_continue() {
        let environment = RestoreBarrierEnvironment::enabled();
        let arm = environment.path("materialize-batch-000007", "arm");
        std::fs::write(&arm, b"armed\n").unwrap();

        let waiter = thread::spawn(|| {
            restore_applied(OPERATION_ID, RestoreAppliedPhase::MaterializeBatch(7))
        });
        let ready = environment.path("materialize-batch-000007", "ready");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready.exists() {
            assert!(
                Instant::now() < deadline,
                "restore barrier did not become ready"
            );
            thread::sleep(Duration::from_millis(2));
        }
        assert!(!waiter.is_finished());
        std::fs::write(
            environment.path("materialize-batch-000007", "continue"),
            b"continue\n",
        )
        .unwrap();
        waiter.join().unwrap().unwrap();
    }

    #[test]
    fn restore_initialization_put_uses_explicit_boundaries() {
        let environment = RestoreBarrierEnvironment::enabled();
        for (boundary, phase) in [
            (
                RestoreInitializationPutBoundary::Before,
                "initialization-put-before-000003",
            ),
            (
                RestoreInitializationPutBoundary::After,
                "initialization-put-after-000003",
            ),
        ] {
            std::fs::write(environment.path(phase, "arm"), b"armed\n").unwrap();
            std::fs::write(environment.path(phase, "continue"), b"continue\n").unwrap();
            restore_initialization_put(OPERATION_ID, 3, boundary).unwrap();
            assert!(environment.path(phase, "ready").exists());
        }
    }

    #[test]
    fn restore_barrier_rejects_ambiguous_names_and_indexes() {
        assert!(validated_restore_operation_id("restore-ABC").is_err());
        assert!(validated_restore_operation_id("../../restore-deadbeef").is_err());
        assert!(restore_batch_index(1_000_000).is_err());
    }

    #[test]
    fn disabled_restore_barrier_has_no_filesystem_side_effect() {
        let _guard = ENV_LOCK.lock().expect("restore barrier env lock");
        let previous = std::env::var_os(RESTORE_BARRIER_DIR_ENV);
        std::env::remove_var(RESTORE_BARRIER_DIR_ENV);
        assert!(restore_applied("not-an-operation-id", RestoreAppliedPhase::Hold).is_ok());
        restore_environment_variable(RESTORE_BARRIER_DIR_ENV, &previous);
    }
}
