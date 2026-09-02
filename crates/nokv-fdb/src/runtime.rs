/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::{Arc, OnceLock};

use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};

use crate::lifecycle::{RuntimeAcquireError, RuntimeCore, RuntimeRegistry};
use crate::FdbOperationError;

/// Exact FoundationDB API behavior selected by the `fdb-7_3` binding feature.
pub const FDB_API_VERSION: i32 = 730;

static PROCESS_RUNTIME: OnceLock<Arc<RuntimeRegistry<NetworkAutoStop>>> = OnceLock::new();

/// Cloneable guard for the one FoundationDB network runtime in this process.
///
/// The first call starts the network. All later calls share the same guard.
/// Dropping the final runtime, database, and transaction handle stops the
/// network permanently; this process cannot start it again.
#[derive(Clone)]
pub struct FdbRuntime {
    pub(crate) _core: Arc<RuntimeCore<NetworkAutoStop>>,
}

impl FdbRuntime {
    pub fn start() -> Result<Self, FdbRuntimeError> {
        let registry = PROCESS_RUNTIME.get_or_init(|| Arc::new(RuntimeRegistry::new()));
        registry
            .acquire(boot_network)
            .map(|core| Self { _core: core })
            .map_err(|error| match error {
                RuntimeAcquireError::Start(error) => FdbRuntimeError::Start(error),
                RuntimeAcquireError::Stopped => FdbRuntimeError::Stopped,
            })
    }
}

fn boot_network() -> Result<NetworkAutoStop, FdbOperationError> {
    let builder = FdbApiBuilder::default()
        .set_runtime_version(FDB_API_VERSION)
        .build()
        .map_err(|error| {
            FdbOperationError::from_binding("select FoundationDB API version", error)
        })?;
    // SAFETY: RuntimeRegistry creates one process-global guard and marks the
    // runtime permanently stopped before the final guard is dropped. Database
    // and transaction wrappers retain a clone until their FDB handles drop.
    unsafe { builder.boot() }
        .map_err(|error| FdbOperationError::from_binding("start FoundationDB network", error))
}

/// Failure to acquire the process-global network runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FdbRuntimeError {
    Start(FdbOperationError),
    Stopped,
}

impl fmt::Display for FdbRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start(error) => error.fmt(formatter),
            Self::Stopped => formatter.write_str(
                "FoundationDB network runtime has stopped and cannot restart in this process",
            ),
        }
    }
}

impl std::error::Error for FdbRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Start(error) => Some(error),
            Self::Stopped => None,
        }
    }
}
