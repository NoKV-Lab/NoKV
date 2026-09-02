/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Process-global FoundationDB runtime and provider-neutral physical envelope.
//!
//! The default build contains only configuration, prefix, and error contracts;
//! it does not import or link the FoundationDB client. Enable `fdb` to start
//! the one non-restartable network runtime and open database transactions.

mod error;
#[cfg(any(feature = "fdb", test))]
mod lifecycle;
mod options;
mod prefix;

#[cfg(feature = "fdb")]
mod database;
#[cfg(feature = "fdb")]
mod runtime;

pub use error::{classify_error, FdbErrorDisposition, FdbLimit, FdbOperationError};
pub use options::{FdbConfigError, FdbConnectionOptions};
pub use prefix::{
    lexicographic_successor, FdbStorePrefix, FdbSubspace, FdbSubspaceKind,
    FDB_PHYSICAL_ENCODING_VERSION, MAX_STORE_PREFIX_BYTES,
};

#[cfg(feature = "fdb")]
pub use database::{
    FdbDatabase, FdbKeyValue, FdbOpenError, FdbRangePage, FdbRangeRequest, FdbTransaction,
};
#[cfg(feature = "fdb")]
pub use runtime::{FdbRuntime, FdbRuntimeError, FDB_API_VERSION};

#[cfg(test)]
mod tests;
