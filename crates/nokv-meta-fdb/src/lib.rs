/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! FoundationDB adapter for the NoKV metadata store contract.
//!
//! The default build exposes configuration and physical-envelope validation
//! without importing or linking the FoundationDB client. Enable the `fdb`
//! feature to construct an exact-session-fenced [`FdbStore`]. `nokv-server`
//! selects it only in the non-default FDB runtime. That composition remains
//! unqualified until the documented live serving gates pass.

#[cfg(any(feature = "fdb", test))]
mod affected_bytes;
#[cfg(any(feature = "fdb", test))]
mod codec;
#[cfg(any(feature = "fdb", test))]
mod diagnostics;
mod options;
#[cfg(any(feature = "fdb", test))]
mod profile;
#[cfg(feature = "fdb")]
mod store;

#[cfg(feature = "fdb")]
pub use diagnostics::FdbStoreDiagnostics;
#[cfg(feature = "fdb")]
pub use nokv_fdb::{FdbRuntime, FdbRuntimeError};
pub use options::{FdbMetadataSessionFence, FdbOptions};
#[cfg(feature = "fdb")]
pub use profile::FDB_PHYSICAL_TRANSACTION_GUARD_BYTES;
#[cfg(feature = "fdb")]
pub use store::FdbStore;

#[cfg(test)]
mod tests;
