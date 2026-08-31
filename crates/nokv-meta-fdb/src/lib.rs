/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! FoundationDB characterization adapter for the NoKV metadata store contract.
//!
//! The default build exposes configuration and physical-envelope validation
//! without importing or linking the FoundationDB client. Enable the `fdb`
//! feature to construct [`FdbStore`]. This adapter is not qualified for NoKV
//! serving and is not wired into `nokv-server`.

#[cfg(any(feature = "fdb", test))]
mod affected_bytes;
#[cfg(any(feature = "fdb", test))]
mod codec;
mod options;
#[cfg(any(feature = "fdb", test))]
mod profile;
#[cfg(feature = "fdb")]
mod store;

#[cfg(feature = "fdb")]
pub use nokv_fdb::{FdbRuntime, FdbRuntimeError};
pub use options::FdbOptions;
#[cfg(feature = "fdb")]
pub use store::FdbStore;

#[cfg(test)]
mod tests;
