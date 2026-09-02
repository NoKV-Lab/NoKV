/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! FoundationDB persistence for NoKV's provider-neutral distributed control
//! contract.
//!
//! The default build exercises physical keys, frozen record codecs, ownership
//! transitions, and local monotonic observation without importing or linking
//! the FoundationDB client. Enable `fdb` for the live store implementation.

#[cfg(any(feature = "fdb", test))]
mod codec;
#[cfg(any(feature = "fdb", test))]
mod observer;
mod options;
mod physical_keys;
#[cfg(any(feature = "fdb", test))]
mod session_fence;

#[cfg(feature = "fdb")]
mod store;

pub use options::FdbControlOptions;
pub use physical_keys::FdbControlKeys;
#[cfg(any(feature = "fdb", test))]
pub use session_fence::FdbSessionFence;

#[cfg(feature = "fdb")]
pub use store::FdbControlStore;

#[cfg(test)]
mod tests;
