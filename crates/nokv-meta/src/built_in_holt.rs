/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Local object-identity guard for NoKV's built-in Holt adapter.
//!
//! This is deliberately separate from the provider-neutral workspace commit
//! acknowledgement contract. Unix directory and lock-file identities are
//! properties of the local Holt runtime, not metadata-schema semantics.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::provider::v1::{ProviderError, ProviderOperationV1};
use crate::workspace::provider::HoltProviderFactory;
use crate::workspace::MetadataCommitRecoveryFenceFactoryV1;

/// Return a configured in-memory built-in Holt factory behind provider SPI v1.
///
/// The factory owns physical provider setup only. The semantic facade retains
/// schema initialization, authority, recovery, and acknowledgement sinks.
#[must_use]
pub fn memory_provider_factory_v1() -> Arc<dyn MetadataCommitRecoveryFenceFactoryV1> {
    Arc::new(HoltProviderFactory::memory())
}

/// Return a configured file-backed built-in Holt factory behind provider SPI v1.
///
/// `runtime_guard` remains adapter-specific and is captured before any generic
/// create or reopen request. An acknowledgement sink is deliberately not part
/// of this factory boundary.
pub fn file_provider_factory_v1(
    path: impl AsRef<Path>,
    runtime_guard: Arc<dyn HoltRuntimeGuard>,
) -> Arc<dyn MetadataCommitRecoveryFenceFactoryV1> {
    Arc::new(HoltProviderFactory::file(path.as_ref(), runtime_guard))
}

/// Acquire one exact existing Holt store before constructing its provider
/// factory.
///
/// The returned carrier is opaque and intentionally not [`Clone`]. It holds
/// Holt's exclusive reservation for the expected directory and `store.lock`
/// kernel objects. Passing it to
/// [`reserved_existing_file_provider_factory_v1`] moves that exact carrier
/// behind the provider-v1 type-erasure boundary; no raw descriptor or Holt
/// reservation type crosses the `nokv-meta` boundary.
///
/// This is only the local existing-store reopen primitive. It does not
/// qualify any owner-admission transition or prove an exact-resume control,
/// journal, and receipt contract.
pub fn acquire_existing_file_store_reservation_v1(
    expected: HoltStoreObjectIdentity,
) -> Result<HoltExistingStoreReservation, ProviderError> {
    HoltExistingStoreReservation::acquire(expected)
}

/// Build a provider-v1 factory that can reopen only the exact store already
/// held by `reservation`.
///
/// This path never falls back to [`file_provider_factory_v1`],
/// [`holt::DB::open`], or a second lock acquisition. Factory clones share one
/// carrier state and only one successful provider delivery is permitted.
/// Only this reserved-existing factory allocation exposes a supported
/// old-dispatch-exclusion installation capability; ordinary memory and path
/// factories report it as unsupported.
/// Owner admission remains independently not qualified until its durable
/// plan, control outcome, journal, and receipt invariants are proven.
#[must_use]
pub fn reserved_existing_file_provider_factory_v1(
    reservation: HoltExistingStoreReservation,
    runtime_guard: Arc<dyn HoltRuntimeGuard>,
) -> Arc<dyn MetadataCommitRecoveryFenceFactoryV1> {
    Arc::new(HoltProviderFactory::reserved_existing(
        reservation,
        runtime_guard,
    ))
}

#[derive(Clone, PartialEq, Eq)]
pub struct HoltStoreObjectIdentity {
    canonical_locator: PathBuf,
    directory_device: u64,
    directory_inode: u64,
    lock_device: u64,
    lock_inode: u64,
}

impl fmt::Debug for HoltStoreObjectIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HoltStoreObjectIdentity")
            .field("directory_device", &self.directory_device)
            .field("directory_inode", &self.directory_inode)
            .field("lock_device", &self.lock_device)
            .field("lock_inode", &self.lock_inode)
            .finish_non_exhaustive()
    }
}

impl HoltStoreObjectIdentity {
    /// Reconstruct an identity previously obtained from
    /// [`HoltRuntimeGuard::bind_store`].
    ///
    /// These values are only an expected fence. Constructing this value does
    /// not grant authority: reservation acquisition verifies every component
    /// against descriptors held by Holt before returning a carrier.
    #[must_use]
    pub fn from_parts(
        canonical_locator: PathBuf,
        directory_device: u64,
        directory_inode: u64,
        lock_device: u64,
        lock_inode: u64,
    ) -> Self {
        Self {
            canonical_locator,
            directory_device,
            directory_inode,
            lock_device,
            lock_inode,
        }
    }

    pub(crate) fn from_holt(
        canonical_locator: PathBuf,
        identity: holt::FileStoreObjectIdentity,
    ) -> Self {
        Self::from_parts(
            canonical_locator,
            identity.directory_device,
            identity.directory_inode,
            identity.lock_device,
            identity.lock_inode,
        )
    }

    pub(crate) const fn holt_identity(&self) -> holt::FileStoreObjectIdentity {
        holt::FileStoreObjectIdentity {
            directory_device: self.directory_device,
            directory_inode: self.directory_inode,
            lock_device: self.lock_device,
            lock_inode: self.lock_inode,
        }
    }

    pub fn canonical_locator(&self) -> &Path {
        &self.canonical_locator
    }

    pub const fn directory_device(&self) -> u64 {
        self.directory_device
    }

    pub const fn directory_inode(&self) -> u64 {
        self.directory_inode
    }

    pub const fn lock_device(&self) -> u64 {
        self.lock_device
    }

    pub const fn lock_inode(&self) -> u64 {
        self.lock_inode
    }
}

/// Opaque exclusive authority to reopen one existing file-backed Holt store.
///
/// This carrier is deliberately non-Clone. Before factory construction it is
/// the only NoKV value that owns the pre-open exclusion lock. After factory
/// construction, every clone of the type-erased factory shares the same
/// single-consumption state.
pub struct HoltExistingStoreReservation {
    reservation: holt::FileStoreReservation,
    expected: HoltStoreObjectIdentity,
}

impl HoltExistingStoreReservation {
    fn acquire(expected: HoltStoreObjectIdentity) -> Result<Self, ProviderError> {
        let reservation = holt::FileStoreReservation::acquire_existing(
            expected.canonical_locator.clone(),
            expected.holt_identity(),
        )
        .map_err(existing_reservation_error)?;
        Ok(Self {
            reservation,
            expected,
        })
    }

    /// Return the exact expected identity verified when this carrier was
    /// acquired.
    #[must_use]
    pub const fn expected_identity(&self) -> &HoltStoreObjectIdentity {
        &self.expected
    }

    /// Return whether the held Holt reservation remains ready for adoption.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.reservation.is_ready()
    }

    pub(crate) fn reservation_mut(&mut self) -> &mut holt::FileStoreReservation {
        &mut self.reservation
    }
}

impl fmt::Debug for HoltExistingStoreReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HoltExistingStoreReservation")
            .field("expected", &self.expected)
            .field("ready", &self.reservation.is_ready())
            .finish_non_exhaustive()
    }
}

fn existing_reservation_error(error: holt::Error) -> ProviderError {
    match error {
        holt::Error::FileStoreIdentityMismatch { .. } => {
            ProviderError::authority_mismatch(ProviderOperationV1::Reopen)
        }
        holt::Error::BlobStoreIo(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::AlreadyExists
                    | std::io::ErrorKind::InvalidInput
                    | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            ProviderError::authority_mismatch(ProviderOperationV1::Reopen)
        }
        holt::Error::BlobStoreIo(_) => ProviderError::unavailable(ProviderOperationV1::Reopen),
        error => ProviderError::backend(ProviderOperationV1::Reopen, error),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoltRuntimeGuardError {
    Rejected,
    Poisoned,
}

impl fmt::Display for HoltRuntimeGuardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected => formatter.write_str("Holt runtime guard rejected access"),
            Self::Poisoned => formatter.write_str("Holt runtime guard is poisoned"),
        }
    }
}

impl std::error::Error for HoltRuntimeGuardError {}

pub trait HoltRuntimeGuard: Send + Sync {
    /// Bind this guard to the exact held store identity.
    ///
    /// An exact replay is required to be idempotent. Provider reopen can
    /// successfully bind and then fail a later runtime or schema validation;
    /// retrying that same held DB calls `bind_store` again with the identical
    /// identity. Implementations must continue to reject every different
    /// identity.
    fn bind_store(&self, identity: &HoltStoreObjectIdentity) -> Result<(), HoltRuntimeGuardError>;

    fn validate_runtime(&self) -> Result<(), HoltRuntimeGuardError>;

    fn poison(&self);
}

#[derive(Default)]
pub(crate) struct NoopHoltRuntimeGuard;

impl HoltRuntimeGuard for NoopHoltRuntimeGuard {
    fn bind_store(&self, _identity: &HoltStoreObjectIdentity) -> Result<(), HoltRuntimeGuardError> {
        Ok(())
    }

    fn validate_runtime(&self) -> Result<(), HoltRuntimeGuardError> {
        Ok(())
    }

    fn poison(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::admission::admit_provider_offer_v1;

    #[test]
    fn public_factory_entrypoints_are_trait_objects_and_do_not_open_on_offer() {
        let schema = crate::workspace::canonical_provider_schema_v1();
        for factory in [
            memory_provider_factory_v1(),
            file_provider_factory_v1("unused-metadata-location", Arc::new(NoopHoltRuntimeGuard)),
        ] {
            let offer = factory.contract_offer(&schema).unwrap();
            assert!(admit_provider_offer_v1(&schema, &offer).is_qualified());
        }
    }

    #[test]
    fn store_identity_debug_redacts_the_canonical_locator() {
        let identity = HoltStoreObjectIdentity::from_parts(
            PathBuf::from("/private/metadata/SECRET-LOCATOR-SENTINEL"),
            11,
            12,
            13,
            14,
        );

        let debug = format!("{identity:?}");
        assert!(!debug.contains("SECRET-LOCATOR-SENTINEL"));
        assert!(!debug.contains("/private/metadata"));
        assert!(debug.contains("directory_inode: 12"));
        assert!(debug.contains("lock_inode: 14"));
    }
}
