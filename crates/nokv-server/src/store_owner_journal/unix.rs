/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Unix installation authority for one store-lifetime owner journal.
//!
//! A caller locator names one stable final directory entry. The host
//! filesystem, through a no-replace directory rename, is the only authority
//! for case- or Unicode-equivalent names. A binding-specific intent may use a
//! different hashed name, but it can never become serving authority until the
//! raw final namespace accepts that directory. The final directory contains an
//! immutable marker, one permanent lock file, an immutable initial-head
//! receipt, and a replaceable journal head. Existing-open never creates or
//! repairs any of them.

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

const CREATION_BINDING_BYTES: usize = 32;
const INSTALLATION_MARKER_MAGIC: &[u8; 16] = b"NOKV-JNL-DIR-V1\0";
const INITIAL_RECEIPT_MAGIC: &[u8; 16] = b"NOKV-JNL-INI-V1\0";
const INSTALLATION_MARKER_DIGEST_DOMAIN: &[u8] =
    b"nokv.server.store-owner-journal-installation-marker.v1\0";
const INITIAL_RECEIPT_DIGEST_DOMAIN: &[u8] =
    b"nokv.server.store-owner-journal-initial-receipt.v1\0";
const INITIAL_HEAD_DIGEST_DOMAIN: &[u8] = b"nokv.server.store-owner-journal-initial-head.v1\0";
const TARGET_NAME_DIGEST_DOMAIN: &[u8] = b"nokv.server.store-owner-journal-target-name.v1\0";
const INTENT_NAME_DOMAIN: &[u8] = b"nokv.server.store-owner-journal-intent-name.v1\0";
const STAGING_NAME_DOMAIN: &[u8] = b"nokv.server.store-owner-journal-staging-name.v1\0";
const LOCATOR_DIGEST_DOMAIN: &[u8] = b"nokv.server.store-owner-journal-locator.v1\0";
const AUTHORITY_IDENTITY_DIGEST_DOMAIN: &[u8] =
    b"nokv.server.store-owner-journal-authority-identity.v1\0";
const INSTALLATION_DIRECTORY_MODE: u32 = 0o700;
const AUTHORITY_FILE_MODE: u32 = 0o600;
const MARKER_LEAF: &[u8] = b"installation.marker";
const LOCK_LEAF: &[u8] = b"authority.lock";
const INITIAL_RECEIPT_LEAF: &[u8] = b"initial-head.receipt";
const HEAD_LEAF: &[u8] = b"head.v4";
const HEAD_TEMP_NAME_DOMAIN: &[u8] = b"nokv.server.store-owner-journal-head-temp.v1\0";
const RANDOM_NONCE_BYTES: usize = 16;
const MAX_TEMP_CREATE_ATTEMPTS: usize = 16;

#[cfg(test)]
std::thread_local! {
    static STABLE_HEAD_READ_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static REPLACE_BEFORE_RENAME_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static REPLACE_AFTER_RENAME_TEST_FAILURE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static FINAL_PUBLISH_AFTER_RENAME_TEST_FAILURE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StoreOwnerJournalAuthorityErrorV4 {
    InvalidLocator,
    InvalidObject,
    InvalidCreationBinding,
    InstallationAbsent,
    CreationConflict,
    InitialHeadMismatch,
    LockContended,
    BindingLost,
    HeadCompareMismatch,
    HeadTooLarge,
    TemporaryNameExhausted,
    UnstableHead,
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    UnsupportedPlatform,
    Io(io::ErrorKind),
}

impl fmt::Display for StoreOwnerJournalAuthorityErrorV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLocator => "store-owner journal locator is invalid",
            Self::InvalidObject => "store-owner journal authority object is invalid",
            Self::InvalidCreationBinding => {
                "store-owner journal creation binding is zero or invalid"
            }
            Self::InstallationAbsent => "store-owner journal installation is absent",
            Self::CreationConflict => {
                "store-owner journal installation belongs to another creation binding"
            }
            Self::InitialHeadMismatch => {
                "store-owner journal initial head does not match its durable receipt"
            }
            Self::LockContended => "store-owner journal authority is already held",
            Self::BindingLost => "store-owner journal authority binding was lost",
            Self::HeadCompareMismatch => {
                "store-owner journal head no longer matches the expected generation"
            }
            Self::HeadTooLarge => "store-owner journal head exceeds its size limit",
            Self::TemporaryNameExhausted => {
                "store-owner journal temporary-name allocation is exhausted"
            }
            Self::UnstableHead => "store-owner journal head changed during a stable read",
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            Self::UnsupportedPlatform => {
                "store-owner journal publication is unsupported on this platform"
            }
            Self::Io(_) => "store-owner journal authority I/O failed",
        })
    }
}

impl std::error::Error for StoreOwnerJournalAuthorityErrorV4 {}

impl From<io::Error> for StoreOwnerJournalAuthorityErrorV4 {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct StoreOwnerJournalCreationBindingV4([u8; CREATION_BINDING_BYTES]);

impl fmt::Debug for StoreOwnerJournalCreationBindingV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreOwnerJournalCreationBindingV4(<redacted>)")
    }
}

impl StoreOwnerJournalCreationBindingV4 {
    /// Construct the durable logical-create identity.
    ///
    /// The caller must derive this value from the complete immutable store
    /// creation intent, including its configured installation locator, and
    /// reuse it only for exact retries of that one logical creation.
    pub(super) fn from_bytes(
        bytes: [u8; CREATION_BINDING_BYTES],
    ) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(StoreOwnerJournalAuthorityErrorV4::InvalidCreationBinding);
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct StoreOwnerJournalObjectIdentityV4 {
    device: u64,
    inode: u64,
}

impl fmt::Debug for StoreOwnerJournalObjectIdentityV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreOwnerJournalObjectIdentityV4")
            .field("device", &self.device)
            .field("inode", &self.inode)
            .finish()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct InstallationMarkerV4 {
    creation_binding: StoreOwnerJournalCreationBindingV4,
    target_name_digest: [u8; 32],
    locator_digest: [u8; 32],
    directory_identity: StoreOwnerJournalObjectIdentityV4,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct InitialHeadReceiptV4 {
    length: u64,
    digest: [u8; 32],
}

struct LocatorContextV4 {
    configured_installation: PathBuf,
    configured_parent: PathBuf,
    canonical_parent: PathBuf,
    requested_final_leaf: OsString,
    parent: File,
    parent_identity: StoreOwnerJournalObjectIdentityV4,
}

struct HeldInstallationCoreV4 {
    configured_installation: PathBuf,
    configured_parent: PathBuf,
    canonical_parent: PathBuf,
    parent: File,
    parent_identity: StoreOwnerJournalObjectIdentityV4,
    directory: File,
    directory_identity: StoreOwnerJournalObjectIdentityV4,
    marker: File,
    marker_identity: StoreOwnerJournalObjectIdentityV4,
    marker_value: InstallationMarkerV4,
    lock: File,
    lock_identity: StoreOwnerJournalObjectIdentityV4,
}

/// Non-cloneable phase-one creation authority.
///
/// `Unpublished` holds the exact intent directory and its original lock OFD.
/// `Published` is an exact response-loss replay of a complete final
/// installation. Neither state exposes an unlock or a clone operation.
#[must_use = "creation authority must be finished or intentionally abandoned"]
pub(super) struct StoreOwnerJournalCreateTokenV4 {
    state: StoreOwnerJournalCreateStateV4,
}

enum StoreOwnerJournalCreateStateV4 {
    Unpublished {
        core: HeldInstallationCoreV4,
        intent_leaf: CString,
    },
    Published(StoreOwnerJournalAuthorityV4),
}

/// Held cross-process authority for one complete final installation.
///
/// The final directory and permanent lock are opened once and retained for the
/// authority's whole lifetime. Head I/O is relative to the held directory;
/// no path-based head open is permitted.
#[must_use = "dropping the authority releases the permanent installation lock"]
pub(super) struct StoreOwnerJournalAuthorityV4 {
    core: HeldInstallationCoreV4,
    canonical_installation: PathBuf,
    final_leaf: CString,
    initial_receipt: File,
    initial_receipt_identity: StoreOwnerJournalObjectIdentityV4,
    initial_receipt_value: InitialHeadReceiptV4,
}

impl fmt::Debug for StoreOwnerJournalCreateTokenV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoreOwnerJournalCreateTokenV4(<redacted>)")
    }
}

impl fmt::Debug for StoreOwnerJournalAuthorityV4 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreOwnerJournalAuthorityV4")
            .field("locator", &"<redacted>")
            .field("directory_identity", &self.core.directory_identity)
            .field("lock_identity", &self.core.lock_identity)
            .finish_non_exhaustive()
    }
}

impl StoreOwnerJournalCreateTokenV4 {
    pub(super) fn canonical_locator_digest(
        &self,
    ) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
        match &self.state {
            StoreOwnerJournalCreateStateV4::Unpublished { core, intent_leaf } => {
                core.validate_at_name(intent_leaf)?;
                Ok(core.marker_value.locator_digest)
            }
            StoreOwnerJournalCreateStateV4::Published(authority) => {
                authority.canonical_locator_digest()
            }
        }
    }

    pub(super) fn authority_identity_digest(
        &self,
    ) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
        match &self.state {
            StoreOwnerJournalCreateStateV4::Unpublished { core, intent_leaf } => {
                core.validate_at_name(intent_leaf)?;
                authority_identity_digest(core)
            }
            StoreOwnerJournalCreateStateV4::Published(authority) => {
                authority.authority_identity_digest()
            }
        }
    }

    /// Bind and publish the immutable initial head, then publish the whole
    /// installation directory through the raw final namespace with NOREPLACE.
    pub(super) fn finish(
        self,
        initial_head: &[u8],
        max_bytes: usize,
    ) -> Result<StoreOwnerJournalAuthorityV4, StoreOwnerJournalAuthorityErrorV4> {
        validate_requested_head(initial_head, max_bytes)?;
        let receipt = InitialHeadReceiptV4::for_head(initial_head)?;
        match self.state {
            StoreOwnerJournalCreateStateV4::Published(authority) => {
                if authority.initial_receipt_value != receipt {
                    return Err(StoreOwnerJournalAuthorityErrorV4::InitialHeadMismatch);
                }
                authority.validate_complete(max_bytes)?;
                Ok(authority)
            }
            StoreOwnerJournalCreateStateV4::Unpublished { core, intent_leaf } => {
                finish_unpublished(core, intent_leaf, initial_head, receipt, max_bytes)
            }
        }
    }
}

impl StoreOwnerJournalAuthorityV4 {
    /// Begin a first-create attempt or classify an exact completed replay.
    ///
    /// A missing final entry is never created in place. Creation first owns a
    /// complete binding-specific intent. Only `finish` can publish that held
    /// directory into the caller's raw final namespace.
    pub(super) fn begin_create(
        installation_path: &Path,
        creation_binding: StoreOwnerJournalCreationBindingV4,
        max_bytes: usize,
    ) -> Result<StoreOwnerJournalCreateTokenV4, StoreOwnerJournalAuthorityErrorV4> {
        if max_bytes == 0 {
            return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
        }
        let context = LocatorContextV4::resolve(installation_path)?;
        if let Some(final_entry) = context.open_final()? {
            let authority =
                open_final_authority(context, final_entry, Some(creation_binding), max_bytes)?;
            return Ok(StoreOwnerJournalCreateTokenV4 {
                state: StoreOwnerJournalCreateStateV4::Published(authority),
            });
        }

        let target_name_digest =
            target_name_digest(&context.canonical_parent, &context.requested_final_leaf)?;
        let intent_leaf = intent_leaf(creation_binding, target_name_digest)?;
        if let Some(directory) =
            open_named_directory_optional(context.parent.as_raw_fd(), &intent_leaf)?
        {
            let core = open_held_core(
                context,
                directory,
                target_name_digest,
                Some(creation_binding),
            )?;
            core.validate_at_name(&intent_leaf)?;
            return Ok(StoreOwnerJournalCreateTokenV4 {
                state: StoreOwnerJournalCreateStateV4::Unpublished { core, intent_leaf },
            });
        }
        create_and_publish_intent(
            context,
            creation_binding,
            target_name_digest,
            intent_leaf,
            max_bytes,
        )
    }

    /// Open only an already-complete final installation.
    ///
    /// This path never calls mkdir, O_CREAT, chmod, rename, unlink, or a
    /// fallback create path. Missing or partial marker/lock/receipt/head state
    /// is rejected without repair.
    pub(super) fn open_existing(
        installation_path: &Path,
        max_bytes: usize,
    ) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        if max_bytes == 0 {
            return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
        }
        let context = LocatorContextV4::resolve(installation_path)?;
        let final_entry = context
            .open_final()?
            .ok_or(StoreOwnerJournalAuthorityErrorV4::InstallationAbsent)?;
        open_final_authority(context, final_entry, None, max_bytes)
    }

    /// Test-only bridge retained for the parent module's characterization
    /// test. Production code must choose `begin_create` or `open_existing`.
    #[cfg(test)]
    pub(super) fn acquire(
        installation_path: &Path,
    ) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        let absolute = std::path::absolute(installation_path)
            .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.server.store-owner-journal-test-bridge.v1\0");
        hash_bytes(&mut hasher, absolute.as_os_str().as_bytes())?;
        let binding = StoreOwnerJournalCreationBindingV4::from_bytes(hasher.finalize().into())?;
        Self::begin_create(installation_path, binding, super::MAX_WIRE_BYTES)?.finish(
            b"test-only-store-owner-journal-head-v4",
            super::MAX_WIRE_BYTES,
        )
    }

    pub(super) fn validate_binding(&self) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
        self.core.validate_at_name(&self.final_leaf)?;
        let current = self
            .core
            .configured_installation
            .canonicalize()
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        if current != self.canonical_installation
            || current.parent() != Some(self.core.canonical_parent.as_path())
            || current.file_name().map(OsStr::as_bytes) != Some(self.final_leaf.as_bytes())
        {
            return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
        }
        let current_leaf = current
            .file_name()
            .ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        let target = target_name_digest(&self.core.canonical_parent, current_leaf)?;
        if target != self.core.marker_value.target_name_digest {
            return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
        }
        validate_immutable_named_file(
            &self.initial_receipt,
            self.core.directory.as_raw_fd(),
            &fixed_leaf(INITIAL_RECEIPT_LEAF)?,
            self.initial_receipt_identity,
            &encode_initial_receipt(self.initial_receipt_value),
        )
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        Ok(())
    }

    pub(super) fn canonical_locator_digest(
        &self,
    ) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
        self.validate_binding()?;
        Ok(self.core.marker_value.locator_digest)
    }

    pub(super) fn authority_identity_digest(
        &self,
    ) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
        self.validate_binding()?;
        authority_identity_digest(&self.core)
    }

    pub(super) fn read_head(
        &self,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, StoreOwnerJournalAuthorityErrorV4> {
        self.read_head_snapshot(max_bytes)
            .map(|(bytes, _)| Some(bytes))
    }

    fn read_head_snapshot(
        &self,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, StoreOwnerJournalHeadShapeV4), StoreOwnerJournalAuthorityErrorV4> {
        if max_bytes == 0 {
            return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
        }
        self.validate_binding()?;
        let head_leaf = fixed_leaf(HEAD_LEAF)?;
        let head = open_named_read(self.core.directory.as_raw_fd(), &head_leaf)?
            .ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        let before = validate_head_shape(&head, max_bytes, true)?;
        validate_named_head(
            self.core.directory.as_raw_fd(),
            &head_leaf,
            before,
            max_bytes,
            true,
        )?;
        let first = read_exact_file(&head, before.length)?;
        #[cfg(test)]
        STABLE_HEAD_READ_TEST_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let middle = validate_head_shape(&head, max_bytes, true)?;
        validate_named_head(
            self.core.directory.as_raw_fd(),
            &head_leaf,
            middle,
            max_bytes,
            true,
        )?;
        let second = read_exact_file(&head, middle.length)?;
        let after = validate_head_shape(&head, max_bytes, true)?;
        validate_named_head(
            self.core.directory.as_raw_fd(),
            &head_leaf,
            after,
            max_bytes,
            true,
        )?;
        self.validate_binding()?;
        if before != middle || middle != after || first != second {
            return Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead);
        }
        Ok((first, after))
    }

    pub(super) fn replace_head(
        &self,
        expected: &[u8],
        next: &[u8],
        max_bytes: usize,
    ) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
        validate_requested_head(expected, max_bytes)?;
        validate_requested_head(next, max_bytes)?;
        let (current, expected_shape) = self.read_head_snapshot(max_bytes)?;
        if current != expected {
            return Err(StoreOwnerJournalAuthorityErrorV4::HeadCompareMismatch);
        }

        let head_leaf = fixed_leaf(HEAD_LEAF)?;
        let (mut temporary, temporary_leaf, temporary_identity) =
            create_unique_head_temp(self.core.directory.as_raw_fd())?;
        let replaced = (|| {
            temporary
                .write_all(next)
                .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            temporary
                .sync_all()
                .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            let temporary_shape = validate_head_shape(&temporary, max_bytes, true)?;
            if temporary_shape.identity != temporary_identity
                || temporary_shape.length != next.len()
                || read_exact_file(&temporary, next.len())? != next
            {
                return Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead);
            }
            validate_named_head(
                self.core.directory.as_raw_fd(),
                &temporary_leaf,
                temporary_shape,
                max_bytes,
                true,
            )?;
            #[cfg(test)]
            REPLACE_BEFORE_RENAME_TEST_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let (still_current, still_shape) = self.read_head_snapshot(max_bytes)?;
            if still_current != expected || still_shape != expected_shape {
                return Err(StoreOwnerJournalAuthorityErrorV4::HeadCompareMismatch);
            }
            self.validate_binding()?;
            rename_replace(
                self.core.directory.as_raw_fd(),
                &temporary_leaf,
                self.core.directory.as_raw_fd(),
                &head_leaf,
            )?;
            #[cfg(test)]
            if REPLACE_AFTER_RENAME_TEST_FAILURE.with(|failure| failure.replace(false)) {
                return Err(StoreOwnerJournalAuthorityErrorV4::Io(io::ErrorKind::Other));
            }
            self.core
                .directory
                .sync_all()
                .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            self.validate_binding()?;
            let (installed, installed_shape) = self.read_head_snapshot(max_bytes)?;
            if installed == next && installed_shape.identity == temporary_identity {
                Ok(())
            } else {
                Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead)
            }
        })();
        if replaced.is_err() {
            let _ = unlink_owned_head_temp(
                self.core.directory.as_raw_fd(),
                &temporary_leaf,
                temporary_identity,
                max_bytes,
            );
        }
        replaced
    }

    fn validate_complete(&self, max_bytes: usize) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
        self.validate_binding()?;
        let head_leaf = fixed_leaf(HEAD_LEAF)?;
        let head = open_named_read(self.core.directory.as_raw_fd(), &head_leaf)?
            .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidObject)?;
        let shape = validate_head_shape(&head, max_bytes, true)?;
        validate_named_head(
            self.core.directory.as_raw_fd(),
            &head_leaf,
            shape,
            max_bytes,
            true,
        )?;
        self.validate_binding()
    }

    #[cfg(test)]
    fn directory_identity(&self) -> StoreOwnerJournalObjectIdentityV4 {
        self.core.directory_identity
    }

    #[cfg(test)]
    fn lock_identity(&self) -> StoreOwnerJournalObjectIdentityV4 {
        self.core.lock_identity
    }

    #[cfg(test)]
    fn lock_path(&self) -> PathBuf {
        self.canonical_installation
            .join(OsStr::from_bytes(LOCK_LEAF))
    }
}

impl InstallationMarkerV4 {
    fn new(
        creation_binding: StoreOwnerJournalCreationBindingV4,
        target_name_digest: [u8; 32],
        canonical_parent: &Path,
        directory_identity: StoreOwnerJournalObjectIdentityV4,
    ) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        if target_name_digest.iter().all(|byte| *byte == 0) {
            return Err(StoreOwnerJournalAuthorityErrorV4::InvalidLocator);
        }
        let locator_digest = locator_digest(
            canonical_parent,
            target_name_digest,
            directory_identity,
            creation_binding,
        )?;
        Ok(Self {
            creation_binding,
            target_name_digest,
            locator_digest,
            directory_identity,
        })
    }

    fn validate(
        self,
        canonical_parent: &Path,
        directory_identity: StoreOwnerJournalObjectIdentityV4,
    ) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
        StoreOwnerJournalCreationBindingV4::from_bytes(self.creation_binding.0)?;
        if self.directory_identity != directory_identity
            || self.locator_digest
                != locator_digest(
                    canonical_parent,
                    self.target_name_digest,
                    directory_identity,
                    self.creation_binding,
                )?
        {
            return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
        }
        Ok(())
    }
}

impl InitialHeadReceiptV4 {
    fn for_head(bytes: &[u8]) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::HeadTooLarge)?;
        let mut hasher = Sha256::new();
        hasher.update(INITIAL_HEAD_DIGEST_DOMAIN);
        hasher.update(length.to_be_bytes());
        hasher.update(bytes);
        Ok(Self {
            length,
            digest: hasher.finalize().into(),
        })
    }
}

impl LocatorContextV4 {
    fn resolve(path: &Path) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        let configured_installation =
            std::path::absolute(path).map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
        let configured_parent = configured_installation
            .parent()
            .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?
            .to_owned();
        let requested_final_leaf = configured_installation
            .file_name()
            .filter(|leaf| !leaf.is_empty())
            .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?
            .to_owned();
        component(&requested_final_leaf)?;
        let canonical_parent = configured_parent
            .canonicalize()
            .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
        let parent = open_directory(&canonical_parent)?;
        let parent_identity = directory_identity(&parent)?;
        validate_current_parent(&configured_parent, &canonical_parent, parent_identity)?;
        Ok(Self {
            configured_installation,
            configured_parent,
            canonical_parent,
            requested_final_leaf,
            parent,
            parent_identity,
        })
    }

    fn open_final(&self) -> Result<Option<ExistingFinalV4>, StoreOwnerJournalAuthorityErrorV4> {
        validate_current_parent(
            &self.configured_parent,
            &self.canonical_parent,
            self.parent_identity,
        )?;
        let requested = component(&self.requested_final_leaf)?;
        let Some(directory) = open_named_directory_optional(self.parent.as_raw_fd(), &requested)?
        else {
            validate_current_parent(
                &self.configured_parent,
                &self.canonical_parent,
                self.parent_identity,
            )?;
            return Ok(None);
        };
        let identity = directory_identity(&directory)?;
        validate_directory_shape(&directory)?;
        let canonical_installation = self
            .configured_installation
            .canonicalize()
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        if canonical_installation.parent() != Some(self.canonical_parent.as_path()) {
            return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
        }
        let actual_leaf = component(
            canonical_installation
                .file_name()
                .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?,
        )?;
        validate_named_directory(self.parent.as_raw_fd(), &actual_leaf, identity)?;
        validate_current_parent(
            &self.configured_parent,
            &self.canonical_parent,
            self.parent_identity,
        )?;
        Ok(Some(ExistingFinalV4 {
            directory,
            canonical_installation,
            actual_leaf,
        }))
    }
}

struct ExistingFinalV4 {
    directory: File,
    canonical_installation: PathBuf,
    actual_leaf: CString,
}

impl HeldInstallationCoreV4 {
    fn validate_at_name(&self, name: &CString) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
        validate_current_parent(
            &self.configured_parent,
            &self.canonical_parent,
            self.parent_identity,
        )?;
        if directory_identity(&self.parent)? != self.parent_identity
            || directory_identity(&self.directory)? != self.directory_identity
        {
            return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
        }
        validate_directory_shape(&self.directory)
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        validate_named_directory(self.parent.as_raw_fd(), name, self.directory_identity)?;
        self.marker_value
            .validate(&self.canonical_parent, self.directory_identity)
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        validate_immutable_named_file(
            &self.marker,
            self.directory.as_raw_fd(),
            &fixed_leaf(MARKER_LEAF)?,
            self.marker_identity,
            &encode_marker(self.marker_value),
        )
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        validate_lock_shape(&self.lock)
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        if regular_file_identity(&self.lock)? != self.lock_identity {
            return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
        }
        validate_named_regular(
            self.directory.as_raw_fd(),
            &fixed_leaf(LOCK_LEAF)?,
            self.lock_identity,
            Some(0),
        )?;
        validate_current_parent(
            &self.configured_parent,
            &self.canonical_parent,
            self.parent_identity,
        )
    }
}

const INSTALLATION_MARKER_WIRE_BYTES: usize = 16 + 32 + 32 + 32 + 8 + 8 + 32;
const INITIAL_RECEIPT_WIRE_BYTES: usize = 16 + 8 + 32 + 32;

fn create_and_publish_intent(
    context: LocatorContextV4,
    creation_binding: StoreOwnerJournalCreationBindingV4,
    target_name_digest: [u8; 32],
    intent_leaf: CString,
    max_bytes: usize,
) -> Result<StoreOwnerJournalCreateTokenV4, StoreOwnerJournalAuthorityErrorV4> {
    let configured_installation = context.configured_installation.clone();
    let (directory, staging_leaf) = create_unique_staging_directory(
        context.parent.as_raw_fd(),
        creation_binding,
        target_name_digest,
    )?;
    validate_owned_directory(&directory)?;
    set_exact_mode(&directory, INSTALLATION_DIRECTORY_MODE)?;
    validate_directory_shape(&directory)?;
    let directory_identity = directory_identity(&directory)?;
    let marker_value = InstallationMarkerV4::new(
        creation_binding,
        target_name_digest,
        &context.canonical_parent,
        directory_identity,
    )?;
    let marker = create_marker(directory.as_raw_fd(), marker_value)?;
    let marker_identity = regular_file_identity(&marker)?;
    let lock = create_permanent_lock(directory.as_raw_fd())?;
    let lock_identity = regular_file_identity(&lock)?;
    directory
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    let core = HeldInstallationCoreV4 {
        configured_installation,
        configured_parent: context.configured_parent,
        canonical_parent: context.canonical_parent,
        parent: context.parent,
        parent_identity: context.parent_identity,
        directory,
        directory_identity,
        marker,
        marker_identity,
        marker_value,
        lock,
        lock_identity,
    };
    core.validate_at_name(&staging_leaf)?;
    if !rename_no_replace(
        core.parent.as_raw_fd(),
        &staging_leaf,
        core.parent.as_raw_fd(),
        &intent_leaf,
    )? {
        let retry_path = core.configured_installation.clone();
        cleanup_unpublished(core, &staging_leaf)?;
        return StoreOwnerJournalAuthorityV4::begin_create(
            &retry_path,
            creation_binding,
            max_bytes,
        );
    }
    core.parent
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    core.validate_at_name(&intent_leaf)?;
    Ok(StoreOwnerJournalCreateTokenV4 {
        state: StoreOwnerJournalCreateStateV4::Unpublished { core, intent_leaf },
    })
}

fn finish_unpublished(
    core: HeldInstallationCoreV4,
    intent_leaf: CString,
    initial_head: &[u8],
    receipt: InitialHeadReceiptV4,
    max_bytes: usize,
) -> Result<StoreOwnerJournalAuthorityV4, StoreOwnerJournalAuthorityErrorV4> {
    core.validate_at_name(&intent_leaf)?;
    ensure_initial_receipt(&core, receipt)?;
    ensure_unpublished_head(&core, initial_head, max_bytes)?;
    core.directory
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    core.validate_at_name(&intent_leaf)?;
    let final_leaf = component(
        core.configured_installation
            .file_name()
            .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?,
    )?;
    if !rename_no_replace(
        core.parent.as_raw_fd(),
        &intent_leaf,
        core.parent.as_raw_fd(),
        &final_leaf,
    )? {
        let path = core.configured_installation.clone();
        let binding = core.marker_value.creation_binding;
        cleanup_unpublished(core, &intent_leaf)?;
        let authority =
            StoreOwnerJournalAuthorityV4::open_existing_for_creation(&path, binding, max_bytes)?;
        validate_initial_receipt(&authority.core, receipt)?;
        return Ok(authority);
    }
    #[cfg(test)]
    if FINAL_PUBLISH_AFTER_RENAME_TEST_FAILURE.with(|failure| failure.replace(false)) {
        return Err(StoreOwnerJournalAuthorityErrorV4::Io(io::ErrorKind::Other));
    }
    core.parent
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    let canonical_installation = core
        .configured_installation
        .canonicalize()
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    if canonical_installation.parent() != Some(core.canonical_parent.as_path()) {
        return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
    }
    let actual_leaf = component(
        canonical_installation
            .file_name()
            .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?,
    )?;
    if target_name_digest(
        &core.canonical_parent,
        OsStr::from_bytes(actual_leaf.as_bytes()),
    )? != core.marker_value.target_name_digest
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
    }
    let authority = build_authority(core, canonical_installation, actual_leaf, max_bytes)?;
    if authority.read_head(max_bytes)?.as_deref() != Some(initial_head) {
        return Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead);
    }
    Ok(authority)
}

impl StoreOwnerJournalAuthorityV4 {
    fn open_existing_for_creation(
        installation_path: &Path,
        creation_binding: StoreOwnerJournalCreationBindingV4,
        max_bytes: usize,
    ) -> Result<Self, StoreOwnerJournalAuthorityErrorV4> {
        let context = LocatorContextV4::resolve(installation_path)?;
        let final_entry = context
            .open_final()?
            .ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
        open_final_authority(context, final_entry, Some(creation_binding), max_bytes)
    }
}

fn open_final_authority(
    context: LocatorContextV4,
    final_entry: ExistingFinalV4,
    expected_binding: Option<StoreOwnerJournalCreationBindingV4>,
    max_bytes: usize,
) -> Result<StoreOwnerJournalAuthorityV4, StoreOwnerJournalAuthorityErrorV4> {
    let target_name_digest = target_name_digest(
        &context.canonical_parent,
        OsStr::from_bytes(final_entry.actual_leaf.as_bytes()),
    )?;
    let core = open_held_core(
        context,
        final_entry.directory,
        target_name_digest,
        expected_binding,
    )?;
    build_authority(
        core,
        final_entry.canonical_installation,
        final_entry.actual_leaf,
        max_bytes,
    )
}

fn build_authority(
    core: HeldInstallationCoreV4,
    canonical_installation: PathBuf,
    final_leaf: CString,
    max_bytes: usize,
) -> Result<StoreOwnerJournalAuthorityV4, StoreOwnerJournalAuthorityErrorV4> {
    let (initial_receipt, initial_receipt_identity, initial_receipt_value) =
        open_initial_receipt(&core)?;
    let initial_length = usize::try_from(initial_receipt_value.length)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::HeadTooLarge)?;
    if initial_length > max_bytes {
        return Err(StoreOwnerJournalAuthorityErrorV4::HeadTooLarge);
    }
    let authority = StoreOwnerJournalAuthorityV4 {
        core,
        canonical_installation,
        final_leaf,
        initial_receipt,
        initial_receipt_identity,
        initial_receipt_value,
    };
    authority.validate_complete(max_bytes)?;
    Ok(authority)
}

fn open_held_core(
    context: LocatorContextV4,
    directory: File,
    expected_target: [u8; 32],
    expected_binding: Option<StoreOwnerJournalCreationBindingV4>,
) -> Result<HeldInstallationCoreV4, StoreOwnerJournalAuthorityErrorV4> {
    validate_directory_shape(&directory)?;
    let directory_identity = directory_identity(&directory)?;
    let marker_leaf = fixed_leaf(MARKER_LEAF)?;
    let marker = open_named_read(directory.as_raw_fd(), &marker_leaf)?
        .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidObject)?;
    let marker_identity = regular_file_identity(&marker)?;
    let marker_value = read_marker(&marker)?;
    marker_value.validate(&context.canonical_parent, directory_identity)?;
    if marker_value.target_name_digest != expected_target {
        return Err(StoreOwnerJournalAuthorityErrorV4::CreationConflict);
    }
    if expected_binding.is_some_and(|binding| binding != marker_value.creation_binding) {
        return Err(StoreOwnerJournalAuthorityErrorV4::CreationConflict);
    }
    validate_named_regular(
        directory.as_raw_fd(),
        &marker_leaf,
        marker_identity,
        Some(INSTALLATION_MARKER_WIRE_BYTES),
    )?;
    let lock_leaf = fixed_leaf(LOCK_LEAF)?;
    let lock = open_named_write(directory.as_raw_fd(), &lock_leaf)?
        .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidObject)?;
    validate_lock_shape(&lock)?;
    acquire_exclusive_lock(&lock)?;
    let lock_identity = regular_file_identity(&lock)?;
    validate_named_regular(directory.as_raw_fd(), &lock_leaf, lock_identity, Some(0))?;
    Ok(HeldInstallationCoreV4 {
        configured_installation: context.configured_installation,
        configured_parent: context.configured_parent,
        canonical_parent: context.canonical_parent,
        parent: context.parent,
        parent_identity: context.parent_identity,
        directory,
        directory_identity,
        marker,
        marker_identity,
        marker_value,
        lock,
        lock_identity,
    })
}

fn ensure_initial_receipt(
    core: &HeldInstallationCoreV4,
    expected: InitialHeadReceiptV4,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let leaf = fixed_leaf(INITIAL_RECEIPT_LEAF)?;
    let bytes = encode_initial_receipt(expected);
    match open_named_write(core.directory.as_raw_fd(), &leaf)? {
        Some(mut existing) => {
            let metadata = validate_regular_base(&existing)?;
            if metadata.len() == INITIAL_RECEIPT_WIRE_BYTES as u64 {
                if read_exact_file(&existing, INITIAL_RECEIPT_WIRE_BYTES)? != bytes {
                    return Err(StoreOwnerJournalAuthorityErrorV4::InitialHeadMismatch);
                }
            } else if metadata.len() < INITIAL_RECEIPT_WIRE_BYTES as u64 {
                existing
                    .set_len(0)
                    .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
                existing
                    .write_all(&bytes)
                    .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            } else {
                return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
            }
            existing
                .sync_all()
                .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            let identity = regular_file_identity(&existing)?;
            validate_named_regular(
                core.directory.as_raw_fd(),
                &leaf,
                identity,
                Some(INITIAL_RECEIPT_WIRE_BYTES),
            )?;
        }
        None => {
            let mut receipt = create_named_file(core.directory.as_raw_fd(), &leaf)?;
            receipt
                .write_all(&bytes)
                .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            receipt
                .sync_all()
                .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
            let identity = regular_file_identity(&receipt)?;
            validate_named_regular(
                core.directory.as_raw_fd(),
                &leaf,
                identity,
                Some(INITIAL_RECEIPT_WIRE_BYTES),
            )?;
        }
    }
    core.directory
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    validate_initial_receipt(core, expected)
}

fn validate_initial_receipt(
    core: &HeldInstallationCoreV4,
    expected: InitialHeadReceiptV4,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    if read_initial_receipt(core)? == expected {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::InitialHeadMismatch)
    }
}

fn read_initial_receipt(
    core: &HeldInstallationCoreV4,
) -> Result<InitialHeadReceiptV4, StoreOwnerJournalAuthorityErrorV4> {
    open_initial_receipt(core).map(|(_, _, receipt)| receipt)
}

fn open_initial_receipt(
    core: &HeldInstallationCoreV4,
) -> Result<
    (
        File,
        StoreOwnerJournalObjectIdentityV4,
        InitialHeadReceiptV4,
    ),
    StoreOwnerJournalAuthorityErrorV4,
> {
    let leaf = fixed_leaf(INITIAL_RECEIPT_LEAF)?;
    let file = open_named_read(core.directory.as_raw_fd(), &leaf)?
        .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidObject)?;
    let identity = regular_file_identity(&file)?;
    validate_named_regular(
        core.directory.as_raw_fd(),
        &leaf,
        identity,
        Some(INITIAL_RECEIPT_WIRE_BYTES),
    )?;
    let receipt = decode_initial_receipt(&read_exact_file(&file, INITIAL_RECEIPT_WIRE_BYTES)?)?;
    Ok((file, identity, receipt))
}

fn ensure_unpublished_head(
    core: &HeldInstallationCoreV4,
    expected: &[u8],
    max_bytes: usize,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let leaf = fixed_leaf(HEAD_LEAF)?;
    let mut head = match open_named_write(core.directory.as_raw_fd(), &leaf)? {
        Some(existing) => existing,
        None => create_named_file(core.directory.as_raw_fd(), &leaf)?,
    };
    validate_regular_base(&head)?;
    let current_length = usize::try_from(
        head.metadata()
            .map_err(StoreOwnerJournalAuthorityErrorV4::from)?
            .len(),
    )
    .map_err(|_| StoreOwnerJournalAuthorityErrorV4::HeadTooLarge)?;
    let already_exact = current_length == expected.len()
        && current_length <= max_bytes
        && read_exact_file(&head, current_length)? == expected;
    if !already_exact {
        head.set_len(0)
            .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
        head.write_all(expected)
            .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    }
    head.sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    let shape = validate_head_shape(&head, max_bytes, true)?;
    if shape.length != expected.len() || read_exact_file(&head, expected.len())? != expected {
        return Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead);
    }
    validate_named_head(core.directory.as_raw_fd(), &leaf, shape, max_bytes, true)?;
    core.directory
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)
}

fn cleanup_unpublished(
    core: HeldInstallationCoreV4,
    name: &CString,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    core.validate_at_name(name)?;
    let mut expected_names = vec![MARKER_LEAF.to_vec(), LOCK_LEAF.to_vec()];
    let receipt_leaf = fixed_leaf(INITIAL_RECEIPT_LEAF)?;
    let receipt = open_named_read(core.directory.as_raw_fd(), &receipt_leaf)?;
    if receipt.is_some() {
        expected_names.push(INITIAL_RECEIPT_LEAF.to_vec());
    }
    let head_leaf = fixed_leaf(HEAD_LEAF)?;
    let head = open_named_read(core.directory.as_raw_fd(), &head_leaf)?;
    if head.is_some() {
        expected_names.push(HEAD_LEAF.to_vec());
    }
    expected_names.sort();
    let mut actual_names = read_directory_names(core.directory.as_raw_fd())?;
    actual_names.sort();
    if actual_names != expected_names {
        return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
    }
    if let Some(head) = head {
        unlink_named_exact(
            core.directory.as_raw_fd(),
            &head_leaf,
            regular_file_identity(&head)?,
        )?;
    }
    if let Some(receipt) = receipt {
        unlink_named_exact(
            core.directory.as_raw_fd(),
            &receipt_leaf,
            regular_file_identity(&receipt)?,
        )?;
    }
    unlink_named_exact(
        core.directory.as_raw_fd(),
        &fixed_leaf(LOCK_LEAF)?,
        core.lock_identity,
    )?;
    unlink_named_exact(
        core.directory.as_raw_fd(),
        &fixed_leaf(MARKER_LEAF)?,
        core.marker_identity,
    )?;
    core.directory
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    validate_named_directory(core.parent.as_raw_fd(), name, core.directory_identity)?;
    // SAFETY: `parent` is a live directory descriptor and `name` is a NUL-terminated
    // single component whose identity was revalidated immediately above.
    let result =
        unsafe { libc::unlinkat(core.parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
    if result != 0 {
        return Err(StoreOwnerJournalAuthorityErrorV4::from(
            io::Error::last_os_error(),
        ));
    }
    core.parent
        .sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    validate_current_parent(
        &core.configured_parent,
        &core.canonical_parent,
        core.parent_identity,
    )
}

fn open_directory(path: &Path) -> Result<File, StoreOwnerJournalAuthorityErrorV4> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    validate_owned_directory(&file)?;
    Ok(file)
}

fn open_named_directory_optional(
    parent_fd: RawFd,
    leaf: &CString,
) -> Result<Option<File>, StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: `parent_fd` is borrowed for the call and `leaf` is a live,
    // NUL-terminated single component. `openat` does not retain either pointer.
    let descriptor = unsafe {
        libc::openat(
            parent_fd,
            leaf.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor >= 0 {
        // SAFETY: a nonnegative descriptor returned by `openat` is uniquely owned
        // here and is transferred exactly once to `File`.
        return Ok(Some(unsafe { File::from_raw_fd(descriptor) }));
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(None)
    } else if matches!(
        error.raw_os_error(),
        Some(libc::ENOTDIR) | Some(libc::ELOOP)
    ) {
        Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::from(error))
    }
}

fn open_named_read(
    parent_fd: RawFd,
    leaf: &CString,
) -> Result<Option<File>, StoreOwnerJournalAuthorityErrorV4> {
    open_named_file_optional(parent_fd, leaf, libc::O_RDONLY)
}

fn open_named_write(
    parent_fd: RawFd,
    leaf: &CString,
) -> Result<Option<File>, StoreOwnerJournalAuthorityErrorV4> {
    open_named_file_optional(parent_fd, leaf, libc::O_RDWR)
}

fn open_named_file_optional(
    parent_fd: RawFd,
    leaf: &CString,
    access: libc::c_int,
) -> Result<Option<File>, StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: `parent_fd` is borrowed for the call and `leaf` is a live,
    // NUL-terminated single component. `openat` does not retain either pointer.
    let descriptor = unsafe {
        libc::openat(
            parent_fd,
            leaf.as_ptr(),
            access | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if descriptor >= 0 {
        // SAFETY: a nonnegative descriptor returned by `openat` is uniquely owned
        // here and is transferred exactly once to `File`.
        return Ok(Some(unsafe { File::from_raw_fd(descriptor) }));
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(None)
    } else if matches!(error.raw_os_error(), Some(libc::ELOOP) | Some(libc::EISDIR)) {
        Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::from(error))
    }
}

fn create_named_file(
    parent_fd: RawFd,
    leaf: &CString,
) -> Result<File, StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: `parent_fd` is borrowed for the call and `leaf` is a live,
    // NUL-terminated single component. `openat` does not retain either pointer.
    let descriptor = unsafe {
        libc::openat(
            parent_fd,
            leaf.as_ptr(),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK,
            AUTHORITY_FILE_MODE,
        )
    };
    if descriptor < 0 {
        return Err(StoreOwnerJournalAuthorityErrorV4::from(
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: the successful `openat` result is uniquely owned here and is
    // transferred exactly once to `File`.
    let file = unsafe { File::from_raw_fd(descriptor) };
    set_exact_mode(&file, AUTHORITY_FILE_MODE)?;
    validate_regular_base(&file)?;
    Ok(file)
}

fn create_marker(
    directory_fd: RawFd,
    marker: InstallationMarkerV4,
) -> Result<File, StoreOwnerJournalAuthorityErrorV4> {
    let leaf = fixed_leaf(MARKER_LEAF)?;
    let mut file = create_named_file(directory_fd, &leaf)?;
    file.write_all(&encode_marker(marker))
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    file.sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    let identity = regular_file_identity(&file)?;
    validate_marker_file(&file, identity, marker)?;
    validate_named_regular(
        directory_fd,
        &leaf,
        identity,
        Some(INSTALLATION_MARKER_WIRE_BYTES),
    )?;
    Ok(file)
}

fn create_permanent_lock(directory_fd: RawFd) -> Result<File, StoreOwnerJournalAuthorityErrorV4> {
    let leaf = fixed_leaf(LOCK_LEAF)?;
    let file = create_named_file(directory_fd, &leaf)?;
    validate_lock_shape(&file)?;
    acquire_exclusive_lock(&file)?;
    file.sync_all()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    let identity = regular_file_identity(&file)?;
    validate_named_regular(directory_fd, &leaf, identity, Some(0))?;
    Ok(file)
}

fn create_unique_staging_directory(
    parent_fd: RawFd,
    binding: StoreOwnerJournalCreationBindingV4,
    target: [u8; 32],
) -> Result<(File, CString), StoreOwnerJournalAuthorityErrorV4> {
    for _ in 0..MAX_TEMP_CREATE_ATTEMPTS {
        let leaf = staging_leaf(binding, target, random_nonce()?)?;
        // SAFETY: `parent_fd` is borrowed for the call and `leaf` is a live,
        // NUL-terminated single component. `mkdirat` does not retain the pointer.
        let result = unsafe {
            libc::mkdirat(
                parent_fd,
                leaf.as_ptr(),
                INSTALLATION_DIRECTORY_MODE as libc::mode_t,
            )
        };
        if result == 0 {
            let directory = open_named_directory_optional(parent_fd, &leaf)?
                .ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
            return Ok((directory, leaf));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(StoreOwnerJournalAuthorityErrorV4::from(error));
        }
    }
    Err(StoreOwnerJournalAuthorityErrorV4::TemporaryNameExhausted)
}

fn create_unique_head_temp(
    directory_fd: RawFd,
) -> Result<(File, CString, StoreOwnerJournalObjectIdentityV4), StoreOwnerJournalAuthorityErrorV4> {
    for _ in 0..MAX_TEMP_CREATE_ATTEMPTS {
        let leaf = head_temp_leaf(random_nonce()?)?;
        match create_named_file(directory_fd, &leaf) {
            Ok(file) => {
                let shape = validate_head_shape(&file, usize::MAX, false)?;
                return Ok((file, leaf, shape.identity));
            }
            Err(StoreOwnerJournalAuthorityErrorV4::Io(io::ErrorKind::AlreadyExists)) => {}
            Err(error) => return Err(error),
        }
    }
    Err(StoreOwnerJournalAuthorityErrorV4::TemporaryNameExhausted)
}

fn validate_current_parent(
    configured_path: &Path,
    canonical_path: &Path,
    expected: StoreOwnerJournalObjectIdentityV4,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let current_canonical = configured_path
        .canonicalize()
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    if current_canonical != canonical_path {
        return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
    }
    let current = open_directory(canonical_path)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    if directory_identity(&current)? == expected {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
    }
}

fn validate_directory_shape(file: &File) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let metadata = file
        .metadata()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    if metadata.is_dir()
        && metadata.uid() == effective_uid()
        && metadata.mode() & 0o7777 == INSTALLATION_DIRECTORY_MODE
    {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
    }
}

fn validate_owned_directory(file: &File) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let metadata = file
        .metadata()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    if metadata.is_dir() && metadata.uid() == effective_uid() {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
    }
}

fn validate_regular_base(
    file: &File,
) -> Result<std::fs::Metadata, StoreOwnerJournalAuthorityErrorV4> {
    let metadata = file
        .metadata()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    if metadata.is_file()
        && metadata.uid() == effective_uid()
        && metadata.nlink() == 1
        && metadata.mode() & 0o7777 == AUTHORITY_FILE_MODE
    {
        Ok(metadata)
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
    }
}

fn validate_lock_shape(file: &File) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    if validate_regular_base(file)?.len() == 0 {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
    }
}

fn validate_marker_file(
    file: &File,
    identity: StoreOwnerJournalObjectIdentityV4,
    expected: InstallationMarkerV4,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    validate_immutable_file(file, identity, &encode_marker(expected))
}

fn validate_immutable_named_file(
    file: &File,
    parent_fd: RawFd,
    leaf: &CString,
    expected_identity: StoreOwnerJournalObjectIdentityV4,
    expected_bytes: &[u8],
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    validate_immutable_file(file, expected_identity, expected_bytes)?;
    validate_named_regular(
        parent_fd,
        leaf,
        expected_identity,
        Some(expected_bytes.len()),
    )?;
    validate_immutable_file(file, expected_identity, expected_bytes)
}

fn validate_immutable_file(
    file: &File,
    expected_identity: StoreOwnerJournalObjectIdentityV4,
    expected_bytes: &[u8],
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let before = validate_regular_base(file)?;
    if regular_file_identity(file)? != expected_identity
        || before.len() != expected_bytes.len() as u64
        || read_exact_file(file, expected_bytes.len())? != expected_bytes
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    let after = validate_regular_base(file)?;
    if regular_file_identity(file)? != expected_identity
        || after.len() != before.len()
        || read_exact_file(file, expected_bytes.len())? != expected_bytes
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    Ok(())
}

fn validate_named_directory(
    parent_fd: RawFd,
    leaf: &CString,
    expected: StoreOwnerJournalObjectIdentityV4,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let current = open_named_directory_optional(parent_fd, leaf)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?
        .ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    validate_directory_shape(&current)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    if directory_identity(&current)? == expected {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
    }
}

fn validate_named_regular(
    parent_fd: RawFd,
    leaf: &CString,
    expected: StoreOwnerJournalObjectIdentityV4,
    expected_length: Option<usize>,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let current = open_named_read(parent_fd, leaf)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?
        .ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    let metadata = validate_regular_base(&current)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    if regular_file_identity(&current)? != expected
        || expected_length.is_some_and(|length| metadata.len() != length as u64)
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
    }
    Ok(())
}

fn validate_head_shape(
    file: &File,
    max_bytes: usize,
    require_nonempty: bool,
) -> Result<StoreOwnerJournalHeadShapeV4, StoreOwnerJournalAuthorityErrorV4> {
    let metadata = validate_regular_base(file)?;
    let length = usize::try_from(metadata.len())
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::HeadTooLarge)?;
    if length > max_bytes {
        return Err(StoreOwnerJournalAuthorityErrorV4::HeadTooLarge);
    }
    if require_nonempty && length == 0 {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    Ok(StoreOwnerJournalHeadShapeV4 {
        identity: StoreOwnerJournalObjectIdentityV4 {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
        length,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StoreOwnerJournalHeadShapeV4 {
    identity: StoreOwnerJournalObjectIdentityV4,
    length: usize,
}

fn validate_named_head(
    parent_fd: RawFd,
    leaf: &CString,
    expected: StoreOwnerJournalHeadShapeV4,
    max_bytes: usize,
    require_nonempty: bool,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let named =
        open_named_read(parent_fd, leaf)?.ok_or(StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    let current = validate_head_shape(&named, max_bytes, require_nonempty)
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::BindingLost)?;
    if current == expected {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
    }
}

fn directory_identity(
    file: &File,
) -> Result<StoreOwnerJournalObjectIdentityV4, StoreOwnerJournalAuthorityErrorV4> {
    let metadata = file
        .metadata()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    if !metadata.is_dir() {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    Ok(StoreOwnerJournalObjectIdentityV4 {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn regular_file_identity(
    file: &File,
) -> Result<StoreOwnerJournalObjectIdentityV4, StoreOwnerJournalAuthorityErrorV4> {
    let metadata = file
        .metadata()
        .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
    if !metadata.is_file() {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    Ok(StoreOwnerJournalObjectIdentityV4 {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn set_exact_mode(file: &File, mode: u32) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: `file` owns a live descriptor for the duration of the call.
    if unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) } == 0 {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::from(
            io::Error::last_os_error(),
        ))
    }
}

fn acquire_exclusive_lock(file: &File) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: `file` owns a live descriptor for the duration of the call.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::PermissionDenied
    ) {
        Err(StoreOwnerJournalAuthorityErrorV4::LockContended)
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::from(error))
    }
}

fn validate_requested_head(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    if bytes.is_empty() || max_bytes == 0 {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    if bytes.len() > max_bytes {
        return Err(StoreOwnerJournalAuthorityErrorV4::HeadTooLarge);
    }
    Ok(())
}

fn read_exact_file(
    file: &File,
    length: usize,
) -> Result<Vec<u8>, StoreOwnerJournalAuthorityErrorV4> {
    let mut bytes = vec![0; length];
    let mut offset = 0;
    while offset < bytes.len() {
        let read = file
            .read_at(&mut bytes[offset..], offset as u64)
            .map_err(StoreOwnerJournalAuthorityErrorV4::from)?;
        if read == 0 {
            return Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead);
        }
        offset = offset
            .checked_add(read)
            .ok_or(StoreOwnerJournalAuthorityErrorV4::HeadTooLarge)?;
    }
    Ok(bytes)
}

fn encode_marker(marker: InstallationMarkerV4) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(INSTALLATION_MARKER_WIRE_BYTES);
    bytes.extend_from_slice(INSTALLATION_MARKER_MAGIC);
    bytes.extend_from_slice(&marker.creation_binding.0);
    bytes.extend_from_slice(&marker.target_name_digest);
    bytes.extend_from_slice(&marker.locator_digest);
    bytes.extend_from_slice(&marker.directory_identity.device.to_be_bytes());
    bytes.extend_from_slice(&marker.directory_identity.inode.to_be_bytes());
    let digest = domain_digest(INSTALLATION_MARKER_DIGEST_DOMAIN, &bytes);
    bytes.extend_from_slice(&digest);
    bytes
}

fn read_marker(file: &File) -> Result<InstallationMarkerV4, StoreOwnerJournalAuthorityErrorV4> {
    let metadata = validate_regular_base(file)?;
    if metadata.len() != INSTALLATION_MARKER_WIRE_BYTES as u64 {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    decode_marker(&read_exact_file(file, INSTALLATION_MARKER_WIRE_BYTES)?)
}

fn decode_marker(bytes: &[u8]) -> Result<InstallationMarkerV4, StoreOwnerJournalAuthorityErrorV4> {
    if bytes.len() != INSTALLATION_MARKER_WIRE_BYTES
        || bytes.get(..16) != Some(INSTALLATION_MARKER_MAGIC.as_slice())
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    let payload_len = bytes.len() - 32;
    if domain_digest(INSTALLATION_MARKER_DIGEST_DOMAIN, &bytes[..payload_len]).as_slice()
        != &bytes[payload_len..]
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    let binding = array_at::<32>(bytes, 16)?;
    let target_name_digest = array_at::<32>(bytes, 48)?;
    let locator_digest = array_at::<32>(bytes, 80)?;
    let device = u64::from_be_bytes(array_at::<8>(bytes, 112)?);
    let inode = u64::from_be_bytes(array_at::<8>(bytes, 120)?);
    Ok(InstallationMarkerV4 {
        creation_binding: StoreOwnerJournalCreationBindingV4::from_bytes(binding)?,
        target_name_digest,
        locator_digest,
        directory_identity: StoreOwnerJournalObjectIdentityV4 { device, inode },
    })
}

fn encode_initial_receipt(receipt: InitialHeadReceiptV4) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(INITIAL_RECEIPT_WIRE_BYTES);
    bytes.extend_from_slice(INITIAL_RECEIPT_MAGIC);
    bytes.extend_from_slice(&receipt.length.to_be_bytes());
    bytes.extend_from_slice(&receipt.digest);
    let digest = domain_digest(INITIAL_RECEIPT_DIGEST_DOMAIN, &bytes);
    bytes.extend_from_slice(&digest);
    bytes
}

fn decode_initial_receipt(
    bytes: &[u8],
) -> Result<InitialHeadReceiptV4, StoreOwnerJournalAuthorityErrorV4> {
    if bytes.len() != INITIAL_RECEIPT_WIRE_BYTES
        || bytes.get(..16) != Some(INITIAL_RECEIPT_MAGIC.as_slice())
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    let payload_len = bytes.len() - 32;
    if domain_digest(INITIAL_RECEIPT_DIGEST_DOMAIN, &bytes[..payload_len]).as_slice()
        != &bytes[payload_len..]
    {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    let receipt = InitialHeadReceiptV4 {
        length: u64::from_be_bytes(array_at::<8>(bytes, 16)?),
        digest: array_at::<32>(bytes, 24)?,
    };
    if receipt.length == 0 || receipt.digest.iter().all(|byte| *byte == 0) {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidObject);
    }
    Ok(receipt)
}

fn array_at<const N: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<[u8; N], StoreOwnerJournalAuthorityErrorV4> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|slice| slice.try_into().ok())
        .ok_or(StoreOwnerJournalAuthorityErrorV4::InvalidObject)
}

fn target_name_digest(
    canonical_parent: &Path,
    final_leaf: &OsStr,
) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
    let mut hasher = Sha256::new();
    hasher.update(TARGET_NAME_DIGEST_DOMAIN);
    hash_bytes(&mut hasher, canonical_parent.as_os_str().as_bytes())?;
    hash_bytes(&mut hasher, final_leaf.as_bytes())?;
    Ok(hasher.finalize().into())
}

fn locator_digest(
    canonical_parent: &Path,
    target: [u8; 32],
    directory: StoreOwnerJournalObjectIdentityV4,
    binding: StoreOwnerJournalCreationBindingV4,
) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
    let mut hasher = Sha256::new();
    hasher.update(LOCATOR_DIGEST_DOMAIN);
    hash_bytes(&mut hasher, canonical_parent.as_os_str().as_bytes())?;
    hasher.update(target);
    hasher.update(directory.device.to_be_bytes());
    hasher.update(directory.inode.to_be_bytes());
    hasher.update(binding.0);
    Ok(hasher.finalize().into())
}

fn authority_identity_digest(
    core: &HeldInstallationCoreV4,
) -> Result<[u8; 32], StoreOwnerJournalAuthorityErrorV4> {
    let mut hasher = Sha256::new();
    hasher.update(AUTHORITY_IDENTITY_DIGEST_DOMAIN);
    for identity in [
        core.parent_identity,
        core.directory_identity,
        core.marker_identity,
        core.lock_identity,
    ] {
        hasher.update(identity.device.to_be_bytes());
        hasher.update(identity.inode.to_be_bytes());
    }
    hasher.update(core.marker_value.creation_binding.0);
    Ok(hasher.finalize().into())
}

fn intent_leaf(
    binding: StoreOwnerJournalCreationBindingV4,
    target: [u8; 32],
) -> Result<CString, StoreOwnerJournalAuthorityErrorV4> {
    let mut hasher = Sha256::new();
    hasher.update(INTENT_NAME_DOMAIN);
    hasher.update(binding.0);
    hasher.update(target);
    encoded_leaf(".nokv-owner-journal-intent-", &hasher.finalize(), "")
}

fn staging_leaf(
    binding: StoreOwnerJournalCreationBindingV4,
    target: [u8; 32],
    nonce: [u8; RANDOM_NONCE_BYTES],
) -> Result<CString, StoreOwnerJournalAuthorityErrorV4> {
    let mut hasher = Sha256::new();
    hasher.update(STAGING_NAME_DOMAIN);
    hasher.update(binding.0);
    hasher.update(target);
    hasher.update(nonce);
    encoded_leaf(
        ".nokv-owner-journal-stage-",
        &hasher.finalize()[..16],
        ".tmp",
    )
}

fn random_nonce() -> Result<[u8; RANDOM_NONCE_BYTES], StoreOwnerJournalAuthorityErrorV4> {
    #[cfg(target_os = "linux")]
    {
        let mut nonce = [0; RANDOM_NONCE_BYTES];
        let mut offset = 0;
        while offset < nonce.len() {
            // SAFETY: the suffix of `nonce` is a valid writable region of the
            // supplied length, and `getrandom` does not retain the pointer.
            let read = unsafe {
                libc::getrandom(nonce[offset..].as_mut_ptr().cast(), nonce.len() - offset, 0)
            };
            if read > 0 {
                offset += usize::try_from(read)
                    .map_err(|_| StoreOwnerJournalAuthorityErrorV4::InvalidObject)?;
                continue;
            }
            if read == 0 {
                return Err(StoreOwnerJournalAuthorityErrorV4::Io(
                    io::ErrorKind::UnexpectedEof,
                ));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(StoreOwnerJournalAuthorityErrorV4::from(error));
            }
        }
        Ok(nonce)
    }
    #[cfg(target_os = "macos")]
    {
        let mut nonce = [0; RANDOM_NONCE_BYTES];
        // SAFETY: `nonce` provides a valid writable region of exactly the length
        // passed to libc, and `getentropy` does not retain the pointer.
        if unsafe { libc::getentropy(nonce.as_mut_ptr().cast(), nonce.len()) } != 0 {
            return Err(StoreOwnerJournalAuthorityErrorV4::from(
                io::Error::last_os_error(),
            ));
        }
        Ok(nonce)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(StoreOwnerJournalAuthorityErrorV4::UnsupportedPlatform)
    }
}

fn head_temp_leaf(
    nonce: [u8; RANDOM_NONCE_BYTES],
) -> Result<CString, StoreOwnerJournalAuthorityErrorV4> {
    let mut hasher = Sha256::new();
    hasher.update(HEAD_TEMP_NAME_DOMAIN);
    hasher.update(nonce);
    encoded_leaf(".n4-", &hasher.finalize()[..16], ".tmp")
}

fn encoded_leaf(
    prefix: &str,
    digest: &[u8],
    suffix: &str,
) -> Result<CString, StoreOwnerJournalAuthorityErrorV4> {
    let mut name = String::with_capacity(prefix.len() + digest.len() * 2 + suffix.len());
    name.push_str(prefix);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}")
            .map_err(|_| StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?;
    }
    name.push_str(suffix);
    CString::new(name).map_err(|_| StoreOwnerJournalAuthorityErrorV4::InvalidLocator)
}

fn component(value: &OsStr) -> Result<CString, StoreOwnerJournalAuthorityErrorV4> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.contains(&0) || bytes == b"." || bytes == b".." {
        return Err(StoreOwnerJournalAuthorityErrorV4::InvalidLocator);
    }
    CString::new(bytes).map_err(|_| StoreOwnerJournalAuthorityErrorV4::InvalidLocator)
}

fn fixed_leaf(bytes: &[u8]) -> Result<CString, StoreOwnerJournalAuthorityErrorV4> {
    CString::new(bytes).map_err(|_| StoreOwnerJournalAuthorityErrorV4::InvalidLocator)
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| StoreOwnerJournalAuthorityErrorV4::InvalidLocator)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn domain_digest(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().into()
}

fn rename_no_replace(
    source_parent: RawFd,
    source: &CString,
    destination_parent: RawFd,
    destination: &CString,
) -> Result<bool, StoreOwnerJournalAuthorityErrorV4> {
    #[cfg(target_os = "linux")]
    // SAFETY: both descriptors are live directory descriptors and both names are
    // live NUL-terminated components; the syscall does not retain their pointers.
    let result = unsafe {
        libc::renameat2(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    // SAFETY: both descriptors are live directory descriptors and both names are
    // live NUL-terminated components; the syscall does not retain their pointers.
    let result = unsafe {
        libc::renameatx_np(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source_parent, source, destination_parent, destination);
        return Err(StoreOwnerJournalAuthorityErrorV4::UnsupportedPlatform);
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if result == 0 {
        Ok(true)
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::AlreadyExists {
            Ok(false)
        } else {
            Err(StoreOwnerJournalAuthorityErrorV4::from(error))
        }
    }
}

fn rename_replace(
    source_parent: RawFd,
    source: &CString,
    destination_parent: RawFd,
    destination: &CString,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: both descriptors are live directory descriptors and both names are
    // live NUL-terminated components; the syscall does not retain their pointers.
    let result = unsafe {
        libc::renameat(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::from(
            io::Error::last_os_error(),
        ))
    }
}

fn unlink_owned_head_temp(
    directory_fd: RawFd,
    leaf: &CString,
    expected_identity: StoreOwnerJournalObjectIdentityV4,
    max_bytes: usize,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    let Some(file) = open_named_read(directory_fd, leaf)? else {
        return Ok(());
    };
    let shape = validate_head_shape(&file, max_bytes, false)?;
    if shape.identity != expected_identity {
        return Err(StoreOwnerJournalAuthorityErrorV4::BindingLost);
    }
    validate_named_head(directory_fd, leaf, shape, max_bytes, false)?;
    unlink_named_exact(directory_fd, leaf, expected_identity)
}

fn unlink_named_exact(
    parent_fd: RawFd,
    leaf: &CString,
    expected: StoreOwnerJournalObjectIdentityV4,
) -> Result<(), StoreOwnerJournalAuthorityErrorV4> {
    validate_named_regular(parent_fd, leaf, expected, None)?;
    // SAFETY: `parent_fd` is a live directory descriptor and `leaf` is a live,
    // NUL-terminated component revalidated immediately above.
    if unsafe { libc::unlinkat(parent_fd, leaf.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(StoreOwnerJournalAuthorityErrorV4::from(
            io::Error::last_os_error(),
        ))
    }
}

fn read_directory_names(
    directory_fd: RawFd,
) -> Result<Vec<Vec<u8>>, StoreOwnerJournalAuthorityErrorV4> {
    // SAFETY: `directory_fd` is live for the call; the returned descriptor, when
    // nonnegative, is a new independently owned close-on-exec descriptor.
    let duplicate = unsafe { libc::fcntl(directory_fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(StoreOwnerJournalAuthorityErrorV4::from(
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `duplicate` is uniquely owned here. On success ownership transfers
    // to the returned DIR; on failure it remains ours and is closed below.
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: `fdopendir` failed, so ownership of `duplicate` was not
        // transferred and this is its only close.
        unsafe { libc::close(duplicate) };
        return Err(StoreOwnerJournalAuthorityErrorV4::from(error));
    }
    let mut names = Vec::new();
    loop {
        set_errno(0);
        // SAFETY: `directory` is a live DIR owned by this function. The returned
        // entry is consumed before the next `readdir` or `closedir` call.
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            let error = current_errno();
            // SAFETY: `directory` is live and this terminal branch closes it once.
            unsafe { libc::closedir(directory) };
            if error == 0 {
                return Ok(names);
            }
            return Err(StoreOwnerJournalAuthorityErrorV4::from(
                io::Error::from_raw_os_error(error),
            ));
        }
        // SAFETY: a non-null `readdir` result points to a live dirent until the
        // next directory call; POSIX guarantees a NUL-terminated `d_name` field.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec());
        }
    }
}

#[cfg(target_os = "linux")]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: libc returns the current thread's live errno storage pointer.
    unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: libc returns the current thread's live errno storage pointer.
    unsafe { libc::__error() }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn errno_location() -> *mut libc::c_int {
    std::ptr::null_mut()
}

fn set_errno(value: libc::c_int) {
    let location = errno_location();
    if !location.is_null() {
        // SAFETY: supported libc implementations return writable thread-local
        // errno storage; the unsupported-platform branch is null and excluded.
        unsafe { *location = value };
    }
}

fn current_errno() -> libc::c_int {
    let location = errno_location();
    if location.is_null() {
        0
    } else {
        // SAFETY: supported libc implementations return readable thread-local
        // errno storage; null was checked above.
        unsafe { *location }
    }
}

fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no pointer arguments or caller-side preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;
    use std::os::unix::fs::{symlink, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
    use std::process::Command;

    use super::*;

    const MAX_TEST_HEAD: usize = 1024;
    const INITIAL_HEAD: &[u8] = b"canonical-head-generation-1";

    fn creation_binding(byte: u8) -> StoreOwnerJournalCreationBindingV4 {
        StoreOwnerJournalCreationBindingV4::from_bytes([byte; 32]).unwrap()
    }

    fn create_installation(
        path: &Path,
        binding: StoreOwnerJournalCreationBindingV4,
        head: &[u8],
    ) -> StoreOwnerJournalAuthorityV4 {
        StoreOwnerJournalAuthorityV4::begin_create(path, binding, MAX_TEST_HEAD)
            .unwrap()
            .finish(head, MAX_TEST_HEAD)
            .unwrap()
    }

    fn names(path: &Path) -> Vec<OsString> {
        let mut names = fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn zero_creation_binding_is_rejected() {
        assert_eq!(
            StoreOwnerJournalCreationBindingV4::from_bytes([0; 32]),
            Err(StoreOwnerJournalAuthorityErrorV4::InvalidCreationBinding)
        );
    }

    #[test]
    fn create_and_existing_installation_modes_are_disjoint() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");

        assert!(matches!(
            StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::InstallationAbsent)
        ));
        assert!(!installation.exists());

        let create = StoreOwnerJournalAuthorityV4::begin_create(
            &installation,
            creation_binding(1),
            MAX_TEST_HEAD,
        )
        .unwrap();
        assert!(!installation.exists());
        assert!(matches!(
            StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::InstallationAbsent)
        ));
        assert!(!installation.exists());

        let authority = create.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(INITIAL_HEAD.to_vec())
        );
        assert!(matches!(
            StoreOwnerJournalAuthorityV4::begin_create(
                &installation,
                creation_binding(2),
                MAX_TEST_HEAD,
            ),
            Err(StoreOwnerJournalAuthorityErrorV4::CreationConflict)
        ));
    }

    #[test]
    fn different_bindings_compete_only_at_the_raw_final_namespace() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let first = StoreOwnerJournalAuthorityV4::begin_create(
            &installation,
            creation_binding(1),
            MAX_TEST_HEAD,
        )
        .unwrap();
        let second = StoreOwnerJournalAuthorityV4::begin_create(
            &installation,
            creation_binding(2),
            MAX_TEST_HEAD,
        )
        .unwrap();
        assert!(!installation.exists());

        let winner = first.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        assert!(matches!(
            second.finish(b"foreign-initial-head", MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::CreationConflict)
        ));
        assert_eq!(
            winner.read_head(MAX_TEST_HEAD).unwrap(),
            Some(INITIAL_HEAD.to_vec())
        );
        assert_eq!(
            names(directory.path()),
            vec![OsString::from("owner-journal")]
        );
    }

    #[test]
    fn exact_intent_and_final_lock_live_until_the_last_guard_drops() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let binding = creation_binding(3);
        let first =
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD)
                .unwrap();
        let locator_digest = first.canonical_locator_digest().unwrap();
        let authority_digest = first.authority_identity_digest().unwrap();
        assert!(matches!(
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD,),
            Err(StoreOwnerJournalAuthorityErrorV4::LockContended)
        ));

        drop(first);
        let recovered =
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD)
                .unwrap();
        assert_eq!(
            recovered.canonical_locator_digest().unwrap(),
            locator_digest
        );
        assert_eq!(
            recovered.authority_identity_digest().unwrap(),
            authority_digest
        );
        let authority = recovered.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        let directory_identity = authority.directory_identity();
        let lock_identity = authority.lock_identity();
        assert!(matches!(
            StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::LockContended)
        ));
        drop(authority);
        let reopened =
            StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD).unwrap();
        assert_eq!(reopened.directory_identity(), directory_identity);
        assert_eq!(reopened.lock_identity(), lock_identity);
        reopened.validate_binding().unwrap();
    }

    #[test]
    fn final_publish_response_loss_replays_the_exact_initial_receipt() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let binding = creation_binding(4);
        let create =
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD)
                .unwrap();
        FINAL_PUBLISH_AFTER_RENAME_TEST_FAILURE.with(|failure| failure.set(true));
        assert_eq!(
            create.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap_err(),
            StoreOwnerJournalAuthorityErrorV4::Io(io::ErrorKind::Other)
        );
        assert!(installation.is_dir());

        let replay =
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD)
                .unwrap();
        let authority = replay.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(INITIAL_HEAD.to_vec())
        );
    }

    #[test]
    fn immutable_initial_receipt_classifies_replay_after_head_advanced() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let binding = creation_binding(5);
        let authority = create_installation(&installation, binding, INITIAL_HEAD);
        let next = b"canonical-head-generation-2";
        authority
            .replace_head(INITIAL_HEAD, next, MAX_TEST_HEAD)
            .unwrap();
        drop(authority);

        let replay =
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD)
                .unwrap();
        let authority = replay.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(next.to_vec())
        );
        drop(authority);

        let wrong =
            StoreOwnerJournalAuthorityV4::begin_create(&installation, binding, MAX_TEST_HEAD)
                .unwrap();
        assert_eq!(
            wrong
                .finish(b"different-original-head", MAX_TEST_HEAD)
                .unwrap_err(),
            StoreOwnerJournalAuthorityErrorV4::InitialHeadMismatch
        );
    }

    #[test]
    fn partial_final_installation_is_never_repaired_by_either_mode() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        fs::create_dir(&installation).unwrap();
        fs::set_permissions(
            &installation,
            fs::Permissions::from_mode(INSTALLATION_DIRECTORY_MODE),
        )
        .unwrap();
        let marker = installation.join(OsStr::from_bytes(MARKER_LEAF));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(AUTHORITY_FILE_MODE)
            .open(&marker)
            .unwrap();
        let before_inode = fs::symlink_metadata(&marker).unwrap().ino();
        let before_names = names(&installation);

        assert!(StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD).is_err());
        assert!(StoreOwnerJournalAuthorityV4::begin_create(
            &installation,
            creation_binding(6),
            MAX_TEST_HEAD,
        )
        .is_err());
        assert_eq!(names(&installation), before_names);
        let after = fs::symlink_metadata(&marker).unwrap();
        assert_eq!(after.ino(), before_inode);
        assert_eq!(after.len(), 0);
        assert!(!installation.join(OsStr::from_bytes(LOCK_LEAF)).exists());
        assert!(!installation.join(OsStr::from_bytes(HEAD_LEAF)).exists());
    }

    #[test]
    fn partial_final_components_are_never_modified_by_either_mode() {
        for (leaf, replacement) in [
            (MARKER_LEAF, b"partial".as_slice()),
            (LOCK_LEAF, b"partial".as_slice()),
            (INITIAL_RECEIPT_LEAF, b"partial".as_slice()),
            (HEAD_LEAF, b"".as_slice()),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let installation = directory.path().join("owner-journal");
            let binding = creation_binding(61);
            drop(create_installation(&installation, binding, INITIAL_HEAD));
            let component = installation.join(OsStr::from_bytes(leaf));
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&component)
                .unwrap();
            file.write_all(replacement).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let before_names = names(&installation);
            let before_inode = fs::symlink_metadata(&component).unwrap().ino();
            let before_bytes = fs::read(&component).unwrap();

            assert!(
                StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD).is_err()
            );
            assert!(StoreOwnerJournalAuthorityV4::begin_create(
                &installation,
                binding,
                MAX_TEST_HEAD,
            )
            .is_err());
            assert_eq!(names(&installation), before_names);
            assert_eq!(
                fs::symlink_metadata(&component).unwrap().ino(),
                before_inode
            );
            assert_eq!(fs::read(&component).unwrap(), before_bytes);
        }
    }

    #[test]
    fn existing_rejects_each_missing_final_component_without_recreating_it() {
        for missing in [MARKER_LEAF, LOCK_LEAF, INITIAL_RECEIPT_LEAF, HEAD_LEAF] {
            let directory = tempfile::tempdir().unwrap();
            let installation = directory.path().join("owner-journal");
            let binding = creation_binding(7);
            drop(create_installation(&installation, binding, INITIAL_HEAD));
            let missing_path = installation.join(OsStr::from_bytes(missing));
            fs::remove_file(&missing_path).unwrap();
            let before_names = names(&installation);
            assert!(
                StoreOwnerJournalAuthorityV4::open_existing(&installation, MAX_TEST_HEAD).is_err()
            );
            assert!(StoreOwnerJournalAuthorityV4::begin_create(
                &installation,
                binding,
                MAX_TEST_HEAD,
            )
            .is_err());
            assert_eq!(names(&installation), before_names);
            assert!(!missing_path.exists());
        }
    }

    #[test]
    fn parent_and_final_directory_replacement_fail_closed() {
        let sandbox = tempfile::tempdir().unwrap();
        let parent = sandbox.path().join("authority");
        let moved_parent = sandbox.path().join("authority-moved");
        fs::create_dir(&parent).unwrap();
        let installation = parent.join("owner-journal");
        let authority = create_installation(&installation, creation_binding(8), INITIAL_HEAD);

        fs::rename(&parent, &moved_parent).unwrap();
        fs::create_dir(&parent).unwrap();
        assert_eq!(
            authority.validate_binding(),
            Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
        );
        fs::remove_dir(&parent).unwrap();
        fs::rename(&moved_parent, &parent).unwrap();
        authority.validate_binding().unwrap();

        let moved_installation = parent.join("owner-journal-moved");
        fs::rename(&installation, &moved_installation).unwrap();
        fs::create_dir(&installation).unwrap();
        fs::set_permissions(
            &installation,
            fs::Permissions::from_mode(INSTALLATION_DIRECTORY_MODE),
        )
        .unwrap();
        assert_eq!(
            authority.validate_binding(),
            Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
        );
        fs::remove_dir(&installation).unwrap();
        fs::rename(&moved_installation, &installation).unwrap();
        authority.validate_binding().unwrap();
    }

    #[test]
    fn marker_and_lock_replacement_each_fail_closed() {
        let marker_directory = tempfile::tempdir().unwrap();
        let marker_installation = marker_directory.path().join("owner-journal");
        let marker_authority =
            create_installation(&marker_installation, creation_binding(81), INITIAL_HEAD);

        let marker_path = marker_installation.join(OsStr::from_bytes(MARKER_LEAF));
        let marker_bytes = fs::read(&marker_path).unwrap();
        fs::remove_file(&marker_path).unwrap();
        let mut replacement_marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(AUTHORITY_FILE_MODE)
            .open(&marker_path)
            .unwrap();
        replacement_marker.write_all(&marker_bytes).unwrap();
        replacement_marker.sync_all().unwrap();
        assert_eq!(
            marker_authority.validate_binding(),
            Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
        );

        let lock_directory = tempfile::tempdir().unwrap();
        let lock_installation = lock_directory.path().join("owner-journal");
        let lock_authority =
            create_installation(&lock_installation, creation_binding(82), INITIAL_HEAD);
        let lock_path = lock_authority.lock_path();
        fs::remove_file(&lock_path).unwrap();
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(AUTHORITY_FILE_MODE)
            .open(&lock_path)
            .unwrap();
        assert_eq!(
            lock_authority.validate_binding(),
            Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
        );
    }

    #[test]
    fn immutable_initial_receipt_replacement_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let authority = create_installation(&installation, creation_binding(83), INITIAL_HEAD);
        let receipt_path = installation.join(OsStr::from_bytes(INITIAL_RECEIPT_LEAF));
        let receipt_bytes = fs::read(&receipt_path).unwrap();
        fs::remove_file(&receipt_path).unwrap();
        let mut replacement = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(AUTHORITY_FILE_MODE)
            .open(&receipt_path)
            .unwrap();
        replacement.write_all(&receipt_bytes).unwrap();
        replacement.sync_all().unwrap();
        assert_eq!(
            authority.validate_binding(),
            Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
        );
    }

    #[test]
    fn configured_symlink_parent_retarget_fails_every_old_authority_cut() {
        let sandbox = tempfile::tempdir().unwrap();
        let parent_a = sandbox.path().join("parent-a");
        let parent_b = sandbox.path().join("parent-b");
        let configured = sandbox.path().join("configured-parent");
        fs::create_dir(&parent_a).unwrap();
        fs::create_dir(&parent_b).unwrap();
        symlink(&parent_a, &configured).unwrap();
        let through_link = configured.join("owner-journal");
        let binding = creation_binding(9);
        let create =
            StoreOwnerJournalAuthorityV4::begin_create(&through_link, binding, MAX_TEST_HEAD)
                .unwrap();

        fs::remove_file(&configured).unwrap();
        symlink(&parent_b, &configured).unwrap();
        assert_eq!(
            create.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap_err(),
            StoreOwnerJournalAuthorityErrorV4::BindingLost
        );
        assert!(!parent_a.join("owner-journal").exists());

        fs::remove_file(&configured).unwrap();
        symlink(&parent_a, &configured).unwrap();
        let authority =
            StoreOwnerJournalAuthorityV4::begin_create(&through_link, binding, MAX_TEST_HEAD)
                .unwrap()
                .finish(INITIAL_HEAD, MAX_TEST_HEAD)
                .unwrap();
        let other = create_installation(
            &parent_b.join("owner-journal"),
            creation_binding(10),
            b"other-parent-head",
        );
        drop(other);

        fs::remove_file(&configured).unwrap();
        symlink(&parent_b, &configured).unwrap();
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::BindingLost)
        );
        let current = StoreOwnerJournalAuthorityV4::open_existing(&through_link, MAX_TEST_HEAD)
            .expect("a fresh open must bind to the newly configured parent");
        assert_eq!(
            current.read_head(MAX_TEST_HEAD).unwrap(),
            Some(b"other-parent-head".to_vec())
        );
    }

    #[test]
    fn case_equivalent_final_names_are_arbitrated_by_the_filesystem() {
        let directory = tempfile::tempdir().unwrap();
        let probe = directory.path().join("CaseProbe");
        fs::create_dir(&probe).unwrap();
        let case_insensitive = directory.path().join("caseprobe").is_dir();
        fs::remove_dir(&probe).unwrap();
        if !case_insensitive {
            return;
        }

        let upper = directory.path().join("OwnerJournal");
        let lower = directory.path().join("ownerjournal");
        let winner =
            StoreOwnerJournalAuthorityV4::begin_create(&upper, creation_binding(11), MAX_TEST_HEAD)
                .unwrap();
        let loser =
            StoreOwnerJournalAuthorityV4::begin_create(&lower, creation_binding(12), MAX_TEST_HEAD)
                .unwrap();
        let authority = winner.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        assert!(matches!(
            loser.finish(b"must-not-win", MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::CreationConflict)
        ));
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(INITIAL_HEAD.to_vec())
        );
        assert_eq!(names(directory.path()).len(), 1);
    }

    #[test]
    fn unicode_equivalent_final_names_are_arbitrated_by_the_filesystem() {
        let directory = tempfile::tempdir().unwrap();
        let composed_probe = directory.path().join("Probe-é");
        let decomposed_probe = directory.path().join("Probe-e\u{301}");
        fs::create_dir(&composed_probe).unwrap();
        let normalization_insensitive = decomposed_probe.is_dir();
        fs::remove_dir(&composed_probe).unwrap();
        if !normalization_insensitive {
            return;
        }

        let composed = directory.path().join("Journal-é");
        let decomposed = directory.path().join("Journal-e\u{301}");
        let winner = StoreOwnerJournalAuthorityV4::begin_create(
            &composed,
            creation_binding(13),
            MAX_TEST_HEAD,
        )
        .unwrap();
        let loser = StoreOwnerJournalAuthorityV4::begin_create(
            &decomposed,
            creation_binding(14),
            MAX_TEST_HEAD,
        )
        .unwrap();
        let authority = winner.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        assert!(matches!(
            loser.finish(b"must-not-win", MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::CreationConflict)
        ));
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(INITIAL_HEAD.to_vec())
        );
        assert_eq!(names(directory.path()).len(), 1);
    }

    #[test]
    fn head_reads_and_replacements_stay_under_the_held_installation_directory() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let authority = create_installation(&installation, creation_binding(15), INITIAL_HEAD);
        let next = b"canonical-head-generation-2";
        let initial_inode = fs::symlink_metadata(installation.join("head.v4"))
            .unwrap()
            .ino();
        authority
            .replace_head(INITIAL_HEAD, next, MAX_TEST_HEAD)
            .unwrap();
        assert_ne!(
            fs::symlink_metadata(installation.join("head.v4"))
                .unwrap()
                .ino(),
            initial_inode
        );
        assert_eq!(
            authority.replace_head(INITIAL_HEAD, b"stale", MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::HeadCompareMismatch)
        );
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(next.to_vec())
        );
    }

    #[test]
    fn replacement_rejects_a_same_bytes_named_inode_swap_before_rename() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let authority = create_installation(&installation, creation_binding(151), INITIAL_HEAD);
        let head = installation.join(OsStr::from_bytes(HEAD_LEAF));
        let foreign = installation.join("foreign-head");
        let mut foreign_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(AUTHORITY_FILE_MODE)
            .open(&foreign)
            .unwrap();
        foreign_file.write_all(INITIAL_HEAD).unwrap();
        foreign_file.sync_all().unwrap();
        drop(foreign_file);

        REPLACE_BEFORE_RENAME_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(foreign, head).unwrap();
            }));
        });
        assert_eq!(
            authority.replace_head(INITIAL_HEAD, b"canonical-head-generation-2", MAX_TEST_HEAD,),
            Err(StoreOwnerJournalAuthorityErrorV4::HeadCompareMismatch)
        );
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(INITIAL_HEAD.to_vec())
        );
    }

    #[test]
    fn post_replace_rename_failure_is_reconciled_only_from_the_final_head() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let authority = create_installation(&installation, creation_binding(152), INITIAL_HEAD);
        let next = b"canonical-head-generation-2";
        REPLACE_AFTER_RENAME_TEST_FAILURE.with(|failure| failure.set(true));
        assert_eq!(
            authority.replace_head(INITIAL_HEAD, next, MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::Io(io::ErrorKind::Other))
        );
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD).unwrap(),
            Some(next.to_vec())
        );
        assert_eq!(
            authority.replace_head(INITIAL_HEAD, b"must-not-replay", MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::HeadCompareMismatch)
        );
    }

    #[test]
    fn stable_head_read_rejects_same_inode_content_drift() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let authority = create_installation(&installation, creation_binding(16), INITIAL_HEAD);
        let replacement = b"canonical-head-generation-X";
        assert_eq!(replacement.len(), INITIAL_HEAD.len());
        let head_path = installation.join(OsStr::from_bytes(HEAD_LEAF));
        STABLE_HEAD_READ_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let mut head = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(head_path)
                    .unwrap();
                head.write_all(replacement).unwrap();
                head.sync_all().unwrap();
            }));
        });
        assert_eq!(
            authority.read_head(MAX_TEST_HEAD),
            Err(StoreOwnerJournalAuthorityErrorV4::UnstableHead)
        );
    }

    #[test]
    fn debug_redacts_locator_and_creation_binding() {
        let directory = tempfile::Builder::new()
            .prefix("SECRET-JOURNAL-LOCATOR")
            .tempdir()
            .unwrap();
        let installation = directory.path().join("secret-owner-journal");
        let token = StoreOwnerJournalAuthorityV4::begin_create(
            &installation,
            creation_binding(17),
            MAX_TEST_HEAD,
        )
        .unwrap();
        let token_debug = format!("{token:?}");
        assert!(!token_debug.contains("SECRET"));
        assert!(!token_debug.contains("secret-owner-journal"));
        let authority = token.finish(INITIAL_HEAD, MAX_TEST_HEAD).unwrap();
        let authority_debug = format!("{authority:?}");
        assert!(!authority_debug.contains("SECRET"));
        assert!(!authority_debug.contains("secret-owner-journal"));
    }

    #[test]
    fn permanent_lock_excludes_an_independent_process() {
        let directory = tempfile::tempdir().unwrap();
        let installation = directory.path().join("owner-journal");
        let authority = create_installation(&installation, creation_binding(18), INITIAL_HEAD);
        run_lock_probe(&installation, "contended");
        drop(authority);
        run_lock_probe(&installation, "available");
    }

    #[test]
    #[ignore = "subprocess helper"]
    fn installation_lock_subprocess_helper() {
        let Some(path) = std::env::var_os("NOKV_STORE_OWNER_JOURNAL_PROBE_PATH") else {
            return;
        };
        let expected = std::env::var("NOKV_STORE_OWNER_JOURNAL_PROBE_EXPECTED").unwrap();
        match expected.as_str() {
            "contended" => assert!(matches!(
                StoreOwnerJournalAuthorityV4::open_existing(Path::new(&path), MAX_TEST_HEAD,),
                Err(StoreOwnerJournalAuthorityErrorV4::LockContended)
            )),
            "available" => {
                StoreOwnerJournalAuthorityV4::open_existing(Path::new(&path), MAX_TEST_HEAD)
                    .unwrap()
                    .validate_binding()
                    .unwrap()
            }
            _ => panic!("unexpected subprocess probe expectation"),
        }
    }

    fn run_lock_probe(path: &Path, expected: &str) {
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store_owner_journal::unix::tests::installation_lock_subprocess_helper")
            .arg("--ignored")
            .arg("--nocapture")
            .env("NOKV_STORE_OWNER_JOURNAL_PROBE_PATH", path)
            .env("NOKV_STORE_OWNER_JOURNAL_PROBE_EXPECTED", expected)
            .status()
            .unwrap();
        assert!(status.success(), "subprocess lock probe failed");
    }
}
