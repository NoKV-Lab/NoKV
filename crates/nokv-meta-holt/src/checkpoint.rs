/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use holt::{CheckpointImage, DB};
use nokv_meta_store::{
    CheckpointCatalogCommitment, CheckpointInstallError, FreshStoreCheckpointInstaller, Keyspace,
    StoreCheckpointEnvelope, StoreError, StoreLimits, WholeStoreCheckpointSource,
    MAX_CHECKPOINT_IMAGE_BYTES,
};
use sha2::{Digest, Sha256};

#[cfg(test)]
use std::sync::Mutex;

use crate::{HoltOptions, HoltStore};

/// Holt 0.8.5 whole-DB checkpoint format qualified by this adapter.
pub const HOLT_CHECKPOINT_FORMAT_ID: &str = "holt.0.8.5.checkpoint.v1";
const HOLT_CHECKPOINT_MAGIC: &[u8; 8] = b"holtdbi1";
const HOLT_CHECKPOINT_HEADER_BYTES: usize = 12;
// Must remain equal to Holt 0.8.5's bounded checkpoint default.
const HOLT_CHECKPOINT_MAX_RECORDS: usize = 16 * 1024 * 1024;
const INSTALL_SENTINEL_FILE: &str = ".nokv-checkpoint-installing-v1";
const INSTALL_SENTINEL_MAGIC: &[u8] = b"nokv.holt-checkpoint-install.v1\0";
const COMPLETE_MARKER_FILE: &str = ".nokv-checkpoint-complete-v1";
const COMPLETE_MARKER_MAGIC: &[u8] = b"nokv.holt-checkpoint-complete.v1\0";
const ADOPTED_MARKER_FILE: &str = ".nokv-checkpoint-adopted-v1";
const RECOVERY_STAGING_IDENTITY_MAGIC: &[u8; 8] = b"NOKVHRSI";
const RECOVERY_STAGING_IDENTITY_VERSION: u16 = 1;
const RECOVERY_STAGING_DIGEST_BYTES: usize = 32;
const RECOVERY_STAGING_MARKER_CHECKSUM_BYTES: usize = 32;
const RECOVERY_STAGING_IDENTITY_BYTES: usize = RECOVERY_STAGING_IDENTITY_MAGIC.len()
    + std::mem::size_of::<u16>()
    + RECOVERY_STAGING_DIGEST_BYTES
    + 32;

#[cfg(test)]
static FAIL_AFTER_PARTIAL_INSTALL: Mutex<Option<PathBuf>> = Mutex::new(None);
#[cfg(test)]
static FAIL_FINALIZE_DIRECTORY_SYNC: Mutex<Option<PathBuf>> = Mutex::new(None);
#[cfg(test)]
static FAIL_INSTALL_SENTINEL_RESTORE: Mutex<Option<PathBuf>> = Mutex::new(None);
#[cfg(test)]
static FAIL_ADOPTION_BEFORE_RENAME: Mutex<Option<PathBuf>> = Mutex::new(None);
#[cfg(test)]
static FAIL_ADOPTION_AFTER_RENAME: Mutex<Option<PathBuf>> = Mutex::new(None);

/// Exact checkpoint identity required to reopen one pre-control recovery staging store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HoltRecoveryStagingIdentity {
    envelope_digest: [u8; RECOVERY_STAGING_DIGEST_BYTES],
    catalog_commitment: CheckpointCatalogCommitment,
}

/// Caller attestation that control durably finalized the exact staging identity.
///
/// The Holt adapter cannot construct or verify control-plane durability. Calling
/// [`Self::after_durable_control_finalize`] is the external coordinator's
/// assertion that its durable finalize record reached its acknowledgement
/// boundary before physical marker adoption begins.
#[derive(Debug)]
pub struct HoltRecoveryStagingAdoptionAuthority {
    expected_identity: HoltRecoveryStagingIdentity,
}

impl HoltRecoveryStagingIdentity {
    /// Canonically encode an identity for durable coordinator storage.
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(RECOVERY_STAGING_IDENTITY_BYTES);
        encoded.extend_from_slice(RECOVERY_STAGING_IDENTITY_MAGIC);
        encoded.extend_from_slice(&RECOVERY_STAGING_IDENTITY_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.envelope_digest);
        encoded.extend_from_slice(self.catalog_commitment.as_bytes());
        encoded
    }

    /// Decode one exact identity without accepting trailing or alternate forms.
    pub fn decode(encoded: &[u8]) -> Result<Self, StoreError> {
        if encoded.len() != RECOVERY_STAGING_IDENTITY_BYTES
            || &encoded[..RECOVERY_STAGING_IDENTITY_MAGIC.len()] != RECOVERY_STAGING_IDENTITY_MAGIC
        {
            return Err(StoreError::InvalidRequest(
                "invalid Holt recovery staging identity".to_owned(),
            ));
        }
        let version_offset = RECOVERY_STAGING_IDENTITY_MAGIC.len();
        let version_end = version_offset + std::mem::size_of::<u16>();
        let version = u16::from_be_bytes(
            encoded[version_offset..version_end]
                .try_into()
                .expect("fixed identity version width"),
        );
        if version != RECOVERY_STAGING_IDENTITY_VERSION {
            return Err(StoreError::InvalidRequest(
                "unsupported Holt recovery staging identity version".to_owned(),
            ));
        }
        let digest_end = version_end + RECOVERY_STAGING_DIGEST_BYTES;
        let mut envelope_digest = [0_u8; RECOVERY_STAGING_DIGEST_BYTES];
        envelope_digest.copy_from_slice(&encoded[version_end..digest_end]);
        let mut catalog = [0_u8; 32];
        catalog.copy_from_slice(&encoded[digest_end..]);
        Ok(Self {
            envelope_digest,
            catalog_commitment: CheckpointCatalogCommitment::from_bytes(catalog),
        })
    }

    pub(crate) const fn catalog_commitment(&self) -> CheckpointCatalogCommitment {
        self.catalog_commitment
    }

    fn marker_bytes(&self, magic: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            magic.len()
                + RECOVERY_STAGING_DIGEST_BYTES
                + 32
                + RECOVERY_STAGING_MARKER_CHECKSUM_BYTES,
        );
        bytes.extend_from_slice(magic);
        bytes.extend_from_slice(&self.envelope_digest);
        bytes.extend_from_slice(self.catalog_commitment.as_bytes());
        bytes.extend_from_slice(&checkpoint_marker_checksum(&bytes));
        bytes
    }
}

fn checkpoint_marker_checksum(marker_identity: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"nokv.meta-holt.checkpoint-marker.v1\0");
    digest.update(marker_identity);
    digest.finalize().into()
}

impl HoltRecoveryStagingAdoptionAuthority {
    /// Cross the explicit boundary after the caller durably finalizes control state.
    #[must_use]
    pub fn after_durable_control_finalize(expected_identity: HoltRecoveryStagingIdentity) -> Self {
        Self { expected_identity }
    }

    pub(crate) fn into_expected_identity(self) -> HoltRecoveryStagingIdentity {
        self.expected_identity
    }
}

pub(crate) fn catalog_commitment(
    trees: &BTreeMap<Keyspace, String>,
) -> CheckpointCatalogCommitment {
    let mut digest = Sha256::new();
    digest.update(b"nokv.meta-holt.catalog.v1\0");
    digest.update((trees.len() as u64).to_be_bytes());
    for (keyspace, name) in trees {
        digest.update(keyspace.get().to_be_bytes());
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
    }
    CheckpointCatalogCommitment::from_bytes(digest.finalize().into())
}

pub(crate) fn validate_holt_checkpoint_image(
    image: &[u8],
    trees: &BTreeMap<Keyspace, String>,
    limits: &StoreLimits,
) -> Result<(), String> {
    validate_holt_checkpoint_image_with_record_limit(
        image,
        trees,
        limits,
        HOLT_CHECKPOINT_MAX_RECORDS,
    )
}

pub(crate) fn validate_holt_checkpoint_image_with_record_limit(
    image: &[u8],
    trees: &BTreeMap<Keyspace, String>,
    limits: &StoreLimits,
    max_records: usize,
) -> Result<(), String> {
    if image.len() > MAX_CHECKPOINT_IMAGE_BYTES {
        return Err(format!(
            "Holt checkpoint image is {} bytes; maximum is {MAX_CHECKPOINT_IMAGE_BYTES}",
            image.len()
        ));
    }
    if image.len() < HOLT_CHECKPOINT_HEADER_BYTES
        || &image[..HOLT_CHECKPOINT_MAGIC.len()] != HOLT_CHECKPOINT_MAGIC
    {
        return Err("Holt checkpoint image has bad magic or a truncated header".to_owned());
    }
    let family_count = u32::from_le_bytes(
        image[8..12]
            .try_into()
            .map_err(|_| "Holt checkpoint family count is truncated".to_owned())?,
    ) as usize;
    if family_count != trees.len() {
        return Err(format!(
            "Holt checkpoint has {family_count} families; configured catalog has {}",
            trees.len()
        ));
    }

    let expected_names = trees.values().collect::<BTreeSet<_>>();
    let mut offset = HOLT_CHECKPOINT_HEADER_BYTES;
    let mut records = 0_usize;
    for expected_name in expected_names {
        let name = take_holt_bytes(image, &mut offset)?;
        if name != expected_name.as_bytes() {
            return Err("Holt checkpoint family order or catalog is not canonical".to_owned());
        }
        let block = take_holt_bytes(image, &mut offset)?;
        validate_holt_family_block(block, limits, &mut records, max_records)?;
    }
    if offset != image.len() {
        return Err("Holt checkpoint image has trailing bytes".to_owned());
    }
    Ok(())
}

pub(crate) fn reject_install_sentinel(
    path: &Path,
    expected_catalog: CheckpointCatalogCommitment,
) -> Result<(), StoreError> {
    validate_install_markers(path, None, expected_catalog)
}

pub(crate) fn validate_install_markers(
    path: &Path,
    expected_complete: Option<&HoltRecoveryStagingIdentity>,
    expected_catalog: CheckpointCatalogCommitment,
) -> Result<(), StoreError> {
    reject_incomplete_install_marker(path)?;

    let complete = complete_marker_path(path);
    let adopted = adopted_marker_path(path);
    let complete_exists = inspect_marker_exists(&complete, "completed")?;
    let adopted_exists = inspect_marker_exists(&adopted, "adopted")?;
    if complete_exists && adopted_exists {
        return Err(StoreError::Corrupt(format!(
            "HoltStore directory {} has conflicting completed and adopted checkpoint markers",
            path.display()
        )));
    }
    match (complete_exists, adopted_exists, expected_complete) {
        (false, false, None) => Ok(()),
        (false, false, Some(_)) => Err(StoreError::Corrupt(format!(
            "HoltStore directory {} lost its expected completed checkpoint marker",
            path.display()
        ))),
        (true, false, None) => Err(StoreError::Corrupt(format!(
            "HoltStore directory {} has a completed checkpoint marker that requires exact expected-checkpoint reconciliation",
            path.display()
        ))),
        (true, false, Some(expected)) => verify_identity_marker(
            &complete,
            COMPLETE_MARKER_MAGIC,
            expected,
            "completed",
            path,
        ),
        (false, true, None) => verify_adopted_marker_catalog(&adopted, expected_catalog, path),
        (false, true, Some(_)) => Err(StoreError::Corrupt(format!(
            "HoltStore directory {} was already adopted and is no longer recovery staging",
            path.display()
        ))),
        (true, true, _) => unreachable!("conflicting markers returned above"),
    }
}

pub(crate) fn validate_recovery_staging_adoption(
    path: &Path,
    expected: &HoltRecoveryStagingIdentity,
) -> Result<(), StoreError> {
    reject_incomplete_install_marker(path)?;
    let complete = complete_marker_path(path);
    let adopted = adopted_marker_path(path);
    let complete_exists = inspect_marker_exists(&complete, "completed")?;
    let adopted_exists = inspect_marker_exists(&adopted, "adopted")?;
    match (complete_exists, adopted_exists) {
        (true, false) => verify_identity_marker(
            &complete,
            COMPLETE_MARKER_MAGIC,
            expected,
            "completed",
            path,
        ),
        (false, true) => {
            verify_identity_marker(&adopted, COMPLETE_MARKER_MAGIC, expected, "adopted", path)
        }
        (true, true) => Err(StoreError::Corrupt(format!(
            "HoltStore directory {} has conflicting completed and adopted checkpoint markers",
            path.display()
        ))),
        (false, false) => Err(StoreError::Corrupt(format!(
            "HoltStore directory {} has no exact recovery staging identity marker",
            path.display()
        ))),
    }
}

pub(crate) fn adopt_recovery_staging_marker(
    target: &Path,
    expected: &HoltRecoveryStagingIdentity,
) -> Result<(), StoreError> {
    validate_recovery_staging_adoption(target, expected)?;
    let complete = complete_marker_path(target);
    let adopted = adopted_marker_path(target);
    if marker_exists(&adopted).map_err(|error| {
        StoreError::Unavailable(format!(
            "inspect adopted Holt recovery staging marker: {error}"
        ))
    })? {
        return sync_directory(target).map_err(|error| {
            StoreError::Unavailable(format!(
                "sync reconciled Holt recovery staging adoption: {error}"
            ))
        });
    }

    #[cfg(test)]
    if take_adoption_failure(&FAIL_ADOPTION_BEFORE_RENAME, target) {
        return Err(StoreError::Unavailable(
            "injected Holt recovery staging adoption failure before marker rename".to_owned(),
        ));
    }
    std::fs::rename(&complete, &adopted).map_err(|error| {
        StoreError::Unavailable(format!(
            "convert completed Holt recovery staging marker to adopted: {error}"
        ))
    })?;
    #[cfg(test)]
    if take_adoption_failure(&FAIL_ADOPTION_AFTER_RENAME, target) {
        return Err(StoreError::Unavailable(
            "injected Holt recovery staging adoption failure after marker rename".to_owned(),
        ));
    }
    sync_directory(target).map_err(|error| {
        StoreError::Unavailable(format!(
            "sync Holt recovery staging marker adoption: {error}"
        ))
    })
}

/// Fresh-only installer that may reconcile only the same completed identity after restart.
pub struct HoltFreshCheckpointInstaller {
    options: HoltOptions,
}

impl HoltFreshCheckpointInstaller {
    pub fn new(options: HoltOptions) -> Self {
        Self { options }
    }

    /// Derive the exact identity that a coordinator must persist before install.
    ///
    /// This is a zero-I/O validation step. The returned identity is sufficient
    /// for [`HoltStore::open_recovery_staging`] after log replay has changed the
    /// installed database away from the original checkpoint image.
    pub fn recovery_staging_identity(
        &self,
        checkpoint: &StoreCheckpointEnvelope,
    ) -> Result<HoltRecoveryStagingIdentity, CheckpointInstallError> {
        let trees = validate_checkpoint_for_options(&self.options, checkpoint)?;
        Ok(recovery_staging_identity(
            checkpoint,
            catalog_commitment(&trees),
        ))
    }
}

impl FreshStoreCheckpointInstaller for HoltFreshCheckpointInstaller {
    type Store = HoltStore;

    fn install(
        self,
        checkpoint: &StoreCheckpointEnvelope,
    ) -> Result<Self::Store, CheckpointInstallError> {
        install_fresh_checkpoint(self.options, checkpoint)
    }
}

impl WholeStoreCheckpointSource for HoltStore {
    fn export_checkpoint(
        &self,
    ) -> Result<StoreCheckpointEnvelope, nokv_meta_store::CheckpointError> {
        self.export_whole_store_checkpoint()
    }
}

fn install_fresh_checkpoint(
    options: HoltOptions,
    checkpoint: &StoreCheckpointEnvelope,
) -> Result<HoltStore, CheckpointInstallError> {
    let trees = validate_checkpoint_for_options(&options, checkpoint)?;
    let expected_commitment = catalog_commitment(&trees);
    let identity = recovery_staging_identity(checkpoint, expected_commitment);
    let target = options
        .file_path()
        .map_err(|error| CheckpointInstallError::unchanged(error.to_string()))?
        .ok_or_else(|| {
            CheckpointInstallError::unchanged(
                "checkpoint installation requires a file-backed Holt target",
            )
        })?
        .to_path_buf();
    let sentinel_bytes = identity.marker_bytes(INSTALL_SENTINEL_MAGIC);
    let complete_bytes = identity.marker_bytes(COMPLETE_MARKER_MAGIC);
    if marker_exists(&adopted_marker_path(&target))
        .map_err(|error| CheckpointInstallError::unchanged(error.to_string()))?
    {
        return Err(CheckpointInstallError::unchanged(
            "checkpoint target has already been adopted and is not fresh",
        ));
    }
    if marker_exists(&complete_marker_path(&target))
        .map_err(|error| CheckpointInstallError::unchanged(error.to_string()))?
    {
        return reconcile_completed_checkpoint(
            options,
            checkpoint,
            trees,
            &target,
            identity,
            sentinel_bytes,
            complete_bytes,
        );
    }
    if install_sentinel_exists(&target)
        .map_err(|error| CheckpointInstallError::unchanged(error.to_string()))?
    {
        return Err(CheckpointInstallError::poisoned(
            "checkpoint target already has an incomplete installation marker; discard it",
        ));
    }
    prepare_fresh_target(&target).map_err(CheckpointInstallError::unchanged)?;

    let sentinel_path = install_sentinel_path(&target);
    let mut sentinel = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&sentinel_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(CheckpointInstallError::poisoned(
                "checkpoint target acquired an incomplete installation marker; discard it",
            ));
        }
        Err(error) => {
            return Err(CheckpointInstallError::unchanged(format!(
                "create Holt checkpoint installation marker: {error}"
            )));
        }
    };

    let installed = (|| -> Result<DB, String> {
        sentinel
            .write_all(&sentinel_bytes)
            .map_err(|error| format!("write checkpoint installation marker: {error}"))?;
        sentinel
            .sync_all()
            .map_err(|error| format!("sync checkpoint installation marker: {error}"))?;
        drop(sentinel);
        sync_directory(&target)
            .map_err(|error| format!("sync checkpoint target directory: {error}"))?;
        require_only_sentinel(&target)?;

        #[cfg(test)]
        if take_partial_install_failure(&target) {
            inject_partial_install(&options, &trees)?;
            return Err("injected failure after partial Holt checkpoint install".to_owned());
        }

        let db = DB::open(options.config.clone()).map_err(|error| error.to_string())?;
        let image = CheckpointImage::from_bytes(checkpoint.image().to_vec());
        db.install_checkpoint(&image)
            .map_err(|error| error.to_string())?;
        require_exact_catalog(&db, &trees)?;
        db.checkpoint().map_err(|error| error.to_string())?;
        drop(db);

        let verified = DB::open(options.config.clone()).map_err(|error| error.to_string())?;
        require_exact_catalog(&verified, &trees)?;
        let reexported = verified
            .export_checkpoint()
            .map_err(|error| error.to_string())?;
        if reexported.as_bytes() != checkpoint.image() {
            return Err(
                "installed Holt checkpoint does not exactly re-export the source image".to_owned(),
            );
        }
        Ok(verified)
    })();

    let verified = installed.map_err(CheckpointInstallError::poisoned)?;
    write_durable_marker(&complete_marker_path(&target), &complete_bytes).map_err(|error| {
        CheckpointInstallError::poisoned(format!(
            "publish completed Holt checkpoint identity: {error}"
        ))
    })?;
    let store = HoltStore::from_installed_checkpoint(options, verified, trees, identity)
        .map_err(|error| CheckpointInstallError::poisoned(error.to_string()))?;
    if let Err(error) = remove_install_sentinel(&target) {
        restore_install_sentinel(&sentinel_path, &sentinel_bytes);
        return Err(CheckpointInstallError::poisoned(format!(
            "finalize Holt checkpoint installation marker: {error}"
        )));
    }
    Ok(store)
}

fn validate_checkpoint_for_options(
    options: &HoltOptions,
    checkpoint: &StoreCheckpointEnvelope,
) -> Result<BTreeMap<Keyspace, String>, CheckpointInstallError> {
    let trees = options
        .validate(false)
        .map_err(|error| CheckpointInstallError::unchanged(error.to_string()))?;
    if checkpoint.format_id().as_str() != HOLT_CHECKPOINT_FORMAT_ID {
        return Err(CheckpointInstallError::unchanged(
            "checkpoint format is not supported by this Holt adapter",
        ));
    }
    let expected_commitment = catalog_commitment(&trees);
    if checkpoint.catalog_commitment() != expected_commitment {
        return Err(CheckpointInstallError::unchanged(
            "checkpoint catalog commitment does not match the target catalog",
        ));
    }
    validate_holt_checkpoint_image(checkpoint.image(), &trees, &options.limits)
        .map_err(CheckpointInstallError::unchanged)?;
    Ok(trees)
}

fn reconcile_completed_checkpoint(
    options: HoltOptions,
    checkpoint: &StoreCheckpointEnvelope,
    trees: BTreeMap<Keyspace, String>,
    target: &Path,
    identity: HoltRecoveryStagingIdentity,
    sentinel_bytes: Vec<u8>,
    complete_bytes: Vec<u8>,
) -> Result<HoltStore, CheckpointInstallError> {
    verify_marker_bytes(&complete_marker_path(target), &complete_bytes)
        .map_err(CheckpointInstallError::poisoned)?;
    let verified = open_and_verify_installed_checkpoint(&options, checkpoint, &trees)
        .map_err(CheckpointInstallError::poisoned)?;
    if install_sentinel_exists(target)
        .map_err(|error| CheckpointInstallError::poisoned(error.to_string()))?
    {
        verify_marker_bytes(&install_sentinel_path(target), &sentinel_bytes)
            .map_err(CheckpointInstallError::poisoned)?;
        if let Err(error) = remove_install_sentinel(target) {
            restore_install_sentinel(&install_sentinel_path(target), &sentinel_bytes);
            return Err(CheckpointInstallError::poisoned(format!(
                "reconcile Holt checkpoint installation marker: {error}"
            )));
        }
    }
    HoltStore::from_installed_checkpoint(options, verified, trees, identity)
        .map_err(|error| CheckpointInstallError::poisoned(error.to_string()))
}

fn open_and_verify_installed_checkpoint(
    options: &HoltOptions,
    checkpoint: &StoreCheckpointEnvelope,
    trees: &BTreeMap<Keyspace, String>,
) -> Result<DB, String> {
    let verified = DB::open(options.config.clone()).map_err(|error| error.to_string())?;
    require_exact_catalog(&verified, trees)?;
    let reexported = verified
        .export_checkpoint()
        .map_err(|error| error.to_string())?;
    if reexported.as_bytes() != checkpoint.image() {
        return Err(
            "installed Holt checkpoint does not exactly re-export the expected image".to_owned(),
        );
    }
    Ok(verified)
}

#[cfg(test)]
pub(crate) fn fail_install_after_partial_apply_for_test(target: PathBuf) {
    *FAIL_AFTER_PARTIAL_INSTALL
        .lock()
        .expect("lock partial checkpoint install failpoint") = Some(target);
}

#[cfg(test)]
pub(crate) fn fail_finalize_after_unlink_and_restore_for_test(target: PathBuf) {
    *FAIL_FINALIZE_DIRECTORY_SYNC
        .lock()
        .expect("lock checkpoint finalize sync failpoint") = Some(target.clone());
    *FAIL_INSTALL_SENTINEL_RESTORE
        .lock()
        .expect("lock checkpoint sentinel restore failpoint") = Some(target);
}

#[cfg(test)]
pub(crate) fn fail_next_adoption_before_rename_for_test(target: PathBuf) {
    *FAIL_ADOPTION_BEFORE_RENAME
        .lock()
        .expect("lock checkpoint adoption pre-rename failpoint") = Some(target);
}

#[cfg(test)]
pub(crate) fn fail_next_adoption_after_rename_for_test(target: PathBuf) {
    *FAIL_ADOPTION_AFTER_RENAME
        .lock()
        .expect("lock checkpoint adoption post-rename failpoint") = Some(target);
}

#[cfg(test)]
fn take_adoption_failure(failpoint: &Mutex<Option<PathBuf>>, target: &Path) -> bool {
    let mut configured = failpoint
        .lock()
        .expect("lock checkpoint adoption failpoint");
    if configured.as_deref() == Some(target) {
        configured.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
fn take_partial_install_failure(target: &Path) -> bool {
    let mut configured = FAIL_AFTER_PARTIAL_INSTALL
        .lock()
        .expect("lock partial checkpoint install failpoint");
    if configured.as_deref() == Some(target) {
        configured.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
fn inject_partial_install(
    options: &HoltOptions,
    trees: &BTreeMap<Keyspace, String>,
) -> Result<(), String> {
    let db = DB::open(options.config.clone()).map_err(|error| error.to_string())?;
    for name in trees.values() {
        db.create_tree(name).map_err(|error| error.to_string())?;
    }
    let first = trees
        .values()
        .next()
        .ok_or_else(|| "partial-install injection requires one tree".to_owned())?;
    db.open_tree(first)
        .map_err(|error| error.to_string())?
        .put(b"partial-key", b"partial-value")
        .map_err(|error| error.to_string())?;
    db.checkpoint().map_err(|error| error.to_string())?;
    Ok(())
}

fn take_holt_bytes<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], String> {
    let length_end = offset
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| "Holt checkpoint image has a truncated length".to_owned())?;
    let len = u32::from_le_bytes(
        bytes[*offset..length_end]
            .try_into()
            .map_err(|_| "Holt checkpoint image has a truncated length".to_owned())?,
    ) as usize;
    let body_end = length_end
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| "Holt checkpoint image has a truncated body".to_owned())?;
    *offset = body_end;
    Ok(&bytes[length_end..body_end])
}

fn validate_holt_family_block(
    block: &[u8],
    limits: &StoreLimits,
    records: &mut usize,
    max_records: usize,
) -> Result<(), String> {
    let mut offset = 0_usize;
    let mut previous_key: Option<&[u8]> = None;
    while offset < block.len() {
        let key = take_holt_bytes(block, &mut offset)?;
        let value = take_holt_bytes(block, &mut offset)?;
        let attempted = records
            .checked_add(1)
            .ok_or_else(|| "Holt checkpoint record count overflows usize".to_owned())?;
        if attempted > max_records {
            return Err(format!(
                "Holt checkpoint has at least {attempted} records; maximum is {max_records}"
            ));
        }
        if key.len() > limits.max_key_bytes {
            return Err(format!(
                "Holt checkpoint key is {} bytes; configured maximum is {}",
                key.len(),
                limits.max_key_bytes
            ));
        }
        if value.len() > limits.max_value_bytes {
            return Err(format!(
                "Holt checkpoint value is {} bytes; configured maximum is {}",
                value.len(),
                limits.max_value_bytes
            ));
        }
        if previous_key.is_some_and(|previous| key <= previous) {
            return Err("Holt checkpoint keys are not in strict canonical order".to_owned());
        }
        previous_key = Some(key);
        *records = attempted;
    }
    Ok(())
}

fn prepare_fresh_target(target: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(format!(
                "{} is not a fresh Holt checkpoint directory",
                target.display()
            ));
        }
        Ok(_) => {
            let mut entries = std::fs::read_dir(target)
                .map_err(|error| format!("inspect checkpoint target: {error}"))?;
            if entries
                .next()
                .transpose()
                .map_err(|error| format!("inspect checkpoint target: {error}"))?
                .is_some()
            {
                return Err(format!(
                    "Holt checkpoint target {} is not empty",
                    target.display()
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = usable_parent(target);
            let metadata = std::fs::symlink_metadata(parent)
                .map_err(|error| format!("inspect checkpoint target parent: {error}"))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err("Holt checkpoint target parent is not a directory".to_owned());
            }
            std::fs::create_dir(target)
                .map_err(|error| format!("create checkpoint target directory: {error}"))?;
            sync_directory(parent)
                .map_err(|error| format!("sync checkpoint target parent: {error}"))?;
        }
        Err(error) => return Err(format!("inspect checkpoint target: {error}")),
    }
    Ok(())
}

fn install_sentinel_exists(target: &Path) -> Result<bool, std::io::Error> {
    marker_exists(&install_sentinel_path(target))
}

pub(crate) fn install_sentinel_path(target: &Path) -> PathBuf {
    target.join(INSTALL_SENTINEL_FILE)
}

pub(crate) fn complete_marker_path(target: &Path) -> PathBuf {
    target.join(COMPLETE_MARKER_FILE)
}

pub(crate) fn adopted_marker_path(target: &Path) -> PathBuf {
    target.join(ADOPTED_MARKER_FILE)
}

fn marker_exists(path: &Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn inspect_marker_exists(path: &Path, phase: &str) -> Result<bool, StoreError> {
    marker_exists(path).map_err(|error| {
        StoreError::Unavailable(format!(
            "inspect {phase} Holt checkpoint marker {}: {error}",
            path.display()
        ))
    })
}

fn reject_incomplete_install_marker(path: &Path) -> Result<(), StoreError> {
    let installing = install_sentinel_path(path);
    if inspect_marker_exists(&installing, "incomplete")? {
        Err(StoreError::Corrupt(format!(
            "HoltStore directory {} has an incomplete checkpoint installation marker and must be discarded",
            path.display()
        )))
    } else {
        Ok(())
    }
}

fn verify_identity_marker(
    path: &Path,
    magic: &[u8],
    expected: &HoltRecoveryStagingIdentity,
    phase: &str,
    target: &Path,
) -> Result<(), StoreError> {
    verify_marker_bytes(path, &expected.marker_bytes(magic)).map_err(|reason| {
        StoreError::Corrupt(format!(
            "HoltStore directory {} {phase} checkpoint marker is invalid: {reason}",
            target.display()
        ))
    })
}

fn verify_adopted_marker_catalog(
    path: &Path,
    expected_catalog: CheckpointCatalogCommitment,
    target: &Path,
) -> Result<(), StoreError> {
    let identity_len = COMPLETE_MARKER_MAGIC.len() + RECOVERY_STAGING_DIGEST_BYTES + 32;
    let expected_len = identity_len + RECOVERY_STAGING_MARKER_CHECKSUM_BYTES;
    let bytes = read_exact_marker(path, expected_len).map_err(|reason| {
        StoreError::Corrupt(format!(
            "HoltStore directory {} adopted checkpoint marker is invalid: {reason}",
            target.display()
        ))
    })?;
    if !bytes.starts_with(COMPLETE_MARKER_MAGIC) {
        return Err(StoreError::Corrupt(format!(
            "HoltStore directory {} adopted checkpoint marker has wrong magic",
            target.display()
        )));
    }
    if bytes[identity_len..] != checkpoint_marker_checksum(&bytes[..identity_len]) {
        return Err(StoreError::Corrupt(format!(
            "HoltStore directory {} adopted checkpoint marker checksum is invalid",
            target.display()
        )));
    }
    let catalog_offset = COMPLETE_MARKER_MAGIC.len() + RECOVERY_STAGING_DIGEST_BYTES;
    if &bytes[catalog_offset..catalog_offset + 32] != expected_catalog.as_bytes() {
        return Err(StoreError::Corrupt(format!(
            "HoltStore directory {} adopted checkpoint marker has a foreign catalog",
            target.display()
        )));
    }
    Ok(())
}

fn recovery_staging_identity(
    checkpoint: &StoreCheckpointEnvelope,
    catalog: CheckpointCatalogCommitment,
) -> HoltRecoveryStagingIdentity {
    let mut envelope = Sha256::new();
    envelope.update(b"nokv.store-checkpoint-envelope.v1\0");
    envelope.update((checkpoint.format_id().as_str().len() as u64).to_be_bytes());
    envelope.update(checkpoint.format_id().as_str().as_bytes());
    envelope.update(checkpoint.catalog_commitment().as_bytes());
    envelope.update((checkpoint.image().len() as u64).to_be_bytes());
    envelope.update(checkpoint.image());
    let envelope_digest: [u8; 32] = envelope.finalize().into();
    HoltRecoveryStagingIdentity {
        envelope_digest,
        catalog_commitment: catalog,
    }
}

fn write_durable_marker(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let mut marker = OpenOptions::new().write(true).create_new(true).open(path)?;
    marker.write_all(bytes)?;
    marker.sync_all()?;
    drop(marker);
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

fn verify_marker_bytes(path: &Path, expected: &[u8]) -> Result<(), String> {
    let actual = read_exact_marker(path, expected.len())?;
    if actual == expected {
        Ok(())
    } else {
        Err("marker does not match expected checkpoint identity".to_owned())
    }
}

fn read_exact_marker(path: &Path, expected_len: usize) -> Result<Vec<u8>, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect marker {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("marker is not a regular file".to_owned());
    }
    if metadata.len() != expected_len as u64 {
        return Err("marker length does not match expected identity".to_owned());
    }
    let mut actual = vec![0_u8; expected_len];
    File::open(path)
        .and_then(|mut file| file.read_exact(&mut actual))
        .map_err(|error| format!("read marker {}: {error}", path.display()))?;
    Ok(actual)
}

fn require_only_sentinel(target: &Path) -> Result<(), String> {
    let expected = install_sentinel_path(target);
    let entries = std::fs::read_dir(target)
        .map_err(|error| format!("inspect checkpoint target after marker creation: {error}"))?;
    let mut count = 0_usize;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("inspect checkpoint target entry: {error}"))?
            .path();
        count += 1;
        if path != expected {
            return Err("checkpoint target changed before Holt installation".to_owned());
        }
    }
    if count != 1 {
        return Err("checkpoint installation marker disappeared before Holt open".to_owned());
    }
    Ok(())
}

fn require_exact_catalog(db: &DB, trees: &BTreeMap<Keyspace, String>) -> Result<(), String> {
    let actual = db
        .list_trees()
        .map_err(|error| error.to_string())?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected = trees.values().cloned().collect::<BTreeSet<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err("installed Holt tree registry does not match the catalog commitment".to_owned())
    }
}

fn remove_install_sentinel(target: &Path) -> Result<(), std::io::Error> {
    std::fs::remove_file(install_sentinel_path(target))?;
    #[cfg(test)]
    {
        let mut failpoint = FAIL_FINALIZE_DIRECTORY_SYNC
            .lock()
            .expect("lock checkpoint finalize sync failpoint");
        if failpoint.as_deref() == Some(target) {
            failpoint.take();
            return Err(std::io::Error::other(
                "injected directory sync failure after sentinel unlink",
            ));
        }
    }
    sync_directory(target)
}

fn restore_install_sentinel(path: &Path, bytes: &[u8]) {
    #[cfg(test)]
    {
        let target = path.parent().unwrap_or_else(|| Path::new("."));
        let mut failpoint = FAIL_INSTALL_SENTINEL_RESTORE
            .lock()
            .expect("lock checkpoint sentinel restore failpoint");
        if failpoint.as_deref() == Some(target) {
            failpoint.take();
            return;
        }
    }
    let restored = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .and_then(|mut file| {
            file.write_all(bytes)?;
            file.sync_all()
        });
    if restored.is_ok() {
        if let Some(parent) = path.parent() {
            let _ = sync_directory(parent);
        }
    }
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    File::open(path)?.sync_all()
}

fn usable_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
