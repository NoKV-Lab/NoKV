//! Durable restore-to-fork metadata state and service entry points.
//!
//! Restore state is private to `nokv-meta` and stored in `RecordFamily::System`.
//! The public API only exposes a completed outcome; no partially materialized
//! inode is a valid user-visible result.

use super::*;
use crate::command::{ReadItem, ScanItem};
use std::collections::{BTreeMap, HashMap};

pub(super) const RESTORE_FORMAT_VERSION: u8 = 1;
pub(super) const MAX_RESTORE_PATH_BYTES: usize = 4096;
pub(super) const MAX_RESTORE_INITIALIZATION_ENTRIES: usize = 1024;
pub(super) const MAX_RESTORE_INITIALIZATION_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_RESTORE_SUBTREE_ENTRIES: usize = 1_000_000;
pub(super) const RESTORE_BATCH_ENTRIES: usize = 64;
const RESTORE_BACKLOG_PAGE_ROWS: usize = 256;

const OPERATION_MAGIC: &[u8; 8] = b"NKRSTOP\0";
const STAGING_MEMBER_MAGIC: &[u8; 8] = b"NKRSTGM\0";
const OPERATION_KEY_LABEL: &[u8] = b"restore-operation\0";
const CLAIM_KEY_LABEL: &[u8] = b"restore-destination\0";
const ACTIVE_KEY_LABEL: &[u8] = b"restore-to-fork-v1-active\0";
const ACTIVATION_FENCE_KEY_LABEL: &[u8] = b"restore-to-fork-v1-activation-fence\0";
const STAGING_KEY_LABEL: &[u8] = b"restore-staging-member\0";
const STAGING_INVERSE_KEY_LABEL: &[u8] = b"restore-staging-inode\0";
const STAGING_INVERSE_OWNER_KEY_LABEL: &[u8] = b"restore-staging-inverse-owner\0";
const ROOT_KEY_LABEL: &[u8] = b"restore-root-index\0";
const BASE_OWNER_KEY_LABEL: &[u8] = b"restore-base-owner\0";
const BASE_INVERSE_KEY_LABEL: &[u8] = b"restore-base-inverse\0";
const BASE_INVERSE_OWNER_KEY_LABEL: &[u8] = b"restore-base-inverse-owner\0";
const BASE_SEAL_KEY_LABEL: &[u8] = b"restore-base-seal\0";
const CLEANUP_KEY_LABEL: &[u8] = b"restore-cleanup-job\0";
const CLEANUP_MAGIC: &[u8; 8] = b"NKRCLNJ\0";
const INIT_UPLOAD_KEY_LABEL: &[u8] = b"restore-init-upload-intent\0";
const INIT_UPLOAD_MAGIC: &[u8; 8] = b"NKRINIT\0";
const INIT_UPLOAD_TOMBSTONE_KEY_LABEL: &[u8] = b"restore-init-upload-tombstone\0";
const INIT_UPLOAD_TOMBSTONE_MAGIC: &[u8; 8] = b"NKRITMB\0";
const INIT_UPLOAD_TOMBSTONE_CURSOR_KEY_LABEL: &[u8] = b"restore-init-upload-tombstone-cursor\0";
const RELEASE_KEY_LABEL: &[u8] = b"restore-release-job\0";
const RELEASE_MAGIC: &[u8; 8] = b"NKRRELJ\0";
const RELEASE_CURSOR_KEY_LABEL: &[u8] = b"restore-release-cursor\0";
const RELEASE_CURSOR_MAGIC: &[u8; 8] = b"NKRRCUR\0";
const RELEASE_QUARANTINE_KEY_LABEL: &[u8] = b"restore-release-quarantine\0";
const RELEASE_QUARANTINE_MAGIC: &[u8; 8] = b"NKRRELQ\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreState {
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreOutcome {
    pub operation_id: String,
    pub state: RestoreState,
    pub source_root: InodeId,
    pub destination_root: InodeId,
    pub snapshot_id: u64,
    pub read_version: u64,
    pub cleanup_pending: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreInitialization {
    pub remove_relative_paths: Vec<String>,
    pub files: Vec<RestoreInitializationFile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreInitializationFile {
    pub relative_path: String,
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreOperationState {
    Preparing,
    ReadyToAttach,
    Complete,
    Cleaning,
    Discarding,
    Releasing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreOperation {
    pub(super) operation_digest: [u8; 32],
    pub(super) initialization_digest: [u8; 32],
    pub(super) state: RestoreOperationState,
    pub(super) source_root: InodeId,
    pub(super) destination_root: InodeId,
    pub(super) snapshot_id: u64,
    pub(super) read_version: u64,
    pub(super) created_version: u64,
    pub(super) ref_set_id: u64,
    pub(super) source_path: String,
    pub(super) destination_path: String,
}

struct RestorePathProof {
    inode: InodeId,
    predicates: Vec<PredicateRef>,
}

struct RestoreHold<'a> {
    operation: &'a RestoreOperation,
    object_reference: ObjectReferenceMutation,
    pin_version: Version,
    source_attr: &'a InodeAttr,
    source_predicates: &'a [PredicateRef],
    destination_parent: InodeId,
    destination_name: &'a DentryName,
    destination_predicates: &'a [PredicateRef],
}

struct RestoreInitializationRemoveCommand<'a> {
    operation: &'a RestoreOperation,
    parent: InodeId,
    name: &'a DentryName,
    entry: &'a DentryWithAttr,
    dentry_version: Version,
    staging: &'a RestoreStagingProof,
    chunks: &'a [ChunkManifest],
    xattr_keys: &'a [Vec<u8>],
    version: Version,
}

struct RestoreInitializationPublishCommand<'a> {
    operation: &'a RestoreOperation,
    parent: InodeId,
    name: &'a DentryName,
    file: &'a RestoreInitializationFile,
    existing: Option<&'a (DentryWithAttr, Version)>,
    inode: InodeId,
    object_generation: Version,
    intent_version: Version,
    object_reference: ObjectReferenceMutation,
    body: &'a BodyDescriptor,
    chunks: &'a [ChunkManifest],
    old_chunks: &'a [ChunkManifest],
    staging: Option<&'a RestoreStagingProof>,
    version: Version,
}

struct RestoreCloneFrame {
    source: InodeId,
    destination: InodeId,
    relative_components: Vec<DentryName>,
    after: Option<DentryName>,
}

struct RestoreCloneEntry {
    source: DentryWithAttr,
    destination: InodeId,
    relative_path: String,
    body: Option<BodyDescriptor>,
    chunks: Vec<ChunkManifest>,
}

#[derive(Clone)]
struct RestoreReferenceReleaseEntry {
    owner_key: Vec<u8>,
    owner_version: Version,
    inverse_key: Vec<u8>,
    inverse_version: Version,
    inverse_owner_key: Vec<u8>,
    inverse_owner_version: Version,
    reference: super::restore_gc::RestoreBaseReference,
    object_digest: [u8; 32],
    identity: (InodeId, u64, u64, u64),
}

#[derive(Clone)]
struct RestoreReferenceReleaseGuard {
    owner_key: Vec<u8>,
    owner_version: Version,
    inverse_key: Vec<u8>,
    inverse_version: Version,
    inverse_owner_key: Vec<u8>,
    inverse_owner_version: Version,
    reference: super::restore_gc::RestoreBaseReference,
    object_digest: [u8; 32],
    identity: (InodeId, u64, u64, u64),
}

enum RestoreReferenceReleaseGuardValidation {
    Valid(Box<RestoreReferenceReleaseGuard>),
    Corrupt {
        reason: String,
        object_digest: Option<[u8; 32]>,
    },
}

impl From<&RestoreReferenceReleaseEntry> for RestoreReferenceReleaseGuard {
    fn from(entry: &RestoreReferenceReleaseEntry) -> Self {
        Self {
            owner_key: entry.owner_key.clone(),
            owner_version: entry.owner_version,
            inverse_key: entry.inverse_key.clone(),
            inverse_version: entry.inverse_version,
            inverse_owner_key: entry.inverse_owner_key.clone(),
            inverse_owner_version: entry.inverse_owner_version,
            reference: entry.reference.clone(),
            object_digest: entry.object_digest,
            identity: entry.identity,
        }
    }
}

fn restore_release_error_is_retryable(error: &MetadError) -> bool {
    matches!(
        error,
        MetadError::Metadata(MetadataError::PredicateFailed)
            | MetadError::Metadata(MetadataError::Backend(_))
            | MetadError::SyncLogArchiveFailed { .. }
            | MetadError::StaleOwnerEpoch { .. }
            | MetadError::LeaseExpired { .. }
            | MetadError::NotOwner { .. }
            | MetadError::MetadataCheckpointInstallUncertain
    )
}

struct RestoreReferenceReleaseCommand<'a> {
    operation: &'a RestoreOperation,
    operation_version: Version,
    job: &'a RestoreReleaseJob,
    job_version: Version,
    entries: &'a [RestoreReferenceReleaseEntry],
    defer_gc_for: Option<[u8; 32]>,
    deferred_guard: Option<&'a RestoreReferenceReleaseGuard>,
    object_reference: ObjectReferenceMutation,
    version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreStagingMember {
    pub(super) operation_digest: [u8; 32],
    pub(super) source_inode: Option<InodeId>,
    pub(super) destination_inode: InodeId,
    pub(super) destination_parent: Option<InodeId>,
    pub(super) name: Option<DentryName>,
    pub(super) relative_path: String,
    /// Cursor over ordinary (canonical) PathIndex rows created after attach.
    /// Snapshot-materialized members use only the private restore overlay and
    /// start complete; dynamically enrolled members are drained page by page.
    pub(super) canonical_index_cursor: Vec<u8>,
    pub(super) canonical_index_complete: bool,
    /// Physical ChunkManifest row currently being drained during release.
    /// The row is retained until every owned block has been durably enqueued.
    pub(super) manifest_cursor: Vec<u8>,
    /// Ordinal of the next canonical owned block in `manifest_cursor`.
    pub(super) manifest_block_cursor: u64,
}

struct RestoreStagingProof {
    member: RestoreStagingMember,
    member_version: Version,
    inverse_version: Version,
    inverse_owner_version: Version,
}

enum RestoreNamespaceActivityFence {
    Inactive(PredicateRef),
    Active { key: Vec<u8>, version: Version },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreInitUploadIntent {
    pub(super) operation_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) inode: InodeId,
    pub(super) generation: u64,
    pub(super) size: u64,
    pub(super) relative_path: String,
    /// 0 = upload may still become visible; 1 = the permanent global cleanup
    /// tombstone is durable, so this operation-local intent may be retired.
    pub(super) cleanup_pass: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreInitUploadTombstone {
    pub(super) operation_digest: [u8; 32],
    pub(super) initialization_digest: [u8; 32],
    pub(super) inode: InodeId,
    pub(super) generation: u64,
    pub(super) size: u64,
    pub(super) relative_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreCleanupJob {
    pub(super) operation_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) index_complete: bool,
    pub(super) index_cursor: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreReleasePhase {
    ExactReferences,
    Members,
    Overlay,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreReleaseJob {
    pub(super) operation_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) phase: RestoreReleasePhase,
    pub(super) cursor: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreReleaseWorkerCursor {
    /// Frozen metadata row-version frontier for one fair scan cycle. Jobs
    /// inserted or rewritten after this boundary are deferred until the next
    /// cycle, including old restores whose release starts late.
    pub(super) cycle_high_water: u64,
    /// Last physical release-job key visited in this cycle. The row may have
    /// been deleted or rewritten after the cursor was committed.
    pub(super) start_after: Vec<u8>,
}

pub(super) struct RestoreReleaseTransition {
    pub(super) predicates: Vec<PredicateRef>,
    pub(super) mutations: Vec<Mutation>,
}

pub(super) struct RestoreNamespaceEnrollmentPlan {
    pub(super) predicates: Vec<PredicateRef>,
    pub(super) mutations: Vec<Mutation>,
}

/// Stable public request identity. Initialization is intentionally excluded:
/// changing it for an existing request is a destination conflict, not a new
/// operation.
pub fn restore_operation_id(
    mount: MountId,
    source_path: &str,
    snapshot_id: u64,
    destination_path: &str,
) -> Result<String, MetadError> {
    let source = canonical_path(&parse_absolute_path(source_path)?)?;
    let destination = canonical_path(&parse_absolute_path(destination_path)?)?;
    Ok(format!(
        "restore-{}",
        hex_digest(&restore_operation_digest(
            mount,
            &source,
            snapshot_id,
            &destination,
        ))
    ))
}

pub(super) fn restore_operation_digest(
    mount: MountId,
    source_path: &str,
    snapshot_id: u64,
    destination_path: &str,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-restore-to-fork-request-v1\0");
    hasher.update(mount.get().to_be_bytes());
    put_hash_field(&mut hasher, source_path.as_bytes());
    hasher.update(snapshot_id.to_be_bytes());
    put_hash_field(&mut hasher, destination_path.as_bytes());
    hasher.finalize().into()
}

pub(super) fn restore_operation_key(mount: MountId, digest: &[u8; 32]) -> Vec<u8> {
    system_key_with_digest(mount, OPERATION_KEY_LABEL, digest)
}

pub(super) fn restore_destination_claim_key(mount: MountId, path: &str) -> Vec<u8> {
    let digest: [u8; 32] = Sha256::digest(path.as_bytes()).into();
    system_key_with_digest(mount, CLAIM_KEY_LABEL, &digest)
}

pub(super) fn restore_active_key(mount: MountId) -> Vec<u8> {
    system_key(mount, ACTIVE_KEY_LABEL)
}

pub(super) fn restore_activation_fence_key(mount: MountId) -> Vec<u8> {
    system_key(mount, ACTIVATION_FENCE_KEY_LABEL)
}

pub(super) fn restore_staging_member_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, STAGING_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_staging_member_key(
    mount: MountId,
    ref_set_id: u64,
    inode: InodeId,
) -> Vec<u8> {
    let mut key = restore_staging_member_prefix(mount, ref_set_id);
    key.extend_from_slice(&inode.get().to_be_bytes());
    key
}

pub(super) fn restore_staging_inode_key(mount: MountId, inode: InodeId) -> Vec<u8> {
    system_key_with_u64(mount, STAGING_INVERSE_KEY_LABEL, inode.get())
}

pub(super) fn restore_staging_inverse_owner_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, STAGING_INVERSE_OWNER_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_staging_inverse_owner_key(
    mount: MountId,
    ref_set_id: u64,
    inode: InodeId,
) -> Vec<u8> {
    let mut key = restore_staging_inverse_owner_prefix(mount, ref_set_id);
    key.extend_from_slice(&inode.get().to_be_bytes());
    key
}

pub(super) fn encode_restore_staging_member(
    member: &RestoreStagingMember,
) -> Result<Vec<u8>, MetadError> {
    validate_restore_staging_member(member)?;
    let relative = member.relative_path.as_bytes();
    let name = member.name.as_ref().map_or(&[][..], DentryName::as_bytes);
    let name_len = u32::try_from(name.len())
        .map_err(|_| MetadError::Codec("restore member name is too long".to_owned()))?;
    let relative_len = u32::try_from(relative.len())
        .map_err(|_| MetadError::Codec("restore relative path is too long".to_owned()))?;
    let cursor_len = u32::try_from(member.canonical_index_cursor.len())
        .map_err(|_| MetadError::Codec("restore member index cursor is too long".to_owned()))?;
    let manifest_cursor_len = u32::try_from(member.manifest_cursor.len())
        .map_err(|_| MetadError::Codec("restore member manifest cursor is too long".to_owned()))?;
    let mut value = Vec::with_capacity(
        92 + name.len()
            + relative.len()
            + member.canonical_index_cursor.len()
            + member.manifest_cursor.len(),
    );
    value.extend_from_slice(STAGING_MEMBER_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&member.operation_digest);
    value.push(u8::from(member.source_inode.is_some()));
    value.extend_from_slice(&member.source_inode.map_or(0, InodeId::get).to_be_bytes());
    value.extend_from_slice(&member.destination_inode.get().to_be_bytes());
    value.push(u8::from(member.destination_parent.is_some()));
    value.extend_from_slice(
        &member
            .destination_parent
            .map_or(0, InodeId::get)
            .to_be_bytes(),
    );
    value.extend_from_slice(&name_len.to_be_bytes());
    value.extend_from_slice(name);
    value.extend_from_slice(&relative_len.to_be_bytes());
    value.extend_from_slice(relative);
    value.push(u8::from(member.canonical_index_complete));
    value.extend_from_slice(&cursor_len.to_be_bytes());
    value.extend_from_slice(&member.canonical_index_cursor);
    value.extend_from_slice(&manifest_cursor_len.to_be_bytes());
    value.extend_from_slice(&member.manifest_cursor);
    value.extend_from_slice(&member.manifest_block_cursor.to_be_bytes());
    Ok(value)
}

pub(super) fn decode_restore_staging_member(
    value: &[u8],
) -> Result<RestoreStagingMember, MetadError> {
    let mut decoder = RestoreDecoder::new(value);
    if decoder.take(8)? != STAGING_MEMBER_MAGIC {
        return Err(MetadError::Codec(
            "invalid restore staging member magic".to_owned(),
        ));
    }
    if decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "unsupported restore staging member version".to_owned(),
        ));
    }
    let operation_digest = decoder.array_32()?;
    let source_tag = decoder.u8()?;
    let source_raw = decoder.u64()?;
    let source_inode = match (source_tag, source_raw) {
        (0, 0) => None,
        (1, raw) => Some(InodeId::new(raw)?),
        _ => {
            return Err(MetadError::Codec(
                "restore staging member has an invalid source inode tag".to_owned(),
            ))
        }
    };
    let destination_inode = InodeId::new(decoder.u64()?)?;
    let parent_tag = decoder.u8()?;
    let parent_raw = decoder.u64()?;
    let destination_parent = match (parent_tag, parent_raw) {
        (0, 0) => None,
        (1, raw) => Some(InodeId::new(raw)?),
        _ => {
            return Err(MetadError::Codec(
                "restore staging member has an invalid parent inode tag".to_owned(),
            ))
        }
    };
    let name_bytes = decoder.bytes()?;
    let name = if name_bytes.is_empty() {
        None
    } else {
        Some(DentryName::new(name_bytes).map_err(|error| {
            MetadError::Codec(format!("invalid restore staging member name: {error}"))
        })?)
    };
    let relative_path = decoder.string()?;
    let canonical_index_complete = match decoder.u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(MetadError::Codec(
                "restore staging member has an invalid index-complete tag".to_owned(),
            ))
        }
    };
    let canonical_index_cursor = decoder.bytes()?;
    let manifest_cursor = decoder.bytes()?;
    let manifest_block_cursor = decoder.u64()?;
    let member = RestoreStagingMember {
        operation_digest,
        source_inode,
        destination_inode,
        destination_parent,
        name,
        relative_path,
        canonical_index_cursor,
        canonical_index_complete,
        manifest_cursor,
        manifest_block_cursor,
    };
    decoder.finish()?;
    validate_restore_staging_member(&member)?;
    Ok(member)
}

fn validate_restore_staging_member(member: &RestoreStagingMember) -> Result<(), MetadError> {
    if member.relative_path.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::Codec(
            "restore staging member relative path is too long".to_owned(),
        ));
    }
    if member.canonical_index_cursor.len() > MAX_RESTORE_PATH_BYTES + 16 {
        return Err(MetadError::Codec(
            "restore staging member index cursor is too long".to_owned(),
        ));
    }
    if member.canonical_index_complete && !member.canonical_index_cursor.is_empty() {
        return Err(MetadError::Codec(
            "completed restore member index scan retains a cursor".to_owned(),
        ));
    }
    if member.manifest_cursor.len() > MAX_RESTORE_PATH_BYTES + 16 {
        return Err(MetadError::Codec(
            "restore staging member manifest cursor is too long".to_owned(),
        ));
    }
    if member.manifest_cursor.is_empty() != (member.manifest_block_cursor == 0) {
        return Err(MetadError::Codec(
            "restore staging member manifest cursor shape is invalid".to_owned(),
        ));
    }
    if !member.relative_path.is_empty()
        && canonical_restore_relative_path(&member.relative_path)? != member.relative_path
    {
        return Err(MetadError::Codec(
            "restore staging member relative path is not canonical".to_owned(),
        ));
    }
    match (&member.destination_parent, &member.name) {
        (None, None) if member.relative_path.is_empty() => {}
        (Some(_), Some(_)) if member.source_inode.is_none() && member.relative_path.is_empty() => {}
        (Some(_), Some(name)) if !member.relative_path.is_empty() => {
            let components = restore_relative_components(&member.relative_path)?;
            if components.last() != Some(name) {
                return Err(MetadError::Codec(
                    "restore staging member name does not match relative path".to_owned(),
                ));
            }
        }
        _ => {
            return Err(MetadError::Codec(
                "restore staging member parent/name shape is invalid".to_owned(),
            ))
        }
    }
    Ok(())
}

pub(super) fn encode_restore_staging_inverse(operation: &RestoreOperation) -> Vec<u8> {
    let mut value = Vec::with_capacity(40);
    value.extend_from_slice(&operation.operation_digest);
    value.extend_from_slice(&operation.ref_set_id.to_be_bytes());
    value
}

pub(super) fn decode_restore_staging_inverse(value: &[u8]) -> Result<([u8; 32], u64), MetadError> {
    if value.len() != 40 {
        return Err(MetadError::Codec(
            "restore staging inverse has an invalid length".to_owned(),
        ));
    }
    let digest = value[..32].try_into().expect("digest width");
    let ref_set_id = u64::from_be_bytes(value[32..].try_into().expect("u64 width"));
    if ref_set_id == 0 {
        return Err(MetadError::Codec(
            "restore staging inverse has a zero set id".to_owned(),
        ));
    }
    Ok((digest, ref_set_id))
}

pub(super) fn restore_root_index_key(mount: MountId, root: InodeId) -> Vec<u8> {
    system_key_with_u64(mount, ROOT_KEY_LABEL, root.get())
}

pub(super) fn restore_base_owner_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, BASE_OWNER_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_base_owner_key(
    mount: MountId,
    ref_set_id: u64,
    object_digest: &[u8; 32],
    borrower_inode: InodeId,
    borrower_generation: u64,
) -> Vec<u8> {
    let mut key = restore_base_owner_prefix(mount, ref_set_id);
    key.extend_from_slice(object_digest);
    key.extend_from_slice(&borrower_inode.get().to_be_bytes());
    key.extend_from_slice(&borrower_generation.to_be_bytes());
    key
}

fn restore_base_owner_object_digest_from_key(
    mount: MountId,
    ref_set_id: u64,
    key: &[u8],
) -> Option<[u8; 32]> {
    let prefix = restore_base_owner_prefix(mount, ref_set_id);
    if !key.starts_with(&prefix) || key.len() != prefix.len() + 32 + 8 + 8 {
        return None;
    }
    key[prefix.len()..prefix.len() + 32].try_into().ok()
}

pub(super) fn restore_base_inverse_prefix(mount: MountId, object_digest: &[u8; 32]) -> Vec<u8> {
    let mut key = system_key(mount, BASE_INVERSE_KEY_LABEL);
    key.extend_from_slice(object_digest);
    key
}

pub(super) fn restore_base_inverse_key(
    mount: MountId,
    object_digest: &[u8; 32],
    ref_set_id: u64,
    borrower_inode: InodeId,
    borrower_generation: u64,
) -> Vec<u8> {
    let mut key = restore_base_inverse_prefix(mount, object_digest);
    key.extend_from_slice(&ref_set_id.to_be_bytes());
    key.extend_from_slice(&borrower_inode.get().to_be_bytes());
    key.extend_from_slice(&borrower_generation.to_be_bytes());
    key
}

pub(super) fn restore_base_inverse_owner_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, BASE_INVERSE_OWNER_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_base_inverse_owner_key(
    mount: MountId,
    ref_set_id: u64,
    object_digest: &[u8; 32],
    borrower_inode: InodeId,
    borrower_generation: u64,
) -> Vec<u8> {
    let mut key = restore_base_inverse_owner_prefix(mount, ref_set_id);
    key.extend_from_slice(object_digest);
    key.extend_from_slice(&borrower_inode.get().to_be_bytes());
    key.extend_from_slice(&borrower_generation.to_be_bytes());
    key
}

pub(super) fn restore_base_seal_key(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, BASE_SEAL_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_cleanup_job_key(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, CLEANUP_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_init_upload_intent_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, INIT_UPLOAD_KEY_LABEL, ref_set_id)
}

pub(super) fn restore_init_upload_intent_key(
    mount: MountId,
    ref_set_id: u64,
    inode: InodeId,
    generation: u64,
) -> Vec<u8> {
    let mut key = restore_init_upload_intent_prefix(mount, ref_set_id);
    key.extend_from_slice(&inode.get().to_be_bytes());
    key.extend_from_slice(&generation.to_be_bytes());
    key
}

pub(super) fn restore_init_upload_tombstone_prefix(mount: MountId) -> Vec<u8> {
    system_key(mount, INIT_UPLOAD_TOMBSTONE_KEY_LABEL)
}

pub(super) fn restore_init_upload_tombstone_key(
    mount: MountId,
    operation_digest: &[u8; 32],
    inode: InodeId,
    generation: u64,
) -> Vec<u8> {
    let mut key = restore_init_upload_tombstone_prefix(mount);
    key.extend_from_slice(operation_digest);
    key.extend_from_slice(&inode.get().to_be_bytes());
    key.extend_from_slice(&generation.to_be_bytes());
    key
}

pub(super) fn restore_init_upload_tombstone_cursor_key(mount: MountId) -> Vec<u8> {
    system_key(mount, INIT_UPLOAD_TOMBSTONE_CURSOR_KEY_LABEL)
}

pub(super) fn restore_release_job_prefix(mount: MountId) -> Vec<u8> {
    system_key(mount, RELEASE_KEY_LABEL)
}

pub(super) fn restore_release_job_key(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    system_key_with_u64(mount, RELEASE_KEY_LABEL, ref_set_id)
}

fn restore_release_job_ref_set_id(mount: MountId, key: &[u8]) -> Option<u64> {
    let prefix = restore_release_job_prefix(mount);
    if !key.starts_with(&prefix) || key.len() != prefix.len() + 8 {
        return None;
    }
    let ref_set_id = u64::from_be_bytes(key[prefix.len()..].try_into().ok()?);
    (ref_set_id != 0).then_some(ref_set_id)
}

pub(super) fn restore_release_cursor_key(mount: MountId) -> Vec<u8> {
    system_key(mount, RELEASE_CURSOR_KEY_LABEL)
}

pub(super) fn restore_release_quarantine_prefix(mount: MountId) -> Vec<u8> {
    system_key(mount, RELEASE_QUARANTINE_KEY_LABEL)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreReleaseQuarantineScope {
    Diagnostic,
    Object([u8; 32]),
    MountWide,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreReleaseQuarantine {
    pub(super) family: RecordFamily,
    pub(super) scope: RestoreReleaseQuarantineScope,
    pub(super) original_version: Version,
    pub(super) original_key: Vec<u8>,
    pub(super) original_value: Vec<u8>,
    pub(super) reason: String,
}

pub(super) fn restore_release_object_quarantine_prefix(
    mount: MountId,
    object_digest: &[u8; 32],
) -> Vec<u8> {
    let mut key = restore_release_quarantine_prefix(mount);
    key.push(2);
    key.extend_from_slice(object_digest);
    key
}

pub(super) fn restore_release_mount_wide_quarantine_prefix(mount: MountId) -> Vec<u8> {
    let mut key = restore_release_quarantine_prefix(mount);
    key.push(3);
    key
}

fn restore_release_quarantine_key(
    mount: MountId,
    family: RecordFamily,
    row: &crate::command::ScanItem,
    scope: RestoreReleaseQuarantineScope,
) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-restore-release-quarantine-v1\0");
    hasher.update([restore_record_family_tag(family)]);
    hasher.update((row.key.len() as u64).to_be_bytes());
    hasher.update(&row.key);
    hasher.update(row.version.get().to_be_bytes());
    hasher.update((row.value.0.len() as u64).to_be_bytes());
    hasher.update(&row.value.0);
    let digest: [u8; 32] = hasher.finalize().into();
    let mut key = match scope {
        RestoreReleaseQuarantineScope::Diagnostic => {
            let mut key = restore_release_quarantine_prefix(mount);
            key.push(1);
            key
        }
        RestoreReleaseQuarantineScope::Object(object_digest) => {
            restore_release_object_quarantine_prefix(mount, &object_digest)
        }
        RestoreReleaseQuarantineScope::MountWide => {
            restore_release_mount_wide_quarantine_prefix(mount)
        }
    };
    key.extend_from_slice(&digest);
    key
}

fn encode_restore_release_quarantine(
    family: RecordFamily,
    row: &crate::command::ScanItem,
    reason: &str,
    scope: RestoreReleaseQuarantineScope,
) -> Result<Vec<u8>, MetadError> {
    let key_len = u32::try_from(row.key.len())
        .map_err(|_| MetadError::Codec("restore release quarantine key is too long".to_owned()))?;
    let value_len = u32::try_from(row.value.0.len()).map_err(|_| {
        MetadError::Codec("restore release quarantine value is too long".to_owned())
    })?;
    let mut reason_len = reason.len().min(MAX_RESTORE_PATH_BYTES);
    while !reason.is_char_boundary(reason_len) {
        reason_len -= 1;
    }
    let reason = reason.as_bytes();
    let reason_len = u32::try_from(reason_len).map_err(|_| {
        MetadError::Codec("restore release quarantine reason is too long".to_owned())
    })?;
    let mut value = Vec::with_capacity(
        22_usize
            .saturating_add(row.key.len())
            .saturating_add(row.value.0.len())
            .saturating_add(reason.len().min(MAX_RESTORE_PATH_BYTES)),
    );
    value.extend_from_slice(RELEASE_QUARANTINE_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.push(restore_record_family_tag(family));
    match scope {
        RestoreReleaseQuarantineScope::Diagnostic => value.push(1),
        RestoreReleaseQuarantineScope::Object(object_digest) => {
            value.push(2);
            value.extend_from_slice(&object_digest);
        }
        RestoreReleaseQuarantineScope::MountWide => value.push(3),
    }
    value.extend_from_slice(&row.version.get().to_be_bytes());
    value.extend_from_slice(&key_len.to_be_bytes());
    value.extend_from_slice(&row.key);
    value.extend_from_slice(&value_len.to_be_bytes());
    value.extend_from_slice(&row.value.0);
    value.extend_from_slice(&reason_len.to_be_bytes());
    value.extend_from_slice(&reason[..reason_len as usize]);
    Ok(value)
}

fn restore_record_family_tag(family: RecordFamily) -> u8 {
    match family {
        RecordFamily::System => 0,
        RecordFamily::Mount => 1,
        RecordFamily::Inode => 2,
        RecordFamily::Dentry => 3,
        RecordFamily::Parent => 4,
        RecordFamily::Xattr => 5,
        RecordFamily::ChunkManifest => 6,
        RecordFamily::Session => 7,
        RecordFamily::PathIndex => 8,
        RecordFamily::Watch => 9,
        RecordFamily::Snapshot => 10,
        RecordFamily::Gc => 11,
        RecordFamily::CommandDedupe => 12,
        RecordFamily::History => 13,
        RecordFamily::ForkBinding => 14,
        RecordFamily::ForkShadow => 15,
    }
}

fn restore_record_family_from_tag(tag: u8) -> Result<RecordFamily, MetadError> {
    match tag {
        0 => Ok(RecordFamily::System),
        1 => Ok(RecordFamily::Mount),
        2 => Ok(RecordFamily::Inode),
        3 => Ok(RecordFamily::Dentry),
        4 => Ok(RecordFamily::Parent),
        5 => Ok(RecordFamily::Xattr),
        6 => Ok(RecordFamily::ChunkManifest),
        7 => Ok(RecordFamily::Session),
        8 => Ok(RecordFamily::PathIndex),
        9 => Ok(RecordFamily::Watch),
        10 => Ok(RecordFamily::Snapshot),
        11 => Ok(RecordFamily::Gc),
        12 => Ok(RecordFamily::CommandDedupe),
        13 => Ok(RecordFamily::History),
        14 => Ok(RecordFamily::ForkBinding),
        15 => Ok(RecordFamily::ForkShadow),
        tag => Err(MetadError::Codec(format!(
            "invalid restore quarantine record family {tag}"
        ))),
    }
}

pub(super) fn validate_restore_release_quarantine_row(
    mount: MountId,
    row: &crate::command::ScanItem,
) -> Result<RestoreReleaseQuarantine, MetadError> {
    let mut decoder = RestoreDecoder::new(&row.value.0);
    if decoder.take(8)? != RELEASE_QUARANTINE_MAGIC || decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "invalid restore release quarantine header".to_owned(),
        ));
    }
    let family = restore_record_family_from_tag(decoder.u8()?)?;
    let scope = match decoder.u8()? {
        1 => RestoreReleaseQuarantineScope::Diagnostic,
        2 => RestoreReleaseQuarantineScope::Object(decoder.array_32()?),
        3 => RestoreReleaseQuarantineScope::MountWide,
        tag => {
            return Err(MetadError::Codec(format!(
                "invalid restore release quarantine scope {tag}"
            )))
        }
    };
    let original_version = Version::new(decoder.u64()?)?;
    let original_key = decoder.bytes()?;
    let original_value = decoder.bytes()?;
    let reason_bytes = decoder.bytes()?;
    decoder.finish()?;
    if reason_bytes.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::Codec(
            "restore release quarantine reason is too long".to_owned(),
        ));
    }
    let reason = String::from_utf8(reason_bytes).map_err(|_| {
        MetadError::Codec("restore release quarantine reason is not utf-8".to_owned())
    })?;
    let original_row = crate::command::ScanItem {
        key: original_key.clone(),
        value: Value(original_value.clone()),
        version: original_version,
    };
    if row.key != restore_release_quarantine_key(mount, family, &original_row, scope) {
        return Err(MetadError::Codec(
            "restore release quarantine key changed identity".to_owned(),
        ));
    }
    // Re-encoding closes over all length fields and prevents accepting a
    // non-canonical envelope that merely hashes to the same physical key.
    if row.value.0 != encode_restore_release_quarantine(family, &original_row, &reason, scope)? {
        return Err(MetadError::Codec(
            "restore release quarantine value is not canonical".to_owned(),
        ));
    }
    Ok(RestoreReleaseQuarantine {
        family,
        scope,
        original_version,
        original_key,
        original_value,
        reason,
    })
}

fn encode_restore_release_job(job: &RestoreReleaseJob) -> Result<Vec<u8>, MetadError> {
    if job.ref_set_id == 0 || job.cursor.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::Codec(
            "restore release job has an invalid identity or cursor".to_owned(),
        ));
    }
    let cursor_len = u32::try_from(job.cursor.len())
        .map_err(|_| MetadError::Codec("restore release cursor is too long".to_owned()))?;
    let mut value = Vec::with_capacity(54 + job.cursor.len());
    value.extend_from_slice(RELEASE_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&job.operation_digest);
    value.extend_from_slice(&job.ref_set_id.to_be_bytes());
    value.push(match job.phase {
        RestoreReleasePhase::ExactReferences => 1,
        RestoreReleasePhase::Members => 2,
        RestoreReleasePhase::Overlay => 3,
    });
    value.extend_from_slice(&cursor_len.to_be_bytes());
    value.extend_from_slice(&job.cursor);
    Ok(value)
}

pub(super) fn decode_restore_release_job(value: &[u8]) -> Result<RestoreReleaseJob, MetadError> {
    let mut decoder = RestoreDecoder::new(value);
    if decoder.take(8)? != RELEASE_MAGIC || decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "invalid restore release job header".to_owned(),
        ));
    }
    let operation_digest = decoder.array_32()?;
    let ref_set_id = decoder.u64()?;
    let phase = match decoder.u8()? {
        1 => RestoreReleasePhase::ExactReferences,
        2 => RestoreReleasePhase::Members,
        3 => RestoreReleasePhase::Overlay,
        tag => {
            return Err(MetadError::Codec(format!(
                "invalid restore release phase {tag}"
            )))
        }
    };
    let cursor = decoder.bytes()?;
    decoder.finish()?;
    let job = RestoreReleaseJob {
        operation_digest,
        ref_set_id,
        phase,
        cursor,
    };
    encode_restore_release_job(&job)?;
    Ok(job)
}

pub(super) fn encode_restore_release_worker_cursor(
    mount: MountId,
    cursor: &RestoreReleaseWorkerCursor,
) -> Result<Vec<u8>, MetadError> {
    if cursor.cycle_high_water == 0 || cursor.start_after.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::Codec(
            "restore release worker cursor has an invalid boundary".to_owned(),
        ));
    }
    if !cursor.start_after.is_empty()
        && restore_release_job_ref_set_id(mount, &cursor.start_after).is_none()
    {
        return Err(MetadError::Codec(
            "restore release worker cursor has a non-canonical job key".to_owned(),
        ));
    }
    let start_after_len = u32::try_from(cursor.start_after.len())
        .map_err(|_| MetadError::Codec("restore release worker cursor is too long".to_owned()))?;
    let mut value = Vec::with_capacity(21 + cursor.start_after.len());
    value.extend_from_slice(RELEASE_CURSOR_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&cursor.cycle_high_water.to_be_bytes());
    value.extend_from_slice(&start_after_len.to_be_bytes());
    value.extend_from_slice(&cursor.start_after);
    Ok(value)
}

pub(super) fn decode_restore_release_worker_cursor(
    mount: MountId,
    value: &[u8],
) -> Result<RestoreReleaseWorkerCursor, MetadError> {
    let mut decoder = RestoreDecoder::new(value);
    if decoder.take(8)? != RELEASE_CURSOR_MAGIC || decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "invalid restore release worker cursor header".to_owned(),
        ));
    }
    let cursor = RestoreReleaseWorkerCursor {
        cycle_high_water: decoder.u64()?,
        start_after: decoder.bytes()?,
    };
    decoder.finish()?;
    // Reuse the canonical encoder as the single shape and keyspace validator.
    encode_restore_release_worker_cursor(mount, &cursor)?;
    Ok(cursor)
}

pub(super) fn decode_restore_release_worker_cursor_at_version(
    mount: MountId,
    value: &[u8],
    read_version: Version,
) -> Result<RestoreReleaseWorkerCursor, MetadError> {
    let cursor = decode_restore_release_worker_cursor(mount, value)?;
    if cursor.cycle_high_water > read_version.get() {
        return Err(MetadError::Codec(
            "restore release worker cursor is ahead of metadata".to_owned(),
        ));
    }
    Ok(cursor)
}

fn encode_restore_cleanup_job(job: &RestoreCleanupJob) -> Result<Vec<u8>, MetadError> {
    if job.ref_set_id == 0 || job.index_cursor.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::Codec(
            "restore cleanup job has an invalid identity or cursor".to_owned(),
        ));
    }
    if job.index_complete && !job.index_cursor.is_empty() {
        return Err(MetadError::Codec(
            "completed restore cleanup job has a cursor".to_owned(),
        ));
    }
    let cursor_len = u32::try_from(job.index_cursor.len())
        .map_err(|_| MetadError::Codec("restore cleanup cursor is too long".to_owned()))?;
    let mut value = Vec::with_capacity(54 + job.index_cursor.len());
    value.extend_from_slice(CLEANUP_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&job.operation_digest);
    value.extend_from_slice(&job.ref_set_id.to_be_bytes());
    value.push(u8::from(job.index_complete));
    value.extend_from_slice(&cursor_len.to_be_bytes());
    value.extend_from_slice(&job.index_cursor);
    Ok(value)
}

pub(super) fn decode_restore_cleanup_job(value: &[u8]) -> Result<RestoreCleanupJob, MetadError> {
    let mut decoder = RestoreDecoder::new(value);
    if decoder.take(8)? != CLEANUP_MAGIC || decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "invalid restore cleanup job header".to_owned(),
        ));
    }
    let operation_digest = decoder.array_32()?;
    let ref_set_id = decoder.u64()?;
    let index_complete = match decoder.u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(MetadError::Codec(
                "restore cleanup job has an invalid completion flag".to_owned(),
            ))
        }
    };
    let index_cursor = decoder.bytes()?;
    decoder.finish()?;
    let job = RestoreCleanupJob {
        operation_digest,
        ref_set_id,
        index_complete,
        index_cursor,
    };
    encode_restore_cleanup_job(&job)?;
    Ok(job)
}

fn encode_restore_init_upload_intent(
    intent: &RestoreInitUploadIntent,
) -> Result<Vec<u8>, MetadError> {
    if intent.ref_set_id == 0 || intent.generation == 0 || intent.cleanup_pass > 1 {
        return Err(MetadError::Codec(
            "restore init upload intent contains a zero identity".to_owned(),
        ));
    }
    let relative_path = canonical_restore_relative_path(&intent.relative_path)?;
    if relative_path != intent.relative_path {
        return Err(MetadError::Codec(
            "restore init upload intent path is not canonical".to_owned(),
        ));
    }
    let path = relative_path.as_bytes();
    let path_len = u32::try_from(path.len())
        .map_err(|_| MetadError::Codec("restore init upload path is too long".to_owned()))?;
    let mut value = Vec::with_capacity(77 + path.len());
    value.extend_from_slice(INIT_UPLOAD_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&intent.operation_digest);
    value.extend_from_slice(&intent.ref_set_id.to_be_bytes());
    value.extend_from_slice(&intent.inode.get().to_be_bytes());
    value.extend_from_slice(&intent.generation.to_be_bytes());
    value.extend_from_slice(&intent.size.to_be_bytes());
    value.push(intent.cleanup_pass);
    value.extend_from_slice(&path_len.to_be_bytes());
    value.extend_from_slice(path);
    Ok(value)
}

pub(super) fn decode_restore_init_upload_intent(
    value: &[u8],
) -> Result<RestoreInitUploadIntent, MetadError> {
    let mut decoder = RestoreDecoder::new(value);
    if decoder.take(8)? != INIT_UPLOAD_MAGIC || decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "invalid restore init upload intent header".to_owned(),
        ));
    }
    let intent = RestoreInitUploadIntent {
        operation_digest: decoder.array_32()?,
        ref_set_id: decoder.u64()?,
        inode: InodeId::new(decoder.u64()?)?,
        generation: decoder.u64()?,
        size: decoder.u64()?,
        cleanup_pass: decoder.u8()?,
        relative_path: decoder.string()?,
    };
    decoder.finish()?;
    encode_restore_init_upload_intent(&intent)?;
    Ok(intent)
}

pub(super) fn encode_restore_init_upload_tombstone(
    tombstone: &RestoreInitUploadTombstone,
) -> Result<Vec<u8>, MetadError> {
    if tombstone.generation == 0 {
        return Err(MetadError::Codec(
            "restore init upload tombstone has a zero generation".to_owned(),
        ));
    }
    let relative_path = canonical_restore_relative_path(&tombstone.relative_path)?;
    if relative_path != tombstone.relative_path {
        return Err(MetadError::Codec(
            "restore init upload tombstone path is not canonical".to_owned(),
        ));
    }
    let path = relative_path.as_bytes();
    let path_len = u32::try_from(path.len())
        .map_err(|_| MetadError::Codec("restore tombstone path is too long".to_owned()))?;
    let mut value = Vec::with_capacity(101 + path.len());
    value.extend_from_slice(INIT_UPLOAD_TOMBSTONE_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&tombstone.operation_digest);
    value.extend_from_slice(&tombstone.initialization_digest);
    value.extend_from_slice(&tombstone.inode.get().to_be_bytes());
    value.extend_from_slice(&tombstone.generation.to_be_bytes());
    value.extend_from_slice(&tombstone.size.to_be_bytes());
    value.extend_from_slice(&path_len.to_be_bytes());
    value.extend_from_slice(path);
    Ok(value)
}

pub(super) fn decode_restore_init_upload_tombstone(
    value: &[u8],
) -> Result<RestoreInitUploadTombstone, MetadError> {
    let mut decoder = RestoreDecoder::new(value);
    if decoder.take(8)? != INIT_UPLOAD_TOMBSTONE_MAGIC || decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "invalid restore init upload tombstone header".to_owned(),
        ));
    }
    let tombstone = RestoreInitUploadTombstone {
        operation_digest: decoder.array_32()?,
        initialization_digest: decoder.array_32()?,
        inode: InodeId::new(decoder.u64()?)?,
        generation: decoder.u64()?,
        size: decoder.u64()?,
        relative_path: decoder.string()?,
    };
    decoder.finish()?;
    encode_restore_init_upload_tombstone(&tombstone)?;
    Ok(tombstone)
}

pub(super) fn validate_restore_init_upload_tombstone_row(
    mount: MountId,
    row: &crate::command::ScanItem,
) -> Result<RestoreInitUploadTombstone, MetadError> {
    let tombstone = decode_restore_init_upload_tombstone(&row.value.0)?;
    if row.key
        != restore_init_upload_tombstone_key(
            mount,
            &tombstone.operation_digest,
            tombstone.inode,
            tombstone.generation,
        )
    {
        return Err(MetadError::Codec(
            "restore init upload tombstone key changed identity".to_owned(),
        ));
    }
    Ok(tombstone)
}

pub(super) fn encode_restore_operation(
    operation: &RestoreOperation,
) -> Result<Vec<u8>, MetadError> {
    validate_restore_operation(operation)?;
    let source = operation.source_path.as_bytes();
    let destination = operation.destination_path.as_bytes();
    let source_len = u32::try_from(source.len())
        .map_err(|_| MetadError::Codec("restore source path is too long".to_owned()))?;
    let destination_len = u32::try_from(destination.len())
        .map_err(|_| MetadError::Codec("restore destination path is too long".to_owned()))?;
    let mut out = Vec::with_capacity(166 + source.len() + destination.len());
    out.extend_from_slice(OPERATION_MAGIC);
    out.push(RESTORE_FORMAT_VERSION);
    out.push(operation_state_tag(operation.state));
    out.extend_from_slice(&operation.operation_digest);
    out.extend_from_slice(&operation.initialization_digest);
    for value in [
        operation.source_root.get(),
        operation.destination_root.get(),
        operation.snapshot_id,
        operation.read_version,
        operation.created_version,
        operation.ref_set_id,
    ] {
        out.extend_from_slice(&value.to_be_bytes());
    }
    out.extend_from_slice(&source_len.to_be_bytes());
    out.extend_from_slice(source);
    out.extend_from_slice(&destination_len.to_be_bytes());
    out.extend_from_slice(destination);
    Ok(out)
}

pub(super) fn decode_restore_operation(bytes: &[u8]) -> Result<RestoreOperation, MetadError> {
    let mut decoder = RestoreDecoder::new(bytes);
    if decoder.take(8)? != OPERATION_MAGIC {
        return Err(MetadError::Codec(
            "invalid restore operation magic".to_owned(),
        ));
    }
    if decoder.u8()? != RESTORE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "unsupported restore operation format version".to_owned(),
        ));
    }
    let state = operation_state(decoder.u8()?)?;
    let operation_digest = decoder.array_32()?;
    let initialization_digest = decoder.array_32()?;
    let source_root = InodeId::new(decoder.u64()?)?;
    let destination_root = InodeId::new(decoder.u64()?)?;
    let snapshot_id = decoder.u64()?;
    let read_version = decoder.u64()?;
    let created_version = decoder.u64()?;
    let ref_set_id = decoder.u64()?;
    let source_path = decoder.string()?;
    let destination_path = decoder.string()?;
    decoder.finish()?;
    let operation = RestoreOperation {
        operation_digest,
        initialization_digest,
        state,
        source_root,
        destination_root,
        snapshot_id,
        read_version,
        created_version,
        ref_set_id,
        source_path,
        destination_path,
    };
    validate_restore_operation(&operation)?;
    Ok(operation)
}

fn validate_restore_operation(operation: &RestoreOperation) -> Result<(), MetadError> {
    if operation.snapshot_id == 0
        || operation.read_version == 0
        || operation.created_version == 0
        || operation.ref_set_id == 0
    {
        return Err(MetadError::Codec(
            "restore operation contains a zero durable identity".to_owned(),
        ));
    }
    let source = canonical_path(&parse_absolute_path(&operation.source_path)?)?;
    let destination = canonical_path(&parse_absolute_path(&operation.destination_path)?)?;
    if source != operation.source_path || destination != operation.destination_path {
        return Err(MetadError::Codec(
            "restore operation path is not canonical".to_owned(),
        ));
    }
    if source == destination {
        return Err(MetadError::Codec(
            "restore source and destination are identical".to_owned(),
        ));
    }
    Ok(())
}

/// Bind a decoded operation to both its physical durable key and the canonical
/// public request identity. The codec cannot perform this check because the
/// mount is deliberately not duplicated inside the operation value.
pub(super) fn validate_restore_operation_identity(
    mount: MountId,
    expected_digest: &[u8; 32],
    operation: &RestoreOperation,
) -> Result<(), MetadError> {
    if operation.operation_digest != *expected_digest {
        return Err(MetadError::Codec(
            "restore operation value does not match its durable key".to_owned(),
        ));
    }
    let canonical_digest = restore_operation_digest(
        mount,
        &operation.source_path,
        operation.snapshot_id,
        &operation.destination_path,
    );
    if canonical_digest != *expected_digest {
        return Err(MetadError::Codec(
            "restore operation digest does not match its request identity".to_owned(),
        ));
    }
    Ok(())
}

fn operation_state_tag(state: RestoreOperationState) -> u8 {
    match state {
        RestoreOperationState::Preparing => 1,
        RestoreOperationState::ReadyToAttach => 2,
        RestoreOperationState::Complete => 3,
        RestoreOperationState::Cleaning => 4,
        RestoreOperationState::Discarding => 5,
        RestoreOperationState::Releasing => 6,
    }
}

fn operation_state(tag: u8) -> Result<RestoreOperationState, MetadError> {
    match tag {
        1 => Ok(RestoreOperationState::Preparing),
        2 => Ok(RestoreOperationState::ReadyToAttach),
        3 => Ok(RestoreOperationState::Complete),
        4 => Ok(RestoreOperationState::Cleaning),
        5 => Ok(RestoreOperationState::Discarding),
        6 => Ok(RestoreOperationState::Releasing),
        _ => Err(MetadError::Codec(format!(
            "invalid restore operation state {tag}"
        ))),
    }
}

fn system_key(mount: MountId, label: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + label.len());
    key.extend_from_slice(&mount.get().to_be_bytes());
    key.extend_from_slice(label);
    key
}

fn system_key_with_digest(mount: MountId, label: &[u8], digest: &[u8; 32]) -> Vec<u8> {
    let mut key = system_key(mount, label);
    key.extend_from_slice(digest);
    key
}

fn system_key_with_u64(mount: MountId, label: &[u8], value: u64) -> Vec<u8> {
    let mut key = system_key(mount, label);
    key.extend_from_slice(&value.to_be_bytes());
    key
}

/// Authoritative registry of restore-private control keyspaces, excluding the
/// active marker itself. Diagnostics and the explicit downgrade transaction use
/// this list so a newly-added durable row cannot be omitted from the drain
/// proof. Index-overlay keyspaces are registered by `restore_index` because
/// that module owns their physical MVCC layout.
pub(super) fn restore_control_keyspaces(mount: MountId) -> Vec<(&'static str, Vec<u8>)> {
    [
        ("operation", OPERATION_KEY_LABEL),
        ("destination_claim", CLAIM_KEY_LABEL),
        ("staging_member", STAGING_KEY_LABEL),
        ("staging_inode_inverse", STAGING_INVERSE_KEY_LABEL),
        ("staging_inverse_owner", STAGING_INVERSE_OWNER_KEY_LABEL),
        ("root_index", ROOT_KEY_LABEL),
        ("base_owner", BASE_OWNER_KEY_LABEL),
        ("base_inverse", BASE_INVERSE_KEY_LABEL),
        ("base_inverse_owner", BASE_INVERSE_OWNER_KEY_LABEL),
        ("base_seal", BASE_SEAL_KEY_LABEL),
        ("cleanup_job", CLEANUP_KEY_LABEL),
        ("init_upload_intent", INIT_UPLOAD_KEY_LABEL),
        ("init_upload_tombstone", INIT_UPLOAD_TOMBSTONE_KEY_LABEL),
        (
            "init_upload_tombstone_cursor",
            INIT_UPLOAD_TOMBSTONE_CURSOR_KEY_LABEL,
        ),
        ("release_job", RELEASE_KEY_LABEL),
        ("release_cursor", RELEASE_CURSOR_KEY_LABEL),
        ("release_quarantine", RELEASE_QUARANTINE_KEY_LABEL),
    ]
    .into_iter()
    .map(|(name, label)| (name, system_key(mount, label)))
    .collect()
}

/// Return whether a command can change the authoritative restore graph.
///
/// The two mount-global worker cursors are scheduling hints rather than graph
/// ownership. Excluding their exact keys lets long fsck/object HEAD scans make
/// progress while permanent upload tombstones rotate round-robin. Corrupt
/// suffixed rows still match their registered keyspace and therefore fence the
/// scan like any other restore mutation.
pub(super) fn command_mutates_restore_graph(mount: MountId, command: &MetadataCommand) -> bool {
    if !command
        .mutations
        .iter()
        .any(|mutation| mutation.family == RecordFamily::System)
    {
        return false;
    }
    let init_cursor = restore_init_upload_tombstone_cursor_key(mount);
    let release_cursor = restore_release_cursor_key(mount);
    let active = restore_active_key(mount);
    let activation_fence = restore_activation_fence_key(mount);
    let control = restore_control_keyspaces(mount);
    let index = super::restore_index::restore_index_private_keyspaces(mount);

    command.mutations.iter().any(|mutation| {
        if mutation.family != RecordFamily::System {
            return false;
        }
        if mutation.key == init_cursor || mutation.key == release_cursor {
            return false;
        }
        mutation.key == active
            || mutation.key == activation_fence
            || control
                .iter()
                .any(|(_, prefix)| mutation.key.starts_with(prefix))
            || index
                .iter()
                .any(|(_, prefix)| mutation.key.starts_with(prefix))
    })
}

fn put_hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn preview_restore_initialization_artifact(
    mount: MountId,
    file: &RestoreInitializationFile,
) -> Result<(InodeId, Version, BodyDescriptor, Vec<ChunkManifest>), MetadError> {
    // Use the widest decimal inode/generation in object keys. Metadata ids are
    // binary fixed-width, while object keys are embedded strings in chunk
    // manifests; using u64::MAX therefore makes this preview conservative for
    // every allocator state without writing an object.
    let inode = InodeId::new(u64::MAX)?;
    let generation = Version::new(u64::MAX)?;
    let size = file.bytes.len() as u64;
    let mut chunks = Vec::new();
    let mut chunk_offset = 0_u64;
    while chunk_offset < size {
        let chunk_index = chunk_offset / DEFAULT_CHUNK_SIZE;
        let chunk_len = DEFAULT_CHUNK_SIZE.min(size - chunk_offset);
        let mut blocks = Vec::new();
        let mut block_offset = 0_u64;
        while block_offset < chunk_len {
            let block_index = block_offset / DEFAULT_BLOCK_SIZE as u64;
            let block_len = (DEFAULT_BLOCK_SIZE as u64).min(chunk_len - block_offset);
            blocks.push(BlockDescriptor {
                object_key: format!(
                    "blocks/{}/{}/{}/{}/{}",
                    mount.get(),
                    inode.get(),
                    generation.get(),
                    chunk_index,
                    block_index
                ),
                logical_offset: chunk_offset + block_offset,
                object_offset: 0,
                len: block_len,
                digest_uri: format!("sha256:{}", "0".repeat(64)),
            });
            block_offset = block_offset.saturating_add(block_len);
        }
        chunks.push(ChunkManifest {
            chunk_index,
            logical_offset: chunk_offset,
            len: chunk_len,
            slices: vec![SliceManifest {
                slice_id: 1,
                logical_offset: chunk_offset,
                len: chunk_len,
                blocks,
            }],
        });
        chunk_offset = chunk_offset.saturating_add(chunk_len);
    }
    Ok((
        inode,
        generation,
        BodyDescriptor {
            producer: "nokv-restore-initialization".to_owned(),
            digest_uri: body_digest_uri(&file.bytes),
            size,
            content_type: file.content_type.clone(),
            manifest_id: format!("restore-init/{}", file.relative_path),
            generation: generation.get(),
            base_generation: 0,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
        },
        chunks,
    ))
}

fn hex_digest(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for byte in bytes {
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

pub(super) fn restore_barrier_operation_id(operation: &RestoreOperation) -> String {
    format!("restore-{}", hex_digest(&operation.operation_digest))
}

struct RestoreDecoder<'a> {
    input: &'a [u8],
}

impl<'a> RestoreDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadError> {
        let Some((head, tail)) = self.input.split_at_checked(len) else {
            return Err(MetadError::Codec(
                "restore operation record is truncated".to_owned(),
            ));
        };
        self.input = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, MetadError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, MetadError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, MetadError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("u64 width"),
        ))
    }

    fn array_32(&mut self) -> Result<[u8; 32], MetadError> {
        Ok(self.take(32)?.try_into().expect("digest width"))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, MetadError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String, MetadError> {
        String::from_utf8(self.bytes()?)
            .map_err(|_| MetadError::Codec("restore operation path is not utf-8".to_owned()))
    }

    fn finish(self) -> Result<(), MetadError> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(MetadError::Codec(
                "restore operation record has trailing bytes".to_owned(),
            ))
        }
    }
}

fn canonical_restore_initialization(
    mut initialization: RestoreInitialization,
) -> Result<(RestoreInitialization, [u8; 32]), MetadError> {
    let count = initialization
        .remove_relative_paths
        .len()
        .saturating_add(initialization.files.len());
    if count > MAX_RESTORE_INITIALIZATION_ENTRIES {
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore initialization entries".to_owned(),
            limit: MAX_RESTORE_INITIALIZATION_ENTRIES as u64,
            actual: count as u64,
        });
    }
    for path in &mut initialization.remove_relative_paths {
        *path = canonical_restore_relative_path(path)?;
    }
    for file in &mut initialization.files {
        file.relative_path = canonical_restore_relative_path(&file.relative_path)?;
    }
    initialization.remove_relative_paths.sort();
    initialization
        .files
        .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));

    // Bound the complete canonical initialization payload, not only file
    // contents. This mirrors the digest framing below: every path/string/blob
    // contributes its length field and fixed mode/uid/gid metadata.
    let bytes = initialization
        .remove_relative_paths
        .iter()
        .fold(0_usize, |total, path| {
            total
                .saturating_add(1)
                .saturating_add(std::mem::size_of::<u64>())
                .saturating_add(path.len())
        });
    let bytes = initialization.files.iter().fold(bytes, |total, file| {
        total
            .saturating_add(1)
            .saturating_add(std::mem::size_of::<u64>())
            .saturating_add(file.relative_path.len())
            .saturating_add(std::mem::size_of::<u64>())
            .saturating_add(file.content_type.len())
            .saturating_add(3 * std::mem::size_of::<u32>())
            .saturating_add(std::mem::size_of::<u64>())
            .saturating_add(file.bytes.len())
    });
    if bytes > MAX_RESTORE_INITIALIZATION_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore initialization bytes".to_owned(),
            limit: MAX_RESTORE_INITIALIZATION_BYTES as u64,
            actual: bytes as u64,
        });
    }
    let mut paths = initialization.remove_relative_paths.clone();
    paths.extend(
        initialization
            .files
            .iter()
            .map(|file| file.relative_path.clone()),
    );
    paths.sort();
    for pair in paths.windows(2) {
        if pair[0] == pair[1] {
            return Err(MetadError::InvalidPath(
                "restore initialization contains duplicate paths".to_owned(),
            ));
        }
        if pair[1]
            .strip_prefix(&pair[0])
            .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return Err(MetadError::InvalidPath(
                "restore initialization paths overlap as ancestor and descendant".to_owned(),
            ));
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(b"nokv-restore-initialization-v1\0");
    for path in &initialization.remove_relative_paths {
        hasher.update([1]);
        put_hash_field(&mut hasher, path.as_bytes());
    }
    for file in &initialization.files {
        hasher.update([2]);
        put_hash_field(&mut hasher, file.relative_path.as_bytes());
        put_hash_field(&mut hasher, file.content_type.as_bytes());
        hasher.update(file.mode.to_be_bytes());
        hasher.update(file.uid.to_be_bytes());
        hasher.update(file.gid.to_be_bytes());
        put_hash_field(&mut hasher, &file.bytes);
    }
    Ok((initialization, hasher.finalize().into()))
}

fn canonical_restore_relative_path(path: &str) -> Result<String, MetadError> {
    if path.is_empty() || path.starts_with('/') {
        return Err(MetadError::InvalidPath(
            "restore initialization path must be non-empty and relative".to_owned(),
        ));
    }
    let absolute = format!("/{path}");
    let components = parse_absolute_path(&absolute)?;
    if components.is_empty() {
        return Err(MetadError::InvalidPath(
            "restore initialization cannot address its root".to_owned(),
        ));
    }
    let canonical = canonical_path(&components)?;
    let relative = canonical
        .strip_prefix('/')
        .expect("canonical non-root path starts with slash")
        .to_owned();
    if relative.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore initialization relative path bytes".to_owned(),
            limit: MAX_RESTORE_PATH_BYTES as u64,
            actual: relative.len() as u64,
        });
    }
    Ok(relative)
}

fn restore_relative_components(path: &str) -> Result<Vec<DentryName>, MetadError> {
    let canonical = canonical_restore_relative_path(path)?;
    Ok(parse_absolute_path(&format!("/{canonical}"))?)
}

fn canonical_restore_relative_components(components: &[DentryName]) -> Result<String, MetadError> {
    if components.is_empty() {
        return Ok(String::new());
    }
    let absolute = canonical_path(components)?;
    let relative = absolute
        .strip_prefix('/')
        .expect("canonical non-root path starts with slash")
        .to_owned();
    if relative.len() > MAX_RESTORE_PATH_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore relative path bytes".to_owned(),
            limit: MAX_RESTORE_PATH_BYTES as u64,
            actual: relative.len() as u64,
        });
    }
    Ok(relative)
}

// The state machine implementation is added below this durable contract. Keep
// the entry points present while protocol/server work compiles against it.
impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub(super) fn ensure_restore_snapshot_retirable(
        &self,
        snapshot_id: u64,
        bindings: &[super::snapshot::VersionedForkBinding],
        version: Version,
    ) -> Result<(), MetadError> {
        for binding in bindings {
            let root = binding.binding.fork_root;
            let Some(root_index) = self.metadata.get(
                RecordFamily::System,
                &restore_root_index_key(self.mount, root),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            else {
                continue;
            };
            let digest: [u8; 32] = root_index
                .0
                .as_slice()
                .try_into()
                .map_err(|_| MetadError::RestoreRootChanged { root })?;
            let operation = self
                .metadata
                .get(
                    RecordFamily::System,
                    &restore_operation_key(self.mount, &digest),
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::RestoreRootChanged { root })?;
            let operation = decode_restore_operation(&operation.0)?;
            if operation.operation_digest != digest
                || operation.destination_root != root
                || operation.snapshot_id != snapshot_id
                || binding.binding.snapshot_id != snapshot_id
                || binding.binding.source_root != operation.source_root
                || binding.binding.pinned_read_version != operation.read_version
            {
                return Err(MetadError::RestoreBindingChanged { root });
            }
            // A restore's temporary ForkBinding is the only durable history
            // floor while staging. It may not be retired through the generic
            // clone path, even if no borrowed manifest has been written yet.
            return Err(MetadError::RestoreInProgress);
        }
        Ok(())
    }

    pub(super) fn is_complete_restore_root(&self, root: InodeId) -> Result<bool, MetadError> {
        let Some((operation, _, _)) = self.restore_root_operation_at(root, self.read_version()?)?
        else {
            return Ok(false);
        };
        match operation.state {
            RestoreOperationState::Complete => Ok(true),
            RestoreOperationState::Releasing => Err(MetadError::RestoreInProgress),
            _ => Err(MetadError::RestoreRootChanged { root }),
        }
    }

    /// Install the stable pre-activation fence once metadata is safe to mutate.
    ///
    /// `NoKvFs::new` may wrap a pristine store which is about to receive a
    /// checkpoint image, so construction cannot create this row. Bootstrap,
    /// reopen, and recovery call this method before admitting namespace work.
    /// The first restore rewrites the row in the same command as the active
    /// marker; ordinary writes therefore conflict only with restore activation,
    /// never with unrelated allocator reservations.
    pub(super) fn ensure_restore_activation_fence(&self) -> Result<(), MetadError> {
        const MAX_ATTEMPTS: usize = 8;

        let key = restore_activation_fence_key(self.mount);
        if let Some(item) = self.metadata.get(
            RecordFamily::System,
            &key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )? {
            if item.0 != [RESTORE_FORMAT_VERSION] {
                return Err(MetadError::Codec(
                    "restore activation fence has an invalid value".to_owned(),
                ));
            }
            return Ok(());
        }
        for attempt in 0..MAX_ATTEMPTS {
            let version = self.next_version()?;
            let read_version = predecessor(version)?;
            if let Some(item) = self.metadata.get_versioned(
                RecordFamily::System,
                &key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )? {
                if item.value.0 != [RESTORE_FORMAT_VERSION] {
                    return Err(MetadError::Codec(
                        "restore activation fence has an invalid value".to_owned(),
                    ));
                }
                return Ok(());
            }
            let active_key = restore_active_key(self.mount);
            let active = self.metadata.get_versioned(
                RecordFamily::System,
                &active_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?;
            if active
                .as_ref()
                .is_some_and(|item| item.value.0 != [RESTORE_FORMAT_VERSION])
            {
                return Err(MetadError::Codec(
                    "restore active marker has an invalid value".to_owned(),
                ));
            }
            let command = MetadataCommand {
                request_id: request_id(
                    b"restore-activation-fence",
                    self.mount,
                    InodeId::root(),
                    version,
                ),
                kind: CommandKind::ReserveAllocator,
                read_version,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: key.clone(),
                predicates: vec![
                    PredicateRef {
                        family: RecordFamily::System,
                        key: key.clone(),
                        predicate: Predicate::NotExists,
                    },
                    PredicateRef {
                        family: RecordFamily::System,
                        key: active_key,
                        predicate: active
                            .map(|item| Predicate::VersionEquals(item.version))
                            .unwrap_or(Predicate::NotExists),
                    },
                ],
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(vec![RESTORE_FORMAT_VERSION])),
                }],
                watch: Vec::new(),
            };
            match self.commit_metadata(command) {
                Ok(_) => return Ok(()),
                Err(MetadError::Metadata(MetadataError::PredicateFailed))
                    if attempt + 1 < MAX_ATTEMPTS =>
                {
                    continue;
                }
                Err(
                    error @ MetadError::SyncLogArchiveFailed {
                        committed: true, ..
                    },
                ) => {
                    let installed = self.metadata.get(
                        RecordFamily::System,
                        &key,
                        self.read_version()?,
                        ReadPurpose::WritePlanLocal,
                    )?;
                    if installed
                        .as_ref()
                        .is_some_and(|value| value.0 == [RESTORE_FORMAT_VERSION])
                    {
                        return Ok(());
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    }

    /// Build the durable fence required by an ordinary inode-addressed write.
    ///
    /// Restore materialization deliberately creates real inode/dentry records
    /// before the destination is attached. An inode id is not an authority to
    /// mutate those records: a caller can guess one, and an attach can race a
    /// read-only visibility check. Every ordinary namespace mutation therefore
    /// joins the owning operation, member, and inverse rows in the same
    /// `MetadataCommand` CAS.
    ///
    /// Complete operations behave like an ordinary namespace. During release,
    /// only an inode which is still reachable through an escaped link may be
    /// mutated; this lets the last escaped holder be removed without reopening
    /// access to the detached tree. All materialization/discard states are
    /// fail-closed.
    pub(super) fn restore_namespace_write_predicates(
        &self,
        inodes: &[InodeId],
        read_version: Version,
    ) -> Result<Vec<PredicateRef>, MetadError> {
        self.restore_namespace_write_predicates_with_policy(inodes, read_version, true, true)
    }

    /// A restore destination parent is already proven reachable by its full
    /// path-dentry CAS. This variant is also used while the first-hold
    /// visibility write fence is held, so it must not recursively enter the
    /// visibility read path. A Releasing parent is never a valid destination.
    fn restore_destination_parent_predicates(
        &self,
        parent: InodeId,
        read_version: Version,
    ) -> Result<Vec<PredicateRef>, MetadError> {
        self.restore_namespace_write_predicates_with_policy(&[parent], read_version, false, false)
    }

    fn restore_namespace_write_predicates_with_policy(
        &self,
        inodes: &[InodeId],
        read_version: Version,
        require_complete_reachability: bool,
        allow_releasing_reachable: bool,
    ) -> Result<Vec<PredicateRef>, MetadError> {
        let (active_key, active_version) = match self.restore_namespace_activity_fence()? {
            RestoreNamespaceActivityFence::Inactive(fence) => return Ok(vec![fence]),
            RestoreNamespaceActivityFence::Active { key, version } => (key, version),
        };
        let mut predicates = Vec::new();
        let mut guarded_keys = HashSet::<Vec<u8>>::new();
        let mut operation_states = HashMap::<[u8; 32], (RestoreOperationState, Version)>::new();
        let unique_inodes = inodes.iter().copied().collect::<HashSet<_>>();
        let mut reachable_inodes = None::<HashSet<InodeId>>;
        let active_marker = (active_key, active_version);

        for inode in unique_inodes.iter().copied() {
            let inverse_key = restore_staging_inode_key(self.mount, inode);
            let Some(inverse) = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            else {
                // Absence is also part of the write plan. Without this CAS an
                // ordinary inode could be enrolled into a completed restore
                // after planning, then become detached by release while the
                // stale ordinary command still commits against it.
                if guarded_keys.insert(inverse_key.clone()) {
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: inverse_key,
                        predicate: Predicate::NotExists,
                    });
                }
                continue;
            };
            let (operation_digest, ref_set_id) = decode_restore_staging_inverse(&inverse.value.0)?;
            let operation_key = restore_operation_key(self.mount, &operation_digest);
            let (state, operation_version) = match operation_states.get(&operation_digest) {
                Some(state) => *state,
                None => {
                    let operation_item = self
                        .metadata
                        .get_versioned(
                            RecordFamily::System,
                            &operation_key,
                            read_version,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "restore staging inverse has no owning operation".to_owned(),
                            )
                        })?;
                    let operation = decode_restore_operation(&operation_item.value.0)?;
                    if operation.operation_digest != operation_digest
                        || operation.ref_set_id != ref_set_id
                    {
                        return Err(MetadError::Codec(
                            "restore staging inverse changed operation identity".to_owned(),
                        ));
                    }
                    let state = (operation.state, operation_item.version);
                    operation_states.insert(operation_digest, state);
                    state
                }
            };
            match state {
                RestoreOperationState::Complete => {
                    if require_complete_reachability
                        && self.restore_staging_possible.load(Ordering::Acquire)
                    {
                        if reachable_inodes.is_none() {
                            reachable_inodes = Some(
                                self.restore_reachable_inodes_at(&unique_inodes, read_version)?,
                            );
                        }
                        if !reachable_inodes
                            .as_ref()
                            .is_some_and(|reachable| reachable.contains(&inode))
                        {
                            return Err(MetadError::RestoreInProgress);
                        }
                    }
                }
                RestoreOperationState::Releasing => {
                    if !allow_releasing_reachable {
                        return Err(MetadError::RestoreInProgress);
                    }
                    if reachable_inodes.is_none() {
                        reachable_inodes =
                            Some(self.restore_reachable_inodes_at(&unique_inodes, read_version)?);
                    }
                    if !reachable_inodes
                        .as_ref()
                        .is_some_and(|reachable| reachable.contains(&inode))
                    {
                        return Err(MetadError::RestoreInProgress);
                    }
                }
                RestoreOperationState::Preparing
                | RestoreOperationState::ReadyToAttach
                | RestoreOperationState::Cleaning
                | RestoreOperationState::Discarding => {
                    return Err(MetadError::RestoreInProgress);
                }
            }

            let member_key = restore_staging_member_key(self.mount, ref_set_id, inode);
            let member = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &member_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or_else(|| {
                    MetadError::Codec("restore staging inverse has no owning member".to_owned())
                })?;
            let decoded_member = decode_restore_staging_member(&member.value.0)?;
            if decoded_member.operation_digest != operation_digest
                || decoded_member.destination_inode != inode
            {
                return Err(MetadError::Codec(
                    "restore staging member changed operation identity".to_owned(),
                ));
            }
            let inverse_owner_key =
                restore_staging_inverse_owner_key(self.mount, ref_set_id, inode);
            let inverse_owner = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &inverse_owner_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or_else(|| {
                    MetadError::Codec("restore staging inverse has no ref-set owner".to_owned())
                })?;
            if inverse_owner.value != inverse.value {
                return Err(MetadError::Codec(
                    "restore staging inverse owner changed identity".to_owned(),
                ));
            }

            let (active_key, active_version) = active_marker.clone();

            for (key, version) in [
                (active_key, active_version),
                (operation_key.clone(), operation_version),
                (inverse_key, inverse.version),
                (member_key, member.version),
                (inverse_owner_key, inverse_owner.version),
            ] {
                if guarded_keys.insert(key.clone()) {
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key,
                        predicate: Predicate::VersionEquals(version),
                    });
                }
            }
        }
        Ok(predicates)
    }

    /// Fence the first restore hold without probing an inode-private key when
    /// restore-to-fork has never been activated on this mount.
    ///
    /// Predicate-only `NotExists` guards are implemented by Holt as an atomic
    /// empty sentinel create/delete pair. Making every ordinary write use such
    /// a guard on a missing staging-inverse key lets concurrent commands race
    /// on the same physical key. A dedicated, stable activation row avoids
    /// that sentinel and is rewritten only by restore activation; allocator
    /// reservations must not invalidate unrelated ordinary writes.
    fn restore_namespace_activity_fence(
        &self,
    ) -> Result<RestoreNamespaceActivityFence, MetadError> {
        // The first restore hold can land while an ordinary write is staged.
        // Read the latest control state; the returned exact-version predicate
        // then invalidates only a command which crossed that activation.
        let control_version = self.read_version()?;
        let active_key = restore_active_key(self.mount);
        if let Some(active) = self.metadata.get_versioned(
            RecordFamily::System,
            &active_key,
            control_version,
            ReadPurpose::WritePlanLocal,
        )? {
            if active.value.0 != [RESTORE_FORMAT_VERSION] {
                return Err(MetadError::Codec(
                    "restore active marker has an invalid value".to_owned(),
                ));
            }
            return Ok(RestoreNamespaceActivityFence::Active {
                key: active_key,
                version: active.version,
            });
        }

        let key = restore_activation_fence_key(self.mount);
        let activation = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                control_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore activation fence is missing".to_owned()))?;
        if activation.value.0 != [RESTORE_FORMAT_VERSION] {
            return Err(MetadError::Codec(
                "restore activation fence has an invalid value".to_owned(),
            ));
        }
        Ok(RestoreNamespaceActivityFence::Inactive(PredicateRef {
            family: RecordFamily::System,
            key,
            predicate: Predicate::VersionEquals(activation.version),
        }))
    }

    /// Enrol newly named local inodes in the ref-set of a completed restore.
    /// The rows are written with the namespace mutation which first exposes the
    /// inode below that restore, so release never has a visibility/membership
    /// gap. Directory renames only enrol their root here; the bounded release
    /// walker discovers descendants before it is allowed to remove the parent.
    pub(super) fn restore_namespace_enrollment_plan(
        &self,
        parent: InodeId,
        projections: &[DentryProjection],
        read_version: Version,
    ) -> Result<RestoreNamespaceEnrollmentPlan, MetadError> {
        if matches!(
            self.restore_namespace_activity_fence()?,
            RestoreNamespaceActivityFence::Inactive(_)
        ) {
            // Every enrollment caller first joins
            // `restore_namespace_write_predicates`, whose allocator-version
            // fence invalidates the command if the first restore hold races
            // this fast path.
            return Ok(RestoreNamespaceEnrollmentPlan {
                predicates: Vec::new(),
                mutations: Vec::new(),
            });
        }
        let inverse_key = restore_staging_inode_key(self.mount, parent);
        let Some(inverse) = self.metadata.get_versioned(
            RecordFamily::System,
            &inverse_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(RestoreNamespaceEnrollmentPlan {
                predicates: Vec::new(),
                mutations: Vec::new(),
            });
        };
        let (operation_digest, ref_set_id) = decode_restore_staging_inverse(&inverse.value.0)?;
        let operation_key = restore_operation_key(self.mount, &operation_digest);
        let operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore namespace parent has no operation".to_owned())
            })?;
        let operation = decode_restore_operation(&operation_item.value.0)?;
        if operation.operation_digest != operation_digest || operation.ref_set_id != ref_set_id {
            return Err(MetadError::Codec(
                "restore namespace parent changed operation identity".to_owned(),
            ));
        }
        if operation.state != RestoreOperationState::Complete {
            return Err(MetadError::RestoreInProgress);
        }
        self.restore_namespace_enrollment_plan_for_operation(
            &operation,
            operation_item.version,
            projections,
            read_version,
        )
    }

    fn restore_namespace_enrollment_plan_for_operation(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        projections: &[DentryProjection],
        read_version: Version,
    ) -> Result<RestoreNamespaceEnrollmentPlan, MetadError> {
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: restore_operation_key(self.mount, &operation.operation_digest),
            predicate: Predicate::VersionEquals(operation_version),
        }];
        let mut mutations = Vec::new();
        let inverse_value = Value(encode_restore_staging_inverse(operation));
        let mut seen = HashSet::new();
        for projection in projections {
            let inode = projection.attr.inode;
            if !seen.insert(inode) {
                continue;
            }
            if inode.shard_index() != self.shard_index {
                return Err(MetadError::RestoreCrossShardUnsupported { inode });
            }
            let member_key = restore_staging_member_key(self.mount, operation.ref_set_id, inode);
            let inverse_key = restore_staging_inode_key(self.mount, inode);
            let inverse_owner_key =
                restore_staging_inverse_owner_key(self.mount, operation.ref_set_id, inode);
            if let Some(existing) = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )? {
                let (digest, ref_set_id) = decode_restore_staging_inverse(&existing.value.0)?;
                if digest != operation.operation_digest || ref_set_id != operation.ref_set_id {
                    return Err(MetadError::InvalidPath(
                        "inode cannot be enrolled by two restore ref-sets".to_owned(),
                    ));
                }
                let member = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &member_key,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore namespace inverse has no member".to_owned())
                    })?;
                let inverse_owner = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &inverse_owner_key,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore namespace inverse has no ref-set owner".to_owned(),
                        )
                    })?;
                let decoded = decode_restore_staging_member(&member.value.0)?;
                if decoded.operation_digest != operation.operation_digest
                    || decoded.destination_inode != inode
                    || inverse_owner.value != existing.value
                {
                    return Err(MetadError::Codec(
                        "restore namespace membership changed identity".to_owned(),
                    ));
                }
                predicates.extend([
                    PredicateRef {
                        family: RecordFamily::System,
                        key: inverse_key,
                        predicate: Predicate::VersionEquals(existing.version),
                    },
                    PredicateRef {
                        family: RecordFamily::System,
                        key: member_key,
                        predicate: Predicate::VersionEquals(member.version),
                    },
                    PredicateRef {
                        family: RecordFamily::System,
                        key: inverse_owner_key,
                        predicate: Predicate::VersionEquals(inverse_owner.version),
                    },
                ]);
                continue;
            }
            for key in [&member_key, &inverse_key, &inverse_owner_key] {
                if self
                    .metadata
                    .get(
                        RecordFamily::System,
                        key,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some()
                {
                    return Err(MetadError::Codec(
                        "restore namespace membership is only partially present".to_owned(),
                    ));
                }
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                });
            }
            let member = RestoreStagingMember {
                operation_digest: operation.operation_digest,
                source_inode: None,
                destination_inode: inode,
                destination_parent: Some(projection.dentry.parent),
                name: Some(projection.dentry.name.clone()),
                // Dynamic membership is keyed by inode and retains its exact
                // raw parent/name. It intentionally has no snapshot-relative
                // path, which may not be UTF-8 for ordinary namespace writes.
                relative_path: String::new(),
                canonical_index_cursor: Vec::new(),
                canonical_index_complete: false,
                manifest_cursor: Vec::new(),
                manifest_block_cursor: 0,
            };
            mutations.extend([
                Mutation {
                    family: RecordFamily::System,
                    key: member_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_member(&member)?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: inverse_key,
                    op: MutationOp::Put,
                    value: Some(inverse_value.clone()),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: inverse_owner_key,
                    op: MutationOp::Put,
                    value: Some(inverse_value.clone()),
                },
            ]);
        }
        Ok(RestoreNamespaceEnrollmentPlan {
            predicates,
            mutations,
        })
    }

    /// Whether a durable `ForkBinding` is allowed to anchor an inode-addressed
    /// namespace reachability proof. Generic clone/rollback bindings have no
    /// restore root-index row and remain legal anchors. A restore binding is a
    /// history-retention hold only: its detached tree must stay unreachable
    /// until the single attach transaction removes the binding and publishes
    /// the destination dentry.
    pub(super) fn restore_fork_binding_is_namespace_anchor(
        &self,
        binding: &ForkBinding,
        version: Version,
    ) -> Result<bool, MetadError> {
        let root_key = restore_root_index_key(self.mount, binding.fork_root);
        let Some(root_index) = self.metadata.get(
            RecordFamily::System,
            &root_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(true);
        };
        let operation_digest: [u8; 32] =
            root_index
                .0
                .as_slice()
                .try_into()
                .map_err(|_| MetadError::RestoreRootChanged {
                    root: binding.fork_root,
                })?;
        let operation = self
            .metadata
            .get(
                RecordFamily::System,
                &restore_operation_key(self.mount, &operation_digest),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: binding.fork_root,
            })?;
        let operation = decode_restore_operation(&operation.0)?;
        if operation.operation_digest != operation_digest
            || operation.destination_root != binding.fork_root
            || operation.source_root != binding.source_root
            || operation.snapshot_id != binding.snapshot_id
            || operation.read_version != binding.pinned_read_version
            || operation.created_version != binding.created_version
        {
            return Err(MetadError::RestoreBindingChanged {
                root: binding.fork_root,
            });
        }
        match operation.state {
            RestoreOperationState::Preparing
            | RestoreOperationState::ReadyToAttach
            | RestoreOperationState::Cleaning
            | RestoreOperationState::Discarding => Ok(false),
            RestoreOperationState::Complete | RestoreOperationState::Releasing => {
                Err(MetadError::Codec(
                    "attached restore operation still has a temporary ForkBinding".to_owned(),
                ))
            }
        }
    }

    pub(super) fn prepare_restore_root_release(
        &self,
        root: InodeId,
        version: Version,
    ) -> Result<Option<RestoreReleaseTransition>, MetadError> {
        let read_version = predecessor(version)?;
        let Some((mut operation, operation_item, root_item)) =
            self.restore_root_operation_at(root, read_version)?
        else {
            return Ok(None);
        };
        if operation.state != RestoreOperationState::Complete {
            return Err(MetadError::RestoreInProgress);
        }
        let active_key = restore_active_key(self.mount);
        let active_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &active_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("complete restore has no active marker".to_owned()))?;
        if active_item.value.0 != [RESTORE_FORMAT_VERSION] {
            return Err(MetadError::Codec(
                "restore active marker has an invalid value".to_owned(),
            ));
        }
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let root_key = restore_root_index_key(self.mount, root);
        let release_key = restore_release_job_key(self.mount, operation.ref_set_id);
        operation.state = RestoreOperationState::Releasing;
        let job = RestoreReleaseJob {
            operation_digest: operation.operation_digest,
            ref_set_id: operation.ref_set_id,
            phase: RestoreReleasePhase::ExactReferences,
            cursor: Vec::new(),
        };
        Ok(Some(RestoreReleaseTransition {
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: operation_key.clone(),
                    predicate: Predicate::VersionEquals(operation_item.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: root_key,
                    predicate: Predicate::VersionEquals(root_item.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: release_key.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: active_key.clone(),
                    predicate: Predicate::VersionEquals(active_item.version),
                },
            ],
            mutations: vec![
                Mutation {
                    family: RecordFamily::System,
                    key: operation_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_operation(&operation)?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: release_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_release_job(&job)?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: active_key,
                    op: MutationOp::Put,
                    value: Some(Value(vec![RESTORE_FORMAT_VERSION])),
                },
            ],
        }))
    }

    fn restore_root_operation_at(
        &self,
        root: InodeId,
        version: Version,
    ) -> Result<Option<(RestoreOperation, ReadItem, ReadItem)>, MetadError> {
        let root_key = restore_root_index_key(self.mount, root);
        let Some(root_item) = self.metadata.get_versioned(
            RecordFamily::System,
            &root_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(None);
        };
        let digest: [u8; 32] = root_item
            .value
            .0
            .as_slice()
            .try_into()
            .map_err(|_| MetadError::RestoreRootChanged { root })?;
        let operation_key = restore_operation_key(self.mount, &digest);
        let operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged { root })?;
        let operation = decode_restore_operation(&operation_item.value.0)?;
        validate_restore_operation_identity(self.mount, &digest, &operation)?;
        if operation.destination_root != root {
            return Err(MetadError::RestoreRootChanged { root });
        }
        Ok(Some((operation, operation_item, root_item)))
    }

    pub fn restore_subtree_path_to_fork(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
    ) -> Result<RestoreOutcome, MetadError> {
        self.restore_subtree_path_to_fork_initialized(
            source_path,
            snapshot_id,
            destination_path,
            RestoreInitialization::default(),
        )
    }

    pub fn restore_subtree_path_to_fork_initialized(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
        initialization: RestoreInitialization,
    ) -> Result<RestoreOutcome, MetadError> {
        let started = Instant::now();
        self.restore_to_fork_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let _restore = self
            .restore_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let result = self.restore_subtree_path_to_fork_initialized_locked(
            source_path,
            snapshot_id,
            destination_path,
            initialization,
        );
        match &result {
            Ok(_) => {
                self.restore_to_fork_success_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.restore_to_fork_failure_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.restore_to_fork_elapsed_ns_total
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.restore_to_fork_elapsed_ns_max
            .fetch_max(elapsed_ns, Ordering::Relaxed);
        result
    }

    fn restore_subtree_path_to_fork_initialized_locked(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
        initialization: RestoreInitialization,
    ) -> Result<RestoreOutcome, MetadError> {
        let source_components = parse_absolute_path(source_path)?;
        let source_path = canonical_path(&source_components)?;
        let destination_components = parse_absolute_path(destination_path)?;
        let destination_path = canonical_path(&destination_components)?;
        if source_path == destination_path {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path,
            });
        }
        let path_bytes = source_path.len().saturating_add(destination_path.len());
        if path_bytes > MAX_RESTORE_PATH_BYTES {
            return Err(MetadError::RestoreResourceLimit {
                resource: "restore source and destination path bytes".to_owned(),
                limit: MAX_RESTORE_PATH_BYTES as u64,
                actual: path_bytes as u64,
            });
        }
        let (initialization, initialization_digest) =
            canonical_restore_initialization(initialization)?;
        let operation_digest =
            restore_operation_digest(self.mount, &source_path, snapshot_id, &destination_path);
        let operation_id =
            restore_operation_id(self.mount, &source_path, snapshot_id, &destination_path)?;

        // Terminal lookup is intentionally first. A completed retry does not
        // need the source namespace or caller-owned snapshot pin to survive.
        if let Some(operation) = self.restore_operation(operation_digest)? {
            if operation.initialization_digest != initialization_digest
                || operation.source_path != source_path
                || operation.destination_path != destination_path
            {
                return Err(MetadError::RestoreDestinationConflict {
                    destination: destination_path,
                });
            }
            return match operation.state {
                RestoreOperationState::Complete => {
                    self.completed_restore_outcome(&operation_id, &operation)
                }
                RestoreOperationState::Preparing
                | RestoreOperationState::Cleaning
                | RestoreOperationState::Discarding => {
                    self.discard_preparing_restore(&operation)?;
                    self.restore_subtree_path_to_fork_initialized_locked(
                        &source_path,
                        snapshot_id,
                        &destination_path,
                        initialization,
                    )
                }
                RestoreOperationState::ReadyToAttach => {
                    self.attach_restore_destination(&operation_id, &operation)
                }
                RestoreOperationState::Releasing => Err(MetadError::RestoreInProgress),
            };
        }

        // The destination claim is a durable cross-owner serialization point.
        // Check it before resolving the source or validating its pin so a
        // conflicting request never leaks a generic predicate failure after
        // expensive preflight work.
        if let Some(claim) = self.metadata.get(
            RecordFamily::System,
            &restore_destination_claim_key(self.mount, &destination_path),
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )? {
            let claim_digest: [u8; 32] = claim.0.as_slice().try_into().map_err(|_| {
                MetadError::Codec("restore destination claim has an invalid length".to_owned())
            })?;
            if claim_digest != operation_digest {
                return Err(MetadError::RestoreDestinationConflict {
                    destination: destination_path,
                });
            }
            // Claim and operation are installed atomically. A matching orphan
            // claim is therefore a fail-closed recovery condition, never a
            // license to start a second materialization.
            return Err(MetadError::RestoreInProgress);
        }

        let Some((destination_name, destination_parent_components)) =
            destination_components.split_last()
        else {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path,
            });
        };
        let destination_proof = self.restore_directory_path_proof(destination_parent_components)?;
        if destination_proof.inode.shard_index() != self.shard_index {
            return Err(MetadError::RestoreCrossShardUnsupported {
                inode: destination_proof.inode,
            });
        }
        if self.lookup_path(&destination_path)?.is_some() {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path,
            });
        }
        let source_proof = self.restore_directory_path_proof(&source_components)?;
        if source_proof.inode.shard_index() != self.shard_index {
            return Err(MetadError::RestoreCrossShardUnsupported {
                inode: source_proof.inode,
            });
        }
        let pin_item = self
            .versioned_snapshot_pin_at(
                snapshot_id,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        self.ensure_snapshot_id_shard(snapshot_id, source_proof.inode)?;
        if pin_item.pin.root != source_proof.inode {
            return Err(MetadError::SnapshotRootMismatch {
                snapshot_id,
                expected_root: source_proof.inode,
                actual_root: Some(pin_item.pin.root),
                actual_shard: self.shard_index,
            });
        }
        self.ensure_snapshot_pin_live(&pin_item.pin)?;
        let source_read_version = Version::new(pin_item.pin.read_version)?;
        let snapshot_path_root = match self
            .resolve_components_as_directory_from_at_version_for_purpose(
                InodeId::root(),
                &source_components,
                source_read_version,
                ReadPurpose::Snapshot,
            ) {
            Ok(root) => Some(root),
            Err(MetadError::NotFound | MetadError::NotDirectory) => None,
            Err(error) => return Err(error),
        };
        if snapshot_path_root != Some(pin_item.pin.root) {
            return Err(MetadError::SnapshotRootMismatch {
                snapshot_id,
                expected_root: pin_item.pin.root,
                actual_root: snapshot_path_root,
                actual_shard: self.shard_index,
            });
        }
        let source_attr = self
            .get_attr_at_version_for_purpose(
                source_proof.inode,
                source_read_version,
                ReadPurpose::Snapshot,
            )?
            .ok_or(MetadError::NotFound)?;
        if source_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        self.preflight_restore_subtree(
            source_proof.inode,
            source_read_version,
            &source_path,
            &destination_path,
            destination_proof.inode,
            destination_name,
            &initialization,
        )?;

        // A fresh mount may need to durably initialize the object-GC Open
        // claim. Do that before allocating the operation commit version: the
        // claim's exact record version must be visible at the hold command's
        // read version.
        let object_reference = self.begin_object_reference_mutation()?;
        let destination_root = self.next_inode()?;
        let created_version = self.next_version()?;
        let operation = RestoreOperation {
            operation_digest,
            initialization_digest,
            state: RestoreOperationState::Preparing,
            source_root: source_proof.inode,
            destination_root,
            snapshot_id,
            read_version: pin_item.pin.read_version,
            created_version: created_version.get(),
            ref_set_id: created_version.get(),
            source_path,
            destination_path: destination_path.clone(),
        };
        let install_result = {
            // Linearize the fast-path hint with the first durable staging
            // write. Recovery/failover takes this same exclusive fence, so it
            // cannot prove an empty keyspace and clear the hint immediately
            // before the hold becomes visible.
            let _visibility = self
                .restore_visibility_fence
                .write()
                .unwrap_or_else(|error| error.into_inner());
            self.restore_staging_possible.store(true, Ordering::Release);
            self.install_restore_hold(RestoreHold {
                operation: &operation,
                object_reference,
                pin_version: pin_item.version,
                source_attr: &source_attr,
                source_predicates: &source_proof.predicates,
                destination_parent: destination_proof.inode,
                destination_name,
                destination_predicates: &destination_proof.predicates,
            })
        };
        if let Err(error) = install_result {
            // Predicate failure may mean no hold was installed, or that a
            // concurrent owner installed one. Re-prove rather than guessing;
            // failure leaves the hint true.
            let _ = self.recover_restore_staging_visibility();
            if matches!(error, MetadError::Metadata(MetadataError::PredicateFailed)) {
                if let Some(durable) = self.restore_operation(operation_digest)? {
                    if durable.initialization_digest != initialization_digest {
                        return Err(MetadError::RestoreDestinationConflict {
                            destination: destination_path,
                        });
                    }
                    return match durable.state {
                        RestoreOperationState::Complete => {
                            self.completed_restore_outcome(&operation_id, &durable)
                        }
                        _ => Err(MetadError::RestoreInProgress),
                    };
                }
                if let Some(claim) = self.metadata.get(
                    RecordFamily::System,
                    &restore_destination_claim_key(self.mount, &destination_path),
                    self.read_version()?,
                    ReadPurpose::WritePlanLocal,
                )? {
                    let claim_digest: [u8; 32] = claim.0.as_slice().try_into().map_err(|_| {
                        MetadError::Codec(
                            "restore destination claim has an invalid length".to_owned(),
                        )
                    })?;
                    return if claim_digest == operation_digest {
                        Err(MetadError::RestoreInProgress)
                    } else {
                        Err(MetadError::RestoreDestinationConflict {
                            destination: destination_path,
                        })
                    };
                }
                if self
                    .lookup_plus_for_write_plan(destination_proof.inode, destination_name)?
                    .is_some()
                {
                    return Err(MetadError::RestoreDestinationConflict {
                        destination: destination_path,
                    });
                }
            }
            return Err(error);
        }

        self.materialize_restore_tree(&operation)?;
        self.apply_restore_initialization(&operation, &initialization)?;
        self.materialize_and_seal_restore_indexes(
            &operation,
            destination_proof.inode,
            destination_name,
        )?;
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(&operation),
            live_test_barrier::RestoreAppliedPhase::IndexSealed,
        )?;
        self.seal_restore_base_references(&operation)?;
        self.mark_restore_ready_to_attach(&operation)?;
        self.attach_restore_destination(&operation_id, &operation)
    }

    fn restore_operation(
        &self,
        operation_digest: [u8; 32],
    ) -> Result<Option<RestoreOperation>, MetadError> {
        let Some(value) = self.metadata.get(
            RecordFamily::System,
            &restore_operation_key(self.mount, &operation_digest),
            self.read_version()?,
            ReadPurpose::UserStrong,
        )?
        else {
            return Ok(None);
        };
        let operation = decode_restore_operation(&value.0)?;
        validate_restore_operation_identity(self.mount, &operation_digest, &operation)?;
        Ok(Some(operation))
    }

    /// Enter the fail-closed visibility mode while owner/process-local state is
    /// being reconstructed. The first restore hold uses the same write fence
    /// across both this store and its durable commit, so a concurrent recovery
    /// proof cannot clear the hint between those two actions.
    pub(super) fn mark_restore_staging_possible(&self) {
        let _visibility = self
            .restore_visibility_fence
            .write()
            .unwrap_or_else(|error| error.into_inner());
        self.restore_staging_possible.store(true, Ordering::Release);
    }

    /// Commit a Complete -> Releasing namespace transition under the same
    /// visibility write fence used by the first restore hold. Setting the hint
    /// before the durable command prevents a Complete fast-path reader/writer
    /// from crossing the transition and addressing the newly detached tree.
    pub(super) fn commit_restore_release_transition(
        &self,
        command: MetadataCommand,
        starts_release: bool,
    ) -> Result<CommitResult, MetadError> {
        if !starts_release {
            return self.commit_metadata(command);
        }
        let result = {
            let _visibility = self
                .restore_visibility_fence
                .write()
                .unwrap_or_else(|error| error.into_inner());
            self.restore_staging_possible.store(true, Ordering::Release);
            self.commit_metadata(command)
        };
        match result {
            // A successful Complete -> Releasing transition deliberately
            // leaves the process-local hint in the slow path. The release
            // worker clears it only after the final durable command removes
            // the last Releasing operation.
            Ok(committed) => Ok(committed),
            Err(error) => {
                // Predicate failure may mean a concurrent transition won. A
                // failed reconstruction deliberately leaves the hint true.
                let _ = self.recover_restore_staging_visibility();
                Err(error)
            }
        }
    }

    /// Rebuild the process-local visibility hint from durable restore-private
    /// rows. The hint is only an optimization: malformed or incomplete durable
    /// state returns an error with the hint left fail closed.
    pub(super) fn recover_restore_staging_visibility(&self) -> Result<(), MetadError> {
        const RECOVERY_PAGE_ROWS: usize = 256;

        self.ensure_restore_activation_fence()?;
        let _visibility = self
            .restore_visibility_fence
            .write()
            .unwrap_or_else(|error| error.into_inner());
        // Callers invoke recovery only after bootstrap or after an image/log
        // install has made metadata scans safe. Remember that lifecycle fact
        // separately from whether this particular proof succeeds.
        self.restore_visibility_recovery_ready
            .store(true, Ordering::Release);
        self.restore_staging_possible.store(true, Ordering::Release);

        let version = self.read_version()?;
        let keyspaces = restore_control_keyspaces(self.mount);
        let operation_prefix = keyspaces
            .iter()
            .find_map(|(name, prefix)| (*name == "operation").then_some(prefix.clone()))
            .ok_or_else(|| MetadError::Codec("restore operation keyspace is missing".to_owned()))?;
        let inverse_prefix = keyspaces
            .iter()
            .find_map(|(name, prefix)| (*name == "staging_inode_inverse").then_some(prefix.clone()))
            .ok_or_else(|| {
                MetadError::Codec("restore staging inverse keyspace is missing".to_owned())
            })?;

        let mut staging_possible = false;
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: operation_prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: RECOVERY_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            for row in &rows {
                let digest_bytes = row
                    .key
                    .strip_prefix(operation_prefix.as_slice())
                    .filter(|bytes| bytes.len() == 32)
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore operation row has an invalid durable key".to_owned(),
                        )
                    })?;
                let digest: [u8; 32] = digest_bytes.try_into().map_err(|_| {
                    MetadError::Codec("restore operation digest has an invalid length".to_owned())
                })?;
                if restore_operation_key(self.mount, &digest) != row.key {
                    return Err(MetadError::Codec(
                        "restore operation row key changed identity".to_owned(),
                    ));
                }
                let operation = decode_restore_operation(&row.value.0)?;
                validate_restore_operation_identity(self.mount, &digest, &operation)?;
                if operation.state == RestoreOperationState::Complete {
                    self.validate_restore_complete_visibility_marker(&operation, version)?;
                } else {
                    staging_possible = true;
                }
            }
            start_after = rows.last().map(|row| row.key.clone());
            if rows.len() < RECOVERY_PAGE_ROWS {
                break;
            }
        }

        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: inverse_prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: RECOVERY_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            for row in &rows {
                let inode_bytes = row
                    .key
                    .strip_prefix(inverse_prefix.as_slice())
                    .filter(|bytes| bytes.len() == 8)
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore staging inverse has an invalid durable key".to_owned(),
                        )
                    })?;
                let inode = InodeId::new(u64::from_be_bytes(inode_bytes.try_into().map_err(
                    |_| {
                        MetadError::Codec(
                            "restore staging inverse inode has an invalid length".to_owned(),
                        )
                    },
                )?))?;
                if restore_staging_inode_key(self.mount, inode) != row.key {
                    return Err(MetadError::Codec(
                        "restore staging inverse key changed identity".to_owned(),
                    ));
                }
                let (operation_digest, ref_set_id) = decode_restore_staging_inverse(&row.value.0)?;
                let operation = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &restore_operation_key(self.mount, &operation_digest),
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore staging inverse has no durable operation".to_owned(),
                        )
                    })?;
                let operation = decode_restore_operation(&operation.0)?;
                if operation.operation_digest != operation_digest {
                    return Err(MetadError::Codec(
                        "restore staging inverse operation changed identity".to_owned(),
                    ));
                }
                if operation.ref_set_id != ref_set_id {
                    return Err(MetadError::Codec(
                        "restore staging inverse changed operation identity".to_owned(),
                    ));
                }
            }
            start_after = rows.last().map(|row| row.key.clone());
            if rows.len() < RECOVERY_PAGE_ROWS {
                break;
            }
        }
        self.restore_staging_possible
            .store(staging_possible, Ordering::Release);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn restore_staging_slow_path_enabled(&self) -> bool {
        self.restore_staging_possible.load(Ordering::Acquire)
    }

    fn completed_restore_outcome(
        &self,
        operation_id: &str,
        operation: &RestoreOperation,
    ) -> Result<RestoreOutcome, MetadError> {
        // The terminal operation row is authoritative. The restored root may
        // have been renamed since completion, and exact retry remains valid
        // after source pin retirement/source deletion.
        Ok(RestoreOutcome {
            operation_id: operation_id.to_owned(),
            state: RestoreState::Complete,
            source_root: operation.source_root,
            destination_root: operation.destination_root,
            snapshot_id: operation.snapshot_id,
            read_version: operation.read_version,
            cleanup_pending: false,
        })
    }

    fn restore_directory_path_proof(
        &self,
        components: &[DentryName],
    ) -> Result<RestorePathProof, MetadError> {
        let version = self.read_version()?;
        let mut inode = InodeId::root();
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, inode),
            predicate: Predicate::Exists,
        }];
        for component in components {
            let (entry, dentry_version) = self
                .lookup_plus_at_version_for_purpose(
                    inode,
                    component,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_key(self.mount, inode, component),
                predicate: Predicate::VersionEquals(dentry_version),
            });
            inode = entry.attr.inode;
        }
        Ok(RestorePathProof { inode, predicates })
    }

    fn install_restore_hold(&self, hold: RestoreHold<'_>) -> Result<(), MetadError> {
        let RestoreHold {
            operation,
            object_reference,
            pin_version,
            source_attr,
            source_predicates,
            destination_parent,
            destination_name,
            destination_predicates,
        } = hold;
        let version = Version::new(operation.created_version)?;
        let read_version = predecessor(version)?;
        // All inode/version allocation happened before entering this critical
        // section. Hold allocator_gate through the hold apply so a reservation
        // cannot rewrite a v1 allocator row between the active-marker CAS and
        // the atomic v2 downgrade-fence mutation.
        let _allocator_guard = self.allocator_gate.lock().map_err(|error| {
            MetadataError::Backend(format!("metadata allocator gate poisoned: {error}"))
        })?;
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let claim_key = restore_destination_claim_key(self.mount, &operation.destination_path);
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let root_index_key = restore_root_index_key(self.mount, operation.destination_root);
        let member_key = restore_staging_member_key(
            self.mount,
            operation.ref_set_id,
            operation.destination_root,
        );
        let staging_inode_key = restore_staging_inode_key(self.mount, operation.destination_root);
        let staging_inverse_owner_key = restore_staging_inverse_owner_key(
            self.mount,
            operation.ref_set_id,
            operation.destination_root,
        );
        let active_key = restore_active_key(self.mount);
        let active = self.metadata.get_versioned(
            RecordFamily::System,
            &active_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        if let Some(active) = &active {
            if active.value.0 != [RESTORE_FORMAT_VERSION] {
                return Err(MetadError::Codec(
                    "invalid restore-to-fork active marker".to_owned(),
                ));
            }
        }
        let activation_key = restore_activation_fence_key(self.mount);
        let activation = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &activation_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore activation fence is missing".to_owned()))?;
        if activation.value.0 != [RESTORE_FORMAT_VERSION] {
            return Err(MetadError::Codec(
                "restore activation fence has an invalid value".to_owned(),
            ));
        }
        let mut root_attr = source_attr.clone();
        root_attr.inode = operation.destination_root;
        root_attr.nlink = FileType::Directory.initial_link_count();
        root_attr.generation = version.get();
        root_attr.ctime_ms = current_time_ms();
        let binding = ForkBinding {
            fork_root: operation.destination_root,
            source_root: operation.source_root,
            pinned_read_version: operation.read_version,
            snapshot_id: operation.snapshot_id,
            created_version: version.get(),
        };
        let mut predicates = source_predicates.to_vec();
        predicates.extend_from_slice(destination_predicates);
        predicates
            .extend(self.restore_destination_parent_predicates(destination_parent, read_version)?);
        predicates.extend([
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::Snapshot,
                key: snapshot_pin_key(self.mount, operation.snapshot_id),
                predicate: Predicate::VersionEquals(pin_version),
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_key(self.mount, destination_parent, destination_name),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, operation.destination_root),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: claim_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: root_index_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: member_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: staging_inode_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: staging_inverse_owner_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: active_key.clone(),
                predicate: active
                    .as_ref()
                    .map(|item| Predicate::VersionEquals(item.version))
                    .unwrap_or(Predicate::NotExists),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: activation_key.clone(),
                predicate: Predicate::VersionEquals(activation.version),
            },
        ]);
        let mut command = MetadataCommand {
            request_id: request_id(
                b"restore-install-hold",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::SnapshotSubtree,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: operation_key.clone(),
            predicates,
            mutations: vec![
                Mutation {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, operation.destination_root),
                    op: MutationOp::Put,
                    value: Some(Value(encode_inode_attr(&root_attr))),
                },
                Mutation {
                    family: RecordFamily::ForkBinding,
                    key: binding_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_fork_binding(&binding))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: operation_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_operation(operation)?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: claim_key,
                    op: MutationOp::Put,
                    value: Some(Value(operation.operation_digest.to_vec())),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: root_index_key,
                    op: MutationOp::Put,
                    value: Some(Value(operation.operation_digest.to_vec())),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: member_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_member(
                        &RestoreStagingMember {
                            operation_digest: operation.operation_digest,
                            source_inode: Some(operation.source_root),
                            destination_inode: operation.destination_root,
                            destination_parent: None,
                            name: None,
                            relative_path: String::new(),
                            canonical_index_cursor: Vec::new(),
                            canonical_index_complete: true,
                            manifest_cursor: Vec::new(),
                            manifest_block_cursor: 0,
                        },
                    )?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: staging_inode_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_inverse(operation))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: staging_inverse_owner_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_inverse(operation))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: active_key,
                    op: MutationOp::Put,
                    value: Some(Value(vec![RESTORE_FORMAT_VERSION])),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: activation_key,
                    op: MutationOp::Put,
                    value: Some(Value(vec![RESTORE_FORMAT_VERSION])),
                },
            ],
            watch: Vec::new(),
        };
        self.commit_metadata_from_factory(|| {
            // Build the allocator mutation inside the owner epoch fence used by
            // apply; its encoded epoch can therefore never lag a concurrent
            // failover. The exact allocator version joins the same command as
            // the first restore marker and temporary ForkBinding.
            let (allocator_predicate, allocator_mutation) =
                self.restore_allocator_fence_plan(read_version)?;
            command.predicates.push(allocator_predicate);
            command.mutations.push(allocator_mutation);
            validate_restore_command_bounds(&command, "restore install hold")?;
            Ok(command)
        })?;
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(operation),
            live_test_barrier::RestoreAppliedPhase::Hold,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn preflight_restore_subtree(
        &self,
        source_root: InodeId,
        read_version: Version,
        source_path: &str,
        destination_path: &str,
        destination_parent: InodeId,
        destination_name: &DentryName,
        initialization: &RestoreInitialization,
    ) -> Result<(), MetadError> {
        // Build the same command shape used by materialization before any
        // durable restore state or inode allocation exists. All ids are
        // fixed-width in metadata keys/values, so source ids are exact size
        // stand-ins for the detached destination ids.
        let bounds_version = Version::new(2)?;
        let bounds_operation = RestoreOperation {
            operation_digest: [0; 32],
            initialization_digest: [0; 32],
            state: RestoreOperationState::Preparing,
            source_root,
            destination_root: source_root,
            snapshot_id: 1,
            read_version: read_version.get(),
            created_version: bounds_version.get(),
            ref_set_id: bounds_version.get(),
            source_path: source_path.to_owned(),
            destination_path: destination_path.to_owned(),
        };
        let mut base_reference_preflight =
            self.begin_restore_base_reference_preflight(&bounds_operation);
        let bounds_object_reference = ObjectReferenceMutation::from_version(Version::new(1)?);
        let initialization_paths = initialization
            .remove_relative_paths
            .iter()
            .cloned()
            .chain(
                initialization
                    .files
                    .iter()
                    .map(|file| file.relative_path.clone()),
            )
            .collect::<HashSet<_>>();
        let removed_paths = initialization
            .remove_relative_paths
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let initialization_files = initialization
            .files
            .iter()
            .map(|file| (file.relative_path.as_str(), file))
            .collect::<BTreeMap<_, _>>();
        let mut matched_initialization_files = HashSet::<String>::new();
        let mut canonical_index_batch =
            super::restore_index::RestoreCanonicalIndexPreflightBatch::default();
        self.preflight_restore_xattrs(source_root, read_version)?;
        let mut root_attr = self
            .get_attr_at_version_for_purpose(source_root, read_version, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
        root_attr.inode = source_root;
        root_attr.nlink = FileType::Directory.initial_link_count();
        root_attr.generation = bounds_version.get();
        root_attr.ctime_ms = current_time_ms();
        self.push_restore_canonical_index_preflight_member(
            &bounds_operation,
            &RestoreStagingMember {
                operation_digest: bounds_operation.operation_digest,
                source_inode: Some(source_root),
                destination_inode: source_root,
                destination_parent: None,
                name: None,
                relative_path: String::new(),
                canonical_index_cursor: Vec::new(),
                canonical_index_complete: true,
                manifest_cursor: Vec::new(),
                manifest_block_cursor: 0,
            },
            &projection(
                destination_parent,
                destination_name.clone(),
                root_attr,
                None,
            ),
            &mut canonical_index_batch,
        )?;
        let mut queue = vec![RestoreCloneFrame {
            source: source_root,
            destination: source_root,
            relative_components: Vec::new(),
            after: None,
        }];
        self.preflight_restore_custom_index_catalog(
            &bounds_operation,
            source_root,
            &[],
            &initialization_paths,
        )?;
        let mut entries = 1_usize;
        while let Some(frame) = queue.pop() {
            let page = self.read_dir_plus_page_at_version_for_purpose(
                frame.source,
                frame.after.as_ref(),
                RESTORE_BATCH_ENTRIES,
                read_version,
                ReadPurpose::Snapshot,
            )?;
            let children = page.entries;
            entries = entries.saturating_add(children.len());
            if entries > MAX_RESTORE_SUBTREE_ENTRIES {
                return Err(MetadError::RestoreResourceLimit {
                    resource: "restore subtree entries".to_owned(),
                    limit: MAX_RESTORE_SUBTREE_ENTRIES as u64,
                    actual: entries as u64,
                });
            }
            if let Some(after) = page.next_cursor {
                queue.push(RestoreCloneFrame {
                    source: frame.source,
                    destination: frame.destination,
                    relative_components: frame.relative_components.clone(),
                    after: Some(after),
                });
            }
            if !children.is_empty() {
                let mut bounds_batch = Vec::with_capacity(children.len());
                for child in &children {
                    let mut child_relative_components = frame.relative_components.clone();
                    child_relative_components.push(child.dentry.name.clone());
                    let relative_path =
                        canonical_restore_relative_components(&child_relative_components)?;
                    if child.attr.inode.shard_index() != self.shard_index {
                        return Err(MetadError::RestoreCrossShardUnsupported {
                            inode: child.attr.inode,
                        });
                    }
                    if child.attr.file_type != FileType::Directory && child.attr.nlink != 1 {
                        return Err(MetadError::RestoreHardlinkUnsupported {
                            inode: child.attr.inode,
                        });
                    }
                    let (body, chunks) = match &child.body {
                        Some(source_body) => {
                            let mut body = source_body.clone();
                            body.base_generation = 0;
                            let chunks = self.chunk_manifests_for_body_at_version(
                                child.attr.inode,
                                source_body,
                                read_version,
                                ReadPurpose::Snapshot,
                            )?;
                            (Some(body), chunks)
                        }
                        None => (None, Vec::new()),
                    };
                    if !removed_paths.contains(&relative_path)
                        && !initialization_files.contains_key(relative_path.as_str())
                    {
                        if let Some(source_body) = &child.body {
                            // Source manifests are borrowed in the detached clone even
                            // when their canonical object key embeds the source inode.
                            // Initialization replacements are newly owned destination
                            // objects and are intentionally excluded above.
                            self.preflight_restore_base_references_for_entry(
                                &bounds_operation,
                                child.attr.inode,
                                source_body.generation,
                                None,
                                &chunks,
                                &mut base_reference_preflight,
                            )?;
                        }
                    }
                    self.preflight_restore_xattrs(child.attr.inode, read_version)?;
                    if child.attr.file_type == FileType::Directory {
                        self.preflight_restore_custom_index_catalog(
                            &bounds_operation,
                            child.attr.inode,
                            &child_relative_components,
                            &initialization_paths,
                        )?;
                        queue.push(RestoreCloneFrame {
                            source: child.attr.inode,
                            destination: child.attr.inode,
                            relative_components: child_relative_components,
                            after: None,
                        });
                    }
                    if !removed_paths.contains(&relative_path) {
                        let (source_inode, destination_body) =
                            if let Some(file) = initialization_files.get(relative_path.as_str()) {
                                if child.attr.file_type != FileType::File {
                                    return Err(MetadError::NotFile);
                                }
                                matched_initialization_files.insert(relative_path.clone());
                                let (_, _, body, _) =
                                    preview_restore_initialization_artifact(self.mount, file)?;
                                (None, Some(body))
                            } else {
                                (Some(child.attr.inode), body.clone())
                            };
                        let mut destination_attr = child.attr.clone();
                        destination_attr.inode = child.attr.inode;
                        destination_attr.nlink = destination_attr.file_type.initial_link_count();
                        destination_attr.generation = destination_body
                            .as_ref()
                            .map_or(bounds_version.get(), |body| body.generation);
                        destination_attr.ctime_ms = current_time_ms();
                        let destination_projection = projection(
                            frame.destination,
                            child.dentry.name.clone(),
                            destination_attr,
                            destination_body,
                        );
                        self.push_restore_canonical_index_preflight_member(
                            &bounds_operation,
                            &RestoreStagingMember {
                                operation_digest: bounds_operation.operation_digest,
                                source_inode,
                                destination_inode: child.attr.inode,
                                destination_parent: Some(frame.destination),
                                name: Some(child.dentry.name.clone()),
                                relative_path: relative_path.clone(),
                                canonical_index_cursor: Vec::new(),
                                canonical_index_complete: true,
                                manifest_cursor: Vec::new(),
                                manifest_block_cursor: 0,
                            },
                            &destination_projection,
                            &mut canonical_index_batch,
                        )?;
                    }
                    bounds_batch.push(RestoreCloneEntry {
                        source: child.clone(),
                        destination: child.attr.inode,
                        relative_path,
                        body,
                        chunks,
                    });
                }
                let command = self.build_restore_children_command(
                    &bounds_operation,
                    frame.source,
                    &bounds_batch,
                    bounds_object_reference,
                    bounds_version,
                )?;
                validate_restore_command_bounds(&command, "restore materialization batch")?;
            }
        }
        for (index, file) in initialization.files.iter().enumerate() {
            if matched_initialization_files.contains(&file.relative_path) {
                continue;
            }
            let components = restore_relative_components(&file.relative_path)?;
            let (name, parent_components) = components.split_last().ok_or_else(|| {
                MetadError::InvalidPath("restore initialization cannot write root".to_owned())
            })?;
            let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
                source_root,
                parent_components,
                read_version,
                ReadPurpose::Snapshot,
            )?;
            let (_, generation, body, _) =
                preview_restore_initialization_artifact(self.mount, file)?;
            let synthetic_inode =
                InodeId::new(u64::MAX.checked_sub(index as u64).ok_or_else(|| {
                    MetadError::Codec("restore preview inode underflow".to_owned())
                })?)?;
            let now_ms = current_time_ms();
            let destination_projection = projection(
                parent,
                name.clone(),
                InodeAttr {
                    inode: synthetic_inode,
                    file_type: FileType::File,
                    mode: file.mode,
                    uid: file.uid,
                    gid: file.gid,
                    rdev: 0,
                    nlink: FileType::File.initial_link_count(),
                    size: body.size,
                    generation: generation.get(),
                    mtime_ms: now_ms,
                    ctime_ms: now_ms,
                },
                Some(body),
            );
            self.push_restore_canonical_index_preflight_member(
                &bounds_operation,
                &RestoreStagingMember {
                    operation_digest: bounds_operation.operation_digest,
                    source_inode: None,
                    destination_inode: synthetic_inode,
                    destination_parent: Some(parent),
                    name: Some(name.clone()),
                    relative_path: file.relative_path.clone(),
                    canonical_index_cursor: Vec::new(),
                    canonical_index_complete: true,
                    manifest_cursor: Vec::new(),
                    manifest_block_cursor: 0,
                },
                &destination_projection,
                &mut canonical_index_batch,
            )?;
        }
        self.preflight_restore_initialization(source_root, read_version, initialization)?;
        self.finish_restore_canonical_index_preflight(
            &bounds_operation,
            &mut canonical_index_batch,
        )?;
        self.finish_restore_base_reference_preflight(
            &bounds_operation,
            &mut base_reference_preflight,
        )
    }

    fn preflight_restore_xattrs(
        &self,
        inode: InodeId,
        read_version: Version,
    ) -> Result<(), MetadError> {
        self.restore_xattrs_for_copy(inode, read_version, ReadPurpose::Snapshot)?;
        Ok(())
    }

    fn restore_xattrs_for_copy(
        &self,
        inode: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<ScanItem>, MetadError> {
        const PAGE_ROWS: usize = 64;
        let prefix = xattr_prefix(self.mount, inode);
        let mut rows = Vec::new();
        let mut start_after = None;
        loop {
            let page = self.metadata.scan(ScanRequest {
                family: RecordFamily::Xattr,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: PAGE_ROWS,
                purpose,
            })?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            start_after = page.last().map(|row| row.key.clone());
            rows.extend(page);
            self.validate_restore_xattr_rows(inode, &rows)?;
            if page_len < PAGE_ROWS {
                break;
            }
        }
        Ok(rows)
    }

    fn validate_restore_xattr_rows(
        &self,
        inode: InodeId,
        rows: &[ScanItem],
    ) -> Result<(), MetadError> {
        // Runtime copy has four fixed predicates (GC claim, operation,
        // temporary binding, destination inode), plus one destination
        // NotExists predicate and one Put per xattr.
        let items = 4_usize.saturating_add(rows.len().saturating_mul(2));
        if items > 4096 {
            return Err(MetadError::RestoreResourceLimit {
                resource: "restore inode xattr command items".to_owned(),
                limit: 4096,
                actual: items as u64,
            });
        }
        let operation_key_len = restore_operation_key(self.mount, &[0; 32]).len();
        let object_gc_claim_key_len = object_gc_claim_key(self.mount).len();
        let binding_key_len = fork_binding_key(self.mount, inode).len();
        let inode_key_len = inode_key(self.mount, inode).len();
        let primary_key_len = xattr_prefix(self.mount, inode).len();
        let bytes = b"restore-copy-xattrs"
            .len()
            .saturating_add(24)
            .saturating_add(primary_key_len)
            .saturating_add(object_gc_claim_key_len)
            .saturating_add(32)
            .saturating_add(operation_key_len)
            .saturating_add(32)
            .saturating_add(binding_key_len)
            .saturating_add(32)
            .saturating_add(inode_key_len)
            .saturating_add(32)
            .saturating_add(rows.iter().fold(0_usize, |total, row| {
                // Source and destination xattr keys have identical widths.
                total
                    .saturating_add(row.key.len())
                    .saturating_add(32)
                    .saturating_add(row.key.len())
                    .saturating_add(row.value.0.len())
                    .saturating_add(16)
            }));
        if bytes > MAX_RESTORE_INITIALIZATION_BYTES {
            return Err(MetadError::RestoreResourceLimit {
                resource: "restore inode xattr command bytes".to_owned(),
                limit: MAX_RESTORE_INITIALIZATION_BYTES as u64,
                actual: bytes as u64,
            });
        }
        Ok(())
    }

    fn preflight_restore_initialization(
        &self,
        source_root: InodeId,
        read_version: Version,
        initialization: &RestoreInitialization,
    ) -> Result<(), MetadError> {
        let bounds_version = Version::new(2)?;
        let bounds_operation = RestoreOperation {
            operation_digest: [0; 32],
            initialization_digest: [0; 32],
            state: RestoreOperationState::Preparing,
            source_root,
            destination_root: source_root,
            snapshot_id: 1,
            read_version: read_version.get(),
            created_version: bounds_version.get(),
            ref_set_id: bounds_version.get(),
            source_path: "/".to_owned(),
            destination_path: "/".to_owned(),
        };
        let bounds_object_reference = ObjectReferenceMutation::from_version(Version::new(1)?);
        for path in &initialization.remove_relative_paths {
            let components = restore_relative_components(path)?;
            let (name, parents) = components.split_last().ok_or_else(|| {
                MetadError::InvalidPath("restore initialization cannot remove root".to_owned())
            })?;
            let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
                source_root,
                parents,
                read_version,
                ReadPurpose::Snapshot,
            ) {
                Ok(parent) => parent,
                Err(MetadError::NotFound) => continue,
                Err(err) => return Err(err),
            };
            if let Some((entry, dentry_version)) = self.lookup_plus_at_version_for_purpose(
                parent,
                name,
                read_version,
                ReadPurpose::Snapshot,
            )? {
                if entry.attr.file_type == FileType::Directory {
                    return Err(MetadError::InvalidPath(format!(
                        "restore initialization cannot remove directory {path}"
                    )));
                }
                let chunks = if let Some(body) = &entry.body {
                    self.chunk_manifests_for_body_at_version(
                        entry.attr.inode,
                        body,
                        read_version,
                        ReadPurpose::Snapshot,
                    )?
                } else {
                    Vec::new()
                };
                let xattr_keys = self
                    .restore_xattrs_for_copy(entry.attr.inode, read_version, ReadPurpose::Snapshot)?
                    .into_iter()
                    .map(|row| row.key)
                    .collect::<Vec<_>>();
                let staging = RestoreStagingProof {
                    member: RestoreStagingMember {
                        operation_digest: bounds_operation.operation_digest,
                        source_inode: Some(entry.attr.inode),
                        destination_inode: entry.attr.inode,
                        destination_parent: Some(parent),
                        name: Some(name.clone()),
                        relative_path: path.clone(),
                        canonical_index_cursor: Vec::new(),
                        canonical_index_complete: true,
                        manifest_cursor: Vec::new(),
                        manifest_block_cursor: 0,
                    },
                    member_version: bounds_version,
                    inverse_version: bounds_version,
                    inverse_owner_version: bounds_version,
                };
                let command = self.build_restore_initialization_remove_command(
                    RestoreInitializationRemoveCommand {
                        operation: &bounds_operation,
                        parent,
                        name,
                        entry: &entry,
                        dentry_version,
                        staging: &staging,
                        chunks: &chunks,
                        xattr_keys: &xattr_keys,
                        version: bounds_version,
                    },
                )?;
                validate_restore_command_bounds(&command, "restore initialization remove")?;
            }
        }
        for file in &initialization.files {
            let components = restore_relative_components(&file.relative_path)?;
            let (name, parents) = components.split_last().ok_or_else(|| {
                MetadError::InvalidPath("restore initialization cannot write root".to_owned())
            })?;
            let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
                source_root,
                parents,
                read_version,
                ReadPurpose::Snapshot,
            )?;
            let existing = self.lookup_plus_at_version_for_purpose(
                parent,
                name,
                read_version,
                ReadPurpose::Snapshot,
            )?;
            if existing
                .as_ref()
                .is_some_and(|(entry, _)| entry.attr.file_type != FileType::File)
            {
                return Err(MetadError::NotFile);
            }
            let (preview_inode, object_generation, body, chunks) =
                preview_restore_initialization_artifact(self.mount, file)?;
            let old_chunks = match existing.as_ref().and_then(|(entry, _)| entry.body.as_ref()) {
                Some(old_body) => self.chunk_manifests_for_body_at_version(
                    existing
                        .as_ref()
                        .expect("body implies an existing entry")
                        .0
                        .attr
                        .inode,
                    old_body,
                    read_version,
                    ReadPurpose::Snapshot,
                )?,
                None => Vec::new(),
            };
            let staging = existing.as_ref().map(|(entry, _)| RestoreStagingProof {
                member: RestoreStagingMember {
                    operation_digest: bounds_operation.operation_digest,
                    source_inode: Some(entry.attr.inode),
                    destination_inode: preview_inode,
                    destination_parent: Some(parent),
                    name: Some(name.clone()),
                    relative_path: file.relative_path.clone(),
                    canonical_index_cursor: Vec::new(),
                    canonical_index_complete: true,
                    manifest_cursor: Vec::new(),
                    manifest_block_cursor: 0,
                },
                member_version: bounds_version,
                inverse_version: bounds_version,
                inverse_owner_version: bounds_version,
            });
            let command = self.build_restore_initialization_publish_command(
                RestoreInitializationPublishCommand {
                    operation: &bounds_operation,
                    parent,
                    name,
                    file,
                    existing: existing.as_ref(),
                    inode: preview_inode,
                    object_generation,
                    intent_version: bounds_version,
                    object_reference: bounds_object_reference,
                    body: &body,
                    chunks: &chunks,
                    old_chunks: &old_chunks,
                    staging: staging.as_ref(),
                    version: bounds_version,
                },
            )?;
            validate_restore_command_bounds(&command, "restore initialization publish")?;
        }
        Ok(())
    }

    fn materialize_restore_tree(&self, operation: &RestoreOperation) -> Result<(), MetadError> {
        let source_version = Version::new(operation.read_version)?;
        let mut batch_index = 0_u64;
        self.copy_restore_xattrs(
            operation,
            operation.source_root,
            operation.destination_root,
            source_version,
        )?;
        let mut queue = vec![RestoreCloneFrame {
            source: operation.source_root,
            destination: operation.destination_root,
            relative_components: Vec::new(),
            after: None,
        }];
        while let Some(frame) = queue.pop() {
            let page = self.read_dir_plus_page_at_version_for_purpose(
                frame.source,
                frame.after.as_ref(),
                RESTORE_BATCH_ENTRIES,
                source_version,
                ReadPurpose::Snapshot,
            )?;
            if let Some(after) = page.next_cursor {
                queue.push(RestoreCloneFrame {
                    source: frame.source,
                    destination: frame.destination,
                    relative_components: frame.relative_components.clone(),
                    after: Some(after),
                });
            }
            if !page.entries.is_empty() {
                let mut batch = Vec::with_capacity(page.entries.len());
                for source in &page.entries {
                    let mut relative_components = frame.relative_components.clone();
                    relative_components.push(source.dentry.name.clone());
                    let relative_path =
                        canonical_restore_relative_components(&relative_components)?;
                    let destination = self.next_inode()?;
                    let (body, chunks) = match &source.body {
                        Some(source_body) => {
                            // Effective manifests are materialized under the
                            // destination's top generation. Clearing the base
                            // pointer is mandatory: copied sparse/append bodies
                            // do not have the source inode's older summaries.
                            let mut body = source_body.clone();
                            body.base_generation = 0;
                            let chunks = self.chunk_manifests_for_body_at_version(
                                source.attr.inode,
                                source_body,
                                source_version,
                                ReadPurpose::Snapshot,
                            )?;
                            (Some(body), chunks)
                        }
                        None => (None, Vec::new()),
                    };
                    batch.push(RestoreCloneEntry {
                        source: source.clone(),
                        destination,
                        relative_path,
                        body,
                        chunks,
                    });
                }
                self.commit_restore_children(operation, frame.destination, &batch, batch_index)?;
                batch_index = batch_index.checked_add(1).ok_or_else(|| {
                    MetadError::Codec("restore materialization batch index overflow".to_owned())
                })?;
                for entry in batch {
                    self.copy_restore_xattrs(
                        operation,
                        entry.source.attr.inode,
                        entry.destination,
                        source_version,
                    )?;
                    if entry.source.attr.file_type == FileType::Directory {
                        queue.push(RestoreCloneFrame {
                            source: entry.source.attr.inode,
                            destination: entry.destination,
                            relative_components: restore_relative_components(&entry.relative_path)?,
                            after: None,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn commit_restore_children(
        &self,
        operation: &RestoreOperation,
        destination_parent: InodeId,
        entries: &[RestoreCloneEntry],
        batch_index: u64,
    ) -> Result<(), MetadError> {
        if entries.is_empty() {
            return Ok(());
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let command = self.build_restore_children_command(
            operation,
            destination_parent,
            entries,
            object_reference,
            version,
        )?;
        validate_restore_command_bounds(&command, "restore materialization batch")?;
        self.commit_metadata(command)?;
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(operation),
            live_test_barrier::RestoreAppliedPhase::MaterializeBatch(batch_index),
        )?;
        Ok(())
    }

    fn build_restore_children_command(
        &self,
        operation: &RestoreOperation,
        destination_parent: InodeId,
        entries: &[RestoreCloneEntry],
        object_reference: ObjectReferenceMutation,
        version: Version,
    ) -> Result<MetadataCommand, MetadError> {
        let read_version = predecessor(version)?;
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key,
                predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key,
                predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, destination_parent),
                predicate: Predicate::Exists,
            },
        ];
        let mut mutations = Vec::new();
        for entry in entries {
            let mut attr = entry.source.attr.clone();
            attr.inode = entry.destination;
            attr.nlink = attr.file_type.initial_link_count();
            attr.generation = entry
                .body
                .as_ref()
                .map_or(version.get(), |body| body.generation);
            attr.ctime_ms = current_time_ms();
            let projection = projection(
                destination_parent,
                entry.source.dentry.name.clone(),
                attr,
                entry.body.clone(),
            );
            let dentry = dentry_key(self.mount, destination_parent, &entry.source.dentry.name);
            let inode = inode_key(self.mount, entry.destination);
            let member =
                restore_staging_member_key(self.mount, operation.ref_set_id, entry.destination);
            let staging_inode = restore_staging_inode_key(self.mount, entry.destination);
            let staging_inverse_owner = restore_staging_inverse_owner_key(
                self.mount,
                operation.ref_set_id,
                entry.destination,
            );
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: member.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: staging_inode.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: staging_inverse_owner.clone(),
                    predicate: Predicate::NotExists,
                },
            ]);
            mutations.extend([
                Mutation {
                    family: RecordFamily::Inode,
                    key: inode,
                    op: MutationOp::Put,
                    value: Some(Value(encode_inode_attr(&projection.attr))),
                },
                put_projection_mutation(RecordFamily::Dentry, dentry, &projection),
                Mutation {
                    family: RecordFamily::System,
                    key: member,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_member(
                        &RestoreStagingMember {
                            operation_digest: operation.operation_digest,
                            source_inode: Some(entry.source.attr.inode),
                            destination_inode: entry.destination,
                            destination_parent: Some(destination_parent),
                            name: Some(entry.source.dentry.name.clone()),
                            relative_path: entry.relative_path.clone(),
                            canonical_index_cursor: Vec::new(),
                            canonical_index_complete: true,
                            manifest_cursor: Vec::new(),
                            manifest_block_cursor: 0,
                        },
                    )?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: staging_inode,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_inverse(operation))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: staging_inverse_owner,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_inverse(operation))),
                },
            ]);
            if let Some(body) = &projection.body {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(
                        self.mount,
                        entry.destination,
                        body.generation,
                        BODY_SUMMARY_CHUNK_INDEX,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_body_descriptor(body))),
                });
                mutations.extend(entry.chunks.iter().map(|chunk| Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(
                        self.mount,
                        entry.destination,
                        body.generation,
                        chunk.chunk_index,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_chunk_manifest(chunk))),
                }));
            }
        }
        Ok(MetadataCommand {
            request_id: request_id(
                b"restore-materialize-batch",
                self.mount,
                destination_parent,
                version,
            ),
            kind: CommandKind::CreateFiles,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, destination_parent),
            predicates,
            mutations,
            // Detached materialization never emits user-visible watch events.
            watch: Vec::new(),
        })
    }

    fn copy_restore_xattrs(
        &self,
        operation: &RestoreOperation,
        source: InodeId,
        destination: InodeId,
        source_version: Version,
    ) -> Result<(), MetadError> {
        let source_prefix = xattr_prefix(self.mount, source);
        let rows = self.restore_xattrs_for_copy(source, source_version, ReadPurpose::Snapshot)?;
        if rows.is_empty() {
            return Ok(());
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key,
                predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: fork_binding_key(self.mount, operation.destination_root),
                predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, destination),
                predicate: Predicate::Exists,
            },
        ];
        let mut mutations = Vec::with_capacity(rows.len());
        for row in rows {
            let name = row
                .key
                .strip_prefix(source_prefix.as_slice())
                .ok_or_else(|| {
                    MetadError::Codec("restore xattr scan escaped source prefix".to_owned())
                })?;
            let key = xattr_key(self.mount, destination, name);
            predicates.push(PredicateRef {
                family: RecordFamily::Xattr,
                key: key.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::Xattr,
                key,
                op: MutationOp::Put,
                value: Some(row.value),
            });
        }
        let command = MetadataCommand {
            request_id: request_id(b"restore-copy-xattrs", self.mount, destination, version),
            kind: CommandKind::SetXattr,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Xattr,
            primary_key: xattr_prefix(self.mount, destination),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore xattr copy")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn apply_restore_initialization(
        &self,
        operation: &RestoreOperation,
        initialization: &RestoreInitialization,
    ) -> Result<(), MetadError> {
        for relative_path in &initialization.remove_relative_paths {
            let components = restore_relative_components(relative_path)?;
            let (name, parents) = components.split_last().ok_or_else(|| {
                MetadError::InvalidPath("restore initialization cannot remove root".to_owned())
            })?;
            let version = self.read_version()?;
            let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
                operation.destination_root,
                parents,
                version,
                ReadPurpose::RestoreStaging,
            ) {
                Ok(parent) => parent,
                Err(MetadError::NotFound) => continue,
                Err(err) => return Err(err),
            };
            match self.lookup_plus_at_version_for_purpose(
                parent,
                name,
                version,
                ReadPurpose::RestoreStaging,
            )? {
                None => continue,
                Some((entry, _)) if entry.attr.file_type == FileType::Directory => {
                    return Err(MetadError::InvalidPath(format!(
                        "restore initialization cannot remove directory {relative_path}"
                    )))
                }
                Some((entry, dentry_version)) => {
                    self.remove_restore_initialization_entry(
                        operation,
                        parent,
                        name,
                        &entry,
                        dentry_version,
                    )?;
                }
            }
        }

        for (file_index, file) in initialization.files.iter().enumerate() {
            let components = restore_relative_components(&file.relative_path)?;
            let (name, parents) = components.split_last().ok_or_else(|| {
                MetadError::InvalidPath("restore initialization cannot write root".to_owned())
            })?;
            let version = self.read_version()?;
            let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
                operation.destination_root,
                parents,
                version,
                ReadPurpose::RestoreStaging,
            )?;
            let existing = self.lookup_plus_at_version_for_purpose(
                parent,
                name,
                version,
                ReadPurpose::RestoreStaging,
            )?;
            if existing
                .as_ref()
                .is_some_and(|(entry, _)| entry.attr.file_type != FileType::File)
            {
                return Err(MetadError::NotFile);
            }
            self.publish_restore_initialization_file(
                operation,
                parent,
                name,
                file,
                existing,
                file_index as u64,
            )?;
        }
        Ok(())
    }

    fn restore_staging_proof(
        &self,
        operation: &RestoreOperation,
        inode: InodeId,
        version: Version,
    ) -> Result<RestoreStagingProof, MetadError> {
        let member_key = restore_staging_member_key(self.mount, operation.ref_set_id, inode);
        let member = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &member_key,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged { root: inode })?;
        let decoded_member = decode_restore_staging_member(&member.value.0)?;
        if decoded_member.operation_digest != operation.operation_digest
            || decoded_member.destination_inode != inode
        {
            return Err(MetadError::RestoreRootChanged { root: inode });
        }
        let inverse_key = restore_staging_inode_key(self.mount, inode);
        let inverse = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &inverse_key,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged { root: inode })?;
        let (digest, ref_set_id) = decode_restore_staging_inverse(&inverse.value.0)?;
        if digest != operation.operation_digest || ref_set_id != operation.ref_set_id {
            return Err(MetadError::RestoreRootChanged { root: inode });
        }
        let inverse_owner_key =
            restore_staging_inverse_owner_key(self.mount, operation.ref_set_id, inode);
        let inverse_owner = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &inverse_owner_key,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged { root: inode })?;
        if inverse_owner.value != inverse.value {
            return Err(MetadError::RestoreRootChanged { root: inode });
        }
        Ok(RestoreStagingProof {
            member: decoded_member,
            member_version: member.version,
            inverse_version: inverse.version,
            inverse_owner_version: inverse_owner.version,
        })
    }

    fn remove_restore_initialization_entry(
        &self,
        operation: &RestoreOperation,
        parent: InodeId,
        name: &DentryName,
        entry: &DentryWithAttr,
        dentry_version: Version,
    ) -> Result<(), MetadError> {
        let read_version = self.read_version()?;
        let staging = self.restore_staging_proof(operation, entry.attr.inode, read_version)?;
        let chunks = if let Some(body) = &entry.body {
            self.chunk_manifests_for_body_at_version(
                entry.attr.inode,
                body,
                read_version,
                ReadPurpose::RestoreStaging,
            )?
        } else {
            Vec::new()
        };
        let xattr_keys = self
            .restore_xattrs_for_copy(entry.attr.inode, read_version, ReadPurpose::RestoreStaging)?
            .into_iter()
            .map(|row| row.key)
            .collect::<Vec<_>>();
        let version = self.next_version()?;
        let command =
            self.build_restore_initialization_remove_command(RestoreInitializationRemoveCommand {
                operation,
                parent,
                name,
                entry,
                dentry_version,
                staging: &staging,
                chunks: &chunks,
                xattr_keys: &xattr_keys,
                version,
            })?;
        validate_restore_command_bounds(&command, "restore initialization remove")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn build_restore_initialization_remove_command(
        &self,
        plan: RestoreInitializationRemoveCommand<'_>,
    ) -> Result<MetadataCommand, MetadError> {
        let RestoreInitializationRemoveCommand {
            operation,
            parent,
            name,
            entry,
            dentry_version,
            staging,
            chunks,
            xattr_keys,
            version,
        } = plan;
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, dentry_key(self.mount, parent, name)),
            delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
            delete_mutation(
                RecordFamily::System,
                restore_staging_member_key(self.mount, operation.ref_set_id, entry.attr.inode),
            ),
            delete_mutation(
                RecordFamily::System,
                restore_staging_inode_key(self.mount, entry.attr.inode),
            ),
            delete_mutation(
                RecordFamily::System,
                restore_staging_inverse_owner_key(
                    self.mount,
                    operation.ref_set_id,
                    entry.attr.inode,
                ),
            ),
        ];
        if let Some(body) = &entry.body {
            mutations.push(delete_mutation(
                RecordFamily::ChunkManifest,
                chunk_manifest_key(
                    self.mount,
                    entry.attr.inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
            ));
            mutations.extend(chunks.iter().map(|chunk| {
                delete_mutation(
                    RecordFamily::ChunkManifest,
                    chunk_manifest_key(
                        self.mount,
                        entry.attr.inode,
                        body.generation,
                        chunk.chunk_index,
                    ),
                )
            }));
        }
        mutations.extend(
            xattr_keys
                .iter()
                .cloned()
                .map(|key| delete_mutation(RecordFamily::Xattr, key)),
        );
        Ok(MetadataCommand {
            request_id: request_id(
                b"restore-init-remove",
                self.mount,
                entry.attr.inode,
                version,
            ),
            kind: CommandKind::RemoveFile,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_key(self.mount, parent, name),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
                },
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: fork_binding_key(self.mount, operation.destination_root),
                    predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
                },
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry_key(self.mount, parent, name),
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, entry.attr.inode),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_member_key(
                        self.mount,
                        operation.ref_set_id,
                        entry.attr.inode,
                    ),
                    predicate: Predicate::VersionEquals(staging.member_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_inode_key(self.mount, entry.attr.inode),
                    predicate: Predicate::VersionEquals(staging.inverse_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_inverse_owner_key(
                        self.mount,
                        operation.ref_set_id,
                        entry.attr.inode,
                    ),
                    predicate: Predicate::VersionEquals(staging.inverse_owner_version),
                },
            ],
            mutations,
            watch: Vec::new(),
        })
    }

    fn publish_restore_initialization_file(
        &self,
        operation: &RestoreOperation,
        parent: InodeId,
        name: &DentryName,
        file: &RestoreInitializationFile,
        existing: Option<(DentryWithAttr, Version)>,
        file_index: u64,
    ) -> Result<(), MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let inode = existing
            .as_ref()
            .map_or_else(|| self.next_inode(), |(entry, _)| Ok(entry.attr.inode))?;
        let object_generation = self.next_version()?;
        let (_intent, intent_version) = self.persist_restore_init_upload_intent(
            operation,
            inode,
            object_generation,
            file,
            object_reference,
        )?;
        let request = PublishArtifact {
            parent,
            name: name.clone(),
            producer: "nokv-restore-initialization".to_owned(),
            digest_uri: body_digest_uri(&file.bytes),
            content_type: file.content_type.clone(),
            manifest_id: format!("restore-init/{}", file.relative_path),
            bytes: file.bytes.clone(),
            mode: file.mode,
            uid: file.uid,
            gid: file.gid,
        };
        let operation_id = restore_barrier_operation_id(operation);
        live_test_barrier::restore_initialization_put(
            &operation_id,
            file_index,
            live_test_barrier::RestoreInitializationPutBoundary::Before,
        )?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, inode, object_generation)?;
        if let Err(error) = live_test_barrier::restore_initialization_put(
            &operation_id,
            file_index,
            live_test_barrier::RestoreInitializationPutBoundary::After,
        ) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(error),
                staged,
            });
        }
        let result = self.commit_restore_initialization_file(
            operation,
            parent,
            name,
            file,
            existing,
            inode,
            object_generation,
            intent_version,
            object_reference,
            body,
            chunks,
        );
        if let Err(error) = result {
            // Preserve both bytes and the durable intent for every error. Even
            // a proven metadata predicate failure does not prove object-store
            // writer quiescence or immediate PUT visibility. Explicit discard
            // first publishes a permanent tombstone ledger, then may delete
            // these operation-local rows; the GC sweeper keeps re-deleting old
            // keys after arbitrarily late PUT completion.
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(error),
                staged,
            });
        }
        Ok(())
    }

    fn persist_restore_init_upload_intent(
        &self,
        operation: &RestoreOperation,
        inode: InodeId,
        generation: Version,
        file: &RestoreInitializationFile,
        object_reference: ObjectReferenceMutation,
    ) -> Result<(RestoreInitUploadIntent, Version), MetadError> {
        let intent = RestoreInitUploadIntent {
            operation_digest: operation.operation_digest,
            ref_set_id: operation.ref_set_id,
            inode,
            generation: generation.get(),
            size: file.bytes.len() as u64,
            relative_path: file.relative_path.clone(),
            cleanup_pass: 0,
        };
        let key = restore_init_upload_intent_key(
            self.mount,
            operation.ref_set_id,
            inode,
            generation.get(),
        );
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"restore-init-upload-intent", self.mount, inode, version),
            kind: CommandKind::PublishArtifact,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![
                object_reference.predicate(self.mount),
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
                },
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: fork_binding_key(self.mount, operation.destination_root),
                    predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_init_upload_intent(&intent)?)),
            }],
            watch: Vec::new(),
        })?;
        Ok((intent, version))
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_restore_initialization_file(
        &self,
        operation: &RestoreOperation,
        parent: InodeId,
        name: &DentryName,
        file: &RestoreInitializationFile,
        existing: Option<(DentryWithAttr, Version)>,
        inode: InodeId,
        object_generation: Version,
        intent_version: Version,
        object_reference: ObjectReferenceMutation,
        body: BodyDescriptor,
        chunks: Vec<ChunkManifest>,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let (staging, old_chunks) = if let Some((existing, _)) = existing.as_ref() {
            let staging =
                self.restore_staging_proof(operation, existing.attr.inode, predecessor(version)?)?;
            let old_chunks = if let Some(old_body) = &existing.body {
                self.chunk_manifests_for_body_at_version(
                    existing.attr.inode,
                    old_body,
                    predecessor(version)?,
                    ReadPurpose::RestoreStaging,
                )?
            } else {
                Vec::new()
            };
            (Some(staging), old_chunks)
        } else {
            (None, Vec::new())
        };
        let command = self.build_restore_initialization_publish_command(
            RestoreInitializationPublishCommand {
                operation,
                parent,
                name,
                file,
                existing: existing.as_ref(),
                inode,
                object_generation,
                intent_version,
                object_reference,
                body: &body,
                chunks: &chunks,
                old_chunks: &old_chunks,
                staging: staging.as_ref(),
                version,
            },
        )?;
        validate_restore_command_bounds(&command, "restore initialization publish")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn build_restore_initialization_publish_command(
        &self,
        plan: RestoreInitializationPublishCommand<'_>,
    ) -> Result<MetadataCommand, MetadError> {
        let RestoreInitializationPublishCommand {
            operation,
            parent,
            name,
            file,
            existing,
            inode,
            object_generation,
            intent_version,
            object_reference,
            body,
            chunks,
            old_chunks,
            staging,
            version,
        } = plan;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: file.mode,
            uid: file.uid,
            gid: file.gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: body.size,
            generation: object_generation.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name.clone(), attr, Some(body.clone()));
        let dentry = dentry_key(self.mount, parent, name);
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: fork_binding_key(self.mount, operation.destination_root),
                predicate: Predicate::VersionEquals(Version::new(operation.created_version)?),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_init_upload_intent_key(
                    self.mount,
                    operation.ref_set_id,
                    inode,
                    object_generation.get(),
                ),
                predicate: Predicate::VersionEquals(intent_version),
            },
        ];
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            },
            put_projection_mutation(RecordFamily::Dentry, dentry.clone(), &projection),
            Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            },
            delete_mutation(
                RecordFamily::System,
                restore_init_upload_intent_key(
                    self.mount,
                    operation.ref_set_id,
                    inode,
                    object_generation.get(),
                ),
            ),
        ];
        mutations.extend(chunks.iter().map(|chunk| Mutation {
            family: RecordFamily::ChunkManifest,
            key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
            op: MutationOp::Put,
            value: Some(Value(encode_chunk_manifest(chunk))),
        }));
        let kind = if let Some((existing, dentry_version)) = existing {
            let staging = staging.ok_or_else(|| {
                MetadError::Codec(
                    "restore initialization replace command is missing staging proof".to_owned(),
                )
            })?;
            if staging.member.relative_path != file.relative_path {
                return Err(MetadError::RestoreRootChanged { root: inode });
            }
            let member_key = restore_staging_member_key(self.mount, operation.ref_set_id, inode);
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry.clone(),
                    predicate: Predicate::VersionEquals(*dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, inode),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: member_key.clone(),
                    predicate: Predicate::VersionEquals(staging.member_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_inode_key(self.mount, inode),
                    predicate: Predicate::VersionEquals(staging.inverse_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_inverse_owner_key(self.mount, operation.ref_set_id, inode),
                    predicate: Predicate::VersionEquals(staging.inverse_owner_version),
                },
            ]);
            mutations.push(Mutation {
                family: RecordFamily::System,
                key: member_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_staging_member(
                    &RestoreStagingMember {
                        operation_digest: operation.operation_digest,
                        source_inode: None,
                        destination_inode: inode,
                        destination_parent: Some(parent),
                        name: Some(name.clone()),
                        relative_path: file.relative_path.clone(),
                        canonical_index_cursor: Vec::new(),
                        canonical_index_complete: true,
                        manifest_cursor: Vec::new(),
                        manifest_block_cursor: 0,
                    },
                )?)),
            });
            if let Some(old_body) = &existing.body {
                mutations.push(delete_mutation(
                    RecordFamily::ChunkManifest,
                    chunk_manifest_key(
                        self.mount,
                        inode,
                        old_body.generation,
                        BODY_SUMMARY_CHUNK_INDEX,
                    ),
                ));
                mutations.extend(old_chunks.iter().map(|chunk| {
                    delete_mutation(
                        RecordFamily::ChunkManifest,
                        chunk_manifest_key(
                            self.mount,
                            inode,
                            old_body.generation,
                            chunk.chunk_index,
                        ),
                    )
                }));
            }
            CommandKind::ReplaceArtifact
        } else {
            let member = restore_staging_member_key(self.mount, operation.ref_set_id, inode);
            let inverse = restore_staging_inode_key(self.mount, inode);
            let inverse_owner =
                restore_staging_inverse_owner_key(self.mount, operation.ref_set_id, inode);
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, inode),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: member.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_owner.clone(),
                    predicate: Predicate::NotExists,
                },
            ]);
            mutations.extend([
                Mutation {
                    family: RecordFamily::System,
                    key: member,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_member(
                        &RestoreStagingMember {
                            operation_digest: operation.operation_digest,
                            source_inode: None,
                            destination_inode: inode,
                            destination_parent: Some(parent),
                            name: Some(name.clone()),
                            relative_path: file.relative_path.clone(),
                            canonical_index_cursor: Vec::new(),
                            canonical_index_complete: true,
                            manifest_cursor: Vec::new(),
                            manifest_block_cursor: 0,
                        },
                    )?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: inverse,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_inverse(operation))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: inverse_owner,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_inverse(operation))),
                },
            ]);
            CommandKind::PublishArtifact
        };
        Ok(MetadataCommand {
            request_id: request_id(b"restore-init-publish", self.mount, inode, version),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry,
            predicates,
            mutations,
            watch: Vec::new(),
        })
    }

    fn discard_preparing_restore(&self, operation: &RestoreOperation) -> Result<(), MetadError> {
        let operation_version = self.begin_restore_discard(operation)?;
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(operation),
            live_test_barrier::RestoreAppliedPhase::CleanupBatch(0),
        )?;
        let mut discarding = operation.clone();
        discarding.state = RestoreOperationState::Discarding;
        if !self.cleanup_restore_init_upload_intents(&discarding, operation_version)? {
            return Err(MetadError::RestoreInProgress);
        }
        if !self.cleanup_restore_staging_members(&discarding, operation_version)? {
            return Err(MetadError::RestoreInProgress);
        }
        if !self.cleanup_restore_base_references(&discarding, operation_version)? {
            return Err(MetadError::RestoreInProgress);
        }
        if !self.cleanup_restore_index_page(&discarding, operation_version)? {
            return Err(MetadError::RestoreInProgress);
        }
        self.finish_restore_discard(&discarding, operation_version)?;
        self.recover_restore_staging_visibility()?;
        Ok(())
    }

    fn begin_restore_discard(&self, operation: &RestoreOperation) -> Result<Version, MetadError> {
        let key = restore_operation_key(self.mount, &operation.operation_digest);
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            })?;
        let current = decode_restore_operation(&item.value.0)?;
        if current.operation_digest != operation.operation_digest
            || current.initialization_digest != operation.initialization_digest
        {
            return Err(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            });
        }
        if matches!(
            current.state,
            RestoreOperationState::Cleaning | RestoreOperationState::Discarding
        ) {
            let cleanup = self
                .metadata
                .get(
                    RecordFamily::System,
                    &restore_cleanup_job_key(self.mount, operation.ref_set_id),
                    self.read_version()?,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or_else(|| {
                    MetadError::Codec("discarding restore has no cleanup job".to_owned())
                })?;
            let cleanup = decode_restore_cleanup_job(&cleanup.0)?;
            if cleanup.operation_digest != operation.operation_digest
                || cleanup.ref_set_id != operation.ref_set_id
            {
                return Err(MetadError::Codec(
                    "restore cleanup job changed identity".to_owned(),
                ));
            }
            return Ok(item.version);
        }
        if current.state != RestoreOperationState::Preparing {
            return Err(MetadError::RestoreInProgress);
        }
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let binding = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            })?;
        let decoded = crate::layout::decode_fork_binding(&binding.value.0).map_err(|_| {
            MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            }
        })?;
        if decoded.fork_root != operation.destination_root
            || decoded.source_root != operation.source_root
            || decoded.snapshot_id != operation.snapshot_id
            || decoded.pinned_read_version != operation.read_version
        {
            return Err(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            });
        }
        let mut discarding = current;
        discarding.state = RestoreOperationState::Discarding;
        let cleanup_key = restore_cleanup_job_key(self.mount, operation.ref_set_id);
        let cleanup = RestoreCleanupJob {
            operation_digest: operation.operation_digest,
            ref_set_id: operation.ref_set_id,
            index_complete: false,
            index_cursor: Vec::new(),
        };
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"restore-begin-discard",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::VersionEquals(item.version),
                },
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: binding_key,
                    predicate: Predicate::VersionEquals(binding.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: cleanup_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![
                Mutation {
                    family: RecordFamily::System,
                    key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_operation(&discarding)?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: cleanup_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_cleanup_job(&cleanup)?)),
                },
            ],
            watch: Vec::new(),
        })?;
        Ok(version)
    }

    fn cleanup_restore_init_upload_intents(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
    ) -> Result<bool, MetadError> {
        let prefix = restore_init_upload_intent_prefix(self.mount, operation.ref_set_id);
        let read_version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: prefix.clone(),
            start_after: None,
            version: read_version,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            return Ok(true);
        }
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: restore_operation_key(self.mount, &operation.operation_digest),
            predicate: Predicate::VersionEquals(operation_version),
        }];
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        for row in rows {
            let mut intent = decode_restore_init_upload_intent(&row.value.0)?;
            if intent.operation_digest != operation.operation_digest
                || intent.ref_set_id != operation.ref_set_id
                || restore_init_upload_intent_key(
                    self.mount,
                    intent.ref_set_id,
                    intent.inode,
                    intent.generation,
                ) != row.key
            {
                return Err(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                });
            }
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            let tombstone = RestoreInitUploadTombstone {
                operation_digest: intent.operation_digest,
                initialization_digest: operation.initialization_digest,
                inode: intent.inode,
                generation: intent.generation,
                size: intent.size,
                relative_path: intent.relative_path.clone(),
            };
            let tombstone_key = restore_init_upload_tombstone_key(
                self.mount,
                &tombstone.operation_digest,
                tombstone.inode,
                tombstone.generation,
            );
            let existing_tombstone = self.metadata.get_versioned(
                RecordFamily::System,
                &tombstone_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?;
            match intent.cleanup_pass {
                0 => {
                    match existing_tombstone {
                        Some(existing) => {
                            if decode_restore_init_upload_tombstone(&existing.value.0)? != tombstone
                            {
                                return Err(MetadError::Codec(
                                    "restore init upload tombstone changed identity".to_owned(),
                                ));
                            }
                            predicates.push(PredicateRef {
                                family: RecordFamily::System,
                                key: tombstone_key,
                                predicate: Predicate::VersionEquals(existing.version),
                            });
                        }
                        None => {
                            predicates.push(PredicateRef {
                                family: RecordFamily::System,
                                key: tombstone_key.clone(),
                                predicate: Predicate::NotExists,
                            });
                            mutations.push(Mutation {
                                family: RecordFamily::System,
                                key: tombstone_key,
                                op: MutationOp::Put,
                                value: Some(Value(encode_restore_init_upload_tombstone(
                                    &tombstone,
                                )?)),
                            });
                        }
                    }
                    // The first cleanup pass only publishes the permanent
                    // tombstone. No object DELETE may happen before that CAS:
                    // an older owner can still be blocked inside PUT.
                    intent.cleanup_pass = 1;
                    mutations.push(Mutation {
                        family: RecordFamily::System,
                        key: row.key.clone(),
                        op: MutationOp::Put,
                        value: Some(Value(encode_restore_init_upload_intent(&intent)?)),
                    });
                }
                1 => {
                    let existing = existing_tombstone.ok_or_else(|| {
                        MetadError::Codec(
                            "restore init intent is tombstoned without a global ledger".to_owned(),
                        )
                    })?;
                    if decode_restore_init_upload_tombstone(&existing.value.0)? != tombstone {
                        return Err(MetadError::Codec(
                            "restore init upload tombstone changed identity".to_owned(),
                        ));
                    }
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: tombstone_key,
                        predicate: Predicate::VersionEquals(existing.version),
                    });
                    self.delete_restore_init_object_range(
                        tombstone.inode,
                        tombstone.generation,
                        tombstone.size,
                    )?;
                    mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
                }
                _ => unreachable!("restore init intent codec validates cleanup pass"),
            }
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-discard-init-intents",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: prefix,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore discard init intents")?;
        self.commit_metadata(command)?;
        Ok(false)
    }

    pub(super) fn delete_restore_init_object_range(
        &self,
        inode: InodeId,
        generation: u64,
        size: u64,
    ) -> Result<bool, MetadError> {
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let block_size = DEFAULT_BLOCK_SIZE as u64;
        let mut chunk_offset = 0_u64;
        let mut deleted_any = false;
        while chunk_offset < size {
            let chunk_index = chunk_offset / chunk_size;
            let chunk_len = chunk_size.min(size - chunk_offset);
            let block_count = chunk_len.div_ceil(block_size);
            for block_index in 0..block_count {
                let key = ObjectKey::new(format!(
                    "blocks/{}/{}/{}/{}/{}",
                    self.mount.get(),
                    inode.get(),
                    generation,
                    chunk_index,
                    block_index
                ))?;
                deleted_any |= self.objects.delete(&key)?;
            }
            chunk_offset = chunk_offset.saturating_add(chunk_len);
        }
        Ok(deleted_any)
    }

    pub(super) fn restore_init_object_range_absent(
        &self,
        inode: InodeId,
        generation: u64,
        size: u64,
    ) -> Result<bool, MetadError> {
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let block_size = DEFAULT_BLOCK_SIZE as u64;
        let mut chunk_offset = 0_u64;
        while chunk_offset < size {
            let chunk_index = chunk_offset / chunk_size;
            let chunk_len = chunk_size.min(size - chunk_offset);
            let block_count = chunk_len.div_ceil(block_size);
            for block_index in 0..block_count {
                let key = ObjectKey::new(format!(
                    "blocks/{}/{}/{}/{}/{}",
                    self.mount.get(),
                    inode.get(),
                    generation,
                    chunk_index,
                    block_index
                ))?;
                if self.objects.head(&key)?.is_some() {
                    return Ok(false);
                }
            }
            chunk_offset = chunk_offset.saturating_add(chunk_len);
        }
        Ok(true)
    }

    /// Re-delete object keys tracked by permanent initialization tombstones.
    /// This automatic sweep deliberately never removes ledger rows: only an
    /// explicit global-writer drain can prove that no old owner remains blocked
    /// inside PUT and safely CAS the tombstone away.
    pub(super) fn cleanup_restore_init_upload_tombstones_locked(
        &self,
        limit: usize,
    ) -> Result<usize, MetadError> {
        let version = self.read_version()?;
        let prefix = restore_init_upload_tombstone_prefix(self.mount);
        let cursor_key = restore_init_upload_tombstone_cursor_key(self.mount);
        let cursor_item = self.metadata.get_versioned(
            RecordFamily::System,
            &cursor_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        let cursor = cursor_item
            .as_ref()
            .map_or_else(Vec::new, |item| item.value.0.clone());
        if cursor.len() > MAX_RESTORE_PATH_BYTES
            || (!cursor.is_empty() && !cursor.starts_with(&prefix))
        {
            return Err(MetadError::Codec(
                "restore init tombstone cursor changed identity".to_owned(),
            ));
        }
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: prefix.clone(),
            start_after: (!cursor.is_empty()).then_some(cursor.clone()),
            version,
            limit: limit.max(1),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        for row in &rows {
            let tombstone = match validate_restore_init_upload_tombstone_row(self.mount, row) {
                Ok(tombstone) => tombstone,
                Err(error) => {
                    self.quarantine_restore_release_job(row, &error.to_string())?;
                    continue;
                }
            };
            if let Err(error) = self.delete_restore_init_object_range(
                tombstone.inode,
                tombstone.generation,
                tombstone.size,
            ) {
                self.quarantine_restore_release_job(row, &error.to_string())?;
            }
        }
        let next_cursor = rows.last().map_or_else(Vec::new, |row| row.key.clone());
        if !rows.is_empty() || !cursor.is_empty() {
            self.update_restore_round_robin_cursor(
                cursor_key,
                cursor_item,
                next_cursor,
                &prefix,
                b"restore-init-tombstone-cursor",
            )?;
        }
        Ok(rows.len())
    }

    fn update_restore_round_robin_cursor(
        &self,
        cursor_key: Vec<u8>,
        cursor_item: Option<ReadItem>,
        next_cursor: Vec<u8>,
        item_prefix: &[u8],
        request_prefix: &[u8],
    ) -> Result<(), MetadError> {
        if next_cursor.len() > MAX_RESTORE_PATH_BYTES
            || (!next_cursor.is_empty() && !next_cursor.starts_with(item_prefix))
        {
            return Err(MetadError::Codec(
                "restore round-robin cursor escaped its keyspace".to_owned(),
            ));
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(request_prefix, self.mount, InodeId::root(), version),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates: vec![
                object_reference.predicate(self.mount),
                PredicateRef {
                    family: RecordFamily::System,
                    key: cursor_key.clone(),
                    predicate: cursor_item
                        .as_ref()
                        .map(|item| Predicate::VersionEquals(item.version))
                        .unwrap_or(Predicate::NotExists),
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cursor_key,
                op: MutationOp::Put,
                value: Some(Value(next_cursor)),
            }],
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore round-robin cursor")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn update_restore_release_worker_cursor(
        &self,
        cursor_key: Vec<u8>,
        cursor_item: Option<ReadItem>,
        next_cursor: &RestoreReleaseWorkerCursor,
    ) -> Result<(), MetadError> {
        let encoded = encode_restore_release_worker_cursor(self.mount, next_cursor)?;
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-worker-cursor",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates: vec![
                object_reference.predicate(self.mount),
                PredicateRef {
                    family: RecordFamily::System,
                    key: cursor_key.clone(),
                    predicate: cursor_item
                        .as_ref()
                        .map(|item| Predicate::VersionEquals(item.version))
                        .unwrap_or(Predicate::NotExists),
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cursor_key,
                op: MutationOp::Put,
                value: Some(Value(encoded)),
            }],
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release worker cursor")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn cleanup_restore_staging_members(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
    ) -> Result<bool, MetadError> {
        let prefix = restore_staging_member_prefix(self.mount, operation.ref_set_id);
        let Some(row) = self
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix,
                start_after: None,
                version: self.read_version()?,
                limit: 1,
                purpose: ReadPurpose::RestoreStaging,
            })?
            .into_iter()
            .next()
        else {
            return Ok(true);
        };
        let member = decode_restore_staging_member(&row.value.0)?;
        if member.operation_digest != operation.operation_digest
            || restore_staging_member_key(
                self.mount,
                operation.ref_set_id,
                member.destination_inode,
            ) != row.key
        {
            return Err(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            });
        }
        self.cleanup_restore_staging_member(operation, operation_version, member, row.version)?;
        Ok(false)
    }

    fn cleanup_restore_staging_member(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        member: RestoreStagingMember,
        member_version: Version,
    ) -> Result<bool, MetadError> {
        let read_version = self.read_version()?;
        if member.source_inode.is_none() {
            if let (Some(parent), Some(name)) = (member.destination_parent, member.name.as_ref()) {
                if let Some((entry, _)) = self.lookup_plus_at_version_for_purpose(
                    parent,
                    name,
                    read_version,
                    ReadPurpose::RestoreStaging,
                )? {
                    if let Some(body) = entry.body {
                        let chunks = self.chunk_manifests_for_body_at_version(
                            member.destination_inode,
                            &body,
                            read_version,
                            ReadPurpose::RestoreStaging,
                        )?;
                        for block in chunks
                            .iter()
                            .flat_map(|chunk| chunk.slices.iter())
                            .flat_map(|slice| slice.blocks.iter())
                        {
                            if !self.owns_block_object_key(
                                member.destination_inode,
                                body.generation,
                                &block.object_key,
                            ) {
                                return Err(MetadError::RestoreRootChanged {
                                    root: member.destination_inode,
                                });
                            }
                            self.objects
                                .delete(&ObjectKey::new(block.object_key.clone())?)?;
                        }
                    }
                }
            }
        }
        if !self.delete_restore_metadata_prefix(
            operation,
            operation_version,
            RecordFamily::ChunkManifest,
            inode_key(self.mount, member.destination_inode),
        )? {
            return Ok(false);
        }
        if !self.delete_restore_metadata_prefix(
            operation,
            operation_version,
            RecordFamily::Xattr,
            xattr_prefix(self.mount, member.destination_inode),
        )? {
            return Ok(false);
        }
        let inverse_key = restore_staging_inode_key(self.mount, member.destination_inode);
        let inverse = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &inverse_key,
                self.read_version()?,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            })?;
        let (digest, ref_set_id) = decode_restore_staging_inverse(&inverse.value.0)?;
        if digest != operation.operation_digest || ref_set_id != operation.ref_set_id {
            return Err(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            });
        }
        let inverse_owner_key = restore_staging_inverse_owner_key(
            self.mount,
            operation.ref_set_id,
            member.destination_inode,
        );
        let inverse_owner = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &inverse_owner_key,
                self.read_version()?,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            })?;
        if inverse_owner.value != inverse.value {
            return Err(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            });
        }
        let member_key =
            restore_staging_member_key(self.mount, operation.ref_set_id, member.destination_inode);
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: member_key.clone(),
                predicate: Predicate::VersionEquals(member_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_key.clone(),
                predicate: Predicate::VersionEquals(inverse.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_owner_key.clone(),
                predicate: Predicate::VersionEquals(inverse_owner.version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, member.destination_inode),
                predicate: Predicate::Exists,
            },
        ];
        let mut mutations = vec![
            delete_mutation(
                RecordFamily::Inode,
                inode_key(self.mount, member.destination_inode),
            ),
            delete_mutation(RecordFamily::System, member_key),
            delete_mutation(RecordFamily::System, inverse_key),
            delete_mutation(RecordFamily::System, inverse_owner_key),
        ];
        if let (Some(parent), Some(name)) = (member.destination_parent, member.name) {
            let dentry_key = dentry_key(self.mount, parent, &name);
            let dentry = self
                .metadata
                .get_versioned(
                    RecordFamily::Dentry,
                    &dentry_key,
                    self.read_version()?,
                    ReadPurpose::RestoreStaging,
                )?
                .ok_or(MetadError::RestoreRootChanged {
                    root: member.destination_inode,
                })?;
            let projection =
                crate::layout::decode_dentry_projection(&dentry.value.0).map_err(|_| {
                    MetadError::RestoreRootChanged {
                        root: member.destination_inode,
                    }
                })?;
            if projection.attr.inode != member.destination_inode {
                return Err(MetadError::RestoreRootChanged {
                    root: member.destination_inode,
                });
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_key.clone(),
                predicate: Predicate::VersionEquals(dentry.version),
            });
            mutations.push(delete_mutation(RecordFamily::Dentry, dentry_key));
        }
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"restore-discard-member",
                self.mount,
                member.destination_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: restore_staging_member_prefix(self.mount, operation.ref_set_id),
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(true)
    }

    fn delete_restore_metadata_prefix(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        family: RecordFamily,
        prefix: Vec<u8>,
    ) -> Result<bool, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family,
            prefix: prefix.clone(),
            start_after: None,
            version: self.read_version()?,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::RestoreStaging,
        })?;
        if rows.is_empty() {
            return Ok(true);
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-discard-metadata",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: family,
            primary_key: prefix,
            predicates: std::iter::once(PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            })
            .chain(rows.iter().map(|row| PredicateRef {
                family,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            }))
            .collect(),
            mutations: rows
                .into_iter()
                .map(|row| delete_mutation(family, row.key))
                .collect(),
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore discard metadata")?;
        self.commit_metadata(command)?;
        Ok(false)
    }

    fn cleanup_restore_base_references(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
    ) -> Result<bool, MetadError> {
        // Reference-page creation validates existing inverse/owner pairs under
        // this same gate. Serialize bounded Preparing cleanup with that read so
        // a legitimate discard cannot be mistaken for durable corruption.
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let prefix = restore_base_owner_prefix(self.mount, operation.ref_set_id);
        let read_version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: prefix.clone(),
            start_after: None,
            version: read_version,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            return Ok(true);
        }
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: restore_operation_key(self.mount, &operation.operation_digest),
            predicate: Predicate::VersionEquals(operation_version),
        }];
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        for row in &rows {
            let reference = super::restore_gc::decode_restore_base_reference(&row.value.0)?;
            if reference.operation_digest != operation.operation_digest
                || reference.ref_set_id != operation.ref_set_id
            {
                return Err(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                });
            }
            let object_digest: [u8; 32] = Sha256::digest(reference.object_key.as_bytes()).into();
            let expected_owner = restore_base_owner_key(
                self.mount,
                reference.ref_set_id,
                &object_digest,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            if row.key != expected_owner {
                return Err(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                });
            }
            let inverse_key = restore_base_inverse_key(
                self.mount,
                &object_digest,
                reference.ref_set_id,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            let inverse = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &inverse_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                })?;
            let decoded_inverse = super::restore_gc::decode_restore_base_inverse(&inverse.value.0)?;
            if decoded_inverse.operation_digest != operation.operation_digest
                || decoded_inverse.ref_set_id != reference.ref_set_id
                || decoded_inverse.object_digest != object_digest
                || decoded_inverse.borrower_inode != reference.borrower_inode
                || decoded_inverse.borrower_generation != reference.borrower_generation
            {
                return Err(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                });
            }
            let inverse_owner_key = restore_base_inverse_owner_key(
                self.mount,
                reference.ref_set_id,
                &object_digest,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            let inverse_owner = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &inverse_owner_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                })?;
            if inverse_owner.value != inverse.value {
                return Err(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                });
            }
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::System,
                    key: expected_owner.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::VersionEquals(inverse.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_owner_key.clone(),
                    predicate: Predicate::VersionEquals(inverse_owner.version),
                },
            ]);
            mutations.extend([
                delete_mutation(RecordFamily::System, expected_owner),
                delete_mutation(RecordFamily::System, inverse_key),
                delete_mutation(RecordFamily::System, inverse_owner_key),
            ]);
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-discard-base-refs",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: prefix,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore discard base refs")?;
        self.commit_metadata(command)?;
        Ok(false)
    }

    /// Advance exactly one durable private-index cleanup page. The index helper
    /// may commit its own bounded delete batch first; persisting the opaque
    /// cursor in a second CAS is ACK-loss safe because replaying an older cursor
    /// only rescans already-absent keys.
    fn cleanup_restore_index_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
    ) -> Result<bool, MetadError> {
        let cleanup_key = restore_cleanup_job_key(self.mount, operation.ref_set_id);
        let cleanup_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &cleanup_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore cleanup job is missing".to_owned()))?;
        let mut cleanup = decode_restore_cleanup_job(&cleanup_item.value.0)?;
        if cleanup.operation_digest != operation.operation_digest
            || cleanup.ref_set_id != operation.ref_set_id
        {
            return Err(MetadError::Codec(
                "restore cleanup job changed identity".to_owned(),
            ));
        }
        if cleanup.index_complete {
            return Ok(true);
        }
        let outcome = self.release_restore_index_page(
            operation,
            operation_version,
            &cleanup.index_cursor,
            RESTORE_BATCH_ENTRIES,
        )?;
        cleanup.index_complete = outcome.complete;
        cleanup.index_cursor = if outcome.complete {
            Vec::new()
        } else {
            outcome.cursor
        };
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-discard-index-cursor",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cleanup_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: cleanup_key.clone(),
                    predicate: Predicate::VersionEquals(cleanup_item.version),
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cleanup_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_cleanup_job(&cleanup)?)),
            }],
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore discard index cursor")?;
        self.commit_metadata(command)?;
        Ok(cleanup.index_complete)
    }

    fn finish_restore_discard(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
    ) -> Result<(), MetadError> {
        let read_version = self.read_version()?;
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let binding = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            })?;
        let decoded_binding =
            crate::layout::decode_fork_binding(&binding.value.0).map_err(|_| {
                MetadError::RestoreBindingChanged {
                    root: operation.destination_root,
                }
            })?;
        if decoded_binding.fork_root != operation.destination_root
            || decoded_binding.source_root != operation.source_root
            || decoded_binding.pinned_read_version != operation.read_version
            || decoded_binding.snapshot_id != operation.snapshot_id
            || decoded_binding.created_version != operation.created_version
        {
            return Err(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            });
        }
        let claim_key = restore_destination_claim_key(self.mount, &operation.destination_path);
        let claim = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            })?;
        let root_key = restore_root_index_key(self.mount, operation.destination_root);
        let root = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &root_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            })?;
        if claim.value.0 != operation.operation_digest || root.value.0 != operation.operation_digest
        {
            return Err(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            });
        }
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let seal_key = restore_base_seal_key(self.mount, operation.ref_set_id);
        let cleanup_key = restore_cleanup_job_key(self.mount, operation.ref_set_id);
        let cleanup = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &cleanup_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore cleanup job is missing".to_owned()))?;
        let cleanup_job = decode_restore_cleanup_job(&cleanup.value.0)?;
        if cleanup_job.operation_digest != operation.operation_digest
            || cleanup_job.ref_set_id != operation.ref_set_id
            || !cleanup_job.index_complete
        {
            return Err(MetadError::Codec(
                "restore cleanup job is not complete".to_owned(),
            ));
        }
        let seal = self.metadata.get_versioned(
            RecordFamily::System,
            &seal_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        if let Some(item) = &seal {
            let identity_matches =
                match super::restore_gc::decode_restore_base_seal_record(&item.value.0)? {
                    super::restore_gc::RestoreBaseSealRecord::Building(build) => {
                        build.operation_digest == operation.operation_digest
                            && build.initialization_digest == operation.initialization_digest
                            && build.ref_set_id == operation.ref_set_id
                            && build.incarnation == operation.created_version
                    }
                    super::restore_gc::RestoreBaseSealRecord::Sealed(seal) => {
                        seal.operation_digest == operation.operation_digest
                            && seal.initialization_digest == operation.initialization_digest
                            && seal.ref_set_id == operation.ref_set_id
                            && seal.incarnation == operation.created_version
                    }
                };
            if !identity_matches {
                return Err(MetadError::Codec(
                    "restore discard base seal/progress changed identity".to_owned(),
                ));
            }
        }
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.clone(),
                predicate: Predicate::VersionEquals(binding.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: claim_key.clone(),
                predicate: Predicate::VersionEquals(claim.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: root_key.clone(),
                predicate: Predicate::VersionEquals(root.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: cleanup_key.clone(),
                predicate: Predicate::VersionEquals(cleanup.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_inverse_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_base_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_base_inverse_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_init_upload_intent_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: seal_key.clone(),
                predicate: seal
                    .as_ref()
                    .map(|item| Predicate::VersionEquals(item.version))
                    .unwrap_or(Predicate::NotExists),
            },
        ];
        predicates.extend(
            super::restore_index::restore_index_release_empty_predicates(
                self.mount,
                operation.ref_set_id,
            ),
        );
        let mut mutations = vec![
            delete_mutation(RecordFamily::System, operation_key.clone()),
            delete_mutation(RecordFamily::ForkBinding, binding_key),
            delete_mutation(RecordFamily::System, claim_key),
            delete_mutation(RecordFamily::System, root_key),
            delete_mutation(RecordFamily::System, cleanup_key),
        ];
        if seal.is_some() {
            mutations.push(delete_mutation(RecordFamily::System, seal_key));
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-finish-discard",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: operation_key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore finish discard")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    pub fn cleanup_restore_releases(&self, limit: usize) -> Result<usize, MetadError> {
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.cleanup_restore_releases_locked(limit)
    }

    pub(super) fn cleanup_restore_releases_locked(
        &self,
        limit: usize,
    ) -> Result<usize, MetadError> {
        let version = self.read_version()?;
        let prefix = restore_release_job_prefix(self.mount);
        let cursor_key = restore_release_cursor_key(self.mount);
        let cursor_item = self.metadata.get_versioned(
            RecordFamily::System,
            &cursor_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        let persisted_cursor = cursor_item
            .as_ref()
            .map(|item| {
                decode_restore_release_worker_cursor_at_version(self.mount, &item.value.0, version)
            })
            .transpose()?;
        let mut active_cursor =
            persisted_cursor
                .clone()
                .unwrap_or_else(|| RestoreReleaseWorkerCursor {
                    cycle_high_water: version.get(),
                    start_after: Vec::new(),
                });
        let effective_limit = limit.max(1);
        let scan_cycle = |cursor: &RestoreReleaseWorkerCursor| {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: prefix.clone(),
                start_after: (!cursor.start_after.is_empty()).then_some(cursor.start_after.clone()),
                version,
                limit: effective_limit,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            let reached_physical_tail = rows.len() < effective_limit;
            let mut visited = Vec::with_capacity(rows.len());
            let mut reached_cycle_boundary = false;
            for row in rows {
                if row.version.get() > cursor.cycle_high_water {
                    // Both newly-created operations and old Complete restores
                    // released late are outside this frozen cycle. Stop at the
                    // first newer row version instead of chasing a key tail
                    // that can grow forever.
                    reached_cycle_boundary = true;
                    break;
                }
                visited.push(row);
            }
            Ok::<_, MetadError>((visited, reached_physical_tail, reached_cycle_boundary))
        };

        let (mut visited, mut reached_physical_tail, mut reached_version_boundary) =
            scan_cycle(&active_cursor)?;
        let rolled_cursor = RestoreReleaseWorkerCursor {
            cycle_high_water: version.get(),
            start_after: Vec::new(),
        };
        // If the prior page ended exactly at the high-water boundary, start
        // the next cycle in this same bounded worker call. The first scan did
        // not visit a row, so at most `effective_limit` jobs are processed.
        if visited.is_empty()
            && (reached_physical_tail || reached_version_boundary)
            && active_cursor != rolled_cursor
        {
            active_cursor = rolled_cursor;
            (visited, reached_physical_tail, reached_version_boundary) =
                scan_cycle(&active_cursor)?;
        }
        if visited.is_empty() {
            // Keep an idle cursor unchanged. Advancing only its high-water on
            // every background GC tick would create an unbounded metadata-log
            // write loop and continually invalidate stable-epoch fsck scans.
            return Ok(0);
        }

        let mut processed = 0_usize;
        for row in &visited {
            let Some(ref_set_id) = restore_release_job_ref_set_id(self.mount, &row.key) else {
                self.quarantine_restore_invalid_release_job_key(
                    row,
                    "restore release job key has an invalid identity",
                )?;
                processed = processed.saturating_add(1);
                continue;
            };
            let job = match (|| {
                if ref_set_id > row.version.get() {
                    return Err(MetadError::Codec(
                        "restore release job identity is newer than its row".to_owned(),
                    ));
                }
                decode_restore_release_job(&row.value.0)
            })()
            .and_then(|job| {
                if restore_release_job_key(self.mount, job.ref_set_id) != row.key {
                    return Err(MetadError::Codec(
                        "restore release job key does not match its identity".to_owned(),
                    ));
                }
                Ok(job)
            }) {
                Ok(job) => job,
                Err(error) => {
                    self.quarantine_restore_release_job(row, &error.to_string())?;
                    processed = processed.saturating_add(1);
                    continue;
                }
            };
            if let Err(error) = self.process_restore_release_job(job, row.version) {
                if restore_release_error_is_retryable(&error) {
                    // A concurrent worker, lost acknowledgement, owner fence,
                    // or recovery-log outage is resolved by a later durable
                    // scan. Never quarantine a healthy job for an external or
                    // retryable control-plane fault.
                    processed = processed.saturating_add(1);
                    continue;
                }
                // Keep the active job for fsck and a later retry, but record
                // the failure and advance the mount-global cursor. One damaged
                // or repeatedly failing ref-set must not starve unrelated
                // release jobs behind it.
                self.quarantine_restore_release_job(row, &error.to_string())?;
                processed = processed.saturating_add(1);
                continue;
            }
            processed = processed.saturating_add(1);
        }

        let next_cursor = if reached_version_boundary {
            RestoreReleaseWorkerCursor {
                // A newer row ended this frozen cycle. Raise the frontier to
                // the scan snapshot, even when older blocked jobs preceded the
                // boundary, so that mixed queues cannot pin it forever.
                cycle_high_water: version.get(),
                start_after: Vec::new(),
            }
        } else if reached_physical_tail {
            RestoreReleaseWorkerCursor {
                // Reset the key position but keep the frontier frozen. If a
                // job actually changed, its newer row version becomes the
                // next call's explicit boundary and raises the frontier then.
                // A snapshot-blocked job therefore produces no idle writes.
                cycle_high_water: active_cursor.cycle_high_water,
                start_after: Vec::new(),
            }
        } else {
            RestoreReleaseWorkerCursor {
                cycle_high_water: active_cursor.cycle_high_water,
                start_after: visited
                    .iter()
                    .rev()
                    .find(|row| restore_release_job_ref_set_id(self.mount, &row.key).is_some())
                    .map_or_else(|| active_cursor.start_after.clone(), |row| row.key.clone()),
            }
        };
        let cursor_changed = persisted_cursor.as_ref() != Some(&next_cursor);
        if cursor_changed {
            self.update_restore_release_worker_cursor(cursor_key, cursor_item, &next_cursor)?;
        }
        Ok(processed)
    }

    pub(super) fn restore_release_backlog(&self) -> Result<(usize, usize, usize), MetadError> {
        let version = self.read_version()?;
        Ok((
            self.count_restore_control_rows(restore_release_job_prefix(self.mount), version)?,
            self.count_restore_control_rows(
                restore_release_quarantine_prefix(self.mount),
                version,
            )?,
            self.count_restore_control_rows(
                restore_release_mount_wide_quarantine_prefix(self.mount),
                version,
            )?,
        ))
    }

    fn count_restore_control_rows(
        &self,
        prefix: Vec<u8>,
        version: Version,
    ) -> Result<usize, MetadError> {
        let mut count = 0_usize;
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: RESTORE_BACKLOG_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if rows.is_empty() {
                return Ok(count);
            }
            count = count.saturating_add(rows.len());
            start_after = rows.last().map(|row| row.key.clone());
            if rows.len() < RESTORE_BACKLOG_PAGE_ROWS {
                return Ok(count);
            }
        }
    }

    fn quarantine_restore_release_job(
        &self,
        row: &crate::command::ScanItem,
        reason: &str,
    ) -> Result<(), MetadError> {
        self.quarantine_restore_release_job_with_disposition(row, reason, false)
    }

    fn quarantine_restore_invalid_release_job_key(
        &self,
        row: &crate::command::ScanItem,
        reason: &str,
    ) -> Result<(), MetadError> {
        self.quarantine_restore_release_job_with_disposition(row, reason, true)
    }

    fn quarantine_restore_release_job_with_disposition(
        &self,
        row: &crate::command::ScanItem,
        reason: &str,
        remove_original: bool,
    ) -> Result<(), MetadError> {
        let scope = RestoreReleaseQuarantineScope::Diagnostic;
        let quarantine_key =
            restore_release_quarantine_key(self.mount, RecordFamily::System, row, scope);
        let version = self.next_version()?;
        let mut mutations = vec![Mutation {
            family: RecordFamily::System,
            key: quarantine_key.clone(),
            op: MutationOp::Put,
            value: Some(Value(encode_restore_release_quarantine(
                RecordFamily::System,
                row,
                reason,
                scope,
            )?)),
        }];
        if remove_original {
            // A non-canonical physical key cannot own a release operation.
            // Move its exact bytes into quarantine atomically so the damaged
            // row remains diagnosable without blocking the bounded scanner.
            mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-quarantine-release-job",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: row.key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: quarantine_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release job quarantine")?;
        match self.commit_metadata(command) {
            Ok(_)
            | Err(MetadError::Metadata(MetadataError::PredicateFailed))
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn process_restore_release_job(
        &self,
        mut job: RestoreReleaseJob,
        mut job_version: Version,
    ) -> Result<(), MetadError> {
        // A snapshot pin is a new historical borrower. Keep its publication
        // mutually exclusive with the reachability proof and exact-reference
        // CAS below, without serializing ordinary object GC against snapshot
        // minting (the durable object-GC claim version already fences that).
        let _restore_snapshot_gate = self
            .restore_snapshot_gate
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let (operation, operation_version) =
            self.releasing_restore_operation(&job.operation_digest, job.ref_set_id)?;
        // System-family restore rows have no historical MVCC view. If a live
        // snapshot can still reach any member in this ref-set, retain the whole
        // operation (members, exact refs, overlay, seals, and root index) until
        // that pin retires. Snapshot minting and this worker share
        // `restore_snapshot_gate`, so the proof cannot race a newly published
        // pin.
        // Members -> Overlay is a durable, prefix-empty cut made while this
        // worker holds both object_gc_gate and restore_snapshot_gate. Before
        // that cut, every historical holder is discoverable through a staging
        // inverse and must be rechecked on every page. After it, no current
        // namespace inode can identify this ref-set and a newly minted
        // snapshot/ForkBinding cannot resurrect one, so repeating a full
        // historical subtree walk for every one of the index-release stages is
        // both redundant and pathologically expensive on a deep Holt history.
        if job.phase != RestoreReleasePhase::Overlay
            && self.restore_live_snapshot_holds_ref_set(&operation)?
        {
            return Ok(());
        }
        match job.phase {
            RestoreReleasePhase::ExactReferences => {
                self.release_restore_reference_page(
                    &operation,
                    operation_version,
                    &mut job,
                    &mut job_version,
                )?;
            }
            RestoreReleasePhase::Members => {
                self.release_restore_member_page(
                    &operation,
                    operation_version,
                    &mut job,
                    &mut job_version,
                )?;
            }
            RestoreReleasePhase::Overlay => {
                let outcome = self.release_restore_index_page(
                    &operation,
                    operation_version,
                    &job.cursor,
                    RESTORE_BATCH_ENTRIES,
                )?;
                job.cursor = outcome.cursor;
                if outcome.complete {
                    self.finish_restore_release(&operation, operation_version, &job, job_version)?;
                } else {
                    self.update_restore_release_job(
                        &operation,
                        operation_version,
                        &job,
                        &mut job_version,
                    )?;
                }
            }
        }
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(&operation),
            live_test_barrier::RestoreAppliedPhase::ReleaseBatch(0),
        )?;
        Ok(())
    }

    fn releasing_restore_operation(
        &self,
        digest: &[u8; 32],
        ref_set_id: u64,
    ) -> Result<(RestoreOperation, Version), MetadError> {
        let key = restore_operation_key(self.mount, digest);
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::Codec(
                "restore release job has no operation".to_owned(),
            ))?;
        let operation = decode_restore_operation(&item.value.0)?;
        if operation.operation_digest != *digest
            || operation.ref_set_id != ref_set_id
            || operation.state != RestoreOperationState::Releasing
        {
            return Err(MetadError::Codec(
                "restore release job operation identity changed".to_owned(),
            ));
        }
        Ok((operation, item.version))
    }

    fn restore_reference_release_command(
        &self,
        input: RestoreReferenceReleaseCommand<'_>,
    ) -> Result<MetadataCommand, MetadError> {
        let RestoreReferenceReleaseCommand {
            operation,
            operation_version,
            job,
            job_version,
            entries,
            defer_gc_for,
            deferred_guard,
            object_reference,
            version,
        } = input;
        let first = entries.first().ok_or_else(|| {
            MetadError::Codec("restore reference release batch is empty".to_owned())
        })?;
        let mut predicates = Vec::with_capacity(3 + entries.len().saturating_mul(3));
        predicates.extend([
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(job_version),
            },
            object_reference.predicate(self.mount),
        ]);
        match (defer_gc_for, deferred_guard) {
            (Some(object_digest), Some(guard)) if guard.object_digest == object_digest => {
                predicates.extend([
                    PredicateRef {
                        family: RecordFamily::System,
                        key: guard.owner_key.clone(),
                        predicate: Predicate::VersionEquals(guard.owner_version),
                    },
                    PredicateRef {
                        family: RecordFamily::System,
                        key: guard.inverse_key.clone(),
                        predicate: Predicate::VersionEquals(guard.inverse_version),
                    },
                    PredicateRef {
                        family: RecordFamily::System,
                        key: guard.inverse_owner_key.clone(),
                        predicate: Predicate::VersionEquals(guard.inverse_owner_version),
                    },
                ]);
            }
            (None, None) => {}
            _ => {
                return Err(MetadError::Codec(
                    "restore release deferred GC has no matching continuation proof".to_owned(),
                ));
            }
        }
        let mut mutations = Vec::with_capacity(1 + entries.len().saturating_mul(4));
        mutations.push(Mutation {
            family: RecordFamily::System,
            key: restore_release_job_key(self.mount, operation.ref_set_id),
            op: MutationOp::Put,
            value: Some(Value(encode_restore_release_job(job)?)),
        });
        let enqueue_unix_ms = current_time_ms();
        let mut gc_records = BTreeMap::new();
        for entry in entries {
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::System,
                    key: entry.owner_key.clone(),
                    predicate: Predicate::VersionEquals(entry.owner_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: entry.inverse_key.clone(),
                    predicate: Predicate::VersionEquals(entry.inverse_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: entry.inverse_owner_key.clone(),
                    predicate: Predicate::VersionEquals(entry.inverse_owner_version),
                },
            ]);
            mutations.extend([
                delete_mutation(RecordFamily::System, entry.owner_key.clone()),
                delete_mutation(RecordFamily::System, entry.inverse_key.clone()),
                delete_mutation(RecordFamily::System, entry.inverse_owner_key.clone()),
            ]);
            if defer_gc_for == Some(entry.object_digest) {
                // The next owner row belongs to the same canonical object.
                // Defer enqueue until its final owner page so a malformed
                // continuation cannot be hidden between release pages.
                continue;
            }
            let (owner_inode, owner_generation, chunk_index, block_index) = entry.identity;
            let record = ObjectGcRecord {
                inode: owner_inode,
                generation: owner_generation,
                object_key: entry.reference.object_key.clone(),
                size: entry.reference.size,
                digest_uri: entry.reference.digest_uri.clone(),
                enqueue_version: version.get(),
                enqueue_unix_ms,
            };
            let key = gc_object_key(
                self.mount,
                version.get(),
                owner_inode,
                owner_generation,
                chunk_index,
                block_index,
            );
            if let Some(existing) = gc_records.insert(key, record.clone()) {
                if existing != record {
                    return Err(MetadError::Codec(
                        "restore release batch has inconsistent canonical object identity"
                            .to_owned(),
                    ));
                }
            }
        }
        mutations.extend(gc_records.into_iter().map(|(key, record)| Mutation {
            family: RecordFamily::Gc,
            key,
            op: MutationOp::Put,
            value: Some(Value(encode_object_gc_record(&record))),
        }));
        Ok(MetadataCommand {
            request_id: request_id(
                b"restore-release-reference-batch",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: first.owner_key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_restore_reference_release_batch(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &RestoreReleaseJob,
        job_version: &mut Version,
        entries: &mut Vec<RestoreReferenceReleaseEntry>,
        defer_gc_for: Option<[u8; 32]>,
        deferred_guard: Option<&RestoreReferenceReleaseGuard>,
    ) -> Result<(), MetadError> {
        if entries.is_empty() {
            return Ok(());
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let command = self.restore_reference_release_command(RestoreReferenceReleaseCommand {
            operation,
            operation_version,
            job,
            job_version: *job_version,
            entries,
            defer_gc_for,
            deferred_guard,
            object_reference,
            version,
        })?;
        validate_restore_command_bounds(&command, "restore release reference batch")?;
        let request_id = command.request_id.clone();
        let expected_mutations = command.mutations.len();
        let commit = self.commit_metadata(command);
        match commit {
            Ok(_) => {}
            Err(MetadError::Metadata(MetadataError::Backend(message))) => {
                let applied = self
                    .metadata
                    .committed_request_result(&request_id)?
                    .is_some_and(|result| {
                        result.commit_version == version
                            && result.applied_mutations == expected_mutations
                            && result.watch_events == 0
                    });
                if !applied {
                    return Err(MetadError::Metadata(MetadataError::Backend(message)));
                }
            }
            Err(error) => return Err(error),
        }
        *job_version = version;
        entries.clear();
        Ok(())
    }

    fn quarantine_restore_release_row_and_advance(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
        row: &crate::command::ScanItem,
        reason: &str,
    ) -> Result<(), MetadError> {
        self.quarantine_restore_release_row_and_advance_scoped(
            operation,
            operation_version,
            job,
            job_version,
            row,
            reason,
            RestoreReleaseQuarantineScope::Diagnostic,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn quarantine_restore_release_object_row_and_advance(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
        pending: &mut Vec<RestoreReferenceReleaseEntry>,
        row: &crate::command::ScanItem,
        reason: &str,
        object_digest: Option<[u8; 32]>,
    ) -> Result<(), MetadError> {
        self.quarantine_restore_release_row_and_advance_scoped(
            operation,
            operation_version,
            job,
            job_version,
            row,
            reason,
            object_digest.map_or(
                RestoreReleaseQuarantineScope::MountWide,
                RestoreReleaseQuarantineScope::Object,
            ),
        )?;
        // The marker must become durable before a preceding valid row can
        // enqueue this object's GC candidate. Otherwise a crash between the
        // batch and this quarantine could hide the only malformed borrower.
        self.commit_restore_reference_release_batch(
            operation,
            operation_version,
            job,
            job_version,
            pending,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn quarantine_restore_release_row_and_advance_scoped(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
        row: &crate::command::ScanItem,
        reason: &str,
        scope: RestoreReleaseQuarantineScope,
    ) -> Result<(), MetadError> {
        job.cursor = row.key.clone();
        let quarantine_key =
            restore_release_quarantine_key(self.mount, RecordFamily::System, row, scope);
        let quarantine = self.metadata.get_versioned(
            RecordFamily::System,
            &quarantine_key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?;
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(*job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            },
        ];
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                op: MutationOp::Put,
                value: Some(Value(encode_restore_release_job(job)?)),
            },
            // Retain the malformed active row. Removing it could make the
            // ref-set owner prefix appear empty while a target-first inverse
            // still exists, allowing finalization to erase the operation that
            // fsck needs for repair. The per-job cursor advances past it, and
            // the mount-global round-robin cursor keeps unrelated jobs moving.
        ];
        match quarantine {
            Some(item) => predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: quarantine_key,
                predicate: Predicate::VersionEquals(item.version),
            }),
            None => {
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: quarantine_key.clone(),
                    predicate: Predicate::NotExists,
                });
                mutations.push(Mutation {
                    family: RecordFamily::System,
                    key: quarantine_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_release_quarantine(
                        RecordFamily::System,
                        row,
                        reason,
                        scope,
                    )?)),
                });
            }
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-quarantine-release-row",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: row.key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release row quarantine")?;
        self.commit_metadata(command)?;
        *job_version = version;
        Ok(())
    }

    fn restore_reference_release_guard_for_row(
        &self,
        operation: &RestoreOperation,
        row: &crate::command::ScanItem,
        read_version: Version,
    ) -> Result<RestoreReferenceReleaseGuardValidation, MetadError> {
        let keyed_object_digest =
            restore_base_owner_object_digest_from_key(self.mount, operation.ref_set_id, &row.key);
        let Some(keyed_object_digest) = keyed_object_digest else {
            return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                reason: "restore release owner continuation has an invalid key identity".to_owned(),
                object_digest: None,
            });
        };
        let reference = match super::restore_gc::decode_restore_base_reference(&row.value.0) {
            Ok(reference) => reference,
            Err(error) => {
                return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                    reason: error.to_string(),
                    object_digest: None,
                });
            }
        };
        let object_digest: [u8; 32] = Sha256::digest(reference.object_key.as_bytes()).into();
        if reference.operation_digest != operation.operation_digest
            || reference.ref_set_id != operation.ref_set_id
            || object_digest != keyed_object_digest
            || restore_base_owner_key(
                self.mount,
                reference.ref_set_id,
                &object_digest,
                reference.borrower_inode,
                reference.borrower_generation,
            ) != row.key
        {
            return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                reason: "restore release owner continuation changed identity".to_owned(),
                object_digest: (object_digest == keyed_object_digest).then_some(object_digest),
            });
        }
        let inverse_key = restore_base_inverse_key(
            self.mount,
            &object_digest,
            reference.ref_set_id,
            reference.borrower_inode,
            reference.borrower_generation,
        );
        let inverse = self.metadata.get_versioned(
            RecordFamily::System,
            &inverse_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        let Some(inverse) = inverse else {
            return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                reason: "restore release owner continuation has no inverse row".to_owned(),
                object_digest: Some(object_digest),
            });
        };
        let decoded_inverse = match super::restore_gc::decode_restore_base_inverse(&inverse.value.0)
        {
            Ok(inverse) => inverse,
            Err(error) => {
                return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                    reason: error.to_string(),
                    object_digest: Some(object_digest),
                });
            }
        };
        if decoded_inverse.operation_digest != operation.operation_digest
            || decoded_inverse.ref_set_id != operation.ref_set_id
            || decoded_inverse.object_digest != object_digest
            || decoded_inverse.borrower_inode != reference.borrower_inode
            || decoded_inverse.borrower_generation != reference.borrower_generation
        {
            return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                reason: "restore release inverse continuation changed identity".to_owned(),
                object_digest: Some(object_digest),
            });
        }
        let inverse_owner_key = restore_base_inverse_owner_key(
            self.mount,
            operation.ref_set_id,
            &object_digest,
            reference.borrower_inode,
            reference.borrower_generation,
        );
        let inverse_owner = self.metadata.get_versioned(
            RecordFamily::System,
            &inverse_owner_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        let Some(inverse_owner) = inverse_owner else {
            return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                reason: "restore release inverse continuation has no ref-set owner".to_owned(),
                object_digest: Some(object_digest),
            });
        };
        if inverse_owner.value != inverse.value {
            return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                reason: "restore release inverse-owner continuation changed identity".to_owned(),
                object_digest: Some(object_digest),
            });
        }
        let identity = match self.canonical_block_object_identity(&reference.object_key) {
            Ok(identity) => identity,
            Err(error) => {
                return Ok(RestoreReferenceReleaseGuardValidation::Corrupt {
                    reason: error.to_string(),
                    object_digest: Some(object_digest),
                });
            }
        };
        Ok(RestoreReferenceReleaseGuardValidation::Valid(Box::new(
            RestoreReferenceReleaseGuard {
                owner_key: row.key.clone(),
                owner_version: row.version,
                inverse_key,
                inverse_version: inverse.version,
                inverse_owner_key,
                inverse_owner_version: inverse_owner.version,
                reference,
                object_digest,
                identity,
            },
        )))
    }

    fn release_restore_reference_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
    ) -> Result<(), MetadError> {
        let prefix = restore_base_owner_prefix(self.mount, operation.ref_set_id);
        let read_version = self.read_version()?;
        let scanned_rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: prefix.clone(),
            start_after: (!job.cursor.is_empty()).then_some(job.cursor.clone()),
            version: read_version,
            limit: RESTORE_BATCH_ENTRIES.saturating_add(1),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let rows = &scanned_rows[..scanned_rows.len().min(RESTORE_BATCH_ENTRIES)];
        let borrower_inodes = rows
            .iter()
            .filter_map(|row| {
                super::restore_gc::decode_restore_base_reference(&row.value.0)
                    .ok()
                    .map(|reference| reference.borrower_inode)
            })
            .collect::<HashSet<_>>();
        let reachable_bodies =
            self.restore_reachable_inode_bodies_at(&borrower_inodes, read_version)?;
        let mut cursor_dirty = false;
        let mut processed_all_rows = true;
        let mut pending = Vec::new();
        for row in rows {
            let keyed_object_digest = restore_base_owner_object_digest_from_key(
                self.mount,
                operation.ref_set_id,
                &row.key,
            );
            let reference = match super::restore_gc::decode_restore_base_reference(&row.value.0) {
                Ok(reference) => reference,
                Err(error) => {
                    self.quarantine_restore_release_object_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        &mut pending,
                        row,
                        &error.to_string(),
                        None,
                    )?;
                    cursor_dirty = false;
                    continue;
                }
            };
            let object_digest: [u8; 32] = Sha256::digest(reference.object_key.as_bytes()).into();
            if reference.operation_digest != operation.operation_digest
                || reference.ref_set_id != operation.ref_set_id
                || restore_base_owner_key(
                    self.mount,
                    reference.ref_set_id,
                    &object_digest,
                    reference.borrower_inode,
                    reference.borrower_generation,
                ) != row.key
            {
                self.quarantine_restore_release_object_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &mut pending,
                    row,
                    "restore release owner row changed identity",
                    (keyed_object_digest == Some(object_digest)).then_some(object_digest),
                )?;
                cursor_dirty = false;
                continue;
            }
            let inverse_key = restore_base_inverse_key(
                self.mount,
                &object_digest,
                reference.ref_set_id,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            let inverse = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?;
            let Some(inverse) = inverse else {
                self.quarantine_restore_release_object_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &mut pending,
                    row,
                    "restore release owner has no inverse row",
                    keyed_object_digest,
                )?;
                cursor_dirty = false;
                continue;
            };
            let decoded_inverse =
                match super::restore_gc::decode_restore_base_inverse(&inverse.value.0) {
                    Ok(inverse) => inverse,
                    Err(error) => {
                        self.quarantine_restore_release_object_row_and_advance(
                            operation,
                            operation_version,
                            job,
                            job_version,
                            &mut pending,
                            row,
                            &error.to_string(),
                            keyed_object_digest,
                        )?;
                        cursor_dirty = false;
                        continue;
                    }
                };
            if decoded_inverse.operation_digest != operation.operation_digest
                || decoded_inverse.ref_set_id != operation.ref_set_id
                || decoded_inverse.object_digest != object_digest
                || decoded_inverse.borrower_inode != reference.borrower_inode
                || decoded_inverse.borrower_generation != reference.borrower_generation
            {
                self.quarantine_restore_release_object_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &mut pending,
                    row,
                    "restore release inverse row changed identity",
                    keyed_object_digest,
                )?;
                cursor_dirty = false;
                continue;
            }
            let inverse_owner_key = restore_base_inverse_owner_key(
                self.mount,
                operation.ref_set_id,
                &object_digest,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            let inverse_owner = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_owner_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?;
            let Some(inverse_owner) = inverse_owner else {
                self.quarantine_restore_release_object_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &mut pending,
                    row,
                    "restore release base inverse has no ref-set owner",
                    keyed_object_digest,
                )?;
                cursor_dirty = false;
                continue;
            };
            if inverse_owner.value != inverse.value {
                self.quarantine_restore_release_object_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &mut pending,
                    row,
                    "restore release base inverse owner changed identity",
                    keyed_object_digest,
                )?;
                cursor_dirty = false;
                continue;
            }
            let retained = match self.restore_borrower_references_object(
                &reference,
                read_version,
                &reachable_bodies,
            ) {
                Ok(retained) => retained,
                Err(error) if restore_release_error_is_retryable(&error) => return Err(error),
                Err(error) => {
                    self.quarantine_restore_release_object_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        &mut pending,
                        row,
                        &error.to_string(),
                        keyed_object_digest,
                    )?;
                    cursor_dirty = false;
                    continue;
                }
            };
            if retained {
                // Retained rows only advance the release frontier. Persist one
                // frontier per page instead of issuing one metadata command per
                // row. A crash before the page commit causes a bounded rescan;
                // object deletion and GC enqueue remain atomic in the batch.
                job.cursor = row.key.clone();
                cursor_dirty = true;
                continue;
            }
            let identity = match self.canonical_block_object_identity(&reference.object_key) {
                Ok(identity) => identity,
                Err(error) => {
                    self.quarantine_restore_release_object_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        &mut pending,
                        row,
                        &error.to_string(),
                        keyed_object_digest,
                    )?;
                    cursor_dirty = false;
                    continue;
                }
            };
            if pending.iter().any(|entry: &RestoreReferenceReleaseEntry| {
                entry.reference.object_key == reference.object_key
                    && (entry.identity != identity
                        || entry.reference.size != reference.size
                        || entry.reference.digest_uri != reference.digest_uri)
            }) {
                self.quarantine_restore_release_object_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &mut pending,
                    row,
                    "restore release references disagree on canonical object identity",
                    keyed_object_digest,
                )?;
                cursor_dirty = false;
                continue;
            }
            let previous_cursor = job.cursor.clone();
            job.cursor = row.key.clone();
            cursor_dirty = true;
            pending.push(RestoreReferenceReleaseEntry {
                owner_key: row.key.clone(),
                owner_version: row.version,
                inverse_key,
                inverse_version: inverse.version,
                inverse_owner_key,
                inverse_owner_version: inverse_owner.version,
                reference,
                object_digest,
                identity,
            });
            let bounds_version = Version::new(2)?;
            let reserve_guard = RestoreReferenceReleaseGuard::from(
                pending
                    .last()
                    .expect("the candidate release entry was just appended"),
            );
            let bounds_command =
                self.restore_reference_release_command(RestoreReferenceReleaseCommand {
                    operation,
                    operation_version,
                    job,
                    job_version: *job_version,
                    entries: &pending,
                    defer_gc_for: None,
                    deferred_guard: None,
                    object_reference: ObjectReferenceMutation::from_version(bounds_version),
                    version: bounds_version,
                })?;
            let deferred_bounds_command =
                self.restore_reference_release_command(RestoreReferenceReleaseCommand {
                    operation,
                    operation_version,
                    job,
                    job_version: *job_version,
                    entries: &pending,
                    defer_gc_for: Some(reserve_guard.object_digest),
                    deferred_guard: Some(&reserve_guard),
                    object_reference: ObjectReferenceMutation::from_version(bounds_version),
                    version: bounds_version,
                })?;
            let bounds =
                validate_restore_command_bounds(&bounds_command, "restore release reference batch")
                    .and_then(|()| {
                        validate_restore_command_bounds(
                            &deferred_bounds_command,
                            "restore release reference batch with continuation proof",
                        )
                    });
            match bounds {
                Ok(()) => {}
                Err(MetadError::RestoreResourceLimit { .. }) => {
                    let continuation = pending
                        .pop()
                        .expect("the candidate release entry was just appended");
                    job.cursor = previous_cursor;
                    if pending.is_empty() {
                        self.quarantine_restore_release_object_row_and_advance(
                            operation,
                            operation_version,
                            job,
                            job_version,
                            &mut pending,
                            row,
                            "restore release reference exceeds the command byte budget",
                            keyed_object_digest,
                        )?;
                        cursor_dirty = false;
                        continue;
                    }
                    let defer_gc_for = pending
                        .last()
                        .is_some_and(|entry| entry.object_digest == object_digest)
                        .then_some(object_digest);
                    let deferred_guard =
                        defer_gc_for.map(|_| RestoreReferenceReleaseGuard::from(&continuation));
                    self.commit_restore_reference_release_batch(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        &mut pending,
                        defer_gc_for,
                        deferred_guard.as_ref(),
                    )?;
                    cursor_dirty = false;
                    processed_all_rows = false;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        let mut deferred_guard = None;
        if processed_all_rows && scanned_rows.len() > RESTORE_BATCH_ENTRIES {
            let lookahead = &scanned_rows[RESTORE_BATCH_ENTRIES];
            match self.restore_reference_release_guard_for_row(
                operation,
                lookahead,
                read_version,
            )? {
                RestoreReferenceReleaseGuardValidation::Valid(guard) => {
                    let continuation_needed = pending
                        .last()
                        .is_some_and(|entry| entry.object_digest == guard.object_digest);
                    if continuation_needed
                        && pending.iter().any(|entry| {
                            entry.reference.object_key == guard.reference.object_key
                                && (entry.identity != guard.identity
                                    || entry.reference.size != guard.reference.size
                                    || entry.reference.digest_uri != guard.reference.digest_uri)
                        })
                    {
                        self.quarantine_restore_release_object_row_and_advance(
                            operation,
                            operation_version,
                            job,
                            job_version,
                            &mut pending,
                            lookahead,
                            "restore release continuation disagrees on canonical object identity",
                            Some(guard.object_digest),
                        )?;
                        cursor_dirty = false;
                        processed_all_rows = false;
                    } else if continuation_needed {
                        deferred_guard = Some(guard);
                    }
                }
                RestoreReferenceReleaseGuardValidation::Corrupt {
                    reason,
                    object_digest,
                } => {
                    self.quarantine_restore_release_object_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        &mut pending,
                        lookahead,
                        &reason,
                        object_digest,
                    )?;
                    cursor_dirty = false;
                    processed_all_rows = false;
                }
            }
        }
        if !pending.is_empty() {
            let defer_gc_for = deferred_guard.as_ref().map(|guard| guard.object_digest);
            self.commit_restore_reference_release_batch(
                operation,
                operation_version,
                job,
                job_version,
                &mut pending,
                defer_gc_for,
                deferred_guard.as_deref(),
            )?;
            cursor_dirty = false;
        }
        let any_owner = self.restore_ref_set_has_base_owners(operation.ref_set_id)?;
        if !any_owner || processed_all_rows && rows.len() < RESTORE_BATCH_ENTRIES {
            // A reachable escaped borrower may keep a small set of exact
            // references indefinitely. Once one full owner pass reaches its
            // tail, let the member worker reclaim unrelated detached members
            // (including initialization artifacts), then return here for the
            // next reachability pass. This prevents one live borrower from
            // starving the rest of the release job.
            job.phase = RestoreReleasePhase::Members;
            job.cursor.clear();
            cursor_dirty = true;
        }
        if cursor_dirty {
            self.update_restore_release_job(operation, operation_version, job, job_version)?;
        }
        Ok(())
    }

    fn restore_ref_set_has_base_owners(&self, ref_set_id: u64) -> Result<bool, MetadError> {
        let version = self.read_version()?;
        for prefix in [
            restore_base_owner_prefix(self.mount, ref_set_id),
            restore_base_inverse_owner_prefix(self.mount, ref_set_id),
        ] {
            if !self
                .metadata
                .scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix,
                    start_after: None,
                    version,
                    limit: 1,
                    purpose: ReadPurpose::WritePlanLocal,
                })?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn release_restore_member_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
    ) -> Result<(), MetadError> {
        let prefix = restore_staging_member_prefix(self.mount, operation.ref_set_id);
        let read_version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: prefix.clone(),
            start_after: (!job.cursor.is_empty()).then_some(job.cursor.clone()),
            version: read_version,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::RestoreStaging,
        })?;
        let candidate_inodes = rows
            .iter()
            .filter_map(|row| {
                let member = decode_restore_staging_member(&row.value.0).ok()?;
                (member.operation_digest == operation.operation_digest
                    && restore_staging_member_key(
                        self.mount,
                        operation.ref_set_id,
                        member.destination_inode,
                    ) == row.key)
                    .then_some(member.destination_inode)
            })
            .collect::<HashSet<_>>();
        let reachable_inodes = self.restore_reachable_inodes_at(&candidate_inodes, read_version)?;
        for row in &rows {
            let member = match decode_restore_staging_member(&row.value.0) {
                Ok(member)
                    if member.operation_digest == operation.operation_digest
                        && restore_staging_member_key(
                            self.mount,
                            operation.ref_set_id,
                            member.destination_inode,
                        ) == row.key =>
                {
                    member
                }
                Ok(_) => {
                    self.quarantine_restore_release_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        row,
                        "restore release member row changed identity",
                    )?;
                    continue;
                }
                Err(error) => {
                    self.quarantine_restore_release_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        row,
                        &error.to_string(),
                    )?;
                    continue;
                }
            };
            let inverse_key = restore_staging_inode_key(self.mount, member.destination_inode);
            let inverse = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )?;
            let Some(inverse) = inverse else {
                self.quarantine_restore_release_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    row,
                    "restore release member has no staging inverse",
                )?;
                continue;
            };
            let inverse_identity = decode_restore_staging_inverse(&inverse.value.0);
            if !matches!(
                inverse_identity,
                Ok((digest, ref_set_id))
                    if digest == operation.operation_digest
                        && ref_set_id == operation.ref_set_id
            ) {
                let reason = inverse_identity.err().map_or_else(
                    || "restore release staging inverse changed identity".to_owned(),
                    |error| error.to_string(),
                );
                self.quarantine_restore_release_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    row,
                    &reason,
                )?;
                continue;
            }
            let inverse_owner_key = restore_staging_inverse_owner_key(
                self.mount,
                operation.ref_set_id,
                member.destination_inode,
            );
            let inverse_owner = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_owner_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )?;
            let Some(inverse_owner) = inverse_owner else {
                self.quarantine_restore_release_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    row,
                    "restore release member has no ref-set inverse owner",
                )?;
                continue;
            };
            if inverse_owner.value != inverse.value {
                self.quarantine_restore_release_row_and_advance(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    row,
                    "restore release staging inverse owner changed identity",
                )?;
                continue;
            }
            let reachable = reachable_inodes.contains(&member.destination_inode);
            if !reachable {
                if self.release_restore_directory_children(
                    operation,
                    operation_version,
                    job,
                    job_version,
                    &member,
                    row.version,
                    &inverse_key,
                    inverse.version,
                    &inverse_owner_key,
                    inverse_owner.version,
                )? {
                    return Ok(());
                }
                if self.release_restore_member_canonical_index_page(
                    operation,
                    operation_version,
                    job,
                    *job_version,
                    &member,
                    row.version,
                    &inverse_key,
                    inverse.version,
                )? {
                    return Ok(());
                }
                if self.release_restore_member_manifest_page(
                    operation,
                    operation_version,
                    job,
                    *job_version,
                    &member,
                    row.version,
                    &inverse_key,
                    inverse.version,
                )? {
                    return Ok(());
                }
                if self.release_restore_member_metadata_page(
                    operation,
                    operation_version,
                    job,
                    *job_version,
                    &member,
                    row.version,
                    &inverse_key,
                    inverse.version,
                    RecordFamily::Xattr,
                    xattr_prefix(self.mount, member.destination_inode),
                    b"restore-release-member-xattrs",
                )? {
                    return Ok(());
                }
                if self.release_restore_member_dentry_page(
                    operation,
                    operation_version,
                    job,
                    *job_version,
                    &member,
                    row.version,
                    &inverse_key,
                    inverse.version,
                )? {
                    return Ok(());
                }
            }
            if let Err(error) = self.finish_restore_release_member(
                operation,
                operation_version,
                job,
                job_version,
                &member,
                row.version,
                &inverse_key,
                inverse.version,
                &inverse_owner_key,
                inverse_owner.version,
                reachable,
            ) {
                if matches!(
                    &error,
                    MetadError::Codec(_)
                        | MetadError::MissingBodyDescriptor
                        | MetadError::RestoreResourceLimit { .. }
                ) {
                    self.quarantine_restore_release_row_and_advance(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        row,
                        &error.to_string(),
                    )?;
                    continue;
                }
                return Err(error);
            }
        }
        let any_member = !self
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix,
                start_after: None,
                version: self.read_version()?,
                limit: 1,
                purpose: ReadPurpose::RestoreStaging,
            })?
            .is_empty()
            || !self
                .metadata
                .scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: restore_staging_inverse_owner_prefix(self.mount, operation.ref_set_id),
                    start_after: None,
                    version: self.read_version()?,
                    limit: 1,
                    purpose: ReadPurpose::RestoreStaging,
                })?
                .is_empty();
        let any_owner = self.restore_ref_set_has_base_owners(operation.ref_set_id)?;
        if !any_member {
            if any_owner {
                return Err(MetadError::Codec(
                    "restore release base references have no staging members".to_owned(),
                ));
            }
            job.phase = RestoreReleasePhase::Overlay;
            job.cursor.clear();
            self.transition_restore_release_to_overlay(
                operation,
                operation_version,
                job,
                job_version,
            )?;
        } else if rows.len() < RESTORE_BATCH_ENTRIES {
            if any_owner {
                job.phase = RestoreReleasePhase::ExactReferences;
            }
            job.cursor.clear();
            self.update_restore_release_job(operation, operation_version, job, job_version)?;
        }
        Ok(())
    }

    /// Persistently discover one page below a detached directory before its
    /// inode can be reclaimed. Each child is enrolled in the same ref-set in
    /// the command that advances the release cursor. Advancing past the parent
    /// prevents a low-id directory from starving newly discovered children;
    /// the ordinary end-of-keyspace wrap revisits it after the children drain.
    #[allow(clippy::too_many_arguments)]
    fn release_restore_directory_children(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
        inverse_owner_key: &[u8],
        inverse_owner_version: Version,
    ) -> Result<bool, MetadError> {
        let read_version = self.read_version()?;
        let inode_key = inode_key(self.mount, member.destination_inode);
        let Some(inode) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &inode_key,
            read_version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(false);
        };
        let attr = decode_inode_attr(&inode.value.0)
            .map_err(|error| MetadError::Codec(error.to_string()))?;
        if attr.inode != member.destination_inode || attr.file_type != FileType::Directory {
            return Ok(false);
        }
        let prefix = dentry_prefix(self.mount, member.destination_inode);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: prefix.clone(),
            start_after: None,
            version: read_version,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::RestoreStaging,
        })?;
        if rows.is_empty() {
            return Ok(false);
        }
        let mut projections = Vec::with_capacity(rows.len());
        for row in &rows {
            let projection = decode_dentry_projection(&row.value.0)
                .map_err(|error| MetadError::Codec(error.to_string()))?;
            if projection.dentry.parent != member.destination_inode
                || dentry_key(
                    self.mount,
                    projection.dentry.parent,
                    &projection.dentry.name,
                ) != row.key
            {
                return Err(MetadError::Codec(
                    "restore release child dentry changed identity".to_owned(),
                ));
            }
            let child_inverse_key = restore_staging_inode_key(self.mount, projection.attr.inode);
            if let Some(child_inverse) = self.metadata.get_versioned(
                RecordFamily::System,
                &child_inverse_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )? {
                let (child_digest, child_ref_set_id) =
                    decode_restore_staging_inverse(&child_inverse.value.0)?;
                if child_digest != operation.operation_digest
                    || child_ref_set_id != operation.ref_set_id
                {
                    return self.cascade_nested_restore_release(
                        operation,
                        operation_version,
                        job,
                        job_version,
                        member,
                        member_version,
                        inverse_key,
                        inverse_version,
                        inverse_owner_key,
                        inverse_owner_version,
                        &inode_key,
                        inode.version,
                        row,
                        &projection,
                        &child_inverse_key,
                        child_inverse,
                        read_version,
                    );
                }
            }
            projections.push(projection);
        }
        let enrollment = self.restore_namespace_enrollment_plan_for_operation(
            operation,
            operation_version,
            &projections,
            read_version,
        )?;
        job.cursor =
            restore_staging_member_key(self.mount, operation.ref_set_id, member.destination_inode);
        let version = self.next_version()?;
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(*job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    member.destination_inode,
                ),
                predicate: Predicate::VersionEquals(member_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key,
                predicate: Predicate::VersionEquals(inode.version),
            },
        ];
        predicates.extend(enrollment.predicates);
        predicates.extend(rows.iter().map(|row| PredicateRef {
            family: RecordFamily::Dentry,
            key: row.key.clone(),
            predicate: Predicate::VersionEquals(row.version),
        }));
        let mut mutations = enrollment.mutations;
        mutations.push(Mutation {
            family: RecordFamily::System,
            key: restore_release_job_key(self.mount, operation.ref_set_id),
            op: MutationOp::Put,
            value: Some(Value(encode_restore_release_job(job)?)),
        });
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-discover-children",
                self.mount,
                member.destination_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: prefix,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release child discovery")?;
        self.commit_metadata(command)?;
        *job_version = version;
        Ok(true)
    }

    /// Detach one nested restore root from an already-detached outer restore.
    /// The child operation owns its inode/ref-set, so the outer worker must not
    /// enrol or reclaim it. Instead, one command starts (or joins) the child's
    /// release, removes the outer parent dentry, and advances the outer cursor.
    #[allow(clippy::too_many_arguments)]
    fn cascade_nested_restore_release(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
        inverse_owner_key: &[u8],
        inverse_owner_version: Version,
        parent_inode_key: &[u8],
        parent_inode_version: Version,
        dentry_row: &crate::command::ScanItem,
        projection: &DentryProjection,
        child_inverse_key: &[u8],
        child_inverse: ReadItem,
        read_version: Version,
    ) -> Result<bool, MetadError> {
        if projection.attr.file_type != FileType::Directory
            || projection.attr.inode.shard_index() != self.shard_index()
        {
            return Err(MetadError::Codec(
                "restore release found a foreign ref-set on a non-local directory".to_owned(),
            ));
        }
        let child_root = projection.attr.inode;
        let (child_digest, child_ref_set_id) =
            decode_restore_staging_inverse(&child_inverse.value.0)?;
        if child_digest == operation.operation_digest
            || child_ref_set_id == operation.ref_set_id
            || child_ref_set_id == 0
        {
            return Err(MetadError::Codec(
                "restore release nested inverse has an invalid owner".to_owned(),
            ));
        }

        let child_operation_key = restore_operation_key(self.mount, &child_digest);
        let child_operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &child_operation_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or_else(|| MetadError::Codec("nested restore root has no operation".to_owned()))?;
        let mut child_operation = decode_restore_operation(&child_operation_item.value.0)?;
        if child_operation.operation_digest != child_digest
            || child_operation.ref_set_id != child_ref_set_id
            || child_operation.destination_root != child_root
        {
            return Err(MetadError::Codec(
                "nested restore operation changed identity".to_owned(),
            ));
        }

        let child_root_key = restore_root_index_key(self.mount, child_root);
        let child_root_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &child_root_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or_else(|| {
                MetadError::Codec("nested restore operation has no root index".to_owned())
            })?;
        if child_root_item.value.0 != child_digest {
            return Err(MetadError::Codec(
                "nested restore root index changed identity".to_owned(),
            ));
        }

        let child_member_key = restore_staging_member_key(self.mount, child_ref_set_id, child_root);
        let child_member_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &child_member_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or_else(|| {
                MetadError::Codec("nested restore root has no staging member".to_owned())
            })?;
        let child_member = decode_restore_staging_member(&child_member_item.value.0)?;
        if child_member.operation_digest != child_digest
            || child_member.destination_inode != child_root
            || child_member.destination_parent.is_some()
            || child_member.name.is_some()
        {
            return Err(MetadError::Codec(
                "nested restore root member changed identity".to_owned(),
            ));
        }
        let child_inverse_owner_key =
            restore_staging_inverse_owner_key(self.mount, child_ref_set_id, child_root);
        let child_inverse_owner = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &child_inverse_owner_key,
                read_version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or_else(|| {
                MetadError::Codec("nested restore root has no inverse owner".to_owned())
            })?;
        if child_inverse_owner.value != child_inverse.value {
            return Err(MetadError::Codec(
                "nested restore root inverse owner changed identity".to_owned(),
            ));
        }

        let child_release_key = restore_release_job_key(self.mount, child_ref_set_id);
        let child_release = self.metadata.get_versioned(
            RecordFamily::System,
            &child_release_key,
            read_version,
            ReadPurpose::RestoreStaging,
        )?;
        let mut predicates = vec![
            self.begin_object_reference_mutation()?
                .predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(*job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    member.destination_inode,
                ),
                predicate: Predicate::VersionEquals(member_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_owner_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_owner_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: parent_inode_key.to_vec(),
                predicate: Predicate::VersionEquals(parent_inode_version),
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_row.key.clone(),
                predicate: Predicate::VersionEquals(dentry_row.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: child_operation_key.clone(),
                predicate: Predicate::VersionEquals(child_operation_item.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: child_root_key,
                predicate: Predicate::VersionEquals(child_root_item.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: child_member_key,
                predicate: Predicate::VersionEquals(child_member_item.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: child_inverse_key.to_vec(),
                predicate: Predicate::VersionEquals(child_inverse.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: child_inverse_owner_key,
                predicate: Predicate::VersionEquals(child_inverse_owner.version),
            },
        ];

        job.cursor =
            restore_staging_member_key(self.mount, operation.ref_set_id, member.destination_inode);
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, dentry_row.key.clone()),
            Mutation {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                op: MutationOp::Put,
                value: Some(Value(encode_restore_release_job(job)?)),
            },
        ];
        match child_operation.state {
            RestoreOperationState::Complete => {
                if child_release.is_some() {
                    return Err(MetadError::Codec(
                        "complete nested restore already has a release job".to_owned(),
                    ));
                }
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: child_release_key.clone(),
                    predicate: Predicate::NotExists,
                });
                child_operation.state = RestoreOperationState::Releasing;
                let child_job = RestoreReleaseJob {
                    operation_digest: child_digest,
                    ref_set_id: child_ref_set_id,
                    phase: RestoreReleasePhase::ExactReferences,
                    cursor: Vec::new(),
                };
                mutations.extend([
                    Mutation {
                        family: RecordFamily::System,
                        key: child_operation_key,
                        op: MutationOp::Put,
                        value: Some(Value(encode_restore_operation(&child_operation)?)),
                    },
                    Mutation {
                        family: RecordFamily::System,
                        key: child_release_key,
                        op: MutationOp::Put,
                        value: Some(Value(encode_restore_release_job(&child_job)?)),
                    },
                ]);
            }
            RestoreOperationState::Releasing => {
                let child_release = child_release.ok_or_else(|| {
                    MetadError::Codec("releasing nested restore has no release job".to_owned())
                })?;
                let child_job = decode_restore_release_job(&child_release.value.0)?;
                if child_job.operation_digest != child_digest
                    || child_job.ref_set_id != child_ref_set_id
                {
                    return Err(MetadError::Codec(
                        "nested restore release job changed identity".to_owned(),
                    ));
                }
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: child_release_key,
                    predicate: Predicate::VersionEquals(child_release.version),
                });
            }
            RestoreOperationState::Preparing
            | RestoreOperationState::ReadyToAttach
            | RestoreOperationState::Cleaning
            | RestoreOperationState::Discarding => {
                return Err(MetadError::Codec(
                    "nested restore root is attached in a non-visible state".to_owned(),
                ));
            }
        }

        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-cascade-nested",
                self.mount,
                child_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_row.key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore nested release cascade")?;
        self.commit_metadata(command)?;
        *job_version = version;
        Ok(true)
    }

    /// Drain canonical PathIndex rows for a post-attach member without an
    /// unbounded mount scan. The cursor lives in the member row itself, so a
    /// crash or owner failover resumes after the last physical key. Ordinary
    /// writes cannot create another link to an unreachable Releasing inode.
    #[allow(clippy::too_many_arguments)]
    fn release_restore_member_canonical_index_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        _job: &RestoreReleaseJob,
        job_version: Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
    ) -> Result<bool, MetadError> {
        if member.canonical_index_complete {
            return Ok(false);
        }
        let read_version = self.read_version()?;
        let prefix = path_index_prefix(self.mount, &[]);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::PathIndex,
            prefix: prefix.clone(),
            start_after: (!member.canonical_index_cursor.is_empty())
                .then(|| member.canonical_index_cursor.clone()),
            version: read_version,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::RestoreStaging,
        })?;
        let mut next_member = member.clone();
        if rows.len() < RESTORE_BATCH_ENTRIES {
            next_member.canonical_index_complete = true;
            next_member.canonical_index_cursor.clear();
        } else {
            next_member.canonical_index_cursor = rows
                .last()
                .expect("a full restore index page has a last row")
                .key
                .clone();
        }
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    member.destination_inode,
                ),
                predicate: Predicate::VersionEquals(member_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_version),
            },
        ];
        let mut mutations = vec![Mutation {
            family: RecordFamily::System,
            key: restore_staging_member_key(
                self.mount,
                operation.ref_set_id,
                member.destination_inode,
            ),
            op: MutationOp::Put,
            value: Some(Value(encode_restore_staging_member(&next_member)?)),
        }];
        for row in rows {
            if !row.key.starts_with(&prefix) {
                return Err(MetadError::Codec(
                    "restore release canonical index scan escaped its mount".to_owned(),
                ));
            }
            let projection = decode_dentry_projection(&row.value.0)
                .map_err(|error| MetadError::Codec(error.to_string()))?;
            if projection.attr.inode != member.destination_inode {
                continue;
            }
            predicates.push(PredicateRef {
                family: RecordFamily::PathIndex,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            mutations.push(delete_mutation(RecordFamily::PathIndex, row.key));
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-member-canonical-index",
                self.mount,
                member.destination_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::PathIndex,
            primary_key: prefix,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release canonical path index")?;
        self.commit_metadata(command)?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn restore_owned_manifest_blocks(
        &self,
        destination_inode: InodeId,
        chunk_index: u64,
        manifest: &ChunkManifest,
    ) -> Result<Vec<BlockDescriptor>, MetadError> {
        if manifest.chunk_index != chunk_index {
            return Err(MetadError::Codec(
                "restore release chunk manifest changed identity".to_owned(),
            ));
        }
        let mut blocks = BTreeMap::<(u64, u64), BlockDescriptor>::new();
        for block in manifest.slices.iter().flat_map(|slice| slice.blocks.iter()) {
            if !self.block_object_is_owned_by_inode(destination_inode, &block.object_key)? {
                // Inherited manifests retain the source object's canonical key.
                // The source owner is responsible for enqueueing that object;
                // this restore only releases its exact borrower reference.
                continue;
            }
            let (object_owner, object_generation, object_chunk, block_index) =
                self.canonical_block_object_identity(&block.object_key)?;
            if object_owner != destination_inode || object_chunk != chunk_index {
                return Err(MetadError::Codec(
                    "restore release owned object changed manifest identity".to_owned(),
                ));
            }
            match blocks.entry((object_generation, block_index)) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(block.clone());
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if entry.get().object_key == block.object_key
                        && entry.get().digest_uri == block.digest_uri => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(MetadError::Codec(
                        "restore release manifest has conflicting owned block identities"
                            .to_owned(),
                    ));
                }
            }
        }
        Ok(blocks.into_values().collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn release_restore_member_manifest_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        _job: &RestoreReleaseJob,
        job_version: Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
    ) -> Result<bool, MetadError> {
        let prefix = inode_key(self.mount, member.destination_inode);
        let read_version = self.read_version()?;
        let row = if member.manifest_cursor.is_empty() {
            self.metadata
                .scan(ScanRequest {
                    family: RecordFamily::ChunkManifest,
                    prefix: prefix.clone(),
                    start_after: None,
                    version: read_version,
                    limit: 1,
                    purpose: ReadPurpose::RestoreStaging,
                })?
                .into_iter()
                .next()
        } else {
            if member.manifest_cursor.len() != prefix.len() + 16
                || !member.manifest_cursor.starts_with(&prefix)
            {
                return Err(MetadError::Codec(
                    "restore release manifest cursor has an invalid shape".to_owned(),
                ));
            }
            self.metadata
                .get_versioned(
                    RecordFamily::ChunkManifest,
                    &member.manifest_cursor,
                    read_version,
                    ReadPurpose::RestoreStaging,
                )?
                .map(|item| ScanItem {
                    key: member.manifest_cursor.clone(),
                    value: item.value,
                    version: item.version,
                })
        };
        let Some(row) = row else {
            if member.manifest_cursor.is_empty() {
                return Ok(false);
            }
            return Err(MetadError::Codec(
                "restore release manifest cursor points to a missing row".to_owned(),
            ));
        };
        if row.key.len() != prefix.len() + 16 || !row.key.starts_with(&prefix) {
            return Err(MetadError::Codec(
                "restore release chunk-manifest key has an invalid shape".to_owned(),
            ));
        }
        let generation = u64::from_be_bytes(
            row.key[prefix.len()..prefix.len() + 8]
                .try_into()
                .expect("validated generation width"),
        );
        let chunk_index = u64::from_be_bytes(
            row.key[prefix.len() + 8..]
                .try_into()
                .expect("validated chunk-index width"),
        );
        if generation == 0 {
            return Err(MetadError::Codec(
                "restore release chunk manifest has a zero generation".to_owned(),
            ));
        }

        let release_blocks = if chunk_index == BODY_SUMMARY_CHUNK_INDEX {
            if !member.manifest_cursor.is_empty() {
                return Err(MetadError::Codec(
                    "restore release body-summary row retains a block cursor".to_owned(),
                ));
            }
            let body = decode_body_descriptor(&row.value.0)
                .map_err(|error| MetadError::Codec(error.to_string()))?;
            if body.generation != generation {
                return Err(MetadError::Codec(
                    "restore release body summary generation changed identity".to_owned(),
                ));
            }
            Vec::new()
        } else {
            let manifest = decode_chunk_manifest(&row.value.0)
                .map_err(|error| MetadError::Codec(error.to_string()))?;
            self.restore_owned_manifest_blocks(member.destination_inode, chunk_index, &manifest)?
        };
        let block_cursor = usize::try_from(member.manifest_block_cursor).map_err(|_| {
            MetadError::Codec("restore release manifest block cursor overflow".to_owned())
        })?;
        if block_cursor > release_blocks.len()
            || (!member.manifest_cursor.is_empty() && block_cursor == release_blocks.len())
        {
            return Err(MetadError::Codec(
                "restore release manifest block cursor is out of range".to_owned(),
            ));
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    member.destination_inode,
                ),
                predicate: Predicate::VersionEquals(member_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_version),
            },
            PredicateRef {
                family: RecordFamily::ChunkManifest,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            },
        ];
        let mut next_member = member.clone();
        next_member.manifest_cursor = row.key.clone();
        next_member.manifest_block_cursor = member.manifest_block_cursor.saturating_add(1);
        let member_key =
            restore_staging_member_key(self.mount, operation.ref_set_id, member.destination_inode);
        // Reserve both the durable cursor update and the eventual manifest
        // deletion while packing GC rows. A partial page removes the delete,
        // so every committed command remains within both advertised bounds.
        let mutations = vec![
            Mutation {
                family: RecordFamily::System,
                key: member_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_staging_member(&next_member)?)),
            },
            delete_mutation(RecordFamily::ChunkManifest, row.key.clone()),
        ];
        let mut command = MetadataCommand {
            request_id: request_id(
                b"restore-release-member-manifests",
                self.mount,
                member.destination_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::ChunkManifest,
            primary_key: prefix,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release member manifests")?;

        let mut accepted = 0_usize;
        for block in release_blocks.iter().skip(block_cursor) {
            let (object_owner, object_generation, object_chunk, block_index) =
                self.canonical_block_object_identity(&block.object_key)?;
            let record = ObjectGcRecord {
                inode: object_owner,
                generation: object_generation,
                object_key: block.object_key.clone(),
                size: block.len,
                digest_uri: block.digest_uri.clone(),
                enqueue_version: version.get(),
                enqueue_unix_ms: current_time_ms(),
            };
            command.mutations.push(Mutation {
                family: RecordFamily::Gc,
                key: gc_object_key(
                    self.mount,
                    version.get(),
                    object_owner,
                    object_generation,
                    object_chunk,
                    block_index,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_object_gc_record(&record))),
            });
            match validate_restore_command_bounds(&command, "restore release member manifests") {
                Ok(()) => accepted += 1,
                Err(error) => {
                    command.mutations.pop();
                    if accepted == 0 {
                        return Err(error);
                    }
                    break;
                }
            }
        }

        let next_block_cursor = block_cursor.checked_add(accepted).ok_or_else(|| {
            MetadError::Codec("restore release manifest block cursor overflow".to_owned())
        })?;
        if next_block_cursor == release_blocks.len() {
            next_member.manifest_cursor.clear();
            next_member.manifest_block_cursor = 0;
        } else {
            next_member.manifest_cursor = row.key.clone();
            next_member.manifest_block_cursor = u64::try_from(next_block_cursor).map_err(|_| {
                MetadError::Codec("restore release manifest block cursor overflow".to_owned())
            })?;
            command.mutations.remove(1);
        }
        let encoded_next_member = encode_restore_staging_member(&next_member)?;
        command.mutations[0].value = Some(Value(encoded_next_member.clone()));
        validate_restore_command_bounds(&command, "restore release member manifests")?;
        match self.commit_metadata(command) {
            Ok(_) => {}
            Err(error @ MetadError::Metadata(MetadataError::Backend(_))) => {
                // A backend error may be a lost acknowledgement after the
                // atomic cursor+GC apply. Reconcile from the durable member and
                // manifest visibility instead of replaying or quarantining a
                // healthy release job. The member CAS is the transaction's
                // commit witness, so observing it also proves every GC row in
                // this page landed exactly once.
                let read_version = self.read_version()?;
                let member_matches = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &restore_staging_member_key(
                            self.mount,
                            operation.ref_set_id,
                            member.destination_inode,
                        ),
                        read_version,
                        ReadPurpose::RestoreStaging,
                    )?
                    .is_some_and(|value| value.0 == encoded_next_member);
                let manifest_exists = self
                    .metadata
                    .get(
                        RecordFamily::ChunkManifest,
                        &row.key,
                        read_version,
                        ReadPurpose::RestoreStaging,
                    )?
                    .is_some();
                let manifest_complete = next_block_cursor == release_blocks.len();
                if !member_matches || manifest_exists == manifest_complete {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn release_restore_member_metadata_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        _job: &RestoreReleaseJob,
        job_version: Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
        family: RecordFamily,
        prefix: Vec<u8>,
        request_prefix: &[u8],
    ) -> Result<bool, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family,
            prefix: prefix.clone(),
            start_after: None,
            version: self.read_version()?,
            limit: RESTORE_BATCH_ENTRIES,
            purpose: ReadPurpose::RestoreStaging,
        })?;
        if rows.is_empty() {
            return Ok(false);
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                request_prefix,
                self.mount,
                member.destination_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: family,
            primary_key: prefix,
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_release_job_key(self.mount, operation.ref_set_id),
                    predicate: Predicate::VersionEquals(job_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_member_key(
                        self.mount,
                        operation.ref_set_id,
                        member.destination_inode,
                    ),
                    predicate: Predicate::VersionEquals(member_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.to_vec(),
                    predicate: Predicate::VersionEquals(inverse_version),
                },
            ]
            .into_iter()
            .chain(rows.iter().map(|row| PredicateRef {
                family,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            }))
            .collect(),
            mutations: rows
                .into_iter()
                .map(|row| delete_mutation(family, row.key))
                .collect(),
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release member metadata")?;
        self.commit_metadata(command)?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn release_restore_member_dentry_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        _job: &RestoreReleaseJob,
        job_version: Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
    ) -> Result<bool, MetadError> {
        let version = self.read_version()?;
        let rows = self.restore_linked_dentry_projection_page(
            member.destination_inode,
            version,
            RESTORE_BATCH_ENTRIES,
        )?;
        if rows.is_empty() {
            return Ok(false);
        }
        let commit_version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-member-dentries",
                self.mount,
                member.destination_inode,
                commit_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Dentry,
            primary_key: rows[0].key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_release_job_key(self.mount, operation.ref_set_id),
                    predicate: Predicate::VersionEquals(job_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_member_key(
                        self.mount,
                        operation.ref_set_id,
                        member.destination_inode,
                    ),
                    predicate: Predicate::VersionEquals(member_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.to_vec(),
                    predicate: Predicate::VersionEquals(inverse_version),
                },
            ]
            .into_iter()
            .chain(rows.iter().map(|linked| PredicateRef {
                family: RecordFamily::Dentry,
                key: linked.key.clone(),
                predicate: Predicate::VersionEquals(linked.version),
            }))
            .collect(),
            mutations: rows
                .into_iter()
                .map(|linked| delete_mutation(RecordFamily::Dentry, linked.key))
                .collect(),
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release member dentries")?;
        self.commit_metadata(command)?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_restore_release_member(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &mut RestoreReleaseJob,
        job_version: &mut Version,
        member: &RestoreStagingMember,
        member_version: Version,
        inverse_key: &[u8],
        inverse_version: Version,
        inverse_owner_key: &[u8],
        inverse_owner_version: Version,
        reachable: bool,
    ) -> Result<(), MetadError> {
        if !member.manifest_cursor.is_empty() || member.manifest_block_cursor != 0 {
            return Err(MetadError::Codec(
                "restore release cannot finish a member with an active manifest cursor".to_owned(),
            ));
        }
        if self.restore_ref_set_has_base_owners(operation.ref_set_id)? {
            // Exact references are released in a separate paged pass. The
            // current member may already be detached and fully cleaned, but
            // keep its durable identity until every exact row is gone so fsck
            // can still validate borrower ownership. Advancing the cursor lets
            // other members make progress before the next exact-reference
            // sweep.
            job.cursor = restore_staging_member_key(
                self.mount,
                operation.ref_set_id,
                member.destination_inode,
            );
            let retained_inode_key = inode_key(self.mount, member.destination_inode);
            let read_version = self.read_version()?;
            let retained_inode = if reachable {
                None
            } else {
                self.metadata.get_versioned(
                    RecordFamily::Inode,
                    &retained_inode_key,
                    read_version,
                    ReadPurpose::RestoreStaging,
                )?
            };
            let version = self.next_version()?;
            let mut predicates = vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_release_job_key(self.mount, operation.ref_set_id),
                    predicate: Predicate::VersionEquals(*job_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: job.cursor.clone(),
                    predicate: Predicate::VersionEquals(member_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.to_vec(),
                    predicate: Predicate::VersionEquals(inverse_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_owner_key.to_vec(),
                    predicate: Predicate::VersionEquals(inverse_owner_version),
                },
            ];
            let mut mutations = vec![Mutation {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                op: MutationOp::Put,
                value: Some(Value(encode_restore_release_job(job)?)),
            }];
            if !reachable {
                predicates.extend([
                    PredicateRef {
                        family: RecordFamily::ChunkManifest,
                        key: retained_inode_key.clone(),
                        predicate: Predicate::PrefixEmpty,
                    },
                    PredicateRef {
                        family: RecordFamily::Xattr,
                        key: xattr_prefix(self.mount, member.destination_inode),
                        predicate: Predicate::PrefixEmpty,
                    },
                ]);
                match retained_inode {
                    Some(inode) => {
                        let attr = decode_inode_attr(&inode.value.0)
                            .map_err(|error| MetadError::Codec(error.to_string()))?;
                        if attr.inode != member.destination_inode {
                            return Err(MetadError::Codec(
                                "restore retained member inode changed identity".to_owned(),
                            ));
                        }
                        if attr.file_type == FileType::Directory {
                            predicates.push(PredicateRef {
                                family: RecordFamily::Dentry,
                                key: dentry_prefix(self.mount, member.destination_inode),
                                predicate: Predicate::PrefixEmpty,
                            });
                        }
                        predicates.push(PredicateRef {
                            family: RecordFamily::Inode,
                            key: retained_inode_key.clone(),
                            predicate: Predicate::VersionEquals(inode.version),
                        });
                        mutations.push(delete_mutation(
                            RecordFamily::Inode,
                            retained_inode_key.clone(),
                        ));
                    }
                    None => predicates.push(PredicateRef {
                        family: RecordFamily::Inode,
                        key: retained_inode_key,
                        predicate: Predicate::NotExists,
                    }),
                }
            }
            let command = MetadataCommand {
                request_id: request_id(
                    b"restore-retain-release-member",
                    self.mount,
                    member.destination_inode,
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: job.cursor.clone(),
                predicates,
                mutations,
                watch: Vec::new(),
            };
            validate_restore_command_bounds(&command, "restore retained member cursor")?;
            self.commit_metadata(command)?;
            *job_version = version;
            return Ok(());
        }
        let read_version = self.read_version()?;
        let member_inode_key = inode_key(self.mount, member.destination_inode);
        let inode = self.metadata.get_versioned(
            RecordFamily::Inode,
            &member_inode_key,
            read_version,
            ReadPurpose::RestoreStaging,
        )?;
        let inode_attr = inode
            .as_ref()
            .map(|item| {
                decode_inode_attr(&item.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))
            })
            .transpose()?;
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        job.cursor =
            restore_staging_member_key(self.mount, operation.ref_set_id, member.destination_inode);
        let member_key = job.cursor.clone();
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_release_job_key(self.mount, operation.ref_set_id),
                predicate: Predicate::VersionEquals(*job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: member_key.clone(),
                predicate: Predicate::VersionEquals(member_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: inverse_owner_key.to_vec(),
                predicate: Predicate::VersionEquals(inverse_owner_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_base_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
        ];
        let mut mutations = vec![Mutation {
            family: RecordFamily::System,
            key: restore_release_job_key(self.mount, operation.ref_set_id),
            op: MutationOp::Put,
            value: Some(Value(encode_restore_release_job(job)?)),
        }];
        if !reachable {
            mutations.extend([
                delete_mutation(RecordFamily::System, member_key),
                delete_mutation(RecordFamily::System, inverse_key.to_vec()),
                delete_mutation(RecordFamily::System, inverse_owner_key.to_vec()),
            ]);
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::ChunkManifest,
                    key: inode_key(self.mount, member.destination_inode),
                    predicate: Predicate::PrefixEmpty,
                },
                PredicateRef {
                    family: RecordFamily::Xattr,
                    key: xattr_prefix(self.mount, member.destination_inode),
                    predicate: Predicate::PrefixEmpty,
                },
            ]);
            if inode_attr
                .as_ref()
                .is_some_and(|attr| attr.file_type == FileType::Directory)
            {
                predicates.push(PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry_prefix(self.mount, member.destination_inode),
                    predicate: Predicate::PrefixEmpty,
                });
            }
            if let Some(inode) = inode {
                predicates.push(PredicateRef {
                    family: RecordFamily::Inode,
                    key: member_inode_key.clone(),
                    predicate: Predicate::VersionEquals(inode.version),
                });
                mutations.push(delete_mutation(RecordFamily::Inode, member_inode_key));
            } else {
                predicates.push(PredicateRef {
                    family: RecordFamily::Inode,
                    key: member_inode_key,
                    predicate: Predicate::NotExists,
                });
            }
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-member",
                self.mount,
                member.destination_inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: restore_staging_member_prefix(self.mount, operation.ref_set_id),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release member")?;
        self.commit_metadata(command)?;
        *job_version = version;
        Ok(())
    }

    fn finish_restore_release(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        _job: &RestoreReleaseJob,
        job_version: Version,
    ) -> Result<(), MetadError> {
        let read_version = self.read_version()?;
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let release_key = restore_release_job_key(self.mount, operation.ref_set_id);
        let claim_key = restore_destination_claim_key(self.mount, &operation.destination_path);
        let claim = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            })?;
        let root_key = restore_root_index_key(self.mount, operation.destination_root);
        let root = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &root_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            })?;
        if claim.value.0 != operation.operation_digest || root.value.0 != operation.operation_digest
        {
            return Err(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            });
        }
        let seal_key = restore_base_seal_key(self.mount, operation.ref_set_id);
        let seal = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &seal_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore release base-reference seal is missing".to_owned())
            })?;
        let decoded_seal = super::restore_gc::decode_restore_base_seal(&seal.value.0)?;
        if decoded_seal.operation_digest != operation.operation_digest
            || decoded_seal.initialization_digest != operation.initialization_digest
            || decoded_seal.ref_set_id != operation.ref_set_id
            || decoded_seal.incarnation != operation.created_version
        {
            return Err(MetadError::Codec(
                "restore release base-reference seal changed identity".to_owned(),
            ));
        }
        let cleanup_key = restore_cleanup_job_key(self.mount, operation.ref_set_id);
        let cleanup = self.metadata.get_versioned(
            RecordFamily::System,
            &cleanup_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        if cleanup
            .as_ref()
            .is_some_and(|item| item.value.0 != operation.operation_digest)
        {
            return Err(MetadError::Codec(
                "restore release cleanup marker changed identity".to_owned(),
            ));
        }
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: release_key.clone(),
                predicate: Predicate::VersionEquals(job_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: claim_key.clone(),
                predicate: Predicate::VersionEquals(claim.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: root_key.clone(),
                predicate: Predicate::VersionEquals(root.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: seal_key.clone(),
                predicate: Predicate::VersionEquals(seal.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_base_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_base_inverse_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_inverse_owner_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_init_upload_intent_prefix(self.mount, operation.ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
        ];
        match &cleanup {
            Some(item) => predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: cleanup_key.clone(),
                predicate: Predicate::VersionEquals(item.version),
            }),
            None => predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: cleanup_key.clone(),
                predicate: Predicate::NotExists,
            }),
        }
        predicates.extend(
            super::restore_index::restore_index_release_empty_predicates(
                self.mount,
                operation.ref_set_id,
            ),
        );
        let mut mutations = vec![
            delete_mutation(RecordFamily::System, operation_key.clone()),
            delete_mutation(RecordFamily::System, release_key),
            delete_mutation(RecordFamily::System, claim_key),
            delete_mutation(RecordFamily::System, root_key),
            delete_mutation(RecordFamily::System, seal_key),
        ];
        if cleanup.is_some() {
            mutations.push(delete_mutation(RecordFamily::System, cleanup_key));
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-finish-release",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: operation_key,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore finish release")?;
        match self.commit_metadata(command) {
            Ok(_) => {
                // This is the only successful release boundary that may return
                // the process to the fast path. Recovery proves globally that
                // no detached restore state remains; malformed state stays
                // fail closed.
                self.recover_restore_staging_visibility()?;
                Ok(())
            }
            Err(
                error @ MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                },
            ) => {
                // Metadata is already durable even though its archive ACK was
                // lost. There is no release-job row left to trigger another
                // worker pass, so reconcile the hint at this durable boundary
                // before preserving the caller-visible acknowledgement error.
                let _ = self.recover_restore_staging_visibility();
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Return true when any unexpired snapshot's pinned namespace contains an
    /// inode owned by this restore ref-set. Checking staging inverses rather
    /// than only `destination_root` also covers an escaped child that was
    /// renamed or hard-linked outside the restored root before root deletion.
    fn restore_live_snapshot_holds_ref_set(
        &self,
        operation: &RestoreOperation,
    ) -> Result<bool, MetadError> {
        const RETENTION_CONTROL_PAGE: usize = 64;

        let current_version = self.read_version()?;
        let now_ms = self.now_ms();
        let pin_prefix = snapshot_pin_prefix(self.mount);
        let mut pin_start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Snapshot,
                prefix: pin_prefix.clone(),
                start_after: pin_start_after.clone(),
                version: current_version,
                limit: RETENTION_CONTROL_PAGE,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let pin = decode_snapshot_pin(&row.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))?;
                if row.key != snapshot_pin_key(self.mount, pin.snapshot_id)
                    || pin.read_version == 0
                    || pin.created_version != pin.read_version.saturating_add(1)
                {
                    return Err(MetadError::Codec(
                        "restore release snapshot pin changed identity".to_owned(),
                    ));
                }
                self.ensure_snapshot_id_shard(pin.snapshot_id, pin.root)?;
                if now_ms >= pin.lease_expires_unix_ms {
                    continue;
                }
                if self.restore_snapshot_subtree_holds_ref_set(
                    pin.root,
                    Version::new(pin.read_version)?,
                    current_version,
                    operation,
                )? {
                    return Ok(true);
                }
            }
            pin_start_after = rows.last().map(|row| row.key.clone());
            if rows.len() < RETENTION_CONTROL_PAGE {
                break;
            }
        }

        if self.visit_versioned_fork_bindings_at(
            current_version,
            ReadPurpose::WritePlanLocal,
            |versioned| {
                // The restore's own temporary binding is removed atomically at
                // attach and must never make staging a namespace anchor. A binding
                // still carrying that exact fork-root identity here is corruption,
                // not a generic holder to silently accept.
                if versioned.binding.fork_root == operation.destination_root
                    && versioned.binding.source_root == operation.source_root
                    && versioned.binding.snapshot_id == operation.snapshot_id
                {
                    return Err(MetadError::Codec(
                        "releasing restore still has its temporary ForkBinding".to_owned(),
                    ));
                }
                self.restore_snapshot_subtree_holds_ref_set(
                    versioned.binding.source_root,
                    Version::new(versioned.binding.pinned_read_version)?,
                    current_version,
                    operation,
                )
            },
        )? {
            return Ok(true);
        }
        Ok(false)
    }

    fn restore_snapshot_subtree_holds_ref_set(
        &self,
        root: InodeId,
        snapshot_version: Version,
        current_version: Version,
        operation: &RestoreOperation,
    ) -> Result<bool, MetadError> {
        let mut pending = vec![root];
        let mut visited = HashSet::new();
        let mut entries = 0_usize;
        while let Some(inode) = pending.pop() {
            if !visited.insert(inode) {
                continue;
            }
            if let Some(inverse) = self.metadata.get(
                RecordFamily::System,
                &restore_staging_inode_key(self.mount, inode),
                current_version,
                ReadPurpose::WritePlanLocal,
            )? {
                let (digest, ref_set_id) = decode_restore_staging_inverse(&inverse.0)?;
                if digest == operation.operation_digest && ref_set_id == operation.ref_set_id {
                    return Ok(true);
                }
            }
            let Some(attr) = self.get_attr_at_version_for_purpose(
                inode,
                snapshot_version,
                ReadPurpose::Snapshot,
            )?
            else {
                return Err(MetadError::Codec(format!(
                    "live snapshot retention inode {} is missing",
                    inode.get()
                )));
            };
            if attr.file_type != FileType::Directory {
                continue;
            }
            let mut after = None;
            loop {
                let page = self.read_dir_plus_page_at_version_for_purpose(
                    inode,
                    after.as_ref(),
                    RESTORE_BATCH_ENTRIES,
                    snapshot_version,
                    ReadPurpose::Snapshot,
                )?;
                entries = entries.saturating_add(page.entries.len());
                if entries > MAX_RESTORE_SUBTREE_ENTRIES {
                    return Err(MetadError::RestoreResourceLimit {
                        resource: "restore release snapshot retention entries".to_owned(),
                        limit: MAX_RESTORE_SUBTREE_ENTRIES as u64,
                        actual: entries as u64,
                    });
                }
                for child in page.entries {
                    if let Some(inverse) = self.metadata.get(
                        RecordFamily::System,
                        &restore_staging_inode_key(self.mount, child.attr.inode),
                        current_version,
                        ReadPurpose::WritePlanLocal,
                    )? {
                        let (digest, ref_set_id) = decode_restore_staging_inverse(&inverse.0)?;
                        if digest == operation.operation_digest
                            && ref_set_id == operation.ref_set_id
                        {
                            return Ok(true);
                        }
                    }
                    if child.attr.file_type == FileType::Directory
                        && child.attr.inode.shard_index() == self.shard_index()
                    {
                        pending.push(child.attr.inode);
                    }
                }
                let Some(next) = page.next_cursor else {
                    break;
                };
                after = Some(next);
            }
        }
        Ok(false)
    }

    fn restore_borrower_references_object(
        &self,
        reference: &super::restore_gc::RestoreBaseReference,
        version: Version,
        reachable_bodies: &HashMap<InodeId, Option<BodyDescriptor>>,
    ) -> Result<bool, MetadError> {
        let Some(body) = reachable_bodies.get(&reference.borrower_inode) else {
            return Ok(false);
        };
        let Some(body) = body.as_ref() else {
            return Ok(false);
        };
        if body.chunk_size == 0 || body.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout.into());
        }
        if body.size == 0 {
            return Ok(false);
        }
        let (_, _, chunk_index, _) = self.canonical_block_object_identity(&reference.object_key)?;
        if chunk_index > (body.size - 1) / body.chunk_size {
            return Ok(false);
        }
        let chain = self.resolve_generation_chain(
            reference.borrower_inode,
            body,
            version,
            ReadPurpose::RestoreStaging,
        )?;
        let Some(manifest) = self.chain_chunk_manifest(
            reference.borrower_inode,
            &chain,
            chunk_index,
            version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(false);
        };
        if manifest.chunk_index != chunk_index {
            return Err(MetadError::Codec(
                "restore borrower manifest changed chunk identity".to_owned(),
            ));
        }
        let manifest_len = usize::try_from(manifest.len).map_err(|_| ObjectError::InvalidRange)?;
        let plan = plan_chunk_manifest_reads(
            std::slice::from_ref(&manifest),
            manifest.logical_offset,
            manifest_len,
        )?;
        if let Some(block) = plan
            .blocks
            .into_iter()
            .find(|block| block.object_key == reference.object_key)
        {
            if block.digest_uri != reference.digest_uri || block.object_len != reference.size {
                return Err(MetadError::Codec(
                    "restore borrower object identity changed".to_owned(),
                ));
            }
            return Ok(true);
        }
        // Historical holders are gated once per ref-set page by
        // `restore_live_snapshot_holds_ref_set` before this function runs. At
        // that point every borrower still has its staging inverse (member
        // release is a later phase), and snapshot/ForkBinding publication shares
        // `object_gc_gate` with this worker. Rewalking every historical subtree
        // here would turn release into O(exact refs × historical namespace)
        // without closing any additional race.
        Ok(false)
    }

    fn update_restore_release_job(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &RestoreReleaseJob,
        job_version: &mut Version,
    ) -> Result<(), MetadError> {
        let key = restore_release_job_key(self.mount, operation.ref_set_id);
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-update-release-job",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::VersionEquals(*job_version),
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_release_job(job)?)),
            }],
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release cursor")?;
        self.commit_metadata(command)?;
        *job_version = version;
        Ok(())
    }

    /// Publish the one-way cut after which historical retention no longer
    /// needs to walk namespace subtrees. The four ref-set-first owner prefixes
    /// make the proof durable in the same CAS as the phase transition; global
    /// inverse keyspaces remain fail-closed through their paired owner audits.
    fn transition_restore_release_to_overlay(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        job: &RestoreReleaseJob,
        job_version: &mut Version,
    ) -> Result<(), MetadError> {
        if job.phase != RestoreReleasePhase::Overlay || !job.cursor.is_empty() {
            return Err(MetadError::Codec(
                "restore release overlay transition has an invalid job state".to_owned(),
            ));
        }
        let key = restore_release_job_key(self.mount, operation.ref_set_id);
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-enter-release-overlay",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::VersionEquals(*job_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_member_prefix(self.mount, operation.ref_set_id),
                    predicate: Predicate::PrefixEmpty,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_staging_inverse_owner_prefix(self.mount, operation.ref_set_id),
                    predicate: Predicate::PrefixEmpty,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_base_owner_prefix(self.mount, operation.ref_set_id),
                    predicate: Predicate::PrefixEmpty,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_base_inverse_owner_prefix(self.mount, operation.ref_set_id),
                    predicate: Predicate::PrefixEmpty,
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_release_job(job)?)),
            }],
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore release overlay transition")?;
        self.commit_metadata(command)?;
        *job_version = version;
        Ok(())
    }

    fn attach_restore_destination(
        &self,
        operation_id: &str,
        operation: &RestoreOperation,
    ) -> Result<RestoreOutcome, MetadError> {
        validate_restore_operation_identity(self.mount, &operation.operation_digest, operation)?;
        let destination_components = parse_absolute_path(&operation.destination_path)?;
        let (destination_name, parent_components) = destination_components
            .split_last()
            .ok_or_else(|| MetadError::RestoreDestinationConflict {
                destination: operation.destination_path.clone(),
            })?;
        let destination_proof = self.restore_directory_path_proof(parent_components)?;
        if destination_proof.inode.shard_index() != self.shard_index {
            return Err(MetadError::RestoreCrossShardUnsupported {
                inode: destination_proof.inode,
            });
        }
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        let durable_operation = decode_restore_operation(&operation_item.value.0)?;
        validate_restore_operation_identity(
            self.mount,
            &operation.operation_digest,
            &durable_operation,
        )?;
        if durable_operation.state != RestoreOperationState::ReadyToAttach {
            return Err(MetadError::RestoreInProgress);
        }
        let mut expected_operation = operation.clone();
        expected_operation.state = RestoreOperationState::ReadyToAttach;
        if durable_operation != expected_operation {
            return Err(MetadError::Codec(
                "restore ReadyToAttach operation changed identity".to_owned(),
            ));
        }
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let binding_item = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        let binding = crate::layout::decode_fork_binding(&binding_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        if binding.fork_root != operation.destination_root
            || binding.source_root != operation.source_root
            || binding.pinned_read_version != operation.read_version
            || binding.snapshot_id != operation.snapshot_id
        {
            return Err(MetadError::Codec(
                "restore temporary ForkBinding changed identity".to_owned(),
            ));
        }
        let seal_key = restore_base_seal_key(self.mount, operation.ref_set_id);
        let seal_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &seal_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore base-reference seal is missing".to_owned())
            })?;
        let seal = super::restore_gc::decode_restore_base_seal(&seal_item.value.0)?;
        if seal.operation_digest != operation.operation_digest
            || seal.initialization_digest != operation.initialization_digest
            || seal.ref_set_id != operation.ref_set_id
            || seal.incarnation != operation.created_version
        {
            return Err(MetadError::Codec(
                "restore base-reference seal changed identity".to_owned(),
            ));
        }
        let base_seal_predicate = self.restore_base_seal_predicate(operation, read_version)?;
        let index_seal_predicate = self.restore_index_seal_predicate(operation, read_version)?;
        let claim_key = restore_destination_claim_key(self.mount, &operation.destination_path);
        let claim = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore destination claim is missing".to_owned()))?;
        if claim.value.0 != operation.operation_digest {
            return Err(MetadError::RestoreDestinationConflict {
                destination: operation.destination_path.clone(),
            });
        }
        let root_index_key = restore_root_index_key(self.mount, operation.destination_root);
        let root_index = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &root_index_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore root index is missing".to_owned()))?;
        if root_index.value.0 != operation.operation_digest {
            return Err(MetadError::Codec(
                "restore root index changed identity".to_owned(),
            ));
        }
        let mut root_attr = self
            .get_attr_at_version_for_purpose(
                operation.destination_root,
                read_version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::NotFound)?;
        root_attr.ctime_ms = current_time_ms();
        let projection = projection(
            destination_proof.inode,
            destination_name.clone(),
            root_attr,
            None,
        );
        let dentry = dentry_key(self.mount, destination_proof.inode, destination_name);
        let cleanup_job = restore_cleanup_job_key(self.mount, operation.ref_set_id);
        let object_reference = self.begin_object_reference_mutation()?;
        let index_visibility = self.restore_index_complete_plan(operation, version)?;
        let mut predicates = destination_proof.predicates;
        predicates.extend(
            self.restore_destination_parent_predicates(destination_proof.inode, read_version)?,
        );
        predicates.extend([
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(operation_item.version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.clone(),
                predicate: Predicate::VersionEquals(binding_item.version),
            },
            base_seal_predicate,
            index_seal_predicate,
            PredicateRef {
                family: RecordFamily::System,
                key: claim_key,
                predicate: Predicate::VersionEquals(claim.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: root_index_key,
                predicate: Predicate::VersionEquals(root_index.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: cleanup_job.clone(),
                predicate: Predicate::NotExists,
            },
        ]);
        predicates.extend(index_visibility.predicates);
        let mut complete = durable_operation;
        complete.state = RestoreOperationState::Complete;
        let mut mutations = vec![
            put_projection_mutation(RecordFamily::Dentry, dentry.clone(), &projection),
            Mutation {
                family: RecordFamily::System,
                key: operation_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_operation(&complete)?)),
            },
            delete_mutation(RecordFamily::ForkBinding, binding_key),
        ];
        mutations.extend(index_visibility.mutations);
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-attach-destination",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CreateDir,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    projection.dentry.parent,
                    WatchEvent {
                        kind: WatchEventKind::Create,
                        parent: Some(projection.dentry.parent),
                        name: Some(projection.dentry.name.clone()),
                        inode: operation.destination_root,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        validate_restore_command_bounds(&command, "restore final attach")?;
        match self.commit_metadata(command) {
            Ok(_) => {
                // Reconcile once at the durable state transition, before a
                // crash barrier can interrupt the acknowledgement path.
                // Exact terminal retries stay O(1).
                self.recover_restore_staging_visibility()?;
                live_test_barrier::restore_applied(
                    operation_id,
                    live_test_barrier::RestoreAppliedPhase::Attach,
                )?;
                self.completed_restore_outcome(operation_id, &complete)
            }
            Err(
                error @ MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                },
            ) => {
                let terminal = self.restore_operation(operation.operation_digest)?;
                match terminal {
                    Some(terminal) if terminal.state == RestoreOperationState::Complete => {
                        self.recover_restore_staging_visibility()?;
                        // The local attach applied, but its archived log tail was
                        // not proven in the control plane. Preserve the committed
                        // publication error and do not expose the post-publication
                        // crash barrier. An exact retry observes Complete after
                        // the server repairs the recovery reference.
                        Err(error)
                    }
                    _ => Err(MetadError::RestoreInProgress),
                }
            }
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => self
                .reprove_restore_attach_predicate_failure(
                    operation_id,
                    operation,
                    object_reference,
                ),
            Err(error) => Err(error),
        }
    }

    /// Classify a failed final-attach CAS from a bounded set of durable facts.
    ///
    /// The destination dentry is the only public namespace mutation in the
    /// command. Re-reading it after checking the terminal operation turns a
    /// concurrent occupation into the public restore conflict instead of
    /// leaking the storage engine's generic predicate failure. Other races
    /// remain retryable and are resolved by an exact request retry.
    fn reprove_restore_attach_predicate_failure(
        &self,
        operation_id: &str,
        operation: &RestoreOperation,
        object_reference: ObjectReferenceMutation,
    ) -> Result<RestoreOutcome, MetadError> {
        let Some(durable_operation) = self.restore_operation(operation.operation_digest)? else {
            return Err(MetadError::RestoreInProgress);
        };
        match durable_operation.state {
            RestoreOperationState::Complete => {
                self.recover_restore_staging_visibility()?;
                return self.completed_restore_outcome(operation_id, &durable_operation);
            }
            RestoreOperationState::ReadyToAttach => {
                let mut expected_operation = operation.clone();
                expected_operation.state = RestoreOperationState::ReadyToAttach;
                if durable_operation != expected_operation {
                    return Err(MetadError::Codec(
                        "restore ReadyToAttach operation changed identity".to_owned(),
                    ));
                }
            }
            RestoreOperationState::Preparing
            | RestoreOperationState::Cleaning
            | RestoreOperationState::Discarding
            | RestoreOperationState::Releasing => {
                return Err(MetadError::RestoreInProgress);
            }
        }

        let claim_key = restore_destination_claim_key(self.mount, &operation.destination_path);
        match self.metadata.get(
            RecordFamily::System,
            &claim_key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )? {
            Some(claim) if claim.0 == operation.operation_digest => {}
            Some(_) => {
                return Err(MetadError::RestoreDestinationConflict {
                    destination: operation.destination_path.clone(),
                });
            }
            None => {
                return Err(MetadError::Codec(
                    "restore destination claim is missing".to_owned(),
                ));
            }
        }

        if self.lookup_path(&operation.destination_path)?.is_some() {
            return Err(MetadError::RestoreDestinationConflict {
                destination: operation.destination_path.clone(),
            });
        }

        let current_object_reference = match self.begin_object_reference_mutation() {
            Ok(reference) => reference,
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                return Err(MetadError::RestoreInProgress);
            }
            Err(error) => return Err(error),
        };
        if current_object_reference.version() != object_reference.version() {
            return Err(MetadError::StalePreparedArtifactObjectGcEpoch {
                expected: object_reference.version().get(),
                current: current_object_reference.version().get(),
            });
        }

        Err(MetadError::RestoreInProgress)
    }
}

pub(super) fn validate_restore_command_bounds(
    command: &MetadataCommand,
    resource: &str,
) -> Result<(), MetadError> {
    const MAX_ITEMS: usize = 4096;
    const MAX_BYTES: usize = 8 * 1024 * 1024;
    let items = command
        .predicates
        .len()
        .saturating_add(command.mutations.len())
        .saturating_add(command.watch.len());
    if items > MAX_ITEMS {
        return Err(MetadError::RestoreResourceLimit {
            resource: format!("{resource} items"),
            limit: MAX_ITEMS as u64,
            actual: items as u64,
        });
    }
    // Bound the durable command encoding, including its three collection
    // counts and watch payloads. Predicate rows use the largest on-wire form
    // (`VersionEquals`), while mutation rows include the optional value length
    // prefix even for deletes. The estimate is therefore conservative but can
    // never admit a log command larger than the advertised limit.
    let bytes = 58_usize
        .saturating_add(command.request_id.len())
        .saturating_add(command.primary_key.len())
        .saturating_add(command.predicates.iter().fold(0_usize, |total, predicate| {
            total.saturating_add(predicate.key.len()).saturating_add(18)
        }))
        .saturating_add(command.mutations.iter().fold(0_usize, |total, mutation| {
            total
                .saturating_add(mutation.key.len())
                .saturating_add(mutation.value.as_ref().map_or(0, |value| value.0.len()))
                .saturating_add(19)
        }))
        .saturating_add(command.watch.iter().fold(0_usize, |total, watch| {
            total
                .saturating_add(watch.key.len())
                .saturating_add(watch.event.len())
                .saturating_add(17)
        }));
    if bytes > MAX_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: format!("{resource} bytes"),
            limit: MAX_BYTES as u64,
            actual: bytes as u64,
        });
    }
    Ok(())
}

/// Only an existing staging-inverse row opts the merged ordinary namespace
/// command into the restore-specific command budget. Before the first restore,
/// ordinary commands carry an allocator `VersionEquals` activation fence; that
/// global predicate is not restore membership.
pub(super) fn restore_write_predicates_include_owner(predicates: &[PredicateRef]) -> bool {
    predicates.iter().any(|predicate| {
        predicate.family == RecordFamily::System
            && matches!(predicate.predicate, Predicate::VersionEquals(_))
            && predicate.key.len() == 8 + STAGING_INVERSE_KEY_LABEL.len() + 8
            && predicate.key[8..].starts_with(STAGING_INVERSE_KEY_LABEL)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holtstore::HoltMetadataStore;
    use nokv_object::MemoryObjectStore;

    fn mount(raw: u64) -> MountId {
        MountId::new(raw).unwrap()
    }

    fn operation() -> RestoreOperation {
        RestoreOperation {
            operation_digest: [7; 32],
            initialization_digest: [8; 32],
            state: RestoreOperationState::Preparing,
            source_root: InodeId::new(10).unwrap(),
            destination_root: InodeId::new(20).unwrap(),
            snapshot_id: 30,
            read_version: 40,
            created_version: 50,
            ref_set_id: 50,
            source_path: "/workbenches/source".to_owned(),
            destination_path: "/workbenches/destination".to_owned(),
        }
    }

    #[test]
    fn restore_operation_codec_round_trips_and_fails_closed() {
        let operation = operation();
        let encoded = encode_restore_operation(&operation).unwrap();
        assert_eq!(decode_restore_operation(&encoded).unwrap(), operation);
        assert!(decode_restore_operation(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_restore_operation(&trailing).is_err());
    }

    #[test]
    fn restore_release_worker_cursor_codec_is_strict_and_mount_scoped() {
        let mount_id = mount(1);
        let cursor = RestoreReleaseWorkerCursor {
            cycle_high_water: 50,
            start_after: restore_release_job_key(mount_id, 7),
        };
        let encoded = encode_restore_release_worker_cursor(mount_id, &cursor).unwrap();
        assert_eq!(
            decode_restore_release_worker_cursor(mount_id, &encoded).unwrap(),
            cursor
        );
        assert!(decode_restore_release_worker_cursor(mount(2), &encoded).is_err());
        assert!(
            decode_restore_release_worker_cursor(mount_id, &encoded[..encoded.len() - 1]).is_err()
        );

        let mut invalid_magic = encoded.clone();
        invalid_magic[0] ^= 0xff;
        assert!(decode_restore_release_worker_cursor(mount_id, &invalid_magic).is_err());
        let mut invalid_version = encoded.clone();
        invalid_version[8] = RESTORE_FORMAT_VERSION.saturating_add(1);
        assert!(decode_restore_release_worker_cursor(mount_id, &invalid_version).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_restore_release_worker_cursor(mount_id, &trailing).is_err());
        assert!(encode_restore_release_worker_cursor(
            mount_id,
            &RestoreReleaseWorkerCursor {
                cycle_high_water: 0,
                start_after: Vec::new(),
            },
        )
        .is_err());
        assert!(encode_restore_release_worker_cursor(
            mount_id,
            &RestoreReleaseWorkerCursor {
                cycle_high_water: 1,
                start_after: vec![b'x'; MAX_RESTORE_PATH_BYTES + 1],
            },
        )
        .is_err());
        let mut malformed_job_key = restore_release_job_prefix(mount_id);
        malformed_job_key.push(1);
        assert!(encode_restore_release_worker_cursor(
            mount_id,
            &RestoreReleaseWorkerCursor {
                cycle_high_water: 1,
                start_after: malformed_job_key,
            },
        )
        .is_err());
    }

    #[test]
    fn restore_staging_member_codec_preserves_manifest_release_cursor() {
        let operation = operation();
        let member = RestoreStagingMember {
            operation_digest: operation.operation_digest,
            source_inode: Some(operation.source_root),
            destination_inode: operation.destination_root,
            destination_parent: None,
            name: None,
            relative_path: String::new(),
            canonical_index_cursor: Vec::new(),
            canonical_index_complete: true,
            manifest_cursor: vec![9; 32],
            manifest_block_cursor: 4_087,
        };
        let encoded = encode_restore_staging_member(&member).unwrap();
        assert_eq!(decode_restore_staging_member(&encoded).unwrap(), member);
        assert!(decode_restore_staging_member(&encoded[..encoded.len() - 1]).is_err());

        let mut invalid = member.clone();
        invalid.manifest_cursor.clear();
        assert!(encode_restore_staging_member(&invalid).is_err());
        invalid.manifest_block_cursor = 0;
        invalid.manifest_cursor = vec![1];
        assert!(encode_restore_staging_member(&invalid).is_err());
    }

    #[test]
    fn operation_id_is_mount_scoped_and_canonical() {
        let first = restore_operation_id(mount(1), "/a/", 7, "/b").unwrap();
        let canonical = restore_operation_id(mount(1), "/a", 7, "/b").unwrap();
        let other_mount = restore_operation_id(mount(2), "/a", 7, "/b").unwrap();
        assert_eq!(first, canonical);
        assert_ne!(first, other_mount);
    }

    #[test]
    fn private_restore_keys_are_mount_scoped_and_disjoint() {
        let digest = [9; 32];
        let operation = restore_operation_key(mount(1), &digest);
        let claim = restore_destination_claim_key(mount(1), "/dst");
        let borrower = InodeId::new(11).unwrap();
        let owner = restore_base_owner_key(mount(1), 7, &digest, borrower, 13);
        let inverse = restore_base_inverse_key(mount(1), &digest, 7, borrower, 13);
        assert_ne!(operation, claim);
        assert_ne!(owner, inverse);
        assert_ne!(operation, restore_operation_key(mount(2), &digest));
    }

    #[test]
    fn restore_graph_command_classifier_covers_every_private_keyspace() {
        let mount_id = mount(1);
        let command = |mutations: Vec<Mutation>| MetadataCommand {
            request_id: b"restore-graph-classifier".to_vec(),
            kind: CommandKind::CleanupObjects,
            read_version: Version::new(1).unwrap(),
            commit_version: Version::new(2).unwrap(),
            primary_family: mutations
                .first()
                .map_or(RecordFamily::System, |mutation| mutation.family),
            primary_key: mutations
                .first()
                .map_or_else(|| b"none".to_vec(), |mutation| mutation.key.clone()),
            predicates: Vec::new(),
            mutations,
            watch: Vec::new(),
        };
        let put = |family, key| Mutation {
            family,
            key,
            op: MutationOp::Put,
            value: Some(Value(vec![1])),
        };

        for key in [
            restore_active_key(mount_id),
            restore_activation_fence_key(mount_id),
        ] {
            assert!(command_mutates_restore_graph(
                mount_id,
                &command(vec![put(RecordFamily::System, key)])
            ));
        }
        for (name, prefix) in restore_control_keyspaces(mount_id) {
            if matches!(name, "init_upload_tombstone_cursor" | "release_cursor") {
                assert!(!command_mutates_restore_graph(
                    mount_id,
                    &command(vec![put(RecordFamily::System, prefix.clone())])
                ));
            }
            let mut suffixed = prefix;
            suffixed.push(1);
            assert!(command_mutates_restore_graph(
                mount_id,
                &command(vec![put(RecordFamily::System, suffixed)])
            ));
        }
        for (_, mut prefix) in super::restore_index::restore_index_private_keyspaces(mount_id) {
            prefix.push(1);
            assert!(command_mutates_restore_graph(
                mount_id,
                &command(vec![put(RecordFamily::System, prefix)])
            ));
        }

        assert!(!command_mutates_restore_graph(
            mount_id,
            &command(vec![put(RecordFamily::System, allocator_key(mount_id))])
        ));
        assert!(!command_mutates_restore_graph(
            mount_id,
            &command(vec![put(
                RecordFamily::System,
                restore_operation_key(mount(2), &[4; 32]),
            )])
        ));
        assert!(!command_mutates_restore_graph(
            mount_id,
            &command(vec![put(
                RecordFamily::Dentry,
                restore_active_key(mount_id),
            )])
        ));
        assert!(command_mutates_restore_graph(
            mount_id,
            &command(vec![
                put(
                    RecordFamily::System,
                    restore_init_upload_tombstone_cursor_key(mount_id),
                ),
                put(RecordFamily::System, restore_active_key(mount_id)),
            ])
        ));
    }

    #[test]
    fn materialization_preflight_counts_the_complete_sixty_four_entry_command() {
        let service = NoKvFs::new(
            mount(1),
            HoltMetadataStore::open_memory().unwrap(),
            MemoryObjectStore::new(),
        );
        let operation = operation();
        let entries = (0_u64..RESTORE_BATCH_ENTRIES as u64)
            .map(|index| {
                let inode = InodeId::new(100 + index).unwrap();
                let generation = 1_000 + index;
                let name = DentryName::new(format!("file-{index:02}")).unwrap();
                let body = BodyDescriptor {
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:test".to_owned(),
                    size: 60 * DEFAULT_CHUNK_SIZE,
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: format!("manifest-{index}"),
                    generation,
                    base_generation: 0,
                    chunk_size: DEFAULT_CHUNK_SIZE,
                    block_size: DEFAULT_BLOCK_SIZE as u64,
                };
                let chunks = (0_u64..60)
                    .map(|chunk_index| ChunkManifest {
                        chunk_index,
                        logical_offset: chunk_index * DEFAULT_CHUNK_SIZE,
                        len: DEFAULT_CHUNK_SIZE,
                        slices: Vec::new(),
                    })
                    .collect();
                RestoreCloneEntry {
                    source: DentryWithAttr {
                        dentry: DentryRecord {
                            parent: InodeId::root(),
                            name: name.clone(),
                            child: inode,
                            child_type: FileType::File,
                            attr_generation: generation,
                        },
                        attr: InodeAttr {
                            inode,
                            file_type: FileType::File,
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                            rdev: 0,
                            nlink: 1,
                            size: body.size,
                            generation,
                            mtime_ms: 0,
                            ctime_ms: 0,
                        },
                        body: Some(body.clone()),
                    },
                    destination: inode,
                    relative_path: String::from_utf8_lossy(name.as_bytes()).into_owned(),
                    body: Some(body),
                    chunks,
                }
            })
            .collect::<Vec<_>>();
        let version = Version::new(60).unwrap();
        let object_reference = ObjectReferenceMutation::from_version(Version::new(1).unwrap());

        let single = service
            .build_restore_children_command(
                &operation,
                InodeId::root(),
                std::slice::from_ref(&entries[0]),
                object_reference,
                version,
            )
            .unwrap();
        validate_restore_command_bounds(&single, "restore materialization batch").unwrap();

        let aggregate = service
            .build_restore_children_command(
                &operation,
                InodeId::root(),
                &entries,
                object_reference,
                version,
            )
            .unwrap();
        assert!(matches!(
            validate_restore_command_bounds(&aggregate, "restore materialization batch"),
            Err(MetadError::RestoreResourceLimit { resource, .. })
                if resource == "restore materialization batch items"
        ));
    }

    #[test]
    fn restore_command_byte_limit_includes_watch_payloads() {
        let version = Version::new(2).unwrap();
        let command = MetadataCommand {
            request_id: b"restore-watch-bound".to_vec(),
            kind: CommandKind::CreateDir,
            read_version: Version::new(1).unwrap(),
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: vec![0; 32],
            predicates: Vec::new(),
            mutations: Vec::new(),
            watch: vec![WatchProjection {
                family: RecordFamily::Watch,
                key: vec![0; 32],
                event: vec![0; 8 * 1024 * 1024],
            }],
        };

        assert!(matches!(
            validate_restore_command_bounds(&command, "restore watch bound"),
            Err(MetadError::RestoreResourceLimit { resource, .. })
                if resource == "restore watch bound bytes"
        ));
    }
}
