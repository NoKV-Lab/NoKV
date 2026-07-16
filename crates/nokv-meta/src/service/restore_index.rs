//! Visibility fences and operation-scoped indexes for detached restores.
//!
//! Restore indexes deliberately live in the private `System` family.  A row is
//! useful only while its owning operation is durably `Complete`; writing rows
//! during detached materialization therefore cannot make the destination
//! visible early.  The owner-side keyspace makes release page-able, while the
//! inverse keyspaces let normal namespace mutations find every nested restore
//! that owns an index row for a dentry.

use super::restore::{
    decode_restore_operation, decode_restore_staging_inverse, decode_restore_staging_member,
    encode_restore_staging_member, restore_operation_key, restore_staging_inode_key,
    restore_staging_member_key, restore_staging_member_prefix, RestoreOperation,
    RestoreOperationState, RestoreStagingMember, MAX_RESTORE_SUBTREE_ENTRIES,
    RESTORE_BATCH_ENTRIES, RESTORE_FORMAT_VERSION,
};
use super::*;

const INDEX_ENTRY_MAGIC: &[u8; 8] = b"NKRIDXE\0";
const INDEX_CATALOG_MAGIC: &[u8; 8] = b"NKRIDXC\0";
const INDEX_ROW_MAGIC: &[u8; 8] = b"NKRIDXR\0";
const INDEX_SEAL_MAGIC: &[u8; 8] = b"NKRIDXS\0";
const INDEX_MVCC_MAGIC: &[u8; 8] = b"NKRIDXM\0";
const INDEX_COMPLETE_MAGIC: &[u8; 8] = b"NKRIDXP\0";
const INDEX_SOURCE_MEMBER_MAGIC: &[u8; 8] = b"NKRIDXI\0";

const INDEX_ENTRY_LABEL: &[u8] = b"restore-index-entry\0";
const INDEX_PARENT_OWNER_LABEL: &[u8] = b"restore-index-parent-owner\0";
const INDEX_PARENT_INVERSE_LABEL: &[u8] = b"restore-index-parent-inverse\0";
const INDEX_CATALOG_LABEL: &[u8] = b"restore-index-catalog\0";
const INDEX_CATALOG_INVERSE_LABEL: &[u8] = b"restore-index-catalog-inverse\0";
const INDEX_ROW_LABEL: &[u8] = b"restore-index-row\0";
const INDEX_TARGET_INVERSE_LABEL: &[u8] = b"restore-index-target-inverse\0";
const INDEX_SEAL_LABEL: &[u8] = b"restore-index-seal\0";
const INDEX_COMPLETE_LABEL: &[u8] = b"restore-index-complete\0";
const INDEX_SOURCE_MEMBER_LABEL: &[u8] = b"restore-index-source-member\0";

// `RecordFamily::System` intentionally has no Holt history.  Every mutable
// restore-index head therefore has an append-only physical history keyspace.
// The physical key mirrors the logical key layout and appends the metadata
// command's commit version.  This keeps parent/target and ref-set scans
// bounded without enabling history for unrelated System records.
const INDEX_MVCC_ENTRY_LABEL: &[u8] = b"restore-index-mvcc-entry\0";
const INDEX_MVCC_PARENT_OWNER_LABEL: &[u8] = b"restore-index-mvcc-parent-owner\0";
const INDEX_MVCC_PARENT_INVERSE_LABEL: &[u8] = b"restore-index-mvcc-parent-inverse\0";
const INDEX_MVCC_CATALOG_LABEL: &[u8] = b"restore-index-mvcc-catalog\0";
const INDEX_MVCC_CATALOG_INVERSE_LABEL: &[u8] = b"restore-index-mvcc-catalog-inverse\0";
const INDEX_MVCC_ROW_LABEL: &[u8] = b"restore-index-mvcc-row\0";
const INDEX_MVCC_TARGET_INVERSE_LABEL: &[u8] = b"restore-index-mvcc-target-inverse\0";
const INDEX_MVCC_SOURCE_MEMBER_LABEL: &[u8] = b"restore-index-mvcc-source-member\0";

const OWNER_BYTES: usize = 40;
const RESTORE_INDEX_SCAN_PAGE: usize = 256;
// The namespace page helper asks the backing store for `requested + 1` rows to
// determine whether a next cursor exists. Keep that physical scan within the
// same 256-row bound as every private restore-index scan.
const RESTORE_INDEX_DENTRY_PAGE: usize = RESTORE_INDEX_SCAN_PAGE - 1;
// Keep this in lock-step with `restore::validate_restore_command_bounds`.
// Overlay plans are merged into an ordinary namespace command, so they must
// fail before the caller can publish a namespace mutation that cannot carry
// the complete owner/inverse/MVCC update atomically.
const MAX_RESTORE_INDEX_PLAN_ITEMS: usize = 4_096;
const MAX_RESTORE_INDEX_PLAN_BYTES: usize = 8 * 1024 * 1024;
const RESTORE_INDEX_MVCC_VERSION_DELIMITER: u8 = 0;
const MAX_RESTORE_INDEX_INSPECTION_REF_SETS: usize = 65_536;
const MAX_RESTORE_INDEX_INSPECTION_ISSUES: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RestoreIndexOwner {
    operation_digest: [u8; 32],
    ref_set_id: u64,
}

impl RestoreIndexOwner {
    fn from_operation(operation: &RestoreOperation) -> Self {
        Self {
            operation_digest: operation.operation_digest,
            ref_set_id: operation.ref_set_id,
        }
    }

    fn validate(self) -> Result<Self, MetadError> {
        if self.ref_set_id == 0 {
            return Err(MetadError::Codec(
                "restore index owner has a zero ref-set id".to_owned(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexEntry {
    owner: RestoreIndexOwner,
    projection: DentryProjection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexCatalog {
    owner: RestoreIndexOwner,
    catalog_root: InodeId,
    record: PathIndexCatalogRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexTarget {
    parent: Option<InodeId>,
    name: Vec<u8>,
    inode: InodeId,
    file_type: FileType,
    attr_generation: u64,
    body_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexRow {
    owner: RestoreIndexOwner,
    catalog_root: InodeId,
    target: RestoreIndexTarget,
    record: PathIndexRowRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexSourceMember {
    owner: RestoreIndexOwner,
    source_inode: InodeId,
    member: RestoreStagingMember,
}

/// Source-member rows seal the immutable snapshot-to-destination identity.
/// Release may advance only the live staging member's manifest cursor; that
/// progress must not invalidate fsck or prevent retaining a reachable source
/// row. Canonical-index cleanup remains exact because it has different
/// enrollment semantics and is never legal for snapshot source members.
fn restore_index_source_member_matches_staging(
    source: &RestoreStagingMember,
    staging: &RestoreStagingMember,
) -> bool {
    source.manifest_cursor.is_empty()
        && source.manifest_block_cursor == 0
        && source.operation_digest == staging.operation_digest
        && source.source_inode == staging.source_inode
        && source.destination_inode == staging.destination_inode
        && source.destination_parent == staging.destination_parent
        && source.name == staging.name
        && source.relative_path == staging.relative_path
        && source.canonical_index_cursor == staging.canonical_index_cursor
        && source.canonical_index_complete == staging.canonical_index_complete
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreIndexMvccKind {
    Put = 1,
    Tombstone = 2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexMvccRecord {
    commit_version: u64,
    kind: RestoreIndexMvccKind,
    logical_key: Vec<u8>,
    /// A tombstone retains the last full typed value.  Release/fsck can thus
    /// still prove owner/inverse identity instead of trusting only a digest.
    value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RestoreIndexCompleteMarker {
    operation_digest: [u8; 32],
    initialization_digest: [u8; 32],
    ref_set_id: u64,
    incarnation: u64,
    complete_version: u64,
}

#[derive(Clone, Debug)]
struct SelectedRestoreIndexMvcc {
    logical_key: Vec<u8>,
    record: RestoreIndexMvccRecord,
}

#[derive(Clone, Debug)]
struct RestoreIndexForkSource {
    binding: ForkBinding,
    operation: RestoreOperation,
}

type RestoreIndexCounterpart = (&'static [u8], Vec<u8>);

struct RestoreIndexInspectedRowIdentity {
    ref_set_id: u64,
    operation_digest: [u8; 32],
    is_put: bool,
    is_tombstone: bool,
    seal_identity: Option<RestoreIndexDurableIdentity>,
    complete_identity: Option<RestoreIndexDurableIdentity>,
}

#[derive(Default)]
struct RestoreIndexInspectionIssueBudget {
    emitted: usize,
    truncated: bool,
}

impl RestoreIndexInspectionIssueBudget {
    fn push(&mut self, issues: &mut Vec<String>, issue: String) {
        if self.emitted < MAX_RESTORE_INDEX_INSPECTION_ISSUES {
            issues.push(issue);
            self.emitted += 1;
        } else {
            self.truncated = true;
        }
    }
}

struct RestoreIndexReleasePage<'a> {
    operation: &'a RestoreOperation,
    operation_version: Version,
    stage: RestoreIndexReleaseStage,
    logical_prefix: Vec<u8>,
    start_after: Option<Vec<u8>>,
    limit: usize,
    version: Version,
}

#[derive(Clone, Copy)]
struct RestoreIndexChildrenQuery<'a> {
    parent: InodeId,
    after: Option<&'a DentryName>,
    keep: usize,
    version: Version,
    purpose: ReadPurpose,
}

#[derive(Clone, Debug)]
struct VersionedRestoreIndexEntry {
    key: Vec<u8>,
    version: Version,
    entry: RestoreIndexEntry,
    operation_key: Vec<u8>,
    operation_version: Version,
}

#[derive(Clone, Debug)]
struct VersionedRestoreIndexRow {
    owner_key: Vec<u8>,
    owner_version: Version,
    inverse_key: Vec<u8>,
    inverse_version: Version,
    row: RestoreIndexRow,
    operation_key: Vec<u8>,
    operation_version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreIndexSeal {
    pub(super) operation_digest: [u8; 32],
    pub(super) initialization_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) incarnation: u64,
    pub(super) entry_count: u64,
    pub(super) catalog_count: u64,
    pub(super) row_count: u64,
    pub(super) digest: [u8; 32],
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct RestoreIndexMutationPlan {
    pub(super) predicates: Vec<PredicateRef>,
    pub(super) mutations: Vec<Mutation>,
}

#[derive(Default)]
pub(super) struct RestoreCanonicalIndexPreflightBatch {
    members: usize,
    desired: Vec<Mutation>,
}

impl RestoreIndexMutationPlan {
    pub(super) fn extend(&mut self, other: Self) -> Result<(), MetadError> {
        for predicate in other.predicates {
            self.push_predicate(predicate)?;
        }
        for mutation in other.mutations {
            self.set_mutation(mutation);
        }
        self.validate_budget("restore index mutation plan")
    }

    fn push_predicate(&mut self, predicate: PredicateRef) -> Result<(), MetadError> {
        if let Some(existing) = self
            .predicates
            .iter()
            .find(|existing| existing.family == predicate.family && existing.key == predicate.key)
        {
            if existing.predicate != predicate.predicate {
                return Err(MetadError::Codec(
                    "restore index mutation plan has conflicting predicates".to_owned(),
                ));
            }
            return Ok(());
        }
        self.predicates.push(predicate);
        Ok(())
    }

    fn set_mutation(&mut self, mutation: Mutation) {
        if let Some(existing) = self
            .mutations
            .iter_mut()
            .find(|existing| existing.family == mutation.family && existing.key == mutation.key)
        {
            *existing = mutation;
        } else {
            self.mutations.push(mutation);
        }
    }

    fn validate_budget(&self, resource: &str) -> Result<(), MetadError> {
        let items = self.predicates.len().saturating_add(self.mutations.len());
        if items > MAX_RESTORE_INDEX_PLAN_ITEMS {
            return Err(MetadError::RestoreResourceLimit {
                resource: format!("{resource} items"),
                limit: MAX_RESTORE_INDEX_PLAN_ITEMS as u64,
                actual: items as u64,
            });
        }
        let bytes = self
            .predicates
            .iter()
            .fold(0_usize, |total, predicate| {
                total.saturating_add(predicate.key.len()).saturating_add(32)
            })
            .saturating_add(self.mutations.iter().fold(0_usize, |total, mutation| {
                total
                    .saturating_add(mutation.key.len())
                    .saturating_add(mutation.value.as_ref().map_or(0, |value| value.0.len()))
                    .saturating_add(16)
            }));
        if bytes > MAX_RESTORE_INDEX_PLAN_BYTES {
            return Err(MetadError::RestoreResourceLimit {
                resource: format!("{resource} bytes"),
                limit: MAX_RESTORE_INDEX_PLAN_BYTES as u64,
                actual: bytes as u64,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreCustomIndex {
    pub(super) catalog: PathIndexCatalogRecord,
    pub(super) rows: Vec<PathIndexRowRecord>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct RestoreIndexReleaseOutcome {
    /// Opaque cursor to persist in the restore release job. An empty cursor
    /// means a retained escaped row requires a later full pass.
    pub(super) cursor: Vec<u8>,
    /// True only after every owner/inverse row and the index seal are absent.
    pub(super) complete: bool,
    /// Reachable escaped rows observed in this page/cycle.
    pub(super) retained: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct RestoreIndexInspection {
    pub(super) counts: BTreeMap<String, usize>,
    pub(super) mvcc_puts: usize,
    pub(super) mvcc_tombstones: usize,
    pub(super) ref_sets: BTreeMap<u64, RestoreIndexRefSetInspection>,
    pub(super) unowned_issues: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct RestoreIndexRefSetInspection {
    pub(super) counts: BTreeMap<String, usize>,
    pub(super) operation_digests: std::collections::BTreeSet<[u8; 32]>,
    pub(super) seal_identity: Option<RestoreIndexDurableIdentity>,
    pub(super) complete_identity: Option<RestoreIndexDurableIdentity>,
    pub(super) closure_issues: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreIndexDurableIdentity {
    pub(super) operation_digest: [u8; 32],
    pub(super) initialization_digest: [u8; 32],
    pub(super) incarnation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreIndexReleaseStage {
    Entries = 1,
    Rows = 2,
    Catalogs = 3,
    Parents = 4,
    MvccEntries = 5,
    MvccRows = 6,
    MvccCatalogs = 7,
    MvccParents = 8,
    AuditParentInverses = 9,
    AuditCatalogInverses = 10,
    AuditTargetInverses = 11,
    AuditMvccParentInverses = 12,
    AuditMvccCatalogInverses = 13,
    AuditMvccTargetInverses = 14,
    Seal = 15,
    SourceMembers = 16,
    MvccSourceMembers = 17,
}

impl RestoreIndexReleaseStage {
    fn decode(tag: u8) -> Result<Self, MetadError> {
        match tag {
            1 => Ok(Self::Entries),
            2 => Ok(Self::Rows),
            3 => Ok(Self::Catalogs),
            4 => Ok(Self::Parents),
            5 => Ok(Self::MvccEntries),
            6 => Ok(Self::MvccRows),
            7 => Ok(Self::MvccCatalogs),
            8 => Ok(Self::MvccParents),
            9 => Ok(Self::AuditParentInverses),
            10 => Ok(Self::AuditCatalogInverses),
            11 => Ok(Self::AuditTargetInverses),
            12 => Ok(Self::AuditMvccParentInverses),
            13 => Ok(Self::AuditMvccCatalogInverses),
            14 => Ok(Self::AuditMvccTargetInverses),
            15 => Ok(Self::Seal),
            16 => Ok(Self::SourceMembers),
            17 => Ok(Self::MvccSourceMembers),
            _ => Err(MetadError::Codec(format!(
                "restore index release cursor has invalid stage {tag}"
            ))),
        }
    }

    fn next(self) -> Option<Self> {
        match self {
            Self::Entries => Some(Self::Rows),
            Self::Rows => Some(Self::Catalogs),
            Self::Catalogs => Some(Self::Parents),
            Self::Parents => Some(Self::MvccEntries),
            Self::MvccEntries => Some(Self::MvccRows),
            Self::MvccRows => Some(Self::MvccCatalogs),
            Self::MvccCatalogs => Some(Self::MvccParents),
            Self::MvccParents => Some(Self::AuditParentInverses),
            Self::AuditParentInverses => Some(Self::AuditCatalogInverses),
            Self::AuditCatalogInverses => Some(Self::AuditTargetInverses),
            Self::AuditTargetInverses => Some(Self::AuditMvccParentInverses),
            Self::AuditMvccParentInverses => Some(Self::AuditMvccCatalogInverses),
            Self::AuditMvccCatalogInverses => Some(Self::AuditMvccTargetInverses),
            Self::AuditMvccTargetInverses => Some(Self::SourceMembers),
            Self::SourceMembers => Some(Self::MvccSourceMembers),
            Self::MvccSourceMembers => Some(Self::Seal),
            Self::Seal => None,
        }
    }
}

fn encode_restore_index_release_cursor(stage: RestoreIndexReleaseStage, key: &[u8]) -> Vec<u8> {
    let mut cursor = Vec::with_capacity(1 + key.len());
    cursor.push(stage as u8);
    cursor.extend_from_slice(key);
    cursor
}

fn decode_restore_index_release_cursor(
    cursor: &[u8],
) -> Result<(RestoreIndexReleaseStage, Option<Vec<u8>>), MetadError> {
    if cursor.is_empty() {
        return Ok((RestoreIndexReleaseStage::Entries, None));
    }
    let stage = RestoreIndexReleaseStage::decode(cursor[0])?;
    Ok((stage, (cursor.len() > 1).then(|| cursor[1..].to_vec())))
}

fn restore_index_release_page_outcome(
    stage: RestoreIndexReleaseStage,
    rows: &[crate::command::ScanItem],
    limit: usize,
    retained: usize,
) -> RestoreIndexReleaseOutcome {
    let cursor = if rows.len() >= limit {
        encode_restore_index_release_cursor(
            stage,
            rows.last().map_or(&[][..], |row| row.key.as_slice()),
        )
    } else {
        encode_restore_index_release_cursor(
            stage
                .next()
                .expect("all paged restore index release stages have a successor"),
            &[],
        )
    };
    RestoreIndexReleaseOutcome {
        cursor,
        complete: false,
        retained,
    }
}

fn restore_index_system_key(mount: MountId, label: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + label.len());
    key.extend_from_slice(&mount.get().to_be_bytes());
    key.extend_from_slice(label);
    key
}

/// Authoritative registry for every private restore-index keyspace. Downgrade,
/// drain, metrics and fsck must consume this list instead of duplicating labels
/// or assuming System's current view represents append-only MVCC state.
pub(super) fn restore_index_private_keyspaces(mount: MountId) -> Vec<(&'static str, Vec<u8>)> {
    [
        ("index_entry", INDEX_ENTRY_LABEL),
        ("index_parent_owner", INDEX_PARENT_OWNER_LABEL),
        ("index_parent_inverse", INDEX_PARENT_INVERSE_LABEL),
        ("index_catalog", INDEX_CATALOG_LABEL),
        ("index_catalog_inverse", INDEX_CATALOG_INVERSE_LABEL),
        ("index_row", INDEX_ROW_LABEL),
        ("index_target_inverse", INDEX_TARGET_INVERSE_LABEL),
        ("index_source_member", INDEX_SOURCE_MEMBER_LABEL),
        ("index_seal", INDEX_SEAL_LABEL),
        ("index_complete", INDEX_COMPLETE_LABEL),
        ("index_mvcc_entry", INDEX_MVCC_ENTRY_LABEL),
        ("index_mvcc_parent_owner", INDEX_MVCC_PARENT_OWNER_LABEL),
        ("index_mvcc_parent_inverse", INDEX_MVCC_PARENT_INVERSE_LABEL),
        ("index_mvcc_catalog", INDEX_MVCC_CATALOG_LABEL),
        (
            "index_mvcc_catalog_inverse",
            INDEX_MVCC_CATALOG_INVERSE_LABEL,
        ),
        ("index_mvcc_row", INDEX_MVCC_ROW_LABEL),
        ("index_mvcc_target_inverse", INDEX_MVCC_TARGET_INVERSE_LABEL),
        ("index_mvcc_source_member", INDEX_MVCC_SOURCE_MEMBER_LABEL),
    ]
    .into_iter()
    .map(|(name, label)| (name, restore_index_system_key(mount, label)))
    .collect()
}

pub(super) fn restore_index_global_empty_predicates(mount: MountId) -> Vec<PredicateRef> {
    restore_index_private_keyspaces(mount)
        .into_iter()
        .map(|(_, key)| PredicateRef {
            family: RecordFamily::System,
            key,
            predicate: Predicate::PrefixEmpty,
        })
        .collect()
}

fn restore_index_ref_set_prefix(mount: MountId, label: &[u8], ref_set_id: u64) -> Vec<u8> {
    let mut key = restore_index_system_key(mount, label);
    key.extend_from_slice(&ref_set_id.to_be_bytes());
    key
}

fn restore_index_entry_prefix(mount: MountId, ref_set_id: u64, parent: Option<InodeId>) -> Vec<u8> {
    let mut key = restore_index_ref_set_prefix(mount, INDEX_ENTRY_LABEL, ref_set_id);
    if let Some(parent) = parent {
        key.extend_from_slice(&parent.get().to_be_bytes());
    }
    key
}

fn restore_index_entry_key(
    mount: MountId,
    ref_set_id: u64,
    parent: InodeId,
    name: &DentryName,
) -> Vec<u8> {
    let mut key = restore_index_entry_prefix(mount, ref_set_id, Some(parent));
    key.extend_from_slice(name.as_bytes());
    key
}

fn restore_index_parent_owner_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    restore_index_ref_set_prefix(mount, INDEX_PARENT_OWNER_LABEL, ref_set_id)
}

fn restore_index_parent_owner_key(mount: MountId, ref_set_id: u64, parent: InodeId) -> Vec<u8> {
    let mut key = restore_index_parent_owner_prefix(mount, ref_set_id);
    key.extend_from_slice(&parent.get().to_be_bytes());
    key
}

pub(super) fn restore_index_parent_inverse_prefix_for_read(
    mount: MountId,
    parent: InodeId,
) -> Vec<u8> {
    let mut key = restore_index_system_key(mount, INDEX_PARENT_INVERSE_LABEL);
    key.extend_from_slice(&parent.get().to_be_bytes());
    key
}

fn restore_index_parent_inverse_key(mount: MountId, parent: InodeId, ref_set_id: u64) -> Vec<u8> {
    let mut key = restore_index_parent_inverse_prefix_for_read(mount, parent);
    key.extend_from_slice(&ref_set_id.to_be_bytes());
    key
}

fn restore_index_catalog_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    restore_index_ref_set_prefix(mount, INDEX_CATALOG_LABEL, ref_set_id)
}

fn restore_index_catalog_key(mount: MountId, ref_set_id: u64, catalog_root: InodeId) -> Vec<u8> {
    let mut key = restore_index_catalog_prefix(mount, ref_set_id);
    key.extend_from_slice(&catalog_root.get().to_be_bytes());
    key
}

fn restore_index_catalog_inverse_prefix(mount: MountId, catalog_root: InodeId) -> Vec<u8> {
    let mut key = restore_index_system_key(mount, INDEX_CATALOG_INVERSE_LABEL);
    key.extend_from_slice(&catalog_root.get().to_be_bytes());
    key
}

fn restore_index_catalog_inverse_key(
    mount: MountId,
    catalog_root: InodeId,
    ref_set_id: u64,
) -> Vec<u8> {
    let mut key = restore_index_catalog_inverse_prefix(mount, catalog_root);
    key.extend_from_slice(&ref_set_id.to_be_bytes());
    key
}

fn restore_index_row_prefix(
    mount: MountId,
    ref_set_id: u64,
    catalog_root: Option<InodeId>,
) -> Vec<u8> {
    let mut key = restore_index_ref_set_prefix(mount, INDEX_ROW_LABEL, ref_set_id);
    if let Some(catalog_root) = catalog_root {
        key.extend_from_slice(&catalog_root.get().to_be_bytes());
    }
    key
}

fn restore_index_target_digest(parent: Option<InodeId>, name: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-restore-index-target-v1\0");
    hasher.update(parent.map_or(0, InodeId::get).to_be_bytes());
    hasher.update((name.len() as u64).to_be_bytes());
    hasher.update(name);
    hasher.finalize().into()
}

fn restore_index_row_key(
    mount: MountId,
    ref_set_id: u64,
    catalog_root: InodeId,
    target: &RestoreIndexTarget,
) -> Vec<u8> {
    let mut key = restore_index_row_prefix(mount, ref_set_id, Some(catalog_root));
    key.extend_from_slice(&restore_index_target_digest(target.parent, &target.name));
    key
}

fn restore_index_target_inverse_prefix(
    mount: MountId,
    parent: Option<InodeId>,
    name: &[u8],
) -> Vec<u8> {
    let mut key = restore_index_system_key(mount, INDEX_TARGET_INVERSE_LABEL);
    key.extend_from_slice(&restore_index_target_digest(parent, name));
    key
}

fn restore_index_target_inverse_key(
    mount: MountId,
    target: &RestoreIndexTarget,
    ref_set_id: u64,
    catalog_root: InodeId,
) -> Vec<u8> {
    let mut key = restore_index_target_inverse_prefix(mount, target.parent, &target.name);
    key.extend_from_slice(&ref_set_id.to_be_bytes());
    key.extend_from_slice(&catalog_root.get().to_be_bytes());
    key
}

fn restore_index_source_member_prefix(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    restore_index_ref_set_prefix(mount, INDEX_SOURCE_MEMBER_LABEL, ref_set_id)
}

fn restore_index_source_member_key(
    mount: MountId,
    ref_set_id: u64,
    source_inode: InodeId,
) -> Vec<u8> {
    let mut key = restore_index_source_member_prefix(mount, ref_set_id);
    key.extend_from_slice(&source_inode.get().to_be_bytes());
    key
}

pub(super) fn restore_index_seal_key(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    restore_index_ref_set_prefix(mount, INDEX_SEAL_LABEL, ref_set_id)
}

pub(super) fn restore_index_complete_key(mount: MountId, ref_set_id: u64) -> Vec<u8> {
    restore_index_ref_set_prefix(mount, INDEX_COMPLETE_LABEL, ref_set_id)
}

fn restore_index_mvcc_label(logical_label: &[u8]) -> Option<&'static [u8]> {
    match logical_label {
        INDEX_ENTRY_LABEL => Some(INDEX_MVCC_ENTRY_LABEL),
        INDEX_PARENT_OWNER_LABEL => Some(INDEX_MVCC_PARENT_OWNER_LABEL),
        INDEX_PARENT_INVERSE_LABEL => Some(INDEX_MVCC_PARENT_INVERSE_LABEL),
        INDEX_CATALOG_LABEL => Some(INDEX_MVCC_CATALOG_LABEL),
        INDEX_CATALOG_INVERSE_LABEL => Some(INDEX_MVCC_CATALOG_INVERSE_LABEL),
        INDEX_ROW_LABEL => Some(INDEX_MVCC_ROW_LABEL),
        INDEX_TARGET_INVERSE_LABEL => Some(INDEX_MVCC_TARGET_INVERSE_LABEL),
        INDEX_SOURCE_MEMBER_LABEL => Some(INDEX_MVCC_SOURCE_MEMBER_LABEL),
        _ => None,
    }
}

fn restore_index_logical_label_for_key(mount: MountId, key: &[u8]) -> Option<&'static [u8]> {
    [
        INDEX_ENTRY_LABEL,
        INDEX_PARENT_OWNER_LABEL,
        INDEX_PARENT_INVERSE_LABEL,
        INDEX_CATALOG_LABEL,
        INDEX_CATALOG_INVERSE_LABEL,
        INDEX_ROW_LABEL,
        INDEX_TARGET_INVERSE_LABEL,
        INDEX_SOURCE_MEMBER_LABEL,
    ]
    .into_iter()
    .find(|label| key.starts_with(&restore_index_system_key(mount, label)))
}

fn restore_index_owner_for_logical_value(
    label: &[u8],
    value: &[u8],
) -> Result<RestoreIndexOwner, MetadError> {
    if label == INDEX_ENTRY_LABEL {
        return Ok(decode_restore_index_entry(value)?.owner);
    }
    if label == INDEX_PARENT_OWNER_LABEL || label == INDEX_PARENT_INVERSE_LABEL {
        return decode_restore_index_owner(value);
    }
    if label == INDEX_CATALOG_LABEL || label == INDEX_CATALOG_INVERSE_LABEL {
        return Ok(decode_restore_index_catalog(value)?.owner);
    }
    if label == INDEX_ROW_LABEL || label == INDEX_TARGET_INVERSE_LABEL {
        return Ok(decode_restore_index_row(value)?.owner);
    }
    if label == INDEX_SOURCE_MEMBER_LABEL {
        return Ok(decode_restore_index_source_member(value)?.owner);
    }
    Err(MetadError::Codec(
        "restore index value has no ref-set owner".to_owned(),
    ))
}

fn restore_index_inspection_keyspace_name(
    logical_label: &[u8],
    mvcc: bool,
) -> Result<&'static str, MetadError> {
    match (logical_label, mvcc) {
        (INDEX_ENTRY_LABEL, false) => Ok("index_entry"),
        (INDEX_PARENT_OWNER_LABEL, false) => Ok("index_parent_owner"),
        (INDEX_PARENT_INVERSE_LABEL, false) => Ok("index_parent_inverse"),
        (INDEX_CATALOG_LABEL, false) => Ok("index_catalog"),
        (INDEX_CATALOG_INVERSE_LABEL, false) => Ok("index_catalog_inverse"),
        (INDEX_ROW_LABEL, false) => Ok("index_row"),
        (INDEX_TARGET_INVERSE_LABEL, false) => Ok("index_target_inverse"),
        (INDEX_SOURCE_MEMBER_LABEL, false) => Ok("index_source_member"),
        (INDEX_ENTRY_LABEL, true) => Ok("index_mvcc_entry"),
        (INDEX_PARENT_OWNER_LABEL, true) => Ok("index_mvcc_parent_owner"),
        (INDEX_PARENT_INVERSE_LABEL, true) => Ok("index_mvcc_parent_inverse"),
        (INDEX_CATALOG_LABEL, true) => Ok("index_mvcc_catalog"),
        (INDEX_CATALOG_INVERSE_LABEL, true) => Ok("index_mvcc_catalog_inverse"),
        (INDEX_ROW_LABEL, true) => Ok("index_mvcc_row"),
        (INDEX_TARGET_INVERSE_LABEL, true) => Ok("index_mvcc_target_inverse"),
        (INDEX_SOURCE_MEMBER_LABEL, true) => Ok("index_mvcc_source_member"),
        _ => Err(MetadError::Codec(
            "restore index inspection found an unknown keyspace label".to_owned(),
        )),
    }
}

/// Validate a logical row's own key and derive the exact paired logical row.
/// Entries point at their parent owner in the current view, but historical
/// entry tombstones do not require a same-command parent-owner tombstone.
fn restore_index_inspection_counterpart(
    mount: MountId,
    logical_label: &[u8],
    logical_key: &[u8],
    value: &[u8],
    include_entry_parent: bool,
) -> Result<Option<RestoreIndexCounterpart>, MetadError> {
    if logical_label == INDEX_ENTRY_LABEL {
        let entry = decode_restore_index_entry(value)?;
        let expected = restore_index_entry_key(
            mount,
            entry.owner.ref_set_id,
            entry.projection.dentry.parent,
            &entry.projection.dentry.name,
        );
        if logical_key != expected {
            return Err(MetadError::Codec(
                "restore index entry key/value identity mismatch".to_owned(),
            ));
        }
        return Ok(include_entry_parent.then(|| {
            (
                INDEX_PARENT_OWNER_LABEL,
                restore_index_parent_owner_key(
                    mount,
                    entry.owner.ref_set_id,
                    entry.projection.dentry.parent,
                ),
            )
        }));
    }
    if logical_label == INDEX_PARENT_OWNER_LABEL {
        let owner = decode_restore_index_owner(value)?;
        let prefix = restore_index_parent_owner_prefix(mount, owner.ref_set_id);
        if logical_key.len() != prefix.len() + 8 || !logical_key.starts_with(&prefix) {
            return Err(MetadError::Codec(
                "restore index parent owner key/value identity mismatch".to_owned(),
            ));
        }
        let parent = InodeId::new(u64::from_be_bytes(
            logical_key[prefix.len()..].try_into().expect("u64 width"),
        ))?;
        return Ok(Some((
            INDEX_PARENT_INVERSE_LABEL,
            restore_index_parent_inverse_key(mount, parent, owner.ref_set_id),
        )));
    }
    if logical_label == INDEX_PARENT_INVERSE_LABEL {
        let owner = decode_restore_index_owner(value)?;
        let prefix = restore_index_system_key(mount, INDEX_PARENT_INVERSE_LABEL);
        if logical_key.len() != prefix.len() + 16 || !logical_key.starts_with(&prefix) {
            return Err(MetadError::Codec(
                "restore index parent inverse key/value identity mismatch".to_owned(),
            ));
        }
        let parent = InodeId::new(u64::from_be_bytes(
            logical_key[prefix.len()..prefix.len() + 8]
                .try_into()
                .expect("u64 width"),
        ))?;
        let encoded_ref_set = u64::from_be_bytes(
            logical_key[prefix.len() + 8..]
                .try_into()
                .expect("u64 width"),
        );
        if encoded_ref_set != owner.ref_set_id {
            return Err(MetadError::Codec(
                "restore index parent inverse ref-set mismatch".to_owned(),
            ));
        }
        return Ok(Some((
            INDEX_PARENT_OWNER_LABEL,
            restore_index_parent_owner_key(mount, owner.ref_set_id, parent),
        )));
    }
    if logical_label == INDEX_CATALOG_LABEL || logical_label == INDEX_CATALOG_INVERSE_LABEL {
        let catalog = decode_restore_index_catalog(value)?;
        let owner_key =
            restore_index_catalog_key(mount, catalog.owner.ref_set_id, catalog.catalog_root);
        let inverse_key = restore_index_catalog_inverse_key(
            mount,
            catalog.catalog_root,
            catalog.owner.ref_set_id,
        );
        let (expected, counterpart_label, counterpart) = if logical_label == INDEX_CATALOG_LABEL {
            (owner_key, INDEX_CATALOG_INVERSE_LABEL, inverse_key)
        } else {
            (inverse_key, INDEX_CATALOG_LABEL, owner_key)
        };
        if logical_key != expected {
            return Err(MetadError::Codec(
                "restore index catalog key/value identity mismatch".to_owned(),
            ));
        }
        return Ok(Some((counterpart_label, counterpart)));
    }
    if logical_label == INDEX_ROW_LABEL || logical_label == INDEX_TARGET_INVERSE_LABEL {
        let row = decode_restore_index_row(value)?;
        let owner_key =
            restore_index_row_key(mount, row.owner.ref_set_id, row.catalog_root, &row.target);
        let inverse_key = restore_index_target_inverse_key(
            mount,
            &row.target,
            row.owner.ref_set_id,
            row.catalog_root,
        );
        let (expected, counterpart_label, counterpart) = if logical_label == INDEX_ROW_LABEL {
            (owner_key, INDEX_TARGET_INVERSE_LABEL, inverse_key)
        } else {
            (inverse_key, INDEX_ROW_LABEL, owner_key)
        };
        if logical_key != expected {
            return Err(MetadError::Codec(
                "restore index row key/value identity mismatch".to_owned(),
            ));
        }
        return Ok(Some((counterpart_label, counterpart)));
    }
    if logical_label == INDEX_SOURCE_MEMBER_LABEL {
        let source = decode_restore_index_source_member(value)?;
        if logical_key
            != restore_index_source_member_key(mount, source.owner.ref_set_id, source.source_inode)
        {
            return Err(MetadError::Codec(
                "restore index source-member key/value identity mismatch".to_owned(),
            ));
        }
        return Ok(None);
    }
    Err(MetadError::Codec(
        "restore index inspection cannot validate an unknown logical row".to_owned(),
    ))
}

fn restore_index_mvcc_key(
    mount: MountId,
    logical_key: &[u8],
    commit_version: Version,
) -> Result<Vec<u8>, MetadError> {
    let logical_label =
        restore_index_logical_label_for_key(mount, logical_key).ok_or_else(|| {
            MetadError::Codec("restore index MVCC key is outside a tracked keyspace".to_owned())
        })?;
    let logical_prefix = restore_index_system_key(mount, logical_label);
    let mvcc_label = restore_index_mvcc_label(logical_label).expect("tracked label has MVCC label");
    let mut key = restore_index_system_key(mount, mvcc_label);
    key.extend_from_slice(&logical_key[logical_prefix.len()..]);
    // Dentry names reject NUL, while every other logical suffix has a fixed
    // width. This delimiter keeps all versions of one logical row contiguous
    // and lets a caller seek past an exact logical key without skipping names
    // that merely share its byte prefix.
    key.push(RESTORE_INDEX_MVCC_VERSION_DELIMITER);
    key.extend_from_slice(&commit_version.get().to_be_bytes());
    Ok(key)
}

fn restore_index_mvcc_prefix(mount: MountId, logical_prefix: &[u8]) -> Result<Vec<u8>, MetadError> {
    let logical_label =
        restore_index_logical_label_for_key(mount, logical_prefix).ok_or_else(|| {
            MetadError::Codec("restore index MVCC prefix is outside a tracked keyspace".to_owned())
        })?;
    let base = restore_index_system_key(mount, logical_label);
    let mvcc_label = restore_index_mvcc_label(logical_label).expect("tracked label has MVCC label");
    let mut prefix = restore_index_system_key(mount, mvcc_label);
    prefix.extend_from_slice(&logical_prefix[base.len()..]);
    Ok(prefix)
}

fn restore_index_mvcc_owner_prefixes(mount: MountId, ref_set_id: u64) -> Vec<Vec<u8>> {
    [
        restore_index_entry_prefix(mount, ref_set_id, None),
        restore_index_parent_owner_prefix(mount, ref_set_id),
        restore_index_catalog_prefix(mount, ref_set_id),
        restore_index_row_prefix(mount, ref_set_id, None),
        restore_index_source_member_prefix(mount, ref_set_id),
    ]
    .into_iter()
    .map(|prefix| restore_index_mvcc_prefix(mount, &prefix).expect("owner prefix is MVCC tracked"))
    .collect()
}

fn encode_restore_index_mvcc(record: &RestoreIndexMvccRecord) -> Vec<u8> {
    let mut value = Vec::with_capacity(26 + record.logical_key.len() + record.value.len());
    value.extend_from_slice(INDEX_MVCC_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.push(record.kind as u8);
    value.extend_from_slice(&record.commit_version.to_be_bytes());
    push_restore_index_bytes(&mut value, &record.logical_key);
    push_restore_index_bytes(&mut value, &record.value);
    value
}

fn decode_restore_index_mvcc(value: &[u8]) -> Result<RestoreIndexMvccRecord, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_MVCC_MAGIC, "MVCC record")?;
    let kind = match decoder.u8()? {
        1 => RestoreIndexMvccKind::Put,
        2 => RestoreIndexMvccKind::Tombstone,
        tag => {
            return Err(MetadError::Codec(format!(
                "restore index MVCC record has invalid kind {tag}"
            )))
        }
    };
    let commit_version = decoder.u64()?;
    if commit_version == 0 {
        return Err(MetadError::Codec(
            "restore index MVCC record has a zero commit version".to_owned(),
        ));
    }
    let logical_key = decoder.bytes()?.to_vec();
    let value = decoder.bytes()?.to_vec();
    decoder.finish("MVCC record")?;
    if logical_key.is_empty() || value.is_empty() {
        return Err(MetadError::Codec(
            "restore index MVCC record has an empty key/value".to_owned(),
        ));
    }
    Ok(RestoreIndexMvccRecord {
        commit_version,
        kind,
        logical_key,
        value,
    })
}

fn encode_restore_index_complete(marker: &RestoreIndexCompleteMarker) -> Vec<u8> {
    let mut value = Vec::with_capacity(97);
    value.extend_from_slice(INDEX_COMPLETE_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&marker.operation_digest);
    value.extend_from_slice(&marker.initialization_digest);
    value.extend_from_slice(&marker.ref_set_id.to_be_bytes());
    value.extend_from_slice(&marker.incarnation.to_be_bytes());
    value.extend_from_slice(&marker.complete_version.to_be_bytes());
    value
}

fn decode_restore_index_complete(value: &[u8]) -> Result<RestoreIndexCompleteMarker, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_COMPLETE_MAGIC, "complete marker")?;
    let marker = RestoreIndexCompleteMarker {
        operation_digest: decoder.array_32()?,
        initialization_digest: decoder.array_32()?,
        ref_set_id: decoder.u64()?,
        incarnation: decoder.u64()?,
        complete_version: decoder.u64()?,
    };
    decoder.finish("complete marker")?;
    if marker.ref_set_id == 0 || marker.incarnation == 0 || marker.complete_version == 0 {
        return Err(MetadError::Codec(
            "restore index complete marker has an invalid identity".to_owned(),
        ));
    }
    Ok(marker)
}

fn restore_index_mvcc_inverse_logical_key(
    mount: MountId,
    record: &RestoreIndexMvccRecord,
) -> Result<Option<Vec<u8>>, MetadError> {
    let label =
        restore_index_logical_label_for_key(mount, &record.logical_key).ok_or_else(|| {
            MetadError::Codec("restore index MVCC record has an unknown logical label".to_owned())
        })?;
    if label == INDEX_ENTRY_LABEL {
        let entry = decode_restore_index_entry(&record.value)?;
        if record.logical_key
            != restore_index_entry_key(
                mount,
                entry.owner.ref_set_id,
                entry.projection.dentry.parent,
                &entry.projection.dentry.name,
            )
        {
            return Err(MetadError::Codec(
                "restore index MVCC entry changed logical identity".to_owned(),
            ));
        }
        return Ok(None);
    }
    if label == INDEX_PARENT_OWNER_LABEL {
        let owner = decode_restore_index_owner(&record.value)?;
        let prefix = restore_index_parent_owner_prefix(mount, owner.ref_set_id);
        if record.logical_key.len() != prefix.len() + 8 || !record.logical_key.starts_with(&prefix)
        {
            return Err(MetadError::Codec(
                "restore index MVCC parent owner changed logical identity".to_owned(),
            ));
        }
        let parent = InodeId::new(u64::from_be_bytes(
            record.logical_key[prefix.len()..]
                .try_into()
                .expect("u64 width"),
        ))?;
        return Ok(Some(restore_index_parent_inverse_key(
            mount,
            parent,
            owner.ref_set_id,
        )));
    }
    if label == INDEX_CATALOG_LABEL {
        let catalog = decode_restore_index_catalog(&record.value)?;
        if record.logical_key
            != restore_index_catalog_key(mount, catalog.owner.ref_set_id, catalog.catalog_root)
        {
            return Err(MetadError::Codec(
                "restore index MVCC catalog changed logical identity".to_owned(),
            ));
        }
        return Ok(Some(restore_index_catalog_inverse_key(
            mount,
            catalog.catalog_root,
            catalog.owner.ref_set_id,
        )));
    }
    if label == INDEX_ROW_LABEL {
        let row = decode_restore_index_row(&record.value)?;
        if record.logical_key
            != restore_index_row_key(mount, row.owner.ref_set_id, row.catalog_root, &row.target)
        {
            return Err(MetadError::Codec(
                "restore index MVCC row changed logical identity".to_owned(),
            ));
        }
        return Ok(Some(restore_index_target_inverse_key(
            mount,
            &row.target,
            row.owner.ref_set_id,
            row.catalog_root,
        )));
    }
    if label == INDEX_SOURCE_MEMBER_LABEL {
        let source = decode_restore_index_source_member(&record.value)?;
        if record.logical_key
            != restore_index_source_member_key(mount, source.owner.ref_set_id, source.source_inode)
        {
            return Err(MetadError::Codec(
                "restore index MVCC source member changed logical identity".to_owned(),
            ));
        }
        return Ok(None);
    }
    Err(MetadError::Codec(
        "restore index MVCC release encountered an inverse-side owner".to_owned(),
    ))
}

/// Final release fence. Inverse keyspaces are target-first and cannot be
/// expressed as one ref-set prefix; `release_restore_index_page` deletes each
/// inverse under an exact owner-row CAS before these owner-side predicates can
/// become true.
pub(super) fn restore_index_release_empty_predicates(
    mount: MountId,
    ref_set_id: u64,
) -> Vec<PredicateRef> {
    [
        INDEX_ENTRY_LABEL,
        INDEX_PARENT_OWNER_LABEL,
        INDEX_CATALOG_LABEL,
        INDEX_ROW_LABEL,
        INDEX_SOURCE_MEMBER_LABEL,
    ]
    .into_iter()
    .map(|label| PredicateRef {
        family: RecordFamily::System,
        key: restore_index_ref_set_prefix(mount, label, ref_set_id),
        predicate: Predicate::PrefixEmpty,
    })
    .chain(std::iter::once(PredicateRef {
        family: RecordFamily::System,
        key: restore_index_seal_key(mount, ref_set_id),
        predicate: Predicate::NotExists,
    }))
    .chain(std::iter::once(PredicateRef {
        family: RecordFamily::System,
        key: restore_index_complete_key(mount, ref_set_id),
        predicate: Predicate::NotExists,
    }))
    .chain(
        restore_index_mvcc_owner_prefixes(mount, ref_set_id)
            .into_iter()
            .map(|key| PredicateRef {
                family: RecordFamily::System,
                key,
                predicate: Predicate::PrefixEmpty,
            }),
    )
    .collect()
}

fn encode_restore_index_owner(owner: RestoreIndexOwner) -> Vec<u8> {
    let mut value = Vec::with_capacity(OWNER_BYTES);
    value.extend_from_slice(&owner.operation_digest);
    value.extend_from_slice(&owner.ref_set_id.to_be_bytes());
    value
}

fn decode_restore_index_owner(value: &[u8]) -> Result<RestoreIndexOwner, MetadError> {
    if value.len() != OWNER_BYTES {
        return Err(MetadError::Codec(
            "restore index owner has an invalid length".to_owned(),
        ));
    }
    RestoreIndexOwner {
        operation_digest: value[..32].try_into().expect("digest width"),
        ref_set_id: u64::from_be_bytes(value[32..].try_into().expect("u64 width")),
    }
    .validate()
}

fn encode_restore_index_entry(entry: &RestoreIndexEntry) -> Vec<u8> {
    let projection = encode_dentry_projection(&entry.projection);
    let mut value = Vec::with_capacity(53 + projection.len());
    value.extend_from_slice(INDEX_ENTRY_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&encode_restore_index_owner(entry.owner));
    push_restore_index_bytes(&mut value, &projection);
    value
}

fn decode_restore_index_entry(value: &[u8]) -> Result<RestoreIndexEntry, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_ENTRY_MAGIC, "entry")?;
    let owner = decoder.owner()?;
    let projection = decode_dentry_projection(decoder.bytes()?)
        .map_err(|error| MetadError::Codec(error.to_string()))?;
    decoder.finish("entry")?;
    Ok(RestoreIndexEntry { owner, projection })
}

fn encode_restore_index_catalog(catalog: &RestoreIndexCatalog) -> Vec<u8> {
    let record = encode_path_index_catalog(&catalog.record);
    let mut value = Vec::with_capacity(61 + record.len());
    value.extend_from_slice(INDEX_CATALOG_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&encode_restore_index_owner(catalog.owner));
    value.extend_from_slice(&catalog.catalog_root.get().to_be_bytes());
    push_restore_index_bytes(&mut value, &record);
    value
}

fn decode_restore_index_catalog(value: &[u8]) -> Result<RestoreIndexCatalog, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_CATALOG_MAGIC, "catalog")?;
    let owner = decoder.owner()?;
    let catalog_root = InodeId::new(decoder.u64()?)?;
    let record = decode_path_index_catalog(decoder.bytes()?)
        .map_err(|error| MetadError::Codec(error.to_string()))?;
    decoder.finish("catalog")?;
    Ok(RestoreIndexCatalog {
        owner,
        catalog_root,
        record,
    })
}

fn encode_restore_index_target(value: &mut Vec<u8>, target: &RestoreIndexTarget) {
    value.push(u8::from(target.parent.is_some()));
    value.extend_from_slice(&target.parent.map_or(0, InodeId::get).to_be_bytes());
    push_restore_index_bytes(value, &target.name);
    value.extend_from_slice(&target.inode.get().to_be_bytes());
    value.push(restore_index_file_type_tag(target.file_type));
    value.extend_from_slice(&target.attr_generation.to_be_bytes());
    match target.body_digest {
        Some(digest) => {
            value.push(1);
            value.extend_from_slice(&digest);
        }
        None => value.push(0),
    }
}

fn decode_restore_index_target(
    decoder: &mut RestoreIndexDecoder<'_>,
) -> Result<RestoreIndexTarget, MetadError> {
    let parent_tag = decoder.u8()?;
    let parent_raw = decoder.u64()?;
    let parent = match (parent_tag, parent_raw) {
        (0, 0) => None,
        (1, raw) => Some(InodeId::new(raw)?),
        _ => {
            return Err(MetadError::Codec(
                "restore index target has an invalid parent tag".to_owned(),
            ))
        }
    };
    let name = decoder.bytes()?.to_vec();
    if parent.is_some() == name.is_empty() {
        return Err(MetadError::Codec(
            "restore index target has an invalid parent/name shape".to_owned(),
        ));
    }
    if !name.is_empty() {
        DentryName::new(name.clone()).map_err(|error| MetadError::Codec(error.to_string()))?;
    }
    let inode = InodeId::new(decoder.u64()?)?;
    let file_type = restore_index_file_type(decoder.u8()?)?;
    let attr_generation = decoder.u64()?;
    let body_digest = match decoder.u8()? {
        0 => None,
        1 => Some(decoder.array_32()?),
        _ => {
            return Err(MetadError::Codec(
                "restore index target has an invalid body tag".to_owned(),
            ))
        }
    };
    Ok(RestoreIndexTarget {
        parent,
        name,
        inode,
        file_type,
        attr_generation,
        body_digest,
    })
}

fn encode_restore_index_row(row: &RestoreIndexRow) -> Vec<u8> {
    let record = encode_path_index_row(&row.record);
    let mut value = Vec::with_capacity(112 + row.target.name.len() + record.len());
    value.extend_from_slice(INDEX_ROW_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&encode_restore_index_owner(row.owner));
    value.extend_from_slice(&row.catalog_root.get().to_be_bytes());
    encode_restore_index_target(&mut value, &row.target);
    push_restore_index_bytes(&mut value, &record);
    value
}

fn decode_restore_index_row(value: &[u8]) -> Result<RestoreIndexRow, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_ROW_MAGIC, "row")?;
    let owner = decoder.owner()?;
    let catalog_root = InodeId::new(decoder.u64()?)?;
    let target = decode_restore_index_target(&mut decoder)?;
    let record = decode_path_index_row(decoder.bytes()?)
        .map_err(|error| MetadError::Codec(error.to_string()))?;
    decoder.finish("row")?;
    Ok(RestoreIndexRow {
        owner,
        catalog_root,
        target,
        record,
    })
}

fn encode_restore_index_source_member(
    source: &RestoreIndexSourceMember,
) -> Result<Vec<u8>, MetadError> {
    let member = encode_restore_staging_member(&source.member)?;
    let mut value = Vec::with_capacity(61 + member.len());
    value.extend_from_slice(INDEX_SOURCE_MEMBER_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&encode_restore_index_owner(source.owner));
    value.extend_from_slice(&source.source_inode.get().to_be_bytes());
    push_restore_index_bytes(&mut value, &member);
    Ok(value)
}

fn decode_restore_index_source_member(
    value: &[u8],
) -> Result<RestoreIndexSourceMember, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_SOURCE_MEMBER_MAGIC, "source member")?;
    let owner = decoder.owner()?;
    let source_inode = InodeId::new(decoder.u64()?)?;
    let member = decode_restore_staging_member(decoder.bytes()?)?;
    decoder.finish("source member")?;
    if member.operation_digest != owner.operation_digest
        || member.source_inode != Some(source_inode)
    {
        return Err(MetadError::Codec(
            "restore index source member changed identity".to_owned(),
        ));
    }
    Ok(RestoreIndexSourceMember {
        owner,
        source_inode,
        member,
    })
}

pub(super) fn encode_restore_index_seal(seal: &RestoreIndexSeal) -> Vec<u8> {
    let mut value = Vec::with_capacity(153);
    value.extend_from_slice(INDEX_SEAL_MAGIC);
    value.push(RESTORE_FORMAT_VERSION);
    value.extend_from_slice(&seal.operation_digest);
    value.extend_from_slice(&seal.initialization_digest);
    value.extend_from_slice(&seal.ref_set_id.to_be_bytes());
    value.extend_from_slice(&seal.incarnation.to_be_bytes());
    value.extend_from_slice(&seal.entry_count.to_be_bytes());
    value.extend_from_slice(&seal.catalog_count.to_be_bytes());
    value.extend_from_slice(&seal.row_count.to_be_bytes());
    value.extend_from_slice(&seal.digest);
    value
}

pub(super) fn decode_restore_index_seal(value: &[u8]) -> Result<RestoreIndexSeal, MetadError> {
    let mut decoder = RestoreIndexDecoder::new(value);
    decoder.header(INDEX_SEAL_MAGIC, "seal")?;
    let seal = RestoreIndexSeal {
        operation_digest: decoder.array_32()?,
        initialization_digest: decoder.array_32()?,
        ref_set_id: decoder.u64()?,
        incarnation: decoder.u64()?,
        entry_count: decoder.u64()?,
        catalog_count: decoder.u64()?,
        row_count: decoder.u64()?,
        digest: decoder.array_32()?,
    };
    decoder.finish("seal")?;
    if seal.ref_set_id == 0 || seal.incarnation == 0 {
        return Err(MetadError::Codec(
            "restore index seal has an invalid identity".to_owned(),
        ));
    }
    Ok(seal)
}

fn restore_index_file_type_tag(file_type: FileType) -> u8 {
    match file_type {
        FileType::File => 1,
        FileType::Directory => 2,
        FileType::Symlink => 3,
        FileType::NamedPipe => 4,
        FileType::CharDevice => 5,
        FileType::BlockDevice => 6,
        FileType::Socket => 7,
    }
}

fn restore_index_file_type(tag: u8) -> Result<FileType, MetadError> {
    match tag {
        1 => Ok(FileType::File),
        2 => Ok(FileType::Directory),
        3 => Ok(FileType::Symlink),
        4 => Ok(FileType::NamedPipe),
        5 => Ok(FileType::CharDevice),
        6 => Ok(FileType::BlockDevice),
        7 => Ok(FileType::Socket),
        _ => Err(MetadError::Codec(format!(
            "restore index target has invalid file type {tag}"
        ))),
    }
}

fn restore_index_body_digest(body: Option<&BodyDescriptor>) -> Option<[u8; 32]> {
    body.map(|body| {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv-restore-index-body-v1\0");
        hasher.update(encode_body_descriptor(body));
        hasher.finalize().into()
    })
}

fn restore_index_relative_components(
    base: &str,
    path: &str,
) -> Result<Option<Vec<DentryName>>, MetadError> {
    let base = parse_absolute_path(base)?;
    let path = parse_absolute_path(path)?;
    if path.len() < base.len()
        || !base
            .iter()
            .zip(path.iter())
            .all(|(left, right)| left == right)
    {
        return Ok(None);
    }
    Ok(Some(path[base.len()..].to_vec()))
}

fn restore_index_relative_string(components: &[DentryName]) -> Result<String, MetadError> {
    let absolute = canonical_path(components)?;
    Ok(absolute.strip_prefix('/').unwrap_or(&absolute).to_owned())
}

fn restore_index_join_path(base: &str, relative: &[DentryName]) -> Result<String, MetadError> {
    let mut components = parse_absolute_path(base)?;
    components.extend_from_slice(relative);
    canonical_path(&components)
}

fn restore_index_target_from_projection(projection: &DentryProjection) -> RestoreIndexTarget {
    RestoreIndexTarget {
        parent: Some(projection.dentry.parent),
        name: projection.dentry.name.as_bytes().to_vec(),
        inode: projection.dentry.child,
        file_type: projection.dentry.child_type,
        attr_generation: projection.dentry.attr_generation,
        body_digest: restore_index_body_digest(projection.body.as_ref()),
    }
}

fn restore_index_root_target(metadata: &PathMetadata) -> RestoreIndexTarget {
    RestoreIndexTarget {
        parent: None,
        name: Vec::new(),
        inode: metadata.attr.inode,
        file_type: metadata.attr.file_type,
        attr_generation: metadata.attr.generation,
        body_digest: restore_index_body_digest(metadata.body.as_ref()),
    }
}

/// A generic clone remints inode ids while preserving the source entry's
/// metadata and body descriptor.  Compare only fields that survive that
/// reminting so an inherited index row is not applied to a later replacement
/// that merely reused the same path.
fn restore_index_clone_metadata_matches_source(fork: &PathMetadata, source: &PathMetadata) -> bool {
    fork.attr.file_type == source.attr.file_type
        && fork.attr.mode == source.attr.mode
        && fork.attr.uid == source.attr.uid
        && fork.attr.gid == source.attr.gid
        && fork.attr.rdev == source.attr.rdev
        && fork.attr.size == source.attr.size
        && fork.attr.mtime_ms == source.attr.mtime_ms
        && fork.body == source.body
}

fn restore_index_clone_entry_matches_source(
    fork: &DentryWithAttr,
    source: &DentryWithAttr,
) -> bool {
    restore_index_clone_metadata_matches_source(
        &PathMetadata {
            attr: fork.attr.clone(),
            body: fork.body.clone(),
        },
        &PathMetadata {
            attr: source.attr.clone(),
            body: source.body.clone(),
        },
    )
}

fn restore_index_target_matches_projection(
    target: &RestoreIndexTarget,
    projection: &DentryProjection,
) -> bool {
    target.parent == Some(projection.dentry.parent)
        && target.name == projection.dentry.name.as_bytes()
        && target.inode == projection.dentry.child
        && target.file_type == projection.dentry.child_type
        && target.attr_generation == projection.dentry.attr_generation
        && target.body_digest == restore_index_body_digest(projection.body.as_ref())
}

fn push_restore_index_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn fold_restore_index_seal_row(hasher: &mut Sha256, kind: u8, key: &[u8], value: &[u8]) {
    hasher.update([kind]);
    hasher.update((key.len() as u64).to_be_bytes());
    hasher.update(key);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn account_restore_index_buffer(
    resource: &str,
    items: usize,
    bytes: &mut usize,
    additional_bytes: usize,
) -> Result<(), MetadError> {
    if items > MAX_RESTORE_INDEX_PLAN_ITEMS {
        return Err(MetadError::RestoreResourceLimit {
            resource: format!("{resource} items"),
            limit: MAX_RESTORE_INDEX_PLAN_ITEMS as u64,
            actual: items as u64,
        });
    }
    *bytes = bytes.saturating_add(additional_bytes);
    if *bytes > MAX_RESTORE_INDEX_PLAN_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: format!("{resource} bytes"),
            limit: MAX_RESTORE_INDEX_PLAN_BYTES as u64,
            actual: *bytes as u64,
        });
    }
    Ok(())
}

fn validate_restore_index_collection(resource: &str, actual: usize) -> Result<(), MetadError> {
    if actual > MAX_RESTORE_SUBTREE_ENTRIES {
        return Err(MetadError::RestoreResourceLimit {
            resource: resource.to_owned(),
            limit: MAX_RESTORE_SUBTREE_ENTRIES as u64,
            actual: actual as u64,
        });
    }
    Ok(())
}

struct RestoreIndexDecoder<'a> {
    input: &'a [u8],
}

impl<'a> RestoreIndexDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadError> {
        let Some((head, tail)) = self.input.split_at_checked(len) else {
            return Err(MetadError::Codec(
                "restore index record is truncated".to_owned(),
            ));
        };
        self.input = tail;
        Ok(head)
    }

    fn header(&mut self, magic: &[u8; 8], kind: &str) -> Result<(), MetadError> {
        if self.take(8)? != magic {
            return Err(MetadError::Codec(format!(
                "invalid restore index {kind} magic"
            )));
        }
        if self.u8()? != RESTORE_FORMAT_VERSION {
            return Err(MetadError::Codec(format!(
                "unsupported restore index {kind} version"
            )));
        }
        Ok(())
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

    fn bytes(&mut self) -> Result<&'a [u8], MetadError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn owner(&mut self) -> Result<RestoreIndexOwner, MetadError> {
        decode_restore_index_owner(self.take(OWNER_BYTES)?)
    }

    fn finish(self, kind: &str) -> Result<(), MetadError> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(MetadError::Codec(format!(
                "restore index {kind} has trailing bytes"
            )))
        }
    }
}

fn restore_index_owner_mutations(
    mount: MountId,
    owner: RestoreIndexOwner,
    parent: InodeId,
) -> [Mutation; 2] {
    let value = Value(encode_restore_index_owner(owner));
    [
        Mutation {
            family: RecordFamily::System,
            key: restore_index_parent_owner_key(mount, owner.ref_set_id, parent),
            op: MutationOp::Put,
            value: Some(value.clone()),
        },
        Mutation {
            family: RecordFamily::System,
            key: restore_index_parent_inverse_key(mount, parent, owner.ref_set_id),
            op: MutationOp::Put,
            value: Some(value),
        },
    ]
}

fn restore_index_catalog_mutations(mount: MountId, catalog: &RestoreIndexCatalog) -> [Mutation; 2] {
    let value = Value(encode_restore_index_catalog(catalog));
    [
        Mutation {
            family: RecordFamily::System,
            key: restore_index_catalog_key(mount, catalog.owner.ref_set_id, catalog.catalog_root),
            op: MutationOp::Put,
            value: Some(value.clone()),
        },
        Mutation {
            family: RecordFamily::System,
            key: restore_index_catalog_inverse_key(
                mount,
                catalog.catalog_root,
                catalog.owner.ref_set_id,
            ),
            op: MutationOp::Put,
            value: Some(value),
        },
    ]
}

fn restore_index_row_mutations(mount: MountId, row: &RestoreIndexRow) -> [Mutation; 2] {
    let value = Value(encode_restore_index_row(row));
    [
        Mutation {
            family: RecordFamily::System,
            key: restore_index_row_key(mount, row.owner.ref_set_id, row.catalog_root, &row.target),
            op: MutationOp::Put,
            value: Some(value.clone()),
        },
        Mutation {
            family: RecordFamily::System,
            key: restore_index_target_inverse_key(
                mount,
                &row.target,
                row.owner.ref_set_id,
                row.catalog_root,
            ),
            op: MutationOp::Put,
            value: Some(value),
        },
    ]
}

fn restore_index_source_member_mutation(
    mount: MountId,
    source: &RestoreIndexSourceMember,
) -> Result<Mutation, MetadError> {
    Ok(Mutation {
        family: RecordFamily::System,
        key: restore_index_source_member_key(mount, source.owner.ref_set_id, source.source_inode),
        op: MutationOp::Put,
        value: Some(Value(encode_restore_index_source_member(source)?)),
    })
}

/// Mutations used while building a detached tree.  Callers must place them in
/// the same command as the corresponding dentry, or in a later hidden staging
/// command guarded by the operation and temporary binding versions.
pub(super) fn restore_index_materialization_mutations(
    mount: MountId,
    operation: &RestoreOperation,
    projection: &DentryProjection,
) -> RestoreIndexMutationPlan {
    let owner = RestoreIndexOwner::from_operation(operation);
    let key = restore_index_entry_key(
        mount,
        operation.ref_set_id,
        projection.dentry.parent,
        &projection.dentry.name,
    );
    let mut mutations = restore_index_owner_mutations(mount, owner, projection.dentry.parent)
        .into_iter()
        .collect::<Vec<_>>();
    mutations.push(Mutation {
        family: RecordFamily::System,
        key,
        op: MutationOp::Put,
        value: Some(Value(encode_restore_index_entry(&RestoreIndexEntry {
            owner,
            projection: projection.clone(),
        }))),
    });
    RestoreIndexMutationPlan {
        predicates: Vec::new(),
        mutations,
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Resolve an exact generic clone root back to a durably Complete restore.
    /// The existing 40-byte ForkBinding remains the sole clone contract; the
    /// restore staging inverse is only used to prove that its source owns a
    /// sealed private index overlay.
    fn restore_index_fork_source(
        &self,
        fork_root: InodeId,
    ) -> Result<Option<RestoreIndexForkSource>, MetadError> {
        let control_version = self.read_version()?;
        let binding_key = fork_binding_key(self.mount, fork_root);
        let Some(binding_item) = self.metadata.get_versioned(
            RecordFamily::ForkBinding,
            &binding_key,
            control_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(None);
        };
        let binding = crate::layout::decode_fork_binding(&binding_item.value.0)
            .map_err(|error| MetadError::Codec(error.to_string()))?;
        if binding.fork_root != fork_root
            || binding_key != fork_binding_key(self.mount, binding.fork_root)
            || binding.created_version != binding_item.version.get()
            || binding.fork_root.shard_index() != self.shard_index
            || binding.source_root.shard_index() != self.shard_index
        {
            return Err(MetadError::Codec(
                "restore index ForkBinding changed identity".to_owned(),
            ));
        }
        let pinned_version = Version::new(binding.pinned_read_version)?;
        if !self.restore_fork_binding_is_namespace_anchor(&binding, control_version)? {
            // A detached restore's temporary binding is a retention fence, not
            // a generic clone whose reads may fall through to a source index.
            return Ok(None);
        }

        let Some(inverse) = self.metadata.get(
            RecordFamily::System,
            &restore_staging_inode_key(self.mount, binding.source_root),
            control_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(None);
        };
        let (operation_digest, ref_set_id) = decode_restore_staging_inverse(&inverse.0)?;
        let owner = RestoreIndexOwner {
            operation_digest,
            ref_set_id,
        }
        .validate()?;
        let Some(operation) =
            self.visible_restore_index_operation(owner, pinned_version, ReadPurpose::Snapshot)?
        else {
            return Ok(None);
        };
        if operation.destination_root != binding.source_root
            || operation.operation_digest != operation_digest
            || operation.ref_set_id != ref_set_id
        {
            return Err(MetadError::Codec(
                "restore index ForkBinding source changed restore identity".to_owned(),
            ));
        }
        Ok(Some(RestoreIndexForkSource { binding, operation }))
    }

    pub(super) fn restore_index_parent_has_fork_source(
        &self,
        parent: InodeId,
    ) -> Result<bool, MetadError> {
        let control_version = self.read_version()?;
        let binding_key = fork_binding_key(self.mount, parent);
        if self
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::ForkBinding,
                prefix: binding_key,
                start_after: None,
                version: control_version,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })?
            .is_empty()
        {
            return Ok(false);
        }
        Ok(self.restore_index_fork_source(parent)?.is_some())
    }

    pub(super) fn inspect_restore_index_state(
        &self,
        read_version: Version,
    ) -> Result<RestoreIndexInspection, MetadError> {
        let mut inspection = RestoreIndexInspection::default();
        let mut issue_budget = RestoreIndexInspectionIssueBudget::default();
        for (name, prefix) in restore_index_private_keyspaces(self.mount) {
            let mut count = 0_usize;
            let mut start_after = None;
            loop {
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after: start_after.clone(),
                    version: read_version,
                    limit: RESTORE_INDEX_SCAN_PAGE,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                if rows.is_empty() {
                    break;
                }
                count = count.checked_add(rows.len()).ok_or_else(|| {
                    MetadError::Codec("restore index inspection row count overflow".to_owned())
                })?;
                for row in &rows {
                    let decoded = self.restore_index_inspected_row_identity(name, row);
                    let decoded = match decoded {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            issue_budget.push(
                                &mut inspection.unowned_issues,
                                format!(
                                    "{name} row (key bytes {}) cannot be attributed: {error}",
                                    row.key.len()
                                ),
                            );
                            continue;
                        }
                    };

                    if !inspection.ref_sets.contains_key(&decoded.ref_set_id)
                        && inspection.ref_sets.len() >= MAX_RESTORE_INDEX_INSPECTION_REF_SETS
                    {
                        return Err(MetadError::RestoreResourceLimit {
                            resource: "restore index fsck ref sets".to_owned(),
                            limit: MAX_RESTORE_INDEX_INSPECTION_REF_SETS as u64,
                            actual: inspection.ref_sets.len().saturating_add(1) as u64,
                        });
                    }
                    let ref_set = inspection.ref_sets.entry(decoded.ref_set_id).or_default();
                    let keyspace_count = ref_set.counts.entry(name.to_owned()).or_default();
                    *keyspace_count = keyspace_count.checked_add(1).ok_or_else(|| {
                        MetadError::Codec(
                            "restore index inspection ref-set count overflow".to_owned(),
                        )
                    })?;
                    if !ref_set
                        .operation_digests
                        .contains(&decoded.operation_digest)
                        && ref_set.operation_digests.len() < 2
                    {
                        ref_set.operation_digests.insert(decoded.operation_digest);
                    }
                    if decoded.is_put {
                        inspection.mvcc_puts =
                            inspection.mvcc_puts.checked_add(1).ok_or_else(|| {
                                MetadError::Codec(
                                    "restore index inspection MVCC PUT count overflow".to_owned(),
                                )
                            })?;
                    }
                    if decoded.is_tombstone {
                        inspection.mvcc_tombstones =
                            inspection.mvcc_tombstones.checked_add(1).ok_or_else(|| {
                                MetadError::Codec(
                                    "restore index inspection MVCC tombstone count overflow"
                                        .to_owned(),
                                )
                            })?;
                    }
                    if let Some(identity) = decoded.seal_identity {
                        match &ref_set.seal_identity {
                            Some(existing) if existing != &identity => issue_budget.push(
                                &mut ref_set.closure_issues,
                                format!(
                                    "ref-set {} has conflicting index seal identities",
                                    decoded.ref_set_id
                                ),
                            ),
                            None => ref_set.seal_identity = Some(identity),
                            Some(_) => {}
                        }
                    }
                    if let Some(identity) = decoded.complete_identity {
                        match &ref_set.complete_identity {
                            Some(existing) if existing != &identity => issue_budget.push(
                                &mut ref_set.closure_issues,
                                format!(
                                    "ref-set {} has conflicting Complete marker identities",
                                    decoded.ref_set_id
                                ),
                            ),
                            None => ref_set.complete_identity = Some(identity),
                            Some(_) => {}
                        }
                    }

                    if !matches!(name, "index_seal" | "index_complete") {
                        match self.restore_index_inspected_row_closure(name, row, read_version) {
                            Ok((owner, Some(issue))) => {
                                if let Some(ref_set) =
                                    inspection.ref_sets.get_mut(&owner.ref_set_id)
                                {
                                    issue_budget.push(&mut ref_set.closure_issues, issue);
                                } else {
                                    issue_budget.push(
                                        &mut inspection.unowned_issues,
                                        format!(
                                            "{name} row closure resolved an unknown ref-set {}",
                                            owner.ref_set_id
                                        ),
                                    );
                                }
                            }
                            Ok((_, None)) => {}
                            Err(error) => issue_budget.push(
                                &mut inspection.unowned_issues,
                                format!(
                                    "{name} row (key bytes {}) fails exact closure: {error}",
                                    row.key.len()
                                ),
                            ),
                        }
                    }
                }
                let reached_tail = rows.len() < RESTORE_INDEX_SCAN_PAGE;
                start_after = rows.last().map(|row| row.key.clone());
                if reached_tail {
                    break;
                }
            }
            inspection.counts.insert(name.to_owned(), count);
        }

        self.inspect_complete_restore_index_source_closure(
            read_version,
            &mut inspection,
            &mut issue_budget,
        )?;

        for (ref_set_id, ref_set) in &mut inspection.ref_sets {
            if ref_set.operation_digests.len() != 1 {
                issue_budget.push(
                    &mut ref_set.closure_issues,
                    format!(
                        "ref-set {ref_set_id} has at least {} operation identities",
                        ref_set.operation_digests.len()
                    ),
                );
            }
            if let (Some(seal), Some(complete)) =
                (&ref_set.seal_identity, &ref_set.complete_identity)
            {
                if seal != complete {
                    issue_budget.push(
                        &mut ref_set.closure_issues,
                        format!(
                            "ref-set {ref_set_id} index seal and Complete marker identities disagree"
                        ),
                    );
                }
            }
            for (owner, inverse) in [
                ("index_parent_owner", "index_parent_inverse"),
                ("index_catalog", "index_catalog_inverse"),
                ("index_row", "index_target_inverse"),
                ("index_mvcc_parent_owner", "index_mvcc_parent_inverse"),
                ("index_mvcc_catalog", "index_mvcc_catalog_inverse"),
                ("index_mvcc_row", "index_mvcc_target_inverse"),
            ] {
                let owner_count = ref_set.counts.get(owner).copied().unwrap_or(0);
                let inverse_count = ref_set.counts.get(inverse).copied().unwrap_or(0);
                if owner_count != inverse_count {
                    issue_budget.push(
                        &mut ref_set.closure_issues,
                        format!(
                            "ref-set {ref_set_id} {owner}/{inverse} count mismatch: {owner_count}/{inverse_count}"
                        ),
                    );
                }
            }
        }
        if issue_budget.truncated {
            inspection.unowned_issues.push(format!(
                "restore index inspection diagnostics exceeded the {}-issue limit",
                MAX_RESTORE_INDEX_INSPECTION_ISSUES
            ));
        }
        Ok(inspection)
    }

    fn inspect_complete_restore_index_source_closure(
        &self,
        read_version: Version,
        inspection: &mut RestoreIndexInspection,
        issue_budget: &mut RestoreIndexInspectionIssueBudget,
    ) -> Result<(), MetadError> {
        let complete_ref_sets = inspection
            .ref_sets
            .iter()
            .filter_map(|(ref_set_id, ref_set)| {
                ref_set
                    .complete_identity
                    .as_ref()
                    .map(|identity| (*ref_set_id, identity.operation_digest))
            })
            .collect::<Vec<_>>();
        for (ref_set_id, operation_digest) in complete_ref_sets {
            let prefix = restore_staging_member_prefix(self.mount, ref_set_id);
            let mut scanned = 0_usize;
            self.restore_index_for_each_raw(
                RecordFamily::System,
                &prefix,
                None,
                read_version,
                ReadPurpose::WritePlanLocal,
                RESTORE_INDEX_SCAN_PAGE,
                |row| {
                    scanned = scanned.saturating_add(1);
                    validate_restore_index_collection(
                        "restore index fsck staging members",
                        scanned,
                    )?;
                    let Ok(member) = decode_restore_staging_member(&row.value.0) else {
                        // The staging-member fsck owns malformed staging rows.
                        return Ok(true);
                    };
                    let Some(source_inode) = member.source_inode else {
                        return Ok(true);
                    };
                    let source_key =
                        restore_index_source_member_key(self.mount, ref_set_id, source_inode);
                    let matches = self
                        .metadata
                        .get(
                            RecordFamily::System,
                            &source_key,
                            read_version,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .is_some_and(|value| {
                            decode_restore_index_source_member(&value.0).is_ok_and(|source| {
                                source.owner.operation_digest == operation_digest
                                    && source.owner.ref_set_id == ref_set_id
                                    && source.source_inode == source_inode
                                    && restore_index_source_member_matches_staging(
                                        &source.member,
                                        &member,
                                    )
                            })
                        });
                    if !matches {
                        let Some(ref_set) = inspection.ref_sets.get_mut(&ref_set_id) else {
                            return Err(MetadError::Codec(
                                "restore index source closure lost its ref-set".to_owned(),
                            ));
                        };
                        issue_budget.push(
                            &mut ref_set.closure_issues,
                            format!(
                                "ref-set {ref_set_id} staging member for source inode {} has no exact source member",
                                source_inode.get()
                            ),
                        );
                    }
                    Ok(true)
                },
            )?;
        }
        Ok(())
    }

    fn restore_index_inspected_row_identity(
        &self,
        name: &str,
        row: &crate::command::ScanItem,
    ) -> Result<RestoreIndexInspectedRowIdentity, MetadError> {
        if name == "index_seal" {
            let seal = decode_restore_index_seal(&row.value.0)?;
            if row.key != restore_index_seal_key(self.mount, seal.ref_set_id) {
                return Err(MetadError::Codec(
                    "restore index inspection found an invalid seal key".to_owned(),
                ));
            }
            return Ok(RestoreIndexInspectedRowIdentity {
                ref_set_id: seal.ref_set_id,
                operation_digest: seal.operation_digest,
                is_put: false,
                is_tombstone: false,
                seal_identity: Some(RestoreIndexDurableIdentity {
                    operation_digest: seal.operation_digest,
                    initialization_digest: seal.initialization_digest,
                    incarnation: seal.incarnation,
                }),
                complete_identity: None,
            });
        }
        if name == "index_complete" {
            let marker = decode_restore_index_complete(&row.value.0)?;
            if row.key != restore_index_complete_key(self.mount, marker.ref_set_id) {
                return Err(MetadError::Codec(
                    "restore index inspection found an invalid Complete key".to_owned(),
                ));
            }
            return Ok(RestoreIndexInspectedRowIdentity {
                ref_set_id: marker.ref_set_id,
                operation_digest: marker.operation_digest,
                is_put: false,
                is_tombstone: false,
                seal_identity: None,
                complete_identity: Some(RestoreIndexDurableIdentity {
                    operation_digest: marker.operation_digest,
                    initialization_digest: marker.initialization_digest,
                    incarnation: marker.incarnation,
                }),
            });
        }
        if name.starts_with("index_mvcc_") {
            let record = decode_restore_index_mvcc(&row.value.0)?;
            let commit_version = Version::new(record.commit_version)?;
            if row.version != commit_version
                || row.key
                    != restore_index_mvcc_key(self.mount, &record.logical_key, commit_version)?
            {
                return Err(MetadError::Codec(
                    "restore index inspection found an invalid MVCC physical key".to_owned(),
                ));
            }
            let label = restore_index_logical_label_for_key(self.mount, &record.logical_key)
                .ok_or_else(|| {
                    MetadError::Codec(
                        "restore index inspection found an unknown MVCC logical key".to_owned(),
                    )
                })?;
            let owner = restore_index_owner_for_logical_value(label, &record.value)?;
            return Ok(RestoreIndexInspectedRowIdentity {
                ref_set_id: owner.ref_set_id,
                operation_digest: owner.operation_digest,
                is_put: record.kind == RestoreIndexMvccKind::Put,
                is_tombstone: record.kind == RestoreIndexMvccKind::Tombstone,
                seal_identity: None,
                complete_identity: None,
            });
        }
        let label = restore_index_logical_label_for_key(self.mount, &row.key).ok_or_else(|| {
            MetadError::Codec("restore index inspection found an unknown logical key".to_owned())
        })?;
        let owner = restore_index_owner_for_logical_value(label, &row.value.0)?;
        Ok(RestoreIndexInspectedRowIdentity {
            ref_set_id: owner.ref_set_id,
            operation_digest: owner.operation_digest,
            is_put: false,
            is_tombstone: false,
            seal_identity: None,
            complete_identity: None,
        })
    }

    fn restore_index_inspected_row_closure(
        &self,
        name: &str,
        row: &crate::command::ScanItem,
        read_version: Version,
    ) -> Result<(RestoreIndexOwner, Option<String>), MetadError> {
        if name.starts_with("index_mvcc_") {
            let record = decode_restore_index_mvcc(&row.value.0)?;
            let commit_version = Version::new(record.commit_version)?;
            let logical_label =
                restore_index_logical_label_for_key(self.mount, &record.logical_key).ok_or_else(
                    || {
                        MetadError::Codec(
                            "restore index MVCC inspection found an unknown logical key".to_owned(),
                        )
                    },
                )?;
            let owner = restore_index_owner_for_logical_value(logical_label, &record.value)?;
            if row.version != commit_version
                || row.key
                    != restore_index_mvcc_key(self.mount, &record.logical_key, commit_version)?
            {
                return Err(MetadError::Codec(
                    "restore index MVCC inspection found a physical identity mismatch".to_owned(),
                ));
            }
            let Some((counterpart_label, counterpart_logical_key)) =
                restore_index_inspection_counterpart(
                    self.mount,
                    logical_label,
                    &record.logical_key,
                    &record.value,
                    false,
                )?
            else {
                return Ok((owner, None));
            };
            let counterpart_name = restore_index_inspection_keyspace_name(counterpart_label, true)?;
            let counterpart_key =
                restore_index_mvcc_key(self.mount, &counterpart_logical_key, commit_version)?;
            let mut counterpart_record = record;
            counterpart_record.logical_key = counterpart_logical_key;
            let expected = encode_restore_index_mvcc(&counterpart_record);
            let matches = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &counterpart_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .is_some_and(|item| item.value.0 == expected && item.version == commit_version);
            let issue = (!matches).then(|| {
                format!(
                    "ref-set {} {name} row has no exact {counterpart_name} at commit {}",
                    owner.ref_set_id,
                    commit_version.get()
                )
            });
            return Ok((owner, issue));
        }

        let logical_label =
            restore_index_logical_label_for_key(self.mount, &row.key).ok_or_else(|| {
                MetadError::Codec(
                    "restore index inspection found an unknown logical key".to_owned(),
                )
            })?;
        let owner = restore_index_owner_for_logical_value(logical_label, &row.value.0)?;
        if logical_label == INDEX_SOURCE_MEMBER_LABEL {
            let source = decode_restore_index_source_member(&row.value.0)?;
            let staging_key = restore_staging_member_key(
                self.mount,
                owner.ref_set_id,
                source.member.destination_inode,
            );
            let matches = self
                .metadata
                .get(
                    RecordFamily::System,
                    &staging_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .is_some_and(|value| {
                    decode_restore_staging_member(&value.0).is_ok_and(|member| {
                        restore_index_source_member_matches_staging(&source.member, &member)
                    })
                });
            return Ok((
                owner,
                (!matches).then(|| {
                    format!(
                        "ref-set {} source member has no exact staging member",
                        owner.ref_set_id
                    )
                }),
            ));
        }
        let Some((counterpart_label, counterpart_key)) = restore_index_inspection_counterpart(
            self.mount,
            logical_label,
            &row.key,
            &row.value.0,
            true,
        )?
        else {
            return Ok((owner, None));
        };
        let counterpart_name = restore_index_inspection_keyspace_name(counterpart_label, false)?;
        let expected = if logical_label == INDEX_ENTRY_LABEL {
            encode_restore_index_owner(owner)
        } else {
            row.value.0.clone()
        };
        let matches = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &counterpart_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .is_some_and(|item| {
                item.value.0 == expected
                    && (logical_label == INDEX_ENTRY_LABEL || item.version == row.version)
            });
        let issue = (!matches).then(|| {
            format!(
                "ref-set {} {name} row has no exact {counterpart_name}",
                owner.ref_set_id
            )
        });
        Ok((owner, issue))
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_index_for_each_raw<F>(
        &self,
        family: RecordFamily,
        prefix: &[u8],
        initial_start_after: Option<&[u8]>,
        version: Version,
        purpose: ReadPurpose,
        page_limit: usize,
        mut visit: F,
    ) -> Result<(), MetadError>
    where
        F: FnMut(crate::command::ScanItem) -> Result<bool, MetadError>,
    {
        if page_limit == 0 || page_limit > RESTORE_INDEX_SCAN_PAGE {
            return Err(MetadError::Codec(format!(
                "restore index scan page {page_limit} is outside 1..={RESTORE_INDEX_SCAN_PAGE}"
            )));
        }
        let mut start_after = initial_start_after.map(ToOwned::to_owned);
        loop {
            let page = self.metadata.scan(ScanRequest {
                family,
                prefix: prefix.to_vec(),
                start_after: start_after.clone(),
                version,
                limit: page_limit,
                purpose,
            })?;
            if page.is_empty() {
                break;
            }
            let reached_tail = page.len() < page_limit;
            let mut previous = start_after.as_deref();
            for row in &page {
                if !row.key.starts_with(prefix)
                    || previous.is_some_and(|previous| previous >= row.key.as_slice())
                {
                    return Err(MetadError::Codec(
                        "restore index paged scan returned an invalid key order".to_owned(),
                    ));
                }
                previous = Some(&row.key);
            }
            start_after = page.last().map(|row| row.key.clone());
            for row in page {
                if !visit(row)? {
                    return Ok(());
                }
            }
            if reached_tail {
                break;
            }
        }
        Ok(())
    }

    fn restore_index_for_each_mvcc_latest<F>(
        &self,
        logical_prefix: &[u8],
        logical_start_after: Option<&[u8]>,
        exact: bool,
        requested_version: Version,
        purpose: ReadPurpose,
        mut visit: F,
    ) -> Result<(), MetadError>
    where
        F: FnMut(SelectedRestoreIndexMvcc) -> Result<bool, MetadError>,
    {
        let mut physical_prefix = restore_index_mvcc_prefix(self.mount, logical_prefix)?;
        if exact {
            physical_prefix.push(RESTORE_INDEX_MVCC_VERSION_DELIMITER);
        }
        let mut start_after = logical_start_after
            .map(|logical_key| {
                restore_index_mvcc_key(
                    self.mount,
                    logical_key,
                    Version::new(u64::MAX).expect("u64::MAX is a valid non-zero version"),
                )
            })
            .transpose()?;
        // System is current-only, so the physical COW rows are always read at
        // the current control version and selected by their embedded commit
        // version. Reading them at `requested_version` would silently drop the
        // very history this keyspace exists to preserve.
        let control_version = self.read_version()?;
        let scan_purpose = match purpose {
            ReadPurpose::Snapshot => ReadPurpose::WritePlanLocal,
            other => other,
        };
        let mut current_logical = None::<Vec<u8>>;
        let mut current_candidate = None::<SelectedRestoreIndexMvcc>;
        loop {
            let page = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: physical_prefix.clone(),
                start_after: start_after.clone(),
                version: control_version,
                limit: RESTORE_INDEX_SCAN_PAGE,
                purpose: scan_purpose,
            })?;
            if page.is_empty() {
                break;
            }
            let reached_tail = page.len() < RESTORE_INDEX_SCAN_PAGE;
            for item in &page {
                let record = decode_restore_index_mvcc(&item.value.0)?;
                let commit_version = Version::new(record.commit_version)?;
                if item.version != commit_version
                    || item.key
                        != restore_index_mvcc_key(self.mount, &record.logical_key, commit_version)?
                    || !record.logical_key.starts_with(logical_prefix)
                    || (exact && record.logical_key != logical_prefix)
                {
                    return Err(MetadError::Codec(
                        "restore index MVCC physical/logical identity changed".to_owned(),
                    ));
                }
                if current_logical
                    .as_ref()
                    .is_some_and(|logical| logical != &record.logical_key)
                {
                    if current_logical
                        .as_ref()
                        .is_some_and(|logical| logical >= &record.logical_key)
                    {
                        return Err(MetadError::Codec(
                            "restore index MVCC logical rows are not strictly ordered".to_owned(),
                        ));
                    }
                    if let Some(candidate) = current_candidate.take() {
                        if !visit(candidate)? {
                            return Ok(());
                        }
                    }
                    current_logical = Some(record.logical_key.clone());
                } else if current_logical.is_none() {
                    current_logical = Some(record.logical_key.clone());
                }
                if record.commit_version <= requested_version.get() {
                    if current_candidate.as_ref().is_some_and(|candidate| {
                        candidate.record.commit_version >= record.commit_version
                    }) {
                        return Err(MetadError::Codec(
                            "restore index MVCC versions are not strictly ordered".to_owned(),
                        ));
                    }
                    current_candidate = Some(SelectedRestoreIndexMvcc {
                        logical_key: record.logical_key.clone(),
                        record,
                    });
                }
            }
            start_after = page.last().map(|item| item.key.clone());
            if reached_tail {
                break;
            }
        }
        if let Some(candidate) = current_candidate {
            let _ = visit(candidate)?;
        }
        Ok(())
    }

    fn restore_index_mvcc_value(
        &self,
        logical_key: &[u8],
        requested_version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<SelectedRestoreIndexMvcc>, MetadError> {
        let mut selected = None;
        self.restore_index_for_each_mvcc_latest(
            logical_key,
            None,
            true,
            requested_version,
            purpose,
            |row| {
                if selected.replace(row).is_some() {
                    return Err(MetadError::Codec(
                        "restore index MVCC exact lookup is ambiguous".to_owned(),
                    ));
                }
                Ok(true)
            },
        )?;
        Ok(selected)
    }

    fn restore_index_effective_for_each<F>(
        &self,
        logical_prefix: &[u8],
        logical_start_after: Option<&[u8]>,
        requested_version: Version,
        purpose: ReadPurpose,
        mut visit: F,
    ) -> Result<(), MetadError>
    where
        F: FnMut(crate::command::ScanItem) -> Result<bool, MetadError>,
    {
        if purpose == ReadPurpose::Snapshot {
            return self.restore_index_for_each_mvcc_latest(
                logical_prefix,
                logical_start_after,
                false,
                requested_version,
                purpose,
                |row| {
                    if row.record.kind == RestoreIndexMvccKind::Tombstone {
                        return Ok(true);
                    }
                    visit(crate::command::ScanItem {
                        key: row.logical_key,
                        value: Value(row.record.value),
                        version: Version::new(row.record.commit_version)?,
                    })
                },
            );
        }

        let mut start_after = logical_start_after.map(ToOwned::to_owned);
        loop {
            let page = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: logical_prefix.to_vec(),
                start_after: start_after.clone(),
                version: requested_version,
                limit: RESTORE_INDEX_SCAN_PAGE,
                purpose,
            })?;
            if page.is_empty() {
                break;
            }
            let reached_tail = page.len() < RESTORE_INDEX_SCAN_PAGE;
            for row in &page {
                if !visit(row.clone())? {
                    return Ok(());
                }
            }
            start_after = page.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }
        Ok(())
    }

    fn restore_index_effective_get(
        &self,
        logical_key: &[u8],
        requested_version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<crate::command::ReadItem>, MetadError> {
        if purpose != ReadPurpose::Snapshot {
            return Ok(self.metadata.get_versioned(
                RecordFamily::System,
                logical_key,
                requested_version,
                purpose,
            )?);
        }
        let Some(row) = self.restore_index_mvcc_value(logical_key, requested_version, purpose)?
        else {
            return Ok(None);
        };
        if row.record.kind == RestoreIndexMvccKind::Tombstone {
            return Ok(None);
        }
        Ok(Some(crate::command::ReadItem {
            value: Value(row.record.value),
            version: Version::new(row.record.commit_version)?,
        }))
    }

    fn finalize_restore_index_mvcc_plan(
        &self,
        mut plan: RestoreIndexMutationPlan,
        read_version: Version,
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        plan.validate_budget("restore index mutation plan")?;
        let head_mutations = plan.mutations.clone();
        for mutation in head_mutations {
            if mutation.family != RecordFamily::System
                || restore_index_logical_label_for_key(self.mount, &mutation.key).is_none()
            {
                continue;
            }
            let (kind, value) = match mutation.op {
                MutationOp::Put => (
                    RestoreIndexMvccKind::Put,
                    mutation
                        .value
                        .as_ref()
                        .ok_or_else(|| {
                            MetadError::Codec("restore index put mutation has no value".to_owned())
                        })?
                        .0
                        .clone(),
                ),
                MutationOp::Delete => {
                    let existing = self
                        .metadata
                        .get(
                            RecordFamily::System,
                            &mutation.key,
                            read_version,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "restore index tombstone has no durable head".to_owned(),
                            )
                        })?;
                    (RestoreIndexMvccKind::Tombstone, existing.0)
                }
            };
            let physical_key = restore_index_mvcc_key(self.mount, &mutation.key, commit_version)?;
            plan.push_predicate(PredicateRef {
                family: RecordFamily::System,
                key: physical_key.clone(),
                predicate: Predicate::NotExists,
            })?;
            plan.set_mutation(Mutation {
                family: RecordFamily::System,
                key: physical_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_index_mvcc(&RestoreIndexMvccRecord {
                    commit_version: commit_version.get(),
                    kind,
                    logical_key: mutation.key,
                    value,
                }))),
            });
            plan.validate_budget("restore index mutation plan with MVCC")?;
        }
        Ok(plan)
    }

    /// Attach-time visibility pointer. The caller must merge this plan into
    /// the single command that publishes the destination dentry and changes
    /// the operation to `Complete`.
    pub(super) fn restore_index_complete_plan(
        &self,
        operation: &RestoreOperation,
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        let key = restore_index_complete_key(self.mount, operation.ref_set_id);
        let marker = RestoreIndexCompleteMarker {
            operation_digest: operation.operation_digest,
            initialization_digest: operation.initialization_digest,
            ref_set_id: operation.ref_set_id,
            incarnation: operation.created_version,
            complete_version: commit_version.get(),
        };
        Ok(RestoreIndexMutationPlan {
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_index_complete(&marker))),
            }],
        })
    }

    /// Ordinary inode-addressed reads must not bypass the detached restore's
    /// first-visibility boundary. Only explicit restore staging reads may
    /// materialize and seal the tree before attach. Callers hold
    /// `restore_visibility_fence` through this check and the namespace read,
    /// which linearizes the false fast path with the first durable hold.
    pub(super) fn restore_inode_visible_at(
        &self,
        inode: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<bool, MetadError> {
        self.ensure_metadata_checkpoint_install_stable()?;
        if purpose == ReadPurpose::RestoreStaging {
            return Ok(true);
        }
        if !self.restore_staging_possible.load(Ordering::Acquire) {
            return Ok(true);
        }
        let control_version = self.read_version()?;
        let Some(inverse) = self.metadata.get(
            RecordFamily::System,
            &restore_staging_inode_key(self.mount, inode),
            control_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(true);
        };
        let (operation_digest, ref_set_id) = decode_restore_staging_inverse(&inverse.0)?;
        let operation = self
            .metadata
            .get(
                RecordFamily::System,
                &restore_operation_key(self.mount, &operation_digest),
                control_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore staging inode has no operation".to_owned())
            })?;
        let operation = decode_restore_operation(&operation.0)?;
        if operation.operation_digest != operation_digest || operation.ref_set_id != ref_set_id {
            return Err(MetadError::Codec(
                "restore staging inode does not match its operation".to_owned(),
            ));
        }
        let marker_key = restore_index_complete_key(self.mount, ref_set_id);
        let marker = self
            .metadata
            .get(
                RecordFamily::System,
                &marker_key,
                control_version,
                ReadPurpose::WritePlanLocal,
            )?
            .map(|value| decode_restore_index_complete(&value.0))
            .transpose()?;
        let marker_visible = marker.as_ref().is_some_and(|marker| {
            marker.operation_digest == operation.operation_digest
                && marker.initialization_digest == operation.initialization_digest
                && marker.ref_set_id == operation.ref_set_id
                && marker.incarnation == operation.created_version
                && marker.complete_version <= version.get()
        });
        match operation.state {
            RestoreOperationState::Complete if purpose == ReadPurpose::Snapshot => {
                Ok(marker_visible)
            }
            RestoreOperationState::Complete => Ok(marker_visible
                && self.restore_inode_reachable_from_mount(inode, control_version)?),
            RestoreOperationState::Releasing if purpose == ReadPurpose::Snapshot => {
                // Release is fenced by live SnapshotPin/ref-set retention. A
                // historical snapshot must not be filtered through current
                // namespace reachability after the root was removed.
                Ok(marker_visible)
            }
            RestoreOperationState::Releasing => Ok(marker_visible
                && self.restore_inode_reachable_from_mount(inode, control_version)?),
            RestoreOperationState::Preparing
            | RestoreOperationState::ReadyToAttach
            | RestoreOperationState::Cleaning
            | RestoreOperationState::Discarding => Ok(false),
        }
    }

    /// A Complete operation may enable the process-local read fast path only
    /// when its visibility marker is durably present and matches the exact
    /// operation incarnation. This is checked during explicit recovery hooks,
    /// never on an ordinary read.
    pub(super) fn validate_restore_complete_visibility_marker(
        &self,
        operation: &RestoreOperation,
        version: Version,
    ) -> Result<(), MetadError> {
        let marker = self
            .metadata
            .get(
                RecordFamily::System,
                &restore_index_complete_key(self.mount, operation.ref_set_id),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("complete restore operation has no visibility marker".to_owned())
            })?;
        let marker = decode_restore_index_complete(&marker.0)?;
        if marker.operation_digest != operation.operation_digest
            || marker.initialization_digest != operation.initialization_digest
            || marker.ref_set_id != operation.ref_set_id
            || marker.incarnation != operation.created_version
            || marker.complete_version > version.get()
        {
            return Err(MetadError::Codec(
                "complete restore visibility marker changed identity".to_owned(),
            ));
        }
        Ok(())
    }

    fn visible_restore_index_operation(
        &self,
        owner: RestoreIndexOwner,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<RestoreOperation>, MetadError> {
        Ok(self
            .visible_restore_index_operation_versioned(owner, version, purpose)?
            .map(|(operation, _)| operation))
    }

    fn visible_restore_index_operation_versioned(
        &self,
        owner: RestoreIndexOwner,
        version: Version,
        _purpose: ReadPurpose,
    ) -> Result<Option<(RestoreOperation, Version)>, MetadError> {
        let key = restore_operation_key(self.mount, &owner.operation_digest);
        let control_version = self.read_version()?;
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::System,
            &key,
            control_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::Codec(
                "restore index owner has no operation".to_owned(),
            ));
        };
        let operation = decode_restore_operation(&item.value.0)?;
        if operation.operation_digest != owner.operation_digest
            || operation.ref_set_id != owner.ref_set_id
        {
            return Err(MetadError::Codec(
                "restore index owner does not match its operation".to_owned(),
            ));
        }
        if !matches!(
            operation.state,
            RestoreOperationState::Complete | RestoreOperationState::Releasing
        ) {
            return Ok(None);
        }
        let marker_key = restore_index_complete_key(self.mount, owner.ref_set_id);
        let marker = self
            .metadata
            .get(
                RecordFamily::System,
                &marker_key,
                control_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("visible restore index has no Complete marker".to_owned())
            })?;
        let marker = decode_restore_index_complete(&marker.0)?;
        if marker.operation_digest != operation.operation_digest
            || marker.initialization_digest != operation.initialization_digest
            || marker.ref_set_id != operation.ref_set_id
            || marker.incarnation != operation.created_version
        {
            return Err(MetadError::Codec(
                "restore index Complete marker changed identity".to_owned(),
            ));
        }
        if marker.complete_version > version.get() {
            return Ok(None);
        }
        Ok(Some((operation, item.version)))
    }

    fn restore_index_staging_guards(
        &self,
        operation: &RestoreOperation,
        version: Version,
    ) -> Result<Vec<PredicateRef>, MetadError> {
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreInProgress)?;
        let durable = decode_restore_operation(&operation_item.value.0)?;
        if durable != *operation || durable.state != RestoreOperationState::Preparing {
            return Err(MetadError::RestoreInProgress);
        }
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let binding_item = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            })?;
        let binding = crate::layout::decode_fork_binding(&binding_item.value.0)
            .map_err(|error| MetadError::Codec(error.to_string()))?;
        if binding.fork_root != operation.destination_root
            || binding.source_root != operation.source_root
            || binding.pinned_read_version != operation.read_version
            || binding.snapshot_id != operation.snapshot_id
            || binding.created_version != operation.created_version
        {
            return Err(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            });
        }
        Ok(vec![
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key,
                predicate: Predicate::VersionEquals(operation_item.version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key,
                predicate: Predicate::VersionEquals(binding_item.version),
            },
        ])
    }

    fn restore_index_unique_puts(
        desired: Vec<Mutation>,
        context: &str,
    ) -> Result<BTreeMap<Vec<u8>, Mutation>, MetadError> {
        let mut unique = BTreeMap::<Vec<u8>, Mutation>::new();
        for mutation in desired {
            if mutation.family != RecordFamily::System
                || mutation.op != MutationOp::Put
                || mutation.value.is_none()
            {
                return Err(MetadError::Codec(format!(
                    "restore index {context} emitted an invalid mutation"
                )));
            }
            match unique.entry(mutation.key.clone()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(mutation);
                }
                std::collections::btree_map::Entry::Occupied(slot)
                    if slot.get().value != mutation.value =>
                {
                    return Err(MetadError::Codec(format!(
                        "restore index {context} disagrees on a durable row"
                    )));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
        Ok(unique)
    }

    fn append_fresh_restore_index_put(
        &self,
        mutation: Mutation,
        version: Version,
        predicates: &mut Vec<PredicateRef>,
        mutations: &mut Vec<Mutation>,
    ) -> Result<(), MetadError> {
        let desired_value = mutation
            .value
            .as_ref()
            .ok_or_else(|| MetadError::Codec("restore index fresh put has no value".to_owned()))?;
        let physical_key = restore_index_mvcc_key(self.mount, &mutation.key, version)?;
        predicates.extend([
            PredicateRef {
                family: RecordFamily::System,
                key: mutation.key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: physical_key.clone(),
                predicate: Predicate::NotExists,
            },
        ]);
        mutations.push(Mutation {
            family: RecordFamily::System,
            key: physical_key,
            op: MutationOp::Put,
            value: Some(Value(encode_restore_index_mvcc(&RestoreIndexMvccRecord {
                commit_version: version.get(),
                kind: RestoreIndexMvccKind::Put,
                logical_key: mutation.key.clone(),
                value: desired_value.0.clone(),
            }))),
        });
        mutations.push(mutation);
        Ok(())
    }

    fn restore_index_materialization_command(
        &self,
        operation: &RestoreOperation,
        primary_key: Vec<u8>,
        version: Version,
        predicates: Vec<PredicateRef>,
        mutations: Vec<Mutation>,
    ) -> Result<MetadataCommand, MetadError> {
        Ok(MetadataCommand {
            request_id: request_id(
                b"restore-materialize-index",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::RegisterNamespaceIndex,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key,
            predicates,
            mutations,
            watch: Vec::new(),
        })
    }

    /// Validate the fresh-operation command shape without consulting or
    /// mutating durable restore state. A new ref-set has no existing heads, so
    /// every desired owner/inverse row carries the same head + physical-MVCC
    /// predicates and puts that `commit_restore_index_put_batch` will emit
    /// after the hold.
    fn preflight_restore_index_put_batch(
        &self,
        operation: &RestoreOperation,
        primary_key: Vec<u8>,
        desired: Vec<Mutation>,
    ) -> Result<(), MetadError> {
        if desired.is_empty() {
            return Ok(());
        }
        let unique = Self::restore_index_unique_puts(desired, "preflight")?;

        let version = Version::new(operation.created_version)?;
        let read_version = predecessor(version)?;
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(read_version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: fork_binding_key(self.mount, operation.destination_root),
                predicate: Predicate::VersionEquals(read_version),
            },
        ];
        let mut mutations = Vec::with_capacity(unique.len() * 2);
        for (_, mutation) in unique {
            self.append_fresh_restore_index_put(
                mutation,
                version,
                &mut predicates,
                &mut mutations,
            )?;
        }
        let command = self.restore_index_materialization_command(
            operation,
            primary_key,
            version,
            predicates,
            mutations,
        )?;
        super::restore::validate_restore_command_bounds(
            &command,
            "restore index materialization batch",
        )
    }

    fn commit_restore_index_put_batch(
        &self,
        operation: &RestoreOperation,
        primary_key: Vec<u8>,
        desired: Vec<Mutation>,
    ) -> Result<(), MetadError> {
        if desired.is_empty() {
            return Ok(());
        }
        let unique = Self::restore_index_unique_puts(desired, "materialization")?;

        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let mut predicates = self.restore_index_staging_guards(operation, read_version)?;
        let mut mutations = Vec::with_capacity(unique.len() * 2);
        for (_, mutation) in unique {
            let desired_value = mutation.value.as_ref().expect("put value");
            match self.metadata.get_versioned(
                RecordFamily::System,
                &mutation.key,
                read_version,
                ReadPurpose::RestoreStaging,
            )? {
                Some(existing) if existing.value == *desired_value => {
                    let history = self
                        .restore_index_mvcc_value(
                            &mutation.key,
                            read_version,
                            ReadPurpose::RestoreStaging,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "restore index durable head has no MVCC Put".to_owned(),
                            )
                        })?;
                    if history.record.kind != RestoreIndexMvccKind::Put
                        || history.record.commit_version != existing.version.get()
                        || history.record.value != desired_value.0
                    {
                        return Err(MetadError::Codec(
                            "restore index durable head/MVCC Put mismatch".to_owned(),
                        ));
                    }
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: mutation.key,
                        predicate: Predicate::VersionEquals(existing.version),
                    });
                }
                Some(_) => {
                    return Err(MetadError::Codec(
                        "restore index materialization found a conflicting row".to_owned(),
                    ))
                }
                None => {
                    self.append_fresh_restore_index_put(
                        mutation,
                        version,
                        &mut predicates,
                        &mut mutations,
                    )?;
                }
            }
        }
        if mutations.is_empty() {
            return Ok(());
        }
        let command = self.restore_index_materialization_command(
            operation,
            primary_key,
            version,
            predicates,
            mutations,
        )?;
        super::restore::validate_restore_command_bounds(
            &command,
            "restore index materialization batch",
        )?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn restore_index_staging_member_from_row(
        &self,
        operation: &RestoreOperation,
        version: Version,
        row: &crate::command::ScanItem,
    ) -> Result<RestoreStagingMember, MetadError> {
        let member = decode_restore_staging_member(&row.value.0)?;
        if member.operation_digest != operation.operation_digest
            || row.key
                != restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    member.destination_inode,
                )
        {
            return Err(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            });
        }
        let inverse_key = restore_staging_inode_key(self.mount, member.destination_inode);
        let inverse = self
            .metadata
            .get(
                RecordFamily::System,
                &inverse_key,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            })?;
        let (digest, ref_set_id) = decode_restore_staging_inverse(&inverse.0)?;
        if digest != operation.operation_digest || ref_set_id != operation.ref_set_id {
            return Err(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            });
        }
        Ok(member)
    }

    fn restore_index_projection_for_member(
        &self,
        operation: &RestoreOperation,
        member: &RestoreStagingMember,
        destination_parent: InodeId,
        destination_name: &DentryName,
        version: Version,
    ) -> Result<DentryProjection, MetadError> {
        if member.destination_inode == operation.destination_root {
            if member.destination_parent.is_some()
                || member.name.is_some()
                || !member.relative_path.is_empty()
            {
                return Err(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                });
            }
            let attr = self
                .get_attr_at_version_for_purpose(
                    operation.destination_root,
                    version,
                    ReadPurpose::RestoreStaging,
                )?
                .ok_or(MetadError::RestoreRootChanged {
                    root: operation.destination_root,
                })?;
            return Ok(projection(
                destination_parent,
                destination_name.clone(),
                attr,
                None,
            ));
        }
        let (Some(parent), Some(name)) = (member.destination_parent, member.name.as_ref()) else {
            return Err(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            });
        };
        let (entry, _) = self
            .lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::RestoreStaging)?
            .ok_or(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            })?;
        if entry.attr.inode != member.destination_inode {
            return Err(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            });
        }
        Ok(DentryProjection {
            dentry: entry.dentry,
            attr: entry.attr,
            body: entry.body,
        })
    }

    fn flush_restore_canonical_index_preflight_batch(
        &self,
        operation: &RestoreOperation,
        batch: &mut RestoreCanonicalIndexPreflightBatch,
    ) -> Result<(), MetadError> {
        if batch.members == 0 {
            return Ok(());
        }
        let desired = std::mem::take(&mut batch.desired);
        batch.members = 0;
        self.preflight_restore_index_put_batch(
            operation,
            restore_index_entry_prefix(self.mount, operation.ref_set_id, None),
            desired,
        )
    }

    /// Feed the initialization-adjusted final staging-member stream in
    /// destination-inode order. The indexed predicate and mutation builders are
    /// shared with runtime materialization; only the durable reads/commit are
    /// replaced by the fresh-ref-set command-shape validator.
    pub(super) fn push_restore_canonical_index_preflight_member(
        &self,
        operation: &RestoreOperation,
        member: &RestoreStagingMember,
        projection: &DentryProjection,
        batch: &mut RestoreCanonicalIndexPreflightBatch,
    ) -> Result<(), MetadError> {
        if let Some(source_inode) = member.source_inode {
            batch.desired.push(restore_index_source_member_mutation(
                self.mount,
                &RestoreIndexSourceMember {
                    owner: RestoreIndexOwner::from_operation(operation),
                    source_inode,
                    member: member.clone(),
                },
            )?);
        }
        if self.restore_source_member_is_indexed(operation, member, projection)? {
            batch.desired.extend(
                restore_index_materialization_mutations(self.mount, operation, projection)
                    .mutations,
            );
        }
        batch.members = batch.members.saturating_add(1);
        if batch.members == RESTORE_BATCH_ENTRIES {
            self.flush_restore_canonical_index_preflight_batch(operation, batch)?;
        }
        Ok(())
    }

    pub(super) fn finish_restore_canonical_index_preflight(
        &self,
        operation: &RestoreOperation,
        batch: &mut RestoreCanonicalIndexPreflightBatch,
    ) -> Result<(), MetadError> {
        self.flush_restore_canonical_index_preflight_batch(operation, batch)
    }

    fn materialize_restore_canonical_indexes(
        &self,
        operation: &RestoreOperation,
        destination_parent: InodeId,
        destination_name: &DentryName,
    ) -> Result<(), MetadError> {
        let scan_version = self.read_version()?;
        let prefix = restore_staging_member_prefix(self.mount, operation.ref_set_id);
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version: scan_version,
                limit: RESTORE_BATCH_ENTRIES,
                purpose: ReadPurpose::RestoreStaging,
            })?;
            if rows.is_empty() {
                break;
            }
            let reached_tail = rows.len() < RESTORE_BATCH_ENTRIES;
            let version = self.read_version()?;
            let mut desired = Vec::with_capacity(rows.len() * 4);
            for row in &rows {
                let member =
                    self.restore_index_staging_member_from_row(operation, scan_version, row)?;
                if let Some(source_inode) = member.source_inode {
                    desired.push(restore_index_source_member_mutation(
                        self.mount,
                        &RestoreIndexSourceMember {
                            owner: RestoreIndexOwner::from_operation(operation),
                            source_inode,
                            member: member.clone(),
                        },
                    )?);
                }
                let projection = self.restore_index_projection_for_member(
                    operation,
                    &member,
                    destination_parent,
                    destination_name,
                    version,
                )?;
                if self.restore_source_member_is_indexed(operation, &member, &projection)? {
                    desired.extend(
                        restore_index_materialization_mutations(self.mount, operation, &projection)
                            .mutations,
                    );
                }
            }
            self.commit_restore_index_put_batch(
                operation,
                restore_index_entry_prefix(self.mount, operation.ref_set_id, None),
                desired,
            )?;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }
        Ok(())
    }

    fn restore_source_member_is_indexed(
        &self,
        operation: &RestoreOperation,
        member: &RestoreStagingMember,
        destination: &DentryProjection,
    ) -> Result<bool, MetadError> {
        // Initialization-created/replaced artifacts follow the normal
        // path-aware publish behavior. Directories and bodiless nodes are not
        // introduced into an index merely because restore traversed them.
        let Some(source_inode) = member.source_inode else {
            return Ok(destination.body.is_some());
        };
        let mut components = parse_absolute_path(&operation.source_path)?;
        if !member.relative_path.is_empty() {
            components.extend(parse_absolute_path(&format!("/{}", member.relative_path))?);
        }
        let Some((name, parent_components)) = components.split_last() else {
            return Ok(false);
        };
        let source_version = Version::new(operation.read_version)?;
        let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
            InodeId::root(),
            parent_components,
            source_version,
            ReadPurpose::Snapshot,
        ) {
            Ok(parent) => parent,
            Err(MetadError::NotFound | MetadError::NotDirectory) => return Ok(false),
            Err(error) => return Err(error),
        };
        let Some((source, _)) = self.lookup_plus_at_version_for_purpose(
            parent,
            name,
            source_version,
            ReadPurpose::Snapshot,
        )?
        else {
            return Ok(false);
        };
        if source.attr.inode != source_inode {
            return Err(MetadError::RestoreRootChanged {
                root: operation.source_root,
            });
        }
        let source_projection = DentryProjection {
            dentry: source.dentry,
            attr: source.attr,
            body: source.body,
        };
        let path_key = path_index_key(self.mount, &components);
        if let Some(value) = self.metadata.get(
            RecordFamily::PathIndex,
            &path_key,
            source_version,
            ReadPurpose::Snapshot,
        )? {
            let indexed = decode_dentry_projection(&value.0)
                .map_err(|error| MetadError::Codec(error.to_string()))?;
            if indexed == source_projection {
                return Ok(true);
            }
        }
        if self
            .restore_index_entries_for_location_at(
                parent,
                name,
                source_version,
                ReadPurpose::Snapshot,
            )?
            .into_iter()
            .any(|indexed| indexed.entry.projection == source_projection)
        {
            return Ok(true);
        }

        let Some(fork_source) = self.restore_index_fork_source(operation.source_root)? else {
            return Ok(false);
        };
        if member.relative_path.is_empty() {
            return Ok(false);
        }
        let relative = parse_absolute_path(&format!("/{}", member.relative_path))?;
        let Some((source_name, source_parent_components)) = relative.split_last() else {
            return Ok(false);
        };
        let pinned_version = Version::new(fork_source.binding.pinned_read_version)?;
        let source_parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            fork_source.binding.source_root,
            source_parent_components,
            pinned_version,
            ReadPurpose::Snapshot,
        )?;
        let Some((fork_origin, _)) = self.lookup_plus_at_version_for_purpose(
            source_parent,
            source_name,
            pinned_version,
            ReadPurpose::Snapshot,
        )?
        else {
            return Ok(false);
        };
        let clone_source: DentryWithAttr = source_projection.into();
        if !restore_index_clone_entry_matches_source(&clone_source, &fork_origin) {
            return Ok(false);
        }
        let origin_projection = DentryProjection {
            dentry: fork_origin.dentry.clone(),
            attr: fork_origin.attr.clone(),
            body: fork_origin.body.clone(),
        };
        let origin_components = {
            let mut components = parse_absolute_path(&fork_source.operation.destination_path)?;
            components.extend_from_slice(&relative);
            components
        };
        if let Some(value) = self.metadata.get(
            RecordFamily::PathIndex,
            &path_index_key(self.mount, &origin_components),
            pinned_version,
            ReadPurpose::Snapshot,
        )? {
            let indexed = decode_dentry_projection(&value.0)
                .map_err(|error| MetadError::Codec(error.to_string()))?;
            if indexed == origin_projection {
                return Ok(true);
            }
        }
        Ok(self
            .restore_index_entries_for_location_at(
                source_parent,
                source_name,
                pinned_version,
                ReadPurpose::Snapshot,
            )?
            .into_iter()
            .any(|indexed| indexed.entry.projection == origin_projection))
    }

    fn restore_index_metadata_for_inode(
        &self,
        inode: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<PathMetadata>, MetadError> {
        let Some(attr) = self.get_attr_at_version_for_purpose(inode, version, purpose)? else {
            return Ok(None);
        };
        let body = if attr.file_type == FileType::File {
            self.body_descriptor_at_version_for_purpose(inode, attr.generation, version, purpose)?
        } else {
            None
        };
        Ok(Some(PathMetadata { attr, body }))
    }

    fn restore_index_staging_member_for_inode(
        &self,
        operation: &RestoreOperation,
        inode: InodeId,
        version: Version,
    ) -> Result<Option<RestoreStagingMember>, MetadError> {
        let key = restore_staging_member_key(self.mount, operation.ref_set_id, inode);
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::System,
            &key,
            version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(None);
        };
        let row = crate::command::ScanItem {
            key,
            value: item.value,
            version: item.version,
        };
        Ok(Some(self.restore_index_staging_member_from_row(
            operation, version, &row,
        )?))
    }

    fn restore_index_staging_member_for_relative(
        &self,
        operation: &RestoreOperation,
        relative: &[DentryName],
        version: Version,
    ) -> Result<Option<RestoreStagingMember>, MetadError> {
        let relative_string = restore_index_relative_string(relative)?;
        let inode = if relative.is_empty() {
            operation.destination_root
        } else {
            let relative_path = canonical_path(relative)?;
            let Some(metadata) = self.stat_path_from_at_version_for_purpose(
                operation.destination_root,
                &relative_path,
                version,
                ReadPurpose::RestoreStaging,
            )?
            else {
                return Ok(None);
            };
            metadata.attr.inode
        };
        let Some(member) =
            self.restore_index_staging_member_for_inode(operation, inode, version)?
        else {
            return Ok(None);
        };
        if member.relative_path != relative_string {
            return Err(MetadError::RestoreRootChanged { root: inode });
        }
        Ok(Some(member))
    }

    fn restore_index_staging_member_for_source_inode(
        &self,
        operation: &RestoreOperation,
        source_inode: InodeId,
        version: Version,
    ) -> Result<Option<RestoreStagingMember>, MetadError> {
        let key = restore_index_source_member_key(self.mount, operation.ref_set_id, source_inode);
        let Some(value) = self.metadata.get(
            RecordFamily::System,
            &key,
            version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(None);
        };
        let source = decode_restore_index_source_member(&value.0)?;
        if source.owner != RestoreIndexOwner::from_operation(operation)
            || source.source_inode != source_inode
            || key
                != restore_index_source_member_key(
                    self.mount,
                    source.owner.ref_set_id,
                    source.source_inode,
                )
        {
            return Err(MetadError::RestoreRootChanged {
                root: operation.destination_root,
            });
        }
        let Some(current) = self.restore_index_staging_member_for_inode(
            operation,
            source.member.destination_inode,
            version,
        )?
        else {
            return Err(MetadError::RestoreRootChanged {
                root: source.member.destination_inode,
            });
        };
        if current != source.member {
            return Err(MetadError::RestoreRootChanged {
                root: source.member.destination_inode,
            });
        }
        Ok(Some(source.member))
    }

    fn preflight_restore_index_target(
        &self,
        catalog_root: InodeId,
        relative: &[DentryName],
        version: Version,
    ) -> Result<RestoreIndexTarget, MetadError> {
        if relative.is_empty() {
            let metadata = self
                .restore_index_metadata_for_inode(catalog_root, version, ReadPurpose::Snapshot)?
                .ok_or(MetadError::RestoreRootChanged { root: catalog_root })?;
            return Ok(restore_index_root_target(&metadata));
        }
        let relative_path = canonical_path(relative)?;
        let Some((entry, _)) = self.lookup_path_from_at_version_for_purpose(
            catalog_root,
            &relative_path,
            version,
            ReadPurpose::Snapshot,
        )?
        else {
            return Err(MetadError::RestoreRootChanged { root: catalog_root });
        };
        Ok(restore_index_target_from_projection(&DentryProjection {
            dentry: entry.dentry,
            attr: entry.attr,
            body: entry.body,
        }))
    }

    /// Stream one directory discovered by the existing subtree preflight.
    /// The cheap exact-root probes avoid an O(subtree) custom-index merge for
    /// directories with no catalog, while canonical, completed-overlay, and
    /// generic-fork catalogs all flow through the same effective snapshot view
    /// used by runtime materialization.
    pub(super) fn preflight_restore_custom_index_catalog(
        &self,
        operation: &RestoreOperation,
        source_catalog_root: InodeId,
        catalog_relative: &[DentryName],
        initialization_paths: &HashSet<String>,
    ) -> Result<(), MetadError> {
        let source_version = Version::new(operation.read_version)?;
        let source_catalog_path =
            restore_index_join_path(&operation.source_path, catalog_relative)?;
        let canonical_exists = self
            .metadata
            .get(
                RecordFamily::PathIndex,
                &path_index_catalog_key(self.mount, &source_catalog_path),
                source_version,
                ReadPurpose::Snapshot,
            )?
            .is_some();
        let mut overlay_exists = false;
        let inverse_prefix = restore_index_catalog_inverse_prefix(self.mount, source_catalog_root);
        self.restore_index_effective_for_each(
            &inverse_prefix,
            None,
            source_version,
            ReadPurpose::Snapshot,
            |_| {
                overlay_exists = true;
                Ok(false)
            },
        )?;
        let generic_fork_exists = self
            .restore_index_fork_source(source_catalog_root)?
            .is_some();
        if !canonical_exists && !overlay_exists && !generic_fork_exists {
            return Ok(());
        }

        let Some(effective) = self.restore_custom_index_at_path(
            &source_catalog_path,
            source_catalog_root,
            source_version,
            ReadPurpose::Snapshot,
        )?
        else {
            return Ok(());
        };
        let destination_catalog_path =
            restore_index_join_path(&operation.destination_path, catalog_relative)?;
        let owner = RestoreIndexOwner::from_operation(operation);
        let mut restored_batch = Vec::<RestoreIndexRow>::with_capacity(RESTORE_BATCH_ENTRIES);
        let mut restored_count = 0_u64;
        for row in effective.rows {
            let Some(row_relative) =
                restore_index_relative_components(&source_catalog_path, &row.path)?
            else {
                return Err(MetadError::Codec(
                    "source namespace row escaped its effective catalog".to_owned(),
                ));
            };
            let mut operation_relative = catalog_relative.to_vec();
            operation_relative.extend_from_slice(&row_relative);
            if initialization_paths.contains(&restore_index_relative_string(&operation_relative)?) {
                continue;
            }
            let target = self.preflight_restore_index_target(
                source_catalog_root,
                &row_relative,
                source_version,
            )?;
            restored_batch.push(RestoreIndexRow {
                owner,
                catalog_root: source_catalog_root,
                target,
                record: PathIndexRowRecord {
                    path: restore_index_join_path(&destination_catalog_path, &row_relative)?,
                    values: row.values,
                },
            });
            restored_count = restored_count.checked_add(1).ok_or_else(|| {
                MetadError::Codec("restore custom index row count overflow".to_owned())
            })?;
            if restored_batch.len() == RESTORE_BATCH_ENTRIES {
                let desired = restored_batch
                    .drain(..)
                    .flat_map(|row| restore_index_row_mutations(self.mount, &row))
                    .collect();
                self.preflight_restore_index_put_batch(
                    operation,
                    restore_index_row_prefix(
                        self.mount,
                        operation.ref_set_id,
                        Some(source_catalog_root),
                    ),
                    desired,
                )?;
            }
        }
        if !restored_batch.is_empty() {
            let desired = restored_batch
                .drain(..)
                .flat_map(|row| restore_index_row_mutations(self.mount, &row))
                .collect();
            self.preflight_restore_index_put_batch(
                operation,
                restore_index_row_prefix(
                    self.mount,
                    operation.ref_set_id,
                    Some(source_catalog_root),
                ),
                desired,
            )?;
        }
        let catalog = RestoreIndexCatalog {
            owner,
            catalog_root: source_catalog_root,
            record: PathIndexCatalogRecord {
                path: destination_catalog_path,
                fields: effective.catalog.fields,
                row_count: restored_count,
            },
        };
        self.preflight_restore_index_put_batch(
            operation,
            restore_index_catalog_prefix(self.mount, operation.ref_set_id),
            restore_index_catalog_mutations(self.mount, &catalog)
                .into_iter()
                .collect(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_restore_custom_index_catalog(
        &self,
        operation: &RestoreOperation,
        destination_parent: InodeId,
        destination_name: &DentryName,
        staging_version: Version,
        source_catalog_root: InodeId,
        catalog_version: Version,
        source_catalog_path: &str,
        catalog_relative: &[DentryName],
        generic_fork: bool,
    ) -> Result<(), MetadError> {
        let Some(destination_catalog_member) = self.restore_index_staging_member_for_relative(
            operation,
            catalog_relative,
            staging_version,
        )?
        else {
            return Ok(());
        };
        let destination_catalog_root = destination_catalog_member.destination_inode;
        if generic_fork {
            let existing_key = restore_index_catalog_key(
                self.mount,
                operation.ref_set_id,
                destination_catalog_root,
            );
            if let Some(existing) = self.metadata.get(
                RecordFamily::System,
                &existing_key,
                self.read_version()?,
                ReadPurpose::RestoreStaging,
            )? {
                let catalog = decode_restore_index_catalog(&existing.0)?;
                if catalog.owner != RestoreIndexOwner::from_operation(operation)
                    || catalog.catalog_root != destination_catalog_root
                {
                    return Err(MetadError::Codec(
                        "restore custom index catalog conflicts with an earlier source".to_owned(),
                    ));
                }
                return Ok(());
            }
        }
        let Some(effective) = self.restore_custom_index_at_path(
            source_catalog_path,
            source_catalog_root,
            catalog_version,
            ReadPurpose::Snapshot,
        )?
        else {
            return Ok(());
        };
        let destination_catalog_path =
            restore_index_join_path(&operation.destination_path, catalog_relative)?;
        let owner = RestoreIndexOwner::from_operation(operation);
        let mut restored_count = 0_u64;
        let mut restored_batch = Vec::<RestoreIndexRow>::with_capacity(RESTORE_BATCH_ENTRIES);
        for row in effective.rows {
            let Some(row_relative) =
                restore_index_relative_components(source_catalog_path, &row.path)?
            else {
                return Err(MetadError::Codec(
                    "source namespace row escaped its effective catalog".to_owned(),
                ));
            };
            let mut operation_relative = catalog_relative.to_vec();
            operation_relative.extend_from_slice(&row_relative);
            let Some(member) = self.restore_index_staging_member_for_relative(
                operation,
                &operation_relative,
                staging_version,
            )?
            else {
                // Initialization removed the indexed source entry.
                continue;
            };
            let source_relative_path = canonical_path(&row_relative)?;
            let Some(source_metadata) = self.stat_path_from_at_version_for_purpose(
                source_catalog_root,
                &source_relative_path,
                catalog_version,
                ReadPurpose::Snapshot,
            )?
            else {
                continue;
            };
            let source_matches = if generic_fork {
                match member.source_inode {
                    Some(member_source_inode) => self
                        .restore_index_metadata_for_inode(
                            member_source_inode,
                            Version::new(operation.read_version)?,
                            ReadPurpose::Snapshot,
                        )?
                        .is_some_and(|clone_metadata| {
                            restore_index_clone_metadata_matches_source(
                                &clone_metadata,
                                &source_metadata,
                            )
                        }),
                    None => false,
                }
            } else {
                member.source_inode == Some(source_metadata.attr.inode)
            };
            if !source_matches {
                continue;
            }
            let target = if row_relative.is_empty() {
                let metadata = self
                    .restore_index_metadata_for_inode(
                        destination_catalog_root,
                        staging_version,
                        ReadPurpose::RestoreStaging,
                    )?
                    .ok_or(MetadError::RestoreRootChanged {
                        root: destination_catalog_root,
                    })?;
                restore_index_root_target(&metadata)
            } else {
                let projection = self.restore_index_projection_for_member(
                    operation,
                    &member,
                    destination_parent,
                    destination_name,
                    staging_version,
                )?;
                restore_index_target_from_projection(&projection)
            };
            restored_batch.push(RestoreIndexRow {
                owner,
                catalog_root: destination_catalog_root,
                target,
                record: PathIndexRowRecord {
                    path: restore_index_join_path(&destination_catalog_path, &row_relative)?,
                    values: row.values,
                },
            });
            restored_count = restored_count.checked_add(1).ok_or_else(|| {
                MetadError::Codec("restore custom index row count overflow".to_owned())
            })?;
            if restored_count > MAX_RESTORE_SUBTREE_ENTRIES as u64 {
                return Err(MetadError::RestoreResourceLimit {
                    resource: "restore custom index rows".to_owned(),
                    limit: MAX_RESTORE_SUBTREE_ENTRIES as u64,
                    actual: restored_count,
                });
            }
            if restored_batch.len() == RESTORE_BATCH_ENTRIES {
                let desired = restored_batch
                    .drain(..)
                    .flat_map(|row| restore_index_row_mutations(self.mount, &row))
                    .collect();
                self.commit_restore_index_put_batch(
                    operation,
                    restore_index_row_prefix(
                        self.mount,
                        operation.ref_set_id,
                        Some(destination_catalog_root),
                    ),
                    desired,
                )?;
            }
        }
        if !restored_batch.is_empty() {
            let desired = restored_batch
                .drain(..)
                .flat_map(|row| restore_index_row_mutations(self.mount, &row))
                .collect();
            self.commit_restore_index_put_batch(
                operation,
                restore_index_row_prefix(
                    self.mount,
                    operation.ref_set_id,
                    Some(destination_catalog_root),
                ),
                desired,
            )?;
        }
        let catalog = RestoreIndexCatalog {
            owner,
            catalog_root: destination_catalog_root,
            record: PathIndexCatalogRecord {
                path: destination_catalog_path,
                fields: effective.catalog.fields,
                row_count: restored_count,
            },
        };
        self.commit_restore_index_put_batch(
            operation,
            restore_index_catalog_prefix(self.mount, operation.ref_set_id),
            restore_index_catalog_mutations(self.mount, &catalog)
                .into_iter()
                .collect(),
        )
    }

    fn materialize_restore_custom_indexes(
        &self,
        operation: &RestoreOperation,
        destination_parent: InodeId,
        destination_name: &DentryName,
    ) -> Result<(), MetadError> {
        let staging_version = self.read_version()?;
        let source_version = Version::new(operation.read_version)?;
        let namespace_catalog_prefix = path_index_catalog_key(self.mount, "");
        self.restore_index_for_each_raw(
            RecordFamily::PathIndex,
            &namespace_catalog_prefix,
            None,
            source_version,
            ReadPurpose::Snapshot,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let catalog = decode_path_index_catalog(&item.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))?;
                if item.key != path_index_catalog_key(self.mount, &catalog.path) {
                    return Err(MetadError::Codec(
                        "source namespace catalog path does not match its key".to_owned(),
                    ));
                }
                let Some(relative) =
                    restore_index_relative_components(&operation.source_path, &catalog.path)?
                else {
                    return Ok(true);
                };
                let Some(member) = self.restore_index_staging_member_for_relative(
                    operation,
                    &relative,
                    staging_version,
                )?
                else {
                    return Ok(true);
                };
                let Some(source_inode) = member.source_inode else {
                    return Ok(true);
                };
                self.materialize_restore_custom_index_catalog(
                    operation,
                    destination_parent,
                    destination_name,
                    staging_version,
                    source_inode,
                    source_version,
                    &catalog.path,
                    &relative,
                    false,
                )?;
                Ok(true)
            },
        )?;

        // A source subtree may itself be a completed restore whose custom
        // catalog path predates a later namespace move. Resolve its current
        // relative location through this operation's durable staging members
        // instead of retaining a whole-subtree source-inode map.
        let catalog_owner_prefix = restore_index_system_key(self.mount, INDEX_CATALOG_LABEL);
        self.restore_index_for_each_mvcc_latest(
            &catalog_owner_prefix,
            None,
            false,
            source_version,
            ReadPurpose::Snapshot,
            |item| {
                if item.record.kind == RestoreIndexMvccKind::Tombstone {
                    return Ok(true);
                }
                let catalog = decode_restore_index_catalog(&item.record.value)?;
                if item.logical_key
                    != restore_index_catalog_key(
                        self.mount,
                        catalog.owner.ref_set_id,
                        catalog.catalog_root,
                    )
                {
                    return Err(MetadError::Codec(
                        "source restore catalog path does not match its key".to_owned(),
                    ));
                }
                if self
                    .visible_restore_index_operation(
                        catalog.owner,
                        source_version,
                        ReadPurpose::Snapshot,
                    )?
                    .is_none()
                {
                    return Ok(true);
                }
                let Some(source_member) = self.restore_index_staging_member_for_source_inode(
                    operation,
                    catalog.catalog_root,
                    staging_version,
                )?
                else {
                    return Ok(true);
                };
                let relative = if source_member.relative_path.is_empty() {
                    Vec::new()
                } else {
                    parse_absolute_path(&format!("/{}", source_member.relative_path))?
                };
                let source_catalog_path =
                    restore_index_join_path(&operation.source_path, &relative)?;
                self.materialize_restore_custom_index_catalog(
                    operation,
                    destination_parent,
                    destination_name,
                    staging_version,
                    catalog.catalog_root,
                    source_version,
                    &source_catalog_path,
                    &relative,
                    false,
                )?;
                Ok(true)
            },
        )?;

        if let Some(fork_source) = self.restore_index_fork_source(operation.source_root)? {
            let pinned_version = Version::new(fork_source.binding.pinned_read_version)?;
            self.restore_index_for_each_mvcc_latest(
                &catalog_owner_prefix,
                None,
                false,
                pinned_version,
                ReadPurpose::Snapshot,
                |item| {
                    if item.record.kind == RestoreIndexMvccKind::Tombstone {
                        return Ok(true);
                    }
                    let catalog = decode_restore_index_catalog(&item.record.value)?;
                    if item.logical_key
                        != restore_index_catalog_key(
                            self.mount,
                            catalog.owner.ref_set_id,
                            catalog.catalog_root,
                        )
                    {
                        return Err(MetadError::Codec(
                            "generic fork source catalog path does not match its key".to_owned(),
                        ));
                    }
                    if self
                        .visible_restore_index_operation(
                            catalog.owner,
                            pinned_version,
                            ReadPurpose::Snapshot,
                        )?
                        .is_none()
                    {
                        return Ok(true);
                    }
                    let Some(relative) = restore_index_relative_components(
                        &fork_source.operation.destination_path,
                        &catalog.record.path,
                    )?
                    else {
                        return Ok(true);
                    };
                    if self
                        .restore_index_staging_member_for_relative(
                            operation,
                            &relative,
                            staging_version,
                        )?
                        .is_none()
                    {
                        return Ok(true);
                    }
                    self.materialize_restore_custom_index_catalog(
                        operation,
                        destination_parent,
                        destination_name,
                        staging_version,
                        catalog.catalog_root,
                        pinned_version,
                        &catalog.record.path,
                        &relative,
                        true,
                    )?;
                    Ok(true)
                },
            )?;
        }
        Ok(())
    }

    /// Materialize navigation and custom catalog overlays while the tree is
    /// detached. The returned seal proof must be included in the later
    /// ReadyToAttach transition and final attach CAS.
    pub(super) fn materialize_and_seal_restore_indexes(
        &self,
        operation: &RestoreOperation,
        destination_parent: InodeId,
        destination_name: &DentryName,
    ) -> Result<(RestoreIndexSeal, Version), MetadError> {
        self.materialize_restore_canonical_indexes(
            operation,
            destination_parent,
            destination_name,
        )?;
        self.materialize_restore_custom_indexes(operation, destination_parent, destination_name)?;
        self.seal_restore_indexes(operation)
    }

    fn restore_index_catalog_row_count(
        &self,
        owner: RestoreIndexOwner,
        catalog_root: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<u64, MetadError> {
        let prefix = restore_index_row_prefix(self.mount, owner.ref_set_id, Some(catalog_root));
        let mut count = 0_u64;
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let row = decode_restore_index_row(&item.value.0)?;
                if row.owner != owner
                    || row.catalog_root != catalog_root
                    || item.key
                        != restore_index_row_key(
                            self.mount,
                            owner.ref_set_id,
                            catalog_root,
                            &row.target,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index catalog row changed identity".to_owned(),
                    ));
                }
                count = count.checked_add(1).ok_or_else(|| {
                    MetadError::Codec("restore index catalog row count overflow".to_owned())
                })?;
                Ok(true)
            },
        )?;
        Ok(count)
    }

    fn compute_restore_index_seal(
        &self,
        operation: &RestoreOperation,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<RestoreIndexSeal, MetadError> {
        let owner = RestoreIndexOwner::from_operation(operation);
        let mut hasher = Sha256::new();
        hasher.update(b"nokv-restore-index-seal-v1\0");
        let mut entry_count = 0_u64;
        let mut catalog_count = 0_u64;
        let mut row_count = 0_u64;

        let entry_prefix = restore_index_entry_prefix(self.mount, owner.ref_set_id, None);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &entry_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let entry = decode_restore_index_entry(&item.value.0)?;
                if entry.owner != owner
                    || item.key
                        != restore_index_entry_key(
                            self.mount,
                            owner.ref_set_id,
                            entry.projection.dentry.parent,
                            &entry.projection.dentry.name,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index seal found an invalid entry owner/key".to_owned(),
                    ));
                }
                for key in [
                    restore_index_parent_owner_key(
                        self.mount,
                        owner.ref_set_id,
                        entry.projection.dentry.parent,
                    ),
                    restore_index_parent_inverse_key(
                        self.mount,
                        entry.projection.dentry.parent,
                        owner.ref_set_id,
                    ),
                ] {
                    let value = self
                        .metadata
                        .get(RecordFamily::System, &key, version, purpose)?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "restore index entry has no parent owner/inverse".to_owned(),
                            )
                        })?;
                    if decode_restore_index_owner(&value.0)? != owner {
                        return Err(MetadError::Codec(
                            "restore index entry parent owner/inverse mismatch".to_owned(),
                        ));
                    }
                }
                fold_restore_index_seal_row(&mut hasher, 1, &item.key, &item.value.0);
                entry_count = entry_count.checked_add(1).ok_or_else(|| {
                    MetadError::Codec("restore index entry count overflow".to_owned())
                })?;
                Ok(true)
            },
        )?;

        let catalog_prefix = restore_index_catalog_prefix(self.mount, owner.ref_set_id);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &catalog_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let catalog = decode_restore_index_catalog(&item.value.0)?;
                if catalog.owner != owner
                    || item.key
                        != restore_index_catalog_key(
                            self.mount,
                            owner.ref_set_id,
                            catalog.catalog_root,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index seal found an invalid catalog owner/key".to_owned(),
                    ));
                }
                let inverse_key = restore_index_catalog_inverse_key(
                    self.mount,
                    catalog.catalog_root,
                    owner.ref_set_id,
                );
                let inverse = self
                    .metadata
                    .get(RecordFamily::System, &inverse_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index catalog has no inverse".to_owned())
                    })?;
                if decode_restore_index_catalog(&inverse.0)? != catalog {
                    return Err(MetadError::Codec(
                        "restore index catalog owner/inverse mismatch".to_owned(),
                    ));
                }
                let actual = self.restore_index_catalog_row_count(
                    owner,
                    catalog.catalog_root,
                    version,
                    purpose,
                )?;
                if catalog.record.row_count != actual {
                    return Err(MetadError::Codec(format!(
                        "restore index catalog row count mismatch: expected {}, found {actual}",
                        catalog.record.row_count
                    )));
                }
                fold_restore_index_seal_row(&mut hasher, 2, &item.key, &item.value.0);
                catalog_count = catalog_count.checked_add(1).ok_or_else(|| {
                    MetadError::Codec("restore index catalog count overflow".to_owned())
                })?;
                Ok(true)
            },
        )?;

        let row_prefix = restore_index_row_prefix(self.mount, owner.ref_set_id, None);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &row_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let row = decode_restore_index_row(&item.value.0)?;
                if row.owner != owner
                    || item.key
                        != restore_index_row_key(
                            self.mount,
                            owner.ref_set_id,
                            row.catalog_root,
                            &row.target,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index seal found an invalid custom row owner/key".to_owned(),
                    ));
                }
                let catalog_key =
                    restore_index_catalog_key(self.mount, owner.ref_set_id, row.catalog_root);
                let catalog = self
                    .metadata
                    .get(RecordFamily::System, &catalog_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index row has no catalog owner".to_owned())
                    })?;
                if decode_restore_index_catalog(&catalog.0)?.owner != owner {
                    return Err(MetadError::Codec(
                        "restore index row catalog changed identity".to_owned(),
                    ));
                }
                let inverse_key = restore_index_target_inverse_key(
                    self.mount,
                    &row.target,
                    owner.ref_set_id,
                    row.catalog_root,
                );
                let inverse = self
                    .metadata
                    .get(RecordFamily::System, &inverse_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index row has no target inverse".to_owned())
                    })?;
                if decode_restore_index_row(&inverse.0)? != row {
                    return Err(MetadError::Codec(
                        "restore index row owner/inverse mismatch".to_owned(),
                    ));
                }
                fold_restore_index_seal_row(&mut hasher, 3, &item.key, &item.value.0);
                row_count = row_count.checked_add(1).ok_or_else(|| {
                    MetadError::Codec("restore index row count overflow".to_owned())
                })?;
                Ok(true)
            },
        )?;

        let parent_prefix = restore_index_parent_owner_prefix(self.mount, owner.ref_set_id);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &parent_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                if item.key.len() != parent_prefix.len() + 8
                    || decode_restore_index_owner(&item.value.0)? != owner
                {
                    return Err(MetadError::Codec(
                        "restore index seal found an invalid parent owner".to_owned(),
                    ));
                }
                let parent = InodeId::new(u64::from_be_bytes(
                    item.key[parent_prefix.len()..]
                        .try_into()
                        .expect("u64 width"),
                ))?;
                let inverse_key =
                    restore_index_parent_inverse_key(self.mount, parent, owner.ref_set_id);
                let inverse = self
                    .metadata
                    .get(RecordFamily::System, &inverse_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index parent owner has no inverse".to_owned())
                    })?;
                if decode_restore_index_owner(&inverse.0)? != owner
                    || self
                        .metadata
                        .scan(ScanRequest {
                            family: RecordFamily::System,
                            prefix: restore_index_entry_prefix(
                                self.mount,
                                owner.ref_set_id,
                                Some(parent),
                            ),
                            start_after: None,
                            version,
                            limit: 1,
                            purpose,
                        })?
                        .is_empty()
                {
                    return Err(MetadError::Codec(
                        "restore index parent owner is orphaned".to_owned(),
                    ));
                }
                Ok(true)
            },
        )?;

        let source_member_prefix = restore_index_source_member_prefix(self.mount, owner.ref_set_id);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &source_member_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let source = decode_restore_index_source_member(&item.value.0)?;
                if source.owner != owner
                    || item.key
                        != restore_index_source_member_key(
                            self.mount,
                            owner.ref_set_id,
                            source.source_inode,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index seal found an invalid source member".to_owned(),
                    ));
                }
                let staging_key = restore_staging_member_key(
                    self.mount,
                    owner.ref_set_id,
                    source.member.destination_inode,
                );
                let staging = self
                    .metadata
                    .get(RecordFamily::System, &staging_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore index source member has no staging member".to_owned(),
                        )
                    })?;
                if decode_restore_staging_member(&staging.0)? != source.member {
                    return Err(MetadError::Codec(
                        "restore index source/staging member mismatch".to_owned(),
                    ));
                }
                fold_restore_index_seal_row(&mut hasher, 4, &item.key, &item.value.0);
                Ok(true)
            },
        )?;

        let staging_prefix = restore_staging_member_prefix(self.mount, owner.ref_set_id);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &staging_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let member =
                    self.restore_index_staging_member_from_row(operation, version, &item)?;
                let Some(source_inode) = member.source_inode else {
                    return Ok(true);
                };
                let source_key =
                    restore_index_source_member_key(self.mount, owner.ref_set_id, source_inode);
                let source = self
                    .metadata
                    .get(RecordFamily::System, &source_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore staging member has no source-member index".to_owned(),
                        )
                    })?;
                let source = decode_restore_index_source_member(&source.0)?;
                if source.owner != owner
                    || source.source_inode != source_inode
                    || source.member != member
                {
                    return Err(MetadError::Codec(
                        "restore staging/source member mismatch".to_owned(),
                    ));
                }
                Ok(true)
            },
        )?;

        self.validate_restore_index_inverse_closure(operation.ref_set_id, version, purpose)?;

        Ok(RestoreIndexSeal {
            operation_digest: operation.operation_digest,
            initialization_digest: operation.initialization_digest,
            ref_set_id: operation.ref_set_id,
            incarnation: operation.created_version,
            entry_count,
            catalog_count,
            row_count,
            digest: hasher.finalize().into(),
        })
    }

    fn validate_restore_index_inverse_closure(
        &self,
        ref_set_id: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<(), MetadError> {
        let parent_prefix = restore_index_system_key(self.mount, INDEX_PARENT_INVERSE_LABEL);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &parent_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                if item.key.len() != parent_prefix.len() + 16 {
                    return Err(MetadError::Codec(
                        "restore index parent inverse key has an invalid length".to_owned(),
                    ));
                }
                let row_ref_set = u64::from_be_bytes(
                    item.key[item.key.len() - 8..]
                        .try_into()
                        .expect("u64 width"),
                );
                if row_ref_set != ref_set_id {
                    return Ok(true);
                }
                let parent = InodeId::new(u64::from_be_bytes(
                    item.key[parent_prefix.len()..parent_prefix.len() + 8]
                        .try_into()
                        .expect("u64 width"),
                ))?;
                let owner = decode_restore_index_owner(&item.value.0)?;
                if owner.ref_set_id != ref_set_id
                    || item.key != restore_index_parent_inverse_key(self.mount, parent, ref_set_id)
                {
                    return Err(MetadError::Codec(
                        "restore index parent inverse changed identity".to_owned(),
                    ));
                }
                let owner_value = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &restore_index_parent_owner_key(self.mount, ref_set_id, parent),
                        version,
                        purpose,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index parent inverse is orphaned".to_owned())
                    })?;
                if decode_restore_index_owner(&owner_value.0)? != owner {
                    return Err(MetadError::Codec(
                        "restore index parent inverse/owner mismatch".to_owned(),
                    ));
                }
                Ok(true)
            },
        )?;

        let catalog_prefix = restore_index_system_key(self.mount, INDEX_CATALOG_INVERSE_LABEL);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &catalog_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                if item.key.len() != catalog_prefix.len() + 16 {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse key has an invalid length".to_owned(),
                    ));
                }
                let row_ref_set = u64::from_be_bytes(
                    item.key[item.key.len() - 8..]
                        .try_into()
                        .expect("u64 width"),
                );
                if row_ref_set != ref_set_id {
                    return Ok(true);
                }
                let catalog = decode_restore_index_catalog(&item.value.0)?;
                if catalog.owner.ref_set_id != ref_set_id
                    || item.key
                        != restore_index_catalog_inverse_key(
                            self.mount,
                            catalog.catalog_root,
                            ref_set_id,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse changed identity".to_owned(),
                    ));
                }
                let owner_value = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &restore_index_catalog_key(self.mount, ref_set_id, catalog.catalog_root),
                        version,
                        purpose,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index catalog inverse is orphaned".to_owned())
                    })?;
                if decode_restore_index_catalog(&owner_value.0)? != catalog {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse/owner mismatch".to_owned(),
                    ));
                }
                Ok(true)
            },
        )?;

        let target_prefix = restore_index_system_key(self.mount, INDEX_TARGET_INVERSE_LABEL);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &target_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                if item.key.len() != target_prefix.len() + 48 {
                    return Err(MetadError::Codec(
                        "restore index target inverse key has an invalid length".to_owned(),
                    ));
                }
                let row_ref_set = u64::from_be_bytes(
                    item.key[item.key.len() - 16..item.key.len() - 8]
                        .try_into()
                        .expect("u64 width"),
                );
                if row_ref_set != ref_set_id {
                    return Ok(true);
                }
                let row = decode_restore_index_row(&item.value.0)?;
                if row.owner.ref_set_id != ref_set_id
                    || item.key
                        != restore_index_target_inverse_key(
                            self.mount,
                            &row.target,
                            ref_set_id,
                            row.catalog_root,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index target inverse changed identity".to_owned(),
                    ));
                }
                let owner_value = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &restore_index_row_key(
                            self.mount,
                            ref_set_id,
                            row.catalog_root,
                            &row.target,
                        ),
                        version,
                        purpose,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index target inverse is orphaned".to_owned())
                    })?;
                if decode_restore_index_row(&owner_value.0)? != row {
                    return Err(MetadError::Codec(
                        "restore index target inverse/owner mismatch".to_owned(),
                    ));
                }
                Ok(true)
            },
        )?;
        Ok(())
    }

    fn seal_restore_indexes(
        &self,
        operation: &RestoreOperation,
    ) -> Result<(RestoreIndexSeal, Version), MetadError> {
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let seal =
            self.compute_restore_index_seal(operation, read_version, ReadPurpose::RestoreStaging)?;
        let key = restore_index_seal_key(self.mount, operation.ref_set_id);
        if let Some(existing) = self.metadata.get_versioned(
            RecordFamily::System,
            &key,
            read_version,
            ReadPurpose::RestoreStaging,
        )? {
            let decoded = decode_restore_index_seal(&existing.value.0)?;
            if decoded != seal {
                return Err(MetadError::Codec(
                    "restore index seal conflicts with materialized rows".to_owned(),
                ));
            }
            return Ok((decoded, existing.version));
        }
        let mut predicates = self.restore_index_staging_guards(operation, read_version)?;
        predicates.push(PredicateRef {
            family: RecordFamily::System,
            key: key.clone(),
            predicate: Predicate::NotExists,
        });
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-seal-index",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::RegisterNamespaceIndex,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates,
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_index_seal(&seal))),
            }],
            watch: Vec::new(),
        };
        super::restore::validate_restore_command_bounds(&command, "restore index seal")?;
        self.commit_metadata(command)?;
        Ok((seal, version))
    }

    /// Recompute the attach-time seal at `version` and return its exact CAS
    /// predicate. This is intentionally stronger than checking only the seal's
    /// identity: a missing/tampered owner or inverse row fails before attach.
    pub(super) fn restore_index_seal_predicate(
        &self,
        operation: &RestoreOperation,
        version: Version,
    ) -> Result<PredicateRef, MetadError> {
        let key = restore_index_seal_key(self.mount, operation.ref_set_id);
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore index seal is missing".to_owned()))?;
        let durable = decode_restore_index_seal(&item.value.0)?;
        let computed =
            self.compute_restore_index_seal(operation, version, ReadPurpose::WritePlanLocal)?;
        if durable != computed {
            return Err(MetadError::Codec(
                "restore index seal no longer matches owner rows".to_owned(),
            ));
        }
        Ok(PredicateRef {
            family: RecordFamily::System,
            key,
            predicate: Predicate::VersionEquals(item.version),
        })
    }

    fn restore_index_entries_for_location_at(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<VersionedRestoreIndexEntry>, MetadError> {
        let inverse_prefix = restore_index_parent_inverse_prefix_for_read(self.mount, parent);
        let mut entries = Vec::new();
        let mut buffered_bytes = 0_usize;
        self.restore_index_effective_for_each(
            &inverse_prefix,
            None,
            version,
            purpose,
            |inverse| {
                if inverse.key.len() != inverse_prefix.len() + 8 {
                    return Err(MetadError::Codec(
                        "restore index parent inverse key has an invalid length".to_owned(),
                    ));
                }
                let owner = decode_restore_index_owner(&inverse.value.0)?;
                let ref_set_id = u64::from_be_bytes(
                    inverse.key[inverse_prefix.len()..]
                        .try_into()
                        .expect("u64 width"),
                );
                if owner.ref_set_id != ref_set_id
                    || inverse.key
                        != restore_index_parent_inverse_key(self.mount, parent, ref_set_id)
                {
                    return Err(MetadError::Codec(
                        "restore index parent inverse changed identity".to_owned(),
                    ));
                }
                let Some((_, operation_version)) =
                    self.visible_restore_index_operation_versioned(owner, version, purpose)?
                else {
                    return Ok(true);
                };
                let parent_owner_key =
                    restore_index_parent_owner_key(self.mount, owner.ref_set_id, parent);
                let parent_owner = self
                    .restore_index_effective_get(&parent_owner_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index parent inverse has no owner".to_owned())
                    })?;
                if decode_restore_index_owner(&parent_owner.value.0)? != owner {
                    return Err(MetadError::Codec(
                        "restore index parent owner/inverse mismatch".to_owned(),
                    ));
                }
                let key = restore_index_entry_key(self.mount, owner.ref_set_id, parent, name);
                let Some(item) = self.restore_index_effective_get(&key, version, purpose)? else {
                    return Ok(true);
                };
                let entry = decode_restore_index_entry(&item.value.0)?;
                if entry.owner != owner
                    || entry.projection.dentry.parent != parent
                    || entry.projection.dentry.name != *name
                    || key
                        != restore_index_entry_key(
                            self.mount,
                            owner.ref_set_id,
                            entry.projection.dentry.parent,
                            &entry.projection.dentry.name,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index entry changed identity".to_owned(),
                    ));
                }
                account_restore_index_buffer(
                    "restore index location candidates",
                    entries.len().saturating_add(1),
                    &mut buffered_bytes,
                    inverse
                        .key
                        .len()
                        .saturating_add(inverse.value.0.len())
                        .saturating_add(key.len())
                        .saturating_add(item.value.0.len()),
                )?;
                entries.push(VersionedRestoreIndexEntry {
                    key,
                    version: item.version,
                    entry,
                    operation_key: restore_operation_key(self.mount, &owner.operation_digest),
                    operation_version,
                });
                Ok(true)
            },
        )?;
        Ok(entries)
    }

    fn restore_index_entries_for_location(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Vec<VersionedRestoreIndexEntry>, MetadError> {
        self.restore_index_entries_for_location_at(
            parent,
            name,
            version,
            ReadPurpose::WritePlanLocal,
        )
    }

    /// Return whether this exact namespace location is backed by a visible
    /// restore overlay entry. An inode may have another inherited hardlink
    /// while `parent/name` itself is a later canonical link, so inode ownership
    /// or a non-empty aggregate publish plan is not sufficient evidence.
    pub(super) fn restore_index_manages_projection_location(
        &self,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<bool, MetadError> {
        let entries = self.restore_index_entries_for_location(
            projection.dentry.parent,
            &projection.dentry.name,
            version,
        )?;
        for indexed in &entries {
            if indexed.entry.projection != *projection {
                return Err(MetadError::Codec(
                    "restore index location projection changed identity".to_owned(),
                ));
            }
        }
        Ok(!entries.is_empty())
    }

    fn restore_index_rows_for_target(
        &self,
        target: &RestoreIndexTarget,
        version: Version,
    ) -> Result<Vec<VersionedRestoreIndexRow>, MetadError> {
        let inverse_prefix =
            restore_index_target_inverse_prefix(self.mount, target.parent, &target.name);
        let mut rows = Vec::new();
        let mut buffered_bytes = 0_usize;
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &inverse_prefix,
            None,
            version,
            ReadPurpose::WritePlanLocal,
            RESTORE_INDEX_SCAN_PAGE,
            |inverse| {
                if inverse.key.len() != inverse_prefix.len() + 16 {
                    return Err(MetadError::Codec(
                        "restore index target inverse key has an invalid length".to_owned(),
                    ));
                }
                let row = decode_restore_index_row(&inverse.value.0)?;
                let ref_set_id = u64::from_be_bytes(
                    inverse.key[inverse_prefix.len()..inverse_prefix.len() + 8]
                        .try_into()
                        .expect("u64 width"),
                );
                let catalog_root = InodeId::new(u64::from_be_bytes(
                    inverse.key[inverse_prefix.len() + 8..]
                        .try_into()
                        .expect("u64 width"),
                ))?;
                if inverse.key
                    != restore_index_target_inverse_key(
                        self.mount,
                        &row.target,
                        row.owner.ref_set_id,
                        row.catalog_root,
                    )
                    || row.owner.ref_set_id != ref_set_id
                    || row.catalog_root != catalog_root
                {
                    return Err(MetadError::Codec(
                        "restore index target inverse changed identity".to_owned(),
                    ));
                }
                // A SHA-256 target-prefix collision is not corruption; the full
                // encoded identity disambiguates it before mutation planning.
                if row.target != *target {
                    return Ok(true);
                }
                let Some((_, operation_version)) = self.visible_restore_index_operation_versioned(
                    row.owner,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                else {
                    return Ok(true);
                };
                let owner_key = restore_index_row_key(
                    self.mount,
                    row.owner.ref_set_id,
                    row.catalog_root,
                    &row.target,
                );
                let owner = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &owner_key,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index target inverse has no owner".to_owned())
                    })?;
                if decode_restore_index_row(&owner.value.0)? != row {
                    return Err(MetadError::Codec(
                        "restore index custom row owner/inverse mismatch".to_owned(),
                    ));
                }
                let catalog_key =
                    restore_index_catalog_key(self.mount, row.owner.ref_set_id, row.catalog_root);
                let catalog = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &catalog_key,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index custom row has no catalog".to_owned())
                    })?;
                if decode_restore_index_catalog(&catalog.0)?.owner != row.owner {
                    return Err(MetadError::Codec(
                        "restore index custom row catalog changed identity".to_owned(),
                    ));
                }
                account_restore_index_buffer(
                    "restore index target candidates",
                    rows.len().saturating_add(1),
                    &mut buffered_bytes,
                    inverse
                        .key
                        .len()
                        .saturating_add(inverse.value.0.len())
                        .saturating_add(owner_key.len())
                        .saturating_add(owner.value.0.len()),
                )?;
                rows.push(VersionedRestoreIndexRow {
                    owner_key,
                    owner_version: owner.version,
                    inverse_key: inverse.key,
                    inverse_version: inverse.version,
                    operation_key: restore_operation_key(self.mount, &row.owner.operation_digest),
                    operation_version,
                    row,
                });
                Ok(true)
            },
        )?;
        Ok(rows)
    }

    fn restore_index_plan_row_cas(
        plan: &mut RestoreIndexMutationPlan,
        family: RecordFamily,
        key: Vec<u8>,
        version: Version,
    ) -> Result<(), MetadError> {
        plan.push_predicate(PredicateRef {
            family,
            key,
            predicate: Predicate::VersionEquals(version),
        })
    }

    fn restore_index_plan_operation_cas(
        plan: &mut RestoreIndexMutationPlan,
        key: Vec<u8>,
        version: Version,
    ) -> Result<(), MetadError> {
        Self::restore_index_plan_row_cas(plan, RecordFamily::System, key, version)
    }

    fn restore_index_plan_put_at(
        &self,
        plan: &mut RestoreIndexMutationPlan,
        key: Vec<u8>,
        value: Vec<u8>,
        version: Version,
    ) -> Result<(), MetadError> {
        match self.metadata.get_versioned(
            RecordFamily::System,
            &key,
            version,
            ReadPurpose::WritePlanLocal,
        )? {
            Some(existing) => Self::restore_index_plan_row_cas(
                plan,
                RecordFamily::System,
                key.clone(),
                existing.version,
            )?,
            None => plan.push_predicate(PredicateRef {
                family: RecordFamily::System,
                key: key.clone(),
                predicate: Predicate::NotExists,
            })?,
        }
        plan.set_mutation(Mutation {
            family: RecordFamily::System,
            key,
            op: MutationOp::Put,
            value: Some(Value(value)),
        });
        Ok(())
    }

    fn restore_index_plan_parent_owner(
        &self,
        plan: &mut RestoreIndexMutationPlan,
        owner: RestoreIndexOwner,
        parent: InodeId,
        version: Version,
    ) -> Result<(), MetadError> {
        let value = encode_restore_index_owner(owner);
        for key in [
            restore_index_parent_owner_key(self.mount, owner.ref_set_id, parent),
            restore_index_parent_inverse_key(self.mount, parent, owner.ref_set_id),
        ] {
            if let Some(existing) = self.metadata.get_versioned(
                RecordFamily::System,
                &key,
                version,
                ReadPurpose::WritePlanLocal,
            )? {
                if existing.value.0 != value {
                    return Err(MetadError::Codec(
                        "restore index parent owner/inverse changed identity".to_owned(),
                    ));
                }
                Self::restore_index_plan_row_cas(
                    plan,
                    RecordFamily::System,
                    key,
                    existing.version,
                )?;
            } else {
                plan.push_predicate(PredicateRef {
                    family: RecordFamily::System,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                })?;
                plan.set_mutation(Mutation {
                    family: RecordFamily::System,
                    key,
                    op: MutationOp::Put,
                    value: Some(Value(value.clone())),
                });
            }
        }
        Ok(())
    }

    fn restore_index_directory_contains(
        &self,
        root: InodeId,
        candidate: InodeId,
        version: Version,
    ) -> Result<bool, MetadError> {
        if root == candidate {
            return Ok(true);
        }
        let mut visited = HashSet::new();
        let mut pending = vec![root];
        let mut traversed = 0_usize;
        while let Some(parent) = pending.pop() {
            if !visited.insert(parent) {
                continue;
            }
            validate_restore_index_collection(
                "restore index containment directories",
                visited.len(),
            )?;
            let mut after = None;
            loop {
                let page = self.read_dir_plus_page_at_version_for_purpose(
                    parent,
                    after.as_ref(),
                    RESTORE_INDEX_DENTRY_PAGE,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?;
                for entry in page.entries {
                    traversed = traversed.saturating_add(1);
                    validate_restore_index_collection(
                        "restore index containment entries",
                        traversed,
                    )?;
                    if entry.attr.inode == candidate {
                        return Ok(true);
                    }
                    if entry.attr.file_type == FileType::Directory
                        && entry.attr.inode.shard_index() == self.shard_index
                    {
                        pending.push(entry.attr.inode);
                        validate_restore_index_collection(
                            "restore index containment pending directories",
                            pending.len(),
                        )?;
                    }
                }
                let Some(next) = page.next_cursor else {
                    break;
                };
                if after
                    .as_ref()
                    .is_some_and(|cursor: &DentryName| cursor.as_bytes() >= next.as_bytes())
                {
                    return Err(MetadError::Codec(
                        "restore index containment cursor did not advance".to_owned(),
                    ));
                }
                after = Some(next);
            }
        }
        Ok(false)
    }

    pub(super) fn restore_index_unlink_plan(
        &self,
        projection: &DentryProjection,
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        let read_version = predecessor(commit_version)?;
        let mut plan = RestoreIndexMutationPlan::default();
        for indexed in self.restore_index_entries_for_location(
            projection.dentry.parent,
            &projection.dentry.name,
            read_version,
        )? {
            if indexed.entry.projection != *projection {
                return Err(MetadError::Codec(
                    "restore index unlink projection changed identity".to_owned(),
                ));
            }
            Self::restore_index_plan_operation_cas(
                &mut plan,
                indexed.operation_key,
                indexed.operation_version,
            )?;
            Self::restore_index_plan_row_cas(
                &mut plan,
                RecordFamily::System,
                indexed.key.clone(),
                indexed.version,
            )?;
            plan.set_mutation(delete_mutation(RecordFamily::System, indexed.key));
        }
        let target = restore_index_target_from_projection(projection);
        for indexed in self.restore_index_rows_for_target(&target, read_version)? {
            Self::restore_index_plan_operation_cas(
                &mut plan,
                indexed.operation_key,
                indexed.operation_version,
            )?;
            for (key, row_version) in [
                (indexed.owner_key, indexed.owner_version),
                (indexed.inverse_key, indexed.inverse_version),
            ] {
                Self::restore_index_plan_row_cas(
                    &mut plan,
                    RecordFamily::System,
                    key.clone(),
                    row_version,
                )?;
                plan.set_mutation(delete_mutation(RecordFamily::System, key));
            }
        }
        self.finalize_restore_index_mvcc_plan(plan, read_version, commit_version)
    }

    pub(super) fn restore_index_publish_plan(
        &self,
        old: &DentryProjection,
        new: &DentryProjection,
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        if old.dentry.parent != new.dentry.parent || old.dentry.name != new.dentry.name {
            return Err(MetadError::Codec(
                "restore index publish plan cannot move a dentry".to_owned(),
            ));
        }
        let read_version = predecessor(commit_version)?;
        let mut plan = RestoreIndexMutationPlan::default();
        for indexed in self.restore_index_entries_for_location(
            old.dentry.parent,
            &old.dentry.name,
            read_version,
        )? {
            if indexed.entry.projection != *old {
                return Err(MetadError::Codec(
                    "restore index publish projection changed identity".to_owned(),
                ));
            }
            Self::restore_index_plan_operation_cas(
                &mut plan,
                indexed.operation_key,
                indexed.operation_version,
            )?;
            Self::restore_index_plan_row_cas(
                &mut plan,
                RecordFamily::System,
                indexed.key.clone(),
                indexed.version,
            )?;
            plan.set_mutation(Mutation {
                family: RecordFamily::System,
                key: indexed.key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_index_entry(&RestoreIndexEntry {
                    owner: indexed.entry.owner,
                    projection: new.clone(),
                }))),
            });
        }
        let old_target = restore_index_target_from_projection(old);
        let new_target = restore_index_target_from_projection(new);
        for indexed in self.restore_index_rows_for_target(&old_target, read_version)? {
            Self::restore_index_plan_operation_cas(
                &mut plan,
                indexed.operation_key,
                indexed.operation_version,
            )?;
            for (key, row_version) in [
                (indexed.owner_key.clone(), indexed.owner_version),
                (indexed.inverse_key.clone(), indexed.inverse_version),
            ] {
                Self::restore_index_plan_row_cas(
                    &mut plan,
                    RecordFamily::System,
                    key,
                    row_version,
                )?;
            }
            let mut row = indexed.row;
            row.target = new_target.clone();
            let encoded = encode_restore_index_row(&row);
            plan.set_mutation(Mutation {
                family: RecordFamily::System,
                key: indexed.owner_key,
                op: MutationOp::Put,
                value: Some(Value(encoded.clone())),
            });
            plan.set_mutation(Mutation {
                family: RecordFamily::System,
                key: indexed.inverse_key,
                op: MutationOp::Put,
                value: Some(Value(encoded)),
            });
        }
        self.finalize_restore_index_mvcc_plan(plan, read_version, commit_version)
    }

    pub(super) fn restore_index_link_plan(
        &self,
        updated_existing_links: &[(DentryProjection, DentryProjection)],
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        let mut plan = RestoreIndexMutationPlan::default();
        for (old, new) in updated_existing_links {
            plan.extend(self.restore_index_publish_plan(old, new, commit_version)?)?;
        }
        // The newly-created link deliberately has no inherited custom row; its
        // normal PathIndex mutation remains owned by the namespace command.
        Ok(plan)
    }

    pub(super) fn restore_index_remove_plan(
        &self,
        removed: &DentryProjection,
        updated_remaining_links: &[(DentryProjection, DentryProjection)],
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        let mut plan = self.restore_index_unlink_plan(removed, commit_version)?;
        plan.extend(self.restore_index_link_plan(updated_remaining_links, commit_version)?)?;
        Ok(plan)
    }

    /// A normal namespace-index registration is a replacement, not an
    /// additive merge. Delete inherited custom catalog/rows for this exact
    /// root in the same command that writes the canonical registration so
    /// omitted old rows cannot reappear from the overlay.
    pub(super) fn restore_index_replace_catalog_plan(
        &self,
        catalog_root: InodeId,
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        let version = predecessor(commit_version)?;
        let mut plan = RestoreIndexMutationPlan::default();
        let inverse_prefix = restore_index_catalog_inverse_prefix(self.mount, catalog_root);
        self.restore_index_for_each_raw(
            RecordFamily::System,
            &inverse_prefix,
            None,
            version,
            ReadPurpose::WritePlanLocal,
            RESTORE_INDEX_SCAN_PAGE,
            |inverse| {
                if inverse.key.len() != inverse_prefix.len() + 8 {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse key has an invalid length".to_owned(),
                    ));
                }
                let catalog = decode_restore_index_catalog(&inverse.value.0)?;
                if catalog.catalog_root != catalog_root
                    || inverse.key
                        != restore_index_catalog_inverse_key(
                            self.mount,
                            catalog_root,
                            catalog.owner.ref_set_id,
                        )
                {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse changed identity".to_owned(),
                    ));
                }
                let Some((_, operation_version)) = self.visible_restore_index_operation_versioned(
                    catalog.owner,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                else {
                    return Ok(true);
                };
                Self::restore_index_plan_operation_cas(
                    &mut plan,
                    restore_operation_key(self.mount, &catalog.owner.operation_digest),
                    operation_version,
                )?;
                let owner_key =
                    restore_index_catalog_key(self.mount, catalog.owner.ref_set_id, catalog_root);
                let owner = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &owner_key,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index catalog inverse has no owner".to_owned())
                    })?;
                if decode_restore_index_catalog(&owner.value.0)? != catalog {
                    return Err(MetadError::Codec(
                        "restore index catalog owner/inverse mismatch".to_owned(),
                    ));
                }
                for (key, row_version) in
                    [(owner_key, owner.version), (inverse.key, inverse.version)]
                {
                    Self::restore_index_plan_row_cas(
                        &mut plan,
                        RecordFamily::System,
                        key.clone(),
                        row_version,
                    )?;
                    plan.set_mutation(delete_mutation(RecordFamily::System, key));
                }
                plan.validate_budget("restore index catalog replacement")?;
                let row_prefix = restore_index_row_prefix(
                    self.mount,
                    catalog.owner.ref_set_id,
                    Some(catalog_root),
                );
                self.restore_index_for_each_raw(
                    RecordFamily::System,
                    &row_prefix,
                    None,
                    version,
                    ReadPurpose::WritePlanLocal,
                    RESTORE_INDEX_SCAN_PAGE,
                    |item| {
                        let row = decode_restore_index_row(&item.value.0)?;
                        if row.owner != catalog.owner
                            || row.catalog_root != catalog_root
                            || item.key
                                != restore_index_row_key(
                                    self.mount,
                                    row.owner.ref_set_id,
                                    catalog_root,
                                    &row.target,
                                )
                        {
                            return Err(MetadError::Codec(
                                "restore index replacement found an invalid row".to_owned(),
                            ));
                        }
                        let target_inverse_key = restore_index_target_inverse_key(
                            self.mount,
                            &row.target,
                            row.owner.ref_set_id,
                            catalog_root,
                        );
                        let target_inverse = self
                            .metadata
                            .get_versioned(
                                RecordFamily::System,
                                &target_inverse_key,
                                version,
                                ReadPurpose::WritePlanLocal,
                            )?
                            .ok_or_else(|| {
                                MetadError::Codec(
                                    "restore index replacement row has no inverse".to_owned(),
                                )
                            })?;
                        if decode_restore_index_row(&target_inverse.value.0)? != row {
                            return Err(MetadError::Codec(
                                "restore index replacement row/inverse mismatch".to_owned(),
                            ));
                        }
                        for (key, row_version) in [
                            (item.key, item.version),
                            (target_inverse_key, target_inverse.version),
                        ] {
                            Self::restore_index_plan_row_cas(
                                &mut plan,
                                RecordFamily::System,
                                key.clone(),
                                row_version,
                            )?;
                            plan.set_mutation(delete_mutation(RecordFamily::System, key));
                        }
                        plan.validate_budget("restore index catalog replacement")?;
                        Ok(true)
                    },
                )?;
                Ok(true)
            },
        )?;
        self.finalize_restore_index_mvcc_plan(plan, version, commit_version)
    }

    pub(super) fn restore_index_rename_plan(
        &self,
        source: &DentryProjection,
        destination: &DentryProjection,
        replaced: Option<&DentryProjection>,
        commit_version: Version,
    ) -> Result<RestoreIndexMutationPlan, MetadError> {
        let version = predecessor(commit_version)?;
        let mut plan = RestoreIndexMutationPlan::default();
        if let Some(replaced) = replaced {
            plan.extend(self.restore_index_unlink_plan(replaced, commit_version)?)?;
        }
        for indexed in self.restore_index_entries_for_location(
            source.dentry.parent,
            &source.dentry.name,
            version,
        )? {
            if indexed.entry.projection != *source {
                return Err(MetadError::Codec(
                    "restore index rename projection changed identity".to_owned(),
                ));
            }
            Self::restore_index_plan_operation_cas(
                &mut plan,
                indexed.operation_key,
                indexed.operation_version,
            )?;
            Self::restore_index_plan_row_cas(
                &mut plan,
                RecordFamily::System,
                indexed.key.clone(),
                indexed.version,
            )?;
            let destination_key = restore_index_entry_key(
                self.mount,
                indexed.entry.owner.ref_set_id,
                destination.dentry.parent,
                &destination.dentry.name,
            );
            if destination_key != indexed.key {
                plan.set_mutation(delete_mutation(RecordFamily::System, indexed.key));
                self.restore_index_plan_put_at(
                    &mut plan,
                    destination_key,
                    encode_restore_index_entry(&RestoreIndexEntry {
                        owner: indexed.entry.owner,
                        projection: destination.clone(),
                    }),
                    version,
                )?;
                self.restore_index_plan_parent_owner(
                    &mut plan,
                    indexed.entry.owner,
                    destination.dentry.parent,
                    version,
                )?;
            } else {
                plan.set_mutation(Mutation {
                    family: RecordFamily::System,
                    key: destination_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_index_entry(&RestoreIndexEntry {
                        owner: indexed.entry.owner,
                        projection: destination.clone(),
                    }))),
                });
            }
        }

        let source_target = restore_index_target_from_projection(source);
        let destination_target = restore_index_target_from_projection(destination);
        let mut containment = BTreeMap::<u64, bool>::new();
        for indexed in self.restore_index_rows_for_target(&source_target, version)? {
            Self::restore_index_plan_operation_cas(
                &mut plan,
                indexed.operation_key,
                indexed.operation_version,
            )?;
            for (key, row_version) in [
                (indexed.owner_key.clone(), indexed.owner_version),
                (indexed.inverse_key.clone(), indexed.inverse_version),
            ] {
                Self::restore_index_plan_row_cas(
                    &mut plan,
                    RecordFamily::System,
                    key.clone(),
                    row_version,
                )?;
                plan.set_mutation(delete_mutation(RecordFamily::System, key));
            }
            let within_catalog =
                if let Some(within) = containment.get(&indexed.row.catalog_root.get()).copied() {
                    within
                } else {
                    let within = self.restore_index_directory_contains(
                        indexed.row.catalog_root,
                        destination.dentry.parent,
                        version,
                    )?;
                    containment.insert(indexed.row.catalog_root.get(), within);
                    within
                };
            if !within_catalog {
                continue;
            }
            let mut row = indexed.row;
            row.target = destination_target.clone();
            let owner_key = restore_index_row_key(
                self.mount,
                row.owner.ref_set_id,
                row.catalog_root,
                &row.target,
            );
            let inverse_key = restore_index_target_inverse_key(
                self.mount,
                &row.target,
                row.owner.ref_set_id,
                row.catalog_root,
            );
            let encoded = encode_restore_index_row(&row);
            self.restore_index_plan_put_at(&mut plan, owner_key, encoded.clone(), version)?;
            self.restore_index_plan_put_at(&mut plan, inverse_key, encoded, version)?;
        }
        self.finalize_restore_index_mvcc_plan(plan, version, commit_version)
    }

    fn restore_index_target_is_current(
        &self,
        target: &RestoreIndexTarget,
        version: Version,
    ) -> Result<bool, MetadError> {
        let Some(parent) = target.parent else {
            let Some(attr) = self.get_attr_at_version_for_purpose(
                target.inode,
                version,
                ReadPurpose::RestoreStaging,
            )?
            else {
                return Ok(false);
            };
            let body = if attr.file_type == FileType::File {
                self.body_descriptor_at_version_for_purpose(
                    target.inode,
                    attr.generation,
                    version,
                    ReadPurpose::RestoreStaging,
                )?
            } else {
                None
            };
            return Ok(target.inode == attr.inode
                && target.file_type == attr.file_type
                && target.attr_generation == attr.generation
                && target.body_digest == restore_index_body_digest(body.as_ref()));
        };
        let name = DentryName::new(target.name.clone())
            .map_err(|error| MetadError::Codec(error.to_string()))?;
        let Some((entry, _)) = self.lookup_plus_at_version_for_purpose(
            parent,
            &name,
            version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(false);
        };
        Ok(restore_index_target_matches_projection(
            target,
            &DentryProjection {
                dentry: entry.dentry,
                attr: entry.attr,
                body: entry.body,
            },
        ))
    }

    fn commit_restore_index_release_deletes(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        primary_key: Vec<u8>,
        mut predicates: Vec<PredicateRef>,
        mutations: Vec<Mutation>,
    ) -> Result<(), MetadError> {
        if mutations.is_empty() {
            return Ok(());
        }
        predicates.push(PredicateRef {
            family: RecordFamily::System,
            key: restore_operation_key(self.mount, &operation.operation_digest),
            predicate: Predicate::VersionEquals(operation_version),
        });
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-release-index",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        super::restore::validate_restore_command_bounds(&command, "restore index release page")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn release_restore_index_mvcc_page(
        &self,
        page: RestoreIndexReleasePage<'_>,
    ) -> Result<RestoreIndexReleaseOutcome, MetadError> {
        let RestoreIndexReleasePage {
            operation,
            operation_version,
            stage,
            logical_prefix,
            start_after,
            limit,
            version,
        } = page;
        let physical_prefix = restore_index_mvcc_prefix(self.mount, &logical_prefix)?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: physical_prefix.clone(),
            start_after,
            version,
            limit,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut predicates = Vec::with_capacity(rows.len() * 2);
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        for row in &rows {
            let record = decode_restore_index_mvcc(&row.value.0)?;
            let commit_version = Version::new(record.commit_version)?;
            if row.version != commit_version
                || row.key
                    != restore_index_mvcc_key(self.mount, &record.logical_key, commit_version)?
                || !record.logical_key.starts_with(&logical_prefix)
            {
                return Err(MetadError::Codec(
                    "restore index MVCC release row changed identity".to_owned(),
                ));
            }
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
            if let Some(inverse_logical) =
                restore_index_mvcc_inverse_logical_key(self.mount, &record)?
            {
                let inverse_key =
                    restore_index_mvcc_key(self.mount, &inverse_logical, commit_version)?;
                let inverse = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &inverse_key,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore index MVCC owner has no inverse version".to_owned(),
                        )
                    })?;
                let inverse_record = decode_restore_index_mvcc(&inverse.value.0)?;
                if inverse.version != commit_version
                    || inverse_record.commit_version != record.commit_version
                    || inverse_record.kind != record.kind
                    || inverse_record.logical_key != inverse_logical
                    || inverse_record.value != record.value
                {
                    return Err(MetadError::Codec(
                        "restore index MVCC owner/inverse version mismatch".to_owned(),
                    ));
                }
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::VersionEquals(inverse.version),
                });
                mutations.push(delete_mutation(RecordFamily::System, inverse_key));
            }
        }
        self.commit_restore_index_release_deletes(
            operation,
            operation_version,
            physical_prefix,
            predicates,
            mutations,
        )?;
        Ok(restore_index_release_page_outcome(stage, &rows, limit, 0))
    }

    fn audit_restore_index_inverse_page(
        &self,
        operation: &RestoreOperation,
        stage: RestoreIndexReleaseStage,
        start_after: Option<Vec<u8>>,
        limit: usize,
        version: Version,
    ) -> Result<RestoreIndexReleaseOutcome, MetadError> {
        let (label, logical_label, kind, mvcc) = match stage {
            RestoreIndexReleaseStage::AuditParentInverses => (
                INDEX_PARENT_INVERSE_LABEL,
                INDEX_PARENT_INVERSE_LABEL,
                1_u8,
                false,
            ),
            RestoreIndexReleaseStage::AuditCatalogInverses => (
                INDEX_CATALOG_INVERSE_LABEL,
                INDEX_CATALOG_INVERSE_LABEL,
                2_u8,
                false,
            ),
            RestoreIndexReleaseStage::AuditTargetInverses => (
                INDEX_TARGET_INVERSE_LABEL,
                INDEX_TARGET_INVERSE_LABEL,
                3_u8,
                false,
            ),
            RestoreIndexReleaseStage::AuditMvccParentInverses => (
                INDEX_MVCC_PARENT_INVERSE_LABEL,
                INDEX_PARENT_INVERSE_LABEL,
                1_u8,
                true,
            ),
            RestoreIndexReleaseStage::AuditMvccCatalogInverses => (
                INDEX_MVCC_CATALOG_INVERSE_LABEL,
                INDEX_CATALOG_INVERSE_LABEL,
                2_u8,
                true,
            ),
            RestoreIndexReleaseStage::AuditMvccTargetInverses => (
                INDEX_MVCC_TARGET_INVERSE_LABEL,
                INDEX_TARGET_INVERSE_LABEL,
                3_u8,
                true,
            ),
            _ => {
                return Err(MetadError::Codec(
                    "restore index inverse audit has an invalid phase".to_owned(),
                ))
            }
        };
        let prefix = restore_index_system_key(self.mount, label);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: prefix.clone(),
            start_after,
            version,
            limit,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        for row in &rows {
            let suffix_len = match (kind, mvcc) {
                (1 | 2, false) => 16,
                (3, false) => 48,
                (1 | 2, true) => 24,
                (3, true) => 56,
                _ => unreachable!(),
            };
            if row.key.len() != prefix.len() + suffix_len {
                // The row cannot be attributed to this ref-set from its key.
                // Mount-wide fsck reports it; stranding every unrelated release
                // job on one unowned malformed key would violate isolation.
                continue;
            }
            let ref_set_range = match (kind, mvcc) {
                (1 | 2, false) => row.key.len() - 8..row.key.len(),
                (3, false) => row.key.len() - 16..row.key.len() - 8,
                (1 | 2, true) => row.key.len() - 16..row.key.len() - 8,
                (3, true) => row.key.len() - 24..row.key.len() - 16,
                _ => unreachable!(),
            };
            let key_ref_set =
                u64::from_be_bytes(row.key[ref_set_range].try_into().expect("u64 width"));
            if key_ref_set != operation.ref_set_id {
                continue;
            }
            let (owner, logical_key) = if mvcc {
                let record = decode_restore_index_mvcc(&row.value.0)?;
                let commit_version = Version::new(record.commit_version)?;
                if row.version != commit_version
                    || row.key
                        != restore_index_mvcc_key(self.mount, &record.logical_key, commit_version)?
                    || restore_index_logical_label_for_key(self.mount, &record.logical_key)
                        != Some(logical_label)
                {
                    return Err(MetadError::Codec(
                        "restore index MVCC inverse audit changed identity".to_owned(),
                    ));
                }
                let owner = match kind {
                    1 => decode_restore_index_owner(&record.value)?,
                    2 => decode_restore_index_catalog(&record.value)?.owner,
                    3 => decode_restore_index_row(&record.value)?.owner,
                    _ => unreachable!(),
                };
                (owner, record.logical_key)
            } else {
                let owner = match kind {
                    1 => decode_restore_index_owner(&row.value.0)?,
                    2 => decode_restore_index_catalog(&row.value.0)?.owner,
                    3 => decode_restore_index_row(&row.value.0)?.owner,
                    _ => unreachable!(),
                };
                (owner, row.key.clone())
            };
            if owner.ref_set_id != operation.ref_set_id {
                return Err(MetadError::Codec(
                    "restore index inverse audit owner/key ref-set mismatch".to_owned(),
                ));
            }
            return Err(MetadError::Codec(format!(
                "restore index inverse remains after owner release for ref-set {} (logical key bytes {})",
                operation.ref_set_id,
                logical_key.len()
            )));
        }
        Ok(restore_index_release_page_outcome(stage, &rows, limit, 0))
    }

    /// Delete one bounded page of operation-scoped index state. Releasing
    /// operations retain escaped, still-reachable identities; cleanup and
    /// discard operations delete hidden rows unconditionally. A retained row
    /// advances the cursor exactly like a deleted row so one borrower cannot
    /// starve later entries.
    pub(super) fn release_restore_index_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        cursor: &[u8],
        limit: usize,
    ) -> Result<RestoreIndexReleaseOutcome, MetadError> {
        if !matches!(
            operation.state,
            RestoreOperationState::Releasing
                | RestoreOperationState::Cleaning
                | RestoreOperationState::Discarding
        ) {
            return Err(MetadError::RestoreInProgress);
        }
        let retain_reachable = operation.state == RestoreOperationState::Releasing;
        let limit = limit.clamp(1, super::restore::RESTORE_BATCH_ENTRIES);
        let (stage, start_after) = decode_restore_index_release_cursor(cursor)?;
        let version = self.read_version()?;
        let owner = RestoreIndexOwner::from_operation(operation);
        let mut retained = 0_usize;

        match stage {
            RestoreIndexReleaseStage::Entries => {
                let prefix = restore_index_entry_prefix(self.mount, operation.ref_set_id, None);
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after,
                    version,
                    limit,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                let mut predicates = Vec::with_capacity(rows.len());
                let mut mutations = Vec::with_capacity(rows.len());
                let mut decoded = Vec::with_capacity(rows.len());
                let mut candidates = HashSet::with_capacity(rows.len());
                for row in &rows {
                    let entry = decode_restore_index_entry(&row.value.0)?;
                    if entry.owner != owner
                        || row.key
                            != restore_index_entry_key(
                                self.mount,
                                owner.ref_set_id,
                                entry.projection.dentry.parent,
                                &entry.projection.dentry.name,
                            )
                    {
                        return Err(MetadError::Codec(
                            "restore index release entry changed identity".to_owned(),
                        ));
                    }
                    let target = restore_index_target_from_projection(&entry.projection);
                    if retain_reachable {
                        candidates.insert(target.inode);
                    }
                    decoded.push(target);
                }
                let reachable = self.restore_reachable_inodes_at(&candidates, version)?;
                for (row, target) in rows.iter().zip(decoded) {
                    if retain_reachable
                        && reachable.contains(&target.inode)
                        && self.restore_index_target_is_current(&target, version)?
                    {
                        retained = retained.saturating_add(1);
                        continue;
                    }
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: row.key.clone(),
                        predicate: Predicate::VersionEquals(row.version),
                    });
                    mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
                }
                self.commit_restore_index_release_deletes(
                    operation,
                    operation_version,
                    prefix,
                    predicates,
                    mutations,
                )?;
                return Ok(restore_index_release_page_outcome(
                    stage, &rows, limit, retained,
                ));
            }
            RestoreIndexReleaseStage::Rows => {
                let prefix = restore_index_row_prefix(self.mount, operation.ref_set_id, None);
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after,
                    version,
                    limit,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                let mut predicates = Vec::with_capacity(rows.len() * 2);
                let mut mutations = Vec::with_capacity(rows.len() * 2);
                let mut decoded_rows = Vec::with_capacity(rows.len());
                let mut candidates = HashSet::with_capacity(rows.len());
                for row in &rows {
                    let decoded = decode_restore_index_row(&row.value.0)?;
                    if decoded.owner != owner
                        || row.key
                            != restore_index_row_key(
                                self.mount,
                                owner.ref_set_id,
                                decoded.catalog_root,
                                &decoded.target,
                            )
                    {
                        return Err(MetadError::Codec(
                            "restore index release custom row changed identity".to_owned(),
                        ));
                    }
                    let inverse_key = restore_index_target_inverse_key(
                        self.mount,
                        &decoded.target,
                        owner.ref_set_id,
                        decoded.catalog_root,
                    );
                    let inverse = self
                        .metadata
                        .get_versioned(
                            RecordFamily::System,
                            &inverse_key,
                            version,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "restore index custom row has no target inverse".to_owned(),
                            )
                        })?;
                    if decode_restore_index_row(&inverse.value.0)? != decoded {
                        return Err(MetadError::Codec(
                            "restore index custom row/inverse mismatch".to_owned(),
                        ));
                    }
                    if retain_reachable {
                        candidates.insert(decoded.target.inode);
                    }
                    decoded_rows.push((decoded, inverse_key, inverse.version));
                }
                let reachable = self.restore_reachable_inodes_at(&candidates, version)?;
                for (row, (decoded, inverse_key, inverse_version)) in rows.iter().zip(decoded_rows)
                {
                    if retain_reachable
                        && reachable.contains(&decoded.target.inode)
                        && self.restore_index_target_is_current(&decoded.target, version)?
                    {
                        retained = retained.saturating_add(1);
                        continue;
                    }
                    predicates.extend([
                        PredicateRef {
                            family: RecordFamily::System,
                            key: row.key.clone(),
                            predicate: Predicate::VersionEquals(row.version),
                        },
                        PredicateRef {
                            family: RecordFamily::System,
                            key: inverse_key.clone(),
                            predicate: Predicate::VersionEquals(inverse_version),
                        },
                    ]);
                    mutations.extend([
                        delete_mutation(RecordFamily::System, row.key.clone()),
                        delete_mutation(RecordFamily::System, inverse_key),
                    ]);
                }
                self.commit_restore_index_release_deletes(
                    operation,
                    operation_version,
                    prefix,
                    predicates,
                    mutations,
                )?;
                return Ok(restore_index_release_page_outcome(
                    stage, &rows, limit, retained,
                ));
            }
            RestoreIndexReleaseStage::Catalogs => {
                let prefix = restore_index_catalog_prefix(self.mount, operation.ref_set_id);
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after,
                    version,
                    limit,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                let mut predicates = Vec::with_capacity(rows.len() * 2);
                let mut mutations = Vec::with_capacity(rows.len() * 2);
                let mut decoded_rows = Vec::with_capacity(rows.len());
                let mut candidates = HashSet::with_capacity(rows.len());
                for row in &rows {
                    let decoded = decode_restore_index_catalog(&row.value.0)?;
                    if decoded.owner != owner
                        || row.key
                            != restore_index_catalog_key(
                                self.mount,
                                owner.ref_set_id,
                                decoded.catalog_root,
                            )
                    {
                        return Err(MetadError::Codec(
                            "restore index release catalog changed identity".to_owned(),
                        ));
                    }
                    let inverse_key = restore_index_catalog_inverse_key(
                        self.mount,
                        decoded.catalog_root,
                        owner.ref_set_id,
                    );
                    let inverse = self
                        .metadata
                        .get_versioned(
                            RecordFamily::System,
                            &inverse_key,
                            version,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec("restore index catalog has no inverse".to_owned())
                        })?;
                    if decode_restore_index_catalog(&inverse.value.0)? != decoded {
                        return Err(MetadError::Codec(
                            "restore index catalog/inverse mismatch".to_owned(),
                        ));
                    }
                    let has_rows = !self
                        .metadata
                        .scan(ScanRequest {
                            family: RecordFamily::System,
                            prefix: restore_index_row_prefix(
                                self.mount,
                                owner.ref_set_id,
                                Some(decoded.catalog_root),
                            ),
                            start_after: None,
                            version,
                            limit: 1,
                            purpose: ReadPurpose::WritePlanLocal,
                        })?
                        .is_empty();
                    if retain_reachable && !has_rows {
                        candidates.insert(decoded.catalog_root);
                    }
                    decoded_rows.push((decoded, inverse_key, inverse.version, has_rows));
                }
                let reachable = self.restore_reachable_inodes_at(&candidates, version)?;
                for (row, (decoded, inverse_key, inverse_version, has_rows)) in
                    rows.iter().zip(decoded_rows)
                {
                    if retain_reachable && (has_rows || reachable.contains(&decoded.catalog_root)) {
                        retained = retained.saturating_add(1);
                        continue;
                    }
                    predicates.extend([
                        PredicateRef {
                            family: RecordFamily::System,
                            key: row.key.clone(),
                            predicate: Predicate::VersionEquals(row.version),
                        },
                        PredicateRef {
                            family: RecordFamily::System,
                            key: inverse_key.clone(),
                            predicate: Predicate::VersionEquals(inverse_version),
                        },
                    ]);
                    mutations.extend([
                        delete_mutation(RecordFamily::System, row.key.clone()),
                        delete_mutation(RecordFamily::System, inverse_key),
                    ]);
                }
                self.commit_restore_index_release_deletes(
                    operation,
                    operation_version,
                    prefix,
                    predicates,
                    mutations,
                )?;
                return Ok(restore_index_release_page_outcome(
                    stage, &rows, limit, retained,
                ));
            }
            RestoreIndexReleaseStage::Parents => {
                let prefix = restore_index_parent_owner_prefix(self.mount, operation.ref_set_id);
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after,
                    version,
                    limit,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                let mut predicates = Vec::with_capacity(rows.len() * 2);
                let mut mutations = Vec::with_capacity(rows.len() * 2);
                for row in &rows {
                    if row.key.len() != prefix.len() + 8 {
                        return Err(MetadError::Codec(
                            "restore index parent owner key has an invalid length".to_owned(),
                        ));
                    }
                    let decoded_owner = decode_restore_index_owner(&row.value.0)?;
                    if decoded_owner != owner {
                        return Err(MetadError::Codec(
                            "restore index parent owner changed identity".to_owned(),
                        ));
                    }
                    let parent = InodeId::new(u64::from_be_bytes(
                        row.key[prefix.len()..].try_into().expect("u64 width"),
                    ))?;
                    if !self
                        .metadata
                        .scan(ScanRequest {
                            family: RecordFamily::System,
                            prefix: restore_index_entry_prefix(
                                self.mount,
                                owner.ref_set_id,
                                Some(parent),
                            ),
                            start_after: None,
                            version,
                            limit: 1,
                            purpose: ReadPurpose::WritePlanLocal,
                        })?
                        .is_empty()
                    {
                        retained = retained.saturating_add(1);
                        continue;
                    }
                    let inverse_key =
                        restore_index_parent_inverse_key(self.mount, parent, owner.ref_set_id);
                    let inverse = self
                        .metadata
                        .get_versioned(
                            RecordFamily::System,
                            &inverse_key,
                            version,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "restore index parent owner has no inverse".to_owned(),
                            )
                        })?;
                    if decode_restore_index_owner(&inverse.value.0)? != owner {
                        return Err(MetadError::Codec(
                            "restore index parent owner/inverse mismatch".to_owned(),
                        ));
                    }
                    predicates.extend([
                        PredicateRef {
                            family: RecordFamily::System,
                            key: row.key.clone(),
                            predicate: Predicate::VersionEquals(row.version),
                        },
                        PredicateRef {
                            family: RecordFamily::System,
                            key: inverse_key.clone(),
                            predicate: Predicate::VersionEquals(inverse.version),
                        },
                    ]);
                    mutations.extend([
                        delete_mutation(RecordFamily::System, row.key.clone()),
                        delete_mutation(RecordFamily::System, inverse_key),
                    ]);
                }
                self.commit_restore_index_release_deletes(
                    operation,
                    operation_version,
                    prefix,
                    predicates,
                    mutations,
                )?;
                return Ok(restore_index_release_page_outcome(
                    stage, &rows, limit, retained,
                ));
            }
            RestoreIndexReleaseStage::MvccEntries => {
                return self.release_restore_index_mvcc_page(RestoreIndexReleasePage {
                    operation,
                    operation_version,
                    stage,
                    logical_prefix: restore_index_entry_prefix(
                        self.mount,
                        operation.ref_set_id,
                        None,
                    ),
                    start_after,
                    limit,
                    version,
                });
            }
            RestoreIndexReleaseStage::MvccRows => {
                return self.release_restore_index_mvcc_page(RestoreIndexReleasePage {
                    operation,
                    operation_version,
                    stage,
                    logical_prefix: restore_index_row_prefix(
                        self.mount,
                        operation.ref_set_id,
                        None,
                    ),
                    start_after,
                    limit,
                    version,
                });
            }
            RestoreIndexReleaseStage::MvccCatalogs => {
                return self.release_restore_index_mvcc_page(RestoreIndexReleasePage {
                    operation,
                    operation_version,
                    stage,
                    logical_prefix: restore_index_catalog_prefix(self.mount, operation.ref_set_id),
                    start_after,
                    limit,
                    version,
                });
            }
            RestoreIndexReleaseStage::MvccParents => {
                return self.release_restore_index_mvcc_page(RestoreIndexReleasePage {
                    operation,
                    operation_version,
                    stage,
                    logical_prefix: restore_index_parent_owner_prefix(
                        self.mount,
                        operation.ref_set_id,
                    ),
                    start_after,
                    limit,
                    version,
                });
            }
            RestoreIndexReleaseStage::AuditParentInverses
            | RestoreIndexReleaseStage::AuditCatalogInverses
            | RestoreIndexReleaseStage::AuditTargetInverses
            | RestoreIndexReleaseStage::AuditMvccParentInverses
            | RestoreIndexReleaseStage::AuditMvccCatalogInverses
            | RestoreIndexReleaseStage::AuditMvccTargetInverses => {
                return self.audit_restore_index_inverse_page(
                    operation,
                    stage,
                    start_after,
                    limit,
                    version,
                );
            }
            RestoreIndexReleaseStage::SourceMembers => {
                let prefix = restore_index_source_member_prefix(self.mount, operation.ref_set_id);
                let rows = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after,
                    version,
                    limit,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                let mut predicates = Vec::with_capacity(rows.len());
                let mut mutations = Vec::with_capacity(rows.len());
                for row in &rows {
                    let source = decode_restore_index_source_member(&row.value.0)?;
                    if source.owner != owner
                        || row.key
                            != restore_index_source_member_key(
                                self.mount,
                                owner.ref_set_id,
                                source.source_inode,
                            )
                    {
                        return Err(MetadError::Codec(
                            "restore index release source member changed identity".to_owned(),
                        ));
                    }
                    if retain_reachable {
                        let staging_key = restore_staging_member_key(
                            self.mount,
                            owner.ref_set_id,
                            source.member.destination_inode,
                        );
                        if let Some(staging) = self.metadata.get(
                            RecordFamily::System,
                            &staging_key,
                            version,
                            ReadPurpose::WritePlanLocal,
                        )? {
                            if !restore_index_source_member_matches_staging(
                                &source.member,
                                &decode_restore_staging_member(&staging.0)?,
                            ) {
                                return Err(MetadError::Codec(
                                    "restore index release source/staging member mismatch"
                                        .to_owned(),
                                ));
                            }
                            retained = retained.saturating_add(1);
                            continue;
                        }
                    }
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: row.key.clone(),
                        predicate: Predicate::VersionEquals(row.version),
                    });
                    mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
                }
                self.commit_restore_index_release_deletes(
                    operation,
                    operation_version,
                    prefix,
                    predicates,
                    mutations,
                )?;
                return Ok(restore_index_release_page_outcome(
                    stage, &rows, limit, retained,
                ));
            }
            RestoreIndexReleaseStage::MvccSourceMembers => {
                return self.release_restore_index_mvcc_page(RestoreIndexReleasePage {
                    operation,
                    operation_version,
                    stage,
                    logical_prefix: restore_index_source_member_prefix(
                        self.mount,
                        operation.ref_set_id,
                    ),
                    start_after,
                    limit,
                    version,
                });
            }
            RestoreIndexReleaseStage::Seal => {}
        }

        let owner_prefixes = [
            restore_index_entry_prefix(self.mount, operation.ref_set_id, None),
            restore_index_parent_owner_prefix(self.mount, operation.ref_set_id),
            restore_index_catalog_prefix(self.mount, operation.ref_set_id),
            restore_index_row_prefix(self.mount, operation.ref_set_id, None),
            restore_index_source_member_prefix(self.mount, operation.ref_set_id),
        ];
        for prefix in owner_prefixes {
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
                return Ok(RestoreIndexReleaseOutcome {
                    cursor: Vec::new(),
                    complete: false,
                    retained: 1,
                });
            }
        }
        // Target-first inverse keyspaces cannot participate in the final
        // ref-set PrefixEmpty predicates. Audit their full closure here; an
        // orphan is corruption and must strand/quarantine the release rather
        // than letting operation deletion hide it.
        let seal_key = restore_index_seal_key(self.mount, operation.ref_set_id);
        let marker_key = restore_index_complete_key(self.mount, operation.ref_set_id);
        let marker = self.metadata.get_versioned(
            RecordFamily::System,
            &marker_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        if let Some(marker) = &marker {
            let decoded = decode_restore_index_complete(&marker.value.0)?;
            if decoded.operation_digest != operation.operation_digest
                || decoded.initialization_digest != operation.initialization_digest
                || decoded.ref_set_id != operation.ref_set_id
                || decoded.incarnation != operation.created_version
            {
                return Err(MetadError::Codec(
                    "restore index release Complete marker changed identity".to_owned(),
                ));
            }
        }
        if let Some(seal) = self.metadata.get_versioned(
            RecordFamily::System,
            &seal_key,
            version,
            ReadPurpose::WritePlanLocal,
        )? {
            let decoded = decode_restore_index_seal(&seal.value.0)?;
            if decoded.operation_digest != operation.operation_digest
                || decoded.initialization_digest != operation.initialization_digest
                || decoded.ref_set_id != operation.ref_set_id
                || decoded.incarnation != operation.created_version
            {
                return Err(MetadError::Codec(
                    "restore index release seal changed identity".to_owned(),
                ));
            }
            let mut predicates = vec![PredicateRef {
                family: RecordFamily::System,
                key: seal_key.clone(),
                predicate: Predicate::VersionEquals(seal.version),
            }];
            let mut mutations = vec![delete_mutation(RecordFamily::System, seal_key.clone())];
            match marker {
                Some(marker) => {
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: marker_key.clone(),
                        predicate: Predicate::VersionEquals(marker.version),
                    });
                    mutations.push(delete_mutation(RecordFamily::System, marker_key));
                }
                None => predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: marker_key,
                    predicate: Predicate::NotExists,
                }),
            }
            self.commit_restore_index_release_deletes(
                operation,
                operation_version,
                seal_key,
                predicates,
                mutations,
            )?;
        } else if let Some(marker) = marker {
            self.commit_restore_index_release_deletes(
                operation,
                operation_version,
                marker_key.clone(),
                vec![PredicateRef {
                    family: RecordFamily::System,
                    key: marker_key.clone(),
                    predicate: Predicate::VersionEquals(marker.version),
                }],
                vec![delete_mutation(RecordFamily::System, marker_key)],
            )?;
        }
        Ok(RestoreIndexReleaseOutcome {
            cursor: Vec::new(),
            complete: true,
            retained: 0,
        })
    }

    fn canonical_custom_index_at_path(
        &self,
        path: &str,
        catalog_value: &Value,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<RestoreCustomIndex, MetadError> {
        let catalog = decode_path_index_catalog(&catalog_value.0)
            .map_err(|error| MetadError::Codec(error.to_string()))?;
        if catalog.path != path {
            return Err(MetadError::Codec(
                "namespace index catalog path does not match its key".to_owned(),
            ));
        }
        let mut rows = Vec::new();
        let row_prefix = path_index_row_prefix(self.mount, path);
        self.restore_index_for_each_raw(
            RecordFamily::PathIndex,
            &row_prefix,
            None,
            version,
            purpose,
            RESTORE_INDEX_SCAN_PAGE,
            |item| {
                let row = decode_path_index_row(&item.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))?;
                if item.key != path_index_row_key(self.mount, path, &row.path) {
                    return Err(MetadError::Codec(
                        "namespace index row path does not match its key".to_owned(),
                    ));
                }
                if restore_index_relative_components(path, &row.path)?.is_none() {
                    return Err(MetadError::Codec(
                        "namespace index row escaped its catalog root".to_owned(),
                    ));
                }
                rows.push(row);
                Ok(true)
            },
        )?;
        Ok(RestoreCustomIndex {
            catalog: PathIndexCatalogRecord {
                path: catalog.path,
                fields: catalog.fields,
                row_count: rows.len() as u64,
            },
            rows,
        })
    }

    fn restore_index_path_metadata_if_current(
        &self,
        root: InodeId,
        relative_path: &str,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<PathMetadata>, MetadError> {
        match self.stat_path_from_at_version_for_purpose(root, relative_path, version, purpose) {
            Ok(metadata) => Ok(metadata),
            Err(MetadError::NotFound | MetadError::NotDirectory) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Rebase one durable row from the catalog path recorded at restore time to
    /// the catalog's current query path, then prove that the exact dentry/body
    /// identity still occupies that path. Runtime queries are intentionally
    /// driven by index rows: they never enumerate unrelated namespace entries
    /// and therefore never inherit the restore creation-time one-million-entry
    /// resource limit.
    fn current_restore_index_row_path(
        &self,
        query_root: InodeId,
        query_path: &str,
        catalog_path: &str,
        row: &RestoreIndexRow,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<String>, MetadError> {
        let Some(relative) = restore_index_relative_components(catalog_path, &row.record.path)?
        else {
            return Err(MetadError::Codec(
                "restore index row escaped its recorded catalog root".to_owned(),
            ));
        };
        let current_path = restore_index_join_path(query_path, &relative)?;
        if relative.is_empty() {
            let Some(metadata) =
                self.restore_index_path_metadata_if_current(query_root, "/", version, purpose)?
            else {
                return Ok(None);
            };
            return Ok((restore_index_root_target(&metadata) == row.target).then_some(current_path));
        }

        let relative_path = canonical_path(&relative)?;
        let entry = match self.lookup_path_from_at_version_for_purpose(
            query_root,
            &relative_path,
            version,
            purpose,
        ) {
            Ok(entry) => entry,
            Err(MetadError::NotFound | MetadError::NotDirectory) => None,
            Err(error) => return Err(error),
        };
        let Some((entry, _)) = entry else {
            return Ok(None);
        };
        let projection = DentryProjection {
            dentry: entry.dentry,
            attr: entry.attr,
            body: entry.body,
        };
        Ok(
            restore_index_target_matches_projection(&row.target, &projection)
                .then_some(current_path),
        )
    }

    fn restore_index_recorded_path_is_current(
        &self,
        query_root: InodeId,
        query_path: &str,
        recorded_path: &str,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<bool, MetadError> {
        let Some(relative) = restore_index_relative_components(query_path, recorded_path)? else {
            return Ok(false);
        };
        let relative_path = canonical_path(&relative)?;
        Ok(self
            .restore_index_path_metadata_if_current(query_root, &relative_path, version, purpose)?
            .is_some())
    }

    /// Effective custom index at an exact namespace root. Canonical records
    /// override inherited overlay rows of the same current path; otherwise
    /// fields and rows are merged across complete nested restores. Every
    /// overlay row is checked against both its inverse and the current dentry /
    /// body identity before it is returned.
    pub(super) fn restore_custom_index_at_path(
        &self,
        path: &str,
        root: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<RestoreCustomIndex>, MetadError> {
        let canonical_value = self.metadata.get(
            RecordFamily::PathIndex,
            &path_index_catalog_key(self.mount, path),
            version,
            purpose,
        )?;
        let fork_source = self.restore_index_fork_source(root)?;
        let inverse_prefix = restore_index_catalog_inverse_prefix(self.mount, root);
        let mut has_visible_overlay = false;
        self.restore_index_effective_for_each(
            &inverse_prefix,
            None,
            version,
            purpose,
            |inverse| {
                if inverse.key.len() != inverse_prefix.len() + 8 {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse key has an invalid length".to_owned(),
                    ));
                }
                let catalog = decode_restore_index_catalog(&inverse.value.0)?;
                let ref_set_id = u64::from_be_bytes(
                    inverse.key[inverse_prefix.len()..]
                        .try_into()
                        .expect("u64 width"),
                );
                if catalog.catalog_root != root
                    || catalog.owner.ref_set_id != ref_set_id
                    || inverse.key
                        != restore_index_catalog_inverse_key(self.mount, root, ref_set_id)
                {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse changed identity".to_owned(),
                    ));
                }
                if self
                    .visible_restore_index_operation(catalog.owner, version, purpose)?
                    .is_some()
                {
                    has_visible_overlay = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )?;

        if canonical_value.is_none() && !has_visible_overlay && fork_source.is_none() {
            return Ok(None);
        }
        if !has_visible_overlay && fork_source.is_none() {
            return canonical_value
                .as_ref()
                .map(|catalog| self.canonical_custom_index_at_path(path, catalog, version, purpose))
                .transpose();
        }

        // Overlay and generic-fork queries validate only index candidates. They
        // must not build a map of every dentry under `root`: unindexed growth is
        // unrelated to the index result and may continue beyond restore's
        // creation-time subtree limit.
        let mut fields = BTreeMap::<String, PathIndexFieldRecord>::new();
        let mut rows = BTreeMap::<String, PathIndexRowRecord>::new();
        let mut found_catalog = false;

        self.restore_index_effective_for_each(
            &inverse_prefix,
            None,
            version,
            purpose,
            |inverse| {
                if inverse.key.len() != inverse_prefix.len() + 8 {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse key has an invalid length".to_owned(),
                    ));
                }
                let catalog = decode_restore_index_catalog(&inverse.value.0)?;
                let ref_set_id = u64::from_be_bytes(
                    inverse.key[inverse_prefix.len()..]
                        .try_into()
                        .expect("u64 width"),
                );
                if catalog.catalog_root != root
                    || catalog.owner.ref_set_id != ref_set_id
                    || inverse.key
                        != restore_index_catalog_inverse_key(self.mount, root, ref_set_id)
                {
                    return Err(MetadError::Codec(
                        "restore index catalog inverse changed identity".to_owned(),
                    ));
                }
                if self
                    .visible_restore_index_operation(catalog.owner, version, purpose)?
                    .is_none()
                {
                    return Ok(true);
                }
                let owner_key = restore_index_catalog_key(self.mount, ref_set_id, root);
                let owner_item = self
                    .restore_index_effective_get(&owner_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index catalog inverse has no owner".to_owned())
                    })?;
                if decode_restore_index_catalog(&owner_item.value.0)? != catalog {
                    return Err(MetadError::Codec(
                        "restore index catalog owner/inverse mismatch".to_owned(),
                    ));
                }
                found_catalog = true;
                for field in catalog.record.fields.iter().cloned() {
                    match fields.entry(field.field.clone()) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(field);
                        }
                        std::collections::btree_map::Entry::Occupied(slot)
                            if slot.get() != &field =>
                        {
                            return Err(MetadError::Codec(
                                "nested restore custom catalogs disagree on a field".to_owned(),
                            ));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    }
                }
                let row_prefix = restore_index_row_prefix(self.mount, ref_set_id, Some(root));
                self.restore_index_effective_for_each(
                    &row_prefix,
                    None,
                    version,
                    purpose,
                    |item| {
                        let mut row = decode_restore_index_row(&item.value.0)?;
                        if row.owner != catalog.owner
                            || row.catalog_root != root
                            || item.key
                                != restore_index_row_key(self.mount, ref_set_id, root, &row.target)
                        {
                            return Err(MetadError::Codec(
                                "restore index custom row changed identity".to_owned(),
                            ));
                        }
                        let target_inverse_key = restore_index_target_inverse_key(
                            self.mount,
                            &row.target,
                            ref_set_id,
                            root,
                        );
                        let target_inverse = self
                            .restore_index_effective_get(&target_inverse_key, version, purpose)?
                            .ok_or_else(|| {
                                MetadError::Codec(
                                    "restore index custom row has no target inverse".to_owned(),
                                )
                            })?;
                        if decode_restore_index_row(&target_inverse.value.0)? != row {
                            return Err(MetadError::Codec(
                                "restore index custom row/inverse mismatch".to_owned(),
                            ));
                        }
                        let Some(current_path) = self.current_restore_index_row_path(
                            root,
                            path,
                            &catalog.record.path,
                            &row,
                            version,
                            purpose,
                        )?
                        else {
                            return Ok(true);
                        };
                        row.record.path = current_path;
                        match rows.entry(row.record.path.clone()) {
                            std::collections::btree_map::Entry::Vacant(slot) => {
                                slot.insert(row.record);
                            }
                            std::collections::btree_map::Entry::Occupied(slot)
                                if slot.get() != &row.record =>
                            {
                                return Err(MetadError::Codec(
                                    "nested restore custom rows disagree on a current path"
                                        .to_owned(),
                                ));
                            }
                            std::collections::btree_map::Entry::Occupied(_) => {}
                        }
                        Ok(true)
                    },
                )?;
                Ok(true)
            },
        )?;

        if let Some(fork_source) = fork_source {
            let source_version = Version::new(fork_source.binding.pinned_read_version)?;
            let source_path = &fork_source.operation.destination_path;
            if let Some(inherited) = self.restore_custom_index_at_path(
                source_path,
                fork_source.binding.source_root,
                source_version,
                ReadPurpose::Snapshot,
            )? {
                found_catalog = true;
                for field in inherited.catalog.fields {
                    match fields.entry(field.field.clone()) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(field);
                        }
                        std::collections::btree_map::Entry::Occupied(slot)
                            if slot.get() != &field =>
                        {
                            return Err(MetadError::Codec(
                                "generic fork custom catalogs disagree on a field".to_owned(),
                            ));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    }
                }
                for mut row in inherited.rows {
                    let Some(relative) = restore_index_relative_components(source_path, &row.path)?
                    else {
                        return Err(MetadError::Codec(
                            "generic fork custom row escaped its source catalog".to_owned(),
                        ));
                    };
                    let fork_path = restore_index_join_path(path, &relative)?;
                    let relative_path = canonical_path(&relative)?;
                    let Some(source_metadata) = self.restore_index_path_metadata_if_current(
                        fork_source.binding.source_root,
                        &relative_path,
                        source_version,
                        ReadPurpose::Snapshot,
                    )?
                    else {
                        continue;
                    };
                    let Some(fork_metadata) = self.restore_index_path_metadata_if_current(
                        root,
                        &relative_path,
                        version,
                        purpose,
                    )?
                    else {
                        continue;
                    };
                    if !restore_index_clone_metadata_matches_source(
                        &fork_metadata,
                        &source_metadata,
                    ) {
                        continue;
                    }
                    row.path = fork_path;
                    match rows.entry(row.path.clone()) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(row);
                        }
                        std::collections::btree_map::Entry::Occupied(slot)
                            if slot.get() != &row =>
                        {
                            return Err(MetadError::Codec(
                                "generic fork and restore overlay disagree on a custom row"
                                    .to_owned(),
                            ));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    }
                }
            }
        }

        if let Some(catalog_value) = canonical_value {
            let canonical =
                self.canonical_custom_index_at_path(path, &catalog_value, version, purpose)?;
            found_catalog = true;
            for field in canonical.catalog.fields {
                fields.insert(field.field.clone(), field);
            }
            for row in canonical.rows {
                if self.restore_index_recorded_path_is_current(
                    root,
                    &canonical.catalog.path,
                    &row.path,
                    version,
                    purpose,
                )? {
                    rows.insert(row.path.clone(), row);
                }
            }
        }

        if !found_catalog {
            return Ok(None);
        }
        let rows = rows.into_values().collect::<Vec<_>>();
        Ok(Some(RestoreCustomIndex {
            catalog: PathIndexCatalogRecord {
                path: path.to_owned(),
                fields: fields.into_values().collect(),
                row_count: rows.len() as u64,
            },
            rows,
        }))
    }

    fn collect_restore_index_owner_children(
        &self,
        query: RestoreIndexChildrenQuery<'_>,
        owner: RestoreIndexOwner,
        visible_operation: &RestoreOperation,
        entries: &mut BTreeMap<Vec<u8>, DentryWithAttr>,
        releasing_parent_reachable: &mut Option<bool>,
    ) -> Result<(), MetadError> {
        if visible_operation.state == RestoreOperationState::Releasing
            && query.purpose != ReadPurpose::Snapshot
        {
            let parent_reachable = if let Some(reachable) = *releasing_parent_reachable {
                reachable
            } else {
                let candidates = HashSet::from([query.parent]);
                let reachable = self
                    .restore_reachable_inodes_at(&candidates, query.version)?
                    .contains(&query.parent);
                *releasing_parent_reachable = Some(reachable);
                reachable
            };
            if !parent_reachable {
                return Ok(());
            }
        }
        let entry_prefix =
            restore_index_entry_prefix(self.mount, owner.ref_set_id, Some(query.parent));
        let start_after = query
            .after
            .map(|name| restore_index_entry_key(self.mount, owner.ref_set_id, query.parent, name));
        let mut accepted = 0_usize;
        self.restore_index_effective_for_each(
            &entry_prefix,
            start_after.as_deref(),
            query.version,
            query.purpose,
            |item| {
                let entry = decode_restore_index_entry(&item.value.0)?;
                if entry.owner != owner
                    || item.key
                        != restore_index_entry_key(
                            self.mount,
                            owner.ref_set_id,
                            entry.projection.dentry.parent,
                            &entry.projection.dentry.name,
                        )
                    || entry.projection.dentry.parent != query.parent
                {
                    return Err(MetadError::Codec(
                        "restore index entry does not match its key/owner".to_owned(),
                    ));
                }
                let Some((current, _)) = self.lookup_plus_at_version_for_purpose(
                    query.parent,
                    &entry.projection.dentry.name,
                    query.version,
                    query.purpose,
                )?
                else {
                    return Ok(true);
                };
                let indexed: DentryWithAttr = entry.projection.into();
                if current != indexed {
                    return Ok(true);
                }
                match entries.entry(current.dentry.name.as_bytes().to_vec()) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(current);
                    }
                    std::collections::btree_map::Entry::Occupied(slot)
                        if slot.get() != &current =>
                    {
                        return Err(MetadError::Codec(
                            "nested restore indexes disagree on a current dentry".to_owned(),
                        ));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
                if entries.len() > query.keep {
                    entries.pop_last();
                }
                accepted = accepted.saturating_add(1);
                Ok(accepted < query.keep)
            },
        )
    }

    /// Returns only rows owned by a durably complete operation and still equal
    /// to the authoritative current dentry. Corrupt owner/inverse pairs fail
    /// closed instead of silently falling back to a partial namespace view.
    /// Every owner stream contributes at most `limit + 1` candidates and MVCC
    /// history is folded in fixed pages, so pagination memory is O(limit), not
    /// O(directory entries) or O(history rows).
    pub(super) fn restore_indexed_children_page(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let requested = limit.max(1);
        let keep = requested.saturating_add(1);
        let query = RestoreIndexChildrenQuery {
            parent,
            after,
            keep,
            version,
            purpose,
        };
        let inverse_prefix = restore_index_parent_inverse_prefix_for_read(self.mount, parent);
        let mut entries = BTreeMap::<Vec<u8>, DentryWithAttr>::new();
        let mut releasing_parent_reachable = None;
        self.restore_index_effective_for_each(
            &inverse_prefix,
            None,
            version,
            purpose,
            |inverse| {
                if inverse.key.len() != inverse_prefix.len() + 8 {
                    return Err(MetadError::Codec(
                        "restore index parent inverse key has an invalid length".to_owned(),
                    ));
                }
                let owner = decode_restore_index_owner(&inverse.value.0)?;
                let ref_set_id = u64::from_be_bytes(
                    inverse.key[inverse_prefix.len()..]
                        .try_into()
                        .expect("u64 width"),
                );
                if owner.ref_set_id != ref_set_id
                    || inverse.key
                        != restore_index_parent_inverse_key(self.mount, parent, ref_set_id)
                {
                    return Err(MetadError::Codec(
                        "restore index parent inverse does not match its owner".to_owned(),
                    ));
                }
                let Some(visible_operation) =
                    self.visible_restore_index_operation(owner, version, purpose)?
                else {
                    return Ok(true);
                };
                let owner_key = restore_index_parent_owner_key(self.mount, ref_set_id, parent);
                let owner_value = self
                    .restore_index_effective_get(&owner_key, version, purpose)?
                    .ok_or_else(|| {
                        MetadError::Codec("restore index parent inverse has no owner".to_owned())
                    })?;
                if decode_restore_index_owner(&owner_value.value.0)? != owner {
                    return Err(MetadError::Codec(
                        "restore index parent owner/inverse mismatch".to_owned(),
                    ));
                }
                self.collect_restore_index_owner_children(
                    query,
                    owner,
                    &visible_operation,
                    &mut entries,
                    &mut releasing_parent_reachable,
                )?;
                Ok(true)
            },
        )?;
        if let Some(fork_source) = self.restore_index_fork_source(parent)? {
            let source_version = Version::new(fork_source.binding.pinned_read_version)?;
            let mut source_after = after.cloned();
            let mut accepted = 0_usize;
            loop {
                let source_page = self.restore_indexed_children_page(
                    fork_source.binding.source_root,
                    source_after.as_ref(),
                    keep,
                    source_version,
                    ReadPurpose::Snapshot,
                )?;
                for source in source_page.entries {
                    let Some((fork, _)) = self.lookup_plus_at_version_for_purpose(
                        parent,
                        &source.dentry.name,
                        version,
                        purpose,
                    )?
                    else {
                        continue;
                    };
                    if !restore_index_clone_entry_matches_source(&fork, &source) {
                        continue;
                    }
                    match entries.entry(fork.dentry.name.as_bytes().to_vec()) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(fork);
                        }
                        std::collections::btree_map::Entry::Occupied(slot)
                            if slot.get() != &fork =>
                        {
                            return Err(MetadError::Codec(
                                "generic fork and restore overlay disagree on a current dentry"
                                    .to_owned(),
                            ));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    }
                    if entries.len() > keep {
                        entries.pop_last();
                    }
                    accepted = accepted.saturating_add(1);
                    if accepted >= keep {
                        break;
                    }
                }
                if accepted >= keep {
                    break;
                }
                let Some(next) = source_page.next_cursor else {
                    break;
                };
                if source_after
                    .as_ref()
                    .is_some_and(|after| after.as_bytes() >= next.as_bytes())
                {
                    return Err(MetadError::Codec(
                        "generic fork inherited index cursor did not advance".to_owned(),
                    ));
                }
                source_after = Some(next);
            }
        }
        let has_more = entries.len() > requested;
        let mut entries = entries.into_values().take(requested).collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| entries.last().map(|entry| entry.dentry.name.clone()))
            .flatten();
        Ok(ReadDirPlusPage {
            entries: std::mem::take(&mut entries),
            next_cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount() -> MountId {
        MountId::new(7).unwrap()
    }

    fn operation() -> RestoreOperation {
        RestoreOperation {
            operation_digest: [3; 32],
            initialization_digest: [5; 32],
            state: RestoreOperationState::Preparing,
            source_root: InodeId::new(10).unwrap(),
            destination_root: InodeId::new(20).unwrap(),
            snapshot_id: 30,
            read_version: 40,
            created_version: 50,
            ref_set_id: 50,
            source_path: "/source".to_owned(),
            destination_path: "/destination".to_owned(),
        }
    }

    fn projection() -> DentryProjection {
        let inode = InodeId::new(22).unwrap();
        DentryProjection {
            dentry: DentryRecord {
                parent: InodeId::new(20).unwrap(),
                name: DentryName::new(b"file.txt".to_vec()).unwrap(),
                child: inode,
                child_type: FileType::File,
                attr_generation: 11,
            },
            attr: InodeAttr {
                inode,
                file_type: FileType::File,
                mode: 0o644,
                uid: 1,
                gid: 2,
                rdev: 0,
                nlink: 1,
                size: 3,
                generation: 11,
                mtime_ms: 12,
                ctime_ms: 13,
            },
            body: Some(BodyDescriptor {
                producer: "test".to_owned(),
                digest_uri: "sha256:abc".to_owned(),
                size: 3,
                content_type: "text/plain".to_owned(),
                manifest_id: "manifest".to_owned(),
                generation: 11,
                base_generation: 0,
                chunk_size: 64,
                block_size: 16,
            }),
        }
    }

    #[test]
    fn restore_index_entry_codec_is_strict() {
        let entry = RestoreIndexEntry {
            owner: RestoreIndexOwner::from_operation(&operation()),
            projection: projection(),
        };
        let encoded = encode_restore_index_entry(&entry);
        assert_eq!(decode_restore_index_entry(&encoded).unwrap(), entry);
        assert!(decode_restore_index_entry(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_restore_index_entry(&trailing).is_err());
    }

    #[test]
    fn restore_index_custom_codecs_preserve_path_specific_identity() {
        let projection = projection();
        let owner = RestoreIndexOwner::from_operation(&operation());
        let target = restore_index_target_from_projection(&projection);
        let catalog = RestoreIndexCatalog {
            owner,
            catalog_root: InodeId::new(20).unwrap(),
            record: PathIndexCatalogRecord {
                path: "/destination".to_owned(),
                fields: Vec::new(),
                row_count: 1,
            },
        };
        let row = RestoreIndexRow {
            owner,
            catalog_root: catalog.catalog_root,
            target,
            record: PathIndexRowRecord {
                path: "/destination/file.txt".to_owned(),
                values: vec![(
                    "kind".to_owned(),
                    PathIndexValueRecord::String("report".to_owned()),
                )],
            },
        };
        assert_eq!(
            decode_restore_index_catalog(&encode_restore_index_catalog(&catalog)).unwrap(),
            catalog
        );
        assert_eq!(
            decode_restore_index_row(&encode_restore_index_row(&row)).unwrap(),
            row
        );
        assert!(restore_index_target_matches_projection(
            &row.target,
            &projection
        ));
        let mut changed_generation = projection.clone();
        changed_generation.dentry.attr_generation += 1;
        changed_generation.attr.generation += 1;
        assert!(!restore_index_target_matches_projection(
            &row.target,
            &changed_generation
        ));

        let mut trailing = encode_restore_index_row(&row);
        trailing.push(0);
        assert!(decode_restore_index_row(&trailing).is_err());
    }

    #[test]
    fn restore_index_source_member_codec_is_strict() {
        let operation = operation();
        let source_inode = InodeId::new(11).unwrap();
        let source = RestoreIndexSourceMember {
            owner: RestoreIndexOwner::from_operation(&operation),
            source_inode,
            member: RestoreStagingMember {
                operation_digest: operation.operation_digest,
                source_inode: Some(source_inode),
                destination_inode: InodeId::new(21).unwrap(),
                destination_parent: Some(operation.destination_root),
                name: Some(DentryName::new(b"child".to_vec()).unwrap()),
                relative_path: "child".to_owned(),
                canonical_index_cursor: Vec::new(),
                canonical_index_complete: true,
                manifest_cursor: Vec::new(),
                manifest_block_cursor: 0,
            },
        };
        let encoded = encode_restore_index_source_member(&source).unwrap();
        assert_eq!(
            decode_restore_index_source_member(&encoded).unwrap(),
            source
        );
        assert!(decode_restore_index_source_member(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn restore_index_seal_codec_is_strict() {
        let seal = RestoreIndexSeal {
            operation_digest: [1; 32],
            initialization_digest: [2; 32],
            ref_set_id: 7,
            incarnation: 8,
            entry_count: 9,
            catalog_count: 10,
            row_count: 11,
            digest: [12; 32],
        };
        let encoded = encode_restore_index_seal(&seal);
        assert_eq!(decode_restore_index_seal(&encoded).unwrap(), seal);
        assert!(decode_restore_index_seal(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn restore_index_mvcc_codec_and_physical_key_are_strict() {
        let logical_key = restore_index_entry_key(
            mount(),
            50,
            InodeId::new(20).unwrap(),
            &DentryName::new(b"file.txt".to_vec()).unwrap(),
        );
        let record = RestoreIndexMvccRecord {
            commit_version: 70,
            kind: RestoreIndexMvccKind::Tombstone,
            logical_key: logical_key.clone(),
            value: encode_restore_index_entry(&RestoreIndexEntry {
                owner: RestoreIndexOwner::from_operation(&operation()),
                projection: projection(),
            }),
        };
        let encoded = encode_restore_index_mvcc(&record);
        assert_eq!(decode_restore_index_mvcc(&encoded).unwrap(), record);
        assert!(decode_restore_index_mvcc(&encoded[..encoded.len() - 1]).is_err());

        let version = Version::new(record.commit_version).unwrap();
        let physical = restore_index_mvcc_key(mount(), &logical_key, version).unwrap();
        assert!(physical.starts_with(&restore_index_mvcc_prefix(mount(), &logical_key).unwrap()));
        assert_ne!(
            physical,
            restore_index_mvcc_key(mount(), &logical_key, Version::new(71).unwrap()).unwrap()
        );
    }

    #[test]
    fn restore_index_complete_marker_codec_is_strict() {
        let marker = RestoreIndexCompleteMarker {
            operation_digest: [1; 32],
            initialization_digest: [2; 32],
            ref_set_id: 3,
            incarnation: 4,
            complete_version: 5,
        };
        let encoded = encode_restore_index_complete(&marker);
        assert_eq!(decode_restore_index_complete(&encoded).unwrap(), marker);
        assert!(decode_restore_index_complete(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn restore_index_keys_keep_nested_refsets_distinct() {
        let parent = InodeId::new(90).unwrap();
        let name = DentryName::new(b"child".to_vec()).unwrap();
        assert_ne!(
            restore_index_entry_key(mount(), 1, parent, &name),
            restore_index_entry_key(mount(), 2, parent, &name)
        );
        assert_ne!(
            restore_index_parent_inverse_key(mount(), parent, 1),
            restore_index_parent_inverse_key(mount(), parent, 2)
        );
    }

    #[test]
    fn restore_index_path_boundaries_are_component_aware() {
        assert_eq!(
            restore_index_relative_string(
                &restore_index_relative_components("/work/a", "/work/a/child")
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            "child"
        );
        assert!(restore_index_relative_components("/work/a", "/work/ab")
            .unwrap()
            .is_none());
    }

    #[test]
    fn restore_index_release_cursor_rejects_unknown_stage() {
        assert!(decode_restore_index_release_cursor(&[0]).is_err());
        assert!(decode_restore_index_release_cursor(&[18]).is_err());
        assert_eq!(
            decode_restore_index_release_cursor(&[]).unwrap(),
            (RestoreIndexReleaseStage::Entries, None)
        );
    }

    #[test]
    fn restore_index_mutation_plan_deduplicates_and_rejects_conflicting_cas() {
        let key = b"key".to_vec();
        let mut plan = RestoreIndexMutationPlan::default();
        let version = Version::new(7).unwrap();
        plan.push_predicate(PredicateRef {
            family: RecordFamily::System,
            key: key.clone(),
            predicate: Predicate::VersionEquals(version),
        })
        .unwrap();
        plan.push_predicate(PredicateRef {
            family: RecordFamily::System,
            key: key.clone(),
            predicate: Predicate::VersionEquals(version),
        })
        .unwrap();
        assert_eq!(plan.predicates.len(), 1);
        assert!(plan
            .push_predicate(PredicateRef {
                family: RecordFamily::System,
                key,
                predicate: Predicate::NotExists,
            })
            .is_err());
    }

    #[test]
    fn restore_index_mutation_plan_rejects_item_and_byte_overflow() {
        let mut too_many = RestoreIndexMutationPlan::default();
        for index in 0..=MAX_RESTORE_INDEX_PLAN_ITEMS {
            too_many.predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: (index as u64).to_be_bytes().to_vec(),
                predicate: Predicate::NotExists,
            });
        }
        assert!(matches!(
            too_many.validate_budget("test restore index plan"),
            Err(MetadError::RestoreResourceLimit {
                resource,
                limit,
                actual,
            }) if resource.ends_with(" items")
                && limit == MAX_RESTORE_INDEX_PLAN_ITEMS as u64
                && actual == limit + 1
        ));

        let too_large = RestoreIndexMutationPlan {
            predicates: Vec::new(),
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: b"large-row".to_vec(),
                op: MutationOp::Put,
                value: Some(Value(vec![0; MAX_RESTORE_INDEX_PLAN_BYTES])),
            }],
        };
        assert!(matches!(
            too_large.validate_budget("test restore index plan"),
            Err(MetadError::RestoreResourceLimit {
                resource,
                limit,
                actual,
            }) if resource.ends_with(" bytes")
                && limit == MAX_RESTORE_INDEX_PLAN_BYTES as u64
                && actual > limit
        ));
    }
}
