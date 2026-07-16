//! Filesystem and durable restore consistency checks.
//!
//! The object scan verifies every effective manifest reachable from the live
//! namespace.  Full mode additionally walks unexpired snapshot pins and
//! `ForkBinding` history roots, follows sparse `base_generation` chains, and
//! includes object-backed symlinks.  Restore-private graph validation lives in
//! this module as well so operational diagnostics and the downgrade proof use
//! one definition of consistency.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use super::*;
use crate::layout::inode_prefix;

const RESTORE_FSCK_PAGE_ROWS: usize = 256;
const MAX_RESTORE_FSCK_OPERATIONS: usize = 65_536;
const OBJECT_FSCK_PAGE_ROWS: usize = 256;
// `read_dir_plus_page_at_version_for_purpose` reads one look-ahead row to
// determine whether another page exists. Keep the physical metadata scan at
// or below `OBJECT_FSCK_PAGE_ROWS`.
const OBJECT_FSCK_DIRECTORY_PAGE_ENTRIES: usize = OBJECT_FSCK_PAGE_ROWS - 1;

#[derive(Clone, Copy)]
enum RestoreCursorFormat {
    RawKey,
    ReleaseWorker,
}

/// Scope of an object-reference fsck.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FsckMode {
    /// Current inode records only.
    #[default]
    Live,
    /// Live records plus every unexpired snapshot and durable fork history root.
    Full,
}

/// A metadata block reference whose backing object is missing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DanglingBlock {
    pub inode: u64,
    pub generation: u64,
    pub object_key: String,
}

/// A metadata block reference whose object has a different byte length.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MismatchedBlock {
    pub inode: u64,
    pub generation: u64,
    pub object_key: String,
    pub expected_size: u64,
    pub actual_size: u64,
}

/// The object-reference portion of an fsck scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FsckReport {
    pub inodes_scanned: usize,
    pub files_scanned: usize,
    pub symlinks_scanned: usize,
    pub snapshot_pins_scanned: usize,
    pub fork_bindings_scanned: usize,
    pub historical_bodies_scanned: usize,
    pub blocks_checked: usize,
    pub dangling: Vec<DanglingBlock>,
    pub size_mismatches: Vec<MismatchedBlock>,
}

/// Counts and alert inputs for durable restore operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreMetrics {
    pub read_version: u64,
    pub active_marker: bool,
    pub allocator_v2_fenced: bool,
    pub preparing: usize,
    pub ready_to_attach: usize,
    pub complete: usize,
    pub cleaning: usize,
    pub discarding: usize,
    pub releasing: usize,
    /// Commit-version distance from the oldest non-terminal operation. This is
    /// durable across reopen and is the honest long-running alert input because
    /// v1 operation records do not contain a wall-clock timestamp.
    pub max_preparing_version_age: u64,
    pub max_releasing_version_age: u64,
    pub control_rows: BTreeMap<String, usize>,
    pub staging_rows: usize,
    pub exact_reference_rows: usize,
    pub index_rows: usize,
    pub cleanup_backlog: usize,
    pub release_backlog: usize,
    pub quarantine_rows: usize,
}

impl RestoreMetrics {
    pub fn operation_count(&self) -> usize {
        self.preparing
            .saturating_add(self.ready_to_attach)
            .saturating_add(self.complete)
            .saturating_add(self.cleaning)
            .saturating_add(self.discarding)
            .saturating_add(self.releasing)
    }
}

/// One fail-closed restore graph violation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreFsckIssue {
    pub code: String,
    pub message: String,
    pub operation_id: Option<String>,
    pub ref_set_id: Option<u64>,
}

/// Restore-private consistency report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreFsckReport {
    pub metrics: RestoreMetrics,
    pub borrowed_objects_checked: usize,
    pub dangling_borrowed_objects: Vec<DanglingBlock>,
    pub borrowed_object_size_mismatches: Vec<MismatchedBlock>,
    pub issues: Vec<RestoreFsckIssue>,
}

impl RestoreFsckReport {
    pub fn is_consistent(&self) -> bool {
        self.issues.is_empty()
            && self.dangling_borrowed_objects.is_empty()
            && self.borrowed_object_size_mismatches.is_empty()
    }
}

/// Explicit acknowledgement required by the destructive downgrade API.
///
/// Construct this value only after the deployment has stopped advertising and
/// accepting `restore_to_fork_v1`. The metadata layer cannot infer that
/// external routing state, so it requires the operator to make the dependency
/// visible at the call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreCapabilityDisabled(());

impl RestoreCapabilityDisabled {
    pub fn acknowledged() -> Self {
        Self(())
    }
}

/// Explicit proof supplied by deployment orchestration after every metadata
/// owner capable of running restore initialization PUTs has been stopped or
/// fenced. A process-local lock alone cannot establish this global fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreWritersQuiesced(());

impl RestoreWritersQuiesced {
    pub fn acknowledged() -> Self {
        Self(())
    }
}

#[must_use = "a restore downgrade is not safe until full fsck and a fresh metadata checkpoint complete"]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreDowngradeOutcome {
    pub already_drained: bool,
    pub commit_version: Option<u64>,
    /// A full restore/object-reference fsck is still required after the fence
    /// transaction and before creating the downgrade checkpoint.
    pub full_fsck_required: bool,
    /// A post-drain metadata checkpoint is still required before an older
    /// metadata binary may be started.
    pub metadata_checkpoint_required: bool,
}

#[derive(Debug)]
pub enum RestoreDowngradeError {
    Metadata(MetadError),
    PrivateStatePresent {
        keyspace: String,
        /// Lower bound observed by the bounded preflight scan.
        observed_rows: usize,
    },
    ConcurrentMutation,
}

impl fmt::Display for RestoreDowngradeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(error) => write!(formatter, "restore downgrade metadata error: {error}"),
            Self::PrivateStatePresent {
                keyspace,
                observed_rows,
            } => write!(
                formatter,
                "restore downgrade is blocked by at least {observed_rows} row(s) in {keyspace}"
            ),
            Self::ConcurrentMutation => write!(
                formatter,
                "restore downgrade raced a metadata mutation; run fsck and retry"
            ),
        }
    }
}

impl std::error::Error for RestoreDowngradeError {}

impl From<MetadError> for RestoreDowngradeError {
    fn from(error: MetadError) -> Self {
        Self::Metadata(error)
    }
}

impl FsckReport {
    pub fn is_consistent(&self) -> bool {
        self.dangling.is_empty() && self.size_mismatches.is_empty()
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Preserve the original live-only entry point.
    pub fn fsck_dangling_blocks(&self, limit: usize) -> Result<FsckReport, MetadError> {
        self.fsck_object_references(FsckMode::Live, limit)
    }

    /// Verify object references reachable from metadata.
    ///
    /// `limit == 0` means unlimited.  A non-zero limit bounds the total number
    /// of inode bodies inspected across live and historical roots; it is an
    /// operational sampling control, not a consistency proof.
    pub fn fsck_object_references(
        &self,
        mode: FsckMode,
        limit: usize,
    ) -> Result<FsckReport, MetadError> {
        let epoch = self.path_cache_epoch.load(Ordering::Acquire);
        let report = self.fsck_object_references_at_stable_epoch(mode, limit)?;
        if self.path_cache_epoch.load(Ordering::Acquire) != epoch {
            return Err(MetadError::Codec(
                "object-reference fsck raced a metadata write; retry the scan".to_owned(),
            ));
        }
        Ok(report)
    }

    fn fsck_object_references_at_stable_epoch(
        &self,
        mode: FsckMode,
        limit: usize,
    ) -> Result<FsckReport, MetadError> {
        let version = self.read_version()?;
        let mut report = FsckReport::default();
        let mut visited_bodies = HashSet::new();

        let prefix = inode_prefix(self.mount);
        let mut start_after = None;
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Inode,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: OBJECT_FSCK_PAGE_ROWS,
                purpose: ReadPurpose::RestoreStaging,
            })?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if fsck_limit_reached(limit, &report) {
                    break;
                }
                let attr = decode_inode_attr(&row.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))?;
                report.inodes_scanned = report.inodes_scanned.saturating_add(1);
                self.fsck_body_for_attr(
                    &attr,
                    version,
                    ReadPurpose::RestoreStaging,
                    false,
                    &mut visited_bodies,
                    &mut report,
                )?;
            }
            if fsck_limit_reached(limit, &report) {
                break;
            }
            let reached_tail = rows.len() < OBJECT_FSCK_PAGE_ROWS;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }

        if mode == FsckMode::Full && !fsck_limit_reached(limit, &report) {
            self.fsck_snapshot_roots(version, limit, &mut visited_bodies, &mut report)?;
            self.fsck_fork_binding_roots(version, limit, &mut visited_bodies, &mut report)?;
        }
        Ok(report)
    }

    fn fsck_snapshot_roots(
        &self,
        version: Version,
        limit: usize,
        visited_bodies: &mut HashSet<(u64, u64, u64)>,
        report: &mut FsckReport,
    ) -> Result<(), MetadError> {
        let prefix = snapshot_pin_prefix(self.mount);
        let mut start_after = None;
        let now_ms = self.now_ms();
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Snapshot,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: OBJECT_FSCK_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if fsck_limit_reached(limit, report) {
                    return Ok(());
                }
                let pin = decode_snapshot_pin(&row.value.0)
                    .map_err(|error| MetadError::Codec(error.to_string()))?;
                if row.key != snapshot_pin_key(self.mount, pin.snapshot_id) {
                    return Err(MetadError::Codec(format!(
                        "snapshot pin key does not match snapshot {}",
                        pin.snapshot_id
                    )));
                }
                if now_ms >= pin.lease_expires_unix_ms {
                    continue;
                }
                report.snapshot_pins_scanned += 1;
                self.fsck_namespace_root(
                    pin.root,
                    Version::new(pin.read_version)?,
                    ReadPurpose::Snapshot,
                    limit,
                    visited_bodies,
                    report,
                )?;
                if fsck_limit_reached(limit, report) {
                    return Ok(());
                }
            }
            let reached_tail = rows.len() < OBJECT_FSCK_PAGE_ROWS;
            start_after = rows.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }
        Ok(())
    }

    fn fsck_fork_binding_roots(
        &self,
        version: Version,
        limit: usize,
        visited_bodies: &mut HashSet<(u64, u64, u64)>,
        report: &mut FsckReport,
    ) -> Result<(), MetadError> {
        self.visit_versioned_fork_bindings_at(version, ReadPurpose::WritePlanLocal, |binding| {
            if fsck_limit_reached(limit, report) {
                return Ok(true);
            }
            report.fork_bindings_scanned += 1;
            self.fsck_namespace_root(
                binding.binding.source_root,
                Version::new(binding.binding.pinned_read_version)?,
                ReadPurpose::RestoreStaging,
                limit,
                visited_bodies,
                report,
            )?;
            Ok(fsck_limit_reached(limit, report))
        })?;
        Ok(())
    }

    fn fsck_namespace_root(
        &self,
        root: InodeId,
        version: Version,
        purpose: ReadPurpose,
        limit: usize,
        visited_bodies: &mut HashSet<(u64, u64, u64)>,
        report: &mut FsckReport,
    ) -> Result<(), MetadError> {
        if fsck_limit_reached(limit, report) {
            return Ok(());
        }
        let root_attr = self
            .get_attr_at_version_for_purpose(root, version, purpose)?
            .ok_or_else(|| {
                MetadError::Codec(format!(
                    "retained fsck root {} is absent at version {}",
                    root.get(),
                    version.get()
                ))
            })?;
        report.inodes_scanned = report.inodes_scanned.saturating_add(1);
        self.fsck_body_for_attr(&root_attr, version, purpose, true, visited_bodies, report)?;
        if root_attr.file_type != FileType::Directory || fsck_limit_reached(limit, report) {
            return Ok(());
        }

        let mut pending = vec![root];
        let mut visited_directories = HashSet::new();
        while let Some(parent) = pending.pop() {
            if !visited_directories.insert(parent) {
                continue;
            }
            let mut after = None;
            loop {
                let page = self.read_dir_plus_page_at_version_for_purpose(
                    parent,
                    after.as_ref(),
                    OBJECT_FSCK_DIRECTORY_PAGE_ENTRIES,
                    version,
                    purpose,
                )?;
                for child in page.entries {
                    if fsck_limit_reached(limit, report) {
                        return Ok(());
                    }
                    report.inodes_scanned = report.inodes_scanned.saturating_add(1);
                    self.fsck_body_for_attr(
                        &child.attr,
                        version,
                        purpose,
                        true,
                        visited_bodies,
                        report,
                    )?;
                    if child.attr.file_type == FileType::Directory
                        && child.attr.inode.shard_index() == self.shard_index
                    {
                        pending.push(child.attr.inode);
                    }
                    if fsck_limit_reached(limit, report) {
                        return Ok(());
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                after = Some(next_cursor);
                if fsck_limit_reached(limit, report) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn fsck_body_for_attr(
        &self,
        attr: &InodeAttr,
        version: Version,
        purpose: ReadPurpose,
        historical: bool,
        visited_bodies: &mut HashSet<(u64, u64, u64)>,
        report: &mut FsckReport,
    ) -> Result<(), MetadError> {
        if !matches!(attr.file_type, FileType::File | FileType::Symlink) {
            return Ok(());
        }
        if !visited_bodies.insert((attr.inode.get(), attr.generation, version.get())) {
            return Ok(());
        }
        let body = self
            .body_descriptor_at_version_for_purpose(attr.inode, attr.generation, version, purpose)?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        if body.generation != attr.generation {
            return Err(MetadError::Codec(format!(
                "body generation {} does not match inode {} generation {}",
                body.generation,
                attr.inode.get(),
                attr.generation
            )));
        }
        match attr.file_type {
            FileType::File => report.files_scanned += 1,
            FileType::Symlink => report.symlinks_scanned += 1,
            _ => unreachable!("body-bearing file type checked above"),
        }
        if historical {
            report.historical_bodies_scanned += 1;
        }
        let manifests =
            self.chunk_manifests_for_body_at_version(attr.inode, &body, version, purpose)?;
        for block in manifests
            .iter()
            .flat_map(|manifest| &manifest.slices)
            .flat_map(|slice| &slice.blocks)
        {
            report.blocks_checked += 1;
            let key = ObjectKey::new(block.object_key.clone())?;
            match self.objects.head(&key)? {
                None => report.dangling.push(DanglingBlock {
                    inode: attr.inode.get(),
                    generation: body.generation,
                    object_key: block.object_key.clone(),
                }),
                Some(info) if info.size != block.len => {
                    report.size_mismatches.push(MismatchedBlock {
                        inode: attr.inode.get(),
                        generation: body.generation,
                        object_key: block.object_key.clone(),
                        expected_size: block.len,
                        actual_size: info.size,
                    });
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct VersionedRestoreOperation {
    operation: super::restore::RestoreOperation,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Durable restore gauges.
    ///
    /// The authoritative graph is exact across one restore-graph sequence and
    /// remains O(private rows). The two worker cursors are scheduler hints and
    /// are sampled from their atomic current rows so their round-robin movement
    /// cannot starve metrics. Unlike full fsck, this never materializes an
    /// entire keyspace: every keyspace is counted in bounded pages and only
    /// operation rows are decoded for state/age gauges.
    pub fn restore_metrics(&self) -> Result<RestoreMetrics, MetadError> {
        self.restore_metrics_with_page_size(4_096)
    }

    fn restore_metrics_with_page_size(
        &self,
        page_size: usize,
    ) -> Result<RestoreMetrics, MetadError> {
        if page_size == 0 {
            return Err(MetadError::Codec(
                "restore metrics page size must be positive".to_owned(),
            ));
        }
        self.ensure_metadata_checkpoint_install_stable()?;
        for _ in 0..3 {
            let Some(sequence) = self.restore_graph_read_sequence() else {
                std::thread::yield_now();
                continue;
            };
            let metrics = self.collect_restore_metrics(page_size);
            if self.restore_graph_read_is_stable(sequence) {
                self.ensure_owner_epoch_current()?;
                return metrics;
            }
        }
        Err(MetadError::Codec(
            "restore metrics raced a restore graph write; retry the scan".to_owned(),
        ))
    }

    fn collect_restore_metrics(&self, page_size: usize) -> Result<RestoreMetrics, MetadError> {
        let version = self.read_version()?;
        self.collect_restore_metrics_at(version, page_size, true)
    }

    fn collect_restore_metrics_at(
        &self,
        version: Version,
        page_size: usize,
        strict: bool,
    ) -> Result<RestoreMetrics, MetadError> {
        let mut metrics = RestoreMetrics {
            read_version: version.get(),
            ..RestoreMetrics::default()
        };
        if strict {
            self.read_restore_fence_metrics(version, &mut metrics)?;
        }

        let mut keyspaces = super::restore::restore_control_keyspaces(self.mount);
        keyspaces.extend(super::restore_index::restore_index_private_keyspaces(
            self.mount,
        ));
        for (name, prefix) in keyspaces {
            let scan_version = if matches!(name, "init_upload_tombstone_cursor" | "release_cursor")
            {
                // Cursor rows are exact single-key scheduler hints. Read their
                // atomic current representation independently from the stable
                // ownership graph so a permanent tombstone cannot starve the
                // metrics scan by rotating this value forever.
                Version::new(u64::MAX)?
            } else {
                version
            };
            let mut count = 0_usize;
            let mut start_after = None;
            loop {
                let page = self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after: start_after.clone(),
                    version: scan_version,
                    limit: page_size,
                    purpose: ReadPurpose::WritePlanLocal,
                })?;
                if page.is_empty() {
                    break;
                }
                if strict && name == "operation" {
                    for row in &page {
                        let operation = super::restore::decode_restore_operation(&row.value.0)?;
                        if row.key
                            != super::restore::restore_operation_key(
                                self.mount,
                                &operation.operation_digest,
                            )
                        {
                            return Err(MetadError::Codec(
                                "restore metrics found an operation key/value mismatch".to_owned(),
                            ));
                        }
                        update_restore_operation_metrics(&mut metrics, &operation);
                    }
                }
                count = count.checked_add(page.len()).ok_or_else(|| {
                    MetadError::Codec("restore metrics row count overflow".to_owned())
                })?;
                let reached_tail = page.len() < page_size;
                start_after = page.last().map(|row| row.key.clone());
                if reached_tail {
                    break;
                }
            }
            metrics.control_rows.insert(name.to_owned(), count);
        }
        metrics.staging_rows = restore_metric_count(&metrics, "staging_member")
            .checked_add(restore_metric_count(&metrics, "staging_inode_inverse"))
            .and_then(|value| {
                value.checked_add(restore_metric_count(&metrics, "staging_inverse_owner"))
            })
            .ok_or_else(|| MetadError::Codec("restore staging metric overflow".to_owned()))?;
        metrics.exact_reference_rows = restore_metric_count(&metrics, "base_owner")
            .checked_add(restore_metric_count(&metrics, "base_inverse"))
            .and_then(|value| {
                value.checked_add(restore_metric_count(&metrics, "base_inverse_owner"))
            })
            .ok_or_else(|| MetadError::Codec("restore reference metric overflow".to_owned()))?;
        metrics.cleanup_backlog = restore_metric_count(&metrics, "cleanup_job");
        metrics.release_backlog = restore_metric_count(&metrics, "release_job");
        metrics.quarantine_rows = restore_metric_count(&metrics, "release_quarantine");
        metrics.index_rows = metrics
            .control_rows
            .iter()
            .filter(|(name, _)| name.starts_with("index_"))
            .try_fold(0_usize, |total, (_, count)| total.checked_add(*count))
            .ok_or_else(|| MetadError::Codec("restore index metric overflow".to_owned()))?;
        Ok(metrics)
    }

    fn read_restore_fence_metrics(
        &self,
        version: Version,
        metrics: &mut RestoreMetrics,
    ) -> Result<(), MetadError> {
        let activation = self
            .metadata
            .get(
                RecordFamily::System,
                &super::restore::restore_activation_fence_key(self.mount),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore activation fence is missing".to_owned()))?;
        if activation.0 != [super::restore::RESTORE_FORMAT_VERSION] {
            return Err(MetadError::Codec(
                "restore activation fence has an invalid value".to_owned(),
            ));
        }
        let active = self.metadata.get_versioned(
            RecordFamily::System,
            &super::restore::restore_active_key(self.mount),
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        if active
            .as_ref()
            .is_some_and(|item| item.value.0 != [super::restore::RESTORE_FORMAT_VERSION])
        {
            return Err(MetadError::Codec(
                "restore metrics found an invalid active marker".to_owned(),
            ));
        }
        metrics.active_marker = active.is_some();
        let allocator = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &allocator_key(self.mount),
                // The allocator is one current-only atomic projection. Its
                // ordinary high-water reservations do not change the restore
                // fence, and must not invalidate a long graph scan. Restore
                // activation/downgrade and owner transitions are separately
                // bracketed by the restore graph sequence.
                Version::new(u64::MAX)?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore metrics found no allocator record".to_owned())
            })?;
        let (_, _, epoch, fenced) = decode_allocator_state_with_restore_fence(&allocator.value.0)?;
        if metrics.active_marker != fenced {
            return Err(MetadError::Codec(
                "restore metrics found an allocator/active-marker mismatch".to_owned(),
            ));
        }
        let current_epoch = self.epoch.load(Ordering::Relaxed);
        if epoch != current_epoch {
            return Err(MetadError::StaleOwnerEpoch {
                owner_epoch: epoch,
                required_epoch: current_epoch,
            });
        }
        metrics.allocator_v2_fenced = fenced;
        Ok(())
    }

    /// Validate the complete restore-private control graph.
    ///
    /// `verify_objects` adds object HEADs and effective-manifest borrower
    /// proofs. The structural pass never mutates state and reports independent
    /// breaks together instead of stopping at the first malformed row.
    pub fn fsck_restore_state(
        &self,
        verify_objects: bool,
    ) -> Result<RestoreFsckReport, MetadError> {
        self.ensure_metadata_checkpoint_install_stable()?;
        // System rows are current-only, so the captured version must be paired
        // with the restore-specific writer sequence. Unrelated namespace writes
        // and the two scheduler-only cursors do not invalidate this proof.
        // Object verification additionally holds the physical-reclaim gate so
        // a body cannot disappear between its metadata proof and HEAD.
        let _object_gc_guard = if verify_objects {
            Some(
                self.object_gc_gate
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
            )
        } else {
            None
        };
        for _ in 0..3 {
            let Some(sequence) = self.restore_graph_read_sequence() else {
                std::thread::yield_now();
                continue;
            };
            let report = self.fsck_restore_state_at_version(verify_objects);
            if self.restore_graph_read_is_stable(sequence) {
                self.ensure_owner_epoch_current()?;
                return report;
            }
        }
        Err(MetadError::Codec(
            "restore fsck raced a restore graph write; retry the scan".to_owned(),
        ))
    }

    fn for_each_restore_system_row<F>(
        &self,
        prefix: Vec<u8>,
        version: Version,
        mut visit: F,
    ) -> Result<(), MetadError>
    where
        F: FnMut(&crate::command::ScanItem) -> Result<(), MetadError>,
    {
        let mut start_after = None;
        loop {
            let page = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: RESTORE_FSCK_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if page.is_empty() {
                return Ok(());
            }
            for row in &page {
                visit(row)?;
            }
            let reached_tail = page.len() < RESTORE_FSCK_PAGE_ROWS;
            start_after = page.last().map(|row| row.key.clone());
            if reached_tail {
                return Ok(());
            }
        }
    }

    fn fsck_restore_state_at_version(
        &self,
        verify_objects: bool,
    ) -> Result<RestoreFsckReport, MetadError> {
        let version = self.read_version()?;
        let mut report = RestoreFsckReport {
            metrics: self.collect_restore_metrics_at(version, RESTORE_FSCK_PAGE_ROWS, false)?,
            ..RestoreFsckReport::default()
        };
        let private_rows = report
            .metrics
            .control_rows
            .values()
            .try_fold(0_usize, |total, count| total.checked_add(*count))
            .ok_or_else(|| MetadError::Codec("restore fsck row count overflow".to_owned()))?;
        self.fsck_restore_allocator_fence_streaming(version, private_rows, &mut report)?;

        let operation_prefix = super::restore::restore_control_keyspaces(self.mount)
            .into_iter()
            .find_map(|(name, prefix)| (name == "operation").then_some(prefix))
            .expect("restore operation keyspace is registered");
        let operation_row_prefix = operation_prefix.clone();
        let mut operations = HashMap::<[u8; 32], VersionedRestoreOperation>::new();
        self.for_each_restore_system_row(operation_prefix, version, |row| {
            match super::restore::decode_restore_operation(&row.value.0) {
                Ok(operation) => {
                    update_restore_operation_metrics(&mut report.metrics, &operation);
                    let keyed_digest = row
                        .key
                        .strip_prefix(operation_row_prefix.as_slice())
                        .filter(|suffix| suffix.len() == 32)
                        .and_then(|suffix| <[u8; 32]>::try_from(suffix).ok());
                    match keyed_digest {
                        Some(digest) => {
                            if let Err(error) =
                                super::restore::validate_restore_operation_identity(
                                    self.mount,
                                    &digest,
                                    &operation,
                                )
                            {
                                push_restore_issue(
                                    &mut report,
                                    "operation_identity_mismatch",
                                    format!(
                                        "restore operation is not bound to its durable request identity: {error}"
                                    ),
                                    Some(&operation),
                                );
                            }
                        }
                        None => push_restore_issue(
                            &mut report,
                            "operation_key_mismatch",
                            "restore operation key has an invalid identity",
                            Some(&operation),
                        ),
                    }
                    if operations.len() >= MAX_RESTORE_FSCK_OPERATIONS
                        && !operations.contains_key(&operation.operation_digest)
                    {
                        return Err(MetadError::RestoreResourceLimit {
                            resource: "restore fsck operations".to_owned(),
                            limit: MAX_RESTORE_FSCK_OPERATIONS as u64,
                            actual: operations.len().saturating_add(1) as u64,
                        });
                    }
                    if operations
                        .insert(
                            operation.operation_digest,
                            VersionedRestoreOperation {
                                operation: operation.clone(),
                            },
                        )
                        .is_some()
                    {
                        push_restore_issue(
                            &mut report,
                            "duplicate_operation",
                            "duplicate restore operation digest",
                            Some(&operation),
                        );
                    }
                }
                Err(error) => push_restore_raw_issue(
                    &mut report,
                    "operation_decode_failed",
                    format!("restore operation row cannot be decoded: {error}"),
                ),
            }
            Ok(())
        })?;

        for versioned in operations.values() {
            self.fsck_restore_operation_streaming(versioned, version, verify_objects, &mut report)?;
        }
        self.fsck_restore_index_state(version, &operations, &mut report)?;
        self.fsck_restore_orphan_rows_streaming(version, &operations, verify_objects, &mut report)?;
        Ok(report)
    }

    fn fsck_restore_index_state(
        &self,
        version: Version,
        operations: &HashMap<[u8; 32], VersionedRestoreOperation>,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        // The index module owns both its logical codecs and physical MVCC
        // envelope. Consume its strict inspection result rather than decoding
        // either representation in fsck.
        let inspection = self.inspect_restore_index_state(version)?;
        report.metrics.index_rows = inspection
            .counts
            .values()
            .copied()
            .fold(0_usize, usize::saturating_add);
        for message in inspection.unowned_issues {
            push_restore_raw_issue(report, "index_row_unowned", message);
        }

        let mut operation_by_ref_set =
            HashMap::<u64, Vec<&super::restore::RestoreOperation>>::new();
        for versioned in operations.values() {
            operation_by_ref_set
                .entry(versioned.operation.ref_set_id)
                .or_default()
                .push(&versioned.operation);
        }
        let mut inspected_ref_sets = HashSet::new();
        for (ref_set_id, ref_set) in inspection.ref_sets {
            inspected_ref_sets.insert(ref_set_id);
            let owners = operation_by_ref_set
                .get(&ref_set_id)
                .map_or(&[][..], Vec::as_slice);
            let operation = match owners {
                [operation] => Some(*operation),
                [] => {
                    push_restore_raw_issue(
                        report,
                        "orphan_index_ref_set",
                        format!("restore index ref-set {ref_set_id} has no operation"),
                    );
                    None
                }
                _ => {
                    push_restore_raw_issue(
                        report,
                        "ambiguous_index_ref_set",
                        format!(
                            "restore index ref-set {ref_set_id} is owned by {} operations",
                            owners.len()
                        ),
                    );
                    None
                }
            };
            if let Some(operation) = operation {
                if ref_set.operation_digests.len() != 1
                    || !ref_set
                        .operation_digests
                        .contains(&operation.operation_digest)
                {
                    push_restore_issue(
                        report,
                        "index_operation_identity_mismatch",
                        format!(
                            "restore index ref-set {ref_set_id} is not owned exclusively by its operation"
                        ),
                        Some(operation),
                    );
                }
                for (kind, identity) in [
                    ("seal", ref_set.seal_identity.as_ref()),
                    ("Complete marker", ref_set.complete_identity.as_ref()),
                ] {
                    if identity.is_some_and(|identity| {
                        identity.operation_digest != operation.operation_digest
                            || identity.initialization_digest != operation.initialization_digest
                            || identity.incarnation != operation.created_version
                    }) {
                        push_restore_issue(
                            report,
                            "index_durable_identity_mismatch",
                            format!(
                                "restore index ref-set {ref_set_id} {kind} does not match operation/incarnation"
                            ),
                            Some(operation),
                        );
                    }
                }
            }
            for message in ref_set.closure_issues {
                push_restore_issue(report, "index_closure_mismatch", message, operation);
            }
            let seal_count = ref_set.counts.get("index_seal").copied().unwrap_or(0);
            let complete_count = ref_set.counts.get("index_complete").copied().unwrap_or(0);
            let Some(operation) = operation else {
                continue;
            };
            let expected = match operation.state {
                super::restore::RestoreOperationState::ReadyToAttach => Some((1, 0)),
                super::restore::RestoreOperationState::Complete => Some((1, 1)),
                super::restore::RestoreOperationState::Preparing => {
                    if complete_count == 0 && seal_count <= 1 {
                        None
                    } else {
                        Some((seal_count.min(1), 0))
                    }
                }
                super::restore::RestoreOperationState::Cleaning
                | super::restore::RestoreOperationState::Discarding => {
                    if complete_count == 0 && seal_count <= 1 {
                        None
                    } else {
                        Some((seal_count.min(1), 0))
                    }
                }
                // Release is paged and may already have removed either marker.
                super::restore::RestoreOperationState::Releasing => {
                    if seal_count <= 1 && complete_count <= 1 {
                        None
                    } else {
                        Some((seal_count.min(1), complete_count.min(1)))
                    }
                }
            };
            if let Some((expected_seal, expected_complete)) = expected {
                if seal_count != expected_seal || complete_count != expected_complete {
                    push_restore_issue(
                        report,
                        "index_visibility_state_mismatch",
                        format!(
                            "restore index ref-set {ref_set_id} has seal/Complete counts {seal_count}/{complete_count}, expected {expected_seal}/{expected_complete} for {:?}",
                            operation.state
                        ),
                        Some(operation),
                    );
                }
            }
        }
        for (ref_set_id, owners) in operation_by_ref_set {
            if owners.len() != 1 {
                if !inspected_ref_sets.contains(&ref_set_id) {
                    push_restore_raw_issue(
                        report,
                        "ambiguous_operation_ref_set",
                        format!(
                            "restore ref-set {ref_set_id} is claimed by {} operations",
                            owners.len()
                        ),
                    );
                }
                continue;
            }
            if inspected_ref_sets.contains(&ref_set_id) {
                continue;
            }
            let operation = owners[0];
            if matches!(
                operation.state,
                super::restore::RestoreOperationState::ReadyToAttach
                    | super::restore::RestoreOperationState::Complete
            ) {
                push_restore_issue(
                    report,
                    "index_visibility_state_missing",
                    format!(
                        "restore index ref-set {ref_set_id} has no seal/Complete state for {:?}",
                        operation.state
                    ),
                    Some(operation),
                );
            }
        }
        Ok(())
    }

    /// Remove permanent late-PUT tombstones after deployment has globally
    /// quiesced restore writers. The caller holds `restore_gate` followed by
    /// `object_gc_gate`; `allocator_gate` is deliberately not held because
    /// each CAS needs a normally reserved commit version.
    fn drain_restore_init_tombstones_quiesced_locked(&self) -> Result<(), RestoreDowngradeError> {
        loop {
            let read_version = self.read_version()?;
            let Some(row) = self
                .metadata
                .scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: super::restore::restore_init_upload_tombstone_prefix(self.mount),
                    start_after: None,
                    version: read_version,
                    limit: 1,
                    purpose: ReadPurpose::WritePlanLocal,
                })
                .map_err(MetadError::from)?
                .into_iter()
                .next()
            else {
                return Ok(());
            };
            let tombstone =
                super::restore::validate_restore_init_upload_tombstone_row(self.mount, &row)?;
            self.delete_restore_init_object_range(
                tombstone.inode,
                tombstone.generation,
                tombstone.size,
            )?;
            if !self.restore_init_object_range_absent(
                tombstone.inode,
                tombstone.generation,
                tombstone.size,
            )? {
                return Err(RestoreDowngradeError::Metadata(MetadError::Codec(
                    "restore initialization object reappeared during quiesced drain".to_owned(),
                )));
            }
            let version = self.next_version()?;
            let command = MetadataCommand {
                request_id: request_id(
                    b"restore-drain-init-tombstone",
                    self.mount,
                    tombstone.inode,
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).map_err(MetadError::from)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: row.key.clone(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::System,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                }],
                mutations: vec![delete_mutation(RecordFamily::System, row.key)],
                watch: Vec::new(),
            };
            match self.commit_metadata(command) {
                Ok(_) => {}
                Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                    return Err(RestoreDowngradeError::ConcurrentMutation);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn drain_restore_cursor_locked(
        &self,
        cursor_key: Vec<u8>,
        item_prefix: Vec<u8>,
        keyspace: &str,
        format: RestoreCursorFormat,
    ) -> Result<(), RestoreDowngradeError> {
        let read_version = self.read_version()?;
        let remaining = self
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: item_prefix.clone(),
                start_after: None,
                version: read_version,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })
            .map_err(MetadError::from)?;
        if !remaining.is_empty() {
            return Err(RestoreDowngradeError::PrivateStatePresent {
                keyspace: keyspace.to_owned(),
                observed_rows: remaining.len(),
            });
        }
        let Some(cursor) = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &cursor_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )
            .map_err(MetadError::from)?
        else {
            return Ok(());
        };
        match format {
            RestoreCursorFormat::RawKey => {
                if cursor.value.0.len() > super::restore::MAX_RESTORE_PATH_BYTES
                    || (!cursor.value.0.is_empty() && !cursor.value.0.starts_with(&item_prefix))
                {
                    return Err(RestoreDowngradeError::Metadata(MetadError::Codec(format!(
                        "restore drain cursor for {keyspace} changed identity"
                    ))));
                }
            }
            RestoreCursorFormat::ReleaseWorker => {
                super::restore::decode_restore_release_worker_cursor_at_version(
                    self.mount,
                    &cursor.value.0,
                    read_version,
                )?;
            }
        }
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-drain-worker-cursor",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version).map_err(MetadError::from)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: item_prefix,
                    predicate: Predicate::PrefixEmpty,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: cursor_key.clone(),
                    predicate: Predicate::VersionEquals(cursor.version),
                },
            ],
            mutations: vec![delete_mutation(RecordFamily::System, cursor_key)],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(_) => Ok(()),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                Err(RestoreDowngradeError::ConcurrentMutation)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Explicitly drain all restore-to-fork private state and remove the cold
    /// downgrade fence.
    ///
    /// The deployment must first disable the capability and globally quiesce
    /// every restore writer. This call does not infer or alter either external
    /// condition. On success the caller must run both
    /// [`NoKvFs::fsck_restore_state`] and
    /// [`NoKvFs::fsck_object_references`] in [`FsckMode::Full`], require clean
    /// reports, and create a fresh metadata checkpoint before starting a
    /// pre-restore binary. The typed acknowledgements and returned requirement
    /// flags make every external safety dependency explicit at the call site.
    pub fn drain_restore_to_fork_v1(
        &self,
        _capability_disabled: RestoreCapabilityDisabled,
        _writers_quiesced: RestoreWritersQuiesced,
    ) -> Result<RestoreDowngradeOutcome, RestoreDowngradeError> {
        let _restore_guard = self
            .restore_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _object_gc_guard = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.ensure_owner_epoch_current()?;

        self.drain_restore_init_tombstones_quiesced_locked()?;
        self.drain_restore_cursor_locked(
            super::restore::restore_init_upload_tombstone_cursor_key(self.mount),
            super::restore::restore_init_upload_tombstone_prefix(self.mount),
            "init_upload_tombstone",
            RestoreCursorFormat::RawKey,
        )?;
        self.drain_restore_cursor_locked(
            super::restore::restore_release_cursor_key(self.mount),
            super::restore::restore_release_job_prefix(self.mount),
            "release_job",
            RestoreCursorFormat::ReleaseWorker,
        )?;

        // Allocator reservations may race before this lock. Allocate a version,
        // then reject/retry if its predecessor did not include the exact current
        // allocator row. Never call next_version while holding allocator_gate.
        for _ in 0..8 {
            let commit_version = self.next_version()?;
            let read_version = predecessor(commit_version).map_err(MetadError::from)?;
            let allocator_guard = self
                .allocator_gate
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.ensure_owner_epoch_current()?;
            let max_version = Version::new(u64::MAX).map_err(MetadError::from)?;
            let allocator_key = allocator_key(self.mount);
            let current_allocator = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &allocator_key,
                    max_version,
                    ReadPurpose::WritePlanLocal,
                )
                .map_err(MetadError::from)?
                .ok_or_else(|| {
                    RestoreDowngradeError::Metadata(MetadError::Codec(
                        "allocator is missing during restore downgrade".to_owned(),
                    ))
                })?;
            if current_allocator.version.get() > read_version.get() {
                drop(allocator_guard);
                continue;
            }
            return self.finish_restore_downgrade_locked(
                read_version,
                commit_version,
                allocator_key,
                current_allocator,
            );
        }
        Err(RestoreDowngradeError::ConcurrentMutation)
    }

    fn finish_restore_downgrade_locked(
        &self,
        read_version: Version,
        commit_version: Version,
        allocator_key: Vec<u8>,
        allocator: crate::command::ReadItem,
    ) -> Result<RestoreDowngradeOutcome, RestoreDowngradeError> {
        let active_key = super::restore::restore_active_key(self.mount);
        let active = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &active_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )
            .map_err(MetadError::from)?;
        if active
            .as_ref()
            .is_some_and(|item| item.value.0 != [super::restore::RESTORE_FORMAT_VERSION])
        {
            return Err(RestoreDowngradeError::Metadata(MetadError::Codec(
                "restore active marker has an invalid value".to_owned(),
            )));
        }
        let (last_commit_version, next_inode, allocator_epoch, fenced) =
            decode_allocator_state_with_restore_fence(&allocator.value.0)?;
        if active.is_some() != fenced {
            return Err(RestoreDowngradeError::Metadata(MetadError::Codec(
                "restore active marker and allocator v2 fence disagree".to_owned(),
            )));
        }
        if allocator_epoch != self.epoch.load(Ordering::Relaxed) {
            return Err(RestoreDowngradeError::Metadata(
                MetadError::StaleOwnerEpoch {
                    owner_epoch: allocator_epoch,
                    required_epoch: self.epoch.load(Ordering::Relaxed),
                },
            ));
        }

        let control_keyspaces = super::restore::restore_control_keyspaces(self.mount);
        let index_keyspaces = super::restore_index::restore_index_private_keyspaces(self.mount);
        for (name, prefix) in control_keyspaces.iter().chain(index_keyspaces.iter()) {
            let rows = self
                .metadata
                .scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix: prefix.clone(),
                    start_after: None,
                    version: read_version,
                    limit: 1,
                    purpose: ReadPurpose::WritePlanLocal,
                })
                .map_err(MetadError::from)?;
            if !rows.is_empty() {
                return Err(RestoreDowngradeError::PrivateStatePresent {
                    keyspace: (*name).to_owned(),
                    observed_rows: rows.len(),
                });
            }
        }
        let empty_predicates = restore_downgrade_empty_predicates(self.mount);

        let Some(active) = active else {
            return Ok(RestoreDowngradeOutcome {
                already_drained: true,
                commit_version: None,
                full_fsck_required: true,
                metadata_checkpoint_required: true,
            });
        };
        let claim_key = object_gc_claim_key(self.mount);
        let claim = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )
            .map_err(MetadError::from)?
            .ok_or_else(|| {
                RestoreDowngradeError::Metadata(MetadError::Codec(
                    "object-GC claim is missing during restore downgrade".to_owned(),
                ))
            })?;
        if decode_object_gc_claim(self.mount, &claim.value.0)? != ObjectGcClaim::Open {
            return Err(RestoreDowngradeError::Metadata(MetadError::Codec(
                "object-GC claim is not Open during restore downgrade".to_owned(),
            )));
        }
        let persisted_last_version = last_commit_version
            .max(self.reserved_version.load(Ordering::Relaxed))
            .max(commit_version.get());
        let persisted_next_inode = next_inode.max(self.reserved_next_inode.load(Ordering::Relaxed));
        InodeId::new(persisted_next_inode).map_err(MetadError::from)?;
        let allocator_v1 = encode_allocator_state(
            persisted_last_version,
            persisted_next_inode,
            allocator_epoch,
        );
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: active_key.clone(),
                predicate: Predicate::VersionEquals(active.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: allocator_key.clone(),
                predicate: Predicate::VersionEquals(allocator.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: claim_key.clone(),
                predicate: Predicate::VersionEquals(claim.version),
            },
        ];
        predicates.extend(empty_predicates);
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-downgrade-v1",
                self.mount,
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::ReserveAllocator,
            read_version,
            commit_version,
            primary_family: RecordFamily::System,
            primary_key: allocator_key.clone(),
            predicates,
            mutations: vec![
                Mutation {
                    family: RecordFamily::System,
                    key: allocator_key,
                    op: MutationOp::Put,
                    value: Some(Value(allocator_v1)),
                },
                delete_mutation(RecordFamily::System, active_key),
                Mutation {
                    family: RecordFamily::System,
                    key: claim_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_claim(&ObjectGcClaim::Open)?)),
                },
            ],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(result) => Ok(RestoreDowngradeOutcome {
                already_drained: false,
                commit_version: Some(result.commit_version.get()),
                full_fsck_required: true,
                metadata_checkpoint_required: true,
            }),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                Err(RestoreDowngradeError::ConcurrentMutation)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn fsck_restore_allocator_fence_streaming(
        &self,
        version: Version,
        private_rows: usize,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        match self.metadata.get(
            RecordFamily::System,
            &super::restore::restore_activation_fence_key(self.mount),
            version,
            ReadPurpose::WritePlanLocal,
        )? {
            Some(value) if value.0 == [super::restore::RESTORE_FORMAT_VERSION] => {}
            Some(_) => push_restore_raw_issue(
                report,
                "activation_fence_invalid",
                "restore activation fence has an invalid value",
            ),
            None => push_restore_raw_issue(
                report,
                "activation_fence_missing",
                "restore activation fence is missing",
            ),
        }
        let active = self.metadata.get_versioned(
            RecordFamily::System,
            &super::restore::restore_active_key(self.mount),
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        report.metrics.active_marker = active.is_some();
        if active
            .as_ref()
            .is_some_and(|item| item.value.0 != [super::restore::RESTORE_FORMAT_VERSION])
        {
            push_restore_raw_issue(
                report,
                "active_marker_invalid",
                "restore active marker has an invalid value",
            );
        }
        let allocator = self.metadata.get_versioned(
            RecordFamily::System,
            &allocator_key(self.mount),
            // Sample the atomic current fence projection independently from
            // the captured graph version. System has no history, so an
            // unrelated allocator high-water reservation would otherwise make
            // the prior row appear missing halfway through fsck.
            Version::new(u64::MAX)?,
            ReadPurpose::WritePlanLocal,
        )?;
        match allocator {
            Some(item) => match decode_allocator_state_with_restore_fence(&item.value.0) {
                Ok((_, _, epoch, fenced)) => {
                    report.metrics.allocator_v2_fenced = fenced;
                    if fenced != report.metrics.active_marker {
                        push_restore_raw_issue(
                            report,
                            "allocator_marker_mismatch",
                            "restore active marker and allocator v2 fence disagree",
                        );
                    }
                    if fenced && epoch != self.epoch.load(Ordering::Relaxed) {
                        push_restore_raw_issue(
                            report,
                            "allocator_owner_epoch_mismatch",
                            format!(
                                "restore-fenced allocator epoch {epoch} differs from owner epoch {}",
                                self.epoch.load(Ordering::Relaxed)
                            ),
                        );
                    }
                }
                Err(error) => push_restore_raw_issue(
                    report,
                    "allocator_decode_failed",
                    format!("allocator cannot be decoded: {error}"),
                ),
            },
            None => {
                push_restore_raw_issue(report, "allocator_missing", "allocator record is missing")
            }
        }
        if private_rows != 0
            && !(report.metrics.active_marker && report.metrics.allocator_v2_fenced)
        {
            push_restore_raw_issue(
                report,
                "private_state_without_downgrade_fence",
                format!(
                    "{private_rows} restore-private row(s) exist without both active marker and allocator v2 fence"
                ),
            );
        }
        Ok(())
    }

    fn fsck_restore_operation_streaming(
        &self,
        versioned: &VersionedRestoreOperation,
        read_version: Version,
        verify_objects: bool,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let operation = &versioned.operation;
        self.fsck_restore_claim_and_root(operation, read_version, report)?;
        self.fsck_restore_temporary_binding(operation, read_version, report)?;
        self.fsck_restore_jobs_streaming(operation, read_version, report)?;
        self.fsck_restore_staging_members_streaming(operation, read_version, report)?;
        self.fsck_restore_base_references(operation, read_version, verify_objects, report)?;
        Ok(())
    }

    fn fsck_restore_claim_and_root(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let claim_key =
            super::restore::restore_destination_claim_key(self.mount, &operation.destination_path);
        match self.metadata.get(
            RecordFamily::System,
            &claim_key,
            version,
            ReadPurpose::WritePlanLocal,
        )? {
            Some(value) if value.0 == operation.operation_digest => {}
            Some(_) => push_restore_issue(
                report,
                "destination_claim_mismatch",
                "destination claim names another operation",
                Some(operation),
            ),
            None => push_restore_issue(
                report,
                "destination_claim_missing",
                "restore operation has no destination claim",
                Some(operation),
            ),
        }
        let root_key =
            super::restore::restore_root_index_key(self.mount, operation.destination_root);
        match self.metadata.get(
            RecordFamily::System,
            &root_key,
            version,
            ReadPurpose::WritePlanLocal,
        )? {
            Some(value) if value.0 == operation.operation_digest => {}
            Some(_) => push_restore_issue(
                report,
                "root_index_mismatch",
                "restore root index names another operation",
                Some(operation),
            ),
            None => push_restore_issue(
                report,
                "root_index_missing",
                "restore operation has no root index",
                Some(operation),
            ),
        }
        Ok(())
    }

    fn fsck_restore_temporary_binding(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let key = fork_binding_key(self.mount, operation.destination_root);
        let binding = self.metadata.get_versioned(
            RecordFamily::ForkBinding,
            &key,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        let requires_binding = matches!(
            operation.state,
            super::restore::RestoreOperationState::Preparing
                | super::restore::RestoreOperationState::ReadyToAttach
                | super::restore::RestoreOperationState::Cleaning
                | super::restore::RestoreOperationState::Discarding
        );
        match (requires_binding, binding) {
            (true, None) => push_restore_issue(
                report,
                "temporary_binding_missing",
                "detached restore operation has no temporary ForkBinding",
                Some(operation),
            ),
            (false, Some(_)) => push_restore_issue(
                report,
                "temporary_binding_after_attach",
                "attached/releasing restore still has a temporary ForkBinding",
                Some(operation),
            ),
            (true, Some(item)) => match crate::layout::decode_fork_binding(&item.value.0) {
                Ok(binding)
                    if binding.fork_root == operation.destination_root
                        && binding.source_root == operation.source_root
                        && binding.snapshot_id == operation.snapshot_id
                        && binding.pinned_read_version == operation.read_version
                        && binding.created_version == operation.created_version
                        && item.version.get() == operation.created_version => {}
                Ok(_) => push_restore_issue(
                    report,
                    "temporary_binding_mismatch",
                    "temporary ForkBinding identity does not match operation",
                    Some(operation),
                ),
                Err(error) => push_restore_issue(
                    report,
                    "temporary_binding_decode_failed",
                    format!("temporary ForkBinding cannot be decoded: {error}"),
                    Some(operation),
                ),
            },
            (false, None) => {}
        }
        Ok(())
    }

    fn fsck_restore_jobs_streaming(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let cleanup = self
            .metadata
            .get(
                RecordFamily::System,
                &super::restore::restore_cleanup_job_key(self.mount, operation.ref_set_id),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .map(|value| super::restore::decode_restore_cleanup_job(&value.0))
            .transpose()?;
        let expects_cleanup = matches!(
            operation.state,
            super::restore::RestoreOperationState::Cleaning
                | super::restore::RestoreOperationState::Discarding
        );
        if expects_cleanup != cleanup.is_some() {
            push_restore_issue(
                report,
                if expects_cleanup {
                    "cleanup_job_missing"
                } else {
                    "unexpected_cleanup_job"
                },
                "restore cleanup job does not match operation state",
                Some(operation),
            );
        } else if cleanup.is_some_and(|job| job.operation_digest != operation.operation_digest) {
            push_restore_issue(
                report,
                "cleanup_job_owner_mismatch",
                "restore cleanup job has the wrong operation owner",
                Some(operation),
            );
        }

        let release = self
            .metadata
            .get(
                RecordFamily::System,
                &super::restore::restore_release_job_key(self.mount, operation.ref_set_id),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .map(|value| super::restore::decode_restore_release_job(&value.0))
            .transpose()?;
        let expects_release = operation.state == super::restore::RestoreOperationState::Releasing;
        if expects_release != release.is_some() {
            push_restore_issue(
                report,
                if expects_release {
                    "release_job_missing"
                } else {
                    "unexpected_release_job"
                },
                "restore release job does not match operation state",
                Some(operation),
            );
        } else if release.is_some_and(|job| job.operation_digest != operation.operation_digest) {
            push_restore_issue(
                report,
                "release_job_owner_mismatch",
                "restore release job has the wrong operation owner",
                Some(operation),
            );
        }
        Ok(())
    }

    fn fsck_restore_staging_members_streaming(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let release = self
            .metadata
            .get(
                RecordFamily::System,
                &super::restore::restore_release_job_key(self.mount, operation.ref_set_id),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .map(|value| super::restore::decode_restore_release_job(&value.0))
            .transpose()?;
        let prefix =
            super::restore::restore_staging_member_prefix(self.mount, operation.ref_set_id);
        self.for_each_restore_system_row(prefix, version, |row| {
            match super::restore::decode_restore_staging_member(&row.value.0) {
                Ok(member) => {
                    let expected = super::restore::restore_staging_member_key(
                        self.mount,
                        operation.ref_set_id,
                        member.destination_inode,
                    );
                    if row.key != expected || member.operation_digest != operation.operation_digest
                    {
                        push_restore_issue(
                            report,
                            "staging_member_identity_mismatch",
                            "restore staging member does not match key/operation",
                            Some(operation),
                        );
                        return Ok(());
                    }
                    for (code, message, key) in [
                        (
                            "staging_inverse_missing_or_mismatched",
                            "restore staging member has no matching inode inverse",
                            super::restore::restore_staging_inode_key(
                                self.mount,
                                member.destination_inode,
                            ),
                        ),
                        (
                            "staging_inverse_owner_missing_or_mismatched",
                            "restore staging member has no matching ref-set-first inverse owner",
                            super::restore::restore_staging_inverse_owner_key(
                                self.mount,
                                operation.ref_set_id,
                                member.destination_inode,
                            ),
                        ),
                    ] {
                        let matches = self
                            .metadata
                            .get(
                                RecordFamily::System,
                                &key,
                                version,
                                ReadPurpose::WritePlanLocal,
                            )?
                            .and_then(|value| {
                                super::restore::decode_restore_staging_inverse(&value.0).ok()
                            })
                            .is_some_and(|(digest, ref_set)| {
                                digest == operation.operation_digest
                                    && ref_set == operation.ref_set_id
                            });
                        if !matches {
                            push_restore_issue(report, code, message, Some(operation));
                        }
                    }
                    if !member.manifest_cursor.is_empty() {
                        if operation.state
                            != super::restore::RestoreOperationState::Releasing
                            || !matches!(
                                release.as_ref(),
                                Some(job)
                                    if job.operation_digest == operation.operation_digest
                                        && job.ref_set_id == operation.ref_set_id
                                        && job.phase
                                            == super::restore::RestoreReleasePhase::Members
                            )
                        {
                            push_restore_issue(
                                report,
                                "staging_manifest_cursor_state_mismatch",
                                "restore manifest cursor exists outside the member-release phase",
                                Some(operation),
                            );
                        }
                        let manifest_prefix = inode_key(self.mount, member.destination_inode);
                        if member.manifest_cursor.len() != manifest_prefix.len() + 16
                            || !member.manifest_cursor.starts_with(&manifest_prefix)
                        {
                            push_restore_issue(
                                report,
                                "staging_manifest_cursor_shape_invalid",
                                "restore manifest cursor is not a physical member manifest key",
                                Some(operation),
                            );
                            return Ok(());
                        }
                        let generation = u64::from_be_bytes(
                            member.manifest_cursor[manifest_prefix.len()
                                ..manifest_prefix.len() + 8]
                                .try_into()
                                .expect("validated generation width"),
                        );
                        let chunk_index = u64::from_be_bytes(
                            member.manifest_cursor[manifest_prefix.len() + 8..]
                                .try_into()
                                .expect("validated chunk-index width"),
                        );
                        if generation == 0 || chunk_index == BODY_SUMMARY_CHUNK_INDEX {
                            push_restore_issue(
                                report,
                                "staging_manifest_cursor_identity_invalid",
                                "restore manifest cursor has an invalid generation/chunk identity",
                                Some(operation),
                            );
                            return Ok(());
                        }
                        let Some(manifest) = self.metadata.get(
                            RecordFamily::ChunkManifest,
                            &member.manifest_cursor,
                            version,
                            ReadPurpose::RestoreStaging,
                        )?
                        else {
                            push_restore_issue(
                                report,
                                "staging_manifest_cursor_missing",
                                "restore manifest cursor points to a missing manifest",
                                Some(operation),
                            );
                            return Ok(());
                        };
                        let manifest = match decode_chunk_manifest(&manifest.0) {
                            Ok(manifest) => manifest,
                            Err(error) => {
                                push_restore_issue(
                                    report,
                                    "staging_manifest_cursor_decode_failed",
                                    format!(
                                        "restore manifest cursor cannot decode its manifest: {error}"
                                    ),
                                    Some(operation),
                                );
                                return Ok(());
                            }
                        };
                        match self.restore_owned_manifest_blocks(
                            member.destination_inode,
                            chunk_index,
                            &manifest,
                        ) {
                            Ok(blocks)
                                if usize::try_from(member.manifest_block_cursor)
                                    .is_ok_and(|cursor| cursor > 0 && cursor < blocks.len()) => {}
                            Ok(_) => push_restore_issue(
                                report,
                                "staging_manifest_block_cursor_out_of_range",
                                "restore manifest block cursor is not a strict partial-page ordinal",
                                Some(operation),
                            ),
                            Err(error) => push_restore_issue(
                                report,
                                "staging_manifest_cursor_manifest_invalid",
                                format!(
                                    "restore manifest cursor has invalid release blocks: {error}"
                                ),
                                Some(operation),
                            ),
                        }
                    }
                }
                Err(error) => push_restore_issue(
                    report,
                    "staging_member_decode_failed",
                    format!("restore staging member cannot be decoded: {error}"),
                    Some(operation),
                ),
            }
            Ok(())
        })
    }

    fn fsck_restore_base_references(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        verify_objects: bool,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let prefix = super::restore::restore_base_owner_prefix(self.mount, operation.ref_set_id);
        let mut start_after = None;
        let mut owner_count = 0_u64;
        let mut owner_accumulator = super::restore_gc::initial_restore_base_reference_digest();
        loop {
            let page = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: RESTORE_FSCK_PAGE_ROWS,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let reference = match super::restore_gc::decode_restore_base_reference(&row.value.0)
                {
                    Ok(reference) => reference,
                    Err(error) => {
                        push_restore_issue(
                            report,
                            "base_owner_decode_failed",
                            format!("restore base owner cannot be decoded: {error}"),
                            Some(operation),
                        );
                        continue;
                    }
                };
                let object_digest: [u8; 32] =
                    Sha256::digest(reference.object_key.as_bytes()).into();
                let expected_owner = super::restore::restore_base_owner_key(
                    self.mount,
                    operation.ref_set_id,
                    &object_digest,
                    reference.borrower_inode,
                    reference.borrower_generation,
                );
                if row.key != expected_owner
                    || reference.operation_digest != operation.operation_digest
                    || reference.ref_set_id != operation.ref_set_id
                {
                    push_restore_issue(
                        report,
                        "base_owner_identity_mismatch",
                        "restore base owner does not match key/operation",
                        Some(operation),
                    );
                    continue;
                }
                owner_count = owner_count.checked_add(1).ok_or_else(|| {
                    MetadError::Codec("restore fsck base-owner count overflow".to_owned())
                })?;
                owner_accumulator = super::restore_gc::extend_restore_base_reference_digest(
                    owner_accumulator,
                    &reference,
                )?;
                self.fsck_restore_base_reference_inverses(
                    operation,
                    version,
                    &reference,
                    &object_digest,
                    report,
                )?;
                if verify_objects {
                    report.borrowed_objects_checked =
                        report.borrowed_objects_checked.saturating_add(1);
                    let key = ObjectKey::new(reference.object_key.clone())?;
                    match self.objects.head(&key)? {
                        None => report.dangling_borrowed_objects.push(DanglingBlock {
                            inode: reference.borrower_inode.get(),
                            generation: reference.borrower_generation,
                            object_key: reference.object_key.clone(),
                        }),
                        Some(info) if info.size != reference.size => {
                            report
                                .borrowed_object_size_mismatches
                                .push(MismatchedBlock {
                                    inode: reference.borrower_inode.get(),
                                    generation: reference.borrower_generation,
                                    object_key: reference.object_key.clone(),
                                    expected_size: reference.size,
                                    actual_size: info.size,
                                });
                        }
                        Some(_) => {}
                    }
                }
            }
            let reached_tail = page.len() < RESTORE_FSCK_PAGE_ROWS;
            start_after = page.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }

        let seal_key = super::restore::restore_base_seal_key(self.mount, operation.ref_set_id);
        let seal = self.metadata.get(
            RecordFamily::System,
            &seal_key,
            version,
            ReadPurpose::WritePlanLocal,
        )?;
        let requires_seal = matches!(
            operation.state,
            super::restore::RestoreOperationState::ReadyToAttach
                | super::restore::RestoreOperationState::Complete
                | super::restore::RestoreOperationState::Releasing
        );
        match seal {
            None if requires_seal => push_restore_issue(
                report,
                "base_seal_missing",
                "sealed/attached restore has no base-reference seal",
                Some(operation),
            ),
            Some(value) => match super::restore_gc::decode_restore_base_seal_record(&value.0) {
                Ok(super::restore_gc::RestoreBaseSealRecord::Building(build)) => {
                    if build.operation_digest != operation.operation_digest
                        || build.initialization_digest != operation.initialization_digest
                        || build.ref_set_id != operation.ref_set_id
                        || build.incarnation != operation.created_version
                    {
                        push_restore_issue(
                            report,
                            "base_build_identity_mismatch",
                            "base-reference build identity does not match operation",
                            Some(operation),
                        );
                    }
                    if build.reference_count != owner_count
                        || build.reference_digest != owner_accumulator
                    {
                        push_restore_issue(
                            report,
                            "base_build_closure_mismatch",
                            "base-reference build progress does not close over durable owner rows",
                            Some(operation),
                        );
                    }
                    if requires_seal {
                        push_restore_issue(
                            report,
                            "base_seal_incomplete",
                            "attached/releasing restore still has a base-reference build cursor",
                            Some(operation),
                        );
                    }
                }
                Ok(super::restore_gc::RestoreBaseSealRecord::Sealed(seal)) => {
                    if seal.operation_digest != operation.operation_digest
                        || seal.initialization_digest != operation.initialization_digest
                        || seal.ref_set_id != operation.ref_set_id
                        || seal.incarnation != operation.created_version
                    {
                        push_restore_issue(
                            report,
                            "base_seal_identity_mismatch",
                            "base-reference seal identity does not match operation",
                            Some(operation),
                        );
                    }
                    if matches!(
                        operation.state,
                        super::restore::RestoreOperationState::Preparing
                            | super::restore::RestoreOperationState::ReadyToAttach
                            | super::restore::RestoreOperationState::Complete
                    ) {
                        let owner_digest =
                            super::restore_gc::finalize_restore_base_reference_digest(
                                owner_count,
                                owner_accumulator,
                            );
                        if seal.reference_count != owner_count
                            || seal.reference_digest != owner_digest
                        {
                            push_restore_issue(
                                report,
                                "base_seal_closure_mismatch",
                                "base-reference seal count/digest does not close over owner rows",
                                Some(operation),
                            );
                        }
                    }
                    // Only the detached ReadyToAttach tree is immutable. Once
                    // Complete, ordinary unlink/publish/link operations may
                    // legitimately remove or replace a borrower's current
                    // manifest while its sealed exact-reference rows remain
                    // as conservative lifetime protection until root release.
                    // Re-deriving the original seal from the live namespace at
                    // that point would reject valid post-attach mutations.
                    if operation.state == super::restore::RestoreOperationState::ReadyToAttach {
                        let (expected_count, expected_accumulator) =
                            self.fsck_restore_expected_base_references(operation, version, report)?;
                        let expected_digest =
                            super::restore_gc::finalize_restore_base_reference_digest(
                                expected_count,
                                expected_accumulator,
                            );
                        if seal.reference_count != expected_count
                            || seal.reference_digest != expected_digest
                        {
                            push_restore_issue(
                                report,
                                "base_manifest_closure_mismatch",
                                "base-reference seal does not close over staging member manifests",
                                Some(operation),
                            );
                        }
                    }
                }
                Err(error) => push_restore_issue(
                    report,
                    "base_seal_decode_failed",
                    format!("base-reference seal/progress cannot be decoded: {error}"),
                    Some(operation),
                ),
            },
            None => {}
        }
        Ok(())
    }

    fn fsck_restore_base_reference_inverses(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        reference: &super::restore_gc::RestoreBaseReference,
        object_digest: &[u8; 32],
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let expected = super::restore_gc::RestoreBaseInverse {
            operation_digest: operation.operation_digest,
            ref_set_id: operation.ref_set_id,
            object_digest: *object_digest,
            borrower_inode: reference.borrower_inode,
            borrower_generation: reference.borrower_generation,
        };
        for (code, message, key) in [
            (
                "base_inverse_missing_or_mismatched",
                "restore base owner has no matching inverse",
                super::restore::restore_base_inverse_key(
                    self.mount,
                    object_digest,
                    operation.ref_set_id,
                    reference.borrower_inode,
                    reference.borrower_generation,
                ),
            ),
            (
                "base_inverse_owner_missing_or_mismatched",
                "restore base owner has no matching ref-set-first inverse owner",
                super::restore::restore_base_inverse_owner_key(
                    self.mount,
                    operation.ref_set_id,
                    object_digest,
                    reference.borrower_inode,
                    reference.borrower_generation,
                ),
            ),
        ] {
            let matches = self
                .metadata
                .get(
                    RecordFamily::System,
                    &key,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .and_then(|value| super::restore_gc::decode_restore_base_inverse(&value.0).ok())
                .is_some_and(|inverse| inverse == expected);
            if !matches {
                push_restore_issue(report, code, message, Some(operation));
            }
        }
        Ok(())
    }

    fn fsck_restore_expected_base_references(
        &self,
        operation: &super::restore::RestoreOperation,
        version: Version,
        report: &mut RestoreFsckReport,
    ) -> Result<(u64, [u8; 32]), MetadError> {
        let prefix =
            super::restore::restore_staging_member_prefix(self.mount, operation.ref_set_id);
        let mut start_after = None;
        let mut count = 0_u64;
        let mut accumulator = super::restore_gc::initial_restore_base_reference_digest();
        loop {
            let page = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: RESTORE_FSCK_PAGE_ROWS,
                purpose: ReadPurpose::RestoreStaging,
            })?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let member = match super::restore::decode_restore_staging_member(&row.value.0) {
                    Ok(member)
                        if member.operation_digest == operation.operation_digest
                            && row.key
                                == super::restore::restore_staging_member_key(
                                    self.mount,
                                    operation.ref_set_id,
                                    member.destination_inode,
                                ) =>
                    {
                        member
                    }
                    Ok(_) => {
                        push_restore_issue(
                            report,
                            "staging_member_identity_mismatch",
                            "restore staging member does not match key/operation",
                            Some(operation),
                        );
                        continue;
                    }
                    Err(error) => {
                        push_restore_issue(
                            report,
                            "staging_member_decode_failed",
                            format!("restore staging member cannot be decoded: {error}"),
                            Some(operation),
                        );
                        continue;
                    }
                };
                let Some(layout) = self.restore_member_base_reference_layout(&member, version)?
                else {
                    continue;
                };
                let mut chunk_index = 0_u64;
                loop {
                    for (_, reference) in self.restore_member_base_references_for_chunk(
                        operation,
                        &member,
                        &layout,
                        chunk_index,
                        version,
                    )? {
                        count = count.checked_add(1).ok_or_else(|| {
                            MetadError::Codec(
                                "restore fsck expected-reference count overflow".to_owned(),
                            )
                        })?;
                        accumulator = super::restore_gc::extend_restore_base_reference_digest(
                            accumulator,
                            &reference,
                        )?;
                    }
                    if chunk_index == layout.end_chunk {
                        break;
                    }
                    chunk_index = chunk_index.checked_add(1).ok_or_else(|| {
                        MetadError::Codec(
                            "restore fsck expected-reference chunk overflow".to_owned(),
                        )
                    })?;
                }
            }
            let reached_tail = page.len() < RESTORE_FSCK_PAGE_ROWS;
            start_after = page.last().map(|row| row.key.clone());
            if reached_tail {
                break;
            }
        }
        Ok((count, accumulator))
    }

    fn fsck_restore_orphan_rows_streaming(
        &self,
        version: Version,
        operations: &HashMap<[u8; 32], VersionedRestoreOperation>,
        verify_objects: bool,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        self.fsck_restore_cursor_row_streaming(
            version,
            "init_upload_tombstone_cursor",
            super::restore::restore_init_upload_tombstone_cursor_key(self.mount),
            super::restore::restore_init_upload_tombstone_prefix(self.mount),
            RestoreCursorFormat::RawKey,
            report,
        )?;
        self.fsck_restore_cursor_row_streaming(
            version,
            "release_cursor",
            super::restore::restore_release_cursor_key(self.mount),
            super::restore::restore_release_job_prefix(self.mount),
            RestoreCursorFormat::ReleaseWorker,
            report,
        )?;
        self.fsck_restore_claim_orphans_streaming(version, operations, report)?;
        self.fsck_restore_staging_orphans_streaming(version, operations, report)?;
        self.fsck_restore_base_orphans_streaming(version, operations, report)?;
        self.fsck_restore_auxiliary_orphans_streaming(version, operations, verify_objects, report)
    }

    fn fsck_restore_cursor_row_streaming(
        &self,
        _version: Version,
        name: &str,
        expected_key: Vec<u8>,
        target_prefix: Vec<u8>,
        format: RestoreCursorFormat,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        let current_version = Version::new(u64::MAX)?;
        let mut count = 0_usize;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, name),
            current_version,
            |row| {
                count = count.saturating_add(1);
                let value_valid = match format {
                    RestoreCursorFormat::RawKey => {
                        row.value.0.len() <= super::restore::MAX_RESTORE_PATH_BYTES
                            && (row.value.0.is_empty() || row.value.0.starts_with(&target_prefix))
                    }
                    RestoreCursorFormat::ReleaseWorker => {
                        super::restore::decode_restore_release_worker_cursor_at_version(
                            self.mount,
                            &row.value.0,
                            row.version,
                        )
                        .is_ok()
                    }
                };
                if row.key != expected_key || !value_valid {
                    push_restore_raw_issue(
                        report,
                        "restore_cursor_invalid",
                        format!("restore {name} changed key or target-prefix identity"),
                    );
                }
                Ok(())
            },
        )?;
        if count > 1 {
            push_restore_raw_issue(
                report,
                "restore_cursor_duplicate",
                format!("restore {name} keyspace contains {count} rows"),
            );
        }
        Ok(())
    }

    fn fsck_restore_claim_orphans_streaming(
        &self,
        version: Version,
        operations: &HashMap<[u8; 32], VersionedRestoreOperation>,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "destination_claim"),
            version,
            |row| {
                let Ok(digest) = <[u8; 32]>::try_from(row.value.0.as_slice()) else {
                    push_restore_raw_issue(
                        report,
                        "destination_claim_decode_failed",
                        "destination claim value is not an operation digest",
                    );
                    return Ok(());
                };
                match operations.get(&digest) {
                    Some(versioned)
                        if row.key
                            == super::restore::restore_destination_claim_key(
                                self.mount,
                                &versioned.operation.destination_path,
                            ) => {}
                    Some(versioned) => push_restore_issue(
                        report,
                        "destination_claim_key_mismatch",
                        "destination claim key does not match operation path",
                        Some(&versioned.operation),
                    ),
                    None => push_restore_raw_issue(
                        report,
                        "orphan_destination_claim",
                        "destination claim has no operation",
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "root_index"),
            version,
            |row| {
                let Ok(digest) = <[u8; 32]>::try_from(row.value.0.as_slice()) else {
                    push_restore_raw_issue(
                        report,
                        "root_index_decode_failed",
                        "root index value is not an operation digest",
                    );
                    return Ok(());
                };
                match operations.get(&digest) {
                    Some(versioned)
                        if row.key
                            == super::restore::restore_root_index_key(
                                self.mount,
                                versioned.operation.destination_root,
                            ) => {}
                    Some(versioned) => push_restore_issue(
                        report,
                        "root_index_key_mismatch",
                        "root index key does not match operation destination root",
                        Some(&versioned.operation),
                    ),
                    None => push_restore_raw_issue(
                        report,
                        "orphan_root_index",
                        "root index has no operation",
                    ),
                }
                Ok(())
            },
        )
    }

    fn fsck_restore_staging_orphans_streaming(
        &self,
        version: Version,
        operations: &HashMap<[u8; 32], VersionedRestoreOperation>,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "staging_member"),
            version,
            |row| {
                match super::restore::decode_restore_staging_member(&row.value.0) {
                    Ok(member) => match operations.get(&member.operation_digest) {
                        Some(versioned)
                            if row.key
                                == super::restore::restore_staging_member_key(
                                    self.mount,
                                    versioned.operation.ref_set_id,
                                    member.destination_inode,
                                ) => {}
                        Some(versioned) => push_restore_issue(
                            report,
                            "staging_member_key_mismatch",
                            "staging member key does not match operation ref-set",
                            Some(&versioned.operation),
                        ),
                        None => push_restore_raw_issue(
                            report,
                            "orphan_staging_member",
                            "staging member has no operation",
                        ),
                    },
                    Err(_) => push_restore_raw_issue(
                        report,
                        "staging_member_unowned",
                        "malformed staging member cannot be attributed to an operation",
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "staging_inode_inverse"),
            version,
            |row| {
                let (digest, ref_set_id) =
                    match super::restore::decode_restore_staging_inverse(&row.value.0) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            push_restore_raw_issue(
                                report,
                                "staging_inverse_decode_failed",
                                format!("staging inode inverse cannot be decoded: {error}"),
                            );
                            return Ok(());
                        }
                    };
                let Some(operation) = operations.get(&digest).map(|item| &item.operation) else {
                    push_restore_raw_issue(
                        report,
                        "orphan_staging_inverse",
                        "staging inode inverse has no operation",
                    );
                    return Ok(());
                };
                let Some(inode) = restore_key_tail_inode(&row.key) else {
                    push_restore_issue(
                        report,
                        "orphan_staging_inode_inverse",
                        "staging inode inverse key has no inode identity",
                        Some(operation),
                    );
                    return Ok(());
                };
                let member_key =
                    super::restore::restore_staging_member_key(self.mount, ref_set_id, inode);
                let member_matches = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &member_key,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .and_then(|value| super::restore::decode_restore_staging_member(&value.0).ok())
                    .is_some_and(|member| {
                        member.operation_digest == digest && member.destination_inode == inode
                    });
                if operation.ref_set_id != ref_set_id
                    || row.key != super::restore::restore_staging_inode_key(self.mount, inode)
                    || !member_matches
                {
                    push_restore_issue(
                        report,
                        "orphan_staging_inode_inverse",
                        "staging inode inverse has no matching member owner",
                        Some(operation),
                    );
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "staging_inverse_owner"),
            version,
            |row| {
                let (digest, ref_set_id) =
                    match super::restore::decode_restore_staging_inverse(&row.value.0) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            push_restore_raw_issue(
                                report,
                                "staging_inverse_owner_decode_failed",
                                format!("ref-set-first staging inverse cannot be decoded: {error}"),
                            );
                            return Ok(());
                        }
                    };
                let Some(operation) = operations.get(&digest).map(|item| &item.operation) else {
                    push_restore_raw_issue(
                        report,
                        "unowned_staging_inverse_owner",
                        "ref-set-first staging inverse has no operation",
                    );
                    return Ok(());
                };
                let Some(inode) = restore_key_tail_inode(&row.key) else {
                    push_restore_issue(
                        report,
                        "orphan_staging_inverse_owner",
                        "ref-set-first staging inverse key has no inode identity",
                        Some(operation),
                    );
                    return Ok(());
                };
                let member_matches = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &super::restore::restore_staging_member_key(self.mount, ref_set_id, inode),
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .and_then(|value| super::restore::decode_restore_staging_member(&value.0).ok())
                    .is_some_and(|member| {
                        member.operation_digest == digest && member.destination_inode == inode
                    });
                let primary_matches = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &super::restore::restore_staging_inode_key(self.mount, inode),
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some_and(|value| value.0 == row.value.0);
                if operation.ref_set_id != ref_set_id
                    || row.key
                        != super::restore::restore_staging_inverse_owner_key(
                            self.mount, ref_set_id, inode,
                        )
                    || !member_matches
                    || !primary_matches
                {
                    push_restore_issue(
                        report,
                        "orphan_staging_inverse_owner",
                        "ref-set-first staging inverse has no matching member/primary inverse",
                        Some(operation),
                    );
                }
                Ok(())
            },
        )
    }

    fn fsck_restore_base_orphans_streaming(
        &self,
        version: Version,
        operations: &HashMap<[u8; 32], VersionedRestoreOperation>,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "base_owner"),
            version,
            |row| {
                match super::restore_gc::decode_restore_base_reference(&row.value.0) {
                    Ok(reference) => match operations.get(&reference.operation_digest) {
                        Some(versioned) => {
                            let object_digest: [u8; 32] =
                                Sha256::digest(reference.object_key.as_bytes()).into();
                            if versioned.operation.ref_set_id != reference.ref_set_id
                                || row.key
                                    != super::restore::restore_base_owner_key(
                                        self.mount,
                                        reference.ref_set_id,
                                        &object_digest,
                                        reference.borrower_inode,
                                        reference.borrower_generation,
                                    )
                            {
                                push_restore_issue(
                                    report,
                                    "base_owner_ref_set_mismatch",
                                    "base owner key/ref-set does not match operation",
                                    Some(&versioned.operation),
                                );
                            }
                        }
                        None => push_restore_raw_issue(
                            report,
                            "orphan_base_owner",
                            "base owner has no operation",
                        ),
                    },
                    Err(_) => push_restore_raw_issue(
                        report,
                        "base_owner_unowned",
                        "malformed base owner cannot be attributed to an operation",
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "base_inverse"),
            version,
            |row| {
                let inverse = match super::restore_gc::decode_restore_base_inverse(&row.value.0) {
                    Ok(inverse) => inverse,
                    Err(error) => {
                        push_restore_raw_issue(
                            report,
                            "base_inverse_decode_failed",
                            format!("base inverse cannot be decoded: {error}"),
                        );
                        return Ok(());
                    }
                };
                let Some(operation) = operations
                    .get(&inverse.operation_digest)
                    .map(|item| &item.operation)
                else {
                    push_restore_raw_issue(
                        report,
                        "orphan_base_inverse",
                        "base inverse has no operation",
                    );
                    return Ok(());
                };
                let owner_key = super::restore::restore_base_owner_key(
                    self.mount,
                    inverse.ref_set_id,
                    &inverse.object_digest,
                    inverse.borrower_inode,
                    inverse.borrower_generation,
                );
                let owner_matches = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &owner_key,
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .and_then(|value| {
                        super::restore_gc::decode_restore_base_reference(&value.0).ok()
                    })
                    .is_some_and(|reference| {
                        reference.operation_digest == inverse.operation_digest
                            && reference.ref_set_id == inverse.ref_set_id
                            && reference.borrower_inode == inverse.borrower_inode
                            && reference.borrower_generation == inverse.borrower_generation
                            && Sha256::digest(reference.object_key.as_bytes())[..]
                                == inverse.object_digest
                    });
                if operation.ref_set_id != inverse.ref_set_id
                    || row.key
                        != super::restore::restore_base_inverse_key(
                            self.mount,
                            &inverse.object_digest,
                            inverse.ref_set_id,
                            inverse.borrower_inode,
                            inverse.borrower_generation,
                        )
                    || !owner_matches
                {
                    push_restore_issue(
                        report,
                        "orphan_or_mismatched_base_inverse",
                        "base inverse has no matching owner/ref-set",
                        Some(operation),
                    );
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "base_inverse_owner"),
            version,
            |row| {
                let inverse = match super::restore_gc::decode_restore_base_inverse(&row.value.0) {
                    Ok(inverse) => inverse,
                    Err(error) => {
                        push_restore_raw_issue(
                            report,
                            "base_inverse_owner_decode_failed",
                            format!("ref-set-first base inverse cannot be decoded: {error}"),
                        );
                        return Ok(());
                    }
                };
                let Some(operation) = operations
                    .get(&inverse.operation_digest)
                    .map(|item| &item.operation)
                else {
                    push_restore_raw_issue(
                        report,
                        "unowned_base_inverse_owner",
                        "ref-set-first base inverse has no operation",
                    );
                    return Ok(());
                };
                let expected_key = super::restore::restore_base_inverse_owner_key(
                    self.mount,
                    inverse.ref_set_id,
                    &inverse.object_digest,
                    inverse.borrower_inode,
                    inverse.borrower_generation,
                );
                let primary_matches = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &super::restore::restore_base_inverse_key(
                            self.mount,
                            &inverse.object_digest,
                            inverse.ref_set_id,
                            inverse.borrower_inode,
                            inverse.borrower_generation,
                        ),
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some_and(|value| value.0 == row.value.0);
                let owner_exists = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &super::restore::restore_base_owner_key(
                            self.mount,
                            inverse.ref_set_id,
                            &inverse.object_digest,
                            inverse.borrower_inode,
                            inverse.borrower_generation,
                        ),
                        version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some();
                if operation.ref_set_id != inverse.ref_set_id
                    || row.key != expected_key
                    || !primary_matches
                    || !owner_exists
                {
                    push_restore_issue(
                        report,
                        "orphan_base_inverse_owner",
                        "ref-set-first base inverse has no matching owner/primary inverse",
                        Some(operation),
                    );
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "base_seal"),
            version,
            |row| {
                let (digest, initialization_digest, ref_set_id, incarnation) =
                    match super::restore_gc::decode_restore_base_seal_record(&row.value.0) {
                        Ok(super::restore_gc::RestoreBaseSealRecord::Building(build)) => (
                            build.operation_digest,
                            build.initialization_digest,
                            build.ref_set_id,
                            build.incarnation,
                        ),
                        Ok(super::restore_gc::RestoreBaseSealRecord::Sealed(seal)) => (
                            seal.operation_digest,
                            seal.initialization_digest,
                            seal.ref_set_id,
                            seal.incarnation,
                        ),
                        Err(error) => {
                            push_restore_raw_issue(
                                report,
                                "base_seal_decode_failed",
                                format!("base seal/progress cannot be decoded: {error}"),
                            );
                            return Ok(());
                        }
                    };
                match operations.get(&digest) {
                    Some(versioned)
                        if versioned.operation.ref_set_id == ref_set_id
                            && versioned.operation.initialization_digest
                                == initialization_digest
                            && versioned.operation.created_version == incarnation
                            && row.key
                                == super::restore::restore_base_seal_key(
                                    self.mount, ref_set_id,
                                ) => {}
                    Some(versioned) => push_restore_issue(
                        report,
                        "base_seal_key_or_owner_mismatch",
                        "base seal/progress does not match key/operation",
                        Some(&versioned.operation),
                    ),
                    None => push_restore_raw_issue(
                        report,
                        "orphan_base_seal",
                        "base seal/progress has no operation",
                    ),
                }
                Ok(())
            },
        )
    }

    fn fsck_restore_auxiliary_orphans_streaming(
        &self,
        version: Version,
        operations: &HashMap<[u8; 32], VersionedRestoreOperation>,
        verify_objects: bool,
        report: &mut RestoreFsckReport,
    ) -> Result<(), MetadError> {
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "init_upload_intent"),
            version,
            |row| {
                match super::restore::decode_restore_init_upload_intent(&row.value.0) {
                    Ok(intent) => match operations.get(&intent.operation_digest) {
                        Some(versioned)
                            if versioned.operation.ref_set_id == intent.ref_set_id
                                && row.key
                                    == super::restore::restore_init_upload_intent_key(
                                        self.mount,
                                        intent.ref_set_id,
                                        intent.inode,
                                        intent.generation,
                                    ) => {}
                        Some(versioned) => push_restore_issue(
                            report,
                            "init_intent_identity_mismatch",
                            "initialization upload intent does not match key/operation",
                            Some(&versioned.operation),
                        ),
                        None => push_restore_raw_issue(
                            report,
                            "orphan_init_intent",
                            "initialization upload intent has no operation",
                        ),
                    },
                    Err(error) => push_restore_raw_issue(
                        report,
                        "init_intent_decode_failed",
                        format!("initialization upload intent cannot be decoded: {error}"),
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "init_upload_tombstone"),
            version,
            |row| {
                match super::restore::validate_restore_init_upload_tombstone_row(self.mount, row) {
                    Ok(tombstone) => {
                        if let Some(versioned) = operations.get(&tombstone.operation_digest) {
                            if versioned.operation.initialization_digest
                                != tombstone.initialization_digest
                            {
                                push_restore_issue(
                                    report,
                                    "init_tombstone_owner_mismatch",
                                    "initialization tombstone digest does not match live operation",
                                    Some(&versioned.operation),
                                );
                            }
                        }
                        if verify_objects
                            && !self.restore_init_object_range_absent(
                                tombstone.inode,
                                tombstone.generation,
                                tombstone.size,
                            )?
                        {
                            push_restore_raw_issue(
                                report,
                                "init_tombstone_object_present",
                                format!(
                                    "initialization tombstone for inode {} generation {} still has an object",
                                    tombstone.inode.get(),
                                    tombstone.generation,
                                ),
                            );
                        }
                    }
                    Err(error) => push_restore_raw_issue(
                        report,
                        "init_tombstone_decode_failed",
                        format!("initialization tombstone cannot be decoded: {error}"),
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "cleanup_job"),
            version,
            |row| {
                match super::restore::decode_restore_cleanup_job(&row.value.0) {
                    Ok(job) => match operations.get(&job.operation_digest) {
                        Some(versioned)
                            if versioned.operation.ref_set_id == job.ref_set_id
                                && row.key
                                    == super::restore::restore_cleanup_job_key(
                                        self.mount,
                                        job.ref_set_id,
                                    ) => {}
                        Some(versioned) => push_restore_issue(
                            report,
                            "cleanup_job_key_or_owner_mismatch",
                            "cleanup job does not match key/operation",
                            Some(&versioned.operation),
                        ),
                        None => push_restore_raw_issue(
                            report,
                            "orphan_cleanup_job",
                            "cleanup job has no operation",
                        ),
                    },
                    Err(error) => push_restore_raw_issue(
                        report,
                        "cleanup_job_decode_failed",
                        format!("cleanup job cannot be decoded: {error}"),
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "release_job"),
            version,
            |row| {
                match super::restore::decode_restore_release_job(&row.value.0) {
                    Ok(job) => match operations.get(&job.operation_digest) {
                        Some(versioned)
                            if versioned.operation.ref_set_id == job.ref_set_id
                                && row.key
                                    == super::restore::restore_release_job_key(
                                        self.mount,
                                        job.ref_set_id,
                                    ) => {}
                        Some(versioned) => push_restore_issue(
                            report,
                            "release_job_key_or_owner_mismatch",
                            "release job does not match key/operation",
                            Some(&versioned.operation),
                        ),
                        None => push_restore_raw_issue(
                            report,
                            "orphan_release_job",
                            "release job has no operation",
                        ),
                    },
                    Err(error) => push_restore_raw_issue(
                        report,
                        "release_job_decode_failed",
                        format!("release job cannot be decoded: {error}"),
                    ),
                }
                Ok(())
            },
        )?;
        self.for_each_restore_system_row(
            restore_control_prefix(self.mount, "release_quarantine"),
            version,
            |row| {
                match super::restore::validate_restore_release_quarantine_row(self.mount, row) {
                    Ok(quarantine) => {
                        let scope = match quarantine.scope {
                            super::restore::RestoreReleaseQuarantineScope::Diagnostic => {
                                "diagnostic".to_owned()
                            }
                            super::restore::RestoreReleaseQuarantineScope::Object(digest) => {
                                format!("object:{}", restore_digest_hex(&digest))
                            }
                            super::restore::RestoreReleaseQuarantineScope::MountWide => {
                                "mount-wide".to_owned()
                            }
                        };
                        push_restore_raw_issue(
                            report,
                            "release_quarantine_present",
                            format!(
                                "restore release quarantined a {:?} row at version {} ({scope}): {}",
                                quarantine.family,
                                quarantine.original_version.get(),
                                quarantine.reason,
                            ),
                        );
                    }
                    Err(error) => push_restore_raw_issue(
                        report,
                        "release_quarantine_decode_failed",
                        format!("restore release quarantine cannot be validated: {error}"),
                    ),
                }
                Ok(())
            },
        )
    }
}

fn restore_control_prefix(mount: MountId, name: &str) -> Vec<u8> {
    super::restore::restore_control_keyspaces(mount)
        .into_iter()
        .find_map(|(candidate, prefix)| (candidate == name).then_some(prefix))
        .unwrap_or_else(|| panic!("restore control keyspace {name} is not registered"))
}

fn restore_key_tail_inode(key: &[u8]) -> Option<InodeId> {
    let raw = u64::from_be_bytes(key.get(key.len().checked_sub(8)?..)?.try_into().ok()?);
    InodeId::new(raw).ok()
}

fn restore_metric_count(metrics: &RestoreMetrics, name: &str) -> usize {
    metrics.control_rows.get(name).copied().unwrap_or(0)
}

fn restore_downgrade_empty_predicates(mount: MountId) -> Vec<PredicateRef> {
    let mut predicates = super::restore::restore_control_keyspaces(mount)
        .into_iter()
        .map(|(_, prefix)| PredicateRef {
            family: RecordFamily::System,
            key: prefix,
            predicate: Predicate::PrefixEmpty,
        })
        .collect::<Vec<_>>();
    predicates.extend(super::restore_index::restore_index_global_empty_predicates(
        mount,
    ));
    predicates
}

fn update_restore_operation_metrics(
    metrics: &mut RestoreMetrics,
    operation: &super::restore::RestoreOperation,
) {
    let version_age = metrics
        .read_version
        .saturating_sub(operation.created_version);
    match operation.state {
        super::restore::RestoreOperationState::Preparing => {
            metrics.preparing += 1;
            metrics.max_preparing_version_age = metrics.max_preparing_version_age.max(version_age);
        }
        super::restore::RestoreOperationState::ReadyToAttach => metrics.ready_to_attach += 1,
        super::restore::RestoreOperationState::Complete => metrics.complete += 1,
        super::restore::RestoreOperationState::Cleaning => {
            metrics.cleaning += 1;
            metrics.max_preparing_version_age = metrics.max_preparing_version_age.max(version_age);
        }
        super::restore::RestoreOperationState::Discarding => {
            metrics.discarding += 1;
            metrics.max_preparing_version_age = metrics.max_preparing_version_age.max(version_age);
        }
        super::restore::RestoreOperationState::Releasing => {
            metrics.releasing += 1;
            metrics.max_releasing_version_age = metrics.max_releasing_version_age.max(version_age);
        }
    }
}

fn push_restore_issue(
    report: &mut RestoreFsckReport,
    code: &str,
    message: impl Into<String>,
    operation: Option<&super::restore::RestoreOperation>,
) {
    report.issues.push(RestoreFsckIssue {
        code: code.to_owned(),
        message: message.into(),
        operation_id: operation.map(|operation| {
            format!(
                "restore-{}",
                restore_digest_hex(&operation.operation_digest)
            )
        }),
        ref_set_id: operation.map(|operation| operation.ref_set_id),
    });
}

fn push_restore_raw_issue(report: &mut RestoreFsckReport, code: &str, message: impl Into<String>) {
    push_restore_issue(report, code, message, None);
}

fn restore_digest_hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn fsck_limit_reached(limit: usize, report: &FsckReport) -> bool {
    limit != 0 && report.files_scanned.saturating_add(report.symlinks_scanned) >= limit
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{ReadItem, ScanItem};
    use crate::holtstore::HoltMetadataStore;
    use nokv_object::MemoryObjectStore;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct FsckScanTrackingStore {
        inner: HoltMetadataStore,
        counts: Arc<FsckScanCounts>,
    }

    #[derive(Default)]
    struct FsckScanCounts {
        unbounded_scans: AtomicUsize,
        largest_scan: AtomicUsize,
    }

    impl FsckScanTrackingStore {
        fn new(inner: HoltMetadataStore) -> Self {
            Self {
                inner,
                counts: Arc::new(FsckScanCounts::default()),
            }
        }

        fn reset(&self) {
            self.counts
                .unbounded_scans
                .store(0, AtomicOrdering::Relaxed);
            self.counts.largest_scan.store(0, AtomicOrdering::Relaxed);
        }

        fn unbounded_scans(&self) -> usize {
            self.counts.unbounded_scans.load(AtomicOrdering::Relaxed)
        }

        fn largest_scan(&self) -> usize {
            self.counts.largest_scan.load(AtomicOrdering::Relaxed)
        }
    }

    impl MetadataStore for FsckScanTrackingStore {
        fn get_versioned(
            &self,
            family: RecordFamily,
            key: &[u8],
            version: Version,
            purpose: ReadPurpose,
        ) -> Result<Option<ReadItem>, MetadataError> {
            self.inner.get_versioned(family, key, version, purpose)
        }

        fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
            if request.limit == 0 {
                self.counts
                    .unbounded_scans
                    .fetch_add(1, AtomicOrdering::Relaxed);
            } else {
                self.counts
                    .largest_scan
                    .fetch_max(request.limit, AtomicOrdering::Relaxed);
            }
            self.inner.scan(request)
        }

        fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
            self.inner.commit_metadata(command)
        }

        fn commit_independent_batch(
            &self,
            commands: &[MetadataCommand],
        ) -> Vec<Result<CommitResult, MetadataError>> {
            self.inner.commit_independent_batch(commands)
        }

        fn committed_request_result(
            &self,
            request_id: &[u8],
        ) -> Result<Option<CommitResult>, MetadataError> {
            self.inner.committed_request_result(request_id)
        }

        fn history_retention_epoch(&self) -> Result<u64, MetadataError> {
            self.inner.history_retention_epoch()
        }

        fn prune_history(
            &self,
            request: HistoryPruneRequest,
        ) -> Result<HistoryPruneOutcome, MetadataError> {
            self.inner.prune_history(request)
        }
    }

    fn service() -> (
        NoKvFs<HoltMetadataStore, MemoryObjectStore>,
        HoltMetadataStore,
        MemoryObjectStore,
    ) {
        let metadata = HoltMetadataStore::open_memory().unwrap();
        let objects = MemoryObjectStore::new();
        let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        (service, metadata, objects)
    }

    fn install_restore_downgrade_fence(service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>) {
        let object_reference = service.begin_object_reference_mutation().unwrap();
        let version = service.next_version().unwrap();
        let read_version = predecessor(version).unwrap();
        let (allocator_predicate, allocator_mutation) =
            service.restore_allocator_fence_plan(read_version).unwrap();
        let active_key = super::super::restore::restore_active_key(service.mount);
        service
            .commit_metadata(MetadataCommand {
                request_id: b"fsck-install-restore-fence".to_vec(),
                kind: CommandKind::CleanupObjects,
                read_version,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: active_key.clone(),
                predicates: vec![
                    object_reference.predicate(service.mount),
                    allocator_predicate,
                    PredicateRef {
                        family: RecordFamily::System,
                        key: active_key.clone(),
                        predicate: Predicate::NotExists,
                    },
                ],
                mutations: vec![
                    allocator_mutation,
                    Mutation {
                        family: RecordFamily::System,
                        key: active_key,
                        op: MutationOp::Put,
                        value: Some(Value(vec![super::super::restore::RESTORE_FORMAT_VERSION])),
                    },
                ],
                watch: Vec::new(),
            })
            .unwrap();
    }

    #[test]
    fn restore_downgrade_cas_covers_every_registered_private_prefix() {
        let mount = MountId::new(1).unwrap();
        let expected = super::super::restore::restore_control_keyspaces(mount)
            .into_iter()
            .chain(super::super::restore_index::restore_index_private_keyspaces(mount))
            .map(|(_, prefix)| prefix)
            .collect::<std::collections::BTreeSet<_>>();
        let predicates = restore_downgrade_empty_predicates(mount);
        let actual = predicates
            .iter()
            .map(|predicate| predicate.key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(predicates.len(), actual.len(), "duplicate CAS prefixes");
        assert!(predicates.iter().all(|predicate| {
            predicate.family == RecordFamily::System
                && predicate.predicate == Predicate::PrefixEmpty
        }));
    }

    #[test]
    fn restore_metrics_strictly_decode_operation_rows_without_running_graph_fsck() {
        let (service, _metadata, _objects) = service();
        let mut operation_key = super::super::restore::restore_control_keyspaces(service.mount)
            .into_iter()
            .find(|(name, _)| *name == "operation")
            .unwrap()
            .1;
        operation_key.extend_from_slice(&[0_u8; 32]);
        let version = service.next_version().unwrap();
        service
            .commit_metadata(MetadataCommand {
                request_id: b"fsck-malformed-restore-operation".to_vec(),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).unwrap(),
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: operation_key.clone(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::System,
                    key: operation_key.clone(),
                    predicate: Predicate::NotExists,
                }],
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: operation_key,
                    op: MutationOp::Put,
                    value: Some(Value(b"malformed-operation".to_vec())),
                }],
                watch: Vec::new(),
            })
            .unwrap();

        assert!(matches!(
            service.restore_metrics_with_page_size(1),
            Err(MetadError::Codec(_))
        ));
        let report = service.fsck_restore_state(false).unwrap();
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "operation_decode_failed"));
    }

    #[test]
    fn full_object_fsck_covers_symlinks_and_snapshot_pinned_generations() {
        let (service, _metadata, objects) = service();
        let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
        let runs_inode = runs.attr.inode;
        let file_name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        let file = service
            .publish_artifact(PublishArtifact {
                parent: runs_inode,
                name: file_name.clone(),
                producer: "fsck-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "fsck/snapshot".to_owned(),
                bytes: b"snapshot-body".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        service
            .create_symlink(
                runs_inode,
                DentryName::new(b"latest".to_vec()).unwrap(),
                b"artifact.bin".to_vec(),
                0o777,
                1000,
                1000,
            )
            .unwrap();
        let _pin = service
            .snapshot_subtree_with_lease(runs_inode, DEFAULT_SNAPSHOT_LEASE_MS)
            .unwrap();
        service.remove_file(runs_inode, &file_name).unwrap();

        let lost = ObjectKey::new(format!(
            "blocks/{}/{}/{}/0/0",
            service.mount.get(),
            file.attr.inode.get(),
            file.attr.generation,
        ))
        .unwrap();
        assert!(objects.delete(&lost).unwrap());

        let live = service.fsck_object_references(FsckMode::Live, 0).unwrap();
        assert!(live.is_consistent(), "live report: {live:?}");
        assert_eq!(live.symlinks_scanned, 1);

        let full = service.fsck_object_references(FsckMode::Full, 0).unwrap();
        assert!(!full.is_consistent());
        assert_eq!(full.snapshot_pins_scanned, 1);
        assert!(full.historical_bodies_scanned >= 1);
        assert!(full.dangling.iter().any(|entry| {
            entry.inode == file.attr.inode.get() && entry.object_key == lost.as_str()
        }));
    }

    #[test]
    fn full_object_fsck_pages_physical_reads_and_limits_bodies_not_inode_rows() {
        let metadata = HoltMetadataStore::open_memory().unwrap();
        let tracking = FsckScanTrackingStore::new(metadata);
        let objects = MemoryObjectStore::new();
        let service = NoKvFs::new(MountId::new(1).unwrap(), tracking.clone(), objects);
        service.bootstrap_root(0o755, 1000, 1000).unwrap();

        let bulk = service
            .create_dir(
                InodeId::root(),
                DentryName::new(b"bulk".to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
        let pinned = service
            .create_dir(
                InodeId::root(),
                DentryName::new(b"pinned".to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
        for index in 0..270 {
            service
                .create_dir(
                    bulk.attr.inode,
                    DentryName::new(format!("directory-{index:03}").into_bytes()).unwrap(),
                    0o755,
                    1000,
                    1000,
                )
                .unwrap();
        }
        service
            .publish_artifact(PublishArtifact {
                parent: bulk.attr.inode,
                name: DentryName::new(b"last-body.bin".to_vec()).unwrap(),
                producer: "bounded-fsck-test".to_owned(),
                digest_uri: "sha256:bounded-fsck".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "fsck/bounded".to_owned(),
                bytes: b"bounded-fsck-body".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        for _ in 0..257 {
            service
                .snapshot_subtree_with_lease(pinned.attr.inode, DEFAULT_SNAPSHOT_LEASE_MS)
                .unwrap();
        }

        tracking.reset();
        let limited = service.fsck_object_references(FsckMode::Full, 1).unwrap();
        assert_eq!(limited.files_scanned + limited.symlinks_scanned, 1);
        assert!(
            limited.inodes_scanned > OBJECT_FSCK_PAGE_ROWS,
            "a body limit must not truncate the live inode scan"
        );
        assert_eq!(limited.snapshot_pins_scanned, 0);
        assert_eq!(tracking.unbounded_scans(), 0);
        assert!(tracking.largest_scan() <= OBJECT_FSCK_PAGE_ROWS);

        tracking.reset();
        let full = service.fsck_object_references(FsckMode::Full, 0).unwrap();
        assert!(full.is_consistent(), "full object fsck report: {full:#?}");
        assert_eq!(full.files_scanned, 1);
        assert_eq!(full.snapshot_pins_scanned, 257);
        assert!(full.inodes_scanned > OBJECT_FSCK_PAGE_ROWS);
        assert_eq!(tracking.unbounded_scans(), 0);
        assert!(tracking.largest_scan() <= OBJECT_FSCK_PAGE_ROWS);
    }

    #[test]
    fn restore_fsck_accepts_a_complete_real_restore_graph() {
        let (service, _metadata, _objects) = service();
        let source = service
            .create_dir_path("/source", 0o755, 1000, 1000)
            .unwrap();
        let artifact_name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
        service
            .publish_artifact(PublishArtifact {
                parent: source.attr.inode,
                name: artifact_name.clone(),
                producer: "fsck-test".to_owned(),
                digest_uri: "sha256:restore-fsck".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "fsck/restore-source".to_owned(),
                bytes: b"durable-restore-body".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let pin = service
            .snapshot_subtree_with_lease(source.attr.inode, DEFAULT_SNAPSHOT_LEASE_MS)
            .unwrap();
        let outcome = service
            .restore_subtree_path_to_fork("/source", pin.snapshot_id, "/restored")
            .unwrap();
        assert_eq!(outcome.state, RestoreState::Complete);

        service
            .remove_file(source.attr.inode, &artifact_name)
            .unwrap();
        service.remove_empty_dir_path("/source").unwrap();
        assert!(service.retire_snapshot(pin.snapshot_id).unwrap());
        for _ in 0..4 {
            service.cleanup_pending_objects(128).unwrap();
        }
        let restored = service
            .lookup_path("/restored/artifact.bin")
            .unwrap()
            .unwrap();
        assert_eq!(
            service.read_file(restored.attr.inode, 0, 64).unwrap(),
            b"durable-restore-body"
        );

        let report = service.fsck_restore_state(true).unwrap();
        assert!(report.is_consistent(), "restore fsck report: {report:#?}");
        assert_eq!(report.metrics.complete, 1);
        assert!(report.metrics.staging_rows > 0);
        assert!(report.metrics.index_rows > 0);
        let metrics = service.restore_metrics().unwrap();
        assert_eq!(metrics.complete, 1);
        assert_eq!(metrics.staging_rows, report.metrics.staging_rows);
        assert_eq!(
            metrics.exact_reference_rows,
            report.metrics.exact_reference_rows
        );
        assert_eq!(metrics.index_rows, report.metrics.index_rows);
        assert!(service
            .fsck_object_references(FsckMode::Full, 0)
            .unwrap()
            .is_consistent());

        service
            .remove_file(outcome.destination_root, &artifact_name)
            .unwrap();
        let mutated = service.fsck_restore_state(true).unwrap();
        assert!(
            mutated.is_consistent(),
            "post-attach mutation restore fsck report: {mutated:#?}"
        );
        assert_eq!(mutated.metrics.complete, 1);
    }

    #[test]
    fn restore_fsck_and_reopen_fail_closed_on_marker_allocator_mismatch() {
        let (service, metadata, objects) = service();
        let version = service.next_version().unwrap();
        service
            .commit_metadata(MetadataCommand {
                request_id: b"fsck-marker-mismatch".to_vec(),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).unwrap(),
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: super::super::restore::restore_active_key(service.mount),
                predicates: vec![PredicateRef {
                    family: RecordFamily::System,
                    key: super::super::restore::restore_active_key(service.mount),
                    predicate: Predicate::NotExists,
                }],
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: super::super::restore::restore_active_key(service.mount),
                    op: MutationOp::Put,
                    value: Some(Value(vec![super::super::restore::RESTORE_FORMAT_VERSION])),
                }],
                watch: Vec::new(),
            })
            .unwrap();

        let report = service.fsck_restore_state(false).unwrap();
        assert!(!report.is_consistent());
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "allocator_marker_mismatch"));
        let drain = service.drain_restore_to_fork_v1(
            RestoreCapabilityDisabled::acknowledged(),
            RestoreWritersQuiesced::acknowledged(),
        );
        assert!(matches!(
            drain,
            Err(RestoreDowngradeError::Metadata(MetadError::Codec(message)))
                if message.contains("marker") && message.contains("fence")
        ));
        assert!(NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects, 0,).is_err());
    }

    #[test]
    fn restore_downgrade_drains_fence_and_survives_checkpoint_reopen() {
        let (service, metadata, objects) = service();
        install_restore_downgrade_fence(&service);
        let claim_key = object_gc_claim_key(service.mount);
        let claim_before = metadata
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                service.read_version().unwrap(),
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();

        let outcome = service
            .drain_restore_to_fork_v1(
                RestoreCapabilityDisabled::acknowledged(),
                RestoreWritersQuiesced::acknowledged(),
            )
            .unwrap();
        assert!(!outcome.already_drained);
        assert!(outcome.commit_version.is_some());
        assert!(outcome.full_fsck_required);
        assert!(outcome.metadata_checkpoint_required);
        let version = service.read_version().unwrap();
        assert!(metadata
            .get(
                RecordFamily::System,
                &super::super::restore::restore_active_key(service.mount),
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_none());
        let allocator = metadata
            .get(
                RecordFamily::System,
                &allocator_key(service.mount),
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();
        assert!(
            !decode_allocator_state_with_restore_fence(&allocator.0)
                .unwrap()
                .3
        );
        let claim_after = metadata
            .get_versioned(
                RecordFamily::System,
                &claim_key,
                version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();
        assert!(claim_after.version.get() > claim_before.version.get());
        assert_eq!(
            decode_object_gc_claim(service.mount, &claim_after.value.0).unwrap(),
            ObjectGcClaim::Open
        );
        assert!(service.fsck_restore_state(false).unwrap().is_consistent());

        metadata.checkpoint().unwrap();
        let reopened =
            NoKvFs::open_existing(MountId::new(1).unwrap(), metadata.clone(), objects, 0).unwrap();
        assert!(reopened.fsck_restore_state(false).unwrap().is_consistent());
        let repeated = reopened
            .drain_restore_to_fork_v1(
                RestoreCapabilityDisabled::acknowledged(),
                RestoreWritersQuiesced::acknowledged(),
            )
            .unwrap();
        assert!(repeated.already_drained);
        assert!(repeated.commit_version.is_none());
        assert!(repeated.full_fsck_required);
        assert!(repeated.metadata_checkpoint_required);
    }

    #[test]
    fn restore_downgrade_refuses_private_index_state_and_keeps_fence() {
        let (service, metadata, _objects) = service();
        install_restore_downgrade_fence(&service);
        let (keyspace, mut private_key) =
            super::super::restore_index::restore_index_private_keyspaces(service.mount)
                .into_iter()
                .next()
                .unwrap();
        private_key.extend_from_slice(b"fsck-blocker-");
        let private_keys = (0_u8..3)
            .map(|suffix| {
                let mut key = private_key.clone();
                key.push(suffix);
                key
            })
            .collect::<Vec<_>>();
        let version = service.next_version().unwrap();
        service
            .commit_metadata(MetadataCommand {
                request_id: b"fsck-private-index-blocker".to_vec(),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version).unwrap(),
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: private_keys[0].clone(),
                predicates: private_keys
                    .iter()
                    .cloned()
                    .map(|key| PredicateRef {
                        family: RecordFamily::System,
                        key,
                        predicate: Predicate::NotExists,
                    })
                    .collect(),
                mutations: private_keys
                    .into_iter()
                    .map(|key| Mutation {
                        family: RecordFamily::System,
                        key,
                        op: MutationOp::Put,
                        value: Some(Value(b"invalid-but-durable".to_vec())),
                    })
                    .collect(),
                watch: Vec::new(),
            })
            .unwrap();

        let metrics = service.restore_metrics_with_page_size(2).unwrap();
        assert_eq!(metrics.index_rows, 3);
        assert_eq!(metrics.control_rows.get(keyspace), Some(&3));
        let report = service.fsck_restore_state(false).unwrap();
        assert!(!report.is_consistent());
        assert!(
            report
                .issues
                .iter()
                .filter(|issue| issue.code == "index_row_unowned")
                .count()
                >= 3
        );

        let result = service.drain_restore_to_fork_v1(
            RestoreCapabilityDisabled::acknowledged(),
            RestoreWritersQuiesced::acknowledged(),
        );
        assert!(matches!(
            result,
            Err(RestoreDowngradeError::PrivateStatePresent {
                keyspace: blocked,
                observed_rows: 1,
            }) if blocked == keyspace
        ));
        let read_version = service.read_version().unwrap();
        assert!(metadata
            .get(
                RecordFamily::System,
                &super::super::restore::restore_active_key(service.mount),
                read_version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .is_some());
        let allocator = metadata
            .get(
                RecordFamily::System,
                &allocator_key(service.mount),
                read_version,
                ReadPurpose::WritePlanLocal,
            )
            .unwrap()
            .unwrap();
        assert!(
            decode_allocator_state_with_restore_fence(&allocator.0)
                .unwrap()
                .3
        );
    }
}
