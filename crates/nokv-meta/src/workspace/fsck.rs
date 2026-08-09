/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Provider-neutral, shard-wide metadata consistency diagnostics.
//!
//! A report is derived from exactly one provider-native consistent read view.
//! Pagination never opens a replacement view. A provider lifetime, row, byte,
//! or finding budget is therefore a qualification boundary, not permission to
//! splice several snapshots into one apparent PASS.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use nokv_types::{
    ArtifactRevisionId, CommitId, CommitRetirePhase, CommitState, CommitVersion, GcClaimState,
    HistoryHoldKind, HistoryHoldState, LogicalShardId, OperationId, OperationKind, OwnerEpoch,
    PlacementGeneration, ReferenceKind, RequestId, RevisionState, RootActivationState, RootId,
    RootLayoutGeneration, RootLayoutProfile, RootPartitionId, SnapshotAliasName, SnapshotId,
    WorkspaceIncarnationId, WorkspaceState, FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::authority::{
    decode_authority_marker, decode_store_identity, workspace_metadata_contract_digest,
    MetadataAuthorityState, MetadataStoreIdentity,
};
use super::build_commit_records::{BuildCommitOperationRecord, CommitRetireOperationRecord};
use super::codec::{
    artifact_manifest_key, artifact_revision_key, build_commit_history_hold_key,
    child_commit_consumer_key, commit_key, commit_member_key, decode_artifact_manifest_key,
    decode_change_event_key, decode_commit_key, decode_commit_member_key, decode_gc_candidate_key,
    decode_operation_key, decode_path_current_key, decode_revision_dependency_ref_key,
    decode_snapshot_ref_key, decode_workspace_current_key, gc_candidate_key,
    gc_history_barrier_key, lease_commit_consumer_key, object_block_key, operation_key,
    path_current_key, path_revision_ref_key, restore_history_hold_key, revision_dependency_ref_key,
    snapshot_alias_key, snapshot_history_hold_key, snapshot_id_claim_key, staged_object_key,
    tag_commit_consumer_key, tag_key, validate_schema_marker, workbench_commit_head_key,
    workbench_head_commit_consumer_key, workspace_current_key, workspace_incarnation_claim_key,
    SYSTEM_SCHEMA_KEY,
};
use super::commit::{advance_commit_parent_rolling_digest, advance_commit_revision_rolling_digest};
use super::commit_records::{
    advance_commit_member_rolling_digest, commit_member_row_digest, CommitConsumerRecord,
    CommitMemberRecord, CommitRecord, TagRecord, WorkbenchCommitHeadRecord,
};
use super::engine::{
    decode_system_digest, decode_system_u64, hash_logical_state_frame, logical_state_space_tag,
    logical_state_spaces, AgentMetadataStore, MetadataFamily, SYSTEM_APPLIED_RECOVERY_LSN_KEY,
    SYSTEM_COMMIT_CLOCK_KEY, SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY, SYSTEM_METADATA_AUTHORITY_KEY,
    SYSTEM_OWNER_FENCE_KEY, SYSTEM_RECOVERY_CHAIN_DIGEST_KEY, SYSTEM_STORE_IDENTITY_KEY,
};
use super::gc_records::{GcHistoryBarrierRecord, GcOperationRecord};
use super::provider::{
    all_ordered_spaces, MetadataReadView, OrderedSpaceId, ProviderCapabilities, ProviderScan,
    ProviderScanItem, ReadScope,
};
use super::publication::dependency_owner_digest;
use super::publication_records::{
    ArtifactRevisionRecord, GcCandidateRecord, PathEntry, RevisionRefRecord,
    WorkspaceIncarnationClaimRecord, WorkspaceRecord,
};
use super::publish_operation_records::{
    ArtifactManifestRow, PublishOperationRecord, StagedObjectRecord,
};
use super::query_records::{
    secondary_index_key, ChangeEventRecord, SecondaryIndexRecord, TypedProjection,
};
use super::records::{CommandDedupeRecord, CurrentValue, HistoryValue, RootFence};
use super::recovery::{
    assemble_recovery_storage, decode_recovery_outbox_key, recovery_chunk_key,
    recovery_genesis_digest, recovery_storage_chunk_count, RecoveryMutationV1,
    RecoveryOutboxRecord, RecoveryResultV1, RecoveryState,
};
use super::restore_records::{RestoreMemberRecord, RestoreOperationRecord, RestoreSource};
use super::snapshot_records::{HistoryHoldRecord, SnapshotAliasRecord, SnapshotRefRecord};

pub const METADATA_FSCK_REPORT_SCHEMA_VERSION: u16 = 1;

const DEFAULT_MAX_ROWS: u64 = 1_000_000;
const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_PAGE_SIZE: usize = 1_024;
const DEFAULT_MAX_DURATION: Duration = Duration::from_secs(4);
const DEFAULT_MAX_FINDINGS: usize = 128;
const HARD_MAX_ROWS: u64 = 2_000_000;
const HARD_MAX_BYTES: u64 = 512 * 1024 * 1024;
const HARD_MAX_DURATION: Duration = Duration::from_secs(60);
const HARD_MAX_FINDINGS: usize = 512;
const FINDING_DETAIL_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataFsckStatus {
    Pass,
    Corrupt,
    NotQualified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetadataFsckFindingKind {
    Corruption,
    QualificationGap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataFsckFinding {
    pub kind: MetadataFsckFindingKind,
    pub code: String,
    pub family: String,
    /// SHA-256 of the provider-neutral logical key. Raw keys are deliberately
    /// absent from the bounded operator report.
    pub key_digest: [u8; SHA256_BYTES],
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataFsckFamilyCoverage {
    pub family: String,
    pub checked_rows: u64,
    pub checked_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataFsckLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub page_size: usize,
    pub max_duration: Duration,
    pub max_findings: usize,
}

impl Default for MetadataFsckLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: DEFAULT_MAX_BYTES,
            page_size: DEFAULT_PAGE_SIZE,
            max_duration: DEFAULT_MAX_DURATION,
            max_findings: DEFAULT_MAX_FINDINGS,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataFsckRequest {
    /// One active route authorizes the shard-wide diagnostic. The report
    /// checks every row in the logical shard, not only this root.
    pub trigger_root_id: RootId,
    pub placement_generation: PlacementGeneration,
    pub owner_epoch: OwnerEpoch,
    pub layout_profile: RootLayoutProfile,
    pub layout_generation: RootLayoutGeneration,
    pub partition_id: RootPartitionId,
    pub limits: MetadataFsckLimits,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataFsckReport {
    pub schema_version: u16,
    pub status: MetadataFsckStatus,
    pub logical_shard_id: LogicalShardId,
    pub checked_commit_version: Option<CommitVersion>,
    pub recovery_frontier: Option<RecoveryState>,
    pub state_digest: Option<[u8; SHA256_BYTES]>,
    pub coverage_digest: [u8; SHA256_BYTES],
    pub report_digest: [u8; SHA256_BYTES],
    pub families: Vec<MetadataFsckFamilyCoverage>,
    pub findings: Vec<MetadataFsckFinding>,
    pub findings_truncated: bool,
}

#[derive(Clone)]
struct RawRow {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct ScanBudget {
    started: Instant,
    deadline: Duration,
    max_rows: u64,
    max_bytes: u64,
    rows: u64,
    bytes: u64,
}

#[derive(Clone, Debug)]
enum ScanFailure {
    Corrupt {
        family: String,
        key: Vec<u8>,
        detail: String,
    },
    NotQualified {
        family: String,
        detail: String,
    },
}

struct ReportBuilder {
    logical_shard_id: LogicalShardId,
    max_findings: usize,
    checked_commit_version: Option<CommitVersion>,
    recovery_frontier: Option<RecoveryState>,
    state_digest: Option<[u8; SHA256_BYTES]>,
    families: Vec<MetadataFsckFamilyCoverage>,
    findings: Vec<MetadataFsckFinding>,
    findings_truncated: bool,
    checks: BTreeSet<&'static str>,
    validation_deadline: Option<(Instant, Duration)>,
    validation_deadline_reported: bool,
}

impl ReportBuilder {
    fn new(logical_shard_id: LogicalShardId, max_findings: usize) -> Self {
        Self {
            logical_shard_id,
            max_findings,
            checked_commit_version: None,
            recovery_frontier: None,
            state_digest: None,
            families: Vec::new(),
            findings: Vec::new(),
            findings_truncated: false,
            checks: BTreeSet::new(),
            validation_deadline: None,
            validation_deadline_reported: false,
        }
    }

    fn check(&mut self, name: &'static str) {
        self.checks.insert(name);
    }

    fn set_validation_deadline(&mut self, started: Instant, deadline: Duration) {
        self.validation_deadline = Some((started, deadline));
    }

    fn time_available(&mut self, family: &str) -> bool {
        let exhausted = self
            .validation_deadline
            .is_some_and(|(started, deadline)| started.elapsed() >= deadline);
        if exhausted && !self.validation_deadline_reported {
            self.validation_deadline_reported = true;
            self.not_qualified(
                "validation_time_budget",
                family,
                &[],
                "record validation exhausted the single-view lifetime",
            );
        }
        !exhausted
    }

    fn corrupt(&mut self, code: &str, family: &str, key: &[u8], detail: impl Into<String>) {
        self.find(
            MetadataFsckFindingKind::Corruption,
            code,
            family,
            key,
            detail,
        );
    }

    fn not_qualified(&mut self, code: &str, family: &str, key: &[u8], detail: impl Into<String>) {
        self.find(
            MetadataFsckFindingKind::QualificationGap,
            code,
            family,
            key,
            detail,
        );
    }

    fn find(
        &mut self,
        kind: MetadataFsckFindingKind,
        code: &str,
        family: &str,
        key: &[u8],
        detail: impl Into<String>,
    ) {
        if self.findings.len() == self.max_findings {
            self.findings_truncated = true;
            return;
        }
        let mut detail = detail.into();
        if detail.len() > FINDING_DETAIL_BYTES {
            let mut boundary = FINDING_DETAIL_BYTES;
            while !detail.is_char_boundary(boundary) {
                boundary -= 1;
            }
            detail.truncate(boundary);
        }
        self.findings.push(MetadataFsckFinding {
            kind,
            code: code.to_owned(),
            family: family.to_owned(),
            key_digest: Sha256::digest(key).into(),
            detail,
        });
    }

    fn finish(mut self) -> MetadataFsckReport {
        self.families
            .sort_by(|left, right| left.family.cmp(&right.family));
        self.findings.sort_by(|left, right| {
            (
                left.kind,
                left.code.as_str(),
                left.family.as_str(),
                left.key_digest,
                left.detail.as_str(),
            )
                .cmp(&(
                    right.kind,
                    right.code.as_str(),
                    right.family.as_str(),
                    right.key_digest,
                    right.detail.as_str(),
                ))
        });
        let status = if self
            .findings
            .iter()
            .any(|finding| finding.kind == MetadataFsckFindingKind::Corruption)
        {
            MetadataFsckStatus::Corrupt
        } else if self.findings_truncated
            || self
                .findings
                .iter()
                .any(|finding| finding.kind == MetadataFsckFindingKind::QualificationGap)
        {
            MetadataFsckStatus::NotQualified
        } else {
            MetadataFsckStatus::Pass
        };
        let coverage_digest = coverage_digest(&self.checks, &self.families);
        let report_digest = report_digest(ReportDigestInput {
            status,
            shard: self.logical_shard_id,
            commit: self.checked_commit_version,
            recovery: self.recovery_frontier,
            state_digest: self.state_digest,
            coverage_digest,
            findings: &self.findings,
            findings_truncated: self.findings_truncated,
        });
        MetadataFsckReport {
            schema_version: METADATA_FSCK_REPORT_SCHEMA_VERSION,
            status,
            logical_shard_id: self.logical_shard_id,
            checked_commit_version: self.checked_commit_version,
            recovery_frontier: self.recovery_frontier,
            state_digest: self.state_digest,
            coverage_digest,
            report_digest,
            families: self.families,
            findings: self.findings,
            findings_truncated: self.findings_truncated,
        }
    }
}

/// Run one fail-closed, logical-shard-wide metadata consistency diagnostic.
pub fn run_metadata_fsck(
    store: &AgentMetadataStore,
    request: MetadataFsckRequest,
) -> MetadataFsckReport {
    let identity = store.metadata_store_identity();
    let mut report = ReportBuilder::new(identity.logical_shard_id, request.limits.max_findings);
    if let Err(detail) = validate_limits(request.limits) {
        report.not_qualified("invalid_limits", "fsck", &[], detail);
        return report.finish();
    }
    let capabilities = store.provider_capabilities();
    let Some(deadline) = effective_deadline(request.limits.max_duration, capabilities) else {
        report.not_qualified(
            "read_view_lifetime_too_short",
            "provider",
            &[],
            "provider read-view lifetime leaves no diagnostic safety margin",
        );
        return report.finish();
    };
    let spaces = all_ordered_spaces();
    let scopes = spaces
        .iter()
        .copied()
        .map(|space| ReadScope {
            space,
            prefix: Vec::new(),
        })
        .collect::<Vec<_>>();
    let read_view = match store.begin_diagnostic_read(&scopes) {
        Ok(read_view) => read_view,
        Err(error) => {
            report.not_qualified(
                "provider_read_view_unavailable",
                "provider",
                &[],
                error.to_string(),
            );
            return report.finish();
        }
    };
    let mut budget = ScanBudget {
        started: Instant::now(),
        deadline,
        max_rows: request.limits.max_rows,
        max_bytes: request.limits.max_bytes,
        rows: 0,
        bytes: 0,
    };
    let mut rows = BTreeMap::new();
    for space in spaces {
        match scan_space(
            read_view.as_ref(),
            space,
            request.limits.page_size,
            capabilities,
            &mut budget,
        ) {
            Ok(space_rows) => {
                report.families.push(MetadataFsckFamilyCoverage {
                    family: space_name(space),
                    checked_rows: space_rows.len() as u64,
                    checked_bytes: space_rows.iter().fold(0_u64, |total, row| {
                        total.saturating_add((row.key.len() + row.value.len()) as u64)
                    }),
                });
                rows.insert(space, space_rows);
            }
            Err(ScanFailure::Corrupt {
                family,
                key,
                detail,
            }) => {
                report.corrupt("provider_scan_contract", &family, &key, detail);
                return report.finish();
            }
            Err(ScanFailure::NotQualified { family, detail }) => {
                report.not_qualified("scan_budget", &family, &[], detail);
                return report.finish();
            }
        }
    }
    report.set_validation_deadline(budget.started, budget.deadline);
    report.state_digest = compute_state_digest(&rows, &mut report);
    if report.state_digest.is_none() {
        return report.finish();
    }
    drop(read_view);
    validate_snapshot(identity, request, &rows, &mut report);
    report.finish()
}

fn validate_limits(limits: MetadataFsckLimits) -> Result<(), String> {
    if limits.max_rows == 0 || limits.max_rows > HARD_MAX_ROWS {
        return Err(format!("max_rows must be within 1..={HARD_MAX_ROWS}"));
    }
    if limits.max_bytes == 0 || limits.max_bytes > HARD_MAX_BYTES {
        return Err(format!("max_bytes must be within 1..={HARD_MAX_BYTES}"));
    }
    if limits.page_size == 0 || limits.page_size > 16_384 {
        return Err("page_size must be within 1..=16384".to_owned());
    }
    if limits.max_duration.is_zero() || limits.max_duration > HARD_MAX_DURATION {
        return Err("max_duration must be within 1ms..=60s".to_owned());
    }
    if limits.max_findings == 0 || limits.max_findings > HARD_MAX_FINDINGS {
        return Err(format!(
            "max_findings must be within 1..={HARD_MAX_FINDINGS}"
        ));
    }
    Ok(())
}

fn effective_deadline(requested: Duration, capabilities: ProviderCapabilities) -> Option<Duration> {
    let provider = capabilities.max_read_view_duration.unwrap_or(requested);
    let bounded = requested.min(provider);
    if capabilities.max_read_view_duration.is_none() {
        return Some(bounded);
    }
    let margin = bounded.div_f32(10.0).max(Duration::from_millis(1));
    bounded.checked_sub(margin)
}

fn scan_space(
    reader: &dyn MetadataReadView,
    space: OrderedSpaceId,
    requested_page_size: usize,
    capabilities: ProviderCapabilities,
    budget: &mut ScanBudget,
) -> Result<Vec<RawRow>, ScanFailure> {
    let page_size = capabilities
        .max_scan_items
        .map_or(requested_page_size, |limit| requested_page_size.min(limit));
    if page_size == 0 {
        return Err(ScanFailure::NotQualified {
            family: space_name(space),
            detail: "provider advertises a zero-item scan limit".to_owned(),
        });
    }
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        require_time(space, budget)?;
        let page = reader
            .scan(&ProviderScan {
                space,
                prefix: Vec::new(),
                start_after: cursor.clone(),
                delimiter: None,
                limit: page_size,
            })
            .map_err(|error| ScanFailure::NotQualified {
                family: space_name(space),
                detail: format!("provider scan failed inside one read view: {error}"),
            })?;
        if page.items.len() > page_size {
            return Err(ScanFailure::Corrupt {
                family: space_name(space),
                key: Vec::new(),
                detail: format!(
                    "provider returned {} rows for page limit {page_size}",
                    page.items.len()
                ),
            });
        }
        let returned = page.items.len();
        for item in page.items {
            let ProviderScanItem::Key { key, value } = item else {
                return Err(ScanFailure::Corrupt {
                    family: space_name(space),
                    key: Vec::new(),
                    detail: "undelimited scan returned a common prefix".to_owned(),
                });
            };
            if cursor.as_ref().is_some_and(|last| key <= *last) {
                return Err(ScanFailure::Corrupt {
                    family: space_name(space),
                    key,
                    detail: "provider scan did not advance strictly after its cursor".to_owned(),
                });
            }
            budget.rows = budget.rows.saturating_add(1);
            budget.bytes = budget
                .bytes
                .saturating_add((key.len() + value.len()) as u64);
            if budget.rows > budget.max_rows || budget.bytes > budget.max_bytes {
                return Err(ScanFailure::NotQualified {
                    family: space_name(space),
                    detail: format!(
                        "shard exceeds fsck budget (rows {}/{}, bytes {}/{})",
                        budget.rows, budget.max_rows, budget.bytes, budget.max_bytes
                    ),
                });
            }
            cursor = Some(key.clone());
            rows.push(RawRow { key, value });
        }
        require_time(space, budget)?;
        if returned < page_size {
            return Ok(rows);
        }
    }
}

fn require_time(space: OrderedSpaceId, budget: &ScanBudget) -> Result<(), ScanFailure> {
    if budget.started.elapsed() >= budget.deadline {
        return Err(ScanFailure::NotQualified {
            family: space_name(space),
            detail: format!(
                "single provider read view exceeded diagnostic lifetime {:?}",
                budget.deadline
            ),
        });
    }
    Ok(())
}

fn space_name(space: OrderedSpaceId) -> String {
    crate::workspace::provider_catalog::diagnostic_name(space)
        .unwrap_or_else(|| format!("ordered_space_{:04x}", space.get()))
}

fn compute_state_digest(
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) -> Option<[u8; SHA256_BYTES]> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.logical-state.v1\0");
    for space in logical_state_spaces() {
        if !report.time_available("state_digest") {
            return None;
        }
        hash_logical_state_frame(&mut hasher, 1, &logical_state_space_tag(space));
        if let Some(space_rows) = rows.get(&space) {
            for row in space_rows {
                if !report.time_available("state_digest") {
                    return None;
                }
                hash_logical_state_frame(&mut hasher, 2, &row.key);
                hash_logical_state_frame(&mut hasher, 3, &row.value);
            }
        }
        hash_logical_state_frame(&mut hasher, 4, &[]);
    }
    Some(hasher.finalize().into())
}

fn coverage_digest(
    checks: &BTreeSet<&'static str>,
    families: &[MetadataFsckFamilyCoverage],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.fsck.coverage.v1\0");
    for check in checks {
        hash_frame(&mut hasher, check.as_bytes());
    }
    for family in families {
        hash_frame(&mut hasher, family.family.as_bytes());
        hasher.update(family.checked_rows.to_be_bytes());
        hasher.update(family.checked_bytes.to_be_bytes());
    }
    hasher.finalize().into()
}

struct ReportDigestInput<'a> {
    status: MetadataFsckStatus,
    shard: LogicalShardId,
    commit: Option<CommitVersion>,
    recovery: Option<RecoveryState>,
    state_digest: Option<[u8; SHA256_BYTES]>,
    coverage_digest: [u8; SHA256_BYTES],
    findings: &'a [MetadataFsckFinding],
    findings_truncated: bool,
}

fn report_digest(input: ReportDigestInput<'_>) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.fsck.report.v1\0");
    hasher.update(METADATA_FSCK_REPORT_SCHEMA_VERSION.to_be_bytes());
    hasher.update([match input.status {
        MetadataFsckStatus::Pass => 1,
        MetadataFsckStatus::Corrupt => 2,
        MetadataFsckStatus::NotQualified => 3,
    }]);
    hasher.update(input.shard.as_bytes());
    hasher.update(input.commit.map_or(0, CommitVersion::get).to_be_bytes());
    match input.recovery {
        None => hasher.update([0]),
        Some(frontier) => {
            hasher.update([1]);
            hasher.update(frontier.applied_recovery_lsn.to_be_bytes());
            hasher.update(frontier.chain_digest);
        }
    }
    hasher.update(input.state_digest.unwrap_or([0; SHA256_BYTES]));
    hasher.update(input.coverage_digest);
    hasher.update([u8::from(input.findings_truncated)]);
    for finding in input.findings {
        hasher.update([match finding.kind {
            MetadataFsckFindingKind::Corruption => 1,
            MetadataFsckFindingKind::QualificationGap => 2,
        }]);
        hash_frame(&mut hasher, finding.code.as_bytes());
        hash_frame(&mut hasher, finding.family.as_bytes());
        hasher.update(finding.key_digest);
        hash_frame(&mut hasher, finding.detail.as_bytes());
    }
    hasher.finalize().into()
}

fn hash_frame(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

// The invariant walker is kept below the snapshot/budget machinery so it can
// never accidentally open another provider view.
fn validate_snapshot(
    identity: MetadataStoreIdentity,
    request: MetadataFsckRequest,
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) {
    validate_system(identity, request, rows, report);
    if !report.time_available("system") {
        return;
    }
    let Some(recovery) = validate_recovery(identity, rows, report) else {
        return;
    };
    if !validate_recovery_projection(identity, request.limits, &recovery, rows, report) {
        return;
    }
    validate_records(identity, request, rows, report);
}

fn rows_for(rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>, space: OrderedSpaceId) -> &[RawRow] {
    rows.get(&space).map(Vec::as_slice).unwrap_or_default()
}

fn system_value<'a>(system: &'a [RawRow], key: &[u8]) -> Option<&'a [u8]> {
    system
        .iter()
        .find(|row| row.key == key)
        .map(|row| row.value.as_slice())
}

fn validate_system(
    identity: MetadataStoreIdentity,
    request: MetadataFsckRequest,
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) {
    report.check("system-codecs-v1");
    report.check("store-identity-exact-v1");
    report.check("active-authority-owner-v1");
    let system = rows_for(rows, crate::workspace::provider_catalog::SYSTEM_SPACE);
    let known = [
        SYSTEM_SCHEMA_KEY,
        SYSTEM_STORE_IDENTITY_KEY,
        SYSTEM_METADATA_AUTHORITY_KEY,
        SYSTEM_OWNER_FENCE_KEY,
        SYSTEM_COMMIT_CLOCK_KEY,
        SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
        SYSTEM_APPLIED_RECOVERY_LSN_KEY,
        SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
    ];
    for row in system {
        if !known.contains(&row.key.as_slice()) {
            report.corrupt(
                "unknown_system_record",
                "system",
                &row.key,
                "system key is not part of format 11",
            );
        }
    }
    let Some(schema) = system_value(system, SYSTEM_SCHEMA_KEY) else {
        report.corrupt(
            "missing_system_record",
            "system",
            SYSTEM_SCHEMA_KEY,
            "schema",
        );
        return;
    };
    if let Err(error) = validate_schema_marker(schema) {
        report.corrupt(
            "schema_marker",
            "system",
            SYSTEM_SCHEMA_KEY,
            error.to_string(),
        );
    }
    match system_value(system, SYSTEM_STORE_IDENTITY_KEY)
        .ok_or("missing")
        .and_then(|value| decode_store_identity(value).map_err(|_| "malformed"))
    {
        Ok(durable)
            if durable == identity
                && durable.contract_digest == workspace_metadata_contract_digest() => {}
        Ok(_) => report.corrupt(
            "store_identity_mismatch",
            "system",
            SYSTEM_STORE_IDENTITY_KEY,
            "durable identity differs from the exact opened identity or contract",
        ),
        Err(detail) => report.corrupt(
            "store_identity_codec",
            "system",
            SYSTEM_STORE_IDENTITY_KEY,
            detail,
        ),
    }
    let authority_marker = match system_value(system, SYSTEM_METADATA_AUTHORITY_KEY)
        .ok_or("missing")
        .and_then(|value| decode_authority_marker(value).map_err(|_| "malformed"))
    {
        Ok(marker)
            if marker.matches_identity(identity)
                && marker.state == MetadataAuthorityState::Active =>
        {
            Some(marker)
        }
        Ok(marker) => {
            report.corrupt(
                "authority_not_exact_active",
                "system",
                SYSTEM_METADATA_AUTHORITY_KEY,
                format!(
                    "authority marker is {:?} or has a foreign binding",
                    marker.state
                ),
            );
            None
        }
        Err(detail) => {
            report.corrupt(
                "authority_codec",
                "system",
                SYSTEM_METADATA_AUTHORITY_KEY,
                detail,
            );
            None
        }
    };
    let owner = decode_required_u64(
        system,
        SYSTEM_OWNER_FENCE_KEY,
        "System(owner_fence)",
        report,
    );
    if owner.is_some_and(|owner| owner != request.owner_epoch.get()) {
        report.corrupt(
            "owner_epoch_mismatch",
            "system",
            SYSTEM_OWNER_FENCE_KEY,
            format!(
                "requested owner epoch {}, durable epoch {}",
                request.owner_epoch.get(),
                owner.unwrap_or_default()
            ),
        );
    }
    if let Some(clock) = decode_required_u64(
        system,
        SYSTEM_COMMIT_CLOCK_KEY,
        "System(commit_clock)",
        report,
    ) {
        match CommitVersion::new(clock) {
            Ok(version) => report.checked_commit_version = Some(version),
            Err(error) => report.corrupt(
                "commit_clock",
                "system",
                SYSTEM_COMMIT_CLOCK_KEY,
                error.to_string(),
            ),
        }
    }
    let _ = decode_required_u64(
        system,
        SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
        "System(lease_clock_high_water)",
        report,
    );
    let lsn = decode_required_u64(
        system,
        SYSTEM_APPLIED_RECOVERY_LSN_KEY,
        "System(applied_recovery_lsn)",
        report,
    );
    let digest = system_value(system, SYSTEM_RECOVERY_CHAIN_DIGEST_KEY).and_then(|value| {
        match decode_system_digest(value, "System(recovery_chain_digest)") {
            Ok(digest) => Some(digest),
            Err(error) => {
                report.corrupt(
                    "recovery_tail_codec",
                    "system",
                    SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
                    error.to_string(),
                );
                None
            }
        }
    });
    if let (Some(applied_recovery_lsn), Some(chain_digest)) = (lsn, digest) {
        report.recovery_frontier = Some(RecoveryState {
            applied_recovery_lsn,
            chain_digest,
        });
        if authority_marker.is_some_and(|marker| marker.write_sequence != applied_recovery_lsn) {
            report.corrupt(
                "authority_write_sequence",
                "system",
                SYSTEM_METADATA_AUTHORITY_KEY,
                "active authority write sequence differs from recovery LSN",
            );
        }
    }
}

fn decode_required_u64(
    system: &[RawRow],
    key: &[u8],
    record: &'static str,
    report: &mut ReportBuilder,
) -> Option<u64> {
    let Some(value) = system_value(system, key) else {
        report.corrupt("missing_system_record", "system", key, record);
        return None;
    };
    match decode_system_u64(value, record) {
        Ok(value) => Some(value),
        Err(error) => {
            report.corrupt("system_u64_codec", "system", key, error.to_string());
            None
        }
    }
}

fn validate_recovery(
    identity: MetadataStoreIdentity,
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) -> Option<BTreeMap<u64, RecoveryOutboxRecord>> {
    report.check("recovery-contiguous-chain-chunks-v1");
    report.check("dedupe-recovery-binding-v1");
    let mut expected_lsn = 1_u64;
    let mut previous = recovery_genesis_digest(identity.logical_shard_id, identity.contract_digest);
    let mut recovery_by_lsn = BTreeMap::new();
    let mut expected_chunk_keys = BTreeSet::new();
    let recovery_rows = rows_for(
        rows,
        crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
    );
    let captured_rows = recovery_rows
        .iter()
        .map(|row| (row.key.as_slice(), row.value.as_slice()))
        .collect::<BTreeMap<_, _>>();
    for row in recovery_rows
        .iter()
        .filter(|row| row.key.first() == Some(&0))
    {
        if !report.time_available("recovery_outbox") {
            return None;
        }
        let key_lsn = match decode_recovery_outbox_key(&row.key) {
            Ok(lsn) => lsn,
            Err(error) => {
                report.corrupt(
                    "recovery_key_codec",
                    "recovery_outbox",
                    &row.key,
                    error.to_string(),
                );
                continue;
            }
        };
        let chunk_count = match recovery_storage_chunk_count(&row.value) {
            Ok(chunk_count) => chunk_count,
            Err(error) => {
                report.corrupt(
                    "recovery_header_codec",
                    "recovery_outbox",
                    &row.key,
                    error.to_string(),
                );
                continue;
            }
        };
        let mut chunks = Vec::with_capacity(chunk_count as usize);
        let mut missing_chunk = false;
        for index in 0..chunk_count {
            if !report.time_available("recovery_outbox") {
                return None;
            }
            let key = recovery_chunk_key(key_lsn, index).to_vec();
            expected_chunk_keys.insert(key.clone());
            match captured_rows.get(key.as_slice()) {
                Some(value) => chunks.push((*value).to_vec()),
                None => missing_chunk = true,
            }
        }
        if missing_chunk {
            continue;
        }
        let record = match assemble_recovery_storage(&row.value, chunks)
            .and_then(|logical| RecoveryOutboxRecord::decode(&logical))
        {
            Ok(record) => record,
            Err(error) => {
                report.corrupt(
                    "recovery_row_codec",
                    "recovery_outbox",
                    &row.key,
                    error.to_string(),
                );
                continue;
            }
        };
        if key_lsn != expected_lsn
            || record.recovery_lsn != expected_lsn
            || record.previous_chain_digest != previous
        {
            report.corrupt(
                "recovery_chain_discontinuity",
                "recovery_outbox",
                &row.key,
                format!("expected LSN {expected_lsn}, found {key_lsn}"),
            );
        }
        previous = record.chain_digest;
        expected_lsn = expected_lsn.saturating_add(1);
        recovery_by_lsn.insert(key_lsn, record);
    }
    for row in recovery_rows {
        if !report.time_available("recovery_outbox") {
            return None;
        }
        match row.key.first() {
            Some(0) => {}
            Some(1) if expected_chunk_keys.remove(&row.key) => {}
            Some(1) => report.corrupt(
                "recovery_orphan_chunk",
                "recovery_outbox",
                &row.key,
                "orphaned, malformed, or duplicate recovery chunk key",
            ),
            _ => report.corrupt(
                "recovery_storage_key_tag",
                "recovery_outbox",
                &row.key,
                "unknown recovery storage key tag",
            ),
        }
    }
    if !expected_chunk_keys.is_empty() {
        report.corrupt(
            "recovery_missing_chunk",
            "recovery_outbox",
            &[],
            "one or more recovery header-declared chunks are missing",
        );
    }
    if let Some(frontier) = report.recovery_frontier {
        if frontier.applied_recovery_lsn != expected_lsn.saturating_sub(1)
            || frontier.chain_digest != previous
        {
            report.corrupt(
                "recovery_tail_mismatch",
                "system",
                SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                "System recovery tail differs from the fully verified outbox",
            );
        }
    }
    let dedupe_rows = rows_for(
        rows,
        crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
    );
    for row in dedupe_rows {
        if !report.time_available("command_dedupe") {
            return None;
        }
        if row.key.len() != FIXED_ID_BYTES * 2 {
            report.corrupt(
                "dedupe_key_codec",
                "command_dedupe",
                &row.key,
                "dedupe key must contain root and request ids",
            );
            continue;
        }
        let root = RootId::from_bytes(row.key[..FIXED_ID_BYTES].try_into().expect("width"));
        let request_id =
            RequestId::from_bytes(row.key[FIXED_ID_BYTES..].try_into().expect("width"));
        let dedupe = match CommandDedupeRecord::decode(&row.value) {
            Ok(dedupe) => dedupe,
            Err(error) => {
                report.corrupt(
                    "dedupe_value_codec",
                    "command_dedupe",
                    &row.key,
                    error.to_string(),
                );
                continue;
            }
        };
        let Some(recovery) = recovery_by_lsn.get(&dedupe.recovery_lsn) else {
            report.corrupt(
                "dedupe_missing_recovery",
                "command_dedupe",
                &row.key,
                format!("recovery LSN {} is absent", dedupe.recovery_lsn),
            );
            continue;
        };
        match (&recovery.mutation, &recovery.result) {
            (
                RecoveryMutationV1::MetadataCommand { command, .. },
                RecoveryResultV1::MetadataCommand {
                    commit_version,
                    deterministic_result,
                },
            ) if command.root_id == root
                && command.request_id == request_id
                && command.command_digest == dedupe.command_digest
                && *commit_version == dedupe.commit_version
                && *deterministic_result == dedupe.deterministic_result => {}
            _ => report.corrupt(
                "dedupe_recovery_mismatch",
                "command_dedupe",
                &row.key,
                "dedupe result is not exactly bound to its recovery command/result",
            ),
        }
    }
    for (lsn, recovery) in &recovery_by_lsn {
        if !report.time_available("recovery_outbox") {
            return None;
        }
        if let RecoveryMutationV1::MetadataCommand { command, .. } = &recovery.mutation {
            let key = [
                command.root_id.as_bytes().as_slice(),
                command.request_id.as_bytes(),
            ]
            .concat();
            if !dedupe_rows.iter().any(|row| row.key == key) {
                report.corrupt(
                    "recovery_missing_dedupe",
                    "recovery_outbox",
                    &lsn.to_be_bytes(),
                    "metadata command recovery row has no exact dedupe row",
                );
            }
        }
    }
    Some(recovery_by_lsn)
}

fn validate_recovery_projection(
    identity: MetadataStoreIdentity,
    limits: MetadataFsckLimits,
    recovery: &BTreeMap<u64, RecoveryOutboxRecord>,
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) -> bool {
    report.check("recovery-authoritative-state-projection-v1");
    let oracle = match AgentMetadataStore::open_memory(identity.logical_shard_id) {
        Ok(oracle) => oracle,
        Err(error) => {
            report.not_qualified(
                "recovery_oracle_unavailable",
                "recovery_outbox",
                &[],
                error.to_string(),
            );
            return false;
        }
    };
    for (lsn, record) in recovery {
        if !report.time_available("recovery_projection") {
            return false;
        }
        if let Err(error) = oracle.replay_recovery_record(record) {
            report.corrupt(
                "recovery_projection_replay",
                "recovery_outbox",
                &lsn.to_be_bytes(),
                error.to_string(),
            );
            return false;
        }
        if !report.time_available("recovery_projection") {
            return false;
        }
    }

    let spaces = all_ordered_spaces();
    let scopes = spaces
        .iter()
        .copied()
        .map(|space| ReadScope {
            space,
            prefix: Vec::new(),
        })
        .collect::<Vec<_>>();
    let read_view = match oracle.begin_diagnostic_read(&scopes) {
        Ok(read_view) => read_view,
        Err(error) => {
            report.not_qualified(
                "recovery_oracle_read_unavailable",
                "recovery_outbox",
                &[],
                error.to_string(),
            );
            return false;
        }
    };
    let Some((started, deadline)) = report.validation_deadline else {
        report.not_qualified(
            "recovery_oracle_deadline_missing",
            "recovery_outbox",
            &[],
            "diagnostic validation deadline is not installed",
        );
        return false;
    };
    let capabilities = oracle.provider_capabilities();
    let mut budget = ScanBudget {
        started,
        deadline,
        max_rows: limits.max_rows,
        max_bytes: limits.max_bytes,
        rows: 0,
        bytes: 0,
    };
    let mut expected_rows = BTreeMap::new();
    for space in spaces {
        let projected = match scan_space(
            read_view.as_ref(),
            space,
            limits.page_size,
            capabilities,
            &mut budget,
        ) {
            Ok(projected) => projected,
            Err(ScanFailure::Corrupt {
                family,
                key,
                detail,
            }) => {
                report.corrupt("recovery_oracle_scan", &family, &key, detail);
                return false;
            }
            Err(ScanFailure::NotQualified { family, detail }) => {
                report.not_qualified("recovery_projection_budget", &family, &[], detail);
                return false;
            }
        };
        expected_rows.insert(space, projected);
    }
    drop(read_view);

    for space in all_ordered_spaces() {
        if !report.time_available("recovery_projection") {
            return false;
        }
        let expected = comparable_recovery_rows(space, rows_for(&expected_rows, space));
        let actual = comparable_recovery_rows(space, rows_for(rows, space));
        if expected.len() != actual.len() {
            report.corrupt(
                "recovery_projection_row_count",
                &space_name(space),
                &[],
                format!(
                    "authoritative replay contains {} comparable rows, captured state contains {}",
                    expected.len(),
                    actual.len()
                ),
            );
        }
        for expected_row in &expected {
            if !report.time_available("recovery_projection") {
                return false;
            }
            match actual.binary_search_by(|row| row.key.cmp(&expected_row.key)) {
                Ok(index) if actual[index].value == expected_row.value => {}
                Ok(_) => report.corrupt(
                    "recovery_projection_value",
                    &space_name(space),
                    &expected_row.key,
                    "captured row differs from the authoritative recovery replay",
                ),
                Err(_) => report.corrupt(
                    "recovery_projection_missing_row",
                    &space_name(space),
                    &expected_row.key,
                    "authoritative recovery-replayed row is missing",
                ),
            }
        }
        for actual_row in &actual {
            if !report.time_available("recovery_projection") {
                return false;
            }
            if expected
                .binary_search_by(|row| row.key.cmp(&actual_row.key))
                .is_err()
            {
                report.corrupt(
                    "recovery_projection_unexpected_row",
                    &space_name(space),
                    &actual_row.key,
                    "captured row was never produced by authoritative recovery replay",
                );
            }
        }
    }
    true
}

fn comparable_recovery_rows(space: OrderedSpaceId, rows: &[RawRow]) -> Vec<&RawRow> {
    rows.iter()
        .filter(|row| {
            space != crate::workspace::provider_catalog::SYSTEM_SPACE
                || !matches!(
                    row.key.as_slice(),
                    SYSTEM_STORE_IDENTITY_KEY | SYSTEM_METADATA_AUTHORITY_KEY
                )
        })
        .collect()
}
// Additional family/cross-reference validation is added in focused helpers
// below. Keeping one entrypoint makes it explicit that every helper consumes
// the already-captured snapshot only.
fn validate_records(
    identity: MetadataStoreIdentity,
    request: MetadataFsckRequest,
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) {
    report.check("all-current-history-codecs-v1");
    report.check("root-layout-fences-v2");
    report.check("workspace-path-revision-reachability-v1");
    report.check("commit-consumer-seals-v1");
    report.check("snapshot-hold-lifecycle-v1");
    report.check("operation-cursor-phase-v1");
    let Some(checked_version) = report.checked_commit_version else {
        return;
    };
    if !report.time_available("root_fence") {
        return;
    }
    let fences = decode_root_fences(identity, request, rows, report);
    let mut state = DecodedState::default();
    for family in MetadataFamily::ALL {
        if !report.time_available(family.tree_name()) {
            return;
        }
        for row in rows_for(
            rows,
            crate::workspace::provider_catalog::domain_space(family),
        ) {
            if !report.time_available(family.tree_name()) {
                return;
            }
            decode_current_family_row(family, row, checked_version, &fences, &mut state, report);
        }
    }
    if !report.time_available("history") {
        return;
    }
    validate_history(rows, checked_version, &fences, report);
    if !report.time_available("change_event") {
        return;
    }
    validate_change_events(rows, checked_version, &fences, report);
    if !report.time_available("cross_reference") {
        return;
    }
    state.validate(identity.logical_shard_id, report);
}

#[derive(Default)]
struct DecodedState {
    workspaces: BTreeMap<(RootId, String), WorkspaceRecord>,
    claims: BTreeMap<(RootId, WorkspaceIncarnationId), WorkspaceIncarnationClaimRecord>,
    paths: Vec<PathFact>,
    revisions: BTreeMap<(RootId, ArtifactRevisionId), ArtifactRevisionRecord>,
    manifests: BTreeMap<(RootId, ArtifactRevisionId), Vec<(u64, ArtifactManifestRow)>>,
    revision_refs: Vec<RevisionRefFact>,
    commits: BTreeMap<(RootId, CommitId), CommitRecord>,
    commit_members:
        BTreeMap<(RootId, CommitId), Vec<(nokv_types::NormalizedRelativePath, CommitMemberRecord)>>,
    heads: Vec<HeadFact>,
    tags: Vec<TagFact>,
    snapshots: Vec<SnapshotFact>,
    snapshot_claims: BTreeMap<(RootId, SnapshotId), WorkspaceIncarnationId>,
    aliases: Vec<AliasFact>,
    holds: Vec<HoldFact>,
    consumers: Vec<ConsumerFact>,
    secondary_indexes: BTreeMap<Vec<u8>, SecondaryIndexRecord>,
    operations: Vec<OperationFact>,
    restore_members: Vec<RestoreMemberFact>,
    staged_objects: Vec<StagedObjectFact>,
    gc_candidates: Vec<GcCandidateFact>,
    gc_barriers: BTreeMap<RootId, GcHistoryBarrierRecord>,
}

struct PathFact {
    root: RootId,
    workspace: WorkspaceIncarnationId,
    path: nokv_types::NormalizedRelativePath,
    entry: PathEntry,
}

enum RevisionRefOwner {
    Path,
    Commit { commit: CommitId },
    Dependency { child: ArtifactRevisionId },
}

struct RevisionRefFact {
    root: RootId,
    revision: ArtifactRevisionId,
    owner: RevisionRefOwner,
    key: Vec<u8>,
    record: RevisionRefRecord,
}

struct HeadFact {
    root: RootId,
    workspace: WorkspaceIncarnationId,
    record: WorkbenchCommitHeadRecord,
}

struct TagFact {
    root: RootId,
    workspace: WorkspaceIncarnationId,
    key: Vec<u8>,
    record: TagRecord,
}

struct SnapshotFact {
    root: RootId,
    workspace: WorkspaceIncarnationId,
    snapshot: SnapshotId,
    record: SnapshotRefRecord,
}

struct AliasFact {
    root: RootId,
    workspace: WorkspaceIncarnationId,
    key: Vec<u8>,
    record: SnapshotAliasRecord,
}

struct HoldFact {
    root: RootId,
    key: Vec<u8>,
    record: HistoryHoldRecord,
}

struct ConsumerFact {
    root: RootId,
    commit: CommitId,
    key: Vec<u8>,
    record: CommitConsumerRecord,
}

enum OperationRecord {
    Publish(PublishOperationRecord),
    BuildCommit(BuildCommitOperationRecord),
    Restore(RestoreOperationRecord),
    CommitRetire(CommitRetireOperationRecord),
    Gc(GcOperationRecord),
}

struct OperationFact {
    root: RootId,
    kind: OperationKind,
    id: OperationId,
    record: OperationRecord,
}

struct RestoreMemberFact {
    root: RootId,
    operation: OperationId,
    sequence: u64,
    record: RestoreMemberRecord,
}

struct StagedObjectFact {
    root: RootId,
    operation: OperationId,
    sequence: u64,
    record: StagedObjectRecord,
}

struct GcCandidateFact {
    root: RootId,
    revision: ArtifactRevisionId,
    epoch: nokv_types::ReferenceEpoch,
    record: GcCandidateRecord,
}

fn decode_root_fences(
    identity: MetadataStoreIdentity,
    request: MetadataFsckRequest,
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    report: &mut ReportBuilder,
) -> BTreeMap<RootId, RootFence> {
    let mut fences = BTreeMap::new();
    for row in rows_for(rows, crate::workspace::provider_catalog::ROOT_FENCE_SPACE) {
        if !report.time_available("root_fence") {
            return fences;
        }
        if row.key.len() != FIXED_ID_BYTES {
            report.corrupt(
                "root_fence_key",
                "root_fence",
                &row.key,
                "root fence key is not one RootId",
            );
            continue;
        }
        let root = RootId::from_bytes(row.key.as_slice().try_into().expect("width checked"));
        match RootFence::decode(&row.value) {
            Ok(fence) if fence.logical_shard_id == identity.logical_shard_id => {
                fences.insert(root, fence);
            }
            Ok(_) => report.corrupt(
                "root_fence_shard",
                "root_fence",
                &row.key,
                "root fence names another logical shard",
            ),
            Err(error) => report.corrupt(
                "root_fence_codec",
                "root_fence",
                &row.key,
                error.to_string(),
            ),
        }
    }
    match fences.get(&request.trigger_root_id) {
        Some(fence)
            if fence.placement_generation == request.placement_generation
                && fence.layout_profile == request.layout_profile
                && fence.layout_generation == request.layout_generation
                && fence.partition_id == request.partition_id
                && fence.activation_state == RootActivationState::Active => {}
        Some(_) => report.corrupt(
            "trigger_root_fence_mismatch",
            "root_fence",
            request.trigger_root_id.as_bytes(),
            "trigger route does not exactly match the active durable layout fence",
        ),
        None => report.corrupt(
            "trigger_root_fence_missing",
            "root_fence",
            request.trigger_root_id.as_bytes(),
            "trigger root fence is missing",
        ),
    }
    fences
}

fn root_from_key(key: &[u8]) -> Option<RootId> {
    (key.len() >= FIXED_ID_BYTES)
        .then(|| RootId::from_bytes(key[..FIXED_ID_BYTES].try_into().expect("width checked")))
}

fn decode_current_family_row(
    family: MetadataFamily,
    row: &RawRow,
    checked_version: CommitVersion,
    fences: &BTreeMap<RootId, RootFence>,
    state: &mut DecodedState,
    report: &mut ReportBuilder,
) {
    let Some(root) = root_from_key(&row.key) else {
        report.corrupt(
            "root_scoped_key",
            family.tree_name(),
            &row.key,
            "family key is shorter than RootId",
        );
        return;
    };
    if !fences.contains_key(&root) {
        report.corrupt(
            "row_without_root_fence",
            family.tree_name(),
            &row.key,
            "root-scoped row has no shard-local RootFence",
        );
    }
    let current = match CurrentValue::decode(&row.value) {
        Ok(current) => current,
        Err(error) => {
            report.corrupt(
                "current_value_codec",
                family.tree_name(),
                &row.key,
                error.to_string(),
            );
            return;
        }
    };
    if current.modified_version > checked_version {
        report.corrupt(
            "current_version_future",
            family.tree_name(),
            &row.key,
            format!(
                "row version {} exceeds commit clock {}",
                current.modified_version.get(),
                checked_version.get()
            ),
        );
    }
    if let Err(error) = decode_family_payload(family, root, &row.key, &current.payload, state) {
        report.corrupt("family_record_codec", family.tree_name(), &row.key, error);
    }
}

fn decode_family_payload(
    family: MetadataFamily,
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    match family {
        MetadataFamily::WorkspaceCurrent => {
            let workbench = decode_workspace_current_key(root, key)
                .ok_or_else(|| "malformed workspace key".to_owned())?;
            state.workspaces.insert(
                (root, workbench.as_str().to_owned()),
                WorkspaceRecord::decode(payload).map_err(|error| error.to_string())?,
            );
        }
        MetadataFamily::WorkspaceIncarnationClaim => {
            if key.len() != FIXED_ID_BYTES * 2 {
                return Err("malformed workspace incarnation claim key".to_owned());
            }
            let incarnation = WorkspaceIncarnationId::from_bytes(
                key[FIXED_ID_BYTES..].try_into().expect("width checked"),
            );
            state.claims.insert(
                (root, incarnation),
                WorkspaceIncarnationClaimRecord::decode(payload)
                    .map_err(|error| error.to_string())?,
            );
        }
        MetadataFamily::PathCurrent => {
            if key.len() < FIXED_ID_BYTES * 2 {
                return Err("malformed path key".to_owned());
            }
            let workspace = WorkspaceIncarnationId::from_bytes(
                key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2]
                    .try_into()
                    .expect("width checked"),
            );
            let path = decode_path_current_key(root, workspace, key)
                .ok_or_else(|| "malformed canonical path key".to_owned())?;
            state.paths.push(PathFact {
                root,
                workspace,
                path,
                entry: PathEntry::decode(payload).map_err(|error| error.to_string())?,
            });
        }
        MetadataFamily::ArtifactRevision => {
            if key.len() != FIXED_ID_BYTES * 2 {
                return Err("malformed artifact revision key".to_owned());
            }
            let revision = ArtifactRevisionId::from_bytes(
                key[FIXED_ID_BYTES..].try_into().expect("width checked"),
            );
            state.revisions.insert(
                (root, revision),
                ArtifactRevisionRecord::decode(payload).map_err(|error| error.to_string())?,
            );
        }
        MetadataFamily::ArtifactManifest => {
            if key.len() != FIXED_ID_BYTES * 2 + 8 {
                return Err("malformed artifact manifest key".to_owned());
            }
            let revision = ArtifactRevisionId::from_bytes(
                key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2]
                    .try_into()
                    .expect("width checked"),
            );
            let index = decode_artifact_manifest_key(root, revision, key)
                .ok_or_else(|| "malformed artifact manifest index".to_owned())?;
            state.manifests.entry((root, revision)).or_default().push((
                index,
                ArtifactManifestRow::decode(payload).map_err(|error| error.to_string())?,
            ));
        }
        MetadataFamily::RevisionRef => decode_revision_ref(root, key, payload, state)?,
        MetadataFamily::Commit => {
            let commit =
                decode_commit_key(root, key).ok_or_else(|| "malformed commit key".to_owned())?;
            state.commits.insert(
                (root, commit),
                CommitRecord::decode(payload).map_err(|error| error.to_string())?,
            );
        }
        MetadataFamily::CommitMember => {
            if key.len() < FIXED_ID_BYTES + CommitId::BYTE_WIDTH {
                return Err("malformed commit member key".to_owned());
            }
            let commit = CommitId::from_bytes(
                key[FIXED_ID_BYTES..FIXED_ID_BYTES + CommitId::BYTE_WIDTH]
                    .try_into()
                    .expect("width checked"),
            );
            let path = decode_commit_member_key(root, commit, key)
                .ok_or_else(|| "malformed commit member path".to_owned())?;
            state
                .commit_members
                .entry((root, commit))
                .or_default()
                .push((
                    path,
                    CommitMemberRecord::decode(payload).map_err(|error| error.to_string())?,
                ));
        }
        MetadataFamily::WorkbenchCommitHead => {
            if key.len() != FIXED_ID_BYTES * 2 {
                return Err("malformed workbench head key".to_owned());
            }
            let workspace = WorkspaceIncarnationId::from_bytes(
                key[FIXED_ID_BYTES..].try_into().expect("width checked"),
            );
            state.heads.push(HeadFact {
                root,
                workspace,
                record: WorkbenchCommitHeadRecord::decode(payload)
                    .map_err(|error| error.to_string())?,
            });
        }
        MetadataFamily::Tag => {
            if key.len() < FIXED_ID_BYTES * 2 + 2 {
                return Err("malformed tag key".to_owned());
            }
            let workspace = WorkspaceIncarnationId::from_bytes(
                key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2]
                    .try_into()
                    .expect("width checked"),
            );
            state.tags.push(TagFact {
                root,
                workspace,
                key: key.to_vec(),
                record: TagRecord::decode(payload).map_err(|error| error.to_string())?,
            });
        }
        MetadataFamily::SnapshotRef => decode_snapshot_ref(root, key, payload, state)?,
        MetadataFamily::SnapshotAlias => {
            if key.len() < FIXED_ID_BYTES * 2 + 2 {
                return Err("malformed snapshot alias key".to_owned());
            }
            let workspace = WorkspaceIncarnationId::from_bytes(
                key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2]
                    .try_into()
                    .expect("width checked"),
            );
            state.aliases.push(AliasFact {
                root,
                workspace,
                key: key.to_vec(),
                record: SnapshotAliasRecord::decode(payload).map_err(|error| error.to_string())?,
            });
        }
        MetadataFamily::HistoryHold => state.holds.push(HoldFact {
            root,
            key: key.to_vec(),
            record: HistoryHoldRecord::decode(payload).map_err(|error| error.to_string())?,
        }),
        MetadataFamily::CommitConsumer => decode_commit_consumer(root, key, payload, state)?,
        MetadataFamily::SecondaryIndex => {
            state.secondary_indexes.insert(
                key.to_vec(),
                SecondaryIndexRecord::decode(payload).map_err(|error| error.to_string())?,
            );
        }
        MetadataFamily::Operation => decode_operation(root, key, payload, state)?,
        MetadataFamily::RestoreMember => decode_restore_member(root, key, payload, state)?,
        MetadataFamily::StagedObject => decode_staged_object(root, key, payload, state)?,
        MetadataFamily::GcCandidate => {
            let (revision, epoch) = decode_gc_candidate_key(root, key)
                .ok_or_else(|| "malformed GC candidate key".to_owned())?;
            state.gc_candidates.push(GcCandidateFact {
                root,
                revision,
                epoch,
                record: GcCandidateRecord::decode(payload).map_err(|error| error.to_string())?,
            });
        }
        MetadataFamily::GcBarrier => {
            if key != gc_history_barrier_key(root) {
                return Err("malformed GC history barrier key".to_owned());
            }
            state.gc_barriers.insert(
                root,
                GcHistoryBarrierRecord::decode(payload).map_err(|error| error.to_string())?,
            );
        }
    }
    Ok(())
}

fn decode_revision_ref(
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    if key.len() < FIXED_ID_BYTES + 1 + FIXED_ID_BYTES {
        return Err("malformed revision reference key".to_owned());
    }
    let kind = ReferenceKind::try_from(key[FIXED_ID_BYTES]).map_err(|error| error.to_string())?;
    let revision = ArtifactRevisionId::from_bytes(
        key[key.len() - FIXED_ID_BYTES..]
            .try_into()
            .expect("width checked"),
    );
    let owner = match kind {
        ReferenceKind::Path => RevisionRefOwner::Path,
        ReferenceKind::Commit => {
            if key.len() != FIXED_ID_BYTES + 1 + CommitId::BYTE_WIDTH + FIXED_ID_BYTES {
                return Err("malformed commit revision reference key".to_owned());
            }
            RevisionRefOwner::Commit {
                commit: CommitId::from_bytes(
                    key[FIXED_ID_BYTES + 1..FIXED_ID_BYTES + 1 + CommitId::BYTE_WIDTH]
                        .try_into()
                        .expect("width checked"),
                ),
            }
        }
        ReferenceKind::RevisionDependency => {
            if key.len() != FIXED_ID_BYTES * 3 + 1 {
                return Err("malformed dependency reference key".to_owned());
            }
            let child = ArtifactRevisionId::from_bytes(
                key[FIXED_ID_BYTES + 1..FIXED_ID_BYTES * 2 + 1]
                    .try_into()
                    .expect("width checked"),
            );
            let owner = decode_revision_dependency_ref_key(root, child, key)
                .ok_or_else(|| "malformed dependency owner".to_owned())?;
            if owner != revision {
                return Err("dependency reference suffix differs from decoded owner".to_owned());
            }
            RevisionRefOwner::Dependency { child }
        }
    };
    state.revision_refs.push(RevisionRefFact {
        root,
        revision,
        owner,
        key: key.to_vec(),
        record: RevisionRefRecord::decode(payload).map_err(|error| error.to_string())?,
    });
    Ok(())
}

fn decode_snapshot_ref(
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    if key.len() == FIXED_ID_BYTES + 1 + 8 && key[FIXED_ID_BYTES] == 0xff {
        if payload.len() != FIXED_ID_BYTES {
            return Err("snapshot id claim payload is not one incarnation id".to_owned());
        }
        let snapshot = SnapshotId::new(u64::from_be_bytes(
            key[FIXED_ID_BYTES + 1..].try_into().expect("width checked"),
        ));
        let workspace =
            WorkspaceIncarnationId::from_bytes(payload.try_into().expect("payload width checked"));
        state.snapshot_claims.insert((root, snapshot), workspace);
        return Ok(());
    }
    let (workspace, snapshot) = decode_snapshot_ref_key(root, key)
        .ok_or_else(|| "malformed snapshot row key".to_owned())?;
    state.snapshots.push(SnapshotFact {
        root,
        workspace,
        snapshot,
        record: SnapshotRefRecord::decode(payload).map_err(|error| error.to_string())?,
    });
    Ok(())
}

fn decode_commit_consumer(
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    if key.len() < FIXED_ID_BYTES + CommitId::BYTE_WIDTH + 1 {
        return Err("malformed commit consumer key".to_owned());
    }
    let commit = CommitId::from_bytes(
        key[FIXED_ID_BYTES..FIXED_ID_BYTES + CommitId::BYTE_WIDTH]
            .try_into()
            .expect("width checked"),
    );
    let kind = nokv_types::CommitConsumerKind::try_from(key[FIXED_ID_BYTES + CommitId::BYTE_WIDTH])
        .map_err(|error| error.to_string())?;
    let suffix = &key[FIXED_ID_BYTES + CommitId::BYTE_WIDTH + 1..];
    let valid_width = match kind {
        nokv_types::CommitConsumerKind::WorkbenchHead | nokv_types::CommitConsumerKind::Lease => {
            suffix.len() == FIXED_ID_BYTES
        }
        nokv_types::CommitConsumerKind::ChildCommit => suffix.len() == CommitId::BYTE_WIDTH,
        nokv_types::CommitConsumerKind::Tag => suffix.len() >= FIXED_ID_BYTES + 2,
    };
    if !valid_width {
        return Err("malformed commit consumer owner id".to_owned());
    }
    state.consumers.push(ConsumerFact {
        root,
        commit,
        key: key.to_vec(),
        record: CommitConsumerRecord::decode(payload).map_err(|error| error.to_string())?,
    });
    Ok(())
}

fn decode_operation(
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    if key.len() != FIXED_ID_BYTES * 2 + 1 {
        return Err("malformed operation key".to_owned());
    }
    let kind = OperationKind::try_from(key[FIXED_ID_BYTES]).map_err(|error| error.to_string())?;
    let id = decode_operation_key(root, kind, key)
        .ok_or_else(|| "malformed operation identity".to_owned())?;
    let record = match kind {
        OperationKind::Publish => OperationRecord::Publish(
            PublishOperationRecord::decode(payload).map_err(|error| error.to_string())?,
        ),
        OperationKind::BuildCommit => OperationRecord::BuildCommit(
            BuildCommitOperationRecord::decode(payload).map_err(|error| error.to_string())?,
        ),
        OperationKind::Restore => OperationRecord::Restore(
            RestoreOperationRecord::decode(payload).map_err(|error| error.to_string())?,
        ),
        OperationKind::CommitRetire => OperationRecord::CommitRetire(
            CommitRetireOperationRecord::decode(payload).map_err(|error| error.to_string())?,
        ),
        OperationKind::Gc => OperationRecord::Gc(
            GcOperationRecord::decode(payload).map_err(|error| error.to_string())?,
        ),
    };
    let payload_id = match &record {
        OperationRecord::Publish(record) => record.operation_id,
        OperationRecord::BuildCommit(record) => record.operation_id,
        OperationRecord::Restore(record) => record.operation_id,
        OperationRecord::CommitRetire(record) => record.operation_id,
        OperationRecord::Gc(record) => record.operation_id,
    };
    if payload_id != id {
        return Err("operation payload identity differs from key".to_owned());
    }
    state.operations.push(OperationFact {
        root,
        kind,
        id,
        record,
    });
    Ok(())
}

fn decode_restore_member(
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    if key.len() != FIXED_ID_BYTES * 2 + 8 {
        return Err("malformed restore member key".to_owned());
    }
    let operation = OperationId::from_bytes(
        key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2]
            .try_into()
            .expect("width checked"),
    );
    let sequence = u64::from_be_bytes(key[FIXED_ID_BYTES * 2..].try_into().expect("width"));
    state.restore_members.push(RestoreMemberFact {
        root,
        operation,
        sequence,
        record: RestoreMemberRecord::decode(payload).map_err(|error| error.to_string())?,
    });
    Ok(())
}

fn decode_staged_object(
    root: RootId,
    key: &[u8],
    payload: &[u8],
    state: &mut DecodedState,
) -> Result<(), String> {
    if key.len() != FIXED_ID_BYTES * 2 + 8 {
        return Err("malformed staged object key".to_owned());
    }
    let operation = OperationId::from_bytes(
        key[FIXED_ID_BYTES..FIXED_ID_BYTES * 2]
            .try_into()
            .expect("width checked"),
    );
    let sequence = u64::from_be_bytes(key[FIXED_ID_BYTES * 2..].try_into().expect("width"));
    state.staged_objects.push(StagedObjectFact {
        root,
        operation,
        sequence,
        record: StagedObjectRecord::decode(payload).map_err(|error| error.to_string())?,
    });
    Ok(())
}

fn validate_history(
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    checked: CommitVersion,
    fences: &BTreeMap<RootId, RootFence>,
    report: &mut ReportBuilder,
) {
    for row in rows_for(rows, crate::workspace::provider_catalog::HISTORY_SPACE) {
        if !report.time_available("history") {
            return;
        }
        let history = match HistoryValue::decode(&row.value) {
            Ok(history) => history,
            Err(error) => {
                report.corrupt(
                    "history_value_codec",
                    "history",
                    &row.key,
                    error.to_string(),
                );
                continue;
            }
        };
        if history.transition_version > checked || row.key.len() < 1 + 4 + 8 {
            report.corrupt(
                "history_version_or_key",
                "history",
                &row.key,
                "history transition exceeds commit clock or key is truncated",
            );
            continue;
        }
        let family = match family_from_tag(row.key[0]) {
            Some(family) => family,
            None => {
                report.corrupt(
                    "history_family_tag",
                    "history",
                    &row.key,
                    "unknown history source family",
                );
                continue;
            }
        };
        let length = u32::from_be_bytes(row.key[1..5].try_into().expect("width")) as usize;
        if row.key.len() != 1 + 4 + length + 8 {
            report.corrupt(
                "history_key_length",
                "history",
                &row.key,
                "history user-key framing is malformed",
            );
            continue;
        }
        let user_key = &row.key[5..5 + length];
        let transition = !u64::from_be_bytes(row.key[5 + length..].try_into().expect("width"));
        if transition != history.transition_version.get() {
            report.corrupt(
                "history_key_transition",
                "history",
                &row.key,
                "inverted key version differs from payload transition",
            );
        }
        let Some(root) = root_from_key(user_key) else {
            report.corrupt(
                "history_root_key",
                "history",
                &row.key,
                "history user key is not root scoped",
            );
            continue;
        };
        if !fences.contains_key(&root) {
            report.corrupt(
                "history_without_root_fence",
                "history",
                &row.key,
                "history row belongs to a root without a fence",
            );
        }
        if let Some(payload) = &history.previous_payload {
            let mut throwaway = DecodedState::default();
            if let Err(error) =
                decode_family_payload(family, root, user_key, payload, &mut throwaway)
            {
                report.corrupt("history_payload_codec", "history", &row.key, error);
            }
        }
    }
}

fn family_from_tag(tag: u8) -> Option<MetadataFamily> {
    MetadataFamily::ALL
        .into_iter()
        .find(|family| family.history_tag() == tag)
}

fn validate_change_events(
    rows: &BTreeMap<OrderedSpaceId, Vec<RawRow>>,
    checked: CommitVersion,
    fences: &BTreeMap<RootId, RootFence>,
    report: &mut ReportBuilder,
) {
    for row in rows_for(rows, crate::workspace::provider_catalog::CHANGE_EVENT_SPACE) {
        if !report.time_available("change_event") {
            return;
        }
        let Some(root) = root_from_key(&row.key) else {
            report.corrupt(
                "event_key",
                "change_event",
                &row.key,
                "event key is truncated",
            );
            continue;
        };
        let Some((version, _)) = decode_change_event_key(root, &row.key) else {
            report.corrupt(
                "event_key",
                "change_event",
                &row.key,
                "event key is malformed",
            );
            continue;
        };
        if version > checked || !fences.contains_key(&root) {
            report.corrupt(
                "event_version_or_root",
                "change_event",
                &row.key,
                "event exceeds commit clock or belongs to an unfenced root",
            );
        }
        let current = match CurrentValue::decode(&row.value) {
            Ok(current) => current,
            Err(error) => {
                report.corrupt(
                    "event_envelope_codec",
                    "change_event",
                    &row.key,
                    error.to_string(),
                );
                continue;
            }
        };
        if current.modified_version != version || current.created_version != version {
            report.corrupt(
                "event_envelope_version",
                "change_event",
                &row.key,
                "event envelope version differs from its ordered key",
            );
        }
        if let Err(error) = ChangeEventRecord::decode(&current.payload) {
            report.corrupt(
                "event_value_codec",
                "change_event",
                &row.key,
                error.to_string(),
            );
        }
    }
}

impl DecodedState {
    fn validate(&mut self, shard: LogicalShardId, report: &mut ReportBuilder) {
        self.validate_workspace_paths(report);
        if !report.time_available("workspace_path_closure") {
            return;
        }
        self.validate_revisions(shard, report);
        if !report.time_available("revision_closure") {
            return;
        }
        self.validate_commits(report);
        if !report.time_available("commit_closure") {
            return;
        }
        self.validate_snapshots(report);
        if !report.time_available("snapshot_closure") {
            return;
        }
        self.validate_operations(report);
    }

    fn validate_workspace_paths(&self, report: &mut ReportBuilder) {
        let mut expected_indexes = BTreeMap::new();
        let mut expected_path_refs = BTreeSet::new();
        for ((root, workbench), workspace) in &self.workspaces {
            if !report.time_available("workspace_current") {
                return;
            }
            match self.claims.get(&(*root, workspace.incarnation_id)) {
                Some(claim) if claim.workbench_id.as_str() == workbench => {}
                _ => report.corrupt(
                    "workspace_claim_mismatch",
                    "workspace_current",
                    &workspace_current_key(
                        *root,
                        &nokv_types::WorkbenchId::new(workbench).expect("decoded id"),
                    ),
                    "workspace marker has no exact permanent incarnation claim",
                ),
            }
            if workspace.state == WorkspaceState::Visible && workspace.owning_operation_id.is_some()
            {
                report.corrupt(
                    "visible_workspace_operation",
                    "workspace_current",
                    root.as_bytes(),
                    "visible workspace retains an owning operation",
                );
            }
        }
        for ((root, incarnation), claim) in &self.claims {
            if !report.time_available("workspace_incarnation_claim") {
                return;
            }
            if !self
                .workspaces
                .iter()
                .any(|((candidate_root, workbench), workspace)| {
                    candidate_root == root
                        && workspace.incarnation_id == *incarnation
                        && workbench == claim.workbench_id.as_str()
                })
            {
                report.corrupt(
                    "orphan_workspace_claim",
                    "workspace_incarnation_claim",
                    &workspace_incarnation_claim_key(*root, *incarnation),
                    "incarnation claim has no matching current workspace marker",
                );
            }
        }
        for path in &self.paths {
            if !report.time_available("path_current") {
                return;
            }
            let workspace = self.workspaces.iter().find(|((root, _), workspace)| {
                *root == path.root && workspace.incarnation_id == path.workspace
            });
            if workspace.is_none() {
                report.corrupt(
                    "orphan_path_workspace",
                    "path_current",
                    &path_current_key(path.root, path.workspace, &path.path),
                    "path references a missing workspace incarnation",
                );
            }
            if !self
                .revisions
                .contains_key(&(path.root, path.entry.artifact_revision_id))
            {
                report.corrupt(
                    "path_missing_revision",
                    "path_current",
                    &path_current_key(path.root, path.workspace, &path.path),
                    "path references a missing artifact revision",
                );
            }
            expected_path_refs.insert(path_revision_ref_key(
                path.root,
                path.workspace,
                &path.path,
                path.entry.artifact_revision_id,
            ));
            match TypedProjection::decode(&path.entry.typed_index_projection) {
                Ok(projection) => {
                    let index_record = SecondaryIndexRecord {
                        path_generation: path.entry.generation,
                        compact_projection: projection.clone(),
                    };
                    for (field, scalar) in projection.fields() {
                        if !report.time_available("secondary_index") {
                            return;
                        }
                        expected_indexes.insert(
                            secondary_index_key(
                                path.root,
                                field,
                                scalar,
                                path.workspace,
                                &path.path,
                            ),
                            index_record.clone(),
                        );
                    }
                }
                Err(error) => report.corrupt(
                    "path_projection_codec",
                    "path_current",
                    &path_current_key(path.root, path.workspace, &path.path),
                    error.to_string(),
                ),
            }
        }
        let mut actual_path_refs = BTreeSet::new();
        for reference in &self.revision_refs {
            if !report.time_available("revision_ref") {
                return;
            }
            if matches!(reference.owner, RevisionRefOwner::Path) {
                actual_path_refs.insert(reference.key.clone());
            }
        }
        if actual_path_refs != expected_path_refs {
            report.corrupt(
                "path_reference_closure",
                "revision_ref",
                &[],
                "PathCurrent and path RevisionRef sets differ",
            );
        }
        if self.secondary_indexes != expected_indexes {
            report.corrupt(
                "secondary_index_projection",
                "secondary_index",
                &[],
                "secondary index rows differ from canonical current path projections",
            );
        }
    }

    fn validate_revisions(&self, shard: LogicalShardId, report: &mut ReportBuilder) {
        let mut reference_counts: BTreeMap<(RootId, ArtifactRevisionId), u64> = BTreeMap::new();
        for reference in &self.revision_refs {
            if !report.time_available("revision_ref") {
                return;
            }
            *reference_counts
                .entry((reference.root, reference.revision))
                .or_default() += 1;
            match self.revisions.get(&(reference.root, reference.revision)) {
                Some(revision)
                    if reference.record.reference_epoch_at_add <= revision.reference_epoch => {}
                Some(_) => report.corrupt(
                    "reference_epoch_future",
                    "revision_ref",
                    &reference.key,
                    "reference epoch is newer than its revision lifetime epoch",
                ),
                None => report.corrupt(
                    "orphan_revision_ref",
                    "revision_ref",
                    &reference.key,
                    "reference targets a missing revision",
                ),
            }
        }
        for ((root, revision_id), revision) in &self.revisions {
            if !report.time_available("artifact_revision") {
                return;
            }
            let count = reference_counts
                .get(&(*root, *revision_id))
                .copied()
                .unwrap_or_default();
            if count != revision.strong_reference_count {
                report.corrupt(
                    "revision_refcount",
                    "artifact_revision",
                    &artifact_revision_key(*root, *revision_id),
                    format!(
                        "stored strong count {}, recomputed {count}",
                        revision.strong_reference_count
                    ),
                );
            }
            let mut rows = self
                .manifests
                .get(&(*root, *revision_id))
                .cloned()
                .unwrap_or_default();
            rows.sort_by_key(|(index, _)| *index);
            if rows.len() as u64 != revision.block_count
                || rows
                    .iter()
                    .enumerate()
                    .any(|(expected, (index, _))| *index != expected as u64)
            {
                report.corrupt(
                    "manifest_count_or_order",
                    "artifact_manifest",
                    &artifact_revision_key(*root, *revision_id),
                    "manifest positions are not contiguous or do not match block_count",
                );
            }
            let mut owners = BTreeSet::new();
            let mut expected_offset = 0_u64;
            for (index, row) in &rows {
                if !report.time_available("artifact_manifest") {
                    return;
                }
                if row.logical_offset != expected_offset {
                    report.corrupt(
                        "manifest_logical_coverage",
                        "artifact_manifest",
                        &artifact_manifest_key(*root, *revision_id, *index),
                        "manifest logical ranges are not contiguous",
                    );
                }
                expected_offset = expected_offset.saturating_add(row.length);
                if !self
                    .revisions
                    .contains_key(&(*root, row.physical_owner_revision_id))
                {
                    report.corrupt(
                        "manifest_missing_physical_owner",
                        "artifact_manifest",
                        &artifact_manifest_key(*root, *revision_id, *index),
                        "manifest physical owner revision is missing",
                    );
                }
                if row.object_key
                    != object_block_key(
                        shard,
                        *root,
                        row.physical_owner_revision_id,
                        row.physical_object_index,
                    )
                {
                    report.corrupt(
                        "manifest_object_key",
                        "artifact_manifest",
                        &artifact_manifest_key(*root, *revision_id, *index),
                        "object key differs from physical-owner canonical identity",
                    );
                }
                if row.physical_owner_revision_id != *revision_id {
                    owners.insert(row.physical_owner_revision_id);
                }
            }
            if expected_offset != revision.logical_size {
                report.corrupt(
                    "manifest_logical_size",
                    "artifact_manifest",
                    &artifact_revision_key(*root, *revision_id),
                    "manifest logical closure differs from revision logical size",
                );
            }
            let owner_vec = owners.iter().copied().collect::<Vec<_>>();
            if owner_vec.len() as u32 != revision.dependency_count
                || dependency_owner_digest(&owner_vec)
                    .is_ok_and(|digest| digest != revision.dependency_digest)
            {
                report.corrupt(
                    "revision_dependency_seal",
                    "artifact_revision",
                    &artifact_revision_key(*root, *revision_id),
                    "sealed dependency count/digest differs from manifest owners",
                );
            }
            let mut actual_dependencies = BTreeSet::new();
            for reference in &self.revision_refs {
                if !report.time_available("revision_ref") {
                    return;
                }
                if let RevisionRefOwner::Dependency { child } = reference.owner {
                    if reference.root == *root && child == *revision_id {
                        actual_dependencies.insert(reference.revision);
                    }
                }
            }
            let expected_dependencies = if revision.state == RevisionState::Deleted {
                BTreeSet::new()
            } else {
                owners
            };
            if actual_dependencies != expected_dependencies {
                report.corrupt(
                    "revision_dependency_refs",
                    "revision_ref",
                    &revision_dependency_ref_key(*root, *revision_id, *revision_id),
                    "dependency RevisionRef set differs from manifest physical owners",
                );
            }
        }
        for (root, revision) in self.manifests.keys() {
            if !report.time_available("artifact_manifest") {
                return;
            }
            if !self.revisions.contains_key(&(*root, *revision)) {
                report.corrupt(
                    "orphan_manifest",
                    "artifact_manifest",
                    &artifact_revision_key(*root, *revision),
                    "manifest rows have no artifact revision",
                );
            }
        }
        for candidate in &self.gc_candidates {
            if !report.time_available("gc_candidate") {
                return;
            }
            match self.revisions.get(&(candidate.root, candidate.revision)) {
                Some(revision) if candidate.epoch < revision.reference_epoch => {
                    if candidate.record.claim_state != GcClaimState::Candidate
                        || candidate.record.quarantine_evidence.is_some()
                    {
                        report.corrupt(
                            "gc_stale_candidate_state",
                            "gc_candidate",
                            &gc_candidate_key(candidate.root, candidate.revision, candidate.epoch),
                            "a stale epoch candidate must remain an unclaimed candidate",
                        );
                    }
                }
                Some(revision)
                    if candidate.epoch == revision.reference_epoch
                        && revision.strong_reference_count == 0
                        && revision.last_zero_ref_version
                            == Some(candidate.record.last_zero_ref_version) => {}
                _ => report.corrupt(
                    "gc_candidate_revision",
                    "gc_candidate",
                    &gc_candidate_key(candidate.root, candidate.revision, candidate.epoch),
                    "GC candidate is not bound to a compatible revision epoch/zero version",
                ),
            }
        }
    }

    fn validate_commits(&self, report: &mut ReportBuilder) {
        let mut actual_refs = BTreeMap::<_, Vec<_>>::new();
        for reference in &self.revision_refs {
            if !report.time_available("revision_ref") {
                return;
            }
            if let RevisionRefOwner::Commit { commit } = reference.owner {
                actual_refs
                    .entry((reference.root, commit))
                    .or_default()
                    .push((reference.revision, reference));
            }
        }

        let mut retire_operations = BTreeMap::<_, Vec<_>>::new();
        for operation in &self.operations {
            if !report.time_available("operation") {
                return;
            }
            if let OperationRecord::CommitRetire(record) = &operation.record {
                retire_operations
                    .entry((operation.root, record.commit_id))
                    .or_default()
                    .push((operation.id, record));
            }
        }

        let mut expected_consumers = BTreeSet::new();
        for ((root, commit_id), commit) in &self.commits {
            if !report.time_available("commit") {
                return;
            }
            let retire_operation = match retire_operations.get(&(*root, *commit_id)) {
                None => None,
                Some(operations) if operations.len() == 1 => Some(operations[0].1),
                Some(operations) => {
                    report.corrupt(
                        "commit_retire_operation_count",
                        "operation",
                        &commit_key(*root, *commit_id),
                        format!(
                            "commit has {} retirement operations instead of one",
                            operations.len()
                        ),
                    );
                    operations.first().map(|(_, record)| *record)
                }
            };

            match commit.state {
                CommitState::Sealed => {
                    if retire_operation.is_some() {
                        report.corrupt(
                            "sealed_commit_retire_operation",
                            "operation",
                            &commit_key(*root, *commit_id),
                            "sealed commit unexpectedly has a retirement operation",
                        );
                    }
                }
                CommitState::Retiring => match retire_operation.map(|record| record.phase) {
                    Some(CommitRetirePhase::Claiming | CommitRetirePhase::Releasing) => {}
                    Some(CommitRetirePhase::Quarantined) => report.not_qualified(
                        "commit_retire_quarantined",
                        "operation",
                        &commit_key(*root, *commit_id),
                        "quarantined commit retirement requires operator resolution",
                    ),
                    Some(CommitRetirePhase::Complete) => report.corrupt(
                        "commit_retire_phase_state",
                        "operation",
                        &commit_key(*root, *commit_id),
                        "retiring commit is paired with a complete retirement operation",
                    ),
                    None => report.corrupt(
                        "commit_retire_operation_missing",
                        "operation",
                        &commit_key(*root, *commit_id),
                        "retiring commit has no retirement operation",
                    ),
                },
                CommitState::Retired => match retire_operation.map(|record| record.phase) {
                    Some(CommitRetirePhase::Complete) => {}
                    Some(_) => report.corrupt(
                        "commit_retire_phase_state",
                        "operation",
                        &commit_key(*root, *commit_id),
                        "retired commit is not paired with a complete retirement operation",
                    ),
                    None => report.corrupt(
                        "commit_retire_operation_missing",
                        "operation",
                        &commit_key(*root, *commit_id),
                        "retired commit has no retirement operation",
                    ),
                },
            }

            if let Some(operation) = retire_operation {
                if operation.claimed_consumer_epoch != commit.consumer_epoch
                    || operation.member_count != commit.member_count
                    || operation.member_digest != commit.member_digest
                    || operation.revision_count != commit.unique_revision_count
                    || operation.revision_digest != commit.revision_digest
                    || operation.parent_commits != commit.parent_commits
                    || operation.parent_digest != commit.parent_digest
                {
                    report.corrupt(
                        "commit_retire_seal_binding",
                        "operation",
                        &commit_key(*root, *commit_id),
                        "retirement operation is not bound to the commit consumer epoch and exact closure seals",
                    );
                }
            }

            let use_release_progress = commit.state != CommitState::Sealed;
            let released_member_count = retire_operation
                .filter(|_| use_release_progress)
                .map_or(0, |operation| operation.released_member_count);
            let released_member_digest = retire_operation
                .filter(|_| use_release_progress)
                .map_or([0; SHA256_BYTES], |operation| {
                    operation.released_member_digest
                });
            let released_revision_count = retire_operation
                .filter(|_| use_release_progress)
                .map_or(0, |operation| operation.released_revision_count);
            let released_revision_digest = retire_operation
                .filter(|_| use_release_progress)
                .map_or([0; SHA256_BYTES], |operation| {
                    operation.released_revision_digest
                });
            let released_parent_count = retire_operation
                .filter(|_| use_release_progress)
                .map_or(0, |operation| operation.released_parent_count);

            let mut members = self
                .commit_members
                .get(&(*root, *commit_id))
                .cloned()
                .unwrap_or_default();
            members.sort_by(|left, right| left.0.cmp(&right.0));
            let mut member_digest = released_member_digest;
            let mut member_revisions = BTreeSet::new();
            for (offset, (path, member)) in members.iter().enumerate() {
                if !report.time_available("commit_member") {
                    return;
                }
                let sequence = released_member_count.saturating_add(offset as u64);
                match commit_member_row_digest(path, member) {
                    Ok(row_digest) => {
                        member_digest = advance_commit_member_rolling_digest(
                            member_digest,
                            sequence,
                            row_digest,
                        );
                    }
                    Err(error) => report.corrupt(
                        "commit_member_digest",
                        "commit_member",
                        &commit_member_key(*root, *commit_id, path),
                        error.to_string(),
                    ),
                }
                member_revisions.insert(member.artifact_revision_id);
                if !self
                    .revisions
                    .contains_key(&(*root, member.artifact_revision_id))
                {
                    report.corrupt(
                        "commit_member_revision",
                        "commit_member",
                        &commit_member_key(*root, *commit_id, path),
                        "commit member references a missing revision",
                    );
                }
            }
            let member_total = released_member_count.checked_add(members.len() as u64);
            if member_total != Some(commit.member_count) || member_digest != commit.member_digest {
                report.corrupt(
                    "commit_member_seal",
                    "commit",
                    &commit_key(*root, *commit_id),
                    "released prefix plus remaining ordered members differs from the commit seal",
                );
            }
            let mut refs = actual_refs
                .get(&(*root, *commit_id))
                .cloned()
                .unwrap_or_default();
            refs.sort_by_key(|(revision, _)| *revision);
            let mut revision_digest = released_revision_digest;
            for (offset, (revision, reference)) in refs.iter().enumerate() {
                if !report.time_available("revision_ref") {
                    return;
                }
                revision_digest = advance_commit_revision_rolling_digest(
                    revision_digest,
                    released_revision_count.saturating_add(offset as u64),
                    *revision,
                    &reference
                        .record
                        .encode()
                        .expect("decoded reference re-encodes"),
                );
            }
            let revision_total = released_revision_count.checked_add(refs.len() as u64);
            if revision_total != Some(commit.unique_revision_count)
                || revision_digest != commit.revision_digest
            {
                report.corrupt(
                    "commit_revision_seal",
                    "commit",
                    &commit_key(*root, *commit_id),
                    "released prefix plus remaining revision references differs from the commit seal",
                );
            }
            if released_member_count == 0 {
                let all_member_revisions = member_revisions.iter().copied().collect::<Vec<_>>();
                let remaining_revisions = refs
                    .iter()
                    .map(|(revision, _)| *revision)
                    .collect::<Vec<_>>();
                if all_member_revisions.len() as u64 != commit.unique_revision_count
                    || all_member_revisions.get(released_revision_count as usize..)
                        != Some(remaining_revisions.as_slice())
                {
                    report.corrupt(
                        "commit_revision_member_closure",
                        "revision_ref",
                        &commit_key(*root, *commit_id),
                        "remaining revision references are not the canonical suffix of member revisions",
                    );
                }
            }

            let mut parent_digest = [0; SHA256_BYTES];
            for (sequence, parent) in commit.parent_commits.iter().enumerate() {
                if !report.time_available("commit") {
                    return;
                }
                parent_digest =
                    advance_commit_parent_rolling_digest(parent_digest, sequence as u64, *parent);
                if !self.commits.contains_key(&(*root, *parent)) {
                    report.corrupt(
                        "commit_parent_missing",
                        "commit",
                        &commit_key(*root, *commit_id),
                        "parent commit is missing",
                    );
                }
                if sequence >= released_parent_count as usize {
                    expected_consumers
                        .insert(child_commit_consumer_key(*root, *parent, *commit_id));
                }
            }
            if parent_digest != commit.parent_digest {
                report.corrupt(
                    "commit_parent_seal",
                    "commit",
                    &commit_key(*root, *commit_id),
                    "parent digest differs from direct parent list",
                );
            }
            let commit_consumers = self
                .consumers
                .iter()
                .filter(|consumer| consumer.root == *root && consumer.commit == *commit_id)
                .collect::<Vec<_>>();
            for consumer in &commit_consumers {
                if consumer.record.consumer_epoch_at_add > commit.consumer_epoch {
                    report.corrupt(
                        "consumer_epoch_future",
                        "commit_consumer",
                        &consumer.key,
                        "consumer epoch-at-add exceeds the commit consumer epoch",
                    );
                }
            }
            let consumers = commit_consumers.len() as u64;
            if consumers != commit.consumer_count {
                report.corrupt(
                    "commit_consumer_count",
                    "commit",
                    &commit_key(*root, *commit_id),
                    format!("stored {}, recomputed {consumers}", commit.consumer_count),
                );
            }
        }
        for (root, commit) in self.commit_members.keys() {
            if !report.time_available("commit_member") {
                return;
            }
            if !self.commits.contains_key(&(*root, *commit)) {
                report.corrupt(
                    "orphan_commit_member",
                    "commit_member",
                    &commit_key(*root, *commit),
                    "member closure has no commit",
                );
            }
        }
        for head in &self.heads {
            if !report.time_available("workbench_commit_head") {
                return;
            }
            if !self
                .commits
                .contains_key(&(head.root, head.record.commit_id))
            {
                report.corrupt(
                    "head_missing_commit",
                    "workbench_commit_head",
                    &workbench_commit_head_key(head.root, head.workspace),
                    "workbench head references a missing commit",
                );
            }
            expected_consumers.insert(workbench_head_commit_consumer_key(
                head.root,
                head.record.commit_id,
                head.workspace,
            ));
        }
        for tag in &self.tags {
            if !report.time_available("tag") {
                return;
            }
            if !self.commits.contains_key(&(tag.root, tag.record.commit_id)) {
                report.corrupt(
                    "tag_missing_commit",
                    "tag",
                    &tag.key,
                    "tag references a missing commit",
                );
            }
            let suffix = &tag.key[FIXED_ID_BYTES * 2..];
            if suffix.len() < 2 {
                continue;
            }
            let length = u16::from_be_bytes(suffix[..2].try_into().expect("width")) as usize;
            let Some(name) = suffix.get(2..2 + length) else {
                report.corrupt("tag_key", "tag", &tag.key, "tag name framing is malformed");
                continue;
            };
            let Ok(name) = std::str::from_utf8(name)
                .map_err(|error| error.to_string())
                .and_then(|name| nokv_types::TagName::new(name).map_err(|error| error.to_string()))
            else {
                report.corrupt("tag_key", "tag", &tag.key, "tag name is invalid");
                continue;
            };
            if tag.key != tag_key(tag.root, tag.workspace, &name) {
                report.corrupt("tag_key", "tag", &tag.key, "tag key is non-canonical");
            }
            expected_consumers.insert(tag_commit_consumer_key(
                tag.root,
                tag.record.commit_id,
                tag.workspace,
                &name,
            ));
        }
        for operation in &self.operations {
            if !report.time_available("operation") {
                return;
            }
            if let OperationRecord::Restore(record) = &operation.record {
                if let RestoreSource::Commit { commit_id } = record.source {
                    let should_retain = !matches!(
                        record.phase,
                        nokv_types::RestorePhase::Complete | nokv_types::RestorePhase::Cleaned
                    );
                    if should_retain {
                        expected_consumers.insert(lease_commit_consumer_key(
                            operation.root,
                            commit_id,
                            operation.id,
                        ));
                    }
                }
            }
        }

        let actual_consumers = self
            .consumers
            .iter()
            .map(|consumer| consumer.key.clone())
            .collect::<BTreeSet<_>>();
        for key in expected_consumers.difference(&actual_consumers) {
            if !report.time_available("commit_consumer") {
                return;
            }
            report.corrupt(
                "missing_commit_consumer",
                "commit_consumer",
                key,
                "durable consumer owner has no exact consumer row",
            );
        }
        for consumer in &self.consumers {
            if !report.time_available("commit_consumer") {
                return;
            }
            if !self.commits.contains_key(&(consumer.root, consumer.commit)) {
                report.corrupt(
                    "orphan_commit_consumer",
                    "commit_consumer",
                    &consumer.key,
                    "consumer targets a missing commit",
                );
            }
            if !expected_consumers.contains(&consumer.key) {
                report.corrupt(
                    "unexpected_commit_consumer",
                    "commit_consumer",
                    &consumer.key,
                    "consumer row has no live durable owner",
                );
            }
        }
    }

    fn validate_snapshots(&self, report: &mut ReportBuilder) {
        for snapshot in &self.snapshots {
            if !report.time_available("snapshot_ref") {
                return;
            }
            if self
                .snapshot_claims
                .get(&(snapshot.root, snapshot.snapshot))
                != Some(&snapshot.workspace)
            {
                report.corrupt(
                    "snapshot_claim",
                    "snapshot_ref",
                    &snapshot_id_claim_key(snapshot.root, snapshot.snapshot),
                    "snapshot row has no exact root-global id claim",
                );
            }
            let hold_key = snapshot_history_hold_key(snapshot.root, snapshot.snapshot);
            let hold = self.holds.iter().find(|hold| hold.key == hold_key);
            let active = matches!(snapshot.record.state, nokv_types::SnapshotState::Active);
            if active
                != hold.is_some_and(|hold| {
                    hold.root == snapshot.root
                        && hold.record.read_version == snapshot.record.read_version
                        && hold.record.source_snapshot_id.is_none()
                        && hold.record.state == HistoryHoldState::Active
                })
            {
                report.corrupt(
                    "snapshot_history_hold",
                    "history_hold",
                    &hold_key,
                    "active snapshot hold does not exactly bind its read version/state",
                );
            }
        }
        for ((root, snapshot), workspace) in &self.snapshot_claims {
            if !report.time_available("snapshot_ref") {
                return;
            }
            if !self.snapshots.iter().any(|candidate| {
                candidate.root == *root
                    && candidate.snapshot == *snapshot
                    && candidate.workspace == *workspace
            }) {
                report.corrupt(
                    "orphan_snapshot_claim",
                    "snapshot_ref",
                    &snapshot_id_claim_key(*root, *snapshot),
                    "snapshot id claim has no snapshot row",
                );
            }
        }
        for alias in &self.aliases {
            if !report.time_available("snapshot_alias") {
                return;
            }
            let suffix = &alias.key[FIXED_ID_BYTES * 2..];
            if suffix.len() < 2 {
                report.corrupt(
                    "snapshot_alias_key",
                    "snapshot_alias",
                    &alias.key,
                    "snapshot alias key is truncated",
                );
                continue;
            }
            let length = u16::from_be_bytes(suffix[..2].try_into().expect("width")) as usize;
            let Some(name) = suffix.get(2..2 + length) else {
                report.corrupt(
                    "snapshot_alias_key",
                    "snapshot_alias",
                    &alias.key,
                    "snapshot alias name framing is malformed",
                );
                continue;
            };
            let Ok(name) = std::str::from_utf8(name)
                .map_err(|error| error.to_string())
                .and_then(|name| SnapshotAliasName::new(name).map_err(|error| error.to_string()))
            else {
                report.corrupt(
                    "snapshot_alias_key",
                    "snapshot_alias",
                    &alias.key,
                    "snapshot alias name is invalid",
                );
                continue;
            };
            if alias.key != snapshot_alias_key(alias.root, alias.workspace, &name) {
                report.corrupt(
                    "snapshot_alias_key",
                    "snapshot_alias",
                    &alias.key,
                    "snapshot alias key is non-canonical",
                );
            }
            let selected = self.snapshots.iter().find(|snapshot| {
                snapshot.root == alias.root
                    && snapshot.workspace == alias.workspace
                    && snapshot.snapshot == alias.record.snapshot_id
            });
            if selected.is_none_or(|snapshot| {
                snapshot.record.state != alias.record.snapshot_state
                    || snapshot.record.alias.as_ref() != Some(&name)
            }) {
                report.corrupt(
                    "orphan_snapshot_alias",
                    "snapshot_alias",
                    &alias.key,
                    "alias selects a missing snapshot or stale lifecycle projection",
                );
            }
        }
        for hold in &self.holds {
            if !report.time_available("history_hold") {
                return;
            }
            if hold.key.len() < FIXED_ID_BYTES + 1
                || hold.key[..FIXED_ID_BYTES] != *hold.root.as_bytes()
            {
                report.corrupt(
                    "history_hold_key",
                    "history_hold",
                    &hold.key,
                    "history hold key is malformed",
                );
                continue;
            }
            if hold.record.state != HistoryHoldState::Active {
                report.not_qualified(
                    "history_hold_releasing",
                    "history_hold",
                    &hold.key,
                    "runtime does not currently create a releasing hold; coverage is fail-closed",
                );
                continue;
            }
            let kind = match HistoryHoldKind::try_from(hold.key[FIXED_ID_BYTES]) {
                Ok(kind) => kind,
                Err(error) => {
                    report.corrupt(
                        "history_hold_kind",
                        "history_hold",
                        &hold.key,
                        error.to_string(),
                    );
                    continue;
                }
            };
            match kind {
                HistoryHoldKind::Snapshot => {
                    if hold.key.len() != FIXED_ID_BYTES + 1 + 8 {
                        report.corrupt(
                            "snapshot_hold_key",
                            "history_hold",
                            &hold.key,
                            "snapshot hold key has the wrong width",
                        );
                    }
                }
                HistoryHoldKind::BuildCommit => {
                    if hold.key.len() != FIXED_ID_BYTES * 2 + 1 {
                        report.corrupt(
                            "build_hold_key",
                            "history_hold",
                            &hold.key,
                            "build hold key has the wrong width",
                        );
                        continue;
                    }
                    let operation = OperationId::from_bytes(
                        hold.key[FIXED_ID_BYTES + 1..]
                            .try_into()
                            .expect("width checked"),
                    );
                    if !self.operations.iter().any(|candidate| {
                        candidate.root == hold.root
                            && candidate.id == operation
                            && matches!(
                                &candidate.record,
                                OperationRecord::BuildCommit(record)
                                    if !matches!(
                                        record.phase,
                                        nokv_types::BuildCommitPhase::Complete
                                            | nokv_types::BuildCommitPhase::Cleaned
                                    )
                            )
                    }) {
                        report.corrupt(
                            "orphan_build_hold",
                            "history_hold",
                            &hold.key,
                            "build hold has no nonterminal build operation",
                        );
                    }
                }
                HistoryHoldKind::Restore => {
                    if hold.key.len() != FIXED_ID_BYTES * 2 + 1 {
                        report.corrupt(
                            "restore_hold_key",
                            "history_hold",
                            &hold.key,
                            "restore hold key has the wrong width",
                        );
                        continue;
                    }
                    let operation = OperationId::from_bytes(
                        hold.key[FIXED_ID_BYTES + 1..]
                            .try_into()
                            .expect("width checked"),
                    );
                    if !self.operations.iter().any(|candidate| {
                        candidate.root == hold.root
                            && candidate.id == operation
                            && matches!(
                                &candidate.record,
                                OperationRecord::Restore(record)
                                    if matches!(record.source, RestoreSource::Snapshot { .. })
                                        && !matches!(
                                            record.phase,
                                            nokv_types::RestorePhase::Complete
                                                | nokv_types::RestorePhase::Cleaned
                                        )
                            )
                    }) {
                        report.corrupt(
                            "orphan_restore_hold",
                            "history_hold",
                            &hold.key,
                            "restore hold has no retained snapshot-source restore",
                        );
                    }
                }
            }
        }
    }

    fn validate_operations(&self, report: &mut ReportBuilder) {
        for operation in &self.operations {
            if !report.time_available("operation") {
                return;
            }
            if operation.key() != operation_key(operation.root, operation.kind, operation.id) {
                unreachable!("decoded operation key is canonical by construction");
            }
            match &operation.record {
                OperationRecord::Publish(record) => {
                    match record.phase {
                        nokv_types::PublishPhase::Uploading
                        | nokv_types::PublishPhase::Finalizing
                        | nokv_types::PublishPhase::Aborting
                        | nokv_types::PublishPhase::Cleaning => report.not_qualified(
                            "publish_in_flight",
                            "operation",
                            operation.id.as_bytes(),
                            "in-flight publication is validated fail-closed until it reaches a terminal phase",
                        ),
                        nokv_types::PublishPhase::Quarantined => report.not_qualified(
                            "publish_quarantined",
                            "operation",
                            operation.id.as_bytes(),
                            "quarantined publication requires operator resolution",
                        ),
                        nokv_types::PublishPhase::Published
                        | nokv_types::PublishPhase::Cleaned => {}
                    }
                    let mut rows = self
                        .staged_objects
                        .iter()
                        .filter(|row| row.root == operation.root && row.operation == operation.id)
                        .collect::<Vec<_>>();
                    rows.sort_by_key(|row| row.sequence);
                    let expected_remaining = record
                        .staged_object_cursor
                        .saturating_sub(record.cleanup_staged_object_cursor);
                    if rows.len() as u32 != expected_remaining {
                        report.corrupt(
                            "publish_staged_cursor",
                            "operation",
                            operation.id.as_bytes(),
                            "remaining staged-object row count differs from cleanup cursors",
                        );
                    }
                    for (expected, row) in rows.iter().enumerate() {
                        if !report.time_available("staged_object") {
                            return;
                        }
                        let expected = u64::from(record.cleanup_staged_object_cursor)
                            .saturating_add(expected as u64);
                        if row.sequence != expected
                            || u64::from(row.record.object_sequence) != expected
                            || row.record.artifact_revision_id != record.artifact_revision_id
                        {
                            report.corrupt(
                                "publish_staged_closure",
                                "staged_object",
                                &staged_object_key(operation.root, operation.id, row.sequence),
                                "staged-object closure is not contiguous/exact",
                            );
                        }
                    }
                }
                OperationRecord::Restore(record) => {
                    match record.phase {
                        nokv_types::RestorePhase::Preparing
                        | nokv_types::RestorePhase::Copying
                        | nokv_types::RestorePhase::SourceSealed
                        | nokv_types::RestorePhase::Ready
                        | nokv_types::RestorePhase::Aborting
                        | nokv_types::RestorePhase::Cleaning => report.not_qualified(
                            "restore_in_flight",
                            "operation",
                            operation.id.as_bytes(),
                            "in-flight restore is validated fail-closed until it reaches a terminal phase",
                        ),
                        nokv_types::RestorePhase::Quarantined => report.not_qualified(
                            "restore_quarantined",
                            "operation",
                            operation.id.as_bytes(),
                            "quarantined restore requires operator resolution",
                        ),
                        nokv_types::RestorePhase::Complete
                        | nokv_types::RestorePhase::Cleaned => {}
                    }
                    let mut members = self
                        .restore_members
                        .iter()
                        .filter(|member| {
                            member.root == operation.root && member.operation == operation.id
                        })
                        .collect::<Vec<_>>();
                    members.sort_by_key(|member| member.sequence);
                    for _ in &members {
                        if !report.time_available("restore_member") {
                            return;
                        }
                    }
                    let expected_remaining = record
                        .next_member_sequence
                        .saturating_sub(record.cleanup_member_cursor);
                    if members.len() as u64 != expected_remaining
                        || members.iter().enumerate().any(|(expected, member)| {
                            member.sequence
                                != record.cleanup_member_cursor.saturating_add(expected as u64)
                        })
                    {
                        report.corrupt(
                            "restore_member_cursor",
                            "restore_member",
                            operation.id.as_bytes(),
                            "restore member closure is not contiguous to its durable cursor",
                        );
                    }
                    let destination = self.workspaces.iter().find(|((root, _), workspace)| {
                        *root == operation.root
                            && workspace.incarnation_id
                                == record.destination_workspace_incarnation_id
                    });
                    if destination.is_none() {
                        report.corrupt(
                            "restore_destination_marker",
                            "operation",
                            operation.id.as_bytes(),
                            "restore destination workspace marker is missing",
                        );
                    }
                    match record.source {
                        RestoreSource::Snapshot {
                            snapshot_id,
                            read_version,
                        } => {
                            if !self.snapshots.iter().any(|snapshot| {
                                snapshot.root == operation.root
                                    && snapshot.snapshot == snapshot_id
                                    && snapshot.workspace == record.source_workspace_incarnation_id
                            }) {
                                report.corrupt(
                                    "restore_snapshot_source",
                                    "operation",
                                    operation.id.as_bytes(),
                                    "restore snapshot source is missing",
                                );
                            }
                            let hold_key = restore_history_hold_key(operation.root, operation.id);
                            let should_retain = !matches!(
                                record.phase,
                                nokv_types::RestorePhase::Complete
                                    | nokv_types::RestorePhase::Cleaned
                            );
                            let exact_hold = self.holds.iter().any(|hold| {
                                hold.key == hold_key
                                    && hold.record.read_version == read_version
                                    && hold.record.source_snapshot_id == Some(snapshot_id)
                                    && hold.record.state == HistoryHoldState::Active
                            });
                            if should_retain != exact_hold {
                                report.corrupt(
                                    "restore_snapshot_hold",
                                    "history_hold",
                                    &hold_key,
                                    "snapshot-source restore retention differs from its phase",
                                );
                            }
                        }
                        RestoreSource::Commit { commit_id } => {
                            if !self.commits.contains_key(&(operation.root, commit_id)) {
                                report.corrupt(
                                    "restore_commit_source",
                                    "operation",
                                    operation.id.as_bytes(),
                                    "restore commit source is missing",
                                );
                            }
                            let consumer_key =
                                lease_commit_consumer_key(operation.root, commit_id, operation.id);
                            let should_retain = !matches!(
                                record.phase,
                                nokv_types::RestorePhase::Complete
                                    | nokv_types::RestorePhase::Cleaned
                            );
                            if should_retain
                                != self
                                    .consumers
                                    .iter()
                                    .any(|consumer| consumer.key == consumer_key)
                            {
                                report.corrupt(
                                    "restore_commit_consumer",
                                    "commit_consumer",
                                    &consumer_key,
                                    "commit-source restore retention differs from its phase",
                                );
                            }
                        }
                    }
                }
                OperationRecord::BuildCommit(record) => {
                    match record.phase {
                        nokv_types::BuildCommitPhase::Building
                        | nokv_types::BuildCommitPhase::Sealing
                        | nokv_types::BuildCommitPhase::Aborting
                        | nokv_types::BuildCommitPhase::Cleaning => report.not_qualified(
                            "build_commit_in_flight",
                            "operation",
                            operation.id.as_bytes(),
                            "in-flight commit build is validated fail-closed until it reaches a terminal phase",
                        ),
                        nokv_types::BuildCommitPhase::Quarantined => report.not_qualified(
                            "build_commit_quarantined",
                            "operation",
                            operation.id.as_bytes(),
                            "quarantined commit build requires operator resolution",
                        ),
                        nokv_types::BuildCommitPhase::Complete
                        | nokv_types::BuildCommitPhase::Cleaned => {}
                    }
                    if matches!(record.phase, nokv_types::BuildCommitPhase::Complete)
                        && !self
                            .commits
                            .contains_key(&(operation.root, record.commit_id))
                    {
                        report.corrupt(
                            "build_commit_terminal_record",
                            "operation",
                            operation.id.as_bytes(),
                            "completed build operation has no sealed commit",
                        );
                    }
                    let hold_key = build_commit_history_hold_key(operation.root, operation.id);
                    let should_retain = !matches!(
                        record.phase,
                        nokv_types::BuildCommitPhase::Complete
                            | nokv_types::BuildCommitPhase::Cleaned
                    );
                    if should_retain
                        != self.holds.iter().any(|hold| {
                            hold.key == hold_key
                                && hold.record.read_version == record.source_read_version
                                && hold.record.source_snapshot_id.is_none()
                                && hold.record.state == HistoryHoldState::Active
                        })
                    {
                        report.corrupt(
                            "build_commit_hold",
                            "history_hold",
                            &hold_key,
                            "build operation retention differs from its phase/source version",
                        );
                    }
                }
                OperationRecord::CommitRetire(record) => {
                    if record.phase == CommitRetirePhase::Quarantined {
                        report.not_qualified(
                            "commit_retire_quarantined",
                            "operation",
                            operation.id.as_bytes(),
                            "quarantined commit retirement requires operator resolution",
                        );
                    }
                    if !self
                        .commits
                        .contains_key(&(operation.root, record.commit_id))
                    {
                        report.corrupt(
                            "retire_commit_record",
                            "operation",
                            operation.id.as_bytes(),
                            "commit-retire operation has no commit record",
                        );
                    }
                }
                OperationRecord::Gc(record) => {
                    match record.phase {
                        nokv_types::GcPhase::Queued
                        | nokv_types::GcPhase::Claimed
                        | nokv_types::GcPhase::Deleting => report.not_qualified(
                            "gc_in_flight",
                            "operation",
                            operation.id.as_bytes(),
                            "in-flight GC is validated fail-closed until it reaches a terminal phase",
                        ),
                        nokv_types::GcPhase::Quarantined => report.not_qualified(
                            "gc_quarantined",
                            "operation",
                            operation.id.as_bytes(),
                            "quarantined GC requires operator resolution",
                        ),
                        nokv_types::GcPhase::Deleted => {}
                    }
                    if !self
                        .revisions
                        .contains_key(&(operation.root, record.artifact_revision_id))
                    {
                        report.corrupt(
                            "gc_operation_revision",
                            "operation",
                            operation.id.as_bytes(),
                            "GC operation references a missing revision",
                        );
                    }
                }
            }
        }
        for member in &self.restore_members {
            if !report.time_available("restore_member") {
                return;
            }
            if !self.operations.iter().any(|operation| {
                operation.root == member.root
                    && operation.id == member.operation
                    && operation.kind == OperationKind::Restore
            }) {
                report.corrupt(
                    "orphan_restore_member",
                    "restore_member",
                    member.operation.as_bytes(),
                    "restore member has no restore operation",
                );
            }
            if !self
                .revisions
                .contains_key(&(member.root, member.record.artifact_revision_id))
            {
                report.corrupt(
                    "restore_member_revision",
                    "restore_member",
                    member.operation.as_bytes(),
                    "restore member references a missing revision",
                );
            }
        }
        for staged in &self.staged_objects {
            if !report.time_available("staged_object") {
                return;
            }
            if !self.operations.iter().any(|operation| {
                operation.root == staged.root
                    && operation.id == staged.operation
                    && operation.kind == OperationKind::Publish
            }) {
                report.corrupt(
                    "orphan_staged_object",
                    "staged_object",
                    staged.operation.as_bytes(),
                    "staged object has no publish operation",
                );
            }
        }
    }
}

impl OperationFact {
    fn key(&self) -> Vec<u8> {
        operation_key(self.root, self.kind, self.id)
    }
}

#[cfg(test)]
mod tests {
    use nokv_types::{
        CommandDigest, ConsumerEpoch, Generation, NormalizedRelativePath, ReferenceEpoch,
        RequestId, RootLayoutGeneration, RootPartitionId, WorkbenchId, WorkspaceIncarnationId,
    };

    use super::*;
    use crate::workspace::{
        create_visible_workspace, mint_snapshot, CommandMutation, CommandPredicate,
        HistoryProjection, MetadataCommand, MintSnapshotRequest, RootFenceAction, RootWriteContext,
        SCHEMA_ID,
    };

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([0x11; FIXED_ID_BYTES])
    }

    fn root() -> RootId {
        RootId::from_bytes([0x22; FIXED_ID_BYTES])
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(1).unwrap()
    }

    fn install_root(store: &AgentMetadataStore) {
        store.advance_owner_epoch(None, owner()).unwrap();
        let command = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id: RequestId::from_bytes([0x33; FIXED_ID_BYTES]),
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: RootFenceAction::Install {
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
            },
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&command).unwrap();
        let activate = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id: RequestId::from_bytes([0x34; FIXED_ID_BYTES]),
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&activate).unwrap();
    }

    fn request() -> MetadataFsckRequest {
        MetadataFsckRequest {
            trigger_root_id: root(),
            placement_generation: placement(),
            owner_epoch: owner(),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            limits: MetadataFsckLimits::default(),
        }
    }

    #[test]
    fn healthy_nonempty_workspace_is_a_real_pass() {
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        install_root(&store);
        create_visible_workspace(
            &store,
            RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([0x44; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &WorkbenchId::new("healthy").unwrap(),
            WorkspaceIncarnationId::from_bytes([0x55; FIXED_ID_BYTES]),
        )
        .unwrap();

        let report = run_metadata_fsck(&store, request());

        assert_eq!(report.status, MetadataFsckStatus::Pass, "{report:#?}");
        assert!(report.checked_commit_version.is_some());
        assert!(report.recovery_frontier.is_some());
        assert!(report.state_digest.is_some());
        assert!(report.findings.is_empty());
    }

    #[test]
    fn recovery_projection_rejects_a_deleted_closed_workspace_subgraph() {
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        install_root(&store);
        let workbench = WorkbenchId::new("closed-subgraph").unwrap();
        let incarnation = WorkspaceIncarnationId::from_bytes([0x56; FIXED_ID_BYTES]);
        create_visible_workspace(
            &store,
            RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([0x45; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &workbench,
            incarnation,
        )
        .unwrap();
        store
            .delete_diagnostic_row_for_test(
                crate::workspace::provider_catalog::domain_space(MetadataFamily::WorkspaceCurrent),
                workspace_current_key(root(), &workbench),
            )
            .unwrap();
        store
            .delete_diagnostic_row_for_test(
                crate::workspace::provider_catalog::domain_space(
                    MetadataFamily::WorkspaceIncarnationClaim,
                ),
                workspace_incarnation_claim_key(root(), incarnation),
            )
            .unwrap();

        let report = run_metadata_fsck(&store, request());

        assert_eq!(report.status, MetadataFsckStatus::Corrupt, "{report:#?}");
        assert!(report.findings.iter().any(|finding| {
            finding.code == "recovery_projection_missing_row"
                || finding.code == "recovery_projection_row_count"
        }));
    }

    #[test]
    fn recovery_projection_rejects_history_required_by_an_active_snapshot() {
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        install_root(&store);
        let workbench = WorkbenchId::new("history-gap").unwrap();
        let incarnation = WorkspaceIncarnationId::from_bytes([0x57; FIXED_ID_BYTES]);
        create_visible_workspace(
            &store,
            RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([0x46; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &workbench,
            incarnation,
        )
        .unwrap();
        let minted = mint_snapshot(
            &store,
            RootWriteContext::current(
                &store,
                root(),
                shard(),
                placement(),
                owner(),
                RequestId::from_bytes([0x47; FIXED_ID_BYTES]),
            )
            .unwrap(),
            &MintSnapshotRequest {
                workbench_id: workbench.clone(),
                snapshot_id: SnapshotId::new(1),
                alias: None,
                lease_deadline_ms: 10_000,
                annotation: Vec::new(),
            },
        )
        .unwrap();
        let key = workspace_current_key(root(), &workbench);
        let context = RootWriteContext::current(
            &store,
            root(),
            shard(),
            placement(),
            owner(),
            RequestId::from_bytes([0x48; FIXED_ID_BYTES]),
        )
        .unwrap();
        let payload = store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::WorkspaceCurrent,
                &key,
                context.read_version,
            )
            .unwrap()
            .unwrap();
        let replace = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id: context.request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: context.read_version,
            root_fence_action: RootFenceAction::RequireActive,
            predicates: vec![CommandPredicate::Value {
                family: MetadataFamily::WorkspaceCurrent,
                key: key.clone(),
                expected: Some(payload.clone()),
            }],
            mutations: vec![CommandMutation::Put {
                family: MetadataFamily::WorkspaceCurrent,
                key: key.clone(),
                value: payload,
            }],
            history_projection: vec![HistoryProjection {
                family: MetadataFamily::WorkspaceCurrent,
                key: key.clone(),
            }],
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&replace).unwrap();
        let read_view = store
            .begin_diagnostic_read(&[ReadScope {
                space: crate::workspace::provider_catalog::HISTORY_SPACE,
                prefix: Vec::new(),
            }])
            .unwrap();
        let history = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::HISTORY_SPACE,
                prefix: Vec::new(),
                start_after: None,
                delimiter: None,
                limit: 16,
            })
            .unwrap();
        let history_key = history
            .items
            .into_iter()
            .find_map(|item| match item {
                ProviderScanItem::Key { key, .. } => Some(key),
                ProviderScanItem::CommonPrefix(_) => None,
            })
            .expect("replacement created one history row");
        drop(read_view);
        store
            .delete_diagnostic_row_for_test(
                crate::workspace::provider_catalog::HISTORY_SPACE,
                history_key,
            )
            .unwrap();
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::WorkspaceCurrent,
                &key,
                minted.snapshot.record.read_version,
            )
            .unwrap()
            .is_none());

        let report = run_metadata_fsck(&store, request());

        assert_eq!(report.status, MetadataFsckStatus::Corrupt, "{report:#?}");
        assert!(report.findings.iter().any(|finding| {
            finding.family == "history"
                && (finding.code == "recovery_projection_missing_row"
                    || finding.code == "recovery_projection_row_count")
        }));
    }

    #[test]
    fn invalid_time_budget_is_not_qualified() {
        let store = AgentMetadataStore::open_memory(shard()).unwrap();
        let mut request = request();
        request.limits.max_duration = Duration::ZERO;

        let report = run_metadata_fsck(&store, request);

        assert_eq!(report.status, MetadataFsckStatus::NotQualified);
        assert_eq!(report.findings[0].code, "invalid_limits");
    }

    #[test]
    fn unicode_finding_detail_truncates_at_a_character_boundary() {
        let mut builder = ReportBuilder::new(shard(), 1);
        let detail = format!("{}é", "a".repeat(FINDING_DETAIL_BYTES - 1));

        builder.corrupt("unicode", "test", &[], detail);

        assert_eq!(builder.findings.len(), 1);
        assert_eq!(
            builder.findings[0].detail,
            "a".repeat(FINDING_DETAIL_BYTES - 1)
        );
        assert!(builder.findings[0]
            .detail
            .is_char_boundary(builder.findings[0].detail.len()));
    }

    fn retiring_commit_state(released_revision_digest: [u8; SHA256_BYTES]) -> DecodedState {
        let commit_id = CommitId::from_bytes([0x61; SHA256_BYTES]);
        let revision_id = ArtifactRevisionId::from_bytes([0x62; FIXED_ID_BYTES]);
        let operation_id = OperationId::from_bytes([0x63; FIXED_ID_BYTES]);
        let path = NormalizedRelativePath::new("artifact.bin").unwrap();
        let member = CommitMemberRecord {
            artifact_revision_id: revision_id,
            path_generation: Generation::new(1).unwrap(),
            body_digest_uri: "sha256:body".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            logical_size: 1,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: None,
            manifest_id: None,
            typed_projection: Vec::new(),
        };
        let member_row_digest = commit_member_row_digest(&path, &member).unwrap();
        let member_digest =
            advance_commit_member_rolling_digest([0; SHA256_BYTES], 0, member_row_digest);
        let reference = RevisionRefRecord {
            reference_epoch_at_add: ReferenceEpoch::new(1),
        };
        let revision_digest = advance_commit_revision_rolling_digest(
            [0; SHA256_BYTES],
            0,
            revision_id,
            &reference.encode().unwrap(),
        );
        let commit = CommitRecord {
            source_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes(
                [0x64; FIXED_ID_BYTES],
            ),
            content_digest_uri: "sha256:content".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            tree_manifest_revision_id: revision_id,
            tree_digest_uri: "sha256:tree".to_owned(),
            member_count: 1,
            member_digest,
            unique_revision_count: 1,
            revision_digest,
            parent_commits: Vec::new(),
            parent_digest: [0; SHA256_BYTES],
            producer: None,
            lineage_projection: Vec::new(),
            consumer_count: 0,
            consumer_epoch: ConsumerEpoch::new(1),
            last_zero_consumer_version: Some(CommitVersion::new(1).unwrap()),
            state: CommitState::Retiring,
        };
        let mut retirement = CommitRetireOperationRecord {
            operation_id,
            identity_digest: [0; SHA256_BYTES],
            commit_id,
            claimed_consumer_epoch: commit.consumer_epoch,
            member_count: commit.member_count,
            member_digest: commit.member_digest,
            revision_count: commit.unique_revision_count,
            revision_digest: commit.revision_digest,
            parent_commits: commit.parent_commits.clone(),
            parent_digest: commit.parent_digest,
            phase: CommitRetirePhase::Releasing,
            released_member_count: 0,
            released_member_digest: [0; SHA256_BYTES],
            released_revision_count: 1,
            released_revision_digest,
            released_parent_count: 0,
            released_parent_digest: [0; SHA256_BYTES],
            terminal_error: None,
        };
        retirement.seal_identity();

        let mut state = DecodedState::default();
        state.commits.insert((root(), commit_id), commit);
        state
            .commit_members
            .insert((root(), commit_id), vec![(path, member)]);
        state.revisions.insert(
            (root(), revision_id),
            ArtifactRevisionRecord {
                logical_size: 1,
                body_digest_uri: "sha256:body".to_owned(),
                manifest_digest_uri: "sha256:manifest".to_owned(),
                block_count: 0,
                dependency_count: 0,
                dependency_depth: 0,
                dependency_digest: [0; SHA256_BYTES],
                content_type: "application/octet-stream".to_owned(),
                state: RevisionState::Available,
                reference_epoch: ReferenceEpoch::new(1),
                strong_reference_count: 0,
                last_zero_ref_version: Some(CommitVersion::new(1).unwrap()),
            },
        );
        state.operations.push(OperationFact {
            root: root(),
            kind: OperationKind::CommitRetire,
            id: operation_id,
            record: OperationRecord::CommitRetire(retirement),
        });
        state
    }

    #[test]
    fn retiring_commit_accepts_an_exact_released_revision_prefix() {
        let exact_digest = {
            let state = retiring_commit_state([1; SHA256_BYTES]);
            let OperationRecord::CommitRetire(operation) = &state.operations[0].record else {
                unreachable!();
            };
            operation.revision_digest
        };
        let state = retiring_commit_state(exact_digest);
        let mut report = ReportBuilder::new(shard(), 16);

        state.validate_commits(&mut report);

        assert!(report.findings.is_empty(), "{:#?}", report.findings);
    }

    #[test]
    fn retiring_commit_rejects_a_forged_released_revision_digest() {
        let state = retiring_commit_state([0x99; SHA256_BYTES]);
        let mut report = ReportBuilder::new(shard(), 16);

        state.validate_commits(&mut report);

        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "commit_revision_seal"));
    }
}
