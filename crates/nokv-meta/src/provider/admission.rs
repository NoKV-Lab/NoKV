//! Storage-neutral qualification of a provider SPI v1 contract offer.
//!
//! Factories report mechanics and limits; they never self-declare that they
//! are qualified. Runtime admission compares that offer with this engine's
//! canonical schema and complete legal-plan ceiling before opening a store.

use super::v1::{
    ProviderContractOfferV1, ProviderSchemaV1, ProviderTransactionModel, ProviderVersionModel,
};

/// Canonical provider limits and scan semantics required by this workspace
/// engine generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceProviderRequirementsV1 {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_atomic_operations: usize,
    pub max_logical_plan_bytes: usize,
    pub requires_consistent_cross_space_reads: bool,
    pub requires_all_ambiguous_commit_outcomes_settled_before_return: bool,
    pub requires_commit_resolution_reads_causally_current: bool,
    pub requires_exclusive_scan_start_after: bool,
    pub requires_consistent_snapshot_scans: bool,
    pub requires_unbounded_read_view: bool,
    pub requires_unbounded_scan_items: bool,
}

/// Closed, machine-readable reason that an offer is not qualified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProviderAdmissionCode {
    SpiMajorMismatch,
    WorkspaceContractDigestMismatch,
    OrderedSpaceCatalogMismatch,
    TransactionModelUnsupported,
    VersionModelUnsupported,
    CrossSpaceReadConsistencyMissing,
    AmbiguousCommitMayRemainInFlight,
    CommitCausalResolutionMissing,
    KeyLimitTooSmall,
    ValueLimitTooSmall,
    AtomicOperationLimitTooSmall,
    LogicalPlanLimitTooSmall,
    ExclusiveScanStartAfterMissing,
    ConsistentSnapshotScanMissing,
    ReadViewLifetimeBounded,
    ScanItemCountBounded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAdmissionReportV1 {
    pub requirements: WorkspaceProviderRequirementsV1,
    pub rejection_codes: Vec<ProviderAdmissionCode>,
}

impl ProviderAdmissionReportV1 {
    #[must_use]
    pub fn is_qualified(&self) -> bool {
        self.rejection_codes.is_empty()
    }
}

/// Return the engine-owned requirements derived from the same command,
/// envelope, and recovery limits used to validate and lower real writes.
#[must_use]
pub fn workspace_provider_requirements_v1() -> WorkspaceProviderRequirementsV1 {
    let values = crate::workspace::canonical_provider_requirement_values();
    WorkspaceProviderRequirementsV1 {
        max_key_bytes: values.max_key_bytes,
        max_value_bytes: values.max_value_bytes,
        max_atomic_operations: values.max_atomic_operations,
        max_logical_plan_bytes: values.max_logical_plan_bytes,
        requires_consistent_cross_space_reads: true,
        requires_all_ambiguous_commit_outcomes_settled_before_return: true,
        requires_commit_resolution_reads_causally_current: true,
        requires_exclusive_scan_start_after: true,
        requires_consistent_snapshot_scans: true,
        requires_unbounded_read_view: true,
        requires_unbounded_scan_items: true,
    }
}

/// Compare one factory offer with the canonical schema and engine-owned
/// requirements. An empty rejection list is the only qualified result.
#[must_use]
pub fn admit_provider_offer_v1(
    schema: &ProviderSchemaV1,
    offer: &ProviderContractOfferV1,
) -> ProviderAdmissionReportV1 {
    let canonical_schema = crate::workspace::canonical_provider_schema_v1();
    let requirements = workspace_provider_requirements_v1();
    let capabilities = offer.capabilities;
    let mut rejection_codes = Vec::new();
    if schema.spi_major() != canonical_schema.spi_major() {
        rejection_codes.push(ProviderAdmissionCode::SpiMajorMismatch);
    }
    if schema.workspace_contract_digest() != canonical_schema.workspace_contract_digest() {
        rejection_codes.push(ProviderAdmissionCode::WorkspaceContractDigestMismatch);
    }
    if schema.ordered_spaces() != canonical_schema.ordered_spaces() {
        rejection_codes.push(ProviderAdmissionCode::OrderedSpaceCatalogMismatch);
    }
    if capabilities.transaction_model != ProviderTransactionModel::CrossSpaceAtomicBatch {
        rejection_codes.push(ProviderAdmissionCode::TransactionModelUnsupported);
    }
    if capabilities.version_model != ProviderVersionModel::OpaqueRecordWitness {
        rejection_codes.push(ProviderAdmissionCode::VersionModelUnsupported);
    }
    if requirements.requires_consistent_cross_space_reads
        && !capabilities.consistent_cross_space_reads
    {
        rejection_codes.push(ProviderAdmissionCode::CrossSpaceReadConsistencyMissing);
    }
    if requirements.requires_all_ambiguous_commit_outcomes_settled_before_return
        && !capabilities.all_ambiguous_commit_outcomes_settled_before_return
    {
        rejection_codes.push(ProviderAdmissionCode::AmbiguousCommitMayRemainInFlight);
    }
    if requirements.requires_commit_resolution_reads_causally_current
        && !capabilities.commit_resolution_reads_causally_current
    {
        rejection_codes.push(ProviderAdmissionCode::CommitCausalResolutionMissing);
    }
    if capabilities.max_key_bytes < requirements.max_key_bytes {
        rejection_codes.push(ProviderAdmissionCode::KeyLimitTooSmall);
    }
    if capabilities.max_value_bytes < requirements.max_value_bytes {
        rejection_codes.push(ProviderAdmissionCode::ValueLimitTooSmall);
    }
    if capabilities.max_atomic_operations < requirements.max_atomic_operations {
        rejection_codes.push(ProviderAdmissionCode::AtomicOperationLimitTooSmall);
    }
    if capabilities.max_logical_plan_bytes < requirements.max_logical_plan_bytes {
        rejection_codes.push(ProviderAdmissionCode::LogicalPlanLimitTooSmall);
    }
    if requirements.requires_exclusive_scan_start_after && !capabilities.exclusive_scan_start_after
    {
        rejection_codes.push(ProviderAdmissionCode::ExclusiveScanStartAfterMissing);
    }
    if requirements.requires_consistent_snapshot_scans && !capabilities.consistent_snapshot_scans {
        rejection_codes.push(ProviderAdmissionCode::ConsistentSnapshotScanMissing);
    }
    if requirements.requires_unbounded_read_view && capabilities.max_read_view_duration.is_some() {
        rejection_codes.push(ProviderAdmissionCode::ReadViewLifetimeBounded);
    }
    if requirements.requires_unbounded_scan_items && capabilities.max_scan_items.is_some() {
        rejection_codes.push(ProviderAdmissionCode::ScanItemCountBounded);
    }
    ProviderAdmissionReportV1 {
        requirements,
        rejection_codes,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::provider::v1::ProviderCapabilities;

    fn qualified_capabilities() -> ProviderCapabilities {
        let required = workspace_provider_requirements_v1();
        ProviderCapabilities {
            transaction_model: ProviderTransactionModel::CrossSpaceAtomicBatch,
            version_model: ProviderVersionModel::OpaqueRecordWitness,
            consistent_cross_space_reads: true,
            all_ambiguous_commit_outcomes_settled_before_return: true,
            commit_resolution_reads_causally_current: true,
            max_key_bytes: required.max_key_bytes,
            max_value_bytes: required.max_value_bytes,
            max_transaction_bytes: usize::MAX,
            max_atomic_operations: required.max_atomic_operations,
            max_logical_plan_bytes: required.max_logical_plan_bytes,
            exclusive_scan_start_after: true,
            consistent_snapshot_scans: true,
            max_read_view_duration: None,
            max_scan_items: None,
        }
    }

    #[test]
    fn requirements_freeze_the_complete_engine_plan_ceiling() {
        assert_eq!(
            workspace_provider_requirements_v1(),
            WorkspaceProviderRequirementsV1 {
                max_key_bytes: 8_205,
                max_value_bytes: 61_493,
                max_atomic_operations: 2_128,
                max_logical_plan_bytes: 148_317_344,
                requires_consistent_cross_space_reads: true,
                requires_all_ambiguous_commit_outcomes_settled_before_return: true,
                requires_commit_resolution_reads_causally_current: true,
                requires_exclusive_scan_start_after: true,
                requires_consistent_snapshot_scans: true,
                requires_unbounded_read_view: true,
                requires_unbounded_scan_items: true,
            }
        );
    }

    #[test]
    fn admission_is_engine_computed_and_reports_every_independent_gap() {
        let schema = crate::workspace::canonical_provider_schema_v1();
        let qualified = ProviderContractOfferV1 {
            capabilities: qualified_capabilities(),
        };
        assert!(admit_provider_offer_v1(&schema, &qualified).is_qualified());

        let mut capabilities = qualified.capabilities;
        capabilities.max_atomic_operations -= 1;
        capabilities.max_logical_plan_bytes -= 1;
        capabilities.all_ambiguous_commit_outcomes_settled_before_return = false;
        capabilities.commit_resolution_reads_causally_current = false;
        capabilities.max_read_view_duration = Some(Duration::from_secs(5));
        capabilities.max_scan_items = Some(1_024);
        let report = admit_provider_offer_v1(&schema, &ProviderContractOfferV1 { capabilities });
        assert_eq!(
            report.rejection_codes,
            vec![
                ProviderAdmissionCode::AmbiguousCommitMayRemainInFlight,
                ProviderAdmissionCode::CommitCausalResolutionMissing,
                ProviderAdmissionCode::AtomicOperationLimitTooSmall,
                ProviderAdmissionCode::LogicalPlanLimitTooSmall,
                ProviderAdmissionCode::ReadViewLifetimeBounded,
                ProviderAdmissionCode::ScanItemCountBounded,
            ]
        );
    }
}
