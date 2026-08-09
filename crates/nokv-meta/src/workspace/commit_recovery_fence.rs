/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! One-shot provider-open authority for exact commit-receipt recovery.
//!
//! A durable `Pending` receipt and an exact-prior provider read do not by
//! themselves prove that an older runtime can no longer dispatch the planned
//! commit. This module carries the narrower authority established when one
//! qualified provider installation excludes that older dispatch and retains
//! the same exclusion resource through recovery.

use std::fmt;
use std::sync::Arc;

use crate::provider::v1::{
    AtomicCommitOutcome, AtomicPlan, MetadataProvider, MetadataProviderFactoryV1, MetadataReadView,
    MetadataTransaction, OrderedSpaceId, ProviderCapabilities, ProviderDiagnosticsV1,
    ProviderError, ProviderRecord, ProviderScan, ProviderScanPage, ProviderSchemaV1, ReadScope,
};

use super::commit_receipt::{MetadataCommitReceiptDirtySourceV1, PlannedMetadataCommitV1};
use super::engine::MetadataCommitEngineMintAuthorityV1;

/// Installation-scoped capability for excluding an older commit dispatch.
///
/// Public code can construct only [`Self::unsupported`]. A supported value is
/// minted together with a private authority by a qualified built-in provider
/// installation. Clones retain that nominal identity, and equality compares
/// the identity rather than a provider-wide boolean.
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataOldDispatchExclusionInstallationV1;
///
/// let forged = MetadataOldDispatchExclusionInstallationV1 { core: None };
/// ```
#[derive(Clone)]
pub struct MetadataOldDispatchExclusionInstallationV1 {
    core: Option<Arc<MetadataOldDispatchExclusionInstallationCoreV1>>,
}

struct MetadataOldDispatchExclusionInstallationCoreV1;

impl MetadataOldDispatchExclusionInstallationV1 {
    /// Return an installation that cannot establish an old-dispatch fence.
    #[must_use]
    pub const fn unsupported() -> Self {
        Self { core: None }
    }

    /// Whether this exact installation carries a private fence authority.
    #[must_use]
    pub fn is_supported(&self) -> bool {
        self.core.is_some()
    }
}

impl Default for MetadataOldDispatchExclusionInstallationV1 {
    fn default() -> Self {
        Self::unsupported()
    }
}

impl PartialEq for MetadataOldDispatchExclusionInstallationV1 {
    fn eq(&self, other: &Self) -> bool {
        match (&self.core, &other.core) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

impl Eq for MetadataOldDispatchExclusionInstallationV1 {}

impl fmt::Debug for MetadataOldDispatchExclusionInstallationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataOldDispatchExclusionInstallationV1")
            .field("supported", &self.is_supported())
            .finish_non_exhaustive()
    }
}

/// Private half of one supported installation capability.
pub(crate) struct MetadataOldDispatchExclusionInstallationAuthorityV1 {
    core: Arc<MetadataOldDispatchExclusionInstallationCoreV1>,
}

pub(crate) fn mint_old_dispatch_exclusion_installation_v1() -> (
    MetadataOldDispatchExclusionInstallationV1,
    MetadataOldDispatchExclusionInstallationAuthorityV1,
) {
    let core = Arc::new(MetadataOldDispatchExclusionInstallationCoreV1);
    (
        MetadataOldDispatchExclusionInstallationV1 {
            core: Some(Arc::clone(&core)),
        },
        MetadataOldDispatchExclusionInstallationAuthorityV1 { core },
    )
}

impl MetadataOldDispatchExclusionInstallationAuthorityV1 {
    pub(crate) fn capability(&self) -> MetadataOldDispatchExclusionInstallationV1 {
        MetadataOldDispatchExclusionInstallationV1 {
            core: Some(Arc::clone(&self.core)),
        }
    }

    pub(crate) fn bind_opened_provider<G>(
        &self,
        planned: &PlannedMetadataCommitV1,
        provider: &Arc<dyn MetadataProvider>,
        lifetime_guard: G,
    ) -> MetadataOldDispatchExcludedBackendAuthorityV1
    where
        G: Send + Sync + 'static,
    {
        MetadataOldDispatchExcludedBackendAuthorityV1 {
            installation_core: Arc::clone(&self.core),
            planned: planned.clone(),
            provider_allocation: Arc::clone(provider),
            _lifetime_guard: Arc::new(lifetime_guard),
        }
    }
}

/// Backend-only proof that one exact provider and its exclusion resource are
/// retained together.
pub(crate) struct MetadataOldDispatchExcludedBackendAuthorityV1 {
    installation_core: Arc<MetadataOldDispatchExclusionInstallationCoreV1>,
    planned: PlannedMetadataCommitV1,
    provider_allocation: Arc<dyn MetadataProvider>,
    _lifetime_guard: Arc<dyn Send + Sync>,
}

/// Opaque binding to the exact recovery-open command allocation and backend
/// authority retained by one proof-carrying provider.
///
/// This value is deliberately non-Clone. Resolution construction consumes it,
/// so one recovery-open allocation cannot authorize two independent receipt
/// terminalizations. Retaining the backend authority here also keeps the
/// provider and its exclusion resource alive through the receipt mutation.
pub(super) struct MetadataCommitRecoveryOpenAllocationV1 {
    core: Arc<MetadataPendingRecoveryOpenCommandCoreV1>,
    authority: Arc<MetadataOldDispatchExcludedBackendAuthorityV1>,
}

impl MetadataCommitRecoveryOpenAllocationV1 {
    pub(super) fn matches(
        &self,
        planned: &PlannedMetadataCommitV1,
        source: MetadataCommitReceiptDirtySourceV1,
    ) -> bool {
        &self.core.planned == planned
            && self.core.source == source
            && self.authority.planned == *planned
    }
}

impl fmt::Debug for MetadataCommitRecoveryOpenAllocationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCommitRecoveryOpenAllocationV1")
            .field("identity", &"<opaque>")
            .finish()
    }
}

impl MetadataOldDispatchExcludedBackendAuthorityV1 {
    fn matches(
        &self,
        planned: &PlannedMetadataCommitV1,
        provider: &Arc<dyn MetadataProvider>,
        installation: &MetadataOldDispatchExclusionInstallationV1,
    ) -> bool {
        &self.planned == planned
            && Arc::ptr_eq(&self.provider_allocation, provider)
            && installation
                .core
                .as_ref()
                .is_some_and(|core| Arc::ptr_eq(&self.installation_core, core))
    }
}

/// Closed reason why a recovery-open command definitely did not execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataPendingRecoveryOpenNotDispatchedV1 {
    Unsupported,
    Unavailable,
    InvalidBinding,
}

/// Closed backend phase visible to a forwarding runtime bundle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataPendingRecoveryOpenBackendResultV1 {
    OpenedOldDispatchExcluded,
    NotDispatched(MetadataPendingRecoveryOpenNotDispatchedV1),
    OutcomeUnknown,
}

/// Redacted result of consuming a pending-recovery open outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataPendingRecoveryOpenErrorV1 {
    Unsupported,
    UnavailableBeforeEffect,
    InvalidBinding,
    OutcomeUnknown,
}

impl fmt::Display for MetadataPendingRecoveryOpenErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => formatter.write_str("old-dispatch exclusion is unsupported"),
            Self::UnavailableBeforeEffect => {
                formatter.write_str("old-dispatch exclusion is unavailable")
            }
            Self::InvalidBinding => {
                formatter.write_str("old-dispatch exclusion binding is invalid")
            }
            Self::OutcomeUnknown => {
                formatter.write_str("old-dispatch exclusion outcome is unknown")
            }
        }
    }
}

impl std::error::Error for MetadataPendingRecoveryOpenErrorV1 {}

struct MetadataPendingRecoveryOpenCommandCoreV1 {
    planned: PlannedMetadataCommitV1,
    source: MetadataCommitReceiptDirtySourceV1,
    schema: ProviderSchemaV1,
    expected_installation: MetadataOldDispatchExclusionInstallationV1,
}

/// One engine-minted request to reopen the exact planned store while
/// retaining an old-dispatch exclusion fence.
///
/// Phase 1 deliberately exposes no production mint entry point. Phase 2 adds
/// one engine-owned nominal token after exact runtime-bundle validation.
///
/// The command and its claimed phase are deliberately non-Clone. Only the
/// ultimate provider implementation consumes it with
/// [`Self::claim_execution`]; forwarding layers pass it unchanged.
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataPendingRecoveryOpenCommandV1;
///
/// fn clone_command(command: MetadataPendingRecoveryOpenCommandV1) {
///     let _duplicate = command.clone();
/// }
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataPendingRecoveryOpenCommandV1;
///
/// fn forge_literal() {
///     let _forged = MetadataPendingRecoveryOpenCommandV1 { core: todo!() };
/// }
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataPendingRecoveryOpenCommandV1;
///
/// fn double_claim(command: MetadataPendingRecoveryOpenCommandV1) {
///     let _claimed = command.claim_execution();
///     let _second = command.claim_execution();
/// }
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataPendingRecoveryOpenCommandV1;
///
/// fn skip_claim(command: MetadataPendingRecoveryOpenCommandV1) {
///     let _outcome = command.complete_outcome_unknown();
/// }
/// ```
pub struct MetadataPendingRecoveryOpenCommandV1 {
    core: Arc<MetadataPendingRecoveryOpenCommandCoreV1>,
}

impl MetadataPendingRecoveryOpenCommandV1 {
    fn mint(
        planned: &PlannedMetadataCommitV1,
        source: MetadataCommitReceiptDirtySourceV1,
        schema: ProviderSchemaV1,
        expected_installation: MetadataOldDispatchExclusionInstallationV1,
    ) -> Result<(Self, MetadataPendingRecoveryOpenWitnessV1), MetadataPendingRecoveryOpenErrorV1>
    {
        planned
            .validate_binding(planned.store_identity(), planned.frozen_bundle_digest())
            .map_err(|_| MetadataPendingRecoveryOpenErrorV1::InvalidBinding)?;
        if !expected_installation.is_supported() {
            return Err(MetadataPendingRecoveryOpenErrorV1::Unsupported);
        }
        let core = Arc::new(MetadataPendingRecoveryOpenCommandCoreV1 {
            planned: planned.clone(),
            source,
            schema,
            expected_installation,
        });
        Ok((
            Self {
                core: Arc::clone(&core),
            },
            MetadataPendingRecoveryOpenWitnessV1 { core },
        ))
    }

    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn source(&self) -> MetadataCommitReceiptDirtySourceV1 {
        self.core.source
    }

    #[must_use]
    pub fn schema(&self) -> &ProviderSchemaV1 {
        &self.core.schema
    }

    #[must_use]
    pub fn expected_installation(&self) -> &MetadataOldDispatchExclusionInstallationV1 {
        &self.core.expected_installation
    }

    #[must_use]
    pub fn claim_execution(self) -> ClaimedMetadataPendingRecoveryOpenCommandV1 {
        ClaimedMetadataPendingRecoveryOpenCommandV1 { core: self.core }
    }

    #[must_use]
    pub fn reject_before_execution(
        self,
        reason: MetadataPendingRecoveryOpenNotDispatchedV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        MetadataPendingRecoveryOpenOutcomeV1 {
            core: self.core,
            status: MetadataPendingRecoveryOpenOutcomeStatusV1::RejectedBeforeExecution(reason),
        }
    }
}

impl fmt::Debug for MetadataPendingRecoveryOpenCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataPendingRecoveryOpenCommandV1")
            .field("planned", &"<redacted>")
            .field("schema", &"<redacted>")
            .field("expected_installation", &"<opaque>")
            .finish_non_exhaustive()
    }
}

/// Exact engine-held witness for one recovery-open command allocation.
pub struct MetadataPendingRecoveryOpenWitnessV1 {
    core: Arc<MetadataPendingRecoveryOpenCommandCoreV1>,
}

impl fmt::Debug for MetadataPendingRecoveryOpenWitnessV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataPendingRecoveryOpenWitnessV1")
            .field("identity", &"<opaque>")
            .finish()
    }
}

/// Unique claimed phase of one pending-recovery provider open.
pub struct ClaimedMetadataPendingRecoveryOpenCommandV1 {
    core: Arc<MetadataPendingRecoveryOpenCommandCoreV1>,
}

impl ClaimedMetadataPendingRecoveryOpenCommandV1 {
    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn source(&self) -> MetadataCommitReceiptDirtySourceV1 {
        self.core.source
    }

    #[must_use]
    pub fn schema(&self) -> &ProviderSchemaV1 {
        &self.core.schema
    }

    #[must_use]
    pub fn expected_installation(&self) -> &MetadataOldDispatchExclusionInstallationV1 {
        &self.core.expected_installation
    }

    #[must_use]
    pub fn complete_not_dispatched(
        self,
        reason: MetadataPendingRecoveryOpenNotDispatchedV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        MetadataPendingRecoveryOpenOutcomeV1 {
            core: self.core,
            status: MetadataPendingRecoveryOpenOutcomeStatusV1::BackendNotDispatched(reason),
        }
    }

    #[must_use]
    pub fn complete_outcome_unknown(self) -> MetadataPendingRecoveryOpenOutcomeV1 {
        MetadataPendingRecoveryOpenOutcomeV1 {
            core: self.core,
            status: MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown { _held: None },
        }
    }

    pub(crate) fn complete_outcome_unknown_retaining(
        self,
        guard: Arc<dyn Send + Sync>,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        MetadataPendingRecoveryOpenOutcomeV1 {
            core: self.core,
            status: MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown {
                _held: Some(MetadataPendingRecoveryOpenHeldUnknownV1::BackendGuard {
                    _guard: guard,
                }),
            },
        }
    }

    pub(crate) fn complete_opened_old_dispatch_excluded(
        self,
        provider: Arc<dyn MetadataProvider>,
        authority: MetadataOldDispatchExcludedBackendAuthorityV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        let status = if authority.matches(
            &self.core.planned,
            &provider,
            &self.core.expected_installation,
        ) {
            let authority = Arc::new(authority);
            MetadataPendingRecoveryOpenOutcomeStatusV1::Opened(
                MetadataOldDispatchExcludedProviderV1 {
                    core: Arc::clone(&self.core),
                    provider,
                    _authority: authority,
                },
            )
        } else {
            MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown {
                _held: Some(
                    MetadataPendingRecoveryOpenHeldUnknownV1::ProviderAuthority {
                        _provider: provider,
                        _authority: Arc::new(authority),
                    },
                ),
            }
        };
        MetadataPendingRecoveryOpenOutcomeV1 {
            core: self.core,
            status,
        }
    }
}

impl fmt::Debug for ClaimedMetadataPendingRecoveryOpenCommandV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaimedMetadataPendingRecoveryOpenCommandV1")
            .field("planned", &"<redacted>")
            .field("identity", &"<opaque>")
            .finish_non_exhaustive()
    }
}

enum MetadataPendingRecoveryOpenHeldUnknownV1 {
    ProviderAuthority {
        _provider: Arc<dyn MetadataProvider>,
        _authority: Arc<MetadataOldDispatchExcludedBackendAuthorityV1>,
    },
    BackendGuard {
        _guard: Arc<dyn Send + Sync>,
    },
}

enum MetadataPendingRecoveryOpenOutcomeStatusV1 {
    RejectedBeforeExecution(MetadataPendingRecoveryOpenNotDispatchedV1),
    BackendNotDispatched(MetadataPendingRecoveryOpenNotDispatchedV1),
    Opened(MetadataOldDispatchExcludedProviderV1),
    OutcomeUnknown {
        _held: Option<MetadataPendingRecoveryOpenHeldUnknownV1>,
    },
}

/// Closed outcome of one exact pending-recovery provider open.
pub struct MetadataPendingRecoveryOpenOutcomeV1 {
    core: Arc<MetadataPendingRecoveryOpenCommandCoreV1>,
    status: MetadataPendingRecoveryOpenOutcomeStatusV1,
}

impl MetadataPendingRecoveryOpenOutcomeV1 {
    /// Read the backend phase without gaining success-construction authority.
    #[must_use]
    pub fn backend_result_for_forwarding(
        &self,
    ) -> Option<MetadataPendingRecoveryOpenBackendResultV1> {
        match &self.status {
            MetadataPendingRecoveryOpenOutcomeStatusV1::RejectedBeforeExecution(_) => None,
            MetadataPendingRecoveryOpenOutcomeStatusV1::BackendNotDispatched(reason) => Some(
                MetadataPendingRecoveryOpenBackendResultV1::NotDispatched(*reason),
            ),
            MetadataPendingRecoveryOpenOutcomeStatusV1::Opened(_) => {
                Some(MetadataPendingRecoveryOpenBackendResultV1::OpenedOldDispatchExcluded)
            }
            MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown { .. } => {
                Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
            }
        }
    }

    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn source(&self) -> MetadataCommitReceiptDirtySourceV1 {
        self.core.source
    }

    #[must_use]
    pub fn expected_installation(&self) -> &MetadataOldDispatchExclusionInstallationV1 {
        &self.core.expected_installation
    }

    /// Monotonically turn a forwarded success into an unknown outcome.
    ///
    /// The opened provider and its lifetime guard remain owned by the
    /// downgraded outcome until the engine consumes it.
    #[must_use]
    pub fn downgrade_after_forwarding_failure(self) -> Self {
        let status = match self.status {
            MetadataPendingRecoveryOpenOutcomeStatusV1::Opened(opened) => {
                MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown {
                    _held: Some(opened.into_held_unknown()),
                }
            }
            status => status,
        };
        Self {
            core: self.core,
            status,
        }
    }

    /// Consume this exact outcome for the witness minted with its command.
    pub fn into_result_for(
        self,
        witness: MetadataPendingRecoveryOpenWitnessV1,
    ) -> Result<MetadataOldDispatchExcludedProviderV1, MetadataPendingRecoveryOpenErrorV1> {
        if !Arc::ptr_eq(&self.core, &witness.core) {
            return Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown);
        }
        match self.status {
            MetadataPendingRecoveryOpenOutcomeStatusV1::RejectedBeforeExecution(reason)
            | MetadataPendingRecoveryOpenOutcomeStatusV1::BackendNotDispatched(reason) => {
                Err(match reason {
                    MetadataPendingRecoveryOpenNotDispatchedV1::Unsupported => {
                        MetadataPendingRecoveryOpenErrorV1::Unsupported
                    }
                    MetadataPendingRecoveryOpenNotDispatchedV1::Unavailable => {
                        MetadataPendingRecoveryOpenErrorV1::UnavailableBeforeEffect
                    }
                    MetadataPendingRecoveryOpenNotDispatchedV1::InvalidBinding => {
                        MetadataPendingRecoveryOpenErrorV1::InvalidBinding
                    }
                })
            }
            MetadataPendingRecoveryOpenOutcomeStatusV1::Opened(opened) => Ok(opened),
            MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown { .. } => {
                Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
            }
        }
    }
}

impl fmt::Debug for MetadataPendingRecoveryOpenOutcomeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match &self.status {
            MetadataPendingRecoveryOpenOutcomeStatusV1::RejectedBeforeExecution(_) => {
                "rejected-before-execution"
            }
            MetadataPendingRecoveryOpenOutcomeStatusV1::BackendNotDispatched(_) => "not-dispatched",
            MetadataPendingRecoveryOpenOutcomeStatusV1::Opened(_) => "opened-old-dispatch-excluded",
            MetadataPendingRecoveryOpenOutcomeStatusV1::OutcomeUnknown { .. } => "outcome-unknown",
        };
        formatter
            .debug_struct("MetadataPendingRecoveryOpenOutcomeV1")
            .field("planned", &"<redacted>")
            .field("status", &status)
            .finish_non_exhaustive()
    }
}

/// Exact opened provider whose old-dispatch exclusion resource remains held.
///
/// This value is non-Clone. It retains both the exact provider allocation and
/// the backend lifetime guard until receipt recovery consumes or drops it.
/// It implements [`MetadataProvider`] directly and never exposes the raw
/// provider allocation. Captured views and transactions carry the same guard.
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataOldDispatchExcludedProviderV1;
///
/// fn escape_raw_provider(opened: MetadataOldDispatchExcludedProviderV1) {
///     let _raw = opened.provider_arc().clone();
/// }
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataOldDispatchExcludedProviderV1;
///
/// fn borrow_raw_provider(opened: &MetadataOldDispatchExcludedProviderV1) {
///     let _raw = opened.provider();
/// }
/// ```
///
/// The proof is affine: consuming it once prevents minting a second recovery
/// allocation from the same opened value.
///
/// ```compile_fail
/// use nokv_meta::workspace::MetadataOldDispatchExcludedProviderV1;
///
/// fn consume_twice(opened: MetadataOldDispatchExcludedProviderV1) {
///     let _first = opened;
///     let _second = opened;
/// }
/// ```
pub struct MetadataOldDispatchExcludedProviderV1 {
    core: Arc<MetadataPendingRecoveryOpenCommandCoreV1>,
    provider: Arc<dyn MetadataProvider>,
    _authority: Arc<MetadataOldDispatchExcludedBackendAuthorityV1>,
}

impl MetadataOldDispatchExcludedProviderV1 {
    #[must_use]
    pub fn planned(&self) -> &PlannedMetadataCommitV1 {
        &self.core.planned
    }

    #[must_use]
    pub fn installation(&self) -> &MetadataOldDispatchExclusionInstallationV1 {
        &self.core.expected_installation
    }

    pub(super) fn into_recovery_parts_v1(
        self,
    ) -> (
        Arc<dyn MetadataProvider>,
        MetadataCommitRecoveryOpenAllocationV1,
    ) {
        let allocation = MetadataCommitRecoveryOpenAllocationV1 {
            core: Arc::clone(&self.core),
            authority: Arc::clone(&self._authority),
        };
        let guarded_provider: Arc<dyn MetadataProvider> = Arc::new(Self {
            core: self.core,
            provider: self.provider,
            _authority: self._authority,
        });
        (guarded_provider, allocation)
    }

    fn into_held_unknown(self) -> MetadataPendingRecoveryOpenHeldUnknownV1 {
        MetadataPendingRecoveryOpenHeldUnknownV1::ProviderAuthority {
            _provider: self.provider,
            _authority: self._authority,
        }
    }
}

pub(super) fn mint_pending_recovery_open_v1(
    _authority: &MetadataCommitEngineMintAuthorityV1,
    planned: &PlannedMetadataCommitV1,
    source: MetadataCommitReceiptDirtySourceV1,
    schema: ProviderSchemaV1,
    expected_installation: MetadataOldDispatchExclusionInstallationV1,
) -> Result<
    (
        MetadataPendingRecoveryOpenCommandV1,
        MetadataPendingRecoveryOpenWitnessV1,
    ),
    MetadataPendingRecoveryOpenErrorV1,
> {
    MetadataPendingRecoveryOpenCommandV1::mint(planned, source, schema, expected_installation)
}

impl MetadataProvider for MetadataOldDispatchExcludedProviderV1 {
    fn logical_shard_id(&self) -> nokv_types::LogicalShardId {
        self.provider.logical_shard_id()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.provider.capabilities()
    }

    fn validate_runtime(&self) -> Result<(), ProviderError> {
        self.provider.validate_runtime()
    }

    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.provider.get(space, key)
    }

    fn begin_read(
        &self,
        scopes: &[ReadScope],
    ) -> Result<Box<dyn MetadataReadView + 'static>, ProviderError> {
        Ok(Box::new(MetadataOldDispatchExcludedReadViewV1 {
            inner: self.provider.begin_read(scopes)?,
            _authority: Arc::clone(&self._authority),
        }))
    }

    fn begin_write(&self) -> Result<Box<dyn MetadataTransaction + 'static>, ProviderError> {
        Ok(Box::new(MetadataOldDispatchExcludedTransactionV1 {
            inner: self.provider.begin_write()?,
            _authority: Arc::clone(&self._authority),
        }))
    }

    fn diagnostics(&self) -> Option<&dyn ProviderDiagnosticsV1> {
        self.provider.diagnostics()
    }
}

struct MetadataOldDispatchExcludedReadViewV1 {
    inner: Box<dyn MetadataReadView>,
    _authority: Arc<MetadataOldDispatchExcludedBackendAuthorityV1>,
}

impl MetadataReadView for MetadataOldDispatchExcludedReadViewV1 {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.inner.get(space, key)
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        self.inner.scan(request)
    }
}

struct MetadataOldDispatchExcludedTransactionV1 {
    inner: Box<dyn MetadataTransaction>,
    _authority: Arc<MetadataOldDispatchExcludedBackendAuthorityV1>,
}

impl MetadataReadView for MetadataOldDispatchExcludedTransactionV1 {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.inner.get(space, key)
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        self.inner.scan(request)
    }
}

impl MetadataTransaction for MetadataOldDispatchExcludedTransactionV1 {
    fn prefix_is_empty(&self, space: OrderedSpaceId, prefix: &[u8]) -> Result<bool, ProviderError> {
        self.inner.prefix_is_empty(space, prefix)
    }

    fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
        let Self { inner, _authority } = *self;
        let result = inner.commit(plan);
        drop(_authority);
        result
    }
}

impl fmt::Debug for MetadataOldDispatchExcludedProviderV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataOldDispatchExcludedProviderV1")
            .field("planned", &"<redacted>")
            .field("provider", &"<opaque>")
            .field("installation", &"<opaque>")
            .finish_non_exhaustive()
    }
}

/// Provider-factory extension for exact pending-receipt recovery.
///
/// A factory that cannot produce a private backend authority must return an
/// unsupported installation and reject every command before execution.
/// The supported capability is process-local and allocation-scoped; the
/// runtime bundle must retain this exact factory installation together with
/// the receipt store whose frozen digest appears in the command plan.
pub trait MetadataCommitRecoveryFenceFactoryV1: MetadataProviderFactoryV1 {
    fn old_dispatch_exclusion_installation_v1(&self) -> MetadataOldDispatchExclusionInstallationV1;

    fn reopen_pending_with_old_dispatch_excluded_v1(
        &self,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1;
}

#[cfg(feature = "foundationdb-provider")]
impl MetadataCommitRecoveryFenceFactoryV1 for super::provider::FoundationDbProviderFactory {
    fn old_dispatch_exclusion_installation_v1(&self) -> MetadataOldDispatchExclusionInstallationV1 {
        MetadataOldDispatchExclusionInstallationV1::unsupported()
    }

    fn reopen_pending_with_old_dispatch_excluded_v1(
        &self,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        command.reject_before_execution(MetadataPendingRecoveryOpenNotDispatchedV1::Unsupported)
    }
}

#[cfg(test)]
pub(crate) fn mint_pending_recovery_open_for_test_v1(
    planned: &PlannedMetadataCommitV1,
    schema: ProviderSchemaV1,
    expected_installation: MetadataOldDispatchExclusionInstallationV1,
) -> (
    MetadataPendingRecoveryOpenCommandV1,
    MetadataPendingRecoveryOpenWitnessV1,
) {
    MetadataPendingRecoveryOpenCommandV1::mint(
        planned,
        MetadataCommitReceiptDirtySourceV1::Pending,
        schema,
        expected_installation,
    )
    .expect("test command binding is valid")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use nokv_types::{
        CommitVersion, ConsistencyDomainId, LogicalShardId, MetadataAuthorityGeneration,
        MetadataAuthorityId,
    };

    use super::*;
    use crate::workspace::{
        AcknowledgedMetadataFrontier, MetadataCommitPurposeV1, MetadataFrontierPointV1,
        MetadataStoreIdentity,
    };

    fn planned(byte: u8) -> PlannedMetadataCommitV1 {
        let logical_shard_id = LogicalShardId::from_bytes([byte; 16]);
        PlannedMetadataCommitV1::plan_exact(
            MetadataStoreIdentity {
                logical_shard_id,
                authority_id: MetadataAuthorityId::from_bytes([2; 16]),
                authority_generation: MetadataAuthorityGeneration::new(3).unwrap(),
                consistency_domain_id: ConsistencyDomainId::from_bytes([4; 16]),
                profile_fingerprint: [5; 32],
                contract_digest: crate::workspace::workspace_metadata_contract_digest(),
            },
            [6; 32],
            MetadataCommitPurposeV1::Genesis {
                authority_marker_digest: [7; 32],
            },
            MetadataFrontierPointV1::Absent,
            AcknowledgedMetadataFrontier {
                write_sequence: 0,
                commit_version: CommitVersion::new(1).unwrap(),
                recovery_lsn: 0,
                chain_digest: [8; 32],
            },
        )
        .unwrap()
    }

    fn opened_provider(logical_shard_id: LogicalShardId) -> Arc<dyn MetadataProvider> {
        Arc::new(crate::workspace::provider::HoltProvider::open_memory(logical_shard_id).unwrap())
    }

    #[test]
    fn installation_capabilities_are_nominal_and_public_default_is_unsupported() {
        let unsupported = MetadataOldDispatchExclusionInstallationV1::unsupported();
        assert!(!unsupported.is_supported());
        assert_eq!(
            unsupported,
            MetadataOldDispatchExclusionInstallationV1::default()
        );

        let (first, _) = mint_old_dispatch_exclusion_installation_v1();
        let first_clone = first.clone();
        let (second, _) = mint_old_dispatch_exclusion_installation_v1();
        assert!(first.is_supported());
        assert_eq!(first, first_clone);
        assert_ne!(first, second);
        assert!(!format!("{first:?}").contains("0x"));
    }

    #[test]
    fn exact_command_witness_returns_the_bound_provider_and_plan() {
        let planned = planned(9);
        let (installation, authority) = mint_old_dispatch_exclusion_installation_v1();
        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let provider = opened_provider(planned.store_identity().logical_shard_id);
        let backend = authority.bind_opened_provider(&planned, &provider, ());
        let outcome = command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(Arc::clone(&provider), backend);
        assert_eq!(
            outcome.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OpenedOldDispatchExcluded)
        );

        let opened = outcome.into_result_for(witness).unwrap();
        assert_eq!(opened.planned(), &planned);
        assert_eq!(opened.installation(), &installation);
        assert!(Arc::ptr_eq(&opened.provider, &provider));
    }

    #[test]
    fn wrong_provider_or_capability_cannot_complete_success() {
        let planned = planned(10);
        let (installation, authority) = mint_old_dispatch_exclusion_installation_v1();
        let (foreign_installation, foreign_authority) =
            mint_old_dispatch_exclusion_installation_v1();
        let expected_provider = opened_provider(planned.store_identity().logical_shard_id);
        let foreign_provider = opened_provider(planned.store_identity().logical_shard_id);

        let (wrong_provider_command, wrong_provider_witness) =
            mint_pending_recovery_open_for_test_v1(
                &planned,
                crate::workspace::canonical_provider_schema_v1(),
                installation.clone(),
            );
        let backend = authority.bind_opened_provider(&planned, &expected_provider, ());
        let wrong_provider = wrong_provider_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(foreign_provider, backend);
        assert!(matches!(
            wrong_provider.into_result_for(wrong_provider_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));

        let (wrong_capability_command, wrong_capability_witness) =
            mint_pending_recovery_open_for_test_v1(
                &planned,
                crate::workspace::canonical_provider_schema_v1(),
                installation,
            );
        let backend = foreign_authority.bind_opened_provider(&planned, &expected_provider, ());
        let wrong_capability = wrong_capability_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(expected_provider, backend);
        assert!(matches!(
            wrong_capability.into_result_for(wrong_capability_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));
        assert!(foreign_installation.is_supported());
    }

    #[test]
    fn wrong_plan_allocation_witness_and_forwarding_downgrade_fail_closed() {
        let first_plan = planned(11);
        let second_plan = planned(12);
        let (installation, authority) = mint_old_dispatch_exclusion_installation_v1();
        let (wrong_plan_command, wrong_plan_witness) = mint_pending_recovery_open_for_test_v1(
            &second_plan,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let provider = opened_provider(second_plan.store_identity().logical_shard_id);
        let backend = authority.bind_opened_provider(&first_plan, &provider, ());
        let wrong_plan = wrong_plan_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(provider, backend);
        assert!(matches!(
            wrong_plan.into_result_for(wrong_plan_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));

        let (first_command, first_witness) = mint_pending_recovery_open_for_test_v1(
            &first_plan,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let (second_command, second_witness) = mint_pending_recovery_open_for_test_v1(
            &second_plan,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        drop(first_witness);
        drop(second_command);
        let provider = opened_provider(first_plan.store_identity().logical_shard_id);
        let backend = authority.bind_opened_provider(&first_plan, &provider, ());
        let outcome = first_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(provider, backend);
        assert_eq!(outcome.planned(), &first_plan);
        assert!(matches!(
            outcome.into_result_for(second_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));

        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &first_plan,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let guard_drops = Arc::new(AtomicUsize::new(0));
        let provider = opened_provider(first_plan.store_identity().logical_shard_id);
        let backend = authority.bind_opened_provider(
            &first_plan,
            &provider,
            GuardDropSignal(Arc::clone(&guard_drops)),
        );
        let downgraded = command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(provider, backend)
            .downgrade_after_forwarding_failure();
        assert_eq!(guard_drops.load(Ordering::Acquire), 0);
        assert_eq!(
            downgraded.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
        );
        assert!(matches!(
            downgraded.into_result_for(witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));
        assert_eq!(guard_drops.load(Ordering::Acquire), 1);
    }

    struct GuardDropSignal(Arc<AtomicUsize>);

    impl Drop for GuardDropSignal {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn captured_views_and_transactions_retain_the_exclusion_guard() {
        let planned = planned(14);
        let (installation, authority) = mint_old_dispatch_exclusion_installation_v1();
        let space = crate::workspace::provider::all_ordered_spaces()[0];
        let scan = ProviderScan {
            space,
            prefix: Vec::new(),
            start_after: None,
            delimiter: None,
            limit: 0,
        };

        let read_guard_drops = Arc::new(AtomicUsize::new(0));
        let (read_command, read_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let read_provider = opened_provider(planned.store_identity().logical_shard_id);
        let read_backend = authority.bind_opened_provider(
            &planned,
            &read_provider,
            GuardDropSignal(Arc::clone(&read_guard_drops)),
        );
        let read_opened = read_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(read_provider, read_backend)
            .into_result_for(read_witness)
            .unwrap();
        assert!(read_opened.get(space, b"missing").unwrap().is_none());
        #[cfg(feature = "metadata-read-stats")]
        assert!(read_opened
            .diagnostics()
            .expect("Holt diagnostics must remain a borrowed redacted facade")
            .snapshot()
            .is_ok());
        #[cfg(not(feature = "metadata-read-stats"))]
        assert!(read_opened.diagnostics().is_none());
        let read_view = read_opened
            .begin_read(&[ReadScope {
                space,
                prefix: Vec::new(),
            }])
            .unwrap();
        drop(read_opened);
        assert_eq!(read_guard_drops.load(Ordering::Acquire), 0);
        assert!(read_view.get(space, b"missing").unwrap().is_none());
        assert!(read_view.scan(&scan).unwrap().items.is_empty());
        drop(read_view);
        assert_eq!(read_guard_drops.load(Ordering::Acquire), 1);

        let transaction_guard_drops = Arc::new(AtomicUsize::new(0));
        let (transaction_command, transaction_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let transaction_provider = opened_provider(planned.store_identity().logical_shard_id);
        let transaction_backend = authority.bind_opened_provider(
            &planned,
            &transaction_provider,
            GuardDropSignal(Arc::clone(&transaction_guard_drops)),
        );
        let transaction_opened = transaction_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(transaction_provider, transaction_backend)
            .into_result_for(transaction_witness)
            .unwrap();
        let transaction = transaction_opened.begin_write().unwrap();
        drop(transaction_opened);
        assert_eq!(transaction_guard_drops.load(Ordering::Acquire), 0);
        assert!(transaction.get(space, b"missing").unwrap().is_none());
        assert!(transaction.scan(&scan).unwrap().items.is_empty());
        assert!(transaction.prefix_is_empty(space, b"").unwrap());
        assert_eq!(
            transaction.commit(AtomicPlan::default()).unwrap(),
            AtomicCommitOutcome::Committed
        );
        assert_eq!(transaction_guard_drops.load(Ordering::Acquire), 1);
    }

    #[test]
    fn consuming_recovery_parts_keeps_the_guard_in_both_affine_halves() {
        let planned = planned(15);
        let (installation, authority) = mint_old_dispatch_exclusion_installation_v1();

        let allocation_guard_drops = Arc::new(AtomicUsize::new(0));
        let (allocation_command, allocation_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let allocation_provider = opened_provider(planned.store_identity().logical_shard_id);
        let allocation_backend = authority.bind_opened_provider(
            &planned,
            &allocation_provider,
            GuardDropSignal(Arc::clone(&allocation_guard_drops)),
        );
        let allocation_opened = allocation_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(allocation_provider, allocation_backend)
            .into_result_for(allocation_witness)
            .unwrap();
        let (guarded_provider, recovery_allocation) = allocation_opened.into_recovery_parts_v1();
        drop(guarded_provider);
        assert_eq!(allocation_guard_drops.load(Ordering::Acquire), 0);
        drop(recovery_allocation);
        assert_eq!(allocation_guard_drops.load(Ordering::Acquire), 1);

        let provider_guard_drops = Arc::new(AtomicUsize::new(0));
        let (provider_command, provider_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let provider = opened_provider(planned.store_identity().logical_shard_id);
        let backend = authority.bind_opened_provider(
            &planned,
            &provider,
            GuardDropSignal(Arc::clone(&provider_guard_drops)),
        );
        let opened = provider_command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(provider, backend)
            .into_result_for(provider_witness)
            .unwrap();
        let (guarded_provider, recovery_allocation) = opened.into_recovery_parts_v1();
        drop(recovery_allocation);
        assert_eq!(provider_guard_drops.load(Ordering::Acquire), 0);
        drop(guarded_provider);
        assert_eq!(provider_guard_drops.load(Ordering::Acquire), 1);
    }

    #[test]
    fn debug_output_redacts_plan_provider_and_allocation_bindings() {
        let planned = planned(13);
        let (installation, authority) = mint_old_dispatch_exclusion_installation_v1();
        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let command_debug = format!("{command:?}");
        assert!(command_debug.contains("<redacted>"));
        assert!(!command_debug.contains(&format!("{:?}", planned.canonical_digest())));

        let provider = opened_provider(planned.store_identity().logical_shard_id);
        let backend = authority.bind_opened_provider(&planned, &provider, ());
        let outcome = command
            .claim_execution()
            .complete_opened_old_dispatch_excluded(provider, backend);
        let outcome_debug = format!("{outcome:?}");
        assert!(outcome_debug.contains("<redacted>"));
        let opened = outcome.into_result_for(witness).unwrap();
        let opened_debug = format!("{opened:?}");
        assert!(opened_debug.contains("<opaque>"));
        assert!(!opened_debug.contains(&format!("{:?}", planned.canonical_digest())));
    }
}
