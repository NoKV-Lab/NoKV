/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Immutable process-local registry for metadata runtime profiles.
//!
//! This module deliberately owns selection and lifecycle admission only. It
//! contains no provider configuration, connection material, control-plane
//! record, dynamic loading hook, or global registry. Exact commit receipts and
//! runtime guards remain server-owned concerns outside the provider SPI.

use std::any::TypeId;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;

use nokv_control::{ConsistencyDomainId, LogicalShardLease, MetadataProviderProfileId};
use nokv_meta::provider::admission::{admit_provider_offer_v1, ProviderAdmissionReportV1};
use nokv_meta::provider::v1::{
    CreateRecoveryIntentV1, MetadataProvider, MetadataProviderFactoryV1, ProviderContractOfferV1,
    ProviderCreateRequestV1, ProviderError, ProviderOperationV1, ProviderReopenRequestV1,
    ProviderSchemaV1,
};
use nokv_meta::workspace::{
    canonical_provider_schema_v1, AgentMetadataError, AgentMetadataStore,
    MetadataCommitReceiptErrorV1, MetadataCommitReceiptMutationBackendResultV1,
    MetadataCommitReceiptMutationNotDispatchedV1, MetadataCommitReceiptPersistBackendResultV1,
    MetadataCommitReceiptPersistCommandV1, MetadataCommitReceiptPersistNotDispatchedV1,
    MetadataCommitReceiptPersistOutcomeV1, MetadataCommitReceiptPoisonCommandV1,
    MetadataCommitReceiptPoisonOutcomeV1, MetadataCommitReceiptQualificationV1,
    MetadataCommitReceiptResolveCommandV1, MetadataCommitReceiptResolveOutcomeV1,
    MetadataCommitReceiptStateV1, MetadataCommitReceiptStoreV1,
    MetadataCommitRecoveryFenceFactoryV1, MetadataOldDispatchExclusionInstallationV1,
    MetadataPendingRecoveryOpenCommandV1, MetadataPendingRecoveryOpenNotDispatchedV1,
    MetadataPendingRecoveryOpenOutcomeV1, MetadataStoreCreateModeV1, MetadataStoreIdentity,
    PlannedMetadataCommitV1,
};
#[cfg(test)]
use nokv_meta::workspace::{
    MetadataCommitReceiptDirtySourceV1, MetadataCommitReceiptPoisonReasonV1,
    MetadataCommitResolutionBasisV1,
};
use nokv_types::{MetadataContractDigest, SHA256_BYTES};

/// Closed reason why a runtime is not admitted to the complete metadata
/// command surface.
///
/// Production `Qualified` status must be computed by trusted server admission
/// from the canonical `nokv-meta` schema and requirements applied to the exact
/// provider offer. A provider or runtime factory must never self-qualify, and
/// profile-name checks are not a substitute for that calculation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QualificationCode {
    CompleteCommandSurfaceUnproven,
}

/// Fail-closed qualification state captured in a descriptor snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeQualification {
    Qualified,
    NotQualified(QualificationCode),
}

/// Server-owned durability boundary for owner-session receipts.
///
/// This is lifecycle metadata, not a provider SPI argument. In particular, an
/// external journal, exact commit receipt, or runtime guard must not be passed
/// through a metadata provider factory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerReceiptMode {
    ProviderDurable,
    ExternalOwnerJournal,
}

/// Atomicity-domain shape bound by one runtime descriptor.
///
/// `ShardLocal` derives a distinct domain from the logical shard and authority
/// identity. `Shared` carries one already-secret-free stable domain identity
/// shared by every authority opened through the descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeConsistencyDomain {
    ShardLocal,
    Shared(ConsistencyDomainId),
}

/// Storage-neutral intent used to open one admitted metadata installation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenIntent {
    CreateFresh,
    ReconcilePreparedCreate,
    ReopenExisting,
}

/// Exact owner/open transition requested by bootstrap.
///
/// Prepared paths remain distinct because providers can support first-create,
/// direct successor-create, and resume-or-successor reconciliation
/// independently.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LifecycleTransition {
    FreshCreate = 0,
    ExactResume = 1,
    SuccessorReopen = 2,
    PreparedFirstCreate = 3,
    PreparedSuccessorCreate = 4,
    PreparedResumeOrSuccessor = 5,
}

impl LifecycleTransition {
    const ALL: [Self; 6] = [
        Self::FreshCreate,
        Self::ExactResume,
        Self::SuccessorReopen,
        Self::PreparedFirstCreate,
        Self::PreparedSuccessorCreate,
        Self::PreparedResumeOrSuccessor,
    ];

    const fn bit(self) -> u8 {
        1_u8 << (self as u8)
    }

    const fn open_intent(self) -> OpenIntent {
        match self {
            Self::FreshCreate => OpenIntent::CreateFresh,
            Self::ExactResume | Self::SuccessorReopen => OpenIntent::ReopenExisting,
            Self::PreparedFirstCreate
            | Self::PreparedSuccessorCreate
            | Self::PreparedResumeOrSuccessor => OpenIntent::ReconcilePreparedCreate,
        }
    }
}

/// Exact closed set of lifecycle transitions offered by one runtime.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LifecycleCapabilities {
    transition_bits: u8,
    owner_receipt_mode: OwnerReceiptMode,
}

impl LifecycleCapabilities {
    /// Build a capability set from closed transition discriminants.
    #[must_use]
    pub fn new(owner_receipt_mode: OwnerReceiptMode, transitions: &[LifecycleTransition]) -> Self {
        let transition_bits = transitions
            .iter()
            .fold(0_u8, |bits, transition| bits | transition.bit());
        Self {
            transition_bits,
            owner_receipt_mode,
        }
    }

    #[must_use]
    pub const fn owner_receipt_mode(self) -> OwnerReceiptMode {
        self.owner_receipt_mode
    }

    #[must_use]
    pub const fn supports(self, transition: LifecycleTransition) -> bool {
        self.transition_bits & transition.bit() != 0
    }

    /// Classify the exact open/owner cross-product without provider-specific
    /// branches or implicit fallback.
    pub fn classify_bootstrap(
        self,
        open_intent: OpenIntent,
        transition: LifecycleTransition,
    ) -> Result<BootstrapAdmission, AdmissionCode> {
        if open_intent != transition.open_intent() {
            return Err(AdmissionCode::OpenTransitionMismatch);
        }
        if !self.supports(transition) {
            return Err(AdmissionCode::TransitionUnsupported);
        }
        Ok(BootstrapAdmission {
            transition,
            owner_receipt_mode: self.owner_receipt_mode,
        })
    }
}

impl fmt::Debug for LifecycleCapabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let transitions: Vec<_> = LifecycleTransition::ALL
            .into_iter()
            .filter(|transition| self.supports(*transition))
            .collect();
        formatter
            .debug_struct("LifecycleCapabilities")
            .field("transitions", &transitions)
            .field("owner_receipt_mode", &self.owner_receipt_mode)
            .finish()
    }
}

/// Positive, exact bootstrap classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapAdmission {
    transition: LifecycleTransition,
    owner_receipt_mode: OwnerReceiptMode,
}

impl BootstrapAdmission {
    #[must_use]
    pub const fn transition(self) -> LifecycleTransition {
        self.transition
    }

    #[must_use]
    pub const fn owner_receipt_mode(self) -> OwnerReceiptMode {
        self.owner_receipt_mode
    }
}

/// Closed lifecycle admission rejection code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionCode {
    OpenTransitionMismatch,
    TransitionUnsupported,
    PlannedOwnerAdmissionNotQualifiedV1,
    ExactResumeNotQualifiedV1,
    PreparedResumeOrSuccessorNotQualifiedV1,
}

impl fmt::Display for AdmissionCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenTransitionMismatch => formatter.write_str("open_transition_mismatch"),
            Self::TransitionUnsupported => formatter.write_str("transition_unsupported"),
            Self::PlannedOwnerAdmissionNotQualifiedV1 => {
                formatter.write_str("planned_owner_admission_not_qualified_v1")
            }
            Self::ExactResumeNotQualifiedV1 => {
                formatter.write_str("exact_resume_admission_not_qualified_v1")
            }
            Self::PreparedResumeOrSuccessorNotQualifiedV1 => {
                formatter.write_str("prepared_resume_or_successor_admission_not_qualified_v1")
            }
        }
    }
}

/// Fail-closed bootstrap admission failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeAdmissionError {
    NotQualified(QualificationCode),
    Rejected(AdmissionCode),
}

impl fmt::Display for RuntimeAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotQualified(code) => {
                write!(formatter, "metadata runtime is not qualified ({code:?})")
            }
            Self::Rejected(code) => {
                write!(
                    formatter,
                    "metadata runtime lifecycle admission rejected ({code:?})"
                )
            }
        }
    }
}

impl std::error::Error for RuntimeAdmissionError {}

/// Immutable, secret-free identity and admission contract for one runtime.
///
/// Construction always obtains `schema` from the canonical `nokv-meta` facade
/// accessor and derives qualification through generic provider admission. The
/// exact schema, provider offer, and typed admission report are retained so a
/// later factory change cannot be mistaken for the admitted contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeDescriptor {
    profile_id: MetadataProviderProfileId,
    profile_fingerprint: [u8; SHA256_BYTES],
    schema: ProviderSchemaV1,
    provider_offer: ProviderContractOfferV1,
    provider_admission: ProviderAdmissionReportV1,
    lifecycle: LifecycleCapabilities,
    consistency_domain: RuntimeConsistencyDomain,
    qualification: RuntimeQualification,
}

impl RuntimeDescriptor {
    pub fn new(
        profile_id: MetadataProviderProfileId,
        profile_fingerprint: [u8; SHA256_BYTES],
        provider_offer: ProviderContractOfferV1,
        lifecycle: LifecycleCapabilities,
        consistency_domain: RuntimeConsistencyDomain,
    ) -> Result<Self, RuntimeDescriptorError> {
        if profile_fingerprint.iter().all(|byte| *byte == 0) {
            return Err(RuntimeDescriptorError::ZeroProfileFingerprint);
        }
        if matches!(
            consistency_domain,
            RuntimeConsistencyDomain::Shared(domain)
                if domain.as_bytes().iter().all(|byte| *byte == 0)
        ) {
            return Err(RuntimeDescriptorError::ZeroSharedConsistencyDomain);
        }
        let schema = canonical_provider_schema_v1();
        let provider_admission = admit_provider_offer_v1(&schema, &provider_offer);
        let qualification = if provider_admission.is_qualified() {
            RuntimeQualification::Qualified
        } else {
            RuntimeQualification::NotQualified(QualificationCode::CompleteCommandSurfaceUnproven)
        };
        Ok(Self {
            profile_id,
            profile_fingerprint,
            schema,
            provider_offer,
            provider_admission,
            lifecycle,
            consistency_domain,
            qualification,
        })
    }

    #[must_use]
    pub fn profile_id(&self) -> &MetadataProviderProfileId {
        &self.profile_id
    }

    #[must_use]
    pub const fn profile_fingerprint(&self) -> &[u8; SHA256_BYTES] {
        &self.profile_fingerprint
    }

    #[must_use]
    pub const fn schema(&self) -> &ProviderSchemaV1 {
        &self.schema
    }

    #[must_use]
    pub const fn contract_digest(&self) -> MetadataContractDigest {
        self.schema.workspace_contract_digest()
    }

    #[must_use]
    pub const fn provider_offer(&self) -> ProviderContractOfferV1 {
        self.provider_offer
    }

    #[must_use]
    pub const fn provider_admission(&self) -> &ProviderAdmissionReportV1 {
        &self.provider_admission
    }

    #[must_use]
    pub const fn lifecycle(&self) -> LifecycleCapabilities {
        self.lifecycle
    }

    #[must_use]
    pub const fn consistency_domain(&self) -> RuntimeConsistencyDomain {
        self.consistency_domain
    }

    #[must_use]
    pub const fn qualification(&self) -> RuntimeQualification {
        self.qualification
    }

    /// Apply qualification before the lifecycle cross-product classifier.
    pub fn classify_bootstrap(
        &self,
        open_intent: OpenIntent,
        transition: LifecycleTransition,
    ) -> Result<BootstrapAdmission, RuntimeAdmissionError> {
        if let RuntimeQualification::NotQualified(code) = self.qualification {
            return Err(RuntimeAdmissionError::NotQualified(code));
        }
        match self.lifecycle.classify_bootstrap(open_intent, transition) {
            Ok(admission) => Ok(admission),
            Err(code) => Err(RuntimeAdmissionError::Rejected(code)),
        }
    }
}

/// Descriptor construction failure. It carries no rejected bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeDescriptorError {
    ZeroProfileFingerprint,
    ZeroSharedConsistencyDomain,
}

impl fmt::Display for RuntimeDescriptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroProfileFingerprint => {
                formatter.write_str("metadata runtime profile fingerprint must not be all zeroes")
            }
            Self::ZeroSharedConsistencyDomain => formatter
                .write_str("metadata runtime shared consistency domain must not be all zeroes"),
        }
    }
}

impl std::error::Error for RuntimeDescriptorError {}

/// Closed runtime/lifecycle validation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeLifecycleValidationError {
    Rejected,
    Poisoned,
}

impl fmt::Display for RuntimeLifecycleValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected => formatter.write_str("metadata runtime validation was rejected"),
            Self::Poisoned => formatter.write_str("metadata runtime validator is poisoned"),
        }
    }
}

impl std::error::Error for RuntimeLifecycleValidationError {}

/// Storage-neutral process/runtime validation kept outside provider SPI.
pub trait RuntimeLifecycleValidator: Send + Sync {
    fn validate(&self) -> Result<(), RuntimeLifecycleValidationError>;

    fn poison(&self);
}

/// Closed failure from the durable, release-only owner receipt view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerReleaseReceiptError {
    UnsupportedV1,
    BindingDriftV1,
    PersistenceRejectedV1,
}

impl fmt::Display for OwnerReleaseReceiptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedV1 => formatter.write_str("owner_release_receipt_unsupported_v1"),
            Self::BindingDriftV1 => formatter.write_str("owner_release_receipt_binding_drift_v1"),
            Self::PersistenceRejectedV1 => {
                formatter.write_str("owner_release_receipt_persistence_rejected_v1")
            }
        }
    }
}

impl std::error::Error for OwnerReleaseReceiptError {}

/// Durable release-only view bound to the same concrete runtime allocation as
/// provider open, exact commit receipt, and lifecycle validation.
///
/// Implementations with mutable configuration must compare `expected` and
/// perform the durable write in one atomic configuration critical section.
/// Pre/post sampling is insufficient because an A-to-B-to-A change could write
/// the exact lease into B's receipt while both samples observe A.
pub trait OwnerReleaseReceipt: Send + Sync {
    type Binding: Clone + Eq + Send + Sync + 'static;

    fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError>;

    fn preflight_owner_release_at_binding(
        &self,
        expected: &Self::Binding,
    ) -> Result<(), OwnerReleaseReceiptError>;

    fn persist_owner_releasing_at_binding(
        &self,
        expected: &Self::Binding,
        lease: &LogicalShardLease,
    ) -> Result<(), OwnerReleaseReceiptError>;
}

/// One atomic snapshot of the exact provider offer and installation selected
/// by a runtime factory.
///
/// The fields stay private and the type deliberately does not implement
/// `Debug` or `Display`: installation identities may contain process-local
/// locators that must not enter errors or diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeProviderBinding<I> {
    offer: ProviderContractOfferV1,
    installation: I,
    recovery_fence_installation: MetadataOldDispatchExclusionInstallationV1,
}

impl<I> RuntimeProviderBinding<I> {
    #[must_use]
    pub const fn new(offer: ProviderContractOfferV1, installation: I) -> Self {
        Self {
            offer,
            installation,
            recovery_fence_installation: MetadataOldDispatchExclusionInstallationV1::unsupported(),
        }
    }

    #[must_use]
    pub const fn with_recovery_fence_installation(
        offer: ProviderContractOfferV1,
        installation: I,
        recovery_fence_installation: MetadataOldDispatchExclusionInstallationV1,
    ) -> Self {
        Self {
            offer,
            installation,
            recovery_fence_installation,
        }
    }

    #[must_use]
    pub const fn offer(&self) -> ProviderContractOfferV1 {
        self.offer
    }

    #[must_use]
    pub const fn installation(&self) -> &I {
        &self.installation
    }

    #[must_use]
    pub const fn recovery_fence_installation(&self) -> &MetadataOldDispatchExclusionInstallationV1 {
        &self.recovery_fence_installation
    }
}

/// Provider factory whose configured offer and installation can be
/// snapshotted atomically without opening or touching provider state.
///
/// Implementations are trusted process-composition code. The binding is used
/// to reject accidental offer/factory/locator drift and bundle mix-and-match;
/// it is not a defence against a malicious provider implementation that lies
/// about its own configuration. Snapshot inspection must not perform backend
/// I/O. Exact-bound create/reopen must compare the complete binding and start
/// backend touch under the same configuration lock or immutable configuration
/// instance, so neither offer nor installation can change in between.
pub trait RuntimeProviderFactory: MetadataProviderFactoryV1 {
    type InstallationIdentity: Clone + Eq + Send + Sync + 'static;

    fn binding_snapshot(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<RuntimeProviderBinding<Self::InstallationIdentity>, ProviderError>;

    /// Compare `expected_binding` and execute create as one exact-bound
    /// configuration operation. If either component differs, this method must
    /// return before touching backend, path, or runtime state.
    fn create_at_binding(
        &self,
        expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError>;

    /// Exact-bound counterpart of [`Self::create_at_binding`].
    fn reopen_at_binding(
        &self,
        expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError>;

    /// Exact-bound recovery counterpart. Unsupported installations must reject
    /// the command before claiming it or touching provider state.
    fn reopen_pending_with_old_dispatch_excluded_at_binding_v1(
        &self,
        _expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        command.reject_before_execution(MetadataPendingRecoveryOpenNotDispatchedV1::Unsupported)
    }
}

/// One concrete external-owner bundle used for provider open, durable exact
/// commit receipts, lifecycle validation, and exact owner release.
///
/// A public caller supplies one owned value rather than three independently
/// erasable trait objects. [`ResolvedRuntime::external_owner_journal`] then
/// seals that value in one process-local `Arc`, so its concrete type and exact
/// allocation participate in the runtime binding.
pub trait ExternalOwnerRuntimeBundle:
    RuntimeProviderFactory
    + MetadataCommitReceiptStoreV1
    + RuntimeLifecycleValidator
    + OwnerReleaseReceipt
    + Send
    + Sync
{
}

impl<T> ExternalOwnerRuntimeBundle for T where
    T: RuntimeProviderFactory
        + MetadataCommitReceiptStoreV1
        + RuntimeLifecycleValidator
        + OwnerReleaseReceipt
        + Send
        + Sync
{
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct RuntimeBundleIdentity {
    concrete_type: TypeId,
    data_address: usize,
}

impl RuntimeBundleIdentity {
    fn of<T: 'static>(bundle: &Arc<T>) -> Self {
        Self {
            concrete_type: TypeId::of::<T>(),
            data_address: Arc::as_ptr(bundle) as *const () as usize,
        }
    }

    fn matches<T: ?Sized>(&self, view: &Arc<T>) -> bool {
        self.data_address == Arc::as_ptr(view) as *const () as usize
    }
}

trait ResolvedRuntimeBundle: Send + Sync {
    fn validate_current(&self, schema: &ProviderSchemaV1) -> Result<(), RuntimeFactoryError>;

    fn bundle_identity(&self) -> RuntimeBundleIdentity;

    fn validate_lifecycle(&self) -> Result<(), RuntimeLifecycleValidationError>;

    fn poison_lifecycle(&self);

    fn preflight_owner_release(&self) -> Result<(), OwnerReleaseReceiptError>;

    fn persist_owner_releasing(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<(), OwnerReleaseReceiptError>;

    fn open_store(
        self: Arc<Self>,
        intent: OpenIntent,
        identity: MetadataStoreIdentity,
    ) -> Result<AgentMetadataStore, RuntimeOpenError>;
}

/// One exact process-local runtime bundle.
///
/// All fields stay private so bootstrap cannot combine a provider factory from
/// one configured registry entry with another entry's commit receipt or
/// lifecycle validator. A configured entry may be scoped to one server
/// composition and one local provider locator; that locator never enters the
/// descriptor or durable control state.
pub struct ResolvedRuntime {
    descriptor: RuntimeDescriptor,
    runtime_bundle: Arc<dyn ResolvedRuntimeBundle>,
    bundle_identity: RuntimeBundleIdentity,
}

impl ResolvedRuntime {
    fn assemble<B>(descriptor: RuntimeDescriptor, bundle: B) -> Result<Self, RuntimeFactoryError>
    where
        B: ExternalOwnerRuntimeBundle + 'static,
    {
        let bundle = Arc::new(FrozenRuntimeBundle::new(
            bundle,
            descriptor.provider_offer(),
            descriptor.schema(),
        )?);
        let bundle_identity = RuntimeBundleIdentity::of(&bundle);
        let runtime_bundle: Arc<dyn ResolvedRuntimeBundle> = bundle;
        if runtime_bundle.bundle_identity() != bundle_identity
            || !bundle_identity.matches(&runtime_bundle)
        {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::RuntimeBundleIdentityMismatch,
            ));
        }
        let resolved = Self {
            descriptor,
            runtime_bundle,
            bundle_identity,
        };
        resolved.validate_provider_binding()?;
        Ok(resolved)
    }

    #[must_use]
    pub const fn descriptor(&self) -> &RuntimeDescriptor {
        &self.descriptor
    }

    /// Assemble a provider-durable mode from one concrete bundle that also
    /// owns a durable exact commit receipt. Provider atomicity alone is not a
    /// substitute for the external logical-operation receipt.
    pub fn provider_durable<B>(
        descriptor: RuntimeDescriptor,
        bundle: B,
    ) -> Result<Self, RuntimeFactoryError>
    where
        B: ExternalOwnerRuntimeBundle + 'static,
    {
        if descriptor.lifecycle().owner_receipt_mode() != OwnerReceiptMode::ProviderDurable {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::OwnerReceiptModeMismatch,
            ));
        }
        Self::assemble(descriptor, bundle)
    }

    /// Seal one concrete external-owner bundle as the only source of provider
    /// open, durable exact commit receipt, and lifecycle-validation views.
    ///
    /// A provider factory and an independently allocated receipt store cannot
    /// be supplied as separate arguments:
    ///
    /// ```compile_fail
    /// # use nokv_server::{ResolvedRuntime, RuntimeDescriptor};
    /// # fn cannot_mix<B, R>(descriptor: RuntimeDescriptor, factory: B, receipt: R) {
    /// let _ = ResolvedRuntime::external_owner_journal(descriptor, factory, receipt);
    /// # }
    /// ```
    pub fn external_owner_journal<B>(
        descriptor: RuntimeDescriptor,
        bundle: B,
    ) -> Result<Self, RuntimeFactoryError>
    where
        B: ExternalOwnerRuntimeBundle + 'static,
    {
        if descriptor.lifecycle().owner_receipt_mode() != OwnerReceiptMode::ExternalOwnerJournal {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::OwnerReceiptModeMismatch,
            ));
        }
        Self::assemble(descriptor, bundle)
    }

    pub(crate) fn validate_provider_binding(&self) -> Result<(), RuntimeFactoryError> {
        self.runtime_bundle
            .validate_current(self.descriptor.schema())
    }

    pub(crate) fn validate_lifecycle(&self) -> Result<(), RuntimeLifecycleValidationError> {
        self.runtime_bundle.validate_lifecycle()
    }

    pub(crate) fn poison_lifecycle(&self) {
        self.runtime_bundle.poison_lifecycle();
    }

    pub(crate) fn preflight_owner_release(&self) -> Result<(), OwnerReleaseReceiptError> {
        self.runtime_bundle.preflight_owner_release()
    }

    pub(crate) fn persist_owner_releasing(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<(), OwnerReleaseReceiptError> {
        self.runtime_bundle.persist_owner_releasing(lease)
    }

    /// Open through the generic metadata facade only after bootstrap has
    /// completed placement, authority, migration, frontier, qualification,
    /// and lifecycle admission.
    pub(crate) fn open_store(
        &self,
        intent: OpenIntent,
        identity: MetadataStoreIdentity,
    ) -> Result<AgentMetadataStore, RuntimeOpenError> {
        Arc::clone(&self.runtime_bundle).open_store(intent, identity)
    }
}

/// Closed failure returned by generic runtime opening.
#[derive(Debug)]
pub(crate) enum RuntimeOpenError {
    Runtime(RuntimeFactoryError),
    Metadata(AgentMetadataError),
}

impl fmt::Display for RuntimeOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Metadata(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RuntimeOpenError {}

#[cfg(test)]
pub(crate) struct RecordingCommitReceiptStoreV1 {
    digest: [u8; SHA256_BYTES],
    state: std::sync::Mutex<Option<MetadataCommitReceiptStateV1>>,
    last_plan: std::sync::Mutex<Option<PlannedMetadataCommitV1>>,
    reject_load: AtomicBool,
    reject_next_persist_before_effect: AtomicBool,
    recover_next_persist_after_effect: AtomicBool,
    reject_resolve: AtomicBool,
    reject_next_poison_before_effect: AtomicBool,
    load_calls: std::sync::atomic::AtomicUsize,
    persist_calls: std::sync::atomic::AtomicUsize,
    resolve_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl RecordingCommitReceiptStoreV1 {
    pub(crate) fn new(digest: [u8; SHA256_BYTES]) -> Self {
        assert!(digest.iter().any(|byte| *byte != 0));
        Self {
            digest,
            state: std::sync::Mutex::new(None),
            last_plan: std::sync::Mutex::new(None),
            reject_load: AtomicBool::new(false),
            reject_next_persist_before_effect: AtomicBool::new(false),
            recover_next_persist_after_effect: AtomicBool::new(false),
            reject_resolve: AtomicBool::new(false),
            reject_next_poison_before_effect: AtomicBool::new(false),
            load_calls: std::sync::atomic::AtomicUsize::new(0),
            persist_calls: std::sync::atomic::AtomicUsize::new(0),
            resolve_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn reject_load(&self, reject: bool) {
        self.reject_load.store(reject, AtomicOrdering::Release);
    }

    pub(crate) fn reject_resolve(&self, reject: bool) {
        self.reject_resolve.store(reject, AtomicOrdering::Release);
    }

    pub(crate) fn reject_next_persist_before_effect(&self) {
        self.reject_next_persist_before_effect
            .store(true, AtomicOrdering::Release);
    }

    pub(crate) fn recover_next_persist_after_effect(&self) {
        self.recover_next_persist_after_effect
            .store(true, AtomicOrdering::Release);
    }

    pub(crate) fn reject_next_poison_before_effect(&self) {
        self.reject_next_poison_before_effect
            .store(true, AtomicOrdering::Release);
    }

    pub(crate) fn load_calls(&self) -> usize {
        self.load_calls.load(AtomicOrdering::SeqCst)
    }

    pub(crate) fn persist_calls(&self) -> usize {
        self.persist_calls.load(AtomicOrdering::SeqCst)
    }

    pub(crate) fn resolve_calls(&self) -> usize {
        self.resolve_calls.load(AtomicOrdering::SeqCst)
    }

    pub(crate) fn seed_exact_frontier_for_reopen(&self, store_identity: MetadataStoreIdentity) {
        let mut state = self.state.lock().unwrap();
        assert!(state.is_none());
        *state = Some(MetadataCommitReceiptStateV1::Clean {
            store_identity,
            frozen_bundle_digest: self.digest,
            frontier: nokv_meta::workspace::MetadataFrontierPointV1::Exact(
                nokv_meta::workspace::AcknowledgedMetadataFrontier {
                    write_sequence: 0,
                    commit_version: nokv_types::CommitVersion::new(1).unwrap(),
                    recovery_lsn: 0,
                    chain_digest: [0x5d; SHA256_BYTES],
                },
            ),
        });
    }

    pub(crate) fn last_plan(&self) -> PlannedMetadataCommitV1 {
        self.last_plan
            .lock()
            .unwrap()
            .clone()
            .expect("a commit plan must have been persisted")
    }
}

#[cfg(test)]
impl Default for RecordingCommitReceiptStoreV1 {
    fn default() -> Self {
        Self::new([0xa7; SHA256_BYTES])
    }
}

#[cfg(test)]
impl MetadataCommitReceiptStoreV1 for RecordingCommitReceiptStoreV1 {
    fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
        MetadataCommitReceiptQualificationV1::Durable
    }

    fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
        self.digest
    }

    fn load_commit_receipt_v1(
        &self,
        store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
        self.load_calls.fetch_add(1, AtomicOrdering::SeqCst);
        if self.reject_load.load(AtomicOrdering::Acquire) {
            return Err(MetadataCommitReceiptErrorV1::Unavailable);
        }
        let mut state = self.state.lock().unwrap();
        let durable = state.get_or_insert_with(|| MetadataCommitReceiptStateV1::Clean {
            store_identity,
            frozen_bundle_digest: self.digest,
            frontier: nokv_meta::workspace::MetadataFrontierPointV1::Absent,
        });
        let durable_identity = match durable {
            MetadataCommitReceiptStateV1::Clean { store_identity, .. } => *store_identity,
            MetadataCommitReceiptStateV1::Pending(planned)
            | MetadataCommitReceiptStateV1::PoisonedSettled(planned)
            | MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => planned.store_identity(),
            MetadataCommitReceiptStateV1::UntrackedStandalone => {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
        };
        if durable_identity != store_identity {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        Ok(durable.clone())
    }

    fn persist_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptPersistCommandV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        let command = command.claim_execution();
        let result = (|| {
            self.persist_calls.fetch_add(1, AtomicOrdering::SeqCst);
            if self
                .reject_next_persist_before_effect
                .swap(false, AtomicOrdering::AcqRel)
            {
                return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                    MetadataCommitReceiptPersistNotDispatchedV1::Unavailable,
                );
            }
            let planned = command.planned();
            if planned.frozen_bundle_digest() != self.digest {
                return MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                    MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
                );
            }
            let mut state = self.state.lock().unwrap();
            match state.as_ref() {
                Some(MetadataCommitReceiptStateV1::Clean {
                    store_identity,
                    frozen_bundle_digest,
                    frontier,
                }) if *store_identity == planned.store_identity()
                    && *frozen_bundle_digest == self.digest
                    && *frontier == planned.prior() =>
                {
                    *state = Some(MetadataCommitReceiptStateV1::Pending(planned.clone()));
                    *self.last_plan.lock().unwrap() = Some(planned.clone());
                    if self
                        .recover_next_persist_after_effect
                        .swap(false, AtomicOrdering::AcqRel)
                    {
                        MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired
                    } else {
                        MetadataCommitReceiptPersistBackendResultV1::Persisted
                    }
                }
                _ => MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                    MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
                ),
            }
        })();
        command.complete(result)
    }

    fn resolve_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptResolveCommandV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        let command = command.claim_execution();
        let result = (|| {
            self.resolve_calls.fetch_add(1, AtomicOrdering::SeqCst);
            if self.reject_resolve.load(AtomicOrdering::Acquire) {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::Unavailable,
                );
            }
            let planned = command.planned();
            let resolution = command.resolution();
            let evidence = resolution.purpose_evidence_digest();
            let mut state = self.state.lock().unwrap();
            let Some(durable_state) = state.as_ref() else {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                );
            };
            if !resolution.source().matches_state(durable_state, planned) {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                );
            }
            let frontier = match resolution.basis() {
                MetadataCommitResolutionBasisV1::ExactNextApplied
                    if resolution.applied_exact_next() == Some(planned.exact_next())
                        && resolution.not_applied_exact_prior().is_none()
                        && evidence.iter().any(|byte| *byte != 0) =>
                {
                    nokv_meta::workspace::MetadataFrontierPointV1::Exact(planned.exact_next())
                }
                MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled
                    if resolution.source()
                        == MetadataCommitReceiptDirtySourceV1::PoisonedSettled
                        && resolution.applied_exact_next().is_none()
                        && resolution.not_applied_exact_prior() == Some(planned.prior())
                        && evidence.iter().any(|byte| *byte != 0) =>
                {
                    planned.prior()
                }
                _ => {
                    return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                        MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                    )
                }
            };
            *state = Some(MetadataCommitReceiptStateV1::Clean {
                store_identity: planned.store_identity(),
                frozen_bundle_digest: self.digest,
                frontier,
            });
            MetadataCommitReceiptMutationBackendResultV1::Completed
        })();
        command.complete(result)
    }

    fn poison_commit_receipt_v1(
        &self,
        command: MetadataCommitReceiptPoisonCommandV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        let command = command.claim_execution();
        let result = (|| {
            if self
                .reject_next_poison_before_effect
                .swap(false, AtomicOrdering::AcqRel)
            {
                return MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::Unavailable,
                );
            }
            let planned = command.planned();
            let reason = command.reason();
            let mut state = self.state.lock().unwrap();
            match state.as_ref() {
                Some(MetadataCommitReceiptStateV1::Pending(durable)) if durable == planned => {
                    *state = Some(match reason {
                        MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome => {
                            MetadataCommitReceiptStateV1::PoisonedSettled(planned.clone())
                        }
                        MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome => {
                            MetadataCommitReceiptStateV1::PoisonedUnsettled(planned.clone())
                        }
                    });
                    MetadataCommitReceiptMutationBackendResultV1::Completed
                }
                Some(MetadataCommitReceiptStateV1::PoisonedSettled(durable))
                    if durable == planned
                        && reason == MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome =>
                {
                    MetadataCommitReceiptMutationBackendResultV1::Completed
                }
                Some(MetadataCommitReceiptStateV1::PoisonedUnsettled(durable))
                    if durable == planned
                        && reason
                            == MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome =>
                {
                    MetadataCommitReceiptMutationBackendResultV1::Completed
                }
                _ => MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                    MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
                ),
            }
        })();
        command.complete(result)
    }
}

/// Seal one concrete runtime bundle and freeze both the descriptor offer and
/// provider installation selected at construction.
struct FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle,
{
    delegate: B,
    frozen_binding: RuntimeProviderBinding<B::InstallationIdentity>,
    frozen_commit_bundle_digest: [u8; SHA256_BYTES],
    frozen_owner_release_binding: B::Binding,
    poisoned: AtomicBool,
}

impl<B> FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle,
{
    fn new(
        delegate: B,
        frozen_offer: ProviderContractOfferV1,
        schema: &ProviderSchemaV1,
    ) -> Result<Self, RuntimeFactoryError> {
        let frozen_binding = delegate.binding_snapshot(schema).map_err(|_| {
            RuntimeFactoryError::new(RuntimeFactoryErrorCode::ProviderContractInspectionFailed)
        })?;
        if frozen_binding.offer() != frozen_offer {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::ProviderOfferDrift,
            ));
        }
        if delegate.commit_receipt_qualification_v1()
            != MetadataCommitReceiptQualificationV1::Durable
        {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::CommitReceiptNotDurable,
            ));
        }
        let frozen_commit_bundle_digest = delegate.frozen_runtime_bundle_digest_v1();
        if frozen_commit_bundle_digest.iter().all(|byte| *byte == 0) {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::CommitReceiptBindingInvalid,
            ));
        }
        let frozen_owner_release_binding = delegate.owner_release_binding().map_err(|_| {
            RuntimeFactoryError::new(RuntimeFactoryErrorCode::OwnerReleaseReceiptInspectionFailed)
        })?;
        let bundle = Self {
            delegate,
            frozen_binding,
            frozen_commit_bundle_digest,
            frozen_owner_release_binding,
            poisoned: AtomicBool::new(false),
        };
        bundle.validate_current(schema)?;
        Ok(bundle)
    }

    fn validate_current(&self, schema: &ProviderSchemaV1) -> Result<(), RuntimeFactoryError> {
        if self.poisoned.load(AtomicOrdering::Acquire) {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::RuntimeBundlePoisoned,
            ));
        }
        self.validate_recovery_current(schema)
    }

    /// Validate the immutable provider/receipt binding without consulting the
    /// process-local serving stop. Durable receipt load and exact resolution
    /// must retain this recovery-only cutpoint after the current allocation is
    /// locally poisoned, while still rejecting any underlying bundle drift.
    fn validate_recovery_current(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<(), RuntimeFactoryError> {
        if self.delegate.commit_receipt_qualification_v1()
            != MetadataCommitReceiptQualificationV1::Durable
            || self.delegate.frozen_runtime_bundle_digest_v1() != self.frozen_commit_bundle_digest
        {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::CommitReceiptBindingDrift,
            ));
        }
        let current = self.delegate.binding_snapshot(schema).map_err(|_| {
            RuntimeFactoryError::new(RuntimeFactoryErrorCode::ProviderContractInspectionFailed)
        })?;
        if current.installation() != self.frozen_binding.installation() {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::ProviderInstallationDrift,
            ));
        }
        if current.offer() != self.frozen_binding.offer() {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::ProviderOfferDrift,
            ));
        }
        if current.recovery_fence_installation()
            != self.frozen_binding.recovery_fence_installation()
        {
            return Err(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::ProviderInstallationDrift,
            ));
        }
        Ok(())
    }

    fn stop_serving_locally(&self) {
        self.poisoned.store(true, AtomicOrdering::Release);
    }

    fn fail_stop(&self) {
        self.stop_serving_locally();
        RuntimeLifecycleValidator::poison(&self.delegate);
    }

    fn validate_receipt_state_binding(
        &self,
        store_identity: MetadataStoreIdentity,
        state: &MetadataCommitReceiptStateV1,
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        let matches = match state {
            MetadataCommitReceiptStateV1::Clean {
                store_identity: durable_identity,
                frozen_bundle_digest,
                ..
            } => {
                *durable_identity == store_identity
                    && *frozen_bundle_digest == self.frozen_commit_bundle_digest
            }
            MetadataCommitReceiptStateV1::Pending(planned)
            | MetadataCommitReceiptStateV1::PoisonedSettled(planned)
            | MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => {
                planned.store_identity() == store_identity
                    && planned.frozen_bundle_digest() == self.frozen_commit_bundle_digest
            }
            MetadataCommitReceiptStateV1::UntrackedStandalone => false,
        };
        if matches {
            Ok(())
        } else {
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        }
    }

    fn validate_plan_binding(
        &self,
        planned: &PlannedMetadataCommitV1,
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        if planned.frozen_bundle_digest() == self.frozen_commit_bundle_digest {
            Ok(())
        } else {
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        }
    }

    fn complete_persist_without_delegate(
        command: MetadataCommitReceiptPersistCommandV1,
        reason: MetadataCommitReceiptPersistNotDispatchedV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        command.reject_before_execution(reason)
    }

    fn complete_resolve_without_delegate(
        command: MetadataCommitReceiptResolveCommandV1,
        reason: MetadataCommitReceiptMutationNotDispatchedV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        command.reject_before_execution(reason)
    }

    fn complete_poison_without_delegate(
        command: MetadataCommitReceiptPoisonCommandV1,
        reason: MetadataCommitReceiptMutationNotDispatchedV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        command.reject_before_execution(reason)
    }
}

impl<B> ResolvedRuntimeBundle for FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle + 'static,
{
    fn validate_current(&self, schema: &ProviderSchemaV1) -> Result<(), RuntimeFactoryError> {
        Self::validate_current(self, schema)
    }

    fn bundle_identity(&self) -> RuntimeBundleIdentity {
        RuntimeBundleIdentity {
            concrete_type: TypeId::of::<Self>(),
            data_address: self as *const Self as *const () as usize,
        }
    }

    fn validate_lifecycle(&self) -> Result<(), RuntimeLifecycleValidationError> {
        RuntimeLifecycleValidator::validate(self)
    }

    fn poison_lifecycle(&self) {
        self.fail_stop();
    }

    fn preflight_owner_release(&self) -> Result<(), OwnerReleaseReceiptError> {
        self.delegate
            .preflight_owner_release_at_binding(&self.frozen_owner_release_binding)
    }

    fn persist_owner_releasing(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<(), OwnerReleaseReceiptError> {
        self.delegate
            .persist_owner_releasing_at_binding(&self.frozen_owner_release_binding, lease)
    }

    fn open_store(
        self: Arc<Self>,
        intent: OpenIntent,
        identity: MetadataStoreIdentity,
    ) -> Result<AgentMetadataStore, RuntimeOpenError> {
        self.validate_current(&canonical_provider_schema_v1())
            .map_err(RuntimeOpenError::Runtime)?;
        let result = match intent {
            OpenIntent::CreateFresh => AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                Arc::clone(&self),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            ),
            OpenIntent::ReconcilePreparedCreate => {
                AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                    Arc::clone(&self),
                    identity,
                    CreateRecoveryIntentV1::ReconcilePrepared,
                    MetadataStoreCreateModeV1::Active,
                )
            }
            OpenIntent::ReopenExisting => AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                Arc::clone(&self),
                identity,
            ),
        };
        if let Err(error) = self.validate_current(&canonical_provider_schema_v1()) {
            self.fail_stop();
            return Err(RuntimeOpenError::Runtime(error));
        }
        result.map_err(RuntimeOpenError::Metadata)
    }
}

impl<B> MetadataProviderFactoryV1 for FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle,
{
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        self.validate_current(schema)
            .map_err(|_| ProviderError::schema())?;
        Ok(self.frozen_binding.offer())
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        self.validate_current(request.schema())
            .map_err(|_| ProviderError::schema())?;
        let provider = self
            .delegate
            .create_at_binding(&self.frozen_binding, request)?;
        if self.validate_current(request.schema()).is_err() {
            self.fail_stop();
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Create,
            ));
        }
        Ok(provider)
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        self.validate_current(request.schema())
            .map_err(|_| ProviderError::schema())?;
        let provider = self
            .delegate
            .reopen_at_binding(&self.frozen_binding, request)?;
        if self.validate_current(request.schema()).is_err() {
            self.fail_stop();
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Reopen,
            ));
        }
        Ok(provider)
    }
}

impl<B> MetadataCommitRecoveryFenceFactoryV1 for FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle,
{
    fn old_dispatch_exclusion_installation_v1(&self) -> MetadataOldDispatchExclusionInstallationV1 {
        if self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            MetadataOldDispatchExclusionInstallationV1::unsupported()
        } else {
            self.frozen_binding.recovery_fence_installation().clone()
        }
    }

    fn reopen_pending_with_old_dispatch_excluded_v1(
        &self,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        if self.validate_recovery_current(command.schema()).is_err()
            || self.validate_plan_binding(command.planned()).is_err()
            || command.expected_installation() != self.frozen_binding.recovery_fence_installation()
        {
            self.fail_stop();
            return command.reject_before_execution(
                MetadataPendingRecoveryOpenNotDispatchedV1::InvalidBinding,
            );
        }
        let outcome = self
            .delegate
            .reopen_pending_with_old_dispatch_excluded_at_binding_v1(&self.frozen_binding, command);
        if self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            return outcome.downgrade_after_forwarding_failure();
        }
        outcome
    }
}

impl<B> MetadataCommitReceiptStoreV1 for FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle,
{
    fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
        MetadataCommitReceiptQualificationV1::Durable
    }

    fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
        self.frozen_commit_bundle_digest
    }

    fn load_commit_receipt_v1(
        &self,
        store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
        if self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        let loaded = self.delegate.load_commit_receipt_v1(store_identity);
        if self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        let state = loaded?;
        if self
            .validate_receipt_state_binding(store_identity, &state)
            .is_err()
        {
            self.fail_stop();
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        Ok(state)
    }

    fn persist_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptPersistCommandV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        if self.poisoned.load(AtomicOrdering::Acquire) {
            return Self::complete_persist_without_delegate(
                command,
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
            );
        }
        if self.validate_plan_binding(command.planned()).is_err() {
            self.fail_stop();
            return Self::complete_persist_without_delegate(
                command,
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
            );
        }
        if self
            .validate_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            return Self::complete_persist_without_delegate(
                command,
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
            );
        }
        let outcome = self.delegate.persist_pending_commit_v1(command);
        let binding_drifted = self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err();
        if binding_drifted {
            self.fail_stop();
            return outcome.downgrade_after_forwarding_failure();
        }
        if outcome.backend_result_for_forwarding()
            == Some(MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired)
        {
            self.stop_serving_locally();
        }
        outcome
    }

    fn resolve_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptResolveCommandV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        if self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            return Self::complete_resolve_without_delegate(
                command,
                MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
            );
        }
        if self.validate_plan_binding(command.planned()).is_err() {
            self.fail_stop();
            return Self::complete_resolve_without_delegate(
                command,
                MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
            );
        }
        let outcome = self.delegate.resolve_pending_commit_v1(command);
        let binding_drifted = self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err();
        if binding_drifted {
            self.fail_stop();
            return outcome.downgrade_after_forwarding_failure();
        }
        if outcome.backend_result_for_forwarding()
            != Some(MetadataCommitReceiptMutationBackendResultV1::Completed)
        {
            self.stop_serving_locally();
        }
        outcome
    }

    fn poison_commit_receipt_v1(
        &self,
        command: MetadataCommitReceiptPoisonCommandV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        if self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err()
        {
            self.fail_stop();
            return Self::complete_poison_without_delegate(
                command,
                MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
            );
        }
        if self.validate_plan_binding(command.planned()).is_err() {
            self.fail_stop();
            return Self::complete_poison_without_delegate(
                command,
                MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
            );
        }
        // Stop ordinary validation, provider open, and new receipt persistence
        // on this frozen allocation before touching the durable receipt. Do
        // not poison the delegate lifecycle here: the already-open provider
        // must remain usable for the engine's immediate exact-resolution read.
        // External lifecycle faults use `fail_stop`, which also poisons the
        // delegate. Durable load/resolve remain recovery-only in both cases.
        self.stop_serving_locally();
        let outcome = self.delegate.poison_commit_receipt_v1(command);
        let binding_drifted = self
            .validate_recovery_current(&canonical_provider_schema_v1())
            .is_err();
        if binding_drifted {
            self.fail_stop();
            return outcome.downgrade_after_forwarding_failure();
        }
        outcome
    }
}

impl<B> RuntimeLifecycleValidator for FrozenRuntimeBundle<B>
where
    B: ExternalOwnerRuntimeBundle,
{
    fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
        if self.poisoned.load(AtomicOrdering::Acquire) {
            return Err(RuntimeLifecycleValidationError::Poisoned);
        }
        self.delegate.validate()
    }

    fn poison(&self) {
        self.poisoned.store(true, AtomicOrdering::Release);
        RuntimeLifecycleValidator::poison(&self.delegate);
    }
}

impl Clone for ResolvedRuntime {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            runtime_bundle: Arc::clone(&self.runtime_bundle),
            bundle_identity: self.bundle_identity,
        }
    }
}

impl fmt::Debug for ResolvedRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedRuntime")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

/// Closed runtime-resolution failure code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeFactoryErrorCode {
    ConfigurationRejected,
    Unavailable,
    InitializationFailed,
    ProviderContractInspectionFailed,
    ProviderOfferDrift,
    ProviderInstallationDrift,
    CommitReceiptNotDurable,
    CommitReceiptBindingInvalid,
    CommitReceiptBindingDrift,
    RuntimeBundleIdentityMismatch,
    RuntimeBundlePoisoned,
    OwnerReceiptModeMismatch,
    OwnerReleaseReceiptInspectionFailed,
}

/// Redacted factory failure. Provider sources and configuration are never
/// retained or rendered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeFactoryError {
    code: RuntimeFactoryErrorCode,
}

impl RuntimeFactoryError {
    #[must_use]
    pub const fn new(code: RuntimeFactoryErrorCode) -> Self {
        Self { code }
    }

    #[must_use]
    pub const fn code(self) -> RuntimeFactoryErrorCode {
        self.code
    }
}

impl fmt::Display for RuntimeFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "metadata runtime factory failed ({:?})",
            self.code
        )
    }
}

impl std::error::Error for RuntimeFactoryError {}

/// Immutable descriptor-only registry used before process-local runtime
/// composition.
///
/// This registry deliberately accepts no factories and performs no runtime
/// resolution. Provisioning and other control-plane preflights can therefore
/// select an admitted descriptor without manufacturing a process runtime or
/// weakening [`RuntimeRegistry`]'s eager single-resolution contract.
pub struct RuntimeDescriptorRegistry {
    entries: BTreeMap<MetadataProviderProfileId, RuntimeDescriptor>,
}

impl RuntimeDescriptorRegistry {
    pub fn new(
        descriptors: Vec<RuntimeDescriptor>,
    ) -> Result<Self, RuntimeDescriptorRegistryError> {
        let mut entries = BTreeMap::new();
        for descriptor in descriptors {
            let profile_id = descriptor.profile_id().clone();
            if entries.insert(profile_id.clone(), descriptor).is_some() {
                return Err(RuntimeDescriptorRegistryError::DuplicateProfile { profile_id });
            }
        }
        Ok(Self { entries })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn descriptor(
        &self,
        profile_id: &MetadataProviderProfileId,
    ) -> Result<&RuntimeDescriptor, RuntimeDescriptorRegistryError> {
        self.entries
            .get(profile_id)
            .ok_or_else(|| RuntimeDescriptorRegistryError::UnknownProfile {
                profile_id: profile_id.clone(),
            })
    }
}

impl fmt::Debug for RuntimeDescriptorRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let profile_ids: Vec<_> = self.entries.keys().map(|id| id.as_str()).collect();
        formatter
            .debug_struct("RuntimeDescriptorRegistry")
            .field("profile_ids", &profile_ids)
            .finish()
    }
}

/// Typed descriptor-only registry failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeDescriptorRegistryError {
    DuplicateProfile {
        profile_id: MetadataProviderProfileId,
    },
    UnknownProfile {
        profile_id: MetadataProviderProfileId,
    },
}

impl fmt::Display for RuntimeDescriptorRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateProfile { profile_id } => {
                write!(formatter, "duplicate metadata runtime profile {profile_id}")
            }
            Self::UnknownProfile { profile_id } => {
                write!(formatter, "unknown metadata runtime profile {profile_id}")
            }
        }
    }
}

impl std::error::Error for RuntimeDescriptorRegistryError {}

/// Object-safe process-local runtime factory.
pub trait RuntimeFactory: Send + Sync {
    /// Return the complete secret-free descriptor used for registry snapshot
    /// and drift checks.
    fn descriptor(&self) -> RuntimeDescriptor;

    /// Resolve process-local runtime state without changing the descriptor.
    ///
    /// This is pure process composition: implementations may assemble and
    /// validate already-configured in-memory values, but must not create,
    /// reopen, or otherwise touch a provider, backend, path, or runtime state.
    /// [`RuntimeRegistry::new`] calls this exactly once for each qualified,
    /// non-duplicate entry and never calls it for a not-qualified entry.
    fn resolve(&self) -> Result<ResolvedRuntime, RuntimeFactoryError>;
}

enum RegisteredRuntime {
    Qualified {
        descriptor: RuntimeDescriptor,
        resolved: Box<ResolvedRuntime>,
    },
    NotQualified {
        descriptor: RuntimeDescriptor,
        code: QualificationCode,
    },
}

impl RegisteredRuntime {
    const fn descriptor(&self) -> &RuntimeDescriptor {
        match self {
            Self::Qualified { descriptor, .. } | Self::NotQualified { descriptor, .. } => {
                descriptor
            }
        }
    }
}

/// Immutable registry keyed by durable metadata-provider profile id.
pub struct RuntimeRegistry {
    entries: BTreeMap<MetadataProviderProfileId, RegisteredRuntime>,
}

impl RuntimeRegistry {
    /// Snapshot all descriptors and reject duplicate profile ids before any
    /// factory resolution, then resolve each qualified entry exactly once.
    pub fn new(factories: Vec<Arc<dyn RuntimeFactory>>) -> Result<Self, RuntimeRegistryError> {
        struct PendingRuntime {
            descriptor: RuntimeDescriptor,
            factory: Arc<dyn RuntimeFactory>,
        }

        let pending: Vec<_> = factories
            .into_iter()
            .map(|factory| PendingRuntime {
                descriptor: factory.descriptor(),
                factory,
            })
            .collect();

        let mut profile_ids = BTreeMap::new();
        for runtime in &pending {
            let profile_id = runtime.descriptor.profile_id().clone();
            if profile_ids.insert(profile_id.clone(), ()).is_some() {
                return Err(RuntimeRegistryError::DuplicateProfile { profile_id });
            }
        }

        let mut entries = BTreeMap::new();
        for PendingRuntime {
            descriptor,
            factory,
        } in pending
        {
            let profile_id = descriptor.profile_id().clone();
            if let RuntimeQualification::NotQualified(code) = descriptor.qualification() {
                entries.insert(
                    profile_id,
                    RegisteredRuntime::NotQualified { descriptor, code },
                );
                continue;
            }

            if factory.descriptor() != descriptor {
                return Err(RuntimeRegistryError::DescriptorDrift { profile_id });
            }
            let resolution = factory.resolve();
            if factory.descriptor() != descriptor {
                return Err(RuntimeRegistryError::DescriptorDrift { profile_id });
            }
            let resolved = resolution.map_err(|error| RuntimeRegistryError::Factory {
                profile_id: profile_id.clone(),
                code: error.code(),
            })?;
            if resolved.descriptor() != &descriptor {
                return Err(RuntimeRegistryError::DescriptorDrift { profile_id });
            }
            resolved.validate_provider_binding().map_err(|error| {
                RuntimeRegistryError::Factory {
                    profile_id: profile_id.clone(),
                    code: error.code(),
                }
            })?;
            entries.insert(
                profile_id,
                RegisteredRuntime::Qualified {
                    descriptor,
                    resolved: Box::new(resolved),
                },
            );
        }
        Ok(Self { entries })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn descriptor(
        &self,
        profile_id: &MetadataProviderProfileId,
    ) -> Result<&RuntimeDescriptor, RuntimeRegistryError> {
        self.entries
            .get(profile_id)
            .map(RegisteredRuntime::descriptor)
            .ok_or_else(|| RuntimeRegistryError::UnknownProfile {
                profile_id: profile_id.clone(),
            })
    }

    /// Return the canonical resolved runtime while revalidating its frozen
    /// provider binding. This never calls the registered factory again.
    pub fn resolve(
        &self,
        profile_id: &MetadataProviderProfileId,
    ) -> Result<ResolvedRuntime, RuntimeRegistryError> {
        let entry =
            self.entries
                .get(profile_id)
                .ok_or_else(|| RuntimeRegistryError::UnknownProfile {
                    profile_id: profile_id.clone(),
                })?;
        match entry {
            RegisteredRuntime::NotQualified { code, .. } => {
                Err(RuntimeRegistryError::NotQualified {
                    profile_id: profile_id.clone(),
                    code: *code,
                })
            }
            RegisteredRuntime::Qualified { resolved, .. } => {
                resolved.validate_provider_binding().map_err(|error| {
                    RuntimeRegistryError::Factory {
                        profile_id: profile_id.clone(),
                        code: error.code(),
                    }
                })?;
                Ok(resolved.as_ref().clone())
            }
        }
    }
}

impl fmt::Debug for RuntimeRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let profile_ids: Vec<_> = self.entries.keys().map(|id| id.as_str()).collect();
        formatter
            .debug_struct("RuntimeRegistry")
            .field("profile_ids", &profile_ids)
            .finish()
    }
}

/// Typed immutable-registry failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeRegistryError {
    DuplicateProfile {
        profile_id: MetadataProviderProfileId,
    },
    UnknownProfile {
        profile_id: MetadataProviderProfileId,
    },
    DescriptorDrift {
        profile_id: MetadataProviderProfileId,
    },
    NotQualified {
        profile_id: MetadataProviderProfileId,
        code: QualificationCode,
    },
    Factory {
        profile_id: MetadataProviderProfileId,
        code: RuntimeFactoryErrorCode,
    },
}

impl fmt::Display for RuntimeRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateProfile { profile_id } => {
                write!(formatter, "duplicate metadata runtime profile {profile_id}")
            }
            Self::UnknownProfile { profile_id } => {
                write!(formatter, "unknown metadata runtime profile {profile_id}")
            }
            Self::DescriptorDrift { profile_id } => {
                write!(
                    formatter,
                    "metadata runtime descriptor drift for profile {profile_id}"
                )
            }
            Self::NotQualified { profile_id, code } => {
                write!(
                    formatter,
                    "metadata runtime profile {profile_id} is not qualified ({code:?})"
                )
            }
            Self::Factory { profile_id, code } => {
                write!(
                    formatter,
                    "metadata runtime factory {profile_id} failed ({code:?})"
                )
            }
        }
    }
}

impl std::error::Error for RuntimeRegistryError {}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::Mutex;
    use std::time::Duration;

    use nokv_meta::provider::admission::{
        workspace_provider_requirements_v1, ProviderAdmissionCode,
    };
    use nokv_meta::provider::v1::{
        AtomicCommitOutcome, AtomicPlan, MetadataProvider, MetadataReadView, MetadataTransaction,
        OrderedSpaceId, ProviderCapabilities, ProviderCreateRequestV1, ProviderDiagnosticsV1,
        ProviderError, ProviderOperationV1, ProviderRecord, ProviderReopenRequestV1, ProviderScan,
        ProviderScanPage, ProviderTransactionModel, ProviderVersionModel, ReadScope,
    };

    use super::*;

    const ALL_TRANSITIONS: [LifecycleTransition; 6] = [
        LifecycleTransition::FreshCreate,
        LifecycleTransition::ExactResume,
        LifecycleTransition::SuccessorReopen,
        LifecycleTransition::PreparedFirstCreate,
        LifecycleTransition::PreparedSuccessorCreate,
        LifecycleTransition::PreparedResumeOrSuccessor,
    ];

    fn profile_id(value: &str) -> MetadataProviderProfileId {
        MetadataProviderProfileId::new(value).unwrap()
    }

    fn qualified_offer() -> ProviderContractOfferV1 {
        let requirements = workspace_provider_requirements_v1();
        ProviderContractOfferV1 {
            capabilities: ProviderCapabilities {
                transaction_model: ProviderTransactionModel::CrossSpaceAtomicBatch,
                version_model: ProviderVersionModel::OpaqueRecordWitness,
                consistent_cross_space_reads: true,
                all_ambiguous_commit_outcomes_settled_before_return: true,
                commit_resolution_reads_causally_current: true,
                max_key_bytes: requirements.max_key_bytes,
                max_value_bytes: requirements.max_value_bytes,
                max_transaction_bytes: usize::MAX,
                max_atomic_operations: requirements.max_atomic_operations,
                max_logical_plan_bytes: requirements.max_logical_plan_bytes,
                exclusive_scan_start_after: true,
                consistent_snapshot_scans: true,
                max_read_view_duration: None,
                max_scan_items: None,
            },
        }
    }

    fn insufficient_offer() -> ProviderContractOfferV1 {
        let mut capabilities = qualified_offer().capabilities;
        capabilities.max_logical_plan_bytes -= 1;
        capabilities.max_read_view_duration = Some(Duration::from_secs(30));
        ProviderContractOfferV1 { capabilities }
    }

    fn descriptor_with(id: &str, fingerprint_fill: u8) -> RuntimeDescriptor {
        descriptor_with_offer(id, fingerprint_fill, qualified_offer())
    }

    fn descriptor_with_offer(
        id: &str,
        fingerprint_fill: u8,
        provider_offer: ProviderContractOfferV1,
    ) -> RuntimeDescriptor {
        RuntimeDescriptor::new(
            profile_id(id),
            [fingerprint_fill; SHA256_BYTES],
            provider_offer,
            LifecycleCapabilities::new(OwnerReceiptMode::ExternalOwnerJournal, &ALL_TRANSITIONS),
            RuntimeConsistencyDomain::ShardLocal,
        )
        .unwrap()
    }

    fn store_identity(descriptor: &RuntimeDescriptor) -> MetadataStoreIdentity {
        MetadataStoreIdentity {
            logical_shard_id: nokv_types::LogicalShardId::from_bytes([0x21; 16]),
            authority_id: nokv_control::MetadataAuthorityId::from_bytes([0x22; 16]),
            authority_generation: nokv_control::MetadataAuthorityGeneration::new(1).unwrap(),
            consistency_domain_id: ConsistencyDomainId::from_bytes([0x23; 16]),
            profile_fingerprint: *descriptor.profile_fingerprint(),
            contract_digest: descriptor.contract_digest(),
        }
    }

    fn owner_release_lease() -> LogicalShardLease {
        let logical_shard_id = nokv_types::LogicalShardId::from_bytes([0x31; 16]);
        LogicalShardLease {
            logical_shard_id,
            owner: nokv_control::NodeId::new("node-a").unwrap(),
            owner_epoch: nokv_control::OwnerEpoch::new(1).unwrap(),
            owner_incarnation_id: nokv_control::OwnerIncarnationId::from_bytes([0x33; 16]),
            lease_id: 9,
            authority: nokv_control::MetadataAuthorityFence {
                logical_shard_id,
                authority_id: nokv_control::MetadataAuthorityId::from_bytes([0x32; 16]),
                authority_generation: nokv_control::MetadataAuthorityGeneration::new(1).unwrap(),
            },
        }
    }

    fn resolved(descriptor: RuntimeDescriptor) -> ResolvedRuntime {
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        external_resolved(descriptor, bundle)
    }

    fn external_resolved(
        descriptor: RuntimeDescriptor,
        bundle: MutableRuntimeBundle,
    ) -> ResolvedRuntime {
        ResolvedRuntime::external_owner_journal(descriptor, bundle).unwrap()
    }

    #[derive(Clone)]
    enum ResolutionBehavior {
        Stable,
        ReturnDescriptor(RuntimeDescriptor),
        DriftAfterResolve(RuntimeDescriptor),
        Fail(RuntimeFactoryError),
    }

    struct FakeFactory {
        descriptor: Mutex<RuntimeDescriptor>,
        behavior: ResolutionBehavior,
        resolve_calls: AtomicUsize,
        _secret_sentinel: String,
    }

    impl FakeFactory {
        fn new(descriptor: RuntimeDescriptor, behavior: ResolutionBehavior) -> Self {
            Self {
                descriptor: Mutex::new(descriptor),
                behavior,
                resolve_calls: AtomicUsize::new(0),
                _secret_sentinel: String::new(),
            }
        }

        fn with_secret(
            descriptor: RuntimeDescriptor,
            behavior: ResolutionBehavior,
            secret_sentinel: &str,
        ) -> Self {
            Self {
                descriptor: Mutex::new(descriptor),
                behavior,
                resolve_calls: AtomicUsize::new(0),
                _secret_sentinel: secret_sentinel.to_owned(),
            }
        }

        fn replace_descriptor(&self, descriptor: RuntimeDescriptor) {
            *self.descriptor.lock().unwrap() = descriptor;
        }

        fn resolve_calls(&self) -> usize {
            self.resolve_calls.load(Ordering::SeqCst)
        }
    }

    impl RuntimeFactory for FakeFactory {
        fn descriptor(&self) -> RuntimeDescriptor {
            self.descriptor.lock().unwrap().clone()
        }

        fn resolve(&self) -> Result<ResolvedRuntime, RuntimeFactoryError> {
            self.resolve_calls.fetch_add(1, Ordering::SeqCst);
            let before = self.descriptor();
            match &self.behavior {
                ResolutionBehavior::Stable => Ok(resolved(before)),
                ResolutionBehavior::ReturnDescriptor(descriptor) => {
                    Ok(resolved(descriptor.clone()))
                }
                ResolutionBehavior::DriftAfterResolve(descriptor) => {
                    self.replace_descriptor(descriptor.clone());
                    Ok(resolved(before))
                }
                ResolutionBehavior::Fail(error) => Err(*error),
            }
        }
    }

    fn registry(factory: &Arc<FakeFactory>) -> RuntimeRegistry {
        let erased: Arc<dyn RuntimeFactory> = factory.clone();
        RuntimeRegistry::new(vec![erased]).unwrap()
    }

    fn assert_object_safe(_factory: Option<&dyn RuntimeFactory>) {}

    #[test]
    fn runtime_factory_is_object_safe() {
        assert_object_safe(None);
    }

    #[test]
    fn descriptor_registry_rejects_duplicates_and_unknown_profiles_without_factories() {
        let empty = RuntimeDescriptorRegistry::new(Vec::new()).unwrap();
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let first = descriptor_with("profile-a", 0x11);
        let second = descriptor_with("profile-b", 0x12);
        let registry = RuntimeDescriptorRegistry::new(vec![first.clone(), second.clone()]).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert_eq!(registry.descriptor(first.profile_id()).unwrap(), &first);
        assert_eq!(registry.descriptor(second.profile_id()).unwrap(), &second);

        let unknown = profile_id("missing-profile");
        assert_eq!(
            registry.descriptor(&unknown).unwrap_err(),
            RuntimeDescriptorRegistryError::UnknownProfile {
                profile_id: unknown
            }
        );

        let duplicate = RuntimeDescriptorRegistry::new(vec![first.clone(), first]).unwrap_err();
        assert_eq!(
            duplicate,
            RuntimeDescriptorRegistryError::DuplicateProfile {
                profile_id: profile_id("profile-a")
            }
        );
    }

    #[test]
    fn descriptor_registry_debug_renders_only_profile_ids() {
        let registry =
            RuntimeDescriptorRegistry::new(vec![descriptor_with("profile-a", 0x11)]).unwrap();
        let rendered = format!("{registry:?}");
        assert!(rendered.contains("profile-a"));
        for private_field in [
            "profile_fingerprint",
            "provider_offer",
            "provider_admission",
            "consistency_domain",
        ] {
            assert!(!rendered.contains(private_field));
        }
    }

    #[test]
    fn registry_rejects_duplicate_and_unknown_profiles_with_typed_errors() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let first = Arc::new(FakeFactory::new(
            descriptor.clone(),
            ResolutionBehavior::Stable,
        ));
        let second = Arc::new(FakeFactory::new(descriptor, ResolutionBehavior::Stable));
        let first_erased: Arc<dyn RuntimeFactory> = first.clone();
        let second_erased: Arc<dyn RuntimeFactory> = second.clone();
        let error = RuntimeRegistry::new(vec![first_erased, second_erased]).unwrap_err();
        assert_eq!(
            error,
            RuntimeRegistryError::DuplicateProfile {
                profile_id: profile_id("profile-a")
            }
        );
        assert_eq!(first.resolve_calls(), 0);
        assert_eq!(second.resolve_calls(), 0);

        let registry = RuntimeRegistry::new(Vec::new()).unwrap();
        let unknown = profile_id("missing-profile");
        assert_eq!(
            registry.resolve(&unknown).unwrap_err(),
            RuntimeRegistryError::UnknownProfile {
                profile_id: unknown
            }
        );
    }

    #[test]
    fn registry_freezes_descriptor_and_rejects_drift_during_or_in_result() {
        let admitted = descriptor_with("profile-a", 0x11);
        let changed = descriptor_with("profile-a", 0x12);

        let frozen_factory = Arc::new(FakeFactory::new(
            admitted.clone(),
            ResolutionBehavior::Stable,
        ));
        let frozen_registry = registry(&frozen_factory);
        frozen_factory.replace_descriptor(changed.clone());
        let frozen = frozen_registry.resolve(admitted.profile_id()).unwrap();
        assert_eq!(frozen.descriptor(), &admitted);
        assert_eq!(frozen_factory.resolve_calls(), 1);

        let result_factory = Arc::new(FakeFactory::new(
            admitted.clone(),
            ResolutionBehavior::ReturnDescriptor(changed.clone()),
        ));
        let result_erased: Arc<dyn RuntimeFactory> = result_factory;
        assert!(matches!(
            RuntimeRegistry::new(vec![result_erased]),
            Err(RuntimeRegistryError::DescriptorDrift { .. })
        ));

        let during_factory = Arc::new(FakeFactory::new(
            admitted.clone(),
            ResolutionBehavior::DriftAfterResolve(changed),
        ));
        let during_erased: Arc<dyn RuntimeFactory> = during_factory;
        assert!(matches!(
            RuntimeRegistry::new(vec![during_erased]),
            Err(RuntimeRegistryError::DescriptorDrift { .. })
        ));
    }

    #[test]
    fn registry_resolves_qualified_factory_once_across_sequential_and_concurrent_lookups() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let factory = Arc::new(FakeFactory::new(
            descriptor.clone(),
            ResolutionBehavior::Stable,
        ));
        let registry = Arc::new(registry(&factory));
        assert_eq!(factory.resolve_calls(), 1);

        let first = registry.resolve(descriptor.profile_id()).unwrap();
        for _ in 0..8 {
            let next = registry.resolve(descriptor.profile_id()).unwrap();
            assert!(next.bundle_identity == first.bundle_identity);
        }

        let mut lookups = Vec::new();
        for _ in 0..16 {
            let registry = Arc::clone(&registry);
            let profile_id = descriptor.profile_id().clone();
            lookups.push(std::thread::spawn(move || {
                registry.resolve(&profile_id).unwrap().bundle_identity
            }));
        }
        for lookup in lookups {
            assert!(lookup.join().unwrap() == first.bundle_identity);
        }
        assert_eq!(factory.resolve_calls(), 1);
    }

    #[test]
    fn registry_poison_is_shared_by_all_future_resolves() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let factory = Arc::new(FakeFactory::new(
            descriptor.clone(),
            ResolutionBehavior::Stable,
        ));
        let registry = registry(&factory);
        let runtime = registry.resolve(descriptor.profile_id()).unwrap();

        runtime.poison_lifecycle();

        assert_eq!(
            registry.resolve(descriptor.profile_id()).unwrap_err(),
            RuntimeRegistryError::Factory {
                profile_id: descriptor.profile_id().clone(),
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned,
            }
        );
        assert_eq!(factory.resolve_calls(), 1);
    }

    #[test]
    fn descriptor_rejects_an_all_zero_profile_fingerprint() {
        let error = RuntimeDescriptor::new(
            profile_id("profile-a"),
            [0; SHA256_BYTES],
            qualified_offer(),
            LifecycleCapabilities::new(
                OwnerReceiptMode::ProviderDurable,
                &[LifecycleTransition::FreshCreate],
            ),
            RuntimeConsistencyDomain::ShardLocal,
        )
        .unwrap_err();
        assert_eq!(error, RuntimeDescriptorError::ZeroProfileFingerprint);
    }

    #[test]
    fn descriptor_rejects_an_all_zero_shared_consistency_domain() {
        let error = RuntimeDescriptor::new(
            profile_id("profile-a"),
            [0x11; SHA256_BYTES],
            qualified_offer(),
            LifecycleCapabilities::new(
                OwnerReceiptMode::ProviderDurable,
                &[LifecycleTransition::FreshCreate],
            ),
            RuntimeConsistencyDomain::Shared(ConsistencyDomainId::from_bytes([0; 16])),
        )
        .unwrap_err();
        assert_eq!(error, RuntimeDescriptorError::ZeroSharedConsistencyDomain);
    }

    #[test]
    fn bundle_constructors_enforce_the_descriptor_receipt_mode() {
        let external_descriptor = descriptor_with("external-profile", 0x31);
        let external_provider = MutableRuntimeBundle::new(external_descriptor.provider_offer());
        assert_eq!(
            ResolvedRuntime::provider_durable(external_descriptor, external_provider)
                .unwrap_err()
                .code(),
            RuntimeFactoryErrorCode::OwnerReceiptModeMismatch
        );

        let durable_descriptor = RuntimeDescriptor::new(
            profile_id("durable-profile"),
            [0x32; SHA256_BYTES],
            qualified_offer(),
            LifecycleCapabilities::new(
                OwnerReceiptMode::ProviderDurable,
                &[LifecycleTransition::FreshCreate],
            ),
            RuntimeConsistencyDomain::ShardLocal,
        )
        .unwrap();
        let durable_provider = MutableRuntimeBundle::new(durable_descriptor.provider_offer());
        assert_eq!(
            ResolvedRuntime::external_owner_journal(durable_descriptor, durable_provider)
                .unwrap_err()
                .code(),
            RuntimeFactoryErrorCode::OwnerReceiptModeMismatch
        );
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    struct TestInstallationIdentity(&'static str);

    const TEST_INSTALLATION_A: TestInstallationIdentity =
        TestInstallationIdentity("/private/runtime/install-a");
    const TEST_INSTALLATION_B: TestInstallationIdentity =
        TestInstallationIdentity("/private/runtime/install-b");

    struct MutableRuntimeConfiguration {
        offer: ProviderContractOfferV1,
        installation: TestInstallationIdentity,
        owner_release_installation: TestInstallationIdentity,
    }

    struct BoundOpenPause {
        entered: Sender<()>,
        resume: Mutex<Receiver<()>>,
        rejected: Sender<()>,
        return_error: Mutex<Receiver<()>>,
    }

    struct BoundOpenController {
        entered: Receiver<()>,
        resume: Sender<()>,
        rejected: Receiver<()>,
        return_error: Sender<()>,
    }

    impl BoundOpenPause {
        fn new() -> (Arc<Self>, BoundOpenController) {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let (rejected_tx, rejected_rx) = mpsc::channel();
            let (return_error_tx, return_error_rx) = mpsc::channel();
            (
                Arc::new(Self {
                    entered: entered_tx,
                    resume: Mutex::new(resume_rx),
                    rejected: rejected_tx,
                    return_error: Mutex::new(return_error_rx),
                }),
                BoundOpenController {
                    entered: entered_rx,
                    resume: resume_tx,
                    rejected: rejected_rx,
                    return_error: return_error_tx,
                },
            )
        }
    }

    struct MutableRuntimeState {
        configuration: Mutex<MutableRuntimeConfiguration>,
        commit_receipt: RecordingCommitReceiptStoreV1,
        commit_receipt_qualification: Mutex<MetadataCommitReceiptQualificationV1>,
        next_bound_open_pause: Mutex<Option<Arc<BoundOpenPause>>>,
        next_bound_owner_release_pause: Mutex<Option<Arc<BoundOpenPause>>>,
        create_calls: AtomicUsize,
        reopen_calls: AtomicUsize,
        owner_release_write_calls: AtomicUsize,
    }

    #[derive(Clone)]
    struct MutableRuntimeBundle {
        state: Arc<MutableRuntimeState>,
    }

    impl MutableRuntimeBundle {
        fn new(offer: ProviderContractOfferV1) -> Self {
            Self {
                state: Arc::new(MutableRuntimeState {
                    configuration: Mutex::new(MutableRuntimeConfiguration {
                        offer,
                        installation: TEST_INSTALLATION_A,
                        owner_release_installation: TEST_INSTALLATION_A,
                    }),
                    commit_receipt: RecordingCommitReceiptStoreV1::new([0xc1; SHA256_BYTES]),
                    commit_receipt_qualification: Mutex::new(
                        MetadataCommitReceiptQualificationV1::Durable,
                    ),
                    next_bound_open_pause: Mutex::new(None),
                    next_bound_owner_release_pause: Mutex::new(None),
                    create_calls: AtomicUsize::new(0),
                    reopen_calls: AtomicUsize::new(0),
                    owner_release_write_calls: AtomicUsize::new(0),
                }),
            }
        }

        fn replace_offer(&self, offer: ProviderContractOfferV1) {
            self.state.configuration.lock().unwrap().offer = offer;
        }

        fn replace_installation(&self, installation: TestInstallationIdentity) {
            self.state.configuration.lock().unwrap().installation = installation;
        }

        fn replace_owner_release_installation(&self, installation: TestInstallationIdentity) {
            self.state
                .configuration
                .lock()
                .unwrap()
                .owner_release_installation = installation;
        }

        fn replace_commit_receipt_qualification(
            &self,
            qualification: MetadataCommitReceiptQualificationV1,
        ) {
            *self.state.commit_receipt_qualification.lock().unwrap() = qualification;
        }

        fn pause_next_bound_open(&self) -> BoundOpenController {
            let (pause, controller) = BoundOpenPause::new();
            *self.state.next_bound_open_pause.lock().unwrap() = Some(Arc::clone(&pause));
            controller
        }

        fn pause_next_bound_owner_release(&self) -> BoundOpenController {
            let (pause, controller) = BoundOpenPause::new();
            *self.state.next_bound_owner_release_pause.lock().unwrap() = Some(Arc::clone(&pause));
            controller
        }

        fn create_calls(&self) -> usize {
            self.state.create_calls.load(Ordering::SeqCst)
        }

        fn reopen_calls(&self) -> usize {
            self.state.reopen_calls.load(Ordering::SeqCst)
        }

        fn owner_release_write_calls(&self) -> usize {
            self.state.owner_release_write_calls.load(Ordering::SeqCst)
        }

        fn pause_before_bound_open(&self) -> Option<Arc<BoundOpenPause>> {
            let pause = self.state.next_bound_open_pause.lock().unwrap().take();
            if let Some(pause) = pause.as_ref() {
                pause.entered.send(()).unwrap();
                pause
                    .resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
            pause
        }

        fn pause_before_bound_owner_release(&self) -> Option<Arc<BoundOpenPause>> {
            let pause = self
                .state
                .next_bound_owner_release_pause
                .lock()
                .unwrap()
                .take();
            if let Some(pause) = pause.as_ref() {
                pause.entered.send(()).unwrap();
                pause
                    .resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
            pause
        }
    }

    impl MetadataProviderFactoryV1 for MutableRuntimeBundle {
        fn contract_offer(
            &self,
            _schema: &ProviderSchemaV1,
        ) -> Result<ProviderContractOfferV1, ProviderError> {
            Ok(self.state.configuration.lock().unwrap().offer)
        }

        fn create(
            &self,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            request.claim_execution()?;
            self.state.create_calls.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::unavailable(ProviderOperationV1::Create))
        }

        fn reopen(
            &self,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            request.claim_execution()?;
            self.state.reopen_calls.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::unavailable(ProviderOperationV1::Reopen))
        }
    }

    impl RuntimeProviderFactory for MutableRuntimeBundle {
        type InstallationIdentity = TestInstallationIdentity;

        fn binding_snapshot(
            &self,
            _schema: &ProviderSchemaV1,
        ) -> Result<RuntimeProviderBinding<Self::InstallationIdentity>, ProviderError> {
            let configuration = self.state.configuration.lock().unwrap();
            Ok(RuntimeProviderBinding::new(
                configuration.offer,
                configuration.installation,
            ))
        }

        fn create_at_binding(
            &self,
            expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            let pause = self.pause_before_bound_open();
            let configuration = self.state.configuration.lock().unwrap();
            let current_binding =
                RuntimeProviderBinding::new(configuration.offer, configuration.installation);
            if &current_binding != expected_binding {
                drop(configuration);
                if let Some(pause) = pause {
                    pause.rejected.send(()).unwrap();
                    pause
                        .return_error
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
                return Err(ProviderError::authority_mismatch(
                    ProviderOperationV1::Create,
                ));
            }
            request.claim_execution()?;
            self.state.create_calls.fetch_add(1, Ordering::SeqCst);
            drop(configuration);
            Err(ProviderError::unavailable(ProviderOperationV1::Create))
        }

        fn reopen_at_binding(
            &self,
            expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            let pause = self.pause_before_bound_open();
            let configuration = self.state.configuration.lock().unwrap();
            let current_binding =
                RuntimeProviderBinding::new(configuration.offer, configuration.installation);
            if &current_binding != expected_binding {
                drop(configuration);
                if let Some(pause) = pause {
                    pause.rejected.send(()).unwrap();
                    pause
                        .return_error
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
                return Err(ProviderError::authority_mismatch(
                    ProviderOperationV1::Reopen,
                ));
            }
            request.claim_execution()?;
            self.state.reopen_calls.fetch_add(1, Ordering::SeqCst);
            drop(configuration);
            Err(ProviderError::unavailable(ProviderOperationV1::Reopen))
        }
    }

    impl MetadataCommitReceiptStoreV1 for MutableRuntimeBundle {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            *self.state.commit_receipt_qualification.lock().unwrap()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.state.commit_receipt.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            self.state
                .commit_receipt
                .load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            self.state.commit_receipt.persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            self.state.commit_receipt.resolve_pending_commit_v1(command)
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            self.state.commit_receipt.poison_commit_receipt_v1(command)
        }
    }

    impl RuntimeLifecycleValidator for MutableRuntimeBundle {
        fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
            Ok(())
        }

        fn poison(&self) {}
    }

    impl OwnerReleaseReceipt for MutableRuntimeBundle {
        type Binding = TestInstallationIdentity;

        fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError> {
            Ok(self
                .state
                .configuration
                .lock()
                .unwrap()
                .owner_release_installation)
        }

        fn preflight_owner_release_at_binding(
            &self,
            expected: &Self::Binding,
        ) -> Result<(), OwnerReleaseReceiptError> {
            if self
                .state
                .configuration
                .lock()
                .unwrap()
                .owner_release_installation
                != *expected
            {
                return Err(OwnerReleaseReceiptError::BindingDriftV1);
            }
            Ok(())
        }

        fn persist_owner_releasing_at_binding(
            &self,
            expected: &Self::Binding,
            _lease: &LogicalShardLease,
        ) -> Result<(), OwnerReleaseReceiptError> {
            let pause = self.pause_before_bound_owner_release();
            let configuration = self.state.configuration.lock().unwrap();
            if configuration.owner_release_installation != *expected {
                drop(configuration);
                if let Some(pause) = pause {
                    pause.rejected.send(()).unwrap();
                    pause
                        .return_error
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
                return Err(OwnerReleaseReceiptError::BindingDriftV1);
            }
            self.state
                .owner_release_write_calls
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    struct PostOpenInstallationIdentity {
        canonical_locator: PathBuf,
        services_address: usize,
        generation: u8,
    }

    struct PostOpenDriftState {
        canonical_locator: PathBuf,
        generation: Mutex<u8>,
        commit_receipt: RecordingCommitReceiptStoreV1,
        bound_open_calls: AtomicUsize,
        receipt_resolution_calls: AtomicUsize,
        poisoned: AtomicBool,
    }

    impl nokv_meta::built_in_holt::HoltRuntimeGuard for PostOpenDriftState {
        fn bind_store(
            &self,
            identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            if identity.canonical_locator() != self.canonical_locator {
                return Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Rejected);
            }
            Ok(())
        }

        fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            Ok(())
        }

        fn poison(&self) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    struct PostOpenDriftBundle {
        provider_factory: Arc<dyn MetadataProviderFactoryV1>,
        state: Arc<PostOpenDriftState>,
    }

    impl PostOpenDriftBundle {
        fn new(path: &std::path::Path) -> Self {
            let canonical_locator = std::fs::canonicalize(path.parent().unwrap())
                .unwrap()
                .join(path.file_name().unwrap());
            let state = Arc::new(PostOpenDriftState {
                canonical_locator: canonical_locator.clone(),
                generation: Mutex::new(0),
                commit_receipt: RecordingCommitReceiptStoreV1::new([0xd2; SHA256_BYTES]),
                bound_open_calls: AtomicUsize::new(0),
                receipt_resolution_calls: AtomicUsize::new(0),
                poisoned: AtomicBool::new(false),
            });
            let guard: Arc<dyn nokv_meta::built_in_holt::HoltRuntimeGuard> = state.clone();
            let provider_factory =
                nokv_meta::built_in_holt::file_provider_factory_v1(canonical_locator, guard);
            Self {
                provider_factory,
                state,
            }
        }

        fn current_identity(&self, generation: u8) -> PostOpenInstallationIdentity {
            PostOpenInstallationIdentity {
                canonical_locator: self.state.canonical_locator.clone(),
                services_address: Arc::as_ptr(&self.state) as *const () as usize,
                generation,
            }
        }
    }

    impl MetadataProviderFactoryV1 for PostOpenDriftBundle {
        fn contract_offer(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<ProviderContractOfferV1, ProviderError> {
            self.provider_factory.contract_offer(schema)
        }

        fn create(
            &self,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.provider_factory.create(request)
        }

        fn reopen(
            &self,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.provider_factory.reopen(request)
        }
    }

    impl RuntimeProviderFactory for PostOpenDriftBundle {
        type InstallationIdentity = PostOpenInstallationIdentity;

        fn binding_snapshot(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<RuntimeProviderBinding<Self::InstallationIdentity>, ProviderError> {
            let generation = self.state.generation.lock().unwrap();
            Ok(RuntimeProviderBinding::new(
                self.provider_factory.contract_offer(schema)?,
                self.current_identity(*generation),
            ))
        }

        fn create_at_binding(
            &self,
            expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            let generation = self.state.generation.lock().unwrap();
            let current_binding = RuntimeProviderBinding::new(
                self.provider_factory.contract_offer(request.schema())?,
                self.current_identity(*generation),
            );
            if &current_binding != expected_binding {
                return Err(ProviderError::authority_mismatch(
                    ProviderOperationV1::Create,
                ));
            }
            self.state.bound_open_calls.fetch_add(1, Ordering::SeqCst);
            self.provider_factory.create(request)
        }

        fn reopen_at_binding(
            &self,
            expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            let generation = self.state.generation.lock().unwrap();
            let current_binding = RuntimeProviderBinding::new(
                self.provider_factory.contract_offer(request.schema())?,
                self.current_identity(*generation),
            );
            if &current_binding != expected_binding {
                return Err(ProviderError::authority_mismatch(
                    ProviderOperationV1::Reopen,
                ));
            }
            self.state.bound_open_calls.fetch_add(1, Ordering::SeqCst);
            self.provider_factory.reopen(request)
        }
    }

    impl MetadataCommitReceiptStoreV1 for PostOpenDriftBundle {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            self.state.commit_receipt.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.state.commit_receipt.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            self.state
                .commit_receipt
                .load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            self.state.commit_receipt.persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            let outcome = self.state.commit_receipt.resolve_pending_commit_v1(command);
            if outcome.backend_result_for_forwarding()
                == Some(MetadataCommitReceiptMutationBackendResultV1::Completed)
            {
                self.state
                    .receipt_resolution_calls
                    .fetch_add(1, Ordering::SeqCst);
                *self.state.generation.lock().unwrap() = 1;
            }
            outcome
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            self.state.commit_receipt.poison_commit_receipt_v1(command)
        }
    }

    impl RuntimeLifecycleValidator for PostOpenDriftBundle {
        fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
            if self.state.poisoned.load(Ordering::Acquire) {
                Err(RuntimeLifecycleValidationError::Poisoned)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {
            self.state.poisoned.store(true, Ordering::Release);
        }
    }

    impl OwnerReleaseReceipt for PostOpenDriftBundle {
        type Binding = ();

        fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError> {
            Ok(())
        }

        fn preflight_owner_release_at_binding(
            &self,
            _expected: &Self::Binding,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }

        fn persist_owner_releasing_at_binding(
            &self,
            _expected: &Self::Binding,
            _lease: &LogicalShardLease,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    struct ReceiptRecoveryInstallation {
        canonical_locator: PathBuf,
        services_address: usize,
        binding_generation: u64,
    }

    #[derive(Default)]
    struct ReceiptRecoveryRuntimeState {
        poisoned: AtomicBool,
    }

    struct ReceiptRecoveryHoltGuard {
        runtime: Arc<ReceiptRecoveryRuntimeState>,
    }

    impl nokv_meta::built_in_holt::HoltRuntimeGuard for ReceiptRecoveryHoltGuard {
        fn bind_store(
            &self,
            _identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            self.validate_runtime()
        }

        fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            if self.runtime.poisoned.load(Ordering::Acquire) {
                Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Poisoned)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {
            self.runtime.poisoned.store(true, Ordering::Release);
        }
    }

    struct InjectedUnknownCommitProvider {
        delegate: Arc<dyn MetadataProvider>,
        unknown_after_next_commit: Arc<AtomicBool>,
        unsettled_before_next_commit: Arc<AtomicBool>,
    }

    impl MetadataProvider for InjectedUnknownCommitProvider {
        fn logical_shard_id(&self) -> nokv_types::LogicalShardId {
            self.delegate.logical_shard_id()
        }

        fn capabilities(&self) -> ProviderCapabilities {
            self.delegate.capabilities()
        }

        fn validate_runtime(&self) -> Result<(), ProviderError> {
            self.delegate.validate_runtime()
        }

        fn get(
            &self,
            space: OrderedSpaceId,
            key: &[u8],
        ) -> Result<Option<ProviderRecord>, ProviderError> {
            self.delegate.get(space, key)
        }

        fn begin_read(
            &self,
            scopes: &[ReadScope],
        ) -> Result<Box<dyn MetadataReadView + 'static>, ProviderError> {
            self.delegate.begin_read(scopes)
        }

        fn begin_write(&self) -> Result<Box<dyn MetadataTransaction + 'static>, ProviderError> {
            Ok(Box::new(InjectedUnknownCommitTransaction {
                delegate: self.delegate.begin_write()?,
                unknown_after_next_commit: Arc::clone(&self.unknown_after_next_commit),
                unsettled_before_next_commit: Arc::clone(&self.unsettled_before_next_commit),
            }))
        }

        fn diagnostics(&self) -> Option<&dyn ProviderDiagnosticsV1> {
            self.delegate.diagnostics()
        }
    }

    struct InjectedUnknownCommitTransaction {
        delegate: Box<dyn MetadataTransaction>,
        unknown_after_next_commit: Arc<AtomicBool>,
        unsettled_before_next_commit: Arc<AtomicBool>,
    }

    impl MetadataReadView for InjectedUnknownCommitTransaction {
        fn get(
            &self,
            space: OrderedSpaceId,
            key: &[u8],
        ) -> Result<Option<ProviderRecord>, ProviderError> {
            self.delegate.get(space, key)
        }

        fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
            self.delegate.scan(request)
        }
    }

    impl MetadataTransaction for InjectedUnknownCommitTransaction {
        fn prefix_is_empty(
            &self,
            space: OrderedSpaceId,
            prefix: &[u8],
        ) -> Result<bool, ProviderError> {
            self.delegate.prefix_is_empty(space, prefix)
        }

        fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
            if self
                .unsettled_before_next_commit
                .swap(false, Ordering::AcqRel)
            {
                return Err(ProviderError::unknown_commit_unsettled());
            }
            let inject_unknown = self.unknown_after_next_commit.swap(false, Ordering::AcqRel);
            let outcome = self.delegate.commit(plan)?;
            if inject_unknown {
                Err(ProviderError::unknown_commit_settled())
            } else {
                Ok(outcome)
            }
        }
    }

    struct ReceiptRecoveryShared {
        canonical_locator: PathBuf,
        provider_factory: Arc<dyn MetadataProviderFactoryV1>,
        runtime: Arc<ReceiptRecoveryRuntimeState>,
        commit_receipt: RecordingCommitReceiptStoreV1,
        reported_commit_digest: Mutex<[u8; SHA256_BYTES]>,
        unknown_after_next_commit: Arc<AtomicBool>,
        unsettled_before_next_commit: Arc<AtomicBool>,
        binding_generation: Mutex<u64>,
        next_receipt_load_pause: Mutex<Option<Arc<BoundOpenPause>>>,
        next_receipt_persist_pause: Mutex<Option<Arc<BoundOpenPause>>>,
        next_receipt_resolve_pause: Mutex<Option<Arc<BoundOpenPause>>>,
        bound_open_calls: AtomicUsize,
    }

    impl ReceiptRecoveryShared {
        fn pause_next_receipt_load(&self) -> BoundOpenController {
            let (pause, controller) = BoundOpenPause::new();
            *self.next_receipt_load_pause.lock().unwrap() = Some(pause);
            controller
        }

        fn pause_next_receipt_resolve(&self) -> BoundOpenController {
            let (pause, controller) = BoundOpenPause::new();
            *self.next_receipt_resolve_pause.lock().unwrap() = Some(pause);
            controller
        }

        fn pause_next_receipt_persist(&self) -> BoundOpenController {
            let (pause, controller) = BoundOpenPause::new();
            *self.next_receipt_persist_pause.lock().unwrap() = Some(pause);
            controller
        }

        fn pause_receipt_call(slot: &Mutex<Option<Arc<BoundOpenPause>>>) {
            if let Some(pause) = slot.lock().unwrap().take() {
                pause.entered.send(()).unwrap();
                pause
                    .resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }

        fn replace_binding_generation(&self, generation: u64) {
            *self.binding_generation.lock().unwrap() = generation;
        }

        fn replace_reported_commit_digest(&self, digest: [u8; SHA256_BYTES]) {
            *self.reported_commit_digest.lock().unwrap() = digest;
        }

        fn inject_unknown_after_next_commit(&self) {
            self.unknown_after_next_commit
                .store(true, Ordering::Release);
        }

        fn inject_unsettled_before_next_commit(&self) {
            self.unsettled_before_next_commit
                .store(true, Ordering::Release);
        }

        fn wrap_provider(&self, delegate: Arc<dyn MetadataProvider>) -> Arc<dyn MetadataProvider> {
            Arc::new(InjectedUnknownCommitProvider {
                delegate,
                unknown_after_next_commit: Arc::clone(&self.unknown_after_next_commit),
                unsettled_before_next_commit: Arc::clone(&self.unsettled_before_next_commit),
            })
        }
    }

    struct ReceiptRecoveryBundle {
        shared: Arc<ReceiptRecoveryShared>,
    }

    impl ReceiptRecoveryBundle {
        fn new(path: &std::path::Path) -> Self {
            let canonical_locator = std::fs::canonicalize(path.parent().unwrap())
                .unwrap()
                .join(path.file_name().unwrap());
            let runtime = Arc::new(ReceiptRecoveryRuntimeState::default());
            let guard: Arc<dyn nokv_meta::built_in_holt::HoltRuntimeGuard> =
                Arc::new(ReceiptRecoveryHoltGuard {
                    runtime: Arc::clone(&runtime),
                });
            let provider_factory =
                nokv_meta::built_in_holt::file_provider_factory_v1(&canonical_locator, guard);
            Self {
                shared: Arc::new(ReceiptRecoveryShared {
                    canonical_locator,
                    provider_factory,
                    runtime,
                    commit_receipt: RecordingCommitReceiptStoreV1::new([0xe3; SHA256_BYTES]),
                    reported_commit_digest: Mutex::new([0xe3; SHA256_BYTES]),
                    unknown_after_next_commit: Arc::new(AtomicBool::new(false)),
                    unsettled_before_next_commit: Arc::new(AtomicBool::new(false)),
                    binding_generation: Mutex::new(0),
                    next_receipt_load_pause: Mutex::new(None),
                    next_receipt_persist_pause: Mutex::new(None),
                    next_receipt_resolve_pause: Mutex::new(None),
                    bound_open_calls: AtomicUsize::new(0),
                }),
            }
        }

        fn successor(shared: Arc<ReceiptRecoveryShared>) -> Self {
            Self { shared }
        }

        fn current_binding(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<RuntimeProviderBinding<ReceiptRecoveryInstallation>, ProviderError> {
            Ok(RuntimeProviderBinding::new(
                self.shared.provider_factory.contract_offer(schema)?,
                ReceiptRecoveryInstallation {
                    canonical_locator: self.shared.canonical_locator.clone(),
                    services_address: Arc::as_ptr(&self.shared) as *const () as usize,
                    binding_generation: *self.shared.binding_generation.lock().unwrap(),
                },
            ))
        }
    }

    impl MetadataProviderFactoryV1 for ReceiptRecoveryBundle {
        fn contract_offer(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<ProviderContractOfferV1, ProviderError> {
            self.shared.provider_factory.contract_offer(schema)
        }

        fn create(
            &self,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.shared
                .provider_factory
                .create(request)
                .map(|provider| self.shared.wrap_provider(provider))
        }

        fn reopen(
            &self,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.shared
                .provider_factory
                .reopen(request)
                .map(|provider| self.shared.wrap_provider(provider))
        }
    }

    impl RuntimeProviderFactory for ReceiptRecoveryBundle {
        type InstallationIdentity = ReceiptRecoveryInstallation;

        fn binding_snapshot(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<RuntimeProviderBinding<Self::InstallationIdentity>, ProviderError> {
            self.current_binding(schema)
        }

        fn create_at_binding(
            &self,
            expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            if &self.current_binding(request.schema())? != expected_binding {
                return Err(ProviderError::authority_mismatch(
                    ProviderOperationV1::Create,
                ));
            }
            self.shared.bound_open_calls.fetch_add(1, Ordering::SeqCst);
            self.shared
                .provider_factory
                .create(request)
                .map(|provider| self.shared.wrap_provider(provider))
        }

        fn reopen_at_binding(
            &self,
            expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            if &self.current_binding(request.schema())? != expected_binding {
                return Err(ProviderError::authority_mismatch(
                    ProviderOperationV1::Reopen,
                ));
            }
            self.shared.bound_open_calls.fetch_add(1, Ordering::SeqCst);
            self.shared
                .provider_factory
                .reopen(request)
                .map(|provider| self.shared.wrap_provider(provider))
        }
    }

    impl MetadataCommitReceiptStoreV1 for ReceiptRecoveryBundle {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            self.shared.commit_receipt.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            *self.shared.reported_commit_digest.lock().unwrap()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            ReceiptRecoveryShared::pause_receipt_call(&self.shared.next_receipt_load_pause);
            self.shared
                .commit_receipt
                .load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            ReceiptRecoveryShared::pause_receipt_call(&self.shared.next_receipt_persist_pause);
            self.shared
                .commit_receipt
                .persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            ReceiptRecoveryShared::pause_receipt_call(&self.shared.next_receipt_resolve_pause);
            self.shared
                .commit_receipt
                .resolve_pending_commit_v1(command)
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            self.shared.commit_receipt.poison_commit_receipt_v1(command)
        }
    }

    impl RuntimeLifecycleValidator for ReceiptRecoveryBundle {
        fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
            if self.shared.runtime.poisoned.load(Ordering::Acquire) {
                Err(RuntimeLifecycleValidationError::Poisoned)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {
            self.shared.runtime.poisoned.store(true, Ordering::Release);
        }
    }

    impl OwnerReleaseReceipt for ReceiptRecoveryBundle {
        type Binding = ();

        fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError> {
            Ok(())
        }

        fn preflight_owner_release_at_binding(
            &self,
            _expected: &Self::Binding,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }

        fn persist_owner_releasing_at_binding(
            &self,
            _expected: &Self::Binding,
            _lease: &LogicalShardLease,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StockRegistryNoIoServices {
        guard_bindings: AtomicUsize,
        guard_validations: AtomicUsize,
        lifecycle_validations: AtomicUsize,
        commit_receipt: RecordingCommitReceiptStoreV1,
        poisoned: AtomicBool,
    }

    impl StockRegistryNoIoServices {
        fn calls(&self) -> [usize; 5] {
            [
                self.guard_bindings.load(Ordering::SeqCst),
                self.guard_validations.load(Ordering::SeqCst),
                self.lifecycle_validations.load(Ordering::SeqCst),
                self.commit_receipt.load_calls(),
                self.commit_receipt.resolve_calls(),
            ]
        }

        fn reject_if_poisoned(
            &self,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            if self.poisoned.load(Ordering::Acquire) {
                Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Poisoned)
            } else {
                Ok(())
            }
        }
    }

    impl nokv_meta::built_in_holt::HoltRuntimeGuard for StockRegistryNoIoServices {
        fn bind_store(
            &self,
            _identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            self.reject_if_poisoned()?;
            self.guard_bindings.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            self.reject_if_poisoned()?;
            self.guard_validations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn poison(&self) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    impl RuntimeLifecycleValidator for StockRegistryNoIoServices {
        fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
            if self.poisoned.load(Ordering::Acquire) {
                return Err(RuntimeLifecycleValidationError::Poisoned);
            }
            self.lifecycle_validations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn poison(&self) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    impl OwnerReleaseReceipt for StockRegistryNoIoServices {
        type Binding = ();

        fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError> {
            Ok(())
        }

        fn preflight_owner_release_at_binding(
            &self,
            _expected: &Self::Binding,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }

        fn persist_owner_releasing_at_binding(
            &self,
            _expected: &Self::Binding,
            _lease: &LogicalShardLease,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }
    }

    impl MetadataCommitReceiptStoreV1 for StockRegistryNoIoServices {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            self.commit_receipt.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.commit_receipt.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            self.commit_receipt.load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            self.commit_receipt.persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            self.commit_receipt.resolve_pending_commit_v1(command)
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            self.commit_receipt.poison_commit_receipt_v1(command)
        }
    }

    #[test]
    fn stock_holt_registry_resolution_does_not_touch_services_or_create_the_locator() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let services = Arc::new(StockRegistryNoIoServices::default());
        let factory = crate::holt_file_runtime_factory(&locator, Arc::clone(&services)).unwrap();
        let descriptor = factory.descriptor();
        let calls_after_factory_construction = services.calls();
        assert!(!locator.exists());

        let registry = RuntimeRegistry::new(vec![factory]).unwrap();
        assert_eq!(services.calls(), calls_after_factory_construction);
        assert!(!locator.exists());

        let runtime = registry.resolve(descriptor.profile_id()).unwrap();
        assert_eq!(services.calls(), calls_after_factory_construction);
        assert!(!locator.exists());

        let authority = descriptor.initial_authority(nokv_types::LogicalShardId::from_bytes(
            [0x21; nokv_types::FIXED_ID_BYTES],
        ));
        let identity = descriptor.validate_authority(&authority).unwrap();
        let _store = runtime
            .open_store(OpenIntent::CreateFresh, identity)
            .unwrap();
        assert!(locator.exists());
        assert_eq!(services.guard_bindings.load(Ordering::SeqCst), 1);
        assert!(services.guard_validations.load(Ordering::SeqCst) > 0);
        assert!(services.commit_receipt.resolve_calls() > 0);
        assert!(services.commit_receipt.persist_calls() > 0);
    }

    struct PreResolvedFactory {
        descriptor: RuntimeDescriptor,
        resolved: ResolvedRuntime,
    }

    impl RuntimeFactory for PreResolvedFactory {
        fn descriptor(&self) -> RuntimeDescriptor {
            self.descriptor.clone()
        }

        fn resolve(&self) -> Result<ResolvedRuntime, RuntimeFactoryError> {
            Ok(self.resolved.clone())
        }
    }

    #[test]
    fn registry_construction_never_creates_or_reopens_provider_state() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(qualified_offer());
        let resolved = external_resolved(descriptor.clone(), bundle.clone());
        let factory: Arc<dyn RuntimeFactory> = Arc::new(PreResolvedFactory {
            descriptor: descriptor.clone(),
            resolved,
        });

        let registry = RuntimeRegistry::new(vec![factory]).unwrap();

        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
        registry.resolve(descriptor.profile_id()).unwrap();
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn bundle_and_registry_reject_provider_offer_drift_without_opening_state() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let weaker = MutableRuntimeBundle::new(insufficient_offer());
        let error =
            ResolvedRuntime::external_owner_journal(descriptor.clone(), weaker).unwrap_err();
        assert_eq!(error.code(), RuntimeFactoryErrorCode::ProviderOfferDrift);

        let provider = MutableRuntimeBundle::new(qualified_offer());
        let resolved = external_resolved(descriptor.clone(), provider.clone());
        let runtime_factory: Arc<dyn RuntimeFactory> = Arc::new(PreResolvedFactory {
            descriptor: descriptor.clone(),
            resolved,
        });
        let registry = RuntimeRegistry::new(vec![runtime_factory]).unwrap();

        provider.replace_offer(insufficient_offer());
        assert_eq!(
            registry.resolve(descriptor.profile_id()).unwrap_err(),
            RuntimeRegistryError::Factory {
                profile_id: descriptor.profile_id().clone(),
                code: RuntimeFactoryErrorCode::ProviderOfferDrift,
            }
        );

        let open_provider = MutableRuntimeBundle::new(qualified_offer());
        let runtime = external_resolved(descriptor.clone(), open_provider.clone());
        open_provider.replace_offer(insufficient_offer());
        let identity = store_identity(&descriptor);
        let error = runtime
            .open_store(OpenIntent::CreateFresh, identity)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RuntimeOpenError::Runtime(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::ProviderOfferDrift
            })
        ));
        assert_eq!(open_provider.create_calls(), 0);
    }

    #[test]
    fn durable_receipt_preflight_failure_prevents_provider_delegate_touch() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        bundle.state.commit_receipt.reject_load(true);
        let runtime = external_resolved(descriptor.clone(), bundle.clone());

        let error = runtime
            .open_store(OpenIntent::CreateFresh, store_identity(&descriptor))
            .err()
            .expect("receipt preflight must reject before provider open");

        assert!(matches!(error, RuntimeOpenError::Metadata(_)));
        assert_eq!(bundle.state.commit_receipt.load_calls(), 1);
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn external_owner_bundle_binds_one_concrete_type_and_arc_instance() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let first = external_resolved(
            descriptor.clone(),
            MutableRuntimeBundle::new(qualified_offer()),
        );
        let first_clone = first.clone();
        let second = external_resolved(descriptor, MutableRuntimeBundle::new(qualified_offer()));

        assert!(first.bundle_identity == first_clone.bundle_identity);
        assert!(first.bundle_identity != second.bundle_identity);
    }

    #[test]
    fn exact_bound_owner_release_rejects_receipt_locator_drift_without_delegate_write() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        let runtime = external_resolved(descriptor, bundle.clone());
        bundle.replace_owner_release_installation(TEST_INSTALLATION_B);

        assert_eq!(
            runtime.persist_owner_releasing(&owner_release_lease()),
            Err(OwnerReleaseReceiptError::BindingDriftV1)
        );
        assert_eq!(bundle.owner_release_write_calls(), 0);
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn exact_bound_owner_release_rejects_a_to_b_to_a_before_delegate_write() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        let runtime = external_resolved(descriptor, bundle.clone());
        let pause = bundle.pause_next_bound_owner_release();
        let releasing =
            std::thread::spawn(move || runtime.persist_owner_releasing(&owner_release_lease()));

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_owner_release_installation(TEST_INSTALLATION_B);
        pause.resume.send(()).unwrap();
        pause.rejected.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_owner_release_installation(TEST_INSTALLATION_A);
        pause.return_error.send(()).unwrap();

        assert_eq!(
            releasing.join().unwrap(),
            Err(OwnerReleaseReceiptError::BindingDriftV1)
        );
        assert_eq!(bundle.owner_release_write_calls(), 0);
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn poisoned_runtime_retains_only_the_exact_bound_owner_release_view() {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        let runtime = external_resolved(descriptor, bundle.clone());
        runtime.poison_lifecycle();

        assert_eq!(
            runtime.persist_owner_releasing(&owner_release_lease()),
            Ok(())
        );
        assert_eq!(bundle.owner_release_write_calls(), 1);
        assert!(runtime.validate_provider_binding().is_err());
        assert!(runtime.validate_lifecycle().is_err());
    }

    fn assert_bound_open_rejects_locator_swap_without_delegate_touch(intent: OpenIntent) {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(qualified_offer());
        let runtime = external_resolved(descriptor.clone(), bundle.clone());
        let pause = bundle.pause_next_bound_open();
        let identity = store_identity(&descriptor);
        if intent == OpenIntent::ReopenExisting {
            bundle
                .state
                .commit_receipt
                .seed_exact_frontier_for_reopen(identity);
        }
        let opening = std::thread::spawn(move || runtime.open_store(intent, identity).is_err());

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_installation(TEST_INSTALLATION_B);
        pause.resume.send(()).unwrap();
        pause.rejected.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_installation(TEST_INSTALLATION_A);
        pause.return_error.send(()).unwrap();

        assert!(opening.join().unwrap());
        let schema = canonical_provider_schema_v1();
        assert!(bundle.binding_snapshot(&schema).unwrap().installation() == &TEST_INSTALLATION_A);
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    fn assert_bound_open_rejects_offer_swap_without_delegate_touch(intent: OpenIntent) {
        let descriptor = descriptor_with("profile-a", 0x11);
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        let runtime = external_resolved(descriptor.clone(), bundle.clone());
        let pause = bundle.pause_next_bound_open();
        let identity = store_identity(&descriptor);
        if intent == OpenIntent::ReopenExisting {
            bundle
                .state
                .commit_receipt
                .seed_exact_frontier_for_reopen(identity);
        }
        let opening = std::thread::spawn(move || runtime.open_store(intent, identity).is_err());

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_offer(insufficient_offer());
        pause.resume.send(()).unwrap();
        pause.rejected.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_offer(descriptor.provider_offer());
        pause.return_error.send(()).unwrap();

        assert!(opening.join().unwrap());
        let schema = canonical_provider_schema_v1();
        assert_eq!(
            bundle.binding_snapshot(&schema).unwrap().offer(),
            descriptor.provider_offer()
        );
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn exact_bound_create_rejects_installation_a_to_b_to_a_before_delegate_touch() {
        assert_bound_open_rejects_locator_swap_without_delegate_touch(OpenIntent::CreateFresh);
    }

    #[test]
    fn exact_bound_reopen_rejects_installation_a_to_b_to_a_before_delegate_touch() {
        assert_bound_open_rejects_locator_swap_without_delegate_touch(OpenIntent::ReopenExisting);
    }

    #[test]
    fn exact_bound_create_rejects_offer_a_to_b_to_a_before_delegate_touch() {
        assert_bound_open_rejects_offer_swap_without_delegate_touch(OpenIntent::CreateFresh);
    }

    #[test]
    fn exact_bound_reopen_rejects_offer_a_to_b_to_a_before_delegate_touch() {
        assert_bound_open_rejects_offer_swap_without_delegate_touch(OpenIntent::ReopenExisting);
    }

    #[test]
    fn provider_durable_bundle_preserves_exact_offer_binding_before_delegate_touch() {
        let descriptor = RuntimeDescriptor::new(
            profile_id("durable-profile"),
            [0x32; SHA256_BYTES],
            qualified_offer(),
            LifecycleCapabilities::new(
                OwnerReceiptMode::ProviderDurable,
                &[LifecycleTransition::FreshCreate],
            ),
            RuntimeConsistencyDomain::ShardLocal,
        )
        .unwrap();
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        let runtime =
            ResolvedRuntime::provider_durable(descriptor.clone(), bundle.clone()).unwrap();
        let pause = bundle.pause_next_bound_open();
        let identity = store_identity(&descriptor);
        let opening = std::thread::spawn(move || {
            runtime
                .open_store(OpenIntent::CreateFresh, identity)
                .is_err()
        });

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_offer(insufficient_offer());
        pause.resume.send(()).unwrap();
        pause.rejected.recv_timeout(Duration::from_secs(5)).unwrap();
        bundle.replace_offer(descriptor.provider_offer());
        pause.return_error.send(()).unwrap();

        assert!(opening.join().unwrap());
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn provider_durable_bundle_without_durable_exact_receipt_is_not_qualified() {
        let descriptor = RuntimeDescriptor::new(
            profile_id("durable-profile"),
            [0x32; SHA256_BYTES],
            qualified_offer(),
            LifecycleCapabilities::new(
                OwnerReceiptMode::ProviderDurable,
                &[LifecycleTransition::FreshCreate],
            ),
            RuntimeConsistencyDomain::ShardLocal,
        )
        .unwrap();
        let bundle = MutableRuntimeBundle::new(descriptor.provider_offer());
        bundle.replace_commit_receipt_qualification(
            MetadataCommitReceiptQualificationV1::UntrackedStandalone,
        );

        let error = ResolvedRuntime::provider_durable(descriptor, bundle.clone()).unwrap_err();

        assert_eq!(
            error.code(),
            RuntimeFactoryErrorCode::CommitReceiptNotDurable
        );
        assert_eq!(bundle.create_calls(), 0);
        assert_eq!(bundle.reopen_calls(), 0);
    }

    #[test]
    fn post_open_binding_drift_poison_stops_reuse_without_claiming_rollback() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = PostOpenDriftBundle::new(&locator);
        let state = Arc::clone(&bundle.state);
        let schema = nokv_meta::workspace::canonical_provider_schema_v1();
        let descriptor =
            descriptor_with_offer("profile-a", 0x11, bundle.contract_offer(&schema).unwrap());
        let runtime = ResolvedRuntime::external_owner_journal(descriptor.clone(), bundle).unwrap();
        let identity = store_identity(&descriptor);

        let error = runtime
            .open_store(OpenIntent::CreateFresh, identity)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RuntimeOpenError::Runtime(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(state.receipt_resolution_calls.load(Ordering::SeqCst) >= 1);
        assert!(state.poisoned.load(Ordering::Acquire));
        assert_eq!(
            runtime.validate_lifecycle(),
            Err(RuntimeLifecycleValidationError::Poisoned)
        );
        assert_eq!(state.bound_open_calls.load(Ordering::SeqCst), 1);
        assert!(locator.exists());

        assert!(matches!(
            state
                .commit_receipt
                .load_commit_receipt_v1(identity)
                .unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: nokv_meta::workspace::MetadataFrontierPointV1::Exact(_),
                ..
            }
        ));

        let reuse = runtime
            .open_store(OpenIntent::ReopenExisting, identity)
            .err()
            .unwrap();
        assert!(matches!(
            reuse,
            RuntimeOpenError::Runtime(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert_eq!(state.bound_open_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn recovery_load_rejects_in_delegate_binding_drift_and_fail_stops_old_allocation() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-load-drift", 0x62, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let pause = shared.pause_next_receipt_load();
        let loading = {
            let frozen = Arc::clone(&frozen);
            std::thread::spawn(move || frozen.load_commit_receipt_v1(identity))
        };

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        shared.replace_binding_generation(1);
        pause.resume.send(()).unwrap();

        assert_eq!(
            loading.join().unwrap(),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
        shared.replace_binding_generation(0);
        assert!(shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: nokv_meta::workspace::MetadataFrontierPointV1::Absent,
                ..
            }
        ));
    }

    #[test]
    fn recovery_load_rejects_in_delegate_receipt_digest_drift() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-digest-drift", 0x68, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let pause = shared.pause_next_receipt_load();
        let loading = {
            let frozen = Arc::clone(&frozen);
            std::thread::spawn(move || frozen.load_commit_receipt_v1(identity))
        };

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        shared.replace_reported_commit_digest([0xe4; SHA256_BYTES]);
        pause.resume.send(()).unwrap();

        assert_eq!(
            loading.join().unwrap(),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        );
        shared.replace_reported_commit_digest([0xe3; SHA256_BYTES]);
        assert!(shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: nokv_meta::workspace::MetadataFrontierPointV1::Absent,
                ..
            }
        ));
    }

    #[test]
    fn recovery_resolve_rejects_in_delegate_binding_drift_without_returning_to_serving() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-resolve-drift", 0x63, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let pause = shared.pause_next_receipt_resolve();
        let resolving = std::thread::spawn(move || {
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap())
        });

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        shared.replace_binding_generation(1);
        pause.resume.send(()).unwrap();

        assert_eq!(
            resolving.join().unwrap(),
            Err(AgentMetadataError::CommitOutcomeUnknown)
        );
        shared.replace_binding_generation(0);
        let planned = shared.commit_receipt.last_plan();
        assert!(shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: nokv_meta::workspace::MetadataFrontierPointV1::Exact(frontier),
                ..
            } if frontier == planned.exact_next()
        ));
    }

    #[test]
    fn persist_post_dispatch_binding_drift_is_recovery_required_and_full_fail_stopped() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-persist-drift", 0x67, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let store_clone = store.clone();
        let pause = shared.pause_next_receipt_persist();
        let persisting = std::thread::spawn(move || {
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap())
        });

        pause.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        shared.replace_binding_generation(1);
        pause.resume.send(()).unwrap();

        assert_eq!(
            persisting.join().unwrap(),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        );
        shared.replace_binding_generation(0);
        let planned = shared.commit_receipt.last_plan();
        assert!(shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
        ));
        assert!(matches!(
            store_clone.current_read_version(),
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        assert_eq!(shared.bound_open_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn persist_before_effect_unavailable_does_not_poison_frozen_allocation() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-persist-before", 0x64, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        shared.commit_receipt.reject_next_persist_before_effect();

        assert!(matches!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Err(AgentMetadataError::ProviderUnavailable { .. })
        ));
        assert!(frozen.validate_current(&schema).is_ok());
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        store.current_read_version().unwrap();
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Clean { .. }
        ));

        store
            .advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap())
            .unwrap();
    }

    #[test]
    fn persist_recovery_required_stops_only_current_allocation_and_retains_pending() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-persist-recovery", 0x65, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let store_clone = store.clone();
        shared.commit_receipt.recover_next_persist_after_effect();

        assert_eq!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        );
        let planned = shared.commit_receipt.last_plan();
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
        ));
        for old_store in [&store, &store_clone] {
            assert!(matches!(
                old_store.current_read_version(),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
        }
        let bound_open_calls = shared.bound_open_calls.load(Ordering::SeqCst);
        assert!(Arc::clone(&frozen)
            .open_store(OpenIntent::ReopenExisting, identity)
            .is_err());
        assert_eq!(
            shared.bound_open_calls.load(Ordering::SeqCst),
            bound_open_calls
        );
        drop(store_clone);
        drop(store);

        for _ in 0..3 {
            let successor = Arc::new(
                FrozenRuntimeBundle::new(
                    ReceiptRecoveryBundle::successor(Arc::clone(&shared)),
                    descriptor.provider_offer(),
                    descriptor.schema(),
                )
                .unwrap(),
            );
            assert!(matches!(
                successor.load_commit_receipt_v1(identity).unwrap(),
                MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
            ));
            let reopen = Arc::clone(&successor)
                .open_store(OpenIntent::ReopenExisting, identity)
                .err()
                .expect("a prior-only observation cannot close an older pending dispatch");
            assert!(matches!(
                reopen,
                RuntimeOpenError::Metadata(AgentMetadataError::CommitReceiptRecoveryRequired)
            ));
            assert!(matches!(
                successor.load_commit_receipt_v1(identity).unwrap(),
                MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
            ));
            assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        }
    }

    #[test]
    fn poison_delegate_rejection_stops_current_allocation_and_unsupported_successor_stays_dirty() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-poison-rejected", 0x66, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let store_clone = store.clone();
        shared.commit_receipt.reject_next_poison_before_effect();
        shared.inject_unknown_after_next_commit();

        assert_eq!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Err(AgentMetadataError::CommitOutcomeUnknown)
        );
        let planned = shared.commit_receipt.last_plan();
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
        ));
        for old_store in [&store, &store_clone] {
            assert!(matches!(
                old_store.current_read_version(),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
        }
        let bound_open_calls = shared.bound_open_calls.load(Ordering::SeqCst);
        assert!(Arc::clone(&frozen)
            .open_store(OpenIntent::ReopenExisting, identity)
            .is_err());
        assert_eq!(
            shared.bound_open_calls.load(Ordering::SeqCst),
            bound_open_calls
        );
        drop(store_clone);
        drop(store);

        let successor = Arc::new(
            FrozenRuntimeBundle::new(
                ReceiptRecoveryBundle::successor(Arc::clone(&shared)),
                descriptor.provider_offer(),
                descriptor.schema(),
            )
            .unwrap(),
        );
        let recovery_only = Arc::clone(&successor)
            .open_store(OpenIntent::ReopenExisting, identity)
            .err()
            .expect("the resolving allocation must remain recovery-only");
        assert!(matches!(
            recovery_only,
            RuntimeOpenError::Metadata(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert!(matches!(
            successor.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
        ));
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
    }

    #[test]
    fn resolve_unavailable_stops_current_allocation_and_unsupported_successor_stays_dirty() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-resolve-unavailable", 0x69, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let store_clone = store.clone();
        shared.commit_receipt.reject_resolve(true);

        assert_eq!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Err(AgentMetadataError::CommitOutcomeUnknown)
        );
        let planned = shared.commit_receipt.last_plan();
        shared.commit_receipt.reject_resolve(false);
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.validate_current(&schema),
            Err(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
        ));
        for old_store in [&store, &store_clone] {
            assert!(matches!(
                old_store.current_read_version(),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
        }
        drop(store_clone);
        drop(store);

        let successor = Arc::new(
            FrozenRuntimeBundle::new(
                ReceiptRecoveryBundle::successor(Arc::clone(&shared)),
                descriptor.provider_offer(),
                descriptor.schema(),
            )
            .unwrap(),
        );
        let recovery_only = Arc::clone(&successor)
            .open_store(OpenIntent::ReopenExisting, identity)
            .err()
            .expect("the resolving allocation must remain recovery-only");
        assert!(matches!(
            recovery_only,
            RuntimeOpenError::Metadata(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert!(matches!(
            successor.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(ref durable) if durable == &planned
        ));
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
    }

    #[test]
    fn unsettled_receipt_exact_prior_never_becomes_clean_across_reopens() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-unsettled", 0x6a, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );
        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let store_clone = store.clone();
        shared.inject_unsettled_before_next_commit();

        assert_eq!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Err(AgentMetadataError::CommitOutcomeUnknown)
        );
        let planned = shared.commit_receipt.last_plan();
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::PoisonedUnsettled(ref durable)
                if durable == &planned
        ));
        for old_store in [&store, &store_clone] {
            assert!(matches!(
                old_store.current_read_version(),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
            assert!(matches!(
                old_store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
        }
        drop(store_clone);
        drop(store);

        for _ in 0..3 {
            let successor = Arc::new(
                FrozenRuntimeBundle::new(
                    ReceiptRecoveryBundle::successor(Arc::clone(&shared)),
                    descriptor.provider_offer(),
                    descriptor.schema(),
                )
                .unwrap(),
            );
            let reopen = Arc::clone(&successor)
                .open_store(OpenIntent::ReopenExisting, identity)
                .err()
                .expect("exact prior cannot settle an unsettled durable receipt");
            assert!(matches!(
                reopen,
                RuntimeOpenError::Metadata(AgentMetadataError::CommitReceiptRecoveryRequired)
            ));
            assert!(matches!(
                successor.load_commit_receipt_v1(identity).unwrap(),
                MetadataCommitReceiptStateV1::PoisonedUnsettled(ref durable)
                    if durable == &planned
            ));
            assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        }
    }

    #[test]
    fn exact_receipt_poison_fail_stops_current_runtime_but_preserves_closed_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let bundle = ReceiptRecoveryBundle::new(&locator);
        let shared = Arc::clone(&bundle.shared);
        let schema = canonical_provider_schema_v1();
        let offer = bundle.contract_offer(&schema).unwrap();
        let descriptor = descriptor_with_offer("receipt-recovery", 0x61, offer);
        let identity = store_identity(&descriptor);
        let frozen = Arc::new(
            FrozenRuntimeBundle::new(bundle, descriptor.provider_offer(), descriptor.schema())
                .unwrap(),
        );

        let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            Arc::clone(&frozen),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let store_clone = store.clone();
        assert_eq!(shared.bound_open_calls.load(Ordering::SeqCst), 1);
        shared.inject_unknown_after_next_commit();

        // The wrapped real Holt transaction applies the write and then loses
        // its response. The engine must persist PoisonedSettled, use this same
        // already-open provider for the exact resolution view, close the
        // receipt to Clean, and only then trip the store-shared serving fence.
        // A successful exact outcome proves the recovery view happened before
        // the fence; it would be rejected if the order were reversed.
        assert_eq!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Ok(())
        );
        let planned = shared.commit_receipt.last_plan();
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        assert!(matches!(
            frozen.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: nokv_meta::workspace::MetadataFrontierPointV1::Exact(frontier),
                ..
            } if frontier == planned.exact_next()
        ));
        let persist_calls = shared.commit_receipt.persist_calls();
        for old_store in [&store, &store_clone] {
            assert!(matches!(
                old_store.current_read_version(),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
            assert!(matches!(
                old_store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
                Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
            ));
        }
        assert_eq!(shared.commit_receipt.persist_calls(), persist_calls);
        let blocked = Arc::clone(&frozen)
            .open_store(OpenIntent::ReopenExisting, identity)
            .err()
            .expect("poisoned allocation must not reopen provider state");
        assert!(matches!(
            blocked,
            RuntimeOpenError::Runtime(RuntimeFactoryError {
                code: RuntimeFactoryErrorCode::RuntimeBundlePoisoned
            })
        ));
        assert_eq!(shared.bound_open_calls.load(Ordering::SeqCst), 1);
        drop(store_clone);
        drop(store);

        let successor = Arc::new(
            FrozenRuntimeBundle::new(
                ReceiptRecoveryBundle::successor(Arc::clone(&shared)),
                descriptor.provider_offer(),
                descriptor.schema(),
            )
            .unwrap(),
        );
        let reopened = Arc::clone(&successor)
            .open_store(OpenIntent::ReopenExisting, identity)
            .unwrap();
        assert_eq!(shared.bound_open_calls.load(Ordering::SeqCst), 2);
        assert!(!shared.runtime.poisoned.load(Ordering::Acquire));
        reopened.metadata_frontier().unwrap();
        assert!(matches!(
            successor.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Clean { .. }
        ));
        assert_eq!(shared.bound_open_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn qualification_remains_fail_closed_under_cfg_test_and_factory_drift() {
        let qualified = descriptor_with("profile-a", 0x11);
        assert_eq!(
            qualified.schema(),
            &nokv_meta::workspace::canonical_provider_schema_v1()
        );
        assert_eq!(qualified.qualification(), RuntimeQualification::Qualified);
        assert!(qualified.provider_admission().is_qualified());

        let unqualified = descriptor_with_offer("profile-a", 0x11, insufficient_offer());
        assert_eq!(
            unqualified.provider_admission().rejection_codes,
            vec![
                ProviderAdmissionCode::LogicalPlanLimitTooSmall,
                ProviderAdmissionCode::ReadViewLifetimeBounded,
            ]
        );
        assert_eq!(
            unqualified
                .classify_bootstrap(OpenIntent::CreateFresh, LifecycleTransition::FreshCreate,),
            Err(RuntimeAdmissionError::NotQualified(
                QualificationCode::CompleteCommandSurfaceUnproven
            ))
        );

        let factory = Arc::new(FakeFactory::new(
            unqualified.clone(),
            ResolutionBehavior::ReturnDescriptor(qualified),
        ));
        assert_eq!(
            registry(&factory)
                .resolve(unqualified.profile_id())
                .unwrap_err(),
            RuntimeRegistryError::NotQualified {
                profile_id: unqualified.profile_id().clone(),
                code: QualificationCode::CompleteCommandSurfaceUnproven,
            }
        );
        assert_eq!(factory.resolve_calls(), 0);
    }

    #[test]
    fn lifecycle_classifier_freezes_the_complete_three_by_six_matrix() {
        let capabilities =
            LifecycleCapabilities::new(OwnerReceiptMode::ExternalOwnerJournal, &ALL_TRANSITIONS);
        let matrix = [
            (
                OpenIntent::CreateFresh,
                [true, false, false, false, false, false],
            ),
            (
                OpenIntent::ReconcilePreparedCreate,
                [false, false, false, true, true, true],
            ),
            (
                OpenIntent::ReopenExisting,
                [false, true, true, false, false, false],
            ),
        ];
        for (intent, expected_row) in matrix {
            for (transition, expected_admission) in ALL_TRANSITIONS.into_iter().zip(expected_row) {
                let result = capabilities.classify_bootstrap(intent, transition);
                if expected_admission {
                    let admission = result.unwrap();
                    assert_eq!(admission.transition(), transition);
                    assert_eq!(
                        admission.owner_receipt_mode(),
                        OwnerReceiptMode::ExternalOwnerJournal
                    );
                } else {
                    assert_eq!(result, Err(AdmissionCode::OpenTransitionMismatch));
                }
            }
        }
    }

    #[test]
    fn lifecycle_capabilities_gate_each_transition_independently() {
        let supported = [
            LifecycleTransition::FreshCreate,
            LifecycleTransition::PreparedFirstCreate,
            LifecycleTransition::PreparedResumeOrSuccessor,
        ];
        let capabilities =
            LifecycleCapabilities::new(OwnerReceiptMode::ExternalOwnerJournal, &supported);
        for transition in ALL_TRANSITIONS {
            let result = capabilities.classify_bootstrap(transition.open_intent(), transition);
            if supported.contains(&transition) {
                assert!(result.is_ok());
            } else {
                assert_eq!(result, Err(AdmissionCode::TransitionUnsupported));
            }
        }
    }

    #[test]
    fn registry_and_closed_errors_redact_factory_secret_sentinel() {
        let sentinel = "DO_NOT_LEAK_PROVIDER_SECRET=/private/metadata/profile-a.cluster";
        let descriptor = descriptor_with("profile-a", 0x11);
        let factory = Arc::new(FakeFactory::with_secret(
            descriptor.clone(),
            ResolutionBehavior::Fail(RuntimeFactoryError::new(
                RuntimeFactoryErrorCode::Unavailable,
            )),
            sentinel,
        ));
        let erased: Arc<dyn RuntimeFactory> = factory;
        let error = RuntimeRegistry::new(vec![erased]).unwrap_err();

        for rendered in [
            format!("{descriptor:?}"),
            format!("{error:?}"),
            error.to_string(),
        ] {
            assert!(!rendered.contains(sentinel));
            assert!(!rendered.contains("/private/metadata"));
        }

        let bundle = MutableRuntimeBundle::new(qualified_offer());
        let resolved = external_resolved(descriptor.clone(), bundle.clone());
        let runtime_factory: Arc<dyn RuntimeFactory> = Arc::new(PreResolvedFactory {
            descriptor: descriptor.clone(),
            resolved: resolved.clone(),
        });
        let registry = RuntimeRegistry::new(vec![runtime_factory]).unwrap();
        bundle.replace_installation(TEST_INSTALLATION_B);
        let drift = registry.resolve(descriptor.profile_id()).unwrap_err();
        assert_eq!(
            drift,
            RuntimeRegistryError::Factory {
                profile_id: descriptor.profile_id().clone(),
                code: RuntimeFactoryErrorCode::ProviderInstallationDrift,
            }
        );
        for rendered in [
            format!("{resolved:?}"),
            format!("{registry:?}"),
            format!("{drift:?}"),
            drift.to_string(),
        ] {
            assert!(!rendered.contains(TEST_INSTALLATION_A.0));
            assert!(!rendered.contains(TEST_INSTALLATION_B.0));
        }
    }
}
