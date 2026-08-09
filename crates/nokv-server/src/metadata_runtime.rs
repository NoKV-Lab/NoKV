/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Built-in runtime composition and durable metadata-authority derivation.
//!
//! The registry core remains provider-neutral. This module is only the stock
//! binary's composition edge: provider paths and connection material stay in
//! concrete factories, while control persists the descriptor's profile id,
//! secret-free fingerprint, consistency domain, and exact contract digest.

use std::any::TypeId;
#[cfg(feature = "foundationdb-provider")]
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nokv_control::{
    ConsistencyDomainId, MetadataAuthorityBinding, MetadataAuthorityGeneration,
    MetadataAuthorityId, MetadataAuthorityRecord, MetadataAuthorityRevision,
    MetadataProviderProfileId,
};
use nokv_meta::built_in_holt::{
    HoltExistingStoreReservation, HoltRuntimeGuard, HoltStoreObjectIdentity,
};
use nokv_meta::provider::v1::{
    MetadataProvider, MetadataProviderFactoryV1, ProviderContractOfferV1, ProviderCreateRequestV1,
    ProviderError, ProviderReopenRequestV1, ProviderSchemaV1,
};
use nokv_meta::workspace::{
    MetadataCommitReceiptErrorV1, MetadataCommitReceiptPersistCommandV1,
    MetadataCommitReceiptPersistOutcomeV1, MetadataCommitReceiptPoisonCommandV1,
    MetadataCommitReceiptPoisonOutcomeV1, MetadataCommitReceiptQualificationV1,
    MetadataCommitReceiptResolveCommandV1, MetadataCommitReceiptResolveOutcomeV1,
    MetadataCommitReceiptStateV1, MetadataCommitReceiptStoreV1,
    MetadataCommitRecoveryFenceFactoryV1, MetadataPendingRecoveryOpenCommandV1,
    MetadataPendingRecoveryOpenNotDispatchedV1, MetadataPendingRecoveryOpenOutcomeV1,
    MetadataStoreIdentity,
};
use nokv_types::{LogicalShardId, SHA256_BYTES};
use sha2::{Digest as _, Sha256};

#[cfg(feature = "foundationdb-provider")]
use crate::runtime_registry::RuntimeFactoryErrorCode;
use crate::runtime_registry::{
    LifecycleCapabilities, LifecycleTransition, OwnerReceiptMode, OwnerReleaseReceipt,
    OwnerReleaseReceiptError, ResolvedRuntime, RuntimeConsistencyDomain, RuntimeDescriptor,
    RuntimeFactory, RuntimeFactoryError, RuntimeLifecycleValidationError,
    RuntimeLifecycleValidator, RuntimeProviderBinding, RuntimeProviderFactory,
};
use crate::ServerError;

pub const HOLT_LOCAL_METADATA_PROFILE_ID: &str = "holt-local-v1";
pub const FOUNDATIONDB_METADATA_PROFILE_ID: &str = "foundationdb-v1";

const PROFILE_FINGERPRINT_DOMAIN: &[u8] = b"nokv.metadata-runtime.profile-fingerprint.v1\0";
const AUTHORITY_ID_DOMAIN: &[u8] = b"nokv.metadata-runtime.authority-id.v1\0";
const CONSISTENCY_DOMAIN_ID_DOMAIN: &[u8] = b"nokv.metadata-runtime.consistency-domain-id.v1\0";
const HOLT_LOCAL_PROFILE_CONTRACT: &[u8] = b"holt-file-local-wal\0single-process\0no-successor\0";

#[cfg(feature = "foundationdb-provider")]
const FDB_CLUSTER_KEY_DIGEST_DOMAIN: &[u8] = b"nokv.metadata-runtime.foundationdb.cluster-key.v1\0";
#[cfg(feature = "foundationdb-provider")]
const FDB_EXPLICIT_ID_DIGEST_DOMAIN: &[u8] =
    b"nokv.metadata-runtime.foundationdb.explicit-stable-id.v1\0";
#[cfg(feature = "foundationdb-provider")]
const FDB_CONSISTENCY_DOMAIN_FAMILY: &[u8] = b"foundationdb-cluster-namespace-v1";
#[cfg(feature = "foundationdb-provider")]
const FDB_PROFILE_API_CONTRACT: &[u8] = b"foundationdb-api-730";
#[cfg(feature = "foundationdb-provider")]
const FDB_PROFILE_NAMESPACE_CONTRACT: &[u8] = b"nokv.metadata.fdb.v1";
#[cfg(feature = "foundationdb-provider")]
const FDB_PROFILE_TRANSACTION_CONTRACT: &[u8] =
    b"cross-space-atomic-batch-v1\0opaque-record-witness-v1";
#[cfg(feature = "foundationdb-provider")]
const MAX_CLUSTER_FILE_BYTES: u64 = 8 * 1024;
#[cfg(feature = "foundationdb-provider")]
const MAX_CLUSTER_COMPONENT_BYTES: usize = 255;
#[cfg(feature = "foundationdb-provider")]
const MAX_EXPLICIT_STABLE_ID_BYTES: usize = 255;
#[cfg(feature = "foundationdb-provider")]
const MAX_NAMESPACE_BYTES: usize = 128;

// Holt now exposes a reservation-backed actual-held full object-set validator,
// but that local reopen primitive is not an owner-admission contract. No local
// lifecycle transition is admitted until the durable plan, control outcome,
// journal, receipt, and exact release path are jointly qualified.
const HOLT_TRANSITIONS: [LifecycleTransition; 0] = [];

#[cfg(feature = "foundationdb-provider")]
const FOUNDATIONDB_TRANSITIONS: [LifecycleTransition; 0] = [];

/// Build the stock Holt descriptor without touching a store path.
pub fn holt_runtime_descriptor() -> Result<RuntimeDescriptor, ServerError> {
    let provider_factory = nokv_meta::built_in_holt::memory_provider_factory_v1();
    descriptor_from_factory(
        holt_profile_id(),
        holt_profile_fingerprint(),
        provider_factory.as_ref(),
        LifecycleCapabilities::new(OwnerReceiptMode::ExternalOwnerJournal, &HOLT_TRANSITIONS),
        RuntimeConsistencyDomain::ShardLocal,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ProcessLocalServiceIdentity {
    concrete_type: TypeId,
    data_address: usize,
}

impl ProcessLocalServiceIdentity {
    fn of<T: 'static>(services: &Arc<T>) -> Self {
        Self {
            concrete_type: TypeId::of::<T>(),
            data_address: Arc::as_ptr(services) as *const () as usize,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct HoltFileInstallationIdentity {
    canonical_locator: PathBuf,
    bundle_services: ProcessLocalServiceIdentity,
    expected_store_object_identity: Option<HoltStoreObjectIdentity>,
}

struct ExactHoltRuntimeGuard<T> {
    canonical_locator: PathBuf,
    services: Arc<T>,
}

impl<T> HoltRuntimeGuard for ExactHoltRuntimeGuard<T>
where
    T: HoltRuntimeGuard + Send + Sync,
{
    fn bind_store(
        &self,
        identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
    ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
        if identity.canonical_locator() != self.canonical_locator {
            return Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Rejected);
        }
        self.services.bind_store(identity)
    }

    fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
        self.services.validate_runtime()
    }

    fn poison(&self) {
        HoltRuntimeGuard::poison(self.services.as_ref());
    }
}

struct HoltFileRuntimeBundle<T> {
    provider_factory: Arc<dyn MetadataCommitRecoveryFenceFactoryV1>,
    services: Arc<T>,
    installation_identity: HoltFileInstallationIdentity,
}

impl<T> MetadataProviderFactoryV1 for HoltFileRuntimeBundle<T>
where
    T: Send + Sync,
{
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

impl<T> RuntimeProviderFactory for HoltFileRuntimeBundle<T>
where
    T: Send + Sync,
{
    type InstallationIdentity = HoltFileInstallationIdentity;

    fn binding_snapshot(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<RuntimeProviderBinding<Self::InstallationIdentity>, ProviderError> {
        Ok(RuntimeProviderBinding::with_recovery_fence_installation(
            self.provider_factory.contract_offer(schema)?,
            self.installation_identity.clone(),
            self.provider_factory
                .old_dispatch_exclusion_installation_v1(),
        ))
    }

    fn create_at_binding(
        &self,
        expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        if &self.binding_snapshot(request.schema())? != expected_binding {
            return Err(ProviderError::authority_mismatch(
                nokv_meta::provider::v1::ProviderOperationV1::Create,
            ));
        }
        self.provider_factory.create(request)
    }

    fn reopen_at_binding(
        &self,
        expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        if &self.binding_snapshot(request.schema())? != expected_binding {
            return Err(ProviderError::authority_mismatch(
                nokv_meta::provider::v1::ProviderOperationV1::Reopen,
            ));
        }
        self.provider_factory.reopen(request)
    }

    fn reopen_pending_with_old_dispatch_excluded_at_binding_v1(
        &self,
        expected_binding: &RuntimeProviderBinding<Self::InstallationIdentity>,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        let exact_binding = self
            .binding_snapshot(command.schema())
            .is_ok_and(|current| &current == expected_binding);
        if !exact_binding
            || command.expected_installation() != expected_binding.recovery_fence_installation()
        {
            return command.reject_before_execution(
                MetadataPendingRecoveryOpenNotDispatchedV1::InvalidBinding,
            );
        }
        self.provider_factory
            .reopen_pending_with_old_dispatch_excluded_v1(command)
    }
}

impl<T> MetadataCommitReceiptStoreV1 for HoltFileRuntimeBundle<T>
where
    T: MetadataCommitReceiptStoreV1 + Send + Sync,
{
    fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
        self.services.commit_receipt_qualification_v1()
    }

    fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
        self.services.frozen_runtime_bundle_digest_v1()
    }

    fn load_commit_receipt_v1(
        &self,
        store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
        self.services.load_commit_receipt_v1(store_identity)
    }

    fn persist_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptPersistCommandV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        self.services.persist_pending_commit_v1(command)
    }

    fn resolve_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptResolveCommandV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        self.services.resolve_pending_commit_v1(command)
    }

    fn poison_commit_receipt_v1(
        &self,
        command: MetadataCommitReceiptPoisonCommandV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        self.services.poison_commit_receipt_v1(command)
    }
}

impl<T> RuntimeLifecycleValidator for HoltFileRuntimeBundle<T>
where
    T: RuntimeLifecycleValidator + Send + Sync,
{
    fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
        self.services.validate()
    }

    fn poison(&self) {
        RuntimeLifecycleValidator::poison(self.services.as_ref());
    }
}

impl<T> OwnerReleaseReceipt for HoltFileRuntimeBundle<T>
where
    T: OwnerReleaseReceipt + Send + Sync,
{
    type Binding = T::Binding;

    fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError> {
        self.services.owner_release_binding()
    }

    fn preflight_owner_release_at_binding(
        &self,
        expected: &Self::Binding,
    ) -> Result<(), OwnerReleaseReceiptError> {
        self.services.preflight_owner_release_at_binding(expected)
    }

    fn persist_owner_releasing_at_binding(
        &self,
        expected: &Self::Binding,
        lease: &nokv_control::LogicalShardLease,
    ) -> Result<(), OwnerReleaseReceiptError> {
        self.services
            .persist_owner_releasing_at_binding(expected, lease)
    }
}

/// Build one file-backed Holt entry whose provider guard, exact commit receipt,
/// and neutral lifecycle validator are bound to one services instance.
/// The locator is canonicalized once, services are preflighted before provider
/// construction, and the first actual Holt object identity must match that
/// locator before it reaches the services binding. Neither the locator nor the
/// process-local services identity enters the descriptor or durable control
/// state.
pub fn holt_file_runtime_factory<T>(
    path: impl AsRef<Path>,
    services: Arc<T>,
) -> Result<Arc<dyn RuntimeFactory>, ServerError>
where
    T: HoltRuntimeGuard
        + MetadataCommitReceiptStoreV1
        + RuntimeLifecycleValidator
        + OwnerReleaseReceipt
        + 'static,
{
    let canonical_locator = canonical_holt_locator(path.as_ref())?;
    validate_holt_bundle_services(services.as_ref())?;
    let installation_identity = HoltFileInstallationIdentity {
        canonical_locator: canonical_locator.clone(),
        bundle_services: ProcessLocalServiceIdentity::of(&services),
        expected_store_object_identity: None,
    };
    let runtime_guard: Arc<dyn HoltRuntimeGuard> = Arc::new(ExactHoltRuntimeGuard {
        canonical_locator: canonical_locator.clone(),
        services: Arc::clone(&services),
    });
    let provider_factory =
        nokv_meta::built_in_holt::file_provider_factory_v1(&canonical_locator, runtime_guard);
    let bundle = HoltFileRuntimeBundle {
        provider_factory,
        services,
        installation_identity,
    };
    finish_holt_file_runtime_factory(bundle)
}

/// Build one exact existing-store Holt runtime from an already-held
/// reservation.
///
/// The reservation is consumed by value and its complete directory/lock
/// object identity participates in the process-local installation binding.
/// Provider open, exact commit receipt, lifecycle validation, and owner
/// release remain views of one concrete runtime-bundle allocation.
/// This primitive does not qualify ExactResume or any other owner transition;
/// the public lifecycle descriptor remains fail closed.
pub fn holt_reserved_existing_runtime_factory<T>(
    reservation: HoltExistingStoreReservation,
    services: Arc<T>,
) -> Result<Arc<dyn RuntimeFactory>, ServerError>
where
    T: HoltRuntimeGuard
        + MetadataCommitReceiptStoreV1
        + RuntimeLifecycleValidator
        + OwnerReleaseReceipt
        + 'static,
{
    let bundle = reserved_existing_holt_file_runtime_bundle(reservation, services)?;
    finish_holt_file_runtime_factory(bundle)
}

fn reserved_existing_holt_file_runtime_bundle<T>(
    reservation: HoltExistingStoreReservation,
    services: Arc<T>,
) -> Result<HoltFileRuntimeBundle<T>, ServerError>
where
    T: HoltRuntimeGuard
        + MetadataCommitReceiptStoreV1
        + RuntimeLifecycleValidator
        + OwnerReleaseReceipt
        + 'static,
{
    let expected_store_object_identity = reservation.expected_identity().clone();
    let canonical_locator = expected_store_object_identity
        .canonical_locator()
        .to_owned();
    validate_reserved_holt_locator(&canonical_locator)?;
    validate_holt_bundle_services(services.as_ref())?;
    let installation_identity = HoltFileInstallationIdentity {
        canonical_locator: canonical_locator.clone(),
        bundle_services: ProcessLocalServiceIdentity::of(&services),
        expected_store_object_identity: Some(expected_store_object_identity),
    };
    let runtime_guard: Arc<dyn HoltRuntimeGuard> = Arc::new(ExactHoltRuntimeGuard {
        canonical_locator,
        services: Arc::clone(&services),
    });
    let provider_factory = nokv_meta::built_in_holt::reserved_existing_file_provider_factory_v1(
        reservation,
        runtime_guard,
    );
    Ok(HoltFileRuntimeBundle {
        provider_factory,
        services,
        installation_identity,
    })
}

fn finish_holt_file_runtime_factory<T>(
    bundle: HoltFileRuntimeBundle<T>,
) -> Result<Arc<dyn RuntimeFactory>, ServerError>
where
    T: HoltRuntimeGuard
        + MetadataCommitReceiptStoreV1
        + RuntimeLifecycleValidator
        + OwnerReleaseReceipt
        + 'static,
{
    let descriptor = descriptor_from_factory(
        holt_profile_id(),
        holt_profile_fingerprint(),
        &bundle,
        LifecycleCapabilities::new(OwnerReceiptMode::ExternalOwnerJournal, &HOLT_TRANSITIONS),
        RuntimeConsistencyDomain::ShardLocal,
    )?;
    let resolved = ResolvedRuntime::external_owner_journal(descriptor.clone(), bundle)
        .map_err(runtime_factory_error)?;
    Ok(Arc::new(ExactRuntimeFactory {
        descriptor,
        resolved,
    }))
}

fn validate_reserved_holt_locator(path: &Path) -> Result<(), ServerError> {
    use std::path::Component;

    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(ServerError::InvalidOptions(
            "reserved Holt metadata locator must be canonical and absolute".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_holt_locator(path: &Path) -> Result<PathBuf, ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ServerError::InvalidOptions(
                    "Holt metadata locator must be a directory or an absent path".to_owned(),
                ));
            }
            std::fs::canonicalize(path).map_err(|error| {
                ServerError::InvalidOptions(format!(
                    "Holt metadata locator cannot be resolved ({:?})",
                    error.kind()
                ))
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let name = path.file_name().ok_or_else(|| {
                ServerError::InvalidOptions(
                    "Holt metadata locator must name one installation".to_owned(),
                )
            })?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let canonical_parent = std::fs::canonicalize(parent).map_err(|parent_error| {
                ServerError::InvalidOptions(format!(
                    "Holt metadata locator parent cannot be resolved ({:?})",
                    parent_error.kind()
                ))
            })?;
            Ok(canonical_parent.join(name))
        }
        Err(error) => Err(ServerError::InvalidOptions(format!(
            "Holt metadata locator cannot be inspected ({:?})",
            error.kind()
        ))),
    }
}

fn validate_holt_bundle_services<T>(services: &T) -> Result<(), ServerError>
where
    T: HoltRuntimeGuard + MetadataCommitReceiptStoreV1 + RuntimeLifecycleValidator,
{
    HoltRuntimeGuard::validate_runtime(services).map_err(|_| holt_bundle_services_rejected())?;
    RuntimeLifecycleValidator::validate(services).map_err(|_| holt_bundle_services_rejected())?;
    if services.commit_receipt_qualification_v1() != MetadataCommitReceiptQualificationV1::Durable
        || services
            .frozen_runtime_bundle_digest_v1()
            .iter()
            .all(|byte| *byte == 0)
    {
        return Err(holt_bundle_services_rejected());
    }
    Ok(())
}

fn holt_bundle_services_rejected() -> ServerError {
    ServerError::InvalidOptions(
        "Holt runtime bundle services rejected the configured installation".to_owned(),
    )
}

fn descriptor_from_factory(
    profile_id: MetadataProviderProfileId,
    profile_fingerprint: [u8; SHA256_BYTES],
    provider_factory: &dyn MetadataProviderFactoryV1,
    lifecycle: LifecycleCapabilities,
    consistency_domain: RuntimeConsistencyDomain,
) -> Result<RuntimeDescriptor, ServerError> {
    let schema = nokv_meta::workspace::canonical_provider_schema_v1();
    let offer = provider_factory.contract_offer(&schema).map_err(|_| {
        ServerError::InvalidOptions(
            "metadata provider contract offer could not be inspected".to_owned(),
        )
    })?;
    RuntimeDescriptor::new(
        profile_id,
        profile_fingerprint,
        offer,
        lifecycle,
        consistency_domain,
    )
    .map_err(|error| ServerError::InvalidOptions(error.to_string()))
}

struct ExactRuntimeFactory {
    descriptor: RuntimeDescriptor,
    resolved: ResolvedRuntime,
}

impl RuntimeFactory for ExactRuntimeFactory {
    fn descriptor(&self) -> RuntimeDescriptor {
        self.descriptor.clone()
    }

    fn resolve(&self) -> Result<ResolvedRuntime, RuntimeFactoryError> {
        self.resolved.validate_provider_binding()?;
        Ok(self.resolved.clone())
    }
}

#[cfg(feature = "foundationdb-provider")]
struct DescriptorOnlyRuntimeFactory {
    descriptor: RuntimeDescriptor,
}

#[cfg(feature = "foundationdb-provider")]
impl RuntimeFactory for DescriptorOnlyRuntimeFactory {
    fn descriptor(&self) -> RuntimeDescriptor {
        self.descriptor.clone()
    }

    fn resolve(&self) -> Result<ResolvedRuntime, RuntimeFactoryError> {
        Err(RuntimeFactoryError::new(
            RuntimeFactoryErrorCode::Unavailable,
        ))
    }
}

fn runtime_factory_error(error: RuntimeFactoryError) -> ServerError {
    ServerError::InvalidOptions(error.to_string())
}

fn holt_profile_id() -> MetadataProviderProfileId {
    MetadataProviderProfileId::new(HOLT_LOCAL_METADATA_PROFILE_ID)
        .expect("built-in Holt metadata profile id is canonical")
}

fn holt_profile_fingerprint() -> [u8; SHA256_BYTES] {
    derive_profile_digest(
        PROFILE_FINGERPRINT_DOMAIN,
        &[
            HOLT_LOCAL_METADATA_PROFILE_ID.as_bytes(),
            HOLT_LOCAL_PROFILE_CONTRACT,
        ],
    )
}

impl RuntimeDescriptor {
    /// Construct the exact generation-one authority persisted by fresh-root
    /// provisioning. This derivation is provider-neutral and depends only on
    /// the immutable descriptor.
    #[must_use]
    pub fn initial_authority(&self, logical_shard_id: LogicalShardId) -> MetadataAuthorityRecord {
        MetadataAuthorityRecord {
            logical_shard_id,
            record_revision: MetadataAuthorityRevision::new(1)
                .expect("initial metadata authority revision is non-zero"),
            authority_generation: MetadataAuthorityGeneration::new(1)
                .expect("initial metadata authority generation is non-zero"),
            active: self.initial_binding(logical_shard_id),
            migration: None,
        }
    }

    pub(crate) fn validate_authority(
        &self,
        authority: &MetadataAuthorityRecord,
    ) -> Result<MetadataStoreIdentity, ServerError> {
        if authority.active.provider_profile_id != *self.profile_id() {
            return Err(ServerError::InvalidBootstrap(format!(
                "control metadata authority selects profile {:?}, runtime resolved {:?}",
                authority.active.provider_profile_id.as_str(),
                self.profile_id().as_str()
            )));
        }
        if authority.active.profile_fingerprint != *self.profile_fingerprint() {
            return Err(ServerError::InvalidBootstrap(
                "control metadata authority profile fingerprint does not match the resolved runtime profile"
                    .to_owned(),
            ));
        }
        if authority.active.contract_digest != self.contract_digest() {
            return Err(ServerError::InvalidBootstrap(
                "control metadata authority contract digest does not match this nokv-meta build"
                    .to_owned(),
            ));
        }
        let expected_consistency_domain =
            self.consistency_domain_id(authority.logical_shard_id, authority.active.authority_id);
        if authority.active.consistency_domain_id != expected_consistency_domain {
            return Err(ServerError::InvalidBootstrap(
                "control metadata authority consistency domain does not match the resolved runtime"
                    .to_owned(),
            ));
        }
        if authority
            .active
            .authority_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        {
            return Err(ServerError::InvalidBootstrap(
                "control metadata authority has an all-zero authority id".to_owned(),
            ));
        }
        let generation_one_source = authority
            .migration
            .as_ref()
            .map_or(&authority.active, |migration| &migration.source);
        if authority.authority_generation.get() == 1
            && generation_one_source != &self.initial_binding(authority.logical_shard_id)
        {
            return Err(ServerError::InvalidBootstrap(
                "generation-one metadata authority id does not match the resolved runtime derivation"
                    .to_owned(),
            ));
        }
        Ok(MetadataStoreIdentity {
            logical_shard_id: authority.logical_shard_id,
            authority_id: authority.active.authority_id,
            authority_generation: authority.authority_generation,
            consistency_domain_id: authority.active.consistency_domain_id,
            profile_fingerprint: authority.active.profile_fingerprint,
            contract_digest: authority.active.contract_digest,
        })
    }

    fn initial_binding(&self, logical_shard_id: LogicalShardId) -> MetadataAuthorityBinding {
        let authority_digest = match self.consistency_domain() {
            RuntimeConsistencyDomain::ShardLocal => derive_profile_digest(
                AUTHORITY_ID_DOMAIN,
                &[
                    self.profile_id().as_str().as_bytes(),
                    logical_shard_id.as_bytes(),
                    self.profile_fingerprint(),
                    self.contract_digest().as_bytes(),
                ],
            ),
            RuntimeConsistencyDomain::Shared(domain) => derive_profile_digest(
                AUTHORITY_ID_DOMAIN,
                &[
                    self.profile_id().as_str().as_bytes(),
                    logical_shard_id.as_bytes(),
                    self.profile_fingerprint(),
                    domain.as_bytes(),
                    self.contract_digest().as_bytes(),
                ],
            ),
        };
        let authority_id = MetadataAuthorityId::from_bytes(
            authority_digest[..nokv_types::FIXED_ID_BYTES]
                .try_into()
                .expect("digest prefix has the metadata authority id width"),
        );
        MetadataAuthorityBinding {
            authority_id,
            provider_profile_id: self.profile_id().clone(),
            profile_fingerprint: *self.profile_fingerprint(),
            consistency_domain_id: self.consistency_domain_id(logical_shard_id, authority_id),
            contract_digest: self.contract_digest(),
        }
    }

    fn consistency_domain_id(
        &self,
        logical_shard_id: LogicalShardId,
        authority_id: MetadataAuthorityId,
    ) -> ConsistencyDomainId {
        match self.consistency_domain() {
            RuntimeConsistencyDomain::Shared(domain) => domain,
            RuntimeConsistencyDomain::ShardLocal => {
                let digest = derive_profile_digest(
                    CONSISTENCY_DOMAIN_ID_DOMAIN,
                    &[
                        self.profile_id().as_str().as_bytes(),
                        logical_shard_id.as_bytes(),
                        authority_id.as_bytes(),
                        self.profile_fingerprint(),
                    ],
                );
                ConsistencyDomainId::from_bytes(
                    digest[..nokv_types::FIXED_ID_BYTES]
                        .try_into()
                        .expect("digest prefix has the consistency-domain id width"),
                )
            }
        }
    }
}

/// Secret-free, fully resolved FoundationDB runtime configuration.
///
/// The canonical cluster-file path remains process-local and is deliberately
/// redacted from `Debug`. Only a domain-separated digest of the stable cluster
/// identity survives descriptor construction.
#[cfg(feature = "foundationdb-provider")]
#[derive(Clone, PartialEq, Eq)]
pub struct FoundationDbRuntimeConfig {
    canonical_cluster_file: PathBuf,
    cluster_identity_digest: [u8; SHA256_BYTES],
    namespace: String,
    transaction_policy: FoundationDbTransactionPolicy,
}

#[cfg(feature = "foundationdb-provider")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FoundationDbTransactionPolicy {
    pub transaction_budget_bytes: usize,
    pub transaction_timeout_ms: u32,
}

#[cfg(feature = "foundationdb-provider")]
impl Default for FoundationDbTransactionPolicy {
    fn default() -> Self {
        Self {
            transaction_budget_bytes: 1_000_000,
            transaction_timeout_ms: 5_000,
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbRuntimeConfig {
    pub fn from_cluster_file(
        cluster_file: impl AsRef<Path>,
        namespace: impl Into<String>,
        transaction_policy: FoundationDbTransactionPolicy,
    ) -> Result<Self, ServerError> {
        let canonical_cluster_file = canonical_cluster_file(cluster_file.as_ref())?;
        let cluster_identity_digest = cluster_key_digest(&canonical_cluster_file)?;
        Self::finish(
            canonical_cluster_file,
            cluster_identity_digest,
            namespace.into(),
            transaction_policy,
        )
    }

    pub fn with_explicit_stable_id(
        cluster_file: impl AsRef<Path>,
        stable_id: &str,
        namespace: impl Into<String>,
        transaction_policy: FoundationDbTransactionPolicy,
    ) -> Result<Self, ServerError> {
        validate_token(
            stable_id.as_bytes(),
            MAX_EXPLICIT_STABLE_ID_BYTES,
            "FoundationDB stable cluster id",
        )?;
        let canonical_cluster_file = canonical_cluster_file(cluster_file.as_ref())?;
        let cluster_identity_digest =
            derive_profile_digest(FDB_EXPLICIT_ID_DIGEST_DOMAIN, &[stable_id.as_bytes()]);
        Self::finish(
            canonical_cluster_file,
            cluster_identity_digest,
            namespace.into(),
            transaction_policy,
        )
    }

    fn finish(
        canonical_cluster_file: PathBuf,
        cluster_identity_digest: [u8; SHA256_BYTES],
        namespace: String,
        transaction_policy: FoundationDbTransactionPolicy,
    ) -> Result<Self, ServerError> {
        validate_namespace(&namespace)?;
        provider_config(transaction_policy)
            .validate()
            .map_err(|error| {
                ServerError::InvalidOptions(format!(
                    "invalid FoundationDB transaction policy: {error}"
                ))
            })?;
        Ok(Self {
            canonical_cluster_file,
            cluster_identity_digest,
            namespace,
            transaction_policy,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub const fn transaction_budget_bytes(&self) -> usize {
        self.transaction_policy.transaction_budget_bytes
    }

    pub const fn transaction_timeout_ms(&self) -> u32 {
        self.transaction_policy.transaction_timeout_ms
    }

    fn consistency_domain_id(&self) -> ConsistencyDomainId {
        let digest = derive_profile_digest(
            CONSISTENCY_DOMAIN_ID_DOMAIN,
            &[
                FDB_CONSISTENCY_DOMAIN_FAMILY,
                &self.cluster_identity_digest,
                self.namespace.as_bytes(),
            ],
        );
        ConsistencyDomainId::from_bytes(
            digest[..nokv_types::FIXED_ID_BYTES]
                .try_into()
                .expect("digest prefix has the consistency-domain id width"),
        )
    }

    fn profile_fingerprint(&self) -> [u8; SHA256_BYTES] {
        let budget = u64::try_from(self.transaction_budget_bytes())
            .expect("validated FoundationDB transaction budget fits u64")
            .to_be_bytes();
        let timeout = self.transaction_timeout_ms().to_be_bytes();
        derive_profile_digest(
            PROFILE_FINGERPRINT_DOMAIN,
            &[
                FOUNDATIONDB_METADATA_PROFILE_ID.as_bytes(),
                &self.cluster_identity_digest,
                self.namespace.as_bytes(),
                FDB_PROFILE_API_CONTRACT,
                FDB_PROFILE_NAMESPACE_CONTRACT,
                FDB_PROFILE_TRANSACTION_CONTRACT,
                &budget,
                &timeout,
            ],
        )
    }
}

#[cfg(feature = "foundationdb-provider")]
impl fmt::Debug for FoundationDbRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FoundationDbRuntimeConfig")
            .field("canonical_cluster_file", &"<redacted>")
            .field("cluster_identity", &"sha256:<redacted>")
            .field("namespace", &self.namespace)
            .field("transaction_budget_bytes", &self.transaction_budget_bytes())
            .field("transaction_timeout_ms", &self.transaction_timeout_ms())
            .finish()
    }
}

#[cfg(feature = "foundationdb-provider")]
pub fn foundationdb_runtime_descriptor(
    config: &FoundationDbRuntimeConfig,
) -> Result<RuntimeDescriptor, ServerError> {
    let provider_offer = nokv_meta::built_in_foundationdb::contract_offer_v1(provider_config(
        config.transaction_policy,
    ))
    .map_err(|_| {
        ServerError::InvalidOptions(
            "FoundationDB provider contract offer could not be inspected".to_owned(),
        )
    })?;
    RuntimeDescriptor::new(
        MetadataProviderProfileId::new(FOUNDATIONDB_METADATA_PROFILE_ID)
            .expect("built-in FoundationDB profile id is canonical"),
        config.profile_fingerprint(),
        provider_offer,
        LifecycleCapabilities::new(OwnerReceiptMode::ProviderDurable, &FOUNDATIONDB_TRANSITIONS),
        RuntimeConsistencyDomain::Shared(config.consistency_domain_id()),
    )
    .map_err(|error| ServerError::InvalidOptions(error.to_string()))
}

/// Compose the current FoundationDB descriptor without starting the network
/// runtime or opening a database. Generic admission is intentionally expected
/// to reject this entry before `resolve` while its public offer is incomplete.
#[cfg(feature = "foundationdb-provider")]
pub fn foundationdb_runtime_factory(
    config: &FoundationDbRuntimeConfig,
) -> Result<Arc<dyn RuntimeFactory>, ServerError> {
    Ok(Arc::new(DescriptorOnlyRuntimeFactory {
        descriptor: foundationdb_runtime_descriptor(config)?,
    }))
}

#[cfg(feature = "foundationdb-provider")]
fn provider_config(
    policy: FoundationDbTransactionPolicy,
) -> nokv_meta::built_in_foundationdb::FoundationDbProviderConfig {
    nokv_meta::built_in_foundationdb::FoundationDbProviderConfig {
        transaction_budget_bytes: policy.transaction_budget_bytes,
        transaction_timeout_ms: policy.transaction_timeout_ms,
    }
}

fn derive_profile_digest(domain: &[u8], fields: &[&[u8]]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(
        u64::try_from(domain.len())
            .expect("profile hash domain length fits u64")
            .to_be_bytes(),
    );
    hasher.update(domain);
    for field in fields {
        hasher.update(
            u64::try_from(field.len())
                .expect("profile hash field length fits u64")
                .to_be_bytes(),
        );
        hasher.update(field);
    }
    hasher.finalize().into()
}

#[cfg(feature = "foundationdb-provider")]
fn canonical_cluster_file(path: &Path) -> Result<PathBuf, ServerError> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        ServerError::InvalidOptions(format!(
            "FoundationDB cluster file cannot be resolved ({:?})",
            error.kind()
        ))
    })?;
    let metadata = std::fs::metadata(&canonical).map_err(|error| {
        ServerError::InvalidOptions(format!(
            "FoundationDB cluster file metadata is unavailable ({:?})",
            error.kind()
        ))
    })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_CLUSTER_FILE_BYTES {
        return Err(ServerError::InvalidOptions(
            "FoundationDB cluster file must be a nonempty regular file no larger than 8 KiB"
                .to_owned(),
        ));
    }
    Ok(canonical)
}

#[cfg(feature = "foundationdb-provider")]
fn cluster_key_digest(cluster_file: &Path) -> Result<[u8; SHA256_BYTES], ServerError> {
    let bytes = std::fs::read(cluster_file).map_err(|error| {
        ServerError::InvalidOptions(format!(
            "FoundationDB cluster file cannot be read ({:?})",
            error.kind()
        ))
    })?;
    let line = trim_single_line(&bytes)?;
    let separator = line.iter().position(|byte| *byte == b'@').ok_or_else(|| {
        ServerError::InvalidOptions(
            "FoundationDB cluster file has no canonical cluster-key separator".to_owned(),
        )
    })?;
    let (cluster_key, coordinators_with_separator) = line.split_at(separator);
    if coordinators_with_separator.len() <= 1 {
        return Err(ServerError::InvalidOptions(
            "FoundationDB cluster file has no coordinator section".to_owned(),
        ));
    }
    let cluster_separator = cluster_key
        .iter()
        .position(|byte| *byte == b':')
        .ok_or_else(|| {
            ServerError::InvalidOptions(
                "FoundationDB cluster key has no description/id separator".to_owned(),
            )
        })?;
    if cluster_key[cluster_separator + 1..].contains(&b':') {
        return Err(ServerError::InvalidOptions(
            "FoundationDB cluster key has multiple description/id separators".to_owned(),
        ));
    }
    let description = &cluster_key[..cluster_separator];
    let cluster_id = &cluster_key[cluster_separator + 1..];
    validate_token(
        description,
        MAX_CLUSTER_COMPONENT_BYTES,
        "FoundationDB cluster description",
    )?;
    validate_token(
        cluster_id,
        MAX_CLUSTER_COMPONENT_BYTES,
        "FoundationDB cluster id",
    )?;
    Ok(derive_profile_digest(
        FDB_CLUSTER_KEY_DIGEST_DOMAIN,
        &[description, cluster_id],
    ))
}

#[cfg(feature = "foundationdb-provider")]
fn trim_single_line(bytes: &[u8]) -> Result<&[u8], ServerError> {
    let line = bytes
        .strip_suffix(b"\r\n")
        .or_else(|| bytes.strip_suffix(b"\n"))
        .unwrap_or(bytes);
    if line.is_empty() || line.contains(&b'\n') || line.contains(&b'\r') {
        return Err(ServerError::InvalidOptions(
            "FoundationDB cluster file must contain exactly one canonical record".to_owned(),
        ));
    }
    Ok(line)
}

#[cfg(feature = "foundationdb-provider")]
fn validate_namespace(namespace: &str) -> Result<(), ServerError> {
    let bytes = namespace.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_NAMESPACE_BYTES {
        return Err(ServerError::InvalidOptions(format!(
            "FoundationDB namespace must contain 1..={MAX_NAMESPACE_BYTES} bytes"
        )));
    }
    let canonical = bytes.iter().all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(*byte, b'-' | b'_' | b'.' | b'/')
    }) && bytes.first() != Some(&b'/')
        && bytes.last() != Some(&b'/')
        && !bytes.windows(2).any(|window| window == b"//");
    if !canonical {
        return Err(ServerError::InvalidOptions(
            "FoundationDB namespace must be a canonical lowercase token/path".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(feature = "foundationdb-provider")]
fn validate_token(bytes: &[u8], maximum: usize, name: &str) -> Result<(), ServerError> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ServerError::InvalidOptions(format!(
            "{name} must contain 1..={maximum} bytes"
        )));
    }
    if !bytes
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.'))
    {
        return Err(ServerError::InvalidOptions(format!(
            "{name} must be a canonical ASCII token"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;

    const ALL_LIFECYCLE_TRANSITIONS: [(crate::OpenIntent, LifecycleTransition); 6] = [
        (
            crate::OpenIntent::CreateFresh,
            LifecycleTransition::FreshCreate,
        ),
        (
            crate::OpenIntent::ReopenExisting,
            LifecycleTransition::ExactResume,
        ),
        (
            crate::OpenIntent::ReopenExisting,
            LifecycleTransition::SuccessorReopen,
        ),
        (
            crate::OpenIntent::ReconcilePreparedCreate,
            LifecycleTransition::PreparedFirstCreate,
        ),
        (
            crate::OpenIntent::ReconcilePreparedCreate,
            LifecycleTransition::PreparedSuccessorCreate,
        ),
        (
            crate::OpenIntent::ReconcilePreparedCreate,
            LifecycleTransition::PreparedResumeOrSuccessor,
        ),
    ];

    #[derive(Default)]
    struct RecoveryFenceDriftControl {
        drifted: AtomicBool,
        drift_after_next_open: AtomicBool,
        typed_open_calls: AtomicUsize,
    }

    struct DriftingRecoveryFenceFactory {
        inner: Arc<dyn MetadataCommitRecoveryFenceFactoryV1>,
        control: Arc<RecoveryFenceDriftControl>,
    }

    impl MetadataProviderFactoryV1 for DriftingRecoveryFenceFactory {
        fn contract_offer(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<ProviderContractOfferV1, ProviderError> {
            self.inner.contract_offer(schema)
        }

        fn create(
            &self,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.inner.create(request)
        }

        fn reopen(
            &self,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.inner.reopen(request)
        }
    }

    impl MetadataCommitRecoveryFenceFactoryV1 for DriftingRecoveryFenceFactory {
        fn old_dispatch_exclusion_installation_v1(
            &self,
        ) -> nokv_meta::workspace::MetadataOldDispatchExclusionInstallationV1 {
            if self.control.drifted.load(Ordering::Acquire) {
                nokv_meta::workspace::MetadataOldDispatchExclusionInstallationV1::unsupported()
            } else {
                self.inner.old_dispatch_exclusion_installation_v1()
            }
        }

        fn reopen_pending_with_old_dispatch_excluded_v1(
            &self,
            command: MetadataPendingRecoveryOpenCommandV1,
        ) -> MetadataPendingRecoveryOpenOutcomeV1 {
            self.control.typed_open_calls.fetch_add(1, Ordering::AcqRel);
            let outcome = self
                .inner
                .reopen_pending_with_old_dispatch_excluded_v1(command);
            if self
                .control
                .drift_after_next_open
                .swap(false, Ordering::AcqRel)
            {
                self.control.drifted.store(true, Ordering::Release);
            }
            outcome
        }
    }

    fn install_recovery_fence_drift(
        bundle: &mut HoltFileRuntimeBundle<BindingServices>,
    ) -> Arc<RecoveryFenceDriftControl> {
        let control = Arc::new(RecoveryFenceDriftControl::default());
        bundle.provider_factory = Arc::new(DriftingRecoveryFenceFactory {
            inner: Arc::clone(&bundle.provider_factory),
            control: Arc::clone(&control),
        });
        control
    }

    fn shard(fill: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([fill; nokv_types::FIXED_ID_BYTES])
    }

    fn create_existing_holt_runtime_store(
        locator: &Path,
        services: Arc<BindingServices>,
        shard_fill: u8,
    ) -> (MetadataStoreIdentity, HoltStoreObjectIdentity) {
        let factory = holt_file_runtime_factory(locator, Arc::clone(&services)).unwrap();
        let descriptor = factory.descriptor();
        let identity = descriptor
            .validate_authority(&descriptor.initial_authority(shard(shard_fill)))
            .unwrap();
        let runtime = factory.resolve().unwrap();
        let store = runtime
            .open_store(crate::OpenIntent::CreateFresh, identity)
            .unwrap();
        let held_identity = services.bound_store_identity();
        drop(store);
        drop(runtime);
        drop(factory);
        (identity, held_identity)
    }

    fn create_existing_holt_runtime_store_with_pending_receipt(
        locator: &Path,
        services: Arc<BindingServices>,
        shard_fill: u8,
    ) -> (MetadataStoreIdentity, HoltStoreObjectIdentity) {
        let factory = holt_file_runtime_factory(locator, Arc::clone(&services)).unwrap();
        let descriptor = factory.descriptor();
        let identity = descriptor
            .validate_authority(&descriptor.initial_authority(shard(shard_fill)))
            .unwrap();
        let runtime = factory.resolve().unwrap();
        let store = runtime
            .open_store(crate::OpenIntent::CreateFresh, identity)
            .unwrap();
        services.receipt.recover_next_persist_after_effect();
        assert_eq!(
            store.advance_owner_epoch(None, nokv_types::OwnerEpoch::new(1).unwrap()),
            Err(nokv_meta::workspace::AgentMetadataError::CommitReceiptRecoveryRequired)
        );
        let held_identity = services.bound_store_identity();
        drop(store);
        drop(runtime);
        drop(factory);
        (identity, held_identity)
    }

    #[derive(Default)]
    struct BindingServices {
        bound_locator: Mutex<Option<PathBuf>>,
        bound_store_identity: Mutex<Option<HoltStoreObjectIdentity>>,
        guard_bindings: AtomicUsize,
        guard_validations: AtomicUsize,
        reject_next_guard_validation: std::sync::atomic::AtomicBool,
        panic_next_guard_validation: std::sync::atomic::AtomicBool,
        receipt: crate::runtime_registry::RecordingCommitReceiptStoreV1,
        validations: AtomicUsize,
        release_views: AtomicUsize,
    }

    impl BindingServices {
        fn bound_store_identity(&self) -> HoltStoreObjectIdentity {
            self.bound_store_identity
                .lock()
                .unwrap()
                .clone()
                .expect("Holt provider must bind its actual held identity")
        }

        fn reject_next_guard_validation(&self) {
            self.reject_next_guard_validation
                .store(true, Ordering::Release);
        }

        fn panic_next_guard_validation(&self) {
            self.panic_next_guard_validation
                .store(true, Ordering::Release);
        }
    }

    impl HoltRuntimeGuard for BindingServices {
        fn bind_store(
            &self,
            identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            self.guard_bindings.fetch_add(1, Ordering::SeqCst);
            let mut binding = self.bound_store_identity.lock().unwrap();
            if binding
                .as_ref()
                .is_some_and(|expected| expected != identity)
            {
                return Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Rejected);
            }
            *binding = Some(identity.clone());
            *self.bound_locator.lock().unwrap() = Some(identity.canonical_locator().to_owned());
            Ok(())
        }

        fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            self.guard_validations.fetch_add(1, Ordering::SeqCst);
            if self
                .panic_next_guard_validation
                .swap(false, Ordering::AcqRel)
            {
                panic!("injected reserved Holt runtime validation unwind");
            }
            if self
                .reject_next_guard_validation
                .swap(false, Ordering::AcqRel)
            {
                return Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Rejected);
            }
            Ok(())
        }

        fn poison(&self) {}
    }

    impl MetadataCommitReceiptStoreV1 for BindingServices {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            self.receipt.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.receipt.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            self.receipt.load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            self.receipt.persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            self.receipt.resolve_pending_commit_v1(command)
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            self.receipt.poison_commit_receipt_v1(command)
        }
    }

    impl RuntimeLifecycleValidator for BindingServices {
        fn validate(&self) -> Result<(), crate::RuntimeLifecycleValidationError> {
            self.validations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn poison(&self) {}
    }

    impl OwnerReleaseReceipt for BindingServices {
        type Binding = ();

        fn owner_release_binding(&self) -> Result<Self::Binding, OwnerReleaseReceiptError> {
            self.release_views.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn preflight_owner_release_at_binding(
            &self,
            _expected: &Self::Binding,
        ) -> Result<(), OwnerReleaseReceiptError> {
            self.release_views.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn persist_owner_releasing_at_binding(
            &self,
            _expected: &Self::Binding,
            _lease: &nokv_control::LogicalShardLease,
        ) -> Result<(), OwnerReleaseReceiptError> {
            self.release_views.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct RejectingBundleServices {
        reject_runtime: bool,
        reject_journal: bool,
    }

    impl HoltRuntimeGuard for RejectingBundleServices {
        fn bind_store(
            &self,
            _identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            panic!("bundle service preflight must precede Holt store binding")
        }

        fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            if self.reject_runtime {
                Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Rejected)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {}
    }

    impl MetadataCommitReceiptStoreV1 for RejectingBundleServices {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            if self.reject_journal {
                MetadataCommitReceiptQualificationV1::UntrackedStandalone
            } else {
                MetadataCommitReceiptQualificationV1::Durable
            }
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            [0xb4; SHA256_BYTES]
        }

        fn load_commit_receipt_v1(
            &self,
            _store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            panic!("bundle service preflight must precede receipt load")
        }

        fn persist_pending_commit_v1(
            &self,
            _command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            panic!("bundle service preflight must precede receipt persistence")
        }

        fn resolve_pending_commit_v1(
            &self,
            _command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            panic!("bundle service preflight must precede receipt resolution")
        }

        fn poison_commit_receipt_v1(
            &self,
            _command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            panic!("bundle service preflight must precede receipt poison")
        }
    }

    impl RuntimeLifecycleValidator for RejectingBundleServices {
        fn validate(&self) -> Result<(), RuntimeLifecycleValidationError> {
            if self.reject_runtime {
                Err(RuntimeLifecycleValidationError::Rejected)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {}
    }

    impl OwnerReleaseReceipt for RejectingBundleServices {
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
            _lease: &nokv_control::LogicalShardLease,
        ) -> Result<(), OwnerReleaseReceiptError> {
            Ok(())
        }
    }

    #[test]
    fn holt_profile_derivation_preserves_the_frozen_golden() {
        let descriptor = holt_runtime_descriptor().unwrap();
        let authority = descriptor.initial_authority(shard(2));
        assert_eq!(
            descriptor.profile_id().as_str(),
            HOLT_LOCAL_METADATA_PROFILE_ID
        );
        assert_eq!(
            authority.active.profile_fingerprint,
            [
                0xeb, 0x56, 0xd2, 0x2c, 0x7e, 0xa7, 0x93, 0x17, 0x91, 0xe6, 0x7b, 0x6d, 0xf3, 0x60,
                0xae, 0x57, 0xe8, 0x16, 0x37, 0xe3, 0xa8, 0x37, 0x67, 0x87, 0x70, 0xf5, 0xe8, 0x8a,
                0x4b, 0xf5, 0x6d, 0xb2,
            ]
        );
        assert_eq!(
            authority.active.authority_id.as_bytes(),
            &[
                0xfc, 0xb0, 0x16, 0x1f, 0x11, 0x42, 0x7e, 0x92, 0x58, 0x19, 0x0f, 0x59, 0x72, 0x0f,
                0x56, 0x52,
            ]
        );
        assert_eq!(
            authority.active.consistency_domain_id.as_bytes(),
            &[
                0xcf, 0x97, 0xe7, 0x9c, 0x29, 0xb2, 0x4c, 0xa2, 0xf2, 0x7f, 0xbf, 0xb2, 0x61, 0xec,
                0xb0, 0xff,
            ]
        );
        assert_ne!(
            authority.active.authority_id,
            descriptor.initial_authority(shard(3)).active.authority_id
        );
        assert_ne!(
            authority.active.consistency_domain_id,
            descriptor
                .initial_authority(shard(3))
                .active
                .consistency_domain_id
        );
    }

    #[test]
    fn production_holt_lifecycle_remains_fail_closed_for_every_transition() {
        let descriptor = holt_runtime_descriptor().unwrap();
        for (intent, transition) in ALL_LIFECYCLE_TRANSITIONS {
            assert_eq!(
                descriptor
                    .lifecycle()
                    .classify_bootstrap(intent, transition),
                Err(crate::AdmissionCode::TransitionUnsupported)
            );
        }
    }

    #[test]
    fn holt_factory_binds_provider_receipt_and_validator_to_one_service() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let services = Arc::new(BindingServices::default());
        let factory = holt_file_runtime_factory(&locator, Arc::clone(&services)).unwrap();
        let descriptor = factory.descriptor();
        let registry = crate::RuntimeRegistry::new(vec![factory]).unwrap();
        let runtime = registry.resolve(descriptor.profile_id()).unwrap();
        runtime.validate_lifecycle().unwrap();
        let authority = descriptor.initial_authority(shard(7));
        let identity = descriptor.validate_authority(&authority).unwrap();
        let _store = runtime
            .open_store(crate::OpenIntent::CreateFresh, identity)
            .unwrap();

        assert!(services.validations.load(Ordering::SeqCst) >= 2);
        assert!(services.guard_validations.load(Ordering::SeqCst) >= 1);
        assert!(services.receipt.load_calls() >= 1);
        assert_eq!(services.guard_bindings.load(Ordering::SeqCst), 1);
        assert!(services.receipt.resolve_calls() >= 1);
        assert_eq!(
            services.bound_locator.lock().unwrap().as_deref(),
            Some(std::fs::canonicalize(locator).unwrap().as_path())
        );
    }

    #[test]
    fn reserved_holt_factory_freezes_full_identity_and_one_service_allocation() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let services = Arc::new(BindingServices::default());
        let (identity, expected_store_identity) =
            create_existing_holt_runtime_store_with_pending_receipt(
                &locator,
                Arc::clone(&services),
                11,
            );
        let receipt_loads_before = services.receipt.load_calls();
        let lifecycle_validations_before = services.validations.load(Ordering::SeqCst);
        let release_views_before = services.release_views.load(Ordering::SeqCst);
        let reservation = nokv_meta::built_in_holt::acquire_existing_file_store_reservation_v1(
            expected_store_identity.clone(),
        )
        .unwrap();
        let bundle =
            reserved_existing_holt_file_runtime_bundle(reservation, Arc::clone(&services)).unwrap();
        let binding = bundle
            .binding_snapshot(&nokv_meta::workspace::canonical_provider_schema_v1())
            .unwrap();
        assert!(
            binding.installation().bundle_services == ProcessLocalServiceIdentity::of(&services)
        );
        assert_eq!(
            binding.installation().canonical_locator,
            expected_store_identity.canonical_locator()
        );
        assert_eq!(
            binding
                .installation()
                .expected_store_object_identity
                .as_ref(),
            Some(&expected_store_identity)
        );

        let factory = finish_holt_file_runtime_factory(bundle).unwrap();
        let descriptor = factory.descriptor();
        for (intent, transition) in ALL_LIFECYCLE_TRANSITIONS {
            assert_eq!(
                descriptor
                    .lifecycle()
                    .classify_bootstrap(intent, transition),
                Err(crate::AdmissionCode::TransitionUnsupported)
            );
        }
        let runtime = factory.resolve().unwrap();
        runtime.validate_lifecycle().unwrap();
        runtime.preflight_owner_release().unwrap();
        let recovery_only = runtime
            .open_store(crate::OpenIntent::ReopenExisting, identity)
            .err()
            .expect("the dirty reserved allocation must remain recovery-only");
        assert!(matches!(
            recovery_only,
            crate::runtime_registry::RuntimeOpenError::Metadata(
                nokv_meta::workspace::AgentMetadataError::CommitReceiptRecoveryRequired
            )
        ));

        assert!(services.guard_bindings.load(Ordering::SeqCst) >= 2);
        assert!(services.receipt.load_calls() > receipt_loads_before);
        assert!(services.validations.load(Ordering::SeqCst) > lifecycle_validations_before);
        assert!(services.release_views.load(Ordering::SeqCst) > release_views_before);
        assert_eq!(services.bound_store_identity(), expected_store_identity);
    }

    #[test]
    fn frozen_reserved_recovery_rejects_capability_drift_before_typed_open() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let services = Arc::new(BindingServices::default());
        let (identity, expected_store_identity) =
            create_existing_holt_runtime_store_with_pending_receipt(
                &locator,
                Arc::clone(&services),
                15,
            );
        let reservation = nokv_meta::built_in_holt::acquire_existing_file_store_reservation_v1(
            expected_store_identity,
        )
        .unwrap();
        let mut bundle =
            reserved_existing_holt_file_runtime_bundle(reservation, Arc::clone(&services)).unwrap();
        let drift = install_recovery_fence_drift(&mut bundle);
        let runtime = finish_holt_file_runtime_factory(bundle)
            .unwrap()
            .resolve()
            .unwrap();

        drift.drifted.store(true, Ordering::Release);
        let error = runtime
            .open_store(crate::OpenIntent::ReopenExisting, identity)
            .err()
            .expect("a frozen recovery capability cannot be swapped before dispatch");

        assert!(matches!(
            error,
            crate::runtime_registry::RuntimeOpenError::Runtime(ref runtime_error)
                if runtime_error.code()
                    == crate::runtime_registry::RuntimeFactoryErrorCode::ProviderInstallationDrift
        ));
        assert_eq!(drift.typed_open_calls.load(Ordering::Acquire), 0);
        assert!(matches!(
            services.receipt.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(_)
        ));
    }

    #[test]
    fn frozen_reserved_recovery_downgrades_post_dispatch_capability_drift() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let services = Arc::new(BindingServices::default());
        let (identity, expected_store_identity) =
            create_existing_holt_runtime_store_with_pending_receipt(
                &locator,
                Arc::clone(&services),
                16,
            );
        let reservation = nokv_meta::built_in_holt::acquire_existing_file_store_reservation_v1(
            expected_store_identity,
        )
        .unwrap();
        let mut bundle =
            reserved_existing_holt_file_runtime_bundle(reservation, Arc::clone(&services)).unwrap();
        let drift = install_recovery_fence_drift(&mut bundle);
        let runtime = finish_holt_file_runtime_factory(bundle)
            .unwrap()
            .resolve()
            .unwrap();

        drift.drift_after_next_open.store(true, Ordering::Release);
        let error = runtime
            .open_store(crate::OpenIntent::ReopenExisting, identity)
            .err()
            .expect("post-dispatch capability drift must fail-stop the allocation");

        assert!(matches!(
            error,
            crate::runtime_registry::RuntimeOpenError::Runtime(ref runtime_error)
                if runtime_error.code()
                    == crate::runtime_registry::RuntimeFactoryErrorCode::RuntimeBundlePoisoned
        ));
        assert_eq!(drift.typed_open_calls.load(Ordering::Acquire), 1);
        assert!(matches!(
            services.receipt.load_commit_receipt_v1(identity).unwrap(),
            MetadataCommitReceiptStateV1::Pending(_)
        ));
        assert!(runtime
            .open_store(crate::OpenIntent::ReopenExisting, identity)
            .is_err());
        assert_eq!(drift.typed_open_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn reserved_holt_binding_distinguishes_foreign_objects_at_the_same_service_and_locator() {
        let temporary = tempfile::tempdir().unwrap();
        let locator = temporary.path().join("metadata");
        let displaced = temporary.path().join("metadata-displaced");
        let first_services = Arc::new(BindingServices::default());
        let (_, first_store) =
            create_existing_holt_runtime_store(&locator, Arc::clone(&first_services), 12);
        std::fs::rename(&locator, &displaced).unwrap();

        let second_services = Arc::new(BindingServices::default());
        let (_, second_store) = create_existing_holt_runtime_store(&locator, second_services, 13);
        assert_eq!(
            first_store.canonical_locator(),
            second_store.canonical_locator()
        );
        assert_ne!(first_store, second_store);

        let common_services = ProcessLocalServiceIdentity::of(&first_services);
        let first = HoltFileInstallationIdentity {
            canonical_locator: first_store.canonical_locator().to_owned(),
            bundle_services: common_services,
            expected_store_object_identity: Some(first_store),
        };
        let second = HoltFileInstallationIdentity {
            canonical_locator: second_store.canonical_locator().to_owned(),
            bundle_services: common_services,
            expected_store_object_identity: Some(second_store),
        };
        assert!(first != second);
    }

    #[test]
    fn reserved_holt_server_type_erasure_retries_guard_error_and_unwind() {
        for unwind in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let locator = temporary.path().join("metadata");
            let services = Arc::new(BindingServices::default());
            let (identity, expected_store_identity) =
                create_existing_holt_runtime_store_with_pending_receipt(
                    &locator,
                    Arc::clone(&services),
                    14,
                );
            let reservation = nokv_meta::built_in_holt::acquire_existing_file_store_reservation_v1(
                expected_store_identity.clone(),
            )
            .unwrap();
            let factory =
                holt_reserved_existing_runtime_factory(reservation, Arc::clone(&services)).unwrap();
            let runtime = factory.resolve().unwrap();

            if unwind {
                services.panic_next_guard_validation();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _ = runtime.open_store(crate::OpenIntent::ReopenExisting, identity);
                }));
                assert!(result.is_err());
            } else {
                services.reject_next_guard_validation();
                assert!(runtime
                    .open_store(crate::OpenIntent::ReopenExisting, identity)
                    .is_err());
            }

            let recovery_only = runtime
                .open_store(crate::OpenIntent::ReopenExisting, identity)
                .err()
                .expect("the exact reserved Holt authority must complete a recovery-only open");
            assert!(matches!(
                recovery_only,
                crate::runtime_registry::RuntimeOpenError::Metadata(
                    nokv_meta::workspace::AgentMetadataError::CommitReceiptRecoveryRequired
                )
            ));
            assert_eq!(services.bound_store_identity(), expected_store_identity);
            assert!(services.guard_bindings.load(Ordering::SeqCst) >= 3);
        }
    }

    #[test]
    fn holt_bundle_rejects_known_runtime_or_journal_conflict_without_path_or_secret_leak() {
        let temporary = tempfile::tempdir().unwrap();
        for (name, services) in [
            (
                "secret-runtime-location",
                RejectingBundleServices {
                    reject_runtime: true,
                    reject_journal: false,
                },
            ),
            (
                "secret-journal-location",
                RejectingBundleServices {
                    reject_runtime: false,
                    reject_journal: true,
                },
            ),
        ] {
            let locator = temporary.path().join(name);
            let error = holt_file_runtime_factory(&locator, Arc::new(services))
                .err()
                .unwrap();
            assert!(!locator.exists());
            for rendered in [format!("{error:?}"), error.to_string()] {
                assert!(!rendered.contains(name));
                assert!(!rendered.contains(temporary.path().to_str().unwrap()));
            }
        }
    }

    #[cfg(feature = "foundationdb-provider")]
    fn write_cluster_file(directory: &tempfile::TempDir, name: &str, value: &str) -> PathBuf {
        let path = directory.path().join(name);
        std::fs::write(&path, value).unwrap();
        path
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn foundationdb_profile_authority_and_domain_preserve_the_frozen_golden() {
        let temporary = tempfile::TempDir::new().unwrap();
        let cluster = write_cluster_file(
            &temporary,
            "fdb.cluster",
            "ignored:identity@127.0.0.1:4500\n",
        );
        let descriptor = foundationdb_runtime_descriptor(
            &FoundationDbRuntimeConfig::with_explicit_stable_id(
                cluster,
                "cluster-a",
                "openviking/metadata",
                FoundationDbTransactionPolicy::default(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            descriptor.qualification(),
            crate::RuntimeQualification::NotQualified(
                crate::QualificationCode::CompleteCommandSurfaceUnproven
            )
        );
        assert_eq!(
            descriptor.provider_admission().rejection_codes,
            vec![
                nokv_meta::provider::admission::ProviderAdmissionCode::AtomicOperationLimitTooSmall,
                nokv_meta::provider::admission::ProviderAdmissionCode::LogicalPlanLimitTooSmall,
                nokv_meta::provider::admission::ProviderAdmissionCode::ReadViewLifetimeBounded,
            ]
        );
        for (intent, transition) in ALL_LIFECYCLE_TRANSITIONS {
            assert_eq!(
                descriptor
                    .lifecycle()
                    .classify_bootstrap(intent, transition),
                Err(crate::AdmissionCode::TransitionUnsupported)
            );
        }
        let authority = descriptor.initial_authority(shard(2));
        assert_eq!(
            descriptor.profile_fingerprint(),
            &[
                0x01, 0x55, 0x57, 0x82, 0x06, 0x0c, 0x48, 0x39, 0x00, 0x67, 0xd7, 0x2d, 0x1c, 0x09,
                0xdb, 0x79, 0xba, 0x7a, 0x0b, 0xa8, 0x66, 0xff, 0x98, 0x12, 0x62, 0x60, 0x76, 0xdb,
                0xa5, 0xdf, 0x50, 0xf7,
            ]
        );
        assert_eq!(
            authority.active.authority_id.as_bytes(),
            &[
                0x91, 0x36, 0xb5, 0x3a, 0xc8, 0x03, 0x57, 0x3c, 0xd2, 0xaa, 0xd5, 0x46, 0x18, 0x22,
                0xb8, 0xb7,
            ]
        );
        assert_eq!(
            authority.active.consistency_domain_id.as_bytes(),
            &[
                0xce, 0xab, 0xe5, 0x6f, 0x08, 0xa4, 0x51, 0xf5, 0xf2, 0xd4, 0x30, 0xba, 0xfe, 0x1f,
                0xa8, 0xc0,
            ]
        );
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn foundationdb_fingerprint_ignores_path_and_coordinator_changes() {
        let temporary = tempfile::TempDir::new().unwrap();
        let first = write_cluster_file(
            &temporary,
            "first.cluster",
            "nokv:0123456789abcdef@10.0.0.1:4500\n",
        );
        let second = write_cluster_file(
            &temporary,
            "second.cluster",
            "nokv:0123456789abcdef@10.0.0.2:4500,10.0.0.3:4500\n",
        );
        let first = foundationdb_runtime_descriptor(
            &FoundationDbRuntimeConfig::from_cluster_file(
                first,
                "openviking/metadata",
                FoundationDbTransactionPolicy::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let second = foundationdb_runtime_descriptor(
            &FoundationDbRuntimeConfig::from_cluster_file(
                second,
                "openviking/metadata",
                FoundationDbTransactionPolicy::default(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(first.profile_fingerprint(), second.profile_fingerprint());
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn foundationdb_domain_is_shared_but_authority_is_shard_specific() {
        let temporary = tempfile::TempDir::new().unwrap();
        let cluster = write_cluster_file(
            &temporary,
            "fdb.cluster",
            "nokv:0123456789abcdef@127.0.0.1:4500\n",
        );
        let descriptor = foundationdb_runtime_descriptor(
            &FoundationDbRuntimeConfig::from_cluster_file(
                cluster,
                "openviking/metadata",
                FoundationDbTransactionPolicy::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let first = descriptor.initial_authority(shard(2));
        let second = descriptor.initial_authority(shard(3));
        assert_eq!(
            first.active.consistency_domain_id,
            second.active.consistency_domain_id
        );
        assert_ne!(first.active.authority_id, second.active.authority_id);
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn foundationdb_debug_redacts_cluster_path_and_identity() {
        let temporary = tempfile::TempDir::new().unwrap();
        let cluster = write_cluster_file(
            &temporary,
            "private.cluster",
            "secret-description:secret-id@192.0.2.4:4500\n",
        );
        let config = FoundationDbRuntimeConfig::from_cluster_file(
            &cluster,
            "openviking/metadata",
            FoundationDbTransactionPolicy::default(),
        )
        .unwrap();
        let rendered = format!("{config:?}");
        assert!(!rendered.contains(temporary.path().to_str().unwrap()));
        assert!(!rendered.contains("secret-description"));
        assert!(!rendered.contains("secret-id"));
        assert!(!rendered.contains("192.0.2.4"));
    }
}
