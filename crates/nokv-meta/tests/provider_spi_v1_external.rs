use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use nokv_meta::provider::admission::workspace_provider_requirements_v1;
use nokv_meta::provider::v1::{
    AtomicCommitOutcome, AtomicOp, AtomicPlan, CreateRecoveryIntentV1, MetadataProvider,
    MetadataProviderFactoryV1, MetadataReadView, MetadataTransaction, OrderedSpaceId,
    ProviderCapabilities, ProviderContractOfferV1, ProviderCreateRequestV1, ProviderError,
    ProviderInstanceToken, ProviderOperationV1, ProviderRecord, ProviderReopenRequestV1,
    ProviderScan, ProviderScanItem, ProviderScanPage, ProviderScanStats, ProviderSchemaV1,
    ProviderTransactionModel, ProviderVersionModel, ReadScope, ReadWitness,
};
use nokv_meta::workspace::{
    run_metadata_fsck, workspace_metadata_contract_digest, AcknowledgedMetadataFrontier,
    AgentMetadataError, AgentMetadataStore, CommandMutation, CommandPredicate, MetadataCommand,
    MetadataCommitReceiptErrorV1, MetadataCommitReceiptMutationBackendResultV1,
    MetadataCommitReceiptMutationNotDispatchedV1, MetadataCommitReceiptPersistBackendResultV1,
    MetadataCommitReceiptPersistCommandV1, MetadataCommitReceiptPersistErrorV1,
    MetadataCommitReceiptPersistNotDispatchedV1, MetadataCommitReceiptPersistOutcomeV1,
    MetadataCommitReceiptPoisonCommandV1, MetadataCommitReceiptPoisonOutcomeV1,
    MetadataCommitReceiptPoisonReasonV1, MetadataCommitReceiptQualificationV1,
    MetadataCommitReceiptResolveCommandV1, MetadataCommitReceiptResolveOutcomeV1,
    MetadataCommitReceiptStateV1, MetadataCommitReceiptStoreV1,
    MetadataCommitRecoveryFenceFactoryV1, MetadataCommitResolutionV1, MetadataFamily,
    MetadataFrontierPointV1, MetadataFsckLimits, MetadataFsckRequest,
    MetadataOldDispatchExclusionInstallationV1, MetadataPendingRecoveryOpenCommandV1,
    MetadataPendingRecoveryOpenNotDispatchedV1, MetadataPendingRecoveryOpenOutcomeV1,
    MetadataRuntimeCommitBundleV1, MetadataStoreCreateModeV1, MetadataStoreIdentity,
    PlannedMetadataCommitV1, RootFenceAction, SCHEMA_ID,
};
use nokv_types::{
    CommandDigest, ConsistencyDomainId, LogicalShardId, MetadataAuthorityGeneration,
    MetadataAuthorityId, OwnerEpoch, PlacementGeneration, ReadVersion, RequestId,
    RootActivationState, RootId, RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
};

#[derive(Clone)]
struct StoredValue {
    value: Vec<u8>,
    version: u64,
}

#[derive(Clone, Default)]
struct DurableState {
    identity: Option<MetadataStoreIdentity>,
    rows: BTreeMap<OrderedSpaceId, BTreeMap<Vec<u8>, StoredValue>>,
    next_version: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CommitBehavior {
    #[default]
    Normal,
    UnknownApplied,
    UnknownNotApplied,
    UnknownStaleThenApplied,
    UnknownUnsettledStaleThenApplied,
    ConflictAppliedSamePurpose,
    ConflictWithForeignHigherTail,
    CommittedWithPurposeEvidenceMismatch,
}

struct DelayedCommit {
    rows: BTreeMap<OrderedSpaceId, BTreeMap<Vec<u8>, StoredValue>>,
    next_version: u64,
    remaining_resolution_reads: usize,
}

#[derive(Default)]
struct ProviderControl {
    contract_offer_calls: AtomicUsize,
    create_calls: AtomicUsize,
    reopen_calls: AtomicUsize,
    get_calls: AtomicUsize,
    begin_read_calls: AtomicUsize,
    resolution_read_calls: AtomicUsize,
    begin_write_calls: AtomicUsize,
    commit_calls: AtomicUsize,
    fail_begin_write_once: AtomicBool,
    next_commit: Mutex<CommitBehavior>,
    delayed_commit: Mutex<Option<DelayedCommit>>,
}

#[derive(Default)]
struct ReceiptResolveBarrier {
    state: Mutex<ReceiptResolveBarrierState>,
    changed: Condvar,
}

#[derive(Default)]
struct ReceiptResolveBarrierState {
    armed: bool,
    entered: bool,
    released: bool,
}

impl ReceiptResolveBarrier {
    fn arm(&self) {
        *self.state.lock().unwrap() = ReceiptResolveBarrierState {
            armed: true,
            entered: false,
            released: false,
        };
    }

    fn block_if_armed(&self) {
        let mut state = self.state.lock().unwrap();
        if !state.armed {
            return;
        }
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
        state.armed = false;
    }

    fn wait_until_entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.entered)
            .unwrap();
        assert!(state.entered, "receipt resolve barrier was not reached");
        assert!(!timeout.timed_out(), "receipt resolve barrier timed out");
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

impl ProviderControl {
    fn arm(&self, behavior: CommitBehavior) {
        *self.next_commit.lock().unwrap() = behavior;
    }

    fn take_behavior(&self) -> CommitBehavior {
        std::mem::take(&mut *self.next_commit.lock().unwrap())
    }
}

const FROZEN_RUNTIME_BUNDLE_DIGEST: [u8; 32] = [0x71; 32];

#[derive(Default)]
struct DurableReceipt {
    state: Mutex<Option<MetadataCommitReceiptStateV1>>,
    unavailable: AtomicBool,
    reject_persist: AtomicBool,
    persist_then_error: AtomicBool,
    panic_after_persist: AtomicBool,
    resolve_then_error: AtomicBool,
    poison_then_error: AtomicBool,
    panic_on_resolve: AtomicBool,
    resolve_barrier: ReceiptResolveBarrier,
}

impl DurableReceipt {
    fn load(
        &self,
        store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
        if self.unavailable.load(Ordering::Acquire) {
            return Err(MetadataCommitReceiptErrorV1::Unavailable);
        }
        let mut state = self.state.lock().unwrap();
        let durable = state.get_or_insert(MetadataCommitReceiptStateV1::Clean {
            store_identity,
            frozen_bundle_digest: FROZEN_RUNTIME_BUNDLE_DIGEST,
            frontier: MetadataFrontierPointV1::Absent,
        });
        if receipt_store_identity(durable) != Some(store_identity) {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        }
        Ok(durable.clone())
    }

    fn persist(
        &self,
        planned: &PlannedMetadataCommitV1,
    ) -> Result<(), MetadataCommitReceiptPersistErrorV1> {
        if self.unavailable.load(Ordering::Acquire)
            || planned.frozen_bundle_digest() != FROZEN_RUNTIME_BUNDLE_DIGEST
        {
            return Err(MetadataCommitReceiptPersistErrorV1::UnavailableBeforeEffect);
        }
        if self.reject_persist.load(Ordering::Acquire) {
            return Err(MetadataCommitReceiptPersistErrorV1::UnavailableBeforeEffect);
        }
        let mut state = self.state.lock().unwrap();
        let persisted = match state.as_ref() {
            Some(MetadataCommitReceiptStateV1::Clean {
                store_identity,
                frozen_bundle_digest,
                frontier,
            }) if *store_identity == planned.store_identity()
                && *frozen_bundle_digest == FROZEN_RUNTIME_BUNDLE_DIGEST
                && *frontier == planned.prior() =>
            {
                *state = Some(MetadataCommitReceiptStateV1::Pending(planned.clone()));
                true
            }
            _ => false,
        };
        if !persisted {
            return Err(MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect);
        }
        drop(state);
        if self.panic_after_persist.swap(false, Ordering::AcqRel) {
            panic!("injected receipt persist panic after durable Pending");
        }
        if self.persist_then_error.swap(false, Ordering::AcqRel) {
            Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    fn resolve(
        &self,
        planned: &PlannedMetadataCommitV1,
        resolution: &MetadataCommitResolutionV1,
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        if self.panic_on_resolve.swap(false, Ordering::AcqRel) {
            panic!("injected receipt resolve panic");
        }
        self.resolve_barrier.block_if_armed();
        let mut state = self.state.lock().unwrap();
        let unsettled = match state.as_ref() {
            Some(MetadataCommitReceiptStateV1::Pending(durable))
            | Some(MetadataCommitReceiptStateV1::PoisonedSettled(durable))
                if durable == planned =>
            {
                false
            }
            Some(MetadataCommitReceiptStateV1::PoisonedUnsettled(durable))
                if durable == planned =>
            {
                true
            }
            _ => return Err(MetadataCommitReceiptErrorV1::InvalidBinding),
        };
        let frontier = if resolution.applied_exact_next() == Some(planned.exact_next()) {
            MetadataFrontierPointV1::Exact(planned.exact_next())
        } else if !unsettled && resolution.not_applied_exact_prior() == Some(planned.prior()) {
            planned.prior()
        } else {
            return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
        };
        *state = Some(MetadataCommitReceiptStateV1::Clean {
            store_identity: planned.store_identity(),
            frozen_bundle_digest: FROZEN_RUNTIME_BUNDLE_DIGEST,
            frontier,
        });
        if self.resolve_then_error.swap(false, Ordering::AcqRel) {
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        } else {
            Ok(())
        }
    }

    fn poison(
        &self,
        planned: &PlannedMetadataCommitV1,
        reason: MetadataCommitReceiptPoisonReasonV1,
    ) -> Result<(), MetadataCommitReceiptErrorV1> {
        let mut state = self.state.lock().unwrap();
        let resolved = match state.as_ref() {
            Some(MetadataCommitReceiptStateV1::Pending(durable)) if durable == planned => {
                *state = Some(match reason {
                    MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome => {
                        MetadataCommitReceiptStateV1::PoisonedSettled(planned.clone())
                    }
                    MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome => {
                        MetadataCommitReceiptStateV1::PoisonedUnsettled(planned.clone())
                    }
                });
                Ok(())
            }
            Some(MetadataCommitReceiptStateV1::PoisonedSettled(durable))
                if durable == planned
                    && reason == MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome =>
            {
                Ok(())
            }
            Some(MetadataCommitReceiptStateV1::PoisonedUnsettled(durable))
                if durable == planned
                    && reason == MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome =>
            {
                Ok(())
            }
            _ => Err(MetadataCommitReceiptErrorV1::InvalidBinding),
        };
        resolved?;
        if self.poison_then_error.swap(false, Ordering::AcqRel) {
            Err(MetadataCommitReceiptErrorV1::Unavailable)
        } else {
            Ok(())
        }
    }

    fn force_clean(&self, identity: MetadataStoreIdentity, frontier: MetadataFrontierPointV1) {
        *self.state.lock().unwrap() = Some(MetadataCommitReceiptStateV1::Clean {
            store_identity: identity,
            frozen_bundle_digest: FROZEN_RUNTIME_BUNDLE_DIGEST,
            frontier,
        });
    }

    fn state(&self) -> MetadataCommitReceiptStateV1 {
        self.state
            .lock()
            .unwrap()
            .clone()
            .expect("test receipt is bound")
    }
}

fn receipt_store_identity(state: &MetadataCommitReceiptStateV1) -> Option<MetadataStoreIdentity> {
    match state {
        MetadataCommitReceiptStateV1::Clean { store_identity, .. } => Some(*store_identity),
        MetadataCommitReceiptStateV1::Pending(planned)
        | MetadataCommitReceiptStateV1::PoisonedSettled(planned)
        | MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => {
            Some(planned.store_identity())
        }
        MetadataCommitReceiptStateV1::UntrackedStandalone => None,
    }
}

fn persist_backend_result(
    result: Result<(), MetadataCommitReceiptPersistErrorV1>,
) -> MetadataCommitReceiptPersistBackendResultV1 {
    match result {
        Ok(()) => MetadataCommitReceiptPersistBackendResultV1::Persisted,
        Err(MetadataCommitReceiptPersistErrorV1::UnavailableBeforeEffect) => {
            MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                MetadataCommitReceiptPersistNotDispatchedV1::Unavailable,
            )
        }
        Err(MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect) => {
            MetadataCommitReceiptPersistBackendResultV1::NotDispatched(
                MetadataCommitReceiptPersistNotDispatchedV1::InvalidBinding,
            )
        }
        Err(MetadataCommitReceiptPersistErrorV1::RecoveryRequired) => {
            MetadataCommitReceiptPersistBackendResultV1::RecoveryRequired
        }
    }
}

fn mutation_backend_result(
    result: Result<(), MetadataCommitReceiptErrorV1>,
) -> MetadataCommitReceiptMutationBackendResultV1 {
    match result {
        Ok(()) => MetadataCommitReceiptMutationBackendResultV1::Completed,
        Err(MetadataCommitReceiptErrorV1::Poisoned) => {
            MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                MetadataCommitReceiptMutationNotDispatchedV1::Poisoned,
            )
        }
        Err(MetadataCommitReceiptErrorV1::InvalidBinding) => {
            MetadataCommitReceiptMutationBackendResultV1::NotDispatched(
                MetadataCommitReceiptMutationNotDispatchedV1::InvalidBinding,
            )
        }
        Err(MetadataCommitReceiptErrorV1::Unavailable) => {
            MetadataCommitReceiptMutationBackendResultV1::OutcomeUnknown
        }
    }
}

trait HasDurableReceipt {
    fn durable_receipt(&self) -> &DurableReceipt;
}

macro_rules! impl_durable_receipt {
    ($bundle:ty) => {
        impl MetadataCommitReceiptStoreV1 for $bundle {
            fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
                MetadataCommitReceiptQualificationV1::Durable
            }

            fn frozen_runtime_bundle_digest_v1(&self) -> [u8; 32] {
                FROZEN_RUNTIME_BUNDLE_DIGEST
            }

            fn load_commit_receipt_v1(
                &self,
                store_identity: MetadataStoreIdentity,
            ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
                self.durable_receipt().load(store_identity)
            }

            fn persist_pending_commit_v1(
                &self,
                command: MetadataCommitReceiptPersistCommandV1,
            ) -> MetadataCommitReceiptPersistOutcomeV1 {
                let command = command.claim_execution();
                let result = self.durable_receipt().persist(command.planned());
                command.complete(persist_backend_result(result))
            }

            fn resolve_pending_commit_v1(
                &self,
                command: MetadataCommitReceiptResolveCommandV1,
            ) -> MetadataCommitReceiptResolveOutcomeV1 {
                let command = command.claim_execution();
                let result = self
                    .durable_receipt()
                    .resolve(command.planned(), command.resolution());
                command.complete(mutation_backend_result(result))
            }

            fn poison_commit_receipt_v1(
                &self,
                command: MetadataCommitReceiptPoisonCommandV1,
            ) -> MetadataCommitReceiptPoisonOutcomeV1 {
                let command = command.claim_execution();
                let result = self
                    .durable_receipt()
                    .poison(command.planned(), command.reason());
                command.complete(mutation_backend_result(result))
            }
        }
    };
}

#[derive(Default)]
struct MemoryFactory {
    durable: Arc<Mutex<DurableState>>,
    offered_schema: Mutex<Option<ProviderSchemaV1>>,
    runtime_rejected: Arc<AtomicBool>,
    receipt: DurableReceipt,
    control: Arc<ProviderControl>,
}

impl HasDurableReceipt for MemoryFactory {
    fn durable_receipt(&self) -> &DurableReceipt {
        &self.receipt
    }
}

impl_durable_receipt!(MemoryFactory);

impl MemoryFactory {
    fn provider(&self, identity: MetadataStoreIdentity) -> Arc<dyn MetadataProvider> {
        Arc::new(MemoryProvider {
            durable: Arc::clone(&self.durable),
            identity,
            instance: ProviderInstanceToken::new(),
            runtime_rejected: Arc::clone(&self.runtime_rejected),
            control: Arc::clone(&self.control),
        })
    }

    fn last_schema(&self) -> ProviderSchemaV1 {
        self.offered_schema.lock().unwrap().clone().unwrap()
    }
}

impl MetadataProviderFactoryV1 for MemoryFactory {
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        self.control
            .contract_offer_calls
            .fetch_add(1, Ordering::AcqRel);
        if schema.spi_major() != ProviderSchemaV1::SPI_MAJOR
            || schema.ordered_spaces().is_empty()
            || schema
                .workspace_contract_digest()
                .as_bytes()
                .iter()
                .all(|byte| *byte == 0)
        {
            return Err(ProviderError::schema());
        }
        *self.offered_schema.lock().unwrap() = Some(schema.clone());
        Ok(ProviderContractOfferV1 {
            capabilities: capabilities(),
        })
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        self.control.create_calls.fetch_add(1, Ordering::AcqRel);
        request.claim_execution()?;
        if self.last_schema() != *request.schema() {
            return Err(ProviderError::schema());
        }
        let store_identity = request.store_identity();
        let mut durable = self.durable.lock().unwrap();
        match (durable.identity, request.recovery_intent()) {
            (None, _) => durable.identity = Some(store_identity),
            (Some(identity), CreateRecoveryIntentV1::ReconcilePrepared)
                if identity == store_identity => {}
            (Some(_), _) => return Err(ProviderError::schema()),
        }
        drop(durable);
        Ok(self.provider(store_identity))
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        self.control.reopen_calls.fetch_add(1, Ordering::AcqRel);
        request.claim_execution()?;
        let expected_store_identity = request.expected_store_identity();
        if self.last_schema() != *request.schema()
            || self.durable.lock().unwrap().identity != Some(expected_store_identity)
        {
            return Err(ProviderError::schema());
        }
        Ok(self.provider(expected_store_identity))
    }
}

#[derive(Default)]
struct UnclaimedFactory {
    provider: MemoryFactory,
}

impl HasDurableReceipt for UnclaimedFactory {
    fn durable_receipt(&self) -> &DurableReceipt {
        &self.provider.receipt
    }
}

impl_durable_receipt!(UnclaimedFactory);

impl MetadataProviderFactoryV1 for UnclaimedFactory {
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        self.provider.contract_offer(schema)
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        Ok(self.provider.provider(request.store_identity()))
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        Ok(self.provider.provider(request.expected_store_identity()))
    }
}

#[test]
fn facade_rejects_a_provider_returned_without_claiming_the_open_execution() {
    let factory = Arc::new(UnclaimedFactory::default());
    let store_identity = identity(0x19);

    assert!(matches!(
        AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            factory.clone(),
            store_identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        ),
        Err(AgentMetadataError::SchemaGate { .. })
    ));
    factory.provider.receipt.force_clean(
        store_identity,
        MetadataFrontierPointV1::Exact(AcknowledgedMetadataFrontier {
            write_sequence: 0,
            commit_version: nokv_types::CommitVersion::new(1).unwrap(),
            recovery_lsn: 0,
            chain_digest: [0x31; 32],
        }),
    );
    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(factory, store_identity,),
        Err(AgentMetadataError::SchemaGate { .. })
    ));
}

#[derive(Clone)]
struct MemoryProvider {
    durable: Arc<Mutex<DurableState>>,
    identity: MetadataStoreIdentity,
    instance: ProviderInstanceToken,
    runtime_rejected: Arc<AtomicBool>,
    control: Arc<ProviderControl>,
}

impl MemoryProvider {
    fn record(&self, stored: &StoredValue) -> ProviderRecord {
        ProviderRecord {
            value: stored.value.clone(),
            witness: self
                .instance
                .issue_witness(stored.version.to_be_bytes().to_vec()),
        }
    }

    fn witness_version(&self, witness: &ReadWitness) -> Result<u64, ProviderError> {
        let bytes: [u8; 8] = self
            .instance
            .parse_witness(witness)
            .map_err(|_| ProviderError::authority_mismatch(ProviderOperationV1::ValidateWitness))?
            .try_into()
            .map_err(|_| ProviderError::invalid_plan())?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn validate_runtime_inner(&self) -> Result<(), ProviderError> {
        if self.runtime_rejected.load(Ordering::Acquire) {
            Err(ProviderError::authority_mismatch(
                ProviderOperationV1::ValidateRuntime,
            ))
        } else {
            Ok(())
        }
    }
}

impl MetadataProvider for MemoryProvider {
    fn logical_shard_id(&self) -> LogicalShardId {
        self.identity.logical_shard_id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        capabilities()
    }

    fn validate_runtime(&self) -> Result<(), ProviderError> {
        self.validate_runtime_inner()
    }

    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.validate_runtime_inner()?;
        self.control.get_calls.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .durable
            .lock()
            .unwrap()
            .rows
            .get(&space)
            .and_then(|rows| rows.get(key))
            .map(|stored| self.record(stored)))
    }

    fn begin_read(&self, scopes: &[ReadScope]) -> Result<Box<dyn MetadataReadView>, ProviderError> {
        self.validate_runtime_inner()?;
        self.control.begin_read_calls.fetch_add(1, Ordering::AcqRel);
        let is_resolution_read =
            scopes.len() > 2 && scopes.iter().all(|scope| scope.prefix.is_empty());
        if is_resolution_read {
            self.control
                .resolution_read_calls
                .fetch_add(1, Ordering::AcqRel);
        }
        let rows = self.durable.lock().unwrap().rows.clone();
        let mut delayed_guard = self.control.delayed_commit.lock().unwrap();
        if let Some(mut delayed) = delayed_guard.take() {
            if !is_resolution_read {
                *delayed_guard = Some(delayed);
            } else if delayed.remaining_resolution_reads == 0 {
                let mut durable = self.durable.lock().unwrap();
                durable.rows = delayed.rows;
                durable.next_version = delayed.next_version;
            } else {
                delayed.remaining_resolution_reads -= 1;
                *delayed_guard = Some(delayed);
            }
        }
        Ok(Box::new(MemoryReadView {
            rows,
            scopes: coalesce_scopes(scopes),
            instance: self.instance.clone(),
            runtime_rejected: Arc::clone(&self.runtime_rejected),
        }))
    }

    fn begin_write(&self) -> Result<Box<dyn MetadataTransaction>, ProviderError> {
        self.validate_runtime_inner()?;
        self.control
            .begin_write_calls
            .fetch_add(1, Ordering::AcqRel);
        if self
            .control
            .fail_begin_write_once
            .swap(false, Ordering::AcqRel)
        {
            return Err(ProviderError::backend(
                ProviderOperationV1::BeginWrite,
                "injected definite begin-write failure",
            ));
        }
        Ok(Box::new(MemoryTransaction {
            provider: self.clone(),
            snapshot: self.durable.lock().unwrap().rows.clone(),
        }))
    }
}

struct MemoryReadView {
    rows: BTreeMap<OrderedSpaceId, BTreeMap<Vec<u8>, StoredValue>>,
    scopes: BTreeMap<OrderedSpaceId, Vec<u8>>,
    instance: ProviderInstanceToken,
    runtime_rejected: Arc<AtomicBool>,
}

impl MemoryReadView {
    fn validate_scope(&self, space: OrderedSpaceId, key: &[u8]) -> Result<(), ProviderError> {
        if self.runtime_rejected.load(Ordering::Acquire) {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::ValidateRuntime,
            ));
        }
        if self
            .scopes
            .get(&space)
            .is_some_and(|prefix| key.starts_with(prefix))
        {
            Ok(())
        } else {
            Err(ProviderError::invalid_plan())
        }
    }
}

impl MetadataReadView for MemoryReadView {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.validate_scope(space, key)?;
        Ok(self
            .rows
            .get(&space)
            .and_then(|rows| rows.get(key))
            .map(|stored| ProviderRecord {
                value: stored.value.clone(),
                witness: self
                    .instance
                    .issue_witness(stored.version.to_be_bytes().to_vec()),
            }))
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        self.validate_scope(request.space, &request.prefix)?;
        Ok(scan_rows(self.rows.get(&request.space), request))
    }
}

struct MemoryTransaction {
    provider: MemoryProvider,
    snapshot: BTreeMap<OrderedSpaceId, BTreeMap<Vec<u8>, StoredValue>>,
}

impl MetadataReadView for MemoryTransaction {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.provider.validate_runtime_inner()?;
        Ok(self
            .snapshot
            .get(&space)
            .and_then(|rows| rows.get(key))
            .map(|stored| self.provider.record(stored)))
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        self.provider.validate_runtime_inner()?;
        Ok(scan_rows(self.snapshot.get(&request.space), request))
    }
}

impl MetadataTransaction for MemoryTransaction {
    fn prefix_is_empty(&self, space: OrderedSpaceId, prefix: &[u8]) -> Result<bool, ProviderError> {
        self.provider.validate_runtime_inner()?;
        Ok(self
            .snapshot
            .get(&space)
            .is_none_or(|rows| rows.keys().all(|key| !key.starts_with(prefix))))
    }

    fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
        self.provider.validate_runtime_inner()?;
        self.provider
            .control
            .commit_calls
            .fetch_add(1, Ordering::AcqRel);
        let behavior = self.provider.control.take_behavior();
        let mut durable = self.provider.durable.lock().unwrap();
        let mut candidate = durable.rows.clone();
        let mut next_version = durable.next_version;
        for operation in plan.operations {
            let conflict = match operation {
                AtomicOp::AssertUnchanged {
                    space,
                    key,
                    witness,
                } => {
                    let expected = self.provider.witness_version(&witness)?;
                    candidate
                        .get(&space)
                        .and_then(|rows| rows.get(&key))
                        .is_none_or(|stored| expected != stored.version)
                }
                AtomicOp::AssertAbsent { space, key } => candidate
                    .get(&space)
                    .is_some_and(|rows| rows.contains_key(&key)),
                AtomicOp::AssertPrefixEmpty { space, prefix } => candidate
                    .get(&space)
                    .is_some_and(|rows| rows.keys().any(|key| key.starts_with(&prefix))),
                AtomicOp::Put { space, key, value } => {
                    next_version = next_version.saturating_add(1);
                    candidate.entry(space).or_default().insert(
                        key,
                        StoredValue {
                            value,
                            version: next_version,
                        },
                    );
                    false
                }
                AtomicOp::PutIfAbsent { space, key, value } => {
                    let rows = candidate.entry(space).or_default();
                    if let std::collections::btree_map::Entry::Vacant(entry) = rows.entry(key) {
                        next_version = next_version.saturating_add(1);
                        entry.insert(StoredValue {
                            value,
                            version: next_version,
                        });
                        false
                    } else {
                        true
                    }
                }
                AtomicOp::CompareAndPut {
                    space,
                    key,
                    witness,
                    value,
                } => {
                    let expected = self.provider.witness_version(&witness)?;
                    let rows = candidate.entry(space).or_default();
                    if rows
                        .get(&key)
                        .is_none_or(|stored| stored.version != expected)
                    {
                        true
                    } else {
                        next_version = next_version.saturating_add(1);
                        rows.insert(
                            key,
                            StoredValue {
                                value,
                                version: next_version,
                            },
                        );
                        false
                    }
                }
                AtomicOp::Delete { space, key } => {
                    candidate.entry(space).or_default().remove(&key);
                    false
                }
            };
            if conflict {
                return Ok(AtomicCommitOutcome::Conflict);
            }
        }
        match behavior {
            CommitBehavior::Normal => {
                durable.rows = candidate;
                durable.next_version = next_version;
                Ok(AtomicCommitOutcome::Committed)
            }
            CommitBehavior::UnknownApplied => {
                durable.rows = candidate;
                durable.next_version = next_version;
                Err(ProviderError::unknown_commit_settled())
            }
            CommitBehavior::UnknownNotApplied => Err(ProviderError::unknown_commit_settled()),
            CommitBehavior::UnknownStaleThenApplied => {
                *self.provider.control.delayed_commit.lock().unwrap() = Some(DelayedCommit {
                    rows: candidate,
                    next_version,
                    remaining_resolution_reads: 0,
                });
                Err(ProviderError::unknown_commit_settled())
            }
            CommitBehavior::UnknownUnsettledStaleThenApplied => {
                *self.provider.control.delayed_commit.lock().unwrap() = Some(DelayedCommit {
                    rows: candidate,
                    next_version,
                    remaining_resolution_reads: 2,
                });
                Err(ProviderError::unknown_commit_unsettled())
            }
            CommitBehavior::ConflictAppliedSamePurpose => {
                durable.rows = candidate;
                durable.next_version = next_version;
                Ok(AtomicCommitOutcome::Conflict)
            }
            CommitBehavior::ConflictWithForeignHigherTail => {
                bump_commit_clock(&mut candidate, &mut next_version)?;
                durable.rows = candidate;
                durable.next_version = next_version;
                Ok(AtomicCommitOutcome::Conflict)
            }
            CommitBehavior::CommittedWithPurposeEvidenceMismatch => {
                candidate.remove(&OrderedSpaceId::new(0x0103));
                durable.rows = candidate;
                durable.next_version = next_version;
                Ok(AtomicCommitOutcome::Committed)
            }
        }
    }
}

fn bump_commit_clock(
    rows: &mut BTreeMap<OrderedSpaceId, BTreeMap<Vec<u8>, StoredValue>>,
    next_version: &mut u64,
) -> Result<(), ProviderError> {
    let clock = rows
        .get_mut(&OrderedSpaceId::new(0x0101))
        .and_then(|system| system.get_mut(b"commit_clock".as_slice()))
        .ok_or_else(ProviderError::invalid_plan)?;
    if clock.value.len() != 9 || clock.value[0] != 1 {
        return Err(ProviderError::invalid_plan());
    }
    let current = u64::from_be_bytes(
        clock.value[1..]
            .try_into()
            .map_err(|_| ProviderError::invalid_plan())?,
    );
    *next_version = next_version.saturating_add(1);
    clock.value[1..].copy_from_slice(&current.saturating_add(1).to_be_bytes());
    clock.version = *next_version;
    Ok(())
}

fn capabilities() -> ProviderCapabilities {
    ProviderCapabilities {
        transaction_model: ProviderTransactionModel::CrossSpaceAtomicBatch,
        version_model: ProviderVersionModel::OpaqueRecordWitness,
        consistent_cross_space_reads: true,
        all_ambiguous_commit_outcomes_settled_before_return: true,
        commit_resolution_reads_causally_current: true,
        max_key_bytes: 64 * 1024,
        max_value_bytes: 64 * 1024,
        max_transaction_bytes: 256 * 1024 * 1024,
        max_atomic_operations: 4_096,
        max_logical_plan_bytes: 256 * 1024 * 1024,
        exclusive_scan_start_after: true,
        consistent_snapshot_scans: true,
        max_read_view_duration: None,
        max_scan_items: None,
    }
}

struct NeverOpenedFactory {
    capabilities: ProviderCapabilities,
    create_calls: AtomicUsize,
    reopen_calls: AtomicUsize,
    receipt: DurableReceipt,
}

impl NeverOpenedFactory {
    fn new(capabilities: ProviderCapabilities) -> Self {
        Self {
            capabilities,
            create_calls: AtomicUsize::new(0),
            reopen_calls: AtomicUsize::new(0),
            receipt: DurableReceipt::default(),
        }
    }
}

impl HasDurableReceipt for NeverOpenedFactory {
    fn durable_receipt(&self) -> &DurableReceipt {
        &self.receipt
    }
}

impl_durable_receipt!(NeverOpenedFactory);

impl MetadataProviderFactoryV1 for NeverOpenedFactory {
    fn contract_offer(
        &self,
        _schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        Ok(ProviderContractOfferV1 {
            capabilities: self.capabilities,
        })
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        request.claim_execution()?;
        self.create_calls.fetch_add(1, Ordering::AcqRel);
        Err(ProviderError::unavailable(ProviderOperationV1::Create))
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        request.claim_execution()?;
        self.reopen_calls.fetch_add(1, Ordering::AcqRel);
        Err(ProviderError::unavailable(ProviderOperationV1::Reopen))
    }
}

macro_rules! impl_unsupported_commit_recovery_fence {
    ($($factory:ty),+ $(,)?) => {
        $(
            impl MetadataCommitRecoveryFenceFactoryV1 for $factory {
                fn old_dispatch_exclusion_installation_v1(
                    &self,
                ) -> MetadataOldDispatchExclusionInstallationV1 {
                    MetadataOldDispatchExclusionInstallationV1::unsupported()
                }

                fn reopen_pending_with_old_dispatch_excluded_v1(
                    &self,
                    command: MetadataPendingRecoveryOpenCommandV1,
                ) -> MetadataPendingRecoveryOpenOutcomeV1 {
                    command.reject_before_execution(
                        MetadataPendingRecoveryOpenNotDispatchedV1::Unsupported,
                    )
                }
            }
        )+
    };
}

impl_unsupported_commit_recovery_fence!(MemoryFactory, UnclaimedFactory, NeverOpenedFactory);

#[test]
fn facade_rejects_every_capability_gap_before_provider_side_effects() {
    let requirements = workspace_provider_requirements_v1();
    let mut rejected = Vec::new();

    let mut cross_space = capabilities();
    cross_space.consistent_cross_space_reads = false;
    rejected.push(cross_space);

    let mut unsettled_unknown = capabilities();
    unsettled_unknown.all_ambiguous_commit_outcomes_settled_before_return = false;
    rejected.push(unsettled_unknown);

    let mut stale_unknown_resolution = capabilities();
    stale_unknown_resolution.commit_resolution_reads_causally_current = false;
    rejected.push(stale_unknown_resolution);

    let mut key_limit = capabilities();
    key_limit.max_key_bytes = requirements.max_key_bytes - 1;
    rejected.push(key_limit);

    let mut value_limit = capabilities();
    value_limit.max_value_bytes = requirements.max_value_bytes - 1;
    rejected.push(value_limit);

    let mut operation_limit = capabilities();
    operation_limit.max_atomic_operations = requirements.max_atomic_operations - 1;
    rejected.push(operation_limit);

    let mut plan_limit = capabilities();
    plan_limit.max_logical_plan_bytes = requirements.max_logical_plan_bytes - 1;
    rejected.push(plan_limit);

    let mut exclusive_cursor = capabilities();
    exclusive_cursor.exclusive_scan_start_after = false;
    rejected.push(exclusive_cursor);

    let mut snapshot_scan = capabilities();
    snapshot_scan.consistent_snapshot_scans = false;
    rejected.push(snapshot_scan);

    let mut bounded_view = capabilities();
    bounded_view.max_read_view_duration = Some(Duration::from_secs(5));
    rejected.push(bounded_view);

    let mut bounded_scan = capabilities();
    bounded_scan.max_scan_items = Some(1_024);
    rejected.push(bounded_scan);

    for (index, offered) in rejected.into_iter().enumerate() {
        let factory = Arc::new(NeverOpenedFactory::new(offered));
        let store_identity = identity(u8::try_from(index + 1).unwrap());
        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                factory.clone(),
                store_identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(AgentMetadataError::SchemaGate { .. })
        ));
        factory.receipt.force_clean(
            store_identity,
            MetadataFrontierPointV1::Exact(AcknowledgedMetadataFrontier {
                write_sequence: 0,
                commit_version: nokv_types::CommitVersion::new(1).unwrap(),
                recovery_lsn: 0,
                chain_digest: [0x32; 32],
            }),
        );
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                factory.clone(),
                store_identity,
            ),
            Err(AgentMetadataError::SchemaGate { .. })
        ));
        assert_eq!(factory.create_calls.load(Ordering::Acquire), 0);
        assert_eq!(factory.reopen_calls.load(Ordering::Acquire), 0);
    }
}

fn coalesce_scopes(scopes: &[ReadScope]) -> BTreeMap<OrderedSpaceId, Vec<u8>> {
    let mut result = BTreeMap::new();
    for scope in scopes {
        result
            .entry(scope.space)
            .and_modify(|prefix: &mut Vec<u8>| {
                let common = prefix
                    .iter()
                    .zip(&scope.prefix)
                    .take_while(|(left, right)| left == right)
                    .count();
                prefix.truncate(common);
            })
            .or_insert_with(|| scope.prefix.clone());
    }
    result
}

fn scan_rows(
    rows: Option<&BTreeMap<Vec<u8>, StoredValue>>,
    request: &ProviderScan,
) -> ProviderScanPage {
    let mut items = Vec::new();
    let mut common_prefixes = BTreeSet::new();
    let mut visited = 0_u64;
    if let Some(rows) = rows {
        for (key, stored) in rows {
            if !key.starts_with(&request.prefix) {
                continue;
            }
            visited = visited.saturating_add(1);
            let item = if let Some(delimiter) = request.delimiter {
                let suffix = &key[request.prefix.len()..];
                if let Some(index) = suffix.iter().position(|byte| *byte == delimiter) {
                    let end = request.prefix.len() + index + 1;
                    let prefix = key[..end].to_vec();
                    ProviderScanItem::CommonPrefix(prefix)
                } else {
                    ProviderScanItem::Key {
                        key: key.clone(),
                        value: stored.value.clone(),
                    }
                }
            } else {
                ProviderScanItem::Key {
                    key: key.clone(),
                    value: stored.value.clone(),
                }
            };
            let boundary = match &item {
                ProviderScanItem::Key { key, .. } => key,
                ProviderScanItem::CommonPrefix(prefix) => prefix,
            };
            if request
                .start_after
                .as_ref()
                .is_some_and(|start| boundary <= start)
            {
                continue;
            }
            if let ProviderScanItem::CommonPrefix(prefix) = &item {
                if !common_prefixes.insert(prefix.clone()) {
                    continue;
                }
            }
            items.push(item);
            if request.limit != 0 && items.len() == request.limit {
                break;
            }
        }
    }
    ProviderScanPage {
        stats: ProviderScanStats {
            visited,
            returned: items.len() as u64,
            common_prefixes: common_prefixes.len() as u64,
            restarts: 0,
        },
        items,
    }
}

fn identity(fill: u8) -> MetadataStoreIdentity {
    MetadataStoreIdentity {
        logical_shard_id: LogicalShardId::from_bytes([fill; 16]),
        authority_id: MetadataAuthorityId::from_bytes([fill.saturating_add(1); 16]),
        authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
        consistency_domain_id: ConsistencyDomainId::from_bytes([fill.saturating_add(2); 16]),
        profile_fingerprint: [fill.saturating_add(3); 32],
        contract_digest: workspace_metadata_contract_digest(),
    }
}

fn ready_external_store(
    fill: u8,
) -> (
    Arc<MemoryFactory>,
    AgentMetadataStore,
    RootId,
    MetadataStoreIdentity,
) {
    let runtime_bundle = Arc::new(MemoryFactory::default());
    let store_identity = identity(fill);
    let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
        runtime_bundle.clone(),
        store_identity,
        CreateRecoveryIntentV1::Fresh,
        MetadataStoreCreateModeV1::Active,
    )
    .unwrap();
    store
        .advance_owner_epoch(None, OwnerEpoch::new(1).unwrap())
        .unwrap();
    let root = RootId::from_bytes([fill.saturating_add(0x20); 16]);
    store
        .execute(&command(
            &store,
            root,
            1,
            RootFenceAction::Install {
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
            },
            None,
        ))
        .unwrap();
    store
        .execute(&command(
            &store,
            root,
            2,
            RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            None,
        ))
        .unwrap();
    (runtime_bundle, store, root, store_identity)
}

fn fault_command(store: &AgentMetadataStore, root: RootId, suffix: &[u8]) -> MetadataCommand {
    let key = [root.as_bytes().as_slice(), suffix].concat();
    command(
        store,
        root,
        3,
        RootFenceAction::RequireActive,
        Some((key, b"fault-value".to_vec())),
    )
}

fn spawn_store_probe<F>(
    store: AgentMetadataStore,
    started: std::sync::mpsc::Sender<()>,
    finished: std::sync::mpsc::Sender<()>,
    probe: F,
) -> std::thread::JoinHandle<()>
where
    F: FnOnce(AgentMetadataStore) + Send + 'static,
{
    std::thread::spawn(move || {
        started.send(()).unwrap();
        probe(store);
        finished.send(()).unwrap();
    })
}

#[test]
fn receipt_preflight_failure_has_zero_factory_delegates() {
    let unavailable = Arc::new(MemoryFactory::default());
    unavailable
        .receipt
        .unavailable
        .store(true, Ordering::Release);
    let store_identity = identity(0x31);
    assert!(matches!(
        AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            unavailable.clone(),
            store_identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        ),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
            unavailable.clone(),
            store_identity,
        ),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert_eq!(
        unavailable
            .control
            .contract_offer_calls
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(unavailable.control.create_calls.load(Ordering::Acquire), 0);
    assert_eq!(unavailable.control.reopen_calls.load(Ordering::Acquire), 0);

    let binding_drift = Arc::new(MemoryFactory::default());
    binding_drift
        .receipt
        .force_clean(identity(0x32), MetadataFrontierPointV1::Absent);
    assert!(matches!(
        AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            binding_drift.clone(),
            identity(0x33),
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        ),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert_eq!(
        binding_drift
            .control
            .contract_offer_calls
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        binding_drift.control.create_calls.load(Ordering::Acquire),
        0
    );
    assert_eq!(
        binding_drift.control.reopen_calls.load(Ordering::Acquire),
        0
    );
}

#[test]
fn definite_pending_rejection_has_zero_provider_write_delegates() {
    let (runtime_bundle, store, root, _) = ready_external_store(0x34);
    let command = fault_command(&store, root, b"/persist-failure");
    let before_receipt = runtime_bundle.receipt.state();
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle
        .receipt
        .reject_persist
        .store(true, Ordering::Release);

    let failure = store.execute(&command);
    assert!(
        matches!(failure, Err(AgentMetadataError::ProviderUnavailable { .. })),
        "unexpected receipt preflight failure result: {failure:?}"
    );
    assert_eq!(runtime_bundle.receipt.state(), before_receipt);
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit
    );
    assert!(store.current_read_version().is_ok());

    runtime_bundle
        .receipt
        .reject_persist
        .store(false, Ordering::Release);
    store.execute(&command).unwrap();
}

#[test]
fn pending_persist_response_loss_requires_recovery_without_provider_write() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x35);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/persist-response-loss");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle
        .receipt
        .persist_then_error
        .store(true, Ordering::Release);

    assert_eq!(
        store.execute(&command),
        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit
    );
    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }
    let before_recovery_reads = runtime_bundle
        .control
        .resolution_read_calls
        .load(Ordering::Acquire);
    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            store_identity,
        ),
        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .resolution_read_calls
            .load(Ordering::Acquire),
        before_recovery_reads
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit
    );

    let planned = match runtime_bundle.receipt.state() {
        MetadataCommitReceiptStateV1::Pending(planned) => planned,
        state => panic!("expected pending receipt, found {state:?}"),
    };
    let simulated = Arc::new(MemoryFactory {
        durable: Arc::new(Mutex::new(runtime_bundle.durable.lock().unwrap().clone())),
        ..MemoryFactory::default()
    });
    simulated
        .receipt
        .force_clean(store_identity, planned.prior());
    let simulated_store =
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(simulated.clone(), store_identity)
            .unwrap();
    simulated_store.execute(&command).unwrap();
    *runtime_bundle.durable.lock().unwrap() = simulated.durable.lock().unwrap().clone();

    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            store_identity,
        ),
        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    ));
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .resolution_read_calls
            .load(Ordering::Acquire),
        before_recovery_reads
    );
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit
    );
}

#[test]
fn applied_response_loss_resolves_exactly_and_replay_does_not_write() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x36);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/unknown-applied");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle.control.arm(CommitBehavior::UnknownApplied);

    let applied = store.execute(&command).unwrap();
    assert!(!applied.replayed);
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        }
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );

    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }
    assert!(matches!(
        store.execute(&command),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    let recovered = AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
        runtime_bundle.clone(),
        store_identity,
    )
    .unwrap();
    let replay = recovered.execute(&command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.commit_version, applied.commit_version);
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );
}

#[test]
fn committed_receipt_resolution_response_loss_fail_stops_only_that_allocation() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x37);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/resolve-response-loss");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle
        .receipt
        .resolve_then_error
        .store(true, Ordering::Release);

    assert_eq!(
        store.execute(&command),
        Err(AgentMetadataError::CommitOutcomeUnknown)
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        }
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );
    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }

    let recovered = AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
        runtime_bundle.clone(),
        store_identity,
    )
    .unwrap();
    assert!(recovered.current_read_version().is_ok());
    assert!(matches!(
        clone.current_read_version(),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
}

#[test]
fn unknown_not_applied_resolves_to_the_exact_prior() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x38);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/unknown-not-applied");
    let prior_receipt = runtime_bundle.receipt.state();
    runtime_bundle
        .control
        .arm(CommitBehavior::UnknownNotApplied);

    assert_eq!(
        store.execute(&command),
        Err(AgentMetadataError::WriteConflict)
    );
    assert_eq!(runtime_bundle.receipt.state(), prior_receipt);
    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }
    let recovered = AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
        runtime_bundle.clone(),
        store_identity,
    )
    .unwrap();
    assert!(recovered.current_read_version().is_ok());
}

#[test]
fn definite_begin_write_failure_cleans_prior_and_keeps_the_store_serving() {
    let (runtime_bundle, store, root, _) = ready_external_store(0x43);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/definite-begin-write-error");
    let prior_receipt = runtime_bundle.receipt.state();
    runtime_bundle
        .control
        .fail_begin_write_once
        .store(true, Ordering::Release);

    assert!(matches!(
        store.execute(&command),
        Err(AgentMetadataError::Backend { .. })
    ));
    assert_eq!(runtime_bundle.receipt.state(), prior_receipt);
    assert!(store.current_read_version().is_ok());
    assert!(clone.current_read_version().is_ok());

    let applied = store.execute(&command).unwrap();
    assert!(!applied.replayed);
    assert!(clone.current_read_version().is_ok());
}

#[test]
fn unknown_stale_first_view_converges_without_a_second_write() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x39);
    let command = fault_command(&store, root, b"/stale-first");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle
        .control
        .arm(CommitBehavior::UnknownStaleThenApplied);

    store.execute(&command).unwrap();
    assert!(runtime_bundle
        .control
        .delayed_commit
        .lock()
        .unwrap()
        .is_none());
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        }
    ));
    assert!(matches!(
        store.current_read_version(),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert!(AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
        runtime_bundle,
        store_identity,
    )
    .is_ok());
}

#[test]
fn logical_reads_and_fsck_wait_for_the_exact_receipt_to_be_clean() {
    let (runtime_bundle, store, root, _) = ready_external_store(0x40);
    runtime_bundle.receipt.resolve_barrier.arm();
    let writer_store = store.clone();
    let writer_command = fault_command(&writer_store, root, b"/resolve-barrier");
    let writer = std::thread::spawn(move || writer_store.execute(&writer_command));
    runtime_bundle.receipt.resolve_barrier.wait_until_entered();
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let mut probes = Vec::new();
    probes.push(spawn_store_probe(
        store.clone(),
        started_tx.clone(),
        finished_tx.clone(),
        |store| {
            store.current_read_version().unwrap();
        },
    ));
    probes.push(spawn_store_probe(
        store.clone(),
        started_tx.clone(),
        finished_tx.clone(),
        |store| {
            store.metadata_authority_state().unwrap();
        },
    ));
    probes.push(spawn_store_probe(
        store.clone(),
        started_tx.clone(),
        finished_tx.clone(),
        |store| {
            store.current_owner_epoch().unwrap();
        },
    ));
    probes.push(spawn_store_probe(
        store.clone(),
        started_tx.clone(),
        finished_tx.clone(),
        |store| {
            store.lease_clock_high_water().unwrap();
        },
    ));
    probes.push(spawn_store_probe(
        store,
        started_tx,
        finished_tx,
        move |store| {
            let _ = run_metadata_fsck(
                &store,
                MetadataFsckRequest {
                    trigger_root_id: root,
                    placement_generation: PlacementGeneration::new(1).unwrap(),
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    layout_profile: RootLayoutProfile::SingleShardRoot,
                    layout_generation: RootLayoutGeneration::new(1).unwrap(),
                    partition_id: RootPartitionId::SINGLE_SHARD,
                    limits: MetadataFsckLimits::default(),
                },
            );
        },
    ));
    for _ in 0..probes.len() {
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    assert!(matches!(
        finished_rx.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));

    runtime_bundle.receipt.resolve_barrier.release();
    assert!(writer.join().unwrap().is_ok());
    for _ in 0..probes.len() {
        finished_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    for probe in probes {
        probe.join().unwrap();
    }
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        }
    ));
}

#[test]
fn receipt_resolution_unwind_fail_stops_the_store_and_every_clone() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x41);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/resolve-panic");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle
        .receipt
        .panic_on_resolve
        .store(true, Ordering::Release);

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = store.execute(&command);
    }));
    assert!(unwind.is_err());
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );

    for guarded in [&store, &clone] {
        assert!(guarded.current_read_version().is_err());
        assert!(guarded.metadata_authority_state().is_err());
        assert!(guarded.current_owner_epoch().is_err());
        assert!(guarded.lease_clock_high_water().is_err());
        assert!(guarded.validate_provider_runtime().is_err());
        assert!(guarded.execute(&command).is_err());
    }
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );

    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            store_identity,
        ),
        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    ));
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
}

#[test]
fn pending_persist_unwind_fail_stops_before_any_provider_write() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x42);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/persist-panic");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    runtime_bundle
        .receipt
        .panic_after_persist
        .store(true, Ordering::Release);

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = store.execute(&command);
    }));
    assert!(unwind.is_err());
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit
    );

    for guarded in [&store, &clone] {
        assert!(guarded.current_read_version().is_err());
        assert!(guarded.metadata_authority_state().is_err());
        assert!(guarded.current_owner_epoch().is_err());
        assert!(guarded.lease_clock_high_water().is_err());
        assert!(guarded.validate_provider_runtime().is_err());
        assert!(guarded.execute(&command).is_err());
    }
    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            store_identity,
        ),
        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    ));
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Pending(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit
    );
}

#[test]
fn unsettled_unknown_remains_dirty_when_recovery_fence_is_unsupported() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x3f);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/unsettled-late-commit");
    let before_begin = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let before_commit = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    // This deliberately violates the fixture's admitted settlement claim. The
    // engine must still fail closed when an external provider misclassifies a
    // native outcome and later publishes the commit.
    runtime_bundle
        .control
        .arm(CommitBehavior::UnknownUnsettledStaleThenApplied);

    assert_eq!(
        store.execute(&command),
        Err(AgentMetadataError::CommitOutcomeUnknown)
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::PoisonedUnsettled(_)
    ));
    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );

    let recovery_reads = runtime_bundle
        .control
        .resolution_read_calls
        .load(Ordering::Acquire);
    // An external factory without the nominal typed capability cannot inspect
    // the provider at all. Reopen stays recovery-only and cannot advance the
    // simulated native completion or clear PoisonedUnsettled.
    for _ in 0..3 {
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                store_identity,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert!(matches!(
            runtime_bundle.receipt.state(),
            MetadataCommitReceiptStateV1::PoisonedUnsettled(_)
        ));
    }
    assert!(runtime_bundle
        .control
        .delayed_commit
        .lock()
        .unwrap()
        .is_some());
    assert_eq!(
        runtime_bundle
            .control
            .resolution_read_calls
            .load(Ordering::Acquire),
        recovery_reads
    );
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        before_begin + 1
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        before_commit + 1
    );
}

#[test]
fn conflict_with_same_purpose_evidence_converges_applied() {
    let (runtime_bundle, store, root, _) = ready_external_store(0x3a);
    runtime_bundle
        .control
        .arm(CommitBehavior::ConflictAppliedSamePurpose);

    store
        .execute(&fault_command(&store, root, b"/same-purpose"))
        .unwrap();
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        }
    ));
}

#[test]
fn conflict_with_foreign_higher_tail_keeps_recovery_only_receipt() {
    let (runtime_bundle, store, root, store_identity) = ready_external_store(0x3b);
    let clone = store.clone();
    let safe_version = store.current_read_version().unwrap();
    let key = [root.as_bytes().as_slice(), b"/foreign-tail"].concat();
    let command = command(
        &store,
        root,
        3,
        RootFenceAction::RequireActive,
        Some((key.clone(), b"fault-value".to_vec())),
    );
    runtime_bundle
        .control
        .arm(CommitBehavior::ConflictWithForeignHigherTail);

    assert_eq!(
        store.execute(&command),
        Err(AgentMetadataError::CommitOutcomeUnknown)
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::PoisonedSettled(_)
    ));
    let gets = runtime_bundle.control.get_calls.load(Ordering::Acquire);
    let reads = runtime_bundle
        .control
        .begin_read_calls
        .load(Ordering::Acquire);
    let writes = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let commits = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }
    assert!(matches!(
        store.metadata_frontier(),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert!(matches!(
        store.read_at(
            root,
            PlacementGeneration::new(1).unwrap(),
            OwnerEpoch::new(1).unwrap(),
            MetadataFamily::Operation,
            &key,
            safe_version,
        ),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert!(matches!(
        store.scan_prefix_at(
            root,
            PlacementGeneration::new(1).unwrap(),
            OwnerEpoch::new(1).unwrap(),
            MetadataFamily::Operation,
            root.as_bytes(),
            safe_version,
            None,
            16,
        ),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert!(matches!(
        store.execute(&command),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert_eq!(
        runtime_bundle.control.get_calls.load(Ordering::Acquire),
        gets
    );
    assert_eq!(
        runtime_bundle
            .control
            .begin_read_calls
            .load(Ordering::Acquire),
        reads
    );
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        writes
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        commits
    );

    let planned = match runtime_bundle.receipt.state() {
        MetadataCommitReceiptStateV1::PoisonedSettled(planned) => planned,
        state => panic!("expected poisoned receipt, found {state:?}"),
    };
    {
        let mut durable = runtime_bundle.durable.lock().unwrap();
        durable.next_version = durable.next_version.saturating_add(1);
        let record_version = durable.next_version;
        let clock = durable
            .rows
            .get_mut(&OrderedSpaceId::new(0x0101))
            .and_then(|system| system.get_mut(b"commit_clock".as_slice()))
            .unwrap();
        clock.value = [
            [1].as_slice(),
            planned
                .exact_next()
                .commit_version
                .get()
                .to_be_bytes()
                .as_slice(),
        ]
        .concat();
        clock.version = record_version;
    }
    let recovery_reads = runtime_bundle
        .control
        .resolution_read_calls
        .load(Ordering::Acquire);
    assert!(matches!(
        AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            store_identity,
        ),
        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    ));
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::PoisonedSettled(_)
    ));
    assert_eq!(
        runtime_bundle
            .control
            .resolution_read_calls
            .load(Ordering::Acquire),
        recovery_reads
    );
    assert!(matches!(
        clone.current_read_version(),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
}

#[test]
fn foreign_tail_poison_response_loss_is_unknown_and_fail_stopped() {
    let (runtime_bundle, store, root, _) = ready_external_store(0x3c);
    let clone = store.clone();
    let command = fault_command(&store, root, b"/poison-response-loss");
    runtime_bundle
        .receipt
        .poison_then_error
        .store(true, Ordering::Release);
    runtime_bundle
        .control
        .arm(CommitBehavior::ConflictWithForeignHigherTail);

    assert_eq!(
        store.execute(&command),
        Err(AgentMetadataError::CommitOutcomeUnknown)
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::PoisonedSettled(_)
    ));
    let reads = runtime_bundle
        .control
        .begin_read_calls
        .load(Ordering::Acquire);
    let writes = runtime_bundle
        .control
        .begin_write_calls
        .load(Ordering::Acquire);
    let commits = runtime_bundle.control.commit_calls.load(Ordering::Acquire);
    for result in [store.current_read_version(), clone.current_read_version()] {
        assert!(matches!(
            result,
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
    }
    assert!(matches!(
        store.execute(&command),
        Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_read_calls
            .load(Ordering::Acquire),
        reads
    );
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        writes
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        commits
    );
}

#[test]
fn exact_next_with_purpose_evidence_mismatch_is_not_applied() {
    let (runtime_bundle, store, root, _) = ready_external_store(0x3d);
    runtime_bundle
        .control
        .arm(CommitBehavior::CommittedWithPurposeEvidenceMismatch);

    assert_eq!(
        store.execute(&fault_command(&store, root, b"/evidence-mismatch")),
        Err(AgentMetadataError::CommitOutcomeUnknown)
    );
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::PoisonedSettled(_)
    ));
}

#[test]
fn partial_genesis_is_rejected_before_pending_or_provider_write() {
    let runtime_bundle = Arc::new(MemoryFactory::default());
    runtime_bundle
        .durable
        .lock()
        .unwrap()
        .rows
        .entry(OrderedSpaceId::new(0x0101))
        .or_default()
        .insert(
            b"partial-genesis".to_vec(),
            StoredValue {
                value: b"foreign".to_vec(),
                version: 1,
            },
        );
    let store_identity = identity(0x3e);

    assert!(matches!(
        AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            store_identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        ),
        Err(AgentMetadataError::SchemaGate { .. })
    ));
    assert!(matches!(
        runtime_bundle.receipt.state(),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Absent,
            ..
        }
    ));
    assert_eq!(
        runtime_bundle
            .control
            .begin_write_calls
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime_bundle.control.commit_calls.load(Ordering::Acquire),
        0
    );
}

#[test]
fn external_provider_is_object_safe_and_opens_only_through_the_facade() {
    fn object_safe(
        _factory: Option<&dyn MetadataProviderFactoryV1>,
        _runtime_bundle: Option<&dyn MetadataRuntimeCommitBundleV1>,
        _provider: Option<&dyn MetadataProvider>,
        _view: Option<&dyn MetadataReadView>,
        _transaction: Option<Box<dyn MetadataTransaction>>,
    ) {
    }

    object_safe(None, None, None, None, None);

    let runtime_bundle = Arc::new(MemoryFactory::default());
    let store_identity = identity(0x21);
    let store = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
        runtime_bundle.clone(),
        store_identity,
        CreateRecoveryIntentV1::Fresh,
        MetadataStoreCreateModeV1::Active,
    )
    .unwrap();
    assert_eq!(runtime_bundle.last_schema().ordered_spaces().len(), 26);
    assert_eq!(store.metadata_store_identity(), store_identity);

    store
        .advance_owner_epoch(None, OwnerEpoch::new(1).unwrap())
        .unwrap();
    let root = RootId::from_bytes([0x42; 16]);
    let install = command(
        &store,
        root,
        1,
        RootFenceAction::Install {
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
        },
        None,
    );
    store.execute(&install).unwrap();
    let activate = command(
        &store,
        root,
        2,
        RootFenceAction::Transition {
            expected: RootActivationState::Installing,
            next: RootActivationState::Active,
        },
        None,
    );
    store.execute(&activate).unwrap();
    let key = [root.as_bytes().as_slice(), b"/external-provider"].concat();
    let write = command(
        &store,
        root,
        3,
        RootFenceAction::RequireActive,
        Some((key.clone(), b"value".to_vec())),
    );
    store.execute(&write).unwrap();
    drop(store);

    let reopened = AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
        runtime_bundle.clone(),
        store_identity,
    )
    .unwrap();
    let version = reopened.current_read_version().unwrap();
    assert_eq!(
        reopened
            .read_at(
                root,
                PlacementGeneration::new(1).unwrap(),
                OwnerEpoch::new(1).unwrap(),
                MetadataFamily::Operation,
                &key,
                version,
            )
            .unwrap(),
        Some(b"value".to_vec())
    );

    runtime_bundle
        .runtime_rejected
        .store(true, Ordering::Release);
    let error = reopened.validate_provider_runtime().unwrap_err();
    assert!(matches!(
        error,
        AgentMetadataError::ProviderAuthorityMismatch { .. }
    ));
}

fn command(
    store: &AgentMetadataStore,
    root_id: RootId,
    request_fill: u8,
    root_fence_action: RootFenceAction,
    mutation: Option<(Vec<u8>, Vec<u8>)>,
) -> MetadataCommand {
    let predicates = mutation
        .as_ref()
        .map(|(key, _)| CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: None,
        })
        .into_iter()
        .collect();
    let mutations = mutation
        .map(|(key, value)| CommandMutation::Put {
            family: MetadataFamily::Operation,
            key,
            value,
        })
        .into_iter()
        .collect();
    MetadataCommand {
        schema_id: SCHEMA_ID.to_owned(),
        root_id,
        logical_shard_id: store.metadata_store_identity().logical_shard_id,
        placement_generation: PlacementGeneration::new(1).unwrap(),
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        request_id: RequestId::from_bytes([request_fill; 16]),
        command_digest: CommandDigest::from_bytes([0; 32]),
        read_version: store
            .current_read_version()
            .unwrap_or_else(|_| ReadVersion::new(1).unwrap()),
        root_fence_action,
        predicates,
        mutations,
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result: b"ok".to_vec(),
    }
    .seal()
}
