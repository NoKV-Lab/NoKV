use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use holt::{Durability, RecordVersion, Tree, TreeConfig, DB};
use nokv_types::LogicalShardId;

use crate::built_in_holt::{
    HoltExistingStoreReservation, HoltRuntimeGuard, HoltStoreObjectIdentity, NoopHoltRuntimeGuard,
};

#[cfg(test)]
use super::ProviderErrorKind;
use super::{
    all_ordered_spaces, AtomicCommitOutcome, AtomicOp, AtomicPlan, MetadataProvider,
    MetadataReadView, MetadataTransaction, OrderedSpaceId, ProviderCapabilities,
    ProviderContractOfferV1, ProviderCreateRequestV1, ProviderError, ProviderInstanceToken,
    ProviderOperationV1, ProviderRecord, ProviderReopenRequestV1, ProviderScan, ProviderScanItem,
    ProviderScanPage, ProviderScanStats, ProviderSchemaV1, ProviderTransactionModel,
    ProviderVersionModel, ReadScope, ReadWitness,
};
use crate::provider::v1::{CreateRecoveryIntentV1, MetadataProviderFactoryV1};
#[cfg(feature = "metadata-read-stats")]
use crate::provider::v1::{ProviderDiagnosticsSnapshotV1, ProviderDiagnosticsV1};
use crate::workspace::codec::SCHEMA_TREES;
use crate::workspace::commit_recovery_fence::{
    mint_old_dispatch_exclusion_installation_v1, MetadataCommitRecoveryFenceFactoryV1,
    MetadataOldDispatchExclusionInstallationAuthorityV1,
    MetadataOldDispatchExclusionInstallationV1, MetadataPendingRecoveryOpenCommandV1,
    MetadataPendingRecoveryOpenNotDispatchedV1, MetadataPendingRecoveryOpenOutcomeV1,
};

#[derive(Clone)]
pub(crate) struct HoltProvider {
    db: DB,
    logical_shard_id: LogicalShardId,
    identity: ProviderInstanceToken,
    trees: BTreeMap<OrderedSpaceId, Tree>,
    runtime_guard: Arc<dyn HoltRuntimeGuard>,
    store_object_identity: Option<HoltStoreObjectIdentity>,
}

#[derive(Clone)]
enum HoltProviderLocation {
    Memory,
    File {
        path: PathBuf,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
    },
    ReservedExisting(Arc<HoltReservedExistingLocation>),
}

struct HoltReservedExistingLocation {
    expected: HoltStoreObjectIdentity,
    runtime_guard: Arc<dyn HoltRuntimeGuard>,
    recovery_fence_installation: MetadataOldDispatchExclusionInstallationAuthorityV1,
    state: Mutex<HoltReservedExistingState>,
}

enum HoltReservedExistingState {
    Ready(HoltExistingStoreReservation),
    Opened(DB),
    Delivered,
}

#[derive(Clone)]
pub(crate) struct HoltProviderFactory {
    location: HoltProviderLocation,
}

impl HoltProviderFactory {
    pub(crate) fn memory() -> Self {
        Self {
            location: HoltProviderLocation::Memory,
        }
    }

    pub(crate) fn file(path: &Path, runtime_guard: Arc<dyn HoltRuntimeGuard>) -> Self {
        Self {
            location: HoltProviderLocation::File {
                path: path.to_owned(),
                runtime_guard,
            },
        }
    }

    pub(crate) fn reserved_existing(
        reservation: HoltExistingStoreReservation,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
    ) -> Self {
        let expected = reservation.expected_identity().clone();
        let (_, recovery_fence_installation) = mint_old_dispatch_exclusion_installation_v1();
        Self {
            location: HoltProviderLocation::ReservedExisting(Arc::new(
                HoltReservedExistingLocation {
                    expected,
                    runtime_guard,
                    recovery_fence_installation,
                    state: Mutex::new(HoltReservedExistingState::Ready(reservation)),
                },
            )),
        }
    }

    fn validate_schema(schema: &ProviderSchemaV1) -> Result<(), ProviderError> {
        if schema.spi_major() != ProviderSchemaV1::SPI_MAJOR
            || schema.workspace_contract_digest()
                != crate::workspace::workspace_metadata_contract_digest()
            || schema.ordered_spaces() != all_ordered_spaces()
        {
            return Err(ProviderError::schema());
        }
        Ok(())
    }
}

impl MetadataProviderFactoryV1 for HoltProviderFactory {
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        Self::validate_schema(schema)?;
        Ok(ProviderContractOfferV1 {
            capabilities: HoltProvider::capabilities_value(),
        })
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        if matches!(&self.location, HoltProviderLocation::ReservedExisting(_))
            || request.recovery_intent() == CreateRecoveryIntentV1::ReconcilePrepared
        {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Create,
            ));
        }
        request.claim_execution()?;
        Self::validate_schema(request.schema())?;
        let store_identity = request.store_identity();
        let provider = match &self.location {
            HoltProviderLocation::Memory => {
                HoltProvider::open_memory(store_identity.logical_shard_id)?
            }
            HoltProviderLocation::File {
                path,
                runtime_guard,
            } => HoltProvider::create_file_observed(
                path,
                store_identity.logical_shard_id,
                runtime_guard.clone(),
            )?,
            HoltProviderLocation::ReservedExisting(_) => unreachable!(
                "reserved existing locations are rejected before provider create execution"
            ),
        };
        Ok(Arc::new(provider))
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        if matches!(
            &self.location,
            HoltProviderLocation::Memory | HoltProviderLocation::ReservedExisting(_)
        ) {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Reopen,
            ));
        }
        request.claim_execution()?;
        Self::validate_schema(request.schema())?;
        match &self.location {
            HoltProviderLocation::File {
                path,
                runtime_guard,
            } => Ok(Arc::new(HoltProvider::reopen_file_observed(
                path,
                request.expected_store_identity().logical_shard_id,
                runtime_guard.clone(),
            )?)),
            HoltProviderLocation::Memory | HoltProviderLocation::ReservedExisting(_) => {
                unreachable!("non-file locations are rejected before provider reopen execution")
            }
        }
    }
}

impl MetadataCommitRecoveryFenceFactoryV1 for HoltProviderFactory {
    fn old_dispatch_exclusion_installation_v1(&self) -> MetadataOldDispatchExclusionInstallationV1 {
        match &self.location {
            HoltProviderLocation::ReservedExisting(location) => {
                location.recovery_fence_installation.capability()
            }
            HoltProviderLocation::Memory | HoltProviderLocation::File { .. } => {
                MetadataOldDispatchExclusionInstallationV1::unsupported()
            }
        }
    }

    fn reopen_pending_with_old_dispatch_excluded_v1(
        &self,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        let HoltProviderLocation::ReservedExisting(location) = &self.location else {
            return command
                .reject_before_execution(MetadataPendingRecoveryOpenNotDispatchedV1::Unsupported);
        };
        if Self::validate_schema(command.schema()).is_err()
            || command.expected_installation() != &location.recovery_fence_installation.capability()
        {
            return command.reject_before_execution(
                MetadataPendingRecoveryOpenNotDispatchedV1::InvalidBinding,
            );
        }

        let planned = command.planned().clone();
        let logical_shard_id = planned.store_identity().logical_shard_id;
        let claimed = command.claim_execution();
        let provider = match location.reopen(logical_shard_id) {
            Ok(provider) => provider,
            Err(_) => {
                let guard: Arc<dyn Send + Sync> = Arc::clone(location) as Arc<dyn Send + Sync>;
                return claimed.complete_outcome_unknown_retaining(guard);
            }
        };
        let lifetime_guard = provider.db.clone();
        let provider: Arc<dyn MetadataProvider> = Arc::new(provider);
        let backend_authority = location.recovery_fence_installation.bind_opened_provider(
            &planned,
            &provider,
            lifetime_guard,
        );
        claimed.complete_opened_old_dispatch_excluded(provider, backend_authority)
    }
}

impl HoltReservedExistingLocation {
    fn reopen(&self, logical_shard_id: LogicalShardId) -> Result<HoltProvider, ProviderError> {
        // Holt guarantees that an adoption error or unwind leaves the token
        // ready. Recovering a poisoned mutex is therefore intentional: the
        // protected state remains the single exact carrier after a caller
        // catches an unwind from provider-specific validation.
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        if let HoltReservedExistingState::Ready(reservation) = &mut *state {
            let mut config = TreeConfig::new(self.expected.canonical_locator())
                .with_expected_file_store_identity(self.expected.holt_identity());
            config.durability = Durability::Wal { sync: true };
            config.checkpoint.auto_vacuum = false;
            let db = DB::open_with_file_store_reservation(config, reservation.reservation_mut())
                .map_err(reserved_existing_open_error)?;
            // Store the adopted DB before invoking any external guard or
            // schema code. Errors and caught unwinds can then retry against
            // these same held kernel objects without reacquiring by name.
            *state = HoltReservedExistingState::Opened(db);
        }

        let HoltReservedExistingState::Opened(db) = &*state else {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Reopen,
            ));
        };
        let identity = held_store_object_identity(
            db,
            self.expected.canonical_locator().to_owned(),
            ProviderOperationV1::Reopen,
        )?;
        if identity != self.expected {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Reopen,
            ));
        }
        self.runtime_guard
            .bind_store(&identity)
            .map_err(|_| runtime_guard_error("bind reserved Holt store"))?;
        self.runtime_guard
            .validate_runtime()
            .map_err(|_| runtime_guard_error("validate reserved Holt store"))?;
        validate_tree_registry(db)?;
        let provider = HoltProvider::open(
            db.clone(),
            logical_shard_id,
            self.runtime_guard.clone(),
            Some(identity),
        )?;
        *state = HoltReservedExistingState::Delivered;
        Ok(provider)
    }
}

impl HoltProvider {
    const MAX_KEY_BYTES: usize = u16::MAX as usize;
    const MAX_VALUE_BYTES: usize = u16::MAX as usize;
    // Holt journals one DB-wide batch in a record whose body length is u32.
    // `holt_affected_bytes` includes conservative per-operation framing.
    const MAX_TRANSACTION_BYTES: usize = u32::MAX as usize;

    pub(crate) fn open_memory(logical_shard_id: LogicalShardId) -> Result<Self, ProviderError> {
        Self::initialize_fresh(
            TreeConfig::memory(),
            logical_shard_id,
            Arc::new(NoopHoltRuntimeGuard),
            None,
        )
    }

    pub(crate) fn create_file_observed(
        path: &Path,
        logical_shard_id: LogicalShardId,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
    ) -> Result<Self, ProviderError> {
        require_fresh_location(path)?;
        let before = preopen_create_identity(path)?;
        let mut config = TreeConfig::new(path);
        config.durability = Durability::Wal { sync: true };
        config.checkpoint.auto_vacuum = false;
        let db = DB::open(config).map_err(|error| read_error("create database", error))?;
        let identity = held_store_object_identity(
            &db,
            before.canonical_locator.clone(),
            ProviderOperationV1::Create,
        )?;
        before.validate_after(&identity)?;
        runtime_guard
            .bind_store(&identity)
            .map_err(|_| runtime_guard_error("bind opened Holt store"))?;
        runtime_guard
            .validate_runtime()
            .map_err(|_| runtime_guard_error("validate opened Holt store"))?;
        Self::initialize_fresh_db(db, logical_shard_id, runtime_guard, Some(identity))
    }

    pub(crate) fn reopen_file_observed(
        path: &Path,
        logical_shard_id: LogicalShardId,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
    ) -> Result<Self, ProviderError> {
        require_existing_location(path)?;
        let before = capture_store_object_identity(path)?;
        let mut config = TreeConfig::new(path);
        config.durability = Durability::Wal { sync: true };
        config.checkpoint.auto_vacuum = false;
        let db = DB::open(config).map_err(|error| read_error("open database", error))?;
        let after = held_store_object_identity(
            &db,
            before.canonical_locator().to_owned(),
            ProviderOperationV1::Reopen,
        )?;
        if before != after {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Reopen,
            ));
        }
        runtime_guard
            .bind_store(&after)
            .map_err(|_| runtime_guard_error("bind reopened Holt store"))?;
        runtime_guard
            .validate_runtime()
            .map_err(|_| runtime_guard_error("validate reopened Holt store"))?;
        validate_tree_registry(&db)?;
        Self::open(db, logical_shard_id, runtime_guard, Some(after))
    }

    fn initialize_fresh(
        config: TreeConfig,
        logical_shard_id: LogicalShardId,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
        store_object_identity: Option<HoltStoreObjectIdentity>,
    ) -> Result<Self, ProviderError> {
        let db = DB::open(config).map_err(|error| read_error("create database", error))?;
        Self::initialize_fresh_db(db, logical_shard_id, runtime_guard, store_object_identity)
    }

    fn initialize_fresh_db(
        db: DB,
        logical_shard_id: LogicalShardId,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
        store_object_identity: Option<HoltStoreObjectIdentity>,
    ) -> Result<Self, ProviderError> {
        let existing = db
            .list_trees()
            .map_err(|error| read_error("inspect fresh database", error))?;
        if !existing.is_empty() {
            return Err(ProviderError::schema());
        }
        for tree in SCHEMA_TREES {
            db.create_tree(tree)
                .map_err(|error| read_error("create schema tree", error))?;
        }
        Self::open(db, logical_shard_id, runtime_guard, store_object_identity)
    }

    fn open(
        db: DB,
        logical_shard_id: LogicalShardId,
        runtime_guard: Arc<dyn HoltRuntimeGuard>,
        store_object_identity: Option<HoltStoreObjectIdentity>,
    ) -> Result<Self, ProviderError> {
        let mut trees = BTreeMap::new();
        for space in all_ordered_spaces() {
            let tree = db
                .open_tree(tree_name(space))
                .map_err(|error| read_error("open metadata space", error))?;
            trees.insert(space, tree);
        }
        Ok(Self {
            db,
            logical_shard_id,
            identity: ProviderInstanceToken::new(),
            trees,
            runtime_guard,
            store_object_identity,
        })
    }

    fn validate_runtime(&self, operation: &'static str) -> Result<(), ProviderError> {
        self.runtime_guard
            .validate_runtime()
            .map_err(|_| runtime_guard_error(operation))?;
        if let Some(expected) = &self.store_object_identity {
            let current = held_store_object_identity(
                &self.db,
                expected.canonical_locator().to_owned(),
                operation_code(operation),
            )?;
            if &current != expected {
                return Err(ProviderError::authority_mismatch(operation_code(operation)));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn tree_names(&self) -> Result<Vec<String>, ProviderError> {
        self.db
            .list_trees()
            .map_err(|error| read_error("list Holt schema trees", error))
    }

    fn tree(&self, space: OrderedSpaceId) -> Result<&Tree, ProviderError> {
        self.trees
            .get(&space)
            .ok_or_else(ProviderError::invalid_plan)
    }

    fn record(&self, record: holt::Record) -> ProviderRecord {
        ProviderRecord {
            value: record.value,
            witness: self
                .identity
                .issue_witness(record.version.as_u64().to_be_bytes().to_vec()),
        }
    }

    fn version(&self, witness: &ReadWitness) -> Result<RecordVersion, ProviderError> {
        let raw: [u8; 8] = self
            .identity
            .parse_witness(witness)
            .map_err(|_| ProviderError::authority_mismatch(ProviderOperationV1::ValidateWitness))?
            .try_into()
            .map_err(|_| ProviderError::invalid_plan())?;
        Ok(RecordVersion::from_raw(u64::from_be_bytes(raw)))
    }

    fn capabilities_value() -> ProviderCapabilities {
        let requirements = crate::provider::admission::workspace_provider_requirements_v1();
        ProviderCapabilities {
            transaction_model: ProviderTransactionModel::CrossSpaceAtomicBatch,
            version_model: ProviderVersionModel::OpaqueRecordWitness,
            consistent_cross_space_reads: true,
            all_ambiguous_commit_outcomes_settled_before_return: true,
            // Holt captures each view from the current committed database;
            // a repeated post-Unknown resolution view crosses the settled cut.
            commit_resolution_reads_causally_current: true,
            max_key_bytes: Self::MAX_KEY_BYTES,
            max_value_bytes: Self::MAX_VALUE_BYTES,
            max_transaction_bytes: Self::MAX_TRANSACTION_BYTES,
            max_atomic_operations: requirements.max_atomic_operations,
            max_logical_plan_bytes: requirements.max_logical_plan_bytes,
            exclusive_scan_start_after: true,
            consistent_snapshot_scans: true,
            max_read_view_duration: None,
            max_scan_items: None,
        }
    }

    fn preflight_plan(&self, plan: &AtomicPlan) -> Result<(), ProviderError> {
        let capabilities = Self::capabilities_value();
        let affected_bytes = holt_affected_bytes(plan);
        debug_assert!(affected_bytes >= plan.logical_footprint());
        if affected_bytes > capabilities.max_transaction_bytes {
            return Err(ProviderError::transaction_too_large(
                affected_bytes,
                capabilities.max_transaction_bytes,
            ));
        }
        for operation in &plan.operations {
            let space = match operation {
                AtomicOp::AssertUnchanged { space, .. }
                | AtomicOp::AssertAbsent { space, .. }
                | AtomicOp::AssertPrefixEmpty { space, .. }
                | AtomicOp::Put { space, .. }
                | AtomicOp::PutIfAbsent { space, .. }
                | AtomicOp::CompareAndPut { space, .. }
                | AtomicOp::Delete { space, .. } => *space,
            };
            self.tree(space)?;
            match operation {
                AtomicOp::AssertUnchanged { key, witness, .. } => {
                    validate_length(key.len(), capabilities.max_key_bytes)?;
                    self.version(witness)?;
                }
                AtomicOp::AssertAbsent { key, .. } => {
                    validate_length(key.len(), capabilities.max_key_bytes)?;
                }
                AtomicOp::AssertPrefixEmpty { prefix, .. } => {
                    validate_length(prefix.len(), capabilities.max_key_bytes)?;
                }
                AtomicOp::Put { key, value, .. } | AtomicOp::PutIfAbsent { key, value, .. } => {
                    validate_length(key.len(), capabilities.max_key_bytes)?;
                    validate_length(value.len(), capabilities.max_value_bytes)?;
                }
                AtomicOp::CompareAndPut {
                    key,
                    witness,
                    value,
                    ..
                } => {
                    validate_length(key.len(), capabilities.max_key_bytes)?;
                    validate_length(value.len(), capabilities.max_value_bytes)?;
                    self.version(witness)?;
                }
                AtomicOp::Delete { key, .. } => {
                    validate_length(key.len(), capabilities.max_key_bytes)?;
                }
            }
        }
        Ok(())
    }

    fn scan_tree(
        tree: HoltReadTree<'_>,
        request: &ProviderScan,
    ) -> Result<ProviderScanPage, ProviderError> {
        validate_length(request.prefix.len(), Self::MAX_KEY_BYTES)?;
        if let Some(start_after) = &request.start_after {
            validate_length(start_after.len(), Self::MAX_KEY_BYTES)?;
        }
        macro_rules! scan_range {
            ($range:expr) => {{
                let mut range = $range;
                if let Some(start_after) = &request.start_after {
                    range = range.start_after(start_after);
                }
                if let Some(delimiter) = request.delimiter {
                    range = range.delimiter(delimiter);
                }
                let mut items = Vec::new();
                let mut returned = 0_u64;
                let mut common_prefixes = 0_u64;
                let mut iterator = range.into_iter();
                for entry in iterator.by_ref() {
                    let item = match entry.map_err(|error| read_error("scan metadata", error))? {
                        holt::RangeEntry::Key { key, value, .. } => {
                            ProviderScanItem::Key { key, value }
                        }
                        holt::RangeEntry::CommonPrefix(prefix) => {
                            ProviderScanItem::CommonPrefix(prefix)
                        }
                        _ => continue,
                    };
                    let boundary = match &item {
                        ProviderScanItem::Key { key, .. } => key,
                        ProviderScanItem::CommonPrefix(prefix) => prefix,
                    };
                    // Holt's physical lower bound is evaluated before
                    // delimiter projection. A cursor that is itself a common
                    // prefix can therefore expose a child leaf and project it
                    // back to the same prefix. Enforce the provider contract
                    // on the logical item boundary as well, so every page is
                    // strictly greater than its key-or-prefix cursor.
                    if request
                        .start_after
                        .as_ref()
                        .is_some_and(|start| boundary <= start)
                    {
                        continue;
                    }
                    match &item {
                        ProviderScanItem::Key { .. } => returned = returned.saturating_add(1),
                        ProviderScanItem::CommonPrefix(_) => {
                            common_prefixes = common_prefixes.saturating_add(1);
                        }
                    }
                    items.push(item);
                    if request.limit != 0 && items.len() == request.limit {
                        break;
                    }
                }
                let stats = iterator.stats();
                Ok(ProviderScanPage {
                    items,
                    stats: ProviderScanStats {
                        visited: stats.visited,
                        returned,
                        common_prefixes,
                        restarts: stats.restarts,
                    },
                })
            }};
        }

        match tree {
            HoltReadTree::Tree(tree) => scan_range!(tree.range().prefix(&request.prefix)),
            HoltReadTree::View(view) => {
                let range = view
                    .scan(&request.prefix)
                    .map_err(|error| read_error("open metadata view scan", error))?;
                scan_range!(range)
            }
        }
    }
}

fn holt_affected_bytes(plan: &AtomicPlan) -> usize {
    const OPERATION_OVERHEAD_BYTES: usize = 32;

    plan.operations.iter().fold(0_usize, |total, operation| {
        let physical = match operation {
            AtomicOp::AssertUnchanged { key, .. } => {
                key.len().saturating_add(std::mem::size_of::<u64>())
            }
            AtomicOp::AssertAbsent { key, .. } => key.len().saturating_mul(2).saturating_add(1),
            AtomicOp::AssertPrefixEmpty { prefix, .. } => prefix.len(),
            AtomicOp::Put { key, value, .. }
            | AtomicOp::PutIfAbsent { key, value, .. }
            | AtomicOp::CompareAndPut { key, value, .. } => key.len().saturating_add(value.len()),
            AtomicOp::Delete { key, .. } => key.len(),
        };
        total
            .saturating_add(OPERATION_OVERHEAD_BYTES)
            .saturating_add(physical)
    })
}

impl MetadataProvider for HoltProvider {
    fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        Self::capabilities_value()
    }

    fn validate_runtime(&self) -> Result<(), ProviderError> {
        HoltProvider::validate_runtime(self, "validate metadata provider runtime")
    }

    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.validate_runtime("read metadata record")?;
        validate_length(key.len(), Self::MAX_KEY_BYTES)?;
        let record = self
            .tree(space)?
            .get_record(key)
            .map(|record| record.map(|record| self.record(record)))
            .map_err(|error| read_error("read metadata record", error))?;
        self.validate_runtime("validate metadata record read")?;
        Ok(record)
    }

    fn begin_read(&self, scopes: &[ReadScope]) -> Result<Box<dyn MetadataReadView>, ProviderError> {
        self.validate_runtime("begin metadata read")?;
        for scope in scopes {
            validate_length(scope.prefix.len(), Self::MAX_KEY_BYTES)?;
            self.tree(scope.space)?;
        }
        let scopes = coalesce_scopes(scopes);
        let raw_scopes = scopes
            .iter()
            .map(|(space, prefix)| (tree_name(*space), prefix.as_slice()))
            .collect::<Vec<_>>();
        let views = self
            .db
            .view(&raw_scopes, |view| {
                Ok(scopes
                    .keys()
                    .map(|space| {
                        let tree = view
                            .tree(tree_name(*space))
                            .expect("every requested Holt scope is captured")
                            .clone();
                        (*space, tree)
                    })
                    .collect::<BTreeMap<_, _>>())
            })
            .map_err(|error| read_error("capture metadata read view", error))?;
        Ok(Box::new(HoltReadView {
            provider: self.clone(),
            views,
        }))
    }

    fn begin_write(&self) -> Result<Box<dyn MetadataTransaction>, ProviderError> {
        self.validate_runtime("begin metadata write")?;
        Ok(Box::new(HoltTransaction {
            provider: self.clone(),
        }))
    }

    fn diagnostics(&self) -> Option<&dyn crate::provider::v1::ProviderDiagnosticsV1> {
        #[cfg(feature = "metadata-read-stats")]
        {
            Some(self)
        }
        #[cfg(not(feature = "metadata-read-stats"))]
        {
            None
        }
    }
}

#[cfg(feature = "metadata-read-stats")]
impl ProviderDiagnosticsV1 for HoltProvider {
    fn snapshot(&self) -> Result<ProviderDiagnosticsSnapshotV1, ProviderError> {
        let storage = self.db.stats();
        Ok(ProviderDiagnosticsSnapshotV1 {
            cache_hits: Some(storage.bm_cache_hits),
            cache_misses: Some(storage.bm_cache_misses),
            full_read_operations: Some(storage.bm_full_blob_reads),
            full_read_bytes: Some(storage.bm_full_blob_read_bytes),
            point_full_read_operations: Some(storage.bm_point_full_blob_reads),
            scan_full_read_operations: Some(storage.bm_scan_full_blob_reads),
            internal_full_read_operations: Some(storage.bm_silent_full_blob_reads),
            partial_read_cache_hits: Some(storage.bm_read_page_hits),
            partial_read_cache_misses: Some(storage.bm_read_page_misses),
        })
    }
}

struct HoltReadView {
    provider: HoltProvider,
    views: BTreeMap<OrderedSpaceId, holt::View>,
}

impl HoltReadView {
    fn view(&self, space: OrderedSpaceId) -> Result<&holt::View, ProviderError> {
        self.views
            .get(&space)
            .ok_or_else(ProviderError::invalid_plan)
    }
}

impl MetadataReadView for HoltReadView {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.provider
            .validate_runtime("read metadata view record")?;
        let record = self
            .view(space)?
            .get_record(key)
            .map(|record| {
                record.map(|record| ProviderRecord {
                    value: record.value,
                    witness: self
                        .provider
                        .identity
                        .issue_witness(record.version.as_u64().to_be_bytes().to_vec()),
                })
            })
            .map_err(|error| read_error("read metadata view", error))?;
        self.provider
            .validate_runtime("validate metadata view record read")?;
        Ok(record)
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        self.provider.validate_runtime("scan metadata view")?;
        let page = HoltProvider::scan_tree(HoltReadTree::View(self.view(request.space)?), request)?;
        self.provider
            .validate_runtime("validate metadata view scan")?;
        Ok(page)
    }
}

struct HoltTransaction {
    provider: HoltProvider,
}

impl MetadataReadView for HoltTransaction {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.provider.get(space, key)
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        self.provider
            .validate_runtime("scan metadata transaction")?;
        let page = HoltProvider::scan_tree(
            HoltReadTree::Tree(self.provider.tree(request.space)?),
            request,
        )?;
        self.provider
            .validate_runtime("validate metadata transaction scan")?;
        Ok(page)
    }
}

impl MetadataTransaction for HoltTransaction {
    fn prefix_is_empty(&self, space: OrderedSpaceId, prefix: &[u8]) -> Result<bool, ProviderError> {
        self.provider
            .validate_runtime("check metadata transaction prefix")?;
        let mut iterator = self
            .provider
            .tree(space)?
            .range()
            .prefix(prefix)
            .into_iter();
        let empty = match iterator.next() {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(error)) => Err(read_error("check metadata prefix", error)),
        }?;
        self.provider
            .validate_runtime("validate metadata transaction prefix check")?;
        Ok(empty)
    }

    fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
        self.provider
            .validate_runtime("commit metadata transaction")?;
        self.provider.preflight_plan(&plan)?;
        let mut resolved = Vec::with_capacity(plan.operations.len());
        for operation in plan.operations {
            let operation = match operation {
                AtomicOp::AssertUnchanged {
                    space,
                    key,
                    witness,
                } => ResolvedAtomicOp::AssertUnchanged {
                    space,
                    key,
                    version: self.provider.version(&witness)?,
                },
                AtomicOp::AssertAbsent { space, key } => ResolvedAtomicOp::AssertAbsent {
                    space,
                    key,
                    sentinel: vec![0],
                },
                AtomicOp::AssertPrefixEmpty { space, prefix } => {
                    ResolvedAtomicOp::AssertPrefixEmpty { space, prefix }
                }
                AtomicOp::Put { space, key, value } => ResolvedAtomicOp::Put { space, key, value },
                AtomicOp::PutIfAbsent { space, key, value } => {
                    ResolvedAtomicOp::PutIfAbsent { space, key, value }
                }
                AtomicOp::CompareAndPut {
                    space,
                    key,
                    witness,
                    value,
                } => ResolvedAtomicOp::CompareAndPut {
                    space,
                    key,
                    version: self.provider.version(&witness)?,
                    value,
                },
                AtomicOp::Delete { space, key } => ResolvedAtomicOp::Delete { space, key },
            };
            resolved.push(operation);
        }
        let committed = match self.provider.db.atomic(|batch| {
            for operation in &resolved {
                match operation {
                    ResolvedAtomicOp::AssertUnchanged {
                        space,
                        key,
                        version,
                    } => batch.assert_version(tree_name(*space), key, *version),
                    ResolvedAtomicOp::AssertAbsent {
                        space,
                        key,
                        sentinel,
                    } => {
                        batch.put_if_absent(tree_name(*space), key, sentinel);
                        batch.delete(tree_name(*space), key);
                    }
                    ResolvedAtomicOp::AssertPrefixEmpty { space, prefix } => {
                        batch.assert_prefix_empty(tree_name(*space), prefix);
                    }
                    ResolvedAtomicOp::Put { space, key, value } => {
                        batch.put(tree_name(*space), key, value);
                    }
                    ResolvedAtomicOp::PutIfAbsent { space, key, value } => {
                        batch.put_if_absent(tree_name(*space), key, value);
                    }
                    ResolvedAtomicOp::CompareAndPut {
                        space,
                        key,
                        version,
                        value,
                    } => batch.compare_and_put(tree_name(*space), key, *version, value),
                    ResolvedAtomicOp::Delete { space, key } => {
                        batch.delete(tree_name(*space), key);
                    }
                }
            }
        }) {
            Ok(committed) => committed,
            Err(error) => {
                self.provider.runtime_guard.poison();
                return Err(commit_error("execute metadata transaction", error));
            }
        };
        if self
            .provider
            .validate_runtime("validate committed metadata transaction")
            .is_err()
        {
            self.provider.runtime_guard.poison();
            return Err(ProviderError::unknown_commit_settled());
        }
        Ok(if committed {
            AtomicCommitOutcome::Committed
        } else {
            AtomicCommitOutcome::Conflict
        })
    }
}

fn validate_length(actual: usize, maximum: usize) -> Result<(), ProviderError> {
    if actual <= maximum {
        Ok(())
    } else {
        Err(ProviderError::transaction_too_large(actual, maximum))
    }
}

fn read_error(operation: &'static str, error: holt::Error) -> ProviderError {
    match error {
        holt::Error::BlobStoreIo(_) => ProviderError::unavailable(operation_code(operation)),
        error => ProviderError::backend(operation_code(operation), error),
    }
}

fn reserved_existing_open_error(error: holt::Error) -> ProviderError {
    held_store_error(ProviderOperationV1::Reopen, error)
}

fn held_store_object_identity(
    db: &DB,
    canonical_locator: PathBuf,
    operation: ProviderOperationV1,
) -> Result<HoltStoreObjectIdentity, ProviderError> {
    let opened = db
        .file_store_object_identity()
        .ok_or_else(|| ProviderError::authority_mismatch(operation))?;
    let validated = db
        .validate_file_store_object_set()
        .map_err(|error| held_store_error(operation, error))?
        .ok_or_else(|| ProviderError::authority_mismatch(operation))?;
    if opened != validated {
        return Err(ProviderError::authority_mismatch(operation));
    }
    Ok(HoltStoreObjectIdentity::from_holt(
        canonical_locator,
        validated,
    ))
}

fn held_store_error(operation: ProviderOperationV1, error: holt::Error) -> ProviderError {
    match error {
        holt::Error::FileStoreIdentityMismatch { .. } => {
            ProviderError::authority_mismatch(operation)
        }
        holt::Error::BlobStoreIo(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::AlreadyExists
                    | std::io::ErrorKind::InvalidInput
                    | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            ProviderError::authority_mismatch(operation)
        }
        holt::Error::BlobStoreIo(_) => ProviderError::unavailable(operation),
        error => ProviderError::backend(operation, error),
    }
}

fn commit_error(_operation: &'static str, _error: holt::Error) -> ProviderError {
    // The pinned Holt API does not expose whether DB::atomic failed before or
    // after mutating its live tree. Until Holt returns a phase-typed commit
    // error, every error from that boundary is an unknown outcome. Definite
    // plan/size rejection must happen before DB::atomic is entered.
    ProviderError::unknown_commit_settled()
}

enum ResolvedAtomicOp {
    AssertUnchanged {
        space: OrderedSpaceId,
        key: Vec<u8>,
        version: RecordVersion,
    },
    AssertAbsent {
        space: OrderedSpaceId,
        key: Vec<u8>,
        sentinel: Vec<u8>,
    },
    AssertPrefixEmpty {
        space: OrderedSpaceId,
        prefix: Vec<u8>,
    },
    Put {
        space: OrderedSpaceId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    PutIfAbsent {
        space: OrderedSpaceId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    CompareAndPut {
        space: OrderedSpaceId,
        key: Vec<u8>,
        version: RecordVersion,
        value: Vec<u8>,
    },
    Delete {
        space: OrderedSpaceId,
        key: Vec<u8>,
    },
}

enum HoltReadTree<'a> {
    Tree(&'a Tree),
    View(&'a holt::View),
}

fn tree_name(space: OrderedSpaceId) -> &'static str {
    crate::workspace::provider_catalog::holt_tree_name(space)
        .expect("Holt operations validate ordered spaces before lowering")
}

fn coalesce_scopes(scopes: &[ReadScope]) -> BTreeMap<OrderedSpaceId, Vec<u8>> {
    let mut coalesced = BTreeMap::new();
    for scope in scopes {
        coalesced
            .entry(scope.space)
            .and_modify(|prefix: &mut Vec<u8>| {
                let length = prefix
                    .iter()
                    .zip(&scope.prefix)
                    .take_while(|(left, right)| left == right)
                    .count();
                prefix.truncate(length);
            })
            .or_insert_with(|| scope.prefix.clone());
    }
    coalesced
}

fn validate_tree_registry(db: &DB) -> Result<(), ProviderError> {
    let mut actual = db
        .list_trees()
        .map_err(|error| read_error("inspect schema trees", error))?;
    actual.sort();
    let mut expected = SCHEMA_TREES
        .iter()
        .map(|tree| (*tree).to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    if actual == expected {
        Ok(())
    } else {
        Err(ProviderError::schema())
    }
}

fn require_fresh_location(path: &Path) -> Result<(), ProviderError> {
    match std::fs::metadata(path) {
        Ok(metadata) if !metadata.is_dir() => Err(ProviderError::schema()),
        Ok(_) => {
            let mut entries = std::fs::read_dir(path)
                .map_err(|error| ProviderError::backend(ProviderOperationV1::Create, error))?;
            if entries
                .next()
                .transpose()
                .map_err(|error| ProviderError::backend(ProviderOperationV1::Create, error))?
                .is_some()
            {
                Err(ProviderError::schema())
            } else {
                Ok(())
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ProviderError::backend(ProviderOperationV1::Create, error)),
    }
}

fn require_existing_location(path: &Path) -> Result<(), ProviderError> {
    let metadata = std::fs::metadata(path).map_err(|_| ProviderError::schema())?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(ProviderError::schema())
    }
}

#[derive(Clone, Debug)]
struct PreopenCreateIdentity {
    canonical_locator: PathBuf,
    existing_directory: Option<(u64, u64)>,
}

impl PreopenCreateIdentity {
    fn validate_after(&self, after: &HoltStoreObjectIdentity) -> Result<(), ProviderError> {
        if after.canonical_locator() != self.canonical_locator {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Create,
            ));
        }
        if self.existing_directory.is_some_and(|(device, inode)| {
            device != after.directory_device() || inode != after.directory_inode()
        }) {
            return Err(ProviderError::authority_mismatch(
                ProviderOperationV1::Create,
            ));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn preopen_create_identity(path: &Path) -> Result<PreopenCreateIdentity, ProviderError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ProviderError::schema());
            }
            Ok(PreopenCreateIdentity {
                canonical_locator: std::fs::canonicalize(path)
                    .map_err(|error| ProviderError::backend(ProviderOperationV1::Create, error))?,
                existing_directory: Some((metadata.dev(), metadata.ino())),
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let name = path.file_name().ok_or_else(ProviderError::schema)?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let canonical_parent = std::fs::canonicalize(parent)
                .map_err(|error| ProviderError::backend(ProviderOperationV1::Create, error))?;
            Ok(PreopenCreateIdentity {
                canonical_locator: canonical_parent.join(name),
                existing_directory: None,
            })
        }
        Err(error) => Err(ProviderError::backend(ProviderOperationV1::Create, error)),
    }
}

#[cfg(not(unix))]
fn preopen_create_identity(_path: &Path) -> Result<PreopenCreateIdentity, ProviderError> {
    Err(ProviderError::schema())
}

#[cfg(unix)]
fn capture_store_object_identity(path: &Path) -> Result<HoltStoreObjectIdentity, ProviderError> {
    let directory = std::fs::symlink_metadata(path)
        .map_err(|error| ProviderError::backend(ProviderOperationV1::Reopen, error))?;
    if directory.file_type().is_symlink() || !directory.is_dir() {
        return Err(ProviderError::authority_mismatch(
            ProviderOperationV1::Reopen,
        ));
    }
    let canonical_locator = std::fs::canonicalize(path)
        .map_err(|error| ProviderError::backend(ProviderOperationV1::Reopen, error))?;
    let lock_path = canonical_locator.join("store.lock");
    let lock = std::fs::symlink_metadata(&lock_path)
        .map_err(|error| ProviderError::backend(ProviderOperationV1::Reopen, error))?;
    if lock.file_type().is_symlink() || !lock.is_file() {
        return Err(ProviderError::authority_mismatch(
            ProviderOperationV1::Reopen,
        ));
    }
    Ok(HoltStoreObjectIdentity::from_parts(
        canonical_locator,
        directory.dev(),
        directory.ino(),
        lock.dev(),
        lock.ino(),
    ))
}

#[cfg(not(unix))]
fn capture_store_object_identity(_path: &Path) -> Result<HoltStoreObjectIdentity, ProviderError> {
    Err(ProviderError::schema())
}

fn runtime_guard_error(operation: &'static str) -> ProviderError {
    ProviderError::authority_mismatch(operation_code(operation))
}

fn operation_code(operation: &str) -> ProviderOperationV1 {
    if operation.contains("commit") || operation.contains("transaction") {
        ProviderOperationV1::Commit
    } else if operation.contains("scan") {
        ProviderOperationV1::Scan
    } else if operation.contains("read view") || operation.contains("begin metadata read") {
        ProviderOperationV1::BeginRead
    } else if operation.contains("read") || operation.contains("prefix") {
        ProviderOperationV1::ReadRecord
    } else if operation.contains("create") || operation.contains("fresh") {
        ProviderOperationV1::Create
    } else if operation.contains("open") || operation.contains("reopen") {
        ProviderOperationV1::Reopen
    } else if operation.contains("schema") || operation.contains("tree") {
        ProviderOperationV1::InspectSchema
    } else {
        ProviderOperationV1::ValidateRuntime
    }
}

#[cfg(test)]
mod tests {
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use nokv_types::CommitVersion;
    use tempfile::tempdir;

    use super::*;
    use crate::built_in_holt::HoltRuntimeGuardError;
    use crate::provider::admission::admit_provider_offer_v1;
    use crate::workspace::codec::OPERATION_TREE;
    use crate::workspace::commit_recovery_fence::mint_pending_recovery_open_for_test_v1;
    use crate::workspace::{
        AcknowledgedMetadataFrontier, MetadataCommitPurposeV1, MetadataFrontierPointV1,
        MetadataPendingRecoveryOpenBackendResultV1, MetadataPendingRecoveryOpenErrorV1,
        PlannedMetadataCommitV1,
    };

    #[derive(Default)]
    struct FailAtValidation {
        calls: AtomicUsize,
        fail_on: AtomicUsize,
        poisoned: AtomicBool,
        bindings: Mutex<Vec<HoltStoreObjectIdentity>>,
    }

    impl FailAtValidation {
        fn fail_on_nth_from_now(&self, offset: usize) {
            self.fail_on.store(
                self.calls.load(Ordering::Acquire) + offset,
                Ordering::Release,
            );
        }

        fn is_poisoned(&self) -> bool {
            self.poisoned.load(Ordering::Acquire)
        }
    }

    impl HoltRuntimeGuard for FailAtValidation {
        fn bind_store(
            &self,
            identity: &HoltStoreObjectIdentity,
        ) -> Result<(), HoltRuntimeGuardError> {
            self.bindings.lock().unwrap().push(identity.clone());
            Ok(())
        }

        fn validate_runtime(&self) -> Result<(), HoltRuntimeGuardError> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
            if self.poisoned.load(Ordering::Acquire) || call == self.fail_on.load(Ordering::Acquire)
            {
                Err(HoltRuntimeGuardError::Rejected)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    fn existing_store_fixture(path: &Path) -> (LogicalShardId, HoltStoreObjectIdentity) {
        let logical_shard_id = LogicalShardId::from_bytes([9; 16]);
        let provider = HoltProvider::create_file_observed(
            path,
            logical_shard_id,
            Arc::new(NoopHoltRuntimeGuard),
        )
        .unwrap();
        let identity = provider
            .store_object_identity
            .clone()
            .expect("file provider exposes held store identity");
        drop(provider);
        (logical_shard_id, identity)
    }

    fn reopen_request(path: &Path, logical_shard_id: LogicalShardId) -> ProviderReopenRequestV1 {
        let absolute = std::path::absolute(path).unwrap();
        ProviderReopenRequestV1::mint(
            crate::workspace::canonical_provider_schema_v1(),
            crate::workspace::MetadataStoreIdentity::standalone_holt_file(
                logical_shard_id,
                &absolute,
            ),
        )
    }

    fn create_request(
        path: &Path,
        logical_shard_id: LogicalShardId,
        recovery_intent: CreateRecoveryIntentV1,
    ) -> ProviderCreateRequestV1 {
        let absolute = std::path::absolute(path).unwrap();
        ProviderCreateRequestV1::mint(
            crate::workspace::canonical_provider_schema_v1(),
            crate::workspace::MetadataStoreIdentity::standalone_holt_file(
                logical_shard_id,
                &absolute,
            ),
            recovery_intent,
        )
    }

    fn pending_recovery_plan(
        path: &Path,
        logical_shard_id: LogicalShardId,
    ) -> PlannedMetadataCommitV1 {
        let absolute = std::path::absolute(path).unwrap();
        PlannedMetadataCommitV1::plan_exact(
            crate::workspace::MetadataStoreIdentity::standalone_holt_file(
                logical_shard_id,
                &absolute,
            ),
            [0x61; 32],
            MetadataCommitPurposeV1::Genesis {
                authority_marker_digest: [0x62; 32],
            },
            MetadataFrontierPointV1::Absent,
            AcknowledgedMetadataFrontier {
                write_sequence: 0,
                commit_version: CommitVersion::new(1).unwrap(),
                recovery_lsn: 0,
                chain_digest: [0x63; 32],
            },
        )
        .unwrap()
    }

    fn expect_provider_open_error(
        result: Result<Arc<dyn MetadataProvider>, ProviderError>,
        success_message: &str,
    ) -> ProviderError {
        match result {
            Ok(_) => panic!("{success_message}"),
            Err(error) => error,
        }
    }

    #[cfg(unix)]
    fn child_can_lock_store(path: &Path) -> bool {
        Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("workspace::provider::holt::tests::reserved_existing_child_lock_probe")
            .arg("--test-threads=1")
            .env(
                "NOKV_HOLT_RESERVED_EXISTING_CHILD_LOCK_PATH",
                path.join("store.lock"),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    }

    #[cfg(unix)]
    #[test]
    fn reserved_existing_child_lock_probe() {
        let Some(path) = std::env::var_os("NOKV_HOLT_RESERVED_EXISTING_CHILD_LOCK_PATH") else {
            return;
        };
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        lock.try_lock()
            .expect("child process could not acquire the store lock");
    }

    struct PanicOnceOnBind {
        pending: AtomicBool,
    }

    impl PanicOnceOnBind {
        fn new() -> Self {
            Self {
                pending: AtomicBool::new(true),
            }
        }
    }

    impl HoltRuntimeGuard for PanicOnceOnBind {
        fn bind_store(
            &self,
            _identity: &HoltStoreObjectIdentity,
        ) -> Result<(), HoltRuntimeGuardError> {
            if self.pending.swap(false, Ordering::AcqRel) {
                panic!("injected outer validation unwind");
            }
            Ok(())
        }

        fn validate_runtime(&self) -> Result<(), HoltRuntimeGuardError> {
            Ok(())
        }

        fn poison(&self) {}
    }

    #[cfg(unix)]
    #[test]
    fn reserved_existing_acquisition_rejects_a_wrong_expected_identity() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("SECRET-LOCATOR-SENTINEL");
        let (_, expected) = existing_store_fixture(&path);
        let wrong = HoltStoreObjectIdentity::from_parts(
            expected.canonical_locator().to_owned(),
            expected.directory_device(),
            expected.directory_inode(),
            expected.lock_device(),
            expected.lock_inode().wrapping_add(1),
        );

        let error = crate::built_in_holt::acquire_existing_file_store_reservation_v1(wrong)
            .expect_err("wrong expected lock identity acquired authority");
        assert_eq!(error.kind(), ProviderErrorKind::AuthorityMismatch);

        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        assert!(reservation.is_ready());
        assert!(!format!("{reservation:?}").contains("SECRET-LOCATOR-SENTINEL"));
    }

    #[cfg(unix)]
    #[test]
    fn reserved_existing_base_trait_is_rejected_without_consuming_the_typed_fence() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        assert!(reservation.is_ready());

        assert!(!child_can_lock_store(&path));
        let same_process_error = DB::open(TreeConfig::new(&path)).unwrap_err();
        assert!(matches!(same_process_error, holt::Error::BlobStoreIo(_)));

        let factory = crate::built_in_holt::reserved_existing_file_provider_factory_v1(
            reservation,
            Arc::new(NoopHoltRuntimeGuard),
        );
        let erased: Arc<dyn MetadataProviderFactoryV1> = Arc::clone(&factory) as Arc<_>;
        let create_request = create_request(&path, logical_shard_id, CreateRecoveryIntentV1::Fresh);
        let create = expect_provider_open_error(
            erased.create(&create_request),
            "reserved existing factory created through its base trait",
        );
        assert_eq!(create.kind(), ProviderErrorKind::AuthorityMismatch);
        assert!(matches!(
            create_request.ensure_execution_claimed(),
            Err(error) if error.kind() == ProviderErrorKind::OpenExecutionRejected
        ));
        let reopen_request = reopen_request(&path, logical_shard_id);
        let reopen = expect_provider_open_error(
            erased.reopen(&reopen_request),
            "reserved existing factory reopened through its base trait",
        );
        assert_eq!(reopen.kind(), ProviderErrorKind::AuthorityMismatch);
        assert!(matches!(
            reopen_request.ensure_execution_claimed(),
            Err(error) if error.kind() == ProviderErrorKind::OpenExecutionRejected
        ));
        assert!(!child_can_lock_store(&path));

        let planned = pending_recovery_plan(&path, logical_shard_id);
        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            factory.old_dispatch_exclusion_installation_v1(),
        );
        let opened = factory
            .reopen_pending_with_old_dispatch_excluded_v1(command)
            .into_result_for(witness)
            .expect("base-trait rejection consumed the reserved existing carrier");
        assert!(!child_can_lock_store(&path));

        drop(erased);
        drop(factory);
        assert!(!child_can_lock_store(&path));
        drop(opened);
        assert!(child_can_lock_store(&path));
    }

    #[test]
    fn ordinary_holt_installations_do_not_advertise_old_dispatch_exclusion() {
        let factories = [
            HoltProviderFactory::memory(),
            HoltProviderFactory::file(
                Path::new("unused-metadata-location"),
                Arc::new(NoopHoltRuntimeGuard),
            ),
        ];
        let planned = pending_recovery_plan(
            Path::new("unused-metadata-location"),
            LogicalShardId::from_bytes([0x64; 16]),
        );
        for factory in factories {
            assert!(!factory
                .old_dispatch_exclusion_installation_v1()
                .is_supported());
            let (foreign_installation, _) = mint_old_dispatch_exclusion_installation_v1();
            let (command, witness) = mint_pending_recovery_open_for_test_v1(
                &planned,
                crate::workspace::canonical_provider_schema_v1(),
                foreign_installation,
            );
            let outcome = factory.reopen_pending_with_old_dispatch_excluded_v1(command);
            assert_eq!(outcome.backend_result_for_forwarding(), None);
            assert!(matches!(
                outcome.into_result_for(witness),
                Err(MetadataPendingRecoveryOpenErrorV1::Unsupported)
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn reserved_recovery_fence_returns_exact_provider_and_retains_the_same_lock() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let factory = crate::built_in_holt::reserved_existing_file_provider_factory_v1(
            reservation,
            Arc::new(NoopHoltRuntimeGuard),
        );
        let installation = factory.old_dispatch_exclusion_installation_v1();
        assert!(installation.is_supported());

        let planned = pending_recovery_plan(&path, logical_shard_id);
        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let outcome = factory.reopen_pending_with_old_dispatch_excluded_v1(command);
        assert_eq!(
            outcome.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OpenedOldDispatchExcluded)
        );
        let opened = outcome.into_result_for(witness).unwrap();
        assert_eq!(opened.planned(), &planned);
        assert_eq!(opened.installation(), &installation);
        assert_eq!(opened.logical_shard_id(), logical_shard_id);
        opened.validate_runtime().unwrap();
        assert!(!child_can_lock_store(&path));

        drop(factory);
        assert!(!child_can_lock_store(&path));
        drop(opened);
        assert!(child_can_lock_store(&path));
    }

    #[cfg(unix)]
    #[test]
    fn pending_recovery_unknown_retains_the_adopted_db_after_factory_drop() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let guard = Arc::new(FailAtValidation::default());
        guard.fail_on_nth_from_now(1);
        let factory = HoltProviderFactory::reserved_existing(reservation, guard);
        let planned = pending_recovery_plan(&path, logical_shard_id);
        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            factory.old_dispatch_exclusion_installation_v1(),
        );

        let outcome = factory.reopen_pending_with_old_dispatch_excluded_v1(command);
        assert_eq!(
            outcome.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
        );
        drop(witness);
        assert!(!child_can_lock_store(&path));
        drop(factory);
        assert!(!child_can_lock_store(&path));
        drop(outcome);
        assert!(child_can_lock_store(&path));
    }

    #[test]
    fn pending_recovery_unknown_allows_retry_on_the_same_factory_allocation() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let guard = Arc::new(FailAtValidation::default());
        guard.fail_on_nth_from_now(1);
        let factory = HoltProviderFactory::reserved_existing(reservation, guard);
        let planned = pending_recovery_plan(&path, logical_shard_id);
        let installation = factory.old_dispatch_exclusion_installation_v1();
        let (first_command, first_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let first = factory.reopen_pending_with_old_dispatch_excluded_v1(first_command);
        assert_eq!(
            first.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
        );
        drop(first_witness);

        let (retry_command, retry_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let opened = factory
            .reopen_pending_with_old_dispatch_excluded_v1(retry_command)
            .into_result_for(retry_witness)
            .expect("post-adoption unknown was not retryable on the same held DB");
        opened.validate_runtime().unwrap();
        drop(first);
    }

    #[test]
    fn reserved_recovery_fence_rejects_a_capability_swap_before_opening() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let factory =
            HoltProviderFactory::reserved_existing(reservation, Arc::new(NoopHoltRuntimeGuard));
        let (foreign_installation, _) = mint_old_dispatch_exclusion_installation_v1();
        let planned = pending_recovery_plan(&path, logical_shard_id);
        let (wrong_command, wrong_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            foreign_installation,
        );
        let wrong = factory.reopen_pending_with_old_dispatch_excluded_v1(wrong_command);
        assert!(matches!(
            wrong.into_result_for(wrong_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::InvalidBinding)
        ));

        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            factory.old_dispatch_exclusion_installation_v1(),
        );
        let outcome = factory.reopen_pending_with_old_dispatch_excluded_v1(command);
        let opened = outcome
            .into_result_for(witness)
            .expect("capability rejection consumed the held reservation");
        opened.validate_runtime().unwrap();
    }

    #[test]
    fn reserved_existing_guard_error_retries_the_same_opened_store() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let expected_replay = expected.clone();
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let guard = Arc::new(FailAtValidation::default());
        guard.fail_on_nth_from_now(1);
        let factory = HoltProviderFactory::reserved_existing(reservation, guard.clone());

        let planned = pending_recovery_plan(&path, logical_shard_id);
        let installation = factory.old_dispatch_exclusion_installation_v1();
        let (first_command, first_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let first = factory.reopen_pending_with_old_dispatch_excluded_v1(first_command);
        assert_eq!(
            first.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
        );
        assert!(matches!(
            first.into_result_for(first_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));

        let (retry_command, retry_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let provider = factory
            .reopen_pending_with_old_dispatch_excluded_v1(retry_command)
            .into_result_for(retry_witness)
            .expect("guard recovery did not reuse the already-held DB");
        provider.validate_runtime().unwrap();
        assert_eq!(
            guard.bindings.lock().unwrap().as_slice(),
            &[expected_replay.clone(), expected_replay],
            "retry must replay the exact held identity after bind succeeded but validation failed"
        );
    }

    #[test]
    fn reserved_existing_outer_unwind_retries_the_same_opened_store() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let factory =
            HoltProviderFactory::reserved_existing(reservation, Arc::new(PanicOnceOnBind::new()));

        let planned = pending_recovery_plan(&path, logical_shard_id);
        let installation = factory.old_dispatch_exclusion_installation_v1();

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (command, _witness) = mint_pending_recovery_open_for_test_v1(
                &planned,
                crate::workspace::canonical_provider_schema_v1(),
                installation.clone(),
            );
            let _ = factory.reopen_pending_with_old_dispatch_excluded_v1(command);
        }));
        assert!(unwind.is_err());

        let (retry_command, retry_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let provider = factory
            .reopen_pending_with_old_dispatch_excluded_v1(retry_command)
            .into_result_for(retry_witness)
            .expect("caught unwind did not preserve exact opened-store authority");
        provider.validate_runtime().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn reserved_existing_replacement_failures_do_not_fallback_and_remain_retryable() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let held_path = temporary.path().join("metadata-held");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let factory =
            HoltProviderFactory::reserved_existing(reservation, Arc::new(NoopHoltRuntimeGuard));
        let planned = pending_recovery_plan(&path, logical_shard_id);
        let installation = factory.old_dispatch_exclusion_installation_v1();

        std::fs::rename(&path, &held_path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let (locator_command, locator_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let locator_outcome = factory.reopen_pending_with_old_dispatch_excluded_v1(locator_command);
        assert_eq!(
            locator_outcome.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
        );
        assert!(matches!(
            locator_outcome.into_result_for(locator_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));
        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(&held_path, &path).unwrap();

        let lock_path = path.join("store.lock");
        let held_lock_path = path.join("store.lock.held");
        std::fs::rename(&lock_path, &held_lock_path).unwrap();
        std::fs::File::create(&lock_path)
            .unwrap()
            .sync_all()
            .unwrap();
        let (identity_command, identity_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation.clone(),
        );
        let identity_outcome =
            factory.reopen_pending_with_old_dispatch_excluded_v1(identity_command);
        assert_eq!(
            identity_outcome.backend_result_for_forwarding(),
            Some(MetadataPendingRecoveryOpenBackendResultV1::OutcomeUnknown)
        );
        assert!(matches!(
            identity_outcome.into_result_for(identity_witness),
            Err(MetadataPendingRecoveryOpenErrorV1::OutcomeUnknown)
        ));
        std::fs::remove_file(&lock_path).unwrap();
        std::fs::rename(&held_lock_path, &lock_path).unwrap();

        let (retry_command, retry_witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            installation,
        );
        let provider = factory
            .reopen_pending_with_old_dispatch_excluded_v1(retry_command)
            .into_result_for(retry_witness)
            .expect("restored exact locator and lock identity were not retryable");
        provider.validate_runtime().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn reserved_existing_runtime_uses_db_held_full_object_set() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let (logical_shard_id, expected) = existing_store_fixture(&path);
        let reservation =
            crate::built_in_holt::acquire_existing_file_store_reservation_v1(expected).unwrap();
        let factory =
            HoltProviderFactory::reserved_existing(reservation, Arc::new(NoopHoltRuntimeGuard));
        let planned = pending_recovery_plan(&path, logical_shard_id);
        let (command, witness) = mint_pending_recovery_open_for_test_v1(
            &planned,
            crate::workspace::canonical_provider_schema_v1(),
            factory.old_dispatch_exclusion_installation_v1(),
        );
        let provider = factory
            .reopen_pending_with_old_dispatch_excluded_v1(command)
            .into_result_for(witness)
            .unwrap();

        let data_path = path.join("blobs.dat");
        let held_data_path = path.join("blobs.dat.held");
        std::fs::rename(&data_path, &held_data_path).unwrap();
        std::fs::File::create(&data_path)
            .unwrap()
            .sync_all()
            .unwrap();
        let error = provider.validate_runtime().unwrap_err();
        assert!(matches!(
            error.kind(),
            ProviderErrorKind::AuthorityMismatch | ProviderErrorKind::Unavailable
        ));
        assert_eq!(error.operation(), ProviderOperationV1::ValidateRuntime);

        std::fs::remove_file(&data_path).unwrap();
        std::fs::rename(&held_data_path, &data_path).unwrap();
        provider.validate_runtime().unwrap();
    }

    #[test]
    fn built_in_holt_offer_meets_the_complete_workspace_envelope() {
        let schema = crate::workspace::canonical_provider_schema_v1();
        let offer = HoltProviderFactory::memory()
            .contract_offer(&schema)
            .unwrap();
        let report = admit_provider_offer_v1(&schema, &offer);
        assert!(report.is_qualified(), "{report:?}");
    }

    #[test]
    fn captured_read_view_revalidates_runtime_around_access() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let guard = Arc::new(FailAtValidation::default());
        let provider = HoltProvider::create_file_observed(
            &path,
            LogicalShardId::from_bytes([1; 16]),
            guard.clone(),
        )
        .unwrap();
        let view = provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                prefix: Vec::new(),
            }])
            .unwrap();
        guard.fail_on_nth_from_now(1);

        let error = view
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"schema")
            .unwrap_err();

        assert_eq!(error.kind(), ProviderErrorKind::AuthorityMismatch);
        assert_eq!(error.operation(), ProviderOperationV1::ReadRecord);

        guard.fail_on_nth_from_now(2);
        let error = view
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"schema")
            .unwrap_err();

        assert_eq!(error.kind(), ProviderErrorKind::AuthorityMismatch);
        assert_eq!(error.operation(), ProviderOperationV1::ReadRecord);
    }

    #[test]
    fn direct_and_transaction_get_discard_values_after_failed_post_validation() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let guard = Arc::new(FailAtValidation::default());
        let provider = HoltProvider::create_file_observed(
            &path,
            LogicalShardId::from_bytes([1; 16]),
            guard.clone(),
        )
        .unwrap();
        let space = crate::workspace::provider_catalog::SYSTEM_SPACE;
        provider
            .begin_write()
            .unwrap()
            .commit(AtomicPlan {
                operations: vec![AtomicOp::Put {
                    space,
                    key: b"runtime/post-read".to_vec(),
                    value: b"must-not-escape".to_vec(),
                }],
            })
            .unwrap();

        guard.fail_on_nth_from_now(2);
        let direct_error = provider.get(space, b"runtime/post-read").unwrap_err();
        assert_eq!(direct_error.kind(), ProviderErrorKind::AuthorityMismatch);
        assert_eq!(direct_error.operation(), ProviderOperationV1::ReadRecord);

        let transaction = provider.begin_write().unwrap();
        guard.fail_on_nth_from_now(2);
        let transaction_error = transaction.get(space, b"runtime/post-read").unwrap_err();
        assert_eq!(
            transaction_error.kind(),
            ProviderErrorKind::AuthorityMismatch
        );
        assert_eq!(
            transaction_error.operation(),
            ProviderOperationV1::ReadRecord
        );
    }

    #[test]
    fn every_db_atomic_error_is_an_unknown_outcome_until_holt_types_commit_phase() {
        for error in [
            holt::Error::Internal("post-mutation WAL submission failed"),
            holt::Error::BlobStoreIo(std::io::Error::other("fsync failed")),
        ] {
            let mapped = commit_error("execute metadata transaction", error);
            assert_eq!(mapped.kind(), ProviderErrorKind::UnknownCommitSettled);
            assert_eq!(mapped.operation(), ProviderOperationV1::Commit);
        }
    }

    #[test]
    fn ordinary_file_factory_rejects_prepared_create_without_touching_the_location() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let factory = HoltProviderFactory::file(&path, Arc::new(NoopHoltRuntimeGuard));
        let request = create_request(
            &path,
            LogicalShardId::from_bytes([1; 16]),
            CreateRecoveryIntentV1::ReconcilePrepared,
        );

        let error = expect_provider_open_error(
            factory.create(&request),
            "ordinary path factory adopted a prepared create without an exact reservation",
        );
        assert_eq!(error.kind(), ProviderErrorKind::AuthorityMismatch);
        assert_eq!(error.operation(), ProviderOperationV1::Create);
        assert!(matches!(
            request.ensure_execution_claimed(),
            Err(error) if error.kind() == ProviderErrorKind::OpenExecutionRejected
        ));
        assert!(!path.exists());
    }

    #[test]
    fn post_atomic_guard_failure_is_unknown_and_poisons_on_commit_or_conflict() {
        for conflict in [false, true] {
            let temporary = tempdir().unwrap();
            let path = temporary.path().join("metadata");
            let guard = Arc::new(FailAtValidation::default());
            let provider = HoltProvider::create_file_observed(
                &path,
                LogicalShardId::from_bytes([1; 16]),
                guard.clone(),
            )
            .unwrap();

            if conflict {
                let transaction = provider.begin_write().unwrap();
                assert_eq!(
                    transaction
                        .commit(AtomicPlan {
                            operations: vec![AtomicOp::Put {
                                space: crate::workspace::provider_catalog::domain_space(
                                    crate::workspace::MetadataFamily::Operation,
                                ),
                                key: b"key".to_vec(),
                                value: b"first".to_vec(),
                            }],
                        })
                        .unwrap(),
                    AtomicCommitOutcome::Committed
                );
            }

            guard.fail_on_nth_from_now(3);
            let transaction = provider.begin_write().unwrap();
            let operation = if conflict {
                AtomicOp::PutIfAbsent {
                    space: crate::workspace::provider_catalog::domain_space(
                        crate::workspace::MetadataFamily::Operation,
                    ),
                    key: b"key".to_vec(),
                    value: b"second".to_vec(),
                }
            } else {
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::domain_space(
                        crate::workspace::MetadataFamily::Operation,
                    ),
                    key: b"key".to_vec(),
                    value: b"first".to_vec(),
                }
            };
            assert!(matches!(
                transaction.commit(AtomicPlan {
                    operations: vec![operation],
                }),
                Err(error) if error.kind() == ProviderErrorKind::UnknownCommitSettled
            ));
            assert!(guard.is_poisoned());

            if !conflict {
                assert_eq!(
                    provider
                        .db
                        .open_tree(OPERATION_TREE)
                        .unwrap()
                        .get(b"key")
                        .unwrap()
                        .unwrap(),
                    b"first"
                );
            }
        }
    }
}
