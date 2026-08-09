use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};
use foundationdb::options::{MutationType, StreamingMode, TransactionOption};
use foundationdb::{Database, FdbError, KeySelector, RangeOption, Transaction};
use nokv_types::LogicalShardId;
use tokio::runtime::Runtime;

use super::{
    all_ordered_spaces, AtomicCommitOutcome, AtomicOp, AtomicPlan, MetadataProvider,
    MetadataReadView, MetadataTransaction, OrderedSpaceId, ProviderCapabilities,
    ProviderContractOfferV1, ProviderCreateRequestV1, ProviderError, ProviderErrorKind,
    ProviderInstanceToken, ProviderOperationV1, ProviderRecord, ProviderReopenRequestV1,
    ProviderScan, ProviderScanItem, ProviderScanPage, ProviderScanStats, ProviderSchemaV1,
    ProviderTransactionModel, ProviderVersionModel, ReadScope, ReadWitness,
};
use crate::provider::v1::{CreateRecoveryIntentV1, MetadataProviderFactoryV1};
use crate::workspace::authority::MetadataStoreIdentity;

const FDB_API_VERSION: i32 = 730;
const FDB_MAX_PHYSICAL_KEY_BYTES: usize = 10_000;
const FDB_MAX_PHYSICAL_VALUE_BYTES: usize = 100_000;
const FDB_MAX_AFFECTED_BYTES: usize = 10_000_000;
const DEFAULT_TRANSACTION_BUDGET_BYTES: usize = 1_000_000;
const DEFAULT_TRANSACTION_TIMEOUT_MS: u32 = 5_000;
const FDB_MAX_READ_VIEW_DURATION: Duration = Duration::from_secs(5);
const FDB_SCAN_BATCH_ROWS: usize = 64;

const PHYSICAL_KEY_DOMAIN: &[u8] = b"\x15nokv.metadata.fdb.v1\0";
const SPACE_TAG_BYTES: usize = 2;
const VALUE_FORMAT_VERSION: u8 = 1;
const VERSIONSTAMP_BYTES: usize = 10;
const STORED_VALUE_HEADER_BYTES: usize = 1 + VERSIONSTAMP_BYTES;
const VERSIONSTAMP_OFFSET_BYTES: usize = 4;
const VERSIONSTAMP_PLACEHOLDER: [u8; VERSIONSTAMP_BYTES] = [0xff; VERSIONSTAMP_BYTES];

const RUNTIME_NEVER_STARTED: u8 = 0;
const RUNTIME_RUNNING: u8 = 1;
const RUNTIME_STOPPED: u8 = 2;
static PROCESS_RUNTIME_STATE: AtomicU8 = AtomicU8::new(RUNTIME_NEVER_STARTED);

/// FoundationDB transaction policy for one admitted metadata authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FoundationDbProviderConfig {
    /// Native FoundationDB affected-data budget. The default is one megabyte.
    pub transaction_budget_bytes: usize,
    /// Per-transaction timeout used to prevent an unavailable cluster hanging
    /// the synchronous metadata facade indefinitely.
    pub transaction_timeout_ms: u32,
}

/// Invalid FoundationDB transaction policy supplied before provider binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoundationDbProviderConfigError {
    TransactionBudgetOutOfRange,
    InvalidTransactionTimeout,
}

impl std::fmt::Display for FoundationDbProviderConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TransactionBudgetOutOfRange => write!(
                formatter,
                "FoundationDB transaction budget must be between 32 and {FDB_MAX_AFFECTED_BYTES} bytes"
            ),
            Self::InvalidTransactionTimeout => formatter.write_str(
                "FoundationDB transaction timeout must fit a positive i32",
            ),
        }
    }
}

impl std::error::Error for FoundationDbProviderConfigError {}

impl FoundationDbProviderConfig {
    /// Validate the native transaction policy without opening a database or
    /// starting the process-wide FoundationDB network.
    pub fn validate(self) -> Result<(), FoundationDbProviderConfigError> {
        if !(32..=FDB_MAX_AFFECTED_BYTES).contains(&self.transaction_budget_bytes) {
            return Err(FoundationDbProviderConfigError::TransactionBudgetOutOfRange);
        }
        if self.transaction_timeout_ms == 0 || i32::try_from(self.transaction_timeout_ms).is_err() {
            return Err(FoundationDbProviderConfigError::InvalidTransactionTimeout);
        }
        Ok(())
    }
}

impl Default for FoundationDbProviderConfig {
    fn default() -> Self {
        Self {
            transaction_budget_bytes: DEFAULT_TRANSACTION_BUDGET_BYTES,
            transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT_MS,
        }
    }
}

/// Failure to start the process-owned FoundationDB client runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoundationDbRuntimeError {
    InvalidClusterFile { message: String },
    AlreadyStarted,
    Api { code: i32, message: String },
    Tokio { message: String },
    Database { code: i32, message: String },
}

impl std::fmt::Display for FoundationDbRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidClusterFile { message } => {
                write!(formatter, "invalid FoundationDB cluster file: {message}")
            }
            Self::AlreadyStarted => formatter
                .write_str("the process-owned FoundationDB network was already started or stopped"),
            Self::Api { code, message } => {
                write!(
                    formatter,
                    "FoundationDB API initialization failed ({code}): {message}"
                )
            }
            Self::Tokio { message } => {
                write!(
                    formatter,
                    "FoundationDB Tokio runtime initialization failed: {message}"
                )
            }
            Self::Database { code, message } => {
                write!(
                    formatter,
                    "FoundationDB database open failed ({code}): {message}"
                )
            }
        }
    }
}

impl std::error::Error for FoundationDbRuntimeError {}

/// Explicit owner of the one FoundationDB network and asynchronous runtime in
/// this process. The network guard is stopped and joined when the last clone is
/// dropped; it is never leaked through static storage.
#[derive(Clone)]
pub struct FoundationDbRuntime {
    inner: Arc<FoundationDbRuntimeInner>,
}

struct FoundationDbRuntimeInner {
    cluster_file: PathBuf,
    database: Option<Database>,
    runtime: Option<Runtime>,
    network: Option<NetworkAutoStop>,
}

impl FoundationDbRuntime {
    /// Start the FoundationDB 7.3 client against one explicit cluster file.
    pub fn start(cluster_file: impl AsRef<Path>) -> Result<Self, FoundationDbRuntimeError> {
        let cluster_file = std::fs::canonicalize(cluster_file.as_ref()).map_err(|error| {
            FoundationDbRuntimeError::InvalidClusterFile {
                message: error.to_string(),
            }
        })?;
        let cluster_file_text =
            cluster_file
                .to_str()
                .ok_or_else(|| FoundationDbRuntimeError::InvalidClusterFile {
                    message: "path is not valid UTF-8".to_owned(),
                })?;
        if PROCESS_RUNTIME_STATE
            .compare_exchange(
                RUNTIME_NEVER_STARTED,
                RUNTIME_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(FoundationDbRuntimeError::AlreadyStarted);
        }

        let started = Self::start_inner(cluster_file.clone(), cluster_file_text);
        if started.is_err() {
            // FoundationDB cannot be initialized a second time after API
            // selection or network startup, even when later setup failed.
            PROCESS_RUNTIME_STATE.store(RUNTIME_STOPPED, Ordering::Release);
        }
        started
    }

    fn start_inner(
        cluster_file: PathBuf,
        cluster_file_text: &str,
    ) -> Result<Self, FoundationDbRuntimeError> {
        let network_builder = std::panic::catch_unwind(|| {
            FdbApiBuilder::default()
                .set_runtime_version(FDB_API_VERSION)
                .build()
        })
        .map_err(|_| FoundationDbRuntimeError::AlreadyStarted)?
        .map_err(runtime_api_error)?;
        // Safety: the returned guard is owned below and dropped after every
        // database handle and Tokio future executor.
        let network = unsafe { network_builder.boot() }.map_err(runtime_api_error)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("nokv-foundationdb")
            .build()
            .map_err(|error| FoundationDbRuntimeError::Tokio {
                message: error.to_string(),
            })?;
        let database = Database::from_path(cluster_file_text).map_err(|error| {
            FoundationDbRuntimeError::Database {
                code: error.code(),
                message: error.to_string(),
            }
        })?;
        Ok(Self {
            inner: Arc::new(FoundationDbRuntimeInner {
                cluster_file,
                database: Some(database),
                runtime: Some(runtime),
                network: Some(network),
            }),
        })
    }

    pub fn cluster_file(&self) -> &Path {
        &self.inner.cluster_file
    }

    fn database(&self) -> &Database {
        self.inner
            .database
            .as_ref()
            .expect("FoundationDB database outlives every runtime clone")
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner
            .runtime
            .as_ref()
            .expect("FoundationDB Tokio runtime outlives every runtime clone")
            .block_on(future)
    }
}

impl Drop for FoundationDbRuntimeInner {
    fn drop(&mut self) {
        // Drop C handles and all futures before stopping and joining the FDB
        // network thread. Field declaration order is not relied upon.
        self.database.take();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(1));
        }
        self.network.take();
        PROCESS_RUNTIME_STATE.store(RUNTIME_STOPPED, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct FoundationDbProvider {
    runtime: FoundationDbRuntime,
    logical_shard_id: LogicalShardId,
    namespace_prefix: Arc<[u8]>,
    identity: ProviderInstanceToken,
    config: FoundationDbProviderConfig,
}

#[derive(Clone)]
pub(crate) struct FoundationDbProviderFactory {
    runtime: FoundationDbRuntime,
    config: FoundationDbProviderConfig,
}

impl FoundationDbProviderFactory {
    pub(crate) fn new(
        runtime: FoundationDbRuntime,
        config: FoundationDbProviderConfig,
    ) -> Result<Self, ProviderError> {
        config
            .validate()
            .map_err(|_| ProviderError::invalid_plan())?;
        Ok(Self { runtime, config })
    }

    pub(crate) fn contract_offer_for_config(
        config: FoundationDbProviderConfig,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        config
            .validate()
            .map_err(|_| ProviderError::invalid_plan())?;
        Ok(ProviderContractOfferV1 {
            capabilities: FoundationDbProvider::capabilities_for_config(config),
        })
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

impl MetadataProviderFactoryV1 for FoundationDbProviderFactory {
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        Self::validate_schema(schema)?;
        Self::contract_offer_for_config(self.config)
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        request.claim_execution()?;
        Self::validate_schema(request.schema())?;
        if request.recovery_intent() != CreateRecoveryIntentV1::Fresh {
            return Err(ProviderError::schema());
        }
        Ok(Arc::new(FoundationDbProvider::bind(
            self.runtime.clone(),
            request.store_identity(),
            self.config,
        )?))
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        request.claim_execution()?;
        Self::validate_schema(request.schema())?;
        Ok(Arc::new(FoundationDbProvider::bind(
            self.runtime.clone(),
            request.expected_store_identity(),
            self.config,
        )?))
    }
}

impl FoundationDbProvider {
    pub(crate) fn bind(
        runtime: FoundationDbRuntime,
        store_identity: MetadataStoreIdentity,
        config: FoundationDbProviderConfig,
    ) -> Result<Self, ProviderError> {
        config
            .validate()
            .map_err(|_| ProviderError::invalid_plan())?;
        let namespace_prefix = encode_namespace_prefix(store_identity);
        if namespace_prefix.len().saturating_add(SPACE_TAG_BYTES) >= FDB_MAX_PHYSICAL_KEY_BYTES {
            return Err(ProviderError::invalid_plan());
        }
        Ok(Self {
            runtime,
            logical_shard_id: store_identity.logical_shard_id,
            namespace_prefix: namespace_prefix.into(),
            identity: ProviderInstanceToken::new(),
            config,
        })
    }

    fn capabilities_for_config(config: FoundationDbProviderConfig) -> ProviderCapabilities {
        ProviderCapabilities {
            transaction_model: ProviderTransactionModel::CrossSpaceAtomicBatch,
            version_model: ProviderVersionModel::OpaqueRecordWitness,
            consistent_cross_space_reads: true,
            // The binding uses FoundationDB's non-risky commit path. Native
            // commit_unknown_result is returned only after its dummy commit
            // barrier proves the original transaction is no longer in flight.
            all_ambiguous_commit_outcomes_settled_before_return: false,
            // A new transaction read version after that barrier is causally
            // ordered after the resolved native commit attempt.
            commit_resolution_reads_causally_current: true,
            max_key_bytes: FDB_MAX_PHYSICAL_KEY_BYTES
                - (PHYSICAL_KEY_DOMAIN.len() + 16 + 16 + 16 + 8)
                - SPACE_TAG_BYTES,
            max_value_bytes: FDB_MAX_PHYSICAL_VALUE_BYTES - STORED_VALUE_HEADER_BYTES,
            max_transaction_bytes: config.transaction_budget_bytes,
            max_atomic_operations: 1_024,
            max_logical_plan_bytes: config.transaction_budget_bytes / 2,
            exclusive_scan_start_after: true,
            consistent_snapshot_scans: true,
            // FoundationDB transactions become too old after five seconds.
            // A root-wide scan must complete inside this one consistent view;
            // reopening a transaction would not preserve the same snapshot.
            max_read_view_duration: Some(FDB_MAX_READ_VIEW_DURATION),
            // The facade still receives a complete consistent view. Native FDB
            // streaming pages remain an implementation detail.
            max_scan_items: None,
        }
    }

    fn capabilities_value(&self) -> ProviderCapabilities {
        Self::capabilities_for_config(self.config)
    }

    fn create_transaction(&self, operation: &'static str) -> Result<Transaction, ProviderError> {
        let transaction = self
            .runtime
            .database()
            .create_trx()
            .map_err(|error| read_error(operation, error))?;
        let timeout = i32::try_from(self.config.transaction_timeout_ms)
            .map_err(|_| ProviderError::invalid_plan())?;
        transaction
            .set_option(TransactionOption::Timeout(timeout))
            .map_err(|error| read_error(operation, error))?;
        Ok(transaction)
    }

    fn physical_space_prefix(&self, space: OrderedSpaceId) -> Vec<u8> {
        let mut physical = Vec::with_capacity(self.namespace_prefix.len() + SPACE_TAG_BYTES);
        physical.extend_from_slice(&self.namespace_prefix);
        physical.extend_from_slice(&space_tag(space));
        physical
    }

    fn physical_key(&self, space: OrderedSpaceId, logical_key: &[u8]) -> Vec<u8> {
        let mut physical =
            Vec::with_capacity(self.namespace_prefix.len() + SPACE_TAG_BYTES + logical_key.len());
        physical.extend_from_slice(&self.namespace_prefix);
        physical.extend_from_slice(&space_tag(space));
        physical.extend_from_slice(logical_key);
        physical
    }

    fn validate_key(&self, key: &[u8]) -> Result<(), ProviderError> {
        validate_length(key.len(), self.capabilities_value().max_key_bytes)
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), ProviderError> {
        validate_length(value.len(), self.capabilities_value().max_value_bytes)
    }

    fn validate_space(&self, space: OrderedSpaceId) -> Result<(), ProviderError> {
        if all_ordered_spaces().binary_search(&space).is_ok() {
            Ok(())
        } else {
            Err(ProviderError::invalid_plan())
        }
    }

    fn preflight_plan(&self, plan: &AtomicPlan) -> Result<(), ProviderError> {
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
            self.validate_space(space)?;
            match operation {
                AtomicOp::AssertUnchanged { key, witness, .. } => {
                    self.validate_key(key)?;
                    self.validate_witness(witness)?;
                }
                AtomicOp::AssertAbsent { key, .. } => {
                    self.validate_key(key)?;
                }
                AtomicOp::AssertPrefixEmpty { prefix, .. } => self.validate_key(prefix)?,
                AtomicOp::Put { key, value, .. } | AtomicOp::PutIfAbsent { key, value, .. } => {
                    self.validate_key(key)?;
                    self.validate_value(value)?;
                }
                AtomicOp::CompareAndPut {
                    key,
                    witness,
                    value,
                    ..
                } => {
                    self.validate_key(key)?;
                    self.validate_witness(witness)?;
                    self.validate_value(value)?;
                }
                AtomicOp::Delete { key, .. } => self.validate_key(key)?,
            }
        }
        Ok(())
    }

    fn validate_witness(&self, witness: &ReadWitness) -> Result<(), ProviderError> {
        self.witness_bytes(witness)?;
        Ok(())
    }

    fn witness_bytes<'a>(&self, witness: &'a ReadWitness) -> Result<&'a [u8], ProviderError> {
        let bytes = self
            .identity
            .parse_witness(witness)
            .map_err(|_| ProviderError::authority_mismatch(ProviderOperationV1::ValidateWitness))?;
        decode_stored_value(bytes)?;
        Ok(bytes)
    }

    fn record(&self, stored: Vec<u8>) -> Result<ProviderRecord, ProviderError> {
        let value = decode_stored_value(&stored)?.to_vec();
        Ok(ProviderRecord {
            value,
            witness: self.identity.issue_witness(stored),
        })
    }

    fn get_from(
        &self,
        transaction: &Transaction,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.validate_space(space)?;
        self.validate_key(key)?;
        let physical = self.physical_key(space, key);
        let stored = self
            .runtime
            .block_on(transaction.get(&physical, false))
            .map_err(|error| read_error("read FoundationDB metadata record", error))?
            .map(|value| value.to_vec());
        stored.map(|value| self.record(value)).transpose()
    }

    fn scan_from(
        &self,
        transaction: &Transaction,
        request: &ProviderScan,
    ) -> Result<ProviderScanPage, ProviderError> {
        self.validate_space(request.space)?;
        self.validate_key(&request.prefix)?;
        if let Some(start_after) = &request.start_after {
            self.validate_key(start_after)?;
        }

        let Some((mut begin, end)) = self.scan_range(request)? else {
            return Ok(ProviderScanPage::default());
        };

        let physical_space = self.physical_space_prefix(request.space);
        let mut items = Vec::new();
        let mut last_common_prefix = None;
        let mut visited = 0_u64;
        let mut returned = 0_u64;
        let mut common_prefixes = 0_u64;
        let mut native_requests = 0_u64;

        // A bounded logical page never asks FDB for more than the still
        // missing logical items (or one small native batch). If a batch ends
        // inside a delimiter rollup, the next selector jumps over that whole
        // subtree. Therefore a logical limit L visits at most
        // L * min(L, FDB_SCAN_BATCH_ROWS) native rows, independent of the
        // number of physical children below any one common prefix.
        loop {
            let remaining = if request.limit == 0 {
                FDB_SCAN_BATCH_ROWS
            } else {
                request.limit.saturating_sub(items.len())
            };
            if remaining == 0 {
                break;
            }
            let native_limit = remaining.min(FDB_SCAN_BATCH_ROWS);
            let option = RangeOption {
                begin: begin.clone(),
                end: end.clone(),
                limit: Some(native_limit),
                mode: StreamingMode::Exact,
                ..RangeOption::default()
            };
            native_requests = native_requests.saturating_add(1);
            let rows = self
                .runtime
                .block_on(transaction.get_range(&option, 1, false))
                .map_err(|error| read_error("scan FoundationDB metadata", error))?;
            let more = rows.more();
            visited = visited.saturating_add(rows.len() as u64);
            if rows.is_empty() {
                break;
            }

            let mut last_advance = None;
            for row in rows {
                let logical_key = row
                    .key()
                    .strip_prefix(physical_space.as_slice())
                    .ok_or_else(ProviderError::schema)?;
                let projected =
                    project_scan_row(request, logical_key, row.value(), &mut last_common_prefix)?;
                last_advance = Some(projected.advance);
                if let Some(item) = projected.item {
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
            }
            if request.limit != 0 && items.len() == request.limit {
                break;
            }
            if !more {
                break;
            }

            begin = match last_advance.ok_or_else(ProviderError::schema)? {
                ScanAdvance::AfterKey(key) => {
                    KeySelector::first_greater_than(self.physical_key(request.space, &key))
                }
                ScanAdvance::AfterCommonPrefix(prefix) => {
                    let physical = self.physical_key(request.space, &prefix);
                    let successor =
                        prefix_end(&physical).ok_or_else(ProviderError::invalid_plan)?;
                    KeySelector::first_greater_or_equal(successor)
                }
            };
        }

        if let Some(ceiling) = bounded_scan_native_row_ceiling(request.limit) {
            debug_assert!(visited <= ceiling);
        }

        Ok(ProviderScanPage {
            stats: ProviderScanStats {
                visited,
                returned,
                common_prefixes,
                restarts: native_requests.saturating_sub(1),
            },
            items,
        })
    }

    fn scan_range(
        &self,
        request: &ProviderScan,
    ) -> Result<Option<(KeySelector<'static>, KeySelector<'static>)>, ProviderError> {
        let physical_prefix = self.physical_key(request.space, &request.prefix);
        let end = prefix_end(&physical_prefix).ok_or_else(ProviderError::invalid_plan)?;
        let begin = match logical_scan_start(request) {
            LogicalScanStart::Prefix => KeySelector::first_greater_or_equal(physical_prefix),
            LogicalScanStart::AfterKey(cursor) => {
                KeySelector::first_greater_than(self.physical_key(request.space, &cursor))
            }
            LogicalScanStart::AfterCommonPrefix(common_prefix) => {
                let physical = self.physical_key(request.space, &common_prefix);
                let successor = prefix_end(&physical).ok_or_else(ProviderError::invalid_plan)?;
                KeySelector::first_greater_or_equal(successor)
            }
            LogicalScanStart::Empty => return Ok(None),
        };
        Ok(Some((begin, KeySelector::first_greater_or_equal(end))))
    }

    fn prefix_is_empty_from(
        &self,
        transaction: &Transaction,
        space: OrderedSpaceId,
        prefix: &[u8],
    ) -> Result<bool, ProviderError> {
        self.validate_space(space)?;
        self.validate_key(prefix)?;
        let begin = self.physical_key(space, prefix);
        let end = prefix_end(&begin).ok_or_else(ProviderError::invalid_plan)?;
        let option = RangeOption {
            limit: Some(1),
            mode: StreamingMode::Exact,
            ..RangeOption::from((begin, end))
        };
        let values = self
            .runtime
            .block_on(transaction.get_range(&option, 1, false))
            .map_err(|error| read_error("check FoundationDB metadata prefix", error))?;
        Ok(values.is_empty())
    }

    fn set_versionstamped(transaction: &Transaction, key: &[u8], value: &[u8]) {
        let operand = encode_versionstamped_value(value);
        transaction.atomic_op(key, &operand, MutationType::SetVersionstampedValue);
    }
}

impl MetadataProvider for FoundationDbProvider {
    fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities_value()
    }

    fn validate_runtime(&self) -> Result<(), ProviderError> {
        // This provider retains the process-owned FoundationDB runtime for its
        // complete lifetime. Serving remains unqualified for the full command
        // surface, but primitive users still share this lifecycle cut point.
        let _ = &self.runtime;
        if PROCESS_RUNTIME_STATE.load(Ordering::Acquire) == RUNTIME_RUNNING {
            Ok(())
        } else {
            Err(ProviderError::unavailable(
                ProviderOperationV1::ValidateRuntime,
            ))
        }
    }

    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        with_runtime_validation(
            || self.validate_runtime(),
            || {
                let transaction = self.create_transaction("begin FoundationDB point read")?;
                self.get_from(&transaction, space, key)
            },
        )
    }

    fn begin_read(&self, scopes: &[ReadScope]) -> Result<Box<dyn MetadataReadView>, ProviderError> {
        with_runtime_validation(
            || self.validate_runtime(),
            || {
                let scopes = coalesce_scopes(scopes);
                for (space, prefix) in &scopes {
                    self.validate_space(*space)?;
                    self.validate_key(prefix)?;
                }
                let transaction = self.create_transaction("begin FoundationDB read view")?;
                // Capture the read version now rather than lazily on the first
                // later read, so the returned view is immutable from
                // begin_read onward.
                self.runtime
                    .block_on(transaction.get_read_version())
                    .map_err(|error| read_error("capture FoundationDB read version", error))?;
                Ok(Box::new(FoundationDbReadView {
                    provider: self.clone(),
                    transaction,
                    scopes,
                }) as Box<dyn MetadataReadView>)
            },
        )
    }

    fn begin_write(&self) -> Result<Box<dyn MetadataTransaction>, ProviderError> {
        with_runtime_validation(
            || self.validate_runtime(),
            || {
                Ok(Box::new(FoundationDbTransaction {
                    provider: self.clone(),
                    transaction: self.create_transaction("begin FoundationDB transaction")?,
                }) as Box<dyn MetadataTransaction>)
            },
        )
    }
}

struct FoundationDbReadView {
    provider: FoundationDbProvider,
    transaction: Transaction,
    scopes: BTreeMap<OrderedSpaceId, Vec<u8>>,
}

impl FoundationDbReadView {
    fn validate_scope(&self, space: OrderedSpaceId, key: &[u8]) -> Result<(), ProviderError> {
        let prefix = self
            .scopes
            .get(&space)
            .ok_or_else(ProviderError::invalid_plan)?;
        if key.starts_with(prefix) {
            Ok(())
        } else {
            Err(ProviderError::invalid_plan())
        }
    }
}

impl MetadataReadView for FoundationDbReadView {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        with_runtime_validation(
            || self.provider.validate_runtime(),
            || {
                self.validate_scope(space, key)?;
                self.provider.get_from(&self.transaction, space, key)
            },
        )
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        with_runtime_validation(
            || self.provider.validate_runtime(),
            || {
                self.validate_scope(request.space, &request.prefix)?;
                self.provider.scan_from(&self.transaction, request)
            },
        )
    }
}

struct FoundationDbTransaction {
    provider: FoundationDbProvider,
    transaction: Transaction,
}

impl MetadataReadView for FoundationDbTransaction {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        with_runtime_validation(
            || self.provider.validate_runtime(),
            || self.provider.get_from(&self.transaction, space, key),
        )
    }

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
        with_runtime_validation(
            || self.provider.validate_runtime(),
            || self.provider.scan_from(&self.transaction, request),
        )
    }
}

impl MetadataTransaction for FoundationDbTransaction {
    fn prefix_is_empty(&self, space: OrderedSpaceId, prefix: &[u8]) -> Result<bool, ProviderError> {
        with_runtime_validation(
            || self.provider.validate_runtime(),
            || {
                self.provider
                    .prefix_is_empty_from(&self.transaction, space, prefix)
            },
        )
    }

    fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
        self.provider.validate_runtime()?;
        let FoundationDbTransaction {
            provider,
            transaction,
        } = *self;
        let mut entered_native_commit = false;
        let result = (|| {
            provider.preflight_plan(&plan)?;
            let mut pending = BTreeMap::<(OrderedSpaceId, Vec<u8>), Option<Vec<u8>>>::new();

            for operation in plan.operations {
                let guard_satisfied = match operation {
                    AtomicOp::AssertUnchanged {
                        space,
                        key,
                        witness,
                    } => {
                        if pending.contains_key(&(space, key.clone())) {
                            false
                        } else {
                            let physical = provider.physical_key(space, &key);
                            let actual = provider
                                .runtime
                                .block_on(transaction.get(&physical, false))
                                .map_err(|error| read_error("check FoundationDB witness", error))?;
                            actual.as_deref() == Some(provider.witness_bytes(&witness)?)
                        }
                    }
                    AtomicOp::AssertAbsent { space, key, .. } => {
                        match pending.get(&(space, key.clone())) {
                            Some(value) => value.is_none(),
                            None => {
                                let physical = provider.physical_key(space, &key);
                                provider
                                    .runtime
                                    .block_on(transaction.get(&physical, false))
                                    .map_err(|error| {
                                        read_error("check FoundationDB absence", error)
                                    })?
                                    .is_none()
                            }
                        }
                    }
                    AtomicOp::AssertPrefixEmpty { space, prefix } => {
                        let pending_present =
                            pending.iter().any(|((pending_space, key), value)| {
                                *pending_space == space
                                    && key.starts_with(&prefix)
                                    && value.is_some()
                            });
                        !pending_present
                            && provider.prefix_is_empty_from(&transaction, space, &prefix)?
                    }
                    AtomicOp::Put { space, key, value } => {
                        pending.insert((space, key), Some(value));
                        true
                    }
                    AtomicOp::PutIfAbsent { space, key, value } => {
                        let pending_key = (space, key.clone());
                        let absent = match pending.get(&pending_key) {
                            Some(value) => value.is_none(),
                            None => {
                                let physical = provider.physical_key(space, &key);
                                provider
                                    .runtime
                                    .block_on(transaction.get(&physical, false))
                                    .map_err(|error| {
                                        read_error("check FoundationDB put-if-absent", error)
                                    })?
                                    .is_none()
                            }
                        };
                        if absent {
                            pending.insert(pending_key, Some(value));
                        }
                        absent
                    }
                    AtomicOp::CompareAndPut {
                        space,
                        key,
                        witness,
                        value,
                    } => {
                        let pending_key = (space, key.clone());
                        let unchanged = if pending.contains_key(&pending_key) {
                            false
                        } else {
                            let physical = provider.physical_key(space, &key);
                            let actual = provider
                                .runtime
                                .block_on(transaction.get(&physical, false))
                                .map_err(|error| {
                                    read_error("check FoundationDB compare-and-put", error)
                                })?;
                            actual.as_deref() == Some(provider.witness_bytes(&witness)?)
                        };
                        if unchanged {
                            pending.insert(pending_key, Some(value));
                        }
                        unchanged
                    }
                    AtomicOp::Delete { space, key } => {
                        transaction.clear(&provider.physical_key(space, &key));
                        pending.insert((space, key), None);
                        true
                    }
                };
                if !guard_satisfied {
                    return Ok(AtomicCommitOutcome::Conflict);
                }
            }

            // Versionstamped values are unreadable inside their creating FDB
            // transaction. Ordered guards above therefore execute against the
            // native transaction plus `pending`, and final value mutations are
            // lowered only after every guard has succeeded. Deletes are applied
            // eagerly because native read-your-writes can observe them.
            for ((space, key), value) in pending {
                if let Some(value) = value {
                    Self::set_value(&transaction, &provider.physical_key(space, &key), &value);
                }
            }

            let budget = provider.config.transaction_budget_bytes;
            transaction
                .set_option(TransactionOption::SizeLimit(budget as i32))
                .map_err(|error| read_error("set FoundationDB transaction size limit", error))?;
            let approximate = provider
                .runtime
                .block_on(transaction.get_approximate_size())
                .map_err(|error| read_error("estimate FoundationDB transaction size", error))?;
            let approximate = usize::try_from(approximate).map_err(|_| {
                ProviderError::backend(
                    ProviderOperationV1::Commit,
                    "native approximate size was negative",
                )
            })?;
            if approximate > budget {
                return Err(ProviderError::transaction_too_large(approximate, budget));
            }

            entered_native_commit = true;
            match provider.runtime.block_on(transaction.commit()) {
                Ok(_) => Ok(AtomicCommitOutcome::Committed),
                Err(error) => match classify_commit_error(&error) {
                    CommitErrorClass::Conflict => Ok(AtomicCommitOutcome::Conflict),
                    CommitErrorClass::UnknownSettled => {
                        Err(ProviderError::unknown_commit_settled())
                    }
                    CommitErrorClass::UnknownUnsettled => {
                        Err(ProviderError::unknown_commit_unsettled())
                    }
                    CommitErrorClass::TooLarge => {
                        Err(ProviderError::transaction_too_large(approximate, budget))
                    }
                },
            }
        })();
        let post_validation = provider.validate_runtime();
        if entered_native_commit {
            finish_commit_with_runtime_validation(result, post_validation)
        } else {
            post_validation?;
            result
        }
    }
}

fn with_runtime_validation<T>(
    validate: impl Fn() -> Result<(), ProviderError>,
    operation: impl FnOnce() -> Result<T, ProviderError>,
) -> Result<T, ProviderError> {
    validate()?;
    let result = operation();
    let post_validation = validate();
    match post_validation {
        Ok(()) => result,
        Err(error) => Err(error),
    }
}

fn finish_commit_with_runtime_validation(
    result: Result<AtomicCommitOutcome, ProviderError>,
    post_validation: Result<(), ProviderError>,
) -> Result<AtomicCommitOutcome, ProviderError> {
    match (result, post_validation) {
        (Ok(AtomicCommitOutcome::Committed), Err(_)) => {
            Err(ProviderError::unknown_commit_settled())
        }
        (Err(error), _)
            if matches!(
                error.kind(),
                ProviderErrorKind::UnknownCommitSettled | ProviderErrorKind::UnknownCommitUnsettled
            ) =>
        {
            Err(error)
        }
        (result, Ok(())) => result,
        (_, Err(error)) => Err(error),
    }
}

impl FoundationDbTransaction {
    fn set_value(transaction: &Transaction, physical_key: &[u8], value: &[u8]) {
        FoundationDbProvider::set_versionstamped(transaction, physical_key, value);
    }
}

fn encode_namespace_prefix(identity: MetadataStoreIdentity) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(PHYSICAL_KEY_DOMAIN.len() + 16 + 16 + 16 + 8);
    prefix.extend_from_slice(PHYSICAL_KEY_DOMAIN);
    prefix.extend_from_slice(identity.consistency_domain_id.as_bytes());
    prefix.extend_from_slice(identity.logical_shard_id.as_bytes());
    prefix.extend_from_slice(identity.authority_id.as_bytes());
    prefix.extend_from_slice(&identity.authority_generation.get().to_be_bytes());
    prefix
}

fn space_tag(space: OrderedSpaceId) -> [u8; SPACE_TAG_BYTES] {
    space.to_be_bytes()
}

fn encode_versionstamped_value(logical: &[u8]) -> Vec<u8> {
    let mut value =
        Vec::with_capacity(STORED_VALUE_HEADER_BYTES + logical.len() + VERSIONSTAMP_OFFSET_BYTES);
    value.push(VALUE_FORMAT_VERSION);
    value.extend_from_slice(&VERSIONSTAMP_PLACEHOLDER);
    value.extend_from_slice(logical);
    value.extend_from_slice(&1_u32.to_le_bytes());
    value
}

fn decode_stored_value(value: &[u8]) -> Result<&[u8], ProviderError> {
    if value.len() < STORED_VALUE_HEADER_BYTES || value.first() != Some(&VALUE_FORMAT_VERSION) {
        return Err(ProviderError::schema());
    }
    Ok(&value[STORED_VALUE_HEADER_BYTES..])
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScanAdvance {
    AfterKey(Vec<u8>),
    AfterCommonPrefix(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogicalScanStart {
    Prefix,
    AfterKey(Vec<u8>),
    AfterCommonPrefix(Vec<u8>),
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectedScanRow {
    item: Option<ProviderScanItem>,
    advance: ScanAdvance,
}

fn delimiter_common_prefix(request: &ProviderScan, logical_key: &[u8]) -> Option<Vec<u8>> {
    let delimiter = request.delimiter?;
    let suffix = logical_key.strip_prefix(request.prefix.as_slice())?;
    let delimiter_index = suffix.iter().position(|byte| *byte == delimiter)?;
    let common_end = request.prefix.len() + delimiter_index + 1;
    Some(logical_key[..common_end].to_vec())
}

fn bounded_scan_native_row_ceiling(logical_limit: usize) -> Option<u64> {
    if logical_limit == 0 {
        return None;
    }
    let batch_rows = u64::try_from(FDB_SCAN_BATCH_ROWS.min(logical_limit)).unwrap_or(u64::MAX);
    let logical_limit = u64::try_from(logical_limit).unwrap_or(u64::MAX);
    Some(logical_limit.saturating_mul(batch_rows))
}

fn logical_scan_start(request: &ProviderScan) -> LogicalScanStart {
    match &request.start_after {
        None => LogicalScanStart::Prefix,
        Some(cursor) if cursor.starts_with(&request.prefix) => {
            delimiter_common_prefix(request, cursor).map_or_else(
                || LogicalScanStart::AfterKey(cursor.clone()),
                LogicalScanStart::AfterCommonPrefix,
            )
        }
        Some(cursor) if cursor.as_slice() < request.prefix.as_slice() => LogicalScanStart::Prefix,
        Some(_) => LogicalScanStart::Empty,
    }
}

fn project_scan_row(
    request: &ProviderScan,
    logical_key: &[u8],
    stored_value: &[u8],
    last_common_prefix: &mut Option<Vec<u8>>,
) -> Result<ProjectedScanRow, ProviderError> {
    if !logical_key.starts_with(&request.prefix) {
        return Err(ProviderError::schema());
    }

    if let Some(common_prefix) = delimiter_common_prefix(request, logical_key) {
        let excluded_by_cursor = request
            .start_after
            .as_ref()
            .is_some_and(|cursor| common_prefix.as_slice() <= cursor.as_slice());
        let duplicate = last_common_prefix.as_ref() == Some(&common_prefix);
        let item = if excluded_by_cursor || duplicate {
            None
        } else {
            *last_common_prefix = Some(common_prefix.clone());
            Some(ProviderScanItem::CommonPrefix(common_prefix.clone()))
        };
        return Ok(ProjectedScanRow {
            item,
            advance: ScanAdvance::AfterCommonPrefix(common_prefix),
        });
    }

    let item = if request
        .start_after
        .as_ref()
        .is_some_and(|cursor| logical_key <= cursor.as_slice())
    {
        None
    } else {
        Some(ProviderScanItem::Key {
            key: logical_key.to_vec(),
            value: decode_stored_value(stored_value)?.to_vec(),
        })
    };
    Ok(ProjectedScanRow {
        item,
        advance: ScanAdvance::AfterKey(logical_key.to_vec()),
    })
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while end.last() == Some(&0xff) {
        end.pop();
    }
    let last = end.last_mut()?;
    *last += 1;
    Some(end)
}

fn coalesce_scopes(scopes: &[ReadScope]) -> BTreeMap<OrderedSpaceId, Vec<u8>> {
    let mut coalesced = BTreeMap::new();
    for scope in scopes {
        coalesced
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
    coalesced
}

fn validate_length(actual: usize, maximum: usize) -> Result<(), ProviderError> {
    if actual <= maximum {
        Ok(())
    } else {
        Err(ProviderError::transaction_too_large(actual, maximum))
    }
}

fn read_error(operation: &'static str, error: FdbError) -> ProviderError {
    if error.is_retryable() {
        ProviderError::unavailable(operation_code(operation))
    } else {
        ProviderError::backend(
            operation_code(operation),
            format!("{} ({})", error, error.code()),
        )
    }
}

fn operation_code(operation: &str) -> ProviderOperationV1 {
    if operation.contains("commit") || operation.contains("transaction") {
        ProviderOperationV1::Commit
    } else if operation.contains("scan") {
        ProviderOperationV1::Scan
    } else if operation.contains("read view") {
        ProviderOperationV1::BeginRead
    } else if operation.contains("read") || operation.contains("prefix") {
        ProviderOperationV1::ReadRecord
    } else {
        ProviderOperationV1::ValidatePlan
    }
}

fn runtime_api_error(error: FdbError) -> FoundationDbRuntimeError {
    FoundationDbRuntimeError::Api {
        code: error.code(),
        message: error.to_string(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitErrorClass {
    Conflict,
    UnknownSettled,
    UnknownUnsettled,
    TooLarge,
}

fn classify_commit_error(error: &foundationdb::TransactionCommitError) -> CommitErrorClass {
    classify_commit_code(
        error.code(),
        error.is_maybe_committed(),
        error.is_retryable(),
    )
}

fn classify_commit_code(code: i32, _maybe_committed: bool, _retryable: bool) -> CommitErrorClass {
    match code {
        1020 => CommitErrorClass::Conflict,
        // transaction_too_large
        2101 => CommitErrorClass::TooLarge,
        // commit_unknown_result is the only ambiguous FoundationDB error that
        // guarantees the native request is no longer in flight on return.
        1021 => CommitErrorClass::UnknownSettled,
        // transaction_timed_out and conservative remaining ambiguity can
        // still commit later. This binding never retries either class.
        1031 => CommitErrorClass::UnknownUnsettled,
        // Other commit-stage errors are conservatively unsettled. In
        // particular, cancellation is not retryable and does not carry the
        // commit_unknown_result settlement guarantee.
        _ => CommitErrorClass::UnknownUnsettled,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::provider::admission::{admit_provider_offer_v1, ProviderAdmissionCode};
    use nokv_types::{
        ConsistencyDomainId, MetadataAuthorityGeneration, MetadataAuthorityId,
        MetadataContractDigest,
    };

    fn identity() -> MetadataStoreIdentity {
        MetadataStoreIdentity {
            logical_shard_id: LogicalShardId::from_bytes([0x22; 16]),
            authority_id: MetadataAuthorityId::from_bytes([0x33; 16]),
            authority_generation: MetadataAuthorityGeneration::new(7).unwrap(),
            consistency_domain_id: ConsistencyDomainId::from_bytes([0x11; 16]),
            profile_fingerprint: [0x44; 32],
            contract_digest: MetadataContractDigest::from_bytes([0x55; 32]),
        }
    }

    #[test]
    fn physical_namespace_contains_every_authority_dimension_and_space() {
        let identity = identity();
        let prefix = encode_namespace_prefix(identity);
        assert!(prefix.starts_with(PHYSICAL_KEY_DOMAIN));
        assert!(prefix
            .windows(16)
            .any(|window| window == identity.consistency_domain_id.as_bytes()));
        assert!(prefix
            .windows(16)
            .any(|window| window == identity.logical_shard_id.as_bytes()));
        assert!(prefix
            .windows(16)
            .any(|window| window == identity.authority_id.as_bytes()));
        assert!(prefix.ends_with(&identity.authority_generation.get().to_be_bytes()));
        assert_ne!(
            space_tag(crate::workspace::provider_catalog::SYSTEM_SPACE),
            space_tag(crate::workspace::provider_catalog::domain_space(
                crate::workspace::engine::MetadataFamily::WorkspaceCurrent
            ))
        );
    }

    #[test]
    fn physical_space_tags_are_the_frozen_ordered_space_ids() {
        let actual = all_ordered_spaces()
            .into_iter()
            .map(space_tag)
            .collect::<Vec<_>>();
        let expected = [
            0x0101_u16, 0x0102, 0x0103, 0x0104, 0x0105, 0x0106, 0x0202, 0x0203, 0x0204, 0x0205,
            0x0206, 0x0207, 0x0208, 0x0209, 0x020a, 0x020b, 0x020c, 0x020d, 0x020e, 0x020f, 0x0211,
            0x0212, 0x0213, 0x0215, 0x0216, 0x0217,
        ]
        .map(u16::to_be_bytes);
        assert_eq!(actual, expected);
    }

    #[test]
    fn versionstamped_value_keeps_provider_witness_separate_from_logical_value() {
        let operand = encode_versionstamped_value(b"logical");
        assert_eq!(
            &operand[operand.len() - VERSIONSTAMP_OFFSET_BYTES..],
            &1_u32.to_le_bytes()
        );
        let mut stored = operand[..operand.len() - VERSIONSTAMP_OFFSET_BYTES].to_vec();
        stored[1..1 + VERSIONSTAMP_BYTES].copy_from_slice(&[0x42; VERSIONSTAMP_BYTES]);
        assert_eq!(decode_stored_value(&stored).unwrap(), b"logical");
    }

    #[test]
    fn commit_error_mapping_never_replays_an_unknown_outcome() {
        assert_eq!(
            classify_commit_code(1020, false, true),
            CommitErrorClass::Conflict
        );
        assert_eq!(
            classify_commit_code(1021, true, true),
            CommitErrorClass::UnknownSettled
        );
        assert_eq!(
            classify_commit_code(1031, false, true),
            CommitErrorClass::UnknownUnsettled
        );
        assert_eq!(
            classify_commit_code(1101, false, false),
            CommitErrorClass::UnknownUnsettled
        );
        assert_eq!(
            classify_commit_code(2101, false, false),
            CommitErrorClass::TooLarge
        );
    }

    #[test]
    fn continuous_runtime_validation_brackets_each_successful_operation() {
        let validations = Cell::new(0_usize);
        let operation_ran = Cell::new(false);
        let error = with_runtime_validation(
            || {
                let call = validations.get() + 1;
                validations.set(call);
                if call == 2 {
                    Err(ProviderError::unavailable(
                        ProviderOperationV1::ValidateRuntime,
                    ))
                } else {
                    Ok(())
                }
            },
            || {
                operation_ran.set(true);
                Ok(b"must-not-escape".to_vec())
            },
        )
        .unwrap_err();

        assert!(operation_ran.get());
        assert_eq!(validations.get(), 2);
        assert_eq!(error.kind(), ProviderErrorKind::Unavailable);
        assert_eq!(error.operation(), ProviderOperationV1::ValidateRuntime);
    }

    #[test]
    fn continuous_runtime_validation_runs_after_a_native_error() {
        let validations = Cell::new(0_usize);
        let error = with_runtime_validation(
            || {
                let call = validations.get() + 1;
                validations.set(call);
                if call == 2 {
                    Err(ProviderError::authority_mismatch(
                        ProviderOperationV1::ValidateRuntime,
                    ))
                } else {
                    Ok(())
                }
            },
            || {
                Err::<(), _>(ProviderError::backend(
                    ProviderOperationV1::ReadRecord,
                    "native read failed",
                ))
            },
        )
        .unwrap_err();

        assert_eq!(validations.get(), 2);
        assert_eq!(error.kind(), ProviderErrorKind::AuthorityMismatch);
        assert_eq!(error.operation(), ProviderOperationV1::ValidateRuntime);
    }

    #[test]
    fn failed_post_commit_validation_is_always_outcome_unknown() {
        let runtime_failure = || {
            Err(ProviderError::authority_mismatch(
                ProviderOperationV1::ValidateRuntime,
            ))
        };
        let error = finish_commit_with_runtime_validation(
            Ok(AtomicCommitOutcome::Committed),
            runtime_failure(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ProviderErrorKind::UnknownCommitSettled);
        assert_eq!(error.operation(), ProviderOperationV1::Commit);

        let original_unknown = ProviderError::unknown_commit_unsettled();
        assert_eq!(
            finish_commit_with_runtime_validation(Err(original_unknown.clone()), runtime_failure()),
            Err(original_unknown)
        );
        assert_eq!(
            finish_commit_with_runtime_validation(
                Ok(AtomicCommitOutcome::Conflict),
                runtime_failure(),
            )
            .unwrap_err()
            .kind(),
            ProviderErrorKind::AuthorityMismatch
        );
    }

    #[test]
    fn prefix_successor_handles_trailing_ff_bytes() {
        assert_eq!(prefix_end(&[0x10, 0xff, 0xff]), Some(vec![0x11]));
        assert_eq!(prefix_end(&[0xff]), None);
    }

    #[test]
    fn delimiter_cursor_skips_the_complete_projected_common_prefix() {
        let request = |start_after| ProviderScan {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            prefix: b"scan/".to_vec(),
            start_after,
            delimiter: Some(b'/'),
            limit: 1,
        };

        assert_eq!(
            logical_scan_start(&request(Some(b"scan/dir/".to_vec()))),
            LogicalScanStart::AfterCommonPrefix(b"scan/dir/".to_vec())
        );
        assert_eq!(
            logical_scan_start(&request(Some(b"scan/dir/leaf".to_vec()))),
            LogicalScanStart::AfterCommonPrefix(b"scan/dir/".to_vec())
        );
        assert_eq!(
            logical_scan_start(&request(Some(b"scan/a".to_vec()))),
            LogicalScanStart::AfterKey(b"scan/a".to_vec())
        );
        assert_eq!(
            logical_scan_start(&request(Some(b"before".to_vec()))),
            LogicalScanStart::Prefix
        );
        assert_eq!(
            logical_scan_start(&request(Some(b"scan0".to_vec()))),
            LogicalScanStart::Empty
        );

        let mut undelimited = request(Some(b"scan/dir/".to_vec()));
        undelimited.delimiter = None;
        assert_eq!(
            logical_scan_start(&undelimited),
            LogicalScanStart::AfterKey(b"scan/dir/".to_vec())
        );
    }

    #[test]
    fn pure_projection_deduplicates_rollups_and_preserves_the_next_sibling() {
        let request = ProviderScan {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            prefix: b"scan/".to_vec(),
            start_after: Some(b"scan/a".to_vec()),
            delimiter: Some(b'/'),
            limit: 2,
        };
        let mut last_common_prefix = None;

        let first = project_scan_row(
            &request,
            b"scan/dir/0000",
            b"not-a-value-envelope",
            &mut last_common_prefix,
        )
        .unwrap();
        assert_eq!(
            first,
            ProjectedScanRow {
                item: Some(ProviderScanItem::CommonPrefix(b"scan/dir/".to_vec())),
                advance: ScanAdvance::AfterCommonPrefix(b"scan/dir/".to_vec()),
            }
        );

        let duplicate = project_scan_row(
            &request,
            b"scan/dir/9999",
            b"also-not-a-value-envelope",
            &mut last_common_prefix,
        )
        .unwrap();
        assert_eq!(
            duplicate,
            ProjectedScanRow {
                item: None,
                advance: ScanAdvance::AfterCommonPrefix(b"scan/dir/".to_vec()),
            }
        );

        let mut stored = vec![VALUE_FORMAT_VERSION];
        stored.extend_from_slice(&[0x42; VERSIONSTAMP_BYTES]);
        stored.extend_from_slice(b"sibling-value");
        let sibling =
            project_scan_row(&request, b"scan/z", &stored, &mut last_common_prefix).unwrap();
        assert_eq!(
            sibling,
            ProjectedScanRow {
                item: Some(ProviderScanItem::Key {
                    key: b"scan/z".to_vec(),
                    value: b"sibling-value".to_vec(),
                }),
                advance: ScanAdvance::AfterKey(b"scan/z".to_vec()),
            }
        );
    }

    #[test]
    fn pure_projection_applies_exclusive_cursor_to_projected_items() {
        let request = ProviderScan {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            prefix: b"scan/".to_vec(),
            start_after: Some(b"scan/dir/leaf".to_vec()),
            delimiter: Some(b'/'),
            limit: 1,
        };
        let mut last_common_prefix = None;
        assert_eq!(
            project_scan_row(
                &request,
                b"scan/dir/next",
                b"invalid-but-unread",
                &mut last_common_prefix,
            )
            .unwrap(),
            ProjectedScanRow {
                item: None,
                advance: ScanAdvance::AfterCommonPrefix(b"scan/dir/".to_vec()),
            }
        );
    }

    #[test]
    fn bounded_page_has_a_dataset_independent_native_row_ceiling() {
        assert_eq!(bounded_scan_native_row_ceiling(0), None);
        assert_eq!(bounded_scan_native_row_ceiling(1), Some(1));
        assert_eq!(bounded_scan_native_row_ceiling(2), Some(4));
        assert_eq!(
            bounded_scan_native_row_ceiling(FDB_SCAN_BATCH_ROWS + 1),
            Some(u64::try_from((FDB_SCAN_BATCH_ROWS + 1) * FDB_SCAN_BATCH_ROWS).unwrap())
        );
    }

    #[test]
    fn default_budget_is_below_the_native_hard_limit() {
        let config = FoundationDbProviderConfig::default();
        assert_eq!(config.transaction_budget_bytes, 1_000_000);
        assert_eq!(FDB_MAX_READ_VIEW_DURATION, Duration::from_secs(5));
        assert!(config.transaction_budget_bytes <= FDB_MAX_AFFECTED_BYTES);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn built_in_foundationdb_offer_is_not_workspace_qualified() {
        let schema = crate::workspace::canonical_provider_schema_v1();
        let capabilities =
            FoundationDbProvider::capabilities_for_config(FoundationDbProviderConfig::default());
        assert!(!capabilities.all_ambiguous_commit_outcomes_settled_before_return);
        assert!(capabilities.commit_resolution_reads_causally_current);
        let offer = ProviderContractOfferV1 { capabilities };
        let report = admit_provider_offer_v1(&schema, &offer);
        assert_eq!(
            report.rejection_codes,
            vec![
                ProviderAdmissionCode::AmbiguousCommitMayRemainInFlight,
                ProviderAdmissionCode::AtomicOperationLimitTooSmall,
                ProviderAdmissionCode::LogicalPlanLimitTooSmall,
                ProviderAdmissionCode::ReadViewLifetimeBounded,
            ]
        );
    }

    #[test]
    fn physical_key_value_and_transaction_hard_limits_are_exact() {
        let namespace = encode_namespace_prefix(identity());
        let logical_key_bytes = FDB_MAX_PHYSICAL_KEY_BYTES - namespace.len() - SPACE_TAG_BYTES;
        assert_eq!(
            namespace.len() + SPACE_TAG_BYTES + logical_key_bytes,
            FDB_MAX_PHYSICAL_KEY_BYTES
        );
        let logical_value_bytes = FDB_MAX_PHYSICAL_VALUE_BYTES - STORED_VALUE_HEADER_BYTES;
        let operand = encode_versionstamped_value(&vec![0; logical_value_bytes]);
        assert_eq!(
            operand.len() - VERSIONSTAMP_OFFSET_BYTES,
            FDB_MAX_PHYSICAL_VALUE_BYTES
        );
        assert_eq!(
            FoundationDbProviderConfig {
                transaction_budget_bytes: FDB_MAX_AFFECTED_BYTES + 1,
                ..FoundationDbProviderConfig::default()
            }
            .validate(),
            Err(FoundationDbProviderConfigError::TransactionBudgetOutOfRange)
        );
    }

    #[test]
    #[ignore = "requires NOKV_FDB_CLUSTER_FILE and a live FoundationDB 7.3 cluster"]
    fn foundationdb_provider_live_primitives() {
        let cluster_file = std::env::var("NOKV_FDB_CLUSTER_FILE")
            .unwrap_or_else(|_| panic!("NOT QUALIFIED: NOKV_FDB_CLUSTER_FILE is not set"));
        let run_id = std::env::var("NOKV_FDB_TEST_RUN_ID").unwrap_or_else(|_| {
            panic!("NOT QUALIFIED: NOKV_FDB_TEST_RUN_ID is not set to 32 hex characters")
        });
        let seed = decode_hex_16(&run_id).unwrap_or_else(|reason| {
            panic!("NOT QUALIFIED: invalid NOKV_FDB_TEST_RUN_ID: {reason}")
        });
        let runtime = FoundationDbRuntime::start(cluster_file)
            .unwrap_or_else(|error| panic!("NOT QUALIFIED: cannot start FDB runtime: {error}"));
        let identity = live_identity(seed, 0x20);
        let provider = FoundationDbProvider::bind(
            runtime.clone(),
            identity,
            FoundationDbProviderConfig::default(),
        )
        .unwrap();

        let fresh = AtomicPlan {
            operations: super::super::all_ordered_spaces()
                .into_iter()
                .map(|space| AtomicOp::AssertPrefixEmpty {
                    space,
                    prefix: Vec::new(),
                })
                .collect(),
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(fresh).unwrap(),
            AtomicCommitOutcome::Committed,
            "live namespace is not fresh; use a new NOKV_FDB_TEST_RUN_ID"
        );

        let domain = crate::workspace::provider_catalog::domain_space(
            crate::workspace::engine::MetadataFamily::Operation,
        );
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::Put {
                            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                            key: b"cross/system".to_vec(),
                            value: b"one".to_vec(),
                        },
                        AtomicOp::Put {
                            space: domain,
                            key: b"cross/domain".to_vec(),
                            value: b"one".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Committed
        );

        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::Put {
                            space: domain,
                            key: b"ryw/present".to_vec(),
                            value: b"value".to_vec(),
                        },
                        AtomicOp::AssertPrefixEmpty {
                            space: domain,
                            prefix: b"ryw/".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert!(provider.get(domain, b"ryw/present").unwrap().is_none());
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::Put {
                            space: domain,
                            key: b"ryw/transient".to_vec(),
                            value: b"value".to_vec(),
                        },
                        AtomicOp::Delete {
                            space: domain,
                            key: b"ryw/transient".to_vec(),
                        },
                        AtomicOp::AssertAbsent {
                            space: domain,
                            key: b"ryw/transient".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Committed
        );

        live_put(&provider, domain, b"occupied", b"original");
        let absence = AtomicPlan {
            operations: vec![
                AtomicOp::AssertAbsent {
                    space: domain,
                    key: b"occupied".to_vec(),
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"absence/must-not-commit".to_vec(),
                    value: b"bad".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(absence).unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert!(provider
            .get(
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"absence/must-not-commit"
            )
            .unwrap()
            .is_none());

        let absence_race = provider.begin_write().unwrap();
        assert!(absence_race.get(domain, b"absence/race").unwrap().is_none());
        live_put(&provider, domain, b"absence/race", b"winner");
        assert_eq!(
            absence_race
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::AssertAbsent {
                            space: domain,
                            key: b"absence/race".to_vec(),
                        },
                        AtomicOp::Put {
                            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                            key: b"absence/race-must-not-commit".to_vec(),
                            value: b"bad".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert!(provider
            .get(
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"absence/race-must-not-commit"
            )
            .unwrap()
            .is_none());

        live_put(&provider, domain, b"prefix/child", b"child");
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![AtomicOp::AssertPrefixEmpty {
                        space: domain,
                        prefix: b"prefix/".to_vec(),
                    }],
                })
                .unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::Delete {
                            space: domain,
                            key: b"prefix/child".to_vec(),
                        },
                        AtomicOp::AssertPrefixEmpty {
                            space: domain,
                            prefix: b"prefix/".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Committed
        );

        let prefix_race = provider.begin_write().unwrap();
        assert!(prefix_race
            .prefix_is_empty(domain, b"prefix-race/")
            .unwrap());
        live_put(&provider, domain, b"prefix-race/winner", b"winner");
        assert_eq!(
            prefix_race
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::AssertPrefixEmpty {
                            space: domain,
                            prefix: b"prefix-race/".to_vec(),
                        },
                        AtomicOp::Put {
                            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                            key: b"prefix-race/must-not-commit".to_vec(),
                            value: b"bad".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert!(provider
            .get(
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"prefix-race/must-not-commit"
            )
            .unwrap()
            .is_none());

        live_put(
            &provider,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"witness",
            b"one",
        );
        let stale_transaction = provider.begin_write().unwrap();
        let stale = stale_transaction
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"witness")
            .unwrap()
            .unwrap()
            .witness;
        // A same-value write still receives a new committed versionstamp.
        live_put(
            &provider,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"witness",
            b"one",
        );
        assert_eq!(
            stale_transaction
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::AssertUnchanged {
                            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                            key: b"witness".to_vec(),
                            witness: stale,
                        },
                        AtomicOp::Put {
                            space: domain,
                            key: b"witness/race-must-not-commit".to_vec(),
                            value: b"bad".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert!(provider
            .get(domain, b"witness/race-must-not-commit")
            .unwrap()
            .is_none());

        let foreign_provider = FoundationDbProvider::bind(
            runtime.clone(),
            identity,
            FoundationDbProviderConfig::default(),
        )
        .unwrap();
        let foreign = provider
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"witness")
            .unwrap()
            .unwrap()
            .witness;
        assert!(matches!(
            foreign_provider.begin_write().unwrap().commit(AtomicPlan {
                operations: vec![AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"witness".to_vec(),
                    witness: foreign,
                }],
            }),
            Err(error) if error.kind() == ProviderErrorKind::AuthorityMismatch
        ));

        live_put(
            &provider,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"view/system",
            b"old",
        );
        live_put(&provider, domain, b"view/domain", b"old");
        let view = provider
            .begin_read(&[
                ReadScope {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    prefix: b"view/".to_vec(),
                },
                ReadScope {
                    space: domain,
                    prefix: b"view/".to_vec(),
                },
            ])
            .unwrap();
        live_put(
            &provider,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"view/system",
            b"new",
        );
        live_put(&provider, domain, b"view/domain", b"new");
        assert_eq!(
            view.get(
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"view/system"
            )
            .unwrap()
            .unwrap()
            .value,
            b"old"
        );
        assert_eq!(
            view.get(domain, b"view/domain").unwrap().unwrap().value,
            b"old"
        );

        for key in [
            b"scan/a".as_slice(),
            b"scan/dir/one",
            b"scan/dir/two",
            b"scan/z",
        ] {
            live_put(&provider, domain, key, key);
        }
        let scan_view = provider
            .begin_read(&[ReadScope {
                space: domain,
                prefix: b"scan/".to_vec(),
            }])
            .unwrap();
        assert_eq!(
            scan_view
                .scan(&ProviderScan {
                    space: domain,
                    prefix: b"scan/".to_vec(),
                    start_after: Some(b"scan/a".to_vec()),
                    delimiter: Some(b'/'),
                    limit: 2,
                })
                .unwrap()
                .items,
            vec![
                ProviderScanItem::CommonPrefix(b"scan/dir/".to_vec()),
                ProviderScanItem::Key {
                    key: b"scan/z".to_vec(),
                    value: b"scan/z".to_vec(),
                },
            ]
        );

        let bounded_rows = (0..4_096_u32)
            .map(|index| {
                (
                    format!("bounded/dir/file-{index:04}").into_bytes(),
                    index.to_be_bytes().to_vec(),
                )
            })
            .chain(std::iter::once((
                b"bounded/z".to_vec(),
                b"sibling".to_vec(),
            )))
            .collect::<Vec<_>>();
        live_put_many(&provider, domain, &bounded_rows);
        let bounded_view = provider
            .begin_read(&[ReadScope {
                space: domain,
                prefix: b"bounded/".to_vec(),
            }])
            .unwrap();

        let first_page = bounded_view
            .scan(&ProviderScan {
                space: domain,
                prefix: b"bounded/".to_vec(),
                start_after: None,
                delimiter: Some(b'/'),
                limit: 1,
            })
            .unwrap();
        assert_eq!(
            first_page.items,
            vec![ProviderScanItem::CommonPrefix(b"bounded/dir/".to_vec())]
        );
        assert_eq!(
            first_page.stats,
            ProviderScanStats {
                visited: 1,
                returned: 0,
                common_prefixes: 1,
                restarts: 0,
            },
            "a one-item logical page must issue a one-row native read"
        );

        let second_page = bounded_view
            .scan(&ProviderScan {
                space: domain,
                prefix: b"bounded/".to_vec(),
                start_after: Some(b"bounded/dir/".to_vec()),
                delimiter: Some(b'/'),
                limit: 1,
            })
            .unwrap();
        assert_eq!(
            second_page.items,
            vec![ProviderScanItem::Key {
                key: b"bounded/z".to_vec(),
                value: b"sibling".to_vec(),
            }]
        );
        assert_eq!(
            second_page.stats,
            ProviderScanStats {
                visited: 1,
                returned: 1,
                common_prefixes: 0,
                restarts: 0,
            },
            "a common-prefix cursor must seek past all 4096 physical children"
        );

        let eof_page = bounded_view
            .scan(&ProviderScan {
                space: domain,
                prefix: b"bounded/".to_vec(),
                start_after: Some(b"bounded/z".to_vec()),
                delimiter: Some(b'/'),
                limit: 1,
            })
            .unwrap();
        assert!(eof_page.items.is_empty());

        let combined_page = bounded_view
            .scan(&ProviderScan {
                space: domain,
                prefix: b"bounded/".to_vec(),
                start_after: None,
                delimiter: Some(b'/'),
                limit: 2,
            })
            .unwrap();
        assert_eq!(
            combined_page.items,
            vec![
                ProviderScanItem::CommonPrefix(b"bounded/dir/".to_vec()),
                ProviderScanItem::Key {
                    key: b"bounded/z".to_vec(),
                    value: b"sibling".to_vec(),
                },
            ]
        );
        assert_eq!(
            combined_page.stats,
            ProviderScanStats {
                visited: 3,
                returned: 1,
                common_prefixes: 1,
                restarts: 1,
            },
            "a two-item page has a three-row native upper bound for this rollup shape"
        );

        let unbounded_page = bounded_view
            .scan(&ProviderScan {
                space: domain,
                prefix: b"bounded/".to_vec(),
                start_after: None,
                delimiter: Some(b'/'),
                limit: 0,
            })
            .unwrap();
        assert_eq!(unbounded_page.items, combined_page.items);
        assert_eq!(
            unbounded_page.stats,
            ProviderScanStats {
                visited: 65,
                returned: 1,
                common_prefixes: 1,
                restarts: 1,
            },
            "an unbounded logical scan must still consume bounded native batches"
        );

        let tight = FoundationDbProvider::bind(
            runtime.clone(),
            live_identity(seed, 0x21),
            FoundationDbProviderConfig {
                transaction_budget_bytes: 32,
                ..FoundationDbProviderConfig::default()
            },
        )
        .unwrap();
        for operation in [
            AtomicOp::AssertPrefixEmpty {
                space: domain,
                prefix: b"budget/prefix/".to_vec(),
            },
            AtomicOp::AssertAbsent {
                space: domain,
                key: b"budget/point".to_vec(),
            },
        ] {
            assert!(matches!(
                tight.begin_write().unwrap().commit(AtomicPlan {
                    operations: vec![operation],
                }),
                Err(error) if error.kind() == ProviderErrorKind::TransactionTooLarge
                    && error.limit().is_some_and(|limit| limit.max_bytes == 32)
            ));
        }

        eprintln!(
            "PASS: FoundationDB provider primitive live conformance; bounded delimiter scan: \
             children=4096, limit1_visited=1, limit2_visited=3, unbounded_visited=65"
        );
    }

    fn live_put(provider: &FoundationDbProvider, space: OrderedSpaceId, key: &[u8], value: &[u8]) {
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![AtomicOp::Put {
                        space,
                        key: key.to_vec(),
                        value: value.to_vec(),
                    }],
                })
                .unwrap(),
            AtomicCommitOutcome::Committed
        );
    }

    fn live_put_many(
        provider: &FoundationDbProvider,
        space: OrderedSpaceId,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) {
        for batch in rows.chunks(256) {
            assert_eq!(
                provider
                    .begin_write()
                    .unwrap()
                    .commit(AtomicPlan {
                        operations: batch
                            .iter()
                            .map(|(key, value)| AtomicOp::Put {
                                space,
                                key: key.clone(),
                                value: value.clone(),
                            })
                            .collect(),
                    })
                    .unwrap(),
                AtomicCommitOutcome::Committed
            );
        }
    }

    fn live_identity(seed: [u8; 16], discriminator: u8) -> MetadataStoreIdentity {
        let mut shard = seed;
        shard[0] ^= discriminator;
        let mut authority = seed;
        authority[1] ^= discriminator;
        let mut consistency = seed;
        consistency[2] ^= discriminator;
        MetadataStoreIdentity {
            logical_shard_id: LogicalShardId::from_bytes(shard),
            authority_id: MetadataAuthorityId::from_bytes(authority),
            authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
            consistency_domain_id: ConsistencyDomainId::from_bytes(consistency),
            profile_fingerprint: [discriminator; 32],
            contract_digest: crate::workspace::workspace_metadata_contract_digest(),
        }
    }

    fn decode_hex_16(input: &str) -> Result<[u8; 16], &'static str> {
        if input.len() != 32 {
            return Err("expected exactly 32 hex characters");
        }
        let mut decoded = [0_u8; 16];
        for (index, output) in decoded.iter_mut().enumerate() {
            let offset = index * 2;
            *output = u8::from_str_radix(&input[offset..offset + 2], 16)
                .map_err(|_| "contains a non-hex character")?;
        }
        if decoded.iter().all(|byte| *byte == 0) {
            return Err("all-zero run ids are reserved");
        }
        Ok(decoded)
    }
}
