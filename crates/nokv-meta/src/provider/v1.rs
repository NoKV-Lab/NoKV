//! Public metadata provider SPI version 1.
//!
//! Providers supply ordered key spaces, owned consistent read views, opaque
//! instance-bound witnesses, and one atomic batch across all touched spaces.
//! Workspace schema, command, fencing, recovery, and GC semantics remain owned
//! by [`crate::workspace::AgentMetadataStore`].

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nokv_types::{LogicalShardId, MetadataContractDigest};

use crate::workspace::MetadataStoreIdentity;

/// Stable, opaque identifier for one ordered provider key space.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderedSpaceId(u16);

impl OrderedSpaceId {
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 2] {
        self.0.to_be_bytes()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadScope {
    pub space: OrderedSpaceId,
    pub prefix: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderScan {
    pub space: OrderedSpaceId,
    pub prefix: Vec<u8>,
    pub start_after: Option<Vec<u8>>,
    pub delimiter: Option<u8>,
    /// Zero means unbounded.
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderScanItem {
    Key { key: Vec<u8>, value: Vec<u8> },
    CommonPrefix(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderScanStats {
    pub visited: u64,
    pub returned: u64,
    pub common_prefixes: u64,
    pub restarts: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderScanPage {
    pub items: Vec<ProviderScanItem>,
    pub stats: ProviderScanStats,
}

/// Opaque identity shared by one provider and every witness it issues.
#[derive(Clone, Default)]
pub struct ProviderInstanceToken {
    identity: Arc<()>,
}

impl fmt::Debug for ProviderInstanceToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderInstanceToken")
            .field("identity", &"<opaque>")
            .finish()
    }
}

impl ProviderInstanceToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn issue_witness(&self, provider_bytes: Vec<u8>) -> ReadWitness {
        ReadWitness {
            identity: Arc::clone(&self.identity),
            provider_bytes,
        }
    }

    pub fn parse_witness<'a>(&self, witness: &'a ReadWitness) -> Result<&'a [u8], ProviderError> {
        if Arc::ptr_eq(&self.identity, &witness.identity) {
            Ok(&witness.provider_bytes)
        } else {
            Err(ProviderError::invalid_plan())
        }
    }
}

/// Provider-instance-bound compare witness. It is never durable metadata.
#[derive(Clone)]
pub struct ReadWitness {
    identity: Arc<()>,
    provider_bytes: Vec<u8>,
}

impl fmt::Debug for ReadWitness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadWitness")
            .field("identity", &"<opaque>")
            .field("provider_bytes", &"<opaque>")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ProviderRecord {
    pub value: Vec<u8>,
    pub witness: ReadWitness,
}

#[derive(Clone, Debug)]
pub enum AtomicOp {
    AssertUnchanged {
        space: OrderedSpaceId,
        key: Vec<u8>,
        witness: ReadWitness,
    },
    AssertAbsent {
        space: OrderedSpaceId,
        key: Vec<u8>,
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
        witness: ReadWitness,
        value: Vec<u8>,
    },
    Delete {
        space: OrderedSpaceId,
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct AtomicPlan {
    pub operations: Vec<AtomicOp>,
}

impl AtomicPlan {
    /// Logical bytes named by this plan, excluding provider-private framing.
    #[must_use]
    pub fn logical_footprint(&self) -> usize {
        self.operations.iter().fold(0_usize, |total, operation| {
            let payload = match operation {
                AtomicOp::AssertUnchanged { key, .. }
                | AtomicOp::AssertAbsent { key, .. }
                | AtomicOp::Delete { key, .. } => key.len(),
                AtomicOp::AssertPrefixEmpty { prefix, .. } => prefix.len(),
                AtomicOp::Put { key, value, .. }
                | AtomicOp::PutIfAbsent { key, value, .. }
                | AtomicOp::CompareAndPut { key, value, .. } => {
                    key.len().saturating_add(value.len())
                }
            };
            total.saturating_add(payload)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicCommitOutcome {
    Committed,
    Conflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderTransactionModel {
    CrossSpaceAtomicBatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderVersionModel {
    OpaqueRecordWitness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderCapabilities {
    pub transaction_model: ProviderTransactionModel,
    pub version_model: ProviderVersionModel,
    pub consistent_cross_space_reads: bool,
    /// Every ambiguous native commit outcome is settled before it is returned.
    ///
    /// A provider reporting `true` must never emit
    /// [`ProviderErrorKind::UnknownCommitUnsettled`]. Providers whose timeout,
    /// cancellation, or native error surface can leave a commit in flight must
    /// report `false` and are not qualified for serving workspace writes until
    /// they add a real settlement barrier.
    pub all_ambiguous_commit_outcomes_settled_before_return: bool,
    /// Resolution reads after a settled commit outcome cross that outcome's
    /// causal cut. A provider may expose one already-captured pre-cut view;
    /// when that view reports exact prior, the immediately repeated
    /// `begin_read` must be at or after the settled cut. This capability never
    /// upgrades [`ProviderErrorKind::UnknownCommitUnsettled`].
    pub commit_resolution_reads_causally_current: bool,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_transaction_bytes: usize,
    /// Maximum operation count accepted when all other advertised limits hold.
    pub max_atomic_operations: usize,
    /// Provider-framing-independent logical plan bytes guaranteed to fit when
    /// `max_atomic_operations` and the per-key/value limits also hold.
    pub max_logical_plan_bytes: usize,
    /// `start_after` excludes the supplied key or common-prefix boundary.
    pub exclusive_scan_start_after: bool,
    /// Repeated scans on one read view retain one consistent snapshot.
    pub consistent_snapshot_scans: bool,
    pub max_read_view_duration: Option<Duration>,
    pub max_scan_items: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Backend,
    SchemaGate,
    OpenExecutionRejected,
    InvalidPlan,
    UnknownCommitSettled,
    UnknownCommitUnsettled,
    TransactionTooLarge,
    Unavailable,
    AuthorityMismatch,
}

/// Closed operation vocabulary used in redacted public provider failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderOperationV1 {
    ContractOffer,
    Create,
    Reopen,
    InspectSchema,
    ValidateRuntime,
    ValidatePlan,
    ValidateWitness,
    ReadRecord,
    BeginRead,
    Scan,
    BeginWrite,
    Commit,
    Diagnostics,
}

impl ProviderOperationV1 {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ContractOffer => "contract offer",
            Self::Create => "create",
            Self::Reopen => "reopen",
            Self::InspectSchema => "inspect schema",
            Self::ValidateRuntime => "validate runtime",
            Self::ValidatePlan => "validate plan",
            Self::ValidateWitness => "validate witness",
            Self::ReadRecord => "read record",
            Self::BeginRead => "begin read",
            Self::Scan => "scan",
            Self::BeginWrite => "begin write",
            Self::Commit => "commit",
            Self::Diagnostics => "diagnostics",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderTransactionLimit {
    pub affected_bytes: usize,
    pub max_bytes: usize,
}

/// Redacted provider failure safe to cross the public SPI boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderError {
    kind: ProviderErrorKind,
    operation: ProviderOperationV1,
    limit: Option<ProviderTransactionLimit>,
}

impl ProviderError {
    #[must_use]
    pub const fn kind(&self) -> ProviderErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn operation(&self) -> ProviderOperationV1 {
        self.operation
    }

    #[must_use]
    pub const fn limit(&self) -> Option<ProviderTransactionLimit> {
        self.limit
    }

    #[must_use]
    pub fn backend(operation: ProviderOperationV1, _source: impl fmt::Display) -> Self {
        Self::new(ProviderErrorKind::Backend, operation)
    }

    #[must_use]
    pub const fn schema() -> Self {
        Self::new(
            ProviderErrorKind::SchemaGate,
            ProviderOperationV1::InspectSchema,
        )
    }

    #[must_use]
    pub const fn invalid_plan() -> Self {
        Self::new(
            ProviderErrorKind::InvalidPlan,
            ProviderOperationV1::ValidatePlan,
        )
    }

    #[must_use]
    pub const fn unknown_commit_settled() -> Self {
        Self::new(
            ProviderErrorKind::UnknownCommitSettled,
            ProviderOperationV1::Commit,
        )
    }

    /// The native commit may still become visible after this error returns.
    #[must_use]
    pub const fn unknown_commit_unsettled() -> Self {
        Self::new(
            ProviderErrorKind::UnknownCommitUnsettled,
            ProviderOperationV1::Commit,
        )
    }

    #[must_use]
    pub const fn unavailable(operation: ProviderOperationV1) -> Self {
        Self::new(ProviderErrorKind::Unavailable, operation)
    }

    #[must_use]
    pub const fn authority_mismatch(operation: ProviderOperationV1) -> Self {
        Self::new(ProviderErrorKind::AuthorityMismatch, operation)
    }

    const fn open_execution_rejected(operation: ProviderOperationV1) -> Self {
        Self::new(ProviderErrorKind::OpenExecutionRejected, operation)
    }

    #[must_use]
    pub const fn transaction_too_large(affected_bytes: usize, max_bytes: usize) -> Self {
        Self {
            kind: ProviderErrorKind::TransactionTooLarge,
            operation: ProviderOperationV1::Commit,
            limit: Some(ProviderTransactionLimit {
                affected_bytes,
                max_bytes,
            }),
        }
    }

    const fn new(kind: ProviderErrorKind, operation: ProviderOperationV1) -> Self {
        Self {
            kind,
            operation,
            limit: None,
        }
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(limit) = self.limit {
            return write!(
                formatter,
                "metadata provider transaction is too large: {} bytes exceeds {} bytes",
                limit.affected_bytes, limit.max_bytes
            );
        }
        write!(
            formatter,
            "metadata provider {:?} during {}",
            self.kind,
            self.operation.as_str()
        )
    }
}

impl std::error::Error for ProviderError {}

/// Storage-neutral provider counters understood by the workspace facade.
///
/// `None` means the provider does not expose that dimension. Provider-private
/// diagnostics must remain in the binding and must not mint numeric IDs that
/// the facade could accidentally interpret as another provider's schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderDiagnosticsSnapshotV1 {
    pub cache_hits: Option<u64>,
    pub cache_misses: Option<u64>,
    pub full_read_operations: Option<u64>,
    pub full_read_bytes: Option<u64>,
    pub point_full_read_operations: Option<u64>,
    pub scan_full_read_operations: Option<u64>,
    pub internal_full_read_operations: Option<u64>,
    pub partial_read_cache_hits: Option<u64>,
    pub partial_read_cache_misses: Option<u64>,
}

pub trait ProviderDiagnosticsV1: Send + Sync {
    fn snapshot(&self) -> Result<ProviderDiagnosticsSnapshotV1, ProviderError>;
}

/// An owned consistent read view. Implementations must not borrow the provider
/// reference passed to `begin_read`; returned trait objects are `'static`.
///
/// Capturing a view does not permanently validate its backing runtime. Every
/// `get` and `scan` must revalidate the retained provider resources immediately
/// before native access and again after the result has been materialized. A
/// failed post-read validation discards that result rather than returning data
/// from a runtime whose authority or availability changed during the read.
pub trait MetadataReadView: Send + Sync {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError>;

    fn scan(&self, request: &ProviderScan) -> Result<ProviderScanPage, ProviderError>;
}

/// A captured provider transaction with the same continuous runtime-validation
/// requirement as [`MetadataReadView`].
///
/// `commit` must validate before entering the provider's native commit
/// boundary. Once that boundary can have committed, a failed post-commit
/// runtime validation is never a definite authority or backend failure. It is
/// [`ProviderErrorKind::UnknownCommitSettled`] when the provider has already
/// crossed its settlement barrier, and
/// [`ProviderErrorKind::UnknownCommitUnsettled`] otherwise. Callers preserve
/// the durable logical receipt and must not replay the operation as a new
/// commit.
pub trait MetadataTransaction: MetadataReadView {
    fn prefix_is_empty(&self, space: OrderedSpaceId, prefix: &[u8]) -> Result<bool, ProviderError>;

    fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError>;
}

pub trait MetadataProvider: Send + Sync {
    fn logical_shard_id(&self) -> LogicalShardId;

    fn capabilities(&self) -> ProviderCapabilities;

    /// Revalidate the process-local runtime resources backing this provider.
    ///
    /// This storage-neutral lifecycle cut point must not mutate logical
    /// workspace state or acquire ownership on the caller's behalf. A
    /// successful check at provider or captured-object creation time is not a
    /// lease for later operations: direct reads, read views, and transactions
    /// must apply the continuous pre/post validation rules documented above.
    fn validate_runtime(&self) -> Result<(), ProviderError>;

    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError>;

    /// Open one cross-space consistent view.
    ///
    /// When
    /// [`ProviderCapabilities::commit_resolution_reads_causally_current`] is
    /// true, a resolution sequence opened after a commit return may expose at
    /// most one already-captured pre-cut view. If that first view is exact
    /// prior, the immediately repeated call must observe a view at or after
    /// that commit outcome's settled causal cut.
    fn begin_read(
        &self,
        scopes: &[ReadScope],
    ) -> Result<Box<dyn MetadataReadView + 'static>, ProviderError>;

    fn begin_write(&self) -> Result<Box<dyn MetadataTransaction + 'static>, ProviderError>;

    fn diagnostics(&self) -> Option<&dyn ProviderDiagnosticsV1> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSchemaV1 {
    spi_major: u16,
    workspace_contract_digest: MetadataContractDigest,
    ordered_spaces: Vec<OrderedSpaceId>,
}

impl ProviderSchemaV1 {
    pub const SPI_MAJOR: u16 = 1;

    pub(crate) fn new(
        workspace_contract_digest: MetadataContractDigest,
        ordered_spaces: Vec<OrderedSpaceId>,
    ) -> Result<Self, ProviderError> {
        if ordered_spaces.is_empty() {
            return Err(ProviderError::schema());
        }
        if ordered_spaces.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ProviderError::schema());
        }
        Ok(Self {
            spi_major: Self::SPI_MAJOR,
            workspace_contract_digest,
            ordered_spaces,
        })
    }

    #[must_use]
    pub const fn spi_major(&self) -> u16 {
        self.spi_major
    }

    #[must_use]
    pub const fn workspace_contract_digest(&self) -> MetadataContractDigest {
        self.workspace_contract_digest
    }

    #[must_use]
    pub fn ordered_spaces(&self) -> &[OrderedSpaceId] {
        &self.ordered_spaces
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderContractOfferV1 {
    pub capabilities: ProviderCapabilities,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateRecoveryIntentV1 {
    Fresh,
    /// Characterized intent only. A built-in provider must reject it unless
    /// the call carries a reviewed provider-specific same-resource authority;
    /// the ordinary Holt path factory does not.
    ReconcilePrepared,
}

struct ProviderOpenExecutionCapabilityV1 {
    operation: ProviderOperationV1,
    claimed: AtomicBool,
}

impl ProviderOpenExecutionCapabilityV1 {
    fn mint(operation: ProviderOperationV1) -> Self {
        Self {
            operation,
            claimed: AtomicBool::new(false),
        }
    }

    fn claim(&self) -> Result<(), ProviderError> {
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| ProviderError::open_execution_rejected(self.operation))
    }

    fn ensure_claimed(&self) -> Result<(), ProviderError> {
        if self.claimed.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(ProviderError::open_execution_rejected(self.operation))
        }
    }
}

/// One engine-authorized provider-create execution.
///
/// The request is deliberately neither constructible nor cloneable outside
/// `nokv-meta`. A provider factory may inspect its storage-neutral fields, but
/// must call [`Self::claim_execution`] before touching backend, path, or
/// runtime state.
///
/// This base request carries no pending-receipt or old-dispatch-exclusion
/// authority. Reserved recovery installations must reject it before claim.
///
/// A crate-external caller cannot construct a request and use a stock built-in
/// factory to obtain the raw provider transaction surface:
///
/// ```compile_fail
/// use nokv_meta::built_in_holt::memory_provider_factory_v1;
/// use nokv_meta::provider::v1::{
///     CreateRecoveryIntentV1, MetadataProviderFactoryV1, ProviderCreateRequestV1,
/// };
/// use nokv_meta::workspace::{canonical_provider_schema_v1, MetadataStoreIdentity};
///
/// let identity: MetadataStoreIdentity = todo!();
/// let request = ProviderCreateRequestV1 {
///     schema: canonical_provider_schema_v1(),
///     store_identity: identity,
///     recovery_intent: CreateRecoveryIntentV1::Fresh,
/// };
/// let factory = memory_provider_factory_v1();
/// let _raw_provider = factory.create(&request);
/// ```
///
/// The engine execution cannot be duplicated for later replay:
///
/// ```compile_fail
/// use nokv_meta::provider::v1::ProviderCreateRequestV1;
///
/// fn duplicate(request: ProviderCreateRequestV1) {
///     let _replay = request.clone();
/// }
/// ```
pub struct ProviderCreateRequestV1 {
    schema: ProviderSchemaV1,
    store_identity: MetadataStoreIdentity,
    recovery_intent: CreateRecoveryIntentV1,
    execution: ProviderOpenExecutionCapabilityV1,
}

impl ProviderCreateRequestV1 {
    pub(crate) fn mint(
        schema: ProviderSchemaV1,
        store_identity: MetadataStoreIdentity,
        recovery_intent: CreateRecoveryIntentV1,
    ) -> Self {
        Self {
            schema,
            store_identity,
            recovery_intent,
            execution: ProviderOpenExecutionCapabilityV1::mint(ProviderOperationV1::Create),
        }
    }

    #[must_use]
    pub const fn schema(&self) -> &ProviderSchemaV1 {
        &self.schema
    }

    #[must_use]
    pub const fn store_identity(&self) -> MetadataStoreIdentity {
        self.store_identity
    }

    #[must_use]
    pub const fn recovery_intent(&self) -> CreateRecoveryIntentV1 {
        self.recovery_intent
    }

    /// Claim this exact engine-minted execution before opening provider state.
    ///
    /// A request can be claimed exactly once. Forwarding wrappers must leave
    /// the claim to the ultimate factory that touches backend state.
    pub fn claim_execution(&self) -> Result<(), ProviderError> {
        self.execution.claim()
    }

    pub(crate) fn ensure_execution_claimed(&self) -> Result<(), ProviderError> {
        self.execution.ensure_claimed()
    }
}

impl fmt::Debug for ProviderCreateRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCreateRequestV1")
            .field("schema", &self.schema)
            .field("store_identity", &self.store_identity)
            .field("recovery_intent", &self.recovery_intent)
            .finish_non_exhaustive()
    }
}

/// One engine-authorized provider-reopen execution.
///
/// Like [`ProviderCreateRequestV1`], this request is private-construction and
/// one-shot.
///
/// This base request carries no pending-receipt or old-dispatch-exclusion
/// authority. Reserved recovery installations must reject it before claim.
///
/// Crate-external code therefore cannot directly reopen a stock built-in
/// factory and retain its raw provider handle:
///
/// ```compile_fail
/// use nokv_meta::built_in_holt::file_provider_factory_v1;
/// use nokv_meta::built_in_holt::HoltRuntimeGuard;
/// use nokv_meta::provider::v1::{MetadataProviderFactoryV1, ProviderReopenRequestV1};
/// use nokv_meta::workspace::{canonical_provider_schema_v1, MetadataStoreIdentity};
/// use std::sync::Arc;
///
/// let guard: Arc<dyn HoltRuntimeGuard> = todo!();
/// let identity: MetadataStoreIdentity = todo!();
/// let request = ProviderReopenRequestV1 {
///     schema: canonical_provider_schema_v1(),
///     expected_store_identity: identity,
/// };
/// let factory = file_provider_factory_v1("metadata", guard);
/// let _raw_provider = factory.reopen(&request);
/// ```
pub struct ProviderReopenRequestV1 {
    schema: ProviderSchemaV1,
    expected_store_identity: MetadataStoreIdentity,
    execution: ProviderOpenExecutionCapabilityV1,
}

impl ProviderReopenRequestV1 {
    pub(crate) fn mint(
        schema: ProviderSchemaV1,
        expected_store_identity: MetadataStoreIdentity,
    ) -> Self {
        Self {
            schema,
            expected_store_identity,
            execution: ProviderOpenExecutionCapabilityV1::mint(ProviderOperationV1::Reopen),
        }
    }

    #[must_use]
    pub const fn schema(&self) -> &ProviderSchemaV1 {
        &self.schema
    }

    #[must_use]
    pub const fn expected_store_identity(&self) -> MetadataStoreIdentity {
        self.expected_store_identity
    }

    /// Claim this exact engine-minted execution before opening provider state.
    ///
    /// A request can be claimed exactly once. Forwarding wrappers must leave
    /// the claim to the ultimate factory that touches backend state.
    pub fn claim_execution(&self) -> Result<(), ProviderError> {
        self.execution.claim()
    }

    pub(crate) fn ensure_execution_claimed(&self) -> Result<(), ProviderError> {
        self.execution.ensure_claimed()
    }
}

impl fmt::Debug for ProviderReopenRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReopenRequestV1")
            .field("schema", &self.schema)
            .field("expected_store_identity", &self.expected_store_identity)
            .finish_non_exhaustive()
    }
}

pub trait MetadataProviderFactoryV1: Send + Sync {
    /// Validate the canonical engine contract without opening provider state.
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError>;

    /// Open one create execution minted by the canonical metadata engine.
    ///
    /// The ultimate factory that first touches backend, path, or runtime state
    /// must call [`ProviderCreateRequestV1::claim_execution`] before doing so.
    /// A forwarding wrapper validates only storage-neutral request data and
    /// leaves the one-shot claim to its delegate.
    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError>;

    /// Open one reopen execution minted by the canonical metadata engine.
    ///
    /// The ultimate factory follows the same claim-before-side-effect rule as
    /// [`Self::create`].
    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_object_safe(
        _provider: Option<&dyn MetadataProvider>,
        _view: Option<&dyn MetadataReadView>,
        _transaction: Option<&dyn MetadataTransaction>,
        _factory: Option<&dyn MetadataProviderFactoryV1>,
    ) {
    }

    #[test]
    fn traits_are_object_safe() {
        assert_object_safe(None, None, None, None);
    }

    #[test]
    fn witness_is_instance_bound_and_debug_redacted() {
        let first = ProviderInstanceToken::new();
        let second = ProviderInstanceToken::new();
        let witness = first.issue_witness(b"secret-version".to_vec());
        assert_eq!(first.parse_witness(&witness).unwrap(), b"secret-version");
        assert_eq!(
            second.parse_witness(&witness).unwrap_err().kind(),
            ProviderErrorKind::InvalidPlan
        );
        assert!(!format!("{witness:?}").contains("secret-version"));
    }

    #[test]
    fn provider_error_display_and_debug_never_include_backend_source() {
        let error = ProviderError::backend(
            ProviderOperationV1::ReadRecord,
            "DO_NOT_LEAK_PROVIDER_SECRET=/private/metadata/path",
        );
        assert!(!error.to_string().contains("DO_NOT_LEAK_PROVIDER_SECRET"));
        assert!(!format!("{error:?}").contains("DO_NOT_LEAK_PROVIDER_SECRET"));
        assert_eq!(error.kind(), ProviderErrorKind::Backend);
        assert_eq!(error.operation(), ProviderOperationV1::ReadRecord);
    }

    #[test]
    fn open_execution_capability_is_one_shot() {
        for operation in [ProviderOperationV1::Create, ProviderOperationV1::Reopen] {
            let execution = ProviderOpenExecutionCapabilityV1::mint(operation);
            assert_eq!(
                execution.ensure_claimed().unwrap_err().kind(),
                ProviderErrorKind::OpenExecutionRejected
            );
            execution.claim().unwrap();
            execution.ensure_claimed().unwrap();
            let replay = execution.claim().unwrap_err();
            assert_eq!(replay.kind(), ProviderErrorKind::OpenExecutionRejected);
            assert_eq!(replay.operation(), operation);
        }
    }
}
